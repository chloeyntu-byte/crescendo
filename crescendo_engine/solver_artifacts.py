"""Bounded, owned solver and Batch scheduler log evidence; never completion authority.

The solver receipt and native result reader remain authoritative. Scheduler
statistics and ALPS companion logs are retained before raw point retirement so
the two portability logs can explain a failure without another user upload.
"""
import hashlib
import os
import re

from .scheduler_protocol import valid_job_id
from .owned_solver_logs import OwnedSolverLogError, owned_solver_log_reader

MAX_LOG_BYTES = 64 * 1024
MAX_CONTEXT_LINES = 80
MAX_TASK_RECORDS = 16
_FIELD = re.compile(r'^\s*([A-Z][A-Z0-9_]*(?:\(%\))?)\s*:\s*(.*?)\s*$')
_ERROR = re.compile(r'\berror\b|\bfatal\b|\bwarn(?:ing)?\b|\*Error\*|'
                    r'license|licence|permission denied|no space left', re.I)
_WARNING = re.compile(r'\bwarn(?:ing)?\b|\bunsupported\b|\bnot supported\b|\bignored\b', re.I)
_TASK_FIELDS = frozenset(('JOB_ID', 'JOB_NAME', 'TASK_NAME', 'REPLICA',
    'TASKGROUP_NAME', 'SUBMIT_NODE', 'EXEC_NODE', 'EXIT_CODE', 'EXIT_MESSAGE'))
_RESOURCE_FIELDS = frozenset(('REQ_CPU', 'REQ_MEM', 'USER_CPU_TIME',
    'SYSTEM_CPU_TIME', 'MEMSW_MAX', 'SWAP_MAX', 'MEM_MAX', 'MEM_AVG',
    'MRUN_TIME', 'CPU_UTIL_AVG(%)', 'CPU_UTIL_RECENT(%)'))


def parse_scheduler_task_log(text, expected_id):
    """Select complete TASK_INFO sections for the exact owned Job ID.

    A zero TASK_INFO exit is a scheduler observation, not a solver result or
    termination proof. Batch scheduler output can append previous jobs: their records
    and statistics are explicitly ignored. An ambiguous or incomplete owned
    record cannot be collapsed into a successful last record.
    """
    expected = str(expected_id) if valid_job_id(expected_id) else None
    result = {'schema_version': 'crescendo.scheduler_task_log.v1',
        'expected_job_id': expected, 'job_id': None, 'observed_job_ids': [],
        'valid': False, 'records': [], 'resource_usage': {},
        'ignored_job_ids': [], 'foreign_record_count': 0, 'selected_line_ranges': [],
        'scheduler_task_exit_all_zero': False,
        'native_completion_authority': False, 'remote_stop_authority': False}
    if expected is None or not isinstance(text, str) or len(text.encode('utf-8')) > MAX_LOG_BYTES + 256:
        result['reason'] = 'invalid_job_id_or_text_limit'
        return result
    identities, segments = set(), []
    current, section, unbound_identity = None, None, False
    lines = text.splitlines()
    for number, line in enumerate(lines, 1):
        match = _FIELD.match(line)
        if not match:
            continue
        key, value = match.groups()
        if key == 'TASK_INFO' and not value:
            if current is not None:
                current['end'] = number - 1
            current = {'task': {}, 'resources': {}, 'ids': [], 'invalid': False,
                       'start': number, 'end': len(lines)}
            segments.append(current)
            section = 'task'
            continue
        if key == 'RESOURCE_USAGE_SUMMARY' and not value:
            section = 'resource'
            continue
        if key == 'JOB_ID':
            if valid_job_id(value):
                identities.add(value)
                if current is not None:
                    current['ids'].append(value)
                else:
                    unbound_identity = True
            else:
                if current is not None:
                    current['invalid'] = True
                else:
                    unbound_identity = True
        if section == 'task' and current is not None and key in _TASK_FIELDS:
            if key in current['task']:
                current['invalid'] = True
            current['task'][key] = value[:512]
        elif section == 'resource' and current is not None and key in _RESOURCE_FIELDS:
            if key in current['resources']:
                current['invalid'] = True
            if not re.fullmatch(r'-?[0-9]+(?:\.[0-9]+)?', value):
                current['invalid'] = True
            else:
                current['resources'][key] = float(value) if '.' in value else int(value)
        elif key not in _TASK_FIELDS and key not in _RESOURCE_FIELDS and not value:
            section = None
    result['observed_job_ids'] = sorted(identities)
    result['ignored_job_ids'] = sorted(identities - {expected})
    selected, invalid = [], unbound_identity or len(segments) > MAX_TASK_RECORDS
    for segment in segments[:MAX_TASK_RECORDS]:
        ids = segment['ids']
        if not ids or len(set(ids)) != 1:
            # Missing/ambiguous identity cannot safely be assigned to an old job.
            invalid = True
        elif ids[0] != expected:
            result['foreign_record_count'] += 1
            continue
        if expected in ids:
            selected.append(segment)
    task_keys = set()
    usage_records = []
    for segment in selected:
        record = segment['task']
        row = {key.lower(): value for key, value in record.items()}
        code = row.get('exit_code')
        if (segment['invalid'] or record.get('JOB_ID') != expected or len(segment['ids']) != 1
                or code is None or not re.fullmatch(r'-?[0-9]{1,10}', code)):
            invalid = True
        else:
            row['exit_code'] = int(code)
        task_key = (row.get('task_name'), row.get('replica'))
        if len(selected) > 1 and (not task_key[0] or task_key in task_keys):
            invalid = True
        task_keys.add(task_key)
        result['records'].append(row)
        result['selected_line_ranges'].append([segment['start'], segment['end']])
        usage = {key.lower().replace('(%)', '_percent'): value for key, value in segment['resources'].items()}
        usage_records.append(dict(task_name=row.get('task_name'), replica=row.get('replica'), values=usage))
    result['resource_usage_records'] = usage_records
    if usage_records:
        # Shared request values can be shown once; task-specific usage remains
        # per task, and is never summed or borrowed from a foreign job.
        result['resource_usage'] = {key: value for key, value in usage_records[0]['values'].items()
            if all(record['values'].get(key) == value for record in usage_records)}
    result['valid'] = bool(selected and not invalid)
    if result['valid']:
        result['job_id'] = expected
        result['scheduler_task_exit_all_zero'] = all(row['exit_code'] == 0 for row in result['records'])
        result['reason'] = 'exact_owned_job_task_info'
    else:
        result['reason'] = 'invalid_owned_or_ambiguous_task_records' if invalid else 'task_info_identity_unavailable'
    return result


def log_context(text):
    """Keep errors/warnings and scheduler identity even before a long tail."""
    lines = text.splitlines()
    critical, hits = [], []
    for index, line in enumerate(lines):
        field = _FIELD.match(line)
        if field and field.group(1) in _TASK_FIELDS | _RESOURCE_FIELDS | {'TASK_INFO', 'RESOURCE_USAGE_SUMMARY'}:
            critical.append(index)
        if _ERROR.search(line):
            hits.append(index)
    priority = list(dict.fromkeys(hits[:24] + critical))
    selected = set(priority[:MAX_CONTEXT_LINES])
    for index in hits:
        selected.update(range(max(0, index - 2), min(len(lines), index + 3)))
        if len(selected) >= MAX_CONTEXT_LINES - 12:
            break
    selected.update(range(min(4, len(lines))))
    selected.update(range(max(0, len(lines) - 8), len(lines)))
    # Preserve important fields first when output has a very large warning set.
    chosen = set(priority[:MAX_CONTEXT_LINES])
    chosen.update(sorted(selected - chosen)[:max(0, MAX_CONTEXT_LINES - len(chosen))])
    chosen = sorted(chosen)
    return {'line_count': len(lines), 'error_or_warning_count': len(hits),
        'first_error_or_warning_line': hits[0] + 1 if hits else None,
        'lines': [{'line': index + 1, 'text': lines[index]} for index in chosen],
        'context_truncated': len(selected) > len(chosen),
        'complete_text_included': len(chosen) == len(lines),
        'warning_taxonomy': warning_taxonomy(text)}


def warning_taxonomy(text):
    """Bounded warning hints; preserve every count and never suppress errors."""
    categories = {}
    suppression = re.compile(r'maxwarnstologfile|remaining.*suppress|(?:warn|limit).*exceed|exceed.*(?:warn|limit)', re.I)
    rules = (('warning_suppression_notice', suppression),
        ('numerical_option', re.compile(r'\bpivrel\b', re.I)),
        ('ignored_solver_option', re.compile(r'\b(?:sensfile|checklimitdest)\b', re.I)),
        ('physical_circuit', re.compile(r'no\s+dc\s+(?:path|value)|\bPORT0\b.*reset|reset.*\bPORT0\b', re.I)),
        ('pdk_parameter_redefinition', re.compile(r'\bredefinedparams\b|parameter.*redefin', re.I)),
        ('annotation_or_display', re.compile(r'\b(?:annotate|cols)\b', re.I)),
        ('unsupported_metadata', re.compile(r'\binfo\b.*\b(?:output|primitives)\b|\bwhat\b.*\b(?:output|primitives)\b', re.I)))
    total, suppression_observed = 0, False
    for number, line in enumerate(text.splitlines(), 1):
        suppressed = bool(suppression.search(line))
        if not _WARNING.search(line) and not suppressed:
            continue
        suppression_observed = suppression_observed or suppressed
        total += 1
        category = next((name for name, pattern in rules if pattern.search(line)), 'unknown_warning')
        item = categories.setdefault(category, {'count': 0, 'first_examples': [], 'additional_examples_omitted': 0})
        item['count'] += 1
        if len(item['first_examples']) < 2:
            item['first_examples'].append({'line': number, 'text': line[:512]})
        else:
            item['additional_examples_omitted'] += 1
    return {'warning_line_count': total, 'categories': categories,
            'warning_suppression_observed': suppression_observed,
            'suppressed_warning_count': None,
            'scope': 'diagnostic classification only; warnings are counted, not suppressed',
            'completion_authority': False}


def scheduler_log_context(text, observation):
    """Keep selected scheduler fields, not errors from an appended old run.

    Text before a footer cannot be bound to that footer's job reliably. Only
    explicitly selected TASK_INFO/resource fields are attributed to this job.
    """
    lines = text.splitlines()
    selected = []
    for start, end in observation.get('selected_line_ranges') or []:
        for index in range(max(0, start - 1), min(len(lines), end)):
            match = _FIELD.match(lines[index])
            if match and match.group(1) in _TASK_FIELDS | _RESOURCE_FIELDS | {'TASK_INFO', 'RESOURCE_USAGE_SUMMARY'}:
                selected.append(index)
    selected = sorted(set(selected))
    return {'line_count': len(lines), 'lines': [{'line': index + 1, 'text': lines[index]} for index in selected[:MAX_CONTEXT_LINES]],
        'context_truncated': len(selected) > MAX_CONTEXT_LINES,
        'complete_text_included': False, 'expected_job_id': observation.get('expected_job_id'),
        'ignored_job_ids': observation.get('ignored_job_ids') or [],
        'unbound_text_attributed': False,
        'scope': 'exact-job TASK_INFO/resource fields only; unbound stdout is not solver evidence'}


def _read_owned_log(path, owned_root, launch_epoch, task=None):
    row = {'path': path, 'status': 'MISSING'}
    if not isinstance(path, str) or not path:
        row.update(status='REJECTED', reason='invalid_path')
        return row, None
    root, absolute = os.path.abspath(owned_root), os.path.abspath(path)
    row['path'] = os.path.relpath(absolute, root) if absolute.startswith(root + os.sep) else absolute
    try:
        with owned_solver_log_reader(absolute, root, launch_epoch, task=task) as (fd, metadata):
            opened = os.fstat(fd)
            data = os.read(fd, MAX_LOG_BYTES + 1)
            truncated = len(data) > MAX_LOG_BYTES or opened.st_size > MAX_LOG_BYTES
            if truncated:
                os.lseek(fd, max(0, opened.st_size - MAX_LOG_BYTES // 2), os.SEEK_SET)
                data = data[:MAX_LOG_BYTES // 2] + b'\n[bounded log head/tail; middle omitted]\n' + os.read(fd, MAX_LOG_BYTES // 2)
            final = os.fstat(fd)
            growing = (final.st_size, final.st_mtime) != (opened.st_size, opened.st_mtime)
        row.update(bytes=opened.st_size, mtime=opened.st_mtime, leaf_link=metadata['leaf_link'],
            resolved_path=os.path.relpath(metadata['resolved_source_path'], root),
            source_device=metadata['source_device'], source_inode=metadata['source_inode'],
            target_device=metadata['target_device'], target_inode=metadata['target_inode'])
        text = data.decode('utf-8', 'replace')
        row.update(status='RECORDED', fresh=True, read_truncated=truncated,
            changed_during_read=growing, hash_scope='head_tail' if truncated else 'full',
            sha256=hashlib.sha256(data).hexdigest(), context=log_context(text))
        return row, text
    except FileNotFoundError:
        return row, None
    except OwnedSolverLogError as error:
        row.update(status='REJECTED', reason=error.reason)
        if error.reason == 'stale_log':
            row['fresh'] = False
        return row, None
    except (OSError, TypeError, ValueError) as error:
        row.update(status='REJECTED', reason=type(error).__name__)
        return row, None


def collect_solver_artifacts(task, owned_root, launch_epoch, job_id=None,
                             scheduler_stdout=None, scheduler_stderr=None):
    """Snapshot known fresh artifacts without reading unowned user paths.

    Paths are explicit task-owned paths. ALPS emits <psf>.log and <psf>.warn
    beside its PSF directory; these names must not be guessed from Spectre's
    logFile. This function observes optional logs and does not certify results.
    """
    psf = task.get('psf_dir') or ''
    execution = os.path.join(os.path.dirname(psf), 'execution') if psf else ''
    paths = [('solver', task.get('simulator_log') or task.get('spectre_log')),
        ('solver_stdout', os.path.join(execution, 'simulator.stdout')),
        ('solver_stderr', os.path.join(execution, 'simulator.stderr'))]
    if psf:
        paths.append(('psf_log', os.path.join(psf, 'logFile')))
        if task.get('execution_simulator', 'spectre') == 'alps':
            paths.extend((('alps_psf_log', psf + '.log'), ('alps_psf_warning', psf + '.warn')))
    paths.extend((('scheduler_stdout', scheduler_stdout), ('scheduler_stderr', scheduler_stderr)))
    artifacts, seen = [], set()
    for kind, path in paths:
        if not path or path in seen:
            continue
        seen.add(path)
        row, text = _read_owned_log(path, owned_root, launch_epoch, task=task)
        row['kind'] = kind
        if kind.startswith('scheduler') and text is not None:
            observation = parse_scheduler_task_log(text, job_id)
            if row.get('read_truncated') or row.get('changed_during_read'):
                observation.update(valid=False, scheduler_task_exit_all_zero=False,
                    reason='partial_or_changing_scheduler_log', job_id=None)
            row['scheduler_task_observation'] = observation
            row['context'] = scheduler_log_context(text, observation)
        artifacts.append(row)
    return {'schema_version': 'crescendo.solver_artifacts.v1',
        'task_id': task.get('task_id'), 'test': task.get('test'), 'corner': task.get('corner'),
        'point_id': task.get('point_id'), 'execution_simulator': task.get('execution_simulator', 'spectre'),
        'scheduler_job_id': str(job_id) if valid_job_id(job_id) else None,
        'artifacts': artifacts, 'native_completion_authority': False,
        'remote_stop_authority': False,
        'scope': 'owned fresh solver/PSF companions and exact-job Batch scheduler TASK_INFO; receipts and native measurements qualify completion separately'}
