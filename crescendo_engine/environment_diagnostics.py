"""Small, self-contained portability reports from owned run evidence.

Reads never grant execution, completion or cleanup authority. Every source is
closed before this function returns, so archiving cannot keep its own NFS files
open. Python 3.6 is intentionally supported.
"""
from __future__ import print_function

import collections
import hashlib
import json
import os
import re
import stat
import sys
import time
import xml.etree.ElementTree as ET

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .environment_probes import _redact

MAX_FILE = 2 * 1024 * 1024
MAX_TOTAL = 1024 * 1024
MAX_FILES = 4096
MAX_ROWS = 512
SETTINGS_QUOTAS = collections.OrderedDict((
    ('setup_identity', 32), ('job_policy', 64), ('save_options', 24),
    ('corner_model_groups', 112), ('user_scripts', 32), ('analysis', 40),
    ('outputs', 48), ('global_test_variables', 80), ('sweep_runplan', 48),
    ('other_test_settings', 32)))
MAX_READ_BYTES = 32 * 1024 * 1024
COLLECTION_SECONDS = 12
ERROR = re.compile(r'\*Error\*|syntax error|\bfatal\b|\bexception\b|Traceback|'
                   r'\b(?:ERROR|SFE-\d+|SPECTRE-\d+|ASSEMBLER-5031)\b|'
                   r'undefined function|unbound variable|token.*(?:missing|not found)|'
                   r'permission denied|quota exceeded|no space left|cannot.*submit', re.I)
JSON_NAMES = frozenset((_contract_text('baseline_shutdown_roots.json'), _contract_text('raw_cleanup_proof.json'), _contract_text('native_baseline_evidence.json'), _contract_text('native_baseline_result.json'), _contract_text('native_baseline_state.json'), _contract_text('corner_identity.json'), _contract_text('preparation_failure.json'),
    _contract_text('reader_request.json'), _contract_text('reader_status.json'),
    _contract_text('portability_settings.json'), _contract_text('capture_diagnostics.json'), _contract_text('capture_generation.json'),
    _contract_text('command.json'), _contract_text('native_storage.json'), _contract_text('native_storage_readback.json'),
    _contract_text('worker_shutdown.json'), _contract_text('parameter_application.json'), _contract_text('source_guard.json'), _contract_text('native_file_inputs.json'),
    _contract_text('cleanup_failure.json'), _contract_text('cleanup_diagnostics.json'), _contract_text('scheduler_context.json'),
    _contract_text('native_submission.json'), _contract_text('point_identity.json'), _contract_text('submission.json'), _contract_text('error.json'),
    _contract_text('result.json'), _contract_text('measurement_error.json'), _contract_text('execution_diagnostics.json'),
    _contract_text('effective_job_policy.json'), _contract_text('native_storage_evidence.json'), _contract_text('worker_manifest.json'), _contract_text('guardian_state.json'),
    _contract_text('worker_state.json'), _contract_text('startup_session_ownership.json'), _contract_text('ownership.json'),
    _contract_text('persistent_command.json'), _contract_text('bootstrap_diagnostics.json'), _contract_text('compute_metadata.json'),
    _contract_text('private_netlist.json'), _contract_text('private_netlist_candidate.json'), _contract_text('netlist_generation.json'),
    _contract_text('native_test_projection.json'), _contract_text('native_capture_projection.json'), _contract_text('native_generation_required.json'),
    _contract_text('execution_pipeline.json'), _contract_text('cli_results.json'), _contract_text('completion_evidence.json'),
    _contract_text('solver_artifacts.json'), _contract_text('measurement.json'), _contract_text('result_reader_capability.json'), _contract_text('native_worker_diagnostics.json')))
TEXT_NAMES = frozenset(('native_baseline_protocol.tsv', 'native_corner_schedule.tsv', _contract_text('native_dispatch.jsonl'),
    'ocean.stdout', 'ocean.stderr', 'spectre.out', _contract_text('spectre.log'), _contract_text('Job1.log'),
    'capture.il', 'bootstrap.il', 'native_capture_ready', _contract_text('crescendo.log'),
    'effective_job_policy.tsv', 'startup.tsv', 'persistent_worker_status.tsv',
    'simulator.stdout', 'simulator.stderr', 'started.receipt', 'terminal.receipt',
    'compute_metadata.tsv', 'alps.out', _contract_text('psf.log'), 'psf.warn', 'scheduler.stdout', 'scheduler.stderr'))
from .scheduler_providers import log_artifacts as _scheduler_log_artifacts
_SCHEDULER_LOG_NAMES = tuple(os.path.basename(path) for path in
    _scheduler_log_artifacts(os.path.abspath(os.curdir)).values())
TEXT_NAMES = TEXT_NAMES.union(_SCHEDULER_LOG_NAMES)
_SCHEDULER_AUX_PATTERN = (r'^(?:' + '|'.join(re.escape(name) for name in
    sorted(set(['scheduler'] + [os.path.splitext(name)[0] for name in _SCHEDULER_LOG_NAMES]))) +
    r')_(?:query|status|cancel|help|cancel_help)(?:_[0-9]+)?\.(?:stdout|stderr)$')
POINT_READER_FILES = ('read_point_parameters.il', 'parameters.tsv', 'reader_progress.tsv',
                      _contract_text('reader_request.json'), _contract_text('reader_status.json'))


def _diagnostic_text_name(name):
    return (name in TEXT_NAMES or name.endswith((_contract_text('.psf.log'), '.psf.warn'))
            or bool(re.match(_SCHEDULER_AUX_PATTERN, name)))


def _alps_companion_name(name):
    return name in (_contract_text('psf.log'), 'psf.warn') or name.endswith((_contract_text('.psf.log'), '.psf.warn'))


def _raw_scheduler_text_name(name):
    return (name in (('scheduler.stdout', 'scheduler.stderr') + _SCHEDULER_LOG_NAMES) or
            bool(re.match(_SCHEDULER_AUX_PATTERN, name)))


def _serialized(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def package_identity():
    """Hash shipped code, never the host's tokens, user setup or old results."""
    from . import __version__
    from .runtime import contract
    application_version = contract()['identity'].get('version') or __version__
    package = os.path.dirname(os.path.abspath(__file__))
    project = os.path.dirname(package)
    digest = hashlib.sha256()
    count, size = 0, 0
    roots = [package] + [os.path.join(project, name) for name in ('cadence', 'tools')
                         if os.path.isdir(os.path.join(project, name))]
    paths = []
    for code_root in roots:
        for directory, children, files in os.walk(code_root, followlinks=False):
            children[:] = sorted(name for name in children
                                 if name not in ('__pycache__', '.git') and not os.path.islink(os.path.join(directory, name)))
            paths.extend(os.path.join(directory, name) for name in sorted(files)
                         if name.endswith(('.py', '.il', '.sh')))
    paths.extend(os.path.join(project, name) for name in ('load.il', 'start.sh')
                 if os.path.isfile(os.path.join(project, name)))
    for path in sorted(paths):
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE:
            continue
        with open(path, 'rb') as stream:
            data = stream.read(MAX_FILE + 1)
        relative = os.path.relpath(path, project)
        digest.update(relative.encode('utf-8') + b'\0' + hashlib.sha256(data).digest())
        count += 1
        size += len(data)
    result = {'application_version': application_version, 'engine_version': __version__,
              'source_tree_sha256': digest.hexdigest(), 'source_file_count': count,
              'source_bytes': size, 'source_root': project,
              'scope': _contract_text('crescendo_engine/cadence/tools Python/SKILL/shell plus load.il/start.sh; backups/cache excluded')}
    for name in (_contract_text('DELIVERY_MANIFEST.json'), _contract_text('DELIVERY_MANIFEST.txt')):
        path = os.path.join(project, name)
        if os.path.isfile(path) and not os.path.islink(path):
            try:
                data, identity = _read(path)
                manifest = json.loads(data.decode('utf-8')) if name.endswith('.json') else {}
                result['delivery_manifest'] = dict(identity, filename=name,
                    metadata=dict((key, manifest.get(key)) for key in
                                  ('version', 'source_commit', 'tag', 'release_id', 'created_at',
                                   'source_tree_sha256', 'code_sha256', 'package_sha256') if key in manifest))
            except (ValueError, OSError):
                result['delivery_manifest'] = {'filename': name, 'status': 'UNREADABLE'}
            break
    from .runtime import build_identity
    result['engine_runtime'] = build_identity()
    return result


def _read(path, maximum=None):
    from .native_log_io import native_log_read_scope
    with native_log_read_scope():
        return _read_closed(path, maximum)


def _read_closed(path, maximum=None):
    limit = MAX_FILE if maximum is None else max(1, min(MAX_FILE, int(maximum)))
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError('linked or non-regular diagnostic source')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        if (info.st_dev, info.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError('diagnostic source changed during open')
        data = stream.read(limit + 1)
        io_read_bytes = len(data)
        tail = b''
        if info.st_size > limit and _diagnostic_text_name(os.path.basename(path)):
            stream.seek(max(0, info.st_size - limit // 2))
            tail = stream.read(limit // 2)
            io_read_bytes += len(tail)
    truncated = len(data) > limit or info.st_size > limit
    data = data[:limit]
    if tail:
        data = data[:limit // 2] + b'\n[diagnostic byte gap: file head/tail included; middle omitted]\n' + tail
    return data, {'bytes': info.st_size, 'read_bytes': io_read_bytes, 'sha256': _sha(data),
                  'hash_scope': ('head_tail' if tail else 'prefix') if truncated else 'full', 'read_truncated': truncated}


def _bounded_value(value, depth=0):
    """Retain semantics and explicitly index the remainder of large structures."""
    if depth > 14:
        return {'omitted_depth': True, 'sha256': _sha(_serialized(value).encode('utf-8'))}
    if isinstance(value, dict):
        return dict((key, _bounded_value(item, depth + 1)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        rows = [_bounded_value(item, depth + 1) for item in value[:MAX_ROWS]]
        if len(value) > MAX_ROWS:
            rows.append({'rows_truncated': len(value) - MAX_ROWS, 'total_rows': len(value),
                         'sha256': _sha(_serialized(value).encode('utf-8'))})
        return rows
    if isinstance(value, str) and len(value) > 16384:
        return {'text': value[:8192] + '\n...\n' + value[-8192:],
                'text_truncated': True, 'characters': len(value),
                'sha256': _sha(value.encode('utf-8'))}
    return value


def _would_truncate(value, depth=3):
    if depth > 14:
        return True
    if isinstance(value, dict):
        return any(_would_truncate(item, depth + 1) for item in value.values())
    if isinstance(value, (list, tuple)):
        return len(value) > MAX_ROWS or any(_would_truncate(item, depth + 1) for item in value)
    return isinstance(value, str) and len(value) > 16384


def _intern_structures(value, seen=None, prefix='data'):
    """Inline each repeated settings/evidence structure once within an artifact."""
    seen = seen if seen is not None else {}
    def visit(item, path):
        if isinstance(item, (dict, list)):
            serialized = _serialized(item)
            if len(serialized) > 256:
                digest = _sha(serialized.encode('utf-8'))
                if digest in seen:
                    return {'same_as': seen[digest], 'sha256': digest}
                seen[digest] = path
            if isinstance(item, dict):
                return dict((key, visit(child, path + '/' + str(key))) for key, child in item.items())
            return [visit(child, path + '/' + str(index)) for index, child in enumerate(item)]
        return item
    return visit(value, prefix)


def _capture_setup_summary(value):
    """Collapse proven policy operations, keeping actual modes and a receipt."""
    if not (isinstance(value, dict) and value.get('policy_confirmed') is True
            and value.get('mode_confirmed') is True and value.get('readback_status') == 'collected'):
        return value
    result = dict((key, item) for key, item in value.items()
                  if item is None or isinstance(item, (str, bool, int, float)))
    records = value.get('records') or []
    result.update(successful_readback_summary=True, records_count=len(records),
                  records_sha256=_sha(_serialized(records).encode('utf-8')),
                  full_readback_path=value.get('records_path'))
    modes = [row for row in records if isinstance(row, dict) and row.get('kind') in ('MODE', 'ENV')]
    result['actual_modes'] = modes[:64]
    result['actual_modes_omitted'] = max(0, len(modes) - 64)
    expected = value.get('expected_policy_readback') or {}
    names = value.get('final_policy_names') or {}
    scopes = []
    if isinstance(expected, dict):
        fields = ('distributionmethod', 'maxjobs', 'usesameprocess', 'preemptivestart',
                  'starttimeout', 'configuretimeout', 'configuredtimeout', 'runtimeout', 'lingertimeout')
        for scope, policy in sorted(expected.items()):
            if not isinstance(policy, dict):
                continue
            scopes.append({'scope': scope, 'name': names.get(scope) if isinstance(names, dict) else None,
                           'confirmed_values': dict((key, policy[key]) for key in fields if key in policy)})
    result['policy_scopes'] = scopes[:8]
    result['policy_scopes_omitted'] = max(0, len(scopes) - 8)
    if isinstance(value.get('environment_option_observations'), list):
        result['environment_option_observations'] = value['environment_option_observations']
    result['readback_sha256'] = _sha(_serialized(value).encode('utf-8'))
    return result


def _worker_report_projection(report):
    """Keep at most two noncausal wrapper messages only after parent failure."""
    result = dict(report)
    samples = result.pop('expected_wrapper_aftermath_samples', None)
    if isinstance(samples, list):
        result['expected_wrapper_aftermath_sample_count'] = len(samples)
        if report.get('parent_capture_failed') is True:
            allowed = ('console_source', 'line', 'code', 'message', 'stage', 'primary_failure')
            result['expected_wrapper_aftermath_samples'] = [
                dict((key, row[key]) for key in allowed if key in row)
                for row in samples[:2] if isinstance(row, dict)]
            result['expected_wrapper_aftermath_samples_omitted'] = max(0, len(samples) - 2)
    return result


def _identity_coverage_summary(parsed, prepared):
    """Describe recorded point checks; absence never means zero differences."""
    rows, contracts, point_coverage = [], [], []
    def bounded(value):
        return value[:8] if isinstance(value, list) else value
    def schedule_summary(value):
        if not isinstance(value, dict):
            return None
        groups = value.get('groups') or {}
        def members(group):
            tests = group.get('test_members') or {}
            return dict((name, {'count': len(labels), 'sample': labels[:8],
                                'omitted': max(0, len(labels) - 8)})
                        for name, labels in list(tests.items())[:8] if isinstance(labels, list))
        return {'read_ok': value.get('read_ok'), 'members_state': value.get('members_state'),
                'nominal_enabled': value.get('nominal_enabled'),
                'nominal_tests': dict(list((value.get('nominal_tests') or {}).items())[:8]),
                'groups': dict((name, {'enabled': group.get('enabled'),
                    'disabled_tests': bounded(group.get('disabled_tests')), 'count': group.get('count'),
                    'test_members': members(group),
                    'member_tests_omitted': max(0, len(group.get('test_members') or {}) - 8)})
                    for name, group in list(groups.items())[:8] if isinstance(group, dict)),
                'group_count': len(groups), 'groups_omitted': max(0, len(groups) - 8)}
    failed_phases = [os.path.dirname(path) for path, value in parsed.items()
                     if path.endswith(_contract_text('capture_diagnostics.json')) and isinstance(value, dict)
                     and (value.get('skill') or {}).get('ok') is False]
    for path, document in parsed.items():
        if path.endswith(_contract_text('capture_diagnostics.json')) and isinstance(document, dict):
            points = document.get('point_coverage')
            if isinstance(points, dict):
                point_row = dict((key, bounded(points.get(key))) for key in
                    ('ok', 'expected_test_point_count', 'captured_test_point_count', 'missing_test_points',
                     'unexpected_test_points', 'duplicate_test_points', 'masked_test_points', 'rdb_path'))
                point_row['omitted_counts'] = dict((key, max(0, len(points.get(key) or []) - 8))
                    for key in ('missing_test_points', 'unexpected_test_points',
                                'duplicate_test_points', 'masked_test_points'))
                point_coverage.append(point_row)
        if not path.endswith(_contract_text('corner_identity.json')) or not isinstance(document, dict):
            continue
        coverage = document.get('native_coverage')
        if not isinstance(coverage, dict):
            continue
        contracts.append({'artifact': path, 'ok': coverage.get('ok'),
                          'schema_version': coverage.get('schema_version'),
                          'enabled_tests': coverage.get('enabled_tests'),
                          'explicit_enabled_corners': coverage.get('explicit_enabled_corners'),
                          'disabled_corners': coverage.get('disabled_corners'),
                          'duplicate_source_corners': coverage.get('duplicate_source_corners'),
                          'unexpected_tests': coverage.get('unexpected_tests'),
                          'native_group_enabled_difference': coverage.get('native_group_enabled_difference'),
                          'native_nominal_difference': coverage.get('native_nominal_difference'),
                          'native_schedule': schedule_summary(document.get('native_schedule'))})
        for test in coverage.get('test_differences') or []:
            if not isinstance(test, dict):
                continue
            expansion = test.get('corner_expansion') or {}
            failures = expansion.get('identity_failures') or []
            member_diff = test.get('native_member_difference')
            member_summary = None
            if isinstance(member_diff, dict):
                member_summary = dict((key, bounded(member_diff.get(key))) for key in
                                      ('expected', 'actual', 'missing', 'unexpected'))
                member_summary['omitted_counts'] = dict((key, max(0, len(member_diff.get(key) or []) - 8))
                    for key in ('expected', 'actual', 'missing', 'unexpected'))
            rows.append({'artifact': path, 'test': test.get('test'),
                'unverified_variable_check_count': test.get('unverified_variable_check_count'),
                'expected': bounded(test.get('expected')), 'actual': bounded(test.get('actual')),
                'mapping_evidence': test.get('mapping_evidence'), 'native_member_difference': member_summary,
                'mapping_error': test.get('mapping_error'), 'missing': bounded(test.get('missing')),
                'duplicate_points': bounded(test.get('duplicate_points')), 'point_counts': test.get('point_counts'),
                'native_to_source_corner': test.get('native_to_source_corner'),
                'variable_mismatches': bounded(test.get('variable_mismatches')),
                'execution_plan': test.get('execution_plan'),
                'unexpected': bounded(test.get('unexpected')), 'sweep_differences': bounded(test.get('sweep_differences')),
                'corner_identity_errors': expansion.get('errors') or [],
                'corner_identity_failures': failures[:8],
                'corner_identity_failures_omitted': max(0, len(failures) - 8) + expansion.get('identity_failures_omitted', 0),
                'source_model_path_errors': dict((name, contract.get('model_path_errors'))
                    for name, contract in (expansion.get('contracts') or {}).items() if contract.get('model_path_errors'))})
    qualified_phases = set(os.path.dirname(row['artifact']) for row in contracts)
    unqualified_failed_phases = sorted(set(failed_phases) - qualified_phases)
    if any(row['ok'] is False for row in contracts):
        status, reason = 'FAILED', 'producer_recorded_point_qualification_failure'
    elif not unqualified_failed_phases and contracts and rows and all(row['ok'] is True for row in contracts):
        status, reason = 'VERIFIED', 'producer_recorded_point_qualification'
    else:
        status = 'NOT_VERIFIED'
        reason = ('native_capture_failed_before_point_qualification' if unqualified_failed_phases else
                  'native_point_qualification_not_recorded' if not contracts else
                  'native_point_qualification_incomplete')
    if status == 'NOT_VERIFIED':
        observed_tests = set(row.get('test') for row in rows)
        for test in (prepared.get('setup') or {}).get('tests') or []:
            if (isinstance(test, dict) and test.get('enabled', True) and test.get('name')
                    and test['name'] not in observed_tests):
                rows.append({'test': test['name'], 'status': status, 'reason': reason,
                             'unverified_variable_check_count': None})
                observed_tests.add(test['name'])
    counts = [row.get('unverified_variable_check_count') for row in rows]
    complete_counts = bool(rows) and all(isinstance(count, int) and not isinstance(count, bool)
                                        and count >= 0 for count in counts)
    return {'status': status, 'reason': reason, 'tests': rows,
        'qualification_artifact_count': len(contracts), 'contracts': contracts[:8], 'point_coverage': point_coverage[:8],
        'unqualified_failed_phases': unqualified_failed_phases[:8],
        'unqualified_failed_phases_omitted': max(0, len(unqualified_failed_phases) - 8),
        'unverified_count': sum(counts) if complete_counts and status != 'NOT_VERIFIED' else None,
        'scope': 'producer-recorded numeric checks of this fresh point set; missing evidence is not a pass; unknown expressions remain WARN'}


def _scanned_capture_ocean_summary(command, skill):
    """Reference duplicate tails only when a clean process was fully scanned."""
    cleanup = command.get('guardian_cleanup') or {}
    proof = cleanup.get('termination_proof') if isinstance(cleanup, dict) else None
    if not (command.get('returncode') == 0 and not isinstance(command.get('returncode'), bool)
            and skill.get('process_ok') is True and skill.get('scan_complete') is True
            and skill.get('console_scope') == 'owned_full_logs' and skill.get('first_error')
            and skill.get('native_error_contexts')
            and not any(command.get(key) for key in ('timed_out', 'cancelled', 'termination_failed',
                                                     'startup_timed_out', 'progress_timed_out'))
            and isinstance(proof, dict) and proof.get('ok') is True):
        return None
    result = dict(command)
    inspected = skill.get('inspected_logs') or []
    for name in ('stdout', 'stderr', 'stdout_tail', 'stderr_tail'):
        text = command.get(name)
        source = 'ocean.stdout' if name.startswith('stdout') else 'ocean.stderr'
        if not isinstance(text, str) or source not in inspected:
            continue
        encoded = text.encode('utf-8')
        result[name] = {'bytes': len(encoded), 'sha256': _sha(encoded), 'console_source': source,
            'context_reference': 'skill.first_error and skill.native_error_contexts; same capture directory',
            'scope': 'duplicate scanned command text; original console retained separately'}
    return result


def _semantic_artifact(value):
    """Compress successful point checks to a matrix, retaining every point."""
    if isinstance(value, list):
        return [_semantic_artifact(item) for item in value]
    if not isinstance(value, dict):
        return value
    if value.get('schema_version') == _contract_text('crescendo.native_worker_diagnostics.v1'):
        value = _worker_report_projection(value)
    result = {}
    for key, item in value.items():
        if key == 'native_capture_setup' and isinstance(item, dict):
            result[key] = _capture_setup_summary(item)
            continue
        if key == 'ocean' and isinstance(item, dict) and value.get('capture_only') is True and (value.get('skill') or {}).get('ok') is True:
            result[key] = dict((name, item.get(name)) for name in
                ('argv', 'cwd', 'returncode', 'timed_out', 'cancelled', 'termination_failed', 'duration_seconds', 'guardian_cleanup'))
            result[key]['log_scope'] = 'successful capture command; expected wrapper errors summarized once below'
            continue
        if key == 'ocean' and isinstance(item, dict) and value.get('capture_only') is True:
            summary = _scanned_capture_ocean_summary(item, value.get('skill') or {})
            if summary is not None:
                result[key] = _semantic_artifact(summary)
                continue
        if key in ('stdout', 'stderr', 'stdout_tail', 'stderr_tail') and isinstance(item, str):
            result[key] = _windows(item)
        elif key == 'point_identity_checks' and isinstance(item, list):
            domains, points, failures = collections.OrderedDict(), collections.OrderedDict(), []
            for check in item:
                if not isinstance(check, dict) or not check.get('variable'):
                    failures.append(check)
                    continue
                domain = dict((name, check.get(name)) for name in
                              ('variable', 'source', 'expected_expression', 'expected_domain', 'evaluation'))
                digest = _sha(_serialized(domain).encode('utf-8'))[:16]
                domains[digest] = domain
                point_key = (check.get('point_id'), check.get('corner'))
                point = points.setdefault(point_key, {'point_id': point_key[0], 'corner': point_key[1], 'variables': {}})
                actual = check.get('actual') or {}
                point['variables'][check['variable']] = [digest, actual.get('value'), check.get('rdb_value'), check.get('ok')]
                if check.get('ok') is not True:
                    failures.append(check)
            result[key] = {'check_count': len(item), 'point_count': len(points),
                'columns': ['domain_id', 'netlist_value', 'rdb_value', 'ok'],
                'domains': [dict(domain, id=digest) for digest, domain in domains.items()],
                'points': list(points.values()), 'failed_or_unverified': failures}
        else:
            result[key] = _semantic_artifact(item)
    return result


def _template_qualification_summary(manifest):
    """Index existing native-reference proof without claiming candidate CDF."""
    qualification = manifest.get('qualification') or {}
    tasks = []
    for task in qualification.get('tasks') or []:
        bindings = task.get('symbols') or {}
        tasks.append({'task_id': task.get('task_id'), 'ok': task.get('ok'), 'scope': task.get('scope'),
            'unchanged_test': task.get('unchanged_test'), 'references': task.get('references') or [],
            'symbol_binding_count': sum(len(rows) for rows in bindings.values()),
            'symbol_bindings_sha256': _sha(_serialized(bindings).encode('utf-8'))})
    return {'schema_version': manifest.get('schema_version'), 'manifest_sha256': manifest.get('manifest_sha256'),
        'parameter_symbols': manifest.get('parameter_symbols'), 'seed_values': manifest.get('seed_values'),
        'native_reference_values': manifest.get('native_reference_values'),
        'qualification': dict((key, qualification.get(key)) for key in
            ('ok', 'scope', 'optional_range_validation_required', 'callback_legality_per_candidate')),
        'task_qualification': tasks, 'template_task_count': len(manifest.get('tasks') or []),
        'template_tasks_sha256': _sha(_serialized(manifest.get('tasks') or []).encode('utf-8')),
        'source_fingerprint': (manifest.get('source_identity') or {}).get('fingerprint'),
        'candidate_value_policy': manifest.get('candidate_value_policy'),
        'candidate_cdf_callback_verification': 'NOT_TESTED',
        'scope': 'frozen native expression mapping and baseline/low/high reference readbacks; not per-candidate CDF application proof'}


def _point_termination_qualification(task):
    """Index exact producer bindings; local submit cleanup never proves remote Stop."""
    receipt = task.get('terminal_receipt') or task.get('receipt') or {}
    raw = task.get('raw_terminal_receipt')
    raw_explicit = isinstance(raw, dict)
    raw = raw if raw_explicit else receipt
    problems = []
    if not isinstance(receipt, dict) or not isinstance(raw, dict):
        return {'ok': False, 'problems': ['terminal receipt malformed'], 'basis': None}
    for value in (receipt, raw):
        if isinstance(value.get('returncode'), bool) or value.get('returncode') != 0:
            problems.append('zero terminal exit unproven')
    if receipt.get('cleanup_proven') is not True:
        problems.append('qualified terminal cleanup unproven')
    evidence = task.get('termination_evidence') or {}
    evidence = evidence if isinstance(evidence, dict) else {}
    plan = task.get('submission_command_plan') or {}
    plan = plan if isinstance(plan, dict) else {}
    if raw.get('cleanup_proven') is True and receipt.get('raw_cleanup_proven') is not False:
        if raw_explicit and raw.get('token') != receipt.get('token'):
            problems.append('raw/effective receipt identities differ')
        if evidence.get('receipt_token') is not None and evidence.get('receipt_token') != raw.get('token'):
            problems.append('termination evidence receipt identity differs')
        return {'ok': not problems, 'problems': problems, 'basis': 'owned_compute_receipt'}
    if not raw_explicit or raw.get('cleanup_proven') is not False:
        problems.append('original raw cleanup receipt unavailable or inconsistent')
    if plan.get('remote') is not False or evidence.get('remote') is not False:
        problems.append('local guardian cannot substitute for remote task cleanup')
    if (receipt.get('cleanup_basis') != 'owned_local_guardian' or receipt.get('raw_cleanup_proven') is not False
            or evidence.get('source') != 'owned_local_guardian' or evidence.get('cleanup_proven') is not True
            or evidence.get('raw_cleanup_proven') is not False):
        problems.append('qualified owned local guardian basis unproven')
    guardian = evidence.get('guardian') or {}
    guardian = guardian if isinstance(guardian, dict) else {}
    token = raw.get('token')
    if not isinstance(token, str) or not re.match(r'^[0-9a-f]{32}$', token):
        problems.append('owned raw task token unavailable')
    elif (receipt.get('token') != token or evidence.get('receipt_token') != token
            or guardian.get('token') != _contract_text('cr_netlist_task_') + token):
        problems.append('local guardian/raw/effective token bindings differ')
    bindings = plan.get('bindings') or {}
    bindings = bindings if isinstance(bindings, dict) else {}
    if (guardian.get('argv') != plan.get('argv') or not isinstance(guardian.get('argv'), list)
            or not guardian.get('argv') or guardian.get('cwd') != bindings.get('cwd')
            or not isinstance(guardian.get('cwd'), str) or not guardian.get('cwd')):
        problems.append('local guardian command/cwd differs from submission plan')
    proof = guardian.get('termination_proof') or {}
    proof = proof if isinstance(proof, dict) else {}
    if (guardian.get('state') != 'dead' or proof.get('ok') is not True
            or proof.get('kind') != 'guardian_cleanup_proven' or proof.get('status') != 'exited'
            or proof.get('remaining_group_pids') != [] or proof.get('remaining_adopted_pids') != []):
        problems.append('local guardian full termination proof missing')
    for field in ('pid', 'pid_start_ticks', 'guardian_pid', 'guardian_start_ticks'):
        value = guardian.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            problems.append('local guardian process identity incomplete: ' + field)
    uid = guardian.get('expected_uid')
    if isinstance(uid, bool) or not isinstance(uid, int) or uid < 0:
        problems.append('local guardian UID unavailable')
    if guardian.get('returncode') != raw.get('returncode'):
        problems.append('local guardian exit differs from raw task receipt')
    if not isinstance(guardian.get('launch_boot_id'), str) or not guardian['launch_boot_id']:
        problems.append('local guardian boot identity unavailable')
    for field in ('manifest_sha256', 'state_sha256'):
        value = guardian.get(field)
        if not isinstance(value, str) or not re.match(r'^[0-9a-f]{64}$', value):
            problems.append('local guardian manifest/state identity missing: ' + field)
    return {'ok': not problems, 'problems': problems, 'basis': 'owned_local_guardian'}


def pipeline_qualification(candidate, engine, simulator):
    """Index producer proof for the shared path; never grant execution authority."""
    candidate = candidate or {}
    completion = candidate.get('completion_evidence') or {}
    pipeline = completion.get('execution_pipeline') or candidate.get('execution_pipeline') or {}
    if engine == 'maestro_baseline':
        return _baseline_pipeline_qualification(candidate, completion, pipeline, simulator)
    problems = []
    if not isinstance(pipeline, dict):
        pipeline = {}
    if pipeline.get('schema_version') != _contract_text('crescendo.execution_pipeline.v1'):
        problems.append('shared execution pipeline schema missing or mismatched')
    if pipeline.get('solver') != 'shared_cli' or pipeline.get('submission') != 'shared_local_or_command':
        problems.append('solver/submission are not the common CLI layers')
    generation = pipeline.get('netlist_generation') or completion.get('netlist_generation') or candidate.get('netlist_generation') or {}
    if not isinstance(generation, dict):
        generation = {}
    fallback = generation.get('template_fallback') or {}
    native_fallback = (engine == 'private_netlist' and isinstance(fallback, dict)
        and generation.get('requested_backend') == engine and generation.get('native_generation_required') is True
        and generation.get('template_qualified') is False
        and ((fallback.get('failure_stage') == 'native_numeric_derived_parameters'
                and bool(fallback.get('numeric_derived_parameters')))
             or (fallback.get('failure_stage') == 'native_selected_parameter_binding'
                and bool(fallback.get('selected_parameter_bindings'))))
        and fallback.get('native_generation_required') is True and fallback.get('template_qualified') is False
        and (fallback.get('source_guard') or {}).get('ok') is True
        and (fallback.get('preparation_cleanup') or {}).get('cleanup_proven') is True
        and not fallback.get('cleanup_pending') and not fallback.get('termination_failed'))
    expected_mode = 'maestro_netlist' if native_fallback else engine
    expected_source = 'maestro' if expected_mode == 'maestro_netlist' else 'template'
    if pipeline.get('netlist_source') != expected_source:
        problems.append('netlist source does not match this diagnostic case')
    if generation.get('source_mode') != expected_mode or not generation.get('private_identity'):
        problems.append('private netlist generation identity missing or mismatched')
    if generation.get('solver_invocations') != 0 or generation.get('remote_submissions') != 0:
        problems.append('netlist generation did not prove capture-only isolation')
    tasks = pipeline.get('points') or pipeline.get('tasks') or []
    if not isinstance(tasks, list) or not tasks:
        problems.append('point solver/submission command evidence missing')
        tasks = []
    if isinstance(generation.get('point_count'), bool) or generation.get('point_count') != len(tasks):
        problems.append('generated netlist points and common execution point count differ')
    identities = set()
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            problems.append('point %s command evidence malformed' % index)
            continue
        task_id = task.get('task_id')
        if not isinstance(task_id, str) or not task_id or task_id in identities:
            problems.append('point %s identity missing or duplicate' % index)
        if isinstance(task_id, str):
            identities.add(task_id)
        for field, layer in (('solver_command_plan', 'solve'), ('submission_command_plan', 'submit')):
            plan = task.get(field) or {}
            argv = plan.get('argv') if isinstance(plan, dict) else None
            if not (isinstance(plan, dict) and plan.get('schema_version') == _contract_text('crescendo.execution_command_plan.v1')
                    and plan.get('layer') == layer and isinstance(argv, list) and argv
                    and all(isinstance(arg, str) and arg for arg in argv)):
                problems.append('point %s expanded %s argv unproven' % (task_id, layer))
        termination = _point_termination_qualification(task)
        if not termination['ok']:
            problems.extend('point %s %s' % (task_id, reason) for reason in termination['problems'])
        plan_simulator = (task.get('solver_command_plan') or {}).get('execution_simulator')
        actual_simulators = {value for value in (task.get('execution_simulator'), task.get('simulator'), plan_simulator) if isinstance(value, str) and value}
        if actual_simulators != {simulator}:
            problems.append('point %s selected solver identity mismatched' % task_id)
    return {'ok': not problems, 'problems': problems, 'requested_source': engine,
        'requested_simulator': simulator, 'pipeline': dict(pipeline, netlist_generation=generation),
        'native_generation_required': bool(native_fallback), 'template_qualified': False if native_fallback else None,
        'scope': 'diagnostic index of current candidate producer proof; missing evidence is not a pass'}


def _baseline_pipeline_qualification(candidate, completion, pipeline, simulator):
    """Index native producer evidence without inventing CLI command receipts."""
    pipeline = pipeline if isinstance(pipeline, dict) else {}
    problems = []
    if pipeline.get('schema_version') != _contract_text('crescendo.maestro_baseline.v1'):
        problems.append('native Maestro execution protocol missing or mismatched')
    if pipeline.get('solver') != 'native_maestro' or pipeline.get('submission') != 'native_job_setup':
        problems.append('baseline did not use native Maestro Run and Job Setup')
    for key in ('native_run_invoked', 'native_wait_completed', 'native_session_closed', 'solver_completion_proven'):
        if pipeline.get(key) is not True:
            problems.append('native completion evidence missing: ' + key)
    if not pipeline.get('run_id') or not pipeline.get('history_name') or not pipeline.get('rdb_path'):
        problems.append('exact native run/history/RDB identity missing')
    if pipeline.get('history_name') != completion.get('history_name'):
        problems.append('native Run history differs from validated completion history')
    if completion.get('ok') is not True or completion.get('execution_simulator') != simulator:
        problems.append('native completion does not prove the selected simulator')
    if not completion.get('points') or not completion.get('simulator_runs'):
        problems.append('native point and solver completion evidence missing')
    return {'ok': not problems, 'problems': problems, 'requested_source': 'maestro_baseline',
            'requested_simulator': simulator, 'pipeline': dict(pipeline, points=completion.get('points') or []),
            'scope': 'diagnostic index of exact native Run/history proof; not lifecycle authority'}


def pipeline_summary(pipeline):
    """Keep actual commands and point identities without repeating raw results."""
    if not isinstance(pipeline, dict):
        return {'status': 'NOT_TESTED', 'reason': 'shared execution pipeline not produced'}
    if pipeline.get('schema_version') == _contract_text('crescendo.maestro_baseline.v1'):
        points = pipeline.get('points') or []
        return dict({key: pipeline.get(key) for key in ('schema_version', 'solver', 'submission', 'run_id',
                    'history_name', 'rdb_path', 'native_run_invoked', 'native_wait_completed',
                    'native_session_closed', 'solver_completion_proven')},
                    points=points[:MAX_ROWS], point_count=len(points), omitted_point_count=max(0, len(points)-MAX_ROWS))
    generation = pipeline.get('netlist_generation') or {}
    generation = generation if isinstance(generation, dict) else {}
    rows = []
    tasks = pipeline.get('points') or pipeline.get('tasks') or []
    tasks = tasks if isinstance(tasks, list) else []
    for task in tasks[:MAX_ROWS]:
        if not isinstance(task, dict):
            continue
        row = {key: task.get(key) for key in ('task_id', 'test', 'corner', 'point_id',
            'execution_simulator', 'job_id', 'scheduler_job_id', 'submission_returncode') if task.get(key) is not None}
        for field in ('solver_command_plan', 'submission_command_plan'):
            plan = task.get(field)
            if isinstance(plan, dict):
                row[field] = {key: plan.get(key) for key in ('schema_version', 'layer', 'argv', 'cwd',
                    'template', 'fingerprint', 'remote', 'resource_cpu', 'returncode', 'execution_simulator') if plan.get(key) is not None}
        receipt = task.get('receipt') or task.get('terminal_receipt')
        if isinstance(task.get('netlist_generation'), dict):
            row['netlist_generation'] = {key: value for key, value in task['netlist_generation'].items() if key in ('mode', 'source_mode', 'method', 'private_identity', 'source_identity', 'solver_invocations', 'remote_submissions')}
        if isinstance(receipt, dict):
            row['terminal_receipt'] = {key: receipt.get(key) for key in ('returncode', 'cleanup_proven',
                'compute_hostname', 'compute_uid', 'state', 'raw_cleanup_proven', 'cleanup_basis') if receipt.get(key) is not None}
        for field in ('scheduler_observation', 'solver_artifacts'):
            if isinstance(task.get(field), dict):
                row[field] = task[field]
        raw = task.get('raw_terminal_receipt')
        if isinstance(raw, dict):
            row['raw_terminal_receipt'] = {key: raw.get(key) for key in ('returncode', 'cleanup_proven',
                'compute_hostname', 'compute_uid', 'state') if raw.get(key) is not None}
        termination = task.get('termination_evidence')
        if isinstance(termination, dict):
            row['termination_evidence'] = {key: termination.get(key) for key in ('source', 'remote',
                'cleanup_proven', 'raw_cleanup_proven', 'reason') if termination.get(key) is not None}
            guardian = termination.get('guardian')
            if isinstance(guardian, dict):
                row['termination_evidence']['guardian'] = {key: guardian.get(key) for key in ('state',
                    'termination_proof', 'argv', 'cwd', 'expected_uid', 'pid', 'pid_start_ticks', 'guardian_pid',
                    'guardian_start_ticks', 'launch_boot_id', 'returncode', 'manifest_sha256', 'state_sha256') if guardian.get(key) is not None}
        rows.append(row)
    result = {key: pipeline.get(key) for key in ('schema_version', 'solver', 'submission', 'netlist_source')}
    generation = dict(generation)
    if isinstance(generation.get('template_fallback'), dict):
        fallback = dict(generation['template_fallback'])
        guard = fallback.get('source_guard')
        if isinstance(guard, dict):
            fallback['source_guard'] = {key: guard[key] for key in
                ('ok', 'state', 'fingerprint', 'schema_version', 'evidence_ref') if key in guard}
        generation['template_fallback'] = fallback
    result.update(netlist_generation={key: generation.get(key) for key in ('source_mode', 'method',
        'private_identity', 'source_identity', 'point_count', 'solver_invocations', 'remote_submissions',
        'parameter_application', 'source_guard', 'requested_backend', 'native_generation_required',
        'template_qualified', 'template_fallback') if generation.get(key) is not None},
        points=rows, point_count=len(tasks), omitted_point_count=max(0, len(tasks) - len(rows)),
        tasks_sha256=_sha(_serialized(tasks).encode('utf-8')),
        scope='expanded solve/submit commands and owned terminal receipts from this candidate only')
    return result


def _candidate_manifest_summary(document):
    if not isinstance(document, dict):
        return document
    tasks = document.get('tasks') or []
    return {'schema_version': document.get('schema_version'), 'candidate_id': document.get('candidate_id'),
        'template_sha256': document.get('template_sha256'), 'values': document.get('values'),
        'netlist_generation': document.get('netlist_generation'), 'task_count': len(tasks),
        'tasks': [{key: task.get(key) for key in ('task_id', 'test', 'corner', 'point_id', 'netlist',
            'netlist_sha256', 'execution_simulator', 'capture_source_simulator') if task.get(key) is not None}
            for task in tasks[:MAX_ROWS] if isinstance(task, dict)],
        'omitted_point_count': max(0, len(tasks) - MAX_ROWS),
        'tasks_sha256': _sha(_serialized(tasks).encode('utf-8')),
        'scope': 'private generated netlist identities; solve/submit commands are indexed in the shared pipeline'}


def _pipeline_artifact_summary(document):
    if not isinstance(document, dict):
        return document
    completion = document.get('completion') or document.get('completion_evidence') or {}
    pipeline = document.get('execution_pipeline') or (completion.get('execution_pipeline') if isinstance(completion, dict) else None)
    if document.get('schema_version') == _contract_text('crescendo.execution_pipeline.v1'):
        pipeline = document
    if pipeline:
        generation = document.get('netlist_generation') or (completion.get('netlist_generation') if isinstance(completion, dict) else None)
        if generation and not pipeline.get('netlist_generation'):
            pipeline = dict(pipeline, netlist_generation=generation)
        return pipeline_summary(pipeline)
    if 'task_results' in document:
        return {'candidate_id': document.get('candidate_id'), 'status': 'NOT_TESTED',
            'reason': 'CLI results produced without shared execution pipeline metadata',
            'task_count': len(document.get('task_results') or [])}
    return document


def _windows(text, radius=4, max_lines=240, console_source=None):
    """Include first error context, all bounded distinct errors, and final state."""
    lines = text.splitlines()
    si_log = isinstance(console_source, str) and os.path.basename(console_source) == _contract_text('si.foregnd.log')
    warning = re.compile(r'^\s*WARNING\s*\([A-Z]+-\d+\)\s*:', re.I)
    def is_error(line):
        if warning.match(line):
            # Cadence codes such as SFE-2322 occur in both warnings and errors.
            # Their explicit severity takes precedence over a code match.
            return False
        if si_log and (re.match(r'^\s*si:\s+simSetDef\(\)\.+.*\bis initialized to\b', line)
                       or re.match(r'^\s*(?:"[^"\r\n]*"\s*)+\.?\s*$', line)):
            # SI dumps policy defaults and a quoted Spectre keyword table;
            # literal "error"/"fatal" in those two formats is not a failure.
            return False
        return bool(ERROR.search(line))
    hits = [i for i, line in enumerate(lines) if is_error(line)]
    warning_hits = [i for i, line in enumerate(lines) if warning.match(line)]
    # Repeated startup messages must not spend the context budget before a
    # later, different solver/measurement failure. Keep their counts and first
    # occurrence without guessing which native errors are harmless.
    distinct = collections.OrderedDict()
    for index in hits:
        distinct.setdefault(lines[index], []).append(index)
    distinct_hits = [indices[0] for indices in distinct.values()]
    chosen_errors = distinct_hits
    error_budget = min(24, max_lines // 3)
    if len(chosen_errors) > error_budget:
        first_count = (error_budget + 1) // 2
        chosen_errors = chosen_errors[:first_count] + chosen_errors[-(error_budget - first_count):]
    selected = set(range(min(4, len(lines))))
    selected.update(warning_hits[:4])
    for index in chosen_errors:
        selected.update(range(max(0, index - radius), min(len(lines), index + radius + 1)))
        if len(selected) >= max_lines - 12:
            break
    selected.update(range(max(0, len(lines) - 12), len(lines)))
    # Batch scheduler's TASK_INFO precedes a long resource tail. Keep its exact Job ID,
    # compute node and exit evidence even when no error appears in stdout.
    from .solver_artifacts import _FIELD, _TASK_FIELDS, _RESOURCE_FIELDS
    critical = [i for i, line in enumerate(lines) if _FIELD.match(line)
        and _FIELD.match(line).group(1) in _TASK_FIELDS | _RESOURCE_FIELDS | {'TASK_INFO', 'RESOURCE_USAGE_SUMMARY'}]
    selected.update(critical)
    callback_inputs = [i for i,line in enumerate(lines) if _contract_text('CRESCENDO_CDF_CALLBACK_INPUT') in line]
    retained_callback_inputs = callback_inputs[-min(8,max_lines):]
    selected.update(retained_callback_inputs)
    native_following = []
    if isinstance(console_source, str) and os.path.basename(console_source) in ('ocean.stdout', 'ocean.stderr'):
        from .native_diagnostics import NATIVE_ERROR_FOLLOWING_LINES, MAX_NATIVE_ERROR_CONTEXTS
        native_hits = [index for index in chosen_errors if '*Error*' in lines[index]]
        native_following = list(dict.fromkeys(number for index in native_hits[:MAX_NATIVE_ERROR_CONTEXTS]
            for number in range(index + 1, min(len(lines), index + 1 + NATIVE_ERROR_FOLLOWING_LINES))))
        selected.update(native_following)
    # Native console status often has no ERROR token.  Match concrete runtime
    # events, not arbitrary job-policy settings or repetitive Loading lines.
    semantic = collections.OrderedDict()
    if isinstance(console_source, str) and (os.path.basename(console_source) in ('ocean.stdout', 'ocean.stderr')
                                           or console_source.startswith('native_storage/')):
        event = re.compile(r'\b(?:job[ _-]*id|task[ _-]*id|submission|submitted|submitting|'
            r'queue(?:d|ing)?|icrp|worker|licen[cs]e|timed?\s*out|timeout|failed|failure|'
            r'started|starting|startup)\b', re.I)
        setting = re.compile(r'^\s*(?:loading\b|(?:job[ _-]*policy|job[ _-]*limit|queue[ _-]*limit|worker[ _-]*limit|icrp[ _-]*limit)\s*[=:]\s*(?:nil|true|false|-?\d+)\s*$)', re.I)
        for i, line in enumerate(lines):
            if event.search(line) and not setting.search(line):
                semantic.setdefault(line.strip(), []).append(i)
    # Reserve first and last lines and first error before bounded optional hits.
    # A repeated semantic line is represented once with its count.
    boundary = list(range(min(4, len(lines)))) + list(range(max(0, len(lines) - 12), len(lines)))
    semantic_indices = [indices[0] for indices in semantic.values()]
    priority = list(dict.fromkeys(boundary + chosen_errors[:1] + retained_callback_inputs +
                                  chosen_errors[1:] + native_following + critical + semantic_indices))[:max_lines]
    chosen = sorted(set(priority) | set(sorted(selected - set(priority))[:max(0, max_lines - len(priority))]))
    rendered = []
    for index in chosen:
        line = lines[index]
        # Bound serialized bytes, including escaped Unicode, so a single
        # oversized native line cannot discard the entire first-cause record.
        if len(_serialized(line).encode('utf-8')) > 440:
            match = ERROR.search(line)
            start = max(0, match.start() - 60) if match else 0
            excerpt = line[start:start + 320]
            while len(_serialized(excerpt).encode('utf-8')) > 440:
                excerpt = excerpt[:max(1, len(excerpt) * 3 // 4)]
            rendered.append({'line': index + 1, 'text': excerpt, 'text_truncated': True,
                             'original_characters': len(line), 'excerpt_start': start})
        else:
            rendered.append({'line': index + 1, 'text': line})
    result = {'line_count': len(lines), 'error_count': len(hits),
            'warning_count': len(warning_hits),
            'first_warning_line': warning_hits[0] + 1 if warning_hits else None,
            'callback_input_lines_retained':len(retained_callback_inputs),
            'callback_input_lines_omitted':max(0,len(callback_inputs)-len(retained_callback_inputs)),
            'distinct_error_count': len(distinct), 'repeated_error_count': len(hits) - len(distinct),
            'first_error_line': hits[0] + 1 if hits else None,
            'lines': rendered,
            'lines_with_truncated_text': sum(1 for row in rendered if row.get('text_truncated')),
            'selected_context_truncated': len(selected) > max_lines or len(hits) > sum(1 for i in hits if i in chosen)
                or len(callback_inputs) > len(retained_callback_inputs),
            'context_truncated': len(chosen) != len(lines) or any(row.get('text_truncated') for row in rendered) or '[diagnostic byte gap:' in text,
            'complete_text_included': len(chosen) == len(lines) and not any(row.get('text_truncated') for row in rendered) and '[diagnostic byte gap:' not in text,
            'lines_total': len(lines), 'lines_retained': len(chosen),
            'lines_omitted': len(lines) - len(chosen),
            'line_count_scope': 'read_excerpt_only' if '[diagnostic byte gap:' in text else 'full_read_text',
            'context_selection': 'bounded first/last, error context, scheduler fields, callback and distinct native runtime events',
            'semantic_event_distinct': len(semantic),
            'semantic_event_repeated': sum(len(indices) - 1 for indices in semantic.values()),
            'semantic_event_omitted': sum(1 for indices in semantic.values() if indices[0] not in chosen)}
    if (isinstance(console_source, str) and os.path.basename(console_source) in ('ocean.stdout', 'ocean.stderr')
            and '[diagnostic byte gap:' not in text):
        from .native_diagnostics import cdf_callback_failures
        callbacks = cdf_callback_failures(text, console_source)
        if callbacks:
            result['cdf_callback_failures'] = callbacks
    return result


def _native_capture_cdf_failures(capture, capture_path, console_path, root, scripts):
    """Project only this console's already-read, same-directory native proof.

    The full native scanner owns physical line numbers even when the later
    collector has only a head/tail excerpt. No JSON tree search or extra read.
    """
    if not isinstance(capture, dict) or capture.get('schema_version') != _contract_text('crescendo.native_capture_diagnostics.v1') or capture.get('capture_only') is not True:
        return []
    directory, name = os.path.dirname(console_path), os.path.basename(console_path)
    script = os.path.join(directory, 'capture.il')
    if (name not in ('ocean.stdout', 'ocean.stderr') or script not in scripts
            or capture_path != os.path.join(directory, _contract_text('capture_diagnostics.json'))
            or capture.get('script_path') != os.path.join(root, script)):
        return []
    skill = capture.get('skill') or {}
    inspected = skill.get('inspected_logs') if isinstance(skill, dict) else None
    if (not isinstance(skill, dict) or skill.get('schema_version') != _contract_text('crescendo.native_skill_diagnostics.v1')
            or skill.get('console_scope') != 'owned_full_logs'
            or not isinstance(inspected, list) or len(inspected) > 2 or name not in inspected):
        return []
    rows = skill.get('cdf_callback_failures') or []
    if not isinstance(rows, list):
        return []
    result = []
    for row in rows[:8]:
        if (not isinstance(row, dict) or row.get('schema_version') != _contract_text('crescendo.cdf_callback_failure.v1')
                or row.get('capture_stage') != 'callback_failure' or row.get('console_source') != name
                or not isinstance(row.get('property'), str) or not row['property']
                or not isinstance(row.get('callback_error'), str)
                or not re.match(r'^\s*\*Error\*.*\bCDF callback failed for ' + re.escape(row['property']) + r':', row['callback_error'])):
            continue
        line, error_line = row.get('console_line'), row.get('callback_error_line')
        if (not isinstance(line, int) or isinstance(line, bool) or line < 1
                or not isinstance(error_line, int) or isinstance(error_line, bool)
                or not line < error_line <= line + 12):
            continue
        fields = row.get('expression_fields') or []
        selected = row.get('selected_parameter_names') or []
        if (not isinstance(fields, list) or len(fields) > 16
                or not isinstance(selected, list) or len(selected) > 32):
            continue
        result.append(dict(row, console_source=console_path, diagnostic_path=capture_path))
    return result


def _reader_capability_summary(document, path):
    """Index actual owned Ocean observations, never substitute parameter proof."""
    if (not isinstance(document, dict) or document.get('schema_version') != _contract_text('crescendo.result_reader_capability.v1')
            or not isinstance(document.get('task_id'), str) or not document['task_id']
            or document.get('execution_simulator') not in ('spectre', 'alps')):
        return {'path': path, 'status': 'NOT_TESTED', 'reason': 'Current reader observation identity/schema unavailable'}
    parameters = document.get('native_parameter_readback') or {}
    parameters = parameters if isinstance(parameters, dict) else {}
    outputs = document.get('output_diagnostics') or []
    outputs = [row for row in outputs if isinstance(row, dict)] if isinstance(outputs, list) else []
    parameter_ok = parameters.get('ok')
    qualified = document.get('candidate_qualification') or {}
    result = dict((key, document.get(key)) for key in
        ('task_id', 'test', 'corner', 'point_id', 'execution_simulator', 'result_reader', 'psf_dir',
         'netlist_sha256', 'open_results_status', 'analysis_names', 'analysis_inventory_status'))
    result.update({
            'path': path, 'status': 'RECORDED',
            'native_parameter_readback': dict(parameters, ok=parameter_ok),
            'output_diagnostics': outputs,
            'candidate_qualification': qualified,
            'scope': 'same current owned Ocean evaluation; opening/waveform reads do not prove candidate parameter identity'})
    return result


def _event_log_context(text):
    """Keep stage/message plus error detail from structured native Crescendo logs."""
    events, unstructured = [], []
    for line in text.splitlines():
        try:
            row = json.loads(line[line.index('{'):])
        except (ValueError, TypeError):
            unstructured.append(line)
            continue
        entry = dict((key, row.get(key)) for key in
                     ('event', 'kind', 'stage', 'message', 'error', 'failure_stage', 'level') if row.get(key) is not None)
        label = _serialized(entry)
        if not entry:
            entry = {'record_keys': sorted(row), 'record_sha256': _sha(line.encode('utf-8'))}
        if re.search(r'error|fail|mismatch|ambigu|unproven', label, re.I):
            entry['details'] = _semantic_artifact(row.get('details') or row.get('data') or row.get('payload') or {})
        events.append(entry)
    # Consecutive equal stage messages and full setup/context dumps are omitted.
    unique = []
    for event in events:
        if not unique or event != unique[-1]:
            unique.append(event)
    return {'event_count': len(events), 'events': unique[:MAX_ROWS],
            'events_truncated': max(0, len(unique) - MAX_ROWS),
            'unstructured_context': _windows('\n'.join(unstructured)) if unstructured else None}


def _protocol_context(name, text):
    rows = []
    for line in text.splitlines()[:MAX_ROWS]:
        fields = line.split('\t')
        if name in ('started.receipt', 'terminal.receipt'):
            row = {'schema': fields[0] if fields else None, 'token': '<redacted>',
                   'state': fields[2] if len(fields) > 2 else None,
                   'pid' if name == 'started.receipt' else 'returncode': fields[3] if len(fields) > 3 else None}
            if name == 'terminal.receipt' and len(fields) > 4:
                row['cleanup_proven'] = fields[4] == '1'
            index = 4 if name == 'started.receipt' else 5
            row['compute_hostname'] = fields[index] if len(fields) > index else None
            row['compute_uid'] = fields[index + 1] if len(fields) > index + 1 else None
            row['compute_identity_status'] = 'RECORDED' if len(fields) > index + 1 else 'NOT_TESTED'
        elif name in ('startup.tsv', 'persistent_worker_status.tsv'):
            row = {'schema': _contract_text('crescendo.worker_protocol.v1'), 'identity_fields_redacted': True,
                   'candidate_id': fields[3] if len(fields) > 3 else None,
                   'state': fields[4] if len(fields) > 4 else None,
                   'message': fields[6] if len(fields) > 6 else None}
        else:
            row = {'fields': fields}
        rows.append(row)
    return {'rows': rows, 'line_count': len(text.splitlines()),
            'rows_truncated': max(0, len(text.splitlines()) - MAX_ROWS)}


def _causal_summary(documents, texts, stop_proven):
    """Small causal index; records retain their native meaning and provenance."""
    counts, samples = collections.Counter(), []
    stages = {_contract_text('native_submission.json'): 'submission', _contract_text('submission.json'): 'submission',
              _contract_text('compute_metadata.json'): 'compute_context', _contract_text('measurement_error.json'): 'measurement',
              _contract_text('result_reader_capability.json'): 'result_reader'}
    for path, document in sorted(documents.items()):
        stage = stages.get(os.path.basename(path))
        if not stage or not isinstance(document, dict):
            continue
        counts[stage] += 1
        if any(sample['stage'] == stage for sample in samples):
            continue
        sample = {'stage': stage, 'path': path, 'source': 'owned native producer file'}
        sample.update({key: document.get(key) for key in ('returncode', 'job_id', 'job_ids',
            'submission_mode', 'cwd', 'compute_hostname', 'compute_uid', 'threads', 'allocated_cpu',
            'execution_simulator', 'result_reader') if document.get(key) is not None})
        presence = document.get('environment_presence')
        if isinstance(presence, dict):
            sample['environment_presence'] = {key: value for key, value in presence.items()
                if isinstance(value, bool) or value in ('set', 'empty', 'unset')}
        samples.append(sample)
        if os.path.basename(path) == _contract_text('result_reader_capability.json'):
            sample['reader_observation'] = _reader_capability_summary(document, path)
    for path, value in sorted(texts.items()):
        name = os.path.basename(path)
        if name not in ('started.receipt', 'terminal.receipt'):
            continue
        stage = 'compute_started' if name == 'started.receipt' else 'solver_terminal'
        counts[stage] += 1
        if not any(sample['stage'] == stage for sample in samples):
            samples.append({'stage': stage, 'path': path, 'source': 'owned compute receipt',
                            'receipt': _protocol_context(name, value[0])})
    return {'stage_counts': dict(counts), 'first_stage_samples': samples,
            'stop_proven': bool(stop_proven), 'missing_stages_are_not_passes': True,
            'scope': 'submission/query rc, job state, compute receipt, solver and measurement are distinct; submit node is not compute hostname'}


def _script_context(text, locations):
    lines = text.splitlines()
    selected = set(range(min(5, len(lines))))
    for index in locations:
        selected.update(range(max(0, index - 5), min(len(lines), index + 4)))
    if not locations:
        selected.update(range(max(0, len(lines) - 8), len(lines)))
    return {'line_count': len(lines), 'reported_error_lines': sorted(set(locations)),
            'lines': [{'line': i + 1, 'text': lines[i]} for i in sorted(selected)[:160]],
            'context_truncated': len(selected) > 160}


def _native_selected_rows(script):
    """Read our literal native Parameters declaration, never evaluate SKILL."""
    literal = r'"(?:\\.|[^"\\])*"'
    declaration = re.findall(_contract_text(r'^\s*crescNpRows=list\(([^\n]*)\)\s*$'), script, re.M)
    if len(declaration) != 1 or len(declaration[0]) > 512 * 1024:
        return [], 'one bounded literal native Parameters declaration unavailable'
    entry = re.compile(r'\s*list\(\s*(%s)\s+(%s)\s+(%s)\s*\)' % ((literal,) * 3))
    rows, offset = [], 0
    while declaration[0][offset:].strip():
        match = entry.match(declaration[0], offset)
        if not match or len(rows) >= 512:
            return [], 'native Parameters declaration is nonliteral or exceeds 512 entries'
        try:
            identifier, path, value = [json.loads(token) for token in match.groups()]
        except ValueError:
            return [], 'native Parameters string literal could not be decoded'
        parts = path.split('/')
        if (len(parts) != 5 or any(not part or len(part) > 256 for part in parts)
                or len(identifier) > 1024 or len(value) > 1024
                or any(ord(char) < 32 for char in identifier + path + value)):
            return [], 'native Parameters identity/value exceeds literal bounds'
        row = dict(zip(('library', 'cell', 'view', 'instance', 'property'), parts))
        row.update(parameter_id=identifier, path=path, requested=value)
        rows.append(row)
        offset = match.end()
    return rows, None


def _native_run_failure_context(script, capture, prepared, netlists,
                                indirect_locations=None, run_stack_frames=None):
    """A bounded failure-only index; source bindings and SCS are distinct proof."""
    rows, unavailable = _native_selected_rows(script)
    selected = rows[:32]
    skill = capture.get('skill') or {}
    block = (skill.get('first_error') or {}).get('last_successful_block') or skill.get('last_successful_block') or {}
    lines = script.splitlines()
    first, last = block.get('body_first_line'), block.get('body_last_line')
    block_lines = []
    if (isinstance(first, int) and isinstance(last, int) and 1 <= first <= last <= len(lines)):
        block_lines = [{'line': index + 1, 'text': lines[index]} for index in
            range(max(0, first - 2), min(last + 1, first + 30, len(lines)))]
    relevant_calls = [{'line': index + 1, 'text': line} for index, line in enumerate(lines)
        if re.match(r'^\s*(?:runId\s*=\s*maeRunSimulation\(|maeWaitUntilDone\()', line)][:4]
    phase = {'source': 'last entered generated SKILL block; not a native traceback',
             'block': block, 'lines': block_lines, 'Run_wait_calls': relevant_calls}
    if indirect_locations and run_stack_frames:
        phase.update(source='reported indirect reader location and native Run stack; payload not located',
            attribution='indirect_reader_during_native_Run', block={}, lines=[],
            reported_locations=indirect_locations[:4], native_stack=run_stack_frames[:8],
            explanation='首条错误后的调用栈包含原生 Run；报告列号与已保存脚本行不匹配，可能涉及间接读取，尚未定位具体输入字符串。')
    elif block.get('name') and block.get('name') != _contract_text('crescNetlistCaptureBodyResult'):
        # The scanner sees nested patch markers too. Their body offsets belong
        # to that other script and cannot identify a line in capture.il.
        phase.update(source='nested script marker does not identify the capture script phase',
                     block={}, lines=[], nested_marker=block)
    boundary = (skill.get('first_error') or {}).get('native_run_boundary') or {}
    if (isinstance(boundary, dict) and boundary.get('stage') in ('Run', 'Wait')
            and type(boundary.get('marker_line')) is int and boundary['marker_line'] > 0):
        phase['native_call_boundary'] = dict(boundary)
        if boundary['stage'] == 'Wait' or not phase.get('attribution'):
            phase.update(source='same-console native call marker before the first error',
                attribution='native_' + boundary['stage'] + '_boundary', block={}, lines=[],
                explanation='首错发生在原生 %s 调用窗口；内部解析字符串仅以原生实际打印内容为证据。' % boundary['stage'])
    ids = {row['parameter_id'] for row in selected}
    source_rows = [row for row in prepared.get('parameters') or [] if row.get('parameter_id') in ids]
    paths = {row.get('hierarchy_path') for row in source_rows if row.get('hierarchy_path')}
    unbound = re.search(r'unbound variable\s*-\s*([^\s;]+)', str((skill.get('first_error') or {}).get('message') or ''))
    symbol = unbound.group(1) if unbound else None
    bindings, roots, view_counts = [], [], collections.Counter()
    binding_count = 0
    for design in (prepared.get('hierarchy') or {}).get('designs') or []:
        body = design.get('hierarchy') or {}
        top = design.get('resolved_top') or body.get('resolved_top')
        if top and len(roots) < 8:
            roots.append({'design': design.get('design'), 'resolved_top': top})
        for binding in design.get('config_bindings') or body.get('config_bindings') or []:
            binding_count += 1
            view_counts[(str(binding.get('resolved_view')), str(binding.get('resolved_view_type')))] += 1
            path = binding.get('hierarchy_path')
            if (binding.get('resolved_view') == symbol or isinstance(path, str) and
                    any(candidate == path or candidate.startswith(path.rstrip('/') + '/') for candidate in paths)):
                bindings.append({key: binding.get(key) for key in ('hierarchy_path', 'owner_library',
                    'owner_cell', 'owner_view', 'bound_library', 'bound_cell', 'bound_view',
                    'resolved_library', 'resolved_cell', 'resolved_view', 'resolved_view_type', 'stop', 'scan_traversable')})
    point_rows, seen = [], set()
    from .private_netlist import _logical_statements
    for netlist in netlists:
        if netlist['sha256'] in seen:
            continue
        seen.add(netlist['sha256'])
        if len(point_rows) >= 4:
            continue
        try:
            statements = _logical_statements(netlist['text'], include_view=True)
        except (ValueError, RuntimeError) as error:
            point_rows.append({'path': netlist['path'], 'sha256': netlist['sha256'],
                'status': 'UNKNOWN', 'reason': 'SCS statements could not be indexed: ' + str(error)[:256]})
            continue
        devices = []
        for row in selected[:16]:
            matches = [statement for library, cell, statement, view in statements
                if (library, cell, view) == (row['library'], row['cell'], row['view'])
                and re.match(r'^' + re.escape(row['instance']) + r'\s*\(', statement)]
            entry = {'parameter_id': row['parameter_id'], 'path': row['path'], 'match_count': len(matches),
                     'status': 'exact_private_CV_instance_observed' if len(matches) == 1 and not netlist['read_truncated'] else 'UNKNOWN'}
            if len(matches) == 1:
                entry.update(statement=matches[0][:1600], statement_sha256=_sha(matches[0].encode('utf-8')),
                             statement_truncated=len(matches[0]) > 1600)
            if entry['status'] == 'UNKNOWN':
                entry['reason'] = 'SCS excerpt is incomplete' if netlist['read_truncated'] else 'exact private library/cell/view/instance is missing or ambiguous; no alias guessed'
            devices.append(entry)
        point_rows.append({'path': netlist['path'], 'sha256': netlist['sha256'], 'selected_devices': devices,
            'settings_reference': 'diagnostic.netlist_points: same netlist_sha256',
            'selected_devices_omitted': max(0, len(rows) - len(devices))})
    return {'label': '原生 Run 失败上下文：参数、配置绑定与已生成网表',
        'scope': 'current generated script and SCS observations only; source config is the inspected snapshot, not private binding readback; none proves native callbacks, point qualification or solver success',
        'phase': phase,
        'selected_native_parameters': selected, 'selected_count': len(rows),
        'selected_omitted': max(0, len(rows) - len(selected)), 'selected_unavailable_reason': unavailable,
        'private_config_reference': 'diagnostic.setup_private_session' if prepared.get('setup_private_session') else None,
        'source_config': {'scope': 'prepared source hierarchy snapshot', 'roots': roots,
            'binding_count': binding_count, 'related_bindings': bindings[:48],
            'related_bindings_omitted': max(0, len(bindings) - 48),
            'views': [{'view': view, 'view_type': kind, 'count': count}
                      for (view, kind), count in sorted(view_counts.items())[:64]],
            'views_omitted': max(0, len(view_counts) - 64)},
        'generated_netlists': point_rows, 'unique_netlists_omitted': max(0, len(seen) - len(point_rows)),
        'limits': {'selected_paths': 32, 'selected_devices_per_SCS': 16, 'unique_SCS': 4,
                   'statement_characters': 1600, 'related_bindings': 48,
                   'reason': 'bounded failure evidence; full capture.il and SCS remain retained'}}


def _setup_private_summary(reference):
    """Index the UI's saved native binding receipt once, without another probe."""
    fields = ('library', 'cell', 'view', 'path')
    configs = reference.get('configs') or []
    views = reference.get('views') or []
    verification = reference.get('binding_verification') or {}
    return {'label': 'Setup 私有 Maestro 与 config 绑定',
        'session_id': reference.get('session_id'), 'status': reference.get('status'),
        'suite_name': reference.get('label'), 'baseline_id': reference.get('baseline_id'),
        'selected_test': reference.get('selected_test'),
        'permission_read_only': reference.get('private_views_read_only'),
        'suite_warnings': reference.get('metadata_warnings') or [],
        'unsafe': reference.get('unsafe'), 'diagnostic_log': reference.get('diagnostic_log'),
        'maestro': {key: (reference.get('maestro') or {}).get(key) for key in fields},
        'configs': [dict({key: row.get(key) for key in fields}, top=row.get('top'),
            binding_count=len(row.get('bindings') or []),
            bindings_sha256=_sha(_serialized(row.get('bindings') or []).encode('utf-8')))
            for row in configs[:32]], 'config_count': len(configs), 'configs_omitted': max(0, len(configs)-32),
        'private_view_count': len(views), 'private_views_sha256': _sha(_serialized(views).encode('utf-8')),
        'native_binding_receipt': {key: verification.get(key) for key in
            ('ok', 'records', 'config_records', 'command_root', 'native_run_invoked', 'solver_invocations')
            if key in verification},
        'scope': 'saved Setup native binding producer receipt; no new HDB probe, run, solve or private-view cleanup'}



def _read_reported_patch(path, root, maximum):
    """Read only an explicitly reported owned script within existing caps."""
    from .maestro_progress import _owned_binary_stream
    limit = max(1, min(MAX_FILE, int(maximum)))
    with _owned_binary_stream(path, root) as stream:
        if stream is None:
            raise ValueError('reported patch is not an owned single-link regular file')
        info = os.fstat(stream.fileno())
        data = stream.read(limit + 1)
        read_bytes = len(data)
        truncated = len(data) > limit or info.st_size > limit
        if truncated:
            stream.seek(max(0, info.st_size - limit // 2))
            tail = stream.read(limit // 2)
            read_bytes += len(tail)
            data = data[:limit // 2] + b'\n[diagnostic byte gap: physical middle lines omitted]\n' + tail
        return data, {'bytes':info.st_size, 'read_bytes':read_bytes, 'sha256':_sha(data),
            'hash_scope':'head_tail' if truncated else 'full', 'read_truncated':truncated}


def _reported_patch_context(text, locations, identity, console_text):
    """Keep actual reported locations before matching candidate callback blocks."""
    lines = text.splitlines()
    truncated = identity.get('read_truncated', False)
    physical = [row['line'] for row in locations if 1 <= row['line'] <= len(lines)] if not truncated else []
    selected = set(range(min(5, len(lines))))
    physical_windows = set()
    for number in physical:
        physical_windows.update(range(max(0, number-5), min(len(lines), number+4)))
    selected.update(physical_windows)
    callback_names = set(re.findall(r'CDF callback failed for ([^:\s]+)', console_text))
    anchors = [index for index, line in enumerate(lines)
               if any('CDF callback failed for ' + name + ':' in line for name in callback_names)]
    # Property names can occur in thousands of generated callback blocks. Keep
    # a small invocation/value sketch, never the diagnostic helper scaffolding.
    invocation = re.compile(r'cdfParam.*(?:value|callback)|cdfgData\s*=|callback(?:Expr|Result)\s*=|CDF callback failed for')
    callback_candidates = []
    for index in anchors:
        for nearby in range(max(0, index-36), min(len(lines), index+3)):
            line = lines[nearby]
            if invocation.search(line) and _contract_text('crescDiag') not in line and _contract_text('CRESCENDO_CDF_CALLBACK_INPUT') not in line:
                if nearby not in callback_candidates:
                    callback_candidates.append(nearby)
    selected.update(callback_candidates[:24])
    if not physical and not anchors:
        selected.update(range(max(0,len(lines)-8),len(lines)))
    priority = list(dict.fromkeys([number-1 for number in physical] + sorted(physical_windows)))[:160]
    retained = sorted(set(priority) | set(sorted(selected-set(priority))[:max(0,160-len(priority))]))
    context = {'line_count':None if truncated else len(lines), 'reported_error_lines':physical,
        'reported_error_lines_omitted_from_excerpt':[number for number in physical if number-1 not in retained],
        'reported_error_window_lines_omitted':len(physical_windows-set(retained)),
        'reported_error_locations':locations, 'attribution':'explicit_native_load_filename',
        'line_number_scope':'head_tail_excerpt_only; physical line numbers unavailable' if truncated else 'physical_file_lines',
        'callback_context_scope':'candidate callback blocks: invocation/value lines only; property name alone does not identify the failing instance or vendor callback implementation',
        'callback_candidate_lines_omitted': max(0, len(callback_candidates)-24),
        'lines':[dict({'line':None,'excerpt_line':index+1} if truncated else {'line':index+1}, text=lines[index])
                 for index in retained],
        'context_truncated':truncated or len(selected)>160 or len(callback_candidates)>24}
    if truncated:
        context['omission_reason'] = 'source exceeds existing per-file/read budget; head/tail kept; no excerpt line promoted to a physical location'
    return context

def _error_locations(text, source):
    """Only error locations with a filename can identify a generated script.

    Library warnings and unrelated system stack frames often contain `Line N`;
    a line number alone must never become a capture.il source position.
    """
    rows = []
    for console_line, line in enumerate(text.splitlines(), 1):
        if not ERROR.search(line):
            continue
        load_location = re.search(r"\bload:\s*error while loading file\s*-\s*([\"'])(.+?)\1\s+at line\s+(\d+)", line, re.I)
        if load_location:
            rows.append({'reported_file':load_location.group(2), 'line':int(load_location.group(3)),
                'column':None, 'console_source':source, 'console_line':console_line,
                'location_kind':'native_load_file'})
            continue
        location = re.search(r'\bline\s+(\d+)\s*[,;:]?\s*(?:column\s+(\d+)\s+)?in file\s+[\"\']?(.+?)[\"\']?\s*$', line, re.I)
        if location:
            rows.append({'reported_file': location.group(3).strip('\"\''),
                'line': int(location.group(1)), 'column': int(location.group(2)) if location.group(2) else None,
                'console_source': source, 'console_line': console_line})
            continue
        location = re.search(r'([^\s\"\']+\.il)\s*:\s*(\d+)(?::(\d+))?', line, re.I)
        if location:
            rows.append({'reported_file': location.group(1), 'line': int(location.group(2)),
                'column': int(location.group(3)) if location.group(3) else None,
                'console_source': source, 'console_line': console_line})
            continue
        location = re.search(r'\bline\s+(\d+)\s*[,;:]?\s*(?:column\s+(\d+))?', line, re.I)
        if location:
            rows.append({'reported_file': None, 'line': int(location.group(1)),
                'column': int(location.group(2)) if location.group(2) else None,
                'console_source': source, 'console_line': console_line,
                'attribution': 'filename_unavailable; retained console context is authoritative'})
    return rows


def _settings_category(scope, option_hint=''):
    scope = scope.lower()
    hint = scope + '/' + option_hint.lower()
    if re.search(r'(?:pre|post|user|eval).*script', hint):
        return 'user_scripts'
    if re.search(r'/(?:vars|var|variables|variable)(?:/|$)', scope):
        return 'global_test_variables'
    if re.search(r'sweep|runplan', hint):
        return 'sweep_runplan'
    if re.search(r'corner|model|pdk|includepath|includefile|includedir', hint):
        return 'corner_model_groups'
    if re.search(r'saveoption|savedir|saveresdir|projectdir|results(?:location|dir)|'
                 r'local(?:psf|rdb)|simulationroot|rdbroot|simulationdir', hint):
        return 'save_options'
    if re.search(r'job|distribution|submit|starttimeout|configuretimeout|runtimeout|'
                 r'lingertimeout|simulationtimeout|cpu|memory|runin|parallel', hint):
        return 'job_policy'
    if re.search(r'analys', hint):
        return 'analysis'
    if re.search(r'output|expression|measurement|signal|specification', hint):
        return 'outputs'
    if re.search(r'currentmode|simulator|tooloption|design|library|cell|view', hint) or \
            scope.count('/') <= 2 or re.search(r'/tests/test$', scope):
        return 'setup_identity'
    return 'other_test_settings'


def _settings_xml(path, data=None, identity=None, selected_test=None):
    if data is None:
        data, identity = _read(path)
    if identity['read_truncated']:
        return dict(identity, schema_version=_contract_text('crescendo.current_setup_diagnostics.v1'),
                    settings_status='TRUNCATED', path=path)
    root = ET.fromstring(data)
    if selected_test:
        active = root if root.tag.rsplit('}', 1)[-1].lower() == 'active' else next(
            (child for child in root if child.tag.rsplit('}', 1)[-1].lower() == 'active'), None)
        tests = next((child for child in active if child.tag.rsplit('}', 1)[-1].lower() == 'tests'), None) if active is not None else None
        if tests is not None:
            for child in list(tests):
                if child.tag.rsplit('}', 1)[-1].lower() == 'test' and ((child.text or '').strip() or child.get('Name')) != selected_test:
                    tests.remove(child)
    from .setup_scripts import inspect_setup_script_hooks
    hook_inventory = inspect_setup_script_hooks(root)
    groups = collections.OrderedDict((key, []) for key in SETTINGS_QUOTAS)
    historical = collections.Counter()
    historical_tags = frozenset(('history', 'historyentry', 'checkpoint', 'snapshot', 'snapshots'))
    interested = re.compile(r'global|variable|designvar|sweep|runplan|corner|model|pdk|include|'
        r'currentmode|analys|simulation|simulator|tooloption|job|timeout|save|results|'
        r'output|expression|measurement|design|library|cell|view|script|^test$|^vars$|^var$', re.I)
    active_present = root.tag.rsplit('}', 1)[-1].lower() == 'active' or \
        any(child.tag.rsplit('}', 1)[-1].lower() == 'active' for child in root)
    private_name = re.compile(r'token|password|secret|credential|cookie|api[_-]?key|cds_lic_file|lm_license_file', re.I)
    def visit(node, prefix, indexed, current=True, selected=False, option_hint='', private_value=False):
        name = node.tag.rsplit('}', 1)[-1]
        here = prefix + '/' + name
        current = current and name.lower() not in historical_tags
        text = (node.text or '').strip()
        if name.lower() in ('option', 'property', 'field'):
            option_hint = node.attrib.get('name') or node.attrib.get('Name') or text
        value_name = node.attrib.get('name') or node.attrib.get('Name') or \
            (text if name.lower() in ('option', 'property', 'field', 'var', 'variable', 'designvar') else name)
        private_value = private_value or bool(private_name.search(value_name))
        selected = selected or bool(interested.search(name)) or \
            bool(interested.search(option_hint)) or node is root
        script = bool(re.match(r'^(?:pre|post|user|eval).*script', name, re.I))
        if selected and (node.attrib or text or script):
            category = _settings_category(here, option_hint)
            if current and not script:
                attributes = dict(node.attrib)
                if private_value:
                    attributes = dict((key, '<redacted>' if key.lower() in ('value', 'default', 'expression') else value)
                                      for key, value in attributes.items())
                name_only = name.lower() in ('option', 'property', 'field', 'var', 'variable', 'designvar') and \
                    not (node.attrib.get('name') or node.attrib.get('Name'))
                row = {'scope': here, 'xml_path': indexed, 'attributes': attributes,
                       'value': '<redacted>' if private_value and text and not name_only else text}
                row['category'] = category
                groups[category].append(row)
            elif not current:
                historical[category] += 1
        if script:
            return
        siblings = collections.Counter()
        for child in node:
            child_name = child.tag.rsplit('}', 1)[-1]
            siblings[child_name] += 1
            child_current = current
            if node is root and active_present and name.lower() != 'active':
                child_current = current and child_name.lower() == 'active'
            visit(child, here, indexed + '/' + child_name + '[%s]' % siblings[child_name],
                  child_current, selected and node is not root, option_hint, private_value)
    visit(root, '', '/' + root.tag.rsplit('}', 1)[-1])
    for check in hook_inventory['checks']:
        groups['user_scripts'].append({'scope': check['xml_path'], 'xml_path': check['xml_path'],
            'attributes': {}, 'value': '', 'category': 'user_scripts', 'script_state': check})
    counts = collections.Counter(dict((key, len(rows)) for key, rows in groups.items() if rows))
    if hook_inventory['hook_count']:
        counts['user_scripts'] = hook_inventory['hook_count']
    retained_counts = collections.Counter()
    retained = []
    # Reserve space per category before allowing any class to use spare rows.
    for key, quota in SETTINGS_QUOTAS.items():
        take = min(len(groups[key]), quota, max(0, MAX_ROWS - len(retained)))
        retained.extend(groups[key][:take])
        retained_counts[key] = take
    while len(retained) < MAX_ROWS:
        previous = len(retained)
        for key, rows in groups.items():
            if key == 'other_test_settings' and retained_counts[key] >= SETTINGS_QUOTAS[key]:
                continue
            if retained_counts[key] < len(rows) and len(retained) < MAX_ROWS:
                retained.append(rows[retained_counts[key]])
                retained_counts[key] += 1
        if len(retained) == previous:
            break
    lost = collections.Counter(dict((key, count - retained_counts[key])
                                    for key, count in counts.items() if count > retained_counts[key]))
    lost_fields = collections.Counter(row['category']
                                     for row in retained if _would_truncate(row))
    return dict(identity, schema_version=_contract_text('crescendo.current_setup_diagnostics.v1'),
                path=path, settings_status='PARTIAL' if lost or lost_fields else 'READ',
                settings_rows=_bounded_value(retained), row_count=sum(counts.values()),
                scope_coverage={'total_rows': dict(counts), 'omitted_rows': dict(lost),
                    'retained_rows': dict((key, value) for key, value in retained_counts.items() if value),
                    'truncated_fields_by_scope': dict(lost_fields),
                    'necessary_contract_incomplete': any(lost.get(key) or lost_fields.get(key) for key in
                        ('setup_identity', 'job_policy', 'save_options', 'user_scripts',
                         'global_test_variables', 'sweep_runplan', 'corner_model_groups'))},
                excluded_noncurrent={'reason': 'history/checkpoint/snapshot or outside current active setup; never effective configuration',
                    'omitted_rows': dict(historical), 'row_count': sum(historical.values())},
                script_hook_inventory=dict((key, value) for key, value in hook_inventory.items() if key != 'checks'),
                selection_basis='current active setup' if active_present else 'current root; history/checkpoint/snapshot excluded',
                scope='saved current setup by bounded category quota; scripts are metadata only; excluded history is count-only',
                selected_test_filter=selected_test)


def _producer_failure_index(document):
    """Retain precise producer failure metadata before bulky secondary evidence."""
    if not isinstance(document, dict) or not (document.get('error') or document.get('error_type')
            or document.get('message') or document.get('failure_stage')):
        return None
    keys = ('candidate_id', 'error', 'error_type', 'message', 'failure_stage', 'execution_started',
            'cleanup_proven', 'source_setup_path', 'source_setup_sha256', 'native_worker_diagnostics',
            'source_guard', 'native_file_inputs', 'native_run_invoked', 'prepared_freshness',
            'phase', 'native_coverage', 'native_generation_required', 'template_qualified',
            'numeric_derived_parameters', 'selected_parameter_bindings', 'reference_checks',
            'template_fallback', 'preparation_cleanup',
            'cleanup_pending', 'termination_failed')
    result = dict((key, document[key]) for key in keys if key in document)
    detail = document.get('detail')
    if isinstance(detail, dict):
        result['detail'] = dict((key, detail[key]) for key in keys if key in detail)
        checks = []
        for check in detail.get('script_checks') or []:
            if not isinstance(check, dict):
                continue
            item = dict((key, check[key]) for key in ('xml_path', 'tag', 'enabled_state', 'enabled_flags',
                'status', 'blocking', 'child_tags', 'disabled_descendants', 'payload_field_count',
                'payload_fields_omitted', 'attribute_names', 'attribute_count') if key in check)
            metadata = check.get('payload_metadata')
            allowed = ('xml_path', 'status', 'sha256', 'bytes', 'kind', 'text_included', 'file_reference')
            if isinstance(metadata, dict):
                item['payload_metadata'] = dict((key, metadata[key]) for key in allowed if key in metadata)
            elif isinstance(metadata, list):
                item['payload_metadata'] = [dict((key, row[key]) for key in allowed if key in row)
                    for row in metadata if isinstance(row, dict)]
            checks.append(item)
        if checks:
            result['detail']['script_checks'] = checks
    return result


def native_worker_reports(document):
    """Find explicit producer worker reports, without reading their log paths.

    Freshness/ownership and native failure qualification belong to the producer;
    this projection must not scan old user histories or grant that authority.
    """
    pending, inspected, seen, reports = collections.deque([(document, 0)]), 0, set(), []
    while pending and inspected < 256 and len(reports) < 8:
        value, depth = pending.popleft()
        if depth > 10 or not isinstance(value, (dict, list)) or id(value) in seen:
            continue
        seen.add(id(value))
        inspected += 1
        if isinstance(value, dict):
            if value.get('schema_version') == _contract_text('crescendo.native_worker_diagnostics.v1') and not value.get('evidence_ref') and value not in reports:
                reports.append(value)
            report = value.get('native_worker_diagnostics')
            if isinstance(report, dict) and not report.get('evidence_ref') and report not in reports:
                reports.append(report)
            preferred = ('detail', 'capture_diagnostics', 'generation', 'netlist_generation')
            children = [value[key] for key in preferred if isinstance(value.get(key), (dict, list))]
            children.extend(child for key, child in value.items() if key not in preferred
                            and key != 'native_worker_diagnostics' and isinstance(child, (dict, list)))
        else:
            children = value[:64]
        pending.extend((child, depth + 1) for child in children[:64] if isinstance(child, (dict, list)))
    return reports


def _worker_report_references(value, retained, depth=0):
    """Reference only worker reports whose complete projection was accepted."""
    if depth > 16:
        return value
    if isinstance(value, dict):
        if value.get('schema_version') == _contract_text('crescendo.native_worker_diagnostics.v1'):
            digest = _sha(_serialized(value).encode('utf-8'))
            if digest in retained:
                return {'schema_version': value['schema_version'],
                        'evidence_ref': 'diagnostic.native_worker_diagnostics', 'sha256': digest}
        result = {}
        for key, item in value.items():
            if key == 'native_worker_diagnostics' and isinstance(item, dict):
                digest = _sha(_serialized(item).encode('utf-8'))
                if digest in retained:
                    result[key] = {'evidence_ref': 'diagnostic.native_worker_diagnostics', 'sha256': digest}
                    continue
            result[key] = _worker_report_references(item, retained, depth + 1)
        return result
    if isinstance(value, list):
        return [_worker_report_references(item, retained, depth + 1) for item in value]
    return value


_INPUT_EVIDENCE_SCHEMAS = {
    _contract_text('crescendo.source_guard_diagnostic.v1'): 'source_guard',
    _contract_text('crescendo.native_file_inputs.v1'): 'native_file_inputs',
}


def _input_evidence_reports(document):
    """Find bounded producer metadata; it never grants execution authority."""
    pending, inspected, reports = collections.deque([(document, 0)]), 0, []
    while pending and inspected < 256 and len(reports) < 16:
        value, depth = pending.popleft()
        if depth > 12 or not isinstance(value, (dict, list)):
            continue
        inspected += 1
        if isinstance(value, dict):
            schema = value.get('schema_version')
            kind = _INPUT_EVIDENCE_SCHEMAS.get(schema) if isinstance(schema, str) else None
            if kind and not value.get('evidence_ref'):
                reports.append((kind, value))
                continue
            preferred = ('detail', 'source_guard', 'native_file_inputs')
            children = [value[key] for key in preferred if isinstance(value.get(key), (dict, list))]
            children.extend(child for key, child in value.items() if key not in preferred
                            and isinstance(child, (dict, list)))
        else:
            children = value[:64]
        pending.extend((child, depth + 1) for child in children[:64])
    return reports


def _input_evidence_references(value, retained, depth=0):
    """Reference only a matching complete source/file record already retained."""
    if depth > 16:
        return value
    if isinstance(value, dict):
        schema = value.get('schema_version')
        kind = _INPUT_EVIDENCE_SCHEMAS.get(schema) if isinstance(schema, str) else None
        if kind and not value.get('evidence_ref'):
            digest = _sha(_serialized(value).encode('utf-8'))
            if (kind, digest) in retained:
                result = {'schema_version': value['schema_version'],
                          'evidence_ref': 'diagnostic.' + kind, 'sha256': digest}
                for key in ('ok', 'deferred_count', 'interpretation'):
                    if key in value:
                        result[key] = value[key]
                return result
        return {key: _input_evidence_references(item, retained, depth + 1) for key, item in value.items()}
    if isinstance(value, list):
        return [_input_evidence_references(item, retained, depth + 1) for item in value]
    return value


def _netlist_identity(text):
    lines, statements, depth = text.splitlines(), [], 0
    for number, line in enumerate(lines):
        if re.match(r'^\s*(?:subckt|\.subckt)\b', line, re.I):
            depth += 1
        if re.match(r'^\s*(?:ends|\.ends)\b', line, re.I):
            depth = max(0, depth - 1)
            continue
        if depth:
            continue
        if re.match(r'^\s*(parameters\b|include\b|simulatorOptions\b)', line) or re.search(r'\btemp\s*=', line):
            statement = line
            next_line = number + 1
            while statement.rstrip().endswith('\\') and next_line < len(lines):
                statement += '\n' + lines[next_line]
                next_line += 1
            statements.append({'line': number + 1, 'text': statement})
    return {'settings': _bounded_value(statements), 'temperature_sources': [row for row in statements
            if re.search(r'\btemp\s*=', row['text'])],
        'scope': 'top-level parameters, include/model sections and simulatorOptions; interactive history headers can be nominal; independently captured points require their current capture/RDB identity, with PSF variables-file evidence only after an actual solve'}


def _open_reader_evidence(root):
    readers = []
    try:
        for name in os.listdir('/proc/self/fd'):
            try:
                target = os.readlink('/proc/self/fd/' + name)
                if target.startswith(root + os.sep):
                    readers.append({'fd': name, 'path': os.path.relpath(target, root)})
            except OSError:
                pass
    except OSError as error:
        return {'status': 'NOT_TESTED', 'reason': str(error)}
    return {'status': 'READ', 'open_run_readers': readers,
            'collector_handles_closed': True, 'scope': 'current Python process only; other process proof belongs to engine'}


def _missing_output_requests(error_text, spec):
    """Only this explicit primary preflight failure permits extra state reading."""
    try:
        error = json.loads(error_text) if isinstance(error_text, str) else error_text
    except (ValueError, TypeError):
        return []
    if not isinstance(error, dict):
        return []
    primary = error.get('first_cause') if isinstance(error.get('first_cause'), dict) else error
    if primary.get('failure_stage') != 'measurement_preflight':
        return []
    text = json.dumps(primary, ensure_ascii=False, default=str)
    requested = []
    selected = [row.get('output_key') for field in ('objectives','constraints')
                for row in (spec or {}).get(field) or [] if isinstance(row, dict)]
    for key in selected:
        if not isinstance(key, str) or '::' not in key or key in requested:
            continue
        token = 'missing_output_expression:' + key
        position = text.find(token)
        if position < 0:
            continue
        following = text[position + len(token):position + len(token) + 1]
        if following and following not in ' ;"\'\\,]}\r\n\t':
            continue
        requested.append(key)
    return requested[:32]


def _missing_output_state(data, requested, deadline=None, stop_check=None):
    """Expose saved current test/outputList literals without evaluating them.

    Uses the same direct test and outputList fields as ade_setup._state_details;
    historical subtrees are excluded explicitly. Unknown wrappers stay unknown.
    """
    from .ade_setup import _local_name, _children, _name, _output_row
    root = ET.fromstring(data)
    historical = frozenset(('history','historyentry','checkpoint','snapshot','snapshots'))
    literal_budget, inspected = [32768], [0]
    def literal(value):
        raw = str(value or '')
        take = min(1024, literal_budget[0])
        literal_budget[0] -= min(len(raw),take)
        return {'literal':raw[:take], 'characters':len(raw),
                'sha256':_sha(raw.encode('utf-8')), 'truncated':len(raw)>take}
    def walk(node, path, depth=0):
        if depth > 14:
            raise ValueError('current output state XML nesting exceeds diagnostic bound')
        counts = collections.Counter()
        for child in node:
            inspected[0] += 1
            if stop_check is not None and stop_check():
                raise ValueError('current output state inspection cancelled or out of existing budget')
            if inspected[0] > 250000 or deadline is not None and time.monotonic() >= deadline:
                raise ValueError('current output state inspection reached existing collection deadline or node bound')
            tag = _local_name(child)
            counts[tag] += 1
            child_path = path + '/' + tag + '[%d]' % counts[tag]
            field_scope = (child.attrib.get('Name') or child.attrib.get('name') or '').lower()
            if tag in historical or field_scope in historical:
                continue
            yield child, child_path
            for descendant in walk(child,child_path,depth+1):
                yield descendant
    tests = _children(root,'test')
    records = []
    for key in requested:
        test_name, output_name = key.split('::',1)
        matching = [(number,test) for number,test in enumerate(tests,1) if _name(test) == test_name]
        row = {'output_key':key,'test':test_name,'output':output_name,
               'test_match_count':len(matching),'output_match_count':0,'matches':[],
               'available_output_names':[],'status':'TEST_NOT_FOUND' if not matching else 'OUTPUT_NOT_FOUND'}
        for number,test in matching[:4]:
            test_path = '/' + _local_name(root) + '/test[%d]' % number
            for field,path in walk(test,test_path):
                field_name = field.attrib.get('Name') or field.attrib.get('name') or ''
                if _local_name(field) != 'field' or not field_name.startswith('outputList_'):
                    continue
                parsed = _output_row(field,'current active.state diagnostic')
                parsed_name = (parsed or {}).get('name')
                if parsed_name and parsed_name not in row['available_output_names'] and len(row['available_output_names']) < 64:
                    row['available_output_names'].append(str(parsed_name)[:256])
                if parsed_name != output_name:
                    continue
                row['output_match_count'] += 1
                if len(row['matches']) >= 2:
                    continue
                fields = []
                for child,child_path in walk(field,path):
                    if len(fields) >= 32:
                        break
                    name = child.attrib.get('Name') or child.attrib.get('name') or _local_name(child)
                    item = {'xml_path':child_path,'tag':_local_name(child),
                            'name':name[:256],'type':(child.attrib.get('Type') or child.attrib.get('type') or '')[:128]}
                    if not re.search(r'token|password|secret|credential|cookie|api[_-]?key|cds_lic_file|lm_license_file',name,re.I):
                        item['value'] = literal(child.text)
                    else:
                        item['value'] = {'literal':'<redacted-private-field>'}
                    fields.append(item)
                expression = (parsed or {}).get('expression')
                row['matches'].append({'xml_path':path,'slot':field_name,
                    'field_names':[item['name'] for item in fields], 'fields':fields,
                    'parsed_expression_present':isinstance(expression,str) and bool(expression.strip()),
                    'parsed_expression_literal':literal(expression) if isinstance(expression,str) else None})
        if row['output_match_count']:
            row['status'] = ('DUPLICATE_MATCHES' if row['test_match_count'] > 1 or row['output_match_count'] > 1 else
                             'EXPRESSION_LITERAL_PRESENT' if row['matches'][0]['parsed_expression_present'] else 'EXPRESSION_LITERAL_UNAVAILABLE')
        records.append(row)
    return {'root_tag':_local_name(root), 'root_child_tags':list(collections.OrderedDict.fromkeys(_local_name(child) for child in root))[:32],
            'direct_test_names':[str(_name(test) or '')[:256] for test in tests[:64]], 'requested_outputs':records,
            'literal_budget_characters':32768,'literal_characters_retained':32768-literal_budget[0],
            'scope':'current direct test/outputList literal metadata only; history excluded; no SKILL/Ocean evaluation; not a parser or simulator qualification'}


def _single_test_scope_summary(prepared, spec, parsed=None):
    """Summarize loaded identities only; never inspect other designs."""
    setup = (prepared or {}).get('setup') or {}
    spec = spec or {}
    selected = setup.get('selected_test') or spec.get('selected_test')
    loaded = setup.get('tests') or []
    source = setup.get('_source_tests') if setup.get('_source_tests') is not None else loaded
    def names(rows):
        return [str(row.get('name') or '')[:256] for row in rows[:16] if isinstance(row, dict)]
    from .corner_selection import eligible_corner_names
    try:
        eligible = eligible_corner_names(setup, selected) if selected else []
        eligible_error = None
    except (ValueError, TypeError, KeyError) as error:
        eligible, eligible_error = [], str(error)[:512]
    metrics = []
    for kind in ('objectives', 'constraints'):
        definitions = spec.get(kind) or []
        if not isinstance(definitions, list):
            continue
        for row in definitions[:32]:
            if isinstance(row, dict):
                choices = row.get('corners')
                metrics.append({'kind': kind, 'output_key': str(row.get('output_key') or '')[:256],
                                'corners': [str(x)[:256] for x in choices[:32]] if isinstance(choices, list) else None,
                                'corners_omitted': max(0, len(choices) - 32) if isinstance(choices, list) else 0})
    receipts = []
    for path, document in (parsed or {}).items():
        if path.endswith(_contract_text('corner_identity.json')) and isinstance(document, dict) and document.get('source_projection_manifest'):
            receipts.append({'path': document['source_projection_manifest'], 'source': path, 'status': 'producer_reference_only'})
            if len(receipts) == 8:
                break
        if os.path.basename(path) not in (_contract_text('native_test_projection.json'), _contract_text('native_capture_projection.json')) or not isinstance(document, dict):
            continue
        receipts.append({'path': path, 'schema_version': document.get('schema_version'),
                         'selected_test': document.get('selected_test'),
                         'removed_count': len(document.get('removed') or []),
                         'files': (document.get('files') or [])[:4]})
        if len(receipts) == 8:
            break
    return {'schema_version': _contract_text('crescendo.single_test_scope_diagnostics.v1'),
            'selected_test': selected, 'spec_selected_test': spec.get('selected_test'),
            'loaded_test_count': len(loaded), 'loaded_test_names_sample': names(loaded),
            'source_test_count': len(source), 'source_test_names_sample': names(source),
            'eligible_corners': eligible[:32], 'eligible_corner_count': len(eligible),
            'eligible_error': eligible_error, 'metrics': metrics, 'private_projection_receipts': receipts,
            'scope': 'loaded catalogue and retained producer receipts only; no unselected design or analysis inspection; diagnostic only'}


def collect_run_diagnostics(root, engine, config=None, prepared=None, spec=None, stop_proven=False,
                            error_text=None, cancel=None):
    """Return at most 1 MiB of deduplicated, inline evidence before compaction."""
    root, config, prepared, spec = os.path.abspath(root), config or {}, prepared or {}, spec or {}
    environment = dict(os.environ)
    cadence = config.get('cadence') or {}
    snapshot = cadence.get('launch_environment')
    if isinstance(snapshot, dict):
        environment.update(snapshot)
    if cadence.get('license_file'):
        environment['CDS_LIC_FILE'] = cadence['license_file']
    records, files, manifest, omitted, seen_content = [], [], [], [], set()
    excluded_non_contract = []
    selected_source = (prepared.get('setup') or {}).get('source') or {}
    declared_sdb_paths = set(os.path.abspath(selected_source[key]) for key in ('setup_db', 'maestro_sdb')
                             if selected_source.get(key))
    started = time.monotonic()
    used, read_bytes, read_files = 0, 0, 0
    retained_contracts = set()
    def stopped():
        if cancel and (cancel.is_set() if hasattr(cancel, 'is_set') else cancel()):
            return 'cancelled'
        if time.monotonic() - started >= COLLECTION_SECONDS:
            return 'collection deadline'
        if read_bytes >= MAX_READ_BYTES - 2:
            return 'total read byte budget'
        return None
    def add(kind, payload):
        nonlocal used
        safe = _redact(_bounded_value(payload), environment)
        encoded = _serialized(safe)
        size = len(encoded.encode('utf-8'))
        if kind == 'collection' and size > 65536:
            safe['files'] = safe.get('files', [])[:32]
            safe['manifest_entries_truncated'] = max(0, len(manifest) - 32)
            safe['omitted_or_truncated'] = safe.get('omitted_or_truncated', [])[:16]
            safe['omission_count'] = len(omitted)
            safe['omission_index_sha256'] = _sha(_serialized(omitted).encode('utf-8'))
            safe['status'] = 'PARTIAL'
            encoded, size = _serialized(safe), len(_serialized(safe).encode('utf-8'))
            if size > 65536:
                safe = dict((key, safe.get(key)) for key in ('status', 'bytes_inlined', 'limit_bytes',
                    'read_bytes', 'read_limit_bytes', 'files_read', 'discovered_file_count', 'deadline_seconds',
                    'missing_artifacts', 'omission_count', 'omission_index_sha256'))
                safe['omitted_or_truncated'] = [{str(key): str(item)[:256] for key, item in row.items()}
                                               for row in omitted[:8]]
                safe['collection_summary_compacted'] = True
                encoded, size = _serialized(safe), len(_serialized(safe).encode('utf-8'))
        if size > 192 * 1024 or kind != 'collection' and used + size > MAX_TOTAL - 192 * 1024:
            omitted.append({'kind': kind, 'bytes': size, 'reason': 'inline evidence byte budget',
                            'sha256': _sha(encoded.encode('utf-8'))})
            return False
        signature = _sha(encoded.encode('utf-8'))
        if signature in seen_content:
            return True
        seen_content.add(signature)
        records.append({'observation': 'diagnostic.' + kind, 'payload': safe})
        used += size
        return True
    def artifact_sections(document, relative, metadata):
        tentative = dict(shared_structures)
        semantic = _semantic_artifact(_input_evidence_references(
            _worker_report_references(document, worker_reports_seen), input_reports_seen))
        interned = _intern_structures(semantic, tentative, relative + '#data')
        if (not _would_truncate(interned)
                and len(_serialized(interned).encode('utf-8')) < 160 * 1024
                and len(_serialized(_redact(interned, environment)).encode('utf-8')) < 160 * 1024):
            retained = add('artifact', {'path': relative, 'identity': metadata, 'data': interned})
            if retained:
                shared_structures.update(tentative)
            return retained
        add('artifact_index', {'path': relative, 'identity': metadata, 'segmented': True,
            'sections': list(semantic) if isinstance(semantic, dict) else ['rows'],
            'reason': 'large semantic artifact split into inline sections; collection lists omissions'})
        complete = True
        def part(value, field):
            nonlocal complete
            reason = stopped()
            if reason:
                omitted.append({'path': relative, 'field': field, 'reason': reason})
                complete = False
                return
            trial = dict(shared_structures)
            compact = _intern_structures(value, trial, relative + '#data/' + field)
            if (_would_truncate(compact)
                    or len(_serialized(compact).encode('utf-8')) > 160 * 1024
                    or len(_serialized(_redact(compact, environment)).encode('utf-8')) > 160 * 1024):
                if isinstance(value, dict):
                    for key, child in value.items():
                        part(child, field + '/' + str(key))
                    return
                if isinstance(value, list):
                    for first in range(0, len(value), 64):
                        part(value[first:first + 64], field + '/rows[%s:%s]' % (first, min(len(value), first + 64)))
                    return
            if add('artifact_section', {'path': relative, 'field': field, 'data': compact}):
                shared_structures.update(trial)
                if _would_truncate(compact):
                    omitted.append({'path': relative, 'field': field, 'reason': 'inline semantic leaf/depth truncated'})
                    complete = False
            else:
                complete = False
        if isinstance(semantic, dict):
            for field, value in semantic.items():
                part(value, str(field))
        else:
            part(semantic, 'rows')
        return complete
    add('scope', {'engine': engine, 'requested_netlist_source': {'maestro_netlist': 'maestro', 'private_netlist': 'template', 'maestro_baseline': 'native_maestro'}.get(engine, engine),
                  'execution_pipeline_schema': _contract_text('crescendo.maestro_baseline.v1') if engine == 'maestro_baseline' else _contract_text('crescendo.execution_pipeline.v1'), 'run_root': root, 'stop_proven': stop_proven,
                  'scope': 'this fresh run only; actual netlist source is recorded in the candidate pipeline; diagnostic, never completion/stop authority'})
    setup_private = prepared.get('setup_private_session') or {}
    if isinstance(setup_private, dict) and setup_private.get('schema_version') == _contract_text('crescendo.setup_private_session.v1'):
        add('setup_private_session', _setup_private_summary(setup_private))
        if prepared.get('suite_update'):
            add('private_suite_update', prepared['suite_update'])
    if os.path.islink(root):
        add('collection', {'status': 'FAIL', 'reason': 'linked run root rejected'})
        return records
    requested_missing = _missing_output_requests(error_text, spec)
    selected_test = (prepared.get('setup') or {}).get('selected_test')
    if selected_test:
        requested_missing = [key for key in requested_missing if key.rsplit('::', 1)[0] == selected_test]
    if requested_missing:
        state_path = selected_source.get('active_state')
        targeted = {'schema_version':_contract_text('crescendo.missing_output_source.v1'),'path':state_path,
                    'requested_output_keys':requested_missing,'trigger':'measurement_preflight/missing_output_expression'}
        reason = stopped()
        if not isinstance(state_path,str) or not state_path:
            targeted.update(status='NOT_CONFIGURED',reason='current source.active_state path unavailable; no location guessed')
        elif reason:
            targeted.update(status='NOT_READ',reason=reason)
        else:
            try:
                state_data,state_identity = _read(state_path, maximum=min(MAX_FILE,max(1,(MAX_READ_BYTES-read_bytes-2)//2)))
                read_bytes += state_identity['read_bytes']
                read_files += 1
                manifest.append(dict(state_identity,path=state_path,source='explicit current active.state'))
                targeted['identity'] = state_identity
                if state_identity.get('read_truncated'):
                    targeted.update(status='TRUNCATED',reason='state exceeds remaining existing read budget; XML not partially parsed')
                else:
                    targeted.update(_missing_output_state(state_data,requested_missing,deadline=started+COLLECTION_SECONDS,stop_check=stopped),status='READ')
            except (OSError, ValueError, ET.ParseError) as error:
                targeted.update(status='MISSING' if isinstance(error,OSError) and getattr(error,'errno',None) == 2 else 'UNREADABLE_OR_UNSUPPORTED_XML',
                                reason='%s: %s' % (type(error).__name__,error))
        add('missing_output_source',targeted)
    for directory, children, names in os.walk(root, followlinks=False):
        if stopped():
            omitted.append({'reason': stopped(), 'phase': 'file discovery'})
            break
        children[:] = sorted(name for name in children if not os.path.islink(os.path.join(directory, name))
                             and name not in ('raw', '__pycache__'))
        if os.path.basename(directory) == 'native_point_parameters':
            add('point_parameter_reader_files', {
                'path': os.path.relpath(directory, root), 'diagnostic_only': True,
                'files': [{'name': name, 'status': 'present' if name in names else 'not_produced'}
                          for name in POINT_READER_FILES],
                'scope': 'directory inventory only; presence is not successful readback'})
        for name in sorted(names):
            path = os.path.join(directory, name)
            if (name in JSON_NAMES or _diagnostic_text_name(name) or name in (_contract_text('input.scs'), 'maestro.sdb') or name.startswith('capture.') or re.match(r'^retained_spectre_.*\.log$', name)
                    or (name in POINT_READER_FILES and os.path.basename(directory) == 'native_point_parameters')
                    or (name in ('startup.il', 'run.il', 'finish.il', 'stop.il') and os.path.basename(directory) == 'native_baseline')):
                files.append(path)
            if len(files) >= MAX_FILES or stopped():
                omitted.append({'reason': 'scan capacity/deadline', 'directory': os.path.relpath(directory, root)})
                break
        if omitted and omitted[-1].get('reason') == 'scan capacity/deadline':
            break
    # Exact producer causes are small and must precede large setup/history
    # artifacts when the existing read or inline budget is exhausted.
    first_names = {_contract_text('preparation_failure.json'): 0, _contract_text('native_worker_diagnostics.json'): 1,
                   _contract_text('capture_diagnostics.json'): 2, _contract_text('execution_diagnostics.json'): 3,
                   _contract_text('source_guard.json'): 4, _contract_text('native_file_inputs.json'): 5,
                   _contract_text('error.json'): 6, _contract_text('measurement_error.json'): 7,
                   _contract_text('reader_status.json'): 6, _contract_text('reader_request.json'): 7,
                   'reader_progress.tsv': 7, 'read_point_parameters.il': 7, 'parameters.tsv': 7,
                   'ocean.stdout': 8, 'ocean.stderr': 9}
    files.sort(key=lambda path: first_names.get(os.path.basename(path), 10))
    # Native storage belongs to this candidate only when its producer binding
    # validates. Charge every collector read to the same global budgets.
    if engine == 'maestro_baseline':
        from .native_diagnostic_evidence import collect_native_storage_evidence
        native_byte_limit, native_file_limit = 4 * 1024 * 1024, 64
        native_read_bytes, native_read_files, native_inline_used = 0, 0, 0
        native_inline_limit = 128 * 1024
        native_deadline = min(started + COLLECTION_SECONDS, time.monotonic() + 2.0)
        def native_add(kind, payload):
            nonlocal native_inline_used
            size = len(_serialized(_redact(_bounded_value(payload), environment)).encode('utf-8'))
            if native_inline_used + size > native_inline_limit:
                return False
            if not add(kind, payload):
                return False
            native_inline_used += size
            return True
        states = [path for path in files if os.path.basename(path) == _contract_text('native_baseline_state.json')
                  and os.path.basename(os.path.dirname(path)) == 'native_baseline']
        for state_path in states:
            candidate_root = os.path.dirname(os.path.dirname(state_path))
            relative_candidate = os.path.relpath(candidate_root, root)
            if (stopped() or read_files >= MAX_FILES or native_read_files >= native_file_limit
                    or native_read_bytes >= native_byte_limit or time.monotonic() >= native_deadline):
                omitted.append({'path': relative_candidate, 'reason': stopped() or 'native shared sub-budget exhausted',
                                'phase': 'native storage collection'})
                break
            byte_allowance = min(native_byte_limit - native_read_bytes, MAX_READ_BYTES - read_bytes - 2)
            file_allowance = min(native_file_limit - native_read_files, MAX_FILES - read_files)
            try:
                result = collect_native_storage_evidence(candidate_root,
                    max(0, byte_allowance), max(0, file_allowance),
                    native_deadline, cancelled=lambda: bool(stopped()))
            except Exception as error:
                # Charge the allowance when actual usage is unknown; leave the
                # primary evidence reserve available for subsequent reads.
                omitted.append({'path': relative_candidate, 'phase': 'native storage collection',
                    'reason': 'collector failed; read usage unknown: %s: %s' % (type(error).__name__, error)})
                read_bytes += byte_allowance
                read_files += file_allowance
                break
            native_read_bytes += result.get('read_bytes', 0)
            native_read_files += result.get('files_read', 0)
            read_bytes += result.get('read_bytes', 0)
            read_files += result.get('files_read', 0)
            if not native_add('native_storage_collection', {'candidate': relative_candidate,
                'status': result.get('status'), 'manifest': result.get('manifest'),
                'files_read': result.get('files_read'), 'read_bytes': result.get('read_bytes'),
                'diagnostic_only': True, 'scope': 'candidate-bound read only; no completion or stop authority'}):
                omitted.append({'path': relative_candidate, 'reason': 'native inline sub-budget',
                                'phase': 'native storage collection'})
            for entry in result.get('records') or []:
                if time.monotonic() >= native_deadline:
                    omitted.append({'path': relative_candidate, 'reason': 'native collection time sub-budget',
                                    'phase': 'native storage processing'})
                    break
                record = dict(entry)
                content = record.pop('text', None)
                samples = record.pop('samples', None)
                if samples is not None:
                    contexts = []
                    for sample in samples:
                        context = _windows(sample.get('text') or '', max_lines=80,
                                           console_source=record.get('path'))
                        if record.get('truncated') or len(samples) > 1:
                            context['line_count_scope'] = 'read_excerpt_only'
                            context['complete_text_included'] = False
                            context['context_truncated'] = True
                        contexts.append({'offset': sample.get('offset'), 'length': sample.get('length'),
                                         'context': context})
                    record['sample_contexts'] = contexts
                elif content is not None:
                    context = _windows(content, console_source=record.get('path'))
                    if record.get('truncated'):
                        context['line_count_scope'] = 'read_excerpt_only'
                        context['complete_text_included'] = False
                        context['context_truncated'] = True
                    record['context'] = context
                record['candidate'] = relative_candidate
                record['diagnostic_only'] = True
                if not native_add('native_storage_evidence', record):
                    omitted.append({'path': record.get('path'), 'reason': 'inline budget',
                                    'phase': 'native storage collection'})
            for omission in result.get('omissions') or []:
                omitted.append(dict(omission, candidate=relative_candidate, phase='native storage collection'))
            if result.get('status') != 'ok' and not result.get('omissions'):
                omitted.append({'path': relative_candidate, 'reason': 'native storage ' + str(result.get('status')),
                                'phase': 'native storage collection'})
    parsed, texts, scripts, netlist_rows, capture_invocations = {}, {}, [], [], []
    native_captures, worker_reports_seen = {}, set()
    input_reports_seen = set()
    failure_netlists = []
    shared_structures = {}
    deferred_artifacts = []
    for file_index, path in enumerate(files):
        reason = stopped() or ('total file budget' if read_files >= MAX_FILES else None)
        if reason:
            unread = [os.path.relpath(item, root) for item in files[file_index:]]
            omitted.append({'reason': reason, 'phase': 'file reading', 'unread_file_count': len(unread),
                            'unread_files': unread[:32], 'unread_files_omitted': max(0, len(unread) - 32)})
            break
        relative, name = os.path.relpath(path, root), os.path.basename(path)
        try:
            if _alps_companion_name(name):
                # Optional raw sidecars must not bypass the producer's fresh
                # artifact contract. An already-retired point uses its saved
                # solver_artifacts excerpt instead of an unbound raw file.
                started_path = os.path.join(os.path.dirname(path), 'execution', 'started.receipt')
                try:
                    marker = os.lstat(started_path)
                except OSError:
                    omitted.append({'path': relative, 'reason': 'ALPS companion freshness NOT_TESTED; owned start receipt unavailable'})
                    continue
                if (not stat.S_ISREG(marker.st_mode) or marker.st_nlink != 1
                        or os.path.realpath(started_path) != os.path.abspath(started_path)
                        or os.lstat(path).st_mtime + 2 < marker.st_mtime):
                    omitted.append({'path': relative, 'reason': 'stale or unbound ALPS companion rejected; saved producer excerpt retained separately'})
                    continue
            data, metadata = _read(path, maximum=min(MAX_FILE, max(1, (MAX_READ_BYTES - read_bytes - 2) // 2)))
            read_bytes += metadata['read_bytes']
            read_files += 1
            manifest.append(dict(metadata, path=relative))
            if metadata.get('read_truncated') and name not in JSON_NAMES:
                omitted.append({'path': relative, 'reason': 'bounded read retained only file head/tail or prefix',
                                'hash_scope': metadata.get('hash_scope')})
            text = data.decode('utf-8', 'replace')
            if name in JSON_NAMES:
                if metadata.get('read_truncated'):
                    # A bounded prefix is not a malformed complete JSON file.
                    # Retain its identity and the partial collection result,
                    # without parsing or acquiring completion/cleanup authority.
                    omission = {'path': relative, 'reason': 'structured JSON exceeds bounded read; prefix not parsed',
                                'status': 'STRUCTURED_JSON_NOT_PARSED'}
                    omitted.append(omission)
                    add('artifact_index', dict(omission, identity=metadata,
                        scope='bounded diagnostic omission; no fields or lifecycle state inferred from the prefix'))
                    continue
                document = json.loads(text)
                parsed[relative] = document
                if name in (_contract_text('native_baseline_state.json'), _contract_text('native_storage_readback.json')) and isinstance(document, dict):
                    add('native_baseline_runtime', {'path': relative, 'identity': metadata,
                        'operation_budgets': document.get('operation_budgets'),
                        'native_storage_readback': document if name == _contract_text('native_storage_readback.json') else None,
                        'diagnostic_only': True,
                        'scope': 'producer timing and storage readback; not completion or stop proof'})
                for report in native_worker_reports(document):
                    digest = _sha(_serialized(report).encode('utf-8'))
                    if digest not in worker_reports_seen:
                        if add('native_worker_diagnostics', {'path': relative, 'identity': metadata,
                            'native_worker_diagnostics': _worker_report_projection(report),
                            'scope': 'current owned producer diagnostic projection; worker paths are not independently read or qualified here'}):
                            worker_reports_seen.add(digest)
                for kind, report in _input_evidence_reports(document):
                    digest = _sha(_serialized(report).encode('utf-8'))
                    if (kind, digest) not in input_reports_seen:
                        payload = {'path': relative, 'identity': metadata, 'sha256': digest,
                            kind: report, 'qualification': 'diagnostic_only',
                            'scope': 'producer-recorded source/file metadata; not native execution, solver or completion proof'}
                        if not _would_truncate(payload, depth=0) and add(kind, payload):
                            input_reports_seen.add((kind, digest))
                if name == _contract_text('capture_diagnostics.json') and not metadata.get('read_truncated'):
                    native_captures[relative] = (document, metadata)
                    skill = document.get('skill') if isinstance(document, dict) else None
                    if isinstance(skill, dict) and skill.get('ok') is False:
                        errors = [dict((key, item.get(key)) for key in
                                  ('message', 'console_source', 'console_line', 'native_run_boundary') if key in item)
                                  for item in (skill.get('errors') or [])[:32] if isinstance(item, dict)]
                        add('native_stage_evidence', {'path': relative, 'identity': metadata,
                            'skill': {'errors': errors, 'native_run_evidence': (skill.get('native_run_evidence') or [])[:24],
                                      'error_order_scope': skill.get('error_order_scope')}})
                if name in first_names:
                    failure_index = _producer_failure_index(_input_evidence_references(
                        _worker_report_references(document, worker_reports_seen), input_reports_seen))
                    if failure_index:
                        add('producer_failure', {'path': relative, 'identity': metadata, 'data': failure_index})
                if name == _contract_text('result_reader_capability.json'):
                    add('result_reader_capability', _reader_capability_summary(document, relative))
                if name == _contract_text('corner_identity.json') and isinstance(document, dict):
                    for field in ('saved_source', 'private_setup'):
                        value = document.get(field) or {}
                        if isinstance(value, dict) and value.get('path'):
                            declared_sdb_paths.add(os.path.abspath(value['path']))
                capture = parsed.get(os.path.join(os.path.dirname(relative), _contract_text('capture_diagnostics.json'))) or {}
                projection = document
                if name == _contract_text('private_netlist_candidate.json'):
                    projection = _candidate_manifest_summary(document)
                if name in (_contract_text('execution_pipeline.json'), _contract_text('cli_results.json'), _contract_text('completion_evidence.json')):
                    projection = _pipeline_artifact_summary(document)
                if name == _contract_text('private_netlist.json') and isinstance(document, dict):
                    projection = _template_qualification_summary(document)
                if name == _contract_text('command.json') and capture.get('capture_only') is True and (capture.get('skill') or {}).get('ok') is True:
                    projection = dict((key, document.get(key)) for key in
                        ('argv', 'cwd', 'returncode', 'timed_out', 'cancelled', 'termination_failed', 'duration_seconds', 'guardian_cleanup'))
                    projection['capture_stdout_scope'] = 'expected wrapper failure; first successful capture Ocean context is inline'
                if name == _contract_text('corner_identity.json') or metadata['bytes'] > 160 * 1024:
                    # Expand large artifacts after owned logs and scripts.
                    deferred_artifacts.append((projection, relative, metadata))
                else:
                    artifact_sections(projection, relative, metadata)
            elif name == 'maestro.sdb':
                result_placeholder = metadata['bytes'] == 0 and '/simulation/' in '/' + relative and \
                    os.path.isdir(os.path.join(os.path.dirname(path), 'results', 'maestro')) and \
                    os.path.abspath(path) not in declared_sdb_paths
                if result_placeholder:
                    excluded_non_contract.append({'path': relative,
                        'reason': 'zero-byte native simulation result placeholder; not declared source/private SetupDB'})
                else:
                    add('settings', dict(_settings_xml(path, data, metadata), path=relative))
            elif name == _contract_text('input.scs'):
                netlist_rows.append(dict(_netlist_identity(text), path=relative, sha256=metadata['sha256'],
                                         read_truncated=metadata['read_truncated']))
                if any(isinstance(value, dict) and value.get('capture_only') is True
                       and (value.get('skill') or {}).get('ok') is False for value, _identity in native_captures.values()):
                    failure_netlists.append({'path': relative, 'text': text, 'sha256': metadata['sha256'],
                                             'read_truncated': metadata['read_truncated']})
            elif name.startswith('capture.') and name != 'capture.il':
                fields = [value.decode('utf-8', 'replace') for value in data.split(b'\0') if value]
                capture_invocations.append({'path': relative, 'cwd': fields[0] if fields else None,
                                            'argv': fields[1:], 'identity': metadata})
            elif name in POINT_READER_FILES and os.path.basename(os.path.dirname(path)) == 'native_point_parameters':
                context = _windows(text, console_source=relative)
                if not metadata.get('read_truncated') and len(data) <= 65536 and len(text.splitlines()) <= 256:
                    context = {'lines': [{'line': i + 1, 'text': line} for i, line in enumerate(text.splitlines())],
                               'line_count': len(text.splitlines()), 'complete_text_included': True,
                               'context_truncated': False}
                add('point_parameter_reader', {'path': relative, 'identity': metadata,
                    'context': context, 'diagnostic_only': True})
            elif name in ('capture.il', 'bootstrap.il') or (name in ('startup.il', 'run.il', 'finish.il', 'stop.il') and os.path.basename(os.path.dirname(path)) == 'native_baseline'):
                scripts.append((relative, text, metadata))
            else:
                texts[relative] = (text, metadata)
        except Exception as error:
            omitted.append({'path': relative, 'reason': '%s: %s' % (type(error).__name__, error)})
    capture_only = bool(capture_invocations or any('capture_only' in value[0]
                        for path, value in texts.items() if path.endswith('native_capture_ready')) or
                        any(isinstance(value, dict) and value.get('capture_only') is True for value in parsed.values()))
    add('single_test_scope', _single_test_scope_summary(prepared, spec, parsed))
    add('identity_coverage', _identity_coverage_summary(parsed, prepared))
    invocation_groups = collections.OrderedDict()
    for invocation in capture_invocations:
        phase = invocation['path'].split('/invocations/')[0]
        group = invocation_groups.setdefault(phase, {'phase': phase, 'invocation_count': 0, 'sample': invocation})
        group['invocation_count'] += 1
    add('capture_semantics', {'capture_only_detected': capture_only,
        'expected_wrapper_exit': 1 if capture_only else None,
        'meaning': 'capture-only wrapper records argv and intentionally returns nonzero; ASSEMBLER-5031 alone is not solver/OOM proof'
                   if capture_only else 'capture-only execution not proven; simulator failures require invocation/solver evidence',
        'invocation_count': len(capture_invocations), 'phase_samples': list(invocation_groups.values()),
        'identity_scope': 'complete captured point identity and netlist settings are indexed separately; each phase argv sample is inline'})
    locations = []
    unexpected_error = any(isinstance(value, dict) and value.get('skill', {}).get('ok') is False for value in parsed.values())
    successful_capture_phases = set(os.path.dirname(path) for path, value in parsed.items()
        if path.endswith(_contract_text('capture_diagnostics.json')) and isinstance(value, dict)
        and value.get('capture_only') is True and (value.get('skill') or {}).get('ok') is True)
    expected_capture_context_recorded = False
    read_scripts = set(row[0] for row in scripts)
    for relative, (text, metadata) in texts.items():
        if stopped():
            omitted.append({'reason': stopped(), 'phase': 'text processing'})
            break
        if os.path.basename(relative) in ('ocean.stdout', 'ocean.stderr'):
            capture_path = os.path.join(os.path.dirname(relative), _contract_text('capture_diagnostics.json'))
            capture, capture_identity = native_captures.get(capture_path, ({}, {}))
            callbacks = _native_capture_cdf_failures(capture, capture_path, relative, root, read_scripts)
            native_scan = bool(callbacks)
            if not callbacks and not metadata.get('read_truncated'):
                from .native_diagnostics import cdf_callback_failures
                callbacks = cdf_callback_failures(text, relative)
            if callbacks:
                add('cdf_callback_failure', {'path': relative, 'identity': metadata,
                    'cdf_callback_failures': callbacks,
                    'cdf_callback_failures_truncated': len(callbacks) >= 8 or (
                        (capture.get('skill') or {}).get('cdf_callback_failures_truncated', False)
                        if native_scan else metadata.get('read_truncated', False)),
                    'native_full_console_scan': native_scan,
                    'producer': {'path': capture_path, 'identity': capture_identity} if native_scan else None,
                    'scope': 'current owned Ocean console only; failure context is not parameter or netlist qualification'})
        if os.path.basename(relative) == 'native_corner_schedule.tsv':
            add('native_schedule', {'path': relative, 'identity': metadata,
                                    'rows': text.splitlines()[:MAX_ROWS],
                                    'rows_truncated': max(0, len(text.splitlines()) - MAX_ROWS)})
        elif os.path.basename(relative) == _contract_text('native_dispatch.jsonl'):
            rows = []
            for line in text.splitlines()[:MAX_ROWS]:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    rows.append({'parse_status': 'INVALID', 'text': line})
            add('native_dispatch', {'path': relative, 'identity': metadata, 'rows': rows,
                                    'rows_truncated': max(0, len(text.splitlines()) - MAX_ROWS)})
        elif os.path.basename(relative) == _contract_text('crescendo.log'):
            add('log_context', {'path': relative, 'identity': metadata, 'context': _event_log_context(text)})
        elif os.path.basename(relative) in ('effective_job_policy.tsv', 'startup.tsv', 'persistent_worker_status.tsv',
                                           'started.receipt', 'terminal.receipt', 'compute_metadata.tsv'):
            add('protocol', {'path': relative, 'identity': metadata, 'context': _protocol_context(os.path.basename(relative), text)})
        elif _raw_scheduler_text_name(os.path.basename(relative)):
            # Shared scheduler stdout can contain older JOB_ID blocks. Its raw
            # text is a diagnostic appendix, never a current-job/runtime cause.
            # The producer's solver_artifacts retains the selected owned ID.
            add('log_context', {'path': relative, 'identity': metadata,
                'attribution': 'unbound_raw_scheduler_text', 'current_job_authority': False,
                'scope': 'raw appendix only; current JobID/exit context comes from producer solver_artifacts',
                'context': _windows(text, console_source=relative)})
            continue
        elif os.path.basename(relative) not in ('native_capture_ready',):
            successful_capture_log = os.path.dirname(relative) in successful_capture_phases and 'ocean' in os.path.basename(relative)
            if successful_capture_log:
                if expected_capture_context_recorded:
                    add('log_context', {'path': relative, 'identity': metadata,
                        'capture_wrapper_failures_expected': True,
                        'context': {'source': 'first successful capture Ocean context already inlined',
                                    'skill_ok': True, 'real_spectre_execution_started': False}})
                else:
                    add('log_context', {'path': relative, 'identity': metadata,
                        'capture_wrapper_failures_expected': True, 'context': _windows(text, radius=1, max_lines=48, console_source=relative)})
                    expected_capture_context_recorded = True
                continue
            from .solver_artifacts import log_context
            failed_capture = parsed.get(os.path.join(os.path.dirname(relative), _contract_text('capture_diagnostics.json'))) or {}
            add('log_context', {'path': relative, 'identity': metadata,
                'capture_wrapper_failures_expected': capture_only and 'ocean' in relative.lower(),
                'context': log_context(text) if os.path.basename(relative).endswith('.warn') else
                    _windows(text, radius=8 if 'ocean' in os.path.basename(relative) and
                             (failed_capture.get('skill') or {}).get('ok') is False else 4,
                             console_source=relative)})
        if os.path.basename(relative) != _contract_text('crescendo.log'):
            locations.extend(_error_locations(text, relative))
        if os.path.basename(relative) != _contract_text('crescendo.log') and any(ERROR.search(line) and 'ASSEMBLER-5031' not in line for line in text.splitlines()):
            unexpected_error = True
    # A Cadence load failure can name a parameter patch instead of capture.il.
    # Read only that owned reported patch; all ordinary patches remain unscanned.
    reported_patches = collections.OrderedDict()
    for location in locations:
        filename = location.get('reported_file')
        if not filename or os.path.basename(filename) != 'patch.il':
            continue
        if not os.path.isabs(filename):
            omitted.append({'path':filename,'reason':'reported relative patch filename has no proven cwd; not read'})
            continue
        path = os.path.abspath(filename)
        if not path.startswith(root+os.sep) or os.path.realpath(path) != path:
            omitted.append({'path':filename,'reason':'reported patch outside owned root or linked; not read'})
            continue
        reported_patches.setdefault(path,[]).append(location)
    for path, patch_locations in reported_patches.items():
        reason = stopped()
        if reason or len(files) >= MAX_FILES:
            omitted.append({'path':os.path.relpath(path,root),'reason':reason or 'scan capacity','phase':'reported patch reading'})
            break
        relative = os.path.relpath(path,root)
        try:
            data, metadata = _read_reported_patch(path,root,maximum=min(MAX_FILE,max(1,(MAX_READ_BYTES-read_bytes-2)//2)))
            read_bytes += metadata['read_bytes']
            read_files += 1
            files.append(path)
            manifest.append(dict(metadata,path=relative))
            if metadata.get('read_truncated'):
                omitted.append({'path':relative,'reason':'reported patch head/tail excerpt; physical middle lines omitted'})
            console_text = '\n'.join(value[0] for source,value in texts.items()
                                     if source in set(row['console_source'] for row in patch_locations))
            context = _reported_patch_context(data.decode('utf-8','replace'),patch_locations,metadata,console_text)
            add('script_context',{'path':relative,'identity':metadata,'context':context})
        except (OSError,ValueError) as error:
            omitted.append({'path':relative,'reason':'%s: %s' % (type(error).__name__,error)})
    for relative, text, metadata in scripts:
        if stopped():
            omitted.append({'reason': stopped(), 'phase': 'script processing'})
            break
        script_paths = {os.path.abspath(os.path.join(root, relative))}
        capture = parsed.get(os.path.join(os.path.dirname(relative), _contract_text('capture_diagnostics.json'))) or {}
        if isinstance(capture, dict) and capture.get('script_path'):
            script_paths.add(os.path.abspath(capture['script_path']))
        attributed, other = [], []
        for location in locations:
            filename = location.get('reported_file')
            same_file = bool(filename and (os.path.abspath(filename) in script_paths or
                filename == os.path.basename(relative) and os.path.dirname(location['console_source']) == os.path.dirname(relative)))
            if same_file and 1 <= location['line'] <= len(text.splitlines()):
                physical = text.splitlines()[location['line'] - 1]
                if location.get('column') and location['column'] > len(physical) + 1:
                    other.append(dict(location, attribution='reported_column_outside_physical_line_possible_indirect_reader',
                                      physical_line_characters=len(physical)))
                else:
                    attributed.append(location['line'])
            else:
                other.append(dict(location, attribution='external_file' if filename else 'filename_unavailable'))
        context = _script_context(text, attributed) if unexpected_error or attributed else \
            {'line_count': len(text.splitlines()), 'no_generated_script_error_attributed': True}
        context['unattributed_or_external_locations'] = other[:16]
        if (isinstance(capture, dict) and capture.get('capture_only') is True
                and (capture.get('skill') or {}).get('ok') is False):
            phase = os.path.dirname(relative)
            current_netlists = {os.path.normpath(os.path.join(row['cwd'], row['argv'][0]))
                for row in capture_invocations if row['path'].startswith(phase + '/invocations/')
                and isinstance(row.get('cwd'), str) and os.path.isabs(row['cwd']) and row.get('argv')}
            # A failed capture can already have retired its native workspace.
            # Only this phase's producer-declared, independently retained SCS
            # may augment the current invocation paths. The regular owned
            # files were read above under the same caps; do not follow a
            # producer path to a new file, adjacent candidate or external PDK.
            retained = capture.get('failure_inputs') or {}
            retained_root = os.path.join(root, phase, 'failure_inputs') + os.sep
            if (isinstance(retained, dict) and retained.get('schema_version') == _contract_text('crescendo.failed_capture_inputs.v1')
                    and retained.get('status') == 'retained' and retained.get('qualification') == 'diagnostic_only'):
                retained_paths = retained.get('retained_scs_paths') or []
                for path in retained_paths[:512] if isinstance(retained_paths, list) else []:
                    if (isinstance(path, str) and os.path.isabs(path) and path.startswith(retained_root)
                            and os.path.normpath(path) == path and os.path.realpath(path) == path):
                        current_netlists.add(path)
            first_error = (capture.get('skill') or {}).get('first_error') or {}
            first_source = first_error.get('console_source')
            first_number = first_error.get('console_line')
            # The producer's first error identifies one console and physical
            # line. Later reader failures cannot replace that first cause.
            indirect = [location for location in other
                        if location.get('attribution') == 'reported_column_outside_physical_line_possible_indirect_reader'
                        and first_source in ('ocean.stdout', 'ocean.stderr')
                        and location.get('console_source') == os.path.join(phase, first_source)
                        and isinstance(first_number, int) and first_number > 0
                        and location.get('console_line') == first_number]
            run_stack = []
            if indirect:
                frame = re.compile(r'^\s*(?:\(\.\.\.\s+in\s+(?:_axlRunAllTestsWithCallback|axlRunSimulation|maeRunSimulation)\s+\.\.\.\)|maeRunSimulation\()')
                for location in indirect:
                    source, number = location.get('console_source'), location.get('console_line')
                    value = texts.get(source)
                    if (not value or os.path.dirname(source) != phase
                            or os.path.basename(source) not in ('ocean.stdout', 'ocean.stderr')
                            or value[1].get('read_truncated') or not isinstance(number, int)):
                        continue
                    console_lines = value[0].splitlines()
                    if (number > len(console_lines) or not first_error.get('message')
                            or not console_lines[number - 1].strip().startswith(first_error['message'])):
                        continue
                    # A preceding Run stack from another error cannot locate
                    # this reader failure. Use only this exact console's next
                    # 24 physical lines, stopping at a new error boundary.
                    for index in range(number, min(number + 24, len(console_lines))):
                        line = console_lines[index]
                        if ERROR.search(line) and not frame.match(line):
                            break
                        if frame.match(line) and len(run_stack) < 8:
                            entry = {'console_source': source, 'line': index + 1, 'text': line[:512]}
                            if entry not in run_stack:
                                run_stack.append(entry)
            add('native_run_context', dict(_native_run_failure_context(text, capture, prepared,
                [row for row in failure_netlists if os.path.join(root, row['path']) in current_netlists],
                indirect_locations=indirect, run_stack_frames=run_stack),
                path=relative, identity=metadata))
        from .native_diagnostics import generated_guard_context, MAX_GUARD_SCRIPT_BYTES
        if len(text.encode('utf-8')) <= MAX_GUARD_SCRIPT_BYTES:
            guards = []
            for source, value in texts.items():
                if (os.path.dirname(source) != os.path.dirname(relative)
                        or os.path.basename(source) not in ('ocean.stdout', 'ocean.stderr')):
                    continue
                for message in re.findall(_contract_text(r'^\s*\*Error\*\s+Crescendo[^\r\n]+'), value[0], re.M):
                    association = generated_guard_context(text, message.strip())
                    if association and association not in guards and len(guards) < 8:
                        guards.append(association)
            if guards:
                context['generated_guard_associations'] = guards
        add('script_context', {'path': relative, 'identity': metadata, 'context': context})
    # Repeated netlists become one settings variant and a small point index.
    variants, point_index = collections.OrderedDict(), []
    for row in netlist_rows:
        if stopped():
            omitted.append({'reason': stopped(), 'phase': 'netlist settings processing'})
            break
        settings = dict((key, row[key]) for key in ('settings', 'temperature_sources', 'scope'))
        digest = _sha(_serialized(settings).encode('utf-8'))
        if digest not in variants:
            variants[digest] = settings
        point_index.append({'path': row['path'], 'netlist_sha256': row['sha256'],
                            'settings_variant': digest[:16], 'read_truncated': row['read_truncated']})
    add('netlist_points', {'point_index': point_index,
                           'settings_variants': [dict(value, id=key[:16]) for key, value in variants.items()]})
    for projection, relative, metadata in deferred_artifacts:
        if stopped():
            omitted.append({'path': relative, 'phase': 'artifact processing', 'reason': stopped()})
            continue
        if artifact_sections(projection, relative, metadata) and relative.endswith(_contract_text('corner_identity.json')):
            retained_contracts.add(relative)
    complete_contract = next((document for path, document in parsed.items()
        if path in retained_contracts and path.endswith(_contract_text('corner_identity.json')) and isinstance(document, dict)
        and isinstance(document.get('saved_source'), dict)
        and 'execution_settings' in document['saved_source'] and 'global_variables' in document['saved_source']), None)
    source = (prepared.get('setup') or {}).get('source') or {}
    source_path = source.get('setup_db') or source.get('maestro_sdb')
    if complete_contract:
        add('source_settings', {'source': 'corner_identity.saved_source',
            'sha256': complete_contract['saved_source'].get('sha256'),
            'scope': 'saved source contract retained in corner_identity.json; successful comparisons may be summarized as counts/hashes in this report'})
    elif source_path and not stopped():
        try:
            source_data, source_metadata = _read(source_path, maximum=min(MAX_FILE, max(1, (MAX_READ_BYTES - read_bytes - 2) // 2)))
            read_bytes += source_metadata['read_bytes']
            add('source_settings', _settings_xml(source_path, source_data, source_metadata, selected_test=(prepared.get('setup') or {}).get('selected_test')))
        except Exception as error:
            omitted.append({'source': 'explicit prepared source SetupDB', 'reason': str(error)})
    else:
        omitted.append({'source': 'saved source SetupDB', 'reason': stopped() or 'prepared source path unavailable'})
    from .environment_probes import failure_environment_details
    errors = str(error_text or '') + '\n' + '\n'.join(value[0][:65536] for path, value in texts.items()
        if not _raw_scheduler_text_name(os.path.basename(path)))
    errors += '\n' + '\n'.join(_serialized(document)[:65536] for document in parsed.values())
    targeted = failure_environment_details(config, root, errors, cancel) if not stopped() else {}
    if targeted:
        add('failure_environment', targeted)
    add('causal_summary', _causal_summary(parsed, texts, stop_proven))
    add('cleanup_readers', _open_reader_evidence(root))
    observed_names = set(os.path.basename(path) for path in files)
    missing = [name for name in (_contract_text('corner_identity.json'), _contract_text('preparation_failure.json'),
               _contract_text('native_dispatch.jsonl'), 'native_corner_schedule.tsv', 'capture.il') if name not in observed_names]
    add('collection', {'status': 'PARTIAL' if omitted else 'COLLECTED', 'bytes_inlined': used,
        'limit_bytes': MAX_TOTAL, 'read_bytes': read_bytes, 'read_limit_bytes': MAX_READ_BYTES,
        'files_read': read_files, 'discovered_file_count': len(files), 'deadline_seconds': COLLECTION_SECONDS,
        'files': manifest[:24], 'manifest_file_count': len(manifest),
        'manifest_sha256': _sha(_serialized(manifest).encode('utf-8')),
        'manifest_scope': 'identities are in each inline artifact and netlist point index; first 24 read sources listed here',
        'excluded_non_contract_artifacts': excluded_non_contract,
        'missing_artifacts': missing,
        'missing_meaning': 'not produced/not applicable or stage not reached; absence is never PASS',
        'omitted_or_truncated': omitted, 'fresh_evidence': True})
    return records


def failure_layer(primary):
    """Map an explicit producer stage; vague solver/results failures stay unknown."""
    stage = str((primary or {}).get('failure_stage') or '')
    if stage == 'native_point_parameter_readback':
        return 'measurement'
    if stage.startswith(('model_include', 'native_template', 'native_capture', 'native_user_scripts', 'capture', 'netlist',
                         'native_parameter', 'native_device_parameter', 'native_run', 'native_history',
                         'maestro_startup', 'workspace', 'parameter_', 'source_guard', 'source_freshness', 'prepared_freshness', 'native_file_input')):
        return 'netlist_generation'
    if stage.startswith(('command_render', 'solver_configuration', 'simulator_readiness', 'simulator_contract')):
        return 'solver_command'
    if stage.startswith(('submission', 'submit', 'scheduler')):
        return 'submission'
    if stage.startswith(('solve', 'spectre', 'alps')):
        return 'solve'
    if stage.startswith(('measurement', 'output', 'result_reader')):
        return 'measurement'
    if stage.startswith(('cleanup', 'termination', 'stop')):
        return 'termination_and_cleanup'
    if stage == 'execution_pipeline':
        return 'pipeline_proof'
    return None


def failure_summary(status, candidates, timed_out=False):
    """Keep the native producer's first cause separate from secondary cleanup."""
    status = status or {}
    primary = status.get('primary_failure')
    failures = [row for row in candidates or [] if row.get('status') != 'completed' and row.get('error')]
    # Candidate lists are normally sorted by index, not failure time. Prefer
    # the adapter's latched cause; older runs lack it, so keep an actual error
    # ahead of cancellation fallout without inferring its underlying cause.
    def cancellation(cause):
        return bool(cause.get('cancelled') or re.search(r'\bcancelled\b', str(cause.get('error') or ''), re.I))
    failed = next((row for row in failures if isinstance(row.get('primary_failure'), dict)
                   and not cancellation(row['primary_failure'])), None)
    if failed is None:
        failed = next((row for row in failures if not row.get('secondary_failure')
                       and row.get('failure_stage') != 'peer_cancellation'
                       and not re.search(r'\bcancelled\b', str(row.get('error') or ''), re.I)), None)
    if failed is None:
        failed = next(iter(failures), None)
    aggregate = bool(isinstance(primary, dict) and (
        primary.get('failure_stage') == 'optimization' or
        re.search(r'all\s+\d+\s+candidates?\s+failed|inspect candidate errors|systemic infrastructure failure',
                  str(primary.get('message') or ''), re.I)))
    lifecycle = None
    if failed and (not primary or aggregate):
        lifecycle = primary
        structured = (failed.get('primary_failure') or failed.get('failure') or
                      failed.get('native_failure_detail'))
        primary = dict(structured) if isinstance(structured, dict) else {}
        for key in ('candidate_id', 'error', 'error_type', 'failure_stage', 'failure_class', 'diagnostic_path'):
            if failed.get(key) is not None and key not in primary:
                primary[key] = failed[key]
        # The exception label is emitted by the producer in its error string;
        # keep its existing stage/class and do not infer a hardware/root cause.
        if not primary.get('error_type'):
            exception = re.match(r'^([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception|Unsupported))\s*:', str(failed.get('error') or ''))
            if exception:
                primary['error_type'] = exception.group(1)
                primary['error_type_source'] = 'producer_error_prefix'
        primary['source'] = 'first_failed_candidate_producer'
    if not primary and (status or {}).get('state') == 'failed':
        primary = {'message': status.get('message'), 'source': 'native status, stage unavailable'}
    if not primary and timed_out:
        primary = {'failure_stage': 'diagnostic_deadline', 'message': 'run exceeded this test deadline; underlying native cause unproven'}
    return {'first_cause': primary, 'first_cause_layer': failure_layer(primary), 'secondary_lifecycle': lifecycle,
            'secondary_cleanup': status.get('cleanup_failure'),
            'scope': 'native first failure retained; capture-only wrapper failure is not a solver/OOM diagnosis'}


def retained_artifact_index(root, maximum=4096):
    """Bounded metadata-only index of existing owned files, never guessed paths.

    This describes retention for display. It grants no native ownership or
    completion proof and never follows linked directories or reads source PDKs.
    """
    root = os.path.abspath(root)
    result = {'netlists': [], 'artifacts': [], 'retention_status': 'retained',
              'index_complete': True, 'run_root': root}
    if os.path.islink(root) or not os.path.isdir(root) or os.path.realpath(root) != root:
        result.update(retention_status='run_directory_unavailable', index_complete=False)
        return result
    count, deadline = 0, time.monotonic() + 2
    capture_names = {'ocean.stdout', 'ocean.stderr', 'capture.il', 'bootstrap.il',
                     _contract_text('capture_diagnostics.json'), _contract_text('capture_generation.json'), _contract_text('preparation_failure.json'),
                     _contract_text('execution_diagnostics.json'), _contract_text('error.json')}
    def inaccessible(error):
        result.update(index_complete=False, retention_status='index_partial_original_files_retained')
        result['artifacts'].append({'path':getattr(error,'filename',None) or root,
                                    'status':'unreadable_directory','reason':str(error)})
    for directory, children, names in os.walk(root, followlinks=False, onerror=inaccessible):
        if time.monotonic() > deadline:
            result.update(index_complete=False, retention_status='index_partial_original_files_retained')
            return result
        children[:] = sorted(name for name in children if name not in ('raw', 'psf', 'ahdl', 'home', '__pycache__')
                             and not os.path.islink(os.path.join(directory, name)))
        for name in sorted(names):
            count += 1
            if count > maximum or time.monotonic() > deadline:
                result.update(index_complete=False, retention_status='index_partial_original_files_retained')
                return result
            path = os.path.join(directory, name)
            if not (name.endswith('.scs') or name in capture_names):
                continue
            try:
                info = os.lstat(path)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    result['artifacts'].append({'path': path, 'status': 'unsafe_link_or_non_regular', 'role': 'not_read'})
                    continue
                entry = {'path': path, 'status': 'generated', 'exists': True, 'bytes': info.st_size}
                if name.endswith('.scs'):
                    entry['kind'] = ('solver_netlist_snapshot' if 'diagnostic_netlists' in path.split(os.sep) else
                                     'candidate_netlist' if '/tasks/' in path.replace(os.sep, '/') else 'capture_or_template_netlist')
                    result['netlists'].append(entry)
                else:
                    entry['role'] = 'capture_or_failure_evidence'
                    result['artifacts'].append(entry)
            except OSError as error:
                result['artifacts'].append({'path': path, 'status': 'unavailable', 'reason': str(error)})
    if not result['netlists']:
        result['retention_status'] = 'no_generated_scs_observed_check_capture_evidence'
    return result


def concise_success_records(records, case, checks=(), full=False):
    """Presentation only: successful stopped cases need no repeated raw setup.

    Failed/unknown cases keep all evidence. Never infer success from absence
    of errors; execution checks and raw collection have already completed.
    """
    if (full or case.get('state') != 'completed' or case.get('stop_proven') is not True
            or case.get('first_cause') or any(check.get('status') in ('FAIL', 'BLOCKED', 'CANCELLED')
            or (check.get('status') == 'WARN' and not str(check.get('id', '')).endswith('diagnostics.collection'))
            for check in checks)):
        return records
    keep = {'diagnostic.identity', 'diagnostic.scope', 'diagnostic.single_test_scope',
            'diagnostic.first_cause', 'diagnostic.causal_summary',
            'diagnostic.native_worker_diagnostics', 'diagnostic.cdf_callback_failure',
            'diagnostic.producer_failure'}
    selected, omitted = [], collections.Counter()
    for record in records:
        kind, payload = record.get('observation'), record.get('payload') or {}
        if (kind in keep or (kind == 'diagnostic.artifact'
                and os.path.basename(payload.get('path') or '') == _contract_text('native_generation_required.json'))):
            # A retained hash reference cannot point to a suppressed record.
            # Keep this entire case detailed if its core proof is interned.
            if '"same_as"' in _serialized(payload):
                return records
            if kind == 'diagnostic.first_cause':
                record = dict(record, payload={key: value for key, value in payload.items() if key != 'metric_summary'})
            selected.append(record)
        elif kind == 'diagnostic.collection':
            selected.append(dict(record, payload={key: value for key, value in payload.items()
                if key not in ('files', 'artifacts')}))
        else:
            omitted[kind or 'unknown'] += 1
    if not omitted:
        return records
    selected.append({'observation': 'diagnostic.success_detail_summary', 'payload': {
        'mode': 'concise_success', 'collected_record_count': len(records),
        'suppressed_record_count': sum(omitted.values()), 'suppressed_by_kind': dict(omitted),
        'collected_records_sha256': _sha(_serialized(records).encode('utf-8')),
        'scope': 'presentation only; checks, parameter/output proofs and collection omissions retained',
        'detail_policy': 'failed or uncertain cases remain detailed; diagnostic_log_detail=full expands successful cases'}})
    return selected


_COMPACT_BAD = re.compile(r'fail|error|exception|fatal|cancel|timeout|timed.out|unknown|unproven|blocked|mismatch|missing|unavailable|warn|denied|not.found', re.I)
_COMPACT_CONTEXT = re.compile(r'error|cause|reason|message|summary|detail|context|stage|status|state|stop|source|corner|model|param|binding|identity|test|point|candidate|task|job|path|file|line|sha256|code|return|expected|actual|name|value|text|stdout|stderr|verified|termination|^ok$', re.I)
_COMPACT_ARRAYS = frozenset(('population', 'metric_summary', 'objectives', 'constraints', 'genes', 'fitness', 'pareto_front', 'optimization_history'))


def _compact_stamp(value):
    return {'count': len(value), 'sha256': _sha(_serialized(value).encode('utf-8'))}


def _compact_failure(value):
    """Recognize an abnormal local node, without labelling its ancestors failed."""
    if not isinstance(value, dict):
        return isinstance(value, str) and bool(ERROR.search(value) or _COMPACT_BAD.search(value))
    for key, item in value.items():
        if key in ('returncode', 'return_code', 'exit_code', 'rc') and isinstance(item, (int, float)) and item != 0:
            return True
        if key in ('ok', 'stop_proven', 'source_unchanged', 'verified', 'process_termination_proven') and item is False:
            return True
        if re.search(r'error|exception|first_cause|failure|mismatch', key, re.I) and item not in (None, False, '', [], {}):
            return True
        if key in ('status', 'state', 'level', 'message', 'reason') and isinstance(item, str) and _COMPACT_BAD.search(item):
            return True
    return False


def _compact_has_issue(value):
    if _compact_failure(value):
        return True
    if isinstance(value, dict):
        return any(_compact_has_issue(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_compact_has_issue(item) for item in value)
    return False


def _compact_structural_issue(value, recursive=True):
    """Check producer fields, not explanatory prose such as 'not stop proof'."""
    if isinstance(value, dict):
        for name, item in value.items():
            if name in ('stop_proven', 'remote_stop_proven', 'process_termination_proven',
                        'source_unchanged', 'source_guard_ok', 'ok', 'verified') and item is not True:
                return True
            if name in ('returncode', 'return_code', 'exit_code', 'rc') and isinstance(item, (int, float)) and item != 0:
                return True
            if name in ('status', 'state', 'level') and isinstance(item, str) and _COMPACT_BAD.search(item):
                return True
            if name in ('message', 'reason', 'summary') and isinstance(item, str) and ERROR.search(item):
                return True
            if (re.search(r'(?:^|_)(?:errors?|failures?|exception|mismatches|unresolved|missing|unexpected)$', name)
                    or name in ('first_cause', 'failure_detail', 'cleanup_pending', 'termination_failed', 'timed_out')):
                if item not in (None, False, '', [], {}):
                    return True
            if recursive and _compact_structural_issue(item):
                return True
    elif recursive and isinstance(value, (list, tuple)):
        return any(_compact_structural_issue(item) for item in value)
    return False


def _compact_success_signal(value):
    if not isinstance(value, dict) or _compact_structural_issue(value, recursive=False):
        return False
    return (value.get('ok') is True or value.get('status') == 'completed' or
            any(isinstance(value.get(name), dict) and value[name].get('ok') is True
                for name in ('completion', 'completion_evidence', 'parameter_application')))


def _compact_proof_summary(value, origin, log_context=False):
    """Small proof receipt, only after an explicit producer success signal."""
    result = dict(_compact_stamp(value), successful_subtree_summary=True,
                  detail='successful proof values summarized; full detail requires diagnostic_log_detail=full on a subsequent run')
    labels, flags = {}, {}
    def collect(node, route=''):
        if isinstance(node, dict):
            for name, item in node.items():
                field = route + name
                if isinstance(item, bool) and (name == 'ok' or re.search(r'proven|verified|unchanged|cleanup_pending', name)):
                    flag = flags.setdefault(field, {'values': [], 'true_count': 0, 'false_count': 0})
                    if item not in flag['values']:
                        flag['values'].append(item)
                    flag['true_count' if item else 'false_count'] += 1
                if name in ('candidate_id', 'test', 'corner', 'point_id', 'selected_test', 'status') and not isinstance(item, (dict, list, tuple)):
                    labels.setdefault(name, set()).add(str(item))
                if name in ('tests', 'corners') and isinstance(item, list) and all(isinstance(x, str) for x in item):
                    labels.setdefault(name, set()).update(item)
                if isinstance(item, (dict, list, tuple)):
                    collect(item, field + '.')
        elif isinstance(node, (list, tuple)):
            for item in node:
                collect(item, route + '[].')
    collect(value)
    result['identity_samples'] = {name: sorted(items)[:4] for name, items in labels.items()}
    result['qualification_flags'] = flags
    if isinstance(value, dict):
        result['structure_counts'] = {name: len(item) for name, item in value.items() if isinstance(item, (dict, list, tuple))}
        for name in ('schema_version', 'candidate_id', 'status', 'state', 'ok', 'point_count', 'task_count', 'returncode'):
            if name in value:
                result[name] = value[name]
    result['original_evidence_locator'] = origin
    issues = _compact_issues(value, origin, predicate=lambda node: _compact_structural_issue(node, recursive=False)
        or (log_context and any(isinstance(node.get(name), str) and ERROR.search(node[name])
                                for name in ('text', 'message', 'reason'))))
    if issues:
        result['abnormal_context'] = issues
    return result


def _compact_value(value, key='', origin='collected_records', depth=0):
    """Bound display volume; original evidence and its digest remain authoritative."""
    if isinstance(value, dict):
        if 'same_as' in value:
            # Interning targets name original document fields, not this projection.
            # Do not expand a shared structure under a different candidate identity.
            return {'original_evidence_reference': value['same_as'],
                    'reference_scope': 'original evidence locator; enable diagnostic_log_detail=full for a subsequent run',
                    'sha256': value.get('sha256'), 'record_source': origin}
        if (not value.get('successful_subtree_summary') and _compact_success_signal(value)
                and len(_serialized(value)) > 900):
            return _compact_proof_summary(value, origin)
        if (key in ('effective_job_policy', 'configured_paths', 'manifest', 'native_baseline', 'native_model_resolution')
                and len(_serialized(value)) > 1200 and not _compact_has_issue(value)):
            return dict(_compact_stamp(value),
                        status_fields={name: item for name, item in value.items()
                            if not isinstance(item, (dict, list, tuple)) and not (isinstance(item, str) and len(item) > 200)},
                        detail='successful setup structure summarized; existing file retention policy unchanged')
        result = {}
        for name, item in value.items():
            if name in _COMPACT_ARRAYS and isinstance(item, (dict, list, tuple)):
                result[name] = dict(_compact_stamp(item), detail='optimization values omitted from display')
                issues = _compact_issues(item, origin)
                if issues:
                    result[name]['abnormal_context'] = issues
            elif name == 'argv' and isinstance(item, (list, tuple)):
                result[name] = dict(_compact_stamp(item), executable=item[0] if item else None,
                                    flags=[x for x in item[1:] if isinstance(x, str) and x.startswith(('-', '+')) and '/' not in x][:12])
                if _compact_failure(value):
                    result[name]['failure_arguments'] = (list(item) if len(_serialized(item)) <= 8192
                        else _compact_value(item, origin=origin))
            else:
                result[name] = _compact_value(item, name, origin, depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if not value:
            return []
        # Keep every distinct abnormal row, even if it is in the middle of a
        # large, otherwise successful inventory. Unknown schemas get head/tail.
        indexes = sorted(set([0, len(value) - 1] + [i for i, item in enumerate(value) if _compact_has_issue(item)]))
        if len(value) <= 3 and key not in ('files', 'artifacts', 'rows', 'events', 'tasks', 'candidates', 'submissions'):
            indexes = list(range(len(value)))
        samples, seen = [], set()
        for i in indexes:
            row = _compact_value(value[i], key, origin, depth + 1)
            digest = _serialized(row)
            if digest not in seen:
                samples.append(row)
                seen.add(digest)
        if len(indexes) == len(value) and len(samples) == len(value):
            return samples
        return dict(_compact_stamp(value), samples=samples, omitted=len(value) - len(samples),
                    selection='first/last and distinct abnormal rows; existing file retention policy unchanged')
    if isinstance(value, str) and len(value) > 1400:
        lines = value.splitlines()
        selected = set([0, len(lines) - 1])
        for i, line in enumerate(lines):
            if ERROR.search(line) or _COMPACT_BAD.search(line):
                selected.update(range(max(0, i - 1), min(len(lines), i + 2)))
        excerpts = []
        for i in sorted(selected):
            line = lines[i]
            excerpts.append({'line': i + 1, 'text': line if len(line) <= 1400 else line[:900] + ' ... [display omitted] ... ' + line[-350:]})
        return {'characters': len(value), 'sha256': _sha(value.encode('utf-8')),
                'context': excerpts, 'detail': 'head/tail and error context; original evidence locator; full detail requires a subsequent run with diagnostic_log_detail=full',
                'record_source': origin}
    return value


def _compact_issues(value, origin, context=None, predicate=None):
    """Retain failures hidden inside structures removed from progress snapshots."""
    found, seen = [], set()
    def visit(node, inherited):
        if isinstance(node, dict):
            here = dict(inherited)
            for name in ('candidate_id', 'task_id', 'job_id', 'point_id', 'test', 'corner', 'path',
                         'console', 'console_path', 'source', 'source_path', 'file', 'library', 'cell',
                         'view', 'output_key', 'parameter', 'parameter_name', 'model', 'section', 'diagnostic_path'):
                if name in node:
                    here[name] = node[name]
            if (predicate or _compact_failure)(node):
                detail = {name: item for name, item in node.items()
                          if (_COMPACT_CONTEXT.search(name) and not isinstance(item, (dict, list, tuple)))
                          or name in ('error', 'errors', 'exception', 'detail', 'context', 'expected', 'actual', 'binding', 'parameters', 'models')}
                detail.update(here)
                detail = _compact_value(detail, origin=origin)
                digest = _serialized(detail)
                if detail and digest not in seen:
                    found.append(detail)
                    seen.add(digest)
            for item in node.values():
                visit(item, here)
        elif isinstance(node, (list, tuple)):
            for item in node:
                visit(item, inherited)
    visit(value, context or {})
    return found


def concise_case_records(records, case, checks=(), full=False):
    """Pure display projection for both failed and successful diagnostic cases.

    Never changes checks, stop/source authority, execution, cleanup or raw files.
    Full mode is lossless. Default mode states every sampling policy and keeps
    first causes, distinct abnormal context and original-evidence references.
    """
    if full:
        return records
    original_records = records
    records = concise_success_records(records, case, checks)
    selected, omitted = [], collections.Counter()
    grouped = collections.OrderedDict()
    progress_last = {}
    completed_candidates = set()
    for check in checks:
        match = re.search(r'(g[0-9]+_c[0-9]+)\.(?:completion_evidence|parameter_application)$', str(check.get('id', '')))
        if match and check.get('status') == 'PASS':
            completed_candidates.add(match.group(1))
    for record in records:
        payload = record.get('payload') if isinstance(record, dict) else None
        if not isinstance(payload, dict):
            continue
        match = re.search(r'(?:^|/)candidates/([^/]+)/', str(payload.get('path', '')))
        data = payload.get('data')
        if match and _compact_success_signal(data):
            completed_candidates.add(match.group(1))
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            selected.append(_compact_value(record))
            continue
        kind = record.get('observation', 'unknown')
        payload = record.get('payload') or {}
        origin = payload.get('path') if isinstance(payload, dict) else None
        origin = origin or 'collected_records[{}]'.format(index)
        candidate_locator = origin if '/candidates/' in '/' + origin else str(payload.get('candidate', '')) if isinstance(payload, dict) else origin
        match = re.search(r'(?:^|/)candidates/([^/]+)(?:/|$)', candidate_locator)
        if (match and match.group(1) in completed_candidates
                and kind in ('diagnostic.artifact', 'diagnostic.artifact_section', 'diagnostic.settings',
                             'diagnostic.point_parameter_reader', 'diagnostic.protocol', 'diagnostic.native_dispatch',
                             'diagnostic.script_context', 'diagnostic.point_parameter_reader_files',
                             'diagnostic.native_storage_evidence', 'diagnostic.log_context')
                and len(_serialized(payload)) > 900):
            brief = {name: item for name, item in payload.items()
                     if name in ('path', 'identity', 'field', 'sha256', 'hash_scope', 'bytes', 'read_truncated')}
            brief['proof_summary'] = _compact_proof_summary(payload.get('data', payload), origin,
                log_context=kind in ('diagnostic.log_context', 'diagnostic.native_storage_evidence'))
            record = dict(record, payload=brief)
            payload = brief
            if not brief['proof_summary'].get('abnormal_context'):
                grouped.setdefault('diagnostic.completed_candidate.' + match.group(1), []).append(record)
                omitted[kind] += 1
                continue
        if kind in ('native.log.discovered', 'diagnostic.native_storage_evidence'):
            # Directory/log discovery is metadata, not execution evidence.
            # Abnormal discovery records continue through the ordinary path.
            if not _compact_issues(record, origin):
                grouped.setdefault(kind, []).append(record)
                omitted[kind] += 1
                continue
        if kind == 'execution.progress' and isinstance(payload, dict):
            brief = {name: item for name, item in payload.items()
                     if name not in ('candidates', 'submissions', 'gate_progress')}
            candidates = payload.get('candidates') or []
            if isinstance(candidates, list):
                brief['candidate_states'] = dict(collections.Counter(str(item.get('status', item.get('state', 'unknown')))
                    for item in candidates if isinstance(item, dict)))
            issues = _compact_issues(payload, origin)
            if issues:
                brief['abnormal_context'] = issues
            projected = dict(record, payload=_compact_value(brief, origin=origin))
            signature = _serialized({name: brief.get(name) for name in ('state', 'execution_stage', 'command_states', 'candidate_states', 'abnormal_context')})
            run = record.get('run_id', '')
            previous = progress_last.get(run)
            if previous and signature == previous[0]:
                previous[1]['repeated_observations'] = previous[1].get('repeated_observations', 1) + 1
                previous[1]['last_message'] = _compact_value(payload.get('message'), origin=origin)
                omitted[kind] += 1
                continue
            selected.append(projected)
            progress_last[run] = (signature, projected)
            continue
        if kind == 'diagnostic.log_context' and isinstance(payload, dict):
            context = payload.get('context') or {}
            events = context.get('events') if isinstance(context, dict) else None
            if isinstance(events, list):
                brief = {name: item for name, item in payload.items() if name != 'context'}
                brief['context'] = dict(_compact_stamp(events),
                    boundary_events=[_compact_value(events[i], origin=origin) for i in sorted(set([0, len(events)-1]))] if events else [],
                    abnormal_context=_compact_issues(events, origin),
                    selection='first/last events and distinct abnormal context')
                record = dict(record, payload=brief)
        projected = _compact_value(record, origin=origin)
        selected.append(projected)
    for kind, items in grouped.items():
        examples = [items[0]] if len(items) == 1 else [items[0], items[-1]]
        selected.append({'observation': kind + '.summary', 'payload': dict(_compact_stamp(items),
            examples=[_compact_value(item) for item in examples],
            selection='first/last metadata only; enable diagnostic_log_detail=full for a subsequent run to retain the complete inventory')})
    selected.append({'observation': 'diagnostic.detail_summary', 'payload': {
        'mode': 'concise_case', 'state': case.get('state'), 'stop_proven': case.get('stop_proven'),
        'collected_record_count': len(original_records), 'suppressed_by_kind': dict(omitted),
        'collected_records_sha256': _sha(_serialized(original_records).encode('utf-8')),
        'check_status_counts': dict(collections.Counter(item.get('status', 'UNKNOWN') for item in checks)),
        'detail_policy': 'presentation only: repeated progress, command argv, inventories and optimization arrays summarized; unknown structures retain first/last and abnormal context; enable diagnostic_log_detail=full for a subsequent run to retain all collected detail; existing file retention policy unchanged'}})
    return selected


def concise_diagnostic_value(value):
    """Public, non-mutating projection for check details and case context."""
    return _compact_value(value)
