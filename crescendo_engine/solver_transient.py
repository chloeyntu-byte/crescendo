"""Profile-driven transient solver recovery; observations never prove completion."""
import math
import os
import shutil
import uuid

from .owned_solver_logs import OwnedSolverLogError, owned_solver_log_reader
from .simulator_profiles import transient_crash_signatures
from .runtime import bind_text
_contract_text = bind_text()

RETRY_FIELDS = ('solver_transient_retries', 'solver_transient_last_retry_single_core')
ATTEMPTS_SCHEMA = _contract_text('crescendo.solver_attempts.v1')


def normalize_retry_settings(values):
    value = values.get(RETRY_FIELDS[0], 2)
    if isinstance(value, bool):
        raise ValueError(RETRY_FIELDS[0] + ' must be a nonnegative integer')
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(RETRY_FIELDS[0] + ' must be a nonnegative integer')
    if not math.isfinite(number) or number < 0 or number != int(number):
        raise ValueError(RETRY_FIELDS[0] + ' must be a nonnegative integer')
    single = values.get(RETRY_FIELDS[1], False)
    if type(single) is not bool:
        raise ValueError(RETRY_FIELDS[1] + ' must be boolean')
    return {RETRY_FIELDS[0]: int(number), RETRY_FIELDS[1]: single}


def retry_settings(config, spec):
    values = dict(config.get('runtime') or {})
    values.update((key, spec[key]) for key in RETRY_FIELDS if key in spec)
    return normalize_retry_settings(values)


def match_transient_crash(task, files, terminal, root, launch_epoch):
    code = (terminal or {}).get('returncode')
    if type(code) is not int:
        return None
    # The POSIX compute wrapper reports signals as 128 + signum; direct
    # subprocess adapters can report negative signal numbers.
    signatures = [row for row in transient_crash_signatures(task.get('execution_simulator', 'spectre'))
                  if code in row['returncodes'] or row.get('allow_signal') and
                  (code < 0 or 129 <= code <= 192)]
    if not signatures:
        return None
    paths = [task['spectre_log'], os.path.join(task['psf_dir'], 'log'),
             files['simulator.stdout'], files['simulator.stderr']]
    baseline = task.get('solver_log_baseline') or {}
    for path in dict.fromkeys(paths):
        try:
            with owned_solver_log_reader(path, os.path.dirname(task['psf_dir']), launch_epoch, task=task) as (fd, metadata):
                info = os.fstat(fd)
                previous = baseline.get(path)
                if previous and previous[:2] == (info.st_dev, info.st_ino):
                    # Only bytes beyond the pre-launch extent can authorize a
                    # retry. In-place rewrites/truncations are ambiguous: do not
                    # reuse any byte from the old extent, even if mtime changed.
                    os.lseek(fd, previous[2], os.SEEK_SET)
                metadata['attempt_start_offset'] = os.lseek(fd, 0, os.SEEK_CUR)
                # Stream literal matching, retaining only a small overlap.
                # Each match must be in one owned log from this attempt.
                found = set()
                first_error = None
                overlap = ''
                keywords = set(word for row in signatures for word in row['all_text'])
                while True:
                    raw = os.read(fd, 65536)
                    if not raw:
                        break
                    text = overlap + raw.decode('utf-8', 'replace')
                    found.update(word for word in keywords if word in text)
                    if first_error is None:
                        first_error = next((line[:2048] for line in text.splitlines()
                                            if any(word in line for word in keywords)), None)
                    overlap = text[-max(len(word) for word in keywords):]
                matched = next((row for row in signatures if set(row['all_text']) <= found), None)
            if matched:
                return {'reason': 'solver_transient_crash', 'signature': matched['id'],
                        'returncode': code, 'log': path, 'first_error': first_error, 'source_identity': metadata}
        except (OSError, OwnedSolverLogError):
            continue
    return None


def last_retry_prefix(local):
    if not local:
        return [], 'ignored_cluster_mode'
    executable = shutil.which('taskset')
    if not executable:
        return [], 'taskset_unavailable'
    try:
        cpus = sorted(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        return [], 'available_cpu_unknown'
    if not cpus:
        return [], 'available_cpu_unknown'
    return [executable, '-c', str(cpus[0])], 'entire_solve_single_cpu'


def retry_observations(candidates):
    """Passive per-point counts; never feeds feasibility or diagnostic verdicts."""
    result = {'transient_crashes': 0, 'retries': 0, 'finally_succeeded_points': 0,
              'exhausted_points': 0, 'observed_points': 0}
    seen = set()
    for candidate in candidates:
        for point in (candidate.get('solver_transient') or {}).get('points', []):
            tokens = tuple(attempt['token'] for attempt in point.get('attempts', []) if attempt.get('token'))
            identity = ('tokens', tokens) if tokens else None
            physical = candidate.get('physical_execution_candidate')
            if identity is None and physical and point.get('task_id') is not None:
                reference = candidate.get('batch_reference') or {}
                identity = ('physical', reference.get('batch_epoch'), physical, point['task_id'])
            if identity is not None:
                if identity in seen:
                    continue
                seen.add(identity)
            result['observed_points'] += 1
            crashes = sum(bool(attempt.get('transient_crash')) for attempt in point.get('attempts', []))
            result['transient_crashes'] += crashes
            result['retries'] += max(0, len(point.get('attempts', [])) - 1)
            result['finally_succeeded_points'] += int(crashes > 0 and point.get('outcome') == 'completed')
            result['exhausted_points'] += int(point.get('outcome') == 'transient_crash_exhausted')
    return result


def solver_log_baseline(task):
    """Record pre-launch extents; only later bytes in the same inode are eligible."""
    paths = [task['spectre_log'], os.path.join(task['psf_dir'], 'log')]
    result = {}
    for path in paths:
        try:
            info = os.lstat(path)
            result[path] = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        except OSError:
            result[path] = None
    return result


def retain_attempt_evidence(task, manifest, run_root, record):
    """Full per-attempt logs/first error outside the retired candidate raw tree.

    Copies are observational evidence, never ownership or completion authority.
    Control originals stay in place until the normal lifecycle retires them.
    """
    from .retention import _diagnostic_directory_fd
    from .utils import atomic_write_json
    import re
    root = os.path.abspath(run_root or manifest['root'])
    components = ['private_netlist', 'solver_attempts', manifest['candidate_id'], task['task_id'],
                  'attempt_%03d' % record['attempt'], record.get('token') or uuid.uuid4().hex]
    if any(not re.fullmatch(r'[A-Za-z0-9_.-]+', part) or part in ('.', '..') for part in components):
        raise ValueError('Solver evidence identity is unsafe')
    descriptor = _diagnostic_directory_fd(root, components, os.stat(root).st_dev, create=True)
    target = os.path.join(root, *components)
    result = {'path': target, 'files': [], 'warnings': [], 'completion_authority': False}
    sources = [('solver.log', task['spectre_log']), ('solver_psf.log', os.path.join(task['psf_dir'], 'log'))]
    control = record['control_path']
    sources += [(name, os.path.join(control, name)) for name in
                ('simulator.stdout', 'simulator.stderr', 'started.receipt', 'terminal.receipt', _contract_text('error.json'),
                 _contract_text('submission.json'), 'scheduler.stdout', 'scheduler.stderr', _contract_text('task_timeouts.json'))]
    try:
        for name, source in sources:
            try:
                with owned_solver_log_reader(source, manifest['root'], task=task) as (fd, _meta):
                    destination = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=descriptor)
                    with os.fdopen(destination, 'wb') as stream:
                        while True:
                            chunk = os.read(fd, 65536)
                            if not chunk:
                                break
                            stream.write(chunk)
                result['files'].append(name)
            except FileNotFoundError:
                continue
            except (OSError, OwnedSolverLogError) as error:
                result['warnings'].append(name + ': ' + type(error).__name__)
        # The unique directory is anchored during publication; its parents are
        # checked again before publishing the compact record by path.
        if os.path.realpath(target) != target or not os.path.samestat(os.fstat(descriptor), os.lstat(target)):
            raise ValueError('Solver attempt evidence directory changed')
        atomic_write_json(os.path.join(target, 'attempt.json'), record)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return result
