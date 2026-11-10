"""One owned Ocean read of exact point PSF design parameters; no simulation."""
import copy
import math
import os
import time
import uuid

from .cadence import child_environment
from .netlist_measurement import _quote, run_owned_command, NetlistMeasurementError
from .utils import atomic_write_json, atomic_write_text, ensure_dir

PROTOCOL = 'CRESCENDO_NATIVE_POINT_PARAMS_1'


def prefetch_point_parameters(requests, config, candidate_root, session, cancel_check=None):
    """Bind one live read to the exact requests rederived by final validation."""
    bound_requests = copy.deepcopy(requests)
    values = (read_point_parameters(bound_requests, config, candidate_root,
                                    native_session=session, cancel_check=cancel_check)
              if any(request['names'] for request in bound_requests) else [{} for request in bound_requests])
    def cached_reader(requested):
        if requested != bound_requests:
            raise NetlistMeasurementError('Final exact point requests differ from the completed session read')
        return copy.deepcopy(values)
    return cached_reader


def render_point_parameter_script(requests, path, token, result_reader="spectre", progress_path=None, startup_lines=None,
                                  exit_after=True):
    if result_reader not in ("spectre", "alps"):
        raise NetlistMeasurementError("Unsupported native point result reader")
    lines = ['; Read exact completed point PSF parameters; no ADE/source writes.']
    def mark(stage):
        if progress_path:
            lines.extend(['crescProgressPort=outfile(%s)' % _quote(progress_path),
                          'when(crescProgressPort fprintf(crescProgressPort %s) close(crescProgressPort))' % _quote(token + '\t' + stage + '\n')])
    mark('script_started')
    for index, line in enumerate(startup_lines or []):
        mark('startup_extension_%d' % index)
        lines.append(line)
    mark('select_reader')
    lines += ['crescReader=errset(simulator(\'%s) t)' % result_reader,
             'unless(crescReader error("Crescendo selected PSF reader unavailable\\n"))',
             'crescPointPort=outfile(%s)' % _quote(path),
             'unless(crescPointPort error("Crescendo point parameter records unavailable\\n"))',
             'fprintf(crescPointPort "%s\\t%s\\n")' % (PROTOCOL, token), 'crescPointFailed=nil']
    for index, request in enumerate(requests):
        mark('open_results_%d' % index)
        lines += ['crescPointOpened=errset(openResults(%s) t)' % _quote(request['psf_dir']),
                  'unless(crescPointOpened && car(crescPointOpened) error("Crescendo exact point PSF unavailable\\n"))']
        for variable_index, name in enumerate(request['names']):
            mark('read_parameter_%d_%d' % (index, variable_index))
            lines += ['crescPointValue=errset(pv("top-level" %s ?result \'designParamVals) t)' % _quote(name)]
            if name == 'temperature':
                lines += ['unless(crescPointValue && numberp(car(crescPointValue)) crescPointValue=errset(pv("top-level" "temp" ?result \'designParamVals) t))']
            lines += [
                      'if(crescPointValue && numberp(car(crescPointValue)) then',
                      'fprintf(crescPointPort "P\\t%d\\t%d\\t%%.17g\\n" car(crescPointValue))' % (index, variable_index),
                      'else', 'fprintf(crescPointPort "X\\t%d\\t%d\\tunavailable_design_parameter\\n")' % (index, variable_index),
                      'crescPointFailed=t', ')']
    mark('write_complete')
    lines += ['fprintf(crescPointPort "END\\t%d\\n")' % len(requests), 'close(crescPointPort)']
    mark('script_complete')
    if exit_after:
        lines.append('if(crescPointFailed then exit(3) else exit(0))')
    return '\n'.join(lines) + '\n'


def parse_point_parameter_records(path, token, requests):
    if not os.path.isfile(path) or os.path.islink(path) or os.path.getsize(path) > 8 * 1024 * 1024:
        raise NetlistMeasurementError('Native point parameter records are missing or invalid')
    with open(path, 'r', encoding='utf-8') as stream:
        rows = stream.read().splitlines()
    if not rows or rows[0] != PROTOCOL + '\t' + token or rows[-1] != 'END\t%d' % len(requests):
        raise NetlistMeasurementError('Native point parameter identity/termination is unproven')
    values, seen = [{} for unused in requests], set()
    missing = []
    for row in rows[1:-1]:
        fields = row.split('\t')
        if len(fields) != 4 or fields[0] not in ('P', 'X'):
            raise NetlistMeasurementError('Malformed native point parameter record')
        index, variable = int(fields[1]), int(fields[2])
        if not 0 <= index < len(requests) or not 0 <= variable < len(requests[index]['names']) or (index, variable) in seen:
            raise NetlistMeasurementError('Unexpected or duplicated native point parameter identity')
        seen.add((index, variable))
        name = requests[index]['names'][variable]
        if fields[0] == 'X':
            missing.append({'point_id': requests[index]['point_id'], 'test': requests[index]['test'], 'variable': name})
            continue
        number = float(fields[3])
        if not math.isfinite(number):
            raise NetlistMeasurementError('Nonfinite native point parameter value')
        values[index][name] = number
    if missing or len(seen) != sum(len(request['names']) for request in requests):
        raise NetlistMeasurementError('Required exact point parameters are unavailable in PSF', {'missing': missing})
    return values


def _read_in_session(session, script, token, budget, cancel_check):
    """Read after native Close; process termination remains the owner's job."""
    if not any(row['state'] == 'CLOSED' for row in session.status()):
        raise NetlistMeasurementError('Existing native session has not closed')
    with open(script, encoding='utf-8') as stream:
        body = stream.read()
    atomic_write_text(script, '\n'.join([
        'unless(equal(crescBaselineNonce %s) error("Point reader session nonce mismatch\\n"))' % _quote(session.nonce),
        'crescPointRead=errset(progn(', body, 't) t)',
        'if(crescPointRead then crescBaselineRecord("POINT_PARAMS_DONE" %s)' % _quote(token),
        'else crescBaselineRecord("POINT_PARAMS_ERROR" %s))' % _quote(token)]) + '\n')
    command = {'reader_transport': 'existing_native_baseline_ocean', 'execution_mode': 'same_owned_ocean',
               'native_session_nonce': session.nonce,
               'guardian_pid': session.state.get('guardian_pid'), 'process_exit_observed': False}
    end = time.monotonic() + budget
    session.send(script)
    while True:
        if cancel_check and cancel_check():
            return dict(command, returncode=None, cancelled=True)
        for row in session.status():
            if row['value'] == token and row['state'] in ('POINT_PARAMS_DONE', 'POINT_PARAMS_ERROR'):
                # This return code describes the script, never guardian death.
                return dict(command, returncode=0 if row['state'] == 'POINT_PARAMS_DONE' else 1)
        if session.process is None or session.process.poll() is not None:
            return dict(command, returncode=1, process_exit_observed=True)
        if time.monotonic() >= end:
            return dict(command, returncode=None, timed_out=True)
        time.sleep(0.1)


def read_point_parameters(requests, config, candidate_root, command_runner=None, timeout_seconds=180, deadline=None,
                          cancel_check=None, native_session=None):
    root = os.path.abspath(ensure_dir(os.path.join(candidate_root, 'native_point_parameters')))
    token = uuid.uuid4().hex
    script, records = os.path.join(root, 'read_point_parameters.il'), os.path.join(root, 'parameters.tsv')
    progress = os.path.join(root, 'reader_progress.tsv')
    paths = {'script_path': script, 'records_path': records, 'progress_path': progress,
             'stdout_path': os.path.join(root, 'ocean.stdout'), 'stderr_path': os.path.join(root, 'ocean.stderr')}
    if native_session is not None:
        paths.update(stdout_path=os.path.join(native_session.control, 'ocean.stdout'),
                     stderr_path=os.path.join(native_session.control, 'ocean.stderr'))
    binary = (config.get('cadence') or {}).get('ocean_bin')
    if native_session is None and (not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK)):
        raise NetlistMeasurementError('Ocean is unavailable for exact point parameter readback', paths)
    reader = (config.get("cadence") or {}).get("simulator_result_reader") or "spectre"
    from .netlist_measurement import _startup_lines
    atomic_write_json(os.path.join(root, 'reader_request.json'), {'token': token, 'requests': requests, 'reader': reader, 'paths': paths})
    atomic_write_text(progress, token + '\tscript_not_observed\n')
    startup_lines = _startup_lines(config) if native_session is None else []
    atomic_write_text(script, render_point_parameter_script(requests, records, token, reader, progress, startup_lines,
                                                           exit_after=native_session is None))
    # With no extensions, isolate the reader's CWD. Explicit extensions retain
    # the original candidate CWD because they may load relative paths.
    cwd = native_session.workspace['root'] if native_session is not None else (
        os.path.abspath(candidate_root) if startup_lines else ensure_dir(os.path.join(root, 'work')))
    budget = float(timeout_seconds)
    if deadline is not None:
        budget = min(budget, deadline - time.monotonic())
    if budget <= 0:
        raise NetlistMeasurementError('Exact point PSF parameter readback deadline exhausted before launch', paths)
    if native_session is not None:
        try:
            command = _read_in_session(native_session, script, token, budget, cancel_check)
        except Exception as error:
            # Preserve a diagnostic receipt even if stdin or protocol access
            # fails. The caller still performs owned-process cleanup.
            command = {'execution_mode': 'same_owned_ocean', 'returncode': None,
                       'reader_error': '%s: %s' % (type(error).__name__, error),
                       'cancelled': bool(getattr(error, 'cancelled', False))}
    else:
        env = child_environment(config, ensure_dir(os.path.join(root, 'home')))
        for name in list(env):
            if name.startswith('CDS_MPS_'):
                env.pop(name, None)
        command = run_owned_command([binary, '-nograph', '-restore', script], cwd=cwd, env=env,
                                    timeout_seconds=budget, control_root=root,
                                    token='native_point_params_' + token, command_runner=command_runner, cancel_check=cancel_check)
    stage = 'unknown'
    try:
        with open(progress, 'r', encoding='utf-8') as stream:
            row = stream.read(1024).strip().split('\t', 1)
        if len(row) == 2 and row[0] == token:
            stage = row[1]
    except (OSError, UnicodeError):
        pass
    records_present = os.path.isfile(records) and not os.path.islink(records)
    records_current = False
    if records_present:
        try:
            with open(records, 'r', encoding='utf-8') as stream:
                records_current = stream.readline(1024).rstrip('\r\n') == PROTOCOL + '\t' + token
        except (OSError, UnicodeError):
            pass
    detail = dict(paths, command=command, last_stage=stage, timeout_seconds=budget, cwd=cwd,
                  records_present=records_present, records_current_token=records_current)
    atomic_write_json(os.path.join(root, 'reader_status.json'), dict(detail, token=token))
    if command.get('returncode') != 0 or any(command.get(key) for key in ('timed_out', 'cancelled', 'termination_failed')):
        reason = ('timed out' if command.get('timed_out') else
                  'cancelled' if command.get('cancelled') else
                  'termination unproven' if command.get('termination_failed') else
                  command.get('reader_error') or 'exited unsuccessfully')
        error = NetlistMeasurementError('Exact point PSF parameter readback %s (last stage: %s; script: %s; records: %s)' %
                                        (reason, stage, script, records), detail)
        error.cancelled = bool(command.get('cancelled'))
        raise error
    try:
        values = parse_point_parameter_records(records, token, requests)
    except (NetlistMeasurementError, ValueError) as error:
        detail['record_error'] = str(error)
        detail['record_detail'] = getattr(error, 'detail', {})
        raise NetlistMeasurementError('Exact point PSF parameter records invalid: %s (script: %s; records: %s)' %
                                      (error, script, records), detail)
    atomic_write_json(os.path.join(root, 'point_identity.json'), {'schema_version': 'crescendo.native_point_parameters.v1',
        'param_source': 'current_psf_designParamVals', 'points': [dict(request, parameters=value) for request, value in zip(requests, values)],
        'command': command})
    return values
