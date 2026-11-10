"""Reconcile retained native candidates without legacy workspace deletion."""
from __future__ import print_function

import hashlib
import os
import re

from .utils import atomic_write_json, utc_timestamp


def reconcile_baseline_run(run_root, request_cancel=False, inspect_only=False):
    from .setup_private_session import (_load, _reconcile_owned_capture,
                                        preparation_session_reference, REFERENCE)
    from .native_baseline import SCHEMA_VERSION, _read_protocol
    from .legacy_guardian import inspect_legacy_guardian_attempt
    from .workspace import verify_sources_unchanged, compact_source_guard

    root = os.path.abspath(run_root)
    if os.path.realpath(root) != root or not os.path.isdir(root):
        raise ValueError('Baseline recovery root is not a real owned directory')
    for namespace in ('workers', 'controllers'):
        path = os.path.join(root, namespace)
        if os.path.lexists(path) and (os.path.islink(path) or not os.path.isdir(path) or os.listdir(path)):
            raise ValueError('Baseline recovery found an unrelated worker namespace; retained')
    candidates = os.path.join(root, 'candidates')
    rows = []
    expected_roots = set()
    index_path = os.path.join(root, 'baseline_shutdown_roots.json')
    if os.path.lexists(index_path):
        index = _load(index_path)
        if index.get('schema_version') != 'crescendo.maestro_baseline_shutdown_roots.v1' or not isinstance(index.get('roots'), list):
            raise ValueError('Baseline shutdown root index is invalid')
        for item in index['roots']:
            relative = item.get('candidate_root')
            if not isinstance(relative, str) or not re.fullmatch(r'candidates/g[0-9]+_c[0-9]+', relative):
                raise ValueError('Baseline shutdown root index escapes its candidate namespace')
            expected_roots.add(os.path.basename(relative))
    if os.path.lexists(candidates):
        if os.path.islink(candidates) or not os.path.isdir(candidates):
            raise ValueError('Baseline candidate namespace is unsafe')
        for name in sorted(os.listdir(candidates)):
            path = os.path.join(candidates, name)
            row = {'candidate_id': name, 'ok': False, 'raw_deleted': False,
                   'process_termination_proven': False, 'remote_stop_proven': False}
            try:
                if not re.fullmatch(r'g[0-9]+_c[0-9]+', name) or os.path.realpath(path) != path or not os.path.isdir(path):
                    raise ValueError('Baseline candidate identity/path is invalid')
                candidate = _load(os.path.join(path, 'candidate.json'))
                if candidate.get('candidate_id') != name or candidate.get('execution_backend') != 'maestro_baseline':
                    raise ValueError('Baseline candidate record has a different identity/backend')
                state_path = os.path.join(path, 'native_baseline', 'native_baseline_state.json')
                native = _load(state_path)
                observed_native = native
                if (native.get('schema_version') != SCHEMA_VERSION or native.get('candidate_id') != name
                        or native.get('request_root') != path):
                    raise ValueError('Baseline native state identity mismatch')
                no_launch = native.get('launch_attempted') is False
                owner = preparation_session_reference(path)
                if owner is None:
                    if not no_launch or os.path.lexists(os.path.join(path, 'native_baseline', 'legacy_guardian_launch.json')):
                        raise ValueError('Baseline native launch has no durable suite binding')
                    row.update(ok=True, process_termination_proven=True, remote_stop_proven=True,
                               native_terminal_state='not_launched', source_state='not_exposed_to_native')
                else:
                    if native.get('workspace_root') != owner['workspace_root']:
                        raise ValueError('Baseline workspace differs from its durable suite owner')
                    ref = _load(os.path.join(path, REFERENCE))
                    if ref.get('capture_kind') != 'native_baseline':
                        raise ValueError('Baseline recovery received another preparation protocol')
                    native = _reconcile_owned_capture(path, request_cancel=request_cancel,
                                                       timeout_seconds=12 if request_cancel else 0)
                    no_launch = native.get('launch_attempted') is False
                    observed_native = _load(state_path)
                    expected = dict(observed_native)
                    if no_launch:
                        # The native reconciler derives this safe prelaunch
                        # result without rewriting its durable intent record.
                        expected.update(cleanup_proven=True, cleanup_pending=False)
                    if native != expected:
                        raise ValueError('Baseline reconciliation differs from its durable native state')
                    local = native.get('local_cleanup') or {}
                    if not no_launch:
                        local = inspect_legacy_guardian_attempt(os.path.join(path, 'native_baseline'),
                            expected_workspace_root=owner['workspace_root'],
                            expected_token_identity={'legacy': True, 'token': native.get('nonce')})
                    process = bool(no_launch or (local.get('state') == 'dead'
                                   and (local.get('termination_proof') or {}).get('ok') is True))
                    protocol = [] if no_launch else _read_protocol(
                        os.path.join(path, 'native_baseline', 'native_baseline_protocol.tsv'), native.get('nonce'))
                    states = {item['state'] for item in protocol}
                    stop = native.get('native_stop') or {}
                    order = [item['state'] for item in protocol]
                    required = ['READY', 'RUN_BEGIN', 'RUN_ID', 'SIM_DONE', 'WAIT_RETURN', 'HISTORY', 'CLOSED']
                    # The callback may fire before Run returns its ID. Every
                    # other boundary has a strict order and a unique record.
                    normal = (all(order.count(state) == 1 for state in required)
                              and order.index('READY') < order.index('RUN_BEGIN')
                              and order.index('RUN_BEGIN') < min(order.index('RUN_ID'), order.index('SIM_DONE'))
                              and max(order.index('RUN_ID'), order.index('SIM_DONE')) < order.index('WAIT_RETURN')
                              < order.index('HISTORY') < order.index('CLOSED'))
                    complete = (native.get('native_session_closed') is True and
                                normal and not states.intersection({'STOP_BEGIN','STOP_DONE','STOP_ERROR','ERROR'})
                                and stop.get('requested') is False)
                    cancelled = (native.get('native_session_closed') is True and 'STOP_DONE' in states
                                 and order.count('STOP_BEGIN') == order.count('STOP_DONE') == 1
                                 and order.index('STOP_BEGIN') < order.index('STOP_DONE')
                                 and 'CLOSED' not in states and 'STOP_ERROR' not in states
                                 and stop.get('requested') is True and stop.get('remote_stop_proven') is True)
                    terminal = bool(native.get('cleanup_proven') is True and (no_launch or complete or cancelled))
                    workspace = _load(os.path.join(owner['workspace_root'], 'workspace.json'),
                                      max_bytes=128 * 1024 * 1024)
                    if workspace.get('setup_private_session') != owner:
                        raise ValueError('Baseline workspace owner metadata differs')
                    snapshots = workspace.get('source_snapshots')
                    libraries = workspace.get('source_libraries')
                    baseline = (workspace.get('setup') or {}).get('baseline') or {}
                    if (not isinstance(snapshots, dict) or not snapshots or not isinstance(libraries, dict)
                            or set(snapshots) != set(libraries) or not baseline.get('baseline_id')
                            or any(not isinstance(value, dict) or not value.get('fingerprint')
                                   or not isinstance(value.get('paths'), list) or not value['paths']
                                   for value in snapshots.values())):
                        raise ValueError('Baseline source snapshots or frozen setup identity are incomplete')
                    guard = compact_source_guard(verify_sources_unchanged(workspace))
                    row.update(process_termination_proven=process, remote_stop_proven=terminal,
                               native_terminal_state='not_launched' if no_launch else
                                   'completed' if complete else 'cancelled' if cancelled else 'unknown',
                               source_guard=guard, source_state='verified' if guard.get('ok') else 'changed_or_unverified',
                               suite_id=owner['session_id'], suite_root=owner['root'])
                    row['ok'] = bool(process and terminal and guard.get('ok') is True)
                    if not process:
                        row['reason'] = 'Owned local process termination remains unproven'
                    elif not terminal:
                        row['reason'] = 'Native/remote terminal state remains unproven; suite and evidence retained'
                    elif not guard.get('ok'):
                        row['reason'] = 'Source integrity is changed or unverified; evidence retained'
                current_native = _load(state_path)
                if current_native != observed_native:
                    raise ValueError('Baseline native state changed during recovery')
                import json
                row['native_state_sha256'] = hashlib.sha256(json.dumps(current_native, sort_keys=True).encode('utf-8')).hexdigest()
                row['native_state_hash_scope'] = 'canonical JSON snapshot'
            except Exception as error:
                row.update(ok=False, process_termination_proven=False, remote_stop_proven=False)
                row['reason'] = '%s: %s' % (type(error).__name__, error)
            rows.append(row)
    for missing in sorted(expected_roots.difference(row['candidate_id'] for row in rows)):
        rows.append({'candidate_id': missing, 'ok': False, 'raw_deleted': False,
                     'process_termination_proven': False, 'remote_stop_proven': False,
                     'reason': 'Registered Baseline candidate evidence is missing'})
    if not rows:
        rows.append({'candidate_id': None, 'ok': False, 'raw_deleted': False,
                     'process_termination_proven': False, 'remote_stop_proven': False,
                     'reason': 'No durable native candidate evidence; absence does not prove no launch'})
    result = {'schema_version': 'crescendo.baseline_recovery.v1',
              'state': 'inspected' if inspect_only else 'completed',
              'artifact_policy': 'retain_owned_suite_and_candidate_evidence', 'raw_deleted': False,
              'candidates': rows, 'retained_candidates': [row['candidate_id'] for row in rows],
              'process_termination_proven': all(row['process_termination_proven'] for row in rows),
              'remote_stop_proven': all(row['remote_stop_proven'] for row in rows),
              'artifact_cleanup_complete': all(row['ok'] for row in rows), 'observed_at': utc_timestamp()}
    if not result['artifact_cleanup_complete']:
        result['state'] = 'blocked'
    if not inspect_only:
        atomic_write_json(os.path.join(root, 'baseline_recovery.json'), result)
    if result['state'] == 'blocked':
        error = RuntimeError('Baseline recovery retained unresolved resources: ' + '; '.join(
            '%s: %s' % (row['candidate_id'], row.get('reason', 'unproven')) for row in rows if not row['ok']))
        error.detail = result
        error.cleanup_failed = True
        raise error
    return result
