"""Read-only isolation checks and evidence-preserving, exact-view recycling.

Checks never signal processes or change OA locks. A busy suite's metadata stays
at its live path; the recycle receipt records an immutable metadata snapshot.
"""
import errno
import fcntl
import os
import stat
import time
import uuid

from .runtime import bind_text
from .utils import atomic_write_json, _fsync_directory

_contract_text = bind_text()
RECEIPT = _contract_text('suite_verification.json')
RECYCLE = _contract_text('suite_recycle.json')


def _sessions():
    from . import setup_private_session
    return setup_private_session


def _reason(code, message, **evidence):
    return dict(code=code, message=message, **evidence)


def _lease(root):
    """Open an existing lease only; verification must not manufacture proof."""
    from .private_view_access import _object_identity
    path = os.path.join(root, 'suite.lease')
    before = _object_identity(path)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError('Suite lease is not a regular file')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    try:
        if not os.path.samestat(before, os.fstat(fd)):
            raise ValueError('Suite lease changed while opening')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _native_checks(owner, state):
    s = _sessions()
    from .legacy_guardian import inspect_legacy_guardian_attempt
    from .private_netlist import PREPARATION_PHASES
    checks, reasons = [], []

    def inspect(control):
        proof = inspect_legacy_guardian_attempt(control, expected_workspace_root=owner['workspace_root'])
        identities = []
        # Supplement the guardian's terminal/group proof with direct recorded
        # PID/start-time checks. A reused or unreadable PID is never evidence of
        # absence, even when an older terminal receipt happens to exist.
        state_path = proof.get('state_path')
        if state_path and os.path.lexists(state_path):
            try:
                from .legacy_guardian import _current_boot_id, _process_stat
                native = s._load(state_path)
                boot = native.get('launch_boot_id') or native.get('boot_id')
                current_boot = _current_boot_id()
                prior_boot = bool(boot and current_boot and boot != current_boot)
                for pid_key, ticks_key in (('guardian_pid', 'guardian_start_ticks'), ('pid', 'pid_start_ticks')):
                    pid, ticks = native.get(pid_key), native.get(ticks_key)
                    if pid is None:
                        continue
                    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
                        raise ValueError('Native PID record is invalid')
                    observed = None if prior_boot else _process_stat(pid)
                    identities.append({'pid': pid, 'start_ticks': ticks, 'boot_id': boot,
                                       'prior_boot': prior_boot, 'observed': observed})
                    if prior_boot:
                        continue
                    if observed:
                        if not isinstance(ticks, int) or observed.get('start_ticks') != ticks:
                            reasons.append(_reason('native_identity_unproven', 'Recorded PID has a different start time.', pid=pid))
                        elif observed.get('state') != 'Z':
                            reasons.append(_reason('native_process_alive', 'Recorded native process is still alive.', pid=pid))
                    elif os.path.lexists('/proc/%d' % pid):
                        reasons.append(_reason('native_identity_unproven', 'Recorded PID cannot be inspected.', pid=pid))
            except (OSError, ValueError, TypeError) as error:
                reasons.append(_reason('native_identity_unproven', str(error), control=control))
        checks.append(dict(control=control, proof=proof, identities=identities))
        if proof.get('state') != 'dead':
            code = 'native_process_alive' if proof.get('state') == 'active' else 'native_identity_unproven'
            reasons.append(_reason(code, 'Native process stop or exact process identity is unproven.', control=control))

    for control in state.get('operations') or []:
        inspect(control)
    for root in state.get('capture_roots') or []:
        try:
            if os.path.realpath(root) != root:
                raise ValueError('Capture evidence path is unsafe')
            if not os.path.lexists(root):
                if (state.get('capture_proofs') or {}).get(root, {}).get('cleanup_proven') is not True:
                    raise ValueError('Capture evidence is missing')
                checks.append(dict(root=root, proof=(state.get('capture_proofs') or {})[root]))
                continue
            if not os.path.isdir(root):
                raise ValueError('Capture evidence root is not a directory')
            if s.preparation_session_reference(root) != owner:
                raise ValueError('Capture reference differs from suite registry')
            kind = (state.get('capture_kinds') or {}).get(root)
            reference = s._load(os.path.join(root, s.REFERENCE))
            if kind not in (None, 'native_baseline') or reference.get('capture_kind') != kind:
                raise ValueError('Capture kind differs from suite registry')
            if kind == 'native_baseline':
                control = os.path.join(root, 'native_baseline')
                if not os.path.isdir(control) or os.path.realpath(control) != control:
                    raise ValueError('Native session evidence directory is unsafe')
                path = os.path.join(control, _contract_text('native_baseline_state.json'))
                proof = s._load(path)
                from .native_baseline import SCHEMA_VERSION
                if proof.get('schema_version') != SCHEMA_VERSION or proof.get('request_root') != root:
                    raise ValueError('Native session evidence differs from capture registry')
                checks.append(dict(root=root, proof=proof))
                if not isinstance(proof.get('launch_attempted'), bool):
                    raise ValueError('Native launch state is incomplete')
                if proof['launch_attempted']:
                    inspect(control)
                    if not (proof.get('cleanup_proven') and proof.get('native_session_closed')):
                        reasons.append(_reason('native_session_unproven', 'Native session or remote work has no terminal proof.', root=root))
            else:
                for phase in PREPARATION_PHASES:
                    control = os.path.join(root, phase)
                    if not os.path.lexists(control):
                        continue
                    if not os.path.isdir(control) or os.path.realpath(control) != control:
                        raise ValueError('Capture phase evidence directory is unsafe')
                    launch = os.path.join(control, _contract_text('legacy_guardian_launch.json'))
                    if os.path.lexists(launch):
                        inspect(control)
                    elif any(os.path.lexists(os.path.join(control, name)) for name in
                             (_contract_text('guardian_state.json'), _contract_text('command.json'), 'ocean.stdout', 'native_capture_ready')):
                        raise ValueError('Native launch manifest is missing')
        except (OSError, ValueError, TypeError) as error:
            reasons.append(_reason('native_identity_unproven', str(error), root=root))
    if state.get('unsafe') and not (state.get('operations') or state.get('capture_roots')):
        reasons.append(_reason('native_identity_unproven', 'Unsafe suite has no recorded native stop evidence.'))
    return checks, reasons


def assess(owner, state, workspace, lease_held=False):
    """No writes, signals, permission changes or native Stop requests."""
    from .private_view_access import _inventory
    checks = {'native_processes': [], 'oa_locks': [], 'lease': {}, 'ownership': []}
    reasons = []
    fd = None
    try:
        if not lease_held:
            fd = _lease(owner['root'])
        checks['lease'] = {'active': False, 'path': os.path.join(owner['root'], 'suite.lease')}
    except OSError as error:
        code = 'lease_active' if error.errno in (errno.EAGAIN, errno.EACCES) else 'lease_unproven'
        reasons.append(_reason(code, 'Suite lease is held or cannot be verified.', error=str(error)))
        checks['lease'] = {'active': True, 'error': str(error)}
    except ValueError as error:
        reasons.append(_reason('lease_unproven', str(error)))
    try:
        checks['native_processes'], native_reasons = _native_checks(owner, state)
        reasons.extend(native_reasons)
        seen_paths = set()
        for row in workspace.get('same_library_registry') or []:
            try:
                if row.get('path') in seen_paths:
                    raise ValueError('Duplicate view in suite registry')
                seen_paths.add(row.get('path'))
                member = dict(workspace, same_library_registry=[row])
                unused, locks, views = _inventory(member)
                if not views:
                    raise ValueError('Registered view is missing: ' + str(row.get('path')))
                checks['ownership'].append({'path': row['path'], 'ok': True})
                checks['oa_locks'].extend(locks)
            except (OSError, ValueError, KeyError, TypeError) as error:
                checks['ownership'].append({'path': row.get('path'), 'ok': False, 'error': str(error)})
                reasons.append(_reason('marker_mismatch', str(error), path=row.get('path')))
        if checks['oa_locks']:
            reasons.append(_reason('oa_lock_held', 'OA lock presence requires release by its original holder.', locks=checks['oa_locks']))
        if state.get('refresh_pending') or state.get('restore_pending') or _sessions()._pending_creation_stages(owner, workspace):
            reasons.append(_reason('recovery_pending', 'An interrupted view update requires explicit recovery.'))
    finally:
        if fd is not None:
            _sessions()._release_lease(fd)
    return {'checks': checks, 'blocked_reasons': reasons, 'passed': not reasons}


def verify(reference):
    """Publish a released state only while holding the suite's exclusive lease."""
    s = _sessions()
    owner, state, workspace = s._read_suite(reference, deep=False, require_snapshot=False)
    if os.path.lexists(os.path.join(owner['root'], RECYCLE)):
        return None
    quarantine = s._quarantine(owner)
    if not (quarantine or (state.get('unsafe') and state.get('status') != 'delete_incomplete')):
        return None
    fd = None
    try:
        try:
            fd = _lease(owner['root'])
        except (OSError, ValueError):
            return assess(owner, state, workspace)
        # Reread under the lease, including a committed metadata transaction.
        owner, state, workspace = s._read_suite(owner, deep=False, require_snapshot=False)
        result = assess(owner, state, workspace, lease_held=True)
        if not result['passed']:
            return result
        receipt = dict(result, schema_version=_contract_text('crescendo.suite_verification.v1'),
                       time=time.time(), owner=owner, quarantine=quarantine, state='released')
        archive = None
        if os.path.lexists(os.path.join(owner['root'], s.QUARANTINE)):
            archive = os.path.join(owner['root'], s.QUARANTINE + '.verified-' + uuid.uuid4().hex)
        receipt['quarantine_archive'] = archive
        # Persist evidence before clearing either the durable or memory fence.
        atomic_write_json(os.path.join(owner['root'], RECEIPT), receipt)
        state = dict(state, status='released', unsafe=False, closed=True)
        revision = uuid.uuid4().hex
        state['_suite_revision'] = workspace['_suite_revision'] = revision
        atomic_write_json(os.path.join(owner['root'], _contract_text('suite_metadata_commit.json')), {
            'schema_version': _contract_text('crescendo.suite_metadata_commit.v1'), 'owner': owner,
            'revision': revision, 'state': state, 'workspace': workspace})
        atomic_write_json(os.path.join(owner['root'], _contract_text('session_state.json')), state)
        atomic_write_json(os.path.join(owner['workspace_root'], _contract_text('workspace.json')), workspace)
        if archive:
            os.rename(os.path.join(owner['root'], s.QUARANTINE), archive)
            _fsync_directory(owner['root'])
        with s._REGISTRY_LOCK:
            s._QUARANTINED.discard(owner['session_id'])
        return result
    finally:
        if fd is not None:
            s._release_lease(fd)


def _directory(path):
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        pass
    info = os.lstat(path)
    if (not stat.S_ISDIR(info.st_mode) or os.path.realpath(path) != path or info.st_uid != os.getuid()):
        raise ValueError('Recycle directory ownership or path is invalid')
    return path


def recycle(reference):
    """Serialize administrative recycling separately from a live native lease."""
    s = _sessions()
    owner = s._owner(reference)
    path = os.path.join(owner['root'], 'suite.recycle.lease')
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise ValueError('Recycle lease identity is invalid')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _recycle_locked(owner)
    finally:
        os.close(fd)


def _recycle_locked(reference):
    """Retain locked/unproven members; rename only exact idle unlocked trees.

    Records are snapshotted, not relocated out from under live writers/capture
    references. A durable tombstone removes the suite from the active list.
    """
    s = _sessions()
    owner, state, workspace = s._read_suite(reference, deep=False, require_snapshot=False)
    pointer = os.path.join(owner['root'], RECYCLE)
    if os.path.lexists(pointer):
        previous = s._load(pointer)
        if previous.get('owner') != owner:
            raise ValueError('Recycle receipt owner differs from registry')
        directory = previous['recycle_root']
        if (os.path.realpath(directory) != directory or
                os.path.dirname(os.path.dirname(directory)) != os.path.dirname(os.path.dirname(owner['root'])) or
                os.path.basename(os.path.dirname(directory)) != 'quarantine_recycle'):
            raise ValueError('Recycle receipt path differs from storage root')
        receipt_path = os.path.join(directory, 'receipt.json')
        receipt = s._load(receipt_path)
        if receipt.get('owner') != owner:
            raise ValueError('Recycle archive owner differs from registry')
        # Reconcile a crash between rename and receipt publication without
        # moving anything else or trusting just the destination's pathname.
        changed = False
        for entry in receipt.get('views') or []:
            if entry.get('action') != 'move_pending':
                continue
            destination = entry.get('destination') or ''
            if os.path.dirname(destination) != directory:
                raise ValueError('Recycle destination escapes archive')
            expected = entry.get('object_identity')
            if os.path.lexists(destination):
                info = os.lstat(destination)
                if [info.st_dev, info.st_ino] != expected or os.path.lexists(entry['path']):
                    raise ValueError('Pending recycle rename identity is ambiguous')
                entry['action'] = 'moved'
                if entry['path'] not in receipt['removed']:
                    receipt['removed'].append(entry['path'])
            else:
                entry.update(action='retained', reason='Interrupted before rename; original view retained')
                if entry['path'] not in receipt['retained']:
                    receipt['retained'].append(entry['path'])
            changed = True
        recorded = {entry.get('path') for entry in receipt.get('views') or []}
        for member in workspace.get('same_library_registry') or []:
            if member.get('path') not in recorded:
                receipt['views'].append({'path': member.get('path'), 'target': member.get('target'),
                    'action': 'retained', 'reason': 'Interrupted before inspection; original view retained'})
                receipt['retained'].append(member.get('path'))
                changed = True
        if changed or receipt.get('complete') is not True:
            receipt['complete'] = True
            receipt['cleanup_proven'] = not receipt['retained'] and len(receipt['views']) == len(workspace.get('same_library_registry') or [])
            receipt['warnings'] = ['Interrupted recycle reconciled; inspect retained members and original metadata.']
            atomic_write_json(receipt_path, receipt)
        return receipt
    fd = None
    try:
        try:
            fd = _lease(owner['root'])
        except (OSError, ValueError):
            pass
        if fd is not None:
            owner, state, workspace = s._read_suite(owner, deep=False, require_snapshot=False)
        result = assess(owner, state, workspace, lease_held=fd is not None)
        base = _directory(os.path.join(os.path.dirname(os.path.dirname(owner['root'])), 'quarantine_recycle'))
        directory = _directory(os.path.join(base, time.strftime('%Y%m%d-%H%M%S') + '_' + owner['session_id'] + '_' + uuid.uuid4().hex[:8]))
        records = _directory(os.path.join(directory, 'records'))
        # Copy bounded, validated JSON only. Never traverse a tree with locks.
        for parent, names in ((owner['root'], (s.OWNER, s.QUARANTINE, _contract_text('session_state.json'),
                                              _contract_text('suite_metadata_commit.json'), RECEIPT)),
                              (owner['workspace_root'], (_contract_text('workspace.json'), _contract_text('same_library_views.json')))):
            for name in names:
                path = os.path.join(parent, name)
                if os.path.lexists(path):
                    atomic_write_json(os.path.join(records, name), s._load(path, max_bytes=256 * 1024 * 1024))
        receipt = dict(schema_version=_contract_text('crescendo.suite_recycle.v1'), owner=owner,
                       time=time.time(), state='recycled', recycled=True, cleanup_proven=False, complete=False, durable=True,
                       recycle_root=directory, retained_metadata_root=directory, checks=result['checks'],
                       blocked_reasons=result['blocked_reasons'], removed=[], retained=[], views=[],
                       metadata_policy='snapshot; live registry retained as tombstone', warnings=[])
        atomic_write_json(os.path.join(directory, 'receipt.json'), receipt)
        atomic_write_json(pointer, {'owner': owner, 'recycle_root': directory})
        from .private_view_access import _inventory
        native_blocked = any(row['code'].startswith('native_') for row in result['blocked_reasons'])
        for index, row in enumerate(workspace.get('same_library_registry') or []):
            entry = {'path': row.get('path'), 'target': row.get('target'), 'action': 'retained'}
            try:
                if fd is None or native_blocked:
                    raise ValueError('Active or unproven writer; live view retained')
                unused, locks, views = _inventory(dict(workspace, same_library_registry=[row]))
                if locks:
                    entry['locks'] = locks
                    raise ValueError('OA locks retained for their original holder')
                if not views:
                    raise ValueError('Registered view is missing')
                destination = os.path.join(directory, 'view_%d' % index)
                # A same-filesystem rename preserves bytes and permissions and
                # never deletes contents. Cross-device trees stay where they are.
                info = os.lstat(row['path'])
                entry.update(destination=destination, action='move_pending', object_identity=[info.st_dev, info.st_ino])
                receipt['views'].append(entry)
                atomic_write_json(os.path.join(directory, 'receipt.json'), receipt)
                # Recheck immediately before rename; no lock is bypassed.
                unused, locks, views = _inventory(dict(workspace, same_library_registry=[row]))
                if locks:
                    entry['locks'] = locks
                    raise ValueError('OA lock appeared before recycle')
                os.rename(row['path'], destination)
                entry['action'] = 'moved'
                receipt['removed'].append(row['path'])
                try:
                    _fsync_directory(os.path.dirname(row['path']))
                    _fsync_directory(directory)
                except OSError as error:
                    # The rename already happened: do not misreport the old
                    # path as retained if only durability confirmation failed.
                    entry['durability_warning'] = str(error)
                    receipt['durable'] = False
                    receipt['warnings'].append('Archive rename durability is unconfirmed: ' + str(error))
            except (OSError, ValueError, KeyError, TypeError) as error:
                entry.update(action='retained', reason=str(error))
                receipt['retained'].append(row.get('path'))
            if entry not in receipt['views']:
                receipt['views'].append(entry)
            atomic_write_json(os.path.join(directory, 'receipt.json'), receipt)
        receipt['complete'] = True
        receipt['cleanup_proven'] = not receipt['retained']
        if receipt['retained']:
            receipt['warnings'].append('Some live views remain at their original paths; see the recycle receipt.')
        atomic_write_json(os.path.join(directory, 'receipt.json'), receipt)
        return receipt
    finally:
        if fd is not None:
            s._release_lease(fd)
