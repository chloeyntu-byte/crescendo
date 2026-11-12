"""Small explicitly owned scratch directories; no HOME-wide scavenging.

A dead UI is not proof that its EDA children stopped. Cleanup therefore requires
an explicit producer-stop proof, or a previously sealed receipt. Registered
scratch is expendable; source designs, states and run proofs are never accepted.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import errno
import json
import os
import re
import shutil
import stat
import time
import uuid

SCHEMA = _contract_text('crescendo.scratch_owner.v1')
OWNER_FILE = _contract_text('.cr_scratch_owner.json')
DEFAULT_BUDGET_BYTES = 128 * 1024 * 1024
DEFAULT_MAX_FILES = 4096
_PREFIX = re.compile(_contract_text(r'^(?:launch_|browse_|job_policy_|probe_|crescendo-launch\.|cr_tmp_)'))


class ScratchSafetyError(ValueError):
    pass


def _process_start(pid):
    try:
        with open('/proc/%d/stat' % int(pid)) as stream:
            data = stream.read(8192)
        return data.rsplit(')', 1)[1].split()[19]
    except OSError as error:
        if error.errno == errno.ENOENT:
            return None
        raise ScratchSafetyError('scratch owner process identity is unavailable')
    except (ValueError, IndexError):
        raise ScratchSafetyError('scratch owner process identity is malformed')


def _root_info(path):
    path = os.path.abspath(path)
    if (os.path.realpath(path) != path or not _PREFIX.match(os.path.basename(path))):
        raise ScratchSafetyError(_contract_text('scratch path must be an explicit private Crescendo temporary directory'))
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ScratchSafetyError('scratch directory must be private and owned by this user')
    return path, info


def _read_owner(path):
    path, info = _root_info(path)
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    fd = os.open(os.path.join(path, OWNER_FILE), flags)
    try:
        identity = os.fstat(fd)
        if (not stat.S_ISREG(identity.st_mode) or identity.st_nlink != 1
                or identity.st_uid != os.getuid() or identity.st_size > 16384):
            raise ScratchSafetyError('scratch receipt is unsafe')
        with os.fdopen(fd, 'r') as stream:
            fd = None
            owner = json.load(stream)
    finally:
        if fd is not None:
            os.close(fd)
    if (owner.get('schema') != SCHEMA or owner.get('path') != path
            or owner.get('uid') != os.getuid()
            or owner.get('device') != info.st_dev or owner.get('inode') != info.st_ino
            or not isinstance(owner.get('token'), str) or len(owner['token']) != 32):
        raise ScratchSafetyError('scratch receipt does not bind to this directory')
    return owner


def register_scratch(path, kind='probe', budget_bytes=None):
    path, info = _root_info(path)
    if os.path.lexists(os.path.join(path, OWNER_FILE)):
        raise ScratchSafetyError('scratch directory is already registered')
    if budget_bytes is not None:
        enforce_scratch_budget(os.path.dirname(path), budget_bytes=budget_bytes)
    owner = {'schema': SCHEMA, 'token': uuid.uuid4().hex, 'path': path,
             'uid': os.getuid(), 'device': info.st_dev, 'inode': info.st_ino,
             'owner_pid': os.getpid(), 'owner_start': _process_start(os.getpid()),
             'kind': str(kind), 'sealed': False, 'created_at': time.time()}
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(os.path.join(path, OWNER_FILE), flags, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(owner, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    return owner


def _assert_writer(owner, token):
    if (token != owner['token'] or owner['owner_pid'] != os.getpid()
            or owner['owner_start'] != _process_start(os.getpid())):
        raise ScratchSafetyError('scratch producer stop proof is not from its owner')


def seal_scratch(path, token):
    """Call only after all producers are joined and will never write again."""
    owner = _read_owner(path)
    _assert_writer(owner, token)
    owner['sealed'] = True
    # Receipt is expendable. Atomic replacement is safe inside the same owned
    # directory; an interrupted seal never grants cleanup permission.
    target = os.path.join(path, OWNER_FILE)
    temporary = target + '.' + uuid.uuid4().hex + '.tmp'
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(owner, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return owner


def _tree_usage(path, max_files=DEFAULT_MAX_FILES):
    path, root_info = _root_info(path)
    files, byte_count = 0, 0
    for directory, dirs, names in os.walk(path, followlinks=False):
        for name in dirs + names:
            target = os.path.join(directory, name)
            info = os.lstat(target)
            if (info.st_dev != root_info.st_dev or info.st_uid != os.getuid()
                    or stat.S_ISLNK(info.st_mode)
                    or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode))
                    or (stat.S_ISREG(info.st_mode) and info.st_nlink != 1)):
                raise ScratchSafetyError('scratch has an unsafe entry: %s' % target)
            files += 1
            if files > max_files:
                raise ScratchSafetyError('scratch file-count budget exceeded')
            if stat.S_ISREG(info.st_mode):
                byte_count += info.st_size
    return {'files': files, 'bytes': byte_count}


def cleanup_scratch(path, token=None, producers_stopped=False):
    """Return accurate cleanup state. Never kill or guess producer ownership."""
    result = {'ok': False, 'state': 'unproven', 'path': os.path.abspath(path)}
    if not os.path.lexists(path):
        return dict(result, ok=True, state='absent')
    try:
        owner = _read_owner(path)
        if producers_stopped:
            _assert_writer(owner, token)
        elif not owner.get('sealed'):
            raise ScratchSafetyError('scratch producer termination has not been proven')
        elif _process_start(owner['owner_pid']) == owner['owner_start']:
            raise ScratchSafetyError('sealed scratch owner is still active')
        # Validate every entry before deletion; no source symlink, hardlink,
        # mount point or unknown device can pass this gate.
        usage = _tree_usage(path)
        result.update(usage)
        for attempt in range(2):
            try:
                # Keep the receipt until all body artifacts are deleted. An
                # interrupted cleanup remains attributable and retryable.
                for name in os.listdir(path):
                    if name == OWNER_FILE:
                        continue
                    target = os.path.join(path, name)
                    info = os.lstat(target)
                    if stat.S_ISDIR(info.st_mode):
                        shutil.rmtree(target)
                    else:
                        os.unlink(target)
                os.unlink(os.path.join(path, OWNER_FILE))
                try:
                    os.rmdir(path)
                except OSError:
                    # Do not falsely claim success if a writer/NFS residue
                    # appeared after the producer-stop boundary.
                    fd = os.open(os.path.join(path, OWNER_FILE), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, 'w') as stream:
                        json.dump(owner, stream)
                    raise
                return dict(result, ok=True, state='removed', attempts=attempt + 1)
            except OSError as error:
                if attempt or error.errno not in (errno.ENOTEMPTY, errno.EBUSY):
                    raise
                # Finite immediate retry; the caller can Retry Exit later.
        raise ScratchSafetyError('scratch cleanup did not complete')
    except (OSError, ValueError) as error:
        result['error'] = '%s: %s' % (error.__class__.__name__, error)
        result['state'] = 'artifact_cleanup_failed' if producers_stopped else 'unproven'
        try:
            result['remaining'] = sorted(os.listdir(path))[:32]
        except OSError:
            result['remaining'] = []
        return result


def enforce_scratch_budget(parent, budget_bytes=DEFAULT_BUDGET_BYTES,
                           extra_bytes=0, max_files=DEFAULT_MAX_FILES):
    """Bounded accounting of registered scratch only; unknown data is untouched."""
    total, files, checked = 0, 0, 0
    for name in os.listdir(parent):
        if not _PREFIX.match(name):
            continue
        path = os.path.join(parent, name)
        if not os.path.isfile(os.path.join(path, OWNER_FILE)):
            continue
        checked += 1
        if checked > 128:
            raise ScratchSafetyError(_contract_text('too many Crescendo scratch directories; close owned UIs and clean their proven scratch'))
        _read_owner(path)
        usage = _tree_usage(path, max_files=max_files)
        total += usage['bytes']
        files += usage['files']
        if total + extra_bytes > int(budget_bytes) or files > max_files:
            raise ScratchSafetyError(_contract_text('Crescendo temporary cache budget exceeded; simulation data belongs in the configured data directory'))
    return {'bytes': total, 'files': files, 'directories': checked, 'budget_bytes': int(budget_bytes)}


def reap_sealed_scratch(parent, max_directories=128):
    results = []
    for name in os.listdir(parent):
        if len(results) >= max_directories:
            break
        if not _PREFIX.match(name):
            continue
        path = os.path.join(parent, name)
        try:
            owner = _read_owner(path)
            if owner.get('sealed') and _process_start(owner['owner_pid']) != owner['owner_start']:
                results.append(cleanup_scratch(path))
        except (OSError, ValueError):
            continue
    return results
