"""Bounded, diagnostic-only reads from an owned native baseline storage root."""
from __future__ import print_function

import json
import math
import os
import re
import stat
import time

_SCHEMA = 'crescendo.native_storage_evidence.v1'
_STATE = 'crescendo.maestro_baseline.v1'
_TOKEN = re.compile(r'^[0-9a-f]{32}$')
# Traversal limits apply even if no interesting files exist.
_MAX_ENTRIES = 4096
_MAX_DIRS = 512
_MAX_DEPTH = 20
_METADATA_LIMIT = 65536
_REGISTRY_LIMIT = 2 * 1024 * 1024
_LOG_LIMIT = 128 * 1024
_SOLVER_TEXT = frozenset(('spectre.out', 'spectre.log', 'alps.out', 'psf.log', 'psf.warn'))


def _json_file(path, remaining, limit=_METADATA_LIMIT):
    if os.path.abspath(path) != os.path.realpath(path):
        raise ValueError('identity path contains a symbolic link')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size > limit or st.st_size > remaining:
            raise ValueError('identity file is not a bounded regular file')
        data = os.read(fd, min(limit + 1, remaining))
        if len(data) > limit:
            raise ValueError('identity file exceeds limit')
        return data
    finally:
        os.close(fd)


def _interesting(name):
    name = name.lower()
    return (name in _SOLVER_TEXT or name.endswith('.log') or name in ('stdout', 'stderr', 'submit', 'submit.txt',
            'stdout.txt', 'stderr.txt', 'run.stdout', 'run.stderr', 'submit.log'))


def collect_native_storage_evidence(candidate_root, max_bytes, max_files, deadline, cancelled=None):
    """Read only owned log text. deadline is an absolute monotonic time.

    Result is never evidence of submission, completion or remote termination.
    No links, raw data, OA, netlists or PSF data are read. Only named solver
    text logs may be read directly inside a PSF/raw directory.
    """
    result = dict(records=[], manifest=None, omissions=[], read_bytes=0, files_read=0,
                  status='unavailable', diagnostic_only=True)
    def omit(reason, path=None):
        item = {'reason': reason, 'diagnostic_only': True}
        if path is not None:
            item['path'] = path
        result['omissions'].append(item)
    def expired():
        return time.monotonic() >= deadline or (cancelled is not None and cancelled())
    try:
        max_bytes = min(4 * 1024 * 1024, max(0, int(max_bytes)))
        max_files = min(64, max(0, int(max_files)))
        candidate_root = os.path.abspath(candidate_root)
        if expired():
            omit('deadline_or_cancelled')
            return result
        if os.path.realpath(candidate_root) != candidate_root:
            raise ValueError('candidate root contains a symbolic link')
        control = os.path.join(candidate_root, 'native_baseline')
        def read_identity(path, limit=_METADATA_LIMIT):
            if expired() or result['files_read'] >= max_files:
                raise ValueError('identity read budget exhausted')
            data = _json_file(path, max_bytes - result['read_bytes'], limit)
            result['read_bytes'] += len(data)
            result['files_read'] += 1
            obj = json.loads(data.decode('utf-8'))
            if not isinstance(obj, dict):
                raise ValueError('identity file is not an object')
            return obj
        manifest = read_identity(os.path.join(control, 'native_storage_evidence.json'))
        state = read_identity(os.path.join(control, 'native_baseline_state.json'))
        result['manifest'] = manifest
        if manifest.get('schema_version') != _SCHEMA or state.get('schema_version') != _STATE:
            raise ValueError('schema mismatch')
        if not _TOKEN.match(str(manifest.get('nonce') or '')):
            raise ValueError('invalid nonce')
        for key in ('nonce', 'candidate_id', 'request_root', 'workspace_root'):
            if not manifest.get(key) or manifest.get(key) != state.get(key):
                raise ValueError('state identity mismatch: ' + key)
        if manifest['request_root'] != candidate_root or manifest['candidate_id'] != os.path.basename(candidate_root):
            raise ValueError('candidate identity mismatch')
        workspace = manifest['workspace_root']
        storage = manifest.get('storage_root')
        if not isinstance(workspace, str) or not isinstance(storage, str):
            raise ValueError('missing workspace or storage root')
        if os.path.abspath(workspace) != workspace or os.path.realpath(workspace) != workspace:
            raise ValueError('unsafe workspace root')
        parent = os.path.join(workspace, 'native_baseline_runs')
        if (os.path.dirname(storage) != parent or not _TOKEN.match(os.path.basename(storage))
                or os.path.realpath(storage) != storage):
            raise ValueError('unsafe storage root')
        # Same durable ownership and capture registration checks as
        # preparation_session_reference, with all identity reads budgeted.
        ref = read_identity(os.path.join(candidate_root, 'setup_private_session_reference.json'))
        session_root = ref.get('root')
        session_id = ref.get('session_id')
        if (ref.get('schema_version') != 'crescendo.setup_private_session.v1'
                or not isinstance(session_root, str) or not os.path.isabs(session_root)
                or not isinstance(session_id, str) or not _TOKEN.match(session_id)
                or os.path.basename(session_root) != session_id
                or os.path.basename(os.path.dirname(session_root)) != 'setup_private_sessions'
                or os.path.realpath(session_root) != session_root
                or ref.get('capture_root') != candidate_root
                or ref.get('capture_kind') != 'native_baseline'):
            raise ValueError('private session reference mismatch')
        owner = read_identity(os.path.join(session_root, 'session_owner.json'))
        keys = ('schema_version', 'session_id', 'root', 'workspace_root', 'owner_uid')
        if (any(ref.get(key) != owner.get(key) for key in keys)
                or owner.get('workspace_root') != os.path.join(session_root, 'workspace')
                or owner.get('workspace_root') != workspace or owner.get('owner_uid') != os.getuid()):
            raise ValueError('private session owner mismatch')
        registry = read_identity(os.path.join(session_root, 'session_state.json'), _REGISTRY_LIMIT)
        if registry.get('session_id') != session_id or candidate_root not in registry.get('capture_roots', []):
            raise ValueError('capture is absent from private session registry')
        if expired():
            omit('deadline_or_cancelled')
            return result
        flags = os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0)
        root_fd = os.open(storage, flags)
        try:
            st = os.fstat(root_fd)
            identity = manifest.get('storage_identity') or {}
            if not stat.S_ISDIR(st.st_mode) or st.st_uid != owner['owner_uid'] or any(identity.get(k) != v for k, v in
                    (('dev', st.st_dev), ('ino', st.st_ino), ('uid', st.st_uid))):
                raise ValueError('storage directory identity mismatch')
            if not isinstance(manifest.get('native_readback_verified'), bool):
                raise ValueError('missing native readback status')
            created = float(manifest['created_epoch'])
            if (not math.isfinite(created) or not math.isfinite(float(state['started_epoch']))
                    or created != float(state['started_epoch'])):
                raise ValueError('creation time does not match baseline state')
            result['status'] = 'ok'
            if not manifest['native_readback_verified']:
                omit('native_storage_readback_unverified')
            counts = {'entries': 0, 'dirs': 0}
            candidates = []
            def visit(fd, parts, depth, solver_text_only=False):
                rel = 'native_storage' + ('/' + '/'.join(parts) if parts else '')
                if expired():
                    omit('deadline_or_cancelled', rel)
                    return
                if depth > _MAX_DEPTH or counts['dirs'] >= _MAX_DIRS:
                    omit('directory_limit', rel)
                    return
                counts['dirs'] += 1
                try:
                    entries = os.scandir('/proc/self/fd/%d' % fd)
                    try:
                        for entry in entries:
                            name = entry.name
                            path = rel + '/' + name
                            if expired():
                                omit('deadline_or_cancelled', path)
                                return
                            counts['entries'] += 1
                            if counts['entries'] > _MAX_ENTRIES:
                                omit('entry_limit', path)
                                return
                            if solver_text_only and name.lower() not in _SOLVER_TEXT:
                                continue
                            try:
                                child = os.stat(name, dir_fd=fd, follow_symlinks=False)
                                if stat.S_ISLNK(child.st_mode):
                                    omit('symbolic_link_skipped', path)
                                elif child.st_dev != st.st_dev or child.st_uid != owner['owner_uid']:
                                    omit('different_device_or_owner', path)
                                elif stat.S_ISDIR(child.st_mode):
                                    solver_directory = name.lower() in ('psf', 'raw') or name.lower().endswith(('.raw', '.psf'))
                                    if solver_directory or name.lower() == 'oa' or name.lower().endswith(('.oa', '.sdb')):
                                        result['records'].append({'path': path, 'kind': 'directory_index', 'diagnostic_only': True})
                                        if not solver_directory or solver_text_only:
                                            continue
                                    cfd = os.open(name, flags, dir_fd=fd)
                                    try:
                                        actual = os.fstat(cfd)
                                        if (not os.path.samestat(child, actual) or actual.st_dev != st.st_dev
                                                or actual.st_uid != owner['owner_uid']):
                                            omit('directory_changed', path)
                                        else:
                                            visit(cfd, parts + [name], depth + 1, solver_text_only=solver_directory)
                                    finally:
                                        os.close(cfd)
                                elif stat.S_ISREG(child.st_mode) and _interesting(name):
                                    candidates.append((parts + [name], child))
                                elif stat.S_ISREG(child.st_mode) and name.lower().endswith(('.scs', '.sp', '.cir')):
                                    result['records'].append({'path': path, 'kind': 'netlist_index', 'diagnostic_only': True})
                            except OSError:
                                omit('entry_unreadable_or_changed', path)
                    finally:
                        entries.close()
                except OSError:
                    omit('directory_unreadable', rel)
            visit(root_fd, [], 0)
            if not candidates:
                omit('no_log_files_observed', 'native_storage')
            def priority(item):
                name = item[0][-1].lower()
                return (0 if name == 'run.log' or (name.startswith('job') and name.endswith('.log'))
                        or name == 'icrp.log' or name.startswith('submit') else 1, '/'.join(item[0]))
            for parts, observed in sorted(candidates, key=priority):
                path = 'native_storage/' + '/'.join(parts)
                if expired():
                    omit('deadline_or_cancelled', path)
                    break
                if result['files_read'] >= max_files or result['read_bytes'] >= max_bytes:
                    omit('file_or_byte_limit', path)
                    continue
                if observed.st_nlink != 1:
                    omit('hard_link_skipped', path)
                    continue
                parent_fd = os.dup(root_fd)
                try:
                    for component in parts[:-1]:
                        next_fd = os.open(component, flags, dir_fd=parent_fd)
                        os.close(parent_fd)
                        parent_fd = next_fd
                        parent_st = os.fstat(parent_fd)
                        if parent_st.st_dev != st.st_dev or parent_st.st_uid != owner['owner_uid']:
                            raise ValueError('directory ownership changed')
                    f = os.open(parts[-1], os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0), dir_fd=parent_fd)
                    try:
                        actual = os.fstat(f)
                        if (not stat.S_ISREG(actual.st_mode) or actual.st_nlink != 1
                                or actual.st_dev != st.st_dev or actual.st_uid != owner['owner_uid']
                                or not os.path.samestat(observed, actual)):
                            omit('file_changed', path)
                            continue
                        allowance = min(_LOG_LIMIT, max_bytes - result['read_bytes'])
                        truncated = actual.st_size > allowance
                        head_len = (allowance + 1) // 2 if truncated else allowance
                        tail_len = allowance - head_len if truncated else 0
                        first = os.read(f, head_len)
                        chunks = [(0, first)]
                        result['files_read'] += 1
                        result['read_bytes'] += len(first)
                        if tail_len and not expired():
                            offset = max(head_len, actual.st_size - tail_len)
                            os.lseek(f, offset, os.SEEK_SET)
                            last = os.read(f, tail_len)
                            result['read_bytes'] += len(last)
                            chunks.append((offset, last))
                        count = sum(len(data) for _, data in chunks)
                        if any(b'\0' in data for _, data in chunks):
                            omit('binary_content_skipped', path)
                            continue
                        samples = [{'offset': off, 'length': len(data), 'text': data.decode('utf-8', 'replace')}
                                   for off, data in chunks]
                        scanned_end = chunks[0][0] + len(chunks[0][1])
                        missing = []
                        for off, data in chunks[1:]:
                            if off > scanned_end:
                                missing.append({'start': scanned_end, 'end': off})
                            scanned_end = off + len(data)
                        if scanned_end < actual.st_size:
                            missing.append({'start': scanned_end, 'end': actual.st_size})
                        record = {'path': path, 'kind': 'text_log', 'text': samples[0]['text'], 'samples': samples,
                                  'size': actual.st_size, 'read_bytes': count, 'truncated': bool(missing),
                                  'unscanned_ranges': missing, 'hash_scope': 'not_computed',
                                  'mtime': actual.st_mtime, 'older_than_manifest': actual.st_mtime < created,
                                  'diagnostic_only': True}
                        result['records'].append(record)
                        if missing:
                            omit('file_partially_scanned', path)
                    finally:
                        os.close(f)
                except (OSError, ValueError):
                    omit('entry_unreadable_or_changed', path)
                finally:
                    os.close(parent_fd)
        finally:
            os.close(root_fd)
        if result['omissions']:
            result['status'] = 'partial'
    except (OSError, ValueError, TypeError, KeyError) as error:
        omit('identity_or_storage_unavailable: ' + str(error))
        result['status'] = 'unavailable'
    return result
