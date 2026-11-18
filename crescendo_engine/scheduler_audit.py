"""Durable query originals; hashes carry audit identity, never Stop authority."""
import hashlib
import base64
import json
import os

from .runtime import bind_text
_text = bind_text()


def append_query(record, query, observation, task_root, final=False, prefix='', source_stem='scheduler_status'):
    from .run_data_store import _encoded, _open_read, _safe_stat, RunDataError, _fsync_dir
    log = record.get('query_log') or {}
    name = prefix + 'scheduler_queries.jsonl'
    path = os.path.join(task_root, name)
    previous_bytes = log.get('bytes', 0)
    if os.path.realpath(task_root) != os.path.abspath(task_root):
        raise RunDataError('unsafe scheduler audit directory')
    flags = os.O_WRONLY | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0)
    if previous_bytes:
        if _safe_stat(path).st_size != previous_bytes:
            raise RunDataError('scheduler audit log length changed')
    else:
        flags |= os.O_CREAT | os.O_EXCL
    # Archive complete command files when available, falling back to the runner's
    # result for injected runners. Parsing and proof checks happen before this.
    original = dict(query)
    for stream in ('stdout', 'stderr'):
        source = os.path.join(task_root, source_stem + '.' + stream)
        if os.path.lexists(source):
            with _open_read(source) as handle:
                raw = handle.read(64 * 1024 * 1024 + 1)
            if len(raw) > 64 * 1024 * 1024:
                raise RunDataError('scheduler query original exceeds audit limit')
            try:
                original[stream + '_tail'] = raw.decode('utf-8')
            except UnicodeError:
                original[stream + '_base64'] = base64.b64encode(raw).decode('ascii')
                original[stream + '_tail'] = raw.decode('utf-8', errors='replace')
    entry = {'index': record['query_count'], 'query': original,
             'observation_sha256': hashlib.sha256(_encoded(observation)).hexdigest()}
    raw = _encoded(entry) + b'\n'
    fd = os.open(path, flags, 0o600)
    try:
        before = os.fstat(fd)
        if before.st_size != previous_bytes or before.st_nlink != 1:
            raise RunDataError('scheduler audit log changed')
        offset = 0
        while offset < len(raw):
            amount = os.write(fd, raw[offset:])
            if amount <= 0:
                raise RunDataError('scheduler audit write did not advance')
            offset += amount
        os.fsync(fd)
    finally:
        os.close(fd)
    if not previous_bytes:
        _fsync_dir(task_root)
    # Hash the append-only original segment. Its verified copy is committed to
    # the dataset before the point/checkpoint ACK permits directory cleanup.
    with _open_read(path) as stream:
        segment = stream.read()
    if len(segment) != previous_bytes + len(raw):
        raise RunDataError('scheduler audit segment changed')
    entries = list(log.get('entries') or [])
    entries.append({'index': record['query_count'], 'offset': previous_bytes, 'bytes': len(raw),
                    'sha256': hashlib.sha256(raw).hexdigest(), 'state': observation.get('state'),
                    'terminal': observation.get('terminal'), 'invalid': observation.get('invalid')})
    record['query_log'] = {'schema_version': _text('crescendo.scheduler_query_log.v2'),
        'path': path, 'bytes': len(segment), 'sha256': hashlib.sha256(segment).hexdigest(), 'entries': entries}
    transitions = record.setdefault('state_transitions', [])
    state = {key: observation.get(key) for key in ('state', 'terminal', 'invalid', 'not_found', 'reason')}
    if not transitions or transitions[-1]['state'] != state:
        transitions.append({'index': record['query_count'], 'state': state})
    first = record.get('first_query')
    current = {'index': record['query_count'], 'query': original}
    record['first_query'] = first or current
    record['last_query'] = current
    record['omitted_query_count'] = max(0, record['query_count'] - 2)


def verify_segment(log, text):
    from .run_data_store import RunDataError
    raw = text.encode('utf-8')
    if (type(log.get('bytes')) is not int or len(raw) != log['bytes']
            or hashlib.sha256(raw).hexdigest() != log.get('sha256')):
        raise RunDataError('scheduler audit segment checksum/length differs')
    entries = log.get('entries')
    if not isinstance(entries, list):
        raise RunDataError('invalid scheduler audit entry list')
    offset = 0
    originals = []
    for index, entry in enumerate(entries, 1):
        if (not isinstance(entry, dict) or type(entry.get('offset')) is not int
                or entry['offset'] != offset or type(entry.get('bytes')) is not int
                or entry['bytes'] < 1 or offset + entry['bytes'] > len(raw)
                or type(entry.get('index')) is not int or entry['index'] != index):
            raise RunDataError('scheduler audit entry is outside verified segment')
        part = raw[offset:offset + entry['bytes']]
        if not part.endswith(b'\n') or hashlib.sha256(part).hexdigest() != entry.get('sha256'):
            raise RunDataError('scheduler audit entry checksum differs')
        try:
            original = json.loads(part.decode('utf-8'))
        except (ValueError, UnicodeError):
            raise RunDataError('invalid scheduler query original')
        if original.get('index') != index or not isinstance(original.get('query'), dict):
            raise RunDataError('scheduler query original identity differs')
        originals.append(original)
        offset += entry['bytes']
    if offset != len(raw):
        raise RunDataError('scheduler audit entries do not cover verified segment')
    return originals
