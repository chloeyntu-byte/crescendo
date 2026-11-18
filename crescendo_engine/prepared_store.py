"""Versioned catalogue references with single-copy, content-addressed payloads.

Old inline objects are returned unchanged. Never relabel or re-sign old evidence.
References are data only and do not grant native ownership or Resume authority.
"""
import os
import re
import stat
import tempfile
import hashlib
import time

from .runtime import bind_text
from . import catalog_json
from .utils import _fsync_directory

_contract_text = bind_text()
SCHEMA = _contract_text('crescendo.prepared_reference.v2')
_HASH = re.compile(r'^[a-f0-9]{64}$')


def _content_sha256(path):
    # Also used while persisting cancellation/cleanup evidence. A cancelled
    # native task must not interrupt its own durable ownership receipt.
    result = hashlib.sha256()
    with open(path, 'rb') as stream:
        while True:
            part = stream.read(65536)
            if not part:
                break
            result.update(part)
    return result.hexdigest()


def _require_execution_object(prepared):
    if prepared.get('schema_version') == _contract_text('crescendo.prepared_gui_summary.v1'):
        raise ValueError('GUI catalogue summary is not an execution prepared object')


def compact_catalog(prepared):
    """Apply only to newly discovered v2 objects, before freshness is sealed.

    Keep every optimizable row and all native cell/instance/config/error proof.
    Hierarchy parameter rows are only GUI join keys; the catalogue owns metadata.
    """
    _require_execution_object(prepared)
    prepared['schema_version'] = _contract_text('crescendo.prepared.v2')
    rows = [row for row in prepared.get('parameters') or [] if row.get('optimizable')]
    prepared['parameters'] = rows
    by_id = dict((row['parameter_id'], row) for row in rows)
    for design in (prepared.get('hierarchy') or {}).get('designs') or []:
        body = design.get('hierarchy') or {}
        body['parameters'] = [dict(parameter_id=row['parameter_id'], hierarchy_path=row.get('hierarchy_path'))
                              for row in body.get('parameters') or [] if row.get('parameter_id') in by_id]
    return prepared


def catalogue_hierarchy(prepared):
    """Read-only view for reclassification of a compact saved suite.

    The caller owns parameter metadata; hierarchy rows are versioned join keys.
    Do not mutate or relabel the original saved hierarchy/freshness evidence.
    """
    hierarchy = prepared.get('hierarchy') or {}
    if prepared.get('schema_version') != _contract_text('crescendo.prepared.v2'):
        return hierarchy
    metadata = dict((row['parameter_id'], row) for row in prepared.get('parameters') or [])
    designs = []
    for design in hierarchy.get('designs') or []:
        body = dict(design.get('hierarchy') or {})
        rows = []
        for row in body.get('parameters') or []:
            key = row.get('parameter_id')
            if key not in metadata:
                raise ValueError('Saved hierarchy parameter reference has no catalogue metadata')
            rows.append(metadata[key])
        body['parameters'] = rows
        designs.append(dict(design, hierarchy=body))
    return dict(hierarchy, designs=designs)


def _regular(path):
    before = os.lstat(path)
    # A publisher links a fully fsynced temporary file, then removes its private
    # name. A concurrent publisher may observe the brief two-link interval.
    # Wait only for that interval; persistent hard links still fail closed.
    for _attempt in range(100):
        if before.st_nlink != 2:
            break
        time.sleep(.001)
        before = os.lstat(path)
    if (os.path.realpath(path) != os.path.abspath(path) or
            not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or
            before.st_uid != os.getuid()):
        raise ValueError('Unsafe prepared content object')
    return before


def _put(root, kind, value):
    directory = os.path.abspath(os.path.join(root, 'prepared_objects'))
    # Concurrent publishers may both encounter a previously absent directory.
    os.makedirs(directory, exist_ok=True)
    _regular_directory = os.lstat(directory)
    if (os.path.realpath(directory) != directory or not stat.S_ISDIR(_regular_directory.st_mode)
            or _regular_directory.st_uid != os.getuid()):
        raise ValueError('Unsafe prepared content directory')
    digest = catalog_json.digest(value)
    path = os.path.join(directory, kind + '-' + digest + '.json')
    if os.path.lexists(path):
        _regular(path)
        if _content_sha256(path) != digest:
            raise ValueError('Prepared content object changed')
    else:
        # Publish without replacing an existing content address. The payload has
        # no newline: its byte hash equals the canonical JSON identity.
        fd, temporary = tempfile.mkstemp(prefix='.object-', dir=directory)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                stream.writelines(catalog_json.chunks(value))
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                _regular(path)
                if _content_sha256(path) != digest:
                    raise ValueError('Prepared content object changed')
        finally:
            os.unlink(temporary)
        _fsync_directory(directory)
    return {'kind': kind, 'path': path, 'sha256': digest}


def write(path, prepared, root=None):
    document = reference(prepared, root=root or os.path.dirname(os.path.abspath(path)))
    catalog_json.write(path, document)


def reference(prepared, root=None):
    """Build a data reference for owned worker/configuration transports."""
    _require_execution_object(prepared)
    baseline = ((prepared.get('setup') or {}).get('baseline') or {}).get('root')
    root = os.path.abspath(baseline or root)
    if os.path.realpath(root) != root:
        raise ValueError('Unsafe prepared content root')
    body = dict(prepared)
    annotations = dict((key, body.pop(key)) for key in
                       ('setup_baseline_snapshot', 'resident_validation', 'setup_private_session') if key in body)
    references = {}
    for key in ('hierarchy', 'parameters'):
        if key in body:
            references[key] = _put(root, key, body.pop(key))
    payload = {'schema_version': _contract_text('crescendo.prepared_payload.v2'),
               'prepared': body, 'objects': references}
    return {'schema_version': SCHEMA, 'payload': _put(root, 'prepared', payload), 'annotations': annotations}


def _get(key, reference):
    if not isinstance(reference, dict):
        raise ValueError('Invalid prepared content address')
    path, digest = reference.get('path'), reference.get('sha256')
    if (set(reference) != {'kind', 'path', 'sha256'} or reference['kind'] != key or
            not isinstance(digest, str) or not _HASH.match(digest) or
            not isinstance(path, str) or not os.path.isabs(path) or
            os.path.basename(path) != key + '-' + digest + '.json' or
            os.path.basename(os.path.dirname(path)) != 'prepared_objects'):
        raise ValueError('Invalid prepared content address')
    before = _regular(path)
    if _content_sha256(path) != digest:
        raise ValueError('Prepared content object changed')
    with open(path, encoding='utf-8') as stream:
        if not os.path.samestat(before, os.fstat(stream.fileno())):
            raise ValueError('Prepared content object replaced')
        result = catalog_json.load(stream)
    after = _regular(path)
    def stamp(metadata):
        return (metadata.st_dev, metadata.st_ino, metadata.st_size,
                metadata.st_mtime_ns, metadata.st_ctime_ns)
    if stamp(before) != stamp(after):
        raise ValueError('Prepared content object changed while reading')
    return result


def resolve(document):
    if not isinstance(document, dict) or document.get('schema_version') != SCHEMA:
        return document  # exact legacy object; original hash validators follow
    if (set(document) != {'schema_version', 'payload', 'annotations'} or
            not isinstance(document['annotations'], dict) or
            set(document['annotations']).difference(
                ('setup_baseline_snapshot', 'resident_validation', 'setup_private_session'))):
        raise ValueError('Invalid prepared reference')
    payload = _get('prepared', document['payload'])
    if (set(payload) != {'schema_version', 'prepared', 'objects'} or
            payload['schema_version'] != _contract_text('crescendo.prepared_payload.v2') or
            not isinstance(payload['prepared'], dict) or not isinstance(payload['objects'], dict) or
            set(payload['objects']).difference(('hierarchy', 'parameters'))):
        raise ValueError('Invalid prepared payload')
    result = dict(payload['prepared'])
    for key, reference in payload['objects'].items():
        if key in result:
            raise ValueError('Duplicate prepared content key')
        result[key] = _get(key, reference)
    if set(result).intersection(document['annotations']):
        raise ValueError('Duplicate prepared annotation key')
    result.update(document['annotations'])
    return result


def load(path):
    with open(path, encoding='utf-8') as stream:
        return resolve(catalog_json.load(stream))
