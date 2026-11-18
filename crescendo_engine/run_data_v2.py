"""Version two wire codec. Evidence lives in the verified append-only prefix.

No file paths are accepted by the resolver. Blob records are fsynced before
references, and may only refer backwards. Keeping blobs in the core file makes
portable exports and retention independent of deleted point directories.
"""
import copy
import hashlib
import base64
import zlib
import json
import os
import re

from .runtime import bind_text
_text = bind_text()
REF_SCHEMA = _text('crescendo.evidence_ref.v2')
BLOB_SCHEMA = _text('crescendo.evidence_blob.v2')
_RESERVED = ('$evidence', '$point', '$aliases', '$audit')
PROTECTED = ('candidate_id', 'generation', 'index', 'values', 'status')


def _api():
    from .run_data_store import _encoded, RunDataError
    return _encoded, RunDataError


def digest(value):
    encoded, unused = _api()
    return hashlib.sha256(encoded(value)).hexdigest()


class Decoder(object):
    def __init__(self):
        self.blobs = {}
        self.blob_owners = {}
        self.point_receipts = {}
        self.versions = {}
        self.audit_verified = set()

    def owners(self, value):
        if isinstance(value, list):
            result = set()
            for item in value:
                result.update(self.owners(item))
            return result
        if not isinstance(value, dict):
            return set()
        if '$evidence' in value:
            self.resolve(value)
            return self.blob_owners[value['$evidence']['sha256']]
        result = set()
        if '$point' in value:
            result.add(self.resolve(value['$point']).get('candidate_id'))
        for item in value.values():
            result.update(self.owners(item))
        return result

    def query_originals(self, log):
        unused, Error = _api()
        if not isinstance(log, dict) or not isinstance(log.get("evidence"), dict) or set(log["evidence"]) != {"$audit"}:
            raise Error("scheduler query log has no durable originals")
        original = self.resolve({"$evidence": log["evidence"]["$audit"]})
        if not isinstance(original, dict) or not isinstance(original.get("text"), str):
            raise Error("invalid scheduler query originals blob")
        from .scheduler_audit import verify_segment
        return verify_segment(log, original["text"])

    def resolve(self, value):
        encoded, Error = _api()
        if isinstance(value, list):
            return [self.resolve(item) for item in value]
        if not isinstance(value, dict):
            return value
        if '$audit' in value:
            if set(value) != {'$audit'}:
                raise Error('invalid audit reference')
            self.resolve({'$evidence': value['$audit']})
            return value
        if '$evidence' in value:
            if set(value) != {'$evidence'}:
                raise Error('invalid evidence reference envelope')
            ref = value['$evidence']
            if (not isinstance(ref, dict) or set(ref) != {
                    'schema_version', 'type', 'schema', 'sha256', 'bytes', 'revision'}
                    or ref.get('schema_version') != REF_SCHEMA
                    or type(ref.get('revision')) is not int or ref['revision'] < 1
                    or type(ref.get('bytes')) is not int or not 1 <= ref['bytes'] <= 64 * 1024 * 1024
                    or ref.get('type') not in ('dict', 'list')
                    or (ref.get('schema') is not None and not isinstance(ref['schema'], str))
                    or not isinstance(ref.get('sha256'), str)
                    or re.fullmatch(r'[0-9a-f]{64}', ref['sha256']) is None):
                raise Error('invalid evidence reference')
            stored = self.blobs.get(ref.get('sha256'))
            if stored is None or stored[0] != ref:
                raise Error('evidence reference is missing or outside verified prefix')
            # Stored objects are immutable; callers receive copies at API edges.
            return stored[1]
        if '$point' in value:
            if set(value) != {'$point', 'fields', 'extra'}:
                raise Error('invalid point projection')
            point = self.resolve(value['$point'])
            reference = value['$point'].get('$evidence') if isinstance(value['$point'], dict) else None
            if (not isinstance(reference, dict)
                    or reference['sha256'] not in self.point_receipts
                    or self.point_receipts[reference['sha256']][0] != reference):
                raise Error('point reference has no committed point receipt')
            fields = value['fields']
            aliases = {'raw_terminal_receipt': 'terminal_receipt', 'terminal_receipt': 'effective_terminal_receipt'}
            if (not isinstance(point, dict) or point.get('candidate_completed') is not False
                    or not isinstance(fields, dict) or not fields
                    or not {'task_id', 'test', 'corner', 'point_id', 'raw_terminal_receipt'} <= set(fields)
                    or any(not isinstance(k, str) or not isinstance(v, str) or v not in point
                           or v != aliases.get(k, k) for k, v in fields.items())):
                raise Error('invalid point receipt reference')
            extra = self.resolve(value['extra'])
            if not isinstance(extra, dict) or set(extra).intersection(fields):
                raise Error('invalid point projection fields')
            return dict(extra, **{key: point[source] for key, source in fields.items()})
        if any(key in value for key in _RESERVED):
            raise Error('reserved evidence wire key')
        resolved = {key: self.resolve(item) for key, item in value.items()}
        if resolved.get('schema_version') == _text('crescendo.scheduler_query_log.v2'):
            sha = digest(resolved)
            if sha not in self.audit_verified:
                self.query_originals(resolved)
                self.audit_verified.add(sha)
        if resolved.get('schema_version') == _text('crescendo.scheduler_task_tracking.v2'):
            if len((resolved.get('query_log') or {}).get('entries') or []) != resolved.get('query_count'):
                raise Error('scheduler query census differs from durable originals')
        return resolved

    def add_blob(self, value, revision):
        encoded, Error = _api()
        if not isinstance(value, dict) or set(value) != {'schema_version', 'reference', 'value', 'codec'} or value['schema_version'] != BLOB_SCHEMA:
            raise Error('invalid evidence blob')
        ref = value['reference']
        packed = value['value']
        if value['codec'] == 'zlib-json':
            try:
                compressed = base64.b64decode(packed.encode('ascii'), validate=True)
                decoder = zlib.decompressobj()
                raw = decoder.decompress(compressed, 64 * 1024 * 1024 + 1)
                if len(raw) > 64 * 1024 * 1024 or not decoder.eof or decoder.unused_data:
                    raise ValueError('compressed evidence exceeds limit or has trailing data')
                packed = json.loads(raw.decode('utf-8'))
            except (ValueError, TypeError, AttributeError, zlib.error) as error:
                raise Error('invalid compressed evidence: %s' % error)
        elif value['codec'] != 'json':
            raise Error('unknown evidence codec')
        resolved = self.resolve(packed)
        raw = encoded(resolved)
        expected = {'schema_version': REF_SCHEMA, 'type': type(resolved).__name__,
                    'schema': resolved.get('schema_version') if isinstance(resolved, dict) else None,
                    'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw), 'revision': revision}
        if ref != expected or expected['sha256'] in self.blobs:
            raise Error('invalid or repeated evidence blob checksum/length/revision')
        self.blob_owners[expected['sha256']] = self.owners(packed)
        self.blobs[expected['sha256']] = (expected, resolved)

    def candidate(self, packed):
        unused, Error = _api()
        if not isinstance(packed, dict) or set(packed) != {'row', 'aliases'}:
            raise Error('invalid v2 candidate envelope')
        row = self.resolve(packed['row'])
        if not isinstance(row, dict) or not isinstance(packed['aliases'], list):
            raise Error('invalid candidate row or alias list')
        if self.owners(packed['row']) - {row.get('candidate_id')}:
            raise Error('candidate references another candidate point receipt')
        for path in packed['aliases']:
            if path not in (['completion', 'execution_pipeline'], ['completion_evidence', 'execution_pipeline']):
                raise Error('invalid candidate pipeline alias')
            target = row.get(path[0])
            if (not isinstance(target, dict) or path[1] in target or 'execution_pipeline' not in row):
                raise Error('invalid candidate pipeline alias target')
            # Never mutate shared decoded evidence.
            row = dict(row)
            row[path[0]] = dict(target, execution_pipeline=row['execution_pipeline'])
        return row

    def patch(self, value, rows):
        unused, Error = _api()
        if not isinstance(value, dict) or set(value) != {'candidate_id', 'base_revision', 'base_sha256', 'set', 'remove'}:
            raise Error('invalid candidate delta')
        cid = value['candidate_id']
        if not isinstance(cid, str):
            raise Error('invalid candidate delta identity')
        previous = rows.get(cid)
        if (type(value['base_revision']) is not int or previous is None or previous.get('status') not in ('completed', 'failed', 'infrastructure_failed')
                or self.versions.get(cid) != (value['base_revision'], value['base_sha256'])):
            raise Error('candidate delta base is not committed')
        changes = self.resolve(value['set'])
        if self.owners(value['set']) - {cid}:
            raise Error('candidate delta references another candidate point receipt')
        removed = value['remove']
        if (not isinstance(changes, dict) or not isinstance(removed, list)
                or any(not isinstance(key, str) for key in removed)
                or len(set(removed)) != len(removed) or set(removed).intersection(changes)
                or any(key in changes or key in removed for key in PROTECTED)
                or any(key not in previous for key in removed)):
            raise Error('candidate delta changes protected fields')
        row = dict(previous)
        for key in removed:
            del row[key]
        row.update(changes)
        return row


class Encoder(object):
    def __init__(self, store):
        self.store = store
        self.blobs = {}
        self.rows = {}
        self.versions = {}
        self.points = {}
        self.audit_logs = {}
        self.imported_audits = {}

    def seed(self, reader):
        self.blobs = {key: value[0] for key, value in reader.codec.blobs.items()}
        self.rows = dict(reader.rows)
        self.versions = dict(reader.codec.versions)
        for ref, value in reader.codec.blobs.values():
            if isinstance(value, dict) and value.get('schema_version') == _text('crescendo.scheduler_query_log.v2') and 'evidence' in value:
                source = {key: item for key, item in value.items() if key != 'evidence'}
                self.audit_logs[digest(source)] = value
        for ref, value, revision, checksum in reader.codec.point_receipts.values():
            if value.get('candidate_completed') is False and value.get('token'):
                self.points[(value.get('candidate_id'), value.get('task_id'), value['token'])] = (ref, value)

    def prepare(self, value):
        unused, Error = _api()
        if isinstance(value, list):
            return [self.prepare(item) for item in value]
        if not isinstance(value, dict):
            return value
        if value.get('schema_version') == _text('crescendo.scheduler_query_log.v2') and 'evidence' in value:
            audit = value['evidence']
            if not isinstance(audit, dict) or set(audit) != {'$audit'}:
                raise Error('invalid imported scheduler audit reference')
            reference = audit['$audit']
            replacement = self.imported_audits.get(digest(reference))
            if replacement is not None:
                value = dict(value, evidence={'$audit': replacement})
        if value.get('schema_version') == _text('crescendo.scheduler_query_log.v2') and 'evidence' not in value:
            cached = self.audit_logs.get(digest(value))
            if cached is not None:
                return cached
            from .run_data_store import _open_read
            source_sha = digest(value)
            path = value.get('path', '')
            root = os.path.dirname(self.store.path)
            if (not isinstance(path, str) or os.path.realpath(path) != path
                    or not path.startswith(root + os.sep + 'candidates' + os.sep)
                    or os.path.basename(path) not in ('scheduler_queries.jsonl', 'cancel_scheduler_queries.jsonl')):
                raise Error('unsafe scheduler audit source')
            with _open_read(path) as stream:
                raw_log = stream.read(value['bytes'] + 1)
            if len(raw_log) != value['bytes'] or hashlib.sha256(raw_log).hexdigest() != value['sha256']:
                raise Error('scheduler audit source checksum/length differs')
            blob = self.pack({'schema_version': _text('crescendo.scheduler_query_originals.v2'),
                              'text': raw_log.decode('utf-8')}, force=True)
            value = dict(value, evidence={'$audit': blob['$evidence']})
            self.audit_logs[source_sha] = value
        return {key: self.prepare(item) if key != 'evidence' else item for key, item in value.items()}

    def pack(self, value, candidate_id=None, force=False):
        encoded, Error = _api()
        if not isinstance(value, (dict, list)):
            return value
        if isinstance(value, dict) and set(value) == {'$audit'}:
            ref = value['$audit']
            if self.blobs.get(ref.get('sha256')) != ref:
                raise Error('audit reference is not committed')
            return value
        if isinstance(value, dict) and any(key in value for key in _RESERVED):
            raise Error('reserved evidence wire key')
        raw = encoded(value)
        sha = hashlib.sha256(raw).hexdigest()
        if sha in self.blobs:
            return {'$evidence': self.blobs[sha]}
        # Pipeline points are exact projections of the measured point receipt.
        if isinstance(value, dict) and candidate_id and 'raw_terminal_receipt' in value:
            token = (value.get('raw_terminal_receipt') or {}).get('token')
            point = self.points.get((candidate_id, value.get('task_id'), token))
            if point and all(value.get(key) == point[1].get(key) for key in ('task_id', 'test', 'corner', 'point_id')):
                aliases = {'raw_terminal_receipt': 'terminal_receipt', 'terminal_receipt': 'effective_terminal_receipt'}
                fields = {key: aliases.get(key, key) for key, item in value.items()
                          if aliases.get(key, key) in point[1] and point[1][aliases.get(key, key)] == item}
                return {'$point': {'$evidence': point[0]}, 'fields': fields,
                        'extra': self.pack({key: item for key, item in value.items() if key not in fields}, candidate_id)}
        if isinstance(value, dict):
            packed = {key: self.pack(item, candidate_id) for key, item in value.items()}
        else:
            packed = [self.pack(item, candidate_id) for item in value]
        if force or len(raw) >= 1024:
            ref = {'schema_version': REF_SCHEMA, 'type': type(value).__name__,
                   'schema': value.get('schema_version') if isinstance(value, dict) else None,
                   'sha256': sha, 'bytes': len(raw), 'revision': self.store.revision + 1}
            packed_raw = encoded(packed)
            compressed = base64.b64encode(zlib.compress(packed_raw)).decode('ascii')
            codec = 'zlib-json' if len(compressed) < len(packed_raw) * 0.8 else 'json'
            self.store._append_wire('evidence', {'schema_version': BLOB_SCHEMA, 'reference': ref,
                                               'value': compressed if codec == 'zlib-json' else packed,
                                               'codec': codec})
            self.blobs[sha] = ref
            return {'$evidence': ref}
        return packed

    def point(self, value):
        packed = self.pack(value, value.get('candidate_id'), force=True)
        ref = packed['$evidence']
        return packed

    def candidate(self, row):
        row = copy.deepcopy(row)
        aliases = []
        for key in ('completion', 'completion_evidence'):
            if (isinstance(row.get(key), dict) and 'execution_pipeline' in row
                    and row[key].get('execution_pipeline') == row['execution_pipeline']):
                del row[key]['execution_pipeline']
                aliases.append([key, 'execution_pipeline'])
        return {'row': self.pack(row, row['candidate_id']), 'aliases': aliases}
