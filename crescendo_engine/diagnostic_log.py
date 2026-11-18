"""Presentation-only compact reports and content-addressed JSONL evidence v1."""
import collections
import hashlib
import json
import os
import stat

from .runtime import bind_text
_contract_text = bind_text()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def validate_examples(value, allow_text=False):
    if allow_text and isinstance(value, str):
        try:
            value = int(value)
        except ValueError:
            pass
    if type(value) is not int or not 0 <= value <= 20:
        raise ValueError('diagnostic_log_examples must be an integer in 0..20')
    return value


def report_options(runtime, spec=None):
    spec = spec or {}
    detail = spec.get('diagnostic_log_detail', runtime.get('diagnostic_log_detail', 'compact'))
    examples = spec.get('diagnostic_log_examples', runtime.get('diagnostic_log_examples', 3))
    if detail not in ('compact', 'full'):
        raise ValueError('diagnostic_log_detail must be compact or full')
    validate_examples(examples)
    return detail, examples


class EvidenceStore(object):
    """Typed nodes avoid collisions with user dictionaries; identical trees share nodes.

    Hashes in the manifest cover the exact uncompressed UTF-8 JSONL file bytes.
    Nodes are in child-before-parent order and roots are named in the final row.
    """
    def __init__(self, path):
        self.path = path
        self.seen = set()
        self.stream = open(path, 'x', encoding='utf-8')
        os.chmod(path, 0o600)

    def put(self, value):
        if isinstance(value, dict):
            node = {'type': 'dict', 'items': [[key, self.put(item)] for key, item in sorted(value.items())]}
        elif isinstance(value, (list, tuple)):
            node = {'type': 'list', 'items': [self.put(item) for item in value]}
        elif isinstance(value, str) and len(value) > 128:
            node = {'type': 'text', 'value': value}
        else:
            return {'value': value}
        digest = hashlib.sha256(encoded(node)).hexdigest()
        if digest not in self.seen:
            self.stream.write(encoded({'sha256': digest, 'node': node}).decode('utf-8') + '\n')
            self.seen.add(digest)
        return {'ref': digest}

    def finish(self, documents):
        try:
            roots = {name: self.put(value) for name, value in sorted(documents.items())}
            self.stream.write(encoded({'schema_version': _contract_text('crescendo.diagnostic_evidence.v1'), 'roots': roots}).decode('utf-8') + '\n')
        finally:
            self.stream.close()
        size, digest = file_identity(self.path)
        return {'path': self.path, 'bytes': size, 'sha256': digest,
                'hash_scope': 'uncompressed file bytes', 'schema_version': _contract_text('crescendo.diagnostic_evidence.v1')}


def file_identity(path):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    with os.fdopen(descriptor, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise ValueError('Sidecar must be a regular single-link file')
        digest, size = hashlib.sha256(), 0
        for block in iter(lambda: stream.read(65536), b''):
            digest.update(block)
            size += len(block)
        after = os.fstat(stream.fileno())
        if (before.st_size, before.st_mtime) != (after.st_size, after.st_mtime):
            raise ValueError('Sidecar changed during verification')
    return size, digest.hexdigest()


def verify_sidecars(manifest):
    rows = []
    for item in manifest:
        row = {'path': item['path'], 'status': 'PASS'}
        try:
            size, digest = file_identity(item['path'])
            row.update(actual_bytes=size, actual_sha256=digest)
            if size != item['bytes'] or digest != item['sha256']:
                row.update(status='FAIL', reason='Sidecar size or sha256 mismatch')
        except (OSError, ValueError) as error:
            row.update(status='FAIL', reason=str(error))
        rows.append(row)
    return {'status': 'PASS' if rows and all(row['status'] == 'PASS' for row in rows) else 'FAIL',
            'scope': 'sidecar file size and sha256; collector omissions are reported separately', 'sidecars': rows}


def read_evidence(path):
    """Restore plain documents without executing data; also validate every node hash."""
    nodes, roots = {}, None
    def resolve(reference):
        return nodes[reference['ref']] if 'ref' in reference else reference['value']
    with open(path, encoding='utf-8') as stream:
        for line in stream:
            row = json.loads(line)
            if 'roots' in row:
                roots = row['roots']
                continue
            node = row['node']
            if hashlib.sha256(encoded(node)).hexdigest() != row['sha256']:
                raise ValueError('Evidence node digest mismatch')
            if node['type'] == 'dict':
                value = {key: resolve(ref) for key, ref in node['items']}
            elif node['type'] == 'list':
                value = [resolve(ref) for ref in node['items']]
            else:
                value = node['value']
            nodes[row['sha256']] = value
    if roots is None:
        raise ValueError('Evidence roots missing')
    return {key: resolve(ref) for key, ref in roots.items()}


def summarize(value, examples=3):
    """Keep status, criteria and scalar measurements; bound example collections."""
    if isinstance(value, dict):
        result = {key: ([summarize(row, examples) for row in item]
                       if key == 'generations' and isinstance(item, list) else summarize(item, examples))
                  for key, item in value.items() if key != 'unpaired'}
        if isinstance(value.get('unpaired'), list):
            groups = collections.Counter((str(item.get('output') or '*'), str(item.get('reason') or 'unknown'))
                                         for item in value['unpaired'])
            result['unpaired'] = {'count': len(value['unpaired']), 'groups': [
                {'output': output, 'reason': reason, 'count': count}
                for (output, reason), count in sorted(groups.items())],
                'examples': [summarize(item, examples) for item in value['unpaired'][:examples]]}
        return result
    if isinstance(value, (list, tuple)):
        return {'count': len(value), 'examples': [summarize(item, examples) for item in value[:examples]]}
    if isinstance(value, str) and len(value) > 600:
        return value[:600] + ' [full text in sidecar]'
    return value


def grouped_checks(checks, examples=3):
    groups = collections.OrderedDict()
    for check in checks:
        if check.get('status') == 'PASS':
            continue
        p = check.get('presentation') or {}
        message = check.get('message') or ''
        # Query states are observations, never solver completion or stop proof.
        unknown = 'Exact owned Job ID query: UNKNOWN' in message
        cause = bool(p.get('blocked_by') or (check.get('check_id') == 'execution' and (check.get('details') or {}).get('first_cause')))
        key = (check.get('status'), 'owned_query_unknown' if unknown else
               'first_cause' if cause else check.get('check_id'),
               '' if unknown or cause else message,
               '' if unknown else check.get('simulator'), '' if unknown else check.get('engine'))
        group = groups.setdefault(key, {'status': check.get('status'), 'check': key[1],
            'summary': 'Exact owned Job ID query: UNKNOWN' if unknown else
                       'See mode first_cause' if cause else summarize(p.get('summary') or message, examples),
            'count': 0, 'modes': set(), 'job_ids': set(), 'examples': []})
        group['count'] += 1
        group['modes'].add('.'.join(str(check.get(k) or '') for k in ('simulator', 'engine')).strip('.'))
        details = check.get('details') or {}
        def jobs(value):
            if isinstance(value, dict):
                for name, item in value.items():
                    if name in ('job_id', 'job_ids', 'owned_job_ids'):
                        for job in item if isinstance(item, list) else [item]:
                            group['job_ids'].add(str(job))
                    elif isinstance(item, (dict, list)):
                        jobs(item)
            elif isinstance(value, list):
                for item in value:
                    jobs(item)
        jobs(details)
        if len(group['examples']) < examples:
            group['examples'].append(check['id'])
    result = []
    for group in groups.values():
        group['modes'] = sorted(group['modes'])
        ids = sorted(group.pop('job_ids'), key=lambda job: (not job.isdigit(), int(job) if job.isdigit() else job))
        if ids:
            group['job_id_count'] = len(ids)
            group['job_id_range'] = [ids[0], ids[-1]]
        result.append(group)
    return result


def compact_report(row, checks, overview, contexts, focus, contributions, ladder):
    """No gate mutation. The returned manifest belongs to this report directory."""
    from .diagnostic_sections import SECTION_LIMIT, TOTAL_LIMIT
    from .diagnosis_catalog import describe_check
    from .environment_test import _report_truncation_paths
    examples = row['diagnostic_log_examples']
    documents = {'case_results': row['case_results'], 'focused_checks': focus,
                 'checks': checks, 'focused_cases': row.get('focused_cases') or {}, 'case_records': row.get('case_records') or {},
                 'case_contexts': row.get('case_contexts') or {}, 'diagnostic_sections': contributions,
                 'overview': overview}
    path = os.path.join(row['report_root'], _contract_text('diagnostic_evidence.jsonl'))
    manifest = [EvidenceStore(path).finish(documents)]
    lines = []
    def emit(prefix, value):
        lines.append(prefix + ' ' + encoded(value).decode('utf-8'))
    # This is also the structured parity surface, kept lossless in the sidecar.
    shown_overview = dict(overview)
    shown_cases, causes, cause_sources = [], {}, {}
    for case in overview.get('cases') or []:
        shown = dict(case)
        cause = case.get('first_cause')
        if cause:
            digest = hashlib.sha256(encoded(cause)).hexdigest()
            if digest in causes:
                shown.pop('first_cause', None)
                shown['first_cause_ref'] = causes[digest]
            else:
                causes[digest] = case['case_id']
            cause_sources[case['case_id']] = causes[digest]
        shown_cases.append(shown)
    shown_overview['cases'] = shown_cases
    emit('diagnostic_overview', shown_overview)
    lines.extend(['generation_timing_table (seconds; missing = unknown)',
                  '|mode|generation|phase|scope|seconds|measured/total|', '|---|---|---|---|---|---|'])
    for mode, timings in (overview.get('generation_timings') or {}).items():
        for timing in timings:
            def phase(name, scope, seconds, counts=''):
                lines.append('|{}|{}|{}|{}|{}|{}|'.format(mode, timing['generation'], name, scope,
                             'unknown' if seconds is None else seconds, counts))
            phase('wall', 'generation', timing.get('wall_seconds'))
            for name, seconds in sorted((timing.get('phases_seconds') or {}).items()):
                phase(name, 'generation', seconds)
            for scope, phases in sorted((timing.get('phase_sums') or {}).items()):
                for name, values in sorted(phases.items()):
                    phase(name, scope, values[0], '{}/{}'.format(values[1], values[2]))
            for name, values in sorted((timing.get('storage_operations') or {}).items()):
                phase(name, 'nested storage', values[0], str(values[1]) + ' calls')
    lines.append('SECTION BEGIN focused_checks | Focused checks')
    emit('focused_checks_summary', summarize(focus, examples))
    lines.append('SECTION END focused_checks')
    emit('contributor_budget', {'per_section_characters': SECTION_LIMIT, 'total_characters': TOTAL_LIMIT,
         'maximum_utf8_body_bytes': 4 * TOTAL_LIMIT, 'policy': 'same title/body truncation as full; included in compact log budget'})
    for section in contributions:
        emit('diagnostic_section', section)
    # The ladder's decisions/actions were computed from the untouched cases.
    # Keep the first cause readable at the verdict, with a lossless reference.
    stage, ladder_lines = 0, []
    engines = ('maestro_baseline', 'private_netlist', 'maestro_netlist')
    for line in ladder.splitlines():
        if stage < 3 and line.startswith(str(stage + 1) + '. '):
            case = next((case for case in row['case_results'].values() if case.get('engine') == engines[stage]), {})
            source = cause_sources.get(case.get('case_id'))
            if source:
                from .environment_probes import _redact
                cause = case.get('first_cause') or {}
                cause = cause if isinstance(cause, dict) else {'message': str(cause)}
                reason = cause.get('message') or cause.get('error') or cause.get('failure_stage')
                cadence = row.get('config', {}).get('cadence') or {}
                environment = dict(os.environ)
                environment.update(cadence.get('launch_environment') or {})
                if cadence.get('license_file'):
                    environment['CDS_LIC_FILE'] = cadence['license_file']
                reason = _redact(str(reason or case.get('skip_reason') or ''), environment)
                line = line.split(' — ', 1)[0] + ' — ' + ' '.join(reason.split())
            line = ' '.join(line.split())
            if len(line) > 200:
                line = line[:200 - len(' [truncated]')] + ' [truncated]'
            ladder_lines.append(line)
            if source:
                ladder_lines.append('   first_cause_ref: ' + source)
            stage += 1
            continue
        elif line.startswith('   ') and stage:
            line = '   Auxiliary worker evidence: sidecar/case_contexts'
        ladder_lines.append(line)
    lines.append('\n'.join(ladder_lines))
    # Keep each warning visible once per mode, with the raw native message.
    for scope, records in (row.get('case_records') or {}).items():
        for record in records:
            if record.get('observation') == 'diagnostic.ocean_tool_warning':
                emit('ocean_tool_warning', dict(record['payload'], scope=scope))
            elif record.get('observation') in ('diagnostic.variable_selection_notice', 'diagnostic.template_variable_notice'):
                emit('variable_selection_notice', dict(record['payload'], scope=scope))
    # Preserve standalone Copy anchors; evidence appears once via sidecar refs.
    sections = collections.OrderedDict()
    for check in checks:
        p = describe_check(check, {})
        sections.setdefault((p['group_id'], p['section_id']), []).append(check)
    for scope, context in contexts.items():
        shown = {key: context[key] for key in ('run_root', 'path_alias', 'report_detail_mode') if key in context}
        shown.update(first_cause_ref='diagnostic_overview/cases/' + scope, evidence_ref='sidecar/case_contexts/' + scope)
        emit('run_context', {'scope': scope, 'context': shown})
    for (scope, section), selected in sections.items():
        lines.append('=== SECTION BEGIN {}/{} ==='.format(scope, section))
        lines.append(_contract_text('report_format crescendo.diagnosis.compact.v1'))
        emit('check_group', {'counts': dict(collections.Counter(c['status'] for c in selected)),
                            'evidence_ref': 'sidecar/checks', 'cause_ref': 'diagnostic_overview/cases/' + scope})
        lines.append('=== SECTION END {}/{} ==='.format(scope, section))
    for group in grouped_checks(checks, examples):
        emit('check_group', group)
    emit('report_evidence_integrity', {'scope': 'collector only; writer completion uses report_ready',
        'cases': {scope: {'record_count': len(records),
            'collection_omissions': context.get('omitted_or_truncated') or [],
            'cache_omission': context.get('report_omissions'),
            'bounded_evidence_paths': _report_truncation_paths(records),
            'status': 'PARTIAL' if context.get('omitted_or_truncated') or context.get('report_omissions') or _report_truncation_paths(records) else 'COMPLETE_FOR_COLLECTED_RECORDS'}
            for scope, records in (row.get('case_records') or {}).items()
            for context in [row.get('case_contexts', {}).get(scope) or {}]}})
    emit('success_summary', {'count': sum(check['status'] == 'PASS' for check in checks),
        'by_check': dict(collections.Counter(check.get('check_id') or 'other' for check in checks if check['status'] == 'PASS')),
        'proofs_ref': 'sidecar/checks'})
    emit('sidecar_manifest', manifest)
    emit('final_checklist', {'final_checklist': [{'id': check['id'], 'status': check['status']} for check in checks],
         'state': row['state'], 'summary': dict(collections.Counter(c['status'] for c in checks)),
         'case_results_ref': 'diagnostic_overview/cases', 'focused_checks_ref': 'focused_checks_summary',
         'diagnostic_sections_ref': 'diagnostic_section', 'sidecar_ref': manifest[0]['path']})
    return '\n'.join(lines) + '\n', manifest
