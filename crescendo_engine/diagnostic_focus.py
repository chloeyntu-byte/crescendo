"""Passive focused checks over this diagnostic's own frozen samples and files.

These results are report data only. They never participate in run qualification.
"""
import collections
import hashlib
import itertools
import json
import math
import os
import stat

from .diagnostic_timing import timing_seconds
from .output_status import point_status
from .runtime import bind_text
_contract_text = bind_text()

RELATIVE_TOLERANCE = 1e-6
ABSOLUTE_TOLERANCES = {'V': 1e-9, 'A': 1e-12, 'W': 1e-12,
                       'Hz': 1e-3, 's': 1e-15, 'Ohm': 1e-6, 'F': 1e-18,
                       'H': 1e-15, 'dB': 1e-6}
VOLUME_LIMIT = 256 * 1024


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      allow_nan=False, separators=(',', ':')).encode('utf-8')


def result(status, criterion, **data):
    return dict(data, status=status, criterion=criterion)


def checkpoint_check(timings):
    criterion = 'scans/generation <= 1 (expected 1); last/first transaction seconds <= 2; generation_commit verified'
    rows, missing, warn, failed = [], False, False, False
    for item in timings:
        operations = item.get('storage_operations') or {}
        scan, transaction = operations.get('dataset_prefix_scan') or {}, operations.get('checkpoint_transaction') or {}
        audits = item.get('generation_commit') or []
        samples = item.get('prefix_scans') or []
        seconds = timing_seconds(transaction.get('seconds'))
        verified = bool(audits) and all(a.get('verified') is True for a in audits)
        failed = failed or any(a.get('verified') is False for a in audits)
        missing = missing or not verified or seconds is None or not samples or scan.get('calls') is None
        count = scan.get('calls')
        warn = warn or (isinstance(count, int) and count > 1)
        rows.append({'generation': item.get('generation'), 'prefix_scan_count': count,
                     'scan_bytes': scan.get('bytes'),
                     'scan_bytes_over_file_size': [s['bytes'] / float(s['file_size'])
                         if s.get('file_size', 0) > 0 else None for s in samples],
                     'transaction_seconds': seconds, 'generation_commit': audits})
    ratios = [r['transaction_seconds'] for r in rows]
    valid_ends = len(ratios) >= 2 and ratios[0] is not None and ratios[-1] is not None
    ratio = ratios[-1] / ratios[0] if valid_ends and ratios[0] > 0 else None
    warn = warn or (ratio is not None and ratio > 2) or (valid_ends and ratios[0] == 0 < ratios[-1])
    status = 'FAIL' if failed else 'WARN' if warn else 'NOT_TESTED' if missing or not rows else 'PASS'
    return result(status, criterion, generations=rows, last_first_transaction_ratio=ratio,
                  ratio_unavailable=bool(valid_ends and ratios[0] == 0))


def raw_output_points(parsed):
    """Copy raw scalar values before cleanup; never substitute aggregate values."""
    rows = []
    for key, output in sorted((parsed.get('output_map') or {}).items()):
        for point in output.get('values') or []:
            value = point.get('numeric')
            rows.append({'test': output.get('test') or key.split('::')[0],
                         'corner': point.get('corner'), 'output': key,
                         'point_id': point.get('point_id'), 'unit': output.get('unit') or output.get('units') or '',
                         'value': value if type(value) in (int, float) and math.isfinite(value) else None,
                         'output_status': point_status(point),
                         'first_error': point.get('first_error') or point.get('error')})
    return rows


def consistency_check(cases, outputs=(), expected_points=(), expected_candidates=None, classified=None):
    criterion = 'all completed frozen candidates; all mode pairs: abs(delta) <= max(1e-6 * max(abs(values)), unit absolute tolerance); equal constraints and feasibility'
    tables, candidates, unpaired, mismatches, decisions, units = {}, {}, [], [], [], {}
    metadata = {o.get('key', o.get('output_key')): o for o in outputs}
    required_keys = set(classified['required_scalars']) if classified is not None else None
    maximum, paired, pair_count, missing_decisions = None, 0, 0, False
    for output in outputs:
        units[output.get('key', output.get('output_key'))] = output.get('unit') or output.get('units') or ''
    for mode, rows in cases.items():
        table, by_id = {}, {}
        for candidate in rows:
            identifier = candidate.get('candidate_id')
            by_id[identifier] = candidate
            raw_points = (candidate.get('metrics') or {}).get('diagnostic_raw_points') or []
            group_counts = collections.Counter((p.get('test'), p.get('corner'), p.get('output')) for p in raw_points)
            for point in raw_points:
                if required_keys is not None:
                    corners = classified.get('required_corners', {}).get(point.get('output'))
                    if point.get('output') not in required_keys or corners is not None and point.get('corner') not in corners:
                        continue
                group = (point.get('test'), point.get('corner'), point.get('output'))
                key = (identifier,) + group + (point.get('point_id') if group_counts[group] > 1 else None,)
                if key in table:
                    return result('FAIL', criterion, error='Ambiguous duplicate raw point', identity=list(key))
                table[key] = point
        tables[mode], candidates[mode] = table, by_id
    modes = list(cases)
    all_ids = set().union(*(set(c) for c in candidates.values())) if candidates else set()
    expected_candidates = expected_candidates or {}
    all_ids.update(expected_candidates)
    eligible = set()
    for identifier in sorted(all_ids):
        rows = [candidates[m].get(identifier) or {} for m in modes]
        if len(modes) != 3 or any(r.get('status') != 'completed' for r in rows):
            unpaired.append({'candidate': identifier, 'reason': 'missing or failed mode',
                             'mode_states': {m: r.get('status', 'missing') for m, r in zip(modes, rows)},
                             'output_states': {m: r.get('output_statuses') or [] for m, r in zip(modes, rows)}})
            continue
        if (any(r.get('values') != rows[0].get('values') for r in rows[1:]) or
                identifier in expected_candidates and rows[0].get('values') != expected_candidates[identifier]):
            unpaired.append({'candidate': identifier, 'reason': 'frozen sample values differ'})
            continue
        eligible.add(identifier)
        verdicts = []
        for row in rows:
            metrics = row.get('metrics') or {}
            constraints = metrics.get('constraints')
            if metrics.get('feasible') is None or constraints is None:
                missing_decisions = True
            verdicts.append({'feasible': metrics.get('feasible'), 'constraints': sorted([
                (c.get('output_key'), c.get('name'), c.get('aggregation'), tuple(c.get('corners') or []), c.get('satisfied'))
                for c in constraints or []], key=repr)})
        feasibility = [v['feasible'] for v in verdicts]
        if all(type(v) is bool for v in feasibility) and len(set(feasibility)) != 1:
            decisions.append({'candidate': identifier, 'field': 'feasible', 'modes': dict(zip(modes, feasibility))})
        constraint_maps = [{tuple(c[:-1]): c[-1] for c in v['constraints']} for v in verdicts]
        constraint_keys = set().union(*(set(c) for c in constraint_maps))
        for constraint in sorted(constraint_keys, key=repr):
            states = [c.get(constraint) for c in constraint_maps]
            if not all(type(v) is bool for v in states):
                missing_decisions = True
                unpaired.append({'candidate': identifier, 'constraint': list(constraint),
                                 'reason': 'constraint decision unavailable', 'modes': dict(zip(modes, states))})
            elif len(set(states)) != 1:
                decisions.append({'candidate': identifier, 'constraint': list(constraint),
                                  'modes': dict(zip(modes, states))})
        if not any(k[0] == identifier for table in tables.values() for k in table):
            unpaired.append({'candidate': identifier, 'reason': 'raw point evidence unavailable'})
    keys = set().union(*(set(t) for t in tables.values())) if tables else set()
    # Include selected points absent in every mode, not just the observed union.
    for identifier in eligible:
        for output in outputs:
            key = output.get('key', output.get('output_key'))
            if required_keys is not None and key not in required_keys:
                continue
            if not key or output.get('enabled') is False:
                continue
            test = output.get('test') or key.split('::')[0]
            for expected_test, corner in expected_points:
                corners = classified.get('required_corners', {}).get(key) if classified else None
                if corners is not None and corner not in corners:
                    continue
                group = (identifier, test, corner, key)
                if test == expected_test and not any(k[:4] == group for k in keys):
                    keys.add(group + (None,))
    tolerances = {}
    for key in sorted(keys, key=repr):
        if key[0] not in eligible:
            continue
        rows = [tables[m].get(key) for m in modes]
        identity = dict(zip(('candidate', 'test', 'corner', 'output', 'point_id'), key))
        if any(r is None or type(r.get('value')) not in (int, float) or not math.isfinite(r['value']) for r in rows):
            states = {}
            for mode, point in zip(modes, rows):
                status = point_status(point) if point else 'not_collected'
                first_error = (point or {}).get('first_error')
                if point is None:
                    saved = (candidates[mode].get(key[0]) or {}).get('output_statuses') or []
                    evidence = next((s for s in saved if s.get('output') == key[3]
                                     and s.get('corner') in (None, key[2])
                                     and (key[4] is None or s.get('point_id') == key[4])), {})
                    status, first_error = evidence.get('status', 'unknown'), evidence.get('first_error')
                simulator = str(mode).split('.', 1)[0]
                if isinstance(first_error, str):
                    from .netlist_measurement import _reader_console_diagnostics
                    console = _reader_console_diagnostics({'stdout_tail': first_error}, (simulator,))
                    leading_tool = any(warning['console_line'] == 1
                                       for warning in console.get('ocean_tool_warnings', []))
                    leading_stage = any(warning['stage_begin_line'] == 1
                                        for warning in console.get('library_init_warnings', []))
                    if leading_tool or leading_stage:
                        first_error = console['reader_first_error']
                states[mode] = {'status': status, 'first_error': first_error}
            unpaired.append(dict(identity, reason='required scalar unavailable', output_states=states,
                                 missing_modes=[m for m in modes if states[m]['status'] != 'available']))
            continue
        point_units = [r.get('unit') or units.get(key[3], '') for r in rows]
        if len(set(point_units)) != 1:
            unpaired.append(dict(identity, reason='output units differ', units=point_units))
            continue
        unit = point_units[0]
        scale = max(abs(r['value']) for r in rows)
        fallback = unit not in ABSOLUTE_TOLERANCES
        # No fixed engineering-scale floor for unknown units. At zero the
        # floor is zero; subnormal values may underflow to exact comparison.
        absolute = scale * 1e-12 if fallback else ABSOLUTE_TOLERANCES[unit]
        source = 'fallback' if fallback else (metadata.get(key[3], {}).get('unit_source') or 'native_result')
        policy = tolerances.setdefault(key[3], {'unit': unit, 'absolute': absolute,
            'relative': RELATIVE_TOLERANCE, 'unknown_unit_fallback': fallback,
            'source': source, 'absolute_source': 'magnitude * 1e-12' if fallback else 'unit absolute tolerance',
            'actual_min': None, 'actual_max': None})
        policy['absolute'] = max(policy['absolute'], absolute)
        paired += 1
        for (left_mode, left), (right_mode, right) in itertools.combinations(zip(modes, rows), 2):
            pair_count += 1
            delta = abs(left['value'] - right['value'])
            tolerance = max(absolute, RELATIVE_TOLERANCE * max(abs(left['value']), abs(right['value'])))
            policy['actual_min'] = tolerance if policy['actual_min'] is None else min(policy['actual_min'], tolerance)
            policy['actual_max'] = tolerance if policy['actual_max'] is None else max(policy['actual_max'], tolerance)
            entry = dict(identity, modes=[left_mode, right_mode], values=[left['value'], right['value']],
                         deviation=delta, tolerance=tolerance)
            if maximum is None or delta > maximum['deviation']:
                maximum = entry
            if delta > tolerance:
                mismatches.append(entry)
    for output_key in sorted(required_keys or []):
        if output_key in tolerances:
            continue
        output = metadata.get(output_key) or {}
        unit = output.get('unit') or output.get('units') or ''
        fallback = unit not in ABSOLUTE_TOLERANCES
        tolerances[output_key] = {'unit': unit, 'absolute': ABSOLUTE_TOLERANCES.get(unit),
            'relative': RELATIVE_TOLERANCE, 'unknown_unit_fallback': fallback,
            'source': 'fallback' if fallback else output.get('unit_source', 'native_result'),
            'absolute_source': 'magnitude * 1e-12' if fallback else 'unit absolute tolerance',
            'actual_min': None, 'actual_max': None,
            'not_applied_reason': 'No finite comparable triplet'}
    status = ('FAIL' if mismatches or decisions else 'NOT_TESTED' if not paired else
              'WARN' if unpaired or missing_decisions else 'PASS')
    return result(status, criterion, paired_items=paired, pair_count=pair_count,
                  maximum_deviation=maximum, tolerances=tolerances,
                  compared_candidate_count=len(eligible), compared_candidates=sorted(eligible),
                  completed_candidates_by_mode={m: sum(r.get('status') == 'completed' for r in rows) for m, rows in cases.items()},
                  output_classification=classified,
                  outside_tolerance=mismatches[:20], outside_tolerance_count=len(mismatches),
                  decision_mismatches=decisions, unpaired=unpaired,
                  missing_decisions=missing_decisions)


def volume_check(path):
    """Stream exactly one owned JSONL dataset, no directory/history traversal.

    Record sizes are exact; field/subtree sizes use canonical value encoding
    (key/punctuation envelope excluded). Nested observation bytes are not additive.
    """
    criterion = 'dataset bytes / distinct simulation points <= 262144 bytes'
    kinds, fields, observations, points = collections.Counter(), collections.Counter(), {}, set()
    observation_count, observation_bytes, unknown, total = 0, 0, 0, 0
    query_census, incomplete_census, unknown_originals = {}, set(), set()
    def visit(value, location, revision, v2=False):
        nonlocal observation_count, observation_bytes, unknown
        if isinstance(value, dict):
            for name, child in value.items():
                route = location + '.' + name
                if name == 'scheduler_observation' and isinstance(child, dict):
                    raw = encoded(child)
                    digest = hashlib.sha256(raw).hexdigest()
                    entry = observations.setdefault(digest, {'sha256': digest, 'bytes': len(raw),
                        'occurrences': 0, 'record_revisions': set(), 'locations': []})
                    entry['occurrences'] += 1
                    entry['record_revisions'].add(revision)
                    if len(entry['locations']) < 20:
                        entry['locations'].append('{}:{}'.format(revision, route))
                    observation_count += 1
                    observation_bytes += len(raw)
                    counts = child.get('diagnostic_query_states')
                    # Identity binds each task; repeated snapshots use its largest census.
                    identity = tuple(str(child.get(k)) for k in ('candidate_id', 'test', 'corner', 'point_id', 'task_id', 'job_id'))
                    log = child.get('query_log')
                    if v2 and isinstance(log, dict):
                        entries = log.get('entries') or []
                        # Aliases and growing snapshots may refer to the same
                        # original. Bind its index/hash to its owned log path;
                        # cancellation logs are distinct even for the same task.
                        owner = log.get('path') or identity
                        for query in entries:
                            if str(query.get('state', '')).upper() == 'UNKNOWN':
                                unknown += 1
                                unknown_originals.add((owner, query.get('index'), query.get('sha256')))
                        if (len(entries) != child.get('query_count')
                                or any(not isinstance(q.get('state'), str) or not q['state'] for q in entries)):
                            incomplete_census.add(identity)
                    elif isinstance(counts, dict):
                        amount = counts.get('UNKNOWN', 0)
                        unknown += amount
                        query_census[identity] = max(query_census.get(identity, 0), amount)
                    else:
                        unknown += sum(str(s.get('observation', s).get('state', '')).upper() == 'UNKNOWN'
                            for s in child.get('samples') or [] if isinstance(s, dict))
                        if child.get('query_count'):
                            incomplete_census.add(identity)
                visit(child, route, revision, v2)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, location + '[{}]'.format(index), revision, v2)
    try:
        if not path or not os.path.exists(path):
            return result('NOT_TESTED', criterion, reason='Owned dataset unavailable')
        from .run_data_store import _open_read, MAX_RECORD_BYTES, RunDataReader, RECORD_V2
        v2_reader = RunDataReader(path)
        with _open_read(path) as stream:
            initial = os.fstat(stream.fileno())
            while True:
                raw = stream.readline(MAX_RECORD_BYTES + 1)
                if not raw:
                    break
                if len(raw) > MAX_RECORD_BYTES or not raw.endswith(b'\n'):
                    raise ValueError('Incomplete or oversized dataset record')
                document = json.loads(raw.decode('utf-8'))
                kind, payload = document['kind'], document['payload']
                total += len(raw)
                kinds[kind] += len(raw)
                logical_payload = payload
                if document.get('schema_version') == RECORD_V2:
                    v2_reader._apply(document)
                    if kind == 'evidence':
                        logical_payload = None
                    elif kind == 'candidates':
                        logical_payload = [v2_reader.codec.candidate(row) for row in payload]
                    elif kind == 'candidate_patch':
                        logical_payload = [v2_reader.rows[row['candidate_id']] for row in payload]
                    else:
                        logical_payload = v2_reader.codec.resolve(payload)
                rows = payload if isinstance(payload, list) else [payload]
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    for name, value in row.items():
                        fields[kind + '.' + name] += len(encoded(value))
                rows = logical_payload if isinstance(logical_payload, list) else [logical_payload]
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if kind == 'point':
                        points.add(tuple(str(row.get(k)) for k in ('candidate_id', 'test', 'corner', 'point_id', 'task_id')))
                    if kind in ('candidates', 'candidate_patch'):
                        for point in (row.get('metrics') or {}).get('diagnostic_raw_points') or []:
                            points.add((str(row.get('candidate_id')), str(point.get('test')),
                                        str(point.get('corner')), str(point.get('point_id')), 'raw_output_point'))
                visit(logical_payload, kind, document.get('revision'),
                      document.get('schema_version') == RECORD_V2)
            final = os.fstat(stream.fileno())
            if initial.st_size != total or initial.st_size != final.st_size or initial.st_mtime != final.st_mtime:
                raise ValueError('Dataset changed during read-only profiling')
        # Prefer durable point receipts; native Run has only retained raw output points.
        receipt_points = [p for p in points if p[-1] != 'raw_output_point']
        count = len(receipt_points) if receipt_points else len(points)
        average = total / float(count) if count else None
        duplicates = []
        for entry in observations.values():
            entry['record_count'] = len(entry.pop('record_revisions'))
            if entry['occurrences'] > 1:
                duplicates.append(entry)
        return result('NOT_TESTED' if average is None else 'WARN' if average > VOLUME_LIMIT else 'PASS',
                      criterion, file_bytes=total, point_count=count, bytes_per_point=average,
                      records_top10=kinds.most_common(10), fields_top10=fields.most_common(10),
                      scheduler_observation_count=observation_count, scheduler_observation_bytes=observation_bytes,
                      scheduler_unknown_occurrences=unknown,
                      scheduler_unknown_queries=sum(query_census.values()) + len(unknown_originals) if not incomplete_census else None,
                      scheduler_unknown_census_complete=not bool(incomplete_census),
                      scheduler_unique_observations=len(observations),
                      scheduler_duplicates=sorted(duplicates, key=lambda d: -d['occurrences']),
                      accounting='exact JSONL bytes; canonical top-level field values; nested observation occurrences include aliases')
    except Exception as error:
        return result('FAIL', criterion, error=str(error))


def prepared_statistics(prepared):
    """Format dispatcher. Only explicit current prepared references are counted."""
    schema = prepared.get('schema_version', 'legacy')
    if schema not in ('legacy', _contract_text('crescendo.prepared.v1'),
                       _contract_text('crescendo.prepared.v2')):
        return {'format': schema, 'available': False, 'note': 'Unknown prepared format; adapter required'}
    paths, omissions = set(), []
    baseline = prepared.get('setup_baseline_snapshot') or {}
    if baseline.get('path'):
        paths.add(baseline['path'])
    owner = prepared.get('setup_private_session') or {}
    if owner.get('root'):
        state_path = os.path.join(owner['root'], _contract_text('session_state.json'))
        if os.path.isfile(state_path):
            with open(state_path, 'rb') as stream:
                raw = stream.read(1024 * 1024 + 1)
            if len(raw) <= 1024 * 1024:
                state = json.loads(raw.decode('utf-8'))
                digest = state.get('prepared_hash') or ''
                if len(digest) == 64 and all(c in '0123456789abcdef' for c in digest):
                    paths.add(os.path.join(owner['root'], 'prepared-' + digest + '.json'))
                else:
                    omissions.append('Current session has no valid prepared reference')
            else:
                omissions.append('Current session metadata exceeds 1 MiB read limit')
        else:
            omissions.append('Current session metadata unavailable')
    if schema == _contract_text('crescendo.prepared.v2'):
        return _prepared_v2_statistics(prepared, paths, omissions)
    files, missing = [], []
    for path in sorted(paths):
        if not os.path.exists(path):
            missing.append(path)
            continue
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode):
            missing.append(path)
            continue
        files.append({'path': path, 'bytes': info.st_size, 'inode': [info.st_dev, info.st_ino]})
    parameters = prepared.get('parameters') or []
    # iterencode avoids constructing another large serialization in memory.
    memory_bytes = None if paths else sum(len(chunk.encode('utf-8')) for chunk in
        json.JSONEncoder(ensure_ascii=True, separators=(',', ':'), allow_nan=False).iterencode(prepared))
    unique = {tuple(f['inode']): f['bytes'] for f in files}
    return {'format': schema, 'available': not missing and not omissions, 'note': 'Legacy inline prepared format',
            'files': files, 'copies': len(files) if paths else 1,
            'bytes': sum(f['bytes'] for f in files) if paths else memory_bytes,
            'physical_bytes': sum(unique.values()) if paths else None,
            'scope': 'explicit current prepared references only; in-memory canonical bytes if no file reference',
            'missing': missing, 'omissions': omissions, 'parameter_count': len(parameters),
            'optimizable_count': sum(p.get('optimizable') is True for p in parameters)}


def _prepared_v2_statistics(prepared, paths, omissions):
    """Count only the current references and their verified CAS graph.

    Logical bytes are canonical resolved JSON per copy. CAS bytes count unique
    reachable objects once; reference bytes are reported separately. No history
    or object-directory scan, and no publication/repair is performed here.
    """
    from . import catalog_json, prepared_store
    def size(value):
        return sum(len(part.encode('utf-8')) for part in catalog_json.chunks(value))
    files, objects, missing = [], {}, []
    logical_bytes, cas_references = 0, 0
    for path in sorted(paths):
        try:
            prepared_store._regular(path)
            with open(path, encoding='utf-8') as stream:
                document = catalog_json.load(stream)
            resolved = prepared_store.resolve(document)
            if (not isinstance(resolved, dict) or
                    resolved.get('schema_version') != _contract_text('crescendo.prepared.v2')):
                raise ValueError('Prepared format differs from current catalogue')
            reachable = []
            if document.get('schema_version') == prepared_store.SCHEMA:
                payload = prepared_store._get('prepared', document['payload'])
                reachable = [document['payload']] + list(payload['objects'].values())
            logical_bytes += size(resolved)
            info = os.lstat(path)
            files.append({'path': path, 'bytes': info.st_size, 'inode': [info.st_dev, info.st_ino]})
            cas_references += len(reachable)
            for reference in reachable:
                info = prepared_store._regular(reference['path'])
                objects[(info.st_dev, info.st_ino)] = {'path': reference['path'], 'bytes': info.st_size,
                    'kind': reference['kind'], 'sha256': reference['sha256']}
        except (OSError, ValueError, KeyError, TypeError) as error:
            missing.append(path)
            omissions.append(str(error))
    if not paths:
        logical_bytes = size(prepared)
    parameters = prepared.get('parameters') or []
    cas_bytes = sum(obj['bytes'] for obj in objects.values())
    reference_bytes = sum({tuple(f['inode']): f['bytes'] for f in files}.values())
    return {'format': prepared.get('schema_version'), 'available': not missing and not omissions,
            'note': 'Resolved canonical bytes per copy; unique reachable CAS bytes',
            'files': files, 'cas_objects': sorted(objects.values(), key=lambda obj: obj['path']),
            'copies': len(files) if paths else 1, 'reference_count': len(files),
            'cas_reference_count': cas_references, 'cas_object_count': len(objects),
            'bytes': logical_bytes, 'logical_bytes': logical_bytes, 'cas_bytes': cas_bytes,
            'reference_bytes': reference_bytes, 'physical_bytes': cas_bytes + reference_bytes,
            'scope': 'explicit current references and reachable CAS only; in-memory canonical bytes if no reference',
            'missing': missing, 'omissions': omissions, 'parameter_count': len(parameters),
            'optimizable_count': sum(p.get('optimizable') is True for p in parameters)}


def setup_check(statistics, gui=None):
    criterion = 'Tk main-loop gap <= 1 s; count gaps > 200 ms; separate diagnostic and latest Load/Apply windows'
    gui = gui or {}
    windows = gui.get('windows') or {}
    measured = [w for w in windows.values() if w.get('samples', 0) > 0]
    maximum = max([w.get('max_pause_seconds', 0) for w in measured] or [0])
    status = ('FAIL' if statistics.get('error') else
              'NOT_TESTED' if not measured or gui.get('available') is False or not statistics.get('available') else
              'WARN' if maximum > 1 else 'PASS')
    return result(status, criterion, prepared=statistics, gui=gui,
                  max_pause_seconds=maximum if measured else None)


def _frozen_outputs(prepared):
    """Enrich the diagnostic catalogue from saved Setup, without changing it.

    Production parsed metrics and the selectable prepared catalogue can omit
    units. Keep those report-only units attached to their exact Test/output key.
    """
    setup = prepared.get('setup') or {}
    sources = [dict(output, test=test['name']) for test in setup.get('tests') or []
               for output in test.get('outputs') or []]
    sources += list(setup.get('outputs') or [])
    sources += list(prepared.get('outputs') or [])
    outputs = collections.OrderedDict()
    for source in sources:
        key = source.get('key') or source.get('output_key')
        if not key:
            name, test = source.get('name'), source.get('test')
            if not name or not test or name.startswith('/'):
                continue
            key = '%s::%s' % (test, name)
        previous = outputs.get(key) or {}
        unit = source.get('unit') or source.get('units') or previous.get('unit') or ''
        outputs[key] = dict(previous, **source)
        outputs[key].update(key=key, unit=unit)
        if source.get('unit') or source.get('units'):
            outputs[key]['unit_source'] = 'frozen_setup'
    return list(outputs.values())


def _comparison_outputs(outputs, spec, cases):
    """Scope report expectations to metrics and their same-Test dependencies."""
    import re
    from .netlist_measurement import evaluation_plan
    definitions = list(spec.get('objectives') or []) + list(spec.get('constraints') or [])
    if not definitions:
        definitions = [d for case in cases.values() for c in case.get('candidates') or []
                       for field in ('objectives', 'constraints') for d in (c.get('metrics') or {}).get(field) or []]
    roots = {d['output_key'] for d in definitions if d.get('output_key')}
    observed, waves, scalars = set(), set(), set()
    for case in cases.values():
        for candidate in case.get('candidates') or []:
            for point in (candidate.get('metrics') or {}).get('diagnostic_raw_points') or []:
                observed.add(point.get('output'))
                if point.get('output_status') == 'waveform':
                    waves.add(point['output'])
                value = point.get('value')
                if type(value) in (int, float) and math.isfinite(value):
                    scalars.add(point['output'])
    waves.difference_update(scalars)
    # Old diagnostics without metric definitions can still compare observed
    # scalars, but must not turn the whole saved catalogue into expectations.
    if not roots:
        roots = observed - waves
    by_key = {o['key']: dict(o) for o in outputs}
    for key in roots - set(by_key):
        by_key[key] = {'key': key, 'name': key.rsplit('::', 1)[-1], 'test': key.rsplit('::', 1)[0]}
    closure, required_corners = set(roots), {}
    root_corners = {}
    for key in roots:
        selections = [d.get('corners') for d in definitions if d.get('output_key') == key]
        root_corners[key] = (None if not selections or any(not c for c in selections) else
                             sorted({c for corners in selections for c in corners}))
    for test in sorted({o.get('test') or o['key'].rsplit('::', 1)[0] for o in by_key.values()}):
        rows = [o for o in by_key.values() if (o.get('test') or o['key'].rsplit('::', 1)[0]) == test]
        ordered, unused = evaluation_plan({'test': test, 'outputs': rows}, roots)
        closure.update(o['key'] for o in ordered)
        for key in sorted(roots & {o['key'] for o in rows}):
            dependencies, unused = evaluation_plan({'test': test, 'outputs': rows}, [key])
            for output in dependencies:
                dependency = output['key']
                corners = root_corners[key]
                if dependency not in required_corners:
                    required_corners[dependency] = corners
                elif required_corners[dependency] is None or corners is None:
                    required_corners[dependency] = None
                else:
                    required_corners[dependency] = sorted(set(required_corners[dependency]) | set(corners))
        aliases = {o.get('name') or o['key'].rsplit('::', 1)[-1]: o for o in rows}
        for output in rows:
            kind = str(output.get('output_type') or output.get('type') or '').lower()
            if kind in ('wave', 'waveform', 'vector', 'signal'):
                waves.add(output['key'])
        # Only required consumers can supply structural type evidence. An idle
        # expression must not change expectations for a selected output.
        for output in ordered:
            output = by_key[output['key']]
            expression = output.get('expression') or output.get('expr') or ''
            match = re.fullmatch(r'\s*value\(\s*([A-Za-z_][A-Za-z0-9_]*)\s+[^()]+\)\s*', expression)
            if match and match.group(1) in aliases:
                source = aliases[match.group(1)]
                if source['key'] not in scalars:
                    waves.add(source['key'])
                if not output.get('unit') and source.get('unit'):
                    output.update(unit=source['unit'], unit_source='inferred_value_dependency')
    # Any finite scalar readback keeps this output in scalar comparisons, even
    # when another mode or saved metadata calls it a waveform. Otherwise a type
    # conflict could hide both numeric differences and missing observations.
    waves.difference_update(scalars)
    required = sorted(closure - waves)
    return list(by_key.values()), {
        'required_scalars': required, 'selected_metrics': sorted(roots),
        'required_corners': {key: required_corners.get(key) for key in required},
        'waveforms': [{'output': key, 'status': 'not_compared',
                       'reason': 'Frequency-axis waveform evidence unavailable; no waveform comparison performed',
                       'required_dependency': key in closure} for key in sorted(waves)],
        'idle': [{'output': key, 'reason': 'Not selected by objectives or constraints and not a required dependency'}
                 for key in sorted(set(by_key) - closure - waves)]}


def _expected_run_points(prepared, spec):
    """Use the execution planners and Test masks over this frozen Setup only."""
    from .test_selection import selection_plan as test_plan
    from .corner_selection import selection_plan as corner_plan
    from .ade_setup import test_corner_availability
    setup = prepared.get('setup') or {}
    tests = setup.get('tests') or []
    plan = test_plan(setup, spec)
    selected = (plan['selected'] if plan is not None else
                [test['name'] for test in tests if test.get('enabled', True)])
    # A shallow projection is sufficient: the planners only inspect these flags.
    # Avoid copying the potentially large prepared hierarchy and parameter data.
    selected_setup = dict(setup, tests=[dict(test, enabled=test['name'] in selected) for test in tests])
    corners = corner_plan(selected_setup, spec)
    selected_corners = set(corners['selected']) if corners is not None else None
    return [(test, corner['name']) for test in selected
            for corner in test_corner_availability(setup, test)
            if corner['enabled'] and corner.get('test_enabled') is True
            and (selected_corners is None or corner['name'] in selected_corners)]


def focused_checks(row, gui=None):
    cases = row.get('focused_cases') or {}
    checks = collections.OrderedDict()
    checks['F1'] = {mode: checkpoint_check(case.get('generation_timings') or [])
                    for mode, case in row['case_results'].items()}
    expected_candidates = {'g%03d_c%04d' % (item['generation'], item['index']): item['values']
                           for item in (row.get('effective_diagnostic') or {}).get('plan', row.get('frozen_plan')) or []}
    try:
        expected_points = _expected_run_points(row['prepared'], row.get('spec') or {})
        outputs, classified = _comparison_outputs(_frozen_outputs(row['prepared']), row.get('spec') or {}, cases)
        checks['F2'] = consistency_check({mode: cases.get(mode, {}).get('candidates') or []
                                        for mode in row['case_results']}, outputs,
                                        expected_points, expected_candidates, classified)
    except (KeyError, TypeError, ValueError) as error:
        checks['F2'] = result('NOT_TESTED', 'Frozen run selection must be available', reason=str(error))
    checks['F3'] = {mode: cases.get(mode, {}).get('volume') or
                   result('NOT_TESTED', 'bytes/point <= 262144', reason='Mode did not produce a dataset')
                   for mode in row['case_results']}
    checks['F4'] = setup_check(row.get('prepared_statistics') or {}, gui)
    for name in ('F1', 'F3'):
        modes = checks[name]
        statuses = [v['status'] for v in modes.values()]
        status = next((s for s in ('FAIL', 'WARN', 'NOT_TESTED', 'PASS') if s in statuses), 'NOT_TESTED')
        checks[name] = {'status': status, 'modes': modes}
    from .solver_transient import retry_observations
    checks['netlist_preparation'] = {'observation_only': True,
        'scope': 'Exclusive preparation stages; nested within generation wall time',
        'modes': {mode: [dict(generation=t.get('generation'),
                             timing=t.get('netlist_preparation') or {})
                         for t in case.get('generation_timings') or []]
                  for mode, case in row['case_results'].items()}}
    checks['solver_transient'] = {'observation_only': True,
        'modes': {mode: retry_observations(cases.get(mode, {}).get('candidates') or [])
                  for mode in row['case_results']}}
    return checks
