"""Versioned native candidate axis and exact-history result partitioning.

No point identity is inferred from an ordinal or from completion order. Equal
logical inputs share a physical row only under the deterministic qualification.
"""
import copy
import hashlib
import json
import os
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json

SCHEMA = _contract_text('crescendo.generation_batch.v1')
NETLIST_BATCH_PROTOCOLS = {
    'maestro_netlist': _contract_text('crescendo.maestro_netlist_batch.v1'),
    'private_netlist': _contract_text('crescendo.private_netlist_batch.v1'),
}


def generation_batching_enabled(spec):
    if spec.get('native_candidate_batching') != 'generation':
        return False
    expected = NETLIST_BATCH_PROTOCOLS.get(spec.get('execution_backend'))
    return expected is None or spec.get('native_batching_protocol') == expected


def validate_batching_protocol(spec):
    protocol = spec.get('native_batching_protocol')
    if protocol is not None and protocol != NETLIST_BATCH_PROTOCOLS.get(spec.get('execution_backend')):
        raise ValueError('Unsupported native batching checkpoint protocol')


class BatchUnsupported(ValueError):
    """Safe serial fallback, only before a native submission intent exists."""


def freeze_batching(spec, config, resuming=False):
    value = spec.get('native_candidate_batching')
    if resuming and value is None:
        # Absence is the legacy serial identity. Do not add fields to its
        # continuation schema or silently migrate the saved checkpoint.
        return 'serial'
    if value is None:
        value = 'serial' if resuming else (config.get('runtime') or {}).get('native_candidate_batching', 'serial')
    if value not in ('serial', 'generation'):
        raise ValueError('native_candidate_batching must be serial or generation')
    spec['native_candidate_batching'] = value
    if value == 'generation':
        if spec.get('native_batch_schema', SCHEMA if not resuming else None) != SCHEMA:
            raise ValueError('Unsupported native batch checkpoint schema')
        spec['native_batch_schema'] = SCHEMA
    expected = NETLIST_BATCH_PROTOCOLS.get(spec.get('execution_backend'))
    if value == 'generation' and expected is not None and not resuming:
        spec.setdefault('native_batching_protocol', expected)
    validate_batching_protocol(spec)
    return value if generation_batching_enabled(spec) else 'serial'


def build_generation_plan(generation, pending, parameters, identity):
    ids = sorted(row['parameter_id'] for row in parameters)
    if not ids or len(set(ids)) != len(ids) or not pending:
        raise BatchUnsupported('Empty or ambiguous candidate axis')
    candidates, physical, signatures = [], [], {}
    for index, values in pending:
        if set(values) != set(ids):
            raise BatchUnsupported('Candidate columns have different parameter identities')
        normalized = []
        from .hierarchy import parse_physical_number
        for key in ids:
            number = parse_physical_number(values[key])
            if number is None:
                raise BatchUnsupported('Candidate column is not numeric')
            import math
            if not math.isfinite(number):
                raise BatchUnsupported('Candidate column is not finite')
            normalized.append(number)
        signature = tuple(normalized)
        if signature not in signatures:
            signatures[signature] = len(physical)
            physical.append({'row_id': 'r%04d' % len(physical), 'values': dict(values),
                             'normalized': normalized, 'candidate_ids': []})
        row = physical[signatures[signature]]
        candidate_id = 'g%03d_c%04d' % (generation, index)
        if candidate_id in [c['candidate_id'] for c in candidates]:
            raise BatchUnsupported('Duplicate logical candidate identity')
        row['candidate_ids'].append(candidate_id)
        candidates.append({'candidate_id': candidate_id, 'index': index, 'generation': generation,
                           'values': dict(values), 'row_id': row['row_id']})
    return {'schema_version': SCHEMA, 'batch_epoch': uuid.uuid4().hex,
            'generation': generation, 'parameter_ids': ids, 'candidates': candidates,
            'rows': physical, 'identity': copy.deepcopy(identity), 'submitted': False,
            'logical_count': len(candidates), 'physical_row_count': len(physical)}


def qualify_deterministic(setup):
    # Closed set: stochastic/unknown analyses cannot share duplicate inputs.
    allowed = {'dc', 'ac', 'tran', 'sp', 'noise', 'xf', 'pz', 'pss', 'pac', 'pnoise', 'pxf', 'hb', 'hbac', 'hbnoise', 'stb', 'pstb'}
    for test in setup.get('tests') or []:
        if not test.get('enabled', True):
            continue
        analyses = [a.get('name', '').lower() for a in test.get('analyses') or [] if a.get('enabled', True)]
        if not analyses or any(a not in allowed for a in analyses):
            raise BatchUnsupported('Analysis lacks deterministic native batch qualification')
    # Saved native tran forms include noiseseed=1 even when noise is off.
    # Spectre enables transient noise with nonzero noisefmax, not a seed alone.
    # Ignore only that known inactive field; opaque/stochastic metadata remains
    # conservative, and this does not grant duplicate-model sharing authority.
    def active_metadata(value):
        if isinstance(value, list):
            return [active_metadata(item) for item in value]
        if not isinstance(value, dict):
            return value
        result = {key: active_metadata(item) for key, item in value.items()}
        if str(result.get('name', '')).lower() == 'tran' and isinstance(result.get('fields'), dict):
            fields = result['fields']
            if (fields.get('noisefmax') not in (None, '', 0, '0') or
                    fields.get('noiseonoff') not in (None, '', False, 'no', 'off')):
                raise BatchUnsupported('Transient noise requires independent serial samples')
            fields.pop('noiseseed', None)
        return result
    text = json.dumps(active_metadata(setup), sort_keys=True).lower()
    if any(word in text for word in ('montecarlo', 'monte carlo', 'statistics', 'mismatch', 'random', 'seed')):
        raise BatchUnsupported('Stochastic setup requires independent serial samples')


def build_native_columns(plan, parameter_rows):
    parameter_rows = sorted(parameter_rows, key=lambda r: r['parameter_id'])
    paths = [row['path'] for row in parameter_rows]
    if len(paths) != len(plan['parameter_ids']) or len(set(paths)) != len(paths):
        raise BatchUnsupported('Native parameter columns are ambiguous')
    columns = []
    for key, path in zip(plan['parameter_ids'], paths):
        # Reuse the scalar validator; the only new syntax is a space-separated
        # column of independently validated finite scalar inputs.
        values = []
        for row in plan['rows']:
            item = next(r for r in parameter_rows if r['parameter_id'] == key)
            item = dict(item, value=row['values'][key])
            from .native_maestro_parameters import _number
            values.append(_number(item['value']))
        columns.append({'parameter_id': key, 'path': path, 'values': values, 'text': ' '.join(values)})
    if not columns or len(set(len(c['values']) for c in columns)) != 1:
        raise BatchUnsupported('Native columns must have equal lengths')
    plan['columns'] = columns
    plan['owned_fingerprint'] = hashlib.sha256(json.dumps(columns, sort_keys=True).encode('utf-8')).hexdigest()
    return columns


def parse_native_rows(path):
    from .workspace import _record_literal
    rows, conditions, corner_parameters = [], {}, {}
    with open(path, encoding='utf-8') as stream:
        lines = stream.read().splitlines()
    if not lines or lines[0] != _contract_text('CR_BATCH_POINTS_1') or lines[-1] != 'END':
        raise ValueError('Native batch point readback incomplete')
    for line in lines[1:-1]:
        fields = line.split('\t')
        if fields[0] == 'CONDITION' and len(fields) == 4:
            design, name, value = [_record_literal(v) for v in fields[1:]]
            item = conditions.setdefault(design, {})
            if name in item:
                raise ValueError('Duplicate native condition')
            item[name] = value
        elif fields[0] == 'PARAM' and len(fields) == 5:
            design, corner, name, value = [_record_literal(v) for v in fields[1:]]
            key = (design, corner)
            if name in corner_parameters.setdefault(key, {}):
                raise ValueError('Duplicate corner parameter')
            corner_parameters[key][name] = value
        elif fields[0] == 'POINT' and len(fields) == 6:
            design, corner, test, directory, params = [_record_literal(v) for v in fields[1:]]
            rows.append({'design_point': design, 'corner': corner, 'test': test,
                         'results_dir': directory, 'corner_parameters': params})
        else:
            raise ValueError('Malformed native batch point readback')
    for row in rows:
        row['conditions'] = conditions.get(row['design_point'], {})
        row['corner_parameters'] = corner_parameters.get((row['design_point'], row['corner']), {})
    return rows


def map_native_points(plan, run_id, history, parsed, native_rows):
    from .workspace import _physical_values_match
    if not history or run_id is None:
        raise ValueError('Exact native run/history missing')
    points = {p['pointID']: p for p in parsed['points']}
    statuses = {(s['pointID'], s['test_name']): s for s in parsed['test_statuses']}
    if len(points) != len(parsed['points']) or len(statuses) != len(parsed['test_statuses']):
        raise ValueError('Duplicate RDB point or test status')
    mapped, seen = {}, set()
    for native in native_rows:
        conditions = native['conditions']
        matches = [r for r in plan['rows'] if all(c['path'] in conditions and
                   _physical_values_match(r['values'][c['parameter_id']], conditions[c['path']]) for c in plan['columns'])]
        if len(matches) != 1:
            raise ValueError('Native design conditions have no unique physical candidate row')
        # Native resultsDir is authoritative; directory number is independently
        # checked against the RDB, never derived from candidate ordinal.
        directory = os.path.normpath(native['results_dir'])
        try:
            point_id = int(os.path.basename(os.path.dirname(directory)))
        except ValueError:
            raise ValueError('Native resultsDir lacks physical RDB point identity')
        p = points.get(point_id)
        if not p or str(p['designPointNumber']) != str(native['design_point']) or p['corner_name'] != native['corner']:
            raise ValueError('Native/RDB design, corner or resultsDir identity differs')
        key = (point_id, native['test'])
        if key not in statuses or key in seen:
            raise ValueError('Native physical point absent or duplicated')
        seen.add(key)
        for column in plan['columns']:
            stem, prop = column['path'].rsplit('/', 1)
            name = stem + '.' + prop
            corner_params = native['corner_parameters']
            if name not in corner_params or not _physical_values_match(matches[0]['values'][column['parameter_id']], corner_params[name]):
                raise ValueError('Native RDB corner device parameter differs')
        row_id = matches[0]['row_id']
        record = dict(native, point_id=point_id, batch_epoch=plan['batch_epoch'], run_id=run_id, history=history)
        record['physical_id'] = json.dumps([plan['batch_epoch'], run_id, history, native['design_point'], native['test'], native['corner'], directory])
        mapped.setdefault(row_id, []).append(record)
    if seen != set(statuses) or set(mapped) != {r['row_id'] for r in plan['rows']}:
        raise ValueError('Missing or extra native batch points')
    plan['candidate_to_physical'] = {c['candidate_id']: [p['physical_id'] for p in mapped[c['row_id']]] for c in plan['candidates']}
    plan['physical_to_candidates'] = {}
    for cid, physical in plan['candidate_to_physical'].items():
        for point in physical:
            plan['physical_to_candidates'].setdefault(point, []).append(cid)
    plan.update(run_id=run_id, history=history, physical_count=len(seen), mapping=mapped)
    return mapped


class HistoryReferences(object):
    """Durable consumer ledger. Candidate compaction cannot own shared paths."""
    def __init__(self, path, plan):
        self.path = path
        self.data = {'schema_version': SCHEMA, 'batch_epoch': plan['batch_epoch'],
                     'consumers': {c['candidate_id']: False for c in plan['candidates']},
                     'shared_paths_retained': True}
        self.persist()

    def persist(self):
        self.data['remaining'] = sum(not v for v in self.data['consumers'].values())
        atomic_write_json(self.path, self.data)

    def commit(self, candidate_id):
        if candidate_id not in self.data['consumers']:
            raise ValueError('Unknown shared history consumer')
        self.data['consumers'][candidate_id] = True
        self.persist()


def native_batch_skill_lines(batch, scalar_plan, path):
    from .native_baseline import _quote as q
    paths = [c['path'] for c in batch['columns']]
    entries = ' '.join('list(%s %s)' % (q(c['path']), q(c['text'])) for c in batch['columns'])
    # First edition conservatively declines all source sets, including disjoint
    # ones. Never remove a user's group or its matched members to qualify.
    lines = [
        "unless(isCallable('ocnxlSweepParam) && isCallable('ocnxlParametricSet) && isCallable('ocnxlGetSession) && isCallable('maeGetParamConditions) error(\"Native batching APIs unavailable\\n\"))",
        'unless(equal(session ocnxlGetSession()) error("Native XL session differs\\n"))',
        _contract_text('crActive=axlGetActiveSetup(mainSDB)'),
        _contract_text('crSets=axlSDBGetChild(crActive "parametersets")'),
        _contract_text("crGroups=if(crSets then axlSDBGetChildren(crSets ?name \"parameterset\" ?returnAs 'handles) else nil)"),
        _contract_text('when(crGroups error("Source parameter sets need serial execution; user groups preserved\\n"))'),
        _contract_text('crBeforeCorners=ocnxlGetCorners()'),
        _contract_text('crPaths=list(%s)') % ' '.join(q(p) for p in paths),
        _contract_text('foreach(crP maeGetSetup(?typeName "parameters" ?session session)'),
        _contract_text(' unless(member(crP list(%s)) error("Unowned or matched source parameter conflict\\n")))') % ' '.join(q(p) for p in scalar_plan.get('owned_paths') or []),
        _contract_text('foreach(crP crPaths when(axlGetParameter(mainSDB crP) maeDeleteParameter(crP ?session session)))'),
        _contract_text('foreach(crEntry list(%s)') % entries,
        _contract_text(' unless(ocnxlSweepParam(car(crEntry) cadr(crEntry)) error("Native column rejected\\n")))'),
        _contract_text('unless(ocnxlParametricSet(crPaths) error("Native set rejected\\n"))'),
        _contract_text('crSets=axlSDBGetChild(crActive "parametersets")'),
        _contract_text("crGroups=axlSDBGetChildren(crSets ?name \"parameterset\" ?returnAs 'handles)"),
        _contract_text('unless(length(crGroups)==1 && axlGetEnabled(car(crGroups)) error("Native set count/enable differs\\n"))'),
        # Read actual members rather than trusting the requested member list.
        _contract_text('crMembers=nil'),
        _contract_text('crMembersRoot=axlSDBGetChild(car(crGroups) "parameters")'),
        _contract_text("foreach(crLoc axlSDBGetChildren(crMembersRoot ?name \"location\" ?returnAs 'handles)"),
        _contract_text(" foreach(crParam axlSDBGetChildren(crLoc ?name \"parameter\" ?returnAs 'handles)"),
        _contract_text('  crMembers=cons(strcat(axlSDBGetValue(crLoc) "/" axlSDBGetValue(crParam)) crMembers)))'),
        _contract_text('unless(length(crMembers)==length(crPaths) && forall(crP crPaths member(crP crMembers)) error("Native set matched members differ\\n"))'),
        'when(axlGetAllParametersDisabled(mainSDB) axlSetAllParametersDisabled(mainSDB 0))',
        _contract_text('unless(!axlGetAllParametersDisabled(mainSDB) && member(axlSDBGetChildVal(crActive "allparametersetsdisabled") list(nil "0" 0)) error("Native axis globally disabled\\n"))'),
        _contract_text('unless(equal(crBeforeCorners ocnxlGetCorners()) error("Native corners changed\\n"))'),
        _contract_text('crBatchOut=outfile(%s)') % q(path),
        _contract_text('unless(crBatchOut error("Native batch readback unavailable\\n"))'),
        _contract_text('foreach(crEntry list(%s)') % entries,
        _contract_text(' crP=car(crEntry)'),
        _contract_text(' unless(equal(maeGetParameter(crP ?session session) cadr(crEntry)) && axlGetEnabled(axlGetParameter(mainSDB crP)) error("Native column readback differs\\n"))'),
        _contract_text(' fprintf(crBatchOut "%s\\t%s\\n" crP maeGetParameter(crP ?session session)))'),
        _contract_text('fprintf(crBatchOut "OWNED\\t%s\\n")') % batch['owned_fingerprint'],
        _contract_text('fprintf(crBatchOut "AUDIT\\tgroup_enabled\\t%L\\n" axlGetEnabled(car(crGroups)))'),
        _contract_text('fprintf(crBatchOut "AUDIT\\tall_parameters_disabled\\t%L\\n" axlGetAllParametersDisabled(mainSDB))'),
        _contract_text('fprintf(crBatchOut "AUDIT\\tall_sets_disabled\\t%L\\n" axlSDBGetChildVal(crActive "allparametersetsdisabled"))'),
        _contract_text('fprintf(crBatchOut "AUDIT\\tcorners\\t%L\\n" ocnxlGetCorners())'),
        _contract_text('foreach(crP crPaths fprintf(crBatchOut "AUDIT\\tcolumn_enabled\\t%s\\t%L\\n" crP axlGetEnabled(axlGetParameter(mainSDB crP))))'),
        _contract_text('close(crBatchOut)'),
    ]
    return lines


def read_native_batch_columns(path, plan):
    with open(path, encoding='utf-8') as stream:
        rows = stream.read().splitlines()
    expected = [c['path'] + '\t' + c['text'] for c in plan['columns']]
    expected.append('OWNED\t' + plan['owned_fingerprint'])
    if rows[:len(expected)] != expected:
        raise ValueError('Native batch columns or fingerprint differ')
    audit = [row.split('\t')[1:] for row in rows[len(expected):] if row.startswith('AUDIT\t')]
    if (len(audit) != 4 + len(plan['columns']) or
            any(len(row) != 2 for row in audit[:4]) or
            any(len(row) != 3 or row[0] != 'column_enabled' for row in audit[4:]) or
            [row[0] for row in audit[:4]] != ['group_enabled', 'all_parameters_disabled', 'all_sets_disabled', 'corners'] or
            [row[1] for row in audit[4:]] != [c['path'] for c in plan['columns']]):
        raise ValueError('Native batch enable/corner audit incomplete')
    from .workspace import _record_literal
    if (_record_literal(audit[0][1]) not in (True, 1) or
            any(_record_literal(row[2]) not in (True, 1) for row in audit[4:]) or
            any(_record_literal(row[1]) not in (None, False, 0, '0') for row in audit[1:3])):
        raise ValueError('Native batch enable readback differs')
    plan['native_readback'] = {'columns': expected[:-1], 'group_enabled': audit[0][1],
        'all_parameters_disabled': audit[1][1], 'all_sets_disabled': audit[2][1],
        'corners': audit[3][1], 'column_enabled': {row[1]: row[2] for row in audit[4:]}}
    return {'ok': True, 'batch_readback': rows, 'native_readback': plan['native_readback']}


def native_point_export_lines(path):
    from .native_baseline import _quote as q
    return [
        'unless(maeOpenResults(?session session ?history historyName) error("Native exact results unavailable\\n"))',
        _contract_text('crBatchRdb=maeReadResDB(?historyName historyName ?session session)'),
        _contract_text('crBatchOut=outfile(%s)') % q(path),
        _contract_text('fprintf(crBatchOut "CR_BATCH_POINTS_1\\n")'),
        _contract_text('foreach(crPoint crBatchRdb->points()'),
        _contract_text(' foreach(crCond maeGetParamConditions(crPoint->id)'),
        _contract_text('  fprintf(crBatchOut "CONDITION\\t%L\\t%L\\t%L\\n" crPoint->id car(crCond) cadr(crCond)))'),
        _contract_text(' foreach(crCorner crPoint->corners()'),
        _contract_text('  foreach(crParam crCorner->params() fprintf(crBatchOut "PARAM\\t%L\\t%L\\t%L\\t%L\\n" crPoint->id crCorner->name crParam->name crParam->value))'),
        _contract_text('  foreach(crTest crCorner->tests()'),
        _contract_text('   fprintf(crBatchOut "POINT\\t%L\\t%L\\t%L\\t%L\\t%L\\n" crPoint->id crCorner->name crTest->name crTest->resultsDir nil))))'),
        _contract_text('fprintf(crBatchOut "END\\n") close(crBatchOut)'),
        'maeCloseResults()',
    ]


def batch_parameter_reader(requests, config, root, driver, cancel_check):
    from .native_point_parameters import read_point_parameters
    values = read_point_parameters(requests, config, root, native_session=driver, cancel_check=cancel_check) if any(r['names'] for r in requests) else [{} for r in requests]
    def reader(subset):
        result = []
        for request in subset:
            matches = [i for i, r in enumerate(requests) if r == request]
            if len(matches) != 1:
                raise ValueError('Final batch parameter request differs from exact readback')
            result.append(copy.deepcopy(values[matches[0]]))
        return result
    return reader


def native_batch_policy(policy):
    result = copy.deepcopy(policy)
    for scope in [result['global']] + result.get('tests', []):
        scope['properties'].update(usesameprocess='0', preemptivestart='0')
    return result


def qualify_shared_inputs(plan, prepared):
    if plan['logical_count'] == plan['physical_row_count']:
        plan['sharing_qualification'] = 'no_shared_rows'
        return
    # First edition proves sharing only for a closed, model-free primitive
    # fixture. Unknown model/statistical semantics use serial independent Runs.
    if (prepared.get('eda_context') or {}).get('pdk', {}).get('contexts'):
        raise BatchUnsupported('Duplicate candidates with external models require independent serial Runs')
    instances = [i for d in (prepared.get('hierarchy') or {}).get('designs') or []
                 for i in (d.get('hierarchy') or {}).get('instances') or [] if i.get('kind') == 'device']
    allowed = {'res', 'cap', 'ind', 'vdc', 'idc', 'gnd'}
    if not instances or any(i.get('master_library') != 'analogLib' or i.get('master_cell') not in allowed for i in instances):
        raise BatchUnsupported('Duplicate candidates lack model-free deterministic primitive qualification')
    plan['sharing_qualification'] = 'frozen_model_free_deterministic_primitives'


def require_history_released(reference, candidate_id):
    root = reference.get('root')
    if (not isinstance(root, str) or os.path.realpath(root) != root or
            os.path.basename(root) != 'batch_' + str(reference.get('batch_epoch'))):
        raise ValueError('Shared history owner differs')
    path = os.path.join(root, _contract_text('history_references.json'))
    if os.path.islink(path):
        raise ValueError('Shared history ledger is unsafe')
    with open(path, encoding='utf-8') as stream:
        data = json.load(stream)
    consumers = data.get('consumers') or {}
    if (data.get('schema_version') != SCHEMA or data.get('batch_epoch') != reference['batch_epoch']
            or consumers.get(candidate_id) is not True
            or any(v is not True and k not in data.get('retired_consumers', []) for k, v in consumers.items())
            or data.get('remaining') != 0):
        raise ValueError('Shared history still has uncommitted consumers')
    return data


def retire_resumed_history(reference, completed_ids, run_root):
    """Retire an old epoch after continuation ownership reconciliation.

    Unfinished consumers are retired, never relabelled as successful commits.
    The manifest, ledger and all shared paths remain available for audit.
    """
    root = reference.get('root')
    parent = os.path.realpath(os.path.join(run_root, 'candidates'))
    if (not isinstance(root, str) or os.path.realpath(root) != root or
            os.path.dirname(root) != parent or
            os.path.basename(root) != 'batch_' + str(reference.get('batch_epoch'))):
        raise ValueError('Resumed shared history is outside the previous run')
    path = os.path.join(root, _contract_text('history_references.json'))
    if os.path.islink(path):
        raise ValueError('Shared history ledger is unsafe')
    with open(path, encoding='utf-8') as stream:
        data = json.load(stream)
    consumers = data.get('consumers')
    if (data.get('schema_version') != SCHEMA or data.get('batch_epoch') != reference['batch_epoch']
            or not isinstance(consumers, dict) or not consumers or
            any(type(value) is not bool for value in consumers.values()) or
            not set(completed_ids).issubset(consumers)):
        raise ValueError('Resumed shared history identity differs')
    for candidate_id in completed_ids:
        consumers[candidate_id] = True
    data['retired_consumers'] = sorted(key for key, value in consumers.items() if not value)
    data['retirement_reason'] = 'continuation_reconciled_new_epoch'
    data['remaining'] = 0
    atomic_write_json(path, data)
    return data
