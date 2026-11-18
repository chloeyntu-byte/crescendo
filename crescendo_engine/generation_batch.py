"""Versioned native candidate axis and exact-history result partitioning.

No point identity is inferred from an ordinal or from completion order. Equal
logical inputs share a physical row only under the deterministic qualification.
"""
import copy
import hashlib
import json
import math
import os
import sys
import uuid
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, localcontext

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .config import DEFAULTS
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
        value = (config.get('runtime') or {}).get(
            'native_candidate_batching', DEFAULTS['runtime']['native_candidate_batching'])
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


# IC618 expands Device Parameter sets to seven significant decimal digits.
# Leave one guard digit for the native parametric set and point netlister.
_NATIVE_RUN_DIGITS = 6
_NATIVE_RUN_FORMAT = ".%dg" % _NATIVE_RUN_DIGITS


def _round_significant(number, rounding=ROUND_HALF_EVEN):
    if not number:
        return Decimal(0)
    unit = Decimal((0, (1,), number.adjusted() - _NATIVE_RUN_DIGITS + 1))
    return number.quantize(unit, rounding=rounding)


class NativeRunValueUnsupported(BatchUnsupported):
    """Precision failure with the values needed to explain a serial fallback."""

    def __init__(self, reason, variable, original, normalized=None, **details):
        key = variable['parameter_id']
        super().__init__(reason + ': ' + key)
        if isinstance(original, float) and not math.isfinite(original):
            original = repr(original)
        self.details = dict(details, parameter_id=key, original_value=original,
                            normalized_value=None if normalized is None else str(normalized),
                            reason=str(self))


def _native_grid_value(number, variable, step):
    lower = Decimal(str(variable['minimum']))
    upper = Decimal(str(variable['maximum']))
    origin = variable.get('grid_origin')
    origin = lower if origin is None else Decimal(str(origin))
    grid_inputs = (lower, upper, origin, number, step)
    with localcontext() as context:
        # Keep subtraction, the grid index and multiplication exact even
        # when the step and grid origin have very different magnitudes.
        context.prec = max(context.prec, max(x.adjusted() for x in grid_inputs)
                           - min(x.as_tuple().exponent for x in grid_inputs) + 8)
        index = ((number - origin) / step).to_integral_value(rounding=ROUND_HALF_EVEN)
        canonical = origin + index * step
        # The optimizer constructs points with float multiplication and
        # addition relative to the lower bound. Allow their arithmetic error
        # while keeping the tolerance small compared with a grid interval.
        tolerance = min(step / 8, 4 * Decimal(str(sys.float_info.epsilon))
                        * max(abs(number), abs(lower), abs(number - lower)))
        on_grid = (abs(number - canonical) <= tolerance
                   and lower - tolerance <= canonical <= upper + tolerance)
    return canonical, on_grid


def _native_run_value(original, variable, quantize, linked_coordinate):
    from .hierarchy import parse_physical_number
    from .optimizers import _decode

    number = parse_physical_number(original)
    if number is None:
        raise NativeRunValueUnsupported('Native batch value is not finite numeric', variable, original)
    kind = variable.get('value_type', 'float')
    step = variable.get('step')
    continuous = kind == 'float' and step is None
    if quantize and continuous and linked_coordinate is not None:
        number = _decode(linked_coordinate, variable)
    decimal_value = Decimal(str(number))

    if kind == 'integer' or (kind == 'float' and step is not None):
        canonical, on_grid = _native_grid_value(decimal_value, variable, Decimal(str(1 if step is None else step)))
        if not on_grid:
            raise NativeRunValueUnsupported('Native batch fixed candidate value is outside its grid',
                                            variable, original, canonical)
        rounded = _round_significant(canonical)
        if rounded != canonical:
            raise NativeRunValueUnsupported('Native batch fixed grid value requires more than six significant digits',
                                            variable, original, canonical, rounded_value=str(rounded))
        if quantize:
            return int(canonical) if kind == 'integer' else float(canonical)
        if canonical != decimal_value:
            raise NativeRunValueUnsupported('Native batch would change a fixed candidate value',
                                            variable, original, canonical)
        return original

    rounded = _round_significant(decimal_value)
    if continuous:
        lower = Decimal(str(variable['minimum']))
        upper = Decimal(str(variable['maximum']))
        if rounded < lower:
            rounded = _round_significant(lower, ROUND_CEILING)
        elif rounded > upper:
            rounded = _round_significant(upper, ROUND_FLOOR)
        if not lower <= rounded <= upper:
            raise NativeRunValueUnsupported('No representable Native batch value within bounds',
                                            variable, original, rounded)
        if quantize:
            return float(rounded)
    if rounded != decimal_value:
        raise NativeRunValueUnsupported('Native batch would change a fixed candidate value',
                                        variable, original, rounded)
    return original


def native_run_values(values, variables, quantize=False, allow_partial=False):
    """Qualify a Native Run row or prepare a new proposal for its frozen plan."""
    from .optimizers import _encode, validate_candidate_values

    result = dict(values)
    linked_coordinates, linked_groups = {}, {}
    failed_groups = set()
    for variable in variables:
        key = variable['parameter_id']
        group = variable.get('link_group')
        if group:
            linked_groups.setdefault(group, []).append(variable)
        try:
            result[key] = _native_run_value(values[key], variable, quantize, linked_coordinates.get(group))
        except NativeRunValueUnsupported:
            if not allow_partial:
                raise
            if group:
                failed_groups.add(group)
            continue
        if (quantize and group and group not in linked_coordinates
                and variable.get('value_type', 'float') == 'float' and variable.get('step') is None):
            linked_coordinates[group] = _encode(result[key], variable)

    for group, members in linked_groups.items():
        group_values = {member['parameter_id']: result[member['parameter_id']] for member in members}
        if group not in failed_groups:
            try:
                validate_candidate_values(group_values, members)
                continue
            except ValueError as error:
                if not allow_partial:
                    anchor = next((member for member in members if member.get('step') is None), members[0])
                    key = anchor['parameter_id']
                    raise NativeRunValueUnsupported('Native batch cannot preserve linked values: ' + str(error),
                                                    anchor, values[key], result[key], link_group=group,
                                                    linked_values=group_values)
        # A linked group must keep a shared coordinate in both batch and
        # scalar execution, even when only one member failed quantization.
        result.update((key, values[key]) for key in group_values)
    return result


def _native_run_values_match(expected, actual):
    from .hierarchy import parse_physical_number
    expected_number = parse_physical_number(expected)
    return expected_number is not None and expected_number == parse_physical_number(actual)


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
    for index, (key, path) in enumerate(zip(plan['parameter_ids'], paths)):
        # Reuse the scalar validator; the only new syntax is a space-separated
        # column of independently validated finite scalar inputs.
        values = []
        for row in plan['rows']:
            item = next(r for r in parameter_rows if r['parameter_id'] == key)
            item = dict(item, value=row['values'][key])
            from .native_maestro_parameters import _number
            if plan.get('native_run_exact_values'):
                values.append(format(row['normalized'][index], _NATIVE_RUN_FORMAT))
            else:
                values.append(_number(item['value']))
        column = {'parameter_id': key, 'path': path, 'values': values, 'text': ' '.join(values)}
        if item.get("source_kind") in ("global", "top"):
            column["source_kind"] = item["source_kind"]
            plan["design_variables_protocol"] = _contract_text("crescendo.design_variables.v1")
        columns.append(column)
    if not columns or len(set(len(c['values']) for c in columns)) != 1:
        raise BatchUnsupported('Native columns must have equal lengths')
    if any(c.get("source_kind") in ("global", "top") for c in columns):
        # IC618 separates variable and device parameter sets. Bind each mixed
        # device axis to an exact owned Global symbol, then zip all Globals.
        for column in columns:
            column["axis_kind"] = "global"
            if column.get("source_kind") not in ("global", "top"):
                column["native_device_path"] = column["path"]
                column["path"] = _contract_text("CRESCENDO_NL_BATCH_") + hashlib.sha256(column["parameter_id"].encode("utf-8")).hexdigest()[:16]
    if len(columns) == 1 and columns[0].get("axis_kind") == "global":
        # IC618 rejects one-member sets at Run. A constant owned companion
        # supplies the second member without adding an optimizer dimension.
        zeros = ["0"] * len(plan["rows"])
        columns.append(dict(parameter_id="$native_set_companion", path=_contract_text("CRESCENDO_NL_BATCH_COMPANION"),
            source_kind="global", axis_kind="global", auxiliary=True, values=zeros, text=" ".join(zeros)))
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


def map_native_points(plan, run_id, history, parsed, native_rows, allow_missing_status=False):
    from .workspace import _physical_values_match
    values_match = _native_run_values_match if plan.get('native_run_exact_values') else _physical_values_match
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
                   values_match(r['values'][c['parameter_id']], conditions[c['path']]) for c in plan['columns'] if not c.get('auxiliary'))]
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
        if key in seen or (key not in statuses and not allow_missing_status):
            raise ValueError('Native physical point absent or duplicated')
        seen.add(key)
        for column in plan['columns']:
            if column.get('auxiliary'):
                if not values_match(0, conditions.get(column['path'])):
                    raise ValueError('Native set companion condition differs')
                continue
            if column.get("axis_kind") == "global":
                name = column["path"]
            else:
                stem, prop = column['path'].rsplit('/', 1)
                name = stem + '.' + prop
            corner_params = native['corner_parameters']
            if name not in corner_params or not values_match(matches[0]['values'][column['parameter_id']], corner_params[name]):
                raise ValueError('Native RDB corner device parameter differs')
        row_id = matches[0]['row_id']
        record = dict(native, point_id=point_id, batch_epoch=plan['batch_epoch'], run_id=run_id, history=history)
        record['physical_id'] = json.dumps([plan['batch_epoch'], run_id, history, native['design_point'], native['test'], native['corner'], directory])
        mapped.setdefault(row_id, []).append(record)
    if not set(statuses).issubset(seen) or set(mapped) != {r['row_id'] for r in plan['rows']}:
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
    if any(c.get("source_kind") in ("global", "top") for c in batch["columns"]):
        from .design_variable_native import batch_lines
        return batch_lines(batch, scalar_plan, path)
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
