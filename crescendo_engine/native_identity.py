"""Saved scope and native point qualification, independent of corner spelling."""
import itertools
import json
import math
import os
import re
import sqlite3
from urllib.parse import quote

from .spectre_values import parse_spectre_number


def numeric_domain(expression):
    """Only literal lists are evaluated here; native formulas remain native."""
    # Quotes denote strings, including numeric-looking strings and spaces.
    if isinstance(expression, str) and '"' in expression:
        return None
    scalar = parse_spectre_number(expression)
    if scalar is not None:
        return [scalar]
    parts = str(expression or "").strip().split()
    values = [parse_spectre_number(part) for part in parts]
    if len(parts) > 1 and all(value is not None for value in values):
        return values
    # Known saved ADE linear sweep syntax. Auto/log sweeps stay native.
    match = re.match(r'^\{From/To\}Linear:([^:]+):([^:]+):([^:]+)\{From/To\}$', str(expression or ""))
    if match:
        start, step, stop = [parse_spectre_number(value) for value in match.groups()]
        if None not in (start, step, stop) and step and (stop - start) / step >= 0:
            count = int(math.floor((stop - start) / step + 1e-10)) + 1
            if 0 < count <= 100000:
                return [start + index * step for index in range(count)]
    return None


def literal_domain(expression):
    """Finite numeric or quoted-string literals; never evaluate a formula.

    JSON-compatible string escapes are decoded without collapsing whitespace.
    Unknown native escape/sweep forms remain unqualified instead of guessed.
    """
    if not isinstance(expression, str) or '"' not in expression:
        return numeric_domain(expression)
    text, values = expression.strip(), []
    decoder = json.JSONDecoder()
    while text:
        if text.startswith('"'):
            try:
                value, end = decoder.raw_decode(text)
            except (ValueError, TypeError):
                return None
            if not isinstance(value, str):
                return None
        else:
            match = re.match(r'[^\s"]+', text)
            if not match:
                return None
            end = match.end()
            value = parse_spectre_number(text[:end])
            if value is None:
                return None
        if end < len(text) and not text[end].isspace():
            return None
        values.append(value)
        if len(values) > 100000:
            return None
        text = text[end:].lstrip()
    return values or None


def _string_expression(expression):
    # Quotes used as arguments of a numeric formula do not make it a string.
    return isinstance(expression, str) and (expression.lstrip().startswith('"') or
        re.match(r'^strcat\s*\(', expression.lstrip()) is not None)


def _same_literal(actual, expected):
    if isinstance(expected, str):
        return isinstance(actual, str) and actual == expected
    if isinstance(actual, (bool, str)) or actual is None:
        return False
    try:
        return math.isclose(float(actual), expected, rel_tol=1e-10, abs_tol=1e-300)
    except (ValueError, TypeError, OverflowError):
        return False


def _native_literal(row):
    expression = row.get('expression')
    if isinstance(expression, str) and '"' in expression:
        domain = literal_domain(expression)
        return domain[0] if domain is not None and len(domain) == 1 else None
    return row.get('value')


def effective_variables(setup, test, corner=None):
    result = dict((row['name'], dict(row, source='test')) for row in test.get('variables') or [])
    from .variable_scope_contract import global_variable_status, VariableScopeContractError
    for row in setup.get('global_variables') or []:
        status = global_variable_status(setup, row, test)
        if status['applies'] is None:
            raise VariableScopeContractError({'issues': [{
                'code': 'global_variable_participation_unknown', 'scope': 'global',
                'name': row.get('name'), 'test': test.get('name'),
                'message': status.get('reason') or 'Global enabled state is unknown'}]},
                'resolve effective variables')
        if status['applies']:
            result[row['name']] = dict(row, source='global')
    plan = execution_plan_variables(setup, test['name'])
    result.update(plan['variables'])
    for row in (corner or {}).get('variables') or []:
        result[row['name']] = dict(row, source='corner')
    return result


def execution_plan_variables(setup, test):
    """Recognize only a uniquely enabled, explicitly scoped variable plan.

    Unknown native plan/sweep schemas retain their complete saved tree and
    fail qualification. An RDB value never authorizes overriding a constant.
    """
    variables, errors = {}, []

    def enabled(node):
        return str(node.get('attributes', {}).get('enabled', '1')).lower() not in ('0', 'false', 'nil', 'off')

    def children(node, tag):
        return [child for child in node.get('children', []) if child.get('tag') == tag]

    def read_plan(node, scope):
        unknown = [child.get('tag') for child in node.get('children', []) if child.get('tag') not in ('vars', 'tests', 'name')]
        if unknown:
            errors.append({'reason': 'saved_execution_plan_semantics_unresolved', 'tag': node.get('tag'), 'unknown_children': unknown})
            return
        masks = children(node, 'tests')
        if masks:
            selected = [row for mask in masks for row in children(mask, 'test') if enabled(row)]
            if not any(row.get('text') == test or row.get('attributes', {}).get('name') == test for row in selected):
                return
        if scope is not None and scope != test:
            return
        for parent in children(node, 'vars'):
            for row in parent.get('children', []):
                value_nodes = children(row, 'value')
                name = row.get('attributes', {}).get('name') or row.get('attributes', {}).get('Name') or row.get('text')
                if row.get('tag') != 'var' or not name or len(value_nodes) != 1 or any(child.get('tag') != 'value' for child in row.get('children', [])):
                    errors.append({'reason': 'saved_execution_plan_variable_unresolved', 'plan': node.get('text')})
                    continue
                if not enabled(row):
                    continue
                value = value_nodes[0].get('text')
                if name in variables and variables[name]['value'] != value:
                    errors.append({'reason': 'saved_execution_plan_variable_scope_ambiguous', 'variable': name})
                    continue
                variables[name] = {'name': name, 'value': value, 'source': 'runplan',
                                   'plan': node.get('text'), 'sweep_enabled': True}

    for setting in setup.get('execution_settings') or []:
        scope = setting.get('test')
        node = setting.get('content', setting)
        if scope is not None and scope != test:
            continue
        if not enabled(node) or not node.get('children') and not node.get('text'):
            continue
        tag = node.get('tag', '').replace('_', '')
        if tag in ('runplan', 'runplans'):
            plans = [node] if tag == 'runplan' else children(node, 'runplan')
            plans = [plan for plan in plans if enabled(plan)]
            if len(plans) > 1 or (tag == 'runplans' and len(children(node, 'runplan')) != len(node.get('children', []))):
                errors.append({'reason': 'saved_execution_plan_selection_unresolved', 'tag': tag, 'enabled_plan_count': len(plans)})
            elif plans:
                read_plan(plans[0], scope)
        else:
            errors.append({'reason': 'saved_execution_plan_semantics_unresolved', 'tag': tag})
    return {'variables': variables, 'errors': errors}


def _path(path, cwd=None, normalizations=None, source_cwd=None):
    def absolute(value, base):
        if not isinstance(value, str) or not value or '$' in value or value.startswith('~'):
            raise ValueError('native model path is not a resolved file reference')
        if not os.path.isabs(value):
            if not base or not os.path.isabs(base):
                raise ValueError('relative native model path has no explicit working-directory base: ' + value)
            value = os.path.join(base, value)
        return os.path.normpath(value)
    candidate = absolute(path, cwd)
    for proof in normalizations or []:
        if candidate == absolute(proof.get('resolved'), cwd):
            return absolute(proof.get('original'), source_cwd)
    return candidate


def _native_scalar_string_evidence(variable, tasks):
    """Use emitted native types, never a function-name or filename guess.

    A finite source literal/formula remains numeric even when a malformed
    capture emits quotes. Unknown expressions can be opaque strings only
    after every applicable native point actually emitted one quoted scalar.
    """
    expression = variable.get('value')
    if literal_domain(expression) is not None or not tasks:
        return False
    from .private_netlist import _expression_value
    for task in tasks:
        rows = task.get('native_parameters') or []
        actual = [row for row in rows if row.get('name') == variable.get('name')]
        if len(actual) != 1 or actual[0].get('value') is not None:
            return False
        raw = actual[0].get('expression')
        if not isinstance(raw, str) or not re.fullmatch(r'"(?:[^"\\]|\\.)*"', raw.strip(), re.DOTALL):
            return False
        numbers = {row['name']: row['value'] for row in rows if row.get('value') is not None}
        try:
            numeric = _expression_value(str(expression or ''), numbers)
        except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
            numeric = None
        if numeric is not None:
            return False
    return True


def _saved_model_path(reference, source_work_dir, native_rows):
    """Use the saved reference's own base and proven capture search context."""
    paths = []
    for task in native_rows or []:
        resolution = task.get('native_model_resolution')
        if resolution is None:
            continue
        row = next((item for item in resolution.get('source_references') or []
                    if item.get('reference') == reference), None)
        if row is None or row.get('error'):
            raise ValueError((row or {}).get('error') or 'Saved model resolution is missing: ' + reference)
        paths.append(row.get('physical_path') or os.path.realpath(row['resolved']))
    if paths:
        physical = set(paths)
        if len(physical) != 1:
            raise ValueError('Saved model resolves differently across captured points: ' + reference)
        return paths[0]
    return os.path.realpath(_path(reference, source_work_dir))


def corner_contract(setup, corner, test, snapshot=None, native_rows=None):
    """Expected Cartesian corner identities, including model-group choices."""
    dimensions, unresolved, opaque, empty = [], [], [], []
    for row in corner.get('variables') or []:
        if row.get('value') == '':
            empty.append(row['name'])
            continue
        domain = literal_domain(row.get('value'))
        expression = row.get('value')
        if _string_expression(expression) and (domain is None or len(domain) == 1):
            # Ordinary strings are native data, not Crescendo qualification gates.
            # Retain them verbatim; their meaning/files belong to the simulator.
            opaque.append({'name': row['name'], 'expression': expression})
        elif domain is None and _native_scalar_string_evidence(row, native_rows):
            opaque.append({'name': row['name'], 'expression': expression,
                           'evidence': 'native_emitted_scalar_string'})
        elif domain is None:
            # Qualify bounded constant arithmetic only; variable-dependent or
            # otherwise native-only expressions remain unresolved.
            try:
                from .private_netlist import _expression_value
                value = _expression_value(str(expression or ''), {})
            except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
                unresolved.append(row['name'])
            else:
                dimensions.append((row['name'], [value]))
        else:
            # A literal string sweep can help identify expanded native points;
            # it is never interpreted as a filename or checked on disk.
            dimensions.append((row['name'], domain))
    model_choices = []
    source_work_dir = ((snapshot or {}).get('model_path_context') or {}).get('source_work_dir')
    path_errors = []
    for name in corner.get('model_groups') or []:
        group = next((row for row in setup.get('model_groups') or [] if row['name'] == name), None)
        if group is None:
            unresolved.append('model_group:' + name)
            continue
        models = [row for row in group.get('models') or [] if row.get('enabled', True)
                  and row.get('test', 'All') in ('All', test)]
        if not models or any(not row.get('file') for row in models):
            unresolved.append('model_group:' + name)
            continue
        try:
            model_choices.append((name, tuple(sorted(((_saved_model_path(row['file'], source_work_dir, native_rows), row.get('section')) for row in models),
                                                    key=lambda item: (item[0], item[1] or '')))))
        except ValueError as error:
            unresolved.append('model_group:' + name)
            path_errors.append(str(error))
    if not model_choices:
        model_choices = [(None, ())]
    size = len(model_choices)
    for _name, values in dimensions:
        size *= len(values)
    if size > 100000:
        return {'error': 'corner_identity_product_exceeds_limit', 'expected': []}
    expected = []
    for values in itertools.product(*[values for _name, values in dimensions]):
        for model_group, model_set in model_choices:
            expected.append({'variables': dict(zip([name for name, _values in dimensions], values)),
                             'model_group': model_group, 'models': model_set})
    return {'dimensions': dimensions, 'expected': expected, 'unresolved': unresolved,
            'empty_variables': empty,
            'opaque_variables': opaque,
            'model_path_errors': path_errors,
            'variable_count': size // len(model_choices), 'model_count': len(model_choices),
            'error': ('saved_model_path_resolution_failed' if any(row.get('native_model_resolution') is not None
                        for row in native_rows or []) else 'relative_model_path_base_unavailable')
                     if path_errors else ('corner_identity_source_unresolved' if unresolved else None)}


def resolve_task_model_identity(task, config, capture_simulator, setup=None, snapshot=None, path_cache=None):
    """Resolve only emitted model includes using the existing solver search rule.

    The native netlist directory may contain symlinks to the original model.
    Keep both the search hit and its physical path; a basename is never proof.
    Variable strings are deliberately outside this operation.
    """
    from .native_includes import (NativeIncludeError, native_include_environment,
                                  freeze_include_argv, resolve_include_file, expand_native_path)
    cwd = task.get('native_cwd')
    result = {'schema_version': 'crescendo.native_model_identity.v1', 'native_cwd': cwd,
              'include_dirs': [], 'references': [], 'source_references': [], 'errors': [],
              'search_order': 'captured_cwd_then_captured_I_then_source_model_dir'}
    try:
        if not isinstance(cwd, str) or not os.path.isabs(cwd):
            raise NativeIncludeError('Native model identity needs its captured absolute cwd')
        environ = native_include_environment(config)
        model_dir = ((config or {}).get('cadence') or {}).get(capture_simulator + '_model_dir')
        _argv, directories = freeze_include_argv(task.get('argv'), cwd, environ, model_dir)
        result['include_dirs'] = directories
    except (ValueError, OSError) as error:
        result['errors'].append(str(error))
        return result
    cache = path_cache if path_cache is not None else {}
    def resolve(reference, base):
        # Cache is owned by one completed capture phase, never another Load.
        expanded = expand_native_path(reference, environ)
        key = (expanded, base, tuple(directories))
        if key not in cache:
            try:
                resolved = resolve_include_file(reference, base, directories, environ)
                cache[key] = {'resolved': resolved, 'physical_path': os.path.realpath(resolved)}
            except (ValueError, OSError) as error:
                cache[key] = {'error': str(error)}
        return cache[key]
    for model in task.get('native_models') or []:
        reference = model.get('file')
        try:
            identity = resolve(reference, cwd)
        except (ValueError, OSError) as error:
            identity = {'error': str(error)}
        result['references'].append(dict(identity, reference=reference, section=model.get('section')))
    source_cwd = ((snapshot or {}).get('model_path_context') or {}).get('source_work_dir')
    result['source_work_dir'] = source_cwd
    source_references = sorted(set(row['file'] for group in (setup or {}).get('model_groups') or []
        for row in group.get('models') or [] if row.get('file') and row.get('enabled', True)
        and row.get('test', 'All') in ('All', task.get('test'))))
    for reference in source_references:
        try:
            approved = next((proof for proof in (snapshot or {}).get('verified_model_path_normalizations') or []
                             if proof.get('original') == reference), None)
            if approved:
                resolved = _path(reference, source_cwd)
                result['source_references'].append({'reference': reference, 'resolved': resolved,
                    'physical_path': os.path.realpath(resolved), 'basis': 'verified_private_model_path_normalization'})
                continue
            if not os.path.isabs(reference) and (not source_cwd or not os.path.isabs(source_cwd)):
                raise NativeIncludeError('Relative saved model needs its captured source working directory: ' + reference)
            result['source_references'].append(dict(resolve(reference, source_cwd or cwd), reference=reference))
        except (ValueError, OSError) as error:
            result['source_references'].append({'reference': reference, 'error': str(error)})
    return result


def _corner_actual_identity(task, snapshot=None):
    actual = dict((row['name'], _native_literal(row)) for row in task.get('native_parameters') or [])
    temperature = task.get('temperature')
    if temperature:
        actual['temperature'] = temperature.get('value')
    resolution = task.get('native_model_resolution')
    references = (resolution or {}).get('references') or []
    models, errors = set(), list((resolution or {}).get('errors') or [])
    for row in task.get('native_models') or []:
        reference = next((value for value in references if value.get('reference') == row['file']
                          and value.get('section') == row.get('section')), None)
        if resolution is not None and (reference is None or reference.get('error')):
            errors.append((reference or {}).get('error') or 'Captured model resolution is missing: ' + row['file'])
            continue
        try:
            path = _path(reference['resolved'] if reference else row['file'], task.get('native_cwd'),
                         (snapshot or {}).get('verified_model_path_normalizations'),
                         ((snapshot or {}).get('model_path_context') or {}).get('source_work_dir'))
            physical = (reference.get('physical_path') if reference and path == reference['resolved'] else None)
            models.add((physical or os.path.realpath(path), row.get('section')))
        except ValueError as error:
            errors.append(str(error))
    return actual, models, errors


def corner_matches(task, contract, snapshot=None):
    actual, models, _errors = _corner_actual_identity(task, snapshot)
    matches = []
    for index, identity in enumerate(contract['expected']):
        expected_models = set(identity['models'])
        if all(actual.get(name) is not None and _same_literal(actual[name], value)
               for name, value in identity['variables'].items()) and expected_models.issubset(models):
            matches.append(index)
    # Prefer exact emitted numbers over tolerant near-equality; tolerance is
    # only a fallback when native formatting changes numeric representation.
    exact = []
    for index in matches:
        variables = contract['expected'][index]['variables']
        if all(actual.get(name) == value and
               isinstance(actual.get(name), str) == isinstance(value, str)
               for name, value in variables.items()):
            exact.append(index)
    return exact or matches


def _corner_match_failure(task, contract, snapshot=None):
    actual, models, errors = _corner_actual_identity(task, snapshot)
    nearest = []
    for index, identity in enumerate(contract['expected']):
        variables = [{'name': name, 'expected': value, 'actual': actual.get(name)}
                     for name, value in identity['variables'].items()
                     if not _same_literal(actual.get(name), value)]
        expected = set(identity['models'])
        missing = sorted(expected - models, key=lambda row: (row[0], row[1] or ''))
        nearest.append({'identity_index': index, 'model_group': identity.get('model_group'),
                        'variable_differences': variables, 'missing_models': missing,
                        'difference_count': len(variables) + len(missing)})
        nearest.sort(key=lambda row: (row['difference_count'], row['identity_index']))
        del nearest[3:]
    return {'point_id': task.get('point_id'), 'native_corner': task.get('corner'),
            'actual_models': sorted(models, key=lambda row: (row[0], row[1] or '')),
            'model_resolution': task.get('native_model_resolution'),
            'model_path_errors': sorted(set(errors)), 'closest_identities': nearest,
            'other_identities_omitted': max(0, len(contract['expected']) - 3)}


def qualify_expansion(setup, rows, active_groups, mapping, snapshot=None):
    """Map labels only when actual variables/models prove unique membership."""
    corners = dict((row['name'], row) for row in setup.get('corners') or [])
    contracts, proofs, failures = {}, [], []
    failures_omitted = 0
    for name in active_groups:
        corner = corners.get(name, {})
        if corner.get('variables') or corner.get('model_groups'):
            applicable = [row for row in rows if mapping.get(row['corner']) == name]
            if not applicable:
                applicable = [row for row in rows if row['corner'] not in mapping]
            contracts[name] = corner_contract(setup, corner, rows[0]['test'] if rows else '', snapshot, applicable)
    errors = []
    observed = {}
    for label in sorted(set(row['corner'] for row in rows)):
        if label in mapping and mapping[label] not in contracts:
            continue
        tasks = [row for row in rows if row['corner'] == label]
        candidates = []
        for name, contract in contracts.items():
            if contract['error']:
                continue
            try:
                matches = [corner_matches(row, contract, snapshot) for row in tasks]
                if not all(matches) and any(_corner_actual_identity(row, snapshot)[2] for row in tasks) and any(
                        identity['models'] for identity in contract['expected']):
                    errors.append('native_model_path_resolution_failed' if any(
                        row.get('native_model_resolution') is not None for row in tasks) else 'native_model_path_base_unavailable')
            except ValueError:
                errors.append('native_model_path_base_unavailable')
                continue
            if matches and all(value == matches[0] for value in matches) and matches[0]:
                # Repeated literal values produce distinct native labels with
                # indistinguishable identities. Allocate each saved occurrence
                # once, only when those identities are exactly identical.
                indices = matches[0]
                identities = [contract['expected'][index] for index in indices]
                unused = [index for index in indices if index not in observed.get(name, [])]
                if unused and all(identity == identities[0] for identity in identities):
                    candidates.append((name, unused[0]))
        if label in mapping:
            candidates = [value for value in candidates if value[0] == mapping[label]]
        elif len(candidates) > 1:
            # Strings may distinguish otherwise identical expanded groups.
            # They are positive mapping hints only: absent/different strings
            # never invalidate an independently established group identity.
            hinted = []
            for name, index in candidates:
                hints = []
                for opaque in contracts[name].get('opaque_variables') or []:
                    values = literal_domain(opaque['expression'])
                    if values is not None and len(values) == 1:
                        hints.append((opaque['name'], values[0]))
                if hints and all(all(_same_literal(next((_native_literal(value) for value in task.get('native_parameters') or []
                        if value['name'] == variable), None), expected) for variable, expected in hints) for task in tasks):
                    hinted.append((name, index))
            if len(hinted) == 1:
                candidates = hinted
        if len(candidates) == 1:
            name, index = candidates[0]
            mapping[label] = name
            observed.setdefault(name, []).append(index)
            proofs.append({'native_corner': label, 'source_corner': name,
                           'identity': contracts[name]['expected'][index], 'point_count': len(tasks)})
        elif label in mapping and mapping[label] in contracts:
            errors.append('native_corner_identity_mismatch')
        if len(candidates) != 1:
            names = [mapping[label]] if label in mapping and mapping[label] in contracts else sorted(contracts)
            for name in names:
                for task in tasks:
                    if len(failures) < 32:
                        failures.append(dict(_corner_match_failure(task, contracts[name], snapshot),
                                             source_corner=name, candidate_matches=candidates))
                    else:
                        failures_omitted += 1
    counts = {}
    for name, contract in contracts.items():
        counts[name] = len(contract['expected'])
        if contract['error']:
            errors.append(contract['error'])
        raw = active_groups[name]['count']
        if raw not in (contract.get('variable_count'), len(contract['expected'])):
            errors.append('native_corner_count_semantics_unqualified')
        values = observed.get(name, [])
        if sorted(values) != list(range(len(contract['expected']))):
            errors.append('native_corner_combinations_missing_or_duplicated')
    return {'counts': counts, 'proofs': proofs, 'contracts': contracts,
            'identity_failures': failures, 'identity_failures_omitted': failures_omitted,
            'errors': sorted(set(errors), key=lambda value: (value != 'native_corner_identity_mismatch', value))}


def _point_rdb_values(parsed, task):
    """Read a point's effective design variables for one exact native Test.

    ``parameterValue`` alone is the shared value, including when a Test has
    disabled the corresponding Global. ``hasLocal`` and an exact Test ID are
    required before using ``localParameterValue``. Never borrow another Test's
    row or fall back from an existing but unreadable Local value.
    """
    values, evidence, issues = {}, {}, []
    path = parsed.get('path')
    if not path:
        # In-memory legacy callers have no native schema/identity evidence.
        # Keep their numeric comparison without claiming Local readback.
        for row in parsed.get('parameters') or []:
            if row.get('pointID') == task['point_id']:
                name = row.get('name')
                number = None if isinstance(row.get('value'), str) and '"' in row['value'] else parse_spectre_number(row.get('value'))
                evidence[name] = {'scope': 'unproven_flat_parameters',
                                  'local_identity_proven': False}
                if number is not None:
                    values[name] = number
        return values, evidence, issues

    def issue(reason, name=None, **detail):
        row = dict(detail, reason=reason)
        if name is not None:
            row['variable'] = name
        issues.append(row)

    def require_columns(connection, table, required):
        columns = set(row[1] for row in connection.execute('PRAGMA table_info(%s)' % table))
        missing = sorted(set(required) - columns)
        if missing:
            issue('native_rdb_variable_schema_unavailable', table=table, missing_columns=missing)
        return not missing

    def bounded_rows(cursor):
        rows = cursor.fetchmany(10001)
        if len(rows) > 10000:
            raise ValueError('native single-point variable row limit exceeded')
        return rows

    connection = None
    try:
        if task.get('point_id') is None:
            issue('native_rdb_point_identity_unqualified')
            return values, evidence, issues
        path = os.path.abspath(path)
        connection = sqlite3.connect('file:%s?mode=ro' % quote(path, safe='/'), uri=True, timeout=1)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA query_only=ON')
        connection.execute('PRAGMA cache_size=-2048')
        operations = [0]
        def bounded_query():
            operations[0] += 1
            return int(operations[0] > 10000)
        connection.set_progress_handler(bounded_query, 1000)
        schemas = [require_columns(connection, table, columns) for table, columns in (
            ('test', ('testID', 'name')),
            ('parameter', ('parameterID', 'name', 'type', 'testID', 'hasLocal')),
            ('parameterValue', ('pointID', 'parameterID', 'value')))]
        if not all(schemas):
            return values, evidence, issues
        name = task.get('test')
        tests = connection.execute('SELECT testID FROM test WHERE name=? LIMIT 2', (name,)).fetchall() if name else []
        if len(tests) != 1 or tests[0]['testID'] is None:
            issue('native_rdb_test_identity_unqualified', test=name, matching_test_count=len(tests))
            return values, evidence, issues
        test_id = tests[0]['testID']
        metadata = bounded_rows(connection.execute('''
            SELECT parameterID, name, hasLocal FROM parameter
            WHERE type='desVar' AND (testID IS NULL OR testID='' OR testID=?)
            LIMIT 10001''', (test_id,)))
        by_name = {}
        for row in metadata:
            by_name.setdefault(row['name'], []).append(row)
        global_rows = {}
        for row in bounded_rows(connection.execute('''
                SELECT parameterID, value FROM parameterValue WHERE pointID=? LIMIT 10001''', (task['point_id'],))):
            global_rows.setdefault(row['parameterID'], []).append(row['value'])
        needs_local = any(str(row['hasLocal']).lower() in ('true', '1') for row in metadata)
        local_rows = {}
        local_schema = True
        if needs_local:
            local_schema = require_columns(connection, 'localParameterValue', ('pointID', 'parameterID', 'testID', 'value'))
            if local_schema:
                for row in bounded_rows(connection.execute('''
                        SELECT parameterID, value FROM localParameterValue
                        WHERE pointID=? AND testID=? LIMIT 10001''', (task['point_id'], test_id))):
                    local_rows.setdefault(row['parameterID'], []).append(row['value'])
        for name, rows in sorted(by_name.items(), key=lambda item: str(item[0])):
            if len(rows) != 1 or not name:
                issue('native_rdb_parameter_identity_ambiguous', name, matching_parameter_count=len(rows))
                continue
            row = rows[0]
            flag = str(row['hasLocal']).lower()
            proof = {'parameter_id': row['parameterID'], 'test_id': test_id,
                     'point_id': task['point_id'], 'test': task['test'],
                     'has_local': None, 'local_identity_proven': False}
            evidence[name] = proof
            if flag not in ('true', 'false', '1', '0'):
                issue('native_rdb_local_flag_unknown', name, raw_has_local=row['hasLocal'])
                continue
            has_local = flag in ('true', '1')
            proof['has_local'] = has_local
            if has_local and not local_schema:
                continue
            native_local = local_rows.get(row['parameterID'], []) if has_local else []
            candidates = native_local or global_rows.get(row['parameterID'], [])
            proof['scope'] = ('test_local' if native_local else
                              'point_global_no_test_local' if has_local else 'point_global')
            proof['local_identity_proven'] = True
            if len(candidates) != 1:
                issue('native_rdb_variable_value_ambiguous' if candidates else 'native_rdb_variable_value_missing',
                      name, scope=proof['scope'], row_count=len(candidates))
                continue
            raw = candidates[0]
            proof['raw_value'] = raw if isinstance(raw, (str, int, float)) or raw is None else repr(raw)
            proof['raw_type'] = type(raw).__name__
            number = None if isinstance(raw, str) and '"' in raw else parse_spectre_number(raw)
            proof['numeric_value'] = number
            if number is not None:
                values[name] = number
            elif raw is None or not isinstance(raw, str):
                issue('native_rdb_variable_value_unavailable', name, scope=proof['scope'], raw_type=proof['raw_type'])
            else:
                # Native strings/expressions remain strings. The caller checks
                # exact saved/native expression agreement; no numeric fallback.
                proof['numeric_state'] = 'unavailable_non_numeric_native_value'
                text = raw.strip()
                if text.startswith(('[', '(')) or len(numeric_domain(text) or []) > 1:
                    issue('native_rdb_variable_value_non_scalar', name, scope=proof['scope'])
    except (OSError, sqlite3.Error, TypeError, ValueError) as error:
        values.clear()
        issue('native_rdb_variable_read_failed', error='%s: %s' % (error.__class__.__name__, error))
    finally:
        if connection is not None:
            connection.close()
    return values, evidence, issues


def point_variable_checks(setup, test, corner, task, parsed, snapshot=None, effective_scope=None):
    expected = effective_variables(setup, test, corner) if effective_scope is None else effective_scope
    actual = dict((row['name'], row) for row in task.get('native_parameters') or [])
    if task.get('temperature'):
        actual['temperature'] = dict(task['temperature'], name='temperature')
    differences, checks = [], []
    if not expected:
        return differences, checks
    rdb_values, rdb_evidence, rdb_issues = _point_rdb_values(parsed, task)
    variable_issues = {}
    for issue in rdb_issues:
        if issue.get('variable') is not None:
            variable_issues.setdefault(issue['variable'], []).append(issue)
            continue
        check = dict(issue, point_id=task['point_id'], corner=task['corner'],
                     ok=False, evaluation='native_rdb_variable_identity_unqualified')
        differences.append(dict(check))
        checks.append(check)
    for name, row in sorted(expected.items()):
        if row.get('source') == 'corner' and row.get('value') == '':
            check = {'point_id': task['point_id'], 'corner': task['corner'], 'variable': name,
                     'source': 'corner', 'expected_expression': '', 'actual': actual.get(name),
                     'evaluation': 'empty_corner_native_value_observed', 'ok': None}
            checks.append(check)
            continue
        typed_domain = literal_domain(row.get('value'))
        if (_string_expression(row.get('value')) or
                (typed_domain is None and rdb_values.get(name) is None and
                 _native_scalar_string_evidence(row, [task]))):
            # Report what native generation emitted without imposing string
            # syntax, meaning, existence or equality as a simulation preflight.
            value = _native_literal(actual.get(name, {}))
            proof = rdb_evidence.get(name) or {}
            raw = proof.get('raw_value')
            rdb_domain = literal_domain(raw) if raw is not None else None
            rdb_literal = rdb_domain[0] if rdb_domain is not None and len(rdb_domain) == 1 else None
            same = typed_domain is not None and any(_same_literal(value, item) for item in typed_domain)
            check = {'point_id': task['point_id'], 'corner': task['corner'], 'variable': name,
                     'source': row.get('source'), 'expected_expression': row.get('value'),
                     'expected_domain': typed_domain, 'actual': actual.get(name),
                     'literal_value': value, 'rdb_literal': rdb_literal, 'rdb_value': None,
                     'evaluation': 'string_value_deferred_to_simulator',
                     'ok': True if same and (raw is None or _same_literal(value, rdb_literal)) else None}
            if proof:
                check['rdb_identity'] = proof
            if variable_issues.get(name):
                check['rdb_issues'] = variable_issues[name]
            if check['ok'] is True and raw is not None:
                check['evaluation'] = 'saved_rdb_netlist_expression_preserved_numeric_unavailable'
            elif typed_domain is None and raw is not None and str(actual.get(name, {}).get('expression') or '').strip() == str(raw).strip():
                check['evaluation'] = 'native_rdb_netlist_expression_matches_saved_evaluation_unavailable'
            checks.append(check)
            continue
        domain = numeric_domain(row.get('value'))
        value = actual.get(name, {}).get('value')
        rdb_value = rdb_values.get(name)
        check = {'point_id': task['point_id'], 'corner': task['corner'], 'variable': name,
                 'source': row.get('source'), 'expected_expression': row.get('value'),
                 'expected_domain': domain, 'rdb_value': rdb_value, 'actual': actual.get(name)}
        if name in rdb_evidence:
            check['rdb_identity'] = rdb_evidence[name]
        if task.get('param_source'):
            check['native_value_source'] = task['param_source']
        is_sweep = (domain is not None and len(domain) > 1) or str(row.get('value') or '').startswith('{From/To}')
        inactive_sweep = row.get('sweep_enabled') is False and is_sweep
        if inactive_sweep and rdb_value is not None:
            check['ok'] = value is not None and math.isclose(float(value), rdb_value, rel_tol=1e-10, abs_tol=1e-300)
            check['evaluation'] = 'native_inactive_sweep_readback'
        elif domain is not None:
            check['ok'] = value is not None and any(math.isclose(float(value), number, rel_tol=1e-10, abs_tol=1e-300) for number in domain)
        elif rdb_value is not None:
            check['ok'] = value is not None and math.isclose(float(value), rdb_value, rel_tol=1e-10, abs_tol=1e-300)
            check['evaluation'] = 'native_rdb_after_saved_scope_identity'
        else:
            from .private_netlist import _expression_value, _space
            native_numbers = dict((key, item['value']) for key, item in actual.items() if item.get('value') is not None)
            try:
                expression_value = _expression_value(str(row.get('value') or ''), native_numbers)
            except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
                expression_value = None
            if expression_value is not None:
                check['ok'] = value is not None and math.isclose(float(value), expression_value, rel_tol=1e-10, abs_tol=1e-300)
                check['evaluation'] = 'bounded_saved_expression_using_qualified_dependencies'
                check['expected_value'] = expression_value
            elif name in actual and _space(str(actual[name].get('expression') or '')) == _space(str(row.get('value') or '')):
                check['ok'] = True
                check['evaluation'] = 'saved_native_expression_preserved'
            else:
                # This is visible incomplete numeric verification, never a
                # fabricated evaluated value for an opaque native expression.
                check['ok'] = None
                check['evaluation'] = 'saved_expression_preserved_native_evaluation_unavailable'
        if rdb_value is not None and value is not None and not math.isclose(float(value), rdb_value, rel_tol=1e-10, abs_tol=1e-300):
            check['ok'] = False
            check['reason'] = 'native_rdb_netlist_value_mismatch'
        native_identity = rdb_evidence.get(name) or {}
        if native_identity.get('numeric_state'):
            raw = native_identity['raw_value']
            if domain is not None:
                check.update(ok=False, reason='native_rdb_numeric_value_unavailable')
            elif name in actual and str(actual[name].get('expression') or '').strip() == raw.strip():
                if str(row.get('value') or '').strip() == raw.strip():
                    check.update(ok=True, evaluation='saved_rdb_netlist_expression_preserved_numeric_unavailable')
                else:
                    check.update(ok=None, evaluation='native_rdb_netlist_expression_matches_saved_evaluation_unavailable')
            else:
                check.update(ok=False, reason='native_rdb_netlist_expression_mismatch')
        if variable_issues.get(name):
            check.update(ok=False, reason=variable_issues[name][0]['reason'], rdb_issues=variable_issues[name])
        if check['ok'] is False:
            mismatch = dict(check)
            if domain is not None and len(domain) == 1:
                mismatch['expected_value'] = domain[0]
            differences.append(mismatch)
        checks.append(check)
    return differences, checks


def sweep_coverage(setup, test, corners, rows, mapping, used_names=None, strict_multiplicity=False):
    """Every literal source sweep value must occur at every captured corner."""
    errors = []
    for label in sorted(set(row['corner'] for row in rows)):
        source = corners.get(mapping.get(label), {})
        expected = effective_variables(setup, test, source)
        tasks = [row for row in rows if row['corner'] == label]
        native_values = []
        for task in tasks:
            values = dict((item['name'], _native_literal(item)) for item in task.get('native_parameters') or [])
            if task.get('temperature'):
                values['temperature'] = task['temperature'].get('value')
            native_values.append(values)
        dimensions = []
        for name, variable in expected.items():
            if used_names is not None and name not in used_names:
                continue
            domain = literal_domain(variable.get('value'))
            if domain is None or len(domain) < 2 or variable.get('source') == 'corner' or variable.get('sweep_enabled') is False:
                continue
            dimensions.append((name, domain))
            values = [values.get(name) for values in native_values]
            missing = [number for number in domain if not any(_same_literal(value, number) for value in values)]
            if missing:
                errors.append({'corner': label, 'variable': name, 'source': variable.get('source'), 'missing_values': missing})
        size = 1
        for _name, values in dimensions:
            size *= len(values)
        if strict_multiplicity:
            from collections import Counter
            domain_counts = [Counter(values) for _name, values in dimensions]
            distinct_size = 1
            for counts in domain_counts:
                distinct_size *= len(counts)
            observed, invalid = Counter(), []
            for task, actual in zip(tasks, native_values):
                combination = []
                for (name, _domain), counts in zip(dimensions, domain_counts):
                    value = actual.get(name)
                    matches = [number for number in counts if _same_literal(value, number)]
                    exact = [number for number in matches if value == number and
                             isinstance(value, str) == isinstance(number, str)]
                    if exact:
                        matches = exact
                    if len(matches) != 1:
                        invalid.append({'point_id': task['point_id'], 'variable': name,
                                        'reason': 'native_sweep_value_membership_unqualified'})
                        break
                    combination.append(matches[0])
                else:
                    observed[tuple(combination)] += 1
            absent = distinct_size - len(observed)
            if absent > 0:
                errors.append({'corner': label, 'variables': [name for name, _domain in dimensions],
                               'reason': 'design_sweep_combinations_missing', 'missing_combination_count': absent,
                               'expected_point_count': size, 'observed_point_count': len(tasks)})
            repeats = []
            for combination, count in observed.items():
                expected_count = 1
                for number, counts in zip(combination, domain_counts):
                    expected_count *= counts[number]
                if count != expected_count:
                    repeats.append({'values': dict((name, number) for (name, _domain), number in zip(dimensions, combination)),
                                    'expected_count': expected_count, 'observed_count': count})
            if invalid:
                errors.append({'corner': label, 'reason': 'native_sweep_value_membership_unqualified',
                               'points': invalid[:8]})
            if repeats:
                errors.append({'corner': label,
                               'reason': 'design_sweep_point_multiplicity_mismatch' if dimensions else 'native_point_repeat_dimension_unqualified',
                               'expected_point_count': size, 'observed_point_count': len(tasks),
                               'combinations': repeats[:8]})
            continue
        if len(dimensions) > 1 and size <= 100000:
            absent = 0
            for combination in itertools.product(*[domain for _name, domain in dimensions]):
                if not any(all(_same_literal(values.get(name), number)
                               for (name, _domain), number in zip(dimensions, combination)) for values in native_values):
                    absent += 1
            if absent:
                errors.append({'corner': label, 'variables': [name for name, _domain in dimensions],
                               'reason': 'design_sweep_combinations_missing', 'missing_combination_count': absent})
    return errors
