"""Per-run corner choices; source Maestro definitions remain immutable."""
import copy
import json

SCHEMA = 'crescendo.run_corner_selection.v1'
DEFAULT = '_default'


def catalogue(setup):
    rows = (setup or {}).get('corners') or []
    if not rows:
        return [{'name': DEFAULT, 'enabled': True, 'variables': [], 'implicit': True}]
    names = [row.get('name') for row in rows]
    if any(not isinstance(name, str) or not name.strip() for name in names) or len(names) != len(set(names)):
        raise ValueError('Corner catalogue has missing or duplicate names')
    return rows


def eligible_corner_names(setup, test_name):
    """Corners proved enabled globally and for the selected Test."""
    mask = ((setup or {}).get('test_corner_masks') or {}).get(test_name) or {}
    disabled = mask.get('disabled_corners') or []
    result = []
    for row in catalogue(setup):
        name = row['name']
        if not row.get('enabled', True):
            continue
        if name == DEFAULT:
            if mask.get('nominal_enabled') is not True:
                continue
        elif not mask.get('known') or name in disabled:
            continue
        result.append(name)
    return result


def selection_plan(setup, spec):
    definitions = list((spec or {}).get('objectives') or []) + list((spec or {}).get('constraints') or [])
    if not definitions:
        return None  # Legacy generation/range callers inherit the source.
    rows = catalogue(setup)
    available = [row['name'] for row in rows]
    selected_test = (spec or {}).get('selected_test') or (setup or {}).get('selected_test')
    defaults = (eligible_corner_names(setup, selected_test) if selected_test else
                [row['name'] for row in rows if row.get('enabled', True)])
    mask = (((setup or {}).get('test_corner_masks') or {}).get(selected_test) or {}) if selected_test else {}
    unknown_mask = {row['name'] for row in rows if row.get('enabled', True) and
                    ((row['name'] == DEFAULT and mask.get('nominal_enabled') is None) or
                     (row['name'] != DEFAULT and mask.get('known') is not True))} if selected_test else set()
    selected, requirements = set(), []
    tests = {row.get('name') for row in (setup or {}).get('tests') or []}
    outputs = (setup or {}).get('outputs') or []
    for definition in definitions:
        choices = definition.get('corners')
        if choices is None and not defaults and unknown_mask:
            raise ValueError('Test corner mask unavailable or incomplete; Load and Inspect again: ' + ', '.join(sorted(unknown_mask)))
        # Omitted or null means source defaults; an explicit empty list is an
        # unresolved measurement scope and must never be silently populated.
        choices = defaults if choices is None else choices
        if not isinstance(choices, (list, tuple)) or any(not isinstance(name, str) for name in choices):
            raise ValueError('Metric corners must be a list of corner names')
        unknown = set(choices) - set(available)
        if unknown:
            raise ValueError('Unknown selected corners: ' + ', '.join(sorted(unknown)))
        if not choices:
            raise ValueError('No enabled corner selected for metric: ' + str(definition.get('output_key')))
        disallowed = set(choices) - set(defaults) if selected_test else set()
        if disallowed & unknown_mask:
            raise ValueError('Test corner mask unavailable or incomplete; Load and Inspect again: ' + ', '.join(sorted(disallowed & unknown_mask)))
        if disallowed:
            raise ValueError('Corners unavailable for selected Test: ' + ', '.join(sorted(disallowed)))
        selected.update(choices)
        key = definition.get('output_key')
        matches = {row.get('test') for row in outputs if row.get('key') == key and row.get('test')}
        test = next(iter(matches)) if len(matches) == 1 else None
        if test is None and isinstance(key, str) and '::' in key:
            prefix = key.rsplit('::', 1)[0]
            if prefix in tests:
                test = prefix
        if selected_test and test is not None and test != selected_test:
            raise ValueError('Metric %s belongs to another Test %s' % (key, test))
        requirements.append({'output_key': key, 'test': test or selected_test, 'corners': list(dict.fromkeys(choices))})
    return {'schema_version': SCHEMA, 'available': available, 'source_enabled': defaults,
            'selected': [name for name in available if name in selected], 'metric_requirements': requirements}


def capture_selection_plan(setup, requested, previous):
    if requested is not None or not previous:
        return requested
    # A reused headless workspace may still hold the preceding explicit run.
    # An inheritance request restores source flags, just like session reset.
    rows = catalogue(setup)
    selected_test = (setup or {}).get('selected_test')
    defaults = (eligible_corner_names(setup, selected_test) if selected_test else
                [row['name'] for row in rows if row.get('enabled', True)])
    return {'schema_version': SCHEMA, 'available': [row['name'] for row in rows],
            'source_enabled': defaults, 'selected': defaults, 'metric_requirements': []}


def effective_setup(setup, plan):
    result = copy.deepcopy(setup)
    if plan is None:
        return result
    rows = catalogue(setup)
    if plan.get('schema_version') != SCHEMA or plan.get('available') != [row['name'] for row in rows]:
        raise ValueError('Run corner selection belongs to a different source catalogue')
    selected = plan.get('selected')
    if not isinstance(selected, list) or not selected or len(selected) != len(set(selected)) or set(selected) - set(plan['available']):
        raise ValueError('Run corner selection is empty or invalid')
    result['corners'] = copy.deepcopy(rows)
    for row in result['corners']:
        row['enabled'] = row['name'] in selected
    return result


def selection_skill_lines(plan):
    if plan is None:
        return []
    quote = lambda value: json.dumps(value, ensure_ascii=True)
    lines = []
    for name in plan['available']:
        if name == DEFAULT:
            continue
        enabled = name in plan['selected']
        lines += [
            'unless(member(%s cadr(axlGetCorners(mainSDB))) error("Crescendo selected corner is unavailable\\n"))' % quote(name),
            'unless(maeSetSetup(?corners list(%s) ?enabled %s ?session session) error("Crescendo corner selection setter failed\\n"))' % (quote(name), 't' if enabled else 'nil'),
            '%s(axlGetEnabled(axlGetCorner(mainSDB %s)) error("Crescendo corner selection readback differs\\n"))' % ('unless' if enabled else 'when', quote(name)),
        ]
    nominal = DEFAULT in plan['selected']
    lines += [
        'unless(axlSetNominalCornerEnabled(mainSDB %d) error("Crescendo nominal corner selection failed\\n"))' % int(nominal),
        '%s(axlGetNominalCornerEnabled(mainSDB) error("Crescendo nominal corner selection readback differs\\n"))' % ('unless' if nominal else 'when'),
    ]
    return lines


def normalize_previous_selection(original, private, applied):
    """Normalize only the exact previously proved private enabled-bit changes."""
    if not applied:
        return private, None
    source_rows, actual_rows = original.get('corners') or [], private.get('corners') or []
    source = {row['name']: row for row in source_rows}
    actual = {row['name']: row for row in actual_rows}
    if set(source) != set(actual):
        return private, None
    source_flags = {name: bool(row.get('enabled', True)) for name, row in source.items()}
    actual_flags = {name: bool(row.get('enabled', True)) for name, row in actual.items()}
    if actual_flags == source_flags:
        return private, None
    selected = applied.get('selected') or []
    expected = {name: name in selected for name in source}
    if applied.get('schema_version') != SCHEMA or actual_flags != expected or set(applied.get('available') or []) != set(source):
        return private, None
    result = copy.deepcopy(private)
    for row in result['corners']:
        before = source[row['name']]
        row['enabled'] = before.get('enabled', True)
        original_attrs = (before.get('native_content') or {}).get('attributes') or {}
        attrs = (row.get('native_content') or {}).get('attributes')
        if attrs is not None:
            for key in ('enabled', 'Enabled'):
                if key in original_attrs:
                    attrs[key] = original_attrs[key]
                else:
                    attrs.pop(key, None)
    return result, {'schema_version': SCHEMA, 'source_flags': source_flags, 'private_flags': actual_flags,
                    'scope': 'only previous qualified owned corner enabled bits; other fields unchanged'}


def missing_metric_corners(tasks, plan):
    """Respect Maestro test masks, but never silently reduce a metric's scope."""
    missing = []
    for row in (plan or {}).get('metric_requirements') or []:
        if row.get('test') is None:
            continue  # Output binding qualification owns unresolved output keys.
        covered = {task.get('source_corner') or task.get('corner') for task in tasks
                   if task.get('test') == row['test']}
        absent = sorted(set(row['corners']) - covered)
        if absent:
            missing.append({'output_key': row['output_key'], 'test': row['test'], 'corners': absent})
    return missing
