"""Owned Test/Analysis choices; the inspected source remains immutable."""
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

import copy
import json

SCHEMA = _contract_text('crescendo.execution_selection.v1')


def catalogue(setup):
    rows = (setup or {}).get('tests') or []
    names = [row.get('name') for row in rows]
    if any(not isinstance(name, str) or not name.strip() for name in names) or len(names) != len(set(names)):
        raise ValueError('Test catalogue has missing or duplicate names')
    for row in rows:
        analyses = [item.get('name') for item in row.get('analyses') or []]
        if any(not isinstance(name, str) or not name.strip() for name in analyses) or len(analyses) != len(set(analyses)):
            raise ValueError('Analysis catalogue has missing or duplicate names for Test ' + row['name'])
    return rows


def _names(value, available, label):
    if not isinstance(value, list) or any(not isinstance(name, str) for name in value):
        raise ValueError(label + ' must be a list of names')
    if not value or len(value) != len(set(value)):
        raise ValueError(label + ' must contain at least one unique selection')
    unknown = set(value) - set(available)
    if unknown:
        raise ValueError('Unknown ' + label + ': ' + ', '.join(sorted(unknown)))
    return [name for name in available if name in value]


def selection_plan(setup, spec):
    raw = (spec or {}).get('execution_selection')
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) - {'tests', 'analyses'}:
        raise ValueError('Execution selection must contain only tests and analyses')
    rows = catalogue(setup)
    available = [row['name'] for row in rows]
    defaults = [row['name'] for row in rows if row.get('enabled', True)]
    selected = _names(raw.get('tests', defaults), available, 'Tests')
    overrides = raw.get('analyses', {})
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict) or set(overrides) - set(available):
        raise ValueError('Analysis selections refer to an unknown Test')
    analyses, source_analyses = {}, {}
    for row in rows:
        name = row['name']
        choices = [item['name'] for item in row.get('analyses') or []]
        source = [item['name'] for item in row.get('analyses') or [] if item.get('enabled', True)]
        source_analyses[name] = source
        if name in overrides:
            analyses[name] = _names(overrides[name], choices, 'analyses for ' + name)
        else:
            analyses[name] = list(source)
        if name in selected and choices and not analyses[name]:
            raise ValueError('No analysis selected for Test ' + name)
    # A requested metric cannot silently target a disabled test.
    for metric in list((spec or {}).get('objectives') or []) + list((spec or {}).get('constraints') or []):
        key = metric.get('output_key') or ''
        test = key.rsplit('::', 1)[0] if '::' in key else None
        if test in available and test not in selected:
            raise ValueError('Metric %s belongs to unselected Test %s' % (key, test))
    return {'schema_version': SCHEMA, 'available': available, 'source_enabled': defaults,
            'selected': selected, 'analyses': analyses, 'source_analyses': source_analyses,
            'available_analyses': {row['name']: [item['name'] for item in row.get('analyses') or []] for row in rows}}


def capture_selection_plan(setup, requested, previous):
    if requested is not None or not previous:
        return requested
    rows = catalogue(setup)
    return selection_plan(setup, {'execution_selection': {
        'tests': [row['name'] for row in rows if row.get('enabled', True)]}})


def effective_setup(setup, plan):
    result = copy.deepcopy(setup)
    if plan is None:
        return result
    rows = catalogue(setup)
    if plan.get('schema_version') != SCHEMA or plan.get('available') != [row['name'] for row in rows]:
        raise ValueError('Execution selection belongs to a different Test catalogue')
    _names(plan.get('selected'), plan['available'], 'Tests')
    for row in result['tests']:
        row['enabled'] = row['name'] in plan['selected']
        available = [item['name'] for item in row.get('analyses') or []]
        if available != plan.get('available_analyses', {}).get(row['name']):
            raise ValueError('Execution selection belongs to a different analysis catalogue')
        chosen = plan['analyses'].get(row['name'], [])
        if set(chosen) - set(available):
            raise ValueError('Execution selection contains an unknown analysis')
        for analysis in row.get('analyses') or []:
            analysis['enabled'] = analysis['name'] in chosen
    return result


def selection_skill_lines(plan):
    if plan is None:
        return []
    quote = lambda value: json.dumps(value, ensure_ascii=True)
    lines = []
    for name in plan['available']:
        enabled = name in plan['selected']
        lines.append(_contract_text('unless(member(%s maeGetSetup(?typeName "tests" ?session session)) error("Crescendo selected Test unavailable\\n"))') % quote(name))
        for analysis in plan['available_analyses'][name]:
            wanted = analysis in plan['analyses'][name]
            lines.extend([
                _contract_text('crescAnalysisBefore=maeGetAnalysis(%s %s ?includeEmpty t ?session session)') % (quote(name), quote(analysis)),
                _contract_text('unless(maeSetAnalysis(%s %s ?enable %s ?session session) error("Crescendo analysis selection setter failed\\n"))') % (quote(name), quote(analysis), 't' if wanted else 'nil'),
                _contract_text('crescAnalysisAfter=maeGetAnalysis(%s %s ?includeEmpty t ?session session)') % (quote(name), quote(analysis)),
                _contract_text('unless(equal(setof(item crescAnalysisBefore car(item)!="enable") setof(item crescAnalysisAfter car(item)!="enable")) error("Crescendo analysis options changed while selecting analysis\\n"))'),
            ])
        expected = 'list(%s)' % ' '.join(quote(value) for value in plan['analyses'][name])
        lines.append(_contract_text('unless(equal(sort(copy(maeGetEnabledAnalysis(%s ?session session)) \'alphalessp) sort(%s \'alphalessp)) error("Crescendo enabled analysis readback differs\\n"))') % (quote(name), expected))
    # Qualify the final active Test set after all configuration changes.
    # IC618 stores active/disabledtests separately from test enabled attributes.
    for name in plan['available']:
        enabled = name in plan['selected']
        lines.extend([
            _contract_text('unless(maeSetSetup(?tests list(%s) ?enabled %s ?session session) error("Crescendo Test selection setter failed\\n"))') % (quote(name), 't' if enabled else 'nil'),
        ])
    expected_tests = 'list(%s)' % ' '.join(quote(value) for value in plan['selected'])
    lines.append(_contract_text('unless(equal(sort(copy(maeGetSetup(?typeName "tests" ?enabled t ?session session)) \'alphalessp) sort(%s \'alphalessp)) error("Crescendo final enabled Test set differs\\n"))') % expected_tests)
    return lines


def normalize_previous_selection(original, private, applied):
    """Permit only previously proved enabled bits; retain all other differences."""
    if not applied or applied.get('schema_version') != SCHEMA:
        return private, None
    source = {row['name']: row for row in original.get('tests') or []}
    actual = {row['name']: row for row in private.get('tests') or []}
    if set(source) != set(actual) or set(source) != set(applied.get('available') or []):
        return private, None
    def flags(row):
        return bool(row.get('enabled', True)), {item['name']: bool(item.get('enabled', True)) for item in row.get('analyses') or []}
    source_flags = {name: flags(row) for name, row in source.items()}
    actual_flags = {name: flags(row) for name, row in actual.items()}
    if source_flags == actual_flags:
        return private, None
    expected_flags = {name: (name in applied['selected'],
        {analysis: analysis in applied['analyses'][name] for analysis in applied['available_analyses'][name]})
        for name in source}
    if actual_flags != expected_flags:
        return private, None
    result = copy.deepcopy(private)
    for row in result['tests']:
        before = source[row['name']]
        row['enabled'] = before.get('enabled', True)
        originals = {item['name']: item for item in before.get('analyses') or []}
        for analysis in row.get('analyses') or []:
            item = originals.get(analysis['name'])
            if item is None:
                return private, None
            analysis['enabled'] = item.get('enabled', True)
            if 'fields' in analysis:
                # Native save rewrites only enable after maeSetAnalysis.
                if 'enable' in item.get('fields', {}):
                    analysis['fields']['enable'] = item['fields']['enable']
                else:
                    analysis['fields'].pop('enable', None)
    return result, {'schema_version': SCHEMA, 'scope': 'only previously qualified owned Test and Analysis enabled bits'}
