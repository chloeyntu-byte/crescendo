"""Bounded display-only gate progress, driven by completed operations."""


def gate_progress(value):
    """Accept producer counts, never infer completion from time or stage names."""
    if not isinstance(value, dict):
        return None
    passed, total = value.get('passed'), value.get('total')
    scope = value.get('scope')
    if (type(passed) is not int or type(total) is not int or total <= 0 or
            passed < 0 or passed > total or not isinstance(scope, str) or not scope):
        return None
    return {'scope': scope[:160], 'passed': passed, 'total': total,
            'current': str(value.get('current') or '')[:160]}


def merge_gate_progress(previous, incoming):
    current = gate_progress(incoming)
    prior = gate_progress(previous)
    if current is None:
        return prior
    if prior and prior['scope'] == current['scope']:
        # Repeated/late observations do not move a gate or change its scale.
        if current['total'] != prior['total'] or current['passed'] < prior['passed']:
            return prior
    return current


# Each later entry is emitted only after the previous blocking operation returns.
# Errors/warnings are deliberately absent. The producer emits the final entry
# only after the private copy or parameter application has been validated.
TEST_CATALOGUE_GATES = ('saved_setup', 'inspection_complete')
INSPECT_GATES = ('saved_setup', 'job_policy', 'hierarchy', 'private_copy', 'inspection_complete')
APPLY_GATES = ('parameter_scan', 'private_apply', 'parameters_ready')
RESTORE_GATES = ('source_selection', 'saved_setup', 'job_policy', 'hierarchy',
                 'private_copy', 'inspection_complete', 'configuration_matching',
                 'parameter_scan', 'private_apply', 'parameters_ready', 'configuration_complete')


SUITE_LOAD_GATES = ('suite_open', 'suite_compare', 'suite_publish', 'suite_ready')
SUITE_UPDATE_GATES = ('suite_merge', 'suite_native_check', 'suite_publish', 'suite_ready')
SUITE_UNCHANGED_GATES = ('suite_merge', 'suite_unchanged', 'suite_publish', 'suite_ready', 'suite_reuse_ready')


def suite_gate_progress(stage, message, previous):
    prior = gate_progress(previous)
    if stage == 'suite_open':
        scope, plan, index = 'suite_load', SUITE_LOAD_GATES, 0
    elif stage == 'suite_merge':
        scope, plan, index = 'suite_update', SUITE_UPDATE_GATES, 0
    elif stage == 'suite_unchanged':
        if not prior or prior['scope'] != 'suite_update' or prior['passed'] != 0:
            return prior
        scope, plan, index = 'suite_unchanged', SUITE_UNCHANGED_GATES, 1
    else:
        plans = {'suite_load': SUITE_LOAD_GATES, 'suite_update': SUITE_UPDATE_GATES,
                 'suite_unchanged': SUITE_UNCHANGED_GATES}
        if not prior or prior['scope'] not in plans:
            return None
        scope, plan = prior['scope'], plans[prior['scope']]
        if stage not in plan:
            return prior
        index = plan.index(stage)
        if index > prior['passed'] + 1:
            return prior
    return {'scope': scope, 'passed': index, 'total': len(plan) - 1,
            'current': str(message or stage)[:160]}


def setup_gate_progress(action, stage, message='', previous=None):
    if isinstance(stage, str) and stage.startswith('suite_'):
        return suite_gate_progress(stage, message, previous)
    if action in ('load_configuration', '_load_configuration_document', 'load_settings'):
        plan = RESTORE_GATES
    elif action == 'apply_selected_cells':
        plan = APPLY_GATES
    elif action == 'load_setup':
        plan = TEST_CATALOGUE_GATES
    elif action == '_test_selected':
        plan = INSPECT_GATES
    else:
        return None
    if stage not in plan:
        return None
    if (plan is RESTORE_GATES and stage == 'configuration_complete' and
            (not previous or previous.get('passed', 0) < len(plan) - 2)):
        # A draft with no matching cells legitimately returns without Apply.
        # It must not claim the skipped preparation gates were passed.
        return previous
    return {'scope': action, 'passed': plan.index(stage), 'total': len(plan) - 1,
            'current': str(message or stage)[:160]}


DIAGNOSTIC_GATES = (('request', 'launch') + tuple('native.%d' % index for index in range(1, 6)) +
                    ('execution.pipeline', 'execution', 'statistics_and_dataset',
                     'termination_and_cleanup', 'test.summary'))


def diagnostic_native_progress(progress, incoming, scope):
    """One diagnostic owns one candidate; retain its five native producer gates."""
    value = gate_progress(incoming)
    if value is None or value['total'] != 5:
        return progress.get('gate_progress')
    native_scope = progress.get('native_gate_scope')
    if native_scope and native_scope != value['scope']:
        return progress.get('gate_progress')
    progress['native_gate_scope'] = value['scope']
    completed = progress.setdefault('passed_gates', [])
    for index in range(1, value['passed'] + 1):
        name = 'native.%d' % index
        if name not in completed:
            completed.append(name)
    return diagnostic_gate_progress(progress, '', '', scope, value['current'])


def diagnostic_gate_progress(progress, identifier, status, scope, message=''):
    """Only PASS records for the selected execution path close these gates."""
    completed = progress.setdefault('passed_gates', [])
    if status == 'PASS' and identifier in DIAGNOSTIC_GATES and identifier not in completed:
        completed.append(identifier)
    count = 0
    for name in DIAGNOSTIC_GATES:
        if name not in completed:
            break
        count += 1
    return {'scope': scope, 'passed': count, 'total': len(DIAGNOSTIC_GATES),
            'current': str(message or identifier)[:160]}


def ladder_gate_progress(cases, scope, message=''):
    """Count only finalized successful cases in this diagnostic's own snapshot."""
    rows = list(cases.values()) if isinstance(cases, dict) else list(cases or [])
    stages = ('maestro_baseline', 'private_netlist', 'maestro_netlist')
    passed = sum(1 for engine in stages if any(
        isinstance(row, dict) and row.get('engine') == engine and row.get('state') == 'completed'
        for row in rows))
    return {'scope': str(scope or 'diagnostics')[:160], 'passed': passed, 'total': 3,
            'current': str(message or '')[:160]}
