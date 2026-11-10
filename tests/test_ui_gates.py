import unittest
import os
import tempfile
import json

from crescendo_engine.ui_gates import (gate_progress, merge_gate_progress,
    setup_gate_progress, diagnostic_gate_progress, diagnostic_native_progress, DIAGNOSTIC_GATES)


class GateProgressTests(unittest.TestCase):
    def test_service_projects_durable_top_level_gate_ahead_of_unrelated_latest_event(self):
        from crescendo_engine.server import OptimizerService
        with tempfile.TemporaryDirectory() as root:
            directory = os.path.join(root, 'run-1', 'private_netlist')
            os.makedirs(directory)
            gate = {'scope': 'run-1:c1', 'passed': 2, 'total': 5, 'current': 'c1: netlist verified'}
            with open(os.path.join(directory, 'progress.json'), 'w') as stream:
                json.dump({'schema_version': 'crescendo.private_netlist_optimizer.v1', 'gate_progress': gate,
                           'latest': {'stage': 'solver_command', 'gate_progress': dict(gate, passed=1)}}, stream)
            service = OptimizerService({'runtime': {'run_root': root}})
            projected = service._execution_progress('run-1', {'state': 'running'},
                                                   {'execution_backend': 'maestro_netlist'}, [])
            self.assertEqual(projected['gate_progress'], gate)

    def test_malformed_unknown_and_bool_counts_do_not_claim_progress(self):
        for value in (None, {}, {'scope': 'a', 'passed': True, 'total': 2},
                      {'scope': 'a', 'passed': 3, 'total': 2},
                      {'scope': 'a', 'passed': 0, 'total': 0}):
            self.assertIsNone(gate_progress(value))

    def test_same_scope_is_idempotent_monotonic_and_has_fixed_denominator(self):
        prior = {'scope': 'candidate1', 'passed': 3, 'total': 5, 'current': 'results read'}
        self.assertEqual(merge_gate_progress(prior, dict(prior, passed=1)), prior)
        self.assertEqual(merge_gate_progress(prior, dict(prior, total=10)), prior)
        self.assertEqual(merge_gate_progress(prior, prior), prior)
        self.assertEqual(merge_gate_progress(prior, dict(prior, scope='candidate2', passed=0))['passed'], 0)

    def test_inspection_counts_only_completed_prior_operations(self):
        self.assertEqual(setup_gate_progress('load_setup', 'saved_setup')['passed'], 0)
        self.assertIsNone(setup_gate_progress('load_setup', 'private_copy'))
        self.assertEqual(setup_gate_progress('_test_selected', 'private_copy')['passed'], 3)
        self.assertEqual(setup_gate_progress('load_setup', 'inspection_complete')['passed'], 1)
        self.assertEqual(setup_gate_progress('_test_selected', 'inspection_complete')['passed'], 4)
        self.assertIsNone(setup_gate_progress('load_setup', 'inspection_failed'))
        self.assertIsNone(setup_gate_progress('load_setup', 'configuration_warning'))
        self.assertIsNone(setup_gate_progress('load_setup', None))

    def test_restored_draft_does_not_claim_skipped_apply_gates(self):
        previous = setup_gate_progress('load_configuration', 'configuration_matching')
        self.assertEqual(setup_gate_progress('load_configuration', 'configuration_complete', previous=previous), previous)
        ready = setup_gate_progress('load_configuration', 'parameters_ready')
        complete = setup_gate_progress('load_configuration', 'configuration_complete', previous=ready)
        self.assertEqual(complete['passed'], complete['total'])

    def test_suite_load_and_update_gates_follow_completed_events(self):
        previous = None
        for index, stage in enumerate(('suite_open', 'suite_compare', 'suite_publish', 'suite_ready')):
            previous = setup_gate_progress('load_configuration', stage, previous=previous)
            self.assertEqual((previous['scope'], previous['passed'], previous['total']), ('suite_load', index, 3))
        previous = None
        for index, stage in enumerate(('suite_merge', 'suite_native_check', 'suite_publish', 'suite_ready')):
            previous = setup_gate_progress('load_configuration', stage, previous=previous)
            self.assertEqual((previous['scope'], previous['passed'], previous['total']), ('suite_update', index, 3))
        self.assertEqual(setup_gate_progress('load_configuration', 'suite_hierarchy', previous=previous), previous)

    def test_suite_unchanged_has_own_plan_and_never_claims_native_validation(self):
        merge = setup_gate_progress('load_configuration', 'suite_merge')
        self.assertEqual(setup_gate_progress('load_configuration', 'suite_ready', previous=merge), merge)
        unchanged = setup_gate_progress('load_configuration', 'suite_unchanged', previous=merge)
        self.assertEqual((unchanged['scope'], unchanged['passed'], unchanged['total']), ('suite_unchanged', 1, 4))
        published = setup_gate_progress('load_configuration', 'suite_publish', previous=unchanged)
        ready = setup_gate_progress('load_configuration', 'suite_ready', previous=published)
        ready = setup_gate_progress('load_configuration', 'suite_reuse_ready', previous=ready)
        self.assertEqual((ready['passed'], ready['total']), (4, 4))
        self.assertEqual(setup_gate_progress('load_configuration', 'suite_native_check', previous=ready), ready)
        self.assertIsNone(setup_gate_progress('load_configuration', 'suite_publish'))

    def test_failed_cancelled_or_unrelated_diagnostic_checks_never_close_gates(self):
        state = {}
        diagnostic_gate_progress(state, 'request', 'PASS', 'test1')
        for status in ('FAIL', 'WARN', 'BLOCKED', 'CANCELLED', 'NOT_TESTED'):
            value = diagnostic_gate_progress(state, 'launch', status, 'test1')
            self.assertEqual(value['passed'], 1)
        diagnostic_gate_progress(state, 'termination_and_cleanup', 'PASS', 'test1')
        self.assertEqual(diagnostic_gate_progress(state, 'test.summary', 'PASS', 'test1')['passed'], 1)
        for name in DIAGNOSTIC_GATES:
            value = diagnostic_gate_progress(state, name, 'PASS', 'test1')
        self.assertEqual(value['passed'], value['total'])

    def test_diagnostic_native_gates_advance_one_overall_scale_with_no_scope_reset(self):
        state = {}
        diagnostic_gate_progress(state, 'request', 'PASS', 'test1')
        diagnostic_gate_progress(state, 'launch', 'PASS', 'test1')
        native = {'scope': 'owned:c1', 'passed': 2, 'total': 5, 'current': 'Netlist verified'}
        value = diagnostic_native_progress(state, native, 'test1')
        state['gate_progress'] = value
        self.assertEqual((value['passed'], value['total']), (4, 12))
        self.assertEqual(diagnostic_native_progress(state, dict(native, passed=1), 'test1')['passed'], 4)
        self.assertEqual(diagnostic_native_progress(state, dict(native, scope='other:c1', passed=5), 'test1')['passed'], 4)
        value = diagnostic_native_progress(state, dict(native, passed=5), 'test1')
        self.assertEqual(value['passed'], 7)
        value = diagnostic_gate_progress(state, 'execution', 'FAIL', 'test1')
        self.assertEqual(value['passed'], 7)


if __name__ == '__main__':
    unittest.main()
