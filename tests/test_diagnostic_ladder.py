"""Diagnostic ladder decisions using isolated fake stage executions."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from test_environment_test import FixtureService
from crescendo_engine.environment_test import EnvironmentTestManager, _freeze_diagnostic_candidate


class LadderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.service = FixtureService(os.path.join(self.tmp.name, 'fixture'))
        self.service.prepared['prepared-fixture']['setup']['tests'][0]['simulator'] = 'spectre'
        self.manager = EnvironmentTestManager(self.service)
        self.service.manager = self.manager
        self.spec = {'variables': [{'parameter_id': 'x', 'minimum': 1e-6, 'maximum': 2e-6}],
                     'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}],
                     'simulator': 'alps', 'execution_commands': {'solver': 'configured'}}
        self.calls = []
        self.behavior = {}
        def engine(row, name):
            stage_spec = self.manager._engine_spec(row, name)
            self.calls.append(stage_spec)
            kind = self.behavior.get(name, 'pass')
            if kind == 'cancel':
                row['cancel'].set()
            if kind.startswith('corner:'):
                reason = kind.split(':', 1)[1]
                row['engine_outcomes'][name] = {'first_cause': {'failure_stage': 'native_corner_setup_identity',
                    'corner_identity': {'differences': [{'reason': reason}]}}}
            if kind == 'source':
                row['engine_outcomes'][name] = {'shared_source_failure': {'ok': False}}
            if kind != 'pass':
                self.manager._check(row, 'execution', 'FAIL', 'fixture failure', engine=name)
            else:
                self.manager._check(row, 'execution', 'PASS', 'fixture success', engine=name)
            return kind != 'unsafe'
        self.patches = [mock.patch.object(self.manager, '_engine', side_effect=engine),
                        mock.patch('crescendo_engine.environment_test._case_capability', return_value={'ready': True}),
                        mock.patch('crescendo_engine.environment_test.run_environment_probes', return_value=[])]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.addCleanup(self.manager.cancel_all)

    def run_ladder(self):
        result = self.manager.start('prepared-fixture', self.spec)
        self.assertTrue(self.manager.join(30))
        return self.manager.progress(result['test_id'])

    def test_current_value_preferred_and_invalid_value_falls_back(self):
        candidate, origin = _freeze_diagnostic_candidate(self.spec, self.service.prepared['prepared-fixture'])
        self.assertEqual(candidate['x'], 1e-6)
        self.assertEqual(origin['x']['source'], 'design_current_value')
        prepared = copy.deepcopy(self.service.prepared['prepared-fixture'])
        prepared['parameters'][0]['current_value'] = '3u'
        candidate, origin = _freeze_diagnostic_candidate(self.spec, prepared)
        self.assertEqual(origin['x']['source'], 'seed1_sample')
        self.assertGreater(candidate['x'], 1e-6)
        self.assertLess(candidate['x'], 2e-6)

    def test_order_shared_candidate_and_mixed_solver(self):
        result = self.run_ladder()
        self.assertEqual([s['execution_backend'] for s in self.calls], ['maestro_baseline', 'private_netlist', 'maestro_netlist'])
        self.assertEqual([s['simulator'] for s in self.calls], ['spectre', 'alps', 'alps'])
        self.assertEqual(len({repr(s['environment_test']['frozen_candidate']) for s in self.calls}), 1)
        self.assertNotIn('execution_commands', self.calls[0])
        self.assertEqual(self.calls[0]['maestro_job_overrides'], {})
        self.assertEqual(self.calls[1]['execution_commands'], self.spec['execution_commands'])
        check = next(c for c in result['checks'] if c['id'] == 'test.scope')
        self.assertEqual(check['details']['numeric_equivalence'], 'not_comparable_across_solvers')
        self.assertEqual(len(result['case_results']), 3)
        with open(result['main_log'], encoding='utf-8') as stream:
            text = stream.read()
        self.assertIn('diagnostic_shape', text)
        self.assertEqual(result['diagnostic_shape']['total_candidates'], 6)
        self.assertEqual(result['diagnostic_shape']['distinct_candidates'], 6)
        self.assertEqual(len({repr(s['environment_test']['frozen_plan']) for s in self.calls}), 1)
        self.assertIn('三级诊断摘要', text)

    def test_safe_failure_continues_and_unsafe_stops(self):
        self.behavior['maestro_baseline'] = 'fail'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(result['case_results'][0]['state'], 'failed')

    def test_private_failure_does_not_block_independent_maestro_capture(self):
        self.behavior['private_netlist'] = 'fail'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 3)
        self.assertEqual([x['state'] for x in result['case_results']], ['completed', 'failed', 'completed'])

    def test_unknown_source_simulator_blocks_all_stages(self):
        self.service.prepared['prepared-fixture']['setup']['tests'][0]['simulator'] = None
        result = self.run_ladder()
        self.assertFalse(self.calls)
        self.assertEqual([x['state'] for x in result['case_results']], ['blocked'] * 3)
        self.assertFalse(result['case_results'][0]['simulator_identity_known'])

    def test_unproven_cleanup_blocks_later_stages(self):
        self.behavior['maestro_baseline'] = 'unsafe'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([x['state'] for x in result['case_results']], ['failed', 'blocked', 'blocked'])
        self.assertIn('unproven', result['case_results'][1]['skip_reason'])

    def test_invalid_cli_does_not_block_baseline(self):
        self.spec.update(command_settings_valid=False, job_settings_valid=True)
        result = self.run_ladder()
        self.assertEqual([s['execution_backend'] for s in self.calls], ['maestro_baseline'])
        self.assertEqual([x['state'] for x in result['case_results']], ['completed', 'blocked', 'blocked'])

    def test_job_input_failure_blocks_all_modes(self):
        self.spec['job_settings_valid'] = False
        result = self.run_ladder()
        self.assertEqual(self.calls, [])
        self.assertTrue(all(case['state'] == 'blocked' for case in result['case_results']))

    def test_job_input_values_reach_baseline_and_both_cli_modes(self):
        self.spec['maestro_job_overrides'] = {'command': 'user_submit', 'max_jobs': 7,
                                             'timeouts': {'run_seconds': 90}}
        self.run_ladder()
        self.assertEqual(len(self.calls), 3)
        for spec in self.calls:
            self.assertEqual(spec['maestro_job_overrides'], self.spec['maestro_job_overrides'])

    def test_cancel_and_shared_source_block_later_stages(self):
        self.behavior['maestro_baseline'] = 'cancel'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([x['state'] for x in result['case_results']][1:], ['cancelled', 'cancelled'])

    def test_corner_source_and_private_clone_have_distinct_ladder_effects(self):
        from crescendo_engine.environment_test import _corner_source_untrusted
        self.assertTrue(_corner_source_untrusted({'failure_stage': 'native_corner_setup_identity'}))
        self.assertTrue(_corner_source_untrusted({'failure_stage': 'native_corner_setup_identity',
            'corner_identity': {'differences': [{'reason': 'unexpected'}]}}))
        self.behavior['maestro_baseline'] = 'corner:inspected_source_is_stale_or_incomplete'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([x['state'] for x in result['case_results']], ['failed', 'blocked', 'blocked'])

    def test_corner_private_clone_difference_allows_next_stages_after_cleanup(self):
        self.behavior['maestro_baseline'] = 'corner:private_setup_clone_changed_semantics'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 3)
        self.assertEqual([x['state'] for x in result['case_results']], ['failed', 'completed', 'completed'])

    def test_source_guard_blocks_later_stages(self):
        self.behavior['maestro_baseline'] = 'source'
        result = self.run_ladder()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual([x['state'] for x in result['case_results']][1:], ['blocked', 'blocked'])

if __name__ == '__main__':
    unittest.main()
