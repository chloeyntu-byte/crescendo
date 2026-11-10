import copy
import unittest
from unittest import mock

from crescendo_engine import environment_probes as probes


class EnvironmentProbeSelectionTests(unittest.TestCase):
    def setUp(self):
        self.prepared = {'source_identity': {'original': 'frozen'}, 'setup': {
            'source': {'library': 'SOURCE_LIB', 'maestro_sdb': '/source/maestro/maestro.sdb'},
            'tests': [
                {'name': 'A', 'enabled': True, 'analyses': [{'name': 'tran', 'enabled': True}]},
                {'name': 'B', 'enabled': False, 'analyses': [{'name': 'ac', 'enabled': False}, {'name': 'dc', 'enabled': True}]}]},
            'job_policy': {'available': True, 'global': {'available': True, 'properties': {
                'distributionmethod': 'Command', 'jobsubmitcommand': '/missing/batch-submit -q source'}},
                'tests': [{'name': 'A', 'available': True, 'properties': {
                    'distributionmethod': 'Command', 'jobsubmitcommand': '/missing/batch-submit -q A'}},
                    {'name': 'B', 'available': True, 'properties': {'distributionmethod': 'Local'}}]}}
        self.spec = {'execution_selection': {'tests': ['B'], 'analyses': {'B': ['ac']}},
                     'objectives': [{'output_key': 'B::gain'}], 'constraints': [{'output_key': 'B::power'}],
                     'simulator': 'alps', '_diagnostic_package_identity': {'application_version': 'fixture'}}

    def test_summary_uses_current_selection_and_preserves_original_identity_without_io(self):
        original, spec = copy.deepcopy(self.prepared), copy.deepcopy(self.spec)
        with mock.patch.object(probes, 'bounded_command', side_effect=AssertionError('no process')), \
                mock.patch('crescendo_engine.environment_diagnostics.package_identity', side_effect=AssertionError('no extra I/O')):
            checks = probes.run_environment_probes({}, self.prepared, '/unused', spec=self.spec)
        details = checks[0]['details']
        self.assertEqual(details['enabled_tests'], ['B'])
        self.assertEqual(details['enabled_analyses'], {'B': ['ac']})
        self.assertEqual(details['source_enabled_tests'], ['A'])
        self.assertEqual(details['setup']['library'], 'SOURCE_LIB')
        self.assertEqual(details['setup']['maestro_sdb'], '/source/maestro/maestro.sdb')
        self.assertEqual(details['selected_simulator'], 'alps')
        self.assertEqual(details['selected_measurements'], {'objectives': ['B::gain'], 'constraints': ['B::power']})
        self.assertEqual([row['test'] for row in details['job_policies']], [None, 'B'])
        self.assertEqual(self.prepared, original)
        self.assertEqual(self.spec, spec)

    def test_local_selected_test_does_not_probe_unused_global_or_other_test_scheduler(self):
        with mock.patch.object(probes, 'bounded_command', side_effect=AssertionError('unused scheduler must not run')):
            state, _message, details = probes._scheduler_capability({}, self.prepared, '/unused', {}, None, self.spec)
        self.assertEqual(state, 'NOT_TESTED')
        self.assertEqual(details['commands'], [])

    def test_selected_test_with_no_policy_row_inherits_global_command(self):
        self.prepared['job_policy']['tests'] = [self.prepared['job_policy']['tests'][0]]
        state, _message, details = probes._scheduler_capability({}, self.prepared, '/unused', {}, None, self.spec)
        self.assertEqual(state, 'FAIL')
        self.assertEqual(details['commands'][0]['test'], 'B')
        self.assertEqual(details['commands'][0]['submit_argv'], ['/missing/batch-submit', '-q', 'source'])

    def test_bad_command_in_unselected_test_does_not_block_selected_policy(self):
        self.prepared['job_policy']['tests'][0]['properties']['jobsubmitcommand'] = ''
        policy, rows = probes._policy_rows(self.prepared, self.spec)
        self.assertEqual([row['name'] for row in policy['tests']], ['B'])
        self.assertEqual(rows[-1]['distribution'], 'Local')
        self.assertEqual(self.prepared['job_policy']['tests'][0]['properties']['jobsubmitcommand'], '')

    def test_default_selection_observes_source_enabled_tests(self):
        policy, rows = probes._policy_rows(self.prepared, {})
        self.assertEqual([row['name'] for row in policy['tests']], ['A'])
        self.assertEqual([row['test'] for row in rows], [None, 'A'])

    def test_unknown_catalogue_keeps_legacy_policy_observation(self):
        self.prepared.pop('setup')
        policy, rows = probes._policy_rows(self.prepared, {})
        self.assertEqual([row['name'] for row in policy['tests']], ['A', 'B'])
        self.assertEqual(len(rows), 3)

    def test_analysis_and_output_conflicts_are_reported_by_the_shared_selection_contract(self):
        self.spec['execution_selection']['analyses']['B'] = ['unknown']
        with self.assertRaisesRegex(ValueError, 'Unknown analyses'):
            probes.run_environment_probes({}, self.prepared, '/unused', spec=self.spec)
        self.spec['execution_selection']['analyses']['B'] = ['ac']
        self.spec['objectives'] = [{'output_key': 'A::gain'}]
        with self.assertRaisesRegex(ValueError, 'unselected Test A'):
            probes.run_environment_probes({}, self.prepared, '/unused', spec=self.spec)
