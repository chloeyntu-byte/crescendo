import copy
import unittest
from unittest import mock

from crescendo_engine.environment_test import EnvironmentTestManager, _report_context
from tests import test_gui_environment_test as gui_fixtures


def setup_fixture():
    return {'tests': [
        {'name': 'A', 'enabled': True, 'analyses': [{'name': 'tran', 'enabled': True}]},
        {'name': 'B', 'enabled': False, 'analyses': [{'name': 'ac', 'enabled': False}, {'name': 'dc', 'enabled': True}]}],
        'corners': [{'name': 'TT', 'enabled': True}, {'name': 'SS', 'enabled': False}],
        'test_corner_masks': {'A': {'known': True, 'disabled_corners': ['SS']},
                              'B': {'known': True, 'disabled_corners': ['TT']}}}


class DiagnosticSelectionTests(unittest.TestCase):
    def test_ui_diagnostic_spec_keeps_multiple_selected_analyses(self):
        app = gui_fixtures.EnvironmentTestInputTests.app(None)
        selected = {'tests': ['B'], 'analyses': {'B': ['ac', 'sp']}}
        app.execution_selection_panel = mock.Mock()
        app.execution_selection_panel.collect.return_value = selected
        spec = app._collect_environment_test_spec()
        self.assertEqual(spec['execution_selection'], selected)
        self.assertTrue(spec['execution_selection_valid'])
        app._build_run_settings.assert_not_called()

    def test_ui_empty_test_or_analysis_draft_is_retained_and_blocks_execution(self):
        for draft in ({'tests': [], 'analyses': {}}, {'tests': ['B'], 'analyses': {'B': []}}):
            app = gui_fixtures.EnvironmentTestInputTests.app(None)
            app.execution_selection_panel = mock.Mock()
            app.execution_selection_panel.collect.side_effect = lambda require_selection=True: (
                (_ for _ in ()).throw(ValueError('Test/Analysis selection is empty')) if require_selection else draft)
            spec = app._collect_environment_test_spec()
            self.assertEqual(spec['execution_selection'], draft)
            self.assertFalse(spec['execution_selection_valid'])
            self.assertIn('Test/Analysis selection is empty', ' '.join(spec['ui_input_warnings']))

    def test_engine_spec_keeps_exact_test_analysis_selection_and_does_not_mutate_it(self):
        selected = {'tests': ['B'], 'analyses': {'B': ['ac']}}
        row = {'spec': {'execution_selection': copy.deepcopy(selected)},
               'selected_simulator': 'spectre', 'test_id': 'EnvTest_fixture'}
        spec = EnvironmentTestManager(None)._engine_spec(row, 'maestro_netlist')
        self.assertEqual(spec['execution_selection'], selected)
        self.assertEqual(spec['population'], 1)
        self.assertEqual(spec['generations'], 1)
        spec['execution_selection']['analyses']['B'].append('dc')
        self.assertEqual(row['spec']['execution_selection'], selected)

    def test_report_context_shows_disabled_catalogue_and_per_test_corner_masks_without_io(self):
        setup = setup_fixture()
        row = {'test_id': 'fixture', 'prepared': {'setup': setup},
               'spec': {'execution_selection': {'tests': ['B'], 'analyses': {'B': ['ac']}}}}
        before = copy.deepcopy(row)
        with mock.patch('builtins.open', side_effect=AssertionError('no additional I/O')):
            context = _report_context(row)['execution_selection']
        self.assertEqual(context['requested_tests'], ['B'])
        a, b = context['tests']
        self.assertFalse(a['selected'])
        self.assertTrue(b['selected'])
        self.assertFalse(b['source_enabled'])
        self.assertEqual(b['requested_analyses'], ['ac'])
        self.assertEqual(b['available_analyses'][0], {'name': 'ac', 'enabled': False})
        self.assertTrue(a['corners'][0]['test_enabled'])
        self.assertFalse(b['corners'][0]['test_enabled'])
        self.assertEqual(row, before)

    def test_static_capability_uses_requested_tests_and_analyses(self):
        manager = EnvironmentTestManager(None)
        setup = setup_fixture()
        row = {'prepared': {'setup': setup}, 'spec': {'execution_selection': {'tests': ['B'], 'analyses': {'B': ['ac']}}}}
        seen = []
        def capability(_config, prepared, _simulator, _engine):
            seen.append(prepared['setup'])
            return {'ready': True}
        with mock.patch('crescendo_engine.environment_test._case_capability', side_effect=capability), \
                mock.patch.object(manager, '_check'):
            manager._shared_case_capability(row, {}, 'spectre', 'maestro_netlist')
        self.assertFalse(seen[0]['tests'][0]['enabled'])
        self.assertTrue(seen[0]['tests'][1]['enabled'])
        self.assertTrue(seen[0]['tests'][1]['analyses'][0]['enabled'])
        self.assertFalse(seen[0]['tests'][1]['analyses'][1]['enabled'])
        self.assertEqual(setup, setup_fixture())

    def test_native_phase_updates_shared_progress_without_copying_diagnostic_log(self):
        manager = EnvironmentTestManager(None)
        progress = {'revision': 0, 'checks_revision': 0, 'passed_gates': ['request', 'launch']}
        row = {'config': {}, 'ui_progress': progress, 'case_records': {}, 'case_record_bytes': {},
               'record_signatures': set(), 'case_id': 'spectre.maestro_netlist', 'selected_simulator': 'spectre'}
        manager._log(dict(row), {'observation': 'execution.progress', 'payload': {
            'execution_stage': 'native_capture', 'message': 'Reading private native setup',
            'gate_progress': {'scope': 'owned:c1', 'passed': 3, 'total': 5, 'current': 'Results read'}}})
        self.assertEqual(progress['stage'], 'native_capture')
        self.assertEqual(progress['message'], 'Reading private native setup')
        self.assertEqual(progress['revision'], 1)
        self.assertEqual(progress['checks_revision'], 0)
        self.assertEqual((progress['gate_progress']['passed'], progress['gate_progress']['total']), (0, 3))
        self.assertEqual(progress['gate_progress']['current'], 'Results read')
