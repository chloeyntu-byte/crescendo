"""Real GUI task queue and service; native hierarchy/binding are fixtures."""
import copy
import json
import os
import unittest
from unittest import mock
from crescendo_engine import gui
from tests import test_gui_cell_selection as tk_fixtures
from tests import test_setup_private_ui as service_fixtures


@unittest.skipUnless(os.environ.get('DISPLAY'), 'requires real Tk')
class GuiSingleTestConfigurationTests(unittest.TestCase):
    setUp = tk_fixtures.GuiCellSelectionTests.setUp
    _close = tk_fixtures.GuiCellSelectionTests._close
    _wait = tk_fixtures.GuiCellSelectionTests._wait

    def _open_real(self):
        case = service_fixtures.SetupPrivateServiceIntegrationTests('test_inspect_creates_private_suite_before_apply_and_apply_reuses_identity')
        case.setUp(); self.addCleanup(case.doCleanups)
        self.case = case
        self.service = case.service
        def prepare_selected(*args, **kwargs):
            from crescendo_engine.single_test import project_setup
            self.assertEqual(kwargs.get('selected_test'), 'C')
            prepared = copy.deepcopy(case.fixture.prepared)
            prepared['setup'] = project_setup(prepared['setup'], kwargs['selected_test'])
            return prepared
        for target, kwargs in [
            ('crescendo_engine.hierarchy.probe_hierarchy', {'return_value': case.fixture.hierarchy}),
            ('crescendo_engine.hierarchy.cell_scope_records', {'return_value': [case.scope]}),
            ('crescendo_engine.server.prepare_setup', {'side_effect': prepare_selected}),
        ]:
            patch = mock.patch(target, **kwargs); patch.start(); self.addCleanup(patch.stop)
        self.app = gui.OptimizerGui(self.root, self.service)
        self.root.report_callback_exception = lambda exc, value, trace: self.callback_errors.append((exc, value))
        self.app.setup_path_var.set(case.fixture.path)

    def test_save_and_restore_single_test_through_real_service(self):
        self._open_real()
        from crescendo_engine import setup_private_session as sessions
        native_bind = sessions._PrivateSession._native_bind
        patch = mock.patch.object(sessions._PrivateSession, '_native_bind',
            lambda controller, rows, config, cancel: native_bind(controller, [], config, cancel))
        patch.start(); self.addCleanup(patch.stop)
        self.case.fixture.prepared['parameters'] = [{
            'parameter_id': '/M0/w', 'name': 'w', 'prompt': 'Width', 'semantic': 'width',
            'design_library': 'design', 'design_cell': 'TB', 'design_view': 'schematic',
            'optimizable': True, 'hierarchy_path': '/M0', 'instance_name': 'M0',
            'master_library': 'design', 'master_cell': 'nmos', 'numeric_value': 1e-6,
            'current_value': '1u', 'value_type': 'float'}]
        self.case.fixture.prepared['outputs'] = [{'key': 'C::gain', 'name': 'gain', 'test': 'C'}]
        self.case.fixture.prepared['setup']['corners'] = [{'name': 'A', 'enabled': True}, {'name': 'B', 'enabled': True}]
        self.case.fixture.prepared['setup']['test_corner_masks'] = {'C': {'known': True, 'disabled_corners': []}}
        self.app.load_setup()
        self._wait(lambda: self.app.cell_inspection_id is not None, timeout=10)
        self.assertEqual(self.app.selected_test, 'C')
        self.app.select_all_cells(True); self.app.apply_selected_cells()
        self._wait(lambda: not self.app.setup_loading, timeout=10)
        self.assertIsNotNone(self.app.prepared_id, self.app.status_var.get())
        self.app.parameter_configs['/M0/w'].update(selected=True, minimum='0.5u', maximum='2u')
        self.app._reset_metric_rows()
        self.app.add_objective({'output_key': 'C::gain', 'direction': 'minimize', 'aggregation': 'max', 'corners': ['B']})
        self.app.add_constraint({'output_key': 'C::gain', 'operator': '<=', 'limit': '4', 'aggregation': 'min', 'corners': ['A']})
        path = os.path.join(self.case.fixture.root, 'single.json')
        with mock.patch.object(gui.filedialog, 'asksaveasfilename', return_value=path):
            self.app.save_configuration()
            self._wait(lambda: str(self.app.save_config_button['state']) == 'normal', timeout=10)
        self.assertTrue(os.path.exists(path), self.app.settings_status_var.get())
        with open(path) as f: document = json.load(f)
        self.assertEqual(document['execution_selection']['tests'], ['C'])
        original_inspection_id = self.app.test_inspection_id
        original_suite_ids = set(self.service.setup_private_sessions)
        with mock.patch.object(gui.filedialog, 'askopenfilename', return_value=path), \
                mock.patch('crescendo_engine.hierarchy.probe_hierarchy', side_effect=AssertionError('suite restore must not rescan')):
            self.app.load_configuration()
            self._wait(lambda: not self.app.configuration_loading, timeout=10)
        self.assertEqual(self.app.selected_test, 'C', self.app.settings_status_var.get())
        self.assertEqual(self.app.test_inspection_id, original_inspection_id)
        self.assertEqual(self.app.test_choice_var.get(), 'C')
        self.assertEqual(set(self.service.setup_private_sessions), original_suite_ids)
        self.assertIsNotNone(self.app.cell_inspection_id)
        self.assertIsNotNone(self.app.prepared_id, self.app.settings_status_var.get())
        self.assertEqual(self.app.selected_cell_scope_ids, {'root'})
        self.assertEqual(self.app.execution_selection_panel.collect()['tests'], ['C'])
        parameter = self.app.parameter_configs['/M0/w']
        self.assertEqual((parameter['minimum'], parameter['maximum']), ('0.5u', '2u'), self.app.settings_match_detail)
        self.assertEqual(self.app.objective_rows[0].definition('objective'), {'output_key': 'C::gain', 'direction': 'minimize', 'aggregation': 'max', 'corners': ['B']})
        self.assertEqual(self.app.constraint_rows[0].definition('constraint'), {'output_key': 'C::gain', 'operator': '<=', 'limit': '4', 'aggregation': 'min', 'corners': ['A']})

    def test_old_multi_draft_does_not_scan_and_unselected_uses_only_test(self):
        self._open_real()
        for selection in ({'tests': ['C', 'OTHER']}, None):
            path = os.path.join(self.case.fixture.root, 'draft.json')
            document = {'schema_version': 'crescendo.configuration.v1',
                'source_setup': {'path': self.case.fixture.path, 'backend': 'maestro'},
                'settings': {'schema_version': 'crescendo.ui_settings.v1', 'cells': [], 'parameters': [],
                             'outputs': [], 'objectives': [], 'constraints': [], 'optimization': {}}}
            if selection is not None: document['execution_selection'] = selection
            with open(path, 'w') as f: json.dump(document, f)
            with mock.patch.object(gui.filedialog, 'askopenfilename', return_value=path):
                self.app.load_configuration()
                self._wait(lambda: not self.app.configuration_loading, timeout=10)
            self.assertIsNotNone(self.app.test_inspection_id, self.app.settings_status_var.get())
            if selection is not None:
                self.assertIsNone(self.app.selected_test)
                self.assertIsNone(self.app.cell_inspection_id)
                self.assertIn('Select a Test', self.app.settings_status_var.get())
            else:
                self.assertEqual(self.app.selected_test, 'C')
                self.assertIsNotNone(self.app.cell_inspection_id)
            self.assertIsNone(self.app.prepared_id)
