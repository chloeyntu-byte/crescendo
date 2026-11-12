"""Configuration import recovery checks in the real Tk UI, with fixture services."""
import copy
import json
import os
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.unified_configuration import read_configuration, save_configuration
from tests import test_gui_optional_settings as fixture
from tests import test_unified_configuration_gui as base_module


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk requires DISPLAY')
class UnifiedConfigurationGuiEdgeTests(unittest.TestCase):
    setUp = fixture.GuiOptionalSettingsTkTests.setUp
    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait
    _prepared = fixture.GuiOptionalSettingsTkTests._prepared
    _install = base_module.UnifiedConfigurationGuiTests._install
    _save = base_module.UnifiedConfigurationGuiTests._save
    _load = base_module.UnifiedConfigurationGuiTests._load

    def _legacy_file(self):
        document = {'schema_version': 'crescendo.design_config.v1',
            'ade_setup': {'path': self.setup_path, 'backend': 'maestro'},
            'optimization': {'variables': [{'parameter_id': '/M0/w', 'minimum': 1e-6, 'maximum': 3e-6, 'scale': 'linear'}],
                'objectives': [{'output_key': 'frequency', 'direction': 'target', 'target': 123,
                                'aggregation': 'mean', 'corners': ['tt']}],
                'constraints': [{'output_key': 'frequency', 'operator': '>=', 'limit': 50,
                                 'aggregation': 'min', 'corners': ['tt']}],
                'run': {'generations': 11, 'algorithm': 'differential_evolution',
                        'algorithm_parameters': {'population': 8, 'seed': 3},
                        'simulator': 'alps', 'execution_backend': 'private_netlist'}}}
        path = os.path.join(self.directory.name, 'legacy.json')
        with open(path, 'w') as stream:
            json.dump(document, stream)
        return path

    def test_legacy_import_then_manual_apply_restores_all_settings(self):
        app = self._install()
        self._load(self._legacy_file())
        self.assertFalse(app.prepared_id)
        app.selected_cell_scope_ids = {'cell-B'}
        app._render_cell_scopes()
        app._update_cell_selection_state()
        app.apply_selected_cells()
        self._wait(lambda: not app.setup_loading and app.pending_settings_state is None)
        self.assertTrue(app.parameter_configs['/M0/w']['selected'])
        self.assertEqual(float(app.parameter_configs['/M0/w']['minimum']), 1e-6)
        self.assertEqual(app.objective_rows[0].value_var.get(), '123')
        self.assertEqual(app.constraint_rows[0].value_var.get(), '50')
        self.assertEqual(app.generations_var.get(), '11')
        self.assertEqual(app.simulator_var.get(), 'alps')
        self.assertEqual(app.execution_backend_var.get(), 'private_netlist')
        self.assertEqual(app.selected_cell_scope_ids, {'cell-B'})
        self.assertEqual(app.applied_cell_scope_ids, {'cell-B'})
        self.assertFalse(app.cell_scope_dirty)
        self.assertEqual(str(app.start_button['state']), 'normal')
        self.assertFalse(self.showerror.called)

    def test_configuration_without_cells_does_not_clear_manual_apply_choice(self):
        app = self._install()
        app.parameter_configs['/M0/w'].update(selected=True, minimum='1', maximum='3')
        path = self._save()
        document = read_configuration(path)
        document['settings'].pop('cells', None)
        document['selection']['provided'] = False
        save_configuration(path, document)
        self._load(path)
        app.selected_cell_scope_ids = {'cell-B'}
        app._render_cell_scopes()
        app._update_cell_selection_state()
        app.apply_selected_cells()
        self._wait(lambda: not app.setup_loading and app.pending_settings_state is None)
        self.assertEqual(app.selected_cell_scope_ids, {'cell-B'})
        self.assertFalse(app.cell_scope_dirty)
        self.assertTrue(app.parameter_configs['/M0/w']['selected'])
        self.assertFalse(self.showerror.called)

    def test_corrupt_file_does_not_invalidate_prepared_or_effective_environment(self):
        app = self._install()
        app.generations_var.set('13')
        prepared_id = app.prepared_id
        original_scope = set(app.selected_cell_scope_ids)
        original_eda = copy.deepcopy(app.eda_setup)
        path = os.path.join(self.directory.name, 'broken.json')
        with open(path, 'w') as stream:
            stream.write('{"schema_version":')
        self._load(path)
        self.assertEqual(app.prepared_id, prepared_id)
        self.assertEqual(app.generations_var.get(), '13')
        self.assertEqual(app.selected_cell_scope_ids, original_scope)
        self.assertEqual(app.eda_setup, original_eda)
        self.assertEqual(str(app.start_button['state']), 'normal')
        self.assertFalse(self.showerror.called)

    def test_unknown_schema_keeps_ready_controls_and_settings(self):
        app = self._install()
        app.job_max_var.set('7')
        prepared_id = app.prepared_id
        path = os.path.join(self.directory.name, 'future.json')
        with open(path, 'w') as stream:
            json.dump({'schema_version': 'future.v99'}, stream)
        self._load(path)
        self.assertEqual(app.prepared_id, prepared_id)
        self.assertEqual(app.job_max_var.get(), '7')
        self.assertEqual(str(app.load_config_button['state']), 'normal')
        self.assertFalse(self.showerror.called)

    def test_cancelled_file_dialog_has_no_prepare_side_effect(self):
        app = self._install()
        prepared_id = app.prepared_id
        with mock.patch.object(gui.filedialog, 'askopenfilename', return_value=''):
            app.load_configuration()
        self.assertFalse(app.configuration_loading)
        self.assertEqual(app.prepared_id, prepared_id)
        self.assertFalse(self.service.configuration_calls)
        self.assertFalse(self.showerror.called)

    def test_cancelled_load_keeps_ui_and_reports_retained_suite(self):
        from crescendo_engine.cadence import CommandCancelled
        app = self._install()
        path = self._save()
        app.generations_var.set('37')
        before = (app.prepared_id, set(app.selected_cell_scope_ids),
                  copy.deepcopy(app.parameter_configs), copy.deepcopy(app.active_private_suite))
        def cancel(_document, **kwargs):
            error = CommandCancelled('stop')
            error.configuration_import = {'document': read_configuration(path), 'cancelled': True,
                'warnings': ['Cancellation retained working-copy resources and source binding for recovery.']}
            raise error
        self.service.load_configuration = cancel
        self._load(path)
        self.assertEqual(app.generations_var.get(), '37')
        self.assertEqual(before, (app.prepared_id, set(app.selected_cell_scope_ids),
                                  app.parameter_configs, app.active_private_suite))
        self.assertIn('Private Suites', app.settings_status_var.get())
        self.assertFalse(self.showerror.called)

    def test_save_dialog_cancel_does_not_change_configuration_document(self):
        app = self._install()
        before = copy.deepcopy(app.configuration_document)
        with mock.patch.object(gui.filedialog, 'asksaveasfilename', return_value=''):
            app.save_configuration()
        self.assertEqual(app.configuration_document, before)
        self.assertFalse(self.showerror.called)

    def test_unavailable_parameter_is_preserved_after_load_and_save(self):
        app = self._install()
        path = self._save()
        document = read_configuration(path)
        document['settings']['parameters'].append({'parameter_id': 'removed-device',
            'evidence': {'design_library': 'fixture_lib', 'design_cell': 'TB', 'design_view': 'schematic',
                         'hierarchy_path': '/Removed/M1', 'name': 'w', 'master_library': 'PDK', 'master_cell': 'nmos'},
            'config': {'selected': True, 'minimum': 'unfinished', 'maximum': '9', 'unit': 'u'}})
        save_configuration(path, document)
        self._load(path)
        next_path = self._save('retained.json')
        next_doc = read_configuration(next_path)
        removed = [row for row in next_doc['settings']['parameters'] if row['parameter_id'] == 'removed-device']
        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0]['config']['minimum'], 'unfinished')
        self.assertFalse(self.showerror.called)

    def test_environment_draft_keeps_effective_profile_and_does_not_require_override(self):
        app = self._install()
        path = self._save()
        document = read_configuration(path)
        document['environment_draft']['fields']['alps_bin'] = '/old/server/no-alps'
        document['environment_draft']['pdk_profiles_text'] = '[unfinished'
        save_configuration(path, document)
        self._load(path)
        self.assertEqual(app.eda_vars['alps_bin'].get(), '/old/server/no-alps')
        self.assertNotEqual(app.eda_setup.get('effective', {}).get('alps_bin'), '/old/server/no-alps')
        self.assertFalse(app.eda_revalidation_required)
        self.assertEqual(str(app.start_button['state']), 'normal')
        self.assertFalse(self.showerror.called)

    def test_generic_only_policy_draft_is_restored(self):
        app = self._install()
        path = self._save()
        document = read_configuration(path)
        document['environment_draft']['policy'] = 'generic-only'
        save_configuration(path, document)
        app.eda_policy_var.set('auto')
        self._load(path)
        self.assertEqual(app.eda_policy_var.get(), 'generic-only')
        self.assertFalse(self.showerror.called)

    def test_apply_after_configuration_load_preserves_every_edited_setting(self):
        app = self._install()
        app.parameter_configs['/M0/w'].update(selected=True, minimum='1', maximum='3')
        app.objective_rows[0].mode_var.set('target')
        app.objective_rows[0]._mode_changed()
        app.objective_rows[0].value_var.set('234')
        app.add_constraint({'output_key': 'frequency', 'operator': '>=', 'limit': '51', 'corners': ['tt']})
        app.generations_var.set('12')
        path = self._save()
        self._load(path)
        app.parameter_configs['/M0/w']['minimum'] = '1.75'
        app.objective_rows[0].value_var.set('345')
        app.constraint_rows[0].value_var.set('65')
        app.generations_var.set('15')
        app.apply_selected_cells()
        self._wait(lambda: not app.setup_loading and app.pending_settings_state is None)
        self.assertEqual(app.parameter_configs['/M0/w']['minimum'], '1.75')
        self.assertTrue(app.parameter_configs['/M0/w']['selected'])
        self.assertEqual(app.objective_rows[0].value_var.get(), '345')
        self.assertEqual(app.constraint_rows[0].value_var.get(), '65')
        self.assertEqual(app.generations_var.get(), '15')
        self.assertEqual(app.selected_cell_scope_ids, {'cell-B'})
        self.assertFalse(app.cell_scope_dirty)
        self.assertFalse(self.showerror.called)
