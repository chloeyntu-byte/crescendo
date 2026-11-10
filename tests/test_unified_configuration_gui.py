"""Real Tk configuration round trips with fixture services; no native EDA."""
import copy
import json
import os
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.unified_configuration import (
    build_configuration, read_configuration, normalize_configuration,
    match_configuration, save_configuration,
)
from tests import test_gui_optional_settings as fixture


class UnifiedFixtureService(fixture.OptionalSettingsService):
    def __init__(self, *args):
        super(UnifiedFixtureService, self).__init__(*args)
        self.configuration_calls = []
        self.configuration_hook = None

    def load_configuration(self, document, path=None, selected_scope_ids=None, selected_test=None):
        self.configuration_calls.append((copy.deepcopy(document), path))
        normalized = normalize_configuration(document)
        canonical = normalized['document']
        inspection_id, inspection = self.inspect_cells(path or self.setup_path)
        source = self.config['_selected_setup_source']
        setup = dict(inspection['setup'], identity=source, designs=inspection['setup']['tests'], path=self.setup_path)
        context = {'setup': setup, 'cell_scopes': self.scopes()}
        initial = match_configuration(canonical, context)
        scopes = initial['apply']['selected_cell_scope_ids']
        result = {'document': canonical, 'settings': normalized['settings'],
                  'inspection_id': inspection_id, 'inspection': inspection,
                  'selected_scope_ids': scopes, 'warnings': normalized['warnings'],
                  'match': initial}
        if scopes:
            prepared_id, prepared = self.prepare_cells(inspection_id, scopes)
            prepared['cell_selection']['scope_ids'] = list(scopes)
            context.update(parameter_by_id={row['parameter_id']: row for row in prepared['parameters']},
                           outputs=prepared['outputs'], corners=['tt'])
            result.update(prepared_id=prepared_id, prepared=prepared,
                          match=match_configuration(canonical, context))
        if self.configuration_hook:
            self.configuration_hook(result)
        return result


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk requires DISPLAY')
class UnifiedConfigurationGuiTests(unittest.TestCase):
    setUp = fixture.GuiOptionalSettingsTkTests.setUp
    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait
    _prepared = fixture.GuiOptionalSettingsTkTests._prepared

    def _install(self):
        # Reuse the fixture's queues and all of the real UI; only native service
        # calls are replaced by an explicit deterministic service.
        service = UnifiedFixtureService(self.setup_path, self.directory.name)
        self.service = service
        self.app.service = service
        self._prepared()
        return self.app

    def _save(self, name='configuration.json'):
        path = os.path.join(self.directory.name, name)
        with mock.patch.object(gui.filedialog, 'asksaveasfilename', return_value=path):
            self.app.save_configuration()
        self._wait(lambda: os.path.isfile(path) and str(self.app.save_config_button['state']) == 'normal')
        return path

    def _load(self, path):
        with mock.patch.object(gui.filedialog, 'askopenfilename', return_value=path):
            self.app.load_configuration()
        self._wait(lambda: not self.app.configuration_loading and not self.app.setup_loading)

    def test_single_visible_pair_and_save_without_preparation(self):
        app = self._install()
        app._invalidate_prepared('fixture', preserve_edits=False)
        self.assertEqual(str(app.save_config_button['state']), 'normal')
        self.assertIs(app.save_config_button, app.save_state_button)
        self.assertIs(app.load_config_button, app.load_state_button)
        self.assertEqual(app.save_config_button['text'], 'Save Configuration...')
        path = self._save()
        self.assertEqual(read_configuration(path)['schema_version'], 'crescendo.configuration.v1')
        self.assertFalse(self.showerror.called)

    def test_one_click_restores_every_page_and_applies_without_retyping(self):
        app = self._install()
        app.parameter_configs['/M0/w'].update(selected=True, minimum='1.25', maximum='7.25', unit='u', scale='log')
        app.objective_rows[0].mode_var.set('target')
        app.objective_rows[0]._mode_changed()
        app.objective_rows[0].value_var.set('123')
        app.add_constraint({'output_key': 'frequency', 'operator': '>=', 'limit': '50', 'corners': ['tt']})
        app.generations_var.set('7')
        app.diagnostic_generations_var.set('3')
        app.diagnostic_total_var.set('12')
        app.timeout_var.set('345')
        app.job_max_override_var.set(True)
        app.job_max_var.set('3')
        app.execution_backend_var.set('private_netlist')
        app.simulator_var.set('alps')
        app.job_command_override_var.set(True)
        app.job_command_var.set('batch-submit fixture')
        for name, variable in app.job_timeout_vars.items():
            variable.set('17')
            app.job_timeout_override_vars[name].set(True)
        expected_commands = copy.deepcopy(app._execution_command_values())
        path = self._save()
        app.parameter_configs['/M0/w']['minimum'] = '999'
        app.generations_var.set('999')
        app.diagnostic_generations_var.set('2')
        app.diagnostic_total_var.set('6')
        app.timeout_var.set('999')
        app.simulator_var.set('spectre')
        app.job_max_var.set('99')
        app._reset_metric_rows()
        self._load(path)
        self.assertEqual(app.diagnostic_generations_var.get(), '3')
        self.assertEqual(app.diagnostic_total_var.get(), '12')
        self.assertEqual(app.generations_var.get(), '7')
        self.assertEqual(len(self.service.configuration_calls), 1)
        self.assertEqual(self.service.apply_calls[-1][1], ['cell-B'])
        self.assertEqual(app.selected_cell_scope_ids, {'cell-B'})
        self.assertEqual(app.applied_cell_scope_ids, {'cell-B'})
        self.assertFalse(app.cell_scope_dirty)
        self.assertEqual(app.parameter_configs['/M0/w']['minimum'], '1.25')
        self.assertTrue(app.parameter_configs['/M0/w']['selected'])
        self.assertEqual(app.objective_rows[0].value_var.get(), '123')
        self.assertEqual(app.constraint_rows[0].value_var.get(), '50')
        self.assertEqual(app.generations_var.get(), '7')
        self.assertEqual(app.timeout_var.get(), '345')
        self.assertEqual(app.job_max_var.get(), '3')
        self.assertTrue(app.job_max_override_var.get())
        self.assertEqual(app.job_command_var.get(), 'batch-submit fixture')
        self.assertEqual(app.simulator_var.get(), 'alps')
        self.assertEqual(app.execution_backend_var.get(), 'private_netlist')
        self.assertEqual(app._execution_command_values(), expected_commands)
        for name, variable in app.job_timeout_vars.items():
            self.assertEqual(variable.get(), '17')
            self.assertTrue(app.job_timeout_override_vars[name].get())
        self.assertEqual(str(app.start_button['state']), 'normal')
        self.assertFalse(self.showerror.called)

    def test_incomplete_invalid_draft_can_save_load_and_resave(self):
        app = self._install()
        app.generations_var.set('unfinished')
        app.parameter_configs['/M0/w'].update(selected=True, minimum='unfinished', maximum='')
        app.timeout_var.set('0')
        path = self._save()
        self._load(path)
        self.assertEqual(app.generations_var.get(), 'unfinished')
        self.assertEqual(app.parameter_configs['/M0/w']['minimum'], 'unfinished')
        self.assertEqual(app.timeout_var.get(), '0')
        self._save('draft-again.json')
        self.assertFalse(self.showerror.called)

    def test_missing_file_does_not_clear_or_block_current_settings(self):
        app = self._install()
        app.generations_var.set('12')
        prepared_id = app.prepared_id
        self._load(os.path.join(self.directory.name, 'missing.json'))
        self.assertEqual(app.generations_var.get(), '12')
        self.assertEqual(app.prepared_id, prepared_id)
        self.assertIn('保留', app.settings_status_var.get())
        self.assertEqual(str(app.load_config_button['state']), 'normal')
        self.assertFalse(self.showerror.called)

    def test_missing_saved_cell_keeps_draft_and_import_is_not_failure(self):
        app = self._install()
        path = self._save()
        document = read_configuration(path)
        for row in document['settings']['cells']:
            if row['selected']:
                row['evidence']['cell'] = 'deleted_cell'
        save_configuration(path, document)
        self._load(path)
        self.assertIsNotNone(app.configuration_document)
        self.assertFalse(app.selected_cell_scope_ids)
        self.assertIn('草稿', app.settings_status_var.get())
        self.assertFalse(self.showerror.called)

    def test_restore_last_uses_same_automatic_pipeline(self):
        app = self._install()
        app.generations_var.set('11')
        self._save()
        app.generations_var.set('2')
        app.restore_last_ui_state()
        self._wait(lambda: not app.configuration_loading)
        self.assertEqual(app.generations_var.get(), '11')
        self.assertTrue(self.service.apply_calls)

    def test_json_editor_draft_does_not_block_save(self):
        app = self._install()
        gui.OptimizerGui._replace_text_widget(app.eda_profiles, '[unfinished')
        app.eda_vars['simulator_threads'].set('unfinished')
        path = self._save()
        document = read_configuration(path)
        self.assertEqual(document['environment_draft']['pdk_profiles_text'], '[unfinished')
        self._load(path)
        self.assertEqual(app.eda_profiles.get('1.0', 'end').strip(), '[unfinished')
        self.assertEqual(app.eda_vars['simulator_threads'].get(), 'unfinished')
        self.assertFalse(self.showerror.called)

    def test_preparation_warning_is_not_qualified_as_ready(self):
        app = self._install()
        path = self._save()
        def failed(result):
            result.pop('prepared', None)
            result.pop('prepared_id', None)
            result['preparation_error'] = 'fixture native binding failed; proof retained'
        self.service.configuration_hook = failed
        self._load(path)
        self.assertFalse(app.prepared_id)
        self.assertIsNotNone(app.pending_settings_state)
        self.assertIn('草稿', app.settings_status_var.get())
        self.assertIn('native binding failed', app.settings_match_detail)
        self.assertFalse(self.showerror.called)

    def test_start_autosave_keeps_unified_schema_environment_and_unmatched_drafts(self):
        app = self._install()
        # The initial blank row predates preparation and has no corner choices.
        # Build a valid objective from the prepared fixture before testing save.
        app.prepared['setup']['test_corner_masks'] = {'VCO': {'known': True, 'disabled_corners': []}}
        app._reset_metric_rows()
        app.add_objective({'output_key': 'frequency', 'corners': ['tt']})
        app.parameter_configs['/M1/w']['selected'] = False
        app.job_command_override_var.set(True)
        app.job_command_var.set('/bin/true')  # mocked service; no external command is submitted
        raw, context = app._snapshot_configuration()
        previous = build_configuration(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        previous['settings']['parameters'].append({'parameter_id': 'missing-device',
            'evidence': {'design_library': 'old-lib'}, 'config': {'selected': True, 'minimum': 'draft'}})
        app.configuration_document = previous
        app.eda_vars['alps_bin'].set('/missing-but-retained/alps')
        with mock.patch.object(app, '_poll_progress'), mock.patch.object(app, '_request_run_display_name', return_value='Unified autosave'):
            app.start_run()
            self._wait(lambda: self.service.start.call_count == 1 and not app.start_request_pending)
        from crescendo_engine.unified_configuration import read_last_configuration
        saved = read_last_configuration(self.service.config, app._active_context(), app._settings_context()['setup'])
        self.assertEqual(saved['schema_version'], 'crescendo.configuration.v1')
        self.assertEqual(saved['environment_overrides']['alps_bin'], '/missing-but-retained/alps')
        self.assertTrue(any(row['parameter_id'] == 'missing-device' for row in saved['settings']['parameters']))
        self.assertFalse(self.showerror.called)

    def test_late_save_callback_never_replaces_newly_loaded_configuration(self):
        app = self._install()
        raw, context = app._snapshot_configuration()
        imported = build_configuration(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        imported['settings']['parameters'].append({'parameter_id': 'new-unmatched',
            'evidence': {'design_library': 'external'}, 'config': {'selected': True, 'minimum': 'keep'}})
        for during_load in (False, True):
            with self.subTest(during_load=during_load):
                callbacks = []
                path = os.path.join(self.directory.name, 'late-%s.json' % during_load)
                app.configuration_loading = during_load
                with mock.patch.object(app.tasks, 'submit', side_effect=lambda *args: callbacks.append(args)), \
                        mock.patch.object(gui.filedialog, 'asksaveasfilename', return_value=path):
                    app.save_configuration()
                self.assertEqual(len(callbacks), 1)
                app.settings_load_epoch += 1
                app.prepared_epoch += 1
                app.configuration_loading = False
                app.configuration_document = copy.deepcopy(imported)
                app.settings_status_var.set('new configuration already loaded')
                function, saved, failed = callbacks[0]
                saved(function())
                self.assertEqual(app.configuration_document, imported)
                self.assertEqual(app.settings_status_var.get(), 'new configuration already loaded')
                self.assertTrue(os.path.isfile(path))
                self.assertEqual(str(app.save_config_button['state']), 'normal')

    def test_source_selection_failure_retains_existing_scope_consistency(self):
        app = self._install()
        path = self._save()
        before = (app.prepared_id, set(app.selected_cell_scope_ids), set(app.applied_cell_scope_ids), app.cell_scope_dirty)
        def source_failure(result):
            for name in ('inspection', 'inspection_id', 'prepared', 'prepared_id', 'selection'):
                result.pop(name, None)
            result['selected_cell_scope_ids'] = []
            result['match'] = {'apply': {'setup': {'matches': False}, 'optimization': {}},
                               'summary': 'draft retained', 'warnings': ['source unavailable']}
            result['preparation_error'] = 'source selection failed; current binding retained'
        self.service.configuration_hook = source_failure
        self._load(path)
        after = (app.prepared_id, set(app.selected_cell_scope_ids), set(app.applied_cell_scope_ids), app.cell_scope_dirty)
        self.assertEqual(before, after)
        self.assertEqual(app.selected_cell_scope_ids, {'cell-B'})
        self.assertFalse(self.showerror.called)
