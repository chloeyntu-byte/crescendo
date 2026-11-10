import os
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.unified_configuration import read_configuration, save_configuration, configuration_revision
from tests import test_gui_optional_settings as fixture
from tests import test_unified_configuration_gui as base


@unittest.skipUnless(os.environ.get('DISPLAY'), 'real Tk required')
class ConfigurationGuiCasTests(unittest.TestCase):
    setUp = fixture.GuiOptionalSettingsTkTests.setUp
    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait
    _prepared = fixture.GuiOptionalSettingsTkTests._prepared
    _install = base.UnifiedConfigurationGuiTests._install
    _save = base.UnifiedConfigurationGuiTests._save
    _load = base.UnifiedConfigurationGuiTests._load

    def test_loaded_file_external_change_is_not_overwritten(self):
        app = self._install()
        path = self._save()
        self._load(path)
        original_revision = app.configuration_revisions[path]
        other = read_configuration(path)
        other['saved_at'] = 'external-change'
        save_configuration(path, other)
        changed = configuration_revision(path)
        self.assertNotEqual(changed, original_revision)
        with mock.patch.object(gui.filedialog, 'asksaveasfilename', return_value=path):
            app.save_configuration()
        self._wait(lambda: str(app.save_config_button['state']) == 'normal')
        self.assertEqual(configuration_revision(path), changed)
        self.assertIn('changed since it was read', app.settings_match_detail)
        self.assertIn('未确认', app.settings_status_var.get())
        self.assertEqual(app.configuration_revisions[path], original_revision)

    def test_start_autosave_conflict_keeps_external_last_and_still_starts(self):
        from crescendo_engine.settings_state import last_settings_path
        app = self._install()
        self._save()
        path = os.path.abspath(last_settings_path(self.service.config, app._active_context()))
        previous = app.configuration_revisions[path]
        other = read_configuration(path)
        other['saved_at'] = 'external-last'
        save_configuration(path, other)
        changed = configuration_revision(path)
        self.assertNotEqual(previous, changed)
        app.prepared['setup']['test_corner_masks'] = {'VCO': {'known': True, 'disabled_corners': []}}
        app._reset_metric_rows()
        app.add_objective({'output_key': 'frequency', 'corners': ['tt']})
        app.parameter_configs['/M1/w']['selected'] = False
        app.job_command_override_var.set(True)
        app.job_command_var.set('/bin/true')
        with mock.patch.object(app, '_poll_progress'), mock.patch.object(app, '_request_run_display_name', return_value='CAS fixture'):
            app.start_run()
            self._wait(lambda: self.service.start.call_count == 1 and not app.start_request_pending)
        self.assertEqual(configuration_revision(path), changed)
        self.assertIn('Last settings save not confirmed', app.settings_status_var.get())
        self.assertEqual(app.configuration_revisions[path], previous)

    def test_exit_suite_conflict_warns_and_still_closes(self):
        from crescendo_engine.settings_state import ConfigurationConflict
        app = self._install()
        app.active_private_suite = {'id': 'suite-fixture', 'source_path': self.setup_path}
        app.suite_configuration_revisions = {'suite-fixture': 'previous-revision'}
        self.service.save_private_suite_configuration = mock.Mock(side_effect=ConfigurationConflict('external update'))
        self.service.exit_optimizer = mock.Mock(return_value={'cleanup_proven': True})
        with mock.patch.object(gui.messagebox, 'showwarning') as warning, mock.patch.object(self.root, 'destroy') as destroy:
            app.request_exit(confirm=False)
            self._wait(lambda: destroy.called)
        self.service.save_private_suite_configuration.assert_called_once()
        self.assertEqual(self.service.save_private_suite_configuration.call_args[1]['expected_revision'], 'previous-revision')
        self.assertTrue(self.service.exit_optimizer.called)
        self.assertIn('external update', str(warning.call_args))

    def test_read_returns_raw_revision_of_same_document(self):
        self._install()
        path = self._save()
        item = read_configuration(path, return_result=True)
        self.assertEqual(item['revision'], configuration_revision(path))
        self.assertEqual(item['document'], read_configuration(path))
