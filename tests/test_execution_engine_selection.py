"""Exercise engine selection through real GUI state and run registration."""
import copy
import gc
import os
import unittest
from unittest import mock

from crescendo_engine import gui, settings_state
from tests import test_gui_optional_settings as fixtures


@unittest.skipUnless(os.environ.get("DISPLAY"), "An existing display is required")
class ExecutionEngineTkTests(unittest.TestCase):
    # Reuse fixture lifecycle without inheriting and repeating its test cases.
    setUp = fixtures.GuiOptionalSettingsTkTests.setUp
    _wait = fixtures.GuiOptionalSettingsTkTests._wait
    _prepared = fixtures.GuiOptionalSettingsTkTests._prepared
    _edit_draft = fixtures.GuiOptionalSettingsTkTests._edit_draft
    _save = fixtures.GuiOptionalSettingsTkTests._save
    _load = fixtures.GuiOptionalSettingsTkTests._load

    def _close(self):
        fixtures.GuiOptionalSettingsTkTests._close(self)
        # Release GUI cycles on the owning Tk thread before later worker tests
        # trigger automatic collection in a different thread.
        self.app = None
        gc.collect()

    def test_new_engine_roundtrips_drafts_through_automatic_load_and_apply(self):
        self._prepared()
        self._edit_draft()
        self.app.execution_backend_var.set("private_netlist")
        before = copy.deepcopy(self.app._snapshot_ui_state()[0])
        inspected = len(self.service.inspection_calls)
        applied = len(self.service.apply_calls)
        path, saved = self._save("engine.json")
        self.assertEqual(saved["settings"]["optimization"]["execution_backend"], "private_netlist")
        self.app.execution_backend_var.set("auto")
        self._load(path)
        self.assertEqual(self.app._snapshot_ui_state()[0], before)
        self.assertEqual(len(self.service.inspection_calls), inspected + 1)
        self.assertEqual(len(self.service.apply_calls), applied + 1)
        self.service.select_setup.assert_not_called()

    def test_explicit_engine_is_captured_by_actual_start_registration(self):
        self._prepared()
        self.app.execution_backend_var.set("private_netlist")
        self.app.job_command_var.set("batch-submit -q fixture")
        self.app.execution_selection_panel.set_setup(self.app.prepared["setup"])
        self.app.generations_var.set("2")
        prepared_calls = len(self.service.prepare_calls)
        with mock.patch.object(self.app, "_request_run_display_name", return_value="engine fixture"), \
                mock.patch.object(self.app, "_poll_progress", return_value=None):
            self.app.start_run()
            self._wait(lambda: self.service.start.called)
            self._wait(lambda: not self.app.start_request_pending)
        spec = self.service.start.call_args[0][1]
        self.assertEqual(spec["execution_backend"], "private_netlist")
        self.assertEqual(spec["generations"], 2)
        self.assertEqual(self.service.validation_calls, [])
        self.assertEqual(len(self.service.prepare_calls), prepared_calls)
        self.assertEqual(self.app.live_spec["execution_backend"], "private_netlist")

    def test_legacy_restore_and_engine_switch_do_not_change_job_overrides(self):
        self.app._restore_job_settings({"execution_backend": "persistent_maestro",
            "maestro_job_overrides": {"command": 'batch-submit -R "cpu=6;mem=30000"', "max_jobs": 64}})
        self.assertEqual(self.app.execution_backend_var.get(), "auto")
        original = self.app._collect_job_settings()
        self.app.execution_backend_var.set("private_netlist")
        self.assertEqual(self.app._collect_job_settings(), original)
        self.app.execution_selection_panel.set_setup({"tests": [{"name": "VCO"}]})
        spec = self.app._build_run_settings()
        restored = self.app._checkpoint_design_config({"spec": spec})["optimization"]["run"]
        self.assertEqual(restored["execution_backend"], "private_netlist")
        self.app._restore_job_settings(restored)
        self.assertEqual(self.app.execution_backend_var.get(), "private_netlist")
        self.assertEqual(self.app._collect_job_settings(), original)

    def test_saved_inherited_job_fields_keep_inherited_flags_on_load(self):
        self._prepared()
        self.app.job_command_var.set("source command")
        self.app.job_max_var.set("7")
        self.app.job_command_override_var.set(False)
        self.app.job_max_override_var.set(False)
        path, saved = self._save("inherited-job.json")
        self.assertFalse(saved["settings"]["optimization"]["job_command_override"])
        self.assertFalse(saved["settings"]["optimization"]["job_max_override"])
        self.app.job_command_var.set("manual command")
        self.app.job_max_var.set("12")
        self._load(path)
        self.assertFalse(self.app.job_command_override_var.get())
        self.assertFalse(self.app.job_max_override_var.get())

    def test_unknown_saved_engine_skips_without_corrupting_current_engine(self):
        raw, context = self.app._snapshot_ui_state()
        raw["optimization"]["execution_backend"] = "unknown_engine"
        state = settings_state.build_settings_state(raw)
        result = settings_state.match_settings_state(state, context)
        self.assertNotIn("execution_backend", result["apply"]["optimization"])
        self.assertTrue(any("execution_backend" in warning for warning in result["warnings"]))
