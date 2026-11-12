"""Small real-Tk regression for preserving drafts across EDA setting updates."""
import copy
import os
import threading
import unittest
from unittest import mock

from crescendo_engine import gui
from tests import test_gui_optional_settings as fixture


@unittest.skipUnless(os.environ.get("DISPLAY"), "Requires an existing DISPLAY")
class GuiEdaEditRetentionTkTests(unittest.TestCase):
    setUp = fixture.GuiOptionalSettingsTkTests.setUp
    _close = fixture.GuiOptionalSettingsTkTests._close
    _wait = fixture.GuiOptionalSettingsTkTests._wait
    _prepared = fixture.GuiOptionalSettingsTkTests._prepared
    def _edit_draft(self):
        fixture.GuiOptionalSettingsTkTests._edit_draft(self)
        # The fixture sets dictionaries directly; a real checkbox change also
        # refreshes derived algorithm defaults for the selected dimension.
        self.app._refresh_parameter_selection_display(refresh_items=False)

    def _reply(self, body):
        payload = self.service.eda_setup()
        payload.update(prepared_invalidated=True, mode=body["mode"], manual=body.get("manual") or {})
        return payload

    def test_save_keeps_latest_edits_and_reapply_restores_them_before_start(self):
        # Exercise manual retry separately from the default automatic chain.
        self.app._begin_eda_revalidation = mock.Mock()
        self._prepared()
        self._edit_draft()
        self.app.add_objective({"output_key": "frequency", "direction": "minimize", "corners": ["tt"]})
        self.app.algorithm_parameter_vars[("nsga2", "population")].set("23")
        self.app.algorithm_parameter_manual.add(("nsga2", "population"))
        self.assertEqual(self.app.algorithm_var.get(), "nsga2")
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def update(body):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("Fixture save was not released")
            return self._reply(body)
        self.service.update_eda_setup = mock.Mock(side_effect=update)
        self.app.eda_vars["spectre_model_dir"].set("/manual/models/spectre")
        self.app.save_eda_button.invoke()
        self.assertTrue(entered.wait(10))
        self.app.parameter_configs["/M0/w"]["minimum"] = "edited while saving"
        self.app.objective_rows[0].value_var.set("target while saving")
        expected = copy.deepcopy(self.app._snapshot_ui_state()[0])
        release.set()
        self._wait(lambda: str(self.app.save_eda_button.cget("state")) == "normal")
        actual = self.app._snapshot_ui_state()[0]
        for key in ("setup", "parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(actual[key], expected[key], key)
        self.assertIsNone(self.app.prepared_id)
        self.assertEqual(self.app.algorithm_var.get(), "nsga2")
        self.assertIsNone(self.app.cell_inspection_id)
        self.assertIsNotNone(self.app.pending_settings_state)
        self.app._setup_path_changed()
        for name in ("start_button", "validate_button", "apply_cells_button"):
            self.assertEqual(str(getattr(self.app, name).cget("state")), "disabled", name)
        self.app.start_run()
        self.service.start.assert_not_called()
        with self.assertRaisesRegex(ValueError, "EDA settings|revalidat"):
            self.app._require_prepared_selection()
        # Edits after Save must replace the earlier deferred draft at Load.
        self.app.parameter_configs["/M0/w"]["maximum"] = "edited after saving"
        self.app.constraint_rows[0].value_var.set("limit after saving")
        expected = copy.deepcopy(self.app._snapshot_ui_state()[0])
        self.service.inspection_calls[:] = []
        self.service.apply_calls[:] = []
        self.app.load_setup_button.invoke()
        self.assertIsNone(self.app.eda_auto_revalidation)
        self._wait(lambda: self.app.cell_inspection_id == "fixture-inspection" and
                   self.app.selected_cell_scope_ids == {"cell-B"})
        self.assertIsNone(self.app.prepared_id)
        matching, matched = threading.Event(), threading.Event()
        self.releases.append(matched)
        original_match = gui.match_settings_state
        def match(document, context):
            matching.set()
            if not matched.wait(3):
                raise RuntimeError("Fixture matching was not released")
            return original_match(document, context)
        with mock.patch.object(gui, "match_settings_state", side_effect=match):
            self.app.apply_cells_button.invoke()
            self._wait(matching.is_set)
            self.assertIsNone(self.app.prepared_id)
            self.assertEqual(str(self.app.start_button.cget("state")), "disabled")
            self.app.start_run()
            self.service.start.assert_not_called()
            # The visible old form remains editable while the new catalogue is
            # matched. A late edit must win over the earlier deferred draft.
            self.app.parameter_configs["/M0/w"]["minimum"] = "edited during final match"
            self.app.objective_rows[0].value_var.set("new target during final match")
            expected = copy.deepcopy(self.app._snapshot_ui_state()[0])
            matched.set()
            self._wait(lambda: self.app.parameter_configs == expected["parameter_configs"] and
                       self.app.pending_settings_state is None)
        actual = self.app._snapshot_ui_state()[0]
        for key in ("parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(actual[key], expected[key], key)
        self.assertEqual(len(self.service.inspection_calls), 1)
        self.assertEqual(self.service.apply_calls, [("fixture-inspection", ["cell-B"])])
        self.app._require_prepared_selection()
        # A newly unavailable corner can leave only one matched objective.
        # After the deferred phase, policy must use the actual restored rows.
        raw, context = self.app._snapshot_ui_state()
        document = gui.build_settings_state(raw, context["parameter_by_id"], context["cell_scopes"], context["outputs"])
        document["objectives"][1]["corners"] = ["removed_corner"]
        result = gui.match_settings_state(document, context)
        self.assertEqual(len(result["apply"]["objectives"]), 1)
        self.app.pending_settings_state = document
        self.app.eda_revalidation_required = True
        self.app.eda_edit_draft_path = self.setup_path
        self.app._apply_ui_settings(document, result)
        self.assertEqual(len(self.app.objective_rows), 1)
        self.assertNotEqual(self.app.algorithm_var.get(), "nsga2")
        self.assertEqual(str(self.app.algorithm_combo.cget("state")), "readonly")
        self.assertEqual(self.app.algorithm_parameter_vars[("nsga2", "population")].get(), "23")

    def test_initial_override_without_loaded_setup_does_not_create_an_empty_draft_gate(self):
        self.app.setup_path_var.set("")
        self.app.generations_var.set("19")
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        self.app.save_eda_button.invoke()
        self._wait(lambda: self.service.update_eda_setup.called and
                   str(self.app.save_eda_button.cget("state")) == "normal")
        self.assertFalse(self.app.eda_revalidation_required)
        self.assertIsNone(self.app.eda_edit_draft_path)
        self.assertEqual(self.app.generations_var.get(), "19")
        self.app.setup_path_var.set(self.setup_path)
        self.app.load_setup_button.invoke()
        self._wait(lambda: self.app.cell_inspection_id == "fixture-inspection")
        self.app._toggle_cell_scope("cell-B")
        self.app.apply_cells_button.invoke()
        self._wait(lambda: self.app.prepared_id == "prepared-fixture")
        self.assertFalse(self.app.eda_revalidation_required)
        self.app._require_prepared_selection()

    def test_override_automatically_reinspects_and_applies_original_cells_with_all_edits(self):
        self._prepared()
        self._edit_draft()
        self.app.add_objective({"output_key": "frequency", "direction": "minimize", "corners": ["tt"]})
        self.app.algorithm_parameter_vars[("nsga2", "population")].set("23")
        self.app.algorithm_parameter_manual.add(("nsga2", "population"))
        self.app.show_page("EDA Settings")
        expected = copy.deepcopy(self.app._snapshot_ui_state()[0])
        self.service.inspection_calls[:] = []
        self.service.apply_calls[:] = []
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        self.app.save_eda_button.invoke()
        self._wait(lambda: self.service.apply_calls and self.app.eda_auto_revalidation is None and not self.app.eda_revalidation_required)
        actual = self.app._snapshot_ui_state()[0]
        for key in ("parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(actual[key], expected[key], key)
        self.assertEqual(len(self.service.inspection_calls), 1)
        self.assertEqual(self.service.inspection_calls[0][1]["initialize_validation"] if "initialize_validation" in self.service.inspection_calls[0][1] else False, False)
        self.assertEqual(self.service.apply_calls, [("fixture-inspection", ["cell-B"])])
        self.assertEqual(self.app.current_page, "EDA Settings")
        self.app._require_prepared_selection()
        self.service.start.assert_not_called()
        self.assertEqual(self.service.validation_calls, [])

    def test_auto_override_prepare_failure_preserves_visible_draft_and_revokes_run_authority(self):
        self._prepared()
        self._edit_draft()
        expected = copy.deepcopy(self.app._snapshot_ui_state()[0])
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        self.service.prepare_cells = mock.Mock(side_effect=ValueError("fixture unreadable after override"))
        self.app.save_eda_button.invoke()
        self._wait(lambda: self.service.prepare_cells.called and self.app.eda_auto_revalidation is None)
        for key in ("parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(self.app._snapshot_ui_state()[0][key], expected[key], key)
        self.assertTrue(self.app.eda_revalidation_required)
        self.assertIsNone(self.app.prepared_id)
        self.assertIsNotNone(self.app.pending_settings_state)
        with self.assertRaises(ValueError):
            self.app._require_prepared_selection()
        self.service.start.assert_not_called()
        self.assertIn("自动重检未完成", self.app.eda_status_var.get())

    def test_final_match_failure_does_not_replace_visible_inputs_with_defaults(self):
        self._prepared()
        self._edit_draft()
        expected = copy.deepcopy(self.app._snapshot_ui_state()[0])
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        original_match = gui.match_settings_state
        def match(document, context):
            if self.app.setup_loading:
                raise ValueError("fixture final identity match unavailable")
            return original_match(document, context)
        with mock.patch.object(gui, "match_settings_state", side_effect=match):
            self.app.save_eda_button.invoke()
            self._wait(lambda: self.showerror.called and self.app.eda_auto_revalidation is None)
        for key in ("parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(self.app._snapshot_ui_state()[0][key], expected[key], key)
        self.assertIsNone(self.app.prepared_id)
        self.assertTrue(self.app.eda_revalidation_required)
        self.assertIsNotNone(self.app.pending_settings_state)
        self.service.start.assert_not_called()

    def test_snapshot_save_time_change_is_not_a_new_user_edit_or_an_endless_retry(self):
        self._prepared()
        self._edit_draft()
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        original_build = gui.build_settings_state
        calls = []
        def build(*args, **kwargs):
            document = original_build(*args, **kwargs)
            calls.append(1)
            document["saved_at"] = "different-clock-second-" + str(len(calls))
            return document
        with mock.patch.object(gui, "build_settings_state", side_effect=build):
            self.app.save_eda_button.invoke()
            self._wait(lambda: self.service.update_eda_setup.called and self.app.eda_auto_revalidation is None and not self.app.eda_revalidation_required)
        self.assertLess(len(calls), 15)
        self.assertEqual(self.app.parameter_configs["/M0/w"]["minimum"], "unfinished min")
        self.service.start.assert_not_called()

    def test_failed_save_keeps_qualification_and_draft(self):
        self._prepared()
        self._edit_draft()
        before = copy.deepcopy(self.app._snapshot_ui_state()[0])
        original_id, inspection_id = self.app.prepared_id, self.app.cell_inspection_id
        self.service.update_eda_setup = mock.Mock(side_effect=ValueError("fixture rejected model path"))
        self.app.save_eda_button.invoke()
        self._wait(lambda: self.showerror.called and str(self.app.save_eda_button.cget("state")) == "normal")
        self.assertEqual(self.app._snapshot_ui_state()[0], before)
        self.assertEqual(self.app.prepared_id, original_id)
        self.assertEqual(self.app.cell_inspection_id, inspection_id)
        self.app._require_prepared_selection()

    def _change_selected_module_before_refresh(self):
        self._prepared()
        self._edit_draft()
        self.app.selected_cell_scope_ids = {"cell-root"}
        self.app._update_cell_selection_state()
        self.assertTrue(self.app.cell_scope_dirty)
        self.changed_module_draft = copy.deepcopy(self.app._snapshot_ui_state()[0])
        self.service.apply_calls[:] = []

    def _assert_changed_module_refresh_completed(self):
        self._wait(lambda: bool(self.service.apply_calls) and not self.app.setup_loading)
        self.assertFalse(self.app.eda_revalidation_required)
        self.assertIsNone(self.app.pending_settings_state)
        self.assertIsNone(self.app.eda_auto_revalidation)
        self.assertEqual(self.app.selected_cell_scope_ids, {"cell-root"})
        self.assertEqual(self.app.applied_cell_scope_ids, {"cell-root"})
        self.assertFalse(self.app.cell_scope_dirty)
        self.assertEqual(self.app.parameter_configs["/M0/w"]["minimum"], "unfinished min")
        actual = self.app._snapshot_ui_state()[0]
        for key in ("setup", "parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(actual[key], self.changed_module_draft[key], key)
        if hasattr(self, "expected_refresh_page"):
            self.assertEqual(self.app.current_page, self.expected_refresh_page)
        self.app._require_prepared_selection()
        self.service.start.assert_not_called()
        self.assertEqual(self.service.validation_calls, [])

    def test_override_after_module_selection_change_completes_fresh_preparation_gate(self):
        self._change_selected_module_before_refresh()
        self.app.show_page("EDA Settings")
        self.expected_refresh_page = "EDA Settings"
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        self.app.save_eda_button.invoke()
        self._assert_changed_module_refresh_completed()

    def test_manual_reload_after_module_selection_change_completes_fresh_preparation_gate(self):
        self._change_selected_module_before_refresh()
        self.app.load_setup_button.invoke()
        self._assert_changed_module_refresh_completed()

    def test_incomplete_prepared_commit_restores_scope_and_draft_but_never_run_authority(self):
        self._change_selected_module_before_refresh()
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        def incomplete_commit(*_args, **_kwargs):
            self.app.prepared_id = "incomplete-commit"
            self.app.eda_revalidation_required = False
            self.app.pending_settings_state = None
            raise ValueError("fixture prepared UI commit did not complete")
        with mock.patch.object(self.app, "_set_prepared", side_effect=incomplete_commit):
            self.app.save_eda_button.invoke()
            self._wait(lambda: self.showerror.called and self.app.eda_auto_revalidation is None)
        self.assertIsNone(self.app.prepared_id)
        self.assertTrue(self.app.eda_revalidation_required)
        self.assertIsNotNone(self.app.pending_settings_state)
        self.assertEqual(self.app.applied_cell_scope_ids, {"cell-B"})
        self.assertEqual(self.app.selected_cell_scope_ids, {"cell-root"})
        self.assertTrue(self.app.cell_scope_dirty)
        self.assertEqual(self.app.parameter_configs["/M0/w"]["minimum"], "unfinished min")
        with self.assertRaises(ValueError):
            self.app._require_prepared_selection()
        self.assertEqual(str(self.app.start_button.cget("state")), "disabled")
        self.service.start.assert_not_called()

    def test_diagnostic_spec_preserves_prepared_gate_reason_and_cached_state(self):
        self._prepared()
        self.app._stash_eda_edit_draft()
        self.app.eda_revalidation_required = True
        spec = self.app._collect_environment_test_spec()
        state = spec["ui_prepared_state"]
        self.assertFalse(spec["prepared_selection_valid"])
        self.assertEqual(state["prepared_id"], self.app.prepared_id)
        self.assertEqual(state["inspection_id"], self.app.cell_inspection_id)
        self.assertTrue(state["eda_settings_requires_revalidation"])
        self.assertTrue(state["pending_restore_same_setup"])
        self.assertTrue(state["prepared_selection_matches"])
        self.assertFalse(state["cell_scope_dirty"])
        self.assertIn("EDA settings changed", state["error"])
        self.assertTrue(any(state["error"] in row for row in spec["ui_input_warnings"]))

    def test_manual_load_refreshes_inherited_settings_and_outputs_but_keeps_explicit_overrides(self):
        self._prepared()
        self._edit_draft()
        self.app.job_command_var.set("old inherited command")
        self.app.job_command_override_var.set(False)
        self.app.data_root_override_var.set(False)
        self.app.data_root_var.set("/old/inherited/data")
        timeout_keys = sorted(self.app.job_timeout_vars)
        manual_key = timeout_keys[0]
        self.app.job_timeout_override_vars[manual_key].set(True)
        self.app.job_timeout_vars[manual_key].set("891")
        properties = {"distributionmethod": "Command", "maxjobs": "7", "jobsubmitcommand": "batch-submit -q refreshed"}
        properties.update({gui.JOB_TIMEOUT_PROPERTIES[key]: "731" for key in timeout_keys})
        policy = {"source": "saved_setup_native", "available": True,
                  "global": {"available": True, "properties": properties}, "tests": []}
        options = {"source": "saved_setup_native", "effective": {
            "simulation_root": os.path.join(self.directory.name, "new-saved-simulation")}}
        original_inspect, original_prepare = self.service.inspect_cells, self.service.prepare_cells
        def inspect(path, **kwargs):
            token, preview = original_inspect(path, **kwargs)
            preview["job_policy"] = copy.deepcopy(policy)
            preview["setup"]["save_options"] = copy.deepcopy(options)
            return token, preview
        def prepare(inspection_id, scope_ids):
            token, prepared = original_prepare(inspection_id, scope_ids)
            prepared["job_policy"] = copy.deepcopy(policy)
            prepared["setup"]["save_options"] = copy.deepcopy(options)
            prepared["outputs"].append({"key": "new_saved_output"})
            return token, prepared
        self.service.inspect_cells = inspect
        self.service.prepare_cells = prepare
        expected_parameters = copy.deepcopy(self.app.parameter_configs)
        self.service.apply_calls[:] = []
        self.app.load_setup_button.invoke()
        self._wait(lambda: bool(self.service.apply_calls) and not self.app.setup_loading and
                   self.app.eda_auto_revalidation is None)
        self.assertEqual(self.app.parameter_configs, expected_parameters)
        self.assertEqual(self.app.job_command_var.get(), "batch-submit -q refreshed")
        self.assertEqual(self.app.job_max_var.get(), "19")
        self.assertTrue(self.app.job_max_override_var.get())
        for key in timeout_keys:
            self.assertEqual(self.app.job_timeout_vars[key].get(), "891" if key == manual_key else "731")
        self.assertEqual(self.app.data_root_var.get(), options["effective"]["simulation_root"])
        self.assertIn("new_saved_output", self.app._output_names())
        self.assertEqual(self.service.apply_calls, [("fixture-inspection", ["cell-B"])])
        self.assertFalse(self.app.eda_revalidation_required)
        self.service.start.assert_not_called()
        self.assertEqual(self.service.validation_calls, [])
        properties["jobsubmitcommand"] = "batch-submit -q changed-again"
        self.service.apply_calls[:] = []
        self.app.load_setup_button.invoke()
        self._wait(lambda: bool(self.service.apply_calls) and not self.app.setup_loading and
                   self.app.eda_auto_revalidation is None)
        self.assertEqual(self.app.job_command_var.get(), "batch-submit -q changed-again")

    def test_restored_inherited_max_refreshes_and_explicit_command_survives(self):
        self._prepared()
        app = self.app
        def policy(command, maximum):
            return {"available": True, "global": {"available": True, "properties": {
                "distributionmethod": "Command", "jobsubmitcommand": command, "maxjobs": str(maximum)}}, "tests": []}
        app._render_job_policy(policy("source-one", 7), reset=True)
        app.job_command_var.set("user-command")
        raw, _context = app._snapshot_ui_state()
        optimization = copy.deepcopy(raw["optimization"])
        self.assertTrue(optimization["job_command_override"])
        self.assertFalse(optimization["job_max_override"])
        app._render_job_policy(policy("source-two", 8))
        app._apply_ui_settings(raw, {"apply": {"optimization": optimization}, "warnings": []})
        app._render_job_policy(policy("source-three", 9))
        self.assertEqual(app.job_command_var.get(), "user-command")
        self.assertEqual(app.job_max_var.get(), "9")
        self.assertTrue(app.job_command_override_var.get())
        self.assertFalse(app.job_max_override_var.get())

    def test_manual_reload_does_not_auto_apply_a_missing_previously_selected_module(self):
        self._prepared()
        self._edit_draft()
        expected = copy.deepcopy(self.app.parameter_configs)
        self.service.scopes = mock.Mock(return_value=[self.service.scopes()[0]])
        self.service.apply_calls[:] = []
        self.app.load_setup_button.invoke()
        self._wait(lambda: self.app.eda_auto_revalidation is None and not self.app.setup_loading and self.app.eda_revalidation_required)
        self.assertEqual(self.service.apply_calls, [])
        self.assertEqual(self.app.parameter_configs, expected)
        self.assertIsNone(self.app.prepared_id)
        self.assertIn("没有可证明的匹配", self.app.eda_status_var.get())
        with self.assertRaises(ValueError):
            self.app._require_prepared_selection()

    def test_unconfirmed_reload_root_shows_previous_path_as_hint_and_keeps_manual_choice(self):
        self._prepared()
        self._edit_draft()
        self.service.config["_maestro_launch_context"] = {"saved_setup_refresh": {
            "source": "explicit_saved_setup_reload", "previous_simulation_root_hint": "/previous/native/results"}}
        self.app.data_root_override_var.set(False)
        self.app._render_save_options({})
        self.assertEqual(self.app.data_root_var.get(), "")
        self.assertIn("仅供核对", self.app.save_options_status_var.get())
        self.assertIn("/previous/native/results", self.app.save_options_status_var.get())
        self.assertIn("不会偷偷改用其他目录", self.app.save_options_status_var.get())
        self.app.data_root_override_var.set(True)
        self.app.data_root_var.set("/chosen/native/results")
        self.app._render_save_options({})
        self.assertEqual(self.app.data_root_var.get(), "/chosen/native/results")
        self.assertEqual(self.app.parameter_configs["/M0/w"]["minimum"], "unfinished min")

    def test_reset_keeps_inputs_and_other_setup_does_not_receive_deferred_draft(self):
        self.app._begin_eda_revalidation = mock.Mock()
        self._prepared()
        self._edit_draft()
        before = copy.deepcopy(self.app._snapshot_ui_state()[0])
        self.service.update_eda_setup = mock.Mock(side_effect=self._reply)
        self.app.reset_eda_setup()
        self._wait(lambda: self.service.update_eda_setup.called and self.app.prepared_id is None)
        actual = self.app._snapshot_ui_state()[0]
        for key in ("parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(actual[key], before[key], key)
        other = os.path.join(self.directory.name, "other", "maestro.sdb")
        os.makedirs(os.path.dirname(other))
        with open(other, "w", encoding="utf-8") as stream:
            stream.write("<setupdb/>")
        self.app._apply_setup_selection({"setup_path": other, "context": None})
        self.assertIsNone(self.app.pending_settings_state)
        self.assertFalse(getattr(self.app, "eda_revalidation_required", False))


if __name__ == "__main__":
    unittest.main()
