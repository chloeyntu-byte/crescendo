"""Scoped real-Tk checks using fixture services only, never native EDA."""
import copy
import gc
import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_state import RunUiState
from tests.test_gui_light_loading import LightweightUiService
from tests.configuration_service_fixture import load_fixture_configuration


class OptionalSettingsService(LightweightUiService):
    def __init__(self, setup_path, project):
        super(OptionalSettingsService, self).__init__(setup_path, parameter_count=2)
        self.config = {"cadence": {"project_dir": project},
                       "_selected_setup_source": {"library": "fixture_lib", "cell": "VCO", "view": "maestro"}}
        self.validation_entered = threading.Event()
        self.validation_release = threading.Event()
        self.validation_release.set()
        self.validation_calls = []
        self.inspection_calls = []
        self.apply_calls = []
        self.configuration_calls = []
        self.parameter_master_library = None
        self.validation_status = {"state": "completed", "message": "fixture check complete", "results": []}
        self.start = mock.Mock(return_value={"run_id": "fixture-run", "run_root": project + "/fixture-run"})
        self.cancel_range_validations = mock.Mock(return_value={"cancelled": True})
        self.select_setup = mock.Mock(side_effect=AssertionError("State loading must not switch setup"))
        self.load_design_config = mock.Mock(side_effect=AssertionError("State loading must not load legacy config"))

    def prepared_fixture(self):
        identifier, prepared = super(OptionalSettingsService, self).prepare(self.setup_path)
        for row in prepared["parameters"]:
            row.update(design_library="fixture_lib", design_cell="TB", design_view="schematic")
            if self.parameter_master_library:
                row["master_library"] = self.parameter_master_library
        prepared["outputs"] = [{"key": "frequency", "name": "frequency", "test": "VCO"}]
        prepared["setup"]["corners"] = [{"name": "tt", "enabled": True}]
        prepared["setup"]["test_corner_masks"] = {'VCO': {'known': True, 'disabled_corners': []}}
        prepared["cell_selection"] = {"scopes": self.scopes()}
        return identifier, prepared

    def scopes(self):
        design = {"library": "fixture_lib", "cell": "TB", "view": "schematic"}
        return [{"scope_id": "cell-root", "hierarchy_path": "/", "design": design,
                 "library": "fixture_lib", "cell": "TB", "view": "schematic", "selectable": True},
                {"scope_id": "cell-B", "hierarchy_path": "/B", "design": design,
                 "library": "fixture_lib", "cell": "B", "view": "schematic", "selectable": True}]

    def fixture_tests(self):
        return [{"name": "VCO", "library": "fixture_lib", "cell": "TB", "view": "schematic"}]

    def inspect_cells(self, path, **options):
        self.inspection_calls.append((path, options))
        return "fixture-inspection", {"setup_path": self.setup_path,
            "setup": {"backend": "maestro", "tests": [{"name": "VCO", "library": "fixture_lib", "cell": "TB", "view": "schematic"}]},
            "cell_scopes": self.scopes()}

    def prepare_cells(self, inspection_id, scope_ids):
        self.apply_calls.append((inspection_id, list(scope_ids)))
        return self.prepared_fixture()

    def load_configuration(self, document, path=None, selected_scope_ids=None, selected_test=None):
        self.configuration_calls.append(copy.deepcopy(document))
        return load_fixture_configuration(self, document, path=path)

    def validate_ranges(self, prepared_id, endpoints):
        self.validation_calls.append(copy.deepcopy(endpoints))
        self.validation_entered.set()
        if not self.validation_release.wait(5):
            raise RuntimeError("Fixture validation blocker was not released")
        return {"job_id": "range_validation_fixture"}

    def range_validation_status(self, job_id):
        return copy.deepcopy(self.validation_status)


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk checks require an existing DISPLAY")
class GuiOptionalSettingsTkTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-optional-state-")
        self.addCleanup(self.directory.cleanup)
        self.setup_path = os.path.join(self.directory.name, "fixture_lib", "VCO", "maestro", "maestro.sdb")
        os.makedirs(os.path.dirname(self.setup_path))
        with open(self.setup_path, "w", encoding="utf-8") as stream:
            stream.write('<setupdb application="maestro"/>')
        self.root = gui.tk.Tk()
        self.root.withdraw()
        self.destroyed = False
        destroy = self.root.destroy
        def tracked_destroy():
            self.destroyed = True
            destroy()
        self.root.destroy = tracked_destroy
        self.releases = []
        self.service = OptionalSettingsService(self.setup_path, self.directory.name)
        self.app = None
        self.errors = []
        self.addCleanup(self._close)
        patch = mock.patch.object(gui.messagebox, "showerror")
        self.showerror = patch.start()
        self.addCleanup(patch.stop)
        for function in ("parse_rdb", "parse_rdb_summary"):
            patch = mock.patch("crescendo_engine.rdb." + function, side_effect=AssertionError("UI state must not read RDB"))
            patch.start()
            self.addCleanup(patch.stop)
        self.app = gui.OptimizerGui(self.root, self.service)
        self.root.report_callback_exception = lambda kind, value, trace: self.errors.append((kind, value))
        self.app.setup_path_var.set(self.setup_path)
        self._wait(lambda: "/fixture/ocean" in self.app.eda_effective_vars["ocean_bin"].get())

    def _close(self):
        self.service.validation_release.set()
        for release in self.releases:
            release.set()
        if self.app:
            self.app.tasks.close()
            for name in ("worker", "control_worker", "history_worker", "optional_worker", "exit_worker"):
                worker = getattr(self.app.tasks, name, None)
                if worker:
                    worker.join(1)
        # Tcl variable traces outlive Tk windows and retain bound app callbacks.
        # Remove only callbacks in this fixture's interpreter, on its owner
        # thread, so the closed application can actually become collectible.
        owned_variables = [value for value in gc.get_objects()
                           if isinstance(value, gui.tk.Variable) and value._tk is self.root.tk]
        for variable in owned_variables:
            for modes, callback in variable.trace_info():
                variable.trace_remove(modes, callback)
        if not self.destroyed:
            for identifier in self.root.tk.splitlist(self.root.tk.call("after", "info")):
                self.root.after_cancel(identifier)
            self.root.destroy()
        # Tkinter also registers bindings with needcleanup=False; root.destroy
        # leaves those ID-prefixed Python callback commands in this interpreter.
        # Delete them after its windows are gone, releasing bound app methods.
        for command in self.root.tk.splitlist(self.root.tk.call("info", "commands")):
            if command and command[0].isdigit():
                self.root.tk.deletecommand(command)
        # All fixture producers have joined and Tk callbacks are cancelled.
        # Release the closed app on its owner thread before another worker's
        # automatic GC can finalize its TkVariable/callback cycles.
        self.app = None
        gc.collect()

    def _wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.destroyed:
                self.root.update()
            if self.errors:
                self.fail("Unexpected Tk callback: {}".format(self.errors))
            if predicate():
                return
            time.sleep(.003)
        self.fail("Fixture did not reach expected state")

    def _prepared(self):
        preview = self.service.inspect_cells(self.setup_path)[1]
        self.app._set_cell_inspection("fixture-inspection", preview)
        self.app.selected_cell_scope_ids = {"cell-B"}
        self.app.applied_cell_scope_ids = {"cell-B"}
        self.app._set_prepared(*self.service.prepared_fixture())
        self.app.selected_test = 'VCO'
        self.app._reset_metric_rows()
        self.app.add_objective({'output_key': 'frequency', 'direction': 'minimize', 'corners': ['tt']})
        self.app._update_cell_selection_state()
        self.assertFalse(self.app.cell_scope_dirty)

    def _edit_draft(self):
        app = self.app
        app.parameter_configs["/M0/w"].update(selected=True, minimum="unfinished min", maximum="7.25", unit="u", scale="log")
        app.parameter_configs["/M1/w"].update(selected=False, minimum="2.25", maximum="8.75")
        app.objective_rows[0].mode_var.set("target")
        app.objective_rows[0]._mode_changed()
        app.objective_rows[0].value_var.set("unfinished target")
        app.add_constraint({"output_key": "frequency", "operator": ">=", "limit": "unfinished limit", "corners": ["tt"]})
        app.generations_var.set("unfinished generations")
        app.timeout_var.set("345")
        app.job_command_override_var.set(True)
        app.job_command_var.set('batch-submit -q alternate -R "cpu=6;mem=30000"')
        app.job_max_override_var.set(True)
        app.job_max_var.set("19")
        app.maestro_candidate_mode_var.set("batch")
        app.advanced_visible.set(True)
        app.last_run_display_name = "fixture saved run"
        app.algorithm_parameter_vars[("differential_evolution", "population")].set("17")
        app.algorithm_parameter_manual.add(("differential_evolution", "population"))

    def _save(self, name="fixture-state.json"):
        path = os.path.join(self.directory.name, "elsewhere", name)
        with mock.patch.object(gui.filedialog, "asksaveasfilename", return_value=path):
            self.app.save_ui_state()
        self._wait(lambda: os.path.isfile(path) and str(self.app.save_config_button.cget("state")) == "normal")
        with open(path, encoding="utf-8") as stream:
            return path, json.load(stream)

    def _load(self, path):
        with mock.patch.object(gui.filedialog, "askopenfilename", return_value=path):
            self.app.load_ui_state()
        self._wait(lambda: not self.app.configuration_loading)

    def _text(self, window):
        return next(widget for widget in window.winfo_children() if isinstance(widget, gui.tk.Text))

    def test_real_tcl_popup_mapping_is_ignored_and_normal_page_wheel_still_scrolls(self):
        combo = gui.ttk.Combobox(self.root, values=("A", "B"))
        popup = str(self.root.tk.call("ttk::combobox::PopdownWindow", str(combo))) + ".f.l"
        with self.assertRaisesRegex(KeyError, "popdown"):
            self.root.nametowidget(popup)
        event = type("Event", (), {"x_root": 1, "y_root": 1, "num": 4})()
        self.app.pages = {"fixture": self.root}
        self.app.current_page = "fixture"
        self.root.scroll_units = mock.Mock()
        with mock.patch.object(self.root, "winfo_ismapped", return_value=True), \
                mock.patch.object(self.root, "winfo_containing", side_effect=lambda x, y: self.root.nametowidget(popup)):
            self.assertIsNone(self.app._page_mousewheel(event))
        self.root.scroll_units.assert_not_called()
        label = gui.ttk.Label(self.root, text="normal owned widget")
        with mock.patch.object(self.root, "winfo_ismapped", return_value=True), \
                mock.patch.object(self.root, "winfo_containing", return_value=label):
            self.assertEqual(self.app._page_mousewheel(event), "break")
        self.root.scroll_units.assert_called_once_with(-1)

    def test_hung_optional_startup_does_not_queue_optimization_or_control(self):
        self._prepared()
        self.app.job_command_override_var.set(True)
        self.app.job_command_var.set('fixture-submit')
        self.app.parameter_configs["/M1/w"]["selected"] = False
        self.service.validation_release.clear()
        self.app.validate_all_ranges()
        self._wait(self.service.validation_entered.is_set)
        control = threading.Event()
        self.app.tasks.submit_control(control.set)
        with mock.patch.object(self.app, "_request_run_display_name", return_value="fixture optimization"), \
                mock.patch.object(self.app, "_poll_progress", return_value=None):
            self.app.start_run()
            self._wait(lambda: self.service.start.called and self.app.run_ui.state == "running" and control.is_set())
        self.assertFalse(self.service.validation_release.is_set())
        self.assertEqual([row["parameter_id"] for row in self.service.start.call_args[0][1]["variables"]], ["/M0/w"])
        self.assertEqual([row["parameter_id"] for row in self.service.validation_calls[0]], ["/M0/w"])

    def test_exit_bypasses_hung_optional_startup(self):
        self._prepared()
        self.service.validation_release.clear()
        self.app.validate_all_ranges()
        self._wait(self.service.validation_entered.is_set)
        self.app.request_exit(confirm=False)
        self._wait(lambda: self.destroyed)
        self.assertEqual(self.service.exit_calls, [True])
        self.assertFalse(self.service.validation_release.is_set())
        self.assertTrue(self.app.cleanup_confirmed)

    def test_short_validation_status_and_details_preserve_long_native_error(self):
        self._prepared()
        detail = "BEGIN_NATIVE_ERROR\n" + "fixture OA readability detail\n" * 200 + "END_NATIVE_ERROR"
        self.service.validation_status = {"state": "failed", "message": "Optional PDK check unavailable",
                                          "detail": detail, "detail_path": "/fixture/check/error.json", "results": []}
        self.app.range_validation_job_id = "range_validation_fixture"
        self.app._poll_range_validation()
        self._wait(lambda: "END_NATIVE_ERROR" in self.app.range_validation_detail)
        self.assertLess(len(self.app.range_status_var.get()), 130)
        self.assertIn("Optimization can start", self.app.range_status_var.get())
        window = self.app.show_validation_details()
        text = self._text(window)
        self.assertIn("BEGIN_NATIVE_ERROR", text.get("1.0", "end-1c"))
        self.assertIn("END_NATIVE_ERROR", text.get("1.0", "end-1c"))
        self.assertEqual(str(text.cget("state")), "disabled")
        self.assertTrue(text.cget("yscrollcommand"))
        text.tag_add("sel", "1.0", "end-1c")
        self.assertIn("END_NATIVE_ERROR", text.get("sel.first", "sel.last"))
        window.destroy()
        self.app._set_error(detail)
        self.assertLessEqual(len(self.app.status_var.get()), 165)
        window = self.app.show_diagnostic_details()
        self.assertEqual(self._text(window).get("1.0", "end-1c"), detail)
        window.destroy()

    def test_save_without_preparation_stores_raw_draft_at_user_selected_path(self):
        self.app.generations_var.set("unfinished generation")
        self.app.timeout_var.set("unfinished timeout")
        with mock.patch.object(self.app, "build_design_config", side_effect=AssertionError("State is not legacy config")):
            path, document = self._save()
        self.assertTrue(path.startswith(os.path.join(self.directory.name, "elsewhere") + os.sep))
        self.assertEqual(document["schema_version"], "crescendo.configuration.v1")
        self.assertEqual(document["settings"]["optimization"]["generations"], "unfinished generation")
        self.assertEqual(document["settings"]["optimization"]["candidate_timeout_seconds"], "unfinished timeout")
        self.assertEqual(document["source_setup"]["path"], self.setup_path)
        self.assertIsNone(self.app.prepared)
        self.assertEqual(self.service.prepare_calls, [])
        self.service.load_design_config.assert_not_called()

    def test_same_tb_roundtrip_retains_parameters_raw_metrics_and_optimization(self):
        self._prepared()
        self._edit_draft()
        self.app.solver_command_vars["spectre"].set(gui.DEFAULT_SOLVER_TEMPLATES["spectre"])
        self.app.solver_command_vars["alps"].set(gui.DEFAULT_SOLVER_TEMPLATES["alps"])
        self.app.submission_command_var.set("batch-submit -q roundtrip {job_script}")
        self.app.scheduler_command_vars['query'].set('batch-query --id {job_id}')
        self.app.scheduler_command_vars['cancel'].set('unfinished cancel draft')
        expected = self.app._snapshot_ui_state()[0]
        path, document = self._save()
        self.app.parameter_configs["/M0/w"].update(selected=False, minimum="9", maximum="10")
        self.app.parameter_configs["/M1/w"]["selected"] = True
        self.app._reset_metric_rows()
        self.app.add_objective()
        self.app.generations_var.set("20")
        self.app.job_command_var.set("changed")
        self.app.solver_command_vars["spectre"].set("")
        self.app.solver_command_vars["alps"].set("")
        self.app.submission_command_var.set("")
        self.app.job_max_var.set("2")
        self.app.selected_cell_scope_ids = {"cell-root"}
        prepare_count = len(self.service.prepare_calls)
        # Replaced fixture metric widgets must finalize on their Tk owner before
        # a background settings matcher can trigger Python cyclic collection.
        gc.collect()
        self._load(path)
        actual = self.app._snapshot_ui_state()[0]
        for key in ("parameter_configs", "selected_cell_scope_ids", "objectives", "constraints", "optimization"):
            self.assertEqual(actual[key], expected[key], key)
        self.assertEqual(len(self.service.prepare_calls), prepare_count + 1)
        self.service.select_setup.assert_not_called()
        self.service.load_design_config.assert_not_called()
        self.assertEqual(document["settings"]["constraints"][0]["limit"], "unfinished limit")

    def test_other_tb_and_pdk_skip_unmatched_after_automatic_current_tb_inspection(self):
        self._prepared()
        self._edit_draft()
        path, _ = self._save()
        self.service.config["_selected_setup_source"]["cell"] = "OtherVCO"
        for row in self.app.parameter_by_id.values():
            row["master_library"] = "anotherPDK"
        self.service.parameter_master_library = "anotherPDK"
        self.app.prepared["outputs"] = []
        self.app.parameter_configs["/M0/w"].update(minimum="safe current", selected=False)
        self.app.job_command_var.set("current command")
        prepare_count = len(self.service.prepare_calls)
        self._load(path)
        self.assertNotEqual(self.app.parameter_configs["/M0/w"]["minimum"], "unfinished min")
        self.assertEqual(self.app.job_command_var.get(), 'batch-submit -q alternate -R "cpu=6;mem=30000"')
        self.assertIn("skipped", self.app.settings_match_detail)
        self.assertIn("anotherPDK", self.app.parameter_by_id["/M0/w"]["master_library"])
        self.assertEqual(len(self.service.prepare_calls), prepare_count + 1)
        self.assertEqual(self.app.setup_path_var.get(), self.setup_path)
        self.service.select_setup.assert_not_called()
        self.service.load_design_config.assert_not_called()

    def test_configuration_automatically_loads_applies_and_matches_unloaded_ui(self):
        self._prepared()
        self._edit_draft()
        expected = copy.deepcopy(self.app.parameter_configs)
        path, _ = self._save()
        self.app._invalidate_prepared("Fixture returned to unloaded UI")
        self.service.inspection_calls[:] = []
        self.service.prepare_calls[:] = []
        self._load(path)
        self._wait(lambda: self.app.prepared_id == "prepared-fixture" and self.app.parameter_configs == expected)
        self.assertEqual(self.service.apply_calls, [("fixture-inspection", ["cell-B"])])
        self.assertEqual(len(self.service.inspection_calls), 1)
        self.assertEqual(len(self.service.prepare_calls), 1)
        self.service.select_setup.assert_not_called()
        self.service.load_design_config.assert_not_called()

    def test_active_run_or_setup_loading_refuses_state_load_before_dialog(self):
        for state, loading in (("running", False), ("stopping", False), ("idle", True)):
            self.app.run_ui.state = state
            self.app.setup_loading = loading
            with mock.patch.object(gui.filedialog, "askopenfilename") as dialog:
                self.app.load_ui_state()
            dialog.assert_not_called()
        self.assertEqual(self.service.prepare_calls, [])

    def test_state_load_callback_is_fenced_on_tb_catalogue_or_active_run_changes(self):
        self._prepared()
        self._edit_draft()
        path, document = self._save()
        for changed in ("selection", "catalogue", "run", "new_request"):
            with self.subTest(changed=changed):
                entered, release = threading.Event(), threading.Event()
                drained = threading.Event()
                self.releases.append(release)
                self.app.run_ui = RunUiState()
                self.app.setup_loading = False
                self.app.pending_settings_state = None
                self.app.job_command_var.set("retain current " + changed)
                def read(path, return_result=False):
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("State read fixture was not released")
                    return {'document': copy.deepcopy(document), 'path': path, 'revision': 'fixture'} if return_result else copy.deepcopy(document)
                with mock.patch.object(gui, "read_configuration", side_effect=read):
                    with mock.patch.object(gui.filedialog, "askopenfilename", return_value=path):
                        self.app.load_ui_state()
                    self.assertTrue(entered.wait(10))
                    if changed == "selection":
                        self.app.setup_selection_epoch += 1
                    elif changed == "catalogue":
                        self.app.prepared_epoch += 1
                    elif changed == "run":
                        self.app.run_ui.started("a-new-run")
                    else:
                        self.app.settings_load_epoch += 1
                    release.set()
                    self.app.tasks.submit(lambda: None, lambda unused: drained.set())
                    self._wait(drained.is_set)
                    if changed == "new_request":
                        # The earlier callback must not release controls owned
                        # by the newer request token.
                        self.assertTrue(self.app.configuration_loading)
                        self.app.configuration_loading = False
                        self.app.setup_loading = False
                        self.app._set_setup_controls(True)
                self.assertEqual(self.app.job_command_var.get(), "retain current " + changed)
                self.assertIsNone(self.app.pending_settings_state)

    def test_start_preflight_pending_rejects_state_load_without_waiting_for_registration(self):
        self._prepared()
        self.app.job_command_override_var.set(True)
        self.app.job_command_var.set('fixture-submit')
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def static(prepared_id, variables):
            entered.set()
            if not release.wait(5):
                raise RuntimeError("Start static fixture was not released")
            return {"valid": True, "issues": [], "variables": variables}
        with mock.patch.object(self.service, "validate_static_ranges", side_effect=static, create=True), \
                mock.patch.object(self.app, "_request_run_display_name", return_value="fixture optimization"), \
                mock.patch.object(self.app, "_poll_progress", return_value=None):
            self.app.start_run()
            self.assertTrue(entered.wait(10))
            self.assertTrue(self.app.start_request_pending)
            self.assertEqual(self.app.run_ui.state, "idle")
            reason = self.app._setup_reload_block_reason()
            self.assertIsNotNone(reason)
            self.assertIn("start", reason.lower())
            inspections_before = list(self.service.inspection_calls)
            self.app.load_setup()
            self.assertEqual(self.service.inspection_calls, inspections_before)
            self.assertIn("start", self.app.last_diagnostic_detail.lower())
            with mock.patch.object(gui.filedialog, "askopenfilename") as dialog:
                self.app.load_ui_state()
            dialog.assert_not_called()
            self.service.start.assert_not_called()
            release.set()
            self._wait(lambda: self.service.start.called and self.app.run_ui.state == "running")
            self.assertFalse(self.app.start_request_pending)

    def test_deferred_match_callback_does_not_modify_changed_tb_or_pending_start(self):
        self._prepared()
        self._edit_draft()
        _, document = self._save()
        document = document["settings"]
        original_match = gui.match_settings_state
        for changed in ("selection", "inspection", "catalogue", "run", "loading", "start"):
            with self.subTest(changed=changed):
                self.app.run_ui = RunUiState()
                self.app.setup_loading = False
                self.app.start_request_pending = False
                self.app.pending_settings_state = copy.deepcopy(document)
                self.app.job_command_var.set("retain deferred " + changed)
                entered, release, drained = threading.Event(), threading.Event(), threading.Event()
                self.releases.append(release)
                def match(saved, context):
                    entered.set()
                    if not release.wait(5):
                        raise RuntimeError("Deferred match fixture was not released")
                    return original_match(saved, context)
                with mock.patch.object(gui, "match_settings_state", side_effect=match):
                    self.app._restore_pending_ui_state()
                    self.assertTrue(entered.wait(10))
                    if changed == "selection":
                        self.app.setup_selection_epoch += 1
                    elif changed == "inspection":
                        self.app.cell_inspection_id = "a-new-inspection"
                    elif changed == "catalogue":
                        self.app.prepared_epoch += 1
                    elif changed == "run":
                        self.app.run_ui.started("a-new-run")
                    elif changed == "loading":
                        self.app.setup_loading = True
                    else:
                        self.app.start_request_pending = True
                    self.app.tasks.submit(lambda: None, lambda unused: drained.set())
                    release.set()
                    self._wait(drained.is_set)
                self.assertEqual(self.app.job_command_var.get(), "retain deferred " + changed)
