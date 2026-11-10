from __future__ import print_function

import copy
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.ade_setup import resolve_setup_locator


class SelectionService(object):
    """Exercise the real GUI queue without running Cadence or a simulation."""

    def __init__(self, context):
        self.context = copy.deepcopy(context)
        self.origin_path = context["source"]["setup_path"]
        self.selected_path = self.origin_path
        self.prepare_calls = []
        self.select_calls = []
        self.prepare_errors = {}

    def eda_setup(self):
        library = resolve_setup_locator(self.selected_path)["library_name"]
        return {
            "mode": "auto", "ok": True, "manual": {},
            "effective": {
                "ocean_bin": "/fixture/cadence/ocean",
                "virtuoso_bin": "/fixture/cadence/virtuoso",
                "cds_lib": "/fixture/work/cds.lib",
                "pdk_name": "PDK_" + library,
            },
            "sources": {"ocean_bin": "invoking Maestro installation"},
        }

    def list_runs(self):
        return []

    def select_setup(self, path):
        selected = resolve_setup_locator(path)["path"]
        self.selected_path = selected
        self.select_calls.append(selected)
        context = copy.deepcopy(self.context) if selected == self.origin_path else None
        return {
            "context": context, "setup_path": selected,
            "mode": "invoking" if context else "manual",
            "eda_setup": self.eda_setup(),
        }

    def prepare(self, path, **options):
        selected = resolve_setup_locator(path)["path"]
        if selected != self.selected_path:
            raise AssertionError("GUI and service disagree about the selected setup")
        self.prepare_calls.append((selected, copy.deepcopy(options)))
        if selected in self.prepare_errors:
            raise self.prepare_errors[selected]
        locator = resolve_setup_locator(selected)
        library = locator["library_name"]
        return "prepared-" + library, {
            "setup_path": selected, "setup_locator": locator,
            "setup": {
                "backend": "maestro", "tests": [{
                    "name": "VCO", "library": library,
                    "cell": "TB", "view": "schematic",
                }], "corners": [], "analysis_names": ["tran"],
            },
            "parameters": [{
                "parameter_id": library + "/M0/w", "name": "w",
                "prompt": "Width", "semantic": "width", "optimizable": True,
                "hierarchy_path": "/M0", "instance_name": "M0",
                "master_library": "fixturePDK", "master_cell": "nmos",
                "numeric_value": 1e-6, "current_value": "1u",
                "value_type": "float",
            }],
            "outputs": [{"key": "frequency"}], "hierarchy": {"designs": []},
            "resident_validation": {"state": "deferred"},
            "eda_context": {"pdk": {"pdk_name": "PDK_" + library}},
        }


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk selection tests require an existing DISPLAY")
class GuiSetupSelectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-tk-selection-")
        self.addCleanup(self.directory.cleanup)
        self.paths = {}
        for library in ("lib_A", "lib_B"):
            folder = os.path.join(self.directory.name, library, "VCO", "maestro")
            os.makedirs(folder)
            path = os.path.join(folder, "maestro.sdb")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write('<setupdb application="maestro"/>')
            self.paths[library] = path
        self.context = {
            "source": {
                "library": "lib_A", "cell": "VCO", "view": "maestro",
                "backend": "maestro", "setup_path": self.paths["lib_A"],
                "view_dir": os.path.dirname(self.paths["lib_A"]),
                "modified": False,
            },
            "live_tests": [{
                "name": "VCO", "library": "lib_A", "cell": "TB", "view": "schematic",
            }],
        }
        try:
            self.root = gui.tk.Tk()
        except gui.tk.TclError as error:
            self.skipTest("Existing DISPLAY cannot initialize Tk: {}".format(error))
        self.root.withdraw()
        self.app = None
        self.callback_errors = []
        self.addCleanup(self._close_gui)
        self.dialogs = mock.patch.object(gui.messagebox, "showerror")
        self.showerror = self.dialogs.start()
        self.addCleanup(self.dialogs.stop)

    def _close_gui(self):
        if self.app is not None:
            self.app.tasks.close()
            self.app.tasks.worker.join(1)
            self.app.tasks.control_worker.join(1)
            self.app.tasks.history_worker.join(1)
        for identifier in self.root.tk.splitlist(self.root.tk.call("after", "info")):
            self.root.after_cancel(identifier)
        self.root.destroy()

    def _open(self):
        self.service = SelectionService(self.context)
        self.app = gui.OptimizerGui(self.root, self.service, launch_context=self.context)
        self.root.report_callback_exception = (
            lambda exception, value, trace: self.callback_errors.append((exception, value))
        )
        self.assertIsNone(self.app.prepared_id)
        self.assertEqual(self.service.prepare_calls, [])
        self._wait(lambda: "no hierarchy scan" in self.app.status_var.get())
        self.app.load_setup_button.invoke()
        self._wait(lambda: self.app.prepared_id == "prepared-lib_A")
        return self.app

    def _wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.root.update()
            if self.callback_errors:
                self.fail("Tk callback failed: {}".format(self.callback_errors))
            if predicate():
                return
            time.sleep(0.005)
        self.fail("GUI did not reach expected state: {}".format(self.app.status_var.get()))

    def _widgets(self, widget):
        for child in widget.winfo_children():
            yield child
            for descendant in self._widgets(child):
                yield descendant

    def _button(self, text):
        buttons = [widget for widget in self._widgets(self.root)
                   if isinstance(widget, gui.ttk.Button) and widget.cget("text") == text]
        self.assertEqual(len(buttons), 1, "Missing or duplicate button: " + text)
        return buttons[0]

    def _browse_file(self, path):
        with mock.patch.object(gui.filedialog, "askopenfilename", return_value=path):
            self._button("Browse File...").invoke()

    def _load_b(self):
        self._browse_file(self.paths["lib_B"])
        self.app.load_setup_button.invoke()
        self._wait(lambda: self.app.prepared_id == "prepared-lib_B")

    def test_plugin_opens_with_default_binding_and_usable_controls(self):
        app = self._open()
        self.assertEqual(str(self._button("Browse File...").cget("state")), "normal")
        self.assertEqual(str(self._button("Browse Folder...").cget("state")), "normal")
        self.assertEqual(str(app.setup_path_entry.cget("state")), "normal")
        self.assertEqual(app.load_setup_button.cget("text"), "新建 / Load and Inspect")
        self.assertEqual(app.active_launch_context["source"]["library"], "lib_A")
        self.assertEqual(app.active_setup_path, self.paths["lib_A"])
        self.assertFalse(self.service.prepare_calls[0][1]["initialize_validation"])
        self.assertFalse(self.showerror.called)

    def test_cancel_browse_preserves_prepared_setup_and_binding(self):
        app = self._open()
        selection_calls = list(self.service.select_calls)
        self._browse_file("")
        with mock.patch.object(gui.filedialog, "askdirectory", return_value=""):
            self._button("Browse Folder...").invoke()
        self.assertEqual(app.prepared_id, "prepared-lib_A")
        self.assertEqual(app.active_setup_path, self.paths["lib_A"])
        self.assertEqual(app.active_launch_context["source"]["library"], "lib_A")
        self.assertEqual(self.service.select_calls, selection_calls)

    def test_same_source_saved_reload_updates_old_live_binding_without_transferring_foreign_design_parameters(self):
        app = self._open()
        app.active_launch_context["source"]["modified"] = True
        self.service.context["source"]["modified"] = False
        self.service.context["live_tests"][0]["cell"] = "SavedTB"
        identifier = next(iter(app.parameter_configs))
        app.parameter_configs[identifier]["minimum"] = "unfinished original edit"
        app.generations_var.set("23")
        original_prepare = self.service.prepare
        def current_saved(path, **options):
            token, prepared = original_prepare(path, **options)
            prepared["setup"]["tests"][0]["cell"] = "SavedTB"
            prepared["outputs"].append({"key": "new_saved_output"})
            return token, prepared
        self.service.prepare = mock.Mock(side_effect=current_saved)
        app.load_setup_button.invoke()
        self._wait(lambda: len(self.service.prepare_calls) == 2 and not app.setup_loading and
                   app.eda_auto_revalidation is None)
        self.assertFalse(self.showerror.called)
        self.assertEqual(app.active_launch_context["live_tests"][0]["cell"], "SavedTB")
        # The test now points to another schematic. Accept the fresh binding,
        # but never transfer a same-named device edit across that design change.
        self.assertNotEqual(app.parameter_configs[identifier]["minimum"], "unfinished original edit")
        self.assertEqual(app.generations_var.get(), "23")
        self.assertIn("new_saved_output", app._output_names())
        self.assertFalse(app.eda_revalidation_required)
        app._require_prepared_selection()

    def test_manual_file_selection_changes_gui_and_service_together(self):
        app = self._open()
        app.launch_history_summary_var.set("OLD SOURCE RDB")
        self._load_b()
        self.assertIsNone(app.active_launch_context)
        self.assertEqual(app.active_setup_path, self.paths["lib_B"])
        self.assertEqual(self.service.selected_path, self.paths["lib_B"])
        self.assertIn("lib_B", next(iter(app.parameter_by_id)))
        self.assertNotIn("OLD SOURCE RDB", app.launch_history_summary_var.get())
        self.assertNotIn("lib_A/VCO/maestro", self.root.title())
        self.assertFalse(self.service.prepare_calls[-1][1]["initialize_validation"])

    def test_manual_folder_selection_can_return_to_invoking_maestro(self):
        app = self._open()
        with mock.patch.object(gui.filedialog, "askdirectory",
                               return_value=os.path.dirname(self.paths["lib_B"])):
            self._button("Browse Folder...").invoke()
        app.load_setup_button.invoke()
        self._wait(lambda: app.prepared_id == "prepared-lib_B")
        self.assertFalse(hasattr(app, "use_invoking_setup_button"))
        app.setup_path_var.set("")
        app.load_setup_button.invoke()
        self._wait(lambda: app.prepared_id == "prepared-lib_A" and not app.setup_loading)
        self.assertEqual(app.active_setup_path, self.paths["lib_A"])
        self.assertEqual(app.active_launch_context["source"]["library"], "lib_A")
        self.assertEqual(self.service.selected_path, self.paths["lib_A"])
        self.assertIn("lib_A", next(iter(app.parameter_by_id)))

    def test_busy_work_prevents_browse_and_load_without_losing_current_setup(self):
        app = self._open()
        for reason in ("setup", "optimization", "range"):
            with self.subTest(reason=reason):
                app.setup_loading = reason == "setup"
                app.run_ui.state = "running" if reason == "optimization" else "idle"
                app.range_validation_inflight = reason == "range"
                prior_calls = list(self.service.prepare_calls)
                with mock.patch.object(gui.filedialog, "askopenfilename") as dialog:
                    app._browse_setup_file()
                    app.load_setup()
                self.assertFalse(dialog.called)
                self.assertEqual(self.service.prepare_calls, prior_calls)
                self.assertEqual(app.prepared_id, "prepared-lib_A")
        app.setup_loading = False
        app.run_ui.state = "idle"
        app.range_validation_inflight = False

    def test_automatic_environment_is_visible_without_becoming_manual_overrides(self):
        app = self._open()
        self._wait(lambda: "ocean" in app.eda_effective_vars["ocean_bin"].get())
        self.assertIn("/fixture/cadence/ocean", app.eda_effective_vars["ocean_bin"].get())
        self.assertIn("invoking Maestro", app.eda_effective_vars["ocean_bin"].get())
        self.assertIn("PDK_lib_A", app.eda_effective_vars["pdk_name"].get())
        self.assertIn("Not detected", app.eda_effective_vars["pdk_dir"].get())
        self.assertTrue(all(not variable.get() for variable in app.eda_vars.values()))
        self.assertEqual(app._collect_eda_overrides(), {})

    def test_failed_inspection_is_visible_and_load_button_recovers(self):
        app = self._open()
        self.service.prepare_errors[self.paths["lib_B"]] = RuntimeError("fixture missing native tool")
        self._browse_file(self.paths["lib_B"])
        app.load_setup_button.invoke()
        self._wait(lambda: self.showerror.called and not app.setup_loading)
        self.assertIn("fixture missing native tool", str(self.showerror.call_args))
        self.assertEqual(str(app.load_setup_button.cget("state")), "normal")
        self.assertIn("fixture missing native tool", app.status_var.get())

    def test_editing_setup_path_requires_load_before_using_prepared_actions(self):
        app = self._open()
        app.setup_path_var.set(self.paths["lib_B"])
        for button in (app.start_button, app.validate_button):
            self.assertEqual(str(button.cget("state")), "disabled")
        self.assertEqual(str(app.save_config_button.cget("state")), "normal")
        self.assertEqual(app.prepared_id, "prepared-lib_A")
        self.assertEqual(self.service.selected_path, self.paths["lib_A"])
        app.setup_path_var.set(self.paths["lib_A"])
        for button in (app.start_button, app.save_config_button, app.validate_button):
            self.assertEqual(str(button.cget("state")), "normal")

    def test_loading_new_setup_blocks_buttons_and_programmatic_prepared_actions(self):
        app = self._open()
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        prepare = self.service.prepare

        def blocked_prepare(path, **options):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("Preparation fixture was not released")
            return prepare(path, **options)

        with mock.patch.object(self.service, "prepare", side_effect=blocked_prepare), \
                mock.patch.object(self.service, "start", create=True) as start, \
                mock.patch.object(self.service, "validate_ranges", create=True) as validate, \
                mock.patch.object(app, "_static_range_report") as static_ranges, \
                mock.patch.object(gui.filedialog, "asksaveasfilename") as save_dialog:
            app.apply_ranges_button.configure(state="normal")
            self._browse_file(self.paths["lib_B"])
            app.load_setup_button.invoke()
            self.assertTrue(entered.wait(1))
            self.assertTrue(app.setup_loading)
            for button in (app.start_button,
                           app.validate_button, app.apply_ranges_button):
                self.assertEqual(str(button.cget("state")), "disabled")
            app.start_run()
            app.validate_all_ranges()
            start.assert_not_called()
            validate.assert_not_called()
            static_ranges.assert_not_called()
            self.assertEqual(str(app.save_config_button.cget("state")), "normal")
            release.set()
            self._wait(lambda: app.prepared_id == "prepared-lib_B" and not app.setup_loading)

    def test_completed_run_and_displayed_candidates_are_cleared_when_switching_setup(self):
        app = self._open()
        app.run_ui.started("OLD_RUN_A")
        app.run_ui.update("completed")
        app.live_candidates = [{
            "candidate_id": "OLD_CANDIDATE_A", "status": "completed",
            "metrics": {"feasible": True}, "values": {},
        }]
        app.selected_candidate_id = "OLD_CANDIDATE_A"
        app.export_csv_button.configure(state="normal")
        app.render_results()
        self.assertIn("OLD_CANDIDATE_A", app.result_tree.get_children(""))
        self._load_b()
        self.assertIsNone(app.run_ui.run_id)
        self.assertEqual(app.run_ui.state, "idle")
        self.assertEqual(app.live_candidates, [])
        self.assertIsNone(app.selected_candidate_id)
        self.assertEqual(app.result_tree.get_children(""), ())
        self.assertNotIn("OLD_CANDIDATE_A", app.candidate_detail.get("1.0", "end"))
        self.assertEqual(str(app.export_csv_button.cget("state")), "disabled")

    def test_invalid_browse_target_does_not_replace_previous_prepared_setup(self):
        app = self._open()
        self._browse_file(os.path.join(self.directory.name, "missing", "maestro.sdb"))
        app.load_setup_button.invoke()
        self._wait(lambda: self.showerror.called and not app.setup_loading)
        self.assertEqual(app.prepared_id, "prepared-lib_A")
        self.assertEqual(app.active_setup_path, self.paths["lib_A"])
        self.assertEqual(app.active_launch_context["source"]["library"], "lib_A")
        self.assertEqual(self.service.selected_path, self.paths["lib_A"])
        self.assertEqual(str(app.load_setup_button.cget("state")), "normal")

    def test_history_result_from_previous_selection_does_not_replace_new_status(self):
        app = self._open()
        record = {"name": "OLD_HISTORY", "rdb_file": "/fixture/old.rdb"}
        app.active_launch_context["histories"] = [record]
        app._populate_history_choices()
        app.history_choice_var.set("History: OLD_HISTORY")
        app._history_choice_changed()
        delayed = []
        submit = app.tasks.submit_history

        def delay_delivery(function, loaded=None, failed=None):
            def hold_result(payload):
                delayed.append(lambda: loaded(payload))
            return submit(function, hold_result, failed)

        with mock.patch("crescendo_engine.rdb.parse_rdb_summary", return_value={
                    "point_count": 1, "output_count": 1, "test_count": 0, "corner_count": 0,
                }):
            with mock.patch.object(app.tasks, "submit_history", side_effect=delay_delivery):
                app._load_launch_history()
                self._wait(lambda: bool(delayed))
        self._load_b()
        delivered = []

        def deliver_old_result():
            delayed[0]()
            delivered.append(True)

        self.root.after(0, deliver_old_result)
        self._wait(lambda: bool(delivered))
        self.assertNotIn("OLD_HISTORY", app.launch_history_summary_var.get())
        self.assertIsNone(app.active_launch_context)


if __name__ == "__main__":
    unittest.main()
