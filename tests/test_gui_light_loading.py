from __future__ import print_function

import copy
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.ade_setup import resolve_setup_locator


class LightweightUiService(object):
    """Real Tk/worker fixture; it owns no native process or simulation."""

    def __init__(self, setup_path, parameter_count=1):
        self.setup_path = setup_path
        self.parameter_count = parameter_count
        self.prepare_calls = []
        self.list_calls = []
        self.exit_calls = []

    def eda_setup(self):
        return {
            "mode": "auto", "ok": True, "manual": {},
            "effective": {"ocean_bin": "/fixture/ocean"},
            "sources": {"ocean_bin": "invoking Maestro installation"},
        }

    def list_runs(self):
        self.list_calls.append(True)
        return []

    def prepare(self, path, **options):
        locator = resolve_setup_locator(path)
        self.prepare_calls.append((locator["path"], copy.deepcopy(options)))
        parameters = [{
            "parameter_id": "/M{}/w".format(number), "name": "w",
            "prompt": "Width", "semantic": "width", "optimizable": True,
            "hierarchy_path": "/M{}".format(number),
            "instance_name": "M{}".format(number),
            "master_library": "fixturePDK", "master_cell": "nmos",
            "numeric_value": 1e-6, "current_value": "1u",
            "value_type": "float",
        } for number in range(self.parameter_count)]
        return "prepared-fixture", {
            "setup_path": locator["path"], "setup_locator": locator,
            "setup": {
                "backend": "maestro", "tests": [{
                    "name": "VCO", "library": "fixture_lib",
                    "cell": "TB", "view": "schematic",
                }], "corners": [], "analysis_names": ["tran"],
            },
            "parameters": parameters,
            "outputs": [{"key": "frequency"}],
            "hierarchy": {"designs": []},
            "resident_validation": {"state": "deferred"},
            "eda_context": {"pdk": {"pdk_name": "fixturePDK"}},
        }

    def exit_optimizer(self):
        self.exit_calls.append(True)
        return {"closed": True, "fixture_native_processes": 0}


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk loading tests require an existing DISPLAY")
class GuiLightLoadingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-tk-light-")
        self.addCleanup(self.directory.cleanup)
        folder = os.path.join(self.directory.name, "fixture_lib", "VCO", "maestro")
        os.makedirs(folder)
        self.setup_path = os.path.join(folder, "maestro.sdb")
        with open(self.setup_path, "w", encoding="utf-8") as stream:
            stream.write('<setupdb application="maestro"/>')
        self.history_path = os.path.join(self.directory.name, "Heavy#selected.rdb")
        connection = sqlite3.connect(self.history_path)
        connection.executescript('''
            CREATE TABLE test(testID INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE corner(cornerID INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE point(pointID INTEGER PRIMARY KEY, designPointNumber INTEGER, cornerID INTEGER);
            CREATE TABLE result(resultID INTEGER PRIMARY KEY, name TEXT, testID INTEGER);
            INSERT INTO test VALUES(1, 'VCO');
            INSERT INTO corner VALUES(1, 'tt');
            INSERT INTO point VALUES(1, 1, 1);
            INSERT INTO point VALUES(2, 2, 1);
            INSERT INTO point VALUES(3, 3, 1);
            INSERT INTO result VALUES(1, 'frequency', 1);
            INSERT INTO result VALUES(2, 'gain', 1);
        ''')
        connection.commit()
        connection.close()
        self.context = {
            "source": {
                "library": "fixture_lib", "cell": "VCO", "view": "maestro",
                "backend": "maestro", "setup_path": self.setup_path,
                "view_dir": folder, "modified": False,
            },
            "live_tests": [{
                "name": "VCO", "library": "fixture_lib", "cell": "TB", "view": "schematic",
            }],
            "selected_history": "Heavy",
            "histories": [{"name": "Heavy", "rdb_file": self.history_path,
                           "results_dir": os.path.join(self.directory.name, "results")}],
        }
        try:
            self.root = gui.tk.Tk()
        except gui.tk.TclError as error:
            self.skipTest("Existing DISPLAY cannot initialize Tk: {}".format(error))
        self.root.withdraw()
        self.destroyed = False
        destroy = self.root.destroy

        def track_destroy():
            self.destroyed = True
            destroy()

        self.root.destroy = track_destroy
        self.app = None
        self.extra_queues = []
        self.callback_errors = []
        self.addCleanup(self._close_gui)
        errors = mock.patch.object(gui.messagebox, "showerror")
        self.showerror = errors.start()
        self.addCleanup(errors.stop)

    def _close_gui(self):
        queues = list(self.extra_queues)
        if self.app is not None:
            queues.append(self.app.tasks)
        for queue in queues:
            queue.close()
            for name in ("worker", "control_worker", "history_worker", "exit_worker", "shutdown_worker"):
                worker = getattr(queue, name, None)
                if worker is not None:
                    worker.join(1)
        if not self.destroyed:
            for identifier in self.root.tk.splitlist(self.root.tk.call("after", "info")):
                self.root.after_cancel(identifier)
            self.root.destroy()

    def _open(self, parameter_count=1, launch_context=True):
        self.service = LightweightUiService(self.setup_path, parameter_count=parameter_count)
        self.app = gui.OptimizerGui(
            self.root, self.service,
            launch_context=self.context if launch_context else None,
        )
        self.root.report_callback_exception = (
            lambda exception, value, trace: self.callback_errors.append((exception, value))
        )
        self._wait(lambda: "/fixture/ocean" in self.app.eda_effective_vars["ocean_bin"].get())
        return self.app

    def _wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            if not self.destroyed:
                self.root.update()
            if self.callback_errors:
                self.fail("Tk callback failed: {}".format(self.callback_errors))
            if predicate():
                return
            time.sleep(0.003)
        self.fail("GUI did not reach the expected state")

    def _pump(self, seconds=0.1):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not self.destroyed:
            self.root.update()
            time.sleep(0.003)

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

    def _choose_history(self, choice):
        self.app.history_choice_var.set(choice)
        self.app.history_combo.event_generate("<<ComboboxSelected>>")
        self.root.update_idletasks()

    def _load(self):
        self.app.load_setup_button.invoke()
        self._wait(lambda: self.app.prepared_id == "prepared-fixture" and not self.app.setup_loading)

    def _block_queue(self, submit):
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def blocked():
            entered.set()
            if not release.wait(4):
                raise RuntimeError("Blocked fixture was not released")
            return "released"

        submit(blocked)
        self.assertTrue(entered.wait(10))
        return release

    def test_plugin_startup_binds_path_without_scan_history_or_run_inventory(self):
        with mock.patch("crescendo_engine.rdb.parse_rdb") as full_reader, \
                mock.patch("crescendo_engine.rdb.parse_rdb_summary") as summary_reader:
            app = self._open()
            self._pump()
            self.assertEqual(app.setup_path_var.get(), self.setup_path)
            self.assertEqual(app.active_setup_path, self.setup_path)
            self.assertEqual(app.history_choice_var.get(), "None")
            self.assertIn("History: Heavy", app.history_combo.cget("values"))
            self.assertIsNone(app.prepared_id)
            self.assertEqual(self.service.prepare_calls, [])
            self.assertEqual(self.service.list_calls, [])
            # Even a programmatic Read must honor None, rather than fall back
            # to the history that happened to be selected in Maestro.
            app._load_launch_history()
            full_reader.assert_not_called()
            summary_reader.assert_not_called()
            self.assertEqual(str(app.load_setup_button.cget("state")), "normal")

    def test_standalone_startup_does_not_eagerly_inventory_runs(self):
        app = self._open(launch_context=False)
        self._pump()
        self.assertEqual(self.service.prepare_calls, [])
        self.assertEqual(self.service.list_calls, [])
        self.assertIsNone(app.prepared_id)

    def test_explicit_load_preserves_parameters_without_hidden_tree_insertion(self):
        app = self._open(parameter_count=2000)
        self._load()
        self.assertEqual(len(self.service.prepare_calls), 1)
        self.assertFalse(self.service.prepare_calls[0][1]["initialize_validation"])
        self.assertEqual(len(app.parameter_by_id), 2000)
        self.assertEqual(len(app.parameter_configs), 2000)
        self.assertEqual(app.parameter_tree.get_children(""), ())
        self.assertEqual(app.current_page, "Design Setup")
        self.assertEqual(str(app.start_button.cget("state")), "normal")

    def test_history_is_explicit_metadata_only_and_optional_before_setup_load(self):
        app = self._open()
        with mock.patch("crescendo_engine.rdb.parse_rdb") as full_reader:
            self._choose_history("History: Heavy")
            self.assertIsNone(app.prepared_id)
            self._button("Read History Summary").invoke()
            self._wait(lambda: "3 points" in app.launch_history_summary_var.get())
            self.assertIn("2 outputs", app.launch_history_summary_var.get())
            full_reader.assert_not_called()
        self.assertEqual(self.service.prepare_calls, [])
        self._choose_history("None")
        self.assertEqual(app.history_choice_var.get(), "None")
        self.assertNotIn("3 points", app.launch_history_summary_var.get())

    def test_none_cancels_inflight_history_and_discards_its_late_result(self):
        app = self._open()
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        observed_events = []

        def delayed_summary(path, cancel_event=None):
            self.assertEqual(path, self.history_path)
            observed_events.append(cancel_event)
            entered.set()
            if not release.wait(3):
                raise RuntimeError("History fixture was not released")
            # Deliberately finish despite cancellation to verify stale handling.
            return {"point_count": 9876, "output_count": 123, "metadata_only": True}

        with mock.patch("crescendo_engine.rdb.parse_rdb_summary", side_effect=delayed_summary):
            self._choose_history("History: Heavy")
            self._button("Read History Summary").invoke()
            self.assertTrue(entered.wait(10))
            self._choose_history("None")
            self.assertTrue(observed_events[0].is_set())
            release.set()
            self._pump(0.2)
        self.assertNotIn("9876", app.launch_history_summary_var.get())
        self.assertEqual(app.history_choice_var.get(), "None")
        self.assertFalse(self.showerror.called)

    def test_history_summary_does_not_wait_for_an_unrelated_normal_task(self):
        app = self._open()
        release = self._block_queue(app.tasks.submit)
        self._choose_history("History: Heavy")
        self._button("Read History Summary").invoke()
        self._wait(lambda: "3 points" in app.launch_history_summary_var.get(), timeout=1)
        self.assertFalse(release.is_set())

    def test_exit_bypasses_normal_control_and_history_tasks(self):
        app = self._open()
        releases = [self._block_queue(submit) for submit in (
            app.tasks.submit, app.tasks.submit_control, app.tasks.submit_history,
        )]
        late_functions = []
        for submit in (app.tasks.submit, app.tasks.submit_control, app.tasks.submit_history):
            submit(lambda: late_functions.append("queued"),
                   lambda _value: late_functions.append("callback"))
        started = time.monotonic()
        app.request_exit(confirm=False)
        self._wait(lambda: self.destroyed, timeout=1)
        self.assertLess(time.monotonic() - started, 1)
        self.assertEqual(self.service.exit_calls, [True])
        self.assertTrue(all(not release.is_set() for release in releases))
        for release in releases:
            release.set()
        for name in ("worker", "control_worker", "history_worker"):
            getattr(app.tasks, name).join(1)
        self.assertEqual(late_functions, [])
        self.assertFalse(self.showerror.called)

    def test_shutdown_discards_late_timeout_error_and_success_callbacks(self):
        queue = gui.BackgroundTaskQueue(self.root, interval_ms=5)
        self.extra_queues.append(queue)
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)
        errors = []
        callbacks = []
        finished = []

        def late_timeout():
            entered.set()
            release.wait(3)
            raise RuntimeError("late lower-priority timeout")

        queue.submit(late_timeout, callbacks.append, errors.append)
        self.assertTrue(entered.wait(10))
        queue.submit(lambda: "must not run", callbacks.append, errors.append)
        queue.begin_shutdown(lambda: "exit complete", finished.append, errors.append)
        self._wait(lambda: finished == ["exit complete"], timeout=1)
        release.set()
        queue.worker.join(1)
        self._pump()
        self.assertEqual(errors, [])
        self.assertEqual(callbacks, [])

    def test_parameter_page_yields_to_tk_and_completes_without_losing_rows(self):
        app = self._open(parameter_count=2000)
        self._load()
        app.parameter_view_var.set("name")
        heartbeat = []

        def during_insertion():
            count = len(app.parameter_items)
            if count:
                heartbeat.append(count)
            else:
                self.root.after(1, during_insertion)

        self.root.after(1, during_insertion)
        app.show_page("Parameters")
        self._wait(lambda: len(app.parameter_items) == 2000, timeout=4)
        self._wait(lambda: bool(heartbeat))
        self.assertLess(heartbeat[0], 2000, "All rows were inserted before Tk could process another event")
        self.assertEqual(len(app.parameter_configs), 2000)
        self.assertEqual(sum(1 for row in app.parameter_configs.values() if row["selected"]), 2000)
        self.assertEqual(len(app.parameter_items), 2000)

    def test_filter_rebuild_discards_rows_from_superseded_parameter_render(self):
        app = self._open(parameter_count=2000)
        self._load()
        app.parameter_view_var.set("name")
        app.show_page("Parameters")
        app.parameter_filter_var.set("/M1999 ")
        self._wait(lambda: set(app.parameter_items) == {"/M1999/w"})
        self._pump(0.15)
        self.assertEqual(set(app.parameter_items), {"/M1999/w"})
        self.assertEqual(len(app.parameter_configs), 2000)
        self.assertFalse(self.showerror.called)

    def test_exit_cancels_parameter_insertion_before_all_rows_are_created(self):
        app = self._open(parameter_count=5000)
        self._load()
        app.parameter_view_var.set("name")
        created_before_exit = []

        def exit_during_insertion():
            count = len(app.parameter_items)
            if count:
                created_before_exit.append(count)
                app.request_exit(confirm=False)
            else:
                self.root.after(1, exit_during_insertion)

        self.root.after(1, exit_during_insertion)
        app.show_page("Parameters")
        self._wait(lambda: self.destroyed, timeout=1)
        self.assertEqual(len(created_before_exit), 1)
        self.assertEqual(len(app.parameter_items), created_before_exit[0])
        self.assertLess(created_before_exit[0], 5000)
        self.assertEqual(self.service.exit_calls, [True])
        self.assertFalse(self.showerror.called)

    def test_parameter_focus_waits_for_rows_and_ignores_superseded_field_edit(self):
        app = self._open(parameter_count=2000)
        self._load()
        target = "/M1999/w"
        app.parameter_view_var.set("name")
        with mock.patch.object(app, "_edit_parameter_cell") as edit:
            app.focus_parameter(target, field="minimum")
            self.assertIsNotNone(app.pending_parameter_focus)
            self._wait(lambda: not app.parameter_tree_dirty and app.pending_parameter_focus is None)
            item = app.parameter_items[target]
            self.assertEqual(app.parameter_tree.selection(), (item,))
            self.assertEqual(app.parameter_tree.focus(), item)
            # Rerender before the delayed editor opens: the old row's scheduled
            # edit must not attach itself to a newer rendering of the table.
            app.render_parameter_tree()
            self._wait(lambda: not app.parameter_tree_dirty)
            self._pump(0.15)
            edit.assert_not_called()
            app.focus_parameter(target, field="maximum")
            self._wait(lambda: edit.called)
            edit.assert_called_once_with(app.parameter_items[target], target, "#3")

    def test_name_and_hierarchy_views_keep_their_own_expansion_state(self):
        app = self._open(parameter_count=15)
        self._load()
        app.parameter_view_var.set("name")
        app.show_page("Parameters")
        self._wait(lambda: app.rendered_parameter_view == "name" and not app.parameter_tree_dirty)
        name_node = app.parameter_tree.get_children("")[0]
        app.parameter_tree.item(name_node, open=False)
        app.parameter_view_var.set("hierarchy")
        app._parameter_view_changed()
        self._wait(lambda: app.rendered_parameter_view == "hierarchy" and not app.parameter_tree_dirty)
        hierarchy_node = app.parameter_tree.get_children("")[0]
        app.parameter_tree.item(hierarchy_node, open=True)
        app.parameter_view_var.set("name")
        app._parameter_view_changed()
        self._wait(lambda: app.rendered_parameter_view == "name" and not app.parameter_tree_dirty)
        self.assertFalse(app.parameter_tree.item(name_node, "open"))
        app.parameter_view_var.set("hierarchy")
        app._parameter_view_changed()
        self._wait(lambda: app.rendered_parameter_view == "hierarchy" and not app.parameter_tree_dirty)
        self.assertTrue(app.parameter_tree.item(hierarchy_node, "open"))
        app.parameter_tree.item(hierarchy_node, open=False)
        app.parameter_view_var.set("name")
        app._parameter_view_changed()
        self._wait(lambda: app.rendered_parameter_view == "name" and not app.parameter_tree_dirty)
        app.parameter_view_var.set("hierarchy")
        app._parameter_view_changed()
        self._wait(lambda: app.rendered_parameter_view == "hierarchy" and not app.parameter_tree_dirty)
        self.assertFalse(app.parameter_tree.item(hierarchy_node, "open"))

    def test_large_select_all_refresh_yields_and_keeps_every_selection(self):
        app = self._open(parameter_count=5000)
        self._load()
        app.parameter_view_var.set("name")
        app.show_page("Parameters")
        self._wait(lambda: not app.parameter_tree_dirty and len(app.parameter_items) == 5000)
        heartbeat = []

        def during_refresh():
            heartbeat.append(sum(
                app.parameter_tree.item(item, "text").startswith(gui.UNCHECKED_MARK)
                for item in app.parameter_items.values()
            ))

        self.root.after(1, during_refresh)
        app.select_all_parameters(False)
        self.assertTrue(all(not row["selected"] for row in app.parameter_configs.values()))
        self._wait(lambda: bool(heartbeat))
        self.assertGreater(heartbeat[0], 0)
        self.assertLess(heartbeat[0], 5000, "Selection labels were all updated before Tk could process an event")
        self._wait(lambda: app.parameter_selection_job is None)
        self.assertTrue(all(app.parameter_tree.item(item, "text").startswith(gui.UNCHECKED_MARK)
                            for item in app.parameter_items.values()))
        app.select_all_parameters(True)
        self._wait(lambda: app.parameter_selection_job is None)
        self.assertEqual(sum(bool(row["selected"]) for row in app.parameter_configs.values()), 5000)
        self.assertTrue(all(app.parameter_tree.item(item, "text").startswith(gui.CHECKED_MARK)
                            for item in app.parameter_items.values()))

    def test_progress_and_range_pollers_do_not_submit_during_exit(self):
        app = self._open()
        app.run_ui.run_id = "active-fixture-run"
        app.range_validation_job_id = "active-fixture-range"
        app.progress_inflight = False
        app.range_validation_inflight = False
        with mock.patch.object(app.tasks, "submit") as submit:
            for closing, shutting_down in ((True, False), (False, True)):
                with self.subTest(closing=closing, shutting_down=shutting_down):
                    app.closing = closing
                    app.tasks.shutting_down = shutting_down
                    app._poll_progress()
                    app._poll_range_validation()
                    self.assertFalse(app.progress_inflight)
                    self.assertFalse(app.range_validation_inflight)
            submit.assert_not_called()
        app.closing = False
        app.tasks.shutting_down = False

    def test_failed_exit_keeps_other_work_disabled_and_allows_exit_retry(self):
        app = self._open()
        app.run_ui.run_id = "active-fixture-run"
        app.range_validation_job_id = "active-fixture-range"
        with mock.patch.object(self.service, "exit_optimizer", side_effect=[
                RuntimeError("fixture owned-session cleanup failed"), {"closed": True},
        ]) as exit_service:
            app.request_exit(confirm=False)
            self._wait(lambda: getattr(app, "shutdown_failed", False), timeout=1)
            self.assertFalse(self.destroyed)
            self.assertTrue(app.closing)
            self.assertTrue(app.tasks.shutting_down)
            self.assertIn("重试 Exit", app.status_var.get())
            self.assertIn("fixture owned-session cleanup failed", app.exit_dialog_message.get())
            self.assertEqual(str(app.exit_retry_button.cget("state")), "normal")
            for button in (app.start_button, app.stop_button, app.load_setup_button):
                self.assertEqual(str(button.cget("state")), "disabled")
            with mock.patch.object(app.tasks, "submit") as submit:
                previous_page = app.current_page
                app._poll_progress()
                app._poll_range_validation()
                app.show_page("Optimization")
                app.show_page("Parameters")
                self.assertEqual(app.current_page, previous_page)
                submit.assert_not_called()
            with self.assertRaises(RuntimeError):
                app.tasks.submit(lambda: None)
            app.exit_retry_button.invoke()
            self._wait(lambda: self.destroyed, timeout=1)
            self.assertEqual(exit_service.call_count, 2)
            self.assertTrue(app.cleanup_confirmed)
            self.assertEqual(self.showerror.call_count, 0)


if __name__ == "__main__":
    unittest.main()
