import copy
import os
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.gui_state import RunUiState, candidate_status


class Value(object):
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


def progress_fixture(run_id="fixture-failed", state="failed"):
    root = "/fixture/crescendo runs/" + run_id
    return {
        "status": {
            "run_id": run_id, "state": state, "message": "Remote startup rejected",
            "run_root": root, "total_candidates": 3,
        },
        "spec": {"objectives": [], "constraints": []},
        "candidates": [],
        "execution": {
            "schema_version": "crescendo.execution_progress.v1", "stage": "controller_start",
            "run_root": root, "status_file": root + "/status.json",
            "error_file": root + "/server_error.json",
            "commands": [
                {"kind": "configured_submit", "state": "configured", "label": "Maestro Job Setup",
                 "command": 'batch-submit -q short -R "cpu=6;mem=30000"', "source": "effective job policy"},
                {"kind": "controller", "state": "recorded", "label": "Owned controller",
                 "argv": ["/fixture/python3", "/fixture/a script.py", "--run-root", root],
                 "source": "owned controller command"},
            ],
            "error": {"message": "Remote startup rejected", "traceback_tail": "FixtureError: startup rejected"},
            "workers": [{"worker_index": 0, "status": "failed", "stage": "workspace_prepare"}],
            "log_tails": [{"path": root + "/controller.log", "text": "fixture stdout", "truncated": False}],
        },
    }


class GuiExecutionProgressUnitTests(unittest.TestCase):
    def settings_app(self, generations):
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        app.generations_var = Value(generations)
        app.timeout_var = Value("600")
        app._collect_algorithm_settings = lambda: ("nsga2", {"population": 8})
        app._collect_job_settings = lambda: {"maestro_job_overrides": {}, "maestro_candidate_mode": "auto"}
        return app

    def test_generations_accept_two_through_large_values_without_100_cap(self):
        for value in ("2", "3", "9", "10", "101", "10001"):
            with self.subTest(value=value):
                self.assertEqual(self.settings_app(value)._build_run_settings()["generations"], int(value))

    def test_generations_reject_noninteger_and_values_not_greater_than_one(self):
        for value in ("", "0", "1", "-2", "2.5", "nan", "inf", "word"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "integer greater than 1"):
                    self.settings_app(value)._build_run_settings()

    def test_console_distinguishes_configured_command_from_observed_launch(self):
        payload = progress_fixture()
        text = gui.execution_progress_text(payload["status"], payload["execution"])
        self.assertIn("Configured submission: Maestro Job Setup", text)
        self.assertIn('batch-submit -q short -R "cpu=6;mem=30000"', text)
        self.assertIn("Recorded launch: Owned controller", text)
        self.assertIn("'/fixture/a script.py'", text)
        self.assertIn("Stage: controller_start", text)
        self.assertIn("server_error.json", text)
        self.assertIn("FixtureError: startup rejected", text)
        self.assertNotIn("Scheduler submitted", text)

    def test_console_displays_common_solver_and_submission_as_distinct_layers(self):
        status = {'state': 'running', 'execution_backend': 'maestro_netlist'}
        execution = {'stage': 'solve', 'commands': [
            {'kind': 'solver', 'state': 'completed', 'argv': ['/solver/alps', '/owned/input with spaces.scs',
                '-format', 'ps', '-o', '/owned/psf'], 'returncode': 0},
            {'kind': 'submission', 'state': 'completed', 'argv': ['/batch/batch-submit', '-q', 'chosen', '/owned/run.sh'],
                'returncode': 0, 'job_ids': ['10000005']}]}
        text = gui.execution_progress_text(status, execution)
        self.assertIn('Solver command: solver', text)
        self.assertIn('Submission command: submission', text)
        self.assertIn("'/owned/input with spaces.scs'", text)
        self.assertIn('Solver return: 0', text)
        self.assertIn('Submission return: 0', text)
        self.assertIn('Observed job IDs: 10000005', text)
        self.assertNotIn('Native max jobs', text)

    def test_console_large_diagnostic_payload_is_bounded_and_keeps_tail(self):
        payload = progress_fixture()
        payload["execution"]["log_tails"][0]["text"] = "synthetic progress\n" * 800 + "TAIL_MARKER"
        payload["execution"]["error"]["traceback_tail"] = "synthetic traceback\n" * 800 + "TRACE_MARKER"
        text = gui.execution_progress_text(payload["status"], payload["execution"])
        self.assertLessEqual(len(text), 49152)
        self.assertIn("TAIL_MARKER", text)
        self.assertIn("TRACE_MARKER", text)
        self.assertLess(text.count("synthetic progress"), 300)

    def test_planned_candidate_does_not_claim_submission_or_running(self):
        self.assertEqual(candidate_status({"status": "planned"}), "Planned · Not Submitted")

    def test_retained_native_error_is_visible_after_worker_directory_cleanup(self):
        payload = progress_fixture()
        execution = payload["execution"]
        execution["startup_error_file"] = execution["run_root"] + "/startup_failure.json"
        execution["startup_diagnostic"] = {"stage": "maestro_setup_open",
                                           "native_error": "*Error* Crescendo cannot open Maestro setup"}
        execution["log_tails"] = [{"path": execution["run_root"] + "/deleted/control/ocean.stdout",
                                    "text": "*Error* Crescendo cannot open Maestro setup", "retained": True}]
        text = gui.execution_progress_text(payload["status"], execution)
        self.assertIn("Stage: maestro_setup_open", text)
        self.assertIn("startup_failure.json", text)
        self.assertIn("Native startup error: *Error* Crescendo cannot open Maestro setup", text)
        self.assertIn("retained after cleanup", text)


class FixtureProgressService(object):
    """Only fixture records: never launches EDA or executes displayed commands."""
    def __init__(self):
        self.payloads = {}
        self.progress_calls = []
        self.progress_error = None
        self.progress_entered = None
        self.progress_release = None

    def eda_setup(self):
        return {"mode": "auto", "ok": True, "manual": {}, "effective": {}, "sources": {}}

    def list_runs(self):
        return []

    def progress(self, run_id):
        self.progress_calls.append(run_id)
        if self.progress_entered is not None:
            self.progress_entered.set()
            self.progress_release.wait(3)
        if self.progress_error is not None:
            raise self.progress_error
        return copy.deepcopy(self.payloads[run_id])


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk checks use an existing DISPLAY")
class GuiExecutionProgressTkTests(unittest.TestCase):
    def setUp(self):
        try:
            self.root = gui.tk.Tk()
        except gui.tk.TclError as error:
            self.skipTest("Existing DISPLAY cannot initialize Tk: {}".format(error))
        self.root.withdraw()
        self.service = FixtureProgressService()
        self.app = None
        self.callback_errors = []
        self.addCleanup(self._close)
        self.error_patch = mock.patch.object(gui.messagebox, "showerror")
        self.showerror = self.error_patch.start()
        self.addCleanup(self.error_patch.stop)
        self.info_patch = mock.patch.object(gui.messagebox, "showinfo")
        self.showinfo = self.info_patch.start()
        self.addCleanup(self.info_patch.stop)
        self.app = gui.OptimizerGui(self.root, self.service)
        self.root.report_callback_exception = (
            lambda exception, value, trace: self.callback_errors.append((exception, value))
        )
        self.root.update_idletasks()

    def _close(self):
        if self.service.progress_release is not None:
            self.service.progress_release.set()
        if self.app is not None:
            self.app.tasks.close()
            for name in ("worker", "control_worker", "history_worker", "exit_worker", "shutdown_worker"):
                worker = getattr(self.app.tasks, name, None)
                if worker is not None:
                    worker.join(1)
        for identifier in self.root.tk.splitlist(self.root.tk.call("after", "info")):
            self.root.after_cancel(identifier)
        self.root.destroy()

    def _wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.root.update()
            if self.callback_errors:
                self.fail("Tk callback failed: {}".format(self.callback_errors))
            if predicate():
                return
            time.sleep(0.005)
        self.fail("GUI fixture did not reach expected state")

    def test_console_is_readonly_after_update_and_inside_settings(self):
        payload = progress_fixture()
        self.app._render_execution_console(payload["status"], payload["execution"])
        console = self.app.execution_console
        self.assertEqual(str(console.cget("state")), "disabled")
        before = console.get("1.0", "end-1c")
        console.insert("1.0", "unexpected edit")
        self.assertEqual(console.get("1.0", "end-1c"), before)
        self.assertIn("Configured submission: Maestro Job Setup", before)
        self.assertFalse(self.app._widget_is_descendant(console, self.app.pages["Optimization"]))
        self.assertTrue(self.app._widget_is_descendant(console, self.app.pages["EDA Settings"]))
        self.assertFalse(self.app._widget_is_descendant(self.app.job_policy_tree, self.app.pages["Optimization"]))
        self.assertTrue(self.app._widget_is_descendant(self.app.job_policy_tree, self.app.pages["EDA Settings"]))
        self.assertIs(self.app.settings_sections["crescendo"].pack_slaves()[-1], console.master.master)

    def test_failed_run_with_no_candidates_has_persistent_banner_and_one_popup(self):
        payload = progress_fixture()
        self.service.payloads["fixture-failed"] = payload
        self.app.run_ui.started("fixture-failed")
        self.app._poll_progress()
        self._wait(lambda: self.app.run_ui.state == "failed")
        self.assertEqual(self.app.live_candidates, [])
        self.assertIn("Remote startup rejected", self.app.results_failure_var.get())
        self.assertIn("server_error.json", self.app.results_failure_var.get())
        self.assertIn("Remote startup rejected", self.app.execution_console.get("1.0", "end"))
        self.assertEqual(self.showerror.call_count, 1)
        enriched = copy.deepcopy(payload["status"])
        enriched["message"] += " (diagnostic details recorded)"
        self.app._render_run_failure(enriched, payload["execution"])
        self.assertEqual(self.showerror.call_count, 1)
        self.assertEqual(str(self.app.stop_button.cget("state")), "disabled")

    def test_view_recent_failed_run_does_not_require_checkpoint(self):
        run_id = "no-checkpoint"
        self.service.payloads[run_id] = progress_fixture(run_id)
        self.app.recent_tree.insert("", "end", iid=run_id, text=run_id)
        self.app.recent_tree.selection_set(run_id)
        self.app.recent_run_resumable[run_id] = False
        self.app.view_recent_run()
        self._wait(lambda: self.app.run_ui.state == "failed")
        self.assertEqual(self.service.progress_calls, [run_id])
        self.assertEqual(self.app.current_page, "Optimization")
        self.assertIn(run_id, self.app.execution_console.get("1.0", "end"))
        self.assertEqual(self.showinfo.call_count, 0)

    def test_startup_cause_and_retained_diagnostic_path_reach_failed_banner(self):
        payload = progress_fixture()
        payload["status"]["message"] = "PersistentMaestroWorkerStartupError: bootstrap failed"
        payload["execution"]["startup_error_file"] = payload["execution"]["run_root"] + "/startup_failure.json"
        payload["execution"]["startup_diagnostic"] = {
            "stage": "job_policy_override", "native_error": "*Error* native policy API rejected command"}
        self.service.payloads["fixture-failed"] = payload
        self.app.run_ui.started("fixture-failed")
        self.app._poll_progress()
        self._wait(lambda: self.app.run_ui.state == "failed")
        self.assertIn("native policy API rejected command", self.app.results_failure_var.get())
        self.assertIn("startup_failure.json", self.app.results_failure_var.get())
        self.assertIn("job_policy_override", self.app.execution_console.get("1.0", "end"))
        self.assertEqual(self.showerror.call_count, 1)

    def test_view_other_recent_run_preserves_active_run_and_candidate_state(self):
        self.app.run_ui.started("active")
        self.app.run_ui.request_stop()
        self.app.live_candidates = [{"candidate_id": "c000001", "status": "planned"}]
        self.app.recent_tree.insert("", "end", iid="other", text="other")
        self.app.recent_tree.selection_set("other")
        self.app.view_recent_run()
        self.assertEqual(self.app.run_ui.run_id, "active")
        self.assertEqual(self.app.run_ui.state, "stopping")
        self.assertTrue(self.app.run_ui.stop_requested)
        self.assertEqual(len(self.app.live_candidates), 1)
        self.assertEqual(self.service.progress_calls, [])
        self.assertIn("active run", self.app.status_var.get())

    def test_view_same_active_run_keeps_stop_request_and_existing_candidates(self):
        self.service.payloads["active"] = progress_fixture("active", "running")
        self.app.run_ui.started("active")
        self.app.run_ui.request_stop()
        self.app.live_candidates = [{"candidate_id": "c000001", "status": "planned"}]
        self.app.progress_inflight = True
        self.app.recent_tree.insert("", "end", iid="active", text="active")
        self.app.recent_tree.selection_set("active")
        self.app.view_recent_run()
        self.assertEqual(self.app.run_ui.state, "stopping")
        self.assertTrue(self.app.run_ui.stop_requested)
        self.assertEqual(len(self.app.live_candidates), 1)

    def test_progress_load_error_is_visible_and_releases_inflight_for_retry(self):
        self.service.progress_error = OSError("fixture status read denied")
        self.app.run_ui.started("active")
        self.app._poll_progress()
        self._wait(lambda: not self.app.progress_inflight and self.service.progress_calls)
        self.assertIn("Progress unavailable", self.app.run_state_var.get())
        self.assertIn("fixture status read denied", self.app.status_var.get())
        self.assertTrue(self.root.tk.splitlist(self.root.tk.call("after", "info")))

    def test_switch_recent_terminal_run_while_old_poll_is_inflight(self):
        self.service.payloads["old"] = progress_fixture("old", "completed")
        self.service.payloads["new"] = progress_fixture("new", "failed")
        self.service.progress_entered = threading.Event()
        self.service.progress_release = threading.Event()
        self.app.run_ui.started("old")
        self.app.run_ui.update("completed")
        self.app._poll_progress()
        self.assertTrue(self.service.progress_entered.wait(10))
        self.assertTrue(self.app.progress_inflight)
        self.app.recent_tree.insert("", "end", iid="new", text="new")
        self.app.recent_tree.selection_set("new")
        self.app.view_recent_run()
        self.assertEqual(self.app.run_ui.run_id, "new")
        self.service.progress_release.set()
        self._wait(lambda: self.app.run_ui.state == "failed")
        self.assertEqual(self.service.progress_calls, ["old", "new"])
        self.assertIn("/fixture/crescendo runs/new", self.app.results_failure_var.get())
        self.assertFalse(self.app.progress_inflight)


if __name__ == "__main__":
    unittest.main()
