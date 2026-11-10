"""Only the new environment-test UI: fixtures never launch native jobs."""
import copy
import gc
import json
import os
import threading
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine import gui


class Value(object):
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


class FakeTk(object):
    def call(self, *arguments):
        return "x11" if arguments == ("tk", "windowingsystem") else "8.5.13"


class EnvironmentTestInputTests(unittest.TestCase):
    def app(self):
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        app.root = mock.Mock(tk=FakeTk())
        app.prepared_id = "prepared-fixture"
        app._require_prepared_selection = mock.Mock()
        app._local_range_report = lambda: {"variables": [{"parameter_id": "/M/w", "minimum": 1e-6, "maximum": 2e-6}], "issues": []}
        app._collect_objectives = lambda: [{"output_key": "gain", "aggregation": "min"}]
        app._collect_constraints = lambda: [{"output_key": "power", "aggregation": "max"}]
        app.simulator_var = Value("alps")
        app.execution_backend_var = Value("private_netlist")
        app.solver_command_vars = {name: Value(gui.DEFAULT_SOLVER_TEMPLATES[name]) for name in ("spectre", "alps")}
        app.submission_command_var = Value("batch-submit -q actual {job_script}")
        app.timeout_var = Value("345")
        app._collect_job_settings = lambda: {"maestro_job_overrides": {"command": 'batch-submit -q short -R "cpu=6;mem=20000"', "max_jobs": 50}, "maestro_candidate_mode": "batch"}
        app.data_root_override_var = Value(True)
        app.data_root_var = Value("/fixture/simulation with spaces")
        app._build_run_settings = mock.Mock(side_effect=AssertionError("Environment tests must not parse generations/population"))
        return app

    def test_snapshot_reuses_current_ranges_metrics_command_and_storage_without_population_rules(self):
        app = self.app()
        spec = app._collect_environment_test_spec()
        self.assertEqual(spec["simulator"], "alps")
        self.assertEqual(spec["execution_backend"], "private_netlist")
        self.assertEqual(spec["execution_commands"]["solver_templates"], gui.DEFAULT_SOLVER_TEMPLATES)
        self.assertEqual(spec["execution_commands"]["submission_template"], "batch-submit -q actual {job_script}")
        self.assertEqual(spec["candidate_timeout_seconds"], 345)
        self.assertEqual(spec["maestro_job_overrides"]["max_jobs"], 50)
        self.assertIn("batch-submit -q short", spec["maestro_job_overrides"]["command"])
        self.assertEqual(spec["data_root_override"], "/fixture/simulation with spaces")
        self.assertEqual(spec["objectives"][0]["aggregation"], "min")
        self.assertEqual(spec["constraints"][0]["aggregation"], "max")
        self.assertEqual(spec["gui_runtime"]["tk_version"], "8.5.13")
        self.assertTrue(spec["gui_runtime"]["initialized"])
        self.assertNotIn("generations", spec)
        self.assertNotIn("algorithm_parameters", spec)
        app._build_run_settings.assert_not_called()

    def test_diagnostic_size_snapshot_and_invalid_inputs(self):
        app = self.app()
        app.diagnostic_generations_var = Value("3")
        app.diagnostic_total_var = Value("12")
        spec = app._collect_environment_test_spec()
        self.assertEqual(spec["diagnostic_generations"], 3)
        self.assertEqual(spec["diagnostic_total_candidates"], 12)
        self.assertNotIn("diagnostic_samples", spec)
        for generations, total in (("3", "11"), ("0", "6"), ("1", "17"), ("5", "65"), ("2.0", "6"), ("", "6")):
            app.diagnostic_generations_var = Value(generations)
            app.diagnostic_total_var = Value(total)
            with self.assertRaises(ValueError):
                app._collect_environment_test_spec()

    def test_invalid_draft_inputs_become_warnings_and_validity_flags_not_environment_blockers(self):
        app = self.app()
        app._require_prepared_selection = mock.Mock(side_effect=ValueError("changed TB"))
        app._local_range_report = lambda: {"variables": [], "issues": [{"message": "unfinished width"}]}
        app._collect_objectives = mock.Mock(side_effect=ValueError("unfinished target"))
        app._collect_job_settings = mock.Mock(side_effect=ValueError("invalid max jobs"))
        app.timeout_var = Value("unfinished")
        app.data_root_var = Value("")
        spec = app._collect_environment_test_spec()
        self.assertFalse(spec["prepared_selection_valid"])
        self.assertFalse(spec["job_settings_valid"])
        self.assertFalse(spec["storage_settings_valid"])
        self.assertEqual(spec["candidate_timeout_seconds"], 600)
        self.assertEqual(spec["objectives"], [])
        self.assertGreaterEqual(len(spec["ui_input_warnings"]), 5)
        self.assertIn("unfinished width", " ".join(spec["ui_input_warnings"]))
        self.assertEqual(spec["constraints"][0]["output_key"], "power")

    def test_timeout_blank_is_unlimited_and_zero_does_not_launch_with_a_fallback(self):
        app = self.app()
        app.timeout_var = Value("")
        self.assertEqual(app._collect_environment_test_spec()["candidate_timeout_seconds"], -1)
        app.timeout_var = Value("0")
        spec = app._collect_environment_test_spec()
        self.assertFalse(spec["timeout_settings_valid"])
        self.assertFalse(spec["job_settings_valid"])
        self.assertIn("zero is not allowed", " ".join(spec["ui_input_warnings"]))

    def test_source_auto_means_maestro_and_invalid_selection_has_no_fallback(self):
        app = self.app()
        app.execution_backend_var = Value('auto')
        self.assertEqual(app._collect_environment_test_spec()['execution_backend'], 'maestro_netlist')
        app.execution_backend_var = Value('unknown_source')
        with self.assertRaisesRegex(ValueError, 'Unknown diagnostic netlist source'):
            app._collect_environment_test_spec()


class EnvironmentTestService(object):
    def __init__(self):
        self.starts = []
        self.stops = []
        self.progress_calls = []
        self.progress_entered = threading.Event()
        self.progress_release = threading.Event()
        self.progress_release.set()
        self.start_error = None
        self.runs = []
        self.payload = {
            "test_id": "environment-fixture", "state": "running",
            "main_log": "/fixture/simulation/crescendo/TB/_environment_tests/test/environment_test.log",
            "log_tail": "Initial fixture report\n",
            "checks": [{"check_id": "python", "name": "Python runtime", "category": "environment", "status": "PASS", "message": "Python 3.6 fixture"},
                       {"check_id": "design", "name": "Design execution", "category": "Maestro", "status": "BLOCKED", "message": "No design was loaded"}],
            "summary": {"PASS": 1, "BLOCKED": 1},
            "run_ids": {"alps.maestro_netlist": "owned-test-run-alps-maestro_netlist"},
            "case_results": [{"case_id": sim + "." + engine, "simulator": sim, "engine": engine,
                              "state": "running", "stop_proven": None}
                for sim, engine in [("alps", "maestro_netlist")]],
            "simulator_logs": {
                "spectre": "/fixture/simulation/crescendo/TB/_environment_tests/test/environment_test.log",
                "alps": "/fixture/simulation/crescendo/TB/_environment_tests/test/environment_test.log",
            },
            "simulator_log_tails": {
                "spectre": "spectre.maestro_netlist SPECTRE_MAESTRO_MARKER\nspectre.private_netlist SPECTRE_PRIVATE_MARKER",
                "alps": "alps.maestro_netlist ALPS_MAESTRO_MARKER\nalps.private_netlist ALPS_PRIVATE_MARKER",
            },
            "engine_logs": {
                "persistent_maestro": "/fixture/simulation/crescendo/TB/_environment_tests/test/maestro.log",
                "private_netlist": "/fixture/simulation/crescendo/TB/_environment_tests/test/private_netlist.log",
            },
            "engine_log_tails": {"persistent_maestro": "MAESTRO_REPORT_MARKER", "private_netlist": "SPECTRE_REPORT_MARKER"},
        }
        self.payload["case_run_ids"] = copy.deepcopy(self.payload["run_ids"])

    def eda_setup(self):
        return {"mode": "auto", "ok": True, "manual": {}, "effective": {}, "sources": {}}

    def list_runs(self):
        return copy.deepcopy(self.runs)

    def start_environment_test(self, prepared_id, spec):
        self.starts.append((prepared_id, copy.deepcopy(spec)))
        if self.start_error:
            raise self.start_error
        return {"test_id": self.payload["test_id"], "state": "queued", "main_log": self.payload["main_log"],
                "engine_logs": self.payload["engine_logs"], "simulator_logs": self.payload["simulator_logs"],
                "case_run_ids": self.payload["case_run_ids"], "case_results": self.payload["case_results"]}

    def environment_test_progress(self, test_id):
        self.progress_calls.append(test_id)
        self.progress_entered.set()
        self.progress_release.wait(8)
        return copy.deepcopy(self.payload)

    def stop_environment_test(self, test_id):
        self.stops.append(test_id)
        self.payload["state"] = "cancelled"
        self.progress_release.set()
        return {"state": "cancelling"}

    def exit_optimizer(self):
        self.progress_release.set()
        return {"closed": True}


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk checks require an existing DISPLAY")
class EnvironmentTestTkTests(unittest.TestCase):
    def setUp(self):
        self.previous_gc = gc.isenabled()
        gc.disable()
        self.root = gui.tk.Tk()
        self.root.withdraw()
        self.service = EnvironmentTestService()
        self.app = None
        self.releases = []
        self.errors = []
        self.addCleanup(self.close)
        patch = mock.patch.object(gui.messagebox, "showerror")
        patch.start()
        self.addCleanup(patch.stop)
        self.app = gui.OptimizerGui(self.root, self.service)
        self.root.report_callback_exception = lambda kind, value, trace: self.errors.append((kind, value))
        self.wait(lambda: self.app.tasks.tasks.empty())

    def close(self):
        self.service.progress_release.set()
        for release in self.releases:
            release.set()
        if self.app:
            self.app.tasks.close()
            for name in ("worker", "control_worker", "history_worker", "optional_worker", "export_worker", "monitor_worker", "exit_worker"):
                worker = getattr(self.app.tasks, name, None)
                if worker:
                    worker.join(1)
        for identifier in self.root.tk.splitlist(self.root.tk.call("after", "info")):
            self.root.after_cancel(identifier)
        self.root.destroy()
        if self.previous_gc:
            gc.enable()

    def wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.root.update()
            if self.errors:
                self.fail("Unexpected Tk callback errors: {}".format(self.errors))
            if predicate():
                return
            time.sleep(.003)
        self.fail("Fixture did not reach expected state")

    def test_status_column_fits_codes_using_tree_font(self):
        payload = copy.deepcopy(self.service.payload)
        self.app._render_environment_test_progress(payload)
        tree = self.app.environment_test_tree
        font = gui.ttk.Style(tree).lookup("Treeview", "font") or "TkDefaultFont"
        labels = [tree.item(item, "values")[0] for item in self.app.environment_test_checks]
        self.assertTrue(any("CRESCENDO-" in label for label in labels))
        width = int(tree.column("status", "width"))
        for label in labels:
            self.assertGreaterEqual(width, int(tree.tk.call("font", "measure", font, label)) + 20)

    def test_selected_diagnostic_uses_real_results_without_replacing_formal_run(self):
        app = self.app
        self.root.deiconify()
        app.run_ui.started("formal-run")
        app.environment_test_id = "environment-fixture"
        payload = copy.deepcopy(self.service.payload)
        payload["case_run_ids"] = {"alps.private_netlist": "diag-private", "alps.maestro_netlist": "diag-native"}
        payload["case_results"][0]["timing"] = {"available": True, "total_seconds": 9.5,
            "phases_seconds": {"readiness": 1.0, "execution": 8.5}, "active_phase": "execution"}
        rows = [{"candidate_id": "g000_c0000", "generation": 0, "status": "completed",
                 "metrics": {"feasible": True, "objectives": [{"value": 2.5}]}},
                {"candidate_id": "g001_c0000", "generation": 1, "status": "completed",
                 "metrics": {"feasible": True, "objectives": [{"value": 4.0}]}}]
        self.service.progress = mock.Mock(return_value={"spec": {"objectives": [{"output_key": "gain"}]}, "candidates": rows})
        app._render_environment_test_progress(payload)
        self.assertIn("总计 9.5s", app.diagnostic_timing_var.get())
        self.assertIn("execution 8.5s", app.diagnostic_timing_var.get())
        app.diagnostic_case_var.set("alps.private_netlist")
        app.show_diagnostic_results()
        self.wait(lambda: app.display_dataset is not None)
        self.root.update()
        self.assertTrue(app.result_trend.winfo_ismapped())
        self.assertTrue(app.result_tree.winfo_ismapped())
        self.assertEqual(app.run_ui.run_id, "formal-run")
        self.service.progress.assert_called_once_with("diag-private")
        self.assertEqual(len(app.result_tree.get_children()), 2)
        self.assertIn("2.5", str(app.result_tree.item("g000_c0000", "values")))
        self.assertEqual(len(app.result_trend.find_all()), 9)
        self.assertIn("alps.private_netlist", app.results_failure_var.get())

    def test_diagnostic_summary_exposes_exact_reader_failure_stage_and_solver(self):
        payload = copy.deepcopy(self.service.payload)
        payload['state'] = 'failed'
        payload['case_results'][0].update(state='failed', first_cause_layer='measurement',
            first_cause={'failure_stage': 'result_reader_parameter_metadata',
                         'message': 'Current ALPS result has no required design parameters'})
        self.app._render_environment_test_progress(payload)
        summary = self.app.environment_test_status_var.get()
        self.assertIn('ALPS', summary)
        self.assertIn('Current ALPS result has no required design parameters', summary)
        self.assertEqual(len(self.app.environment_engine_log_vars), 1)

    def test_native_parameter_item_shows_point_dpar_scs_and_hides_after_deselect(self):
        payload = copy.deepcopy(self.service.payload)
        payload['checks'] = [{'id': 'alps.maestro_netlist.parameter_application',
            'check_id': 'parameter_application', 'status': 'PASS', 'simulator': 'alps',
            'engine': 'maestro_netlist', 'details': {'proof_kind': 'native_run_device_parameters',
                'ok': True, 'private_oa_backannotation_proven': False,
                'parameters': [{'parameter_id': 'nfin', 'requested': 5, 'effective': 5,
                    'points': [{'test': 'main', 'corner': 'SS', 'point_id': 3,
                        'requested': 5, 'effective': 5, 'dpar_status': 'resolved_from_native_run',
                        'scs_path': '/fixture/owned/SS/netlist/input.scs'}]}]}}]
        self.app._render_environment_test_progress(payload)
        item = next(iter(self.app.environment_test_checks))
        self.app.environment_test_tree.selection_set(item)
        self.app.environment_test_tree.event_generate('<<TreeviewSelect>>')
        self.root.update()
        body = self.app.environment_test_detail_text.get('1.0', 'end')
        for expected in ('Corner=SS', 'effective=5', 'resolved_from_native_run',
                         '/fixture/owned/SS/netlist/input.scs'):
            self.assertIn(expected, body)
        self.app._clear_environment_test_selection()
        self.root.update()
        self.assertEqual(self.app.environment_test_detail_frame.winfo_manager(), '')

    def test_diagnostic_inputs_update_validate_lock_and_unlock(self):
        app = self.app
        self.assertIn("每代 3 个", app.diagnostic_size_var.get())
        app.diagnostic_generations_var.set("3")
        app.diagnostic_total_var.set("12")
        self.assertIn("每代 4 个", app.diagnostic_size_var.get())
        self.assertIn("三模式合计 36 个", app.diagnostic_size_var.get())
        app.diagnostic_total_var.set("11")
        app.start_environment_test()
        self.assertEqual(self.service.starts, [])
        self.assertIn("整除", app.environment_test_status_var.get())
        app.diagnostic_total_var.set("12")
        app.start_environment_test()
        self.assertEqual(str(app.diagnostic_total_entry["state"]), "disabled")
        self.assertEqual(str(app.diagnostic_generations_entry["state"]), "disabled")
        self.wait(lambda: bool(self.service.starts))
        self.assertEqual(self.service.starts[0][1]["diagnostic_total_candidates"], 12)
        self.service.payload["state"] = "completed"
        self.wait(lambda: app.environment_test_state == "completed", timeout=5)
        self.assertEqual(str(app.diagnostic_total_entry["state"]), "normal")
        self.assertEqual(str(app.diagnostic_generations_entry["state"]), "normal")

    def test_environment_only_start_preserves_formal_run_and_ignores_generation_draft(self):
        app = self.app
        app.generations_var.set("unfinished generations")
        app.algorithm_parameter_vars[("differential_evolution", "population")].set("unfinished population")
        app.run_ui.started("selected-normal-run")
        app.live_candidates = [{"candidate_id": "formal-candidate", "status": "running"}]
        app.start_button.configure(state="disabled")
        app.stop_button.configure(state="normal")
        app.start_environment_test()
        self.wait(lambda: app.environment_test_state == "running")
        self.assertIsNone(self.service.starts[0][0])
        self.assertEqual(app.run_ui.run_id, "selected-normal-run")
        self.assertEqual(app.live_candidates[0]["candidate_id"], "formal-candidate")
        self.assertEqual(str(app.start_button.cget("state")), "disabled")
        self.assertEqual(str(app.stop_button.cget("state")), "normal")
        self.assertEqual(app.generations_var.get(), "unfinished generations")
        self.assertEqual(app.algorithm_parameter_vars[("differential_evolution", "population")].get(), "unfinished population")
        statuses = [app.environment_test_tree.item(item, "values")[0] for item in app.environment_test_checks]
        self.assertRegex(statuses[0], r"^前置阻塞 · CRESCENDO-[A-Z0-9-]+$")
        self.assertEqual(statuses[1], "通过")
        self.assertIn("owned-test-run", app.environment_test_runs_var.get())
        app.copy_environment_test_log_path()
        self.assertEqual(self.root.clipboard_get(), self.service.payload["main_log"])

    def test_normal_work_queue_block_does_not_block_test_start_or_control_cancel(self):
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        self.app.tasks.submit(lambda: (entered.set(), release.wait(8)))
        self.wait(entered.is_set)
        self.app.start_environment_test()
        self.wait(lambda: self.app.environment_test_state == "running")
        self.service.progress_entered.clear()
        self.service.progress_release.clear()
        self.app._poll_environment_test()
        self.wait(self.service.progress_entered.is_set)
        self.app.stop_environment_test()
        self.wait(lambda: bool(self.service.stops))
        self.wait(lambda: self.app.environment_test_state == "cancelled")
        self.assertEqual(self.service.stops, ["environment-fixture"])
        self.assertFalse(release.is_set())
        self.assertEqual(str(self.app.environment_test_button.cget("state")), "normal")

    def test_report_tail_updates_in_read_only_window_and_retains_terminal_report(self):
        self.app.start_environment_test()
        self.wait(lambda: self.app.environment_test_state == "running")
        window = self.app.show_environment_test_log()
        self.wait(lambda: "Initial fixture report" in self.app.environment_test_log_text.get("1.0", "end"))
        self.service.payload.update(state="completed", log_tail="noise\n" * 15000 + "FINAL_REPORT_MARKER")
        self.service.payload["checks"].append({"check_id": "batch-query", "name": "Scheduler query", "category": "cluster", "status": "NOT_TESTED", "message": "Scheduler CLI missing in fixture"})
        self.app._poll_environment_test()
        self.wait(lambda: self.app.environment_test_state == "completed")
        text = self.app.environment_test_log_text.get("1.0", "end")
        self.assertIn("FINAL_REPORT_MARKER", text)
        self.assertLess(len(text), 65700)
        self.assertEqual(str(self.app.environment_test_log_text.cget("state")), "disabled")
        self.assertIsNone(self.app.environment_test_poll_job)
        self.assertEqual(str(self.app.environment_test_log_button.cget("state")), "normal")
        self.assertIn("尚未验证", [self.app.environment_test_tree.item(item, "values")[0] for item in self.app.environment_test_checks])
        close_command = window.protocol("WM_DELETE_WINDOW")
        self.root.tk.call(close_command)
        self.assertIsNone(self.app.environment_test_log_text)

    def test_start_failure_is_local_to_test_card_and_can_retry(self):
        self.service.start_error = RuntimeError("fixture storage permission denied")
        normal_state = str(self.app.start_button.cget("state"))
        self.app.start_environment_test()
        self.wait(lambda: self.app.environment_test_state == "failed")
        self.assertIn("storage permission denied", self.app.environment_test_status_var.get())
        self.assertEqual(str(self.app.environment_test_button.cget("state")), "normal")
        self.assertEqual(str(self.app.start_button.cget("state")), normal_state)
        self.service.start_error = None
        self.app.start_environment_test()
        self.wait(lambda: self.app.environment_test_state == "running")
        self.assertEqual(len(self.service.starts), 2)

    def test_selected_case_single_report_can_open_and_copy_while_running(self):
        markers = ["ALPS_MAESTRO_MARKER"]
        self.service.payload["log_tail"] = "\n".join(markers)
        self.app.start_environment_test()
        self.wait(lambda: self.app.environment_test_state == "running")
        self.assertEqual(list(self.app.environment_engine_log_vars), ["main"])
        self.assertEqual(self.app.environment_engine_log_vars["main"].get(), self.service.payload["main_log"])
        button = self.app.environment_engine_log_buttons["main"]
        self.assertEqual(str(button.cget("state")), "normal")
        button.invoke()
        self.wait(lambda: all(marker in self.app.environment_test_log_text.get("1.0", "end") for marker in markers))
        self.assertIn("诊断总日志", self.app.environment_test_window.title())
        for key in (None, "main", "spectre", "alps"):
            self.app.copy_environment_test_log_path(key)
            self.assertEqual(self.root.clipboard_get(), self.service.payload["main_log"])
        self.app.copy_environment_test_log_paths()
        self.assertEqual(self.root.clipboard_get(), self.service.payload["main_log"])
        self.assertEqual(len(self.app.environment_test_payload["case_results"]), 1)
        for case_id, run_id in self.service.payload["case_run_ids"].items():
            self.assertIn(case_id, self.app.environment_test_runs_var.get())
            self.assertIn(run_id, self.app.environment_test_runs_var.get())
        with mock.patch.object(self.app, "_show_text_details") as show:
            self.app.show_environment_test_details()
        self.assertIn("选择问题条目", show.call_args[0][1])
        self.assertIsNone(self.app.run_ui.run_id)
        self.assertEqual(self.app.live_candidates, [])
        self.assertEqual(self.app.pareto_ids, set())

    def problem_payload(self):
        payload = copy.deepcopy(self.service.payload)
        payload["state"] = "failed"
        case_id = "alps.maestro_netlist"
        payload["checks"] = [
            {"id": case_id + ".execution", "check_id": "execution", "simulator": "alps", "engine": "maestro_netlist",
             "status": "FAIL", "message": "Netlist/solver/submission pipeline did not qualify; first failure preserved",
             "details": {"path": "/fixture/owned/preparation_failure.json"}},
            {"id": case_id + ".metrics", "check_id": "metrics", "simulator": "alps", "engine": "maestro_netlist",
             "status": "NOT_TESTED", "message": "No native evidence available for this stage"},
            {"id": "spectre.private_netlist.execution", "check_id": "execution", "simulator": "spectre", "engine": "private_netlist",
             "status": "FAIL", "message": "OTHER_CASE_FAILURE", "details": {}},
        ]
        payload["case_contexts"] = {
            case_id: {"case_id": case_id, "run_root": "/fixture/owned", "log_path": "/fixture/ALPS/environment_test.log",
                      "first_cause": {"failure_stage": "native_capture", "message": "CAPTURE_CURRENT_FAILURE"},
                      "netlists": [{"path": "/fixture/owned/capture/netlist/input.scs", "kind": "原始 Maestro 网表", "status": "retained"}]},
            "alps.private_netlist": {"case_id": "alps.private_netlist", "run_root": "/fixture/private",
                      "netlists": [{"path": "/fixture/private/tasks/point/netlist/input.scs", "kind": "私有网表", "status": "retained"}]},
        }
        return payload

    def test_failure_selection_shows_human_cause_and_two_netlist_sources_and_hides_when_cleared(self):
        app = self.app
        self.assertEqual(app.environment_test_detail_frame.winfo_manager(), "")
        app._render_environment_test_progress(self.problem_payload())
        item = next(item for item, row in app.environment_test_checks.items() if row.get("id") == "alps.maestro_netlist.metrics")
        tree = app.environment_test_tree
        tree.selection_set(item)
        tree.event_generate("<<TreeviewSelect>>")
        self.root.update()
        self.assertEqual(app.environment_test_detail_frame.winfo_manager(), "pack")
        body = app.environment_test_detail_text.get("1.0", "end")
        for wanted in ("CAPTURE_CURRENT_FAILURE", "/fixture/ALPS/environment_test.log",
                       "/fixture/owned/capture/netlist/input.scs", "/fixture/private/tasks/point/netlist/input.scs"):
            self.assertIn(wanted, body)
        self.assertIn("未执行", tree.item(item, "values")[0])
        self.assertEqual(str(app.environment_test_detail_text.cget("state")), "disabled")
        tree.selection_remove(item)
        tree.event_generate("<<TreeviewSelect>>")
        self.root.update()
        self.assertEqual(app.environment_test_detail_frame.winfo_manager(), "")

    def test_failures_and_blockers_sort_first_red_and_success_sections_default_collapsed_green(self):
        app = self.app
        payload = self.problem_payload()
        payload["checks"].insert(0, {"id": "python", "check_id": "python", "status": "PASS", "message": "fixture runtime"})
        payload["checks"].insert(1, {"id": "alps.simulator.readiness", "check_id": "simulator.readiness", "simulator": "alps", "status": "BLOCKED", "message": "fixture blocked"})
        app._render_environment_test_progress(payload)
        tree = app.environment_test_tree
        leaves = list(app.environment_test_checks)
        self.assertEqual([app.environment_test_checks[key]["status"] for key in leaves], ["FAIL", "FAIL", "BLOCKED", "NOT_TESTED", "PASS"])
        first_group = tree.get_children()[0]
        self.assertIn("problem", tree.item(first_group, "tags"))
        self.assertEqual(str(tree.tag_configure("problem", "foreground")), gui.DANGER)
        for item in leaves:
            row = app.environment_test_checks[item]
            if row["status"] in ("FAIL", "BLOCKED"):
                self.assertIn("problem", tree.item(item, "tags"))
            elif row["status"] == "PASS":
                self.assertIn("success", tree.item(item, "tags"))
                section = tree.parent(item)
                self.assertIn("success", tree.item(section, "tags"))
                self.assertFalse(tree.item(section, "open"))
                tree.item(section, open=True)
                app._render_environment_test_progress(payload)
                self.assertTrue(tree.item(section, "open"))
                break
        self.assertEqual(str(tree.tag_configure("success", "foreground")), gui.SUCCESS)

    def test_poll_refresh_preserves_selected_problem_and_collapsed_sections(self):
        app = self.app
        payload = self.problem_payload()
        app._render_environment_test_progress(payload)
        item = next(item for item, row in app.environment_test_checks.items() if row.get("id") == "alps.maestro_netlist.execution")
        tree = app.environment_test_tree
        section = tree.parent(item)
        group = tree.parent(section)
        tree.selection_set(item)
        tree.item(section, open=False)
        tree.item(group, open=False)
        app._render_environment_test_progress(payload)
        self.root.update()
        self.assertEqual(tree.selection(), (item,))
        self.assertFalse(tree.item(section, "open"))
        self.assertFalse(tree.item(group, "open"))
        self.assertEqual(app.environment_test_detail_frame.winfo_manager(), "pack")
        app._clear_environment_test_selection()
        self.root.update()
        self.assertEqual(tree.selection(), ())
        self.assertEqual(app.environment_test_detail_frame.winfo_manager(), "")

    def test_copy_subsection_keeps_first_cause_and_excludes_other_case_failure(self):
        app = self.app
        app._render_environment_test_progress(self.problem_payload())
        item = next(item for item, row in app.environment_test_checks.items() if row.get("id") == "alps.maestro_netlist.metrics")
        app.environment_test_tree.selection_set(item)
        self.root.clipboard_clear()
        self.root.clipboard_append("waiting for subsection")
        app.copy_environment_test_section()
        self.wait(lambda: "CAPTURE_CURRENT_FAILURE" in self.root.clipboard_get())
        copied = self.root.clipboard_get()
        self.assertIn("CAPTURE_CURRENT_FAILURE", copied)
        self.assertNotIn("OTHER_CASE_FAILURE", copied)
        self.assertIn("SECTION", copied)
        app.copy_environment_test_netlist_paths()
        self.assertEqual(self.root.clipboard_get().splitlines(), ["/fixture/owned/capture/netlist/input.scs", "/fixture/private/tasks/point/netlist/input.scs"])

    def test_copy_reads_complete_disk_section_with_evidence_not_only_progress_snapshot(self):
        app = self.app
        payload = self.problem_payload()
        with tempfile.TemporaryDirectory(prefix="crescendo-ui-section-") as folder:
            path = os.path.join(folder, "environment_test.log")
            raw = "=== SECTION BEGIN alps.maestro_netlist/results ===\nDISK_ONLY_PRIMARY_EVIDENCE\n=== SECTION END alps.maestro_netlist/results ===\n"
            with open(path, "w", encoding="utf-8") as stream:
                stream.write(raw)
            payload["case_contexts"]["alps.maestro_netlist"]["log_path"] = path
            app._render_environment_test_progress(payload)
            item = next(item for item, row in app.environment_test_checks.items() if row.get("check_id") == "metrics")
            app.environment_test_tree.selection_set(item)
            self.root.clipboard_clear()
            self.root.clipboard_append("waiting for disk evidence")
            app.copy_environment_test_section()
            self.wait(lambda: "DISK_ONLY_PRIMARY_EVIDENCE" in self.root.clipboard_get())
            self.assertIn(raw, self.root.clipboard_get())
            self.assertNotIn("尚无完整落盘", self.root.clipboard_get())

    def test_recent_local_runs_exclude_only_flagged_environment_runs_and_preserve_selection(self):
        self.service.runs = [
            {"run_id": "test-top-level", "environment_test": {"test_id": "environment-fixture"}, "checkpoint_available": True},
            {"run_id": "test-spec", "spec": {"environment_test": True}, "checkpoint_available": True},
            {"run_id": "test-status", "status": {"environment_test": True}, "checkpoint_available": True},
            {"run_id": "normal-run", "display_name": "User optimization", "state": "completed", "checkpoint_available": True},
            {"run_id": "false-test-flag", "environment_test": False, "state": "stopped", "checkpoint_available": False},
        ]
        self.app.run_ui.started("currently-selected-normal-run")
        self.app.live_candidates = [{"candidate_id": "normal-candidate", "status": "completed"}]
        self.app.pareto_ids = {"normal-candidate"}
        self.app.refresh_recent_runs()
        self.wait(lambda: "normal-run" in self.app.recent_tree.get_children())
        self.assertEqual(set(self.app.recent_tree.get_children()), {"normal-run", "false-test-flag"})
        self.assertEqual(self.app.recent_run_resumable, {"normal-run": True, "false-test-flag": False})
        self.assertEqual(self.app.run_ui.run_id, "currently-selected-normal-run")
        self.assertEqual(self.app.live_candidates[0]["candidate_id"], "normal-candidate")
        self.assertEqual(self.app.pareto_ids, {"normal-candidate"})
