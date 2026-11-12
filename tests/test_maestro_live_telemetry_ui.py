"""Live evidence is visible before the blocking native batch returns."""
from __future__ import print_function

import copy
import json
import os
import tempfile
import threading
import time
import unittest

from crescendo_engine import gui
from crescendo_engine.gui_state import BackgroundTaskQueue, candidate_status
from crescendo_engine.run_data_store import CORE_NAME, RunDataStore
from crescendo_engine.server import OptimizerService


class LiveTelemetryBackendTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo-live-evidence-")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.run = os.path.join(self.root, "run-1")
        os.makedirs(self.run)
        self.service = OptimizerService({"runtime": {"run_root": self.root}})
        self.write("status.json", json.dumps({"run_id": "run-1", "state": "running"}))
        self.store = RunDataStore(os.path.join(self.run, CORE_NAME),
                                  metadata={"run_id": "run-1", "spec": {"objectives": []}})
        self.addCleanup(self.store.close)
        self.row = {"candidate_id": "g000_c0000", "generation": 0, "index": 0,
                    "values": {"width": 1e-6}, "status": "planned"}
        self.store.commit_candidates([self.row])

    def write(self, relative, text):
        path = os.path.join(self.run, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            stream.write(text)
        return path

    def task(self, point=0, corner="TT"):
        alias = "cr_g000_c0000_source"
        return {"candidate_id": self.row["candidate_id"], "test": "original",
                "artifact_test": alias, "corner": corner, "point_id": point,
                "state": "running", "job_id": "job-%s" % point,
                "spectre_log": os.path.join(self.run, "_scratch", "results", "History.1",
                                             str(point), alias, "psf", "spectre.out")}

    def observe(self, tasks=None, stage="running"):
        self.row = dict(self.row, status="running", execution_backend="persistent_maestro")
        self.row["batch_execution"] = {"batch_id": "batch_1_unique", "shared_history": True,
                                        "source_test_map": {"cr_g000_c0000_source": "original"}}
        self.row["execution_progress"] = {"stage": stage, "tasks": tasks or []}
        self.store.commit_candidates([self.row])

    def observe_serial(self, tasks):
        self.row = dict(self.row, status="running", execution_backend="persistent_maestro")
        self.row.pop("batch_execution", None)
        self.serial_identity = {"epoch": "worker-000-epoch-000", "sequence": 3,
                                "nonce": "serial-owned", "candidate_id": self.row["candidate_id"]}
        self.serial_started = time.time() - .5
        self.row["execution_progress"] = {
            "stage": "running", "serial_observation": True,
            "batch_id": self.row["candidate_id"], "source_test_map": {"original": "original"},
            "protocol_identity": self.serial_identity, "started_epoch": self.serial_started,
            "log_search": {"roots": [os.path.join(self.run, "_scratch", "results")]}, "tasks": tasks,
        }
        self.serial_state = {"schema_version": "crescendo.persistent_maestro_worker.v1", "worker_index": 0,
                             "epoch": self.serial_identity["epoch"], "state": "busy",
                             "active_identity": self.serial_identity}
        self.write("workers/worker_000/epoch_000/worker_state.json", json.dumps(self.serial_state))
        self.serial_artifact = {"native_serial": True, "batch_id": self.row["candidate_id"],
                                "candidate_id": self.row["candidate_id"], "worker_index": 0,
                                "worker_epoch": self.serial_identity["epoch"],
                                "protocol_identity": self.serial_identity}
        self.write("candidates/g000_c0000/persistent_worker_candidate.json", json.dumps(self.serial_artifact))
        self.store.commit_candidates([self.row])

    def serial_task(self, point=0):
        task = self.task(point, "PVT")
        task["artifact_test"] = "original"
        task["spectre_log"] = task["spectre_log"].replace("cr_g000_c0000_source", "original")
        self.write(os.path.relpath(task["spectre_log"], self.run), "Actual serial Spectre point %d\n" % point)
        return task

    def test_serial_live_logs_preserve_test_identity_and_revoke_when_worker_advances(self):
        tasks = [self.serial_task(0), self.serial_task(1)]
        self.observe_serial(tasks)
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(len(metadata["logs"]), 2)
        self.assertTrue(all(item["test"] == "original" for item in metadata["logs"]))
        log_id = metadata["logs"][0]["log_id"]
        self.assertIn("Actual serial Spectre", self.service.read_candidate_log_chunk(
            "run-1", self.row["candidate_id"], log_id)["text"])
        self.serial_state["active_identity"] = dict(self.serial_identity, sequence=4, nonce="next-serial",
                                                    candidate_id="g000_c0001")
        self.write("workers/worker_000/epoch_000/worker_state.json", json.dumps(self.serial_state))
        self.assertEqual(self.service.candidate_logs("run-1", self.row["candidate_id"])["logs"], [])
        self.assertEqual(self.service.read_candidate_log_chunk("run-1", self.row["candidate_id"], log_id)["text"], "")

    def test_serial_old_history_mtime_and_unobserved_history_root_are_rejected(self):
        task = self.serial_task()
        self.observe_serial([task])
        os.utime(task["spectre_log"], (self.serial_started - 10, self.serial_started - 10))
        self.assertEqual(self.service.candidate_logs("run-1", self.row["candidate_id"])["logs"], [])
        task["spectre_log"] = self.write("elsewhere/History.1/0/original/psf/spectre.out", "Unobserved history\n")
        self.row["execution_progress"]["tasks"] = [task]
        self.store.commit_candidates([self.row])
        with self.assertRaisesRegex(ValueError, "outside its observed history roots"):
            self.service.candidate_logs("run-1", self.row["candidate_id"])

    def test_serial_protocol_mismatch_or_batch_alias_cannot_gain_unfiltered_logs(self):
        task = self.serial_task()
        self.observe_serial([task])
        artifact = dict(self.serial_artifact, protocol_identity=dict(self.serial_identity, nonce="wrong"))
        self.write("candidates/g000_c0000/persistent_worker_candidate.json", json.dumps(artifact))
        with self.assertRaisesRegex(ValueError, "protocol identities disagree"):
            self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.write("candidates/g000_c0000/persistent_worker_candidate.json", json.dumps(self.serial_artifact))
        self.row["execution_progress"]["source_test_map"] = {"cr_batch_alias": "original"}
        self.store.commit_candidates([self.row])
        with self.assertRaisesRegex(ValueError, "preserve original tests"):
            self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.observe([self.task()])
        self.row["execution_progress"]["serial_observation"] = True
        self.store.commit_candidates([self.row])
        with self.assertRaisesRegex(ValueError, "conflicts with candidate batch"):
            self.service.candidate_logs("run-1", self.row["candidate_id"])

    def test_serial_stop_retained_log_remains_readable_after_worker_is_idle(self):
        task = self.serial_task()
        source = task["spectre_log"]
        self.observe_serial([task])
        retained = self.write("candidates/g000_c0000/retained_spectre_live_serial.log", "Serial stopped tail\n")
        self.write("candidates/g000_c0000/retained_spectre_live_serial.meta",
                   "schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n"
                   "candidate_id=g000_c0000\nartifact_test=original\npoint_id=0\n" % source)
        self.row["execution_progress"]["stage"] = "cancelled"
        self.row["execution_progress"]["tasks"] = [dict(task, spectre_log=retained)]
        self.store.commit_candidates([self.row])
        self.serial_state.update(state="idle", active_identity=None)
        self.write("workers/worker_000/epoch_000/worker_state.json", json.dumps(self.serial_state))
        os.unlink(source)
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        descriptor = next(item for item in metadata["logs"] if item["kind"] == "spectre_retained")
        self.assertEqual(descriptor["corner"], "PVT")
        self.assertEqual(self.service.read_candidate_log_chunk("run-1", self.row["candidate_id"],
                                                             descriptor["log_id"])["text"], "Serial stopped tail\n")

    def test_latest_core_progress_wins_over_old_candidate_sidecar(self):
        self.write("candidates/g000_c0000/candidate.json", json.dumps(self.row))
        first = self.service.progress_delta("run-1")
        self.observe(stage="preparing")
        delta = self.service.progress_delta("run-1", first["revision"])
        self.assertEqual(candidate_status(delta["changed_candidates"][0]), "Preparing · Not Submitted")
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(metadata["candidate_status"], "running")
        self.assertEqual(metadata["stage"], "preparing")
        self.assertIn("preparing", metadata["message"])

    def test_planned_core_candidate_is_inspectable_without_sidecar_directory(self):
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(metadata["state"], "waiting")
        self.assertEqual(metadata["candidate_status"], "planned")

    def test_real_save_root_logs_live_before_native_results_and_append_without_fd_leak(self):
        tasks = [self.task(0, "TT"), self.task(1, "PVT")]
        for task in tasks:
            self.write(os.path.relpath(task["spectre_log"], self.run), "Spectre executing %s\n" % task["job_id"])
        self.observe(tasks)
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(len(metadata["logs"]), 2)
        self.assertEqual(metadata["spectre_state"], "started")
        self.assertEqual(set(row["corner"] for row in metadata["logs"]), {"TT", "PVT"})
        descriptor = next(row for row in metadata["logs"] if row["point_id"] == 0)
        first = self.service.read_candidate_log_chunk("run-1", self.row["candidate_id"], descriptor["log_id"])
        with open(tasks[0]["spectre_log"], "a") as stream:
            stream.write("second live line\n")
        before = len(os.listdir("/proc/self/fd"))
        for _ in range(24):
            chunk = self.service.read_candidate_log_chunk(
                "run-1", self.row["candidate_id"], descriptor["log_id"],
                offset=first["next_offset"], file_token=first["file_token"])
            self.assertEqual(chunk["text"], "second live line\n")
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_shared_ocean_is_not_evidence_that_spectre_started(self):
        self.observe(stage="dispatching")
        ocean = self.write("workers/worker_000/epoch_000/control/ocean.stdout", "Preparing tests\n")
        self.row["execution_progress"]["shared_logs"] = [{"path": ocean}]
        self.store.commit_candidates([self.row])
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(metadata["logs"][0]["kind"], "ocean")
        self.assertEqual(metadata["logs"][0]["scope"], "shared")
        self.assertEqual(metadata["spectre_state"], "unknown")
        self.assertIn("no candidate Spectre", metadata["message"])

    def test_capacity_snapshot_keeps_task_total_warning_and_full_mapping_readable(self):
        self.observe(stage="running")
        mapping_file = self.write("batches/g000/execution_progress_full.json",
                                  '{"tasks": ["complete mapping remains available"]}\n')
        self.row["execution_progress"].update({
            "task_count": 12000,
            "telemetry_capacity": {"message": "Compact live snapshot; see complete native task/log mapping"},
            "shared_logs": [{"path": mapping_file, "label": "Complete native task/log mapping"}],
        })
        self.store.commit_candidates([self.row])
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(metadata["task_count"], 12000)
        self.assertEqual(metadata["task_summary_omitted"], 12000)
        self.assertTrue(any("Compact live snapshot" in item for item in metadata["warnings"]))
        descriptor = next(item for item in metadata["logs"] if
                          "Complete native task/log mapping" in item["label"])
        self.assertEqual(descriptor["scope"], "shared")
        self.assertIn("Complete native task/log mapping", descriptor["label"])
        chunk = self.service.read_candidate_log_chunk("run-1", self.row["candidate_id"], descriptor["log_id"])
        self.assertIn("complete mapping remains available", chunk["text"])

    def test_supported_many_tasks_are_not_rejected_by_arbitrary_record_limit(self):
        tasks = [dict(self.task(point), spectre_log=None) for point in range(4100)]
        self.observe(tasks)
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(metadata["task_count"], 4100)
        self.assertEqual(metadata["task_summary_omitted"], 4100 - 32)

    def test_stop_retained_path_requires_source_meta_and_keeps_task_corner(self):
        task = self.task(0, "PVT")
        original = task["spectre_log"]
        task["spectre_log"] = self.write("candidates/g000_c0000/retained_spectre_live_a.log", "Stopped Spectre tail\n")
        self.write("candidates/g000_c0000/retained_spectre_live_a.meta",
                   "schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n"
                   "candidate_id=g000_c0000\nartifact_test=cr_g000_c0000_source\npoint_id=0\n" % original)
        self.observe([task], stage="cancelled")
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(candidate_status(self.row), "Cancelled")
        self.assertEqual(metadata["logs"][0]["kind"], "spectre_retained")
        self.assertEqual(metadata["logs"][0]["corner"], "PVT")
        os.unlink(task["spectre_log"][:-4] + ".meta")
        with self.assertRaisesRegex(ValueError, "source identity"):
            self.service.candidate_logs("run-1", self.row["candidate_id"])

    def test_live_alias_cannot_be_relabelled_as_another_candidate_without_mapping(self):
        task = self.task()
        path = self.write(os.path.relpath(task["spectre_log"], self.run), "owned Spectre header\n")
        self.observe([task])
        for changes in ({"test": "another_original"}, {"artifact_test": "cr_g000_c0001_source",
                     "spectre_log": path.replace("cr_g000_c0000_source", "cr_g000_c0001_source")}):
            with self.subTest(changes=changes):
                self.row["execution_progress"]["tasks"] = [dict(task, **changes)]
                self.store.commit_candidates([self.row])
                with self.assertRaisesRegex(ValueError, "mapping disagrees"):
                    self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.row["execution_progress"]["tasks"] = [task]
        self.row.pop("batch_execution")
        self.store.commit_candidates([self.row])
        with self.assertRaisesRegex(ValueError, "mapping is unavailable"):
            self.service.candidate_logs("run-1", self.row["candidate_id"])

    def test_retained_task_metadata_cannot_relabel_candidate_alias_or_native_point(self):
        task = self.task()
        source = task["spectre_log"]
        task["spectre_log"] = self.write("candidates/g000_c0000/retained_spectre_live_bad.log", "Stopped log\n")
        self.observe([task], stage="cancelled")
        correct = {"candidate_id": "g000_c0000", "artifact_test": "cr_g000_c0000_source", "point_id": "0"}
        for key, value in (("candidate_id", "g000_c0001"), ("artifact_test", "cr_g000_c0001_source"), ("point_id", "1")):
            with self.subTest(field=key):
                fields = dict(correct, **{key: value})
                self.write("candidates/g000_c0000/retained_spectre_live_bad.meta",
                           "schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n%s\n" %
                           (source, "\n".join("%s=%s" % pair for pair in sorted(fields.items()))))
                with self.assertRaisesRegex(ValueError, "metadata identity disagrees"):
                    self.service.candidate_logs("run-1", self.row["candidate_id"])

    def test_other_candidate_alias_point_and_outside_paths_are_rejected(self):
        for update in ({"artifact_test": "cr_g000_c0001_source"}, {"point_id": 9},
                       {"spectre_log": os.path.join(self.root, "outside", "spectre.out")}):
            with self.subTest(update=update):
                task = self.task()
                task.update(update)
                self.observe([task])
                with self.assertRaises(ValueError):
                    self.service.candidate_logs("run-1", self.row["candidate_id"])

    def test_stage_labels_do_not_claim_simulator_running_during_dispatch(self):
        for stage, expected in (("preparing", "Preparing · Not Submitted"),
                                ("dispatching", "Dispatching · Awaiting Task Evidence"),
                                ("submitted", "Submitted"), ("queued", "Queued"), ("running", "Running")):
            row = dict(self.row, status="running", execution_progress={"stage": stage})
            self.assertEqual(candidate_status(row), expected)

    def test_controller_progress_only_alias_mapping_and_shared_submit_evidence(self):
        task = self.task()
        self.write(os.path.relpath(task["spectre_log"], self.run), "Spectre header\n")
        ledger = self.write("native_dispatch.jsonl", "native submission evidence\n")
        task["submit_log"] = ledger
        task["submission"] = {"argv": ["batch-submit", "-q", "short", "owned_script"],
                               "returncode": 0, "stdout": "Job <12345> submitted", "job_ids": ["12345"]}
        self.observe([task])
        self.row["execution_progress"]["source_test_map"] = self.row.pop("batch_execution")["source_test_map"]
        self.row["execution_progress"]["batch_id"] = "batch_1_unique"
        self.row["execution_progress"]["shared_logs"] = [{"path": ledger}]
        self.store.commit_candidates([self.row])
        metadata = self.service.candidate_logs("run-1", self.row["candidate_id"])
        self.assertEqual(metadata["tasks"][0]["submission_returncode"], 0)
        self.assertIn("batch-submit -q short", metadata["tasks"][0]["command"])
        shared = next(log for log in metadata["logs"] if log["kind"] == "submission")
        self.assertEqual(shared["scope"], "shared")
        self.assertIn("shared batch", shared["label"])

    def test_console_keeps_observed_shared_job_submission_separate_from_configured(self):
        status = {"schema_version": "crescendo.run_status.v1", "run_id": "run-1", "state": "running",
                  "execution_backend": "persistent_maestro", "persistent_worker_count": 1,
                  "execution_context": {"configured_command": "batch-submit -q current"},
                  "maestro_max_jobs": 50, "maestro_live_progress": {"batch_id": "batch_1_unique",
                  "shared_submissions": [{"invocation_id": "invoke-1", "argv": ["batch-submit", "-q", "current"],
                  "returncode": 0, "job_ids": ["12345"], "stdout": "submitted current job"}]}}
        self.write("status.json", json.dumps(status))
        self.write("spec.json", json.dumps({"cr_execution_context": {"configured_command": "batch-submit -q current"}}))
        payload = self.service.progress("run-1")
        text = gui.execution_progress_text(status, payload["execution"])
        self.assertIn("Configured submission", text)
        self.assertIn("Observed submission", text)
        self.assertIn("Observed job IDs: 12345", text)
        self.assertIn("candidate point mapping is unproven", text)
        self.assertIn("Ocean workers: 1 | Native max jobs: 50", text)
        self.assertNotIn("candidate g000_c0000 submitted", text)


@unittest.skipUnless(os.environ.get("DISPLAY"), "Existing desktop required for Tk")
class LiveTelemetryTkTests(unittest.TestCase):
    def test_many_point_logs_all_selectable_with_bounded_tabs_and_long_test_names(self):
        try:
            root = gui.tk.Tk()
        except gui.tk.TclError as error:
            self.skipTest(str(error))
        root.withdraw()
        queue = BackgroundTaskQueue(root, interval_ms=10)
        rows = [{"log_id": "log_%03d" % index, "kind": "spectre", "test": "long_original_test_" * 5,
                 "point_id": index, "corner": "PVT", "label": "long_original_test · PVT · point %s" % index}
                for index in range(80)]
        calls = []

        class Service(object):
            def candidate_logs(self, _run, _candidate):
                return {"candidate_status": "running", "state": "available", "logs": rows, "task_count": 80}

            def read_candidate_log_chunk(self, _run, _candidate, log_id, **kwargs):
                calls.append(log_id)
                return {"text": "real selected " + log_id, "next_offset": 20, "file_token": "selected"}

        app = type("App", (), {"root": root, "tasks": queue, "service": Service(), "log_viewers": []})()
        viewer = gui.LiveLogViewer(app, "run", "g000_c0000")
        app.log_viewers.append(viewer)
        try:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline and not viewer.log_choices:
                root.update()
                time.sleep(.005)
            self.assertEqual(len(viewer.log_choice.cget("values")), 80)
            self.assertLessEqual(len(viewer.tabs), viewer.MAX_OPEN_TABS)
            viewer.log_choice.current(79)
            viewer._select_log()
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                root.update()
                tab = viewer.tabs.get("log_079")
                if tab and "log_079" in tab["text"].get("1.0", "end"):
                    break
                time.sleep(.005)
            self.assertIn("log_079", calls)
            self.assertIn("log_079", tab["text"].get("1.0", "end"))
            self.assertTrue(str(viewer.notebook.tab(tab["frame"], "text")).startswith("PVT · point 79"))
            self.assertLessEqual(len(viewer.tabs), viewer.MAX_OPEN_TABS)
            viewer._close()
            self.assertEqual(app.log_viewers, [])
            self.assertIsNone(viewer.poll_job)
        finally:
            if not viewer.closed:
                viewer._close()
            queue.close()
            for name in ("worker", "control_worker", "history_worker", "optional_worker", "export_worker", "monitor_worker"):
                getattr(queue, name).join(1)
            for identifier in root.tk.splitlist(root.tk.call("after", "info")):
                root.after_cancel(identifier)
            root.destroy()

    def test_viewer_refreshes_while_main_worker_blocked_and_closes_timer(self):
        try:
            root = gui.tk.Tk()
        except gui.tk.TclError as error:
            self.skipTest(str(error))
        root.withdraw()
        queue = BackgroundTaskQueue(root, interval_ms=10)
        entered, release = threading.Event(), threading.Event()
        logs = []
        callbacks = []
        root.report_callback_exception = lambda _exc, value, _trace: callbacks.append(value)

        class Service(object):
            def candidate_logs(self, _run_id, _candidate_id):
                return {"candidate_status": "running", "stage": "running", "state": "available",
                        "spectre_state": "started", "logs": [{"log_id": "live", "label": "source · TT"}]}

            def read_candidate_log_chunk(self, _run_id, _candidate_id, _log_id, **kwargs):
                offset = kwargs.get("offset", 0)
                value = "".join(logs)
                return {"text": value[offset:], "next_offset": len(value), "eof": True}

        app = type("App", (), {"root": root, "tasks": queue, "service": Service()})()
        viewer = None
        try:
            def blocked():
                entered.set()
                release.wait(5)
            queue.submit(blocked)
            self.assertTrue(entered.wait(10))
            logs.append("first live line\n")
            viewer = gui.LiveLogViewer(app, "run-1", "g000_c0000")
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                root.update()
                tab = viewer.tabs.get("live")
                if tab and "first live line" in tab["text"].get("1.0", "end"):
                    break
                time.sleep(.01)
            self.assertIsNotNone(tab)
            self.assertIn("first live line", tab["text"].get("1.0", "end"))
            logs.append("second live line\n")
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                root.update()
                if "second live line" in tab["text"].get("1.0", "end"):
                    break
                time.sleep(.01)
            self.assertIn("second live line", tab["text"].get("1.0", "end"))
            viewer._close()
            self.assertIsNone(viewer.poll_job)
            root.update()
            self.assertEqual(callbacks, [])
        finally:
            release.set()
            if viewer is not None and not viewer.closed:
                viewer._close()
            queue.close()
            for name in ("worker", "control_worker", "history_worker", "optional_worker", "export_worker", "monitor_worker"):
                getattr(queue, name).join(1)
            for identifier in root.tk.splitlist(root.tk.call("after", "info")):
                root.after_cancel(identifier)
            root.destroy()


if __name__ == "__main__":
    unittest.main()
