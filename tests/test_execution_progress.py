import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine.persistent_maestro import persistent_worker_control_root
from crescendo_engine.runner import _pack_candidate
from crescendo_engine.server import OptimizerService, _configured_execution_context


class ExecutionProgressTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-progress-")
        self.run = os.path.join(self.root, "run-1")
        self.service = OptimizerService({"runtime": {"run_root": self.root}})
        self.write_json("status.json", {"state": "running", "message": "preparing private workspace"})

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def write(self, relative, value):
        path = os.path.join(self.run, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as stream:
            stream.write(value)
        return path

    def write_json(self, relative, value):
        return self.write(relative, json.dumps(value))

    def checkpoint(self):
        value = {"schema_version": "crescendo.checkpoint.v1", "parameter_ids": ["width"],
                 "spec": {}, "candidates": [],
                 "state": {"active_plan": {"generation": 0,
                                           "planned": [[0, [1]], [1, [2]], [2, [3]]]}}}
        self.write_json("checkpoint.json", value)
        return value

    def controller(self, epoch=0):
        relative = "controllers/slot_000/epoch_%03d" % epoch
        path = self.write_json(relative + "/controller_state.json", {
            "schema_version": "crescendo.persistent_controller.v1", "status": "processing",
            "token": "private-controller-token", "environment": {"SECRET": "private-env"},
            "active_request": {"operation": "EVALUATE_BATCH", "nonce": "private-request"},
            "startup_phase": {"stage": "workspace_prepare"},
            "command": [sys.executable, "-m", "crescendo_engine.persistent_controller",
                        "--bootstrap", os.path.join(self.run, relative, "bootstrap.json")],
        })
        return relative, path

    def test_all_newly_planned_population_visible_before_workspace_or_submission(self):
        self.checkpoint()
        self.write_json("status.json", {"state": "running", "active_batch_candidates": ["g000_c0000"]})
        payload = self.service.progress("run-1")
        self.assertEqual([row["candidate_id"] for row in payload["candidates"]],
                         ["g000_c0000", "g000_c0001", "g000_c0002"])
        self.assertEqual([row["values"] for row in payload["candidates"]],
                         [{"width": 1}, {"width": 2}, {"width": 3}])
        self.assertTrue(all(row["status"] == "planned" for row in payload["candidates"]))
        self.assertFalse(os.path.exists(os.path.join(self.run, "candidates")))

    def test_completed_checkpoint_and_live_wal_override_planned_rows_without_duplicates(self):
        checkpoint = self.checkpoint()
        terminal = {"candidate_id": "g000_c0000", "generation": 0, "index": 0,
                    "status": "failed", "values": {"width": 1}, "error": "Spectre rejected device"}
        checkpoint["candidates"] = [_pack_candidate(terminal, ["width"])]
        self.write_json("checkpoint.json", checkpoint)
        live = {"candidate_id": "g000_c0001", "generation": 0, "index": 1,
                "status": "running", "values": {"width": 2}}
        self.write_json("candidates/g000_c0001/candidate.json", live)
        payload = self.service.progress("run-1")
        self.assertEqual([row["status"] for row in payload["candidates"]], ["failed", "running", "planned"])
        self.assertEqual(payload["candidates"][0]["error"], terminal["error"])

    def test_failed_before_candidate_writes_keeps_plan_and_error(self):
        self.checkpoint()
        self.write_json("status.json", {"state": "failed", "message": "Ocean bootstrap rejected policy"})
        self.write_json("server_error.json", {"traceback": "parent exception", "remote_traceback": "remote policy failure",
                                              "error_type": "PersistentControllerRemoteError"})
        payload = self.service.progress("run-1")
        self.assertEqual(len(payload["candidates"]), 3)
        self.assertEqual(payload["execution"]["error"]["remote_traceback_tail"], "remote policy failure")
        self.assertIn("Ocean bootstrap rejected policy", payload["execution"]["error"]["message"])

    def test_failed_before_spec_checkpoint_exists_has_actionable_status_path(self):
        self.write_json("status.json", {"state": "failed", "message": "FileNotFoundError: cannot find Ocean"})
        payload = self.service.progress("run-1")
        self.assertEqual(payload["candidates"], [])
        self.assertEqual(payload["execution"]["error"]["path"], os.path.join(self.run, "status.json"))
        self.assertIn("cannot find Ocean", payload["execution"]["error"]["message"])

    def test_bad_checkpoint_and_candidate_json_do_not_hide_run_failure(self):
        self.write_json("status.json", {"state": "failed", "message": "actual failure"})
        self.write("checkpoint.json", "{broken")
        self.write("candidates/g000_c0000/candidate.json", "{broken")
        payload = self.service.progress("run-1")
        self.assertEqual(payload["execution"]["error"]["message"], "actual failure")
        self.assertEqual(len(payload["execution"]["warnings"]), 2)

    def test_controller_stage_and_real_recorded_argv_have_no_private_protocol_fields(self):
        relative, _ = self.controller()
        self.write(relative + "/controller.stderr", "failed to load Maestro\nTOKEN=private-value\n{'nonce': 'private-request'}\n")
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["workers"][0]["operation"], "EVALUATE_BATCH")
        self.assertEqual(payload["workers"][0]["stage"], "workspace_prepare")
        self.assertEqual(payload["commands"][0]["kind"], "controller")
        encoded = json.dumps(payload)
        for secret in ("private-controller-token", "private-env", "private-value", "private-request"):
            self.assertNotIn(secret, encoded)
        self.assertIn("failed to load Maestro", encoded)

    def test_worker_control_is_computed_not_taken_from_untrusted_path(self):
        relative = "workers/worker_000/epoch_c000007_l000003"
        epoch = "worker-000-controller-000007-epoch-000003"
        self.write_json(relative + "/worker_state.json", {
            "epoch": epoch, "state": "busy", "control_root": "/tmp/foreign", "guardian_token": "private-worker-token"})
        control = persistent_worker_control_root(os.path.join(self.run, relative), epoch)
        control_relative = os.path.relpath(control, self.run)
        self.write_json(control_relative + "/guardian_state.json", {
            "argv": ["/tools/ocean", "-nograph"], "token": "private-guardian-token"})
        self.write(control_relative + "/ocean.stderr", "native ADE failed\n")
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["commands"][0]["argv"], ["/tools/ocean", "-nograph"])
        self.assertIn("native ADE failed", payload["log_tails"][0]["text"])
        encoded = json.dumps(payload)
        self.assertNotIn("private-guardian-token", encoded)
        self.assertNotIn("/tmp/foreign", encoded)

    def test_sibling_symlink_and_hardlink_logs_are_rejected_without_reading(self):
        relative, _ = self.controller()
        outside = os.path.join(self.root, "private.log")
        with open(outside, "w") as stream:
            stream.write("outside-private-value")
        os.symlink(outside, os.path.join(self.run, relative, "controller.stderr"))
        os.link(outside, os.path.join(self.run, relative, "controller.stdout"))
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["log_tails"], [])
        self.assertEqual(len(payload["warnings"]), 2)
        self.assertNotIn("outside-private-value", json.dumps(payload))

    def test_hardlinked_controller_state_and_symlinked_error_json_are_rejected(self):
        relative, state = self.controller()
        os.link(state, os.path.join(self.root, "linked-state.json"))
        foreign = os.path.join(self.root, "foreign.json")
        with open(foreign, "w") as stream:
            json.dump({"traceback": "outside-private-value"}, stream)
        os.symlink(foreign, os.path.join(self.run, "server_error.json"))
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["commands"], [])
        self.assertEqual(payload["workers"], [])
        self.assertEqual(len(payload["warnings"]), 2)
        self.assertNotIn("outside-private-value", json.dumps(payload))

    def test_log_tail_is_bounded_and_newest_controller_epoch_selected(self):
        old, _ = self.controller(0)
        newest, _ = self.controller(1)
        self.write(old + "/controller.stderr", "old epoch secret\n")
        self.write(newest + "/controller.stderr", "x" * 20000 + "last failure")
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(len(payload["workers"]), 1)
        self.assertEqual(payload["workers"][0]["controller_epoch"], 1)
        self.assertLessEqual(sum(len(row["text"].encode("utf-8")) for row in payload["log_tails"]), 16384)
        self.assertTrue(payload["log_tails"][0]["truncated"])
        self.assertTrue(payload["log_tails"][0]["text"].endswith("last failure"))
        self.assertNotIn("old epoch secret", json.dumps(payload))

    def test_configured_command_is_labelled_as_configuration_not_running_submission(self):
        prepared = {"job_policy": {"source": "invoking_maestro", "global": {"properties": {
            "jobsubmitcommand": 'batch-submit -q original -R "cpu=6;mem=30000"', "maxjobs": "64"}}}}
        spec = {"maestro_job_overrides": {"command": "batch-submit -q override", "max_jobs": 2}}
        context = _configured_execution_context(prepared, spec)
        self.assertEqual(context["original_max_jobs"], "64")
        self.assertEqual(context["configured_max_jobs"], "2")
        self.assertIn("original", context["original_command"])
        spec["cr_execution_context"] = context
        self.write_json("spec.json", spec)
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["commands"][0]["state"], "configured")
        self.assertEqual(payload["commands"][0]["command"], "batch-submit -q override")
        self.assertIn("not an execution record", payload["commands"][0]["label"])
        self.assertEqual(prepared["job_policy"]["global"]["properties"]["maxjobs"], "64")

    def test_queued_without_spec_uses_captured_configuration_and_no_new_files(self):
        context = _configured_execution_context({"job_policy": {"global": {"properties": {
            "jobsubmitcommand": "batch-submit -q inherited", "maxjobs": 16}}}}, {})
        self.write_json("status.json", {"state": "queued", "execution_context": context})
        before = sorted(os.listdir(self.run))
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["commands"][0]["command"], "batch-submit -q inherited")
        self.assertEqual(sorted(os.listdir(self.run)), before)

    def test_server_failure_persists_remote_traceback_before_private_controller_cleanup(self):
        error = RuntimeError("remote initialization failed")
        error.remote_traceback = "remote stack\njob policy readback failed"
        with mock.patch("crescendo_engine.server.startup_storage_preflight", return_value={}), mock.patch(
                "crescendo_engine.server.run_optimization", side_effect=error):
            result = self.service._launch_registered({"generations": 2, "population": 3}, {})
            run_name = result["run_id"]
            self.service.threads[run_name].join(2)
        self.assertFalse(self.service.threads[run_name].is_alive())
        recorded = self.service.read(run_name, "server_error.json")
        self.assertEqual(recorded["error_type"], "RuntimeError")
        self.assertEqual(recorded["message"], "remote initialization failed")
        self.assertEqual(recorded["remote_traceback"], error.remote_traceback)
        payload = self.service.progress(run_name)
        self.assertEqual(payload["status"]["state"], "failed")
        self.assertEqual(payload["execution"]["error"]["remote_traceback_tail"], error.remote_traceback)

    def test_retained_startup_error_keeps_native_cause_after_worker_logs_removed(self):
        self.write_json("status.json", {"state": "failed", "message": "Ocean bootstrap failed"})
        original = os.path.join(self.run, "workers", "worker_000", "epoch_000", "control", "ocean.stdout")
        self.write_json("startup_failure.json", {
            "schema_version": "crescendo.startup_failure.v1", "stage": "job_policy_apply",
            "native_error": "CRESCENDO_ADAPTER_ERROR: unknown distribution method",
            "message": "bootstrap failed at job_policy_apply", "recorded_at": "2026-10-07T11:01:00Z",
            "diagnostic": {"ocean.stdout": "CRESCENDO_ADAPTER_ERROR: unknown distribution method\n"},
            "log_paths": {"ocean.stdout": original}, "marker": {"state": "ERROR", "message": "bootstrap failed"},
            "token": "private-startup-token",
        })
        payload = self.service.progress("run-1")["execution"]
        self.assertFalse(os.path.exists(original))
        self.assertEqual(payload["error"]["startup_stage"], "job_policy_apply")
        self.assertIn("unknown distribution", payload["error"]["startup_cause"])
        self.assertTrue(payload["log_tails"][0]["retained"])
        self.assertEqual(payload["log_tails"][0]["path"], original)
        self.assertEqual(payload["log_tails"][0]["source"], os.path.join(self.run, "startup_failure.json"))
        self.assertNotIn("private-startup-token", json.dumps(payload))

    def test_retained_startup_original_path_is_never_opened_and_unknown_schema_rejected(self):
        foreign = os.path.join(self.root, "external.log")
        with open(foreign, "w") as stream:
            stream.write("outside-private-value")
        record = {"schema_version": "crescendo.startup_failure.v1", "diagnostic": {"ocean.stderr": "retained public error"},
                  "log_paths": {"ocean.stderr": foreign}}
        self.write_json("startup_failure.json", record)
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["log_tails"][0]["path"], os.path.join(self.run, "startup_failure.json"))
        self.assertNotIn("outside-private-value", json.dumps(payload))
        record["schema_version"] = "foreign.protocol"
        self.write_json("startup_failure.json", record)
        payload = self.service.progress("run-1")["execution"]
        self.assertEqual(payload["log_tails"], [])
        self.assertNotIn("startup_diagnostic", payload)
        self.assertIn("unsupported", payload["warnings"][0])


if __name__ == "__main__":
    unittest.main()
