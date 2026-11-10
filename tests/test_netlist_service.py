import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from crescendo_engine.netlist_execution import RECEIPT
from crescendo_engine.gui import execution_progress_text
from crescendo_engine.runner import resolve_design_config
from crescendo_engine.server import OptimizerService
from crescendo_engine.utils import atomic_write_json, atomic_write_text, sha256_file


class NetlistServiceTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-netlist-service-")
        self.run = os.path.join(self.root, "origin")
        os.makedirs(self.run)
        self.service = OptimizerService({"runtime": {"run_root": self.root}})
        self.write("status.json", {"schema_version": "crescendo.run_status.v1", "run_id": "origin",
                                   "run_root": self.run, "state": "failed"})
        self.checkpoint = {"schema_version": "crescendo.checkpoint.v1",
                           "manifest": {"origin_run_id": "origin", "origin_run_root": self.run},
                           "spec": {"execution_backend": "private_netlist"},
                           "parameter_ids": [], "candidates": [], "state": {}}

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def write(self, relative, document):
        path = os.path.join(self.run, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        atomic_write_json(path, document)
        return path

    def ownership(self):
        cwd = os.path.join(self.run, "candidates", "g000_c0000", "simulation", "tasks", "t", "netlist")
        control = os.path.join(cwd, "execution")
        os.makedirs(control)
        token = "e" * 32
        files = {key: os.path.join(control, key) for key in ("terminal.receipt", "cancel", "owner_heartbeat")}
        row = {"task": {"cwd": cwd}, "files": files, "token": token, "remote": True}
        path = self.write("private_netlist/execution_ownership/ownership.json", {
            "schema_version": "crescendo.private_netlist_ownership.v1", "owned_run_root": self.run,
            "owner_token": "previous", "active": {"candidate:t": row}, "unresolved": {}})
        return path, token, files

    def test_unknown_remote_task_blocks_resume_before_deleting_or_resubmitting(self):
        path, token, files = self.ownership()
        before = sha256_file(path)
        with mock.patch("crescendo_engine.server._guard_origin_checkpoint_revision", return_value={"state": "matched"}):
            with self.assertRaisesRegex(RuntimeError, "prevents duplicate submission"):
                self.service._cleanup_checkpoint_origin(self.checkpoint, terminate_owned=True)
        self.assertEqual(before, sha256_file(path))
        self.assertTrue(os.path.isfile(files["cancel"]))
        self.assertTrue(os.path.isdir(os.path.dirname(files["cancel"])))

    def test_same_attempt_remote_terminal_receipt_allows_inspection(self):
        path, token, files = self.ownership()
        atomic_write_text(files["terminal.receipt"], RECEIPT + "\t" + token + "\texited\t0\t1\n")
        with mock.patch("crescendo_engine.server._guard_origin_checkpoint_revision", return_value={"state": "matched"}):
            result = self.service._cleanup_checkpoint_origin(self.checkpoint, terminate_owned=True, inspect_only=True)
        self.assertTrue(result["private_netlist_recovery"]["cleanup_proven"])
        self.assertEqual(result["state"], "inspected")
        self.assertTrue(os.path.isfile(path))

    def test_cli_progress_uses_bounded_recorded_commands_without_private_environment(self):
        self.write("status.json", {"state": "running", "message": "executing point"})
        self.write("spec.json", {"execution_backend": "private_netlist"})
        commands = [{"stage": "spectre_submit", "candidate_id": "g000_c%04d" % i,
                     "argv": ["/tools/spectre", "point_%d.scs" % i],
                     "token": "hidden-token", "environment": {"SECRET": "hidden-environment"}} for i in range(30)]
        self.write("private_netlist/progress.json", {"schema_version": "crescendo.private_netlist_optimizer.v1",
                   "latest": {"stage": "measurement", "message": "evaluating original expressions"}, "commands": commands})
        result = self.service.progress("origin")["execution"]
        self.assertEqual(result["stage"], "measurement")
        self.assertEqual(len(result["commands"]), 7)
        self.assertEqual(result["commands"][0]["argv"], ["/tools/spectre", "point_29.scs"])
        text = json.dumps(result)
        self.assertNotIn("hidden-token", text)
        self.assertNotIn("hidden-environment", text)
        self.assertNotIn("SECRET", text)

    def test_exit_signals_optimization_before_optional_cleanup_timeout(self):
        thread = mock.Mock()
        thread.is_alive.return_value = True
        self.service.threads["origin"] = thread
        event = mock.Mock()
        self.service.range_validation_cancels["optional"] = event
        with mock.patch.object(self.service, "stop") as stop:
            def unavailable(**kwargs):
                stop.assert_called_once_with("origin")
                event.set.assert_called_once_with()
                self.assertTrue(self.service.prepare_cancel_event.is_set())
                raise RuntimeError("optional cleanup timeout")
            with mock.patch.object(self.service, "_cancel_and_wait_range_validations", side_effect=unavailable):
                with self.assertRaisesRegex(RuntimeError, "optional cleanup timeout"):
                    self.service.exit_optimizer()
        self.assertTrue(self.service.closing)

    def test_exit_signals_optimization_before_lifecycle_registration_timeout(self):
        self.service.config["runtime"]["optimizer_exit_timeout_seconds"] = 0
        self.service.lifecycle_operations = 1
        thread = mock.Mock()
        thread.is_alive.return_value = True
        self.service.threads["origin"] = thread
        with mock.patch.object(self.service, "stop") as stop:
            with self.assertRaisesRegex(RuntimeError, "resource operation"):
                self.service.exit_optimizer()
        stop.assert_called_once_with("origin")

    def test_saved_design_config_preserves_execution_engine_and_job_overrides(self):
        document = {"schema_version": "crescendo.design_config.v1", "optimization": {
            "run": {"execution_backend": "private_netlist", "maestro_candidate_mode": "batch",
                    "maestro_job_overrides": {"max_jobs": 20, "command": "batch-submit -q short"}}}}
        resolved = resolve_design_config(document, {"parameters": [], "outputs": [], "setup": {}})
        self.assertEqual(resolved["spec"]["execution_backend"], "private_netlist")
        self.assertEqual(resolved["spec"]["maestro_job_overrides"]["max_jobs"], 20)

    def test_spectre_prepared_command_is_distinct_from_proven_completion(self):
        self.write("spec.json", {"execution_backend": "private_netlist"})
        self.write("private_netlist/progress.json", {
            "schema_version": "crescendo.private_netlist_optimizer.v1", "commands": [
                {"stage": "spectre_command", "argv": ["/tools/spectre", "input.scs"]},
                {"stage": "spectre_completed", "argv": ["/tools/spectre", "input.scs"]}]})
        result = self.service.progress("origin")["execution"]
        self.assertEqual([row["state"] for row in result["commands"]], ["completed", "prepared"])
        rendered = execution_progress_text({"state": "running"}, result)
        self.assertTrue(all(row["kind"] == "solver" for row in result["commands"]))
        self.assertEqual([row["stage"] for row in result["commands"]], ["spectre_completed", "spectre_command"])
        self.assertEqual(rendered.count("Solver command: Solver command"), 2)
        self.assertNotIn("Netlist generation command", rendered)
        self.assertIn("$ /tools/spectre input.scs", rendered)


if __name__ == "__main__":
    unittest.main()
