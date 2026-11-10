"""Keep the first EDA failure while proving and retrying owned UI cleanup."""
import copy
import errno
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.continuation import RuntimeOptimizationLease
from crescendo_engine.persistent_controller import PersistentControllerProxy
from crescendo_engine.private_netlist import PrivateNetlistUnsupported
from crescendo_engine.server import OptimizerService
from crescendo_engine.single_test import project_setup
from crescendo_engine.utils import atomic_write_json, load_json
from tests import test_netlist_optimizer as netlist_fixture
from tests import test_persistent_controller as controller_fixture


class CleanupLifecycleIntegrationTests(unittest.TestCase):
    @staticmethod
    def _nonempty(payload, **_kwargs):
        raise OSError(errno.ENOTEMPTY, "Directory not empty", os.path.join(payload, "control"))

    def _failed_run(self, root):
        prepared, config, spec = netlist_fixture.fixtures(root, max_jobs=2, population=2)
        prepared["setup"]["tests"] = [{"name": "main", "enabled": True}]
        prepared["setup"] = project_setup(prepared["setup"], "main")
        spec.update(evaluator="mock", execution_backend="auto", generations=1)
        name = "optimization_cleanup_fixture"
        run_root = os.path.join(root, name)
        runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
        fixture = controller_fixture.PersistentControllerTest()
        script = fixture._fake_script(root)
        proxy = PersistentControllerProxy(run_root, 0, {}, [], fixture._config(run_root),
                                         command_builder=lambda bootstrap: [sys.executable, script, bootstrap]).start()
        with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=self._nonempty):
            retirement = proxy.terminate_and_prove("bootstrap failed: native Value stack overflow")
        self.assertTrue(retirement["termination_proven"])
        self.assertFalse(retirement["artifact_cleanup_complete"])
        status = load_json(os.path.join(run_root, "status.json"))
        status.update(state="failed", message="native Value stack overflow",
                      primary_failure={"error_type": "StartupError", "message": "native Value stack overflow"},
                      cleanup_failure={"message": "Directory not empty", "recovery_required": True})
        atomic_write_json(os.path.join(run_root, "status.json"), status)
        shutdown = {"schema_version": "crescendo.worker_shutdown.v1", "workers": [{"worker_index": 0,
                    "ok": False, "result": retirement}]}
        atomic_write_json(os.path.join(run_root, "worker_shutdown.json"), shutdown)
        config["runtime"]["optimizer_exit_timeout_seconds"] = 5
        service = OptimizerService(config)
        service.session_run_names.add(name)
        return service, name, run_root, shutdown

    def test_exit_recovers_real_owned_quarantine_without_rewriting_first_failure(self):
        with tempfile.TemporaryDirectory() as root:
            service, name, run_root, shutdown = self._failed_run(root)
            other = os.path.join(root, "optimization_other_session")
            os.makedirs(other)
            marker = os.path.join(other, "must-keep.txt")
            with open(marker, "w") as stream:
                stream.write("other run")
            result = service.exit_optimizer()
            self.assertTrue(result["exiting"])
            self.assertEqual(result["run_cleanup_retry"]["runs"][0]["run_id"], name)
            self.assertFalse(os.path.lexists(os.path.join(run_root, ".cleanup_pending")))
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["message"], "native Value stack overflow")
            self.assertEqual(status["primary_failure"]["message"], "native Value stack overflow")
            self.assertTrue(status["exit_cleanup"]["process_termination_proven"])
            self.assertTrue(status["exit_cleanup"]["remote_stop_proven"])
            self.assertTrue(status["exit_cleanup"]["artifact_cleanup_complete"])
            self.assertFalse(status["cleanup_failure"]["recovery_required"])
            self.assertEqual(load_json(os.path.join(run_root, "worker_shutdown.json")), shutdown)
            self.assertTrue(os.path.isfile(marker))
            self.assertTrue(service.exit_optimizer()["exiting"])

    def test_retry_exit_failure_retains_primary_and_later_retry_recovers(self):
        with tempfile.TemporaryDirectory() as root:
            service, _name, run_root, shutdown = self._failed_run(root)
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=self._nonempty):
                with self.assertRaisesRegex(RuntimeError, "exit cleanup remains blocked"):
                    service.exit_optimizer()
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["message"], "native Value stack overflow")
            self.assertEqual(status["exit_cleanup"]["state"], "failed")
            self.assertTrue(os.path.isdir(os.path.join(run_root, ".cleanup_pending")))
            self.assertTrue(service.exit_optimizer()["exiting"])
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["exit_cleanup"]["attempt_count"], 2)
            self.assertEqual(load_json(os.path.join(run_root, "worker_shutdown.json")), shutdown)

    def test_missing_shutdown_audit_cannot_hide_retained_owned_namespace(self):
        with tempfile.TemporaryDirectory() as root:
            service, _name, run_root, _shutdown = self._failed_run(root)
            os.unlink(os.path.join(run_root, "worker_shutdown.json"))
            status = load_json(os.path.join(run_root, "status.json"))
            status.pop("cleanup_failure")
            atomic_write_json(os.path.join(run_root, "status.json"), status)
            result = service.exit_optimizer()
            self.assertEqual(result["run_cleanup_retry"]["state"], "completed")
            self.assertFalse(os.path.lexists(os.path.join(run_root, ".cleanup_pending")))
            self.assertEqual(load_json(os.path.join(run_root, "status.json"))["message"],
                             "native Value stack overflow")

    def test_malformed_shutdown_audit_requires_verified_checkpoint_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            service, _name, run_root, _shutdown = self._failed_run(root)
            atomic_write_json(os.path.join(run_root, "worker_shutdown.json"), {"workers": []})
            status = load_json(os.path.join(run_root, "status.json"))
            status.pop("cleanup_failure")
            atomic_write_json(os.path.join(run_root, "status.json"), status)
            os.unlink(os.path.join(run_root, "checkpoint.json"))
            with self.assertRaisesRegex(RuntimeError, "no verified checkpoint"):
                service.exit_optimizer()
            self.assertTrue(os.path.isdir(os.path.join(run_root, ".cleanup_pending")))

    def test_missing_checkpoint_never_fabricates_cleanup_permission(self):
        with tempfile.TemporaryDirectory() as root:
            service, _name, run_root, _shutdown = self._failed_run(root)
            os.unlink(os.path.join(run_root, "checkpoint.json"))
            with self.assertRaisesRegex(RuntimeError, "no verified checkpoint"):
                service.exit_optimizer()
            self.assertTrue(os.path.isdir(os.path.join(run_root, ".cleanup_pending")))
            self.assertFalse(os.path.isfile(os.path.join(run_root, "checkpoint.json")))

    def test_other_runtime_owner_blocks_retry_until_lease_released(self):
        with tempfile.TemporaryDirectory() as root:
            service, _name, run_root, _shutdown = self._failed_run(root)
            lease = RuntimeOptimizationLease(root).acquire()
            try:
                with self.assertRaises(RuntimeError):
                    service.exit_optimizer()
                self.assertTrue(os.path.isdir(os.path.join(run_root, ".cleanup_pending")))
            finally:
                lease.release()
            self.assertTrue(service.exit_optimizer()["exiting"])

    def test_never_started_or_already_retired_native_worker_keeps_valid_close_proof(self):
        with tempfile.TemporaryDirectory() as root:
            worker = runner._WholeRunPersistentWorker(root, 0, {}, [], {})
            self.assertTrue(worker.close()["artifact_cleanup_complete"])
            epoch = os.path.join(worker.worker_root, "epoch_000")
            os.makedirs(epoch)
            worker.epoch_root = epoch
            worker.workspace = {"root": os.path.join(epoch, "workspace")}
            controller = SimpleNamespace(closed=False, state="ready", tainted=False,
                process=SimpleNamespace(poll=lambda: 0), is_alive=lambda: False,
                request_batch_abort=lambda **_kwargs: {"remote_stop_proven": True, "no_remote_jobs_started": True})
            def terminate(**_kwargs):
                controller.closed = True
                controller.state = "terminated"
                return {"process_termination_proven": True, "remote_stop_proven": True}
            controller.terminate = terminate
            worker.controller = controller
            def cleanup(path, _run, _guard):
                self.assertEqual(path, epoch)
                shutil.rmtree(path)
                return {"ok": True, "removed": True}
            with mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}), \
                    mock.patch("crescendo_engine.runner.safe_delete_worker_epoch", side_effect=cleanup):
                retired = worker.retire("native bootstrap failed")
            self.assertTrue(retired["process_termination_proven"])
            closed = worker.close()
            self.assertEqual(closed["reason"], "already_closed")
            self.assertEqual(closed["prior_retirement_reason"], "native bootstrap failed")
            self.assertTrue(closed["cleanup"]["ok"])
            self.assertTrue(closed["termination_proven"])

    def test_shared_template_error_and_cleanup_error_keep_distinct_runner_status(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = netlist_fixture.fixtures(root, max_jobs=50, population=20)
            prepared["setup"]["tests"] = [{"name": "main", "enabled": True}]
            prepared["setup"] = project_setup(prepared["setup"], "main")
            run_root = os.path.join(root, "optimization_shared_failure")
            context = mock.Mock()
            context.unsafe.is_set.return_value = False
            context.close.return_value = {"cleanup_proven": False, "retained": ["owned diagnostic"]}
            error = PrivateNetlistUnsupported("native corners differ from inspected source")
            error.detail = {"failure_stage": "corner_identity", "expected": ["slow"], "actual": ["fast"]}
            with mock.patch("crescendo_engine.netlist_optimizer.NetlistExecutionContext", return_value=context), \
                    mock.patch("crescendo_engine.netlist_optimizer.prepare_private_netlist", side_effect=error) as prepare, \
                    mock.patch("crescendo_engine.netlist_optimizer.reconcile_preparation", return_value={"cleanup_proven": True}):
                with self.assertRaisesRegex(RuntimeError, "native corners differ"):
                    runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
            prepare.assert_called_once()
            context.evaluate.assert_not_called()
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertIn("native corners differ", status["message"])
            self.assertEqual(status["primary_failure"]["failure_stage"], "corner_identity")
            self.assertIn("shutdown/cleanup failed", status["secondary_cleanup_error"])
            self.assertTrue(status["cleanup_failure"]["recovery_required"])
            shared = status["shared_preparation_failure"]
            self.assertFalse(shared["execution_started"])
            self.assertEqual(shared["preparation_attempt_count"], 1)
            self.assertEqual(shared["affected_candidate_count"], 20)
            self.assertEqual(status["completed_candidates"], 0)
            candidate_root = os.path.join(run_root, "candidates")
            rows = [load_json(os.path.join(candidate_root, name, "candidate.json")) for name in os.listdir(candidate_root)]
            self.assertEqual(len(rows), 20)
            self.assertTrue(all(row.get("execution_started") is False for row in rows))
            with open(os.path.join(run_root, "crescendo.log")) as stream:
                events = [json.loads(line) for line in stream if line.strip()]
            self.assertEqual(sum(event["stage"] == "shared_template_preparation_failed" for event in events), 1)


if __name__ == "__main__":
    unittest.main()
