"""Export service gates and cancellation, without starting native EDA."""
import copy
import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.continuation import RuntimeOptimizationLease, ContinuationLeaseBusy
from crescendo_engine.server import OptimizerService
from crescendo_engine.optimized_schematic import OptimizedSchematicError
from crescendo_engine.utils import atomic_write_json


class OptimizedViewServiceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-view-service-")
        self.addCleanup(self.directory.cleanup)
        self.root = self.directory.name
        self.run = os.path.join(self.root, "finished-run")
        os.makedirs(self.run)
        self.service = OptimizerService({"runtime": {"run_root": self.root,
                                                        "optimizer_exit_timeout_seconds": 2}})
        self.status = {"schema_version": "crescendo.run_status.v1", "run_id": "finished-run",
                       "run_root": self.run, "state": "completed"}
        self.context = {"schema_version": "crescendo.schematic_export_context.v1",
                        "prepared": {"parameters": [{"parameter_id": "width"}]},
                        "config": {"runtime": {"run_root": self.run}}}
        self.candidate = {"candidate_id": "g000_c0000", "status": "completed",
                          "values": {"width": 3.96e-6}, "execution_backend": "persistent_maestro"}
        self.write("status.json", self.status)
        self.write("spec.json", {"evaluator": "real", "display_name": "Selected_Solution"})
        self.write("schematic_export_context.json", self.context)
        self.write("worker_shutdown.json", {"schema_version": "crescendo.worker_shutdown.v1", "workers": [{"ok": True}]})
        self.write("candidates/g000_c0000/candidate.json", self.candidate)
        self.export = mock.patch("crescendo_engine.optimized_schematic.export_optimized_schematic",
                                 return_value={"ok": True, "views": []}).start()
        self.addCleanup(mock.patch.stopall)

    def write(self, relative, payload):
        path = os.path.join(self.run, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        atomic_write_json(path, payload)
        return path

    def generate(self):
        return self.service.generate_optimized_schematic("finished-run", "g000_c0000")

    def origin_fixture(self, backend="persistent_maestro", completed=True):
        original = os.path.join(self.root, "origin-run")
        context = copy.deepcopy(self.context)
        identity = {"schema_version": "crescendo.source_design_identity.v2", "fingerprint": "source-qualified"}
        context["prepared"]["source_identity"] = identity
        context["config"]["runtime"]["run_root"] = original
        self.context["prepared"]["source_identity"] = identity
        self.context["origin_run_id"] = "origin-run"
        self.write("schematic_export_context.json", self.context)
        candidate = dict(self.candidate, execution_backend=backend)
        if completed:
            candidate["completion_evidence"] = {"ok": True, "parameters": dict(candidate["values"])}
        for relative, value in (("status.json", dict(self.status, run_id="origin-run", run_root=original)),
                                ("worker_shutdown.json", {"schema_version": "crescendo.worker_shutdown.v1", "workers": [{"ok": True}]}),
                                ("schematic_export_context.json", context),
                                ("candidates/g000_c0000/candidate.json", candidate)):
            path = os.path.join(original, relative)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            atomic_write_json(path, value)
        return original, context, candidate

    def uncertain_native_export(self, *args, **kwargs):
        root = args[4]
        context = os.path.join(root, "context")
        control = os.path.join(root, "stage")
        os.makedirs(context)
        os.makedirs(control)
        manifest = {"token": "optimized-export-" + "a" * 32, "attempt_root": control,
                    "workspace_root": context,
                    "argv": ["/fixture/ocean", "-nograph", "-restore", os.path.join(control, "export.il")]}
        atomic_write_json(os.path.join(control, "legacy_guardian_launch.json"), manifest)
        raise OptimizedSchematicError("Owned process termination unavailable", {
            "phase": "stage", "command": {"termination_failed": True,
                                            "guardian_cleanup": {"state": "ambiguous"}}})

    def release_pending_lease(self, service=None):
        service = service or self.service
        if service.optimized_view_cleanup_lease is not None:
            service.optimized_view_cleanup_lease.release()

    def test_completed_real_candidate_uses_durable_context_and_releases_runtime_lease(self):
        result = self.generate()
        self.assertTrue(result["ok"])
        args, keywords = self.export.call_args
        self.assertEqual(args[:4], (self.context["prepared"],
                                   {"evaluator": "real", "display_name": "Selected_Solution"},
                                   self.candidate, self.context["config"]))
        self.assertTrue(args[4].startswith(os.path.join(self.run, "optimized_schematics", "export_")))
        self.assertFalse(keywords["cancel_check"]())
        self.assertTrue(callable(keywords["identity_check"]))
        self.assertEqual(self.service.lifecycle_operations, 0)
        self.assertFalse(self.service.optimized_view_export_active)
        lease = RuntimeOptimizationLease(self.root, run_root=self.run).acquire()
        lease.release()

    def test_private_netlist_template_is_forwarded_with_completed_candidate(self):
        self.candidate["execution_backend"] = "private_netlist"
        self.write("candidates/g000_c0000/candidate.json", self.candidate)
        template = {"manifest_sha256": "native-qualified", "qualification": {"ok": True}}
        self.write("private_netlist/templates/private_netlist.json", template)
        self.generate()
        self.assertEqual(self.export.call_args[1]["template_manifest"], template)

    def test_resumed_solution_uses_original_native_evidence_for_both_backends(self):
        for backend in ("persistent_maestro", "private_netlist"):
            with self.subTest(backend=backend):
                original, context, candidate = self.origin_fixture(backend)
                template = {"manifest_sha256": "original-qualified", "qualification": {"ok": True}}
                if backend == "private_netlist":
                    path = os.path.join(original, "private_netlist", "templates", "private_netlist.json")
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    atomic_write_json(path, template)
                self.generate()
                args, keywords = self.export.call_args
                self.assertEqual(args[0], context["prepared"])
                self.assertEqual(args[1]["display_name"], "Selected_Solution")
                self.assertEqual(args[2], candidate)
                self.assertEqual(args[3], context["config"])
                self.assertTrue(args[4].startswith(self.run + "/optimized_schematics/"))
                self.assertEqual(keywords["template_manifest"], template if backend == "private_netlist" else None)

    def test_origin_value_or_source_mismatch_and_cycle_cannot_export(self):
        for condition in ("different-value", "different-source", "cycle"):
            with self.subTest(condition=condition):
                original, context, candidate = self.origin_fixture(completed=condition != "cycle")
                if condition == "different-value":
                    candidate["values"] = {"width": 4.1e-6}
                    atomic_write_json(os.path.join(original, "candidates/g000_c0000/candidate.json"), candidate)
                elif condition == "different-source":
                    context["prepared"]["source_identity"]["fingerprint"] = "another-source"
                    atomic_write_json(os.path.join(original, "schematic_export_context.json"), context)
                else:
                    context["origin_run_id"] = "finished-run"
                    atomic_write_json(os.path.join(original, "schematic_export_context.json"), context)
                with self.assertRaisesRegex(ValueError, "does not match|chain is unavailable"):
                    self.generate()
                self.export.assert_not_called()
                self.assertFalse(self.service.optimized_view_export_active)

    def test_unknown_native_cleanup_blocks_start_resume_exit_until_exact_recovery(self):
        self.export.side_effect = self.uncertain_native_export
        self.addCleanup(self.release_pending_lease)
        marker = os.path.join(self.root, ".optimized_schematic_cleanup_pending.json")
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "ambiguous"}) as quiesce:
            with self.assertRaisesRegex(RuntimeError, "cleanup is unproven"):
                self.generate()
            self.assertTrue(os.path.isfile(marker))
            self.assertTrue(self.service.optimized_view_export_active)
            self.assertIsNotNone(self.service.optimized_view_cleanup_lease.descriptor)
            self.assertEqual(self.service.lifecycle_operations, 0)
            for action in (lambda: self.service.start("unused", {}), lambda: self.service.resume({}), self.service.exit_optimizer):
                with self.assertRaisesRegex(RuntimeError, "cleanup is unproven"):
                    action()
            self.assertTrue(quiesce.call_args[1]["terminate_owned"])
            self.assertLessEqual(quiesce.call_args[1]["timeout_seconds"], 12)
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "dead"}):
            self.assertTrue(self.service.exit_optimizer()["exiting"])
        self.assertFalse(os.path.exists(marker))
        self.assertFalse(self.service.optimized_view_export_active)
        self.assertIsNone(self.service.optimized_view_cleanup_lease)
        lease = RuntimeOptimizationLease(self.root).acquire()
        lease.release()

    def test_restart_cannot_bypass_durable_cleanup_barrier(self):
        self.export.side_effect = self.uncertain_native_export
        self.addCleanup(self.release_pending_lease)
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "ambiguous"}):
            with self.assertRaises(RuntimeError):
                self.generate()
            restarted = OptimizerService(self.service.config)
            self.addCleanup(lambda: self.release_pending_lease(restarted))
            with self.assertRaises(ContinuationLeaseBusy):
                restarted.start("unused", {})
            self.release_pending_lease()  # Model the original Python process exiting.
            self.service.optimized_view_cleanup_lease = None
            with self.assertRaisesRegex(RuntimeError, "cleanup is unproven"):
                restarted.resume({})
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "dead"}), \
                mock.patch.object(restarted, "_start_registered", return_value={"run_id": "next-run"}) as launch:
            self.assertEqual(restarted.start("prepared", {})["run_id"], "next-run")
        launch.assert_called_once_with("prepared", {})
        self.assertIsNone(restarted.optimized_view_cleanup_lease)

    def test_cleanup_barrier_refuses_mismatched_owned_command_without_quiescing(self):
        self.export.side_effect = self.uncertain_native_export
        self.addCleanup(self.release_pending_lease)
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "ambiguous"}):
            with self.assertRaises(RuntimeError):
                self.generate()
        pending = self.service.optimized_view_cleanup_pending
        manifest_path = os.path.join(self.run, pending["export_relative"], "stage", "legacy_guardian_launch.json")
        atomic_write_json(manifest_path, {"token": "optimized-export-" + "a" * 32,
                                         "workspace_root": "/other/workspace", "argv": []})
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt") as quiesce:
            with self.assertRaisesRegex(RuntimeError, "ownership does not match"):
                self.service.start("unused", {})
        quiesce.assert_not_called()

    def test_cleanup_barrier_cannot_treat_lost_process_evidence_as_not_launched(self):
        self.export.side_effect = self.uncertain_native_export
        self.addCleanup(self.release_pending_lease)
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "ambiguous"}):
            with self.assertRaises(RuntimeError):
                self.generate()
        pending = self.service.optimized_view_cleanup_pending
        control = os.path.join(self.run, pending["export_relative"], "stage")
        os.unlink(os.path.join(control, "legacy_guardian_launch.json"))
        os.rmdir(control)
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt") as quiesce:
            with self.assertRaisesRegex(RuntimeError, "process evidence is missing"):
                self.service.exit_optimizer()
        quiesce.assert_not_called()
        self.assertTrue(self.service.optimized_view_export_active)
        self.assertIsNotNone(self.service.optimized_view_cleanup_lease.descriptor)

    def test_native_export_failure_releases_busy_state_for_review_and_retry(self):
        self.export.side_effect = RuntimeError("Native readback failed; diagnostic retained")
        with self.assertRaisesRegex(RuntimeError, "readback failed"):
            self.generate()
        self.assertFalse(self.service.optimized_view_export_active)
        self.assertEqual(self.service.lifecycle_operations, 0)
        self.export.side_effect = None
        self.assertTrue(self.generate()["ok"])
        self.assertEqual(self.export.call_count, 2)

    def test_native_error_with_dead_guardian_does_not_create_cleanup_barrier(self):
        self.export.side_effect = OptimizedSchematicError("Native save failed", {
            "phase": "stage", "command": {"returncode": 1, "guardian_cleanup": {"state": "dead"}}})
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt") as quiesce:
            with self.assertRaisesRegex(OptimizedSchematicError, "save failed"):
                self.generate()
        quiesce.assert_not_called()
        self.assertFalse(self.service.optimized_view_export_active)
        self.assertFalse(os.path.exists(os.path.join(self.root, ".optimized_schematic_cleanup_pending.json")))

    def test_immediately_recovered_native_cleanup_releases_lease_but_keeps_export_error(self):
        self.export.side_effect = self.uncertain_native_export
        with mock.patch("crescendo_engine.server.quiesce_legacy_guardian_attempt", return_value={"state": "dead"}):
            with self.assertRaisesRegex(OptimizedSchematicError, "termination unavailable"):
                self.generate()
        self.assertFalse(self.service.optimized_view_export_active)
        self.assertIsNone(self.service.optimized_view_cleanup_lease)
        self.assertFalse(os.path.exists(os.path.join(self.root, ".optimized_schematic_cleanup_pending.json")))

    def test_active_run_and_unproven_cleanup_do_not_enter_native_export(self):
        self.status["state"] = "running"
        self.write("status.json", self.status)
        with self.assertRaisesRegex(ValueError, "completed or stopped"):
            self.generate()
        self.status["state"] = "stopped"
        self.write("status.json", self.status)
        self.write("worker_shutdown.json", {"schema_version": "crescendo.worker_shutdown.v1", "workers": [{"ok": False}]})
        with self.assertRaisesRegex(RuntimeError, "cleanup is unproven"):
            self.generate()
        self.export.assert_not_called()
        self.assertFalse(self.service.optimized_view_export_active)
        self.assertEqual(self.service.lifecycle_operations, 0)

    def test_mock_and_incomplete_candidates_are_rejected(self):
        self.write("spec.json", {"evaluator": "mock"})
        with self.assertRaisesRegex(ValueError, "Mock"):
            self.generate()
        self.write("spec.json", {"evaluator": "real"})
        self.candidate["status"] = "planned"
        self.write("candidates/g000_c0000/candidate.json", self.candidate)
        with self.assertRaisesRegex(ValueError, "completed real"):
            self.generate()
        self.export.assert_not_called()

    def test_cross_service_runtime_lease_blocks_export_and_preserves_candidate(self):
        lease = RuntimeOptimizationLease(self.root, run_root=self.run).acquire()
        self.addCleanup(lease.release)
        with self.assertRaises(ContinuationLeaseBusy):
            self.generate()
        self.export.assert_not_called()
        self.assertFalse(self.service.optimized_view_export_active)
        self.assertEqual(self.service.lifecycle_operations, 0)
        self.assertTrue(os.path.isfile(os.path.join(self.run, "candidates", "g000_c0000", "candidate.json")))

    def test_symlink_context_is_rejected_before_export(self):
        path = os.path.join(self.run, "schematic_export_context.json")
        target = path + ".original"
        os.rename(path, target)
        os.symlink(target, path)
        with self.assertRaises(ValueError):
            self.generate()
        self.export.assert_not_called()

    def test_another_active_worker_prevents_export_even_for_finished_run(self):
        self.service.threads["another-run"] = mock.Mock()
        self.service.threads["another-run"].is_alive.return_value = True
        with self.assertRaisesRegex(RuntimeError, "Stop optimization"):
            self.generate()
        self.export.assert_not_called()

    def test_exit_cancels_inflight_export_before_waiting_for_lifecycle_cleanup(self):
        entered, cancelled = threading.Event(), threading.Event()
        outcomes = []

        def export(*args, **kwargs):
            entered.set()
            if not self.service.prepare_cancel_event.wait(1):
                raise AssertionError("Exit did not signal export cancellation")
            self.assertTrue(kwargs["cancel_check"]())
            cancelled.set()
            return {"ok": False, "cancelled": True}

        self.export.side_effect = export
        thread = threading.Thread(target=lambda: outcomes.append(self.generate()))
        thread.start()
        self.addCleanup(lambda: thread.join(1))
        self.assertTrue(entered.wait(10))
        self.assertTrue(self.service.optimized_view_export_active)
        with self.assertRaisesRegex(RuntimeError, "export is active"):
            self.service.start("unused-prepared", {})
        with self.assertRaisesRegex(RuntimeError, "export is active"):
            self.service.resume({})
        with self.assertRaisesRegex(RuntimeError, "Stop optimization"):
            self.generate()
        exited = self.service.exit_optimizer()
        thread.join(1)
        self.assertTrue(exited["exiting"])
        self.assertTrue(cancelled.is_set())
        self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, [{"ok": False, "cancelled": True}])
        self.assertEqual(self.service.lifecycle_operations, 0)
        self.assertFalse(self.service.optimized_view_export_active)


if __name__ == "__main__":
    unittest.main()
