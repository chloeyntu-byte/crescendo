from __future__ import print_function

import errno
import os
import stat
import sys
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.cadence import CommandCancelled, command_cancellation_scope, run_command
from crescendo_engine.continuation import RuntimeOptimizationLease
from crescendo_engine.runner import _WholeRunPersistentWorker
from crescendo_engine.server import OptimizerService
from crescendo_engine.resident_validation import ResidentValidationSession
from crescendo_engine.range_validation import run_range_validation
from crescendo_engine.workspace import (
    WorkspaceBuildFailure, build_workspace, source_design_identity,
)


def _fixture(root):
    source = os.path.join(root, "source", "USER")
    for cell, view in (("TB", "maestro"), ("TOP", "schematic"),
                       ("LEAF", "spectre"), ("UNUSED_DIGITAL", "schematic")):
        path = os.path.join(source, cell, view)
        os.makedirs(path)
        with open(os.path.join(path, "data.sdb" if view == "maestro" else "sch.oa"), "w") as stream:
            stream.write(cell)
    for name in ("data.dm", "prop.xx", "tech.db"):
        with open(os.path.join(source, name), "w") as stream:
            stream.write(name)
    setup = {"source": {"library_root": source,
                        "maestro_sdb": os.path.join(source, "TB", "maestro", "data.sdb")},
             "tests": [{"library": "USER", "cell": "TOP", "view": "schematic"}]}
    hierarchy = {"designs": [{"design": {"library": "USER", "cell": "TOP", "view": "schematic"},
                             "hierarchy": {"cells": [{"library": "USER", "cell": "TOP", "view": "schematic"}],
                                           "instances": [{"master_library": "USER", "master_cell": "LEAF"}]}}]}
    config = {"runtime": {"run_root": os.path.join(root, "runs"),
                           "range_validation_timeout_seconds": 1}, "cadence": {}}
    os.makedirs(config["runtime"]["run_root"])
    return source, setup, hierarchy, config


class OptionalValidationIndependenceTest(unittest.TestCase):
    def _service(self, root, capability=None, timeout=1):
        service = OptimizerService({"runtime": {
            "run_root": root, "range_validation_timeout_seconds": timeout,
            "persistent_min_free_disk_mib": 1,
        }})
        prepared = {"setup": {"tests": [{"name": "T", "enabled": True}], "selected_test": "T", "_source_tests": [{"name": "T", "enabled": True}]}, "parameters": [{"parameter_id": "w", "optimizable": True}]}
        if capability is not None:
            prepared["resident_validation"] = capability
        service.prepared["prepared"] = prepared
        return service

    def test_registers_job_before_native_generation_finishes(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root, {"state": "deferred"})
            entered, release = threading.Event(), threading.Event()
            def native(*args, **kwargs):
                entered.set()
                release.wait(1)
                args[5](state="completed", message="complete")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=native):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                self.assertTrue(entered.wait(1))
                self.assertEqual(response["state"], "queued")
                self.assertFalse(service.range_validation_status(response["job_id"])["blocking"])
                self.assertTrue(service.range_validation_threads[response["job_id"]].is_alive())
                release.set()
                service.range_validation_threads[response["job_id"]].join(1)

    def test_deadline_includes_native_generation_and_signals_cancel(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root, {"state": "deferred"}, timeout=0.05)
            cancelled = threading.Event()

            def native(*args, **kwargs):
                while not args[4].is_set():
                    cancelled.wait(0.005)
                cancelled.set()
                raise CommandCancelled()

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=native):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            status = service.range_validation_status(response["job_id"])
            self.assertEqual(status["state"], "failed")
            self.assertTrue(status["timed_out"])
            self.assertTrue(cancelled.is_set())
            self.assertFalse(status["blocking"])
            self.assertLess(len(status["message"]), 100)

    def test_real_optimization_lease_is_independent_of_callback_lease(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            entered, release = threading.Event(), threading.Event()

            def run(*args, **kwargs):
                entered.set()
                release.wait(1)
                args[5](state="completed", message="complete")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=run):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                self.assertTrue(entered.wait(1))
                lease = RuntimeOptimizationLease(root).acquire()
                self.assertTrue(lease.held)
                lease.release()
                release.set()
                service.range_validation_threads[response["job_id"]].join(1)

    def test_validation_can_start_during_owned_optimization(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            service.threads["optimization"] = mock.Mock(is_alive=mock.Mock(return_value=True))
            with mock.patch("crescendo_engine.server.run_range_validation",
                            side_effect=lambda *args, **kwargs: args[5](state="completed", message="complete")):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            self.assertEqual(service.range_validation_status(response["job_id"])["state"], "completed")

    def test_callback_workspace_failure_does_not_contaminate_prepared(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root, {"state": "deferred"})
            failure = WorkspaceBuildFailure(IOError(errno.EACCES, "audit source denied"), {"ok": True})
            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=failure):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            status = service.range_validation_status(response["job_id"])
            self.assertFalse(status["blocking"])
            self.assertNotIn("optimization_blocked_by", status)
            self.assertNotIn("workspace_preflight", service.prepared["prepared"])
            self.assertIn("audit source denied", status["detail"])
            self.assertLess(len(status["message"]), 100)

    def test_status_disk_error_preserves_original_callback_failure(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root, {"state": "deferred"})
            original_write = __import__("crescendo_engine.server", fromlist=["atomic_write_json"]).atomic_write_json
            writes = [0]

            def write(path, value):
                writes[0] += 1
                if writes[0] > 1:
                    raise OSError(122, "audit quota")
                original_write(path, value)

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=RuntimeError("audit native cause")), \
                    mock.patch("crescendo_engine.server.atomic_write_json", side_effect=write):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            status = service.range_validation_status(response["job_id"])
            self.assertIn("audit native cause", status["detail"])
            self.assertIn("audit quota", status["diagnostic_write_error"])

    def test_start_keeps_optional_job_running_instead_of_waiting(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            entered, release = threading.Event(), threading.Event()

            def run(*args, **kwargs):
                entered.set()
                release.wait(2)
                args[5](state="completed", message="complete")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=run):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                self.assertTrue(entered.wait(1))
                try:
                    with mock.patch.object(service, "_cancel_and_wait_range_validations",
                                           side_effect=AssertionError("Start waited for optional callback")), \
                            mock.patch.object(service, "_launch_registered", return_value={"run_id": "optimization_audit"}):
                        result = service.start("prepared", {
                            "evaluator": "mock", "variables": [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                        })
                    self.assertEqual(result["run_id"], "optimization_audit")
                    self.assertFalse(service.range_validation_cancels[response["job_id"]].is_set())
                finally:
                    release.set()
                    service.range_validation_threads[response["job_id"]].join(1)

    def test_total_deadline_terminates_nonresident_owned_command(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root, timeout=0.2)
            results = []

            def run(*args, **kwargs):
                try:
                    results.append(run_command(
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        root, os.environ.copy(), 1800, cancel_check=args[4].is_set,
                        termination_grace_seconds=0.05, termination_kill_seconds=0.05,
                    ))
                except CommandCancelled as error:
                    results.append(error.result)
                args[5](state="failed", detail="owned command ended after deadline")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=run):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(2)
            self.assertFalse(service.range_validation_threads[response["job_id"]].is_alive())
            self.assertTrue(results[0]["cancelled"])
            self.assertFalse(results[0]["termination_failed"])
            status = service.range_validation_status(response["job_id"])
            self.assertTrue(status["timed_out"])
            self.assertIn("owned command ended", status["detail"])

    def test_unproven_resident_cleanup_preserves_evidence_and_prevents_reuse(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root, {"state": "deferred"})
            session_root = os.path.join(root, "range_validation_resident", "prepared_prepared")
            session = ResidentValidationSession("prepared", {}, service.config, session_root)
            session.workspace = {"root": os.path.join(session_root, "workspace")}
            os.makedirs(session.workspace["root"])
            session.close = mock.Mock(side_effect=RuntimeError("audit owned process still live"))
            session.start = mock.Mock()

            service.resident_validation = session
            error = service._resident_cleanup_error("audit owned process still live", session)

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=error):
                response = service.validate_ranges("prepared", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            status = service.range_validation_status(response["job_id"])
            self.assertTrue(status["unsafe_to_optimize"])
            self.assertTrue(os.path.isdir(session.workspace["root"]))
            self.assertTrue(service._validation_resources_are_isolated())
            # Retained legacy evidence cannot become a reusable private OA
            # session or be cleared merely by saving an environment override.
            with self.assertRaisesRegex(RuntimeError, "cleanup unproven"):
                service._cancel_and_wait_range_validations(action="save EDA settings")
            session.start.assert_not_called()
            self.assertIs(service.resident_validation, session)

    def test_nonresident_unproven_command_cleanup_keeps_private_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = {"root": os.path.join(root, "minimum")}
            os.makedirs(workspace["root"])
            updates = []
            error = CommandCancelled("audit command cleanup unproven", {"termination_failed": True})
            prepared = {"parameters": [{"parameter_id": "w", "optimizable": True}],
                        "setup": {}, "hierarchy": {}}
            with mock.patch("crescendo_engine.netlist_providers.prepare_maestro_netlists", side_effect=error), \
                    mock.patch("crescendo_engine.private_netlist.reconcile_preparation", return_value={"cleanup_proven": False}), \
                    mock.patch("crescendo_engine.range_validation._safe_remove_workspace") as remove:
                run_range_validation({"job_root": root}, prepared,
                                     [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                                     {"runtime": {}}, threading.Event(), lambda **changes: updates.append(changes))
            remove.assert_not_called()
            self.assertTrue(updates[-1]["unsafe_to_optimize"])
            self.assertEqual(updates[-1]["state"], "failed")
            self.assertTrue(os.path.isdir(workspace["root"]))


class WorkspaceDependencyCopyTest(unittest.TestCase):
    def test_unrelated_cell_is_omitted_but_leaf_and_library_metadata_are_copied(self):
        with tempfile.TemporaryDirectory() as root:
            source, setup, hierarchy, config = _fixture(root)
            workspace = build_workspace(setup, hierarchy, config, os.path.join(config["runtime"]["run_root"], "workers", "audit", "workspace"))
            copied = workspace["copied_libraries"]["USER"]
            self.assertFalse(os.path.exists(os.path.join(copied, "UNUSED_DIGITAL")))
            self.assertTrue(os.path.isfile(os.path.join(copied, "LEAF", "spectre", "sch.oa")))
            for name in ("data.dm", "prop.xx", "tech.db"):
                self.assertTrue(os.path.isfile(os.path.join(copied, name)))
            self.assertEqual(workspace["source_identity"]["fingerprint"], workspace["copied_source_identity"]["fingerprint"])

    def test_readonly_selected_source_is_copied_without_source_chmod(self):
        with tempfile.TemporaryDirectory() as root:
            source, setup, hierarchy, config = _fixture(root)
            path = os.path.join(source, "TOP", "schematic", "sch.oa")
            os.chmod(path, 0o444)
            workspace = build_workspace(setup, hierarchy, config, os.path.join(config["runtime"]["run_root"], "workers", "audit", "workspace"))
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o444)
            private = os.path.join(workspace["copied_libraries"]["USER"], "TOP", "schematic", "sch.oa")
            self.assertTrue(stat.S_IMODE(os.stat(private).st_mode) & stat.S_IWUSR)

    def test_required_copy_failure_still_stops_real_worker_before_native_creation(self):
        with tempfile.TemporaryDirectory() as root:
            _source, setup, hierarchy, config = _fixture(root)
            prepared = {"setup": setup, "hierarchy": hierarchy,
                        "source_identity": source_design_identity(setup, hierarchy, config)}
            run = os.path.join(config["runtime"]["run_root"], "optimization_audit")
            worker = _WholeRunPersistentWorker(run, 0, prepared, [], config)
            with mock.patch("crescendo_engine.workspace._copy_private_file",
                            side_effect=IOError(errno.EACCES, "audit required cell unreadable")), \
                    mock.patch("crescendo_engine.runner.PersistentMaestroWorker") as native:
                with self.assertRaises(WorkspaceBuildFailure) as error:
                    worker._new_epoch()
            self.assertIn("audit required cell unreadable", str(error.exception))
            native.assert_not_called()

    def test_changed_actual_source_identity_still_stops_real_worker(self):
        with tempfile.TemporaryDirectory() as root:
            source, setup, hierarchy, config = _fixture(root)
            identity = source_design_identity(setup, hierarchy, config)
            with open(os.path.join(source, "TOP", "schematic", "sch.oa"), "w") as stream:
                stream.write("changed")
            worker = _WholeRunPersistentWorker(os.path.join(config["runtime"]["run_root"], "optimization_audit"),
                                               0, {"setup": setup, "hierarchy": hierarchy, "source_identity": identity}, [], config)
            with mock.patch("crescendo_engine.runner.PersistentMaestroWorker") as native:
                with self.assertRaisesRegex(WorkspaceBuildFailure, "changed after Maestro preparation"):
                    worker._new_epoch()
            native.assert_not_called()

    def test_copy_cancellation_stops_before_native_creation(self):
        with tempfile.TemporaryDirectory() as root:
            _source, setup, hierarchy, config = _fixture(root)
            cancelled = threading.Event()
            from crescendo_engine.workspace import _copy_private_file

            def copy_then_cancel(source, destination):
                result = _copy_private_file(source, destination)
                cancelled.set()
                return result

            with mock.patch("crescendo_engine.workspace._copy_private_file", side_effect=copy_then_cancel):
                with self.assertRaises(WorkspaceBuildFailure) as error:
                    with command_cancellation_scope(cancelled):
                        build_workspace(setup, hierarchy, config, os.path.join(config["runtime"]["run_root"], "workers", "audit", "workspace"))
            self.assertIsInstance(error.exception.cause, CommandCancelled)
