import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.server import OptimizerService


class _RegisteredRunThread(object):
    def __init__(self):
        self.alive = True
        self.join_calls = []

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.join_calls.append(timeout)
        self.alive = False


class _NaturallyFinishingThread(object):
    def __init__(self):
        self.checks = 0

    def is_alive(self):
        self.checks += 1
        return self.checks == 1

    def join(self, timeout=None):
        raise AssertionError("a naturally finished thread must not be joined")


class OptimizerServiceLifecycleTest(unittest.TestCase):
    def _wait_for_closing(self, service, timeout=1.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with service.lifecycle_condition:
                if service.closing:
                    return True
            time.sleep(0.005)
        return False

    def test_exit_waits_for_launch_registration_then_stops_registered_run(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root,
                "optimizer_exit_timeout_seconds": 2,
                "persistent_min_free_disk_mib": 0,
            }})
            launch_entered = threading.Event()
            allow_registration = threading.Event()
            exit_done = threading.Event()
            registered_thread = _RegisteredRunThread()
            launch_results = []
            launch_errors = []
            exit_results = []
            exit_errors = []

            def launch_registered(_spec, _prepared, **_kwargs):
                launch_entered.set()
                allow_registration.wait(1)
                # Production registration writes a real status before publishing
                # a thread. Keep that invariant while testing the exit race.
                run_root = os.path.join(root, "optimization-race")
                os.makedirs(run_root)
                with open(os.path.join(run_root, "status.json"), "w") as stream:
                    json.dump({"schema_version": "crescendo.run_status.v1", "run_id": "optimization-race",
                               "run_root": run_root, "state": "queued"}, stream)
                with service.lock:
                    service.threads["optimization-race"] = registered_thread
                return {"run_id": "optimization-race"}

            def launch_target():
                try:
                    launch_results.append(service._launch(
                        {"evaluator": "mock"}, {},
                    ))
                except Exception as error:
                    launch_errors.append(error)

            def exit_target():
                try:
                    exit_results.append(service.exit_optimizer())
                except Exception as error:
                    exit_errors.append(error)
                finally:
                    exit_done.set()

            with mock.patch(
                    "crescendo_engine.server.startup_storage_preflight"), \
                    mock.patch.object(
                        service, "_cancel_and_wait_range_validations",
                    ), mock.patch.object(
                        service, "_clear_retained_logs", return_value={"removed": []},
                    ), mock.patch.object(
                        service, "_close_resident_validation",
                    ), mock.patch.object(
                        service, "_launch_registered", side_effect=launch_registered,
                    ), mock.patch.object(
                        service, "stop", return_value={"stopping": True},
                    ) as stop:
                launch_thread = threading.Thread(target=launch_target)
                launch_thread.start()
                self.assertTrue(launch_entered.wait(10))

                exit_thread = threading.Thread(target=exit_target)
                exit_thread.start()
                self.assertTrue(self._wait_for_closing(service))
                self.assertFalse(exit_done.is_set())

                allow_registration.set()
                launch_thread.join(1)
                exit_thread.join(1)

            self.assertFalse(launch_thread.is_alive())
            self.assertFalse(exit_thread.is_alive())
            self.assertEqual(launch_errors, [])
            self.assertEqual(exit_errors, [])
            self.assertEqual(launch_results[0]["run_id"], "optimization-race")
            self.assertTrue(exit_results[0]["exiting"])
            stop.assert_called_once_with("optimization-race")
            self.assertEqual(len(registered_thread.join_calls), 1)
            self.assertEqual(service.lifecycle_operations, 0)
            self.assertTrue(service.closing)

    def test_successful_exit_rejects_new_resource_operations(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root,
                "optimizer_exit_timeout_seconds": 1,
            }})
            with mock.patch.object(
                    service, "_cancel_and_wait_range_validations",
                    ), mock.patch.object(
                        service, "_clear_retained_logs", return_value={"removed": []},
                    ), mock.patch.object(service, "_close_resident_validation"):
                result = service.exit_optimizer()
            self.assertTrue(result["exiting"])

            with mock.patch.object(service, "_prepare_registered") as prepare:
                with self.assertRaisesRegex(RuntimeError, "optimizer is closing"):
                    service.prepare("/tmp/not-opened")
            prepare.assert_not_called()

            with mock.patch.object(service, "_validate_ranges_registered") as validate:
                with self.assertRaisesRegex(RuntimeError, "optimizer is closing"):
                    service.validate_ranges("prepared", [])
            validate.assert_not_called()

    def test_exit_waits_for_entire_resume_workflow(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root,
                "optimizer_exit_timeout_seconds": 2,
            }})
            resume_entered = threading.Event()
            allow_resume_finish = threading.Event()
            exit_done = threading.Event()
            errors = []

            def resume_registered(_checkpoint, path=None):
                resume_entered.set()
                allow_resume_finish.wait(1)
                return {"run_id": "resume-dry-run", "path": path}

            def resume_target():
                try:
                    service.resume({}, path="/not-opened")
                except Exception as error:
                    errors.append(error)

            def exit_target():
                try:
                    service.exit_optimizer()
                except Exception as error:
                    errors.append(error)
                finally:
                    exit_done.set()

            with mock.patch.object(
                    service, "_resume_registered", side_effect=resume_registered,
                    ), mock.patch.object(
                        service, "_cancel_and_wait_range_validations",
                    ), mock.patch.object(
                        service, "_clear_retained_logs", return_value={"removed": []},
                    ), mock.patch.object(service, "_close_resident_validation"):
                resume_thread = threading.Thread(target=resume_target)
                resume_thread.start()
                self.assertTrue(resume_entered.wait(10))
                exit_thread = threading.Thread(target=exit_target)
                exit_thread.start()
                self.assertTrue(self._wait_for_closing(service))
                self.assertFalse(exit_done.is_set())
                allow_resume_finish.set()
                resume_thread.join(1)
                exit_thread.join(1)

            self.assertEqual(errors, [])
            self.assertFalse(resume_thread.is_alive())
            self.assertFalse(exit_thread.is_alive())
            self.assertEqual(service.lifecycle_operations, 0)

    def test_eda_reconfiguration_and_start_are_mutually_exclusive(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            update_entered = threading.Event()
            allow_update_finish = threading.Event()

            def update_registered(_body):
                update_entered.set()
                allow_update_finish.wait(1)
                return {"ok": True}

            with mock.patch.object(
                    service, "_update_eda_setup_registered",
                    side_effect=update_registered,
            ):
                update_thread = threading.Thread(
                    target=lambda: service.update_eda_setup({"mode": "auto"}),
                )
                update_thread.start()
                self.assertTrue(update_entered.wait(10))
                with mock.patch.object(service, "_start_registered") as start:
                    with self.assertRaisesRegex(
                            RuntimeError, "EDA settings update is in progress"):
                        service.start("prepared", {})
                start.assert_not_called()
                allow_update_finish.set()
                update_thread.join(1)

            self.assertFalse(update_thread.is_alive())
            self.assertFalse(service.eda_reconfiguring)
            self.assertEqual(service.lifecycle_operations, 0)

            start_entered = threading.Event()
            allow_start_finish = threading.Event()

            def start_registered(_prepared_id, _spec):
                start_entered.set()
                allow_start_finish.wait(1)
                return {"run_id": "start-dry-run"}

            with mock.patch.object(
                    service, "_start_registered", side_effect=start_registered,
            ):
                start_thread = threading.Thread(
                    target=lambda: service.start("prepared", {}),
                )
                start_thread.start()
                self.assertTrue(start_entered.wait(10))
                with mock.patch.object(service, "_update_eda_setup_registered") as update:
                    with self.assertRaisesRegex(
                            RuntimeError, "resource operations are active"):
                        service.update_eda_setup({"mode": "auto"})
                update.assert_not_called()
                allow_start_finish.set()
                start_thread.join(1)

            self.assertFalse(start_thread.is_alive())
            self.assertFalse(service.eda_reconfiguring)
            self.assertEqual(service.lifecycle_operations, 0)

    def test_exit_timeout_is_fail_closed_and_reports_inflight_operations(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root,
                "optimizer_exit_timeout_seconds": 0.01,
            }})
            service._begin_lifecycle_operation("test an in-flight operation")
            try:
                with self.assertRaisesRegex(
                        RuntimeError, "1 resource operation.*finish registration"):
                    service.exit_optimizer()
            finally:
                service._end_lifecycle_operation()
            self.assertTrue(service.closing)
            self.assertEqual(service.lifecycle_operations, 0)

    def test_exit_tolerates_run_finishing_after_active_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root,
                "optimizer_exit_timeout_seconds": 1,
            }})
            service.threads["optimization-natural"] = _NaturallyFinishingThread()
            with mock.patch.object(
                    service, "_cancel_and_wait_range_validations",
                    ), mock.patch.object(
                        service, "_close_resident_validation",
                    ), mock.patch.object(
                        service, "_clear_retained_logs", return_value={"removed": []},
                    ) as clear_logs:
                result = service.exit_optimizer()
            self.assertTrue(result["exiting"])
            self.assertEqual(result["stopped_runs"], ["optimization-natural"])
            clear_logs.assert_called_once_with(session_only=True)

    def test_exit_rejects_invalid_timeout_values(self):
        with tempfile.TemporaryDirectory() as root:
            for value in (float("nan"), float("inf"), -1, "not-a-number"):
                service = OptimizerService({"runtime": {
                    "run_root": root,
                    "optimizer_exit_timeout_seconds": value,
                }})
                with self.subTest(value=value), self.assertRaisesRegex(
                        ValueError, "finite non-negative"):
                    service.exit_optimizer()

    def test_exit_passes_one_remaining_budget_to_validation_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root,
                "optimizer_exit_timeout_seconds": 1,
            }})
            with mock.patch.object(
                    service, "_cancel_and_wait_range_validations",
                    ) as cancel, mock.patch.object(
                        service, "_close_resident_validation",
                    ) as close, mock.patch.object(
                        service, "_clear_retained_logs", return_value={"removed": []},
                    ):
                service.exit_optimizer()
            cancel_timeout = cancel.call_args[1]["timeout_seconds"]
            close_timeout = close.call_args[1]["timeout_seconds"]
            self.assertGreaterEqual(cancel_timeout, 0)
            self.assertLessEqual(cancel_timeout, 1)
            self.assertGreaterEqual(close_timeout, 0)
            self.assertLessEqual(close_timeout, cancel_timeout)

    def test_concurrent_exit_calls_are_serialized(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            first_entered = threading.Event()
            allow_finish = threading.Event()
            state_lock = threading.Lock()
            state = {"active": 0, "maximum": 0}
            results = []

            def serialized():
                with state_lock:
                    state["active"] += 1
                    state["maximum"] = max(state["maximum"], state["active"])
                    first_entered.set()
                allow_finish.wait(1)
                with state_lock:
                    state["active"] -= 1
                return {"exiting": True}

            def exit_target():
                results.append(service.exit_optimizer())

            with mock.patch.object(
                    service, "_exit_optimizer_serialized", side_effect=serialized,
            ) as cleanup:
                first = threading.Thread(target=exit_target)
                second = threading.Thread(target=exit_target)
                first.start()
                self.assertTrue(first_entered.wait(10))
                second.start()
                time.sleep(0.02)
                self.assertEqual(cleanup.call_count, 1)
                allow_finish.set()
                first.join(1)
                second.join(1)

            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(cleanup.call_count, 2)
            self.assertEqual(state["maximum"], 1)
            self.assertEqual(len(results), 2)


if __name__ == "__main__":
    unittest.main()
