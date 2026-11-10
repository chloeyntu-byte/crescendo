import copy
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.cadence import (
    CommandCancelled, check_command_cancellation, command_cancellation_scope,
    run_command,
)
from crescendo_engine.server import OptimizerService


class PrepareCancellationTest(unittest.TestCase):
    def _service(self, root):
        path = os.path.join(root, "maestro.sdb")
        with open(path, "w") as stream:
            stream.write('<setupdb Type="maestro"><active><tests><test Name="T"/></tests></active></setupdb>')
        service = OptimizerService({"runtime": {
            "run_root": root, "optimizer_exit_timeout_seconds": 5,
        }})
        service._test_setup_path = path
        return service

    def _prepare_thread(self, service, errors):
        def prepare():
            try:
                service.prepare(service._test_setup_path, initialize_validation=False)
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=prepare)
        thread.start()
        return thread

    def _exit_without_unrelated_resources(self, service):
        with mock.patch.object(service, "_cancel_and_wait_range_validations"), \
                mock.patch.object(service, "_close_resident_validation"), \
                mock.patch.object(service, "_clear_retained_logs", return_value={}):
            return service.exit_optimizer()

    def test_exit_interrupts_native_startup_lock_without_launch(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            startup_lock = threading.Lock()
            startup_lock.acquire()
            entered = threading.Event()
            errors = []

            def prepare_native(*_args, **_kwargs):
                entered.set()
                run_command(["/not-launched"], root, {}, 1800,
                            startup_lock=startup_lock, lock_timeout_seconds=1800)

            try:
                with mock.patch("crescendo_engine.server.prepare_setup", side_effect=prepare_native), \
                        mock.patch("crescendo_engine.cadence.subprocess.Popen") as popen:
                    thread = self._prepare_thread(service, errors)
                    self.assertTrue(entered.wait(1))
                    started = time.monotonic()
                    result = self._exit_without_unrelated_resources(service)
                    thread.join(1)
                    elapsed = time.monotonic() - started
                    popen.assert_not_called()
            finally:
                startup_lock.release()
            self.assertFalse(thread.is_alive())
            self.assertTrue(result["exiting"])
            self.assertLess(elapsed, 1)
            self.assertIsInstance(errors[0], CommandCancelled)
            self.assertTrue(errors[0].result["cancelled"])
            self.assertEqual(service.prepared, {})
            self.assertEqual(service.lifecycle_operations, 0)

    def test_exit_terminates_only_prepare_owned_native_group(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            marker = os.path.join(root, "owned-pid")
            errors = []
            unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
            # Publish readiness only after the complete PID has been closed.
            code = ("import os,time; p=%r; f=open(p+'.tmp','w'); f.write(str(os.getpid()));"
                    "f.close(); os.replace(p+'.tmp',p); time.sleep(30)") % marker

            def prepare_native(*_args, **_kwargs):
                run_command([sys.executable, "-c", code], root, os.environ.copy(), 1800)

            try:
                with mock.patch("crescendo_engine.server.prepare_setup", side_effect=prepare_native):
                    thread = self._prepare_thread(service, errors)
                    deadline = time.monotonic() + 2
                    while not os.path.isfile(marker) and time.monotonic() < deadline:
                        time.sleep(0.005)
                    self.assertTrue(os.path.isfile(marker))
                    with open(marker) as stream:
                        owned_pid = int(stream.read())
                    started = time.monotonic()
                    result = self._exit_without_unrelated_resources(service)
                    thread.join(1)
                    elapsed = time.monotonic() - started
                    self.assertIsNone(unrelated.poll())
                    with self.assertRaises(OSError):
                        os.kill(owned_pid, 0)
            finally:
                unrelated.terminate()
                unrelated.wait(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(result["exiting"])
            self.assertLess(elapsed, 2)
            self.assertIsInstance(errors[0], CommandCancelled)
            self.assertFalse(errors[0].cleanup_failed)

    def test_python_prepare_checks_cancel_before_registration(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            entered = threading.Event()
            errors = []

            def prepare_python(*_args, **_kwargs):
                entered.set()
                while True:
                    check_command_cancellation()
                    time.sleep(0.005)

            with mock.patch("crescendo_engine.server.prepare_setup", side_effect=prepare_python):
                thread = self._prepare_thread(service, errors)
                self.assertTrue(entered.wait(1))
                result = self._exit_without_unrelated_resources(service)
                thread.join(1)
            self.assertTrue(result["exiting"])
            self.assertFalse(thread.is_alive())
            self.assertIsInstance(errors[0], CommandCancelled)
            self.assertEqual(service.prepared, {})

    def test_cancel_after_probe_success_does_not_register_prepared(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)

            def prepare_python(*_args, **_kwargs):
                service.prepare_cancel_event.set()
                return {"setup": {}, "hierarchy": {}, "eda_context": {"pdk": {}}}

            with mock.patch("crescendo_engine.server.prepare_setup", side_effect=prepare_python):
                with self.assertRaises(CommandCancelled):
                    service.prepare(service._test_setup_path, initialize_validation=False)
            self.assertEqual(service.prepared, {})
            self.assertEqual(service.lifecycle_operations, 0)

    def test_unproven_owned_cleanup_cannot_report_successful_exit(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)

            def failed_cleanup(*_args, **_kwargs):
                run_command(["/not-launched"], root, {}, 1800)

            with mock.patch("crescendo_engine.server.prepare_setup", side_effect=failed_cleanup), \
                    mock.patch("crescendo_engine.cadence._run_command_impl", return_value={
                        "cancelled": True, "termination_failed": True,
                    }):
                with self.assertRaises(CommandCancelled) as caught:
                    service.prepare(service._test_setup_path, initialize_validation=False)
            self.assertTrue(caught.exception.cleanup_failed)
            with self.assertRaisesRegex(RuntimeError, "could not prove read-only probe cleanup"):
                self._exit_without_unrelated_resources(service)
            self.assertEqual(service.prepared, {})

    def test_scope_is_thread_local_and_restored_after_exception(self):
        cancelled = threading.Event()
        entered = threading.Event()
        check_now = threading.Event()
        results = []

        def other_thread():
            with command_cancellation_scope(threading.Event()):
                entered.set()
                check_now.wait(1)
                check_command_cancellation()
                results.append("not cancelled")

        with self.assertRaises(CommandCancelled):
            with command_cancellation_scope(cancelled):
                thread = threading.Thread(target=other_thread)
                thread.start()
                self.assertTrue(entered.wait(1))
                cancelled.set()
                check_now.set()
                thread.join(1)
                check_command_cancellation()
        check_command_cancellation()
        self.assertEqual(results, ["not cancelled"])

    def test_scope_preserves_existing_explicit_cancel_check(self):
        with command_cancellation_scope(threading.Event()):
            with mock.patch("crescendo_engine.cadence.subprocess.Popen") as popen:
                with self.assertRaises(CommandCancelled):
                    run_command(["/not-launched"], "/tmp", {}, 1800,
                                cancel_check=lambda: True)
        popen.assert_not_called()

    def test_cancel_event_is_not_stored_in_config(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            self.assertEqual(copy.deepcopy(service.config), service.config)
            self.assertNotIn("prepare_cancel_event", service.config)


if __name__ == "__main__":
    unittest.main()
