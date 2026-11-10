"""Exercise real controller child exits and owned quarantine deletion failures."""
import concurrent.futures
import errno
import fcntl
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.persistent_controller import (
    PersistentControllerError, PersistentControllerProcessError, PersistentControllerProxy,
)
from crescendo_engine.retention import (
    ArtifactCleanupError, cleanup_abandoned_controller_epoch, resume_pending_cleanups,
)
from crescendo_engine.utils import load_json
from tests import test_persistent_controller as controller_fixture


class CleanupProofSeparationTests(unittest.TestCase):
    def _proxy(self, root, script=None):
        fixture = controller_fixture.PersistentControllerTest()
        script = script or fixture._fake_script(root)
        return PersistentControllerProxy(
            root, 0, {"setup": {}, "hierarchy": {}}, [], fixture._config(root),
            command_builder=lambda bootstrap: [sys.executable, script, bootstrap],
        )

    @staticmethod
    def _nonempty(payload, **_kwargs):
        control = os.path.join(payload, "control")
        os.makedirs(control, exist_ok=True)
        with open(os.path.join(control, "late-state.json"), "w") as stream:
            stream.write("owned delayed artifact")
        raise OSError(errno.ENOTEMPTY, "Directory not empty", control)

    def _assert_stopped_with_artifacts(self, result):
        self.assertTrue(result["process_termination_proven"])
        self.assertTrue(result["remote_stop_proven"])
        self.assertTrue(result["termination_proven"])
        self.assertFalse(result["process_still_alive"])
        self.assertFalse(result["artifact_cleanup_complete"])
        self.assertFalse(result["cleanup"]["ok"])
        self.assertEqual(result["cleanup"]["state"], "artifact_cleanup_failed")
        self.assertIn("artifact directory cleanup failed", result["cleanup_error"])
        self.assertNotIn("termination was not proven", result["cleanup_error"])

    def test_startup_error_survives_delete_failure_and_repeated_retry_close(self):
        with tempfile.TemporaryDirectory() as root:
            script = os.path.join(root, "failed_start.py")
            with open(script, "w") as stream:
                stream.write("import time,sys\ntime.sleep(0.1)\nsys.exit(7)\n")
            proxy = self._proxy(root, script)
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=self._nonempty):
                with self.assertRaisesRegex(PersistentControllerProcessError, "exited before ready with code 7"):
                    proxy.start()
            result = proxy._last_termination
            self._assert_stopped_with_artifacts(result)
            detail = result["cleanup"]["controller_epoch"]["detail"]
            self.assertEqual(detail["attempt_count"], 3)
            self.assertEqual(detail["errno"], errno.ENOTEMPTY)
            self.assertTrue(os.path.isabs(detail["failed_path"]))
            self.assertEqual(detail["remaining_names"], ["late-state.json"])
            epoch = proxy.epoch_root
            self.assertFalse(os.path.lexists(epoch))
            self.assertTrue(os.path.isdir(os.path.join(root, ".cleanup_pending")))
            closed = proxy.close()
            self.assertTrue(closed["cleanup"]["ok"])
            self.assertTrue(closed["artifact_cleanup_complete"])
            self.assertTrue(closed["controller_identity_proof"]["retained_after_quarantine"])
            self.assertFalse(os.path.lexists(epoch))
            self.assertFalse(os.path.lexists(os.path.join(root, ".cleanup_pending")))
            self.assertEqual(proxy.close(), closed)
            self.assertFalse(os.path.lexists(epoch))

    def test_normal_close_and_stop_report_directory_failure_then_retry(self):
        for operation in ("close", "terminate_and_prove"):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as root:
                proxy = self._proxy(root).start()
                with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=self._nonempty):
                    result = proxy.close() if operation == "close" else proxy.terminate_and_prove("Stop")
                    self._assert_stopped_with_artifacts(result)
                    with self.assertRaisesRegex(PersistentControllerError, "not safely reclaimed"):
                        proxy.restart()
                self.assertTrue(proxy.close()["artifact_cleanup_complete"])

    def test_worker_deletion_error_keeps_first_error_and_other_source_views(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root).start()
            epoch = os.path.join(root, "workers", "worker_000", "epoch_000")
            workspace = os.path.join(epoch, "workspace")
            os.makedirs(workspace)
            with open(os.path.join(workspace, "workspace.json"), "w") as stream:
                json.dump({"root": workspace, "source_libraries": {}, "source_snapshots": {}}, stream)
            failure_path = os.path.join(root, "startup_failure.json")
            source_path = os.path.join(root, "original-schematic.oa")
            permanent = os.path.join(root, "schematic_Optimized.oa")
            for path, value in ((failure_path, "native Value stack overflow"),
                                (source_path, "original source"), (permanent, "permanent solution")):
                with open(path, "w") as stream:
                    stream.write(value)
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=self._nonempty):
                result = proxy.terminate_and_prove("bootstrap failed: native Value stack overflow")
            self._assert_stopped_with_artifacts(result)
            self.assertTrue(os.path.isdir(proxy.epoch_root))
            self.assertFalse(os.path.lexists(epoch))
            self.assertEqual(result["reason"], "bootstrap failed: native Value stack overflow")
            self.assertEqual(result["cleanup"]["worker_epochs"][0]["detail"]["remaining_names"], ["late-state.json"])
            self.assertTrue(proxy.close()["artifact_cleanup_complete"])
            for path, value in ((failure_path, "native Value stack overflow"),
                                (source_path, "original source"), (permanent, "permanent solution")):
                with open(path) as stream:
                    self.assertEqual(stream.read(), value)

    def test_transient_nonempty_retries_owned_payload_without_hiding_result(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root).start()
            real_delete = shutil.rmtree
            attempts = []
            def transient(payload, **kwargs):
                attempts.append(payload)
                if len(attempts) == 1:
                    return self._nonempty(payload)
                return real_delete(payload, **kwargs)
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=transient):
                result = proxy.close()
            self.assertTrue(result["artifact_cleanup_complete"])
            self.assertEqual(len(attempts), 2)
            retry = result["cleanup"]["controller_epoch"]["artifact_cleanup"]
            self.assertTrue(retry["artifact_cleanup_complete"])
            self.assertEqual(retry["attempt_count"], 2)
            self.assertFalse(os.path.lexists(os.path.join(root, ".cleanup_pending")))

    def test_ambiguous_identity_and_unknown_remote_jobs_retain_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root).start()
            state = load_json(proxy.state_path)
            state["token"] = "foreign"
            with open(proxy.state_path, "w") as stream:
                json.dump(state, stream)
            result = proxy.close()
            self.assertFalse(result["process_termination_proven"])
            self.assertFalse(result["termination_proven"])
            self.assertFalse(result["artifact_cleanup_complete"])
            self.assertTrue(os.path.isdir(proxy.epoch_root))
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root).start()
            epoch = os.path.join(root, "workers", "worker_000", "epoch_000")
            os.makedirs(epoch)
            with open(os.path.join(epoch, "worker_state.json"), "w") as stream:
                json.dump({"state": "new", "pid": None, "ocean_pid": None,
                           "remote_batch": {"simulation_submitted": True}}, stream)
            result = proxy.terminate_and_prove("unknown remote jobs")
            self.assertTrue(result["process_termination_proven"])
            self.assertFalse(result["remote_stop_proven"])
            self.assertFalse(result["termination_proven"])
            self.assertTrue(os.path.isdir(epoch))
            self.assertTrue(os.path.isdir(proxy.epoch_root))

    def test_concurrent_stop_and_close_share_one_cleanup_owner(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root).start()
            real_delete = shutil.rmtree
            active = [0, 0]
            counts_lock = threading.Lock()
            def slowed_delete(payload, **kwargs):
                with counts_lock:
                    active[0] += 1
                    active[1] = max(active[1], active[0])
                try:
                    time.sleep(0.03)
                    return real_delete(payload, **kwargs)
                finally:
                    with counts_lock:
                        active[0] -= 1
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=slowed_delete):
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    first = executor.submit(proxy.terminate_and_prove, "concurrent Stop")
                    second = executor.submit(proxy.close)
                    results = [first.result(timeout=5), second.result(timeout=5)]
            self.assertEqual(active[1], 1)
            self.assertTrue(all(row["artifact_cleanup_complete"] for row in results))
            self.assertFalse(os.path.lexists(proxy.epoch_root))

    def test_held_controller_liveness_lock_is_process_uncertainty(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = self._proxy(root).start()
            descriptor = os.open(proxy.liveness_lock_path, os.O_RDWR)
            try:
                # Acquire only after the real child releases its inherited lock.
                proxy.process.terminate()
                proxy.process.wait(timeout=3)
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = proxy.close()
                self.assertFalse(result["process_termination_proven"])
                self.assertFalse(result["termination_proven"])
                self.assertEqual(result["cleanup"]["state"], "unproven")
                self.assertTrue(os.path.isdir(proxy.epoch_root))
                self.assertIn("liveness lock remains held", result["cleanup_error"])
            finally:
                os.close(descriptor)
            self.assertTrue(proxy.close()["artifact_cleanup_complete"])

    def test_cross_process_recovery_waits_for_unique_deletion_writer(self):
        with tempfile.TemporaryDirectory() as root:
            epoch = os.path.join(root, "controllers", "slot_000", "epoch_000")
            os.makedirs(epoch)
            with open(os.path.join(epoch, "controller_liveness.lock"), "w") as stream:
                stream.write("released")
            entered = threading.Event()
            release = threading.Event()
            real_delete = shutil.rmtree
            def paused_delete(payload, **kwargs):
                entered.set()
                if not release.wait(timeout=5):
                    raise RuntimeError("test deletion barrier timed out")
                return real_delete(payload, **kwargs)
            process = None
            with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=paused_delete):
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    cleanup = executor.submit(cleanup_abandoned_controller_epoch, epoch, root,
                                              {"ok": True, "kind": "recorded_pid_is_dead"})
                    self.assertTrue(entered.wait(timeout=3))
                    try:
                        process = subprocess.Popen([sys.executable, "-c",
                            "import sys; from crescendo_engine.retention import resume_pending_cleanups; "
                            "resume_pending_cleanups(sys.argv[1])", root],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        with self.assertRaises(subprocess.TimeoutExpired):
                            process.communicate(timeout=0.15)
                    finally:
                        release.set()
                    self.assertTrue(cleanup.result(timeout=5)["ok"])
                    _stdout, stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 0, stderr)
            self.assertFalse(os.path.lexists(os.path.join(root, ".cleanup_pending")))

    def test_permission_error_does_not_retry_and_persists_precise_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            epoch = os.path.join(root, "controllers", "slot_000", "epoch_000")
            os.makedirs(epoch)
            with open(os.path.join(epoch, "controller_liveness.lock"), "w") as stream:
                stream.write("released")
            with mock.patch("crescendo_engine.retention.shutil.rmtree",
                            side_effect=OSError(errno.EACCES, "permission denied")) as delete:
                with self.assertRaises(ArtifactCleanupError) as raised:
                    cleanup_abandoned_controller_epoch(epoch, root, {"ok": True, "kind": "recorded_pid_is_dead"})
            self.assertEqual(delete.call_count, 1)
            self.assertEqual(raised.exception.detail["errno"], errno.EACCES)
            pending = os.path.join(root, ".cleanup_pending")
            manifest = load_json(os.path.join(pending, os.listdir(pending)[0], "manifest.json"))
            self.assertEqual(manifest["artifact_cleanup"]["attempt_count"], 1)
            self.assertEqual(manifest["state"], "quarantined")
            self.assertTrue(resume_pending_cleanups(root)[0]["removed"])


if __name__ == "__main__":
    unittest.main()
