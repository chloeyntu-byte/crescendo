from __future__ import print_function

import os
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.resident_validation import ResidentValidationSession


class _Input(object):
    def __init__(self):
        self.values = []

    def write(self, value):
        self.values.append(value)

    def flush(self):
        pass


class _Process(object):
    def __init__(self):
        self.stdin = _Input()
        self.returncode = None

    def poll(self):
        return self.returncode


class ResidentValidationSessionTest(unittest.TestCase):
    def test_cleanup_lock_wait_uses_the_total_budget_and_retains_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            session = ResidentValidationSession("prepared", {}, {"runtime": {"run_root": root}},
                os.path.join(root, "resident"))
            locked, release = threading.Event(), threading.Event()
            def hold_lock():
                with session._lock:
                    locked.set()
                    release.wait()
            thread = threading.Thread(target=hold_lock)
            thread.start()
            self.assertTrue(locked.wait(1))
            started = time.monotonic()
            try:
                with self.assertRaisesRegex(RuntimeError, "deadline exceeded waiting for the session lock"):
                    session.close(timeout_seconds=0.02)
                self.assertLess(time.monotonic() - started, 0.3)
                self.assertFalse(session.closed)
                self.assertTrue(os.path.isdir(session.control_root))
            finally:
                release.set()
                thread.join(1)

    def test_graceful_and_term_wait_share_one_budget(self):
        for term_succeeds in (False, True):
            with tempfile.TemporaryDirectory() as root:
                clock = [0.0]
                session = ResidentValidationSession("prepared", {}, {"runtime": {"run_root": root}},
                    os.path.join(root, "resident"), clock=lambda: clock[0])
                process = _Process()
                process.pid = 123456
                waits = []
                def wait(timeout):
                    waits.append(timeout)
                    if len(waits) == 2 and term_succeeds:
                        clock[0] += 0.1
                        process.returncode = 0
                        return 0
                    clock[0] += timeout
                    raise subprocess.TimeoutExpired("owned guardian", timeout)
                process.wait = wait
                session.process = process
                session._state = mock.Mock(side_effect=lambda: {
                    "token": session.token, "status": "exited" if process.returncode == 0 else "running",
                    "group_cleanup_complete": process.returncode == 0})
                with mock.patch("crescendo_engine.resident_validation.os.getpgid", return_value=process.pid), \
                        mock.patch("crescendo_engine.resident_validation.os.killpg") as kill:
                    if term_succeeds:
                        self.assertTrue(session.close(timeout_seconds=15)["cleanup_proven"])
                    else:
                        with self.assertRaisesRegex(RuntimeError, "cleanup deadline exceeded"):
                            session.close(timeout_seconds=15)
                        self.assertTrue(os.path.isdir(session.control_root))
                        self.assertFalse(session.closed)
                self.assertEqual(waits, [10.0, 5.0])
                self.assertLessEqual(clock[0], 15)
                kill.assert_called_once_with(process.pid, signal.SIGTERM)

    def test_cleanup_budget_does_not_signal_an_unowned_group(self):
        with tempfile.TemporaryDirectory() as root:
            clock = [0.0]
            session = ResidentValidationSession("prepared", {}, {"runtime": {"run_root": root}},
                os.path.join(root, "resident"), clock=lambda: clock[0])
            process = _Process()
            process.pid = 123456
            def wait(timeout):
                clock[0] += timeout
                raise subprocess.TimeoutExpired("owned guardian", timeout)
            process.wait = wait
            session.process = process
            with mock.patch("crescendo_engine.resident_validation.os.getpgid", return_value=999), \
                    mock.patch("crescendo_engine.resident_validation.os.killpg") as kill:
                with self.assertRaisesRegex(RuntimeError, "cleanup could not be proven"):
                    session.close(timeout_seconds=1)
            kill.assert_not_called()
            self.assertFalse(session.closed)
            self.assertTrue(os.path.isdir(session.control_root))

    def test_zero_cleanup_budget_retains_owned_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            session = ResidentValidationSession("prepared", {}, {"runtime": {"run_root": root}},
                os.path.join(root, "resident"))
            session.process = _Process()
            with self.assertRaisesRegex(RuntimeError, "deadline exceeded"):
                session.close(timeout_seconds=0)
            self.assertEqual(session.process.stdin.values, [])
            self.assertTrue(os.path.isdir(session.control_root))
            self.assertFalse(session.closed)

    def test_exited_process_cannot_substitute_for_matching_guardian_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            session = ResidentValidationSession("prepared", {}, {"runtime": {"run_root": root}},
                os.path.join(root, "resident"))
            session.process = _Process()
            session.process.returncode = 0
            session._state = mock.Mock(return_value={"token": "different-owner", "status": "exited",
                                                       "group_cleanup_complete": True})
            with self.assertRaisesRegex(RuntimeError, "cleanup could not be proven"):
                session.close(timeout_seconds=1)
            self.assertFalse(session.closed)
            self.assertTrue(os.path.isdir(session.control_root))

    def test_export_diagnostics_keeps_bounded_log_tails(self):
        with tempfile.TemporaryDirectory() as root:
            control = os.path.join(root, "resident", "control")
            os.makedirs(control)
            session = ResidentValidationSession(
                "prepared", {}, {"runtime": {"run_root": root}},
                os.path.join(root, "resident"),
            )
            with open(session.stdout_path, "wb") as stream:
                stream.write(b"0123456789")
            with open(session.guardian_state_path, "w") as stream:
                stream.write('{"status":"exited"}\n')

            destination = os.path.join(root, "job", "resident_diagnostics")
            manifest = session.export_diagnostics(destination, max_bytes=4)

            with open(os.path.join(destination, "ocean.stdout"), "rb") as stream:
                self.assertEqual(stream.read(), b"6789")
            rows = dict((row["name"], row) for row in manifest["files"])
            self.assertTrue(rows["ocean.stdout"]["tail_truncated"])

    def test_start_uses_virtuoso_for_schematic_and_cdf_operations(self):
        with tempfile.TemporaryDirectory() as root:
            config = {
                "cadence": {
                    "ocean_bin": "/cadence/bin/ocean",
                    "virtuoso_bin": "/cadence/bin/virtuoso",
                },
                "runtime": {},
            }
            session = ResidentValidationSession(
                "prepared", {"setup": {}, "hierarchy": {}}, config, root,
            )
            session._alive = mock.Mock(return_value=False)
            session._state = mock.Mock(return_value={
                "token": session.token,
                "status": "running",
            })
            session.execute = mock.Mock(return_value={"returncode": 0})
            process = _Process()

            with mock.patch(
                    "crescendo_engine.resident_validation.build_workspace",
                    return_value={"root": root}), mock.patch(
                    "crescendo_engine.resident_validation.build_guardian_command",
                    return_value=["guardian"] ) as guardian, mock.patch(
                    "crescendo_engine.resident_validation.child_environment",
                    return_value={}), mock.patch(
                    "crescendo_engine.resident_validation.guardian_environment",
                    return_value={}), mock.patch.object(
                    session, "_popen", return_value=process):
                session.start(timeout_seconds=1)

            self.assertEqual(guardian.call_args[0][0], ["/cadence/bin/virtuoso", "-nograph"])
            self.assertTrue(guardian.call_args[1]["poll_expected_parent"])

    def test_execute_submits_to_existing_process_and_reads_fenced_marker(self):
        with tempfile.TemporaryDirectory() as root:
            clock = [10.0]
            session = ResidentValidationSession(
                "prepared", {}, {"runtime": {}}, root,
                clock=lambda: clock[0], sleeper=lambda _seconds: None,
            )
            session.workspace = {"root": root}
            session.process = _Process()

            def sleeper(_seconds):
                request = session.process.stdin.values[-1]
                request_path = request.split('"')[1]
                with open(request_path, "r") as stream:
                    marker_line = [line for line in stream if "outfile" in line][0]
                marker = marker_line.split('"')[1]
                with open(marker, "w") as stream:
                    stream.write("OK\n")
                clock[0] += 0.1

            session._sleep = sleeper
            payload = os.path.join(root, "payload.il")
            with open(payload, "w") as stream:
                stream.write('printf("ok\\n")\n')
            result = session.execute(payload, "range", timeout_seconds=2)
            self.assertEqual(result["returncode"], 0)
            self.assertTrue(result["resident_session"])
            self.assertIn("load", session.process.stdin.values[0])


if __name__ == "__main__":
    unittest.main()
