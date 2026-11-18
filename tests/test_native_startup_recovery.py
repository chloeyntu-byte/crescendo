import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from crescendo_engine.native_baseline import (
    NativeBaselineTimeout, NativeStartupCrash, _NativeSession, reconcile_native_baseline,
)
from crescendo_engine.native_startup import STAGE, StartupObservation

SIGNATURE = b"sh: cdsCrashReport: command not found\n"
TERMINAL = {"state": "dead", "termination_proof": {"ok": True, "status": "exited"}}


class NativeStartupRecoveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = temporary.name
        config = {"runtime": {"native_startup_crash_quiet_seconds": 0.1,
                              "native_cleanup_proof_timeout_seconds": 0.3}}
        self.driver = _NativeSession(self.root, {"root": self.root}, config, "candidate")
        self.clock = 0.0
        self.sleep_hook = lambda: None
        clock = mock.patch("time.monotonic", side_effect=lambda: self.clock)
        sleep = mock.patch("time.sleep", side_effect=self.tick)
        clock.start()
        sleep.start()
        self.addCleanup(clock.stop)
        self.addCleanup(sleep.stop)

    def tick(self, seconds):
        self.clock += seconds
        self.sleep_hook()

    def launch(self, crash=True):
        self.driver.process = mock.Mock()
        self.driver.process.poll.return_value = None
        self.driver.state.update(launch_attempted=True, cleanup_pending=True)
        self.driver.persist()
        root = Path(self.driver.control)
        (root / "ocean.stdout").write_bytes(b"Ocean banner\n")
        (root / "ocean.stderr").write_bytes(SIGNATURE if crash else b"")
        if not crash:
            Path(self.driver.protocol).write_text(self.driver.nonce + "\tREADY\t\n")

    def start(self):
        return self.driver.start(lambda: ("startup.il", "run.il", "finish.il"), 10, None)

    def test_crash_without_stage_retries_only_after_late_guardian_proof(self):
        events = []
        inspections = 0

        def inspect(*args, **kwargs):
            nonlocal inspections
            inspections += 1
            if inspections == 1:
                return {"state": "active"}
            if inspections < 4:
                return {"state": "ambiguous",
                        "reason": "live PID identity does not match persisted ownership"}
            events.append("proven")
            return TERMINAL

        def launch(_script):
            attempt = self.driver.state["startup_attempt"]
            events.append("launch %d" % attempt)
            self.launch(crash=attempt == 1)

        with mock.patch.object(self.driver, "launch", side_effect=launch), \
                mock.patch("crescendo_engine.legacy_guardian.inspect_legacy_guardian_attempt", side_effect=inspect):
            self.assertEqual(self.start()[0], "startup.il")

        self.assertEqual(events, ["launch 1", "proven", "launch 2"])
        self.assertEqual(inspections, 4)
        self.assertGreaterEqual(self.clock, 0.25)
        self.assertLess(self.clock, 1)
        self.assertEqual(len(self.driver.state["previous_startup_crashes"]), 1)
        self.assertEqual(self.driver.state["startup_attempt"], 2)
        self.assertTrue(os.path.isfile(os.path.join(self.root, "native_baseline", "ocean.stderr")))

    def test_two_proven_crashes_keep_first_cause_and_terminal_state(self):
        with mock.patch.object(self.driver, "launch", side_effect=lambda _: self.launch()) as launch, \
                mock.patch("crescendo_engine.legacy_guardian.inspect_legacy_guardian_attempt",
                           return_value=TERMINAL):
            with self.assertRaisesRegex(NativeStartupCrash, "cdsCrashReport: command not found"):
                self.start()
            self.assertEqual(launch.call_count, 2)
            proof = reconcile_native_baseline(self.root)
        self.assertTrue(proof["cleanup_proven"])
        self.assertTrue(proof["native_not_submitted"])
        self.assertEqual(proof["previous_startup_crashes"][0]["attempt"], 1)
        self.assertEqual(proof["startup_crash"]["attempt"], 2)

    def test_retry_exit_without_signature_preserves_original_crash(self):
        def launch(_script):
            self.launch()
            if self.driver.state["startup_attempt"] == 2:
                Path(self.driver.control, "ocean.stderr").write_bytes(b"")
                self.driver.process.poll.return_value = 1

        with mock.patch.object(self.driver, "launch", side_effect=launch), \
                mock.patch("crescendo_engine.legacy_guardian.inspect_legacy_guardian_attempt",
                           return_value=TERMINAL):
            with self.assertRaisesRegex(NativeStartupCrash, "cdsCrashReport: command not found") as failure:
                self.start()
            self.assertTrue(self.driver.cleanup()["native_not_submitted"])
        self.assertIn("Owned Ocean exited", failure.exception.detail["startup_retry_error"])

    def test_guardian_cleanup_deadline_prevents_replay(self):
        with mock.patch.object(self.driver, "launch", side_effect=lambda _: self.launch()) as launch, \
                mock.patch("crescendo_engine.legacy_guardian.inspect_legacy_guardian_attempt",
                           return_value={"state": "active"}) as inspect:
            with self.assertRaises(NativeStartupCrash):
                self.start()
            observed = inspect.call_count
            proof = self.driver.cleanup()
            self.assertEqual(observed, inspect.call_count)
        self.assertEqual(launch.call_count, 1)
        self.assertTrue(proof["cleanup_pending"])
        self.assertFalse(proof["cleanup_proven"])
        self.assertIn("before timeout", proof["local_cleanup"]["reason"])

    def test_slow_stdout_with_signature_reaches_ready(self):
        self.launch()
        output = Path(self.driver.control) / "ocean.stdout"

        def progress():
            with output.open("ab") as stream:
                stream.write(b"more native startup output\n")
            if self.clock >= 0.9:
                Path(self.driver.protocol).write_text(self.driver.nonce + "\tREADY\t\n")

        self.sleep_hook = progress
        self.assertEqual(self.driver.wait(("READY",), 1)["state"], "READY")

    def test_stage_marker_disables_crash_detection_without_ready(self):
        self.launch()
        output = Path(self.driver.control) / "ocean.stdout"
        with output.open("ab") as stream:
            stream.write(STAGE + b"\n")
        with self.assertRaises(NativeBaselineTimeout):
            self.driver.wait(("READY",), 0.5)

    def test_no_signature_uses_original_deadline(self):
        self.launch(crash=False)
        Path(self.driver.protocol).unlink()
        with self.assertRaisesRegex(NativeBaselineTimeout, "Ocean startup timed out after 0.5 seconds") as failure:
            self.driver.wait(("READY",), 0.5)
        self.assertEqual(failure.exception.detail, {"timeout_seconds": 0.5, "expected_states": ["READY"],
                                                    "last_protocol_state": None})
        self.assertGreaterEqual(self.clock, 0.5)

    def test_execution_timeout_preserves_last_observed_state(self):
        self.launch(crash=False)
        Path(self.driver.protocol).write_text(self.driver.nonce + "\tRUN_ID\t0\n")
        with self.assertRaisesRegex(NativeBaselineTimeout, "native Run execution timed out") as failure:
            self.driver.wait(("SIM_DONE",), 0.3)
        self.assertEqual(failure.exception.detail["last_protocol_state"], "RUN_ID")

    def test_fallback_to_local_death_cannot_prove_post_submission_stop(self):
        self.launch()
        self.driver.state["submission_intent"] = True
        with mock.patch("crescendo_engine.legacy_guardian.inspect_legacy_guardian_attempt",
                        return_value=TERMINAL):
            proof = self.driver.cleanup(timeout=0.2)
        self.assertFalse(proof["native_not_submitted"])
        self.assertTrue(proof["cleanup_pending"])

    def test_near_match_does_not_authorize_a_retry(self):
        self.launch()
        Path(self.driver.control, "ocean.stderr").write_bytes(b"note: " + SIGNATURE)
        watcher = StartupObservation(self.driver.control, 0.1)
        self.assertIsNone(watcher.observe([]))
        self.tick(0.2)
        self.assertIsNone(watcher.observe([]))
