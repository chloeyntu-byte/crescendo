import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.legacy_guardian import (
    MANIFEST_SCHEMA_VERSION, write_legacy_guardian_manifest,
)
from crescendo_engine.server import OptimizerService


class LegacyResumeIntegrationTest(unittest.TestCase):
    def _fixture(self, include_protocol=True):
        temporary = tempfile.TemporaryDirectory()
        runtime_root = temporary.name
        origin_root = os.path.join(runtime_root, "origin")
        candidate_root = os.path.join(origin_root, "candidates", "g000_c0000")
        attempt_root = os.path.join(candidate_root, "attempts", "attempt_1")
        os.makedirs(attempt_root)
        checkpoint = {
            "manifest": {
                "origin_run_root": origin_root,
                "origin_run_id": "origin",
            },
            "spec": {
                "evaluator": "real",
                "execution_backend": "legacy",
            },
        }
        if include_protocol:
            checkpoint["spec"]["legacy_guardian_protocol"] = MANIFEST_SCHEMA_VERSION
        service = OptimizerService({
            "runtime": {
                "run_root": runtime_root,
                "resume_worker_cleanup_timeout_seconds": 0.2,
            },
        })
        return temporary, service, checkpoint, candidate_root, attempt_root

    def test_guarded_prelaunch_attempt_without_manifest_is_proven_safe(self):
        temporary, service, checkpoint, _candidate_root, attempt_root = self._fixture()
        try:
            with open(os.path.join(attempt_root, "run_identity.json"), "w") as stream:
                json.dump({"history_name": "prelaunch"}, stream)

            result = service._cleanup_checkpoint_origin(checkpoint, inspect_only=True)

            self.assertEqual(result["legacy_attempts"], 1)
            self.assertEqual(result["legacy_guardians_terminated"], 0)
        finally:
            temporary.cleanup()

    def test_guarded_prelaunch_manifest_atomic_tmp_is_proven_safe(self):
        temporary, service, checkpoint, _candidate_root, attempt_root = self._fixture()
        try:
            with open(
                os.path.join(attempt_root, "legacy_guardian_launch.json.ab12_CD3.tmp"), "w",
            ) as stream:
                stream.write("partial")

            result = service._cleanup_checkpoint_origin(checkpoint, inspect_only=True)

            self.assertEqual(result["legacy_attempts"], 1)
        finally:
            temporary.cleanup()

    def test_old_attempt_without_manifest_fails_closed(self):
        temporary, service, checkpoint, _candidate_root, attempt_root = self._fixture(
            include_protocol=False,
        )
        try:
            with open(os.path.join(attempt_root, "run_identity.json"), "w") as stream:
                json.dump({"history_name": "old"}, stream)

            with self.assertRaisesRegex(RuntimeError, "no durable guardian launch proof"):
                service._cleanup_checkpoint_origin(checkpoint, inspect_only=True)
        finally:
            temporary.cleanup()

    def test_missing_manifest_with_postlaunch_artifact_fails_closed(self):
        temporary, service, checkpoint, _candidate_root, attempt_root = self._fixture()
        try:
            with open(os.path.join(attempt_root, "ocean.stderr"), "w") as stream:
                stream.write("possibly launched")

            with self.assertRaisesRegex(RuntimeError, "post-launch artifacts"):
                service._cleanup_checkpoint_origin(checkpoint, inspect_only=True)
        finally:
            temporary.cleanup()

    def test_exact_owned_guardian_is_quiesced_during_preflight(self):
        temporary, service, checkpoint, candidate_root, attempt_root = self._fixture()
        try:
            with open(os.path.join(attempt_root, "legacy_guardian_launch.json"), "w") as stream:
                stream.write("manifest placeholder")
            proven = {
                "state": "dead",
                "termination_requested": True,
                "termination_proof": {"ok": True, "kind": "guardian_cleanup_proven"},
            }
            with mock.patch(
                "crescendo_engine.server.quiesce_legacy_guardian_attempt",
                return_value=proven,
            ) as quiesce:
                result = service._cleanup_checkpoint_origin(
                    checkpoint, terminate_owned=True, inspect_only=True,
                )

            self.assertEqual(result["legacy_guardians_terminated"], 1)
            quiesce.assert_called_once()
            args, kwargs = quiesce.call_args
            self.assertEqual(args, (attempt_root,))
            self.assertEqual(
                kwargs["expected_workspace_root"],
                os.path.join(candidate_root, "workspace"),
            )
            self.assertGreater(kwargs["timeout_seconds"], 0)
            self.assertLessEqual(kwargs["timeout_seconds"], 0.2)
            self.assertTrue(kwargs["terminate_owned"])
        finally:
            temporary.cleanup()

    def test_legacy_resume_cleanup_uses_one_global_timeout_budget(self):
        temporary, service, checkpoint, _candidate_root, attempt_root = self._fixture()
        try:
            second_attempt = os.path.join(os.path.dirname(attempt_root), "attempt_2")
            os.makedirs(second_attempt)
            for root in (attempt_root, second_attempt):
                with open(os.path.join(root, "legacy_guardian_launch.json"), "w") as stream:
                    stream.write("manifest placeholder")
            clock = [50.0]
            budgets = []

            def quiesce(*args, **kwargs):
                budgets.append(kwargs["timeout_seconds"])
                clock[0] += 0.15
                return {
                    "state": "dead", "termination_requested": True,
                    "termination_proof": {"ok": True, "kind": "guardian_cleanup_proven"},
                }

            with mock.patch(
                    "crescendo_engine.server.quiesce_legacy_guardian_attempt",
                    side_effect=quiesce), mock.patch(
                    "crescendo_engine.server.time.monotonic",
                    side_effect=lambda: clock[0]):
                result = service._cleanup_checkpoint_origin(
                    checkpoint, terminate_owned=True, inspect_only=True,
                )

            self.assertEqual(result["legacy_attempts"], 2)
            self.assertAlmostEqual(budgets[0], 0.2)
            self.assertAlmostEqual(budgets[1], 0.05)
        finally:
            temporary.cleanup()

    def test_non_finite_legacy_cleanup_timeout_is_rejected(self):
        temporary, service, checkpoint, _candidate_root, _attempt_root = self._fixture()
        try:
            service.config["runtime"]["resume_worker_cleanup_timeout_seconds"] = float("inf")
            with self.assertRaisesRegex(RuntimeError, "must be finite"):
                service._cleanup_checkpoint_origin(
                    checkpoint, terminate_owned=True, inspect_only=True,
                )
        finally:
            temporary.cleanup()

    def test_ambiguous_legacy_guardian_blocks_resume_and_preserves_candidate(self):
        temporary, service, checkpoint, candidate_root, attempt_root = self._fixture()
        try:
            with open(os.path.join(attempt_root, "legacy_guardian_launch.json"), "w") as stream:
                stream.write("manifest placeholder")
            ambiguous = {
                "state": "ambiguous",
                "reason": "identity mismatch",
                "termination_proof": {"ok": False, "kind": "inspection_failed"},
            }
            with mock.patch(
                "crescendo_engine.server.quiesce_legacy_guardian_attempt",
                return_value=ambiguous,
            ):
                with self.assertRaisesRegex(RuntimeError, "cleanup is not proven"):
                    service._cleanup_checkpoint_origin(
                        checkpoint, terminate_owned=True, inspect_only=True,
                    )

            self.assertTrue(os.path.isdir(candidate_root))
        finally:
            temporary.cleanup()

    def test_legacy_guardian_failure_does_not_expose_scope_token(self):
        temporary, service, checkpoint, _candidate_root, attempt_root = self._fixture()
        identity_marker = "crescendo2-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        try:
            with open(os.path.join(attempt_root, "legacy_guardian_launch.json"), "w") as stream:
                stream.write("manifest placeholder")
            ambiguous = {
                "state": "ambiguous",
                "reason": "identity mismatch for %s" % identity_marker,
                "manifest": {"token": identity_marker},
                "guardian": {"argv": ["--token", identity_marker]},
                "processes": [{
                    "kind": "helper", "pid": 12345, "start_ticks": 67890,
                    "launch_token": identity_marker,
                }],
                "termination_proof": {
                    "ok": False, "kind": "process_identity_conflict",
                },
            }
            with mock.patch(
                "crescendo_engine.server.quiesce_legacy_guardian_attempt",
                return_value=ambiguous,
            ):
                with self.assertRaises(RuntimeError) as raised:
                    service._cleanup_checkpoint_origin(
                        checkpoint, terminate_owned=True, inspect_only=True,
                    )

            message = str(raised.exception)
            self.assertNotIn(identity_marker, message)
            self.assertIn("process_identity_conflict", message)
            self.assertIn("12345", message)
        finally:
            temporary.cleanup()

    def test_terminal_guardian_state_is_accepted_without_mocking(self):
        temporary, service, checkpoint, candidate_root, attempt_root = self._fixture()
        try:
            workspace_root = os.path.join(candidate_root, "workspace")
            os.makedirs(workspace_root)
            state_path = os.path.join(attempt_root, "guardian_state.json")
            attempt_id = "legacy-terminal-test"
            argv = ["/opt/eda/bin/ocean", "-nograph"]
            guardian_script = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "tools", "ocean_worker_guardian.py",
            )
            write_legacy_guardian_manifest(
                attempt_root, workspace_root, state_path,
                os.path.join(attempt_root, "ocean.stdout"),
                os.path.join(attempt_root, "ocean.stderr"),
                attempt_id, argv, os.getpid(), guardian_script,
            )
            with open(state_path, "w") as stream:
                json.dump({
                    "schema_version": "crescendo.ocean_worker_guardian.v1",
                    "token": attempt_id,
                    "cwd": workspace_root,
                    "argv": argv,
                    "expected_parent_pid": os.getpid(),
                    "status": "exited",
                    "guardian_pid": 990001,
                    "pid": 990002,
                    "pgid": 990002,
                    "group_cleanup_complete": True,
                    "descendant_cleanup_complete": True,
                    "descendant_supervision_complete": True,
                    "remaining_group_pids": [],
                    "remaining_adopted_pids": [],
                }, stream)

            result = service._cleanup_checkpoint_origin(checkpoint, inspect_only=True)

            self.assertEqual(result["legacy_attempts"], 1)
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
