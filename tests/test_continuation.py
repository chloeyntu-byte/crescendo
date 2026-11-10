import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine.continuation import (
    ContinuationLease, ContinuationLeaseBusy, RuntimeOptimizationLease,
    build_continuation_pending, checkpoint_continuation_id,
    clear_continuation_pending, continuation_checkpoint_path,
    continuation_pending_path, continuation_schema_sha256,
    persist_continuation_pending,
)


class ContinuationLeaseTest(unittest.TestCase):
    def test_second_process_is_blocked_and_crashed_owner_is_recovered(self):
        with tempfile.TemporaryDirectory() as root:
            code = (
                "import sys\n"
                "from crescendo_engine.continuation import ContinuationLease\n"
                "lease=ContinuationLease(sys.argv[1], sys.argv[2], run_root='child').acquire()\n"
                "sys.stdout.write('ready\\n'); sys.stdout.flush()\n"
                "sys.stdin.read()\n"
            )
            process = subprocess.Popen(
                [sys.executable, "-c", code, root, "continuation-test"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                universal_newlines=True,
            )
            try:
                self.assertEqual(process.stdout.readline().strip(), "ready")
                with self.assertRaisesRegex(ContinuationLeaseBusy, "already running"):
                    ContinuationLease(root, "continuation-test", run_root="contender").acquire()
                process.kill()
                process.wait(timeout=10)
                recovered = ContinuationLease(root, "continuation-test", run_root="resumed").acquire()
                try:
                    self.assertIsNotNone(recovered.recovered_stale_owner)
                    self.assertEqual(recovered.recovered_stale_owner["state"], "held")
                finally:
                    recovered.release()
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                for stream in (process.stdin, process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()

    def test_clean_release_is_reusable_without_stale_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            first = ContinuationLease(root, "continuation-clean").acquire()
            first.release()
            second = ContinuationLease(root, "continuation-clean").acquire()
            try:
                self.assertIsNone(second.recovered_stale_owner)
                with open(second.path, "r") as stream:
                    metadata = json.load(stream)
                self.assertEqual(metadata["state"], "held")
                self.assertEqual(metadata["continuation_id"], "continuation-clean")
            finally:
                second.release()

    def test_runtime_real_eda_lease_has_a_distinct_clear_conflict(self):
        with tempfile.TemporaryDirectory() as root:
            first = RuntimeOptimizationLease(root, run_root=os.path.join(root, "first")).acquire()
            try:
                with self.assertRaisesRegex(
                    ContinuationLeaseBusy, "another real EDA optimization.*runtime_root",
                ):
                    RuntimeOptimizationLease(root, run_root=os.path.join(root, "second")).acquire()
            finally:
                first.release()

    def test_legacy_checkpoint_continuation_id_ignores_exported_candidate_payload(self):
        checkpoint = {
            "schema_version": "crescendo.checkpoint.v1",
            "manifest": {"origin_run_id": "origin", "algorithm": "random"},
            "parameter_ids": ["x"],
            "spec": {"variables": [{"parameter_id": "x"}]},
            "candidates": [],
        }
        exported = dict(checkpoint)
        exported["created_at"] = "later"
        exported["candidates"] = [["g000_c0000"]]
        self.assertEqual(
            checkpoint_continuation_id(checkpoint), checkpoint_continuation_id(exported),
        )

    def test_continuation_checkpoint_path_is_deterministic_and_rejects_symlink_root(self):
        with tempfile.TemporaryDirectory() as root:
            first = continuation_checkpoint_path(root, "stable-id", create=True)
            second = continuation_checkpoint_path(root, "stable-id", create=False)
            self.assertEqual(first, second)
            self.assertTrue(first.startswith(os.path.join(root, ".continuation_checkpoints")))

        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "target")
            os.makedirs(target)
            os.symlink(target, os.path.join(root, ".continuation_checkpoints"))
            with self.assertRaisesRegex(RuntimeError, "real directory"):
                continuation_checkpoint_path(root, "unsafe", create=True)

    def test_continuation_checkpoint_directories_are_durably_linked(self):
        with tempfile.TemporaryDirectory() as root, mock.patch(
            "crescendo_engine.continuation._fsync_directory",
        ) as fsync_directory:
            path = continuation_checkpoint_path(root, "durable", create=True)

            anchor_root = os.path.join(root, ".continuation_checkpoints")
            self.assertTrue(os.path.isdir(os.path.dirname(path)))
            self.assertEqual(
                fsync_directory.call_args_list,
                [mock.call(root), mock.call(anchor_root)],
            )

    def test_pending_journal_is_verified_and_unlink_is_directory_fsynced(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = {"payload_sha256": "a" * 64}
            document = build_continuation_pending(
                checkpoint, "journal-id", 2, None,
                continuation_schema_sha256({"schema": "fixed"}),
            )
            path = persist_continuation_pending(root, "journal-id", document)
            self.assertEqual(path, continuation_pending_path(root, "journal-id"))
            with self.assertRaisesRegex(RuntimeError, "unresolved"):
                persist_continuation_pending(root, "journal-id", document)
            with mock.patch(
                "crescendo_engine.continuation._fsync_directory",
            ) as fsync_directory:
                self.assertTrue(clear_continuation_pending(root, "journal-id"))
                fsync_directory.assert_called_once_with(os.path.dirname(path))
            self.assertFalse(os.path.lexists(path))


if __name__ == "__main__":
    unittest.main()
