import json
import os
import shutil
import tempfile
import unittest

from crescendo_engine.retention import (
    UnsafeCleanupError, cleanup_abandoned_worker_epoch, safe_delete_worker_epoch,
    distribute_batch_spectre_logs, lightweight_candidate_record,
)


class RemoteBatchRetentionTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-remote-retention-")
        self.run = os.path.join(self.root, "run")
        self.epoch = os.path.join(self.run, "workers", "worker_000", "epoch_000")
        self.workspace = os.path.join(self.epoch, "workspace")
        os.makedirs(self.workspace)
        self.write(os.path.join(self.workspace, "workspace.json"), {
            "root": self.workspace, "source_libraries": {"HH": os.path.join(self.root, "source")},
        })

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    @staticmethod
    def write(path, value):
        with open(path, "w") as stream:
            json.dump(value, stream)

    def state(self, remote):
        self.write(os.path.join(self.epoch, "worker_state.json"), {"remote_batch": remote})

    def test_local_process_exit_cannot_authorize_live_remote_batch_deletion(self):
        self.state({"simulation_submitted": True, "native_completion_proven": False,
                    "native_stop_proven": False})
        for operation in (
                lambda: safe_delete_worker_epoch(self.epoch, self.run, {"ok": True}),
                lambda: cleanup_abandoned_worker_epoch(
                    self.epoch, self.run, lambda _: {"ok": True},
                    termination_proof={"ok": True, "termination_proven": True})):
            with self.assertRaisesRegex(UnsafeCleanupError, "remote Maestro batch"):
                operation()
            self.assertTrue(os.path.isfile(os.path.join(self.workspace, "workspace.json")))

    def test_invalid_submission_states_fail_closed_including_numeric_booleans(self):
        for value in (None, 0, 1, "false", [], {}):
            self.state({"simulation_submitted": value, "native_stop_proven": True})
            with self.assertRaisesRegex(UnsafeCleanupError, "submission state is unknown"):
                safe_delete_worker_epoch(self.epoch, self.run, {"ok": True})
            self.assertTrue(os.path.isdir(self.workspace))

    def test_native_completion_authorizes_owned_workspace_cleanup(self):
        self.state({"simulation_submitted": True, "native_completion_proven": True})
        self.assertTrue(safe_delete_worker_epoch(self.epoch, self.run, {"ok": True})["removed"])

    def test_native_stop_authorizes_owned_workspace_cleanup(self):
        self.state({"simulation_submitted": True, "native_stop_proven": True})
        self.assertTrue(safe_delete_worker_epoch(self.epoch, self.run, {"ok": True})["removed"])

    def test_legacy_epoch_without_remote_jobs_keeps_original_cleanup(self):
        self.state({"simulation_submitted": False})
        self.assertTrue(safe_delete_worker_epoch(self.epoch, self.run, {"ok": True})["removed"])

    def test_shared_retained_logs_are_distributed_without_cross_candidate_leaks(self):
        batch = os.path.join(self.run, "batches", "g000")
        os.makedirs(batch)
        candidates = []
        for index, alias in enumerate(("private_a", "private_b")):
            root = os.path.join(self.run, "candidates", "g000_c%04d" % index)
            os.makedirs(root)
            candidates.append({"candidate_root": root, "source_test_map": {alias: "original"}})
            with open(os.path.join(batch, "retained_spectre_%d.log" % index), "w") as stream:
                stream.write("Spectre evidence for " + alias)
            with open(os.path.join(batch, "retained_spectre_%d.meta" % index), "w") as stream:
                stream.write("schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n" %
                             os.path.join(self.workspace, "result", "1", alias, "psf", "spectre.out"))
        result = distribute_batch_spectre_logs(batch, candidates, self.workspace)
        self.assertEqual(result["copied_files"], 4)
        self.assertEqual(os.listdir(batch), [])
        for index, candidate in enumerate(candidates):
            self.assertEqual(sorted(os.listdir(candidate["candidate_root"])),
                             ["retained_spectre_%d.log" % index, "retained_spectre_%d.meta" % index])

    def test_compact_candidate_preserves_batch_log_identity_for_ui_and_resume(self):
        row = {"candidate_id": "g000_c0000", "batch_execution": {
            "batch_id": "batch_1", "source_test_map": {"private_a": "original"}},
            "completion_evidence": {"candidate_test_name_map": {"private_a": "original"},
                                    "spectre_runs": [{"test": "original", "artifact_test": "private_a",
                                                      "point_id": 1, "corner": "FF"}]}}
        compact = lightweight_candidate_record(row)
        self.assertEqual(compact["batch_execution"], row["batch_execution"])
        self.assertEqual(compact["completion"]["candidate_test_name_map"], {"private_a": "original"})
        self.assertEqual(compact["completion"]["spectre_logs"][0]["artifact_test"], "private_a")


if __name__ == "__main__":
    unittest.main()
