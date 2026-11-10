"""Diagnostic cleanup must keep producer identity bytes until unified collection."""
from __future__ import print_function

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import unittest
from unittest import mock

from crescendo_engine.environment_diagnostics import collect_run_diagnostics
from crescendo_engine.environment_test import compact_diagnostic_run
from crescendo_engine import retention


class DiagnosticIdentityRetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp(prefix="crescendo-diagnostic-retention-")
        self.run = os.path.join(self.temp, "owned_diag")
        self.candidate = os.path.join(self.run, "candidates", "g000_c0000")
        os.makedirs(self.candidate)
        self.write_json("spec.json", {"environment_test": {
            "schema_version": "crescendo.environment_test.run.v1", "test_id": "ET_fixture"}})
        self.write_json("candidates/g000_c0000/candidate.json", {
            "candidate_id": "g000_c0000", "status": "failed"})
        self.point_bytes = b'{"schema_version":"crescendo.point_identity.v1","alias_corners_skill":"((Fhigh_0 SW) (Fhigh_3 SW))"}\n'
        self.portability_bytes = b'{"schema_version":"crescendo.batch_native_scope.v1","records":[{"stage":"source_alias","source_corners_skill":"((Fhigh_0 SW) (Fhigh_3 SW))","alias_corners_skill":"((Fhigh_0 SW) (Fhigh_3 SW))"}]}\n'

    def tearDown(self):
        shutil.rmtree(self.temp, ignore_errors=True)

    def write_bytes(self, relative, data):
        path = os.path.join(self.run, *relative.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as stream:
            stream.write(data)
        return path

    def write_json(self, relative, value):
        return self.write_bytes(relative, json.dumps(value).encode("utf-8"))

    def cleanup_candidate(self):
        return retention.safe_delete_candidate_workspace(self.candidate, self.run, {"ok": True},
            required_lightweight_paths=[os.path.join(self.candidate, "candidate.json")])

    def saved_path(self, source, relative, data):
        return "diagnostics/%s/%s/%s" % (source, hashlib.sha256(data).hexdigest(), relative)

    def pending_payload(self):
        pending = os.path.join(self.run, ".cleanup_pending")
        entry = os.path.join(pending, os.listdir(pending)[0])
        with open(os.path.join(entry, "manifest.json"), "r") as stream:
            manifest = json.load(stream)
        return os.path.join(entry, "payload"), manifest

    def make_worker(self):
        epoch = os.path.join(self.run, "workers", "worker_000", "epoch_000")
        workspace = os.path.join(epoch, "workspace")
        self.write_json("workers/worker_000/epoch_000/workspace/workspace.json", {
            "root": workspace, "source_libraries": {}})
        self.write_bytes("workers/worker_000/epoch_000/workspace/portability_settings.json", self.portability_bytes)
        return epoch

    def test_candidate_worker_bytes_survive_cleanup_collector_and_archive(self):
        self.write_bytes("candidates/g000_c0000/point_identity.json", self.point_bytes)
        self.cleanup_candidate()
        self.assertFalse(os.path.exists(os.path.join(self.candidate, "point_identity.json")))
        epoch = self.make_worker()
        retention.safe_delete_worker_epoch(epoch, self.run, {"ok": True})
        self.assertFalse(os.path.exists(epoch))
        point_path = self.saved_path("candidates/g000_c0000", "point_identity.json", self.point_bytes)
        scope_path = self.saved_path("workers/worker_000/epoch_000", "workspace/portability_settings.json", self.portability_bytes)
        for relative, expected in ((point_path, self.point_bytes), (scope_path, self.portability_bytes)):
            with open(os.path.join(self.run, relative), "rb") as stream:
                self.assertEqual(stream.read(), expected)
        records = collect_run_diagnostics(self.run, "Maestro", stop_proven=True)
        artifacts = {row["payload"].get("path"): row["payload"] for row in records
                     if row["observation"] == "diagnostic.artifact"}
        self.assertIn(point_path, artifacts)
        self.assertIn(scope_path, artifacts)
        self.assertIn("Fhigh_3 SW", json.dumps(artifacts))
        self.write_json("status.json", {"status": "failed"})
        self.write_json("worker_shutdown.json", {"termination_proven": True})
        self.write_bytes("optimization.crescendo", b"test core\n")
        result = compact_diagnostic_run(self.run, "owned_diag")
        with tarfile.open(result["archive"], "r:gz") as archive:
            for relative, expected in ((point_path, self.point_bytes), (scope_path, self.portability_bytes)):
                with archive.extractfile(relative) as stream:
                    self.assertEqual(stream.read(), expected)
        self.assertEqual(set(os.listdir(self.run)), {
            "status.json", "worker_shutdown.json", "optimization.crescendo", "evidence.tar.gz"})

    def test_candidate_workspace_scope_is_preserved_too(self):
        workspace = os.path.join(self.candidate, "workspace")
        self.write_json("candidates/g000_c0000/workspace/workspace.json", {
            "root": workspace, "source_libraries": {}})
        self.write_bytes("candidates/g000_c0000/workspace/portability_settings.json", self.portability_bytes)
        self.cleanup_candidate()
        relative = self.saved_path("candidates/g000_c0000", "workspace/portability_settings.json", self.portability_bytes)
        with open(os.path.join(self.run, relative), "rb") as stream:
            self.assertEqual(stream.read(), self.portability_bytes)

    def test_ordinary_optimization_and_invalid_marker_create_no_diagnostic_copies(self):
        for spec in ({"algorithm": "nsga2"}, {"environment_test": {
                "schema_version": "crescendo.environment_test.run.v1", "test_id": "", "extra": True}}):
            self.write_json("spec.json", spec)
            self.write_bytes("candidates/g000_c0000/point_identity.json", self.point_bytes)
            self.cleanup_candidate()
            self.assertFalse(os.path.lexists(os.path.join(self.run, "diagnostics")))

    def test_oversized_metadata_retains_original_payload_and_retry_recovers(self):
        self.write_bytes("candidates/g000_c0000/point_identity.json",
                         b"x" * (retention._DIAGNOSTIC_IDENTITY_LIMIT + 1))
        with self.assertRaises(retention.ArtifactCleanupError) as caught:
            self.cleanup_candidate()
        self.assertEqual(caught.exception.detail["diagnostic_preservation"], "failed_before_artifact_deletion")
        payload, manifest = self.pending_payload()
        self.assertEqual(manifest["state"], "quarantined")
        raw = os.path.join(payload, "point_identity.json")
        self.assertEqual(os.path.getsize(raw), retention._DIAGNOSTIC_IDENTITY_LIMIT + 1)
        with open(raw, "wb") as stream:
            stream.write(self.point_bytes)
        resumed = retention.resume_pending_cleanups(self.run)
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.exists(payload))
        saved = self.saved_path("candidates/g000_c0000", "point_identity.json", self.point_bytes)
        self.assertTrue(os.path.isfile(os.path.join(self.run, saved)))

    def test_symlink_metadata_and_output_directory_are_rejected_without_following(self):
        outside = os.path.join(self.temp, "outside.json")
        with open(outside, "wb") as stream:
            stream.write(self.point_bytes)
        source = os.path.join(self.candidate, "point_identity.json")
        os.symlink(outside, source)
        with self.assertRaises(retention.ArtifactCleanupError):
            self.cleanup_candidate()
        payload, _manifest = self.pending_payload()
        self.assertTrue(os.path.islink(os.path.join(payload, "point_identity.json")))
        with open(outside, "rb") as stream:
            self.assertEqual(stream.read(), self.point_bytes)
        os.unlink(os.path.join(payload, "point_identity.json"))
        with open(os.path.join(payload, "point_identity.json"), "wb") as stream:
            stream.write(self.point_bytes)
        destination = os.path.join(self.temp, "outside_dir")
        os.mkdir(destination)
        os.symlink(destination, os.path.join(self.run, "diagnostics"))
        with self.assertRaises(retention.ArtifactCleanupError):
            retention.resume_pending_cleanups(self.run)
        self.assertTrue(os.path.isfile(os.path.join(payload, "point_identity.json")))
        self.assertEqual(os.listdir(destination), [])

    def test_copy_write_failure_keeps_payload_and_retry_deduplicates(self):
        self.write_bytes("candidates/g000_c0000/point_identity.json", self.point_bytes)
        with mock.patch("crescendo_engine.retention.os.link", side_effect=OSError("injected publish failure")):
            with self.assertRaises(retention.ArtifactCleanupError):
                self.cleanup_candidate()
        payload, manifest = self.pending_payload()
        self.assertTrue(os.path.isfile(os.path.join(payload, "point_identity.json")))
        self.assertIn("publish failure", manifest["artifact_cleanup"]["exception"])
        # Simulate a cleanup interruption after a successful durable copy.
        with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=OSError("injected deletion failure")):
            with self.assertRaises(retention.ArtifactCleanupError):
                retention.resume_pending_cleanups(self.run)
        saved = self.saved_path("candidates/g000_c0000", "point_identity.json", self.point_bytes)
        saved_absolute = os.path.join(self.run, saved)
        first_inode = os.stat(saved_absolute).st_ino
        retention.resume_pending_cleanups(self.run)
        self.assertEqual(os.stat(saved_absolute).st_ino, first_inode)
        self.assertEqual(os.stat(saved_absolute).st_nlink, 1)
        self.assertFalse(os.path.exists(payload))

    def test_error_limit_preserves_exception_prefix_and_end_with_truncation_marker(self):
        error = "CompletionEvidenceError: corner/point mismatch; " + "matrix=" * 2000 + " LAST_CONTEXT"
        compact = retention.lightweight_candidate_record({"candidate_id": "g000_c0000", "error": error})
        self.assertEqual(len(compact["error"]), 4096)
        self.assertTrue(compact["error"].startswith("CompletionEvidenceError: corner/point mismatch; "))
        self.assertTrue(compact["error"].endswith(" LAST_CONTEXT"))
        self.assertIn("middle truncated; original_chars=", compact["error"])
        self.assertEqual(retention.lightweight_candidate_record(compact), compact)
        self.assertEqual(retention._error_tail("short error"), "short error")
        for limit in (0, 1, 2, 3, 20):
            self.assertLessEqual(len(retention._error_tail(error, limit=limit)), limit)


if __name__ == "__main__":
    unittest.main()
