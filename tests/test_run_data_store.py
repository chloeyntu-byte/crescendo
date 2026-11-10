from __future__ import print_function

import copy
import errno
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.run_data_store import (
    CORE_NAME, RunDataError, RunDataReader, RunDataStore, dataset_from_reference,
    export_dataset, load_dataset,
)


def row(index=0, status="planned", value=1.0):
    result = {"candidate_id": "g000_c%04d" % index, "generation": 0, "index": index,
              "values": {"p": value}, "status": status}
    if status == "completed":
        result.update(metrics={"objectives": [{"value": value, "score": value}],
                               "constraints": [], "feasible": True, "total_violation": 0},
                      parameter_application={"ok": True, "parameters": [{"parameter_id": "p",
                          "persisted_schematic_value": value, "persisted_cdf_value": value}]})
    return result


class RunDataStoreTest(unittest.TestCase):
    def make(self, root):
        path = os.path.join(root, CORE_NAME)
        return path, RunDataStore(path, {"run_id": "test", "spec": {"variables": [{"parameter_id": "p"}]}})

    def test_live_plans_deltas_actual_values_and_pareto_roundtrip(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            reader = RunDataReader(path)
            writer.commit_candidates([row(0), row(1)])
            initial = reader.read()
            self.assertEqual(len(initial["candidates"]), 2)
            writer.commit_candidates([row(1, "completed", 1.0)])
            delta = reader.read(since_revision=initial["revision"], include_candidates=False,
                                include_metadata=False)
            self.assertNotIn("candidates", delta)
            self.assertNotIn("metadata", delta)
            self.assertEqual([entry["candidate_id"] for entry in delta["changed_candidates"]], ["g000_c0001"])
            writer.commit_candidates([row(0, "failed", 1.0)])
            writer.commit_result({"state": "completed", "candidates": [],
                                  "pareto_front": [row(1, "completed")], "representative": row(1, "completed")})
            exported = export_dataset(path, os.path.join(root, "portable.crescendo"))
            loaded = load_dataset(exported)
            self.assertEqual(loaded["result"]["representative"]["candidate_id"], "g000_c0001")
            self.assertEqual(loaded["candidates"][1]["parameter_application"]["parameters"][0]["persisted_cdf_value"], 1.0)
            writer.close()

    def test_reject_concurrent_writer_symlinks_hardlinks_and_nan(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            with self.assertRaises((RunDataError, OSError)):
                RunDataStore(path)
            with self.assertRaises(ValueError):
                writer.commit_candidates([row(value=float("nan"))])
            alias = os.path.join(root, "alias")
            os.symlink(path, alias)
            with self.assertRaises((RunDataError, OSError)):
                load_dataset(alias)
            os.unlink(alias)
            os.link(path, alias)
            with self.assertRaises(RunDataError):
                load_dataset(path)
            writer.close()

    def test_torn_append_runtime_prefix_export_and_strict_import(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            writer.commit_candidates([row()])
            reference = writer.reference()
            writer.close()
            with open(path, "ab") as stream:
                stream.write(b'{"incomplete":')
            runtime = RunDataReader(path).read()
            self.assertTrue(runtime["tail_incomplete"])
            self.assertEqual(runtime["revision"], reference["revision"])
            with self.assertRaises(RunDataError):
                load_dataset(path)
            with self.assertRaises(RunDataError):
                RunDataStore(path)
            exported = export_dataset(path, os.path.join(root, "clean.crescendo"))
            self.assertEqual(load_dataset(exported)["revision"], reference["revision"])
            self.assertEqual(len(dataset_from_reference(reference)["candidates"]), 1)

    def test_fsync_quota_failure_never_acknowledges_or_allows_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            writer.commit_candidates([row()])
            before = writer.reference()
            with mock.patch("crescendo_engine.run_data_store.os.fsync", side_effect=OSError(errno.EDQUOT, "quota")):
                with self.assertRaises(OSError):
                    writer.commit_candidates([row(status="completed")])
            self.assertEqual(writer.revision, before["revision"])
            with self.assertRaises(RunDataError):
                writer.reference()
            with self.assertRaises(RunDataError):
                writer.commit_candidates([row(status="failed")])
            writer.close()
            self.assertEqual(dataset_from_reference(before)["candidates"][0]["status"], "planned")

    def test_fixed_prefix_reference_rejects_content_and_embedded_tampering(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            writer.commit_candidates([row(status="completed")])
            reference = writer.reference()
            with open(path, "rb") as stream:
                embedded = stream.read().decode("utf-8")
            writer.commit_candidates([row(1)])
            self.assertEqual(len(dataset_from_reference(reference, embedded)["candidates"]), 1)
            with self.assertRaises(RunDataError):
                dataset_from_reference(reference, embedded.replace('"completed"', '"failed"'))
            with open(path, "r+b") as stream:
                stream.seek(0)
                stream.write(b"!")
            with self.assertRaises(RunDataError):
                dataset_from_reference(reference)
            with self.assertRaises(RunDataError):
                writer.reference()
            writer.close()

    def test_cached_prefix_and_writer_reference_detect_same_stamp_body_tampering(self):
        for location in ("first_byte", "candidate_body"):
            with self.subTest(location=location), tempfile.TemporaryDirectory() as root:
                path, writer = self.make(root)
                try:
                    writer.commit_candidates([row(status="completed")])
                    reference = writer.reference()
                    writer.commit_candidates([row(1)])
                    stamp = writer.expected_stamp
                    with open(path, "rb") as stream:
                        raw = stream.read()
                    offset = 0 if location == "first_byte" else raw.index(b'"values":{"p":1.0}') + len(b'"values":{"p":')
                    with open(path, "r+b") as stream:
                        stream.seek(offset)
                        stream.write(b"!" if location == "first_byte" else b"7")
                    # XFS/CentOS can return an unchanged mtime/ctime within a
                    # tick. Freeze the old metadata to exercise checksum proof,
                    # independent of filesystem clock resolution and sleeps.
                    with mock.patch.object(writer, "_file_stamp", return_value=stamp):
                        with self.assertRaisesRegex(RunDataError, "checksum"):
                            dataset_from_reference(reference)
                        with self.assertRaisesRegex(RunDataError, "checksum"):
                            writer.reference()
                finally:
                    writer.close()

    def test_valid_cached_prefix_after_append_hashes_disk_without_reparsing_json(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                writer.commit_candidates([row(status="completed")])
                reference = writer.reference()
                writer.commit_candidates([row(1)])
                current = writer.reference()
                held_offset = os.lseek(writer.fd, 0, os.SEEK_CUR)
                with mock.patch.object(RunDataReader, "read", side_effect=AssertionError("unexpected JSON reparse")):
                    self.assertEqual(len(dataset_from_reference(reference)["candidates"]), 1)
                    self.assertEqual(writer.reference(), current)
                self.assertEqual(os.lseek(writer.fd, 0, os.SEEK_CUR), held_offset)
            finally:
                writer.close()

    def test_point_records_do_not_complete_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            writer.commit_candidates([row()])
            writer.append_event("point", {"candidate_id": "g000_c0000", "point_id": 1,
                                          "candidate_completed": False, "outputs": [{"value": 2.0}]})
            loaded = load_dataset(path)
            self.assertEqual(loaded["candidates"][0]["status"], "planned")
            self.assertFalse(loaded["result"])
            writer.close()


if __name__ == "__main__":
    unittest.main()
