from __future__ import print_function

import copy
import errno
import hashlib
import json
import os
import tempfile
import subprocess
import sys
from contextlib import contextmanager
import unittest
from unittest import mock

from crescendo_engine import run_data_store as storage
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


@contextmanager
def count_disk_reads():
    counts = {"hash_bytes": 0, "parse_bytes": 0}
    original = storage._open_read

    class CountedFile(object):
        def __init__(self, path):
            self.stream = original(path)

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def read(self, size=-1):
            raw = self.stream.read(size)
            counts["hash_bytes"] += len(raw)
            return raw

        def readline(self, size=-1):
            raw = self.stream.readline(size)
            counts["parse_bytes"] += len(raw)
            return raw

    with mock.patch.object(storage, "_open_read", side_effect=CountedFile):
        yield counts


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
            # Same-tick writes can preserve the complete filesystem stamp.
            # An explicit full audit detects them and invalidates cached proof.
            with self.assertRaises(RunDataError):
                writer.reference(force_full=True)
            with self.assertRaises(RunDataError):
                dataset_from_reference(reference)
            writer.close()

    def test_same_stamp_prefix_edits_reject_generation_commit(self):
        for location in ("first_byte", "candidate_body"):
            with self.subTest(location=location), tempfile.TemporaryDirectory() as root:
                path, writer = self.make(root)
                try:
                    writer.commit_candidates([row(status="completed")])
                    reference = writer.reference()
                    stamp = writer.expected_stamp
                    with open(path, "rb") as stream:
                        raw = stream.read()
                    offset = (0 if location == "first_byte" else
                              raw.index(b'"values":{"p":1.0}') + len(b'"values":{"p":'))
                    with open(path, "r+b") as stream:
                        stream.seek(offset)
                        stream.write(b"!" if location == "first_byte" else b"7")
                    with mock.patch.object(storage, "_stat_stamp", return_value=stamp):
                        with writer.checkpoint_evidence(), count_disk_reads() as counts:
                            for _ in range(260):
                                self.assertEqual(writer.reference(), reference)
                                dataset_from_reference(reference)
                            self.assertEqual(counts["hash_bytes"], 0)
                            self.assertEqual(counts["parse_bytes"], 0)
                        with count_disk_reads() as counts:
                            with self.assertRaisesRegex(RunDataError, "checksum") as raised:
                                writer.verify_generation(3)
                            self.assertEqual(counts["hash_bytes"], writer.offset)
                        self.assertEqual(raised.exception.detail["stage"], "generation_commit")
                        self.assertEqual(raised.exception.detail["generation"], 3)
                        self.assertTrue(writer.failed)
                        with self.assertRaises(RunDataError):
                            writer.reference()
                finally:
                    writer.close()

    def test_writer_snapshots_hash_own_bytes_and_only_parse_new_records(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                with writer.checkpoint_evidence(), count_disk_reads() as counts:
                    before = writer.reference()
                    writer.commit_candidates([row(status="completed")])
                    current = writer.reference()
                    held_offset = os.lseek(writer.fd, 0, os.SEEK_CUR)
                    with mock.patch.object(RunDataReader, "read", side_effect=AssertionError("unexpected reparse")):
                        for _ in range(260):
                            self.assertEqual(writer.reference(), current)
                            self.assertEqual(len(dataset_from_reference(before)["candidates"]), 0)
                            self.assertEqual(len(dataset_from_reference(current)["candidates"]), 1)
                    self.assertEqual(counts["hash_bytes"], 0)
                    self.assertEqual(counts["parse_bytes"], current["bytes"])
                    self.assertEqual(os.lseek(writer.fd, 0, os.SEEK_CUR), held_offset)
                with open(path, "rb") as stream:
                    self.assertEqual(writer.hasher.hexdigest(), hashlib.sha256(stream.read()).hexdigest())
                with count_disk_reads() as counts:
                    writer.verify_generation(0)
                    self.assertEqual(counts["hash_bytes"], current["bytes"])
            finally:
                writer.close()

    def test_metadata_only_changes_do_not_trigger_prefix_reads(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                before = writer.reference()
                os.utime(path, (1, 1))
                with writer.checkpoint_evidence(), count_disk_reads() as counts:
                    writer.commit_candidates([row()])
                    current = writer.reference()
                    dataset_from_reference(before)
                    self.assertEqual(counts["hash_bytes"], 0)
                    self.assertEqual(counts["parse_bytes"], current["bytes"] - before["bytes"])
                writer.verify_generation(0)
            finally:
                writer.close()

    def test_short_writes_hash_exact_successful_slices(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                original, calls = os.write, []
                def short_write(fd, raw):
                    amount = original(fd, raw[:37])
                    calls.append(amount)
                    return amount
                with mock.patch.object(storage.os, "write", side_effect=short_write):
                    writer.commit_candidates([row(status="completed")])
                self.assertGreater(len(calls), 1)
                reference = writer.reference()
                with open(path, "rb") as stream:
                    expected = hashlib.sha256(stream.read()).hexdigest()
                self.assertEqual(reference["prefix_sha256"], expected)
                writer.verify_generation(0)
            finally:
                writer.close()

    def test_wrong_written_bytes_fail_generation_audit(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                original = os.write
                def corrupt(fd, raw):
                    return original(fd, b"!" + raw[1:])
                with mock.patch.object(storage.os, "write", side_effect=corrupt):
                    writer.commit_candidates([row()])
                with self.assertRaisesRegex(RunDataError, "checksum"):
                    writer.verify_generation(0)
                self.assertTrue(writer.failed)
            finally:
                writer.close()

    def test_sync_mutations_reject_immediately_or_at_generation_commit(self):
        for change in ("rewrite", "replace", "truncate", "append", "touch"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                path, writer = self.make(root)
                try:
                    reference = writer.reference()
                    original = os.fsync
                    changed = []
                    def sync(fd):
                        original(fd)
                        if fd != writer.fd or changed:
                            return
                        changed.append(True)
                        if change == "touch":
                            os.utime(path, (1, 1))
                        elif change == "replace":
                            with open(path, "rb") as stream:
                                raw = stream.read()
                            with open(path + ".replacement", "wb") as stream:
                                stream.write(raw)
                            os.replace(path + ".replacement", path)
                        else:
                            with open(path, "r+b") as stream:
                                if change == "rewrite":
                                    stream.write(b"!")
                                elif change == "truncate":
                                    stream.truncate(reference["bytes"] - 1)
                                else:
                                    stream.seek(0, os.SEEK_END)
                                    stream.write(b"!")
                    with mock.patch.object(storage.os, "fsync", side_effect=sync):
                        if change in ("replace", "truncate", "append"):
                            with self.assertRaises(RunDataError):
                                writer.append_event("point", {"point": 1})
                        else:
                            writer.append_event("point", {"point": 1})
                    if change == "touch":
                        writer.verify_generation(0)
                    elif change == "rewrite":
                        with self.assertRaisesRegex(RunDataError, "checksum"):
                            writer.verify_generation(0)
                    else:
                        self.assertTrue(writer.failed)
                finally:
                    writer.close()

    def test_prefix_rewrite_inside_write_boundary_rejects_generation_commit(self):
        # Both syscall boundaries, short writes, and an older cached checkpoint.
        for boundary in ("before", "after"):
            for short in (False, True):
                with self.subTest(boundary=boundary, short=short), tempfile.TemporaryDirectory() as root:
                    path, writer = self.make(root)
                    try:
                        reference = writer.reference()
                        original = os.write
                        changed = []
                        def mutate():
                            if not changed:
                                changed.append(True)
                                with open(path, "r+b") as stream:
                                    stream.write(b"!")
                        def write(fd, raw):
                            if boundary == "before":
                                mutate()
                            amount = original(fd, raw[:max(1, len(raw) // 2)] if short else raw)
                            if boundary == "after":
                                mutate()
                            return amount
                        with mock.patch.object(storage.os, "write", side_effect=write):
                            writer.append_event("point", {})
                        with writer.checkpoint_evidence(), count_disk_reads() as counts:
                            dataset_from_reference(reference)
                            self.assertEqual(counts["hash_bytes"], 0)
                        with self.assertRaisesRegex(RunDataError, "checksum"):
                            writer.verify_generation(0)
                        self.assertTrue(writer.failed)
                    finally:
                        writer.close()

    def test_generation_scan_decision_is_bound_through_open(self):
        for change in ("rewrite", "replace", "truncate", "touch"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                path, writer = self.make(root)
                try:
                    reference = writer.reference()
                    original = writer._disk_prefix_sha256
                    def scan(*args, **kwargs):
                        if change == "touch":
                            os.utime(path, (1, 1))
                        elif change == "replace":
                            with open(path, "rb") as stream:
                                raw = stream.read()
                            with open(path + ".replacement", "wb") as stream:
                                stream.write(raw)
                            os.replace(path + ".replacement", path)
                        else:
                            with open(path, "r+b") as stream:
                                if change == "rewrite":
                                    stream.write(b"!")
                                else:
                                    stream.truncate(reference["bytes"] - 1)
                            os.utime(path, (1, 1))
                        return original(*args, **kwargs)
                    with mock.patch.object(writer, "_disk_prefix_sha256", side_effect=scan):
                        with self.assertRaises(RunDataError):
                            writer.verify_generation(0)
                    self.assertTrue(writer.failed)
                finally:
                    writer.close()

    def test_external_identity_and_size_changes_reject_and_scan_immediately(self):
        for change in ("append", "replace_identical", "truncate"):
            for operation in ("reference", "cached", "append"):
                with self.subTest(change=change, operation=operation), tempfile.TemporaryDirectory() as root:
                    path, writer = self.make(root)
                    try:
                        reference = writer.reference()
                        with open(path, "rb") as stream:
                            raw = stream.read()
                        if change == "append":
                            with open(path, "ab") as stream:
                                stream.write(b"extra")
                        elif change == "truncate":
                            with open(path, "r+b") as stream:
                                stream.truncate(writer.offset - 1)
                        else:
                            with open(path + ".replacement", "wb") as stream:
                                stream.write(raw)
                            os.replace(path + ".replacement", path)
                        with writer.checkpoint_evidence(), count_disk_reads() as counts:
                            with self.assertRaises(RunDataError) as raised:
                                if operation == "reference":
                                    writer.reference()
                                elif operation == "cached":
                                    dataset_from_reference(reference)
                                else:
                                    writer.commit_candidates([row()])
                            self.assertEqual(counts["hash_bytes"], os.path.getsize(path))
                        self.assertIn("observed_sha256", raised.exception.detail)
                        self.assertTrue(writer.failed)
                        # Restoring the bytes cannot resurrect a failed writer.
                        with open(path, "wb") as stream:
                            stream.write(raw)
                        with self.assertRaises(RunDataError):
                            writer.reference()
                    finally:
                        writer.close()

    def test_scan_rejects_concurrent_modification_or_path_replacement(self):
        for change in ("rewrite", "replace"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                path, writer = self.make(root)
                try:
                    writer.append_event("point", {"evidence": "x" * (2 * 1024 * 1024)})
                    original = storage._open_read
                    mutated = []
                    class MutatingFile(object):
                        def __enter__(self):
                            self.stream = original(path)
                            return self
                        def __exit__(self, *args):
                            self.stream.close()
                        def __getattr__(self, name):
                            return getattr(self.stream, name)
                        def read(self, size):
                            raw = self.stream.read(size)
                            if not mutated:
                                mutated.append(True)
                                target = path if change == "rewrite" else path + ".replacement"
                                with open(target, "r+b" if change == "rewrite" else "wb") as output:
                                    output.write(b"!")
                                if change == "replace":
                                    os.replace(target, path)
                            return raw
                    with mock.patch.object(storage, "_open_read", side_effect=lambda _: MutatingFile()):
                        with self.assertRaises(RunDataError):
                            writer.verify_generation(0)
                    self.assertTrue(writer.failed)
                finally:
                    writer.close()

    def test_first_reference_load_and_other_threads_verify_full_disk(self):
        import threading
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                writer.commit_candidates([row()])
                reference = writer.reference()
                with count_disk_reads() as counts:
                    dataset_from_reference(reference)
                    self.assertEqual(counts["parse_bytes"], reference["bytes"])
                errors = []
                def read():
                    try:
                        dataset_from_reference(reference)
                    except Exception as error:
                        errors.append(error)
                # Even while the writer transaction is active, another thread
                # must not inherit that transaction's cached validation.
                with count_disk_reads() as counts:
                    with writer.checkpoint_evidence():
                        thread = threading.Thread(target=read)
                        thread.start()
                    thread.join(5)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(errors, [])
                    self.assertEqual(counts["parse_bytes"], reference["bytes"])
                with open(path, "r+b") as stream:
                    stream.write(b"!")
                with self.assertRaises(RunDataError):
                    dataset_from_reference(reference)
            finally:
                writer.close()

    def test_new_writer_resume_and_new_process_verify_full_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            writer.commit_candidates([row()])
            reference = writer.reference()
            writer.close()
            with count_disk_reads() as counts:
                resumed = RunDataStore(path)
                try:
                    self.assertEqual(counts["parse_bytes"], reference["bytes"])
                    self.assertEqual(resumed.reference(), reference)
                    self.assertEqual(counts["hash_bytes"], 0)
                finally:
                    resumed.close()
            program = ("import json,sys; from crescendo_engine.run_data_store import dataset_from_reference; "
                       "dataset_from_reference(json.loads(sys.argv[1])); print('FULL_PREFIX_OK')")
            result = subprocess.check_output([sys.executable, "-B", "-c", program, json.dumps(reference)])
            self.assertEqual(result.strip(), b"FULL_PREFIX_OK")
            with open(path, "r+b") as stream:
                stream.write(b"!")
            failed = subprocess.run([sys.executable, "-B", "-c", program, json.dumps(reference)],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertNotEqual(failed.returncode, 0)
            with self.assertRaises(RunDataError):
                RunDataStore(path)

    def test_four_generations_read_new_bytes_plus_one_current_file_per_generation(self):
        with tempfile.TemporaryDirectory() as root:
            path, writer = self.make(root)
            try:
                writer.reference()  # Account for startup separately.
                previous = writer.offset
                with writer.checkpoint_evidence(), count_disk_reads() as counts:
                    for generation in range(4):
                        before = dict(counts)
                        for candidate in range(4):
                            for corner in range(2):
                                writer.append_event("point", {"generation": generation,
                                    "candidate": candidate, "corner": corner, "evidence": "x" * 4096})
                            reference = writer.reference()
                            for _ in range(140):
                                dataset_from_reference(reference)
                        added = writer.offset - previous
                        self.assertEqual(counts["hash_bytes"], before["hash_bytes"])
                        self.assertEqual(counts["parse_bytes"] - before["parse_bytes"], added)
                        writer.verify_generation(generation)
                        self.assertEqual(counts["hash_bytes"] - before["hash_bytes"], writer.offset)
                        self.assertEqual(sum(counts.values()) - sum(before.values()), added + writer.offset)
                        previous = writer.offset
            finally:
                writer.close()

    def test_legacy_identity_and_dialect_keep_v1_dataset_readable(self):
        program = """
import os, sys
from crescendo_engine.runtime import initialize, ApplicationIdentity, StorageLayout, ProtocolDialect
initialize(identity=ApplicationIdentity(schema_namespace='archive'),
           storage=StorageLayout(leaf_aliases={'optimization.crescendo': 'optimization.archive'}),
           dialect=ProtocolDialect(procedure_prefix='arch', symbol_prefix='ar_'))
from crescendo_engine.run_data_store import RunDataStore, dataset_from_reference, CORE_NAME
path = os.path.join(sys.argv[1], CORE_NAME)
writer = RunDataStore(path, {'spec': {}})
writer.append_event('point', {'legacy': True})
reference = writer.reference()
assert reference['schema_version'] == 'archive.optimization_dataset.v1'
assert CORE_NAME == 'optimization.archive'
writer.close()
assert dataset_from_reference(reference)['revision'] == 2
resumed = RunDataStore(path)
assert resumed.reference() == reference
resumed.close()
print('LEGACY_V1_OK')
"""
        with tempfile.TemporaryDirectory() as root:
            output = subprocess.check_output([sys.executable, "-B", "-c", program, root])
            self.assertEqual(output.strip(), b"LEGACY_V1_OK")

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
