import builtins
import hashlib
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.cadence import CommandCancelled, command_cancellation_scope
from crescendo_engine import utils, workspace


class _CancelDuringRead(object):
    def __init__(self, stream, event, cancel_after=2):
        self.stream = stream
        self.event = event
        self.cancel_after = cancel_after
        self.read_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.stream.close()

    def read(self, size):
        self.read_count += 1
        data = self.stream.read(size)
        if self.read_count == self.cancel_after:
            self.event.set()
        return data


class SourceIdentityCancellationTest(unittest.TestCase):
    def _write_source(self, root):
        directory = os.path.join(root, "CELL", "schematic")
        os.makedirs(directory)
        path = os.path.join(directory, "data.oa")
        content = b"oa-source-block\x00" * (256 * 1024) + b"end"
        with open(path, "wb") as stream:
            stream.write(content)
        return path, content

    def test_uncancelled_file_digests_match_full_content_sha256(self):
        with tempfile.TemporaryDirectory() as root:
            path, content = self._write_source(root)
            expected = hashlib.sha256(content).hexdigest()
            for function in (utils.sha256_file, workspace._sha256_file):
                self.assertEqual(function(path), expected)
                with command_cancellation_scope(threading.Event()):
                    self.assertEqual(function(path), expected)

    def test_uncancelled_tree_keeps_prechange_golden_identity(self):
        with tempfile.TemporaryDirectory() as root:
            directory = os.path.join(root, "CELL", "schematic")
            os.makedirs(directory)
            for name, content in (("data.oa", b"oa-source-fixture\x00\x01"),
                                  ("master.tag", b"master-cell\n")):
                with open(os.path.join(directory, name), "wb") as stream:
                    stream.write(content)
            expected = {
                "schema_version": "crescendo.tree_content_fingerprint.v1",
                "paths": ["CELL"], "file_count": 2, "directory_count": 1,
                "fingerprint": "a4033b858126e69fd9e9620cb79aaf7f8a1ef8cd7b04695a77f92b528f0bf361",
            }
            # Scope diagnostics are an additive sidecar, not fingerprint input.
            def assert_identity():
                actual = workspace.tree_content_fingerprint(root, ["CELL"])
                self.assertEqual({key: actual[key] for key in expected}, expected)
                self.assertEqual(actual["scope_diagnostics"]["total_scopes"], 1)
                self.assertEqual(actual["scope_diagnostics"]["omitted_scopes"], 0)
            assert_identity()
            with command_cancellation_scope(threading.Event()):
                assert_identity()

    def test_cancelled_file_hash_stops_between_blocks_and_closes_readonly_file(self):
        with tempfile.TemporaryDirectory() as root:
            path, content = self._write_source(root)
            modes_before = os.stat(path).st_mode
            operations = (
                (utils, lambda: utils.sha256_file(path)),
                (workspace, lambda: workspace._sha256_file(path)),
                (workspace, lambda: workspace.tree_content_fingerprint(root, ["CELL"])),
                (workspace, lambda: workspace.tree_content_fingerprint(root, ["CELL/schematic/data.oa"])),
            )
            for module, operation in operations:
                with self.subTest(module=module.__name__, operation=operation):
                    event = threading.Event()
                    readers = []

                    def wrapped_open(filename, mode):
                        self.assertEqual(mode, "rb")
                        reader = _CancelDuringRead(builtins.open(filename, mode), event)
                        readers.append(reader)
                        return reader

                    with mock.patch.object(module, "open", side_effect=wrapped_open, create=True):
                        with self.assertRaises(CommandCancelled):
                            with command_cancellation_scope(event):
                                operation()
                    self.assertEqual(len(readers), 1)
                    self.assertEqual(readers[0].read_count, 2)
                    self.assertTrue(readers[0].stream.closed)
                    with open(path, "rb") as stream:
                        self.assertEqual(stream.read(), content)
                    self.assertEqual(os.stat(path).st_mode, modes_before)

    def test_cancel_after_directory_listing_stops_before_opening_source(self):
        with tempfile.TemporaryDirectory() as root:
            path, content = self._write_source(root)
            event = threading.Event()
            real_listdir = os.listdir

            def cancelled_listing(directory):
                names = real_listdir(directory)
                event.set()
                return names

            with mock.patch.object(workspace.os, "listdir", side_effect=cancelled_listing), \
                    mock.patch.object(workspace, "open", create=True) as source_open:
                with self.assertRaises(CommandCancelled):
                    with command_cancellation_scope(event):
                        workspace.tree_content_fingerprint(root, ["CELL"])
                source_open.assert_not_called()
            with open(path, "rb") as stream:
                self.assertEqual(stream.read(), content)

    def test_cancellation_before_hash_does_not_open_source(self):
        event = threading.Event()
        event.set()
        for module, operation in (
                (utils, lambda: utils.sha256_file("/not-opened")),
                (workspace, lambda: workspace._sha256_file("/not-opened")),
                (workspace, lambda: workspace.tree_content_fingerprint("/not-opened"))):
            with self.subTest(module=module.__name__):
                with mock.patch.object(module, "open", create=True) as source_open:
                    with self.assertRaises(CommandCancelled):
                        with command_cancellation_scope(event):
                            operation()
                    source_open.assert_not_called()

    def test_runtime_import_does_not_create_utils_cadence_cycle(self):
        for order in ("utils,cadence", "cadence,utils"):
            code = "from crescendo_engine import %s; import tempfile,os; f=tempfile.NamedTemporaryFile(delete=False); f.write(b'abc'); f.close(); assert utils.sha256_file(f.name)=='ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'; os.unlink(f.name)" % order
            result = subprocess.run([sys.executable, "-c", code], stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", "replace"))


if __name__ == "__main__":
    unittest.main()
