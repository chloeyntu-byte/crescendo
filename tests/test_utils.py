import json
import errno
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.utils import atomic_write_json, atomic_write_text


class AtomicWriteTest(unittest.TestCase):
    def test_atomic_write_text_and_json_replace_existing_content(self):
        with tempfile.TemporaryDirectory() as root:
            text_path = os.path.join(root, "value.txt")
            with open(text_path, "w") as stream:
                stream.write("old")

            atomic_write_text(text_path, "new\n")
            with open(text_path, "r") as stream:
                self.assertEqual(stream.read(), "new\n")

            json_path = os.path.join(root, "value.json")
            atomic_write_json(json_path, {"value": 3})
            with open(json_path, "r") as stream:
                self.assertEqual(json.load(stream), {"value": 3})

    def test_replace_failure_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "checkpoint.json")
            with mock.patch("crescendo_engine.utils.os.replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    atomic_write_text(path, "payload")

            self.assertFalse(os.path.exists(path))
            self.assertEqual(os.listdir(root), [])

    def test_enospc_before_replace_preserves_previous_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "checkpoint.json")
            with open(path, "w") as stream:
                json.dump({"revision": 7}, stream)

            failure = OSError(errno.ENOSPC, "no space left on device")
            with mock.patch("crescendo_engine.utils._replace_file", side_effect=failure):
                with self.assertRaises(OSError) as raised:
                    atomic_write_json(path, {"revision": 8})

            self.assertEqual(raised.exception.errno, errno.ENOSPC)
            with open(path) as stream:
                self.assertEqual(json.load(stream), {"revision": 7})
            self.assertEqual(os.listdir(root), ["checkpoint.json"])

    def test_parent_directory_is_opened_and_fsynced(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "checkpoint.json")
            original_open = os.open
            original_fsync = os.fsync
            with mock.patch("crescendo_engine.utils.os.open", wraps=original_open) as open_mock, \
                    mock.patch("crescendo_engine.utils.os.fsync", wraps=original_fsync) as fsync_mock:
                atomic_write_text(path, "payload")

            directory_calls = [call for call in open_mock.call_args_list if call[0][0] == root]
            self.assertEqual(len(directory_calls), 1)
            self.assertGreaterEqual(fsync_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
