from __future__ import print_function

import errno
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import gui, utils
from crescendo_engine.storage_errors import (
    annotate_storage_error, format_task_error, is_storage_full_error,
)


EDQUOT = getattr(errno, "EDQUOT", 122)


class StorageErrorTests(unittest.TestCase):
    def test_quota_error_reports_exact_filename_and_phase(self):
        error = OSError(EDQUOT, "Disk quota exceeded", "/work/fixture/probes/design/probe.il")
        message = format_task_error(error, "Load ADE Setup")
        self.assertIn("Load ADE Setup: OSError:", message)
        self.assertIn("user/group disk quota", message)
        self.assertIn("Write path: /work/fixture/probes/design/probe.il", message)
        self.assertIn("free filesystem space does not rule out", message)
        self.assertNotIn("has no available space or inodes", message)

    def test_filesystem_full_is_distinct_from_quota(self):
        error = OSError(errno.ENOSPC, "No space left on device", "/runs/new/status.json")
        message = format_task_error(error, "Apply Selected Cells")
        self.assertIn("Apply Selected Cells", message)
        self.assertIn("filesystem has no available space or inodes", message)
        self.assertNotIn("user/group disk quota", message)

    def test_missing_filename_shows_configured_roots_without_environment(self):
        config = {"runtime": {"run_root": "/work/fixture/runs"},
                  "cadence": {"environment": {"SECRET_TOKEN": "never-print-this"}}}
        message = format_task_error(OSError(EDQUOT, "Disk quota exceeded"), "Load ADE Setup", config)
        self.assertIn("Configured run root: /work/fixture/runs", message)
        self.assertIn("Configured hierarchy probe root: /work/fixture/runs/probes", message)
        self.assertNotIn("Write path:", message)
        self.assertNotIn("never-print-this", message)

    def test_explicit_probe_root_is_respected(self):
        config = {"runtime": {"run_root": "/runs", "hierarchy_probe_root": "/persistent/probes"}}
        message = format_task_error(OSError(EDQUOT, "Disk quota exceeded"), "Load", config)
        self.assertIn("Configured hierarchy probe root: /persistent/probes", message)
        self.assertNotIn("Configured hierarchy probe root: /runs/probes", message)

    def test_ordinary_errors_keep_existing_message(self):
        for error in (ValueError("unknown selection"), OSError(errno.EACCES, "Permission denied", "/source/file")):
            self.assertEqual(format_task_error(error, "Load"), "Load: {}: {}".format(error.__class__.__name__, error))
            self.assertFalse(is_storage_full_error(error))
            self.assertIs(annotate_storage_error(error, "/runs/file", "write"), error)
            self.assertFalse(hasattr(error, "cr_storage_path"))

    def test_annotated_error_preserves_errno_type_and_missing_filename(self):
        error = OSError(EDQUOT, "Disk quota exceeded")
        result = annotate_storage_error(error, "/runs/probes/probe.il", "sync file")
        self.assertIs(result, error)
        self.assertEqual(error.errno, EDQUOT)
        self.assertIsNone(error.filename)
        message = format_task_error(error, "Load")
        self.assertIn("Write path: /runs/probes/probe.il", message)
        self.assertIn("Write operation: sync file", message)


class AtomicStorageErrorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-quota-error-")
        self.addCleanup(self.directory.cleanup)
        self.path = os.path.join(self.directory.name, "evidence.json")
        with open(self.path, "w") as stream:
            stream.write("previous evidence")

    def _assert_old_content_and_no_temp(self):
        with open(self.path) as stream:
            self.assertEqual(stream.read(), "previous evidence")
        self.assertEqual(os.listdir(self.directory.name), ["evidence.json"])

    def test_fsync_quota_retains_original_error_and_target(self):
        error = OSError(EDQUOT, "Disk quota exceeded")
        with mock.patch.object(utils.os, "fsync", side_effect=error):
            with self.assertRaises(OSError) as raised:
                utils.atomic_write_text(self.path, "new evidence")
        self.assertIs(raised.exception, error)
        self.assertEqual(error.cr_storage_path, self.path)
        self.assertEqual(error.cr_storage_operation, "sync file")
        self._assert_old_content_and_no_temp()

    def test_flush_quota_retains_original_error_and_atomic_content(self):
        error = OSError(EDQUOT, "Disk quota exceeded")

        class FailingStream(object):
            def __init__(self, descriptor, *_args, **_kwargs):
                self.descriptor = descriptor

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                os.close(self.descriptor)

            def write(self, _text):
                return None

            def flush(self):
                raise error

        with mock.patch.object(utils.os, "fdopen", side_effect=FailingStream):
            with self.assertRaises(OSError) as raised:
                utils.atomic_write_text(self.path, "new evidence")
        self.assertIs(raised.exception, error)
        self.assertIsNone(error.filename)
        self.assertEqual(error.cr_storage_path, self.path)
        self.assertEqual(error.cr_storage_operation, "write and flush file")
        self._assert_old_content_and_no_temp()

    def test_temporary_file_creation_enospc_reports_destination(self):
        error = OSError(errno.ENOSPC, "No space left on device")
        with mock.patch.object(utils.tempfile, "mkstemp", side_effect=error):
            with self.assertRaises(OSError) as raised:
                utils.atomic_write_json(self.path, {"new": "evidence"})
        self.assertIs(raised.exception, error)
        self.assertEqual(error.cr_storage_path, self.path)
        self.assertEqual(error.cr_storage_operation, "create temporary file")
        self._assert_old_content_and_no_temp()

    def test_directory_creation_quota_retains_actual_directory(self):
        directory = os.path.join(self.directory.name, "new", "probes")
        error = OSError(EDQUOT, "Disk quota exceeded")
        with mock.patch.object(utils.os, "makedirs", side_effect=error):
            with self.assertRaises(OSError) as raised:
                utils.atomic_write_json(os.path.join(directory, "hierarchy.json"), {})
        self.assertIs(raised.exception, error)
        self.assertEqual(error.cr_storage_path, directory)
        self.assertEqual(error.cr_storage_operation, "create directory")

    def test_ordinary_fsync_error_still_propagates_unmodified(self):
        error = OSError(errno.EIO, "Input/output error")
        with mock.patch.object(utils.os, "fsync", side_effect=error):
            with self.assertRaises(OSError) as raised:
                utils.atomic_write_text(self.path, "new evidence")
        self.assertIs(raised.exception, error)
        self.assertFalse(hasattr(error, "cr_storage_path"))
        self._assert_old_content_and_no_temp()


class GuiStorageErrorTests(unittest.TestCase):
    def setUp(self):
        self.app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        self.app.root = object()
        self.app.service = mock.Mock(config={"runtime": {"run_root": "/persistent/runs"}})
        self.app._set_error = mock.Mock()
        self.app._active_context = lambda: True
        self.app.launch_context = {"state_path": "/work/fixture/launch-state.json", "source": {}}
        self.app.launch_state = {"state": "bound"}
        self.app.active_setup_path = "/source/maestro.sdb"

    def test_task_error_uses_phase_and_annotated_write_path(self):
        error = annotate_storage_error(OSError(EDQUOT, "Disk quota exceeded"), "/persistent/probes/probe.il", "sync file")
        with mock.patch.object(gui.messagebox, "showerror") as showerror:
            self.app._task_error("Apply Selected Cells")(error)
        message = self.app._set_error.call_args[0][0]
        self.assertIn("Apply Selected Cells: OSError:", message)
        self.assertIn("Write path: /persistent/probes/probe.il", message)
        title, summary = showerror.call_args[0]
        self.assertEqual(title, "Apply Selected Cells")
        self.assertIn("Disk quota exceeded", summary)
        self.assertIn("Write path: /persistent/probes/probe.il", summary)
        self.assertIn("See Details for the complete report.", summary)
        self.assertLessEqual(len(summary), 450)
        self.assertIn("\n", message)
        self.assertEqual(showerror.call_args[1]["parent"], self.app.root)

    def test_launch_status_quota_does_not_mask_original_load_error(self):
        status_error = annotate_storage_error(OSError(EDQUOT, "Disk quota exceeded"), "/work/fixture/launch-state.json", "sync file")
        original_error = annotate_storage_error(OSError(EDQUOT, "Disk quota exceeded"), "/persistent/probes/probe.il", "sync file")
        with mock.patch.object(gui, "atomic_write_json", side_effect=status_error), \
                mock.patch.object(gui.messagebox, "showerror") as showerror:
            self.assertFalse(self.app._write_launch_state("prepare_failed", error=str(original_error)))
            self.app._task_error("Load ADE Setup")(original_error)
        self.assertEqual(self.app.launch_state["state"], "prepare_failed")
        self.assertIn("launch-state.json", self.app.launch_state_storage_error)
        self.assertIn("Write path: /persistent/probes/probe.il", showerror.call_args[0][1])
        self.assertNotIn("Write path: /work/fixture/launch-state.json", showerror.call_args[0][1])
        with mock.patch.object(gui, "atomic_write_json"):
            self.assertTrue(self.app._write_launch_state("cells_ready"))
        self.assertIsNone(self.app.launch_state_storage_error)

    def test_launch_status_permission_error_remains_explicit(self):
        error = OSError(errno.EACCES, "Permission denied", "/work/fixture/launch-state.json")
        with mock.patch.object(gui, "atomic_write_json", side_effect=error):
            with self.assertRaises(OSError) as raised:
                self.app._write_launch_state("prepare_failed")
        self.assertIs(raised.exception, error)
        self.assertEqual(self.app.launch_state["state"], "bound")
        self.app._set_error.assert_not_called()


if __name__ == "__main__":
    unittest.main()
