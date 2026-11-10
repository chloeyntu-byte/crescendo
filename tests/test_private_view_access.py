"""Idle access guards must never mutate source views or retained native locks."""
import json
import os
import stat
import tempfile
import unittest
from unittest import mock

from crescendo_engine.private_view_access import (
    PrivateViewBusy, check_private_view_edit_locks, protect_private_views, write_private_views)
from crescendo_engine.same_library_views import MARKER, register_view


class PrivateViewAccessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.library = os.path.join(self.temp.name, "lib")
        self.cell = os.path.join(self.library, "cell")
        self.original = os.path.join(self.cell, "maestro")
        os.makedirs(self.original)
        self.original_file = os.path.join(self.original, "maestro.sdb")
        with open(self.original_file, "w") as stream:
            stream.write("original design")
        self.workspace = {"same_library_token": "ab1234567890", "source_libraries": {"lib": self.library}}
        self.record = self.add_view("maestro", "maestro")
        self.private_file = os.path.join(self.record["path"], "data")
        with open(self.private_file, "w") as stream:
            stream.write("private data")
        self.source_modes = {path: self.mode(path) for path in (self.library, self.cell,
                             self.original, self.original_file)}
        self.addCleanup(self.make_writable)

    def add_view(self, source, role):
        row = register_view(self.workspace, ("lib", "cell", source), (source,), role=role)
        os.mkdir(row["path"])
        with open(os.path.join(row["path"], MARKER), "w") as stream:
            json.dump({"token": row["token"], "target": row["target"]}, stream)
        return row

    @staticmethod
    def mode(path):
        return stat.S_IMODE(os.lstat(path).st_mode)

    def make_writable(self):
        for current, dirs, files in os.walk(self.temp.name):
            os.chmod(current, 0o700)
            for name in files:
                path = os.path.join(current, name)
                if not os.path.islink(path):
                    os.chmod(path, 0o600)

    def assert_sources_unchanged(self):
        for path, mode in self.source_modes.items():
            self.assertEqual(self.mode(path), mode)
        with open(self.original_file) as stream:
            self.assertEqual(stream.read(), "original design")

    def test_guard_removes_all_write_bits_and_preserves_read_and_execute(self):
        os.chmod(self.private_file, 0o775)
        os.mkdir(os.path.join(self.record["path"], "nested"), 0o777)
        result = protect_private_views(self.workspace)
        self.assertFalse(result["writable"])
        self.assertEqual(self.mode(self.private_file), 0o555)
        for current, dirs, files in os.walk(self.record["path"]):
            self.assertEqual(self.mode(current) & 0o222, 0)
            for name in files:
                self.assertEqual(self.mode(os.path.join(current, name)) & 0o222, 0)
        self.assert_sources_unchanged()

    def test_writer_is_temporarily_writable_and_finally_protected(self):
        protect_private_views(self.workspace)
        with write_private_views(self.workspace) as workspace:
            self.assertIs(workspace, self.workspace)
            self.assertTrue(self.mode(self.private_file) & 0o200)
            self.assertTrue(self.mode(self.record["path"]) & 0o300)
            with open(self.private_file, "w") as stream:
                stream.write("changed")
        self.assertEqual(self.mode(self.private_file) & 0o222, 0)
        with open(self.private_file) as stream:
            self.assertEqual(stream.read(), "changed")
        self.assert_sources_unchanged()

    @unittest.skipIf(os.getuid() == 0, "root bypasses normal Unix file permissions")
    def test_idle_guard_rejects_normal_user_write_create_and_delete(self):
        protect_private_views(self.workspace)
        with self.assertRaises(PermissionError):
            with open(self.private_file, "w"):
                self.fail("Idle private file was editable")
        with self.assertRaises(PermissionError):
            with open(os.path.join(self.record["path"], "new"), "w"):
                self.fail("Idle private view accepted a new file")
        with self.assertRaises(PermissionError):
            os.unlink(self.private_file)
        with open(self.private_file) as stream:
            self.assertEqual(stream.read(), "private data")
        with write_private_views(self.workspace):
            with open(self.private_file, "w") as stream:
                stream.write("native writer")
        with self.assertRaises(PermissionError):
            with open(self.private_file, "w"):
                self.fail("Writer exit did not restore the idle guard")
        self.assert_sources_unchanged()

    def test_writer_exception_still_protects_views(self):
        with self.assertRaisesRegex(RuntimeError, "native failure"):
            with write_private_views(self.workspace):
                raise RuntimeError("native failure")
        self.assertEqual(self.mode(self.private_file) & 0o222, 0)

    def test_new_registered_views_are_protected_on_exit(self):
        with write_private_views(self.workspace):
            row = self.add_view("config", "config")
            with open(os.path.join(row["path"], "config"), "w") as stream:
                stream.write("binding")
        self.assertEqual(self.mode(row["path"]) & 0o222, 0)
        self.assertEqual(self.mode(os.path.join(row["path"], "config")) & 0o222, 0)

    def test_unproven_native_stop_does_not_change_active_writer_permissions(self):
        protect_private_views(self.workspace)
        with write_private_views(self.workspace, restore_when=lambda: False):
            pass
        self.assertTrue(self.mode(self.private_file) & 0o200)
        protect_private_views(self.workspace)
        self.assertEqual(self.mode(self.private_file) & 0o222, 0)

    def test_restore_predicate_is_evaluated_after_writer_finally(self):
        state = {"unsafe": True}
        with write_private_views(self.workspace, restore_when=lambda: not state["unsafe"]):
            try:
                pass
            finally:
                state["unsafe"] = False
        self.assertEqual(self.mode(self.private_file) & 0o222, 0)

    def test_reserved_but_absent_view_is_not_created_by_guard(self):
        row = register_view(self.workspace, ("lib", "cell", "config"), ("config",), role="config")
        protect_private_views(self.workspace)
        self.assertFalse(os.path.lexists(row["path"]))

    def test_removed_owned_views_are_not_recreated_on_exit(self):
        with write_private_views(self.workspace):
            os.unlink(self.private_file)
            os.unlink(os.path.join(self.record["path"], MARKER))
            os.rmdir(self.record["path"])
        self.assertFalse(os.path.lexists(self.record["path"]))

    def test_unknown_lock_is_retryable_and_left_intact_without_permission_changes(self):
        lock = os.path.join(self.record["path"], "maestro.sdb.cdslck")
        with open(lock, "w") as stream:
            stream.write("unknown lock contents")
        before = self.mode(self.private_file), self.mode(lock), self.mode(self.record["path"])
        for operation in (check_private_view_edit_locks, protect_private_views):
            with self.assertRaises(PrivateViewBusy) as error:
                operation(self.workspace)
            self.assertTrue(error.exception.retryable)
            self.assertEqual(error.exception.view_paths, [self.record["path"]])
            self.assertEqual(error.exception.lock_paths, [lock])
            self.assertEqual(error.exception.locks[0]["lock_type"], "unknown")
        with self.assertRaises(PrivateViewBusy):
            with write_private_views(self.workspace):
                self.fail("Locked view became writable")
        self.assertEqual(before, (self.mode(self.private_file), self.mode(lock), self.mode(self.record["path"])))
        with open(lock) as stream:
            self.assertEqual(stream.read(), "unknown lock contents")

    def test_directory_and_symlink_locks_stay_busy_without_following(self):
        for kind in ("directory", "symlink"):
            lock = os.path.join(self.record["path"], "view.cdslck." + kind)
            if kind == "directory":
                os.mkdir(lock)
            else:
                os.symlink(self.original_file, lock)
            with self.assertRaises(PrivateViewBusy):
                check_private_view_edit_locks(self.workspace)
        self.assert_sources_unchanged()

    def test_regular_symlink_is_rejected_before_any_permission_mutation(self):
        os.symlink(self.original_file, os.path.join(self.record["path"], "linked"))
        before = self.mode(self.private_file)
        with self.assertRaisesRegex(ValueError, "symbolic link"):
            protect_private_views(self.workspace)
        self.assertEqual(self.mode(self.private_file), before)
        self.assert_sources_unchanged()

    def test_hardlink_is_rejected_and_source_is_unchanged(self):
        os.link(self.original_file, os.path.join(self.record["path"], "linked"))
        with self.assertRaisesRegex(ValueError, "hard-linked"):
            protect_private_views(self.workspace)
        self.assert_sources_unchanged()

    def test_special_file_is_rejected_without_opening_or_blocking(self):
        os.mkfifo(os.path.join(self.record["path"], "fifo"))
        with self.assertRaisesRegex(ValueError, "unsupported"):
            protect_private_views(self.workspace)

    def test_marker_link_cannot_grant_ownership(self):
        marker = os.path.join(self.record["path"], MARKER)
        os.unlink(marker)
        os.symlink(self.original_file, marker)
        with self.assertRaisesRegex(ValueError, "symbolic link"):
            protect_private_views(self.workspace)
        self.assert_sources_unchanged()

    def test_forged_record_cannot_change_source_view_permissions(self):
        self.record["path"] = self.original
        with self.assertRaisesRegex(ValueError, "ownership"):
            protect_private_views(self.workspace)
        self.assert_sources_unchanged()

    def test_missing_marker_is_rejected(self):
        os.unlink(os.path.join(self.record["path"], MARKER))
        with self.assertRaises(OSError):
            protect_private_views(self.workspace)

    def test_foreign_uid_is_rejected(self):
        with mock.patch("crescendo_engine.private_view_access.os.getuid", return_value=os.getuid() + 1):
            with self.assertRaisesRegex(ValueError, "another user"):
                protect_private_views(self.workspace)

    def test_validated_file_replaced_before_chmod_is_rejected(self):
        real_open = os.open
        def changed(path, flags):
            if path == self.private_file:
                os.rename(path, path + ".old")
                with open(path, "w") as stream:
                    stream.write("replacement")
            return real_open(path, flags)
        with mock.patch("crescendo_engine.private_view_access.os.open", side_effect=changed):
            with self.assertRaisesRegex(ValueError, "changed during"):
                protect_private_views(self.workspace)
        self.assertTrue(self.mode(self.private_file) & 0o200)

    def test_failed_write_access_enable_attempt_is_guarded_again(self):
        protect_private_views(self.workspace)
        original = os.fchmod
        failed = [False]
        def fail_once(descriptor, mode):
            if mode & 0o200 and not failed[0]:
                failed[0] = True
                raise OSError("permission update failed")
            return original(descriptor, mode)
        with mock.patch("crescendo_engine.private_view_access.os.fchmod", side_effect=fail_once):
            with self.assertRaisesRegex(OSError, "permission update failed"):
                with write_private_views(self.workspace):
                    self.fail("Access failure entered writer")
        self.assertEqual(self.mode(self.private_file) & 0o222, 0)
        self.assert_sources_unchanged()


if __name__ == "__main__":
    unittest.main()
