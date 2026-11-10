"""Dead owned Cadence edit locks can be reconciled without touching foreign views."""
import copy
import json
import os
import pwd
import socket
import tempfile
import unittest

from crescendo_engine.same_library_views import MARKER, cleanup_same_library_views, register_view


class OwnedStaleLockTests(unittest.TestCase):
    def fixture(self, root):
        workspace_root = os.path.join(root, "workspace")
        library = os.path.join(root, "library")
        os.makedirs(workspace_root)
        os.makedirs(os.path.join(library, "C", "maestro"))
        workspace = {"root": workspace_root, "source_libraries": {"L": library},
                     "same_library_token": "a" * 32, "run_display_name": "repair"}
        row = register_view(workspace, ("L", "C", "maestro"), "fixture", role="maestro")
        os.mkdir(row["path"])
        with open(os.path.join(row["path"], MARKER), "w") as stream:
            json.dump({"token": row["token"], "target": row["target"]}, stream)
        target = os.path.join(row["path"], "maestro.sdb")
        with open(target, "w") as stream:
            stream.write("owned fixture")
        lock = target + ".cdslck"
        fields = {"LockStakeVersion": "1.1", "LoginName": pwd.getpwuid(os.getuid()).pw_name,
                  "HostName": socket.gethostname(), "ProcessIdentifier": "99999999",
                  "ProcessCreationTime_UTC": "1000", "FilePathUsedToEditLock": target}
        inspection = {"state": "dead", "termination_proof": {"ok": True},
                      "guardian": {"status": "exited", "group_cleanup_complete": True,
                                   "descendant_cleanup_complete": True, "pid": 99999999,
                                   "pid_start_ticks": 123, "started_epoch": 1000.2,
                                   "cwd": workspace_root, "token": "guardian-token"},
                      "manifest": {"expected_uid": os.getuid(), "workspace_root": workspace_root,
                                   "token": "guardian-token"}}
        evidence = {"process_termination_proven": True, "remote_stop_proven": True,
                    "guardian_inspections": [inspection]}
        return workspace, row, lock, fields, evidence

    def write_lock(self, path, fields):
        with open(path, "w") as stream:
            stream.write("# Cadence test edit stake\n" + "\n".join(
                key + " " + str(value) for key, value in sorted(fields.items())) + "\n")

    def test_exact_dead_owned_lock_is_recorded_then_cleaned_and_retry_is_noop(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, row, lock, fields, evidence = self.fixture(root)
            self.write_lock(lock, fields)
            result = cleanup_same_library_views(workspace, True, evidence)
            self.assertTrue(result["cleanup_proven"])
            self.assertEqual(result["removed"], [row["path"]])
            self.assertEqual(result["lock_reconciliation"][0]["lock_identity"], fields)
            self.assertIn("Cadence test edit stake", result["lock_reconciliation"][0]["lock_text"])
            self.assertTrue(os.path.isdir(os.path.join(workspace["source_libraries"]["L"], "C", "maestro")))
            self.assertEqual(cleanup_same_library_views(workspace, True, evidence)["removed"], [])

    def test_foreign_host_creation_time_target_or_current_pid_is_retained(self):
        for change in ({"HostName": socket.gethostname() + ".foreign"},
                       {"ProcessCreationTime_UTC": "1"},
                       {"ProcessIdentifier": str(os.getpid())},
                       {"FilePathUsedToEditLock": "/foreign/maestro.sdb"}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                workspace, row, lock, fields, evidence = self.fixture(root)
                fields.update(change)
                self.write_lock(lock, fields)
                result = cleanup_same_library_views(workspace, True, evidence)
                self.assertFalse(result["cleanup_proven"])
                self.assertEqual(result["retained"], [row["path"]])
                self.assertTrue(os.path.isfile(lock))
                self.assertTrue(result["errors"])

    def test_partial_guardian_or_remote_proof_cannot_release_lock(self):
        for variant in ("remote", "process", "guardian", "token", "child", "nan"):
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as root:
                workspace, row, lock, fields, evidence = self.fixture(root)
                self.write_lock(lock, fields)
                inspection = evidence["guardian_inspections"][0]
                if variant == "remote": evidence["remote_stop_proven"] = False
                if variant == "process": evidence["process_termination_proven"] = False
                if variant == "guardian": inspection["termination_proof"]["ok"] = False
                if variant == "token": inspection["guardian"]["token"] = "foreign"
                if variant == "child": inspection["guardian"]["pid"] = 99999998
                if variant == "nan": inspection["guardian"]["started_epoch"] = float("nan")
                result = cleanup_same_library_views(workspace, True, evidence)
                self.assertFalse(result["cleanup_proven"])
                self.assertTrue(os.path.isfile(lock))

    def test_plain_process_boolean_never_releases_a_native_lock(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, row, lock, fields, _evidence = self.fixture(root)
            self.write_lock(lock, fields)
            self.assertFalse(cleanup_same_library_views(workspace, True)["cleanup_proven"])
            self.assertTrue(os.path.isfile(lock))

    def test_symlink_or_hardlink_lock_is_retained_without_touching_target(self):
        for kind in ("symlink", "hardlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as root:
                workspace, row, lock, fields, evidence = self.fixture(root)
                original = os.path.join(root, "foreign-lock")
                self.write_lock(original, fields)
                (os.symlink if kind == "symlink" else os.link)(original, lock)
                self.assertFalse(cleanup_same_library_views(workspace, True, evidence)["cleanup_proven"])
                self.assertTrue(os.path.isfile(original))

    def test_foreign_registry_token_is_not_owned_even_with_matching_marker(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, row, lock, fields, evidence = self.fixture(root)
            self.write_lock(lock, fields)
            workspace["same_library_token"] = "b" * 32
            result = cleanup_same_library_views(workspace, True, evidence)
            self.assertFalse(result["cleanup_proven"])
            self.assertTrue(os.path.isfile(lock))


if __name__ == "__main__":
    unittest.main()
