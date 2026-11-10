from __future__ import print_function

import fcntl
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import crescendo_engine.retention as retention_module
from crescendo_engine.legacy_guardian import (
    MANIFEST_SCHEMA_VERSION as GUARDIAN_LAUNCH_PROTOCOL,
    acquire_legacy_guardian_launch_lock,
    close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity,
    write_legacy_guardian_manifest,
)
from crescendo_engine.persistent_maestro import persistent_worker_control_root
from crescendo_engine.resources import guardian_script_path
from crescendo_engine.retention import (
    UnsafeCleanupError, clear_retained_spectre_logs,
    cleanup_abandoned_controller_epoch,
    cleanup_abandoned_nonterminal_candidate,
    cleanup_abandoned_worker_epoch,
    cleanup_terminal_candidate_artifacts,
    lightweight_candidate_record,
    retained_spectre_paths,
    resume_pending_cleanups,
    safe_delete_candidate_workspace,
    safe_delete_worker_epoch,
)


class WorkerRetentionTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-retention-")
        self.run_root = os.path.join(self.root, "run")
        self.epoch_root = os.path.join(self.run_root, "workers", "worker_000", "epoch_000")
        workspace = os.path.join(self.epoch_root, "workspace")
        os.makedirs(workspace)
        with open(os.path.join(workspace, "workspace.json"), "w") as stream:
            json.dump({"root": workspace, "source_libraries": {"HH": os.path.join(self.root, "HH")}}, stream)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _reset_partial_epoch(self, with_workspace=False):
        shutil.rmtree(self.epoch_root)
        os.makedirs(self.epoch_root)
        if with_workspace:
            os.makedirs(os.path.join(self.epoch_root, "workspace"))

    def _make_durable_launch_intent(self):
        workspace = os.path.join(self.epoch_root, "workspace")
        control = persistent_worker_control_root(
            self.epoch_root, "worker-000-epoch-000",
        )
        os.makedirs(control)
        state_path = os.path.join(control, "guardian_state.json")
        stdout_path = os.path.join(control, "ocean.stdout")
        stderr_path = os.path.join(control, "ocean.stderr")
        worker_id = "worker-000-epoch-000-worker-0"
        ocean_command = ["/opt/eda/bin/ocean", "-nograph"]
        guardian_script = guardian_script_path()
        descriptor, lock_path = acquire_legacy_guardian_launch_lock(control)
        parent = legacy_guardian_parent_identity()
        guardian_command = [
            sys.executable, guardian_script,
            "--state-path", state_path,
            "--stdout-path", stdout_path,
            "--stderr-path", stderr_path,
            "--token", worker_id,
            "--cwd", workspace,
            "--expected-parent-pid", str(parent["pid"]),
            "--launch-lock-fd", str(descriptor),
            "--",
        ] + ocean_command
        write_legacy_guardian_manifest(
            control, workspace, state_path, stdout_path, stderr_path, worker_id,
            ocean_command, parent["pid"], guardian_script,
            launch_lock_path=lock_path,
            expected_parent_boot_id=parent["boot_id"],
            expected_parent_start_ticks=parent["start_ticks"],
            expected_uid=parent["uid"], guardian_argv=guardian_command,
        )
        with open(os.path.join(self.epoch_root, "worker_state.json"), "w") as stream:
            json.dump({
                "schema_version": "crescendo.persistent_maestro_worker.v1",
                "worker_index": 0, "epoch": "worker-000-epoch-000",
                "state": "launching", "workspace_root": workspace,
                "control_root": control, "guardian_state_path": state_path,
                "guardian_launch_protocol": GUARDIAN_LAUNCH_PROTOCOL,
                "guardian_launch_manifest_path": os.path.join(
                    control, "legacy_guardian_launch.json",
                ),
                "guardian_launch_lock_path": lock_path,
                "pid": None, "ocean_pid": None,
            }, stream)
        return descriptor

    @staticmethod
    def _fail_manifest_write_on(call_number, message):
        real_atomic_write_json = retention_module.atomic_write_json
        calls = [0]

        def write(path, value):
            calls[0] += 1
            if calls[0] == call_number:
                temporary = path + ".injected.tmp"
                with open(temporary, "w") as stream:
                    json.dump(value, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                raise OSError(message)
            return real_atomic_write_json(path, value)

        return write

    def test_safe_delete_worker_epoch(self):
        result = safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})
        self.assertTrue(result["removed"])
        self.assertFalse(os.path.exists(self.epoch_root))

    def test_held_durable_launch_lock_prevents_direct_worker_epoch_deletion(self):
        descriptor = self._make_durable_launch_intent()
        try:
            with self.assertRaisesRegex(UnsafeCleanupError, "termination is not proven"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})
            self.assertTrue(os.path.isdir(self.epoch_root))
        finally:
            close_legacy_guardian_launch_lock(descriptor)

        result = safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})
        self.assertTrue(result["removed"])
        self.assertFalse(os.path.exists(self.epoch_root))

    def test_safe_delete_namespaced_worker_epoch(self):
        namespaced = os.path.join(
            os.path.dirname(self.epoch_root), "epoch_c000001_l000000",
        )
        os.rename(self.epoch_root, namespaced)
        workspace = os.path.join(namespaced, "workspace")
        metadata_path = os.path.join(workspace, "workspace.json")
        with open(metadata_path, "r") as stream:
            metadata = json.load(stream)
        metadata["root"] = workspace
        with open(metadata_path, "w") as stream:
            json.dump(metadata, stream)
        result = safe_delete_worker_epoch(namespaced, self.run_root, {"ok": True})
        self.assertTrue(result["removed"])
        self.assertFalse(os.path.exists(namespaced))

    def test_worker_cleanup_resumes_after_rmtree_interruption(self):
        def interrupt(payload_root, *_args, **_kwargs):
            os.unlink(os.path.join(payload_root, "workspace", "workspace.json"))
            raise OSError("injected worker rmtree interruption")

        with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "worker rmtree interruption"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})

        self.assertFalse(os.path.exists(self.epoch_root))
        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        with open(os.path.join(entry_root, "manifest.json"), "r") as stream:
            self.assertEqual(json.load(stream)["state"], "quarantined")

        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["cleanup_kind"], "worker_epoch")
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.exists(pending_root))

    def test_prepared_manifest_temp_is_cleaned_as_unstarted_transaction(self):
        with mock.patch(
            "crescendo_engine.retention.atomic_write_json",
            side_effect=self._fail_manifest_write_on(1, "prepared manifest interruption"),
        ):
            with self.assertRaisesRegex(OSError, "prepared manifest interruption"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})

        self.assertTrue(os.path.isdir(self.epoch_root))
        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["state"], "empty_entry_removed")
        self.assertFalse(os.path.lexists(os.path.join(self.run_root, ".cleanup_pending")))

    def test_quarantined_manifest_temp_recovers_from_prepared_canonical(self):
        with mock.patch(
            "crescendo_engine.retention.atomic_write_json",
            side_effect=self._fail_manifest_write_on(2, "quarantined manifest interruption"),
        ):
            with self.assertRaisesRegex(OSError, "quarantined manifest interruption"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})

        self.assertFalse(os.path.exists(self.epoch_root))
        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        with open(os.path.join(entry_root, "manifest.json"), "r") as stream:
            self.assertEqual(json.load(stream)["state"], "prepared")
        self.assertTrue(any(name.endswith(".tmp") for name in os.listdir(entry_root)))

        resumed = resume_pending_cleanups(self.run_root)
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.lexists(pending_root))

    def test_deleted_manifest_temp_recovers_from_quarantined_canonical(self):
        with mock.patch(
            "crescendo_engine.retention.atomic_write_json",
            side_effect=self._fail_manifest_write_on(3, "deleted manifest interruption"),
        ):
            with self.assertRaisesRegex(OSError, "deleted manifest interruption"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})

        self.assertFalse(os.path.exists(self.epoch_root))
        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        with open(os.path.join(entry_root, "manifest.json"), "r") as stream:
            self.assertEqual(json.load(stream)["state"], "quarantined")
        self.assertFalse(os.path.lexists(os.path.join(entry_root, "payload")))
        self.assertTrue(any(name.endswith(".tmp") for name in os.listdir(entry_root)))

        resumed = resume_pending_cleanups(self.run_root)
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.lexists(pending_root))

    def test_manifest_temp_with_payload_but_no_canonical_fails_closed(self):
        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, "cleanup-unstarted")
        os.makedirs(os.path.join(entry_root, "payload"))
        with open(os.path.join(entry_root, "manifest.json.injected.tmp"), "w") as stream:
            stream.write("temporary")

        with self.assertRaisesRegex(UnsafeCleanupError, "no manifest"):
            resume_pending_cleanups(self.run_root)

        self.assertTrue(os.path.isdir(os.path.join(entry_root, "payload")))
        self.assertFalse(os.path.lexists(os.path.join(
            entry_root, "manifest.json.injected.tmp",
        )))

    def test_worker_cleanup_rejects_nested_mount_and_cross_device_tree(self):
        nested = os.path.join(self.epoch_root, "workspace", "nested")
        os.makedirs(nested)
        real_ismount = os.path.ismount
        with mock.patch(
            "crescendo_engine.retention.os.path.ismount",
            side_effect=lambda path: (
                os.path.abspath(path) == os.path.abspath(nested) or real_ismount(path)
            ),
        ):
            with self.assertRaisesRegex(UnsafeCleanupError, "mount"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})

        real_lstat = os.lstat

        def cross_device_lstat(path):
            result = real_lstat(path)
            if os.path.abspath(path) == os.path.abspath(nested):
                fields = list(result)
                fields[2] = result.st_dev + 1
                return os.stat_result(fields)
            return result

        with mock.patch("crescendo_engine.retention.os.lstat", side_effect=cross_device_lstat):
            with self.assertRaisesRegex(UnsafeCleanupError, "filesystem boundary"):
                safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_worker_cleanup_rejects_bidirectional_source_workspace_overlap(self):
        workspace = os.path.join(self.epoch_root, "workspace")
        metadata_path = os.path.join(workspace, "workspace.json")
        inside_source = os.path.join(workspace, "source")
        os.makedirs(inside_source)
        for source in (inside_source, self.epoch_root):
            with self.subTest(source=source):
                with open(metadata_path, "w") as stream:
                    json.dump({"root": workspace, "source_libraries": {"HH": source}}, stream)
                with self.assertRaisesRegex(UnsafeCleanupError, "overlaps"):
                    safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": True})
                self.assertTrue(os.path.isdir(self.epoch_root))

    def test_rejects_failed_source_guard(self):
        with self.assertRaises(UnsafeCleanupError):
            safe_delete_worker_epoch(self.epoch_root, self.run_root, {"ok": False})
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_rejects_path_outside_workers(self):
        outside = os.path.join(self.run_root, "outside")
        os.rename(self.epoch_root, outside)
        with self.assertRaises(UnsafeCleanupError):
            safe_delete_worker_epoch(outside, self.run_root, {"ok": True})

    def test_rejects_worker_parent_symlink(self):
        real_worker = os.path.join(self.run_root, "workers", "worker_real")
        os.rename(os.path.dirname(self.epoch_root), real_worker)
        linked_worker = os.path.join(self.run_root, "workers", "worker_000")
        os.symlink(real_worker, linked_worker)
        linked_epoch = os.path.join(linked_worker, "epoch_000")
        with self.assertRaises(UnsafeCleanupError):
            safe_delete_worker_epoch(linked_epoch, self.run_root, {"ok": True})

    def test_partial_epoch_without_workspace_requires_explicit_proof(self):
        self._reset_partial_epoch()
        with self.assertRaisesRegex(UnsafeCleanupError, "requires explicit"):
            cleanup_abandoned_worker_epoch(
                self.epoch_root, self.run_root,
                lambda _metadata: self.fail("source verifier must not be called"),
            )
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_worker_not_created_proof_removes_partial_epoch(self):
        self._reset_partial_epoch()
        result = cleanup_abandoned_worker_epoch(
            self.epoch_root, self.run_root,
            lambda _metadata: self.fail("source verifier must not be called"),
            termination_proof={"ok": True, "kind": "worker_not_created"},
        )
        self.assertTrue(result["removed"])
        self.assertEqual(result["proof_kind"], "worker_not_created")
        self.assertFalse(os.path.exists(self.epoch_root))

    def test_ocean_not_spawned_proof_removes_partial_epoch(self):
        self._reset_partial_epoch(with_workspace=True)
        with open(os.path.join(self.epoch_root, "worker_state.json"), "w") as stream:
            json.dump({"state": "new", "pid": None, "ocean_pid": None}, stream)
        result = cleanup_abandoned_worker_epoch(
            self.epoch_root, self.run_root,
            lambda _metadata: self.fail("source verifier must not be called"),
            termination_proof={"ok": True, "kind": "ocean_not_spawned"},
        )
        self.assertTrue(result["removed"])
        self.assertEqual(result["proof_kind"], "ocean_not_spawned")
        self.assertFalse(os.path.exists(self.epoch_root))

    def test_partial_epoch_rejects_unapproved_proof_kind(self):
        self._reset_partial_epoch()
        with self.assertRaisesRegex(UnsafeCleanupError, "requires explicit"):
            cleanup_abandoned_worker_epoch(
                self.epoch_root, self.run_root, lambda _metadata: {"ok": True},
                termination_proof={"ok": True, "kind": "pid_is_gone"},
            )
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_partial_epoch_rejects_proof_conflicting_with_worker_state(self):
        self._reset_partial_epoch()
        with open(os.path.join(self.epoch_root, "worker_state.json"), "w") as stream:
            json.dump({"pid": 1234, "ocean_pid": 5678}, stream)
        with self.assertRaisesRegex(UnsafeCleanupError, "conflicts"):
            cleanup_abandoned_worker_epoch(
                self.epoch_root, self.run_root, lambda _metadata: {"ok": True},
                termination_proof={"ok": True, "kind": "ocean_not_spawned"},
            )
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_partial_epoch_rejects_incomplete_worker_state(self):
        self._reset_partial_epoch()
        with open(os.path.join(self.epoch_root, "worker_state.json"), "w") as stream:
            json.dump({}, stream)
        with self.assertRaisesRegex(UnsafeCleanupError, "conflicts"):
            cleanup_abandoned_worker_epoch(
                self.epoch_root, self.run_root, lambda _metadata: {"ok": True},
                termination_proof={"ok": True, "kind": "ocean_not_spawned"},
            )
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_partial_epoch_rejects_any_symlink_without_touching_target(self):
        self._reset_partial_epoch()
        outside = os.path.join(self.root, "outside")
        os.makedirs(outside)
        marker = os.path.join(outside, "keep.txt")
        with open(marker, "w") as stream:
            stream.write("keep")
        os.symlink(outside, os.path.join(self.epoch_root, "linked"))
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_worker_epoch(
                self.epoch_root, self.run_root, lambda _metadata: {"ok": True},
                termination_proof={"ok": True, "kind": "worker_not_created"},
            )
        self.assertTrue(os.path.isfile(marker))
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_partial_epoch_rejects_non_numbered_worker_path(self):
        unsafe_epoch = os.path.join(self.run_root, "workers", "worker_bad", "epoch_000")
        os.makedirs(unsafe_epoch)
        with self.assertRaisesRegex(UnsafeCleanupError, "exactly"):
            cleanup_abandoned_worker_epoch(
                unsafe_epoch, self.run_root, lambda _metadata: {"ok": True},
                termination_proof={"ok": True, "kind": "worker_not_created"},
            )
        self.assertTrue(os.path.isdir(unsafe_epoch))


class ControllerRetentionTests(unittest.TestCase):
    _ALLOWED_PROOFS = (
        "controller_not_started",
        "recorded_pid_is_dead",
        "recorded_pid_was_reused",
        "prior_boot_processes_dead",
        "controller_termination_proven",
    )

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-controller-retention-")
        self.run_root = os.path.join(self.root, "run")
        self.controllers_root = os.path.join(self.run_root, "controllers")
        self.slot_root = os.path.join(self.controllers_root, "slot_000")
        self.epoch_root = os.path.join(self.slot_root, "epoch_000")
        os.makedirs(self.epoch_root)
        self._write_epoch_files()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _write_epoch_files(self):
        with open(os.path.join(self.epoch_root, "controller_liveness.lock"), "w") as stream:
            stream.write("lock\n")
        with open(os.path.join(self.epoch_root, "controller_state.json"), "w") as stream:
            json.dump({"status": "terminated"}, stream)

    def _reset_epoch(self):
        if os.path.lexists(self.epoch_root):
            shutil.rmtree(self.epoch_root)
        os.makedirs(self.epoch_root)
        self._write_epoch_files()

    @staticmethod
    def _proof(kind):
        return {"ok": True, "kind": kind}

    def test_accepts_only_explicit_proven_dead_kinds(self):
        for kind in self._ALLOWED_PROOFS:
            with self.subTest(kind=kind):
                self._reset_epoch()
                result = cleanup_abandoned_controller_epoch(
                    self.epoch_root, self.run_root, self._proof(kind),
                )
                self.assertTrue(result["ok"])
                self.assertTrue(result["removed"])
                self.assertEqual(result["proof_kind"], kind)
                self.assertEqual(
                    result["controller_epoch"], "controllers/slot_000/epoch_000",
                )
                self.assertFalse(os.path.exists(self.epoch_root))

    def test_controller_cleanup_resumes_after_rmtree_interruption(self):
        def interrupt(payload_root, *_args, **_kwargs):
            os.unlink(os.path.join(payload_root, "controller_state.json"))
            raise OSError("injected controller rmtree interruption")

        with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "controller rmtree interruption"):
                cleanup_abandoned_controller_epoch(
                    self.epoch_root, self.run_root,
                    self._proof("recorded_pid_is_dead"),
                )

        self.assertFalse(os.path.exists(self.epoch_root))
        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        with open(os.path.join(entry_root, "manifest.json"), "r") as stream:
            self.assertEqual(json.load(stream)["state"], "quarantined")

        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["cleanup_kind"], "controller_epoch")
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.exists(pending_root))

    def test_rejects_missing_failed_truthy_or_unknown_proof(self):
        rejected = (
            None,
            {},
            {"ok": False, "kind": "recorded_pid_is_dead"},
            {"ok": 1, "kind": "recorded_pid_is_dead"},
            {"ok": True, "kind": "owned_controller_is_alive"},
            {"ok": True, "kind": "current_boot_pid_identity_missing"},
            {"ok": True, "kind": "guardian_cleanup_proven"},
        )
        for proof in rejected:
            with self.subTest(proof=proof):
                with self.assertRaisesRegex(UnsafeCleanupError, "proven-dead"):
                    cleanup_abandoned_controller_epoch(
                        self.epoch_root, self.run_root, proof,
                    )
                self.assertTrue(os.path.isdir(self.epoch_root))

    def test_rejects_live_liveness_lock(self):
        lock_path = os.path.join(self.epoch_root, "controller_liveness.lock")
        descriptor = os.open(lock_path, os.O_RDWR)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with self.assertRaisesRegex(UnsafeCleanupError, "still held"):
                cleanup_abandoned_controller_epoch(
                    self.epoch_root, self.run_root,
                    self._proof("recorded_pid_is_dead"),
                )
            self.assertTrue(os.path.isdir(self.epoch_root))
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def test_rejects_missing_or_nonregular_liveness_lock(self):
        lock_path = os.path.join(self.epoch_root, "controller_liveness.lock")
        os.unlink(lock_path)
        with self.assertRaisesRegex(UnsafeCleanupError, "missing"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("controller_not_started"),
            )
        os.mkdir(lock_path)
        with self.assertRaisesRegex(UnsafeCleanupError, "not a regular"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("controller_not_started"),
            )
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_rejects_non_numbered_and_escaped_targets(self):
        targets = (
            os.path.join(self.controllers_root, "slot_bad", "epoch_000"),
            os.path.join(self.slot_root, "epoch_bad"),
            os.path.join(self.root, "controllers", "slot_000", "epoch_000"),
        )
        for target in targets:
            with self.subTest(target=target):
                os.makedirs(target)
                with open(os.path.join(target, "controller_liveness.lock"), "w") as stream:
                    stream.write("lock\n")
                with self.assertRaisesRegex(UnsafeCleanupError, "exactly"):
                    cleanup_abandoned_controller_epoch(
                        target, self.run_root,
                        self._proof("recorded_pid_is_dead"),
                    )
                self.assertTrue(os.path.isdir(target))

    def test_rejects_symlinked_controllers_root(self):
        real_root = os.path.join(self.root, "real-controllers")
        os.rename(self.controllers_root, real_root)
        os.symlink(real_root, self.controllers_root)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("recorded_pid_is_dead"),
            )
        self.assertTrue(os.path.isfile(os.path.join(
            real_root, "slot_000", "epoch_000", "controller_state.json",
        )))

    def test_rejects_symlinked_run_root(self):
        real_run = os.path.join(self.root, "real-run")
        os.rename(self.run_root, real_run)
        os.symlink(real_run, self.run_root)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("recorded_pid_is_dead"),
            )
        self.assertTrue(os.path.isfile(os.path.join(
            real_run, "controllers", "slot_000", "epoch_000", "controller_state.json",
        )))

    def test_rejects_symlinked_slot(self):
        real_slot = os.path.join(self.root, "real-slot")
        os.rename(self.slot_root, real_slot)
        os.symlink(real_slot, self.slot_root)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("recorded_pid_is_dead"),
            )
        self.assertTrue(os.path.isfile(os.path.join(
            real_slot, "epoch_000", "controller_state.json",
        )))

    def test_rejects_symlinked_epoch(self):
        real_epoch = os.path.join(self.root, "real-epoch")
        os.rename(self.epoch_root, real_epoch)
        os.symlink(real_epoch, self.epoch_root)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("recorded_pid_is_dead"),
            )
        self.assertTrue(os.path.isfile(os.path.join(real_epoch, "controller_state.json")))

    def test_rejects_symlink_anywhere_below_epoch(self):
        outside = os.path.join(self.root, "outside")
        os.makedirs(outside)
        marker = os.path.join(outside, "keep.txt")
        with open(marker, "w") as stream:
            stream.write("keep")
        os.symlink(outside, os.path.join(self.epoch_root, "linked"))
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("recorded_pid_is_dead"),
            )
        self.assertTrue(os.path.isfile(marker))
        self.assertTrue(os.path.isdir(self.epoch_root))

    def test_rejects_symlinked_liveness_lock(self):
        outside_lock = os.path.join(self.root, "outside.lock")
        with open(outside_lock, "w") as stream:
            stream.write("keep")
        lock_path = os.path.join(self.epoch_root, "controller_liveness.lock")
        os.unlink(lock_path)
        os.symlink(outside_lock, lock_path)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_controller_epoch(
                self.epoch_root, self.run_root,
                self._proof("recorded_pid_is_dead"),
            )
        with open(outside_lock, "r") as stream:
            self.assertEqual(stream.read(), "keep")

    def test_rejects_mount_at_hierarchy_or_below_epoch(self):
        nested = os.path.join(self.epoch_root, "nested")
        os.makedirs(nested)
        real_ismount = os.path.ismount
        for mounted in (
            self.controllers_root, self.slot_root, self.epoch_root, nested,
        ):
            with self.subTest(mounted=mounted), mock.patch(
                "crescendo_engine.retention.os.path.ismount",
                side_effect=lambda path, mounted=mounted: (
                    os.path.abspath(path) == os.path.abspath(mounted) or real_ismount(path)
                ),
            ):
                with self.assertRaisesRegex(UnsafeCleanupError, "mount"):
                    cleanup_abandoned_controller_epoch(
                        self.epoch_root, self.run_root,
                        self._proof("recorded_pid_is_dead"),
                    )
                self.assertTrue(os.path.isdir(self.epoch_root))


class CandidateRetentionTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="crescendo-candidate-retention-")
        self.run_root = os.path.join(self.root, "run")
        self.candidate_root = os.path.join(self.run_root, "candidates", "g000_c0000")
        os.makedirs(self.candidate_root)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _write_json(self, name, value):
        with open(os.path.join(self.candidate_root, name), "w") as stream:
            json.dump(value, stream)

    def _write_running_candidate(self, candidate_id="g000_c0000", status="running"):
        self._write_json("candidate.json", {
            "candidate_id": candidate_id, "status": status, "values": {"w": 1e-6},
        })

    def _write_proven_controller_failure(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "infrastructure_failed",
            "failure_class": "persistent_controller", "values": {"w": 1e-6},
            "persistent_worker": {"controller_termination": {
                "termination_proven": True, "process_still_alive": False,
                "controller_pid": 123, "controller_returncode": -9,
                "cleanup": {"ok": True, "removed": True},
            }},
        })
        self._write_json("timing.json", {"phases": {}})

    def test_terminal_cleanup_preserves_retained_logs_until_explicit_clear(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "completed",
            "source_guard": {"ok": True},
        })
        self._write_json("completion_evidence.json", {"ok": True})
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        self._write_json(os.path.join("workspace", "workspace.json"), {
            "root": workspace, "source_libraries": {},
        })
        retained_log = os.path.join(
            self.candidate_root, "retained_spectre_token.log",
        )
        retained_meta = os.path.join(
            self.candidate_root, "retained_spectre_token.meta",
        )
        with open(retained_log, "w") as stream:
            stream.write("spectre log\n")
        with open(retained_meta, "w") as stream:
            stream.write("schema_version=crescendo.retained_spectre_log.v1\n")
        with open(os.path.join(workspace, "raw.bin"), "w") as stream:
            stream.write("raw")

        result = safe_delete_candidate_workspace(
            self.candidate_root, self.run_root, {"ok": True},
        )
        self.assertTrue(result["ok"])
        self.assertFalse(os.path.exists(workspace))
        self.assertEqual(
            retained_spectre_paths(self.candidate_root),
            [retained_log, retained_meta],
        )
        cleared = clear_retained_spectre_logs(self.run_root)
        self.assertEqual(cleared["removed_count"], 2)
        self.assertFalse(os.path.exists(retained_log))
        self.assertFalse(os.path.exists(retained_meta))
        self.assertTrue(os.path.isfile(os.path.join(
            self.candidate_root, "candidate.json",
        )))
        self.assertTrue(os.path.isfile(os.path.join(
            self.candidate_root, "completion_evidence.json",
        )))

    def test_lightweight_candidate_keeps_only_spectre_log_identity(self):
        compact = lightweight_candidate_record({
            "candidate_id": "g000_c0000", "status": "completed",
            "completion_evidence": {
                "ok": True,
                "spectre_runs": [{
                    "test": "AC", "corner": "TT", "point_id": 1,
                    "spectre": {
                        "path": "/private/raw/1/AC/psf/spectre.out",
                        "size_bytes": 123,
                    },
                }],
            },
        })
        self.assertEqual(compact["completion"]["spectre_logs"], [{
            "test": "AC", "corner": "TT", "point_id": 1,
        }])
        self.assertNotIn("/private/raw", json.dumps(compact))

    def test_abandoned_nonterminal_without_workspace_removes_entire_safe_tree(self):
        self._write_running_candidate()
        raw_root = os.path.join(self.candidate_root, "attempts")
        os.makedirs(raw_root)
        with open(os.path.join(raw_root, "ocean.log"), "w") as stream:
            stream.write("raw")
        result = cleanup_abandoned_nonterminal_candidate(
            self.candidate_root, self.run_root,
            lambda _metadata: self.fail("source verifier must not be called without workspace"),
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["removed"])
        self.assertFalse(result["workspace_verified"])
        self.assertEqual(result["candidate_id"], "g000_c0000")
        self.assertEqual(result["candidate_root"], "candidates/g000_c0000")
        self.assertFalse(os.path.exists(self.candidate_root))

    def test_nonterminal_candidate_cleanup_resumes_after_rmtree_interruption(self):
        self._write_running_candidate()
        raw_path = os.path.join(self.candidate_root, "raw.log")
        with open(raw_path, "w") as stream:
            stream.write("raw")

        def interrupt(payload_root, *_args, **_kwargs):
            os.unlink(os.path.join(payload_root, "raw.log"))
            raise OSError("injected candidate rmtree interruption")

        with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "candidate rmtree interruption"):
                cleanup_abandoned_nonterminal_candidate(
                    self.candidate_root, self.run_root,
                    lambda _metadata: self.fail("workspace verifier must not run"),
                )

        self.assertFalse(os.path.exists(self.candidate_root))
        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        with open(os.path.join(entry_root, "manifest.json"), "r") as stream:
            self.assertEqual(json.load(stream)["state"], "quarantined")

        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["cleanup_kind"], "nonterminal_candidate")
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.exists(pending_root))

    def test_abandoned_nonterminal_workspace_requires_successful_source_verification(self):
        self._write_running_candidate()
        source = os.path.join(self.root, "HH")
        os.makedirs(source)
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        metadata = {"root": workspace, "source_libraries": {"HH": source}}
        self._write_json(os.path.join("workspace", "workspace.json"), metadata)
        calls = []

        def verify(document):
            calls.append(document)
            return {"ok": True}

        result = cleanup_abandoned_nonterminal_candidate(
            self.candidate_root, self.run_root, verify,
        )
        self.assertEqual(calls, [metadata])
        self.assertTrue(result["workspace_verified"])
        self.assertFalse(os.path.exists(self.candidate_root))

    def test_abandoned_nonterminal_rejects_failed_source_verification(self):
        self._write_running_candidate()
        source = os.path.join(self.root, "HH")
        os.makedirs(source)
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        self._write_json(os.path.join("workspace", "workspace.json"), {
            "root": workspace, "source_libraries": {"HH": source},
        })
        with self.assertRaisesRegex(UnsafeCleanupError, "source integrity"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": False},
            )
        self.assertTrue(os.path.isdir(self.candidate_root))

    def test_abandoned_nonterminal_rejects_workspace_without_valid_metadata(self):
        self._write_running_candidate()
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        with self.assertRaisesRegex(UnsafeCleanupError, "metadata is missing"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root,
                lambda _metadata: self.fail("invalid metadata must not be verified"),
            )
        self._write_json(os.path.join("workspace", "workspace.json"), {
            "root": os.path.join(self.root, "wrong"), "source_libraries": {},
        })
        with self.assertRaisesRegex(UnsafeCleanupError, "root does not match"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root,
                lambda _metadata: self.fail("mismatched metadata must not be verified"),
            )
        self.assertTrue(os.path.isdir(self.candidate_root))

    def test_abandoned_nonterminal_rejects_source_overlapping_candidate(self):
        self._write_running_candidate()
        source = os.path.join(self.candidate_root, "source")
        os.makedirs(source)
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        self._write_json(os.path.join("workspace", "workspace.json"), {
            "root": workspace, "source_libraries": {"HH": source},
        })
        with self.assertRaisesRegex(UnsafeCleanupError, "overlaps"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root,
                lambda _metadata: self.fail("overlapping source must not be verified"),
            )
        self.assertTrue(os.path.isdir(source))

    def test_abandoned_nonterminal_rejects_terminal_or_malformed_status(self):
        statuses = ("completed", "FAILED", "infrastructure_failed", None, " running ")
        for status in statuses:
            with self.subTest(status=status):
                self._write_running_candidate(status=status)
                with self.assertRaisesRegex(UnsafeCleanupError, "terminal|status"):
                    cleanup_abandoned_nonterminal_candidate(
                        self.candidate_root, self.run_root,
                        lambda _metadata: self.fail("status rejection must precede verification"),
                    )
                self.assertTrue(os.path.isdir(self.candidate_root))

    def test_abandoned_nonterminal_rejects_candidate_identity_mismatch(self):
        self._write_running_candidate(candidate_id="g999_c9999")
        with self.assertRaisesRegex(UnsafeCleanupError, "id does not match"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root,
                lambda _metadata: self.fail("identity rejection must precede verification"),
            )
        self.assertTrue(os.path.isdir(self.candidate_root))

    def test_abandoned_nonterminal_rejects_missing_or_symlinked_candidate_record(self):
        with self.assertRaisesRegex(UnsafeCleanupError, "record is missing"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
            )
        outside_record = os.path.join(self.root, "candidate.json")
        with open(outside_record, "w") as stream:
            json.dump({"candidate_id": "g000_c0000", "status": "running"}, stream)
        os.symlink(outside_record, os.path.join(self.candidate_root, "candidate.json"))
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
            )
        self.assertTrue(os.path.isfile(outside_record))

    def test_abandoned_nonterminal_rejects_non_direct_or_escaped_target(self):
        targets = (
            os.path.join(self.candidate_root, "nested", "g001_c0000"),
            os.path.join(self.root, "candidates", "g001_c0000"),
            os.path.join(self.run_root, "candidates"),
        )
        for target in targets:
            with self.subTest(target=target):
                if not os.path.isdir(target):
                    os.makedirs(target)
                with self.assertRaisesRegex(UnsafeCleanupError, "exactly"):
                    cleanup_abandoned_nonterminal_candidate(
                        target, self.run_root, lambda _metadata: {"ok": True},
                    )
                self.assertTrue(os.path.isdir(target))

    def test_abandoned_nonterminal_rejects_symlinked_hierarchy_and_nested_link(self):
        self._write_running_candidate()
        outside = os.path.join(self.root, "outside")
        os.makedirs(outside)
        marker = os.path.join(outside, "keep.txt")
        with open(marker, "w") as stream:
            stream.write("keep")
        os.symlink(outside, os.path.join(self.candidate_root, "linked"))
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
            )
        self.assertTrue(os.path.isfile(marker))

    def test_abandoned_nonterminal_rejects_symlinked_candidate_root(self):
        self._write_running_candidate()
        real_candidate = os.path.join(self.root, "real-candidate")
        os.rename(self.candidate_root, real_candidate)
        os.symlink(real_candidate, self.candidate_root)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
            )
        self.assertTrue(os.path.isfile(os.path.join(real_candidate, "candidate.json")))

    def test_abandoned_nonterminal_rejects_symlinked_candidates_root(self):
        self._write_running_candidate()
        candidates_root = os.path.dirname(self.candidate_root)
        real_candidates = os.path.join(self.root, "real-candidates")
        os.rename(candidates_root, real_candidates)
        os.symlink(real_candidates, candidates_root)
        with self.assertRaisesRegex(UnsafeCleanupError, "symbolic link"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
            )
        self.assertTrue(os.path.isfile(os.path.join(
            real_candidates, "g000_c0000", "candidate.json",
        )))

    def test_abandoned_nonterminal_rejects_mount_or_cross_device_tree(self):
        self._write_running_candidate()
        nested = os.path.join(self.candidate_root, "nested")
        os.makedirs(nested)
        candidates_root = os.path.dirname(self.candidate_root)
        real_ismount = os.path.ismount
        for mounted in (candidates_root, self.candidate_root, nested):
            with self.subTest(mounted=mounted), mock.patch(
                "crescendo_engine.retention.os.path.ismount",
                side_effect=lambda path, mounted=mounted: (
                    os.path.abspath(path) == os.path.abspath(mounted) or real_ismount(path)
                ),
            ):
                with self.assertRaisesRegex(UnsafeCleanupError, "mount"):
                    cleanup_abandoned_nonterminal_candidate(
                        self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
                    )
                self.assertTrue(os.path.isdir(self.candidate_root))

        real_lstat = os.lstat

        def cross_device_lstat(path):
            result = real_lstat(path)
            if os.path.abspath(path) == os.path.abspath(nested):
                fields = list(result)
                fields[2] = result.st_dev + 1
                return os.stat_result(fields)
            return result

        with mock.patch("crescendo_engine.retention.os.lstat", side_effect=cross_device_lstat):
            with self.assertRaisesRegex(UnsafeCleanupError, "filesystem boundary"):
                cleanup_abandoned_nonterminal_candidate(
                    self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
                )
        self.assertTrue(os.path.isdir(self.candidate_root))

    def test_abandoned_nonterminal_rejects_special_filesystem_object(self):
        self._write_running_candidate()
        os.mkfifo(os.path.join(self.candidate_root, "control.fifo"))
        with self.assertRaisesRegex(UnsafeCleanupError, "non-regular"):
            cleanup_abandoned_nonterminal_candidate(
                self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
            )
        self.assertTrue(os.path.isdir(self.candidate_root))

    def test_terminal_candidate_without_workspace_reclaims_raw_artifacts(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "completed",
            "values": {"w": 1e-6}, "source_guard": {"ok": True},
            "traceback": "raw detail that should not be retained",
        })
        self._write_json("completion_evidence.json", {"ok": True})
        self._write_json("timing.json", {"phases": {}})
        with open(os.path.join(self.candidate_root, "raw.rdb"), "w") as stream:
            stream.write("large raw result")
        os.makedirs(os.path.join(self.candidate_root, "attempts"))

        result = cleanup_terminal_candidate_artifacts(
            self.candidate_root, self.run_root,
            lambda _workspace: self.fail("workspace verifier must not be used"),
            required_lightweight_paths=[
                os.path.join(self.candidate_root, "candidate.json"),
                os.path.join(self.candidate_root, "completion_evidence.json"),
            ],
        )

        self.assertTrue(result["ok"])
        self.assertFalse(result["removed"])
        self.assertEqual(result["raw_artifacts_removed"], ["attempts/", "raw.rdb"])
        self.assertTrue(os.path.isfile(os.path.join(self.candidate_root, "candidate.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.candidate_root, "completion_evidence.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.candidate_root, "timing.json")))
        with open(os.path.join(self.candidate_root, "candidate.json"), "r") as stream:
            candidate = json.load(stream)
        self.assertNotIn("traceback", candidate)

    def test_proven_controller_failure_reclaims_only_raw_candidate_artifacts(self):
        self._write_proven_controller_failure()
        for name in (
            "retained_spectre_failure.log", "retained_spectre_failure.meta",
        ):
            with open(os.path.join(self.candidate_root, name), "w") as stream:
                stream.write("retained")
        for name in (
            "persistent_worker_candidate.il", "persistent_worker_candidate.json",
            "persistent_worker_candidate.status.tsv", "pristine_restore.records",
            "schematic_application.records", "schematic_baseline.records",
        ):
            with open(os.path.join(self.candidate_root, name), "w") as stream:
                stream.write("raw")

        result = cleanup_terminal_candidate_artifacts(
            self.candidate_root, self.run_root,
            lambda _workspace: self.fail("source verifier must not be used"),
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["proof_kind"], "controller_termination_proven")
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)), [
                "candidate.json", "retained_spectre_failure.log",
                "retained_spectre_failure.meta", "timing.json",
            ],
        )
        candidate = retention_module.load_json(os.path.join(
            self.candidate_root, "candidate.json",
        ))
        self.assertNotIn("source_guard", candidate)
        self.assertTrue(candidate["persistent_worker"]["controller_termination"][
            "termination_proven"
        ])

    def test_proven_controller_failure_cleanup_resumes_from_prepared_manifest(self):
        self._write_proven_controller_failure()
        with open(os.path.join(self.candidate_root, "raw.log"), "w") as stream:
            stream.write("raw")

        with mock.patch(
            "crescendo_engine.retention._move_terminal_cleanup_items",
            side_effect=OSError("injected prepared interruption"),
        ):
            with self.assertRaisesRegex(OSError, "prepared interruption"):
                cleanup_terminal_candidate_artifacts(
                    self.candidate_root, self.run_root, lambda _workspace: {"ok": True},
                )

        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        self.assertEqual(
            retention_module.load_json(os.path.join(entry_root, "manifest.json"))["state"],
            "prepared",
        )
        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["manifest"]["proof_kind"], "controller_termination_proven")
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)), ["candidate.json", "timing.json"],
        )

    def test_proven_controller_failure_cleanup_resumes_from_quarantined_manifest(self):
        self._write_proven_controller_failure()
        raw_root = os.path.join(self.candidate_root, "records")
        os.makedirs(raw_root)
        with open(os.path.join(raw_root, "raw.log"), "w") as stream:
            stream.write("raw")

        with mock.patch(
            "crescendo_engine.retention.shutil.rmtree",
            side_effect=OSError("injected quarantined interruption"),
        ):
            with self.assertRaisesRegex(OSError, "quarantined interruption"):
                cleanup_terminal_candidate_artifacts(
                    self.candidate_root, self.run_root, lambda _workspace: {"ok": True},
                )

        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        self.assertEqual(
            retention_module.load_json(os.path.join(entry_root, "manifest.json"))["state"],
            "quarantined",
        )
        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["manifest"]["proof_kind"], "controller_termination_proven")
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)), ["candidate.json", "timing.json"],
        )

    def test_controller_failure_cleanup_requires_proof_and_refuses_workspace(self):
        self._write_proven_controller_failure()
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        with self.assertRaisesRegex(UnsafeCleanupError, "refuses any candidate workspace"):
            cleanup_terminal_candidate_artifacts(
                self.candidate_root, self.run_root, lambda _workspace: {"ok": True},
            )
        self.assertTrue(os.path.isdir(workspace))

        shutil.rmtree(workspace)
        candidate_path = os.path.join(self.candidate_root, "candidate.json")
        candidate = retention_module.load_json(candidate_path)
        candidate["persistent_worker"]["controller_termination"]["process_still_alive"] = True
        with open(candidate_path, "w") as stream:
            json.dump(candidate, stream)
        with self.assertRaisesRegex(UnsafeCleanupError, "termination proof"):
            cleanup_terminal_candidate_artifacts(
                self.candidate_root, self.run_root, lambda _workspace: {"ok": True},
            )
        self.assertTrue(os.path.isfile(candidate_path))

    def test_direct_controller_failure_cleanup_rejects_symlink_and_malformed_record(self):
        self._write_proven_controller_failure()
        candidate_path = os.path.join(self.candidate_root, "candidate.json")
        outside = os.path.join(self.root, "outside.json")
        with open(outside, "w") as stream:
            stream.write("{}")
        os.unlink(candidate_path)
        os.symlink(outside, candidate_path)
        with self.assertRaisesRegex(UnsafeCleanupError, "missing or unsafe"):
            retention_module.cleanup_proven_terminal_infrastructure_candidate(
                self.candidate_root, self.run_root,
            )
        self.assertTrue(os.path.islink(candidate_path))

        os.unlink(candidate_path)
        with open(candidate_path, "w") as stream:
            stream.write("not-json")
        with self.assertRaisesRegex(UnsafeCleanupError, "cannot be read"):
            retention_module.cleanup_proven_terminal_infrastructure_candidate(
                self.candidate_root, self.run_root,
            )
        with open(candidate_path, "r") as stream:
            self.assertEqual(stream.read(), "not-json")

        self._write_proven_controller_failure()
        candidate = retention_module.load_json(candidate_path)
        candidate["candidate_id"] = "g000_c9999"
        with open(candidate_path, "w") as stream:
            json.dump(candidate, stream)
        with self.assertRaisesRegex(UnsafeCleanupError, "identity does not match"):
            retention_module.cleanup_proven_terminal_infrastructure_candidate(
                self.candidate_root, self.run_root,
            )

    def test_terminal_candidate_cleanup_resumes_after_partial_rmtree(self):
        source = os.path.join(self.root, "HH")
        os.makedirs(source)
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "completed",
            "values": {"w": 1e-6}, "source_guard": {"ok": True},
        })
        self._write_json("completion_evidence.json", {"ok": True})
        self._write_json("timing.json", {"phases": {}})
        self._write_json(os.path.join("workspace", "workspace.json"), {
            "root": workspace, "source_libraries": {"HH": source},
        })
        with open(os.path.join(self.candidate_root, "raw.log"), "w") as stream:
            stream.write("raw")

        def interrupt(payload_root, *_args, **_kwargs):
            os.unlink(os.path.join(payload_root, "workspace", "workspace.json"))
            raise OSError("injected terminal rmtree interruption")

        with mock.patch("crescendo_engine.retention.shutil.rmtree", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "terminal rmtree interruption"):
                cleanup_terminal_candidate_artifacts(
                    self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
                )

        pending_root = os.path.join(self.run_root, ".cleanup_pending")
        entry_root = os.path.join(pending_root, os.listdir(pending_root)[0])
        with open(os.path.join(entry_root, "manifest.json"), "r") as stream:
            self.assertEqual(json.load(stream)["state"], "quarantined")
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)),
            ["candidate.json", "completion_evidence.json", "timing.json"],
        )

        resumed = resume_pending_cleanups(self.run_root)
        self.assertEqual(resumed[0]["cleanup_kind"], "terminal_candidate_artifacts")
        self.assertTrue(resumed[0]["removed"])
        self.assertFalse(os.path.lexists(pending_root))
        self.assertEqual(resume_pending_cleanups(self.run_root), [])
        self.assertEqual(
            sorted(os.listdir(self.candidate_root)),
            ["candidate.json", "completion_evidence.json", "timing.json"],
        )
        with open(os.path.join(self.candidate_root, "candidate.json"), "r") as stream:
            candidate = json.load(stream)
        self.assertEqual(candidate["candidate_id"], "g000_c0000")
        self.assertEqual(candidate["status"], "completed")

    def test_terminal_candidate_rejects_nested_mount_and_cross_device_tree(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "completed",
            "source_guard": {"ok": True},
        })
        self._write_json("completion_evidence.json", {"ok": True})
        nested = os.path.join(self.candidate_root, "attempts", "nested")
        os.makedirs(nested)
        real_ismount = os.path.ismount
        with mock.patch(
            "crescendo_engine.retention.os.path.ismount",
            side_effect=lambda path: (
                os.path.abspath(path) == os.path.abspath(nested) or real_ismount(path)
            ),
        ):
            with self.assertRaisesRegex(UnsafeCleanupError, "mount"):
                cleanup_terminal_candidate_artifacts(
                    self.candidate_root, self.run_root,
                    lambda _metadata: self.fail("workspace verifier must not run"),
                )

        real_lstat = os.lstat

        def cross_device_lstat(path):
            result = real_lstat(path)
            if os.path.abspath(path) == os.path.abspath(nested):
                fields = list(result)
                fields[2] = result.st_dev + 1
                return os.stat_result(fields)
            return result

        with mock.patch("crescendo_engine.retention.os.lstat", side_effect=cross_device_lstat):
            with self.assertRaisesRegex(UnsafeCleanupError, "filesystem boundary"):
                cleanup_terminal_candidate_artifacts(
                    self.candidate_root, self.run_root,
                    lambda _metadata: self.fail("workspace verifier must not run"),
                )
        self.assertTrue(os.path.isdir(self.candidate_root))

    def test_terminal_candidate_rejects_bidirectional_source_workspace_overlap(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "completed",
            "source_guard": {"ok": True},
        })
        self._write_json("completion_evidence.json", {"ok": True})
        workspace = os.path.join(self.candidate_root, "workspace")
        os.makedirs(workspace)
        inside_source = os.path.join(workspace, "source")
        os.makedirs(inside_source)
        metadata_path = os.path.join(workspace, "workspace.json")
        for source in (inside_source, self.candidate_root):
            with self.subTest(source=source):
                with open(metadata_path, "w") as stream:
                    json.dump({"root": workspace, "source_libraries": {"HH": source}}, stream)
                with self.assertRaisesRegex(UnsafeCleanupError, "overlaps"):
                    cleanup_terminal_candidate_artifacts(
                        self.candidate_root, self.run_root, lambda _metadata: {"ok": True},
                    )
                self.assertTrue(os.path.isdir(workspace))

    def test_nonterminal_candidate_without_workspace_is_not_reclaimed(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "running", "source_guard": {"ok": True},
        })
        with self.assertRaises(UnsafeCleanupError):
            cleanup_terminal_candidate_artifacts(
                self.candidate_root, self.run_root, lambda _workspace: {"ok": True},
            )

    def test_terminal_candidate_without_successful_source_guard_is_not_reclaimed(self):
        self._write_json("candidate.json", {
            "candidate_id": "g000_c0000", "status": "failed", "source_guard": {"ok": False},
        })
        with self.assertRaises(UnsafeCleanupError):
            cleanup_terminal_candidate_artifacts(
                self.candidate_root, self.run_root, lambda _workspace: {"ok": True},
            )


if __name__ == "__main__":
    unittest.main()
