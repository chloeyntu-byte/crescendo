from __future__ import print_function

import copy
import hashlib
import json
import io
import os
import signal
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HARNESS_ROOT = os.path.join(
    PROJECT_ROOT, "runtime", "experiments", "whole_run_acceptance",
)
REQUIRED_HARNESS_FILES = (
    "common.py", "acceptance_fault_matrix.py", "audit_run.py",
    "fault_probes.py", "run_acceptance.py",
)
missing_harness_files = [
    name for name in REQUIRED_HARNESS_FILES
    if not os.path.isfile(os.path.join(HARNESS_ROOT, name))
]
if missing_harness_files:
    raise unittest.SkipTest(
        "optional historical acceptance harness is not present in the portable source tree"
    )
if HARNESS_ROOT not in sys.path:
    sys.path.insert(0, HARNESS_ROOT)

import common
import acceptance_fault_matrix
import audit_run as audit_harness
import fault_probes
import run_acceptance
from crescendo_engine.legacy_guardian import build_guardian_token_identity


TOKEN_A = "token-a"


class _CompletedProcess(object):
    def __init__(self, pid=42001):
        self.pid = pid
        self.returncode = None
        self._poll_count = 0

    def poll(self):
        self._poll_count += 1
        if self.returncode is None and self._poll_count >= 2:
            self.returncode = 0
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class _RunningProcess(object):
    def __init__(self, pid=42002):
        self.pid = pid
        self.returncode = None

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


def _controller_observation(identity):
    return {
        "epoch": 1.0,
        "workers": [],
        "controllers": [{"process": identity}],
    }


def _checkpoint(revision, candidate_ids):
    document = {
        "schema_version": "crescendo.checkpoint.v1",
        "candidate_layout": ["id"],
        "candidates": [[candidate_id] for candidate_id in candidate_ids],
        "parameter_ids": [], "manifest": {}, "spec": {},
        "state": {"checkpoint_revision": revision},
    }
    document["payload_sha256"] = common.checkpoint_checksum(document)
    return document


def _origin_checkpoint(run_root, continuation_id, revision=1, ancestor=None):
    document = {
        "schema_version": "crescendo.checkpoint.v1",
        "created_at": "2026-07-15T00:00:00Z",
        "manifest": {
            "maestro_path": "/portable/maestro",
            "origin_run_id": os.path.basename(run_root),
            "origin_run_root": run_root,
            "continuation_id": continuation_id,
            "algorithm": "random",
        },
        "parameter_ids": [],
        "candidate_layout": [
            "id", "generation", "index", "status", "values", "metrics",
            "failure_class", "error", "started_at", "finished_at",
        ],
        "spec": {
            "evaluator": "mock", "algorithm": "random",
            "variables": [], "objectives": [], "constraints": [],
        },
        "candidates": [],
        "state": {
            "next_generation": 0, "selection_history": [], "generator": {},
            "active_plan": None, "checkpoint_revision": int(revision),
            "completed_candidates": 0, "failed_candidates": 0,
            "infrastructure_failed_candidates": 0,
            "ancestor_payload_sha256": [ancestor] if ancestor else [],
        },
    }
    document["payload_sha256"] = common.checkpoint_checksum(document)
    return document


class TreeFingerprintTests(unittest.TestCase):
    def test_same_size_mode_and_mtime_content_change_changes_fingerprint(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "source.bin")
            with open(path, "wb") as stream:
                stream.write(b"AAAA")
            os.chmod(path, 0o640)
            original_stat = os.stat(path)
            before = common.tree_fingerprint(root)

            with open(path, "wb") as stream:
                stream.write(b"BBBB")
            os.chmod(path, original_stat.st_mode & 0o7777)
            os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
            after = common.tree_fingerprint(root)

        self.assertNotEqual(before["sha256"], after["sha256"])
        self.assertEqual(before["file_count"], after["file_count"])
        self.assertEqual(before["byte_count"], after["byte_count"])
        self.assertEqual(before["directory_count"], after["directory_count"])
        self.assertEqual(before["symlink_count"], after["symlink_count"])


class MonitoredRunSafetyTests(unittest.TestCase):
    def test_cleanup_refresh_collects_late_recorded_ocean_identity(self):
        ocean = {"pid": 51000, "start_ticks": 7000}
        owned = {"controller": [], "guardian": [], "ocean": []}

        def refresh():
            run_acceptance._merge_owned_identity_roles(owned, {
                "controllers": [],
                "workers": [{
                    "guardian": None, "guardian_recorded_identity": None,
                    "ocean": None, "ocean_recorded_identity": ocean,
                }],
            })

        with mock.patch.object(run_acceptance, "identity_alive", return_value=False):
            cleanup = run_acceptance._wait_owned_identity_roles_dead(
                owned, timeout=0.0, refresh=refresh,
            )

        self.assertTrue(cleanup["all_dead"])
        self.assertEqual(cleanup["observed_count"], 1)
        self.assertEqual(owned["ocean"], [ocean])

    def test_waits_for_observed_exact_identity_then_proves_dead(self):
        identity = {"pid": 51001, "start_ticks": 7001, "command": "controller"}
        process = _CompletedProcess()
        alive = iter((True, False))
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(run_acceptance.subprocess, "Popen", return_value=process), \
                mock.patch.object(
                    run_acceptance, "runtime_observation",
                    return_value=_controller_observation(identity),
                ), \
                mock.patch.object(
                    run_acceptance, "process_identity",
                    return_value={"pid": process.pid, "start_ticks": 6001},
                ), \
                mock.patch.object(
                    run_acceptance, "identity_alive",
                    side_effect=lambda _identity: next(alive),
                ):
            metadata = run_acceptance._run_monitored(
                ["fake-runner"], os.path.join(root, "run"), os.path.join(root, "evidence"),
                observation_interval=0.01, owned_cleanup_timeout_seconds=0.5,
            )

        self.assertTrue(metadata["owned_identity_cleanup"]["all_dead"])
        self.assertEqual(metadata["owned_identity_cleanup"]["observed_count"], 1)
        self.assertEqual(metadata["owned_identities"]["controller"], [identity])
        self.assertTrue(metadata["runner_cleanup"]["termination_proven"])

    def test_repeated_resume_audit_exempts_all_promoted_checkpoint_candidates(self):
        original = {"candidate_layout": ["id"], "candidates": [["g000_c0000"]]}
        promoted = {
            "candidate_layout": ["id"],
            "candidates": [["g000_c0000"], ["g000_c0001"]],
        }
        self.assertEqual(
            run_acceptance._promoted_restored_candidate_ids(original, promoted),
            {"g000_c0000", "g000_c0001"},
        )

    def test_repeated_resume_rejects_checkpoint_that_loses_prior_candidate(self):
        original = {"candidate_layout": ["id"], "candidates": [["g000_c0000"]]}
        promoted = {"candidate_layout": ["id"], "candidates": [["g000_c0001"]]}
        with self.assertRaises(run_acceptance.UnsafeAcceptanceError):
            run_acceptance._promoted_restored_candidate_ids(original, promoted)


class ControllerSigstopWorkerEpochPolicyTests(unittest.TestCase):
    @staticmethod
    def _termination(worker_not_created=True):
        cleanup_rows = []
        proof_rows = []
        if worker_not_created:
            cleanup_rows.append({
                "proof_kind": "worker_not_created", "ok": True, "removed": True,
            })
            proof_rows.append({"kind": "worker_not_created", "ok": True})
        return {
            "cleanup": {"ok": True, "worker_epochs": cleanup_rows},
            "worker_cleanup_proof": {"ok": True, "worker_epochs": proof_rows},
        }

    def test_one_observed_worker_epoch_requires_worker_not_created_proof(self):
        proven = run_acceptance._controller_sigstop_worker_epoch_policy(
            self._termination(), 1,
        )
        missing = run_acceptance._controller_sigstop_worker_epoch_policy(
            self._termination(worker_not_created=False), 1,
        )

        self.assertTrue(proven["ok"])
        self.assertTrue(proven["worker_not_created_proven"])
        self.assertEqual(proven["expected_live_worker_epochs"], 1)
        self.assertFalse(missing["ok"])
        self.assertEqual(missing["expected_live_worker_epochs"], 2)

    def test_two_observed_worker_epochs_require_exact_identity_count(self):
        policy = run_acceptance._controller_sigstop_worker_epoch_policy(
            self._termination(worker_not_created=False), 2,
        )

        self.assertTrue(policy["ok"])
        self.assertEqual(policy["observed_exact_identity_epochs"], 2)
        self.assertEqual(policy["expected_live_worker_epochs"], 2)

    def test_observed_epoch_count_ignores_unproven_recorded_identity(self):
        observations = {
            "controllers": [],
            "workers": [{
                "worker": "worker_000", "worker_epoch": "epoch_000",
                "ocean": {"pid": 51000, "start_ticks": 7000},
            }, {
                "worker": "worker_000", "worker_epoch": "epoch_001",
                "ocean": None,
                "ocean_recorded_identity": {"pid": 51001, "start_ticks": 7001},
            }],
        }
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as stream:
            stream.write(json.dumps(observations) + "\n")
            path = stream.name
        try:
            _controllers, workers = run_acceptance._observed_epoch_counts(path)
        finally:
            os.unlink(path)

        self.assertEqual(workers, 1)

    def test_unexpected_worker_epoch_count_fails_closed(self):
        for observed in (0, 3):
            with self.subTest(observed=observed):
                policy = run_acceptance._controller_sigstop_worker_epoch_policy(
                    self._termination(), observed,
                )
                self.assertFalse(policy["ok"])
                self.assertEqual(policy["expected_live_worker_epochs"], 2)


class FaultProbeIdentityTests(unittest.TestCase):
    def _worker_fixture(self, root, state_updates=None, symlink=False):
        control_root = os.path.join(root, "control")
        workspace_root = os.path.join(root, "workspace")
        os.makedirs(control_root)
        os.makedirs(workspace_root)
        state = {
            "token": TOKEN_A, "cwd": workspace_root,
            "guardian_pid": 51010, "guardian_start_ticks": 7010,
            "pid": 51011, "pid_start_ticks": 7011,
            "pgid": 51011, "sid": 51011, "status": "running",
        }
        state.update(state_updates or {})
        state_path = os.path.join(control_root, "guardian_state.json")
        if symlink:
            target = os.path.join(root, "outside-state.json")
            common.write_json(target, state)
            os.symlink(target, state_path)
        else:
            common.write_json(state_path, state)

        class Process(object):
            pid = 51010

            @staticmethod
            def poll():
                return None

        controller = type("Controller", (object,), {})()
        controller.process = Process()
        controller.guardian_state_path = state_path
        controller.guardian_token = TOKEN_A
        controller.control_root = control_root
        worker = type("Worker", (object,), {})()
        worker.controller = controller
        worker.workspace = {"root": workspace_root}
        return worker

    @staticmethod
    def _identity(pid):
        if pid == 51010:
            return {"pid": pid, "start_ticks": 7010, "state": "S", "ppid": 50000,
                    "pgid": 51010, "sid": 51010}
        if pid == 51011:
            return {"pid": pid, "start_ticks": 7011, "state": "S", "ppid": 51010,
                    "pgid": 51011, "sid": 51011}
        return None

    def test_active_worker_identities_require_exact_guardian_state(self):
        with tempfile.TemporaryDirectory() as root:
            worker = self._worker_fixture(root)
            with mock.patch.object(fault_probes, "process_identity", side_effect=self._identity):
                guardian, ocean = fault_probes._active_worker_identities(worker, timeout=0.0)

        self.assertEqual(guardian["pid"], 51010)
        self.assertEqual(ocean["pid"], 51011)

    def test_active_worker_identities_reject_token_mismatch_and_symlink(self):
        for state_updates, symlink in (({"token": "wrong"}, False), ({}, True)):
            with self.subTest(state_updates=state_updates, symlink=symlink), \
                    tempfile.TemporaryDirectory() as root:
                worker = self._worker_fixture(root, state_updates, symlink=symlink)
                with mock.patch.object(fault_probes, "process_identity", side_effect=self._identity):
                    with self.assertRaises(RuntimeError):
                        fault_probes._active_worker_identities(worker, timeout=0.0)

    def test_timeout_probe_closes_worker_when_identity_proof_fails(self):
        class Worker(object):
            def __init__(self, *_args, **_kwargs):
                self.controller = object()
                self.epoch_root = "/tmp/epoch"

            @staticmethod
            def ensure_started():
                return {"epoch": "epoch-a"}

        closed = []

        def close(worker):
            closed.append(worker)
            worker.controller = None
            worker.epoch_root = None
            return {"ok": True}

        spec = {
            "maestro_path": "/tmp/fake-maestro.sdb", "max_depth": 1,
            "variables": [{"parameter_id": "parameter-a", "minimum": 1, "maximum": 2}],
        }
        prepared = {
            "parameters": [{"parameter_id": "parameter-a", "optimizable": True}],
        }
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch.object(fault_probes, "tree_fingerprint", return_value={"sha256": "x"}), \
                mock.patch.object(fault_probes, "load_config", return_value={"runtime": {}}), \
                mock.patch.object(fault_probes, "_normalized_spec", return_value=spec), \
                mock.patch.object(fault_probes, "prepare_maestro", return_value=prepared), \
                mock.patch.object(fault_probes, "_WholeRunPersistentWorker", Worker), \
                mock.patch.object(
                    fault_probes, "_active_worker_identities",
                    side_effect=RuntimeError("identity proof failed"),
                ), \
                mock.patch.object(fault_probes, "_close", side_effect=close):
            with self.assertRaises(RuntimeError):
                fault_probes.run_timeout(
                    "/tmp/config.json", "/tmp/spec.json", os.path.join(root, "run"),
                )

        self.assertEqual(len(closed), 1)


class ExactFaultGateTests(unittest.TestCase):
    @staticmethod
    def _spectre_install(root):
        spectre_root = os.path.join(root, "spectre-install")
        wrapper_root = os.path.join(spectre_root, "bin")
        actual_root = os.path.join(
            spectre_root, "tools.lnx86", "spectre", "bin", "64bit",
        )
        os.makedirs(wrapper_root)
        os.makedirs(actual_root)
        wrapper_target = os.path.join(wrapper_root, ".cdnWrapperIndep")
        with open(wrapper_target, "w") as stream:
            stream.write("#!/bin/sh\n")
        os.chmod(wrapper_target, 0o755)
        wrapper = os.path.join(wrapper_root, "spectre")
        os.symlink(".cdnWrapperIndep", wrapper)
        actual = os.path.join(actual_root, "spectre")
        with open(actual, "wb") as stream:
            stream.write(b"\x7fELF")
        os.chmod(actual, 0o755)
        return spectre_root, wrapper, actual

    def _controller_fixture(self, root):
        run_root = os.path.join(root, "run")
        epoch_root = os.path.join(
            run_root, "controllers", "slot_000", "epoch_000000",
        )
        requests = os.path.join(epoch_root, "requests")
        responses = os.path.join(epoch_root, "responses")
        os.makedirs(requests)
        os.makedirs(responses)
        identity = {
            "schema_version": "crescendo.persistent_controller.v1", "token": TOKEN_A,
            "controller_epoch": 0, "sequence": 1, "nonce": "nonce-a",
            "operation": "EVALUATE",
        }
        request_name = "00000001_nonce-a.json"
        response_path = os.path.join(responses, request_name)
        request = dict(identity)
        request.update({
            "payload": {"generation": 0, "index": 0, "values": {}, "spec": {}},
            "response_path": response_path,
        })
        common.write_json(os.path.join(requests, request_name), request)
        bootstrap = {
            "schema_version": "crescendo.persistent_controller.v1", "token": TOKEN_A,
            "run_root": run_root, "requests_root": requests,
            "responses_root": responses,
            "state_path": os.path.join(epoch_root, "controller_state.json"),
        }
        common.write_json(os.path.join(epoch_root, "bootstrap.json"), bootstrap)
        state = {
            "schema_version": "crescendo.persistent_controller.v1", "token": TOKEN_A,
            "status": "processing", "pid": 2200, "pgid": 2200,
            "pid_start_ticks": 22, "parent_pid": 1100, "parent_start_ticks": 11,
            "run_root": run_root, "active_request": identity,
        }
        common.write_json(os.path.join(epoch_root, "controller_state.json"), state)
        processes = {
            1100: {"pid": 1100, "start_ticks": 11, "state": "S", "ppid": 1,
                   "pgid": 1100, "sid": 1100, "executable": "/usr/bin/python"},
            2200: {"pid": 2200, "start_ticks": 22, "state": "S", "ppid": 1100,
                   "pgid": 2200, "sid": 2200, "executable": "/usr/bin/python"},
        }
        controller = {
            "epoch_root": epoch_root, "status": "processing",
            "active_request": identity, "identity_matches": True,
            "recorded_pid": 2200, "recorded_pgid": 2200,
            "recorded_start_ticks": 22, "process": dict(processes[2200]),
        }
        return run_root, controller, processes, request

    def _guardian_target_fixture(self, root):
        run_root = os.path.join(root, "run")
        epoch_root = os.path.join(run_root, "workers", "worker_000", "epoch_000")
        workspace = os.path.join(epoch_root, "workspace")
        control = os.path.join(epoch_root, "epochs", "worker-epoch", "control")
        os.makedirs(workspace)
        os.makedirs(control)
        guardian_state_path = os.path.join(control, "guardian_state.json")
        manifest_path = os.path.join(
            control, run_acceptance.GUARDIAN_LAUNCH_MANIFEST_NAME,
        )
        launch_lock_path = manifest_path + ".launch_lock.tmp"
        guardian_script = os.path.join(PROJECT_ROOT, "tools", "ocean_worker_guardian.py")
        token_identity = build_guardian_token_identity(
            "a" * 32, "worker-epoch", 0,
        )
        guardian_argv = [
            sys.executable, guardian_script,
            "--state-path", guardian_state_path,
            "--token", token_identity["token"],
            "--cwd", workspace,
        ]
        controller = {
            "target": {
                "pid": 2200, "start_ticks": 22, "state": "S", "ppid": 1100,
                "pgid": 2200, "sid": 2200, "executable": "/usr/bin/python",
            },
            "candidate_id": "g000_c0000",
        }
        guardian = {
            "pid": 3300, "start_ticks": 33, "state": "S", "ppid": 2200,
            "pgid": 3300, "sid": 3300, "executable": sys.executable,
        }
        guardian_state = {
            "token": token_identity["token"], "guardian_pid": 3300,
            "guardian_start_ticks": 33, "cwd": workspace,
            "pid": 4400, "pgid": 4400, "sid": 4400,
            "pid_start_ticks": 44,
        }
        worker_state = {
            "schema_version": "crescendo.persistent_maestro_worker.v1",
            "worker_index": 0, "epoch": "worker-epoch", "state": "busy",
            "workspace_root": workspace, "control_root": control,
            "guardian_state_path": guardian_state_path,
            "guardian_launch_manifest_path": manifest_path,
            "guardian_launch_lock_path": launch_lock_path,
            "guardian_token": token_identity["token"],
            "guardian_state_token": token_identity["token"],
            "guardian_token_identity": token_identity,
            "guardian_expected_uid": os.geteuid(),
            "guardian_parent_pid": 2200, "guardian_parent_start_ticks": 22,
            "guardian_parent_boot_id": "boot-a",
            "pid": 3300, "pgid": 3300, "guardian_start_ticks": 33,
        }
        manifest = {
            "schema_version": run_acceptance.GUARDIAN_LAUNCH_MANIFEST_SCHEMA,
            "attempt_root": control, "workspace_root": workspace,
            "state_path": guardian_state_path,
            "launch_lock_path": launch_lock_path,
            "guardian_script": guardian_script,
            "token": token_identity["token"],
            "guardian_token_identity": token_identity,
            "expected_parent_pid": 2200, "expected_parent_start_ticks": 22,
            "expected_parent_boot_id": "boot-a", "expected_uid": os.geteuid(),
            "guardian_argv": guardian_argv,
        }
        common.write_json(manifest_path, manifest)
        worker_proof = {
            "worker": {
                "worker": "worker_000", "worker_epoch": "epoch_000",
                "epoch_root": epoch_root, "workspace_root": workspace,
            },
            "worker_state": worker_state, "guardian_state": guardian_state,
            "guardian": guardian, "workspace_root": workspace,
            "candidate_id": "g000_c0000",
        }
        proc_details = {
            "cwd": workspace, "argv": guardian_argv,
            "launch_token": token_identity["token"], "uid": os.geteuid(),
        }
        processes = {2200: controller["target"], 3300: guardian}
        return {
            "run_root": run_root, "controller": controller,
            "worker": worker_proof, "processes": processes,
            "proc_details": proc_details, "manifest_path": manifest_path,
        }

    def test_controller_phase_gate_binds_parent_token_request_and_phase(self):
        with tempfile.TemporaryDirectory() as root:
            run_root, controller, processes, request = self._controller_fixture(root)
            proof = acceptance_fault_matrix.prove_controller_phase(
                run_root, controller, "EVALUATE",
                process_reader=lambda pid: processes.get(pid),
            )
            self.assertEqual(proof["candidate_id"], "g000_c0000")
            request["token"] = "wrong-token"
            common.write_json(proof["request_path"], request)
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_controller_phase(
                    run_root, controller, "EVALUATE",
                    process_reader=lambda pid: processes.get(pid),
                )

    def test_spectre_wrapper_resolves_to_bounded_actual_executable(self):
        with tempfile.TemporaryDirectory() as root:
            spectre_root, wrapper, actual = self._spectre_install(root)
            workspace = os.path.join(root, "owned-workspace")
            os.makedirs(workspace)
            processes = {
                3000: {"pid": 3000, "start_ticks": 30, "state": "S", "ppid": 2500,
                       "pgid": 3000, "sid": 3000, "executable": "/opt/eda/virtuoso"},
                4000: {"pid": 4000, "start_ticks": 40, "state": "S", "ppid": 3000,
                       "pgid": 4000, "sid": 3000, "executable": actual},
                2500: {"pid": 2500, "start_ticks": 25, "state": "S", "ppid": 1,
                       "pgid": 2500, "sid": 2500, "executable": "/usr/bin/python"},
            }
            worker = {"workspace_root": workspace, "ocean": processes[3000]}
            proof = acceptance_fault_matrix.prove_spectre_target(
                worker, wrapper, spectre_root,
                process_reader=lambda pid: processes.get(pid),
                cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000"],
            )
            self.assertEqual(proof["target"]["pid"], 4000)
            self.assertEqual(proof["spectre_binary"], os.path.realpath(actual))
            self.assertIn(os.path.realpath(actual), proof["allowed_spectre_binaries"])

            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, wrapper, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(root, "outside-workspace"),
                    uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000"],
                )
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, wrapper, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                    uid_reader=lambda _pid: os.geteuid() + 1, proc_entries=["4000"],
                )
            processes[4000] = dict(processes[4000], ppid=2500)
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, wrapper, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                    uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000"],
                )

    def test_guardian_target_locks_proc_and_scoped_launch_identity(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._guardian_target_fixture(root)
            proof = run_acceptance._prove_exact_guardian_target(
                fixture["worker"], fixture["controller"],
                process_reader=lambda pid: fixture["processes"].get(pid),
                proc_reader=lambda _pid: dict(fixture["proc_details"]),
            )
            target = proof["target"]
            self.assertEqual(
                (target["pid"], target["start_ticks"], target["pgid"], target["sid"]),
                (3300, 33, 3300, 3300),
            )
            self.assertEqual(target["cwd"], fixture["proc_details"]["cwd"])
            self.assertEqual(target["argv"], fixture["proc_details"]["argv"])
            self.assertEqual(
                target["launch_token"], fixture["proc_details"]["launch_token"],
            )
            self.assertEqual(proof["guardian_manifest_path"], fixture["manifest_path"])
            self.assertEqual(len(proof["guardian_manifest_sha256"]), 64)

    def test_guardian_target_rejects_proc_cwd_argv_token_or_uid_mismatch(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._guardian_target_fixture(root)
            mutations = {
                "cwd": os.path.join(root, "outside"),
                "argv": ["/wrong/guardian"],
                "launch_token": "crescendo2-" + ("0" * 64),
                "uid": os.geteuid() + 1,
            }
            for field, value in mutations.items():
                with self.subTest(field=field):
                    details = dict(fixture["proc_details"])
                    details[field] = value
                    with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                        run_acceptance._prove_exact_guardian_target(
                            fixture["worker"], fixture["controller"],
                            process_reader=lambda pid: fixture["processes"].get(pid),
                            proc_reader=lambda _pid, current=details: dict(current),
                        )

    def test_guardian_target_rejects_identity_change_during_proc_proof(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._guardian_target_fixture(root)
            reads = [
                dict(fixture["processes"][3300]),
                dict(fixture["processes"][3300], start_ticks=34),
            ]

            def process_reader(_pid):
                return reads.pop(0)

            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                run_acceptance._prove_exact_guardian_target(
                    fixture["worker"], fixture["controller"],
                    process_reader=process_reader,
                    proc_reader=lambda _pid: dict(fixture["proc_details"]),
                )

    def test_guardian_target_rejects_invalid_scoped_manifest_token(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._guardian_target_fixture(root)
            manifest = common.load_json(fixture["manifest_path"])
            manifest["guardian_token_identity"]["scope_token"] = "b" * 32
            common.write_json(fixture["manifest_path"], manifest)
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                run_acceptance._prove_exact_guardian_target(
                    fixture["worker"], fixture["controller"],
                    process_reader=lambda pid: fixture["processes"].get(pid),
                    proc_reader=lambda _pid: dict(fixture["proc_details"]),
                )

    def test_guardian_target_rejects_manifest_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._guardian_target_fixture(root)
            replacement = fixture["manifest_path"] + ".replacement"
            os.rename(fixture["manifest_path"], replacement)
            os.symlink(replacement, fixture["manifest_path"])
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                run_acceptance._prove_exact_guardian_target(
                    fixture["worker"], fixture["controller"],
                    process_reader=lambda pid: fixture["processes"].get(pid),
                    proc_reader=lambda _pid: dict(fixture["proc_details"]),
                )

    def test_guardian_exact_lock_rejects_any_change_after_sigstop(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._guardian_target_fixture(root)
            proof = run_acceptance._prove_exact_guardian_target(
                fixture["worker"], fixture["controller"],
                process_reader=lambda pid: fixture["processes"].get(pid),
                proc_reader=lambda _pid: dict(fixture["proc_details"]),
            )
            target_mutations = {
                "pid": 3301, "start_ticks": 34, "pgid": 3301, "sid": 3301,
                "cwd": os.path.join(root, "other"), "argv": ["/changed"],
                "launch_token": "changed", "uid": os.geteuid() + 1,
            }
            for field, value in target_mutations.items():
                with self.subTest(field=field):
                    verified = copy.deepcopy(proof)
                    verified["target"][field] = value
                    with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                        run_acceptance._verify_reverified_fault_lock(
                            proof, verified, "guardian",
                        )
            verified = copy.deepcopy(proof)
            verified["guardian_manifest_sha256"] = "0" * 64
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                run_acceptance._verify_reverified_fault_lock(
                    proof, verified, "guardian",
                )

    def test_exact_fault_proof_dispatches_guardian_evaluate(self):
        observation = {"controllers": [{
            "status": "processing", "identity_matches": True,
            "active_request": {"operation": "EVALUATE"},
        }]}
        controller = {"target": {"pid": 2200}, "candidate_id": "g000_c0000"}
        worker = {"candidate_id": "g000_c0000"}
        guardian = {"target": {"pid": 3300}, "controller": controller}
        with mock.patch.object(
                run_acceptance, "prove_controller_phase", return_value=controller), \
                mock.patch.object(
                    run_acceptance, "prove_worker_evaluate", return_value=worker,
                ), mock.patch.object(
                    run_acceptance, "_prove_exact_guardian_target",
                    return_value=guardian,
                ) as prove_guardian:
            proof = run_acceptance._exact_fault_proof(
                "/owned/run", observation, "guardian", "EVALUATE",
            )
        self.assertIs(proof, guardian)
        prove_guardian.assert_called_once_with(worker, controller)

    def test_spectre_target_rejects_executable_outside_discovered_root(self):
        with tempfile.TemporaryDirectory() as root:
            spectre_root, wrapper, _actual = self._spectre_install(root)
            workspace = os.path.join(root, "owned-workspace")
            os.makedirs(workspace)
            outside = os.path.join(root, "outside", "spectre")
            os.makedirs(os.path.dirname(outside))
            with open(outside, "wb") as stream:
                stream.write(b"\x7fELF")
            os.chmod(outside, 0o755)
            unapproved = os.path.join(spectre_root, "arbitrary", "spectre")
            os.makedirs(os.path.dirname(unapproved))
            with open(unapproved, "wb") as stream:
                stream.write(b"\x7fELF")
            os.chmod(unapproved, 0o755)
            processes = {
                3000: {"pid": 3000, "start_ticks": 30, "state": "S", "ppid": 2500,
                       "pgid": 3000, "sid": 3000, "executable": "/opt/eda/virtuoso"},
                4000: {"pid": 4000, "start_ticks": 40, "state": "S", "ppid": 3000,
                       "pgid": 4000, "sid": 3000, "executable": outside},
                2500: {"pid": 2500, "start_ticks": 25, "state": "S", "ppid": 1,
                       "pgid": 2500, "sid": 2500, "executable": "/usr/bin/python"},
            }
            worker = {"workspace_root": workspace, "ocean": processes[3000]}
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, wrapper, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                    uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000"],
                )
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, outside, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                    uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000"],
                )
            processes[4000] = dict(processes[4000], executable=unapproved)
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, wrapper, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                    uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000"],
                )

    def test_spectre_target_rejects_multiple_exact_owned_candidates(self):
        with tempfile.TemporaryDirectory() as root:
            spectre_root, wrapper, actual = self._spectre_install(root)
            workspace = os.path.join(root, "owned-workspace")
            os.makedirs(workspace)
            processes = {
                3000: {"pid": 3000, "start_ticks": 30, "state": "S", "ppid": 2500,
                       "pgid": 3000, "sid": 3000, "executable": "/opt/eda/virtuoso"},
                4000: {"pid": 4000, "start_ticks": 40, "state": "S", "ppid": 3000,
                       "pgid": 4000, "sid": 3000, "executable": actual},
                4001: {"pid": 4001, "start_ticks": 41, "state": "S", "ppid": 3000,
                       "pgid": 4001, "sid": 3000, "executable": actual},
                2500: {"pid": 2500, "start_ticks": 25, "state": "S", "ppid": 1,
                       "pgid": 2500, "sid": 2500, "executable": "/usr/bin/python"},
            }
            worker = {"workspace_root": workspace, "ocean": processes[3000]}
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_spectre_target(
                    worker, wrapper, spectre_root,
                    process_reader=lambda pid: processes.get(pid),
                    cwd_reader=lambda _pid: os.path.join(workspace, "netlist"),
                    uid_reader=lambda _pid: os.geteuid(), proc_entries=["4000", "4001"],
                )

    def test_worker_evaluate_gate_requires_busy_and_patched_marker(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run")
            epoch_root = os.path.join(run_root, "workers", "worker_000", "epoch_000")
            workspace = os.path.join(epoch_root, "workspace")
            control = os.path.join(epoch_root, "control")
            candidate_root = os.path.join(run_root, "candidates", "g000_c0000")
            os.makedirs(workspace)
            os.makedirs(control)
            os.makedirs(candidate_root)
            marker_path = os.path.join(candidate_root, "persistent_worker_status.tsv")
            with open(marker_path, "w") as stream:
                stream.write("worker-epoch\t1\tnonce\tg000_c0000\tPATCHED\tnow\t\n")
            guardian_path = os.path.join(control, "guardian_state.json")
            common.write_json(guardian_path, {
                "token": "guardian-token", "guardian_pid": 3300,
                "guardian_start_ticks": 33, "cwd": workspace,
                "pid": 4400, "pgid": 4400, "sid": 4400,
                "pid_start_ticks": 44,
            })
            worker_state_path = os.path.join(epoch_root, "worker_state.json")
            worker_state = {
                "state": "busy", "workspace_root": workspace,
                "guardian_state_path": guardian_path,
                "guardian_token": "guardian-token", "pid": 3300,
                "pgid": 3300, "guardian_start_ticks": 33,
                "active_identity": {
                    "epoch": "worker-epoch", "sequence": 1, "nonce": "nonce",
                    "candidate_id": "g000_c0000",
                },
            }
            common.write_json(worker_state_path, worker_state)
            processes = {
                2200: {"pid": 2200, "start_ticks": 22, "state": "S", "ppid": 1,
                       "pgid": 2200, "sid": 2200, "executable": "/usr/bin/python"},
                3300: {"pid": 3300, "start_ticks": 33, "state": "S", "ppid": 2200,
                       "pgid": 3300, "sid": 3300, "executable": "/usr/bin/python"},
                4400: {"pid": 4400, "start_ticks": 44, "state": "S", "ppid": 3300,
                       "pgid": 4400, "sid": 4400, "executable": "/opt/eda/virtuoso"},
            }
            observation = {"workers": [{
                "epoch_root": epoch_root, "workspace_root": workspace,
                "guardian_identity_matches": True, "ocean_identity_matches": True,
                "guardian": processes[3300], "ocean": processes[4400],
            }]}
            controller_proof = {
                "target": processes[2200], "candidate_id": "g000_c0000",
            }
            proof = acceptance_fault_matrix.prove_worker_evaluate(
                run_root, observation, controller_proof,
                process_reader=lambda pid: processes.get(pid),
            )
            self.assertEqual(proof["marker_rows"][-1]["state"], "PATCHED")
            worker_state["state"] = "ready"
            common.write_json(worker_state_path, worker_state)
            with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                acceptance_fault_matrix.prove_worker_evaluate(
                    run_root, observation, controller_proof,
                    process_reader=lambda pid: processes.get(pid),
                )

    def test_sigstop_reverify_then_sigkill_targets_only_exact_pid(self):
        processes = {
            5000: {"pid": 5000, "start_ticks": 50, "state": "R", "ppid": 1,
                   "pgid": 5000, "sid": 5000, "executable": "/bin/fault-target"},
        }
        signals = []

        def send(pid, value):
            signals.append((pid, value))
            if value == signal.SIGSTOP:
                processes[pid] = dict(processes[pid], state="T")
            elif value == signal.SIGCONT:
                processes[pid] = dict(processes[pid], state="R")
            elif value == signal.SIGKILL:
                processes.pop(pid, None)

        report = acceptance_fault_matrix.stop_reverify_kill(
            {"target": dict(processes[5000])},
            lambda: {"target": dict(processes[5000])},
            signal_sender=send, process_reader=lambda pid: processes.get(pid),
            sleep=lambda _seconds: None,
        )
        self.assertEqual(signals, [(5000, signal.SIGSTOP), (5000, signal.SIGKILL)])
        self.assertTrue(report["death_proven"])

    def test_sigstop_reverify_hold_leaves_exact_target_stopped(self):
        processes = {
            5001: {"pid": 5001, "start_ticks": 51, "state": "R", "ppid": 1,
                   "pgid": 5001, "sid": 5001, "executable": "/bin/fault-target"},
        }
        signals = []

        def send(pid, value):
            signals.append((pid, value))
            if value == signal.SIGSTOP:
                processes[pid] = dict(processes[pid], state="T")
            elif value == signal.SIGCONT:
                processes[pid] = dict(processes[pid], state="R")

        report = acceptance_fault_matrix.stop_reverify_hold(
            {"target": dict(processes[5001])},
            lambda: {"target": dict(processes[5001])},
            signal_sender=send, process_reader=lambda pid: processes.get(pid),
            sleep=lambda _seconds: None,
        )
        self.assertEqual(signals, [(5001, signal.SIGSTOP)])
        self.assertEqual(processes[5001]["state"], "T")
        self.assertTrue(report["stop_proven"])

    def test_sigstop_hold_reverify_failure_resumes_exact_target(self):
        processes = {
            5002: {"pid": 5002, "start_ticks": 52, "state": "R", "ppid": 1,
                   "pgid": 5002, "sid": 5002, "executable": "/bin/fault-target"},
        }
        signals = []

        def send(pid, value):
            signals.append((pid, value))
            if value == signal.SIGSTOP:
                processes[pid] = dict(processes[pid], state="T")
            elif value == signal.SIGCONT:
                processes[pid] = dict(processes[pid], state="R")

        with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
            acceptance_fault_matrix.stop_reverify_hold(
                {"target": dict(processes[5002])},
                lambda: {"target": dict(processes[5002], start_ticks=999)},
                signal_sender=send, process_reader=lambda pid: processes.get(pid),
                sleep=lambda _seconds: None,
            )
        self.assertEqual(signals, [
            (5002, signal.SIGSTOP), (5002, signal.SIGCONT),
        ])
        self.assertEqual(processes[5002]["state"], "R")


class OwnedProcessSigkillPolicyTests(unittest.TestCase):
    def test_guardian_sigkill_case_is_registered_in_extended_suite(self):
        definition = run_acceptance.CASES["guardian_sigkill_evaluate"]
        self.assertEqual(definition["kind"], "owned_process_sigkill")
        self.assertEqual(definition["role"], "guardian")
        self.assertEqual(definition["phase"], "EVALUATE")
        self.assertEqual(definition["expected_infrastructure"], 1)
        self.assertEqual(
            definition["allowed_infrastructure_failure_classes"], ["infrastructure"],
        )
        self.assertIn("guardian_sigkill_evaluate", run_acceptance.RELEASE_EXTENDED_ORDER)

    def test_owned_fault_audit_policy_counts_explicit_guardian_replacement(self):
        guardian = run_acceptance._owned_fault_audit_policy(
            run_acceptance.CASES["guardian_sigkill_evaluate"]
        )
        ocean = run_acceptance._owned_fault_audit_policy(
            run_acceptance.CASES["ocean_sigkill_evaluate"]
        )

        self.assertEqual(guardian["expected_total"], 5)
        self.assertEqual(guardian["expected_completed"], 4)
        self.assertEqual(guardian["expected_infrastructure"], 1)
        self.assertEqual(
            guardian["allowed_infrastructure_failure_classes"], ("infrastructure",),
        )
        self.assertTrue(guardian["allow_replacement_layout"])
        self.assertEqual(ocean["expected_total"], 4)
        self.assertEqual(ocean["expected_infrastructure"], 0)
        self.assertFalse(ocean["allow_replacement_layout"])

    def test_guardian_requires_unchanged_controller_and_one_worker_replacement(self):
        metadata = {
            "returncode": 0, "watchdog_timeout": False, "monitor_error": None,
        }
        passing = run_acceptance._owned_sigkill_recovery_checks(
            "guardian", "EVALUATE", metadata, None, 1, 2,
        )
        self.assertTrue(all(passing.values()))
        for controller_epochs, worker_epochs in ((2, 2), (1, 1), (1, 3)):
            with self.subTest(
                    controller_epochs=controller_epochs, worker_epochs=worker_epochs):
                failing = run_acceptance._owned_sigkill_recovery_checks(
                    "guardian", "EVALUATE", metadata, None,
                    controller_epochs, worker_epochs,
                )
                self.assertFalse(all(failing.values()))

    def test_watchdog_always_fails_owned_sigkill_policy(self):
        checks = run_acceptance._owned_sigkill_recovery_checks(
            "ocean", "EVALUATE",
            {"returncode": 0, "watchdog_timeout": True, "monitor_error": None},
            None, 1, 2,
        )
        self.assertFalse(checks["suite_watchdog_not_triggered"])
        self.assertFalse(all(checks.values()))

    def test_commit_requires_origin_run_and_exactly_two_epochs(self):
        metadata = {
            "returncode": 0, "watchdog_timeout": False, "monitor_error": None,
        }
        passing = run_acceptance._owned_sigkill_recovery_checks(
            "controller", "COMMIT", metadata, None, 2, 2,
        )
        self.assertTrue(all(passing.values()))
        for controller_epochs, worker_epochs in ((1, 1), (3, 3)):
            with self.subTest(
                    controller_epochs=controller_epochs,
                    worker_epochs=worker_epochs):
                failing = run_acceptance._owned_sigkill_recovery_checks(
                    "controller", "COMMIT", metadata, None,
                    controller_epochs, worker_epochs,
                )
                self.assertFalse(all(failing.values()))
        resumed = run_acceptance._owned_sigkill_recovery_checks(
            "controller", "COMMIT", metadata, {"completed": True}, 2, 2,
        )
        self.assertFalse(resumed["commit_recovered_in_origin_run"])

    def test_commit_worker_proof_binds_guardian_and_ocean_to_controller(self):
        with tempfile.TemporaryDirectory() as run_root:
            epoch_root = os.path.join(run_root, "workers", "worker_000", "epoch_000")
            workspace_root = os.path.join(epoch_root, "workspace")
            os.makedirs(workspace_root)
            processes = {
                2200: {"pid": 2200, "start_ticks": 22, "state": "S", "ppid": 1,
                       "pgid": 2200, "sid": 2200},
                3300: {"pid": 3300, "start_ticks": 33, "state": "S", "ppid": 2200,
                       "pgid": 3300, "sid": 3300},
                4400: {"pid": 4400, "start_ticks": 44, "state": "S", "ppid": 3300,
                       "pgid": 4400, "sid": 4400},
            }
            observation = {"workers": [{
                "epoch_root": epoch_root, "workspace_root": workspace_root,
                "guardian_identity_matches": True, "ocean_identity_matches": True,
                "guardian": dict(processes[3300]), "ocean": dict(processes[4400]),
            }]}
            controller = {"target": dict(processes[2200])}
            with mock.patch.object(
                    run_acceptance, "process_identity",
                    side_effect=lambda pid: processes.get(pid)):
                proof = run_acceptance._exact_live_worker_for_controller(
                    run_root, observation, controller,
                )
                self.assertEqual(proof["guardian"]["ppid"], 2200)
                processes[3300] = dict(processes[3300], ppid=9999)
                with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                    run_acceptance._exact_live_worker_for_controller(
                        run_root, observation, controller,
                    )


class CombinedFaultCheckpointResumeSafetyTests(unittest.TestCase):
    @staticmethod
    def _proof(controller_epoch, worker_epoch, parent=None):
        parent = parent or {
            "pid": 1100, "start_ticks": 11, "state": "S", "ppid": 1,
            "pgid": 1100, "sid": 1100, "executable": "/usr/bin/python",
        }
        controller = {
            "bootstrap": {"controller_epoch": controller_epoch},
            "parent": dict(parent),
            "target": {
                "pid": 2200 + controller_epoch,
                "start_ticks": 22 + controller_epoch,
                "state": "S", "ppid": parent["pid"],
                "pgid": 2200 + controller_epoch,
                "sid": 2200 + controller_epoch,
                "executable": "/usr/bin/python",
            },
        }
        return {
            "controller": controller,
            "worker": {"worker": {
                "worker": "worker_000", "worker_epoch": worker_epoch,
                "epoch_root": "/owned/workers/worker_000/%s" % worker_epoch,
            }},
        }

    def test_epoch_contract_requires_legacy_to_single_namespaced_replacement(self):
        report = run_acceptance._combined_fault_epoch_contract(
            self._proof(0, "epoch_000"),
            self._proof(1, "epoch_c000001_l000000"),
        )
        self.assertTrue(report["ok"])
        self.assertEqual(report["replacement"]["namespaced_controller_epoch"], 1)
        self.assertEqual(report["replacement"]["namespaced_local_epoch"], 0)

        invalid = (
            (self._proof(0, "epoch_000"), self._proof(1, "epoch_001")),
            (self._proof(0, "epoch_000"), self._proof(2, "epoch_c000002_l000000")),
            (self._proof(0, "epoch_001"), self._proof(1, "epoch_c000001_l000000")),
            (self._proof(0, "epoch_000"), self._proof(1, "epoch_c000002_l000000")),
            (self._proof(0, "epoch_000"), self._proof(1, "epoch_c000001_l000001")),
        )
        for first, replacement in invalid:
            with self.subTest(
                    first=first["worker"]["worker"]["worker_epoch"],
                    replacement=replacement["worker"]["worker"]["worker_epoch"]):
                self.assertFalse(run_acceptance._combined_fault_epoch_contract(
                    first, replacement,
                )["ok"])

    def test_top_level_fault_target_requires_launch_identity_and_parent_binding(self):
        expected = {
            "pid": 1100, "start_ticks": 11, "state": "S", "ppid": 1,
            "pgid": 1100, "sid": 1100, "executable": "/usr/bin/python",
        }
        proof = self._proof(1, "epoch_c000001_l000000", parent=expected)["controller"]
        report = run_acceptance._prove_top_level_parent_target(
            expected, proof, process_reader=lambda pid: dict(expected) if pid == 1100 else None,
        )
        self.assertEqual(report["target"]["pid"], 1100)

        reused = dict(expected, start_ticks=99)
        with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
            run_acceptance._prove_top_level_parent_target(
                expected, proof,
                process_reader=lambda pid: reused if pid == 1100 else None,
            )
        wrong_parent = dict(proof)
        wrong_parent["parent"] = dict(expected, pid=1101)
        with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
            run_acceptance._prove_top_level_parent_target(
                expected, wrong_parent,
                process_reader=lambda pid: dict(expected) if pid == 1100 else None,
            )

    def test_checkpoint_lineage_requires_forward_revision_and_exact_extension(self):
        first = _checkpoint(1, ["g000_c0000"])
        crash = _checkpoint(2, ["g000_c0000", "g000_c0001"])
        final = _checkpoint(4, [
            "g000_c0000", "g000_c0001", "g000_c0002", "g000_c0003",
        ])
        report = run_acceptance._combined_checkpoint_lineage_checks(
            first, crash, final, 4,
        )
        self.assertTrue(report["ok"])
        self.assertEqual(report["revisions"], {
            "first_fault": 1, "crash": 2, "final": 4,
        })

        cases = (
            (first, _checkpoint(1, ["g000_c0000"]), final),
            (first, crash, _checkpoint(2, [
                "g000_c0000", "g000_c0001", "g000_c0002", "g000_c0003",
            ])),
            (first, crash, _checkpoint(4, [
                "g000_c0001", "g000_c0002", "g000_c0003", "g000_c0004",
            ])),
            (first, crash, _checkpoint(4, [
                "g000_c0000", "g000_c0001", "g000_c0001", "g000_c0003",
            ])),
        )
        for candidate_first, candidate_crash, candidate_final in cases:
            with self.subTest(
                    crash_revision=candidate_crash["state"]["checkpoint_revision"],
                    final_revision=candidate_final["state"]["checkpoint_revision"]):
                self.assertFalse(run_acceptance._combined_checkpoint_lineage_checks(
                    candidate_first, candidate_crash, candidate_final, 4,
                )["ok"])


class ServerGuardianOrphanResumeSafetyTests(unittest.TestCase):
    def test_worker_guardian_recovery_reads_preflight_and_legacy_locations(self):
        preflight_row = {"worker_epoch": "worker_000/epoch_000"}
        legacy_row = {"worker_epoch": "worker_000/epoch_001"}
        cleanup = {
            "worker_guardian_recovery": [legacy_row],
            "preflight": {"worker_guardian_recovery": [preflight_row]},
        }

        self.assertEqual(
            run_acceptance._worker_guardian_recovery_rows(cleanup),
            [legacy_row, preflight_row],
        )

    def test_case_is_nondefault_extended_and_uses_short_real_opa_spec(self):
        definition = run_acceptance.CASES["server_guardian_orphan_resume"]
        spec = common.load_json(definition["spec"])

        self.assertEqual(definition["kind"], "server_guardian_orphan_resume")
        self.assertEqual(definition["expected_total"], 4)
        self.assertEqual(
            spec["generations"] * spec["population"],
            definition["expected_total"],
        )
        self.assertNotIn("server_guardian_orphan_resume", run_acceptance.FULL_ORDER)
        self.assertIn(
            "server_guardian_orphan_resume",
            run_acceptance.RELEASE_EXTENDED_ORDER,
        )

    def test_detached_helper_requires_token_and_same_workspace_sentinel_has_none(self):
        token_identity = build_guardian_token_identity("a" * 32, "epoch-test", 0)
        owned = None
        sentinel = None
        with tempfile.TemporaryDirectory() as workspace:
            try:
                owned = run_acceptance._launch_acceptance_scope_process(
                    workspace, "owned-test-helper", token_identity["token"],
                )
                sentinel = run_acceptance._launch_acceptance_scope_process(
                    workspace, "unrelated-test-sentinel", None,
                )
                self.assertTrue(owned["proof"]["launch_token_present"])
                self.assertTrue(all(owned["proof"]["checks"].values()))
                self.assertFalse(sentinel["proof"]["launch_token_present"])
                self.assertTrue(all(sentinel["proof"]["checks"].values()))
                with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
                    run_acceptance._prove_acceptance_scope_process(
                        sentinel["process"], sentinel["command"], workspace,
                        expected_token=token_identity["token"],
                        timeout_seconds=0.05,
                    )
            finally:
                owned_cleanup = run_acceptance._cleanup_acceptance_scope_process(owned)
                sentinel_cleanup = run_acceptance._cleanup_acceptance_scope_process(sentinel)
            self.assertTrue(owned_cleanup["exact_identity_dead"])
            self.assertTrue(sentinel_cleanup["exact_identity_dead"])

    def test_post_server_guardian_kill_rejects_identity_change_without_signal(self):
        target = {
            "pid": 62001, "start_ticks": 8001, "ppid": 62000,
            "pgid": 62001, "sid": 62001, "state": "S",
            "executable": "/usr/bin/python3", "cwd": "/tmp/workspace",
            "argv": ["python3", "guardian.py"], "launch_token": "token",
            "uid": os.getuid(),
        }
        hold = {
            "stop_proven": True,
            "stopped": dict(target, state="T"),
            "reverified": {
                "target": target,
                "guardian_manifest_path": "/tmp/not-reached.json",
                "guardian_manifest_sha256": "not-reached",
            },
        }
        signals = []
        changed = dict(target, start_ticks=9999, state="T")
        with self.assertRaises(acceptance_fault_matrix.FaultSafetyError):
            run_acceptance._kill_held_guardian_after_server_loss(
                hold, process_reader=lambda _pid: changed,
                proc_reader=lambda _pid: {},
                signal_sender=lambda pid, value: signals.append((pid, value)),
                sleep=lambda _seconds: None,
            )
        self.assertEqual(signals, [])

    def test_post_server_guardian_kill_rechecks_manifest_and_signals_exact_pid(self):
        target = {
            "pid": 62002, "start_ticks": 8002, "ppid": 62000,
            "pgid": 62002, "sid": 62002, "state": "T",
            "executable": "/usr/bin/python3", "cwd": "/tmp/workspace",
            "argv": ["python3", "guardian.py"], "launch_token": "token",
            "uid": os.getuid(),
        }
        signals = []
        live = {"value": True}

        def process_reader(_pid):
            return dict(target) if live["value"] else None

        def signal_sender(pid, value):
            signals.append((pid, value))
            live["value"] = False

        with tempfile.TemporaryDirectory() as root:
            manifest_path = os.path.join(root, "guardian_launch_manifest.json")
            payload = b'{"schema_version":"fixture"}\n'
            with open(manifest_path, "wb") as stream:
                stream.write(payload)
            hold = {
                "stop_proven": True, "stopped": dict(target),
                "reverified": {
                    "target": target,
                    "guardian_manifest_path": manifest_path,
                    "guardian_manifest_sha256": hashlib.sha256(payload).hexdigest(),
                },
            }
            report = run_acceptance._kill_held_guardian_after_server_loss(
                hold, process_reader=process_reader,
                proc_reader=lambda _pid: {
                    "cwd": target["cwd"], "argv": target["argv"],
                    "launch_token": target["launch_token"], "uid": target["uid"],
                },
                signal_sender=signal_sender, sleep=lambda _seconds: None,
            )
        self.assertEqual(signals, [(62002, signal.SIGKILL)])
        self.assertTrue(report["death_proven"])


class FaultRecoveryEpochPolicyTests(unittest.TestCase):
    @staticmethod
    def _rows(epochs, starts=None, cleanup_pending=False):
        starts = starts or [1] * len(epochs)
        return [{"persistent_worker": {
            "epoch": epoch, "session_start_count": start,
            "cleanup_pending": cleanup_pending,
        }} for epoch, start in zip(epochs, starts)]

    def test_single_forward_epoch_transition_passes(self):
        policy = audit_harness._fault_recovery_epoch_policy(
            self._rows(["epoch-a", "epoch-a", "epoch-b", "epoch-b"]),
        )
        self.assertTrue(policy["two_candidate_epochs"])
        self.assertTrue(policy["single_forward_epoch_transition"])
        self.assertTrue(policy["sessions_valid"])
        self.assertTrue(policy["cleanup_committed"])

    def test_epoch_reentry_or_wrong_epoch_count_fails(self):
        for epochs in (
                ["epoch-a", "epoch-b", "epoch-a"],
                ["epoch-a", "epoch-a"],
                ["epoch-a", "epoch-b", "epoch-c"],
                [None, "epoch-a"]):
            with self.subTest(epochs=epochs):
                policy = audit_harness._fault_recovery_epoch_policy(self._rows(epochs))
                self.assertFalse(
                    policy["two_candidate_epochs"] and
                    policy["single_forward_epoch_transition"]
                )

    def test_restart_or_pending_cleanup_fails(self):
        restarted = audit_harness._fault_recovery_epoch_policy(
            self._rows(["epoch-a", "epoch-b"], starts=[1, 2]),
        )
        pending = audit_harness._fault_recovery_epoch_policy(
            self._rows(["epoch-a", "epoch-b"], cleanup_pending=True),
        )
        self.assertFalse(restarted["sessions_valid"])
        self.assertFalse(pending["cleanup_committed"])


class ResourceTrendGateTests(unittest.TestCase):
    @staticmethod
    def _rows(rss_values):
        rows = []
        for rss in rss_values:
            rows.append({"persistent_worker": {
                "health": {"rss_kib": 100000, "fd_count": 50},
                "session_health": {
                    "schema_version": "crescendo.persistent_session_health.v1",
                    "triggered": False, "consecutive_limit": 3,
                    "session_identity": {
                        "pid": 1000, "pid_start_ticks": 2000,
                        "controller_epoch": 0, "worker_epoch": "worker-000-epoch-000",
                    },
                    "sample": {
                        "alive": True, "tree_complete": True,
                        "storage_complete": True, "logs_complete": True,
                        "rss_kib": rss, "fd_count": 50, "max_fd_count": 20,
                        "free_disk_mib": 10000, "log_bytes": 4096,
                    },
                },
            }})
        return rows

    @staticmethod
    def _mark_incomplete(rows, indexes):
        for index in indexes:
            sample = rows[index]["persistent_worker"]["session_health"]["sample"]
            sample.update({
                "tree_complete": False,
                "rss_kib": None, "fd_count": None, "max_fd_count": None,
            })

    def _report(self, rows):
        with tempfile.TemporaryDirectory() as run_root:
            common.write_json(os.path.join(run_root, "checkpoint.json"), {})
            with mock.patch.object(run_acceptance, "candidate_rows", return_value=rows), \
                    mock.patch.object(
                        run_acceptance, "raw_artifacts", return_value={"findings": []},
                    ):
                return run_acceptance._resource_trend_report(run_root, 200)

    def test_startup_growth_then_tail_plateau_passes(self):
        rss = [100000 + 800 * index if index < 100 else 180000
               for index in range(200)]
        report = self._report(self._rows(rss))
        self.assertTrue(report["ok"])
        self.assertFalse(report["rss_kib"]["persistent_growth_detected"])

    def test_full_tree_tail_growth_fails_below_hard_span(self):
        rss = [100000 + 800 * index for index in range(200)]
        report = self._report(self._rows(rss))
        self.assertTrue(report["checks"]["rss_span_within_256_mib"])
        self.assertTrue(report["rss_kib"]["persistent_growth_detected"])
        self.assertFalse(report["checks"]["rss_tail_100_has_no_persistent_growth"])
        self.assertFalse(report["ok"])

    def test_strong_rss_growth_is_not_hidden_by_one_increment_ratio_boundary(self):
        tail = [100000.0]
        for index in range(1, 100):
            increment = -100.0 if index % 4 == 0 or index == 99 else 1200.0
            tail.append(tail[-1] + increment)
        report = self._report(self._rows([100000.0] * 100 + tail))

        self.assertLess(
            report["rss_kib"]["tail_100"]["positive_increment_fraction"],
            0.75,
        )
        self.assertGreater(
            report["rss_kib"]["tail_100"]["theil_sen_per_candidate"],
            256.0,
        )
        self.assertGreater(
            report["rss_kib"]["tail_100"]["window_median_drift"],
            16.0 * 1024.0,
        )
        self.assertTrue(report["rss_kib"]["persistent_growth_detected"])
        self.assertFalse(report["ok"])

    def test_single_incomplete_session_sample_is_allowed(self):
        rows = self._rows([100000] * 200)
        self._mark_incomplete(rows, [50])
        report = self._report(rows)
        self.assertTrue(report["ok"])
        self.assertEqual(report["session_health_samples"]["incomplete_count"], 1)
        self.assertEqual(
            report["session_health_samples"]["maximum_consecutive_incomplete"], 1,
        )

    def test_incomplete_streak_reaching_guard_limit_fails(self):
        rows = self._rows([100000] * 200)
        self._mark_incomplete(rows, [50, 51, 52])
        report = self._report(rows)
        self.assertFalse(
            report["checks"]["session_health_incomplete_streak_below_limit"],
        )
        self.assertEqual(
            report["session_health_samples"]["maximum_consecutive_incomplete"], 3,
        )
        self.assertFalse(report["ok"])

    def test_last_incomplete_session_sample_fails(self):
        rows = self._rows([100000] * 200)
        self._mark_incomplete(rows, [199])
        report = self._report(rows)
        self.assertFalse(report["checks"]["last_session_health_sample_complete"])
        self.assertFalse(report["ok"])

    def test_session_health_sampling_error_fails(self):
        rows = self._rows([100000] * 200)
        rows[50]["persistent_worker"]["session_health"]["sampling_error"] = (
            "OSError: transient sampler failure"
        )
        report = self._report(rows)
        self.assertFalse(report["checks"]["no_session_health_sampling_errors"])
        self.assertFalse(report["ok"])

    def test_missing_session_root_identity_fails(self):
        rows = self._rows([100000] * 200)
        del rows[50]["persistent_worker"]["session_health"]["session_identity"]
        report = self._report(rows)
        self.assertFalse(report["checks"]["all_session_health_roots_identified"])
        self.assertFalse(report["ok"])


class CrashResumePortabilityPolicyTests(unittest.TestCase):
    @staticmethod
    def _candidate_checkpoint(status):
        document = _checkpoint(1, ["g000_c0000"])
        document["candidate_layout"] = ["id", "status"]
        document["candidates"] = [["g000_c0000", status]]
        document["state"].update({
            "completed_candidates": 1 if status == "completed" else 0,
            "failed_candidates": 1 if status == "failed" else 0,
            "infrastructure_failed_candidates": (
                1 if status == "infrastructure_failed" else 0
            ),
        })
        document["payload_sha256"] = common.checkpoint_checksum(document)
        return document

    def test_runner_sigkill_resume_uses_jobs2_and_serial_clamp(self):
        definition = run_acceptance.CASES["runner_sigkill_resume"]
        spec = common.load_json(definition["spec"])

        self.assertEqual(spec["jobs"], 2)
        self.assertEqual(definition["expected_requested_jobs"], 2)
        self.assertEqual(definition["expected_effective_jobs"], 1)
        self.assertEqual(
            spec["generations"] * spec["population"],
            definition["expected_total"],
        )

    def test_crash_checkpoint_wait_requires_a_completed_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint_path = os.path.join(root, "checkpoint.json")
            common.write_json(
                checkpoint_path, self._candidate_checkpoint("failed"),
            )
            self.assertIsNone(run_acceptance._checkpoint_ready(
                checkpoint_path, 12, require_completed=True,
            ))

            completed = self._candidate_checkpoint("completed")
            common.write_json(checkpoint_path, completed)
            self.assertEqual(
                run_acceptance._checkpoint_ready(
                    checkpoint_path, 12, require_completed=True,
                ),
                completed,
            )

    def test_source_runtime_anchor_and_pending_are_isolated_then_removed(self):
        from crescendo_engine.continuation import (
            continuation_checkpoint_path, continuation_pending_path,
        )

        with tempfile.TemporaryDirectory() as root:
            source_runtime = os.path.join(root, "source_runtime")
            origin_root = os.path.join(source_runtime, "crash_origin")
            evidence = os.path.join(root, "evidence")
            os.makedirs(origin_root)
            os.makedirs(evidence)
            continuation_id = "continuation-portable-crash"
            checkpoint = _origin_checkpoint(origin_root, continuation_id)
            anchor_path = continuation_checkpoint_path(
                source_runtime, continuation_id, create=True,
            )
            pending_path = continuation_pending_path(
                source_runtime, continuation_id, create=False,
            )
            common.write_json(anchor_path, checkpoint)
            common.write_json(pending_path, {"pending": True})

            isolation = run_acceptance._isolate_crash_source_runtime(
                source_runtime, origin_root, checkpoint, evidence,
            )

            self.assertTrue(all(isolation["checks"].values()))
            self.assertFalse(os.path.lexists(source_runtime))
            self.assertTrue(os.path.isdir(isolation["archived_origin"]))
            self.assertTrue(os.path.isfile(isolation["archived_anchor"]))
            self.assertTrue(os.path.isfile(isolation["archived_pending"]))
            cleanup = run_acceptance._remove_isolated_crash_source(
                isolation, evidence,
            )
            self.assertTrue(cleanup["removed"])
            self.assertFalse(os.path.lexists(isolation["archive_root"]))

    def test_source_isolation_rejects_unrelated_anchor(self):
        from crescendo_engine.continuation import continuation_checkpoint_path

        with tempfile.TemporaryDirectory() as root:
            source_runtime = os.path.join(root, "source_runtime")
            origin_root = os.path.join(source_runtime, "crash_origin")
            evidence = os.path.join(root, "evidence")
            os.makedirs(origin_root)
            os.makedirs(evidence)
            continuation_id = "continuation-unrelated-anchor"
            checkpoint = _origin_checkpoint(origin_root, continuation_id)
            unrelated = _origin_checkpoint(
                origin_root, continuation_id, revision=2, ancestor=None,
            )
            common.write_json(continuation_checkpoint_path(
                source_runtime, continuation_id, create=True,
            ), unrelated)

            with self.assertRaises(run_acceptance.UnsafeAcceptanceError):
                run_acceptance._isolate_crash_source_runtime(
                    source_runtime, origin_root, checkpoint, evidence,
                )
            self.assertTrue(os.path.isdir(source_runtime))
            self.assertFalse(os.path.lexists(os.path.join(
                evidence, "source_runtime_archive",
            )))


class HealthGuardAcceptancePolicyTests(unittest.TestCase):
    def test_active_health_stop_requires_one_explicit_resource_guard_replacement(self):
        definition = run_acceptance.CASES["health_guard_resume"]

        self.assertEqual(definition["expected_total"], 4)
        self.assertEqual(definition["expected_infrastructure"], 1)
        self.assertEqual(
            definition["allowed_infrastructure_failure_classes"],
            ["resource_guard"],
        )
        self.assertTrue(definition["allow_replacement_layout"])


class ResumeAndPlanGateTests(unittest.TestCase):
    def _pending_materialization_fixture(self, root):
        from crescendo_engine.continuation import continuation_checkpoint_path

        run_root = os.path.join(root, "optimization_fixture")
        os.makedirs(run_root)
        continuation_id = "continuation-fixture"
        initial = _origin_checkpoint(run_root, continuation_id)
        current = _origin_checkpoint(
            run_root, continuation_id, revision=2,
            ancestor=initial["payload_sha256"],
        )
        common.write_json(os.path.join(run_root, "status.json"), {
            "schema_version": "crescendo.run_status.v1",
            "run_id": os.path.basename(run_root), "run_root": run_root,
            "continuation_id": continuation_id, "state": "stopped",
        })
        common.write_json(os.path.join(run_root, "checkpoint.json"), current)
        anchor_path = continuation_checkpoint_path(
            root, continuation_id, create=True,
        )
        common.write_json(anchor_path, current)
        return run_root, continuation_id, initial, current, anchor_path

    def test_materialize_first_pending_checkpoint_is_safe_and_exact(self):
        from crescendo_engine.continuation import continuation_pending_path

        with tempfile.TemporaryDirectory() as root:
            run_root, continuation_id, initial, _current, anchor_path = \
                self._pending_materialization_fixture(root)
            result = run_acceptance._materialize_pending_only_snapshot(
                root, run_root, initial,
                {"owned_dead": True, "server_dead": True},
            )
            pending = common.load_json(continuation_pending_path(
                root, continuation_id, create=False,
            ))

            self.assertEqual(pending["checkpoint"], initial)
            self.assertIsNone(pending["parent_payload_sha256"])
            self.assertFalse(os.path.lexists(os.path.join(run_root, "checkpoint.json")))
            self.assertFalse(os.path.lexists(anchor_path))
            self.assertEqual(result["revision"], 1)

    def test_materialize_pending_preflight_failure_preserves_existing_state(self):
        from crescendo_engine.continuation import continuation_pending_path

        with tempfile.TemporaryDirectory() as root:
            run_root, continuation_id, initial, current, anchor_path = \
                self._pending_materialization_fixture(root)
            status = common.load_json(os.path.join(run_root, "status.json"))
            status["run_root"] = os.path.join(root, "wrong")
            common.write_json(os.path.join(run_root, "status.json"), status)

            with self.assertRaises(run_acceptance.UnsafeAcceptanceError):
                run_acceptance._materialize_pending_only_snapshot(
                    root, run_root, initial,
                    {"owned_dead": True, "server_dead": True},
                )

            self.assertEqual(
                common.load_json(os.path.join(run_root, "checkpoint.json")), current,
            )
            self.assertEqual(common.load_json(anchor_path), current)
            self.assertFalse(os.path.lexists(continuation_pending_path(
                root, continuation_id, create=False,
            )))

    def test_same_immutable_old_checkpoint_can_be_submitted_twice(self):
        old = _checkpoint(2, ["g000_c0000"])
        second = _checkpoint(4, ["g000_c0000", "g000_c0001"])
        final = _checkpoint(7, ["g000_c0000", "g000_c0001", "g001_c0000"])
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "checkpoint.json")
            common.write_json(path, old)
            snapshot = acceptance_fault_matrix.checkpoint_file_snapshot(path)
            report = acceptance_fault_matrix.prove_repeated_old_checkpoint(
                path, snapshot, old, [common.load_json(path), common.load_json(path)],
                second, final,
            )
        self.assertTrue(report["ok"])
        self.assertTrue(report["checks"]["both_resume_submissions_used_exact_old_checkpoint"])

    def test_release_extended_dry_run_preflight_never_executes_eda(self):
        output = io.StringIO()
        with mock.patch.object(run_acceptance, "_run_case") as run_case, \
                redirect_stdout(output):
            code = run_acceptance.main(["--profile", "release_extended"])
        plan = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertTrue(plan["preflight"]["ok"])
        self.assertFalse(plan["execute"])
        self.assertIn(
            "pending_only_export_resume",
            [row["name"] for row in plan["cases"]],
        )
        self.assertIn(
            "combined_fault_checkpoint_resume",
            [row["name"] for row in plan["cases"]],
        )
        self.assertIn(
            "server_guardian_orphan_resume",
            [row["name"] for row in plan["cases"]],
        )
        self.assertEqual(plan["cases"][-1]["name"], "opa_soak_200")
        run_case.assert_not_called()

    def test_surviving_owned_identity_writes_metadata_then_fails_closed(self):
        identity = {"pid": 51002, "start_ticks": 7002, "command": "ocean"}
        process = _CompletedProcess()
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(run_acceptance.subprocess, "Popen", return_value=process), \
                mock.patch.object(
                    run_acceptance, "runtime_observation",
                    return_value={
                        "epoch": 1.0, "controllers": [],
                        "workers": [{"guardian": None, "ocean": identity}],
                    },
                ), \
                mock.patch.object(
                    run_acceptance, "process_identity",
                    return_value={"pid": process.pid, "start_ticks": 6002},
                ), \
                mock.patch.object(run_acceptance, "identity_alive", return_value=True):
            evidence = os.path.join(root, "evidence")
            with self.assertRaises(run_acceptance.UnsafeAcceptanceError):
                run_acceptance._run_monitored(
                    ["fake-runner"], os.path.join(root, "run"), evidence,
                    observation_interval=0.01, owned_cleanup_timeout_seconds=0.0,
                )
            with open(os.path.join(evidence, "run_metadata.json"), "r") as stream:
                metadata = json.load(stream)

        self.assertFalse(metadata["owned_identity_cleanup"]["all_dead"])
        self.assertEqual(
            metadata["owned_identity_cleanup"]["survivors"]["ocean"], [identity],
        )
        self.assertTrue(metadata["unsafe_reasons"])

    def test_monitor_error_stops_runner_and_still_proves_owned_dead(self):
        identity = {"pid": 51003, "start_ticks": 7003, "command": "guardian"}
        process = _RunningProcess()

        def fail_monitor(_observation):
            raise RuntimeError("monitor injection")

        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(run_acceptance.subprocess, "Popen", return_value=process), \
                mock.patch.object(
                    run_acceptance, "runtime_observation",
                    return_value={
                        "epoch": 1.0, "controllers": [],
                        "workers": [{"guardian": identity, "ocean": None}],
                    },
                ), \
                mock.patch.object(
                    run_acceptance, "process_identity",
                    return_value={"pid": process.pid, "start_ticks": 6003},
                ), \
                mock.patch.object(run_acceptance, "identity_alive", return_value=False):
            metadata = run_acceptance._run_monitored(
                ["fake-runner"], os.path.join(root, "run"), os.path.join(root, "evidence"),
                on_observation=fail_monitor, owned_cleanup_timeout_seconds=0.0,
            )

        self.assertIn("monitor injection", metadata["monitor_error"])
        self.assertTrue(metadata["runner_cleanup"]["stop_requested"])
        self.assertTrue(metadata["runner_cleanup"]["terminate_sent"])
        self.assertTrue(metadata["runner_cleanup"]["termination_proven"])
        self.assertTrue(metadata["owned_identity_cleanup"]["all_dead"])


if __name__ == "__main__":
    unittest.main()
