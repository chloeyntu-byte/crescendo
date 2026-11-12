import copy
import json
import fcntl
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import shutil
from http.server import HTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest import mock

from regression_fixture import qualified_service, saved_setup
from crescendo_engine.__main__ import main as optimizer_main
from crescendo_engine.server import (
    OptimizerService, _inspect_worker_epoch_processes, _process_snapshot,
    _checkpoint_continuation_schema_payload, _reconcile_continuation_anchor,
    _resume_cleanup_timeout, _terminate_owned_controller,
    make_handler, serve,
)
from crescendo_engine.legacy_guardian import (
    MANIFEST_SCHEMA_VERSION as GUARDIAN_LAUNCH_PROTOCOL,
    acquire_legacy_guardian_launch_lock,
    build_guardian_token_identity,
    close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity,
    write_legacy_guardian_manifest,
)
from crescendo_engine.retention import cleanup_abandoned_worker_epoch
from crescendo_engine.continuation import (
    ContinuationLease, ContinuationLeaseBusy, RuntimeOptimizationLease,
    build_continuation_pending, checkpoint_continuation_id,
    continuation_checkpoint_path, continuation_pending_path,
    continuation_schema_sha256, persist_continuation_pending,
)
from crescendo_engine.persistent_maestro import persistent_worker_control_root
from crescendo_engine.runner import (
    _checkpoint_checksum, _run_coordinated_resume, checkpoint_candidates,
    checkpoint_with_candidates, run_optimization,
)
from crescendo_engine.utils import load_json


def _scratch_root(test):
    """Private run root for one test; never the shared system /tmp."""
    root = tempfile.mkdtemp(prefix="crescendo-test-")
    test.addCleanup(shutil.rmtree, root, True)
    return root


_BASE_CANDIDATE_LAYOUT = [
    "id", "generation", "index", "status", "values", "metrics",
    "failure_class", "error", "started_at", "finished_at",
]


def _snapshot_checkpoint(run_root, continuation_id, revision=1, ancestor=None):
    checkpoint = {
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
        "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
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
    checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
    return checkpoint


def _snapshot_status(run_root, continuation_id):
    return {
        "schema_version": "crescendo.run_status.v1",
        "run_id": os.path.basename(run_root),
        "run_root": run_root,
        "continuation_id": continuation_id,
        "state": "running",
    }


def _write_pending_snapshot(runtime_root, checkpoint, parent=None):
    continuation_id = checkpoint_continuation_id(checkpoint)
    pending = build_continuation_pending(
        checkpoint, continuation_id,
        int(checkpoint["state"]["checkpoint_revision"]), parent,
        continuation_schema_sha256(
            _checkpoint_continuation_schema_payload(checkpoint)
        ),
    )
    persist_continuation_pending(runtime_root, continuation_id, pending)
    return pending


def _coordinated_runner_resume(spec, config, prepared, run_root, checkpoint):
    lease = ContinuationLease(
        config["runtime"]["run_root"], checkpoint_continuation_id(checkpoint),
        run_root=run_root,
    ).acquire()
    try:
        _reconcile_continuation_anchor(
            checkpoint, config["runtime"]["run_root"],
        )
        return _run_coordinated_resume(
            spec, config, prepared=prepared, run_root=run_root,
            resume_checkpoint=checkpoint, continuation_lease=lease,
        )
    finally:
        lease.release()


class OptimizerServiceTest(unittest.TestCase):
    _CURRENT_BOOT_ID = "11111111-2222-3333-4444-555555555555"
    _PREVIOUS_BOOT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def test_resume_cleanup_timeout_rejects_non_finite_values(self):
        for value in (float("inf"), float("-inf"), float("nan")):
            with self.subTest(value=value):
                with self.assertRaisesRegex(RuntimeError, "must be finite"):
                    _resume_cleanup_timeout(
                        {"resume_worker_cleanup_timeout_seconds": value},
                        "resume_worker_cleanup_timeout_seconds", 30.0,
                    )

    def test_worker_resume_cleanup_uses_one_global_timeout_budget(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            for worker_index in (0, 1):
                os.makedirs(os.path.join(
                    origin, "workers", "worker_%03d" % worker_index, "epoch_000",
                ))
            checkpoint = {
                "manifest": {"origin_run_root": origin, "origin_run_id": "origin"},
            }
            service = OptimizerService({
                "runtime": {
                    "run_root": root,
                    "resume_worker_cleanup_timeout_seconds": 5.0,
                },
            })
            inspections = {}
            clock = [100.0]
            budgets = []

            def inspect(epoch_root, worker_index, epoch_index):
                key = (worker_index, epoch_index)
                inspections[key] = inspections.get(key, 0) + 1
                abandoned = inspections[key] > 1
                return {
                    "state": "abandoned" if abandoned else "unsafe",
                    "worker_index": worker_index, "epoch_index": epoch_index,
                    "processes": [],
                    "termination_proof": {
                        "ok": abandoned,
                        "kind": "guardian_cleanup_proven" if abandoned else "cleanup_pending",
                    },
                }

            def quiesce(epoch_root, worker_index, epoch_index, timeout_seconds):
                budgets.append(timeout_seconds)
                clock[0] += 4.0
                return {
                    "state": "abandoned", "worker_index": worker_index,
                    "epoch_index": epoch_index, "processes": [],
                    "termination_requested": True,
                    "termination_proof": {"ok": True, "kind": "guardian_cleanup_proven"},
                }

            with mock.patch(
                    "crescendo_engine.server._inspect_worker_epoch_processes",
                    side_effect=inspect), mock.patch(
                    "crescendo_engine.server._quiesce_worker_epoch_processes",
                    side_effect=quiesce), mock.patch(
                    "crescendo_engine.server.cleanup_abandoned_worker_epoch"), mock.patch(
                    "crescendo_engine.server.time.monotonic",
                    side_effect=lambda: clock[0]):
                result = service._cleanup_checkpoint_origin(
                    checkpoint, terminate_owned=True,
                )

            self.assertEqual(result["cleaned_workers"], [
                "worker_000/epoch_000", "worker_001/epoch_000",
            ])
            self.assertEqual(budgets, [5.0, 1.0])

    def _assert_fstat_failure_closes_last_descriptor(self, invoke, fail_on_call):
        real_open = os.open
        real_fstat = os.fstat
        opened = []
        fstat_calls = [0]

        def tracked_open(*args, **kwargs):
            descriptor = real_open(*args, **kwargs)
            opened.append(descriptor)
            return descriptor

        def failing_fstat(descriptor):
            fstat_calls[0] += 1
            if fstat_calls[0] == fail_on_call:
                raise OSError("injected fstat failure")
            return real_fstat(descriptor)

        with mock.patch("crescendo_engine.server.os.open", side_effect=tracked_open), \
                mock.patch("crescendo_engine.server.os.fstat", side_effect=failing_fstat):
            with self.assertRaisesRegex(OSError, "injected fstat failure"):
                invoke()
        self.assertTrue(opened)
        with self.assertRaises(OSError):
            real_fstat(opened[-1])

    def test_runtime_directory_fstat_failure_closes_descriptor(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            self._assert_fstat_failure_closes_last_descriptor(
                service._open_runtime_directory, 1,
            )

    def test_run_directory_fstat_failure_closes_descriptor(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "run-1"))
            service = OptimizerService({"runtime": {"run_root": root}})
            self._assert_fstat_failure_closes_last_descriptor(
                lambda: service._open_run_directory("run-1"), 2,
            )

    def test_run_relative_fstat_failure_closes_descriptor(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-1")
            os.makedirs(run_root)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                stream.write("{}")
            service = OptimizerService({"runtime": {"run_root": root}})
            self._assert_fstat_failure_closes_last_descriptor(
                lambda: service._open_run_relative("run-1", "status.json"), 3,
            )

    def test_prepare_preserves_simulation_submission_identity(self):
        setup_path = saved_setup(self, "maestro")
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        submission = {
            "schema_version": "crescendo.simulation_submission.v1",
            "controller": "maestro", "simulator": "spectre",
            "submit_script": "/tmp/submit-spectre.sh",
        }
        prepared = {
            "setup": {}, "hierarchy": {},
            "source_identity": {
                "schema_version": "crescendo.source_design_identity.v1",
                "fingerprint": "source-test",
            },
            "eda_context": {"submission": submission},
        }
        resident = mock.Mock(workspace={"root": "/tmp/private-validation"})
        with mock.patch(
                "crescendo_engine.server.prepare_setup",
                return_value=prepared,
        ), mock.patch.object(
                service, "eda_setup", return_value={"version_checks": []},
        ), mock.patch(
                "crescendo_engine.server.detect_pdk_context",
                return_value={"ok": True, "pdk_name": "PDK"},
        ), mock.patch.object(
                service, "_start_resident_validation", return_value=resident,
        ):
            _prepared_id, result = service.prepare(setup_path, backend="maestro")
        self.assertEqual(result["eda_context"]["submission"], submission)
        self.assertEqual(result["resident_validation"]["state"], "deferred")

    def test_prepare_defaults_to_generic_backend_auto_detection(self):
        setup_path = saved_setup(self, "adexl")
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        prepared = {
            "setup": {"backend": "adexl"}, "hierarchy": {}, "eda_context": {},
            "source_identity": {
                "schema_version": "crescendo.source_design_identity.v1",
                "fingerprint": "source-test",
            },
        }
        resident = mock.Mock(workspace={"root": "/tmp/private-validation"})
        with mock.patch(
                "crescendo_engine.server.prepare_setup", return_value=prepared,
        ) as generic_prepare, mock.patch(
                "crescendo_engine.server.prepare_maestro",
        ) as maestro_prepare, mock.patch.object(
                service, "eda_setup", return_value={},
        ), mock.patch(
                "crescendo_engine.server.detect_pdk_context", return_value={},
        ), mock.patch.object(
                service, "_start_resident_validation", return_value=resident,
        ):
            _prepared_id, result = service.prepare(setup_path)
        generic_prepare.assert_called_once_with(
            setup_path, service.config, max_depth=32, backend="auto", selected_cell_paths=None, selected_test="T",
        )
        maestro_prepare.assert_not_called()
        self.assertEqual(result["setup"]["backend"], "adexl")

    def test_prepare_defers_native_validation_without_starting_resident(self):
        setup_path = saved_setup(self, "maestro")
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        prepared = {
            "setup": {}, "hierarchy": {}, "eda_context": {},
            "source_identity": {
                "schema_version": "crescendo.source_design_identity.v1",
                "fingerprint": "source-test",
            },
        }
        with mock.patch("crescendo_engine.server.prepare_setup", return_value=prepared), \
                mock.patch.object(service, "eda_setup", return_value={}), \
                mock.patch("crescendo_engine.server.detect_pdk_context", return_value={}), \
                mock.patch.object(service, "_start_resident_validation", side_effect=RuntimeError("no license")):
            _prepared_id, result = service.prepare(setup_path, backend="maestro")
        self.assertEqual(result["resident_validation"]["state"], "deferred")
        self.assertEqual(result["resident_validation"]["method"], "native_run_device_parameters")

    def test_new_resident_session_closes_previous_before_start(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            old = mock.Mock()
            service.resident_validation = old
            new = mock.Mock()
            with mock.patch.object(service, "_cancel_and_wait_range_validations") as cancel, \
                    mock.patch("crescendo_engine.server.ResidentValidationSession", return_value=new):
                result = service._start_resident_validation("new-id", {"setup": {}, "hierarchy": {}})
            cancel.assert_not_called()
            old.close.assert_called_once()
            new.start.assert_called_once_with()
            self.assertIs(result, new)
            self.assertIs(service.resident_validation, new)

    def test_exit_optimizer_stops_runs_and_closes_resident_session(self):
        service = OptimizerService({"runtime": {
            "run_root": _scratch_root(self), "optimizer_exit_timeout_seconds": 1,
        }})
        resident = mock.Mock()
        service.resident_validation = resident
        shutdown = mock.Mock()
        service.shutdown_callback = shutdown
        thread = mock.Mock()
        thread.is_alive.side_effect = [True, False, False]
        service.threads["optimization-test"] = thread
        with mock.patch.object(service, "stop", return_value={}) as stop, \
                mock.patch.object(service, "_cancel_and_wait_range_validations"):
            result = service.exit_optimizer()
        stop.assert_called_once_with("optimization-test")
        resident.close.assert_called_once()
        self.assertTrue(result["exiting"])

    def _write_origin_status(self, origin, continuation_id=None):
        document = {
            "schema_version": "crescendo.run_status.v1",
            "run_id": os.path.basename(origin),
            "run_root": origin,
            "continuation_id": continuation_id,
            "state": "running",
        }
        with open(os.path.join(origin, "status.json"), "w") as stream:
            json.dump(document, stream)
        return document

    def _make_terminal_wal_checkpoint(self, runtime_root):
        origin = os.path.join(runtime_root, "origin")
        candidate = os.path.join(origin, "candidates", "g000_c0000")
        os.makedirs(candidate)
        self._write_origin_status(origin)
        history_name = "CRESCENDO_g000_c0000_recovery"
        completion = {
            "schema_version": "crescendo.completion_evidence.v1",
            "adapter": "maestro-rdb-psf-v1",
            "ok": True,
            "validated_at": "2026-07-14T00:00:00Z",
            "history_name": history_name,
            "rdb": {
                "history_name": history_name,
                "path": "results/%s.rdb" % history_name,
                "size_bytes": 1,
                "mtime_epoch": 1.0,
            },
            "tests": ["TB"],
            "corners": [],
            "points": [{
                "test": "TB", "point_id": 0, "corner": None,
                "status": "completed", "status_code": 0,
            }],
            "outputs": [],
            "spectre_runs": [{
                "test": "TB", "point_id": 0, "corner": None,
                "psf": "results/%s/0/TB/psf" % history_name,
                "sim_done": "results/%s/0/TB/psf/.simDone" % history_name,
                "spectre": {
                    "path": "results/%s/0/TB/psf/spectre.out" % history_name,
                    "error_count": 0, "size_bytes": 1,
                    "completion": "spectre completes with 0 errors",
                },
            }],
        }
        row = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "values": {"x": 1.25}, "status": "completed",
            "source_guard": {"ok": True},
            "metrics": {
                "feasible": True, "total_violation": 0.0, "merit": 0.0,
                "penalized": False, "objectives": [], "constraints": [],
            },
            "completion": {
                "ok": True, "adapter": completion["adapter"],
                "history_name": history_name, "tests": ["TB"], "corners": [],
                "point_count": 1, "output_count": 0,
            },
        }
        with open(os.path.join(candidate, "candidate.json"), "w") as stream:
            json.dump(row, stream)
        with open(os.path.join(candidate, "completion_evidence.json"), "w") as stream:
            json.dump(completion, stream)
        with open(os.path.join(candidate, "raw.log"), "w") as stream:
            stream.write("raw")
        checkpoint = {
            "schema_version": "crescendo.checkpoint.v1",
            "manifest": {"origin_run_root": origin, "origin_run_id": "origin"},
            "parameter_ids": ["x"],
            "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
            "spec": {
                "evaluator": "mock", "population": 1,
                "variables": [{"parameter_id": "x"}],
                "objectives": [], "constraints": [],
            },
            "candidates": [],
            "state": {
                "next_generation": 0,
                "active_plan": {
                    "generation": 0, "next_index": 1, "replacements": 0,
                    "planned": [[0, [1.25]]],
                },
            },
        }
        return origin, candidate, checkpoint

    def _make_controller_epoch(self, origin, slot_index=0, epoch_index=0,
                               launch_boot_id=None, pid=920001, status="ready",
                               with_liveness=False):
        slot = os.path.join(origin, "controllers", "slot_%03d" % slot_index)
        epoch = os.path.join(slot, "epoch_%03d" % epoch_index)
        requests = os.path.join(epoch, "requests")
        responses = os.path.join(epoch, "responses")
        os.makedirs(requests)
        os.makedirs(responses)
        bootstrap_path = os.path.join(epoch, "bootstrap.json")
        state_path = os.path.join(epoch, "controller_state.json")
        token = "a" * 32
        parent_pid = 920000
        parent_start_ticks = 12340
        bootstrap = {
            "schema_version": "crescendo.persistent_controller.v1",
            "token": token,
            "worker_index": slot_index,
            "controller_epoch": epoch_index,
            "run_root": origin,
            "requests_root": requests,
            "responses_root": responses,
            "state_path": state_path,
            "ready_path": os.path.join(epoch, "ready.json"),
            "parent_pid": parent_pid,
            "parent_start_ticks": parent_start_ticks,
            "prepared": {}, "selected": [], "config": {},
        }
        if with_liveness:
            lock_path = os.path.join(epoch, "controller_liveness.lock")
            with open(lock_path, "a"):
                pass
            bootstrap.update({
                "liveness_lock_schema": "crescendo.controller_liveness.v1",
                "liveness_lock_path": lock_path,
            })
        command = [
            sys.executable, "-m", "crescendo_engine.persistent_controller",
            "--bootstrap", bootstrap_path,
        ]
        state = {
            "schema_version": "crescendo.persistent_controller.v1",
            "token": token,
            "worker_index": slot_index,
            "controller_epoch": epoch_index,
            "status": status,
            "pid": pid,
            "pgid": pid,
            "pid_start_ticks": 45670,
            "launch_boot_id": launch_boot_id or self._CURRENT_BOOT_ID,
            "parent_pid": parent_pid,
            "parent_start_ticks": parent_start_ticks,
            "run_root": origin,
            "command": command,
        }
        if with_liveness:
            state.update({
                "liveness_lock_schema": "crescendo.controller_liveness.v1",
                "liveness_lock_path": bootstrap["liveness_lock_path"],
            })
        with open(bootstrap_path, "w") as stream:
            json.dump(bootstrap, stream)
        with open(state_path, "w") as stream:
            json.dump(state, stream)
        return {
            "epoch": epoch, "bootstrap_path": bootstrap_path, "state_path": state_path,
            "bootstrap": bootstrap, "state": state, "command": command,
        }

    def _make_worker_epoch(self, origin, guardian_status="exited", guardian_pid=910001,
                           ocean_pid=910002, launch_boot_id=None,
                           epoch_name="epoch_000", epoch_identity="worker-000-epoch-000"):
        epoch = os.path.join(origin, "workers", "worker_000", epoch_name)
        workspace = os.path.join(epoch, "workspace")
        control = persistent_worker_control_root(epoch, epoch_identity)
        os.makedirs(workspace)
        os.makedirs(control)
        with open(os.path.join(workspace, "workspace.json"), "w") as stream:
            json.dump({
                "root": workspace, "source_libraries": {}, "source_snapshots": {},
            }, stream)
        guardian_state_path = os.path.join(control, "guardian_state.json")
        with open(guardian_state_path, "w") as stream:
            guardian = {
                "schema_version": "crescendo.ocean_worker_guardian.v1",
                "token": "%s-worker-0" % epoch_identity, "cwd": workspace,
                "guardian_pid": guardian_pid, "pid": ocean_pid, "pgid": ocean_pid,
                "argv": ["/opt/eda/bin/ocean", "-nograph"], "status": guardian_status,
                "group_cleanup_complete": guardian_status == "exited",
                "descendant_cleanup_complete": guardian_status == "exited",
                "descendant_supervision_complete": guardian_status == "exited",
                "remaining_group_pids": [], "remaining_adopted_pids": [],
            }
            if launch_boot_id is not None:
                guardian["launch_boot_id"] = launch_boot_id
            json.dump(guardian, stream)
        with open(os.path.join(epoch, "worker_state.json"), "w") as stream:
            worker = {
                "schema_version": "crescendo.persistent_maestro_worker.v1",
                "worker_index": 0, "epoch": epoch_identity,
                "workspace_root": workspace, "control_root": control,
                "guardian_state_path": guardian_state_path,
                "pid": guardian_pid, "pgid": guardian_pid,
                "ocean_pid": ocean_pid, "ocean_pgid": ocean_pid,
            }
            if launch_boot_id is not None:
                worker["launch_boot_id"] = launch_boot_id
            json.dump(worker, stream)
        return epoch, workspace, guardian_state_path

    def _make_durable_worker_prestate(
            self, origin, epoch_name="epoch_000", epoch_identity="worker-000-epoch-000",
            scope_token=None, ocean_command=None):
        epoch = os.path.join(origin, "workers", "worker_000", epoch_name)
        workspace = os.path.join(epoch, "workspace")
        control = persistent_worker_control_root(epoch, epoch_identity)
        os.makedirs(workspace)
        os.makedirs(control)
        with open(os.path.join(workspace, "workspace.json"), "w") as stream:
            json.dump({
                "root": workspace, "source_libraries": {}, "source_snapshots": {},
            }, stream)
        state_path = os.path.join(control, "guardian_state.json")
        stdout_path = os.path.join(control, "ocean.stdout")
        stderr_path = os.path.join(control, "ocean.stderr")
        token_identity = (
            build_guardian_token_identity(scope_token, epoch_identity, 0)
            if scope_token is not None else None
        )
        token = (
            token_identity["token"] if token_identity is not None
            else "%s-worker-0" % epoch_identity
        )
        ocean_command = list(ocean_command or ["/opt/eda/bin/ocean", "-nograph"])
        guardian_script = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "tools", "ocean_worker_guardian.py",
        )
        descriptor, lock_path = acquire_legacy_guardian_launch_lock(control)
        parent = legacy_guardian_parent_identity()
        guardian_command = [
            sys.executable, guardian_script,
            "--state-path", state_path,
            "--stdout-path", stdout_path,
            "--stderr-path", stderr_path,
            "--token", token,
            "--cwd", workspace,
            "--expected-parent-pid", str(parent["pid"]),
            "--launch-lock-fd", str(descriptor),
            "--",
        ] + ocean_command
        manifest = write_legacy_guardian_manifest(
            control, workspace, state_path, stdout_path, stderr_path, token,
            ocean_command, parent["pid"], guardian_script,
            launch_lock_path=lock_path,
            expected_parent_boot_id=parent["boot_id"],
            expected_parent_start_ticks=parent["start_ticks"],
            expected_uid=parent["uid"], guardian_argv=guardian_command,
            guardian_token_identity=token_identity,
        )
        with open(os.path.join(epoch, "worker_state.json"), "w") as stream:
            worker_state = {
                "schema_version": "crescendo.persistent_maestro_worker.v1",
                "worker_index": 0, "epoch": epoch_identity,
                "state": "launching", "workspace_root": workspace,
                "control_root": control, "guardian_state_path": state_path,
                "guardian_token": token, "guardian_launch_protocol": GUARDIAN_LAUNCH_PROTOCOL,
                "guardian_launch_manifest_path": os.path.join(
                    control, "legacy_guardian_launch.json",
                ),
                "guardian_launch_lock_path": lock_path,
                "launch_boot_id": parent["boot_id"],
                "pid": None, "pgid": None, "ocean_pid": None, "ocean_pgid": None,
            }
            if token_identity is not None:
                worker_state.update({
                    "guardian_token": token,
                    "guardian_token_identity": token_identity,
                })
            json.dump(worker_state, stream)
        return epoch, workspace, control, descriptor, manifest

    def test_scoped_same_epoch_runs_coexist_and_cleanup_is_run_local(self):
        with tempfile.TemporaryDirectory() as root:
            origins = []
            launches = []
            for name, scope in (("run-a", "a" * 32), ("run-b", "b" * 32)):
                origin = os.path.join(root, name)
                os.makedirs(origin)
                origins.append(origin)
                launches.append(self._make_durable_worker_prestate(
                    origin, scope_token=scope,
                ))
            try:
                for launch in launches:
                    close_legacy_guardian_launch_lock(launch[3])
                self.assertNotEqual(
                    launches[0][4]["token"], launches[1][4]["token"],
                )
                first = _inspect_worker_epoch_processes(launches[0][0], 0, 0)
                second = _inspect_worker_epoch_processes(launches[1][0], 0, 0)
                self.assertEqual(first["state"], "abandoned")
                self.assertEqual(second["state"], "abandoned")
                cleanup = cleanup_abandoned_worker_epoch(
                    launches[0][0], origins[0], lambda _metadata: {"ok": True},
                    termination_proof=first["termination_proof"],
                )
                self.assertTrue(cleanup["removed"])
                self.assertFalse(os.path.exists(launches[0][0]))
                self.assertTrue(os.path.isdir(launches[1][0]))
                self.assertEqual(
                    _inspect_worker_epoch_processes(launches[1][0], 0, 0)["state"],
                    "abandoned",
                )
            finally:
                for launch in launches:
                    try:
                        close_legacy_guardian_launch_lock(launch[3])
                    except OSError:
                        pass

    def test_scoped_worker_state_token_tampering_fails_closed(self):
        with tempfile.TemporaryDirectory() as origin:
            epoch, _workspace, _control, descriptor, _manifest = (
                self._make_durable_worker_prestate(origin, scope_token="a" * 32)
            )
            close_legacy_guardian_launch_lock(descriptor)
            state_path = os.path.join(epoch, "worker_state.json")
            worker_state = load_json(state_path)
            worker_state["guardian_token"] = "crescendo2-" + "f" * 64
            with open(state_path, "w") as stream:
                json.dump(worker_state, stream)
            with self.assertRaisesRegex(RuntimeError, "guardian token identity is invalid"):
                _inspect_worker_epoch_processes(epoch, 0, 0)

    def test_legacy_deterministic_worker_token_remains_compatible(self):
        with tempfile.TemporaryDirectory() as origin:
            epoch, _workspace, _state = self._make_worker_epoch(origin)
            inspection = _inspect_worker_epoch_processes(epoch, 0, 0)
            self.assertEqual(inspection["state"], "abandoned")
            self.assertTrue(inspection["termination_proof"]["ok"])

    def test_range_validation_is_optional_and_uses_separate_job_namespace(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {
                "parameters": [{"parameter_id": "w", "optimizable": True}],
            }
            variables = [{"parameter_id": "w", "minimum": 1e-6, "maximum": 8e-6}]
            started = threading.Event()
            finish = threading.Event()

            def hold_validation(_job, _prepared, _variables, _config, _cancel_event, update):
                started.set()
                finish.wait()
                update(state="completed", message="completed")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=hold_validation):
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", variables)
                self.assertTrue(started.wait(10))
                self.assertTrue(response["job_id"].startswith("range_validation_"))
                status = service.range_validation_status(response["job_id"])
                self.assertEqual(status["state"], "running")
                self.assertIn(os.path.join("range_validations", response["job_id"]), status["job_root"])
                finish.set()
                service.range_validation_threads[response["job_id"]].join(1)

    def test_start_waits_for_validation_thread_exit_without_changing_spec(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root, "range_validation_shutdown_timeout_seconds": 1,
            }})
            service.prepared["prepared-1"] = {
                "maestro_path": "/m",
                "parameters": [{"parameter_id": "w", "optimizable": True}],
            }
            event = threading.Event()
            started = threading.Event()
            exited = threading.Event()
            job_root = os.path.join(root, "range_validations", "range_validation_active")
            os.makedirs(job_root)

            def validation_target():
                started.set()
                event.wait()
                exited.set()

            validation_thread = threading.Thread(target=validation_target)
            service.range_validation_cancels["range_validation_active"] = event
            service.range_validations["range_validation_active"] = {
                "job_id": "range_validation_active", "job_root": job_root,
                "prepared_id": "prepared-1", "state": "running",
            }
            service.range_validation_threads["range_validation_active"] = validation_thread
            validation_thread.start()
            self.assertTrue(started.wait(10))
            spec = {
                "evaluator": "mock",
                "variables": [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
            }

            def launch_registered(launch_spec, _prepared, **_kwargs):
                self.assertTrue(exited.is_set())
                return {"run_id": "optimization-now", "spec": launch_spec}

            with mock.patch.object(service, "_launch_registered", side_effect=launch_registered) as launch:
                response = qualified_service(service, "prepared-1").start("prepared-1", spec)
            self.assertEqual(response["run_id"], "optimization-now")
            self.assertTrue(event.is_set())
            self.assertFalse(validation_thread.is_alive())
            self.assertEqual(launch.call_args[0][0]["variables"], spec["variables"])
            self.assertNotIn("maestro_path", spec)

    def test_start_refuses_stale_prepared_design_before_launch(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {"maestro_path": "/m"}
        with mock.patch(
                "crescendo_engine.server.require_prepared_freshness",
                side_effect=ValueError(
                    "Load and Inspect again before Validate or Start."
                ),
        ), mock.patch.object(service, "_launch") as launch:
            with self.assertRaisesRegex(ValueError, "Load and Inspect again"):
                service.start("prepared-1", {"evaluator": "mock"})
        launch.assert_not_called()

    def test_start_rejects_missing_metric_outputs_and_corners_before_launch(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {
            "parameters": [{"parameter_id": "w", "optimizable": True}],
            "outputs": [{"key": "main::gain"}],
            "setup": {"corners": [{"name": "tt"}]},
        }
        base_spec = {
            "evaluator": "real",
            "variables": [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
            "constraints": [],
        }
        cases = (
            (
                [{"output_key": "main::missing", "corners": ["tt"]}],
                "outputs: main::missing",
            ),
            (
                [{"output_key": "main::gain", "corners": ["tt", "ff"]}],
                "corners: ff",
            ),
        )
        with mock.patch("crescendo_engine.server.require_prepared_freshness"), mock.patch.object(
                service, "_launch") as launch:
            for objectives, message in cases:
                spec = dict(base_spec, objectives=objectives)
                with self.assertRaisesRegex(ValueError, message):
                    qualified_service(service, "prepared-1").start("prepared-1", spec)
        launch.assert_not_called()

    def test_mock_start_keeps_dynamic_metric_output_behavior(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {
            "parameters": [{"parameter_id": "w", "optimizable": True}],
            "outputs": [],
            "setup": {"corners": [{"name": "dynamic", "enabled": True}]},
        }
        spec = {
            "evaluator": "mock",
            "variables": [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
            "objectives": [{"output_key": "mock::cost", "corners": ["dynamic"]}],
            "constraints": [],
        }
        with mock.patch("crescendo_engine.server.require_prepared_freshness"), \
                mock.patch.object(
                    service, "_launch", return_value={"run_id": "mock-run"},
                ) as launch:
            result = qualified_service(service, "prepared-1").start("prepared-1", spec)
        self.assertEqual(result["run_id"], "mock-run")
        launch.assert_called_once()

    def test_validation_refuses_stale_prepared_design_before_resident_use(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {
            "parameters": [{"parameter_id": "w", "optimizable": True}],
        }
        with mock.patch(
                "crescendo_engine.server.require_prepared_freshness",
                side_effect=ValueError(
                    "Load and Inspect again before Validate or Start."
                ),
        ), mock.patch.object(service, "_start_resident_validation") as resident:
            response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
            )
            service.range_validation_threads[response["job_id"]].join(1)
        status = service.range_validation_status(response["job_id"])
        self.assertEqual(status["state"], "failed")
        self.assertIn("Load and Inspect again", status["detail"])
        resident.assert_not_called()

    def test_validation_uses_independent_resources_during_active_optimization(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
            active = mock.Mock()
            active.is_alive.return_value = True
            service.threads["optimization"] = active
            with mock.patch("crescendo_engine.server.run_range_validation") as validate:
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            validate.assert_called_once()

    def test_validation_can_register_while_optimization_is_being_registered(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
            service.optimization_launches = 1
            with mock.patch("crescendo_engine.server.run_range_validation") as validate:
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])
                service.range_validation_threads[response["job_id"]].join(1)
            validate.assert_called_once()

    def test_validation_refuses_duplicate_active_job(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
        service.range_validations["range_validation_active"] = {"state": "running"}
        with self.assertRaisesRegex(RuntimeError, "already running"):
            service.validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}])

    def test_launch_without_validation_does_not_wait(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch.object(service, "_cancel_and_wait_range_validations") as cancel:
                with mock.patch.object(service, "_launch_registered", return_value={"run_id": "resumed"}):
                    result = service._launch({"evaluator": "mock"}, {})
            self.assertEqual(result["run_id"], "resumed")
            cancel.assert_not_called()
            self.assertEqual(service.optimization_launches, 0)

    def test_launch_fails_closed_when_validation_thread_does_not_exit(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root, "range_validation_shutdown_timeout_seconds": 0.02,
            }})
            cancel_event = threading.Event()
            release_event = threading.Event()
            started = threading.Event()
            job_id = "range_validation_stuck"
            job_root = os.path.join(root, "range_validations", job_id)
            os.makedirs(job_root)

            def validation_target():
                started.set()
                release_event.wait()

            validation_thread = threading.Thread(target=validation_target)
            service.range_validations[job_id] = {
                "job_id": job_id, "job_root": job_root, "state": "running",
            }
            service.range_validation_cancels[job_id] = cancel_event
            service.range_validation_threads[job_id] = validation_thread
            validation_thread.start()
            self.assertTrue(started.wait(10))
            try:
                with mock.patch.object(service, "_launch_registered") as launch:
                    with self.assertRaisesRegex(RuntimeError, "refusing to start optimization"):
                        service._launch({"evaluator": "mock"}, {})
                launch.assert_not_called()
                self.assertTrue(cancel_event.is_set())
                self.assertEqual(service.optimization_launches, 0)
            finally:
                release_event.set()
                validation_thread.join(1)

    def test_launch_fails_closed_without_validation_thread_proof(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            job_id = "range_validation_untracked"
            job_root = os.path.join(root, "range_validations", job_id)
            os.makedirs(job_root)
            service.range_validations[job_id] = {
                "job_id": job_id, "job_root": job_root, "state": "running",
            }
            service.range_validation_cancels[job_id] = threading.Event()
            with mock.patch.object(service, "_launch_registered") as launch:
                with self.assertRaisesRegex(RuntimeError, "not proven safe.*missing thread tracking"):
                    service._launch({"evaluator": "mock"}, {})
            launch.assert_not_called()

    def test_launch_fails_closed_after_unproven_cadence_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            job_id = "range_validation_unsafe"
            job_root = os.path.join(root, "range_validations", job_id)
            os.makedirs(job_root)
            validation_thread = threading.Thread(target=lambda: None)
            validation_thread.start()
            validation_thread.join()
            service.range_validations[job_id] = {
                "job_id": job_id, "job_root": job_root, "state": "failed",
                "unsafe_to_optimize": True,
            }
            service.range_validation_cancels[job_id] = threading.Event()
            service.range_validation_threads[job_id] = validation_thread
            with mock.patch.object(service, "_launch_registered") as launch:
                with self.assertRaisesRegex(RuntimeError, "Cadence process cleanup unproven"):
                    service._launch({"evaluator": "mock"}, {})
            launch.assert_not_called()

    def test_no_validation_fast_path_does_not_enter_wait_clock(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        with mock.patch("crescendo_engine.server.time.monotonic") as monotonic:
            self.assertEqual(service._cancel_and_wait_range_validations(), [])
        monotonic.assert_not_called()

    def test_cancellation_wins_over_late_completed_update(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {
                "parameters": [{"parameter_id": "w", "optimizable": True}],
            }
            running = threading.Event()
            finish = threading.Event()

            def late_completion(_job, _prepared, _variables, _config, _cancel_event, update):
                update(state="running", message="running")
                running.set()
                finish.wait()
                update(state="completed", message="late completion")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=late_completion):
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                )
                self.assertTrue(running.wait(10))
                service.cancel_range_validations("prepared-1")
                finish.set()
                thread = service.range_validation_threads[response["job_id"]]
                thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertEqual(service.range_validation_status(response["job_id"])["state"], "cancelled")

    def test_range_validation_keeps_cross_process_optimization_lease_independent(self):
        with tempfile.TemporaryDirectory() as root:
            held = RuntimeOptimizationLease(root, run_root=os.path.join(root, "optimization")).acquire()
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {
                "parameters": [{"parameter_id": "w", "optimizable": True}],
            }
            try:
                with mock.patch("crescendo_engine.server.run_range_validation") as validation:
                    response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                    )
                    service.range_validation_threads[response["job_id"]].join(1)
                validation.assert_called_once()
                self.assertTrue(held.held)
            finally:
                held.release()

    def test_range_validation_lease_blocks_other_service_and_releases_on_exit(self):
        with tempfile.TemporaryDirectory() as root:
            first = OptimizerService({"runtime": {"run_root": root}})
            second = OptimizerService({"runtime": {"run_root": root}})
            prepared = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
            first.prepared["prepared-1"] = prepared
            second.prepared["prepared-2"] = prepared
            started = threading.Event()

            def hold_validation(_job, _prepared, _variables, _config, cancel_event, update):
                started.set()
                cancel_event.wait()
                update(state="cancelled", message="cancelled")

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=hold_validation):
                response = qualified_service(first, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                )
                self.assertTrue(started.wait(10))
                with self.assertRaisesRegex(
                    ContinuationLeaseBusy, "another optional PDK validation",
                ):
                    qualified_service(second, "prepared-2").validate_ranges("prepared-2", [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                    )
                first.cancel_range_validations("prepared-1")
                first.range_validation_threads[response["job_id"]].join(1)
            self.assertFalse(first.range_validation_threads[response["job_id"]].is_alive())
            available = RuntimeOptimizationLease(root).acquire()
            available.release()

    def test_resume_keeps_isolated_validation_while_acquiring_optimization_lease(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root, "range_validation_shutdown_timeout_seconds": 1,
            }})
            prepared = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
            service.prepared["prepared-1"] = prepared
            started = threading.Event()

            def hold_validation(_job, _prepared, _variables, _config, cancel_event, update):
                started.set()
                cancel_event.wait()
                update(state="cancelled", message="cancelled")

            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {"maestro_path": "/saved/maestro", "continuation_id": "resume-order"},
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {"evaluator": "real", "variables": [], "objectives": [], "constraints": []},
                "candidates": [], "state": {},
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)

            def launch_registered(_spec, _prepared, **kwargs):
                self.assertTrue(validation_thread.is_alive())
                self.assertFalse(service.range_validation_cancels[response["job_id"]].is_set())
                self.assertTrue(kwargs["runtime_optimization_lease"].held)
                kwargs["runtime_optimization_lease"].release()
                kwargs["continuation_lease"].release()
                return {"run_id": "resumed"}

            with mock.patch("crescendo_engine.server.run_range_validation", side_effect=hold_validation):
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{"parameter_id": "w", "minimum": 1, "maximum": 2}],
                )
                validation_thread = service.range_validation_threads[response["job_id"]]
                self.assertTrue(started.wait(10))
                with mock.patch.object(service, "prepare", return_value=("prepared-2", {})), \
                        mock.patch("crescendo_engine.server.validate_checkpoint"), \
                        mock.patch.object(service, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}), \
                        mock.patch.object(service, "_launch_registered", side_effect=launch_registered):
                    result = service.resume(checkpoint)
                service.cancel_range_validations("prepared-1")
                validation_thread.join(1)
            self.assertEqual(result["run_id"], "resumed")

    def test_runtime_root_allows_only_one_real_eda_optimization_across_services(self):
        with tempfile.TemporaryDirectory() as root:
            held = RuntimeOptimizationLease(root, run_root=os.path.join(root, "first")).acquire()
            service = OptimizerService({"runtime": {"run_root": root}})
            try:
                with mock.patch.object(service, "_launch_registered") as launch:
                    with self.assertRaisesRegex(
                        ContinuationLeaseBusy, "another real EDA optimization.*runtime_root",
                    ):
                        service._launch({"evaluator": "real"}, {})
                launch.assert_not_called()
                self.assertEqual(service.optimization_launches, 0)
            finally:
                held.release()

    def test_design_config_load_reprepares_and_returns_missing_items(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            prepared = {
                "parameters": [{"parameter_id": "x", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
                "setup": {"corners": [{"name": "tt"}]},
            }
            document = {
                "schema_version": "crescendo.design_config.v1", "maestro": {"path": "/saved/maestro"},
                "optimization": {"variables": [{"parameter_id": "missing"}], "objectives": [],
                                 "constraints": [], "run": {}},
            }
            with mock.patch.object(service, "prepare", return_value=("prepared-1", prepared)) as prepare_mock:
                payload = service.load_design_config(document, path="/new/maestro")
            prepare_mock.assert_called_once_with("/new/maestro")
            self.assertFalse(payload["resolution"]["compatible"])
            self.assertEqual(payload["resolution"]["missing"]["parameters"], ["missing"])

    def test_design_config_load_preserves_adexl_backend(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            prepared = {
                "parameters": [], "outputs": [],
                "setup": {"backend": "adexl", "corners": []},
            }
            document = {
                "schema_version": "crescendo.design_config.v1",
                "setup_locator": {"backend": "adexl", "path": "/saved/data.sdb"},
                "optimization": {
                    "variables": [], "objectives": [], "constraints": [], "run": {},
                },
            }
            with mock.patch.object(
                    service, "prepare", return_value=("prepared-1", prepared),
            ) as prepare_mock:
                payload = service.load_design_config(document)
            prepare_mock.assert_called_once_with("/saved/data.sdb", backend="adexl")
            self.assertEqual(payload["prepared"]["setup"]["backend"], "adexl")

    def test_resume_reprepares_and_launches_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            prepared = {
                "parameters": [{"parameter_id": "x", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1", "created_at": "now",
                "manifest": {"maestro_path": "/saved/maestro"},
                "parameter_ids": ["x"],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {"evaluator": "mock", "variables": [{"parameter_id": "x"}],
                         "objectives": [{"output_key": "main::gain"}], "constraints": []},
                "candidates": [], "state": {},
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            events = []
            with mock.patch.object(service, "prepare", return_value=("prepared-1", prepared)), \
                    mock.patch("crescendo_engine.server.validate_checkpoint", side_effect=lambda *_: events.append("validated")), \
                    mock.patch.object(
                        service, "_cleanup_checkpoint_origin",
                        side_effect=lambda *_, **kwargs: events.append(
                            "inspected" if kwargs.get("inspect_only") else "cleaned"
                        ) or {"state": "skipped"},
                    ), \
                    mock.patch.object(service, "_launch", side_effect=lambda *args, **kwargs: (
                        events.append("launched"), kwargs["continuation_lease"].release(),
                        {"run_id": "resumed"}
                    )[-1]) as launch_mock:
                payload = service.resume(checkpoint)
            self.assertEqual(payload["run_id"], "resumed")
            self.assertEqual(payload["prepared_id"], "prepared-1")
            self.assertIs(launch_mock.call_args[1]["checkpoint"], checkpoint)
            self.assertIn("continuation_lease", launch_mock.call_args[1])
            self.assertIsNone(launch_mock.call_args[1]["runtime_optimization_lease"])
            self.assertEqual(events, ["inspected", "validated", "cleaned", "launched"])

    def test_resume_reprepares_adexl_checkpoint_with_its_backend(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            prepared = {"parameters": [], "outputs": [], "setup": {"backend": "adexl"}}
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1", "created_at": "now",
                "manifest": {
                    "setup_locator": {"backend": "adexl", "path": "/saved/data.sdb"},
                },
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {
                    "evaluator": "mock", "variables": [], "objectives": [],
                    "constraints": [],
                },
                "candidates": [], "state": {},
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)

            def launch(_spec, _prepared, **kwargs):
                kwargs["continuation_lease"].release()
                return {"run_id": "resumed-adexl"}

            with mock.patch.object(
                    service, "prepare", return_value=("prepared-1", prepared),
            ) as prepare_mock, mock.patch(
                    "crescendo_engine.server.validate_checkpoint",
            ), mock.patch.object(
                    service, "_cleanup_checkpoint_origin", return_value={"state": "skipped"},
            ), mock.patch.object(service, "_launch", side_effect=launch):
                payload = service.resume(checkpoint)
            prepare_mock.assert_called_once_with("/saved/data.sdb", backend="adexl", initialize_validation=False)
            self.assertEqual(payload["run_id"], "resumed-adexl")

    def test_cli_resume_terminates_orphan_before_launch_and_waits_for_terminal(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            self._write_origin_status(origin, continuation_id="cli-resume")
            controller = self._make_controller_epoch(origin, with_liveness=True)
            controller_lock = open(controller["bootstrap"]["liveness_lock_path"], "r+")
            fcntl.flock(controller_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {
                    "maestro_path": "/saved/maestro",
                    "origin_run_root": origin,
                    "origin_run_id": "origin",
                    "continuation_id": "cli-resume",
                },
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {
                    "evaluator": "mock", "generations": 1, "population": 1,
                    "variables": [], "objectives": [], "constraints": [],
                },
                "candidates": [],
                "state": {
                    "next_generation": 0, "active_plan": None,
                    "checkpoint_revision": 0,
                },
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            checkpoint_path = os.path.join(root, "resume.json")
            with open(checkpoint_path, "w") as stream:
                json.dump(checkpoint, stream)

            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            service = OptimizerService(config)
            events = []
            terminated = {"value": False}
            runner_started = threading.Event()
            release_runner = threading.Event()
            runner_finished = threading.Event()
            cli_outcome = {}
            project_root = os.path.dirname(os.path.dirname(__file__))
            live_snapshot = {
                "pid": 920001, "alive": True, "state": "S", "start_ticks": 45670,
                "pgid": 920001, "cwd": os.path.realpath(project_root),
                "command": controller["command"],
            }

            def process_snapshot(pid):
                if pid == 920001 and not terminated["value"]:
                    return dict(live_snapshot)
                return {"pid": pid, "alive": False}

            def terminate_controller(inspection, _timeout):
                events.append("orphan_terminated")
                terminated["value"] = True
                fcntl.flock(controller_lock.fileno(), fcntl.LOCK_UN)
                controller_lock.close()
                return {
                    "state": "dead", "slot_index": inspection["slot_index"],
                    "epoch_index": inspection["epoch_index"],
                    "status": inspection.get("status"), "pid": inspection["pid"],
                    "cleanup_supported": True,
                    "proof": {
                        "ok": True, "kind": "controller_termination_proven",
                        "pid": inspection["pid"], "pgid": inspection["pgid"],
                        "pid_start_ticks": inspection["start_ticks"], "signals": [],
                    },
                }

            def prepare(path, initialize_validation=None):
                events.append("prepared")
                self.assertEqual(path, "/override/maestro")
                self.assertIs(initialize_validation, False)
                return "prepared-cli", {}

            def run_coordinated(_spec, _config, **kwargs):
                events.append("runner_started")
                self.assertFalse(os.path.exists(controller["epoch"]))
                runner_started.set()
                self.assertTrue(release_runner.wait(10))
                status_path = os.path.join(kwargs["run_root"], "status.json")
                with open(status_path, "w") as stream:
                    json.dump({
                        "schema_version": "crescendo.run_status.v1",
                        "run_id": os.path.basename(kwargs["run_root"]),
                        "run_root": kwargs["run_root"], "state": "completed",
                    }, stream)
                with open(os.path.join(kwargs["run_root"], "result.json"), "w") as stream:
                    json.dump({"state": "completed", "candidates": []}, stream)
                events.append("runner_finished")
                runner_finished.set()
                return {"state": "completed"}

            def invoke_cli():
                try:
                    cli_outcome["returncode"] = optimizer_main([
                        "--config", os.path.join(root, "config.json"),
                        "resume", checkpoint_path,
                        "--maestro-path", "/override/maestro",
                    ])
                except BaseException as error:
                    cli_outcome["error"] = error

            with mock.patch(
                "crescendo_engine.__main__.OptimizerService", return_value=service,
            ), mock.patch(
                "crescendo_engine.__main__.load_config", return_value=config,
            ), mock.patch(
                "crescendo_engine.__main__.run_optimization",
            ) as direct_runner, mock.patch(
                "crescendo_engine.__main__.print",
            ), mock.patch.object(
                service, "prepare", side_effect=prepare,
            ), mock.patch(
                "crescendo_engine.server.validate_checkpoint",
            ), mock.patch(
                "crescendo_engine.server._current_boot_id",
                return_value=self._CURRENT_BOOT_ID,
            ), mock.patch(
                "crescendo_engine.server._process_snapshot",
                side_effect=process_snapshot,
            ), mock.patch(
                "crescendo_engine.server._terminate_owned_controller",
                side_effect=terminate_controller,
            ), mock.patch(
                "crescendo_engine.server._run_coordinated_resume",
                side_effect=run_coordinated,
            ):
                cli_thread = threading.Thread(target=invoke_cli)
                cli_thread.start()
                self.assertTrue(runner_started.wait(10), cli_outcome)
                self.assertTrue(cli_thread.is_alive())
                self.assertFalse(runner_finished.is_set())
                release_runner.set()
                cli_thread.join(2)

            self.assertFalse(cli_thread.is_alive())
            self.assertNotIn("error", cli_outcome)
            self.assertEqual(cli_outcome["returncode"], 0)
            direct_runner.assert_not_called()
            self.assertEqual(
                events, ["orphan_terminated", "prepared", "runner_started", "runner_finished"],
            )

    def test_cli_resume_returns_failure_and_interrupt_waits_for_cleanup(self):
        checkpoint = {"schema_version": "crescendo.checkpoint.v1"}
        launch = {"run_id": "resume-1", "run_root": "/tmp/resume-1"}
        cleanup = {
            "exiting": True, "stopped_runs": [], "validation_session_closed": True,
        }
        failed = {"status": {"state": "failed"}, "result": None}
        completed = {"status": {"state": "completed"}, "result": {"state": "completed"}}
        stopped = {"status": {"state": "stopped"}, "result": {"state": "stopped"}}

        failed_service = mock.Mock()
        failed_service.resume.return_value = launch
        failed_service.wait_for_terminal.return_value = failed
        failed_service.exit_optimizer.return_value = cleanup
        with mock.patch(
            "crescendo_engine.__main__.OptimizerService", return_value=failed_service,
        ), mock.patch(
            "crescendo_engine.__main__.load_config", return_value={"runtime": {}},
        ), mock.patch(
            "crescendo_engine.__main__.load_json", return_value=checkpoint,
        ), mock.patch(
            "crescendo_engine.__main__.print",
        ):
            returncode = optimizer_main(["resume", "/tmp/checkpoint.json"])
        self.assertEqual(returncode, 1)
        failed_service.stop.assert_not_called()
        failed_service.exit_optimizer.assert_called_once_with()

        interrupted_service = mock.Mock()
        interrupted_service.resume.return_value = launch
        interrupted_service.wait_for_terminal.side_effect = [KeyboardInterrupt(), stopped]
        interrupted_service.exit_optimizer.return_value = cleanup
        with mock.patch(
            "crescendo_engine.__main__.OptimizerService", return_value=interrupted_service,
        ), mock.patch(
            "crescendo_engine.__main__.load_config", return_value={"runtime": {}},
        ), mock.patch(
            "crescendo_engine.__main__.load_json", return_value=checkpoint,
        ), mock.patch(
            "crescendo_engine.__main__.print",
        ):
            returncode = optimizer_main(["resume", "/tmp/checkpoint.json"])
        self.assertEqual(returncode, 0)
        interrupted_service.stop.assert_called_once_with("resume-1")
        self.assertEqual(
            interrupted_service.wait_for_terminal.call_args_list,
            [mock.call("resume-1"), mock.call("resume-1")],
        )
        interrupted_service.exit_optimizer.assert_called_once_with()

        double_interrupted_service = mock.Mock()
        double_interrupted_service.resume.return_value = launch
        double_interrupted_service.wait_for_terminal.side_effect = [
            KeyboardInterrupt(), KeyboardInterrupt(),
        ]
        double_interrupted_service.exit_optimizer.return_value = cleanup
        with mock.patch(
            "crescendo_engine.__main__.OptimizerService",
            return_value=double_interrupted_service,
        ), mock.patch(
            "crescendo_engine.__main__.load_config", return_value={"runtime": {}},
        ), mock.patch(
            "crescendo_engine.__main__.load_json", return_value=checkpoint,
        ), mock.patch(
            "crescendo_engine.__main__.print",
        ) as printed:
            returncode = optimizer_main(["resume", "/tmp/checkpoint.json"])
        self.assertEqual(returncode, 130)
        double_interrupted_service.stop.assert_called_once_with("resume-1")
        payload = json.loads(printed.call_args[0][0])
        self.assertTrue(payload["checkpoint_resumable"])
        self.assertFalse(payload["cleanup_confirmed"])
        self.assertEqual(payload["state"], "interrupted")
        self.assertEqual(payload["optimizer_cleanup"], cleanup)
        double_interrupted_service.exit_optimizer.assert_called_once_with()

        raced_service = mock.Mock()
        raced_service.resume.return_value = launch
        raced_service.wait_for_terminal.side_effect = [KeyboardInterrupt(), stopped]
        raced_service.stop.side_effect = ValueError("run is not active in this optimizer service")
        raced_service.exit_optimizer.return_value = cleanup
        with mock.patch(
            "crescendo_engine.__main__.OptimizerService", return_value=raced_service,
        ), mock.patch(
            "crescendo_engine.__main__.load_config", return_value={"runtime": {}},
        ), mock.patch(
            "crescendo_engine.__main__.load_json", return_value=checkpoint,
        ), mock.patch(
            "crescendo_engine.__main__.print",
        ):
            returncode = optimizer_main(["resume", "/tmp/checkpoint.json"])
        self.assertEqual(returncode, 0)
        self.assertEqual(raced_service.wait_for_terminal.call_count, 2)
        raced_service.exit_optimizer.assert_called_once_with()

        cleanup_failed_service = mock.Mock()
        cleanup_failed_service.resume.return_value = launch
        cleanup_failed_service.wait_for_terminal.return_value = completed
        cleanup_failed_service.exit_optimizer.side_effect = RuntimeError("cleanup failed")
        with mock.patch(
            "crescendo_engine.__main__.OptimizerService", return_value=cleanup_failed_service,
        ), mock.patch(
            "crescendo_engine.__main__.load_config", return_value={"runtime": {}},
        ), mock.patch(
            "crescendo_engine.__main__.load_json", return_value=checkpoint,
        ), mock.patch(
            "crescendo_engine.__main__.print",
        ) as printed:
            returncode = optimizer_main(["resume", "/tmp/checkpoint.json"])
        self.assertEqual(returncode, 1)
        payload = json.loads(printed.call_args[0][0])
        self.assertFalse(payload["cleanup_confirmed"])
        self.assertIn("cleanup failed", payload["cleanup_error"])

        wait_failed_service = mock.Mock()
        wait_failed_service.resume.return_value = launch
        wait_failed_service.wait_for_terminal.side_effect = RuntimeError("status unreadable")
        wait_failed_service.exit_optimizer.return_value = cleanup
        with mock.patch(
            "crescendo_engine.__main__.OptimizerService", return_value=wait_failed_service,
        ), mock.patch(
            "crescendo_engine.__main__.load_config", return_value={"runtime": {}},
        ), mock.patch(
            "crescendo_engine.__main__.load_json", return_value=checkpoint,
        ), mock.patch(
            "crescendo_engine.__main__.print",
        ):
            with self.assertRaisesRegex(RuntimeError, "status unreadable"):
                optimizer_main(["resume", "/tmp/checkpoint.json"])
        wait_failed_service.exit_optimizer.assert_called_once_with()

    def test_cross_service_double_resume_is_rejected_while_first_continuation_is_held(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {"maestro_path": "/saved/maestro", "continuation_id": "same-run"},
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {"evaluator": "mock", "variables": [],
                                                  "objectives": [], "constraints": []},
                "candidates": [], "state": {},
            }
            first = OptimizerService({"runtime": {"run_root": root}})
            second = OptimizerService({"runtime": {"run_root": root}})
            held = []

            def launch(_spec, _prepared, **kwargs):
                held.append(kwargs["continuation_lease"])
                return {"run_id": "first"}

            patches = [
                mock.patch.object(first, "prepare", return_value=("p1", {})),
                mock.patch.object(second, "prepare", return_value=("p2", {})),
                mock.patch("crescendo_engine.server.validate_checkpoint"),
                mock.patch.object(first, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}),
                mock.patch.object(second, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}),
                mock.patch.object(first, "_launch", side_effect=launch),
            ]
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                first.resume(checkpoint)
                try:
                    with self.assertRaisesRegex(ContinuationLeaseBusy, "already running"):
                        second.resume(checkpoint)
                finally:
                    held[0].release()

    def test_double_resume_is_rejected_before_second_prepare(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {"maestro_path": "/saved/maestro", "continuation_id": "same-run"},
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {"evaluator": "mock", "variables": [], "objectives": [], "constraints": []},
                "candidates": [], "state": {},
            }
            first = OptimizerService({"runtime": {"run_root": root}})
            second = OptimizerService({"runtime": {"run_root": root}})
            held = []

            def launch(_spec, _prepared, **kwargs):
                held.append(kwargs["continuation_lease"])
                return {"run_id": "first"}

            with mock.patch.object(first, "prepare", return_value=("p1", {})) as first_prepare, \
                    mock.patch.object(second, "prepare", return_value=("p2", {})) as second_prepare, \
                    mock.patch("crescendo_engine.server.validate_checkpoint"), \
                    mock.patch.object(first, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}), \
                    mock.patch.object(first, "_launch", side_effect=launch):
                first.resume(checkpoint)
                try:
                    with self.assertRaisesRegex(ContinuationLeaseBusy, "already running"):
                        second.resume(checkpoint)
                finally:
                    held[0].release()
            first_prepare.assert_called_once_with("/saved/maestro", initialize_validation=False)
            second_prepare.assert_not_called()

    def test_resume_passes_both_held_leases_to_runner_without_self_conflict(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {"maestro_path": "/saved/maestro", "continuation_id": "resume-real"},
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {"evaluator": "real", "variables": [], "objectives": [], "constraints": []},
                "candidates": [], "state": {},
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch.object(service, "prepare", return_value=("prepared", {})), \
                    mock.patch("crescendo_engine.server.validate_checkpoint"), \
                    mock.patch.object(service, "_cleanup_checkpoint_origin", return_value={"state": "skipped"}), \
                    mock.patch("crescendo_engine.runner._run_optimization_unleased", return_value={"state": "completed"}):
                result = service.resume(checkpoint)
                service.threads[result["run_id"]].join(timeout=10)
            self.assertFalse(service.threads[result["run_id"]].is_alive())
            self.assertFalse(os.path.exists(os.path.join(result["run_root"], "server_error.json")))
            continuation = ContinuationLease(root, "resume-real").acquire()
            try:
                runtime = RuntimeOptimizationLease(root).acquire()
                runtime.release()
            finally:
                continuation.release()

    def test_resume_does_not_cleanup_when_checkpoint_validation_fails(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {"maestro_path": "/m"},
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {
                    "evaluator": "mock", "variables": [],
                    "objectives": [], "constraints": [],
                },
                "candidates": [], "state": {},
            }
            with mock.patch.object(service, "prepare", return_value=("prepared-1", {})), \
                    mock.patch("crescendo_engine.server.validate_checkpoint", side_effect=ValueError("bad checkpoint")), \
                    mock.patch.object(service, "_cleanup_checkpoint_origin") as cleanup_mock:
                with self.assertRaisesRegex(ValueError, "bad checkpoint"):
                    service.resume(checkpoint)
            cleanup_mock.assert_called_once_with(
                checkpoint, terminate_owned=True, inspect_only=True,
            )
            self.assertFalse(os.path.lexists(continuation_checkpoint_path(
                root, checkpoint_continuation_id(checkpoint), create=False,
            )))

    def test_resume_cleanup_rejects_existing_origin_outside_runtime_root(self):
        with tempfile.TemporaryDirectory() as runtime_root, tempfile.TemporaryDirectory() as outside:
            origin = os.path.join(outside, "origin")
            os.makedirs(origin)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": runtime_root}})
            with self.assertRaisesRegex(RuntimeError, "exists outside"):
                service._cleanup_checkpoint_origin(checkpoint)

    def test_resume_cleanup_skips_unavailable_portable_origin(self):
        with tempfile.TemporaryDirectory() as runtime_root:
            origin = os.path.join(os.path.dirname(runtime_root), "portable-origin-missing")
            if os.path.exists(origin):
                self.fail("portable origin fixture unexpectedly exists")
            checkpoint = {
                "manifest": {"origin_run_root": origin, "origin_run_id": "portable-origin-missing"},
            }
            service = OptimizerService({"runtime": {"run_root": runtime_root}})
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["state"], "skipped")
            self.assertIn("unavailable", result["reason"])

    def test_resume_cleanup_rejects_runtime_origin_through_symlink_alias(self):
        with tempfile.TemporaryDirectory() as parent:
            runtime_root = os.path.join(parent, "runtime")
            origin = os.path.join(runtime_root, "origin")
            os.makedirs(origin)
            alias = os.path.join(parent, "runtime-alias")
            os.symlink(runtime_root, alias)
            checkpoint = {
                "manifest": {
                    "origin_run_root": os.path.join(alias, "origin"),
                    "origin_run_id": "origin",
                },
            }
            service = OptimizerService({"runtime": {"run_root": runtime_root}})
            with self.assertRaisesRegex(RuntimeError, "symbolic-link alias"):
                service._cleanup_checkpoint_origin(checkpoint)

    def test_abandoned_completed_candidate_requires_completion_evidence_preservation(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(os.path.join(candidate, "workspace"))
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({"candidate_id": "g000_c0000", "status": "completed"}, stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server.cleanup_terminal_candidate_artifacts") as cleanup_mock:
                result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["cleaned"], ["g000_c0000"])
            required = cleanup_mock.call_args[1]["required_lightweight_paths"]
            self.assertEqual(required, [
                os.path.join(candidate, "candidate.json"),
                os.path.join(candidate, "completion_evidence.json"),
            ])

    def test_resume_cleanup_rejects_active_origin_thread(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            active = mock.Mock()
            active.is_alive.return_value = True
            service.threads["origin"] = active
            with mock.patch("crescendo_engine.server.cleanup_abandoned_worker_epoch") as worker_cleanup:
                with self.assertRaisesRegex(RuntimeError, "origin optimization is still running"):
                    service._cleanup_checkpoint_origin(checkpoint)
            worker_cleanup.assert_not_called()

    def test_resume_cleanup_rejects_live_owned_controller(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            controller = self._make_controller_epoch(origin)
            project_root = os.path.dirname(os.path.dirname(__file__))
            snapshot = {
                "pid": 920001, "alive": True, "state": "S", "start_ticks": 45670,
                "pgid": 920001, "cwd": os.path.realpath(project_root),
                "command": controller["command"],
            }
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot", return_value=snapshot), \
                    mock.patch("crescendo_engine.server.cleanup_abandoned_worker_epoch") as worker_cleanup:
                with self.assertRaisesRegex(RuntimeError, "controller is still alive"):
                    service._cleanup_checkpoint_origin(checkpoint)
            worker_cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(controller["epoch"]))

    def test_owned_controller_termination_is_group_scoped_and_proven(self):
        process = subprocess.Popen(["/bin/sleep", "30"], start_new_session=True)
        try:
            snapshot = _process_snapshot(process.pid)
            result = _terminate_owned_controller({
                "state": "active", "identity_proven": True,
                "pid": process.pid, "pgid": process.pid,
                "start_ticks": snapshot["start_ticks"],
                "slot_index": 0, "epoch_index": 0,
                "cleanup_supported": True,
            }, 2.0)
            process.wait(timeout=2)
            self.assertEqual(result["state"], "dead")
            self.assertEqual(result["proof"]["kind"], "controller_termination_proven")
            self.assertFalse(_process_snapshot(process.pid).get("alive"))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_resume_cleanup_rejects_live_controller_identity_mismatch(self):
        mutations = {
            "pgid": {"pgid": 920002},
            "cwd": {"cwd": "/tmp"},
            "command": {"command": [sys.executable, "-m", "unrelated", "--bootstrap", "/tmp/x"]},
        }
        for name, updates in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                origin = os.path.join(root, "origin")
                os.makedirs(origin)
                controller = self._make_controller_epoch(origin)
                project_root = os.path.dirname(os.path.dirname(__file__))
                snapshot = {
                    "pid": 920001, "alive": True, "state": "S", "start_ticks": 45670,
                    "pgid": 920001, "cwd": os.path.realpath(project_root),
                    "command": controller["command"],
                }
                snapshot.update(updates)
                checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
                service = OptimizerService({"runtime": {"run_root": root}})
                with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                        mock.patch("crescendo_engine.server._process_snapshot", return_value=snapshot):
                    with self.assertRaisesRegex(RuntimeError, "identity is ambiguous"):
                        service._cleanup_checkpoint_origin(checkpoint)

    def test_resume_cleanup_treats_pid_start_tick_reuse_as_proven_dead(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            controller = self._make_controller_epoch(origin)
            project_root = os.path.dirname(os.path.dirname(__file__))
            snapshot = {
                "pid": 920001, "alive": True, "state": "S", "start_ticks": 45671,
                "pgid": 920001, "cwd": os.path.realpath(project_root),
                "command": controller["command"],
            }
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot", return_value=snapshot):
                result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(
                result["retained_controllers"][0]["proof"]["kind"],
                "recorded_pid_was_reused",
            )

    def test_resume_cleanup_retains_proven_dead_controller_without_blocking(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            controller = self._make_controller_epoch(origin, status="processing")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot", return_value={
                        "pid": 920001, "alive": False,
                    }):
                first = service._cleanup_checkpoint_origin(checkpoint)
                second = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(first["state"], "completed")
            self.assertEqual(first["retained_controllers"][0]["controller_epoch"], "slot_000/epoch_000")
            self.assertEqual(first["retained_controllers"][0]["proof"]["kind"], "recorded_pid_is_dead")
            self.assertEqual(second["retained_controllers"], first["retained_controllers"])
            self.assertTrue(os.path.isdir(controller["epoch"]))

    def test_resume_cleanup_removes_proven_dead_controller_with_liveness_lock(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            controller = self._make_controller_epoch(
                origin, status="processing", with_liveness=True,
            )
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot", return_value={
                        "pid": 920001, "alive": False,
                    }):
                result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["cleaned_controllers"], ["slot_000/epoch_000"])
            self.assertFalse(os.path.exists(controller["epoch"]))

    def test_resume_cleanup_removes_partial_controller_epoch_after_released_lock(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            epoch = os.path.join(origin, "controllers", "slot_000", "epoch_000")
            os.makedirs(epoch)
            with open(os.path.join(epoch, "controller_liveness.lock"), "a"):
                pass
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["cleaned_controllers"], ["slot_000/epoch_000"])
            self.assertFalse(os.path.exists(epoch))

    def test_resume_cleanup_rejects_partial_controller_with_held_lock(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            epoch = os.path.join(origin, "controllers", "slot_000", "epoch_000")
            os.makedirs(epoch)
            lock_path = os.path.join(epoch, "controller_liveness.lock")
            descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            try:
                with mock.patch("crescendo_engine.server._matching_controller_processes", return_value=[]):
                    with self.assertRaisesRegex(RuntimeError, "identity is ambiguous"):
                        service._cleanup_checkpoint_origin(checkpoint)
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_accepts_previous_boot_controller_without_pid_probe(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            self._make_controller_epoch(origin, launch_boot_id=self._PREVIOUS_BOOT_ID)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot") as snapshot:
                result = service._cleanup_checkpoint_origin(checkpoint)
            snapshot.assert_not_called()
            self.assertEqual(
                result["retained_controllers"][0]["proof"]["kind"],
                "prior_boot_processes_dead",
            )

    def test_resume_cleanup_rejects_current_boot_controller_without_pid_identity(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            controller = self._make_controller_epoch(origin, status="launch_intent")
            controller["state"].update({"pid": None, "pgid": None, "pid_start_ticks": None})
            with open(controller["state_path"], "w") as stream:
                json.dump(controller["state"], stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot") as snapshot:
                with self.assertRaisesRegex(RuntimeError, "identity is ambiguous"):
                    service._cleanup_checkpoint_origin(checkpoint)
            snapshot.assert_not_called()

    def test_resume_cleanup_rejects_malformed_controller_state_and_bootstrap(self):
        mutations = {
            "schema": lambda controller: controller["state"].update({"schema_version": "unknown"}),
            "slot": lambda controller: controller["state"].update({"worker_index": True}),
            "epoch": lambda controller: controller["bootstrap"].update({"controller_epoch": 2}),
            "token": lambda controller: controller["state"].update({"token": "not-a-token"}),
            "run_root": lambda controller: controller["state"].update({"run_root": "/tmp/other"}),
            "boot_id": lambda controller: controller["state"].update({"launch_boot_id": "unknown"}),
            "start_ticks": lambda controller: controller["state"].update({"pid_start_ticks": 0}),
            "isolated_pgid": lambda controller: controller["state"].update({"pgid": 920002}),
            "command": lambda controller: controller["state"].update({"command": ["python", "bad"]}),
            "requests_path": lambda controller: controller["bootstrap"].update({"requests_root": "/tmp"}),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                origin = os.path.join(root, "origin")
                os.makedirs(origin)
                controller = self._make_controller_epoch(origin)
                mutate(controller)
                with open(controller["state_path"], "w") as stream:
                    json.dump(controller["state"], stream)
                with open(controller["bootstrap_path"], "w") as stream:
                    json.dump(controller["bootstrap"], stream)
                checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
                service = OptimizerService({"runtime": {"run_root": root}})
                with mock.patch("crescendo_engine.server._process_snapshot") as snapshot:
                    with self.assertRaises(RuntimeError):
                        service._cleanup_checkpoint_origin(checkpoint)
                snapshot.assert_not_called()

    def test_resume_cleanup_rejects_unrecognized_controller_namespace_paths(self):
        for relative in ("controllers/not_a_slot", "controllers/slot_000/not_an_epoch"):
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as root:
                origin = os.path.join(root, "origin")
                os.makedirs(os.path.join(origin, relative))
                checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
                service = OptimizerService({"runtime": {"run_root": root}})
                with self.assertRaisesRegex(RuntimeError, "unrecognized controller"):
                    service._cleanup_checkpoint_origin(checkpoint)

    def test_resume_cleanup_rejects_controller_namespace_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            outside = os.path.join(root, "outside-controllers")
            os.makedirs(outside)
            os.symlink(outside, os.path.join(origin, "controllers"))
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with self.assertRaisesRegex(RuntimeError, "controller root"):
                service._cleanup_checkpoint_origin(checkpoint)

    def test_resume_cleanup_removes_dead_owned_worker_epoch(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, _state = self._make_worker_epoch(origin)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})

            result = service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(result["cleaned_workers"], ["worker_000/epoch_000"])
            self.assertFalse(os.path.exists(epoch))

    def test_durable_worker_launch_lock_blocks_cleanup_until_released_double_scan(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, _control, descriptor, _manifest = (
                self._make_durable_worker_prestate(origin)
            )
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({
                "runtime": {"run_root": root, "resume_worker_cleanup_timeout_seconds": 0.01},
            })
            try:
                inspection = _inspect_worker_epoch_processes(epoch, 0, 0)
                self.assertEqual(inspection["state"], "active")
                self.assertTrue(inspection["launch_handshake"])
                self.assertEqual(
                    inspection["termination_proof"]["kind"], "launch_in_progress",
                )
                with mock.patch(
                    "crescendo_engine.server.cleanup_abandoned_worker_epoch",
                ) as worker_cleanup:
                    with self.assertRaisesRegex(RuntimeError, "workers are still alive"):
                        service._cleanup_checkpoint_origin(checkpoint)
                worker_cleanup.assert_not_called()
            finally:
                close_legacy_guardian_launch_lock(descriptor)

            inspection = _inspect_worker_epoch_processes(epoch, 0, 0)
            self.assertEqual(inspection["state"], "abandoned")
            self.assertEqual(
                inspection["termination_proof"], {"ok": True, "kind": "ocean_not_spawned"},
            )
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["cleaned_workers"], ["worker_000/epoch_000"])
            self.assertFalse(os.path.exists(epoch))

    def test_resume_cleanup_recovers_released_namespaced_epoch_after_abnormal_launch_exit(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch_name = "epoch_c000001_l000000"
            epoch_identity = "worker-000-controller-000001-epoch-000000"
            epoch, _workspace, _control, descriptor, _manifest = (
                self._make_durable_worker_prestate(
                    origin, epoch_name=epoch_name, epoch_identity=epoch_identity,
                )
            )
            close_legacy_guardian_launch_lock(descriptor)

            inspection = _inspect_worker_epoch_processes(epoch, 0, 0)
            self.assertEqual(inspection["state"], "abandoned")
            self.assertEqual(inspection["controller_epoch"], 1)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            result = service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(
                result["cleaned_workers"], ["worker_000/%s" % epoch_name],
            )
            self.assertFalse(os.path.exists(epoch))

    def test_worker_epoch_uses_production_hashed_control_root_and_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, _state = self._make_worker_epoch(origin)
            worker_path = os.path.join(epoch, "worker_state.json")
            worker = load_json(worker_path)
            expected = persistent_worker_control_root(epoch, "worker-000-epoch-000")
            self.assertEqual(worker["control_root"], expected)
            self.assertRegex(
                os.path.basename(os.path.dirname(expected)),
                r"^worker-000-epoch-000_[0-9a-f]{8}$",
            )
            self.assertEqual(
                _inspect_worker_epoch_processes(epoch, 0, 0)["state"], "abandoned",
            )
            worker["control_root"] = os.path.join(
                epoch, "epochs", "worker-000-epoch-000", "control",
            )
            with open(worker_path, "w") as stream:
                json.dump(worker, stream)
            with self.assertRaisesRegex(RuntimeError, "control root does not match"):
                _inspect_worker_epoch_processes(epoch, 0, 0)

    def test_worker_epoch_fallback_rejects_unexpected_symlink_child(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            outside = os.path.join(root, "outside")
            os.makedirs(origin)
            os.makedirs(outside)
            epoch, _workspace, _state = self._make_worker_epoch(origin)
            worker_path = os.path.join(epoch, "worker_state.json")
            worker = load_json(worker_path)
            worker.pop("guardian_state_path")
            with open(worker_path, "w") as stream:
                json.dump(worker, stream)
            os.symlink(outside, os.path.join(epoch, "epochs", "unexpected"))
            with self.assertRaisesRegex(RuntimeError, "unexpected child"):
                _inspect_worker_epoch_processes(epoch, 0, 0)

    def test_worker_epoch_from_prelock_protocol_launching_without_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, guardian_state_path = self._make_worker_epoch(
                origin, guardian_status="launching", guardian_pid=None, ocean_pid=None,
            )
            os.unlink(guardian_state_path)
            worker_path = os.path.join(epoch, "worker_state.json")
            worker = load_json(worker_path)
            worker.update({
                "state": "launching", "pid": None, "pgid": None,
                "ocean_pid": None, "ocean_pgid": None,
            })
            with open(worker_path, "w") as stream:
                json.dump(worker, stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                "crescendo_engine.server.cleanup_abandoned_worker_epoch",
            ) as worker_cleanup:
                with self.assertRaisesRegex(RuntimeError, "termination is not proven"):
                    service._cleanup_checkpoint_origin(checkpoint)
            worker_cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_rejects_unrecognized_worker_namespace_paths(self):
        for relative in (
            "workers/not_a_worker", "workers/worker_0/epoch_000",
            "workers/worker_000/not_an_epoch", "workers/worker_000/epoch_c1_l0",
            "workers/worker_000/epoch_c000000_l000000",
        ):
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as root:
                origin = os.path.join(root, "origin")
                os.makedirs(os.path.join(origin, relative))
                checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
                service = OptimizerService({"runtime": {"run_root": root}})
                with self.assertRaisesRegex(RuntimeError, "unrecognized worker"):
                    service._cleanup_checkpoint_origin(checkpoint)

    def test_resume_cleanup_removes_partial_worker_before_ocean_spawn(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            epoch = os.path.join(origin, "workers", "worker_000", "epoch_000")
            os.makedirs(epoch)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["cleaned_workers"], ["worker_000/epoch_000"])
            self.assertFalse(os.path.exists(epoch))

    def test_resume_cleanup_fails_closed_when_dead_worker_cleanup_fails(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, _state = self._make_worker_epoch(origin)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                "crescendo_engine.server.cleanup_abandoned_worker_epoch",
                side_effect=RuntimeError("cleanup refused"),
            ), mock.patch("crescendo_engine.server.cleanup_terminal_candidate_artifacts") as candidate_cleanup:
                with self.assertRaisesRegex(RuntimeError, "worker cleanup failed"):
                    service._cleanup_checkpoint_origin(checkpoint)
            candidate_cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(epoch))

    def test_dead_controller_does_not_bypass_unsafe_worker_proof(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            self._make_controller_epoch(origin)
            epoch, _workspace, guardian_state_path = self._make_worker_epoch(
                origin, guardian_status="cleanup_failed",
            )
            guardian = load_json(guardian_state_path)
            guardian.update({
                "group_cleanup_complete": False,
                "descendant_cleanup_complete": False,
                "descendant_supervision_complete": True,
                "remaining_group_pids": [910003],
                "remaining_adopted_pids": [],
            })
            with open(guardian_state_path, "w") as stream:
                json.dump(guardian, stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value=self._CURRENT_BOOT_ID), \
                    mock.patch("crescendo_engine.server._process_snapshot", side_effect=[
                        {"pid": 920001, "alive": False},
                        {"pid": 910001, "alive": False},
                        {"pid": 910002, "alive": False},
                    ]):
                with self.assertRaisesRegex(RuntimeError, "termination is not proven"):
                    service._cleanup_checkpoint_origin(checkpoint)
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_rejects_live_owned_guardian_before_deleting_anything(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, workspace, guardian_state_path = self._make_worker_epoch(origin, guardian_status="running")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            guardian_script = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "tools", "ocean_worker_guardian.py",
            )
            guardian = {
                "pid": 910001, "alive": True, "state": "S", "cwd": os.path.realpath(workspace),
                "pgid": 910001,
                "command": [
                    sys.executable, guardian_script, "--state-path", guardian_state_path,
                    "--token", "worker-000-epoch-000-worker-0", "--cwd", workspace,
                    "--", "/opt/eda/bin/ocean", "-nograph",
                ],
            }
            ocean = {
                "pid": 910002, "alive": True, "state": "S", "cwd": os.path.realpath(workspace),
                "pgid": 910002, "command": ["/opt/eda/bin/ocean", "-nograph"],
            }
            with mock.patch("crescendo_engine.server._process_snapshot", side_effect=[guardian, ocean]), \
                    mock.patch("crescendo_engine.server.cleanup_abandoned_worker_epoch") as worker_cleanup:
                with self.assertRaisesRegex(RuntimeError, "workers are still alive"):
                    service._cleanup_checkpoint_origin(checkpoint)
            worker_cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_recovers_sigkilled_scoped_guardian_and_detached_helper(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            ocean_script = os.path.join(root, "stubborn_ocean.py")
            helper_script = os.path.join(root, "detached_helper.py")
            pids_path = os.path.join(root, "owned_pids.txt")
            with open(helper_script, "w") as stream:
                stream.write(
                    "import os,signal,time\n"
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                    "with open(os.environ['CRESCENDO_SERVER_ORPHAN_PIDS'], 'a') as f:\n"
                    " f.write('helper %d\\n' % os.getpid()); f.flush(); os.fsync(f.fileno())\n"
                    "while True: time.sleep(1)\n"
                )
            with open(ocean_script, "w") as stream:
                stream.write(
                    "import os,signal,subprocess,sys,time\n"
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                    "helper=subprocess.Popen([sys.executable, sys.argv[1]], start_new_session=True)\n"
                    "with open(os.environ['CRESCENDO_SERVER_ORPHAN_PIDS'], 'a') as f:\n"
                    " f.write('ocean %d\\n' % os.getpid()); f.flush(); os.fsync(f.fileno())\n"
                    "while True: time.sleep(1)\n"
                )
            os.chmod(helper_script, 0o755)
            os.chmod(ocean_script, 0o755)
            environment = dict(os.environ)
            environment["CRESCENDO_SERVER_ORPHAN_PIDS"] = pids_path
            command = [sys.executable, ocean_script, helper_script]
            epoch, workspace, control, descriptor, manifest = self._make_durable_worker_prestate(
                origin, scope_token="a" * 32, ocean_command=command,
            )
            guardian = None
            unrelated = None
            owned_pids = []

            def process_running(pid):
                try:
                    with open("/proc/%d/stat" % pid, "r") as stream:
                        state = stream.read().split()[2]
                    return state != "Z"
                except (IOError, OSError, IndexError):
                    return False

            try:
                guardian = subprocess.Popen(
                    manifest["guardian_argv"], stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    cwd=workspace, env=environment, start_new_session=True,
                    pass_fds=(descriptor,),
                )
                close_legacy_guardian_launch_lock(descriptor)
                descriptor = None
                state_path = os.path.join(control, "guardian_state.json")
                deadline = time.time() + 5
                guardian_state = None
                while time.time() < deadline:
                    if os.path.isfile(state_path):
                        guardian_state = load_json(state_path)
                        if guardian_state.get("status") == "running" and os.path.isfile(pids_path):
                            with open(pids_path) as stream:
                                rows = [line.split() for line in stream if line.strip()]
                            if {row[0] for row in rows} == {"ocean", "helper"}:
                                owned_pids = [int(row[1]) for row in rows]
                                break
                    time.sleep(0.02)
                self.assertIsNotNone(guardian_state)
                self.assertEqual(len(owned_pids), 2)
                self.assertIn(int(guardian_state["pid"]), owned_pids)
                worker_path = os.path.join(epoch, "worker_state.json")
                worker_state = load_json(worker_path)
                worker_state.update({
                    "state": "running", "pid": guardian.pid,
                    "pgid": os.getpgid(guardian.pid),
                    "ocean_pid": guardian_state["pid"],
                    "ocean_pgid": guardian_state["pgid"],
                    "guardian_state_token": guardian_state["token"],
                })
                with open(worker_path, "w") as stream:
                    json.dump(worker_state, stream)
                unrelated_environment = dict(os.environ)
                unrelated_environment.pop("CRESCENDO_LEGACY_GUARDIAN_TOKEN", None)
                unrelated = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    cwd=workspace, env=unrelated_environment, start_new_session=True,
                )
                os.kill(guardian.pid, signal.SIGKILL)
                self.assertEqual(guardian.wait(timeout=2), -signal.SIGKILL)
                guardian.stdin.close()
                checkpoint = {
                    "manifest": {
                        "origin_run_root": origin,
                        "origin_run_id": "origin",
                    },
                }
                service = OptimizerService({
                    "runtime": {
                        "run_root": root,
                        "resume_worker_cleanup_timeout_seconds": 0.01,
                    },
                })
                result = service._cleanup_checkpoint_origin(
                    checkpoint, terminate_owned=True,
                )
                self.assertEqual(result["cleaned_workers"], ["worker_000/epoch_000"])
                self.assertFalse(os.path.exists(epoch))
                deadline = time.time() + 3
                while time.time() < deadline and any(process_running(pid) for pid in owned_pids):
                    time.sleep(0.02)
                self.assertFalse(any(process_running(pid) for pid in owned_pids))
                self.assertIsNone(unrelated.poll())
            finally:
                if descriptor is not None:
                    close_legacy_guardian_launch_lock(descriptor)
                if guardian is not None:
                    if guardian.stdin and not guardian.stdin.closed:
                        guardian.stdin.close()
                    if guardian.poll() is None:
                        try:
                            os.kill(guardian.pid, signal.SIGCONT)
                        except OSError:
                            pass
                        guardian.kill()
                        guardian.wait()
                for pid in owned_pids:
                    if process_running(pid):
                        try:
                            os.killpg(pid, signal.SIGKILL)
                        except OSError:
                            pass
                if unrelated is not None and unrelated.poll() is None:
                    os.killpg(unrelated.pid, signal.SIGKILL)
                    unrelated.wait()

    def test_resume_cleanup_rejects_ambiguous_live_pid_without_signalling_it(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, workspace, _state = self._make_worker_epoch(origin, guardian_status="running")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            unrelated = {
                "pid": 910001, "alive": True, "state": "S", "cwd": os.path.realpath(workspace),
                "pgid": 910001, "command": ["/usr/bin/sleep", "999"],
            }
            dead = {"pid": 910002, "alive": False}
            with mock.patch("crescendo_engine.server._process_snapshot", side_effect=[unrelated, dead]), \
                    mock.patch("crescendo_engine.server.os.kill") as kill, \
                    mock.patch("crescendo_engine.server.os.killpg") as killpg:
                with self.assertRaisesRegex(RuntimeError, "cannot be proven"):
                    service._cleanup_checkpoint_origin(checkpoint)
            kill.assert_not_called()
            killpg.assert_not_called()
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_rejects_dead_worker_without_guardian_cleanup_proof(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, guardian_state_path = self._make_worker_epoch(
                origin, guardian_status="cleanup_failed",
            )
            guardian = load_json(guardian_state_path)
            guardian.update({
                "group_cleanup_complete": False,
                "descendant_cleanup_complete": False,
                "descendant_supervision_complete": True,
                "remaining_group_pids": [910003],
                "remaining_adopted_pids": [910004],
            })
            with open(guardian_state_path, "w") as stream:
                json.dump(guardian, stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})

            with mock.patch("crescendo_engine.server.cleanup_abandoned_worker_epoch") as worker_cleanup:
                with self.assertRaisesRegex(RuntimeError, "termination is not proven"):
                    service._cleanup_checkpoint_origin(checkpoint)
            worker_cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_accepts_structurally_valid_worker_from_previous_boot(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, _state = self._make_worker_epoch(
                origin, guardian_status="running", launch_boot_id="previous-boot",
            )
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value="current-boot"), \
                    mock.patch("crescendo_engine.server._process_snapshot") as snapshot:
                result = service._cleanup_checkpoint_origin(checkpoint)
            snapshot.assert_not_called()
            self.assertEqual(result["cleaned_workers"], ["worker_000/epoch_000"])
            self.assertFalse(os.path.exists(epoch))

    def test_resume_cleanup_accepts_legacy_boot_id_alias_from_previous_boot(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, guardian_path = self._make_worker_epoch(
                origin, guardian_status="running", launch_boot_id="previous-boot",
            )
            guardian = load_json(guardian_path)
            guardian["boot_id"] = guardian.pop("launch_boot_id")
            with open(guardian_path, "w") as stream:
                json.dump(guardian, stream)
            worker_path = os.path.join(epoch, "worker_state.json")
            worker = load_json(worker_path)
            worker["boot_id"] = worker.pop("launch_boot_id")
            with open(worker_path, "w") as stream:
                json.dump(worker, stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value="current-boot"), \
                    mock.patch("crescendo_engine.server._process_snapshot") as snapshot:
                result = service._cleanup_checkpoint_origin(checkpoint)
            snapshot.assert_not_called()
            self.assertEqual(result["cleaned_workers"], ["worker_000/epoch_000"])
            self.assertFalse(os.path.exists(epoch))

    def test_resume_cleanup_without_boot_id_remains_fail_closed_after_restart(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            os.makedirs(origin)
            epoch, _workspace, _state = self._make_worker_epoch(origin, guardian_status="running")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch("crescendo_engine.server._current_boot_id", return_value="current-boot"):
                with self.assertRaisesRegex(RuntimeError, "termination is not proven"):
                    service._cleanup_checkpoint_origin(checkpoint)
            self.assertTrue(os.path.isdir(epoch))

    def test_resume_cleanup_scans_terminal_candidate_without_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "status": "failed",
                    "source_guard": {"ok": True},
                }, stream)
            with open(os.path.join(candidate, "raw.log"), "w") as stream:
                stream.write("raw")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})

            result = service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(result["cleaned"], ["g000_c0000"])
            self.assertFalse(os.path.exists(os.path.join(candidate, "raw.log")))
            self.assertTrue(os.path.isfile(os.path.join(candidate, "candidate.json")))

    def test_resume_cleanup_removes_nonterminal_candidate_after_process_proof(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({"candidate_id": "g000_c0000", "status": "running"}, stream)
            with open(os.path.join(candidate, "raw.log"), "w") as stream:
                stream.write("raw")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})

            result = service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(result["cleaned_nonterminal"], ["g000_c0000"])
            self.assertFalse(os.path.exists(candidate))

    def test_resume_cleanup_recovers_exact_terminal_wal_into_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            _origin, candidate, checkpoint = self._make_terminal_wal_checkpoint(root)
            service = OptimizerService({"runtime": {"run_root": root}})
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["recovered_terminal_wal"], ["g000_c0000"])
            self.assertTrue(result["checkpoint_updated"])
            self.assertEqual(checkpoint_candidates(checkpoint)[0]["status"], "completed")
            self.assertFalse(os.path.exists(os.path.join(candidate, "raw.log")))

    def test_recovered_terminal_wal_is_persisted_before_any_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            origin, candidate, checkpoint = self._make_terminal_wal_checkpoint(root)
            service = OptimizerService({"runtime": {"run_root": root}})
            observed = []

            def assert_persisted(label):
                persisted = load_json(os.path.join(origin, "checkpoint.json"))
                self.assertEqual(
                    checkpoint_candidates(persisted)[0]["candidate_id"], "g000_c0000",
                )
                observed.append(label)

            def resume_pending(run_root):
                self.assertEqual(run_root, origin)
                assert_persisted("pending")
                return []

            def cleanup_terminal(*_args, **_kwargs):
                assert_persisted("terminal")
                return {"ok": True}

            with mock.patch(
                "crescendo_engine.server.resume_pending_cleanups", side_effect=resume_pending,
            ), mock.patch(
                "crescendo_engine.server.cleanup_terminal_candidate_artifacts",
                side_effect=cleanup_terminal,
            ):
                result = service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(observed, ["pending", "terminal"])
            self.assertEqual(
                result["reconciled_checkpoint_path"], os.path.join(origin, "checkpoint.json"),
            )
            self.assertTrue(os.path.isfile(os.path.join(candidate, "raw.log")))

    def test_recovered_terminal_wal_persist_failure_preserves_wal_and_input(self):
        with tempfile.TemporaryDirectory() as root:
            origin, candidate, checkpoint = self._make_terminal_wal_checkpoint(root)
            original = json.loads(json.dumps(checkpoint))
            service = OptimizerService({"runtime": {"run_root": root}})

            with mock.patch(
                "crescendo_engine.server.atomic_write_json",
                side_effect=OSError("injected checkpoint persistence failure"),
            ), mock.patch(
                "crescendo_engine.server.resume_pending_cleanups",
            ) as resume_pending, mock.patch(
                "crescendo_engine.server.cleanup_terminal_candidate_artifacts",
            ) as cleanup_terminal:
                with self.assertRaisesRegex(OSError, "checkpoint persistence failure"):
                    service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(checkpoint, original)
            self.assertTrue(os.path.isfile(os.path.join(candidate, "candidate.json")))
            self.assertTrue(os.path.isfile(os.path.join(candidate, "completion_evidence.json")))
            self.assertTrue(os.path.isfile(os.path.join(candidate, "raw.log")))
            self.assertFalse(os.path.lexists(os.path.join(origin, ".cleanup_pending")))
            resume_pending.assert_not_called()
            cleanup_terminal.assert_not_called()

    def test_old_checkpoint_repeats_recovery_after_first_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            origin, candidate, checkpoint = self._make_terminal_wal_checkpoint(root)
            old_checkpoint = json.loads(json.dumps(checkpoint))
            service = OptimizerService({"runtime": {"run_root": root}})

            first = json.loads(json.dumps(old_checkpoint))
            first_result = service._cleanup_checkpoint_origin(first)
            self.assertEqual(first_result["recovered_terminal_wal"], ["g000_c0000"])
            self.assertFalse(os.path.exists(os.path.join(candidate, "raw.log")))

            second = json.loads(json.dumps(old_checkpoint))
            second_result = service._cleanup_checkpoint_origin(second)
            self.assertEqual(second_result["recovered_terminal_wal"], [])
            self.assertEqual(
                second_result["checkpoint_selection"]["state"],
                "origin_reconciliation_promoted",
            )
            self.assertEqual(checkpoint_candidates(second)[0]["status"], "completed")
            persisted = load_json(os.path.join(origin, "checkpoint.json"))
            self.assertEqual(checkpoint_candidates(persisted)[0]["status"], "completed")

    def test_resume_cleanup_finishes_interrupted_terminal_candidate_transaction(self):
        with tempfile.TemporaryDirectory() as root:
            origin, candidate, checkpoint = self._make_terminal_wal_checkpoint(root)
            workspace = os.path.join(candidate, "workspace")
            os.makedirs(workspace)
            with open(os.path.join(workspace, "workspace.json"), "w") as stream:
                json.dump({"root": workspace, "source_libraries": {}}, stream)
            with open(os.path.join(candidate, "timing.json"), "w") as stream:
                json.dump({"phases": {}}, stream)
            service = OptimizerService({"runtime": {"run_root": root}})

            def interrupt(payload_root, *_args, **_kwargs):
                os.unlink(os.path.join(payload_root, "workspace", "workspace.json"))
                raise OSError("injected terminal candidate cleanup interruption")

            with mock.patch(
                "crescendo_engine.server.verify_sources_unchanged",
                return_value={"ok": True},
            ), mock.patch(
                "crescendo_engine.retention.shutil.rmtree", side_effect=interrupt,
            ):
                with self.assertRaisesRegex(RuntimeError, "candidate cleanup failed"):
                    service._cleanup_checkpoint_origin(checkpoint)

            pending_root = os.path.join(origin, ".cleanup_pending")
            self.assertTrue(os.path.isdir(pending_root))
            self.assertEqual(
                sorted(os.listdir(candidate)),
                ["candidate.json", "completion_evidence.json", "timing.json"],
            )
            self.assertEqual(len(checkpoint_candidates(checkpoint)), 1)

            second = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(second["recovered_terminal_wal"], [])
            self.assertEqual(
                second["resumed_pending_cleanup"][0]["cleanup_kind"],
                "terminal_candidate_artifacts",
            )
            self.assertEqual(second["retained_lightweight"], ["g000_c0000"])
            self.assertFalse(os.path.lexists(pending_root))
            self.assertEqual(len(checkpoint_candidates(checkpoint)), 1)
            self.assertEqual(checkpoint_candidates(checkpoint)[0]["status"], "completed")

            third = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(third["resumed_pending_cleanup"], [])
            self.assertEqual(third["recovered_terminal_wal"], [])
            self.assertEqual(third["retained_lightweight"], ["g000_c0000"])
            persisted = load_json(os.path.join(origin, "checkpoint.json"))
            self.assertEqual(len(checkpoint_candidates(persisted)), 1)
            self.assertEqual(checkpoint_candidates(persisted)[0]["status"], "completed")

    def test_resume_cleanup_continues_interrupted_candidate_quarantine(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({"candidate_id": "g000_c0000", "status": "running"}, stream)
            with open(os.path.join(candidate, "raw.log"), "w") as stream:
                stream.write("raw")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})

            def interrupt(payload_root, *_args, **_kwargs):
                os.unlink(os.path.join(payload_root, "raw.log"))
                raise OSError("injected candidate cleanup interruption")

            with mock.patch(
                "crescendo_engine.retention.shutil.rmtree", side_effect=interrupt,
            ):
                with self.assertRaisesRegex(RuntimeError, "candidate cleanup failed"):
                    service._cleanup_checkpoint_origin(checkpoint)

            self.assertFalse(os.path.exists(candidate))
            self.assertTrue(os.path.isdir(os.path.join(origin, ".cleanup_pending")))
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(
                result["resumed_pending_cleanup"][0]["cleanup_kind"],
                "nonterminal_candidate",
            )
            self.assertFalse(os.path.exists(os.path.join(origin, ".cleanup_pending")))

    def test_resume_cleanup_does_not_recover_terminal_wal_with_wrong_values(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            self._write_origin_status(origin)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                    "values": {"x": 9.0}, "status": "failed",
                    "source_guard": {"ok": True},
                    "metrics": {
                        "feasible": False, "total_violation": 1.0, "merit": 1e30,
                        "penalized": True, "objectives": [], "constraints": [],
                    },
                }, stream)
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {"origin_run_root": origin, "origin_run_id": "origin"},
                "parameter_ids": ["x"],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {
                    "evaluator": "mock", "population": 1,
                    "variables": [{"parameter_id": "x"}],
                    "objectives": [], "constraints": [],
                },
                "candidates": [],
                "state": {
                    "next_generation": 0,
                    "active_plan": {
                        "generation": 0, "next_index": 1, "replacements": 0,
                        "planned": [[0, [1.25]]],
                    },
                },
            }
            service = OptimizerService({"runtime": {"run_root": root}})
            result = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(result["recovered_terminal_wal"], [])
            self.assertFalse(result["checkpoint_updated"])
            self.assertEqual(checkpoint_candidates(checkpoint), [])

    def test_resume_cleanup_keeps_checkpoint_seeded_lightweight_candidate_repeatable(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({"candidate_id": "g000_c0000", "status": "failed"}, stream)
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            first = service._cleanup_checkpoint_origin(checkpoint)
            second = service._cleanup_checkpoint_origin(checkpoint)
            self.assertEqual(first["retained_lightweight"], ["g000_c0000"])
            self.assertEqual(second["retained_lightweight"], ["g000_c0000"])
            self.assertTrue(os.path.isfile(os.path.join(candidate, "candidate.json")))

    def test_resume_cleanup_reclaims_checkpointed_proven_controller_failure(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "status": "infrastructure_failed",
                    "failure_class": "persistent_controller",
                    "persistent_worker": {"controller_termination": {
                        "termination_proven": True, "process_still_alive": False,
                        "controller_pid": 123, "controller_returncode": -9,
                        "cleanup": {"ok": True, "removed": True},
                    }},
                }, stream)
            with open(os.path.join(candidate, "timing.json"), "w") as stream:
                json.dump({"phases": {}}, stream)
            with open(os.path.join(candidate, "schematic_application.records"), "w") as stream:
                stream.write("raw")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})

            result = service._cleanup_checkpoint_origin(checkpoint)

            self.assertEqual(result["cleaned"], ["g000_c0000"])
            self.assertEqual(
                sorted(os.listdir(candidate)), ["candidate.json", "timing.json"],
            )
            persisted = load_json(os.path.join(candidate, "candidate.json"))
            self.assertNotIn("source_guard", persisted)
            self.assertTrue(persisted["persistent_worker"]["controller_termination"][
                "termination_proven"
            ])

    def test_resume_cleanup_fails_closed_on_terminal_candidate_cleanup_error(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "status": "failed",
                    "source_guard": {"ok": True},
                }, stream)
            with open(os.path.join(candidate, "raw.log"), "w") as stream:
                stream.write("raw")
            checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "origin"}}
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                "crescendo_engine.server.cleanup_terminal_candidate_artifacts",
                side_effect=RuntimeError("cleanup refused"),
            ):
                with self.assertRaisesRegex(RuntimeError, "candidate cleanup failed"):
                    service._cleanup_checkpoint_origin(checkpoint)

    def test_single_checkpoint_resume_reclaims_dead_origin_then_launches(self):
        with tempfile.TemporaryDirectory() as root:
            origin = os.path.join(root, "origin")
            candidate = os.path.join(origin, "candidates", "g000_c0000")
            os.makedirs(candidate)
            epoch, _workspace, _state = self._make_worker_epoch(origin)
            self._write_origin_status(origin)
            with open(os.path.join(candidate, "candidate.json"), "w") as stream:
                json.dump({
                    "candidate_id": "g000_c0000", "status": "failed",
                    "source_guard": {"ok": True},
                }, stream)
            with open(os.path.join(candidate, "raw.log"), "w") as stream:
                stream.write("raw")
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {
                    "maestro_path": "/portable/maestro", "origin_run_root": origin,
                    "origin_run_id": "origin",
                },
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "candidates": [], "state": {},
                "spec": {"evaluator": "mock", "variables": [], "objectives": [], "constraints": []},
            }
            service = OptimizerService({"runtime": {"run_root": root}})

            def launch_resume(*_args, **kwargs):
                kwargs["continuation_lease"].release()
                return {"run_id": "resumed"}

            with mock.patch.object(service, "prepare", return_value=("prepared-1", {})), \
                    mock.patch("crescendo_engine.server.validate_checkpoint"), \
                    mock.patch.object(service, "_launch", side_effect=launch_resume) as launch:
                result = service.resume(checkpoint)

            self.assertEqual(result["run_id"], "resumed")
            self.assertEqual(result["abandoned_cleanup"]["cleaned_workers"], ["worker_000/epoch_000"])
            self.assertEqual(result["abandoned_cleanup"]["cleaned"], ["g000_c0000"])
            self.assertFalse(os.path.exists(epoch))
            self.assertFalse(os.path.exists(os.path.join(candidate, "raw.log")))
            launch.assert_called_once_with(
                checkpoint["spec"], {}, checkpoint=checkpoint,
                continuation_lease=mock.ANY,
                runtime_optimization_lease=None, run_config=mock.ANY, tb_optimization_lease=None,
            )

    def test_progress_restores_candidates_from_checkpoint_without_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-checkpoint")
            os.makedirs(run_root)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump({"state": "running"}, stream)
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1", "parameter_ids": ["x"],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {"variables": [{"parameter_id": "x"}],
                         "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                         "constraints": []},
                "candidates": [["g000_c0000", 0, 0, "completed", [1.25],
                                [True, 0.0, -1.25, False, "", [[1.25, -1.25, False]], []],
                                None, "", None, None]],
                "state": {},
            }
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump(checkpoint, stream)
            service = OptimizerService({"runtime": {"run_root": root}})
            progress = service.progress("run-checkpoint")
            self.assertEqual(progress["candidates"][0]["values"], {"x": 1.25})
            self.assertEqual(progress["candidates"][0]["metrics"]["objectives"][0]["value"], 1.25)

    def test_two_resume_cycles_preserve_all_history_for_progress_checkpoint_and_csv(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "maestro_path": "/portable/design/maestro",
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}], "corners": []},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True,
                                "hierarchy_path": "/DUT/M1", "name": "width"}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            spec = {
                "evaluator": "mock", "generations": 2, "population": 4, "jobs": 1, "seed": 9,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize"}],
                "constraints": [],
            }
            first_root = os.path.join(root, "first")
            first_result = run_optimization(spec, config, prepared=prepared, run_root=first_root)
            first_export = checkpoint_with_candidates(
                load_json(os.path.join(first_root, "checkpoint.json")), first_result["candidates"],
            )
            self.assertEqual(len(first_export["candidates"]), 8)

            second_root = os.path.join(root, "second")
            _coordinated_runner_resume(
                spec, config, prepared, second_root, first_export,
            )
            service = OptimizerService(config)
            second_progress = service.progress("second")
            second_export = service.export_checkpoint("second")
            self.assertEqual(len(second_progress["candidates"]), 8)
            self.assertEqual(len(checkpoint_candidates(second_export)), 8)
            self.assertIn("variable:/DUT/M1/width", service.candidates_csv("second").splitlines()[0])
            self.assertEqual(len(service.candidates_csv("second").splitlines()), 9)

            third_root = os.path.join(root, "third")
            _coordinated_runner_resume(
                spec, config, prepared, third_root, second_export,
            )
            third_progress = service.progress("third")
            third_export = service.export_checkpoint("third")
            self.assertEqual(len(third_progress["candidates"]), 8)
            self.assertEqual(len(checkpoint_candidates(third_export)), 8)

    def test_candidates_csv_http_endpoint_downloads_current_run(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-1")
            candidate_root = os.path.join(run_root, "candidates", "g000_c0000")
            os.makedirs(candidate_root)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump({"state": "completed"}, stream)
            with open(os.path.join(run_root, "spec.json"), "w") as stream:
                json.dump({"variable_labels": {"x": "/DUT/M1/width"}}, stream)
            with open(os.path.join(candidate_root, "candidate.json"), "w") as stream:
                json.dump({"candidate_id": "g000_c0000", "generation": 0, "index": 0,
                           "status": "completed", "values": {"x": 1.2},
                           "metrics": {"feasible": True, "objectives": [], "constraints": []}}, stream)
            service = OptimizerService({"runtime": {"run_root": root}})
            server = HTTPServer(("127.0.0.1", 0), make_handler(service))
            thread = threading.Thread(target=server.serve_forever)
            thread.daemon = True
            thread.start()
            try:
                response = urlopen("http://127.0.0.1:%d/api/candidates.csv?run_id=run-1" % server.server_port)
                payload = response.read().decode("utf-8")
                self.assertIn('attachment; filename="run-1-candidates.csv"', response.headers["Content-Disposition"])
                self.assertIn("variable:/DUT/M1/width", payload.splitlines()[0])
                self.assertEqual(len(payload.splitlines()), 2)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(2)

    def test_ui_binds_export_csv_button_to_current_run_endpoint(self):
        web_root = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web")
        with open(os.path.join(web_root, "index.html"), "rb") as stream:
            html = stream.read().decode("utf-8")
        with open(os.path.join(web_root, "app.js"), "rb") as stream:
            script = stream.read().decode("utf-8")
        self.assertIn('id="exportCsvButton"', html)
        self.assertIn("/api/candidates.csv?run_id=", script)
        self.assertIn("$('exportCsvButton').onclick=exportCandidatesCsv", script)

    def test_ui_exposes_folded_algorithm_selector_and_persists_settings(self):
        web_root = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web")
        with open(os.path.join(web_root, "index.html"), "rb") as stream:
            html = stream.read().decode("utf-8")
        with open(os.path.join(web_root, "app.js"), "rb") as stream:
            script = stream.read().decode("utf-8")
        self.assertIn('<details class="algorithm-settings"', html)
        self.assertIn('value="nsga2"', html)
        self.assertIn('value="differential_evolution"', html)
        self.assertIn('value="bayesian_optimization"', html)
        for field_id in ("bayesKernelLengthScale", "bayesMaxObservations", "deDifferentialWeight",
                         "nsgaMutationProbability"):
            self.assertIn('id="%s"' % field_id, html)
        self.assertIn("algorithm_parameters", script)
        self.assertIn("collectAlgorithmSettings()", script)
        self.assertIn("applyAlgorithmSettings(run.algorithm", script)

    def test_server_rejects_non_loopback_binding(self):
        with self.assertRaisesRegex(ValueError, "only binds to loopback"):
            serve({}, host="0.0.0.0", port=0)

    def test_eda_setup_exposes_discovery_without_full_config(self):
        service = OptimizerService({
            "runtime": {"run_root": _scratch_root(self)},
            "cadence": {
                "setup_mode": "auto",
                "user_config_path": "/tmp/crescendo.json",
                "_manual_overrides": {"model_roots": ["/portable/models"]},
                "virtuoso_bin": "/eda/virtuoso",
                "discovery": {"ok": False, "missing": ["spectre_bin"], "recommendations": ["Set Spectre Root."]},
            },
        })
        payload = service.eda_setup()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["missing"], ["spectre_bin"])
        self.assertEqual(payload["effective"]["virtuoso_bin"], "/eda/virtuoso")
        self.assertEqual(payload["manual"]["model_roots"], ["/portable/models"])
        self.assertNotIn("runtime", payload)

    @mock.patch("crescendo_engine.server.configure_eda")
    def test_update_eda_setup_uses_validated_result(self, configure_eda_mock):
        discovery = {
            "ok": True,
            "effective": {"virtuoso_bin": "/eda/bin/virtuoso", "pdk_adapter": "generic-cdf"},
            "sources": {}, "checks": [], "version_checks": [], "missing": [], "recommendations": [], "compatibility": {"ok": True},
        }
        configure_eda_mock.return_value = (discovery, "/tmp/user.json")
        service = OptimizerService({
            "runtime": {"run_root": _scratch_root(self)},
            "cadence": {
                "setup_mode": "auto", "user_config_path": "/tmp/user.json",
                "virtuoso_bin": "/stale/auto/virtuoso",
                "_file_overrides": {"search_roots": ["/portable/eda"]},
            },
        })
        service.prepared["prepared-1"] = {"setup": {}}
        manual = {"virtuoso_bin": "/eda/bin/virtuoso"}
        with mock.patch(
                "crescendo_engine.server.persist_eda_setup",
                return_value="/tmp/user.json",
        ) as persist:
            payload = service.update_eda_setup({"mode": "manual", "manual": manual})
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["mode"], "manual")
        self.assertTrue(payload["prepared_invalidated"])
        self.assertEqual(payload["manual"]["virtuoso_bin"], "/eda/bin/virtuoso")
        self.assertEqual(service.config["cadence"]["virtuoso_bin"], "/eda/bin/virtuoso")
        self.assertEqual(service.prepared, {})
        configure_eda_mock.assert_called_once()
        configure_kwargs = configure_eda_mock.call_args[1]
        self.assertFalse(configure_kwargs["persist"])
        self.assertNotEqual(
            configure_kwargs["base_cadence"].get("virtuoso_bin"),
            "/stale/auto/virtuoso",
        )
        self.assertEqual(
            configure_kwargs["base_cadence"]["search_roots"], ["/portable/eda"],
        )
        persist.assert_called_once_with(
            "manual", manual, discovery, path="/tmp/user.json",
        )

    @mock.patch("crescendo_engine.server.configure_eda")
    def test_update_eda_setup_is_rejected_while_optimization_is_active(
            self, configure_eda_mock):
        service = OptimizerService({
            "runtime": {"run_root": _scratch_root(self)},
            "cadence": {"setup_mode": "auto", "user_config_path": "/tmp/user.json"},
        })
        service.threads["active-run"] = mock.Mock(is_alive=mock.Mock(return_value=True))
        with self.assertRaisesRegex(RuntimeError, "while an optimization is active"):
            service.update_eda_setup({"mode": "auto"})
        configure_eda_mock.assert_not_called()

    @mock.patch("crescendo_engine.server.configure_eda")
    def test_update_eda_setup_rejects_invalid_mode_before_transaction(
            self, configure_eda_mock):
        service = OptimizerService({
            "runtime": {"run_root": _scratch_root(self)},
            "cadence": {"setup_mode": "auto", "user_config_path": "/tmp/user.json"},
        })
        with mock.patch.object(
                service, "_cancel_and_wait_range_validations",
        ) as cancel, mock.patch.object(
                service, "_close_resident_validation",
        ) as close, mock.patch(
                "crescendo_engine.server.persist_eda_setup",
        ) as persist:
            with self.assertRaisesRegex(ValueError, "mode must be auto or manual"):
                service.update_eda_setup({"mode": "automatic"})
        configure_eda_mock.assert_not_called()
        cancel.assert_not_called()
        close.assert_not_called()
        persist.assert_not_called()

    def test_update_eda_setup_keeps_project_file_override_authoritative(self):
        discovery = {
            "ok": True,
            "effective": {"license_file": "5280@project-license"},
            "sources": {}, "checks": [], "version_checks": [],
            "missing": [], "recommendations": [], "compatibility": {"ok": True},
        }
        service = OptimizerService({
            "runtime": {"run_root": _scratch_root(self)},
            "cadence": {
                "setup_mode": "manual", "user_config_path": "/tmp/user.json",
                "_file_overrides": {"license_file": "5280@project-license"},
                "_manual_overrides": {"license_file": "5280@old-user-license"},
            },
        })
        requested = {"license_file": "5280@new-user-license"}
        with mock.patch(
                "crescendo_engine.server.configure_eda",
                return_value=(discovery, "/tmp/user.json"),
        ) as configure, mock.patch(
                "crescendo_engine.server.persist_eda_setup",
                return_value="/tmp/user.json",
        ) as persist:
            payload = service.update_eda_setup({"mode": "manual", "manual": requested})
        self.assertEqual(
            configure.call_args[1]["manual"]["license_file"],
            "5280@project-license",
        )
        persist.assert_called_once_with(
            "manual", requested, discovery, path="/tmp/user.json",
        )
        self.assertEqual(payload["manual"], requested)
        self.assertEqual(payload["effective"]["license_file"], "5280@project-license")

    def test_update_eda_setup_does_not_commit_when_validation_cleanup_fails(self):
        old_cadence = {
            "setup_mode": "auto", "user_config_path": "/tmp/user.json",
            "virtuoso_bin": "/old/virtuoso", "_manual_overrides": {},
        }
        service = OptimizerService({
            "runtime": {"run_root": _scratch_root(self)},
            "cadence": copy.deepcopy(old_cadence),
        })
        prepared = {"setup": {}}
        service.prepared["prepared-1"] = prepared
        discovery = {
            "ok": True, "effective": {"virtuoso_bin": "/new/virtuoso"},
        }
        with mock.patch(
                "crescendo_engine.server.configure_eda",
                return_value=(discovery, "/tmp/user.json"),
        ), mock.patch.object(
                service, "_cancel_and_wait_range_validations",
                side_effect=RuntimeError("cleanup unproven"),
        ), mock.patch(
                "crescendo_engine.server.persist_eda_setup",
        ) as persist, mock.patch.object(service, "_close_resident_validation") as close:
            with self.assertRaisesRegex(RuntimeError, "cleanup unproven"):
                service.update_eda_setup({
                    "mode": "manual", "manual": {"virtuoso_bin": "/new/virtuoso"},
                })
        persist.assert_not_called()
        close.assert_not_called()
        self.assertEqual(service.config["cadence"], old_cadence)
        self.assertIs(service.prepared["prepared-1"], prepared)
        self.assertFalse(service.eda_reconfiguring)

    def test_browse_finds_maestro_directories(self):
        with tempfile.TemporaryDirectory() as root:
            maestro = os.path.join(root, "HH", "TB", "maestro")
            os.makedirs(maestro)
            with open(os.path.join(maestro, "maestro.sdb"), "w") as stream:
                stream.write("setup")
            service = OptimizerService({"runtime": {"run_root": root}})
            cell = service.browse(os.path.join(root, "HH", "TB"))
            self.assertEqual(cell["entries"][0]["path"], maestro)
            self.assertTrue(cell["entries"][0]["is_maestro"])
            selected = service.browse(os.path.join(maestro, "maestro.sdb"))
            self.assertEqual(selected["path"], maestro)
            self.assertTrue(selected["is_maestro"])

    def test_browse_finds_adexl_directories_and_reports_backend(self):
        with tempfile.TemporaryDirectory() as root:
            adexl = os.path.join(root, "HH", "TB", "adexl")
            os.makedirs(adexl)
            data_sdb = os.path.join(adexl, "data.sdb")
            with open(data_sdb, "w") as stream:
                stream.write("setup")
            service = OptimizerService({"runtime": {"run_root": root}})
            selected = service.browse(data_sdb)
            self.assertEqual(selected["path"], adexl)
            self.assertTrue(selected["is_setup"])
            self.assertFalse(selected["is_maestro"])
            self.assertEqual(selected["setup_backend"], "adexl")
            self.assertEqual(selected["setup_db"], data_sdb)

    @mock.patch("crescendo_engine.server.shutil.which", lambda name: "/usr/bin/" + name)
    @mock.patch("crescendo_engine.server._desktop_session")
    @mock.patch("crescendo_engine.server.subprocess.check_output")
    def test_native_browse_returns_selected_maestro_directory(self, check_output, desktop_session):
        with tempfile.TemporaryDirectory() as root:
            maestro = os.path.join(root, "HH", "TB", "maestro")
            os.makedirs(maestro)
            maestro_sdb = os.path.join(maestro, "maestro.sdb")
            with open(maestro_sdb, "w") as stream:
                stream.write("setup")
            desktop_session.return_value = ("designer", {"DISPLAY": ":0"})
            check_output.return_value = (maestro_sdb + "\n").encode("utf-8")
            service = OptimizerService({"runtime": {"run_root": root}})
            selected = service.native_browse(root)
            self.assertFalse(selected["cancelled"])
            self.assertEqual(selected["path"], maestro)
            self.assertIn("DISPLAY=:0", check_output.call_args[0][0])

    @mock.patch("crescendo_engine.server.shutil.which", lambda name: "/usr/bin/" + name)
    @mock.patch("crescendo_engine.server._desktop_session")
    @mock.patch("crescendo_engine.server.subprocess.check_output")
    def test_native_browse_returns_selected_adexl_directory(self, check_output, desktop_session):
        with tempfile.TemporaryDirectory() as root:
            adexl = os.path.join(root, "HH", "TB", "adexl")
            os.makedirs(adexl)
            data_sdb = os.path.join(adexl, "data.sdb")
            with open(data_sdb, "w") as stream:
                stream.write("setup")
            desktop_session.return_value = ("designer", {"DISPLAY": ":0"})
            check_output.return_value = (data_sdb + "\n").encode("utf-8")
            service = OptimizerService({"runtime": {"run_root": root}})
            selected = service.native_browse(root)
            self.assertFalse(selected["cancelled"])
            self.assertEqual(selected["path"], adexl)
            self.assertEqual(selected["setup_backend"], "adexl")
            self.assertIsNone(selected["maestro_sdb"])

    def test_progress_returns_live_candidate_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-1")
            candidate_root = os.path.join(run_root, "candidates", "g000_c0000")
            os.makedirs(candidate_root)
            payloads = {
                "status.json": {"state": "running", "completed_candidates": 1},
                "spec.json": {"objectives": [{"output_key": "test::gain"}], "constraints": []},
            }
            for name, payload in payloads.items():
                with open(os.path.join(run_root, name), "w") as stream:
                    json.dump(payload, stream)
            candidate = {
                "candidate_id": "g000_c0000",
                "generation": 0,
                "index": 0,
                "status": "completed",
                "metrics": {
                    "feasible": True, "total_violation": 0.0,
                    "objectives": [{"output_key": "test::gain", "value": 10.0, "score": -10.0}],
                    "constraints": [],
                },
            }
            with open(os.path.join(candidate_root, "candidate.json"), "w") as stream:
                json.dump(candidate, stream)
            service = OptimizerService({"runtime": {"run_root": root}})
            progress = service.progress("run-1")
            self.assertEqual(progress["status"]["state"], "running")
            self.assertEqual(progress["spec"]["objectives"][0]["output_key"], "test::gain")
            self.assertEqual(progress["candidates"][0]["metrics"]["objectives"][0]["value"], 10.0)
            self.assertEqual(
                progress["objective_presentation"]["pareto_candidate_ids"],
                ["g000_c0000"],
            )

    def test_start_rejects_static_range_errors_before_launch_registration(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {
                "parameters": [{
                    "parameter_id": "width-1", "optimizable": True,
                    "hierarchy_path": "/DUT/M1", "master_library": "pdk",
                    "master_cell": "nmos", "name": "w", "prompt": "Width",
                }],
            }
            spec = {
                "variables": [{
                    "parameter_id": "width-1", "minimum": "20n", "maximum": "20n",
                }],
                "objectives": [{"output_key": "test::gain", "direction": "maximize"}],
            }
            with mock.patch(
                    "crescendo_engine.server.require_prepared_freshness"), \
                    mock.patch.object(service, "_launch") as launch:
                with self.assertRaises(ValueError) as raised:
                    qualified_service(service, "prepared-1").start("prepared-1", spec)
            self.assertFalse(launch.called)
            self.assertTrue(raised.exception.issues)
            self.assertIn("/DUT/M1", raised.exception.issues[0]["message"])
            self.assertEqual(os.listdir(root), [])

    def test_static_range_report_aggregates_readable_device_errors(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {
                "parameters": [{
                    "parameter_id": "width-1", "optimizable": True,
                    "hierarchy_path": "/DUT/M1", "master_library": "pdk",
                    "master_cell": "nmos", "name": "w", "prompt": "Width",
                }],
            }
            with mock.patch("crescendo_engine.server.require_prepared_freshness"):
                report = qualified_service(service, "prepared-1").validate_static_ranges("prepared-1", [{
                    "parameter_id": "width-1", "minimum": "bad", "maximum": "10n",
                }])
            self.assertFalse(report["valid"])
            self.assertEqual(report["issues"][0]["hierarchy_path"], "/DUT/M1")
            self.assertEqual(report["issues"][0]["parameter_name"], "w")

    def test_background_exception_marks_status_failed(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                "crescendo_engine.server.run_optimization",
                side_effect=RuntimeError("injected background failure"),
            ):
                result = service._launch_registered(
                    {
                        "evaluator": "mock", "generations": 1, "population": 1,
                        "display_name": "  OPA AC optimization  ",
                    }, {},
                )
                service.threads[result["run_id"]].join(timeout=5)
            status = load_json(os.path.join(result["run_root"], "status.json"))
            self.assertEqual(status["state"], "failed")
            self.assertEqual(status["display_name"], "OPA AC optimization")
            self.assertNotEqual(status["run_id"], status["display_name"])
            self.assertIn("injected background failure", status["message"])
            self.assertTrue(status.get("finished_at"))

    def test_launch_rejects_unsafe_display_name_before_creating_run(self):
        for display_name in ("", "unsafe\nname", "x" * 121, 7):
            with self.subTest(display_name=display_name), tempfile.TemporaryDirectory() as root:
                service = OptimizerService({"runtime": {"run_root": root}})
                with self.assertRaisesRegex(ValueError, "display_name"):
                    service._launch_registered({
                        "evaluator": "mock", "generations": 1, "population": 1,
                        "display_name": display_name,
                    }, {})
                self.assertEqual(os.listdir(root), [])

    def test_list_runs_reports_safe_checkpoint_metadata_and_skips_symlink_runs(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-safe")
            os.makedirs(run_root)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump({
                    "schema_version": "crescendo.run_status.v1", "run_id": "run-safe",
                    "run_root": run_root, "state": "stopped",
                }, stream)
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {
                    "continuation_id": "list-continuation",
                    "display_name": "OPA corner sweep",
                },
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {
                    "evaluator": "mock", "variables": [], "objectives": [],
                    "constraints": [], "display_name": "OPA corner sweep",
                    "generations": 12, "population": 4,
                },
                "candidates": [],
                "state": {
                    "checkpoint_revision": 7, "generator": {},
                    "next_generation": 3,
                    "active_plan": {
                        "generation": 3, "next_index": 0,
                        "replacements": 0, "planned": [],
                    },
                    "completed_candidates": 8,
                    "failed_candidates": 2,
                    "infrastructure_failed_candidates": 1,
                },
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump(checkpoint, stream)
            os.symlink(run_root, os.path.join(root, "run-link"))

            service = OptimizerService({"runtime": {"run_root": root}})
            rows = service.list_runs()
            self.assertEqual([row["run_id"] for row in rows], ["run-safe"])
            self.assertTrue(rows[0]["checkpoint_available"])
            self.assertEqual(rows[0]["checkpoint_revision"], 7)
            self.assertEqual(rows[0]["continuation_id"], "list-continuation")
            self.assertEqual(rows[0]["display_name"], "OPA corner sweep")
            self.assertEqual(rows[0]["current_generation"], 4)
            self.assertEqual(rows[0]["total_generations"], 12)
            self.assertEqual(rows[0]["successful_candidates"], 8)
            self.assertEqual(rows[0]["simulation_failed_candidates"], 2)
            self.assertEqual(rows[0]["infrastructure_failed_candidates"], 1)
            self.assertEqual(rows[0]["success"], 8)
            self.assertEqual(rows[0]["simulation_fail"], 2)
            self.assertEqual(rows[0]["infrastructure_fail"], 1)
            self.assertFalse(rows[0]["active"])

    def test_run_access_rejects_escape_symlink_and_final_json_swap(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-safe")
            os.makedirs(run_root)
            status_path = os.path.join(run_root, "status.json")
            outside_path = os.path.join(root, "outside.json")
            with open(status_path, "w") as stream:
                json.dump({"state": "running"}, stream)
            with open(outside_path, "w") as stream:
                json.dump({"secret": True}, stream)
            service = OptimizerService({"runtime": {"run_root": root}})
            for invalid in ("", ".", "..", "../run-safe", "run\nunsafe"):
                with self.assertRaises(ValueError):
                    service.run_path(invalid, "status.json")
            os.symlink(run_root, os.path.join(root, "run-link"))
            with self.assertRaises(ValueError):
                service.read("run-link", "status.json")

            real_open = os.open
            swapped = {"done": False}

            def swap_before_final_open(path, flags, *args, **kwargs):
                if path == "status.json" and not swapped["done"]:
                    swapped["done"] = True
                    os.unlink(status_path)
                    os.symlink(outside_path, status_path)
                return real_open(path, flags, *args, **kwargs)

            with mock.patch("crescendo_engine.server.os.open", side_effect=swap_before_final_open):
                with self.assertRaises(ValueError):
                    service.read("run-safe", "status.json")

    def test_stop_requires_local_active_run_and_rejects_stop_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-safe")
            os.makedirs(run_root)
            outside = os.path.join(root, "outside-stop")
            with open(outside, "w") as stream:
                stream.write("unchanged\n")
            service = OptimizerService({"runtime": {"run_root": root}})
            with self.assertRaises(ValueError):
                service.stop("run-safe")
            self.assertFalse(os.path.exists(os.path.join(run_root, "STOP")))
            active_thread = mock.Mock()
            active_thread.is_alive.return_value = True
            service.threads["run-safe"] = active_thread
            os.symlink(outside, os.path.join(run_root, "STOP"))
            with self.assertRaises(ValueError):
                service.stop("run-safe")
            with open(outside, "r") as stream:
                self.assertEqual(stream.read(), "unchanged\n")
            with self.assertRaises(ValueError):
                service.stop("..")
            self.assertFalse(os.path.lexists(os.path.join(os.path.dirname(root), "STOP")))

    def test_progress_rejects_candidate_json_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-safe")
            candidate_root = os.path.join(run_root, "candidates", "g000_c0000")
            os.makedirs(candidate_root)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump({"state": "running"}, stream)
            outside = os.path.join(root, "outside-candidate.json")
            with open(outside, "w") as stream:
                json.dump({"candidate_id": "g999_c9999"}, stream)
            os.symlink(outside, os.path.join(candidate_root, "candidate.json"))
            service = OptimizerService({"runtime": {"run_root": root}})
            progress = service.progress("run-safe")
            self.assertEqual(progress["candidates"], [])
            self.assertTrue(any("candidate.json" in warning
                                for warning in progress["execution"]["warnings"]))

    def test_export_checkpoint_uses_one_authoritative_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-safe")
            os.makedirs(run_root)
            checkpoint = _snapshot_checkpoint(run_root, "snapshot-export")
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(_snapshot_status(run_root, "snapshot-export"), stream)
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump(checkpoint, stream)
            service = OptimizerService({"runtime": {"run_root": root}})
            exported = service.export_checkpoint("run-safe")
        self.assertEqual(exported, checkpoint)
        self.assertIsNot(exported, checkpoint)
        self.assertEqual(exported["payload_sha256"], _checkpoint_checksum(exported))

    def test_pending_only_checkpoint_is_listed_and_exported_without_mutation(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-pending")
            os.makedirs(run_root)
            continuation_id = "pending-only"
            status = _snapshot_status(run_root, continuation_id)
            checkpoint = _snapshot_checkpoint(run_root, continuation_id)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(status, stream)
            _write_pending_snapshot(root, checkpoint)
            pending_path = continuation_pending_path(root, continuation_id)
            with open(pending_path, "rb") as stream:
                before = stream.read()

            service = OptimizerService({"runtime": {"run_root": root}})
            rows = service.list_runs()
            exported = service.export_checkpoint("run-pending")

            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0]["checkpoint_available"])
            self.assertEqual(rows[0]["checkpoint_source"], "pending_journal")
            self.assertEqual(rows[0]["checkpoint_revision"], 1)
            self.assertEqual(exported, checkpoint)
            self.assertFalse(os.path.exists(os.path.join(run_root, "checkpoint.json")))
            self.assertFalse(os.path.exists(continuation_checkpoint_path(root, continuation_id)))
            with open(pending_path, "rb") as stream:
                self.assertEqual(stream.read(), before)

    def test_active_pending_checkpoint_downloads_but_parallel_resume_is_refused(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-pending")
            os.makedirs(run_root)
            continuation_id = "pending-active"
            checkpoint = _snapshot_checkpoint(run_root, continuation_id)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(_snapshot_status(run_root, continuation_id), stream)
            _write_pending_snapshot(root, checkpoint)
            service = OptimizerService({"runtime": {"run_root": root}})
            lease = ContinuationLease(
                root, continuation_id, run_root=run_root,
            ).acquire()
            server = HTTPServer(("127.0.0.1", 0), make_handler(service))
            thread = threading.Thread(target=server.serve_forever)
            thread.daemon = True
            thread.start()
            try:
                base = "http://127.0.0.1:%d" % server.server_port
                runs = json.loads(urlopen(base + "/api/runs").read().decode("utf-8"))
                exported = json.loads(urlopen(
                    base + "/api/checkpoint?run_id=run-pending",
                ).read().decode("utf-8"))
                self.assertTrue(runs["runs"][0]["checkpoint_available"])
                self.assertEqual(
                    runs["runs"][0]["checkpoint_source"], "pending_journal",
                )
                self.assertEqual(exported, checkpoint)
                with self.assertRaises(ContinuationLeaseBusy):
                    service.resume(exported)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(2)
                lease.release()

    def test_pending_only_export_is_the_only_file_needed_on_an_empty_runtime(self):
        with tempfile.TemporaryDirectory() as source_root:
            run_root = os.path.join(source_root, "run-pending")
            os.makedirs(run_root)
            continuation_id = "pending-portable"
            checkpoint = _snapshot_checkpoint(run_root, continuation_id)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(_snapshot_status(run_root, continuation_id), stream)
            _write_pending_snapshot(source_root, checkpoint)
            exported = OptimizerService({
                "runtime": {"run_root": source_root},
            }).export_checkpoint("run-pending")

        with tempfile.TemporaryDirectory() as destination_root:
            service = OptimizerService({"runtime": {"run_root": destination_root}})

            def launch_resume(*_args, **kwargs):
                kwargs["continuation_lease"].release()
                return {"run_id": "resumed-from-one-file"}

            with mock.patch.object(
                    service, "prepare", return_value=("prepared-1", {}),
            ), mock.patch.object(
                    service, "_launch", side_effect=launch_resume,
            ) as launch:
                result = service.resume(exported)

            self.assertEqual(result["run_id"], "resumed-from-one-file")
            self.assertEqual(load_json(continuation_checkpoint_path(
                destination_root, continuation_id, create=False,
            )), exported)
            launch.assert_called_once_with(
                exported["spec"], {}, checkpoint=exported,
                continuation_lease=mock.ANY, runtime_optimization_lease=None, run_config=mock.ANY, tb_optimization_lease=None,
            )

    def test_new_pending_revision_wins_over_old_run_and_anchor(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-pending")
            os.makedirs(run_root)
            continuation_id = "pending-newer"
            status = _snapshot_status(run_root, continuation_id)
            first = _snapshot_checkpoint(run_root, continuation_id, revision=1)
            second = _snapshot_checkpoint(
                run_root, continuation_id, revision=2,
                ancestor=first["payload_sha256"],
            )
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(status, stream)
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump(first, stream)
            anchor_path = continuation_checkpoint_path(root, continuation_id, create=True)
            with open(anchor_path, "w") as stream:
                json.dump(first, stream)
            _write_pending_snapshot(root, second, parent=first["payload_sha256"])

            service = OptimizerService({"runtime": {"run_root": root}})
            rows = service.list_runs()
            exported = service.export_checkpoint("run-pending")

            self.assertEqual(rows[0]["checkpoint_source"], "pending_journal")
            self.assertEqual(rows[0]["checkpoint_revision"], 2)
            self.assertEqual(exported, second)
            self.assertEqual(load_json(os.path.join(run_root, "checkpoint.json")), first)
            self.assertEqual(load_json(anchor_path), first)

    def test_pending_that_appears_after_run_snapshot_is_rechecked_and_wins(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-pending")
            os.makedirs(run_root)
            continuation_id = "pending-race"
            first = _snapshot_checkpoint(run_root, continuation_id, revision=1)
            second = _snapshot_checkpoint(
                run_root, continuation_id, revision=2,
                ancestor=first["payload_sha256"],
            )
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(_snapshot_status(run_root, continuation_id), stream)
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump(first, stream)
            pending = build_continuation_pending(
                second, continuation_id, 2, first["payload_sha256"],
                continuation_schema_sha256(
                    _checkpoint_continuation_schema_payload(second)
                ),
            )
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch.object(
                    service, "_read_continuation_json_snapshot",
                    side_effect=[None, pending, first],
            ):
                snapshot = service._resolve_run_checkpoint_snapshot("run-pending")
            self.assertEqual(snapshot["source"], "pending_journal")
            self.assertEqual(snapshot["checkpoint"], second)

    def test_pending_for_new_origin_does_not_replace_historical_run_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            continuation_id = "pending-origin"
            old_root = os.path.join(root, "run-old")
            new_root = os.path.join(root, "run-new")
            os.makedirs(old_root)
            os.makedirs(new_root)
            old_checkpoint = _snapshot_checkpoint(old_root, continuation_id, revision=1)
            new_checkpoint = _snapshot_checkpoint(
                new_root, continuation_id, revision=2,
                ancestor=old_checkpoint["payload_sha256"],
            )
            for run_root in (old_root, new_root):
                with open(os.path.join(run_root, "status.json"), "w") as stream:
                    json.dump(_snapshot_status(run_root, continuation_id), stream)
            with open(os.path.join(old_root, "checkpoint.json"), "w") as stream:
                json.dump(old_checkpoint, stream)
            anchor_path = continuation_checkpoint_path(root, continuation_id, create=True)
            with open(anchor_path, "w") as stream:
                json.dump(old_checkpoint, stream)
            _write_pending_snapshot(
                root, new_checkpoint, parent=old_checkpoint["payload_sha256"],
            )

            service = OptimizerService({"runtime": {"run_root": root}})
            rows = dict((row["run_id"], row) for row in service.list_runs())

            self.assertEqual(rows["run-old"]["checkpoint_source"], "run_checkpoint")
            self.assertEqual(rows["run-old"]["checkpoint_revision"], 1)
            self.assertEqual(rows["run-new"]["checkpoint_source"], "pending_journal")
            self.assertEqual(rows["run-new"]["checkpoint_revision"], 2)
            self.assertEqual(service.export_checkpoint("run-old"), old_checkpoint)
            self.assertEqual(service.export_checkpoint("run-new"), new_checkpoint)

    def test_invalid_pending_fails_closed_instead_of_exporting_old_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run-pending")
            os.makedirs(run_root)
            continuation_id = "pending-invalid"
            checkpoint = _snapshot_checkpoint(run_root, continuation_id)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump(_snapshot_status(run_root, continuation_id), stream)
            with open(os.path.join(run_root, "checkpoint.json"), "w") as stream:
                json.dump(checkpoint, stream)
            _write_pending_snapshot(root, checkpoint)
            pending_path = continuation_pending_path(root, continuation_id)
            pending = load_json(pending_path)
            pending["journal_sha256"] = "0" * 64
            with open(pending_path, "w") as stream:
                json.dump(pending, stream)

            service = OptimizerService({"runtime": {"run_root": root}})
            row = service.list_runs()[0]

            self.assertFalse(row["checkpoint_available"])
            self.assertIn("checkpoint_error", row)
            with self.assertRaisesRegex(RuntimeError, "checksum"):
                service.export_checkpoint("run-pending")

    def test_pending_symlink_and_oversize_are_rejected(self):
        for mode in ("symlink", "hardlink", "fifo", "oversize"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                run_root = os.path.join(root, "run-pending")
                os.makedirs(run_root)
                continuation_id = "pending-%s" % mode
                checkpoint = _snapshot_checkpoint(run_root, continuation_id)
                with open(os.path.join(run_root, "status.json"), "w") as stream:
                    json.dump(_snapshot_status(run_root, continuation_id), stream)
                pending_path = continuation_pending_path(
                    root, continuation_id, create=True,
                )
                if mode == "symlink":
                    outside = os.path.join(root, "outside.json")
                    with open(outside, "w") as stream:
                        json.dump({}, stream)
                    os.symlink(outside, pending_path)
                elif mode == "hardlink":
                    outside = os.path.join(root, "outside.json")
                    with open(outside, "w") as stream:
                        json.dump({}, stream)
                    os.link(outside, pending_path)
                elif mode == "fifo":
                    os.mkfifo(pending_path)
                else:
                    with open(pending_path, "wb") as stream:
                        stream.truncate(66 * 1024 * 1024)
                service = OptimizerService({"runtime": {"run_root": root}})
                row = service.list_runs()[0]
                self.assertFalse(row["checkpoint_available"])
                self.assertIn("checkpoint_error", row)
                with self.assertRaises((RuntimeError, ValueError)):
                    service.export_checkpoint("run-pending")

    def test_http_run_endpoints_reject_parent_run_id(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            server = HTTPServer(("127.0.0.1", 0), make_handler(service))
            thread = threading.Thread(target=server.serve_forever)
            thread.daemon = True
            thread.start()
            try:
                base = "http://127.0.0.1:%d" % server.server_port
                for endpoint in ("status", "result", "progress", "checkpoint", "candidates.csv"):
                    with self.assertRaises(HTTPError) as raised:
                        urlopen("%s/api/%s?run_id=.." % (base, endpoint))
                    self.assertEqual(raised.exception.code, 400)
                request = Request(
                    base + "/api/stop",
                    data=json.dumps({"run_id": ".."}).encode("utf-8"),
                    headers={"Content-Type": "application/json"}, method="POST",
                )
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request)
                self.assertEqual(raised.exception.code, 400)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(2)

    def test_http_exit_endpoint_returns_cleanup_result(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        server = HTTPServer(("127.0.0.1", 0), make_handler(service))
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        shutdown_requested = threading.Event()
        service.shutdown_callback = shutdown_requested.set
        try:
            with mock.patch.object(service, "exit_optimizer", return_value={
                    "exiting": True, "stopped_runs": [], "validation_session_closed": True,
            }) as exit_optimizer:
                request = Request(
                    "http://127.0.0.1:%d/api/exit" % server.server_port,
                    data=b"{}", headers={"Content-Type": "application/json"}, method="POST",
                )
                payload = json.loads(urlopen(request).read().decode("utf-8"))
            self.assertTrue(payload["exiting"])
            exit_optimizer.assert_called_once_with()
            self.assertTrue(shutdown_requested.wait(10))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def test_http_exit_requests_shutdown_before_writing_response(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        handler = make_handler(service)
        original_json = handler._json
        response_attempts = []

        def fail_first_exit_response(instance, status, payload, filename=None):
            if instance.path == "/api/exit" and not response_attempts:
                response_attempts.append(status)
                raise BrokenPipeError("client disconnected")
            return original_json(instance, status, payload, filename)

        handler._json = fail_first_exit_response
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        shutdown_requested = threading.Event()
        service.shutdown_callback = shutdown_requested.set
        try:
            with mock.patch.object(
                    service, "exit_optimizer", return_value={"exiting": True},
            ):
                request = Request(
                    "http://127.0.0.1:%d/api/exit" % server.server_port,
                    data=b"{}", headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request)
                self.assertEqual(raised.exception.code, 400)
            self.assertEqual(response_attempts, [200])
            self.assertTrue(shutdown_requested.wait(10))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def test_http_start_returns_explicit_reload_message_for_stale_prepared_design(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {"maestro_path": "/m"}
        server = HTTPServer(("127.0.0.1", 0), make_handler(service))
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()
        try:
            with mock.patch(
                    "crescendo_engine.server.require_prepared_freshness",
                    side_effect=ValueError(
                        "Load and Inspect again before Validate or Start."
                    ),
            ):
                request = Request(
                    "http://127.0.0.1:%d/api/start" % server.server_port,
                    data=json.dumps({
                        "prepared_id": "prepared-1", "spec": {"evaluator": "mock"},
                    }).encode("utf-8"),
                    headers={"Content-Type": "application/json"}, method="POST",
                )
                with self.assertRaises(HTTPError) as raised:
                    urlopen(request)
                payload = json.loads(raised.exception.read().decode("utf-8"))
            self.assertEqual(raised.exception.code, 400)
            self.assertIn("Load and Inspect again", payload["error"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)


    def test_optional_callback_unavailable_is_terminal_nonblocking_and_does_not_restart(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            service.prepared["prepared-1"] = {
                "parameters": [{"parameter_id": "w", "optimizable": True}],
                "resident_validation": {
                    "state": "unavailable",
                    "message": "callback capability not found",
                    "failure_stage": "validation_startup",
                    "unsafe_to_optimize": False,
                },
            }
            variables = [{
                "parameter_id": "w", "minimum": 1.0, "maximum": 2.0,
            }]
            with mock.patch.object(
                    service, "_start_resident_validation") as restart:
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", variables)
            restart.assert_not_called()
            status = service.range_validation_status(response["job_id"])
            self.assertEqual(status["state"], "failed")
            self.assertTrue(status["optional"])
            self.assertFalse(status["blocking"])
            self.assertIn("remains available", status["message"])
            self.assertNotIn(response["job_id"], service.range_validation_threads)

            spec = {
                "evaluator": "real", "variables": variables,
                "objectives": [], "constraints": [],
            }
            with mock.patch(
                    "crescendo_engine.server.require_prepared_freshness"), mock.patch.object(
                        service, "_launch", return_value={"run_id": "run-safe"}) as launch:
                started = qualified_service(service, "prepared-1").start("prepared-1", spec)
            self.assertEqual(started["run_id"], "run-safe")
            launch.assert_called_once()

    def test_workspace_preflight_blocks_real_eda_but_not_mock(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {
            "parameters": [{"parameter_id": "w", "optimizable": True}],
            "workspace_preflight": {
                "state": "failed", "message": "permission denied",
            },
        }
        variables = [{"parameter_id": "w", "minimum": 1.0, "maximum": 2.0}]
        with mock.patch(
                "crescendo_engine.server.require_prepared_freshness"), mock.patch.object(
                    service, "_launch", return_value={"run_id": "mock-run"}) as launch:
            response = qualified_service(service, "prepared-1").start("prepared-1", {
                "evaluator": "mock", "variables": variables,
                "objectives": [], "constraints": [],
            })
            self.assertEqual(response["run_id"], "mock-run")
            with self.assertRaisesRegex(RuntimeError, "private optimization workspace"):
                qualified_service(service, "prepared-1").start("prepared-1", {
                    "evaluator": "real", "variables": variables,
                    "objectives": [], "constraints": [],
                })
        self.assertEqual(launch.call_count, 1)

    def test_unsafe_optional_validation_prepared_state_blocks_real_start(self):
        service = OptimizerService({"runtime": {"run_root": _scratch_root(self)}})
        service.prepared["prepared-1"] = {
            "parameters": [{"parameter_id": "w", "optimizable": True}],
            "resident_validation": {
                "state": "unavailable", "message": "cleanup unproven",
                "unsafe_to_optimize": True,
            },
        }
        with mock.patch(
                "crescendo_engine.server.require_prepared_freshness"), mock.patch.object(
                    service, "_launch") as launch:
            with self.assertRaisesRegex(RuntimeError, "cleanup could not be proven"):
                qualified_service(service, "prepared-1").start("prepared-1", {
                    "evaluator": "real",
                    "variables": [{
                        "parameter_id": "w", "minimum": 1.0, "maximum": 2.0,
                    }],
                    "objectives": [], "constraints": [],
                })
        launch.assert_not_called()

    def test_native_workspace_failure_only_updates_optional_job_diagnostics(self):
        from crescendo_engine.workspace import WorkspaceBuildFailure

        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            prepared = {
                "parameters": [{"parameter_id": "w", "optimizable": True}],
                "resident_validation": {"state": "ready"},
            }
            service.prepared["prepared-1"] = prepared
            failure = WorkspaceBuildFailure(
                IOError("permission denied"),
                {"ok": True, "removed": True, "cleanup_error": None},
            )
            with mock.patch("crescendo_engine.netlist_providers.prepare_maestro_netlists",
                            side_effect=failure), mock.patch(
                                "crescendo_engine.private_netlist.reconcile_preparation",
                                return_value={"cleanup_proven": True}):
                response = qualified_service(service, "prepared-1").validate_ranges("prepared-1", [{
                    "parameter_id": "w", "minimum": 1.0, "maximum": 2.0,
                }])
                service.range_validation_threads[response["job_id"]].join(1)
            self.assertNotIn("workspace_preflight", prepared)
            self.assertNotIn("optimization_blocked_by", response)
            status = service.range_validation_status(response["job_id"])
            self.assertNotIn("optimization_blocked_by", status)
            self.assertEqual(status["failure_stage"], "workspace_build")
            self.assertFalse(status["blocking"])
            self.assertIn("optimization remains available", status["message"])


    def test_previous_resident_close_failure_is_unsafe_and_blocks_real_start(self):
        with tempfile.TemporaryDirectory() as root:
            setup_path = saved_setup(self, "maestro")
            service = OptimizerService({"runtime": {"run_root": root}})
            previous = mock.Mock()
            previous.close.side_effect = RuntimeError("old session still alive")
            service.resident_validation = previous
            prepared = {
                "setup": {}, "hierarchy": {}, "eda_context": {},
                "parameters": [{"parameter_id": "w", "optimizable": True}],
                "source_identity": {
                    "schema_version": "crescendo.source_design_identity.v1",
                    "fingerprint": "source-test",
                },
            }
            with mock.patch(
                    "crescendo_engine.server.prepare_setup",
                    return_value=prepared), mock.patch.object(
                        service, "eda_setup", return_value={}), mock.patch(
                            "crescendo_engine.server.detect_pdk_context",
                            return_value={}):
                prepared_id, result = service.prepare(
                    setup_path, backend="maestro",
                )
            previous.close.assert_called_once()
            self.assertEqual(result["resident_validation"]["state"], "unavailable")
            self.assertTrue(
                result["resident_validation"]["unsafe_to_optimize"],
            )

            with mock.patch(
                    "crescendo_engine.server.require_prepared_freshness"), mock.patch.object(
                        service, "_launch") as launch:
                with self.assertRaisesRegex(
                        RuntimeError, "cleanup could not be proven"):
                    qualified_service(service, prepared_id).start(prepared_id, {
                        "evaluator": "real",
                        "variables": [{
                            "parameter_id": "w", "minimum": 1.0,
                            "maximum": 2.0,
                        }],
                        "objectives": [], "constraints": [],
                    })
            launch.assert_not_called()

    def test_resume_uses_same_workspace_preflight_gate_as_start(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            prepared = {
                "workspace_preflight": {
                    "state": "failed", "message": "permission denied",
                },
            }
            checkpoint = {
                "schema_version": "crescendo.checkpoint.v1",
                "manifest": {
                    "maestro_path": "/saved/maestro",
                    "continuation_id": "resume-workspace-preflight",
                },
                "parameter_ids": [],
                "candidate_layout": list(_BASE_CANDIDATE_LAYOUT),
                "spec": {
                    "evaluator": "real", "variables": [],
                    "objectives": [], "constraints": [],
                },
                "candidates": [], "state": {},
            }
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            with mock.patch.object(
                    service, "prepare",
                    return_value=("prepared-1", prepared)), mock.patch(
                        "crescendo_engine.server.validate_checkpoint"), mock.patch.object(
                            service, "_cleanup_checkpoint_origin",
                            return_value={"state": "skipped"}) as cleanup, mock.patch.object(
                                service, "_launch") as launch:
                with self.assertRaisesRegex(
                        RuntimeError, "private optimization workspace"):
                    service.resume(checkpoint)
            self.assertEqual(cleanup.call_count, 1)
            self.assertTrue(cleanup.call_args[1]["inspect_only"])
            launch.assert_not_called()

if __name__ == "__main__":
    unittest.main()
