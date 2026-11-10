import copy
import hashlib
import json
import os
import stat
import tempfile
import unittest
from unittest import mock

from crescendo_engine.config import DEFAULTS
from crescendo_engine.environment_identity import _capture_path_resolution
from crescendo_engine.runner import (
    ExternalEdaEnvironmentChanged, _WholeRunPersistentWorker,
    _checkpoint_checksum, _eda_context_identity_matches,
    _evaluate_persistent_candidate, _prepared_eda_context_identity,
    _run_optimization_unleased,
    _run_optimization_with_leases, validate_checkpoint_integrity,
)
from crescendo_engine.server import OptimizerService
from crescendo_engine.single_test import project_setup
from crescendo_engine.storage_preflight import (
    StoragePreflightError, startup_storage_preflight,
)


def _external_identity(content):
    artifact_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    identity = {
        "schema_version": "crescendo.external_eda_environment.v1",
        "artifacts": [{
            "role": "simulator_executable", "kind": "file",
            "size": len(content), "sha256": artifact_hash, "executable": True,
        }],
        "coverage": {"simulator_executable": True},
        "limitations": [],
    }
    identity["fingerprint"] = hashlib.sha256(json.dumps(
        identity, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")).hexdigest()
    return identity


def _prepared_context(external=None):
    context = {
        "cadence": {"version_checks": []},
        "pdk": {
            "ok": True, "pdk_name": "PDK", "pdk_adapter": "generic-cdf",
            "candidate_libraries": ["PDK"],
        },
        "submission": {
            "controller": "maestro", "simulator": "spectre",
            "submit_script": None,
        },
    }
    if external is not None:
        context["external_environment"] = external
    return {"eda_context": context}


def _mock_checkpoint(root):
    checkpoint = {
        "schema_version": "crescendo.checkpoint.v1",
        "created_at": "2026-07-15T00:00:00Z",
        "manifest": {
            "maestro_path": os.path.join(root, "maestro"),
            "continuation_id": "continuation-test",
        },
        "parameter_ids": [],
        "candidate_layout": [
            "id", "generation", "index", "status", "values", "metrics",
            "failure_class", "error", "started_at", "finished_at",
        ],
        "spec": {
            "evaluator": "mock", "variables": [],
            "objectives": [], "constraints": [],
        },
        "candidates": [],
        "state": {
            "next_generation": 0, "selection_history": [], "generator": {},
            "active_plan": None, "checkpoint_revision": 0,
            "completed_candidates": 0, "failed_candidates": 0,
            "infrastructure_failed_candidates": 0,
            "ancestor_payload_sha256": [],
        },
    }
    checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
    return checkpoint


def _mock_config(root):
    config = copy.deepcopy(DEFAULTS)
    config["runtime"]["run_root"] = root
    config["runtime"]["persistent_min_free_disk_mib"] = 1
    return config


def _mock_spec():
    return {
        "evaluator": "mock", "algorithm": "random",
        "generations": 1, "population": 1, "jobs": 1,
        "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
        "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
        "constraints": [],
    }


def _mock_prepared():
    return {
        "setup": project_setup({"tests": [{"name": "main", "enabled": True}], "corners": []}, "main"),
        "hierarchy": {"designs": []},
        "parameters": [{
            "parameter_id": "x", "backend": "design_variable",
            "optimizable": True, "numeric_value": 0.5,
            "hierarchy_path": "$design_variables/main", "name": "x",
        }],
        "outputs": [{"key": "main::gain"}],
    }


def _external_diagnostics(path):
    inspected = os.lstat(path)
    return {
        "resolved_paths": [{
            "role": "model_file", "path": path,
            "lstat": {
                "device": int(inspected.st_dev),
                "inode": int(inspected.st_ino),
                "size": int(inspected.st_size),
                "mtime_ns": int(getattr(
                    inspected, "st_mtime_ns", int(inspected.st_mtime * 1000000000),
                )),
                "mode": int(stat.S_IMODE(inspected.st_mode)),
                "regular": True,
                "symlink": False,
            },
        }],
        "include_directories": [],
        "limitations": [],
    }


def _external_resolution_diagnostics(path):
    resolution = _capture_path_resolution(path)
    return {
        "resolved_paths": [{
            "role": "model_file",
            "path": resolution["target_path"],
            "lstat": resolution["target_lstat"],
            "reference_path": resolution["reference_path"],
            "symlink_chain": resolution["symlink_chain"],
            "path_components": resolution["path_components"],
        }],
        "include_directories": [],
        "limitations": [],
    }


def _prepared_with_external_file(path):
    prepared = _mock_prepared()
    prepared["eda_context"] = _prepared_context(
        _external_identity("external-environment"),
    )["eda_context"]
    prepared["eda_context"]["external_environment_diagnostics"] = (
        _external_diagnostics(path)
    )
    return prepared


class ResumePreflightHardeningTests(unittest.TestCase):
    def test_external_identity_is_legacy_compatible_only_in_forward_direction(self):
        old_identity = _prepared_eda_context_identity(_prepared_context())
        new_identity = _prepared_eda_context_identity(
            _prepared_context(_external_identity("spectre-a")),
        )
        changed_identity = _prepared_eda_context_identity(
            _prepared_context(_external_identity("spectre-b")),
        )
        self.assertTrue(_eda_context_identity_matches(old_identity, new_identity))
        self.assertFalse(_eda_context_identity_matches(new_identity, old_identity))
        self.assertFalse(_eda_context_identity_matches(new_identity, changed_identity))

        legacy_submission = copy.deepcopy(old_identity)
        legacy_submission["submission"] = {
            "controller": None, "simulator": None,
            "submit_script": {"kind": "missing"},
        }
        payload = dict(legacy_submission)
        payload.pop("fingerprint", None)
        legacy_submission["fingerprint"] = hashlib.sha256(json.dumps(
            payload, separators=(",", ":"), sort_keys=True,
        ).encode("utf-8")).hexdigest()
        self.assertTrue(_eda_context_identity_matches(legacy_submission, new_identity))

    def test_nested_external_identity_tampering_fails_even_with_new_checkpoint_checksum(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = _mock_checkpoint(root)
            identity = _prepared_eda_context_identity(
                _prepared_context(_external_identity("spectre-a")),
            )
            identity["external_environment"]["artifacts"][0]["size"] += 1
            checkpoint["manifest"]["eda_context_identity"] = identity
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            with self.assertRaisesRegex(ValueError, "external EDA environment"):
                validate_checkpoint_integrity(checkpoint)

    def test_runner_basic_preflight_precedes_continuation_lease(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.startup_storage_preflight",
                side_effect=StoragePreflightError("synthetic low disk"),
            ), mock.patch(
                "crescendo_engine.runner.ContinuationLease.acquire",
            ) as acquire:
                with self.assertRaisesRegex(StoragePreflightError, "low disk"):
                    _run_optimization_with_leases(
                        {"evaluator": "mock"}, _mock_config(root), run_root=run_root,
                    )
            acquire.assert_not_called()
            self.assertFalse(os.path.exists(run_root))

    def test_runner_refined_preflight_precedes_run_directory(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.startup_storage_preflight",
                side_effect=StoragePreflightError("synthetic workspace shortage"),
            ):
                with self.assertRaisesRegex(StoragePreflightError, "workspace shortage"):
                    _run_optimization_unleased(
                        _mock_spec(), _mock_config(root), prepared=_mock_prepared(),
                        run_root=run_root, continuation_id="continuation-test",
                        checkpoint_anchor_root=root,
                    )
            self.assertFalse(os.path.exists(run_root))

    def test_service_basic_preflight_precedes_runtime_lease(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService(_mock_config(root))
            with mock.patch(
                "crescendo_engine.server.startup_storage_preflight",
                side_effect=StoragePreflightError("synthetic service shortage"),
            ), mock.patch(
                "crescendo_engine.server.RuntimeOptimizationLease.acquire",
            ) as acquire:
                with self.assertRaisesRegex(StoragePreflightError, "service shortage"):
                    service._launch({"evaluator": "real"}, {})
            acquire.assert_not_called()
            self.assertEqual(service.optimization_launches, 0)

    def test_service_refined_preflight_precedes_queued_status_and_thread(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService(_mock_config(root))
            with mock.patch(
                "crescendo_engine.server.startup_storage_preflight",
                side_effect=StoragePreflightError("synthetic refined shortage"),
            ):
                with self.assertRaisesRegex(StoragePreflightError, "refined shortage"):
                    service._launch_registered(_mock_spec(), _mock_prepared())
            self.assertEqual(service.threads, {})
            self.assertEqual(os.listdir(root), [])

    def test_resume_basic_preflight_precedes_continuation_lease(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService(_mock_config(root))
            checkpoint = _mock_checkpoint(root)
            with mock.patch(
                "crescendo_engine.server.startup_storage_preflight",
                side_effect=StoragePreflightError("synthetic resume shortage"),
            ), mock.patch(
                "crescendo_engine.server.ContinuationLease.acquire",
            ) as acquire:
                with self.assertRaisesRegex(StoragePreflightError, "resume shortage"):
                    service.resume(checkpoint)
            acquire.assert_not_called()

    def test_resume_refined_preflight_precedes_anchor_persist_and_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService(_mock_config(root))
            checkpoint = _mock_checkpoint(root)
            prepared = {
                "setup": {}, "hierarchy": {}, "parameters": [], "outputs": [],
            }
            preflight_report = {"schema_version": "test", "ok": True}
            with mock.patch.object(
                service, "prepare", return_value=("prepared-id", prepared),
            ) as prepare, mock.patch.object(
                service, "_cleanup_checkpoint_origin",
                return_value={"state": "skipped"},
            ) as cleanup, mock.patch(
                "crescendo_engine.server._reconcile_continuation_anchor",
                return_value={"state": "absent"},
            ) as reconcile, mock.patch(
                "crescendo_engine.server.startup_storage_preflight",
                side_effect=[
                    preflight_report,
                    StoragePreflightError("synthetic refined resume shortage"),
                ],
            ), mock.patch.object(service, "_launch") as launch:
                with self.assertRaisesRegex(
                    StoragePreflightError, "refined resume shortage",
                ):
                    service.resume(checkpoint)
            prepare.assert_called_once()
            cleanup.assert_called_once_with(
                checkpoint, terminate_owned=True, inspect_only=True,
            )
            self.assertEqual(reconcile.call_count, 2)
            self.assertTrue(all(
                call[1].get("persist_absent") is False
                for call in reconcile.call_args_list
            ))
            launch.assert_not_called()

    def test_each_checkpoint_write_rechecks_storage_headroom(self):
        with tempfile.TemporaryDirectory() as root:
            config = _mock_config(root)
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.startup_storage_preflight",
                wraps=startup_storage_preflight,
            ) as preflight:
                result = _run_optimization_with_leases(
                    _mock_spec(), config, prepared=_mock_prepared(), run_root=run_root,
                )
            self.assertEqual(result["state"], "completed")
            checkpoint_calls = [
                call for call in preflight.call_args_list
                if isinstance(call[1].get("checkpoint"), dict)
            ]
            self.assertTrue(checkpoint_calls)
            self.assertTrue(all(
                call[1].get("workspace_required") is False
                for call in checkpoint_calls
            ))

    def test_replacement_epoch_reverifies_external_identity_before_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            worker = _WholeRunPersistentWorker(
                root, 0, _mock_prepared(), [], _mock_config(root),
            )
            worker.epoch_index = 2
            with mock.patch(
                "crescendo_engine.runner._reverify_prepared_external_environment",
                return_value={"ok": False, "error": "model content changed"},
            ), mock.patch("crescendo_engine.runner.build_workspace") as build_workspace:
                with self.assertRaisesRegex(
                    ExternalEdaEnvironmentChanged, "model content changed",
                ):
                    worker._new_epoch()
            build_workspace.assert_not_called()
            # Failed preflight consumes the attempted startup identity, so a
            # later replacement cannot reuse the same epoch number.
            self.assertEqual(worker.epoch_index, 3)
            self.assertEqual(os.listdir(worker.worker_root), [])

    def test_candidate_boundary_uses_lstat_and_rehashes_once_per_generation(self):
        with tempfile.TemporaryDirectory() as root:
            model = os.path.join(root, "model.scs")
            with open(model, "w") as stream:
                stream.write("parameters nominal=1\n")
            worker = _WholeRunPersistentWorker(
                root, 0, _prepared_with_external_file(model), [], _mock_config(root),
            )
            with mock.patch(
                "crescendo_engine.runner._reverify_prepared_external_environment",
                return_value={"ok": True},
            ) as reverify:
                first = worker.verify_external_environment_boundary(0)
                second = worker.verify_external_environment_boundary(0)
                third = worker.verify_external_environment_boundary(1)
            self.assertEqual(first["state"], "identity_reverified")
            self.assertEqual(second["state"], "metadata_unchanged")
            self.assertEqual(third["state"], "identity_reverified")
            self.assertEqual(reverify.call_count, 2)

    def test_candidate_boundary_metadata_change_rehashes_and_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            model = os.path.join(root, "model.scs")
            with open(model, "w") as stream:
                stream.write("parameters nominal=1\n")
            worker = _WholeRunPersistentWorker(
                root, 0, _prepared_with_external_file(model), [], _mock_config(root),
            )
            with mock.patch(
                "crescendo_engine.runner._reverify_prepared_external_environment",
                side_effect=[{"ok": True}, {"ok": False, "error": "model changed"}],
            ) as reverify:
                worker.verify_external_environment_boundary(0)
                with open(model, "a") as stream:
                    stream.write("parameters changed=1\n")
                with self.assertRaisesRegex(
                    ExternalEdaEnvironmentChanged, "model changed",
                ):
                    worker.verify_external_environment_boundary(0)
            self.assertEqual(reverify.call_count, 2)

    def test_candidate_boundary_repoints_initial_symlink_and_rehashes_same_generation(self):
        with tempfile.TemporaryDirectory() as root:
            first_target = os.path.join(root, "first.scs")
            second_target = os.path.join(root, "second.scs")
            model = os.path.join(root, "model.scs")
            with open(first_target, "w") as stream:
                stream.write("parameters nominal=1\n")
            with open(second_target, "w") as stream:
                stream.write("parameters nominal=2\n")
            os.symlink(first_target, model)
            prepared = _prepared_with_external_file(model)
            prepared["eda_context"]["external_environment_diagnostics"] = (
                _external_resolution_diagnostics(model)
            )
            worker = _WholeRunPersistentWorker(
                root, 0, prepared, [], _mock_config(root),
            )
            with mock.patch(
                "crescendo_engine.runner._reverify_prepared_external_environment",
                side_effect=[{"ok": True}, {"ok": False, "error": "model link changed"}],
            ) as reverify:
                worker.verify_external_environment_boundary(0)
                os.unlink(model)
                os.symlink(second_target, model)
                with self.assertRaisesRegex(
                    ExternalEdaEnvironmentChanged, "model link changed",
                ):
                    worker.verify_external_environment_boundary(0)
            self.assertEqual(reverify.call_count, 2)

    def test_candidate_boundary_rejects_symlink_without_rehashing(self):
        with tempfile.TemporaryDirectory() as root:
            model = os.path.join(root, "model.scs")
            target = os.path.join(root, "target.scs")
            for path in (model, target):
                with open(path, "w") as stream:
                    stream.write("parameters nominal=1\n")
            worker = _WholeRunPersistentWorker(
                root, 0, _prepared_with_external_file(model), [], _mock_config(root),
            )
            with mock.patch(
                "crescendo_engine.runner._reverify_prepared_external_environment",
                return_value={"ok": True},
            ) as reverify:
                worker.verify_external_environment_boundary(0)
                os.unlink(model)
                os.symlink(target, model)
                with self.assertRaisesRegex(
                    ExternalEdaEnvironmentChanged, "symbolic link",
                ):
                    worker.verify_external_environment_boundary(0)
            self.assertEqual(reverify.call_count, 1)

    def test_candidate_boundary_failure_precedes_maestro_start(self):
        class GuardedWorker(object):
            def __init__(self):
                self.ensure_started_called = False

            def verify_external_environment_boundary(self, generation):
                raise ExternalEdaEnvironmentChanged("technology changed")

            def ensure_started(self):
                self.ensure_started_called = True
                raise AssertionError("Maestro must not start after boundary failure")

        with tempfile.TemporaryDirectory() as root:
            worker = GuardedWorker()
            config = _mock_config(root)
            config["runtime"]["infrastructure_retries"] = 0
            spec = {
                "candidate_timeout_seconds": 30,
                "objectives": [], "constraints": [],
            }
            with self.assertRaisesRegex(
                ExternalEdaEnvironmentChanged, "technology changed",
            ):
                _evaluate_persistent_candidate(
                    worker, root, 0, 0, {}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertFalse(worker.ensure_started_called)


if __name__ == "__main__":
    unittest.main()
