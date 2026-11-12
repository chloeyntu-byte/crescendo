import copy
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.config import DEFAULTS
from crescendo_engine.continuation import (
    ContinuationLease, checkpoint_continuation_id, continuation_checkpoint_path,
    continuation_pending_path,
)
from crescendo_engine.optimizers import PopulationGenerator
from crescendo_engine.persistent_maestro import (
    PersistentMaestroWorkerError, PersistentMaestroWorkerTimeout,
)
from crescendo_engine.persistent_controller import (
    PersistentControllerProcessError, PersistentControllerRemoteError,
)
from crescendo_engine.session_health import PersistentSessionHealthLimit
from crescendo_engine.server import _reconcile_continuation_anchor
from crescendo_engine.runner import (
    ExternalEdaEnvironmentChanged, _WholeRunPersistentWorker,
    _PersistentIdentityGuardSlots,
    _adexl_infrastructure_failure_reason,
    _algorithm_defaults, _automatic_population_size, _enforce_catalog_variable_types, _evaluate_candidate,
    _checkpoint_checksum,
    _evaluate_persistent_candidate, _failure_metrics,
    _normalize_algorithm_settings, _discard_unreturned_candidate_roots,
    _persistent_running_row, _retryable_setup_attempt,
    _prepared_eda_context_identity, _prepared_schema_fingerprint_v1,
    _recover_terminal_candidate_wal,
    _run_coordinated_resume,
    _simulation_submission_metadata,
    _trusted_candidate_infrastructure_evidence,
    checkpoint_candidates, checkpoint_with_candidates, parameter_catalog,
    resolve_design_config, resolve_metric_bindings, run_optimization,
    validate_checkpoint, validate_checkpoint_integrity,
)
from crescendo_engine.workspace import ParameterApplicationInfrastructureFailure
from crescendo_engine.utils import load_json


def _coordinated_runner_resume(spec, config, prepared, run_root, checkpoint):
    runtime_root = config["runtime"]["run_root"]
    lease = ContinuationLease(
        runtime_root, checkpoint_continuation_id(checkpoint), run_root=run_root,
    ).acquire()
    try:
        _reconcile_continuation_anchor(checkpoint, runtime_root)
        return _run_coordinated_resume(
            spec, config, prepared=prepared, run_root=run_root,
            resume_checkpoint=checkpoint, continuation_lease=lease,
        )
    finally:
        lease.release()


def _fake_run_controller(evaluator, instances=None):
    created = instances if instances is not None else []

    class FakeController(object):
        def __init__(self, run_root, worker_index, *_args):
            self.run_root = run_root
            self.worker_index = worker_index
            self.closed = 0
            self.restarts = 0
            created.append(self)

        def start(self):
            return self

        def submit_evaluate(self, index, generation, values, _spec):
            return {"index": index, "generation": generation, "values": values}

        def poll(self, request):
            return evaluator(
                self, self.run_root, request["index"], request["generation"], request["values"],
            )

        def commit_candidate(self, _candidate_id):
            return {"ok": True, "state": "cleaned", "recycled": False}

        def terminate_and_prove(self, _reason):
            return {
                "termination_proven": True, "process_still_alive": False,
                "cleanup": {"ok": True, "removed": False},
            }

        def restart(self):
            self.restarts += 1
            return self

        def close(self):
            self.closed += 1
            return _proven_controller_shutdown()

    return FakeController


def _proven_controller_shutdown():
    return {
        "reason": "closed", "termination_proven": True,
        "process_still_alive": False,
        "cleanup": {"ok": True, "removed": True},
    }


def _proven_session_health(controller_epoch=0, controller_pid=123,
                           controller_start_ticks=456, local_epoch=0,
                           rss_kib=1000, **updates):
    if controller_epoch == 0:
        worker_epoch = "worker-000-epoch-%03d" % local_epoch
    else:
        worker_epoch = "worker-000-controller-%06d-epoch-%06d" % (
            controller_epoch, local_epoch,
        )
    session_pid = controller_pid + 1
    session_start_ticks = controller_start_ticks + 1
    result = {
        "alive": True, "tree_complete": True,
        "storage_complete": True, "logs_complete": True,
        "controller_epoch": controller_epoch,
        "pid": controller_pid, "pid_start_ticks": controller_start_ticks,
        "epoch": worker_epoch, "worker_start_count": local_epoch + 1,
        "rss_kib": rss_kib, "fd_count": 20, "max_fd_count": 20,
        "free_disk_mib": 4096, "log_bytes": 100,
        "session_identity_proven": True,
        "session_identity": {
            "schema_version": "crescendo.persistent_session_identity.v1",
            "proven": True, "worker_index": 0,
            "controller_epoch": controller_epoch, "epoch": worker_epoch,
            "local_epoch": local_epoch, "pid": session_pid,
            "pid_start_ticks": session_start_ticks,
        },
    }
    result.update(updates)
    return result


def _terminal_wal_spec():
    return {
        "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
        "constraints": [{"output_key": "main::power", "operator": "maximum", "maximum": 1.0}],
    }


def _terminal_wal_metrics():
    return {
        "objectives": [{"output_key": "main::gain", "value": 12.0, "score": -12.0}],
        "constraints": [{
            "output_key": "main::power", "value": 0.25, "violation": 0.0,
            "satisfied": True,
        }],
        "feasible": True, "total_violation": 0.0, "merit": -12.0,
    }


def _terminal_wal_completion(row, history_name="Interactive.6"):
    output_keys = sorted(set(
        metric.get("output_key")
        for group in ("objectives", "constraints")
        for metric in (row.get("metrics", {}).get(group) or [])
        if metric.get("output_key")
    ))
    test_names = sorted(set(key.split("::", 1)[0] for key in output_keys)) or ["main"]
    points = []
    spectre_runs = []
    points_per_test = {}
    for point_id, test_name in enumerate(test_names, 1):
        points.append({
            "test": test_name, "point_id": point_id, "design_point": 1,
            "corner": "TT", "status_code": 3, "status": "done",
        })
        points_per_test[test_name] = 1
        psf = (
            "simulation/HH/TB/maestro/results/maestro/%s/%d/%s/psf"
            % (history_name, point_id, test_name)
        )
        spectre_runs.append({
            "test": test_name, "point_id": point_id, "corner": "TT",
            "psf": psf, "sim_done": psf + "/.simDone",
            "spectre": {
                "path": psf + "/spectre.out", "size_bytes": 1024,
                "completion": "spectre completes with 0 errors, 1 warning, and 0 notices.",
                "error_count": 0,
            },
        })
    return {
        "schema_version": "crescendo.completion_evidence.v1",
        "ok": True, "adapter": "maestro-rdb-psf-v1",
        "validated_at": "2026-07-14T12:00:00Z",
        "history_name": history_name,
        "rdb": {
            "path": "libraries/HH/TB/maestro/results/maestro/%s.rdb" % history_name,
            "history_name": history_name, "size_bytes": 4096,
            "mtime_epoch": 1784020800.0,
        },
        "tests": test_names, "corners": ["TT"], "points": points,
        "outputs": [
            {"key": key, "point_count": points_per_test[key.split("::", 1)[0]], "scalar": True}
            for key in output_keys
        ],
        "spectre_runs": spectre_runs,
    }


def _write_terminal_wal(run_root, row, completion=True, completion_document=None):
    candidate_root = os.path.join(run_root, "candidates", row["candidate_id"])
    os.makedirs(candidate_root)
    document = copy.deepcopy(row)
    evidence = None
    if completion:
        evidence = copy.deepcopy(completion_document or _terminal_wal_completion(document))
        document["completion_evidence"] = evidence
    with open(os.path.join(candidate_root, "candidate.json"), "w") as stream:
        json.dump(document, stream)
    with open(os.path.join(candidate_root, "timing.json"), "w") as stream:
        json.dump({"candidate_id": row["candidate_id"], "status": row["status"]}, stream)
    if completion:
        with open(os.path.join(candidate_root, "completion_evidence.json"), "w") as stream:
            json.dump(evidence, stream)
    return candidate_root


def _minimal_checkpoint():
    return {
        "schema_version": "crescendo.checkpoint.v1",
        "parameter_ids": ["x"],
        "candidate_layout": [
            "id", "generation", "index", "status", "values", "metrics",
            "failure_class", "error", "started_at", "finished_at",
        ],
        "spec": {
            "evaluator": "mock",
            "population": 1,
            "variables": [{"parameter_id": "x"}],
            "objectives": [], "constraints": [],
        },
        "candidates": [[
            "g000_c0000", 0, 0, "completed", [0.25], None,
            None, "", None, None,
        ]],
        "state": {
            "next_generation": 0,
            "active_plan": {
                "generation": 0, "next_index": 1, "replacements": 0,
                "planned": [[0, [0.25]]],
            },
            "generator": {
                "previous": ["g000_c0000"], "de_targets": [],
            },
            "checkpoint_revision": 4,
            "completed_candidates": 1,
            "failed_candidates": 0,
            "infrastructure_failed_candidates": 0,
        },
    }


class RunnerTest(unittest.TestCase):
    def test_simulation_submission_records_actual_ade_controller(self):
        metadata = _simulation_submission_metadata({"runtime": {}}, controller="adexl")
        self.assertEqual(metadata["controller"], "adexl")

    def test_adexl_retries_only_proven_infrastructure_startup_failure(self):
        self.assertTrue(_retryable_setup_attempt(
            {"infrastructure_failure": True}, "adexl",
        ))
        self.assertFalse(_retryable_setup_attempt(
            {"infrastructure_failure": True, "termination_failed": True}, "adexl",
        ))
        self.assertFalse(_retryable_setup_attempt(
            {"infrastructure_failure": True}, "maestro",
        ))

    def test_adexl_infrastructure_failure_preserves_finalization_error(self):
        reason = _adexl_infrastructure_failure_reason({
            "returncode": 0,
            "adexl_protocol_states": ["ACCEPTED", "BEGIN", "ERROR"],
            "finalization_error": "AdeXLProtocolError: history identity mismatch",
        })
        self.assertEqual(
            reason,
            "ADE XL infrastructure failure: AdeXLProtocolError: history identity mismatch",
        )

    def test_adexl_infrastructure_failure_reports_process_context_without_finalization_error(self):
        reason = _adexl_infrastructure_failure_reason({
            "returncode": -9,
            "adexl_protocol_states": ["ACCEPTED", "BEGIN"],
        })
        self.assertIn("returncode=-9", reason)
        self.assertIn("protocol_state=BEGIN", reason)

    def test_adexl_candidate_infrastructure_failure_surfaces_finalization_error(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"backend": "adexl", "tests": []},
                "hierarchy": {"designs": []},
            }
            config = {"runtime": {
                "candidate_timeout_seconds": 60,
                "infrastructure_retries": 0,
            }}
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            command = {
                "returncode": 0,
                "timed_out": False,
                "startup_timed_out": False,
                "infrastructure_failure": True,
                "finalization_error": "AdeXLProtocolError: history identity mismatch",
                "adexl_protocol_states": ["ACCEPTED", "BEGIN", "ERROR"],
                "lock_wait_seconds": 0.0,
                "elapsed_seconds": 0.1,
                "spectre_timing": {},
            }
            with mock.patch(
                    "crescendo_engine.runner.build_workspace", return_value={"root": root}), \
                    mock.patch("crescendo_engine.runner.apply_candidate", return_value={}), \
                    mock.patch("crescendo_engine.runner.run_adexl", return_value=command), \
                    mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}):
                row = _evaluate_candidate(
                    root, 0, 0, {"x": 0.5}, spec, prepared, [], config,
                )
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertIn("AdeXLProtocolError: history identity mismatch", row["error"])

    def test_parameter_application_failure_stops_before_next_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 3, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            evaluated = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    return {
                        "candidate_id": "g000_c%04d" % request["index"],
                        "generation": request["generation"], "index": request["index"],
                        "values": request["values"], "status": "failed",
                        "failure_class": "parameter_application",
                        "error": (
                            "ParameterApplicationFailure: schematic property mismatch for x: "
                            "requested='20u' schematic='8u'"
                        ),
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                    }

                def commit_candidate(self, _candidate_id):
                    return {
                        "ok": True, "state": "cleaned", "recycled": False,
                        "health": _proven_session_health(),
                    }

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                with self.assertRaisesRegex(
                    RuntimeError, "stopped after the first PDK-adjusted candidate",
                ):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)

            self.assertEqual(evaluated, [0])
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["failed_candidates"], 1)

    def test_identity_guard_expected_replacement_is_isolated_by_worker_slot(self):
        slots = _PersistentIdentityGuardSlots()

        def entry_factory(_identity, controller_identity, _observed_at):
            return {
                "controller_identity": controller_identity,
                "identity_pending_since": None,
            }

        slots.expect_replacement(0, (0, 10, 100), entry_factory)
        slots.expect_replacement(1, (0, 20, 200), entry_factory)
        slots.begin_expected_replacement(0, 12.5)

        self.assertEqual(slots.boundary[0]["identity_pending_since"], 12.5)
        self.assertEqual(slots.active[0]["identity_pending_since"], 12.5)
        self.assertIsNone(slots.boundary[1]["identity_pending_since"])
        self.assertIsNone(slots.active[1]["identity_pending_since"])

    def test_terminal_candidate_wal_recovery_requires_exact_identity_values_and_evidence(self):
        spec = _terminal_wal_spec()
        values = {"w": 2.0e-6, "nf": 4}
        termination = {"termination_proven": True, "process_still_alive": False}
        base = {
            "candidate_id": "g002_c0007", "generation": 2, "index": 7,
            "values": values, "status": "completed", "metrics": _terminal_wal_metrics(),
            "source_guard": {"ok": True, "libraries": []},
        }
        mutations = (
            ("forged candidate id", {"candidate_id": "g002_c0008"}, values, True),
            ("wrong generation", {"generation": 3}, values, True),
            ("wrong index", {"index": 8}, values, True),
            ("wrong values", {}, {"w": 3.0e-6, "nf": 4}, True),
            ("missing completion evidence", {}, values, False),
        )
        for label, changes, planned_values, completion in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as root:
                row = copy.deepcopy(base)
                row.update(changes)
                candidate_id = row["candidate_id"]
                row["candidate_id"] = "g002_c0007"
                candidate_root = _write_terminal_wal(root, row, completion=completion)
                if candidate_id != row["candidate_id"]:
                    row["candidate_id"] = candidate_id
                    with open(os.path.join(candidate_root, "candidate.json"), "w") as stream:
                        json.dump(row, stream)
                recovered = _recover_terminal_candidate_wal(
                    root, 7, 2, planned_values, spec, 0, termination,
                )
                self.assertIsNone(recovered)

    def test_terminal_candidate_wal_recovery_rejects_untrusted_terminal_rows(self):
        spec = _terminal_wal_spec()
        values = {"w": 2.0e-6, "nf": 4}
        termination = {"termination_proven": True, "process_still_alive": False}
        cases = (
            ("infrastructure status", {"status": "infrastructure_failed"}),
            ("source guard", {"source_guard": {"ok": False}}),
            ("objective count", {"metrics": dict(_terminal_wal_metrics(), objectives=[])}),
        )
        for label, changes in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as root:
                row = {
                    "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                    "values": values, "status": "completed", "metrics": _terminal_wal_metrics(),
                    "source_guard": {"ok": True},
                }
                row.update(changes)
                _write_terminal_wal(root, row)
                self.assertIsNone(_recover_terminal_candidate_wal(
                    root, 0, 0, values, spec, 0, termination,
                ))

    def test_terminal_candidate_wal_recovery_accepts_completed_and_failed_rows(self):
        spec = _terminal_wal_spec()
        values = {"w": 2.0e-6, "nf": 4}
        termination = {"termination_proven": True, "process_still_alive": False}
        with tempfile.TemporaryDirectory() as completed_root:
            completed = {
                "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                "values": values, "status": "completed", "metrics": _terminal_wal_metrics(),
                "source_guard": {"ok": True},
            }
            _write_terminal_wal(completed_root, completed)
            recovered = _recover_terminal_candidate_wal(
                completed_root, 0, 0, values, spec, 3, termination,
            )
            self.assertEqual(recovered["status"], "completed")
            self.assertTrue(recovered["wal_recovery"]["recovered"])
            self.assertTrue(recovered["completion_evidence"]["ok"])
            self.assertFalse(recovered["persistent_worker"]["cleanup_pending"])
            self.assertEqual(recovered["persistent_worker"]["worker_index"], 3)
        with tempfile.TemporaryDirectory() as failed_root:
            failed = {
                "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                "values": values, "status": "failed",
                "metrics": _failure_metrics(spec, RuntimeError("simulation failed")),
                "source_guard": {"ok": True},
            }
            _write_terminal_wal(failed_root, failed, completion=False)
            with open(os.path.join(
                failed_root, "candidates", "g000_c0000", "completion_evidence.json",
            ), "w") as stream:
                json.dump({"ok": True}, stream)
            recovered = _recover_terminal_candidate_wal(
                failed_root, 0, 0, values, spec, 3, termination,
            )
            self.assertEqual(recovered["status"], "failed")
            self.assertIsNone(recovered["wal_recovery"]["completion_evidence"])

    def test_terminal_candidate_wal_completion_evidence_is_strictly_bound(self):
        spec = _terminal_wal_spec()
        values = {"w": 2.0e-6, "nf": 4}
        termination = {"termination_proven": True, "process_still_alive": False}
        row = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "values": values, "status": "completed", "metrics": _terminal_wal_metrics(),
            "source_guard": {"ok": True},
        }

        def recover(evidence, selected_spec=None, compact=False, file_mutation=None):
            root = tempfile.TemporaryDirectory()
            self.addCleanup(root.cleanup)
            candidate_root = _write_terminal_wal(
                root.name, row, completion_document=evidence,
            )
            candidate_path = os.path.join(candidate_root, "candidate.json")
            if compact:
                persisted = load_json(candidate_path)
                bound = persisted.pop("completion_evidence")
                persisted["completion"] = {
                    "ok": True, "adapter": bound["adapter"],
                    "history_name": bound["history_name"],
                    "tests": bound["tests"], "corners": bound["corners"],
                    "point_count": len(bound["points"]),
                    "output_count": len(bound["outputs"]),
                }
                with open(candidate_path, "w") as stream:
                    json.dump(persisted, stream)
            if file_mutation is not None:
                completion_path = os.path.join(candidate_root, "completion_evidence.json")
                persisted = load_json(completion_path)
                file_mutation(persisted)
                with open(completion_path, "w") as stream:
                    json.dump(persisted, stream)
            return _recover_terminal_candidate_wal(
                root.name, 0, 0, values, selected_spec or spec, 0, termination,
            )

        native = _terminal_wal_completion(row)
        self.assertEqual(recover(native)["status"], "completed")
        self.assertEqual(recover(native, compact=True)["status"], "completed")
        cornerless = copy.deepcopy(native)
        cornerless["corners"] = []
        for point in cornerless["points"]:
            point["corner"] = None
        for run in cornerless["spectre_runs"]:
            run["corner"] = None
        self.assertEqual(recover(cornerless)["status"], "completed")
        candidate_history = _terminal_wal_completion(
            row, history_name="CRESCENDO_g000_c0000_run_0123456789",
        )
        self.assertEqual(recover(candidate_history)["status"], "completed")

        cases = []
        old_minimal = {"ok": True, "schema_version": "crescendo.completion_evidence.v1"}
        cases.append(("old minimal document", old_minimal, spec))
        wrong_schema = copy.deepcopy(native)
        wrong_schema["schema_version"] = "crescendo.completion_evidence.v0"
        cases.append(("unsupported schema", wrong_schema, spec))
        wrong_adapter = copy.deepcopy(native)
        wrong_adapter["adapter"] = "unregistered-simulator-v1"
        cases.append(("unsupported adapter", wrong_adapter, spec))
        wrong_candidate = _terminal_wal_completion(
            row, history_name="CRESCENDO_g000_c0001_run_0123456789",
        )
        cases.append(("candidate-coded history mismatch", wrong_candidate, spec))
        wrong_rdb = copy.deepcopy(native)
        wrong_rdb["rdb"]["history_name"] = "Interactive.7"
        cases.append(("RDB history mismatch", wrong_rdb, spec))
        unsafe_rdb = copy.deepcopy(native)
        unsafe_rdb["rdb"]["path"] = "../Interactive.6.rdb"
        cases.append(("unsafe RDB path", unsafe_rdb, spec))
        wrong_status = copy.deepcopy(native)
        wrong_status["points"][0]["status"] = "running"
        cases.append(("unfinished point", wrong_status, spec))
        missing_output = copy.deepcopy(native)
        missing_output["outputs"] = missing_output["outputs"][:1]
        cases.append(("spec output mismatch", missing_output, spec))
        wrong_count = copy.deepcopy(native)
        wrong_count["outputs"][0]["point_count"] = 2
        cases.append(("output point mismatch", wrong_count, spec))
        missing_run = copy.deepcopy(native)
        missing_run["spectre_runs"] = []
        cases.append(("missing Spectre run", missing_run, spec))
        failed_spectre = copy.deepcopy(native)
        failed_spectre["spectre_runs"][0]["spectre"]["completion"] = (
            "spectre completes with 1 error, 0 warnings, and 0 notices."
        )
        cases.append(("Spectre failure marker", failed_spectre, spec))
        wrong_corner_spec = copy.deepcopy(spec)
        wrong_corner_spec["objectives"][0]["corners"] = ["FF"]
        cases.append(("selected corner mismatch", native, wrong_corner_spec))
        for label, evidence, selected_spec in cases:
            with self.subTest(label=label):
                self.assertIsNone(recover(evidence, selected_spec=selected_spec))

        self.assertIsNone(recover(
            native,
            file_mutation=lambda evidence: evidence.update({"unbound_nonce": "changed"}),
        ))

    def test_terminal_candidate_wal_rejects_mismatched_compact_completion_summary(self):
        spec = _terminal_wal_spec()
        values = {"w": 2.0e-6, "nf": 4}
        termination = {"termination_proven": True, "process_still_alive": False}
        row = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "values": values, "status": "completed", "metrics": _terminal_wal_metrics(),
            "source_guard": {"ok": True},
        }
        with tempfile.TemporaryDirectory() as root:
            candidate_root = _write_terminal_wal(root, row)
            candidate_path = os.path.join(candidate_root, "candidate.json")
            persisted = load_json(candidate_path)
            evidence = persisted.pop("completion_evidence")
            persisted["completion"] = {
                "ok": True, "adapter": evidence["adapter"],
                "history_name": "Interactive.999", "tests": evidence["tests"],
                "corners": evidence["corners"], "point_count": len(evidence["points"]),
                "output_count": len(evidence["outputs"]),
            }
            with open(candidate_path, "w") as stream:
                json.dump(persisted, stream)
            self.assertIsNone(_recover_terminal_candidate_wal(
                root, 0, 0, values, spec, 0, termination,
            ))

    def test_custom_run_root_becomes_the_private_workspace_safety_root(self):
        with tempfile.TemporaryDirectory() as root:
            configured_root = os.path.join(root, "configured")
            custom_root = os.path.join(root, "external", "run")
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": configured_root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "batch", "persistent_batch_size": 8,
                "max_parallel_jobs": 1, "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            def evaluate(_controller, run_root, index, generation, values):
                self.assertEqual(run_root, custom_root)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            instances = []
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=custom_root)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(len(instances), 1)
            status = load_json(os.path.join(custom_root, "status.json"))
            self.assertEqual(status["worker_lifetime_override"]["effective"], "run")

    def test_unreturned_staged_candidates_are_removed_instead_of_staying_running(self):
        with tempfile.TemporaryDirectory() as root:
            candidates = []
            for index in range(4):
                candidate_root = os.path.join(root, "candidates", "g000_c%04d" % index)
                os.makedirs(candidate_root)
                with open(os.path.join(candidate_root, "candidate.json"), "w") as stream:
                    json.dump({"status": "running"}, stream)
                candidates.append({
                    "root": candidate_root,
                    "row": {"candidate_id": "g000_c%04d" % index, "status": "running"},
                })
            returned = [candidates[0]]
            _discard_unreturned_candidate_roots(candidates, returned, root, {"objectives": [], "constraints": []})
            self.assertTrue(os.path.isdir(candidates[0]["root"]))
            self.assertEqual([os.path.exists(item["root"]) for item in candidates[1:]], [False, False, False])

    def test_auto_backend_uses_run_lifetime_controller_for_real_schematic_candidates(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 2,
                "persistent_worker_lifetime": "batch", "max_parallel_jobs": 2,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "execution_backend": "persistent_maestro",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            instances = []
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ), mock.patch("crescendo_engine.runner._evaluate_candidate") as legacy:
                result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertEqual(evaluated, [0, 1, 2, 3])
            self.assertEqual(len(instances), 1)
            legacy.assert_not_called()
            self.assertEqual(result["candidates"][0].get("execution_backend"), "persistent_maestro")
            self.assertEqual(load_json(os.path.join(root, "run", "spec.json"))["execution_backend"], "persistent_maestro")

    def test_fully_terminal_checkpoint_resume_does_not_start_maestro_controller(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "schema_version": "crescendo.prepared.v1", "maestro_path": "/portable/maestro",
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
                "source_identity": {
                    "schema_version": "crescendo.source_design_identity.v1",
                    "libraries": [], "fingerprint": "source-a",
                },
                "eda_context": {
                    "cadence": {"version_checks": []},
                    "pdk": {"ok": True, "pdk_name": "PDK", "pdk_adapter": "generic-cdf"},
                    "submission": {"controller": "maestro", "simulator": "spectre"},
                },
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 2, "jobs": 1,
                "display_name": "OPA terminal resume",
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            def evaluate(_controller, _root, index, generation, values):
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed",
                    "metrics": {
                        "objectives": [{"score": -values["x"], "value": values["x"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                }

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate),
            ):
                run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "origin"))
            checkpoint = load_json(os.path.join(root, "origin", "checkpoint.json"))
            status = load_json(os.path.join(root, "origin", "status.json"))
            result = load_json(os.path.join(root, "origin", "result.json"))
            self.assertEqual(status["display_name"], "OPA terminal resume")
            self.assertEqual(status["total_generations"], 1)
            self.assertEqual(result["display_name"], "OPA terminal resume")
            self.assertEqual(checkpoint["spec"]["display_name"], "OPA terminal resume")
            self.assertEqual(checkpoint["manifest"]["display_name"], "OPA terminal resume")
            self.assertEqual(checkpoint["state"]["next_generation"], 1)
            self.assertEqual(checkpoint["state"]["terminal_state"], "completed")
            self.assertEqual(checkpoint["state"]["terminal_reason"], "generation_limit")

            with mock.patch("crescendo_engine.runner.PersistentControllerProxy") as controller:
                resumed = _coordinated_runner_resume(
                    spec, config, prepared, os.path.join(root, "resumed"), checkpoint,
                )
            controller.assert_not_called()
            self.assertEqual(resumed["state"], "completed")
            self.assertEqual(len(resumed["candidates"]), 2)

    def test_unstarted_persistent_candidates_run_before_replacements(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "batch", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 2,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                if index == 0:
                    return {
                        "candidate_id": "g%03d_c%04d" % (generation, index),
                        "generation": generation, "index": index, "values": values,
                        "status": "infrastructure_failed", "failure_class": "infrastructure",
                        "error": "worker timeout",
                    }
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            instances = []
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertEqual(evaluated, [0, 1, 2, 3, 4])
            self.assertEqual(len(instances), 1)
            self.assertEqual(len(result["candidates"]), 5)
            self.assertEqual(sum(row["status"] == "infrastructure_failed" for row in result["candidates"]), 1)

    def test_legacy_run_persists_guardian_protocol_for_checkpoint_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "maestro_variable", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 2, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            def evaluate(_root, index, generation, values, *_args):
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed", "execution_backend": "legacy",
                    "metrics": {
                        "objectives": [{"score": -values["x"], "value": values["x"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                }

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=evaluate):
                run_optimization(spec, config, prepared=prepared, run_root=run_root)
            persisted_spec = load_json(os.path.join(run_root, "spec.json"))
            persisted_status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(persisted_spec["execution_backend"], "legacy")
            self.assertEqual(
                persisted_spec["legacy_guardian_protocol"],
                "crescendo.legacy_maestro_guardian.v1",
            )
            self.assertEqual(
                persisted_status["legacy_guardian_protocol"],
                "crescendo.legacy_maestro_guardian.v1",
            )

    def test_legacy_batch_settings_are_overridden_by_single_run_controller(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 4,
                "persistent_worker_lifetime": "batch", "persistent_batch_size": 4,
                "max_parallel_jobs": 5,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 10, "jobs": 3,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            instances = []
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertEqual(evaluated, list(range(10)))
            self.assertEqual(len(instances), 1)
            self.assertEqual(len(result["candidates"]), 10)
            status = load_json(os.path.join(root, "run", "status.json"))
            self.assertEqual(status["effective_parallel_jobs"], 1)
            self.assertEqual(status["parallel_jobs_override"]["requested"], 3)
            self.assertEqual(status["parallel_jobs_override"]["effective"], 1)
            self.assertEqual(status["worker_lifetime_override"], {
                "requested": "batch", "effective": "run",
                "reason": "scheduled batch restarts are disabled",
            })
            self.assertNotIn("persistent_batch_size", status)

    def test_repeated_persistent_startup_failure_stops_before_whole_population(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "batch", "persistent_batch_size": 4,
                "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "infrastructure_retries": 1,
                "max_infrastructure_replacements": 4,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 8, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "infrastructure_failed",
                    "failure_class": "persistent_startup", "error": "Ocean never reached BEGIN",
                }

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate),
            ):
                with self.assertRaisesRegex(RuntimeError, "startup failed 2 consecutive"):
                    run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertEqual(evaluated, [0, 1])

    def test_stop_after_candidate_preserves_active_plan_for_resume(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
                "source_identity": {
                    "schema_version": "crescendo.source_design_identity.v1",
                    "libraries": [], "fingerprint": "source-a",
                },
                "eda_context": {
                    "cadence": {"version_checks": []},
                    "pdk": {"ok": True, "pdk_name": "PDK", "pdk_adapter": "generic-cdf"},
                    "submission": {"controller": "maestro", "simulator": "spectre"},
                },
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "batch", "persistent_batch_size": 4,
                "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 8, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            first_run_root = os.path.join(root, "first")
            first_calls = []

            def first_evaluate(_controller, run_root, index, generation, values):
                first_calls.append(index)
                with open(os.path.join(run_root, "STOP"), "w") as stream:
                    stream.write("stop requested\n")
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(first_evaluate),
            ):
                stopped = run_optimization(spec, config, prepared=prepared, run_root=first_run_root)
            self.assertEqual(stopped["state"], "stopped")
            self.assertEqual(first_calls, [0])
            self.assertEqual(len(stopped["candidates"]), 1)
            checkpoint = load_json(os.path.join(first_run_root, "checkpoint.json"))
            self.assertIsNotNone(checkpoint["state"]["active_plan"])
            validate_checkpoint(checkpoint, prepared)

            invalid_generation = copy.deepcopy(checkpoint)
            invalid_generation["state"]["active_plan"]["generation"] += 1
            invalid_generation["payload_sha256"] = _checkpoint_checksum(invalid_generation)
            with self.assertRaisesRegex(ValueError, "generation does not match"):
                validate_checkpoint(invalid_generation, prepared)

            invalid_values = copy.deepcopy(checkpoint)
            invalid_values["state"]["active_plan"]["planned"][0][1][0] += 0.125
            invalid_values["payload_sha256"] = _checkpoint_checksum(invalid_values)
            with self.assertRaisesRegex(ValueError, "values do not match"):
                validate_checkpoint(invalid_values, prepared)

            invalid_next_index = copy.deepcopy(checkpoint)
            invalid_next_index["state"]["active_plan"]["next_index"] += 1
            invalid_next_index["payload_sha256"] = _checkpoint_checksum(invalid_next_index)
            with self.assertRaisesRegex(ValueError, "contiguous and match next_index"):
                validate_checkpoint(invalid_next_index, prepared)

            invalid_replacements = copy.deepcopy(checkpoint)
            invalid_replacements["state"]["active_plan"]["replacements"] += 1
            invalid_replacements["payload_sha256"] = _checkpoint_checksum(invalid_replacements)
            with self.assertRaisesRegex(ValueError, "replacement count"):
                validate_checkpoint(invalid_replacements, prepared)

            resumed_calls = []

            def resumed_evaluate(_controller, _run_root, index, generation, values):
                resumed_calls.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(resumed_evaluate),
            ):
                resumed = _coordinated_runner_resume(
                    spec, config, prepared, os.path.join(root, "resumed"), checkpoint,
                )
            self.assertEqual(resumed["state"], "completed")
            self.assertEqual(resumed_calls, [1, 2, 3, 4, 5, 6, 7])
            self.assertEqual(len(resumed["candidates"]), 8)

    def test_invalid_legacy_batch_size_is_ignored(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "batch", "persistent_batch_size": 0,
                "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            instances = []
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(evaluated, [0, 1, 2, 3])
            self.assertEqual(len(instances), 1)
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["worker_lifetime_override"]["effective"], "run")
            self.assertNotIn("persistent_batch_size", status)

    def test_run_lifetime_reuses_one_worker_across_generations_and_checkpoints_before_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 2,
                "persistent_batch_size": 0, "max_parallel_jobs": 2,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 2, "population": 3, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.evaluated = []
                    self.committed = []
                    self.closed = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    return evaluate(
                        self, self.run_root, request["index"], request["generation"],
                        request["values"],
                    )

                def commit_candidate(self, candidate_id):
                    checkpoint = load_json(os.path.join(self.run_root, "checkpoint.json"))
                    durable_ids = set(row["candidate_id"] for row in checkpoint_candidates(checkpoint))
                    if candidate_id not in durable_ids:
                        raise AssertionError("candidate cleanup happened before checkpoint")
                    anchor = load_json(continuation_checkpoint_path(
                        root, checkpoint_continuation_id(checkpoint), create=False,
                    ))
                    anchored_ids = set(
                        row["candidate_id"] for row in checkpoint_candidates(anchor)
                    )
                    if candidate_id not in anchored_ids:
                        raise AssertionError(
                            "candidate cleanup happened before continuation anchor"
                        )
                    if os.path.lexists(continuation_pending_path(
                        root, checkpoint_continuation_id(checkpoint), create=False,
                    )):
                        raise AssertionError(
                            "candidate cleanup happened before pending journal commit"
                        )
                    self.committed.append(candidate_id)
                    return {"ok": True, "state": "cleaned", "recycled": False}

                def close(self):
                    self.status_at_close = load_json(
                        os.path.join(self.run_root, "status.json"),
                    )["state"]
                    self.result_existed_at_close = os.path.exists(
                        os.path.join(self.run_root, "result.json"),
                    )
                    self.closed += 1
                    return _proven_controller_shutdown()

            def evaluate(worker, _run_root, index, generation, values, *_args):
                worker.evaluated.append((generation, index))
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed", "execution_backend": "persistent_maestro",
                    "persistent_worker": {"worker_index": worker.worker_index, "cleanup_pending": True},
                    "metrics": {
                        "objectives": [{"score": -values["x"], "value": values["x"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                }

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController), mock.patch(
                "crescendo_engine.runner.ThreadPoolExecutor",
                side_effect=AssertionError("run-lifetime controller path must not use a thread pool"),
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(len(instances), 1)
            self.assertEqual(len(instances[0].evaluated), 6)
            self.assertEqual(len(instances[0].committed), 6)
            self.assertEqual(instances[0].closed, 1)
            self.assertEqual(instances[0].status_at_close, "finalizing")
            self.assertFalse(instances[0].result_existed_at_close)
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["state"], "completed")
            self.assertEqual(status["checkpoint_revision"], 11)
            self.assertEqual(status["persistent_worker_lifetime"], "optimization_run")
            self.assertEqual(status["requested_parallel_jobs"], 1)
            self.assertEqual(status["effective_parallel_jobs"], 1)
            self.assertEqual(status["persistent_worker_count"], 1)
            self.assertEqual(status["persistent_session_policy"], "single_maestro_for_optimization_run")
            self.assertNotIn("persistent_batch_size", status)
            self.assertEqual(DEFAULTS["runtime"]["persistent_worker_lifetime"], "run")
            self.assertNotIn("persistent_max_workers", DEFAULTS["runtime"])

    def test_pending_journal_write_failure_preserves_raw_candidate_before_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto",
                "max_parallel_jobs": 1, "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            commits = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    candidate_id = "g%03d_c%04d" % (
                        request["generation"], request["index"],
                    )
                    candidate_root = os.path.join(
                        self.run_root, "candidates", candidate_id,
                    )
                    os.makedirs(candidate_root, exist_ok=True)
                    with open(os.path.join(candidate_root, "raw.marker"), "w") as stream:
                        stream.write("must survive failed checkpoint publication")
                    return {
                        "candidate_id": candidate_id,
                        "generation": request["generation"], "index": request["index"],
                        "values": request["values"], "status": "completed",
                        "source_guard": {"ok": True},
                        "persistent_worker": {
                            "worker_index": self.worker_index, "cleanup_pending": True,
                        },
                        "metrics": {
                            "objectives": [{"score": -1.0, "value": 1.0}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -1.0,
                        },
                    }

                def commit_candidate(self, candidate_id):
                    commits.append(candidate_id)
                    return {"ok": True}

                def close(self):
                    return _proven_controller_shutdown()

            from crescendo_engine.runner import persist_continuation_pending as real_persist
            calls = {"count": 0}

            def fail_second_journal(*args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 3:
                    raise OSError("synthetic journal write failure")
                return real_persist(*args, **kwargs)

            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.persist_continuation_pending",
                side_effect=fail_second_journal,
            ):
                with self.assertRaisesRegex(OSError, "journal write failure"):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(commits, [])
            self.assertTrue(os.path.isfile(os.path.join(
                run_root, "candidates", "g000_c0000", "raw.marker",
            )))

    def test_boundary_resource_guard_restarts_clean_epoch_and_continues(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "persistent_health_warmup_samples": 1,
                "persistent_health_baseline_samples": 1,
                "persistent_health_consecutive_limit": 2,
                "persistent_rss_growth_limit_mib": 1,
                "persistent_fd_growth_limit": 128,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.commit_count = 0
                    self.evaluated = []
                    self.closed = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    return {
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "execution_backend": "persistent_maestro",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True, "total_violation": 0.0,
                            "merit": -values["x"],
                        },
                    }

                def commit_candidate(self, _candidate_id):
                    rss_values = [1000, 3000, 3100]
                    rss_kib = rss_values[min(self.commit_count, len(rss_values) - 1)]
                    self.commit_count += 1
                    return {
                        "ok": True, "state": "cleaned", "recycled": False,
                        "health": _proven_session_health(
                            controller_epoch=self.epoch_index,
                            controller_pid=self.process.pid,
                            controller_start_ticks=self._start_ticks,
                            rss_kib=rss_kib,
                        ),
                    }

                def terminate_and_prove(self, reason):
                    return {
                        "reason": reason, "termination_proven": True,
                        "process_still_alive": False,
                        "cleanup": {"ok": True, "removed": True},
                    }

                def restart(self):
                    self.epoch_index += 1
                    self._start_ticks += 100
                    self.process = mock.Mock(pid=self.process.pid + 100)
                    return self

                def close(self):
                    self.closed += 1
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)

            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].evaluated, [0, 1, 2, 3])
            self.assertEqual(instances[0].closed, 1)
            self.assertEqual(result["state"], "completed")
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            rows = checkpoint_candidates(checkpoint)
            self.assertEqual([row["candidate_id"] for row in rows], [
                "g000_c0000", "g000_c0001", "g000_c0002", "g000_c0003",
            ])
            self.assertTrue(all(row["status"] == "completed" for row in rows))
            decision = checkpoint["state"]["persistent_session_health"]
            self.assertTrue(decision["triggered"])
            self.assertEqual(checkpoint["state"]["resource_guard_restart_count"], 1)
            anchor = load_json(continuation_checkpoint_path(
                root, checkpoint_continuation_id(checkpoint), create=False,
            ))
            self.assertEqual(anchor["payload_sha256"], checkpoint["payload_sha256"])
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["state"], "completed")
            self.assertEqual(status["infrastructure_failed_candidates"], 0)
            self.assertEqual(status["resource_guard_recycles"][0]["state"], "restarted")

    def test_active_resource_guard_requeues_same_candidate_without_failure(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_health_warmup_samples": 5,
                "persistent_health_baseline_samples": 3,
                "persistent_health_consecutive_limit": 3,
                "persistent_min_free_disk_mib": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 2, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.current_index = None
                    self.poll_count = 0
                    self.restarts = 0
                    self.terminations = []
                    self.submissions = []
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.current_index = index
                    self.poll_count = 0
                    self.submissions.append((index, dict(values)))
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    self.poll_count += 1
                    if (
                            request["index"] == 0 and self.poll_count >= 2
                            or request["index"] == 1 and self.restarts >= 1):
                        values = request["values"]
                        return {
                            "candidate_id": "g000_c%04d" % request["index"],
                            "generation": 0, "index": request["index"],
                            "values": values, "status": "completed",
                            "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                            "metrics": {
                                "objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True,
                                "total_violation": 0.0, "merit": -values["x"],
                            },
                        }
                    return None

                def sample_health(self):
                    return _proven_session_health(
                        controller_epoch=self.epoch_index,
                        controller_pid=self.process.pid,
                        controller_start_ticks=self._start_ticks,
                        rss_kib=1000, fd_count=40, max_fd_count=40,
                    )

                def commit_candidate(self, _candidate_id):
                    return {"ok": True, "state": "cleaned", "health": self.sample_health()}

                def terminate_and_prove(self, reason):
                    result = {
                        "reason": reason, "termination_proven": True,
                        "process_still_alive": False, "cleanup": {"ok": True},
                    }
                    self.terminations.append(result)
                    return result

                def restart(self):
                    self.restarts += 1
                    self.epoch_index += 1
                    self._start_ticks += 100
                    self.process = mock.Mock(pid=self.process.pid + 100)
                    return self

                def close(self):
                    return _proven_controller_shutdown()

            ticks = iter(range(10000))
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic",
                side_effect=lambda: next(ticks),
            ), mock.patch(
                "crescendo_engine.session_health._nofile_soft_limit", return_value=50,
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)

            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].restarts, 1)
            self.assertEqual(len(instances[0].terminations), 1)
            self.assertEqual([row[0] for row in instances[0].submissions], [0, 1, 1])
            self.assertEqual(instances[0].submissions[1][1], instances[0].submissions[2][1])
            self.assertEqual(result["state"], "completed")
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            rows = checkpoint_candidates(checkpoint)
            self.assertEqual([row["status"] for row in rows], ["completed", "completed"])
            self.assertTrue(all(row.get("failure_class") is None for row in rows))
            decision = checkpoint["state"]["persistent_session_health"]
            self.assertTrue(decision["triggered"])
            self.assertTrue(decision["resource_limits_authorized"])
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(len(status["active_resource_guard_terminations"]), 1)
            self.assertEqual(status["infrastructure_failed_candidates"], 0)

    def test_resource_guard_restart_limit_keeps_pending_candidate_resumable(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_health_warmup_samples": 1,
                "persistent_health_baseline_samples": 1,
                "persistent_health_consecutive_limit": 1,
                "persistent_rss_growth_limit_mib": 1,
                "persistent_resource_guard_max_restarts": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 5, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class RecyclingController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.epoch_commits = 0
                    self.evaluated = []
                    self.restarts = 0

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    return {
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def commit_candidate(self, _candidate_id):
                    rss_kib = 1000 if self.epoch_commits == 0 else 3000
                    self.epoch_commits += 1
                    return {
                        "ok": True, "state": "cleaned",
                        "health": _proven_session_health(
                            controller_epoch=self.epoch_index,
                            controller_pid=self.process.pid,
                            controller_start_ticks=self._start_ticks,
                            rss_kib=rss_kib,
                        ),
                    }

                def terminate_and_prove(self, reason):
                    return dict(_proven_controller_shutdown(), reason=reason)

                def restart(self):
                    self.restarts += 1
                    self.epoch_index += 1
                    self._start_ticks += 100
                    self.process = mock.Mock(pid=self.process.pid + 100)
                    self.epoch_commits = 0
                    return self

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", RecyclingController,
            ):
                with self.assertRaises(PersistentSessionHealthLimit):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)

            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            rows = checkpoint_candidates(checkpoint)
            self.assertEqual([row["index"] for row in rows], [0, 1, 2, 3])
            self.assertTrue(all(row["status"] == "completed" for row in rows))
            self.assertEqual(checkpoint["state"]["resource_guard_restart_count"], 1)
            self.assertEqual(
                checkpoint["state"]["resource_guard_recycles"][-1]["state"],
                "terminated_restart_limit_exhausted",
            )
            resumed_evaluated = []

            class HealthyController(RecyclingController):
                def submit_evaluate(self, index, generation, values, _spec):
                    resumed_evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def commit_candidate(self, _candidate_id):
                    return {
                        "ok": True, "state": "cleaned",
                        "health": _proven_session_health(rss_kib=1000),
                    }

            resumed_root = os.path.join(root, "resumed")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", HealthyController,
            ), mock.patch("crescendo_engine.runner.validate_checkpoint"):
                resumed = _coordinated_runner_resume(
                    spec, config, prepared, resumed_root, checkpoint,
                )
            self.assertEqual(resumed["state"], "completed")
            self.assertEqual(resumed_evaluated, [4])
            resumed_checkpoint = load_json(os.path.join(resumed_root, "checkpoint.json"))
            self.assertEqual(
                resumed_checkpoint["state"]["resource_guard_restart_count"], 1,
            )
            self.assertFalse(any(
                row["status"] == "infrastructure_failed"
                for row in checkpoint_candidates(resumed_checkpoint)
            ))

    def test_unplanned_proven_worker_replacement_fails_without_rewarming(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "persistent_health_warmup_samples": 1,
                "persistent_health_baseline_samples": 1,
                "persistent_health_consecutive_limit": 1,
                "persistent_rss_growth_limit_mib": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.commit_count = 0
                    self.evaluated = []
                    self.rows = []
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    row = {
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }
                    self.rows.append(row)
                    return row

                def commit_candidate(self, _candidate_id):
                    samples = ((0, 900000), (1, 1000), (1, 1500), (1, 3000))
                    local_epoch, rss_kib = samples[self.commit_count]
                    self.commit_count += 1
                    return {
                        "ok": True, "state": "cleaned",
                        "health": _proven_session_health(
                            local_epoch=local_epoch, rss_kib=rss_kib,
                        ),
                    }

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                with self.assertRaises(PersistentSessionHealthLimit):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)

            decision = load_json(os.path.join(run_root, "checkpoint.json"))[
                "state"
            ]["persistent_session_health"]
            self.assertEqual(instances[0].evaluated, [0, 1])
            self.assertEqual(
                decision["identity_transition"], "unexpected_session_identity_change",
            )
            self.assertIn("unexpected_session_identity_change", decision["violations"])
            self.assertEqual(decision["baseline"]["rss_kib"], 900000)

    def test_unproven_session_identity_changes_fail_closed_without_rewarming(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "persistent_health_warmup_samples": 1,
                "persistent_health_baseline_samples": 1,
                "persistent_health_consecutive_limit": 2,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 3, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.commit_count = 0

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    return {
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def commit_candidate(self, _candidate_id):
                    health = _proven_session_health(rss_kib=1000)
                    if self.commit_count:
                        health["rss_kib"] = 1
                        health["session_identity_proven"] = False
                        health["session_identity"] = {
                            "proven": False,
                            "epoch": "untrusted-%d" % self.commit_count,
                        }
                    self.commit_count += 1
                    return {"ok": True, "state": "cleaned", "health": health}

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                with self.assertRaises(PersistentSessionHealthLimit):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)

            decision = load_json(os.path.join(run_root, "checkpoint.json"))[
                "state"
            ]["persistent_session_health"]
            self.assertEqual(decision["identity_transition"], "identity_unproven")
            self.assertFalse(decision["identity_proven"])
            self.assertEqual(decision["baseline"]["rss_kib"], 1000)
            self.assertEqual(decision["violations"], ["session_identity_unproven"])

    def test_authorized_retirement_replacement_resets_resource_baselines(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_health_warmup_samples": 1,
                "persistent_health_baseline_samples": 1,
                "persistent_health_consecutive_limit": 1,
                "persistent_rss_growth_limit_mib": 1,
                "persistent_fd_growth_limit": 10,
                "persistent_log_growth_limit_mib": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.commit_count = 0
                    self.evaluated = []
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    return {
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def commit_candidate(self, _candidate_id):
                    position = self.commit_count
                    self.commit_count += 1
                    if position == 1:
                        return {
                            "ok": True, "state": "cleaned", "recycled": True,
                            "retirement": {
                                "termination_proven": True, "process_still_alive": False,
                                "source_guard": {"ok": True},
                                "cleanup": {"ok": True, "removed": True},
                            },
                        }
                    local_epoch, rss_kib, fd_count, log_bytes = {
                        0: (0, 900000, 500, 9000000),
                        2: (1, 1000, 10, 100),
                        3: (1, 3000, 10, 100),
                    }[position]
                    return {
                        "ok": True, "state": "cleaned", "recycled": False,
                        "health": _proven_session_health(
                            local_epoch=local_epoch, rss_kib=rss_kib,
                            fd_count=fd_count, max_fd_count=fd_count,
                            log_bytes=log_bytes,
                        ),
                    }

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)

            decision = load_json(os.path.join(run_root, "checkpoint.json"))[
                "state"
            ]["persistent_session_health"]
            self.assertEqual(instances[0].evaluated, [0, 1, 2, 3])
            self.assertEqual(decision["baseline"], {
                "rss_kib": 1000, "fd_count": 10, "log_bytes": 100,
            })
            self.assertEqual(decision["violations"], ["rss_growth"])
            self.assertEqual(result["state"], "completed")

    def test_active_guard_keeps_same_session_baseline_across_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_health_warmup_samples": 1,
                "persistent_health_baseline_samples": 1,
                "persistent_health_consecutive_limit": 1,
                "persistent_rss_growth_limit_mib": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 3, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.commit_count = 0
                    self.current_index = None
                    self.terminations = []
                    self.busy_polls = 0
                    self.active_samples = 0
                    self.restarts = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.current_index = index
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    if request["index"] == 2 and self.busy_polls < 2:
                        self.busy_polls += 1
                        return None
                    values = request["values"]
                    return {
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def sample_health(self):
                    self.active_samples += 1
                    return _proven_session_health(
                        controller_epoch=self.epoch_index,
                        controller_pid=self.process.pid,
                        controller_start_ticks=self._start_ticks,
                        rss_kib=2500 if self.restarts == 0 else 1000,
                    )

                def commit_candidate(self, _candidate_id):
                    rss_kib = (
                        (1000, 1800, 1800)[self.commit_count]
                        if self.restarts == 0 else 1000
                    )
                    self.commit_count += 1
                    return {
                        "ok": True, "state": "cleaned",
                        "health": _proven_session_health(
                            controller_epoch=self.epoch_index,
                            controller_pid=self.process.pid,
                            controller_start_ticks=self._start_ticks,
                            rss_kib=rss_kib,
                        ),
                    }

                def terminate_and_prove(self, reason):
                    result = dict(_proven_controller_shutdown(), reason=reason)
                    self.terminations.append(result)
                    return result

                def restart(self):
                    self.restarts += 1
                    self.epoch_index += 1
                    self._start_ticks += 100
                    self.process = mock.Mock(pid=self.process.pid + 100)
                    return self

                def close(self):
                    return _proven_controller_shutdown()

            from crescendo_engine.session_health import PersistentSessionHealthGuard
            health_decisions = []
            original_observe = PersistentSessionHealthGuard.observe

            def record_health(guard, health, comparable=True):
                decision = original_observe(guard, health, comparable=comparable)
                health_decisions.append((health["rss_kib"], comparable, copy.deepcopy(decision)))
                return decision

            ticks = iter(range(10000))
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=lambda: next(ticks),
            ), mock.patch.object(PersistentSessionHealthGuard, "observe", record_health):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)

            busy_decisions = [decision for rss, comparable, decision in health_decisions
                              if rss == 2500 and comparable is False]
            self.assertTrue(busy_decisions)
            self.assertTrue(all(not decision["triggered"] for decision in busy_decisions))
            decision = [decision for rss, comparable, decision in health_decisions
                        if rss == 1800 and comparable is True][-1]
            self.assertEqual(decision["baseline"]["rss_kib"], 1000)
            self.assertEqual(decision["violations"], [])
            self.assertGreaterEqual(instances[0].active_samples, 1)
            self.assertEqual(len(instances[0].terminations), 0)
            self.assertEqual(instances[0].restarts, 0)
            self.assertEqual(result["state"], "completed")

    def test_initial_identity_pending_does_not_use_the_consecutive_failure_limit(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_identity_startup_timeout_seconds": 10,
                "persistent_health_consecutive_limit": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.poll_count = 0
                    self.sample_count = 0
                    self.terminations = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    self.poll_count += 1
                    if self.poll_count < 18:
                        return None
                    values = request["values"]
                    return {
                        "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def sample_health(self):
                    self.sample_count += 1
                    if self.sample_count <= 3:
                        return {
                            "alive": True, "tree_complete": True,
                            "storage_complete": True, "logs_complete": True,
                            "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                            "free_disk_mib": 2000,
                            "session_identity_proven": False,
                            "session_identity": {
                                "proven": False,
                                "reason": "expected exactly one active worker epoch, found 0",
                            },
                        }
                    return _proven_session_health()

                def commit_candidate(self, _candidate_id):
                    return {"ok": True, "state": "cleaned", "health": _proven_session_health()}

                def terminate_and_prove(self, _reason):
                    self.terminations += 1
                    return _proven_controller_shutdown()

                def close(self):
                    return _proven_controller_shutdown()

            clock = {"value": 0.0}

            def monotonic():
                value = clock["value"]
                clock["value"] += 0.1
                return value

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=monotonic,
            ):
                result = run_optimization(
                    spec, config, prepared=prepared, run_root=os.path.join(root, "run"),
                )

            self.assertEqual(result["state"], "completed")
            self.assertGreaterEqual(instances[0].sample_count, 3)
            self.assertEqual(instances[0].terminations, 0)

    def test_initial_identity_pending_has_a_bounded_startup_timeout(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_identity_startup_timeout_seconds": 1,
                "ocean_startup_timeout_seconds": 1,
                "persistent_health_consecutive_limit": 99,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, _request):
                    return None

                def sample_health(self):
                    return {
                        "alive": True, "tree_complete": True,
                        "storage_complete": True, "logs_complete": True,
                        "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                        "free_disk_mib": 2000,
                        "session_identity_proven": False,
                        "session_identity": {
                            "proven": False,
                            "reason": "expected exactly one active worker epoch, found 0",
                        },
                    }

                def terminate_and_prove(self, _reason):
                    return _proven_controller_shutdown()

                def close(self):
                    return _proven_controller_shutdown()

            ticks = iter(range(10000))
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=lambda: next(ticks),
            ):
                with self.assertRaises(PersistentSessionHealthLimit):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)

            decision = load_json(os.path.join(run_root, "checkpoint.json"))[
                "state"
            ]["persistent_session_health"]
            self.assertEqual(decision["state"], "identity_startup_timeout")
            self.assertIn("session_identity_startup_timeout", decision["violations"])
            self.assertEqual(
                decision["identity_unproven_reason"]["code"],
                "worker_epoch_not_materialized",
            )

    def test_proven_retirement_allows_one_exact_lazy_epoch_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_identity_startup_timeout_seconds": 10,
                "persistent_health_consecutive_limit": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 2, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.current_index = None
                    self.poll_count = 0
                    self.commit_count = 0
                    self.sample_count = 0
                    self.terminations = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    if index == 1:
                        clock["value"] += 1000.0
                    self.current_index = index
                    self.poll_count = 0
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    self.poll_count += 1
                    if request["index"] == 1 and self.poll_count < 18:
                        return None
                    values = request["values"]
                    row = {
                        "candidate_id": "g%03d_c%04d" % (request["generation"], request["index"]),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }
                    if request["index"] == 0:
                        _write_terminal_wal(self.run_root, row)
                    return row

                def sample_health(self):
                    self.sample_count += 1
                    if self.current_index == 1 and self.sample_count <= 3:
                        return {
                            "alive": True, "tree_complete": True,
                            "storage_complete": True, "logs_complete": True,
                            "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                            "free_disk_mib": 2000,
                            "session_identity_proven": False,
                            "session_identity": {
                                "proven": False,
                                "reason": "expected exactly one active worker epoch, found 0",
                            },
                        }
                    return _proven_session_health(local_epoch=1)

                def commit_candidate(self, _candidate_id):
                    self.commit_count += 1
                    if self.commit_count == 1:
                        return {
                            "ok": True, "state": "cleaned", "recycled": True,
                            "health": {
                                "alive": True, "tree_complete": True,
                                "storage_complete": True, "logs_complete": True,
                                "pid": 123, "pid_start_ticks": 456,
                                "controller_epoch": 0, "free_disk_mib": 2000,
                                "session_identity_proven": False,
                                "session_identity": {
                                    "proven": False,
                                    "reason": (
                                        "FileNotFoundError: No such file or directory: "
                                        "'/secret/raw/worker_state.json'"
                                    ),
                                },
                            },
                            "retirement": {
                                "termination_proven": True,
                                "process_still_alive": False,
                                "source_guard": {"ok": True},
                                "cleanup": {"ok": True, "removed": True},
                            },
                        }
                    return {
                        "ok": True, "state": "cleaned", "recycled": False,
                        "health": _proven_session_health(local_epoch=1),
                    }

                def terminate_and_prove(self, _reason):
                    self.terminations += 1
                    return _proven_controller_shutdown()

                def close(self):
                    return _proven_controller_shutdown()

            clock = {"value": 0.0}

            def monotonic():
                value = clock["value"]
                clock["value"] += 0.1
                return value

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=monotonic,
            ):
                result = run_optimization(
                    spec, config, prepared=prepared, run_root=os.path.join(root, "run"),
                )

            self.assertEqual(result["state"], "completed")
            self.assertEqual(instances[0].commit_count, 2)
            self.assertEqual(instances[0].terminations, 0)
            persisted = load_json(os.path.join(
                root, "run", "candidates", "g000_c0000", "candidate.json",
            ))
            cleanup_text = json.dumps(persisted["persistent_worker"]["cleanup"])
            self.assertNotIn("/secret/raw", cleanup_text)
            self.assertIn("worker_state_not_materialized", cleanup_text)
            self.assertEqual(
                persisted["persistent_worker"]["session_health"]["state"],
                "replacement_waiting_for_evaluate",
            )

    def test_active_health_sampling_is_throttled_and_does_not_kill_healthy_first_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_health_consecutive_limit": 3,
                "persistent_min_free_disk_mib": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.poll_count = 0
                    self.sample_count = 0
                    self.terminations = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    self.poll_count += 1
                    if self.poll_count < 12:
                        return None
                    values = request["values"]
                    return {
                        "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def health(self):
                    return _proven_session_health(
                        rss_kib=999999, fd_count=40, max_fd_count=40,
                    )

                def sample_health(self):
                    self.sample_count += 1
                    return self.health()

                def commit_candidate(self, _candidate_id):
                    return {"ok": True, "state": "cleaned", "health": self.health()}

                def terminate_and_prove(self, _reason):
                    self.terminations += 1
                    return _proven_controller_shutdown()

                def close(self):
                    return _proven_controller_shutdown()

            clock = {"value": 0.0}

            def monotonic():
                value = clock["value"]
                clock["value"] += 0.1
                return value

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=monotonic,
            ), mock.patch(
                "crescendo_engine.session_health._nofile_soft_limit", return_value=50,
            ):
                result = run_optimization(
                    spec, config, prepared=prepared, run_root=os.path.join(root, "run"),
                )
            self.assertEqual(result["state"], "completed")
            self.assertEqual(instances[0].poll_count, 12)
            self.assertGreaterEqual(instances[0].sample_count, 1)
            self.assertLess(instances[0].sample_count, instances[0].poll_count // 2)
            self.assertEqual(instances[0].terminations, 0)

    def test_active_disk_guard_checkpoints_first_candidate_without_restart(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_min_free_disk_mib": 100,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.restarts = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, _request):
                    return None

                def sample_health(self):
                    return {
                        "alive": True, "tree_complete": True,
                        "storage_complete": True, "logs_complete": True,
                        "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                        "free_disk_mib": 1,
                    }

                def terminate_and_prove(self, reason):
                    return {
                        "reason": reason, "termination_proven": True,
                        "process_still_alive": False, "cleanup": {"ok": True},
                    }

                def restart(self):
                    self.restarts += 1

                def close(self):
                    return _proven_controller_shutdown()

            ticks = iter(range(10000))
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=lambda: next(ticks),
            ):
                with self.assertRaises(PersistentSessionHealthLimit):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(instances[0].restarts, 0)
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            rows = checkpoint_candidates(checkpoint)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["failure_class"], "resource_guard")
            self.assertFalse(
                checkpoint["state"]["persistent_session_health"]["resource_limits_authorized"]
            )

    def test_identity_pending_keeps_process_tree_and_log_guards_active(self):
        cases = (
            ("dead", "owned_process_tree_dead"),
            ("tree", "owned_process_tree_incomplete"),
            ("log", "log_growth"),
        )
        for mode, expected_violation in cases:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                prepared = {
                    "setup": {"tests": [], "corners": []},
                    "hierarchy": {"designs": []},
                    "parameters": [{
                        "parameter_id": "x", "backend": "schematic", "optimizable": True,
                    }],
                    "outputs": [{"key": "main::gain"}],
                }
                config = {"runtime": {
                    "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                    "candidate_timeout_seconds": 60,
                    "persistent_active_health_interval_seconds": 0.5,
                    "persistent_identity_startup_timeout_seconds": 90,
                    "persistent_health_warmup_samples": 1,
                    "persistent_health_baseline_samples": 1,
                    "persistent_health_consecutive_limit": 1,
                    "persistent_log_growth_limit_mib": 1,
                    "persistent_min_free_disk_mib": 1,
                }}
                spec = {
                    "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                    "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                    "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                    "constraints": [],
                }

                class FakeController(object):
                    def __init__(self, run_root, worker_index, *_args):
                        self.run_root = run_root
                        self.worker_index = worker_index
                        self.epoch_index = 0
                        self._start_ticks = 456
                        self.process = mock.Mock(pid=123)
                        self.samples = 0

                    def start(self):
                        return self

                    def submit_evaluate(self, index, generation, values, _spec):
                        return {"index": index, "generation": generation, "values": values}

                    def poll(self, _request):
                        return None

                    def sample_health(self):
                        self.samples += 1
                        health = {
                            "alive": mode != "dead",
                            "tree_complete": mode != "tree",
                            "storage_complete": True, "logs_complete": True,
                            "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                            "free_disk_mib": 2000,
                            "log_bytes": (
                                2 * 1024 * 1024
                                if mode == "log" and self.samples > 1 else 0
                            ),
                            "session_identity_proven": False,
                            "session_identity": {
                                "proven": False,
                                "reason": "expected exactly one active worker epoch, found 0",
                            },
                        }
                        return health

                    def terminate_and_prove(self, _reason):
                        return _proven_controller_shutdown()

                    def close(self):
                        return _proven_controller_shutdown()

                ticks = iter(range(10000))
                run_root = os.path.join(root, "run")
                with mock.patch(
                    "crescendo_engine.runner.PersistentControllerProxy", FakeController,
                ), mock.patch(
                    "crescendo_engine.runner.time.monotonic", side_effect=lambda: next(ticks),
                ):
                    with self.assertRaises(PersistentSessionHealthLimit):
                        run_optimization(spec, config, prepared=prepared, run_root=run_root)

                decision = load_json(os.path.join(run_root, "checkpoint.json"))[
                    "state"
                ]["persistent_session_health"]
                self.assertIn(expected_violation, decision["violations"])

    def test_unsafe_retirement_does_not_authorize_a_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 2, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.evaluated = []
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    self.evaluated.append(index)
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    return {
                        "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                        "values": values, "status": "completed",
                        "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }

                def commit_candidate(self, _candidate_id):
                    return {
                        "ok": False, "state": "retirement_failed", "recycled": True,
                        "retirement": {
                            "termination_proven": False, "process_still_alive": True,
                            "source_guard": {"ok": True},
                            "cleanup": {"ok": False, "removed": False},
                        },
                    }

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                with self.assertRaisesRegex(RuntimeError, "supervisor remained alive"):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)

            self.assertEqual(instances[0].evaluated, [0])

    def test_nonrecyclable_active_guard_recovers_terminal_wal_before_stopping(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "persistent_active_health_interval_seconds": 0.5,
                "persistent_min_free_disk_mib": 100,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.epoch_index = 0
                    self._start_ticks = 456
                    self.process = mock.Mock(pid=123)
                    self.wal_written = False
                    self.restarts = 0

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    if not self.wal_written:
                        values = request["values"]
                        _write_terminal_wal(self.run_root, {
                            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                            "values": values, "status": "completed",
                            "source_guard": {"ok": True, "libraries": []},
                            "metrics": {
                                "objectives": [{
                                    "output_key": "main::gain", "value": values["x"],
                                    "score": -values["x"],
                                }],
                                "constraints": [], "feasible": True,
                                "total_violation": 0.0, "merit": -values["x"],
                            },
                        })
                        self.wal_written = True
                    return None

                def sample_health(self):
                    return {
                        "alive": True, "tree_complete": True,
                        "storage_complete": True, "logs_complete": True,
                        "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                        "free_disk_mib": 1,
                    }

                def terminate_and_prove(self, reason):
                    return {
                        "reason": reason, "termination_proven": True,
                        "process_still_alive": False, "cleanup": {"ok": True},
                    }

                def restart(self):
                    self.restarts += 1
                    self.epoch_index += 1
                    self._start_ticks += 100
                    self.process = mock.Mock(pid=self.process.pid + 100)
                    return self

                def close(self):
                    return _proven_controller_shutdown()

            ticks = iter(range(10000))
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=lambda: next(ticks),
            ):
                with self.assertRaises(PersistentSessionHealthLimit):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            rows = checkpoint_candidates(checkpoint)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "completed")
            self.assertIsNone(rows[0].get("failure_class"))
            status = load_json(os.path.join(run_root, "status.json"))
            recovery = status["active_resource_guard_terminations"][0][
                "terminal_wal_recovery"
            ]
            self.assertTrue(recovery["recovered"])
            self.assertEqual(status["resource_guard_restart_count"], 0)

    def test_controller_process_failure_is_checkpointed_then_restarted_for_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "max_infrastructure_replacements": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.failed = False
                    self.restarts = 0

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    if not self.failed:
                        self.failed = True
                        candidate_root = os.path.join(
                            self.run_root, "candidates", "g000_c0000",
                        )
                        for name in (
                            "persistent_worker_candidate.il",
                            "persistent_worker_candidate.json",
                            "persistent_worker_candidate.status.tsv",
                            "pristine_restore.records",
                            "schematic_application.records",
                            "schematic_baseline.records",
                        ):
                            with open(os.path.join(candidate_root, name), "w") as stream:
                                stream.write("raw")
                        raise PersistentControllerProcessError("synthetic controller crash")
                    values = request["values"]
                    return {
                        "candidate_id": "g%03d_c%04d" % (request["generation"], request["index"]),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True, "total_violation": 0.0,
                            "merit": -values["x"],
                        },
                    }

                def terminate_and_prove(self, _reason):
                    return {
                        "termination_proven": True, "process_still_alive": False,
                        "cleanup": {"ok": True, "removed": False},
                    }

                def restart(self):
                    checkpoint = load_json(os.path.join(self.run_root, "checkpoint.json"))
                    self.assert_durable = any(
                        row["candidate_id"] == "g000_c0000"
                        for row in checkpoint_candidates(checkpoint)
                    )
                    self.restarts += 1
                    return self

                def commit_candidate(self, _candidate_id):
                    return {"ok": True, "state": "cleaned", "recycled": False}

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual([row["status"] for row in result["candidates"]], [
                "infrastructure_failed", "completed",
            ])
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(len(status["scheduler_watchdog_terminations"]), 1)
            failure_root = os.path.join(run_root, "candidates", "g000_c0000")
            self.assertEqual(
                sorted(os.listdir(failure_root)), ["candidate.json", "timing.json"],
            )
            persisted = load_json(os.path.join(failure_root, "candidate.json"))
            self.assertEqual(
                persisted["persistent_worker"]["candidate_cleanup"]["proof_kind"],
                "controller_termination_proven",
            )

    def test_external_environment_remote_failure_stops_without_penalty_or_resample(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic", "optimizable": True,
                }],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto",
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 5,
            }}
            spec = {
                "algorithm": "random", "generations": 2, "population": 1, "jobs": 1,
                "variables": [{
                    "parameter_id": "x", "minimum": 0.0, "maximum": 1.0,
                }],
                "objectives": [{
                    "output_key": "main::gain", "direction": "maximize",
                }],
                "constraints": [],
            }
            instances = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.restarts = 0
                    self.terminations = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, _request):
                    raise PersistentControllerRemoteError(
                        "ExternalEdaEnvironmentChanged: model content changed",
                        remote_error_type="ExternalEdaEnvironmentChanged",
                        failure_class="external_environment", fatal_run=True,
                    )

                def terminate_and_prove(self, _reason):
                    self.terminations += 1
                    return {
                        "termination_proven": True, "process_still_alive": False,
                        "cleanup": {"ok": True, "removed": True},
                    }

                def restart(self):
                    self.restarts += 1
                    raise AssertionError("external environment failure must not restart")

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ):
                with self.assertRaisesRegex(
                    ExternalEdaEnvironmentChanged, "model content changed",
                ):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].terminations, 1)
            self.assertEqual(instances[0].restarts, 0)
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            self.assertEqual(checkpoint_candidates(checkpoint), [])
            active_plan = checkpoint["state"]["active_plan"]
            self.assertEqual(active_plan["planned"][0][0], 0)
            self.assertFalse(os.path.lexists(os.path.join(
                run_root, "candidates", "g000_c0000",
            )))
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(status["state"], "failed")
            self.assertEqual(len(status["external_environment_terminations"]), 1)
            self.assertNotIn("scheduler_watchdog_terminations", status)

    def test_controller_poll_failure_recovers_durable_terminal_candidate_wal(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "max_infrastructure_replacements": 1,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            def evaluate(_controller, run_root, index, generation, values):
                candidate_id = "g%03d_c%04d" % (generation, index)
                row = {
                    "candidate_id": candidate_id, "generation": generation, "index": index,
                    "values": values, "status": "completed",
                    "metrics": {
                        "objectives": [{
                            "output_key": "main::gain", "value": values["x"],
                            "score": -values["x"],
                        }],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                    "source_guard": {"ok": True, "libraries": []},
                }
                _write_terminal_wal(run_root, row)
                raise PersistentControllerProcessError("response lost after durable terminal WAL")

            instances = []
            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual([row["status"] for row in result["candidates"]], ["completed"])
            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].restarts, 1)
            status = load_json(os.path.join(run_root, "status.json"))
            recovery = status["scheduler_watchdog_terminations"][0]["terminal_wal_recovery"]
            self.assertTrue(recovery["recovered"])
            self.assertEqual(recovery["candidate_id"], "g000_c0000")
            persisted = load_json(os.path.join(
                run_root, "candidates", "g000_c0000", "candidate.json",
            ))
            self.assertTrue(persisted["persistent_worker"]["wal_recovery"]["recovered"])
            self.assertFalse(persisted["persistent_worker"]["cleanup_pending"])

    def test_user_stop_terminates_active_controller_without_waiting_for_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 600,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.termination = None

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    item = _persistent_running_row(
                        self.run_root, index, generation, values,
                    )
                    with open(os.path.join(item["root"], "raw.rdb"), "w") as stream:
                        stream.write("raw simulation result\n")
                    with open(os.path.join(self.run_root, "STOP"), "w") as stream:
                        stream.write("stop\n")
                    return {"index": index, "generation": generation, "values": values}

                def request_native_batch_stop(self, reason):
                    self.native_stop_seen = True
                    return {"ok": True, "native_stop_proven": True, "reason": reason}

                def terminate_and_prove(self, reason):
                    assert self.native_stop_seen, "native Stop must precede controller termination"
                    self.termination = {
                        "reason": reason, "termination_proven": True,
                        "process_still_alive": False,
                        "cleanup": {"ok": True, "removed": True},
                    }
                    return self.termination

                def close(self):
                    return self.termination

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(result["state"], "stopped")
            self.assertEqual(result["stopped_reason"], "user_stop")
            self.assertFalse(os.path.lexists(os.path.join(
                run_root, "candidates", "g000_c0000",
            )))
            status = load_json(os.path.join(run_root, "status.json"))
            self.assertEqual(len(status["controller_stop_terminations"]), 1)
            self.assertTrue(status["controller_stop_terminations"][0]["native_stop"]["native_stop_proven"])
            cleanup = status["controller_stop_terminations"][0]["candidate_cleanup"]
            self.assertTrue(cleanup["ok"])
            self.assertTrue(cleanup["removed"])

    def test_user_stop_recovers_terminal_wal_before_saving_checkpoint(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 600,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.termination = None

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    row = {
                        "candidate_id": "g%03d_c%04d" % (generation, index),
                        "generation": generation, "index": index, "values": values,
                        "status": "completed", "source_guard": {"ok": True, "libraries": []},
                        "metrics": {
                            "objectives": [{
                                "output_key": "main::gain", "value": values["x"],
                                "score": -values["x"],
                            }],
                            "constraints": [], "feasible": True, "total_violation": 0.0,
                            "merit": -values["x"],
                        },
                    }
                    _write_terminal_wal(self.run_root, row)
                    with open(os.path.join(self.run_root, "STOP"), "w") as stream:
                        stream.write("stop after terminal WAL\n")
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, _request):
                    raise AssertionError("STOP must be handled before polling")

                def request_native_batch_stop(self, reason):
                    self.native_stop_seen = True
                    return {"ok": True, "native_stop_proven": True, "reason": reason}

                def terminate_and_prove(self, reason):
                    assert self.native_stop_seen, "native Stop must precede controller termination"
                    self.termination = {
                        "reason": reason, "termination_proven": True,
                        "process_still_alive": False,
                        "cleanup": {"ok": True, "removed": True},
                    }
                    return self.termination

                def close(self):
                    return self.termination

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(result["state"], "stopped")
            self.assertEqual([row["status"] for row in result["candidates"]], ["completed"])
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            self.assertEqual(
                [row["candidate_id"] for row in checkpoint_candidates(checkpoint)],
                ["g000_c0000"],
            )
            self.assertIsNotNone(checkpoint["state"]["active_plan"])
            status = load_json(os.path.join(run_root, "status.json"))
            recovery = status["controller_stop_terminations"][0]["terminal_wal_recovery"]
            self.assertTrue(recovery["recovered"])

    def test_safe_retirement_clears_cleanup_pending_after_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    row = {
                        "candidate_id": "g%03d_c%04d" % (request["generation"], request["index"]),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "source_guard": {"ok": True, "libraries": []},
                        "persistent_worker": {"worker_index": self.worker_index, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True, "total_violation": 0.0,
                            "merit": -values["x"],
                        },
                    }
                    _write_terminal_wal(self.run_root, row)
                    return row

                def commit_candidate(self, _candidate_id):
                    return {
                        "ok": False, "state": "cleanup_failed_recycled",
                        "retirement": {
                            "termination_proven": True, "process_still_alive": False,
                            "source_guard": {"ok": True},
                            "cleanup": {"ok": True, "removed": True},
                        },
                    }

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                result = run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(result["state"], "completed")
            from crescendo_engine.run_data_store import load_dataset
            persisted = next(row for row in load_dataset(os.path.join(run_root, "optimization.crescendo"))["candidates"]
                             if row["candidate_id"] == "g000_c0000")
            self.assertFalse(persisted["persistent_worker"]["cleanup_pending"])
            self.assertEqual(
                persisted["persistent_worker"]["cleanup"]["state"],
                "cleanup_failed_recycled",
            )

    def test_cleanup_exception_is_checkpointed_with_pending_state_before_abort(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    values = request["values"]
                    row = {
                        "candidate_id": "g%03d_c%04d" % (request["generation"], request["index"]),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "source_guard": {"ok": True, "libraries": []},
                        "persistent_worker": {"worker_index": self.worker_index, "cleanup_pending": True},
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True, "total_violation": 0.0,
                            "merit": -values["x"],
                        },
                    }
                    _write_terminal_wal(self.run_root, row)
                    return row

                def commit_candidate(self, _candidate_id):
                    raise RuntimeError("synthetic cleanup RPC failure")

                def close(self):
                    return _proven_controller_shutdown()

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                with self.assertRaisesRegex(RuntimeError, "cleanup raised an exception"):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            self.assertEqual(
                [row["candidate_id"] for row in checkpoint_candidates(checkpoint)],
                ["g000_c0000"],
            )
            persisted = load_json(os.path.join(
                run_root, "candidates", "g000_c0000", "candidate.json",
            ))
            cleanup = persisted["persistent_worker"]["cleanup"]
            self.assertTrue(persisted["persistent_worker"]["cleanup_pending"])
            self.assertEqual(cleanup["state"], "cleanup_exception")
            self.assertIn("synthetic cleanup RPC failure", cleanup["error"])

    def test_unproven_cleanup_retirement_remains_pending_and_aborts(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            def evaluate(_controller, run_root, index, generation, values):
                row = {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed", "source_guard": {"ok": True, "libraries": []},
                    "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                    "metrics": {
                        "objectives": [{"score": -values["x"], "value": values["x"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                }
                _write_terminal_wal(run_root, row)
                return row

            Controller = _fake_run_controller(evaluate)

            def unsafe_cleanup(_self, _candidate_id):
                return {
                    "ok": False, "state": "cleanup_failed",
                    "retirement": {
                        "termination_proven": False, "process_still_alive": False,
                        "source_guard": {"ok": True},
                        "cleanup": {"ok": False, "removed": False},
                    },
                }

            Controller.commit_candidate = unsafe_cleanup
            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", Controller):
                with self.assertRaisesRegex(RuntimeError, "termination was not proven"):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            self.assertEqual(len(checkpoint_candidates(checkpoint)), 1)
            persisted = load_json(os.path.join(
                run_root, "candidates", "g000_c0000", "candidate.json",
            ))
            self.assertTrue(persisted["persistent_worker"]["cleanup_pending"])
            self.assertEqual(persisted["persistent_worker"]["cleanup"]["state"], "cleanup_failed")

    def test_negative_infrastructure_replacement_limit_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {
                "run_root": root, "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60, "max_infrastructure_replacements": -1,
            }}
            spec = {
                "evaluator": "mock", "algorithm": "random", "generations": 1,
                "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize"}],
                "constraints": [],
            }
            with self.assertRaisesRegex(ValueError, "must be non-negative"):
                run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))

    def test_mock_evaluator_is_normalized_before_candidate_execution(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}], "corners": []},
                "hierarchy": {"designs": []},
                "parameters": [
                    {"parameter_id": "x", "backend": "schematic", "optimizable": True},
                ],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {
                "run_root": root, "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 0,
            }}
            spec = {
                "evaluator": " MOCK ", "algorithm": "random",
                "generations": 1, "population": 1, "jobs": 1,
                "variables": [
                    {"parameter_id": "x", "minimum": 0.0, "maximum": 1.0},
                ],
                "objectives": [
                    {"output_key": "mock::cost", "direction": "minimize"},
                ],
                "constraints": [],
            }
            run_root = os.path.join(root, "run")

            result = run_optimization(
                spec, config, prepared=prepared, run_root=run_root,
            )

            self.assertEqual(result["state"], "completed")
            checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
            self.assertEqual(checkpoint["spec"]["evaluator"], "mock")

    def test_run_lifetime_serializes_requested_parallel_jobs_through_one_controller(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 2,
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 2,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 2,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            instances = []
            tell_orders = []

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index
                    self.evaluated = []
                    self.committed = []
                    self.closed = 0
                    instances.append(self)

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    self.evaluated.append(request["index"])
                    return evaluate(
                        self, self.run_root, request["index"], request["generation"],
                        request["values"],
                    )

                def commit_candidate(self, candidate_id):
                    self.committed.append(candidate_id)
                    return {"ok": True, "state": "cleaned", "recycled": False}

                def close(self):
                    self.closed += 1
                    return _proven_controller_shutdown()

            def evaluate(worker, _run_root, index, generation, values, *_args):
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed", "execution_backend": "persistent_maestro",
                    "persistent_worker": {"worker_index": worker.worker_index},
                    "metrics": {
                        "objectives": [{"score": -values["x"], "value": values["x"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                }

            original_tell = PopulationGenerator.tell

            def recording_tell(generator, rows):
                tell_orders.append([row["index"] for row in rows])
                return original_tell(generator, rows)

            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController), mock.patch.object(
                PopulationGenerator, "tell", new=recording_tell,
            ):
                result = run_optimization(
                    spec, config, prepared=prepared, run_root=os.path.join(root, "run"),
                )
            self.assertEqual(result["state"], "completed")
            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].evaluated, [0, 1, 2, 3])
            self.assertEqual(tell_orders, [[0, 1, 2, 3]])
            self.assertEqual(instances[0].closed, 1)
            status = load_json(os.path.join(root, "run", "status.json"))
            self.assertEqual(status["requested_parallel_jobs"], 2)
            self.assertEqual(status["effective_parallel_jobs"], 1)
            self.assertEqual(status["persistent_worker_count"], 1)
            self.assertEqual(status["parallel_execution_policy"], "serialized_single_maestro")
            self.assertEqual(status["parallel_jobs_override"]["effective"], 1)

    def test_run_lifetime_closes_every_worker_when_candidate_evaluation_raises(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 2,
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 2,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 2, "jobs": 2,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                if len(evaluated) == 1:
                    raise RuntimeError("candidate evaluator crashed")
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed",
                    "metrics": {"objectives": [{"score": -values["x"], "value": values["x"]}],
                                "constraints": [], "feasible": True, "total_violation": 0.0,
                                "merit": -values["x"]},
                }

            instances = []
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate, instances),
            ):
                result = run_optimization(
                    spec, config, prepared=prepared, run_root=os.path.join(root, "run"),
                )
            self.assertEqual(result["state"], "completed")
            self.assertEqual(evaluated, [0, 1, 2])
            self.assertEqual(
                [row["status"] for row in result["candidates"]],
                ["infrastructure_failed", "completed", "completed"],
            )
            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].restarts, 1)
            self.assertEqual(instances[0].closed, 1)

    def test_run_lifetime_never_reports_completed_when_worker_shutdown_is_unsafe(self):
        with tempfile.TemporaryDirectory() as root:
            close_observations = []
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "run", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            class FakeController(object):
                def __init__(self, run_root, worker_index, *_args):
                    self.run_root = run_root
                    self.worker_index = worker_index

                def start(self):
                    return self

                def submit_evaluate(self, index, generation, values, _spec):
                    return {"index": index, "generation": generation, "values": values}

                def poll(self, request):
                    return evaluate(
                        self, self.run_root, request["index"], request["generation"],
                        request["values"],
                    )

                def commit_candidate(self, _candidate_id):
                    return {"ok": True, "state": "cleaned", "recycled": False}

                def close(self):
                    close_observations.append({
                        "state": load_json(os.path.join(self.run_root, "status.json"))["state"],
                        "result_exists": os.path.exists(os.path.join(self.run_root, "result.json")),
                    })
                    return {
                        "termination_proven": False,
                        "process_still_alive": True,
                        "source_guard": {"ok": True},
                        "cleanup": {"ok": False, "removed": False},
                    }

            def evaluate(worker, _run_root, index, generation, values, *_args):
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "completed", "persistent_worker": {"worker_index": worker.worker_index},
                    "metrics": {
                        "objectives": [{"score": -values["x"], "value": values["x"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0,
                        "merit": -values["x"],
                    },
                }

            run_root = os.path.join(root, "run")
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController):
                with self.assertRaisesRegex(RuntimeError, "shutdown/cleanup failed"):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(close_observations, [{"state": "finalizing", "result_exists": False}])
            self.assertEqual(load_json(os.path.join(run_root, "status.json"))["state"], "failed")
            self.assertEqual(load_json(os.path.join(run_root, "result.json"))["state"], "failed")
            self.assertFalse(load_json(os.path.join(run_root, "worker_shutdown.json"))["workers"][0]["ok"])

    def test_post_done_metric_failure_cleans_without_recycling_verified_worker(self):
        with tempfile.TemporaryDirectory() as root:
            candidate_root = os.path.join(root, "candidates", "g000_c0000")
            os.makedirs(candidate_root)

            class Controller(object):
                epoch = "epoch-1"
                spectre_log = os.path.join(root, "spectre.log")
                pending_cleanup = {"candidate_id": "g000_c0000"}

                def require_candidate_time(self, _artifact, _operation):
                    return 30.0

                def evaluate(self, candidate_id, _values, requested_root, timeout_seconds=None):
                    return {
                        "candidate_id": candidate_id, "candidate_root": requested_root,
                        "states": ["ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY", "DONE"],
                        "history_name": "history", "history_identity": {"name": "history"},
                        "rdb_path": os.path.join(root, "history.rdb"),
                        "started_epoch": 1.0, "finished_epoch": 2.0,
                    }

            class FakeWorker(object):
                worker_index = 0
                start_count = 1

                def __init__(self):
                    self.controller = Controller()
                    self.workspace = {"root": root, "setup": {"tests": []}}
                    self.derived_parameter_cache = {}
                    self.pending_artifact = None
                    self.retire_after_cleanup = None
                    self.retired = []

                def ensure_started(self):
                    return {"started": False, "epoch": "epoch-1"}

                def set_pending(self, artifact, retire_after_cleanup=False):
                    self.pending_artifact = artifact
                    self.retire_after_cleanup = retire_after_cleanup

                def retire(self, reason):
                    self.retired.append(reason)
                    self.workspace = None
                    return {"source_guard": {"ok": True}}

                def health(self):
                    return {"alive": True}

            worker = FakeWorker()
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {"candidate_timeout_seconds": 60, "infrastructure_retries": 0}}
            with mock.patch("crescendo_engine.runner.summarize_spectre_timing", return_value={"run_count": 0, "runs": []}), mock.patch(
                "crescendo_engine.runner.validate_candidate_records", return_value={},
            ), mock.patch(
                "crescendo_engine.runner.verify_candidate_netlist", return_value={"ok": True, "parameters": []},
            ), mock.patch(
                "crescendo_engine.runner.parse_rdb", return_value={"path": os.path.join(root, "history.rdb")},
            ), mock.patch(
                "crescendo_engine.runner.validate_candidate_completion", side_effect=RuntimeError("missing output"),
            ), mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ):
                row = _evaluate_persistent_candidate(
                    worker, root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertEqual(row["status"], "failed")
            self.assertIsNotNone(worker.pending_artifact)
            self.assertFalse(worker.retire_after_cleanup)
            self.assertEqual(worker.retired, [])

    def test_fatal_submit_signal_retries_same_logical_candidate_and_completes(self):
        with tempfile.TemporaryDirectory() as root:
            class Controller(object):
                def __init__(self, epoch):
                    self.epoch = epoch
                    self.spectre_log = os.path.join(root, "spectre.log")
                    self.pending_cleanup = None

                def evaluate(self, candidate_id, _values, requested_root, timeout_seconds=None):
                    evaluated_ids.append(candidate_id)
                    if self.epoch == "epoch-1":
                        return {
                            "candidate_id": candidate_id, "candidate_root": requested_root,
                            "states": ["ACCEPTED", "BEGIN", "PATCHED", "ERROR"],
                        }
                    return {
                        "candidate_id": candidate_id, "candidate_root": requested_root,
                        "states": ["ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY", "DONE"],
                        "history_name": "history", "history_identity": {"name": "history"},
                        "rdb_path": os.path.join(root, "history.rdb"),
                        "started_epoch": 1.0, "finished_epoch": 2.0,
                    }

                def is_alive(self):
                    return True

                def require_candidate_time(self, _artifact, _phase):
                    return None

            class FakeWorker(object):
                worker_index = 0
                pending_artifact = None

                def __init__(self):
                    self.controller = None
                    self.workspace = None
                    self.start_count = 0
                    self.derived_parameter_cache = {}
                    self.pending_artifact = None

                def ensure_started(self):
                    self.start_count += 1
                    self.controller = Controller("epoch-%d" % self.start_count)
                    self.workspace = {"root": root}
                    return {"started": True, "epoch": self.controller.epoch}

                def retire(self, _reason):
                    self.controller = None
                    self.workspace = None
                    return {
                        "termination_proven": True, "process_still_alive": False,
                        "cleanup": {"ok": True}, "source_guard": {"ok": True},
                    }

                def health(self):
                    return {"alive": False}

                def set_pending(self, artifact, retire_after_cleanup=False):
                    self.pending_artifact = artifact

            empty = {"run_count": 0, "completed_run_count": 0, "runs": []}
            killed = {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 139, "elapsed_seconds": 0.1}],
            }
            completed = {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 0, "elapsed_seconds": 0.2}],
            }
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {
                "candidate_timeout_seconds": 60, "infrastructure_retries": 1,
            }}
            worker = FakeWorker()
            worker.config = copy.deepcopy(config)
            evaluated_ids = []
            with mock.patch(
                "crescendo_engine.runner.summarize_spectre_timing",
                side_effect=[empty, killed, empty, completed],
            ), mock.patch(
                "crescendo_engine.runner.validate_candidate_records", return_value={},
            ), mock.patch(
                "crescendo_engine.runner.verify_candidate_netlist",
                return_value={"ok": True, "parameters": []},
            ), mock.patch(
                "crescendo_engine.runner.parse_rdb",
                return_value={"path": os.path.join(root, "history.rdb")},
            ), mock.patch(
                "crescendo_engine.runner.validate_candidate_completion",
                return_value={"ok": True},
            ) as completion, mock.patch(
                "crescendo_engine.runner.evaluate_metrics",
                return_value={
                    "objectives": [{"value": 1.0, "score": -1.0}],
                    "constraints": [], "feasible": True,
                    "total_violation": 0.0, "merit": -1.0,
                },
            ), mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ):
                row = _evaluate_persistent_candidate(
                    worker, root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertEqual(worker.start_count, 2)
            self.assertEqual(row["candidate_id"], "g000_c0000")
            self.assertEqual(row["status"], "completed")
            self.assertEqual(completion.call_count, 1)
            self.assertIs(completion.call_args[1]["config"], worker.config)
            self.assertEqual(evaluated_ids, ["g000_c0000", "g000_c0000"])
            self.assertEqual(len(row["persistent_worker"]["attempts"]), 2)
            self.assertEqual(
                row["persistent_worker"]["attempts"][0]["infrastructure_evidence"],
                [{"kind": "spectre_submit_returncode", "returncode": 139}],
            )
            self.assertIsNotNone(worker.pending_artifact)

    def test_fatal_submit_signal_after_done_retires_and_retries_same_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            class Controller(object):
                def __init__(self, epoch):
                    self.epoch = epoch
                    self.spectre_log = os.path.join(root, "spectre.log")
                    self.pending_cleanup = {"candidate_id": "g000_c0000"}

                def evaluate(self, candidate_id, _values, requested_root, timeout_seconds=None):
                    evaluated_ids.append(candidate_id)
                    return {
                        "candidate_id": candidate_id, "candidate_root": requested_root,
                        "states": ["ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY", "DONE"],
                        "history_name": "history", "history_identity": {"name": "history"},
                        "rdb_path": os.path.join(root, "history.rdb"),
                        "started_epoch": 1.0, "finished_epoch": 2.0,
                    }

                def is_alive(self):
                    return True

                def require_candidate_time(self, _artifact, _phase):
                    return None

            class FakeWorker(object):
                worker_index = 0

                def __init__(self):
                    self.controller = None
                    self.workspace = None
                    self.start_count = 0
                    self.derived_parameter_cache = {}
                    self.pending_artifact = None
                    self.retire_after_cleanup = False

                def ensure_started(self):
                    self.start_count += 1
                    self.controller = Controller("epoch-%d" % self.start_count)
                    self.workspace = {"root": root}
                    return {"started": True, "epoch": self.controller.epoch}

                def retire(self, _reason):
                    self.controller = None
                    self.workspace = None
                    return {
                        "termination_proven": True, "process_still_alive": False,
                        "cleanup": {"ok": True}, "source_guard": {"ok": True},
                    }

                def health(self):
                    return {"alive": self.controller is not None}

                def set_pending(self, artifact, retire_after_cleanup=False):
                    self.pending_artifact = artifact
                    self.retire_after_cleanup = retire_after_cleanup

                def commit_candidate(self, candidate_id):
                    self.assert_pending(candidate_id)
                    retirement = self.retire("result validation tainted the worker")
                    self.pending_artifact = None
                    self.retire_after_cleanup = False
                    return {
                        "ok": True, "state": "cleaned", "recycled": True,
                        "retirement": retirement,
                    }

                def assert_pending(self, candidate_id):
                    if not self.pending_artifact or self.pending_artifact["candidate_id"] != candidate_id:
                        raise AssertionError("candidate cleanup fence mismatch")

            empty = {"run_count": 0, "completed_run_count": 0, "runs": []}
            killed = {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 137, "elapsed_seconds": 0.1}],
            }
            completed = {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 0, "elapsed_seconds": 0.2}],
            }
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {
                "candidate_timeout_seconds": 60, "infrastructure_retries": 1,
            }}
            worker = FakeWorker()
            worker.config = copy.deepcopy(config)
            evaluated_ids = []
            with mock.patch(
                "crescendo_engine.runner.summarize_spectre_timing",
                side_effect=[empty, killed, empty, completed],
            ), mock.patch(
                "crescendo_engine.runner.validate_candidate_records", return_value={},
            ), mock.patch(
                "crescendo_engine.runner.verify_candidate_netlist",
                return_value={"ok": True, "parameters": []},
            ), mock.patch(
                "crescendo_engine.runner.parse_rdb",
                return_value={"path": os.path.join(root, "history.rdb")},
            ), mock.patch(
                "crescendo_engine.runner.validate_candidate_completion",
                side_effect=[RuntimeError("simulator failed to complete"), {"ok": True}],
            ) as completion, mock.patch(
                "crescendo_engine.runner.evaluate_metrics",
                return_value={
                    "objectives": [{"value": 1.0, "score": -1.0}],
                    "constraints": [], "feasible": True,
                    "total_violation": 0.0, "merit": -1.0,
                },
            ), mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ):
                row = _evaluate_persistent_candidate(
                    worker, root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertEqual(worker.start_count, 2)
            self.assertEqual(row["status"], "completed")
            self.assertEqual(completion.call_count, 2)
            self.assertIs(completion.call_args[1]["config"], worker.config)
            self.assertEqual(
                row["persistent_worker"]["attempts"][0]["postprocess_error"],
                "RuntimeError: simulator failed to complete",
            )
            self.assertEqual(evaluated_ids, ["g000_c0000", "g000_c0000"])
            self.assertEqual(
                row["persistent_worker"]["attempts"][0]["infrastructure_evidence"],
                [{"kind": "spectre_submit_returncode", "returncode": 137}],
            )
            self.assertTrue(
                row["persistent_worker"]["attempts"][0]["cleanup"]["recycled"]
            )

    def test_trusted_submit_returncodes_are_an_explicit_fatal_set(self):
        class Controller(object):
            def is_alive(self):
                return True

        worker = mock.Mock()
        worker.controller = Controller()
        trusted = (124, 126, 127, 132, 133, 134, 135, 136, 137, 139, 143)
        for returncode in trusted:
            with self.subTest(returncode=returncode):
                evidence = _trusted_candidate_infrastructure_evidence(
                    worker, {"runs": [{"returncode": returncode}]},
                )
                self.assertEqual(evidence, [{
                    "kind": "spectre_submit_returncode", "returncode": returncode,
                }])
        self.assertEqual(
            _trusted_candidate_infrastructure_evidence(
                worker, {"runs": [{"returncode": 138}]},
            ),
            [],
        )

    def test_protocol_error_with_ordinary_spectre_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as root:
            class Controller(object):
                epoch = "epoch-1"
                spectre_log = os.path.join(root, "spectre.log")
                pending_cleanup = None

                def evaluate(self, candidate_id, _values, requested_root, timeout_seconds=None):
                    return {
                        "candidate_id": candidate_id, "candidate_root": requested_root,
                        "states": ["ACCEPTED", "BEGIN", "PATCHED", "ERROR"],
                    }

                def is_alive(self):
                    return True

            class FakeWorker(object):
                worker_index = 0
                pending_artifact = None
                start_count = 1

                def __init__(self):
                    self.controller = Controller()
                    self.workspace = {"root": root}
                    self.derived_parameter_cache = {}
                    self.retire_count = 0

                def ensure_started(self):
                    return {"started": False, "epoch": self.controller.epoch}

                def retire(self, _reason):
                    self.retire_count += 1
                    self.controller = None
                    self.workspace = None
                    return {
                        "termination_proven": True, "process_still_alive": False,
                        "cleanup": {"ok": True}, "source_guard": {"ok": True},
                    }

                def health(self):
                    return {"alive": False}

            empty = {"run_count": 0, "completed_run_count": 0, "runs": []}
            design_error = {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 1, "elapsed_seconds": 0.1}],
            }
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            worker = FakeWorker()
            with mock.patch(
                "crescendo_engine.runner.summarize_spectre_timing",
                side_effect=[empty, design_error],
            ):
                row = _evaluate_persistent_candidate(
                    worker, root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [],
                    {"runtime": {"candidate_timeout_seconds": 60, "infrastructure_retries": 3}},
                )
            self.assertEqual(worker.retire_count, 1)
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["failure_class"], "design_or_simulation")

    def test_post_simulation_total_deadline_is_infrastructure_failure(self):
        with tempfile.TemporaryDirectory() as root:
            candidate_root = os.path.join(root, "candidates", "g000_c0000")
            os.makedirs(candidate_root)

            class Controller(object):
                epoch = "epoch-1"
                spectre_log = os.path.join(root, "spectre.log")
                pending_cleanup = {"candidate_id": "g000_c0000"}

                def evaluate(self, candidate_id, _values, requested_root, timeout_seconds=None):
                    return {
                        "candidate_id": candidate_id, "candidate_root": requested_root,
                        "states": ["ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY", "DONE"],
                        "history_name": "history", "history_identity": {"name": "history"},
                        "rdb_path": os.path.join(root, "history.rdb"),
                        "started_epoch": 1.0, "finished_epoch": 2.0,
                    }

                def require_candidate_time(self, _artifact, operation):
                    if operation == "Maestro result parsing":
                        raise PersistentMaestroWorkerTimeout("synthetic total deadline")
                    return 30.0

            class FakeWorker(object):
                worker_index = 0
                start_count = 1

                def __init__(self):
                    self.controller = Controller()
                    self.workspace = {"root": root, "setup": {"tests": []}}
                    self.derived_parameter_cache = {}
                    self.pending_artifact = None
                    self.retired = []

                def ensure_started(self):
                    return {"started": False, "epoch": "epoch-1"}

                def retire(self, reason):
                    self.retired.append(reason)
                    self.workspace = None
                    self.controller = None
                    return {"source_guard": {"ok": True}}

                def health(self):
                    return {"alive": False}

            worker = FakeWorker()
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {"candidate_timeout_seconds": 60, "infrastructure_retries": 0}}
            with mock.patch("crescendo_engine.runner.summarize_spectre_timing", return_value={"run_count": 0, "runs": []}), mock.patch(
                "crescendo_engine.runner.validate_candidate_records", return_value={},
            ), mock.patch(
                "crescendo_engine.runner.verify_candidate_netlist", return_value={"ok": True, "parameters": []},
            ):
                row = _evaluate_persistent_candidate(
                    worker, root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "infrastructure")
            self.assertNotIn("metrics", row)
            self.assertEqual(worker.retired, ["persistent infrastructure failure"])

    def test_missing_final_netlist_recycles_and_retries_same_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            candidate_root = os.path.join(root, "candidates", "g000_c0000")
            os.makedirs(candidate_root)

            class Controller(object):
                def __init__(self, epoch):
                    self.epoch = epoch
                    self.spectre_log = os.path.join(root, "spectre.log")
                    self.pending_cleanup = {"candidate_id": "g000_c0000"}

                def evaluate(self, candidate_id, _values, requested_root, timeout_seconds=None):
                    return {
                        "candidate_id": candidate_id, "candidate_root": requested_root,
                        "states": ["ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE", "RESULTS_READY", "DONE"],
                        "history_name": "history-%s" % self.epoch,
                        "history_identity": {"name": "history-%s" % self.epoch},
                        "rdb_path": os.path.join(root, "history-%s.rdb" % self.epoch),
                        "started_epoch": 1.0, "finished_epoch": 2.0,
                    }

                def require_candidate_time(self, _artifact, _operation):
                    return 30.0

            class FakeWorker(object):
                worker_index = 0

                def __init__(self):
                    self.controller = None
                    self.workspace = None
                    self.start_count = 0
                    self.derived_parameter_cache = {}
                    self.pending_artifact = None
                    self.retire_after_cleanup = False

                def ensure_started(self):
                    if self.controller is None:
                        self.start_count += 1
                        self.controller = Controller("epoch-%d" % self.start_count)
                        self.workspace = {"root": root, "setup": {"tests": []}}
                    return {"started": self.start_count > 1, "epoch": self.controller.epoch}

                def set_pending(self, artifact, retire_after_cleanup=False):
                    self.pending_artifact = artifact
                    self.retire_after_cleanup = retire_after_cleanup

                def commit_candidate(self, _candidate_id):
                    self.controller = None
                    self.workspace = None
                    self.pending_artifact = None
                    self.retire_after_cleanup = False
                    return {
                        "ok": True, "recycled": True,
                        "retirement": {
                            "termination_proven": True, "process_still_alive": False,
                            "source_guard": {"ok": True}, "cleanup": {"ok": True},
                        },
                    }

                def retire(self, _reason):
                    raise AssertionError("safe result-publication retry should use fenced cleanup")

                def health(self):
                    return {"alive": True}

            worker = FakeWorker()
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {"candidate_timeout_seconds": 60, "infrastructure_retries": 1}}
            worker.config = copy.deepcopy(config)
            metrics = {
                "objectives": [{"score": -10.0, "value": 10.0}], "constraints": [],
                "feasible": True, "total_violation": 0.0, "merit": -10.0,
            }
            with mock.patch("crescendo_engine.runner.summarize_spectre_timing", return_value={"run_count": 0, "runs": []}), mock.patch(
                "crescendo_engine.runner.validate_candidate_records", return_value={},
            ), mock.patch(
                "crescendo_engine.runner.verify_candidate_netlist", side_effect=[
                    ParameterApplicationInfrastructureFailure("missing final netlist"),
                    {"ok": True, "parameters": []},
                ],
            ) as verify, mock.patch(
                "crescendo_engine.runner.parse_rdb", return_value={"path": os.path.join(root, "history.rdb")},
            ), mock.patch(
                "crescendo_engine.runner.validate_candidate_completion", return_value={"ok": True},
            ) as completion, mock.patch(
                "crescendo_engine.runner.evaluate_metrics", return_value=metrics,
            ), mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ):
                row = _evaluate_persistent_candidate(
                    worker, root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertEqual(row["status"], "completed")
            self.assertEqual(completion.call_count, 1)
            self.assertIs(completion.call_args[1]["config"], worker.config)
            self.assertEqual(row["candidate_id"], "g000_c0000")
            self.assertEqual(worker.start_count, 2)
            self.assertEqual(verify.call_count, 2)
            self.assertTrue(row["persistent_worker"]["attempts"][0]["cleanup"]["recycled"])

    def test_persistent_retirement_source_guard_is_propagated_to_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            control_root = os.path.join(root, "control")
            os.makedirs(control_root)
            with open(os.path.join(control_root, "ocean.stderr"), "w") as stream:
                stream.write("discarded-prefix\n" + "x" * 5000 + "diagnostic-end")

            class Controller(object):
                epoch = "epoch-1"
                spectre_log = os.path.join(root, "spectre.log")
                pending_cleanup = None

                def evaluate(self, *_args, **_kwargs):
                    raise PersistentMaestroWorkerError("worker crashed")

            Controller.control_root = control_root

            class FakeWorker(object):
                worker_index = 0
                start_count = 1
                pending_artifact = None

                def __init__(self):
                    self.controller = Controller()
                    self.workspace = {"root": root}
                    self.derived_parameter_cache = {}

                def ensure_started(self):
                    return {"started": False, "epoch": "epoch-1"}

                def retire(self, _reason):
                    self.workspace = None
                    self.controller = None
                    return {"source_guard": {"ok": False, "error": "source changed"}}

                def health(self):
                    return {"alive": False}

            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {"candidate_timeout_seconds": 60, "infrastructure_retries": 0}}
            with mock.patch("crescendo_engine.runner.summarize_spectre_timing", return_value={"run_count": 0, "runs": []}):
                row = _evaluate_persistent_candidate(
                    FakeWorker(), root, 0, 0, {"x": 0.5}, spec,
                    {"setup": {"tests": []}}, [], config,
                )
            self.assertEqual(row["failure_class"], "source_integrity")
            self.assertFalse(row["source_guard"]["ok"])
            diagnostic = row["persistent_worker"]["attempts"][0]["diagnostic_tail"]
            self.assertLessEqual(sum(len(value) for value in diagnostic.values()), 4096)
            self.assertTrue(diagnostic["ocean.stderr"].endswith("diagnostic-end"))

    def test_live_supervisor_blocks_epoch_deletion_and_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            workspace_root = os.path.join(epoch_root, "workspace")
            os.makedirs(workspace_root)

            class Process(object):
                pid = 12345

                def poll(self):
                    return None

            class Controller(object):
                tainted = True
                process = Process()
                closed = False
                state = "termination_failed"

                def is_alive(self):
                    return False

                def terminate(self, reason=None):
                    raise PersistentMaestroWorkerError("supervisor survived TERM/KILL")

            worker = _WholeRunPersistentWorker.__new__(_WholeRunPersistentWorker)
            worker.run_root = root
            worker.worker_index = 0
            worker.prepared = {"setup": {}, "hierarchy": {}}
            worker.selected = []
            worker.config = {"runtime": {}}
            worker.worker_root = os.path.join(root, "workers", "worker_000")
            worker.epoch_index = 1
            worker.start_count = 1
            worker.workspace = {"root": workspace_root}
            worker.epoch_root = epoch_root
            worker.controller = Controller()
            worker.pending_artifact = None
            worker.retire_after_cleanup = False
            worker.derived_parameter_cache = {}
            worker.last_cleanup = None

            with mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ), mock.patch("crescendo_engine.runner.safe_delete_worker_epoch") as delete_epoch:
                retirement = worker.retire("test")
                self.assertTrue(retirement["process_still_alive"])
                delete_epoch.assert_not_called()
                self.assertTrue(os.path.isdir(epoch_root))
                self.assertIsNotNone(worker.controller)
                with mock.patch.object(worker, "_new_epoch") as new_epoch:
                    with self.assertRaisesRegex(
                        PersistentMaestroWorkerError, "previous worker supervisor is alive",
                    ):
                        worker.ensure_started()
                    new_epoch.assert_not_called()

    def test_fallback_termination_proof_allows_cleanup_after_graceful_close_error(self):
        with tempfile.TemporaryDirectory() as root:
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            workspace_root = os.path.join(epoch_root, "workspace")
            os.makedirs(workspace_root)

            class Process(object):
                def __init__(self):
                    self.returncode = None

                def poll(self):
                    return self.returncode

            class Controller(object):
                tainted = False
                closed = False
                state = "ready"

                def __init__(self):
                    self.process = Process()

                def is_alive(self):
                    return self.process.poll() is None

                def close(self):
                    raise PersistentMaestroWorkerError("graceful close failed")

                def terminate(self, reason=None):
                    self.process.returncode = 0
                    self.closed = True
                    self.state = "terminated"
                    return 0

            worker = _WholeRunPersistentWorker.__new__(_WholeRunPersistentWorker)
            worker.run_root = root
            worker.worker_index = 0
            worker.prepared = {"setup": {}, "hierarchy": {}}
            worker.selected = []
            worker.config = {"runtime": {}}
            worker.worker_root = os.path.join(root, "workers", "worker_000")
            worker.epoch_index = 1
            worker.start_count = 1
            worker.workspace = {"root": workspace_root}
            worker.epoch_root = epoch_root
            worker.controller = Controller()
            worker.pending_artifact = None
            worker.retire_after_cleanup = False
            worker.derived_parameter_cache = {}
            worker.last_cleanup = None

            with mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ), mock.patch(
                "crescendo_engine.runner.safe_delete_worker_epoch",
                return_value={"ok": True, "removed": True},
            ) as delete_epoch:
                retirement = worker.retire("test fallback")
            self.assertTrue(retirement["termination_proven"])
            self.assertIn("graceful close failed", retirement["close_error"])
            delete_epoch.assert_called_once()
            self.assertIsNone(worker.controller)

    def test_cleanup_error_retires_before_raw_history_deletion(self):
        with tempfile.TemporaryDirectory() as root:
            worker = _WholeRunPersistentWorker.__new__(_WholeRunPersistentWorker)
            worker.pending_artifact = {"candidate_id": "g000_c0000"}
            worker.retire_after_cleanup = False
            worker.last_cleanup = None

            class Controller(object):
                def request_cleanup(self, _artifact):
                    return {"ok": False, "states": ["CLEANUP_BEGIN", "CLEANUP_ERROR"]}

            worker.controller = Controller()
            retirement = {
                "termination_proven": True,
                "process_still_alive": False,
                "source_guard": {"ok": True},
                "cleanup": {"ok": True, "removed": True},
            }
            with mock.patch.object(worker, "retire", return_value=retirement) as retire:
                result = worker.commit_candidate("g000_c0000")
            self.assertFalse(result["ok"])
            self.assertEqual(result["state"], "cleanup_failed_recycled")
            self.assertIs(result["retirement"], retirement)
            retire.assert_called_once_with("Maestro result cleanup reported failure")

    def test_current_history_is_deferred_until_a_later_candidate_or_session_close(self):
        with tempfile.TemporaryDirectory() as root:
            worker = _WholeRunPersistentWorker.__new__(_WholeRunPersistentWorker)
            worker.workspace = {
                "root": root,
                "simulation_root": os.path.join(root, "simulation"),
                "setup": {"tests": []},
            }
            artifact = {
                "candidate_id": "g000_c0000",
                "history_name": "Interactive.0",
                "rdb_path": os.path.join(root, "Interactive.0.rdb"),
            }
            worker.pending_artifact = artifact
            worker.deferred_artifacts = []
            worker.retire_after_cleanup = False
            worker.last_cleanup = None

            class Controller(object):
                def request_cleanup(self, _artifact):
                    return {"ok": True, "states": ["CLEANUP_BEGIN", "CLEANUP_DONE"]}

            worker.controller = Controller()
            with mock.patch.object(worker, "health", return_value={"alive": True}):
                result = worker.commit_candidate("g000_c0000")
            self.assertEqual(worker.deferred_artifacts, [artifact])
            self.assertEqual(result["raw_paths_removed"], [])
            self.assertEqual(result["raw_paths_deferred"], [artifact["rdb_path"]])

    def test_fixed_overwrite_history_path_is_never_deleted_as_a_previous_artifact(self):
        with tempfile.TemporaryDirectory() as root:
            worker = _WholeRunPersistentWorker.__new__(_WholeRunPersistentWorker)
            worker.workspace = {
                "root": root,
                "simulation_root": os.path.join(root, "simulation"),
                "setup": {"tests": []},
            }
            history_path = os.path.join(root, "CRESCENDO_active.rdb")
            previous = {
                "candidate_id": "g000_c0000", "history_name": "CRESCENDO_active",
                "rdb_path": history_path,
            }
            current = {
                "candidate_id": "g000_c0001", "history_name": "CRESCENDO_active",
                "rdb_path": history_path,
            }
            worker.pending_artifact = current
            worker.deferred_artifacts = [previous]
            worker.retire_after_cleanup = False
            worker.last_cleanup = None

            class Controller(object):
                def request_cleanup(self, _artifact):
                    return {"ok": True, "states": ["CLEANUP_BEGIN", "CLEANUP_DONE"]}

            worker.controller = Controller()
            with mock.patch.object(worker, "health", return_value={"alive": True}):
                result = worker.commit_candidate("g000_c0001")
            self.assertEqual(worker.deferred_artifacts, [current])
            self.assertEqual(result["raw_paths_deferred"], [history_path])

    def test_unproven_termination_retains_epoch_and_blocks_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            epoch_root = os.path.join(root, "workers", "worker_000", "epoch_000")
            workspace_root = os.path.join(epoch_root, "workspace")
            os.makedirs(workspace_root)

            class Process(object):
                pid = 12345

                def poll(self):
                    return 2

            class Controller(object):
                tainted = True
                process = Process()
                closed = False
                state = "termination_failed"

                def is_alive(self):
                    return False

                def terminate(self, reason=None):
                    raise PersistentMaestroWorkerError("guardian cleanup proof failed")

            worker = _WholeRunPersistentWorker.__new__(_WholeRunPersistentWorker)
            worker.run_root = root
            worker.worker_index = 0
            worker.prepared = {"setup": {}, "hierarchy": {}}
            worker.selected = []
            worker.config = {"runtime": {}}
            worker.worker_root = os.path.join(root, "workers", "worker_000")
            worker.epoch_index = 1
            worker.start_count = 1
            worker.workspace = {"root": workspace_root}
            worker.epoch_root = epoch_root
            worker.controller = Controller()
            worker.pending_artifact = None
            worker.retire_after_cleanup = False
            worker.derived_parameter_cache = {}
            worker.last_cleanup = None

            with mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ), mock.patch("crescendo_engine.runner.safe_delete_worker_epoch") as delete_epoch:
                retirement = worker.retire("test")
                self.assertFalse(retirement["termination_proven"])
                self.assertFalse(retirement["cleanup"]["ok"])
                delete_epoch.assert_not_called()
                self.assertIsNotNone(worker.controller)
                self.assertTrue(os.path.isdir(epoch_root))
                with mock.patch.object(worker, "_new_epoch") as new_epoch:
                    with self.assertRaisesRegex(
                        PersistentMaestroWorkerError, "previous worker was not safely retired",
                    ):
                        worker.ensure_started()
                    new_epoch.assert_not_called()

    def test_startup_failure_epoch_is_owned_and_safely_retired(self):
        with tempfile.TemporaryDirectory() as root:
            worker = _WholeRunPersistentWorker(
                root, 0, {"setup": {}, "hierarchy": {}}, [], {"runtime": {}},
            )
            workspace_root = os.path.join(root, "workers", "worker_000", "epoch_000", "workspace")
            os.makedirs(workspace_root)
            workspace = {"root": workspace_root, "build_timing": {}}

            class FailedController(object):
                def __init__(self, _workspace, _config, _selected, worker_root,
                             worker_index=0, epoch=None):
                    self.worker_root = worker_root
                    self.worker_index = worker_index
                    self.epoch = epoch
                    self.control_root = os.path.join(worker_root, "control")
                    self.process = None
                    self.tainted = True
                    self.closed = False
                    self.state = "new"

                def start(self):
                    self.state = "failed"
                    raise PersistentMaestroWorkerError("startup failed")

                def is_alive(self):
                    return False

                def terminate(self, reason=None):
                    self.closed = True
                    self.state = "terminated"
                    return None

            with mock.patch("crescendo_engine.runner.build_workspace", return_value=workspace), mock.patch(
                "crescendo_engine.runner.PersistentMaestroWorker", FailedController,
            ):
                with self.assertRaisesRegex(PersistentMaestroWorkerError, "startup failed"):
                    worker._new_epoch()
            self.assertEqual(worker.workspace, workspace)
            self.assertEqual(worker.epoch_root, os.path.dirname(workspace_root))
            self.assertIsInstance(worker.controller, FailedController)
            self.assertEqual(worker.start_count, 0)
            with mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
            ), mock.patch(
                "crescendo_engine.runner.safe_delete_worker_epoch",
                return_value={"ok": True, "removed": True},
            ) as delete_epoch:
                retirement = worker.retire("startup failure")
            self.assertTrue(retirement["cleanup"]["ok"])
            delete_epoch.assert_called_once()
            self.assertIsNone(worker.controller)

    def test_new_controller_never_reuses_an_existing_worker_epoch_directory(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "workers", "worker_000", "epoch_003"))
            worker = _WholeRunPersistentWorker(
                root, 0, {"setup": {}, "hierarchy": {}}, [], {"runtime": {}},
            )
            self.assertEqual(worker.epoch_index, 4)

    def test_fault_replacement_rebuilds_pristine_workspace_after_candidate_patch(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source", "D")
            os.makedirs(os.path.join(source, "TOP", "maestro"))
            source_design = os.path.join(source, "TOP", "design.txt")
            source_setup = os.path.join(source, "TOP", "maestro", "data.sdb")
            with open(source_design, "w") as stream:
                stream.write("original-baseline")
            with open(source_setup, "w") as stream:
                stream.write("setup")
            run_root = os.path.join(root, "run")
            prepared = {
                "setup": {"source": {
                    "library_root": source,
                    "maestro_sdb": source_setup,
                }},
                "hierarchy": {},
            }
            config = {"runtime": {"run_root": run_root},
                      "cadence": {"workspace_copy_strategy": "source_reference"}}
            controllers = []

            class Controller(object):
                def __init__(self, workspace, _config, _selected, _worker_root,
                             worker_index=0, epoch=None):
                    self.workspace = workspace
                    self.worker_index = worker_index
                    self.epoch = epoch
                    self.tainted = False
                    self.closed = False
                    self.state = "new"
                    self.process = None
                    controllers.append(self)

                def start(self):
                    copied = self.workspace["copied_libraries"]["D"]
                    with open(os.path.join(copied, "TOP", "design.txt")) as stream:
                        self.starting_baseline = stream.read()
                    self.state = "ready"

                def is_alive(self):
                    return not self.closed

                def terminate(self, reason=None):
                    self.closed = True
                    self.state = "terminated"

                def close(self):
                    self.closed = True
                    self.state = "closed"

            worker = _WholeRunPersistentWorker(run_root, 0, prepared, [], config)
            with mock.patch("crescendo_engine.runner.PersistentMaestroWorker", Controller):
                worker._new_epoch()
                first_workspace = worker.workspace["root"]
                patched_design = os.path.join(
                    worker.workspace["copied_libraries"]["D"], "TOP", "design.txt",
                )
                with open(patched_design, "w") as stream:
                    stream.write("candidate-patched")
                worker.controller.tainted = True
                worker.controller.state = "tainted"

                replacement = worker.ensure_started()
                second_workspace = worker.workspace["root"]

                self.assertTrue(replacement["started"])
                self.assertEqual(worker.start_count, 2)
                self.assertEqual(
                    [controller.starting_baseline for controller in controllers],
                    ["original-baseline", "original-baseline"],
                )
                self.assertNotEqual(first_workspace, second_workspace)
                self.assertFalse(os.path.exists(first_workspace))
                with open(source_design) as stream:
                    self.assertEqual(stream.read(), "original-baseline")
                worker.close()

    def test_source_integrity_failure_aborts_before_pending_or_replacements(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}],
            }
            config = {"runtime": {
                "run_root": root, "execution_backend": "auto", "persistent_max_workers": 1,
                "persistent_worker_lifetime": "batch", "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 4,
            }}
            spec = {
                "algorithm": "random", "generations": 1, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }

            evaluated = []

            def evaluate(_controller, _root, index, generation, values):
                evaluated.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "infrastructure_failed",
                    "failure_class": "source_integrity", "error": "source fingerprint changed",
                }

            run_root = os.path.join(root, "run")
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_run_controller(evaluate),
            ):
                with self.assertRaisesRegex(RuntimeError, "source integrity failure"):
                    run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(evaluated, [0])
            self.assertEqual(load_json(os.path.join(run_root, "status.json"))["state"], "failed")

    def test_parameter_patch_timeout_is_infrastructure_failure(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []}}
            config = {"runtime": {"candidate_timeout_seconds": 60}}
            spec = {"candidate_timeout_seconds": 60, "objectives": [{"output_key": "main::gain", "direction": "maximize"}], "constraints": []}
            with mock.patch("crescendo_engine.runner.build_workspace", return_value={"root": root}), mock.patch(
                "crescendo_engine.runner.apply_candidate",
                side_effect=ParameterApplicationInfrastructureFailure("startup lock timeout"),
            ), mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}):
                row = _evaluate_candidate(root, 0, 0, {"x": 0.5}, spec, prepared, [], config)
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "infrastructure")
            self.assertNotIn("metrics", row)

    def test_unproven_legacy_guardian_cleanup_is_infrastructure_and_retained(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []}}
            config = {"runtime": {"candidate_timeout_seconds": 60, "infrastructure_retries": 0}}
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            command = {
                "returncode": 2, "timed_out": False, "startup_timed_out": False,
                "termination_failed": True, "lock_wait_seconds": 0.0,
                "elapsed_seconds": 0.1, "spectre_timing": {},
                "guardian_cleanup": {
                    "state": "ambiguous", "reason": "terminal proof missing",
                    "termination_proof": {"ok": False},
                },
            }
            with mock.patch("crescendo_engine.runner.build_workspace", return_value={"root": root}), \
                    mock.patch("crescendo_engine.runner.apply_candidate", return_value={}), \
                    mock.patch("crescendo_engine.runner.run_maestro", return_value=command), \
                    mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}):
                row = _evaluate_candidate(root, 0, 0, {"x": 0.5}, spec, prepared, [], config)
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "infrastructure")
            self.assertTrue(row["cleanup_pending"])
            self.assertEqual(row["process_cleanup"]["state"], "ambiguous")
            self.assertNotIn("metrics", row)

    def test_source_integrity_failure_is_not_used_as_optimizer_penalty(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []}}
            config = {"runtime": {"candidate_timeout_seconds": 60}}
            spec = {"candidate_timeout_seconds": 60, "objectives": [{"output_key": "main::gain", "direction": "maximize"}], "constraints": []}
            with mock.patch("crescendo_engine.runner.build_workspace", return_value={"root": root}), mock.patch(
                "crescendo_engine.runner.apply_candidate",
                side_effect=ParameterApplicationInfrastructureFailure("startup lock timeout"),
            ), mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": False}):
                row = _evaluate_candidate(root, 0, 0, {"x": 0.5}, spec, prepared, [], config)
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "source_integrity")
            self.assertNotIn("metrics", row)

    def test_design_config_reports_stable_identity_mismatches(self):
        prepared = {
            "parameters": [{"parameter_id": "HH:TB:schematic:/XDUT/M1:w", "optimizable": True}],
            "outputs": [{"key": "main::gain"}],
            "setup": {"corners": [{"name": "tt"}]},
        }
        document = {
            "schema_version": "crescendo.design_config.v1",
            "optimization": {
                "variables": [{"parameter_id": "HH:TB:schematic:/XDUT/M2:w", "minimum": 1e-6, "maximum": 2e-6}],
                "objectives": [{"output_key": "main::missing", "corners": ["tt", "ff"]}],
                "constraints": [],
                "run": {"generations": 30, "jobs": 2, "candidate_timeout_seconds": 900,
                        "algorithm": "differential_evolution",
                        "algorithm_parameters": {"population": 24, "seed": 8,
                                                 "differential_weight": 0.7, "crossover_rate": 0.85}},
            },
        }
        resolution = resolve_design_config(document, prepared)
        self.assertFalse(resolution["compatible"])
        self.assertEqual(resolution["missing"]["parameters"], ["HH:TB:schematic:/XDUT/M2:w"])
        self.assertEqual(resolution["missing"]["outputs"], ["main::missing"])
        self.assertEqual(resolution["missing"]["corners"], ["ff"])
        self.assertEqual(resolution["spec"]["generations"], 30)
        self.assertEqual(resolution["spec"]["algorithm"], "differential_evolution")
        self.assertEqual(resolution["spec"]["algorithm_parameters"]["population"], 24)

    def test_metric_binding_validation_rejects_malformed_definitions(self):
        prepared = {
            "outputs": [{"key": "main::gain"}],
            "setup": {"corners": [{"name": "tt"}]},
        }
        with self.assertRaisesRegex(ValueError, "output_key.*non-empty string"):
            resolve_metric_bindings(prepared, [{"corners": ["tt"]}], [])
        with self.assertRaisesRegex(ValueError, "corners must be an array"):
            resolve_metric_bindings(
                prepared,
                [{"output_key": "main::gain", "corners": "tt"}],
                [],
            )

    def test_checkpoint_resume_skips_completed_and_reuses_pending_values(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "maestro_path": "/portable/design/maestro",
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}], "corners": []},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            spec = {
                "evaluator": "mock", "generations": 2, "population": 4, "jobs": 1, "seed": 7,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 2.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize"}],
                "constraints": [],
            }

            def completed(_root, index, generation, values, _spec, _prepared, _selected, _config):
                value = values["x"]
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                    "index": index, "values": values, "status": "completed",
                    "metrics": {"objectives": [{"score": value, "value": value}], "constraints": [],
                                "feasible": True, "total_violation": 0.0, "merit": value},
                }

            baseline_root = os.path.join(root, "baseline")
            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=completed):
                baseline = run_optimization(spec, config, prepared=prepared, run_root=baseline_root)

            crashed_root = os.path.join(root, "crashed")

            def crash_in_second_generation(*args):
                if args[2] == 1:
                    raise RuntimeError("simulated power loss")
                return completed(*args)

            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=crash_in_second_generation):
                with self.assertRaisesRegex(RuntimeError, "simulated power loss"):
                    run_optimization(spec, config, prepared=prepared, run_root=crashed_root)
            checkpoint = load_json(os.path.join(crashed_root, "checkpoint.json"))
            self.assertEqual(len(checkpoint_candidates(checkpoint)), 4)
            self.assertGreaterEqual(
                checkpoint["state"]["checkpoint_revision"],
                len(checkpoint_candidates(checkpoint)) + 2,
            )
            self.assertEqual(checkpoint["state"]["next_generation"], 1)
            self.assertEqual(checkpoint["state"]["active_plan"]["generation"], 1)

            resumed_calls = []

            def resumed(*args):
                resumed_calls.append((args[2], args[1], args[3]))
                return completed(*args)

            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=resumed):
                resumed_result = _coordinated_runner_resume(
                    spec, config, prepared, os.path.join(root, "resumed"), checkpoint,
                )
            self.assertEqual(len(resumed_calls), 4)
            self.assertTrue(all(row[0] == 1 for row in resumed_calls))
            baseline_values = [row["values"] for row in baseline["candidates"] if row["generation"] == 1]
            resumed_values = [row["values"] for row in resumed_result["candidates"] if row["generation"] == 1]
            self.assertEqual(resumed_values, baseline_values)
            self.assertEqual(len(resumed_result["candidates"]), 8)
            final_checkpoint = load_json(os.path.join(root, "resumed", "checkpoint.json"))
            self.assertEqual(len(checkpoint_candidates(final_checkpoint)), 8)
            self.assertEqual(
                final_checkpoint["manifest"]["continuation_id"],
                checkpoint["manifest"]["continuation_id"],
            )
            self.assertGreater(final_checkpoint["state"]["checkpoint_revision"], checkpoint["state"]["checkpoint_revision"])
            self.assertLess(os.path.getsize(os.path.join(root, "resumed", "checkpoint.json")), 100 * 1024)

            tampered = dict(final_checkpoint)
            tampered["created_at"] = "tampered"
            with self.assertRaisesRegex(ValueError, "checksum"):
                validate_checkpoint(tampered, prepared)

    def test_checkpoint_candidates_rejects_malformed_packed_payloads_cleanly(self):
        malformed = []
        too_short = _minimal_checkpoint()
        too_short["candidates"] = [[]]
        malformed.append(too_short)
        wrong_values = _minimal_checkpoint()
        wrong_values["candidates"][0][4] = None
        malformed.append(wrong_values)
        wrong_generation = _minimal_checkpoint()
        wrong_generation["candidates"][0][1] = {}
        malformed.append(wrong_generation)
        wrong_metrics = _minimal_checkpoint()
        wrong_metrics["candidates"][0][5] = [True]
        malformed.append(wrong_metrics)
        wrong_container = _minimal_checkpoint()
        wrong_container["candidates"] = {"not": "a list"}
        malformed.append(wrong_container)
        for checkpoint in malformed:
            with self.subTest(payload=checkpoint["candidates"]):
                with self.assertRaises(ValueError) as raised:
                    checkpoint_candidates(checkpoint)
                self.assertNotIsInstance(raised.exception, (IndexError, TypeError))

    def test_checkpoint_integrity_is_independent_from_prepared_design(self):
        checkpoint = _minimal_checkpoint()
        rows = validate_checkpoint_integrity(checkpoint)
        self.assertEqual([row["candidate_id"] for row in rows], ["g000_c0000"])

        legacy_without_revision = copy.deepcopy(checkpoint)
        legacy_without_revision["state"].pop("checkpoint_revision", None)
        validate_checkpoint_integrity(legacy_without_revision)

        for invalid_revision in (True, "7", -1, 1.5):
            malformed_revision = copy.deepcopy(checkpoint)
            malformed_revision.pop("payload_sha256", None)
            malformed_revision["state"]["checkpoint_revision"] = invalid_revision
            with self.subTest(checkpoint_revision=invalid_revision):
                with self.assertRaisesRegex(ValueError, "revision"):
                    validate_checkpoint_integrity(malformed_revision)

        bad_identity = copy.deepcopy(checkpoint)
        bad_identity["candidates"][0][0] = "g000_c0001"
        with self.assertRaisesRegex(ValueError, "identity is inconsistent"):
            validate_checkpoint_integrity(bad_identity)

        bad_reference = copy.deepcopy(checkpoint)
        bad_reference["state"]["generator"]["previous"].append("g000_c9999")
        with self.assertRaisesRegex(ValueError, "references missing"):
            validate_checkpoint_integrity(bad_reference)

        bad_rng = copy.deepcopy(checkpoint)
        bad_rng["state"]["generator"]["rng_state"] = ["not-a-random-state"]
        with self.assertRaisesRegex(ValueError, "rng_state"):
            validate_checkpoint_integrity(bad_rng)

        bad_terminal = copy.deepcopy(checkpoint)
        bad_terminal["state"]["terminal_state"] = "completed"
        with self.assertRaisesRegex(ValueError, "terminal_reason"):
            validate_checkpoint_integrity(bad_terminal)

        signed = checkpoint_with_candidates(checkpoint, rows)
        validate_checkpoint_integrity(signed)
        tampered = copy.deepcopy(signed)
        tampered["created_at"] = "tampered"
        with self.assertRaisesRegex(ValueError, "checksum"):
            validate_checkpoint_integrity(tampered)

    def test_checkpoint_with_candidates_updates_counters_revision_sorting_and_checksum(self):
        checkpoint = _minimal_checkpoint()
        checkpoint["candidates"] = []
        checkpoint["state"] = {
            "next_generation": 2,
            "active_plan": None,
            "generator": {"previous": [], "de_targets": []},
            "checkpoint_revision": 7,
            "completed_candidates": 99,
            "failed_candidates": 99,
            "infrastructure_failed_candidates": 99,
        }
        rows = [
            {"candidate_id": "g001_c0001", "generation": 1, "index": 1,
             "status": "failed", "values": {"x": 0.4}},
            {"candidate_id": "g000_c0002", "generation": 0, "index": 2,
             "status": "infrastructure_failed", "values": {"x": 0.3}},
            {"candidate_id": "g000_c0000", "generation": 0, "index": 0,
             "status": "completed", "values": {"x": 0.2}},
            {"candidate_id": "g000_c0001", "generation": 0, "index": 1,
             "status": "running", "values": {"x": 0.1}},
        ]
        exported = checkpoint_with_candidates(checkpoint, rows)
        self.assertEqual([
            row["candidate_id"] for row in checkpoint_candidates(exported)
        ], ["g000_c0000", "g000_c0002", "g001_c0001"])
        self.assertEqual(exported["state"]["completed_candidates"], 1)
        self.assertEqual(exported["state"]["failed_candidates"], 1)
        self.assertEqual(exported["state"]["infrastructure_failed_candidates"], 1)
        self.assertEqual(exported["state"]["checkpoint_revision"], 8)
        self.assertEqual(checkpoint["state"]["checkpoint_revision"], 7)
        self.assertNotIn("payload_sha256", checkpoint)
        self.assertTrue(exported["payload_sha256"])
        self.assertEqual(
            exported["candidates"],
            checkpoint_with_candidates(checkpoint, list(reversed(rows)))["candidates"],
        )
        validate_checkpoint_integrity(exported)

    def test_checkpoint_fingerprint_v2_is_path_independent_and_semantically_complete(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "schema_version": "crescendo.prepared.v1",
                "maestro_path": "/machine-a/HH/TB/maestro",
                "setup": {
                    "schema_version": "crescendo.maestro_setup.v1",
                    "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}],
                    "source": {
                        "maestro_sdb": "/machine-a/HH/TB/maestro/maestro.sdb",
                        "active_state": "/machine-a/HH/TB/maestro/active.state",
                        "sha256": "setup-content-a",
                    },
                    "mode": "explorer",
                    "tests": [{
                        "name": "main", "enabled": True, "library": "HH", "cell": "TB",
                        "view": "schematic", "simulator": "spectre", "state": "ADE",
                        "variables": [{"name": "VDD", "value": "1.2"}],
                        "analyses": [{"name": "ac", "enabled": True,
                                      "fields": {"start": "1", "stop": "1G"}}],
                        "outputs": [{"name": "gain", "expression": "db20(VF(\"/out\"))",
                                     "eval_type": "point", "unit": "dB"}],
                    }],
                    "corners": [{"name": "tt", "enabled": True,
                                 "variables": [{"name": "TEMP", "value": "27"}]}],
                },
                "hierarchy": {"designs": [{
                    "design": {"library": "HH", "cell": "TB", "view": "schematic"},
                    "hierarchy": {
                        "cells": [{"hierarchy_path": "/XDUT", "library": "HH", "cell": "OPA",
                                   "view": "schematic", "depth": 1, "children": ["/XDUT/M1"]}],
                        "instances": [{"hierarchy_path": "/XDUT/M1", "name": "M1",
                                       "master_library": "PDK", "master_cell": "nmos",
                                       "master_view": "symbol", "master_view_type": "schematicSymbol",
                                       "has_schematic": False, "parent_path": "/XDUT", "depth": 2,
                                       "kind": "device"}],
                    },
                }]},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic", "optimizable": True,
                    "hierarchy_path": "/XDUT/M1", "owner_occurrence_path": "/XDUT",
                    "name": "w", "prompt": "Width", "source": "cdf", "semantic": "width",
                    "value_type": "float", "cdf_parameter_type": "string",
                    "current_value": "1u", "default_value": "1u", "numeric_value": 1e-6,
                    "unit": "m", "editable": True, "tunable": True,
                    "owner_library": "PDK", "owner_cell": "nmos",
                    "master_library": "PDK", "master_cell": "nmos",
                    "design_library": "HH", "design_cell": "TB", "design_view": "schematic",
                }],
                "outputs": [{"key": "main::gain", "test": "main", "name": "gain",
                             "expression": "db20(VF(\"/out\"))"}],
                "source_identity": {
                    "schema_version": "crescendo.source_design_identity.v1",
                    "libraries": [{"library": "HH", "paths": ["TB"], "file_count": 3,
                                   "directory_count": 2, "fingerprint": "library-content-a"}],
                    "fingerprint": "source-content-a",
                },
                "eda_context": {
                    "cadence": {"version_checks": [
                        {"name": "virtuoso_bin", "ok": True, "version": "IC6.1.8", "returncode": 0},
                        {"name": "spectre_bin", "ok": True, "version": "18.1", "returncode": 0},
                    ]},
                    "pdk": {"ok": True, "pdk_name": "PDK", "pdk_adapter": "generic-cdf",
                            "candidate_libraries": ["PDK"]},
                },
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            spec = {
                "evaluator": "mock", "algorithm": "random", "generations": 1,
                "population": 1, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.5e-6, "maximum": 2e-6}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            checkpoint = load_json(os.path.join(root, "run", "checkpoint.json"))
            self.assertEqual(
                checkpoint["manifest"]["prepared_schema_version"],
                "crescendo.prepared_fingerprint.v2",
            )
            moved = copy.deepcopy(prepared)
            moved["maestro_path"] = "/machine-b/copied/TB/maestro"
            moved["setup"]["source"]["maestro_sdb"] = "/machine-b/copied/TB/maestro/maestro.sdb"
            moved["setup"]["source"]["active_state"] = "/machine-b/copied/TB/maestro/active.state"
            validate_checkpoint(checkpoint, moved)

            source_changed = copy.deepcopy(moved)
            source_changed["source_identity"]["fingerprint"] = "source-content-b"
            with self.assertRaisesRegex(ValueError, "source design content has changed"):
                validate_checkpoint(checkpoint, source_changed)

            context_changed = copy.deepcopy(moved)
            context_changed["eda_context"]["pdk"]["pdk_name"] = "OTHER_PDK"
            with self.assertRaisesRegex(ValueError, "PDK"):
                validate_checkpoint(checkpoint, context_changed)

            legacy_server_checkpoint = copy.deepcopy(checkpoint)
            legacy_identity = legacy_server_checkpoint["manifest"]["eda_context_identity"]
            legacy_identity["submission"] = {
                "controller": None, "simulator": None,
                "submit_script": {"kind": "missing"},
            }
            legacy_payload = dict(legacy_identity)
            legacy_payload.pop("fingerprint", None)
            legacy_identity["fingerprint"] = hashlib.sha256(
                json.dumps(
                    legacy_payload, separators=(",", ":"), sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            legacy_server_checkpoint["payload_sha256"] = _checkpoint_checksum(
                legacy_server_checkpoint
            )
            validate_checkpoint(legacy_server_checkpoint, moved)

            mutations = [
                lambda row: row["setup"]["source"].update({"sha256": "setup-content-b"}),
                lambda row: row["setup"]["tests"][0]["variables"][0].update({"value": "1.1"}),
                lambda row: row["setup"]["tests"][0]["analyses"][0]["fields"].update({"stop": "2G"}),
                lambda row: row["setup"]["tests"][0]["outputs"][0].update({"expression": "mag(VF(\"/out\"))"}),
                lambda row: row["setup"]["corners"][0]["variables"][0].update({"value": "125"}),
                lambda row: row["parameters"][0].update({"current_value": "1.2u"}),
                lambda row: row["hierarchy"]["designs"][0]["hierarchy"]["instances"][0].update(
                    {"master_cell": "pmos"}
                ),
            ]
            for mutate in mutations:
                changed = copy.deepcopy(moved)
                mutate(changed)
                with self.assertRaisesRegex(ValueError, "schema has changed"):
                    validate_checkpoint(checkpoint, changed)

            legacy = copy.deepcopy(checkpoint)
            legacy["manifest"].pop("prepared_schema_version")
            legacy["manifest"]["prepared_schema_sha256"] = _prepared_schema_fingerprint_v1(prepared)
            legacy.pop("payload_sha256", None)
            validate_checkpoint(legacy, prepared)

            malformed_source = copy.deepcopy(checkpoint)
            malformed_source.pop("payload_sha256", None)
            malformed_source["manifest"]["source_design_identity"] = "not-an-object"
            with self.assertRaisesRegex(ValueError, "source design identity is malformed"):
                validate_checkpoint(malformed_source, prepared)

            malformed_context = copy.deepcopy(checkpoint)
            malformed_context.pop("payload_sha256", None)
            malformed_context["manifest"]["eda_context_identity"] = {"fingerprint": "x"}
            with self.assertRaisesRegex(ValueError, "submission identity is malformed"):
                validate_checkpoint(malformed_context, prepared)

            real_without_identities = copy.deepcopy(checkpoint)
            real_without_identities.pop("payload_sha256", None)
            real_without_identities["spec"]["evaluator"] = "real"
            real_without_identities["manifest"].pop("source_design_identity", None)
            real_without_identities["manifest"].pop("eda_context_identity", None)
            real_without_identities["payload_sha256"] = _checkpoint_checksum(
                real_without_identities
            )
            with self.assertRaisesRegex(ValueError, "lacks source design identity"):
                validate_checkpoint(real_without_identities, prepared)

            real_without_checksum = copy.deepcopy(checkpoint)
            real_without_checksum["spec"]["evaluator"] = "real"
            real_without_checksum.pop("payload_sha256", None)
            with self.assertRaisesRegex(ValueError, "checksum is missing"):
                validate_checkpoint(real_without_checksum, prepared)

            mock_without_identities = copy.deepcopy(checkpoint)
            mock_without_identities.pop("payload_sha256", None)
            mock_without_identities["manifest"].pop("source_design_identity", None)
            mock_without_identities["manifest"].pop("eda_context_identity", None)
            validate_checkpoint(mock_without_identities, prepared)

    def test_eda_context_file_identities_are_path_independent_and_content_bound(self):
        with tempfile.TemporaryDirectory() as root:
            adapter_a = os.path.join(root, "adapter-a.il")
            adapter_b = os.path.join(root, "renamed-adapter.il")
            submit_a = os.path.join(root, "submit-a.sh")
            submit_b = os.path.join(root, "renamed-submit.sh")
            for path, content in (
                (adapter_a, "procedure(crescCallback() t)\n"),
                (adapter_b, "procedure(crescCallback() t)\n"),
                (submit_a, "#!/bin/sh\nexec spectre \"$@\"\n"),
                (submit_b, "#!/bin/sh\nexec spectre \"$@\"\n"),
            ):
                with open(path, "w") as stream:
                    stream.write(content)

            def prepared(adapter, submit):
                return {"eda_context": {
                    "cadence": {"version_checks": []},
                    "pdk": {"ok": True, "pdk_name": "PDK", "pdk_adapter": adapter},
                    "submission": {
                        "controller": "maestro", "simulator": "spectre",
                        "submit_script": submit,
                    },
                }}

            first = _prepared_eda_context_identity(prepared(adapter_a, submit_a))
            moved = _prepared_eda_context_identity(prepared(adapter_b, submit_b))
            self.assertEqual(first, moved)
            with open(submit_b, "a") as stream:
                stream.write("# changed\n")
            changed = _prepared_eda_context_identity(prepared(adapter_b, submit_b))
            self.assertNotEqual(first["fingerprint"], changed["fingerprint"])

    def test_automatic_population_scales_for_expected_circuit_dimensions(self):
        for dimensions in range(5, 51):
            expected = max(20, min(100, int(round(5.0 * dimensions ** 0.5))))
            self.assertEqual(_automatic_population_size(dimensions), expected)
        self.assertEqual(_automatic_population_size(5), 20)
        self.assertEqual(_automatic_population_size(25), 25)
        self.assertEqual(_automatic_population_size(50), 35)

    def test_algorithm_defaults_scale_with_variable_count(self):
        nsga = _algorithm_defaults("nsga2", 25)
        differential = _algorithm_defaults("differential_evolution", 25)
        bayesian = _algorithm_defaults("bayesian_optimization", 25)
        self.assertEqual(nsga["population"], 25)
        self.assertAlmostEqual(nsga["mutation_probability"], 0.04)
        self.assertEqual(differential["population"], 25)
        self.assertEqual(bayesian["initial_samples"], 51)
        self.assertEqual(bayesian["max_observations"], 80)
        self.assertAlmostEqual(bayesian["kernel_length_scale"], 0.2)

    def test_explicit_algorithm_is_respected_and_multiobjective_mismatch_errors(self):
        algorithm, policy, settings = _normalize_algorithm_settings({
            "algorithm": "nsga2", "algorithm_parameters": {"population": 23, "seed": 41},
        }, 10, 1)
        self.assertEqual(algorithm, "nsga2")
        self.assertEqual(policy, "explicit_user_selection")
        self.assertEqual(settings["population"], 23)
        self.assertEqual(settings["seed"], 41)
        for algorithm in ("differential_evolution", "bayesian_optimization"):
            with self.assertRaisesRegex(ValueError, "one objective only"):
                _normalize_algorithm_settings({"algorithm": algorithm}, 10, 2)

    def test_bayesian_parameter_validation_rejects_unbounded_training_configuration(self):
        defaults = _algorithm_defaults("bayesian_optimization", 20)
        defaults["max_observations"] = defaults["initial_samples"] - 1
        with self.assertRaisesRegex(ValueError, "max_observations"):
            _normalize_algorithm_settings({
                "algorithm": "bayesian_optimization", "algorithm_parameters": defaults,
            }, 20, 1)

    def test_ten_thousand_candidate_checkpoint_stays_below_ui_limit(self):
        variable_ids = ["x%02d" % index for index in range(50)]
        checkpoint = {
            "schema_version": "crescendo.checkpoint.v1",
            "parameter_ids": variable_ids,
            "candidate_layout": [
                "id", "generation", "index", "status", "values", "metrics",
                "failure_class", "error", "started_at", "finished_at",
            ],
            "spec": {
                "variables": [{"parameter_id": parameter_id} for parameter_id in variable_ids],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            },
            "candidates": [],
            "state": {},
        }
        values = dict((parameter_id, float(index)) for index, parameter_id in enumerate(variable_ids))
        metrics = {
            "feasible": True, "total_violation": 0.0, "merit": -1.0,
            "objectives": [{"value": 1.0, "score": -1.0, "penalized": False}],
            "constraints": [],
        }
        rows = [{
            "candidate_id": "g%03d_c%04d" % (index // 100, index % 100),
            "generation": index // 100, "index": index % 100, "status": "completed",
            "values": values, "metrics": metrics,
        } for index in range(10000)]
        exported = checkpoint_with_candidates(checkpoint, rows)
        payload = json.dumps(exported, separators=(",", ":")).encode("utf-8")
        self.assertLess(len(payload), 64 * 1024 * 1024)
        self.assertEqual(len(checkpoint_candidates(exported)), 10000)

    def test_ocean_startup_timeout_retries_once(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []}}
            config = {"runtime": {"candidate_timeout_seconds": 600, "infrastructure_retries": 1}}
            spec = {"candidate_timeout_seconds": 75, "objectives": [{"output_key": "main::gain", "direction": "maximize"}], "constraints": []}
            parsed = {"path": "rdb", "output_map": {"main::gain": {"summary": {"mean": 1.0}}}}
            commands = [{"returncode": -15, "timed_out": True, "startup_timed_out": True}, {"returncode": 0, "timed_out": False, "startup_timed_out": False}]
            with mock.patch("crescendo_engine.runner.build_workspace", return_value={"root": root}), mock.patch("crescendo_engine.runner.apply_candidate"), mock.patch("crescendo_engine.runner.run_maestro", side_effect=commands) as run_maestro_mock, mock.patch("crescendo_engine.runner.verify_parameter_application", return_value={"ok": True}), mock.patch("crescendo_engine.runner.parse_latest_rdb", return_value=parsed), mock.patch("crescendo_engine.runner.validate_candidate_completion", return_value={"ok": True}), mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}):
                row = _evaluate_candidate(root, 0, 0, {"x": 0.5}, spec, prepared, [], config)
            self.assertEqual(row["status"], "completed")
            self.assertEqual(run_maestro_mock.call_count, 2)
            self.assertEqual(len(row["maestro_attempts"]), 2)
            with open(os.path.join(root, "candidates", "g000_c0000", "timing.json")) as stream:
                timing = json.load(stream)
            self.assertEqual(timing["status"], "completed")
            for phase in ("copy", "lock", "active", "maestro", "spectre", "parse"):
                self.assertIn(phase, timing["phases"])

    def test_infrastructure_failure_is_resampled_without_metrics(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []}, "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}], "outputs": [{"key": "mock::cost"}]}
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1, "candidate_timeout_seconds": 60, "max_infrastructure_replacements": 0}}
            spec = {"evaluator": "mock", "algorithm": "random", "generations": 1, "population": 2, "jobs": 1, "candidate_timeout_seconds": 60, "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}], "objectives": [{"output_key": "mock::cost", "direction": "minimize"}], "constraints": []}
            calls = []

            def evaluate(_root, index, generation, values, _spec, _prepared, _selected, _config):
                calls.append(index)
                if index == 0:
                    return {"candidate_id": "g000_c0000", "generation": 0, "index": 0, "values": values, "status": "infrastructure_failed", "failure_class": "infrastructure", "error": "startup timeout"}
                return {"candidate_id": "g000_c%04d" % index, "generation": generation, "index": index, "values": values, "status": "completed", "metrics": {"objectives": [{"score": values["x"], "value": values["x"]}], "constraints": [], "feasible": True, "total_violation": 0.0, "merit": values["x"]}}

            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=evaluate):
                result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "resample"))
            self.assertEqual(len(calls), 3)
            self.assertEqual(sum(row["status"] == "completed" for row in result["candidates"]), 2)
            infrastructure = [row for row in result["candidates"] if row["status"] == "infrastructure_failed"]
            self.assertEqual(len(infrastructure), 1)
            self.assertNotIn("metrics", infrastructure[0])

    def test_systemic_infrastructure_failure_stops_after_one_replacement_batch(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []}, "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}], "outputs": [{"key": "mock::cost"}]}
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1, "candidate_timeout_seconds": 60, "max_infrastructure_replacements": 2}}
            spec = {"evaluator": "mock", "algorithm": "random", "generations": 10, "population": 2, "jobs": 1, "candidate_timeout_seconds": 60, "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}], "objectives": [{"output_key": "mock::cost", "direction": "minimize"}], "constraints": []}
            calls = []

            def infrastructure(_root, index, generation, values, _spec, _prepared, _selected, _config):
                calls.append((generation, index))
                return {"candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation, "index": index, "values": values, "status": "infrastructure_failed", "failure_class": "infrastructure", "error": "startup timeout"}

            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=infrastructure):
                with self.assertRaisesRegex(RuntimeError, "systemic infrastructure failure in generation 1"):
                    run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "systemic"))
            self.assertEqual(calls, [(0, 0), (0, 1), (0, 2), (0, 3)])

    def test_candidate_timeout_is_forwarded_to_maestro(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []}, "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}], "outputs": [{"key": "main::gain"}]}
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1, "candidate_timeout_seconds": 600}}
            spec = {"algorithm": "random", "generations": 1, "population": 4, "jobs": 1, "candidate_timeout_seconds": 75, "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}], "objectives": [{"output_key": "main::gain", "direction": "maximize"}], "constraints": []}
            parsed = {"path": "rdb", "output_map": {"main::gain": {"summary": {"mean": 1.0}}}}
            with mock.patch("crescendo_engine.runner.build_workspace", return_value={"root": root}), mock.patch("crescendo_engine.runner.apply_candidate"), mock.patch("crescendo_engine.runner.run_maestro", return_value={"returncode": 0, "timed_out": False}) as run_maestro_mock, mock.patch("crescendo_engine.runner.verify_parameter_application", return_value={"ok": True}), mock.patch("crescendo_engine.runner.parse_latest_rdb", return_value=parsed), mock.patch("crescendo_engine.runner.validate_candidate_completion", return_value={"ok": True}), mock.patch("crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True}), mock.patch("crescendo_engine.runner.persist_lightweight_candidate"), mock.patch("crescendo_engine.runner.safe_delete_candidate_workspace"):
                run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertGreaterEqual(run_maestro_mock.call_args[1]["timeout_seconds"], 74)
            self.assertLessEqual(run_maestro_mock.call_args[1]["timeout_seconds"], 75)

    def test_candidate_timeout_rejects_zero(self):
        config = {"runtime": {"candidate_timeout_seconds": 600}}
        with self.assertRaisesRegex(ValueError, "0 is not allowed"):
            run_optimization({"candidate_timeout_seconds": 0, "variables": [], "objectives": [{"output_key": "x"}]}, config, prepared={})

    def test_simulation_failure_receives_finite_penalty_metrics(self):
        spec = {
            "objectives": [{"output_key": "main::gain", "direction": "maximize", "corners": ["slow"]}],
            "constraints": [{"output_key": "main::power", "operator": "<=", "limit": 1.0, "corners": ["slow"]}],
        }
        metrics = _failure_metrics(spec, RuntimeError("simulation did not converge"))
        self.assertFalse(metrics["feasible"])
        self.assertTrue(metrics["penalized"])
        self.assertGreater(metrics["total_violation"], 0)
        self.assertGreater(metrics["merit"], 0)
        self.assertTrue(metrics["objectives"][0]["penalized"])
        self.assertEqual(metrics["objectives"][0]["corners"], ["slow"])

    def test_only_meaningful_numeric_cdf_parameters_are_optimizable(self):
        setup = {"tests": []}
        hierarchy = {
            "designs": [{
                "hierarchy": {
                    "parameters": [
                        {
                            "parameter_id": "cdf-vdc",
                            "source": "cdf",
                            "tunable": True,
                            "numeric_value": 0.55,
                            "hierarchy_path": "/VBIAS",
                            "name": "vdc",
                        },
                        {
                            "parameter_id": "db-dc",
                            "source": "db_property",
                            "tunable": True,
                            "numeric_value": 0.55,
                            "hierarchy_path": "/VBIAS",
                            "name": "dc",
                        },
                        {
                            "parameter_id": "cdf-area",
                            "source": "cdf",
                            "tunable": True,
                            "numeric_value": 1e-12,
                            "hierarchy_path": "/M0",
                            "name": "ad",
                            "prompt": "Drain area",
                        },
                        {
                            "parameter_id": "cdf-width",
                            "source": "cdf",
                            "tunable": True,
                            "numeric_value": 2e-6,
                            "hierarchy_path": "/M0",
                            "name": "fingerWidth",
                        },
                    ]
                }
            }]
        }
        catalog = dict((row["parameter_id"], row) for row in parameter_catalog(setup, hierarchy))
        self.assertTrue(catalog["cdf-vdc"]["optimizable"])
        self.assertFalse(catalog["db-dc"]["optimizable"])
        self.assertFalse(catalog["cdf-area"]["optimizable"])
        self.assertTrue(catalog["cdf-width"]["optimizable"])
        self.assertEqual(catalog["cdf-width"]["semantic"], "width")
        self.assertTrue(catalog["cdf-width"]["default_selected"])

    def test_parameter_semantic_fallbacks_cover_common_device_controls(self):
        names = {
            "channelWidth": "width",
            "gateLength": "length",
            "nf": "fingers",
            "multiplicity": "multiplier",
            "numberOfSegments": "segments",
            "nTurns": "turns",
            "resistance": "resistance",
            "capValue": "capacitance",
            "vdc": "voltage",
        }
        hierarchy = {"designs": [{"hierarchy": {"parameters": [
            {"parameter_id": name, "source": "cdf", "tunable": True, "numeric_value": 1.0, "hierarchy_path": "/D", "name": name}
            for name in names
        ]}}]}
        catalog = dict((row["parameter_id"], row) for row in parameter_catalog({"tests": []}, hierarchy))
        for name, semantic in names.items():
            self.assertTrue(catalog[name]["optimizable"], name)
            self.assertEqual(catalog[name]["semantic"], semantic)

    def test_total_width_is_not_exposed(self):
        hierarchy = {"designs": [{"hierarchy": {"parameters": [
            {"parameter_id": "wf", "source": "cdf", "tunable": True, "numeric_value": 2e-6, "hierarchy_path": "/M0", "name": "wf", "prompt": "total_width(M)"},
            {"parameter_id": "total_width", "source": "cdf", "tunable": True, "numeric_value": 2e-6, "hierarchy_path": "/M0", "name": "total_width"},
            {"parameter_id": "wt", "source": "cdf", "tunable": True, "numeric_value": 2e-6, "hierarchy_path": "/M0", "name": "wt", "prompt": "Total width(M)"},
        ]}}]}
        catalog = dict((row["parameter_id"], row) for row in parameter_catalog({"tests": []}, hierarchy))
        self.assertFalse(catalog["wf"]["optimizable"])
        self.assertFalse(catalog["total_width"]["optimizable"])
        self.assertFalse(catalog["wt"]["optimizable"])

    def test_vendor_width_aliases_and_offsets_are_distinguished(self):
        expected = {
            "wf": ("Finger width", "width"),
            "fw": (None, "width"),
            "wg": (None, "width"),
            "lg": (None, "length"),
            "nmult": (None, "multiplier"),
            "nr": ("Number Of Turns", "turns"),
            "segR": ("Segment Resistance", "resistance"),
            "rad": ("Inner_Radius(M)", "radius"),
            "sp": ("Spacing", "spacing"),
            "c_C": ("Offset_Width(F)", None),
            "dw": ("Delta Width", None),
            "effW": ("Effective Width", None),
            "cktL": ("Subckt calculated Length", None),
            "waddr": ("Deep Nwell diode Add Width", None),
        }
        hierarchy = {"designs": [{"hierarchy": {"parameters": [
            {"parameter_id": name, "source": "cdf", "tunable": True,
             "numeric_value": 1.0, "hierarchy_path": "/M0", "name": name, "prompt": prompt}
            for name, (prompt, semantic) in expected.items()
        ]}}]}
        catalog = {row["parameter_id"]: row for row in parameter_catalog({"tests": []}, hierarchy)}
        for name, (prompt, semantic) in expected.items():
            self.assertEqual(catalog[name]["semantic"], semantic, name)
            self.assertEqual(catalog[name]["optimizable"], semantic is not None, name)

    def test_pdk_internal_schematic_controls_remain_read_only(self):
        hierarchy = {"designs": [{"design": {"library": "D", "cell": "TB"}, "hierarchy": {
            "cells": [{"library": "D"}, {"library": "PDK", "technology_library": "PDK"}],
            "instances": [{"master_library": "PDK", "has_schematic": True}],
            "parameters": [
                {"parameter_id": "wrapper", "source": "cdf", "tunable": True,
                 "numeric_value": 2.0, "hierarchy_path": "/R0", "name": "m", "owner_library": "D"},
                {"parameter_id": "internal", "source": "cdf", "tunable": True,
                 "numeric_value": 1.0, "hierarchy_path": "/R0/R0", "name": "m", "owner_library": "PDK"},
            ],
        }}]}
        catalog = {row["parameter_id"]: row for row in parameter_catalog({"tests": []}, hierarchy)}
        self.assertTrue(catalog["wrapper"]["optimizable"])
        self.assertFalse(catalog["internal"]["optimizable"])
        self.assertEqual(catalog["internal"]["parameter_policy"], "reference_library_internal")

    def test_rf_device_prompt_keywords_are_exposed(self):
        expected = {
            "wr": ("Width_per_Finger(M)", "width"),
            "lr": ("Length_per_Finger(M)", "length"),
            "nr": ("Number_of_Fingers", "fingers"),
        }
        hierarchy = {"designs": [{"hierarchy": {"parameters": [
            {"parameter_id": name, "source": "cdf", "tunable": True, "numeric_value": 1.0, "hierarchy_path": "/M0", "name": name, "prompt": values[0]}
            for name, values in expected.items()
        ]}}]}
        catalog = dict((row["parameter_id"], row) for row in parameter_catalog({"tests": []}, hierarchy))
        for name, values in expected.items():
            self.assertTrue(catalog[name]["optimizable"], name)
            self.assertEqual(catalog[name]["semantic"], values[1])
        self.assertEqual(catalog["wr"]["value_type"], "float")
        self.assertEqual(catalog["lr"]["value_type"], "float")
        self.assertEqual(catalog["nr"]["value_type"], "integer")

    def test_catalog_uses_cdf_integer_type_before_semantic_fallback(self):
        hierarchy = {"designs": [{"hierarchy": {"parameters": [
            {"parameter_id": "count", "source": "cdf", "tunable": True, "numeric_value": 2.0,
             "hierarchy_path": "/D", "name": "resistance", "cdf_parameter_type": "natural"},
            {"parameter_id": "mult", "source": "cdf", "tunable": True, "numeric_value": 2.0,
             "hierarchy_path": "/D", "name": "m", "cdf_parameter_type": "string"},
            {"parameter_id": "typed", "source": "cdf", "tunable": True, "numeric_value": 2.0,
             "hierarchy_path": "/D", "name": "width", "cdf_type": "int"},
        ]}}]}
        catalog = dict((row["parameter_id"], row) for row in parameter_catalog({"tests": []}, hierarchy))
        self.assertEqual(catalog["count"]["value_type"], "integer")
        self.assertEqual(catalog["mult"]["value_type"], "integer")
        self.assertEqual(catalog["typed"]["value_type"], "integer")

    def test_turns_type_uses_fractional_device_evidence_without_widening_counts(self):
        cases = [
            ("current", {"current_value": "1.5", "default_value": "1.5", "numeric_value": 1.5}, "float"),
            ("default", {"current_value": "1", "default_value": "1.5"}, "float"),
            ("numeric", {"numeric_value": 1.5}, "float"),
            ("choices", {"choices_raw": '("1" "1.5" "2.5")'}, "float"),
            ("integers", {"current_value": "1", "default_value": "2"}, "integer"),
            ("cyclic", {"choices_raw": ["3", "4", "5", "6"]}, "integer"),
            ("declared_int", {"current_value": "1.5", "cdf_parameter_type": "natural"}, "integer"),
        ]
        for name, metadata, expected in cases:
            parameter = dict({
                "parameter_id": name, "source": "cdf", "tunable": True, "numeric_value": 1.0,
                "hierarchy_path": "/L0", "name": "n", "prompt": "Number of Turns",
                "cdf_parameter_type": "string",
            }, **metadata)
            hierarchy = {"designs": [{"hierarchy": {"parameters": [parameter]}}]}
            with self.subTest(name=name):
                row = parameter_catalog({"tests": []}, hierarchy)[0]
                self.assertEqual(row["semantic"], "turns")
                self.assertEqual(row["value_type"], expected)
        for name in ("nf", "m", "nseg", "nx"):
            hierarchy = {"designs": [{"hierarchy": {"parameters": [{
                "parameter_id": name, "source": "cdf", "tunable": True, "numeric_value": 1.5,
                "hierarchy_path": "/M0", "name": name, "current_value": "1.5",
                "cdf_parameter_type": "string",
            }]}}]}
            with self.subTest(count=name):
                self.assertEqual(parameter_catalog({"tests": []}, hierarchy)[0]["value_type"], "integer")

    def test_explicit_cdf_float_types_override_integer_semantic_heuristic(self):
        for declared in ("float", "real", "double"):
            for name, prompt in (("n", "Number of Turns"), ("nf", "Number of Fingers")):
                hierarchy = {"designs": [{"hierarchy": {"parameters": [{
                    "parameter_id": name, "source": "cdf", "tunable": True, "numeric_value": 1.0,
                    "hierarchy_path": "/L0", "name": name, "prompt": prompt,
                    "cdf_parameter_type": declared,
                }]}}]}
                with self.subTest(declared=declared, name=name):
                    self.assertEqual(parameter_catalog({"tests": []}, hierarchy)[0]["value_type"], "float")

    def test_half_turns_sample_floats_while_cyclic_turns_keep_original_choices(self):
        hierarchy = {"designs": [{"hierarchy": {"parameters": [{
            "parameter_id": "half", "source": "cdf", "tunable": True, "numeric_value": 1.5,
            "hierarchy_path": "/L0", "name": "n", "prompt": "Number of Turns",
            "current_value": "1.5", "default_value": "1.5", "cdf_parameter_type": "string",
        }, {
            "parameter_id": "cyclic", "source": "cdf", "tunable": True, "numeric_value": 3.0,
            "hierarchy_path": "/L1", "name": "n", "prompt": "Number of Turns",
            "current_value": "3", "default_value": "3", "cdf_parameter_type": "string",
            "choices_raw": '("3" "4" "5" "6")',
        }]}}]}
        parameters = parameter_catalog({"tests": []}, hierarchy)
        variables = _enforce_catalog_variable_types([
            {"parameter_id": "half", "minimum": 1, "maximum": 2},
            {"parameter_id": "cyclic", "minimum": 3, "maximum": 6},
        ], parameters)
        generator = PopulationGenerator(variables, "random", seed=7)
        candidates = generator.ask(40)
        self.assertEqual(generator.variables[0]["value_type"], "float")
        self.assertTrue(any(not row["half"].is_integer() for row in candidates))
        self.assertEqual(generator.variables[1]["value_type"], "enum")
        self.assertEqual(generator.variables[1]["choices"], ["3", "4", "5", "6"])
        self.assertEqual(set(row["cyclic"] for row in candidates), {"3", "4", "5", "6"})

    def test_backend_overrides_ui_value_type_from_catalog(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "nf", "backend": "schematic", "optimizable": True,
                                "semantic": "fingers", "value_type": "integer"}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            spec = {
                "evaluator": "mock", "algorithm": "random", "generations": 1, "population": 4,
                "variables": [{"parameter_id": "nf", "minimum": 1, "maximum": 4,
                               "value_type": "float", "scale": "log"}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize"}],
            }
            observed = []

            def evaluate(_root, index, generation, values, _spec, _prepared, _selected, _config):
                observed.append(values["nf"])
                return {"candidate_id": "g000_c%04d" % index, "generation": generation, "index": index,
                        "values": values, "status": "completed", "metrics": {"objectives": [{"score": values["nf"], "value": values["nf"]}],
                        "constraints": [], "feasible": True, "total_violation": 0.0, "merit": values["nf"]}}

            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=evaluate):
                run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "typed"))
            self.assertTrue(all(isinstance(value, int) for value in observed))

    def test_top_level_analog_sources_resolve_design_variables_independently(self):
        setup = {"tests": [{
            "name": "main", "library": "HH", "cell": "TB", "variables": [
                {"name": "VDD", "value": "1.2"},
                {"name": "IB", "value": "600u"},
            ],
        }]}
        hierarchy = {"designs": [{
            "design": ["HH", "TB", "schematic"],
            "hierarchy": {
                "instances": [
                    {"hierarchy_path": "/VSUP", "master_library": "analogLib", "master_cell": "vdc"},
                    {"hierarchy_path": "/IBIAS", "master_library": "analogLib", "master_cell": "idc"},
                    {"hierarchy_path": "/XDUT/VINNER", "master_library": "analogLib", "master_cell": "vdc"},
                ],
                "parameters": [
                    {"parameter_id": "vdd", "hierarchy_path": "/VSUP", "name": "vdc", "prompt": "DC voltage", "current_value": "VDD", "source": "cdf", "editable": True, "tunable": False, "numeric_value": None},
                    {"parameter_id": "ib", "hierarchy_path": "/IBIAS", "name": "idc", "prompt": "DC current", "current_value": "IB", "source": "cdf", "editable": True, "tunable": False, "numeric_value": None},
                    {"parameter_id": "nested", "hierarchy_path": "/XDUT/VINNER", "name": "vdc", "prompt": "DC voltage", "current_value": "VDD", "source": "cdf", "editable": True, "tunable": False, "numeric_value": None},
                ],
            },
        }]}
        catalog = dict((row["parameter_id"], row) for row in parameter_catalog(setup, hierarchy))
        self.assertTrue(catalog["vdd"]["optimizable"])
        self.assertIsNone(catalog["vdd"]["numeric_value"])
        self.assertEqual(catalog["vdd"]["baseline_expression"], "VDD")
        self.assertTrue(catalog["vdd"]["requires_point_numeric_value"])
        self.assertEqual(catalog["vdd"]["parameter_policy"], "top_level_analog_source")
        self.assertTrue(catalog["ib"]["optimizable"])
        self.assertIsNone(catalog["ib"]["numeric_value"])
        self.assertEqual(catalog["ib"]["baseline_expression"], "IB")
        self.assertFalse(catalog["nested"]["optimizable"])

    def test_mock_closed_loop_and_auto_stop(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 3}}
            spec = {
                "evaluator": "mock",
                "algorithm": "nsga2",
                "seed": 3,
                "generations": 3,
                "population": 4,
                "jobs": 2,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 2.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize", "aggregation": "mean", "mock_target": 1.0}],
                "constraints": [],
            }
            result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertEqual(result["state"], "completed")
            self.assertEqual(len(result["candidates"]), 12)
            self.assertTrue(result["pareto_front"])
            timings = result["generation_timings"]
            self.assertEqual([row["generation"] for row in timings], [1, 2, 3])
            for timing in timings:
                self.assertEqual(timing["timed_candidate_count"], 4)
                self.assertTrue({"ask", "evaluate", "tell", "settlement", "persist"} <= set(timing["phases_seconds"]))
                self.assertAlmostEqual(sum(timing["phases_seconds"].values()), timing["total_seconds"])
            self.assertEqual(load_json(os.path.join(root, "run", "status.json"))["generation_timings"], timings)
            self.assertEqual(load_json(os.path.join(root, "run", "result.json"))["generation_timings"], timings)
            self.assertTrue(os.path.isfile(os.path.join(root, "run", "best_assignment.json")))
            self.assertEqual(result["algorithm"], "nsga2")
            self.assertEqual(result["algorithm_policy"], "explicit_user_selection")
            self.assertEqual(result["algorithm_parameters"]["population"], 4)
            self.assertEqual(result["representative_role"], "single_objective_best")

    def test_multiobjective_rejects_explicit_single_objective_algorithm(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": []},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::gain"}, {"key": "mock::power"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 2}}
            spec = {
                "evaluator": "mock",
                "algorithm": "differential_evolution",
                "seed": 5,
                "generations": 3,
                "population": 4,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 2.0}],
                "objectives": [
                    {"output_key": "mock::gain", "direction": "minimize", "mock_target": 0.2},
                    {"output_key": "mock::power", "direction": "minimize", "mock_target": 1.8},
                ],
                "constraints": [],
                "auto_stop": {"patience": 1, "tolerance": 1.0e30},
            }
            with self.assertRaisesRegex(ValueError, "one objective only"):
                run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "multi"))

    def test_automatic_optimizer_rejects_population_below_four(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": []},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root}}
            spec = {
                "evaluator": "mock",
                "generations": 2,
                "population": 3,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize"}],
            }
            with self.assertRaisesRegex(ValueError, "population must be at least 4"):
                run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "small"))

    def test_closed_loop_continues_after_fully_failed_first_generation(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            spec = {
                "evaluator": "mock", "algorithm": "nsga2", "generations": 2, "population": 4, "jobs": 1,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 2.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize", "aggregation": "mean"}],
                "constraints": [],
            }
            calls = []

            def evaluate(_root, index, generation, values, _spec, _prepared, _selected, _config):
                calls.append((generation, index))
                if generation == 0:
                    return {"candidate_id": "g000_c%04d" % index, "generation": generation, "index": index, "values": values, "status": "failed", "error": "no convergence", "metrics": _failure_metrics(spec, RuntimeError("no convergence"))}
                return {"candidate_id": "g001_c%04d" % index, "generation": generation, "index": index, "values": values, "status": "completed", "metrics": {"objectives": [{"score": values["x"], "value": values["x"]}], "constraints": [], "feasible": True, "total_violation": 0.0, "merit": values["x"]}}

            with mock.patch("crescendo_engine.runner._evaluate_candidate", side_effect=evaluate):
                result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "retry"))
            self.assertEqual(len(calls), 8)
            self.assertEqual(result["state"], "completed")
            self.assertEqual(sum(row["status"] == "failed" for row in result["candidates"]), 4)
            self.assertEqual(sum(row["status"] == "completed" for row in result["candidates"]), 4)

    def test_mock_auto_stop(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {"run_root": root, "max_parallel_jobs": 1}}
            spec = {
                "evaluator": "mock",
                "algorithm": "random",
                "generations": 5,
                "population": 2,
                "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 2.0}],
                "objectives": [{"output_key": "mock::cost", "direction": "minimize", "aggregation": "mean"}],
                "constraints": [],
                "auto_stop": {"patience": 1, "tolerance": 1000000000.0},
            }
            result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "auto"))
            self.assertEqual(result["stopped_reason"], "auto_stop_no_improvement")
            self.assertEqual(len(result["candidates"]), 4)
            checkpoint = load_json(os.path.join(root, "auto", "checkpoint.json"))
            self.assertEqual(checkpoint["state"]["terminal_state"], "completed")
            self.assertEqual(
                checkpoint["state"]["terminal_reason"], "auto_stop_no_improvement",
            )
            with mock.patch("crescendo_engine.runner._evaluate_candidate") as evaluate:
                resumed = _coordinated_runner_resume(
                    spec, config, prepared, os.path.join(root, "auto-resumed"), checkpoint,
                )
            evaluate.assert_not_called()
            self.assertEqual(resumed["stopped_reason"], "auto_stop_no_improvement")

    def test_persistent_protocol_failure_diagnostics_reports_actual_layer(self):
        from crescendo_engine.runner import (
            _persistent_protocol_failure_diagnostics,
        )

        cases = (
            (
                ["BEGIN", "ERROR"], {"run_count": 0},
                {"ocean.stderr": "CRESCENDO_ADAPTER_ERROR CDF callback rejected width"},
                ("pdk_callback", "not_started"),
            ),
            (
                ["BEGIN", "PATCHED", "ERROR"], {"run_count": 0}, {},
                ("netlist_or_submission", "not_started"),
            ),
            (
                ["BEGIN", "PATCHED", "ERROR"], {"run_count": 1}, {},
                ("spectre", "started"),
            ),
            (
                ["BEGIN", "PATCHED", "SIM_DONE", "ERROR"],
                {"run_count": 0}, {},
                ("result_publication", "started"),
            ),
        )
        for states, spectre, diagnostic, expected in cases:
            with self.subTest(states=states, spectre=spectre):
                self.assertEqual(
                    _persistent_protocol_failure_diagnostics(
                        states, spectre, diagnostic,
                    ),
                    expected,
                )

    def test_workspace_build_failure_candidate_is_pre_spectre_infrastructure(self):
        from crescendo_engine.workspace import WorkspaceBuildFailure

        with tempfile.TemporaryDirectory() as root:
            failure = WorkspaceBuildFailure(
                IOError("permission denied: maestro.sdb"),
                {"ok": True, "removed": True, "cleanup_error": None},
            )
            prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []}}
            config = {
                "runtime": {
                    "candidate_timeout_seconds": 60,
                    "infrastructure_retries": 0,
                },
                "cadence": {},
            }
            spec = {
                "evaluator": "real", "candidate_timeout_seconds": 60,
                "objectives": [], "constraints": [],
            }
            with mock.patch(
                    "crescendo_engine.runner.build_workspace",
                    side_effect=failure):
                row = _evaluate_candidate(
                    root, 0, 0, {"x": 0.5}, spec, prepared, [], config,
                )
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "workspace_build")
            self.assertEqual(row["failure_stage"], "workspace_build")
            self.assertEqual(row["spectre_state"], "not_started")
            self.assertNotIn("metrics", row)

    def test_legacy_scheduler_stops_after_first_observed_workspace_failure(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic",
                    "optimizable": True,
                }],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {
                "run_root": root, "max_parallel_jobs": 1,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 10,
            }}
            spec = {
                "evaluator": "mock", "algorithm": "random",
                "generations": 1, "population": 5, "jobs": 1,
                "candidate_timeout_seconds": 60,
                "variables": [{
                    "parameter_id": "x", "minimum": 0.0, "maximum": 1.0,
                }],
                "objectives": [{
                    "output_key": "mock::cost", "direction": "minimize",
                }],
                "constraints": [],
            }
            evaluated = []

            def workspace_failure(
                    _root, index, generation, values, _spec, _prepared,
                    _selected, _config):
                evaluated.append(index)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "failed",
                    "failure_class": "design_or_simulation",
                    "error": (
                        "WorkspaceBuildFailure: workspace build failed: "
                        "Permission denied: maestro.sdb"
                    ),
                }

            with mock.patch(
                    "crescendo_engine.runner._evaluate_candidate",
                    side_effect=workspace_failure):
                with self.assertRaisesRegex(
                        RuntimeError, "working workspace build failed"):
                    run_optimization(
                        spec, config, prepared=prepared,
                        run_root=os.path.join(root, "run"),
                    )
            self.assertEqual(evaluated, [0])

    def test_source_integrity_failure_overrides_prior_failure_stage(self):
        from crescendo_engine.runner import _persistent_failure

        row = {"failure_stage": "parameter_application"}
        _persistent_failure(
            row, {"objectives": [], "constraints": []},
            RuntimeError("source changed"), infrastructure=True,
            failure_class="source_integrity",
        )
        self.assertEqual(row["failure_stage"], "source_integrity")
        self.assertEqual(row["failure_class"], "source_integrity")

    def test_batch_protocol_diagnostics_uses_candidate_owned_spectre_evidence(self):
        from crescendo_engine.runner import (
            _persistent_batch_protocol_failure_diagnostics,
        )

        states = ["BEGIN", "PATCHED", "ERROR"]
        with tempfile.TemporaryDirectory() as candidate_root:
            self.assertEqual(
                _persistent_batch_protocol_failure_diagnostics(
                    states, {"run_count": 0}, {}, candidate_root,
                ),
                ("netlist_or_submission", "not_started"),
            )
            self.assertEqual(
                _persistent_batch_protocol_failure_diagnostics(
                    states, {"run_count": 2}, {}, candidate_root,
                ),
                ("netlist_or_spectre", "unknown"),
            )
            with open(os.path.join(
                    candidate_root, "retained_spectre_candidate.log"), "w") as stream:
                stream.write("Spectre started\n")
            self.assertEqual(
                _persistent_batch_protocol_failure_diagnostics(
                    states, {"run_count": 2}, {}, candidate_root,
                ),
                ("spectre", "started"),
            )

    def test_checkpoint_candidate_round_trip_preserves_failure_location(self):
        from crescendo_engine.runner import _pack_candidate, _unpack_candidate

        row = {
            "candidate_id": "g000_c0000", "generation": 0, "index": 0,
            "status": "failed", "values": {"x": 1.0},
            "failure_class": "parameter_application",
            "error": "callback rejected x", "failure_stage": "pdk_callback",
            "spectre_state": "not_started",
        }
        packed = _pack_candidate(row, ["x"])
        restored = _unpack_candidate(
            packed, ["x"], {"objectives": [], "constraints": []},
        )
        self.assertEqual(restored["failure_stage"], "pdk_callback")
        self.assertEqual(restored["spectre_state"], "not_started")
        legacy = _unpack_candidate(
            packed[:10], ["x"], {"objectives": [], "constraints": []},
        )
        self.assertNotIn("failure_stage", legacy)
        self.assertNotIn("spectre_state", legacy)

    def test_checkpoint_integrity_rejects_invalid_candidate_layout(self):
        base = _minimal_checkpoint()
        diagnostics_layout = [
            "id", "generation", "index", "status", "values", "metrics",
            "failure_class", "error", "started_at", "finished_at",
            "failure_stage", "spectre_state",
        ]
        cases = []

        missing = copy.deepcopy(base)
        missing.pop("candidate_layout")
        cases.append(("missing", missing, "candidate_layout is missing"))

        unknown = copy.deepcopy(base)
        unknown["candidate_layout"] = ["id"]
        cases.append(("unknown", unknown, "candidate_layout is not supported"))

        base_with_diagnostics_row = copy.deepcopy(base)
        base_with_diagnostics_row["candidates"][0].extend((None, None))
        cases.append((
            "base_with_diagnostics_row", base_with_diagnostics_row,
            "width does not match candidate_layout",
        ))

        diagnostics_with_base_row = copy.deepcopy(base)
        diagnostics_with_base_row["candidate_layout"] = diagnostics_layout
        cases.append((
            "diagnostics_with_base_row", diagnostics_with_base_row,
            "width does not match candidate_layout",
        ))

        for label, checkpoint, message in cases:
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
            with self.subTest(case=label):
                with self.assertRaisesRegex(ValueError, message):
                    validate_checkpoint_integrity(checkpoint)

    def test_workspace_failure_submits_no_more_than_parallel_job_limit(self):
        import threading

        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic",
                    "optimizable": True,
                }],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {
                "run_root": root, "max_parallel_jobs": 3,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 10,
            }}
            spec = {
                "evaluator": "mock", "algorithm": "random",
                "generations": 1, "population": 8, "jobs": 3,
                "candidate_timeout_seconds": 60,
                "variables": [{
                    "parameter_id": "x", "minimum": 0.0, "maximum": 1.0,
                }],
                "objectives": [{
                    "output_key": "mock::cost", "direction": "minimize",
                }],
                "constraints": [],
            }
            evaluated = []
            initial_wave = threading.Barrier(3)

            def workspace_failure(
                    _root, index, generation, values, _spec, _prepared,
                    _selected, _config):
                evaluated.append(index)
                initial_wave.wait(2)
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index, "values": values,
                    "status": "failed", "failure_class": "workspace_build",
                    "failure_stage": "workspace_build",
                    "spectre_state": "not_started",
                    "error": "WorkspaceBuildFailure: permission denied",
                }

            with mock.patch(
                    "crescendo_engine.runner._evaluate_candidate",
                    side_effect=workspace_failure):
                with self.assertRaisesRegex(
                        RuntimeError, "working workspace build failed"):
                    run_optimization(
                        spec, config, prepared=prepared,
                        run_root=os.path.join(root, "run"),
                    )
            self.assertEqual(sorted(evaluated), [0, 1, 2])

    def test_workspace_failure_does_not_refill_mixed_completed_batch(self):
        import threading

        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic",
                    "optimizable": True,
                }],
                "outputs": [{"key": "mock::cost"}],
            }
            config = {"runtime": {
                "run_root": root, "max_parallel_jobs": 3,
                "candidate_timeout_seconds": 60,
                "max_infrastructure_replacements": 10,
            }}
            spec = {
                "evaluator": "mock", "algorithm": "random",
                "generations": 1, "population": 8, "jobs": 3,
                "candidate_timeout_seconds": 60,
                "variables": [{
                    "parameter_id": "x", "minimum": 0.0, "maximum": 1.0,
                }],
                "objectives": [{
                    "output_key": "mock::cost", "direction": "minimize",
                }],
                "constraints": [],
            }
            evaluated = []
            evaluated_lock = threading.Lock()
            candidate_three_started = threading.Event()

            def mixed_results(
                    _root, index, generation, values, _spec, _prepared,
                    _selected, _config):
                with evaluated_lock:
                    evaluated.append(index)
                if index == 1:
                    candidate_three_started.wait(0.5)
                    return {
                        "candidate_id": "g%03d_c%04d" % (generation, index),
                        "generation": generation, "index": index,
                        "values": values, "status": "failed",
                        "failure_class": "workspace_build",
                        "failure_stage": "workspace_build",
                        "spectre_state": "not_started",
                        "error": "WorkspaceBuildFailure: permission denied",
                    }
                if index == 3:
                    candidate_three_started.set()
                return {
                    "candidate_id": "g%03d_c%04d" % (generation, index),
                    "generation": generation, "index": index,
                    "values": values, "status": "completed",
                    "metrics": {
                        "objectives": [{
                            "score": values["x"], "value": values["x"],
                        }],
                        "constraints": [], "feasible": True,
                        "total_violation": 0.0, "merit": values["x"],
                    },
                }

            def return_whole_batch(futures, return_when=None):
                return set(futures), set()

            with mock.patch(
                    "crescendo_engine.runner._evaluate_candidate",
                    side_effect=mixed_results), mock.patch(
                    "crescendo_engine.runner.wait",
                    side_effect=return_whole_batch):
                with self.assertRaisesRegex(
                        RuntimeError, "working workspace build failed"):
                    run_optimization(
                        spec, config, prepared=prepared,
                        run_root=os.path.join(root, "run"),
                    )
            self.assertEqual(sorted(evaluated), [0, 1, 2])

    def test_checkpoint_with_candidates_preserves_explicit_legacy_layout(self):
        legacy_layout = [
            "id", "generation", "index", "status", "values", "metrics",
            "failure_class", "error", "started_at", "finished_at",
        ]
        checkpoint = _minimal_checkpoint()
        checkpoint["candidate_layout"] = legacy_layout
        rows = checkpoint_candidates(checkpoint)
        rows[0]["failure_stage"] = "parameter_verification"
        rows[0]["spectre_state"] = "started"
        exported = checkpoint_with_candidates(checkpoint, rows)
        self.assertEqual(exported["candidate_layout"], legacy_layout)
        self.assertTrue(all(len(packed) == 10 for packed in exported["candidates"]))
        self.assertNotIn(
            "failure_stage", checkpoint_candidates(exported)[0],
        )

    def test_legacy_layout_resume_keeps_anchor_compatible_on_first_save(self):
        legacy_layout = [
            "id", "generation", "index", "status", "values", "metrics",
            "failure_class", "error", "started_at", "finished_at",
        ]
        with tempfile.TemporaryDirectory() as root:
            initial_runtime = os.path.join(root, "initial-runtime")
            prepared = {
                "setup": {"tests": [{"name": "main", "enabled": True}], "selected_test": "main", "_source_tests": [{"name": "main", "enabled": True}]}, "hierarchy": {"designs": []},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic",
                    "optimizable": True,
                }],
                "outputs": [{"key": "mock::cost"}],
            }
            spec = {
                "evaluator": "mock", "algorithm": "random",
                "generations": 1, "population": 2, "jobs": 1,
                "variables": [{
                    "parameter_id": "x", "minimum": 0.0, "maximum": 1.0,
                }],
                "objectives": [{
                    "output_key": "mock::cost", "direction": "minimize",
                }],
                "constraints": [],
            }
            initial_config = {"runtime": {
                "run_root": initial_runtime, "max_parallel_jobs": 1,
                "persistent_min_free_disk_mib": 1,
            }}
            initial_root = os.path.join(initial_runtime, "initial")
            run_optimization(
                spec, initial_config, prepared=prepared, run_root=initial_root,
            )
            checkpoint = load_json(os.path.join(initial_root, "checkpoint.json"))
            checkpoint["candidate_layout"] = legacy_layout
            checkpoint["candidates"] = [
                packed[:10] for packed in checkpoint["candidates"]
            ]
            checkpoint["spec"]["generations"] = 2
            checkpoint["state"]["next_generation"] = 1
            checkpoint["state"]["active_plan"] = None
            checkpoint["state"].pop("terminal_state", None)
            checkpoint["state"].pop("terminal_reason", None)
            checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)

            resumed_runtime = os.path.join(root, "resumed-runtime")
            resumed_config = {"runtime": {
                "run_root": resumed_runtime, "max_parallel_jobs": 1,
                "persistent_min_free_disk_mib": 1,
            }}
            resumed_root = os.path.join(resumed_runtime, "resumed")
            result = _coordinated_runner_resume(
                checkpoint["spec"], resumed_config, prepared, resumed_root,
                checkpoint,
            )
            self.assertEqual(result["state"], "completed")
            resumed_checkpoint = load_json(os.path.join(
                resumed_root, "checkpoint.json",
            ))
            self.assertEqual(
                resumed_checkpoint["candidate_layout"], legacy_layout,
            )
            self.assertTrue(all(
                len(packed) == 10
                for packed in resumed_checkpoint["candidates"]
            ))


# Retired native-ADE whole-loop contracts use an exact v0.3.0 test fixture.
# Current helpers and ordinary optimizer/mock tests above still use production code.
from tests.legacy_native_fixture import historical_native_loop


class LegacyNativeRunnerTest(unittest.TestCase):
    """Historical native scheduling assertions, excluded from new-pipeline evidence."""


_HISTORICAL_NATIVE_TESTS = (
    'test_parameter_application_failure_stops_before_next_candidate',
    'test_custom_run_root_becomes_the_private_workspace_safety_root',
    'test_auto_backend_uses_run_lifetime_controller_for_real_schematic_candidates',
    'test_fully_terminal_checkpoint_resume_does_not_start_maestro_controller',
    'test_unstarted_persistent_candidates_run_before_replacements',
    'test_legacy_run_persists_guardian_protocol_for_checkpoint_cleanup',
    'test_legacy_batch_settings_are_overridden_by_single_run_controller',
    'test_repeated_persistent_startup_failure_stops_before_whole_population',
    'test_stop_after_candidate_preserves_active_plan_for_resume',
    'test_invalid_legacy_batch_size_is_ignored',
    'test_run_lifetime_reuses_one_worker_across_generations_and_checkpoints_before_cleanup',
    'test_pending_journal_write_failure_preserves_raw_candidate_before_cleanup',
    'test_boundary_resource_guard_restarts_clean_epoch_and_continues',
    'test_active_resource_guard_requeues_same_candidate_without_failure',
    'test_resource_guard_restart_limit_keeps_pending_candidate_resumable',
    'test_unplanned_proven_worker_replacement_fails_without_rewarming',
    'test_unproven_session_identity_changes_fail_closed_without_rewarming',
    'test_authorized_retirement_replacement_resets_resource_baselines',
    'test_active_guard_keeps_same_session_baseline_across_cleanup',
    'test_initial_identity_pending_does_not_use_the_consecutive_failure_limit',
    'test_initial_identity_pending_has_a_bounded_startup_timeout',
    'test_proven_retirement_allows_one_exact_lazy_epoch_replacement',
    'test_active_health_sampling_is_throttled_and_does_not_kill_healthy_first_candidate',
    'test_active_disk_guard_checkpoints_first_candidate_without_restart',
    'test_identity_pending_keeps_process_tree_and_log_guards_active',
    'test_unsafe_retirement_does_not_authorize_a_replacement',
    'test_nonrecyclable_active_guard_recovers_terminal_wal_before_stopping',
    'test_controller_process_failure_is_checkpointed_then_restarted_for_replacement',
    'test_external_environment_remote_failure_stops_without_penalty_or_resample',
    'test_controller_poll_failure_recovers_durable_terminal_candidate_wal',
    'test_user_stop_terminates_active_controller_without_waiting_for_candidate',
    'test_user_stop_recovers_terminal_wal_before_saving_checkpoint',
    'test_safe_retirement_clears_cleanup_pending_after_cleanup_failure',
    'test_cleanup_exception_is_checkpointed_with_pending_state_before_abort',
    'test_unproven_cleanup_retirement_remains_pending_and_aborts',
    'test_run_lifetime_serializes_requested_parallel_jobs_through_one_controller',
    'test_run_lifetime_closes_every_worker_when_candidate_evaluation_raises',
    'test_run_lifetime_never_reports_completed_when_worker_shutdown_is_unsafe',
    'test_source_integrity_failure_aborts_before_pending_or_replacements',
    'test_candidate_timeout_is_forwarded_to_maestro',
)
for _historical_name in _HISTORICAL_NATIVE_TESTS:
    setattr(LegacyNativeRunnerTest, _historical_name, historical_native_loop(getattr(RunnerTest, _historical_name)))
    delattr(RunnerTest, _historical_name)
