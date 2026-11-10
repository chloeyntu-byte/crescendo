import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.runner import (
    ExternalEdaEnvironmentChanged, _WholeRunPersistentWorker,
    _evaluate_persistent_candidate, run_optimization,
)
from crescendo_engine.persistent_controller import _worker_epoch_roots
from crescendo_engine.run_data_store import load_dataset
from tests.test_runner import _write_terminal_wal


def _post_done_validation_case(root, spectre_after, infrastructure_retries,
                               cleanup_proven=True, retry_succeeds=False,
                               completion_succeeds=False):
    evaluated_ids = []

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
                "history_name": "history-%s" % self.epoch,
                "history_identity": {"name": "history-%s" % self.epoch},
                "rdb_path": os.path.join(root, "history-%s.rdb" % self.epoch),
                "started_epoch": 1.0, "finished_epoch": 2.0,
            }

        def is_alive(self):
            return True

        def require_candidate_time(self, _artifact, _phase):
            return None

    class FakeWorker(object):
        worker_index = 0

        def __init__(self, config):
            self.config = config
            self.controller = None
            self.workspace = None
            self.start_count = 0
            self.derived_parameter_cache = {}
            self.pending_artifact = None
            self.retire_after_cleanup = False
            self.commit_count = 0
            self.retire_count = 0

        def ensure_started(self):
            if self.controller is None:
                self.start_count += 1
                self.controller = Controller("epoch-%d" % self.start_count)
                self.workspace = {"root": root, "setup": {"tests": []}}
            return {"started": True, "epoch": self.controller.epoch}

        def set_pending(self, artifact, retire_after_cleanup=False):
            self.pending_artifact = artifact
            self.retire_after_cleanup = retire_after_cleanup

        def commit_candidate(self, candidate_id):
            self.commit_count += 1
            if not self.pending_artifact or self.pending_artifact["candidate_id"] != candidate_id:
                raise AssertionError("candidate cleanup fence mismatch")
            if cleanup_proven:
                retirement = {
                    "termination_proven": True, "process_still_alive": False,
                    "cleanup": {"ok": True}, "source_guard": {"ok": True},
                }
                self.controller = None
                self.workspace = None
                self.pending_artifact = None
                self.retire_after_cleanup = False
                return {
                    "ok": True, "state": "cleaned", "recycled": True,
                    "retirement": retirement,
                }
            return {
                "ok": False, "state": "cleanup_failed", "recycled": False,
                "retirement": {
                    "termination_proven": False, "process_still_alive": True,
                    "cleanup": {"ok": False}, "source_guard": {"ok": True},
                },
            }

        def retire(self, _reason):
            self.retire_count += 1
            self.controller = None
            self.workspace = None
            return {
                "termination_proven": True, "process_still_alive": False,
                "cleanup": {"ok": True}, "source_guard": {"ok": True},
            }

        def health(self):
            return {"alive": self.controller is not None}

    empty = {"run_count": 0, "completed_run_count": 0, "runs": []}
    summaries = [empty, spectre_after]
    completion_results = [
        {"ok": True}
        if completion_succeeds
        else RuntimeError("simulator failed to complete")
    ]
    if retry_succeeds:
        summaries.extend([
            empty,
            {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 0, "elapsed_seconds": 0.2}],
            },
        ])
        completion_results.append({"ok": True})
    spec = {
        "candidate_timeout_seconds": 60,
        "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
        "constraints": [],
    }
    config = {"runtime": {
        "candidate_timeout_seconds": 60,
        "infrastructure_retries": infrastructure_retries,
    }}
    worker = FakeWorker(config)
    with mock.patch(
        "crescendo_engine.runner.summarize_spectre_timing", side_effect=summaries,
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
        side_effect=completion_results,
    ) as completion, mock.patch(
        "crescendo_engine.runner.evaluate_metrics",
        return_value={
            "objectives": [{"value": 1.0, "score": -1.0}],
            "constraints": [], "feasible": True,
            "total_violation": 0.0, "merit": -1.0,
        },
    ) as metrics, mock.patch(
        "crescendo_engine.runner.verify_sources_unchanged", return_value={"ok": True},
    ):
        row = _evaluate_persistent_candidate(
            worker, root, 0, 0, {"x": 0.5}, spec,
            {"setup": {"tests": []}}, [], config,
        )
    for call in completion.call_args_list:
        if "config" in call[1] and call[1]["config"] is not worker.config:
            raise AssertionError("completion must use the owned worker configuration")
    worker.metrics_call_count = metrics.call_count
    return row, worker, evaluated_ids, completion


class RunnerFatalEdgeTest(unittest.TestCase):
    def test_replacement_controller_namespace_prevents_worker_epoch_reuse(self):
        with tempfile.TemporaryDirectory() as root:
            class FakeMaestroWorker(object):
                def __init__(self, _workspace, _config, _selected, _epoch_root,
                             worker_index=None, epoch=None):
                    self.epoch = epoch

                def start(self):
                    return self

            def fake_workspace(_setup, _hierarchy, _config, workspace_root,
                               expected_source_identity=None, parameters=None):
                os.makedirs(workspace_root)
                return {"root": workspace_root, "build_timing": {}}

            with mock.patch(
                "crescendo_engine.runner.build_workspace", side_effect=fake_workspace,
            ) as build_workspace, mock.patch(
                "crescendo_engine.runner.PersistentMaestroWorker", FakeMaestroWorker,
            ):
                original = _WholeRunPersistentWorker(
                    root, 0, {"setup": {}, "hierarchy": {}}, [], {},
                    epoch_namespace=0,
                )
                replacement = _WholeRunPersistentWorker(
                    root, 0, {"setup": {}, "hierarchy": {}}, [], {},
                    epoch_namespace=1,
                )
                first = original._new_epoch()
                second = replacement._new_epoch()
            self.assertIs(build_workspace.call_args_list[0][1]["parameters"], original.selected)
            self.assertIs(build_workspace.call_args_list[1][1]["parameters"], replacement.selected)
            self.assertEqual(first["epoch"], "worker-000-epoch-000")
            self.assertEqual(
                second["epoch"], "worker-000-controller-000001-epoch-000000",
            )
            self.assertNotEqual(original.epoch_root, replacement.epoch_root)
            self.assertEqual(
                [os.path.basename(path) for path in _worker_epoch_roots(root, 0)],
                ["epoch_000", "epoch_c000001_l000000"],
            )

    def test_commit_retirement_without_source_guard_reverifies_prepared_identity(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {
                "setup": {"tests": [], "corners": []},
                "hierarchy": {"designs": []},
                "parameters": [{
                    "parameter_id": "x", "backend": "schematic", "optimizable": True,
                }],
                "outputs": [{"key": "main::gain"}],
                "source_identity": {
                    "schema_version": "crescendo.source_design_identity.v1",
                    "fingerprint": "expected-fingerprint",
                },
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
                        "candidate_id": "g%03d_c%04d" % (
                            request["generation"], request["index"],
                        ),
                        "generation": request["generation"], "index": request["index"],
                        "values": values, "status": "completed",
                        "source_guard": {"ok": True, "libraries": []},
                        "persistent_worker": {
                            "worker_index": self.worker_index, "cleanup_pending": True,
                        },
                        "metrics": {
                            "objectives": [{"score": -values["x"], "value": values["x"]}],
                            "constraints": [], "feasible": True,
                            "total_violation": 0.0, "merit": -values["x"],
                        },
                    }
                    _write_terminal_wal(self.run_root, row)
                    return row

                def commit_candidate(self, _candidate_id):
                    return {
                        "ok": False, "state": "cleanup_failed_recycled",
                        "retirement": {
                            "termination_proven": True, "process_still_alive": False,
                            "cleanup": {"ok": True, "removed": True},
                        },
                    }

                def close(self):
                    return {
                        "reason": "closed", "termination_proven": True,
                        "process_still_alive": False,
                        "cleanup": {"ok": True, "removed": True},
                    }

            run_root = os.path.join(root, "run")
            verified_guard = {
                "ok": True, "before": prepared["source_identity"],
                "after": prepared["source_identity"],
            }
            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy", FakeController,
            ), mock.patch(
                "crescendo_engine.runner._reverify_prepared_source_identity",
                return_value=verified_guard,
            ) as reverify:
                result = run_optimization(
                    spec, config, prepared=prepared, run_root=run_root,
                )
            self.assertEqual(result["state"], "completed")
            reverify.assert_called_once()
            verified_prepared, verified_config = reverify.call_args[0]
            self.assertIs(verified_prepared, prepared)
            self.assertEqual(verified_config["runtime"]["run_root"], run_root)
            persisted_rows = load_dataset(os.path.join(run_root, "optimization.crescendo"))["candidates"]
            self.assertEqual([row["candidate_id"] for row in persisted_rows], ["g000_c0000"])
            persisted = persisted_rows[0]
            self.assertEqual(persisted["status"], "completed")
            self.assertFalse(persisted["persistent_worker"]["cleanup_pending"])
            self.assertEqual(
                persisted["persistent_worker"]["cleanup"]["retirement"]["source_guard"],
                verified_guard,
            )

    def test_unclosed_spectre_start_after_done_retires_and_retries_same_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            row, worker, evaluated_ids, completion = _post_done_validation_case(
                root,
                {"run_count": 1, "completed_run_count": 0, "runs": []},
                infrastructure_retries=1, retry_succeeds=True,
            )
            self.assertEqual(row["status"], "completed")
            self.assertEqual(worker.start_count, 2)
            self.assertEqual(evaluated_ids, ["g000_c0000", "g000_c0000"])
            self.assertEqual(completion.call_count, 2)
            self.assertEqual(
                row["persistent_worker"]["attempts"][0]["infrastructure_evidence"],
                [{"kind": "spectre_submit_process_died", "count": 1}],
            )
            self.assertTrue(
                row["persistent_worker"]["attempts"][0]["cleanup"]["recycled"]
            )

    def test_post_done_ordinary_spectre_rc1_is_design_failure_without_retirement(self):
        with tempfile.TemporaryDirectory() as root:
            row, worker, evaluated_ids, completion = _post_done_validation_case(
                root,
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 1, "elapsed_seconds": 0.1}],
                },
                infrastructure_retries=3,
            )
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["failure_class"], "design_or_simulation")
            self.assertEqual(worker.start_count, 1)
            self.assertEqual(evaluated_ids, ["g000_c0000"])
            self.assertEqual(completion.call_count, 1)
            self.assertEqual(worker.commit_count, 0)
            self.assertEqual(worker.retire_count, 0)
            self.assertIsNotNone(worker.pending_artifact)
            self.assertFalse(worker.retire_after_cleanup)
            self.assertEqual(
                row["persistent_worker"]["attempts"][0]["infrastructure_evidence"],
                [],
            )

    def test_fatal_post_done_evidence_without_cleanup_proof_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            row, worker, evaluated_ids, completion = _post_done_validation_case(
                root,
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 137, "elapsed_seconds": 0.1}],
                },
                infrastructure_retries=2, cleanup_proven=False,
            )
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "simulation_infrastructure")
            self.assertNotIn("metrics", row)
            self.assertEqual(worker.start_count, 1)
            self.assertEqual(evaluated_ids, ["g000_c0000"])
            self.assertEqual(completion.call_count, 1)
            self.assertEqual(worker.commit_count, 1)
            self.assertEqual(worker.retire_count, 0)
            self.assertIsNotNone(worker.pending_artifact)
            first_attempt = row["persistent_worker"]["attempts"][0]
            self.assertFalse(first_attempt["cleanup"]["ok"])
            self.assertFalse(
                first_attempt["cleanup"]["retirement"]["termination_proven"]
            )

    def test_fatal_post_done_evidence_with_zero_retry_budget_is_infrastructure_failed(self):
        with tempfile.TemporaryDirectory() as root:
            row, worker, evaluated_ids, completion = _post_done_validation_case(
                root,
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 137, "elapsed_seconds": 0.1}],
                },
                infrastructure_retries=0,
            )
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertEqual(row["failure_class"], "simulation_infrastructure")
            self.assertNotIn("metrics", row)
            self.assertEqual(worker.start_count, 1)
            self.assertEqual(evaluated_ids, ["g000_c0000"])
            self.assertEqual(completion.call_count, 1)
            self.assertEqual(worker.commit_count, 1)
            self.assertIsNone(worker.controller)
            self.assertEqual(
                row["persistent_worker"]["attempts"][0]["infrastructure_evidence"],
                [{"kind": "spectre_submit_returncode", "returncode": 137}],
            )

    def test_successful_completion_and_metrics_cannot_mask_fatal_post_done_submit_evidence(self):
        cases = [
            (
                "timeout",
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 124, "elapsed_seconds": 0.1}],
                },
                [{"kind": "spectre_submit_returncode", "returncode": 124}],
            ),
            (
                "sigkill",
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 137, "elapsed_seconds": 0.1}],
                },
                [{"kind": "spectre_submit_returncode", "returncode": 137}],
            ),
            (
                "sigsegv",
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 139, "elapsed_seconds": 0.1}],
                },
                [{"kind": "spectre_submit_returncode", "returncode": 139}],
            ),
            (
                "unclosed_start",
                {"run_count": 1, "completed_run_count": 0, "runs": []},
                [{"kind": "spectre_submit_process_died", "count": 1}],
            ),
        ]
        for label, spectre_after, expected_evidence in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as root:
                row, worker, evaluated_ids, completion = _post_done_validation_case(
                    root, spectre_after, infrastructure_retries=0,
                    completion_succeeds=True,
                )
                self.assertEqual(row["status"], "infrastructure_failed")
                self.assertEqual(row["failure_class"], "simulation_infrastructure")
                self.assertNotIn("metrics", row)
                self.assertEqual(completion.call_count, 1)
                self.assertEqual(worker.metrics_call_count, 1)
                self.assertEqual(evaluated_ids, ["g000_c0000"])
                self.assertEqual(worker.commit_count, 1)
                self.assertIsNone(worker.pending_artifact)
                attempt = row["persistent_worker"]["attempts"][0]
                self.assertEqual(attempt["states"][-1], "DONE")
                self.assertEqual(attempt["infrastructure_evidence"], expected_evidence)

    def test_post_done_fatal_after_successful_validation_retries_only_after_proven_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            row, worker, evaluated_ids, completion = _post_done_validation_case(
                root,
                {
                    "run_count": 1, "completed_run_count": 1,
                    "runs": [{"returncode": 137, "elapsed_seconds": 0.1}],
                },
                infrastructure_retries=1, retry_succeeds=True,
                completion_succeeds=True,
            )
            self.assertEqual(row["status"], "completed")
            self.assertEqual(evaluated_ids, ["g000_c0000", "g000_c0000"])
            self.assertEqual(completion.call_count, 2)
            self.assertEqual(worker.metrics_call_count, 2)
            self.assertEqual(worker.commit_count, 1)
            first = row["persistent_worker"]["attempts"][0]
            self.assertTrue(first["cleanup"]["ok"])
            self.assertTrue(first["cleanup"]["recycled"])
            self.assertEqual(
                first["infrastructure_evidence"],
                [{"kind": "spectre_submit_returncode", "returncode": 137}],
            )
            self.assertEqual(
                row["persistent_worker"]["attempts"][1]["infrastructure_evidence"],
                [],
            )

    def test_external_environment_drift_during_candidate_is_fatal_before_completed(self):
        with tempfile.TemporaryDirectory() as root:
            tracked = os.path.join(root, "external_eda_file")
            with open(tracked, "w") as stream:
                stream.write("original")
            events = []

            def boundary_guard(_generation):
                events.append("environment_boundary")
                with open(tracked, "r") as stream:
                    if stream.read() != "original":
                        raise ExternalEdaEnvironmentChanged(
                            "external EDA environment changed during candidate evaluation"
                        )
                return {"ok": True}

            def completion(*_args, **_kwargs):
                events.append("completion")
                return {"ok": True}

            def metrics(*_args, **_kwargs):
                events.append("metrics")
                with open(tracked, "w") as stream:
                    stream.write("modified")
                return {
                    "objectives": [{"value": 1.0, "score": -1.0}],
                    "constraints": [], "feasible": True,
                    "total_violation": 0.0, "merit": -1.0,
                }

            def source_guard(_workspace):
                events.append("source_guard")
                return {"ok": True}

            controller = mock.Mock()
            controller.epoch = "epoch-1"
            controller.spectre_log = os.path.join(root, "spectre.log")
            controller.pending_cleanup = {"candidate_id": "g000_c0000"}
            controller.evaluate.return_value = {
                "candidate_id": "g000_c0000", "candidate_root": root,
                "states": [
                    "ACCEPTED", "BEGIN", "PATCHED", "SIM_DONE",
                    "RESULTS_READY", "DONE",
                ],
                "history_name": "history", "history_identity": {"name": "history"},
                "rdb_path": os.path.join(root, "history.rdb"),
                "started_epoch": 1.0, "finished_epoch": 2.0,
            }
            controller.is_alive.return_value = True
            worker = mock.Mock(spec=_WholeRunPersistentWorker)
            worker._native_stop_requested = False
            worker.worker_index = 0
            worker.start_count = 1
            worker.controller = controller
            worker.workspace = {"root": root}
            worker.derived_parameter_cache = {}
            worker.pending_artifact = None
            worker.ensure_started.return_value = {"started": False, "epoch": "epoch-1"}
            worker.verify_external_environment_boundary.side_effect = boundary_guard
            empty = {"run_count": 0, "completed_run_count": 0, "runs": []}
            completed = {
                "run_count": 1, "completed_run_count": 1,
                "runs": [{"returncode": 0, "elapsed_seconds": 0.1}],
            }
            spec = {
                "candidate_timeout_seconds": 60,
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": [],
            }
            config = {"runtime": {
                "candidate_timeout_seconds": 60, "infrastructure_retries": 0,
            }}
            worker.config = config
            with mock.patch(
                "crescendo_engine.runner.summarize_spectre_timing",
                side_effect=[empty, completed],
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
                side_effect=completion,
            ), mock.patch(
                "crescendo_engine.runner.evaluate_metrics", side_effect=metrics,
            ), mock.patch(
                "crescendo_engine.runner.verify_sources_unchanged",
                side_effect=source_guard,
            ):
                with self.assertRaisesRegex(
                    ExternalEdaEnvironmentChanged, "changed during candidate evaluation",
                ):
                    _evaluate_persistent_candidate(
                        worker, root, 0, 0, {"x": 0.5}, spec,
                        {"setup": {"tests": []}}, [], config,
                    )
            self.assertEqual(events, [
                "environment_boundary", "completion", "metrics", "source_guard",
                "environment_boundary",
            ])
            self.assertEqual(worker.verify_external_environment_boundary.call_count, 2)
            worker.set_pending.assert_not_called()
            worker.commit_candidate.assert_not_called()
            worker.retire.assert_not_called()


if __name__ == "__main__":
    unittest.main()


# Retired native-ADE whole-loop contracts use an exact v0.3.0 test fixture.
# Current helpers and ordinary optimizer/mock tests above still use production code.
from tests.legacy_native_fixture import historical_native_loop


class LegacyNativeRunnerFatalEdgeTest(unittest.TestCase):
    """Historical native scheduling assertions, excluded from new-pipeline evidence."""


_HISTORICAL_NATIVE_TESTS = (
    'test_commit_retirement_without_source_guard_reverifies_prepared_identity',
)
for _historical_name in _HISTORICAL_NATIVE_TESTS:
    setattr(LegacyNativeRunnerFatalEdgeTest, _historical_name, historical_native_loop(getattr(RunnerFatalEdgeTest, _historical_name)))
    delattr(RunnerFatalEdgeTest, _historical_name)
