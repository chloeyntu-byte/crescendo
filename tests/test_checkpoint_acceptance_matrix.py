import copy
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from crescendo_engine.continuation import (
    checkpoint_continuation_id, continuation_checkpoint_path,
    continuation_pending_path, persist_continuation_pending,
)
from crescendo_engine.runner import (
    checkpoint_candidates, checkpoint_continuation_schema,
    prepared_setup_reference, run_optimization,
)
from crescendo_engine.server import OptimizerService
from crescendo_engine.single_test import project_setup
from crescendo_engine.utils import load_json


def _prepared():
    return {
        "maestro_path": "/portable/design/TB/maestro",
        "setup": project_setup({"tests": [{"name": "mock", "enabled": True}], "corners": []}, "mock"),
        "hierarchy": {"designs": []},
        "parameters": [
            {
                "parameter_id": "x", "backend": "schematic", "optimizable": True,
                "hierarchy_path": "/DUT/M1", "name": "width",
            },
            {
                "parameter_id": "y", "backend": "schematic", "optimizable": True,
                "hierarchy_path": "/DUT/M2", "name": "length",
            },
        ],
        "outputs": [
            {"key": "mock::cost"},
            {"key": "mock::tradeoff"},
        ],
        "source_identity": {
            "schema_version": "crescendo.source_design_identity.v1",
            "libraries": [],
            "fingerprint": "checkpoint-acceptance-source",
        },
        "eda_context": {
            "cadence": {"version_checks": []},
            "pdk": {
                "ok": True,
                "pdk_name": "TEST_PDK",
                "pdk_adapter": "generic-cdf",
            },
            "submission": {"controller": "maestro", "simulator": "spectre"},
        },
    }


def _spec(algorithm, evaluator="mock", jobs=1):
    objectives = [{"output_key": "mock::cost", "direction": "minimize"}]
    if algorithm == "nsga2":
        objectives.append({"output_key": "mock::tradeoff", "direction": "minimize"})
    return {
        "evaluator": evaluator,
        "algorithm": algorithm,
        "algorithm_parameters": {"population": 6, "seed": 19},
        "generations": 3,
        "jobs": jobs,
        "candidate_timeout_seconds": 60,
        "variables": [
            {"parameter_id": "x", "minimum": 0.0, "maximum": 1.0},
            {"parameter_id": "y", "minimum": 0.0, "maximum": 1.0},
        ],
        "objectives": objectives,
        "constraints": [],
    }


def _completed_row(_run_root, index, generation, values, spec, *_unused):
    x_value = float(values["x"])
    y_value = float(values["y"])
    cost = (x_value - 0.2) ** 2 + (y_value - 0.8) ** 2
    tradeoff = (x_value - 0.8) ** 2 + (y_value - 0.2) ** 2
    objective_values = [cost]
    if len(spec.get("objectives") or []) > 1:
        objective_values.append(tradeoff)
    objectives = [
        {
            "output_key": definition["output_key"],
            "value": value,
            "score": value if definition.get("direction") == "minimize" else -value,
        }
        for definition, value in zip(spec["objectives"], objective_values)
    ]
    return {
        "candidate_id": "g%03d_c%04d" % (generation, index),
        "generation": generation,
        "index": index,
        "values": dict(values),
        "status": "completed",
        "metrics": {
            "objectives": objectives,
            "constraints": [],
            "feasible": True,
            "total_violation": 0.0,
            "merit": sum(row["score"] for row in objectives),
        },
    }


def _runtime_config(root, persistent=False):
    runtime = {
        "run_root": root,
        "max_parallel_jobs": 4 if persistent else 1,
        "candidate_timeout_seconds": 60,
        "persistent_min_free_disk_mib": 1,
    }
    if persistent:
        runtime.update({
            "execution_backend": "auto",
            "persistent_max_workers": 4,
            "persistent_worker_lifetime": "batch",
            "infrastructure_retries": 0,
        })
    return {"runtime": runtime}


def _fake_controller(evaluator, instances):
    class FakeController(object):
        def __init__(self, run_root, worker_index, *_unused):
            self.run_root = run_root
            self.worker_index = worker_index
            self.evaluated = []
            self.closed = 0
            instances.append(self)

        def start(self):
            return self

        def submit_evaluate(self, index, generation, values, spec):
            return {
                "index": index, "generation": generation,
                "values": values, "spec": spec,
            }

        def poll(self, request):
            self.evaluated.append((request["generation"], request["index"]))
            return evaluator(
                self.run_root, request["index"], request["generation"],
                request["values"], request["spec"], None, None, None,
            )

        def commit_candidate(self, _candidate_id):
            return {"ok": True, "state": "cleaned", "recycled": False}

        def terminate_and_prove(self, _reason):
            return {
                "termination_proven": True,
                "process_still_alive": False,
                "cleanup": {"ok": True, "removed": True},
            }

        def restart(self):
            return self

        def close(self):
            self.closed += 1
            return {
                "reason": "closed",
                "termination_proven": True,
                "process_still_alive": False,
                "cleanup": {"ok": True, "removed": True},
            }

    return FakeController


class CheckpointAcceptanceMatrixTest(unittest.TestCase):
    def test_all_algorithms_resume_mid_generation_from_one_exported_file(self):
        for algorithm in ("nsga2", "differential_evolution", "bayesian_optimization"):
            with self.subTest(algorithm=algorithm), tempfile.TemporaryDirectory() as root:
                prepared = _prepared()
                spec = _spec(algorithm)

                baseline_runtime = os.path.join(root, "baseline-runtime")
                baseline_root = os.path.join(baseline_runtime, "baseline")
                with mock.patch(
                    "crescendo_engine.runner._evaluate_candidate",
                    side_effect=_completed_row,
                ):
                    baseline_result = run_optimization(
                        spec, _runtime_config(baseline_runtime),
                        prepared=prepared, run_root=baseline_root,
                    )
                baseline_checkpoint = load_json(os.path.join(baseline_root, "checkpoint.json"))

                source_runtime = os.path.join(root, "source-runtime")
                source_root = os.path.join(source_runtime, "interrupted")

                def interrupted(*args):
                    if args[2] == 1 and args[1] == 2:
                        raise RuntimeError("simulated process loss")
                    return _completed_row(*args)

                with mock.patch(
                    "crescendo_engine.runner._evaluate_candidate",
                    side_effect=interrupted,
                ):
                    with self.assertRaisesRegex(RuntimeError, "simulated process loss"):
                        run_optimization(
                            spec, _runtime_config(source_runtime),
                            prepared=prepared, run_root=source_root,
                        )

                source_service = OptimizerService(_runtime_config(source_runtime))
                exported = source_service.export_checkpoint("interrupted")
                exported_rows = checkpoint_candidates(exported)
                self.assertEqual(len(exported_rows), 8)
                self.assertEqual(
                    [(row["generation"], row["index"]) for row in exported_rows[-2:]],
                    [(1, 0), (1, 1)],
                )
                self.assertEqual(exported["state"]["next_generation"], 1)
                self.assertEqual(exported["state"]["active_plan"]["generation"], 1)
                self.assertIsNotNone(exported["state"]["generator"]["rng_state"])
                if algorithm == "differential_evolution":
                    self.assertTrue(exported["state"]["generator"]["de_targets"])

                continuation_id = checkpoint_continuation_id(exported)
                self.assertTrue(os.path.isfile(continuation_checkpoint_path(
                    source_runtime, continuation_id, create=False,
                )))
                os.makedirs(os.path.join(source_root, "raw"))
                os.makedirs(os.path.join(source_root, "history"))
                os.makedirs(os.path.join(source_root, "workspace"))
                export_path = os.path.join(root, "%s.checkpoint.json" % algorithm)
                with open(export_path, "w") as stream:
                    json.dump(exported, stream)

                shutil.rmtree(source_runtime)
                self.assertFalse(os.path.exists(source_runtime))
                with open(export_path) as stream:
                    one_file_checkpoint = json.load(stream)

                destination_runtime = os.path.join(root, "destination-runtime")
                self.assertFalse(os.path.lexists(continuation_checkpoint_path(
                    destination_runtime, continuation_id, create=False,
                )))
                self.assertFalse(os.path.lexists(continuation_pending_path(
                    destination_runtime, continuation_id, create=False,
                )))
                destination_service = OptimizerService(_runtime_config(destination_runtime))
                with mock.patch.object(
                    destination_service, "prepare", return_value=("prepared", prepared),
                ), mock.patch(
                    "crescendo_engine.runner._evaluate_candidate",
                    side_effect=_completed_row,
                ):
                    launched = destination_service.resume(one_file_checkpoint)
                    terminal = destination_service.wait_for_terminal(launched["run_id"])

                self.assertEqual(terminal["status"]["state"], "completed")
                self.assertIsNone(terminal["server_error"])
                resumed_checkpoint = load_json(os.path.join(
                    terminal["run_root"], "checkpoint.json",
                ))
                self.assertEqual(
                    resumed_checkpoint["candidates"], baseline_checkpoint["candidates"],
                )
                self.assertEqual(
                    resumed_checkpoint["state"]["generator"],
                    baseline_checkpoint["state"]["generator"],
                )
                self.assertEqual(
                    resumed_checkpoint["state"]["selection_history"],
                    baseline_checkpoint["state"]["selection_history"],
                )
                for key in (
                    "next_generation", "terminal_state", "terminal_reason",
                    "completed_candidates", "failed_candidates",
                    "infrastructure_failed_candidates",
                ):
                    self.assertEqual(
                        resumed_checkpoint["state"].get(key),
                        baseline_checkpoint["state"].get(key),
                    )
                self.assertEqual(
                    [row["candidate_id"] for row in terminal["result"]["candidates"]],
                    [row["candidate_id"] for row in baseline_result["candidates"]],
                )

    def test_path_override_rebases_one_file_resume_and_saves_exactly_once(self):
        with tempfile.TemporaryDirectory() as root:
            source_runtime = os.path.join(root, "source-runtime")
            source_root = os.path.join(source_runtime, "interrupted")
            old_maestro_path = os.path.join(
                source_runtime, "old-design", "TB", "maestro",
            )
            os.makedirs(os.path.dirname(old_maestro_path))
            with open(old_maestro_path, "w") as stream:
                stream.write("old setup\n")

            source_prepared = copy.deepcopy(_prepared())
            source_prepared["maestro_path"] = old_maestro_path
            spec = _spec("random")
            spec["maestro_path"] = old_maestro_path
            legacy_reference = prepared_setup_reference(
                prepared=source_prepared, spec=spec,
            )
            self.assertEqual(legacy_reference["backend"], "maestro")
            self.assertEqual(legacy_reference["path"], old_maestro_path)
            spec["algorithm_parameters"] = {"population": 1, "seed": 19}
            spec["generations"] = 2
            source_calls = []

            def stop_after_first(*args):
                source_calls.append((args[2], args[1]))
                row = _completed_row(*args)
                with open(os.path.join(args[0], "STOP"), "w") as stream:
                    stream.write("stop\n")
                return row

            with mock.patch(
                "crescendo_engine.runner._evaluate_candidate",
                side_effect=stop_after_first,
            ):
                source_result = run_optimization(
                    spec, _runtime_config(source_runtime),
                    prepared=source_prepared, run_root=source_root,
                )

            self.assertEqual(source_result["state"], "stopped")
            self.assertEqual(source_calls, [(0, 0)])
            source_service = OptimizerService(_runtime_config(source_runtime))
            exported = source_service.export_checkpoint("interrupted")
            exported_revision = exported["state"]["checkpoint_revision"]
            exported_checksum = exported["payload_sha256"]
            continuation_id = checkpoint_continuation_id(exported)
            source_anchor = continuation_checkpoint_path(
                source_runtime, continuation_id, create=False,
            )
            source_pending = continuation_pending_path(
                source_runtime, continuation_id, create=False,
            )
            self.assertTrue(os.path.isfile(source_anchor))

            export_path = os.path.join(root, "portable.checkpoint.json")
            with open(export_path, "w") as stream:
                json.dump(exported, stream)
            shutil.rmtree(source_runtime)
            self.assertFalse(os.path.exists(source_runtime))
            self.assertFalse(os.path.exists(old_maestro_path))
            self.assertFalse(os.path.lexists(source_anchor))
            self.assertFalse(os.path.lexists(source_pending))
            with open(export_path) as stream:
                one_file_checkpoint = json.load(stream)

            new_maestro_path = os.path.join(
                root, "relocated-design", "TB", "maestro",
            )
            os.makedirs(os.path.dirname(new_maestro_path))
            with open(new_maestro_path, "w") as stream:
                stream.write("relocated setup\n")
            destination_prepared = copy.deepcopy(source_prepared)
            destination_prepared["setup_locator"] = prepared_setup_reference(
                prepared={"maestro_path": new_maestro_path},
            )
            destination_prepared["setup_path"] = new_maestro_path
            destination_prepared["maestro_path"] = new_maestro_path
            relocated_schema_probe = copy.deepcopy(one_file_checkpoint)
            relocated_schema_probe["manifest"]["maestro_path"] = new_maestro_path
            relocated_schema_probe["spec"]["maestro_path"] = new_maestro_path
            relocated_schema_probe["manifest"]["setup_locator"]["path"] = new_maestro_path
            relocated_schema_probe["spec"]["setup_locator"]["path"] = new_maestro_path
            self.assertEqual(
                checkpoint_continuation_schema(one_file_checkpoint),
                checkpoint_continuation_schema(relocated_schema_probe),
            )
            changed_source = copy.deepcopy(relocated_schema_probe)
            changed_source["manifest"]["source_design_identity"]["fingerprint"] = (
                "different-source"
            )
            self.assertNotEqual(
                checkpoint_continuation_schema(one_file_checkpoint),
                checkpoint_continuation_schema(changed_source),
            )
            changed_eda = copy.deepcopy(relocated_schema_probe)
            changed_eda["manifest"]["eda_context_identity"]["fingerprint"] = (
                "different-eda"
            )
            self.assertNotEqual(
                checkpoint_continuation_schema(one_file_checkpoint),
                checkpoint_continuation_schema(changed_eda),
            )
            destination_runtime = os.path.join(root, "destination-runtime")
            destination_anchor = continuation_checkpoint_path(
                destination_runtime, continuation_id, create=False,
            )
            destination_pending = continuation_pending_path(
                destination_runtime, continuation_id, create=False,
            )
            self.assertFalse(os.path.lexists(destination_anchor))
            self.assertFalse(os.path.lexists(destination_pending))

            destination_service = OptimizerService(
                _runtime_config(destination_runtime),
            )
            resume_calls = []

            def complete_remainder(*args):
                resume_calls.append((args[2], args[1]))
                return _completed_row(*args)

            with mock.patch.object(
                destination_service, "prepare",
                return_value=("prepared", destination_prepared),
            ) as prepare_mock, mock.patch(
                "crescendo_engine.runner._evaluate_candidate",
                side_effect=complete_remainder,
            ), mock.patch(
                "crescendo_engine.runner.persist_continuation_pending",
                wraps=persist_continuation_pending,
            ) as pending_writes:
                launched = destination_service.resume(
                    one_file_checkpoint, path=new_maestro_path,
                )
                terminal = destination_service.wait_for_terminal(launched["run_id"])

            prepare_mock.assert_called_once_with(new_maestro_path, initialize_validation=False)
            self.assertEqual(terminal["status"]["state"], "completed")
            self.assertIsNone(terminal["server_error"])
            self.assertEqual(resume_calls, [(1, 0)])
            self.assertGreaterEqual(pending_writes.call_count, 2)
            pending_checkpoints = [
                call[0][2]["checkpoint"] for call in pending_writes.call_args_list
            ]
            pending_revisions = [
                row["state"]["checkpoint_revision"] for row in pending_checkpoints
            ]
            self.assertEqual(pending_revisions[0], exported_revision + 1)
            self.assertEqual(pending_revisions, sorted(set(pending_revisions)))
            for row in pending_checkpoints:
                self.assertEqual(
                    row["manifest"]["setup_locator"]["path"], new_maestro_path,
                )
                self.assertEqual(
                    row["manifest"]["setup_locator"]["backend"], "maestro",
                )

            resumed_checkpoint = load_json(os.path.join(
                terminal["run_root"], "checkpoint.json",
            ))
            self.assertEqual(
                resumed_checkpoint["manifest"]["setup_locator"]["path"], new_maestro_path,
            )
            self.assertEqual(
                resumed_checkpoint["manifest"]["setup_locator"]["backend"], "maestro",
            )
            self.assertEqual(
                resumed_checkpoint["manifest"]["maestro_path"], new_maestro_path,
            )
            self.assertEqual(
                resumed_checkpoint["spec"]["setup_locator"]["path"], new_maestro_path,
            )
            self.assertEqual(
                resumed_checkpoint["spec"]["setup_locator"]["backend"], "maestro",
            )
            self.assertEqual(
                resumed_checkpoint["spec"]["maestro_path"], new_maestro_path,
            )
            self.assertGreater(
                resumed_checkpoint["state"]["checkpoint_revision"],
                exported_revision + 1,
            )
            self.assertIn(
                exported_checksum,
                resumed_checkpoint["state"]["ancestor_payload_sha256"],
            )
            self.assertEqual(
                [row["candidate_id"] for row in checkpoint_candidates(resumed_checkpoint)],
                ["g000_c0000", "g001_c0000"],
            )
            self.assertEqual(
                len(set(
                    row["candidate_id"]
                    for row in checkpoint_candidates(resumed_checkpoint)
                )),
                2,
            )
            self.assertEqual(load_json(destination_anchor), resumed_checkpoint)
            self.assertFalse(os.path.lexists(destination_pending))

    def test_real_backend_resume_clamps_jobs_and_worker_lifetime_to_one_maestro(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = _prepared()
            spec = _spec("differential_evolution", evaluator="real", jobs=4)
            source_runtime = os.path.join(root, "source-runtime")
            source_root = os.path.join(source_runtime, "interrupted")
            source_instances = []

            def stop_mid_generation(*args):
                row = _completed_row(*args)
                if args[1] == 1 and args[2] == 0:
                    with open(os.path.join(args[0], "STOP"), "w") as stream:
                        stream.write("stop\n")
                return row

            with mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_controller(stop_mid_generation, source_instances),
            ):
                source_result = run_optimization(
                    spec, _runtime_config(source_runtime, persistent=True),
                    prepared=prepared, run_root=source_root,
                )
            self.assertEqual(source_result["state"], "stopped")
            self.assertEqual(len(source_instances), 1)
            self.assertEqual(source_instances[0].evaluated, [(0, 0), (0, 1)])
            source_status = load_json(os.path.join(source_root, "status.json"))
            self.assertEqual(source_status["requested_parallel_jobs"], 4)
            self.assertEqual(source_status["effective_parallel_jobs"], 1)
            self.assertEqual(source_status["persistent_worker_count"], 1)
            self.assertEqual(source_status["persistent_worker_lifetime"], "optimization_run")

            exported = OptimizerService(
                _runtime_config(source_runtime, persistent=True),
            ).export_checkpoint("interrupted")
            export_path = os.path.join(root, "persistent.checkpoint.json")
            with open(export_path, "w") as stream:
                json.dump(exported, stream)
            shutil.rmtree(source_runtime)
            with open(export_path) as stream:
                one_file_checkpoint = json.load(stream)

            destination_runtime = os.path.join(root, "destination-runtime")
            destination_service = OptimizerService(
                _runtime_config(destination_runtime, persistent=True),
            )
            destination_instances = []
            with mock.patch.object(
                destination_service, "prepare", return_value=("prepared", prepared),
            ), mock.patch(
                "crescendo_engine.runner.PersistentControllerProxy",
                _fake_controller(_completed_row, destination_instances),
            ):
                launched = destination_service.resume(one_file_checkpoint)
                terminal = destination_service.wait_for_terminal(launched["run_id"])

            self.assertEqual(terminal["status"]["state"], "completed")
            self.assertIsNone(terminal["server_error"])
            self.assertEqual(len(destination_instances), 1)
            self.assertEqual(destination_instances[0].worker_index, 0)
            self.assertEqual(destination_instances[0].closed, 1)
            resumed_status = terminal["status"]
            self.assertEqual(resumed_status["requested_parallel_jobs"], 4)
            self.assertEqual(resumed_status["effective_parallel_jobs"], 1)
            self.assertEqual(resumed_status["persistent_worker_count"], 1)
            self.assertEqual(
                resumed_status["parallel_jobs_override"],
                {
                    "requested": 4,
                    "effective": 1,
                    "reason": "real EDA uses one run-lifetime Maestro session",
                },
            )
            self.assertEqual(
                resumed_status["worker_lifetime_override"]["effective"], "run",
            )
            self.assertEqual(len(terminal["result"]["candidates"]), 18)


if __name__ == "__main__":
    unittest.main()


# Historical v0.3.0 native-ADE scheduling, never the current shared CLI route.
from tests.legacy_native_fixture import historical_native_loop


class LegacyNativeCheckpointAcceptanceMatrixTest(unittest.TestCase):
    """Retired native scheduling regression assertions."""


_HISTORICAL_NATIVE_TESTS = (
    'test_real_backend_resume_clamps_jobs_and_worker_lifetime_to_one_maestro',
)
for _historical_name in _HISTORICAL_NATIVE_TESTS:
    setattr(LegacyNativeCheckpointAcceptanceMatrixTest, _historical_name, historical_native_loop(getattr(CheckpointAcceptanceMatrixTest, _historical_name)))
    delattr(CheckpointAcceptanceMatrixTest, _historical_name)
