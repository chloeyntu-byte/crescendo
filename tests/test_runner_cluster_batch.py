"""Real runner/checkpoint scheduling with an isolated, simulated native session."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.job_policy import job_policy_fingerprint
from crescendo_engine.optimizers import PopulationGenerator
from crescendo_engine.persistent_controller import (
    PersistentControllerProcessError, PersistentControllerProxy,
)
from crescendo_engine.retention import UnsafeCleanupError, safe_delete_worker_epoch
from crescendo_engine.runner import (
    _checkpoint_checksum, checkpoint_candidates, run_optimization, validate_checkpoint,
    validate_checkpoint_integrity,
)
from crescendo_engine.utils import atomic_write_json, load_json
from tests.test_runner import (
    _proven_controller_shutdown, _proven_session_health, _write_terminal_wal,
)


class RunnerClusterBatchTests(unittest.TestCase):
    def _inputs(self, root, jobs=2, limit=0, generations=1, overrides=None):
        prepared = {
            "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
            "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
            "outputs": [{"key": "main::gain"}],
            "job_policy": {"schema_version": "crescendo.maestro_job_policy.v1",
                           "source": "invoking_session", "available": True,
                           "global": {"available": True, "properties": {
                               "distributionmethod": "Command", "maxjobs": str(jobs),
                               "jobsubmitcommand": 'batch-submit -A acct -q short -R "cpu=6;mem=30000"'}},
                           "tests": [], "warnings": []},
        }
        config = {"runtime": {"run_root": root, "execution_backend": "auto",
                              "max_parallel_jobs": 128, "persistent_worker_lifetime": "run",
                              "candidate_timeout_seconds": 60,
                              "persistent_active_health_interval_seconds": 0.5,
                              "maestro_batch_candidate_limit": limit}}
        spec = {"algorithm": "random", "generations": generations, "population": 5,
                "jobs": 64, "seed": 31415, "maestro_candidate_mode": "auto",
                "variables": [{"parameter_id": "x", "minimum": 0, "maximum": 1}],
                "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
                "constraints": []}
        if overrides is not None:
            spec["maestro_job_overrides"] = overrides
        return spec, config, prepared

    def _controller(self, instances, behavior="normal", remote_stop_proven=True):
        owner = self

        class FakeController(object):
            def __init__(self, run_root, worker_index, _prepared, _selected, config):
                self.run_root = run_root
                self.worker_index = worker_index
                self.config = copy.deepcopy(config)
                self.epoch_index = 0
                self._start_ticks = 456
                self.process = mock.Mock(pid=123)
                self.started = 0
                self.submissions = []
                self.serial_submissions = []
                self.commits = []
                self.terminations = []
                self.restarts = 0
                self.current_ids = []
                self.stopped = False
                self.retained = os.path.join(run_root, "remote_evidence", "retain.txt")
                instances.append(self)

            def start(self):
                self.started += 1
                return self

            def submit_evaluate_batch(self, planned, generation, spec):
                planned = list(planned)
                self.submissions.append(copy.deepcopy(planned))
                self.current_ids = ["g%03d_c%04d" % (generation, i) for i, _v in planned]
                return {"batch": True, "planned": planned, "generation": generation,
                        "spec": spec, "batch_id": "owned_batch_%d" % len(self.submissions)}

            def submit_evaluate(self, index, generation, values, spec):
                self.serial_submissions.append(index)
                self.current_ids = ["g%03d_c%04d" % (generation, index)]
                return {"batch": False, "planned": [(index, values)],
                        "generation": generation, "spec": spec}

            def _row(self, request, index, values):
                value = values["x"]
                row = {"candidate_id": "g%03d_c%04d" % (request["generation"], index),
                       "generation": request["generation"], "index": index,
                       "values": copy.deepcopy(values), "status": "completed",
                       "source_guard": {"ok": True},
                       "metrics": {"objectives": [{"output_key": "main::gain",
                                                   "value": value, "score": -value}],
                                   "constraints": [], "feasible": True,
                                   "total_violation": 0.0, "merit": -value},
                       "persistent_worker": {"worker_index": 0,
                                             "worker_lifetime": "optimization_run",
                                             "cleanup_pending": True}}
                _write_terminal_wal(self.run_root, row)
                return row

            def poll(self, request):
                if request["batch"] and behavior == "unsupported":
                    return {"rows": [], "batch_id": None,
                            "fallback": "native_batch_api_unavailable:maeCreateTest"}
                if request["batch"] and behavior in ("stop", "remote_failure"):
                    if not self.stopped:
                        self.stopped = True
                        os.makedirs(os.path.dirname(self.retained))
                        with open(self.retained, "w") as stream:
                            stream.write("Remote job may still require this evidence")
                        if behavior == "stop":
                            index, values = request["planned"][0]
                            self._row(request, index, values)
                            with open(os.path.join(self.run_root, "STOP"), "w") as stream:
                                stream.write("stop")
                        else:
                            raise PersistentControllerProcessError("synthetic remote submission failure")
                    return None
                rows = [self._row(request, index, values)
                        for index, values in reversed(request["planned"])]
                if request["batch"]:
                    if behavior == "wrong_values":
                        rows[0]["values"]["x"] = 123.0
                    elif behavior == "wrong_index":
                        rows[0]["index"] = 999
                    elif behavior == "wrong_generation":
                        rows[0]["generation"] += 1
                    elif behavior == "duplicate_id":
                        rows[-1] = copy.deepcopy(rows[0])
                    return {"rows": rows, "batch_id": request["batch_id"], "shared_history": True}
                return rows[0]

            def sample_health(self):
                return _proven_session_health()

            def commit_candidate(self, candidate_id):
                # Simulate native history deletion ONLY after the authoritative
                # checkpoint and every terminal recovery artifact survive.
                checkpoint = load_json(os.path.join(self.run_root, "checkpoint.json"))
                saved = set(row["candidate_id"] for row in checkpoint_candidates(checkpoint))
                owner.assertTrue(set(self.current_ids).issubset(saved))
                for current_id in self.current_ids:
                    root = os.path.join(self.run_root, "candidates", current_id)
                    owner.assertEqual(load_json(os.path.join(root, "candidate.json"))["status"], "completed")
                    owner.assertTrue(os.path.isfile(os.path.join(root, "timing.json")))
                    owner.assertTrue(os.path.isfile(os.path.join(root, "completion_evidence.json")))
                self.commits.append(candidate_id)
                return {"ok": True, "state": "cleaned", "health": _proven_session_health()}

            def terminate_and_prove(self, reason):
                self.terminations.append(reason)
                if remote_stop_proven:
                    return dict(_proven_controller_shutdown(), remote_stop={"remote_stop_proven": True})
                return {"termination_proven": False, "process_still_alive": False,
                        "remote_stop": {"remote_stop_proven": False},
                        "cleanup": {"ok": False, "removed": False},
                        "error": "remote batch stop is unproven; preserve evidence"}

            def restart(self):
                self.restarts += 1
                return self

            def close(self):
                if not remote_stop_proven:
                    return self.terminate_and_prove("close without remote stop proof")
                return _proven_controller_shutdown()

        return FakeController

    def _run(self, root, jobs=2, limit=0, generations=1, behavior="normal",
             remote_stop_proven=True, overrides=None):
        spec, config, prepared = self._inputs(root, jobs, limit, generations, overrides)
        instances, tell_indices = [], []
        controller = self._controller(instances, behavior, remote_stop_proven)
        original_tell = PopulationGenerator.tell

        def tell(generator, rows):
            tell_indices.append([row["index"] for row in rows])
            return original_tell(generator, rows)

        with mock.patch("crescendo_engine.runner.PersistentControllerProxy", controller), mock.patch.object(
                PopulationGenerator, "tell", autospec=True, side_effect=tell), mock.patch(
                "crescendo_engine.runner.time.sleep", return_value=None):
            result = run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
        return result, instances, tell_indices, prepared

    def test_max_jobs_below_or_above_population_uses_one_session_and_whole_generation(self):
        for jobs in (1, 2, 64):
            with self.subTest(max_jobs=jobs), tempfile.TemporaryDirectory() as root:
                result, instances, told, _prepared = self._run(root, jobs=jobs)
                self.assertEqual(result["state"], "completed")
                self.assertEqual(len(instances), 1)
                worker = instances[0]
                self.assertEqual(worker.started, 1)
                self.assertEqual([len(batch) for batch in worker.submissions], [5])
                self.assertEqual(worker.serial_submissions, [])
                self.assertEqual(worker.commits, ["owned_batch_1"])
                self.assertEqual(told, [[0, 1, 2, 3, 4]])
                status = load_json(os.path.join(root, "run", "status.json"))
                self.assertEqual(status["maestro_max_jobs"], jobs)
                self.assertEqual(status["completed_candidates"], 5)

    def test_limited_windows_keep_final_partial_batch_and_reuse_session_across_generations(self):
        with tempfile.TemporaryDirectory() as root:
            result, instances, told, _prepared = self._run(root, jobs=64, limit=2, generations=2)
            worker = instances[0]
            self.assertEqual(result["state"], "completed")
            self.assertEqual(worker.started, 1)
            self.assertEqual([len(batch) for batch in worker.submissions], [2, 2, 1, 2, 2, 1])
            self.assertEqual(len(set(worker.commits)), 6)
            self.assertEqual(told, [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]])

    def test_unsupported_batch_falls_back_once_to_same_session_without_population_loss(self):
        with tempfile.TemporaryDirectory() as root:
            result, instances, told, _prepared = self._run(root, behavior="unsupported")
            worker = instances[0]
            self.assertEqual(result["state"], "completed")
            self.assertEqual(len(worker.submissions), 1)
            self.assertEqual(worker.serial_submissions, [0, 1, 2, 3, 4])
            self.assertEqual(worker.commits, ["g000_c0000", "g000_c0001", "g000_c0002", "g000_c0003", "g000_c0004"])
            self.assertEqual(worker.started, 1)
            self.assertEqual(worker.restarts, 0)
            self.assertEqual(told, [[0, 1, 2, 3, 4]])

    def test_stop_recovers_completed_candidate_wal_without_recommitting_shared_history(self):
        with tempfile.TemporaryDirectory() as root:
            result, instances, told, _prepared = self._run(root, behavior="stop")
            self.assertEqual(result["stopped_reason"], "user_stop")
            self.assertEqual(instances[0].commits, [])
            self.assertEqual(len(instances[0].submissions), 1)
            self.assertEqual(told, [])
            checkpoint = load_json(os.path.join(root, "run", "checkpoint.json"))
            self.assertEqual([row["candidate_id"] for row in checkpoint_candidates(checkpoint)], ["g000_c0000"])
            self.assertEqual(checkpoint_candidates(checkpoint)[0]["status"], "completed")
            self.assertTrue(os.path.isfile(os.path.join(root, "run", "candidates", "g000_c0000", "candidate.json")))

    def test_remote_failure_without_stop_proof_preserves_evidence_and_never_resubmits(self):
        with tempfile.TemporaryDirectory() as root:
            spec, config, prepared = self._inputs(root)
            instances = []
            controller = self._controller(instances, "remote_failure", False)
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", controller):
                with self.assertRaises(RuntimeError):
                    run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            worker = instances[0]
            self.assertEqual(len(worker.submissions), 1)
            self.assertEqual(worker.restarts, 0)
            self.assertEqual(worker.commits, [])
            self.assertEqual(worker.serial_submissions, [])
            self.assertTrue(os.path.isfile(worker.retained))
            checkpoint = load_json(os.path.join(root, "run", "checkpoint.json"))
            self.assertEqual(checkpoint_candidates(checkpoint), [])

    def test_unproven_stop_preserves_terminal_wal_and_remote_workspace(self):
        with tempfile.TemporaryDirectory() as root:
            spec, config, prepared = self._inputs(root)
            instances = []
            controller = self._controller(instances, "stop", False)
            with mock.patch("crescendo_engine.runner.PersistentControllerProxy", controller):
                with self.assertRaises(RuntimeError):
                    run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
            self.assertTrue(os.path.isfile(instances[0].retained))
            self.assertTrue(os.path.isfile(os.path.join(root, "run", "candidates", "g000_c0000", "candidate.json")))
            self.assertEqual(len(instances[0].submissions), 1)
            self.assertEqual(instances[0].commits, [])
            self.assertEqual(instances[0].restarts, 0)

    def test_real_retention_refuses_unproven_remote_jobs_even_after_local_death(self):
        with tempfile.TemporaryDirectory() as root:
            epoch = os.path.join(root, "workers", "worker_000", "epoch_000")
            os.makedirs(os.path.join(epoch, "workspace"))
            atomic_write_json(os.path.join(epoch, "worker_state.json"), {"remote_batch": {
                "simulation_submitted": True, "native_completion_proven": False,
                "native_stop_proven": False}})
            with self.assertRaisesRegex(UnsafeCleanupError, "completion/cancellation is unproven"):
                safe_delete_worker_epoch(epoch, root, {"ok": True})
            self.assertTrue(os.path.isdir(os.path.join(epoch, "workspace")))

    def test_checkpoint_keeps_original_policy_identity_and_exact_manual_overrides(self):
        overrides = {"command": 'batch-submit -A different -q long -R "cpu=12;mem=64000"', "max_jobs": 64}
        with tempfile.TemporaryDirectory() as root:
            _result, instances, _told, prepared = self._run(root, overrides=overrides)
            checkpoint = load_json(os.path.join(root, "run", "checkpoint.json"))
            validate_checkpoint_integrity(checkpoint)
            self.assertEqual(checkpoint["spec"]["maestro_job_overrides"], overrides)
            self.assertEqual(instances[0].config["runtime"]["maestro_job_overrides"], overrides)
            self.assertEqual(checkpoint["manifest"]["maestro_job_policy_sha256"],
                             job_policy_fingerprint(prepared["job_policy"], overrides))
            changed = copy.deepcopy(prepared)
            changed["job_policy"]["global"]["properties"]["maxjobs"] = "3"
            with self.assertRaisesRegex(ValueError, "original Maestro job policy changed"):
                validate_checkpoint(checkpoint, changed)
            changed_checkpoint = copy.deepcopy(checkpoint)
            changed_checkpoint["spec"]["maestro_job_overrides"]["command"] = "batch-submit -q altered"
            changed_checkpoint["payload_sha256"] = _checkpoint_checksum(changed_checkpoint)
            with self.assertRaisesRegex(ValueError, "original Maestro job policy changed"):
                validate_checkpoint(changed_checkpoint, prepared)

    def test_malformed_batch_candidate_identity_never_commits_shared_history(self):
        for behavior in ("wrong_values", "wrong_index", "wrong_generation", "duplicate_id"):
            with self.subTest(behavior=behavior), tempfile.TemporaryDirectory() as root:
                spec, config, prepared = self._inputs(root)
                instances = []
                controller = self._controller(instances, behavior)
                with mock.patch("crescendo_engine.runner.PersistentControllerProxy", controller):
                    with self.assertRaises(RuntimeError):
                        run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
                self.assertEqual(len(instances[0].submissions), 1)
                self.assertEqual(instances[0].commits, [])
                self.assertEqual(instances[0].restarts, 0)

    def test_malformed_native_stop_ack_fails_closed_without_blocking_owned_termination(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = PersistentControllerProxy(root, 0, {}, [], {"runtime": {
                "cluster_stop_timeout_seconds": 0.1}})
            proxy.epoch_root = os.path.join(root, "controller")
            os.makedirs(proxy.epoch_root)
            proxy.process = mock.Mock(pid=123)
            proxy.process.poll.return_value = None
            request = {"schema_version": "crescendo.persistent_controller.v1", "token": "current-token",
                       "controller_epoch": 0, "sequence": 1, "nonce": "current-nonce",
                       "operation": "EVALUATE_BATCH"}
            proxy._batch_native_inflight = True
            proxy._batch_request_identity = request
            for document in (["invalid JSON object"], dict(request, native=[1]),
                             dict(request, native="invalid native object"),
                             dict(request, native={"remote_stop_proven": 1})):
                with self.subTest(document=document):
                    atomic_write_json(os.path.join(proxy.epoch_root, "native_stop.json"), document)
                    with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), mock.patch(
                            "crescendo_engine.persistent_controller.os.kill"):
                        result = proxy.request_native_batch_stop("test malformed stop evidence")
                    self.assertTrue(result["required"])
                    self.assertFalse(result["remote_stop_proven"])

    def test_dead_controller_never_receives_native_stop_signal_for_a_reusable_pid(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = PersistentControllerProxy(root, 0, {}, [], {})
            proxy._batch_native_inflight = True
            proxy._batch_request_identity = {"operation": "EVALUATE_BATCH", "token": "owned"}
            dead_process = mock.Mock(pid=123)
            dead_process.poll.return_value = 0
            for process in (None, dead_process):
                with self.subTest(process=process):
                    proxy.process = process
                    # The general cleanup identity helper intentionally permits
                    # an already-dead process. Native signaling requires live proof.
                    with mock.patch.object(proxy, "_controller_identity_is_owned", return_value=True), mock.patch(
                            "crescendo_engine.persistent_controller.os.kill") as signal_call:
                        result = proxy.request_native_batch_stop("controller already exited")
                    signal_call.assert_not_called()
                    self.assertFalse(result["remote_stop_proven"])


if __name__ == "__main__":
    unittest.main()


# Retired native-ADE whole-loop contracts use an exact v0.3.0 test fixture.
# Current helpers and ordinary optimizer/mock tests above still use production code.
from tests.legacy_native_fixture import historical_native_loop


class LegacyNativeRunnerClusterBatchTests(unittest.TestCase):
    """Historical native scheduling assertions, excluded from new-pipeline evidence."""
    _inputs = RunnerClusterBatchTests._inputs
    _controller = RunnerClusterBatchTests._controller
    _run = RunnerClusterBatchTests._run


_HISTORICAL_NATIVE_TESTS = (
    'test_max_jobs_below_or_above_population_uses_one_session_and_whole_generation',
    'test_limited_windows_keep_final_partial_batch_and_reuse_session_across_generations',
    'test_unsupported_batch_falls_back_once_to_same_session_without_population_loss',
    'test_stop_recovers_completed_candidate_wal_without_recommitting_shared_history',
    'test_remote_failure_without_stop_proof_preserves_evidence_and_never_resubmits',
    'test_unproven_stop_preserves_terminal_wal_and_remote_workspace',
    'test_checkpoint_keeps_original_policy_identity_and_exact_manual_overrides',
    'test_malformed_batch_candidate_identity_never_commits_shared_history',
)
for _historical_name in _HISTORICAL_NATIVE_TESTS:
    setattr(LegacyNativeRunnerClusterBatchTests, _historical_name, historical_native_loop(getattr(RunnerClusterBatchTests, _historical_name)))
    delattr(RunnerClusterBatchTests, _historical_name)
