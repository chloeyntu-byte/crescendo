import copy
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.single_test import project_setup
from crescendo_engine.netlist_execution import NetlistExecutionError
from crescendo_engine.private_netlist import PrivateNetlistUnsupported
from crescendo_engine.utils import atomic_write_json, load_json


def model_task():
    # Real immutable configuration contract; solver invocation remains mocked.
    from crescendo_engine.simulators import execution_contract
    return {"test": "main", "corner": "nominal", "point_id": 1,
            "simulator_contract": execution_contract({"cadence": {"spectre_bin": "/bin/true"}}, "spectre", [])}


def fixtures(root, max_jobs=2, population=4):
    prepared = {"setup": {"backend": "maestro", "tests": [], "corners": []},
                "hierarchy": {"designs": []},
                "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                "outputs": [{"key": "main::gain"}]}
    config = {"runtime": {"run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
                           "candidate_timeout_seconds": 60}}
    spec = {"algorithm": "random", "generations": 2, "population": population, "jobs": 1,
            "execution_backend": "private_netlist", "maestro_job_overrides": {"max_jobs": max_jobs},
            "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
            "objectives": [{"output_key": "main::gain", "direction": "maximize"}], "constraints": []}
    return prepared, config, spec


def parsed(value):
    output = {"key": "main::gain", "scalar": True,
              "values": [{"corner": "nominal", "numeric": value}],
              "summary": dict((name, value) for name in ("first", "last", "min", "max", "mean"))}
    return {"outputs": [output], "output_map": {"main::gain": output}}


class ThinAdapterTests(unittest.TestCase):
    def numeric_companion_error(self, root):
        error = PrivateNetlistUnsupported('parameters DPAR_10 requires native generation')
        error.detail = {'failure_stage': 'native_numeric_derived_parameters',
            'native_generation_required': True, 'template_qualified': False,
            'template_root': os.path.join(root, 'private_netlist', 'templates'),
            'source_guard': {'ok': True}, 'numeric_derived_parameters': [{'parameter': 'DPAR_10'}]}
        return error

    def test_numeric_companion_uses_fresh_native_candidates_preserving_backend(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared['parameters'], config, runner._failure_metrics)
            context, producer = mock.Mock(), mock.Mock()
            context.unsafe.is_set.return_value = False
            def generate(values, candidate_root, candidate_id, **kwargs):
                os.makedirs(candidate_root)
                return {'root': candidate_root, 'candidate_id': candidate_id, 'values': values,
                    'applied_values': values, 'workspace': {}, 'source_guard': {'ok': True},
                    'netlist_generation': {'source_mode': 'maestro_netlist', 'parameter_application': {'applied': values}},
                    'tasks': [dict(model_task(), netlist_generation={'mode': 'maestro'})]}
            producer.generate.side_effect = generate
            producer.close.return_value = {'cleanup_proven': True}
            context.close.return_value = {'cleanup_proven': True}
            def evaluate(manifest, **kwargs):
                self.assertEqual(manifest['netlist_generation']['source_mode'], 'maestro_netlist')
                self.assertFalse(manifest['netlist_generation']['template_qualified'])
                return {'parsed': parsed(manifest['values']['x']), 'completion': {
                    'execution_pipeline': {'netlist_source': 'maestro'}},
                    'native_job_state': {'native_completion_proven': True}, 'task_results': []}
            context.evaluate.side_effect = evaluate
            with mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext', return_value=context), \
                    mock.patch('crescendo_engine.netlist_optimizer.prepare_private_netlist', side_effect=self.numeric_companion_error(root)) as prepare, \
                    mock.patch('crescendo_engine.shared_maestro.SharedMaestroProducer', return_value=producer), \
                    mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation', return_value={'cleanup_proven': True}), \
                    mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                    mock.patch('crescendo_engine.netlist_optimizer.materialize_candidate') as frozen, \
                    mock.patch('crescendo_engine.netlist_optimizer.verify_sources_unchanged', return_value={'ok': True}) as guard:
                rows = [adapter.evaluate(i, 0, {'x': value}) for i, value in enumerate((0.4, 0.7))]
                self.assertEqual([row['status'] for row in rows], ['completed', 'completed'])
                self.assertEqual([row['metrics']['objectives'][0]['value'] for row in rows], [0.4, 0.7])
                self.assertTrue(all(row['execution_backend'] == 'private_netlist' for row in rows))
                self.assertTrue(all(row['execution_pipeline']['netlist_source'] == 'maestro' for row in rows))
                self.assertEqual(spec['execution_backend'], 'private_netlist')
                self.assertEqual(producer.generate.call_count, 2)
                self.assertEqual(context.evaluate.call_count, 2)
                prepare.assert_called_once()
                frozen.assert_not_called()
                self.assertTrue(adapter.close()['cleanup_proven'])
                self.assertEqual(guard.call_count, 2)
            self.assertTrue(os.path.isfile(os.path.join(root, 'private_netlist', 'native_generation_required.json')))

    def test_numeric_companion_fallback_requires_source_and_cleanup_proof(self):
        for failure in ('source', 'cleanup', 'cancelled'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as root:
                prepared, config, spec = fixtures(root)
                stopped = threading.Event()
                adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared['parameters'], config,
                                                 runner._failure_metrics, cancel_check=stopped.is_set)
                error = self.numeric_companion_error(root)
                if failure == 'source':
                    error.detail['source_guard']['ok'] = False
                def prepare(*args, **kwargs):
                    if failure == 'cancelled':
                        stopped.set()
                    raise error
                context = mock.Mock()
                context.unsafe.is_set.return_value = False
                with mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext', return_value=context), \
                        mock.patch('crescendo_engine.netlist_optimizer.prepare_private_netlist', side_effect=prepare), \
                        mock.patch('crescendo_engine.shared_maestro.SharedMaestroProducer') as producer, \
                        mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation', return_value={'cleanup_proven': failure != 'cleanup'}):
                    row = adapter.evaluate(0, 0, {'x': 0.4})
                self.assertEqual(row['status'], 'cancelled' if failure == 'cancelled' else 'infrastructure_failed')
                self.assertNotIn('metrics', row)
                if failure == 'cleanup':
                    self.assertTrue(row['cleanup_pending'])
                    self.assertEqual(adapter.prepare_error.detail['failure_stage'], 'native_template_capture_cleanup')
                    self.assertFalse(adapter.prepare_error.detail['preparation_cleanup']['cleanup_proven'])
                    self.assertTrue(adapter.prepare_error.detail['source_guard']['ok'])
                producer.assert_not_called()
                context.evaluate.assert_not_called()

    def selected_binding_error(self, root):
        from tests.test_native_unresolved_binding import captured_binding_references, SYMBOLS
        from crescendo_engine.private_netlist import _native_unresolved_binding_fallback
        proof = _native_unresolved_binding_fallback(
            captured_binding_references(os.path.join(root, 'binding_references')), SYMBOLS)
        error = PrivateNetlistUnsupported('selected native CDF input has no DPAR template binding')
        error.detail = dict(proof, source_guard={'ok': True},
                            template_root=os.path.join(root, 'private_netlist', 'templates'))
        return error

    def test_proven_unresolved_binding_uses_fresh_native_candidates(self):
        with mock.patch.object(self, 'numeric_companion_error', side_effect=self.selected_binding_error):
            self.test_numeric_companion_uses_fresh_native_candidates_preserving_backend()

    def test_unresolved_binding_preserves_source_cleanup_and_cancellation_gates(self):
        with mock.patch.object(self, 'numeric_companion_error', side_effect=self.selected_binding_error):
            self.test_numeric_companion_fallback_requires_source_and_cleanup_proof()

    def test_user_stop_during_preparation_has_no_terminal_fitness(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            stopped = threading.Event()
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config,
                                             runner._failure_metrics, cancel_check=stopped.is_set)
            def interrupted(_phases):
                stopped.set()
                raise RuntimeError("owned preparation was cancelled")
            with mock.patch.object(adapter, "_prepare_once", side_effect=interrupted):
                row = adapter.evaluate(0, 0, {"x": 0.5})
            self.assertEqual(row["status"], "cancelled")
            self.assertEqual(row["failure_class"], "user_stop")
            self.assertNotIn("metrics", row)
            self.assertIsNone(adapter.fatal_error)

    def test_changed_model_profile_blocks_next_candidate_before_solver(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            adapter.preparing = True
            adapter.prepared_event.set()
            adapter.manifest = {"source_identity": {}}
            adapter.context = mock.Mock()
            adapter.context.unsafe.is_set.return_value = False
            adapter.context.evaluate.return_value = {"parsed": parsed(1.0), "completion": {},
                "native_job_state": {"native_completion_proven": True}, "task_results": []}
            original = model_task()
            changed = copy.deepcopy(original)
            changed["simulator_contract"]["threads"] = 2
            from crescendo_engine.simulators import _fingerprint
            changed["simulator_contract"]["fingerprint"] = _fingerprint(changed["simulator_contract"])
            with mock.patch("crescendo_engine.netlist_optimizer.materialize_candidate",
                            side_effect=[{"tasks": [original]}, {"tasks": [changed]}]):
                first = adapter.evaluate(0, 0, {"x": 0.5})
                second = adapter.evaluate(1, 0, {"x": 0.6})
            self.assertEqual(first["status"], "completed")
            self.assertEqual(second["status"], "infrastructure_failed")
            self.assertIn("Frozen run", second["error"])
            self.assertNotIn("metrics", second)
            adapter.context.evaluate.assert_called_once()

    def test_model_drift_at_final_boundary_prevents_successful_close(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            check = mock.Mock(return_value={"ok": False, "error": "model changed after simulation"})
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config,
                                             runner._failure_metrics, environment_check=check)
            adapter.manifest = {"workspace": {}}
            adapter.context = mock.Mock()
            adapter.context.close.return_value = {"cleanup_proven": True, "retained": []}
            with self.assertRaisesRegex(RuntimeError, "model changed after simulation"):
                adapter.close()
            adapter.context.close.assert_called_once()
            adapter.context.cancel.assert_called_once()
            check.assert_called_once_with(-1)
            self.assertEqual(adapter.fatal_error, "model changed after simulation")

    def test_concurrent_candidates_share_one_native_preparation_and_results_contract(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            manifest = {"source_guard": {"ok": True}, "workspace": {}, "source_identity": {},
                        "manifest_sha256": "template", "preparation": {"elapsed_seconds": 0.01}, "tasks": []}
            context = mock.Mock()
            context.unsafe.is_set.return_value = False

            def materialize(_manifest, values, candidate_root, candidate_id):
                os.makedirs(candidate_root)
                return {"candidate_id": candidate_id, "values": values,
                        "tasks": [model_task()]}

            def evaluate(candidate, **_kwargs):
                return {"parsed": parsed(candidate["values"]["x"]),
                        "completion": {"adapter": "private_netlist", "outputs": [{"key": "main::gain"}]},
                        "native_job_state": {"native_completion_proven": True}, "task_results": []}

            context.evaluate.side_effect = evaluate
            context.close.return_value = {"cleanup_proven": True, "retained": []}

            def prepare(*_args, **_kwargs):
                time.sleep(0.05)
                return manifest

            with mock.patch("crescendo_engine.netlist_optimizer.NetlistExecutionContext", return_value=context), \
                    mock.patch("crescendo_engine.netlist_optimizer.prepare_private_netlist", side_effect=prepare) as preparation, \
                    mock.patch("crescendo_engine.netlist_optimizer.materialize_candidate", side_effect=materialize), \
                    mock.patch("crescendo_engine.netlist_optimizer.verify_sources_unchanged", return_value={"ok": True}) as guard:
                with ThreadPoolExecutor(max_workers=4) as pool:
                    rows = list(pool.map(lambda i: adapter.evaluate(i, 0, {"x": i / 10.0}), range(4)))
                self.assertTrue(all(row["status"] == "completed" for row in rows))
                self.assertEqual([row["metrics"]["objectives"][0]["value"] for row in rows], [0.0, 0.1, 0.2, 0.3])
                preparation.assert_called_once()
                # No full OA source hashing for every candidate.
                guard.assert_not_called()
                self.assertTrue(adapter.close()["cleanup_proven"])
                guard.assert_called_once()
            candidate_phases = [row["timing"]["phases"] for row in rows]
            self.assertEqual(sum("shared_setup" in phases for phases in candidate_phases), 1)
            self.assertEqual(sum("shared_setup_wait" in phases for phases in candidate_phases), 3)
            self.assertTrue(all("template_materialization" in phases for phases in candidate_phases))
            for row in rows:
                candidate_root = os.path.join(root, "candidates", row["candidate_id"])
                self.assertTrue(load_json(os.path.join(candidate_root, "completion_evidence.json"))["ok"])
                self.assertTrue(os.path.isfile(os.path.join(candidate_root, "execution_diagnostics.json")))
                self.assertEqual(load_json(os.path.join(candidate_root, "timing.json")), row["timing"])

    def test_candidate_still_completes_if_timing_sidecar_cannot_be_written(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            manifest = {"source_guard": {"ok": True}, "workspace": {}}
            context = mock.Mock()
            context.evaluate.return_value = {"parsed": parsed(0.4), "completion": {}, "native_job_state": {}}
            with mock.patch("crescendo_engine.netlist_optimizer.NetlistExecutionContext", return_value=context), \
                    mock.patch("crescendo_engine.netlist_optimizer.prepare_private_netlist", return_value=manifest), \
                    mock.patch("crescendo_engine.netlist_optimizer.compact_preparation_artifacts", return_value={"ok": True}), \
                    mock.patch("crescendo_engine.netlist_optimizer.materialize_candidate",
                               return_value={"tasks": [model_task()]}), \
                    mock.patch("crescendo_engine.candidate_timing.atomic_write_json", side_effect=OSError("timing write failed")):
                row = adapter.evaluate(0, 0, {"x": 0.4})
            self.assertEqual(row["status"], "completed")
            self.assertGreaterEqual(row["timing"]["elapsed_seconds"], 0)
            candidate = os.path.join(root, "candidates", row["candidate_id"])
            self.assertEqual(load_json(os.path.join(candidate, "candidate.json"))["status"], "completed")
            self.assertFalse(os.path.exists(os.path.join(candidate, "timing.json")))

    def test_unsupported_mapping_fails_once_without_legacy_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            context = mock.Mock()
            context.unsafe.is_set.return_value = False
            with mock.patch("crescendo_engine.netlist_optimizer.NetlistExecutionContext", return_value=context), \
                    mock.patch("crescendo_engine.netlist_optimizer.prepare_private_netlist", side_effect=PrivateNetlistUnsupported("derived field cannot be represented")) as preparation:
                rows = [adapter.evaluate(index, 0, {"x": 0.5}) for index in range(2)]
            preparation.assert_called_once()
            context.evaluate.assert_not_called()
            self.assertTrue(all(row["status"] == "infrastructure_failed" for row in rows))
            self.assertIn("derived field cannot be represented", adapter.fatal_error)

    def test_numerical_spectre_rejection_is_penalty_while_unknown_cleanup_is_fatal(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            adapter.preparing = True
            adapter.prepared_event.set()
            adapter.manifest = {"source_identity": {}}
            adapter.context = mock.Mock()
            adapter.context.unsafe.is_set.return_value = False
            adapter.context.evaluate.side_effect = NetlistExecutionError("illegal device size", infrastructure=False)
            with mock.patch("crescendo_engine.netlist_optimizer.materialize_candidate", return_value={"tasks": [model_task()]}):
                row = adapter.evaluate(0, 0, {"x": 0.5})
            self.assertEqual(row["status"], "failed")
            self.assertTrue(row["metrics"]["penalized"])
            self.assertIsNone(adapter.fatal_error)
            adapter.context.evaluate.side_effect = NetlistExecutionError("queued job ownership unresolved", {"termination_failed": True})
            with mock.patch("crescendo_engine.netlist_optimizer.materialize_candidate", return_value={"tasks": [model_task()]}):
                row = adapter.evaluate(1, 0, {"x": 0.6})
            self.assertEqual(row["status"], "infrastructure_failed")
            self.assertTrue(row["cleanup_pending"])
            self.assertNotIn("metrics", row)
            adapter.context.cancel.assert_called_once()


class RunnerEngineTests(unittest.TestCase):
    def test_design_variable_backend_is_rejected_before_native_preparation(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            prepared["setup"]["tests"] = [{"name": "main", "enabled": True}]
            prepared["setup"] = project_setup(prepared["setup"], "main")
            prepared["parameters"][0]["backend"] = "design_variable"
            with mock.patch("crescendo_engine.runner.PrivateNetlistOptimizer") as context:
                with self.assertRaisesRegex(ValueError, "design variables are not qualified"):
                    runner.run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))
                context.assert_not_called()

    def _run(self, max_jobs, population=4, fail=False):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root, max_jobs, population)
            prepared["setup"]["tests"] = [{"name": "main", "enabled": True}]
            prepared["setup"] = project_setup(prepared["setup"], "main")
            run_root = os.path.join(root, "run")
            calls, contexts = [], []
            concurrency = {"active": 0, "peak": 0}
            lock = threading.Lock()

            class FakeAdapter(object):
                def __init__(self, run_root, spec, *_args, **kwargs):
                    self.root = os.path.join(run_root, "private_netlist")
                    os.makedirs(self.root)
                    self.parallel_candidates = min(max_jobs, population)
                    self.policy = {"max_jobs": max_jobs}
                    self.fatal_error = None
                    self.closed = 0
                    contexts.append(self)

                def evaluate(self, index, generation, values):
                    with lock:
                        concurrency["active"] += 1
                        concurrency["peak"] = max(concurrency["peak"], concurrency["active"])
                    # Real optimizer persisted its full plan before any work.
                    plan = load_json(os.path.join(run_root, "checkpoint.json"))["state"]["active_plan"]
                    self.assert_plan = len(plan["planned"])
                    time.sleep(0.03)
                    calls.append((generation, index))
                    row = {"candidate_id": "g%03d_c%04d" % (generation, index), "generation": generation,
                           "index": index, "values": values, "status": "completed", "execution_backend": "private_netlist",
                           "metrics": runner.evaluate_metrics(parsed(values["x"]), spec["objectives"], [])}
                    if fail:
                        self.fatal_error = "unsupported parameter expression"
                        row.update(status="infrastructure_failed", failure_class="private_netlist_infrastructure", error=self.fatal_error)
                        row.pop("metrics")
                    with lock:
                        concurrency["active"] -= 1
                    return row

                def verify_sources(self):
                    return {"ok": True}

                def cancel(self):
                    pass

                def close(self, **_kwargs):
                    self.closed += 1
                    return {"cleanup_proven": True, "retained": []}

            with mock.patch("crescendo_engine.runner.PrivateNetlistOptimizer", FakeAdapter), \
                    mock.patch("crescendo_engine.runner.PersistentControllerProxy") as old_controller, \
                    mock.patch("crescendo_engine.runner._evaluate_candidate") as old_legacy:
                if fail:
                    with self.assertRaisesRegex(RuntimeError, "unsupported parameter"):
                        runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
                    self.assertEqual(load_json(os.path.join(run_root, "status.json"))["state"], "failed")
                    self.assertLessEqual(len(calls), min(max_jobs, population))
                    self.assertEqual(len(runner.checkpoint_candidates(load_json(os.path.join(run_root, "checkpoint.json")))), len(calls))
                else:
                    result = runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
                    self.assertEqual(result["state"], "completed")
                    self.assertEqual(len(result["candidates"]), population * 2)
                    checkpoint = load_json(os.path.join(run_root, "checkpoint.json"))
                    runner.validate_checkpoint_integrity(checkpoint)
                    self.assertEqual(checkpoint["spec"]["execution_backend"], "private_netlist")
                    self.assertNotIn("legacy_guardian_protocol", checkpoint["spec"])
                    self.assertEqual(contexts[0].assert_plan, population)
                    self.assertEqual(concurrency["peak"], min(max_jobs, population))
                self.assertEqual(contexts[0].closed, 1)
                old_controller.assert_not_called()
                old_legacy.assert_not_called()

    def test_max_jobs_below_population_uses_complete_generation_in_windows(self):
        self._run(2, population=5)

    def test_max_jobs_above_population_has_no_padding_or_old_parallel_cap(self):
        self._run(20, population=6)

    def test_systemic_mapping_failure_has_durable_candidates_and_no_resampling(self):
        self._run(2, population=5, fail=True)


if __name__ == "__main__":
    unittest.main()
