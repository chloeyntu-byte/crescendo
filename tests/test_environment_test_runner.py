"""New diagnostic entry stays at one candidate without weakening evaluation."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.metrics import evaluate_metrics
from crescendo_engine.single_test import project_setup
from crescendo_engine.runner import (
    _environment_test_run, _normalize_algorithm_settings, run_optimization,
)
from crescendo_engine.utils import load_json, atomic_write_json
from tests.test_unified_optimizer import common_adapter


class EnvironmentTestRunnerTests(unittest.TestCase):
    def _spec(self):
        return {
            "environment_test": {"schema_version": "crescendo.environment_test.run.v1",
                                 "test_id": "environment_test_owned_123"},
            "evaluator": "real", "algorithm": "random", "seed": 1,
            "algorithm_parameters": {"population": 1, "seed": 1},
            "population": 1, "generations": 1, "jobs": 1,
            "variables": [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}],
            "objectives": [{"output_key": "main::gain", "direction": "maximize"},
                           {"output_key": "main::power", "direction": "minimize"}],
            "constraints": [{"output_key": "main::power", "operator": "<=", "limit": 2.0}],
        }

    def test_multiple_objectives_do_not_increase_diagnostic_population(self):
        algorithm, policy, settings = _normalize_algorithm_settings(self._spec(), 1, 2)
        self.assertEqual(algorithm, "random")
        self.assertEqual(settings["population"], 1)
        self.assertEqual(policy, "explicit_user_selection")

    def test_normal_entry_without_marker_is_unchanged(self):
        spec = self._spec()
        spec.pop("environment_test")
        spec.update(population=20, generations=5)
        spec["algorithm_parameters"]["population"] = 20
        self.assertFalse(_environment_test_run(spec))
        self.assertEqual(_normalize_algorithm_settings(spec, 1, 2)[2]["population"], 20)

    def test_invalid_marker_cannot_enter_normal_population_path(self):
        for marker in (None, {}, "test", {"schema_version": "crescendo.environment_test.run.v2",
                                        "test_id": "owned"},
                       {"schema_version": "crescendo.environment_test.run.v1", "test_id": "owned", "extra": True}):
            spec = self._spec()
            spec["environment_test"] = marker
            with self.subTest(marker=marker), self.assertRaises(ValueError):
                _normalize_algorithm_settings(spec, 1, 2)

    def test_marker_requires_bounded_real_single_candidate_request(self):
        changes = [{"evaluator": "mock"}, {"algorithm": "nsga2"},
                   {"generations": 2}, {"generations": True}, {"population": 2},
                   {"population": True}, {"algorithm_parameters": {"population": 2}},
                   {"algorithm_parameters": [1]}, {"algorithm_parameters": {"population": True}}]
        for change in changes:
            spec = self._spec()
            spec.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                _normalize_algorithm_settings(spec, 1, 2)

    def test_marker_requires_test_identity(self):
        for test_id in (None, "", "  ", "\nowned", "x" * 129):
            spec = self._spec()
            spec["environment_test"]["test_id"] = test_id
            with self.subTest(test_id=test_id), self.assertRaises(ValueError):
                _environment_test_run(spec)

    def _run_with_fake_execution(self, root, spec, evaluator):
        prepared = {"setup": {"tests": []}, "hierarchy": {"designs": []},
                    "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
                    "outputs": [{"key": "main::gain"}, {"key": "main::power"}]}
        prepared["setup"]["tests"] = [{"name": "main", "enabled": True}]
        prepared["setup"] = project_setup(prepared["setup"], "main")
        config = {"runtime": {"run_root": root, "max_parallel_jobs": 1,
                               "candidate_timeout_seconds": 60,
                               "infrastructure_retries": 5,
                               "max_infrastructure_replacements": 5}}
        before = copy.deepcopy(config)
        instances = []
        base = common_adapter(instances)
        class DiagnosticCommonAdapter(base):
            def __init__(self, run_root, run_spec, prepared, selected, run_config, *args, **kwargs):
                super(DiagnosticCommonAdapter, self).__init__(run_root, run_spec, prepared, selected, run_config, *args, **kwargs)
                self.prepared, self.selected = prepared, selected
            def evaluate(self, index, generation, values):
                self.calls.append((generation, index, copy.deepcopy(values)))
                row = evaluator(self.run_root, index, generation, values, self.spec,
                    self.prepared, self.selected, self.config)
                row['execution_backend'] = self.spec['execution_backend']
                row['source_guard'] = {'ok': True, 'fixture_only': True}
                row['native_job_state'] = {'native_completion_proven': row['status'] == 'completed',
                    'simulation_submitted': True}
                if row['status'] == 'completed':
                    row['completion_evidence'] = {'schema_version': 'crescendo.private_netlist_completion.v1',
                        'ok': True, 'parameters': dict(values), 'fixture_only': True}
                candidate_root = os.path.join(self.run_root, 'candidates', row['candidate_id'])
                os.makedirs(candidate_root, exist_ok=True)
                atomic_write_json(os.path.join(candidate_root, 'candidate.json'), row)
                atomic_write_json(os.path.join(candidate_root, 'timing.json'), {'fixture_only': True})
                if row.get('completion_evidence'):
                    atomic_write_json(os.path.join(candidate_root, 'completion_evidence.json'), row['completion_evidence'])
                return row
        try:
            with mock.patch("crescendo_engine.runner.PrivateNetlistOptimizer", DiagnosticCommonAdapter), \
                    mock.patch("crescendo_engine.runner.startup_storage_preflight", return_value={"ok": True}), \
                    mock.patch("crescendo_engine.runner._reverify_prepared_external_environment", return_value={"ok": True}), \
                    mock.patch("crescendo_engine.runner._evaluate_candidate") as legacy, \
                    mock.patch("crescendo_engine.runner.PersistentControllerProxy") as native:
                result = run_optimization(spec, config, prepared=prepared,
                                          run_root=os.path.join(root, "single_test"))
                legacy.assert_not_called()
                native.assert_not_called()
                self.assertEqual(len(instances), 1)
                self.assertEqual(instances[0].spec['execution_backend'], 'maestro_netlist')
                self.assertIs(instances[0].config.get('_diagnostic_native_run_trace'), True)
        finally:
            self.assertEqual(config, before)
        return result

    def test_shared_infrastructure_failure_does_not_retry_or_resample(self):
        calls = []
        with tempfile.TemporaryDirectory() as root:
            def failure(_root, index, generation, values, spec, _prepared, _selected, config):
                calls.append((generation, index, dict(values)))
                self.assertEqual(config["runtime"]["infrastructure_retries"], 0)
                return {"candidate_id": "g000_c0000", "generation": generation, "index": index,
                        "values": values, "status": "infrastructure_failed", "failure_class": "infrastructure",
                        "error": "native startup unavailable", "failure_stage": "maestro_startup"}

            with self.assertRaisesRegex(RuntimeError, "systemic infrastructure failure"):
                self._run_with_fake_execution(root, self._spec(), failure)
            self.assertEqual(len(calls), 1)
            status = load_json(os.path.join(root, "single_test", "status.json"))
            self.assertEqual(status["total_candidates"], 1)
            self.assertEqual(status["infrastructure_failed_candidates"], 1)
            self.assertEqual(status["completed_candidates"], 0)

    def test_one_candidate_still_computes_all_objectives_constraints_and_pareto(self):
        calls = []
        with tempfile.TemporaryDirectory() as root:
            def completed(_root, index, generation, values, spec, _prepared, _selected, config):
                calls.append((generation, index, dict(values)))
                self.assertEqual(config["runtime"]["infrastructure_retries"], 0)
                parsed = {"output_map": {"main::gain": {"summary": {"mean": 4.0}},
                                         "main::power": {"summary": {"mean": 1.0}}}}
                return {"candidate_id": "g000_c0000", "generation": generation, "index": index,
                        "values": values, "status": "completed",
                        "metrics": evaluate_metrics(parsed, spec["objectives"], spec["constraints"])}

            result = self._run_with_fake_execution(root, self._spec(), completed)
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(result["candidates"]), 1)
            row = result["candidates"][0]
            self.assertEqual(len(row["metrics"]["objectives"]), 2)
            self.assertEqual(len(row["metrics"]["constraints"]), 1)
            self.assertTrue(row["metrics"]["feasible"])
            self.assertEqual(len(result["pareto_front"]), 1)
            self.assertEqual(result["representative"]["candidate_id"], "g000_c0000")


if __name__ == "__main__":
    unittest.main()
