import json
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.multiobjective import build_objective_presentation
from crescendo_engine.server import OptimizerService


def _candidate(value, status="completed"):
    return {
        "candidate_id": "g000_c0000",
        "generation": 0,
        "index": 0,
        "status": status,
        "metrics": {
            "feasible": True,
            "total_violation": 0.0,
            "objectives": [{"value": value, "score": -value}],
            "constraints": [],
        },
    }


def _write_run(root, run_name, value=1.0, state="running"):
    run_root = os.path.join(root, run_name)
    candidate_root = os.path.join(run_root, "candidates", "g000_c0000")
    os.makedirs(candidate_root)
    with open(os.path.join(run_root, "status.json"), "w") as stream:
        json.dump({"run_id": run_name, "state": state}, stream)
    with open(os.path.join(run_root, "spec.json"), "w") as stream:
        json.dump({
            "objectives": [{
                "output_key": "test::gain", "direction": "maximize",
                "aggregation": "mean", "corners": ["typical"],
            }],
        }, stream)
    _write_candidate(root, run_name, value)
    return run_root


def _write_candidate(root, run_name, value, status="completed"):
    path = os.path.join(
        root, run_name, "candidates", "g000_c0000", "candidate.json",
    )
    with open(path, "w") as stream:
        json.dump(_candidate(value, status=status), stream)


class ServerPresentationCacheTest(unittest.TestCase):
    def test_identical_snapshot_reuses_presentation(self):
        with tempfile.TemporaryDirectory() as root:
            _write_run(root, "run-one")
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                    "crescendo_engine.server.build_objective_presentation",
                    wraps=build_objective_presentation) as build:
                first = service.progress("run-one")
                second = service.progress("run-one")
            self.assertEqual(build.call_count, 1)
            self.assertEqual(first["objective_presentation"], second["objective_presentation"])
            self.assertIsNot(first["objective_presentation"], second["objective_presentation"])

    def test_same_candidate_count_with_metric_change_invalidates(self):
        with tempfile.TemporaryDirectory() as root:
            _write_run(root, "run-one", value=1.0)
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                    "crescendo_engine.server.build_objective_presentation",
                    wraps=build_objective_presentation) as build:
                service.progress("run-one")
                _write_candidate(root, "run-one", value=2.0)
                progress = service.progress("run-one")
            self.assertEqual(build.call_count, 2)
            self.assertEqual(
                progress["objective_presentation"]["points"][0]["objective_values"],
                [2.0],
            )

    def test_cache_is_isolated_by_run(self):
        with tempfile.TemporaryDirectory() as root:
            _write_run(root, "run-one", value=1.0)
            _write_run(root, "run-two", value=2.0)
            service = OptimizerService({"runtime": {"run_root": root}})
            with mock.patch(
                    "crescendo_engine.server.build_objective_presentation",
                    wraps=build_objective_presentation) as build:
                service.progress("run-one")
                service.progress("run-two")
                service.progress("run-one")
                service.progress("run-two")
            self.assertEqual(build.call_count, 2)
            self.assertEqual(set(service._objective_presentation_cache), set(("run-one", "run-two")))

    def test_terminal_or_deleted_run_does_not_retain_cache(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = _write_run(root, "run-one")
            service = OptimizerService({"runtime": {"run_root": root}})
            service.progress("run-one")
            self.assertIn("run-one", service._objective_presentation_cache)
            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump({"run_id": "run-one", "state": "completed"}, stream)
            service.progress("run-one")
            self.assertNotIn("run-one", service._objective_presentation_cache)

            with open(os.path.join(run_root, "status.json"), "w") as stream:
                json.dump({"run_id": "run-one", "state": "running"}, stream)
            service.progress("run-one")
            self.assertIn("run-one", service._objective_presentation_cache)
            shutil.rmtree(run_root)
            with self.assertRaises(ValueError):
                service.progress("run-one")
            self.assertNotIn("run-one", service._objective_presentation_cache)

    def test_owned_run_thread_evicts_cache_when_it_finishes(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {"run_root": root}})
            started = threading.Event()
            release = threading.Event()

            def run_optimization(*_args, **_kwargs):
                started.set()
                release.wait(5)

            with mock.patch(
                    "crescendo_engine.server.run_optimization",
                    side_effect=run_optimization):
                result = service._launch_registered(
                    {"evaluator": "mock", "generations": 1, "population": 1}, {},
                )
                self.assertTrue(started.wait(5))
                with service._objective_presentation_lock:
                    service._objective_presentation_cache[result["run_id"]] = {
                        "revision": "test",
                        "presentation": {},
                    }
                release.set()
                service.threads[result["run_id"]].join(5)
            self.assertFalse(service.threads[result["run_id"]].is_alive())
            self.assertNotIn(result["run_id"], service._objective_presentation_cache)


if __name__ == "__main__":
    unittest.main()
