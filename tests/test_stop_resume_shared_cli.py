"""Whole-loop cancellation/resume fixtures, with no native or cluster claim."""
import copy
import os
import unittest
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.single_test import project_setup
from crescendo_engine.run_data_store import CORE_NAME, load_dataset
from crescendo_engine.utils import atomic_write_json, atomic_write_text, load_json
from tests import test_unified_optimizer as fixtures
from tests.test_runner import _coordinated_runner_resume


def stop_second_adapter(instances):
    Base = fixtures.common_adapter(instances)

    class StopSecond(Base):
        def evaluate(self, index, generation, values):
            if (generation, index) != (0, 1):
                return super(StopSecond, self).evaluate(index, generation, values)
            self.calls.append((generation, index, copy.deepcopy(values)))
            self.before_cancel_checkpoint = load_json(os.path.join(self.run_root, "checkpoint.json"))
            candidate_id = "g000_c0001"
            root = os.path.join(self.run_root, "candidates", candidate_id)
            os.makedirs(root, exist_ok=True)
            # One measured point is durable, while this candidate still has
            # unfinished points. Resume must rerun it rather than reuse this
            # partial scalar as optimizer fitness.
            self.point_ack = self.point_commit_callback({
                "candidate_id": candidate_id, "task_id": "partial_fixture",
                "candidate_completed": False,
                "outputs": [{"key": "test::gain", "value": values["x"]}],
                "process_termination_proven": True, "remote_stop_proven": True,
                "fixture": "control-flow-only; no native EDA execution",
            }, lambda: False)
            atomic_write_text(os.path.join(self.run_root, "STOP"), "fixture Stop during unfinished candidate\n")
            row = {"candidate_id": candidate_id, "generation": generation, "index": index,
                   "values": copy.deepcopy(values), "status": "cancelled", "failure_class": "user_stop",
                   "execution_backend": self.spec["execution_backend"],
                   "source_guard": {"ok": True, "source": "fixture"},
                   "failure_stage": "solve", "error": "fixture owned solver was cancelled"}
            atomic_write_json(os.path.join(root, "candidate.json"), row)
            atomic_write_json(os.path.join(root, "timing.json"), {"fixture": True, "status": "cancelled"})
            atomic_write_json(os.path.join(root, "execution_diagnostics.json"), {"fixture": True})
            return row

    return StopSecond


class SharedCliStopResumeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.UnifiedOptimizerTests()
        self.fixture.setUp()
        self.fixture.prepared['setup'] = project_setup(
            {'backend': 'maestro', 'tests': [{'name': 'test', 'enabled': True}], 'corners': []}, 'test')

    def tearDown(self):
        self.fixture.tearDown()

    def test_stop_partial_candidate_preserves_plan_rng_and_resume_retries_only_unfinished(self):
        for backend in ("maestro_netlist", "private_netlist"):
            with self.subTest(backend=backend):
                spec = dict(self.fixture.spec, execution_backend=backend)
                baseline_name = backend + "_baseline"
                baseline, unused = self.fixture.run_fixture(spec, name=baseline_name)
                baseline_checkpoint = load_json(os.path.join(self.fixture.root, baseline_name, "checkpoint.json"))
                stopped_root = os.path.join(self.fixture.root, backend + "_stopped")
                stopped_instances = []
                with mock.patch.object(runner, "PrivateNetlistOptimizer", stop_second_adapter(stopped_instances)), \
                        mock.patch.object(runner, "PersistentControllerProxy") as native, \
                        mock.patch.object(runner, "_evaluate_candidate") as legacy:
                    stopped = runner.run_optimization(spec, self.fixture.config, prepared=self.fixture.prepared,
                                                      run_root=stopped_root)
                    native.assert_not_called()
                    legacy.assert_not_called()
                self.assertEqual(stopped["state"], "stopped")
                self.assertEqual(stopped["stopped_reason"], "user_stop")
                adapter = stopped_instances[0]
                self.assertEqual([(generation, index) for generation, index, values in adapter.calls], [(0, 0), (0, 1)])
                self.assertTrue(adapter.point_ack["ok"])
                self.assertFalse(adapter.point_ack["candidate_completed"])
                self.assertEqual(adapter.closed, [True])
                self.assertIsNone(adapter.fatal_error)

                dataset = load_dataset(os.path.join(stopped_root, CORE_NAME))
                displayed = {row["candidate_id"]: row for row in dataset["candidates"]}
                self.assertEqual(displayed["g000_c0000"]["status"], "completed")
                cancelled = displayed["g000_c0001"]
                self.assertEqual(cancelled["status"], "cancelled")
                self.assertEqual(cancelled["failure_class"], "user_stop")
                self.assertNotIn("metrics", cancelled)

                checkpoint = load_json(os.path.join(stopped_root, "checkpoint.json"))
                runner.validate_checkpoint_integrity(checkpoint)
                saved = runner.checkpoint_candidates(checkpoint)
                self.assertEqual([row["candidate_id"] for row in saved], ["g000_c0000"])
                self.assertIsNotNone(checkpoint["state"]["active_plan"])
                self.assertEqual(checkpoint["state"]["active_plan"], adapter.before_cancel_checkpoint["state"]["active_plan"])
                self.assertEqual(checkpoint["state"]["generator"], adapter.before_cancel_checkpoint["state"]["generator"])
                self.assertEqual(checkpoint["state"]["failed_candidates"], 0)
                self.assertEqual(checkpoint["state"]["infrastructure_failed_candidates"], 0)
                self.assertIsNotNone(checkpoint["spec"].get("simulator_model_profile_identity"))
                self.assertEqual(checkpoint["spec"]["simulator_model_profile_identity"],
                                 checkpoint["manifest"]["simulator_model_profile_identity"])

                resumed_instances = []
                resumed_root = os.path.join(self.fixture.root, backend + "_resumed")
                with mock.patch.object(runner, "PrivateNetlistOptimizer", fixtures.common_adapter(resumed_instances)), \
                        mock.patch.object(runner, "PersistentControllerProxy") as native, \
                        mock.patch.object(runner, "_evaluate_candidate") as legacy:
                    resumed = _coordinated_runner_resume(checkpoint["spec"], self.fixture.config,
                        self.fixture.prepared, resumed_root, checkpoint)
                    native.assert_not_called()
                    legacy.assert_not_called()
                calls = resumed_instances[0].calls
                self.assertEqual(len(calls), 7)
                self.assertEqual((calls[0][0], calls[0][1]), (0, 1))
                self.assertEqual(calls[0][2], cancelled["values"])
                self.assertNotIn((0, 0), [(generation, index) for generation, index, values in calls])
                self.assertEqual(resumed["state"], "completed")
                self.assertEqual(len(resumed["candidates"]), 8)
                self.assertTrue(all(row["status"] == "completed" for row in resumed["candidates"]))
                self.assertEqual([row["values"] for row in resumed["candidates"]],
                                 [row["values"] for row in baseline["candidates"]])
                final_checkpoint = load_json(os.path.join(resumed_root, "checkpoint.json"))
                self.assertEqual(final_checkpoint["state"]["generator"], baseline_checkpoint["state"]["generator"])


if __name__ == "__main__":
    unittest.main()
