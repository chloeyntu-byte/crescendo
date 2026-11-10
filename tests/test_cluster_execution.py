import copy
import unittest

from crescendo_engine.cluster_execution import (
    batch_timeout_seconds, candidate_batches, candidate_execution_plan,
    slice_candidate_spectre_timing,
    serial_timeout_seconds,
)
from crescendo_engine.rdb import aggregate_output, slice_candidate_rdb


class ClusterExecutionTests(unittest.TestCase):
    def policy(self, jobs):
        return {"job_policy": {"available": True, "global": {"properties": {
            "distributionmethod": "Command", "maxjobs": str(jobs),
        }}}}

    def test_job_capacity_never_changes_population_or_pads_candidates(self):
        planned = [(i, {"w": i + 1}) for i in range(5)]
        for jobs in (1, 2, 5, 40):
            spec = {"maestro_candidate_mode": "batch"}
            plan = candidate_execution_plan(spec, self.policy(jobs), {})
            batches = candidate_batches(planned, plan["candidate_limit"])
            self.assertEqual([item for batch in batches for item in batch], planned)
            self.assertEqual(len(batches), 1)
            self.assertEqual(plan["max_jobs"], jobs)

    def test_explicit_command_can_select_cluster_when_original_policy_was_local(self):
        prepared = self.policy(1)
        prepared["job_policy"]["global"]["properties"]["distributionmethod"] = "Local"
        plan = candidate_execution_plan({"maestro_job_overrides": {
            "command": 'batch-submit -R "cpu=6;mem=30000"', "max_jobs": 20,
        }}, prepared, {})
        self.assertTrue(plan["enabled"])
        self.assertEqual(plan["max_jobs"], 20)

    def test_local_and_unknown_policies_preserve_serial_auto(self):
        for prepared in ({}, {"job_policy": {"global": {"properties": {
                "distributionmethod": "Local", "maxjobs": "40"}}}}):
            self.assertFalse(candidate_execution_plan({}, prepared, {})["enabled"])

    def test_candidate_window_preserves_order_and_last_partial_batch(self):
        self.assertEqual(candidate_batches(list(range(5)), 2), [[0, 1], [2, 3], [4]])

    def test_queue_and_worst_case_serial_capacity_have_bounded_budget(self):
        spec = {"candidate_timeout_seconds": 100}
        config = {"runtime": {"cluster_queue_timeout_seconds": 500}}
        small = batch_timeout_seconds(2, spec, config)
        large = batch_timeout_seconds(20, spec, config)
        self.assertEqual(large - small, 1800)
        self.assertGreater(small, 500 + 200)

    def test_wrapper_infrastructure_failure_does_not_contaminate_another_candidate(self):
        summary = {"unclosed_start_count": 1, "runs": [
            {"cwd": "result/1/a_test", "returncode": 0, "elapsed_seconds": 10},
            {"cwd": "result/1/b_test", "returncode": 127, "elapsed_seconds": 1},
            {"cwd": "result/1/a_test_extra", "returncode": 139, "elapsed_seconds": 2},
        ]}
        a = slice_candidate_spectre_timing(summary, ["a_test"])
        b = slice_candidate_spectre_timing(summary, ["b_test"])
        self.assertEqual([row["returncode"] for row in a["runs"]], [0])
        self.assertEqual([row["returncode"] for row in b["runs"]], [127])
        self.assertEqual(a["shared_unclosed_start_count"], 1)
        self.assertEqual(a["unclosed_start_count"], 0)

    def test_serial_cluster_fallback_keeps_queue_budget_and_local_deadline(self):
        spec = {"candidate_timeout_seconds": 100}
        self.assertEqual(serial_timeout_seconds(spec, {"runtime": {}}), 100)
        self.assertEqual(serial_timeout_seconds(spec, {"runtime": {
            "native_cluster_stop_required": True, "cluster_queue_timeout_seconds": 500}}), 600)
        with self.assertRaises(ValueError):
            serial_timeout_seconds(spec, {"runtime": {
                "native_cluster_stop_required": True, "cluster_queue_timeout_seconds": float("inf")}})


class CandidateRdbIsolationTests(unittest.TestCase):
    def parsed(self):
        def output(test, values):
            return {"test": test, "name": "gain", "key": test + "::gain",
                    "values": [{"point_id": i + 1, "numeric": v, "corner": c, "error": None}
                               for i, (v, c) in enumerate(zip(values, ("FF", "SS")))],
                    "summary": {"mean": -9999}, "scalar": True}
        outputs = [output("a_test", [10, 20]), output("b_test", [100, 200])]
        return {"path": "shared.rdb", "outputs": outputs,
                "output_map": dict((row["key"], row) for row in outputs),
                "points": [{"pointID": 1}, {"pointID": 2}, {"pointID": 99}],
                "parameters": [{"pointID": 1}, {"pointID": 99}],
                "test_statuses": [{"pointID": i, "test_name": t, "statusCode": 3}
                                  for t in ("a_test", "b_test") for i in (1, 2)]}

    def test_two_candidates_sharing_point_ids_are_isolated_by_test(self):
        parsed = self.parsed()
        original = copy.deepcopy(parsed)
        a = slice_candidate_rdb(parsed, {"a_test": "orig"})
        b = slice_candidate_rdb(parsed, {"b_test": "orig"})
        self.assertEqual(aggregate_output(a, "orig::gain", "mean"), 15)
        self.assertEqual(aggregate_output(b, "orig::gain", "mean"), 150)
        self.assertEqual(len(a["test_statuses"]), 2)
        self.assertEqual(len(a["points"]), 2)
        self.assertEqual(a["test_statuses"][0]["physical_test"], "a_test")
        self.assertEqual(parsed, original)

    def test_missing_or_failed_point_remains_visible_in_candidate_slice(self):
        parsed = self.parsed()
        parsed["outputs"][0]["values"][1].update(numeric=None, error="Spectre failed")
        a = slice_candidate_rdb(parsed, {"a_test": "orig"})
        b = slice_candidate_rdb(parsed, {"b_test": "orig"})
        self.assertFalse(a["outputs"][0]["scalar"])
        self.assertEqual(a["outputs"][0]["values"][1]["error"], "Spectre failed")
        self.assertEqual(aggregate_output(b, "orig::gain", "mean"), 150)

    def test_optional_point_filter_recomputes_summaries(self):
        result = slice_candidate_rdb(self.parsed(), {"a_test": "orig"}, [2])
        self.assertEqual(aggregate_output(result, "orig::gain", "mean"), 20)
        self.assertEqual(result["points"], [{"pointID": 2}])

    def test_ambiguous_maps_are_rejected(self):
        with self.assertRaises(ValueError):
            slice_candidate_rdb(self.parsed(), {"a_test": "orig", "b_test": "orig"})


if __name__ == "__main__":
    unittest.main()
