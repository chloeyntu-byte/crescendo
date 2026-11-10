import unittest

from crescendo_engine.metrics import constraint_violation, evaluate_metrics, nondominated_front, objective_score, select_representative


class MetricsTest(unittest.TestCase):
    def test_unscaled_objective_score_is_not_normalized_by_candidate_value(self):
        lower_fom = objective_score(174.79, {"direction": "maximize", "target": 999.0})
        higher_fom = objective_score(187.17, {"direction": "maximize", "target": 999.0})
        self.assertEqual(lower_fom, -174.79)
        self.assertEqual(higher_fom, -187.17)
        self.assertLess(higher_fom, lower_fom)

    def test_explicit_objective_scale_is_fixed(self):
        definition = {"direction": "maximize", "scale": 100.0}
        self.assertEqual(objective_score(50.0, definition), -0.5)
        self.assertEqual(objective_score(80.0, definition), -0.8)

    def test_small_engineering_constraint_uses_limit_as_scale(self):
        self.assertAlmostEqual(constraint_violation(200e-6, {"operator": "<=", "limit": 100e-6}), 1.0)
        self.assertAlmostEqual(constraint_violation(50e-9, {"operator": ">=", "limit": 100e-9}), 0.5)

    def test_multiobjective_representative_uses_normalized_ideal_distance(self):
        rows = []
        for candidate_id, scores in (("left", [0.0, 1000000.0]), ("knee", [4.0, 400000.0]), ("right", [10.0, 0.0])):
            rows.append({"candidate_id": candidate_id, "metrics": {
                "total_violation": 0.0,
                "objectives": [{"score": score} for score in scores],
            }})
        representative, distance = select_representative(rows)
        self.assertEqual(representative["candidate_id"], "knee")
        self.assertGreater(distance, 0.0)

    def test_objectives_constraints_and_front(self):
        parsed = {"output_map": {
            "t::gain": {"summary": {"mean": 10.0}},
            "t::power": {"summary": {"mean": 2.0}},
        }}
        metrics = evaluate_metrics(parsed, [
            {"output_key": "t::gain", "direction": "maximize", "aggregation": "mean"},
            {"output_key": "t::power", "direction": "minimize", "aggregation": "mean"},
        ], [{"output_key": "t::power", "operator": "<=", "limit": 3.0, "aggregation": "mean"}])
        self.assertTrue(metrics["feasible"])
        self.assertEqual(metrics["total_violation"], 0.0)
        stronger = {"metrics": metrics}
        weaker_metrics = dict(metrics)
        weaker_metrics["objectives"] = [dict(metrics["objectives"][0], score=0.0), dict(metrics["objectives"][1], score=3.0)]
        weaker = {"metrics": weaker_metrics}
        self.assertEqual(nondominated_front([stronger, weaker]), [stronger])

    def test_statistics_preserve_selected_expanded_corners_and_distinct_scopes(self):
        parsed = {"output_map": {"t::gain": {"values": [
            {"corner": "PVT_0", "source_corner": "PVT", "numeric": 2.0},
            {"corner": "PVT_1", "source_corner": "PVT", "numeric": 8.0},
            {"corner": "TT", "numeric": 100.0},
        ]}}}
        metrics = evaluate_metrics(parsed,
            [{"output_key": "t::gain", "aggregation": "mean", "corners": ["PVT"]}],
            [{"output_key": "t::gain", "aggregation": "max", "corners": ["TT"],
              "operator": "<=", "limit": 200}])
        statistics = metrics["selected_output_statistics"]
        self.assertEqual(len(statistics), 2)
        self.assertEqual((statistics[0]["min"], statistics[0]["mean"], statistics[0]["max"]), (2.0, 5.0, 8.0))
        self.assertEqual(statistics[0]["finite_count"], 2)
        self.assertEqual(statistics[1]["point_count"], 1)
        self.assertEqual(statistics[1]["mean"], 100.0)
        self.assertEqual(metrics["objectives"][0]["value"], 5.0)

    def test_summary_only_statistics_do_not_fabricate_ranges_or_point_counts(self):
        metrics = evaluate_metrics({"output_map": {"t::gain": {"summary": {"mean": 7.0}}}},
            [{"output_key": "t::gain", "aggregation": "mean"}], [])
        statistics = metrics["selected_output_statistics"][0]
        self.assertEqual(statistics["mean"], 7.0)
        self.assertIsNone(statistics["min"])
        self.assertIsNone(statistics["point_count"])
        self.assertEqual(statistics["status"], "PARTIAL")

    def test_each_metric_uses_its_selected_corners(self):
        parsed = {"output_map": {"vco::FRE": {"values": [
            {"corner": "Flow", "numeric": 4.8e9},
            {"corner": "Fhigh", "numeric": 5.3e9},
        ]}, "vco::FoM": {"values": [
            {"corner": "Flow", "numeric": 180.0},
            {"corner": "Fhigh", "numeric": 185.0},
        ]}}}
        metrics = evaluate_metrics(parsed, [
            {"output_key": "vco::FoM", "direction": "maximize", "aggregation": "mean", "corners": ["Flow", "Fhigh"]},
        ], [
            {"output_key": "vco::FRE", "operator": "<=", "limit": 5e9, "aggregation": "mean", "corners": ["Flow"]},
            {"output_key": "vco::FRE", "operator": ">=", "limit": 5e9, "aggregation": "mean", "corners": ["Fhigh"]},
        ])
        self.assertEqual(metrics["objectives"][0]["value"], 182.5)
        self.assertEqual(metrics["objectives"][0]["corners"], ["Flow", "Fhigh"])
        self.assertTrue(metrics["feasible"])
