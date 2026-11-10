import random
import time
import unittest

from crescendo_engine.multiobjective import (
    build_objective_presentation,
    global_nondominated_candidate_ids,
    objective_descriptors,
    objective_pair_descriptors,
    resolve_algorithm_policy,
)


def objective(output_key, aggregation="mean", corners=None, direction="minimize", name=None):
    row = {
        "output_key": output_key,
        "aggregation": aggregation,
        "corners": list(corners or []),
        "direction": direction,
    }
    if name:
        row["name"] = name
    return row


def candidate(candidate_id, scores, values=None, violation=0.0, generation=0, status="completed"):
    if values is None:
        values = list(scores)
    return {
        "candidate_id": candidate_id,
        "generation": generation,
        "status": status,
        "metrics": {
            "feasible": violation <= 0.0,
            "total_violation": violation,
            "objectives": [
                {"score": score, "value": value}
                for score, value in zip(scores, values)
            ],
        },
    }


class MultiobjectiveTest(unittest.TestCase):
    def test_single_objective_policy_permits_all_visible_algorithms(self):
        for algorithm in ("nsga2", "differential", "bayesian"):
            policy = resolve_algorithm_policy(algorithm, 1)
            self.assertFalse(policy["locked"])
            self.assertFalse(policy["coerced"])
            self.assertIn(policy["algorithm"], policy["allowed_algorithms"])
        self.assertEqual(
            resolve_algorithm_policy(None, 1)["algorithm"],
            "differential_evolution",
        )

    def test_multiobjective_policy_is_strict_or_explicitly_gui_coerced(self):
        for objective_count in (2, 3, 4):
            with self.assertRaisesRegex(ValueError, "incompatible"):
                resolve_algorithm_policy("bayesian", objective_count)
            policy = resolve_algorithm_policy("bayesian", objective_count, coerce=True)
            self.assertEqual(policy["algorithm"], "nsga2")
            self.assertTrue(policy["locked"])
            self.assertTrue(policy["coerced"])
            self.assertTrue(policy["notice"])

    def test_objective_identity_distinguishes_duplicate_output_definitions(self):
        definitions = [
            objective("vco::FRE", "min", ["Flow"], "minimize", "Low corner frequency"),
            objective("vco::FRE", "max", ["Fhigh"], "maximize", "High corner frequency"),
        ]
        descriptors = objective_descriptors(definitions)
        self.assertNotEqual(descriptors[0]["objective_id"], descriptors[1]["objective_id"])
        self.assertIn("vco::FRE", descriptors[0]["label"])
        self.assertIn("Flow", descriptors[0]["label"])
        self.assertIn("minimize", descriptors[0]["label"])
        self.assertIn("Fhigh", descriptors[1]["label"])
        self.assertIn("maximize", descriptors[1]["label"])

    def test_pair_count_covers_every_unordered_pair(self):
        for objective_count, expected_pairs in ((1, 0), (2, 1), (3, 3), (4, 6)):
            definitions = [objective("output_%d" % index) for index in range(objective_count)]
            pairs = objective_pair_descriptors(definitions)
            self.assertEqual(len(pairs), expected_pairs)
            indexes = set((pair["x_index"], pair["y_index"]) for pair in pairs)
            self.assertEqual(len(indexes), expected_pairs)
            self.assertTrue(all(left < right for left, right in indexes))

    def test_pareto_ids_are_global_full_dimensional_not_pairwise_fronts(self):
        rows = [
            candidate("best_xy", [0.0, 0.0, 10.0]),
            candidate("best_z", [1.0, 1.0, 0.0]),
            candidate("dominated", [2.0, 2.0, 20.0]),
        ]
        ids = global_nondominated_candidate_ids(rows, 3)
        self.assertEqual(ids, ["best_xy", "best_z"])
        presentation = build_objective_presentation(
            rows, [objective("x"), objective("y"), objective("z")],
        )
        self.assertEqual(presentation["pareto_candidate_ids"], ids)
        self.assertEqual(len(presentation["pairs"]), 3)
        self.assertNotIn("pareto_candidate_ids", presentation["pairs"][0])

    def test_infeasible_candidates_remain_visible_but_not_on_feasible_front(self):
        rows = [
            candidate("feasible", [5.0, 5.0], violation=0.0),
            candidate("infeasible", [0.0, 0.0], violation=0.1),
            candidate("failed", [0.0, 0.0], status="failed"),
        ]
        presentation = build_objective_presentation(
            rows, [objective("gain", direction="maximize"), objective("power")],
        )
        self.assertEqual(presentation["pareto_candidate_ids"], ["feasible"])
        points = dict((point["candidate_id"], point) for point in presentation["points"])
        self.assertEqual(set(points), set(("feasible", "infeasible")))
        self.assertTrue(points["feasible"]["feasible"])
        self.assertTrue(points["feasible"]["pareto"])
        self.assertFalse(points["infeasible"]["feasible"])
        self.assertFalse(points["infeasible"]["pareto"])

    def test_presentation_supports_one_through_four_objectives(self):
        for objective_count in (1, 2, 3, 4):
            definitions = [objective("metric_%d" % index) for index in range(objective_count)]
            presentation = build_objective_presentation(
                [candidate("c1", list(range(objective_count)))], definitions,
            )
            self.assertEqual(presentation["objective_count"], objective_count)
            self.assertEqual(len(presentation["objectives"]), objective_count)
            self.assertEqual(
                len(presentation["pairs"]), objective_count * (objective_count - 1) // 2,
            )
            self.assertEqual(presentation["pareto_candidate_ids"], ["c1"])

    def test_fast_front_matches_naive_constrained_semantics(self):
        randomizer = random.Random(17)
        for objective_count in range(1, 6):
            rows = [
                candidate(
                    "c%03d" % index,
                    [randomizer.randrange(0, 12) for _unused in range(objective_count)],
                    violation=float(randomizer.randrange(0, 3)),
                )
                for index in range(180)
            ]
            expected = []
            for row in rows:
                left = row["metrics"]
                dominated = False
                for other in rows:
                    if other is row:
                        continue
                    right = other["metrics"]
                    if right["total_violation"] != left["total_violation"]:
                        other_dominates = right["total_violation"] < left["total_violation"]
                    else:
                        other_scores = [item["score"] for item in right["objectives"]]
                        scores = [item["score"] for item in left["objectives"]]
                        other_dominates = (
                            all(a <= b for a, b in zip(other_scores, scores)) and
                            any(a < b for a, b in zip(other_scores, scores))
                        )
                    if other_dominates:
                        dominated = True
                        break
                if not dominated:
                    expected.append(row["candidate_id"])
            self.assertEqual(
                global_nondominated_candidate_ids(rows, objective_count), expected,
            )

    def test_two_objective_large_front_is_interactive(self):
        rows = [
            candidate("c%05d" % index, [float(index), float(19999 - index)])
            for index in range(20000)
        ]
        started = time.time()
        result = global_nondominated_candidate_ids(rows, 2)
        elapsed = time.time() - started
        self.assertEqual(len(result), len(rows))
        self.assertLess(elapsed, 3.0)


if __name__ == "__main__":
    unittest.main()
