import unittest

from crescendo_engine.optimizers import (
    PopulationGenerator, _environmental_select, validate_candidate_values, validate_variables,
)


VARIABLES = [{"parameter_id": "x", "minimum": 0.1, "maximum": 10.0, "scale": "log"}]


def evaluated(values):
    return {"status": "completed", "values": values, "metrics": {"total_violation": 0.0, "merit": values["x"], "objectives": [{"score": values["x"]}]}}


def scored(values, scores, candidate_id=""):
    return {"candidate_id": candidate_id, "status": "completed", "values": values, "metrics": {
        "total_violation": 0.0,
        "merit": sum(scores),
        "objectives": [{"score": score} for score in scores],
    }}


class OptimizersTest(unittest.TestCase):
    def test_integer_bounds_must_be_discrete_and_non_degenerate(self):
        with self.assertRaisesRegex(ValueError, "whole numbers"):
            validate_variables([{"parameter_id": "nf", "minimum": 1.0, "maximum": 2.1, "value_type": "integer"}])

    def test_numeric_enum_validation_enforces_constraints_without_catalog_merge(self):
        variable = {
            "parameter_id": "nf", "value_type": "enum", "numeric_value_type": "integer",
            "choices": ["0", "1", "1.5", "2", "3", "4", "5", "7"],
            "minimum": 0, "maximum": 6, "legal_minimum": 1, "legal_maximum": 5,
            "step": 2, "grid_origin": 1,
        }
        variables = validate_variables([variable])
        self.assertEqual(variables[0]["choices"], ["1", "3", "5"])
        self.assertEqual(variable["choices"], ["0", "1", "1.5", "2", "3", "4", "5", "7"])
        self.assertEqual(validate_variables(variables), variables)
        candidates = PopulationGenerator(variables, "random", seed=7).ask(50)
        self.assertEqual(set(candidate["nf"] for candidate in candidates), {"1", "3", "5"})
        for value in ("0", "1.5", "2", "4", "7"):
            with self.assertRaisesRegex(ValueError, "legal enum choice"):
                validate_candidate_values({"nf": value}, variables)

    def test_numeric_enum_validation_rejects_invalid_constraints(self):
        variable = {"parameter_id": "l", "value_type": "enum", "choices": ["10n", "20n", "30n"]}
        for metadata, message in (
                ({"minimum": "bad", "maximum": "30n"}, "invalid numeric syntax"),
                ({"minimum": "30n", "maximum": "10n"}, "minimum must be less"),
                ({"legal_minimum": "30n", "legal_maximum": "10n"}, "CDF legal minimum exceeds"),
                ({"step": -1}, "step must be positive"),
                ({"numeric_choices": True, "choices": ["10n", "not numeric"]}, "must be finite numbers")):
            with self.subTest(metadata=metadata), self.assertRaisesRegex(ValueError, message):
                validate_variables([dict(variable, **metadata)])

    def test_numeric_enum_validation_requires_two_choices_after_constraints(self):
        with self.assertRaisesRegex(ValueError, "at least two choices"):
            validate_variables([{
                "parameter_id": "c", "value_type": "enum", "choices": ["1f", "2f", "4f"],
                "legal_minimum": "2f", "legal_maximum": "3f",
            }])

    def test_all_algorithms_respect_bounds(self):
        for algorithm in ("random", "differential_evolution", "nsga2", "bayesian_optimization"):
            generator = PopulationGenerator(VARIABLES, algorithm, seed=7)
            first = generator.ask(4)
            generator.tell([evaluated(row) for row in first])
            second = generator.ask(4)
            self.assertEqual(len(second), 4)
            self.assertTrue(all(0.1 <= row["x"] <= 10.0 for row in second))

    def test_algorithm_parameters_change_nsga_and_de_sampling_deterministically(self):
        variables = [{"parameter_id": "x%d" % index, "minimum": 0.0, "maximum": 1.0} for index in range(4)]
        for algorithm, left_parameters, right_parameters in (
                ("nsga2", {"crossover_probability": 0.0, "mutation_probability": 0.0},
                 {"crossover_probability": 1.0, "mutation_probability": 1.0, "crossover_eta": 3.0, "mutation_eta": 4.0}),
                ("differential_evolution", {"differential_weight": 0.2, "crossover_rate": 0.0},
                 {"differential_weight": 1.5, "crossover_rate": 1.0}),
        ):
            left = PopulationGenerator(variables, algorithm, seed=13, parameters=left_parameters)
            right = PopulationGenerator(variables, algorithm, seed=13, parameters=right_parameters)
            initial = left.ask(8)
            self.assertEqual(initial, right.ask(8))
            rows = [scored(values, [sum(values.values())], "p%d" % index) for index, values in enumerate(initial)]
            left.tell(rows)
            right.tell(rows)
            self.assertNotEqual(left.ask(8), right.ask(8))

    def test_bayesian_surrogate_is_deterministic_and_caps_observations(self):
        variables = [{"parameter_id": "x%d" % index, "minimum": 0.0, "maximum": 1.0} for index in range(3)]
        parameters = {"initial_samples": 4, "candidate_pool": 24, "acquisition_exploration": 1.7,
                      "noise": 1.0e-5, "kernel_length_scale": 0.4, "max_observations": 6}
        left = PopulationGenerator(variables, "bayesian_optimization", seed=5, parameters=parameters)
        right = PopulationGenerator(variables, "bayesian_optimization", seed=5, parameters=parameters)
        for generation in range(4):
            left_values = left.ask(4)
            right_values = right.ask(4)
            self.assertEqual(left_values, right_values)
            rows = [scored(values, [sum(value * value for value in values.values())],
                           "g%d_c%d" % (generation, index)) for index, values in enumerate(left_values)]
            left.tell(rows)
            right.tell(rows)
            self.assertLessEqual(len(left.previous), 6)
            self.assertTrue(all(0.0 <= value <= 1.0 for row in left_values for value in row.values()))

    def test_failed_generation_retains_previous_population(self):
        generator = PopulationGenerator(VARIABLES, "differential_evolution", seed=9)
        first = generator.ask(4)
        generator.tell([evaluated(row) for row in first])
        self.assertFalse(generator.tell([{"status": "failed"}]))
        self.assertEqual(len(generator.ask(4)), 4)

    def test_failed_initial_generation_resamples_without_aborting(self):
        generator = PopulationGenerator(VARIABLES, "nsga2", seed=11)
        first = generator.ask(4)
        self.assertFalse(generator.tell([{"status": "failed", "values": row} for row in first]))
        second = generator.ask(4)
        self.assertEqual(len(second), 4)
        self.assertNotEqual(first, second)

    def test_nsga2_elitism_retains_better_parent(self):
        generator = PopulationGenerator([{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}], "nsga2", seed=4)
        initial = generator.ask(6)
        parents = [scored(values, [float(index)], "p%d" % index) for index, values in enumerate(initial)]
        generator.tell(parents)
        children = generator.ask(6)
        generator.tell([scored(values, [100.0 + index], "c%d" % index) for index, values in enumerate(children)])
        self.assertIn(parents[0], generator.previous)
        self.assertNotIn(100.0, [row["metrics"]["objectives"][0]["score"] for row in generator.previous])

    def test_nsga2_crowding_preserves_pareto_extremes(self):
        rows = [scored({"x": float(index)}, [float(index), float(10 - index)], "r%d" % index) for index in (0, 2, 5, 8, 10)]
        selected = _environmental_select(rows, 3)
        selected_ids = set(row["candidate_id"] for row in selected)
        self.assertIn("r0", selected_ids)
        self.assertIn("r10", selected_ids)

    def test_de_rejects_worse_trials(self):
        variables = [{"parameter_id": "x", "minimum": 0.0, "maximum": 1.0}]
        generator = PopulationGenerator(variables, "differential_evolution", seed=8)
        initial = generator.ask(6)
        parents = [scored(values, [float(index)], "p%d" % index) for index, values in enumerate(initial)]
        generator.tell(parents)
        trials = generator.ask(6)
        generator.tell([scored(values, [100.0 + index], "t%d" % index) for index, values in enumerate(trials)])
        self.assertEqual(set(row["candidate_id"] for row in generator.previous), set("p%d" % index for index in range(6)))

    def test_de_duplicate_integer_trials_keep_fifo_target_pairing(self):
        variables = [{"parameter_id": "nf", "minimum": 1, "maximum": 2, "value_type": "integer"}]
        generator = PopulationGenerator(variables, "differential_evolution", seed=3)
        initial = generator.ask(8)
        parents = [scored(values, [float(index)], "p%d" % index) for index, values in enumerate(initial)]
        generator.tell(parents)
        trials = generator.ask(8)
        self.assertLess(len(set(values["nf"] for values in trials)), len(trials))
        trial_rows = []
        for index, values in enumerate(trials):
            score = float(index) - 1.0 if index % 2 == 0 else float(index) + 100.0
            trial_rows.append(scored(values, [score], "t%d" % index))
        generator.tell(list(reversed(trial_rows)))
        expected = set("t%d" % index if index % 2 == 0 else "p%d" % index for index in range(8))
        self.assertEqual(set(row["candidate_id"] for row in generator.previous), expected)
        self.assertEqual(generator.unmatched_trials, [])

    def test_integer_variables_and_high_dimensional_runs_are_deterministic(self):
        for algorithm in ("nsga2", "differential_evolution"):
            for dimensions in (5, 50):
                variables = [{
                    "parameter_id": "x%d" % index,
                    "minimum": 1 if index % 3 == 0 else 0.0,
                    "maximum": 20 if index % 3 == 0 else 1.0,
                    "value_type": "integer" if index % 3 == 0 else "float",
                } for index in range(dimensions)]
                left = PopulationGenerator(variables, algorithm, seed=17)
                right = PopulationGenerator(variables, algorithm, seed=17)
                for generation in range(10):
                    left_values = left.ask(20)
                    right_values = right.ask(20)
                    self.assertEqual(left_values, right_values)
                    integer_keys = [variable["parameter_id"] for variable in variables if variable["value_type"] == "integer"]
                    self.assertTrue(all(isinstance(values[key], int) for values in left_values for key in integer_keys))
                    left_rows = [scored(values, [sum(float(value) ** 2 for value in values.values())], "g%d_c%d" % (generation, index)) for index, values in enumerate(left_values)]
                    right_rows = [scored(values, [sum(float(value) ** 2 for value in values.values())], "g%d_c%d" % (generation, index)) for index, values in enumerate(right_values)]
                    left.tell(left_rows)
                    right.tell(right_rows)
                self.assertEqual([row["values"] for row in left.previous], [row["values"] for row in right.previous])

    def test_penalized_failed_candidate_is_eligible_but_cannot_displace_feasible_parent(self):
        generator = PopulationGenerator(VARIABLES, "nsga2", seed=19)
        initial = generator.ask(4)
        parents = [evaluated(values) for values in initial]
        generator.tell(parents)
        failed_values = generator.ask(4)
        failed = [{
            "status": "failed",
            "values": values,
            "metrics": {"total_violation": 1.0e6, "merit": 1.0e30, "objectives": [{"score": 1.0e30}]},
        } for values in failed_values]
        self.assertFalse(generator.tell(failed))
        self.assertTrue(all(row["status"] == "completed" for row in generator.previous))
