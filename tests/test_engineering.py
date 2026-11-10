import math
import unittest

from crescendo_engine.engineering import (
    SUFFIX_POLICY, merge_catalog_domain, normalize_constraint_definition,
    normalize_metric_definitions, normalize_objective_definition,
    parse_engineering_number,
)
from crescendo_engine.metrics import constraint_violation, objective_score
from crescendo_engine.optimizers import (
    PopulationGenerator, validate_candidate_values, validate_variables,
)


class EngineeringTest(unittest.TestCase):
    def test_spectre_suffixes_and_scientific_notation(self):
        expected = {
            "2T": 2e12,
            "3g": 3e9,
            "4MEG": 4e6,
            "5k": 5e3,
            "6m": 6e-3,
            "7M": 7e-3,
            "8u": 8e-6,
            "9n": 9e-9,
            "10p": 10e-12,
            "11f": 11e-15,
            "12a": 12e-18,
            "5e9": 5e9,
            "2.5e-3 k": 2.5,
        }
        for text, value in expected.items():
            self.assertAlmostEqual(parse_engineering_number(text), value, msg=text)
        self.assertIn("M means milli", SUFFIX_POLICY)

    def test_invalid_numbers_are_rejected_readably(self):
        for value in (None, True, "", "  ", "nan", "NaN", "inf", "1x", "1mm"):
            with self.assertRaises(ValueError, msg=repr(value)):
                parse_engineering_number(value, "constraint limit")
        with self.assertRaisesRegex(ValueError, "unknown engineering suffix"):
            parse_engineering_number("3xyz", "minimum")
        with self.assertRaisesRegex(ValueError, "finite"):
            parse_engineering_number(float("nan"), "maximum")
        with self.assertRaisesRegex(ValueError, "finite"):
            parse_engineering_number(float("inf"), "maximum")

    def test_metric_limits_and_scales_accept_engineering_values(self):
        objective = normalize_objective_definition({"direction": "maximize", "scale": "100meg"})
        constraint = normalize_constraint_definition({"operator": "<=", "limit": "100u"})
        self.assertEqual(objective["scale"], 100e6)
        self.assertAlmostEqual(constraint["limit"], 100e-6)
        self.assertAlmostEqual(objective_score(50e6, objective), -0.5)
        self.assertAlmostEqual(constraint_violation(150e-6, constraint), 0.5)
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            normalize_constraint_definition({"operator": ">=", "limit": ""})
        with self.assertRaisesRegex(ValueError, "must be finite"):
            normalize_objective_definition({"direction": "maximize", "scale": math.inf})

    def test_all_constraint_numeric_fields_are_normalized(self):
        objectives, constraints = normalize_metric_definitions(
            [{"direction": "target", "target": "1G", "scale": "10meg"}],
            [
                {"operator": "between", "lower": "20n", "upper": "50n"},
                {"operator": "target", "target": "1m", "tolerance": "5u"},
            ],
        )
        self.assertEqual(objectives[0]["target"], 1e9)
        self.assertAlmostEqual(constraints[0]["lower"], 20e-9)
        self.assertAlmostEqual(constraints[0]["upper"], 50e-9)
        self.assertAlmostEqual(constraints[1]["target"], 1e-3)
        self.assertAlmostEqual(constraints[1]["tolerance"], 5e-6)

    def test_catalog_domain_merge_does_not_guess_missing_metadata(self):
        plain = merge_catalog_domain(
            {"parameter_id": "w", "minimum": "20n", "maximum": "50n"},
            {"parameter_id": "w", "value_type": "float"},
        )
        self.assertNotIn("legal_minimum", plain)
        self.assertNotIn("legal_maximum", plain)
        self.assertNotIn("step", plain)
        bounded = merge_catalog_domain(
            {"parameter_id": "nf", "minimum": 2, "maximum": 10},
            {"value_type": "integer", "minimum": 1, "maximum": 12, "increment": 2},
        )
        self.assertEqual(bounded["legal_minimum"], 1)
        self.assertEqual(bounded["legal_maximum"], 12)
        self.assertEqual(bounded["step"], 2)
        enum = merge_catalog_domain(
            {"parameter_id": "mode"},
            {"choices_raw": '("low" "normal" "high")'},
        )
        self.assertEqual(enum["value_type"], "enum")
        self.assertEqual(enum["choices"], ["low", "normal", "high"])
        with self.assertRaisesRegex(ValueError, "PDK adapter"):
            merge_catalog_domain(
                {"parameter_id": "mode"},
                {"choices_raw": '(("Low" "low") ("High" "high"))'},
            )

    def test_cdf_legal_bounds_are_enforced(self):
        variable = {
            "parameter_id": "w",
            "minimum": "20n",
            "maximum": "50n",
            "legal_minimum": "10n",
            "legal_maximum": "100n",
        }
        normalized = validate_variables([variable])[0]
        self.assertAlmostEqual(normalized["minimum"], 20e-9)
        self.assertAlmostEqual(normalized["maximum"], 50e-9)
        with self.assertRaisesRegex(ValueError, "below CDF legal minimum"):
            validate_variables([dict(variable, minimum="5n")])
        with self.assertRaisesRegex(ValueError, "exceeds CDF legal maximum"):
            validate_variables([dict(variable, maximum="200n")])

    def test_numeric_cyclic_dimensions_honor_requested_interval(self):
        parameter = {"choices_raw": '("2u" "5u" "10u" "20u")'}
        merged = merge_catalog_domain({"parameter_id": "w", "minimum": "4u", "maximum": "12u"}, parameter)
        self.assertEqual(merged["choices"], ["5u", "10u"])
        variables = validate_variables([merged])
        candidates = PopulationGenerator(variables, "random", seed=7).ask(10)
        self.assertTrue(all(candidate["w"] in ("5u", "10u") for candidate in candidates))
        with self.assertRaisesRegex(ValueError, "at least two choices"):
            validate_variables([merge_catalog_domain(
                {"parameter_id": "w", "minimum": "6u", "maximum": "12u"}, parameter)])

    def test_cdf_grid_rejects_shifted_dimensions(self):
        parameter = {"minimum": "60n", "maximum": "200n", "step": "10n"}
        good = merge_catalog_domain({"parameter_id": "l", "minimum": "70n", "maximum": "100n"}, parameter)
        validate_variables([good])
        shifted = merge_catalog_domain({"parameter_id": "l", "minimum": "75n", "maximum": "105n"}, parameter)
        with self.assertRaisesRegex(ValueError, "CDF grid origin"):
            validate_variables([shifted])

    def test_numeric_choices_intersect_cdf_bounds_and_grid(self):
        parameter = {
            "value_type": "float", "minimum": "20n", "maximum": "200n", "step": "30n",
            "choices_raw": ["5n", "20n", "35n", "50N", "80n", "110n", "200n", "250n"],
        }
        merged = merge_catalog_domain(
            {"parameter_id": "l", "minimum": "10n", "maximum": "120n"}, parameter,
        )
        self.assertEqual(merged["choices"], ["20n", "50N", "80n", "110n"])
        variables = validate_variables([merged])
        candidates = PopulationGenerator(variables, "random", seed=19).ask(40)
        self.assertEqual(set(candidate["l"] for candidate in candidates), set(merged["choices"]))
        for value in ("5n", "35n", "200n", "250n"):
            with self.assertRaisesRegex(ValueError, "legal enum choice"):
                validate_candidate_values({"l": value}, variables)

    def test_numeric_choices_keep_declared_integer_type(self):
        merged = merge_catalog_domain(
            {"parameter_id": "nf", "minimum": 1, "maximum": 3, "value_type": "float"},
            {"value_type": "integer", "choices_raw": ["1.0", "1.5", "2e0", "2.5", "3"]},
        )
        self.assertEqual(merged["numeric_value_type"], "integer")
        self.assertEqual(merged["choices"], ["1.0", "2e0", "3"])
        variables = validate_variables([merged])
        with self.assertRaisesRegex(ValueError, "legal enum choice"):
            validate_candidate_values({"nf": "1.5"}, variables)

    def test_choices_only_domain_does_not_invent_limits_or_grid(self):
        choices = ["1.5", "2.5", "4.5"]
        merged = merge_catalog_domain({"parameter_id": "turns"}, {"choices_raw": choices})
        self.assertEqual(merged["choices"], choices)
        for key in ("minimum", "maximum", "legal_minimum", "legal_maximum", "step", "grid_origin",
                    "numeric_value_type"):
            self.assertNotIn(key, merged)
        self.assertEqual(validate_variables([merged])[0]["choices"], choices)

    def test_cyclic_cdf_constraints_can_leave_no_optimization_domain(self):
        for parameter in (
                {"minimum": "10n", "maximum": "20n", "choices_raw": ["5n", "25n"]},
                {"value_type": "integer", "choices_raw": ["1.5", "2.5"]},
                {"step": "10n", "choices_raw": ["5n", "15n"]}):
            with self.subTest(parameter=parameter), self.assertRaisesRegex(ValueError, "at least two choices"):
                validate_variables([merge_catalog_domain({"parameter_id": "x"}, parameter)])

    def test_numeric_choices_reject_invalid_declared_metadata(self):
        for metadata, message in (
                ({"minimum": "20n", "maximum": "10n"}, "CDF legal minimum exceeds"),
                ({"step": "0"}, "step must be positive"),
                ({"step": "bad"}, "invalid numeric syntax")):
            with self.subTest(metadata=metadata), self.assertRaisesRegex(ValueError, message):
                merge_catalog_domain({"parameter_id": "l"}, dict(metadata, choices_raw=["10n", "20n"]))

    def test_tiny_cdf_choice_limits_do_not_borrow_tolerance_from_wide_ui_range(self):
        merged = merge_catalog_domain(
            {"parameter_id": "c", "minimum": 0, "maximum": 1},
            {"minimum": "1f", "maximum": "3f", "choices_raw": ["0.01f", "1f", "2f", "4f"]},
        )
        self.assertEqual(merged["choices"], ["1f", "2f"])

    def test_subpicofarad_candidates_cannot_escape_bounds(self):
        variable = [{"parameter_id": "c", "minimum": "1f", "maximum": "10f"}]
        with self.assertRaisesRegex(ValueError, "outside optimization bounds"):
            validate_candidate_values({"c": 100e-15}, variable)

    def test_integer_enum_and_step_domains_reach_optimizer_candidates(self):
        variables = validate_variables([
            {"parameter_id": "nf", "minimum": 2, "maximum": 10, "value_type": "integer", "step": 2},
            {"parameter_id": "mode", "value_type": "enum", "choices": ["low", "normal", "high"]},
            {"parameter_id": "bias", "minimum": "10u", "maximum": "50u", "step": "5u"},
        ])
        generator = PopulationGenerator(variables, "random", seed=23)
        candidates = generator.ask(50)
        for candidate in candidates:
            self.assertIn(candidate["nf"], (2, 4, 6, 8, 10))
            self.assertIn(candidate["mode"], ("low", "normal", "high"))
            offset = (candidate["bias"] - 10e-6) / 5e-6
            self.assertAlmostEqual(offset, round(offset))
            self.assertEqual(validate_candidate_values(candidate, variables), candidate)

    def test_invalid_discrete_candidates_and_domains_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "at least two choices"):
            validate_variables([{"parameter_id": "mode", "value_type": "enum", "choices": ["only"]}])
        with self.assertRaisesRegex(ValueError, "align to its step"):
            validate_variables([{"parameter_id": "x", "minimum": 0, "maximum": 10, "step": 3}])
        integer = [{"parameter_id": "nf", "minimum": 1, "maximum": 5, "value_type": "integer"}]
        with self.assertRaisesRegex(ValueError, "must be an integer"):
            validate_candidate_values({"nf": 2.5}, integer)
        enum = [{"parameter_id": "mode", "value_type": "enum", "choices": ["a", "b"]}]
        with self.assertRaisesRegex(ValueError, "legal enum choice"):
            validate_candidate_values({"mode": "c"}, enum)


if __name__ == "__main__":
    unittest.main()
