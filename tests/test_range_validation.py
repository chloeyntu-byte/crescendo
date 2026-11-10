import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine.range_validation import (
    VariableRangeValidationError, _endpoint_rows, run_range_validation,
    validate_variable_request, validate_variable_request_report,
)


class RangeValidationTest(unittest.TestCase):
    def test_endpoint_rows_preserve_requested_and_effective_adjustment(self):
        parameters = [{"parameter_id": "w", "backend": "schematic"}]
        patch = {"schematic_verification": {"parameters": [{
            "parameter_id": "w", "cdf_value": "8u", "schematic_value": "8u",
            "callback_status": "ok", "callback_expression": "limit()",
        }]}}
        rows = _endpoint_rows(parameters, "maximum", {"w": "20u"}, patch)
        self.assertEqual(rows[0]["status"], "pdk_adjusted")
        self.assertEqual(rows[0]["requested"], "20u")
        self.assertEqual(rows[0]["effective"], "8u")

    def test_request_validation_does_not_mutate_ranges(self):
        prepared = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
        variables = [{"parameter_id": "w", "minimum": 1e-6, "maximum": 8e-6}]
        normalized = validate_variable_request(prepared, variables)
        self.assertEqual(normalized, variables)

    def test_cyclic_dimensions_validate_allowed_extrema(self):
        with tempfile.TemporaryDirectory() as root:
            prepared = {"parameters": [{"parameter_id": "w", "optimizable": True,
                "choices_raw": '("2u" "5u" "10u" "20u")'}], "setup": {}}
            variables = [{"parameter_id": "w", "minimum": "4u", "maximum": "12u"}]
            requested = []
            def generate(_prepared, _parameters, values, *_args, **_kwargs):
                requested.append(values["w"])
                return {"parameter_application": {"ok": True, "proof_kind": "native_run_device_parameters",
                    "parameters": [{"parameter_id": "w", "points": [{"effective": values["w"],
                                      "callback_adjustment_proven": True}]}]}}
            with mock.patch("crescendo_engine.netlist_providers.prepare_maestro_netlists", side_effect=generate), \
                    mock.patch("crescendo_engine.private_netlist.reconcile_preparation", return_value={"cleanup_proven": True}):
                run_range_validation({"job_root": root}, prepared, variables, {"runtime": {}},
                                     threading.Event(), lambda **changes: None)
            self.assertEqual(requested, ["5u", "10u"])

    def test_request_validation_rejects_reversed_range(self):
        prepared = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
        with self.assertRaisesRegex(ValueError, "minimum exceeds maximum"):
            validate_variable_request(prepared, [{"parameter_id": "w", "minimum": 8, "maximum": 1}])

    def test_request_validation_rejects_equal_range(self):
        prepared = {"parameters": [{"parameter_id": "w", "optimizable": True}]}
        with self.assertRaisesRegex(ValueError, "minimum must be less than maximum"):
            validate_variable_request(
                prepared, [{"parameter_id": "w", "minimum": "20n", "maximum": "20n"}],
            )

    def test_static_report_aggregates_structured_readable_issues(self):
        hidden_hash = "6129a99ed39a82f4a568"
        prepared = {"parameters": [
            {
                "parameter_id": "width-id", "optimizable": True,
                "hierarchy_path": "/X_OPA/X_STAGE1/MN_INN",
                "master_library": "examplekit18pas", "master_cell": "n18",
                "name": "fw", "prompt": "Finger Width",
            },
            {
                "parameter_id": "length-id", "optimizable": True,
                "hierarchy_path": "/X_OPA/X_STAGE1/MN_INN",
                "master_library": "examplekit18pas", "master_cell": "n18",
                "name": "l", "prompt": "Length",
            },
        ]}
        report = validate_variable_request_report(prepared, [
            {"parameter_id": "width-id", "minimum": None, "maximum": float("inf")},
            {"parameter_id": "length-id", "minimum": "bad", "maximum": "2u"},
            {"parameter_id": "length-id", "minimum": "1u", "maximum": "2u"},
            {"parameter_id": hidden_hash, "minimum": "1u", "maximum": "2u"},
        ])

        self.assertFalse(report["valid"])
        self.assertEqual(report["variables"], [])
        self.assertEqual(
            [issue["code"] for issue in report["issues"]],
            ["missing_value", "non_finite_value", "malformed_value",
             "duplicate_parameter", "parameter_unavailable"],
        )
        required = {
            "code", "parameter_id", "hierarchy_path", "instance_name",
            "master_library", "master_cell", "parameter_name", "prompt", "field",
            "raw_value", "normalized_value", "raw_minimum", "raw_maximum",
            "normalized_minimum", "normalized_maximum", "message",
        }
        for issue in report["issues"]:
            self.assertTrue(required.issubset(issue))
        first = report["issues"][0]
        self.assertEqual(first["instance_name"], "MN_INN")
        self.assertEqual(first["parameter_name"], "fw")
        self.assertIn("/X_OPA/X_STAGE1/MN_INN", first["message"])
        self.assertIn("Finger Width [fw]", first["message"])
        self.assertNotIn(hidden_hash, report["issues"][-1]["message"])

    def test_static_report_exposes_si_normalized_bounds_without_mutation(self):
        prepared = {"parameters": [{
            "parameter_id": "w", "optimizable": True,
            "hierarchy_path": "/M0", "name": "w", "prompt": "Width",
        }]}
        variables = [{"parameter_id": "w", "minimum": "20n", "maximum": "20n"}]
        report = validate_variable_request_report(prepared, variables)

        issue = report["issues"][0]
        self.assertAlmostEqual(issue["normalized_minimum"], 20e-9)
        self.assertAlmostEqual(issue["normalized_maximum"], 20e-9)
        self.assertEqual(issue["raw_value"], {"minimum": "20n", "maximum": "20n"})
        self.assertEqual(variables[0]["minimum"], "20n")

    def test_compatibility_exception_carries_all_issues(self):
        prepared = {"parameters": [
            {"parameter_id": "w", "optimizable": True, "name": "w"},
            {"parameter_id": "l", "optimizable": True, "name": "l"},
        ]}
        with self.assertRaises(VariableRangeValidationError) as raised:
            validate_variable_request(prepared, [
                {"parameter_id": "w", "minimum": "", "maximum": "2u"},
                {"parameter_id": "l", "minimum": "3u", "maximum": "3u"},
            ])
        self.assertEqual(len(raised.exception.issues), 2)
        self.assertFalse(raised.exception.report["valid"])

    def test_static_report_rejects_huge_numeric_input_without_crashing(self):
        prepared = {"parameters": [{"parameter_id": "w", "optimizable": True, "name": "w"}]}
        report = validate_variable_request_report(prepared, [{
            "parameter_id": "w", "minimum": 10 ** 10000, "maximum": 2,
        }])
        self.assertFalse(report["valid"])
        self.assertEqual(report["issues"][0]["code"], "non_finite_value")

    def test_static_report_applies_catalog_legal_bounds_and_grid(self):
        prepared = {"parameters": [
            {
                "parameter_id": "nf", "optimizable": True, "name": "nf",
                "prompt": "Number of Fingers", "value_type": "integer", "step": 2,
            },
            {
                "parameter_id": "w", "optimizable": True, "name": "w",
                "prompt": "Width", "minimum": "10n", "maximum": "100n",
            },
        ]}
        report = validate_variable_request_report(prepared, [
            {"parameter_id": "nf", "minimum": 1, "maximum": 10},
            {"parameter_id": "w", "minimum": "5n", "maximum": "120n"},
        ])
        self.assertFalse(report["valid"])
        self.assertEqual(
            [issue["code"] for issue in report["issues"]],
            ["invalid_parameter_domain", "invalid_parameter_domain"],
        )
        self.assertIn("align to its step", report["issues"][0]["message"])
        self.assertIn("below CDF legal minimum", report["issues"][1]["message"])

if __name__ == "__main__":
    unittest.main()
