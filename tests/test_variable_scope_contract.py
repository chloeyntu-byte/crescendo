import copy
import unittest

from crescendo_engine.gui_state import initial_parameter_config
from crescendo_engine.hierarchy import normalize_probe
from crescendo_engine.native_identity import effective_variables
from crescendo_engine.range_validation import validate_variable_request_report
from crescendo_engine.runner import parameter_catalog
from crescendo_engine.variable_scope_contract import (
    VariableScopeContractError, classify_variable_value, global_variable_status,
    inspect_variable_scope, require_unique_run_variables,
)


def scope(value="6", local="2"):
    return {"tests": [{"name": "main", "enabled": True,
                       "variables": [{"name": "FN_gm", "value": local}]}],
            "global_variables": [{"name": "FN_gm", "value": value,
                                  "sweep_enabled": True,
                                  "dependent_tests": [{"test": "main", "enabled": True}]}],
            "corners": [{"name": "PVT", "enabled": True,
                         "variables": [{"name": "FN_gm", "value": "8 10"}]}]}


def native(setup, total=True, variable=True, test=True):
    setup["native_variable_scope"] = {
        "read_ok": True, "global_variables_enabled": total,
        "global_variables": [{"name": "FN_gm", "read_ok": True, "enabled": variable,
                              "tests": [{"test": "main", "read_ok": True, "enabled": test}]}]}
    return setup


class VariableScopeContractTests(unittest.TestCase):
    def test_corner_multiple_values_allowed_global_and_local_are_not(self):
        report = require_unique_run_variables(scope())
        self.assertTrue(report["ok"])
        self.assertEqual([row["point_count"] for row in report["variables"] if row["scope"] == "corner"], [2])
        self.assertFalse(report["native_ready"])
        for field in ("global", "local"):
            setup = scope(value="2 4" if field == "global" else "6",
                          local="2 4" if field == "local" else "2")
            with self.subTest(scope=field), self.assertRaises(VariableScopeContractError) as raised:
                require_unique_run_variables(setup, "validate CDF ranges")
            issue = raised.exception.issues[0]
            self.assertEqual((issue["scope"], issue["name"], issue["test"]), (field, "FN_gm", "main"))
            self.assertIn("validate CDF ranges", str(raised.exception))

    def test_repeated_equal_values_still_have_multiple_points(self):
        self.assertEqual(classify_variable_value("6 6")["point_count"], 2)
        self.assertFalse(inspect_variable_scope(scope(value="6 6"))["ok"])

    def test_known_range_rejected_without_expanding_unknown_native_sweep(self):
        for value in ("{From/To}Linear:2:2:6{From/To}", "{From/To}Log:1:10:100{From/To}"):
            with self.subTest(value=value):
                report = inspect_variable_scope(scope(value=value))
                self.assertFalse(report["ok"])
                self.assertEqual(report["issues"][0]["code"], "noncorner_variable_has_multiple_points")

    def test_single_expression_preserved_requires_point_numeric_proof(self):
        setup = native(scope(value="2 * a + b"))
        before = copy.deepcopy(setup)
        report = require_unique_run_variables(setup)
        self.assertTrue(report["ok"])
        self.assertFalse(report["native_ready"])
        self.assertFalse(report["effective_point_values_proven"])
        proof = next(row for row in report["native_evidence_required"] if row["kind"] == "point_numeric_value")
        self.assertEqual(proof["expression"], "2 * a + b")
        self.assertEqual(setup, before)

    def test_space_in_quoted_model_path_is_not_a_sweep(self):
        self.assertEqual(classify_variable_value('"/pdk/model lib/file.scs"')["kind"], "expression")

    def test_disabled_scalar_global_does_not_override_local(self):
        setup = scope()
        setup["global_variables"][0]["sweep_enabled"] = False
        value = effective_variables(setup, setup["tests"][0])["FN_gm"]
        self.assertEqual((value["source"], value["value"]), ("test", "2"))
        report = inspect_variable_scope(setup)
        self.assertFalse(report["native_ready"])
        self.assertFalse(report["global_participation"][0]["native_verified"])

    def test_disabled_scalar_global_without_local_is_not_fabricated(self):
        setup = scope()
        setup["tests"][0]["variables"] = []
        setup["global_variables"][0]["sweep_enabled"] = False
        self.assertNotIn("FN_gm", effective_variables(setup, setup["tests"][0]))

    def test_per_test_mask_and_omitted_global_do_not_block_unused_sweep(self):
        for mask in ({"test": "main", "enabled": False},
                     {"test": "main", "enabled": True, "omitted": True}):
            setup = scope(value="2 4")
            setup["global_variables"][0]["dependent_tests"] = [mask]
            report = require_unique_run_variables(setup)
            self.assertTrue(report["ok"])
            self.assertEqual(effective_variables(setup, setup["tests"][0])["FN_gm"]["value"], "2")

    def test_each_native_enable_layer_can_disable_global(self):
        for total, variable, test in ((False, True, True), (True, False, True), (True, True, False)):
            setup = native(scope(), total, variable, test)
            with self.subTest(total=total, variable=variable, test=test):
                status = global_variable_status(setup, setup["global_variables"][0], "main")
                self.assertFalse(status["applies"])
                self.assertTrue(status["native_verified"])
                self.assertEqual(effective_variables(setup, setup["tests"][0])["FN_gm"]["value"], "2")

    def test_enabled_global_then_corner_overlay_respects_native_point(self):
        setup = native(scope())
        test = setup["tests"][0]
        self.assertEqual(effective_variables(setup, test)["FN_gm"]["value"], "6")
        for point in ("8", "10"):
            value = effective_variables(setup, test, {"variables": [{"name": "FN_gm", "value": point}]})["FN_gm"]
            self.assertEqual((value["source"], value["value"]), ("corner", point))

    def test_failed_native_read_never_becomes_disabled_or_local_fallback(self):
        setup = native(scope())
        setup["native_variable_scope"]["read_ok"] = False
        status = global_variable_status(setup, setup["global_variables"][0], "main")
        self.assertIsNone(status["applies"])
        self.assertFalse(status["native_verified"])
        with self.assertRaises(VariableScopeContractError):
            effective_variables(setup, setup["tests"][0])
        self.assertEqual(inspect_variable_scope(setup)["issues"][0]["code"], "global_variable_participation_unknown")

    def test_partial_native_test_evidence_cannot_enable_global(self):
        setup = native(scope())
        setup["native_variable_scope"]["global_variables"][0]["tests"] = []
        self.assertIsNone(global_variable_status(setup, setup["global_variables"][0], "main")["applies"])
        self.assertFalse(inspect_variable_scope(setup)["ok"])

    def test_unknown_saved_test_mask_is_visible_instead_of_a_false_disable(self):
        setup = scope()
        setup["global_variables"][0]["dependent_tests"][0]["enabled"] = "unrecognized"
        status = global_variable_status(setup, setup["global_variables"][0], "main")
        self.assertIsNone(status["applies"])
        self.assertFalse(inspect_variable_scope(setup)["ok"])

    def test_disabled_test_definitions_do_not_create_a_false_block(self):
        setup = scope()
        setup["tests"].append({"name": "unused", "enabled": False,
                               "variables": [{"name": "N", "value": "1 2"}]})
        self.assertTrue(inspect_variable_scope(setup)["ok"])

    def test_duplicate_or_missing_source_definition_is_reported_precisely(self):
        setup = scope()
        setup["tests"][0]["variables"].append({"name": "FN_gm", "value": ""})
        issues = inspect_variable_scope(setup)["issues"]
        self.assertEqual({row["code"] for row in issues}, {"duplicate_variable_definition"})
        self.assertTrue(all(row["test"] == "main" for row in issues))

    def test_blank_local_global_override_and_definitions_file_stay_native_pending(self):
        setup = native(scope(local=""))
        setup["corners"][0]["variables"].append({"name": "FROM_DEFS", "value": ""})
        report = require_unique_run_variables(setup)
        self.assertTrue(report["ok"])
        self.assertFalse(report["native_ready"])
        self.assertTrue(report["native_pending"])
        self.assertEqual(effective_variables(setup, setup["tests"][0])["FN_gm"]["value"], "6")
        pending = report["native_evidence_required"]
        self.assertTrue(any(row["kind"] == "point_value_from_native_setup" and row["name"] == "FROM_DEFS" for row in pending))
        self.assertTrue(any(row["kind"] == "missing" and row["scope"] == "local" for row in report["variables"]))

    def test_multiplicity_only_gate_does_not_qualify_unknown_native_context(self):
        setup = native(scope())
        setup["native_variable_scope"]["read_ok"] = False
        report = require_unique_run_variables(setup, multiplicity_only=True)
        self.assertTrue(report["multiplicity_ok"])
        self.assertFalse(report["ok"])
        self.assertFalse(report["native_ready"])
        self.assertTrue(report["native_pending"])
        self.assertFalse(report["effective_point_values_proven"])


def device(current="FN_gm", name="nfin", prompt="Number of Fingers"):
    records = [
        {"kind": "INSTANCE", "values": ["/M0", "M0", "PDK", "nmos", "symbol", False, "/", 0]},
        {"kind": "PARAM", "values": ["/M0", name, prompt, "instData", current, "6", None, None,
                                       True, True, True, None, None, "LIB", "TOP", None, "string"]}]
    design = {"library": "LIB", "cell": "TOP", "view": "schematic"}
    return {"designs": [{"design": design, "hierarchy": normalize_probe(records, design)}]}


class VariableExpressionCandidateTests(unittest.TestCase):
    def catalog(self, hierarchy, setup=None):
        return next(row for row in parameter_catalog(setup or {"tests": []}, hierarchy)
                    if row["backend"] == "schematic")

    def test_global_expression_device_can_be_selected_without_corner_seed(self):
        hierarchy = device()
        row = self.catalog(hierarchy, scope())
        self.assertTrue(row["optimizable"])
        self.assertTrue(row["tunable"])
        self.assertIsNone(row["numeric_value"])
        self.assertEqual(row["baseline_expression"], "FN_gm")
        self.assertTrue(row["requires_point_numeric_value"])
        self.assertEqual(row["value_type"], "integer")
        self.assertEqual(hierarchy["designs"][0]["hierarchy"]["parameters"][0]["current_value"], "FN_gm")

    def test_expression_has_no_fabricated_default_bounds_but_numeric_candidate_range_validates(self):
        row = self.catalog(device())
        settings = initial_parameter_config(row)
        self.assertEqual((settings["minimum"], settings["maximum"]), ("", ""))
        report = validate_variable_request_report({"parameters": [row]}, [
            {"parameter_id": row["parameter_id"], "minimum": 2, "maximum": 8}])
        self.assertTrue(report["valid"], report)

    def test_selected_candidate_policy_does_not_expand_direct_variable_backend(self):
        setup = scope()
        catalog = parameter_catalog(setup, device())
        variable = next(row for row in catalog if row["backend"] == "design_variable")
        self.assertFalse(variable["optimizable"])

    def test_inherited_parameter_and_unrecognized_device_field_stay_blocked(self):
        for current, name, prompt in (('pPar("nf")', "nf", "Fingers"),
                                      ('iPar("nf")', "nf", "Fingers"),
                                      ("FN_gm", "model", "Model name")):
            with self.subTest(current=current, name=name):
                self.assertFalse(self.catalog(device(current, name, prompt))["optimizable"])

    def test_readonly_opaque_and_reference_internal_boundaries_stay_blocked(self):
        for key, value in (("editable", False), ("display", False),
                           ("netlister_ignored", True), ("symbol_masked", True),
                           ("resolved_view_type", "unknown"), ("owner_library", "PDK"),
                           ("source", "db_property")):
            hierarchy = device()
            parameter = hierarchy["designs"][0]["hierarchy"]["parameters"][0]
            parameter[key] = value
            with self.subTest(key=key):
                self.assertFalse(self.catalog(hierarchy)["optimizable"])

    def test_existing_constant_device_values_unchanged(self):
        row = self.catalog(device("6"))
        self.assertTrue(row["optimizable"])
        self.assertEqual(row["numeric_value"], 6)
        self.assertFalse(row["candidate_expression"])


if __name__ == "__main__":
    unittest.main()
