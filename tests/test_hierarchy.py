from __future__ import print_function

import os
import tempfile
import unittest

from crescendo_engine.hierarchy import normalize_probe, parse_physical_number, parse_probe_records, probe_skill


class HierarchyParserTest(unittest.TestCase):
    def test_physical_numbers(self):
        self.assertEqual(parse_physical_number("2.5k"), 2500.0)
        self.assertAlmostEqual(parse_physical_number("180n"), 180e-9)
        self.assertIsNone(parse_physical_number("pPar(\"w\")"))

    def test_nested_hierarchy_and_aliases(self):
        payload = "\n".join([
            'TECH\t"HH"\t"examplekit18"\t"/pdk/examplekit18/tech.db"',
            'CELL\t"/"\t"HH"\t"VCO"\t"schematic"\t0',
            'INSTANCE\t"/I1"\t"I1"\t"HH"\t"SCA"\t"symbol"\tt\t"/"\t0',
            'PARAM\t"/I1"\t"m"\t"Multiplier"\t"int"\t4\t1\tnil\t"(1 2 4)"\tt\tt\tt\t1\t64\t"HH"\t"VCO"',
            'CELL\t"/I1"\t"HH"\t"SCA"\t"schematic"\t1',
            'INSTANCE\t"/I1/I0"\t"I0"\t"HH"\t"INV"\t"symbol"\tt\t"/I1"\t1',
            'CELL\t"/I1/I0"\t"HH"\t"INV"\t"schematic"\t2',
            'INSTANCE\t"/I1/I0/M0"\t"M0"\t"PDK"\t"nmos"\t"symbol"\tnil\t"/I1/I0"\t2',
            'PARAM\t"/I1/I0/M0"\t"w"\t"Width"\t"string"\t"2u"\t"1u"\t"m"\tnil\tt\tt\tt\t"180n"\t"20u"\t"HH"\t"INV"',
        ]) + "\n"
        handle, path = tempfile.mkstemp()
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(payload)
            normalized = normalize_probe(parse_probe_records(path))
        finally:
            os.unlink(path)
        self.assertEqual(normalized["counts"]["cells"], 3)
        self.assertEqual(normalized["counts"]["hierarchical_instances"], 2)
        self.assertEqual(normalized["counts"]["devices"], 1)
        leaf = [row for row in normalized["parameters"] if row["hierarchy_path"].endswith("M0")][0]
        self.assertAlmostEqual(leaf["numeric_value"], 2e-6)
        self.assertTrue(leaf["tunable"])
        self.assertEqual(leaf["owner_cell"], "INV")
        self.assertEqual([row["hierarchy_path"] for row in leaf["occurrence_chain"]], ["/I1", "/I1/I0"])
        self.assertEqual(normalized["technology_bindings"], [{
            "library": "HH", "technology_library": "examplekit18",
            "technology_path": "/pdk/examplekit18/tech.db",
        }])
        self.assertEqual(normalized["cells"][0]["technology_library"], "examplekit18")

    def test_parameter_identity_includes_top_design(self):
        records = [{"kind": "PARAM", "values": ["/M0", "w", "Width", "string", "2u", "1u", None, None, True, True, True, None, None, "LIB", "TOP"], "line": 1}]
        first = normalize_probe(records, {"library": "LIB", "cell": "TOP_A", "view": "schematic"})
        second = normalize_probe(records, {"library": "LIB", "cell": "TOP_B", "view": "schematic"})
        first_parameter = first["parameters"][0]
        second_parameter = second["parameters"][0]
        self.assertNotEqual(first_parameter["parameter_id"], second_parameter["parameter_id"])
        self.assertEqual(first_parameter["design_cell"], "TOP_A")
        self.assertEqual(second_parameter["design_cell"], "TOP_B")

    def test_cdf_parameter_type_is_preserved_separately_from_object_type(self):
        records = [{"kind": "PARAM", "values": [
            "/M0", "nf", "Fingers", "instData", "2", "1", None, None,
            True, True, True, None, None, "LIB", "TOP", None, "natural",
        ], "line": 1}]
        parameter = normalize_probe(records, {"library": "LIB", "cell": "TOP", "view": "schematic"})["parameters"][0]
        self.assertEqual(parameter["cdf_type"], "instData")
        self.assertEqual(parameter["cdf_parameter_type"], "natural")

    def _cdf_parameter(self, current="2u", default="1u", editable=None, display=None, extra=None):
        values = ["/M0", "w", "Width", "instData", current, default, "lengthMetric", None,
                  editable, display, "t", None, None, "LIB", "TOP", None, "string"]
        values.extend(extra or [])
        return normalize_probe([{"kind": "PARAM", "values": values}],
                               {"library": "LIB", "cell": "TOP", "view": "schematic"})["parameters"][0]

    def test_explicit_cdf_nil_differs_from_unspecified_editability(self):
        self.assertTrue(self._cdf_parameter(editable=None)["tunable"])
        disabled = self._cdf_parameter(editable="nil")
        self.assertFalse(disabled["editable"])
        self.assertFalse(disabled["tunable"])
        self.assertEqual(disabled["editable_raw"], "nil")
        self.assertFalse(self._cdf_parameter(display="nil")["tunable"])

    def test_cdf_conditions_preserve_raw_and_evaluated_metadata(self):
        parameter = self._cdf_parameter(editable="pdkEditable('w)", display="pdkDisplay('w)",
                                        extra=["1n", True, False, "evaluated", "evaluated", "t"])
        self.assertTrue(parameter["editable"])
        self.assertFalse(parameter["display"])
        self.assertFalse(parameter["tunable"])
        self.assertEqual(parameter["step"], "1n")
        self.assertTrue(parameter["parse_as_cel"])
        self.assertFalse(self._cdf_parameter(editable="unknownCondition()")["tunable"])

    def test_cdf_limit_aliases_are_recorded_without_guessing(self):
        parameter = self._cdf_parameter(extra=[None, True, True, "default", "default", None,
                                                "2n", "1n", "60n", "10u"])
        self.assertEqual(parameter["step"], "2n")
        self.assertEqual(parameter["resolution"], "1n")
        self.assertEqual(parameter["minimum"], "60n")
        self.assertEqual(parameter["maximum"], "10u")

    def test_current_expression_never_falls_back_to_numeric_default(self):
        parameter = self._cdf_parameter(current='pPar("width")', default="1u")
        self.assertIsNone(parameter["numeric_value"])
        self.assertFalse(parameter["tunable"])
        self.assertTrue(parameter["inherited_expression"])
        self.assertAlmostEqual(self._cdf_parameter(current="")["numeric_value"], 1e-6)

    def test_skill_choices_and_mapping_lists_keep_item_boundaries(self):
        payload = '\n'.join([
            'INSTANCE\t"/M0"\t"M0"\t"PDK"\t"nmos"\t"symbol"\tnil\t"/"\t0',
            'SIMINFO\t"/M0"\t("w" "nf")\t(nil "w" "width" "nf" "fingers")',
            'PARAM\t"/M0"\t"w"\t"Width"\t"instData"\t"9u"\t"9u"\t"lengthMetric"\t("9u" "15u" "30u")\t"t"\t"t"\t"t"\tnil\tnil\t"LIB"\t"TOP"',
            'PARAM\t"/M0"\t"nf"\t"Fingers"\t"instData"\t4\t4\tnil\t(1 2 4)\t"t"\t"t"\t"t"\tnil\tnil\t"LIB"\t"TOP"',
        ]) + '\n'
        handle, path = tempfile.mkstemp()
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(payload)
            normalized = normalize_probe(parse_probe_records(path))
        finally:
            os.unlink(path)
        by_name = {row["name"]: row for row in normalized["parameters"]}
        self.assertEqual(by_name["w"]["choices_raw"], ["9u", "15u", "30u"])
        self.assertEqual(by_name["nf"]["choices_raw"], [1, 2, 4])
        self.assertEqual(normalized["instances"][0]["spectre_prop_mapping"],
                         [None, "w", "width", "nf", "fingers"])

    def test_cycles_and_depth_limits_are_recorded_without_unbounded_recursion(self):
        script = probe_skill("L", "TOP", "schematic", "/tmp/probe.records", 32)
        self.assertIn("member(probeIdentity crescProbeStack)", script)
        self.assertIn("crescProbeStack = cdr(crescProbeStack)", script)
        self.assertIn('deOpenCellView(libName cellName viewName "schematic" nil "r")', script)
        self.assertIn("car(editContext) == cv", script)
        self.assertIn("hiSetCurrentWindow(previousWindow)", script)
        normalized = normalize_probe([
            {"kind": "CYCLE", "values": ["/I0/I1", "L", "TOP", "schematic"]},
            {"kind": "DEPTH_LIMIT", "values": ["/I2", "L", "SUB", "schematic"]},
        ])
        self.assertEqual([row["kind"] for row in normalized["errors"]], ["CYCLE", "DEPTH_LIMIT"])


if __name__ == "__main__":
    unittest.main()
