"""Scalar/Ocean bridge contracts; these tests do not certify native simulation."""
import copy
import os
import tempfile
import unittest

from crescendo_engine import netlist_measurement as measurement
from crescendo_engine.metrics import evaluate_metrics


class NetlistMeasurementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo-measure-tests-")
        self.root = self.temp.name
        self.task = {"task_id": "rc_tt", "test": "rc", "corner": "tt", "point_id": 1,
                     "design_point": 1, "psf_dir": os.path.join(self.root, "psf"), "cwd": self.root,
                     "variables": [{"name": "R", "value": "1k"}],
                     "outputs": [{"key": "rc::gain", "name": "gain", "expression": 'value(mag(VF("/out")) 1000)', "type": "expr"}]}
        self.outputs = self.task["outputs"]
        self.path = os.path.join(self.root, "values.tsv")

    def tearDown(self):
        self.temp.cleanup()

    def records(self, text):
        with open(self.path, "w", encoding="utf-8") as stream:
            stream.write(text)
        return measurement.parse_measurement_records(self.path, "owned", self.outputs, self.task)

    def test_native_script_preserves_expression_psf_variables_and_quoting(self):
        self.task["psf_dir"] = os.path.join(self.root, "space's dir", "psf")
        script, outputs = measurement.render_measurement_script(self.task, self.path, "owned")
        self.assertNotIn('desVar("R" "1k")', script)
        self.assertIn("openResults", script)
        self.assertIn('evalstring("value(mag(VF(\\"/out\\")) 1000)")', script)
        self.assertIn("CRESCENDO_NETLIST_MEASURE_1\\towned", script)
        self.assertEqual(outputs, self.outputs)
        self.assertNotIn("maeOpenSetup", script)
        self.assertNotIn("run()", script)

    def test_protocol_exact_scalar_and_point_identity(self):
        rows = self.records("CRESCENDO_NETLIST_MEASURE_1\towned\nV\t0\t-1.2345678901234567e-18\nEND\t1\n")
        self.assertEqual(rows[0]["value"], -1.2345678901234567e-18)
        self.assertEqual(rows[0]["test"], "rc")
        self.assertEqual(rows[0]["point_id"], 1)

    def test_wrong_token_truncated_duplicate_and_nonfinite_rejected(self):
        invalid = ["CRESCENDO_NETLIST_MEASURE_1\twrong\nV\t0\t1\nEND\t1\n",
                   "CRESCENDO_NETLIST_MEASURE_1\towned\nV\t0\t1\n",
                   "CRESCENDO_NETLIST_MEASURE_1\towned\nV\t0\t1\nV\t0\t2\nEND\t1\n",
                   "CRESCENDO_NETLIST_MEASURE_1\towned\nV\t0\tNaN\nEND\t1\n",
                   "CRESCENDO_NETLIST_MEASURE_1\towned\nV\t0\tinf\nEND\t1\n",
                   "CRESCENDO_NETLIST_MEASURE_1\towned\nEND\t1\n"]
        for text in invalid:
            with self.subTest(text=text), self.assertRaises(measurement.NetlistMeasurementError):
                self.records(text)

    def test_expression_failure_is_not_zero(self):
        rows = self.records("CRESCENDO_NETLIST_MEASURE_1\towned\nE\t0\texpression_failed\nEND\t1\n")
        self.assertIsNone(rows[0]["value"])
        self.assertEqual(rows[0]["error"], "expression_failed")

    def test_required_only_qualification_allows_unused_cross_test_output(self):
        self.task["outputs"].append({"key": "rc::other", "name": "other", "expression": 'calcVal("x" "test2")'})
        self.assertTrue(measurement.qualify_expressions([self.task], ["rc::gain"])["supported"])
        self.assertFalse(measurement.qualify_expressions([self.task], ["rc::other"])["supported"])

    def test_context_reference_and_missing_expression_rejected(self):
        for expression in ('valueFromTest("x")', 'getData("x" ?test "wrong")', ''):
            task = copy.deepcopy(self.task)
            task["outputs"][0]["expression"] = expression
            with self.subTest(expression=expression):
                self.assertFalse(measurement.qualify_expressions([task], ["rc::gain"])["supported"])
        self.assertFalse(measurement.qualify_expressions([self.task], ["missing::out"])["supported"])

    def test_duplicate_output_names_rejected(self):
        self.task["outputs"].append(dict(self.outputs[0]))
        with self.assertRaises(measurement.NetlistMeasurementError):
            measurement.render_measurement_script(self.task, self.path, "owned")

    def test_native_result_shape_reuses_metrics_and_corner_aggregation(self):
        results = []
        for index, (corner, value) in enumerate((("tt", 0.5), ("ss", 0.25)), 1):
            task = dict(self.task, point_id=index, corner=corner)
            row = dict(self.outputs[0], value=value, error=None)
            results.append({"task": task, "measurement": {"outputs": [row]}})
        parsed = measurement.parsed_results(results, "private.json")
        self.assertFalse(parsed["native_rdb"])
        self.assertEqual(parsed["output_map"]["rc::gain"]["summary"]["mean"], 0.375)
        metrics = evaluate_metrics(parsed, [{"output_key": "rc::gain", "direction": "maximize", "aggregation": "min"}], [])
        self.assertEqual(metrics["objectives"][0]["value"], 0.25)

    def test_conflicting_corner_point_identity_rejected(self):
        row = dict(self.outputs[0], value=0.5, error=None)
        with self.assertRaises(measurement.NetlistMeasurementError):
            measurement.parsed_results([
                {"task": self.task, "measurement": {"outputs": [row]}},
                {"task": dict(self.task, corner="wrong"), "measurement": {"outputs": [row]}}])

    def test_same_test_wave_and_scalar_dependencies_topological_and_native(self):
        self.task["outputs"] = [
            {"key": "rc::FoM", "name": "FoM", "expression": "PN_100k + power"},
            {"key": "rc::PN_100k", "name": "PN_100k", "expression": "value(PN 100000)"},
            {"key": "rc::PN", "name": "PN", "expression": "pn('hbnoise)"},
            {"key": "rc::power", "name": "power", "expression": 'VAR("VDD") * current'},
            {"key": "rc::current", "name": "current", "expression": "0.001"}]
        plan, requested = measurement.evaluation_plan(self.task, ["rc::FoM"])
        self.assertEqual([row["name"] for row in plan], ["PN", "PN_100k", "current", "power", "FoM"])
        script, outputs = measurement.render_measurement_script(self.task, self.path, "owned", ["rc::FoM"])
        self.assertIn("set('PN car(crescValue))", script)
        self.assertLess(script.index("pn('hbnoise)"), script.index("value(PN 100000)"))
        self.assertEqual([row["key"] for row in outputs], ["rc::FoM"])
        self.assertTrue(measurement.qualify_expressions([self.task], ["rc::FoM"])["supported"])

    def test_reachable_dependency_cycle_and_cross_test_rejected(self):
        self.task["outputs"] = [{"key": "rc::A", "name": "A", "expression": "B+1"},
                                {"key": "rc::B", "name": "B", "expression": "A+1"}]
        self.assertFalse(measurement.qualify_expressions([self.task], ["rc::A"])["supported"])
        self.task["outputs"][1]["expression"] = 'calcVal("out" "another")'
        self.assertFalse(measurement.qualify_expressions([self.task], ["rc::A"])["supported"])

    def test_strings_analysis_symbols_and_function_names_are_not_aliases(self):
        self.assertEqual(measurement._output_references('value(PN 100000) + VAR("VDD") + pn(\'hbnoise)',
            {"PN", "value", "VDD", "hbnoise", "pn"}), {"PN"})

    def test_var_dependencies_include_alias_closure_but_ignore_strings_comments_and_unused_outputs(self):
        self.task["outputs"] = [{"key": "rc::A", "name": "A", "expression": 'B + VAR("local")'},
                                {"key": "rc::B", "name": "B", "expression": 'VAR("corner")'},
                                {"key": "rc::unused", "name": "unused", "expression": 'VAR("ignore")'}]
        self.assertEqual(measurement.required_variables(self.task, ["rc::A"]), ["corner", "local"])
        self.task["outputs"][0]["expression"] = 'strcat("VAR(\\\"fake\\\")") ; VAR("comment")'
        self.assertEqual(measurement.required_variables(self.task, ["rc::A"]), [])

    def test_dynamic_var_names_are_rejected_for_this_backend(self):
        self.task["outputs"][0]["expression"] = 'VAR(strcat("V" "DD"))'
        self.assertFalse(measurement.qualify_expressions([self.task], ["rc::gain"])["supported"])

    def test_psf_variable_protocol_identity_finite_scalar_and_missing_are_infrastructure(self):
        with open(self.path, "w") as stream:
            stream.write("CRESCENDO_NETLIST_MEASURE_1\towned\nP\t0\t2.5\nV\t0\t3.5\nEND\t1\n")
        variables = {}
        rows = measurement.parse_measurement_records(self.path, "owned", self.outputs, self.task, ["VDD"], variables)
        self.assertEqual(variables, {"VDD": 2.5})
        self.assertEqual(rows[0]["value"], 3.5)
        for text in ("X\t0\tunavailable\n", "P\t0\tNaN\n", "P\t1\t5\n", "P\t0\t2\nP\t0\t3\n", ""):
            with open(self.path, "w") as stream:
                stream.write("CRESCENDO_NETLIST_MEASURE_1\towned\n" + text + "END\t1\n")
            with self.subTest(text=text), self.assertRaises(measurement.NetlistMeasurementError) as caught:
                measurement.parse_measurement_records(self.path, "owned", self.outputs, self.task, ["VDD"])
            self.assertTrue(caught.exception.infrastructure)

    def test_required_var_native_read_overrides_setup_value_after_current_psf_is_opened(self):
        self.task["outputs"][0]["expression"] = 'VAR("VDD")'
        self.task["variables"] = [{"name": "VDD", "value": 99}]
        script, unused = measurement.render_measurement_script(self.task, self.path, "owned")
        self.assertIn('pv("top-level" "VDD" ?result \'designParamVals)', script)
        self.assertIn('desVar("VDD" car(crescParam))', script)
        self.assertLess(script.index("openResults"), script.index('pv("top-level"'))
        self.assertLess(script.index('pv("top-level"'), script.index('evalstring("VAR'))

    def test_output_alias_cannot_overwrite_native_evaluator_state(self):
        self.task["outputs"] = [{"key": "rc::crescValue", "name": "crescValue", "expression": "1.0"}]
        self.assertFalse(measurement.qualify_expressions([self.task], ["rc::crescValue"])["supported"])


if __name__ == "__main__":
    unittest.main()
