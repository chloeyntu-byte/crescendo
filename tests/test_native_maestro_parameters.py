import copy
import unittest

from crescendo_engine.native_maestro_parameters import (
    MAX_SETTINGS, build_native_parameter_plan, native_parameter_skill_lines,
)


class NativeMaestroParameterPlanTests(unittest.TestCase):
    def row(self, **changes):
        row = {"parameter_id": "X1::nf", "library": "lib",
               "cell": "private", "view": "cr_private_schematic",
               "instance": "X1", "property": "nf", "value": "5",
               "current_value": "FN_gm"}
        row.update(changes)
        return row

    def test_expression_and_constant_originals_receive_same_device_override(self):
        expression = build_native_parameter_plan([self.row()], ["C0", "C1"])
        constant = build_native_parameter_plan([self.row(current_value="6")], ["C0", "C1"])
        self.assertEqual(expression, constant)
        self.assertFalse(expression["point_execution_proven"])
        self.assertFalse(expression["private_oa_backannotation_proven"])

    def test_inherited_other_instance_and_variable_remain_untouched(self):
        inherited = [{"path": "lib/old/schematic/X2/nf", "value": "FN_gm"},
                     {"name": "FN_gm", "value": "4", "source": "global"}]
        before = copy.deepcopy(inherited)
        plan = build_native_parameter_plan([self.row()], ["C0"], inherited)
        self.assertEqual(inherited, before)
        self.assertEqual(plan["inherited_parameters"], before)
        plan["inherited_parameters"][0]["value"] = "not inherited"
        self.assertEqual(inherited, before)
        script = "\n".join(native_parameter_skill_lines(plan, "/tmp/point.records"))
        self.assertNotIn("maeSetVar", script)
        self.assertNotIn("lib/old/schematic/X2/nf", script)
        self.assertNotIn("FN_gm", script)

    def test_disabled_corner_is_not_overridden(self):
        plan = build_native_parameter_plan([self.row()], [
            {"name": "C0", "enabled": True}, {"name": "off", "enabled": False}])
        self.assertEqual(plan["enabled_corners"], ["C0"])
        self.assertEqual(plan["selected"][0]["path"],
                         "lib/private/cr_private_schematic/X1/nf")

    def test_native_setter_and_getter_use_correct_corner_argument_shapes(self):
        script = "\n".join(native_parameter_skill_lines(
            build_native_parameter_plan([self.row(value="2u")], ["C0"]), "/tmp/point.records"))
        self.assertIn('?typeName "test" ?session session', script)
        self.assertIn('?typeValue list(crescNpCorner) ?session session', script)
        self.assertIn('?typeValue crescNpCorner ?session session', script)
        self.assertNotIn("evalstring", script)
        self.assertNotIn("cdfGetInstCDF", script)
        self.assertNotIn("maeRunSimulation", script)
        self.assertNotIn("dbReplaceProp", script)

    def test_policy_is_checked_before_any_native_setter(self):
        script = "\n".join(native_parameter_skill_lines(
            build_native_parameter_plan([self.row()], ["C0"]), "/tmp/point.records"))
        first_write = script.index('unless(maeSetParameter(')
        self.assertLess(script.index("boundp('axlExecuteCallbacks)"), first_write)
        self.assertLess(script.index('when(crescNpDisabled'), first_write)
        self.assertNotIn("axlExecuteCallbacks=", script)
        self.assertLess(script.index("member(crescNpPath crescNpOwned)"), script.index("axlSetAllParametersDisabled(mainSDB 0)"))
        self.assertIn("existing native Device Parameters need explicit remapping", script)

    def test_only_selected_paths_enabled_and_inherited_state_checked(self):
        script = "\n".join(native_parameter_skill_lines(
            build_native_parameter_plan([self.row()], []), "/tmp/point.records"))
        self.assertIn("?parameters list(crescNpPath) ?enabled t", script)
        self.assertIn('maeGetSetup(?typeName "parameters"', script)
        self.assertIn("member(crescNpPath crescNpOwned)", script)
        self.assertIn("native_override_settings_only", script)

    def test_generated_skill_is_one_complete_ordered_transaction(self):
        from crescendo_engine.workspace import bounded_skill_body_lines
        lines = native_parameter_skill_lines(build_native_parameter_plan(
            [self.row(), self.row(parameter_id="X2::nf", instance="X2", value="2u")],
            ["C0", "C1"]), "/tmp/private ' candidate/point.records")
        rendered, generation = bounded_skill_body_lines(lines)
        self.assertTrue(rendered)
        self.assertEqual(generation["expression_count"], 1)

    def test_candidates_are_unique_numeric_literals_not_variables_or_sweeps(self):
        for value in ("FN_gm", "1 2", "1+2", "{From/To}Linear:1:1:2{From/To}",
                      "5) error(\"injected\")", '"5"', float("inf"), True, None):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    build_native_parameter_plan([self.row(value=value)], [])

    def test_duplicates_or_ambiguous_private_component_paths_are_refused(self):
        for rows in ([self.row(), self.row(parameter_id="other")],
                     [self.row(), self.row(instance="X2")],
                     [self.row(instance="X1/X2")], [self.row(view="")],
                     [self.row(cell="../original")], [self.row(property="nf\n")]):
            with self.subTest(rows=rows):
                with self.assertRaises(ValueError):
                    build_native_parameter_plan(rows, [])
        with self.assertRaises(ValueError):
            build_native_parameter_plan([self.row()], ["C0", "C0"])

    def test_mutated_plan_and_code_expression_inputs_are_revalidated(self):
        plan = build_native_parameter_plan([self.row()], ["C0"])
        plan["selected"][0]["value"] = "FN_gm"
        with self.assertRaises(ValueError):
            native_parameter_skill_lines(plan, "/tmp/point.records")
        good = build_native_parameter_plan([self.row()], [])
        for field, value in (("session_expr", 'session) error("bad")'),
                             ("main_sdb_expr", "db()")):
            with self.assertRaises(ValueError):
                native_parameter_skill_lines(good, "/tmp/point.records", **{field: value})
        with self.assertRaises(ValueError):
            native_parameter_skill_lines(good, "relative.records")

    def test_large_corner_product_has_explicit_bound(self):
        rows = [self.row(parameter_id=str(index), instance="X%d" % index)
                for index in range(512)]
        with self.assertRaises(ValueError):
            build_native_parameter_plan(rows, ["C%d" % index for index in range(MAX_SETTINGS // 512)])


if __name__ == "__main__":
    unittest.main()
