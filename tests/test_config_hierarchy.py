"""Config identity, actual implementation, and fail-closed traversal contracts."""
import copy
import unittest
from unittest import mock

from crescendo_engine.hierarchy import (
    cell_scope_records, normalize_probe, probe_skill, validate_native_hierarchy,
)


REQUESTED = {"library": "CFG", "cell": "TB", "view": "config"}
TOP = {"library": "USER", "cell": "REAL_TOP", "view": "schematic_alt"}


def record(kind, *values):
    return {"kind": kind, "values": list(values)}


def binding(path, name, parent, master, owner, bound, resolved, stop=False,
            context=None, context_path="/", local_path=None, ignored=False, pcdb_non_subdesign=False):
    context = context or REQUESTED
    return record("CONFIG_BINDING", path, name, parent,
                  *master, *owner, *bound, *resolved, stop,
                  context["library"], context["cell"], context["view"],
                  context_path, local_path or path, True, True, False,
                  None if stop else "schematic", ignored, pcdb_non_subdesign)


def parameter(path, owner=("USER", "AMP")):
    return record("PARAM", path, "w", "Width", "string", "1u", "1u", None,
                  None, None, None, True, None, None, *owner, None, None)


def records():
    child = ("USER", "AMP", "schematic_alt")
    subconfig = {"library": "USER", "cell": "AMP", "view": "nested_config"}
    return [record("DESIGN", "CFG", "TB", "config"),
            record("RESOLVED_TOP", "USER", "REAL_TOP", "schematic_alt"),
            record("CONFIG_DEP", "CFG", "TB", "config"),
            record("CONFIG_DEP", "USER", "AMP", "nested_config"),
            record("CELL", "/", "USER", "REAL_TOP", "schematic_alt", 0),
            record("CELL_VIEW_TYPE", "/", "schematic"),
            binding("/I1", "I1", "/", ("USER", "AMP", "symbol"),
                    ("USER", "REAL_TOP", "schematic_alt"),
                    ("USER", "AMP", "nested_config"), child),
            record("INSTANCE", "/I1", "I1", "USER", "AMP", "symbol", True, "/", 0, "schematicSymbol"),
            record("CELL", "/I1", *child, 1),
            record("CELL_VIEW_TYPE", "/I1", "schematic"),
            binding("/I1/M0", "M0", "/I1", ("PDK", "NMOS", "symbol"), child,
                    ("PDK", "NMOS", "spectre"), ("PDK", "NMOS", "spectre"), True,
                    subconfig, "/I1", "/M0"),
            record("INSTANCE", "/I1/M0", "M0", "PDK", "NMOS", "symbol", False, "/I1", 1, "schematicSymbol"),
            parameter("/I1/M0"),
            binding("/I1/PIN0", "PIN0", "/I1", ("basic", "iopin", "symbol"), child,
                    ("basic", "iopin", "symbol"), ("basic", "iopin", "symbol"), True,
                    subconfig, "/I1", "/PIN0", True),
            record("INSTANCE", "/I1/PIN0", "PIN0", "basic", "iopin", "symbol", False, "/I1", 1, "schematicSymbol"),
            parameter("/I1/PIN0")]


class ConfigHierarchyTests(unittest.TestCase):
    def normalized(self, extra=None):
        return normalize_probe(records() + list(extra or []), REQUESTED)

    def test_requested_config_and_actual_root_remain_distinct(self):
        body = self.normalized()
        validate_native_hierarchy(body, REQUESTED)
        self.assertEqual(body["resolved_top"], TOP)
        self.assertEqual(body["cells"][0]["cell"], "REAL_TOP")
        self.assertEqual(body["cells"][0]["view_type"], "schematic")
        parameter_row = body["parameters"][0]
        self.assertEqual((parameter_row["design_library"], parameter_row["design_cell"], parameter_row["design_view"]),
                         ("CFG", "TB", "config"))

    def test_parameters_use_actual_owner_and_keep_original_cdf_master(self):
        row = next(row for row in self.normalized()["parameters"] if row["hierarchy_path"] == "/I1/M0")
        self.assertEqual((row["owner_library"], row["owner_cell"], row["owner_view"]),
                         ("USER", "AMP", "schematic_alt"))
        self.assertEqual(row["owner_view_type"], "schematic")
        self.assertEqual((row["master_library"], row["master_cell"]), ("PDK", "NMOS"))
        self.assertEqual(row["resolved_view"], "spectre")
        self.assertEqual(row["resolved_top"], TOP)
        self.assertTrue(row["tunable"])

    def test_nested_config_context_is_local_but_occurrence_path_is_absolute(self):
        body = self.normalized()
        row = next(row for row in body["config_bindings"] if row["hierarchy_path"] == "/I1/M0")
        self.assertEqual(row["config_local_path"], "/M0")
        self.assertEqual(row["config_context_path"], "/I1")
        self.assertEqual(row["config_context"]["view"], "nested_config")
        self.assertTrue(row["fixed_library"])
        self.assertTrue(row["fixed_cell"])
        self.assertFalse(row["fixed_view"])

    def test_clone_chain_retains_bound_config_and_resolved_implementation(self):
        row = next(row for row in self.normalized()["parameters"] if row["hierarchy_path"] == "/I1/M0")
        self.assertEqual(row["owner_occurrence_path"], "/I1")
        occurrence = row["occurrence_chain"][0]
        self.assertEqual(occurrence["master_view"], "symbol")
        self.assertEqual(occurrence["bound_view"], "nested_config")
        self.assertEqual(occurrence["resolved_view"], "schematic_alt")
        self.assertEqual(occurrence["resolved_view_type"], "schematic")

    def test_ignored_objects_are_complete_readonly_metadata(self):
        body = self.normalized()
        binding_row = next(row for row in body["config_bindings"] if row["hierarchy_path"] == "/I1/PIN0")
        instance = next(row for row in body["instances"] if row["hierarchy_path"] == "/I1/PIN0")
        parameter_row = next(row for row in body["parameters"] if row["hierarchy_path"] == "/I1/PIN0")
        self.assertTrue(binding_row["netlister_ignored"])
        self.assertTrue(instance["netlister_ignored"])
        self.assertTrue(parameter_row["netlister_ignored"])
        self.assertFalse(parameter_row["tunable"])
        self.assertIn("ignored", parameter_row["parameter_policy_reason"])

    def test_non_subdesign_primitive_is_bound_and_remains_tunable(self):
        extra = [binding("/V1", "V1", "/", ("analogLib", "vdc", "symbol"),
                         ("USER", "REAL_TOP", "schematic_alt"),
                         ("analogLib", "vdc", "spectre"), ("analogLib", "vdc", "spectre"),
                         stop=True, pcdb_non_subdesign=True),
                 record("INSTANCE", "/V1", "V1", "analogLib", "vdc", "symbol", False, "/", 0, "schematicSymbol"),
                 parameter("/V1", ("USER", "REAL_TOP"))]
        body = self.normalized(extra)
        validate_native_hierarchy(body, REQUESTED)
        row = next(row for row in body["parameters"] if row["hierarchy_path"] == "/V1")
        self.assertTrue(row["pcdb_non_subdesign"])
        self.assertFalse(row["netlister_ignored"])
        self.assertEqual(row["bound_view"], "spectre")
        self.assertTrue(row["tunable"])

    def test_stop_points_have_no_synthetic_internal_cell(self):
        body = self.normalized()
        stop = next(row for row in body["instances"] if row["hierarchy_path"] == "/I1/M0")
        self.assertTrue(stop["stop"])
        self.assertFalse(stop["has_schematic"])
        self.assertNotIn("/I1/M0", {row["hierarchy_path"] for row in body["cells"]})

    def test_cell_choices_use_actual_cell_and_requested_design_identity(self):
        source = {"designs": [{"design": REQUESTED, "hierarchy": self.normalized()}]}
        with mock.patch("crescendo_engine.workspace.resolve_copy_libraries",
                        return_value=("USER", None, {"USER": "/private"})):
            scopes = cell_scope_records(source, setup={}, config={})
        self.assertEqual(len(scopes), 2)
        self.assertEqual(scopes[0]["cell"], "REAL_TOP")
        self.assertEqual(scopes[1]["view"], "schematic_alt")
        self.assertTrue(all(row["design"] == REQUESTED and row["selectable"] for row in scopes))

    def test_dependencies_are_deduplicated_without_merging_occurrences(self):
        body = self.normalized([record("CONFIG_DEP", "USER", "AMP", "nested_config")])
        self.assertEqual(len(body["config_dependencies"]), 2)
        self.assertEqual(len(body["config_bindings"]), 3)

    def test_repeated_raw_master_keeps_distinct_occurrence_implementation(self):
        extra = [binding("/I2", "I2", "/", ("USER", "AMP", "symbol"),
                         ("USER", "REAL_TOP", "schematic_alt"),
                         ("USER", "AMP", "schematic"), ("USER", "AMP", "schematic")),
                 record("INSTANCE", "/I2", "I2", "USER", "AMP", "symbol", True, "/", 0, "schematicSymbol"),
                 record("CELL", "/I2", "USER", "AMP", "schematic", 1)]
        body = self.normalized(extra)
        validate_native_hierarchy(body, REQUESTED)
        by_path = {row["hierarchy_path"]: row for row in body["instances"]}
        self.assertEqual(by_path["/I1"]["master_cell"], by_path["/I2"]["master_cell"])
        self.assertEqual(by_path["/I1"]["resolved_view"], "schematic_alt")
        self.assertEqual(by_path["/I2"]["resolved_view"], "schematic")

    def test_missing_binding_is_rejected_even_if_root_opened(self):
        body = self.normalized()
        body["config_bindings"].pop()
        with self.assertRaisesRegex(RuntimeError, "binding evidence does not match"):
            validate_native_hierarchy(body, REQUESTED)

    def test_duplicate_binding_is_rejected(self):
        body = self.normalized()
        body["config_bindings"].append(copy.deepcopy(body["config_bindings"][0]))
        with self.assertRaisesRegex(RuntimeError, "binding evidence does not match"):
            validate_native_hierarchy(body, REQUESTED)

    def test_missing_resolved_implementation_is_rejected(self):
        body = self.normalized()
        body["cells"] = body["cells"][:1]
        with self.assertRaisesRegex(RuntimeError, "implementation was not opened at /I1"):
            validate_native_hierarchy(body, REQUESTED)

    def test_resolved_top_mismatch_is_rejected(self):
        body = self.normalized()
        body["resolved_top"]["cell"] = "UNRELATED"
        with self.assertRaisesRegex(RuntimeError, "resolved top does not match"):
            validate_native_hierarchy(body, REQUESTED)

    def test_config_dependencies_without_root_identity_are_rejected(self):
        body = self.normalized()
        body["resolved_top"] = None
        with self.assertRaisesRegex(RuntimeError, "no complete resolved top"):
            validate_native_hierarchy(body, REQUESTED)

    def test_malformed_native_config_record_is_rejected(self):
        body = normalize_probe([record("RESOLVED_TOP", None),
                                record("CONFIG_DEP", "CFG", "TB", "config")], REQUESTED)
        with self.assertRaisesRegex(RuntimeError, "malformed resolved top identity"):
            validate_native_hierarchy(body, REQUESTED)

    def test_actual_config_failure_does_not_misreport_readonly_root(self):
        body = normalize_probe([record("CONFIG_BIT_RULES_UNSUPPORTED", "/", "unsupported bit rule")], REQUESTED)
        with self.assertRaises(RuntimeError) as caught:
            validate_native_hierarchy(body, REQUESTED)
        self.assertIn("unsupported bit rule", str(caught.exception))
        self.assertNotIn("root cell was not opened", str(caught.exception))

    def test_config_cycles_and_depth_limits_reject_partial_graph(self):
        for kind in ("CYCLE", "DEPTH_LIMIT"):
            with self.subTest(kind=kind):
                body = self.normalized([record(kind, "/I1")])
                with self.assertRaisesRegex(RuntimeError, "config traversal"):
                    validate_native_hierarchy(body, REQUESTED)

    def test_generated_probe_uses_only_public_readonly_hdb_pcdb(self):
        skill = probe_skill("CFG", "TB", "config", "/private/records", 32,
                            cells_only=True)
        self.assertIn('hdbOpen(libName cellName viewName "r" "CDBA")', skill)
        self.assertIn('pcdbOpen(libName cellName viewName "r" "CDBA")', skill)
        self.assertIn("pcdbInstMasterLibFixed(header)", skill)
        self.assertIn("pcdbInstMasterCellFixed(header)", skill)
        self.assertIn("pcdbInstMasterViewFixed(header)", skill)
        self.assertIn("pcdbGetIgnoredInstMasterGen", skill)
        self.assertIn("hdbHasIterInstBitRules", skill)
        self.assertIn('inst~>nlAction == "ignore"', skill)
        self.assertNotIn("ignored = nth(6 flags)", skill)
        self.assertNotIn("hdbSave", skill)
        self.assertNotIn("hdbSet", skill)


if __name__ == "__main__":
    unittest.main()
