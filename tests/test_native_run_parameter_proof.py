import copy
import json
import os
import tempfile
import sqlite3
import unittest

from crescendo_engine.native_maestro_parameters import (
    build_native_parameter_plan, parse_native_parameter_settings,
    verify_native_parameter_application,
)
from crescendo_engine.private_netlist import (
    PrivateNetlistUnsupported, _native_qualification_values,
    _symbolize_native_dpar_phase, qualify_symbolic_netlist,
    _normalize_global_dependency_defaults,
    _capture_worker_logs,
    _normalize_owned_corner_parameters,
)


class NativeRunParameterProofTests(unittest.TestCase):
    def test_only_previously_proven_owned_corner_parameter_can_be_normalized(self):
        path = "L/C/private/RSEL/r"
        corner = {"name": "C0", "native_content": {"children": [{"tag": "parameters", "text": "",
            "attributes": {}, "children": [{"tag": "location", "text": "L/C/private/RSEL", "attributes": {},
                "children": [{"tag": "parameter", "text": "r", "attributes": {},
                    "children": [{"tag": "value", "text": "5k", "attributes": {}, "children": []}]}]}]}]}}
        work = {"native_parameter_paths": [path], "native_parameter_last_values": {path: 5000}}
        normalized, proof = _normalize_owned_corner_parameters([corner], work)
        self.assertEqual(normalized[0]["native_content"]["children"], [])
        self.assertEqual(proof[0]["path"], path)
        for state in ({}, dict(work, native_parameter_last_values={path: 6000}),
                      dict(work, native_parameter_paths=["L/C/other/RSEL/r"])):
            normalized, proof = _normalize_owned_corner_parameters([corner], state)
            self.assertEqual(normalized, [corner])
            self.assertEqual(proof, [])
        foreign_empty = {"name": "C0", "native_content": {"children": [{"tag": "parameters",
            "text": "", "attributes": {}, "children": [{"tag": "location",
                "text": "L/C/foreign/OTHER", "attributes": {}, "children": []}]}]}}
        self.assertEqual(_normalize_owned_corner_parameters([foreign_empty], work)[0], [foreign_empty])

    def test_worker_log_keeps_fresh_pre_generation_errors_and_ignores_wrapper_aftermath(self):
        with tempfile.TemporaryDirectory() as root:
            directory = os.path.join(root, "logs_root", "logs0")
            os.makedirs(directory)
            path = os.path.join(directory, "Job1.log")
            with open(path, "w") as stream:
                stream.write('Run start for Point ID (0 1)\nRunning simulation on testbench [ TB ]\n*Error* Simulator failed\n')
            work = {"root": root}
            _unused, before = _capture_worker_logs(work)
            report, _unused = _capture_worker_logs(work, {})
            self.assertTrue(report["ok"])
            self.assertEqual(report["logs"][0]["error_lines"], [])
            self.assertEqual(report["logs"][0]["expected_wrapper_aftermath_error_count"], 1)
            self.assertEqual(_capture_worker_logs(work, before)[0]["logs"], [])
            with open(path, "a") as stream:
                stream.write('Run start for Point ID (0 2)\n*Error* int: FN_gm from C710_coremos_CB\n')
            report, _unused = _capture_worker_logs(work, before)
            self.assertFalse(report["ok"])
            self.assertEqual(report["logs"][0]["error_lines"][0]["line"], 5)
            self.assertIn("FN_gm", report["logs"][0]["error_lines"][0]["text"])

    def test_fresh_worker_log_overflow_is_not_a_success_and_unchanged_old_logs_do_not_repeat(self):
        with tempfile.TemporaryDirectory() as root:
            directory = os.path.join(root, "logs_root", "logs0")
            os.makedirs(directory)
            for index in range(129):
                with open(os.path.join(directory, "Job%d.log" % index), "w") as stream:
                    stream.write("native prior capture\n")
            work = {"root": root}
            _unused, before = _capture_worker_logs(work)
            self.assertTrue(_capture_worker_logs(work, before)[0]["ok"])
            report, _unused = _capture_worker_logs(work, {})
            self.assertFalse(report["ok"])
            self.assertIn("128", report["unreadable_logs"][0]["error"])

    def test_native_global_save_only_materializes_enabled_default(self):
        row = {"name": "FN_gm", "value": "4", "native_content": {"children": [
            {"tag": "dependenttests", "children": [{"tag": "dependenttest",
                "text": "TB", "attributes": {"omitted": "0"}}]}]}}
        explicit = copy.deepcopy(row)
        explicit["native_content"]["children"][0]["children"][0]["attributes"]["enabled"] = "1"
        self.assertEqual(_normalize_global_dependency_defaults([row]),
                         _normalize_global_dependency_defaults([explicit]))
        explicit["native_content"]["children"][0]["children"][0]["attributes"]["enabled"] = "0"
        self.assertNotEqual(_normalize_global_dependency_defaults([row]),
                            _normalize_global_dependency_defaults([explicit]))
        self.assertNotIn("enabled", row["native_content"]["children"][0]["children"][0]["attributes"])

    def row(self, **changes):
        row = {"parameter_id": "p", "library": "L", "cell": "C", "view": "schematic",
               "instance": "RSEL", "property": "r", "value": "5k"}
        row.update(changes)
        return row

    def text(self, dpar="5k", global_value="3k", selected="DPAR_1"):
        return ("simulator lang=spectre\nparameters DPAR_1=%s FN_gm=%s\n"
                "// Library name: L\n// Cell name: C\n// View name: schematic\n"
                "RSEL (a 0) resistor r=%s area=(DPAR_1 * FN_gm)\n"
                "RKEEP (a 0) resistor r=FN_gm\n") % (dpar, global_value, selected)

    def task(self, root, text, point=1):
        directory = os.path.join(root, str(point), "netlist")
        os.makedirs(directory)
        path = os.path.join(directory, "input.scs")
        with open(path, "w") as stream:
            stream.write(text)
        return {"point_id": point, "test": "TB", "corner": "C%d" % point,
                "task_id": "p%d" % point, "netlist_path": path, "cwd": directory,
                "template_files": [{"path": path, "relative_path": "input.scs"}]}

    def test_numeric_getter_and_unit_string_are_equivalent_with_raw_type_retained(self):
        with tempfile.TemporaryDirectory() as root:
            plan = build_native_parameter_plan([self.row()], ["C0"])
            path = os.path.join(root, "parameters.tsv")
            with open(path, "w") as stream:
                stream.write('CRESCENDO_NATIVE_PARAMETERS_2\nPOLICY\tt\tt\tt\n')
                stream.write('SETTING\t"p"\t"L/C/schematic/RSEL/r"\t"test"\tnil\t5000\tfixnum\n')
                stream.write('SETTING\t"p"\t"L/C/schematic/RSEL/r"\t"corner"\t"C0"\t"5k"\tstring\n')
                stream.write('DONE\t"native_override_settings_only"\n')
            proof = parse_native_parameter_settings(path, plan)
            self.assertEqual(proof["records"][0]["raw_value"], 5000)
            self.assertEqual(proof["records"][0]["raw_type"], "fixnum")
            self.assertFalse(proof["point_execution_proven"])

    def test_wrapped_native_identity_preserves_exact_private_view_gate(self):
        with tempfile.TemporaryDirectory() as root:
            view = 'cr_s_12345678_long_private_maestro_view_abcdef012345'
            text = self.text().replace('// Library name: L', '// Library name:\n// L').replace('// Cell name: C', '// Cell name:\n// C').replace('// View name: schematic', '// View name:\n// ' + view + '\n// Inherited view list:\n// ' + view)
            task = self.task(root, text)
            proof = verify_native_parameter_application([self.row(view=view)], [task], {'p': 5000})
            self.assertEqual(proof['parameters'][0]['effective'], 5000)
            with self.assertRaisesRegex(ValueError, 'absent/ambiguous'):
                verify_native_parameter_application([self.row(view='wrong')], [task], {'p': 5000})

    def test_missing_wrapped_view_does_not_consume_comments_or_identity_headers(self):
        with tempfile.TemporaryDirectory() as root:
            for index, tail in enumerate(('// View name:\n// Inherited view list:\n// schematic', '// View name:\n// schematic\nRKEEP (a 0) resistor r=1k', '// View name:\n// spectre', '// View name:\n// Cell name: C')):
                text = self.text().replace('// View name: schematic', tail)
                task = self.task(root, text, index + 1)
                with self.assertRaisesRegex(ValueError, 'absent/ambiguous'):
                    verify_native_parameter_application([self.row()], [task], {'p': 5000})

    def test_each_point_keeps_original_global_but_selected_uses_native_dpar(self):
        with tempfile.TemporaryDirectory() as root:
            tasks = [self.task(root, self.text(global_value="3k"), 1),
                     self.task(root, self.text(global_value="4k"), 2)]
            proof = verify_native_parameter_application([self.row()], tasks, {"p": 5000})
            self.assertEqual(proof["parameters"][0]["effective"], 5000)
            self.assertEqual(len(proof["parameters"][0]["points"]), 2)
            self.assertEqual(proof["parameters"][0]["points"][0]["dpar_values"], {"DPAR_1": 5000})
            self.assertFalse(proof["private_oa_backannotation_proven"])
            for task in tasks:
                with open(task["netlist_path"]) as stream:
                    self.assertIn("RKEEP (a 0) resistor r=FN_gm", stream.read())

    def test_range_can_retain_adjustment_but_candidate_cannot_accept_it(self):
        with tempfile.TemporaryDirectory() as root:
            tasks = [self.task(root, self.text(dpar="2"))]
            with self.assertRaisesRegex(ValueError, "changed selected candidate"):
                verify_native_parameter_application([self.row()], tasks, {"p": 1})
            proof = verify_native_parameter_application([self.row()], tasks, {"p": 1}, allow_effective_adjustment=True)
            self.assertEqual(proof["parameters"][0]["effective"], 2)
            self.assertFalse(proof["parameters"][0]["points"][0]["matches_requested"])

    def test_no_first_corner_choice_when_effective_values_differ(self):
        with tempfile.TemporaryDirectory() as root:
            tasks = [self.task(root, self.text(dpar="2"), 1), self.task(root, self.text(dpar="3"), 2)]
            proof = verify_native_parameter_application([self.row()], tasks, {"p": 1}, allow_effective_adjustment=True)
            self.assertIsNone(proof["parameters"][0]["effective"])
            self.assertEqual([point["effective"] for point in proof["parameters"][0]["points"]], [2, 3])

    def test_unbound_dpar_or_wrong_private_instance_never_passes(self):
        with tempfile.TemporaryDirectory() as root:
            tasks = [self.task(root, self.text(selected="DPAR_missing"))]
            with self.assertRaises(ValueError):
                verify_native_parameter_application([self.row()], tasks, {"p": 5000})
            with self.assertRaises(ValueError):
                verify_native_parameter_application([self.row(cell="original")], tasks, {"p": 5000})

    def test_single_custom_cdf_field_can_use_unique_dpar_without_claiming_oa_update(self):
        with tempfile.TemporaryDirectory() as root:
            tasks = [self.task(root, self.text())]
            proof = verify_native_parameter_application([self.row(property="fw")], tasks, {"p": 5000})
            point = proof["parameters"][0]["points"][0]
            self.assertEqual(point["proof_source"], "unique_selected_instance_DPAR")
            self.assertFalse(point["callback_adjustment_proven"])
            with self.assertRaises(ValueError):
                verify_native_parameter_application([self.row(property="fw"), self.row(parameter_id="q", property="length")], tasks, {"p": 5000, "q": 1})

    def test_multiple_native_companions_use_exact_rdb_input_without_claiming_clamp(self):
        with tempfile.TemporaryDirectory() as root:
            text = self.text().replace("parameters DPAR_1=5k", "parameters DPAR_2=3u DPAR_1=6").replace(
                "resistor r=DPAR_1 area=(DPAR_1 * FN_gm)", "mos m=(1)*(DPAR_1) area=DPAR_2")
            tasks = [self.task(root, text)]
            path = os.path.join(root, "history.rdb")
            connection = sqlite3.connect(path)
            connection.executescript('''CREATE TABLE parameter(parameterID INTEGER, name TEXT,
                testID TEXT, lib TEXT, cell TEXT, view TEXT, instance TEXT, type TEXT);
                CREATE TABLE parameterValue(parameterID INTEGER,pointID INTEGER,value NUMERIC);
                CREATE TABLE test(testID INTEGER,name TEXT);
                INSERT INTO parameter VALUES(1,'fingers','','L','C','schematic','RSEL','deviceParam');
                INSERT INTO parameter VALUES(2,'fingers','','L','C','schematic','RKEEP','deviceParam');
                INSERT INTO parameterValue VALUES(1,1,6);INSERT INTO parameterValue VALUES(2,1,999);''')
            connection.close()
            proof = verify_native_parameter_application([self.row(property="fingers")], tasks,
                {"p": 6}, parsed={"path": path})
            point = proof["parameters"][0]["points"][0]
            self.assertEqual(point["effective"], 6)
            self.assertEqual(point["proof_source"], "full_identity_native_RDB_input")
            self.assertEqual(point["native_input_proof"]["identity"]["instance"], "RSEL")
            self.assertFalse(point["callback_adjustment_proven"])
            self.assertEqual(point["dpar_values"], {})
            with self.assertRaises(ValueError):
                verify_native_parameter_application([self.row(property="other")], tasks,
                    {"p": 6}, parsed={"path": path})

    def test_template_replaces_only_dpar_declaration_and_qualifies_native_derived_formula(self):
        with tempfile.TemporaryDirectory() as root:
            task = self.task(root, self.text())
            application = verify_native_parameter_application([self.row()], [task], {"p": 5000})
            numeric = {"tasks": [task], "parameter_application": application}
            phase = _symbolize_native_dpar_phase(numeric, {"p": "CRESCENDO_NL_p"}, os.path.join(root, "symbolic"))
            with open(phase["tasks"][0]["netlist_path"]) as stream:
                text = stream.read()
            self.assertIn("DPAR_1=CRESCENDO_NL_p", text)
            self.assertIn("area=(DPAR_1 * FN_gm)", text)
            references = [{"values": {"p": 4000}, "text": self.text(dpar="4k")},
                          {"values": {"p": 6000}, "text": self.text(dpar="6k")}]
            self.assertTrue(qualify_symbolic_netlist(text, references, {"p": "CRESCENDO_NL_p"},
                {"p": ("L", "C", "schematic", "RSEL")})["ok"])
            escaped = text.replace("RKEEP (a 0) resistor r=FN_gm", "RKEEP (a 0) resistor r=DPAR_1")
            with self.assertRaises(PrivateNetlistUnsupported):
                qualify_symbolic_netlist(escaped, references, {"p": "CRESCENDO_NL_p"}, {"p": ("L", "C", "schematic", "RSEL")})

    def test_explicit_probe_domain_does_not_claim_to_be_original_corner_value(self):
        with tempfile.TemporaryDirectory() as root:
            tasks = [self.task(root, self.text(selected="FN_gm", global_value="4"), 1),
                     self.task(root, self.text(selected="FN_gm", global_value="8"), 2)]
            parameter = {"parameter_id": "p", "name": "r", "cdf_type": "int", "current_value": "FN_gm",
                         "owner_library": "L", "owner_cell": "C", "owner_occurrence_path": "/",
                         "hierarchy_path": "/RSEL", "design_library": "L", "design_cell": "C", "design_view": "schematic"}
            work = {"copied_libraries": {"L": root}, "setup": {}}
            baseline = {"tasks": tasks}
            with self.assertRaisesRegex(PrivateNetlistUnsupported, "differs across native points"):
                _native_qualification_values([parameter], {}, baseline, work)
            seed, low, high, origins = _native_qualification_values([parameter], {"variables": [
                {"parameter_id": "p", "minimum": 2, "maximum": 10, "value_type": "integer"}]}, baseline, work)
            self.assertEqual((seed["p"], low["p"], high["p"]), (6, 2, 10))
            self.assertEqual(origins["p"]["role"], "qualification_probe")
            self.assertFalse(origins["p"]["baseline_value_claimed"])


if __name__ == "__main__":
    unittest.main()
