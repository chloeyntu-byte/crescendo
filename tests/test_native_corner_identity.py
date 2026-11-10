import copy
import hashlib
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from crescendo_engine.private_netlist import (
    PrivateNetlistUnsupported, _capture_setup_identity, _saved_corner_contract,
    validate_capture_corners,
    parse_native_corner_schedule, native_corner_schedule_skill, qualify_native_corner_mapping,
)
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine import runner
from crescendo_engine.utils import load_json
from crescendo_engine.rdb import aggregate_output
from crescendo_engine.netlist_measurement import parsed_results
from test_netlist_optimizer import fixtures


def snapshot():
    return {"ok": True, "saved_source": {"corner_read_ok": True},
            "private_setup": {"corner_read_ok": True}}


def setup(corners, tests=("a",)):
    return {"tests": [{"name": name, "enabled": True} for name in tests],
            "corners": [{"name": name, "enabled": True} for name in corners]}


def task(test, corner, point):
    return {"test": test, "corner": corner, "point_id": point,
            "netlist_path": "/owned/%s/%s/input.scs" % (test, point)}


class NativeCornerCoverageTests(unittest.TestCase):
    def schedule(self, groups):
        return {"read_ok": True, "groups": dict((name, {"enabled": True, "count": count, "disabled_tests": []}) for name, count in groups),
                "nominal_enabled": False, "nominal_tests": {"a": False, "b": False}}

    def test_native_schedule_generation_retains_native_format_arguments(self):
        lines = native_corner_schedule_skill(setup(("SS",)), "/owned/schedule.tsv", "nativeSDB")
        text = "\n".join(lines)
        self.assertIn('"NOMINAL\\t%d\\n"', text)
        self.assertIn('"NOMINAL_TEST\\t%L\\t%d\\n" "a"', text)
        self.assertIn('axlGetCornerCountForName(nativeSDB crescCornerName)', text)

    def test_group_helper_preserves_independent_variable_validation_and_commits_aliases_atomically(self):
        current = setup(("SS",), ("a", "b"))
        current["tests"][0]["variables"] = [{"name": "VDD", "value": "1.2"}]
        rows = [task("a", "SS", 1), task("b", "FF", 2)]
        detail = qualify_native_corner_mapping(current, rows, {}, snapshot(), self.schedule([("SS", 1)]))
        self.assertFalse(detail["ok"])
        self.assertTrue(all("source_corner" not in row for row in rows))
        rows[1]["corner"] = "SS"
        detail = qualify_native_corner_mapping(current, rows, {}, snapshot(), self.schedule([("SS", 1)]))
        self.assertTrue(detail["ok"])
        self.assertEqual(detail["scope"], "native_test_corner_group_and_actual_point_coverage")
        self.assertFalse(validate_capture_corners(current, rows, {}, snapshot(), self.schedule([("SS", 1)]))["ok"])

    def test_native_unique_expanded_group_retains_all_actual_points_and_source_alias(self):
        current = setup(("Fhigh",))
        current["tests"][0]["variables"] = [{"name": "SW", "value": "1"}]
        current["corners"][0]["variables"] = [{"name": "SW", "value": "0 1"}]
        tasks = [task("a", "Fhigh_0", 1), task("a", "Fhigh_1", 2)]
        for number, row in enumerate(tasks):
            row["native_parameters"] = [{"name": "SW", "value": number, "expression": str(number)}]
        result = validate_capture_corners(current, tasks, {}, snapshot(), self.schedule([("Fhigh", 2)]))
        self.assertTrue(result["ok"])
        self.assertEqual([row["corner"] for row in tasks], ["Fhigh_0", "Fhigh_1"])
        self.assertEqual([row["source_corner"] for row in tasks], ["Fhigh", "Fhigh"])
        self.assertEqual([row["point_id"] for row in tasks], [1, 2])

    def test_exact_regular_corner_then_unique_remaining_group_are_supported(self):
        current = setup(("TT", "Sweep"))
        tasks = [task("a", "TT", 1), task("a", "opaque_native_left", 2), task("a", "opaque_native_right", 3)]
        result = validate_capture_corners(current, tasks, {}, snapshot(), self.schedule([("TT", 1), ("Sweep", 2)]))
        self.assertTrue(result["ok"])
        self.assertEqual([row["source_corner"] for row in tasks], ["TT", "Sweep", "Sweep"])

    def test_missing_sweep_point_and_ambiguous_groups_are_rejected(self):
        tasks = [task("a", "Sweep_0", 1)]
        self.assertFalse(validate_capture_corners(setup(("Sweep",)), tasks, {}, snapshot(), self.schedule([("Sweep", 2)]))["ok"])
        tasks = [task("a", "Sweep_0", 1), task("a", "Sweep_1", 2), task("a", "other_0", 3), task("a", "other_1", 4)]
        result = validate_capture_corners(setup(("Sweep", "other")), tasks, {}, snapshot(), self.schedule([("Sweep", 2), ("other", 2)]))
        self.assertFalse(result["ok"])
        self.assertEqual(result["test_differences"][0]["mapping_error"], "native_expanded_corner_group_mapping_is_ambiguous")
        self.assertTrue(all("source_corner" not in row for row in tasks))

    def test_native_enabled_group_and_test_mask_require_source_consistency(self):
        current = setup(("SS", "FF"), ("a", "b"))
        schedule = self.schedule([("SS", 1), ("FF", 1)])
        schedule["groups"]["SS"]["disabled_tests"] = ["b"]
        schedule["groups"]["FF"]["disabled_tests"] = ["a"]
        tasks = [task("a", "SS", 1), task("b", "FF", 2)]
        self.assertTrue(validate_capture_corners(current, tasks, {}, snapshot(), schedule)["ok"])
        schedule["groups"]["FF"]["enabled"] = False
        self.assertFalse(validate_capture_corners(current, tasks[:1], {}, snapshot(), schedule)["ok"])

    def test_source_group_reducers_cover_every_native_sweep_point(self):
        results = []
        for point, value in enumerate((10.0, 30.0), 1):
            row = task("a", "Fhigh_%d" % (point - 1), point)
            row["source_corner"] = "Fhigh"
            results.append({"task": row, "measurement": {"outputs": [{"key": "a::gain", "name": "gain", "value": value,
                                                       "expression": "gain", "output_type": "point"}]}})
        parsed = parsed_results(results)
        for reducer, expected in (("min", 10.0), ("mean", 20.0), ("max", 30.0)):
            self.assertEqual(aggregate_output(parsed, "a::gain", reducer, ["Fhigh"]), expected)
        self.assertEqual(aggregate_output(parsed, "a::gain", "mean", ["Fhigh_0"]), 10.0)
        values = parsed["output_map"]["a::gain"]["values"]
        self.assertEqual([(row["corner"], row["source_corner"]) for row in values], [("Fhigh_0", "Fhigh"), ("Fhigh_1", "Fhigh")])

    def test_native_schedule_requires_done_marker_and_explicit_readback(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "schedule.tsv")
            text = 'GROUP\t"Fhigh"\t1\t2\nDISABLED\t"Fhigh"\t"b"\nNOMINAL\t0\nNOMINAL_TEST\t"a"\t0\n'
            with open(path, "w") as stream:stream.write(text)
            with self.assertRaises(PrivateNetlistUnsupported):parse_native_corner_schedule(path)
            with open(path, "a") as stream:stream.write('READ_OK\n')
            parsed = parse_native_corner_schedule(path)
            self.assertEqual(parsed["groups"]["Fhigh"], {"enabled": True, "count": 2, "disabled_tests": ["b"]})

    def test_explicit_nominal_and_default_are_complete_named_corners(self):
        current = setup(("FF", "nominal", "_default"))
        tasks = [task("a", name, index) for index, name in enumerate(("_default", "FF", "nominal"))]
        result = validate_capture_corners(current, tasks, {}, snapshot())
        self.assertTrue(result["ok"])
        self.assertEqual(result["test_differences"][0]["expected"], ["FF", "_default", "nominal"])

    def test_single_corner_and_order_are_not_an_identity_mismatch(self):
        current = setup(("SS",))
        self.assertTrue(validate_capture_corners(current, [task("a", "SS", 4)], {}, snapshot())["ok"])
        current = setup(("SS", "FF"), ("a", "b"))
        tasks = [task("b", "FF", 7), task("a", "SS", 8), task("a", "FF", 7), task("b", "SS", 8)]
        self.assertTrue(validate_capture_corners(current, tasks, {}, snapshot())["ok"])

    def test_disabled_corner_is_not_required_or_permitted(self):
        current = setup(("SS",))
        current["corners"].append({"name": "FF", "enabled": False})
        self.assertTrue(validate_capture_corners(current, [task("a", "SS", 0)], {}, snapshot())["ok"])
        result = validate_capture_corners(current, [task("a", "SS", 0), task("a", "FF", 1)], {}, snapshot())
        self.assertFalse(result["ok"])
        self.assertEqual(result["test_differences"][0]["unexpected"], ["FF"])

    def test_test_context_detects_global_union_false_positive(self):
        current = setup(("SS", "FF"), ("a", "b"))
        result = validate_capture_corners(current, [task("a", "SS", 0), task("b", "FF", 1)], {}, snapshot())
        self.assertFalse(result["ok"])
        self.assertEqual([row["missing"] for row in result["test_differences"]], [["FF"], ["SS"]])

    def test_implicit_default_requires_successful_saved_source_and_private_read(self):
        current = setup(())
        tasks = [task("a", "_default", 0)]
        self.assertTrue(validate_capture_corners(current, tasks, {}, snapshot())["ok"])
        self.assertFalse(validate_capture_corners(current, tasks, {}, {})["ok"])
        self.assertFalse(validate_capture_corners(current, [task("a", "nominal", 0)], {}, snapshot())["ok"])

    def test_duplicate_points_and_source_corners_are_rejected_without_deduplication(self):
        current = setup(("SS",))
        row = task("a", "SS", 0)
        self.assertFalse(validate_capture_corners(current, [row, dict(row)], {}, snapshot())["ok"])
        self.assertFalse(validate_capture_corners(setup(("SS", "SS")), [row], {}, snapshot())["ok"])
        self.assertTrue(validate_capture_corners(current, [row, task("a", "SS", 1)], {}, snapshot())["ok"])

    def test_same_names_wrong_native_variable_value_is_rejected(self):
        current = setup(("SS",))
        current["tests"][0]["variables"] = [{"name": "VDD", "value": "1.2"}]
        current["corners"][0]["variables"] = [{"name": "VDD", "value": "0.9"}]
        row = task("a", "SS", 0)
        row["native_parameters"] = [{"name": "VDD", "value": 1.2, "expression": "1.2"}]
        result = validate_capture_corners(current, [row], {}, snapshot())
        self.assertFalse(result["ok"])
        self.assertEqual(result["test_differences"][0]["variable_mismatches"][0]["expected_value"], .9)
        row["native_parameters"][0]["value"] = .9
        self.assertTrue(validate_capture_corners(current, [row], {}, snapshot())["ok"])


class SavedCornerIdentityTests(unittest.TestCase):
    SOURCE = '<setupdb><active><currentmode>SRSC</currentmode><corners><corner enabled="1">SS<temperature>125</temperature><model section="ss">/pdk/models.scs</model><vars><var>VDD<value>0.9</value></var></vars></corner><corner enabled="0">FF</corner></corners><tests><test enabled="1">a<tooloptions><option>sim<value>spectre</value></option></tooloptions><vars><var>VDD<value>1.2</value></var></vars></test></tests></active></setupdb>'

    def fixture(self, root, private_text=None):
        original = os.path.join(root, "source.sdb")
        private = os.path.join(root, "private.sdb")
        for path, text in ((original, self.SOURCE), (private, private_text or self.SOURCE)):
            with open(path, "w") as stream:
                stream.write(text)
        contract = _saved_corner_contract(original)
        current = {"source": {"setup_db": original, "sha256": contract["sha256"]},
                   "mode": contract["mode"], "corners": [{key: row[key] for key in ("name", "enabled", "variables")} for row in contract["corners"]],
                   "tests": contract["tests"]}
        return {"setup": current, "setup_sdb": private}

    def test_valid_saved_clone_with_design_binding_unrelated_fields_keeps_corner_identity(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = self.fixture(root, self.SOURCE.replace('<option>sim', '<option>view<value>private_config</value></option><option>sim'))
            self.assertTrue(_capture_setup_identity(workspace, root)["ok"])

    def test_model_section_temperature_variable_and_enable_changes_are_rejected(self):
        for before, after in (('section="ss"', 'section="tt"'), ('125', '27'), ('0.9', '1.2'), ('enabled="0"', 'enabled="1"')):
            with self.subTest(field=before), tempfile.TemporaryDirectory() as root:
                workspace = self.fixture(root, self.SOURCE.replace(before, after))
                with self.assertRaises(PrivateNetlistUnsupported) as caught:
                    _capture_setup_identity(workspace, root)
                detail = load_json(caught.exception.detail["diagnostic_path"])
                self.assertFalse(detail["ok"])
                self.assertEqual(detail["differences"][0]["reason"], "private_setup_clone_changed_semantics")

    def test_only_proven_private_model_path_relocation_is_normalized(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "models.scs")
            with open(target, "w") as stream:stream.write("library models\nsection ss\nendsection ss\nendlibrary models\n")
            with open(target, "rb") as stream:target_sha256 = hashlib.sha256(stream.read()).hexdigest()
            workspace = self.fixture(root, self.SOURCE.replace("/pdk/models.scs", target))
            workspace["model_paths"] = {"references": [{"file": workspace["setup_sdb"],
                "changed": True, "directory": False, "original": "/pdk/models.scs", "resolved": target,
                "target_identity": {"size": os.path.getsize(target), "sha256": target_sha256}}]}
            detail = _capture_setup_identity(workspace, root)
            self.assertTrue(detail["ok"])
            self.assertEqual(len(detail["verified_model_path_normalizations"]), 1)
            with open(target, "a") as stream:stream.write("// changed model\n")
            with self.assertRaises(PrivateNetlistUnsupported):
                _capture_setup_identity(workspace, root)

    def test_unrelated_global_include_directory_remap_does_not_block_corners(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "global_models")
            os.mkdir(target)
            original_xml = self.SOURCE.replace('</currentmode>', '</currentmode><field Name="includePath">/old/global_models</field>')
            workspace = self.fixture(root, original_xml.replace('/old/global_models', target))
            with open(workspace["setup"]["source"]["setup_db"], "w") as stream:stream.write(original_xml)
            workspace["setup"]["source"]["sha256"] = _saved_corner_contract(workspace["setup"]["source"]["setup_db"])["sha256"]
            workspace["model_paths"] = {"references": [{"file": workspace["setup_sdb"],
                "changed": True, "directory": True, "original": "/old/global_models", "resolved": target,
                "target_identity": {"size": None, "sha256": None}}]}
            detail = _capture_setup_identity(workspace, root)
            self.assertTrue(detail["ok"])
            self.assertEqual(detail["verified_model_path_normalizations"], [])

    def test_corner_directory_requires_its_own_identity_and_paths_match_whole_tokens(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "corner_models")
            os.mkdir(target)
            corner_source = self.SOURCE.replace('<temperature>', '<field Name="includePath">/old/corner_models</field><temperature>')
            workspace = self.fixture(root, corner_source.replace('/old/corner_models', target))
            with open(workspace["setup"]["source"]["setup_db"], "w") as stream:stream.write(corner_source)
            workspace["setup"]["source"]["sha256"] = _saved_corner_contract(workspace["setup"]["source"]["setup_db"])["sha256"]
            workspace["model_paths"] = {"references": [{"file": workspace["setup_sdb"],
                "changed": True, "directory": True, "original": "/old/corner_models", "resolved": target,
                "target_identity": {"size": None, "sha256": None}}]}
            with self.assertRaises(PrivateNetlistUnsupported) as caught:
                _capture_setup_identity(workspace, root)
            self.assertIn("corner-referenced directory remap", caught.exception.detail["corner_identity"]["error"])
            # A prefix of a corner path is not itself a referenced full token.
            workspace["model_paths"]["references"][0]["resolved"] = os.path.dirname(target)
            with self.assertRaises(PrivateNetlistUnsupported) as caught:
                _capture_setup_identity(workspace, root)
            self.assertEqual(caught.exception.detail["corner_identity"]["differences"][0]["reason"], "private_setup_clone_changed_semantics")

    def test_stale_source_hash_is_distinct_from_clone_difference(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = self.fixture(root)
            workspace["setup"]["source"]["sha256"] = "obsolete"
            with self.assertRaises(PrivateNetlistUnsupported) as caught:
                _capture_setup_identity(workspace, root)
            self.assertEqual(caught.exception.detail["corner_identity"]["differences"][0]["reason"], "inspected_source_is_stale")

    def test_native_discovered_owned_symbol_is_allowed_but_other_new_variable_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = self.fixture(root, self.SOURCE.replace('<var>VDD<value>1.2</value></var>', '<var>VDD<value>1.2</value></var><var>CRESCENDO_NL_owned<value></value></var>'))
            self.assertTrue(_capture_setup_identity(workspace, root, ['CRESCENDO_NL_owned'])["ok"])
            with self.assertRaises(PrivateNetlistUnsupported):
                _capture_setup_identity(workspace, root, ['CRESCENDO_NL_other'])

    def test_unreadable_or_invalid_corner_setup_never_becomes_implicit_nominal(self):
        with tempfile.TemporaryDirectory() as root:
            workspace = self.fixture(root)
            with open(workspace["setup_sdb"], "w") as stream:
                stream.write("not xml")
            with self.assertRaises(PrivateNetlistUnsupported) as caught:
                _capture_setup_identity(workspace, root)
            self.assertIn("could not be read", caught.exception.detail["corner_identity"]["error"])


class SharedPreparationFailureTests(unittest.TestCase):
    def test_source_mismatch_still_cleans_only_proven_owned_capture_before_raising(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            adapter.manifest = {"workspace": {}}
            adapter.context = mock.Mock()
            adapter.context.close.return_value = {"cleanup_proven": True}
            with mock.patch.object(adapter, "verify_sources", side_effect=RuntimeError("source changed")), \
                    mock.patch("crescendo_engine.netlist_optimizer.reconcile_run_preparations", return_value={"cleanup_proven": True}) as reconcile:
                with self.assertRaisesRegex(RuntimeError, "source changed"):
                    adapter.close()
                reconcile.assert_called_once()
            shutdown = load_json(os.path.join(root, "private_netlist", "shutdown.json"))
            self.assertEqual(shutdown["source_guard"]["ok"], False)
            self.assertEqual(shutdown["preparation_cleanup"]["cleanup_proven"], True)

    def test_twenty_waiters_have_one_full_failure_and_no_simulator_execution(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root, 50, 20)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared["parameters"], config, runner._failure_metrics)
            context = mock.Mock()
            context.unsafe.is_set.return_value = False
            failure = PrivateNetlistUnsupported("corner content differs")
            failure.detail = {"failure_stage": "native_corner_setup_identity", "expected": "ss", "actual": "tt"}
            with mock.patch("crescendo_engine.netlist_optimizer.NetlistExecutionContext", return_value=context), \
                    mock.patch("crescendo_engine.netlist_optimizer.prepare_private_netlist", side_effect=failure) as preparation:
                with ThreadPoolExecutor(max_workers=20) as pool:
                    rows = list(pool.map(lambda index: adapter.evaluate(index, 0, {"x": .5}), range(20)))
            preparation.assert_called_once()
            context.evaluate.assert_not_called()
            self.assertTrue(all(row["shared_preparation_failed"] and row["execution_started"] is False for row in rows))
            paths = {row["shared_preparation_failure"]["diagnostic_path"] for row in rows}
            self.assertEqual(len(paths), 1)
            central = load_json(paths.pop())
            self.assertEqual(central["preparation_attempt_count"], 1)
            self.assertEqual(central["affected_candidate_count"], 20)
            self.assertIn("traceback", central)
            for row in rows:
                diagnostic = load_json(os.path.join(root, "candidates", row["candidate_id"], "execution_diagnostics.json"))
                self.assertNotIn("traceback", diagnostic)
                self.assertEqual(diagnostic["detail"]["execution_started"], False)


if __name__ == "__main__":
    unittest.main()
