"""Unchanged native baseline must not enter a numeric CDF transaction."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist, workspace as workspace_module


class NativeBaselineCaptureTests(unittest.TestCase):
    def fixture(self, root):
        copied = os.path.join(root, "copied")
        os.makedirs(copied)
        work = {"root": root, "primary_library": "PRIVATE", "setup_cell": "TB",
                "setup_view": "maestro", "setup_dir": os.path.join(root, "maestro"),
                "copied_libraries": {"SRC": copied},
                "setup": {"tests": [{"name": "tb", "simulator": "spectre"}],
                          "global_variables": [{"name": "FN_gm", "value": "4"}],
                          "corners": []}}
        parameter = {"parameter_id": "p", "backend": "schematic", "owner_library": "SRC",
                     "owner_cell": "TB", "owner_occurrence_path": "/", "name": "nf",
                     "cdf_type": "int", "current_value": "FN_gm", "hierarchy_path": "/M0",
                     "design_library": "SRC", "design_cell": "TB", "design_view": "schematic"}
        config = {"cadence": {"ocean_bin": "/bin/true"}, "runtime": {}}
        return work, [parameter], config

    def run_capture(self, work, parameters, config, root, coverage_ok=True, capture_point=True):
        native_cwd = os.path.join(work["root"], "native", "1", "tb", "netlist")
        os.makedirs(native_cwd)
        text = "simulator lang=spectre\nparameters FN_gm=4\nM0 (d g 0 0) mos nf=FN_gm\n"
        with open(os.path.join(native_cwd, "input.scs"), "w") as stream:
            stream.write(text)
        rdb_dir = os.path.join(work["setup_dir"], "results", "maestro")
        os.makedirs(rdb_dir)
        rdb = os.path.join(rdb_dir, "Interactive.0.rdb")
        with open(rdb, "w") as stream:
            stream.write("fixture")
        parsed = {"path": rdb, "points": [{"pointID": 1, "corner_name": "Nominal", "designPointNumber": 1}],
                  "parameters": [{"pointID": 1, "name": "FN_gm", "value": "4"}],
                  "outputs": [], "test_statuses": [{"pointID": 1, "test_name": "tb"}]}
        def native_command(*args, **kwargs):
            # Model native artifacts without invoking Virtuoso, Spectre or a scheduler.
            with open(os.path.join(root, "patch.records"), "w") as stream:
                stream.write("")
            with open(os.path.join(root, "native_variable_scope.tsv"), "w") as stream:
                stream.write('TOTAL\tt\nVALUE\t"FN_gm"\t"4"\nGLOBAL\t"FN_gm"\tt\nTEST\t"FN_gm"\t"tb"\tt\nDONE\n')
            if capture_point:
                fields = [native_cwd, "input.scs", "+aps"]
                with open(os.path.join(root, "invocations", "capture.fixture"), "wb") as stream:
                    stream.write(("\0".join(fields) + "\0").encode("utf-8"))
            return {"returncode": 0, "guardian_cleanup": {"state": "dead"}}
        identity = {"saved_source": {"sha256": "source"}, "ok": True}
        coverage = {"ok": coverage_ok, "enabled_tests": ["tb"], "explicit_enabled_corners": []}
        with mock.patch.object(private_netlist, "_capture_setup_identity", return_value=identity) as identity_call, \
             mock.patch.object(private_netlist, "reset_results"), \
             mock.patch.object(private_netlist, "workspace_test_design_lines", return_value=[]) as design_call, \
             mock.patch.object(private_netlist, "child_environment", return_value=dict(os.environ)), \
             mock.patch("crescendo_engine.netlist_measurement.run_owned_command", side_effect=native_command), \
             mock.patch("crescendo_engine.native_diagnostics.skill_command_diagnostics", return_value={"ok": True}), \
             mock.patch.object(private_netlist, "parse_latest_rdb", return_value=parsed), \
             mock.patch.object(private_netlist, "parse_native_corner_schedule", return_value={"read_ok": True}), \
             mock.patch.object(private_netlist, "validate_capture_corners", return_value=coverage) as coverage_call:
            result = private_netlist._capture_phase(work, parameters, {}, {}, {}, config, root,
                                                     emit_clones=True, baseline_only=True)
        return result, text, identity_call, design_call, coverage_call

    def test_unmodified_expression_reaches_native_run_and_remains_legally_bound(self):
        with tempfile.TemporaryDirectory(prefix="crescendo baseline ' ") as root:
            work, parameters, config = self.fixture(root)
            before = copy.deepcopy(work["setup"])
            phase = os.path.join(root, "baseline")
            with mock.patch.object(workspace_module, "render_physical_value", side_effect=AssertionError("must not render a seed")):
                result, text, identity, designs, coverage = self.run_capture(work, parameters, config, phase)
            with open(os.path.join(phase, "patch.il")) as stream:
                patch = stream.read()
            with open(os.path.join(phase, "capture.il")) as stream:
                script = stream.read()
            self.assertNotIn("dbReplaceProp", patch)
            self.assertNotIn("cdfGetInstCDF", patch)
            self.assertNotIn("callbackExpr", patch)
            self.assertNotIn("maeSetParameter", script)
            self.assertNotIn("maeSetVar", script)
            self.assertIn("maeOpenSetup", script)
            self.assertIn("maeRunSimulation", script)
            self.assertIn("axlGetCorner", script)
            self.assertEqual(list(identity.call_args[0][2]), [])
            self.assertEqual(coverage.call_count, 1)
            self.assertEqual(designs.call_count, 1)
            self.assertEqual(result["applied_values"], {})
            self.assertFalse(result["unchanged_baseline"]["selected_values_modified"])
            self.assertFalse(result["unchanged_baseline"]["solver_completion_proven"])
            self.assertTrue(result["clone_preparation"]["ok"])
            with open(result["tasks"][0]["netlist_path"]) as stream:
                self.assertEqual(stream.read(), text)
            self.assertEqual(result["tasks"][0]["variables"], [{"name": "FN_gm", "value": "4"}])
            self.assertEqual({key: value for key, value in work["setup"].items() if key != "native_variable_scope"}, before)
            self.assertIn('"interactiveA" \'boolean nil', script)
            self.assertIn('"controlMode" "batch"', script)

    def test_clone_only_keeps_original_properties_and_config_binding_step(self):
        with tempfile.TemporaryDirectory() as root:
            work, parameters, config = self.fixture(root)
            parameters[0].update(owner_occurrence_path="/X0", owner_cell="branch",
                hierarchy_path="/X0/M0", occurrence_chain=[{
                    "hierarchy_path": "/X0", "parent_path": "/", "name": "X0",
                    "master_library": "SRC", "master_cell": "branch",
                    "master_view": "symbol", "master_view_type": "schematicSymbol"}])
            path = os.path.join(root, "clone_only.il")
            with mock.patch.object(workspace_module, "render_physical_value", side_effect=AssertionError("no seed")), \
                 mock.patch.object(workspace_module, "config_binding_lines", return_value=([], [])) as binding:
                plan = workspace_module.write_schematic_patch(work, parameters, {"p": None}, path,
                    emit_parameters=False, emit_clones=True, exit_after=False)
            self.assertEqual(len(plan["clones"]), 1)
            self.assertEqual(plan["parameters"], [])
            binding.assert_called_once()
            self.assertEqual(binding.call_args[0][1], plan["clones"])
            with open(path) as stream:
                text = stream.read()
            self.assertIn("dbCopyCellView", text)
            self.assertIn("oldParams = foreach(mapcar prop oldInst~>prop", text)
            self.assertIn("oldXY oldOrient 1 oldParams", text)
            self.assertNotIn("callbackExpr", text)
            self.assertNotIn("cdfGetInstCDF", text)

    def test_missing_point_is_not_excused_by_unmodified_baseline(self):
        with tempfile.TemporaryDirectory() as root:
            work, parameters, config = self.fixture(root)
            phase = os.path.join(root, "baseline")
            with self.assertRaisesRegex(private_netlist.PrivateNetlistUnsupported, "did not cover") as caught:
                self.run_capture(work, parameters, config, phase, coverage_ok=False, capture_point=False)
            with open(os.path.join(phase, "corner_identity.json")) as stream:
                identity = json.load(stream)
            with open(os.path.join(phase, "capture_diagnostics.json")) as stream:
                capture = json.load(stream)
            self.assertFalse(identity["native_coverage"]["ok"])
            self.assertFalse(capture["point_coverage"]["ok"])
            self.assertEqual(caught.exception.detail["corner_diagnostic_path"],
                             os.path.join(phase, "corner_identity.json"))
            self.assertFalse(caught.exception.detail["corner_coverage_ok"])

    def test_corner_scope_mismatch_remains_a_failure(self):
        with tempfile.TemporaryDirectory() as root:
            work, parameters, config = self.fixture(root)
            with self.assertRaisesRegex(private_netlist.PrivateNetlistUnsupported, "coverage differs"):
                self.run_capture(work, parameters, config, os.path.join(root, "baseline"), coverage_ok=False)

    def test_baseline_flag_cannot_bypass_candidate_or_symbol_verification(self):
        options = ({"values": {"p": 5}}, {"symbols": {"p": "CRESCENDO_NL_p"}},
                   {"seeds": {"p": 6}}, {"symbolic": True}, {"verify_numeric_application": True})
        with tempfile.TemporaryDirectory() as root:
            for index, changes in enumerate(options):
                args = {"values": {}, "symbols": {}, "seeds": {}}
                args.update(changes)
                destination = os.path.join(root, str(index))
                with self.subTest(changes=changes), self.assertRaises(private_netlist.PrivateNetlistError):
                    private_netlist._capture_phase({}, [], config={}, root=destination, baseline_only=True, **args)
                self.assertFalse(os.path.exists(destination))

    def test_public_baseline_entry_preserves_source_and_native_adapter_gates(self):
        with tempfile.TemporaryDirectory() as root:
            work, parameters, config = self.fixture(root)
            destination = os.path.join(root, "baseline")
            with mock.patch.object(private_netlist, "_qualify_setup_capture") as qualify, \
                 mock.patch.object(private_netlist, "_capture_phase", return_value={}) as capture, \
                 mock.patch.object(private_netlist, "verify_sources_unchanged", return_value={"ok": True}) as source:
                private_netlist.capture_unmodified_baseline(work, parameters, config, destination)
            qualify.assert_called_once_with(work["setup"], "spectre")
            self.assertTrue(capture.call_args[1]["baseline_only"])
            self.assertEqual(capture.call_args[0][2:5], ({}, {}, {}))
            source.assert_called_once_with(work)
            with mock.patch.object(private_netlist, "_qualify_setup_capture"), \
                 mock.patch.object(private_netlist, "_capture_phase", return_value={}), \
                 mock.patch.object(private_netlist, "verify_sources_unchanged", return_value={"ok": False}):
                with self.assertRaisesRegex(private_netlist.PrivateNetlistError, "source design changed"):
                    private_netlist.capture_unmodified_baseline(work, parameters, config, destination)


if __name__ == "__main__":
    unittest.main()
