from __future__ import print_function

import copy
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.execution_commands import normalize_execution_commands
from crescendo_engine.netlist_providers import prepare_maestro_netlists, validate_provider_request
from crescendo_engine.private_netlist import PrivateNetlistError, PrivateNetlistUnsupported, validate_manifest


class NumericMaestroProviderTest(unittest.TestCase):
    def fixtures(self, root):
        source = os.path.join(root, "maestro.sdb")
        with open(source, "w") as stream:
            stream.write("<setup><active/></setup>")
        model = os.path.join(root, "model.scs")
        with open(model, "w") as stream:
            stream.write("simulator lang=spectre\n")
        binary = os.path.join(root, "spectre")
        with open(binary, "w") as stream:
            stream.write("#!/bin/sh\nexit 0\n")
        os.chmod(binary, 0o700)
        prepared = {"setup": {"backend": "maestro", "source": {"setup_db": source},
                    "mode": "SRSC", "tests": [{"name": "tb", "simulator": "spectre"}],
                    "outputs": [{"test": "tb", "name": "voltage", "expression": 'average(v("/out"))'}]},
                    "hierarchy": {}, "source_identity": {"fingerprint": "source"}, "job_policy": {}}
        parameters = [{"parameter_id": "p", "name": "w", "backend": "schematic"}]
        config = {"cadence": {"spectre_bin": binary, "simulator": "spectre", "simulator_threads": 1},
                  "runtime": {}, "safety": {}}
        spec = {"jobs": 1, "objectives": [{"output_key": "tb::voltage"}]}
        return prepared, parameters, config, spec, model

    def capture(self, model):
        def implementation(workspace, parameters, values, symbols, seeds, config, root, **kwargs):
            self.assertEqual(symbols, {})
            self.assertEqual(seeds, {})
            self.assertFalse(kwargs["symbolic"])
            self.assertTrue(kwargs["verify_numeric_application"])
            cwd = os.path.join(root, "tasks", "p1_tb", "netlist")
            os.makedirs(cwd)
            netlist = os.path.join(cwd, "input.scs")
            with open(netlist, "w") as stream:
                stream.write('simulator lang=spectre\nparameters VDD=1\ninclude "' + model + '" section=tt\nR0 (out 0) resistor r=1000\n')
            with open(netlist, "rb") as stream:
                digest = hashlib.sha256(stream.read()).hexdigest()
            return {"tasks": [{"task_id": "p1_tb", "point_id": 1, "design_point": 1,
                "corner": "Nominal", "test": "tb", "netlist_path": netlist, "cwd": cwd,
                "psf_dir": os.path.join(os.path.dirname(cwd), "psf"),
                "spectre_log": os.path.join(os.path.dirname(cwd), "psf", "spectre.out"),
                "argv": [config["cadence"]["spectre_bin"], "input.scs", "-inter", "-plugin", "nativeplugin", "+mt=1"],
                "native_cwd": cwd, "outputs": [{"name": "voltage", "key": "tb::voltage", "expression": 'average(v("/out"))'}],
                "variables": [{"name": "VDD", "value": "1"}],
                "template_files": [{"path": netlist, "relative_path": "input.scs", "sha256": digest}]}],
                "command": {"returncode": 0, "guardian_cleanup": {"state": "dead"}},
                "applied_values": dict(values), "parameter_application": {"ok": True, "parameters": [{"parameter_id": "p"}]},
                "corner_identity": {"ok": True}}
        return implementation

    def workspace(self, setup, hierarchy, config, root, **kwargs):
        os.makedirs(root)
        return {"root": root, "primary_library": "PRIVATE", "setup_cell": "TB", "setup_view": "maestro",
                "setup": copy.deepcopy(setup), "copied_libraries": {"PRIVATE": root},
                "private_library_names": {"PRIVATE": "PRIVATE"}}

    def native_capture(self, model):
        """Native DPAR fixture shared by the two frontend mapping checks.

        Only the native process is faked. Point application, template DPAR
        replacement, model adaptation and immutable candidate checks stay real.
        """
        base = self.capture(model)
        def capture(workspace, parameters, values, symbols, seeds, config, root, **kwargs):
            from crescendo_engine.native_maestro_parameters import verify_native_parameter_application
            result = base(workspace, parameters, values, {}, {}, config, root,
                          symbolic=False, verify_numeric_application=True)
            task = result["tasks"][0]
            value = values.get("p", 2e-6)
            with open(task["netlist_path"], "w") as stream:
                stream.write('simulator lang=spectre\nparameters DPAR_1=' + str(value) + '\ninclude "' + model +
                    '" section=TT\n// Library name: PRIVATE\n// Cell name: TB\n// View name: schematic\n'
                    'R0 (out 0) resistor r=DPAR_1\n')
            with open(task["netlist_path"], "rb") as stream:
                task["template_files"][0]["sha256"] = hashlib.sha256(stream.read()).hexdigest()
            row = {"parameter_id": "p", "library": "PRIVATE", "cell": "TB", "view": "schematic",
                   "instance": "R0", "property": "r", "value": value}
            result.update(native_parameter_rows=[row] if values else [], patch_plan={"parameters": []},
                          export_snapshot={}, applied_values=dict(values))
            if values:
                result["parameter_application"] = verify_native_parameter_application([row], [task], values)
            return result
        return capture

    def test_numeric_capture_is_once_and_frozen_for_common_executor(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = self.fixtures(root)
            events = []
            with mock.patch("crescendo_engine.netlist_providers.build_workspace", side_effect=self.workspace), \
                 mock.patch("crescendo_engine.netlist_providers._capture_phase", side_effect=self.capture(model)) as capture, \
                 mock.patch("crescendo_engine.netlist_providers.verify_sources_unchanged", return_value={"ok": True, "fingerprint": "source"}):
                manifest = prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, config,
                    os.path.join(root, "simulation"), "c1", spec=spec, progress_callback=events.append)
            self.assertEqual(capture.call_count, 1)
            self.assertTrue(validate_manifest(manifest))
            self.assertEqual(manifest["parameter_symbols"], {})
            self.assertEqual(manifest["parameter_application"]["parameters"][0]["parameter_id"], "p")
            self.assertEqual(manifest["netlist_generation"]["netlist_method"], "capture_wrapper")
            self.assertEqual(manifest["netlist_generation"]["solver_invocations"], 0)
            task = manifest["tasks"][0]
            self.assertNotIn("-inter", task["argv"])
            self.assertNotIn("-plugin", task["argv"])
            self.assertEqual(task["argv"], task["solver_command_plan"]["argv"])
            self.assertEqual(os.path.dirname(task["psf_dir"]), os.path.join(manifest["root"], "tasks", task["task_id"]))
            self.assertEqual(task["execution_commands"], normalize_execution_commands(config, spec))
            self.assertFalse(os.path.exists(os.path.join(task["cwd"], "cr_candidate_parameters.scs")))
            with open(os.path.join(manifest["root"], "private_netlist_candidate.json"), encoding="utf-8") as stream:
                self.assertEqual(json.load(stream), manifest)
            self.assertEqual(events[-1]["stage"], "netlist_ready")

    def test_custom_solver_command_is_identical_layer_for_numeric_source(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = self.fixtures(root)
            spec["execution_commands"] = {"solver_templates": {"spectre": '{simulator} {netlist} +aps +mt={threads} -format psfxl -raw {psf} +log {log}'}}
            with mock.patch("crescendo_engine.netlist_providers.build_workspace", side_effect=self.workspace), \
                 mock.patch("crescendo_engine.netlist_providers._capture_phase", side_effect=self.capture(model)), \
                 mock.patch("crescendo_engine.netlist_providers.verify_sources_unchanged", return_value={"ok": True}):
                manifest = prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, config,
                    os.path.join(root, "simulation"), "c1", spec=spec)
            self.assertIn("+aps", manifest["tasks"][0]["argv"])
            self.assertEqual(manifest["tasks"][0]["solver_command_plan"]["template"], spec["execution_commands"]["solver_templates"]["spectre"])

    def test_template_candidates_freeze_the_same_solver_plan(self):
        from crescendo_engine.private_netlist import materialize_candidate, _manifest_digest
        from crescendo_engine.simulators import execution_contract
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = self.fixtures(root)
            spec["execution_commands"] = {"solver_templates": {"spectre": '{simulator} {netlist} +aps +mt={threads} -format psfxl -raw {psf} +log {log}'}}
            captured = self.capture(model)({}, parameters, {"p": 1}, {}, {}, config,
                os.path.join(root, "template"), symbolic=False, verify_numeric_application=True)
            task = captured["tasks"][0]
            with open(task["netlist_path"], "a") as stream:
                stream.write('include "cr_candidate_parameters.scs"\nR1 (out 0) resistor r=CRESCENDO_NL_p\n')
            with open(task["netlist_path"], "rb") as stream:
                task["netlist_sha256"] = hashlib.sha256(stream.read()).hexdigest()
            task["template_files"][0]["sha256"] = task["netlist_sha256"]
            task["artifact_sha256"] = {task["netlist_path"]: task["netlist_sha256"]}
            task["simulator_contract"] = execution_contract(config)
            commands = normalize_execution_commands(config, spec)
            task["execution_commands"] = commands
            template = {"schema_version": "crescendo.private_netlist.v1", "root": os.path.join(root, "template"),
                        "parameter_symbols": {"p": "CRESCENDO_NL_p"}, "tasks": [task],
                        "qualification": {"ok": True}, "execution_commands": commands}
            template["manifest_sha256"] = _manifest_digest(template)
            candidate = materialize_candidate(template, {"p": 2000}, os.path.join(root, "candidate"), "c1")
            self.assertTrue(validate_manifest(candidate))
            self.assertEqual(candidate["tasks"][0]["argv"], candidate["tasks"][0]["solver_command_plan"]["argv"])
            self.assertEqual(candidate["tasks"][0]["execution_commands"], commands)
            self.assertEqual(candidate["netlist_generation"]["source_mode"], "private_netlist")
            self.assertIn("+aps", candidate["tasks"][0]["argv"])

    def test_source_change_stops_before_any_candidate_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = self.fixtures(root)
            destination = os.path.join(root, "simulation")
            with mock.patch("crescendo_engine.netlist_providers.build_workspace", side_effect=self.workspace), \
                 mock.patch("crescendo_engine.netlist_providers._capture_phase", side_effect=self.capture(model)), \
                 mock.patch("crescendo_engine.netlist_providers.verify_sources_unchanged", return_value={"ok": False}):
                with self.assertRaisesRegex(PrivateNetlistError, "source design changed"):
                    prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, config, destination, "c1", spec=spec)
            self.assertFalse(os.path.exists(os.path.join(destination, "private_netlist_candidate.json")))

    def test_unsupported_output_is_rejected_before_workspace_creation(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, unused = self.fixtures(root)
            prepared["setup"]["outputs"][0]["expression"] = 'calcVal("voltage" "other_tb")'
            destination = os.path.join(root, "simulation")
            with mock.patch("crescendo_engine.netlist_providers.build_workspace") as workspace:
                with self.assertRaisesRegex(PrivateNetlistUnsupported, "cannot reproduce"):
                    prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, config, destination, "c1", spec=spec)
                workspace.assert_not_called()
            self.assertFalse(os.path.exists(destination))

    def test_unsupported_source_modes_fail_read_only(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, unused = self.fixtures(root)
            for mutation in ("backend", "mode", "design_variable", "mixed_simulators"):
                local, selected = copy.deepcopy(prepared), copy.deepcopy(parameters)
                if mutation == "backend": local["setup"]["backend"] = "adexl"
                elif mutation == "mode": local["setup"]["mode"] = "Monte Carlo Sampling"
                elif mutation == "design_variable": selected[0]["backend"] = "design_variable"
                else: local["setup"]["tests"].append({"name": "alps_tb", "simulator": "alps"})
                with self.assertRaises(PrivateNetlistUnsupported):
                    validate_provider_request(local, selected, config, spec)

    def test_missing_selected_solver_fails_before_native_capture(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, unused = self.fixtures(root)
            config["cadence"]["spectre_bin"] = None
            with mock.patch("crescendo_engine.netlist_providers.build_workspace") as workspace:
                with self.assertRaisesRegex(ValueError, "SPECTRE executable is missing"):
                    prepare_maestro_netlists(prepared, parameters, {"p": 1}, config,
                                             os.path.join(root, "simulation"), "c1", spec=spec)
                workspace.assert_not_called()

    def test_generation_only_keeps_native_proof_without_solver_or_output_qualification(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = self.fixtures(root)
            prepared["setup"]["outputs"][0]["expression"] = 'calcVal("voltage" "other_tb")'
            capture_config = copy.deepcopy(config)
            config["cadence"]["spectre_bin"] = None
            def native_capture(*args, **kwargs):
                args = list(args)
                args[5] = capture_config
                return self.capture(model)(*args, **kwargs)
            with mock.patch("crescendo_engine.netlist_providers.build_workspace", side_effect=self.workspace), \
                 mock.patch("crescendo_engine.netlist_providers._capture_phase", side_effect=native_capture), \
                 mock.patch("crescendo_engine.netlist_providers._freeze_task", side_effect=AssertionError("must not build solver")), \
                 mock.patch("crescendo_engine.netlist_providers.verify_sources_unchanged", return_value={"ok": True}):
                result = prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, config,
                    os.path.join(root, "range"), "range_min", spec=spec, generation_only=True)
            self.assertEqual(result["schema_version"], "crescendo.native_parameter_capture.v1")
            self.assertEqual(result["parameter_application"]["parameters"][0]["parameter_id"], "p")
            self.assertTrue(os.path.isfile(result["tasks"][0]["netlist_path"]))
            self.assertFalse(result["netlist_generation"]["solver_invocations"])
            self.assertTrue(os.path.isfile(os.path.join(result["root"], "native_parameter_capture.json")))

    def test_saved_alps_native_addon_is_blocked_before_copy_or_launch(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, unused = self.fixtures(root)
            prepared["setup"]["tests"][0]["simulator"] = "alps"
            with mock.patch("crescendo_engine.netlist_providers.build_workspace") as workspace:
                with self.assertRaises(PrivateNetlistUnsupported) as caught:
                    prepare_maestro_netlists(prepared, parameters, {"p": 1}, config,
                                             os.path.join(root, "simulation"), "c1", spec=spec)
                workspace.assert_not_called()
            self.assertEqual(caught.exception.detail["failure_stage"], "native_netlist_capture_adapter")
            self.assertFalse(os.path.exists(os.path.join(root, "simulation")))

    def test_existing_root_and_nonfinite_values_do_not_start_native_capture(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, unused = self.fixtures(root)
            with mock.patch("crescendo_engine.netlist_providers._capture_phase") as capture:
                for destination, values in ((root, {"p": 1.0}), (os.path.join(root, "new"), {"p": float("nan")})):
                    with self.assertRaises(PrivateNetlistError):
                        prepare_maestro_netlists(prepared, parameters, values, config, destination, "c1", spec=spec)
                capture.assert_not_called()


if __name__ == "__main__":
    unittest.main()
