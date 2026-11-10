"""Owned CLI/PSF fixtures only; no native ALPS/Cadence acceptance claim."""
import copy
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from crescendo_engine import private_netlist, simulators
from crescendo_engine import netlist_measurement as measurement
from crescendo_engine.maestro_batch import snapshot_test_lines
from crescendo_engine.private_netlist import (
    _manifest_digest, _qualify_setup_capture, materialize_candidate,
)
from tests import test_private_netlist


class SimulatorExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo simulator ' with spaces ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.models = os.path.join(self.root, "models", "alps")
        os.makedirs(self.models)
        self.exe = os.path.join(self.root, "fixture alps")
        with open(self.exe, "w") as stream:
            stream.write("#!/bin/sh\nexit 0\n")
        os.chmod(self.exe, 0o700)
        self.source = os.path.join(self.root, "source.scs")
        self.target = os.path.join(self.models, "source.scs")
        with open(self.source, "w") as stream:
            stream.write("section TT\nparameters source=1\nendsection\n")
        with open(self.target, "w") as stream:
            stream.write("section TT_ALPS\nparameters source=2\nendsection\n")
        self.config = {"cadence": {"simulator": "alps", "alps_bin": self.exe,
            "alps_model_dir": self.models, "simulator_threads": None,
            "alps_model_file_map": {self.source: {"path": self.target, "section": "TT_ALPS"}}}}

    def contract(self, **kwargs):
        return simulators.execution_contract(self.config, "spectre",
            [{"file": self.source, "section": "TT"}], **kwargs)

    def test_three_qualified_source_execution_pairs_still_create_contracts(self):
        pairs = [
            ("spectre", "spectre", {"cadence": {"simulator": "spectre", "spectre_bin": self.exe}},
             [{"file": self.source, "section": "TT"}]),
            ("spectre", "alps", self.config, [{"file": self.source, "section": "TT"}]),
            ("alps", "alps", self.config, [{"file": self.target, "section": "TT_ALPS"}]),
        ]
        for source, selected, config, models in pairs:
            with self.subTest(source=source, selected=selected):
                contract = simulators.execution_contract(config, source, models)
                self.assertEqual(contract["capture_source_simulator"], source)
                self.assertEqual(contract["execution_simulator"], selected)
                self.assertTrue(simulators.validate_execution_contract(contract))
                self.assertEqual(contract["models"][0]["target"]["path"],
                                 self.target if selected == "alps" else self.source)

    def test_reverse_bridge_rejects_creation_and_rehashed_frozen_contract(self):
        config = {"cadence": {"simulator": "spectre", "spectre_bin": self.exe,
                               "spectre_model_dir": self.models}}
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "unqualified"):
            simulators.execution_contract(config, "alps")
        frozen = self.contract()
        frozen["capture_source_simulator"] = "alps"
        frozen["execution_simulator"] = "spectre"
        frozen["fingerprint"] = simulators._fingerprint(frozen)
        # Rehashing excludes identity drift as the rejection reason: an old or
        # restored contract must still meet the supported adapter boundary.
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "unqualified"):
            simulators.validate_execution_contract(frozen)
        captured = [self.exe, "input.scs", "-format", "ps", "-o", "old-psf",
                    "-I", self.models, "-mt", "6"]
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "unqualified"):
            simulators.build_simulator_argv(frozen, captured, "candidate.scs",
                                            "candidate-psf", "spectre.out", "ahdl")

    def test_reverse_prepare_rejects_before_workspace_or_native_capture(self):
        setup_path = os.path.join(self.root, "reverse-source-maestro.sdb")
        with open(setup_path, "w") as stream:
            stream.write("<maestro/>")
        prepared = {"setup": {"source": {"setup_db": setup_path},
                              "tests": [{"name": "test", "enabled": True, "simulator": "alps"}]},
                    "hierarchy": {}, "source_identity": {}}
        parameters = [{"parameter_id": "p", "backend": "schematic", "numeric_value": 1.0}]
        with mock.patch.object(private_netlist, "build_workspace",
                               side_effect=AssertionError("Unexpected native workspace")) as workspace, \
                mock.patch.object(private_netlist, "_capture_phase",
                                  side_effect=AssertionError("Unexpected native capture")) as capture:
            with self.assertRaisesRegex(private_netlist.PrivateNetlistUnsupported, "unqualified"):
                private_netlist.prepare_private_netlist(prepared, parameters, self.config,
                    os.path.join(self.root, "reverse-prepare"), spec={"simulator": "spectre"})
            workspace.assert_not_called()
            capture.assert_not_called()

    def test_selection_is_explicit_and_spec_wins_without_mutating_config(self):
        before = copy.deepcopy(self.config)
        self.assertEqual(simulators.selected_simulator(), "spectre")
        self.assertEqual(simulators.selected_simulator(self.config), "alps")
        self.assertEqual(simulators.selected_simulator(self.config, {"simulator": "spectre"}), "spectre")
        self.assertEqual(self.config, before)
        for value in ("auto", "alps;other", "", None):
            if value is None:
                continue
            with self.assertRaises(simulators.SimulatorConfigurationError):
                simulators.selected_simulator({}, {"simulator": value})

    def test_missing_alps_never_executes_the_source_spectre_binary(self):
        del self.config["cadence"]["alps_bin"]
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "ALPS executable is missing"):
            self.contract(default_executable="/bin/true")
        self.assertFalse(simulators.simulator_readiness(self.config)["ready"])

    def test_spectre_tool_readiness_does_not_claim_missing_model_directory_is_available(self):
        config = {"cadence": {"simulator": "spectre", "spectre_bin": self.exe}}
        before = copy.deepcopy(config)
        readiness = simulators.simulator_readiness(config)
        self.assertTrue(readiness["ready"])
        self.assertTrue(readiness["executable_ready"])
        self.assertFalse(readiness["model_directory_discovered"])
        self.assertFalse(readiness["exact_point_models_qualified"])
        self.assertIn("model search directory is not detected", readiness["message"])
        self.assertNotIn("directory available", readiness["message"])
        self.assertEqual(config, before)

    def test_alps_missing_model_root_still_blocks_instead_of_tool_only_readiness(self):
        config = {"cadence": {"simulator": "alps", "alps_bin": self.exe}}
        readiness = simulators.simulator_readiness(config)
        self.assertFalse(readiness["ready"])
        self.assertIn("model directory is missing", readiness["reason"])

    def test_model_directory_tool_check_does_not_claim_exact_point_qualification(self):
        readiness = simulators.simulator_readiness(self.config)
        self.assertTrue(readiness["ready"])
        self.assertTrue(readiness["model_directory_discovered"])
        self.assertFalse(readiness["exact_point_models_qualified"])
        self.assertIn("exact native file/section points", readiness["message"])

    def test_model_bridge_uses_real_different_bytes_and_explicit_section(self):
        contract = self.contract()
        self.assertNotEqual(contract["models"][0]["source"]["sha256"], contract["models"][0]["target"]["sha256"])
        self.assertFalse(contract["model_equivalence_claimed"])
        original = 'simulator lang=spectre\ninclude "%s" section=TT\n' % self.source
        rewritten = simulators.rewrite_model_references(original, contract)
        self.assertIn('include "%s" section=TT_ALPS' % self.target, rewritten)
        with open(self.source) as stream:
            self.assertIn("source=1", stream.read())

    def test_same_basename_is_not_an_authorized_model_mapping(self):
        self.config["cadence"]["alps_model_file_map"] = {}
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "mapping is missing"):
            self.contract()

    def test_model_mapping_cannot_escape_the_selected_alps_directory(self):
        self.config["cadence"]["alps_model_file_map"] = {self.source: self.source}
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "outside"):
            self.contract()

    def test_unmatched_model_section_is_not_silently_rewritten(self):
        contract = self.contract()
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "absent"):
            simulators.rewrite_model_references('include "%s" section=FF\n' % self.source, contract)

    def test_frozen_model_and_executor_drift_are_rejected(self):
        contract = self.contract()
        with open(self.target, "a") as stream:
            stream.write("changed\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "content changed"):
            simulators.validate_execution_contract(contract)
        contract = self.contract()
        with open(self.exe, "a") as stream:
            stream.write("# changed\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "content changed"):
            simulators.validate_execution_contract(contract)

    def test_alps_argv_has_guide_flags_only_and_resource_cpu_wins_auto(self):
        captured = ["spectre", "input.scs", "+aps", "+mt=8", "-format", "psfxl", "-raw", "old", "-ahdllibdir", "cache"]
        contract = self.contract(template_argv=captured, resource_cpu=6)
        argv = simulators.build_simulator_argv(contract, captured, "/new/input.scs", "/new/psf", "/new/alps.out", "/new/ahdl")
        self.assertEqual(argv, [self.exe, "/new/input.scs", "-format", "ps", "-o", "/new/psf", "-I", self.models, "-mt", "6"])
        self.assertEqual(contract["threads_source"], "selected_submission_cpu")
        self.assertEqual(contract["template_threads"], 8)

    def test_explicit_threads_must_match_known_scheduler_cpu(self):
        self.config["cadence"]["simulator_threads"] = 8
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "differ"):
            self.contract(resource_cpu=6)
        self.assertEqual(self.contract(resource_cpu=8)["threads"], 8)

    def test_submission_cpu_is_exact_and_conflicting_resources_are_rejected(self):
        self.assertEqual(simulators.submission_cpu('/cli/batch-submit -A account -R "cpu=6;mem=8000" -x all'), 6)
        self.assertEqual(simulators.submission_cpu(['batch-submit', '-Rcpu=8;mem=8000']), 8)
        self.assertIsNone(simulators.submission_cpu('bash wrapper.sh -R "cpu=6;mem=8000"'))
        self.assertIsNone(simulators.submission_cpu('batch-submit -R "mem=8000"'))
        self.assertIsNone(simulators.submission_cpu('batch-submit -R "cpu=6" ; other'))
        for command in ('batch-submit -R "cpu=6;cpu=8"', 'batch-submit -R "cpu=6.5"', 'batch-submit -R "cpu=0"'):
            with self.assertRaises(simulators.SimulatorConfigurationError):
                simulators.submission_cpu(command)

    def test_auto_can_use_qualified_template_threads(self):
        contract = self.contract(template_argv=["spectre", "input.scs", "+mt=8"])
        self.assertEqual((contract["threads"], contract["threads_source"]), (8, "qualified_template"))

    def test_spectre_retains_its_qualified_flags_and_removes_native_ipc(self):
        config = {"cadence": {"spectre_bin": self.exe, "simulator_threads": None}}
        captured = [self.exe, "old.scs", "+escchars", "+mt=8", "-inter", "-plugin", "ipc", "-format", "psfxl", "-raw", "old"]
        contract = simulators.execution_contract(config, template_argv=captured)
        argv = simulators.build_simulator_argv(contract, captured, "/new/input.scs", "/new/psf", "/new/log", "/new/ahdl")
        self.assertIn("+escchars", argv)
        self.assertIn("+mt=8", argv)
        self.assertIn("psfxl", argv)
        self.assertNotIn("ipc", argv)
        self.assertNotIn("old", argv)

    def test_spectre_adds_its_selected_model_dir_without_duplicate_I(self):
        config = {"cadence": {"spectre_bin": self.exe, "spectre_model_dir": self.models}}
        captured = [self.exe, "input.scs", "+aps", "+mt=8", "-format", "psfxl"]
        contract = simulators.execution_contract(config, template_argv=captured)
        for include in ([], ["-I", self.models], ["-I" + self.models]):
            with self.subTest(include=include):
                argv = simulators.build_simulator_argv(contract, captured + include, "candidate.scs", "psf", "solver.out", "ahdl")
                directories = [argv[index + 1] if value == "-I" else value[2:]
                               for index, value in enumerate(argv) if value.startswith("-I")]
                self.assertEqual(directories, [self.models])
                self.assertNotIn("-I", argv)

    def test_spectre_joined_include_preserves_order_even_without_an_auto_directory(self):
        captured = [self.exe, "input.scs", "-I", "/first model dir", "-I/second", "-I", "/third model dir"]
        config = {"cadence": {"spectre_bin": self.exe}}
        contract = simulators.execution_contract(config)
        argv = simulators.build_simulator_argv(contract, captured, "/new/input.scs", "/new/psf", "/new/log", "/new/ahdl")
        self.assertEqual([value for value in argv if value.startswith("-I")], ["-I/first model dir", "-I/second", "-I/third model dir"])
        self.assertNotIn("-I", argv)

    def test_spectre_missing_include_argument_is_not_launched(self):
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "no model include directory"):
            simulators.ensure_model_include([self.exe, "input.scs", "-I"], None)

    def test_real_owned_cli_fixture_preserves_separate_I_and_spaced_paths(self):
        capture = os.path.join(self.root, "argv.json")
        with open(self.exe, "w") as stream:
            import sys
            stream.write("#!" + sys.executable + "\nimport json,sys\njson.dump(sys.argv[1:],open(" + repr(capture) + ", 'w'))\n")
        contract = self.contract(resource_cpu=6)
        argv = simulators.build_simulator_argv(contract, [], "/new/input.scs", "/new/psf", "/new/log", "/new/ahdl")
        subprocess.run(argv, cwd=self.root, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(capture) as stream:
            self.assertEqual(json.load(stream), argv[1:])

    def test_materialized_alps_candidate_retains_source_and_execution_identity(self):
        manifest = test_private_netlist.PrivateNetlistCandidateTest().template(self.root)
        manifest["tasks"][0]["simulator_contract"] = self.contract()
        template = manifest["tasks"][0]
        with open(template["netlist_path"], "a") as stream:
            stream.write('include "%s" section=TT_ALPS\n' % self.target)
        template["netlist_sha256"] = test_private_netlist.digest(template["netlist_path"])
        for row in template["template_files"]:
            if row["path"] == template["netlist_path"]:
                row["sha256"] = template["netlist_sha256"]
        template["artifact_sha256"][template["netlist_path"]] = template["netlist_sha256"]
        manifest["manifest_sha256"] = _manifest_digest(manifest)
        candidate = materialize_candidate(manifest, {"p": 1000}, os.path.join(self.root, "candidate"), "one")
        task = candidate["tasks"][0]
        self.assertEqual(task["execution_simulator"], "alps")
        self.assertEqual(task["capture_source_simulator"], "spectre")
        self.assertEqual(task["argv"][0], self.exe)
        self.assertNotIn("-raw", task["argv"])
        self.assertEqual(task["simulator_contract"]["models"][0]["target"]["sha256"], self.contract()["models"][0]["target"]["sha256"])
        self.assertTrue(task["simulator_log"].endswith("alps.out"))
        with open(task["netlist_path"]) as stream:
            self.assertIn('include "%s" section=TT_ALPS' % self.target, stream.read())

    def test_saved_alps_capture_is_matching_and_not_an_arbitrary_plugin_switch(self):
        setup_file = os.path.join(self.root, "maestro.sdb")
        with open(setup_file, "w") as stream:
            stream.write("<maestro/>")
        setup = {"tests": [{"simulator": "alps"}], "source": {"setup_db": setup_file}}
        with self.assertRaisesRegex(Exception, "native addon may bypass") as caught:
            _qualify_setup_capture(setup, "alps")
        self.assertEqual(caught.exception.detail["failure_stage"], "native_netlist_capture_adapter")
        self.assertTrue(caught.exception.detail["cleanup_proven"])
        with self.assertRaisesRegex(Exception, "matching saved"):
            _qualify_setup_capture(setup, "spectre")

    def test_batch_keeps_the_saved_alps_simulator_and_readback_guards(self):
        manifest = {"candidates": [{"verification_workspace": {"root": self.root}}],
                    "original_enabled_tests": ["source"],
                    "batch_tests": [{"name": "alias", "source": "source", "design": ["lib", "cell", "config"], "simulator": "alps"}]}
        lines = "\n".join(snapshot_test_lines(manifest))
        self.assertIn('?simulator "alps"', lines)
        self.assertIn('assoc("sim" sourceToolArgs)', lines)
        self.assertIn("Crescendo batch simulator changed", lines)

    def measurement_task(self):
        return {"task_id": "one", "test": "test", "corner": "TT", "point_id": 1,
                "psf_dir": os.path.join(self.root, "psf"), "simulator_contract": self.contract(),
                "variables": [{"name": "CRESCENDO_NL_p", "value": 1000.0}],
                "outputs": [{"key": "test::gain", "name": "gain", "expression": 'value(VF("/out") 1)'}]}

    def test_alps_ocean_reader_is_guarded_and_reads_actual_selected_parameters(self):
        task = self.measurement_task()
        script, outputs = measurement.render_measurement_script(task, "/records", "token")
        self.assertIn("errset(simulator('spectre) t)", script)
        self.assertIn('pv("top-level" "CRESCENDO_NL_p"', script)
        self.assertIn("candidate PSF cannot open", script)
        self.assertEqual(measurement.required_variables(task), ["CRESCENDO_NL_p"])

    def test_native_addon_startup_is_explicit_and_guarded(self):
        addon = os.path.join(self.root, "explicit addon.il")
        with open(addon, "w") as stream:
            stream.write("; fixture, never executed\n")
        lines = measurement._startup_lines({"cadence": {"startup_extensions": [addon, addon]}})
        self.assertEqual(len(lines), 1)
        self.assertIn("errset(load(", lines[0])
        self.assertIn(addon, lines[0])
        with self.assertRaises(measurement.NetlistMeasurementError):
            measurement._startup_lines({"cadence": {"startup_extensions": ["missing.il"]}})

    def test_authentic_parameter_record_shape_is_required_before_metrics(self):
        task = self.measurement_task()
        path = os.path.join(self.root, "fixture_records.tsv")
        with open(path, "w") as stream:
            stream.write(measurement.PROTOCOL + "\ttoken\nP\t0\t1000\nV\t0\t4.2\nEND\t1\n")
        actual = {}
        rows = measurement.parse_measurement_records(path, "token", task["outputs"], task,
                                                     measurement.required_variables(task), actual)
        self.assertEqual(rows[0]["value"], 4.2)
        self.assertTrue(measurement.verify_actual_parameters(task, actual)["ok"])
        for values in ({}, {"CRESCENDO_NL_p": 900.0}, {"CRESCENDO_NL_p": float("nan")}):
            with self.assertRaises(measurement.NetlistMeasurementError):
                measurement.verify_actual_parameters(task, values)
        with open(path, "w") as stream:
            stream.write(measurement.PROTOCOL + "\ttoken\nV\t0\t4.2\nEND\t1\n")
        with self.assertRaisesRegex(measurement.NetlistMeasurementError, "VAR data"):
            measurement.parse_measurement_records(path, "token", task["outputs"], task,
                                                  measurement.required_variables(task))


if __name__ == "__main__":
    unittest.main()
