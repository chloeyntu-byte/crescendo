"""Independent integration boundaries; all executors/results are owned fixtures."""
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import cadence, completion, config, environment_identity, workspace
from crescendo_engine.simulators import simulator_configuration_identity
from crescendo_engine.server import OptimizerService
from tests import test_completion, test_gui_optional_settings


class AlpsIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo alps integration ' ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.bin = os.path.join(self.root, "bin")
        os.makedirs(self.bin)
        for name in ("virtuoso", "ocean", "spectre", "alps"):
            path = os.path.join(self.bin, name)
            with open(path, "w") as stream:
                stream.write("#!/bin/sh\n# " + name + " fixture never executes EDA\nexit 0\n")
            os.chmod(path, 0o700)
        self.lib = os.path.join(self.root, "cds.lib")
        with open(self.lib, "w") as stream:
            stream.write("; fixture only\n")
        self.discovery = {"simulator": "alps", "cadence_root": self.root,
            "spectre_root": self.root, "virtuoso_bin": os.path.join(self.bin, "virtuoso"),
            "ocean_bin": os.path.join(self.bin, "ocean"), "spectre_bin": os.path.join(self.bin, "spectre"),
            "cds_lib": self.lib, "license_file": "1@fixture-only", "search_roots": [],
            "alps_search_roots": [os.path.join(self.root, "nonexistent")]}

    def discover(self, **overlay):
        return cadence.discover_eda_environment(dict(self.discovery, **overlay),
            validate=False, environ={"PATH": self.bin})

    def test_binary_discovery_tracks_both_simulators_independently(self):
        result = self.discover()
        self.assertEqual(result["effective"]["alps_bin"], os.path.join(self.bin, "alps"))
        self.assertEqual(result["effective"]["spectre_bin"], os.path.join(self.bin, "spectre"))
        self.assertEqual(result["effective"]["simulator"], "alps")

    def test_explicit_invalid_alps_binary_never_falls_back_to_path_or_root(self):
        missing = os.path.join(self.root, "missing-alps")
        result = self.discover(alps_bin=missing, alps_root=self.root)
        self.assertIsNone(result["effective"]["alps_bin"])
        self.assertIn("alps_bin", result["missing"])

    def test_explicit_invalid_alps_root_never_uses_a_different_path_install(self):
        result = self.discover(alps_root=os.path.join(self.root, "missing-root"))
        self.assertIsNone(result["effective"]["alps_bin"])
        self.assertIn("alps_root", result["missing"])

    def test_alps_version_probe_does_not_reuse_spectre_W(self):
        with mock.patch.object(cadence, "_version_probe", return_value={"ok": True}) as probe:
            cadence.discover_eda_environment(self.discovery, validate=True, environ={"PATH": self.bin})
        self.assertEqual([call[0][0] for call in probe.call_args_list], ["virtuoso_bin", "ocean_bin"])

    def test_relative_alps_overrides_and_mapping_are_resolved_once(self):
        values = {"alps_bin": "bin/alps", "alps_root": ".", "alps_model_dir": "kit/alps",
                  "alps_model_file_map": {"kit/spectre/device.scs": {"path": "kit/alps/device.scs", "section": "TT_ALPS"}}}
        resolved = config._resolve_file_overrides(values, self.root)
        self.assertEqual(resolved["alps_bin"], os.path.join(self.bin, "alps"))
        self.assertEqual(resolved["alps_model_file_map"][os.path.join(self.root, "kit/spectre/device.scs")]["path"], os.path.join(self.root, "kit/alps/device.scs"))
        self.assertEqual(values["alps_bin"], "bin/alps")

    def test_run_freeze_is_independent_of_mutable_form_and_prefers_spec(self):
        original = copy.deepcopy(config.DEFAULTS)
        original["cadence"].update(simulator="spectre", alps_bin=os.path.join(self.bin, "alps"))
        original["runtime"].update(run_root=self.root, run_root_origin="explicit")
        service = object.__new__(OptimizerService)
        service.config = original
        frozen = service._freeze_run_config({"evaluator": "mock", "simulator": "alps"}, None)
        self.assertEqual(frozen["cadence"]["simulator"], "alps")
        self.assertEqual(original["cadence"]["simulator"], "spectre")
        original["cadence"]["alps_bin"] = "changed-after-freeze"
        self.assertEqual(frozen["cadence"]["alps_bin"], os.path.join(self.bin, "alps"))

    def test_checkpoint_freeze_uses_the_saved_simulator_over_new_form(self):
        service = object.__new__(OptimizerService)
        service.config = copy.deepcopy(config.DEFAULTS)
        service.config["runtime"]["run_root"] = self.root
        service._run_locations = {}
        frozen = service._freeze_run_config({"simulator": "spectre", "evaluator": "mock"}, None,
            checkpoint={"spec": {"simulator": "alps", "evaluator": "mock"}, "manifest": {}})
        self.assertEqual(frozen["cadence"]["simulator"], "alps")
        self.assertEqual(service.config["cadence"]["simulator"], "spectre")

    def test_native_wrapper_uses_alps_name_bin_and_source_identity(self):
        native_root = os.path.join(self.root, "native-workspace")
        os.makedirs(native_root)
        own = {"root": native_root, "primary_library": "D", "setup_cell": "TB",
               "setup": {"tests": [{"name": "native-test", "simulator": "alps", "enabled": True}]}}
        capture = os.path.join(self.root, "wrapper-argv.json")
        submit = os.path.join(self.root, "owned-submit")
        with open(submit, "w") as stream:
            stream.write("#!" + sys.executable + "\nimport json,sys\njson.dump(sys.argv[1:],open(" + repr(capture) + ", 'w'))\n")
        os.chmod(submit, 0o700)
        selected = {"cadence": {"simulator": "alps", "alps_bin": os.path.join(self.bin, "alps")},
                    "runtime": {"spectre_submit_script": submit}}
        result = workspace.write_batch_files(own, selected, history_name="fixture")
        self.assertEqual(result["simulator_wrapper"], os.path.join(native_root, "bin", "alps"))
        self.assertEqual(result["execution_simulator"], "alps")
        environment = dict(os.environ)
        for key in ("CRESCENDO_REAL_SIMULATOR_BIN", "CRESCENDO_REAL_SPECTRE_BIN", "CRESCENDO_SPECTRE_SUBMIT_SCRIPT"):
            environment.pop(key, None)
        subprocess.run(["/bin/sh", result["simulator_wrapper"], "input.scs", "-format", "ps"],
                       cwd=native_root, env=environment, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(capture) as stream:
            self.assertEqual(json.load(stream), [os.path.join(self.bin, "alps"), "input.scs", "-format", "ps"])
        own["setup"]["tests"][0]["simulator"] = "spectre"
        with self.assertRaisesRegex(ValueError, "saved simulator integration differs"):
            workspace.write_batch_files(own, selected, history_name="fixture2")

    def test_native_alps_external_identity_does_not_bind_unused_spectre(self):
        own = dict(self.discovery, alps_bin=os.path.join(self.bin, "alps"))
        setup = {"tests": [{"enabled": True, "simulator": "alps"}]}
        collected = environment_identity.collect_external_eda_environment(setup, own, {})
        identity, diagnostics = collected["identity"], collected["diagnostics"]
        artifacts = identity["artifacts"]
        primary = next(row for row in artifacts if row["role"] == "simulator_executable")
        # Different fixture contents make the selected primary unambiguous.
        import hashlib
        with open(own["alps_bin"], "rb") as stream:
            expected = hashlib.sha256(stream.read()).hexdigest()
        self.assertEqual(primary["sha256"], expected)
        paths = [row.get("path") for row in diagnostics.get("resolved_paths") or []]
        self.assertNotIn(own["spectre_bin"], paths)

    def test_spectre_capture_alps_bridge_keeps_both_distinct_executable_roles(self):
        own = dict(self.discovery, alps_bin=os.path.join(self.bin, "alps"))
        setup = {"tests": [{"enabled": True, "simulator": "spectre"}]}
        identity = environment_identity.collect_external_eda_environment(setup, own, {})["identity"]
        roles = [row["role"] for row in identity["artifacts"]]
        self.assertIn("simulator_executable", roles)
        self.assertIn("alps_simulator_executable", roles)

    def configuration_identity_fixture(self):
        source = os.path.join(self.root, "source-model.scs")
        directory = os.path.join(self.root, "models", "alps")
        os.makedirs(directory)
        target = os.path.join(directory, "execution-model.scs")
        with open(source, "w") as stream:
            stream.write("source model fixture\n")
        with open(target, "w") as stream:
            stream.write("ALPS model fixture\n")
        return {"cadence": {"simulator": "alps", "alps_bin": os.path.join(self.bin, "alps"),
            "alps_model_dir": directory, "simulator_threads": None, "simulator_result_reader": "spectre",
            "alps_model_file_map": {source: {"path": target, "section": "TT"}}}}

    def test_resume_identity_changes_for_model_section_executable_and_reader(self):
        own = self.configuration_identity_fixture()
        original = simulator_configuration_identity(own)
        changed = copy.deepcopy(own)
        next(iter(changed["cadence"]["alps_model_file_map"].values()))["section"] = "FF"
        self.assertNotEqual(original, simulator_configuration_identity(changed))
        changed = copy.deepcopy(own)
        changed["cadence"]["simulator_result_reader"] = "alps"
        self.assertNotEqual(original, simulator_configuration_identity(changed))
        changed = copy.deepcopy(own)
        changed["cadence"]["simulator_threads"] = 8
        self.assertNotEqual(original, simulator_configuration_identity(changed))
        target = next(iter(own["cadence"]["alps_model_file_map"].values()))["path"]
        with open(target, "rb") as stream:
            original_bytes = stream.read()
        with open(target, "ab") as stream:
            stream.write(b"model changed\n")
        self.assertNotEqual(original, simulator_configuration_identity(own))
        with open(target, "wb") as stream:
            stream.write(original_bytes)
        self.assertEqual(original, simulator_configuration_identity(own))
        with open(own["cadence"]["alps_bin"], "a") as stream:
            stream.write("# executor content changed\n")
        self.assertNotEqual(original, simulator_configuration_identity(own))

    def test_resume_identity_allows_relocation_of_identical_executor_and_models(self):
        own = self.configuration_identity_fixture()
        original = simulator_configuration_identity(own)
        moved = os.path.join(self.root, "relocated")
        os.makedirs(moved)
        changed = copy.deepcopy(own)
        executor = os.path.join(moved, "alps")
        shutil.copy2(own["cadence"]["alps_bin"], executor)
        changed["cadence"]["alps_bin"] = executor
        mappings = {}
        for source, target in own["cadence"]["alps_model_file_map"].items():
            source_copy = os.path.join(moved, os.path.basename(source))
            target_copy = os.path.join(moved, os.path.basename(target["path"]))
            shutil.copy2(source, source_copy)
            shutil.copy2(target["path"], target_copy)
            mappings[source_copy] = {"path": target_copy, "section": target["section"]}
        changed["cadence"]["alps_model_file_map"] = mappings
        changed["cadence"]["alps_model_dir"] = moved
        self.assertEqual(original, simulator_configuration_identity(changed))


class AlpsCompletionBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = test_completion.CompletionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.setup["tests"][0].update(simulator="alps", variables=[{"name": "VDD", "value": "1.8"}])
        self.cfg = {"cadence": {"simulator": "alps", "simulator_result_reader": "spectre"}}
        self.fixture.command.update(execution_simulator="alps", spectre_timing={"runs": []})
        for point in (1, 2):
            base = os.path.join(self.fixture.simulation_root, "D", "TB", "maestro", "results", "maestro", self.fixture.history, str(point), "ac_test")
            netlist = os.path.join(base, "netlist")
            os.makedirs(netlist)
            with open(os.path.join(netlist, "input.scs"), "w") as stream:
                stream.write("simulator lang=spectre\nparameters VDD=1.8\n")
            psf = os.path.join(base, "psf")
            os.unlink(os.path.join(psf, "spectre.out"))
            with open(os.path.join(psf, "logFile"), "w") as stream:
                stream.write("owned synthetic PSF metadata\n")
            self.fixture.command["spectre_timing"]["runs"].append({"cwd": netlist, "returncode": 0,
                "started_epoch": time.time() - 1, "finished_epoch": time.time()})
        self.reader = lambda requests: [dict((name, 1.8) for name in row["names"]) for row in requests]

    def validate(self, **kwargs):
        return completion.validate_candidate_completion(self.fixture.workspace, self.fixture.candidate_root,
            self.fixture.command, self.fixture.parsed, config=self.cfg,
            native_parameter_reader=kwargs.get("reader", self.reader))

    def test_alps_acceptance_is_tagged_receipt_and_psf_readback_not_spectre_log(self):
        evidence = self.validate()
        self.assertEqual(evidence["execution_simulator"], "alps")
        self.assertEqual(evidence["spectre_runs"], [])
        self.assertEqual(len(evidence["simulator_runs"]), 2)
        self.assertTrue(evidence["native_parameter_readback_verified"])

    def test_native_completion_accepts_owned_alps_companion_without_logfile(self):
        for receipt in self.fixture.command["spectre_timing"]["runs"]:
            psf = os.path.join(os.path.dirname(receipt["cwd"]), "psf")
            os.rename(os.path.join(psf, "logFile"), psf + ".log")
        evidence = self.validate()
        self.assertTrue(evidence["native_parameter_readback_verified"])
        self.assertTrue(all(row["solver_log"]["kind"] == "alps_psf_log" for row in evidence["simulator_runs"]))

    def test_missing_or_ambiguous_receipt_is_rejected(self):
        rows = self.fixture.command["spectre_timing"]["runs"]
        rows.append(copy.deepcopy(rows[0]))
        with self.assertRaises(completion.UnsupportedCompletionEvidence):
            self.validate()
        rows[:] = []
        with self.assertRaises(completion.UnsupportedCompletionEvidence):
            self.validate()

    def test_wrong_native_parameter_readback_is_rejected(self):
        with self.assertRaises(completion.CompletionEvidenceError):
            self.validate(reader=lambda requests: [dict((name, 9.0) for name in row["names"]) for row in requests])

    def test_spectre_command_or_stale_spectre_summary_cannot_prove_alps(self):
        self.fixture.command["execution_simulator"] = "spectre"
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "command simulator"):
            self.validate()
        self.fixture.command["execution_simulator"] = "alps"
        receipt = self.fixture.command["spectre_timing"]["runs"][0]
        psf = os.path.join(os.path.dirname(receipt["cwd"]), "psf")
        with open(os.path.join(psf, "spectre.out"), "w") as stream:
            stream.write("spectre completes with 0 errors\n")
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "conflicting Spectre"):
            self.validate()

    def test_stale_psf_cannot_use_a_new_receipt(self):
        receipt = self.fixture.command["spectre_timing"]["runs"][0]
        path = os.path.join(os.path.dirname(receipt["cwd"]), "psf", "logFile")
        os.utime(path, (receipt["started_epoch"] - 20, receipt["started_epoch"] - 20))
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "stale"):
            self.validate()

    def test_missing_current_psf_or_incomplete_actual_values_is_rejected(self):
        with self.assertRaises(completion.CompletionEvidenceError):
            self.validate(reader=lambda requests: [{} for row in requests])
        receipt = self.fixture.command["spectre_timing"]["runs"][0]
        path = os.path.join(os.path.dirname(receipt["cwd"]), "psf", "logFile")
        os.unlink(path)
        with self.assertRaisesRegex(completion.CompletionEvidenceError, "PSF"):
            self.validate()


@unittest.skipUnless(os.environ.get("DISPLAY"), "Tk integration needs a verified existing DISPLAY")
class AlpsGuiStateTests(unittest.TestCase):
    def test_simulator_form_roundtrip_and_run_spec_preserve_explicit_alps(self):
        fixture = test_gui_optional_settings.GuiOptionalSettingsTkTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture._prepared()
        fixture.app.selected_test = "VCO"
        fixture.app.test_choice_var.set("VCO")
        fixture.app.simulator_var.set("alps")
        fixture.app.execution_backend_var.set("private_netlist")
        path, document = fixture._save("alps-settings.json")
        self.assertEqual(document["schema_version"], "crescendo.configuration.v1")
        self.assertEqual(document["settings"]["optimization"]["simulator"], "alps")
        self.assertEqual(document["execution_selection"]["tests"], ["VCO"])
        fixture.app.simulator_var.set("spectre")
        # The legacy empty setupdb fixture has no real Test catalogue. Supply
        # the selected Test and its hierarchy to the real configuration loader.
        preview = fixture.service.inspect_cells(fixture.setup_path)[1]
        catalog = {"setup": copy.deepcopy(preview["setup"]),
                   "tests": copy.deepcopy(preview["setup"]["tests"])}
        with mock.patch.object(OptimizerService, "_inspect_tests_registered",
                               return_value=("fixture-test-catalog", catalog)), \
                mock.patch.object(OptimizerService, "_inspect_test_cells_registered",
                                  return_value=("fixture-inspection", preview)):
            fixture._load(path)
        self.assertEqual(fixture.app.selected_test, "VCO", fixture.app.settings_status_var.get())
        self.assertIsNotNone(fixture.app.prepared_id, fixture.app.settings_status_var.get())
        self.assertEqual(fixture.app.simulator_var.get(), "alps")
        fixture.app.job_command_var.set("batch-submit -q fixture")
        self.assertEqual(fixture.app._build_run_settings()["simulator"], "alps")

    def test_closed_application_cycles_finalize_on_the_tk_owner_thread(self):
        fixture = test_gui_optional_settings.GuiOptionalSettingsTkTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        owner, finalized = threading.get_ident(), []

        class AppCycle(object):
            def __del__(self):
                finalized.append(threading.get_ident())

        cycle = AppCycle()
        cycle.app = fixture.app
        fixture.app._fixture_gc_cycle = cycle
        del cycle
        fixture._close()
        self.assertIsNone(fixture.app)
        self.assertEqual(finalized, [owner])


if __name__ == "__main__":
    unittest.main()
