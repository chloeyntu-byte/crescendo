from __future__ import print_function

import copy
import json
import os
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine import settings_state as state


class SettingsStateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-settings-space '")
        self.addCleanup(self.directory.cleanup)
        self.path = os.path.join(self.directory.name, ".crescendo", "settings", "TB.json")
        self.design = {"library": "DesignLib", "cell": "VCO_TB", "view": "config"}
        self.setup = {"identity": {"library": "DesignLib", "cell": "VCO", "view": "maestro"},
                      "designs": [self.design], "path": "/old/machine/design/VCO/maestro/maestro.sdb",
                      "backend": "maestro"}
        self.parameter = {"parameter_id": "parameter-old-id", "name": "w",
            "design_library": "DesignLib", "design_cell": "VCO_TB", "design_view": "config",
            "hierarchy_path": "/Core/M0", "master_library": "OriginalPDK", "master_cell": "nmos",
            "owner_library": "DesignLib", "owner_cell": "VCO_Core", "owner_view": "schematic",
            "resolved_library": "OriginalPDK", "resolved_cell": "nmos", "resolved_view": "spectre",
            "value_type": "float"}
        self.cell = {"scope_id": "scope-old-id", "design": self.design, "hierarchy_path": "/Core",
                     "library": "DesignLib", "cell": "VCO_Core", "view": "schematic", "selectable": True}
        self.outputs = [{"key": "Osc::frequency", "test": "Osc", "name": "frequency"},
                        {"key": "Osc::/internalNode", "test": "Osc", "name": "/internalNode"}]
        self.raw = {"setup": self.setup, "parameter_configs": {
            "parameter-old-id": {"selected": True, "minimum": "0.3u", "maximum": "unfinished edit",
                                  "unit": "u", "scale": "log", "value_type": "float",
                                  "validation_passed": True}},
            "selected_cell_scope_ids": ["scope-old-id"],
            "objectives": [{"output_key": "Osc::frequency", "aggregation": "mean", "corners": ["tt", "ss"],
                            "direction": "target", "target": "not finished"}],
            "constraints": [{"output_key": "Osc::frequency", "aggregation": "max", "corners": ["tt"],
                             "operator": "<=", "limit": "1g"}],
            "optimization": {"algorithm": "nsga2", "generations": "2", "candidate_timeout_seconds": "600",
                "job_command_override": True, "job_command": "batch-submit -q short -R 'cpu=6;mem=30000'",
                "job_max_override": True, "job_max_jobs": "64", "maestro_candidate_mode": "batch",
                "advanced_visible": True, "algorithm_parameters": {"nsga2": {"population": "8", "seed": "4"}},
                "algorithm_parameter_manual": [["nsga2", "population"]]},
            "selected_history": "unsafe.rdb", "checkpoint": {"run_id": "must-not-save"},
            "runtime_env": {"LD_LIBRARY_PATH": "must-not-save"}, "token": "must-not-save"}

    def document(self):
        return state.build_settings_state(self.raw, {self.parameter["parameter_id"]: self.parameter},
                                          [self.cell], self.outputs)

    def current(self):
        return {"setup": self.setup, "parameter_by_id": {self.parameter["parameter_id"]: self.parameter},
                "cell_scopes": [self.cell], "outputs": self.outputs, "corners": ["tt", "ss"]}

    def test_both_solver_and_submission_templates_roundtrip_with_raw_drafts(self):
        self.raw['optimization']['execution_backend'] = 'maestro_netlist'
        commands = {'solver_templates': {'spectre': '{simulator} {netlist} +log {log} -raw {psf}',
            'alps': 'unfinished ALPS template edit'}, 'submission_template': 'batch-submit -q chosen {job_script}',
            'scheduler_query_template': 'batch-query --id {job_id}',
            'scheduler_cancel_template': 'unfinished cancel draft', 'private_token': 'must-not-save'}
        self.raw['optimization']['execution_commands'] = commands
        document = self.document()
        state.save_settings_state(self.path, document)
        result = state.match_settings_state(state.read_settings_state(self.path), self.current())['apply']['optimization']
        self.assertEqual(result['execution_backend'], 'maestro_netlist')
        self.assertEqual(result['execution_commands'], {'solver_templates': commands['solver_templates'],
            'submission_template': commands['submission_template'],
            'scheduler_query_template': commands['scheduler_query_template'],
            'scheduler_cancel_template': commands['scheduler_cancel_template']})
        self.assertNotIn('must-not-save', json.dumps(document))

    def test_incomplete_raw_edits_save_and_match_without_native_preparation(self):
        document = self.document()
        state.save_settings_state(self.path, document)
        loaded = state.read_settings_state(self.path)
        result = state.match_settings_state(loaded, self.current())
        application = result["apply"]
        self.assertEqual(application["parameter_configs"]["parameter-old-id"]["maximum"], "unfinished edit")
        self.assertEqual(application["objectives"][0]["target"], "not finished")
        self.assertEqual(application["optimization"]["generations"], "2")
        self.assertEqual(application["optimization"]["job_max_jobs"], "64")
        self.assertEqual(application["selected_cell_scope_ids"], ["scope-old-id"])
        self.assertNotIn("value_type", application["parameter_configs"]["parameter-old-id"])
        self.assertNotIn("path", application["setup"])
        self.assertFalse(result["warnings"])

    def test_relocation_is_logical_and_directory_tracks_actual_project(self):
        config = {"cadence": {"project_dir": self.directory.name},
                  "runtime": {"run_root": "/do/not/use/runtime"}}
        launch = {"source": {"work_dir": "/obsolete/source"}}
        self.assertEqual(state.settings_state_directory(config, launch, self.setup["path"]),
                         os.path.join(self.directory.name, ".crescendo", "settings"))
        self.assertEqual(state.settings_state_directory({"_selected_setup_source": {"work_dir": self.directory.name}}, launch),
                         os.path.join(self.directory.name, ".crescendo", "settings"))
        current = self.current()
        current["setup"] = copy.deepcopy(self.setup)
        current["setup"]["path"] = "/entirely/new/workspace/VCO/maestro/maestro.sdb"
        result = state.match_settings_state(self.document(), current)
        self.assertEqual(result["counts"]["parameters"]["matched"], 1)
        self.assertTrue(result["apply"]["setup"]["matches"])

    def test_changed_ids_match_exact_occurrence_and_device_evidence(self):
        current = self.current()
        parameter = copy.deepcopy(self.parameter)
        parameter["parameter_id"] = "new-algorithm-parameter-id"
        current["parameter_by_id"] = {parameter["parameter_id"]: parameter}
        current["cell_scopes"] = [dict(self.cell, scope_id="new-scope-id")]
        result = state.match_settings_state(self.document(), current)
        self.assertEqual(list(result["apply"]["parameter_configs"]), [parameter["parameter_id"]])
        self.assertEqual(result["apply"]["selected_cell_scope_ids"], ["new-scope-id"])

    def test_same_id_different_pdk_or_config_implementation_is_skipped(self):
        for key, value in (("master_library", "OtherPDK"), ("resolved_view", "veriloga"),
                           ("owner_cell", "DifferentCore"), ("value_type", "integer")):
            current = self.current()
            changed = dict(self.parameter)
            changed[key] = value
            current["parameter_by_id"] = {changed["parameter_id"]: changed}
            result = state.match_settings_state(self.document(), current)
            self.assertFalse(result["apply"]["parameter_configs"])
            self.assertEqual(result["counts"]["parameters"]["skipped"], 1)

    def test_mismatched_tb_retains_binding_and_skips_design_items(self):
        current = self.current()
        current["setup"] = {"identity": {"library": "Other", "cell": "Maestro", "view": "maestro"},
                            "designs": [{"library": "Other", "cell": "TB", "view": "config"}]}
        changed = dict(self.parameter, design_library="Other", design_cell="TB")
        current["parameter_by_id"] = {changed["parameter_id"]: changed}
        current["cell_scopes"] = [dict(self.cell, design=current["setup"]["designs"][0])]
        result = state.match_settings_state(self.document(), current)
        self.assertFalse(result["apply"]["setup"]["matches"])
        self.assertFalse(result["apply"]["parameter_configs"])
        self.assertFalse(result["apply"]["selected_cell_scope_ids"])
        self.assertFalse(result["apply"]["objectives"])
        self.assertEqual(result["apply"]["optimization"]["generations"], "2")

    def test_unmatched_items_do_not_block_other_matches(self):
        document = self.document()
        document["parameters"].append({"parameter_id": "deleted", "evidence": {}, "config": {"selected": True}})
        document["cells"].append({"scope_id": "deleted", "evidence": {}, "selected": True})
        document["objectives"].append({"output_key": "deleted", "direction": "minimize"})
        current = self.current()
        current["corners"] = ["tt"]
        result = state.match_settings_state(document, current)
        self.assertEqual(result["counts"]["parameters"], {"matched": 1, "skipped": 1})
        self.assertEqual(result["counts"]["cells"], {"matched": 1, "skipped": 1})
        self.assertEqual(result["counts"]["objectives"], {"matched": 1, "skipped": 1})
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["tt"])
        self.assertTrue(result["warnings"])

    def test_ambiguous_parameter_never_picks_arbitrary_device(self):
        current = self.current()
        current["parameter_by_id"] = [dict(self.parameter, parameter_id="a"), dict(self.parameter, parameter_id="b")]
        result = state.match_settings_state(self.document(), current)
        self.assertFalse(result["apply"]["parameter_configs"])
        self.assertEqual(result["counts"]["parameters"]["skipped"], 1)

    def test_path_outputs_and_changed_output_identity_are_not_restored(self):
        document = self.document()
        document["objectives"].append({"output_key": "Osc::/internalNode", "direction": "minimize"})
        result = state.match_settings_state(document, self.current())
        self.assertEqual(result["counts"]["objectives"], {"matched": 1, "skipped": 1})
        current = self.current()
        current["outputs"] = [dict(self.outputs[0], test="AnotherTest")]
        self.assertFalse(state.match_settings_state(document, current)["apply"]["objectives"])

    def test_same_maestro_changed_test_binding_skips_old_metrics(self):
        self.setup["tests"] = [dict(self.design, name="Osc")]
        document = self.document()
        current = self.current()
        current["setup"] = copy.deepcopy(self.setup)
        current["setup"]["tests"][0]["cell"] = "Other_TB"
        result = state.match_settings_state(document, current)
        self.assertTrue(result["apply"]["setup"]["matches"])
        self.assertFalse(result["apply"]["objectives"])
        self.assertFalse(result["apply"]["constraints"])

    def test_snapshot_excludes_secrets_history_run_evidence_and_checks(self):
        serialized = json.dumps(self.document())
        for token in ("unsafe.rdb", "must-not-save", "LD_LIBRARY_PATH", "validation_passed", "checkpoint"):
            self.assertNotIn(token, serialized)
        document = self.document()
        document.update(runtime_env={"PATH": "injected"}, selected_history="foreign", run_id="injected")
        document["optimization"].update(checkpoint_path="foreign", controller_token="inj")
        result = state.match_settings_state(document, self.current())
        self.assertNotIn("injected", json.dumps(result))
        self.assertNotIn("foreign", json.dumps(result))

    def test_invalid_types_are_skipped_while_raw_numbers_remain_editable(self):
        document = self.document()
        document["optimization"].update(algorithm="unknown", advanced_visible="yes",
            algorithm_parameter_manual=[[{}, "seed"], ["nsga2", {}], ["nsga2", "seed"]],
            maestro_candidate_mode="unknown", job_max_override=1)
        document["parameters"][0]["config"].update(selected="yes", scale="invalid", value_type="integer")
        result = state.match_settings_state(document, self.current())
        self.assertNotIn("algorithm", result["apply"]["optimization"])
        self.assertNotIn("advanced_visible", result["apply"]["optimization"])
        self.assertEqual(result["apply"]["optimization"]["algorithm_parameter_manual"], [["nsga2", "seed"]])
        self.assertNotIn("selected", result["apply"]["parameter_configs"]["parameter-old-id"])
        self.assertNotIn("scale", result["apply"]["parameter_configs"]["parameter-old-id"])

    def test_entry_types_cannot_corrupt_parameter_units_or_optimization_entries(self):
        for invalid in (True, False, None, {"bad": "value"}, ["bad"]):
            document = self.document()
            document["parameters"][0]["config"].update(minimum=invalid, maximum=invalid,
                unit=invalid, scale=invalid)
            document["optimization"].update(generations=invalid, candidate_timeout_seconds=invalid,
                job_max_jobs=invalid, job_command=invalid, run_name=invalid)
            document["optimization"]["algorithm_parameters"]["nsga2"]["population"] = invalid
            result = state.match_settings_state(document, self.current())["apply"]
            config = result["parameter_configs"]["parameter-old-id"]
            for key in ("minimum", "maximum", "unit", "scale"):
                self.assertNotIn(key, config)
            for key in ("generations", "candidate_timeout_seconds", "job_max_jobs", "job_command", "run_name"):
                self.assertNotIn(key, result["optimization"])
            self.assertNotIn("population", result["optimization"]["algorithm_parameters"]["nsga2"])

    def test_legacy_numeric_entries_become_text_without_altering_raw_drafts(self):
        document = self.document()
        document["parameters"][0]["config"].update(minimum=0.3, maximum=4, unit="U")
        document["optimization"].update(generations=2, candidate_timeout_seconds=600.5, job_max_jobs=64)
        document["optimization"]["algorithm_parameters"]["nsga2"]["population"] = 8
        result = state.match_settings_state(document, self.current())["apply"]
        self.assertEqual(result["parameter_configs"]["parameter-old-id"]["minimum"], "0.3")
        self.assertEqual(result["parameter_configs"]["parameter-old-id"]["maximum"], "4")
        self.assertEqual(result["parameter_configs"]["parameter-old-id"]["unit"], "U")
        self.assertEqual(result["optimization"]["generations"], "2")
        self.assertEqual(result["optimization"]["candidate_timeout_seconds"], "600.5")
        self.assertEqual(result["optimization"]["job_max_jobs"], "64")
        self.assertEqual(result["optimization"]["algorithm_parameters"]["nsga2"]["population"], "8")
        for invalid_unit in (123, "unknown_unit"):
            document["parameters"][0]["config"]["unit"] = invalid_unit
            self.assertNotIn("unit", state.match_settings_state(document, self.current())["apply"]
                             ["parameter_configs"]["parameter-old-id"])
        self.assertEqual(state.match_settings_state(self.document(), self.current())["apply"]
                         ["parameter_configs"]["parameter-old-id"]["maximum"], "unfinished edit")

    def test_overwrite_keeps_private_timestamped_exact_backup(self):
        first = self.document()
        state.save_settings_state(self.path, first)
        with open(self.path, "rb") as stream:
            original = stream.read()
        second = self.document()
        second["optimization"]["generations"] = "3"
        state.save_settings_state(self.path, second)
        parent = os.path.dirname(self.path)
        backups = [item for item in os.listdir(parent) if item.startswith("TB.json.bak-")]
        self.assertEqual(len(backups), 1)
        with open(os.path.join(parent, backups[0]), "rb") as stream:
            self.assertEqual(stream.read(), original)
        self.assertEqual(state.read_settings_state(self.path)["optimization"]["generations"], "3")
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_symlinks_hardlinks_and_fifo_are_rejected_without_reading_targets(self):
        state.save_settings_state(self.path, self.document())
        linked = os.path.join(os.path.dirname(self.path), "linked.json")
        os.symlink(self.path, linked)
        for operation in (lambda: state.read_settings_state(linked),
                          lambda: state.save_settings_state(linked, self.document())):
            with self.assertRaises((OSError, ValueError)):
                operation()
        os.unlink(linked)
        os.link(self.path, linked)
        with self.assertRaises(ValueError): state.read_settings_state(linked)
        with self.assertRaises(ValueError): state.save_settings_state(linked, self.document())
        os.unlink(linked)
        fifo = os.path.join(os.path.dirname(self.path), "pipe.json")
        os.mkfifo(fifo)
        with self.assertRaises(ValueError): state.read_settings_state(fifo)
        linked_parent = os.path.join(self.directory.name, "linked-parent")
        os.symlink(os.path.dirname(self.path), linked_parent)
        with self.assertRaises((OSError, ValueError)): state.read_settings_state(os.path.join(linked_parent, "TB.json"))

    def test_bad_json_schema_and_file_limits_do_not_mutate_original_preferences(self):
        state.save_settings_state(self.path, self.document())
        for raw in (b"{", b"{\"schema_version\":\"wrong\"}", b"[]", b"\xff", b"{\"schema_version\": NaN}",
                    b"{\"number\": " + b"9" * 1000 + b"}"):
            with open(self.path, "wb") as stream: stream.write(raw)
            with self.assertRaises(ValueError): state.read_settings_state(self.path)
        with open(self.path, "wb") as stream: stream.write(b"x" * (state.MAX_FILE_BYTES + 1))
        with self.assertRaises(ValueError): state.read_settings_state(self.path)
        self.assertEqual(self.raw["parameter_configs"]["parameter-old-id"]["maximum"], "unfinished edit")
        document = self.document()
        document["oversized"] = "x" * state.MAX_FILE_BYTES
        with self.assertRaises(ValueError): state.save_settings_state(self.path, document)

    def test_warning_details_are_bounded_for_many_obsolete_records(self):
        document = self.document()
        document["parameters"] = [{"parameter_id": "old{}".format(index), "evidence": {}, "config": {}}
                                  for index in range(500)]
        result = state.match_settings_state(document, self.current())
        self.assertEqual(result["counts"]["parameters"]["skipped"], 500)
        self.assertEqual(len(result["warnings"]), state.MAX_WARNINGS + 1)
        self.assertIn("additional", result["warnings"][-1])

    def test_large_catalogue_save_reload_match_is_linear_and_bounded(self):
        count = 25000
        parameters, configs = {}, {}
        for index in range(count):
            key = "parameter-{}".format(index)
            parameters[key] = dict(self.parameter, parameter_id=key, hierarchy_path="/Core/M{}".format(index))
            configs[key] = dict(self.raw["parameter_configs"]["parameter-old-id"])
        raw = dict(self.raw, parameter_configs=configs)
        started = time.monotonic()
        document = state.build_settings_state(raw, parameters, [self.cell], self.outputs)
        state.save_settings_state(self.path, document)
        loaded = state.read_settings_state(self.path)
        current = dict(self.current(), parameter_by_id=parameters)
        with mock.patch.object(state, "_same_evidence", wraps=state._same_evidence) as compare:
            result = state.match_settings_state(loaded, current)
        self.assertEqual(result["counts"]["parameters"], {"matched": count, "skipped": 0})
        self.assertLessEqual(compare.call_count, count * 2)
        self.assertLess(os.stat(self.path).st_size, state.MAX_FILE_BYTES)
        # Evidence records latency, rather than a fragile wall-clock test limit.
        print("SETTINGS_LARGE_CATALOG count={} bytes={} elapsed_seconds={:.3f}".format(
            count, os.stat(self.path).st_size, time.monotonic() - started))

    def test_duplicate_catalogue_does_not_produce_quadratic_matching(self):
        document = self.document()
        document["parameters"] = [document["parameters"][0]] * 1000
        current = self.current()
        current["parameter_by_id"] = [dict(self.parameter)] * 1000
        with mock.patch.object(state, "_same_evidence", wraps=state._same_evidence) as compare:
            result = state.match_settings_state(document, current)
        self.assertEqual(result["counts"]["parameters"], {"matched": 0, "skipped": 1000})
        self.assertLessEqual(compare.call_count, 2000)


if __name__ == "__main__":
    unittest.main()
