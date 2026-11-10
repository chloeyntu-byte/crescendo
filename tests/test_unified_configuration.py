from __future__ import print_function

import copy
import json
import os
import stat
import tempfile
import unittest
from unittest import mock

from crescendo_engine import settings_state as old
from crescendo_engine import unified_configuration as config


class UnifiedConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-unified-config '")
        self.addCleanup(self.directory.cleanup)
        self.path = os.path.join(self.directory.name, "nested", "saved.json")
        self.design = {"library": "User", "cell": "TB", "view": "config"}
        self.setup = {"identity": {"library": "User", "cell": "ADE", "view": "maestro"},
                      "path": "/old/work/ADE/maestro/maestro.sdb", "backend": "maestro",
                      "designs": [self.design], "tests": [dict(self.design, name="Test") ]}
        self.parameter = {"parameter_id": "opaque-id", "design_library": "User", "design_cell": "TB",
            "design_view": "config", "hierarchy_path": "/Core/M1", "name": "nfin",
            "master_library": "PDK", "master_cell": "nmos", "owner_library": "User",
            "owner_cell": "Core", "owner_view": "schematic_abc", "value_type": "integer"}
        self.cell = dict(self.design, scope_id="scope-id", design=self.design, hierarchy_path="/Core",
                         cell="Core", view="schematic_abc", selectable=True)
        self.output = {"key": "Test::gain", "test": "Test", "name": "gain"}
        self.raw = {"setup": self.setup,
            "parameter_configs": {"opaque-id": {"selected": True, "minimum": "2", "maximum": "draft",
                                  "unit": "", "scale": "linear"}},
            "selected_cell_scope_ids": ["scope-id"],
            "objectives": [{"output_key": "Test::gain", "aggregation": "mean", "direction": "maximize", "corners": ["tt"]}],
            "constraints": [{"output_key": "Test::gain", "aggregation": "min", "operator": ">=", "limit": "draft", "corners": ["tt"]}],
            "optimization": {"generations": "unfinished", "algorithm": "nsga2", "job_max_jobs": "8",
                "job_max_override": True, "simulator": "alps", "execution_backend": "maestro_netlist",
                "candidate_timeout_seconds": "", "job_timeout_start_seconds": "",
                "execution_commands": {"solver_templates": {"spectre": "spectre {netlist}", "alps": "alps {netlist}"},
                                       "submission_template": "batch-submit {job_script}",
                                       "scheduler_query_template": "batch-query {job_id}",
                                       "scheduler_cancel_template": "batch-cancel {job_id}"}},
            "environment_overrides": {"alps_bin": "/software/empyrean/alps/bin/alps", "spectre_model_dir": "/pdk/spectre"}}

    def document(self, previous=None):
        return config.build_configuration(self.raw, {"opaque-id": self.parameter}, [self.cell], [self.output], previous)

    def current(self):
        return {"setup": copy.deepcopy(self.setup), "parameter_by_id": {"opaque-id": copy.deepcopy(self.parameter)},
                "cell_scopes": [copy.deepcopy(self.cell)], "outputs": [copy.deepcopy(self.output)], "corners": ["tt"]}

    def legacy(self):
        return {"schema_version": "crescendo.design_config.v1", "ade_setup": {"path": self.setup["path"], "backend": "maestro"},
            "optimization": {"variables": [{"parameter_id": "opaque-id", "minimum": 2, "maximum": 5, "scale": "linear"}],
                "objectives": copy.deepcopy(self.raw["objectives"]), "constraints": copy.deepcopy(self.raw["constraints"]),
                "run": {"algorithm": "nsga2", "algorithm_parameters": {"population": 8, "seed": 4},
                        "generations": 7, "candidate_timeout_seconds": -1, "simulator": "alps",
                        "execution_backend": "private_netlist", "execution_commands": self.raw["optimization"]["execution_commands"],
                        "maestro_job_overrides": {"command": "batch-submit -q short", "max_jobs": 8,
                            "timeouts": {"start_seconds": 10, "configure_seconds": 20, "run_seconds": 30, "linger_seconds": None}}}}}

    def test_complete_configuration_roundtrip(self):
        document = self.document()
        config.save_configuration(self.path, document)
        loaded = config.read_configuration(self.path)
        plan = config.match_configuration(loaded, self.current())
        self.assertEqual(plan["apply"]["selected_cell_scope_ids"], ["scope-id"])
        self.assertEqual(plan["apply"]["parameter_configs"]["opaque-id"]["maximum"], "draft")
        self.assertEqual(plan["apply"]["optimization"]["generations"], "unfinished")
        self.assertEqual(plan["apply"]["constraints"][0]["limit"], "draft")
        self.assertEqual(plan["environment_overrides"], self.raw["environment_overrides"])
        self.assertFalse(plan["warnings"])

    def test_empty_draft_can_be_saved_before_load(self):
        document = config.build_configuration({})
        config.save_configuration(self.path, document)
        loaded = config.read_configuration(self.path)
        self.assertFalse(loaded["settings"]["parameters"])
        self.assertFalse(loaded["selection"]["provided"])
        self.assertFalse(config.match_configuration(loaded, {})["apply"]["parameter_configs"])

    def test_zero_and_invalid_time_drafts_are_preserved_not_validated(self):
        self.raw["optimization"].update(candidate_timeout_seconds="0", job_timeout_start_seconds="not yet")
        plan = config.match_configuration(self.document(), self.current())
        self.assertEqual(plan["apply"]["optimization"]["candidate_timeout_seconds"], "0")
        self.assertEqual(plan["apply"]["optimization"]["job_timeout_start_seconds"], "not yet")

    def test_logical_matching_survives_machine_relocation(self):
        current = self.current()
        current["setup"]["path"] = "/new/server/ADE/maestro/maestro.sdb"
        plan = config.match_configuration(self.document(), current)
        self.assertEqual(plan["counts"]["parameters"]["matched"], 1)
        self.assertEqual(plan["counts"]["objectives"]["matched"], 1)

    def test_changed_owner_view_is_skipped_but_retained(self):
        current = self.current()
        current["parameter_by_id"]["opaque-id"]["owner_view"] = "schematic_new"
        document = self.document()
        plan = config.match_configuration(document, current)
        self.assertFalse(plan["apply"]["parameter_configs"])
        self.assertEqual(plan["pending"]["settings"]["parameters"], document["settings"]["parameters"])
        self.assertTrue(plan["warnings"])

    def test_ambiguous_parameter_id_is_not_applied(self):
        current = self.current()
        current["parameter_by_id"] = [self.parameter, copy.deepcopy(self.parameter)]
        plan = config.match_configuration(self.document(), current)
        self.assertFalse(plan["apply"]["parameter_configs"])

    def test_unavailable_objective_remains_in_pending(self):
        current = self.current()
        current["outputs"] = []
        plan = config.match_configuration(self.document(), current)
        self.assertFalse(plan["apply"]["objectives"])
        self.assertTrue(plan["pending"]["settings"]["objectives"])

    def test_no_cells_never_means_select_everything(self):
        document = self.document()
        del document["settings"]["cells"]
        del document["selection"]
        plan = config.match_configuration_cells(document, [self.cell])
        self.assertFalse(plan["selection_present"])
        self.assertFalse(plan["scope_ids"])
        self.assertTrue(plan["warnings"])

    def test_explicit_empty_cell_selection_stays_empty(self):
        self.raw["selected_cell_scope_ids"] = []
        plan = config.match_configuration_cells(self.document(), [self.cell])
        self.assertTrue(plan["selection_present"])
        self.assertFalse(plan["scope_ids"])

    def test_custom_schematic_cell_identity_matches_new_scope_id(self):
        cell = dict(self.cell, scope_id="new-id")
        plan = config.match_configuration_cells(self.document(), [cell])
        self.assertEqual(plan["scope_ids"], ["new-id"])

    def test_changed_cell_binding_skipped(self):
        plan = config.match_configuration_cells(self.document(), [dict(self.cell, view="ext_abc")])
        self.assertFalse(plan["scope_ids"])
        self.assertEqual(plan["skipped_count"], 1)

    def test_duplicate_cell_identity_skipped(self):
        plan = config.match_configuration_cells(self.document(), [self.cell, dict(self.cell, scope_id="another")])
        self.assertFalse(plan["scope_ids"])

    def test_nonselectable_cell_skipped(self):
        self.assertFalse(config.match_configuration_cells(self.document(), [dict(self.cell, selectable=False)])["scope_ids"])

    def test_old_state_compatibility(self):
        old_document = old.build_settings_state(self.raw, [self.parameter], [self.cell], [self.output])
        plan = config.match_configuration(old_document, self.current())
        self.assertEqual(plan["counts"]["parameters"]["matched"], 1)
        self.assertEqual(plan["apply"]["optimization"]["simulator"], "alps")

    def test_legacy_design_migration_same_path_unique_ids(self):
        plan = config.match_configuration(self.legacy(), self.current())
        self.assertEqual(plan["apply"]["parameter_configs"]["opaque-id"]["minimum"], "2")
        self.assertEqual(plan["apply"]["parameter_configs"]["opaque-id"]["unit"], "")
        self.assertEqual(plan["counts"]["objectives"]["matched"], 1)
        self.assertTrue(plan["warnings"])

    def test_legacy_design_global_settings_migrate(self):
        optimization = config.match_configuration(self.legacy(), self.current())["apply"]["optimization"]
        self.assertEqual(optimization["candidate_timeout_seconds"], "")
        self.assertEqual(optimization["job_command"], "batch-submit -q short")
        self.assertEqual(optimization["job_max_jobs"], "8")
        self.assertEqual(optimization["job_timeout_start_seconds"], "10")
        self.assertEqual(optimization["job_timeout_linger_seconds"], "")
        self.assertEqual(optimization["algorithm_parameters"]["nsga2"]["population"], "8")
        self.assertIn(["nsga2", "population"], optimization["algorithm_parameter_manual"])
        self.assertEqual(optimization["execution_backend"], "private_netlist")
        self.assertEqual(optimization["execution_commands"], self.raw["optimization"]["execution_commands"])

    def test_legacy_design_wrong_source_cannot_hydrate(self):
        current = self.current()
        current["setup"]["path"] = "/different/setup"
        self.assertFalse(config.match_configuration(self.legacy(), current)["apply"]["parameter_configs"])

    def test_legacy_design_matching_identity_allows_relocation(self):
        legacy = self.legacy()
        legacy["ade_setup"]["identity"] = copy.deepcopy(self.setup["identity"])
        current = self.current()
        current["setup"]["path"] = "/new/server/setup"
        self.assertTrue(config.match_configuration(legacy, current)["apply"]["parameter_configs"])

    def test_legacy_design_conflicting_identity_cannot_hydrate_even_same_path(self):
        legacy = self.legacy()
        legacy["ade_setup"]["identity"] = {"library": "Other", "cell": "ADE", "view": "maestro"}
        self.assertFalse(config.match_configuration(legacy, self.current())["apply"]["parameter_configs"])

    def test_legacy_ambiguous_ids_not_hydrated(self):
        current = self.current()
        current["parameter_by_id"] = [self.parameter, copy.deepcopy(self.parameter)]
        self.assertFalse(config.match_configuration(self.legacy(), current)["apply"]["parameter_configs"])

    def test_legacy_design_cells_missing_is_warning_not_all_cells(self):
        plan = config.match_configuration_cells(self.legacy(), [self.cell])
        self.assertFalse(plan["selection_present"])
        self.assertFalse(plan["scope_ids"])
        self.assertTrue(plan["warnings"])

    def test_legacy_migration_can_be_saved_then_loaded(self):
        config.save_configuration(self.path, self.legacy())
        document = config.read_configuration(self.path)
        self.assertTrue(document["legacy"]["design_config"])
        self.assertTrue(config.match_configuration(document, self.current())["apply"]["parameter_configs"])

    def test_legacy_source_locator_name_fields_migrate(self):
        legacy = self.legacy()
        legacy["setup_locator"] = {"path": self.setup["path"], "library_name": "User", "cell_name": "ADE", "view_name": "maestro"}
        self.assertEqual(config.normalize_configuration(legacy)["source"]["identity"], self.setup["identity"])

    def test_all_runtime_authority_fields_are_removed(self):
        self.raw.update(private_session={"token": "no-save"}, checkpoint={"run_id": "no-save"}, runtime_env={"PASSWORD": "no-save"})
        document = self.document()
        document.update(setup_private_session={"token": "no-save"}, checkpoint={"run_id": "no-save"}, **{"token": "no-save"})
        document["settings"]["optimization"]["validation_passed"] = "no-save"
        clean = config.normalize_configuration(document)["document"]
        self.assertNotIn("no-save", json.dumps(clean))

    def test_manual_license_and_unknown_secrets_are_not_saved(self):
        self.raw["environment_overrides"].update({"license_file": "must-not-copy", "LM_LICENSE_FILE": "must-not-copy", "token": "must-not-copy"})
        self.raw["environment_draft"] = {"fields": {"license_file": "must-not-copy", "alps_bin": "draft"}, "unknown": "must-not-copy"}
        document = self.document()
        self.assertNotIn("must-not-copy", json.dumps(document))
        self.assertEqual(document["environment_draft"]["fields"], {"alps_bin": "draft"})

    def test_environment_drafts_preserve_invalid_json_without_parsing(self):
        draft = {"fields": {"alps_bin": "unfinished path", "simulator_threads": "unfinished"},
                 "policy": "custom", "pdk_profiles_text": "[{", "alps_model_map_text": "{unfinished",
                 "extra_binary_paths_text": "/path with spaces\nunfinished", "extra_library_paths_text": ""}
        self.raw["environment_draft"] = draft
        document = self.document()
        self.assertEqual(document["environment_draft"], draft)
        config.save_configuration(self.path, document)
        self.assertEqual(config.read_configuration(self.path)["environment_draft"], draft)

    def test_nested_manual_model_mapping_allowlist(self):
        self.raw["environment_overrides"] = {"alps_model_file_map": {"/old/toplevel.scs": {
            "path": "/new/toplevel.scs", "section": "TT", "sections": {"SS": "ALPS_SS"}, "token": "must-not-copy"}},
            "pdk_profiles": [{"name": "PDK", "model_roots": ["/pdk/models"], "token": "must-not-copy",
                "model_path_remaps": [{"source": "/old", "destination": "/new", "token": "must-not-copy"}],
                "adapter_policy": {"mode": "generic-only", "token": "must-not-copy"}}]}
        document = self.document()
        self.assertNotIn("must-not-copy", json.dumps(document))
        self.assertEqual(document["environment_overrides"]["pdk_profiles"][0]["model_path_remaps"], [{"source": "/old", "destination": "/new"}])

    def test_pair_path_remaps_and_string_lists_are_retained(self):
        result = config.clean_environment_overrides({"model_path_remaps": [["/old", "/new"]], "extra_binary_paths": ["/bin"]})
        self.assertEqual(result["model_path_remaps"], [["/old", "/new"]])
        self.assertEqual(result["extra_binary_paths"], ["/bin"])

    def test_unmatched_parameter_and_metrics_survive_save_after_partial_load(self):
        previous = self.document()
        raw = {"setup": self.setup, "parameter_configs": {}, "objectives": [], "constraints": [], "optimization": {}}
        document = config.build_configuration(raw, [], [], [], previous=previous)
        self.assertEqual(document["settings"]["parameters"], previous["settings"]["parameters"])
        self.assertEqual(document["settings"]["objectives"], previous["settings"]["objectives"])
        self.assertEqual(document["settings"]["constraints"], previous["settings"]["constraints"])
        self.assertEqual(document["settings"]["optimization"], previous["settings"]["optimization"])
        self.assertEqual(document["environment_overrides"], previous["environment_overrides"])

    def test_changed_owner_same_id_keeps_unmatched_old_draft(self):
        previous = self.document()
        self.parameter["owner_view"] = "schematic_new"
        document = self.document(previous)
        self.assertEqual(len(document["settings"]["parameters"]), 2)
        self.assertEqual(len(config.match_configuration(document, self.current())["apply"]["parameter_configs"]), 1)

    def test_known_deleted_metric_not_resurrected_from_previous(self):
        previous = self.document()
        self.raw["objectives"] = []
        self.assertFalse(self.document(previous)["settings"]["objectives"])

    def test_known_deselected_cells_not_resurrected(self):
        previous = self.document()
        self.raw["selected_cell_scope_ids"] = []
        self.assertFalse(config.match_configuration_cells(self.document(previous), [self.cell])["scope_ids"])

    def test_missing_setup_context_retains_previous_source(self):
        previous = self.document()
        document = config.build_configuration({}, previous=previous)
        self.assertEqual(document["source_setup"], previous["source_setup"])
        self.assertTrue(document["selection"]["provided"])

    def test_source_relocation_disables_old_absolute_data_root(self):
        self.raw["optimization"].update(data_root_override_enabled=True, data_root_override="/old/data")
        current = self.current()
        current["setup"]["path"] = "/new/server/setup"
        plan = config.match_configuration(self.document(), current)
        self.assertFalse(plan["apply"]["optimization"]["data_root_override_enabled"])
        self.assertTrue(plan["storage_warning"])

    def test_partial_and_wrong_type_sections_are_ignored(self):
        document = {"schema_version": config.SCHEMA, "settings": ["invalid"], "legacy": [], "selection": []}
        envelope = config.normalize_configuration(document)
        self.assertFalse(envelope["settings"]["parameters"])
        self.assertFalse(envelope["document"]["selection"]["provided"])

    def test_unknown_schema_is_rejected_without_native_activity(self):
        with self.assertRaises(ValueError):
            config.normalize_configuration({"schema_version": "future.v9"})

    def test_unified_envelope_is_accepted_as_input(self):
        envelope = config.normalize_configuration(self.document())
        self.assertEqual(config.configuration_settings(envelope), envelope["settings"])

    def test_backup_retention_and_previous_content(self):
        document = self.document()
        config.save_configuration(self.path, document)
        for value in ("one", "two", "three", "four", "five"):
            document["settings"]["optimization"]["generations"] = value
            config.save_configuration(self.path, document, retain_backups=2)
        backups = [name for name in os.listdir(os.path.dirname(self.path)) if ".bak-" in name]
        self.assertEqual(len(backups), 2)
        self.assertEqual(config.read_configuration(self.path)["settings"]["optimization"]["generations"], "five")
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_failed_replace_retains_existing_configuration(self):
        config.save_configuration(self.path, self.document())
        with mock.patch.object(config.os, "replace", side_effect=OSError("denied")):
            with self.assertRaises(OSError):
                config.save_configuration(self.path, config.build_configuration({}))
        self.assertTrue(config.read_configuration(self.path)["settings"]["parameters"])
        self.assertFalse(any(name.startswith(".configuration_") for name in os.listdir(os.path.dirname(self.path))))

    def test_symlink_read_and_overwrite_are_rejected(self):
        config.save_configuration(self.path, self.document())
        link = os.path.join(self.directory.name, "linked.json")
        os.symlink(self.path, link)
        for operation in (lambda: config.read_configuration(link), lambda: config.save_configuration(link, self.document())):
            with self.assertRaises((OSError, ValueError)):
                operation()

    def test_hardlink_read_and_overwrite_are_rejected(self):
        config.save_configuration(self.path, self.document())
        link = os.path.join(self.directory.name, "linked.json")
        os.link(self.path, link)
        for operation in (lambda: config.read_configuration(link), lambda: config.save_configuration(link, self.document())):
            with self.assertRaises((OSError, ValueError)):
                operation()

    def test_fifo_read_does_not_block(self):
        os.mkfifo(os.path.join(self.directory.name, "fifo"))
        with self.assertRaises(ValueError):
            config.read_configuration(os.path.join(self.directory.name, "fifo"))

    def test_linked_parent_directory_is_rejected(self):
        os.mkdir(os.path.join(self.directory.name, "real"))
        os.symlink(os.path.join(self.directory.name, "real"), os.path.join(self.directory.name, "linked"))
        with self.assertRaises((ValueError, OSError)):
            config.save_configuration(os.path.join(self.directory.name, "linked", "saved.json"), self.document())

    def test_corrupt_json_retains_file_and_reports(self):
        os.makedirs(os.path.dirname(self.path))
        with open(self.path, "w") as stream:
            stream.write('{"schema_version":')
        with self.assertRaises(ValueError):
            config.read_configuration(self.path)
        with open(self.path) as stream:
            self.assertEqual(stream.read(), '{"schema_version":')

    def test_nonfinite_and_huge_integer_are_rejected(self):
        os.makedirs(os.path.dirname(self.path))
        for text in ('{"schema_version":"crescendo.configuration.v1","bad":NaN}',
                     '{"schema_version":"crescendo.configuration.v1","bad":' + '9' * 65 + '}'):
            with open(self.path, "w") as stream:
                stream.write(text)
            with self.assertRaises(ValueError):
                config.read_configuration(self.path)

    def test_oversized_save_rejected_before_creating_directory(self):
        document = self.document()
        document["bad"] = "x" * (old.MAX_FILE_BYTES + 1)
        with self.assertRaises(ValueError):
            config.save_configuration(self.path, document)
        self.assertFalse(os.path.exists(os.path.dirname(self.path)))

    def test_last_configuration_roundtrip(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "states")}}
        config.save_last_configuration(self.document(), cfg)
        document = config.read_last_configuration(cfg, current_setup=self.setup)
        self.assertEqual(document["source_setup"]["identity"], self.setup["identity"])
        self.assertTrue(document["_restore_source"].endswith("last.json"))

    def test_restore_last_reads_old_state_schema(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "states")}}
        old.save_last_settings_state(old.build_settings_state(self.raw, [self.parameter], [self.cell], [self.output]), cfg)
        self.assertEqual(config.read_last_configuration(cfg, current_setup=self.setup)["schema_version"], config.SCHEMA)

    def test_restore_last_wrong_tb_is_not_loaded(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "states")}}
        config.save_last_configuration(self.document(), cfg)
        wrong = copy.deepcopy(self.setup)
        wrong["identity"]["cell"] = "Other"
        with self.assertRaises(ValueError):
            config.read_last_configuration(cfg, current_setup=wrong)

    def test_restore_last_missing_is_clear_optional_error(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "absent")}}
        with self.assertRaisesRegex(ValueError, "No previous configuration"):
            config.read_last_configuration(cfg, current_setup=self.setup)

    def test_restore_last_can_find_moved_namespace(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "new")}}
        config.save_configuration(self.path, self.document())
        with mock.patch.object(old, "_last_settings_candidates", return_value=[self.path]):
            restored = config.read_last_configuration(cfg, current_setup=self.setup)
        self.assertEqual(restored["_restore_source"], self.path)

    def test_match_does_not_modify_imported_document_or_current_catalog(self):
        document, current = self.document(), self.current()
        before = copy.deepcopy((document, current))
        config.match_configuration(document, current)
        config.match_configuration_cells(document, current["cell_scopes"])
        self.assertEqual((document, current), before)

    def test_resaving_hydrated_legacy_does_not_duplicate_matched_parameter(self):
        document = self.document(previous=self.legacy())
        self.assertEqual(len(document["settings"]["parameters"]), 1)
        self.assertFalse(document["legacy"]["design_config"])
        self.assertFalse(config.match_configuration(document, self.current())["warnings"])

    def test_resaving_legacy_preserves_unmatched_variable_as_draft(self):
        legacy = self.legacy()
        legacy["optimization"]["variables"].append({"parameter_id": "removed", "minimum": 1, "maximum": 2})
        document = self.document(previous=legacy)
        self.assertEqual(len(document["settings"]["parameters"]), 2)
        self.assertTrue(document["legacy"]["design_config"])
        self.assertIn("removed", [row["parameter_id"] for row in document["settings"]["parameters"]])

    def test_large_environment_editor_draft_is_preserved(self):
        self.raw["environment_draft"] = {"pdk_profiles_text": "[unfinished " + "x" * 8000}
        document = self.document()
        config.save_configuration(self.path, document)
        self.assertEqual(config.read_configuration(self.path)["environment_draft"], self.raw["environment_draft"])

    def test_real_moved_namespace_lookup_uses_same_lcv_only(self):
        from crescendo_engine.runtime_layout import design_storage_root
        source = {"library": "User", "cell": "ADE", "view": "maestro", "work_dir": self.directory.name,
                  "setup_path": os.path.join(self.directory.name, "User", "ADE", "maestro", "maestro.sdb")}
        root = design_storage_root(source, {})
        old_root = os.path.join(self.directory.name, ".crescendo", os.path.basename(root).rsplit("_", 1)[0] + "_0123456789abcdef", "states")
        config.save_configuration(os.path.join(old_root, "last.json"), self.document(), retain_backups=0)
        cfg = {"cadence": {"project_dir": self.directory.name}, "_selected_setup_source": source,
               "runtime": {"design_root": root, "settings_root": os.path.join(root, "states")}}
        restored = config.read_last_configuration(cfg, current_setup=self.setup)
        self.assertEqual(restored["_restore_source"], os.path.join(old_root, "last.json"))
        self.assertEqual(restored["settings"]["optimization"]["generations"], "unfinished")

    def test_corrupt_current_last_does_not_silently_restore_older_file(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "states")}}
        os.makedirs(cfg["runtime"]["settings_root"])
        with open(os.path.join(cfg["runtime"]["settings_root"], "last.json"), "w") as stream:
            stream.write("{broken")
        config.save_configuration(self.path, self.document())
        with mock.patch.object(old, "_last_settings_candidates", return_value=[self.path]) as fallback:
            with self.assertRaises(ValueError):
                config.read_last_configuration(cfg, current_setup=self.setup)
        self.assertFalse(fallback.called)

    def test_restore_last_repeated_saves_create_no_backups(self):
        cfg = {"runtime": {"settings_root": os.path.join(self.directory.name, "states")}}
        for _index in range(8):
            config.save_last_configuration(self.document(), cfg)
        names = os.listdir(cfg["runtime"]["settings_root"])
        self.assertEqual([name for name in names if not name.startswith('.crescendo-config-lock-')], ["last.json"])
        self.assertEqual(sum(name.startswith('.crescendo-config-lock-') for name in names), 1)

    def test_selection_presence_is_explicit_in_match_result(self):
        document = self.document()
        document["selection"]["provided"] = False
        self.assertFalse(config.match_configuration(document, self.current())["selection_present"])
        document["selection"]["provided"] = True
        self.assertTrue(config.match_configuration(document, self.current())["selection_present"])

    def test_no_native_eda_imports_needed_to_match(self):
        with mock.patch.dict("sys.modules", {"crescendo_engine.cadence": None,
                                               "crescendo_engine.setup_private_session": None}):
            self.assertTrue(config.match_configuration(self.document(), self.current())["apply"]["parameter_configs"])


if __name__ == "__main__":
    unittest.main()
