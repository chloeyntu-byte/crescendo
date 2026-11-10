"""Preference recovery follows current native values without granting authority."""
import copy
import unittest

from crescendo_engine import unified_configuration as config
from tests import test_unified_configuration as base_config
from tests import test_unified_configuration_service as base_service


class NativeConfigurationMergeTests(unittest.TestCase):
    setUp = base_config.UnifiedConfigurationTests.setUp
    document = base_config.UnifiedConfigurationTests.document
    current = base_config.UnifiedConfigurationTests.current
    def native(self, jobs="4", root="/native/old"):
        return config.maestro_settings_snapshot({"tests": [dict(self.design, name="Test", enabled=True,
            analyses=[{"name": "tran", "enabled": True, "fields": {"stop": "1u"}},
                      {"name": "ac", "enabled": False, "fields": {}}])],
            "corners": [{"name": "tt", "enabled": True, "variables": [{"name": "file", "value": '"unused.dat"'}]}],
            "save_options": {"effective": {"simulation_root": root}}},
            {"global": {"available": True, "properties": {"maxjobs": jobs, "jobsubmitcommand": "native submit"}},
             "tests": []})

    def test_native_changes_disable_only_conflicting_overrides(self):
        self.raw["maestro_snapshot"] = self.native()
        self.raw["optimization"].update(job_command_override=True, job_command="custom submit",
            data_root_override_enabled=True, data_root_override="/chosen/data")
        current = self.current()
        current["maestro_snapshot"] = self.native(jobs="12", root="/native/new")
        result = config.match_configuration(self.document(), current)
        applied = result["apply"]["optimization"]
        self.assertFalse(applied["job_max_override"])
        self.assertEqual(applied["job_max_jobs"], "12")
        self.assertFalse(applied["data_root_override_enabled"])
        self.assertEqual(applied["data_root_override"], "/native/new")
        self.assertTrue(applied["job_command_override"])
        self.assertEqual(applied["job_command"], "custom submit")
        self.assertEqual(applied["generations"], "unfinished")
        self.assertEqual(result["counts"]["parameters"]["matched"], 1)
        self.assertTrue(any(row["field"] == "job.global.maxjobs" for row in result["maestro_differences"]))

    def test_unchanged_native_snapshot_preserves_explicit_preferences(self):
        self.raw["maestro_snapshot"] = self.native()
        current = self.current()
        current["maestro_snapshot"] = self.native()
        result = config.match_configuration(self.document(), current)
        self.assertTrue(result["apply"]["optimization"]["job_max_override"])
        self.assertEqual(result["maestro_differences"], [])
        self.assertFalse(result["warnings"])

    def test_changed_test_job_policy_disables_conflicting_global_override(self):
        self.raw["maestro_snapshot"] = self.native()
        self.raw["maestro_snapshot"]["job"]["tests"] = {"Test": {"maxjobs": "4"}}
        current = self.current()
        current["maestro_snapshot"] = copy.deepcopy(self.raw["maestro_snapshot"])
        current["maestro_snapshot"]["job"]["tests"]["Test"]["maxjobs"] = "2"
        result = config.match_configuration(self.document(), current)
        self.assertFalse(result["apply"]["optimization"]["job_max_override"])
        self.assertTrue(any("test:Test" in warning for warning in result["warnings"]))

    def test_legacy_native_conflict_warns_and_keeps_current_without_guessing_unknown(self):
        current = self.current()
        current["maestro_snapshot"] = self.native()
        result = config.match_configuration(self.document(), current)
        self.assertFalse(result["apply"]["optimization"]["job_max_override"])
        current["maestro_snapshot"].pop("job")
        result = config.match_configuration(self.document(), current)
        self.assertTrue(result["apply"]["optimization"]["job_max_override"])

    def test_execution_roundtrip_partial_match_and_native_enabled_changes(self):
        self.raw["maestro_snapshot"] = self.native()
        self.raw["execution_selection"] = {"tests": ["Test", "removed"],
            "analyses": {"Test": ["tran", "missing"]}}
        current = self.current()
        current["maestro_snapshot"] = self.native()
        current["maestro_snapshot"]["tests"]["Test"]["analyses"]["ac"]["enabled"] = True
        document = self.document()
        config.save_configuration(self.path, document)
        result = config.match_configuration(config.read_configuration(self.path), current)
        self.assertEqual(result["apply"]["execution_selection"],
            {"tests": ["Test"], "analyses": {"Test": ["tran", "ac"]}})
        self.assertTrue(any("removed" in warning for warning in result["warnings"]))
        self.assertTrue(any("missing" in warning for warning in result["warnings"]))
        self.assertEqual(result["pending"]["execution_selection"], self.raw["execution_selection"])

    def test_explicit_selected_test_survives_source_disabled_with_mask_restrictions(self):
        old, current = self.corner_fixture()
        self.raw['execution_selection'] = {'tests': ['Test']}
        current['maestro_snapshot']['tests']['Test']['enabled'] = False
        current['maestro_snapshot']['test_corner_masks']['Test']['disabled_corners'] = ['ss']
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result['apply']['execution_selection']['tests'], ['Test'])
        self.assertEqual(result['apply']['objectives'][0]['corners'], ['tt'])
        self.assertEqual(result['apply']['constraints'][0]['corners'], ['tt'])

    def test_removed_execution_choices_use_current_native_defaults(self):
        self.raw["execution_selection"] = {"tests": ["removed"], "analyses": {"Test": ["removed"]}}
        current = self.current()
        current["maestro_snapshot"] = self.native()
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["execution_selection"],
                         {"tests": ["Test"], "analyses": {"Test": ["tran"]}})

    def test_malformed_optional_snapshot_cannot_break_recovery(self):
        self.raw["maestro_snapshot"] = {"tests": {"Test": "invalid"}, "job": "invalid", "save": []}
        self.raw["execution_selection"] = {"tests": ["Test"], "analyses": {"Test": ["tran"]}}
        current = self.current()
        current["maestro_snapshot"] = self.native()
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["execution_selection"]["tests"], ["Test"])

    def corner_fixture(self, selected=None):
        self.raw["maestro_snapshot"] = self.native()
        old = self.raw["maestro_snapshot"]
        old["corners"] = {"tt": {"enabled": True}, "ss": {"enabled": True},
                          "ff": {"enabled": False}, "unused": {"enabled": True}}
        old["test_corner_masks"] = {"Test": {"known": True, "disabled_corners": [], "nominal_enabled": True}}
        self.raw["objectives"][0].update(corners=selected or ["tt", "ss"], direction="target", target="2.5")
        self.raw["constraints"][0]["corners"] = list(selected or ["tt", "ss"])
        current = self.current()
        current["corners"] = list(old["corners"])
        current["maestro_snapshot"] = copy.deepcopy(old)
        return old, current

    def test_changed_global_corner_only_removes_conflicting_choice(self):
        old, current = self.corner_fixture()
        current["maestro_snapshot"]["corners"]["tt"]["enabled"] = False
        result = config.match_configuration(self.document(), current)
        for section in ("objectives", "constraints"):
            self.assertEqual(result["apply"][section][0]["corners"], ["ss"])
        self.assertEqual(result["apply"]["objectives"][0]["target"], "2.5")
        self.assertEqual(result["pending"]["settings"]["objectives"][0]["corners"], ["tt", "ss"])
        self.assertTrue(any("global enabled" in warning for warning in result["warnings"]))

    def test_existing_corner_reenabled_in_native_is_restored_but_new_corner_is_not(self):
        old, current = self.corner_fixture()
        current["maestro_snapshot"]["corners"]["ff"]["enabled"] = True
        current["maestro_snapshot"]["corners"]["brand_new"] = {"enabled": True}
        current["corners"].append("brand_new")
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["tt", "ss", "ff"])
        self.assertNotIn("unused", result["apply"]["objectives"][0]["corners"])

    def test_unchanged_disabled_native_corner_keeps_explicit_measurement_choice(self):
        old, current = self.corner_fixture(["ff"])
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["ff"])
        self.assertFalse(result["metric_corner_conflicts"])

    def test_changed_test_mask_affects_only_its_test(self):
        old, current = self.corner_fixture()
        other_test = dict(self.design, name="Other")
        self.raw["setup"]["tests"].append(other_test)
        self.raw["setup"]["designs"].append(dict(self.design))
        current["setup"]["tests"].append(other_test)
        current["outputs"].append({"key": "Other::gain", "test": "Other", "name": "gain"})
        self.raw["objectives"].append({"output_key": "Other::gain", "direction": "maximize", "corners": ["tt", "ss"]})
        current["maestro_snapshot"]["test_corner_masks"]["Test"]["disabled_corners"] = ["tt"]
        document = config.build_configuration(self.raw, {"opaque-id": self.parameter}, [self.cell], current["outputs"])
        result = config.match_configuration(document, current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["ss"])
        self.assertEqual(result["apply"]["objectives"][1]["corners"], ["tt", "ss"])
        self.assertTrue(any("Test mask" in warning for warning in result["warnings"]))

    def test_nominal_mask_changes_keep_empty_draft_without_borrowing_other_corner(self):
        old, current = self.corner_fixture(["_default"])
        old["corners"]["_default"] = {"enabled": True}
        current["maestro_snapshot"]["corners"]["_default"] = {"enabled": True}
        current["corners"].append("_default")
        current["maestro_snapshot"]["test_corner_masks"]["Test"]["nominal_enabled"] = False
        result = config.match_configuration(self.document(), current)
        definition = result["apply"]["objectives"][0]
        self.assertEqual(definition["corners"], [])
        self.assertEqual(definition["output_key"], "Test::gain")
        self.assertEqual(definition["direction"], "target")
        self.assertEqual(definition["target"], "2.5")
        self.assertTrue(any("empty selection draft" in warning for warning in result["warnings"]))

    def test_mask_unavailable_or_newly_known_does_not_override_measurement(self):
        for old_known, current_known in ((False, True), (True, False)):
            old, current = self.corner_fixture()
            old["test_corner_masks"]["Test"]["known"] = old_known
            current["maestro_snapshot"]["test_corner_masks"]["Test"]["known"] = current_known
            current["maestro_snapshot"]["test_corner_masks"]["Test"]["disabled_corners"] = ["tt"]
            result = config.match_configuration(self.document(), current)
            self.assertEqual(result["apply"]["objectives"][0]["corners"], ["tt", "ss"])

    def test_positive_native_change_does_not_add_corner_without_known_available_mask(self):
        old, current = self.corner_fixture()
        current["maestro_snapshot"]["corners"]["ff"]["enabled"] = True
        current["maestro_snapshot"]["test_corner_masks"]["Test"]["known"] = False
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["tt", "ss"])

    def test_corner_snapshot_cannot_add_a_name_absent_from_current_catalogue(self):
        old, current = self.corner_fixture()
        current["maestro_snapshot"]["corners"]["ff"]["enabled"] = True
        current["corners"].remove("ff")
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["tt", "ss"])

    def test_reenabled_mask_restores_only_existing_corner_with_current_global_enabled(self):
        old, current = self.corner_fixture(["ss"])
        old["test_corner_masks"]["Test"]["disabled_corners"] = ["tt", "ff"]
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["ss", "tt"])

    def test_all_corner_names_removed_keep_valid_output_empty_not_default(self):
        old, current = self.corner_fixture(["tt"])
        current["corners"] = ["unrelated"]
        current["maestro_snapshot"]["corners"] = {"unrelated": {"enabled": True}}
        document = self.document()
        result = config.match_configuration(document, current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], [])
        self.assertEqual(result["apply"]["constraints"][0]["corners"], [])
        self.assertEqual(result["pending"]["settings"]["objectives"][0]["corners"], ["tt"])
        current["outputs"] = []
        missing_output = config.match_configuration(document, current)
        self.assertEqual(missing_output["apply"]["objectives"], [])

    def test_absent_or_nonboolean_native_flags_do_not_invent_corner_change(self):
        old, current = self.corner_fixture()
        old["corners"]["tt"].pop("enabled")
        current["maestro_snapshot"]["corners"]["tt"]["enabled"] = False
        old["corners"]["ss"]["enabled"] = "true"
        current["maestro_snapshot"]["corners"]["ss"]["enabled"] = False
        result = config.match_configuration(self.document(), current)
        self.assertEqual(result["apply"]["objectives"][0]["corners"], ["tt", "ss"])

    def test_parameter_evidence_selects_only_unique_deepest_containing_scope(self):
        document = self.document()
        document["selection"]["provided"] = False
        document["settings"]["cells"] = []
        root = dict(self.cell, scope_id="root", hierarchy_path="/")
        result = config.match_configuration_cells(document, [root, self.cell], current_setup=self.setup)
        self.assertEqual(result["scope_ids"], ["scope-id"])
        self.assertEqual(result["skipped_count"], 0)
        ambiguous = config.match_configuration_cells(document,
            [self.cell, dict(self.cell, scope_id="duplicate")], current_setup=self.setup)
        self.assertEqual(ambiguous["scope_ids"], [])

    def test_parameter_evidence_cannot_expand_an_explicit_empty_or_cross_tb_selection(self):
        self.raw["selected_cell_scope_ids"] = []
        document = self.document()
        self.assertEqual(config.match_configuration_cells(document, [self.cell], current_setup=self.setup)["scope_ids"], [])
        document["selection"]["provided"] = False
        other = dict(self.setup, identity=dict(self.setup["identity"], cell="AnotherTB"))
        self.assertEqual(config.match_configuration_cells(document, [self.cell], current_setup=other)["scope_ids"], [])

    def test_raw_native_content_and_execution_authority_are_not_saved(self):
        snapshot = self.native()
        self.assertNotIn("native_content", repr(snapshot))
        self.assertNotIn("baseline", snapshot)
        self.assertNotIn("checkpoint", snapshot)


class ConfigurationLifecycleProgressTests(unittest.TestCase):
    setUp = base_service.UnifiedConfigurationServiceTests.setUp
    select = base_service.UnifiedConfigurationServiceTests.select
    inspect = base_service.UnifiedConfigurationServiceTests.inspect
    apply = base_service.UnifiedConfigurationServiceTests.apply

    def test_parameter_path_evidence_automatically_applies_matching_scope(self):
        document = copy.deepcopy(self.document)
        document["settings"]["cells"] = []
        document["selection"]["provided"] = False
        result = self.service.load_configuration(document)
        self.assertTrue(result["ready_for_settings"])
        self.assertEqual(result["selected_cell_scope_ids"], ["root"])
    def test_legacy_keeps_only_previously_prepared_matching_cells(self):
        self.service.config["_selected_setup_path"] = self.path
        self.service.prepared["prior"] = dict(self.prepared, setup_path=self.path,
            cell_selection={"scopes": [self.scope]})
        document = copy.deepcopy(self.document)
        document["settings"]["cells"] = []
        document["settings"]["parameters"] = []
        document["selection"]["provided"] = False
        result = self.service.load_configuration(document)
        self.assertEqual([row[0] for row in self.calls], ["select", "inspect", "apply"])
        self.assertEqual(result["selected_cell_scope_ids"], ["root"])
        self.assertTrue(any("matching selected cells" in warning for warning in result["warnings"]))

    def test_activity_is_bounded_copied_and_complete(self):
        self.service.load_configuration(self.document)
        snapshot = self.service.activity_snapshot()
        self.assertEqual(snapshot["state"], "complete")
        self.assertEqual(snapshot["stage"], "configuration_complete")
        snapshot["events"].clear()
        self.assertTrue(self.service.activity_snapshot()["events"])
        for number in range(120):
            self.service._set_activity("fixture", str(number))
        self.assertEqual(len(self.service.activity_snapshot()["events"]), 100)

    def test_failed_activity_does_not_claim_completion(self):
        self.service._inspect_cells_registered.side_effect = ValueError("changed fixture")
        self.service.load_configuration(self.document)
        self.assertEqual(self.service.activity_snapshot()["state"], "error")

    def test_final_display_reuses_current_operation_projection(self):
        self.preview["eda_context"] = {"cadence": {"effective": {"fixture": "current"}}}
        self.service.load_configuration(self.document)
        self.assertFalse(self.service.eda_setup.called)
