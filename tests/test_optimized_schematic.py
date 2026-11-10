from __future__ import print_function

import copy
import json
import os
import re
import tempfile
import unittest
from unittest import mock

from crescendo_engine import optimized_schematic as export


def parameter(key="p1", occurrence="/A", name="w"):
    return {"parameter_id": key, "name": name, "backend": "schematic", "owner_library": "USER",
            "owner_cell": "unit", "owner_view": "schematic", "owner_occurrence_path": occurrence,
            "hierarchy_path": occurrence + "/M0", "design_library": "USER", "design_cell": "TB",
            "design_view": "config", "cdf_parameter_type": "string"}


def old_candidate(parameters, value="3.5u", requested=4e-6):
    return {"candidate_id": "g000_c0000", "status": "completed", "execution_backend": "persistent_maestro",
            "completion": {"ok": True, "adapter": "maestro-rdb-psf-v1"},
            "values": {row["parameter_id"]: requested for row in parameters},
            "parameter_application": {"ok": True, "parameters": [
                {"parameter_id": row["parameter_id"], "property": row["name"], "hierarchy_path": row["hierarchy_path"],
                 "persisted_schematic_value": value, "persisted_cdf_value": value, "requested": "4u"}
                for row in parameters]}}


def snapshot(parameters):
    return {"schema_version": export.SCHEMA + ".snapshot", "instances": [
        {"library": "PRIVATE", "cell": "private_unit", "view": "symbolic", "instance": "M0",
         "parameter_ids": [row["parameter_id"] for row in parameters],
         "properties": {"w": {"type": "string", "value": "CRESCENDO_NL_WIDTH"},
                        "area": {"type": "string", "value": "CRESCENDO_NL_WIDTH*200n"},
                        "display": {"type": "string", "value": "unchanged"}},
         "cdf": {"wr": {"type": "string", "value": "CRESCENDO_NL_WIDTH"}}}]}


class OptimizedSchematicEvidenceTests(unittest.TestCase):
    def test_name_is_legal_and_unicode_loss_or_truncation_does_not_collide(self):
        names = ["run_123", "hello world", "电路 A", "电路 B", "x" * 200 + "a", "x" * 200 + "b"]
        views = [export.optimized_view_name(name) for name in names]
        self.assertEqual(views[0], "schematic_run_123")
        self.assertEqual(len(set(views)), len(views))
        for view in views:
            self.assertLessEqual(len(view), export.MAX_VIEW_NAME)
            self.assertRegex(view, r"^schematic_[A-Za-z0-9_]+$")

    def test_old_candidate_uses_persisted_clamped_value(self):
        rows = [parameter()]
        evidence = export.build_export_evidence(old_candidate(rows), rows)
        self.assertAlmostEqual(evidence["values"]["p1"], 3.5e-6)
        self.assertNotEqual(evidence["values"]["p1"], 4e-6)

    def test_missing_or_disagreeing_actual_values_are_rejected(self):
        rows = [parameter()]
        candidate = old_candidate(rows)
        for bad in (None, "CRESCENDO_NL_WIDTH", "3u"):
            candidate["parameter_application"]["parameters"][0]["persisted_cdf_value"] = bad
            with self.assertRaises(export.OptimizedSchematicError):
                export.build_export_evidence(candidate, rows)

    def test_mock_failed_and_design_variable_candidates_are_rejected(self):
        rows = [parameter()]
        for key, value in (("execution_backend", "mock"), ("status", "failed")):
            candidate = old_candidate(rows); candidate[key] = value
            with self.assertRaises(export.OptimizedSchematicError): export.build_export_evidence(candidate, rows)
        rows[0]["backend"] = "design_variable"
        with self.assertRaises(export.OptimizedSchematicError): export.build_export_evidence(old_candidate(rows), rows)

    def test_private_mode_requires_owned_template_and_actual_completion_values(self):
        rows = [parameter()]
        manifest = {"qualification": {"ok": True}, "manifest_sha256": "good",
                    "parameter_symbols": {"p1": "CRESCENDO_NL_WIDTH"}, "export_snapshot": snapshot(rows)}
        candidate = {"candidate_id": "g000_c0000", "status": "completed", "execution_backend": "private_netlist",
                     "completion": {"ok": True, "template_sha256": "good", "parameters": {"p1": 3.51e-6}},
                     "values": {"p1": 9e-6}}
        evidence = export.build_export_evidence(candidate, rows, manifest)
        self.assertEqual(evidence["values"]["p1"], 3.51e-6)
        candidate["completion"]["template_sha256"] = "wrong"
        with self.assertRaises(export.OptimizedSchematicError): export.build_export_evidence(candidate, rows, manifest)

    def test_conflicting_occurrences_split_views(self):
        rows = [parameter(), parameter("p2", "/B")]
        candidate = old_candidate(rows)
        candidate["parameter_application"]["parameters"][1].update(persisted_schematic_value="5u", persisted_cdf_value="5u")
        plan = export.build_export_plan(export.build_export_evidence(candidate, rows), "run")
        self.assertEqual(len(plan["groups"]), 2)
        self.assertNotEqual(plan["groups"][0]["target_view"], plan["groups"][1]["target_view"])
        self.assertEqual({g["occurrences"][0]["owner_occurrence_path"] for g in plan["groups"]}, {"/A", "/B"})

    def test_equal_occurrences_share_view_but_retain_every_parameter_readback(self):
        rows = [parameter(), parameter("p2", "/B")]
        plan = export.build_export_plan(export.build_export_evidence(old_candidate(rows), rows), "run")
        self.assertEqual(len(plan["groups"]), 1)
        self.assertEqual(len(plan["groups"][0]["occurrences"]), 2)
        self.assertEqual({r["parameter_id"] for r in plan["groups"][0]["assignments"]}, {"p1", "p2"})

    def test_symbolic_derived_properties_are_resolved_without_callbacks(self):
        rows = [parameter()]
        evidence = export.build_export_evidence(old_candidate(rows), rows, snapshot=snapshot(rows))
        evidence.update(execution_backend="private_netlist", parameter_symbols={"p1": "CRESCENDO_NL_WIDTH"})
        group = export.build_export_plan(evidence, "run")["groups"][0]
        result = {row["name"]: row for row in export._resolved_properties(group, evidence)}
        self.assertAlmostEqual(float(result["area"]["value"]), 3.5e-6 * 200e-9)
        self.assertEqual(result["display"]["value"], "unchanged")
        self.assertNotIn("CRESCENDO_NL", json.dumps(result))
        group["staging_view"] = "cr_export_stage_owned"
        script = export._stage_script({"groups": [group]}, evidence, "/tmp/records", "/tmp/ready")
        self.assertNotIn("evalstring", script)
        self.assertNotIn("cdfUpdateInstParam", script)

    def test_unresolved_derived_expression_stops_before_native_write(self):
        rows = [parameter()]
        evidence = export.build_export_evidence(old_candidate(rows), rows, snapshot=snapshot(rows))
        evidence.update(execution_backend="private_netlist", parameter_symbols={"p1": "CRESCENDO_NL_WIDTH"})
        evidence["snapshot"]["instances"][0]["properties"]["area"]["value"] = "unknown(CRESCENDO_NL_WIDTH)"
        with self.assertRaises(export.OptimizedSchematicError):
            export._resolved_properties(export.build_export_plan(evidence, "run")["groups"][0], evidence)

    def test_source_target_collision_never_overwrites_source(self):
        rows = [parameter()]; rows[0]["owner_view"] = "schematic_run"
        group = export.build_export_plan(export.build_export_evidence(old_candidate(rows), rows), "run")["groups"][0]
        self.assertNotEqual(group["target_view"], rows[0]["owner_view"])
        self.assertTrue(group["target_view"].startswith("schematic_run_"))

    def test_native_snapshot_target_mapping_and_duplicate_guard(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "snapshot.records")
            lines = ['TARGET\t"p1"\t"P"\t"C"\t"V"\t"M0"',
                     'PROP\t"P"\t"C"\t"V"\t"M0"\t"w"\t"string"\t"CRESCENDO_NL_WIDTH"',
                     'CDF\t"P"\t"C"\t"V"\t"M0"\t"w"\t"string"\t"CRESCENDO_NL_WIDTH"']
            with open(path, "w") as stream: stream.write("\n".join(lines) + "\n")
            parsed = export.parse_export_snapshot(path)
            self.assertEqual(parsed["instances"][0]["parameter_ids"], ["p1"])
            self.assertEqual(parsed["instances"][0]["properties"]["w"]["value"], "CRESCENDO_NL_WIDTH")
            with open(path, "a") as stream: stream.write(lines[0] + "\n")
            with self.assertRaises(export.OptimizedSchematicError): export.parse_export_snapshot(path)

    def test_one_occurrence_cannot_overwrite_another_occurrence_source_view(self):
        rows = [parameter(), parameter("p2", "/B")]
        rows[0]["owner_view"] = "schematic_A"
        rows[1]["owner_view"] = "schematic_run"
        plan = export.build_export_plan(export.build_export_evidence(old_candidate(rows), rows), "run")
        protected = {row["owner_view"] for row in rows}
        self.assertTrue(all(group["target_view"] not in protected for group in plan["groups"]))

    def test_unselected_actual_source_view_is_protected(self):
        rows = [parameter()]
        plan = export.build_export_plan(export.build_export_evidence(old_candidate(rows), rows), "run",
                                        {("USER", "unit", "schematic_run")})
        self.assertNotEqual(plan["groups"][0]["target_view"], "schematic_run")

    def test_physical_library_aliases_do_not_overwrite_conflicting_solutions(self):
        rows = [parameter(), parameter("p2", "/B")]
        rows[1]["owner_library"] = "USER_ALIAS"
        candidate = old_candidate(rows)
        candidate["parameter_application"]["parameters"][1].update(persisted_schematic_value="5u",persisted_cdf_value="5u")
        plan = export.build_export_plan(export.build_export_evidence(candidate,rows),"run",
                                        library_paths={"USER":"/tmp/library","USER_ALIAS":"/tmp/library"})
        self.assertEqual(len(plan["groups"]),2)
        self.assertNotEqual(plan["groups"][0]["target_view"],plan["groups"][1]["target_view"])

    def test_multiple_selected_parameters_overlay_only_after_snapshot_merge(self):
        rows = [parameter(), parameter("p2", "/A", "l")]
        candidate = old_candidate(rows)
        candidate["parameter_application"]["parameters"][1].update(persisted_schematic_value="200n",persisted_cdf_value="200n")
        native = snapshot(rows)
        native["instances"][0]["properties"]["l"] = {"type":"string","value":"CRESCENDO_NL_LENGTH"}
        evidence = export.build_export_evidence(candidate, rows, snapshot=native)
        evidence.update(execution_backend="private_netlist",parameter_symbols={"p1":"CRESCENDO_NL_WIDTH","p2":"CRESCENDO_NL_LENGTH"})
        group = export.build_export_plan(evidence,"run")["groups"][0]
        resolved = {row["name"]:row["value"] for row in export._resolved_properties(group,evidence)}
        self.assertAlmostEqual(float(resolved["w"]),3.5e-6)
        self.assertAlmostEqual(float(resolved["l"]),200e-9)

    def test_readback_drift_prevents_publishing_the_wrong_solution(self):
        rows = [parameter()]
        plan = export.build_export_plan(export.build_export_evidence(old_candidate(rows), rows), "run")
        plan["groups"][0]["staging_view"] = "cr_export_stage_owned"
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "records")
            with open(path, "w") as stream:
                stream.write('STAGE\t"p1"\t"USER"\t"unit"\t"cr_export_stage_owned"\t"3u"\t"3u"\n')
            with self.assertRaises(export.OptimizedSchematicError): export._verify_readback(path, plan, "STAGE")


if __name__ == "__main__":
    unittest.main()
