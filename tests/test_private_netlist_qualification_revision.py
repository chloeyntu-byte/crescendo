"""Old native mapping evidence must not regain execution authority on reuse."""
from __future__ import print_function

import copy
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from unittest import mock

from crescendo_engine import private_netlist
from crescendo_engine.native_qualification import NATIVE_QUALIFICATION_REVISION
from tests import test_netlist_providers, test_private_netlist


class NativeTemplateQualificationRevisionTests(unittest.TestCase):
    def template(self, root, current=True):
        manifest = test_private_netlist.PrivateNetlistCandidateTest().template(root)
        manifest["qualification"]["scope"] = "native_expression_mapping_and_recorded_reference_points"
        manifest["netlist_generation"] = {
            "source_mode": "private_netlist",
            "method": "qualified_symbolic_template",
            "generation_protocol": "crescendo.native_run_device_parameters.v1",
        }
        if current:
            manifest["qualification"]["revision"] = NATIVE_QUALIFICATION_REVISION
            manifest["netlist_generation"]["qualification_revision"] = NATIVE_QUALIFICATION_REVISION
        self.seal(manifest)
        return manifest

    def seal(self, manifest):
        manifest["manifest_sha256"] = private_netlist._manifest_digest(manifest)

    def test_current_native_template_and_candidate_retain_both_stamps(self):
        with tempfile.TemporaryDirectory() as root:
            template = self.template(root)
            self.assertTrue(private_netlist.validate_manifest(template))
            candidate = private_netlist.materialize_candidate(
                template, {"p": 1000}, os.path.join(root, "candidate"), "c1")
            self.assertEqual(candidate["qualification"]["revision"], NATIVE_QUALIFICATION_REVISION)
            self.assertEqual(candidate["netlist_generation"]["qualification_revision"],
                             NATIVE_QUALIFICATION_REVISION)
            self.assertTrue(private_netlist.validate_manifest(candidate))

    def test_old_native_template_rejected_before_any_candidate_is_written(self):
        with tempfile.TemporaryDirectory() as root:
            template = self.template(root, current=False)
            destination = os.path.join(root, "candidate")
            with self.assertRaises(private_netlist.PrivateNetlistUnsupported):
                private_netlist.materialize_candidate(template, {"p": 1000}, destination, "c1")
            self.assertFalse(os.path.lexists(destination))

    def test_stale_missing_or_mismatched_stamp_rejected_even_without_file_checks(self):
        with tempfile.TemporaryDirectory() as root:
            template = self.template(root)
            for field in ("qualification", "netlist_generation"):
                name = "revision" if field == "qualification" else "qualification_revision"
                for value in (None, "crescendo.native_point_qualification.v1", True, 2):
                    with self.subTest(field=field, value=value):
                        changed = copy.deepcopy(template)
                        if value is None:
                            changed[field].pop(name)
                        else:
                            changed[field][name] = value
                        self.seal(changed)
                        with self.assertRaises(private_netlist.PrivateNetlistUnsupported):
                            private_netlist.validate_manifest(changed, verify_files=False)

    def test_scope_only_native_evidence_cannot_hide_old_qualification(self):
        with tempfile.TemporaryDirectory() as root:
            template = self.template(root, current=False)
            template.pop("netlist_generation")
            self.seal(template)
            with self.assertRaises(private_netlist.PrivateNetlistUnsupported):
                private_netlist.validate_manifest(template)
            template["qualification"]["revision"] = NATIVE_QUALIFICATION_REVISION
            self.seal(template)
            self.assertTrue(private_netlist.validate_manifest(template))

    def test_numeric_native_scope_uses_same_revision_gate(self):
        with tempfile.TemporaryDirectory() as root:
            template = self.template(root, current=False)
            template["qualification"]["scope"] = "numeric_native_CDF_and_generated_netlist_readback"
            self.seal(template)
            with self.assertRaises(private_netlist.PrivateNetlistUnsupported):
                private_netlist.validate_manifest(template)

    def test_non_native_legacy_mapping_fixture_remains_compatible(self):
        with tempfile.TemporaryDirectory() as root:
            template = test_private_netlist.PrivateNetlistCandidateTest().template(root)
            self.assertTrue(private_netlist.validate_manifest(template))
            candidate = private_netlist.materialize_candidate(
                template, {"p": 1000}, os.path.join(root, "candidate"), "legacy")
            self.assertNotIn("revision", candidate["qualification"])
            self.assertNotIn("qualification_revision", candidate["netlist_generation"])
            self.assertTrue(private_netlist.validate_manifest(candidate))

    def test_old_native_cleanup_remains_possible_without_upgrading_authority(self):
        with tempfile.TemporaryDirectory() as root:
            template = self.template(root, current=False)
            with open(os.path.join(template["root"], "private_netlist.json"), "w") as stream:
                json.dump(template, stream)
            with mock.patch.object(private_netlist, "reconcile_preparation",
                                   return_value={"cleanup_proven": True}):
                result = private_netlist.compact_preparation_artifacts(template)
            self.assertTrue(result["artifact_cleanup_complete"], result)
            self.assertNotIn("revision", template["qualification"])
            with self.assertRaises(private_netlist.PrivateNetlistUnsupported):
                private_netlist.validate_manifest(template)

    def test_new_public_mapping_proof_has_current_revision(self):
        fixture = test_private_netlist.PrivateNetlistMappingTest()
        proof = private_netlist.qualify_symbolic_netlist(
            fixture.netlist(), fixture.references(),
            {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"})
        self.assertEqual(proof["revision"], NATIVE_QUALIFICATION_REVISION)

    def test_new_template_builder_stamps_current_qualified_evidence(self):
        fixture = test_netlist_providers.NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = fixture.fixtures(root)
            prepared["setup"]["tests"][0].update(library="PRIVATE", cell="TB", view="schematic")
            parameters[0].update(numeric_value=2e-6, name="r", owner_library="PRIVATE", owner_cell="TB",
                owner_view="schematic", hierarchy_path="/R0", owner_occurrence_path="/",
                design_library="PRIVATE", design_cell="TB", design_view="schematic")
            with mock.patch.object(private_netlist, "build_workspace", side_effect=fixture.workspace), \
                    mock.patch.object(private_netlist, "_capture_phase", side_effect=fixture.native_capture(model)), \
                    mock.patch.object(private_netlist, "verify_sources_unchanged", return_value={"ok": True}):
                template = private_netlist.prepare_private_netlist(prepared, parameters, config,
                    os.path.join(root, "native-template"), spec=spec)
            self.assertEqual(template["qualification"]["revision"], NATIVE_QUALIFICATION_REVISION)
            self.assertEqual(template["netlist_generation"]["qualification_revision"],
                             NATIVE_QUALIFICATION_REVISION)
            self.assertEqual(template["netlist_generation"]["mapping_qualification"]["revision"],
                             NATIVE_QUALIFICATION_REVISION)
            self.assertTrue(private_netlist.validate_manifest(template))

    def test_borrowed_workspace_finalizer_does_not_mutate_sealed_template(self):
        fixture = test_netlist_providers.NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = fixture.fixtures(root)
            prepared["setup"]["tests"][0].update(library="PRIVATE", cell="TB", view="schematic")
            prepared["setup_private_session"] = {"workspace_root": os.path.join(root, "workspace")}
            parameters[0].update(numeric_value=2e-6, name="r", owner_library="PRIVATE", owner_cell="TB",
                owner_view="schematic", hierarchy_path="/R0", owner_occurrence_path="/",
                design_library="PRIVATE", design_cell="TB", design_view="schematic")
            workspace = fixture.workspace(prepared["setup"], prepared["hierarchy"], config,
                                          prepared["setup_private_session"]["workspace_root"])
            workspace.update(_suite_revision=1, native_parameter_last_values={"p": {"value": 2e-6}})

            @contextmanager
            def borrow(*args, **kwargs):
                try:
                    yield workspace
                finally:
                    workspace["_suite_revision"] = 2
                    workspace["native_parameter_last_values"]["p"]["value"] = 3e-6

            with mock.patch("crescendo_engine.setup_private_session.borrow_setup_private_session", borrow), \
                    mock.patch("crescendo_engine.setup_private_session.write_session_reference"), \
                    mock.patch.object(private_netlist, "_capture_phase", side_effect=fixture.native_capture(model)), \
                    mock.patch.object(private_netlist, "verify_sources_unchanged", return_value={"ok": True}):
                template = private_netlist.prepare_private_netlist(prepared, parameters, config,
                    os.path.join(root, "native-template"), spec=spec)
            with open(os.path.join(template["root"], "private_netlist.json"), encoding="utf-8") as stream:
                self.assertEqual(template, json.load(stream))
            self.assertEqual(workspace["_suite_revision"], 2)
            self.assertEqual(template["workspace"]["_suite_revision"], 1)
            self.assertEqual(template["workspace"]["native_parameter_last_values"]["p"]["value"], 2e-6)
            self.assertTrue(private_netlist.validate_manifest(template))
            private_netlist.materialize_candidate(template, {"p": 2e-6}, os.path.join(root, "candidate"), "c1")
            template["workspace"]["native_parameter_last_values"]["p"]["value"] = 4e-6
            with self.assertRaises(private_netlist.PrivateNetlistError):
                private_netlist.materialize_candidate(template, {"p": 2e-6}, os.path.join(root, "tampered"), "c2")


if __name__ == "__main__":
    unittest.main()
