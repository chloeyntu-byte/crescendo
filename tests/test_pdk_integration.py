from __future__ import print_function

import os
import tempfile
import unittest

from crescendo_engine.cadence import detect_pdk_context
from crescendo_engine.pdk import normalize_pdk_profiles
from crescendo_engine.persistent_maestro import _patch_lines
from crescendo_engine.runner import (
    _WholeRunPersistentWorker, _prepared_eda_context_identity,
    _select_candidate_pdk_fallback,
)
from crescendo_engine.workspace import ParameterApplicationFailure, adapter_failure_code


class PdkIntegrationTests(unittest.TestCase):
    def test_detects_two_pdk_contexts_without_treating_shared_user_cell_as_pdk(self):
        with tempfile.TemporaryDirectory() as root:
            design = os.path.join(root, "design")
            shared = os.path.join(root, "shared")
            first = os.path.join(root, "vendor_a", "devices")
            second = os.path.join(root, "vendor_b", "devices")
            for path in (design, shared, first, second):
                os.makedirs(path)
            with open(os.path.join(root, "cds.lib"), "w") as stream:
                stream.write("DEFINE HH ./design\n")
                stream.write("DEFINE SHARED ./shared\n")
                stream.write("DEFINE DEV_A ./vendor_a/devices\n")
                stream.write("DEFINE DEV_B ./vendor_b/devices\n")
            setup = {"libraries": ["HH"], "source": {"library_root": design}}
            hierarchy = {"designs": [{
                "design": {"library": "HH", "cell": "TB"},
                "hierarchy": {
                    "cells": [
                        {"library": "HH", "cell": "TB"},
                        {"library": "SHARED", "cell": "DUT"},
                    ],
                    "instances": [
                        {"master_library": "SHARED", "has_schematic": True},
                        {"master_library": "DEV_A", "has_schematic": False},
                        {"master_library": "DEV_B", "has_schematic": False},
                    ],
                },
            }]}
            result = detect_pdk_context(setup, hierarchy)
            self.assertTrue(result["ok"])
            self.assertFalse(result["blocking"])
            self.assertEqual(result["candidate_libraries"], ["DEV_A", "DEV_B"])
            self.assertEqual(len(result["contexts"]), 2)
            self.assertIsNone(result["pdk_name"])
            self.assertTrue(all(
                row["adapter_selection"]["adapter_id"] == "generic-cdf"
                for row in result["contexts"]
            ))

    def test_examplekit_internal_resistor_schematic_does_not_hide_its_pdk(self):
        for vendor_library in ("examplekit18pas", "examplekit65pas_2tm"):
            with self.subTest(vendor_library=vendor_library), tempfile.TemporaryDirectory() as root:
                design = os.path.join(root, "design")
                shared = os.path.join(root, "shared")
                package = os.path.join(root, "vendor")
                vendor = os.path.join(package, "devices")
                for path in (design, shared, vendor, os.path.join(package, "models")):
                    os.makedirs(path)
                with open(os.path.join(root, "cds.lib"), "w") as stream:
                    stream.write("DEFINE HH ./design\n")
                    stream.write("DEFINE SHARED ./shared\n")
                    stream.write("DEFINE %s ./vendor/devices\n" % vendor_library)
                setup = {"libraries": ["HH"], "source": {"library_root": design}}
                hierarchy = {"designs": [{
                    "design": {"library": "HH", "cell": "ClassB_TB"},
                    "hierarchy": {
                        "cells": [
                            {"library": "HH", "cell": "ClassB_TB", "technology_library": vendor_library},
                            {"library": "SHARED", "cell": "VCO_class_B", "technology_library": vendor_library},
                            {"library": vendor_library, "cell": "resistor_ckt_rf", "technology_library": vendor_library},
                            {"library": "analogLib", "cell": "gnd", "technology_library": "analogLib"},
                        ],
                        "technology_bindings": [
                            {"library": "HH", "technology_library": vendor_library},
                            {"library": vendor_library, "technology_library": vendor_library},
                            {"library": "analogLib", "technology_library": "analogLib"},
                        ],
                        "instances": [
                            {"master_library": "SHARED", "has_schematic": True},
                            {"master_library": vendor_library, "has_schematic": True},
                            {"master_library": "analogLib", "has_schematic": True},
                        ],
                    },
                }]}
                result = detect_pdk_context(setup, hierarchy)
                self.assertTrue(result["ok"])
                self.assertFalse(result["blocking"])
                self.assertEqual(result["candidate_libraries"], [vendor_library])
                self.assertEqual(result["design_libraries"], ["HH", "SHARED"])
                self.assertEqual(result["pdk_name"], vendor_library)
                self.assertEqual(result["pdk_dir"], package)
                self.assertEqual(len(result["contexts"]), 1)
                self.assertEqual(result["contexts"][0]["adapter_selection"]["adapter_id"], "generic-cdf")

    def test_profile_inherits_explicit_top_level_adapter_policy(self):
        profiles = normalize_pdk_profiles({
            "pdk_adapter_policy": "allow:builtin.examplekit18pas-1",
            "pdk_profiles": [{"name": "KIT", "reference_libraries": ["KIT_DEV"]}],
        })
        self.assertEqual(profiles[0]["adapter_policy"], {
            "mode": "allow", "adapter_ids": ["builtin.examplekit18pas-1"],
        })

    def test_legacy_custom_adapter_is_marked_direct_for_compatibility(self):
        with tempfile.TemporaryDirectory() as root:
            profile = normalize_pdk_profiles({
                "pdk_name": "PDK", "pdk_adapter": "adapter.il",
            }, base_dir=root)[0]
            self.assertTrue(profile["adapter_policy"]["legacy_direct"])
            self.assertEqual(profile["adapter_policy"]["path"], os.path.join(root, "adapter.il"))

    def test_generic_skill_capability_probes_optional_cdf_update(self):
        rows = [{
            "library": "HH", "cell": "DUT", "instance": "M0", "property": "w",
            "value": "1u", "prop_type": "string", "parameter_id": "p0",
        }]
        script = "\n".join(_patch_lines(rows, "/tmp/records", None))
        self.assertIn("when(isCallable('cdfUpdateInstParam)", script)
        self.assertNotIn("requires cdfUpdateInstParam", script)
        self.assertIn("CRESCENDO_ADAPTER_ERROR:cdf_callback_incompatible", script)
        self.assertIn("CRESCENDO_ADAPTER_ERROR:cdf_update_api_incompatible", script)

    def test_custom_fallback_activates_only_for_classified_compatibility_failure(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = os.path.join(root, "adapter.il")
            with open(adapter, "w") as stream:
                stream.write("procedure(crescPdkApplyParameter(inst name value) t)\n")
            prepared = {"eda_context": {"pdk": {"contexts": [{
                "id": "pdk-one", "name": "PDK", "package_root": root,
                "reference_libraries": ["PDK"], "technology_libraries": [],
                "adapter_policy": {"mode": "custom", "path": adapter},
            }]}}}
            eligible = _select_candidate_pdk_fallback(
                prepared, ParameterApplicationFailure(
                    "callback failed", code="cdf_callback_incompatible",
                ),
            )
            self.assertTrue(eligible["activated"])
            self.assertEqual(eligible["adapter_path"], adapter)

            simulation = _select_candidate_pdk_fallback(
                prepared, ParameterApplicationFailure(
                    "did not converge", code="simulation_nonconvergence",
                ),
            )
            self.assertFalse(simulation["activated"])
            self.assertEqual(simulation["reason"], "fallback_forbidden")

            unclassified = _select_candidate_pdk_fallback(
                prepared, RuntimeError("ordinary simulation failure"),
            )
            self.assertFalse(unclassified["activated"])
            self.assertEqual(unclassified["reason"], "failure_not_classified")

    def test_auto_examplekit_fallback_resolves_packaged_pas_adapter(self):
        prepared = {"eda_context": {"pdk": {"contexts": [{
            "id": "examplekit-context", "name": "examplekit18pas",
            "package_root": "/portable/EXAMPLEKIT_018_PAS_OA_NATIVE",
            "reference_libraries": ["examplekit18pas"],
            "technology_libraries": ["examplekit18pas"],
            "adapter_policy": {"mode": "auto"},
        }]}}}
        selected = _select_candidate_pdk_fallback(
            prepared, ParameterApplicationFailure(
                "callback failed", code="cdf_callback_incompatible",
            ),
        )
        self.assertTrue(selected["activated"])
        self.assertEqual(selected["adapter_id"], "builtin.examplekit18pas-1")
        self.assertTrue(os.path.isfile(selected["adapter_path"]))

    def test_auto_examplekit65_fallback_requires_identity_and_callback_failure(self):
        prepared = {"eda_context": {"pdk": {"contexts": [{
            "id": "examplekit65-context", "name": "examplekit65pas_2tm",
            "package_root": "/portable/EXAMPLEKIT65PAS_1P10M_OA",
            "reference_libraries": ["examplekit65pas_2tm"],
            "technology_libraries": ["examplekit65pas_2tm"],
            "adapter_policy": {"mode": "auto"},
        }]}}}
        selected = _select_candidate_pdk_fallback(prepared, ParameterApplicationFailure(
            "PAS initialization required", code="cdf_callback_incompatible"))
        self.assertTrue(selected["activated"])
        self.assertEqual(selected["adapter_id"], "builtin.examplekit65pas-1")
        self.assertTrue(os.path.isfile(selected["adapter_path"]))
        circuit_failure = _select_candidate_pdk_fallback(prepared, ParameterApplicationFailure(
            "oscillator did not converge", code="simulation_nonconvergence"))
        self.assertFalse(circuit_failure["activated"])

    def test_diagnostic_marker_and_worker_fallback_are_local(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = os.path.join(root, "adapter.il")
            with open(adapter, "w") as stream:
                stream.write("procedure(crescPdkApplyParameter(inst name value) t)\n")
            self.assertEqual(adapter_failure_code({
                "ocean.stderr": "CRESCENDO_ADAPTER_ERROR:cdf_callback_incompatible failed",
            }), "cdf_callback_incompatible")
            original = {"cadence": {"pdk_adapter": "generic-cdf"}, "runtime": {}}
            worker = _WholeRunPersistentWorker(root, 0, {}, [], original)
            worker.activate_pdk_adapter_fallback({
                "activated": True, "adapter_path": adapter,
                "adapter_id": "custom.test", "reason": "explicit_custom_fallback",
            })
            self.assertEqual(worker.config["cadence"]["pdk_adapter"], adapter)
            self.assertEqual(original["cadence"]["pdk_adapter"], "generic-cdf")

    def test_context_identity_records_policy_and_adapter_hash_path_independently(self):
        with tempfile.TemporaryDirectory() as root:
            first = os.path.join(root, "first.il")
            second = os.path.join(root, "second.il")
            for path in (first, second):
                with open(path, "w") as stream:
                    stream.write("procedure(crescPdkApplyParameter(inst name value) t)\n")

            def prepared(path):
                return {"eda_context": {
                    "cadence": {"version_checks": []},
                    "pdk": {
                        "ok": True, "pdk_name": "PDK", "pdk_adapter": "generic-cdf",
                        "pdk_adapter_policy": {"mode": "custom", "path": path},
                        "candidate_libraries": ["PDK"],
                        "contexts": [{
                            "id": "ctx", "name": "PDK", "profile_id": "profile",
                            "reference_libraries": ["PDK"], "technology_libraries": [],
                            "adapter_policy": {"mode": "custom", "path": path},
                            "adapter_selection": {
                                "adapter_id": "generic-cdf", "identity": {
                                    "adapter_id": "generic-cdf", "version": "1",
                                    "manifest_sha256": "generic-manifest",
                                },
                            },
                        }],
                    },
                }}

            first_identity = _prepared_eda_context_identity(prepared(first))
            second_identity = _prepared_eda_context_identity(prepared(second))
            self.assertEqual(first_identity, second_identity)
            context = first_identity["pdk"]["contexts"][0]
            self.assertEqual(context["policy"]["mode"], "custom")
            self.assertIn("custom_adapter_sha256", context["policy"])
            self.assertEqual(context["adapter_id"], "generic-cdf")


if __name__ == "__main__":
    unittest.main()
