from __future__ import print_function

import copy
import json
import os
import tempfile
import unittest

from crescendo_engine.pdk import (
    ADAPTER_ABI,
    GENERIC_ADAPTER_ID,
    SCHEMA_ADAPTER_MANIFEST,
    adapter_identity,
    adapter_policy_options,
    adapter_registry,
    adapter_registry_identity,
    build_adapter_evidence,
    classify_adapter_failure,
    discover_pdk_contexts,
    hierarchy_library_references,
    load_adapter_manifest,
    match_adapter,
    normalize_adapter_policy,
    normalize_pdk_configuration,
    normalize_pdk_profiles,
    parse_cds_lib,
    select_adapter,
    validate_adapter_manifest,
)


class CdsLibraryMapTests(unittest.TestCase):
    def _mkdir(self, root, relative):
        path = os.path.join(root, relative)
        os.makedirs(path)
        return path

    def test_recursive_include_relative_paths_and_last_definition_win(self):
        with tempfile.TemporaryDirectory() as root:
            first = self._mkdir(root, "vendor/first")
            second = self._mkdir(root, "vendor/second with space")
            nested = os.path.join(root, "vendor", "libraries.lib")
            with open(nested, "w") as stream:
                stream.write("DEFINE DEVICE ./first\n")
            top = os.path.join(root, "cds.lib")
            with open(top, "w") as stream:
                stream.write('INCLUDE "vendor/libraries.lib"\n')
                stream.write('DEFINE DEVICE "vendor/second with space" # override\n')
            parsed = parse_cds_lib(top)
            self.assertTrue(parsed["ok"])
            self.assertEqual(parsed["definitions"]["DEVICE"], second)
            self.assertEqual(len(parsed["definition_records"]), 2)
            self.assertEqual(parsed["active_definitions"]["DEVICE"]["overrides"], 0)
            self.assertEqual(parsed["files"], [os.path.realpath(top), os.path.realpath(nested)])
            self.assertTrue(os.path.isdir(first))

    def test_include_precedence_is_evaluated_at_include_position(self):
        with tempfile.TemporaryDirectory() as root:
            before = self._mkdir(root, "before")
            included = self._mkdir(root, "included")
            after = self._mkdir(root, "after")
            child = os.path.join(root, "child.lib")
            with open(child, "w") as stream:
                stream.write("DEFINE PDK ./included\n")
            top = os.path.join(root, "cds.lib")
            with open(top, "w") as stream:
                stream.write("DEFINE PDK ./before\nINCLUDE child.lib\nDEFINE PDK ./after\n")
            parsed = parse_cds_lib(top)
            self.assertEqual(parsed["definitions"]["PDK"], after)
            self.assertEqual(
                [row["path"] for row in parsed["definition_records"]],
                [before, included, after],
            )

    def test_softinclude_warns_but_include_blocks(self):
        with tempfile.TemporaryDirectory() as root:
            soft = os.path.join(root, "soft.lib")
            with open(soft, "w") as stream:
                stream.write("SOFTINCLUDE absent.lib\n")
            parsed_soft = parse_cds_lib(soft)
            self.assertTrue(parsed_soft["ok"])
            self.assertEqual(parsed_soft["warnings"][0]["code"], "softinclude_missing")

            hard = os.path.join(root, "hard.lib")
            with open(hard, "w") as stream:
                stream.write("INCLUDE absent.lib\n")
            parsed_hard = parse_cds_lib(hard)
            self.assertFalse(parsed_hard["ok"])
            self.assertEqual(parsed_hard["errors"][0]["code"], "include_missing")

    def test_cycle_is_reported_without_recursing_forever(self):
        with tempfile.TemporaryDirectory() as root:
            first = os.path.join(root, "first.lib")
            second = os.path.join(root, "second.lib")
            with open(first, "w") as stream:
                stream.write("INCLUDE second.lib\n")
            with open(second, "w") as stream:
                stream.write("INCLUDE first.lib\n")
            parsed = parse_cds_lib(first)
            self.assertFalse(parsed["ok"])
            self.assertEqual(len(parsed["cycles"]), 1)
            self.assertEqual(parsed["errors"][0]["code"], "include_cycle")

    def test_cadence_and_shell_environment_syntaxes_expand(self):
        with tempfile.TemporaryDirectory() as root:
            for name in ("a", "b", "c"):
                self._mkdir(root, name)
            cds = os.path.join(root, "cds.lib")
            with open(cds, "w") as stream:
                stream.write("DEFINE A $(ROOT)/a\n")
                stream.write("DEFINE B ${ROOT}/b\n")
                stream.write("DEFINE C $ROOT/c\n")
                stream.write("DEFINE D $ABSENT/d\n")
            parsed = parse_cds_lib(cds, environment={"ROOT": root})
            self.assertEqual(parsed["definitions"]["A"], os.path.join(root, "a"))
            self.assertEqual(parsed["definitions"]["B"], os.path.join(root, "b"))
            self.assertEqual(parsed["definitions"]["C"], os.path.join(root, "c"))
            self.assertEqual(parsed["unresolved_variables"], ["ABSENT"])


class PdkContextTests(unittest.TestCase):
    def _fixture(self, root):
        paths = {}
        for relative in (
                "design", "vendor/package/models", "vendor/package/device_a",
                "vendor/package/device_b", "vendor/package/tech"):
            path = os.path.join(root, relative)
            os.makedirs(path)
            paths[relative] = path
        paths["vendor"] = os.path.join(root, "vendor")
        paths["vendor/package"] = os.path.join(root, "vendor", "package")
        cds = os.path.join(root, "cds.lib")
        with open(cds, "w") as stream:
            stream.write("DEFINE DESIGN ./design\n")
            stream.write("DEFINE DEV_A ./vendor/package/device_a\n")
            stream.write("DEFINE DEV_B ./vendor/package/device_b\n")
            stream.write("DEFINE TECH ./vendor/package/tech\n")
        return cds, paths

    def test_profile_groups_multiple_reference_and_technology_libraries(self):
        with tempfile.TemporaryDirectory() as root:
            cds, paths = self._fixture(root)
            result = discover_pdk_contexts(
                cds,
                ["DESIGN", "analogLib", "DEV_A", "DEV_B"],
                design_libraries=["DESIGN"],
                technology_libraries=["TECH"],
                profiles=[{
                    "name": "Portable PDK",
                    "package_root": paths["vendor/package"],
                    "reference_libraries": ["DEV_A", "DEV_B"],
                    "technology_libraries": ["TECH"],
                }],
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["candidate_libraries"], ["DEV_A", "DEV_B", "TECH"])
            self.assertEqual(len(result["contexts"]), 1)
            context = result["contexts"][0]
            self.assertEqual(context["name"], "Portable PDK")
            self.assertEqual(context["reference_libraries"], ["DEV_A", "DEV_B", "TECH"])
            self.assertEqual(context["technology_libraries"], ["TECH"])
            self.assertEqual(context["package_root"], paths["vendor/package"])

    def test_automatic_package_markers_group_related_libraries(self):
        with tempfile.TemporaryDirectory() as root:
            cds, paths = self._fixture(root)
            result = discover_pdk_contexts(cds, ["DEV_A", "DEV_B"])
            self.assertTrue(result["ok"])
            self.assertEqual(len(result["contexts"]), 1)
            self.assertEqual(result["contexts"][0]["package_root"], paths["vendor/package"])

    def test_profile_identifies_vendor_schematic_without_classifying_shared_user_library(self):
        for profile_by_path in (False, True):
            with self.subTest(profile_by_path=profile_by_path), tempfile.TemporaryDirectory() as root:
                cds, paths = self._fixture(root)
                profile = {"name": "Vendor"}
                if profile_by_path:
                    profile["package_root"] = paths["vendor/package"]
                else:
                    profile["reference_libraries"] = ["DEV_A"]
                result = discover_pdk_contexts(
                    cds, ["DESIGN", "DEV_A"],
                    design_libraries=["DESIGN", "DEV_A"], profiles=[profile],
                )
                self.assertTrue(result["ok"])
                self.assertEqual(result["candidate_libraries"], ["DEV_A"])
                self.assertEqual(result["contexts"][0]["reference_libraries"], ["DEV_A"])

    def test_unresolved_library_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            cds, _paths = self._fixture(root)
            result = discover_pdk_contexts(cds, ["MISSING"])
            self.assertFalse(result["ok"])
            self.assertTrue(result["blocking"])
            self.assertEqual(result["unresolved_libraries"], ["MISSING"])
            self.assertEqual(result["undefined_libraries"], ["MISSING"])

    def test_defined_but_missing_library_directory_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            cds = os.path.join(root, "cds.lib")
            with open(cds, "w") as stream:
                stream.write("DEFINE DEVICE ./missing-device-library\n")
            result = discover_pdk_contexts(cds, ["DEVICE"])
            self.assertFalse(result["ok"])
            self.assertEqual(result["unavailable_libraries"], ["DEVICE"])
            self.assertEqual(result["unresolved_libraries"], ["DEVICE"])

    def test_overlapping_profiles_are_blocking_not_arbitrarily_selected(self):
        with tempfile.TemporaryDirectory() as root:
            cds, paths = self._fixture(root)
            profiles = [
                {"id": "one", "name": "One", "package_root": paths["vendor"]},
                {"id": "two", "name": "Two", "reference_libraries": ["DEV_A"]},
            ]
            result = discover_pdk_contexts(cds, ["DEV_A"], profiles=profiles)
            self.assertFalse(result["ok"])
            self.assertEqual(result["ambiguous_profile_matches"][0]["profile_ids"], ["one", "two"])

    def test_hierarchy_reference_extraction_is_backend_neutral(self):
        hierarchy = {"designs": [{
            "design": {"library": "DESIGN", "cell": "TB"},
            "hierarchy": {
                "technology_library": "TECH",
                "cells": [
                    {"library": "DESIGN", "cell": "TB"},
                    {"library": "SHARED", "cell": "DUT", "tech_library": "TECH"},
                ],
                "instances": [
                    {"master_library": "SHARED"},
                    {"master_library": "DEVICE"},
                    {"master_library": "DEVICE"},
                ],
            },
        }]}
        result = hierarchy_library_references(hierarchy)
        self.assertEqual(result["master_libraries"], ["SHARED", "DEVICE"])
        self.assertEqual(result["technology_libraries"], ["TECH"])
        self.assertEqual(result["design_libraries"], ["DESIGN", "SHARED"])

    def test_technology_probe_records_identify_internal_vendor_schematic_library(self):
        hierarchy = {"designs": [{
            "design": {"library": "DESIGN", "cell": "TB"},
            "hierarchy": {
                "cells": [
                    {"library": "DESIGN", "cell": "TB"},
                    {"library": "SHARED", "cell": "DUT"},
                    {"library": "DEVICE", "cell": "resistor_wrapper"},
                ],
                "technology_bindings": [
                    {"library": "DESIGN", "technology_library": "DEVICE"},
                    {"library": "SHARED", "technology_library": "DEVICE"},
                    {"library": "DEVICE", "technology_library": "DEVICE"},
                ],
                "instances": [{"master_library": "SHARED"}, {"master_library": "DEVICE"}],
            },
        }]}
        result = hierarchy_library_references(hierarchy)
        self.assertEqual(result["master_libraries"], ["SHARED", "DEVICE"])
        self.assertEqual(result["technology_libraries"], ["DEVICE"])
        self.assertEqual(result["design_libraries"], ["DESIGN", "SHARED"])

    def test_technology_owner_that_is_an_actual_design_root_remains_a_design(self):
        hierarchy = {"designs": [{
            "design": {"library": "DESIGN", "cell": "TB"},
            "hierarchy": {
                "cells": [{"library": "DESIGN", "technology_library": "DESIGN"}],
                "technology_bindings": [{"library": "DESIGN", "technology_library": "DESIGN"}],
            },
        }]}
        result = hierarchy_library_references(hierarchy)
        self.assertEqual(result["design_libraries"], ["DESIGN"])
        self.assertEqual(result["technology_libraries"], ["DESIGN"])


class PdkProfileTests(unittest.TestCase):
    def test_legacy_fields_normalize_without_changing_legacy_generic_semantics(self):
        with tempfile.TemporaryDirectory() as root:
            result = normalize_pdk_profiles({"cadence": {
                "pdk_name": "DEVICE",
                "pdk_dir": "pdk",
                "pdk_adapter": "generic-cdf",
            }}, base_dir=root)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["name"], "DEVICE")
            self.assertEqual(result[0]["package_root"], os.path.join(root, "pdk"))
            self.assertEqual(result[0]["reference_libraries"], ["DEVICE"])
            self.assertEqual(result[0]["adapter_policy"], {"mode": "generic-only"})

    def test_new_profiles_take_precedence_over_legacy_fields(self):
        config = {"cadence": {
            "pdk_name": "LEGACY",
            "pdk_profiles": [{"id": "new", "name": "NEW", "reference_libraries": ["DEV"]}],
        }}
        normalized = normalize_pdk_configuration(config)
        self.assertEqual(normalized["cadence"]["pdk_profiles"][0]["id"], "new")
        self.assertEqual(normalized["cadence"]["pdk_profiles"][0]["name"], "NEW")
        self.assertEqual(config["cadence"]["pdk_profiles"][0].get("schema_version"), None)

    def test_legacy_adapter_path_is_resolved_relative_to_config(self):
        with tempfile.TemporaryDirectory() as root:
            profiles = normalize_pdk_profiles({
                "pdk_name": "DEVICE", "pdk_adapter": "adapters/vendor.il",
            }, base_dir=root)
            self.assertEqual(
                profiles[0]["adapter_policy"]["path"],
                os.path.join(root, "adapters", "vendor.il"),
            )

    def test_profile_model_paths_and_startup_extensions_resolve_from_config(self):
        with tempfile.TemporaryDirectory() as root:
            profiles = normalize_pdk_profiles({"cadence": {
                "pdk_profiles": [{
                    "id": "portable-pdk",
                    "name": "DEVICE",
                    "package_root": "pdk",
                    "model_roots": ["pdk/models"],
                    "model_path_remaps": {
                        "/vendor/original/models": "pdk/models",
                    },
                    "startup_extensions": ["skill/pdk_startup.il"],
                }],
            }}, base_dir=root)
            self.assertEqual(len(profiles), 1)
            self.assertEqual(profiles[0]["package_root"], os.path.join(root, "pdk"))
            self.assertEqual(profiles[0]["model_roots"], [os.path.join(root, "pdk", "models")])
            self.assertEqual(profiles[0]["model_path_remaps"], [{
                "source": "/vendor/original/models",
                "destination": os.path.join(root, "pdk", "models"),
            }])
            self.assertEqual(
                profiles[0]["startup_extensions"],
                [os.path.join(root, "skill", "pdk_startup.il")],
            )

    def test_legacy_model_path_remaps_accept_pair_records(self):
        with tempfile.TemporaryDirectory() as root:
            profiles = normalize_pdk_profiles({
                "pdk_name": "DEVICE",
                "model_path_remaps": [["legacy/models", "portable/models"]],
            }, base_dir=root)
            self.assertEqual(profiles[0]["model_path_remaps"], [{
                "source": os.path.join(root, "legacy", "models"),
                "destination": os.path.join(root, "portable", "models"),
            }])


class AdapterRegistryTests(unittest.TestCase):
    def test_options_are_generated_from_registry(self):
        options = adapter_policy_options()
        option_ids = [row["id"] for row in options]
        self.assertEqual(option_ids[:2], ["auto", "generic-only"])
        self.assertIn("allow:builtin.examplekit18-1", option_ids)
        self.assertIn("allow:builtin.examplekit18pas-1", option_ids)
        self.assertEqual(option_ids[-1], "custom")
        availability = dict(
            (row["id"], row["available"])
            for row in options if row["id"].startswith("allow:builtin.")
        )
        self.assertFalse(availability["allow:builtin.examplekit18-1"])
        self.assertTrue(availability["allow:builtin.examplekit18pas-1"])

    def test_vendor_manifest_requires_two_strong_evidence_categories(self):
        manifest = {
            "schema_version": SCHEMA_ADAPTER_MANIFEST,
            "id": "vendor.bad-1", "version": "1", "abi": ADAPTER_ABI,
            "label": "Bad", "implementation": {"type": "generic-delegate"},
            "matcher": {"minimum_strong_matches": 1, "rules": [
                {"field": "library_names", "patterns": ["bad"], "strength": "strong"},
            ]},
        }
        with self.assertRaises(ValueError):
            validate_adapter_manifest(manifest)

    def test_vendor_match_requires_two_independent_fields(self):
        manifest = adapter_registry()["builtin.examplekit18-1"]
        one = match_adapter(manifest, {"library_names": ["examplekit18"]})
        self.assertFalse(one["matched"])
        two = match_adapter(manifest, {
            "library_names": ["examplekit18"],
            "package_markers": ["EXAMPLEKIT18_PDK"],
        })
        self.assertTrue(two["matched"])
        self.assertEqual(two["strong_match_count"], 2)

    def test_generic_first_and_verified_vendor_fallback(self):
        evidence = build_adapter_evidence({
            "reference_libraries": ["examplekit18pas"],
            "technology_libraries": ["examplekit18pas"],
            "package_root": "/portable/EXAMPLEKIT_018_PAS_OA_NATIVE",
        })
        first = select_adapter("auto", evidence)
        self.assertEqual(first["selected"]["id"], GENERIC_ADAPTER_ID)
        self.assertFalse(first["used_fallback"])
        builtin = select_adapter("auto", evidence, "cdf_callback_incompatible")
        self.assertEqual(builtin["selected"]["id"], "builtin.examplekit18pas-1")
        self.assertEqual(builtin["reason"], "verified_vendor_fallback")
        self.assertTrue(builtin["used_fallback"])
        self.assertTrue(os.path.isfile(
            builtin["selected"]["implementation"]["skill_path"]
        ))

        operational = copy.deepcopy(adapter_registry()["builtin.examplekit18pas-1"])
        operational["id"] = "vendor.examplekit18pas-special-1"
        operational["implementation"]["overrides"] = [{"operation": "cdf-update-order"}]
        registry = adapter_registry([operational])
        fallback = select_adapter(
            "allow:vendor.examplekit18pas-special-1", evidence,
            "cdf_callback_incompatible", registry=registry,
        )
        self.assertEqual(fallback["selected"]["id"], "vendor.examplekit18pas-special-1")
        self.assertTrue(fallback["used_fallback"])

    def test_examplekit_pas_builtin_uses_pas_transaction_without_destructive_generic_update(self):
        implementation = adapter_registry()["builtin.examplekit18pas-1"]["implementation"]
        self.assertEqual(implementation["type"], "skill")
        with open(implementation["skill_path"], "r") as stream:
            source = stream.read()
        for name in (
                "PasCdfFormInit", "PasCdfSetValue", "PasCdfCallCallbacks", "PasCdfDone"):
            self.assertIn(name, source)
        self.assertNotIn("cdfUpdateInstParam", source)
        self.assertNotIn("examplekit18_mosCB", source)

    def test_forbidden_failure_never_uses_fallback(self):
        evidence = {
            "library_names": ["examplekit18"],
            "technology_libraries": ["examplekit18"],
        }
        for code in (
                "simulation_nonconvergence", "simulation_timeout", "license_error",
                "model_missing", "technology_missing", "constraint_infeasible",
                "pdk_clamped", "pdk_quantized", "infrastructure_error", "unknown"):
            selected = select_adapter("auto", evidence, code)
            self.assertEqual(selected["selected"]["id"], GENERIC_ADAPTER_ID)
            self.assertFalse(selected["used_fallback"])

    def test_generic_only_preserves_legacy_behavior(self):
        evidence = {
            "library_names": ["examplekit18"],
            "technology_libraries": ["examplekit18"],
        }
        selected = select_adapter("generic-cdf", evidence, "cdf_update_order_incompatible")
        self.assertEqual(selected["reason"], "policy_generic_only")
        self.assertFalse(selected["used_fallback"])

    def test_allowed_fallback_does_not_select_another_vendor(self):
        evidence = {
            "library_names": ["examplekit18pas"],
            "technology_libraries": ["examplekit18pas"],
        }
        selected = select_adapter(
            "allow:builtin.examplekit18-1", evidence, "netlist_effect_unverified",
        )
        self.assertEqual(selected["reason"], "no_verified_fallback_match")
        self.assertFalse(selected["used_fallback"])

    def test_custom_adapter_requires_explicit_path(self):
        failure = "cdf_parameter_mapping_incompatible"
        missing = select_adapter("custom", {}, failure)
        self.assertEqual(missing["reason"], "custom_adapter_missing")
        unreadable = select_adapter("relative/vendor.il", {}, failure)
        self.assertEqual(unreadable["reason"], "custom_adapter_unreadable")
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "vendor.il")
            with open(path, "w") as stream:
                stream.write("procedure(crescPdkApplyParameter() t)\n")
            selected = select_adapter(path, {}, failure)
            self.assertTrue(selected["used_fallback"])
            self.assertEqual(selected["selected"]["implementation"]["type"], "skill")

    def test_failure_classification_is_fail_closed(self):
        self.assertTrue(classify_adapter_failure("cdf-update-api-incompatible")["fallback_eligible"])
        self.assertFalse(classify_adapter_failure("new_unknown_failure")["fallback_eligible"])

    def test_adapter_identity_is_content_bound_and_path_independent(self):
        with tempfile.TemporaryDirectory() as root:
            first = os.path.join(root, "one", "adapter.il")
            second = os.path.join(root, "two", "adapter.il")
            os.makedirs(os.path.dirname(first))
            os.makedirs(os.path.dirname(second))
            for path in (first, second):
                with open(path, "w") as stream:
                    stream.write("procedure(crescPdkApplyParameter() t)\n")
            manifest = copy.deepcopy(adapter_registry()[GENERIC_ADAPTER_ID])
            manifest["implementation"] = {
                "type": "skill", "skill_path": first, "overrides": [],
            }
            first_identity = adapter_identity(manifest)
            manifest["implementation"]["skill_path"] = second
            second_identity = adapter_identity(manifest)
            self.assertEqual(first_identity, second_identity)
            with open(second, "a") as stream:
                stream.write("procedure(changed() t)\n")
            self.assertNotEqual(second_identity, adapter_identity(manifest))

    def test_registry_identity_is_deterministic(self):
        first = adapter_registry_identity()
        second = adapter_registry_identity(adapter_registry())
        self.assertEqual(first, second)
        self.assertEqual(len(first["adapters"]), 4)

    def test_external_manifest_resolves_skill_relative_to_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            skill = os.path.join(root, "adapter.il")
            with open(skill, "w") as stream:
                stream.write("procedure(crescPdkApplyParameter() t)\n")
            manifest_path = os.path.join(root, "adapter.json")
            manifest = {
                "schema_version": SCHEMA_ADAPTER_MANIFEST,
                "id": "vendor.external-1", "version": "1", "abi": ADAPTER_ABI,
                "label": "External", "vendor": "Example",
                "implementation": {
                    "type": "skill", "skill_path": "adapter.il", "overrides": [],
                },
                "matcher": {"minimum_strong_matches": 2, "rules": [
                    {"field": "library_names", "patterns": ["EXAMPLE"], "strength": "strong"},
                    {"field": "package_markers", "patterns": ["EXAMPLE"], "strength": "strong"},
                ]},
            }
            with open(manifest_path, "w") as stream:
                json.dump(manifest, stream)
            loaded = load_adapter_manifest(manifest_path)
            self.assertEqual(loaded["implementation"]["skill_path"], skill)
            self.assertIn("vendor.external-1", adapter_registry([manifest_path]))

    def test_duplicate_custom_adapter_ids_are_rejected(self):
        custom = copy.deepcopy(adapter_registry()["builtin.examplekit18-1"])
        with self.assertRaises(ValueError):
            adapter_registry([custom])

    def test_policy_normalization_supports_all_public_modes(self):
        self.assertEqual(normalize_adapter_policy(None), {"mode": "auto"})
        self.assertEqual(normalize_adapter_policy("generic-cdf"), {"mode": "generic-only"})
        self.assertEqual(
            normalize_adapter_policy("allow:builtin.examplekit18-1"),
            {"mode": "allow", "adapter_ids": ["builtin.examplekit18-1"]},
        )
        self.assertEqual(
            normalize_adapter_policy({"mode": "allow", "adapter_id": "vendor"}),
            {"mode": "allow", "adapter_id": "vendor", "adapter_ids": ["vendor"]},
        )


if __name__ == "__main__":
    unittest.main()
