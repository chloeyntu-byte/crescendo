from __future__ import print_function

import os
import tempfile
import unittest

from crescendo_engine.technology import (
    TechnologyBindingError, parse_technology_records, technology_probe_skill,
    validate_workspace_technology, workspace_technology_manifest,
    workspace_technology_validation_required,
)


class TechnologyBindingTest(unittest.TestCase):
    def test_workspace_manifest_requires_validation_for_probed_binding(self):
        hierarchy = {"designs": [{"hierarchy": {"technology_bindings": [{
            "library": "DESIGN", "technology_library": "pdkTech",
            "technology_path": "/source/pdk/tech.db",
        }]}}]}
        manifest = workspace_technology_manifest(
            hierarchy, {"DESIGN": "/private/DESIGN"}, "/private/technology.records",
        )
        self.assertTrue(workspace_technology_validation_required(manifest))
        skill = "\n".join(technology_probe_skill(manifest))
        self.assertIn('ddGetObj("DESIGN")', skill)
        self.assertIn('"pdkTech"', skill)

    def test_unprobed_library_does_not_require_runtime_binding(self):
        manifest = workspace_technology_manifest(
            {}, {"DESIGN": "/private/DESIGN"}, "/private/technology.records",
        )
        self.assertFalse(workspace_technology_validation_required(manifest))

    def test_private_probe_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            actual_tech = os.path.join(root, "actual", "tech.db")
            os.makedirs(os.path.dirname(actual_tech))
            with open(actual_tech, "w") as stream:
                stream.write("tech")
            records = os.path.join(root, "technology.records")
            with open(records, "w") as stream:
                stream.write(
                    'TECH\t"DESIGN"\t"expectedTech"\t"actualTech"\t"%s"\n'
                    % actual_tech
                )
            manifest = {
                "schema_version": "crescendo.workspace_technology.v1",
                "probe_path": records,
                "bindings": [{
                    "library": "DESIGN", "technology_library": "expectedTech",
                    "source_probe_available": True,
                }],
            }
            with self.assertRaisesRegex(TechnologyBindingError, "technology_library_mismatch"):
                validate_workspace_technology(manifest)

    def test_private_probe_matching_binding_is_verified(self):
        with tempfile.TemporaryDirectory() as root:
            tech_file = os.path.join(root, "genericTech", "tech.db")
            os.makedirs(os.path.dirname(tech_file))
            with open(tech_file, "w") as stream:
                stream.write("tech")
            records = os.path.join(root, "technology.records")
            with open(records, "w") as stream:
                stream.write(
                    'TECH\t"DESIGN"\t"genericTech"\t"genericTech"\t"%s"\n'
                    % tech_file
                )
            manifest = {
                "schema_version": "crescendo.workspace_technology.v1",
                "probe_path": records,
                "bindings": [{
                    "library": "DESIGN", "technology_library": "genericTech",
                    "source_probe_available": True,
                }],
            }
            result = validate_workspace_technology(manifest)
            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "verified")

    def test_private_probe_missing_technology_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "technology.records")
            with open(records, "w") as stream:
                stream.write(
                    'TECH\t"DESIGN"\t"genericTech"\t"genericTech"\t"%s"\n'
                    % os.path.join(root, "missing", "tech.db")
                )
            manifest = {
                "schema_version": "crescendo.workspace_technology.v1",
                "probe_path": records,
                "bindings": [{
                    "library": "DESIGN", "technology_library": "genericTech",
                    "source_probe_available": True,
                }],
            }
            with self.assertRaisesRegex(TechnologyBindingError, "technology_file_missing"):
                validate_workspace_technology(manifest)

    def test_private_probe_parser_accepts_nil_binding(self):
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, "technology.records")
            with open(records, "w") as stream:
                stream.write('TECH\t"DIGITAL"\tnil\tnil\tnil\n')
            parsed = parse_technology_records(records)
            self.assertIsNone(parsed["bindings"][0]["technology_library"])


if __name__ == "__main__":
    unittest.main()
