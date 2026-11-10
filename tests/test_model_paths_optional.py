"""Optional automatic model relocation retains unresolved ADE references."""
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.model_paths import (
    AutomaticModelPathResolutionError, ModelPathRemapError,
    configured_model_roots, configured_path_remaps,
    remap_workspace_setup_paths, resolve_relocated_reference,
)


def model(path, text="fixture model\n"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as stream:
        stream.write(text)
    return path


def setup(root, references, escaped_quotes=False):
    private = os.path.join(root, "workspace")
    library = os.path.join(private, "libraries", "D")
    setup_dir = os.path.join(library, "TB", "maestro")
    os.makedirs(setup_dir)
    text = '<Root><Field Name="modelFiles">%s</Field></Root>' % " ".join(
        '"%s"' % reference for reference in references)
    if escaped_quotes:
        text = text.replace('>"', '>&quot;').replace('" "', '&quot; &quot;').replace('"<', '&quot;<')
    source = os.path.join(root, "source.sdb")
    with open(source, "w") as stream:
        stream.write(text)
    os.chmod(source, 0o444)
    setup_sdb = os.path.join(setup_dir, "maestro.sdb")
    with open(setup_sdb, "w") as stream:
        stream.write(text)
    return {
        "root": private, "setup_dir": setup_dir, "setup_sdb": setup_sdb,
        "copied_libraries": {"D": library},
    }, source, text


class OptionalModelPathsTests(unittest.TestCase):
    def test_success_and_missing_reference_are_processed_independently_without_source_writes(self):
        with tempfile.TemporaryDirectory() as root:
            target = model(os.path.join(root, "pdk", "lib", "device.scs"))
            good, missing = "/old/lib/device.scs", "/old/lib/missing.scs"
            workspace, source, before = setup(root, [good, missing])
            source_mode = os.stat(source).st_mode
            report = remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": os.path.join(root, "pdk")}})
            records = {row["original"]: row for row in report["references"]}
            self.assertEqual(records[good]["resolved"], target)
            self.assertTrue(records[good]["changed"])
            self.assertEqual(records[missing]["resolved"], missing)
            self.assertFalse(records[missing]["changed"])
            self.assertEqual(records[missing]["status"], "unresolved")
            self.assertEqual(records[missing]["reason_code"], "missing")
            self.assertEqual(report["limitations"][0]["reference"], missing)
            with open(workspace["setup_sdb"]) as stream:
                self.assertEqual(stream.read(), before.replace(good, target))
            with open(source) as stream:
                self.assertEqual(stream.read(), before)
            self.assertEqual(os.stat(source).st_mode, source_mode)

    def test_ambiguous_reference_keeps_original_without_selecting_either_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            pdk = os.path.join(root, "pdk")
            target = model(os.path.join(pdk, "lib", "good.scs"))
            candidates = [model(os.path.join(pdk, name, "shared.scs")) for name in ("a", "b")]
            good, ambiguous = "/old/lib/good.scs", "/old/shared.scs"
            workspace, _source, before = setup(root, [ambiguous, good])
            report = remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": pdk}})
            row = next(row for row in report["references"] if row["original"] == ambiguous)
            self.assertEqual(row["reason_code"], "ambiguous")
            self.assertEqual(row["resolved"], ambiguous)
            self.assertNotIn(row["resolved"], candidates)
            with open(workspace["setup_sdb"]) as stream:
                self.assertEqual(stream.read(), before.replace(good, target))

    def test_incomplete_search_does_not_accept_partial_match_or_block_valid_sibling(self):
        with tempfile.TemporaryDirectory() as root:
            pdk = os.path.join(root, "pdk")
            target = model(os.path.join(pdk, "lib", "good.scs"))
            partial = model(os.path.join(pdk, "deep", "branch", "shared.scs"))
            good, uncertain = "/old/lib/good.scs", "/old/shared.scs"
            workspace, _source, before = setup(root, [uncertain, good])
            with mock.patch("crescendo_engine.model_paths._bounded_basename_search", return_value=([partial], True)):
                report = remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": pdk}})
            row = next(row for row in report["references"] if row["original"] == uncertain)
            self.assertEqual(row["reason_code"], "search_limited")
            self.assertTrue(row["search_limited"])
            self.assertEqual(row["resolved"], uncertain)
            with open(workspace["setup_sdb"]) as stream:
                self.assertEqual(stream.read(), before.replace(good, target))

    def test_different_unresolved_paths_with_same_basename_have_distinct_fingerprints(self):
        with tempfile.TemporaryDirectory() as root:
            reports = []
            for index, path in enumerate(("/old/one/device.scs", "/old/two/device.scs")):
                directory = os.path.join(root, str(index))
                os.makedirs(directory)
                workspace, _source, before = setup(directory, [path])
                report = remap_workspace_setup_paths(workspace, {})
                self.assertEqual(report["changed_files"], [])
                with open(workspace["setup_sdb"]) as stream:
                    self.assertEqual(stream.read(), before)
                reports.append(report)
            self.assertNotEqual(reports[0]["fingerprint"], reports[1]["fingerprint"])

    def test_unresolved_reason_is_part_of_fingerprint(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, _source, _text = setup(root, ["/old/device.scs"])
            reports = []
            for reason in ("missing", "ambiguous", "search_limited"):
                with mock.patch("crescendo_engine.model_paths.resolve_relocated_reference",
                                side_effect=AutomaticModelPathResolutionError(reason, "optional search did not resolve")):
                    reports.append(remap_workspace_setup_paths(workspace, {}))
            self.assertEqual(len({row["fingerprint"] for row in reports}), 3)

    def test_successful_prefix_is_not_rewritten_inside_an_unresolved_reference(self):
        with tempfile.TemporaryDirectory() as root:
            target = model(os.path.join(root, "pdk", "lib", "a.scs"))
            good, missing = "/old/lib/a.scs", "/old/lib/a.scs.extra"
            workspace, _source, before = setup(root, [good, missing])
            remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": os.path.join(root, "pdk")}})
            with open(workspace["setup_sdb"]) as stream:
                self.assertEqual(stream.read(), before.replace('"%s"' % good, '"%s"' % target))

    def test_xml_escaped_quote_tokens_can_still_be_relocated(self):
        with tempfile.TemporaryDirectory() as root:
            target = model(os.path.join(root, "pdk", "lib", "a.scs"))
            good = "/old/lib/a.scs"
            workspace, _source, before = setup(root, [good], escaped_quotes=True)
            report = remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": os.path.join(root, "pdk")}})
            self.assertEqual(report["changed_files"], [workspace["setup_sdb"]])
            with open(workspace["setup_sdb"]) as stream:
                self.assertEqual(stream.read(), before.replace(good, target))

    def test_success_only_fingerprint_retains_the_existing_identity_format(self):
        with tempfile.TemporaryDirectory() as root:
            model(os.path.join(root, "pdk", "lib", "a.scs"))
            workspace, _source, _text = setup(root, ["/old/lib/a.scs"])
            report = remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": os.path.join(root, "pdk")}})
            rows = [dict((key, row[key]) for key in (
                "role", "directory", "changed", "resolution", "target_identity",
            )) for row in report["references"]]
            expected = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            self.assertEqual(report["fingerprint"], expected)
            self.assertEqual(set(rows[0]["target_identity"]), {"basename", "size", "sha256"})

    def test_explicit_conflicting_remaps_remain_fatal(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, _source, _text = setup(root, ["/old/device.scs"])
            remaps = [{"source": "/old", "destination": os.path.join(root, name)} for name in ("one", "two")]
            with self.assertRaisesRegex(ModelPathRemapError, "conflicting"):
                remap_workspace_setup_paths(workspace, {"cadence": {"model_path_remaps": remaps}})

    def test_explicit_unavailable_remap_target_remains_fatal(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, _source, _text = setup(root, ["/old/device.scs"])
            with self.assertRaisesRegex(ModelPathRemapError, "explicit.*unavailable"):
                remap_workspace_setup_paths(workspace, {"cadence": {
                    "model_path_remaps": [{"source": "/old", "destination": os.path.join(root, "missing")}],
                }})

    def test_setupdb_parse_failure_and_private_containment_remain_fatal(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, source, _text = setup(root, ["/old/device.scs"])
            with open(workspace["setup_sdb"], "w") as stream:
                stream.write("invalid SetupDB")
            with self.assertRaisesRegex(ModelPathRemapError, "SetupDB cannot be parsed"):
                remap_workspace_setup_paths(workspace, {})
            workspace["setup_sdb"] = source
            with self.assertRaisesRegex(ValueError, "escapes safe root"):
                remap_workspace_setup_paths(workspace, {})

    def test_unclassified_resolver_error_is_not_swallowed(self):
        with tempfile.TemporaryDirectory() as root:
            workspace, _source, _text = setup(root, ["/old/device.scs"])
            with mock.patch("crescendo_engine.model_paths.resolve_relocated_reference",
                            side_effect=RuntimeError("unexpected resolver failure")):
                with self.assertRaisesRegex(RuntimeError, "unexpected resolver failure"):
                    remap_workspace_setup_paths(workspace, {})

    def test_public_resolution_function_remains_strict_for_optional_auto_failures(self):
        with self.assertRaises(AutomaticModelPathResolutionError) as raised:
            resolve_relocated_reference("/missing/fixture-for-crescendo/device.scs", [])
        self.assertEqual(raised.exception.reason_code, "missing")
        self.assertIsInstance(raised.exception, ModelPathRemapError)

    def test_excluded_ambiguous_profiles_preserve_other_profile_and_top_explicit_remap(self):
        config = {"cadence": {
            "model_roots": ["/manual/models"],
            "model_path_remaps": [{"source": "/old/top", "destination": "/new/top"}],
            "pdk_profiles": [
                {"name": "PDK", "pdk_dir": "/excluded/one", "model_path_remaps": [["/old/profile", "/new/one"]]},
                {"name": "PDK", "pdk_dir": "/excluded/two", "model_path_remaps": [["/old/profile", "/new/two"]]},
                {"name": "OTHER", "pdk_dir": "/kept/profile", "model_path_remaps": [["/old/other", "/new/other"]]},
            ],
        }}
        context = {"discovery_degraded": True, "excluded_profile_indices": [0, 1],
                   "candidate_libraries": ["PDK", "OTHER"]}
        self.assertEqual(configured_model_roots(config, context), ["/manual/models", "/kept/profile"])
        self.assertEqual(set(configured_path_remaps(config, context)), {
            ("/old/top", "/new/top"), ("/old/other", "/new/other"),
        })

    def test_aggregate_context_honors_precise_excluded_profile_indices(self):
        config = {"cadence": {"pdk_profiles": [
            {"name": "PDK", "pdk_dir": "/excluded"},
            {"name": "PDK", "pdk_dir": "/kept"},
        ]}}
        context = {"contexts": [{"name": "PDK", "excluded_profile_indices": [0]}]}
        self.assertEqual(configured_model_roots(config, context), ["/kept"])


if __name__ == "__main__":
    unittest.main()
