"""Source guard attribution without changing electrical qualification rules."""
import copy
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as sessions
from crescendo_engine.prepared_freshness import verify_prepared_freshness
from crescendo_engine.workspace import (
    SOURCE_SCOPE_DIAGNOSTIC_LIMIT, SOURCE_FILE_DIAGNOSTIC_LIMIT, _content_snapshots_equal,
    compact_source_guard, source_change_summary, source_scope_changes,
    tree_content_fingerprint, verify_sources_unchanged,
)
from tests import test_setup_baseline as baseline_fixtures


class SourceScopeDiagnosticTests(unittest.TestCase):
    def write(self, root, path, value):
        target = os.path.join(root, path)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w") as stream:
            stream.write(value)
        return target

    def workspace(self, root, paths):
        return {"source_libraries": {"design": root},
                "source_snapshots": {"design": tree_content_fingerprint(root, paths)}}

    def test_sidecar_preserves_original_hash_bytes_and_reads_each_file_once(self):
        with tempfile.TemporaryDirectory() as root:
            first = self.write(root, "a", "one")
            second = self.write(root, "TB/schematic/sch.oa", "two")
            rows = ("SCOPE\x1fa\n", "F\x1fa\x1f3\x1f%s\n" % hashlib.sha256(b"one").hexdigest(),
                    "SCOPE\x1fTB/schematic\n",  # scopes retain lexicographic order
                    "F\x1fTB/schematic/sch.oa\x1f3\x1f%s\n" % hashlib.sha256(b"two").hexdigest())
            digest = hashlib.sha256()
            # Uppercase TB sorts before lowercase a, as in the original code.
            for row in rows[2:] + rows[:2]:
                digest.update(row.encode("utf-8"))
            with mock.patch("builtins.open", wraps=open) as opened:
                result = tree_content_fingerprint(root, ["a", "TB/schematic"])
            self.assertEqual(result["fingerprint"], digest.hexdigest())
            self.assertEqual(result["schema_version"], "crescendo.tree_content_fingerprint.v1")
            self.assertEqual(sorted(call[0][0] for call in opened.call_args_list), sorted([first, second]))
            altered = copy.deepcopy(result)
            altered["scope_diagnostics"] = {}
            self.assertTrue(_content_snapshots_equal(result, altered))

    def test_changed_scopes_distinguish_metadata_config_schematic_and_opaque(self):
        with tempfile.TemporaryDirectory() as root:
            scopes = ["data.dm", "TB/data.dm", "TB/config", "TB/schematic", "Leaf/veriloga"]
            files = ["data.dm", "TB/data.dm", "TB/config/binding", "TB/schematic/sch.oa", "Leaf/veriloga/veriloga.va"]
            for path in files:
                self.write(root, path, "before")
            workspace = self.workspace(root, scopes)
            for path in files:
                self.write(root, path, "after")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            changes = proof["libraries"][0]["changes"]
            by_path = dict((row["path"], row["category"]) for row in changes["scopes"])
            self.assertEqual(by_path, {"data.dm": "library_metadata", "TB/data.dm": "cell_metadata",
                "TB/config": "config_view", "TB/schematic": "schematic_view", "Leaf/veriloga": "opaque_view"})
            self.assertEqual(changes["unexamined_scopes"], 0)
            self.assertIn("data.dm", str(proof))
            self.assertNotIn("source schematic changed", source_change_summary(proof))

    def test_native_binding_evidence_classifies_custom_schematic_and_opaque_names(self):
        with tempfile.TemporaryDirectory() as root:
            scopes = ["TB/schematic_abc", "Leaf/ext_abc"]
            for path in scopes:
                self.write(root, path + "/content", "before")
            workspace = self.workspace(root, scopes)
            workspace["config_designs"] = [{"config_bindings": [
                {"resolved_library": "design", "resolved_cell": "TB", "resolved_view": "schematic_abc", "scan_traversable": True},
                {"resolved_library": "design", "resolved_cell": "Leaf", "resolved_view": "ext_abc", "stop": True}]}]
            for path in scopes:
                self.write(root, path + "/content", "after")
            changes = verify_sources_unchanged(workspace)["libraries"][0]["changes"]
            self.assertEqual(dict((row["path"], row["category"]) for row in changes["scopes"]),
                {"TB/schematic_abc": "schematic_view", "Leaf/ext_abc": "opaque_view"})

    def test_old_or_malformed_scope_metadata_never_weakens_rejection(self):
        with tempfile.TemporaryDirectory() as root:
            self.write(root, "data.dm", "before")
            workspace = self.workspace(root, ["data.dm"])
            workspace["source_snapshots"]["design"].pop("scope_diagnostics")
            self.write(root, "data.dm", "after")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            self.assertEqual(proof["libraries"][0]["changes"]["state"], "scope_details_unavailable")
            self.assertIn("exact changed scope unavailable", source_change_summary(proof))
            before = tree_content_fingerprint(root, ["data.dm"])
            self.write(root, "data.dm", "different")
            after = tree_content_fingerprint(root, ["data.dm"])
            before["scope_diagnostics"]["scopes"][0]["fingerprint"] = "malformed"
            self.assertEqual(source_scope_changes(before, after, "design")["state"], "scope_details_unavailable")
            for malformed in ("not a mapping", {"scopes": [{"path": {"bad": "path"}}]}):
                before["scope_diagnostics"] = malformed
                self.assertEqual(source_scope_changes(before, after, "design")["state"], "scope_details_unavailable")

    def test_deleted_property_bag_remains_metadata_and_unknown_files_are_not_guessed(self):
        with tempfile.TemporaryDirectory() as root:
            paths = ["data.dm", "model.scs", "TB/unknown.file"]
            for path in paths:
                self.write(root, path, "before")
            workspace = self.workspace(root, paths)
            os.unlink(os.path.join(root, "data.dm"))
            for path in paths[1:]:
                self.write(root, path, "after")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            self.assertEqual(dict((row["path"], row["category"]) for row in proof["libraries"][0]["changes"]["scopes"]),
                {"data.dm": "library_metadata", "model.scs": "library_content", "TB/unknown.file": "cell_content"})

    def test_scope_budget_reports_unknown_tail_without_losing_guard(self):
        with tempfile.TemporaryDirectory() as root:
            paths = ["file_%03d" % n for n in range(SOURCE_SCOPE_DIAGNOSTIC_LIMIT + 4)]
            for path in paths:
                self.write(root, path, "before")
            workspace = self.workspace(root, paths)
            self.write(root, paths[-1], "after")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            self.assertEqual(len(workspace["source_snapshots"]["design"]["scope_diagnostics"]["scopes"]), SOURCE_SCOPE_DIAGNOSTIC_LIMIT)
            self.assertEqual(proof["libraries"][0]["changes"]["unexamined_scopes"], 4)
            self.assertEqual(proof["libraries"][0]["changes"]["scopes"], [])

    def test_library_bag_is_retained_after_large_cellview_catalogue(self):
        with tempfile.TemporaryDirectory() as root:
            paths = ["CELL_%03d/schematic" % n for n in range(SOURCE_SCOPE_DIAGNOSTIC_LIMIT + 4)]
            for path in paths:
                self.write(root, path + "/sch.oa", "before")
            self.write(root, "data.dm", "before")
            workspace = self.workspace(root, paths + ["data.dm"])
            self.write(root, "data.dm", "after")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            changes = proof["libraries"][0]["changes"]
            self.assertEqual([(row["path"], row["category"]) for row in changes["scopes"]],
                             [("data.dm", "library_metadata")])

    def test_summary_is_bounded_for_many_libraries_and_long_paths(self):
        proof = {"libraries": [{"library": "lib_%d" % n, "unchanged": False,
            "changes": {"scopes": [{"path": "x" * 1000, "label": "Schematic"}] * 12}}
            for n in range(20)]}
        self.assertLessEqual(len(source_change_summary(proof)), 2200)

    def test_file_evidence_identifies_content_change_without_timestamp_guess(self):
        with tempfile.TemporaryDirectory() as root:
            path = self.write(root, "TB/schematic/sch.oa", "before")
            self.write(root, "TB/schematic/master.tag", "unchanged")
            workspace = self.workspace(root, ["TB/schematic"])
            stamp = os.stat(path).st_mtime
            self.write(root, "TB/schematic/sch.oa", "after!")
            os.utime(path, (stamp, stamp))
            self.write(root, "TB/schematic/sch.oa.cdslck", "native lock")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            changes = proof["libraries"][0]["changes"]["scopes"][0]["file_changes"]
            self.assertEqual([(row["path"], row["state"]) for row in changes["files"]],
                             [("TB/schematic/sch.oa", "modified")])
            row = changes["files"][0]
            self.assertEqual(row["before"], {"bytes": 6, "sha256": hashlib.sha256(b"before").hexdigest()})
            self.assertEqual(row["after"], {"bytes": 6, "sha256": hashlib.sha256(b"after!").hexdigest()})

    def test_file_addition_and_removal_require_complete_opposite_sample(self):
        with tempfile.TemporaryDirectory() as root:
            self.write(root, "TB/schematic/old.oa", "before")
            workspace = self.workspace(root, ["TB/schematic"])
            os.unlink(os.path.join(root, "TB/schematic/old.oa"))
            self.write(root, "TB/schematic/new.oa", "after")
            proof = verify_sources_unchanged(workspace)
            changes = proof["libraries"][0]["changes"]["scopes"][0]["file_changes"]
            self.assertEqual([(row["path"], row["state"]) for row in changes["files"]],
                             [("TB/schematic/new.oa", "added"), ("TB/schematic/old.oa", "removed")])
            for index in range(SOURCE_FILE_DIAGNOSTIC_LIMIT + 4):
                self.write(root, "TB/schematic/file_%03d" % index, "unchanged")
            workspace = self.workspace(root, ["TB/schematic"])
            self.write(root, "TB/schematic/aaa", "new leading name displaces sample tail")
            proof = verify_sources_unchanged(workspace)
            changes = proof["libraries"][0]["changes"]["scopes"][0]["file_changes"]
            self.assertFalse(changes["files"])
            self.assertGreater(changes["unexamined_before_files"], 0)
            self.assertGreater(changes["unexamined_after_files"], 0)
            self.assertFalse(proof["ok"])

    def test_old_file_evidence_is_unknown_without_weakening_scope_guard(self):
        with tempfile.TemporaryDirectory() as root:
            self.write(root, "TB/schematic/sch.oa", "before")
            workspace = self.workspace(root, ["TB/schematic"])
            for row in workspace["source_snapshots"]["design"]["scope_diagnostics"]["scopes"]:
                row.pop("files")
            self.write(root, "TB/schematic/sch.oa", "after")
            proof = verify_sources_unchanged(workspace)
            self.assertFalse(proof["ok"])
            scope = proof["libraries"][0]["changes"]["scopes"][0]
            self.assertEqual(scope["file_changes"]["state"], "file_details_unavailable")
            self.assertIn("TB/schematic", source_change_summary(proof))

    def test_compact_failure_drops_whole_catalogue_keeps_changed_file_proof(self):
        with tempfile.TemporaryDirectory() as root:
            self.write(root, "TB/schematic/sch.oa", "before")
            workspace = self.workspace(root, ["TB/schematic"])
            self.write(root, "TB/schematic/sch.oa", "after")
            proof = verify_sources_unchanged(workspace)
            # Reproduce the large worker_shutdown catalogue without reading it.
            for side in ("before", "after"):
                proof["libraries"][0][side]["paths"] = ["unused_scope_" + "x" * 200] * 40000
            compact = compact_source_guard(proof)
            self.assertFalse(compact["ok"])
            self.assertLess(len(json.dumps(compact)), 4096)
            self.assertEqual(compact["libraries"][0]["before"]["scope_count"], 40000)
            self.assertNotIn("paths", compact["libraries"][0]["before"])
            self.assertEqual(compact["libraries"][0]["changes"]["scopes"][0]["file_changes"]["files"][0]["path"],
                             "TB/schematic/sch.oa")
            self.assertIn("paths", proof["libraries"][0]["before"])

    def test_compact_failure_reports_omitted_changed_libraries_and_details(self):
        changes = {"scopes": [{"path": "x" * 50000, "label": "Schematic"}]}
        proof = {"ok": False, "libraries": [
            {"library": "lib_%d" % index, "unchanged": False, "changes": changes}
            for index in range(20)]}
        compact = compact_source_guard(proof)
        self.assertEqual(compact["changed_libraries"], 20)
        self.assertEqual(compact["omitted_changed_libraries"], 12)
        self.assertEqual(compact["libraries"][0]["changes"]["state"], "scope_details_omitted")
        self.assertLess(len(json.dumps(compact)), 50 * 1024)


class PreparedSourceAttributionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = baseline_fixtures.SetupBaselineTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.metadata = os.path.join(self.fixture.library, "data.dm")
        self.fixture.write(self.metadata, "technology and library properties")
        self.fixture.freeze()
        self.prepared, self.config = self.fixture.prepared, self.fixture.config

    def attach_owned_snapshots(self):
        controller = sessions._PrivateSession(self.prepared, self.config)
        self.addCleanup(lambda: sessions._SESSIONS.pop(controller.owner["session_id"], None))
        workspace_root = controller.owner["workspace_root"]
        os.makedirs(workspace_root)
        from crescendo_engine.workspace import _source_fingerprint_scopes
        sources = {"design": self.fixture.library}
        paths = _source_fingerprint_scopes(self.prepared["setup"], self.fixture.hierarchy, sources, actual_views=True)
        controller.workspace = {"root": workspace_root, "setup": copy.deepcopy(self.prepared["setup"]),
            "source_libraries": sources, "source_snapshots": {"design": tree_content_fingerprint(self.fixture.library, paths["design"])}}
        controller.status = "ready"
        with sessions._REGISTRY_LOCK:
            sessions._SESSIONS[controller.owner["session_id"]] = controller
        self.prepared["setup_private_session"] = controller.info()
        return controller

    def test_original_maestro_save_keeps_baseline_and_does_no_supplemental_read(self):
        self.attach_owned_snapshots()
        self.fixture.write(self.fixture.path, self.fixture.xml.replace('<value>3</value>', '<value>9</value>'))
        self.fixture.write(self.fixture.state, '<state><saved>new</saved></state>')
        with mock.patch("crescendo_engine.prepared_freshness.verify_sources_unchanged", side_effect=AssertionError("no failure-path rescan")):
            result = verify_prepared_freshness(self.prepared, self.config)
        self.assertTrue(result["ok"])
        self.assertIsNone(result["source_changes"])

    def test_owned_session_reports_exact_library_metadata_and_stays_blocked(self):
        self.attach_owned_snapshots()
        self.fixture.write(self.metadata, "changed library properties")
        result = verify_prepared_freshness(self.prepared, self.config)
        self.assertFalse(result["ok"])
        self.assertEqual(result["source_changes"]["state"], "owned_workspace_scopes")
        row = result["source_changes"]["libraries"][0]["changes"]["scopes"][0]
        self.assertEqual((row["path"], row["category"]), ("data.dm", "library_metadata"))
        self.assertIn("design/data.dm", result["reasons"][0])

    def test_no_live_owner_does_not_read_arbitrary_workspace_or_claim_exact_path(self):
        self.prepared["setup_private_session"] = {"root": self.fixture.library}
        self.fixture.write(self.metadata, "changed library properties")
        with mock.patch("crescendo_engine.prepared_freshness.verify_sources_unchanged", side_effect=AssertionError("unowned workspace")):
            result = verify_prepared_freshness(self.prepared, self.config)
        self.assertFalse(result["ok"])
        self.assertEqual(result["source_changes"]["detail_state"], "owned_scope_details_unavailable")
        self.assertIn("exact changed scope unavailable", result["reasons"][0])

    def test_busy_owner_is_not_waited_on_for_failure_diagnostics(self):
        controller = self.attach_owned_snapshots()
        self.fixture.write(self.fixture.schematic, "changed electrical view")
        controller.lock.acquire()
        try:
            result = verify_prepared_freshness(self.prepared, self.config)
        finally:
            controller.lock.release()
        self.assertFalse(result["ok"])
        self.assertEqual(result["source_changes"]["detail_state"], "owned_workspace_busy")


if __name__ == "__main__":
    unittest.main()
