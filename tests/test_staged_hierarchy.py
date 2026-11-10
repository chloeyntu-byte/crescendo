"""Staged read-only hierarchy identities and exact occurrence selection gates."""
import contextlib
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import hierarchy


class StagedHierarchyTests(unittest.TestCase):
    def body(self):
        return {"cells": [
            {"hierarchy_path": "/", "library": "USER", "cell": "TOP", "view": "schematic", "depth": 0},
            {"hierarchy_path": "/I1", "library": "SHARED", "cell": "AMP", "view": "schematic", "depth": 1},
            {"hierarchy_path": "/I10", "library": "SHARED", "cell": "AMP", "view": "schematic", "depth": 1},
            {"hierarchy_path": "/I1/XRF", "library": "FOUNDRY", "cell": "WRAPPER", "view": "schematic", "depth": 2}],
            "instances": [{"hierarchy_path": "/I1", "parent_path": "/", "master_library": "SHARED", "master_cell": "AMP"},
                {"hierarchy_path": "/I10", "parent_path": "/", "master_library": "SHARED", "master_cell": "AMP"},
                {"hierarchy_path": "/I1/XRF", "parent_path": "/I1", "master_library": "FOUNDRY", "master_cell": "WRAPPER"}],
            "technology_bindings": [], "parameters": [], "errors": []}

    def test_scope_ids_keep_design_view_and_repeated_occurrences_distinct(self):
        first = {"library": "USER", "cell": "TOP", "view": "schematic"}
        second = dict(first, view="schematic_alt")
        source = {"designs": [{"design": first, "hierarchy": self.body()},
                              {"design": second, "hierarchy": self.body()}]}
        with mock.patch("crescendo_engine.workspace.resolve_copy_libraries", return_value=("USER", None, {"USER": "/owned", "SHARED": "/shared"})):
            records = hierarchy.cell_scope_records(source, setup={"owned": True}, config={})
            again = hierarchy.cell_scope_records(copy.deepcopy(source), setup={"owned": True}, config={})
        self.assertEqual(records, again)
        self.assertEqual(len({row["scope_id"] for row in records}), 8)
        user_rows = [row for row in records if row["selectable"]]
        self.assertEqual(len(user_rows), 6)
        self.assertEqual({row["hierarchy_path"] for row in user_rows}, {"/", "/I1", "/I10"})
        foundry = [row for row in records if row["library"] == "FOUNDRY"]
        self.assertEqual(len(foundry), 2)
        self.assertTrue(all(not row["selectable"] and row["reason"] for row in foundry))
        self.assertEqual(foundry[0]["master_view"], "schematic")

    def test_empty_ambiguous_and_foreign_scopes_never_mean_full_scan(self):
        designs = [("USER", "TOP", "schematic")]
        for selected in ([], ["AMP"], [{"hierarchy_path": "/I1"}],
                         [{"design": {"library": "OTHER", "cell": "TOP"}, "hierarchy_path": "/I1"}],
                         [{"design": {"library": "USER", "cell": "TOP"}, "hierarchy_path": "I1"}],
                         [{"design": {"library": "USER", "cell": "TOP"}, "hierarchy_path": "/I1//M0"}]):
            with self.subTest(selected=selected), self.assertRaises(ValueError):
                hierarchy._selected_scopes(selected, designs)
        self.assertIsNone(hierarchy._selected_scopes(None, designs))

    def test_selected_scopes_are_deduplicated_without_tying_repeated_cells(self):
        design = {"library": "USER", "cell": "TOP", "view": "schematic"}
        selected = [{"design": design, "hierarchy_path": path} for path in ("/I1", "/I1/", "/I10")]
        self.assertEqual(hierarchy._selected_scopes(selected, [("USER", "TOP", "schematic")]),
                         {("USER", "TOP", "schematic"): ["/I1", "/I10"]})

    def test_config_failure_is_visible_and_never_falls_back_to_schematic(self):
        data = hierarchy.normalize_probe([{"kind": "UNSUPPORTED_CONFIG", "values": ["/", "USER", "TOP", "config"]}])
        with self.assertRaisesRegex(RuntimeError, "will not substitute a schematic"):
            hierarchy.validate_native_hierarchy(data, {"library": "USER", "cell": "TOP", "view": "config"})

    @contextlib.contextmanager
    def fake_probe(self, root):
        library = os.path.join(root, "library")
        os.mkdir(library)
        setup = {"source": {"library_root": library}, "setup_locator": {"library_name": "USER"},
                 "tests": [{"name": "A", "library": "USER", "cell": "TOP", "view": "schematic"}]}
        config = {"cadence": {"virtuoso_bin": "owned-fixture"}, "runtime": {"run_root": os.path.join(root, "runtime")}}
        def command(argv, cwd, **kwargs):
            with open(os.path.join(cwd, "hierarchy.records"), "w") as stream:
                stream.write('CELL\t"/"\t"USER"\t"TOP"\t"schematic"\t0\n'
                             'INSTANCE\t"/I1"\t"I1"\t"USER"\t"AMP"\t"symbol"\tt\t"/"\t0\n'
                             'CELL\t"/I1"\t"USER"\t"AMP"\t"schematic"\t1\n'
                             'INSTANCE\t"/I1/M0"\t"M0"\t"FOUNDRY"\t"MOS"\t"symbol"\tnil\t"/I1"\t1\n')
            return {"returncode": 0}
        with mock.patch.object(hierarchy, "child_environment", return_value={"PATH": os.defpath}), \
             mock.patch.object(hierarchy, "run_command", side_effect=command) as launch, \
             mock.patch.object(hierarchy, "owned_headless_display") as display:
            yield setup, config, launch, display

    def test_cell_only_keeps_leaf_identity_and_starts_no_cdf_display(self):
        with tempfile.TemporaryDirectory() as root, self.fake_probe(root) as (setup, config, launch, display):
            result = hierarchy.probe_hierarchy(setup, config, cells_only=True)
        display.assert_not_called()
        launch.assert_called_once()
        self.assertEqual(result["scan_mode"], "cells_only")
        body = result["designs"][0]["hierarchy"]
        self.assertEqual(body["counts"]["devices"], 1)
        self.assertEqual(body["parameters"], [])
        self.assertEqual(body["instances"][-1]["master_library"], "FOUNDRY")
        self.assertEqual(result["designs"][0]["command"]["headless_display"]["status"], "not_needed")

    def test_missing_cell_occurrence_rejects_after_preserving_evidence(self):
        with tempfile.TemporaryDirectory() as root, self.fake_probe(root) as (setup, config, launch, display):
            # A provided display avoids any real EDA or Xvnc invocation in this fixture.
            @contextlib.contextmanager
            def fake_display(env, workdir):
                yield env, {"owned": False}
            display.side_effect = fake_display
            with self.assertRaisesRegex(ValueError, "was not found in the actual hierarchy"):
                hierarchy.probe_hierarchy(setup, config, selected_cell_paths=[
                    {"design": {"library": "USER", "cell": "TOP", "view": "schematic"}, "hierarchy_path": "/missing"}])
            saved = [os.path.join(directory, "hierarchy.json") for directory, _children, files in os.walk(root) if "hierarchy.json" in files]
            self.assertEqual(len(saved), 1)


if __name__ == "__main__":
    unittest.main()
