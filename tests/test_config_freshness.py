import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import hierarchy, runner
from crescendo_engine.ade_setup import inspect_setup
from crescendo_engine.config import DEFAULTS
from crescendo_engine.pdk import hierarchy_library_references
from crescendo_engine.server import OptimizerService


class ConfigFreshnessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.library = os.path.join(self.root, "userLib")
        view = os.path.join(self.library, "TB", "maestro")
        os.makedirs(view)
        self.path = os.path.join(view, "maestro.sdb")
        with open(self.path, "w") as stream:
            stream.write('<setupdb Type="maestro"><active><tests><test Name="T"><tooloptions><option Name="lib"><value>userLib</value></option><option Name="cell"><value>TB</value></option><option Name="view"><value>config</value></option></tooloptions></test></tests></active></setupdb>')
        self.config_files = []
        for cell, config_view in (("TB", "config"), ("NEST", "cfg_custom")):
            directory = os.path.join(self.library, cell, config_view)
            os.makedirs(directory)
            path = os.path.join(directory, "expand.cfg")
            with open(path, "w") as stream:
                stream.write("saved configuration " + cell)
            self.config_files.append(path)
        self.config = copy.deepcopy(DEFAULTS)
        self.config["runtime"]["run_root"] = os.path.join(self.root, "runs")
        mapping = os.path.join(self.root, "cds.lib")
        with open(mapping, "w") as stream:
            stream.write("DEFINE userLib " + self.library + "\n")
        self.config["cadence"]["cds_lib"] = mapping
        self.config["cadence"]["source_cds_lib_override"] = mapping
        self.service = OptimizerService(self.config)
        self.setup = inspect_setup(self.path)
        self.design = {"library": "userLib", "cell": "TB", "view": "config"}
        self.top = {"library": "userLib", "cell": "TB", "view": "schematic"}
        self.scopes = [{"scope_id": "selected", "design": self.design,
                        "hierarchy_path": "/I1", "library": "userLib", "cell": "AMP",
                        "view": "schematic", "depth": 1, "selectable": True}]
        bindings = []
        for name in ("I1", "I2"):
            bindings.append({"hierarchy_path": "/" + name, "name": name, "parent_path": "/",
                             "master_library": "userLib", "master_cell": "AMP", "master_view": "symbol",
                             "owner_library": "userLib", "owner_cell": "TB", "owner_view": "schematic",
                             "bound_library": "userLib", "bound_cell": "AMP", "bound_view": "schematic",
                             "resolved_library": "userLib", "resolved_cell": "AMP", "resolved_view": "schematic",
                             "resolved_view_type": "schematic", "stop": False, "netlister_ignored": False,
                             "fixed_library": True, "fixed_cell": True, "fixed_view": False,
                             "config_context": self.design, "config_context_path": "/", "config_local_path": "/" + name})
        self.body = {"resolved_top": self.top,
                     "config_dependencies": [self.design, {"library": "userLib", "cell": "NEST", "view": "cfg_custom"}],
                     "config_bindings": bindings,
                     "cells": [dict(self.top, hierarchy_path="/"),
                               {"library": "userLib", "cell": "AMP", "view": "schematic", "hierarchy_path": "/I1"}],
                     "instances": copy.deepcopy(bindings), "parameters": []}
        self.hierarchy = {"designs": [{"design": self.design, "hierarchy": self.body}]}

    def tearDown(self):
        self.temp.cleanup()

    def inspect(self):
        with mock.patch.object(hierarchy, "probe_hierarchy", return_value=self.hierarchy), \
                mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes), \
                mock.patch.object(self.service, "_prepare_setup_private_session"):
            # This suite owns binding freshness, not native OA creation. The
            # latter has its own owned-session integration coverage.
            return self.service.inspect_cells(self.path)

    def prepared(self, graph=None):
        prepared = {"setup": self.setup, "hierarchy": graph or self.hierarchy, "parameters": [], "outputs": []}
        from crescendo_engine.workspace import source_design_identity
        prepared["source_identity"] = source_design_identity(prepared["setup"], prepared["hierarchy"], config=self.config)
        return prepared

    def apply(self, token, prepared=None):
        prepared = prepared or self.prepared()
        def register(*args, **kwargs):
            self.service.prepared["new"] = prepared
            return "new", prepared
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes), \
                mock.patch.object(self.service, "_prepare_registered", side_effect=register):
            return self.service.prepare_cells(token, ["selected"])

    def test_unchanged_config_applies_without_retaining_second_full_graph(self):
        token, _preview = self.inspect()
        record = self.service.inspections[token]
        self.assertNotIn("hierarchy", record["preview"])
        self.assertNotIn("config_bindings", repr(record["config_dependencies"]))
        self.assertEqual(len(record["config_identity"]["dependencies"]), 2)
        self.assertEqual(self.apply(token)[0], "new")

    def test_changed_top_or_nested_config_rejected_before_parameter_scan(self):
        for path in self.config_files:
            with self.subTest(path=path):
                token, _preview = self.inspect()
                with open(path, "a") as stream:
                    stream.write(" changed")
                with mock.patch.object(self.service, "_prepare_registered") as prepare:
                    with self.assertRaisesRegex(ValueError, "Config bindings changed since"):
                        self.service.prepare_cells(token, ["selected"])
                    prepare.assert_not_called()

    def test_unselected_binding_change_rejects_and_unregisters_partial_prepare(self):
        token, _preview = self.inspect()
        changed = copy.deepcopy(self.hierarchy)
        changed["designs"][0]["hierarchy"]["config_bindings"][1]["stop"] = True
        with self.assertRaisesRegex(ValueError, "Resolved config hierarchy changed"):
            self.apply(token, self.prepared(changed))
        self.assertNotIn("new", self.service.prepared)

    def test_config_file_change_during_apply_rejects_and_unregisters(self):
        token, _preview = self.inspect()
        prepared = self.prepared()
        def register(*args, **kwargs):
            with open(self.config_files[1], "a") as stream:
                stream.write(" changed during native scan")
            self.service.prepared["new"] = prepared
            return "new", prepared
        with mock.patch.object(hierarchy, "cell_scope_records", return_value=self.scopes), \
                mock.patch.object(self.service, "_prepare_registered", side_effect=register):
            with self.assertRaisesRegex(ValueError, "Config files changed during"):
                self.service.prepare_cells(token, ["selected"])
        self.assertNotIn("new", self.service.prepared)

    def test_semantic_graph_identity_ignores_order_but_protects_each_binding(self):
        before = runner.configuration_binding_identity(self.hierarchy)
        reordered = copy.deepcopy(self.hierarchy)
        body = reordered["designs"][0]["hierarchy"]
        body["config_bindings"].reverse()
        body["config_dependencies"].reverse()
        self.assertEqual(before, runner.configuration_binding_identity(reordered))
        for field, value in (("stop", True), ("fixed_cell", False), ("netlister_ignored", True),
                             ("owner_view", "alternate_schematic"), ("resolved_cell", "OTHER")):
            changed = copy.deepcopy(self.hierarchy)
            changed["designs"][0]["hierarchy"]["config_bindings"][1][field] = value
            self.assertNotEqual(before, runner.configuration_binding_identity(changed), field)

    def test_prepared_fingerprint_protects_config_and_actual_owner_view(self):
        prepared = self.prepared()
        prepared["parameters"] = [{"parameter_id": "p", "optimizable": True,
                                   "owner_view": "schematic", "resolved_top": self.top}]
        before = runner._prepared_schema_fingerprint_v2(prepared)
        changed = copy.deepcopy(prepared)
        changed["parameters"][0]["owner_view"] = "other_schematic"
        self.assertNotEqual(before, runner._prepared_schema_fingerprint_v2(changed))
        changed = copy.deepcopy(prepared)
        changed["hierarchy"]["designs"][0]["hierarchy"]["config_bindings"][1]["stop"] = True
        self.assertNotEqual(before, runner._prepared_schema_fingerprint_v2(changed))

    def test_actual_bound_pdk_and_cross_library_top_are_discovered(self):
        graph = copy.deepcopy(self.hierarchy)
        body = graph["designs"][0]["hierarchy"]
        body["resolved_top"]["library"] = "actualTopLib"
        body["config_bindings"][0]["bound_library"] = "kit_model_lib"
        body["config_bindings"][0]["resolved_library"] = "kit_model_lib"
        references = hierarchy_library_references(graph)
        self.assertIn("actualTopLib", references["design_libraries"])
        self.assertIn("kit_model_lib", references["master_libraries"])

    def test_schematic_prepared_fingerprint_retains_previous_bytes(self):
        # Recorded from the v2 implementation for this portable fixture (stable across releases).
        with open(self.path, "w") as stream:
            stream.write('<setupdb Type="maestro"><active/></setupdb>')
        self.setup = inspect_setup(self.path)  # Exact historical bytes for the v2 compatibility check.
        prepared = self.prepared({"designs": [{"design": self.top, "hierarchy": {
            "cells": self.body["cells"], "instances": [], "parameters": []}}]})
        self.assertEqual("bee9ec5f04c8816f948b8145943eb02243d28db8c92189a13b5c56826e4d37f4",
                         runner._prepared_schema_fingerprint_v2(prepared))


if __name__ == "__main__":
    unittest.main()
