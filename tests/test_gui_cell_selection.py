from __future__ import print_function

import copy
import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.ade_setup import resolve_setup_locator
from tests.configuration_service_fixture import load_fixture_configuration


class FakeStagedService(object):
    """Exercise the real GUI queue without native probes or simulation."""

    def __init__(self, context):
        self.context = copy.deepcopy(context)
        self.origin_path = context["source"]["setup_path"]
        self.selected_path = self.origin_path
        self.inspections = {}
        self.inspect_calls = []
        self.prepare_cells_calls = []
        self.prepare_calls = []
        self.select_calls = []
        self.config_calls = []
        self.exit_calls = []

    def eda_setup(self):
        return {
            "ok": True, "mode": "auto", "manual": {},
            "effective": {"ocean_bin": "/fixture/ocean"},
            "sources": {"ocean_bin": "invoking Maestro installation"},
        }

    def list_runs(self):
        return []

    def select_setup(self, path):
        self.selected_path = resolve_setup_locator(path)["path"]
        self.select_calls.append(self.selected_path)
        context = copy.deepcopy(self.context) if self.selected_path == self.origin_path else None
        return {"context": context, "setup_path": self.selected_path,
                "mode": "invoking" if context else "manual", "eda_setup": self.eda_setup()}

    @staticmethod
    def _document(path):
        locator = resolve_setup_locator(path)
        library = locator["library_name"]
        design = {"library": library, "cell": "TB", "view": "schematic"}
        scopes = []
        for hierarchy_path, cell in (("/", "TB"), ("/CORE_A", "core"),
                                     ("/CORE_A/BIAS", "bias"), ("/CORE_B", "core"),
                                     ("/MONITOR", "monitor")):
            scopes.append({
                "scope_id": library + ":" + hierarchy_path,
                "design": copy.deepcopy(design), "hierarchy_path": hierarchy_path,
                "library": library if hierarchy_path == "/" else "block_lib",
                "cell": cell, "view": "schematic",
                "depth": len([part for part in hierarchy_path.split("/") if part]),
            })
        return {
            "setup_path": locator["path"], "setup_locator": locator,
            "setup": {"backend": "maestro", "tests": [dict(design, name="VCO")],
                      "corners": [], "analysis_names": ["tran"]},
            "hierarchy": {"designs": []}, "cell_scopes": scopes,
            "eda_context": {"pdk": {"pdk_name": "fixturePDK"}},
        }

    def inspect_cells(self, path, **options):
        preview = self._document(path)
        if preview["setup_path"] != self.selected_path:
            raise AssertionError("GUI and service disagree about the selected setup")
        self.inspect_calls.append((preview["setup_path"], copy.deepcopy(options)))
        inspection_id = "inspection-" + preview["setup_locator"]["library_name"]
        self.inspections[inspection_id] = copy.deepcopy(preview)
        return inspection_id, preview

    @staticmethod
    def _prepared(preview, scopes=None):
        scope_paths = [row["hierarchy_path"] for row in (scopes or [])]
        parameters = []
        for owner in ("/Mroot", "/CORE_A/M0", "/CORE_A/BIAS/M1", "/CORE_B/M0", "/MONITOR/M0"):
            if scopes is not None and not any(
                    scope == "/" or owner == scope or owner.startswith(scope.rstrip("/") + "/")
                    for scope in scope_paths):
                continue
            parameters.append({
                "parameter_id": owner + "/w", "name": "w", "prompt": "Width",
                "semantic": "width", "optimizable": True, "hierarchy_path": owner,
                "instance_name": owner.rsplit("/", 1)[-1],
                "master_library": "fixturePDK", "master_cell": "nmos",
                "numeric_value": 1e-6, "current_value": "1u", "value_type": "float",
            })
        prepared = copy.deepcopy(preview)
        prepared.pop("cell_scopes", None)
        prepared.update({"parameters": parameters, "outputs": [{"key": "frequency"}],
                         "resident_validation": {"state": "deferred"}})
        if scopes is not None:
            prepared["cell_selection"] = {
                "scope_ids": [row["scope_id"] for row in scopes], "scopes": copy.deepcopy(scopes),
            }
        return prepared

    def prepare_cells(self, inspection_id, scope_ids):
        self.prepare_cells_calls.append((inspection_id, list(scope_ids)))
        preview = self.inspections[inspection_id]
        scopes = [row for row in preview["cell_scopes"] if row["scope_id"] in scope_ids]
        return "prepared-" + preview["setup_locator"]["library_name"], self._prepared(preview, scopes)

    def prepare(self, path, **options):
        self.prepare_calls.append((path, copy.deepcopy(options)))
        preview = self._document(path)
        return "prepared-full-" + preview["setup_locator"]["library_name"], self._prepared(preview)

    def load_design_config(self, document):
        self.config_calls.append(copy.deepcopy(document))
        prepared_id, prepared = self.prepare(document["ade_setup"]["path"], initialize_validation=False)
        return {"prepared_id": prepared_id, "prepared": prepared, "resolution": {"missing": {}}}

    def load_configuration(self, document, path=None, selected_scope_ids=None, selected_test=None):
        self.config_calls.append(copy.deepcopy(document))
        return load_fixture_configuration(self, document, path=path)

    def exit_optimizer(self):
        self.exit_calls.append(True)
        return {"closed": True, "fixture_native_processes": 0}


@unittest.skipUnless(os.environ.get("DISPLAY"), "Staged real Tk tests require an existing DISPLAY")
class GuiCellSelectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-tk-cell-scope-")
        self.addCleanup(self.directory.cleanup)
        self.paths = {}
        for library in ("lib_A", "lib_B"):
            folder = os.path.join(self.directory.name, library, "VCO", "maestro")
            os.makedirs(folder)
            path = os.path.join(folder, "maestro.sdb")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write('<setupdb application="maestro"/>')
            self.paths[library] = path
        self.context = {
            "source": {"library": "lib_A", "cell": "VCO", "view": "maestro",
                       "backend": "maestro", "setup_path": self.paths["lib_A"],
                       "view_dir": os.path.dirname(self.paths["lib_A"]), "modified": False},
            "live_tests": [{"name": "VCO", "library": "lib_A", "cell": "TB", "view": "schematic"}],
            "histories": [],
        }
        try:
            self.root = gui.tk.Tk()
        except gui.tk.TclError as error:
            self.skipTest("Existing DISPLAY cannot initialize Tk: {}".format(error))
        self.root.withdraw()
        self.destroyed = False
        destroy = self.root.destroy

        def tracked_destroy():
            self.destroyed = True
            destroy()

        self.root.destroy = tracked_destroy
        self.app = None
        self.callback_errors = []
        self.addCleanup(self._close)
        errors = mock.patch.object(gui.messagebox, "showerror")
        self.showerror = errors.start()
        self.addCleanup(errors.stop)

    def _close(self):
        if self.app is not None:
            self.app.tasks.close()
            for name in ("worker", "control_worker", "history_worker", "exit_worker"):
                worker = getattr(self.app.tasks, name, None)
                if worker is not None:
                    worker.join(1)
        if not self.destroyed:
            for identifier in self.root.tk.splitlist(self.root.tk.call("after", "info")):
                self.root.after_cancel(identifier)
            self.root.destroy()

    def _wait(self, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            if not self.destroyed:
                self.root.update()
            if self.callback_errors:
                self.fail("Tk callback failed: {}".format(self.callback_errors))
            if predicate():
                return
            time.sleep(0.003)
        self.fail("GUI did not reach the expected staged state")

    def _open(self):
        self.service = FakeStagedService(self.context)
        self.app = gui.OptimizerGui(self.root, self.service, launch_context=self.context)
        self.root.report_callback_exception = (
            lambda exception, value, trace: self.callback_errors.append((exception, value))
        )
        self._wait(lambda: "/fixture/ocean" in self.app.eda_effective_vars["ocean_bin"].get())
        return self.app

    def _load_cells(self, library="lib_A"):
        self.app.setup_path_var.set(self.paths[library])
        self.app.load_setup_button.invoke()
        self._wait(lambda: self.app.cell_inspection_id == "inspection-" + library and not self.app.setup_loading)
        self._wait(lambda: self.app.cell_render_job is None)

    def _toggle(self, path, library="lib_A"):
        scope_id = library + ":" + path
        self.app.cell_scope_tree.selection_set(scope_id)
        self.app._cell_scope_space()
        return scope_id

    def _apply(self, library="lib_A"):
        self.app.apply_cells_button.invoke()
        self._wait(lambda: self.app.prepared_id == "prepared-" + library and not self.app.setup_loading)
        self._wait(lambda: not self.app.parameter_tree_dirty)

    @staticmethod
    def _tree_items(tree, parent=""):
        items = list(tree.get_children(parent))
        for item in list(items):
            items.extend(GuiCellSelectionTests._tree_items(tree, item))
        return items

    def test_load_discovers_cells_without_device_parameters_or_preparation(self):
        app = self._open()
        self.assertEqual(self.service.inspect_calls, [])
        self._load_cells()
        self.assertEqual(len(self.service.inspect_calls), 1)
        self.assertEqual(self.service.prepare_calls, [])
        self.assertEqual(self.service.prepare_cells_calls, [])
        self.assertEqual(len(self._tree_items(app.cell_scope_tree)), 5)
        self.assertEqual(len(app.cell_scopes), 5)
        self.assertEqual(app.selected_cell_scope_ids, set())
        self.assertEqual(app.parameter_configs, {})
        self.assertEqual(app.parameter_by_id, {})
        self.assertEqual(app.parameter_tree.get_children(""), ())
        self.assertIsNone(app.prepared_id)
        self.assertEqual(str(app.start_button.cget("state")), "disabled")
        self.assertEqual(str(app.apply_cells_button.cget("state")), "disabled")

    def test_checkbox_multiselect_applies_only_selected_occurrences_and_subcells(self):
        app = self._open()
        self._load_cells()
        first = self._toggle("/CORE_A")
        second = self._toggle("/CORE_B")
        self.assertEqual(app.cell_scopes[first]["cell"], app.cell_scopes[second]["cell"])
        self.assertNotEqual(first, second)
        self.assertEqual(app.selected_cell_scope_ids, {first, second})
        self.assertTrue(app.cell_scope_tree.item(first, "text").startswith(gui.CHECKED_MARK))
        self.assertTrue(app.cell_scope_tree.item(second, "text").startswith(gui.CHECKED_MARK))
        self.assertEqual(str(app.apply_cells_button.cget("state")), "normal")
        self._apply()
        self.assertEqual(self.service.prepare_cells_calls, [("inspection-lib_A", sorted([first, second]))])
        expected = {"/CORE_A/M0/w", "/CORE_A/BIAS/M1/w", "/CORE_B/M0/w"}
        self.assertEqual(set(app.parameter_by_id), expected)
        self.assertEqual(set(app.parameter_items), expected)
        self.assertEqual(app.applied_cell_scope_ids, {first, second})
        self.assertFalse(app.cell_scope_dirty)
        self.assertEqual(app.current_page, "Parameters")
        self.assertEqual(app.prepared["resident_validation"]["state"], "deferred")
        self.assertEqual(str(app.start_button.cget("state")), "normal")

    def test_parent_and_child_selection_does_not_duplicate_device_parameters(self):
        app = self._open()
        self._load_cells()
        parent = self._toggle("/CORE_A")
        child = self._toggle("/CORE_A/BIAS")
        self._apply()
        self.assertEqual(app.applied_cell_scope_ids, {parent, child})
        self.assertEqual(set(app.parameter_by_id), {"/CORE_A/M0/w", "/CORE_A/BIAS/M1/w"})
        self.assertEqual(len(app.parameter_items), 2)

    def test_empty_cell_selection_cannot_prepare_or_enable_start(self):
        app = self._open()
        self._load_cells()
        app.apply_selected_cells()
        self.assertEqual(self.service.prepare_cells_calls, [])
        self.assertIsNone(app.prepared_id)
        self.assertIn("Select one or more cells", app.status_var.get())
        self.assertEqual(str(app.apply_cells_button.cget("state")), "disabled")
        self.assertEqual(str(app.start_button.cget("state")), "disabled")

    def test_changed_cell_selection_blocks_prepared_actions_until_reapplied(self):
        app = self._open()
        self._load_cells()
        self._toggle("/CORE_A")
        self._apply()
        app.select_all_cells(False)
        self.assertTrue(app.cell_scope_dirty)
        self.assertEqual(str(app.start_button.cget("state")), "disabled")
        self.assertEqual(str(app.apply_cells_button.cget("state")), "disabled")
        self._toggle("/CORE_B")
        self.assertEqual(str(app.apply_cells_button.cget("state")), "normal")
        with self.assertRaises(ValueError):
            app._require_prepared_selection()
        self._apply()
        self.assertEqual(set(app.parameter_by_id), {"/CORE_B/M0/w"})
        self.assertEqual(set(app.parameter_items), {"/CORE_B/M0/w"})
        self.assertFalse(app.cell_scope_dirty)
        self.assertEqual(str(app.start_button.cget("state")), "normal")

    def test_setup_switch_resets_scopes_and_all_prepared_parameters(self):
        app = self._open()
        self._load_cells()
        old_scope = self._toggle("/CORE_A")
        self._apply()
        self._load_cells("lib_B")
        self.assertEqual(self.service.select_calls, [self.paths["lib_A"], self.paths["lib_B"]])
        self.assertIsNone(app.active_launch_context)
        self.assertEqual(app.active_setup_path, self.paths["lib_B"])
        self.assertNotIn(old_scope, app.cell_scopes)
        self.assertEqual(app.selected_cell_scope_ids, set())
        self.assertEqual(app.applied_cell_scope_ids, set())
        self.assertEqual(app.parameter_by_id, {})
        self.assertEqual(app.parameter_tree.get_children(""), ())
        self.assertIsNone(app.prepared_id)
        self.assertEqual(str(app.start_button.cget("state")), "disabled")
        self.assertEqual(str(app.apply_cells_button.cget("state")), "disabled")

    def _exit_during_stage(self, phase):
        app = self._open()
        if phase == "apply":
            self._load_cells()
            self._toggle("/CORE_A")
        method_name = "inspect_cells" if phase == "load" else "prepare_cells"
        callback_name = "_set_cell_inspection" if phase == "load" else "_set_prepared"
        method = getattr(self.service, method_name)
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(3):
                raise RuntimeError("Staged service fixture was not released")
            return method(*args, **kwargs)

        with mock.patch.object(self.service, method_name, side_effect=blocked), \
                mock.patch.object(app, callback_name, wraps=getattr(app, callback_name)) as callback:
            button = app.load_setup_button if phase == "load" else app.apply_cells_button
            button.invoke()
            self.assertTrue(entered.wait(10))
            self.assertTrue(app.setup_loading)
            self.assertEqual(str(app.start_button.cget("state")), "disabled")
            self.assertEqual(str(app.apply_cells_button.cget("state")), "disabled")
            app.request_exit(confirm=False)
            self._wait(lambda: self.destroyed, timeout=1)
            self.assertFalse(release.is_set())
            self.assertEqual(self.service.exit_calls, [True])
            release.set()
            app.tasks.worker.join(1)
            callback.assert_not_called()
        self.assertIsNone(app.prepared_id)
        self.assertFalse(self.showerror.called)

    def test_exit_during_cell_load_does_not_wait_or_apply_late_preview(self):
        self._exit_during_stage("load")

    def test_exit_during_apply_does_not_wait_or_apply_late_parameters(self):
        self._exit_during_stage("apply")

    def test_legacy_configuration_without_test_uses_only_available_test(self):
        app = self._open()
        path = os.path.join(self.directory.name, "legacy-config.json")
        document = {"schema_version": "crescendo.design_config.v1",
                    "ade_setup": {"path": self.paths["lib_A"], "backend": "maestro"},
                    "optimization": {"variables": [], "objectives": [], "constraints": [], "run": {}}}
        with open(path, "w", encoding="utf-8") as stream:
            json.dump(document, stream)
        self.service.fixture_tests = lambda: self.service._document(self.paths['lib_A'])['setup']['tests']
        with mock.patch.object(gui.filedialog, "askopenfilename", return_value=path):
            app.load_design_config()
            self._wait(lambda: not app.setup_loading)
        self.assertEqual(app.test_inspection_id, 'fixture-tests')
        self.assertEqual(app.selected_test, 'VCO')
        self.assertEqual(app.cell_inspection_id, 'inspection-lib_A')
        self.assertIsNone(app.prepared_id)
        self._toggle('/MONITOR'); self._apply()
        self.assertEqual(app.selected_cell_scope_ids, {'lib_A:/MONITOR'})
        self.assertEqual(len(app.parameter_by_id), 1)
        self.assertFalse(app.cell_scope_dirty)

    def test_selected_readonly_devices_remain_visible_without_becoming_variables(self):
        app = self._open()
        self._load_cells()
        self._toggle("/CORE_A")
        app.parameter_view_var.set("name")
        prepare = self.service.prepare_cells

        def readonly_devices(inspection_id, scope_ids):
            prepared_id, prepared = prepare(inspection_id, scope_ids)
            prepared["parameters"] = []
            prepared["hierarchy"] = {"designs": [{
                "design": {"library": "lib_A", "cell": "TB", "view": "schematic"},
                "hierarchy": {
                    "cells": [{"hierarchy_path": "/", "library": "lib_A", "cell": "TB"}],
                    "instances": [
                        {"name": "CORE_A", "hierarchy_path": "/CORE_A", "parent_path": "/",
                         "depth": 1, "kind": "hierarchical", "master_library": "block_lib", "master_cell": "core"},
                        {"name": "MONITOR", "hierarchy_path": "/MONITOR", "parent_path": "/",
                         "depth": 1, "kind": "hierarchical", "master_library": "block_lib", "master_cell": "monitor"},
                        {"name": "Cfixed", "hierarchy_path": "/CORE_A/Cfixed", "parent_path": "/CORE_A",
                         "depth": 2, "kind": "leaf", "master_library": "analogLib", "master_cell": "cap"},
                        {"name": "Coutside", "hierarchy_path": "/MONITOR/Coutside", "parent_path": "/MONITOR",
                         "depth": 2, "kind": "leaf", "master_library": "analogLib", "master_cell": "cap"},
                    ],
                    "parameters": [],
                },
            }]}
            return prepared_id, prepared

        with mock.patch.object(self.service, "prepare_cells", side_effect=readonly_devices):
            self._apply()
        self.assertEqual(app.parameter_view_var.get(), "hierarchy")
        items = self._tree_items(app.parameter_tree)
        labels = [app.parameter_tree.item(item, "text") for item in items]
        selected_device = "node:design-0:/CORE_A/Cfixed"
        self.assertIn(selected_device, items)
        self.assertIn("Cfixed", app.parameter_tree.item(selected_device, "text"))
        self.assertIn("no optimizable parameters", app.parameter_tree.item(selected_device, "text"))
        self.assertNotIn("node:design-0:/MONITOR/Coutside", items)
        self.assertFalse(any("Coutside" in label for label in labels))
        self.assertEqual(app.parameter_by_id, {})
        self.assertEqual(app.parameter_configs, {})
        self.assertEqual(app.parameter_items, {})
        self.assertEqual(app.node_parameters[selected_device], [])
        self.assertEqual(app.prepared["resident_validation"]["state"], "deferred")
        self.assertEqual(self.service.prepare_calls, [])
        self.assertEqual(len(self.service.prepare_cells_calls), 1)
        self.assertEqual(self.service.exit_calls, [])


if __name__ == "__main__":
    unittest.main()
