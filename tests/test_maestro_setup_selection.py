import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.ade_setup import inspect_setup
from crescendo_engine.cadence import child_environment
from crescendo_engine.config import DEFAULTS
from crescendo_engine.maestro_context import (
    SCHEMA, bind_launch_config, load_launch_context,
)
from crescendo_engine.native_environment import NATIVE_ENV_NAMES, restore_native_environment
from crescendo_engine.server import OptimizerService
from crescendo_engine.workspace import parse_cds_defines


def record(tag, *values):
    return tag + "\t" + "\t".join(
        "nil" if value is None else "t" if value is True else json.dumps(value)
        for value in values
    )


class MaestroSetupSelectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = self.temporary.name
        self.workspace = os.path.join(self.root, "workspace one")
        self.a = self._setup(self.workspace, "physicalA", "LogicalA", "maestro")
        self.b = self._setup(self.workspace, "physicalB", "LogicalB", "maestro")
        self.cds = self._mapping(self.workspace, {
            "LogicalA": os.path.dirname(os.path.dirname(os.path.dirname(self.a))),
            "LogicalB": os.path.dirname(os.path.dirname(os.path.dirname(self.b))),
        })
        self.tools = os.path.join(self.root, "native-tools")
        os.makedirs(self.tools)
        for name in ("virtuoso", "ocean", "spectre"):
            path = os.path.join(self.tools, name)
            with open(path, "w") as stream:
                stream.write("#!/bin/sh\nexit 0\n")
            os.chmod(path, 0o755)
        self.native = dict((name, None) for name in NATIVE_ENV_NAMES)
        self.native.update(PATH=self.tools, LD_LIBRARY_PATH="/native/loader", PYTHONPATH="",
                           TCL_LIBRARY="/native/tcl")
        self.native["CDS_LIC_FILE"] = "27000@native-license"
        rows = [SCHEMA, record("C", "LogicalA", "VCO", "maestro", self.workspace,
                os.path.dirname(self.a), self.cds, os.path.join(self.tools, "ocean")),
                record("M", None), record("T", "VCO", "LogicalA", "TB", "schematic", "spectre"),
                record("H", "original", None, None), record("S", "original")]
        rows += [record("E", name, value) for name, value in self.native.items()]
        rows += [record("L", name, root) for name, root in parse_cds_defines(self.cds).items()]
        context_path = os.path.join(self.root, "native.context")
        with open(context_path, "w") as stream:
            stream.write("\n".join(rows) + "\n")
        with mock.patch.dict(os.environ, {"CRESCENDO_RUN_ROOT": os.path.join(self.root, "runtime")}):
            self.context = load_launch_context(context_path)
        config = copy.deepcopy(DEFAULTS)
        self.config = bind_launch_config(config, self.context)
        self.service = OptimizerService(self.config)

    def _setup(self, workspace, physical, logical, view, name="maestro.sdb"):
        directory = os.path.join(workspace, physical, "VCO", view)
        os.makedirs(directory)
        path = os.path.join(directory, name)
        with open(path, "w") as stream:
            stream.write('<setupdb><active><tests><test Name="VCO"><tooloptions>'
                         '<option Name="lib"><value>%s</value></option>'
                         '<option Name="cell"><value>TB</value></option>'
                         '<option Name="view"><value>schematic</value></option>'
                         '</tooloptions></test></tests></active></setupdb>' % logical)
        return path

    def _mapping(self, workspace, libraries):
        path = os.path.join(workspace, "cds.lib")
        with open(path, "w") as stream:
            for name, root in libraries.items():
                stream.write("DEFINE %s %s\n" % (name, json.dumps(root)))
        return path

    def test_default_prepare_still_rejects_other_setup(self):
        with mock.patch("crescendo_engine.server.prepare_setup") as prepare:
            with self.assertRaisesRegex(ValueError, "bound"):
                self.service.prepare(self.b, initialize_validation=False)
        prepare.assert_not_called()

    def test_same_saved_setup_reload_refreshes_test_binding_without_replacing_native_environment(self):
        native = copy.deepcopy(self.service.config["cadence"]["launch_environment"])
        self.service.config["cadence"]["_manual_overrides"] = {"alps_bin": "/manual/alps"}
        self.service.config["cadence"]["alps_bin"] = "/manual/alps"
        with open(self.a, encoding="utf-8") as stream:
            changed = stream.read().replace("<value>TB</value>", "<value>SavedTB</value>")
        with open(self.a, "w", encoding="utf-8") as stream:
            stream.write(changed)
        result = self.service.select_setup(self.a)
        self.assertEqual(result["context"]["live_tests"][0]["cell"], "SavedTB")
        self.assertEqual(result["context"]["saved_setup_refresh"]["source"], "explicit_saved_setup_reload")
        self.assertEqual(self.service.config["cadence"]["launch_environment"], native)
        self.assertEqual(self.service.config["cadence"]["alps_bin"], "/manual/alps")
        from crescendo_engine.single_test import project_setup
        self.service._validate_live_test_bindings(project_setup(inspect_setup(self.a), 'VCO'))
        wrong = project_setup(inspect_setup(self.a), 'VCO')
        wrong["_source_tests"][0]["cell"] = "OldTB"
        with self.assertRaisesRegex(ValueError, "differs"):
            self.service._validate_live_test_bindings(wrong)
        with self.assertRaisesRegex(ValueError, "bound"):
            self.service._validate_launch_binding(self.b)

    def test_state_only_save_refreshes_old_modified_flag_but_unchanged_disk_is_not_assumed_saved(self):
        self.service.config["_maestro_launch_context"]["source"]["modified"] = True
        with self.assertRaisesRegex(ValueError, "unsaved"):
            self.service.select_setup(self.a)
        state = os.path.join(os.path.dirname(self.a), "active.state")
        with open(state, "w", encoding="utf-8") as stream:
            stream.write("<state><saved>new saved state</saved></state>")
        result = self.service.select_setup(self.a)
        self.assertFalse(result["context"]["source"]["modified"])
        self.assertTrue(result["context"]["source"]["active_state_sha256"])
        self.service._validate_launch_binding(self.a)

    def test_unavailable_launch_fingerprint_is_not_proof_that_unsaved_changes_were_saved(self):
        source = self.service.config["_maestro_launch_context"]["source"]
        source.update(modified=True, setup_content_sha256=None)
        source.pop("active_state_sha256", None)
        with self.assertRaisesRegex(ValueError, "unsaved"):
            self.service.select_setup(self.a)

    def test_explicit_same_mapping_selection_has_separate_identity_and_no_live_tests(self):
        initial_root = self.service.config["runtime"]["run_root"]
        result = self.service.select_setup(os.path.dirname(self.b))
        self.assertEqual(result["setup_path"], self.b)
        self.assertEqual(result["mode"], "manual")
        self.assertIsNone(result["context"])
        self.assertEqual(result["source"]["library"], "LogicalB")
        self.assertNotEqual(self.service.config["runtime"]["run_root"], initial_root)
        mapping = self.service.config["cadence"]["source_cds_lib_override"]
        self.assertEqual(parse_cds_defines(mapping)["LogicalB"], os.path.dirname(os.path.dirname(os.path.dirname(self.b))))
        self.service._validate_live_test_bindings(inspect_setup(self.b))
        with self.assertRaisesRegex(ValueError, "another saved setup"):
            self.service._validate_launch_binding(self.a)

    def test_switch_back_restores_original_context_history_mapping_and_namespace(self):
        initial = copy.deepcopy(self.service.config)
        self.service.prepared["old"] = {"identity": "A"}
        resident = mock.Mock()
        self.service.resident_validation = resident
        self.service.select_setup(self.b)
        resident.close.assert_called_once()
        self.assertEqual(self.service.prepared, {})
        self.assertIsNone(self.service.resident_validation)
        result = self.service.select_setup(self.a)
        self.assertEqual(result["mode"], "invoking")
        self.assertEqual(result["context"], self.context)
        self.assertEqual(self.service.config, initial)

    def test_external_workspace_uses_target_mapping_not_original(self):
        workspace = os.path.join(self.root, "workspace two")
        target = self._setup(workspace, "physicalA", "LogicalA", "maestro")
        target_root = os.path.dirname(os.path.dirname(os.path.dirname(target)))
        cds = self._mapping(workspace, {"LogicalA": target_root})
        result = self.service.select_setup(target)
        self.assertEqual(result["source"]["cds_lib"], cds)
        self.assertEqual(parse_cds_defines(self.service.config["cadence"]["source_cds_lib_override"]), {"LogicalA": target_root})
        self.assertNotEqual(self.service.config["cadence"]["source_cds_lib_override"], self.context["effective_cds_lib"])

    def test_explicit_workspace_mapping_is_respected(self):
        workspace = os.path.join(self.root, "without-mapping")
        target = self._setup(workspace, "external", "Alias", "maestro")
        target_root = os.path.dirname(os.path.dirname(os.path.dirname(target)))
        custom = os.path.join(self.root, "custom-workspace")
        os.makedirs(custom)
        cds = self._mapping(custom, {"Alias": target_root})
        self.service.config["cadence"]["_manual_overrides"] = {"cds_lib": cds}
        result = self.service.select_setup(target)
        self.assertEqual(result["source"]["cds_lib"], cds)
        self.assertEqual(result["source"]["library"], "Alias")

    def test_selection_keeps_native_three_states_and_cleans_automatic_pdk_hints(self):
        self.service.invoking_config["cadence"].update(pdk_name="OldAutomatic", pdk_dir="/old/automatic")
        self.service.config["cadence"]["_manual_overrides"] = {"pdk_adapter_policy": "generic-only"}
        with mock.patch.dict(os.environ, {"LD_LIBRARY_PATH": "/gui/tk", "TCL_LIBRARY": "/gui/tcl", "TK_LIBRARY": "/gui/tk"}):
            self.service.select_setup(self.b)
            env = child_environment(self.service.config, os.path.join(self.root, "child"), include_runtime_ld=False)
            restored = restore_native_environment({}, self.service.config["cadence"]["launch_environment"])
        self.assertEqual(self.service.config["cadence"]["launch_environment"], self.native)
        for name, expected in self.native.items():
            self.assertEqual(restored.get(name), expected)
            self.assertEqual(name in restored, expected is not None)
        for name in ("LD_LIBRARY_PATH", "TCL_LIBRARY", "TK_LIBRARY", "PYTHONPATH"):
            self.assertEqual(env.get(name), self.native[name])
        self.assertIsNone(self.service.config["cadence"]["pdk_name"])
        self.assertIsNone(self.service.config["cadence"]["pdk_dir"])
        self.assertEqual(self.service.config["cadence"]["pdk_adapter_policy"], "generic-only")

    def test_bad_setup_and_missing_target_mapping_preserve_prepared_and_config(self):
        self.service.prepared["old"] = {"unchanged": True}
        initial = copy.deepcopy(self.service.config)
        workspace = os.path.join(self.root, "unmapped")
        target = self._setup(workspace, "physical", "unknown", "maestro")
        for path in (os.path.join(self.root, "absent.sdb"), target):
            with mock.patch.object(self.service, "_close_resident_validation") as close:
                with self.assertRaises(ValueError):
                    self.service.select_setup(path)
            close.assert_not_called()
            self.assertEqual(self.service.config, initial)
            self.assertEqual(self.service.prepared, {"old": {"unchanged": True}})

    def test_busy_optimization_or_validation_does_not_change_selection(self):
        initial = copy.deepcopy(self.service.config)
        self.service.optimization_launches = 1
        with self.assertRaisesRegex(RuntimeError, "optimization"):
            self.service.select_setup(self.b)
        self.service.optimization_launches = 0
        self.service.range_validations["active"] = {"state": "running"}
        with self.assertRaisesRegex(RuntimeError, "range validation"):
            self.service.select_setup(self.b)
        self.assertEqual(self.service.config, initial)
        self.assertEqual(self.service.lifecycle_operations, 0)
        self.assertFalse(self.service.eda_reconfiguring)

    def test_finishing_validation_thread_and_exclusive_operation_block_selection(self):
        self.service.range_validation_threads["finishing"] = mock.Mock(is_alive=lambda: True)
        with self.assertRaisesRegex(RuntimeError, "finishing"):
            self.service.select_setup(self.b)
        self.service.range_validation_threads.clear()
        self.service.lifecycle_operations = 1
        with self.assertRaisesRegex(RuntimeError, "resource operations"):
            self.service.select_setup(self.b)
        self.assertEqual(self.service.lifecycle_operations, 1)
        self.assertFalse(self.service.eda_reconfiguring)

    def test_explicit_config_file_pdk_precedence_survives_a_b_a(self):
        self.service.config["cadence"]["_manual_overrides"] = {"pdk_name": "manual"}
        self.service.config["cadence"]["_file_overrides"] = {"pdk_name": "file"}
        self.service.select_setup(self.b)
        self.assertEqual(self.service.config["cadence"]["pdk_name"], "file")
        self.service.select_setup(self.a)
        self.assertEqual(self.service.config["cadence"]["pdk_name"], "file")

    def test_non_native_search_and_model_overrides_survive_a_b_a(self):
        overrides = {"search_roots": ["/custom/search"], "model_roots": ["/custom/models"],
                     "model_path_remaps": [{"from": "/old", "to": "/new"}],
                     "startup_extensions": ["/custom/init.il"]}
        self.service.config["cadence"]["_manual_overrides"] = copy.deepcopy(overrides)
        for path in (self.b, self.a):
            self.service.select_setup(path)
            for key, value in overrides.items():
                self.assertEqual(self.service.config["cadence"][key], value)
            self.assertEqual(self.service.config["cadence"]["launch_environment"], self.native)
            self.assertEqual(self.service.config["cadence"]["ocean_bin"], self.config["cadence"]["ocean_bin"])

    def test_unsaved_invoking_setup_guard_does_not_apply_to_manual_selection(self):
        self.service.invoking_context["source"]["modified"] = True
        self.service.select_setup(self.b)
        previous = copy.deepcopy(self.service.config)
        with self.assertRaisesRegex(ValueError, "unsaved"):
            self.service.select_setup(self.a)
        self.assertEqual(self.service.config, previous)

    def test_arbitrary_setup_file_symlink_uses_real_library_context(self):
        alias = os.path.join(self.root, "alias.sdb")
        os.symlink(self.b, alias)
        result = self.service.select_setup(alias)
        self.assertEqual(result["setup_path"], self.b)
        self.assertEqual(result["source"]["library"], "LogicalB")

    def test_cleanup_failure_does_not_commit_new_config_or_clear_prepared(self):
        initial = copy.deepcopy(self.service.config)
        self.service.prepared["old"] = {"identity": "A"}
        with mock.patch.object(self.service, "_close_resident_validation", side_effect=RuntimeError("unproven cleanup")):
            with self.assertRaisesRegex(RuntimeError, "unproven"):
                self.service.select_setup(self.b)
        self.assertEqual(self.service.config, initial)
        self.assertEqual(self.service.prepared, {"old": {"identity": "A"}})

    def test_manual_start_guard_rejects_foreign_prepared_identity(self):
        self.service.select_setup(self.b)
        from crescendo_engine.single_test import project_setup
        self.service.prepared["foreign"] = {"setup": project_setup(inspect_setup(self.a), 'VCO')}
        with self.assertRaisesRegex(ValueError, "another saved setup"):
            self.service.start("foreign", {"evaluator": "mock"})

    def test_manual_prepare_accepts_selected_setup_and_defers_native_session(self):
        self.service.select_setup(self.b)
        prepared = {"setup": inspect_setup(self.b), "setup_path": self.b, "parameters": [], "outputs": [],
                    "hierarchy": {"designs": []}}
        with mock.patch("crescendo_engine.server.prepare_setup", return_value=prepared), mock.patch(
                "crescendo_engine.server.detect_pdk_context", return_value={}), mock.patch(
                "crescendo_engine.server.capture_prepared_freshness", return_value={}):
            prepared_id, loaded = self.service.prepare(self.b, initialize_validation=False)
        self.assertIn(prepared_id, self.service.prepared)
        self.assertEqual(loaded["resident_validation"]["state"], "deferred")

    def test_plugin_config_load_defers_native_validation_for_origin_and_manual_selection(self):
        for path in (self.a, self.b):
            with self.subTest(path=path):
                if path == self.b:
                    self.service.select_setup(path)
                    self.assertIsNone(self.service.config["_maestro_launch_context"])
                document = {"schema_version": "crescendo.design_config.v1",
                            "ade_setup": {"path": path, "backend": "maestro"}}
                prepared = {"setup": inspect_setup(path)}
                with mock.patch.object(self.service, "prepare", return_value=("prepared", prepared)) as prepare, mock.patch(
                        "crescendo_engine.server.resolve_design_config", return_value={}):
                    self.service.load_design_config(document)
                prepare.assert_called_once_with(path, backend="maestro", initialize_validation=False)

    def test_standalone_config_load_keeps_default_validation_initialization(self):
        service = OptimizerService(copy.deepcopy(DEFAULTS))
        document = {"schema_version": "crescendo.design_config.v1",
                    "ade_setup": {"path": self.a, "backend": "maestro"}}
        with mock.patch.object(service, "prepare", return_value=("prepared", {})) as prepare, mock.patch(
                "crescendo_engine.server.resolve_design_config", return_value={}):
            service.load_design_config(document)
        prepare.assert_called_once_with(self.a, backend="maestro")

    def test_adexl_selection_is_supported(self):
        target = self._setup(self.workspace, "physicalX", "LogicalX", "adexl", name="data.sdb")
        target_root = os.path.dirname(os.path.dirname(os.path.dirname(target)))
        # A library absent from the loaded native map uses the workspace map.
        self._mapping(self.workspace, {"LogicalX": target_root})
        result = self.service.select_setup(target)
        self.assertEqual(result["source"]["backend"], "adexl")

    def test_selected_settings_retain_native_toolchain_and_selected_mapping(self):
        self.service.select_setup(self.b)
        selected_map = self.service.config["cadence"]["source_cds_lib_override"]
        with mock.patch("crescendo_engine.server.discover_eda_environment",
                        side_effect=lambda cadence, validate=True, environ=None: {
                            "ok": True, "effective": {}, "sources": {},
                        }):
            result = self.service.update_eda_setup({"mode": "manual", "manual": {"pdk_adapter_policy": "generic-only"}})
        self.assertEqual(self.service.config["cadence"]["source_cds_lib_override"], selected_map)
        self.assertIsNone(self.service.config["_maestro_launch_context"])
        self.assertEqual(self.service.config["_selected_setup_path"], self.b)
        self.assertEqual(self.service.config["cadence"]["launch_environment"], self.native)
        self.assertTrue(result["prepared_invalidated"])
        self.service._validate_launch_binding(self.b)


if __name__ == "__main__":
    unittest.main()
