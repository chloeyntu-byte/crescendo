from __future__ import print_function

import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.__main__ import main
from crescendo_engine.continuation import RuntimeOptimizationLease
from crescendo_engine.gui_state import RunUiState
from crescendo_engine.server import OptimizerService
from tests.regression_fixture import qualify


class Value(object):
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class TaskCollector(object):
    def __init__(self):
        self.calls = []

    def submit(self, function, loaded=None, failed=None):
        self.calls.append((function, loaded, failed))

    def finish(self):
        for function, loaded, failed in self.calls:
            try:
                result = function()
            except Exception as error:
                if failed:
                    failed(error)
                else:
                    raise
            else:
                if loaded:
                    loaded(result)


class MaestroPluginGuiTests(unittest.TestCase):
    def _app(self, context=None):
        app = object.__new__(gui.OptimizerGui)
        app.launch_context = copy.deepcopy(context)
        app.launch_state = {}
        app.setup_loading = False
        app.closing = False
        app.run_ui = RunUiState()
        app.range_validation_inflight = False
        app.pending_config = None
        app.prepared = None
        app.prepared_id = None
        app.cell_inspection = None
        app.eda_revalidation_required = False
        app.pending_settings_state = None
        app.test_load_epoch = 0
        app.test_choice_var = Value()
        app.test_combo = mock.Mock(spec=['configure'])
        app.execution_selection_panel = mock.Mock(spec=['set_setup'])
        app._set_setup_controls = mock.Mock()
        app.setup_path_var = Value("/project/lib/VCO/maestro/maestro.sdb")
        app.setup_summary_var = Value()
        app.launch_history_summary_var = Value()
        app.load_setup_button = mock.Mock()
        app.tasks = TaskCollector()
        app.service = mock.Mock(spec=OptimizerService)
        app._set_error = mock.Mock()
        app._set_status = mock.Mock()
        app._invalidate_prepared = mock.Mock()
        return app

    def test_cli_loads_unpersisted_config_before_context_and_forwards_storage_override(self):
        calls = []
        context = {"source": {"library": "lib"}}
        storage_root = "/explicit/storage/root"
        config = {"runtime": {"storage_base_override": storage_root}}
        bound = {"runtime": {}, "_maestro_launch_context": context}
        with mock.patch(
                "crescendo_engine.maestro_context.load_launch_context",
                side_effect=lambda path, storage_base_root: calls.append(
                    ("context", path, storage_base_root)) or context,
        ) as load_context, mock.patch(
                "crescendo_engine.__main__.load_config",
                side_effect=lambda *args, **kwargs: calls.append(("config", args, kwargs)) or config,
        ) as load, mock.patch(
                "crescendo_engine.maestro_context.bind_launch_config", return_value=bound,
        ) as bind, mock.patch.object(gui, "launch_gui", return_value=0) as launch:
            self.assertEqual(main(["gui", "--maestro-context-file", "/tmp/context.json"]), 0)
        self.assertEqual(calls, [
            ("config", (None,), {"persist_discovery": False, "validate_discovery": False}),
            ("context", "/tmp/context.json", storage_root),
        ])
        load.assert_called_once_with(None, persist_discovery=False, validate_discovery=False)
        load_context.assert_called_once_with("/tmp/context.json", storage_base_root=storage_root)
        bind.assert_called_once_with(config, context)
        launch.assert_called_once_with(bound, launch_context=context, force_exit_process=True)

    def test_bound_load_reads_test_catalogue_before_hierarchy_or_preparation(self):
        app = self._app({"source": {"setup_path": app_path()}})
        app._write_launch_state = mock.Mock()
        app._require_launch_setup = mock.Mock()
        app._set_test_catalog = mock.Mock()
        catalogue = {"setup_path": app_path(), "tests": [{"name": "VCO", "enabled": True}]}
        app.service.inspect_tests.return_value = ("catalogue-1", catalogue)
        app.load_setup()
        self.assertTrue(app.setup_loading)
        app.tasks.finish()
        app.service.inspect_tests.assert_called_once_with(app_path(), backend="auto")
        app.service.inspect_test_cells.assert_not_called()
        app.service.prepare.assert_not_called()
        app.service.prepare_cells.assert_not_called()
        app._set_test_catalog.assert_called_once_with("catalogue-1", catalogue)
        self.assertFalse(app.setup_loading)

    def test_active_optimization_blocks_reload_before_prepare(self):
        app = self._app({"source": {"setup_path": app_path()}})
        app.run_ui.started("active-run")
        app.load_setup()
        self.assertEqual(app.tasks.calls, [])
        app.service.prepare.assert_not_called()
        app._set_error.assert_called_once()

    def test_live_design_mismatch_does_not_replace_existing_prepared(self):
        app = self._app({"live_tests": [{
            "name": "VCO", "library": "lib2", "cell": "TB", "view": "schematic",
        }]})
        app.prepared = {"old": True}
        app.prepared_id = "old-id"
        app._require_launch_setup = mock.Mock()
        prepared = {
            "setup_path": app_path(),
            "setup": {"tests": [{"name": "VCO", "library": "lib1", "cell": "TB", "view": "schematic"}]},
        }
        with self.assertRaisesRegex(ValueError, "saved setup differs"):
            app._set_prepared("new-id", prepared)
        self.assertEqual(app.prepared_id, "old-id")
        self.assertEqual(app.prepared, {"old": True})

    def test_config_from_another_setup_cannot_mutate_parameter_selection(self):
        app = self._app({"source": {"setup_path": app_path()}})
        app.parameter_configs = {"w": {"selected": True}}
        document = {
            "schema_version": "crescendo.design_config.v1",
            "ade_setup": {"path": "/other/lib/VCO/maestro"},
            "optimization": {"variables": []},
        }
        with mock.patch(
                "crescendo_engine.maestro_context.validate_bound_setup",
                side_effect=ValueError("different Maestro"),
        ):
            with self.assertRaisesRegex(ValueError, "different Maestro"):
                app.apply_design_config(document)
        self.assertTrue(app.parameter_configs["w"]["selected"])

    def test_history_reads_exact_selected_session_record_without_global_latest(self):
        app = self._app({"selected_history": "older-selected"})
        record = {
            "name": "older-selected", "results_dir": "/lib/history/older-selected",
            "rdb_file": "/lib/history/older-selected/older-selected.rdb",
        }
        app.history_choice_var = Value("History: older-selected")
        app.history_choices = {"History: older-selected": record}
        parsed = {"point_count": 2, "output_count": 1, "test_count": 2, "corner_count": 1}
        with mock.patch(
                "crescendo_engine.rdb.parse_rdb_summary", return_value=parsed,
        ) as parse, mock.patch("crescendo_engine.rdb.parse_latest_rdb") as latest:
            app._load_launch_history()
            app.tasks.finish()
        parse.assert_called_once_with(record["rdb_file"], cancel_event=app.history_cancel_event)
        latest.assert_not_called()
        self.assertIn("older-selected", app.launch_history_summary_var.get())
        self.assertIn("2 points", app.launch_history_summary_var.get())

    def test_missing_source_rdb_is_optional_and_does_not_change_setup(self):
        app = self._app({"selected_history": "selected"})
        app.prepared = {"setup": {"tests": []}}
        app.history_choice_var = Value("History: selected")
        app.history_choices = {"History: selected": {"name": "selected", "rdb_file": None}}
        app._load_launch_history()
        self.assertEqual(app.tasks.calls, [])
        self.assertEqual(app.prepared, {"setup": {"tests": []}})
        self.assertIn("no saved RDB", app.launch_history_summary_var.get())

    def test_launch_state_preserves_history_and_excludes_runtime_environment(self):
        with tempfile.TemporaryDirectory() as root:
            state_path = os.path.join(root, "launch_state.json")
            context = {
                "state_path": state_path,
                "source": {"library": "lib", "cell": "VCO", "view": "maestro", "secret": "not-recorded"},
                "runtime_environment": {"SECRET": "not-recorded"},
            }
            app = self._app(context)
            app._write_launch_state("preparing", history={"state": "loading"})
            app._write_launch_state("prepared", counts={"tests": 1})
            with open(state_path) as stream:
                document = json.load(stream)
            self.assertEqual(document["state"], "prepared")
            self.assertEqual(document["history"]["state"], "loading")
            self.assertEqual(document["counts"]["tests"], 1)
            self.assertEqual(document["source"]["library"], "lib")
            self.assertNotIn("not-recorded", json.dumps(document))


class DeferredMaestroPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo-menu-service-')
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, 'VCO', 'maestro', 'maestro.sdb')
        os.makedirs(os.path.dirname(self.path))
        with open(self.path, 'w') as stream:
            stream.write('<setupdb><active><tests><test Name="VCO">'
                         '<tooloptions><option Name="lib"><value>lib</value></option>'
                         '<option Name="cell"><value>TB</value></option>'
                         '<option Name="view"><value>schematic</value></option>'
                         '</tooloptions></test></tests></active></setupdb>')
        self.runtime = os.path.join(self.temp.name, 'runtime')

    def _prepared(self):
        # prepare_setup is the native inspection boundary in these tests;
        # public prepare still reads a real saved catalogue and selects VCO.
        return qualify({
            "setup": {"tests": [{"name": "VCO", "enabled": True}]},
            "hierarchy": {}, "eda_context": {}, "job_policy": {},
            "source_identity": {"schema_version": "crescendo.source_design_identity.v1", "fingerprint": "test-source"},
        })

    def test_deferred_prepare_closes_legacy_session_without_starting_direct_cdf(self):
        service = OptimizerService({"runtime": {"run_root": self.runtime}})
        old = mock.Mock(spec=['close', 'closed', 'workspace', 'prepared_id', 'session_token'])
        old.closed, old.workspace = True, None
        old.close.return_value = {'cleanup_proven': True}
        service.resident_validation = old
        with mock.patch(
                "crescendo_engine.server.prepare_setup", return_value=self._prepared(),
        ), mock.patch.object(service, "eda_setup", return_value={}), mock.patch(
                "crescendo_engine.server.detect_pdk_context", return_value={},
        ), mock.patch.object(service, "_start_resident_validation") as start:
            prepared_id, prepared = service.prepare(self.path, initialize_validation=False)
        self.assertEqual(prepared["resident_validation"]["state"], "deferred")
        self.assertIs(service.prepared[prepared_id], prepared)
        self.assertIsNone(service.resident_validation)
        start.assert_not_called()
        old.close.assert_called_once()
        self.assertGreater(old.close.call_args[1]["timeout_seconds"], 0)

    def test_live_test_mismatch_cannot_register_or_create_resident_workspace(self):
        for initialize in (False, True):
            with self.subTest(initialize=initialize):
                service = OptimizerService({
                    "runtime": {"run_root": self.runtime},
                    "_maestro_launch_context": {"live_tests": [{"name": "VCO", "library": "current-lib"}]},
                })
                prepared = self._prepared()
                prepared["setup"]["tests"] = [{"name": "VCO", "library": "old-lib"}]
                qualify(prepared)
                with mock.patch.object(service, "_validate_launch_binding"), mock.patch(
                        "crescendo_engine.server.prepare_setup", return_value=prepared,
                ), mock.patch.object(service, "_start_resident_validation") as start:
                    with self.assertRaisesRegex(ValueError, "saved setup differs"):
                        service.prepare(self.path, initialize_validation=initialize)
                start.assert_not_called()
                self.assertEqual(service.prepared, {})

    def test_live_test_mismatch_blocks_run_and_resume_launch_before_preflight(self):
        service = OptimizerService({
            "runtime": {"run_root": self.runtime},
            "_maestro_launch_context": {"live_tests": [{"name": "VCO", "cell": "current-TB"}]},
        })
        prepared = qualify({"setup": {"tests": [{"name": "VCO", "cell": "old-TB"}]}})
        with mock.patch("crescendo_engine.server.startup_storage_preflight") as preflight, mock.patch(
                "crescendo_engine.server.run_optimization",
        ) as optimization:
            for checkpoint in (None, {"resume": True}):
                with self.assertRaisesRegex(ValueError, "saved setup differs"):
                    service._launch({}, prepared, checkpoint=checkpoint)
        preflight.assert_not_called()
        optimization.assert_not_called()
        self.assertEqual(service.optimization_launches, 0)

    def test_active_run_blocks_prepare_before_probe_and_resident_creation(self):
        service = OptimizerService({"runtime": {"run_root": self.runtime}})
        service.threads["run"] = mock.Mock(is_alive=mock.Mock(return_value=True))
        with mock.patch("crescendo_engine.server.prepare_setup") as probe, mock.patch.object(
                service, "_start_resident_validation",
        ) as start:
            with self.assertRaisesRegex(RuntimeError, "optimization is running"):
                service.prepare(self.path, initialize_validation=False)
        probe.assert_not_called()
        start.assert_not_called()

    def test_registered_prepare_preserves_session_pdk_resolution_without_redetection(self):
        service = OptimizerService({"runtime": {"run_root": self.runtime}})
        prepared = self._prepared()
        context = {"source_cds_lib": "/session/cds.lib", "contexts": [{"name": "PDK"}]}
        prepared["eda_context"]["pdk"] = context
        with mock.patch(
                "crescendo_engine.server.prepare_setup", return_value=prepared,
        ), mock.patch.object(service, "eda_setup", return_value={}), mock.patch(
                "crescendo_engine.server.detect_pdk_context",
        ) as detect:
            _prepared_id, result = service.prepare(self.path, initialize_validation=False)
        self.assertEqual(result["eda_context"]["pdk"], context)
        detect.assert_not_called()

    def test_active_range_job_blocks_deferred_prepare(self):
        service = OptimizerService({"runtime": {"run_root": self.runtime}})
        service.range_validations["job"] = {"state": "running"}
        with mock.patch("crescendo_engine.server.prepare_setup") as probe:
            with self.assertRaisesRegex(RuntimeError, "range validation is running"):
                service.prepare(self.path, initialize_validation=False)
        probe.assert_not_called()

    def test_range_validation_during_active_run_uses_an_independent_lease(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root, "range_validation_timeout_seconds": 5,
            }})
            parameter = {"parameter_id": "w", "optimizable": True}
            prepared = qualify({"parameters": [parameter], "resident_validation": {"state": "deferred"}})
            service.prepared["prepared-1"] = prepared
            service.optimization_launches = 1
            active = mock.Mock(is_alive=mock.Mock(return_value=True))
            service.threads["optimization"] = active
            resident = mock.Mock()

            def run(job, _prepared, _variables, _config, cancel, update, resident_session=None):
                self.assertIsNone(resident_session)
                self.assertIs(service.threads["optimization"], active)
                self.assertTrue(active.is_alive())
                self.assertTrue(optimization_lease.held)
                validation_lease = service.range_validation_leases[job["job_id"]]
                self.assertTrue(validation_lease.held)
                self.assertNotEqual(validation_lease.path, optimization_lease.path)
                self.assertEqual(validation_lease.lease_kind, "optional_pdk_callback_validation")
                self.assertEqual(job["resource_scope"], "isolated_optional_validation")
                update(state="completed")

            with RuntimeOptimizationLease(root) as optimization_lease, mock.patch.object(
                    service, "_start_resident_validation", return_value=resident,
            ) as start, mock.patch("crescendo_engine.server.time.monotonic", return_value=100.0), mock.patch(
                    "crescendo_engine.server.run_range_validation", side_effect=run,
            ):
                response = service.validate_ranges("prepared-1", [
                    {"parameter_id": "w", "minimum": 1, "maximum": 2},
                ])
                thread = service.range_validation_threads[response["job_id"]]
                thread.join(2)
                self.assertFalse(thread.is_alive())
                start.assert_not_called()
                self.assertTrue(optimization_lease.held)
                self.assertIs(service.threads["optimization"], active)
                self.assertEqual(service.optimization_launches, 1)
                self.assertEqual(service.range_validation_status(response["job_id"])["state"], "completed")
                resident.close.assert_not_called()
            self.assertFalse(optimization_lease.held)

    def test_first_explicit_range_validation_uses_selected_native_generation_without_resident(self):
        with tempfile.TemporaryDirectory() as root:
            service = OptimizerService({"runtime": {
                "run_root": root, "range_validation_timeout_seconds": 5,
            }})
            selected = {"parameter_id": "w", "optimizable": True}
            prepared = qualify({
                "parameters": [selected, {"parameter_id": "l", "optimizable": True}],
                "resident_validation": {"state": "deferred"},
            })
            variables = [{"parameter_id": "w", "minimum": 1, "maximum": 2}]
            service.prepared["prepared-1"] = prepared
            resident = mock.Mock(workspace={"root": os.path.join(root, "private")})

            def run(job, _prepared, _variables, _config, cancel, update, resident_session=None):
                self.assertIsNone(resident_session)
                self.assertIs(_prepared, prepared)
                self.assertEqual(_variables, variables)
                self.assertIs(cancel, service.range_validation_cancels[job["job_id"]])
                self.assertFalse(cancel.is_set())
                update(state="completed")

            with mock.patch("crescendo_engine.server.ResidentValidationSession", return_value=resident) as session, mock.patch.object(
                    service, "_start_resident_validation", wraps=service._start_resident_validation,
            ) as start, mock.patch("crescendo_engine.server.time.monotonic", return_value=100.0), mock.patch(
                    "crescendo_engine.server.run_range_validation", side_effect=run,
            ):
                response = service.validate_ranges("prepared-1", variables)
                thread = service.range_validation_threads[response["job_id"]]
                thread.join(2)
                self.assertFalse(thread.is_alive())
            cancel = service.range_validation_cancels[response["job_id"]]
            start.assert_not_called()
            session.assert_not_called()
            resident.start.assert_not_called()
            cancel.set()
            self.assertTrue(cancel.is_set())
            resident.close.assert_not_called()
            self.assertIsNone(service.resident_validation)
            self.assertEqual(prepared["resident_validation"]["state"], "deferred")
            self.assertEqual(service.range_validation_status(response["job_id"])["state"], "completed")

    def test_bound_config_loading_rejects_another_library_before_prepare(self):
        service = OptimizerService({"_maestro_launch_context": {"source": {}}, "runtime": {}})
        with mock.patch(
                "crescendo_engine.maestro_context.validate_bound_setup", side_effect=ValueError("different Maestro"),
        ), mock.patch.object(service, "prepare") as prepare:
            with self.assertRaisesRegex(ValueError, "different Maestro"):
                service.load_design_config({"ade_setup": {"path": "/other/lib/VCO/maestro", "backend": "maestro"}})
        prepare.assert_not_called()

    def test_plugin_settings_save_and_reset_rebind_tools_without_global_config_writes(self):
        for mode in ("manual", "auto"):
            with self.subTest(mode=mode):
                context = {"source": {"ocean": "/native/ocean"}, "effective_cds_lib": "/native/cds.lib"}
                local_config = "/context/plugin_eda_config.json"
                service = OptimizerService({
                    "_maestro_launch_context": context, "runtime": {},
                    "cadence": {"user_config_path": local_config},
                })
                manual = {"pdk_adapter_policy": "generic-only"}

                def bind(config, invoking_context):
                    self.assertEqual(invoking_context, context)
                    self.assertEqual(config["cadence"]["_manual_overrides"], manual if mode == "manual" else {})
                    config["cadence"]["ocean_bin"] = "/native/ocean"
                    config["cadence"]["source_cds_lib_override"] = "/native/cds.lib"
                    config["cadence"]["launch_environment"] = {"LD_LIBRARY_PATH": "/native/lib", "CDS_LIC_FILE": "/native/license"}
                    config["cadence"]["discovery"] = {
                        "sources": {"ocean_bin": "invoking Maestro"},
                        "recommendations": ["native source recommendation"],
                    }
                    return config

                def discover(cadence, validate=True, environ=None):
                    self.assertTrue(validate)
                    self.assertEqual(cadence["ocean_bin"], "/native/ocean")
                    self.assertEqual(cadence["source_cds_lib_override"], "/native/cds.lib")
                    self.assertEqual(environ["LD_LIBRARY_PATH"], "/native/lib")
                    self.assertEqual(environ["CDS_LIC_FILE"], "/native/license")
                    return {"effective": {"ocean_bin": "/native/ocean"}, "ok": True, "sources": {"ocean_bin": "manual"}}

                with mock.patch(
                        "crescendo_engine.server.configure_eda",
                ) as standalone, mock.patch("crescendo_engine.server.discover_eda_environment", side_effect=discover) as validate, mock.patch(
                        "crescendo_engine.server.persist_eda_setup", return_value=local_config,
                ) as persist, mock.patch.object(service, "_cancel_and_wait_range_validations"), mock.patch.object(
                        service, "_close_resident_validation",
                ), mock.patch("crescendo_engine.maestro_context.bind_launch_config", side_effect=bind) as rebind:
                    service.update_eda_setup({"mode": mode, "manual": manual})
                rebind.assert_called_once()
                validate.assert_called_once()
                standalone.assert_not_called()
                self.assertEqual(persist.call_args[1]["path"], local_config)
                self.assertEqual(service.config["cadence"]["ocean_bin"], "/native/ocean")
                self.assertEqual(service.config["cadence"]["source_cds_lib_override"], "/native/cds.lib")
                self.assertEqual(service.config["cadence"]["discovery"]["sources"]["ocean_bin"], "invoking Maestro")
                self.assertIn("native source recommendation", service.config["cadence"]["discovery"]["recommendations"])

    def test_plugin_native_validation_failure_keeps_existing_session_and_config(self):
        context = {"source": {"ocean": "/native/ocean"}}
        service = OptimizerService({
            "_maestro_launch_context": context, "runtime": {},
            "cadence": {"user_config_path": "/context/plugin_eda_config.json"},
        })
        service.prepared["prepared-1"] = {"unchanged": True}
        original = copy.deepcopy(service.config)
        failed = {"ok": False, "effective": {}, "version_checks": [{"name": "ocean", "ok": False}]}
        with mock.patch(
                "crescendo_engine.maestro_context.bind_launch_config", side_effect=lambda config, _context: config,
        ), mock.patch("crescendo_engine.server.discover_eda_environment", return_value=failed), mock.patch.object(
                service, "_cancel_and_wait_range_validations",
        ) as cancel, mock.patch.object(service, "_close_resident_validation") as close, mock.patch(
                "crescendo_engine.server.persist_eda_setup",
        ) as persist:
            with self.assertRaisesRegex(ValueError, "Invoking Maestro environment validation failed"):
                service.update_eda_setup({"mode": "manual", "manual": {"pdk_adapter_policy": "generic-only"}})
        cancel.assert_not_called()
        close.assert_not_called()
        persist.assert_not_called()
        self.assertEqual(service.config, original)
        self.assertEqual(service.prepared, {"prepared-1": {"unchanged": True}})


def app_path():
    return "/project/lib/VCO/maestro/maestro.sdb"


if __name__ == "__main__":
    unittest.main()
