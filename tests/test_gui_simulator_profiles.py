import copy
import os
import weakref
import unittest
from unittest import mock

from crescendo_engine import gui
from crescendo_engine.settings_state import build_settings_state, match_settings_state
from crescendo_engine.gui_state import RunUiState


class Value(object):
    def __init__(self, value):
        self.value = value
    def get(self):
        return self.value
    def set(self, value):
        self.value = value


class GuiSimulatorProfileTests(unittest.TestCase):
    def app(self):
        app = gui.OptimizerGui.__new__(gui.OptimizerGui)
        app.simulator_var = Value("spectre")
        app.simulator_profile_var, app.eda_simulator_profiles_var = Value(""), Value("")
        app.eda_setup = {"effective": {}, "sources": {"spectre_bin": "invoking Maestro", "alps_bin": "manual override"},
            "simulator_profiles": {"profiles": {
                "spectre": {"executable": "/spectre/bin/spectre", "model_dir": "/pdk/spectre", "result_reader": "spectre", "status": "ready"},
                "alps": {"executable": "/alps/bin/alps", "model_dir": "/pdk/alps", "result_reader": "alps", "status": "unqualified", "reasons": ["Check PSF output in a real run"]}}}}
        app.service = mock.Mock(config={"cadence": {"simulator": "spectre", "alps_result_reader": "alps"}})
        return app

    def test_global_selection_displays_both_profiles_without_mutating_active_config(self):
        app = self.app()
        original_config, profiles = copy.deepcopy(app.service.config), copy.deepcopy(app.eda_setup)
        app._render_simulator_profiles()
        self.assertIn("Next run execution: Spectre", app.simulator_profile_var.get())
        for path in ("/spectre/bin/spectre", "/alps/bin/alps", "/pdk/spectre", "/pdk/alps"):
            self.assertIn(path, app.simulator_profile_var.get())
        app.simulator_var.set("alps")
        app._render_simulator_profiles()
        self.assertIn("Next run execution: ALPS", app.simulator_profile_var.get())
        self.assertIn("source netlist capture: Spectre", app.simulator_profile_var.get())
        self.assertIn("manual override", app.eda_simulator_profiles_var.get())
        self.assertIn("Active runs retain their frozen configuration", app.simulator_profile_var.get())
        self.assertEqual(app.service.config, original_config)
        self.assertEqual(app.eda_setup, profiles)

    def test_unqualified_profile_explains_native_capture_and_distinguishes_alps_target_models(self):
        app = self.app()
        profiles = app.eda_setup["simulator_profiles"]["profiles"]
        profiles["spectre"]["status"] = "unqualified"
        profiles["spectre"]["reasons"] = ["original Maestro must supply current input.scs and capture cwd/-I"]
        profiles["alps"].update(status="blocked", capture_source_model_dir="/pdk/spectre",
            reasons=["Source /pdk/spectre/toplevel.scs: no readable model/subckt interface"])
        app._render_simulator_profiles()
        shown = app.eda_simulator_profiles_var.get()
        self.assertIn("待本轮网表确认", shown)
        self.assertIn("目标求解模型目录：/pdk/alps", shown)
        self.assertIn("原 Maestro 网表生成阶段模型目录（Spectre 格式）：/pdk/spectre", shown)
        self.assertIn("Load & Inspect → Apply", shown)
        self.assertIn("结果读取接口（Ocean）", shown)
        self.assertIn("不代表实际求解器", shown)
        self.assertIn("ALPS 可选 Spectre/PSF 接口", shown)
        self.assertIn("Source /pdk/spectre/toplevel.scs", shown)
        self.assertEqual(profiles["alps"]["model_dir"], "/pdk/alps")

    def test_effective_model_fields_show_profile_discovery_without_creating_overrides(self):
        app = self.app()
        app.eda_effective_vars = {name + "_model_dir": Value("") for name in ("spectre", "alps")}
        for name, origin in (("spectre", "source_model_root"), ("alps", "source_pdk_sibling")):
            app.eda_setup["simulator_profiles"]["profiles"][name]["model_dir_origin"] = origin
        before = copy.deepcopy(app.eda_setup)
        app._render_effective_eda_setup(app.eda_setup)
        self.assertIn("/pdk/spectre", app.eda_effective_vars["spectre_model_dir"].get())
        self.assertIn("source_model_root", app.eda_effective_vars["spectre_model_dir"].get())
        self.assertIn("source_pdk_sibling", app.eda_effective_vars["alps_model_dir"].get())
        self.assertEqual(app.eda_setup, before)

    def test_effective_explicit_model_override_remains_visible_when_profile_is_blocked(self):
        app = self.app()
        app.eda_effective_vars = {"spectre_model_dir": Value("")}
        app.eda_setup["effective"]["spectre_model_dir"] = "/invalid/manual/path"
        app.eda_setup["sources"]["spectre_model_dir"] = "manual"
        app._render_effective_eda_setup(app.eda_setup)
        self.assertEqual(app.eda_effective_vars["spectre_model_dir"].get(), "/invalid/manual/path · manual")

    def test_time_limits_and_declared_scheduler_freeze_entered_values(self):
        app = self.app()
        app.maestro_candidate_mode_var = Value("auto")
        app.command_scheduler_var = Value("batch")
        app.job_timeout_vars = {key: Value(value) for key, value in {
            "start_seconds": "1800", "configure_seconds": "300", "run_seconds": "", "linger_seconds": ""}.items()}
        settings = app._collect_job_settings()
        self.assertEqual(settings["maestro_job_overrides"]["timeouts"], {
            "start_seconds": 1800, "configure_seconds": 300, "run_seconds": -1, "linger_seconds": -1})
        self.assertEqual(settings["command_scheduler"], "batch")
        for key in app.job_timeout_vars:
            old = app.job_timeout_vars[key].get()
            app.job_timeout_vars[key].set("0")
            with self.assertRaisesRegex(ValueError, "zero is not allowed"):
                app._collect_job_settings()
            app.job_timeout_vars[key].set(old)

    def test_portable_state_preserves_incomplete_timeout_entries_and_scheduler(self):
        raw = {"optimization": {"command_scheduler": "batch", "job_timeout_run_seconds": "unfinished limit",
            "job_timeout_override_run_seconds": True, "job_timeout_linger_seconds": "-1",
            "job_timeout_override_linger_seconds": False, "simulator": "alps"}}
        state = build_settings_state(raw)
        applied = match_settings_state(state, {})["apply"]["optimization"]
        self.assertEqual(applied["job_timeout_run_seconds"], "unfinished limit")
        self.assertTrue(applied["job_timeout_override_run_seconds"])
        self.assertEqual(applied["command_scheduler"], "batch")
        self.assertEqual(applied["simulator"], "alps")

    def test_checkpoint_display_keeps_frozen_profile_identity(self):
        app = self.app()
        identity = {"schema_version": "crescendo.simulator_model_profile_identity.v1", "fingerprint": "fixture"}
        checkpoint = {"spec": {"command_scheduler": "batch", "simulator_model_profile_identity": identity,
            "maestro_job_overrides": {"timeouts": {"start_seconds": 120}}}}
        settings = app._checkpoint_design_config(checkpoint)["optimization"]["run"]
        self.assertEqual(settings["simulator_model_profile_identity"], identity)
        self.assertEqual(settings["maestro_job_overrides"]["timeouts"], {"start_seconds": 120})
        self.assertEqual(settings["command_scheduler"], "batch")

    def test_global_stop_accepts_registered_queued_run_and_preserves_terminal_states(self):
        state = RunUiState()
        state.started("owned-run")
        state.update("queued")
        self.assertTrue(state.request_stop())
        self.assertEqual(state.state, "stopping")
        for phase in ("running", "stopping"):
            state.update(phase)
            self.assertTrue(state.request_stop())
        state.update("stopped")
        self.assertFalse(state.request_stop())

    def test_fallback_measurement_limit_accepts_positive_or_blank_and_rejects_zero(self):
        app = self.app()
        app.generations_var = Value("2")
        app.timeout_var = Value("")
        app._collect_algorithm_settings = lambda: ("differential_evolution", {"population": 2})
        app._collect_job_settings = lambda: {"maestro_job_overrides": {}}
        self.assertEqual(app._build_run_settings()["candidate_timeout_seconds"], -1)
        for value in ("1", "0.5", "29"):
            app.timeout_var.set(value)
            self.assertEqual(app._build_run_settings()["candidate_timeout_seconds"], float(value))
        for value in ("0", "-1", "nan", "unfinished"):
            app.timeout_var.set(value)
            with self.assertRaisesRegex(ValueError, "positive seconds or blank"):
                app._build_run_settings()


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk controls require the existing DISPLAY")
class GuiSimulatorProfileTkTests(unittest.TestCase):
    def setUp(self):
        from tests.test_gui_optional_settings import GuiOptionalSettingsTkTests
        self.fixture = GuiOptionalSettingsTkTests(methodName="test_real_tcl_popup_mapping_is_ignored_and_normal_page_wheel_still_scrolls")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_selection_event_updates_profile_and_preserves_other_solver_overrides(self):
        app = self.fixture.app
        setup = GuiSimulatorProfileTests().app().eda_setup
        setup["manual"] = {"spectre_bin": "/custom/spectre", "alps_bin": "/custom/alps",
                           "spectre_model_dir": "/custom/models/spectre", "alps_model_dir": "/custom/models/alps"}
        app._render_eda_setup(setup)
        previous = copy.deepcopy(self.fixture.service.config)
        app.simulator_var.set("alps")
        app.simulator_combo.event_generate("<<ComboboxSelected>>")
        self.fixture.root.update()
        self.assertIn("Next run execution: ALPS", app.simulator_profile_var.get())
        self.assertEqual(app.eda_vars["spectre_bin"].get(), "/custom/spectre")
        self.assertEqual(app.eda_vars["alps_model_dir"].get(), "/custom/models/alps")
        self.assertEqual(self.fixture.service.config, previous)

    def test_timeout_and_scheduler_controls_roundtrip_in_raw_state(self):
        app = self.fixture.app
        app.command_scheduler_var.set("batch")
        app.job_timeout_override_vars["run_seconds"].set(True)
        app.job_timeout_vars["run_seconds"].set("")
        app.job_timeout_vars["configure_seconds"].set("draft incomplete")
        raw, _context = app._snapshot_ui_state()
        state = build_settings_state(raw)
        self.assertEqual(state["optimization"]["command_scheduler"], "batch")
        self.assertEqual(state["optimization"]["job_timeout_configure_seconds"], "draft incomplete")
        app.job_command_var.set("batch-submit -q fixture")
        with self.assertRaisesRegex(ValueError, "positive integer seconds"):
            app._collect_job_settings()
        app.job_timeout_vars["configure_seconds"].set("")
        self.assertEqual(app._collect_job_settings()["maestro_job_overrides"]["timeouts"]["run_seconds"], -1)

    def test_destroyed_metric_rows_release_tk_variables_on_owner_thread_without_gc(self):
        app = self.fixture.app
        row = gui.MetricRow(app.objective_container, "objective", ["FoM"], ["TT", "FF"], None, None)
        variables = [weakref.ref(getattr(row, key)) for key in ("output_var", "aggregation_var", "mode_var", "value_var")]
        variables.append(weakref.ref(row.corner_picker.label_var))
        variables.extend(weakref.ref(variable) for _name, variable in row.corner_picker.variables)
        picker = weakref.ref(row.corner_picker)
        row.destroy()
        self.assertTrue(all(reference() is None for reference in variables))
        self.assertIsNone(picker())
