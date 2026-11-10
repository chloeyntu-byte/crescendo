from __future__ import print_function

import time
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.gui_state import (
    BackgroundTaskQueue,
    RunUiState,
    algorithm_for_objective_count,
    automatic_population,
    fallback_pareto_ids,
    group_parameters_by_name,
    initial_parameter_config,
    issue_display_name,
    objective_pairs,
    pairwise_points,
    parameter_owner_label,
    parse_physical_input,
    validated_run_display_name,
    validate_variable_configs,
)
from crescendo_engine.gui import LiveLogViewer, OptimizerGui


class FakeRoot(object):
    def __init__(self):
        self.callbacks = []

    def after(self, _milliseconds, callback):
        self.callbacks.append(callback)

    def step(self):
        callbacks = list(self.callbacks)
        self.callbacks = []
        for callback in callbacks:
            callback()


class GuiStateTests(unittest.TestCase):
    def _parameter(self, parameter_id="width", **changes):
        row = {
            "parameter_id": parameter_id,
            "hierarchy_path": "/X_OPA/X_STAGE1/MN_INN",
            "instance_name": "MN_INN",
            "master_library": "examplekit18pas",
            "master_cell": "n18",
            "name": "w",
            "prompt": "Finger Width",
            "semantic": "width",
            "numeric_value": 20e-9,
            "current_value": "20n",
            "value_type": "float",
        }
        row.update(changes)
        return row

    def test_physical_input_uses_unit_outside_entry(self):
        self.assertAlmostEqual(parse_physical_input("20", "n"), 20e-9)
        self.assertAlmostEqual(parse_physical_input("5e9"), 5e9)
        self.assertIsNone(parse_physical_input("20unknown", "n"))

    def test_numeric_choices_supply_actual_default_bounds(self):
        parameter = self._parameter(current_value="2u", numeric_value=2e-6,
                                    choices_raw='("2u" "10u")')
        config = initial_parameter_config(parameter)
        self.assertEqual(parse_physical_input(config["minimum"], config["unit"]), 2e-6)
        self.assertAlmostEqual(parse_physical_input(config["maximum"], config["unit"]), 10e-6)

    def test_width_and_length_are_selected_by_default(self):
        width = initial_parameter_config(self._parameter())
        length = initial_parameter_config(self._parameter(
            "length", name="l", prompt="Length per Finger", semantic="length",
        ))
        fingers = initial_parameter_config(self._parameter(
            "fingers", name="nf", prompt="Number of Fingers", semantic="fingers",
        ))
        self.assertTrue(width["selected"])
        self.assertTrue(length["selected"])
        self.assertFalse(fingers["selected"])

    def test_static_validation_reports_readable_device_and_equal_bounds(self):
        parameter = self._parameter()
        config = initial_parameter_config(parameter)
        config.update({"selected": True, "minimum": "20", "maximum": "20"})
        report = validate_variable_configs([parameter], {"width": config})
        self.assertEqual(report["issues"][0]["code"], "invalid_order")
        self.assertIn("MN_INN", report["issues"][0]["display_name"])
        self.assertIn("Finger Width", report["issues"][0]["display_name"])

    def test_backend_issue_fields_render_as_readable_hierarchy(self):
        label = issue_display_name({
            "hierarchy_path": "/X_OPA/X_STAGE1/MN_INN",
            "instance_name": "MN_INN", "master_library": "examplekit18pas",
            "master_cell": "n18", "parameter_name": "fw",
            "prompt": "Finger Width",
        })
        self.assertIn("/X_OPA/X_STAGE1/MN_INN", label)
        self.assertIn("examplekit18pas/n18", label)
        self.assertIn("Finger Width [fw]", label)

    def test_parameter_name_groups_are_case_insensitive_and_keep_device_ownership(self):
        width_a = self._parameter("width-a", name="nr")
        width_b = self._parameter(
            "width-b", name="NR", hierarchy_path="/X_OPA/X_STAGE2/MP_LOAD",
            instance_name="MP_LOAD", master_cell="p18",
        )
        multiplier = self._parameter(
            "multiplier", name="", cdf_name="m", prompt="Multiplier",
        )
        groups = group_parameters_by_name([width_b, multiplier, width_a])
        self.assertEqual([group["key"] for group in groups], ["m", "nr"])
        self.assertEqual(
            [row["parameter_id"] for row in groups[1]["parameters"]],
            ["width-a", "width-b"],
        )
        owner = parameter_owner_label(width_b)
        self.assertIn("/X_OPA/X_STAGE2/MP_LOAD", owner)
        self.assertIn("examplekit18pas/p18", owner)

    def test_run_display_name_is_required_trimmed_and_bounded(self):
        self.assertEqual(validated_run_display_name("  OPA AC corners  "), "OPA AC corners")
        for value in ("", "   ", "unsafe\nname", "x" * 121):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    validated_run_display_name(value)

    def test_parameter_label_single_click_toggles_without_tree_rebuild(self):
        class Root(object):
            def __init__(self):
                self.callback = None

            def after(self, _milliseconds, callback):
                self.callback = callback
                return "click-job"

            def after_cancel(self, _job):
                self.callback = None

        class Tree(object):
            def __init__(self):
                self.selected = None

            @staticmethod
            def identify_row(_y):
                return "parameter:width"

            @staticmethod
            def identify_column(_x):
                return "#0"

            @staticmethod
            def identify(_kind, _x, _y):
                return "Treeitem.text"

            def selection_set(self, item):
                self.selected = item

            @staticmethod
            def focus(_item):
                pass

        gui = object.__new__(OptimizerGui)
        gui.root = Root()
        gui.parameter_tree = Tree()
        gui.parameter_items = {"width": "parameter:width"}
        gui.node_parameters = {}
        gui.parameter_configs = {"width": {"selected": False}}
        gui.pending_parameter_click = None
        gui.pending_parameter_click_job = None
        dirty = []
        refreshed = []
        gui._mark_range_dirty = dirty.append
        gui._refresh_parameter_selection_display = lambda: refreshed.append(True)
        event = type("Event", (), {"x": 20, "y": 10})()
        self.assertEqual(gui._parameter_single_click(event), "break")
        self.assertTrue(gui.parameter_configs["width"]["selected"])
        self.assertEqual(dirty, ["width"])
        gui.root.callback()
        self.assertEqual(dirty, ["width"])
        self.assertEqual(refreshed, [True])
        self.assertEqual(gui.parameter_tree.selected, "parameter:width")

        self.assertEqual(gui._parameter_single_click(event), "break")
        self.assertFalse(gui.parameter_configs["width"]["selected"])
        self.assertEqual(gui._parameter_double_click(event), "break")
        self.assertTrue(gui.parameter_configs["width"]["selected"])
        self.assertEqual(dirty, ["width", "width"])

    def test_static_validation_rejects_bad_log_and_integer_domains(self):
        integer = self._parameter(
            "nf", name="nf", prompt="Number of Fingers", semantic="fingers",
            numeric_value=4, current_value="4", value_type="integer",
        )
        report = validate_variable_configs([integer], {"nf": {
            "selected": True, "minimum": "1.5", "maximum": "4",
            "unit": "", "scale": "log", "value_type": "integer",
        }})
        self.assertIn("non_integer_bound", [issue["code"] for issue in report["issues"]])

    def test_population_and_multiobjective_policy(self):
        self.assertEqual(automatic_population(1), 20)
        self.assertEqual(automatic_population(25), 25)
        selected, prior = algorithm_for_objective_count("bayesian_optimization", 3)
        self.assertEqual(selected, "nsga2")
        self.assertEqual(prior, "bayesian_optimization")
        selected, prior = algorithm_for_objective_count("nsga2", 1, prior)
        self.assertEqual(selected, "bayesian_optimization")
        self.assertIsNone(prior)

    def test_all_objective_pairs_use_metric_index(self):
        objectives = [
            {"output_key": "gain", "direction": "maximize"},
            {"output_key": "gain", "direction": "minimize"},
            {"output_key": "ugbw", "direction": "maximize"},
        ]
        self.assertEqual(objective_pairs(objectives), [(0, 1), (0, 2), (1, 2)])
        candidates = [{
            "candidate_id": "candidate-1", "generation": 0, "status": "completed",
            "metrics": {
                "feasible": True,
                "objectives": [{"value": 1}, {"value": 9}, {"value": 3}],
            },
        }]
        point = pairwise_points(candidates, objectives, 0, 1, {"candidate-1"})[0]
        self.assertEqual((point["x"], point["y"]), (1.0, 9.0))

    def test_fallback_pareto_is_full_dimensional(self):
        objectives = [
            {"direction": "minimize"}, {"direction": "minimize"},
            {"direction": "minimize"},
        ]
        candidates = []
        for candidate_id, values in (("a", (1, 5, 5)), ("b", (5, 1, 5)), ("c", (5, 5, 1)), ("d", (6, 6, 6))):
            candidates.append({
                "candidate_id": candidate_id, "status": "completed",
                "metrics": {
                    "feasible": True,
                    "objectives": [{"value": value} for value in values],
                },
            })
        self.assertEqual(fallback_pareto_ids(candidates, objectives), {"a", "b", "c"})

    def test_stop_state_is_immediate_and_idempotent(self):
        state = RunUiState()
        state.started("run-1")
        self.assertTrue(state.request_stop())
        self.assertEqual(state.state, "stopping")
        self.assertTrue(state.request_stop())
        state.update("running")
        self.assertEqual(state.state, "stopping")
        state.update("stopped")
        self.assertEqual(state.state, "stopped")
        self.assertFalse(state.stop_requested)

    def test_background_queue_delivers_result_via_root_after(self):
        root = FakeRoot()
        received = []
        task_queue = BackgroundTaskQueue(root, interval_ms=1)
        try:
            task_queue.submit(lambda: 42, received.append, received.append)
            deadline = time.time() + 2
            while not received and time.time() < deadline:
                time.sleep(0.01)
                root.step()
            self.assertEqual(received, [42])
        finally:
            task_queue.close()

    def test_live_log_poll_reads_only_selected_tab(self):
        calls = []

        class Service(object):
            @staticmethod
            def candidate_logs(_run_id, _candidate_id):
                return {"state": "available", "logs": [
                    {"log_id": "a"}, {"log_id": "b"},
                ]}

            @staticmethod
            def read_candidate_log_chunk(_run_id, _candidate_id, log_id, **_kwargs):
                calls.append(log_id)
                return {"log_id": log_id, "text": "", "next_offset": 0}

        class Tasks(object):
            @staticmethod
            def submit(function, success, _failure):
                success(function())

        class Notebook(object):
            @staticmethod
            def select():
                return "frame-b"

        class Viewer(object):
            closed = False
            inflight = False
            notebook = Notebook()
            run_id = "run-1"
            candidate_id = "g000_c0000"
            tabs = {
                "a": {"frame": "frame-a", "offset": 3, "file_token": "token-a"},
                "b": {"frame": "frame-b", "offset": 7, "file_token": "token-b"},
            }
            app = type("App", (), {"service": Service(), "tasks": Tasks()})()

            @staticmethod
            def winfo_exists():
                return True

            def _loaded(self, payload):
                self.payload = payload

            def _failed(self, error):
                raise error

        viewer = Viewer()
        LiveLogViewer._poll(viewer)
        self.assertEqual(calls, ["b"])
        self.assertEqual(viewer.payload[1][0][0]["log_id"], "b")

    def test_live_log_text_is_trimmed_to_memory_limit(self):
        class Text(object):
            def __init__(self):
                self.value = ""

            @staticmethod
            def configure(**_kwargs):
                pass

            def insert(self, _position, value):
                self.value += value

            def get(self, _start, _end):
                return self.value

            def delete(self, _start, _end):
                self.value = ""

        viewer = type("Viewer", (), {"MAX_TAB_CHARACTERS": 10})()
        tab = {"text": Text(), "characters": 0}
        LiveLogViewer._append_text(viewer, tab, "abcdefghijkl")
        self.assertTrue(tab["text"].value.startswith("[Earlier log text discarded"))
        self.assertTrue(tab["text"].value.endswith("cdefghijkl"))

    @mock.patch("crescendo_engine.gui.launch_gui", return_value=0)
    @mock.patch("crescendo_engine.__main__.load_config", return_value={"runtime": {}})
    def test_cli_gui_command_uses_native_launcher(self, _load_config, launch_gui):
        from crescendo_engine.__main__ import main
        self.assertEqual(main(["gui"]), 0)
        launch_gui.assert_called_once_with({"runtime": {}}, force_exit_process=True)

    def test_native_config_file_io_is_utf8_under_ascii_locale(self):
        from crescendo_engine.gui import _read_json_file, _write_json_file
        root = tempfile.mkdtemp(prefix="crescendo-gui-")
        path = os.path.join(root, "config.json")
        try:
            _write_json_file(path, {"label": "差分输入"})
            self.assertEqual(_read_json_file(path)["label"], "差分输入")
        finally:
            if os.path.isfile(path):
                os.remove(path)
            os.rmdir(root)

    def test_eda_overrides_preserve_hidden_manual_fields_without_copying_auto_values(self):
        class Variable(object):
            def __init__(self, value):
                self.value = value

            def get(self):
                return self.value

        class Text(object):
            def __init__(self, value=""):
                self.value = value

            def get(self, _start, _end):
                return self.value

        view = type("EdaView", (), {})()
        view.eda_setup = {
            "manual": {
                "model_roots": ["/portable/models"],
                "model_path_remaps": [{"source": "/old", "target": "/new"}],
                "startup_extensions": ["/portable/start.il"],
                "virtuoso_bin": "/old/virtuoso",
            },
            "effective": {
                "virtuoso_bin": "/auto/virtuoso",
                "search_roots": ["/auto/root"],
            },
        }
        view.eda_vars = {
            "virtuoso_bin": Variable(""),
            "cadence_root": Variable("/manual/cadence"),
            "pdk_adapter": Variable(""),
        }
        view.eda_binary_paths = Text("")
        view.eda_library_paths = Text("")
        view.eda_profiles = Text("")
        view.eda_policy_var = Variable("auto")
        view._text_lines = OptimizerGui._text_lines

        manual = OptimizerGui._collect_eda_overrides(view)

        self.assertNotIn("virtuoso_bin", manual)
        self.assertNotIn("search_roots", manual)
        self.assertEqual(manual["cadence_root"], "/manual/cadence")
        self.assertEqual(manual["model_roots"], ["/portable/models"])
        self.assertEqual(
            manual["model_path_remaps"], [{"source": "/old", "target": "/new"}],
        )
        self.assertEqual(manual["startup_extensions"], ["/portable/start.il"])

    def test_wheel_units_cover_x11_and_small_delta_events(self):
        def event(**values):
            return type("Event", (), values)()

        self.assertEqual(OptimizerGui._wheel_units(event(num=4, delta=0)), -1)
        self.assertEqual(OptimizerGui._wheel_units(event(num=5, delta=0)), 1)
        self.assertEqual(OptimizerGui._wheel_units(event(num=None, delta=120)), -1)
        self.assertEqual(OptimizerGui._wheel_units(event(num=None, delta=-120)), 1)
        self.assertEqual(OptimizerGui._wheel_units(event(num=None, delta=1)), -1)
        self.assertEqual(OptimizerGui._wheel_units(event(num=None, delta=-1)), 1)
        self.assertEqual(OptimizerGui._wheel_units(event(num=None, delta=0)), 0)

    def test_mousewheel_routes_only_to_current_visible_page(self):
        class Page(object):
            def __init__(self):
                self.scrolled = []
                self.child = type("Child", (), {})()
                self.child.master = self
                self.mapped = True
                self.containing = self.child

            def winfo_ismapped(self):
                return self.mapped

            def winfo_containing(self, _x, _y):
                return self.containing

            def scroll_units(self, units):
                self.scrolled.append(units)

        view = OptimizerGui.__new__(OptimizerGui)
        current = Page()
        other = Page()
        view.pages = {"Parameters": current, "Design Setup": other}
        view.current_page = "Parameters"
        event = type("Event", (), {"num": 5, "delta": 0, "x_root": 10, "y_root": 20})()

        self.assertEqual(view._page_mousewheel(event), "break")
        self.assertEqual(current.scrolled, [1])
        self.assertEqual(other.scrolled, [])

        current.containing = other.child
        self.assertIsNone(view._page_mousewheel(event))
        self.assertEqual(current.scrolled, [1])

        current.containing = current.child
        current.child.winfo_class = lambda: "Text"
        self.assertIsNone(view._page_mousewheel(event))
        self.assertEqual(current.scrolled, [1])

    def test_invalidate_prepared_clears_range_validation_controls(self):
        class Button(object):
            def __init__(self):
                self.state = None

            def configure(self, **values):
                self.state = values.get("state", self.state)

        class Variable(object):
            def __init__(self):
                self.value = None

            def set(self, value):
                self.value = value

        view = OptimizerGui.__new__(OptimizerGui)
        view.pending_parameter_click_job = None
        view.pending_parameter_click = object()
        view.prepared_epoch = 4
        view.prepared_id = "prepared"
        view.prepared = {"setup": {}}
        view.parameter_configs = {"w": {}}
        view.parameter_by_id = {"w": {}}
        view.parameter_items = {"w": "item"}
        view.node_parameters = {"node": ["w"]}
        view.parameter_item_labels = {"w": "W"}
        view.node_item_labels = {"node": "Node"}
        view.parameter_open_items_by_view = {"hierarchy": {"node"}, "name": set()}
        view.parameter_views_rendered = {"hierarchy"}
        view.rendered_parameter_view = "hierarchy"
        view.range_validation_job_id = "range-old"
        view.range_validation_inflight = True
        view.range_validation_results = {"w": [{"status": "pdk_adjusted"}]}
        view.range_validation_requested = {"w": {}}
        view._reset_metric_rows = lambda: None
        view._set_status = lambda _message: None
        view.start_button = Button()
        view.save_config_button = Button()
        view.validate_button = Button()
        view.cancel_validation_button = Button()
        view.apply_ranges_button = Button()
        view.setup_summary_var = Variable()
        view.sidebar_status_var = Variable()
        view.analysis_summary_var = Variable()
        view.range_status_var = Variable()

        view._invalidate_prepared("EDA settings changed")

        self.assertIsNone(view.prepared_id)
        self.assertEqual(view.prepared_epoch, 5)
        self.assertIsNone(view.range_validation_job_id)
        self.assertFalse(view.range_validation_inflight)
        self.assertEqual(view.range_validation_results, {})
        self.assertEqual(view.apply_ranges_button.state, "disabled")

    def test_stale_static_range_result_is_ignored_after_prepared_changes(self):
        class Tasks(object):
            def submit(self, function, on_success, on_error):
                self.function = function
                self.on_success = on_success
                self.on_error = on_error

        class Button(object):
            def __init__(self):
                self.state = "disabled"

            def configure(self, **values):
                self.state = values.get("state", self.state)

        variables = [{"parameter_id": "w", "minimum": 1.0, "maximum": 2.0}]
        view = OptimizerGui.__new__(OptimizerGui)
        view.prepared_epoch = 3
        view.prepared_id = "prepared-old"
        view._local_range_report = lambda: {"issues": [], "variables": variables}
        view.service = mock.Mock()
        view.service.validate_static_ranges.return_value = {
            "valid": True, "issues": [], "variables": variables,
        }
        view.tasks = Tasks()
        button = Button()
        accepted = []

        view._static_range_report(accepted.append, button=button)
        payload = view.tasks.function()
        view.prepared_epoch = 4
        view.prepared_id = "prepared-new"
        view.tasks.on_success(payload)

        self.assertEqual(accepted, [])
        self.assertEqual(button.state, "disabled")
        view.service.validate_static_ranges.assert_called_once_with(
            "prepared-old", variables,
        )

    def test_stale_range_validation_start_is_ignored_after_prepared_changes(self):
        class Tasks(object):
            def __init__(self):
                self.submissions = []

            def submit(self, function, on_success, on_error):
                self.submissions.append((function, on_success, on_error))

        class Button(object):
            def __init__(self):
                self.state = None

            def configure(self, **values):
                self.state = values.get("state", self.state)

        class Variable(object):
            def set(self, value):
                self.value = value

        variables = [{"parameter_id": "w", "minimum": 1.0, "maximum": 2.0}]
        view = OptimizerGui.__new__(OptimizerGui)
        view.prepared_epoch = 7
        view.prepared_id = "prepared-old"
        view._local_range_report = lambda: {"issues": [], "variables": variables}
        view.validate_button = Button()
        view.range_status_var = Variable()
        view.range_validation_results = {}
        view.range_validation_requested = {}
        view.range_validation_job_id = None
        view.service = mock.Mock()
        view.service.validate_static_ranges.return_value = {
            "valid": True, "issues": [], "variables": variables,
        }
        view.service.validate_ranges.return_value = {"job_id": "range-old"}
        view.tasks = Tasks()
        view._poll_range_validation = mock.Mock()

        view.validate_all_ranges()
        static_function, static_success, _static_error = view.tasks.submissions[0]
        static_success(static_function())
        start_function, start_success, _start_error = view.tasks.submissions[1]
        payload = start_function()
        view.prepared_epoch = 8
        view.prepared_id = None
        start_success(payload)

        self.assertIsNone(view.range_validation_job_id)
        view._poll_range_validation.assert_not_called()
        view.service.validate_ranges.assert_called_once_with(
            "prepared-old",
            [{"parameter_id": "w", "minimum": 1.0, "maximum": 2.0}],
        )

    def test_stale_range_validation_result_does_not_clear_new_inflight_job(self):
        class Tasks(object):
            def submit(self, function, on_success, on_error):
                self.function = function
                self.on_success = on_success
                self.on_error = on_error

        view = OptimizerGui.__new__(OptimizerGui)
        view.prepared_epoch = 11
        view.range_validation_job_id = "range-old"
        view.range_validation_inflight = False
        view.tasks = Tasks()
        view.service = mock.Mock()
        view.service.range_validation_status.return_value = {
            "state": "completed", "results": [],
        }

        view._poll_range_validation()
        payload = view.tasks.function()
        view.prepared_epoch = 12
        view.range_validation_job_id = "range-new"
        view.range_validation_inflight = True
        view.tasks.on_success(payload)

        view.service.range_validation_status.assert_called_once_with("range-old")
        self.assertTrue(view.range_validation_inflight)
        self.assertEqual(view.range_validation_job_id, "range-new")

    def test_candidate_navigation_renders_after_selecting_target_page(self):
        view = OptimizerGui.__new__(OptimizerGui)
        view.live_candidates = [
            {"candidate_id": "candidate-%03d" % index} for index in range(300)
        ]
        view.result_page = 0
        view.result_page_size = 250
        rendered_pages = []
        view.show_page = lambda name: rendered_pages.append((name, view.result_page))
        view._render_candidate_detail = lambda: None

        view.select_candidate("candidate-275")

        self.assertEqual(rendered_pages, [("Live Results", 1)])

    def test_show_page_redraws_only_the_visible_results_view(self):
        class Page(object):
            def __init__(self):
                self.visible = False

            def pack(self, **_kwargs):
                self.visible = True

            def pack_forget(self):
                self.visible = False

        class Button(object):
            def configure(self, **_kwargs):
                pass

        view = OptimizerGui.__new__(OptimizerGui)
        view.pages = dict((name, Page()) for name in (
            "Design Setup", "Live Results", "Pareto Trade-offs",
        ))
        view.page_buttons = dict((name, Button()) for name in view.pages)
        rendered = []
        view.result_tree = object()
        view.pareto_container = object()
        view.render_results = lambda: rendered.append("results")
        view.render_pareto = lambda: rendered.append("pareto")

        view.show_page("Live Results")
        self.assertEqual(rendered, ["results"])
        self.assertTrue(view.pages["Live Results"].visible)
        self.assertFalse(view.pages["Pareto Trade-offs"].visible)

        view.show_page("Pareto Trade-offs")
        self.assertEqual(rendered, ["results", "pareto"])

    @mock.patch("crescendo_engine.gui.build_objective_presentation")
    def test_progress_presentation_fallback_runs_inside_submitted_task(self, build):
        class Tasks(object):
            def submit(self, function, on_success, on_error):
                self.function = function
                self.on_success = on_success
                self.on_error = on_error

        view = OptimizerGui.__new__(OptimizerGui)
        view.run_ui = RunUiState()
        view.run_ui.started("run-1")
        view.progress_inflight = False
        view.live_spec = {}
        view.tasks = Tasks()
        view.service = mock.Mock()
        view.service.progress_delta = None
        view.service.progress.return_value = {
            "status": {"state": "running"},
            "spec": {"objectives": [{"output_key": "main::gain"}]},
            "candidates": [{"candidate_id": "g000_c0000"}],
        }
        build.return_value = {"pareto_candidate_ids": ["g000_c0000"]}

        view._poll_progress()

        build.assert_not_called()
        progress = view.tasks.function()
        build.assert_called_once()
        self.assertEqual(
            progress["objective_presentation"]["pareto_candidate_ids"],
            ["g000_c0000"],
        )


    def test_candidate_failure_location_distinguishes_pre_spectre_errors(self):
        from crescendo_engine.gui import _candidate_diagnostic_text
        from crescendo_engine.gui_state import (
            candidate_failure_stage_label, candidate_spectre_state,
            candidate_status,
        )

        workspace = {
            "status": "failed", "failure_class": "design_or_simulation",
            "error": "WorkspaceBuildFailure: Permission denied: maestro.sdb",
        }
        self.assertEqual(
            candidate_status(workspace),
            "Workspace Build Failed · Spectre Not Started",
        )
        self.assertEqual(
            candidate_failure_stage_label(workspace),
            "Private workspace build",
        )
        self.assertEqual(candidate_spectre_state(workspace), "not_started")
        self.assertEqual(candidate_spectre_state({
            "status": "failed", "failure_class": "parameter_application",
        }), "unknown")
        self.assertEqual(candidate_spectre_state({
            "status": "failed", "failure_class": "parameter_application",
            "failure_stage": "parameter_application",
        }), "not_started")
        self.assertEqual(candidate_spectre_state({
            "status": "failed", "failure_class": "parameter_application",
            "completion_evidence": {"ok": True},
        }), "started")
        summary = _candidate_diagnostic_text({
            "candidate_status": "failed",
            "spectre_state": "not_started",
            "failure_stage_label": "PDK / CDF callback",
            "failure_reason": "CDF callback rejected width=20u",
        })
        self.assertIn("Spectre: Not started", summary)
        self.assertIn("PDK / CDF callback", summary)
        self.assertIn("rejected width=20u", summary)

    def test_optional_callback_start_error_is_inline_and_nonmodal(self):
        class Tasks(object):
            def submit(self, function, on_success, on_error):
                self.function = function
                self.on_success = on_success
                self.on_error = on_error

        class Button(object):
            def __init__(self):
                self.state = None

            def configure(self, **values):
                self.state = values.get("state", self.state)

        class Variable(object):
            def __init__(self):
                self.value = ""

            def set(self, value):
                self.value = value

        variables = [{
            "parameter_id": "w", "minimum": 1.0, "maximum": 2.0,
        }]
        view = OptimizerGui.__new__(OptimizerGui)
        view.prepared_epoch = 1
        view.prepared_id = "prepared-1"
        view.validate_button = Button()
        view.cancel_validation_button = Button()
        view.range_status_var = Variable()
        view.range_validation_results = {}
        view.range_validation_requested = {}
        view.range_validation_job_id = None
        view.tasks = Tasks()
        view.service = mock.Mock()
        view._static_range_report = (
            lambda on_valid, button=None, optional=False: on_valid(variables)
        )

        with mock.patch("crescendo_engine.gui.messagebox.showerror") as modal:
            view.validate_all_ranges()
            view.tasks.on_error(RuntimeError("callback capability not found"))

        modal.assert_not_called()
        self.assertEqual(view.validate_button.state, "normal")
        self.assertIsNone(view.range_validation_job_id)
        self.assertIn("Optimization can start", view.range_status_var.value)


if __name__ == "__main__":
    unittest.main()
