from __future__ import print_function

import copy
from .gui_performance import ScopeIndex, snapshot_copy, parameter_defaults, CachedCanvas, json_detail_tail
import json
import io
import os
import shlex
import signal
import sys
import tempfile
import time
import threading
import traceback as traceback_module

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, simpledialog, ttk
except ImportError:
    import Tkinter as tk
    import tkFileDialog as filedialog
    import tkMessageBox as messagebox
    import tkSimpleDialog as simpledialog
    import ttk

from .gui_activity import ActivityJournal
from .diagnostic_plan import diagnostic_shape
from .ui_gates import merge_gate_progress, setup_gate_progress
from .gui_state import (
    ALGORITHM_FIELDS,
    ALGORITHM_NAMES,
    BackgroundTaskQueue,
    RunUiState,
    algorithm_for_objective_count,
    candidate_failure_stage_label,
    candidate_spectre_state,
    calculated_algorithm_defaults,
    candidate_status,
    compact_metric_values,
    fallback_pareto_ids,
    finite_number,
    format_physical,
    group_parameters_by_name,
    initial_parameter_config,
    issue_display_name,
    metric_definition_label,
    metric_value,
    objective_pairs,
    pairwise_points,
    parameter_owner_label,
    parameter_label,
    parse_physical_input,
    validated_run_display_name,
    validate_variable_configs,
)
from .multiobjective import build_objective_presentation, resolve_algorithm_policy
from .storage_errors import format_task_error, is_storage_full_error
from .job_policy import job_policy_fingerprint, normalize_job_overrides, job_timeout_policy, job_input_defaults, JOB_TIMEOUT_PROPERTIES
from .execution_commands import normalize_execution_commands, DEFAULT_SOLVER_TEMPLATES
from .utils import atomic_write_json, utc_timestamp
from .diagnosis_catalog import describe_check, format_section, read_section, worker_error_text
from .runtime_layout import settings_filename
from .unified_configuration import (
    build_configuration, normalize_configuration, configuration_settings, match_configuration, maestro_settings_snapshot,
    read_configuration, save_configuration, read_last_configuration, save_last_configuration, configuration_revision,
)
from .settings_state import (
    build_settings_state, match_settings_state, read_settings_state,
    save_settings_state, settings_state_directory, last_settings_path,
    save_last_settings_state, read_last_settings_state,
)


PALETTE = (
    "#668fc4", "#6cae9b", "#c58aa0", "#9b8ec4", "#d5a25f",
    "#6fa6b4", "#9cab67", "#c47b6d", "#7f91aa", "#b18a68",
)

BACKGROUND = "#f5f8fb"
PANEL = "#ffffff"
SIDEBAR = "#e9f0f6"
ACCENT = "#587fa8"
ACCENT_LIGHT = "#dce9f3"
TEXT = "#263746"
MUTED = "#657888"
DANGER = "#a86067"
SUCCESS = "#4f846e"
WARNING = "#a77a3c"

PAGE_NAMES = (
    "Design Setup",
    "Parameters",
    "Objectives & Constraints",
    "Optimization",
    "Live Results",
    "Pareto Trade-offs",
    "EDA Settings",
)

MAX_JSON_BYTES = 64 * 1024 * 1024


def execution_progress_text(status, execution=None):
    """Format observed execution records without providing a command runner."""
    execution = execution or {}
    startup = execution.get("startup_diagnostic") or {}
    lines = [
        "State: {} | Stage: {}".format(status.get("state") or "unknown",
                                       startup.get("stage") or execution.get("stage") or "unknown"),
        str(status.get("message") or execution.get("message") or ""),
    ]
    if status.get("execution_backend") == "persistent_maestro":
        lines.append("Ocean workers: {} | Native max jobs: {}".format(
            status.get("persistent_worker_count", 1), status.get("maestro_max_jobs", "unknown")))
    run_root = execution.get("run_root") or status.get("run_root")
    if run_root:
        lines.append("Run directory: {}".format(run_root))
        lines.append("Main log: {}".format(execution.get("main_log_file") or os.path.join(run_root, "crescendo.log")))
    for key, label in (("status_file", "Status"), ("error_file", "Failure log")):
        path = execution.get(key)
        if not path and run_root:
            path = os.path.join(run_root, "status.json" if key == "status_file" else "server_error.json")
        if path:
            lines.append("{}: {}".format(label, path))
    if execution.get("startup_error_file"):
        lines.append("Startup diagnostics: {}".format(execution["startup_error_file"]))
    for record in (execution.get("commands") or [])[:8]:
        argv = record.get("argv")
        command = " ".join(shlex.quote(str(part)) for part in argv) if isinstance(argv, list) else record.get("command")
        if command:
            layer = record.get("layer") or ("solve" if record.get("kind") in ("solver", "solve", "solver_command") else
                    "submit" if record.get("kind") in ("submission", "submit", "submission_command") else None)
            label = {"prepared": "Prepared command", "completed": "Completed command",
                     "verified": "Native readback", "readback_failed": "Native readback failed",
                     "observed_submission": "Observed submission"}.get(record.get("state"))
            if layer in ("solve", "submit"):
                label = "Solver command" if layer == "solve" else "Submission command"
            if label is None:
                label = ("Configured submission" if record.get("state") == "configured"
                         or record.get("kind") == "configured_submit" else "Recorded launch")
            lines.extend(["", "{}: {}".format(label, record.get("label") or record.get("kind") or "command"),
                          "$ {}".format(str(command)[:8192])])
            if record.get("source"):
                lines.append("Evidence: {}".format(record["source"]))
            if record.get("max_jobs"):
                lines.append("Configured max jobs: {}".format(record["max_jobs"]))
            if record.get("original_command"):
                lines.append("Original command: {}".format(record["original_command"]))
            if record.get("distribution") or record.get("run_mode"):
                lines.append("Distribution: {} | Run mode: {}".format(record.get("distribution"), record.get("run_mode")))
            if record.get("scope") == "shared_native_worker_pool":
                lines.append("Identity: shared native worker pool/batch; candidate point mapping is unproven.")
            if record.get("invocation_id"):
                lines.append("Invocation: {}".format(record["invocation_id"]))
            if record.get("returncode") is not None:
                lines.append("{} return: {}".format("Solver" if layer == "solve" else "Submission" if layer == "submit" or record.get("kind") == "native_submission" else "Command", record["returncode"]))
            if record.get("job_ids"):
                lines.append("Observed job IDs: {}".format(", ".join(str(value) for value in record["job_ids"])))
            for key in ("stdout", "stderr"):
                if record.get(key):
                    lines.append("{}: {}".format(key, str(record[key])[-2048:]))
    for worker in (execution.get("workers") or [])[:4]:
        lines.append("Worker {}: {} | {} | {}".format(
            worker.get("worker_index", "?"), worker.get("status") or "unknown",
            worker.get("stage") or worker.get("operation") or "",
            worker.get("message") or ""))
    error = execution.get("error") or {}
    if error.get("message"):
        lines.extend(["", "Failure: {}".format(error["message"])])
    if startup.get("native_error") or error.get("startup_cause"):
        lines.extend(["", "Native startup error: {}".format(
            startup.get("native_error") or error["startup_cause"])])
    for key, label in (("remote_traceback_tail", "Remote traceback"),
                       ("traceback_tail", "Traceback")):
        if error.get(key):
            lines.extend(["", label + ":", str(error[key])[-8192:]])
    for entry in (execution.get("log_tails") or [])[:4]:
        lines.extend(["", "Log: {}{}".format(entry.get("path") or "",
                                             " (retained after cleanup)" if entry.get("retained") else
                                             " (tail)" if entry.get("truncated") else ""),
                      str(entry.get("text") or "")[-4096:]])
    for warning in (execution.get("warnings") or [])[:8]:
        lines.append("Diagnostic: {}".format(warning))
    return "\n".join(lines)[:49152]
CHECKED_MARK = u"\u2611"
UNCHECKED_MARK = u"\u2610"
PARTIAL_MARK = u"\u25a3"


def _read_json_file(path):
    if os.path.getsize(path) > MAX_JSON_BYTES:
        raise ValueError("File exceeds the 64 MiB limit.")
    with io.open(path, "r", encoding="utf-8") as stream:
        document = json.load(stream)
    if not isinstance(document, dict):
        raise ValueError("JSON document must be an object.")
    return document


def _write_text_file(path, text):
    temporary = path + ".tmp-{}".format(os.getpid())
    with io.open(temporary, "w", encoding="utf-8", newline="") as stream:
        stream.write(text)
    os.replace(temporary, path)


def _write_json_file(path, document):
    _write_text_file(path, json.dumps(document, indent=2, sort_keys=True) + "\n")


def _compact_diagnostic(value, limit=480):
    value = " ".join(str(value or "").split())
    limit = max(4, int(limit))
    if len(value) <= limit:
        return value
    return value[:limit - 3].rstrip() + "..."


def _candidate_diagnostic_text(metadata):
    status = str(metadata.get("candidate_status") or "unknown")
    spectre_state = str(metadata.get("spectre_state") or "unknown")
    spectre_label = {
        "not_started": "Not started",
        "started": "Started",
        "unknown": "Unknown",
    }.get(spectre_state, spectre_state.replace("_", " ").title())
    simulator = str(metadata.get("execution_simulator") or metadata.get("simulator") or "spectre")
    lines = ["Candidate status: {}".format(status), "{}: {}".format("ALPS" if simulator == "alps" else "Spectre", spectre_label)]
    if metadata.get("stage"):
        lines.append("Current stage: {}".format(metadata["stage"]))
    if metadata.get("message"):
        lines.append(_compact_diagnostic(metadata["message"], 540))
    tasks = metadata.get("tasks") or []
    commands = set()
    for task in tasks[:3]:
        identity = " / ".join(str(task.get(key)) for key in ("test", "corner", "point_id")
                              if task.get(key) not in (None, ""))
        lines.append("{}: {}{}".format(identity or "Native task", task.get("state") or "unknown",
                                       " · job " + str(task["job_id"]) if task.get("job_id") else ""))
        if task.get("scheduler_state"):
            lines.append("JOB_STATE: {}{}{}".format(task["scheduler_state"],
                " · execution node " + str(task["execution_host"]) if task.get("execution_host") else "",
                " · exit " + str(task["job_exit_code"]) if task.get("job_exit_code") is not None else ""))
        timeout = task.get("task_timeouts") or {}
        if timeout.get("phase"):
            lines.append("Timer: {} · elapsed {} s · limit {}".format(timeout["phase"],
                round(float(timeout.get("phase_elapsed_seconds") or 0), 1),
                str(timeout.get("limit_seconds")) + " s" if timeout.get("enabled") else "disabled"))
        for label, command in (("Solver", task.get("solver_command")),
                               ("Submission", task.get("submission_command") or task.get("command"))):
            if command and command not in commands:
                commands.add(command)
                lines.append(label + " command: " + _compact_diagnostic(str(command), 900))
        if task.get("submission_returncode") is not None:
            lines.append("Submission return: " + str(task["submission_returncode"]))
    total_tasks = int(metadata.get("task_count", len(tasks)))
    if total_tasks > 3:
        lines.append("{} additional native tasks; choose a log below to inspect its output.".format(total_tasks - 3))
    for warning in (metadata.get("warnings") or [])[:2]:
        lines.append("Telemetry: " + _compact_diagnostic(str(warning), 300))
    failure_stage = metadata.get("failure_stage_label")
    if failure_stage:
        lines.append("Failure stage: {}".format(failure_stage))
    reason = _compact_diagnostic(metadata.get("failure_reason"), 720)
    if reason:
        lines.append("Reason: {}".format(reason))
    return "\n".join(lines)


def _diagnostic_ladder_text(payload):
    """Render ordered stage outcomes without inferring unreported success."""
    stages = (("maestro_baseline", "原生 Baseline"),
              ("private_netlist", "私有网表 CLI"),
              ("maestro_netlist", "每候选原生生成网表 CLI"))
    labels = {"queued": "待执行", "pending": "待执行", "running": "运行中",
              "completed": "通过", "pass": "通过", "failed": "失败", "fail": "失败",
              "skipped": "跳过", "blocked": "阻塞", "cancelled": "已停止"}
    cases = payload.get("case_results") or []
    lines = []
    for number, (engine, title) in enumerate(stages, 1):
        case = next((row for row in cases if isinstance(row, dict) and row.get("engine") == engine), {})
        state = str(case.get("state") or "pending").lower()
        simulator = case.get("simulator")
        cause = case.get("first_cause") or {}
        if not isinstance(cause, dict):
            cause = {"message": str(cause)}
        reason = case.get("skip_reason") or cause.get("message") or cause.get("error") or cause.get("failure_stage")
        line = "{}. {}：{}".format(number, title, labels.get(state, state))
        if case.get("simulator_identity_known") is False:
            line += " [求解器未知]"
        elif simulator:
            line += " [{}]".format(str(simulator).upper())
        if reason:
            line += " — " + _compact_diagnostic(reason, 250)
        lines.append(line)
        contexts = payload.get("case_contexts") or {}
        if isinstance(contexts, list):
            contexts = {item.get("case_id"): item for item in contexts if isinstance(item, dict)}
        if state in ("failed", "fail", "blocked") and isinstance(contexts, dict):
            observation = worker_error_text(contexts.get(case.get("case_id")))
            if observation:
                lines.append("   " + observation)
    return "\n".join(lines)


class ScrollableFrame(ttk.Frame):
    def __init__(self, parent, **kwargs):
        ttk.Frame.__init__(self, parent, **kwargs)
        self.canvas = tk.Canvas(
            self, background=BACKGROUND, highlightthickness=0, borderwidth=0,
        )
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.body = ttk.Frame(self.canvas, style="Page.TFrame")
        self.window = self.canvas.create_window((0, 0), window=self.body, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        self._body_size = None
        self._canvas_width = None
        self.body.bind("<Configure>", self._configure_scroll)
        self.canvas.bind("<Configure>", self._configure_width)

    def _configure_scroll(self, _event=None):
        if _event is not None:
            size = (_event.width, _event.height)
            if size == self._body_size:
                return
            self._body_size = size
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _configure_width(self, event):
        if event.width == self._canvas_width:
            return
        self._canvas_width = event.width
        self.canvas.itemconfigure(self.window, width=event.width)

    def scroll_units(self, units):
        self.canvas.yview_scroll(int(units), "units")


class ParameterViewSwitch(tk.Canvas):
    """Small Tk 8.5-compatible segmented slider for parameter grouping."""

    def __init__(self, parent, variable, command=None):
        self.variable = variable
        self.changed = command
        tk.Canvas.__init__(
            self, parent, width=168, height=28, background=PANEL,
            highlightthickness=0, borderwidth=0, takefocus=1,
        )
        self.bind("<Button-1>", self._clicked)
        self.bind("<space>", self._toggle)
        self.bind("<Return>", self._toggle)
        self.variable.trace("w", lambda *_args: self._draw())
        self._draw()

    def _clicked(self, event):
        self.set_mode("hierarchy" if event.x < 84 else "name")
        self.focus_set()
        return "break"

    def _toggle(self, _event=None):
        self.set_mode("name" if self.variable.get() == "hierarchy" else "hierarchy")
        return "break"

    def set_mode(self, mode):
        if mode not in ("hierarchy", "name") or mode == self.variable.get():
            return
        self.variable.set(mode)
        if self.changed:
            self.changed()

    def _draw(self):
        self.delete("all")
        selected = self.variable.get() if self.variable.get() in ("hierarchy", "name") else "hierarchy"
        self.create_rectangle(1, 1, 167, 27, fill="#e5edf4", outline="#c7d5e0")
        left = (2, 2, 84, 26) if selected == "hierarchy" else (84, 2, 166, 26)
        self.create_rectangle(*left, fill=ACCENT, outline=ACCENT)
        self.create_text(
            43, 14, text="Hierarchy",
            fill="#ffffff" if selected == "hierarchy" else MUTED,
        )
        self.create_text(
            125, 14, text="By Name",
            fill="#ffffff" if selected == "name" else MUTED,
        )


class CornerPicker(ttk.Button):
    """A persistent, scrollable multi-select popup for one metric."""
    def __init__(self, parent, corners, command=None):
        self.corners = list(corners)
        self.changed = command
        self.label_var = tk.StringVar()
        self.popup = None
        self.variables = [(name, tk.BooleanVar(value=True)) for name in self.corners]
        ttk.Button.__init__(self, parent, textvariable=self.label_var, width=19, command=self._toggle_popup)
        self._updated()

    def _toggle_popup(self):
        if self.popup is not None:
            self._close_popup()
            return
        popup = self.popup = tk.Toplevel(self)
        popup.withdraw(); popup.overrideredirect(True); popup.transient(self.winfo_toplevel())
        frame = ttk.Frame(popup, relief='solid', borderwidth=1); frame.pack(fill='both', expand=True)
        actions = ttk.Frame(frame); actions.pack(fill='x')
        ttk.Button(actions, text='All', command=self.select_all).pack(side='left')
        ttk.Button(actions, text='None', command=self.select_none).pack(side='left')
        canvas = tk.Canvas(frame, height=min(220, max(35, len(self.corners) * 26)), width=220, highlightthickness=0)
        scroll = ttk.Scrollbar(frame, orient='vertical', command=canvas.yview)
        canvas.configure(yscrollcommand=scroll.set); scroll.pack(side='right', fill='y'); canvas.pack(fill='both', expand=True)
        inner = ttk.Frame(canvas); window = canvas.create_window((0, 0), window=inner, anchor='nw')
        inner.bind('<Configure>', lambda event: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>', lambda event: canvas.itemconfigure(window, width=event.width))
        first = None
        for corner, variable in self.variables:
            check = ttk.Checkbutton(inner, text=corner, variable=variable, command=self._updated)
            check.pack(anchor='w', fill='x')
            if first is None: first = check
        popup.geometry('+%d+%d' % (self.winfo_rootx(), self.winfo_rooty() + self.winfo_height()))
        popup.bind('<Escape>', lambda event: self._close_popup())
        popup.bind('<ButtonPress-1>', self._outside_click, add='+')
        popup.deiconify(); popup.grab_set()
        (first or popup).focus_force()

    def _outside_click(self, event):
        popup = self.popup
        if popup is None: return
        x, y = event.x_root, event.y_root
        if not (popup.winfo_rootx() <= x < popup.winfo_rootx() + popup.winfo_width() and
                popup.winfo_rooty() <= y < popup.winfo_rooty() + popup.winfo_height()):
            self._close_popup()
            return 'break'

    def _close_popup(self):
        popup, self.popup = self.popup, None
        if popup is not None:
            try: popup.grab_release(); popup.destroy(); self.focus_set()
            except tk.TclError: pass

    def selected(self):
        return [name for name, variable in self.variables if variable.get()]

    def set_selected(self, selected):
        selected = set(selected if selected is not None else self.corners)
        for name, variable in self.variables: variable.set(name in selected)
        self._updated()

    def select_all(self):
        for _name, variable in self.variables: variable.set(True)
        self._updated()

    def select_none(self):
        for _name, variable in self.variables: variable.set(False)
        self._updated()

    def _updated(self):
        count = len(self.selected())
        text = ('All corners ({})'.format(count) if count and count == len(self.corners) else
                '{} corner{}'.format(count, '' if count == 1 else 's') if count else 'No corners')
        self.label_var.set(text)
        if self.changed: self.changed()

    def destroy(self):
        self.changed = None
        self._close_popup()
        ttk.Button.destroy(self)


class MetricRow(ttk.Frame):
    def __init__(self, parent, kind, outputs, corners, remove_command, change_command,
                 definition=None):
        ttk.Frame.__init__(self, parent, style="Card.TFrame", padding=(8, 6))
        self.kind = kind
        self.remove_command = remove_command
        self.change_command = change_command
        definition = definition or {}
        output_values = list(outputs) or [""]
        self.output_var = tk.StringVar(value=definition.get("output_key") or output_values[0])
        self.aggregation_var = tk.StringVar(value=definition.get("aggregation") or "mean")
        self.mode_var = tk.StringVar(value=(
            definition.get("direction") or "minimize" if kind == "objective" else
            definition.get("operator") or "<="
        ))
        self.value_var = tk.StringVar(value=str(
            definition.get("target", "") if kind == "objective" else definition.get("limit", "")
        ))
        output = ttk.Combobox(
            self, textvariable=self.output_var, values=output_values, state="readonly", width=24,
        )
        ttk.Label(self, text="Output", style="CardMuted.TLabel").grid(row=0, column=0, sticky="w")
        output.grid(row=1, column=0, padx=(0, 8), sticky="ew")
        aggregation = ttk.Combobox(
            self, textvariable=self.aggregation_var,
            values=("mean", "min", "max", "first", "last"), state="readonly", width=9,
        )
        ttk.Label(self, text="Aggregation", style="CardMuted.TLabel").grid(row=0, column=1, sticky="w")
        aggregation.grid(row=1, column=1, padx=(0, 8), sticky="ew")
        self.corner_picker = CornerPicker(self, corners, command=self._changed)
        ttk.Label(self, text="Corners", style="CardMuted.TLabel").grid(row=0, column=2, sticky="w")
        self.corner_picker.grid(row=1, column=2, sticky="ew")
        self.corner_picker.set_selected(definition.get("corners", corners))
        if kind == "objective":
            modes = ("minimize", "maximize", "target")
        else:
            modes = ("<=", ">=")
        mode = ttk.Combobox(
            self, textvariable=self.mode_var, values=modes, state="readonly", width=11,
        )
        ttk.Label(self, text="Direction" if kind == "objective" else "Operator", style="CardMuted.TLabel").grid(row=2, column=0, sticky="w", pady=(8, 0))
        mode.grid(row=3, column=0, padx=(0, 8), sticky="ew")
        self.value_entry = ttk.Entry(self, textvariable=self.value_var, width=14, style="Crescendo.Input.TEntry")
        value_label = "Target (only for target direction)" if kind == "objective" else "Limit"
        ttk.Label(self, text=value_label, style="CardMuted.TLabel").grid(row=2, column=1, sticky="w", pady=(8, 0))
        self.value_entry.grid(row=3, column=1, padx=(0, 8), sticky="ew")
        ttk.Button(
            self, text="Remove", style="Danger.TButton",
            command=lambda: self.remove_command(self),
        ).grid(row=3, column=2, sticky="e")
        self.columnconfigure(0, weight=2)
        self.columnconfigure(1, weight=1)
        self.columnconfigure(2, weight=1)
        output.bind("<<ComboboxSelected>>", self._changed)
        aggregation.bind("<<ComboboxSelected>>", self._changed)
        mode.bind("<<ComboboxSelected>>", self._mode_changed)
        self.value_entry.bind("<FocusOut>", self._changed)
        self._mode_changed()

    def _mode_changed(self, _event=None):
        if self.kind == "objective":
            enabled = self.mode_var.get() == "target"
            self.value_entry.configure(state="normal" if enabled else "disabled")
        self._changed()

    def _changed(self, _event=None):
        if self.change_command:
            self.change_command()

    def definition(self, label):
        corners = self.corner_picker.selected()
        if not corners:
            raise ValueError("{} has no corner selected.".format(label))
        definition = {
            "output_key": self.output_var.get(),
            "aggregation": self.aggregation_var.get(),
            "corners": corners,
        }
        if self.kind == "objective":
            definition["direction"] = self.mode_var.get()
            if definition["direction"] == "target":
                if parse_physical_input(self.value_var.get()) is None:
                    raise ValueError("{} target must be numeric.".format(label))
                definition["target"] = self.value_var.get().strip()
        else:
            definition["operator"] = self.mode_var.get()
            if parse_physical_input(self.value_var.get()) is None:
                raise ValueError("{} limit must be numeric.".format(label))
            definition["limit"] = self.value_var.get().strip()
        return definition

    def destroy(self):
        self.remove_command = None
        self.change_command = None
        ttk.Frame.destroy(self)
        self.corner_picker = None
        self.value_entry = None
        for name in ("output_var", "aggregation_var", "mode_var", "value_var"):
            setattr(self, name, None)


class RangeIssueWindow(tk.Toplevel):
    def __init__(self, app, issues):
        tk.Toplevel.__init__(self, app.root)
        self.app = app
        self.title("Parameter Range Problems")
        self.geometry("900x360")
        self.transient(app.root)
        ttk.Label(
            self,
            text="Fix these values before callback validation or optimization.",
            style="PageTitle.TLabel",
        ).pack(anchor="w", padx=16, pady=(14, 8))
        tree = ttk.Treeview(
            self, columns=("field", "problem"), show="tree headings", selectmode="browse",
        )
        tree.heading("#0", text="Device / Parameter")
        tree.heading("field", text="Field")
        tree.heading("problem", text="Problem")
        tree.column("#0", width=480, stretch=True)
        tree.column("field", width=100, stretch=False)
        tree.column("problem", width=280, stretch=True)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side="left", fill="both", expand=True, padx=(16, 0), pady=(0, 16))
        scrollbar.pack(side="right", fill="y", padx=(0, 16), pady=(0, 16))
        self.issue_by_item = {}
        for issue in issues:
            item = tree.insert(
                "", "end", text=issue_display_name(issue),
                values=(issue.get("field") or "", issue.get("message") or "Invalid value"),
            )
            self.issue_by_item[item] = issue
        tree.bind("<Double-1>", self._open_issue)
        tree.bind("<Return>", self._open_issue)
        self.tree = tree

    def _open_issue(self, _event=None):
        selected = self.tree.selection()
        if not selected:
            return
        issue = self.issue_by_item.get(selected[0])
        if issue and issue.get("parameter_id"):
            self.app.focus_parameter(issue["parameter_id"], issue.get("field"))
            self.destroy()


class ParetoPlot(ttk.Frame):
    def __init__(self, parent, app, left_definition, right_definition):
        ttk.Frame.__init__(self, parent, style="Card.TFrame", padding=8)
        self.app = app
        self.left_definition = left_definition
        self.right_definition = right_definition
        ttk.Label(
            self,
            text="{}  vs  {}".format(
                metric_definition_label(left_definition),
                metric_definition_label(right_definition),
            ),
            style="CardTitle.TLabel",
            wraplength=780,
        ).pack(anchor="w", pady=(0, 6))
        self.canvas = tk.Canvas(
            self, width=780, height=320, background="#fbfdff",
            highlightbackground="#d5e0e8", highlightthickness=1,
        )
        self.canvas.pack(fill="x", expand=True)

    def render(self, points):
        canvas = CachedCanvas(self.canvas)
        canvas.delete("all")
        width = max(420, canvas.winfo_width() or 780)
        height = 320
        left, right, top, bottom = 72, width - 24, 28, height - 58
        if not points:
            for item, binding in getattr(self, "_point_bindings", {}).items():
                canvas.tag_unbind(item, "<Button-1>", binding[1])
            self._point_bindings = {}
            canvas.create_text(
                width / 2, height / 2, text="Waiting for completed candidates...",
                fill=MUTED,
            )
            canvas.finish()
            return
        x_values = [point["x"] for point in points]
        y_values = [point["y"] for point in points]
        x_min, x_max = min(x_values), max(x_values)
        y_min, y_max = min(y_values), max(y_values)
        x_pad = (x_max - x_min or max(abs(x_max), 1.0)) * 0.08
        y_pad = (y_max - y_min or max(abs(y_max), 1.0)) * 0.08
        x_min, x_max = x_min - x_pad, x_max + x_pad
        y_min, y_max = y_min - y_pad, y_max + y_pad

        def sx(value):
            return left + (value - x_min) / (x_max - x_min) * (right - left)

        def sy(value):
            return bottom - (value - y_min) / (y_max - y_min) * (bottom - top)

        for index in range(6):
            x_value = x_min + (x_max - x_min) * index / 5.0
            y_value = y_min + (y_max - y_min) * index / 5.0
            x = sx(x_value)
            y = sy(y_value)
            canvas.create_line(x, top, x, bottom, fill="#e7edf2")
            canvas.create_line(left, y, right, y, fill="#e7edf2")
            canvas.create_text(x, bottom + 16, text="{:.4g}".format(x_value), fill=MUTED)
            canvas.create_text(left - 8, y, text="{:.4g}".format(y_value), fill=MUTED, anchor="e")
        canvas.create_line(left, bottom, right, bottom, fill=TEXT)
        canvas.create_line(left, top, left, bottom, fill=TEXT)
        canvas.create_text(
            (left + right) / 2, height - 15,
            text=metric_definition_label(self.left_definition), fill=TEXT,
        )
        try:
            canvas.create_text(14, (top + bottom) / 2,
                               text=metric_definition_label(self.right_definition), fill=TEXT, angle=90)
        except tk.TclError as error:
            if "-angle" not in str(error):
                raise
            # Tk 8.5 on the supported CentOS/Python 3.6 deployment predates
            # rotated Canvas text. Keep the metric label readable above the plot.
            canvas.create_text(left, 6, text="Y: " + metric_definition_label(self.right_definition),
                               fill=TEXT, anchor="nw")
        bindings = getattr(self, "_point_bindings", {})
        active_items = set()
        for point in points:
            fill = "#b8c1c8" if not point["feasible"] else PALETTE[point["generation"] % len(PALETTE)]
            outline = "#25313c" if point["pareto"] else fill
            radius = 6 if point["pareto"] else 4
            tag = "candidate:{}".format(point["candidate_id"])
            item = canvas.create_oval(
                sx(point["x"]) - radius, sy(point["y"]) - radius,
                sx(point["x"]) + radius, sy(point["y"]) + radius,
                fill=fill, outline=outline, width=2 if point["pareto"] else 1,
                tags=(tag, "point"),
            )
            active_items.add(item)
            old = bindings.get(item)
            if old is None or old[0] != point["candidate_id"]:
                if old is not None:
                    canvas.tag_unbind(item, "<Button-1>", old[1])
                command = canvas.tag_bind(item, "<Button-1>", lambda _event, candidate_id=point["candidate_id"]: self.app.select_candidate(candidate_id))
                bindings[item] = (point["candidate_id"], command)
        for item in set(bindings) - active_items:
            canvas.tag_unbind(item, "<Button-1>", bindings.pop(item)[1])
        self._point_bindings = bindings
        canvas.finish()


class LiveLogViewer(tk.Toplevel):
    MAX_TAB_CHARACTERS = 2 * 1024 * 1024
    MAX_OPEN_TABS = 16

    def __init__(self, app, run_id, candidate_id):
        tk.Toplevel.__init__(self, app.root)
        self.app = app
        self.run_id = run_id
        self.candidate_id = candidate_id
        self.title("Candidate Diagnostics · {}".format(candidate_id))
        self.geometry("1000x700")
        self.protocol("WM_DELETE_WINDOW", self._close)
        toolbar = ttk.Frame(self, padding=(10, 8))
        toolbar.pack(fill="x")
        self.status_var = tk.StringVar(value="Discovering candidate diagnostics...")
        ttk.Label(toolbar, textvariable=self.status_var, style="Muted.TLabel").pack(side="left")
        self.follow_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(toolbar, text="Auto follow", variable=self.follow_var).pack(side="right")
        self.diagnostic_var = tk.StringVar(
            value="Candidate status and failure location are loading...",
        )
        ttk.Label(
            self, textvariable=self.diagnostic_var, style="Muted.TLabel",
            wraplength=960, justify="left",
        ).pack(fill="x", padx=12, pady=(0, 8))
        selector = ttk.Frame(self)
        selector.pack(fill="x", padx=10, pady=(0, 8))
        ttk.Label(selector, text="Log").pack(side="left", padx=(0, 8))
        self.log_choice_var = tk.StringVar(value="Waiting for log creation...")
        self.log_choice = ttk.Combobox(selector, textvariable=self.log_choice_var, state="readonly")
        self.log_choice.pack(side="left", fill="x", expand=True)
        self.log_choice.bind("<<ComboboxSelected>>", self._select_log)
        self.log_location_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self.log_location_var, style="Muted.TLabel",
                  wraplength=960).pack(fill="x", padx=12, pady=(0, 6))
        self.log_choices = []
        self.log_choice_signature = None
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.notebook.bind("<<NotebookTabChanged>>", lambda _event: self._schedule_poll(0))
        self.tabs = {}
        self.spectre_seen = False
        self.closed = False
        self.inflight = False
        self.poll_job = None
        self._schedule_poll(50)

    def _close(self):
        if self.closed:
            return
        self.closed = True
        viewers = getattr(self.app, "log_viewers", None)
        if viewers is not None and self in viewers:
            viewers.remove(self)
        identifier = getattr(self, "poll_job", None)
        if identifier is not None:
            try:
                self.after_cancel(identifier)
            except tk.TclError:
                pass
            self.poll_job = None
        self.destroy()

    def _schedule_poll(self, milliseconds):
        if self.closed:
            return
        identifier = getattr(self, "poll_job", None)
        if identifier is not None:
            try:
                self.after_cancel(identifier)
            except tk.TclError:
                pass
        self.poll_job = self.after(milliseconds, self._poll)

    def _select_log(self, _event=None):
        index = self.log_choice.current()
        if self.closed or not 0 <= index < len(self.log_choices):
            return
        tab = self._ensure_tab(self.log_choices[index])
        self.log_location_var.set(self.log_choices[index].get("location") or "")
        self.notebook.select(tab["frame"])
        tab["last_read"] = time.monotonic()
        self._schedule_poll(0)

    def _poll(self):
        self.poll_job = None
        if self.closed or not self.winfo_exists() or self.inflight:
            return
        self.inflight = True
        selected_frame = str(self.notebook.select() or "")
        selected_log_id = next(
            (log_id for log_id, tab in self.tabs.items()
             if str(tab.get("frame")) == selected_frame),
            None,
        )
        tab_states = dict(
            (log_id, {
                "offset": tab.get("offset", 0),
                "file_token": tab.get("file_token"),
            })
            for log_id, tab in self.tabs.items()
        )

        def load():
            if self.closed:
                return {"state": "closed", "logs": []}, []
            metadata = self.app.service.candidate_logs(self.run_id, self.candidate_id)
            chunks = []
            descriptors = metadata.get("logs") or []
            selected = next(
                (row for row in descriptors if row.get("log_id") == selected_log_id),
                descriptors[0] if descriptors else None,
            )
            if selected is not None:
                log_id = selected.get("log_id")
                tab = tab_states.get(log_id) or {}
                kwargs = {
                    "offset": tab.get("offset", max(0, int(selected.get("size_bytes") or 0) - 262144)),
                    "max_bytes": 262144,
                }
                if not tab.get("file_token") and not tab.get("offset"):
                    kwargs["offset"] = max(0, int(selected.get("size_bytes") or 0) - 262144)
                if tab.get("file_token") is not None:
                    kwargs["file_token"] = tab.get("file_token")
                try:
                    chunk = self.app.service.read_candidate_log_chunk(
                        self.run_id, self.candidate_id, log_id, **kwargs
                    )
                except TypeError:
                    kwargs.pop("file_token", None)
                    chunk = self.app.service.read_candidate_log_chunk(
                        self.run_id, self.candidate_id, log_id, **kwargs
                    )
                chunks.append((selected, chunk))
            return metadata, chunks

        submit = (self.app.tasks.submit_monitor if callable(getattr(type(self.app.tasks), "submit_monitor", None))
                  else self.app.tasks.submit)
        submit(load, self._loaded, self._failed)

    def _ensure_tab(self, descriptor):
        log_id = descriptor.get("log_id")
        label = descriptor.get("label") or log_id or "Spectre"
        if str(descriptor.get("kind") or "").startswith(("spectre", "alps")):
            identity = []
            if descriptor.get("corner") not in (None, ""):
                identity.append(str(descriptor["corner"]))
            if descriptor.get("point_id") not in (None, ""):
                identity.append("point " + str(descriptor["point_id"]))
            if identity:
                label = " · ".join(identity) + " · " + str(descriptor.get("test") or "Simulator")[-30:]
                if descriptor.get("file_name"):
                    label += " · " + descriptor["file_name"]
        tab = self.tabs.get(log_id)
        if tab:
            if tab.get("label") != label:
                self.notebook.tab(tab["frame"], text=label[:64])
                tab["label"] = label
            return tab
        if len(self.tabs) >= self.MAX_OPEN_TABS:
            selected = str(self.notebook.select() or "")
            inactive = [(key, row) for key, row in self.tabs.items() if str(row["frame"]) != selected]
            if inactive:
                old_id, old_tab = min(inactive, key=lambda item: item[1].get("last_read", 0))
                self.notebook.forget(old_tab["frame"])
                old_tab["frame"].destroy()
                del self.tabs[old_id]
        frame = ttk.Frame(self.notebook)
        text = tk.Text(
            frame, wrap="none", state="disabled", background="#10171e",
            foreground="#d9e6ef", insertbackground="#d9e6ef",
            font=("TkFixedFont", 10), undo=False,
        )
        y_scroll = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        x_scroll = ttk.Scrollbar(frame, orient="horizontal", command=text.xview)
        text.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        text.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        self.notebook.add(frame, text=label[:64])
        tab = {
            "frame": frame, "text": text, "offset": 0,
            "file_token": None, "characters": 0,
            "kind": descriptor.get("kind"),
            "label": label,
            "last_read": time.monotonic(),
        }
        self.tabs[log_id] = tab
        return tab

    @staticmethod
    def _replace_text(tab, value):
        widget = tab["text"]
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        if value:
            widget.insert("end", value)
        widget.configure(state="disabled")
        tab["characters"] = len(value)

    def _append_text(self, tab, value):
        widget = tab["text"]
        widget.configure(state="normal")
        widget.insert("end", value)
        tab["characters"] = int(tab.get("characters") or 0) + len(value)
        if tab["characters"] > self.MAX_TAB_CHARACTERS:
            retained = widget.get("1.0", "end-1c")[-self.MAX_TAB_CHARACTERS:]
            notice = "[Earlier log text discarded to bound UI memory.]\n"
            widget.delete("1.0", "end")
            widget.insert("end", notice + retained)
            tab["characters"] = len(notice) + len(retained)
        widget.configure(state="disabled")

    def _loaded(self, payload):
        if self.closed:
            return
        self.inflight = False
        metadata, chunks = payload
        if hasattr(self, "diagnostic_var"):
            self.diagnostic_var.set(_candidate_diagnostic_text(metadata))
        descriptors = metadata.get("logs") or []
        signature = tuple((row.get("log_id"), row.get("label")) for row in descriptors)
        if signature != self.log_choice_signature:
            self.log_choice_signature = signature
            self.log_choices = descriptors
            self.log_choice.configure(values=tuple("{} · {} · {}".format(index + 1, row.get("kind") or "log",
                                                    row.get("label") or row.get("log_id")) for index, row in enumerate(descriptors)))
        initial = not self.tabs
        for descriptor in descriptors[:self.MAX_OPEN_TABS] if initial else descriptors:
            if initial or descriptor.get("log_id") in self.tabs:
                self._ensure_tab(descriptor)
        spectre = next((row for row in metadata.get("logs") or []
                        if str(row.get("kind") or "").startswith(("spectre", "alps"))), None)
        if spectre is not None and not getattr(self, "spectre_seen", False):
            self.spectre_seen = True
            self.notebook.select(self._ensure_tab(spectre)["frame"])
        selected = str(self.notebook.select() or "")
        selected_id = next((key for key, tab in self.tabs.items() if str(tab["frame"]) == selected), None)
        selected_index = next((index for index, row in enumerate(descriptors) if row.get("log_id") == selected_id), None)
        if selected_index is not None:
            self.log_choice.current(selected_index)
            if hasattr(self, "log_location_var"):
                self.log_location_var.set(descriptors[selected_index].get("location") or "")
        state = metadata.get("state") or "waiting"
        if not chunks:
            if state == "not_started":
                self.status_var.set(
                    metadata.get("message") or "The simulator was not started; see the candidate failure summary above."
                )
            elif state == "removed":
                self.status_var.set(
                    metadata.get("message") or
                    "No live or retained simulator log is available."
                )
            else:
                self.status_var.set(metadata.get("message") or "Waiting for simulator log creation...")
        else:
            self.status_var.set("{} log{} · {}".format(
                len(metadata.get("logs") or []), "" if len(metadata.get("logs") or []) == 1 else "s", state,
            ))
            if chunks[0][0].get("label"):
                self.status_var.set(_compact_diagnostic(chunks[0][0]["label"], 120) + " · " + state)
        for descriptor, chunk in chunks:
            tab = self._ensure_tab(descriptor)
            tab["last_read"] = time.monotonic()
            widget = tab["text"]
            if chunk.get("reset"):
                self._replace_text(tab, "")
            text = chunk.get("text") or chunk.get("data") or ""
            if text:
                self._append_text(tab, text)
                if self.follow_var.get():
                    widget.see("end")
            tab["offset"] = int(chunk.get("next_offset", tab.get("offset", 0)))
            if "file_token" in chunk:
                tab["file_token"] = chunk.get("file_token")
            if chunk.get("removed"):
                self.status_var.set("The log file is no longer available; displayed text remains in memory.")
            elif chunk.get("waiting"):
                self.status_var.set("Waiting for simulator log data...")
        if not self.closed:
            self._schedule_poll(500)

    def _failed(self, error):
        if self.closed:
            return
        self.inflight = False
        self.status_var.set("{}: {}".format(error.__class__.__name__, error))
        self._schedule_poll(1000)


class FastTreeview(ttk.Treeview):
    def insert(self, parent, index, iid=None, **options):
        # ttk's script=False option encoding, including string-valued Tcl lists.
        arguments = []
        for key, value in options.items():
            arguments.append("-" + key)
            if value is not None:
                arguments.append(tk._join(value) if isinstance(value, (tuple, list)) else value)
        if iid is not None:
            arguments[0:0] = ["-id", iid]
        return self.tk.call(self._w, "insert", parent, index, *arguments)


class OptimizerGui(object):
    def __init__(self, root, service, launch_context=None):
        self.root = root
        self.service = service
        self.launch_context = copy.deepcopy(launch_context) if launch_context else None
        self.active_launch_context = copy.deepcopy(self.launch_context)
        self.active_setup_path = ((self.launch_context or {}).get("source") or {}).get("setup_path")
        self.setup_selection_epoch = 0
        self.cell_inspection_id = None
        self.cell_inspection = None
        self.cell_scopes = {}
        self.selected_cell_scope_ids = set()
        self.applied_cell_scope_ids = set()
        self.cell_scope_dirty = False
        self.cell_render_epoch = 0
        self.cell_render_job = None
        self.launch_state = {}
        self.setup_loading = False
        self.test_inspection_id = None
        self.test_catalog = None
        self.selected_test = None
        self.test_load_epoch = 0
        self.history_read_epoch = 0
        self.history_cancel_event = None
        self.history_choices = {}
        self.tasks = None
        self.prepared_id = None
        self.prepared = None
        self.prepared_epoch = 0
        self.eda_revalidation_required = False
        self.eda_edit_draft_path = None
        self.eda_auto_revalidation = None
        self.parameter_configs = {}
        self.parameter_by_id = {}
        self.parameter_items = {}
        self.node_parameters = {}
        self.parameter_item_labels = {}
        self.node_item_labels = {}
        self.parameter_open_items_by_view = {"hierarchy": set(), "name": set()}
        self.parameter_views_rendered = set()
        self.rendered_parameter_view = None
        self.pending_parameter_click = None
        self.pending_parameter_click_job = None
        self.pending_parameter_focus = None
        self.current_page = None
        self.recent_progress_signature = None
        self.recent_run_resumable = {}
        self.objective_rows = []
        self.constraint_rows = []
        self.pending_config = None
        self.pending_checkpoint = None
        self.run_ui = RunUiState()
        self.live_spec = {"objectives": [], "constraints": []}
        self.live_candidates = []
        self.display_dataset = None
        self.dataset_load_epoch = 0
        self.progress_revision = 0
        self._live_candidate_map = {}
        self.selected_candidate_id = None
        self.progress_inflight = False
        self.notified_run_failures = set()
        self.execution_console_signature = None
        self.range_validation_inflight = False
        self.range_validation_job_id = None
        self.range_validation_results = {}
        self.range_validation_requested = {}
        self.range_validation_detail = "Optional PDK callback validation has not run."
        self.last_diagnostic_detail = "No diagnostic report yet."
        self.pending_settings_state = None
        self.configuration_document = None
        self.pending_configuration_document = None
        self.pending_configuration_settings = None
        self.configuration_loading = False
        self.active_private_suite = None
        self.settings_load_epoch = 0
        self.start_request_pending = False
        self.environment_test_id = None
        self.environment_test_state = "idle"
        self.environment_test_epoch = 0
        self.environment_test_inflight = False
        self.environment_test_poll_job = None
        self.environment_test_payload = {}
        self.environment_test_window = None
        self.environment_test_log_text = None
        self.environment_test_log_engine = None
        self.last_single_algorithm = "differential_evolution"
        self.pareto_ids = set()
        self.log_viewers = []
        self.closing = False
        self.page_buttons = {}
        self.pages = {}
        self.result_page = 0
        self.result_page_size = 250
        self._configure_root()
        self._configure_styles()
        self._build_shell()
        self._bind_page_mousewheel()
        self.show_page("Design Setup")
        self.root.protocol("WM_DELETE_WINDOW", self.request_exit)
        self.root.report_callback_exception = self._callback_exception
        self.tasks = BackgroundTaskQueue(root, observer=self._activity_task_event)
        try:
            self._load_eda_setup()
            if self.launch_context:
                self._write_launch_state("ui_ready")
                self.root.after(0, self._load_launch_setup)
        except BaseException:
            self.tasks.close()
            raise

    def _configure_root(self):
        title = "Cadence ADE Circuit Optimizer"
        if self.launch_context:
            source = self.launch_context.get("source") or {}
            title += " — {}/{}/{}".format(
                source.get("library", ""), source.get("cell", ""), source.get("view", ""),
            )
        self.root.title(title)
        self.root.geometry("1380x860")
        self.root.minsize(1080, 680)
        self.root.configure(background=BACKGROUND)

    def _configure_styles(self):
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("TFrame", background=BACKGROUND)
        style.configure("Page.TFrame", background=BACKGROUND)
        style.configure("Card.TFrame", background=PANEL, relief="flat")
        style.configure("Sidebar.TFrame", background=SIDEBAR)
        style.configure("TLabel", background=BACKGROUND, foreground=TEXT)
        style.configure("Muted.TLabel", background=BACKGROUND, foreground=MUTED)
        style.configure("Card.TLabel", background=PANEL, foreground=TEXT)
        style.configure("CardMuted.TLabel", background=PANEL, foreground=MUTED)
        style.configure("Help.TLabel", background=PANEL, foreground=MUTED, font=("TkDefaultFont", 9))
        style.configure("PageTitle.TLabel", background=BACKGROUND, foreground=TEXT, font=("TkDefaultFont", 17, "bold"))
        style.configure("CardTitle.TLabel", background=PANEL, foreground=TEXT, font=("TkDefaultFont", 11, "bold"))
        style.configure("Status.TLabel", background="#edf3f7", foreground=TEXT, padding=(10, 7))
        style.configure("TButton", padding=(9, 6), background="#dce7ef", foreground=TEXT)
        style.map("TButton", background=[("active", "#cbdde9")])
        style.configure("Accent.TButton", background=ACCENT, foreground="#ffffff")
        style.map("Accent.TButton", background=[("active", "#476f96"), ("disabled", "#aebdca")])
        style.configure("Danger.TButton", background="#eedcdf", foreground=DANGER)
        style.map("Danger.TButton", background=[("active", "#e3c9cd")])
        style.configure("Sidebar.TButton", background=SIDEBAR, foreground=TEXT, anchor="w", padding=(14, 10), relief="flat")
        style.map("Sidebar.TButton", background=[("active", "#dbe7f0")])
        style.configure("SidebarActive.TButton", background=ACCENT_LIGHT, foreground="#315a7c", anchor="w", padding=(14, 10), relief="flat")
        style.configure("Treeview", background=PANEL, fieldbackground=PANEL, foreground=TEXT, rowheight=25)
        style.configure("Treeview.Heading", background="#e8eff5", foreground=TEXT)
        style.map("Treeview", background=[("selected", "#d4e5f2")], foreground=[("selected", TEXT)])
        style.map("Crescendo.Input.TEntry", fieldbackground=[("disabled", "#e9eef2")],
                  foreground=[("disabled", "#687783")])
        style.configure("TNotebook", background=BACKGROUND, borderwidth=0)
        style.configure("TNotebook.Tab", padding=(10, 6))

    def _build_shell(self):
        header = ttk.Frame(self.root, style="Card.TFrame", padding=(18, 12))
        header.pack(fill="x")
        ttk.Label(
            header, text="Cadence ADE Circuit Optimizer", style="CardTitle.TLabel",
            font=("TkDefaultFont", 15, "bold"),
        ).pack(side="left")
        self.health_var = tk.StringVar(value="Native service ready")
        ttk.Label(header, textvariable=self.health_var, style="CardMuted.TLabel").pack(side="left", padx=18)
        ttk.Button(header, text="Exit", style="Danger.TButton", command=self.request_exit).pack(side="right")
        self.stop_button = ttk.Button(
            header, text="Stop", style="Danger.TButton",
            command=self.stop_run, state="disabled",
        )
        self.stop_button.pack(side="right", padx=(6, 0))
        self.start_button = ttk.Button(
            header, text="Start", style="Accent.TButton",
            command=self.start_run, state="disabled",
        )
        self.start_button.pack(side="right", padx=(12, 0))

        self._build_activity_panel()

        body = ttk.Frame(self.root)
        body.pack(fill="both", expand=True)
        sidebar = ttk.Frame(body, style="Sidebar.TFrame", width=225, padding=(8, 12))
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)
        content = ttk.Frame(body, style="Page.TFrame")
        content.pack(side="left", fill="both", expand=True)
        for name in PAGE_NAMES:
            button = ttk.Button(
                sidebar, text=("Settings" if name == "EDA Settings" else "Metrics" if name == "Objectives & Constraints" else name), style="Sidebar.TButton",
                command=lambda page_name=name: self.show_page(page_name),
            )
            button.pack(fill="x", pady=2)
            self.page_buttons[name] = button
            page = ScrollableFrame(content)
            self.pages[name] = page
        ttk.Separator(sidebar).pack(fill="x", pady=12)
        self.sidebar_status_var = tk.StringVar(value="No setup loaded")
        ttk.Label(
            sidebar, textvariable=self.sidebar_status_var, style="Muted.TLabel",
            wraplength=190, justify="left",
        ).pack(fill="x", padx=8)

        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Frame(self.root)
        status_bar.pack(fill="x", side="bottom", before=self.activity_frame)
        ttk.Button(status_bar, text="Details...", command=self.show_diagnostic_details).pack(side="right", padx=8)
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel", wraplength=950).pack(side="left", fill="x", expand=True)

        self._build_design_page(self.pages["Design Setup"].body)
        self._build_parameters_page(self.pages["Parameters"].body)
        self._build_metrics_page(self.pages["Objectives & Constraints"].body)
        self._build_optimization_page(self.pages["Optimization"].body)
        self._build_results_page(self.pages["Live Results"].body)
        self._build_pareto_page(self.pages["Pareto Trade-offs"].body)
        self._build_settings_page(self.pages["EDA Settings"].body)
        self._watch_activity_statuses()

    def _build_activity_panel(self):
        self.activity_journal = ActivityJournal()
        self.activity_cursor = 0
        self.activity_flush_job = None
        self.activity_display_first = None
        self.activity_wait_mode = None
        self.activity_finish_jobs = {}
        self.root.bind("<Destroy>", self._activity_destroyed, add="+")
        self.activity_frame = ttk.Frame(self.root, style="Card.TFrame", padding=(10, 4))
        self.activity_frame.pack(side="bottom", fill="x")
        line = ttk.Frame(self.activity_frame, style="Card.TFrame")
        line.pack(fill="x")
        ttk.Label(line, text="Activity Log", style="CardTitle.TLabel").pack(side="left")
        ttk.Label(line, text="UI stages, warnings and errors · detailed diagnostic reports remain separate", style="Help.TLabel").pack(side="left", padx=10)
        self.activity_wait_frame = ttk.Frame(line, style="Card.TFrame")
        self.activity_wait_var = tk.StringVar(value="")
        ttk.Label(self.activity_wait_frame, textvariable=self.activity_wait_var, style="Help.TLabel", width=36, anchor="e").pack(side="left", padx=6)
        self.progress_var = tk.DoubleVar(value=0.0)
        style = ttk.Style(self.root)
        style.configure("Crescendo.Horizontal.TProgressbar", background="#39e64d",
                        lightcolor="#39e64d", darkcolor="#39e64d", bordercolor="#ced8e0",
                        troughcolor="#e4eaf0")
        self.progress_bar = ttk.Progressbar(self.activity_wait_frame, variable=self.progress_var,
            maximum=1, length=170, mode="determinate", style="Crescendo.Horizontal.TProgressbar")
        self.progress_bar.pack(side="right")
        log_frame = ttk.Frame(self.activity_frame, style="Card.TFrame")
        log_frame.pack(fill="x", pady=(3, 0))
        self.activity_log = tk.Text(log_frame, height=5, wrap="word", state="disabled", undo=False,
                                    font=("TkFixedFont", 9), relief="flat", background="#fbfdff", foreground=TEXT)
        scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.activity_log.yview)
        self.activity_log.configure(yscrollcommand=scroll.set)
        self.activity_log.pack(side="left", fill="x", expand=True)
        scroll.pack(side="right", fill="y")
        self.activity_log.tag_configure("WARN", foreground=WARNING)
        self.activity_log.tag_configure("ERROR", foreground=DANGER)
        self._activity_event("Crescendo ready. Load and Inspect reads the current saved Maestro.")

    def _activity_destroyed(self, event):
        if event.widget is not self.root:
            return
        for identifier in getattr(self, "activity_finish_jobs", {}).values():
            try:
                self.root.after_cancel(identifier)
            except (tk.TclError, ValueError):
                pass
        for name in ("activity_flush_job", "service_activity_poll_job"):
            identifier = getattr(self, name, None)
            if identifier is not None:
                try:
                    self.root.after_cancel(identifier)
                except (tk.TclError, ValueError):
                    pass
                setattr(self, name, None)

    def _activity_event(self, message, level="INFO", key=None):
        journal = getattr(self, "activity_journal", None)
        if journal is None:
            return
        row = journal.append(message, level=level, key=key)
        if row is not None and getattr(self, "activity_flush_job", None) is None:
            self.activity_flush_job = self.root.after(50, self._flush_activity_log)

    def _flush_activity_log(self):
        self.activity_flush_job = None
        if not self.activity_log.winfo_exists():
            return
        rows = list(self.activity_journal.lines)
        if not rows:
            return
        fresh = [row for row in rows if row[0] > self.activity_cursor]
        if not fresh:
            return
        widget = self.activity_log
        position = widget.yview()
        following = not position or position[1] >= 0.99
        widget.configure(state="normal")
        first = getattr(self, "activity_display_first", None)
        if first is not None and rows[0][0] > first:
            removed = min(rows[0][0] - first, max(0, self.activity_cursor - first + 1))
            if removed:
                widget.delete("1.0", "{}.0".format(removed + 1))
        for revision, text, level in fresh:
            widget.insert("end", text, (level,))
        widget.configure(state="disabled")
        self.activity_cursor = rows[-1][0]
        self.activity_display_first = rows[0][0]
        if following:
            widget.see("end")

    def _activity_begin(self, token, label, total=None, completed=0, priority=20, gates=None):
        journal = getattr(self, "activity_journal", None)
        if journal is None:
            return
        pending = getattr(self, "activity_finish_jobs", {}).pop(token, None)
        if pending is not None:
            self.root.after_cancel(pending)
        previous = (journal.active.get(token) or {}).get("gates")
        journal.begin(token, label, total=total, completed=completed, priority=priority)
        self._activity_set_gates(token, gates, previous=previous)
        self._activity_render_wait()

    def _activity_update(self, token, label=None, total=None, completed=None, gates=None):
        journal = getattr(self, "activity_journal", None)
        if journal is not None:
            journal.update(token, label=label, total=total, completed=completed)
            self._activity_set_gates(token, gates)
            self._activity_render_wait()

    def _activity_set_gates(self, token, gates, previous=None):
        row = self.activity_journal.active.get(token)
        if row is None:
            return
        value = merge_gate_progress(previous or row.get("gates"), gates)
        if value:
            row.update(gates=value, completed=value["passed"], total=value["total"])

    def _activity_finish(self, token):
        # Briefly show the last proven value, including failures; no timer
        # advances it. A fully green bar requires an explicit completed gate.
        if token not in self.activity_journal.active:
            return
        jobs = getattr(self, "activity_finish_jobs", {})
        previous = jobs.pop(token, None)
        if previous is not None:
            self.root.after_cancel(previous)
        jobs[token] = self.root.after(650, lambda: self._activity_end(token))

    def _activity_end(self, token):
        journal = getattr(self, "activity_journal", None)
        if journal is not None:
            jobs = getattr(self, "activity_finish_jobs", {})
            previous = jobs.pop(token, None)
            if previous is not None:
                self.root.after_cancel(previous)
            journal.end(token)
            self._activity_render_wait()

    def _activity_render_wait(self):
        row = self.activity_journal.current()
        if row is None:
            self.progress_bar.stop()
            self.activity_wait_frame.pack_forget()
            self.activity_wait_mode = None
            return
        gates = row.get("gates")
        label = ("{}/{} · {}".format(gates["passed"], gates["total"],
                 gates.get("current") or row["label"]) if gates else row["label"])
        self.activity_wait_var.set(label if len(label) <= 43 else label[:42] + "…")
        self.activity_wait_frame.pack(side="right")
        total = row.get("total")
        mode = "determinate"
        if mode != self.activity_wait_mode:
            self.progress_bar.stop()
            self.progress_bar.configure(mode=mode)
            self.activity_wait_mode = mode
        known = isinstance(total, (int, float)) and total > 0
        self.progress_bar.configure(maximum=total if known else 1)
        self.progress_var.set(max(0, min(total, row.get("completed") or 0)) if known else 0)

    def _activity_task_event(self, event):
        name = str(event.get("name") or "")
        action = name.split(".<locals>")[0].split(".")[-1]
        quiet = action in ("_poll_progress", "_poll_environment_test", "_poll_range_validation",
                           "refresh_recent_runs", "_poll", "_poll_service_activity")
        labels = {
            "load_setup": "Load and Inspect", "load_configuration": "Restore configuration",
            "_load_configuration_document": "Restore configuration", "_load_eda_setup": "Detect EDA environment",
            "apply_selected_cells": "Inspect selected cells and parameters", "save_settings": "Save configuration",
            "load_settings": "Load configuration", "_match_eda_prepared": "Match retained settings",
            "_match_pending_settings_state": "Match saved settings", "start_run": "Start optimization",
            "stop_run": "Stop optimization", "start_environment_test": "Start diagnostics",
            "stop_environment_test": "Stop diagnostics", "save_eda_setup": "Validate EDA overrides",
            "reset_eda_setup": "Restore automatic EDA settings", "request_exit": "Stop owned processes and exit",
            "load_results": "Load results", "export_results": "Export results", "export_csv": "Export CSV",
            "export_optimized_schematic": "Create optimized schematic", "validate_ranges": "Validate parameter ranges",
            "resume_recent_run": "Resume optimization", "load_checkpoint": "Load checkpoint",
        }
        label = event.get("label") or labels.get(action) or action.replace("_", " ").strip().capitalize() or "Background task"
        token = "task:" + str(event.get("id"))
        state = event.get("state")
        if state == "queued" and not quiet:
            self._activity_begin(token, label, priority=50 if event.get("lane") == "control" else 30)
            self.activity_journal.active[token].update(action=action, started_at=None)
            self._activity_event(label + "…", key=token)
            self._poll_service_activity()
        elif state == "running" and not quiet:
            active = self.activity_journal.active.get(token)
            if active is not None:
                active["started_at"] = event.get("started_at", time.time())
            self._poll_service_activity()
        elif state in ("completed", "failed"):
            if not quiet:
                self._poll_service_activity()
                self._activity_finish(token)
            if state == "failed":
                self._activity_event(label + ": " + str(event.get("error") or "failed"), "ERROR", key=token)
            elif not quiet:
                self._activity_event(label + " completed.", key=token)
        elif state == "shutdown":
            self.activity_journal.active.clear()
            self._activity_begin("exit", "Stopping owned processes…", priority=100)
            self._activity_event("Stopping owned processes before exit…")

    def _poll_service_activity(self):
        # A new foreground task can request a refresh between timer ticks.
        # Cancel the pending tick so it cannot create another polling chain.
        identifier = getattr(self, "service_activity_poll_job", None)
        self.service_activity_poll_job = None
        if identifier is not None:
            try:
                self.root.after_cancel(identifier)
            except tk.TclError:
                pass
        if getattr(self, "closing", False) or getattr(self, "service_activity_inflight", False):
            return
        method = getattr(type(self.service), "activity_snapshot", None)
        if method is None or not hasattr(self, "tasks") or self.tasks is None:
            return
        active = any(str(key).startswith("task:") for key in self.activity_journal.active)
        if not active:
            return
        self.service_activity_inflight = True
        def loaded(payload):
            self.service_activity_inflight = False
            if getattr(self, "closing", False) or not isinstance(payload, dict):
                return
            rows = payload.get("events") or [payload]
            for row in rows:
                revision = row.get("revision") or 0
                if revision <= getattr(self, "service_activity_revision", 0):
                    continue
                self.service_activity_revision = revision
                message = row.get("message") or row.get("stage")
                self._activity_event(message, str(row.get("level") or ("ERROR" if row.get("state") == "error" else "INFO")).upper(), key="service:" + str(revision))
                if message:
                    for token in list(self.activity_journal.active):
                        if str(token).startswith("task:"):
                            active = self.activity_journal.active[token]
                            if active.get("started_at") is None or row.get("time", 0) < active["started_at"]:
                                continue
                            self.activity_journal.update(token, label=message)
                            gates = setup_gate_progress(active.get("action"), row.get("stage"), message, active.get("gates"))
                            if row.get("state") == "error" and active.get("gates"):
                                gates = dict(active["gates"], current=message)
                            self._activity_set_gates(token, gates)
                    self._activity_render_wait()
            if any(str(key).startswith("task:") for key in self.activity_journal.active):
                self.service_activity_poll_job = self.root.after(350, self._poll_service_activity)
        def failed(error):
            self.service_activity_inflight = False
            self._activity_event("Load stage status unavailable: {}".format(error), "WARN", key="service-activity-error")
            if not self.closing and any(str(key).startswith("task:") for key in self.activity_journal.active):
                self.service_activity_poll_job = self.root.after(1000, self._poll_service_activity)
        self.tasks.submit_monitor(lambda: method(self.service), loaded, failed)

    def _watch_activity_statuses(self):
        for name, label in (("settings_status_var", "Configuration"), ("eda_status_var", "EDA"),
                ("range_status_var", "Range check"), ("optimized_schematic_status_var", "Schematic"),
                ("environment_test_status_var", "Diagnostics"), ("job_policy_status_var", "Job Setup"),
                ("save_options_status_var", "Save Options")):
            variable = getattr(self, name, None)
            if variable is None:
                continue
            def observe(*_args, variable=variable, label=label, key=name):
                message = variable.get()
                lower = message.lower()
                level = "ERROR" if any(word in lower for word in ("error", "fail", "失败")) else "WARN" if any(word in lower for word in ("warning", "unavailable", "保留", "未完成", "需留意", "阻塞")) else "INFO"
                if key == "range_status_var" and "check complete:" in lower and " 0 failed." in lower:
                    level = "INFO"
                self._activity_event(label + ": " + message, level, key=key)
            variable.trace("w", observe)

    def _page_heading(self, parent, title, subtitle):
        frame = ttk.Frame(parent, style="Page.TFrame", padding=(22, 18, 22, 8))
        frame.pack(fill="x")
        ttk.Label(frame, text=title, style="PageTitle.TLabel").pack(anchor="w")
        subtitle_label = ttk.Label(frame, text=subtitle, style="Muted.TLabel", wraplength=950)
        subtitle_label.pack(anchor="w", pady=(4, 0))
        self._watch_help_wrapping(frame, [subtitle_label])

    def _card(self, parent, padding=(14, 12)):
        frame = ttk.Frame(parent, style="Card.TFrame", padding=padding)
        frame.pack(fill="x", padx=22, pady=8)
        settings = getattr(self, "pages", {}).get("EDA Settings")
        if settings is not None and self._widget_is_descendant(parent, settings):
            self._watch_help_wrapping(frame)
        return frame

    def _watch_help_wrapping(self, container, labels=None):
        """Discover a card's help once, then resize only its known label list."""
        state = {"job": None, "labels": [], "closed": False}
        container._cr_help_wrap_state = state

        def padding(widget, key="padding"):
            try:
                values = [int(str(value)) for value in widget.tk.splitlist(widget.cget(key))]
            except (tk.TclError, ValueError, TypeError):
                return 0, 0
            return (values[0], values[2] if len(values) > 2 else values[0]) if values else (0, 0)

        def fit():
            state["job"] = None
            if state["closed"]:
                return
            for label, maximum in state["labels"]:
                parent = label.master
                if not label.winfo_exists() or parent.winfo_width() <= 32:
                    continue
                left, right = padding(parent)
                width = parent.winfo_width() - left - right
                manager = label.winfo_manager()
                if manager == "grid":
                    info = label.grid_info()
                    column, row = int(info["column"]), int(info["row"])
                    x, _y, span_width, _height = parent.grid_bbox(column, row,
                        column + int(info.get("columnspan", 1)) - 1,
                        row + int(info.get("rowspan", 1)) - 1)
                    width = min(span_width, parent.winfo_width() - right - x)
                else:
                    info = label.pack_info() if manager == "pack" else {}
                try:
                    sides = [int(str(value)) for value in label.tk.splitlist(info.get("padx", 0))]
                    width -= sum(sides) if len(sides) > 1 else 2 * (sides[0] if sides else 0)
                except (tk.TclError, ValueError, TypeError):
                    pass
                width = min(maximum, max(40, width - 4))
                if int(float(str(label.cget("wraplength")) or "0")) != width:
                    label.configure(wraplength=width)

        def schedule(_event=None):
            if not state["closed"] and state["job"] is None:
                state["job"] = self.root.after(25, fit)

        def initialize():
            state["job"] = None
            if state["closed"]:
                return
            candidates = labels
            if candidates is None:
                candidates, pending = [], [container]
                while pending:
                    widget = pending.pop()
                    pending.extend(widget.winfo_children())
                    if isinstance(widget, ttk.Label) and str(widget.cget("style")) == "Help.TLabel":
                        if float(str(widget.cget("wraplength")) or "0") > 0 or len(str(widget.cget("text"))) > 70:
                            candidates.append(widget)
            parents = set()
            for label in candidates:
                maximum = int(float(str(label.cget("wraplength")) or "0")) or 950
                state["labels"].append((label, maximum))
                if label.master not in parents:
                    parents.add(label.master)
                    label.master.bind("<Configure>", schedule, add="+")
            fit()

        def destroyed(event):
            if event.widget is container:
                state["closed"] = True
                if state["job"] is not None:
                    try:
                        self.root.after_cancel(state["job"])
                    except tk.TclError:
                        pass
                    state["job"] = None
                state["labels"] = []
        container.bind("<Destroy>", destroyed, add="+")
        state["job"] = self.root.after_idle(initialize)

    def _bind_page_mousewheel(self):
        self.root.bind_all("<MouseWheel>", self._page_mousewheel)
        self.root.bind_all("<Button-4>", self._page_mousewheel)
        self.root.bind_all("<Button-5>", self._page_mousewheel)

    @staticmethod
    def _wheel_units(event):
        number = getattr(event, "num", None)
        if number == 4:
            return -1
        if number == 5:
            return 1
        delta = getattr(event, "delta", 0)
        if delta > 0:
            return -1
        if delta < 0:
            return 1
        return 0

    @staticmethod
    def _widget_is_descendant(widget, ancestor):
        while widget is not None:
            if widget is ancestor:
                return True
            widget = getattr(widget, "master", None)
        return False

    @staticmethod
    def _widget_owns_mousewheel(widget):
        try:
            widget_class = widget.winfo_class()
        except (AttributeError, tk.TclError):
            return False
        return widget_class in ("Text", "Treeview", "Listbox", "TCombobox")

    def _page_mousewheel(self, event):
        page = self.pages.get(self.current_page)
        if page is None or not page.winfo_ismapped():
            return None
        try:
            widget = page.winfo_containing(event.x_root, event.y_root)
        except (tk.TclError, KeyError):
            return None
        if not self._widget_is_descendant(widget, page):
            return None
        units = self._wheel_units(event)
        if not units:
            return None
        panel = getattr(self, "execution_selection_panel", None)
        if panel is not None and self._widget_is_descendant(widget, panel.canvas):
            panel.canvas.yview_scroll(units, "units")
            return "break"
        if self._widget_owns_mousewheel(widget):
            return None
        page.scroll_units(units)
        return "break"

    def show_page(self, name):
        if name == "Settings":
            name = "EDA Settings"
        if getattr(self, "closing", False):
            return
        if name != "Parameters" and getattr(self, "current_page", None) == "Parameters":
            self._cancel_parameter_render()
        self.current_page = name
        for page_name, page in self.pages.items():
            if page_name == name:
                page.pack(fill="both", expand=True)
                self.page_buttons[page_name].configure(style="SidebarActive.TButton")
            else:
                page.pack_forget()
                self.page_buttons[page_name].configure(style="Sidebar.TButton")
        if name == "Optimization" and hasattr(self, "recent_tree"):
            self.refresh_recent_runs()
        elif name == "Parameters" and hasattr(self, "parameter_tree"):
            if (getattr(self, "parameter_tree_dirty", True) or
                    self._parameter_tree_key() != getattr(self, "_parameter_visible_key", None)):
                self.render_parameter_tree()
        elif name == "Live Results" and hasattr(self, "result_tree"):
            self.render_results()
        elif name == "Pareto Trade-offs" and hasattr(self, "pareto_container"):
            self.render_pareto()

    def _build_design_page(self, parent):
        self._page_heading(
            parent, "Design Setup",
            "The invoking Maestro is selected by default. Load and Inspect lists Tests; choose one, select cells, then Apply to discover parameters." if self.launch_context else "Load a Maestro or ADE XL setup, or restore a complete design configuration.",
        )
        card = self._card(parent)
        ttk.Label(card, text="ADE SetupDB", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=5, sticky="w", pady=(0, 8))
        self.setup_path_var = tk.StringVar(value="")
        bound = bool(self.launch_context)
        self.setup_path_entry = ttk.Entry(card, textvariable=self.setup_path_var)
        self.setup_path_entry.grid(row=1, column=0, sticky="ew", padx=(0, 6))
        self.browse_setup_file_button = ttk.Button(card, text="Browse File...", command=self._browse_setup_file)
        self.browse_setup_file_button.grid(row=1, column=1, padx=4)
        self.browse_setup_folder_button = ttk.Button(card, text="Browse Folder...", command=self._browse_setup_folder)
        self.browse_setup_folder_button.grid(row=1, column=2, padx=4)
        self.load_setup_button = ttk.Button(card, text="新建 / Load and Inspect", style="Accent.TButton", command=self.load_setup)
        self.load_setup_button.grid(row=1, column=3, padx=(4, 0))
        card.columnconfigure(0, weight=1)
        self.setup_summary_var = tk.StringVar(value="No setup loaded.")
        ttk.Label(card, textvariable=self.setup_summary_var, style="CardMuted.TLabel", wraplength=950).grid(
            row=2, column=0, columnspan=5, sticky="w", pady=(10, 0),
        )
        if bound:
            source = self.launch_context.get("source") or {}
            change_status = "saved" if source.get("modified") is False else "unsaved" if source.get("modified") is True else "unknown; reading the saved setup"
            identity = "Invoking Maestro: {}/{}/{} · default saved setup\nWorking directory: {}\nLibrary mapping: {}\nMaestro changes at launch: {}".format(
                source.get("library", ""), source.get("cell", ""), source.get("view", ""),
                source.get("work_dir", ""), source.get("cds_lib", ""), change_status,
            )
            self.setup_source_var = tk.StringVar(value=identity)
            ttk.Label(card, textvariable=self.setup_source_var, style="CardMuted.TLabel", wraplength=950).grid(
                row=3, column=0, columnspan=5, sticky="w", pady=(8, 0),
            )
            self.launch_history_summary_var = tk.StringVar(value="History is optional and is not read automatically.")
            ttk.Label(card, textvariable=self.launch_history_summary_var, style="CardMuted.TLabel", wraplength=950).grid(
                row=4, column=0, columnspan=5, sticky="w", pady=(6, 0),
            )
            history = ttk.Frame(card, style="Card.TFrame")
            history.grid(row=6, column=0, columnspan=5, sticky="ew", pady=(8, 0))
            ttk.Label(history, text="History (optional)", style="Card.TLabel").pack(side="left", padx=(0, 8))
            self.history_choice_var = tk.StringVar(value="None")
            self.history_combo = ttk.Combobox(history, textvariable=self.history_choice_var,
                                            values=("None",), state="readonly", width=36)
            self.history_combo.pack(side="left", fill="x", expand=True)
            self.history_combo.bind("<<ComboboxSelected>>", self._history_choice_changed)
            self.read_history_button = ttk.Button(history, text="Read History Summary", command=self._load_launch_history, state="disabled")
            self.read_history_button.pack(side="left", padx=(8, 0))
        self.setup_path_var.trace("w", self._setup_path_changed)

        suite_card = self._card(parent)
        ttk.Label(suite_card, text="私有套件", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(suite_card, text="新建：使用上方 Load and Inspect，依次选择 Test 和模块。复用或更新：打开套件管理，选择已保存的完整套件。", style="CardMuted.TLabel", wraplength=950).pack(anchor="w", pady=(4, 6))
        ttk.Button(suite_card, text="管理 / 加载私有套件…", command=self.open_private_suites).pack(anchor="w")

        test_card = self._card(parent)
        ttk.Label(test_card, text="Test", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(test_card, text="Select one Test to inspect its cell hierarchy.", style="CardMuted.TLabel").pack(anchor="w", pady=(3, 6))
        self.test_choice_var = tk.StringVar(value="")
        self.test_combo = ttk.Combobox(test_card, textvariable=self.test_choice_var, values=(), state="disabled")
        self.test_combo.pack(fill="x")
        self.test_combo.bind("<<ComboboxSelected>>", self._test_selected)

        cell_card = self._card(parent)
        ttk.Label(cell_card, text="Cell Hierarchy", style="CardTitle.TLabel").pack(anchor="w")
        self.cell_scope_summary_var = tk.StringVar(value="Select a Test to load its cell hierarchy, then select cells and Apply to inspect parameters.")
        ttk.Label(cell_card, textvariable=self.cell_scope_summary_var, style="CardMuted.TLabel", wraplength=950).pack(anchor="w", pady=(3, 6))
        self.cell_scope_tree = ttk.Treeview(cell_card, columns=("library", "cell", "path"), show="tree headings", height=9)
        self.cell_scope_tree.heading("#0", text="Select cell (includes its subcells)")
        for column, label, width in (("library", "Library", 190), ("cell", "Cell", 220), ("path", "Instance path", 300)):
            self.cell_scope_tree.heading(column, text=label)
            self.cell_scope_tree.column(column, width=width)
        self.cell_scope_tree.pack(fill="both", expand=True)
        self.cell_scope_tree.bind("<Button-1>", self._cell_scope_click)
        self.cell_scope_tree.bind("<space>", self._cell_scope_space)
        cell_actions = ttk.Frame(cell_card, style="Card.TFrame")
        cell_actions.pack(fill="x", pady=(7, 0))
        self.apply_cells_button = ttk.Button(cell_actions, text="Apply Selected Cells", style="Accent.TButton", command=self.apply_selected_cells, state="disabled")
        self.apply_cells_button.pack(side="left")
        ttk.Button(cell_actions, text="Select All Cells", command=lambda: self.select_all_cells(True)).pack(side="left", padx=6)
        ttk.Button(cell_actions, text="Clear Cell Selection", command=lambda: self.select_all_cells(False)).pack(side="left")
        self.open_config_button = ttk.Button(cell_actions, text="Open Config", command=self.open_private_config, state="disabled")
        self.open_config_button.pack(side="left", padx=(12, 0))
        self.private_setup_var = tk.StringVar(value="私有 Maestro / config 在 Load and Inspect 后创建。")
        ttk.Label(cell_card, textvariable=self.private_setup_var, style="CardMuted.TLabel",
                  wraplength=950, justify="left").pack(fill="x", pady=(8, 0))

        config_card = self._card(parent)
        ttk.Label(config_card, text="Configuration", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(config_card, text="一个 JSON 保存模块勾选、参数、目标约束、Maestro 和优化设置；未完成也能保存。加载自动完成 Load / Apply，旧 State / Config 同样可读。缺失或不匹配项提示后跳过。",
                  style="CardMuted.TLabel", wraplength=950).pack(anchor="w", pady=(4, 8))
        buttons = ttk.Frame(config_card, style="Card.TFrame")
        buttons.pack(fill="x")
        self.save_config_button = ttk.Button(buttons, text="Save Configuration...", command=self.save_configuration)
        self.save_config_button.pack(side="left")
        self.load_config_button = ttk.Button(buttons, text="Load Configuration...", command=self.load_configuration)
        self.load_config_button.pack(side="left", padx=6)
        self.restore_last_button = ttk.Button(buttons, text="Restore Last", command=self.restore_last_ui_state)
        self.restore_last_button.pack(side="left")
        ttk.Button(buttons, text="Details...", command=lambda: self._show_text_details("Configuration Match Details", getattr(self, "settings_match_detail", "No configuration loaded."))).pack(side="right")
        # Keep plug-in callers compatible; there is only one visible save/load pair.
        self.save_state_button = self.save_config_button
        self.load_state_button = self.load_config_button
        self.settings_status_var = tk.StringVar(value="配置可随时保存；加载成功后直接运行，无需重新设置各页。")
        self.config_message_var = self.settings_status_var
        ttk.Label(config_card, textvariable=self.settings_status_var, style="CardMuted.TLabel", wraplength=950).pack(anchor="w", pady=(6, 0))

    def _build_parameters_page(self, parent):
        self._page_heading(
            parent, "Parameters",
            "Expand the schematic hierarchy and edit only meaningful, optimizable device parameters.",
        )
        toolbar = self._card(parent, padding=(12, 9))
        self.parameter_view_var = tk.StringVar(value="hierarchy")
        self.parameter_filter_var = tk.StringVar(value="")
        filter_entry = ttk.Entry(toolbar, textvariable=self.parameter_filter_var, width=24)
        filter_entry.pack(side="left", fill="x", expand=True)
        self.parameter_filter_var.trace("w", self._parameter_filter_changed)
        self.parameter_view_switch = ParameterViewSwitch(
            toolbar, self.parameter_view_var, command=self._parameter_view_changed,
        )
        self.parameter_view_switch.pack(side="left", padx=(8, 4))
        self.parameter_all_button = ttk.Button(
            toolbar, text="All", command=lambda: self.select_all_parameters(True, self.parameter_all_button),
        )
        self.parameter_all_button.pack(side="left", padx=2)
        self.parameter_none_button = ttk.Button(
            toolbar, text="None", command=lambda: self.select_all_parameters(False, self.parameter_none_button),
        )
        self.parameter_none_button.pack(side="left", padx=2)
        self.width_all_button = ttk.Button(toolbar, text="W All", command=lambda: self.select_semantic("width", True, self.width_all_button))
        self.width_all_button.pack(side="left", padx=(8, 2))
        self.width_none_button = ttk.Button(toolbar, text="W None", command=lambda: self.select_semantic("width", False, self.width_none_button))
        self.width_none_button.pack(side="left", padx=2)
        self.length_all_button = ttk.Button(toolbar, text="L All", command=lambda: self.select_semantic("length", True, self.length_all_button))
        self.length_all_button.pack(side="left", padx=(8, 2))
        self.length_none_button = ttk.Button(toolbar, text="L None", command=lambda: self.select_semantic("length", False, self.length_none_button))
        self.length_none_button.pack(side="left", padx=2)
        ttk.Button(toolbar, text="Expand", command=lambda: self._set_tree_open(True)).pack(side="left", padx=(8, 2))
        ttk.Button(toolbar, text="Collapse", command=lambda: self._set_tree_open(False)).pack(side="left", padx=2)

        validation = self._card(parent, padding=(12, 9))
        self.validate_button = ttk.Button(validation, text="Check Selected Ranges", command=self.validate_all_ranges, state="disabled")
        self.validate_button.pack(side="left")
        self.apply_ranges_button = ttk.Button(validation, text="Apply Legal Ranges", command=self.apply_validated_ranges, state="disabled")
        self.apply_ranges_button.pack(side="left", padx=6)
        self.cancel_validation_button = ttk.Button(
            validation, text="Cancel Check", command=self.cancel_optional_validation, state="disabled",
        )
        self.cancel_validation_button.pack(side="left", padx=4)
        ttk.Button(validation, text="Details...", command=self.show_validation_details).pack(side="right")
        self.range_status_var = tk.StringVar(value="Static and optional PDK callback validation have not run.")
        ttk.Label(validation, textvariable=self.range_status_var, style="CardMuted.TLabel", wraplength=550).pack(side="left", padx=10, fill="x", expand=True)

        tree_card = self._card(parent, padding=(8, 8))
        columns = ("current", "minimum", "maximum", "unit", "scale", "validation")
        self.parameter_tree = FastTreeview(tree_card, columns=columns, show="tree headings", selectmode="browse", height=22)
        headings = {
            "#0": "Hierarchy / Parameter", "current": "Current", "minimum": "Minimum",
            "maximum": "Maximum", "unit": "Unit", "scale": "Scale", "validation": "PDK Validation",
        }
        widths = {"#0": 430, "current": 120, "minimum": 100, "maximum": 100, "unit": 55, "scale": 70, "validation": 210}
        for column, heading in headings.items():
            self.parameter_tree.heading(column, text=heading)
            self.parameter_tree.column(column, width=widths[column], stretch=column in ("#0", "validation"))
        y_scroll = ttk.Scrollbar(tree_card, orient="vertical", command=self.parameter_tree.yview)
        x_scroll = ttk.Scrollbar(tree_card, orient="horizontal", command=self.parameter_tree.xview)
        self.parameter_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.parameter_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        tree_card.rowconfigure(0, weight=1)
        tree_card.columnconfigure(0, weight=1)
        self.parameter_tree.bind("<Button-1>", self._parameter_single_click)
        self.parameter_tree.bind("<Double-1>", self._parameter_double_click)
        self.parameter_tree.bind("<space>", self._toggle_selected_tree_item)
        ttk.Label(
            tree_card,
            text="Single-click a checkbox or scale value. Double-click Minimum or Maximum to edit its range.",
            style="CardMuted.TLabel",
        ).grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.parameter_summary_var = tk.StringVar(value="No hierarchy loaded.")
        ttk.Label(parent, textvariable=self.parameter_summary_var, style="Muted.TLabel").pack(anchor="w", padx=24, pady=(0, 10))

    def _build_metrics_page(self, parent):
        self._page_heading(
            parent, "Objectives & Constraints",
            "Assign every metric to one or more corners. Aggregation precedes direction: minimize + max minimizes the worst selected point; mean weights all selected points equally, including sweeps. Multi-objective runs automatically use NSGA-II.",
        )
        from .gui_execution_selection import ExecutionSelectionPanel
        selection_card = self._card(parent)
        self.execution_selection_panel = ExecutionSelectionPanel(selection_card, self._execution_selection_changed)
        self.execution_selection_panel.pack(fill="both", expand=True)
        objective_card = self._card(parent)
        heading = ttk.Frame(objective_card, style="Card.TFrame")
        heading.pack(fill="x")
        ttk.Label(heading, text="Objectives", style="CardTitle.TLabel").pack(side="left")
        ttk.Button(heading, text="Add Objective", command=self.add_objective).pack(side="right")
        self.objective_container = ttk.Frame(objective_card, style="Card.TFrame")
        self.objective_container.pack(fill="x", pady=(10, 0))

        constraint_card = self._card(parent)
        heading = ttk.Frame(constraint_card, style="Card.TFrame")
        heading.pack(fill="x")
        ttk.Label(heading, text="Constraints", style="CardTitle.TLabel").pack(side="left")
        ttk.Button(heading, text="Add Constraint", command=self.add_constraint).pack(side="right")
        self.constraint_container = ttk.Frame(constraint_card, style="Card.TFrame")
        self.constraint_container.pack(fill="x", pady=(10, 0))
        self.analysis_summary_var = tk.StringVar(value="Load a setup to discover outputs and corners.")
        ttk.Label(parent, textvariable=self.analysis_summary_var, style="Muted.TLabel", wraplength=950).pack(anchor="w", padx=24, pady=8)

    def _execution_selection_changed(self):
        selection = self.execution_selection_panel.collect(require_selection=False) or {}
        tests = selection.get("tests") or []
        self._activity_event("Run selection: " + (", ".join(tests) or "no Test selected") +
            "; analyses: " + "; ".join(name + "=" + ",".join(values) for name, values in (selection.get("analyses") or {}).items()),
            key="execution_selection:" + json.dumps(selection, sort_keys=True))
        if getattr(self, "eda_revalidation_required", False):
            self._stash_eda_edit_draft()

    def _render_native_settings(self, catalogue):
        from .ade_setup import native_settings_summary
        setup = catalogue.get("setup") or {}
        reference = catalogue.get("setup_private_session") or {}
        summary = native_settings_summary(setup, catalogue.get("job_policy"), reference)
        summary.update(tests=copy.deepcopy(setup.get("tests") or []),
            corners=copy.deepcopy(setup.get("corners") or []),
            test_corner_masks=copy.deepcopy(setup.get("test_corner_masks") or {}),
            global_variables=copy.deepcopy(setup.get("global_variables") or []),
            model_groups=copy.deepcopy(setup.get("model_groups") or []),
            execution_settings=copy.deepcopy(setup.get("execution_settings") or []))
        self.native_settings_detail = summary
        source = (summary.get("save_options") or {}).get("effective") or {}
        private = summary.get("private_storage") or {}
        lines = ["Private Maestro uses the selected Test; its analysis parameters inherit the saved settings.",
            "Private netlist capture: Local, 2 workers; actual solver command/jobs/timeouts follow source Job Setup unless overridden.",
            "Native/plugin Job properties outside the CLI mapping remain in the copy; observed unsupported fields are listed in Details.",
            "Source simulation root: " + str(source.get("simulation_root") or "Unavailable"),
            "Source RDB root: " + str(source.get("rdb_root") or "Unavailable"),
            "Private simulation root: " + str(private.get("save_res_dir") or "Awaiting private native readback"),
            "Private RDB root: " + str(private.get("save_dir") or "Awaiting private native readback"),
            "Private paths: " + ("native readback verified" if private.get("configured_roots_verified") else "readback pending / unavailable"),
            "History data and locks are excluded. External startup/plugin settings remain environment dependencies."]
        self.private_settings_summary_var.set("\n".join(lines))
        for warning in list(summary.get("warnings") or []) + list(reference.get("metadata_warnings") or []):
            self._activity_event(str(warning), "WARN")

    def _show_native_settings(self):
        self._show_json_details("Native Maestro Settings", getattr(self, "native_settings_detail", {
            "message": "Load and Inspect first to read the saved settings."}))

    def _build_optimization_page(self, parent):
        self._page_heading(
            parent, "Optimization",
            "Configure the closed loop, monitor progress, or resume an automatically saved run.",
        )
        settings = self._card(parent)
        ttk.Label(settings, text="Run Settings", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=8, sticky="w", pady=(0, 8))
        self.generations_var = tk.StringVar(value="20")
        self.jobs_var = tk.StringVar(value="1")
        self.timeout_var = tk.StringVar(value="600")
        self.algorithm_var = tk.StringVar(value="differential_evolution")
        for index, (label, variable, width) in enumerate((
                ("Generations", self.generations_var, 10),)):
            ttk.Label(settings, text=label, style="Card.TLabel").grid(row=1, column=index * 2, sticky="w", padx=(0, 4))
            entry = ttk.Entry(settings, textvariable=variable, width=width)
            entry.grid(row=1, column=index * 2 + 1, sticky="w", padx=(0, 14))
            if variable is self.jobs_var:
                entry.configure(state="disabled")
        ttk.Label(settings, text="Algorithm", style="Card.TLabel").grid(row=2, column=0, sticky="w", pady=(12, 0))
        self.algorithm_combo = ttk.Combobox(
            settings, textvariable=self.algorithm_var,
            values=tuple(ALGORITHM_NAMES.keys()), state="readonly", width=24,
        )
        self.algorithm_combo.grid(row=2, column=1, columnspan=2, sticky="w", pady=(12, 0))
        self.algorithm_combo.bind("<<ComboboxSelected>>", self._algorithm_changed)
        self.algorithm_notice_var = tk.StringVar(value="")
        ttk.Label(settings, textvariable=self.algorithm_notice_var, style="CardMuted.TLabel", wraplength=650).grid(
            row=2, column=3, columnspan=4, sticky="w", padx=12, pady=(12, 0),
        )


        advanced = self._card(parent)
        self.advanced_visible = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            advanced, text="Show advanced algorithm parameters",
            variable=self.advanced_visible, command=self._toggle_advanced,
        ).pack(anchor="w")
        self.algorithm_parameter_frame = ttk.Frame(advanced, style="Card.TFrame")
        self.algorithm_parameter_vars = {}
        self.algorithm_parameter_manual = set()
        for algorithm, fields in ALGORITHM_FIELDS.items():
            for field in fields:
                self.algorithm_parameter_vars[(algorithm, field)] = tk.StringVar(value="")
        self._refresh_algorithm_defaults(force=True)
        ttk.Button(
            advanced, text="Reset calculated defaults",
            command=lambda: self._refresh_algorithm_defaults(force=True),
        ).pack(anchor="w", pady=(6, 0))

        actions = self._card(parent, padding=(12, 10))
        ttk.Button(actions, text="Load Checkpoint...", command=self.load_checkpoint).pack(side="left")
        self.run_state_var = tk.StringVar(value="Waiting for setup.")
        ttk.Label(actions, textvariable=self.run_state_var, style="CardMuted.TLabel").pack(side="left", padx=12)
        ttk.Label(
            actions,
            text="Checkpoint updates automatically after every completed candidate and generation.",
            style="CardMuted.TLabel",
        ).pack(side="right")

        progress_card = self._card(parent)
        self.progress_summary_var = tk.StringVar(value="No run data.")
        ttk.Label(progress_card, textvariable=self.progress_summary_var, style="CardMuted.TLabel", wraplength=950).pack(anchor="w", pady=(7, 0))

        recent = self._card(parent)
        header = ttk.Frame(recent, style="Card.TFrame")
        header.pack(fill="x")
        ttk.Label(header, text="Recent Local Runs", style="CardTitle.TLabel").pack(side="left")
        ttk.Button(header, text="Refresh", command=self.refresh_recent_runs).pack(side="right")
        self.recent_tree = ttk.Treeview(
            recent,
            columns=("generation", "success", "failures", "state", "started", "checkpoint", "active"),
            show="tree headings", height=7, selectmode="browse",
        )
        self.recent_tree.heading("#0", text="Run Name")
        self.recent_tree.heading("generation", text="Generation")
        self.recent_tree.heading("success", text="Success")
        self.recent_tree.heading("failures", text="Failures")
        self.recent_tree.heading("state", text="State")
        self.recent_tree.heading("started", text="Started")
        self.recent_tree.heading("checkpoint", text="Resume")
        self.recent_tree.heading("active", text="Active")
        self.recent_tree.column("#0", width=250, stretch=True)
        self.recent_tree.column("generation", width=100)
        self.recent_tree.column("success", width=80)
        self.recent_tree.column("failures", width=190)
        self.recent_tree.column("state", width=120)
        self.recent_tree.column("started", width=145)
        self.recent_tree.column("checkpoint", width=70)
        self.recent_tree.column("active", width=70)
        self.recent_tree.pack(fill="x", pady=(8, 6))
        self.recent_tree.bind("<Double-1>", lambda _event: self.resume_recent_run())
        recent_actions = ttk.Frame(recent, style="Card.TFrame")
        recent_actions.pack(fill="x")
        ttk.Button(recent_actions, text="Resume Selected", command=self.resume_recent_run).pack(side="left")
        ttk.Button(recent_actions, text="View Progress / Logs", command=self.view_recent_run).pack(side="left", padx=8)

    def _build_execution_details_card(self, parent):
        console = self._card(parent)
        ttk.Label(console, text="Execution Progress / Commands", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(console, text="Expanded solver and local/cluster submission commands, owned task stages, and diagnostic log tails.",
                  style="CardMuted.TLabel").pack(anchor="w", pady=(4, 6))
        content = ttk.Frame(console, style="Card.TFrame")
        content.pack(fill="both", expand=True)
        self.execution_console = tk.Text(content, height=14, wrap="none", state="disabled",
                                         background="#fbfdff", foreground=TEXT,
                                         font=("TkFixedFont", 10), relief="flat")
        y_scroll = ttk.Scrollbar(content, orient="vertical", command=self.execution_console.yview)
        x_scroll = ttk.Scrollbar(content, orient="horizontal", command=self.execution_console.xview)
        self.execution_console.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.execution_console.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        content.columnconfigure(0, weight=1)
        content.rowconfigure(0, weight=1)
        self._render_execution_console({"message": "No run selected."})
        logs = ttk.Frame(console, style="Card.TFrame")
        logs.pack(fill="x", pady=(10, 0))
        ttk.Label(logs, text="诊断总日志 · 三阶段", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=4, sticky="w", pady=(0, 5))
        self.environment_engine_log_vars = {}
        self.environment_engine_log_buttons = {}
        for index, (backend, label) in enumerate((("main", "总日志"),), 1):
            ttk.Label(logs, text=label, style="Card.TLabel").grid(row=index, column=0, sticky="w", padx=(0, 8), pady=3)
            variable = tk.StringVar(value="Not created yet.")
            self.environment_engine_log_vars[backend] = variable
            ttk.Entry(logs, textvariable=variable, state="readonly").grid(row=index, column=1, sticky="ew", pady=3)
            button = ttk.Button(logs, text="View", state="disabled",
                command=lambda kind=backend: self.show_environment_test_log(kind))
            button.grid(row=index, column=2, padx=6, pady=3)
            self.environment_engine_log_buttons[backend] = button
            ttk.Button(logs, text="Copy Path", command=lambda kind=backend: self.copy_environment_test_log_path(kind)).grid(
                row=index, column=3, pady=3)
        logs.columnconfigure(1, weight=1)

    def _build_execution_commands_card(self, parent):
        card = self._card(parent)
        ttk.Label(card, text="Solver / Submission Commands", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=3, sticky="w", pady=(0, 6))
        ttk.Label(card, text=(
            "Both netlist sources use these commands. Blank solver fields use qualified automatic defaults; "
            "blank submission uses Local or the original Job Setup prefix. Baseline disables these fields and retains their drafts."
        ), style="Help.TLabel", wraplength=1000).grid(row=1, column=0, columnspan=3, sticky="w", pady=(0, 8))
        self.command_mode_notice_var = tk.StringVar(value="")
        ttk.Label(card, textvariable=self.command_mode_notice_var, style="Help.TLabel", wraplength=900).grid(
            row=14, column=0, columnspan=3, sticky="w", pady=(5, 0))
        self.cli_command_controls = []
        self.solver_command_vars = {name: tk.StringVar(value="") for name in ("spectre", "alps")}
        self.submission_command_var = tk.StringVar(value="")
        self.scheduler_command_vars = {kind: tk.StringVar(value="") for kind in ("query", "cancel")}
        examples = {
            "query": "batch-query {job_id}", "cancel": "batch-cancel {job_id}",
            "spectre": DEFAULT_SOLVER_TEMPLATES["spectre"],
            "alps": DEFAULT_SOLVER_TEMPLATES["alps"],
            "submission": 'batch-submit --cwd {cwd} --stdout {scheduler_stdout} --stderr {scheduler_stderr} {job_script}',
        }
        for index, (name, label, variable) in enumerate((
                ("spectre", "Spectre solver", self.solver_command_vars["spectre"]),
                ("alps", "ALPS solver", self.solver_command_vars["alps"]),
                ("submission", "Submission", self.submission_command_var),
                ("query", "Scheduler query", self.scheduler_command_vars["query"]),
                ("cancel", "Scheduler cancel", self.scheduler_command_vars["cancel"])), 2):
            row = 2 + (index - 2) * 2
            ttk.Label(card, text=label, style="Card.TLabel").grid(row=row, column=0, sticky="w", padx=(0, 8), pady=3)
            entry = ttk.Entry(card, textvariable=variable, style="Crescendo.Input.TEntry")
            entry.grid(row=row, column=1, sticky="ew", pady=3)
            button = ttk.Button(card, text="Use Example", command=lambda target=variable, value=examples[name]: target.set(value))
            button.grid(row=row, column=2, padx=(8, 0), pady=3)
            self.cli_command_controls.extend((entry, button))
            explanation = ("Required for Cluster; must contain {job_id}. No command is inferred." if name in ("query", "cancel") else
                           "Leave empty for automatic " + ("local/cluster submission inherited from Job Setup." if name == "submission" else name.upper() + " solver arguments."))
            ttk.Label(card, text=explanation, style="Help.TLabel").grid(row=row + 1, column=1, columnspan=2, sticky="w", pady=(0, 4))
        ttk.Label(card, text=(
            "{simulator} is the selected executable path. Solver fields: {netlist}, {psf}, {log}, {ahdl}, "
            "{model_dir}, {threads}. Submission includes {job_script}; task fields: {cwd}, {test}, {corner}, {point_id}. "
            "Scheduler query/cancel fields: {job_id}, {cwd}, {test}, {corner}, {point_id}. "
            "Submission log fields: {scheduler_stdout}, {scheduler_stderr}. Placeholders preserve spaces in paths. Commands are literal argv templates. "
            "Job Setup Override command is a prefix; Submission above is the complete command including {job_script}."
        ), style="Help.TLabel", wraplength=1000).grid(row=12, column=0, columnspan=3, sticky="w", pady=(7, 0))
        automatic = ttk.Button(card, text="Use Automatic Commands", command=lambda: self._restore_execution_commands(
            {"solver_templates": {"spectre": "", "alps": ""}, "submission_template": "", "scheduler_query_template": "", "scheduler_cancel_template": ""}))
        automatic.grid(row=13, column=0, columnspan=3, sticky="w", pady=(7, 0))
        self.cli_command_controls.append(automatic)
        card.columnconfigure(1, weight=1)
        self._restore_execution_commands()
        if hasattr(self, "execution_backend_var"):
            self.execution_backend_var.trace("w", lambda *_args: self._update_command_mode())
            self._update_command_mode()

    def _native_test_simulator(self):
        if not getattr(self, "prepared_id", None):
            return ""
        setup = (getattr(self, "prepared", None) or {}).get("setup") or {}
        tests = setup.get("tests") or []
        selected = getattr(self, "selected_test", None)
        test = next((row for row in tests if row.get("name") == selected), None)
        if test is None and len(tests) == 1:
            test = tests[0]
        return str((test or {}).get("simulator") or "").strip().lower()

    def _update_command_mode(self):
        baseline = self.execution_backend_var.get() == "maestro_baseline"
        previous = getattr(self, "_baseline_mode_active", False)
        if baseline and not previous:
            self._cli_simulator_draft = self.simulator_var.get()
        self._baseline_mode_active = baseline
        if baseline:
            self.simulator_var.set(self._native_test_simulator() or "Awaiting Test")
        elif previous:
            self.simulator_var.set(getattr(self, "_cli_simulator_draft", "spectre"))
        self.simulator_combo.configure(state="disabled" if baseline else "readonly")
        for widget in self.cli_command_controls:
            widget.configure(state="disabled" if baseline else "normal")
        scheduler = getattr(self, "command_scheduler_combo", None)
        if scheduler is not None:
            scheduler.configure(state="disabled" if baseline else "readonly")
        if hasattr(self, "job_mode_notice_var"):
            self.job_mode_notice_var.set("Baseline uses Command distribution and the Job Setup input values below." if baseline else "Job Setup input values apply directly to the selected mode.")
        self.command_mode_notice_var.set(
            "Baseline uses the source simulator and Job Setup fields below; CLI solver templates are retained for other modes."
            if baseline else "CLI commands apply to the two netlist modes.")
        self._render_simulator_profiles()

    def _execution_command_values(self):
        variables = getattr(self, "solver_command_vars", {})
        if not variables and not hasattr(self, "submission_command_var"):
            return {}
        values = {"solver_templates": {name: variables[name].get() if name in variables else "" for name in ("spectre", "alps")},
                "submission_template": self.submission_command_var.get() if hasattr(self, "submission_command_var") else ""}
        for kind, variable in getattr(self, "scheduler_command_vars", {}).items():
            values["scheduler_" + kind + "_template"] = variable.get()
        return values

    def _collect_execution_commands(self):
        values = self._execution_command_values()
        for label, value in list((values.get("solver_templates") or {}).items()) + [(key, values.get(key, "")) for key in ("submission_template", "scheduler_query_template", "scheduler_cancel_template")]:
            if not isinstance(value, str) or len(value) > 4096 or "\x00" in value:
                raise ValueError("Invalid {} command template.".format(label))
        config = getattr(getattr(self, "service", None), "config", {}) or {}
        return normalize_execution_commands(config, {"execution_commands": values})

    def _restore_execution_commands(self, values=None):
        if values is None:
            values = ((getattr(getattr(self, "service", None), "config", {}) or {}).get("runtime") or {}).get("execution_commands") or {}
        if not isinstance(values, dict):
            raise ValueError("Invalid saved execution command templates")
        templates = values.get("solver_templates") or {}
        if not isinstance(templates, dict):
            raise ValueError("Invalid saved solver command templates")
        for name, variable in getattr(self, "solver_command_vars", {}).items():
            value = templates.get(name, "")
            if not isinstance(value, str) or len(value) > 4096 or "\x00" in value:
                raise ValueError("Invalid saved {} command template".format(name))
            variable.set(value)
        if hasattr(self, "submission_command_var"):
            value = values.get("submission_template", "")
            if not isinstance(value, str) or len(value) > 4096 or "\x00" in value:
                raise ValueError("Invalid saved submission command template")
            self.submission_command_var.set(value)
        for kind, variable in getattr(self, "scheduler_command_vars", {}).items():
            value = values.get("scheduler_" + kind + "_template", "")
            if not isinstance(value, str) or len(value) > 4096 or "\x00" in value:
                raise ValueError("Invalid saved scheduler command template")
            variable.set(value)

    def _build_environment_test_card(self, parent):
        card = self._card(parent)
        ttk.Label(card, text="仿真链路诊断", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(card, text=(
            "固定顺序：源 Test 的私有 Maestro Baseline → 私有网表 CLI → 每候选私有 Maestro 生成网表 CLI。"
            "各阶段使用相同候选、测试和角点；求解器可能不同。优先验证提交、求解和结果读取，暂缓导出往返等附加检查。"
            "独立失败且停止已证后继续，原因与未测项目写入一份总日志。"
        ), style="Help.TLabel", wraplength=950).pack(anchor="w", pady=(5, 8))
        actions = ttk.Frame(card, style="Card.TFrame")
        actions.pack(fill="x")
        self.environment_test_button = ttk.Button(
            actions, text="运行诊断", command=self.start_environment_test)
        self.environment_test_button.pack(side="left")
        self.environment_test_cancel_button = ttk.Button(
            actions, text="停止诊断", command=self.stop_environment_test, state="disabled")
        self.environment_test_cancel_button.pack(side="left", padx=8)
        self.environment_test_log_button = ttk.Button(
            actions, text="查看诊断日志", command=self.show_environment_test_log, state="disabled")
        self.environment_test_log_button.pack(side="left")
        ttk.Button(actions, text="复制总日志路径", command=self.copy_environment_test_log_paths).pack(side="left", padx=8)
        ttk.Button(actions, text="查看所选说明", command=self.show_environment_test_details).pack(side="left")
        size_row = ttk.Frame(card, style="Card.TFrame")
        size_row.pack(fill="x", pady=(6, 0))
        self.diagnostic_generations_var = tk.StringVar(value="2")
        self.diagnostic_total_var = tk.StringVar(value="6")
        ttk.Label(size_row, text="诊断代数", style="Help.TLabel").pack(side="left")
        self.diagnostic_generations_entry = ttk.Entry(size_row, textvariable=self.diagnostic_generations_var, width=5)
        self.diagnostic_generations_entry.pack(side="left", padx=(4, 12))
        ttk.Label(size_row, text="每模式总个体数", style="Help.TLabel").pack(side="left")
        self.diagnostic_total_entry = ttk.Entry(size_row, textvariable=self.diagnostic_total_var, width=5)
        self.diagnostic_total_entry.pack(side="left", padx=4)
        self.diagnostic_size_var = tk.StringVar(value="")
        for variable in (self.diagnostic_generations_var, self.diagnostic_total_var):
            variable.trace_add("write", lambda *_: self._update_diagnostic_size())
        ttk.Label(card, textvariable=self.diagnostic_size_var, style="Help.TLabel", wraplength=950).pack(anchor="w")
        self._update_diagnostic_size()
        result_row = ttk.Frame(card, style="Card.TFrame")
        result_row.pack(fill="x", pady=(6, 0))
        self.diagnostic_case_var = tk.StringVar(value="")
        self.diagnostic_case_combo = ttk.Combobox(result_row, textvariable=self.diagnostic_case_var, state="readonly", width=36, values=[])
        self.diagnostic_case_combo.pack(side="left", padx=8)
        self.diagnostic_results_button = ttk.Button(result_row, text="查看 / 刷新所选模式结果", command=self.show_diagnostic_results, state="disabled")
        self.diagnostic_results_button.pack(side="left")
        self.diagnostic_timing_var = tk.StringVar(value="各模式耗时：等待数据。")
        ttk.Label(card, textvariable=self.diagnostic_timing_var, style="Help.TLabel", wraplength=950, justify="left").pack(anchor="w", pady=(4, 0))
        self.environment_test_status_var = tk.StringVar(value="就绪。先验证三级运行链路；附加检查暂缓，未执行不计为通过。")
        ttk.Label(card, textvariable=self.environment_test_status_var,
                  style="Help.TLabel", wraplength=950).pack(anchor="w", pady=(8, 3))
        self.environment_test_ladder_var = tk.StringVar(value="1. 原生 Baseline：待执行\n2. 私有网表 CLI：待执行\n3. 每候选原生生成网表 CLI：待执行")
        ttk.Label(card, textvariable=self.environment_test_ladder_var,
                  style="Help.TLabel", wraplength=950, justify="left").pack(anchor="w", pady=(0, 6))
        self.environment_test_log_var = tk.StringVar(value="诊断日志：尚未创建。")
        ttk.Label(card, textvariable=self.environment_test_log_var,
                  style="Help.TLabel", wraplength=950).pack(anchor="w", pady=(0, 6))
        self.environment_test_runs_var = tk.StringVar(value="本次诊断任务：尚未创建。")
        ttk.Label(card, textvariable=self.environment_test_runs_var,
                  style="Help.TLabel", wraplength=950).pack(anchor="w", pady=(0, 6))
        frame = ttk.Frame(card, style="Card.TFrame")
        frame.pack(fill="x")
        self.environment_test_tree = ttk.Treeview(
            frame, columns=("status", "category", "detail"), show="tree headings", height=10,
            selectmode="browse")
        for key, label in (("#0", "功能 / 检查项"), ("status", "结果"), ("category", "功能小类"), ("detail", "原因与下一步")):
            self.environment_test_tree.heading(key, text=label)
        self.environment_test_tree.column("#0", width=290, stretch=True)
        self.environment_test_tree.column("status", width=130, stretch=False)
        self.environment_test_tree.column("category", width=135, stretch=False)
        self.environment_test_tree.column("detail", width=420, stretch=True)
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.environment_test_tree.yview)
        self.environment_test_tree.configure(yscrollcommand=scroll.set)
        self.environment_test_tree.pack(side="left", fill="x", expand=True)
        scroll.pack(side="right", fill="y")
        for tag, color in (("problem", DANGER), ("warning", WARNING), ("success", SUCCESS)):
            self.environment_test_tree.tag_configure(tag, foreground=color)
        self.environment_test_tree.bind("<<TreeviewSelect>>", self._environment_test_selected)
        self.environment_test_tree.bind("<Button-1>", self._environment_test_blank_click, add="+")
        self.environment_test_tree.bind("<Escape>", self._clear_environment_test_selection)
        self.environment_test_tree.bind("<Double-1>", lambda _event: self._environment_test_selected())
        self.environment_test_checks = {}
        self.environment_test_sections = {}
        self.environment_test_detail_frame = ttk.Frame(card, style="Card.TFrame")
        detail_actions = ttk.Frame(self.environment_test_detail_frame, style="Card.TFrame")
        detail_actions.pack(fill="x", pady=(8, 3))
        self.environment_test_detail_title = tk.StringVar(value="问题说明")
        ttk.Label(detail_actions, textvariable=self.environment_test_detail_title,
                  style="CardTitle.TLabel").pack(side="left")
        ttk.Button(detail_actions, text="复制此小节", command=self.copy_environment_test_section).pack(side="right", padx=6)
        ttk.Button(detail_actions, text="复制网表路径", command=self.copy_environment_test_netlist_paths).pack(side="right", padx=6)
        ttk.Button(detail_actions, text="取消选择", command=self._clear_environment_test_selection).pack(side="right")
        detail_body = ttk.Frame(self.environment_test_detail_frame, style="Card.TFrame")
        detail_body.pack(fill="x")
        self.environment_test_detail_text = tk.Text(detail_body, height=12, wrap="word", state="disabled", undo=False)
        detail_scroll = ttk.Scrollbar(detail_body, orient="vertical", command=self.environment_test_detail_text.yview)
        self.environment_test_detail_text.configure(yscrollcommand=detail_scroll.set)
        self.environment_test_detail_text.pack(side="left", fill="x", expand=True)
        detail_scroll.pack(side="right", fill="y")
        # The explanation has no space in the layout until a check is selected.

    def _environment_test_blank_click(self, event):
        tree = self.environment_test_tree
        if not tree.identify_row(event.y) and tree.identify_region(event.x, event.y) not in ("heading", "separator"):
            self._clear_environment_test_selection()

    def _clear_environment_test_selection(self, _event=None):
        selection = self.environment_test_tree.selection()
        if selection:
            self.environment_test_tree.selection_remove(*selection)
        frame = getattr(self, "environment_test_detail_frame", None)
        if frame is not None:
            frame.pack_forget()
        return "break"

    def _environment_test_context(self, check):
        presentation = check.get("presentation") or {}
        case_id = presentation.get("case_id") or check.get("case_id")
        if not case_id and check.get("simulator") and check.get("engine"):
            case_id = check["simulator"] + "." + check["engine"]
        contexts = self.environment_test_payload.get("case_contexts") or {}
        if isinstance(contexts, list):
            contexts = {row.get("case_id"): row for row in contexts if isinstance(row, dict)}
        group_id = presentation.get("group_id") or describe_check(check).get("group_id")
        return contexts.get(case_id) or contexts.get(group_id) or {}

    def _environment_test_presentation(self, check):
        return check.get("presentation") or describe_check(check, self._environment_test_context(check))

    def _selected_environment_test_checks(self):
        selected = self.environment_test_tree.selection()
        if not selected:
            return []
        item = selected[0]
        check = self.environment_test_checks.get(item)
        if check is None:
            return self.environment_test_sections.get(item) or []
        presentation = self._environment_test_presentation(check)
        group, section = presentation.get("group_id"), presentation.get("section_id")
        return [row for row in self.environment_test_checks.values()
                if self._environment_test_presentation(row).get("group_id") == group
                and self._environment_test_presentation(row).get("section_id") == section]

    def _environment_test_netlists(self):
        contexts = self.environment_test_payload.get("case_contexts") or {}
        if isinstance(contexts, list):
            contexts = {row.get("case_id"): row for row in contexts if isinstance(row, dict)}
        lines, paths = [], []
        cases = self.environment_test_payload.get("case_results") or []
        case_ids = [key for key in contexts if key in ("spectre.maestro_netlist", "spectre.private_netlist", "alps.maestro_netlist", "alps.private_netlist")]
        case_ids += [row.get("case_id") for row in cases if isinstance(row, dict) and row.get("case_id") not in case_ids]
        for case_id in case_ids:
            context = contexts.get(case_id) or {}
            readable = {"spectre.maestro_netlist": "Spectre · 私有 Maestro 生成网表", "spectre.private_netlist": "Spectre · 私有网表改参",
                        "alps.maestro_netlist": "ALPS · 私有 Maestro 生成网表", "alps.private_netlist": "ALPS · 私有网表改参"}
            label = context.get("label") or context.get("case_label") or readable.get(case_id, str(case_id))
            netlists = context.get("netlists") or []
            if isinstance(netlists, dict):
                netlists = list(netlists.values())
            lines.append(label + ":")
            for artifact in netlists:
                path = artifact.get("path") if isinstance(artifact, dict) else artifact
                if not path:
                    continue
                role = (artifact.get("label") or artifact.get("role") or artifact.get("kind") or "网表") if isinstance(artifact, dict) else "网表"
                status = artifact.get("status") if isinstance(artifact, dict) else None
                suffix = " [{}]".format(status) if status else ""
                lines.append("  {}{}: {}".format(role, suffix, path))
                if path not in paths:
                    paths.append(path)
            if not netlists:
                lines.append("  尚未记录本次已生成的网表；请查看上方阻塞原因与生成日志。")
            if context.get("run_root"):
                lines.append("  诊断任务目录: " + str(context["run_root"]))
        return lines, paths

    def _environment_test_section_text(self, checks):
        context = self._environment_test_context(checks[0]) if checks else {}
        sections = {}
        for check in checks:
            presentation = self._environment_test_presentation(check)
            key = (presentation.get("group_id"), presentation.get("section_id"))
            sections.setdefault(key, []).append(check)
        body = "\n\n".join(format_section(rows, self._environment_test_context(rows[0])) for rows in sections.values())
        lines, _paths = self._environment_test_netlists()
        if lines:
            body += "\n\n本次各模式的网表 / SCS 路径（诊断保留，不自动清除）\n" + "\n".join(lines)
        return body

    def _environment_test_explanation_text(self, check, checks):
        focus = check or checks[0]
        p = self._environment_test_presentation(focus)
        context = self._environment_test_context(focus)
        lines = [p.get("group_label") or "运行环境", p.get("section_label") or "诊断说明", ""]
        for row in ([check] if check else checks):
            row_p = self._environment_test_presentation(row)
            lines.append("{}：{}".format(row_p.get("title") or "检查项", row_p.get("status_label") or row.get("status")))
            lines.append(row_p.get("explanation") or row_p.get("summary") or row.get("message") or "尚无详细说明。")
            component = row_p.get("component") or {}
            if component.get("file"):
                lines.append("负责此功能的代码：{}{}".format(component["file"],
                    " / " + component["function"] if component.get("function") else ""))
            evidence = row_p.get("evidence") or []
            if evidence:
                lines.append("可核对的文件与日志：")
                seen = set()
                for item in evidence:
                    if not isinstance(item, dict) or not item.get("path"):
                        continue
                    location = str(item["path"])
                    if item.get("locator"):
                        location += " · " + str(item["locator"])
                    if location not in seen:
                        lines.append("  " + location)
                        seen.add(location)
            lines.append("")
        primary = context.get("first_cause") or {}
        if primary:
            lines.append("本案例先停在：{}".format(primary.get("message") or primary.get("error") or primary.get("failure_stage") or "见错误证据"))
        lines.append("反馈时可点“复制此小节”：包括本小节证据与必要的前置原因，无需复制整份日志。")
        netlists, _paths = self._environment_test_netlists()
        if netlists:
            lines.extend(["", "本次所选链路的网表 / SCS 路径（保留供手工检查）"] + netlists)
        return "\n".join(lines)

    def _environment_test_selected(self, _event=None):
        checks = self._selected_environment_test_checks()
        frame = getattr(self, "environment_test_detail_frame", None)
        if frame is None:
            return
        if not checks:
            frame.pack_forget()
            return
        selected = self.environment_test_tree.selection()
        check = self.environment_test_checks.get(selected[0]) if selected else None
        presentation = self._environment_test_presentation(check or checks[0])
        self.environment_test_detail_title.set(presentation.get("title") or presentation.get("section_label") or "问题说明")
        text = self.environment_test_detail_text
        previous_view = text.yview()
        body = self._environment_test_explanation_text(check, checks)
        if text.get("1.0", "end-1c") != body:
            text.configure(state="normal")
            text.delete("1.0", "end")
            text.insert("1.0", body)
            text.configure(state="disabled")
            if previous_view:
                text.yview_moveto(previous_view[0])
        frame.pack(fill="x", pady=(3, 6))

    def copy_environment_test_section(self):
        checks = copy.deepcopy(self._selected_environment_test_checks())
        if not checks:
            return
        test_id = self.environment_test_id
        sections = {}
        for check in checks:
            p = self._environment_test_presentation(check)
            key = (p.get("group_id"), p.get("section_id"))
            if key not in sections:
                sections[key] = {"checks": [], "context": copy.deepcopy(self._environment_test_context(check))}
            sections[key]["checks"].append(check)
        netlist_lines, _paths = self._environment_test_netlists()
        def read():
            parts = []
            for (group, section), row in sections.items():
                context = row["context"]
                body = read_section(context.get("log_path"), group, section) if context.get("log_path") else None
                if body is None:
                    body = "本小节尚无完整落盘章节；以下包含当前检查、首因及证据路径。完整证据请等待本轮诊断结束。\n" + format_section(row["checks"], context)
                parts.append(body)
            if netlist_lines:
                parts.append("本次所选链路的网表 / SCS 路径（诊断保留）\n" + "\n".join(netlist_lines))
            return "\n\n".join(parts)
        def copied(body):
            if not self.closing and self.environment_test_id == test_id:
                self.root.clipboard_clear()
                self.root.clipboard_append(body)
                self.environment_test_status_var.set("已复制所选功能小节；可直接粘贴反馈。")
        getattr(self.tasks, "submit_optional", self.tasks.submit)(read, copied, self._task_error("复制诊断小节"))

    def copy_environment_test_netlist_paths(self):
        _lines, paths = self._environment_test_netlists()
        if paths:
            self.root.clipboard_clear()
            self.root.clipboard_append("\n".join(paths))
        else:
            self.environment_test_status_var.set("本次尚无已生成的网表路径；请查看所选条目的生成失败原因。")

    def _diagnostic_size_spec(self):
        generations = getattr(self, "diagnostic_generations_var", None)
        total = getattr(self, "diagnostic_total_var", None)
        try:
            raw_generations = generations.get().strip() if generations else "2"
            raw_total = total.get().strip() if total else "6"
            if not raw_generations.isdigit() or not raw_total.isdigit():
                raise ValueError()
            spec = {"diagnostic_generations": int(raw_generations),
                    "diagnostic_total_candidates": int(raw_total)}
            shape = diagnostic_shape(spec)
        except (ValueError, TypeError):
            raise ValueError("请输入整数：代数 1–10；每代 1–16 个；每模式总数不超过 64，且须能被代数整除。")
        return spec, shape

    def _update_diagnostic_size(self):
        try:
            spec, shape = self._diagnostic_size_spec()
            text = "每代 {} 个 · 每模式 {} 个 · 三模式合计 {} 个（不含角点倍率）".format(
                shape["samples_per_generation"], shape["total_candidates"], 3 * shape["total_candidates"])
        except ValueError as error:
            text = str(error)
        self.diagnostic_size_var.set(text)

    def _set_diagnostic_size_enabled(self, enabled):
        for name in ("diagnostic_generations_entry", "diagnostic_total_entry"):
            widget = getattr(self, name, None)
            if widget is not None:
                widget.configure(state="normal" if enabled else "disabled")

    def _collect_environment_test_spec(self, range_report=None):
        """Snapshot current inputs without applying optimization population rules."""
        spec, _ = self._diagnostic_size_spec()
        spec.update({"evaluator": "real", "variables": [], "objectives": [], "constraints": [],
                "candidate_timeout_seconds": 600.0, "ui_input_warnings": []})
        spec["simulator"] = (getattr(self, "_cli_simulator_draft", "spectre")
                             if getattr(self, "_baseline_mode_active", False) else
                             (self.simulator_var.get() if hasattr(self, "simulator_var") else "spectre"))
        backend = self.execution_backend_var.get() if hasattr(self, "execution_backend_var") else "auto"
        if backend not in ("auto", "maestro_netlist", "persistent_maestro", "legacy", "adexl", "private_netlist", "maestro_baseline"):
            raise ValueError("Unknown diagnostic netlist source: " + str(backend))
        spec["execution_backend"] = backend if backend in ("private_netlist", "maestro_baseline") else "maestro_netlist"
        warnings = spec["ui_input_warnings"]
        panel = getattr(self, "execution_selection_panel", None)
        if panel is not None:
            try:
                selected = panel.collect()
                if selected is not None:
                    spec["execution_selection"] = selected
                spec["execution_selection_valid"] = True
            except (TypeError, ValueError) as error:
                # Retain the invalid draft for diagnostics; never fall back to
                # the original enabled tests when the UI selection is empty.
                try:
                    spec["execution_selection"] = panel.collect(require_selection=False)
                except (TypeError, ValueError):
                    spec["execution_selection"] = {"tests": [], "analyses": {}}
                spec["execution_selection_valid"] = False
                warnings.append("Test/Analysis selection: {}".format(error))
        draft_path = getattr(self, "eda_edit_draft_path", None)
        setup_variable = getattr(self, "setup_path_var", None)
        current_path = setup_variable.get().strip() if setup_variable is not None else None
        prepared_state = {
            "prepared_id": getattr(self, "prepared_id", None),
            "inspection_id": getattr(self, "cell_inspection_id", None),
            "cell_scope_dirty": bool(getattr(self, "cell_scope_dirty", False)),
            "eda_settings_requires_revalidation": bool(getattr(self, "eda_revalidation_required", False)),
            "pending_restore_same_setup": bool(getattr(self, "pending_settings_state", None) and
                draft_path and current_path and self._same_setup_paths(draft_path, current_path)),
            "prepared_selection_matches": self._prepared_selection_matches(),
            "error": None,
        }
        spec["ui_prepared_state"] = prepared_state
        spec["execution_commands"] = self._execution_command_values()
        try:
            spec["execution_commands"] = self._collect_execution_commands()
            spec["command_settings_valid"] = True
        except Exception as error:
            warnings.append("Execution commands: {}".format(error))
            spec["command_settings_valid"] = False
        if self.prepared_id:
            try:
                self._require_prepared_selection()
            except Exception as error:
                prepared_state["error"] = str(error)
                warnings.append("Prepared selection: {}".format(error))
                spec["prepared_selection_valid"] = False
            else:
                spec["prepared_selection_valid"] = True
        else:
            spec["prepared_selection_valid"] = False
            prepared_state["error"] = "尚未完成当前模块的准备；请先 Load and Inspect，再 Apply Selected Cells。"
            warnings.append("Prepared selection: " + prepared_state["error"])
        try:
            if range_report is not None:
                spec["_range_warning_index"] = len(warnings)
            value = self._local_range_report() if range_report is None else range_report
            spec["variables"] = copy.deepcopy(value.get("variables") or [])
            for issue in value.get("issues") or []:
                warnings.append("Parameters: {}".format(issue.get("message") if isinstance(issue, dict) else issue))
        except Exception as error:
            warnings.append("Variables: {}".format(error))
        spec["ui_metric_drafts"] = {}
        spec["metric_definition_errors"] = []
        for kind in ("objective", "constraint"):
            name = kind + "s"
            rows = getattr(self, kind + "_rows", None)
            if rows is None:  # Non-widget callers may supply collection methods.
                try:
                    spec[name] = copy.deepcopy(getattr(self, "_collect_" + name)())
                    spec[name + "_valid"] = True
                except Exception as error:
                    spec[name + "_valid"] = False
                    warnings.append("{}: {}".format(name.title(), error))
                continue
            spec["ui_metric_drafts"][name] = [self._raw_metric_settings(row) for row in rows]
            for index, row in enumerate(rows, 1):
                try:
                    spec[name].append(copy.deepcopy(row.definition("{} {}".format(kind.title(), index))))
                except (ValueError, TypeError) as error:
                    detail = {"kind": kind, "index": index,
                              "output_key": row.output_var.get(), "error": str(error)}
                    spec["metric_definition_errors"].append(detail)
                    warnings.append("{}: {}".format(name.title(), error))
            spec[name + "_valid"] = not any(item["kind"] == kind for item in spec["metric_definition_errors"])
        raw_timeout = self.timeout_var.get().strip()
        timeout = -1 if not raw_timeout else finite_number(raw_timeout)
        if not raw_timeout or (timeout is not None and timeout > 0):
            spec["candidate_timeout_seconds"] = timeout
        else:
            warnings.append("Preparation/measurement fallback budget must be positive seconds or blank for no limit; zero is not allowed.")
            spec["timeout_settings_valid"] = False
        try:
            spec.update(self._collect_job_settings())
        except Exception as error:
            warnings.append("Job settings: {}".format(error))
            spec["job_settings_valid"] = False
        else:
            spec["job_settings_valid"] = True
        if spec.get("timeout_settings_valid") is False:
            spec["job_settings_valid"] = False
        if hasattr(self, "data_root_override_var") and self.data_root_override_var.get():
            spec["data_root_override"] = self.data_root_var.get().strip()
            if not spec["data_root_override"] or "\x00" in spec["data_root_override"]:
                warnings.append("Data directory override is invalid.")
                spec["storage_settings_valid"] = False
        try:
            spec["gui_runtime"] = {
                "tcl_version": str(self.root.tk.call("info", "patchlevel")),
                "tk_version": str(self.root.tk.call("package", "provide", "Tk")),
                "windowing_system": str(self.root.tk.call("tk", "windowingsystem")),
                "initialized": True,
            }
        except (AttributeError, tk.TclError) as error:
            warnings.append("GUI runtime identification: {}".format(error))
        return spec

    def start_environment_test(self):
        if self.closing or self.environment_test_state in ("starting", "queued", "running", "cancelling", "stopping"):
            return
        method = getattr(self.service, "start_environment_test", None)
        if not callable(method):
            self.environment_test_status_var.set("NOT_TESTED: this service does not provide environment testing.")
            return
        try:
            spec = self._collect_environment_test_spec(range_report={"variables": [], "issues": []})
            local_work = self._range_report_work()
        except ValueError as error:
            self.environment_test_status_var.set(str(error))
            return
        prepared_id = self.prepared_id
        self.environment_test_epoch += 1
        epoch = self.environment_test_epoch
        self.environment_test_state = "starting"
        self._set_diagnostic_size_enabled(False)
        self._activity_begin("diagnostics", "Starting diagnostics…", priority=15)
        self.environment_test_id = None
        self.environment_test_payload = {}
        self._cancel_results_load()
        self.diagnostic_case_var.set("")
        self.diagnostic_case_combo.configure(values=[])
        self.diagnostic_results_button.configure(state="disabled")
        self.environment_test_checks = {}
        self.environment_test_tree.delete(*self.environment_test_tree.get_children(""))
        self.environment_test_sections = {}
        self._clear_environment_test_selection()
        self.environment_test_button.configure(state="disabled")
        self.environment_test_cancel_button.configure(state="disabled")
        self.environment_test_log_button.configure(state="disabled")
        self.environment_test_status_var.set("Starting simulation diagnostics...")
        self.environment_test_log_var.set("Main report: preparing...")
        for backend, variable in self.environment_engine_log_vars.items():
            variable.set("Preparing...")
            self.environment_engine_log_buttons[backend].configure(state="disabled")

        def started(payload):
            if self.closing or epoch != self.environment_test_epoch:
                return
            self.environment_test_id = payload["test_id"]
            self.environment_test_state = payload.get("state") or "queued"
            self._render_environment_test_progress(payload)
            self._poll_environment_test()

        def failed(error):
            if self.closing or epoch != self.environment_test_epoch:
                return
            self.environment_test_state = "failed"
            self._set_diagnostic_size_enabled(True)
            self._activity_end("diagnostics")
            self.environment_test_button.configure(state="normal")
            self.environment_test_status_var.set("FAIL: environment test could not start. {}".format(_compact_diagnostic(error, 350)))
            self.environment_test_payload = {"state": "failed", "message": str(error)}

        def start():
            index = spec.pop("_range_warning_index", len(spec["ui_input_warnings"]))
            warnings = []
            try:
                report = local_work()
                spec["variables"] = snapshot_copy(report.get("variables") or [])
                warnings = ["Parameters: {}".format(issue.get("message") if isinstance(issue, dict) else issue)
                            for issue in report.get("issues") or []]
            except Exception as error:
                warnings.append("Variables: {}".format(error))
            spec["ui_input_warnings"][index:index] = warnings
            return method(prepared_id, spec)
        self.tasks.submit_monitor(start, started, failed)

    def _cancel_environment_test_poll(self):
        identifier = getattr(self, "environment_test_poll_job", None)
        if identifier is not None:
            try:
                self.root.after_cancel(identifier)
            except tk.TclError:
                pass
            self.environment_test_poll_job = None

    def _poll_environment_test(self):
        self._cancel_environment_test_poll()
        if (self.closing or getattr(self.tasks, "shutting_down", False) or
                not self.environment_test_id or self.environment_test_inflight):
            return
        self.environment_test_inflight = True
        test_id, epoch = self.environment_test_id, self.environment_test_epoch

        def loaded(payload):
            if self.closing or test_id != self.environment_test_id or epoch != self.environment_test_epoch:
                return
            self.environment_test_inflight = False
            self._render_environment_test_progress(payload)
            if (self.environment_test_state not in ("completed", "failed", "cancelled", "stopped")
                    or payload.get("report_ready") is False and not payload.get("report_write_error")):
                self.environment_test_poll_job = self.root.after(1000, self._poll_environment_test)

        def failed(error):
            if self.closing or test_id != self.environment_test_id or epoch != self.environment_test_epoch:
                return
            self.environment_test_inflight = False
            self.environment_test_status_var.set("Test progress unavailable: {}. Main log remains available.".format(_compact_diagnostic(error, 250)))
            if self.environment_test_state not in ("completed", "failed", "cancelled", "stopped"):
                self.environment_test_poll_job = self.root.after(2000, self._poll_environment_test)

        self.tasks.submit_monitor(lambda: self.service.environment_test_progress(test_id), loaded, failed)

    def _render_environment_test_progress(self, payload):
        self.environment_test_payload = dict(payload)
        self.environment_test_state = payload.get("state") or self.environment_test_state
        terminal = self.environment_test_state in ("completed", "failed", "cancelled", "stopped")
        self._set_diagnostic_size_enabled(terminal)
        stage = payload.get("stage") or self.environment_test_state
        message = "Diagnostics: {} · {}".format(stage, payload.get("message") or "")
        self._activity_event(message, "ERROR" if self.environment_test_state == "failed" else "INFO", key="diagnostic-stage")
        if terminal:
            self._activity_update("diagnostics", label=message, gates=payload.get("gate_progress"))
            self._activity_finish("diagnostics")
        else:
            self._activity_begin("diagnostics", message, priority=15, gates=payload.get("gate_progress"))
        for check in payload.get("checks") or []:
            if str(check.get("status") or "").upper() in ("FAIL", "BLOCKED", "WARN"):
                level = "WARN" if check.get("status") == "WARN" else "ERROR"
                self._activity_event("Diagnostics: " + str(check.get("message") or check.get("name") or ""), level,
                    key="diagnostic-check:" + str(check.get("id") or check.get("check_id")))
        self.environment_test_button.configure(state="normal" if terminal else "disabled")
        self.environment_test_cancel_button.configure(state="normal" if self.environment_test_id and not terminal else "disabled")
        path = payload.get("main_log")
        self.environment_test_log_button.configure(state="normal" if path else "disabled")
        self.environment_test_log_var.set("诊断日志：{}".format(path or "正在准备…"))
        engine_logs = payload.get("simulator_logs") or payload.get("engine_logs") or {}
        for backend, variable in self.environment_engine_log_vars.items():
            engine_path = path if backend == "main" else engine_logs.get(backend)
            variable.set(engine_path or "Not created yet.")
            self.environment_engine_log_buttons[backend].configure(state="normal" if engine_path else "disabled")
        cases = payload.get("case_run_ids") or {}
        cases = dict((str(key), str(value)) for key, value in cases.items() if value) if isinstance(cases, dict) else {}
        self.diagnostic_case_combo.configure(values=sorted(cases))
        if self.diagnostic_case_var.get() not in cases:
            self.diagnostic_case_var.set(sorted(cases)[0] if cases else "")
        self.diagnostic_results_button.configure(state="normal" if cases else "disabled")
        timing_lines = []
        for case in payload.get("case_results") or []:
            timing = case.get("timing") or {}
            name = case.get("case_id") or "{}.{}".format(case.get("simulator") or "?", case.get("engine") or "?")
            total = finite_number(timing.get("total_seconds"))
            if not timing.get("available") or total is None:
                timing_lines.append("{}：尚无计时".format(name))
                continue
            phases = timing.get("phases_seconds") or {}
            parts = []
            for phase in ("readiness", "registration", "execution", "verification", "cleanup"):
                seconds = finite_number(phases.get(phase))
                if seconds is not None:
                    parts.append("{} {:.1f}s".format(phase, seconds))
            active = timing.get("active_phase")
            timing_lines.append("{}：总计 {:.1f}s{}{}".format(
                name, total, " · " + " / ".join(parts) if parts else "",
                " · 当前 " + str(active) if active else ""))
        self.diagnostic_timing_var.set("各模式耗时：\n" + "\n".join(timing_lines) if timing_lines else "各模式耗时：等待数据。")
        runs = payload.get("run_ids") or []
        if isinstance(runs, dict):
            runs = ["{}: {}".format(key, value) for key, value in sorted(runs.items())]
        self.environment_test_runs_var.set("本次诊断任务：{}".format(
            "; ".join(str(value) for value in runs) or "none yet"))
        signature = (payload.get("test_id"), payload.get("checks_revision"), payload.get("state"))
        if payload.get("checks_revision") is None:
            signature = tuple((row.get("id") or row.get("check_id"), row.get("status"), row.get("message"),
                               str(row.get("presentation") or {})) for row in payload.get("checks") or [])
        if signature != getattr(self, "environment_checks_signature", None):
            self.environment_checks_signature = signature
            self.environment_check_counts = self._render_environment_test_checks(payload)
        counts = getattr(self, "environment_check_counts", {})
        status_names = {"PASS": "通过", "FAIL": "失败", "WARN": "需留意", "BLOCKED": "阻塞", "NOT_TESTED": "未执行", "CANCELLED": "已停止"}
        summary_text = " · ".join("{} {}".format(status_names.get(key, key), value) for key, value in counts.items())
        causes = [row for row in payload.get("case_results") or []
                  if isinstance(row, dict) and isinstance(row.get("first_cause"), dict)]
        if causes:
            first = causes[0]
            primary = first["first_cause"]
            label = primary.get("message") or primary.get("failure_stage") or first.get("first_cause_layer")
            if label:
                summary_text += " · {}: {}".format(str(first.get("simulator") or "").upper(), _compact_diagnostic(label, 120))
        state_names = {"queued": "等待诊断", "starting": "正在启动", "running": "诊断中", "cancelling": "正在停止", "stopping": "正在停止", "completed": "诊断完成", "failed": "诊断发现问题", "cancelled": "诊断已停止", "stopped": "诊断已停止"}
        self.environment_test_status_var.set("{} · {}".format(state_names.get(self.environment_test_state, self.environment_test_state), summary_text or "正在收集检查结果…"))
        ladder = getattr(self, "environment_test_ladder_var", None)
        if ladder is not None:
            ladder.set(_diagnostic_ladder_text(payload))
        widget = self.environment_test_log_text
        if widget is not None:
            try:
                engine = self.environment_test_log_engine
                if engine:
                    log_path = engine_logs.get(engine)
                    tail = (payload.get("simulator_log_tails") or payload.get("engine_log_tails") or {}).get(engine)
                    if not tail:
                        tail = "Waiting for this engine's report contents. The full log path is available above."
                else:
                    log_path, tail = path, payload.get("log_tail") or "Waiting for report contents..."
                content = "Report: {}\n\n{}".format(log_path or "not created", str(tail)[-65536:])
                signature = (str(widget), content)
                if signature != getattr(self, "environment_test_log_signature", None):
                    position = widget.yview()
                    following = not position or position[1] >= 0.99
                    widget.configure(state="normal")
                    widget.delete("1.0", "end")
                    widget.insert("1.0", content)
                    widget.configure(state="disabled")
                    if following:
                        widget.see("end")
                    elif position:
                        widget.yview_moveto(position[0])
                    self.environment_test_log_signature = signature
            except tk.TclError:
                self.environment_test_log_text = None
                self.environment_test_window = None

    def show_diagnostic_results(self):
        """Read a selected diagnostic run without changing the active formal run."""
        case = self.diagnostic_case_var.get()
        run_id = (self.environment_test_payload.get("case_run_ids") or {}).get(case)
        if not run_id or self.closing:
            return
        self.dataset_load_epoch += 1
        epoch, test_id = self.dataset_load_epoch, self.environment_test_id
        self.environment_test_status_var.set("正在读取 {} 的结果…".format(case))

        def loaded(payload):
            if (self.closing or epoch != self.dataset_load_epoch or test_id != self.environment_test_id
                    or case != self.diagnostic_case_var.get()
                    or (self.environment_test_payload.get("case_run_ids") or {}).get(case) != run_id):
                return
            self.display_dataset = {"metadata": {"spec": payload.get("spec") or {}, "display_name": "诊断 " + case},
                                    "candidates": payload.get("candidates") or [],
                                    "objective_presentation": payload.get("objective_presentation") or {},
                                    "source_path": payload.get("dataset_path")}
            self.selected_candidate_id = None
            self.result_page = 0
            self.export_csv_button.configure(state="normal")
            self.export_data_button.configure(state="normal" if payload.get("dataset_path") else "disabled")
            self.results_failure_var.set("诊断结果：{} · {}（点击诊断页按钮刷新）".format(case, run_id))
            self.render_results()
            self.render_pareto()
            self.show_page("Live Results")
            self.environment_test_status_var.set("已读取 {} 的诊断结果。".format(case))

        def failed(error):
            if not self.closing and epoch == self.dataset_load_epoch and test_id == self.environment_test_id:
                self.environment_test_status_var.set("结果读取失败：{}".format(_compact_diagnostic(error, 250)))

        getattr(self.tasks, "submit_history", self.tasks.submit)(lambda: self.service.progress(run_id), loaded, failed)

    def _render_environment_test_checks(self, payload):
        checks = payload.get("checks") or []
        tree = self.environment_test_tree
        selected = tree.selection()
        previous_scroll = tree.yview()
        open_items = set()
        def remember(parent=""):
            for item in tree.get_children(parent):
                if tree.item(item, "open"):
                    open_items.add(item)
                remember(item)
        remember()
        previous_items = set(self.environment_test_checks) | set(getattr(self, "environment_test_sections", {}))
        tree.delete(*tree.get_children(""))
        self.environment_test_checks, self.environment_test_sections = {}, {}
        counts, groups, sections, tiers = {}, {}, {}, {}
        status_font = ttk.Style(tree).lookup("Treeview", "font") or "TkDefaultFont"
        status_width, status_widths = 130, {}
        priority = {"FAIL": 0, "BLOCKED": 1, "WARN": 2, "NOT_TESTED": 3, "CANCELLED": 4, "PASS": 5}
        ordered_checks = sorted((row for row in checks if isinstance(row, dict)),
            key=lambda row: priority.get(str(row.get("status") or "NOT_TESTED").upper(), 3))
        for index, check in enumerate(ordered_checks):
            if not isinstance(check, dict):
                continue
            status = str(check.get("status") or "NOT_TESTED").upper().replace(" ", "_")
            counts[status] = counts.get(status, 0) + 1
            presentation = self._environment_test_presentation(check)
            group_id = str(presentation.get("group_id") or "environment")
            section_id = str(presentation.get("section_id") or "other")
            tier = "problem" if status in ("FAIL", "BLOCKED") else ("success" if status == "PASS" else "attention")
            tier_item = "environment_priority:" + tier
            if tier_item not in tiers:
                tiers[tier_item] = []
                tree.insert("", "end", iid=tier_item, text={"problem": "失败 / 阻塞", "attention": "需要留意 / 尚未执行", "success": "已通过（可展开查看）"}[tier])
            tiers[tier_item].append(check)
            group = "environment_group:" + tier + ":" + group_id
            section = "environment_section:" + tier + ":" + group_id + ":" + section_id
            if group not in groups:
                groups[group] = []
                tree.insert(tier_item, "end", iid=group, text=presentation.get("group_label") or "运行环境",
                            open=(group in open_items or group not in previous_items))
            if section not in sections:
                sections[section] = []
                tree.insert(group, "end", iid=section, text=presentation.get("section_label") or "其他检查")
            groups[group].append(check)
            sections[section].append(check)
            item = "environment_check:" + str(check.get("id") or check.get("check_id") or index)
            if tree.exists(item):
                item += ":" + str(index)
            self.environment_test_checks[item] = check
            tag = "problem" if status in ("FAIL", "BLOCKED") else ("success" if status == "PASS" else "warning")
            status_text = presentation.get("status_label") or status
            if status_text not in status_widths:
                status_widths[status_text] = int(tree.tk.call("font", "measure", status_font, status_text)) + 20
            status_width = max(status_width, status_widths[status_text])
            tree.insert(section, "end", iid=item,
                text=presentation.get("title") or check.get("name") or "检查项",
                values=(status_text,
                        presentation.get("section_label") or "",
                        _compact_diagnostic(presentation.get("summary") or check.get("message") or "", 240)), tags=(tag,))
        tree.column("status", width=status_width, minwidth=status_width)
        self.environment_test_sections.update(tiers)
        self.environment_test_sections.update(groups)
        self.environment_test_sections.update(sections)
        status_names = {"PASS": "通过", "FAIL": "失败", "WARN": "需留意", "BLOCKED": "阻塞", "NOT_TESTED": "未执行", "CANCELLED": "已停止"}
        for item, rows in list(tiers.items()) + list(groups.items()) + list(sections.items()):
            tallies = {}
            for check in rows:
                status = str(check.get("status") or "NOT_TESTED").upper()
                tallies[status] = tallies.get(status, 0) + 1
            summary = " · ".join("{} {}".format(status_names.get(key, key), value) for key, value in tallies.items())
            problem = any(key in tallies for key in ("FAIL", "BLOCKED", "WARN"))
            tag = "problem" if any(key in tallies for key in ("FAIL", "BLOCKED")) else ("success" if set(tallies) == {"PASS"} else "warning")
            tree.item(item, values=(summary, "", "点击查看本小节说明与证据"), tags=(tag,),
                      open=(item in open_items if item in previous_items else problem))
        if selected and tree.exists(selected[0]):
            tree.selection_set(selected[0])
            tree.focus(selected[0])
        if previous_items and previous_scroll:
            tree.yview_moveto(previous_scroll[0])
        self._environment_test_selected()
        return counts

    def stop_environment_test(self):
        if self.closing or not self.environment_test_id or self.environment_test_state in ("completed", "failed", "cancelled", "stopped"):
            return
        test_id = self.environment_test_id
        self.environment_test_cancel_button.configure(state="disabled")
        self.environment_test_status_var.set("Cancellation requested; proving owned tasks have stopped...")
        self.environment_test_state = "cancelling"

        def stopped(_payload):
            if not self.closing and test_id == self.environment_test_id:
                self._poll_environment_test()

        def failed(error):
            if not self.closing and test_id == self.environment_test_id:
                self.environment_test_cancel_button.configure(state="normal")
                self.environment_test_status_var.set("Cancellation needs attention: {}".format(_compact_diagnostic(error, 300)))
                self._poll_environment_test()

        self.tasks.submit_control(lambda: self.service.stop_environment_test(test_id), stopped, failed)

    def show_environment_test_details(self):
        checks = self._selected_environment_test_checks()
        if checks:
            self._environment_test_selected()
            return self.environment_test_detail_frame
        return self._show_text_details("仿真诊断说明", "请在功能树中选择问题条目或功能小节。说明栏会显示原因、相关文件、证据与本次网表路径。\n\n"
            + self.environment_test_status_var.get())

    def copy_environment_test_log_path(self, engine=None):
        path = self.environment_test_payload.get("main_log") or (
            (self.environment_test_payload.get("simulator_logs") or self.environment_test_payload.get("engine_logs") or {}).get(engine))
        if path:
            self.root.clipboard_clear()
            self.root.clipboard_append(path)
        else:
            self.environment_test_status_var.set("Main report has not been created yet.")

    def copy_environment_test_log_paths(self):
        logs = self.environment_test_payload.get("simulator_logs") or self.environment_test_payload.get("engine_logs") or {}
        keys = ("spectre", "alps") if "spectre" in logs or "alps" in logs else ("persistent_maestro", "private_netlist")
        main = self.environment_test_payload.get("main_log")
        paths = [main] if main else list(dict.fromkeys(logs.get(engine) for engine in keys if logs.get(engine)))
        if paths:
            self.root.clipboard_clear()
            self.root.clipboard_append("\n".join(paths))
        else:
            self.environment_test_status_var.set("Diagnostic logs have not been created yet.")

    def show_environment_test_log(self, engine=None):
        if engine == "main":
            engine = None
        self.environment_test_log_engine = engine
        window = self.environment_test_window
        if window is not None:
            try:
                window.title("Environment Test · {}".format(
                    {"spectre": "Spectre Log", "alps": "ALPS Log", "persistent_maestro": "Maestro Log", "private_netlist": "Private Netlist Log"}.get(engine, "诊断总日志")))
                window.lift()
                self._render_environment_test_progress(self.environment_test_payload)
                self._poll_environment_test()
                return window
            except tk.TclError:
                pass
        window = tk.Toplevel(self.root)
        window.title("Environment Test · {}".format(
            {"spectre": "Spectre Log", "alps": "ALPS Log", "persistent_maestro": "Maestro Log", "private_netlist": "Private Netlist Log"}.get(engine, "诊断总日志")))
        window.geometry("1040x640")
        text = tk.Text(window, wrap="none", state="disabled", font=("TkFixedFont", 10), undo=False)
        y_scroll = ttk.Scrollbar(window, orient="vertical", command=text.yview)
        x_scroll = ttk.Scrollbar(window, orient="horizontal", command=text.xview)
        text.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        text.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=(10, 0))
        y_scroll.grid(row=0, column=1, sticky="ns", pady=(10, 0))
        x_scroll.grid(row=1, column=0, sticky="ew", padx=(10, 0))
        actions = ttk.Frame(window)
        actions.grid(row=2, column=0, columnspan=2, pady=10)
        ttk.Label(actions, text="Read-only report tail · refreshes while the test is active.").pack(side="left", padx=8)
        ttk.Button(actions, text="Refresh", command=self._poll_environment_test).pack(side="left", padx=8)
        self.environment_test_window = window
        self.environment_test_log_text = text
        self.environment_test_log_signature = None

        def close():
            self.environment_test_window = None
            self.environment_test_log_text = None
            window.destroy()

        window.protocol("WM_DELETE_WINDOW", close)
        ttk.Button(actions, text="Close", command=close).pack(side="left")
        window.rowconfigure(0, weight=1)
        window.columnconfigure(0, weight=1)
        self._render_environment_test_progress(self.environment_test_payload)
        self._poll_environment_test()
        return window

    def _build_results_page(self, parent):
        self._page_heading(
            parent, "Live Results",
            "Inspect every candidate, objective, and constraint. Right-click a candidate for its live simulator log.",
        )
        toolbar = self._card(parent, padding=(12, 9))
        self.export_csv_button = ttk.Button(toolbar, text="Export CSV...", command=self.export_csv, state="disabled")
        self.export_csv_button.pack(side="left")
        self.export_data_button = ttk.Button(toolbar, text="Export Results...", command=self.export_results_data, state="disabled")
        self.export_data_button.pack(side="left", padx=6)
        self.load_data_button = ttk.Button(toolbar, text="Load Results...", command=self.load_results_data)
        self.load_data_button.pack(side="left")
        self.live_data_button = ttk.Button(toolbar, text="Show Current Run", command=self.show_current_results)
        self.live_data_button.pack(side="left", padx=6)
        self.results_summary_var = tk.StringVar(value="No candidate data.")
        ttk.Label(toolbar, textvariable=self.results_summary_var, style="CardMuted.TLabel").pack(side="left", padx=12)
        ttk.Button(toolbar, text="Previous", command=lambda: self.change_result_page(-1)).pack(side="right")
        ttk.Button(toolbar, text="Next", command=lambda: self.change_result_page(1)).pack(side="right", padx=4)
        self.result_page_var = tk.StringVar(value="Page 1")
        ttk.Label(toolbar, textvariable=self.result_page_var, style="CardMuted.TLabel").pack(side="right", padx=8)

        self.results_failure_var = tk.StringVar(value="")
        ttk.Label(parent, textvariable=self.results_failure_var, foreground=DANGER,
                  wraplength=950, justify="left").pack(fill="x", padx=24, pady=(0, 8))

        trend_card = self._card(parent, padding=(8, 8))
        ttk.Label(trend_card, text="Objective by generation · dots: samples; line: mean", style="CardTitle.TLabel").pack(anchor="w")
        self.result_trend = tk.Canvas(trend_card, width=780, height=190, background="#fbfdff", highlightbackground="#d5e0e8", highlightthickness=1)
        self.result_trend.pack(fill="x", expand=True)
        result_card = self._card(parent, padding=(8, 8))
        columns = ("generation", "status", "feasible", "objectives", "constraints")
        self.result_tree = ttk.Treeview(result_card, columns=columns, show="tree headings", height=18, selectmode="browse")
        headings = {
            "#0": "Candidate", "generation": "Generation", "status": "Status",
            "feasible": "Feasible", "objectives": "Objectives", "constraints": "Constraints",
        }
        widths = {"#0": 210, "generation": 80, "status": 250, "feasible": 85, "objectives": 270, "constraints": 270}
        for column, label in headings.items():
            self.result_tree.heading(column, text=label)
            self.result_tree.column(column, width=widths[column], stretch=column in ("#0", "objectives", "constraints"))
        y_scroll = ttk.Scrollbar(result_card, orient="vertical", command=self.result_tree.yview)
        x_scroll = ttk.Scrollbar(result_card, orient="horizontal", command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.result_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        result_card.rowconfigure(0, weight=1)
        result_card.columnconfigure(0, weight=1)
        self.result_tree.bind("<<TreeviewSelect>>", self._result_selected)
        self.result_tree.bind("<Button-3>", self._result_context_menu)

        detail = self._card(parent)
        detail_header = ttk.Frame(detail, style="Card.TFrame")
        detail_header.pack(fill="x")
        ttk.Label(detail_header, text="Candidate Diagnostics & Variables", style="CardTitle.TLabel").pack(side="left")
        self.optimized_schematic_button = ttk.Button(
            detail_header, text="Generate Optimized Schematic Cellviews",
            command=self.generate_optimized_schematic, state="disabled",
        )
        self.optimized_schematic_button.pack(side="right")
        self.optimized_schematic_status_var = tk.StringVar(value="Select a completed real candidate after optimization stops.")
        ttk.Label(detail, textvariable=self.optimized_schematic_status_var, style="CardMuted.TLabel",
                  wraplength=950, justify="left").pack(fill="x", pady=(6, 0))
        self.candidate_detail = tk.Text(
            detail, height=11, wrap="none", state="disabled", background="#fbfdff",
            foreground=TEXT, font=("TkFixedFont", 10), relief="flat",
        )
        self.candidate_detail.pack(fill="x", pady=(8, 0))

    def _build_pareto_page(self, parent):
        self._page_heading(
            parent, "Pareto Trade-offs",
            "All objective pairs are shown automatically; emphasized points belong to the full-dimensional Pareto front.",
        )
        self.pareto_notice_var = tk.StringVar(value="Add at least two objectives to enable pairwise trade-offs.")
        ttk.Label(parent, textvariable=self.pareto_notice_var, style="Muted.TLabel", wraplength=950).pack(anchor="w", padx=24, pady=8)
        self.pareto_container = ttk.Frame(parent, style="Page.TFrame")
        self.pareto_container.pack(fill="both", expand=True, padx=22, pady=(0, 16))
        self.pareto_plots = []

    def _build_settings_page(self, parent):
        self._page_heading(parent, "Settings", "Private Maestro settings, EDA environment and Crescendo diagnostics. Automatic values are retained unless explicitly overridden.")
        tabs = ttk.Notebook(parent)
        tabs.pack(fill="both", expand=True, padx=22, pady=(4, 14))
        self.settings_sections = {}
        for key, label in (("maestro", "Private Maestro"), ("eda", "EDA"), ("crescendo", "Crescendo / Diagnostics")):
            section = ttk.Frame(tabs, style="Page.TFrame")
            tabs.add(section, text=label)
            self.settings_sections[key] = section
        self.settings_notebook = tabs
        private = self.settings_sections["maestro"]
        inherited = self._card(private)
        ttk.Label(inherited, text="Private setup inheritance", style="CardTitle.TLabel").pack(anchor="w")
        self.private_settings_summary_var = tk.StringVar(value="Load and Inspect reads the saved Maestro settings and creates an owned private copy.")
        ttk.Label(inherited, textvariable=self.private_settings_summary_var, style="Help.TLabel", wraplength=1000).pack(anchor="w", pady=(4, 0))
        ttk.Button(inherited, text="Native Settings Details...", command=self._show_native_settings).pack(anchor="w", pady=(6, 0))
        self._build_job_policy_card(private)
        self._build_save_options_card(private)
        self._build_eda_page(self.settings_sections["eda"])
        self._build_runtime_settings(self.settings_sections["crescendo"])
        self._build_execution_commands_card(self.settings_sections["crescendo"])
        self._build_environment_test_card(self.settings_sections["crescendo"])
        self._build_execution_details_card(self.settings_sections["crescendo"])

    def _build_runtime_settings(self, parent):
        engine = self._card(parent)
        ttk.Label(engine, text="Netlist Source / Simulator", style="CardTitle.TLabel").grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 6))
        self.execution_backend_var = tk.StringVar(value="auto")
        mode_choices = ttk.Frame(engine, style="Card.TFrame")
        mode_choices.grid(row=1, column=0, columnspan=2, sticky="w")
        for mode_text, mode_value in (("Private Maestro", "auto"),
                                      ("Private Netlist", "private_netlist"),
                                      ("Private Maestro (Baseline)", "maestro_baseline")):
            ttk.Radiobutton(mode_choices, text=mode_text, variable=self.execution_backend_var,
                            value=mode_value).pack(anchor="w", pady=2)
        self.simulator_var = tk.StringVar(value=(getattr(self.service, "config", {}).get("cadence") or {}).get("simulator", "spectre"))
        ttk.Label(engine, text="Simulator", style="Card.TLabel").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.simulator_combo = ttk.Combobox(engine, textvariable=self.simulator_var, values=("spectre", "alps"),
                     state="readonly", width=22)
        self.simulator_combo.grid(row=2, column=1, sticky="w", pady=(8, 0))
        self.simulator_profile_var = tk.StringVar(value="Detecting both simulator profiles...")
        ttk.Label(engine, textvariable=self.simulator_profile_var, style="Help.TLabel", font="TkDefaultFont",
                  wraplength=950, justify="left").grid(row=4, column=0, columnspan=2, sticky="w", pady=(6, 0))
        self.simulator_combo.bind("<<ComboboxSelected>>", lambda _event: self._render_simulator_profiles())
        ttk.Label(engine, text=(
            "Private Maestro regenerates a numeric .scs for each candidate from its private schematic and config. "
            "Private Netlist modifies a qualified frozen template. Baseline runs native Maestro with the source simulator policy and reads native results. "
            "Set each simulator executable and model directory in Settings / EDA."
        ), style="Help.TLabel", wraplength=950).grid(row=3, column=0, columnspan=2, sticky="w", pady=(6, 0))

        budget = self._card(parent)
        ttk.Label(budget, text="Preparation / measurement fallback budget (s)", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Entry(budget, textvariable=self.timeout_var, width=14).pack(anchor="w", pady=(5, 3))
        ttk.Label(budget, text="Positive seconds, or empty for unlimited. Used when a preparation/result-reading limit is unspecified. Solver Run keeps its Job Setup limit; cluster queue time is separate.", style="Help.TLabel", wraplength=1000).pack(anchor="w")

    def _build_eda_page(self, parent):
        description = "Detected values and their sources are shown above optional manual overrides. Empty override fields keep automatic discovery."
        if self.launch_context:
            description += " Settings stay local to this optimizer. The native toolchain comes from the invoking Maestro; library mapping follows the selected setup."
        self._page_heading(
            parent, "EDA",
            description,
        )
        status = self._card(parent)
        self.eda_status_var = tk.StringVar(value="Detecting Cadence and PDK environment...")
        ttk.Label(status, textvariable=self.eda_status_var, style="Help.TLabel", wraplength=950).pack(anchor="w")
        form = self._card(parent)
        self.eda_vars = {}
        self.eda_effective_vars = {}
        fields = (
            ("cadence_root", "Cadence Root"), ("spectre_root", "Spectre Root"),
            ("virtuoso_bin", "Virtuoso Executable"), ("ocean_bin", "Ocean Executable"),
            ("spectre_bin", "Spectre Executable"), ("alps_bin", "ALPS Executable"),
            ("alps_root", "ALPS Installation Root"), ("alps_model_dir", "ALPS Model Directory"),
            ("spectre_model_dir", "Spectre Model Directory (optional)"),
            ("simulator_threads", "Simulator Threads (empty = Auto)"),
            ("simulator_result_reader", "Ocean 结果读取接口（spectre / alps）"),
            ("spectre_result_reader", "Spectre 结果读取接口（留空继承）"),
            ("alps_result_reader", "ALPS 结果读取接口（留空继承；可选 spectre）"),
            ("cds_lib", "Library Mapping cds.lib"),
            ("license_file", "License File or port@server"), ("pdk_name", "PDK Name"),
            ("pdk_dir", "PDK Root"), ("pdk_adapter", "Custom Adapter SKILL"),
        )
        field_help = {
            "cadence_root": "Cadence installation root. Empty keeps the detected native installation.",
            "spectre_root": "Spectre installation root; executable overrides below take precedence.",
            "virtuoso_bin": "Virtuoso used for schematic and hierarchy inspection.",
            "ocean_bin": "Ocean used to read the saved setup and run owned private sessions.",
            "spectre_bin": "Spectre solver executable; no system installation is changed.",
            "alps_bin": "ALPS solver executable for the ALPS selection.",
            "alps_root": "ALPS installation directory used when automatic discovery needs a hint.",
            "alps_model_dir": "ALPS model directory for matching the current design models.",
            "spectre_model_dir": "Optional Spectre model directory. Empty inherits setup/model discovery.",
            "simulator_threads": "Threads per solver task; separate from the number of concurrent jobs.",
            "simulator_result_reader": "Default Ocean interface for reading native simulation results.",
            "spectre_result_reader": "Override the default reader only for Spectre results.",
            "alps_result_reader": "Override the default reader only for ALPS results.",
            "cds_lib": "Library mapping for this selected saved Maestro and its private copy.",
            "license_file": "License input inherited by owned EDA processes; empty retains discovery.",
            "pdk_name": "PDK profile name used to select model and parameter adapters.",
            "pdk_dir": "PDK files used by this design; empty keeps the detected root.",
            "pdk_adapter": "Optional SKILL adapter for PDK-specific parameter discovery.",
        }
        for index, (key, label) in enumerate(fields):
            row, column = divmod(index, 2)
            field = ttk.Frame(form, style="Card.TFrame")
            field.grid(row=row, column=column, sticky="new", padx=(0 if column == 0 else 8, 8 if column == 0 else 0), pady=5)
            ttk.Label(field, text=label, style="Card.TLabel").pack(anchor="w")
            detected = tk.StringVar(value="Detecting...")
            self.eda_effective_vars[key] = detected
            ttk.Label(field, textvariable=detected, style="Help.TLabel", wraplength=470, justify="left").pack(anchor="w", pady=(2, 3))
            variable = tk.StringVar(value="")
            self.eda_vars[key] = variable
            ttk.Entry(field, textvariable=variable).pack(fill="x", pady=(2, 0))
            ttk.Label(field, text=field_help[key], style="Help.TLabel", wraplength=470, justify="left").pack(anchor="w", pady=(2, 0))
        form.columnconfigure(0, weight=1)
        form.columnconfigure(1, weight=1)
        profile_card = self._card(parent)
        ttk.Label(profile_card, text="Detected Simulator Profiles", style="CardTitle.TLabel").pack(anchor="w")
        self.eda_simulator_profiles_var = tk.StringVar(value="Detecting both profiles...")
        ttk.Label(profile_card, textvariable=self.eda_simulator_profiles_var, style="Help.TLabel",
                  wraplength=950, justify="left").pack(anchor="w", pady=(5, 0))
        extras = self._card(parent)
        self.eda_policy_var = tk.StringVar(value="auto")
        ttk.Label(extras, text="PDK Adapter Policy", style="Card.TLabel").grid(row=0, column=0, sticky="w")
        self.eda_policy_combo = ttk.Combobox(extras, textvariable=self.eda_policy_var, values=("auto", "generic-only", "custom"), state="readonly")
        self.eda_policy_combo.grid(row=1, column=0, sticky="ew", padx=(0, 8))
        ttk.Label(extras, text="Auto selects a compatible adapter; generic-only skips PDK callbacks; custom uses the SKILL path above.", style="Help.TLabel", wraplength=230).grid(row=2, column=0, sticky="nw", pady=4)
        self.eda_binary_paths = tk.Text(extras, height=4, width=35)
        self.eda_library_paths = tk.Text(extras, height=4, width=35)
        self.eda_profiles = tk.Text(extras, height=4, width=35)
        for column, (label, widget) in enumerate((
                ("Additional Binary Paths", self.eda_binary_paths),
                ("Additional Library Paths", self.eda_library_paths),
                ("PDK Profiles JSON", self.eda_profiles)), start=1):
            ttk.Label(extras, text=label, style="Card.TLabel").grid(row=0, column=column, sticky="w", padx=4)
            widget.grid(row=1, column=column, sticky="ew", padx=4)
            help_text = ("One directory per line, added only to owned EDA executables search.", "One directory per line for owned EDA shared libraries.", "Optional local PDK profiles as JSON; empty keeps automatic profiles.")[column - 1]
            ttk.Label(extras, text=help_text, style="Help.TLabel", wraplength=230).grid(row=2, column=column, sticky="nw", padx=4, pady=4)
            extras.columnconfigure(column, weight=1)
        models = self._card(parent)
        ttk.Label(models, text="ALPS Model File Mapping JSON", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(models, text="Automatic model pairing uses verified model and section evidence. Manual entries accept an ALPS path, "
                  "{path, section}, or {path, sections: {sourceSection: targetSection}}. Unqualified mappings block Start.",
                  style="Help.TLabel", wraplength=950).pack(anchor="w", pady=(4, 5))
        self.eda_alps_model_map = tk.Text(models, height=5, width=85)
        self.eda_alps_model_map.pack(fill="x")
        actions = self._card(parent, padding=(12, 9))
        self.save_eda_button = ttk.Button(actions, text="Validate and Save Overrides", command=self.save_eda_setup)
        self.save_eda_button.pack(side="left")
        ttk.Button(actions, text="Reset to Auto", command=self.reset_eda_setup).pack(side="left", padx=6)

    def _build_job_policy_card(self, parent):
        card = self._card(parent)
        ttk.Label(card, text="Maestro Job Setup", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=4, sticky="w")
        self.job_policy_status_var = tk.StringVar(value="Original submission settings have not been read.")
        ttk.Label(card, textvariable=self.job_policy_status_var, style="Help.TLabel", wraplength=950).grid(
            row=1, column=0, columnspan=4, sticky="w", pady=(5, 7))
        self.job_command_override_var = tk.BooleanVar(value=False)
        self.job_max_override_var = tk.BooleanVar(value=False)
        self.job_command_var = tk.StringVar(value="")
        self.job_max_var = tk.StringVar(value="")
        self.job_override_controls = []
        job_command_check = ttk.Label(card, text="Command", style="Card.TLabel")
        job_command_check.grid(row=2, column=0, sticky="w")
        self.job_command_entry = ttk.Entry(card, textvariable=self.job_command_var, style="Crescendo.Input.TEntry")
        self.job_command_entry.grid(row=2, column=1, columnspan=3, sticky="ew", pady=4)
        job_max_check = ttk.Label(card, text="Max jobs", style="Card.TLabel")
        job_max_check.grid(row=3, column=0, sticky="w")
        self.job_override_controls.append(self.job_command_entry)
        self.job_max_entry = ttk.Entry(card, textvariable=self.job_max_var, width=12, style="Crescendo.Input.TEntry")
        self.job_max_entry.grid(row=3, column=1, sticky="w", pady=4)
        self.job_override_controls.append(self.job_max_entry)
        self.maestro_candidate_mode_var = tk.StringVar(value="auto")
        ttk.Label(card, text="Solver dispatch", style="Card.TLabel").grid(row=3, column=2, sticky="e", padx=8)
        self.shared_task_mode_var = tk.StringVar(value="Shared CLI tasks")
        self.shared_task_mode_label = ttk.Label(card, textvariable=self.shared_task_mode_var, style="Help.TLabel")
        self.shared_task_mode_label.grid(row=3, column=3, sticky="w")
        ttk.Label(card, text=(
            "Captured settings are prefilled. The input values are used directly for each effective test. "
            "Command distribution is selected; a missing command must be entered before running. "
            "Max jobs limits shared global/per-test solver capacity, independently of population size. "
            "Both netlist sources use the same task dispatcher; saved Serial/Batch preferences do not alter this pipeline."
        ), style="Help.TLabel", wraplength=950).grid(row=4, column=0, columnspan=4, sticky="w", pady=(5, 7))
        self.job_policy_tree = ttk.Treeview(card, columns=("distribution", "max_jobs", "command"), show="tree headings", height=4)
        for name, label, width in (("#0", "Original effective policy", 220), ("distribution", "Distribution", 120),
                                   ("max_jobs", "Max jobs", 90), ("command", "Command", 500)):
            self.job_policy_tree.heading(name, text=label)
            self.job_policy_tree.column(name, width=width, stretch=name == "command")
        self.job_policy_tree.grid(row=5, column=0, columnspan=4, sticky="ew")
        self.job_timeout_vars, self.job_timeout_override_vars = {}, {}
        self.job_timeout_effective_vars = {}
        for index, (key, label) in enumerate((("start_seconds", "Native capture start timeout (s)"),
                ("configure_seconds", "Native capture configure timeout (s)"),
                ("run_seconds", "Solver run timeout (s)"),
                ("linger_seconds", "Native ICRP linger (s)")), 6):
            self.job_timeout_override_vars[key] = tk.BooleanVar(value=False)
            self.job_timeout_vars[key] = tk.StringVar(value="")
            self.job_timeout_effective_vars[key] = tk.StringVar(value="Original value unavailable")
            check = ttk.Label(card, text=label, style="Card.TLabel")
            check.grid(row=index, column=0, sticky="w", pady=3)
            entry = ttk.Entry(card, textvariable=self.job_timeout_vars[key], width=12, style="Crescendo.Input.TEntry")
            entry.grid(row=index, column=1, sticky="w", pady=3)
            self.job_override_controls.append(entry)
            ttk.Label(card, textvariable=self.job_timeout_effective_vars[key], style="Help.TLabel",
                      wraplength=630).grid(row=index, column=2, columnspan=2, sticky="w", pady=3)
        self.job_timeout_policy_var = tk.StringVar(value="")
        ttk.Label(card, textvariable=self.job_timeout_policy_var, style="Help.TLabel",
                  wraplength=950, justify="left").grid(row=10, column=0, columnspan=4, sticky="w", pady=(5, 0))
        ttk.Label(card, text="Enter positive integer seconds or leave the field empty for no limit. Zero is not allowed. "
                  "Linger controls a native ICRP idle session and does not time or kill shared CLI solver jobs. "
                  "Start/Configure apply only to native Maestro capture. CLI Run starts at exact JOB_STATE RUNNING "
                  "or the owned local solver start; PENDING has no Job Setup time limit. "
                  "Local solver execution is serial; cluster max jobs limits all submitted/running solver tasks.",
                  style="Help.TLabel", wraplength=950).grid(row=11, column=0, columnspan=4, sticky="w", pady=(5, 0))
        self.command_scheduler_var = tk.StringVar(value=((getattr(getattr(self, "service", None), "config", {}) or {}).get("runtime") or {}).get("command_scheduler", "auto"))
        ttk.Label(card, text="Command scheduler", style="Card.TLabel").grid(row=12, column=0, sticky="w", pady=(6, 0))
        self.command_scheduler_combo = ttk.Combobox(card, textvariable=self.command_scheduler_var, values=("auto", "batch", "receipt"),
                     state="readonly", width=18)
        self.command_scheduler_combo.grid(row=12, column=1, sticky="w", pady=(6, 0))
        ttk.Label(card, text="Auto uses your query/cancel templates. Batch accepts a submission script returning a unique Job ID, "
                  "then runs your configured query/cancel commands. Receipt uses the existing opaque wrapper completion contract.",
                  style="Help.TLabel", wraplength=650).grid(row=12, column=2, columnspan=2, sticky="w", pady=(6, 0))
        self.job_mode_notice_var = tk.StringVar(value="Job Setup input values apply directly to the selected mode.")
        ttk.Label(card, textvariable=self.job_mode_notice_var, style="Help.TLabel", wraplength=950).grid(
            row=13, column=0, columnspan=4, sticky="w", pady=(6, 0))
        card.columnconfigure(1, weight=1)
        for value, changed in [(self.job_command_var, self.job_command_override_var),
                               (self.job_max_var, self.job_max_override_var)] + [
                (value, self.job_timeout_override_vars[key]) for key, value in self.job_timeout_vars.items()]:
            value.trace("w", lambda *_args, changed=changed: changed.set(True)
                        if not getattr(self, "_job_prefilling", False) else None)
        self._render_job_policy((self.launch_context or {}).get("job_policy"), reset=True)

    def _build_save_options_card(self, parent):
        card = self._card(parent)
        ttk.Label(card, text="Maestro Save Options / Run Data", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=3, sticky="w")
        self.save_options_status_var = tk.StringVar(value="Original Save Options have not been read.")
        ttk.Label(card, textvariable=self.save_options_status_var, style="Help.TLabel", wraplength=950).grid(
            row=1, column=0, columnspan=3, sticky="w", pady=(5, 7))
        self.data_root_override_var = tk.BooleanVar(value=False)
        self.data_root_var = tk.StringVar(value="")
        ttk.Checkbutton(card, text="Override data root", variable=self.data_root_override_var).grid(row=2, column=0, sticky="w")
        self.data_root_entry = ttk.Entry(card, textvariable=self.data_root_var)
        self.data_root_entry.grid(row=2, column=1, sticky="ew", padx=6)
        ttk.Button(card, text="Browse...", command=self._browse_data_root).grid(row=2, column=2, sticky="w")
        ttk.Label(card, text=("Unchecked inherits the current Maestro simulation-results location. "
            "Each run uses crescendo/<TB>/<optimization>/ below this root. The chosen location is frozen at Start; "
            "source Maestro settings remain unchanged."), style="Help.TLabel", wraplength=950).grid(
            row=3, column=0, columnspan=3, sticky="w", pady=(5, 0))
        card.columnconfigure(1, weight=1)
        self._render_save_options((self.launch_context or {}).get("save_options"), reset=True)

    def _browse_data_root(self):
        path = filedialog.askdirectory(parent=self.root, title="Optimization Data Root",
                                       initialdir=self.data_root_var.get() or None)
        if path:
            self.data_root_var.set(path)
            self.data_root_override_var.set(True)

    def _render_save_options(self, options, reset=False):
        if not hasattr(self, "save_options_status_var"):
            return
        options = copy.deepcopy(options or {})
        effective = options.get("effective") or {}
        raw = options.get("raw") or {}
        simulation = effective.get("simulation_root")
        rdb = effective.get("rdb_root")
        data_root = simulation
        storage_source = options.get("source") or "unknown"
        refresh_warning = ""
        try:
            from .storage_policy import resolve_storage_policy
            config = copy.deepcopy(getattr(self.service, "config", {}))
            config.setdefault("runtime", {})["data_root_override"] = None
            policy = resolve_storage_policy(config, {"save_options": options})
            data_root, storage_source = policy.get("data_root"), policy.get("source")
            if policy.get("saved_reload_root_unconfirmed"):
                data_root, storage_source = None, "保存后路径尚未确认"
                refresh_warning = "\n原设置已重新读取，但当前保存目录未获得可信原生证据；请在 Data Root Override 确认一次，实际运行不会偷偷改用其他目录。"
                hint = ((config.get("_maestro_launch_context") or {}).get("saved_setup_refresh") or {}).get("previous_simulation_root_hint")
                if hint:
                    refresh_warning += "\n上次路径（仅供核对）：" + str(hint)
        except (OSError, ValueError):
            pass
        if reset:
            self.data_root_override_var.set(False)
        if reset or not self.data_root_override_var.get():
            self.data_root_var.set(data_root or "")
        def value(item):
            return str(item) if item not in (None, "") else "Unknown / unspecified"
        self.save_options_status_var.set("Original simulation/netlist root: {}\nOriginal RDB/log root: {}\n"
            "Saved simulation location: {} | saved RDB location: {}\nCrescendo data root: {} | source: {}".format(
                value(simulation), value(rdb), value(raw.get("save_res_dir")), value(raw.get("save_dir")),
                value(data_root), storage_source) + refresh_warning)
        self.original_save_options = options

    def _render_job_policy(self, policy, reset=False):
        if not hasattr(self, "job_policy_tree"):
            return
        policy = copy.deepcopy(policy or {})
        fingerprint = job_policy_fingerprint(policy)
        if reset:
            self.job_command_override_var.set(False)
            self.job_max_override_var.set(False)
            for variable in getattr(self, "job_timeout_override_vars", {}).values():
                variable.set(False)
        defaults = job_input_defaults(policy, [self.selected_test] if getattr(self, "selected_test", None) else None)
        self._job_prefilling = True
        if not self.job_command_override_var.get():
            self.job_command_var.set(defaults["command"])
        if not self.job_max_override_var.get():
            self.job_max_var.set(str(defaults["max_jobs"]))
        for key, variable in getattr(self, "job_timeout_vars", {}).items():
            if not self.job_timeout_override_vars[key].get():
                raw = defaults["timeouts"][key]
                variable.set("" if raw in (None, "", -1, "-1") else str(raw))
        self._job_prefilling = False
        self._shown_job_policy_fingerprint = fingerprint
        self.original_job_policy = policy
        self.job_policy_tree.delete(*self.job_policy_tree.get_children(""))
        rows = [("Global", policy.get("global") or {})]
        rows.extend(("Test: " + str(row.get("name")), row) for row in policy.get("tests") or [])
        for index, (name, row) in enumerate(rows):
            properties = row.get("properties") or {}
            def value(key):
                if not row.get("available") or key not in properties:
                    return "Unknown"
                return "(empty)" if properties[key] in (None, "") else str(properties[key])
            self.job_policy_tree.insert("", "end", iid="policy_%s" % index, text=name,
                                        values=(value("distributionmethod"), value("maxjobs"), value("jobsubmitcommand")))
        message = "Source: %s. Per-test rows show effective values; their independent-policy origin is not inferred." % (policy.get("source") or "unknown")
        if not policy.get("available"):
            message += " Original settings are unavailable; no Local/1 default is assumed."
        if policy.get("warnings"):
            message += " " + " ".join(str(value) for value in policy["warnings"])
        self.job_policy_status_var.set(message)
        timeout_rows = []
        for name, row in rows:
            fields = job_timeout_policy(row.get("properties") or {}, scope=name)["fields"]
            summary = []
            for key, field in fields.items():
                raw = field.get("raw_value")
                status = field.get("status") or "unknown"
                value = "empty / no limit" if status == "disabled" else (str(raw) if raw is not None else "unavailable")
                summary.append("{}={} ({})".format(field.get("source_property") or key, value, status))
                if name == "Global" and key in getattr(self, "job_timeout_effective_vars", {}):
                    text = "Original {}: {} · {}".format(field.get("source_property") or key, value, status)
                    if key == "linger_seconds":
                        text += " · native ICRP idle reuse; CLI: not applicable"
                    elif key in ("start_seconds", "configure_seconds"):
                        text += " · private native capture only; CLI: not applicable"
                    else:
                        text += " · CLI: exact RUNNING / owned local solver start"
                    self.job_timeout_effective_vars[key].set(text)
            timeout_rows.append(name + ": " + "; ".join(summary))
        if hasattr(self, "job_timeout_policy_var"):
            self.job_timeout_policy_var.set("\n".join(timeout_rows))

    def _collect_job_settings(self):
        overrides = {}
        if hasattr(self, "job_command_var"):
            overrides["command"] = self.job_command_var.get()
            if not overrides["command"].strip():
                raise ValueError("Command was not captured; enter a Command in Job Setup before running")
        if hasattr(self, "job_max_var"):
            value = self.job_max_var.get().strip()
            if not value.isdigit() or int(value) < 1:
                raise ValueError("Solver max jobs must be a positive integer.")
            overrides["max_jobs"] = int(value)
        timeouts = {}
        for key in getattr(self, "job_timeout_vars", {}):
            value = self.job_timeout_vars[key].get().strip()
            if not value:
                timeouts[key] = -1  # Internal disabled sentinel; the UI uses an empty field.
                continue
            number = finite_number(value)
            if number is None or number != int(number) or number <= 0:
                raise ValueError("Enter positive integer seconds or leave the timeout empty for no limit; zero is not allowed (" + key + ")")
            timeouts[key] = int(number)
        if timeouts:
            overrides["timeouts"] = timeouts
        mode = self.maestro_candidate_mode_var.get() if hasattr(self, "maestro_candidate_mode_var") else "auto"
        if mode not in ("auto", "serial", "batch"):
            raise ValueError("Maestro candidate execution must be auto, serial, or batch.")
        result = {"maestro_job_overrides": normalize_job_overrides(overrides), "maestro_candidate_mode": mode}
        if hasattr(self, "command_scheduler_var"):
            scheduler = self.command_scheduler_var.get()
            if scheduler not in ("auto", "batch", "receipt"):
                raise ValueError("Command scheduler must be auto, batch, or receipt")
            result["command_scheduler"] = scheduler
        return result

    def _restore_job_settings(self, run):
        if hasattr(self, "command_scheduler_var"):
            self.command_scheduler_var.set(run.get("command_scheduler") or "auto")
        self._restore_execution_commands(run.get("execution_commands"))
        if hasattr(self, "simulator_var"):
            if getattr(self, "_baseline_mode_active", False):
                self._cli_simulator_draft = run.get("simulator") or "spectre"
            else:
                self.simulator_var.set(run.get("simulator") or "spectre")
        if hasattr(self, "execution_backend_var"):
            backend = run.get("execution_backend") or "auto"
            if backend not in ("auto", "maestro_netlist", "persistent_maestro", "legacy", "adexl", "private_netlist", "maestro_baseline"):
                raise ValueError("Invalid saved simulation engine")
            self.execution_backend_var.set(backend if backend in ("private_netlist", "maestro_baseline") else "auto")
        overrides = normalize_job_overrides(run.get("maestro_job_overrides"))
        if hasattr(self, "job_command_override_var"):
            self.job_command_override_var.set("command" in overrides)
            self.job_max_override_var.set("max_jobs" in overrides)
            if "command" in overrides:
                self.job_command_var.set(overrides["command"])
            if "max_jobs" in overrides:
                self.job_max_var.set(str(overrides["max_jobs"]))
            for key, enabled in getattr(self, "job_timeout_override_vars", {}).items():
                timeouts = overrides.get("timeouts") or {}
                enabled.set(key in timeouts)
                if key in timeouts:
                    self.job_timeout_vars[key].set("" if timeouts[key] in (None, "", -1, "-1") else str(timeouts[key]))
            mode = run.get("maestro_candidate_mode") or "auto"
            if mode not in ("auto", "serial", "batch"):
                raise ValueError("Invalid saved Maestro candidate execution mode")
            self.maestro_candidate_mode_var.set(mode)
        self._render_simulator_profiles()

    def _callback_exception(self, exception, value, traceback):
        self._set_error("{}: {}".format(exception.__name__, value))
        self.last_diagnostic_detail = "".join(traceback_module.format_exception(exception, value, traceback))[-65536:]

    def _set_status(self, message):
        message = str(message or "")
        if len(message) > 160:
            self.last_diagnostic_detail = message[-65536:]
        self.status_var.set(_compact_diagnostic(message, 160))
        self._activity_event(message, key="status")

    def _set_error(self, message):
        self.last_diagnostic_detail = str(message or "")[-65536:]
        self.status_var.set(_compact_diagnostic(message, 140) + " See Details.")
        self.health_var.set("Action needs attention")
        self._activity_event(message, "ERROR", key="error")

    def _show_text_details(self, title, detail):
        window = tk.Toplevel(self.root)
        window.title(title)
        window.geometry("960x480")
        window.transient(self.root)
        text = tk.Text(window, wrap="word", font=("TkFixedFont", 10), undo=False)
        scroll = ttk.Scrollbar(window, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=scroll.set)
        text.grid(row=0, column=0, sticky="nsew", padx=(10, 0), pady=10)
        scroll.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=10)
        text.insert("1.0", str(detail or "No details available.")[-65536:])
        text.configure(state="disabled")
        ttk.Button(window, text="Close", command=window.destroy).grid(row=1, column=0, columnspan=2, pady=(0, 10))
        window.rowconfigure(0, weight=1)
        window.columnconfigure(0, weight=1)
        return window

    def show_diagnostic_details(self):
        return self._show_text_details("Crescendo Diagnostic Details", self.last_diagnostic_detail)

    def show_validation_details(self):
        return self._show_text_details("Optional PDK Check Details", self.range_validation_detail)

    def _task_error(self, prefix, final=None):
        def handler(error):
            if final:
                final()
            message = format_task_error(error, prefix, config=getattr(self.service, "config", None))
            self._set_error(message)
            messagebox.showerror(prefix, _compact_diagnostic(message, 400) + "\nSee Details for the complete report.", parent=self.root)
        return handler

    def _write_launch_state(self, state=None, **changes):
        context = getattr(self, "launch_context", None)
        if not context or not context.get("state_path"):
            return
        source = context.get("source") or {}
        document = copy.deepcopy(getattr(self, "launch_state", {}) or {})
        document.update({
            "schema_version": "crescendo.maestro_plugin_launch.v1",
            "pid": os.getpid(), "updated_at": utc_timestamp(),
            "source": dict((key, source.get(key)) for key in (
                "library", "cell", "view", "view_dir", "setup_path", "work_dir", "cds_lib", "modified",
            )),
        })
        document["selection"] = {
            "mode": "invoking" if self._active_context() else "manual",
            "setup_path": getattr(self, "active_setup_path", None),
        }
        if state is not None:
            document["state"] = state
        document.update(changes)
        # service.exit_optimizer may already have removed this explicitly owned
        # temporary context. Final telemetry must never recreate its directory.
        if state == "closed" and changes.get("cleanup_confirmed") and not os.path.lexists(os.path.dirname(context["state_path"])):
            self.launch_state = document
            return True
        try:
            atomic_write_json(context["state_path"], document)
        except OSError as error:
            if not is_storage_full_error(error):
                raise
            # Launch status is diagnostic telemetry, not the prepared setup or
            # checkpoint. A second quota failure must not hide the task error
            # or prevent Exit from cancelling its owned processes.
            self.launch_state = document
            self.launch_state_storage_error = format_task_error(
                error, "Save plug-in launch status", config=getattr(self.service, "config", None),
            )
            self._set_error(self.launch_state_storage_error)
            return False
        self.launch_state = document
        self.launch_state_storage_error = None
        return True

    def _load_launch_setup(self):
        if self.closing:
            return
        source = self.launch_context.get("source") or {}
        self.setup_path_var.set(source.get("setup_path") or source.get("view_dir") or "")
        self._populate_history_choices()
        self.setup_summary_var.set("Maestro selected. Click Load and Inspect to discover the complete hierarchy and parameters.")
        self._set_status("Ready to inspect; no hierarchy scan or history read has started.")
        self._write_launch_state("bound", history={"state": "disabled"})

    def _cancel_history_read(self):
        event = getattr(self, "history_cancel_event", None)
        if event is not None:
            event.set()
        self.history_read_epoch = getattr(self, "history_read_epoch", 0) + 1
        self.history_cancel_event = None

    def _populate_history_choices(self):
        self._cancel_history_read()
        context = self._active_context() or {}
        self.history_choices = dict(("History: " + row["name"], copy.deepcopy(row))
                                    for row in context.get("histories") or [])
        if hasattr(self, "history_combo"):
            self.history_combo.configure(values=["None"] + list(self.history_choices),
                                         state="readonly" if context else "disabled")
            self.history_choice_var.set("None")
            self.read_history_button.configure(state="disabled")
        if hasattr(self, "launch_history_summary_var"):
            self.launch_history_summary_var.set("No history is attached. Reading a history summary is optional.")

    def _history_choice_changed(self, event=None):
        self._cancel_history_read()
        record = self.history_choices.get(self.history_choice_var.get())
        self.read_history_button.configure(state="normal" if record and not self.closing else "disabled")
        self.launch_history_summary_var.set(
            "Selected history {}. Click Read History Summary; it is independent of parameter inspection.".format(record["name"])
            if record else "No history is attached. Reading a history summary is optional."
        )
        self._write_launch_state(history={"state": "selected" if record else "disabled",
                                          "name": record["name"] if record else None})

    def _load_launch_history(self):
        context = self._active_context()
        epoch = getattr(self, "setup_selection_epoch", 0)
        if not context:
            self.launch_history_summary_var.set("Manually selected saved setup; no invoking Maestro history is attached.")
            self._write_launch_state(history={"state": "not_attached"})
            return
        record = (getattr(self, "history_choices", {}) or {}).get(self.history_choice_var.get()) if hasattr(self, "history_choice_var") else None
        if not record:
            self._cancel_history_read()
            self.launch_history_summary_var.set("No history is attached. Reading a history summary is optional.")
            self._write_launch_state(history={"state": "disabled"})
            return
        if getattr(self, "closing", False):
            return
        self._cancel_history_read()
        cancel_event = threading.Event()
        self.history_cancel_event = cancel_event
        history_epoch = self.history_read_epoch
        history_identity = dict((key, record.get(key)) for key in ("name", "results_dir", "rdb_file"))
        if not record.get("rdb_file"):
            self.launch_history_summary_var.set("Selected history {} has no saved RDB.".format(record.get("name") or ""))
            self._write_launch_state(history=dict(history_identity, state="missing"))
            return
        self._write_launch_state(history=dict(history_identity, state="loading"))
        if hasattr(self, "read_history_button"):
            self.read_history_button.configure(state="disabled")
        self.launch_history_summary_var.set("Reading optional history summary: {}...".format(record["name"]))

        def read_history():
            from .rdb import parse_rdb_summary
            return parse_rdb_summary(record["rdb_file"], cancel_event=cancel_event)

        def loaded(parsed):
            if getattr(self, "closing", False) or epoch != getattr(self, "setup_selection_epoch", 0) or history_epoch != self.history_read_epoch:
                return
            summary = dict(
                history_identity, state="ready",
                points=parsed["point_count"], outputs=parsed["output_count"],
                tests=parsed.get("test_count"), corners=parsed.get("corner_count"),
            )
            self.launch_history_summary_var.set(
                "Maestro history {} · {} points · {} outputs · source results read-only".format(
                    record.get("name") or "", summary["points"], summary["outputs"],
                )
            )
            self._write_launch_state(history=summary)
            if hasattr(self, "read_history_button"):
                self.read_history_button.configure(state="normal")

        def failed(error):
            if getattr(self, "closing", False) or epoch != getattr(self, "setup_selection_epoch", 0) or history_epoch != self.history_read_epoch:
                return
            message = "{}: {}".format(error.__class__.__name__, error)
            self.launch_history_summary_var.set("Selected Maestro history is unavailable: {}".format(message))
            self._write_launch_state(history=dict(history_identity, state="error", error=message))
            if hasattr(self, "read_history_button"):
                self.read_history_button.configure(state="normal")

        method = getattr(self.tasks, "submit_history", self.tasks.submit)
        method(read_history, loaded, failed)

    def _setup_reload_block_reason(self):
        if getattr(self, "closing", False):
            return "The optimizer is closing."
        if getattr(self, "setup_loading", False):
            return "An ADE setup is already loading."
        if getattr(self, "start_request_pending", False):
            return "An optimization is starting; wait before changing the setup."
        run_ui = getattr(self, "run_ui", None)
        if run_ui and run_ui.state in ("running", "stopping"):
            return "Wait for the active optimization to finish before loading a setup."
        if getattr(self, "range_validation_inflight", False):
            return "Wait for parameter range validation to finish before loading a setup."
        return None

    def _active_context(self):
        return getattr(self, "active_launch_context", getattr(self, "launch_context", None))

    @staticmethod
    def _same_setup_paths(first, second):
        if not first or not second:
            return False
        def canonical(path):
            path = os.path.realpath(os.path.abspath(os.path.expanduser(path)))
            directory = path if os.path.isdir(path) else os.path.dirname(path) if os.path.basename(path) in ("active.state", "data.dm") else None
            if directory:
                candidates = [os.path.join(directory, name) for name in ("maestro.sdb", "data.sdb")
                              if os.path.isfile(os.path.join(directory, name))]
                if len(candidates) == 1:
                    return os.path.realpath(candidates[0])
            return path
        return canonical(first) == canonical(second)

    def _selection_required(self, path):
        current = getattr(self, "active_setup_path", None)
        if not current:
            current = ((self._active_context() or {}).get("source") or {}).get("setup_path")
        return bool(current and not self._same_setup_paths(path, current))

    def _set_setup_controls(self, enabled):
        for name in ("setup_path_entry", "browse_setup_file_button", "browse_setup_folder_button",
                     "load_setup_button", "use_invoking_setup_button"):
            widget = getattr(self, name, None)
            if widget is not None:
                widget.configure(state="normal" if enabled else "disabled")
        if not enabled:
            if getattr(self, "setup_loading", False):
                self.stop_button.configure(state="normal", text="Stop")
            if hasattr(self, "open_config_button"):
                self.open_config_button.configure(state="disabled")
            for name in ("start_button", "validate_button", "apply_ranges_button"):
                widget = getattr(self, name, None)
                if widget is not None:
                    widget.configure(state="disabled")
        else:
            self._setup_path_changed()
            run_ui = getattr(self, "run_ui", None)
            if not run_ui or run_ui.state not in ("queued", "running", "stopping"):
                self.stop_button.configure(state="disabled", text="Stop")
            if hasattr(self, "open_config_button"):
                reference = ((getattr(self, "prepared", None) or {}).get("setup_private_session") or
                             (getattr(self, "cell_inspection", None) or {}).get("setup_private_session"))
                self.open_config_button.configure(state="normal" if reference else "disabled")
        if hasattr(self, "apply_cells_button"):
            self.apply_cells_button.configure(state="normal" if enabled and self.cell_inspection_id and self.selected_cell_scope_ids else "disabled")

    def _prepared_selection_matches(self):
        prepared = getattr(self, "prepared", None) or {}
        loaded = (prepared.get("setup_locator") or {}).get("path") or prepared.get("setup_path") or prepared.get("maestro_path")
        variable = getattr(self, "setup_path_var", None)
        return not (loaded and variable) or self._same_setup_paths(variable.get().strip(), loaded)

    def _require_prepared_selection(self):
        if getattr(self, "eda_revalidation_required", False):
            raise ValueError("EDA settings changed. Your edits are retained; Load and Inspect, then Apply Selected Cells to revalidate before running.")
        if (getattr(self, "setup_loading", False) or getattr(self, "cell_scope_dirty", False) or
                not self._prepared_selection_matches()):
            raise ValueError("Wait for Load and Inspect to finish for the selected setup before using prepared parameters.")

    def _setup_path_changed(self, *unused):
        inspection = getattr(self, "cell_inspection", None)
        if inspection and hasattr(self, "apply_cells_button"):
            enabled = (self.cell_inspection_id and self.selected_cell_scope_ids and not self._setup_reload_block_reason() and
                       self._same_setup_paths(self.setup_path_var.get().strip(), inspection.get("setup_path")))
            self.apply_cells_button.configure(state="normal" if enabled else "disabled")
        prepared = getattr(self, "prepared", None)
        if not prepared or self._setup_reload_block_reason():
            return
        matches = (self._prepared_selection_matches() and not getattr(self, "cell_scope_dirty", False)
                   and not getattr(self, "eda_revalidation_required", False))
        adjusted = any(row.get("status") == "pdk_adjusted"
                       for rows in getattr(self, "range_validation_results", {}).values() for row in rows)
        for name in ("start_button", "validate_button", "apply_ranges_button"):
            widget = getattr(self, name, None)
            if widget is not None:
                widget.configure(state="normal" if matches and (name != "apply_ranges_button" or
                                 adjusted) else "disabled")
        if not matches:
            self._set_status("EDA settings changed; edits are retained. Load and Inspect, then Apply Selected Cells."
                             if getattr(self, "eda_revalidation_required", False) else
                             "Setup selection changed; click Load and Inspect to load it.")

    def _apply_setup_selection(self, selection, preserve_edits=False):
        if preserve_edits:
            self.active_launch_context = copy.deepcopy(selection.get("context"))
            self.active_setup_path = selection["setup_path"]
            self.setup_selection_epoch += 1
            operation = getattr(self, "eda_auto_revalidation", None)
            if operation:
                operation["selection_epoch"] = self.setup_selection_epoch
            self._invalidate_prepared("Saved setup reloaded; restoring matching edits and refreshing inherited settings.", preserve_edits=True)
            if selection.get("eda_setup"):
                self._render_eda_setup(selection["eda_setup"])
            return
        draft_path = getattr(self, "eda_edit_draft_path", None)
        if draft_path and not self._same_setup_paths(draft_path, selection["setup_path"]):
            self._finish_eda_revalidation("已切换设置，自动恢复停止；不会把原设置的编辑套到新设置。")
            self.pending_settings_state = None
            self.eda_revalidation_required = False
            self.eda_edit_draft_path = None
        self.active_launch_context = copy.deepcopy(selection.get("context"))
        self.active_setup_path = selection["setup_path"]
        self.setup_selection_epoch = getattr(self, "setup_selection_epoch", 0) + 1
        self._invalidate_prepared("Selected setup changed; inspecting the schematic hierarchy...")
        self.run_ui = RunUiState()
        self.live_spec = {"objectives": [], "constraints": []}
        self.live_candidates = []
        self.display_dataset = None
        self.progress_revision = 0
        self._live_candidate_map = {}
        self.selected_candidate_id = None
        self.pareto_ids = set()
        self.result_page = 0
        self.progress_inflight = False
        self.objective_presentation = {}
        self.recent_progress_signature = None
        for viewer in list(self.log_viewers):
            viewer._close()
        self.log_viewers = []
        self.render_results()
        self.render_pareto()
        self.run_state_var.set("Waiting for the selected setup.")
        if hasattr(self, "results_failure_var"):
            self.results_failure_var.set("")
        self._render_execution_console({"message": "No run selected for this setup."})
        for name in ("stop_button", "export_csv_button"):
            getattr(self, name).configure(state="disabled")
        self.setup_path_var.set(self.active_setup_path)
        context = self._active_context()
        self._render_job_policy((context or {}).get("job_policy"), reset=True)
        self._render_save_options((context or {}).get("save_options"), reset=True)
        if hasattr(self, "maestro_candidate_mode_var"):
            self.maestro_candidate_mode_var.set("auto")
        if hasattr(self, "simulator_var"):
            self.simulator_var.set((getattr(self.service, "config", {}).get("cadence") or {}).get("simulator", "spectre"))
        if hasattr(self, "execution_backend_var"):
            self.execution_backend_var.set("auto")
        self._restore_execution_commands()
        source = (context or {}).get("source") or {}
        identity = ("Invoking Maestro: {}/{}/{}".format(
            source.get("library") or "?", source.get("cell") or "?", source.get("view") or "?")
            if context else "Manually selected saved setup: {}".format(self.active_setup_path))
        if hasattr(self, "setup_source_var"):
            self.setup_source_var.set(identity)
        self.root.title("Crescendo · " + identity)
        if selection.get("eda_setup"):
            self._render_eda_setup(selection["eda_setup"])
        if hasattr(self, "launch_history_summary_var"):
            self._populate_history_choices()
            self._write_launch_state(history={"state": "disabled"})
        if self.current_page == "Optimization":
            self.refresh_recent_runs()
        self._write_launch_state("preparing", error=None)

    def use_invoking_setup(self):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        source = (getattr(self, "launch_context", None) or {}).get("source") or {}
        if source.get("setup_path"):
            self.setup_path_var.set(source["setup_path"])
            self.load_setup(initialize_validation=False)

    def _require_launch_setup(self, path):
        context = self._active_context()
        if context:
            source = context.get("source") or {}
            if source.get("modified") is True:
                raise ValueError("The invoking Maestro has unsaved changes. Save the setup and reopen Crescendo before inspecting or running.")
            if not self._same_setup_paths(source.get("setup_path"), path):
                raise ValueError("This configuration references a different Maestro. Select its saved setup with Browse and Load and Inspect first.")
        elif getattr(self, "active_setup_path", None) and not self._same_setup_paths(path, self.active_setup_path):
            raise ValueError("Select the referenced setup and click Load and Inspect before applying this configuration.")

    def _require_live_test_bindings(self, prepared):
        context = self._active_context()
        if not context:
            return
        saved = dict((str(row.get("name")), row) for row in (prepared.get("setup") or {}).get("tests") or [])
        for live in context.get("live_tests") or []:
            name = str(live.get("name") or "")
            if not name:
                continue
            if getattr(self, "selected_test", None) and name != self.selected_test:
                continue
            row = saved.get(name)
            fields = [key for key in ("library", "cell", "view") if live.get(key)]
            if fields and (row is None or any(str(row.get(key) or "") != str(live[key]) for key in fields)):
                raise ValueError(
                    "The saved setup differs from Maestro test %s. Save in Maestro and reopen the optimizer from its menu." % name
                )

    def _browse_setup_file(self):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        path = filedialog.askopenfilename(
            parent=self.root,
            title="Select Maestro or ADE XL SetupDB",
            filetypes=(("ADE SetupDB", "maestro.sdb data.sdb"), ("All files", "*")),
        )
        if path:
            self.setup_path_var.set(os.path.dirname(path) if os.path.basename(path) in ("maestro.sdb", "data.sdb") else path)

    def _browse_setup_folder(self):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        path = filedialog.askdirectory(
            parent=self.root, title="Select Maestro or ADE XL Setup Directory",
            initialdir=self.setup_path_var.get() or os.path.expanduser("~"),
        )
        if path:
            self.setup_path_var.set(path)

    def load_setup(self, initialize_validation=None):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        path = self.setup_path_var.get().strip()
        if not path:
            path = ((self.launch_context or {}).get("source") or {}).get("setup_path") or ""
            if path:
                self.setup_path_var.set(path)
        if not path:
            self._set_error("Load ADE Setup: Enter or select an ADE setup path.")
            messagebox.showerror("Load ADE Setup", "Enter or select an ADE setup path.", parent=self.root)
            return
        switch = self._selection_required(path)
        selection = {}
        select = getattr(self.service, "select_setup", None)
        revalidation = getattr(self, "eda_auto_revalidation", None) or {}
        # Environment overrides recheck the existing frozen TB. Only an
        # explicit Load adopts source Maestro saves and creates a new baseline.
        environment_only = bool(revalidation and revalidation.get("reason") != "saved_setup_reload")
        explicit_reload = callable(getattr(type(self.service), "select_setup", None)) and not environment_only
        try:
            if not switch and not explicit_reload:
                self._require_launch_setup(path)
        except Exception as error:
            self._write_launch_state("prepare_failed", error=str(error))
            self._set_error(str(error))
            messagebox.showerror("Load ADE Setup", str(error), parent=self.root)
            return
        if initialize_validation is None:
            initialize_validation = not bool(getattr(self, "launch_context", None))
        if (not callable(getattr(self.service, "inspect_tests", None)) and
                not switch and (self.prepared or self.cell_inspection) and
                not getattr(self, "eda_auto_revalidation", None)):
            self._invalidate_prepared("Reloading the saved setup; current edits are retained.", preserve_edits=True)
            self.eda_auto_revalidation = {"path": self.eda_edit_draft_path,
                "selection_epoch": self.setup_selection_epoch, "prepare": bool(self.prepared),
                "preparing": False, "page": self.current_page, "reason": "saved_setup_reload"}
        if (getattr(self, "eda_revalidation_required", False)
                and self._same_setup_paths(path, getattr(self, "eda_edit_draft_path", None))):
            if not callable(getattr(self.service, "inspect_tests", None)):
                self._stash_eda_edit_draft()
        single_test_staged = callable(getattr(self.service, "inspect_tests", None))
        staged = single_test_staged or callable(getattr(type(self.service), "inspect_cells", None))
        self._cancel_cell_render()
        self._cancel_parameter_render()
        self.test_inspection_id = None
        self.test_catalog = None
        self.selected_test = None
        self.test_choice_var.set("")
        self.test_combo.configure(values=(), state="disabled")
        self.execution_selection_panel.set_setup({})
        self.setup_loading = True
        self._set_setup_controls(False)
        self.test_load_epoch += 1
        load_epoch = self.test_load_epoch
        self.setup_summary_var.set("读取已保存 Test 列表…" if staged else "Loading and recursively inspecting the schematic hierarchy...")
        self._set_status("Inspecting {}".format(path))
        self._write_launch_state("preparing", error=None)

        def load():
            if switch or explicit_reload:
                selection.update(select(path))
            if single_test_staged:
                return self.service.inspect_tests(path, backend="auto")
            if staged:
                return self.service.inspect_cells(path, max_depth=32, backend="auto")
            if not initialize_validation:
                return self._cache_prepared_defaults(self.service.prepare(path, max_depth=32, backend="auto", initialize_validation=False))
            return self._cache_prepared_defaults(self.service.prepare(path, max_depth=32, backend="auto"))

        def loaded(payload, matched_ui_state=None):
            if self.closing or load_epoch != self.test_load_epoch:
                return
            self.setup_loading = False
            self._set_setup_controls(True)
            prepared_id, prepared = payload
            try:
                if selection:
                    self._apply_setup_selection(selection, preserve_edits=bool(not switch and self.eda_revalidation_required))
                    selection.clear()
                if getattr(self, "eda_auto_revalidation", None):
                    self._stash_eda_edit_draft()
                if single_test_staged:
                    self._set_test_catalog(prepared_id, prepared)
                elif staged:
                    self._set_cell_inspection(prepared_id, prepared)
                else:
                    if self.eda_revalidation_required and self.pending_settings_state and matched_ui_state is None:
                        self.setup_loading = True
                        self._set_setup_controls(False)
                        self._match_eda_prepared(payload, lambda state: loaded(payload, state), failed)
                        return
                    self._set_prepared(prepared_id, prepared, matched_ui_state=matched_ui_state)
                    if matched_ui_state is not None:
                        self._advance_eda_revalidation(matched_ui_state[1])
            except Exception as error:
                failed(error)
                return
            if self.pending_config:
                document = self.pending_config
                self.pending_config = None
                try:
                    missing = self.apply_design_config(document)
                    self.config_message_var.set(
                        "Config applied{}".format(
                            "; missing parameter IDs: {}".format(", ".join(missing)) if missing else "."
                        )
                    )
                except Exception as error:
                    self.pending_config = document
                    self._set_error("Config could not be applied: {}".format(error))

        def failed(error):
            if self.closing or load_epoch != self.test_load_epoch:
                return
            self.setup_loading = False
            self._set_setup_controls(True)
            if selection:
                self._apply_setup_selection(selection, preserve_edits=bool(not switch and self.eda_revalidation_required))
                selection.clear()
            if not switch or not getattr(self, "prepared_id", None):
                self._invalidate_prepared("ADE setup could not be loaded: {}".format(error),
                    preserve_edits=bool(getattr(self, "eda_revalidation_required", False)))
            self._finish_eda_revalidation(str(error))
            self._write_launch_state("prepare_failed", error="{}: {}".format(error.__class__.__name__, error))
            self._task_error("Load ADE Setup")(error)

        self.tasks.submit(load, loaded, failed)

    def _set_test_catalog(self, inspection_id, preview, auto_select=True):
        self._invalidate_prepared("Select one Test to inspect its hierarchy.")
        self.test_inspection_id, self.test_catalog = inspection_id, preview
        self.selected_test = None
        rows = preview.get("tests") or (preview.get("setup") or {}).get("tests") or []
        names = [row["name"] for row in rows if row.get("name")]
        self.test_choice_var.set("")
        self.test_combo.configure(values=names, state="readonly" if names else "disabled")
        self.execution_selection_panel.set_setup({})
        self.setup_summary_var.set("{} saved Tests. Select one to inspect its hierarchy.".format(len(names)))
        self._activity_event("Saved Test catalogue loaded: {} Tests.".format(len(names)))
        if auto_select and len(names) == 1:
            self.test_choice_var.set(names[0])
            self._test_selected()

    def _test_selected(self, _event=None):
        name = self.test_choice_var.get()
        if not name or name == self.selected_test or not self.test_inspection_id:
            return
        if self._setup_reload_block_reason():
            self.test_choice_var.set(self.selected_test or "")
            return
        self.test_load_epoch += 1
        epoch, catalog_id = self.test_load_epoch, self.test_inspection_id
        self.selected_test = name
        self._invalidate_prepared("Inspecting Test {} hierarchy…".format(name))
        self.pending_settings_state = None
        self.pending_configuration_document = None
        self.pending_configuration_settings = None
        self.eda_revalidation_required = False
        self.eda_edit_draft_path = None
        self._finish_eda_revalidation("Select cells and Apply to complete the new preparation.")
        self.run_ui = RunUiState()
        for viewer in list(self.log_viewers): viewer._close()
        self.log_viewers = []
        self.run_state_var.set("Waiting for the selected Test.")
        if hasattr(self, "results_failure_var"): self.results_failure_var.set("")
        self.live_spec = {"objectives": [], "constraints": []}
        self.live_candidates = []; self.display_dataset = None
        self._live_candidate_map = {}; self.selected_candidate_id = None; self.pareto_ids = set()
        self.progress_revision = 0; self.progress_inflight = False; self.objective_presentation = {}
        self.render_results(); self.render_pareto()
        self.execution_selection_panel.set_setup({})
        self.setup_loading = True
        self._set_setup_controls(False)
        self.test_combo.configure(state="disabled")
        self._activity_event("Inspecting hierarchy for Test " + name)
        self._set_status("Inspecting Test {} hierarchy…".format(name))
        def current():
            return not self.closing and epoch == self.test_load_epoch and catalog_id == self.test_inspection_id and self.selected_test == name
        def loaded(payload):
            if not current(): return
            self.setup_loading = False
            self._set_setup_controls(True)
            self.test_combo.configure(state="readonly")
            try: self._set_cell_inspection(*payload)
            except Exception as error: failed(error)
        def failed(error):
            if not current(): return
            self.setup_loading = False
            self._set_setup_controls(True)
            self.test_combo.configure(state="readonly")
            self.selected_test = None
            self.test_choice_var.set("")
            self._task_error("Inspect Test hierarchy")(error)
        self.tasks.submit(lambda: self.service.inspect_test_cells(catalog_id, name, max_depth=32), loaded, failed)

    def _cancel_cell_render(self):
        self.cell_render_epoch = getattr(self, "cell_render_epoch", 0) + 1
        job = getattr(self, "cell_render_job", None)
        if job is not None:
            try:
                self.root.after_cancel(job)
            except tk.TclError:
                pass
        self.cell_render_job = None

    def _clear_cell_inspection(self):
        self._cancel_cell_render()
        self.cell_inspection_id = None
        self.cell_inspection = None
        self.cell_scopes = {}
        self.selected_cell_scope_ids = set()
        self.applied_cell_scope_ids = set()
        self.cell_scope_dirty = False
        self._render_private_setup(None)
        if hasattr(self, "cell_scope_tree"):
            self.cell_scope_tree.delete(*self.cell_scope_tree.get_children(""))
            self.apply_cells_button.configure(state="disabled")
            self.cell_scope_summary_var.set("Select a Test to load its cell hierarchy, then select cells and Apply to inspect devices.")

    def _set_cell_inspection(self, inspection_id, preview):
        self._require_live_test_bindings(preview)
        self._invalidate_prepared("Cell hierarchy loaded; choose cells and Apply to inspect their devices.",
            preserve_edits=bool(getattr(self, "eda_revalidation_required", False)))
        self.cell_inspection_id, self.cell_inspection = inspection_id, preview
        self.execution_selection_panel.set_setup(preview.get("setup") or {})
        self._render_native_settings(preview)
        self._render_private_setup(preview.get("setup_private_session"))
        self._render_job_policy(preview.get("job_policy"))
        self._render_save_options((preview.get("setup") or {}).get("save_options") or preview.get("save_options"))
        self.cell_scopes = dict((row["scope_id"], row) for row in preview.get("cell_scopes") or [])
        if not self.cell_scopes:
            raise ValueError("No selectable design cells were found. Inspect the hierarchy probe log.")
        self._render_cell_scopes()
        if (preview.get("eda_context") or {}).get("cadence"):
            self._render_eda_setup(preview["eda_context"]["cadence"])
        self._update_cell_selection_state()
        self.setup_summary_var.set("{} cells · device parameters are not loaded. Select cells below, then Apply Selected Cells.".format(len(self.cell_scopes)))
        self._restore_pending_ui_state()
        self.sidebar_status_var.set("Select cells and Apply")
        self.parameter_summary_var.set("Device parameters load after Apply Selected Cells.")
        self._set_status("Cell hierarchy ready. Select one or more cells and click Apply Selected Cells.")
        self._write_launch_state("cells_ready", inspection_id=inspection_id, error=None,
                                 counts={"cells": len(self.cell_scopes), "parameters": 0})
        if not getattr(self, "eda_auto_revalidation", None):
            self.show_page("Design Setup")

    def _render_private_setup(self, reference):
        if not hasattr(self, "private_setup_var"):
            return
        if not reference:
            self.active_private_suite = None
        if reference and reference.get("session_id"):
            self.active_private_suite = {"id": reference["session_id"],
                "revision": reference.get("revision"),
                "source_path": reference.get("source_path") or getattr(self, "active_setup_path", None)}
        ready = bool(reference and reference.get("status") not in ("closed", "failed"))
        self.open_config_button.configure(state="normal" if ready else "disabled")
        if not reference:
            self.private_setup_var.set("私有 Maestro / config 在 Load and Inspect 后创建。")
            return
        maestro = reference.get("maestro") or {}
        lines = ["私有 Maestro：{}/{}/{}\n{}".format(
            maestro.get("library", "?"), maestro.get("cell", "?"), maestro.get("view", "?"), maestro.get("path", ""))]
        for config in reference.get("configs") or []:
            lines.append("私有 config：{}/{}/{}\n{}".format(
                config.get("library", "?"), config.get("cell", "?"), config.get("view", "?"), config.get("path", "")))
        lines.append("状态：{}；私有套件可在管理窗口复用。".format(reference.get("status") or "unknown"))
        if reference.get("private_views_read_only"):
            lines.append("私有视图闲置时只读，Crescendo 操作时临时可写；绑定请用 Open Config 查看。")
        if reference.get("diagnostic_log"):
            lines.append("Setup 总日志：" + reference["diagnostic_log"])
        self.private_setup_var.set("\n".join(lines))

    def open_private_config(self):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        loader = getattr(self.service, "setup_private_session_info", None)
        if not callable(loader):
            self._set_error("当前服务没有私有 config 绑定信息。")
            return
        self.open_config_button.configure(state="disabled")
        def loaded(reference):
            if self.closing:
                return
            self._render_private_setup(reference)
            self._show_private_config(reference)
        def failed(error):
            if not self.closing:
                self.open_config_button.configure(state="normal")
                self._task_error("Open Config")(error)
        self.tasks.submit(lambda: loader(inspection_id=self.cell_inspection_id,
            prepared_id=self.prepared_id), loaded, failed)

    def open_private_suites(self):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_status(reason)
            return
        if not callable(getattr(self.service, "list_private_suites", None)):
            self._set_error("当前服务不支持私有套件管理。")
            return
        window = tk.Toplevel(self.root)
        window.title("私有套件管理")
        window.geometry("1000x650")
        ttk.Label(window, text="删除会移除登记的私有视图，保留历史和审计资料。隔离套件不影响其他套件的新建与加载。", wraplength=940).pack(fill="x", padx=10, pady=8)
        frame = ttk.Frame(window)
        frame.pack(fill="both", expand=True, padx=10)
        tree = ttk.Treeview(frame, columns=("test", "status", "source"), show="tree headings", height=8)
        for column, label, width in (("#0", "名称", 200), ("test", "Test", 130), ("status", "状态", 110), ("source", "来源", 400)):
            tree.heading(column, text=label)
            tree.column(column, width=width)
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)
        detail = tk.Text(window, height=10, wrap="word")
        detail.pack(fill="both", expand=True, padx=10, pady=8)
        status = ttk.Label(window, text="正在读取套件列表…", wraplength=940)
        status.pack(fill="x", padx=10)
        actions = ttk.Frame(window)
        actions.pack(fill="x", padx=10, pady=8)
        rows = {}
        state = {"busy": False, "epoch": 0}
        show_deleted = tk.BooleanVar(value=False)
        buttons = []
        def exists():
            try:
                return bool(window.winfo_exists()) and not self.closing
            except tk.TclError:
                return False
        def selected():
            selection = tree.selection()
            return rows.get(selection[0]) if selection else None
        def show(_event=None):
            row = selected()
            detail.delete("1.0", "end")
            if row:
                lines = ["ID: " + str(row.get("id", "")), "来源: " + str(row.get("source_path", ""))]
                if row.get("can_load") is False:
                    lines.append("当前不可加载或更新；原因见下方状态与警告。")
                if row.get("can_delete") is False:
                    lines.append("当前不可删除。")
                if row.get("status") == "deleted":
                    lines.append("已删除记录：下列是原私有视图位置；配置、历史和审计资料按保留策略留下。")
                if row.get("can_recover"):
                    lines.append("可尝试恢复中断更新；执行时仍核验归属、停止状态和备份。")
                elif row.get("can_recover_reason"):
                    lines.append(str(row["can_recover_reason"]))
                for member in row.get("members") or []:
                    lines.append("{}: {}/{}/{}\n    {}".format(member.get("role", "view"), member.get("library", ""), member.get("cell", ""), member.get("view", ""), member.get("path", "")))
                lines.extend(str(x) for x in row.get("warnings") or [])
                detail.insert("1.0", "\n".join(lines))
        tree.bind("<<TreeviewSelect>>", show)
        def run(work, done, target_id=None):
            if state["busy"]:
                return
            state["busy"] = True
            state["epoch"] += 1
            epoch = state["epoch"]
            for button in buttons:
                button.configure(state="disabled")
            def finish():
                state["busy"] = False
                for button in buttons:
                    button.configure(state="normal")
            def loaded(value):
                if not exists() or epoch != state["epoch"]:
                    return
                finish()
                done(value)
            def failed(error):
                if not exists() or epoch != state["epoch"]:
                    return
                finish()
                if (getattr(error, "private_suite_invalidated", False) and target_id and
                        (getattr(self, "active_private_suite", None) or {}).get("id") == target_id):
                    self._invalidate_prepared("套件身份已失效；请从列表重新选择套件。", preserve_edits=True)
                    self.active_private_suite = None
                status.configure(text=str(error))
                self._task_error("私有套件")(error)
                if target_id:
                    refresh(note="操作未完成：" + str(error))
            self.tasks.submit(work, loaded, failed)
        def refresh(note=None):
            status.configure(text="正在读取套件列表…")
            def loaded(payload):
                tree.delete(*tree.get_children())
                rows.clear()
                visible = list(payload.get("suites") or [])
                if show_deleted.get():
                    visible.extend(payload.get("deleted_suites") or [])
                for row in visible:
                    item = tree.insert("", "end", text=row.get("name") or row.get("id"), values=(row.get("selected_test", ""), row.get("status", ""), row.get("source_path", "")))
                    rows[item] = row
                status.configure(text=(note + "\n" if note else "") + "共 {} 个套件记录。{}".format(len(rows), "；".join(str(x) for x in payload.get("warnings") or [])))
                show()
            run(self.service.list_private_suites, loaded)
        def require_selection():
            row = selected()
            if not row:
                status.configure(text="请先选择套件。")
            return row
        def load():
            row = require_selection()
            if row and row.get("can_load") is False:
                status.configure(text="此套件当前不可加载；请查看状态和警告。其他正常套件不受影响。")
                return
            if row:
                window.destroy()
                self.load_configuration(suite_id=row["id"])
        def update():
            row = require_selection()
            if not row:
                return
            if row.get("can_load") is False:
                status.configure(text="此旧套件缺少可恢复快照，不能更新；可查看成员或删除。")
                return
            dialog = tk.Toplevel(window)
            dialog.title("选择要更新的来源设置")
            ttk.Label(dialog, text="仅勾选需要从当前已保存 Maestro 更新的部分。", wraplength=620).pack(anchor="w", padx=12, pady=8)
            choices = (("test_design", "Test 设计"), ("test_settings", "Test 启用 / 模拟器"), ("analyses", "分析"), ("local_variables", "局部变量"), ("global_variables", "全局变量"), ("outputs", "输出"), ("corners", "Corners / Test 掩码"), ("job_setup", "作业设置"), ("save_options", "保存路径（不支持的来源会提示）"), ("other_settings", "其他设置 / 模型路径"))
            variables = {}
            for key, label in choices:
                variables[key] = tk.BooleanVar(value=False)
                ttk.Checkbutton(dialog, text=label, variable=variables[key]).pack(anchor="w", padx=12)
            def apply():
                sections = [key for key, _label in choices if variables[key].get()]
                if not sections:
                    return
                dialog.destroy()
                window.destroy()
                self.load_configuration(suite_id=row["id"], suite_sections=sections)
            ttk.Button(dialog, text="更新并加载", command=apply).pack(pady=10)
        def rename():
            row = require_selection()
            if not row:
                return
            if row.get("can_load") is False:
                status.configure(text="此旧套件缺少可恢复快照，不能重命名；可查看成员或删除。")
                return
            name = simpledialog.askstring("重命名套件", "新名称：", initialvalue=row.get("name", ""), parent=window)
            if name is None or not name.strip():
                return
            def done(payload):
                warnings = payload.get("warnings") or []
                if warnings:
                    messagebox.showwarning("套件重命名警告", "\n".join(str(x) for x in warnings), parent=window)
                active_id = (getattr(self, "active_private_suite", None) or {}).get("id")
                replacement = payload.get("suite") or {}
                if active_id == row["id"] and replacement.get("id"):
                    window.destroy()
                    self._invalidate_prepared("套件已重命名；正在恢复新身份。", preserve_edits=True)
                    self.active_private_suite = None
                    self.load_configuration(suite_id=replacement["id"], preserve_suite_draft=True)
                    return
                status.configure(text="已重命名。" + "；".join(str(x) for x in payload.get("warnings") or []))
                refresh()
            run(lambda: self.service.rename_private_suite(row["id"], name.strip()), done, row["id"])
        def delete():
            row = require_selection()
            if row and row.get("can_delete") is False:
                status.configure(text="此套件目前不能删除；请查看成员详情和警告。")
                return
            if not row or not messagebox.askyesno("删除套件", "删除套件 {} 及其私有成员？".format(row.get("name") or row["id"]), parent=window):
                return
            def done(payload):
                warnings = payload.get("warnings") or []
                if warnings:
                    messagebox.showwarning("套件删除警告", "\n".join(str(x) for x in warnings), parent=window)
                if (getattr(self, "active_private_suite", None) or {}).get("id") == row["id"]:
                    self._clear_cell_inspection()
                    self._invalidate_prepared("私有套件已删除；请选择或创建其他套件。")
                    self.active_private_suite = None
                refresh(note="私有视图已删除；历史和审计资料保留在 " + str(payload.get("retained_metadata_root") or row.get("root", "")))
            run(lambda: self.service.delete_private_suite(row["id"]), done, row["id"])
        def recover():
            row = require_selection()
            if not row:
                return
            if not row.get("can_recover"):
                status.configure(text=row.get("can_recover_reason") or "此套件没有可核验的更新回滚记录；未知远端任务不能通过恢复按钮清除。")
                return
            def done(payload):
                if (getattr(self, "active_private_suite", None) or {}).get("id") == row["id"]:
                    self._invalidate_prepared("中断更新已恢复；请重新加载套件。", preserve_edits=True)
                    self.active_private_suite = None
                refresh(note="更新已回滚，可重新加载；备份和旧视图证据保留。")
            run(lambda: self.service.recover_private_suite(row["id"]), done, row["id"])
        for label, command in (("加载", load), ("更新选中部分…", update), ("重命名…", rename), ("删除…", delete), ("恢复中断更新", recover), ("刷新", refresh)):
            button = ttk.Button(actions, text=label, command=command)
            button.pack(side="left", padx=(0, 6))
            buttons.append(button)
        ttk.Checkbutton(actions, text="显示已删除记录", variable=show_deleted, command=refresh).pack(side="left", padx=6)
        ttk.Button(actions, text="关闭", command=window.destroy).pack(side="right")
        refresh()

    def _show_private_config(self, reference):
        """Inspect saved native HDB readback without opening an edit lock."""
        window = tk.Toplevel(self.root)
        window.title("Private Config · 原生绑定读回")
        window.geometry("1120x650")
        ttk.Label(window, text="以下是打开时的原生 HDB 绑定读回快照；更改后请重新打开。选择条目可复制完整身份与路径；此窗口只读，不锁定 config。",
                  wraplength=1080, justify="left").pack(fill="x", padx=12, pady=10)
        frame = ttk.Frame(window)
        frame.pack(fill="both", expand=True, padx=12)
        columns = ("implementation", "mode")
        tree = ttk.Treeview(frame, columns=columns, show="tree headings")
        tree.heading("#0", text="Config / occurrence")
        tree.heading("implementation", text="绑定的 library / cell / view")
        tree.heading("mode", text="层级处理")
        tree.column("#0", width=380)
        tree.column("implementation", width=530)
        tree.column("mode", width=150)
        bar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=bar.set)
        bar.pack(side="right", fill="y")
        tree.pack(fill="both", expand=True)
        details = tk.Text(window, height=9, wrap="word")
        details.pack(fill="x", padx=12, pady=10)
        descriptions = {}
        views = {tuple(row.get("target") or ()): row for row in reference.get("views") or []}
        def triple(value):
            if isinstance(value, dict):
                return tuple(value.get(key) or "?" for key in ("library", "cell", "view"))
            return tuple(value or ())
        for config in reference.get("configs") or []:
            identity = triple(config)
            top = triple(config.get("top"))
            parent = tree.insert("", "end", text="/".join(identity), values=("/".join(top), "config top"), open=True)
            descriptions[parent] = "Config: {}\nPath: {}\nTop: {}\nBinding verification: {}".format(
                "/".join(identity), config.get("path", ""), "/".join(top), reference.get("binding_verification") or "unknown")
            for row in config.get("bindings") or []:
                target = triple(row.get("target") or [row.get("resolved_" + key) for key in ("library", "cell", "view")])
                mode = "保留，不向下扫描" if row.get("stop") or row.get("scan_traversable") is False else "schematic"
                item = tree.insert(parent, "end", text=row.get("hierarchy_path") or "?",
                                   values=("/".join(str(part or "?") for part in target), mode))
                owned = views.get(target) or {}
                descriptions[item] = "Occurrence: {}\nBound view: {}\nPrivate view path: {}\nSource: {}\nStop: {}\nScan traversable: {}".format(
                    row.get("hierarchy_path"), "/".join(str(part or "?") for part in target), owned.get("path") or "原有绑定（未私有化）",
                    "/".join(owned.get("source") or []) or "继承原 config", row.get("stop"), row.get("scan_traversable"))
        def selected(_event=None):
            selection = tree.selection()
            details.configure(state="normal")
            details.delete("1.0", "end")
            if selection:
                details.insert("1.0", descriptions.get(selection[0], ""))
            details.configure(state="disabled")
        tree.bind("<<TreeviewSelect>>", selected)
        details.configure(state="disabled")

    @staticmethod
    def _cell_scope_label(row):
        path = row["hierarchy_path"]
        if path != "/":
            return path.rsplit("/", 1)[-1]
        design = row["design"]
        cell = row.get("cell") or row.get("master_cell") or design["cell"]
        return "{} [{}]".format(cell, design.get("view") or "?")

    def _render_cell_scopes(self):
        self._cancel_cell_render()
        epoch = self.cell_render_epoch
        self.cell_scope_tree.delete(*self.cell_scope_tree.get_children(""))
        def key(row):
            design = row["design"]
            return tuple(design.get(name) or "" for name in ("library", "cell", "view"))
        rows = sorted(self.cell_scopes.values(), key=lambda row: (key(row), row["hierarchy_path"].count("/"), row["hierarchy_path"]))
        parents = dict(((key(row), row["hierarchy_path"]), row["scope_id"]) for row in rows)
        iterator = iter(rows)
        def chunk():
            self.cell_render_job = None
            if self.closing or epoch != self.cell_render_epoch:
                return
            started = time.monotonic()
            for count in range(150):
                try:
                    row = next(iterator)
                except StopIteration:
                    return
                path = row["hierarchy_path"]
                parent_path = path.rsplit("/", 1)[0] or "/"
                parent = parents.get((key(row), parent_path), "") if path != "/" else ""
                marker = CHECKED_MARK if row["scope_id"] in self.selected_cell_scope_ids else UNCHECKED_MARK
                cell = row.get("cell") or row.get("master_cell") or row["design"]["cell"]
                library = row.get("library") or row.get("master_library") or row["design"]["library"]
                label = self._cell_scope_label(row)
                self.cell_scope_tree.insert(parent, "end", iid=row["scope_id"], text="{} {}".format(marker, label),
                                            values=(library, cell, path), open=path == "/")
                if time.monotonic() - started >= 0.012:
                    break
            self.cell_render_job = self.root.after(1, chunk)
        chunk()

    def _update_cell_selection_state(self):
        if not hasattr(self, "apply_cells_button"):
            return
        selected = getattr(self, "selected_cell_scope_ids", set())
        self.cell_scope_dirty = bool(self.cell_inspection_id and selected != self.applied_cell_scope_ids)
        enabled = bool(self.cell_inspection_id and selected and not self._setup_reload_block_reason())
        preview_path = (self.cell_inspection or {}).get("setup_path")
        if preview_path and not self._same_setup_paths(self.setup_path_var.get().strip(), preview_path):
            enabled = False
        self.apply_cells_button.configure(state="normal" if enabled else "disabled")
        self.cell_scope_summary_var.set("{} cells · {} selected. Selection includes devices in each selected cell and its subcells.".format(len(self.cell_scopes), len(selected)))
        self._setup_path_changed()

    def _toggle_cell_scope(self, scope_id):
        if self._setup_reload_block_reason() or scope_id not in self.cell_scopes:
            return
        if scope_id in self.selected_cell_scope_ids:
            self.selected_cell_scope_ids.remove(scope_id)
        else:
            self.selected_cell_scope_ids.add(scope_id)
        row = self.cell_scopes[scope_id]
        label = self._cell_scope_label(row)
        self.cell_scope_tree.item(scope_id, text="{} {}".format(CHECKED_MARK if scope_id in self.selected_cell_scope_ids else UNCHECKED_MARK, label))
        self._update_cell_selection_state()

    def _cell_scope_click(self, event):
        item = self.cell_scope_tree.identify_row(event.y)
        if not item or self.cell_scope_tree.identify_column(event.x) != "#0":
            return
        element = self.cell_scope_tree.identify("element", event.x, event.y)
        if "indicator" in str(element).lower():
            return
        self.cell_scope_tree.selection_set(item)
        self._toggle_cell_scope(item)
        return "break"

    def _cell_scope_space(self, _event=None):
        selected = self.cell_scope_tree.selection()
        if selected:
            self._toggle_cell_scope(selected[0])
        return "break"

    def select_all_cells(self, selected):
        if self._setup_reload_block_reason() or not self.cell_inspection_id:
            return
        self.selected_cell_scope_ids = set(self.cell_scopes) if selected else set()
        self._render_cell_scopes()
        self._update_cell_selection_state()

    def apply_selected_cells(self):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        inspection_id = self.cell_inspection_id
        scope_ids = sorted(self.selected_cell_scope_ids)
        preview_path = (self.cell_inspection or {}).get("setup_path")
        if not inspection_id or not scope_ids:
            self._set_error("Select one or more cells before Apply Selected Cells.")
            return
        if not self._same_setup_paths(self.setup_path_var.get().strip(), preview_path):
            self._set_error("Setup selection changed; click Load and Inspect before applying cells.")
            return
        if getattr(self, "eda_revalidation_required", False):
            self._stash_eda_edit_draft()
        elif self.prepared_id:
            # Re-Apply (including a changed module selection) must not erase
            # settings already matched or edited in the current catalogue.
            raw, context = self._snapshot_ui_state()
            self.pending_settings_state = build_settings_state(raw, context["parameter_by_id"],
                context["cell_scopes"], context["outputs"])
            self.pending_configuration_document = None
            self.pending_configuration_settings = None
        self.setup_loading = True
        self._cancel_parameter_render()
        self._set_setup_controls(False)
        self._set_status("读取 {} 个选中模块的器件，并私有化 schematic、验证 config 绑定…".format(len(scope_ids)))
        self._write_launch_state("applying_cells", selected_cell_scope_ids=scope_ids, error=None)
        def loaded(payload, matched_ui_state=None):
            if self.closing or inspection_id != self.cell_inspection_id:
                return
            if self.eda_revalidation_required and self.pending_settings_state and matched_ui_state is None:
                self._match_eda_prepared(payload, lambda state: loaded(payload, state), failed)
                return
            self.setup_loading = False
            previous_applied = set(self.applied_cell_scope_ids)
            restoring = bool(self.eda_revalidation_required)
            restore_document = copy.deepcopy(self.pending_settings_state)
            restore_path = self.eda_edit_draft_path
            try:
                # The matched draft must see the scopes which produced this
                # fresh preparation, rather than the previous Apply's scopes.
                self.applied_cell_scope_ids = set(scope_ids)
                self.cell_scope_dirty = self.selected_cell_scope_ids != self.applied_cell_scope_ids
                self._set_prepared(*payload, matched_ui_state=matched_ui_state)
            except Exception as error:
                self.applied_cell_scope_ids = previous_applied
                self.cell_scope_dirty = self.selected_cell_scope_ids != previous_applied
                self.prepared_id = None
                if restoring:
                    self.eda_revalidation_required = True
                    self.pending_settings_state = restore_document
                    self.eda_edit_draft_path = restore_path
                failed(error)
                return
            self._set_setup_controls(True)
            self._update_cell_selection_state()
            self.parameter_view_var.set("hierarchy")
            if not getattr(self, "eda_auto_revalidation", None):
                self.show_page("Parameters")
            if matched_ui_state is not None:
                self._advance_eda_revalidation(matched_ui_state[1])
        def failed(error):
            if self.closing or inspection_id != self.cell_inspection_id:
                return
            self.setup_loading = False
            self._set_setup_controls(True)
            self._update_cell_selection_state()
            self._finish_eda_revalidation(str(error))
            self._task_error("Apply Selected Cells")(error)
        self.tasks.submit(lambda: self._cache_prepared_defaults(self.service.prepare_cells(inspection_id, scope_ids)), loaded, failed)

    def _cache_prepared_defaults(self, payload):
        prepared = payload[1]
        parameters = [row for row in prepared.get("parameters") or [] if row.get("optimizable")]
        self._prepared_defaults = (prepared, parameter_defaults(parameters, initial_parameter_config))
        return payload

    def _set_prepared(self, prepared_id, prepared, matched_ui_state=None):
        prepared_path = (prepared.get("setup_locator") or {}).get("path") or prepared.get("setup_path") or prepared.get("maestro_path")
        if prepared_path:
            self._require_launch_setup(prepared_path)
        self._require_live_test_bindings(prepared)
        self.prepared_epoch = getattr(self, "prepared_epoch", 0) + 1
        if self.pending_parameter_click_job is not None:
            try:
                self.root.after_cancel(self.pending_parameter_click_job)
            except tk.TclError:
                pass
        self.pending_parameter_click = None
        self.pending_parameter_click_job = None
        if not prepared.get("cell_selection"):
            self._clear_cell_inspection()
        self.prepared_id = prepared_id
        self.prepared = prepared
        if getattr(self, "_baseline_mode_active", False):
            self._update_command_mode()
        self.execution_selection_panel.set_setup(prepared.get("setup") or {}, preserve=True)
        self._render_native_settings(prepared)
        self._render_private_setup(prepared.get("setup_private_session"))
        self._render_job_policy(prepared.get("job_policy"))
        self._render_save_options((prepared.get("setup") or {}).get("save_options") or prepared.get("save_options"))
        parameters = [row for row in prepared.get("parameters") or [] if row.get("optimizable")]
        self.parameter_by_id = dict((row["parameter_id"], row) for row in parameters)
        cached_defaults = getattr(self, "_prepared_defaults", None)
        self.parameter_configs = (cached_defaults[1] if cached_defaults is not None and cached_defaults[0] is prepared
                                  else parameter_defaults(parameters, initial_parameter_config))
        self._prepared_defaults = None
        self.parameter_open_items_by_view = {"hierarchy": set(), "name": set()}
        self.parameter_views_rendered = set()
        self.rendered_parameter_view = None
        self._activity_end("range:" + str(getattr(self, "range_validation_job_id", None)))
        self.range_validation_job_id = None
        self.range_validation_inflight = False
        self.range_validation_results = {}
        self.range_validation_requested = {}
        setup = prepared.get("setup") or {}
        setup_locator = prepared.get("setup_locator") or {}
        source = setup.get("source") or {}
        setup_path = (
            setup_locator.get("path") or prepared.get("setup_path") or
            prepared.get("maestro_path") or source.get("setup_db") or
            source.get("maestro_sdb") or source.get("data_sdb") or self.setup_path_var.get()
        )
        self.setup_path_var.set(setup_path)
        self.active_setup_path = setup_path
        tests = setup.get("tests") or []
        corners = setup.get("corners") or []
        outputs = prepared.get("outputs") or []
        backend = str(setup.get("backend") or setup_locator.get("backend") or "ADE").upper()
        resident = prepared.get("resident_validation") or {}
        resident_text = {
            "ready": "range workspace ready",
            "deferred": "range workspace starts on request",
        }.get(resident.get("state"), "range workspace unavailable")
        self.setup_summary_var.set(
            "{} · {} tests · {} corners · {} outputs · {} · {}".format(
                backend, len(tests), len(corners), len(outputs), resident_text, setup_path,
            )
        )
        self.sidebar_status_var.set("{} · {} parameters selected".format(
            backend, sum(1 for value in self.parameter_configs.values() if value.get("selected")),
        ))
        from .ade_setup import test_corner_availability
        corner_state = test_corner_availability(setup, self.selected_test or (tests[0].get("name") if tests else None))
        unknown_corners = [row.get("name") for row in corner_state if row.get("enabled", True) and row.get("test_enabled") is None]
        analysis_names = setup.get("analysis_names") or []
        output_names = [str(row.get("key")) for row in outputs]
        self.analysis_summary_var.set(
            "{} named outputs. Analyses above apply to the Test selected in Design Setup.".format(
                len(output_names),
            ) + (" Corner enablement unavailable: " + ", ".join(unknown_corners) if unknown_corners else "")
        )
        self.render_parameter_tree()
        self._reset_metric_rows()
        self.add_objective()
        prepared_state = "disabled" if getattr(self, "eda_revalidation_required", False) else "normal"
        self.start_button.configure(state=prepared_state)
        self.validate_button.configure(state=prepared_state)
        self.cancel_validation_button.configure(state="disabled")
        self.range_status_var.set("Static and optional PDK callback validation have not run.")
        self._refresh_algorithm_defaults(force=True)
        self._set_status("ADE setup loaded successfully")
        pdk = (prepared.get("eda_context") or {}).get("pdk") or {}
        cadence = (prepared.get("eda_context") or {}).get("cadence") or {}
        if cadence.get("effective"):
            self._render_eda_setup(cadence)
        else:
            self._render_effective_eda_setup(getattr(self, "eda_setup", {}) or {})
        self._write_launch_state(
            "prepared", error=None, prepared_id=prepared_id,
            counts={
                "tests": len(tests), "corners": len(corners), "outputs": len(outputs),
                "parameters": len(prepared.get("parameters") or []),
                "optimizable_parameters": len(parameters),
                "hierarchy_designs": len((prepared.get("hierarchy") or {}).get("designs") or []),
            },
            pdk={
                "name": pdk.get("pdk_name") or pdk.get("name"),
                "names": [row.get("name") or row.get("id") for row in pdk.get("contexts") or []],
                "blocking": bool(pdk.get("blocking")),
            },
            resident_validation_state=resident.get("state"),
        )
        if matched_ui_state is None:
            self._restore_pending_ui_state()
        else:
            self._apply_ui_settings(*matched_ui_state)

    @staticmethod
    def _same_eda_edit_draft(first, second):
        # Save time is metadata, not a user edit. Slow matching must not retry
        # forever simply because the next snapshot crossed a clock second.
        return {key: value for key, value in (first or {}).items() if key != "saved_at"} == {
            key: value for key, value in (second or {}).items() if key != "saved_at"}

    def _match_eda_prepared(self, payload, commit, failed):
        """Match the new catalogue before replacing the visible editable draft."""
        self._stash_eda_edit_draft()
        document = copy.deepcopy(self.pending_settings_state)
        context = snapshot_copy(self._settings_context(prepared_override=payload[1]))
        inspection_id = self.cell_inspection_id
        selection_epoch = self.setup_selection_epoch
        prepared_epoch = self.prepared_epoch
        def matched(result):
            if (self.closing or not self.setup_loading or inspection_id != self.cell_inspection_id
                    or selection_epoch != self.setup_selection_epoch or prepared_epoch != self.prepared_epoch):
                return
            self._stash_eda_edit_draft()
            if not self._same_eda_edit_draft(self.pending_settings_state, document):
                # The user edited the still-visible draft while matching. Match
                # that latest document instead of overwriting it with the old one.
                self._match_eda_prepared(payload, commit, failed)
                return
            commit((document, result))
        self.tasks.submit(lambda: match_settings_state(document, context), matched, failed)

    def _stash_eda_edit_draft(self):
        """Keep raw edits apart from the revoked native preparation authority."""
        catalog = self.prepared or self.cell_inspection or {}
        if not catalog:
            # Initial tool configuration has no loaded catalogue to revoke or
            # bind a draft to. Keep any explicitly loaded UI state untouched.
            if not getattr(self, "eda_revalidation_required", False):
                self.eda_edit_draft_path = None
            return
        raw, context = self._snapshot_ui_state()
        path = ((catalog.get("setup_locator") or {}).get("path") or catalog.get("setup_path")
                or catalog.get("maestro_path") or self.setup_path_var.get().strip())
        raw["setup"]["path"] = path
        document = build_settings_state(raw, context["parameter_by_id"], context["cell_scopes"], context["outputs"])
        if not self.prepared and self.pending_settings_state:
            # Inspection has cells but no parameters yet. Keep deferred edits,
            # updating only controls that the user can currently change.
            deferred = copy.deepcopy(self.pending_settings_state)
            deferred["optimization"] = document["optimization"]
            if self.cell_scopes:
                deferred["cells"] = document["cells"]
            document = deferred
        self.pending_settings_state = document
        self.eda_edit_draft_path = path

    def _invalidate_prepared(self, message, preserve_edits=False):
        if preserve_edits:
            self._stash_eda_edit_draft()
            self.eda_revalidation_required = bool(self.eda_edit_draft_path)
            self._cancel_cell_render()
            self.cell_inspection_id = None
            self.apply_cells_button.configure(state="disabled")
        else:
            self._clear_cell_inspection()
        self.pending_parameter_focus = None
        self._cancel_parameter_render()
        self.prepared_epoch = getattr(self, "prepared_epoch", 0) + 1
        if self.pending_parameter_click_job is not None:
            try:
                self.root.after_cancel(self.pending_parameter_click_job)
            except tk.TclError:
                pass
        self.pending_parameter_click = None
        self.pending_parameter_click_job = None
        self.prepared_id = None
        if getattr(self, "_baseline_mode_active", False):
            self._update_command_mode()
        if not preserve_edits:
            self.prepared = None
            self.parameter_configs = {}
            self.parameter_by_id = {}
            self.parameter_items = {}
            self.node_parameters = {}
            self.parameter_item_labels = {}
            self.node_item_labels = {}
            self.parameter_open_items_by_view = {"hierarchy": set(), "name": set()}
            self.parameter_views_rendered = set()
            self.rendered_parameter_view = None
        self._activity_end("range:" + str(getattr(self, "range_validation_job_id", None)))
        self.range_validation_job_id = None
        self.range_validation_inflight = False
        self.range_validation_results = {}
        self.range_validation_requested = {}
        if not preserve_edits and hasattr(self, "parameter_tree"):
            for item in self.parameter_tree.get_children():
                self.parameter_tree.delete(item)
        if not preserve_edits:
            self._reset_metric_rows()
        self.start_button.configure(state="disabled")
        self.validate_button.configure(state="disabled")
        self.cancel_validation_button.configure(state="disabled")
        self.apply_ranges_button.configure(state="disabled")
        self.setup_summary_var.set(message)
        self.sidebar_status_var.set("Edits retained; revalidation required" if preserve_edits else "No setup loaded")
        if not preserve_edits:
            self.analysis_summary_var.set("Load a setup to discover outputs and corners.")
        self.range_status_var.set("Load and Inspect, then Apply Selected Cells before validating ranges.")
        self._set_status(message)

    def _prepared_is_current(self, prepared_epoch, prepared_id):
        return (
            not getattr(self, "setup_loading", False) and
            not getattr(self, "eda_revalidation_required", False) and
            not getattr(self, "cell_scope_dirty", False) and
            self._prepared_selection_matches() and
            getattr(self, "prepared_epoch", 0) == prepared_epoch and
            self.prepared_id == prepared_id
        )

    def _corner_names(self):
        from .corner_selection import eligible_corner_names
        setup = (self.prepared or {}).get("setup", {})
        return eligible_corner_names(setup, getattr(self, "selected_test", None) or next((r.get("name") for r in setup.get("tests") or []), None))

    def _default_corner_names(self, output_key=None):
        return self._corner_names()

    def _output_names(self):
        return [str(row.get("key")) for row in (self.prepared or {}).get("outputs") or []]

    def _reset_metric_rows(self):
        for row in self.objective_rows + self.constraint_rows:
            row.destroy()
        self.objective_rows = []
        self.constraint_rows = []
        self._metric_count_changed()

    def add_objective(self, definition=None):
        definition = dict(definition or {})
        if "corners" not in definition:
            definition["corners"] = self._default_corner_names(definition.get("output_key"))
        row = MetricRow(
            self.objective_container, "objective", self._output_names(), self._corner_names(),
            self._remove_objective, self._metric_count_changed, definition,
        )
        row.pack(fill="x", pady=3)
        self.objective_rows.append(row)
        self._metric_count_changed()

    def add_constraint(self, definition=None):
        definition = dict(definition or {})
        if "corners" not in definition:
            definition["corners"] = self._default_corner_names(definition.get("output_key"))
        row = MetricRow(
            self.constraint_container, "constraint", self._output_names(), self._corner_names(),
            self._remove_constraint, self._metric_count_changed, definition,
        )
        row.pack(fill="x", pady=3)
        self.constraint_rows.append(row)

    def _remove_objective(self, row):
        if row in self.objective_rows:
            self.objective_rows.remove(row)
            row.destroy()
            self._metric_count_changed()

    def _remove_constraint(self, row):
        if row in self.constraint_rows:
            self.constraint_rows.remove(row)
            row.destroy()

    def _settings_objective_count(self):
        pending = getattr(self, "pending_settings_state", None)
        if (getattr(self, "eda_revalidation_required", False) and pending
                and (not self.prepared or self.prepared_id)):
            # Reinspection temporarily has no metric rows. Preserve the
            # pending multiobjective policy until fresh rows are restored.
            return max(1, len(pending.get("objectives") or []))
        return max(1, len(self.objective_rows))

    def _metric_count_changed(self):
        count = self._settings_objective_count()
        requested = self.algorithm_var.get() if hasattr(self, "algorithm_var") else "differential_evolution"
        if count >= 2 and requested != "nsga2":
            self.last_single_algorithm = requested
        try:
            policy = resolve_algorithm_policy(requested, count, coerce=True)
        except ValueError:
            policy = resolve_algorithm_policy(None, count, coerce=True)
        if hasattr(self, "algorithm_var"):
            self.algorithm_var.set(policy["algorithm"])
            self.algorithm_combo.configure(state="disabled" if policy["locked"] else "readonly")
            if policy["locked"]:
                self.algorithm_notice_var.set(policy["notice"] or "Multiple objectives use NSGA-II.")
            else:
                if requested == "nsga2" and self.last_single_algorithm:
                    self.algorithm_var.set(self.last_single_algorithm)
                self.algorithm_notice_var.set("Single-objective algorithms are available.")
            self._render_algorithm_parameter_fields()
        self.render_pareto()

    def _build_parameter_forest(self, prepared=None, catalog=None, cancel_event=None):
        prepared = (self.prepared if prepared is None else prepared) or {}
        catalog = self.parameter_by_id if catalog is None else catalog
        forest = []
        scopes = (prepared.get("cell_selection") or {}).get("scopes") or []
        in_scope = ScopeIndex(scopes).contains
        for design_index, design in enumerate((prepared.get("hierarchy") or {}).get("designs") or []):
            if cancel_event is not None and cancel_event.is_set():
                return []
            hierarchy = design.get("hierarchy") or {}
            cells = dict((row.get("hierarchy_path"), row) for row in hierarchy.get("cells") or [])
            root_cell = cells.get("/") or {}
            design_name = (design.get("design") or ["", "Design {}".format(design_index + 1)])
            root = {
                "id": "design-{}:/".format(design_index), "path": "/",
                "name": root_cell.get("cell") or design_name[1], "kind": "cell",
                "library": root_cell.get("library") or design_name[0],
                "master_cell": root_cell.get("cell") or design_name[1],
                "parameters": [], "children": [],
            }
            nodes = {"/": root}
            instances = sorted(
                hierarchy.get("instances") or [], key=lambda row: int(row.get("depth") or 0),
            )
            for instance in instances:
                if cancel_event is not None and cancel_event.is_set():
                    return []
                path = instance.get("hierarchy_path")
                nodes[path] = {
                    "id": "design-{}:{}".format(design_index, path), "path": path,
                    "name": instance.get("name") or os.path.basename(path),
                    "kind": "cell" if instance.get("kind") == "hierarchical" else "device",
                    "library": instance.get("master_library") or "",
                    "master_cell": instance.get("master_cell") or "",
                    "parameters": [], "children": [],
                    "display_without_parameters": bool(scopes and instance.get("kind") != "hierarchical" and
                                                       in_scope(design.get("design") or {}, path)),
                }
            for instance in instances:
                if cancel_event is not None and cancel_event.is_set():
                    return []
                node = nodes.get(instance.get("hierarchy_path"))
                parent = nodes.get(instance.get("parent_path") or "/")
                if node and parent:
                    parent["children"].append(node)
            attached = {}
            for raw in hierarchy.get("parameters") or []:
                if cancel_event is not None and cancel_event.is_set():
                    return []
                parameter = catalog.get(raw.get("parameter_id"))
                node = nodes.get(raw.get("hierarchy_path"))
                seen = attached.setdefault(raw.get("hierarchy_path"), set())
                if parameter and node and parameter["parameter_id"] not in seen:
                    seen.add(parameter["parameter_id"])
                    node["parameters"].append(parameter)

            def prune(node):
                children = [prune(child) for child in node["children"]]
                node["children"] = [child for child in children if child]
                node["children"].sort(key=lambda child: (child["kind"] != "cell", child["name"]))
                node["parameters"].sort(key=lambda row: str(row.get("name") or ""))
                return node if node["children"] or node["parameters"] or node.get("display_without_parameters") else None

            root = prune(root)
            if root:
                forest.append(root)
        if not forest and catalog:
            root = {
                "id": "flat:/", "path": "/", "name": "Optimizable Parameters",
                "kind": "cell", "library": "", "master_cell": "",
                "parameters": list(catalog.values()), "children": [],
            }
            forest.append(root)
        return forest

    @staticmethod
    def _subtree_parameter_ids(node):
        result = [row["parameter_id"] for row in node["parameters"]]
        for child in node["children"]:
            result.extend(OptimizerGui._subtree_parameter_ids(child))
        return result

    def _parameter_view_changed(self):
        view = self.parameter_view_var.get()
        heading = "Hierarchy / Parameter" if view == "hierarchy" else "Parameter / Owning Device"
        self.parameter_tree.heading("#0", text=heading)
        self.render_parameter_tree()

    @staticmethod
    def _selection_marker(selected_count, total_count):
        if total_count and selected_count == total_count:
            return CHECKED_MARK
        if selected_count:
            return PARTIAL_MARK
        return UNCHECKED_MARK

    def _cancel_parameter_render(self):
        self.parameter_render_epoch = getattr(self, "parameter_render_epoch", 0) + 1
        event = getattr(self, "parameter_render_cancel", None)
        if event is not None:
            event.set()
        identifier = getattr(self, "parameter_render_job", None)
        if identifier is not None:
            try:
                self.root.after_cancel(identifier)
            except tk.TclError:
                pass
        self.parameter_render_job = None
        self._parameter_build_key = None
        self.parameter_selection_epoch = getattr(self, "parameter_selection_epoch", 0) + 1
        identifier = getattr(self, "parameter_selection_job", None)
        if identifier is not None:
            try:
                self.root.after_cancel(identifier)
            except tk.TclError:
                pass
        self.parameter_selection_job = None
        self.parameter_forced_open = None

    def _parameter_filter_changed(self, *_args):
        query = self.parameter_filter_var.get().strip().lower()
        if query == getattr(self, "_parameter_filter_query", ""):
            return
        self._parameter_filter_query = query
        job = getattr(self, "_parameter_filter_job", None)
        if job is not None:
            self.root.after_cancel(job)
        self.parameter_tree_dirty = True
        def render():
            self._parameter_filter_job = None
            self.render_parameter_tree()
        self._parameter_filter_job = self.root.after(180, render)

    def _parameter_tree_key(self):
        return (getattr(self, "prepared_epoch", 0), id(self.prepared), id(self.parameter_by_id),
                len(self.parameter_by_id), self.parameter_view_var.get(),
                self.parameter_filter_var.get().strip().lower())

    def _remember_parameter_tree(self, key):
        self._parameter_tree_cache[key] = {
            "roots": self.parameter_tree.get_children(""),
            "parameter_items": self.parameter_items, "node_parameters": self.node_parameters,
            "parameter_item_labels": self.parameter_item_labels, "node_item_labels": self.node_item_labels,
            "_parameter_row_state": self._parameter_row_state,
        }

    def render_parameter_tree(self):
        if not hasattr(self, "parameter_tree") or getattr(self, "closing", False):
            return
        if self.parameter_view_var.get() not in ("hierarchy", "name"):
            self.parameter_view_var.set("hierarchy")
        key = self._parameter_tree_key()
        if getattr(self, "current_page", None) != "Parameters":
            self.parameter_tree_dirty = True
            return
        job = getattr(self, "_parameter_filter_job", None)
        if job is not None:
            self.root.after_cancel(job)
            self._parameter_filter_job = None
        if key == getattr(self, "_parameter_build_key", None) and getattr(self, "parameter_tree_dirty", False):
            return
        self._cancel_parameter_render()
        self.parameter_tree_dirty = True
        previous_view = self.rendered_parameter_view
        if previous_view:
            # Preserve the previous view before switching; leaves have no expansion state.
            self.parameter_open_items_by_view[previous_view] = set(
                item for item in self.node_parameters
                if self.parameter_tree.exists(item) and self.parameter_tree.item(item, "open")
            )
        cache = getattr(self, "_parameter_tree_cache", {})
        self._parameter_tree_cache = cache
        if cache and next(iter(cache))[:4] != key[:4]:
            for state in cache.values():
                for item in state["roots"]:
                    if self.parameter_tree.exists(item):
                        self.parameter_tree.delete(item)
            cache.clear()
        cached = cache.get(key)
        if cached is not None:
            if getattr(self, "_parameter_visible_key", None) not in cache:
                partial = self.parameter_tree.get_children("")
                if partial:
                    self.parameter_tree.delete(*partial)
            self.parameter_tree.set_children("", *cached["roots"])
            for name, value in cached.items():
                if name != "roots":
                    setattr(self, name, value)
            self.rendered_parameter_view = key[4]
            self._parameter_visible_key = key
            self._parameter_build_key = None
            self.parameter_tree_dirty = False
            self._refresh_parameter_selection_display()
            self._finish_parameter_focus()
            return
        self._parameter_build_key = key
        epoch = self.parameter_render_epoch
        event = threading.Event()
        self.parameter_render_cancel = event
        view = self.parameter_view_var.get()
        if view not in ("hierarchy", "name"):
            view = "hierarchy"
            self.parameter_view_var.set(view)
        query = self.parameter_filter_var.get().strip().lower()
        open_items = set(self.parameter_open_items_by_view.get(view, set()))
        first_render = view not in self.parameter_views_rendered
        prepared, catalog = self.prepared, self.parameter_by_id
        self.parameter_summary_var.set("Preparing {} parameter rows...".format(len(catalog)))

        def matches(parameter):
            if not query:
                return True
            return query in "{} {} {} {} {}".format(
                parameter.get("name") or parameter.get("cdf_name") or "",
                parameter.get("prompt") or "", parameter.get("current_value") or "",
                parameter_owner_label(parameter), parameter.get("parameter_id") or "",
            ).lower()

        def build_rows():
            if event.is_set():
                return []
            if view == "name":
                groups = group_parameters_by_name(catalog.values())
                for group in groups:
                    if event.is_set():
                        return []
                    parameters = group["parameters"]
                    group_matches = not query or query in group["name"].lower()
                    group["render_parameters"] = [row for row in parameters if group_matches or matches(row)]
                    group["render_devices"] = len(set(parameter_owner_label(row) for row in parameters))
                    group["render_parameter_ids"] = [row["parameter_id"] for row in parameters]
                return groups
            forest = self._build_parameter_forest(prepared=prepared, catalog=catalog, cancel_event=event)
            def decorate(node):
                if event.is_set():
                    return
                ids = [row["parameter_id"] for row in node["parameters"]]
                child_matches = False
                for child in node["children"]:
                    decorate(child)
                    ids.extend(child.get("render_parameter_ids", []))
                    child_matches = child_matches or child.get("render_matches", False)
                node["render_parameter_ids"] = ids
                own = "{} {} {} {}".format(node["name"], node["path"], node["library"], node["master_cell"]).lower()
                node["render_matches"] = not query or query in own or child_matches or any(matches(row) for row in node["parameters"])
            for node in forest:
                decorate(node)
            return forest

        def loaded(model):
            if event.is_set() or self.closing or epoch != self.parameter_render_epoch:
                return
            current_roots = self.parameter_tree.get_children("")
            visible = getattr(self, "_parameter_visible_key", None)
            if visible in cache:
                if current_roots:
                    self.parameter_tree.detach(*current_roots)
            elif current_roots:
                self.parameter_tree.delete(*current_roots)
            # Keep only the latest query for each view; bound retained Tk rows.
            for old_key in list(cache):
                if old_key[4] == view:
                    for item in cache.pop(old_key)["roots"]:
                        if self.parameter_tree.exists(item):
                            self.parameter_tree.delete(item)
            self._parameter_visible_key = None
            self._parameter_row_state = {}
            self.parameter_items, self.node_parameters = {}, {}
            self.parameter_item_labels, self.node_item_labels = {}, {}

            def parameter_row(parent, parameter, label):
                return ("parameter", parent, ("parameter:" if view == "hierarchy" else "parameter-name:") + parameter["parameter_id"], label, parameter)

            def hierarchy_rows(parent, node, is_root=False):
                if not node.get("render_matches"):
                    return
                identity = "node:" + node["id"]
                label = node["name"] + (" · " + node["master_cell"] if not is_root and node["master_cell"] else "")
                if node.get("display_without_parameters") and not node["parameters"]:
                    label += " · no optimizable parameters"
                yield ("node", parent, identity, label, (node["render_parameter_ids"], is_root))
                for parameter in node["parameters"]:
                    if query and not matches(parameter):
                        continue
                    label = "{} · {}".format(parameter.get("name") or parameter["parameter_id"],
                                           parameter.get("prompt") or parameter.get("cdf_type") or "Numeric CDF")
                    yield parameter_row(identity, parameter, label)
                for child in node["children"]:
                    for row in hierarchy_rows(identity, child):
                        yield row

            def rows():
                if view == "hierarchy":
                    for node in model:
                        for row in hierarchy_rows("", node, True):
                            yield row
                else:
                    for index, group in enumerate(model):
                        parameters = group["render_parameters"]
                        if not parameters:
                            continue
                        identity = "name-group:{:04d}".format(index)
                        devices = group["render_devices"]
                        label = "{} ({} device{})".format(group["name"], devices, "" if devices == 1 else "s")
                        yield ("node", "", identity, label, (group["render_parameter_ids"], True))
                        for parameter in parameters:
                            yield parameter_row(identity, parameter, parameter_owner_label(parameter))
            iterator = rows()

            def insert_chunk():
                self.parameter_render_job = None
                if event.is_set() or self.closing or epoch != self.parameter_render_epoch:
                    return
                started = time.monotonic()
                for count in range(1200):
                    try:
                        kind, parent, identity, label, value = next(iterator)
                    except StopIteration:
                        self.rendered_parameter_view = view
                        self.parameter_views_rendered.add(view)
                        self.parameter_open_items_by_view[view] = open_items
                        self.parameter_tree_dirty = False
                        self._parameter_visible_key = key
                        self._parameter_build_key = None
                        self._remember_parameter_tree(key)
                        self._refresh_parameter_selection_display(
                            refresh_items=getattr(self, "parameter_selection_job", None) is not None)
                        self._finish_parameter_focus()
                        return
                    if kind == "parameter":
                        parameter_id = value["parameter_id"]
                        config = self.parameter_configs[parameter_id]
                        marker = self._selection_marker(int(bool(config.get("selected"))), 1)
                        values = (value.get("current_value", ""), config.get("minimum", ""), config.get("maximum", ""),
                                  config.get("unit", ""), config.get("scale", "linear"), self._parameter_validation_text(parameter_id))
                        self.parameter_tree.insert(parent, "end", iid=identity, text="{} {}".format(marker, label), values=values, tags=("parameter",))
                        self.parameter_items[parameter_id] = identity
                        self.parameter_item_labels[parameter_id] = label
                        self._parameter_row_state[identity] = ("{} {}".format(marker, label), values)
                    else:
                        parameter_ids, root = value
                        selected = sum(bool(self.parameter_configs.get(key, {}).get("selected")) for key in parameter_ids)
                        marker = self._selection_marker(selected, len(parameter_ids))
                        self.parameter_tree.insert(parent, "end", iid=identity, text="{} {}".format(marker, label),
                                                   values=("", "", "", "", "", "{}/{} selected".format(selected, len(parameter_ids))),
                                                   open=(self.parameter_forced_open if self.parameter_forced_open is not None else
                                                         identity in open_items or (first_render and root)))
                        self.node_parameters[identity] = parameter_ids
                        self.node_item_labels[identity] = label
                        self._parameter_row_state[identity] = ("{} {}".format(marker, label),
                            ("", "", "", "", "", "{}/{} selected".format(selected, len(parameter_ids))))
                    if time.monotonic() - started >= 0.035:
                        break
                self.parameter_render_job = self.root.after(1, insert_chunk)
            insert_chunk()

        def failed(error):
            if not event.is_set() and not self.closing and epoch == self.parameter_render_epoch:
                self._set_error("Parameter view: {}".format(error))
        self.tasks.submit(build_rows, loaded, failed)

    def _refresh_parameter_selection_display(self, refresh_items=True):
        if not hasattr(self, "parameter_tree") or getattr(self, "closing", False):
            return
        self.parameter_selection_epoch = getattr(self, "parameter_selection_epoch", 0) + 1
        epoch = self.parameter_selection_epoch
        render_epoch = getattr(self, "parameter_render_epoch", 0)
        identifier = getattr(self, "parameter_selection_job", None)
        if identifier is not None:
            self.root.after_cancel(identifier)
        self.parameter_selection_job = None
        selected = sum(1 for value in self.parameter_configs.values() if value.get("selected"))
        self.parameter_summary_var.set(
            "{} meaningful parameters · {} selected".format(len(self.parameter_configs), selected)
        )
        self.sidebar_status_var.set(
            "{} parameters selected".format(selected) if self.prepared else "No setup loaded"
        )
        self._refresh_algorithm_defaults(force=False)
        if not refresh_items:
            return

        state = getattr(self, "_parameter_row_state", {})
        self._parameter_row_state = state
        def updates():
            for parameter_id, item in list(self.parameter_items.items()):
                config = self.parameter_configs.get(parameter_id, {})
                marker = self._selection_marker(int(bool(config.get("selected"))), 1)
                label = self.parameter_item_labels.get(parameter_id, parameter_id)
                parameter = self.parameter_by_id.get(parameter_id, {})
                values = (parameter.get("current_value", ""), config.get("minimum", ""), config.get("maximum", ""),
                          config.get("unit", ""), config.get("scale", "linear"), self._parameter_validation_text(parameter_id))
                yield item, "{} {}".format(marker, label), values
            for item, parameter_ids in list(self.node_parameters.items()):
                selected_count = sum(bool(self.parameter_configs.get(key, {}).get("selected")) for key in parameter_ids)
                marker = self._selection_marker(selected_count, len(parameter_ids))
                label = self.node_item_labels.get(item, item)
                yield item, "{} {}".format(marker, label), ("", "", "", "", "", "{}/{} selected".format(selected_count, len(parameter_ids)))
        iterator = updates()
        large = len(self.parameter_items) + len(self.node_parameters) > 512

        def update_chunk():
            self.parameter_selection_job = None
            if (self.closing or epoch != self.parameter_selection_epoch or
                    render_epoch != getattr(self, "parameter_render_epoch", 0)):
                return
            started = time.monotonic()
            for item, text, values in iterator:
                value = (text, values)
                if state.get(item) != value:
                    self.parameter_tree.item(item, text=text, values=values)
                    state[item] = value
                if large and time.monotonic() - started >= 0.012:
                    self.parameter_selection_job = self.root.after(1, update_chunk)
                    return
        update_chunk()

    def _collect_open_items(self, item, result):
        if self.parameter_tree.item(item, "open"):
            result.add(item)
        for child in self.parameter_tree.get_children(item):
            self._collect_open_items(child, result)

    def _collect_tree_items(self, item, result):
        result.add(item)
        for child in self.parameter_tree.get_children(item):
            self._collect_tree_items(child, result)

    def _parameter_validation_text(self, parameter_id):
        rows = self.range_validation_results.get(parameter_id) or []
        if not rows:
            return "Not checked"
        labels = []
        for endpoint in ("minimum", "maximum"):
            row = next((item for item in rows if item.get("endpoint") == endpoint), None)
            if not row:
                continue
            status = row.get("status") or "unknown"
            if status == "pdk_adjusted":
                labels.append("{} adjusted → {}".format(endpoint[:3], row.get("effective")))
            elif status == "validation_failed":
                reason = _compact_diagnostic(row.get("error"), 90)
                labels.append("{} failed{}".format(
                    endpoint[:3], ": " + reason if reason else "",
                ))
            else:
                labels.append("{} {}".format(endpoint[:3], status))
        return "; ".join(labels) or "Not checked"

    def _parameter_id_for_item(self, item):
        for parameter_id, candidate_item in self.parameter_items.items():
            if candidate_item == item:
                return parameter_id
        return None

    def _parameter_single_click(self, event):
        item = self.parameter_tree.identify_row(event.y)
        column = self.parameter_tree.identify_column(event.x)
        if not item:
            return
        self.parameter_tree.selection_set(item)
        self.parameter_tree.focus(item)
        parameter_id = self._parameter_id_for_item(item)
        if column == "#0":
            try:
                element = self.parameter_tree.identify("element", event.x, event.y)
            except (AttributeError, tk.TclError):
                element = ""
            if "indicator" in str(element).lower():
                return
            self._commit_pending_parameter_click()
            previous = self._toggle_tree_item(item, mark_dirty=True)
            self.pending_parameter_click = {"item": item, "previous": previous}
            self.pending_parameter_click_job = self.root.after(
                500, self._commit_pending_parameter_click,
            )
            return "break"
        if parameter_id and column == "#5":
            config = self.parameter_configs[parameter_id]
            if config.get("value_type") != "integer":
                config["scale"] = "log" if config.get("scale") == "linear" else "linear"
                self._mark_range_dirty(parameter_id)
                self.parameter_tree.set(item, "scale", config["scale"])
                self.parameter_tree.set(item, "validation", self._parameter_validation_text(parameter_id))
            return "break"

    def _parameter_double_click(self, event):
        item = self.parameter_tree.identify_row(event.y)
        column = self.parameter_tree.identify_column(event.x)
        if not item:
            return
        parameter_id = self._parameter_id_for_item(item)
        if column == "#0":
            pending = self.pending_parameter_click or {}
            if pending.get("item") == item:
                if self.pending_parameter_click_job is not None:
                    self.root.after_cancel(self.pending_parameter_click_job)
                for changed_id, selected in pending.get("previous", {}).items():
                    self.parameter_configs[changed_id]["selected"] = selected
                self.pending_parameter_click = None
                self.pending_parameter_click_job = None
                self._refresh_parameter_selection_display()
            return "break"
        if parameter_id and column in ("#2", "#3"):
            self._edit_parameter_cell(item, parameter_id, column)
        return "break"

    def _toggle_selected_tree_item(self, _event=None):
        selected = self.parameter_tree.selection()
        if selected:
            self._toggle_tree_item(selected[0])
        return "break"

    def _toggle_tree_item(self, item, mark_dirty=True):
        parameter_id = self._parameter_id_for_item(item)
        previous = {}
        if parameter_id:
            config = self.parameter_configs[parameter_id]
            previous[parameter_id] = bool(config.get("selected"))
            config["selected"] = not config.get("selected")
            if mark_dirty:
                self._mark_range_dirty(parameter_id)
        else:
            parameter_ids = self.node_parameters.get(item) or []
            select = not all(self.parameter_configs[parameter_id].get("selected") for parameter_id in parameter_ids)
            for parameter_id in parameter_ids:
                previous[parameter_id] = bool(self.parameter_configs[parameter_id].get("selected"))
                self.parameter_configs[parameter_id]["selected"] = select
                if mark_dirty:
                    self._mark_range_dirty(parameter_id, update_display=False)
            if mark_dirty and parameter_ids:
                self._mark_range_dirty(parameter_ids[-1])
        self._refresh_parameter_selection_display()
        return previous

    def _commit_pending_parameter_click(self):
        job = self.pending_parameter_click_job
        self.pending_parameter_click = None
        self.pending_parameter_click_job = None
        if job is not None:
            try:
                self.root.after_cancel(job)
            except tk.TclError:
                pass

    def _edit_parameter_cell(self, item, parameter_id, column):
        bbox = self.parameter_tree.bbox(item, column)
        if not bbox:
            return
        x, y, width, height = bbox
        field = "minimum" if column == "#2" else "maximum"
        config = self.parameter_configs[parameter_id]
        editor = ttk.Entry(self.parameter_tree)
        editor.insert(0, config.get(field, ""))
        editor.place(x=x, y=y, width=width, height=height)
        editor.focus_set()
        editor.selection_range(0, "end")

        def finish(_event=None):
            config[field] = editor.get().strip()
            editor.destroy()
            self._mark_range_dirty(parameter_id)
            self.render_parameter_tree()

        editor.bind("<Return>", finish)
        editor.bind("<FocusOut>", finish)
        editor.bind("<Escape>", lambda _event: editor.destroy())

    def _mark_range_dirty(self, parameter_id=None, update_display=True):
        if parameter_id:
            self.range_validation_results.pop(parameter_id, None)
            self.range_validation_requested.pop(parameter_id, None)
        else:
            self.range_validation_results = {}
            self.range_validation_requested = {}
        if update_display:
            self.range_status_var.set("Parameter selection or bounds changed; validate again if callback checking is desired.")
            self.apply_ranges_button.configure(state="disabled")

    def select_semantic(self, semantic, selected, button=None):
        changed = None
        for parameter_id, parameter in self.parameter_by_id.items():
            if str(parameter.get("semantic") or "").lower() == semantic:
                self.parameter_configs[parameter_id]["selected"] = selected
                self._mark_range_dirty(parameter_id, update_display=False)
                changed = parameter_id
        if changed is not None:
            self._mark_range_dirty(changed)
        self._flash_parameter_button(button)
        self._refresh_parameter_selection_display()

    def select_all_parameters(self, selected, button=None):
        for config in self.parameter_configs.values():
            config["selected"] = selected
        self._mark_range_dirty()
        self._flash_parameter_button(button)
        self._refresh_parameter_selection_display()

    def _flash_parameter_button(self, button):
        if not button:
            return
        original = button.cget("text")
        button.configure(text="Done · {}".format(original))
        self.root.after(
            420,
            lambda: button.configure(text=original) if button.winfo_exists() else None,
        )

    def _set_tree_open(self, opened):
        self.parameter_forced_open = bool(opened)
        epoch = getattr(self, "parameter_render_epoch", 0)
        iterator = iter(list(self.node_parameters))
        large = len(self.node_parameters) > 512

        def open_chunk():
            if self.closing or epoch != getattr(self, "parameter_render_epoch", 0):
                return
            started = time.monotonic()
            for item in iterator:
                self.parameter_tree.item(item, open=opened)
                if large and time.monotonic() - started >= 0.012:
                    self.root.after(1, open_chunk)
                    return
        open_chunk()

    def focus_parameter(self, parameter_id, field=None):
        if self.closing or parameter_id not in self.parameter_by_id:
            return
        self.show_page("Parameters")
        self.parameter_filter_var.set("")
        self.pending_parameter_focus = (parameter_id, field, self.prepared_id)
        self.render_parameter_tree()

    def _finish_parameter_focus(self):
        pending = getattr(self, "pending_parameter_focus", None)
        if not pending or self.closing:
            return
        self.pending_parameter_focus = None
        parameter_id, field, prepared_id = pending
        if prepared_id != self.prepared_id:
            return
        item = self.parameter_items.get(parameter_id)
        if not item or not self.parameter_tree.exists(item):
            return
        parent = self.parameter_tree.parent(item)
        while parent:
            self.parameter_tree.item(parent, open=True)
            parent = self.parameter_tree.parent(parent)
        self.parameter_tree.selection_set(item)
        self.parameter_tree.focus(item)
        self.parameter_tree.see(item)
        if field in ("minimum", "maximum"):
            column = "#2" if field == "minimum" else "#3"
            epoch = self.parameter_render_epoch
            def edit():
                if (not self.closing and prepared_id == self.prepared_id and
                        epoch == self.parameter_render_epoch and self.parameter_tree.exists(item)):
                    self._edit_parameter_cell(item, parameter_id, column)
            self.root.after(100, edit)

    def _local_range_report(self):
        return validate_variable_configs(
            list(self.parameter_by_id.values()), self.parameter_configs,
        )

    def _range_report_work(self):
        method = self._local_range_report
        if getattr(method, "__func__", None) is not OptimizerGui._local_range_report:
            # Preserve injected validators and frontend overrides.
            return method
        parameters = list(self.parameter_by_id.values())
        configs = snapshot_copy({key: value for key, value in self.parameter_configs.items()
                                 if value.get("selected")})
        return lambda: validate_variable_configs(parameters, configs)

    def _show_range_issues(self, issues):
        RangeIssueWindow(self, issues)
        self.range_status_var.set("{} range problem{} found.".format(
            len(issues), "" if len(issues) == 1 else "s",
        ))

    def _static_range_report(self, on_valid, button=None, optional=False):
        prepared_epoch = getattr(self, "prepared_epoch", 0)
        prepared_id = self.prepared_id
        local_work = self._range_report_work()

        def validate():
            local_report = local_work()
            if local_report["issues"]:
                return local_report, None
            variables = local_report["variables"]
            method = getattr(self.service, "validate_static_ranges", None)
            report = method(prepared_id, variables) if method else {"valid": True, "variables": variables, "issues": []}
            return local_report, report

        def validated(payload):
            if not self._prepared_is_current(prepared_epoch, prepared_id):
                return
            local_report, report = payload
            variables = local_report["variables"]
            issues = local_report["issues"] or (report or {}).get("issues") or []
            report = report or {"valid": not issues}
            if not report.get("valid", not issues) or issues:
                if button is getattr(self, "start_button", None):
                    self.start_request_pending = False
                self._show_range_issues(issues)
                if button:
                    button.configure(state="normal")
                return
            on_valid(variables)

        def release_button():
            if button:
                button.configure(state="normal")
            if button is getattr(self, "start_button", None):
                self.start_request_pending = False
        task_error = self._task_error("Validate Parameter Ranges", release_button)

        def failed(error):
            if self._prepared_is_current(prepared_epoch, prepared_id):
                task_error(error)

        submit = getattr(self.tasks, "submit_optional", self.tasks.submit) if optional else self.tasks.submit
        submit(validate, validated, failed)

    def validate_all_ranges(self):
        if not self.prepared_id:
            return
        try:
            self._require_prepared_selection()
        except ValueError as error:
            self._set_error(str(error))
            return
        prepared_epoch = getattr(self, "prepared_epoch", 0)
        prepared_id = self.prepared_id
        self.validate_button.configure(state="disabled")
        self.range_status_var.set("Checking numeric ranges...")

        def static_valid(variables):
            if not self._prepared_is_current(prepared_epoch, prepared_id):
                return
            self.range_validation_results = {}
            self.range_validation_requested = dict((row["parameter_id"], row) for row in variables)
            self.range_status_var.set("Optional PDK check is starting. Optimization can start independently.")

            def start_validation():
                endpoints = [
                    {"parameter_id": row["parameter_id"], "minimum": row["minimum"], "maximum": row["maximum"]}
                    for row in variables
                ]
                return self.service.validate_ranges(prepared_id, endpoints)

            def started(payload):
                if not self._prepared_is_current(prepared_epoch, prepared_id):
                    return
                self.range_validation_job_id = payload["job_id"]
                self._activity_begin("range:" + str(payload["job_id"]), "Optional PDK check…", priority=5)
                self.cancel_validation_button.configure(state="normal")
                self._poll_range_validation()

            def failed(error):
                if self._prepared_is_current(prepared_epoch, prepared_id):
                    self.validate_button.configure(state="normal")
                    self.range_validation_job_id = None
                    self.cancel_validation_button.configure(state="disabled")
                    self.range_validation_detail = "{}: {}".format(error.__class__.__name__, error)[-65536:]
                    self.range_status_var.set("Optional PDK check unavailable. Optimization can start; see Details.")

            getattr(self.tasks, "submit_optional", self.tasks.submit)(start_validation, started, failed)

        self._static_range_report(static_valid, button=self.validate_button, optional=True)

    def cancel_optional_validation(self):
        prepared_id = self.prepared_id
        self.cancel_validation_button.configure(state="disabled")
        self.range_status_var.set("Optional PDK check cancellation requested. Optimization is independent.")
        self._activity_update("range:" + str(self.range_validation_job_id), label="Stopping optional PDK check…")
        def cancel():
            return self.service.cancel_range_validations(prepared_id=prepared_id)
        def failed(error):
            self.range_validation_detail = "{}: {}".format(error.__class__.__name__, error)[-65536:]
            self.range_status_var.set("PDK check cancellation needs attention; see Details. Optimization is independent.")
        getattr(self.tasks, "submit_control", self.tasks.submit)(cancel, None, failed)

    def _poll_range_validation(self):
        if getattr(self, "closing", False) or getattr(self.tasks, "shutting_down", False):
            return
        if not self.range_validation_job_id or self.range_validation_inflight:
            return
        job_id = self.range_validation_job_id
        activity_token = "range:" + str(job_id)
        prepared_epoch = getattr(self, "prepared_epoch", 0)
        self.range_validation_inflight = True

        requested = set(getattr(self, "range_validation_requested", {}))
        def load():
            job = self.service.range_validation_status(job_id)
            grouped = {}
            for row in (job or {}).get("results") or []:
                if row.get("parameter_id") in requested:
                    grouped.setdefault(row["parameter_id"], []).append(row)
            detail = json_detail_tail({key: job.get(key) for key in (
                "state", "phase", "stage", "message", "detail", "traceback", "detail_path", "job_root", "timeout_seconds",
                "timed_out", "completed", "total", "cleanup_proven", "cleanup_error", "diagnostic_write_error", "results",
            ) if key in job}) if job else ""
            return job, grouped, detail

        def loaded(payload):
            job, grouped, detail = payload
            if (
                    job_id != self.range_validation_job_id or
                    prepared_epoch != getattr(self, "prepared_epoch", 0)):
                self._activity_end(activity_token)
                return
            self.range_validation_inflight = False
            if not job:
                self._activity_end(activity_token)
                self.validate_button.configure(state="normal")
                self.cancel_validation_button.configure(state="disabled")
                self.range_status_var.set("Optional PDK check status unavailable. Optimization can start.")
                return
            self.range_validation_results = grouped
            adjusted = sum(1 for rows in grouped.values() for row in rows if row.get("status") == "pdk_adjusted")
            failed = sum(1 for rows in grouped.values() for row in rows if row.get("status") == "validation_failed")
            state = str(job.get("state") or "unknown")
            self.range_validation_detail = detail
            if state in ("failed", "skipped", "timed_out"):
                status_text = "Optional PDK check {}. Optimization can start; see Details.".format(
                    "timed out" if job.get("timed_out") or state == "timed_out" else "unavailable")
            elif state in ("cancelled", "cancelling"):
                status_text = "Optional PDK check {}. Optimization is independent.".format(state)
            elif state == "completed":
                status_text = "Optional PDK check complete: {} adjusted, {} failed. See Details.".format(adjusted, failed)
            else:
                status_text = "Optional PDK check: {} ({}/{}). Optimization is independent.".format(
                    job.get("phase") or job.get("stage") or state, len(job.get("results") or []), job.get("parameter_count", 0) * 2)
            self.range_status_var.set(status_text)
            if job.get("state") in ("completed", "failed", "cancelled", "skipped", "timed_out"):
                self._activity_end(activity_token)
            else:
                total = job.get("total") or (job.get("parameter_count") or 0) * 2
                completed = job.get("completed") if job.get("completed") is not None else len(job.get("results") or [])
                self._activity_begin(activity_token, status_text, total=total, completed=completed, priority=5)
            self.apply_ranges_button.configure(state="normal" if adjusted else "disabled")
            self._refresh_parameter_selection_display()
            if job.get("state") in ("completed", "failed", "cancelled", "skipped", "timed_out"):
                self.validate_button.configure(state="normal")
                self.cancel_validation_button.configure(state="disabled")
            else:
                self.root.after(1000, self._poll_range_validation)

        def failed(error):
            if (
                    job_id != self.range_validation_job_id or
                    prepared_epoch != getattr(self, "prepared_epoch", 0)):
                self._activity_end(activity_token)
                return
            self.range_validation_inflight = False
            self.validate_button.configure(state="normal")
            self.cancel_validation_button.configure(state="disabled")
            self._activity_end(activity_token)
            self.range_validation_detail = "{}: {}".format(error.__class__.__name__, error)[-65536:]
            self.range_status_var.set("Optional PDK check status unavailable. Optimization can start; see Details.")

        getattr(self.tasks, "submit_optional", self.tasks.submit)(load, loaded, failed)

    def apply_validated_ranges(self):
        changes = []
        for parameter_id, rows in self.range_validation_results.items():
            config = self.parameter_configs.get(parameter_id)
            if not config:
                continue
            for row in rows:
                if row.get("status") != "pdk_adjusted":
                    continue
                endpoint = row.get("endpoint")
                field = "minimum" if endpoint == "minimum" else "maximum"
                numeric = parse_physical_input(row.get("effective"))
                if numeric is None:
                    continue
                old_value = config.get(field)
                if config.get("value_type") == "integer":
                    new_value = str(int(round(numeric)))
                else:
                    new_value = format_physical(numeric, config.get("unit") or "")
                config[field] = new_value
                row["status"] = "applied"
                changes.append("{} {}: {} → {}".format(
                    parameter_label(self.parameter_by_id[parameter_id]), field, old_value, new_value,
                ))
        self.render_parameter_tree()
        report = self._local_range_report()
        if report["issues"]:
            self._show_range_issues(report["issues"])
        else:
            self.range_status_var.set("Applied {} legal PDK range adjustment{}; static ranges remain valid.".format(
                len(changes), "" if len(changes) == 1 else "s",
            ))
        self.apply_ranges_button.configure(state="disabled")

    def _collect_objectives(self):
        return [row.definition("Objective {}".format(index + 1)) for index, row in enumerate(self.objective_rows)]

    def _collect_constraints(self):
        return [row.definition("Constraint {}".format(index + 1)) for index, row in enumerate(self.constraint_rows)]

    def _algorithm_changed(self, _event=None):
        if len(self.objective_rows) < 2:
            self.last_single_algorithm = self.algorithm_var.get()
        self._render_algorithm_parameter_fields()

    def _toggle_advanced(self):
        if self.advanced_visible.get():
            self.algorithm_parameter_frame.pack(fill="x", pady=(10, 0))
        else:
            self.algorithm_parameter_frame.pack_forget()

    def _refresh_algorithm_defaults(self, force=False):
        if not hasattr(self, "algorithm_parameter_vars"):
            return
        if force:
            self.algorithm_parameter_manual.clear()
        count = max(1, sum(1 for value in self.parameter_configs.values() if value.get("selected")))
        for algorithm in ALGORITHM_FIELDS:
            defaults = calculated_algorithm_defaults(algorithm, count)
            for field, value in defaults.items():
                variable = self.algorithm_parameter_vars[(algorithm, field)]
                if force or (algorithm, field) not in self.algorithm_parameter_manual:
                    variable.set(str(value))
        self._render_algorithm_parameter_fields()

    def _render_algorithm_parameter_fields(self):
        if not hasattr(self, "algorithm_parameter_frame"):
            return
        for child in self.algorithm_parameter_frame.winfo_children():
            child.destroy()
        algorithm = self.algorithm_var.get() or "differential_evolution"
        fields = ALGORITHM_FIELDS.get(algorithm, ())
        for index, field in enumerate(fields):
            row, column = divmod(index, 4)
            frame = ttk.Frame(self.algorithm_parameter_frame, style="Card.TFrame")
            frame.grid(row=row, column=column, sticky="ew", padx=4, pady=4)
            ttk.Label(frame, text=field.replace("_", " ").title(), style="Card.TLabel").pack(anchor="w")
            entry = ttk.Entry(frame, textvariable=self.algorithm_parameter_vars[(algorithm, field)])
            entry.pack(fill="x")
            entry.bind(
                "<KeyRelease>",
                lambda _event, key=(algorithm, field): self.algorithm_parameter_manual.add(key),
            )
            self.algorithm_parameter_frame.columnconfigure(column, weight=1)

    def _collect_algorithm_settings(self):
        algorithm = self.algorithm_var.get()
        policy = resolve_algorithm_policy(algorithm, max(1, len(self.objective_rows)), coerce=True)
        algorithm = policy["algorithm"]
        parameters = {}
        integer_fields = frozenset(("population", "seed", "initial_samples", "candidate_pool", "max_observations"))
        for field in ALGORITHM_FIELDS[algorithm]:
            text = self.algorithm_parameter_vars[(algorithm, field)].get().strip()
            numeric = finite_number(text)
            if numeric is None:
                raise ValueError("{} must be numeric.".format(field.replace("_", " ").title()))
            if field in integer_fields:
                if int(numeric) != numeric:
                    raise ValueError("{} must be an integer.".format(field.replace("_", " ").title()))
                numeric = int(numeric)
            parameters[field] = numeric
        return algorithm, parameters

    def _build_run_settings(self):
        generations = finite_number(self.generations_var.get())
        raw_timeout = self.timeout_var.get().strip()
        timeout = -1 if not raw_timeout else finite_number(raw_timeout)
        if generations is None or int(generations) != generations or generations <= 1:
            raise ValueError("Generations must be an integer greater than 1.")
        if timeout is None or (raw_timeout and timeout <= 0):
            raise ValueError("Preparation/measurement fallback budget must be positive seconds or blank for no limit; zero is not allowed.")
        algorithm, parameters = self._collect_algorithm_settings()
        result = {
            "generations": int(generations), "jobs": 1,
            "candidate_timeout_seconds": timeout,
            "algorithm": algorithm, "algorithm_parameters": parameters,
        }
        backend = self.execution_backend_var.get() if hasattr(self, "execution_backend_var") else "auto"
        if backend not in ("auto", "maestro_netlist", "persistent_maestro", "legacy", "adexl", "private_netlist", "maestro_baseline"):
            raise ValueError("Select Maestro or Private Netlist as the execution method.")
        result["execution_backend"] = backend if backend in ("private_netlist", "maestro_baseline") else "maestro_netlist"
        if backend != "maestro_baseline":
            result["execution_commands"] = self._collect_execution_commands()
        simulator = self.simulator_var.get() if hasattr(self, "simulator_var") else "spectre"
        if simulator not in ("spectre", "alps"):
            raise ValueError("Select Spectre or ALPS as the simulator.")
        result["simulator"] = simulator
        result.update(self._collect_job_settings())
        panel = getattr(self, "execution_selection_panel", None)
        selected_execution = panel.collect() if panel is not None else None
        if selected_execution is not None:
            result["execution_selection"] = selected_execution
        if hasattr(self, "data_root_override_var"):
            root = self.data_root_var.get().strip() if self.data_root_override_var.get() else None
            if self.data_root_override_var.get() and (not root or "\x00" in root):
                raise ValueError("Enter a data directory for the storage override.")
            result["data_root_override"] = root
        return result

    def build_design_config(self):
        if not self.prepared:
            raise ValueError("Load an ADE setup first.")
        self._require_prepared_selection()
        report = self._local_range_report()
        if report["issues"]:
            self._show_range_issues(report["issues"])
            raise ValueError("Parameter ranges are invalid.")
        return {
            "schema_version": "crescendo.design_config.v1",
            "ade_setup": {
                "path": self.setup_path_var.get().strip(),
                "backend": (self.prepared.get("setup") or {}).get("backend") or
                           (self.prepared.get("setup_locator") or {}).get("backend") or "auto",
            },
            "optimization": {
                "variables": report["variables"],
                "objectives": self._collect_objectives(),
                "constraints": self._collect_constraints(),
                "run": self._build_run_settings(),
            },
        }

    def _settings_context(self, prepared_override=None):
        config = getattr(self.service, "config", {}) or {}
        source = config.get("_selected_setup_source") or (self._active_context() or {}).get("source") or {}
        catalog = (prepared_override if prepared_override is not None else
                   (self.prepared if self.prepared_id else self.cell_inspection or self.prepared)) or {}
        locator = catalog.get("setup_locator") or {}
        identity = {key: source.get(key) or locator.get(key + "_name") for key in ("library", "cell", "view")}
        setup = catalog.get("setup") or {}
        parameters = ({row["parameter_id"]: row for row in prepared_override.get("parameters") or [] if row.get("optimizable")}
                      if prepared_override is not None else self.parameter_by_id)
        output_catalog = prepared_override if prepared_override is not None else self.prepared
        corners = setup.get("corners") or []
        return {
            "setup": {"identity": identity, "designs": setup.get("tests") or [], "tests": setup.get("tests") or [],
                      "backend": setup.get("backend") or locator.get("backend"),
                      "path": self.setup_path_var.get()},
            "maestro_snapshot": maestro_settings_snapshot(setup, catalog.get("job_policy") or getattr(self, "original_job_policy", None),
                setup.get("save_options") or catalog.get("save_options") or getattr(self, "original_save_options", None)),
            "parameter_by_id": parameters,
            "cell_scopes": self.cell_scopes,
            "outputs": (output_catalog or {}).get("outputs") or [],
            "corners": (([str(row["name"]) for row in corners if row.get("name")] or (["_default"] if not corners else []))
                        if prepared_override is not None else self._corner_names()),
        }

    @staticmethod
    def _raw_metric_settings(row):
        value = {"output_key": row.output_var.get(), "aggregation": row.aggregation_var.get(),
                 "corners": row.corner_picker.selected()}
        if row.kind == "objective":
            value.update(direction=row.mode_var.get(), target=row.value_var.get())
        else:
            value.update(operator=row.mode_var.get(), limit=row.value_var.get())
        return value

    def _snapshot_ui_state(self):
        context, parameter_configs = snapshot_copy((self._settings_context(), self.parameter_configs))
        optimization = {
            "diagnostic_generations": self.diagnostic_generations_var.get() if hasattr(self, "diagnostic_generations_var") else "2",
            "diagnostic_total_candidates": self.diagnostic_total_var.get() if hasattr(self, "diagnostic_total_var") else "6",
            "algorithm": self.algorithm_var.get(), "generations": self.generations_var.get(),
            "candidate_timeout_seconds": self.timeout_var.get(),
            "advanced_visible": self.advanced_visible.get(),
            "last_single_algorithm": self.last_single_algorithm,
            "run_name": getattr(self, "last_run_display_name", ""),
            "algorithm_parameters": {algorithm: {field: self.algorithm_parameter_vars[(algorithm, field)].get()
                                    for field in fields} for algorithm, fields in ALGORITHM_FIELDS.items()},
            "algorithm_parameter_manual": [list(pair) for pair in sorted(self.algorithm_parameter_manual)],
            "job_command_override": self.job_command_override_var.get(),
            "job_command": self.job_command_var.get(), "job_max_override": self.job_max_override_var.get(),
            "job_max_jobs": self.job_max_var.get(), "maestro_candidate_mode": self.maestro_candidate_mode_var.get(),
            "execution_backend": self.execution_backend_var.get() if hasattr(self, "execution_backend_var") else "auto",
            "simulator": (getattr(self, "_cli_simulator_draft", "spectre") if getattr(self, "_baseline_mode_active", False)
                          else self.simulator_var.get()) if hasattr(self, "simulator_var") else "spectre",
            "execution_commands": self._execution_command_values(),
            "data_root_override_enabled": self.data_root_override_var.get() if hasattr(self, "data_root_override_var") else False,
            "data_root_override": self.data_root_var.get() if hasattr(self, "data_root_var") else "",
        }
        for key, variable in getattr(self, "job_timeout_vars", {}).items():
            optimization["job_timeout_" + key] = variable.get()
            optimization["job_timeout_override_" + key] = self.job_timeout_override_vars[key].get()
        if hasattr(self, "command_scheduler_var"):
            optimization["command_scheduler"] = self.command_scheduler_var.get()
        raw = {"setup": context["setup"], "parameter_configs": parameter_configs,
               "selected_cell_scope_ids": sorted(self.selected_cell_scope_ids),
               "objectives": [self._raw_metric_settings(row) for row in self.objective_rows],
               "constraints": [self._raw_metric_settings(row) for row in self.constraint_rows],
               "optimization": optimization,
               "maestro_snapshot": snapshot_copy(context.get("maestro_snapshot") or {}),
               "execution_selection": self.execution_selection_panel.collect(require_selection=False)}
        return raw, context

    def _snapshot_configuration(self):
        raw, context = self._snapshot_ui_state()
        manual = copy.deepcopy((getattr(self, "eda_setup", {}) or {}).get("manual") or {})
        # Draft text is never validated by Save and is not authority to execute.
        fields = {key: variable.get() for key, variable in getattr(self, "eda_vars", {}).items()
                  if key != "license_file"}
        manual.pop("license_file", None)
        for key, value in fields.items():
            if value.strip():
                manual[key] = value.strip()
            else:
                manual.pop(key, None)
        draft = {"fields": fields}
        for name, key in (("eda_profiles", "pdk_profiles_text"),
                          ("eda_alps_model_map", "alps_model_map_text"),
                          ("eda_binary_paths", "extra_binary_paths_text"),
                          ("eda_library_paths", "extra_library_paths_text")):
            widget = getattr(self, name, None)
            if widget is not None:
                draft[key] = widget.get("1.0", "end").rstrip("\n")
        if hasattr(self, "eda_policy_var"):
            draft["policy"] = self.eda_policy_var.get()
        raw["environment_overrides"] = manual
        raw["environment_draft"] = draft
        return raw, context

    def save_configuration(self):
        if self.closing:
            return
        try:
            raw, context = self._snapshot_configuration()
            config = copy.deepcopy(getattr(self.service, "config", {}))
            launch = copy.deepcopy(self._active_context())
            directory = settings_state_directory(config, launch)
            path = filedialog.asksaveasfilename(parent=self.root, title="Save Configuration",
                initialdir=directory, initialfile=settings_filename(config), defaultextension=".json",
                filetypes=(("JSON", "*.json"),))
            if not path:
                return
            path = os.path.abspath(path)
            previous = copy.deepcopy(getattr(self, "configuration_document", None))
            suite = copy.deepcopy(getattr(self, "active_private_suite", None))
            if suite:
                suite["revision"] = suite.get("revision") or suite.get("baseline_id")
                suite["source_path"] = suite.get("source_path") or getattr(self, "active_setup_path", None)
            known = dict(getattr(self, "configuration_revisions", {}))
            known_suites = dict(getattr(self, "suite_configuration_revisions", {}))
            save_epoch = (self.settings_load_epoch, self.setup_selection_epoch, self.prepared_epoch)
            started_during_load = bool(getattr(self, "configuration_loading", False))
        except Exception as error:
            self._configuration_io_warning("保存配置", error)
            return
        self.save_config_button.configure(state="disabled")
        self.settings_status_var.set("正在保存配置…")
        def save():
            document = build_configuration(raw, context["parameter_by_id"], context["cell_scopes"],
                                           context["outputs"], previous=previous)
            suite_id = suite.get("id") if suite else None
            if suite_id:
                document["private_suite"] = {"suite_id": suite_id,
                    "revision": suite.get("revision"), "source_path": suite.get("source_path")}
            last_path = os.path.abspath(last_settings_path(config, launch))
            outcomes = []
            def attempt(label, target, operation):
                try:
                    value = operation()
                    outcomes.append({"label": label, "target": target, "ok": True, "result": value})
                    return True
                except Exception as error:
                    item = {"label": label, "target": target, "ok": False, "error": str(error),
                            "commit_state": getattr(error, "configuration_commit_state", None)}
                    if item["commit_state"]:
                        try:
                            item["observed_revision"] = (self.service.private_suite_configuration_revision(target)
                                if label == "私有套件" else configuration_revision(target))
                        except Exception as read_error:
                            item["readback_error"] = str(read_error)
                    outcomes.append(item)
                    return False
            def expected(target):
                return known[target] if target in known else configuration_revision(target)
            # Never propagate an obsolete draft to secondary destinations if the
            # explicit user file did not save successfully.
            if not attempt("配置文件", path, lambda: save_configuration(path, document,
                    expected_revision=expected(path), return_result=True)):
                return document, outcomes
            if suite_id:
                def save_suite():
                    revision = (known_suites[suite_id] if suite_id in known_suites else
                                self.service.private_suite_configuration_revision(suite_id))
                    return self.service.save_private_suite_configuration(suite_id, document, expected_revision=revision)
                attempt("私有套件", suite_id, save_suite)
            if last_path == path:
                outcomes.append({"label": "Restore Last", "target": last_path, "ok": True,
                                 "result": outcomes[0]["result"]})
            else:
                attempt("Restore Last", last_path, lambda: save_last_configuration(document, config, launch,
                        expected_revision=expected(last_path), return_result=True))
            return document, outcomes
        def saved(payload):
            if self.closing:
                return
            document, outcomes = payload
            self.save_config_button.configure(state="normal")
            current_epoch = (self.settings_load_epoch, self.setup_selection_epoch, self.prepared_epoch)
            current = not started_during_load and save_epoch == current_epoch and not self.configuration_loading
            if current:
                if not hasattr(self, "configuration_revisions"):
                    self.configuration_revisions = {}
                if not hasattr(self, "suite_configuration_revisions"):
                    self.suite_configuration_revisions = {}
                for item in outcomes:
                    if item["ok"]:
                        revision = item["result"]["revision"]
                        if item["label"] == "私有套件":
                            self.suite_configuration_revisions[item["target"]] = revision
                        else:
                            self.configuration_revisions[item["target"]] = revision
                if outcomes[0]["ok"]:
                    self.configuration_document = document
            lines = []
            for item in outcomes:
                if item["ok"]:
                    lines.append("{}：已保存，revision {}".format(item["label"], item["result"]["revision"]))
                else:
                    detail = "{}：保存未确认：{}".format(item["label"], item["error"])
                    if item.get("commit_state"):
                        detail += "；提交状态 {}，回读 revision {}".format(item["commit_state"],
                            item.get("observed_revision", "不可读取：" + item.get("readback_error", "未知")))
                    lines.append(detail)
            if not current:
                self._set_status("配置文件保存结果已生成；当前加载的设置保留。")
                return
            self.settings_match_detail = "\n".join(lines)
            summary = "配置已保存" if all(item["ok"] for item in outcomes) else "配置保存部分完成或未确认；详见 Details。"
            self.settings_status_var.set(summary)
            self._set_status(summary)
        def failed(error):
            if not self.closing:
                self.save_config_button.configure(state="normal")
                self._configuration_io_warning("保存配置", error)
        self.tasks.submit(save, saved, failed)

    def _configuration_io_warning(self, action, error):
        message = "{}未完成：{}".format(action, error)
        if "保存" not in action:
            message += "；当前界面设置保留。"
        state = getattr(error, "configuration_commit_state", None)
        if state:
            message += "；文件提交状态：{}，请回读确认。".format(state)
        self.settings_status_var.set(message[:500])
        self.settings_match_detail = message
        self._set_status(message)

    def save_ui_state(self):
        return self.save_configuration()

    def load_ui_state(self, restore_last=False):
        return self.load_configuration(restore_last=restore_last)

    def restore_last_ui_state(self):
        return self.load_configuration(restore_last=True)

    def load_configuration(self, restore_last=False, suite_id=None, suite_sections=None, preserve_suite_draft=False):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_status(reason)
            return
        try:
            config = copy.deepcopy(getattr(self.service, "config", {}))
            launch = copy.deepcopy(self._active_context())
            context = snapshot_copy(self._settings_context())
            directory = settings_state_directory(config, launch)
            path = None if restore_last or suite_id else filedialog.askopenfilename(parent=self.root,
                title="Load Configuration (including old State / Config)", initialdir=directory,
                filetypes=(("JSON", "*.json"), ("All files", "*")))
            if not restore_last and not suite_id and not path:
                return
            use_live_draft = bool(suite_id and (preserve_suite_draft or (suite_sections is not None and
                (getattr(self, "active_private_suite", None) or {}).get("id") == suite_id)))
            if suite_id:
                raw, snapshot_context = self._snapshot_configuration()
                draft = build_configuration(raw, snapshot_context["parameter_by_id"],
                    snapshot_context["cell_scopes"], snapshot_context["outputs"],
                    previous=copy.deepcopy(getattr(self, "configuration_document", None)))
        except Exception as error:
            self._configuration_io_warning("读取配置", error)
            return
        self.settings_load_epoch += 1
        epoch = self.settings_load_epoch
        selection_epoch = self.setup_selection_epoch
        prepared_epoch = self.prepared_epoch
        self.configuration_loading = True
        self.setup_loading = True
        self._set_setup_controls(False)
        self.load_config_button.configure(state="disabled")
        self.restore_last_button.configure(state="disabled")
        self.settings_status_var.set("正在加载私有套件及已保存的范围和参数…" if suite_id else
                                     "正在加载配置、建立私有副本并自动 Apply…")
        target_path = self.setup_path_var.get().strip() or None
        current_scope_ids = list(self.selected_cell_scope_ids)
        current_test = getattr(self, "selected_test", None)
        def still_current():
            return (not self.closing and epoch == self.settings_load_epoch and
                    selection_epoch == self.setup_selection_epoch and prepared_epoch == self.prepared_epoch and
                    self.configuration_loading and not self.start_request_pending and
                    self.run_ui.state not in ("running", "stopping"))
        def load():
            if suite_id:
                payload = (self.service.update_private_suite(suite_id, suite_sections)
                           if suite_sections is not None else self.service.load_private_suite(suite_id))
                if use_live_draft:
                    payload["document"] = draft
                    payload.pop("settings", None)
                    payload.pop("match", None)
                elif not payload.get("document"):
                    payload["document"] = payload.get("saved_configuration") or draft
                return payload
            read_result = (read_last_configuration(config, launch, context["setup"], return_result=True) if restore_last
                           else read_configuration(path, return_result=True))
            document = read_result["document"]
            if not still_current():
                return {"configuration_superseded": True}
            # One service lifecycle spans native create + apply, including Stop.
            try:
                payload = self.service.load_configuration(document, path=target_path,
                    selected_scope_ids=current_scope_ids, selected_test=current_test)
            except Exception as error:
                partial = getattr(error, "configuration_import", None)
                if isinstance(partial, dict):
                    partial["configuration_file_revision"] = {"path": read_result["path"], "revision": read_result["revision"]}
                raise
            payload.setdefault("document", document)
            payload["configuration_file_revision"] = {"path": read_result["path"], "revision": read_result["revision"]}
            return payload
        def finish():
            self.configuration_loading = False
            self.setup_loading = False
            active = bool(self.start_request_pending or self.run_ui.state in ("running", "stopping"))
            self._set_setup_controls(not active)
            self.load_config_button.configure(state="disabled" if active else "normal")
            self.restore_last_button.configure(state="disabled" if active else "normal")
        def loaded(payload):
            if self.closing or epoch != self.settings_load_epoch:
                return
            if payload.get("configuration_superseded") or not still_current():
                finish()
                self.settings_status_var.set("当前 TB、参数目录或运行状态已变化；过期配置未应用，现有设置保留。")
                return
            try:
                if payload.get("selection"):
                    self._apply_setup_selection(payload["selection"])
                document = payload["document"]
                def remember_revisions():
                    file_revision = payload.get("configuration_file_revision")
                    if file_revision:
                        if not hasattr(self, "configuration_revisions"):
                            self.configuration_revisions = {}
                        self.configuration_revisions[file_revision["path"]] = file_revision["revision"]
                    active_suite_id = suite_id or (payload.get("suite") or {}).get("id")
                    if active_suite_id and "suite_configuration_revision" in payload:
                        if not hasattr(self, "suite_configuration_revisions"):
                            self.suite_configuration_revisions = {}
                        self.suite_configuration_revisions[active_suite_id] = payload["suite_configuration_revision"]
                if suite_id:
                    suite = payload.get("suite") or {"id": suite_id}
                    self.active_private_suite = copy.deepcopy(suite)
                elif not payload.get("preserve_current_suite"):
                    self.active_private_suite = copy.deepcopy(payload.get("suite"))
                test_catalog = payload.get("test_inspection")
                if (suite_id or payload.get("suite")) and test_catalog is None:
                    suite_setup = ((payload.get("inspection") or {}).get("setup") or
                                   (payload.get("prepared") or {}).get("setup") or {})
                    suite_tests = suite_setup.get("tests") or []
                    if suite_tests:
                        self._set_test_catalog(None, {"setup": suite_setup}, auto_select=False)
                        self.selected_test = (suite_setup.get("selected_test") or
                            (suite_tests[0].get("name") if len(suite_tests) == 1 else None))
                        self.test_choice_var.set(self.selected_test or "")
                if test_catalog is not None:
                    self._set_test_catalog(payload.get("test_inspection_id"), test_catalog, auto_select=False)
                    self.selected_test = payload.get("selected_test")
                    self.test_choice_var.set(self.selected_test or "")
                settings = payload.get("settings") or configuration_settings(document)
                inspection = payload.get("inspection")
                if test_catalog is not None and not self.selected_test:
                    finish()
                    self.configuration_document = document
                    remember_revisions()
                    self.settings_status_var.set("Configuration does not identify one Test. Select a Test to continue; older multi-Test selections are not applied.")
                    for warning in payload.get("warnings") or []:
                        self._activity_event(str(warning), "WARN")
                    return
                if inspection is not None:
                    self._set_cell_inspection(payload["inspection_id"], inspection)
                if suite_id:
                    self.active_private_suite = copy.deepcopy(payload.get("suite") or {"id": suite_id})
                self.configuration_document = document
                if not payload.get("preserve_current_suite"):
                    self.pending_configuration_document = copy.deepcopy(document)
                    self.pending_configuration_settings = copy.deepcopy(settings)
                    self.pending_settings_state = settings
                if inspection is not None:
                    self.selected_cell_scope_ids = set(payload.get("selected_cell_scope_ids", payload.get("selected_scope_ids")) or [])
                    self._render_cell_scopes()
                prepared = payload.get("prepared")
                result = payload.get("match")
                if result is None:
                    result = match_configuration(document, self._settings_context(prepared_override=prepared))
                if prepared is not None:
                    self.applied_cell_scope_ids = set(self.selected_cell_scope_ids)
                    self.cell_scope_dirty = False
                    self._set_prepared(payload["prepared_id"], prepared, matched_ui_state=(settings, result))
                    if payload.get("suite"):
                        self.active_private_suite = copy.deepcopy(payload["suite"])
                    self.eda_revalidation_required = False
                    self.eda_edit_draft_path = None
                    self.eda_auto_revalidation = None
                else:
                    self._apply_ui_settings(settings, result,
                        preserve_existing=payload.get("preserve_current_suite", False))
                if payload.get("eda_setup"):
                    self._render_eda_setup(payload["eda_setup"])
                warnings = list(payload.get("warnings") or [])
                if payload.get("preparation_error"):
                    warnings.append(str(payload["preparation_error"]))
                self._restore_configuration_environment_draft(document)
                remember_revisions()
                finish()
                if inspection is not None:
                    self._update_cell_selection_state()
                display_result = {key: value for key, value in result.items()
                    if key not in ("document", "pending", "environment_overrides", "environment_draft")}
                detail = {"summary": result.get("summary"), "matching": display_result,
                          "warnings": warnings, "preparation_error": payload.get("preparation_error")}
                self.settings_match_detail = json.dumps(detail, indent=2, ensure_ascii=False)[-65536:]
                ready = bool(prepared is not None and not self.cell_scope_dirty)
                if payload.get("preserve_current_suite"):
                    message = "当前私有套件已保留；配置中匹配当前选择的设置已加载。"
                elif ready and (suite_id or payload.get("suite")):
                    message = "私有套件的 Test、范围和参数已恢复；可点击 Start Optimization。"
                else:
                    message = ("配置已恢复，Load / Apply 完成；无需重新设置，可点击 Start Optimization。" if ready else
                               "配置草稿已恢复；可继续选择模块并 Apply，已有设置会自动匹配。")
                for warning in warnings:
                    self._activity_event(str(warning), "WARN")
                if warnings or result.get("warnings"):
                    message += " 差异项采用当前 Maestro，其余已恢复；见主日志和 Details。"
                self.settings_status_var.set(message)
                self._set_status(message)
                self._write_launch_state("prepared" if ready else "cells_ready",
                    configuration_loaded=True, prepared_id=self.prepared_id, error=None)
            except Exception as error:
                finish()
                self._configuration_io_warning("恢复配置", error)
        def failed(error):
            if self.closing or epoch != self.settings_load_epoch:
                return
            if not still_current():
                finish()
                self.settings_status_var.set("当前 TB、参数目录或运行状态已变化；过期配置未应用，现有设置保留。")
                return
            if (getattr(error, "private_suite_invalidated", False) and
                    (getattr(self, "active_private_suite", None) or {}).get("id") == suite_id):
                self._invalidate_prepared("私有套件更新失败且原状态无法证明；请重新选择套件。", preserve_edits=True)
                self.active_private_suite = None
            partial = getattr(error, "configuration_import", None)
            from .cadence import CommandCancelled
            if isinstance(error, CommandCancelled):
                finish()
                warnings = (partial or {}).get("warnings") or []
                for warning in warnings:
                    self._activity_event(str(warning), "WARN")
                message = "配置加载已取消；现有界面设置保留。"
                if any("retained private suite resources" in str(item) for item in warnings):
                    message += " 私有套件资源及其来源已保留，请在 Private Suites 中检查。"
                self.settings_status_var.set(message)
                self._set_status(message)
                return
            if isinstance(partial, dict) and partial.get("preserve_current_suite"):
                finish()
                self._configuration_io_warning("加载配置", error)
                return
            if isinstance(partial, dict) and partial.get("document"):
                partial = copy.deepcopy(partial)
                partial.pop("prepared", None)
                partial.pop("prepared_id", None)
                partial["preparation_error"] = str(error)
                loaded(partial)
                return
            finish()
            self._configuration_io_warning("加载配置", error)
        self.tasks.submit(load, loaded, failed)

    def _restore_configuration_environment_draft(self, document):
        # Effective environment stays the validated service result. Draft fields
        # can be edited without asserting that a foreign executable/model works.
        draft = document.get("environment_draft") or {}
        fields = draft.get("fields") or {}
        for key, value in fields.items():
            variable = getattr(self, "eda_vars", {}).get(key)
            if variable is not None and isinstance(value, str):
                variable.set(value)
        for name, key in (("eda_profiles", "pdk_profiles_text"),
                          ("eda_alps_model_map", "alps_model_map_text"),
                          ("eda_binary_paths", "extra_binary_paths_text"),
                          ("eda_library_paths", "extra_library_paths_text")):
            if key in draft and hasattr(self, name):
                self._replace_text_widget(getattr(self, name), draft[key])
        if draft.get("policy") in ("auto", "custom", "generic-only") and hasattr(self, "eda_policy_var"):
            self.eda_policy_var.set(draft["policy"])
        if draft and hasattr(self, "eda_status_var"):
            self.eda_status_var.set("配置中的环境输入已作为草稿恢复；实际运行使用上方当前有效值。")

    def _restore_pending_ui_state(self):
        document = getattr(self, "pending_settings_state", None)
        if not document or self.closing or getattr(self, "configuration_loading", False):
            return
        if (getattr(self, "eda_revalidation_required", False)
                and not self._same_setup_paths(self.setup_path_var.get().strip(), self.eda_edit_draft_path)):
            return
        epoch = self.settings_load_epoch
        prepared_epoch = self.prepared_epoch
        selection_epoch = self.setup_selection_epoch
        inspection_id = self.cell_inspection_id
        context = snapshot_copy(self._settings_context())
        def loaded(result):
            if (not self.closing and not self.setup_loading and not self.start_request_pending and
                    self.run_ui.state not in ("running", "stopping") and
                    epoch == self.settings_load_epoch and prepared_epoch == self.prepared_epoch and
                    selection_epoch == self.setup_selection_epoch and inspection_id == self.cell_inspection_id):
                if self.eda_revalidation_required:
                    self._stash_eda_edit_draft()
                    if not self._same_eda_edit_draft(self.pending_settings_state, document):
                        self._restore_pending_ui_state()
                        return
                applied_document = result.pop("_configuration_settings", document)
                self._apply_ui_settings(applied_document, result)
                self._advance_eda_revalidation(result)
        def failed(error):
            self._finish_eda_revalidation(str(error))
            self._configuration_io_warning("匹配配置", error)
        configuration = copy.deepcopy(getattr(self, "pending_configuration_document", None))
        imported_settings = getattr(self, "pending_configuration_settings", None)
        use_configuration = bool(configuration and imported_settings and
            self._same_eda_edit_draft(document, imported_settings))
        def match():
            if not use_configuration:
                return match_settings_state(document, context)
            normalized = normalize_configuration(configuration, current_context=context)
            result = match_configuration(normalized["document"], context)
            result["_configuration_settings"] = normalized["settings"]
            return result
        self.tasks.submit(match, loaded, failed)

    def _apply_ui_settings(self, document, result, preserve_existing=False):
        application = result.get("apply") or {}
        if application.get("execution_selection") is not None:
            self.execution_selection_panel.restore(application["execution_selection"], notify=False)
        for warning in result.get("warnings") or []:
            self._activity_event(str(warning), "WARN")
        same_setup = (application.get("setup") or {}).get("matches")
        cells = application.get("selected_cell_scope_ids") or []
        if (result.get("selection_present", True) and self.cell_scopes and
                (cells or same_setup and not any(row.get("selected") for row in document.get("cells") or []))):
            self.selected_cell_scope_ids = set(cells)
            self._render_cell_scopes()
            self._update_cell_selection_state()
        parameters = application.get("parameter_configs") or {}
        for identifier, values in parameters.items():
            config = self.parameter_configs.get(identifier)
            if config is not None:
                config.update(values)
        if parameters:
            self._mark_range_dirty()
            self.render_parameter_tree()
        objectives = application.get("objectives") or []
        constraints = application.get("constraints") or []
        if preserve_existing:
            def merge_rows(existing, incoming):
                replacements = {row.get("output_key"): row for row in incoming}
                merged = []
                for row in existing:
                    old = self._raw_metric_settings(row)
                    merged.append(replacements.pop(old.get("output_key"), old))
                return merged + list(replacements.values())
            objectives = merge_rows(self.objective_rows, objectives)
            constraints = merge_rows(self.constraint_rows, constraints)
        if self.prepared_id and (objectives or constraints or document.get("objectives") or document.get("constraints") or same_setup):
            self._reset_metric_rows()
            for definition in objectives:
                self.add_objective(definition)
            for definition in constraints:
                self.add_constraint(definition)
            if not self.objective_rows and not document.get("objectives"):
                self.add_objective()
        optimization = application.get("optimization") or {}
        if getattr(self, "environment_test_state", None) not in ("starting", "queued", "running", "cancelling", "stopping"):
            for key, name, default in (("diagnostic_generations", "diagnostic_generations_var", "2"),
                                       ("diagnostic_total_candidates", "diagnostic_total_var", "6")):
                if hasattr(self, name):
                    getattr(self, name).set(optimization.get(key, default))
        if "command_scheduler" in optimization and hasattr(self, "command_scheduler_var"):
            self.command_scheduler_var.set(optimization["command_scheduler"])
        if "execution_commands" in optimization:
            self._restore_execution_commands(optimization["execution_commands"])
        if "simulator" in optimization and hasattr(self, "simulator_var"):
            if getattr(self, "_baseline_mode_active", False):
                self._cli_simulator_draft = optimization["simulator"]
            else:
                self.simulator_var.set(optimization["simulator"])
            self._render_simulator_profiles()
        for key, variable in getattr(self, "job_timeout_vars", {}).items():
            if "job_timeout_" + key in optimization:
                value = optimization["job_timeout_" + key]
                variable.set("" if value in (-1, "-1") else value)
            if "job_timeout_override_" + key in optimization:
                self.job_timeout_override_vars[key].set(optimization["job_timeout_override_" + key])
        if "execution_backend" in optimization and hasattr(self, "execution_backend_var"):
            self.execution_backend_var.set(
                optimization["execution_backend"] if optimization["execution_backend"] in ("private_netlist", "maestro_baseline") else "auto")
        for key, variable in (("generations", self.generations_var), ("candidate_timeout_seconds", self.timeout_var),
                              ("job_command", self.job_command_var), ("job_command_override", self.job_command_override_var),
                              ("job_max_jobs", self.job_max_var), ("job_max_override", self.job_max_override_var),
                              ("maestro_candidate_mode", self.maestro_candidate_mode_var), ("advanced_visible", self.advanced_visible)):
            if key in optimization and optimization[key] is not None:
                value = optimization[key]
                variable.set("" if key == "candidate_timeout_seconds" and value in (-1, "-1") else value)
        for key, name in (("data_root_override_enabled", "data_root_override_var"), ("data_root_override", "data_root_var")):
            if key in optimization and hasattr(self, name):
                getattr(self, name).set(optimization[key])
        # Unchecked fields inherit this inspection, rather than the values
        # displayed when an older draft was saved. Explicit overrides survive.
        self._render_job_policy(getattr(self, "original_job_policy", None))
        self._render_save_options(getattr(self, "original_save_options", None))
        if "run_name" in optimization:
            self.last_run_display_name = str(optimization["run_name"] or "")
        requested = optimization.get("algorithm", self.algorithm_var.get())
        self.last_single_algorithm = optimization.get("last_single_algorithm", self.last_single_algorithm)
        policy = resolve_algorithm_policy(requested, self._settings_objective_count(), coerce=True)
        self.algorithm_var.set(policy["algorithm"])
        self._refresh_algorithm_defaults(force=True)
        self.algorithm_parameter_manual = set(tuple(pair) for pair in optimization.get("algorithm_parameter_manual") or [])
        for algorithm, fields in (optimization.get("algorithm_parameters") or {}).items():
            for field, value in fields.items():
                variable = self.algorithm_parameter_vars.get((algorithm, field))
                if variable is not None and value is not None:
                    variable.set(str(value))
        self._metric_count_changed()
        self._toggle_advanced()
        if document.get("_restore_source"):
            result = dict(result, restored_from=document["_restore_source"])
        display_result = {key: value for key, value in result.items()
            if key not in ("document", "pending", "environment_overrides", "environment_draft")}
        self.settings_match_detail = json.dumps(display_result, indent=2, ensure_ascii=False)[-65536:]
        self.settings_status_var.set((result.get("summary") or "Matching settings loaded.") +
                                     (" " + result["storage_warning"] if result.get("storage_warning") else ""))
        if not getattr(self, "prepared_id", None):
            self.settings_status_var.set("配置草稿已恢复；选择模块并 Apply 后继续匹配参数。" +
                                         (" " + result["storage_warning"] if result.get("storage_warning") else ""))
        elif not self.cell_scope_dirty:
            self.pending_settings_state = None
            if (getattr(self, "eda_revalidation_required", False)
                    and self._same_setup_paths(self.setup_path_var.get().strip(), self.eda_edit_draft_path)):
                self.eda_revalidation_required = False
                self.eda_edit_draft_path = None
                self._metric_count_changed()
                self._setup_path_changed()
        self._set_status("配置已恢复；未匹配项已跳过。")

    def save_design_config(self):
        return self.save_configuration()

    def load_design_config(self):
        return self.load_configuration()

    def apply_design_config(self, document):
        if document.get("schema_version") != "crescendo.design_config.v1":
            raise ValueError("Unsupported design config; expected crescendo.design_config.v1.")
        reference = document.get("setup_locator") or document.get("ade_setup") or document.get("maestro") or {}
        if reference.get("path"):
            self._require_launch_setup(reference["path"])
        optimization = document.get("optimization") or {}
        for config in self.parameter_configs.values():
            config["selected"] = False
        missing = []
        for variable in optimization.get("variables") or []:
            parameter_id = variable.get("parameter_id")
            config = self.parameter_configs.get(parameter_id)
            if not config:
                missing.append(parameter_id)
                continue
            config["selected"] = True
            if config.get("value_type") == "integer":
                config["minimum"] = str(int(round(float(variable["minimum"]))))
                config["maximum"] = str(int(round(float(variable["maximum"]))))
                config["scale"] = "linear"
            else:
                config["minimum"] = format_physical(variable.get("minimum"), config.get("unit") or "")
                config["maximum"] = format_physical(variable.get("maximum"), config.get("unit") or "")
                config["scale"] = variable.get("scale") or "linear"
        self.render_parameter_tree()
        self._reset_metric_rows()
        for definition in optimization.get("objectives") or []:
            self.add_objective(definition)
        for definition in optimization.get("constraints") or []:
            self.add_constraint(definition)
        if not self.objective_rows:
            self.add_objective()
        run = optimization.get("run") or {}
        self._restore_job_settings(run)
        if run.get("generations") is not None:
            self.generations_var.set(str(run["generations"]))
        if "candidate_timeout_seconds" in run:
            value = run["candidate_timeout_seconds"]
            self.timeout_var.set("" if value in (None, "", -1, "-1") else str(value))
        requested = run.get("algorithm") or "differential_evolution"
        if len(self.objective_rows) >= 2 and requested != "nsga2":
            self.last_single_algorithm = requested
        policy = resolve_algorithm_policy(requested, max(1, len(self.objective_rows)), coerce=True)
        self.algorithm_var.set(policy["algorithm"])
        if len(self.objective_rows) == 1:
            self.last_single_algorithm = policy["algorithm"]
        self._refresh_algorithm_defaults(force=True)
        for field, value in (run.get("algorithm_parameters") or {}).items():
            variable = self.algorithm_parameter_vars.get((policy["algorithm"], field))
            if variable is not None and value is not None:
                variable.set(str(value))
                self.algorithm_parameter_manual.add((policy["algorithm"], field))
        self._metric_count_changed()
        return missing

    def start_run(self):
        if not self.prepared_id:
            self._set_error("Start Optimization: Load an ADE setup first.")
            messagebox.showerror("Start Optimization", "Load an ADE setup first.", parent=self.root)
            return
        try:
            self._require_prepared_selection()
            objectives = self._collect_objectives()
            constraints = self._collect_constraints()
            if not objectives:
                raise ValueError("Add at least one objective.")
            run_settings = self._build_run_settings()
        except Exception as error:
            self._set_error("Start Optimization: {}".format(error))
            messagebox.showerror("Start Optimization", str(error), parent=self.root)
            return
        self.start_button.configure(state="disabled")
        self.start_request_pending = True
        self.run_state_var.set("Validating selected ranges...")

        def ranges_valid(variables):
            display_name = self._request_run_display_name()
            if display_name is None:
                self.start_request_pending = False
                self.start_button.configure(state="normal")
                self.run_state_var.set("Start cancelled; a run name is required.")
                self._set_status("Optimization start cancelled")
                return
            spec = {
                "evaluator": "real", "variables": variables,
                "objectives": objectives, "constraints": constraints,
                "display_name": display_name,
            }
            spec.update(run_settings)

            raw_state, state_context = self._snapshot_configuration()
            state_config = copy.deepcopy(getattr(self.service, "config", {}))
            state_launch = copy.deepcopy(self._active_context())
            previous_configuration = snapshot_copy(getattr(self, "configuration_document", None))
            state_suite = copy.deepcopy(getattr(self, "active_private_suite", None))
            state_revisions = dict(getattr(self, "configuration_revisions", {}))
            state_suite_revisions = dict(getattr(self, "suite_configuration_revisions", {}))
            saved_revisions = []
            if state_suite:
                state_suite["revision"] = state_suite.get("revision") or state_suite.get("baseline_id")
                state_suite["source_path"] = state_suite.get("source_path") or getattr(self, "active_setup_path", None)
            def start():
                warnings = []
                def save_error(label, error, readback):
                    message = label + " save not confirmed: " + _compact_diagnostic(str(error), 180)
                    state = getattr(error, "configuration_commit_state", None)
                    if state:
                        try:
                            revision = readback()
                        except Exception as read_error:
                            revision = "readback failed: " + str(read_error)
                        message += "; commit state %s, observed revision %s" % (state, revision)
                    return message
                try:
                    document = build_configuration(raw_state, state_context["parameter_by_id"],
                        state_context["cell_scopes"], state_context["outputs"], previous=previous_configuration)
                    if state_suite and state_suite.get("id"):
                        document["private_suite"] = {"suite_id": state_suite["id"],
                            "revision": state_suite.get("revision"), "source_path": state_suite.get("source_path")}
                    last_path = os.path.abspath(last_settings_path(state_config, state_launch))
                    try:
                        revision = state_revisions[last_path] if last_path in state_revisions else configuration_revision(last_path)
                        outcome = save_last_configuration(document, state_config, state_launch,
                            expected_revision=revision, return_result=True)
                        saved_revisions.append(("file", last_path, outcome["revision"]))
                    except Exception as error:
                        warnings.append(save_error("Last settings", error, lambda: configuration_revision(last_path)))
                    if state_suite and state_suite.get("id"):
                        try:
                            suite_id = state_suite["id"]
                            revision = (state_suite_revisions[suite_id] if suite_id in state_suite_revisions else
                                        self.service.private_suite_configuration_revision(suite_id))
                            outcome = self.service.save_private_suite_configuration(suite_id, document, expected_revision=revision)
                            saved_revisions.append(("suite", suite_id, outcome["revision"]))
                        except Exception as error:
                            warnings.append(save_error("Suite configuration snapshot", error,
                                lambda: self.service.private_suite_configuration_revision(suite_id)))
                except (OSError, ValueError, TypeError) as error:
                    warnings.append("Configuration snapshot was not saved: " + _compact_diagnostic(str(error), 180))
                payload = self.service.start(self.prepared_id, spec)
                if warnings:
                    payload = dict(payload, settings_warning="; ".join(warnings))
                return payload

            def remember_saved():
                if not hasattr(self, "configuration_revisions"):
                    self.configuration_revisions = {}
                if not hasattr(self, "suite_configuration_revisions"):
                    self.suite_configuration_revisions = {}
                for kind, target, revision in saved_revisions:
                    (self.configuration_revisions if kind == "file" else self.suite_configuration_revisions)[target] = revision

            def started(payload):
                remember_saved()
                self.start_request_pending = False
                self.run_ui.started(payload["run_id"])
                self._cancel_results_load()
                self.display_dataset = None
                self.progress_revision = 0
                self._live_candidate_map = {}
                self.live_spec = spec
                self.live_candidates = []
                self.results_failure_var.set("")
                self._render_execution_console({"state": "queued", "run_root": payload.get("run_root"),
                                                "message": "Optimization registered; waiting for execution records."})
                self.selected_candidate_id = None
                self.result_page = 0
                self.pareto_ids = set()
                self.run_state_var.set("Running")
                self.start_button.configure(state="disabled")
                self.stop_button.configure(state="normal", text="Stop")
                self.export_csv_button.configure(state="normal")
                self.export_data_button.configure(state="normal")
                if payload.get("settings_warning"):
                    self.settings_status_var.set(payload["settings_warning"])
                self.show_page("Live Results")
                self._set_status("Optimization started: {}".format(display_name))
                self._poll_progress()

            def failed(error):
                remember_saved()
                self.start_request_pending = False
                self.start_button.configure(state="normal")
                issues = getattr(error, "issues", None)
                report = getattr(error, "report", None) or {}
                issues = issues or report.get("issues") or []
                if issues:
                    self._show_range_issues(issues)
                self.run_state_var.set("Start failed; see Details.")
                self._set_error("Start Optimization: {}".format(error))

            self.tasks.submit(start, started, failed)

        self._static_range_report(ranges_valid, button=self.start_button)

    def _request_run_display_name(self):
        while True:
            value = simpledialog.askstring(
                "Name Optimization Run",
                "Enter a clear name for this optimization run:",
                parent=self.root,
                initialvalue=getattr(self, "last_run_display_name", ""),
            )
            if value is None:
                return None
            try:
                self.last_run_display_name = validated_run_display_name(value)
                return self.last_run_display_name
            except ValueError as error:
                self._set_error("Run Name: {}".format(error))
                messagebox.showerror("Run Name", str(error), parent=self.root)

    def stop_run(self):
        if getattr(self, "setup_loading", False):
            method = getattr(self.service, "stop_setup_preparation", None)
            if callable(method):
                self.stop_button.configure(state="disabled", text="Stopping...")
                self._set_status("正在停止层级读取 / 私有化，等待原生进程安全结束；已建私有视图保留到退出。")
                self.tasks.submit_control(method, lambda _reply: None,
                    self._task_error("Stop Setup Preparation"))
            return
        if not self.run_ui.request_stop():
            return
        run_id = self.run_ui.run_id
        self.stop_button.configure(state="disabled", text="Stopping...")
        self.start_button.configure(state="disabled")
        self.run_state_var.set("Stop requested · stopping safely...")
        self._set_status("Stop requested; the active candidate may take time to terminate safely")

        def stop():
            return self.service.stop(run_id)

        def stopped(_payload):
            self.run_state_var.set("Stop requested · waiting for backend confirmation...")
            self._poll_progress()

        def failed(error):
            text = str(error)
            if "not active" in text:
                self.run_state_var.set("Run is no longer active; refreshing final state...")
                self._poll_progress()
                return
            self.run_ui.stop_failed()
            self.stop_button.configure(state="normal", text="Stop")
            self.run_state_var.set("Stop request failed: {}".format(error))
            self._set_error("Stop Optimization: {}".format(error))

        self.tasks.submit_control(stop, stopped, failed)

    def _poll_progress(self):
        if getattr(self, "closing", False) or getattr(self.tasks, "shutting_down", False):
            return
        if not self.run_ui.run_id or self.progress_inflight:
            return
        run_id = self.run_ui.run_id
        selection_epoch = getattr(self, "setup_selection_epoch", 0)
        self.progress_inflight = True
        request = object()
        self._progress_request = request

        def release():
            if getattr(self, "_progress_request", None) is not request:
                return False
            self._progress_request = None
            self.progress_inflight = False
            if (selection_epoch != getattr(self, "setup_selection_epoch", 0) or
                    run_id != self.run_ui.run_id):
                self.root.after(0, self._poll_progress)
                return False
            return True

        revision = getattr(self, "progress_revision", 0)
        def load():
            delta_method = getattr(self.service, "progress_delta", None)
            progress = delta_method(run_id, revision) if delta_method else self.service.progress(run_id)
            presentation = progress.get("objective_presentation")
            spec = progress.get("spec") or self.live_spec
            candidates = progress.get("candidates") or []
            if not presentation and "candidates" in progress and (spec.get("objectives") or []):
                try:
                    presentation = build_objective_presentation(
                        candidates, spec.get("objectives") or [],
                    )
                except (KeyError, TypeError, ValueError):
                    presentation = None
                progress["objective_presentation"] = presentation
            return progress

        def loaded(progress):
            if not release():
                return
            status = progress.get("status") or {}
            self.live_spec = progress.get("spec") or self.live_spec
            changed = bool("revision" not in progress or progress.get("revision") != getattr(self, "progress_revision", 0))
            if "candidates" in progress:
                self.live_candidates = progress.get("candidates") or []
                self._live_candidate_map = dict((row["candidate_id"], row) for row in self.live_candidates)
            elif changed:
                mapping = getattr(self, "_live_candidate_map", {})
                for row in progress.get("changed_candidates") or []:
                    mapping[row["candidate_id"]] = row
                self._live_candidate_map = mapping
                self.live_candidates = sorted(mapping.values(), key=lambda row: (row.get("generation", -1), row.get("index", -1), row["candidate_id"]))
            self.progress_revision = progress.get("revision", 0)
            self.run_ui.update(status.get("state"))
            presentation = progress.get("objective_presentation")
            self.objective_presentation = presentation or {}
            self.pareto_ids = set(self.objective_presentation.get("pareto_candidate_ids") or [])
            self._render_progress(status)
            self._render_execution_console(status, progress.get("execution"))
            self._render_run_failure(status, progress.get("execution"))
            if getattr(self, "display_dataset", None) is None and changed:
                if self.current_page == "Live Results":
                    self.render_results()
                elif self.current_page == "Pareto Trade-offs":
                    self.render_pareto()
            state = status.get("state")
            recent_signature = (
                self.run_ui.run_id, status.get("generation"),
                status.get("completed_candidates"), status.get("failed_candidates"),
                status.get("infrastructure_failed_candidates"), state,
            )
            if (
                    self.current_page == "Optimization"
                    and recent_signature != self.recent_progress_signature):
                self.recent_progress_signature = recent_signature
                self.refresh_recent_runs()
            if state in ("completed", "failed", "stopped"):
                self.start_button.configure(state="normal" if self.prepared_id and
                                            not getattr(self, "eda_revalidation_required", False) else "disabled")
                self.stop_button.configure(state="disabled", text="Stop")
                self.refresh_recent_runs()
                display_name = status.get("display_name") or self.live_spec.get("display_name") or "Optimization run"
                if state != "failed":
                    self._set_status("{}: {}".format(display_name, state))
                return
            self.root.after(1200, self._poll_progress)

        def failed(error):
            if not release():
                return
            self.run_state_var.set("Progress unavailable: {} · use Recent Local Runs to recover.".format(error))
            self._set_error("Progress unavailable: {}".format(error))
            self.root.after(2000, self._poll_progress)

        submit = (self.tasks.submit_monitor if callable(getattr(type(self.tasks), "submit_monitor", None))
                  else self.tasks.submit)
        submit(load, loaded, failed)

    def _render_progress(self, status):
        state = status.get("state") or self.run_ui.state
        token = "optimization"
        journal = getattr(self, "activity_journal", None)
        active = (journal.active.get(token) if journal else None) or {}
        run_id = self.run_ui.run_id
        if active.get("run_id") not in (None, run_id):
            self._activity_end(token)
        if state in ("completed", "failed", "stopped", "idle"):
            self._activity_finish(token)
        else:
            self._activity_begin(token, status.get("message") or "Optimization: " + state, priority=10)
            if getattr(self, "activity_journal", None) and token in self.activity_journal.active:
                self.activity_journal.active[token]["run_id"] = run_id
        self._activity_event("Optimization: " + state + " · " + str(status.get("message") or ""), "ERROR" if state == "failed" else "INFO", key="optimization-state")
        state = status.get("state") or self.run_ui.state
        message = status.get("message") or ""
        if self.run_ui.stop_requested and state not in ("completed", "failed", "stopped"):
            self.run_state_var.set("Stop requested · stopping safely... · {}".format(message))
        else:
            self.run_state_var.set("{} · {}".format(state, message))
        completed = int(status.get("completed_candidates") or 0)
        failed = int(status.get("failed_candidates") or 0)
        infrastructure = int(status.get("infrastructure_failed_candidates") or 0)
        total = int(status.get("total_candidates") or 1)
        done = completed + failed + infrastructure
        feasible = sum(
            1 for row in self.live_candidates
            if row.get("status") == "completed" and (row.get("metrics") or {}).get("feasible") is True
        )
        display_name = status.get("display_name") or self.live_spec.get("display_name") or "Unnamed run"
        self.progress_summary_var.set(
            "Completed {} · Failed {} · Infrastructure replaced {} · Feasible {} · Run {}".format(
                completed, failed, infrastructure, feasible, display_name,
            )
        )
        if getattr(self, "display_dataset", None) is not None:
            return
        self.results_summary_var.set(
            "{} candidates · {} feasible · {} Pareto".format(
                len(self.live_candidates), feasible, len(self.pareto_ids),
            )
        )

    def _render_execution_console(self, status, execution=None):
        execution = execution or {}
        stage = execution.get("stage")
        if stage:
            message = "Optimization: {} · {}".format(stage, execution.get("message") or status.get("message") or "")
            self._activity_event(message, key="optimization-stage")
            self._activity_update("optimization", label=message, gates=execution.get("gate_progress"))
        for warning in execution.get("warnings") or []:
            self._activity_event(warning, "WARN", key="execution-warning:" + str(warning)[:240])
        for command in execution.get("commands") or []:
            kind, state = command.get("kind"), command.get("state")
            if state == "configured":
                continue
            identity = command.get("invocation_id") or str((kind, command.get("candidate_id"), command.get("test"), command.get("corner"), command.get("source")))
            message = "{}: {}".format(command.get("label") or kind or "Execution", state or command.get("stage") or "observed")
            if command.get("job_ids"):
                message += " · Job " + ", ".join(str(value) for value in command["job_ids"])
            if command.get("returncode") is not None:
                message += " · return " + str(command["returncode"])
            self._activity_event(message, "ERROR" if command.get("returncode") not in (None, 0) else "INFO", key="execution-command:" + identity)
        widget = getattr(self, "execution_console", None)
        if widget is None:
            return
        text = execution_progress_text(status, execution)
        if text == getattr(self, "execution_console_signature", None):
            return
        self.execution_console_signature = text
        position = widget.yview()
        at_end = not position or position[1] >= 0.99
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", text)
        widget.configure(state="disabled")
        if at_end:
            widget.see("end")
        elif position:
            widget.yview_moveto(position[0])

    def _render_run_failure(self, status, execution=None):
        execution = execution or {}
        if getattr(self, "display_dataset", None) is not None:
            return
        if status.get("state") != "failed":
            variable = getattr(self, "results_failure_var", None)
            if variable is not None:
                variable.set("")
            return
        error = execution.get("error") or {}
        reason = str(status.get("message") or error.get("message") or "No failure reason was recorded.")
        startup = execution.get("startup_diagnostic") or {}
        native_error = startup.get("native_error") or error.get("startup_cause")
        if native_error and native_error not in reason:
            reason += "\nNative startup error: {}".format(native_error)
        run_root = execution.get("run_root") or status.get("run_root")
        path = execution.get("error_file") or error.get("path")
        if not path and run_root:
            path = os.path.join(run_root, "server_error.json")
        detail = "Optimization failed: {}".format(reason)
        if path:
            detail += "\nFailure log: {}".format(path)
        diagnostic_path = execution.get("startup_error_file") or error.get("startup_diagnostic_path")
        if diagnostic_path:
            detail += "\nStartup diagnostics: {}".format(diagnostic_path)
        detail += "\nOpen Settings / Crescendo / Diagnostics for execution details."
        variable = getattr(self, "results_failure_var", None)
        if variable is not None:
            variable.set(detail)
        self._set_error(detail)
        notified = getattr(self, "notified_run_failures", None)
        if notified is None:
            notified = self.notified_run_failures = set()
        signature = status.get("run_id") or self.run_ui.run_id
        if signature not in notified and not getattr(self, "closing", False):
            notified.add(signature)
            messagebox.showerror("Optimization Failed", detail, parent=self.root)

    def save_checkpoint(self):
        if not self.run_ui.run_id:
            return
        path = filedialog.asksaveasfilename(
            parent=self.root, title="Save Optimization Checkpoint",
            defaultextension=".json", initialfile="{}-checkpoint.json".format(self.run_ui.run_id),
            filetypes=(("JSON", "*.json"), ("All files", "*")),
        )
        if not path:
            return
        run_id = self.run_ui.run_id

        def export():
            checkpoint = self.service.export_checkpoint(run_id)
            if checkpoint is None:
                raise ValueError("No checkpoint is available yet.")
            _write_json_file(path, checkpoint)
            return path

        self.tasks.submit(
            export, lambda saved: self._set_status("Checkpoint saved: {}".format(saved)),
            self._task_error("Save Checkpoint"),
        )

    def load_checkpoint(self):
        path = filedialog.askopenfilename(
            parent=self.root, title="Resume Optimization Checkpoint",
            filetypes=(("JSON", "*.json"), ("All files", "*")),
        )
        if not path:
            return
        try:
            checkpoint = _read_json_file(path)
        except Exception as error:
            self._set_error("Load Checkpoint: {}".format(error))
            messagebox.showerror("Load Checkpoint", str(error), parent=self.root)
            return
        self._resume_checkpoint(checkpoint)

    def _checkpoint_design_config(self, checkpoint):
        spec = checkpoint.get("spec") or {}
        manifest = checkpoint.get("manifest") or {}
        setup = manifest.get("setup_locator") or {
            "path": manifest.get("maestro_path") or "", "backend": "maestro",
        }
        return {
            "schema_version": "crescendo.design_config.v1", "ade_setup": setup,
            "optimization": {
                "variables": spec.get("variables") or [],
                "objectives": spec.get("objectives") or [],
                "constraints": spec.get("constraints") or [],
                "run": {
                    "generations": spec.get("generations"), "jobs": 1,
                    "candidate_timeout_seconds": spec.get("candidate_timeout_seconds"),
                    "algorithm": spec.get("algorithm"),
                    "execution_backend": spec.get("execution_backend") or "auto",
                    "simulator": spec.get("simulator") or "spectre",
                    "execution_commands": copy.deepcopy(spec.get("execution_commands") or {}),
                    "command_scheduler": spec.get("command_scheduler") or "auto",
                    "simulator_model_profile_identity": copy.deepcopy(spec.get("simulator_model_profile_identity")),
                    "maestro_candidate_mode": spec.get("maestro_candidate_mode") or "auto",
                    "maestro_job_overrides": spec.get("maestro_job_overrides") or {},
                    "algorithm_parameters": spec.get("algorithm_parameters") or {
                        "population": spec.get("population"), "seed": spec.get("seed"),
                    },
                },
            },
        }

    def _resume_checkpoint(self, checkpoint):
        reason = self._setup_reload_block_reason()
        if reason:
            self._set_error(reason)
            return
        manifest = checkpoint.get("manifest") or {}
        reference = manifest.get("setup_locator") or {}
        path = reference.get("path") or manifest.get("maestro_path")
        try:
            if path:
                self._require_launch_setup(path)
        except Exception as error:
            self._set_error("Resume Checkpoint: {}".format(error))
            return
        self.pending_checkpoint = checkpoint
        self.setup_loading = True
        self.run_state_var.set("Restoring setup and optimization state...")
        self.start_button.configure(state="disabled")

        def resume():
            payload = self.service.resume(checkpoint)
            self._cache_prepared_defaults((payload["prepared_id"], payload["prepared"]))
            return payload

        def resumed(payload):
            self.setup_loading = False
            self._set_prepared(payload["prepared_id"], payload["prepared"])
            self.apply_design_config(self._checkpoint_design_config(checkpoint))
            self.pending_checkpoint = None
            self.run_ui.started(payload["run_id"])
            self._cancel_results_load()
            self.display_dataset = None
            self.progress_revision = 0
            self._live_candidate_map = {}
            self.live_spec = checkpoint.get("spec") or {}
            self.live_candidates = []
            self.stop_button.configure(state="normal", text="Stop")
            self.export_csv_button.configure(state="normal")
            self.run_state_var.set("Checkpoint restored; optimization is continuing.")
            self.show_page("Live Results")
            self._poll_progress()

        def failed(error):
            self.setup_loading = False
            self.start_button.configure(state="normal" if self.prepared_id and
                                        not getattr(self, "eda_revalidation_required", False) else "disabled")
            self.run_state_var.set("Checkpoint resume failed: {}".format(error))
            self._set_error("Resume Checkpoint: {}".format(error))

        self.tasks.submit(resume, resumed, failed)

    def refresh_recent_runs(self):
        if getattr(self, "closing", False) or not hasattr(self, "recent_tree"):
            return
        selection_epoch = getattr(self, "setup_selection_epoch", 0)

        def loaded(rows):
            if selection_epoch != getattr(self, "setup_selection_epoch", 0):
                return
            self.recent_tree.delete(*self.recent_tree.get_children(""))
            self.recent_run_resumable = {}
            seen_continuations = set()
            ordinary_rows = [row for row in (rows or [])
                             if not (row.get("environment_test") or
                                     (row.get("spec") or {}).get("environment_test") or
                                     (row.get("status") or {}).get("environment_test"))]
            for row in ordinary_rows[:100]:
                run_id = str(row.get("run_id") or row.get("id") or "")
                checkpoint_available = bool(row.get("checkpoint_available"))
                continuation_id = str(row.get("continuation_id") or "")
                if checkpoint_available and continuation_id:
                    if continuation_id in seen_continuations:
                        continue
                    seen_continuations.add(continuation_id)
                display_name = str(row.get("display_name") or "").strip()
                if not display_name or display_name == run_id:
                    started = str(row.get("started_at") or "").replace("T", " ")[:16]
                    display_name = "Unnamed run{}".format(" · " + started if started else "")
                state = row.get("state") or (row.get("status") or {}).get("state") or "unknown"
                current_generation = int(row.get("current_generation") or row.get("generation") or 0)
                total_generations = int(row.get("total_generations") or 0)
                generation = (
                    "{} / {}".format(current_generation, total_generations)
                    if total_generations else str(current_generation)
                )
                successful = int(row.get("successful_candidates") or row.get("success") or 0)
                simulation_failed = int(row.get("simulation_failed_candidates") or row.get("simulation_fail") or 0)
                infrastructure_failed = int(row.get("infrastructure_failed_candidates") or row.get("infrastructure_fail") or 0)
                failures = simulation_failed + infrastructure_failed
                failure_label = "{} (sim {}, infra {})".format(
                    failures, simulation_failed, infrastructure_failed,
                )
                started = str(row.get("started_at") or "").replace("T", " ")[:16]
                self.recent_run_resumable[run_id] = checkpoint_available
                self.recent_tree.insert(
                    "", "end", iid=run_id, text=display_name,
                    values=(
                        generation, successful, failure_label, state, started,
                        "Yes" if checkpoint_available else "No",
                        "Yes" if row.get("active") else "No",
                    ),
                )

        self.tasks.submit(self.service.list_runs, loaded, lambda error: self._set_error("Recent runs: {}".format(error)))

    def _selected_recent_run(self):
        selected = self.recent_tree.selection()
        return selected[0] if selected else None

    def view_recent_run(self):
        run_id = self._selected_recent_run()
        if not run_id:
            messagebox.showinfo("Recent Runs", "Select a run first.", parent=self.root)
            return
        if self.run_ui.run_id == run_id:
            self._cancel_results_load()
            self.display_dataset = None
            self.show_page("Optimization")
            self._poll_progress()
            return
        if (self.run_ui.run_id and self.run_ui.run_id != run_id
                and self.run_ui.state in ("queued", "running", "stopping")):
            self._set_error("Stop the active run before selecting another run's progress.")
            return
        self.progress_inflight = False
        self._cancel_results_load()
        self.run_ui.started(run_id)
        self.live_spec = {"objectives": [], "constraints": []}
        self.live_candidates = []
        self.display_dataset = None
        self.progress_revision = 0
        self._live_candidate_map = {}
        self.selected_candidate_id = None
        self.result_page = 0
        self.pareto_ids = set()
        self.export_csv_button.configure(state="normal")
        self.run_state_var.set("Reading selected run diagnostics...")
        self.show_page("Optimization")
        self._poll_progress()

    def resume_recent_run(self):
        run_id = self._selected_recent_run()
        if not run_id:
            messagebox.showinfo("Recent Runs", "Select a run first.", parent=self.root)
            return
        if not self.recent_run_resumable.get(run_id):
            messagebox.showinfo(
                "Recent Runs",
                "This run has no valid automatic checkpoint and cannot be resumed.",
                parent=self.root,
            )
            return

        def load():
            checkpoint = self.service.export_checkpoint(run_id)
            if checkpoint is None:
                raise ValueError("Selected run has no checkpoint.")
            return checkpoint

        self.tasks.submit(load, self._resume_checkpoint, self._task_error("Resume Recent Run"))

    def save_recent_checkpoint(self):
        run_id = self._selected_recent_run()
        if not run_id:
            messagebox.showinfo("Recent Runs", "Select a run first.", parent=self.root)
            return
        path = filedialog.asksaveasfilename(
            parent=self.root, title="Save Recent Checkpoint",
            defaultextension=".json", initialfile="{}-checkpoint.json".format(run_id),
            filetypes=(("JSON", "*.json"),),
        )
        if not path:
            return

        def export():
            checkpoint = self.service.export_checkpoint(run_id)
            if checkpoint is None:
                raise ValueError("Selected run has no checkpoint.")
            _write_json_file(path, checkpoint)
            return path

        self.tasks.submit(export, lambda saved: self._set_status("Checkpoint saved: {}".format(saved)), self._task_error("Save Recent Checkpoint"))

    def _display_candidates(self):
        dataset = getattr(self, "display_dataset", None)
        return dataset.get("candidates") or [] if dataset is not None else self.live_candidates

    def _display_spec(self):
        dataset = getattr(self, "display_dataset", None)
        return (dataset.get("metadata") or {}).get("spec") or {} if dataset is not None else self.live_spec

    def _display_parameters(self):
        dataset = getattr(self, "display_dataset", None)
        if dataset is None:
            return self.parameter_by_id
        rows = (dataset.get("metadata") or {}).get("parameters") or []
        if isinstance(rows, dict):
            return rows
        return dict((row["parameter_id"], row) for row in rows
                    if isinstance(row, dict) and row.get("parameter_id"))

    def _cancel_results_load(self):
        # A delayed read-only import must never replace a newer run display.
        self.dataset_load_epoch += 1
        self.load_data_button.configure(state="normal")

    def show_current_results(self):
        self._cancel_results_load()
        self.display_dataset = None
        self.selected_candidate_id = None
        self.result_page = 0
        self.results_failure_var.set("")
        self.render_results()
        self.render_pareto()
        self._set_status("Showing the current run's results.")

    def load_results_data(self):
        if self.closing:
            return
        path = filedialog.askopenfilename(parent=self.root, title="Load Optimization Results",
                                         filetypes=(("Crescendo Results", "*.crescendo"), ("All files", "*")))
        if not path:
            return
        self.dataset_load_epoch += 1
        epoch = self.dataset_load_epoch
        self.load_data_button.configure(state="disabled")
        def load():
            from .run_data_store import load_dataset
            dataset = load_dataset(path)
            spec = (dataset.get("metadata") or {}).get("spec") or {}
            dataset["objective_presentation"] = (build_objective_presentation(
                dataset.get("candidates") or [], spec.get("objectives") or []) if spec.get("objectives") else {})
            dataset["source_path"] = os.path.abspath(path)
            return dataset
        def loaded(dataset):
            if self.closing or epoch != self.dataset_load_epoch:
                return
            self.load_data_button.configure(state="normal")
            self.display_dataset = dataset
            self.selected_candidate_id = None
            self.result_page = 0
            self.export_csv_button.configure(state="normal")
            self.export_data_button.configure(state="normal")
            self.results_failure_var.set("Loaded results for viewing; the current optimization remains independent.")
            self.render_results()
            self.render_pareto()
            self.show_page("Live Results")
            self._set_status("Loaded {} candidate(s): {}".format(len(dataset.get("candidates") or []), os.path.basename(path)))
        def failed(error):
            if self.closing or epoch != self.dataset_load_epoch:
                return
            self.load_data_button.configure(state="normal")
            self._task_error("Load Results")(error)
        getattr(self.tasks, "submit_history", self.tasks.submit)(load, loaded, failed)

    def export_results_data(self):
        dataset = getattr(self, "display_dataset", None)
        run_id = self.run_ui.run_id
        if dataset is None and not run_id:
            return
        name = ((dataset or {}).get("metadata") or {}).get("display_name") or run_id or "Optimization"
        path = filedialog.asksaveasfilename(parent=self.root, title="Export Complete Optimization Results",
            defaultextension=".crescendo", initialfile=str(name) + ".crescendo", filetypes=(("Crescendo Results", "*.crescendo"),))
        if not path:
            return
        source = dataset.get("source_path") if dataset is not None else None
        def export():
            if source:
                from .run_data_store import export_dataset
                export_dataset(source, path)
            else:
                self.service.export_results(run_id, path)
            return path
        getattr(self.tasks, "submit_history", self.tasks.submit)(export, lambda saved: self._set_status("Results exported: {}".format(saved)), self._task_error("Export Results"))

    def export_csv(self):
        dataset = getattr(self, "display_dataset", None)
        run_id = self.run_ui.run_id
        if dataset is None and not run_id:
            return
        path = filedialog.asksaveasfilename(
            parent=self.root, title="Export Candidate Results",
            defaultextension=".csv", initialfile="{}-candidates.csv".format(run_id or "Loaded-Results"),
            filetypes=(("CSV", "*.csv"), ("All files", "*")),
        )
        if not path:
            return
        rows = list(self._display_candidates()) if dataset is not None else None
        parameters = dict(self._display_parameters())
        def export():
            if rows is not None:
                from .exports import candidates_csv_text
                text = candidates_csv_text(rows, dict((key, parameter_label(value)) for key, value in parameters.items()))
            else:
                text = self.service.candidates_csv(run_id)
            _write_text_file(path, text)
            return path
        getattr(self.tasks, "submit_history", self.tasks.submit)(export, lambda saved: self._set_status("CSV exported: {}".format(saved)), self._task_error("Export CSV"))

    def change_result_page(self, delta):
        pages = max(1, int((len(self._display_candidates()) + self.result_page_size - 1) / self.result_page_size))
        self.result_page = max(0, min(pages - 1, self.result_page + delta))
        self.render_results()

    def _render_result_trend(self):
        canvas = getattr(self, "result_trend", None)
        if canvas is None:
            return
        canvas = CachedCanvas(canvas)
        canvas.delete("all")
        series = []
        for index, definition in enumerate(self._display_spec().get("objectives") or []):
            points = []
            for row in self._display_candidates():
                value = metric_value(row, index)
                if value is not None:
                    points.append((int(row.get("generation") or 0) + 1, value))
            if points:
                series.append((definition, points))
        if not series:
            canvas.create_text(12, 12, anchor="nw", text="No measured objective values yet.", fill=MUTED)
            canvas.finish()
            return
        width = max(420, canvas.winfo_width() or 780)
        canvas.configure(height=max(190, len(series) * 150))
        for index, (definition, points) in enumerate(series):
            top, bottom, left, right = index * 150 + 32, index * 150 + 125, 70, width - 25
            generations, values = [p[0] for p in points], [p[1] for p in points]
            gmin, gmax, vmin, vmax = min(generations), max(generations), min(values), max(values)
            scale = vmax - vmin or max(abs(vmax), 1.0)
            lo, hi = vmin - scale * .1, vmax + scale * .1
            def sx(g):
                return left + (g - gmin) * (right - left) / max(1, gmax - gmin)
            def sy(v):
                return bottom - (v - lo) * (bottom - top) / (hi - lo)
            canvas.create_text(left, top - 20, anchor="w", text=metric_definition_label(definition), fill=TEXT)
            canvas.create_text(left - 5, top, anchor="e", text="{:.4g}".format(vmax), fill=MUTED)
            canvas.create_text(left - 5, bottom, anchor="e", text="{:.4g}".format(vmin), fill=MUTED)
            canvas.create_line(left, bottom, right, bottom, fill=TEXT)
            means = []
            groups = {}
            for generation, value in points:
                groups.setdefault(generation, []).append(value)
            for generation in sorted(groups):
                group = groups[generation]
                x = sx(generation)
                canvas.create_text(x, bottom + 12, text=str(generation), fill=MUTED)
                means.extend((x, sy(sum(group) / len(group))))
                for value in group:
                    y = sy(value)
                    canvas.create_oval(x - 3, y - 3, x + 3, y + 3, fill=PALETTE[index % len(PALETTE)])
            if len(means) >= 4:
                canvas.create_line(*means, fill=PALETTE[index % len(PALETTE)], width=2)
        canvas.finish()

    def render_results(self):
        self._render_result_trend()
        existing = self.result_tree.get_children("")
        row_state = getattr(self, "_result_row_state", {})
        self._result_row_state = row_state
        count = len(self._display_candidates())
        dataset = getattr(self, "display_dataset", None)
        if dataset is not None:
            feasible = sum(1 for row in self._display_candidates() if (row.get("metrics") or {}).get("feasible") is True)
            self.results_summary_var.set("Loaded: {} candidates | {} feasible".format(count, feasible))
        pages = max(1, int((count + self.result_page_size - 1) / self.result_page_size))
        self.result_page = min(self.result_page, pages - 1)
        start = self.result_page * self.result_page_size
        rows = self._display_candidates()[start:start + self.result_page_size]
        self.result_page_var.set("Page {} / {}".format(self.result_page + 1, pages))
        for row in rows:
            candidate_id = str(row.get("candidate_id") or "")
            state = row.get("status") or "running"
            terminal = state in ("completed", "failed", "infrastructure_failed", "stopped")
            metrics = row.get("metrics") or {}
            if state == "infrastructure_failed":
                feasible = "N/A"
            elif terminal:
                feasible = "Yes" if metrics.get("feasible") is True else "No"
            else:
                feasible = "Pending"
            values = (int(row.get("generation") or 0) + 1,
                      candidate_status(row), feasible,
                      compact_metric_values(row, "objectives"), compact_metric_values(row, "constraints"))
            if candidate_id not in existing:
                self.result_tree.insert("", "end", iid=candidate_id, text=candidate_id, values=values)
            elif row_state.get(candidate_id) != values:
                self.result_tree.item(candidate_id, text=candidate_id, values=values)
            row_state[candidate_id] = values
        desired = tuple(str(row.get("candidate_id") or "") for row in rows)
        removed = set(existing) - set(desired)
        if removed:
            self.result_tree.delete(*removed)
            for item in removed:
                row_state.pop(item, None)
        if existing != desired:
            self.result_tree.set_children("", *desired)
        if self.selected_candidate_id in self.result_tree.get_children(""):
            self.result_tree.selection_set(self.selected_candidate_id)
        self._render_candidate_detail()

    def _result_selected(self, _event=None):
        selected = self.result_tree.selection()
        if selected:
            self.select_candidate(selected[0], navigate=False)

    def select_candidate(self, candidate_id, navigate=True):
        self.selected_candidate_id = candidate_id
        if navigate:
            for index, row in enumerate(self._display_candidates()):
                if row.get("candidate_id") == candidate_id:
                    self.result_page = int(index / self.result_page_size)
                    break
            self.show_page("Live Results")
        self._render_candidate_detail()

    def _render_candidate_detail(self):
        row = next((item for item in self._display_candidates() if item.get("candidate_id") == self.selected_candidate_id), None)
        if hasattr(self, "optimized_schematic_button"):
            enabled = bool(getattr(self, "display_dataset", None) is None and row and row.get("status") == "completed" and
                           self.run_ui.state in ("completed", "stopped", "failed") and
                           (self._display_spec() or {}).get("evaluator", "real") != "mock" and
                           not getattr(self, "optimized_schematic_inflight", False))
            self.optimized_schematic_button.configure(state="normal" if enabled else "disabled")
        lines = []
        if row:
            lines.append("Candidate: {}".format(row.get("candidate_id")))
            lines.append("Status: {}".format(candidate_status(row)))
            if row.get("status") in ("failed", "infrastructure_failed") or row.get("error"):
                lines.append(
                    "Failure stage: {}".format(candidate_failure_stage_label(row))
                )
                spectre_label = {
                    "not_started": "Not started",
                    "started": "Started",
                    "unknown": "Unknown",
                }.get(candidate_spectre_state(row), "Unknown")
                simulator = row.get("execution_simulator") or (self._display_spec() or {}).get("simulator") or "spectre"
                lines.append("{}: {}".format("ALPS" if simulator == "alps" else "Spectre", spectre_label))
            if row.get("error"):
                lines.append("Error: {}".format(
                    _compact_diagnostic(row.get("error"), 1200)
                ))
            lines.append("")
            requested_values = row.get("values") or {}
            display_values = dict(requested_values)
            completion = row.get("completion_evidence") or row.get("completion") or {}
            if row.get("execution_backend") in ("private_netlist", "maestro_netlist") and completion.get("ok"):
                display_values.update(completion.get("parameters") or {})
            application = row.get("parameter_application") or {}
            if application.get("ok"):
                from .hierarchy import parse_physical_number
                for evidence in application.get("parameters") or []:
                    numeric = parse_physical_number(evidence.get("persisted_schematic_value"))
                    if numeric is not None and finite_number(numeric) is not None:
                        display_values[evidence["parameter_id"]] = numeric
            display_parameters = self._display_parameters()
            for parameter_id, value in sorted(display_values.items()):
                parameter = display_parameters.get(parameter_id, {"parameter_id": parameter_id, "name": parameter_id})
                config = self.parameter_configs.get(parameter_id) or {} if getattr(self, "display_dataset", None) is None else {}
                unit = config.get("unit") or ""
                numeric = finite_number(value)
                if numeric is None:
                    display = str(value)
                elif unit:
                    display = "{} {}".format(format_physical(numeric, unit), unit).strip()
                else:
                    display = "{:.8g}".format(numeric)
                lines.append("{}\n    {}".format(parameter_label(parameter), display))
                requested = requested_values.get(parameter_id)
                if numeric is not None and finite_number(requested) is not None and abs(numeric - float(requested)) > max(abs(numeric), abs(float(requested)), 1e-300) * 1e-9:
                    lines.append("    actual saved value; requested {:.8g}".format(float(requested)))
        else:
            lines.append("Select a candidate to inspect its variables.")
        self.candidate_detail.configure(state="normal")
        self.candidate_detail.delete("1.0", "end")
        self.candidate_detail.insert("end", "\n".join(lines))
        self.candidate_detail.configure(state="disabled")

    def generate_optimized_schematic(self):
        if getattr(self, "display_dataset", None) is not None:
            self._set_error("Loaded results cannot authorize native schematic export; select the original local run.")
            return
        run_id, candidate_id = self.run_ui.run_id, self.selected_candidate_id
        if not run_id or not candidate_id or getattr(self, "optimized_schematic_inflight", False):
            return
        self.optimized_schematic_inflight = True
        self.optimized_schematic_button.configure(state="disabled")
        self.optimized_schematic_status_var.set("Generating schematic views with the selected solution's actual values...")

        def generated(payload):
            self.optimized_schematic_inflight = False
            views = payload.get("views") or payload.get("groups") or []
            labels = ["{}/{}/{}".format(row.get("library"), row.get("cell"), row.get("view") or row.get("target_view")) for row in views]
            message = "Created: " + (", ".join(labels) if labels else str(payload.get("message") or "optimized schematic views"))
            self.optimized_schematic_status_var.set(message)
            self._set_status(message)
            self._render_candidate_detail()

        def failed(error):
            self.optimized_schematic_inflight = False
            self.optimized_schematic_status_var.set("Schematic export failed: " + _compact_diagnostic(str(error), 200))
            self._render_candidate_detail()
            self._task_error("Generate Optimized Schematic")(error)

        getattr(self.tasks, "submit_export", self.tasks.submit)(
            lambda: self.service.generate_optimized_schematic(run_id, candidate_id), generated, failed,
        )

    def _result_context_menu(self, event):
        item = self.result_tree.identify_row(event.y)
        if not item:
            return
        self.result_tree.selection_set(item)
        self.select_candidate(item, navigate=False)
        menu = tk.Menu(self.root, tearoff=False)
        menu.add_command(label="Open Candidate Diagnostics", command=lambda: self.open_candidate_logs(item))
        menu.add_command(label="Inspect Variables", command=lambda: self.select_candidate(item))
        menu.tk_popup(event.x_root, event.y_root)

    def open_candidate_logs(self, candidate_id):
        if getattr(self, "display_dataset", None) is not None:
            self._set_status("Loaded result diagnostics are included in the candidate details.")
            return
        if not self.run_ui.run_id:
            return
        if not hasattr(self.service, "candidate_logs") or not hasattr(self.service, "read_candidate_log_chunk"):
            self._set_error("Candidate Diagnostics: This optimizer service does not expose candidate diagnostics.")
            messagebox.showerror(
                "Candidate Diagnostics",
                "This optimizer service does not expose candidate diagnostics.",
                parent=self.root,
            )
            return
        for existing in self.log_viewers:
            if not existing.closed and existing.run_id == self.run_ui.run_id and existing.candidate_id == candidate_id:
                existing.deiconify()
                existing.lift()
                return
        viewer = LiveLogViewer(self, self.run_ui.run_id, candidate_id)
        self.log_viewers.append(viewer)

    def render_pareto(self):
        if not hasattr(self, "pareto_container"):
            return
        objectives = self._display_spec().get("objectives") or []
        if len(objectives) < 2 and getattr(self, "display_dataset", None) is None:
            try:
                objectives = self._collect_objectives()
            except Exception:
                objectives = []
        if len(objectives) < 2:
            for child in self.pareto_container.winfo_children():
                child.destroy()
            self.pareto_plots = []
            self._pareto_plot_key = None
            self.pareto_notice_var.set("Add at least two objectives to enable pairwise trade-offs.")
            return
        if len(objectives) >= 4:
            self.pareto_notice_var.set(
                "{} objectives produce {} pairwise projections. Pareto emphasis remains full-dimensional.".format(
                    len(objectives), len(objective_pairs(objectives)),
                )
            )
        else:
            self.pareto_notice_var.set(
                "{} pairwise projection{} · {} full-dimensional Pareto candidate{}".format(
                    len(objective_pairs(objectives)), "" if len(objective_pairs(objectives)) == 1 else "s",
                    len(self.pareto_ids), "" if len(self.pareto_ids) == 1 else "s",
                )
            )
        dataset = getattr(self, "display_dataset", None)
        presentation = (dataset.get("objective_presentation") or {}) if dataset is not None else (getattr(self, "objective_presentation", None) or {})
        pairs = presentation.get("pairs") or [
            {"x_index": left, "y_index": right}
            for left, right in objective_pairs(objectives)
        ]
        pareto_ids = set(presentation.get("pareto_candidate_ids") or (self.pareto_ids if dataset is None else []))
        if not pareto_ids:
            pareto_ids = fallback_pareto_ids(self._display_candidates(), objectives)
        key = (objectives, pairs)
        reuse = key == getattr(self, "_pareto_plot_key", None)
        if not reuse:
            for child in self.pareto_container.winfo_children():
                child.destroy()
            self.pareto_plots = []
            self._pareto_plot_key = snapshot_copy(key)
        for index, pair in enumerate(pairs):
            left_index = int(pair["x_index"])
            right_index = int(pair["y_index"])
            if reuse:
                plot = self.pareto_plots[index]
            else:
                plot = ParetoPlot(
                    self.pareto_container, self, objectives[left_index], objectives[right_index],
                )
                plot.pack(fill="x", pady=7)
                self.pareto_plots.append(plot)
            points = pairwise_points(
                self._display_candidates(), objectives, left_index, right_index, pareto_ids,
            )
            plot.render(points)

    def _load_eda_setup(self):
        epoch = getattr(self, "setup_selection_epoch", 0)
        def loaded(setup):
            if epoch == getattr(self, "setup_selection_epoch", 0):
                self._render_eda_setup(setup)
        self.tasks.submit(self.service.eda_setup, loaded, self._task_error("EDA Discovery"))

    @staticmethod
    def _policy_value(policy, adapter=None):
        if isinstance(policy, str):
            return policy
        policy = policy or {}
        mode = policy.get("mode") or "auto"
        if mode == "allow":
            identifiers = policy.get("adapter_ids") or [policy.get("adapter_id")]
            return "allow:{}".format(identifiers[0]) if identifiers and identifiers[0] else "auto"
        if mode == "custom":
            return "custom"
        if adapter and adapter != "generic-cdf":
            return "custom"
        return mode

    def _render_eda_setup(self, setup):
        self.eda_setup = setup
        self._render_effective_eda_setup(setup)
        self._render_simulator_profiles()
        manual = setup.get("manual") or {}
        for key, variable in self.eda_vars.items():
            value = manual.get(key)
            variable.set("" if value is None else str(value))
        options = setup.get("pdk_adapter_options") or []
        option_ids = [option.get("id") for option in options if option.get("id") and option.get("available", True)]
        if not option_ids:
            option_ids = ["auto", "generic-only", "custom"]
        self.eda_policy_combo.configure(values=option_ids)
        selected = self._policy_value(manual.get("pdk_adapter_policy"), manual.get("pdk_adapter"))
        self.eda_policy_var.set(selected if selected in option_ids else "auto")
        self._replace_text_widget(self.eda_binary_paths, "\n".join(manual.get("extra_binary_paths") or []))
        self._replace_text_widget(self.eda_library_paths, "\n".join(manual.get("extra_library_paths") or []))
        self._replace_text_widget(
            self.eda_profiles,
            json.dumps(manual.get("pdk_profiles") or [], indent=2) if manual.get("pdk_profiles") else "",
        )
        checks = setup.get("checks") or []
        if hasattr(self, "eda_alps_model_map"):
            self._replace_text_widget(self.eda_alps_model_map, json.dumps(manual.get("alps_model_file_map") or {}, indent=2) if manual.get("alps_model_file_map") else "")
        failed = [check.get("name") for check in checks if not check.get("ok") and not check.get("optional")]
        recommendations = setup.get("recommendations") or []
        state = "Ready" if setup.get("ok") else "Needs attention"
        self.eda_status_var.set(
            "{} mode · {}{}{}".format(
                setup.get("mode") or "auto", state,
                " · Failed: {}".format(", ".join(failed)) if failed else "",
                " · {}".format(" ".join(recommendations)) if recommendations else "",
            )
        )

    def _render_effective_eda_setup(self, setup):
        effective = dict(setup.get("effective") or {})
        sources = dict(setup.get("sources") or {})
        profiles = ((setup.get("simulator_profiles") or {}).get("profiles") or {})
        for name, profile in profiles.items():
            key = name + "_model_dir"
            if profile.get("model_dir") and not effective.get(key):
                effective[key] = profile["model_dir"]
                sources[key] = profile.get("model_dir_origin") or "selected setup model discovery"
        pdk = ((getattr(self, "prepared", None) or {}).get("eda_context") or {}).get("pdk") or {}
        for key, value in (("pdk_name", pdk.get("pdk_name") or pdk.get("name")),
                           ("pdk_dir", pdk.get("pdk_dir") or pdk.get("root"))):
            if value and not effective.get(key):
                effective[key] = value
                sources[key] = "selected setup / PDK discovery"
        for key, variable in getattr(self, "eda_effective_vars", {}).items():
            value = effective.get(key)
            if value is None or value == "":
                text = "Not detected (automatic)" if key != "pdk_adapter" else "Automatic adapter selection"
            else:
                text = "{} · {}".format(value, sources.get(key) or "automatic discovery")
            variable.set(text)

    def _render_simulator_profiles(self):
        setup = getattr(self, "eda_setup", {}) or {}
        catalogue = setup.get("simulator_profiles") or {}
        profiles = catalogue.get("profiles") or {}
        effective, sources = setup.get("effective") or {}, setup.get("sources") or {}
        rows = []
        selected = self.simulator_var.get() if hasattr(self, "simulator_var") else "spectre"
        selected_text = ""
        states = {"ready": "路径与模型结构可用（待实际仿真验收）", "unqualified": "待本轮网表确认（unqualified）", "blocked": "需要处理（blocked）"}
        for name, label in (("spectre", "Spectre"), ("alps", "ALPS")):
            profile = profiles.get(name) or {}
            executable = profile.get("executable") or effective.get(name + "_bin") or "未识别"
            model_dir = profile.get("model_dir") or effective.get(name + "_model_dir") or "未识别"
            reader = profile.get("result_reader") or effective.get(name + "_result_reader") or effective.get("simulator_result_reader") or "spectre"
            status = profile.get("status") or "unqualified"
            reasons = profile.get("reasons") or []
            detail = "{}：{}\n  求解程序：{} · {}\n  目标求解模型目录：{} · {}\n  结果读取接口（Ocean）：{}".format(
                label, states.get(status, status), executable, sources.get(name + "_bin") or "自动识别",
                model_dir, sources.get(name + "_model_dir") or profile.get("model_dir_origin") or "自动识别", reader)
            if name == "alps":
                source_models = profile.get("capture_source_model_dir") or (profiles.get("spectre") or {}).get("model_dir") or effective.get("spectre_model_dir")
                if source_models:
                    detail += "\n  原 Maestro 网表生成阶段模型目录（Spectre 格式）：{}".format(source_models)
                detail += "\n  此处 Spectre 说明网表生成来源；最终 ALPS 的目标模型目录见上方，两者按本轮 include/section 分别核验。"
            if status == "unqualified":
                detail += "\n  含义：已发现的路径不等于当前候选的模型已验证。先由原 Maestro 生成本轮 input.scs，取得实际工作目录与 -I 后核验 include/section。"
            if reasons:
                detail += "\n  原始原因：" + "; ".join(str(value) for value in reasons[:3])
            action = {"native_model_capture_required": "先运行原 Maestro 网表诊断，读取本轮 input.scs、工作目录与 -I；无需重复填写已识别的模型根。",
                      "model_interface_unavailable": "自动映射未读懂来源模型的所选 section。先查看本轮保留的 include/section；特殊或加密模型需明确源到目标映射。",
                      "model_structure_available": "模型路径与结构检查已通过；实际求解及结果仍由本轮诊断核验。"}.get(profile.get("reason_code"), profile.get("next_action"))
            if action:
                detail += "\n  下一步：" + str(action)
            elif status == "unqualified":
                detail += "\n  下一步：Load & Inspect → Apply → 运行诊断；已正确识别的模型根无需重复填写。"
            rows.append(detail)
            if name == selected:
                selected_text = "Next run execution: {} | source netlist capture: Spectre".format(label)
        if hasattr(self, "eda_simulator_profiles_var"):
            self.eda_simulator_profiles_var.set("\n\n".join(rows) +
                "\n结果读取接口只负责打开 PSF，不代表实际求解器；ALPS 可选 Spectre/PSF 接口，能否读取本轮结果以诊断证据为准。模型、求解与结果仍分别核验。")
        if hasattr(self, "simulator_profile_var"):
            if getattr(self, "_baseline_mode_active", False):
                self.simulator_profile_var.set("Baseline uses the selected Test’s native simulator and Maestro Job Setup. CLI simulator and model profiles below are not applied to this mode.")
                return
            self.simulator_profile_var.set(selected_text + "\n" + "\n\n".join(rows) +
                "\nThe selected executable, models, mapped sections, command and reader apply to every candidate in the next run. Active runs retain their frozen configuration. 结果读取接口只负责打开 PSF；ALPS 可选 Spectre/PSF 接口，不代表改用 Spectre 求解。")

    @staticmethod
    def _replace_text_widget(widget, value):
        widget.delete("1.0", "end")
        widget.insert("1.0", value)

    @staticmethod
    def _text_lines(widget):
        return [line.strip() for line in widget.get("1.0", "end").splitlines() if line.strip()]

    def _collect_eda_overrides(self):
        manual = copy.deepcopy((getattr(self, "eda_setup", {}) or {}).get("manual") or {})
        for key, variable in self.eda_vars.items():
            value = variable.get().strip()
            if value:
                if key == "simulator_threads":
                    number = finite_number(value)
                    if number is None or int(number) != number or not 1 <= number <= 4096:
                        raise ValueError("Simulator threads must be an integer from 1 to 4096.")
                    value = int(number)
                if key in ("simulator_result_reader", "spectre_result_reader", "alps_result_reader") and value not in ("spectre", "alps"):
                    raise ValueError("Ocean result reader must be spectre or alps.")
                manual[key] = value
            else:
                manual.pop(key, None)
        for key, widget in (
                ("extra_binary_paths", self.eda_binary_paths),
                ("extra_library_paths", self.eda_library_paths)):
            values = self._text_lines(widget)
            if values:
                manual[key] = values
            else:
                manual.pop(key, None)
        policy = self.eda_policy_var.get()
        if policy == "custom":
            path = self.eda_vars["pdk_adapter"].get().strip()
            if not path:
                raise ValueError("Custom Adapter SKILL path is required.")
            manual["pdk_adapter_policy"] = {"mode": "custom", "path": path}
            manual["pdk_adapter"] = path
        elif policy == "auto":
            manual.pop("pdk_adapter_policy", None)
            manual.pop("pdk_adapter", None)
        else:
            manual["pdk_adapter_policy"] = policy
            manual["pdk_adapter"] = "generic-cdf"
        profiles_text = self.eda_profiles.get("1.0", "end").strip()
        if profiles_text:
            profiles = json.loads(profiles_text)
            if not isinstance(profiles, list):
                raise ValueError("PDK Profiles JSON must be an array.")
            manual["pdk_profiles"] = profiles
        else:
            manual.pop("pdk_profiles", None)
        if hasattr(self, "eda_alps_model_map"):
            text = self.eda_alps_model_map.get("1.0", "end").strip()
            if text:
                mapping = json.loads(text)
                if not isinstance(mapping, dict):
                    raise ValueError("ALPS model mapping JSON must be an object.")
                manual["alps_model_file_map"] = mapping
            else:
                manual.pop("alps_model_file_map", None)
        return manual

    def _begin_eda_revalidation(self):
        """Reuse Load/Apply and its identity matcher after an accepted override."""
        if not self.eda_revalidation_required or not self.eda_edit_draft_path or self._setup_reload_block_reason():
            return
        if not callable(getattr(type(self.service), "inspect_cells", None)):
            return
        self.eda_auto_revalidation = {"path": self.eda_edit_draft_path,
            "selection_epoch": self.setup_selection_epoch, "prepare": bool(self.prepared), "preparing": False,
            "page": self.current_page}
        self.save_eda_button.configure(state="disabled")
        self.eda_status_var.set("覆盖已保存，正在自动重新检查并恢复原有输入…")
        self.load_setup(initialize_validation=False)
        if not self.setup_loading:
            self._finish_eda_revalidation("无法开始重新检查；输入草稿已保留。")

    def _advance_eda_revalidation(self, result):
        operation = getattr(self, "eda_auto_revalidation", None)
        if not operation:
            return
        if (operation["selection_epoch"] != self.setup_selection_epoch or
                not self._same_setup_paths(operation["path"], self.setup_path_var.get())):
            self._finish_eda_revalidation("设置身份已改变，未自动套用原编辑。")
            return
        if self.prepared_id:
            if not self.eda_revalidation_required:
                self._finish_eda_revalidation()
            return
        if not operation["prepare"]:
            self._finish_eda_revalidation()
        elif not operation["preparing"]:
            cells = (result.get("counts") or {}).get("cells") or {}
            if not self.selected_cell_scope_ids:
                self._finish_eda_revalidation("原模块没有可证明的匹配；其余草稿已恢复，可选择新模块。")
                return
            if cells.get("skipped"):
                self._activity_event("部分原模块已不存在；跳过这些模块，自动 Apply 其余匹配模块。", "WARN")
            operation["preparing"] = True
            self.apply_selected_cells()
            if not self.setup_loading:
                self._finish_eda_revalidation("无法继续读取原选择模块；输入草稿已保留。")

    def _finish_eda_revalidation(self, error=None):
        operation = getattr(self, "eda_auto_revalidation", None)
        if not operation:
            return
        self.eda_auto_revalidation = None
        self.save_eda_button.configure(state="normal")
        action = "设置已重读" if operation.get("reason") == "saved_setup_reload" else "覆盖已保存"
        if error:
            self.eda_status_var.set(action + "，自动重检未完成：{}".format(error))
        else:
            message = (action + "，重新检查完成，原有编辑已恢复。" if not self.eda_revalidation_required else
                       action + "，层级与模块选择已恢复；按原步骤 Apply 后继续。")
            self.eda_status_var.set(message)
            self._set_status(message)
            if operation.get("page") in PAGE_NAMES:
                self.show_page(operation["page"])

    def save_eda_setup(self):
        try:
            manual = self._collect_eda_overrides()
        except Exception as error:
            self._set_error("EDA Settings: {}".format(error))
            messagebox.showerror("EDA Settings", str(error), parent=self.root)
            return
        self.save_eda_button.configure(state="disabled")
        self.eda_status_var.set("Validating executables and compatibility...")

        def update():
            return self.service.update_eda_setup({"mode": "manual", "manual": manual})

        def saved(setup):
            self.save_eda_button.configure(state="normal")
            self._render_eda_setup(setup)
            if setup.get("prepared_invalidated"):
                self._invalidate_prepared(
                    "EDA settings saved; your edits are retained. Click Load and Inspect, then Apply Selected Cells before running.",
                    preserve_edits=True,
                )
                self._begin_eda_revalidation()

        self.tasks.submit(
            update, saved,
            self._task_error("Save EDA Settings", lambda: self.save_eda_button.configure(state="normal")),
        )

    def reset_eda_setup(self):
        self.eda_status_var.set("Running automatic EDA discovery...")

        def reset(setup):
            self._render_eda_setup(setup)
            if setup.get("prepared_invalidated"):
                self._invalidate_prepared(
                    "EDA settings reset; your edits are retained. Click Load and Inspect, then Apply Selected Cells before running.",
                    preserve_edits=True,
                )
                self._begin_eda_revalidation()

        self.tasks.submit(
            lambda: self.service.update_eda_setup({"mode": "auto"}),
            reset, self._task_error("Reset EDA Settings"),
        )

    def _exit_launch_snapshot(self, state, **changes):
        context = getattr(self, "launch_context", None)
        if not context or not context.get("state_path"):
            return None
        source = context.get("source") or {}
        document = copy.deepcopy(getattr(self, "launch_state", {}) or {})
        document.update({
            "schema_version": "crescendo.maestro_plugin_launch.v1", "pid": os.getpid(),
            "updated_at": utc_timestamp(),
            "source": dict((key, source.get(key)) for key in (
                "library", "cell", "view", "view_dir", "setup_path", "work_dir", "cds_lib", "modified")),
            "selection": {"mode": "invoking" if self._active_context() else "manual",
                          "setup_path": getattr(self, "active_setup_path", None)},
            "state": state,
        })
        document.update(changes)
        return context["state_path"], document

    @staticmethod
    def _write_exit_launch_snapshot(snapshot):
        if snapshot is None:
            return
        path, document = snapshot
        directory = os.path.dirname(os.path.abspath(path))
        temporary = None
        try:
            # Unlike the general writer, never create a removed launch directory.
            descriptor, temporary = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(document, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            temporary = None
        except OSError:
            pass
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def _write_exit_launch_async(self, snapshot):
        threading.Thread(target=self._write_exit_launch_snapshot, args=(snapshot,),
                         name="crescendo-ui-exit-state", daemon=True).start()

    def _cancel_exit_dialog_timer(self):
        timer = getattr(self, "exit_dialog_timer", None)
        if timer is not None:
            self.root.after_cancel(timer)
            self.exit_dialog_timer = None

    def _show_exit_dialog(self, error=None):
        if not self.closing or getattr(self, "force_exit_requested", False):
            return
        if error is None and getattr(self, "shutdown_failed", False):
            error = getattr(self, "exit_dialog_error", "安全退出未完成")
        if error is not None:
            self.exit_dialog_error = error
        window = getattr(self, "exit_dialog", None)
        if window is None or not window.winfo_exists():
            window = tk.Toplevel(self.root)
            self.exit_dialog = window
            window.title("退出优化器")
            window.transient(self.root)
            window.protocol("WM_DELETE_WINDOW", window.withdraw)
            self.exit_dialog_message = tk.StringVar(master=window)
            ttk.Label(window, textvariable=self.exit_dialog_message, wraplength=440,
                      justify="left").pack(padx=18, pady=(16, 10))
            buttons = ttk.Frame(window)
            buttons.pack(padx=18, pady=(0, 16), anchor="e")
            self.exit_retry_button = ttk.Button(buttons, text="重试安全退出",
                                               command=lambda: self.request_exit(confirm=False))
            self.exit_retry_button.pack(side="left", padx=(0, 8))
            self.exit_force_button = ttk.Button(buttons, text="强制退出",
                                                command=lambda: self.request_force_exit(confirm=False))
            self.exit_force_button.pack(side="left", padx=(0, 8))
            ttk.Button(buttons, text="关闭窗口", command=window.withdraw).pack(side="left")
        self.exit_dialog_message.set(
            ("安全退出未完成：{}".format(error) if error is not None else "正在安全退出，请稍候。") +
            "\n强制退出将关闭界面并保留未决任务和私有套件，不能确认远端任务已停止。")
        self.exit_retry_button.configure(state="normal" if error is not None else "disabled")
        window.deiconify()
        window.lift()

    def request_force_exit(self, confirm=True):
        """Always available, including during a blocked safe-exit attempt."""
        if getattr(self, "force_exit_requested", False):
            return
        if confirm and not messagebox.askyesno("强制退出 UI",
                "立即关闭本窗口并保留未决任务和私有套件。\n"
                "这不会确认远端任务已停止，也不会删除其数据。\n"
                "下次打开可新建或加载正常套件；未决套件会单独隔离。", parent=self.root):
            return
        self.force_exit_requested = True
        self._cancel_exit_dialog_timer()
        self.closing = True
        self.cleanup_confirmed = False
        # No IO or controller lock on the Tk thread. Even a hung normal Exit,
        # storage write or Stop must not trap the user inside this window.
        try:
            self.service.request_force_exit()
        except Exception:
            pass
        done = threading.Event()
        self.force_exit_result = None
        force_snapshot = self._exit_launch_snapshot("forced_closed", cleanup_confirmed=False,
                                                    retained_resources=True)
        def isolate():
            try:
                self.force_exit_result = self.service.force_exit_optimizer()
                if force_snapshot is not None:
                    force_snapshot[1]["exit_report"] = self.force_exit_result
                self._write_exit_launch_snapshot(force_snapshot)
            except BaseException as error:
                self.force_exit_result = {"cleanup_confirmed": False, "error": str(error)}
            finally:
                done.set()
        threading.Thread(target=isolate, name="crescendo-ui-force-exit", daemon=True).start()
        deadline = time.monotonic() + 1.5
        def finish():
            if not done.is_set() and time.monotonic() < deadline:
                self.root.after(25, finish)
                return
            # Dropping callbacks cannot upgrade Stop/cleanup evidence. The CLI
            # launcher skips blocking cleanup and executor atexit on this path.
            self.tasks.abandon()
            self.root.destroy()
        self.root.after(0, finish)

    def request_exit(self, confirm=True):
        if self.closing and not getattr(self, "shutdown_failed", False):
            return self._show_exit_dialog()
        if confirm and not messagebox.askyesno(
                "Exit Optimizer",
                "Stop active work, close owned Cadence sessions, and exit?",
                parent=self.root):
            return
        suite = copy.deepcopy(getattr(self, "active_private_suite", None))
        exit_snapshot = None
        exit_snapshot_warning = None
        exit_suite_revisions = dict(getattr(self, "suite_configuration_revisions", {}))
        exit_saved_revisions = []
        if suite and suite.get("id"):
            try:
                raw, context = self._snapshot_configuration()
                suite["revision"] = suite.get("revision") or suite.get("baseline_id")
                suite["source_path"] = suite.get("source_path") or getattr(self, "active_setup_path", None)
                exit_snapshot = (raw, context, copy.deepcopy(getattr(self, "configuration_document", None)), suite)
            except Exception as error:
                exit_snapshot_warning = "退出前套件配置快照未保存：{}".format(error)
        self.closing = True
        self.shutdown_failed = False
        existing_dialog = getattr(self, "exit_dialog", None)
        if existing_dialog is not None and existing_dialog.winfo_exists():
            existing_dialog.withdraw()
        self._cancel_exit_dialog_timer()
        self.exit_dialog_timer = self.root.after(300, self._show_exit_dialog)
        self._cancel_environment_test_poll()
        self._cancel_history_read()
        self._cancel_cell_render()
        self._cancel_parameter_render()
        closing_snapshot = self._exit_launch_snapshot("closing")
        self.health_var.set("Exiting safely...")
        self._set_status("Stopping active work and closing owned Cadence sessions...")
        self.start_button.configure(state="disabled")
        self.stop_button.configure(state="disabled")
        self._set_setup_controls(False)
        if hasattr(self, "history_combo"):
            self.history_combo.configure(state="disabled")
            self.read_history_button.configure(state="disabled")
        for viewer in list(self.log_viewers):
            viewer._close()

        def remember_exit_saved():
            if not hasattr(self, "suite_configuration_revisions"):
                self.suite_configuration_revisions = {}
            for suite_id, revision in exit_saved_revisions:
                self.suite_configuration_revisions[suite_id] = revision

        def closed(_payload):
            remember_exit_saved()
            if getattr(self, "force_exit_requested", False):
                return
            self._cancel_exit_dialog_timer()
            warnings = (_payload or {}).get("warnings") or []
            if warnings:
                messagebox.showwarning("退出警告", "\n".join(str(item) for item in warnings), parent=self.root)
            self.cleanup_confirmed = True
            self._write_exit_launch_async(self._exit_launch_snapshot("closed", cleanup_confirmed=True))
            self.tasks.close_without_wait()
            self.root.destroy()

        def failed(error):
            remember_exit_saved()
            if getattr(self, "force_exit_requested", False):
                return
            self._cancel_exit_dialog_timer()
            self._activity_end("exit")
            self.shutdown_failed = True
            self._write_exit_launch_async(self._exit_launch_snapshot("exit_failed", cleanup_confirmed=False, error=str(error)))
            self.health_var.set("Exit failed")
            self._set_error("安全退出未完成：{}。可重试 Exit，或点“强制退出”关闭 UI 并保留现场。".format(error))
            self._show_exit_dialog(error)

        def save_and_exit():
            self._write_exit_launch_snapshot(closing_snapshot)
            warnings = [exit_snapshot_warning] if exit_snapshot_warning else []
            if exit_snapshot:
                try:
                    raw, context, previous, selected_suite = exit_snapshot
                    document = build_configuration(raw, context["parameter_by_id"], context["cell_scopes"],
                                                   context["outputs"], previous=previous)
                    document["private_suite"] = {"suite_id": selected_suite["id"],
                        "revision": selected_suite.get("revision"), "source_path": selected_suite.get("source_path")}
                    suite_id = selected_suite["id"]
                    revision = (exit_suite_revisions[suite_id] if suite_id in exit_suite_revisions else
                                self.service.private_suite_configuration_revision(suite_id))
                    outcome = self.service.save_private_suite_configuration(suite_id, document, expected_revision=revision)
                    exit_saved_revisions.append((suite_id, outcome["revision"]))
                except Exception as error:
                    warning = "退出前套件配置快照保存未确认：{}".format(error)
                    state = getattr(error, "configuration_commit_state", None)
                    if state:
                        try:
                            revision = self.service.private_suite_configuration_revision(selected_suite["id"])
                        except Exception as read_error:
                            revision = "读取失败：{}".format(read_error)
                        warning += "；提交状态 {}，回读 revision {}".format(state, revision)
                    warnings.append(warning)
            result = self.service.exit_optimizer()
            result = dict(result or {})
            result["warnings"] = warnings + list(result.get("warnings") or [])
            return result
        self.tasks.shutting_down = True
        threading.Thread(target=lambda: self.tasks.begin_shutdown(save_and_exit, closed, failed),
                         name="crescendo-ui-begin-shutdown", daemon=True).start()


def launch_gui(config, service=None, launch_context=None, force_exit_process=False):
    if service is None:
        from .server import OptimizerService
        service = OptimizerService(config)
    try:
        root = tk.Tk()
    except tk.TclError as error:
        raise RuntimeError(
            "native GUI could not connect to a display; verify DISPLAY/X11 forwarding: {}".format(error)
        )
    try:
        if launch_context is None:
            app = OptimizerGui(root, service)
        else:
            app = OptimizerGui(root, service, launch_context=launch_context)
        from .launcher import record_application_initialized
        record_application_initialized()
    except BaseException as initialization_error:
        cleanup_error = None
        try:
            service.exit_optimizer()
        except BaseException as error:
            cleanup_error = error
        try:
            root.destroy()
        except BaseException:
            pass
        if cleanup_error is not None:
            raise RuntimeError(
                "native GUI initialization failed (%s: %s); safe cleanup also failed (%s: %s)"
                % (
                    initialization_error.__class__.__name__, initialization_error,
                    cleanup_error.__class__.__name__, cleanup_error,
                )
            )
        raise
    previous_handlers = {}

    def signal_handler(_signum, _frame):
        try:
            root.after(0, lambda: app.request_exit(confirm=False))
        except tk.TclError:
            pass

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[signum] = signal.signal(signum, signal_handler)
        except (ValueError, OSError):
            pass
    mainloop_error = None
    cleanup_error = None
    try:
        root.mainloop()
    except BaseException as error:
        mainloop_error = error
    finally:
        if not getattr(app, "cleanup_confirmed", False) and not getattr(app, "force_exit_requested", False):
            try:
                service.exit_optimizer()
            except BaseException as error:
                cleanup_error = error
            try:
                if cleanup_error is None:
                    app._write_launch_state("closed", cleanup_confirmed=True)
                else:
                    app._write_launch_state("exit_failed", cleanup_confirmed=False, error=str(cleanup_error))
            except BaseException as error:
                if cleanup_error is None:
                    cleanup_error = error
        if getattr(app, "force_exit_requested", False) or getattr(app, "cleanup_confirmed", False):
            app.tasks.abandon()
        else:
            app.tasks.close()
        for signum, handler in previous_handlers.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError):
                pass
        if force_exit_process and getattr(app, "force_exit_requested", False):
            # Only the dedicated CLI GUI process may take this path. Python's
            # executor atexit joins can otherwise undo the bounded UI exit.
            os._exit(0)
    if mainloop_error is not None and cleanup_error is not None:
        raise RuntimeError(
            "native GUI main loop failed (%s: %s); safe cleanup also failed (%s: %s)"
            % (
                mainloop_error.__class__.__name__, mainloop_error,
                cleanup_error.__class__.__name__, cleanup_error,
            )
        )
    if mainloop_error is not None:
        raise mainloop_error
    if cleanup_error is not None:
        raise cleanup_error
    return 0
