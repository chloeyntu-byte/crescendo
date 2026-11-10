# -*- coding: utf-8 -*-
"""Crescendo presentation layer over the shared Crescendo kernel.

The kernel (``crescendo_engine``) owns every behaviour: service calls, state
machines, validation, persistence and safety rules. ``CrescendoGui`` inherits
all of it from ``OptimizerGui`` and only replaces how the window is laid out,
styled and worded, plus a few renderers that were redrawn wholesale on every
progress poll. Widget attribute names used by the kernel logic are preserved.
"""
from __future__ import print_function

import json
import os
import time

try:
    import tkinter as tk
    from tkinter import ttk
except ImportError:  # pragma: no cover
    import Tkinter as tk
    import ttk

from crescendo_engine import gui as core
from crescendo_engine import __version__ as KERNEL_VERSION
from crescendo_engine.gui_state import (
    ALGORITHM_FIELDS, ALGORITHM_NAMES, candidate_status, compact_metric_values,
    metric_value, metric_definition_label, finite_number,
)

from . import __version__, theme as T, widgets as W
from . import i18n
from .i18n import tr

PRODUCT = "Crescendo"

# internal page key -> (nav label, step number or None, eyebrow, title, subtitle)
PAGES = (
    ("Design Setup", "Design", 1, "Step 1", "Design Setup",
     "Read the Maestro setup, then pick the Test and the circuit blocks to optimize. Your original design is never modified; Crescendo works on a private copy."),
    ("Parameters", "Parameters", 2, "Step 2", "Choose Parameters",
     "Tick the device parameters the optimizer may tune and check their bounds. All W / L can be selected in one click."),
    ("Objectives & Constraints", "Goals", 3, "Step 3", "Goals & Constraints",
     "Tell Crescendo what \"better\" means: metrics to maximize or minimize, and constraints that must hold. With two or more objectives it searches for the Pareto front automatically."),
    ("Optimization", "Run", 4, "Step 4", "Run Optimization",
     "Set the number of generations and press Start Optimization at the top right. A checkpoint is saved after every candidate, so you can stop at any time and resume later."),
    ("Live Results", "Results", 5, "Step 5", "Live Results",
     "Status, objectives and constraints of every candidate. Select one to see its parameters and diagnostics; right-click for its simulator log."),
    ("Pareto Trade-offs", "Trade-offs", 6, "Step 6", "Pareto Trade-offs",
     "Trade-offs between objectives. Highlighted points lie on the full-dimensional Pareto front; click any point to jump to that candidate."),
    ("EDA Settings", "Settings", None, "Pro mode", "Settings",
     "Private Maestro, EDA toolchain, run commands and link diagnostics. Detected values are kept; only explicit overrides take effect."),
)
PAGE_INFO = dict((row[0], row) for row in PAGES)
PAGE_INFO["Settings"] = PAGE_INFO["EDA Settings"]
SUBHEADINGS = {
    "EDA": ("EDA Environment", "Detected values and their sources are listed first, then optional manual overrides; leave a field empty to keep automatic detection."),
}

# Display-only StringVars (never read back by kernel logic for decisions).
DISPLAY_VARS = (
    "status_var", "setup_summary_var", "cell_scope_summary_var", "parameter_summary_var",
    "range_status_var", "analysis_summary_var", "run_state_var", "progress_summary_var",
    "results_summary_var", "result_page_var", "pareto_notice_var", "sidebar_status_var",
    "health_var", "algorithm_notice_var",
)

PREFS_NAME = "crescendo-ui.json"


def _prefs_path():
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(base, "crescendo", PREFS_NAME)


def _read_prefs():
    try:
        with open(_prefs_path(), "r") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, IOError, ValueError):
        return {}


def _write_prefs(prefs):
    path = _prefs_path()
    try:
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        temporary = path + ".tmp"
        with open(temporary, "w") as handle:
            json.dump(prefs, handle, indent=1, sort_keys=True)
        os.rename(temporary, path)
    except (OSError, IOError):
        pass


class TranslatedButton(ttk.Button):
    """Button whose ``text`` is worded for display (the kernel sets "Stop")."""

    def configure(self, cnf=None, **kw):
        if "text" in kw:
            kw["text"] = tr(kw["text"])
        return ttk.Button.configure(self, cnf, **kw)

    config = configure


class DisplayVar(tk.StringVar):
    """Label-only StringVar shown in English under the Crescendo name."""

    def set(self, value):
        return tk.StringVar.set(self, tr(value) if isinstance(value, str) else value)


class _DisplayDialogs(object):
    """Proxy for the kernel's messagebox/filedialog/simpledialog modules."""

    def __init__(self, module):
        self._module = module

    def __getattr__(self, name):
        function = getattr(self._module, name)
        if not callable(function):
            return function

        def call(*args, **kwargs):
            args = tuple(tr(a) if isinstance(a, str) else a for a in args)
            for key in ("title", "message", "detail", "prompt"):
                if isinstance(kwargs.get(key), str):
                    kwargs[key] = tr(kwargs[key])
            if isinstance(kwargs.get("filetypes"), (list, tuple)):
                kwargs["filetypes"] = tuple((tr(label), pattern) for label, pattern in kwargs["filetypes"])
            return function(*args, **kwargs)
        return call


def _install_text_display():
    if getattr(tk.Text.insert, "_crescendo", False):
        return
    original = tk.Text.insert

    def insert(self, index, chars, *args):
        if isinstance(chars, str) and not getattr(self, "_crescendo_raw", False):
            chars = tr(chars)
        return original(self, index, chars, *args)
    insert._crescendo = True
    tk.Text.insert = insert
    tree_insert, tree_item = ttk.Treeview.insert, ttk.Treeview.item

    def display_options(options):
        for key in ("text", "values"):
            value = options.get(key)
            if isinstance(value, str):
                options[key] = tr(value)
            elif isinstance(value, (list, tuple)):
                options[key] = type(value)(tr(v) if isinstance(v, str) else v for v in value)
        return options

    def tree_insert_display(self, parent, index, iid=None, **kw):
        return tree_insert(self, parent, index, iid, **display_options(kw))

    def tree_item_display(self, item, option=None, **kw):
        return tree_item(self, item, option, **display_options(kw))
    ttk.Treeview.insert = tree_insert_display
    ttk.Treeview.item = tree_item_display


class TranslatedVar(tk.StringVar):
    """StringVar whose displayed value is translated when set."""

    def set(self, value):
        return tk.StringVar.set(self, tr(value) if isinstance(value, str) else value)


def _install_palette():
    """Point the kernel's module-level colours at the Crescendo palette."""
    core.BACKGROUND = T.BACKGROUND
    core.PANEL = T.PANEL
    core.SIDEBAR = T.INK
    core.ACCENT = T.ACCENT
    core.ACCENT_LIGHT = T.ACCENT_SOFT
    core.TEXT = T.TEXT
    core.MUTED = T.MUTED
    core.DANGER = T.DANGER
    core.SUCCESS = T.SUCCESS
    core.WARNING = T.WARNING
    core.PALETTE = T.CHART


class ParetoPlot(core.ParetoPlot):
    """Kernel Pareto plot with Crescendo styling and an explicit front line."""

    def __init__(self, parent, app, left_definition, right_definition):
        ttk.Frame.__init__(self, parent, style="Card.TFrame", padding=(16, 12))
        self.app = app
        self.left_definition = left_definition
        self.right_definition = right_definition
        ttk.Label(self, text="{}  ×  {}".format(left_definition.get("output_key") or "X",
                                               right_definition.get("output_key") or "Y"),
                  style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(self, text="X: {}\nY: {}".format(metric_definition_label(left_definition),
                                                  metric_definition_label(right_definition)),
                  style="Help.TLabel", justify="left").pack(anchor="w", pady=(2, 6))
        self.canvas = tk.Canvas(self, width=780, height=330, background=T.PANEL, highlightthickness=0)
        self.canvas.pack(fill="x", expand=True)
        self._points = None
        self.canvas.bind("<Configure>", lambda _e: self._points is not None and self.render(self._points))

    def render(self, points):
        self._points = points
        canvas = self.canvas
        canvas.delete("all")
        width = max(420, canvas.winfo_width() or 780)
        height = 330
        left, right, top, bottom = 70, width - 20, 16, height - 42
        if not points:
            canvas.create_text(width / 2.0, height / 2.0, text="Waiting for completed candidates…", fill=T.FAINT, font=T.FONTS["body"])
            return
        xs = [p["x"] for p in points]
        ys = [p["y"] for p in points]
        x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
        xp = (x1 - x0) * 0.08 or max(abs(x1), 1.0) * 0.08
        yp = (y1 - y0) * 0.08 or max(abs(y1), 1.0) * 0.08
        x0, x1, y0, y1 = x0 - xp, x1 + xp, y0 - yp, y1 + yp

        def sx(v):
            return left + (v - x0) / (x1 - x0) * (right - left)

        def sy(v):
            return bottom - (v - y0) / (y1 - y0) * (bottom - top)

        for tick in W.nice_ticks(x0, x1, 6):
            x = sx(tick)
            canvas.create_line(x, top, x, bottom, fill=T.CHART_GRID)
            canvas.create_text(x, bottom + 14, text=W.fmt(tick), fill=T.FAINT, font=T.FONTS["tiny"])
        for tick in W.nice_ticks(y0, y1, 5):
            y = sy(tick)
            canvas.create_line(left, y, right, y, fill=T.CHART_GRID)
            canvas.create_text(left - 8, y, text=W.fmt(tick), anchor="e", fill=T.FAINT, font=T.FONTS["tiny"])
        canvas.create_text((left + right) / 2.0, height - 10, text=self.left_definition.get("output_key") or "X",
                           fill=T.MUTED, font=T.FONTS["small"])
        canvas.create_text(left, 4, text="↑ " + str(self.right_definition.get("output_key") or "Y"), anchor="nw",
                           fill=T.MUTED, font=T.FONTS["small"])
        latest = max([p.get("generation") or 0 for p in points] or [0]) or 1
        front = sorted((p for p in points if p["pareto"]), key=lambda p: p["x"])
        for point in points:
            if point["pareto"]:
                continue
            x, y = sx(point["x"]), sy(point["y"])
            if not point["feasible"]:
                fill = "#d9dce2"
            else:
                age = float(point.get("generation") or 0) / latest
                fill = "#c7c9f2" if age < 0.5 else "#9c9ce6"
            item = canvas.create_oval(x - 3.5, y - 3.5, x + 3.5, y + 3.5, fill=fill, outline="",
                                      tags=("candidate:{}".format(point["candidate_id"]), "point"))
            canvas.tag_bind(item, "<Button-1>", lambda _e, cid=point["candidate_id"]: self.app.select_candidate(cid))
        if len(front) >= 2:
            line = []
            for point in front:
                line.extend((sx(point["x"]), sy(point["y"])))
            canvas.create_line(*line, fill=T.ACCENT, width=2, dash=(5, 3))
        for point in front:
            x, y = sx(point["x"]), sy(point["y"])
            item = canvas.create_oval(x - 6, y - 6, x + 6, y + 6, fill=T.ACCENT, outline="#ffffff", width=2,
                                      tags=("candidate:{}".format(point["candidate_id"]), "point", "pareto"))
            canvas.tag_bind(item, "<Button-1>", lambda _e, cid=point["candidate_id"]: self.app.select_candidate(cid))
        canvas.create_text(right, top, anchor="ne", fill=T.ACCENT_TEXT, font=T.FONTS["small"],
                           text="● {} on the Pareto front".format(len(front)))


class ParameterViewSwitch(core.ParameterViewSwitch):
    """Kernel segmented switch, redrawn in Crescendo colours and wording."""

    def _draw(self):
        self.delete("all")
        selected = self.variable.get() if self.variable.get() in ("hierarchy", "name") else "hierarchy"
        self.configure(background=T.PANEL)
        self.create_rectangle(1, 1, 167, 27, fill=T.PANEL_ALT, outline=T.BORDER)
        left = (3, 3, 84, 25) if selected == "hierarchy" else (84, 3, 165, 25)
        self.create_rectangle(*left, fill=T.INK, outline=T.INK)
        for x, value, text in ((43, "hierarchy", "Hierarchy"), (125, "name", "By Name")):
            self.create_text(x, 14, text=text, font=T.FONTS["small"],
                             fill="#ffffff" if selected == value else T.MUTED)


_Base = core.OptimizerGui


class CrescendoGui(_Base):
    # ------------------------------------------------------------------ setup
    def __init__(self, root, service, launch_context=None):
        self._crescendo_prefs = _read_prefs()
        forced = os.environ.get("CRESCENDO_MODE", "").strip().lower()
        if forced in ("pro", "simple"):
            self._crescendo_prefs["pro_mode"] = forced == "pro"
        self._building_page = None
        self._page_layouts = {}
        self._simple_hidden = set()
        self._dynamic_hidden = set()
        self._translatable_widgets = []
        self.nav_items = {}
        _Base.__init__(self, root, service, launch_context=launch_context)
        self._wrap_display_vars()
        self._wrap_label_vars()
        self._install_display_hooks()
        self._translate_static_texts(self.root)
        self._apply_mode(initial=True)
        self._update_nav_progress()

    def _configure_root(self):
        title = PRODUCT
        if self.launch_context:
            source = self.launch_context.get("source") or {}
            title += " — {}/{}/{}".format(source.get("library", ""), source.get("cell", ""), source.get("view", ""))
        self.root.title(title)
        width = min(1480, max(1120, self.root.winfo_screenwidth() - 80))
        height = min(940, max(700, self.root.winfo_screenheight() - 90))
        self.root.geometry("{}x{}".format(width, height))
        self.root.minsize(1080, 680)
        self.root.configure(background=T.BACKGROUND)
        try:
            icon = tk.PhotoImage(width=32, height=32)
            icon.put(T.INK, to=(0, 0, 32, 32))
            for i in range(8, 24):
                spread = int((i - 8) * 0.55)
                icon.put(T.ACCENT, to=(i, 16 - spread - 1, i + 1, 16 - spread + 1))
                icon.put(T.ACCENT, to=(i, 16 + spread - 1, i + 1, 16 + spread + 1))
            self.root.iconphoto(True, icon)
            self._icon = icon
        except tk.TclError:
            pass

    def _configure_styles(self):
        _install_palette()
        T.configure_styles(self.root)

    # ------------------------------------------------------------------ shell
    def _build_shell(self):
        self.health_var = tk.StringVar(value="Native service ready")
        self.pro_mode_var = tk.BooleanVar(value=bool(self._crescendo_prefs.get("pro_mode", False)))
        self.log_open_var = tk.BooleanVar(value=bool(self._crescendo_prefs.get("log_open", False)))

        sidebar = tk.Frame(self.root, background=T.INK, width=236)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)
        W.BrandMark(sidebar).pack(fill="x", pady=(10, 6))
        tk.Label(sidebar, text="WORKFLOW", background=T.INK, foreground="#5d6470",
                 font=(T.FONTS["family"], 8, "bold"), anchor="w").pack(fill="x", padx=22, pady=(14, 4))
        for key, label, number, _eyebrow, _title, _subtitle in PAGES:
            if number is None:
                self._settings_divider = tk.Frame(sidebar, background=T.INK_LINE, height=1)
                self._settings_divider.pack(fill="x", padx=18, pady=10)
            item = W.NavItem(sidebar, number, label, command=lambda name=key: self.show_page(name))
            item.pack(fill="x")
            self.nav_items[key] = item
        self.page_buttons = dict(self.nav_items)

        footer = tk.Frame(sidebar, background=T.INK)
        footer.pack(side="bottom", fill="x", padx=4, pady=(0, 14))
        self.sidebar_status_var = tk.StringVar(value="No setup loaded")
        tk.Label(footer, textvariable=self.sidebar_status_var, background=T.INK, foreground=T.INK_MUTED,
                 font=T.FONTS["small"], wraplength=196, justify="left", anchor="w").pack(fill="x", padx=18, pady=(0, 10))
        tk.Frame(footer, background=T.INK_LINE, height=1).pack(fill="x", padx=14, pady=(0, 6))
        self.pro_switch = W.ToggleSwitch(footer, self.pro_mode_var, text="Pro mode (all options)",
                                         command=self._apply_mode)
        self.pro_switch.pack(fill="x", padx=0)
        tk.Label(footer, text="{} {} · kernel {}".format(PRODUCT, __version__, KERNEL_VERSION), background=T.INK,
                 foreground="#4d535e", font=T.FONTS["tiny"], anchor="w").pack(fill="x", padx=18, pady=(6, 0))

        main = tk.Frame(self.root, background=T.BACKGROUND)
        main.pack(side="left", fill="both", expand=True)
        self._main = main
        self._build_topbar(main)
        self._build_activity_panel_in(main)

        content = ttk.Frame(main, style="Page.TFrame")
        content.pack(side="top", fill="both", expand=True)
        for key, _label, _number, _eyebrow, _title, _subtitle in PAGES:
            self.pages[key] = core.ScrollableFrame(content)

        builders = (
            ("Design Setup", self._build_design_page), ("Parameters", self._build_parameters_page),
            ("Objectives & Constraints", self._build_metrics_page), ("Optimization", self._build_optimization_page),
            ("Live Results", self._build_results_page), ("Pareto Trade-offs", self._build_pareto_page),
            ("EDA Settings", self._build_settings_page),
        )
        for key, builder in builders:
            self._building_page = key
            builder(self.pages[key].body)
            self._record_layout(key)
        self._building_page = None
        self._watch_activity_statuses()

    def _build_topbar(self, parent):
        bar = tk.Frame(parent, background=T.PANEL, height=62)
        bar.pack(side="top", fill="x")
        bar.pack_propagate(False)
        tk.Frame(parent, background=T.BORDER, height=1).pack(side="top", fill="x")
        left = tk.Frame(bar, background=T.PANEL)
        left.pack(side="left", fill="y", padx=(22, 0))
        source = (self.launch_context or {}).get("source") or {}
        if source:
            context = "{}  /  {}  /  {}".format(source.get("library", "?"), source.get("cell", "?"), source.get("view", "?"))
            caption = "Current Maestro"
        else:
            context, caption = "Standalone", "Not bound to a Maestro window"
        tk.Label(left, text=caption, background=T.PANEL, foreground=T.FAINT, font=T.FONTS["tiny"], anchor="w").pack(
            anchor="w", pady=(11, 0))
        line = tk.Frame(left, background=T.PANEL)
        line.pack(anchor="w")
        tk.Label(line, text=context, background=T.PANEL, foreground=T.TEXT, font=T.FONTS["title"]).pack(side="left")
        self.run_pill_var = tk.StringVar(value="Idle")
        self.run_pill = W.Pill(line, textvariable=self.run_pill_var, kind="neutral")
        self.run_pill.pack(side="left", padx=(12, 0))
        self.health_label = tk.Label(line, textvariable=self.health_var, background=T.PANEL, foreground=T.FAINT,
                                     font=T.FONTS["small"])
        self.health_label.pack(side="left", padx=(10, 0))

        right = tk.Frame(bar, background=T.PANEL)
        right.pack(side="right", fill="y", padx=(0, 18))
        self.start_button = ttk.Button(right, text="▶  Start Optimization", style="Hero.TButton", command=self.start_run, state="disabled")
        self.start_button.pack(side="right", pady=12)
        self.stop_button = TranslatedButton(right, text="Stop", style="Danger.TButton", command=self.stop_run, state="disabled")
        self.stop_button.pack(side="right", padx=8, pady=14)
        ttk.Button(right, text="Exit", style="TopDanger.TButton", command=self.request_exit).pack(side="right", padx=(0, 6), pady=14)
        self._translatable_widgets.append(self.stop_button)

    def _build_activity_panel(self):
        # Built from _build_shell so it lives inside the main column.
        pass

    def _build_activity_panel_in(self, parent):
        self.activity_journal = core.ActivityJournal()
        self.activity_cursor = 0
        self.activity_flush_job = None
        self.activity_display_first = None
        self.activity_wait_mode = None
        self.activity_finish_jobs = {}
        self.root.bind("<Destroy>", self._activity_destroyed, add="+")
        self.activity_frame = tk.Frame(parent, background=T.PANEL)
        self.activity_frame.pack(side="bottom", fill="x")
        tk.Frame(self.activity_frame, background=T.BORDER, height=1).pack(side="top", fill="x")
        self.activity_log_frame = tk.Frame(self.activity_frame, background=T.PANEL)
        self.activity_log = tk.Text(self.activity_log_frame, height=7, wrap="word", state="disabled", undo=False,
                                    font=T.FONTS["mono"], relief="flat", background=T.PANEL_ALT, foreground=T.TEXT,
                                    padx=12, pady=6, borderwidth=0, highlightthickness=0)
        scroll = ttk.Scrollbar(self.activity_log_frame, orient="vertical", command=self.activity_log.yview)
        self.activity_log.configure(yscrollcommand=scroll.set)
        self.activity_log.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.activity_log.tag_configure("WARN", foreground=T.WARNING)
        self.activity_log.tag_configure("ERROR", foreground=T.DANGER)

        line = tk.Frame(self.activity_frame, background=T.PANEL, height=38)
        line.pack(side="bottom", fill="x")
        line.pack_propagate(False)
        self._activity_line = line
        self.activity_dot = tk.Canvas(line, width=10, height=10, background=T.PANEL, highlightthickness=0)
        self.activity_dot.pack(side="left", padx=(18, 8))
        self._set_activity_dot(T.SUCCESS)
        self.status_var = TranslatedVar(value="Ready")
        tk.Label(line, textvariable=self.status_var, background=T.PANEL, foreground=T.MUTED, font=T.FONTS["small"],
                 anchor="w").pack(side="left", fill="x", expand=True)
        self.log_toggle_button = ttk.Button(line, text="", style="Ghost.TButton", command=self._toggle_log)
        self.log_toggle_button.pack(side="right", padx=(0, 10))
        ttk.Button(line, text="Details…", style="Ghost.TButton", command=self.show_diagnostic_details).pack(side="right")
        self.activity_wait_frame = tk.Frame(line, background=T.PANEL)
        self.activity_wait_var = TranslatedVar(value="")
        tk.Label(self.activity_wait_frame, textvariable=self.activity_wait_var, background=T.PANEL,
                 foreground=T.ACCENT_TEXT, font=T.FONTS["small"], width=36, anchor="e").pack(side="left", padx=6)
        self.progress_var = tk.DoubleVar(value=0.0)
        self.progress_bar = ttk.Progressbar(self.activity_wait_frame, variable=self.progress_var, maximum=1, length=170,
                                            mode="determinate", style="Crescendo.Horizontal.TProgressbar")
        self.progress_bar.pack(side="right", padx=(0, 12))
        self._render_log_toggle()
        self._activity_event("Crescendo ready. Load and Inspect reads the current saved Maestro.")

    def _set_activity_dot(self, color):
        self.activity_dot.delete("all")
        self.activity_dot.create_oval(1, 1, 9, 9, fill=color, outline="")

    def _render_log_toggle(self):
        opened = bool(self.log_open_var.get())
        self.log_toggle_button.configure(text="Hide Log ▾" if opened else "Activity Log ▴")
        if opened:
            self.activity_log_frame.pack(side="top", fill="x", before=self._activity_line)
        else:
            self.activity_log_frame.pack_forget()

    def _toggle_log(self):
        self.log_open_var.set(not self.log_open_var.get())
        self._render_log_toggle()
        self._save_pref("log_open", bool(self.log_open_var.get()))

    def _save_pref(self, key, value):
        self._crescendo_prefs[key] = value
        _write_prefs(self._crescendo_prefs)

    def _activity_event(self, message, level="INFO", key=None):
        message = tr(message) if isinstance(message, str) else message
        result = _Base._activity_event(self, message, level, key=key)
        if level in ("ERROR", "WARN") and hasattr(self, "activity_dot"):
            self._set_activity_dot(T.DANGER if level == "ERROR" else T.WARNING)
        return result

    def _set_status(self, message):
        _Base._set_status(self, tr(message) if isinstance(message, str) else message)
        if hasattr(self, "activity_dot"):
            self._set_activity_dot(T.SUCCESS)

    def _set_error(self, message):
        _Base._set_error(self, tr(message) if isinstance(message, str) else message)
        if hasattr(self, "activity_dot"):
            self._set_activity_dot(T.DANGER)

    # ------------------------------------------------------------ page chrome
    def _page_heading(self, parent, title, subtitle):
        info = PAGE_INFO.get(title)
        if info is None and title in SUBHEADINGS:
            zh_title, zh_subtitle = SUBHEADINGS[title]
            frame = ttk.Frame(parent, style="Page.TFrame", padding=(26, 14, 26, 4))
            frame.pack(fill="x")
            ttk.Label(frame, text=zh_title, style="CardTitle.TLabel", background=T.BACKGROUND).pack(anchor="w")
            ttk.Label(frame, text=zh_subtitle, style="Muted.TLabel", wraplength=980).pack(anchor="w", pady=(2, 0))
            return
        if info is None:
            return _Base._page_heading(self, parent, title, subtitle)
        _key, _nav, _number, eyebrow, zh_title, zh_subtitle = info
        frame = ttk.Frame(parent, style="Page.TFrame", padding=(28, 24, 28, 8))
        frame.pack(fill="x")
        ttk.Label(frame, text=eyebrow.upper(), style="Eyebrow.TLabel").pack(anchor="w")
        ttk.Label(frame, text=zh_title, style="PageTitle.TLabel").pack(anchor="w", pady=(2, 0))
        label = ttk.Label(frame, text=zh_subtitle, style="Muted.TLabel", wraplength=980, justify="left")
        label.pack(anchor="w", pady=(6, 0))
        self._watch_help_wrapping(frame, [label])

    def _card(self, parent, padding=(18, 16)):
        frame = ttk.Frame(parent, style="Card.TFrame", padding=padding)
        frame.pack(fill="x", padx=28, pady=7)
        settings = getattr(self, "pages", {}).get("EDA Settings")
        if settings is not None and self._widget_is_descendant(parent, settings):
            self._watch_help_wrapping(frame)
        return frame

    def _advanced(self, *widgets):
        """Mark widgets that only appear in professional mode."""
        for widget in widgets:
            if widget is not None:
                self._simple_hidden.add(str(widget))
        return widgets[0] if len(widgets) == 1 else widgets

    def _record_layout(self, key):
        """Snapshot pack order of every page-level and card-level child."""
        page = self.pages[key]
        records = []

        def collect(container, depth):
            for child in container.pack_slaves():
                info = child.pack_info()
                info.pop("in", None)
                records.append((child, container, info))
                if depth < 2 and isinstance(child, (ttk.Frame, tk.Frame)):
                    collect(child, depth + 1)
        collect(page.body, 0)
        self._page_layouts[key] = records

    def _apply_mode(self, initial=False):
        pro = bool(self.pro_mode_var.get())
        if not initial:
            self._save_pref("pro_mode", pro)
        for key, records in self._page_layouts.items():
            hidden_any = any(str(widget) in self._simple_hidden for widget, _parent, _info in records)
            if not hidden_any:
                continue
            by_parent = {}
            for widget, parent, info in records:
                by_parent.setdefault(str(parent), []).append((widget, info))
            for rows in by_parent.values():
                for widget, _info in rows:
                    if widget.winfo_exists() and widget.winfo_manager() == "pack":
                        widget.pack_forget()
                for widget, info in rows:
                    if not widget.winfo_exists():
                        continue
                    if not pro and str(widget) in self._simple_hidden:
                        continue
                    if str(widget) in self._dynamic_hidden:
                        continue
                    widget.pack(**info)
        settings_nav = self.nav_items.get("EDA Settings")
        if settings_nav is not None:
            if pro:
                self._settings_divider.pack(fill="x", padx=18, pady=10)
                settings_nav.pack(fill="x")
            else:
                self._settings_divider.pack_forget()
                settings_nav.pack_forget()
                if self.current_page == "EDA Settings":
                    self.show_page("Design Setup")
        if not initial:
            self._activity_event("Pro mode: all advanced options are shown." if pro else "Simple mode: advanced options are hidden; settings are unchanged.")

    def _wrap_display_vars(self):
        # The replaced Python objects must stay alive: tkinter unsets the Tcl
        # variable when a Variable object is garbage collected.
        self._replaced_vars = getattr(self, "_replaced_vars", [])
        for name in DISPLAY_VARS:
            variable = getattr(self, name, None)
            if variable is None or isinstance(variable, TranslatedVar) or not isinstance(variable, tk.StringVar):
                continue
            # Re-point the same Tcl variable through the translating class.
            wrapped = TranslatedVar(self.root, name=str(variable))
            wrapped.set(variable.get())
            # Traces registered through the old object stay removable here.
            wrapped._tclCommands = variable._tclCommands
            # Both objects share one list: kernel code may remove traces through
            # either reference (the old one is kept alive in _replaced_vars).
            self._replaced_vars.append(variable)
            setattr(self, name, wrapped)

    def _wrap_label_vars(self):
        """Display wording for StringVars that are shown by labels and never
        by an input widget (input values must round-trip untouched)."""
        shown, edited = set(), set()
        pending = [self.root]
        while pending:
            widget = pending.pop()
            pending.extend(widget.winfo_children())
            try:
                name = str(widget.cget("textvariable"))
            except tk.TclError:
                continue
            if not name:
                continue
            (shown if widget.winfo_class() in ("TLabel", "Label", "TButton", "Button") else edited).add(name)
        labels_only = shown - edited
        for attribute, variable in list(vars(self).items()):
            if (isinstance(variable, tk.StringVar) and not isinstance(variable, (TranslatedVar, DisplayVar))
                    and str(variable) in labels_only):
                wrapped = DisplayVar(self.root, name=str(variable))
                wrapped._tclCommands = variable._tclCommands
                wrapped.set(variable.get())
                self._replaced_vars.append(variable)
                setattr(self, attribute, wrapped)

    def _install_display_hooks(self):
        # Editable Text fields (EDA overrides) must keep exactly what was typed.
        for name in ("eda_binary_paths", "eda_library_paths", "eda_profiles", "eda_alps_model_map"):
            widget = getattr(self, name, None)
            if widget is not None:
                widget._crescendo_raw = True
        root_title = self.root.title

        def title(text=None):
            return root_title() if text is None else root_title(tr(text))
        self.root.title = title
        self.root.wm_title = title
        seen = set()

        def mapped(event):
            widget = event.widget
            if not isinstance(widget, tk.Toplevel) or str(widget) in seen:
                return
            seen.add(str(widget))
            try:
                widget.title(tr(widget.title()))
            except tk.TclError:
                pass
            self._translate_static_texts(widget)
        self.root.bind_class("Toplevel", "<Map>", mapped, add="+")

    def _show_text_details(self, title, text):
        return _Base._show_text_details(self, tr(title), tr(text) if isinstance(text, str) else text)

    def _translate_static_texts(self, widget):
        pending = [widget]
        while pending:
            current = pending.pop()
            pending.extend(current.winfo_children())
            try:
                cls = current.winfo_class()
            except tk.TclError:
                continue
            if cls in ("TLabel", "TButton", "TCheckbutton", "TRadiobutton", "Label", "Button"):
                try:
                    if str(current.cget("textvariable")):
                        continue
                    text = current.cget("text")
                except tk.TclError:
                    continue
                if isinstance(text, str) and text:
                    translated = tr(text) if i18n.ENABLED else i18n.kernel_wording(text)
                    if translated != text:
                        current.configure(text=translated)
            elif cls == "TNotebook":
                for tab in current.tabs():
                    try:
                        label = current.tab(tab, "text")
                    except tk.TclError:
                        continue
                    translated = tr(label) if i18n.ENABLED else label
                    if translated != label:
                        current.tab(tab, text=translated)
            elif cls == "Treeview":
                for column in ("#0",) + tuple(current.cget("columns") or ()):
                    try:
                        heading = current.heading(column, "text")
                    except tk.TclError:
                        continue
                    translated = tr(heading)
                    if translated != heading:
                        current.heading(column, text=translated)

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
            else:
                page.pack_forget()
        for page_name, item in getattr(self, "nav_items", {}).items():
            item.set_state(active=page_name == name)
        page = self.pages.get(name)
        if page is not None and hasattr(page, "canvas"):
            page.canvas.yview_moveto(0)
        if name == "Optimization" and hasattr(self, "recent_tree"):
            self.refresh_recent_runs()
            self._render_dashboard()
        elif name == "Parameters" and hasattr(self, "parameter_tree"):
            # The kernel rebuilt the full tree on every visit; with thousands
            # of devices that is seconds of work. Re-render only when stale.
            if (getattr(self, "parameter_tree_dirty", True) or
                    self._parameter_tree_key() != getattr(self, "_parameter_visible_key", None)):
                self.render_parameter_tree()
        elif name == "Live Results" and hasattr(self, "result_tree"):
            self.render_results()
        elif name == "Pareto Trade-offs" and hasattr(self, "pareto_container"):
            self.render_pareto()
        self._update_nav_progress()

    def _update_nav_progress(self):
        if not getattr(self, "nav_items", None):
            return
        prepared = bool(getattr(self, "prepared_id", None))
        selected = any(value.get("selected") for value in getattr(self, "parameter_configs", {}).values())
        objectives = len(getattr(self, "objective_rows", []) or [])
        candidates = len(getattr(self, "live_candidates", []) or [])
        state = getattr(getattr(self, "run_ui", None), "state", None)
        done = {
            "Design Setup": prepared,
            "Parameters": prepared and selected,
            "Objectives & Constraints": objectives > 0,
            "Optimization": state in ("completed", "stopped"),
            "Live Results": candidates > 0 and state in ("completed", "stopped"),
            "Pareto Trade-offs": False,
        }
        for key, item in self.nav_items.items():
            item.set_state(done=done.get(key, False))
        pill = {"queued": ("Queued", "accent"), "running": ("Optimizing", "accent"), "stopping": ("Stopping", "warning"),
                "completed": ("Completed", "success"), "stopped": ("Stopped", "neutral"), "failed": ("Failed", "danger")}
        text, kind = pill.get(state, ("Ready" if prepared else "Idle", "success" if prepared else "neutral"))
        strip = getattr(self, "quickstart_strip", None)
        if strip is not None:
            if prepared and str(strip) not in self._dynamic_hidden:
                self._dynamic_hidden.add(str(strip))
                strip.pack_forget()
        if hasattr(self, "run_pill"):
            self.run_pill_var.set(text)
            self.run_pill.set_kind(kind)

    # ------------------------------------------------------------ design page
    def _build_design_page(self, parent):
        self._page_heading(parent, "Design Setup", "")
        bound = bool(self.launch_context)
        source = (self.launch_context or {}).get("source") or {}

        hero = ttk.Frame(parent, style="Card.TFrame", padding=(22, 20))
        hero.pack(fill="x", padx=28, pady=(10, 7))
        self.design_hero = hero
        top = ttk.Frame(hero, style="Card.TFrame")
        top.pack(fill="x")
        text_col = ttk.Frame(top, style="Card.TFrame")
        text_col.pack(side="left", fill="x", expand=True)
        if bound:
            ttk.Label(text_col, text="{} / {} / {}".format(source.get("library", ""), source.get("cell", ""),
                                                          source.get("view", "")),
                      style="CardTitle.TLabel", font=T.FONTS["h2"]).pack(anchor="w")
            ttk.Label(text_col, text="Reads the Tests, cell hierarchy and saved settings of this Maestro.", style="CardMuted.TLabel").pack(
                anchor="w", pady=(4, 0))
        else:
            ttk.Label(text_col, text="Choose a Maestro / ADE XL setup", style="CardTitle.TLabel",
                      font=T.FONTS["h2"]).pack(anchor="w")
            ttk.Label(text_col, text="Enter a setupDB path or choose its folder, then Load and Inspect.", style="CardMuted.TLabel").pack(
                anchor="w", pady=(4, 0))
        self.load_setup_button = ttk.Button(top, text="Load and Inspect", style="Hero.TButton", command=self.load_setup)
        self.load_setup_button.pack(side="right", padx=(16, 0))

        quick = tk.Frame(hero, background=T.PANEL)
        quick.pack(fill="x", pady=(14, 0))
        for index, (title, note) in enumerate((("Load and Inspect", "Test and circuit blocks"), ("Tick parameters", "All W / L in one click"),
                                                ("Goals", "Maximize / minimize / constrain"), ("Start", "Simulate and search automatically"))):
            cell = tk.Frame(quick, background=T.PANEL_ALT, highlightthickness=1, highlightbackground=T.BORDER)
            cell.grid(row=0, column=index, sticky="nsew", padx=(0 if index == 0 else 8, 0))
            quick.columnconfigure(index, weight=1, uniform="quick")
            tk.Label(cell, text=str(index + 1), background=T.ACCENT if index == 0 else T.INK, foreground="#ffffff",
                     font=(T.FONTS["family"], 9, "bold"), width=2).pack(side="left", padx=(10, 8), pady=10)
            text = tk.Frame(cell, background=T.PANEL_ALT)
            text.pack(side="left", fill="x", pady=6)
            tk.Label(text, text=title, background=T.PANEL_ALT, foreground=T.TEXT, font=T.FONTS["strong"],
                     anchor="w").pack(fill="x")
            tk.Label(text, text=note, background=T.PANEL_ALT, foreground=T.FAINT, font=T.FONTS["tiny"],
                     anchor="w").pack(fill="x")
        self.quickstart_strip = quick
        path_row = ttk.Frame(hero, style="Card.TFrame")
        path_row.pack(fill="x", pady=(14, 0))
        self.setup_path_var = tk.StringVar(value="")
        self.setup_path_entry = ttk.Entry(path_row, textvariable=self.setup_path_var)
        self.setup_path_entry.pack(side="left", fill="x", expand=True)
        self.browse_setup_folder_button = ttk.Button(path_row, text="Browse Folder...", command=self._browse_setup_folder)
        self.browse_setup_folder_button.pack(side="right", padx=(6, 0))
        self.browse_setup_file_button = ttk.Button(path_row, text="Browse File...", command=self._browse_setup_file)
        self.browse_setup_file_button.pack(side="right", padx=(6, 0))
        if bound:
            self._advanced(path_row)
        self.setup_summary_var = tk.StringVar(value="No setup loaded.")
        ttk.Label(hero, textvariable=self.setup_summary_var, style="CardMuted.TLabel", wraplength=980,
                  justify="left").pack(anchor="w", pady=(12, 0))
        if bound:
            change_status = ("saved" if source.get("modified") is False else
                             "unsaved changes (the saved version is read)" if source.get("modified") is True else "unknown; reading the saved setup")
            identity = "Working directory: {}\nLibrary mapping: {}\nMaestro at launch: {}".format(
                source.get("work_dir", ""), source.get("cds_lib", ""), change_status)
            self.setup_source_var = tk.StringVar(value=identity)
            source_label = ttk.Label(hero, textvariable=self.setup_source_var, style="Help.TLabel", wraplength=980,
                                     justify="left")
            source_label.pack(anchor="w", pady=(6, 0))
            history = ttk.Frame(hero, style="Card.TFrame")
            history.pack(fill="x", pady=(10, 0))
            self.launch_history_summary_var = tk.StringVar(value="History is optional and is not read automatically.")
            ttk.Label(history, textvariable=self.launch_history_summary_var, style="Help.TLabel", wraplength=980).pack(
                anchor="w")
            row = ttk.Frame(history, style="Card.TFrame")
            row.pack(fill="x", pady=(6, 0))
            ttk.Label(row, text="History (optional)", style="Card.TLabel").pack(side="left", padx=(0, 8))
            self.history_choice_var = tk.StringVar(value="None")
            self.history_combo = ttk.Combobox(row, textvariable=self.history_choice_var, values=("None",),
                                              state="readonly", width=36)
            self.history_combo.pack(side="left", fill="x", expand=True)
            self.history_combo.bind("<<ComboboxSelected>>", self._history_choice_changed)
            self.read_history_button = ttk.Button(row, text="Read History Summary", command=self._load_launch_history,
                                                  state="disabled")
            self.read_history_button.pack(side="left", padx=(8, 0))
            self._advanced(source_label, history)
        self.setup_path_var.trace("w", self._setup_path_changed)

        test_card = self._card(parent)
        head = ttk.Frame(test_card, style="Card.TFrame")
        head.pack(fill="x")
        ttk.Label(head, text="Test", style="CardTitle.TLabel").pack(side="left")
        ttk.Label(head, text="Each run optimizes one Test; a single Test is selected automatically.", style="Help.TLabel").pack(side="left", padx=10)
        self.test_choice_var = tk.StringVar(value="")
        self.test_combo = ttk.Combobox(test_card, textvariable=self.test_choice_var, values=(), state="disabled")
        self.test_combo.pack(fill="x", pady=(10, 0))
        self.test_combo.bind("<<ComboboxSelected>>", self._test_selected)

        cell_card = self._card(parent)
        head = ttk.Frame(cell_card, style="Card.TFrame")
        head.pack(fill="x")
        ttk.Label(head, text="Cell Hierarchy", style="CardTitle.TLabel").pack(side="left")
        self.open_config_button = ttk.Button(head, text="Open Config", style="Ghost.TButton",
                                             command=self.open_private_config, state="disabled")
        self.open_config_button.pack(side="right")
        self._advanced(self.open_config_button)
        ttk.Button(head, text="Clear", style="Chip.TButton", command=lambda: self.select_all_cells(False)).pack(side="right", padx=4)
        ttk.Button(head, text="Select All", style="Chip.TButton", command=lambda: self.select_all_cells(True)).pack(side="right")
        self.cell_scope_summary_var = tk.StringVar(
            value="Select a Test to load its cell hierarchy, then select cells and Apply to inspect parameters.")
        ttk.Label(cell_card, textvariable=self.cell_scope_summary_var, style="CardMuted.TLabel", wraplength=980).pack(
            anchor="w", pady=(6, 8))
        tree_frame = ttk.Frame(cell_card, style="Card.TFrame")
        tree_frame.pack(fill="both", expand=True)
        self.cell_scope_tree = core.FastTreeview(tree_frame, columns=("library", "cell", "path"), show="tree headings", height=10)
        self.cell_scope_tree.heading("#0", text="Cell (includes its subcells)")
        for column, label, width in (("library", "Library", 170), ("cell", "Cell", 200), ("path", "Instance path", 320)):
            self.cell_scope_tree.heading(column, text=label)
            self.cell_scope_tree.column(column, width=width)
        self.cell_scope_tree.column("#0", width=300)
        scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.cell_scope_tree.yview)
        self.cell_scope_tree.configure(yscrollcommand=scroll.set)
        self.cell_scope_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.cell_scope_tree.bind("<Button-1>", self._cell_scope_click)
        self.cell_scope_tree.bind("<space>", self._cell_scope_space)
        actions = ttk.Frame(cell_card, style="Card.TFrame")
        actions.pack(fill="x", pady=(12, 0))
        self.apply_cells_button = ttk.Button(actions, text="Apply Selected Cells  →", style="Accent.TButton",
                                             command=self.apply_selected_cells, state="disabled")
        self.apply_cells_button.pack(side="left")
        self.private_setup_var = tk.StringVar(value="The private Maestro / config is created by Load and Inspect; the original design stays read-only.")
        private_label = ttk.Label(actions, textvariable=self.private_setup_var, style="Help.TLabel", wraplength=700,
                                  justify="left")
        private_label.pack(side="left", padx=14, fill="x", expand=True)
        self._advanced(private_label)

        suite_card = self._card(parent)
        ttk.Label(suite_card, text="Private Suites", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(suite_card, text="A suite stores the inspected Test, cells, parameters and configuration; load it whole next time, update parts of it, rename or delete it.",
                  style="CardMuted.TLabel", wraplength=980).pack(anchor="w", pady=(4, 8))
        ttk.Button(suite_card, text="Manage / Load Private Suites…", command=self.open_private_suites).pack(anchor="w")
        self._advanced(suite_card)

        config_card = self._card(parent, padding=(18, 12))
        row = ttk.Frame(config_card, style="Card.TFrame")
        row.pack(fill="x")
        ttk.Label(row, text="Configuration", style="CardTitle.TLabel").pack(side="left")
        ttk.Label(row, text="One JSON file keeps every setting, even unfinished ones; loading restores everything without revisiting each page.",
                  style="Help.TLabel").pack(side="left", padx=10)
        buttons = ttk.Frame(config_card, style="Card.TFrame")
        buttons.pack(fill="x", pady=(10, 0))
        self.save_config_button = ttk.Button(buttons, text="Save Configuration...", command=self.save_configuration)
        self.save_config_button.pack(side="left")
        self.load_config_button = ttk.Button(buttons, text="Load Configuration...", command=self.load_configuration)
        self.load_config_button.pack(side="left", padx=6)
        self.restore_last_button = ttk.Button(buttons, text="Restore Last", command=self.restore_last_ui_state)
        self.restore_last_button.pack(side="left")
        details = ttk.Button(buttons, text="Match Details…", style="Ghost.TButton", command=lambda: self._show_text_details(
            "Configuration Match Details", getattr(self, "settings_match_detail", "No configuration loaded.")))
        details.pack(side="right")
        self._advanced(details)
        self.save_state_button = self.save_config_button
        self.load_state_button = self.load_config_button
        self.settings_status_var = tk.StringVar(value="Save the configuration at any time; after a successful load you can run straight away.")
        self.config_message_var = self.settings_status_var
        ttk.Label(config_card, textvariable=self.settings_status_var, style="Help.TLabel", wraplength=980).pack(
            anchor="w", pady=(8, 0))
        ttk.Frame(parent, style="Page.TFrame", height=18).pack(fill="x")

    # -------------------------------------------------------- parameter page
    def _build_parameters_page(self, parent):
        self._page_heading(parent, "Parameters", "")
        toolbar = self._card(parent, padding=(14, 12))
        self.parameter_view_var = tk.StringVar(value="hierarchy")
        self.parameter_filter_var = tk.StringVar(value="")
        search = ttk.Frame(toolbar, style="Card.TFrame")
        search.pack(side="left", fill="x", expand=True)
        ttk.Label(search, text="⌕", style="CardMuted.TLabel", font=(T.FONTS["family"], 13)).pack(side="left", padx=(2, 6))
        filter_entry = ttk.Entry(search, textvariable=self.parameter_filter_var, width=24)
        filter_entry.pack(side="left", fill="x", expand=True)
        self.parameter_filter_var.trace("w", self._parameter_filter_changed)
        self.parameter_view_switch = ParameterViewSwitch(toolbar, self.parameter_view_var,
                                                              command=self._parameter_view_changed)
        self.parameter_view_switch.pack(side="left", padx=(12, 6))
        chips = ttk.Frame(toolbar, style="Card.TFrame")
        chips.pack(side="left")
        self.parameter_all_button = ttk.Button(chips, text="Select All", style="Chip.TButton",
                                               command=lambda: self.select_all_parameters(True, self.parameter_all_button))
        self.parameter_all_button.pack(side="left", padx=2)
        self.parameter_none_button = ttk.Button(chips, text="Clear", style="Chip.TButton",
                                                command=lambda: self.select_all_parameters(False, self.parameter_none_button))
        self.parameter_none_button.pack(side="left", padx=2)
        self.width_all_button = ttk.Button(chips, text="W All", style="Chip.TButton",
                                           command=lambda: self.select_semantic("width", True, self.width_all_button))
        self.width_all_button.pack(side="left", padx=(10, 2))
        self.width_none_button = ttk.Button(chips, text="W None", style="Chip.TButton",
                                            command=lambda: self.select_semantic("width", False, self.width_none_button))
        self.width_none_button.pack(side="left", padx=2)
        self.length_all_button = ttk.Button(chips, text="L All", style="Chip.TButton",
                                            command=lambda: self.select_semantic("length", True, self.length_all_button))
        self.length_all_button.pack(side="left", padx=(10, 2))
        self.length_none_button = ttk.Button(chips, text="L None", style="Chip.TButton",
                                             command=lambda: self.select_semantic("length", False, self.length_none_button))
        self.length_none_button.pack(side="left", padx=2)
        ttk.Button(chips, text="Expand", style="Ghost.TButton", command=lambda: self._set_tree_open(True)).pack(side="left", padx=(10, 0))
        ttk.Button(chips, text="Collapse", style="Ghost.TButton", command=lambda: self._set_tree_open(False)).pack(side="left")

        validation = self._card(parent, padding=(14, 10))
        self.validate_button = ttk.Button(validation, text="Check Selected Ranges", command=self.validate_all_ranges, state="disabled")
        self.validate_button.pack(side="left")
        self.apply_ranges_button = ttk.Button(validation, text="Apply Legal Ranges", command=self.apply_validated_ranges,
                                              state="disabled")
        self.apply_ranges_button.pack(side="left", padx=6)
        self.cancel_validation_button = ttk.Button(validation, text="Cancel Check", command=self.cancel_optional_validation,
                                                   state="disabled")
        self.cancel_validation_button.pack(side="left", padx=4)
        ttk.Button(validation, text="Details…", style="Ghost.TButton", command=self.show_validation_details).pack(side="right")
        self.range_status_var = tk.StringVar(value="Static and optional PDK callback validation have not run.")
        ttk.Label(validation, textvariable=self.range_status_var, style="CardMuted.TLabel", wraplength=560).pack(
            side="left", padx=10, fill="x", expand=True)
        self._advanced(validation)

        tree_card = self._card(parent, padding=(10, 10))
        columns = ("current", "minimum", "maximum", "unit", "scale", "validation")
        self.parameter_tree = core.FastTreeview(tree_card, columns=columns, show="tree headings", selectmode="browse",
                                                height=22)
        headings = {"#0": "Hierarchy / Parameter", "current": "Current", "minimum": "Minimum", "maximum": "Maximum",
                    "unit": "Unit", "scale": "Scale", "validation": "PDK Validation"}
        widths = {"#0": 440, "current": 120, "minimum": 110, "maximum": 110, "unit": 60, "scale": 72, "validation": 200}
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
        ttk.Label(tree_card, text="Click a checkbox to select a parameter or a scale value to toggle linear/log; double-click Minimum or Maximum to edit the range.",
                  style="Help.TLabel").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.parameter_summary_var = tk.StringVar(value="No hierarchy loaded.")
        ttk.Label(parent, textvariable=self.parameter_summary_var, style="Muted.TLabel").pack(anchor="w", padx=30, pady=(2, 16))
    def _refresh_parameter_selection_display(self, *args, **kwargs):
        result = _Base._refresh_parameter_selection_display(self, *args, **kwargs)
        self._update_nav_progress()
        return result

    def _build_metrics_page(self, parent):
        self._page_heading(parent, "Objectives & Constraints", "")
        from crescendo_engine.gui_execution_selection import ExecutionSelectionPanel
        objective_card = self._card(parent)
        heading = ttk.Frame(objective_card, style="Card.TFrame")
        heading.pack(fill="x")
        ttk.Label(heading, text="Objectives", style="CardTitle.TLabel").pack(side="left")
        ttk.Label(heading, text="e.g. maximize gain, minimize power", style="Help.TLabel").pack(side="left", padx=10)
        ttk.Button(heading, text="+ Add Objective", style="Accent.TButton", command=self.add_objective).pack(side="right")
        self.objective_container = ttk.Frame(objective_card, style="Card.TFrame")
        self.objective_container.pack(fill="x", pady=(10, 0))

        constraint_card = self._card(parent)
        heading = ttk.Frame(constraint_card, style="Card.TFrame")
        heading.pack(fill="x")
        ttk.Label(heading, text="Constraints", style="CardTitle.TLabel").pack(side="left")
        ttk.Label(heading, text="e.g. phase margin ≥ 60°", style="Help.TLabel").pack(side="left", padx=10)
        ttk.Button(heading, text="+ Add Constraint", command=self.add_constraint).pack(side="right")
        self.constraint_container = ttk.Frame(constraint_card, style="Card.TFrame")
        self.constraint_container.pack(fill="x", pady=(10, 0))

        explain = self._card(parent, padding=(18, 12))
        ttk.Label(explain, text="Tip", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(explain, text=(
            "Each metric can use one or more corners. Aggregation comes first: \"minimize + max\" pushes down the worst selected corner; "
            "\"mean\" weights every native point equally. Two or more objectives use the NSGA-II multi-objective algorithm automatically."),
            style="CardMuted.TLabel", wraplength=980, justify="left").pack(anchor="w", pady=(4, 0))

        selection_card = self._card(parent)
        self.execution_selection_panel = ExecutionSelectionPanel(selection_card, self._execution_selection_changed)
        self.execution_selection_panel.pack(fill="both", expand=True)
        self._advanced(selection_card)
        self.analysis_summary_var = tk.StringVar(value="Load a setup to discover outputs and corners.")
        ttk.Label(parent, textvariable=self.analysis_summary_var, style="Muted.TLabel", wraplength=980).pack(
            anchor="w", padx=30, pady=(4, 16))

    # ----------------------------------------------------- optimization page
    def _build_optimization_page(self, parent):
        self._page_heading(parent, "Optimization", "")
        settings = self._card(parent)
        ttk.Label(settings, text="Run Settings", style="CardTitle.TLabel").grid(row=0, column=0, columnspan=6, sticky="w",
                                                                         pady=(0, 10))
        self.generations_var = tk.StringVar(value="20")
        self.jobs_var = tk.StringVar(value="1")
        self.timeout_var = tk.StringVar(value="600")
        self.algorithm_var = tk.StringVar(value="differential_evolution")
        ttk.Label(settings, text="Generations", style="Card.TLabel").grid(row=1, column=0, sticky="w", padx=(0, 8))
        ttk.Entry(settings, textvariable=self.generations_var, width=8).grid(row=1, column=1, sticky="w", padx=(0, 24))
        ttk.Label(settings, text="Algorithm", style="Card.TLabel").grid(row=1, column=2, sticky="w", padx=(0, 8))
        self.algorithm_combo = ttk.Combobox(settings, textvariable=self.algorithm_var,
                                            values=tuple(ALGORITHM_NAMES.keys()), state="readonly", width=26)
        self.algorithm_combo.grid(row=1, column=3, sticky="w")
        self.algorithm_combo.bind("<<ComboboxSelected>>", self._algorithm_changed)
        self.algorithm_notice_var = tk.StringVar(value="")
        ttk.Label(settings, textvariable=self.algorithm_notice_var, style="Help.TLabel", wraplength=420).grid(
            row=1, column=4, sticky="w", padx=14)
        settings.columnconfigure(4, weight=1)

        advanced = self._card(parent)
        self.advanced_visible = tk.BooleanVar(value=False)
        ttk.Checkbutton(advanced, text="Show advanced algorithm parameters", variable=self.advanced_visible,
                        command=self._toggle_advanced).pack(anchor="w")
        self.algorithm_parameter_frame = ttk.Frame(advanced, style="Card.TFrame")
        self.algorithm_parameter_vars = {}
        self.algorithm_parameter_manual = set()
        for algorithm, fields in ALGORITHM_FIELDS.items():
            for field in fields:
                self.algorithm_parameter_vars[(algorithm, field)] = tk.StringVar(value="")
        self._refresh_algorithm_defaults(force=True)
        ttk.Button(advanced, text="Reset calculated defaults", style="Ghost.TButton",
                   command=lambda: self._refresh_algorithm_defaults(force=True)).pack(anchor="w", pady=(6, 0))
        self._advanced(advanced)

        dash = ttk.Frame(parent, style="Page.TFrame")
        dash.pack(fill="x", padx=28, pady=(7, 0))
        self.kpi_tiles = {}
        for index, (key, title, color) in enumerate((
                ("evaluated", "Evaluated", T.ACCENT), ("feasible", "Feasible", T.SUCCESS),
                ("best", "Best objective", T.INDIGO), ("generation", "Generation", T.TEAL))):
            tile = W.KpiTile(dash, title, color=color)
            tile.grid(row=0, column=index, sticky="nsew", padx=(0 if index == 0 else 7, 0))
            dash.columnconfigure(index, weight=1, uniform="kpi")
            self.kpi_tiles[key] = tile

        chart_card = self._card(parent, padding=(16, 12))
        head = ttk.Frame(chart_card, style="Card.TFrame")
        head.pack(fill="x")
        ttk.Label(head, text="Convergence", style="CardTitle.TLabel").pack(side="left")
        self.run_state_var = tk.StringVar(value="Waiting for setup.")
        ttk.Label(head, textvariable=self.run_state_var, style="Help.TLabel", wraplength=640).pack(side="right")
        self.convergence_chart = W.ConvergenceChart(chart_card, height=230)
        self.convergence_chart.pack(fill="x", pady=(8, 0))
        self.progress_summary_var = tk.StringVar(value="No run data.")
        ttk.Label(chart_card, textvariable=self.progress_summary_var, style="Help.TLabel", wraplength=980).pack(
            anchor="w", pady=(8, 0))

        actions = self._card(parent, padding=(14, 10))
        ttk.Button(actions, text="Load Checkpoint...", command=self.load_checkpoint).pack(side="left")
        ttk.Label(actions, text="Checkpoint updates automatically after every completed candidate and generation.",
                  style="Help.TLabel").pack(side="left", padx=12)
        self._advanced(actions)

        recent = self._card(parent)
        header = ttk.Frame(recent, style="Card.TFrame")
        header.pack(fill="x")
        ttk.Label(header, text="Recent Runs", style="CardTitle.TLabel").pack(side="left")
        ttk.Button(header, text="Refresh", style="Ghost.TButton", command=self.refresh_recent_runs).pack(side="right")
        self.recent_tree = ttk.Treeview(
            recent, columns=("generation", "success", "failures", "state", "started", "checkpoint", "active"),
            show="tree headings", height=6, selectmode="browse")
        for column, label, width in (("#0", "Run Name", 240), ("generation", "Generation", 100), ("success", "Success", 70),
                                     ("failures", "Failed", 170), ("state", "State", 110), ("started", "Started", 140),
                                     ("checkpoint", "Resume", 70), ("active", "Active", 70)):
            self.recent_tree.heading(column, text=label)
            self.recent_tree.column(column, width=width, stretch=column == "#0")
        self.recent_tree.pack(fill="x", pady=(10, 8))
        self.recent_tree.bind("<Double-1>", lambda _event: self.resume_recent_run())
        recent_actions = ttk.Frame(recent, style="Card.TFrame")
        recent_actions.pack(fill="x")
        ttk.Button(recent_actions, text="Resume Selected", command=self.resume_recent_run).pack(side="left")
        ttk.Button(recent_actions, text="View Progress / Logs", style="Ghost.TButton", command=self.view_recent_run).pack(side="left", padx=8)
        ttk.Frame(parent, style="Page.TFrame", height=18).pack(fill="x")

    def _objective_series(self, index=0):
        spec = self._display_spec() if hasattr(self, "_display_spec") else {}
        objectives = (spec or {}).get("objectives") or []
        if not objectives:
            return None
        definition = objectives[min(index, len(objectives) - 1)]
        minimize = (definition.get("direction") or "minimize") != "maximize"
        samples, by_generation = [], {}
        for row in self._display_candidates():
            value = metric_value(row, index)
            if value is None:
                continue
            generation = int(row.get("generation") or 0) + 1
            samples.append((generation, value))
            current = by_generation.get(generation)
            if current is None or (value < current if minimize else value > current):
                by_generation[generation] = value
        best, running = [], None
        for generation in sorted(by_generation):
            value = by_generation[generation]
            running = value if running is None else (min(running, value) if minimize else max(running, value))
            best.append((generation, running))
        label = definition.get("output_key") or "objective"
        return samples, best, label, minimize

    def _render_dashboard(self):
        if not hasattr(self, "kpi_tiles"):
            return
        candidates = self._display_candidates()
        done = [row for row in candidates if row.get("status") in ("completed", "failed", "infrastructure_failed")]
        feasible = sum(1 for row in candidates if row.get("status") == "completed" and
                       (row.get("metrics") or {}).get("feasible") is True)
        generation = max([int(row.get("generation") or 0) + 1 for row in candidates] or [0])
        try:
            total_generations = int(self.generations_var.get())
        except (TypeError, ValueError):
            total_generations = None
        series = self._objective_series(0)
        self.kpi_tiles["evaluated"].set(str(len(done)), "{} candidates".format(len(candidates)) if candidates else "not started")
        self.kpi_tiles["feasible"].set(str(feasible), "{:.0%} feasible".format(feasible / float(len(done))) if done else "—")
        if series and series[1]:
            self.kpi_tiles["best"].set(W.fmt(series[1][-1][1]), "{} · {}".format(
                series[2], "lower is better" if series[3] else "higher is better"))
        else:
            self.kpi_tiles["best"].set("—", "waiting for the first result")
        self.kpi_tiles["generation"].set(str(generation) if generation else "—",
                                         "of {}".format(total_generations) if total_generations else "")
        if series:
            self.convergence_chart.update_series(series[0], series[1], series[2], series[3])
        else:
            self.convergence_chart.update_series([], [], "", True)

    # ------------------------------------------------------------ results page
    def _build_results_page(self, parent):
        self._page_heading(parent, "Live Results", "")
        toolbar = self._card(parent, padding=(14, 10))
        self.export_csv_button = ttk.Button(toolbar, text="Export CSV...", command=self.export_csv, state="disabled")
        self.export_csv_button.pack(side="left")
        self.export_data_button = ttk.Button(toolbar, text="Export Results...", command=self.export_results_data, state="disabled")
        self.export_data_button.pack(side="left", padx=6)
        self.load_data_button = ttk.Button(toolbar, text="Load Results...", style="Ghost.TButton", command=self.load_results_data)
        self.load_data_button.pack(side="left")
        self.live_data_button = ttk.Button(toolbar, text="Show Current Run", style="Ghost.TButton", command=self.show_current_results)
        self.live_data_button.pack(side="left", padx=6)
        self.results_summary_var = tk.StringVar(value="No candidate data.")
        ttk.Label(toolbar, textvariable=self.results_summary_var, style="CardMuted.TLabel").pack(side="left", padx=12)
        ttk.Button(toolbar, text="Next", style="Ghost.TButton", command=lambda: self.change_result_page(1)).pack(side="right")
        ttk.Button(toolbar, text="Previous", style="Ghost.TButton", command=lambda: self.change_result_page(-1)).pack(side="right")
        self.result_page_var = tk.StringVar(value="Page 1")
        ttk.Label(toolbar, textvariable=self.result_page_var, style="CardMuted.TLabel").pack(side="right", padx=8)

        self.results_failure_var = tk.StringVar(value="")
        ttk.Label(parent, textvariable=self.results_failure_var, foreground=T.DANGER, background=T.BACKGROUND,
                  wraplength=980, justify="left").pack(fill="x", padx=30, pady=(0, 4))

        trend_card = self._card(parent, padding=(16, 12))
        ttk.Label(trend_card, text="Convergence per objective · dots are samples, the line is best so far", style="CardTitle.TLabel").pack(anchor="w")
        self.result_trend_frame = ttk.Frame(trend_card, style="Card.TFrame")
        self.result_trend_frame.pack(fill="x", pady=(6, 0))
        self.result_trend_charts = []
        # Kernel callers only check that the trend area exists and is mapped.
        self.result_trend = self.result_trend_frame

        result_card = self._card(parent, padding=(10, 10))
        columns = ("generation", "status", "feasible", "objectives", "constraints")
        self.result_tree = core.FastTreeview(result_card, columns=columns, show="tree headings", height=16, selectmode="browse")
        headings = {"#0": "Candidate", "generation": "Generation", "status": "State", "feasible": "Feasible",
                    "objectives": "Objectives", "constraints": "Constraints"}
        widths = {"#0": 200, "generation": 95, "status": 240, "feasible": 70, "objectives": 270, "constraints": 270}
        for column, label in headings.items():
            self.result_tree.heading(column, text=label)
            self.result_tree.column(column, width=widths[column], stretch=column in ("#0", "objectives", "constraints"))
        self.result_tree.tag_configure("feasible", foreground=T.TEXT)
        self.result_tree.tag_configure("infeasible", foreground=T.MUTED)
        self.result_tree.tag_configure("failed", foreground=T.DANGER)
        self.result_tree.tag_configure("pareto", foreground=T.ACCENT_TEXT)
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
        self._result_rows = {}

        detail = self._card(parent)
        detail_header = ttk.Frame(detail, style="Card.TFrame")
        detail_header.pack(fill="x")
        ttk.Label(detail_header, text="Candidate Diagnostics & Variables", style="CardTitle.TLabel").pack(side="left")
        self.optimized_schematic_button = ttk.Button(detail_header, text="Generate Optimized Schematic Cellviews",
                                                     command=self.generate_optimized_schematic, state="disabled")
        self.optimized_schematic_button.pack(side="right")
        self._advanced(self.optimized_schematic_button)
        self.optimized_schematic_status_var = tk.StringVar(value="Select a completed real candidate after optimization stops.")
        status_label = ttk.Label(detail, textvariable=self.optimized_schematic_status_var, style="Help.TLabel",
                                 wraplength=980, justify="left")
        status_label.pack(fill="x", pady=(6, 0))
        self._advanced(status_label)
        self.candidate_detail = tk.Text(detail, height=12, wrap="none", state="disabled", background=T.PANEL_ALT,
                                        foreground=T.TEXT, font=T.FONTS["mono"], relief="flat", padx=10, pady=8,
                                        highlightthickness=0, borderwidth=0)
        self.candidate_detail.pack(fill="x", pady=(8, 0))
        ttk.Frame(parent, style="Page.TFrame", height=18).pack(fill="x")

    def _render_result_trend(self):
        frame = getattr(self, "result_trend_frame", None)
        if frame is None or not hasattr(self, "result_trend_charts"):
            return _Base._render_result_trend(self)
        objectives = (self._display_spec() or {}).get("objectives") or []
        count = max(1, len(objectives))
        while len(self.result_trend_charts) < count:
            chart = W.ConvergenceChart(frame, height=190 if count > 1 else 230,
                                       empty_text="No measured objective values yet")
            chart.pack(fill="x", pady=(4, 4))
            self.result_trend_charts.append(chart)
        while len(self.result_trend_charts) > count:
            self.result_trend_charts.pop().destroy()
        for index, chart in enumerate(self.result_trend_charts):
            series = self._objective_series(index) if objectives else None
            if series:
                chart.update_series(*series)
            else:
                chart.update_series([], [], "", True)

    def render_results(self):
        """Incremental table update: rows are inserted once and edited in place."""
        self._render_result_trend()
        candidates = self._display_candidates()
        count = len(candidates)
        dataset = getattr(self, "display_dataset", None)
        if dataset is not None:
            feasible = sum(1 for row in candidates if (row.get("metrics") or {}).get("feasible") is True)
            self.results_summary_var.set("Loaded: {} candidates | {} feasible".format(count, feasible))
        pages = max(1, int((count + self.result_page_size - 1) / self.result_page_size))
        self.result_page = min(self.result_page, pages - 1)
        start = self.result_page * self.result_page_size
        rows = candidates[start:start + self.result_page_size]
        self.result_page_var.set("Page {} / {}".format(self.result_page + 1, pages))
        pareto = getattr(self, "pareto_ids", set()) or set()
        wanted = []
        cache = getattr(self, "_result_rows", {})
        tree = self.result_tree
        existing = tree.get_children("")
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
            values = (int(row.get("generation") or 0) + 1, tr(candidate_status(row)), feasible,
                      compact_metric_values(row, "objectives"), compact_metric_values(row, "constraints"))
            if state in ("failed", "infrastructure_failed"):
                tag = "failed"
            elif candidate_id in pareto:
                tag = "pareto"
            elif feasible == "No":
                tag = "infeasible"
            else:
                tag = "feasible"
            text = ("★ " if candidate_id in pareto else "") + candidate_id
            wanted.append(candidate_id)
            signature = (values, tag, text)
            if cache.get(candidate_id) == signature and tree.exists(candidate_id):
                continue
            if tree.exists(candidate_id):
                tree.item(candidate_id, text=text, values=values, tags=(tag,))
            else:
                tree.insert("", "end", iid=candidate_id, text=text, values=values, tags=(tag,))
            cache[candidate_id] = signature
        wanted_set = set(wanted)
        stale = [item for item in existing if item not in wanted_set]
        if stale:
            tree.delete(*stale)
            for item in stale:
                cache.pop(item, None)
        if list(tree.get_children("")) != wanted:
            for index, item in enumerate(wanted):
                tree.move(item, "", index)
        self._result_rows = cache
        if self.selected_candidate_id in wanted_set:
            if tuple(tree.selection()) != (self.selected_candidate_id,):
                tree.selection_set(self.selected_candidate_id)
        self._render_candidate_detail()
        self._render_dashboard()

    def _build_pareto_page(self, parent):
        self._page_heading(parent, "Pareto Trade-offs", "")
        self.pareto_notice_var = tk.StringVar(value="Add at least two objectives to enable pairwise trade-offs.")
        ttk.Label(parent, textvariable=self.pareto_notice_var, style="Muted.TLabel", wraplength=980).pack(
            anchor="w", padx=30, pady=(4, 8))
        self.pareto_container = ttk.Frame(parent, style="Page.TFrame")
        self.pareto_container.pack(fill="both", expand=True, padx=28, pady=(0, 18))
        self.pareto_plots = []

    # --------------------------------------------------------- live updates
    def add_objective(self, *args, **kwargs):
        result = _Base.add_objective(self, *args, **kwargs)
        self._translate_static_texts(self.objective_container)
        return result

    def add_constraint(self, *args, **kwargs):
        result = _Base.add_constraint(self, *args, **kwargs)
        self._translate_static_texts(self.constraint_container)
        return result

    def _render_progress(self, status):
        _Base._render_progress(self, status)
        self._update_nav_progress()
        if self.current_page == "Optimization":
            self._render_dashboard()

    def _set_prepared(self, *args, **kwargs):
        result = _Base._set_prepared(self, *args, **kwargs)
        self._update_nav_progress()
        return result

    def _metric_count_changed(self, *args, **kwargs):
        result = _Base._metric_count_changed(self, *args, **kwargs)
        self._update_nav_progress()
        return result



def install():
    """Make the kernel's launch path build Crescendo instead of the Crescendo window."""
    _install_palette()
    if i18n.ENABLED:
        for name in ("messagebox", "filedialog", "simpledialog"):
            module = getattr(core, name)
            if not isinstance(module, _DisplayDialogs):
                setattr(core, name, _DisplayDialogs(module))
        _install_text_display()
    core.OptimizerGui = CrescendoGui
    core.ParetoPlot = ParetoPlot
    return CrescendoGui


def launch_gui(config, service=None, launch_context=None, force_exit_process=False):
    install()
    return core.launch_gui(config, service=service, launch_context=launch_context,
                           force_exit_process=force_exit_process)
