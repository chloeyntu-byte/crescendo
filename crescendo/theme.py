"""Crescendo visual system: palette, fonts and ttk styles (Tk 8.5 compatible)."""
from __future__ import print_function

try:
    import tkinter as tk
    from tkinter import font as tkfont, ttk
except ImportError:  # pragma: no cover - Python 2 is not supported, kept for import symmetry
    import Tkinter as tk
    import tkFont as tkfont
    import ttk

# --- palette -----------------------------------------------------------------
INK = "#14161b"          # sidebar / brand surfaces
INK_2 = "#1d2027"
INK_3 = "#272b34"
INK_LINE = "#2e333d"
INK_TEXT = "#eceef2"
INK_MUTED = "#8a909c"

BACKGROUND = "#f2f3f6"
PANEL = "#ffffff"
PANEL_ALT = "#f8f9fb"
BORDER = "#e2e5ea"
BORDER_STRONG = "#cfd4dc"
TEXT = "#1a1d23"
MUTED = "#6b7280"
FAINT = "#9aa1ad"

ACCENT = "#ff6a2b"        # Crescendo orange
ACCENT_HOVER = "#ff8150"
ACCENT_PRESSED = "#e5561b"
ACCENT_SOFT = "#fff1ea"
ACCENT_TEXT = "#c2410c"

INDIGO = "#5b5bd6"
TEAL = "#0f9f8f"
SUCCESS = "#16a34a"
SUCCESS_SOFT = "#e8f7ee"
WARNING = "#c27803"
WARNING_SOFT = "#fdf5e6"
DANGER = "#dc2626"
DANGER_SOFT = "#fdecec"

CHART = ("#ff6a2b", "#5b5bd6", "#0f9f8f", "#e0457b", "#d4a017",
         "#3b82f6", "#8b5cf6", "#10b981", "#f97316", "#64748b")
CHART_GRID = "#eef0f4"

# --- fonts -------------------------------------------------------------------
# Latin UI first; fontconfig falls back to a CJK face for any CJK path or name.
_SANS_CANDIDATES = (
    "Inter", "Open Sans", "Noto Sans", "Cantarell", "Segoe UI", "Helvetica Neue",
    "DejaVu Sans", "Liberation Sans", "WenQuanYi Micro Hei",
)
_MONO_CANDIDATES = (
    "JetBrains Mono", "DejaVu Sans Mono", "Liberation Mono", "WenQuanYi Micro Hei Mono",
)
FONTS = {}


def _pick(families, candidates, fallback):
    lowered = dict((name.lower(), name) for name in families)
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return fallback


def init_fonts(root):
    try:
        families = tkfont.families(root)
    except tk.TclError:
        families = ()
    sans = _pick(families, _SANS_CANDIDATES, "TkDefaultFont")
    mono = _pick(families, _MONO_CANDIDATES, "TkFixedFont")
    FONTS.update(
        family=sans, mono_family=mono,
        body=(sans, 10), small=(sans, 9), tiny=(sans, 8),
        strong=(sans, 10, "bold"), title=(sans, 11, "bold"),
        h1=(sans, 19, "bold"), h2=(sans, 13, "bold"), hero=(sans, 24, "bold"),
        kpi=(sans, 20, "bold"), brand=(sans, 15, "bold"), nav=(sans, 10),
        nav_strong=(sans, 10, "bold"), mono=(mono, 9), mono_small=(mono, 8),
    )
    # Named Tk fonts keep un-themed tk widgets (Text, Listbox, menus) consistent.
    for name, spec in (("TkDefaultFont", FONTS["body"]), ("TkTextFont", FONTS["body"]),
                       ("TkMenuFont", FONTS["body"]), ("TkHeadingFont", FONTS["strong"]),
                       ("TkFixedFont", FONTS["mono"]), ("TkTooltipFont", FONTS["small"])):
        try:
            named = tkfont.nametofont(name)
            named.configure(family=spec[0], size=spec[1],
                            weight="bold" if len(spec) > 2 and spec[2] == "bold" else "normal")
        except tk.TclError:
            pass
    return FONTS


def configure_styles(root):
    init_fonts(root)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    f = FONTS
    root.option_add("*TCombobox*Listbox.font", "TkDefaultFont")
    root.option_add("*TCombobox*Listbox.background", PANEL)
    root.option_add("*TCombobox*Listbox.selectBackground", ACCENT_SOFT)
    root.option_add("*TCombobox*Listbox.selectForeground", TEXT)
    root.option_add("*Menu.background", PANEL)
    root.option_add("*Menu.activeBackground", ACCENT_SOFT)
    root.option_add("*Menu.activeForeground", TEXT)
    root.option_add("*Menu.relief", "flat")

    style.configure(".", background=BACKGROUND, foreground=TEXT, font=f["body"],
                    bordercolor=BORDER, lightcolor=PANEL, darkcolor=BORDER,
                    troughcolor=BACKGROUND, focuscolor=ACCENT, selectbackground=ACCENT_SOFT,
                    selectforeground=TEXT, insertcolor=TEXT)
    # Frames
    style.configure("TFrame", background=BACKGROUND)
    style.configure("Page.TFrame", background=BACKGROUND)
    style.configure("Card.TFrame", background=PANEL, relief="flat", borderwidth=0)
    style.configure("Sidebar.TFrame", background=INK)
    style.configure("Topbar.TFrame", background=PANEL)
    # Labels
    style.configure("TLabel", background=BACKGROUND, foreground=TEXT, font=f["body"])
    style.configure("Muted.TLabel", background=BACKGROUND, foreground=MUTED)
    style.configure("Card.TLabel", background=PANEL, foreground=TEXT)
    style.configure("CardMuted.TLabel", background=PANEL, foreground=MUTED)
    style.configure("Help.TLabel", background=PANEL, foreground=FAINT, font=f["small"])
    style.configure("PageTitle.TLabel", background=BACKGROUND, foreground=TEXT, font=f["h1"])
    style.configure("CardTitle.TLabel", background=PANEL, foreground=TEXT, font=f["title"])
    style.configure("Status.TLabel", background=PANEL, foreground=MUTED, padding=(12, 6), font=f["small"])
    style.configure("Eyebrow.TLabel", background=BACKGROUND, foreground=ACCENT_TEXT, font=(f["family"], 9, "bold"))
    # Buttons: neutral, accent (primary), danger, ghost, sidebar
    common = dict(padding=(12, 6), relief="raised", borderwidth=1, focusthickness=0, font=f["body"])
    style.configure("TButton", background=PANEL, foreground=TEXT, bordercolor=BORDER_STRONG,
                    lightcolor=PANEL, darkcolor=PANEL, **common)
    style.map("TButton",
              background=[("disabled", PANEL_ALT), ("pressed", "#eceff3"), ("active", "#f4f6f9")],
              foreground=[("disabled", FAINT)],
              bordercolor=[("active", "#b9c0cb"), ("disabled", BORDER)])
    style.configure("Accent.TButton", background=ACCENT, foreground="#ffffff", bordercolor=ACCENT,
                    lightcolor=ACCENT, darkcolor=ACCENT, **dict(common, font=f["strong"]))
    style.map("Accent.TButton",
              background=[("disabled", "#f3c9b5"), ("pressed", ACCENT_PRESSED), ("active", ACCENT_HOVER)],
              bordercolor=[("disabled", "#f3c9b5"), ("pressed", ACCENT_PRESSED), ("active", ACCENT_HOVER)],
              lightcolor=[("disabled", "#f3c9b5"), ("active", ACCENT_HOVER)],
              darkcolor=[("disabled", "#f3c9b5"), ("active", ACCENT_HOVER)],
              foreground=[("disabled", "#ffffff")])
    style.configure("Hero.TButton", background=ACCENT, foreground="#ffffff", bordercolor=ACCENT,
                    lightcolor=ACCENT, darkcolor=ACCENT, padding=(18, 9), relief="flat",
                    font=(f["family"], 11, "bold"))
    style.map("Hero.TButton",
              background=[("disabled", "#f3c9b5"), ("pressed", ACCENT_PRESSED), ("active", ACCENT_HOVER)],
              bordercolor=[("disabled", "#f3c9b5"), ("active", ACCENT_HOVER)],
              lightcolor=[("disabled", "#f3c9b5"), ("active", ACCENT_HOVER)],
              darkcolor=[("disabled", "#f3c9b5"), ("active", ACCENT_HOVER)],
              foreground=[("disabled", "#ffffff")])
    style.configure("Danger.TButton", background=PANEL, foreground=DANGER, bordercolor="#f0c4c4",
                    lightcolor=PANEL, darkcolor=PANEL, **common)
    style.map("Danger.TButton",
              background=[("disabled", PANEL_ALT), ("active", DANGER_SOFT)],
              foreground=[("disabled", FAINT)], bordercolor=[("disabled", BORDER)])
    style.configure("Ghost.TButton", background=PANEL, foreground=MUTED, bordercolor=PANEL,
                    lightcolor=PANEL, darkcolor=PANEL, padding=(8, 5), relief="flat", font=f["body"])
    style.map("Ghost.TButton", background=[("active", "#f1f3f6")], foreground=[("active", TEXT), ("disabled", FAINT)],
              bordercolor=[("active", "#f1f3f6")])
    style.configure("Chip.TButton", background=PANEL_ALT, foreground=TEXT, bordercolor=BORDER,
                    lightcolor=PANEL_ALT, darkcolor=PANEL_ALT, padding=(10, 3), relief="raised", font=f["small"])
    style.map("Chip.TButton", background=[("active", ACCENT_SOFT)], foreground=[("active", ACCENT_TEXT), ("disabled", FAINT)],
              bordercolor=[("active", "#ffd3bf")])
    style.configure("TopDanger.TButton", background=PANEL, foreground=MUTED, bordercolor=PANEL,
                    lightcolor=PANEL, darkcolor=PANEL, padding=(10, 7), relief="flat")
    style.map("TopDanger.TButton", background=[("active", DANGER_SOFT)], foreground=[("active", DANGER), ("disabled", FAINT)],
              bordercolor=[("active", DANGER_SOFT)])
    # Legacy sidebar names (still referenced by upstream helpers in tests)
    style.configure("Sidebar.TButton", background=INK, foreground=INK_TEXT, anchor="w", padding=(14, 9), relief="flat")
    style.configure("SidebarActive.TButton", background=INK_3, foreground="#ffffff", anchor="w", padding=(14, 9), relief="flat")
    # Inputs
    style.configure("TEntry", fieldbackground=PANEL, foreground=TEXT, bordercolor=BORDER_STRONG,
                    lightcolor=PANEL, darkcolor=PANEL, padding=(8, 5), insertcolor=TEXT)
    style.map("TEntry", bordercolor=[("focus", ACCENT)], lightcolor=[("focus", ACCENT_SOFT)],
              fieldbackground=[("readonly", PANEL_ALT), ("disabled", PANEL_ALT)],
              foreground=[("disabled", FAINT)])
    style.configure("Crescendo.Input.TEntry", fieldbackground=PANEL, padding=(8, 5))
    style.map("Crescendo.Input.TEntry", fieldbackground=[("disabled", PANEL_ALT)], foreground=[("disabled", FAINT)],
              bordercolor=[("focus", ACCENT)])
    style.configure("TCombobox", fieldbackground=PANEL, background=PANEL, foreground=TEXT,
                    bordercolor=BORDER_STRONG, lightcolor=PANEL, darkcolor=PANEL, arrowcolor=MUTED,
                    padding=(8, 4))
    style.map("TCombobox", fieldbackground=[("readonly", PANEL), ("disabled", PANEL_ALT)],
              foreground=[("disabled", FAINT)], bordercolor=[("focus", ACCENT), ("active", "#b9c0cb")],
              background=[("active", PANEL), ("pressed", PANEL)], arrowcolor=[("disabled", FAINT)])
    style.configure("TCheckbutton", background=PANEL, foreground=TEXT, indicatorcolor=PANEL,
                    indicatorbackground=PANEL, focusthickness=0)
    style.map("TCheckbutton", indicatorcolor=[("selected", ACCENT), ("disabled", PANEL_ALT)],
              background=[("active", PANEL)], foreground=[("disabled", FAINT)])
    style.configure("TRadiobutton", background=PANEL, foreground=TEXT, indicatorcolor=PANEL, focusthickness=0)
    style.map("TRadiobutton", indicatorcolor=[("selected", ACCENT), ("disabled", PANEL_ALT)],
              background=[("active", PANEL)], foreground=[("disabled", FAINT)])
    # Trees
    style.configure("Treeview", background=PANEL, fieldbackground=PANEL, foreground=TEXT,
                    rowheight=28, bordercolor=BORDER, lightcolor=PANEL, darkcolor=PANEL, font=f["body"])
    style.configure("Treeview.Heading", background=PANEL_ALT, foreground=MUTED, relief="flat",
                    bordercolor=BORDER, lightcolor=PANEL_ALT, darkcolor=PANEL_ALT, padding=(8, 6),
                    font=(f["family"], 9, "bold"))
    style.map("Treeview.Heading", background=[("active", "#eef0f4")])
    style.map("Treeview", background=[("selected", ACCENT_SOFT)], foreground=[("selected", TEXT)])
    # Scrollbars: slim, low contrast
    for orient in ("Vertical", "Horizontal"):
        style.configure(orient + ".TScrollbar", background="#d7dbe2", troughcolor=PANEL_ALT,
                        bordercolor=PANEL_ALT, lightcolor="#d7dbe2", darkcolor="#d7dbe2",
                        arrowcolor=FAINT, gripcount=0, arrowsize=11)
        style.map(orient + ".TScrollbar", background=[("active", "#bfc5cf")])
    # Notebook
    style.configure("TNotebook", background=BACKGROUND, borderwidth=0, tabmargins=(0, 0, 0, 0))
    style.configure("TNotebook.Tab", background=BACKGROUND, foreground=MUTED, padding=(16, 8),
                    bordercolor=BACKGROUND, lightcolor=BACKGROUND, font=f["strong"])
    style.map("TNotebook.Tab", background=[("selected", PANEL), ("active", "#e9ebf0")],
              foreground=[("selected", ACCENT_TEXT), ("active", TEXT)],
              lightcolor=[("selected", PANEL)], bordercolor=[("selected", BORDER)])
    # Progress
    style.configure("Crescendo.Horizontal.TProgressbar", background=ACCENT, lightcolor=ACCENT, darkcolor=ACCENT,
                    bordercolor=PANEL_ALT, troughcolor="#eceef2", thickness=6)
    style.configure("Horizontal.TProgressbar", background=ACCENT, lightcolor=ACCENT, darkcolor=ACCENT,
                    bordercolor=PANEL_ALT, troughcolor="#eceef2")
    style.configure("TSeparator", background=BORDER)
    style.configure("TLabelframe", background=PANEL, bordercolor=BORDER)
    style.configure("TLabelframe.Label", background=PANEL, foreground=MUTED, font=f["strong"])
    style.configure("TSpinbox", fieldbackground=PANEL, bordercolor=BORDER_STRONG, arrowcolor=MUTED)
    return style
