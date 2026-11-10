# -*- coding: utf-8 -*-
"""Small canvas/Frame widgets for the Crescendo shell (Tk 8.5 compatible)."""
from __future__ import division

import math

try:
    import tkinter as tk
    from tkinter import ttk
except ImportError:  # pragma: no cover
    import Tkinter as tk
    import ttk

from . import theme as T


def draw_logo(canvas, x, y, size, color=None, background=None):
    """Crescendo hairpin: two strokes opening to the right, plus a rising dot."""
    color = color or T.ACCENT
    s = size
    canvas.create_rectangle(x, y, x + s, y + s, fill=background or T.INK_3, outline="")
    pad = s * 0.24
    mid = y + s / 2.0
    canvas.create_line(x + pad, mid, x + s - pad, y + pad, fill=color, width=max(2, s // 12), capstyle="round")
    canvas.create_line(x + pad, mid, x + s - pad, y + s - pad, fill=color, width=max(2, s // 12), capstyle="round")


class BrandMark(tk.Canvas):
    def __init__(self, parent, title="Crescendo", subtitle="for Cadence Maestro"):
        tk.Canvas.__init__(self, parent, height=58, background=T.INK, highlightthickness=0, borderwidth=0)
        draw_logo(self, 18, 13, 32)
        self.create_text(60, 22, text=title, anchor="w", fill=T.INK_TEXT, font=T.FONTS["brand"])
        self.create_text(61, 42, text=subtitle, anchor="w", fill=T.INK_MUTED, font=T.FONTS["tiny"])


class NavItem(tk.Canvas):
    """Sidebar step: number badge, label, completion tick and active bar."""
    HEIGHT = 40

    def __init__(self, parent, number, label, command):
        tk.Canvas.__init__(self, parent, height=self.HEIGHT, background=T.INK, highlightthickness=0,
                           borderwidth=0, cursor="hand2", takefocus=1)
        self.number = number
        self.label = label
        self.command = command
        self.active = False
        self.done = False
        self.hover = False
        self.enabled = True
        self.bind("<Button-1>", self._click)
        self.bind("<Return>", self._click)
        self.bind("<Enter>", lambda _e: self._set_hover(True))
        self.bind("<Leave>", lambda _e: self._set_hover(False))
        self.bind("<Configure>", lambda _e: self.redraw())

    def _click(self, _event=None):
        if self.command:
            self.command()
        return "break"

    def _set_hover(self, value):
        self.hover = value
        self.redraw()

    def set_state(self, active=None, done=None):
        changed = False
        if active is not None and active != self.active:
            self.active, changed = active, True
        if done is not None and done != self.done:
            self.done, changed = done, True
        if changed:
            self.redraw()

    def redraw(self):
        self.delete("all")
        width = max(self.winfo_width(), 200)
        h = self.HEIGHT
        background = T.INK_3 if self.active else (T.INK_2 if self.hover else T.INK)
        self.create_rectangle(8, 2, width - 8, h - 2, fill=background, outline="")
        if self.active:
            self.create_rectangle(8, 9, 11, h - 9, fill=T.ACCENT, outline="")
        cx, cy, r = 32, h / 2.0, 10
        if self.number is None:
            self.create_text(cx, cy, text="⚙", fill=T.INK_TEXT if self.active else T.INK_MUTED, font=T.FONTS["body"])
        elif self.done and not self.active:
            self.create_oval(cx - r, cy - r, cx + r, cy + r, fill="#1f3a2b", outline="")
            self.create_line(cx - 4.5, cy + 0.5, cx - 1, cy + 4, cx + 5, cy - 3.5, fill="#4ade80", width=2,
                             capstyle="round", joinstyle="round")
        else:
            fill = T.ACCENT if self.active else T.INK
            outline = T.ACCENT if self.active else "#3a404b"
            self.create_oval(cx - r, cy - r, cx + r, cy + r, fill=fill, outline=outline, width=1.5)
            self.create_text(cx, cy, text=str(self.number), fill="#ffffff" if self.active else T.INK_MUTED,
                             font=T.FONTS["tiny"] if self.number >= 10 else (T.FONTS["family"], 8, "bold"))
        self.create_text(52, cy, text=self.label, anchor="w",
                         fill="#ffffff" if self.active else (T.INK_TEXT if self.hover else "#c5c9d1"),
                         font=T.FONTS["nav_strong"] if self.active else T.FONTS["nav"])


class ToggleSwitch(tk.Canvas):
    """iOS-like switch bound to a BooleanVar."""

    def __init__(self, parent, variable, text="", command=None, background=None, foreground=None):
        self.bg = background or T.INK
        self.fg = foreground or T.INK_TEXT
        tk.Canvas.__init__(self, parent, height=30, background=self.bg, highlightthickness=0, borderwidth=0,
                           cursor="hand2", takefocus=1)
        self.variable = variable
        self.text = text
        self.command = command
        self.bind("<Button-1>", self._toggle)
        self.bind("<space>", self._toggle)
        self.bind("<Configure>", lambda _e: self.redraw())
        variable.trace("w", lambda *_a: self.redraw())

    def _toggle(self, _event=None):
        self.variable.set(not self.variable.get())
        if self.command:
            self.command()
        return "break"

    def redraw(self):
        self.delete("all")
        on = bool(self.variable.get())
        x, y, w, h = 18, 7, 32, 16
        track = T.ACCENT if on else "#3a404b"
        self.create_oval(x, y, x + h, y + h, fill=track, outline="")
        self.create_oval(x + w - h, y, x + w, y + h, fill=track, outline="")
        self.create_rectangle(x + h / 2.0, y, x + w - h / 2.0, y + h, fill=track, outline="")
        knob = x + w - h + 2 if on else x + 2
        self.create_oval(knob, y + 2, knob + h - 4, y + h - 2, fill="#ffffff", outline="")
        self.create_text(x + w + 10, y + h / 2.0, text=self.text, anchor="w", fill=self.fg, font=T.FONTS["small"])


class Pill(tk.Label):
    KINDS = {
        "neutral": ("#eef0f4", T.MUTED), "accent": (T.ACCENT_SOFT, T.ACCENT_TEXT),
        "success": (T.SUCCESS_SOFT, T.SUCCESS), "warning": (T.WARNING_SOFT, T.WARNING),
        "danger": (T.DANGER_SOFT, T.DANGER), "dark": (T.INK_3, T.INK_TEXT),
    }

    def __init__(self, parent, textvariable=None, text="", kind="neutral"):
        background, foreground = self.KINDS[kind]
        tk.Label.__init__(self, parent, text=text, textvariable=textvariable, background=background,
                          foreground=foreground, font=(T.FONTS["family"], 9, "bold"), padx=10, pady=3)

    def set_kind(self, kind):
        background, foreground = self.KINDS.get(kind, self.KINDS["neutral"])
        self.configure(background=background, foreground=foreground)


class KpiTile(tk.Frame):
    def __init__(self, parent, title, value="—", note="", color=None):
        tk.Frame.__init__(self, parent, background=T.PANEL, highlightthickness=1,
                          highlightbackground=T.BORDER, highlightcolor=T.BORDER)
        self.accent = tk.Frame(self, background=color or T.ACCENT, height=3)
        self.accent.pack(fill="x")
        body = tk.Frame(self, background=T.PANEL, padx=14, pady=10)
        body.pack(fill="both", expand=True)
        tk.Label(body, text=title, background=T.PANEL, foreground=T.MUTED, font=T.FONTS["small"], anchor="w").pack(fill="x")
        self.value_var = tk.StringVar(value=value)
        tk.Label(body, textvariable=self.value_var, background=T.PANEL, foreground=T.TEXT, font=T.FONTS["kpi"],
                 anchor="w").pack(fill="x", pady=(2, 0))
        self.note_var = tk.StringVar(value=note)
        tk.Label(body, textvariable=self.note_var, background=T.PANEL, foreground=T.FAINT, font=T.FONTS["tiny"],
                 anchor="w").pack(fill="x")

    def set(self, value, note=None):
        if self.value_var.get() != value:
            self.value_var.set(value)
        if note is not None and self.note_var.get() != note:
            self.note_var.set(note)


def nice_ticks(low, high, count=5):
    span = high - low
    if span <= 0 or not math.isfinite(span):
        return [low]
    raw = span / float(count)
    magnitude = 10 ** math.floor(math.log10(raw))
    for step in (1, 2, 2.5, 5, 10):
        if step * magnitude >= raw:
            step *= magnitude
            break
    start = math.ceil(low / step) * step
    ticks = []
    value = start
    while value <= high + step * 1e-9 and len(ticks) < 20:
        ticks.append(value)
        value += step
    return ticks


def fmt(value):
    if value is None:
        return "—"
    magnitude = abs(value)
    if magnitude != 0 and (magnitude >= 1e5 or magnitude < 1e-3):
        prefixes = ((1e9, "G"), (1e6, "M"), (1e3, "k"), (1, ""), (1e-3, "m"), (1e-6, "µ"), (1e-9, "n"),
                    (1e-12, "p"), (1e-15, "f"))
        for scale, prefix in prefixes:
            if magnitude >= scale:
                return "{:.3g}{}".format(value / scale, prefix)
        return "{:.3g}".format(value)
    return "{:.4g}".format(value)


class ConvergenceChart(tk.Canvas):
    """Best-so-far line plus per-generation samples; redraws only on data change."""

    def __init__(self, parent, height=220, empty_text="The convergence curve appears here once the run starts"):
        tk.Canvas.__init__(self, parent, height=height, background=T.PANEL, highlightthickness=0, borderwidth=0)
        self.height = height
        self.empty_text = empty_text
        self.series = None
        self._signature = None
        self.bind("<Configure>", lambda _e: self._draw())

    def update_series(self, samples, best, label="", minimize=True):
        """samples: [(generation, value)], best: [(generation, best_value)]"""
        signature = (len(samples), samples[-1] if samples else None, len(best), label, minimize)
        if signature == self._signature:
            return
        self._signature = signature
        self.series = (list(samples), list(best), label, minimize)
        self._draw()

    def _draw(self):
        self.delete("all")
        width = max(self.winfo_width(), 300)
        height = self.height
        if not self.series or not self.series[0]:
            self.create_text(width / 2.0, height / 2.0, text=self.empty_text, fill=T.FAINT, font=T.FONTS["body"])
            return
        samples, best, label, minimize = self.series
        left, right, top, bottom = 58, width - 18, 26, height - 30
        xs = [g for g, _v in samples] + [g for g, _v in best]
        ys = [v for _g, v in samples] + [v for _g, v in best]
        x0, x1 = min(xs), max(xs)
        if x1 == x0:
            x1 = x0 + 1
        y0, y1 = min(ys), max(ys)
        pad = (y1 - y0) * 0.08 or abs(y1) * 0.1 or 1.0
        y0, y1 = y0 - pad, y1 + pad

        def sx(v):
            return left + (v - x0) / float(x1 - x0) * (right - left)

        def sy(v):
            return bottom - (v - y0) / float(y1 - y0) * (bottom - top)

        for tick in nice_ticks(y0, y1, 4):
            y = sy(tick)
            self.create_line(left, y, right, y, fill=T.CHART_GRID)
            self.create_text(left - 8, y, text=fmt(tick), anchor="e", fill=T.FAINT, font=T.FONTS["tiny"])
        for tick in nice_ticks(x0, x1, 6):
            if abs(tick - round(tick)) > 1e-9:
                continue
            x = sx(tick)
            self.create_text(x, bottom + 14, text=str(int(round(tick))), fill=T.FAINT, font=T.FONTS["tiny"])
        self.create_text(left - 8, bottom + 14, text="gen →", anchor="e", fill=T.FAINT, font=T.FONTS["tiny"])
        jitter = (right - left) / float(max(1, x1 - x0)) * 0.16
        for index, (g, v) in enumerate(samples[-4000:]):
            offset = ((index * 7919) % 11 - 5) / 5.0 * jitter
            if g >= x1:
                offset = -abs(offset)
            elif g <= x0:
                offset = abs(offset)
            x, y = sx(g) + offset, sy(v)
            self.create_oval(x - 2.5, y - 2.5, x + 2.5, y + 2.5, fill="#ffc6ab", outline="")
        if len(best) >= 2:
            points = []
            for g, v in best:
                points.extend((sx(g), sy(v)))
            area = [sx(best[0][0]), bottom] + points + [sx(best[-1][0]), bottom]
            self.create_polygon(*area, fill=T.ACCENT_SOFT, outline="")
            self.create_line(*points, fill=T.ACCENT, width=2.5, smooth=False, capstyle="round", joinstyle="round")
        if best:
            g, v = best[-1]
            x, y = sx(g), sy(v)
            self.create_oval(x - 5, y - 5, x + 5, y + 5, fill=T.ACCENT, outline="#ffffff", width=2)
            self.create_text(x - 8, y - 12, text=fmt(v), anchor="e", fill=T.ACCENT_TEXT, font=T.FONTS["strong"])
        self.create_text(left, 10, text=label, anchor="w", fill=T.MUTED, font=T.FONTS["small"])
        self.create_text(right, 10, text=("lower is better ↓" if minimize else "higher is better ↑"), anchor="e", fill=T.FAINT,
                         font=T.FONTS["tiny"])
