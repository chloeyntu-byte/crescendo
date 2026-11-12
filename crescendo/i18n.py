# -*- coding: utf-8 -*-
"""Display wording for the shared kernel's UI strings (Crescendo shows English).

The engine mixes English and Chinese messages; Crescendo shows every engine
message in English. Only presentation
text passes through here: values the kernel reads back (entry/combobox values,
identifiers, paths) are never translated.
"""
from __future__ import unicode_literals

import os
import re

# CRESCENDO_I18N=off keeps the kernel's own wording (used when the kernel GUI
# tests run against the Crescendo window: they match the kernel's text).
ENABLED = os.environ.get("CRESCENDO_I18N", "on").strip().lower() not in ("off", "0", "no")

_CJK = re.compile("[　-〿一-鿿＀-￯]")
_PLACEHOLDER = re.compile(r"\{[^{}]*\}|%[sdrf]")
_FULLWIDTH = {"：": ": ", "，": ", ", "；": "; ", "。": ". ", "（": " (", "）": ") ",
              "“": '"', "”": '"', "、": ", ", "！": "! ", "？": "? ", "…": "..."}

EXACT = {}
PATTERNS = []
FRAGMENTS = []


def _load():
    try:
        from .i18n_en_data import PAIRS
    except ImportError:  # pragma: no cover - table not generated
        PAIRS = []
    seen = set()
    for chinese, english in PAIRS:
        # Kernel messages are often assembled line by line ("\n  label：{}"),
        # so every entry is also indexed by its stripped single-line form.
        variants = [(chinese, english), (chinese.strip(), english.lstrip("\n ").rstrip("\n"))]
        zh_lines, en_lines = chinese.split("\n"), english.split("\n")
        if len(zh_lines) > 1 and len(zh_lines) == len(en_lines):
            variants.extend((z.strip(), e.strip()) for z, e in zip(zh_lines, en_lines) if _CJK.search(z))
        for key, value in variants:
            if not key or key in seen:
                continue
            seen.add(key)
            if _PLACEHOLDER.search(key):
                pieces = _PLACEHOLDER.split(key)
                if len(_CJK.findall("".join(pieces))) < 2:
                    continue  # e.g. "{}：{}" would capture unrelated text
                regex = "^" + "(.*?)".join(re.escape(piece) for piece in pieces) + "$"
                try:
                    PATTERNS.append((len("".join(pieces)), re.compile(regex, re.S), value))
                except re.error:
                    continue
            else:
                EXACT.setdefault(key, value)
                if _CJK.search(key) and len(key) > 1:
                    FRAGMENTS.append((key, value))
    # Literal specificity must not depend on re.escape's version-specific
    # escaping (Python 3.6 escapes Chinese characters; newer versions do not).
    PATTERNS.sort(key=lambda row: -row[0])
    PATTERNS[:] = [(pattern, english) for _specificity, pattern, english in PATTERNS]
    FRAGMENTS.sort(key=lambda row: -len(row[0]))


_load()


def _fill(template, values):
    values = list(values)

    def take(_match):
        return values.pop(0) if values else ""
    return _PLACEHOLDER.sub(take, template)


def _line_to_english(text, depth=0):
    found = EXACT.get(text)
    if found is not None:
        return found
    if depth < 3:
        for pattern, english in PATTERNS:
            match = pattern.match(text)
            if match:
                text = _fill(english, [_line_to_english(group, depth + 1) if _CJK.search(group) else group
                                       for group in match.groups()])
                if not _CJK.search(text):
                    return text
                break
    for chinese, english in FRAGMENTS:
        if chinese in text:
            text = text.replace(chinese, english)
            if not _CJK.search(text):
                break
    if _CJK.search(text):
        for mark, plain in _FULLWIDTH.items():
            text = text.replace(mark, plain)
    return text


def _to_english(text):
    found = EXACT.get(text)
    if found is not None:
        return found
    lines = []
    for line in text.split("\n"):
        if not _CJK.search(line):
            lines.append(line)
            continue
        body = line.strip()
        start = line.index(body[0]) if body else 0
        lines.append(line[:start] + _line_to_english(body) + line[start + len(body):])
    return "\n".join(lines)



# Kernel English sentences reworded for Crescendo (same meaning, no kernel name).
REWORD = (
    ("Each run uses crescendo/<TB>/<optimization>/ below this root.",
     "Each run is stored in its own folder below this root."),
)


def tr(text):
    """English display wording for one kernel string; other text is unchanged."""
    if not ENABLED or not isinstance(text, str) or not text:
        return text
    if _CJK.search(text):
        text = _to_english(text)
    if "crescendo/" in text:
        for old, new in REWORD:
            text = text.replace(old, new)
    return text


# Crescendo labels that differ from the kernel's wording for the same control.
# Used only in kernel-test mode (ENABLED False) so text-based test lookups work.
_REVERSE = {
    "Load and Inspect": "新建 / Load and Inspect", "Apply Selected Cells  →": "Apply Selected Cells",
    "Clear": "Clear Cell Selection", "Select All": "Select All Cells", "Match Details…": "Details...",
    "Details…": "Details...", "▶  Start Optimization": "Start", "+ Add Objective": "Add Objective",
    "+ Add Constraint": "Add Constraint", "Manage / Load Working Copies…": "管理 / 加载私有套件…",
}


def kernel_wording(text):
    if not isinstance(text, str):
        return text
    return _REVERSE.get(text, text)
