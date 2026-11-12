"""Drive the full demo flow, then list visible text that says "Crescendo" or is not English.

  DISPLAY=:77 python3 scripts/brand_scan.py
Scans labels/buttons (text and variable), notebook tabs, tree headings and rows,
Text widgets, combobox values and window titles of every open window.
"""
import argparse
import os
import re
import sys
import subprocess

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ["CRESCENDO_MODE"] = "pro"
from crescendo import demo_smoke  # noqa: E402

LEGACY = "m" + "co"  # the predecessor name, spelled out so renames cannot rewrite it
WORD = re.compile(LEGACY + "|[\u3000-\u303f\u4e00-\u9fff\uff00-\uffef]", re.I)


# Split spellings keep this scanner's own source from matching its rule.
TRACKED_WORD = re.compile(r"\bHH_" + r"[A-Z]|\b(?:" + "|".join(
    ("S" + "MIC", "T" + "SMC", "U" + "MC", "Global" + "Foundries",
     "do" + "nau", "d" + "sub", "d" + "job", "d" + "kill")) + r")\b", re.I)


def tracked_text_hits():
    names = subprocess.check_output(['git', 'ls-files', '-z'], cwd=ROOT).decode('utf-8').split('\0')
    for name in names:
        if not name:
            continue
        with open(os.path.join(ROOT, name), 'rb') as stream:
            raw = stream.read()
        if b'\0' in raw:
            continue
        text = raw.decode('utf-8', errors='replace')
        for number, line in enumerate(text.splitlines(), 1):
            match = TRACKED_WORD.search(line)
            if match:
                yield ('tracked', name, str(number), match.group(0))


def texts(root):
    tk = root.tk
    pending = [root]
    while pending:
        widget = pending.pop()
        pending.extend(widget.winfo_children())
        cls = widget.winfo_class()
        path = str(widget)
        if cls in ("Toplevel", "Tk"):
            yield path, "title", widget.title()
        for option in ("text", "textvariable"):
            try:
                value = widget.cget(option)
            except Exception:
                continue
            if option == "textvariable" and str(value):
                if cls in ("TEntry", "Entry", "TCombobox", "TSpinbox"):
                    continue  # user input, kept verbatim
                value = tk.globalgetvar(str(value))
            if value:
                yield path, option, str(value)
        if cls == "TNotebook":
            for tab in widget.tabs():
                yield path, "tab", widget.tab(tab, "text")
        if cls == "Treeview":
            for column in ("#0",) + tuple(widget.cget("columns") or ()):
                yield path, "heading", widget.heading(column, "text")
            stack = list(widget.get_children(""))
            while stack:
                item = stack.pop()
                stack.extend(widget.get_children(item))
                yield path, "row", str(widget.item(item, "text")) + " " + " ".join(map(str, widget.item(item, "values") or ()))
        if cls == "Text" and not getattr(widget, "_crescendo_raw", False):
            yield path, "text", widget.get("1.0", "end")
        if cls == "Canvas":
            for item in widget.find_all():
                if widget.type(item) == "text":
                    yield path, "canvas", widget.itemcget(item, "text")


# Use the normal smoke-test budget so the scan can exercise Stop before completion.
args = argparse.Namespace(latency=0.0, speed=14.0, generations=12, population=16, cells=None,
                          profile=False, withdraw=False, ui="crescendo")
probe = demo_smoke.Probe("small", args)
probe.close_original = probe.close
probe.close = lambda: None
try:
    probe.run()
except SystemExit:
    pass
app = probe.app
app.show_page("EDA Settings")
app.open_private_suites()
probe.pump(1.0)
cid = next((c["candidate_id"] for c in app.live_candidates if c.get("status") == "completed"), None)
if cid:
    app.open_candidate_logs(cid)
app._show_text_details("Crescendo Diagnostic Details", app.last_diagnostic_detail)
app.show_timing_details()
probe.pump(1.5)
hits = list(tracked_text_hits())
for page in list(app.pages):
    app.show_page(page)
    probe.pump(0.3)
    for path, kind, value in texts(probe.root):
        if WORD.search(value or ""):
            hits.append((page, path, kind, " | ".join(sorted(set(re.findall(r"[^\s]*(?:" + LEGACY + r"|[\u3000-\u303f\u4e00-\u9fff\uff00-\uffef]+)[^\s]*", value))))[:200]))
seen = set()
for hit in hits:
    key = hit[1:]
    if key in seen:
        continue
    seen.add(key)
    print("HIT", hit[1], hit[2], "|", hit[3])
print("TOTAL", len(seen))
probe.close_original()
