"""Drive the Crescendo window with the demo service and screenshot each page.

  DISPLAY=:77 python3 scripts/tour.py OUTDIR [--scale small] [--pro] [--frames]
--frames additionally records a PNG sequence of the live run (for the film).
"""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from crescendo import demo_smoke  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("out")
parser.add_argument("--scale", default="small")
parser.add_argument("--pro", action="store_true")
parser.add_argument("--speed", type=float, default=3.0)
parser.add_argument("--frames", type=int, default=0, help="record N frames of the live run")
parser.add_argument("--geometry", default="1440x900+0+0")
parser.add_argument("--dump", help="write the finished run's candidates (demo data) to this JSON file")
options = parser.parse_args()
os.environ["CRESCENDO_MODE"] = "pro" if options.pro else "simple"
os.makedirs(options.out, exist_ok=True)
args = argparse.Namespace(latency=0.15, speed=options.speed, generations=14, population=16, cells=None,
                          profile=False, withdraw=False, ui="crescendo")
probe = demo_smoke.Probe(options.scale, args)
app, service, root = probe.app, probe.service, probe.root
root.geometry(options.geometry)
shot_index = [0]


def shot(name, settle=0.6):
    probe.pump(settle)
    path = os.path.join(options.out, "{:02d}-{}.png".format(shot_index[0], name))
    shot_index[0] += 1
    window = root.winfo_id()
    frame = int(root.wm_frame(), 16) if root.wm_frame() else window
    subprocess.check_call([sys.executable, os.path.join(ROOT, "scripts", "xshot.py"), path, "--window", hex(frame)],
                          stdout=subprocess.DEVNULL)
    print("shot", path)


probe.pump(0.8)
shot("welcome")
app.setup_path_var.set(probe.setup_path)
probe.step("load", app.load_setup, lambda: app.test_inspection_id is not None)
app.test_choice_var.set(service.test_rows[0]["name"])
app._test_selected()
probe.wait(lambda: app.cell_inspection_id is not None and not app.setup_loading and app.cell_render_job is None)
rows = app.cell_scope_tree.get_children("")
for item in rows:
    app.cell_scope_tree.item(item, open=True)
children = [c for r in rows for c in app.cell_scope_tree.get_children(r)]
for scope in children[:3]:
    app._toggle_cell_scope(scope)
shot("design")
probe.step("apply", app.apply_selected_cells, lambda: app.prepared_id is not None and not app.setup_loading)
probe.wait(lambda: not getattr(app, "parameter_tree_dirty", True), 120)
app.select_all_parameters(False)
app.select_semantic("width", True)
app.select_semantic("length", True)
probe.wait(lambda: not getattr(app, "parameter_tree_dirty", True), 120)
shot("parameters", 1.0)
app.show_page("Objectives & Constraints")
row = app.objective_rows[0] if app.objective_rows else None
if row is None:
    app.add_objective()
    row = app.objective_rows[0]
row.output_var.set("gain_dB")
row.mode_var.set("maximize")
app.add_objective({"output_key": "power", "direction": "minimize"})
app.add_constraint({"output_key": "pm", "operator": ">=", "limit": "55"})
app._metric_count_changed()
shot("objectives")
app.generations_var.set("14")
app.show_page("Optimization")
shot("optimization-ready")
app.start_run()
probe.wait(lambda: bool(app.run_ui.run_id), 60)
if options.frames:
    frame_dir = os.path.join(options.out, "frames")
    os.makedirs(frame_dir, exist_ok=True)
    for index in range(options.frames):
        probe.pump(0.25)
        path = os.path.join(frame_dir, "f{:04d}.png".format(index))
        subprocess.check_call([sys.executable, os.path.join(ROOT, "scripts", "xshot.py"), path, "--window",
                               hex(int(root.wm_frame(), 16))], stdout=subprocess.DEVNULL)
        if index == options.frames // 2:
            app.show_page("Live Results")
else:
    probe.wait(lambda: sum(1 for c in app.live_candidates if c.get("status") == "completed") >= 60, 300)
    shot("optimization-running", 1.5)
probe.wait(lambda: app.run_ui.state == "completed", 600)
app.show_page("Optimization")
shot("optimization-done", 1.5)
if options.dump:
    import json
    from crescendo_engine.gui_state import metric_value
    rows = []
    for row in app.live_candidates:
        if row.get("status") != "completed":
            continue
        metrics = row.get("metrics") or {}
        rows.append({"id": row["candidate_id"], "g": int(row.get("generation") or 0) + 1,
                     "gain": metric_value(row, 0), "power": metric_value(row, 1),
                     "pm": ((metrics.get("constraints") or [{}])[0] or {}).get("value"),
                     "feasible": metrics.get("feasible") is True, "pareto": row["candidate_id"] in app.pareto_ids})
    with open(options.dump, "w") as handle:
        json.dump({"source": "Crescendo demo service (synthetic data)", "objectives": ["gain_dB max", "power min"],
                   "constraint": "pm >= 55", "generations": 14, "candidates": rows}, handle, separators=(",", ":"))
    print("dumped", len(rows))
app.show_page("Live Results")
cid = sorted(app.pareto_ids)[0] if app.pareto_ids else None
if cid:
    app.select_candidate(cid, navigate=False)
    app.render_results()
shot("results", 1.2)
app.show_page("Pareto Trade-offs")
shot("pareto", 1.2)
app.pro_mode_var.set(True)
app._apply_mode()
app.show_page("EDA Settings")
shot("settings-pro", 1.0)
probe.close()
