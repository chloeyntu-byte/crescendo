"""Stop a demo run, resume it immediately (no settling delay), and require that
live progress keeps updating until the resumed run completes.

  DISPLAY=:77 python3 scripts/resume_race_probe.py [--ui crescendo|classic] [--rounds N]
Exit 0 when every round completes with live updates, 1 otherwise.
"""
import argparse
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from crescendo import demo_smoke  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--ui", default="crescendo")
parser.add_argument("--rounds", type=int, default=3)
parser.add_argument("--slow-progress", type=float, default=1.0,
                    help="seconds each progress request takes, so one is in flight during Resume")
options = parser.parse_args()
args = argparse.Namespace(latency=0.0, speed=6.0, generations=8, population=8, cells=None,
                          profile=False, withdraw=False, ui=options.ui)
probe = demo_smoke.Probe("small", args)
app, service = probe.app, probe.service
for name in ("progress", "progress_delta"):
    original = getattr(service, name, None)
    if original is not None:
        def slowed(*a, _original=original, **k):
            time.sleep(options.slow_progress)
            return _original(*a, **k)
        setattr(service, name, slowed)
probe.pump(0.5)
app.setup_path_var.set(probe.setup_path)
probe.wait(lambda: app.load_setup() or True)
probe.wait(lambda: app.test_inspection_id is not None, 30)
app.test_choice_var.set(service.test_rows[0]["name"])
app._test_selected()
probe.wait(lambda: app.cell_inspection_id is not None and not app.setup_loading and app.cell_render_job is None, 30)
app.select_all_cells(True)
probe.wait(lambda: app.cell_render_job is None, 30)
app.apply_selected_cells()
probe.wait(lambda: app.prepared_id is not None and not app.setup_loading, 60)
if not app.objective_rows:
    app.add_objective()
app.objective_rows[0].output_var.set("gain_dB")
app.objective_rows[0].mode_var.set("maximize")
app.generations_var.set("8")
app.start_run()
probe.wait(lambda: bool(app.run_ui.run_id), 30)
ok = True
for round_index in range(options.rounds):
    probe.wait(lambda: sum(1 for c in app.live_candidates if c.get("status") == "completed") >= 4 + round_index * 6, 120)
    stopped = app.run_ui.run_id
    app.stop_run()
    probe.wait(lambda: app.run_ui.state == "stopped", 60)
    probe.wait(lambda: app.progress_inflight, 5)  # a poll for the stopped run is in flight
    # Resume at once: an earlier poll for the stopped run may still be in flight.
    app.refresh_recent_runs()
    probe.wait(lambda: stopped in app.recent_tree.get_children(""), 10)
    app.recent_tree.selection_set(stopped)
    app.resume_recent_run()
    probe.wait(lambda: app.run_ui.run_id not in (None, stopped), 30)
    seen = len(app.live_candidates)
    deadline = time.time() + 20
    while time.time() < deadline and len(app.live_candidates) <= seen:
        probe.pump(0.2)
    live = len(app.live_candidates) > seen
    print("round %d: resumed run %s live updates %s (inflight=%s)" % (round_index + 1, app.run_ui.run_id, live, app.progress_inflight))
    ok = ok and live
completed = probe.wait(lambda: app.run_ui.state == "completed", 300)
print("final state", app.run_ui.state)
ok = ok and app.run_ui.state == "completed"
probe.close()
print("RESULT", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
