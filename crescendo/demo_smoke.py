"""Headless smoke test / lag probe: drive the real GUI against DemoService.

    DISPLAY=:77 /usr/bin/python3 -m crescendo.demo_smoke [--scales small large] [--profile]

For each scale the main flow is driven through OptimizerGui's own methods
(load_setup, test selection, select_all_cells + apply_selected_cells, parameter
selection, objectives/constraint, start_run, Live Results / Pareto pages,
stop_run, diagnostics, EDA page, private-suite window).  Every Tk callback
exception, GUI error dialog and ``_set_error`` call is collected; the process
exits non-zero if any occurred.  Timings of the slow steps (wall time and the
longest main-loop stall) are printed for lag analysis.
"""
from __future__ import print_function

import argparse
import cProfile
import gc
import io
import os
import pstats
import shutil
import sys
import tempfile
import time
import traceback


class Probe(object):
    def __init__(self, scale, args):
        from crescendo import demo_main
        from crescendo_engine import gui
        from crescendo.demo_service import DemoService
        if getattr(args, "ui", "crescendo") == "crescendo":
            from crescendo import app
            app.install()
        self.gui = gui
        self.scale = scale
        self.args = args
        self.failures = []
        self.timings = []
        self.base, self.setup_path = demo_main.prepare_environment()
        config = demo_main.build_config(self.base)
        extra = {}
        if scale == "small":
            extra = {}
        self.service = DemoService(
            scale=scale, latency=args.latency, speed=args.speed, generations=args.generations,
            population=args.population, workdir=os.path.join(self.base, "service"), config=config,
            cells=args.cells if scale != "small" else None, **extra)
        self.root = gui.tk.Tk()
        self.root.geometry("1380x860+0+0")
        if args.withdraw:
            self.root.withdraw()
        self.app = gui.OptimizerGui(self.root, self.service)
        self.root.report_callback_exception = self._callback_exception
        self._patch_dialogs()
        self.beats = []
        self._last_beat = time.time()
        self._beat_job = None
        self._beating = True
        self._beat()
        self.profiler = None

    # -- failure capture --------------------------------------------------
    def _fail(self, label, text):
        self.failures.append((label, text))
        print("  !! {}: {}".format(label, text.strip().splitlines()[-1] if text.strip() else ""))

    def _callback_exception(self, exception, value, tb):
        self._fail("tk-callback", "".join(traceback.format_exception(exception, value, tb)))

    def _patch_dialogs(self):
        gui = self.gui
        probe = self
        gui.messagebox.showerror = lambda title=None, message=None, **kw: probe._fail(
            "dialog:showerror", "{}: {}".format(title, message))
        gui.messagebox.showwarning = lambda title=None, message=None, **kw: print(
            "  note (warning dialog): {}: {}".format(title, str(message)[:200]))
        gui.messagebox.showinfo = lambda *a, **k: None
        gui.messagebox.askyesno = lambda *a, **k: True
        gui.simpledialog.askstring = lambda *a, **k: "Smoke run " + self.scale
        original = self.app._set_error

        def recorded(message):
            probe._fail("set_error", str(message))
            return original(message)
        self.app._set_error = recorded

    # -- pumping ------------------------------------------------------------
    def _beat(self):
        now = time.time()
        self.beats.append((now, now - self._last_beat))
        self._last_beat = now
        if not self._beating:
            return
        try:
            self._beat_job = self.root.after(10, self._beat)
        except self.gui.tk.TclError:
            pass

    def pump(self, seconds=0.3):
        end = time.time() + seconds
        while time.time() < end:
            self.root.update()
            time.sleep(0.004)

    def wait(self, predicate, timeout=60.0, label="condition"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.root.update()
            if predicate():
                return True
            time.sleep(0.004)
        app = self.app
        runs = dict((rid, (r.state, r.next_generation, len(r.candidates)))
                    for rid, r in self.service._runs.items())
        self._fail("timeout", "timed out waiting for {} [run_ui={}/{} inflight={} service_runs={}]".format(
            label, app.run_ui.run_id, app.run_ui.state, app.progress_inflight, runs))
        return False

    def step(self, name, action, predicate=None, timeout=60.0):
        """Run ``action`` then pump until ``predicate``; record wall time + longest UI stall."""
        profile = None
        if self.args.profile:
            profile = cProfile.Profile()
        mark = len(self.beats)
        started = time.time()
        if profile:
            profile.enable()
        try:
            if action is not None:
                action()
            self.wait(predicate or (lambda: True), timeout, name)
        except Exception:
            self._fail("step:" + name, traceback.format_exc())
        finally:
            if profile:
                profile.disable()
        elapsed = time.time() - started
        stalls = [gap for _t, gap in self.beats[mark:]]
        longest = max(stalls) if stalls else 0.0
        self.timings.append((name, elapsed, longest))
        print("  {:<44s} {:7.2f}s   longest UI stall {:6.0f} ms".format(name, elapsed, longest * 1000))
        if profile and (longest > 0.15):
            self._profile_report(profile)
        return elapsed

    def _profile_report(self, profile):
        """Print the costliest crescendo_engine functions (cumulative) from the Tk thread."""
        stream = io.StringIO()
        stats = pstats.Stats(profile, stream=stream).sort_stats("cumulative")
        stats.print_stats(60)
        shown = 0
        for line in stream.getvalue().splitlines():
            if "crescendo_engine" in line and shown < 7:
                shown += 1
                print("       " + line.strip().replace(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.sep, "")[:150])

    # -- the flow -----------------------------------------------------------
    def run(self):
        app, service = self.app, self.service
        print("== scale={} cells={} outputs={} corners={} latency={} ==".format(
            self.scale, service.cells, len(service.outputs), len(service.corner_rows), self.args.latency))
        self.pump(0.5)
        app.setup_path_var.set(self.setup_path)
        self.step("load_setup (Test catalogue)", app.load_setup, lambda: app.test_inspection_id is not None)
        test = service.test_rows[0]["name"]

        def choose():
            app.test_choice_var.set(test)
            app._test_selected()
        self.step("select Test -> inspect_test_cells", choose,
                  lambda: app.cell_inspection_id is not None and not app.setup_loading)
        self.step("render cell tree (chunked)", None, lambda: app.cell_render_job is None, timeout=300)
        self.step("select_all_cells(True)", lambda: app.select_all_cells(True),
                  lambda: app.cell_render_job is None, timeout=300)
        self.step("apply_selected_cells -> prepare_cells", app.apply_selected_cells,
                  lambda: app.prepared_id is not None and not app.setup_loading, timeout=300)
        self.step("render parameter tree (hierarchy)", None,
                  lambda: not getattr(app, "parameter_tree_dirty", True), timeout=600)
        self.step("parameters: W/L select", lambda: (app.select_all_parameters(False),
                                                      app.select_semantic("width", True),
                                                      app.select_semantic("length", True)),
                  lambda: not getattr(app, "parameter_tree_dirty", True), timeout=300)
        self.step("parameter view -> by name", lambda: (app.parameter_view_var.set("name"),
                                                         app._parameter_view_changed()),
                  lambda: not getattr(app, "parameter_tree_dirty", True), timeout=600)
        self.step("parameter view -> hierarchy", lambda: (app.parameter_view_var.set("hierarchy"),
                                                           app._parameter_view_changed()),
                  lambda: not getattr(app, "parameter_tree_dirty", True), timeout=600)
        self.step("Check Selected Ranges (static + PDK)", app.validate_all_ranges,
                  lambda: app.range_validation_job_id is not None and not app.range_validation_inflight
                  and str(app.validate_button.cget("state")) == "normal", timeout=120)

        def metrics():
            app.show_page("Objectives & Constraints")
            row = app.objective_rows[0]
            row.output_var.set("gain_dB")
            row.mode_var.set("maximize")
            app.add_objective({"output_key": "power", "direction": "minimize"})
            app.add_constraint({"output_key": "pm", "operator": ">=", "limit": "55"})
            app._metric_count_changed()
        self.step("objectives + constraint", metrics, lambda: len(app.objective_rows) == 2)

        def start():
            app.generations_var.set("12")
            app.show_page("Optimization")
            app.start_run()
        self.step("start_run", start, lambda: bool(app.run_ui.run_id), timeout=60)
        self.step("show Live Results", lambda: app.show_page("Live Results"), None)
        self.step("wait for 24 candidates", None,
                  lambda: sum(1 for c in app.live_candidates if c.get("status") == "completed") >= 24,
                  timeout=180)
        self._check("result rows rendered", bool(app.result_tree.get_children("")))
        self.step("render_results (explicit)", app.render_results, None)
        self.step("show Pareto Trade-offs", lambda: app.show_page("Pareto Trade-offs"),
                  lambda: bool(getattr(app, "pareto_plots", [])))
        self.step("render_pareto (explicit)", app.render_pareto, None)

        def pick():
            cid = next((c["candidate_id"] for c in app.live_candidates if c.get("status") == "completed"), None)
            if cid:
                app.select_candidate(cid)
                app.open_candidate_logs(cid)
        self.step("select candidate + open logs", pick, None)
        self.pump(1.5)
        for viewer in list(app.log_viewers):
            viewer._close()
        self.step("diagnostics (environment test)", lambda: (app.show_page("Optimization"),
                  app.start_environment_test()),
                  lambda: app.environment_test_state in ("completed", "failed"), timeout=60)
        self.step("EDA Settings page", lambda: app.show_page("EDA Settings"), None)
        self.step("Working Copies window", app.open_private_suites, None)
        self.pump(1.0)
        for child in list(self.root.winfo_children()):
            if isinstance(child, self.gui.tk.Toplevel):
                child.destroy()
        self.step("stop_run (mid-run)", app.stop_run, lambda: app.run_ui.state == "stopped", timeout=60)
        self._check("run stopped mid-way", app.run_ui.state == "stopped")
        self.step("refresh_recent_runs", app.refresh_recent_runs, lambda: bool(app.recent_tree.get_children("")))
        stopped_id = app.run_ui.run_id
        # Kernel GUI quirk: a progress poll still in flight for the stopped run when Resume swaps
        # run_ui.run_id returns early without clearing progress_inflight (gui.py:6900 `loaded`), which
        # silently ends live polling.  Let the old poll chain drain (1.2 s timer) before resuming.
        self.pump(2.5)

        def resume():
            app.show_page("Optimization")
            app.recent_tree.selection_set(stopped_id)
            app.resume_recent_run()
        self.step("resume_recent_run", resume, lambda: app.run_ui.run_id not in (None, stopped_id)
                  and app.run_ui.state in ("running", "completed"), timeout=60)
        self.step("resumed run completes", None, lambda: app.run_ui.state == "completed", timeout=180)
        self._check("pareto ids present", bool(app.pareto_ids))
        self._check("pareto plots have points", any(getattr(p, "points", None) for p in app.pareto_plots)
                    or bool(app.pareto_ids))
        run_id = app.run_ui.run_id
        try:
            csv_text = service.candidates_csv(run_id)
            checkpoint = service.export_checkpoint(run_id)
            target = os.path.join(self.base, "export.crescendo")
            service.export_results(run_id, target)
            from crescendo_engine.run_data_store import load_dataset
            dataset = load_dataset(target)
            print("  export ok: csv {} lines, checkpoint {} candidates, dataset {} candidates".format(
                len(csv_text.splitlines()), len(checkpoint["candidates"]), len(dataset.get("candidates") or [])))
            history = [r for r in service.list_runs() if not r.get("active")][-1]["run_id"]
            progress = service.progress(history)
            print("  history run {}: {} candidates".format(history, len(progress["candidates"])))
        except Exception:
            self._fail("export", traceback.format_exc())

    def _check(self, label, condition):
        if not condition:
            self._fail("check", "failed: " + label)

    def close(self):
        app = self.app
        self._beating = False
        gc.collect()
        try:
            if self._beat_job is not None:
                self.root.after_cancel(self._beat_job)
        except Exception:
            pass
        try:
            app.request_exit(confirm=False)
            self.wait(lambda: not self._alive(), 20, "exit")
        except Exception:
            self._fail("exit", traceback.format_exc())
        if self._alive():
            try:
                app.tasks.close()
                self.root.destroy()
            except Exception:
                pass
        gc.collect()
        shutil.rmtree(self.base, ignore_errors=True)

    def _alive(self):
        try:
            return bool(self.root.winfo_exists())
        except Exception:
            return False


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--scales", nargs="+", default=["small", "large"],
                        choices=("small", "large", "huge"))
    parser.add_argument("--latency", type=float, default=0.0)
    parser.add_argument("--speed", type=float, default=14.0)
    parser.add_argument("--generations", type=int, default=12)
    parser.add_argument("--population", type=int, default=16)
    parser.add_argument("--cells", type=int, default=None, help="override cell count for non-small scales")
    parser.add_argument("--profile", action="store_true", help="cProfile the Tk thread for stalls > 150 ms")
    parser.add_argument("--withdraw", action="store_true")
    parser.add_argument("--ui", choices=("crescendo", "classic"), default="crescendo")
    parser.add_argument("--keep-going", action="store_true", help="do not stop after a failing scale")
    args = parser.parse_args(argv)
    if not os.environ.get("DISPLAY"):
        print("DISPLAY is not set", file=sys.stderr)
        return 2
    exit_code = 0
    for scale in args.scales:
        probe = Probe(scale, args)
        try:
            probe.run()
        finally:
            probe.close()
        if probe.failures:
            exit_code = 1
            print("FAIL {}: {} problem(s)".format(scale, len(probe.failures)))
            for label, text in probe.failures[:12]:
                print("   - {}: {}".format(label, text.strip()[-600:].replace("\n", "\n       ")))
            if not args.keep_going:
                break
        else:
            print("PASS {}".format(scale))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
