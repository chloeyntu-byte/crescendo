"""Passive candidate and point timing helpers."""
from contextlib import contextmanager
import math
import time

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .diagnostic_timing import timing_seconds, timing_tick
from .utils import atomic_write_json, load_json


def timestamp(clock=None):
    if clock is None:
        return timing_tick()
    try:
        value = clock()
        return value if math.isfinite(value) else None
    except Exception:
        return None


def elapsed(started, finished):
    if started is None or finished is None:
        return None
    return timing_seconds(finished - started)


@contextmanager
def measure(phases, name, **details):
    started = timestamp()
    try:
        yield
    finally:
        phases[name] = dict(details, elapsed_seconds=elapsed(started, timestamp()))


@contextmanager
def timed_lock(lock, phases, name):
    with measure(phases, name):
        lock.acquire()
    try:
        yield
    finally:
        lock.release()


def save_timing(path, timing):
    try:
        atomic_write_json(path, timing)
    except Exception:
        # Timing is optional telemetry. The candidate data has its own commit.
        pass


def update_cleanup_timing(path, **phase):
    try:
        timing = load_json(path)
        timing.setdefault("phases", {})["cleanup"] = phase
        save_timing(path, timing)
    except Exception:
        # An unavailable timing sidecar does not invalidate completed cleanup.
        pass


def summarize_tasks(tasks, name):
    measured = []
    for task in tasks:
        phases = task.get("phases") if isinstance(task, dict) else None
        phase = phases.get(name) if isinstance(phases, dict) else None
        value = timing_seconds(phase.get("elapsed_seconds")) if isinstance(phase, dict) else None
        if value is not None:
            measured.append(value)
    return {"elapsed_seconds": timing_seconds(sum(measured)) if measured else None,
            "count": len(measured), "total_count": len(tasks), "scope": "task_sum"}


class PendingSamples(object):
    """Time between consecutive exact pending samples, from existing polls."""
    def __init__(self):
        self.query_count = 0
        self.previous = None
        self.intervals = 0
        self.total = 0.0

    def observe(self, record, job_id):
        count = record.get("query_count") if isinstance(record, dict) else None
        if type(count) is not int or count < 0:
            self.previous = None
            return
        if count <= self.query_count:
            return
        if count != self.query_count + 1:
            self.previous = None
        self.query_count = count
        observation = record.get("latest")
        pending = (job_id is not None and isinstance(observation, dict)
                   and observation.get("job_id") == job_id
                   and observation.get("reason") == "exact_job_nonterminal"
                   and not observation.get("invalid") and not observation.get("not_found")
                   and observation.get("state") in ("PENDING", "QUEUED"))
        now = timestamp() if pending else None
        duration = elapsed(self.previous, now)
        if duration is not None:
            self.intervals += 1
            self.total += duration
        self.previous = now

    def phase(self):
        return {"elapsed_seconds": self.total if self.intervals else None,
                "source": "consecutive_exact_pending_samples", "count": self.intervals}


class ConcurrencyObserver(object):
    """Observed start/terminal events, independent of configured pool limits."""
    def __init__(self):
        self.generations = {}

    def observe(self, event):
        candidate = event.get("candidate_id") or ""
        if not candidate.startswith("g") or "_c" not in candidate:
            return
        generation = candidate.split("_c", 1)[0]
        row = self.generations.setdefault(generation, {"active": {"solver": set(), "reader": set()},
            "peak": {"solver": 0, "reader": 0}, "events": []})
        stage = event.get("stage")
        transitions = {"solve": ("solver", True), "solver_terminal": ("solver", False),
                       "solve_completed": ("solver", False), "measurement": ("reader", True),
                       "measurement_terminal": ("reader", False)}
        if stage not in transitions:
            return
        kind, start = transitions[stage]
        key = (candidate, event.get("task_id"))
        active = row["active"][kind]
        changed = key not in active if start else key in active
        if not changed:
            return
        if start:
            active.add(key)
        else:
            active.discard(key)
        row["peak"][kind] = max(row["peak"][kind], len(active))
        row["events"].append({"stage": stage, "candidate_id": candidate,
            "task_id": event.get("task_id"), "observed_epoch": timestamp(time.time),
            "active": len(active)})

    def snapshot(self, generation, policy):
        row = self.generations.get("g%03d" % generation)
        capacities = {"global": policy["global"]["solver_capacity"],
                      "tests": {key: value["solver_capacity"] for key, value in policy["tests"].items()},
                      "reader": {"requested": policy["measurement_requested"],
                          "effective": policy["measurement_jobs"],
                          "reason": "default_min_2_global" if policy["measurement_requested"] in (None, "") else ("explicit_reader_jobs_capped_by_global" if int(policy["measurement_requested"]) > policy["measurement_jobs"] else "explicit_reader_jobs")}}
        return {"schema_version": _contract_text("crescendo.generation_capacity.v1"), "generation": generation + 1,
                "capacity": capacities, "limits": {"global_jobs": policy["max_jobs"], "local_jobs": policy["local_solver_max_jobs"]},
                "observed_peak": dict(row["peak"]) if row else {"solver": None, "reader": None},
                "source": "observed_start_terminal_events", "events": list(row["events"]) if row else []}
