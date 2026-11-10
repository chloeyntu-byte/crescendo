"""In-memory demo backend for the crescendo_engine Tk GUI (no EDA tools needed).

``DemoService`` implements the service surface used by
``crescendo_engine.gui.OptimizerGui`` with deterministic synthetic data:

* a Maestro-like setup with several Tests, corners, outputs and a nested
  schematic hierarchy of configurable size (small / large / huge),
* a believable multi-objective optimisation running in a background thread,
  whose candidates improve smoothly towards a real Pareto front,
* "everything OK" payloads for the environment diagnostics, EDA discovery and
  private-suite pages.

Python 3.6 / Tk 8.5 compatible, standard library only (plus the vendored
kernel modules for metric maths and CSV / dataset export).
"""
from __future__ import print_function

import collections
import copy
import hashlib
import json
import math
import os
import random
import shutil
import tempfile
import threading
import time

from crescendo_engine.metrics import constraint_violation, objective_score
from crescendo_engine.multiobjective import build_objective_presentation

SCALES = {
    "small": {"cells": 15, "outputs": 6, "corners": 5, "max_depth": 3},
    "large": {"cells": 800, "outputs": 150, "corners": 40, "max_depth": 6},
    "huge": {"cells": 4000, "outputs": 240, "corners": 40, "max_depth": 8},
}

TEST_NAMES = ("AC_GAIN_PM", "TRAN_STEP", "NOISE_INT", "PSRR_DC", "CMRR_AC",
              "STARTUP_TRAN", "LINEARITY", "OFFSET_MC")
TEST_ANALYSES = {
    "AC_GAIN_PM": ("dc", "ac"),
    "TRAN_STEP": ("dc", "tran"),
    "NOISE_INT": ("dc", "ac", "noise"),
}

LIB = "demo_lib"
PDK = "demo_pdk18"

BLOCKS_L1 = ("input_stage", "bias_core", "output_stage", "cmfb", "startup",
             "ref_gen", "clk_buf", "gm_cell", "comp_net")
BLOCKS_L2 = ("diff_pair", "cs_stage", "mirror_n", "mirror_p", "cascode",
             "level_shift", "src_follower", "tail_src")
BLOCKS_L3 = ("unit_cell", "sw_array", "dac_slice", "res_ladder", "cap_bank",
             "mos_unit", "dummy_ring")

_SCALE_LETTERS = ((1e12, "t"), (1e9, "g"), (1e6, "meg"), (1e3, "k"), (1.0, ""),
                  (1e-3, "m"), (1e-6, "u"), (1e-9, "n"), (1e-12, "p"), (1e-15, "f"))

NAMED_OUTPUTS = (
    ("gain_dB", "dB", 0.04), ("ugbw", "Hz", 0.15), ("pm", "deg", 0.06),
    ("power", "W", 0.20), ("noise", "V/rtHz", 0.10), ("swing", "V", 0.05),
)

_TERMINAL_RUN_STATES = ("completed", "failed", "stopped")


def _eng(value):
    """Format a SI number the way CDF fields do: 180n, 2u, 12.5k, 8."""
    if value == 0:
        return "0"
    magnitude = abs(value)
    for scale, letter in _SCALE_LETTERS:
        if magnitude >= scale * 0.9999:
            return "{:.4g}{}".format(value / scale, letter)
    return "{:.4g}f".format(value / 1e-15)


def _digest(*parts):
    return hashlib.md5("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def _clamp(value, low, high):
    return low if value < low else high if value > high else value


class _Hierarchy(object):
    """Deterministic schematic hierarchy for one Test (cells, devices, parameters)."""

    def __init__(self, service, test_index, test_name):
        self.test_name = test_name
        self.tb_cell = "tb_" + test_name.lower()
        rng = random.Random(service.seed * 7919 + test_index * 104729)
        count = service.cells
        max_depth = service.max_depth
        self.cells = [{"path": "/", "parent": None, "depth": 0, "name": self.tb_cell,
                       "cell": self.tb_cell, "children": [], "devices": []}]
        queue = collections.deque([0])
        counters = {}
        while len(self.cells) < count:
            if not queue:
                # budget not used up: allow deeper levels
                max_depth += 1
                queue.extend(i for i, c in enumerate(self.cells) if c["depth"] == max_depth - 1)
                if not queue:
                    break
            parent = queue.popleft()
            node = self.cells[parent]
            if node["depth"] >= max_depth:
                continue
            fanout = rng.randint(2, 5) if node["depth"] else rng.randint(3, 6)
            for _ in range(fanout):
                if len(self.cells) >= count:
                    break
                depth = node["depth"] + 1
                catalogue = BLOCKS_L1 if depth == 1 else BLOCKS_L2 if depth == 2 else BLOCKS_L3
                master = rng.choice(catalogue)
                counters[master] = counters.get(master, 0) + 1
                name = "X{}{}".format(master.upper().replace("_", ""), counters[master] - 1)
                path = (node["path"].rstrip("/") + "/" + name)
                index = len(self.cells)
                self.cells.append({"path": path, "parent": parent, "depth": depth, "name": name,
                                   "cell": master, "children": [], "devices": []})
                node["children"].append(index)
                queue.append(index)
        self.devices = []
        for index, node in enumerate(self.cells):
            if node["depth"] == 0:
                total = rng.randint(1, 2)  # testbench passives
            else:
                total = rng.randint(2, service.devices_per_cell)
            for number in range(total):
                kind = rng.choice(("nmos", "nmos", "pmos", "pmos", "nmos", "pmos", "res", "cap"))
                prefix = {"nmos": "MN", "pmos": "MP", "res": "R", "cap": "C"}[kind]
                master = {"nmos": "nch_18", "pmos": "pch_18", "res": "rppoly", "cap": "mimcap"}[kind]
                name = "{}{}".format(prefix, number)
                path = node["path"].rstrip("/") + "/" + name
                device = {"path": path, "cell_index": index, "name": name, "kind": kind,
                          "master": master, "depth": node["depth"] + 1}
                device["params"] = self._parameters(device, rng)
                self.devices.append(device)
                node["devices"].append(len(self.devices) - 1)
        self.parameter_count = sum(len(d["params"]) for d in self.devices)
        design = {"library": LIB, "cell": self.tb_cell, "view": "schematic"}
        self.design = design
        self.scope_rows = []
        self.scope_index = {}
        for index, node in enumerate(self.cells):
            scope_id = "cell_scope:" + _digest(test_name, node["path"])[:20]
            parent_path = None
            if node["parent"] is not None:
                parent_path = self.cells[node["parent"]]["path"]
            library = LIB
            row = {
                "scope_id": scope_id, "design": dict(design), "hierarchy_path": node["path"],
                "library": library, "cell": node["cell"], "view": "schematic",
                "depth": node["depth"], "master_library": library, "master_cell": node["cell"],
                "master_view": "schematic", "parent_path": parent_path, "selectable": True,
            }
            self.scope_rows.append(row)
            self.scope_index[scope_id] = index

    @staticmethod
    def _parameters(device, rng):
        kind, path = device["kind"], device["path"]
        rows = []

        def add(name, prompt, semantic, value, value_type):
            if value_type == "integer":
                text = str(int(value))
            else:
                text = _eng(value)
            rows.append({
                "parameter_id": path + "/" + name, "name": name, "prompt": prompt,
                "semantic": semantic, "optimizable": True, "hierarchy_path": path,
                "instance_name": device["name"], "master_library": PDK,
                "master_cell": device["master"], "numeric_value": float(value),
                "current_value": text, "value_type": value_type, "cdf_type": "Numeric CDF",
            })
        if kind in ("nmos", "pmos"):
            add("w", "Total Width", "width", rng.choice((0.5, 1, 2, 4, 6, 10, 20, 40)) * 1e-6, "float")
            add("l", "Length", "length", rng.choice((180, 200, 250, 360, 500, 720, 1000)) * 1e-9, "float")
            add("nf", "Number of Fingers", "fingers", rng.choice((1, 2, 4, 8, 12, 16)), "integer")
            add("m", "Multiplier", "multiplier", rng.choice((1, 1, 2, 4)), "integer")
        elif kind == "res":
            add("r", "Resistance", "resistance", rng.choice((200, 500, 1e3, 2.5e3, 8e3, 25e3, 80e3)), "float")
            add("w", "Width", "width", rng.choice((0.5, 1, 2, 4)) * 1e-6, "float")
            add("l", "Length", "length", rng.choice((2, 5, 10, 20, 40)) * 1e-6, "float")
        else:
            add("c", "Capacitance", "capacitance", rng.choice((20, 50, 120, 250, 800)) * 1e-15, "float")
            add("m", "Multiplier", "multiplier", rng.choice((1, 2, 4, 8)), "integer")
        return rows

    def selection_payload(self, scope_ids):
        """Hierarchy / parameter payload for the selected scope subtrees."""
        selected = set(self.scope_index[s] for s in scope_ids if s in self.scope_index)
        inside = [False] * len(self.cells)
        for index, node in enumerate(self.cells):
            inside[index] = index in selected or (node["parent"] is not None and inside[node["parent"]])
        included = list(inside)
        for index in selected:
            parent = self.cells[index]["parent"]
            while parent is not None and not included[parent]:
                included[parent] = True
                parent = self.cells[parent]["parent"]
        cells, instances, hier_params, parameters = [], [], [], []
        for index, node in enumerate(self.cells):
            if not included[index]:
                continue
            cells.append({"hierarchy_path": node["path"], "library": LIB, "cell": node["cell"],
                          "view": "schematic", "depth": node["depth"]})
            if node["parent"] is not None:
                instances.append({
                    "hierarchy_path": node["path"], "parent_path": self.cells[node["parent"]]["path"],
                    "name": node["name"], "kind": "hierarchical", "master_library": LIB,
                    "master_cell": node["cell"], "depth": node["depth"]})
            if inside[index]:
                for device_index in node["devices"]:
                    device = self.devices[device_index]
                    instances.append({
                        "hierarchy_path": device["path"], "parent_path": node["path"],
                        "name": device["name"], "kind": "device", "master_library": PDK,
                        "master_cell": device["master"], "depth": device["depth"]})
                    for row in device["params"]:
                        parameters.append(row)
                        hier_params.append({"parameter_id": row["parameter_id"],
                                            "hierarchy_path": row["hierarchy_path"]})
        return {"designs": [{"design": dict(self.design),
                             "hierarchy": {"cells": cells, "instances": instances,
                                           "parameters": hier_params}}]}, parameters


class _Run(object):
    """State of one (simulated) optimisation run."""

    def __init__(self, run_id, spec, variables, generations, population, seed, run_root,
                 test_name=None, scope_ids=None, warnings=None):
        self.run_id = run_id
        self.spec = spec
        self.variables = variables
        self.generations = generations
        self.population = population
        self.seed = seed
        self.run_root = run_root
        self.test_name = test_name
        self.scope_ids = list(scope_ids or [])
        self.warnings = list(warnings or [])
        self.rng = random.Random(seed)
        count = len(variables)
        self.center = [self.rng.uniform(0.25, 0.75) for _ in range(count)]
        self.direction = [self.rng.choice((-1.0, 1.0)) * self.rng.uniform(0.15, 0.40) for _ in range(count)]
        norm = sum(value * value for value in self.direction)
        if norm < 0.12:
            factor = math.sqrt(0.12 / max(norm, 1e-9))
            self.direction = [value * factor for value in self.direction]
        self.direction_norm = sum(value * value for value in self.direction)
        self.candidates = collections.OrderedDict()
        self.revisions = {}
        self.revision = 0
        self.genomes = {}
        self.state = "queued"
        self.message = "Optimization registered."
        self.generation = 0
        self.next_generation = 0
        self.thread = None
        self.stop_event = threading.Event()
        self.started_at = time.time()
        self.updated_at = self.started_at
        self.presentation = None
        self.presentation_revision = -1
        self.active = []
        self.recent_commands = []
        self.extra_index = {}
        self.continuation = None

    def bump(self, candidate_id=None):
        self.revision += 1
        self.updated_at = time.time()
        if candidate_id is not None:
            self.revisions[candidate_id] = self.revision

    def counts(self):
        done = failed = infra = 0
        for row in self.candidates.values():
            status = row.get("status")
            if status == "completed":
                done += 1
            elif status == "failed":
                failed += 1
            elif status == "infrastructure_failed":
                infra += 1
        return done, failed, infra


class DemoService(object):
    """Drop-in replacement for ``OptimizerService`` backed by synthetic data."""

    def __init__(self, scale="small", tests=3, cells=None, outputs=None, corners=None,
                 latency=1.0, speed=1.0, generations=12, population=16, seed=2045,
                 devices_per_cell=None, max_run_variables=1500, jobs=4,
                 workdir=None, config=None, history_runs=2):
        preset = SCALES.get(scale)
        if preset is None:
            raise ValueError("scale must be one of: {}".format(", ".join(sorted(SCALES))))
        self.scale = scale
        self.seed = int(seed)
        self.tests = max(1, int(tests))
        self.cells = int(cells if cells is not None else preset["cells"])
        self.max_depth = int(preset["max_depth"])
        self.output_count = max(6, int(outputs if outputs is not None else preset["outputs"]))
        self.corner_count = max(1, int(corners if corners is not None else preset["corners"]))
        self.devices_per_cell = int(devices_per_cell if devices_per_cell is not None
                                    else (4 if self.cells <= 50 else 5))
        self.latency = float(latency or 0.0)
        self.speed = max(0.01, float(speed))
        self.generations = int(generations) if generations else None
        self.population = int(population) if population else None
        self.max_run_variables = max(1, int(max_run_variables))
        self.jobs = max(1, int(jobs))
        self.lock = threading.RLock()
        self._owns_workdir = workdir is None
        self.workdir = workdir or tempfile.mkdtemp(prefix="crescendo-demo-")
        self.runs_root = os.path.join(self.workdir, "runs")
        os.makedirs(self.runs_root, exist_ok=True)
        self.config = config if config is not None else self._default_config()
        self._closing = threading.Event()
        self._rng = random.Random(self.seed)
        self._activity_events = []
        self._activity_revision = 0
        self._catalogs = {}
        self._inspections = {}
        self._prepared = {}
        self._hierarchies = {}
        self._runs = collections.OrderedDict()
        self._history = []
        self._suites = collections.OrderedDict()
        self._suite_configs = {}
        self._environment_tests = {}
        self._range_jobs = {}
        self._eda_manual = {}
        self._eda_mode = "auto"
        self._counter = 0
        self._build_catalogue()
        for number in range(max(0, int(history_runs))):
            self._register_history(number)
        self._set_activity("idle", "Demo service ready", "complete")

    # ------------------------------------------------------------------ setup
    def _default_config(self):
        runtime_root = os.path.join(self.workdir, "runtime")
        os.makedirs(runtime_root, exist_ok=True)
        return {
            "cadence": {"simulator": "spectre", "setup_mode": "auto"},
            "runtime": {"run_root": runtime_root, "execution_commands": {},
                        "command_scheduler": "auto", "storage_base_override": runtime_root,
                        "run_root_override": runtime_root},
        }

    def _build_catalogue(self):
        rng = random.Random(self.seed + 11)
        skews = {"tt": 0.0, "ss": -1.0, "ff": 1.0, "sf": -0.35, "fs": 0.35}
        classic = (("tt", 27, 1.8), ("ss", 125, 1.62), ("ff", -40, 1.98), ("sf", 27, 1.8), ("fs", 27, 1.8))
        combos = []
        if self.corner_count <= len(classic):
            combos = list(classic[:self.corner_count])
        else:
            for supply in (1.8, 1.62, 1.98, 1.71, 1.89):
                for temp in (27, -40, 125, 85, 0, 60):
                    for process in ("tt", "ss", "ff", "sf", "fs"):
                        combos.append((process, temp, supply))
        corners = []
        for process, temp, supply in combos[:self.corner_count]:
            name = "{}_{}c".format(process, str(temp).replace("-", "m"))
            if self.corner_count > len(classic):
                name += "_v{}".format(str(supply).replace(".", "p"))
            skew = _clamp(skews[process] * 0.7 + (temp - 27) / 150.0 + (supply - 1.8) / 0.36 * 0.3, -1.0, 1.0)
            corners.append({"name": name, "enabled": True, "skew": skew,
                            "variables": [{"name": "temperature", "value": str(temp)},
                                          {"name": "vdd", "value": str(supply)},
                                          {"name": "process", "value": process}]})
        while len(corners) < self.corner_count:
            number = len(corners)
            corners.append({"name": "mc_{:03d}".format(number), "enabled": True,
                            "skew": rng.uniform(-1, 1),
                            "variables": [{"name": "seed", "value": str(number)}]})
        self.corner_rows = corners
        self.corner_skew = dict((row["name"], row["skew"]) for row in corners)
        # outputs
        outputs = []
        for name, unit, spread in NAMED_OUTPUTS:
            outputs.append({"key": name, "unit": unit, "spread": spread, "kind": "named"})
        kinds = (("av", "dB", 0.05), ("gm", "S", 0.08), ("ibias", "A", 0.12), ("vov", "V", 0.06),
                 ("pm_loop", "deg", 0.05), ("bw", "Hz", 0.12), ("psrr", "dB", 0.06),
                 ("cmrr", "dB", 0.06), ("vout_dc", "V", 0.03), ("slew", "V/s", 0.10),
                 ("settle", "s", 0.10), ("thd", "dB", 0.08), ("cin", "F", 0.04), ("rout", "Ohm", 0.09))
        blocks = ("stg1", "stg2", "stg3", "bias", "cmfb", "buf", "ref", "load", "tail", "casc")
        extra = self.output_count - len(outputs)
        for number in range(extra):
            kind, unit, spread = kinds[number % len(kinds)]
            block = blocks[(number // len(kinds)) % len(blocks)]
            key = "{}_{}_{}".format(kind, block, number // (len(kinds) * len(blocks)) + 1) \
                if number >= len(kinds) * len(blocks) else "{}_{}".format(kind, block)
            outputs.append({
                "key": key, "unit": unit, "spread": spread, "kind": "generic",
                "scale": 10 ** rng.uniform(-6, 2) if unit not in ("dB", "deg") else rng.uniform(20, 70),
                "w1": rng.uniform(-0.6, 0.6), "w2": rng.uniform(-0.5, 0.5), "w3": rng.uniform(0.0, 0.8),
            })
        self.outputs = outputs
        self.output_by_key = dict((row["key"], row) for row in outputs)
        # tests
        names = list(TEST_NAMES)
        while len(names) < self.tests:
            names.append("TEST_{:02d}".format(len(names) + 1))
        self.test_rows = []
        for number, name in enumerate(names[:self.tests]):
            analyses = TEST_ANALYSES.get(name, ("dc", "ac", "tran"))
            self.test_rows.append({
                "name": name, "library": LIB, "cell": "tb_" + name.lower(), "view": "schematic",
                "enabled": number == 0 or number < 3, "simulator": "spectre",
                "analyses": [{"name": a, "enabled": True} for a in analyses],
            })

    # ---------------------------------------------------------------- helpers
    def _next_id(self, prefix):
        with self.lock:
            self._counter += 1
            return "{}_{:04d}".format(prefix, self._counter)

    def _set_activity(self, stage, message, state="running", level=None):
        with self.lock:
            self._activity_revision += 1
            self._activity_events.append({
                "revision": self._activity_revision, "stage": str(stage)[:96],
                "message": str(message)[:1200], "state": state, "time": time.time(),
                "level": level or ("ERROR" if state == "error" else "INFO")})
            self._activity_events = self._activity_events[-100:]

    def activity_snapshot(self):
        with self.lock:
            result = dict(self._activity_events[-1]) if self._activity_events else {
                "revision": 0, "stage": "idle", "message": "", "state": "complete"}
            result["events"] = copy.deepcopy(self._activity_events)
            return result

    def _pause(self, seconds):
        if self.latency <= 0 or seconds <= 0:
            return
        end = time.time() + seconds * self.latency * (0.85 + 0.3 * self._rng.random())
        while not self._closing.is_set():
            left = end - time.time()
            if left <= 0:
                return
            time.sleep(min(0.05, left))

    def _work(self, steps, total_seconds):
        """Emit stage events (used by the GUI gate progress) while sleeping."""
        per = float(total_seconds) / max(1, len(steps))
        for stage, message in steps:
            self._set_activity(stage, message)
            self._pause(per)

    def _size_factor(self):
        return 1.0 + self.cells / 2000.0

    def _locator(self, path):
        path = os.path.abspath(os.path.expanduser(path or "")) if path else ""
        if os.path.basename(path).lower() in ("maestro.sdb", "data.sdb"):
            setup_path = path
        else:
            setup_path = os.path.join(path or self.workdir, "maestro.sdb")
        view_dir = os.path.dirname(setup_path)
        cell_dir = os.path.dirname(view_dir)
        library_root = os.path.dirname(cell_dir)
        return {"backend": "maestro", "path": setup_path, "view_dir": view_dir,
                "view_name": os.path.basename(view_dir) or "maestro",
                "cell_dir": cell_dir, "cell_name": os.path.basename(cell_dir) or "OTA_TOP",
                "library_root": library_root, "library_name": os.path.basename(library_root) or LIB}

    def _hierarchy(self, test_name):
        with self.lock:
            cached = self._hierarchies.get(test_name)
        if cached is not None:
            return cached
        index = [row["name"] for row in self.test_rows].index(test_name)
        built = _Hierarchy(self, index, test_name)
        with self.lock:
            self._hierarchies[test_name] = built
        return built

    def _analysis_names(self):
        names = []
        for row in self.test_rows:
            for analysis in row["analyses"]:
                if analysis["name"] not in names:
                    names.append(analysis["name"])
        return names

    def _job_policy(self):
        properties = {"distributionmethod": "Command", "maxjobs": "8",
                      "jobsubmitcommand": "batch-submit -q demo -R \"cpu=4;mem=16000\"",
                      "runtimeout": "7200"}
        return {"schema_version": "crescendo.job_policy.v1", "source": "demo Maestro Job Setup",
                "available": True, "global": {"available": True, "properties": dict(properties)},
                "tests": [{"name": row["name"], "available": True, "properties": dict(properties)}
                          for row in self.test_rows],
                "run_options": {}, "warnings": []}

    def _save_options(self, locator):
        root = os.path.join(self.workdir, "simulation", locator["cell_name"])
        return {"source": "native_save_options",
                "effective": {"simulation_root": root, "rdb_root": os.path.join(root, "rdb")},
                "raw": {"save_res_dir": root, "save_dir": os.path.join(root, "rdb")}}

    def _setup_block(self, locator, test_rows, selected=None):
        masks = {}
        for row in test_rows:
            masks[row["name"]] = {"known": True, "nominal_enabled": True, "disabled_corners": [],
                                  "source": "demo native test mask"}
        setup = {
            "backend": "maestro", "tests": copy.deepcopy(test_rows),
            "corners": [{"name": c["name"], "enabled": True, "variables": copy.deepcopy(c["variables"])}
                        for c in self.corner_rows],
            "analysis_names": self._analysis_names(), "test_corner_masks": masks,
            "global_variables": [{"name": "VDD", "value": "1.8"}, {"name": "CL", "value": "2p"}],
            "model_groups": [{"name": "demo_pdk18.scs", "section": "tt"}],
            "execution_settings": [], "save_options": self._save_options(locator),
            "source": {"setup_db": locator["path"]},
        }
        if selected:
            setup["selected_test"] = selected
            setup["_source_tests"] = True
        return setup

    def _private_session(self, test_name, locator):
        session_id = _digest("suite", test_name, locator["path"])
        maestro = {"library": LIB, "cell": locator["cell_name"] + "_private", "view": "maestro",
                   "path": os.path.join(self.workdir, "private", session_id[:8], "maestro")}
        reference = {
            "session_id": session_id, "suite_id": session_id, "status": "ready",
            "revision": "r" + session_id[:8], "source_path": locator["path"],
            "selected_test": test_name, "maestro": maestro, "label": "Private suite · " + test_name,
            "configs": [{"library": LIB, "cell": "tb_" + test_name.lower(), "view": "config",
                         "path": maestro["path"].replace("maestro", "config")}],
            "views": [{"target": [LIB, maestro["cell"], "maestro"], "path": maestro["path"], "role": "maestro"},
                      {"target": [LIB, "tb_" + test_name.lower(), "schematic"],
                       "path": maestro["path"].replace("maestro", "schematic"), "role": "schematic"}],
            "private_views_read_only": True,
            "diagnostic_log": os.path.join(self.workdir, "private", session_id[:8], "setup.log"),
            "can_load": True, "can_delete": True, "can_recover": False, "warnings": [],
            "scope_ids": [],
        }
        with self.lock:
            existing = self._suites.get(session_id)
            if existing is None:
                self._suites[session_id] = reference
            else:
                reference = existing
        return copy.deepcopy(reference)

    # ------------------------------------------------------------ EDA / setup
    def eda_setup(self, setup=None):
        base = "/opt/cadence"
        effective = {
            "virtuoso_bin": base + "/IC618/tools/dfII/bin/virtuoso",
            "ocean_bin": base + "/IC618/tools/dfII/bin/ocean",
            "spectre_bin": base + "/SPECTRE181/bin/spectre",
            "cadence_root": base + "/IC618", "spectre_root": base + "/SPECTRE181",
            "cds_lib": base + "/IC618/share/cdssetup/cds.lib",
            "alps_bin": "/opt/empyrean/alps/bin/alps", "alps_root": "/opt/empyrean/alps",
            "alps_model_dir": "/opt/pdk/demo_pdk18/models/alps",
            "spectre_model_dir": "/opt/pdk/demo_pdk18/models/spectre",
            "simulator": "spectre", "simulator_threads": 8, "simulator_result_reader": "spectre",
            "alps_model_file_map": {}, "spectre_result_reader": "spectre", "alps_result_reader": "spectre",
            "license_file": base + "/license/license.dat",
            "extra_binary_paths": [base + "/IC618/bin", base + "/SPECTRE181/bin"],
            "extra_library_paths": [base + "/IC618/tools.lnx86/lib/64bit"],
            "search_roots": [base], "pdk_name": PDK, "pdk_dir": "/opt/pdk/demo_pdk18",
            "pdk_adapter": "generic-cdf", "pdk_adapter_policy": None, "pdk_profiles": [],
            "model_roots": ["/opt/pdk/demo_pdk18/models"], "model_path_remaps": [],
            "startup_extensions": [],
        }
        effective.update(dict((k, v) for k, v in self._eda_manual.items() if v not in (None, "", [])))
        sources = dict((key, "automatic discovery (demo)") for key in effective)
        names = ("virtuoso_bin", "ocean_bin", "spectre_bin", "alps_bin", "cds_lib", "license_file")
        checks = [{"name": name, "ok": True, "optional": name == "alps_bin",
                   "message": effective.get(name)} for name in names]
        try:
            from crescendo_engine.pdk import adapter_policy_options
            options = adapter_policy_options()
        except Exception:
            options = [{"id": "auto", "label": "Auto (Generic First)", "mode": "auto"},
                       {"id": "generic-only", "label": "Generic Only", "mode": "generic-only"},
                       {"id": "custom", "label": "Custom", "mode": "custom"}]
        profiles = {}
        for name in ("spectre", "alps"):
            profiles[name] = {"status": "ready", "executable": effective[name + "_bin"],
                              "model_dir": effective[name + "_model_dir"], "result_reader": "spectre",
                              "reason_code": "model_structure_available", "reasons": [],
                              "model_dir_origin": "demo discovery"}
        return {
            "schema_version": "crescendo.eda_setup.v2", "mode": self._eda_mode,
            "user_config_path": os.path.join(self.workdir, "eda.json"), "ok": True,
            "manual": copy.deepcopy(self._eda_manual), "effective": effective,
            "pdk_adapter_options": options, "sources": sources, "checks": checks,
            "version_checks": [], "missing": [], "recommendations": [], "compatibility": {},
            "simulator_profiles": {"profiles": profiles}, "model_discovery": {},
        }

    def update_eda_setup(self, body):
        self._pause(0.4)
        body = body or {}
        mode = body.get("mode", "manual")
        if mode not in ("auto", "manual"):
            raise ValueError("EDA setup mode must be auto or manual")
        with self.lock:
            self._eda_mode = mode
            self._eda_manual = dict(body.get("manual") or {}) if mode == "manual" else {}
        setup = self.eda_setup()
        setup["prepared_invalidated"] = False
        return setup

    # ------------------------------------------------------- inspect / prepare
    def inspect_tests(self, path, backend=None):
        self._work([("saved_setup", "Reading saved Maestro setup (demo)"),
                    ("inspection_complete", "Test catalogue ready")], 0.5)
        locator = self._locator(path)
        setup = self._setup_block(locator, self.test_rows)
        preview = {"setup_path": locator["path"], "setup_locator": locator, "setup": setup,
                   "tests": copy.deepcopy(self.test_rows), "hierarchy": {"designs": []},
                   "eda_context": {"pdk": {"pdk_name": PDK}}}
        catalog_id = self._next_id("catalog")
        with self.lock:
            self._catalogs[catalog_id] = preview
        return catalog_id, preview

    def inspect_test_cells(self, test_inspection_id, test_name, max_depth=32):
        with self.lock:
            catalog = self._catalogs.get(test_inspection_id)
        if catalog is None:
            raise ValueError("Test inspection is no longer current; Load and Inspect again.")
        if test_name not in [row["name"] for row in self.test_rows]:
            raise ValueError("Unknown Test: {}".format(test_name))
        factor = self._size_factor()
        self._set_activity("saved_setup", "Reading saved setup for Test " + test_name)
        self._pause(0.35)
        self._set_activity("job_policy", "Capturing Maestro Job Setup")
        self._pause(0.3)
        self._set_activity("hierarchy", "Scanning schematic hierarchy")
        hierarchy = self._hierarchy(test_name)
        self._pause(0.9 * factor)
        self._set_activity("private_copy", "Creating private Maestro / config views")
        self._pause(0.5)
        locator = catalog["setup_locator"]
        row = next(r for r in self.test_rows if r["name"] == test_name)
        preview = {
            "setup_path": locator["path"], "setup_locator": copy.deepcopy(locator),
            "setup": self._setup_block(locator, [row], selected=test_name),
            "hierarchy": {"designs": []}, "cell_scopes": hierarchy.scope_rows,
            "eda_context": {"pdk": {"pdk_name": PDK}}, "job_policy": self._job_policy(),
            "setup_private_session": self._private_session(test_name, locator),
            "save_options": self._save_options(locator),
        }
        self._set_activity("inspection_complete", "{} cells ready".format(len(hierarchy.scope_rows)), "complete")
        inspection_id = self._next_id("inspection")
        with self.lock:
            self._inspections[inspection_id] = {"preview": preview, "test": test_name}
        return inspection_id, preview

    def inspect_cells(self, path, max_depth=32, backend=None, selected_test=None):
        catalog_id, catalog = self.inspect_tests(path, backend=backend)
        names = [row["name"] for row in catalog["tests"]]
        name = selected_test or (names[0] if len(names) == 1 else None)
        if name is None:
            raise ValueError("Select exactly one Test with a Test name.")
        return self.inspect_test_cells(catalog_id, name, max_depth=max_depth)

    def prepare_cells(self, inspection_id, scope_ids):
        with self.lock:
            record = self._inspections.get(inspection_id)
        if record is None:
            raise ValueError("Cell inspection is no longer current; Load Cells again.")
        if not scope_ids:
            raise ValueError("Select at least one cell before Apply Cells.")
        preview, test_name = record["preview"], record["test"]
        hierarchy = self._hierarchy(test_name)
        missing = [s for s in scope_ids if s not in hierarchy.scope_index]
        if missing:
            raise ValueError("Selected cell is absent from this inspection; Load Cells again.")
        factor = self._size_factor()
        self._set_activity("parameter_scan", "Reading parameters in selected cells")
        self._pause(1.0 * factor)
        body, parameters = hierarchy.selection_payload(scope_ids)
        self._set_activity("private_apply", "Applying selected cells to the private setup")
        self._pause(0.5 * factor)
        scopes = [hierarchy.scope_rows[hierarchy.scope_index[s]] for s in scope_ids]
        prepared = self._prepared_from(preview, hierarchy, scope_ids, scopes, body, parameters)
        self._set_activity("parameters_ready", "{} parameters ready".format(len(parameters)), "complete")
        prepared_id = self._next_id("prepared")
        with self.lock:
            self._prepared[prepared_id] = {"prepared": prepared, "test": test_name,
                                           "scope_ids": list(scope_ids)}
            suite = self._suites.get(preview["setup_private_session"]["session_id"])
            if suite is not None:
                suite["scope_ids"] = list(scope_ids)
        return prepared_id, prepared

    def _prepared_from(self, preview, hierarchy, scope_ids, scopes, body, parameters):
        prepared = {
            "setup_path": preview["setup_path"], "setup_locator": copy.deepcopy(preview["setup_locator"]),
            "setup": preview["setup"], "hierarchy": body, "parameters": parameters,
            "outputs": [{"key": row["key"], "unit": row["unit"]} for row in self.outputs],
            "cell_selection": {"scope_ids": list(scope_ids), "scopes": scopes},
            "resident_validation": {"state": "deferred"},
            "eda_context": {"pdk": {"pdk_name": PDK, "pdk_dir": "/opt/pdk/demo_pdk18"}},
            "job_policy": preview.get("job_policy"),
            "setup_private_session": preview.get("setup_private_session"),
            "save_options": preview.get("save_options"),
        }
        return prepared

    # ------------------------------------------------------------ range check
    def validate_static_ranges(self, prepared_id, variables):
        with self.lock:
            record = self._prepared.get(prepared_id)
        if record is None:
            raise ValueError("prepared setup not found; load the ADE setup again")
        self._pause(0.15)
        return {"valid": True, "variables": variables, "issues": []}

    def validate_ranges(self, prepared_id, variables):
        with self.lock:
            if prepared_id not in self._prepared:
                raise ValueError("prepared setup not found; load the ADE setup again")
        self._pause(0.3)
        job_id = "range_validation_" + self._next_id("job")
        rng = random.Random(_digest(self.seed, job_id))
        results = []
        for row in variables:
            for endpoint in ("minimum", "maximum"):
                value = row.get(endpoint)
                status, effective, error = "pdk_ok", value, None
                roll = rng.random()
                if roll < 0.04 and not isinstance(value, int):
                    status = "pdk_adjusted"
                    effective = _eng(round(float(value) / 5e-9) * 5e-9 + 5e-9)
                elif roll < 0.055:
                    status, error = "validation_failed", "CDF callback timed out"
                results.append({"parameter_id": row["parameter_id"], "endpoint": endpoint, "status": status,
                                "requested": value, "effective": effective, "error": error})
        duration = min(6.0, 1.5 + len(results) / 4000.0) * (self.latency if self.latency > 0 else 0.0)
        job = {"job_id": job_id, "prepared_id": prepared_id, "results": results, "start": time.time(),
               "duration": duration, "cancelled": False, "parameter_count": len(variables)}
        with self.lock:
            self._range_jobs[job_id] = job
        return {"job_id": job_id, "state": "running"}

    def range_validation_status(self, job_id):
        with self.lock:
            job = self._range_jobs.get(job_id)
        if job is None:
            return None
        elapsed = time.time() - job["start"]
        fraction = 1.0 if job["duration"] <= 0 else _clamp(elapsed / job["duration"], 0.0, 1.0)
        total = len(job["results"])
        done = int(total * fraction)
        if job["cancelled"]:
            state = "cancelled"
        elif fraction >= 1.0:
            state = "completed"
        else:
            state = "running"
        return {"job_id": job_id, "state": state, "phase": "pdk_callbacks", "stage": "pdk_callbacks",
                "message": "{} / {} endpoints checked".format(done, total), "completed": done,
                "total": total, "parameter_count": job["parameter_count"],
                "results": job["results"][:done], "cleanup_proven": True}

    def cancel_range_validations(self, prepared_id=None):
        cancelled = []
        with self.lock:
            for job_id, job in self._range_jobs.items():
                if prepared_id is not None and job["prepared_id"] != prepared_id:
                    continue
                if not job["cancelled"] and time.time() - job["start"] < job["duration"]:
                    job["cancelled"] = True
                    cancelled.append(job_id)
        return cancelled

    # --------------------------------------------------------- configuration
    def load_configuration(self, document, path=None, max_depth=32, selected_scope_ids=None,
                           selected_test=None):
        settings = (document or {}).get("settings") or {}
        saved_test = (settings.get("setup") or {}).get("selected_test")
        names = [row["name"] for row in self.test_rows]
        test = selected_test if selected_test in names else saved_test if saved_test in names else names[0]
        catalog_id, catalog = self.inspect_tests(path or self.workdir)
        inspection_id, inspection = self.inspect_test_cells(catalog_id, test, max_depth=max_depth)
        hierarchy = self._hierarchy(test)
        wanted = [s for s in (selected_scope_ids or []) if s in hierarchy.scope_index]
        if not wanted:
            paths = set()
            for row in settings.get("cells") or []:
                evidence = (row or {}).get("evidence") or {}
                if evidence.get("hierarchy_path"):
                    paths.add(evidence["hierarchy_path"])
            wanted = [r["scope_id"] for r in hierarchy.scope_rows if r["hierarchy_path"] in paths]
        result = {"document": document, "selected_test": test, "test_inspection_id": catalog_id,
                  "test_inspection": catalog, "inspection_id": inspection_id, "inspection": inspection,
                  "selected_cell_scope_ids": list(wanted), "setup_path": catalog["setup_path"],
                  "warnings": [], "suite": self._suite_row(inspection["setup_private_session"]),
                  "eda_setup": self.eda_setup()}
        if wanted:
            prepared_id, prepared = self.prepare_cells(inspection_id, wanted)
            result["prepared_id"], result["prepared"] = prepared_id, prepared
        return result

    def load_design_config(self, document, path=None):
        raise ValueError("Legacy design configs are not supported by the demo service.")

    # --------------------------------------------------------- private suites
    @staticmethod
    def _suite_row(reference):
        members = []
        for row in reference.get("views") or []:
            member = dict(zip(("library", "cell", "view"), row.get("target") or []))
            member.update(path=row.get("path"), role=row.get("role"))
            members.append(member)
        return dict(reference, id=reference.get("session_id"), name=reference.get("label") or "Private suite",
                    members=members)

    def list_private_suites(self):
        self._pause(0.2)
        with self.lock:
            rows = [self._suite_row(copy.deepcopy(r)) for r in self._suites.values()]
        return {"suites": rows, "deleted_suites": [], "warnings": []}

    def _suite_or_raise(self, suite_id):
        with self.lock:
            reference = self._suites.get(suite_id)
        if reference is None:
            raise ValueError("Invalid private suite identifier")
        return reference

    def load_private_suite(self, suite_id, source_path=None):
        reference = self._suite_or_raise(suite_id)
        self._set_activity("suite_open", "Opening private suite")
        test = reference["selected_test"]
        locator = self._locator(source_path or reference["source_path"])
        catalog_id, catalog = self.inspect_tests(locator["path"])
        inspection_id, inspection = self.inspect_test_cells(catalog_id, test)
        self._set_activity("suite_compare", "Comparing suite with the saved source")
        payload = {"suite": self._suite_row(copy.deepcopy(reference)), "selected_test": test,
                   "test_inspection_id": catalog_id, "test_inspection": catalog,
                   "inspection_id": inspection_id, "inspection": inspection,
                   "selected_cell_scope_ids": list(reference.get("scope_ids") or []),
                   "suite_configuration_revision": self.private_suite_configuration_revision(suite_id),
                   "warnings": []}
        saved = self._suite_configs.get(suite_id)
        if saved:
            payload["saved_configuration"] = copy.deepcopy(saved["document"])
        if reference.get("scope_ids"):
            prepared_id, prepared = self.prepare_cells(inspection_id, reference["scope_ids"])
            payload["prepared_id"], payload["prepared"] = prepared_id, prepared
        self._set_activity("suite_ready", "Private suite ready", "complete")
        return payload

    def update_private_suite(self, suite_id, sections):
        self._set_activity("suite_merge", "Merging selected sections: " + ", ".join(sections or []))
        self._pause(0.5)
        return self.load_private_suite(suite_id)

    def private_suite_configuration_revision(self, suite_id):
        self._suite_or_raise(suite_id)
        with self.lock:
            return (self._suite_configs.get(suite_id) or {}).get("revision", "r0")

    def save_private_suite_configuration(self, suite_id, document, expected_revision=None):
        self._suite_or_raise(suite_id)
        with self.lock:
            number = int(((self._suite_configs.get(suite_id) or {}).get("revision", "r0"))[1:] or 0) + 1
            revision = "r{}".format(number)
            self._suite_configs[suite_id] = {"document": copy.deepcopy(document), "revision": revision}
        return {"revision": revision, "saved": True}

    def rename_private_suite(self, suite_id, name):
        reference = self._suite_or_raise(suite_id)
        self._pause(0.4)
        with self.lock:
            reference["label"] = str(name)
            row = self._suite_row(copy.deepcopy(reference))
        return {"suite": row, "warnings": []}

    def recover_private_suite(self, suite_id):
        self._suite_or_raise(suite_id)
        return {"recovered": True, "warnings": []}

    def delete_private_suite(self, suite_id):
        self._suite_or_raise(suite_id)
        self._pause(0.3)
        with self.lock:
            self._suites.pop(suite_id, None)
        return {"deleted": True, "warnings": [], "retained_metadata_root": os.path.join(self.workdir, "retained")}

    def setup_private_session_info(self, inspection_id=None, prepared_id=None):
        with self.lock:
            record = self._inspections.get(inspection_id) if inspection_id else None
            prepared = self._prepared.get(prepared_id) if prepared_id else None
        reference = None
        if record is not None:
            reference = record["preview"].get("setup_private_session")
        elif prepared is not None:
            reference = prepared["prepared"].get("setup_private_session")
        if not reference:
            raise ValueError("No private Maestro/config views are bound to this inspection.")
        return copy.deepcopy(reference)

    # ----------------------------------------------------- environment tests
    def start_environment_test(self, prepared_id, spec=None):
        self._pause(0.3)
        test_id = self._next_id("envtest")
        duration = 5.0 * self.latency / max(1.0, self.speed ** 0.5) if self.latency > 0 else 0.3
        with self.lock:
            self._environment_tests[test_id] = {"start": time.time(), "duration": max(0.3, duration),
                                                "spec": spec or {}, "stopped": False}
        return {"test_id": test_id, "state": "queued", "stage": "queued", "message": "Diagnostics queued"}

    _ENV_CHECKS = (
        ("host.runtime", "Host / Python runtime", "CentOS 7, Python 3.6.8, Tk 8.5", None),
        ("native.version.virtuoso_bin", "Virtuoso version", "IC6.1.8-64b.500.20", None),
        ("native.version.spectre_bin", "Spectre version", "SPECTRE18.1.0 64bit", None),
        ("storage.filesystem", "Simulation directory writable", "demo simulation root is writable", None),
        ("storage.policy", "Maestro results directory", "inherited from Maestro save options", None),
        ("simulator.readiness", "Solver and models ready", "spectre + demo_pdk18 models resolved", None),
        ("scheduler.query_capability", "Scheduler query command", "User query template configured", None),
        ("scheduler.cli_capability", "Scheduler submission interface", "User submission template configured", None),
        ("coverage.test_selection", "Test selection", "1 Test enabled", "private_netlist"),
        ("coverage.corner_mask", "Corner enablement", "corner mask matches saved state", "private_netlist"),
        ("coverage.parameters", "Parameter coverage", "all swept parameters resolved", "private_netlist"),
        ("parameter_application", "Candidate parameters applied", "round trip verified", "private_netlist"),
        ("completion_evidence", "Simulation completed", "PSF results present", "private_netlist"),
        ("metrics", "Objective evaluation", "all outputs evaluated", "private_netlist"),
        ("termination_and_cleanup", "Task cleanup", "no residual processes", "private_netlist"),
        ("test.summary", "Overall result", "All checks passed", None),
    )

    def environment_test_progress(self, test_id):
        with self.lock:
            record = self._environment_tests.get(test_id)
        if record is None:
            raise ValueError("environment test not found")
        elapsed = time.time() - record["start"]
        fraction = _clamp(elapsed / record["duration"], 0.0, 1.0)
        total = len(self._ENV_CHECKS)
        if record["stopped"]:
            state = "cancelled"
        elif fraction >= 1.0:
            state = "completed"
        elif fraction <= 0.0:
            state = "queued"
        else:
            state = "running"
        shown = total if state == "completed" else int(total * fraction)
        checks = []
        for identifier, name, message, engine in self._ENV_CHECKS[:shown]:
            row = {"id": identifier, "check_id": identifier, "name": name, "status": "PASS",
                   "message": message}
            if engine:
                row.update(engine=engine, simulator="spectre")
            checks.append(row)
        log = os.path.join(self.workdir, "diagnostics", test_id + ".log")
        stage = "complete" if state == "completed" else "checks"
        engines = [{"engine": "maestro_baseline", "simulator": "spectre", "state": "completed" if shown >= 13 else "running"},
                   {"engine": "private_netlist", "simulator": "spectre", "state": "completed" if shown >= 13 else "running"}]
        return {
            "test_id": test_id, "state": state, "stage": stage,
            "message": "{} / {} checks".format(shown, total), "checks": checks,
            "checks_revision": "{}:{}".format(state, shown), "main_log": log,
            "simulator_logs": {"spectre": log + ".spectre"}, "run_ids": [], "case_run_ids": {},
            "case_results": [dict(e, case_id="spectre." + e["engine"], timing={"available": False})
                             for e in engines],
            "log_tail": "demo diagnostics: {} checks passed\n".format(shown), "report_ready": True,
        }

    def stop_environment_test(self, test_id):
        with self.lock:
            record = self._environment_tests.get(test_id)
            if record is None:
                raise ValueError("environment test not found")
            record["stopped"] = True
        return {"test_id": test_id, "state": "cancelling"}

    # ------------------------------------------------------------- optimiser
    def _variable_pool(self, variables):
        """Return (subset used for evolution, warnings)."""
        warnings = []
        if len(variables) > self.max_run_variables:
            step = int(math.ceil(len(variables) / float(self.max_run_variables)))
            subset = variables[::step][:self.max_run_variables]
            warnings.append("Demo evolves {} of {} selected parameters (max_run_variables={}).".format(
                len(subset), len(variables), self.max_run_variables))
            return subset, warnings
        return list(variables), warnings

    def start(self, prepared_id, spec):
        self._pause(0.4)
        with self.lock:
            record = self._prepared.get(prepared_id)
        if record is None:
            raise ValueError("prepared setup not found; load the ADE setup again")
        spec = dict(spec)
        variables = spec.get("variables") or []
        if not variables:
            raise ValueError("Select at least one optimizable parameter.")
        if not spec.get("objectives"):
            raise ValueError("Add at least one objective.")
        for definition in list(spec["objectives"]) + list(spec.get("constraints") or []):
            if definition.get("output_key") not in self.output_by_key:
                raise ValueError("optimization metric bindings are missing from the prepared setup (outputs: {})"
                                 .format(definition.get("output_key")))
        spec["selected_test"] = record["test"]
        run = self._new_run(spec, record, variables)
        self._launch(run)
        return {"run_id": run.run_id, "run_root": run.run_root, "state": "queued"}

    def _new_run(self, spec, record, variables, run_id=None):
        subset, warnings = self._variable_pool(variables)
        generations = self.generations or int(spec.get("generations") or 12)
        population = self.population or int(((spec.get("algorithm_parameters") or {}).get("population")) or 16)
        run_id = run_id or "run_{}_{}".format(time.strftime("%Y%m%d_%H%M%S"), self._next_id("r")[-4:])
        run_root = os.path.join(self.runs_root, run_id)
        os.makedirs(run_root, exist_ok=True)
        seed = int((spec.get("algorithm_parameters") or {}).get("seed") or 1) * 1000003 + self.seed
        spec = dict(spec)
        spec["generations"] = generations
        spec.setdefault("display_name", run_id)
        run = _Run(run_id, spec, subset, generations, population, seed, run_root,
                   test_name=record["test"], scope_ids=record["scope_ids"], warnings=warnings)
        run.parameters = dict((p["parameter_id"], p) for p in record["prepared"].get("parameters") or [])
        run.prepared_record = record
        with self.lock:
            self._runs[run_id] = run
        self._write_json(os.path.join(run_root, "spec.json"), spec)
        return run

    def _launch(self, run):
        run.state = "running"
        run.message = "Optimization started."
        run.bump()
        thread = threading.Thread(target=self._run_loop, args=(run,), name="demo-run-" + run.run_id)
        thread.daemon = True
        run.thread = thread
        thread.start()

    @staticmethod
    def _write_json(path, document):
        try:
            with open(path, "w") as stream:
                json.dump(document, stream, default=str)
        except (OSError, TypeError, ValueError):
            pass

    # genome / metric model -------------------------------------------------
    def _plan_generation(self, run, generation):
        """Return new genomes (lists of u in [0,1]) for one generation."""
        rng = random.Random(run.seed * 31 + generation)
        count = len(run.variables)
        total = max(1, run.generations - 1)
        progress = generation / float(total) if run.generations > 1 else 1.0
        sigma = 0.28 * (1.0 - progress) ** 2.1 + 0.012
        genomes = []
        strata = list(range(run.population))
        rng.shuffle(strata)
        for slot in range(run.population):
            if generation == 0:
                genome = [rng.random() for _ in range(count)]
            else:
                s = (strata[slot] + rng.random()) / float(run.population)
                genome = []
                for i in range(count):
                    target = run.center[i] + run.direction[i] * (s - 0.5)
                    genome.append(_clamp(target + rng.gauss(0.0, sigma), 0.0, 1.0))
            genomes.append(genome)
        return genomes

    def _latents(self, run, genome, rng):
        count = len(genome)
        if count == 0:
            return {"f1": 0.5, "f2": 0.5, "g": 1.0, "d": 0.0}
        dot = 0.0
        for i in range(count):
            dot += run.direction[i] * (genome[i] - run.center[i])
        s_hat = _clamp(0.5 + dot / run.direction_norm, 0.0, 1.0)
        total = 0.0
        for i in range(count):
            target = run.center[i] + run.direction[i] * (s_hat - 0.5)
            total += (genome[i] - target) ** 2
        residual = math.sqrt(total / count)
        d = _clamp(residual / 0.55, 0.0, 1.0)
        f1 = _clamp(s_hat + rng.gauss(0.0, 0.006), 0.0, 1.0)
        g = 1.0 + 4.0 * d ** 1.2
        f2 = g * (1.0 - math.sqrt(f1 / g))
        return {"f1": f1, "f2": f2, "g": g, "d": d}

    def _base_output(self, key, lat):
        f1, f2, d = lat["f1"], lat["f2"], lat["d"]
        if key == "gain_dB":
            return 18.0 + 66.0 * math.exp(-0.85 * f2)
        if key == "ugbw":
            return 4e6 * 10 ** (2.4 * f1) / (1.0 + 2.0 * d)
        if key == "pm":
            return 86.0 - 34.0 * f1 ** 1.3 - 18.0 * d
        if key == "power":
            return (90e-6 + 1.5e-3 * f1) * (1.0 + 1.2 * d)
        if key == "noise":
            return 2.2e-9 * (1.0 + 3.5 * (1.0 - f1) ** 1.5) * (1.0 + 0.8 * d)
        if key == "swing":
            return 1.45 - 0.55 * f1 - 0.4 * d
        row = self.output_by_key[key]
        f2s = f2 / (1.0 + f2)
        return row["scale"] * (1.0 + row["w1"] * f1 + row["w2"] * f2s + row["w3"] * d) \
            if row["unit"] not in ("dB", "deg") else row["scale"] * (1.0 + 0.3 * row["w1"] * f1 - 0.4 * row["w3"] * d)

    def _aggregate(self, key, definition, lat, rng):
        base = self._base_output(key, lat)
        spread = self.output_by_key[key]["spread"]
        names = definition.get("corners") or [row["name"] for row in self.corner_rows]
        values = []
        for name in names:
            skew = self.corner_skew.get(name, 0.0)
            values.append(base * (1.0 + spread * skew) * (1.0 + rng.gauss(0.0, 0.003)))
        mode = definition.get("aggregation") or "mean"
        if mode == "min":
            return min(values)
        if mode == "max":
            return max(values)
        if mode == "first":
            return values[0]
        if mode == "last":
            return values[-1]
        return sum(values) / len(values)

    def _evaluate(self, run, genome, rng):
        lat = self._latents(run, genome, rng)
        objectives, constraints = [], []
        for definition in run.spec.get("objectives") or []:
            value = self._aggregate(definition["output_key"], definition, lat, rng)
            objectives.append({
                "name": definition.get("name") or definition["output_key"],
                "output_key": definition["output_key"], "value": value,
                "score": objective_score(value, definition),
                "direction": definition.get("direction", "minimize"),
                "aggregation": definition.get("aggregation", "mean"),
                "corners": list(definition.get("corners") or [])})
        for definition in run.spec.get("constraints") or []:
            value = self._aggregate(definition["output_key"], definition, lat, rng)
            violation = constraint_violation(value, definition)
            constraints.append({
                "name": definition.get("name") or definition["output_key"],
                "output_key": definition["output_key"], "value": value, "violation": violation,
                "satisfied": violation <= 0.0, "aggregation": definition.get("aggregation", "mean"),
                "corners": list(definition.get("corners") or [])})
        total_violation = sum(row["violation"] for row in constraints)
        return {"objectives": objectives, "constraints": constraints, "feasible": total_violation <= 0.0,
                "total_violation": total_violation, "merit": sum(row["score"] for row in objectives)}

    def _values(self, run, genome):
        values = {}
        for variable, u in zip(run.variables, genome):
            low, high = float(variable["minimum"]), float(variable["maximum"])
            if variable.get("value_type") == "integer":
                values[variable["parameter_id"]] = int(round(low + u * (high - low)))
            elif variable.get("scale") == "log" and low > 0 and high > 0:
                values[variable["parameter_id"]] = math.exp(math.log(low) + u * (math.log(high) - math.log(low)))
            else:
                values[variable["parameter_id"]] = low + u * (high - low)
        return values

    def _new_candidate(self, run, generation, index, genome, status="queued"):
        candidate_id = "g%03d_c%04d" % (generation, index)
        row = {"candidate_id": candidate_id, "generation": generation, "index": index, "status": status,
               "values": self._values(run, genome), "metrics": {},
               "execution_backend": "private_netlist", "execution_simulator": "spectre"}
        run.genomes[candidate_id] = genome
        return row

    def _finish_candidate(self, run, row, rng):
        roll = rng.random()
        if roll < 0.03:
            row.update(status="failed", failure_stage="spectre", failure_class="spectre",
                       spectre_state="started", execution_started=True,
                       error="Spectre ERROR (SPECTRE-16771): no DC solution found after 2000 iterations "
                             "(convergence failure) in corner {}.".format(
                                 (run.spec["objectives"][0].get("corners") or ["tt_27c"])[0]))
            row["execution_progress"] = {"stage": "evaluating"}
            return "failed"
        if roll < 0.042:
            row.update(status="infrastructure_failed", failure_class="simulation_infrastructure",
                       failure_stage="simulation_infrastructure", spectre_state="unknown",
                       error="Scheduler job lost its execution node; replacement candidate sampled.")
            return "infrastructure_failed"
        row["metrics"] = self._evaluate(run, run.genomes[row["candidate_id"]], rng)
        row["status"] = "completed"
        row["spectre_state"] = "started"
        row.pop("execution_progress", None)
        return "completed"

    _STAGES = ((0.00, "preparing"), (0.12, "patching"), (0.22, "netlisting"), (0.34, "submitted"),
               (0.45, "running"), (0.88, "evaluating"))

    def _stage_for(self, fraction):
        stage = self._STAGES[0][1]
        for threshold, name in self._STAGES:
            if fraction >= threshold:
                stage = name
        return stage

    def _run_loop(self, run):
        try:
            self._run_generations(run)
        except Exception as error:  # surfaced through the GUI's normal failure path
            with self.lock:
                run.state = "failed"
                run.message = "{}: {}".format(error.__class__.__name__, error)
                run.bump()
            return
        with self.lock:
            done, failed, infra = run.counts()
            if run.stop_event.is_set() and run.next_generation < run.generations:
                run.state = "stopped"
                run.message = "Stopped by user after {} completed candidates.".format(done)
            else:
                run.state = "completed"
                run.message = "Optimization complete: {} candidates evaluated.".format(done + failed + infra)
            run.active = []
            run.bump()
        self._write_json(os.path.join(run.run_root, "status.json"), self._status(run))

    def _run_generations(self, run):
        rng = random.Random(run.seed + 99)
        for generation in range(run.next_generation, run.generations):
            if run.stop_event.is_set():
                return
            genomes = self._plan_generation(run, generation)
            pending = collections.deque()
            with self.lock:
                run.generation = generation
                run.message = "Generation {}/{} · evaluating {} candidates on {} jobs".format(
                    generation + 1, run.generations, run.population, self.jobs)
                for index, genome in enumerate(genomes):
                    row = self._new_candidate(run, generation, index, genome)
                    run.candidates[row["candidate_id"]] = row
                    run.bump(row["candidate_id"])
                    pending.append((row, rng.uniform(1.6, 3.4) / self.speed))
            extra = run.population
            active = []
            while pending or active:
                if run.stop_event.is_set():
                    self._stop_active(run, active, pending)
                    return
                now = time.time()
                with self.lock:
                    while pending and len(active) < self.jobs:
                        row, duration = pending.popleft()
                        row["status"] = "running"
                        row["execution_progress"] = {"stage": "preparing"}
                        active.append({"row": row, "start": now, "duration": duration})
                        run.bump(row["candidate_id"])
                    finished = []
                    for slot in active:
                        row = slot["row"]
                        fraction = (now - slot["start"]) / slot["duration"]
                        if fraction >= 1.0:
                            finished.append(slot)
                            continue
                        stage = self._stage_for(fraction)
                        if row["execution_progress"]["stage"] != stage:
                            row["execution_progress"] = {"stage": stage}
                            run.bump(row["candidate_id"])
                    for slot in finished:
                        active.remove(slot)
                        row = slot["row"]
                        outcome = self._finish_candidate(run, row, rng)
                        row["duration_seconds"] = round(slot["duration"] * self.speed, 2)
                        run.bump(row["candidate_id"])
                        if outcome == "infrastructure_failed":
                            replacement = self._new_candidate(
                                run, generation, extra,
                                [min(1.0, max(0.0, v + rng.gauss(0, 0.01))) for v in run.genomes[row["candidate_id"]]])
                            extra += 1
                            run.candidates[replacement["candidate_id"]] = replacement
                            run.bump(replacement["candidate_id"])
                            pending.append((replacement, rng.uniform(1.6, 3.4) / self.speed))
                    run.active = [dict(candidate_id=s["row"]["candidate_id"],
                                       stage=s["row"]["execution_progress"]["stage"]) for s in active]
                time.sleep(0.05)
            with self.lock:
                run.next_generation = generation + 1
                run.bump()

    def _stop_active(self, run, active, pending):
        with self.lock:
            for slot in active:
                slot["row"]["status"] = "stopped"
                slot["row"].pop("execution_progress", None)
                run.bump(slot["row"]["candidate_id"])
            for row, _duration in pending:
                run.candidates.pop(row["candidate_id"], None)
                run.revisions.pop(row["candidate_id"], None)
            run.bump()

    # status / progress -----------------------------------------------------
    def _status(self, run):
        done, failed, infra = run.counts()
        return {
            "run_id": run.run_id, "state": run.state, "message": run.message,
            "generation": run.generation, "total_generations": run.generations,
            "completed_candidates": done, "failed_candidates": failed,
            "infrastructure_failed_candidates": infra,
            "total_candidates": run.generations * run.population,
            "display_name": run.spec.get("display_name") or run.run_id, "run_root": run.run_root,
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(run.started_at)),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(run.updated_at)),
            "execution_backend": "private_netlist", "simulator": "spectre", "demo": True,
            "selected_test": run.test_name,
            "best_candidate_id": self._best(run),
        }

    def _best(self, run):
        best, merit = None, None
        for row in run.candidates.values():
            metrics = row.get("metrics") or {}
            if row.get("status") != "completed" or not metrics.get("feasible"):
                continue
            if merit is None or metrics["merit"] < merit:
                best, merit = row["candidate_id"], metrics["merit"]
        return best

    def _presentation(self, run):
        with self.lock:
            if run.presentation_revision == run.revision and run.presentation is not None:
                return run.presentation
            rows = list(run.candidates.values())
            revision = run.revision
        presentation = build_objective_presentation(rows, run.spec["objectives"])
        with self.lock:
            run.presentation, run.presentation_revision = presentation, revision
        return presentation

    def _execution(self, run):
        stage = {"queued": "queued", "running": "evaluating"}.get(run.state, run.state)
        workers = []
        for number, slot in enumerate(run.active):
            workers.append({"worker_index": number, "status": "running", "stage": slot["stage"],
                            "message": slot["candidate_id"]})
        commands = []
        for slot in run.active[:4]:
            if slot["stage"] in ("running", "submitted"):
                commands.append({"kind": "solver", "state": "running", "label": "Spectre " + slot["candidate_id"],
                                 "candidate_id": slot["candidate_id"],
                                 "invocation_id": "inv-" + slot["candidate_id"],
                                 "argv": ["spectre", "+aps", "-format", "psfxl", "input.scs"]})
        return {"stage": stage, "message": run.message, "run_root": run.run_root,
                "main_log_file": os.path.join(run.run_root, "run.log"), "workers": workers,
                "commands": commands, "warnings": list(run.warnings)}

    def _run_or_raise(self, run_name):
        with self.lock:
            run = self._runs.get(run_name)
        if run is None:
            run = self._materialize_history(run_name)
        if run is None:
            raise ValueError("run not found: %s" % run_name)
        return run

    def progress(self, run_name, since_revision=0, include_candidates=True):
        run = self._run_or_raise(run_name)
        with self.lock:
            status = self._status(run)
            revision = run.revision
            payload = {"status": status, "spec": run.spec, "revision": revision,
                       "dataset_path": os.path.join(run.run_root, "optimization.crescendo"),
                       "execution": self._execution(run)}
            if since_revision and not include_candidates and since_revision <= revision:
                payload["changed_candidates"] = [
                    row for row in run.candidates.values()
                    if run.revisions.get(row["candidate_id"], 0) > since_revision]
            else:
                payload["candidates"] = list(run.candidates.values())
                payload["changed_candidates"] = []
                payload["reset"] = bool(since_revision)
        payload["objective_presentation"] = self._presentation(run)
        return payload

    def progress_delta(self, run_name, since_revision=0):
        return self.progress(run_name, since_revision=since_revision, include_candidates=not bool(since_revision))

    def stop(self, run_name):
        with self.lock:
            run = self._runs.get(run_name)
            active = bool(run and run.thread and run.thread.is_alive())
        if not active:
            raise ValueError("run is not active in this optimizer service")
        run.stop_event.set()
        with self.lock:
            run.state = "stopping"
            run.message = "Stop requested; finishing running candidates."
            run.bump()
        return {"run_id": run_name, "stop_requested": True}

    def resume(self, checkpoint, path=None):
        self._pause(0.8)
        demo = (checkpoint or {}).get("demo") or {}
        spec = copy.deepcopy((checkpoint or {}).get("spec") or {})
        test = demo.get("test") or spec.get("selected_test") or self.test_rows[0]["name"]
        scope_ids = demo.get("scope_ids") or []
        hierarchy = self._hierarchy(test)
        scope_ids = [s for s in scope_ids if s in hierarchy.scope_index] or [hierarchy.scope_rows[0]["scope_id"]]
        locator = self._locator(path or ((checkpoint.get("manifest") or {}).get("setup_locator") or {}).get("path"))
        row = next(r for r in self.test_rows if r["name"] == test)
        preview = {"setup_path": locator["path"], "setup_locator": locator,
                   "setup": self._setup_block(locator, [row], selected=test),
                   "job_policy": self._job_policy(), "save_options": self._save_options(locator),
                   "setup_private_session": self._private_session(test, locator)}
        body, parameters = hierarchy.selection_payload(scope_ids)
        scopes = [hierarchy.scope_rows[hierarchy.scope_index[s]] for s in scope_ids]
        prepared = self._prepared_from(preview, hierarchy, scope_ids, scopes, body, parameters)
        prepared_id = self._next_id("prepared")
        record = {"prepared": prepared, "test": test, "scope_ids": scope_ids}
        with self.lock:
            self._prepared[prepared_id] = record
        original_id = (checkpoint.get("manifest") or {}).get("run_id")
        with self.lock:
            previous = self._runs.get(original_id)
        run = self._new_run(spec, record, spec.get("variables") or [], run_id=None)
        run.seed = previous.seed if previous else run.seed
        if previous is not None:
            run.center, run.direction, run.direction_norm = previous.center, previous.direction, previous.direction_norm
            run.variables = previous.variables
        for candidate in (checkpoint.get("candidates") or []):
            run.candidates[candidate["candidate_id"]] = copy.deepcopy(candidate)
            run.bump(candidate["candidate_id"])
        if previous is not None:
            run.genomes.update(previous.genomes)
        run.next_generation = int(((checkpoint.get("state") or {}).get("next_generation")) or 0)
        run.generation = run.next_generation
        self._launch(run)
        return {"prepared_id": prepared_id, "prepared": prepared, "run_id": run.run_id,
                "run_root": run.run_root}

    def export_checkpoint(self, run_name):
        run = self._run_or_raise(run_name)
        with self.lock:
            done, failed, infra = run.counts()
            return {
                "schema_version": "crescendo.checkpoint.v1", "spec": copy.deepcopy(run.spec),
                "manifest": {"run_id": run.run_id, "display_name": run.spec.get("display_name"),
                             "setup_locator": copy.deepcopy(run.prepared_record["prepared"].get("setup_locator"))
                             if hasattr(run, "prepared_record") else {}},
                "state": {"next_generation": run.next_generation, "completed_candidates": done,
                          "failed_candidates": failed, "infrastructure_failed_candidates": infra},
                "candidates": [copy.deepcopy(r) for r in run.candidates.values()
                               if r.get("status") in ("completed", "failed", "infrastructure_failed")],
                "demo": {"test": run.test_name, "scope_ids": list(run.scope_ids)},
            }

    def list_runs(self):
        self._pause(0.15)
        rows = []
        with self.lock:
            runs = list(self._runs.values())
            history = list(self._history)
        for run in runs:
            with self.lock:
                status = self._status(run)
                done, failed, infra = run.counts()
            row = dict(status)
            row.update(checkpoint_available=run.next_generation > 0 or done > 0, checkpoint_revision=run.revision,
                       checkpoint_source="demo", continuation_id=run.run_id,
                       current_generation=min(run.generations, run.generation + (1 if run.state == "completed" else 0)),
                       total_generations=run.generations, successful_candidates=done,
                       simulation_failed_candidates=failed, infrastructure_failed_candidates=infra,
                       success=done, simulation_fail=failed, infrastructure_fail=infra,
                       active=bool(run.thread and run.thread.is_alive()))
            rows.append(row)
        rows.reverse()
        for item in history:
            rows.append(dict(item["row"]))
        return rows

    # history (pre-existing runs) ---------------------------------------------
    def _register_history(self, number):
        names = ("Baseline sweep (gain vs power)", "Wide-range retry")
        run_id = "run_2026110{}_1{}2200_demo".format(5 + number, number)
        started = "2026-11-0{}T1{}:22:00".format(5 + number, number)
        generations = (8, 10)[number % 2]
        row = {"run_id": run_id, "state": "completed", "display_name": names[number % 2],
               "started_at": started, "current_generation": generations, "total_generations": generations,
               "successful_candidates": generations * 15, "simulation_failed_candidates": generations // 4,
               "infrastructure_failed_candidates": 0, "success": generations * 15,
               "simulation_fail": generations // 4, "infrastructure_fail": 0,
               "checkpoint_available": True, "continuation_id": run_id, "active": False}
        self._history.append({"row": row, "generations": generations, "number": number})

    def _materialize_history(self, run_name):
        entry = next((item for item in self._history if item["row"]["run_id"] == run_name), None)
        if entry is None:
            return None
        test = self.test_rows[0]["name"]
        hierarchy = self._hierarchy(test)
        body, parameters = hierarchy.selection_payload([hierarchy.scope_rows[0]["scope_id"]])
        chosen = [p for p in parameters if p["name"] in ("w", "l")][:24]
        variables = [{"parameter_id": p["parameter_id"], "minimum": p["numeric_value"] * 0.5,
                      "maximum": p["numeric_value"] * 2.5, "scale": "linear", "value_type": "float"}
                     for p in chosen]
        corner_names = [c["name"] for c in self.corner_rows[:min(3, len(self.corner_rows))]]
        spec = {"evaluator": "real", "variables": variables, "display_name": entry["row"]["display_name"],
                "objectives": [{"output_key": "gain_dB", "aggregation": "min", "corners": corner_names,
                                "direction": "maximize"},
                               {"output_key": "power", "aggregation": "max", "corners": corner_names,
                                "direction": "minimize"}],
                "constraints": [{"output_key": "pm", "aggregation": "min", "corners": corner_names,
                                 "operator": ">=", "limit": "55"}],
                "generations": entry["generations"], "algorithm": "nsga2", "selected_test": test}
        run_root = os.path.join(self.runs_root, run_name)
        os.makedirs(run_root, exist_ok=True)
        run = _Run(run_name, spec, variables, entry["generations"], 15, self.seed + 500 + entry["number"],
                   run_root, test_name=test, scope_ids=[hierarchy.scope_rows[0]["scope_id"]])
        run.parameters = dict((p["parameter_id"], p) for p in parameters)
        run.prepared_record = {"prepared": {"setup_locator": self._locator(self.workdir)}}
        rng = random.Random(run.seed + 99)
        for generation in range(run.generations):
            for index, genome in enumerate(self._plan_generation(run, generation)):
                row = self._new_candidate(run, generation, index, genome)
                self._finish_candidate(run, row, rng)
                run.candidates[row["candidate_id"]] = row
                run.bump(row["candidate_id"])
        run.state, run.next_generation, run.generation = "completed", run.generations, run.generations - 1
        run.message = "Optimization complete."
        run.started_at = time.mktime(time.strptime(entry["row"]["started_at"], "%Y-%m-%dT%H:%M:%S"))
        with self.lock:
            self._runs[run_name] = run
            self._runs.move_to_end(run_name, last=False)
        return run

    # --------------------------------------------------------- results export
    def export_results(self, run_name, path):
        from crescendo_engine.run_data_store import RunDataStore
        run = self._run_or_raise(run_name)
        self._pause(0.3)
        target = os.path.abspath(path)
        if os.path.lexists(target):
            os.unlink(target)
        with self.lock:
            rows = [copy.deepcopy(r) for r in run.candidates.values()
                    if r.get("status") not in ("queued",)]
            parameters = [dict(run.parameters[v["parameter_id"]]) for v in run.variables
                          if v["parameter_id"] in run.parameters]
            metadata = {"spec": copy.deepcopy(run.spec), "parameters": parameters,
                        "display_name": run.spec.get("display_name") or run.run_id,
                        "run_id": run.run_id}
        store = RunDataStore(target, metadata=metadata)
        try:
            store.commit_candidates(rows)
        finally:
            store.close()
        return {"path": target, "run_id": run_name}

    def candidates_csv(self, run_name):
        from crescendo_engine.exports import candidates_csv_text
        from crescendo_engine.gui_state import parameter_label
        run = self._run_or_raise(run_name)
        labels = dict((pid, parameter_label(row)) for pid, row in run.parameters.items()
                      if any(v["parameter_id"] == pid for v in run.variables))
        with self.lock:
            rows = list(run.candidates.values())
        return candidates_csv_text(rows, variable_labels=labels)

    def generate_optimized_schematic(self, run_name, candidate_id):
        run = self._run_or_raise(run_name)
        row = run.candidates.get(candidate_id)
        if row is None or row.get("status") != "completed":
            raise ValueError("Only a completed candidate can generate an optimized schematic.")
        self._pause(1.8)
        view = "schematic_opt_{}".format(candidate_id)
        return {"views": [{"library": LIB, "cell": "tb_" + (run.test_name or "top").lower(), "view": view},
                          {"library": LIB, "cell": "ota_core", "view": view}],
                "message": "Optimized schematic views created for " + candidate_id}

    # ----------------------------------------------------------- candidate logs
    def candidate_logs(self, run_name, candidate_id):
        run = self._run_or_raise(run_name)
        row = run.candidates.get(candidate_id)
        if row is None:
            raise ValueError("candidate not found: " + candidate_id)
        names = (run.spec["objectives"][0].get("corners") or [self.corner_rows[0]["name"]])[:3]
        text_size = len(self._log_text(run, row, names[0]))
        logs = []
        for corner in names:
            logs.append({"log_id": "spectre:" + corner, "kind": "spectre", "label": "Spectre · " + corner,
                         "corner": corner, "test": run.test_name, "point_id": "1",
                         "file_name": "spectre.out", "size_bytes": self._visible(row, text_size),
                         "location": os.path.join(run.run_root, "candidates", candidate_id, corner, "spectre.out"),
                         "file_token": candidate_id + corner})
        status = row.get("status")
        state = "complete" if status in ("completed", "failed") else "running"
        if status in ("queued", "planned"):
            state = "not_started"
        return {"candidate_status": status, "spectre_state": row.get("spectre_state") or
                ("started" if status == "running" else "unknown"),
                "execution_simulator": "spectre", "state": state, "logs": logs if status != "queued" else [],
                "stage": (row.get("execution_progress") or {}).get("stage"),
                "message": "Demo transcript generated from synthetic evaluation.",
                "task_count": len(names), "tasks": [
                    {"test": run.test_name, "corner": c, "point_id": 1, "state": state, "job_id": 4200 + i,
                     "solver_command": "spectre +aps -format psfxl input.scs"} for i, c in enumerate(names)],
                "warnings": []}

    @staticmethod
    def _visible(row, total):
        status = row.get("status")
        if status in ("completed", "failed", "infrastructure_failed", "stopped"):
            return total
        stage = (row.get("execution_progress") or {}).get("stage")
        fractions = {"preparing": 0.0, "patching": 0.0, "netlisting": 0.0, "submitted": 0.05,
                     "running": 0.55, "evaluating": 0.9}
        return int(total * fractions.get(stage, 0.0))

    def _log_text(self, run, row, corner):
        rng = random.Random(_digest(row["candidate_id"], corner))
        lines = ["Spectre (R) Circuit Simulator", "Version 21.1 64bit -- 8 Oct 2025",
                 "Simulating {}  corner {}  Test {}".format(row["candidate_id"], corner, run.test_name), ""]
        for name in ("dc", "ac", "noise", "tran"):
            lines.append("Running {} analysis `{}' ...".format(name, name))
            for number in range(rng.randint(12, 26)):
                lines.append("    {:<4s} iter {:3d}: residual {:.3e}  max dV {:.3e}".format(
                    "NR", number + 1, 10 ** rng.uniform(-12, -2) / (number + 1), 10 ** rng.uniform(-9, -3)))
            lines.append("    {} completed in {:.2f} s".format(name, rng.uniform(0.3, 4.0)))
            lines.append("")
        if row.get("status") == "failed":
            lines.append(row.get("error") or "ERROR")
        else:
            lines.append("Total time: {:.2f} s  Peak memory: {} MB".format(rng.uniform(4, 30), rng.randint(180, 900)))
            lines.append("spectre completes with 0 errors, 2 warnings, and 3 notices.")
        return "\n".join(lines) + "\n"

    def read_candidate_log_chunk(self, run_name, candidate_id, log_id, offset=0, max_bytes=262144,
                                 file_token=None):
        run = self._run_or_raise(run_name)
        row = run.candidates.get(candidate_id)
        if row is None:
            raise ValueError("candidate not found: " + candidate_id)
        corner = log_id.split(":", 1)[-1]
        text = self._log_text(run, row, corner)
        visible = text[:self._visible(row, len(text))]
        offset = max(0, min(int(offset or 0), len(visible)))
        chunk = visible[offset:offset + int(max_bytes)]
        return {"text": chunk, "next_offset": offset + len(chunk), "file_token": candidate_id + corner,
                "waiting": not chunk and row.get("status") in ("running",)}

    # --------------------------------------------------------------- lifecycle
    def exit_optimizer(self):
        self._closing.set()
        with self.lock:
            runs = list(self._runs.values())
        for run in runs:
            run.stop_event.set()
        for run in runs:
            if run.thread is not None and run.thread.is_alive():
                run.thread.join(2.0)
        return {"closed": True, "validation_session_closed": True, "cleanup_confirmed": True, "warnings": []}

    def request_force_exit(self):
        self._closing.set()
        return {"requested": True}

    def force_exit_optimizer(self, reason="User forced UI exit"):
        self._closing.set()
        with self.lock:
            for run in self._runs.values():
                run.stop_event.set()
        return {"cleanup_confirmed": False, "forced": True, "reason": reason}

    def cleanup_workdir(self):
        if self._owns_workdir:
            shutil.rmtree(self.workdir, ignore_errors=True)
