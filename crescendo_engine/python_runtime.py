"""Find an existing Python/Tk runtime without installing or changing the host.

The shell selects a Python 3.6+ stdlib bootstrap before invoking this module.
All discovery and probes retain the caller's native environment snapshot.
"""
from __future__ import print_function

import argparse
import json
import os
import re
import signal
import stat
import subprocess
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crescendo_engine.native_environment import (
    GUI_ENV_NAMES, NATIVE_ENV_SNAPSHOT_VAR, parse_native_environment_snapshot,
)
from crescendo_engine import gui_runtime_dependencies as dependencies


PROBE_SCHEMA = "crescendo.python_runtime_probe.v1"
_PYTHON_NAME = re.compile(r"^python(?:3(?:\.\d+)?|)$")
_PROBE = r'''
import importlib.util, json, os, sys, sysconfig
r = {"schema": "crescendo.python_runtime_probe.v1", "version": list(sys.version_info[:3]),
     "prefix": sys.prefix, "executable": sys.executable, "state": "rejected",
     "stage": "version", "tk_init": "NOT TESTED", "display_unavailable": False}
r["metadata"] = {"prefix": sys.prefix, "base_prefix": getattr(sys, "base_prefix", sys.prefix),
                 "exec_prefix": sys.exec_prefix,
                 "libdirs": [sysconfig.get_config_var(k) for k in ("LIBDIR", "LIBPL")
                             if sysconfig.get_config_var(k)]}
try:
    if sys.version_info < (3, 6):
        raise RuntimeError("Python 3.6 or newer is required")
    r["stage"] = "tk_import"
    spec = importlib.util.find_spec("_tkinter")
    r["metadata"]["tkinter_file"] = spec.origin if spec else None
    import tkinter as tk
    from tkinter import ttk
    r["metadata"].update(tcl_version=str(tk.TclVersion), tk_version=str(tk.TkVersion))
    if tk.TkVersion < 8.5 or not all(hasattr(ttk, name) for name in ("Treeview", "Combobox")):
        raise RuntimeError("Tk 8.5 or newer with ttk is required")
    r["stage"] = "tcl_init"
    tcl = tk.Tcl()
    r["tcl_version"] = str(tcl.call("info", "patchlevel"))
    if r["tcl_version"].split(".")[:2] != str(tk.TclVersion).split(".")[:2]:
        raise RuntimeError("Tcl runtime ABI does not match Tkinter")
    if tuple(int(v) for v in r["tcl_version"].split(".")[:2]) < (8, 5):
        raise RuntimeError("Tcl 8.5 or newer is required")
    r["state"] = "eligible"
    if not os.environ.get("DISPLAY"):
        r["display_unavailable"] = True
    else:
        r["stage"] = "tk_init"
        root = None
        try:
            root = tk.Tk()
            root.withdraw()
            root.update_idletasks()
            r["tk_version"] = str(root.tk.call("package", "require", "Tk"))
            if r["tk_version"].split(".")[:2] != str(tk.TkVersion).split(".")[:2]:
                raise RuntimeError("Tk runtime ABI does not match Tkinter")
            if tuple(int(v) for v in r["tk_version"].split(".")[:2]) < (8, 5):
                raise RuntimeError("Actual Tk runtime is older than 8.5")
            for widget_type in (ttk.Treeview, ttk.Combobox):
                widget = widget_type(root)
                widget.destroy()
            r["tk_init"] = "PASS"
        except tk.TclError as e:
            message = str(e).lower()
            display_errors = ("no display name", "couldn't connect to display", "cannot connect to display",
                              "could not connect to display", "authorization required", "invalid display")
            if any(part in message for part in display_errors):
                r["display_unavailable"] = True
            else:
                raise
        finally:
            if root is not None:
                root.destroy()
    r["stage"] = "ready"
except Exception as e:
    r["state"] = "rejected"
    r["error_type"] = type(e).__name__
    r["error"] = str(e)[:8192]
print(json.dumps(r, sort_keys=True))
'''


def _invocation_path(value, caller_dir):
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        return None
    # Do not realpath Python: a venv invocation symlink is part of its identity.
    return os.path.abspath(os.path.join(caller_dir, os.path.expanduser(value)))


def discover_candidates(preferred=None, caller_dir=None, module_root=None,
                        environ=None, max_candidates=64):
    environment = dict(os.environ if environ is None else environ)
    caller_dir = os.path.abspath(caller_dir or os.getcwd())
    module_root = os.path.abspath(module_root or os.path.dirname(os.path.dirname(__file__)))
    if type(max_candidates) is not int or max_candidates < 1:
        raise ValueError("max_candidates must be a positive integer")
    candidates, seen = [], set()
    limited = False
    reserved_fallbacks = min(8, max_candidates // 4)

    def add(value, source):
        nonlocal limited
        path = _invocation_path(value, caller_dir)
        if path is None or path in seen:
            return
        if not os.path.isfile(path) or not os.access(path, os.X_OK):
            return
        ceiling = max_candidates if source == "system_installation" else max_candidates - reserved_fallbacks
        if len(candidates) >= ceiling:
            limited = True
            return
        candidates.append({"path": path, "source": source})
        seen.add(path)

    def binaries(directory, source):
        directory = _invocation_path(directory, caller_dir)
        if not directory:
            return
        for name in ("python3", "python"):
            add(os.path.join(directory, name), source)
        try:
            names = sorted(n for n in os.listdir(directory) if _PYTHON_NAME.match(n)
                           and n not in ("python3", "python"))
        except OSError:
            return
        for name in reversed(names[:32]):
            add(os.path.join(directory, name), source)

    if preferred:
        if os.sep not in preferred:
            for directory in environment.get("PATH", "").split(os.pathsep):
                add(os.path.join(directory or caller_dir, preferred), "installed_preference")
        else:
            add(preferred, "installed_preference")
    for directory in (caller_dir, module_root):
        for name in (".venv", "venv"):
            binaries(os.path.join(directory, name, "bin"), "local_environment")
    for directory in environment.get("PATH", "").split(os.pathsep):
        binaries(directory or caller_dir, "PATH")
    user_home = environment.get("HOME")
    if user_home:
        for suffix in (".local/bin", "miniforge3/bin", "miniconda3/bin", "anaconda3/bin",
                       ".venv/bin", "venv/bin"):
            binaries(os.path.join(user_home, suffix), "user_installation")
    # Keep bounded system fallbacks ahead of a large user environment catalog.
    for directory in ("/usr/bin", "/usr/local/bin", "/bin", "/opt/miniforge3/bin",
                      "/opt/miniconda3/bin", "/opt/conda/bin"):
        binaries(directory, "system_installation")
    if user_home:
        for suffix in (".venvs", ".virtualenvs"):
            parent = os.path.join(user_home, suffix)
            try:
                names = sorted(os.listdir(parent))[:16]
            except OSError:
                continue
            for name in names:
                binaries(os.path.join(parent, name, "bin"), "user_environment")
    conda_roots = ["/opt/miniforge3", "/opt/miniconda3", "/opt/conda"]
    if user_home:
        conda_roots += [os.path.join(user_home, name) for name in ("miniforge3", "miniconda3", "anaconda3")]
    for conda_root in conda_roots:
        parent = os.path.join(conda_root, "envs")
        try:
            names = sorted(os.listdir(parent))[:16]
        except OSError:
            continue
        for name in names:
            binaries(os.path.join(parent, name, "bin"), "user_environment")
    for prefix in ("/opt", "/usr/local"):
        try:
            names = sorted(n for n in os.listdir(prefix) if re.match(r"^python3?(?:\d|[.-])", n))[:16]
        except OSError:
            continue
        for name in names:
            binaries(os.path.join(prefix, name, "bin"), "system_installation")
    return {"candidates": candidates, "limited": limited,
            "limits": {"max_candidates": max_candidates, "user_environments_per_root": 16}}


def _stop_probe(process):
    # Retire the entire owned group even when its parent exits before a child
    # which ignores TERM and closes the inherited pipes.
    drained = False
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except OSError:
            pass
        if not drained:
            try:
                process.communicate(timeout=0.25)
                drained = True
            except subprocess.TimeoutExpired:
                pass
    # Never wait indefinitely for a wrapper which detached another session.
    for stream in (process.stdout, process.stderr):
        if stream is not None:
            stream.close()
    try:
        process.wait(timeout=0.25)
    except subprocess.TimeoutExpired:
        pass


def _probe_once(path, environment, timeout):
    result = {"path": path, "state": "rejected", "stage": "python_exec",
              "tk_init": "NOT TESTED", "display_unavailable": False}
    if not os.path.isfile(path) or not os.access(path, os.X_OK):
        result["error_type"] = "UnavailableExecutable"
        return result
    try:
        process = subprocess.Popen([path, "-I", "-B", "-c", _PROBE], env=environment,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
    except OSError as error:
        result["error_type"] = type(error).__name__
        return result
    try:
        output, unused_error = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _stop_probe(process)
        result["error_type"] = "TimeoutExpired"
        result["timed_out"] = True
        return result
    if process.returncode:
        result["error_type"] = "InterpreterExited"
        result["returncode"] = process.returncode
        result["error"] = unused_error.decode("utf-8", "replace")[-8192:]
        return result
    try:
        payload = json.loads(output.decode("utf-8"))
        if (payload.get("schema") != PROBE_SCHEMA or
                payload.get("state") not in ("eligible", "rejected") or
                not isinstance(payload.get("version"), list) or len(payload["version"]) != 3 or
                any(type(item) is not int for item in payload["version"])):
            raise ValueError("Invalid interpreter probe payload")
    except (UnicodeError, ValueError, AttributeError, TypeError):
        result["error_type"] = "InvalidProbeResponse"
        return result
    result.update(payload)
    result["path"] = path
    return result


def probe_candidate(path, environ=None, timeout=3):
    """Probe clean first, repairing only proven missing GUI dependencies."""
    baseline = dict(os.environ if environ is None else environ)
    if timeout <= 0:
        return {"path": path, "state": "rejected", "stage": "python_exec",
                "tk_init": "NOT TESTED", "error_type": "TimeoutExpired", "timed_out": True}
    current = dict(baseline)
    for name in GUI_ENV_NAMES:
        current.pop(name, None)
    # Native values are discovery clues only, never a loader value to restore.
    if NATIVE_ENV_SNAPSHOT_VAR in baseline:
        hint = parse_native_environment_snapshot(baseline[NATIVE_ENV_SNAPSHOT_VAR]).get("LD_LIBRARY_PATH")
    else:
        hint = baseline.get("CRESCENDO_LAUNCH_VALUE_LD_LIBRARY_PATH", baseline.get("LD_LIBRARY_PATH"))
    clues = {"CRESCENDO_DEPENDENCY_LOADER_HINT": hint or ""}
    diagnostics, metadata, added, extra = [], {}, [], []
    tried_data = {"tcl": set(), "tk": set()}
    deadline = time.monotonic() + timeout
    for attempt in range(12):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            result.update(state="rejected", error_type="TimeoutExpired", timed_out=True)
            break
        result = _probe_once(path, current, remaining)
        metadata.update(result.get("metadata", {}))
        if result["state"] == "eligible":
            break
        error = result.get("error", "")
        missing, kind = dependencies._MISSING.search(error), dependencies._missing_data(error)
        if not missing and not kind:
            break
        directories = dependencies._candidate_directories(path, metadata, clues, diagnostics)
        for directory in extra:
            dependencies._add_directory(directories, directory)
        if missing:
            name = missing.group("name")
            selected = dependencies._select_library(name, directories,
                dependencies._reference_elf(path, metadata), metadata, diagnostics)
            if not selected:
                break
            filename, info = selected
            # The loader looks up the requested SONAME in its symlink's
            # directory, even when the fully versioned target lives elsewhere.
            directory = os.path.realpath(os.path.dirname(filename))
            if directory in added:
                break
            added.append(directory)
            current["LD_LIBRARY_PATH"] = os.pathsep.join(added)
            for directory in dependencies._rpath_directories(filename, info):
                dependencies._add_directory(extra, directory)
            if re.match(r"^lib(?:tcl|tk)", name, re.I):
                parent = directory
                for suffix in ("", "..", "../lib", "../lib64", "../share", "../share/tcltk"):
                    dependencies._add_directory(extra, os.path.join(parent, suffix))
                dependencies._add_directory(extra, os.path.dirname(os.path.realpath(filename)))
        else:
            selected = dependencies._data_candidates(kind, directories, metadata, tried_data[kind], diagnostics)
            if not selected:
                break
            tried_data[kind].add(selected)
            current["TCL_LIBRARY" if kind == "tcl" else "TK_LIBRARY"] = selected
    else:
        result.update(state="rejected", error_type="DependencyLimitReached")
    result["gui_overlays"] = {key: current[key] for key in dependencies.OVERLAY_KEYS if key in current}
    # Log actual changes/rejections; do not publish inherited loader directories.
    result["dependency_diagnostics"] = [line for line in diagnostics
        if "bounded candidate directories=" not in line and "ignored unrelated Qt loader clue" not in line]
    return result


def write_runtime_result(path, interpreter, overlays):
    parent = os.stat(os.path.dirname(os.path.abspath(path)))
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise ValueError("Runtime result directory must be owned and private")
    records = [("CRESCENDO_RUNTIME_SCHEMA", "crescendo.gui_dependencies.v1"), ("CRESCENDO_RUNTIME_INTERPRETER", interpreter)]
    records.extend((key, overlays[key]) for key in dependencies.OVERLAY_KEYS if key in overlays)
    if any(not isinstance(value, str) or "\0" in value for key, value in records):
        raise ValueError("Invalid GUI runtime result")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(b"".join((key + "=" + value).encode("utf-8") + b"\0" for key, value in records))


def read_runtime_overlays(path):
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("GUI runtime result must be an owned private regular file")
        raw = stream.read(65537)
    if len(raw) > 65536 or not raw.endswith(b"\0"):
        raise ValueError("Invalid GUI runtime result size or encoding")
    records = {}
    for item in raw[:-1].decode("utf-8").split("\0"):
        key, value = item.split("=", 1)
        if key in records:
            raise ValueError("Duplicate GUI runtime result key")
        records[key] = value
    if records.pop("CRESCENDO_RUNTIME_SCHEMA", None) != "crescendo.gui_dependencies.v1":
        raise ValueError("Invalid GUI runtime schema")
    interpreter = records.pop("CRESCENDO_RUNTIME_INTERPRETER", None)
    if not interpreter or os.path.abspath(interpreter) != os.path.abspath(sys.executable):
        raise ValueError("GUI runtime result belongs to a different interpreter")
    if set(records) - set(dependencies.OVERLAY_KEYS):
        raise ValueError("Unsupported GUI runtime overlay")
    for key, value in records.items():
        paths = value.split(os.pathsep) if key == "LD_LIBRARY_PATH" else [value]
        if not value or any(not os.path.isabs(p) or not os.path.isdir(p) for p in paths):
            raise ValueError("GUI runtime overlay must contain existing absolute directories")
    return records


def select_runtime(preferred=None, caller_dir=None, module_root=None, environ=None,
                   max_candidates=64, total_timeout=20, probe_timeout=3):
    discovery = discover_candidates(preferred, caller_dir, module_root, environ, max_candidates)
    report = {"schema": "crescendo.python_runtime_selection.v1", "selected": None,
              "probes": [], "discovery": discovery, "deadline_reached": False,
              "display_unavailable": False}
    deadline = time.monotonic() + total_timeout
    for candidate in discovery["candidates"]:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            report["deadline_reached"] = True
            break
        probe = probe_candidate(candidate["path"], environ, min(probe_timeout, remaining))
        probe["source"] = candidate["source"]
        report["probes"].append(probe)
        if probe["state"] == "eligible":
            report["selected"] = candidate["path"]
            report["display_unavailable"] = probe.get("display_unavailable", False)
            break
    return report


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--caller-dir", default=os.getcwd())
    parser.add_argument("--module-root")
    parser.add_argument("--python", dest="explicit")
    parser.add_argument("--result-file")
    args = parser.parse_args(arguments)
    from crescendo_engine.launcher import _write_log, bootstrap_environment
    try:
        bootstrap_environment()
    except ValueError as error:
        _write_log("Crescendo launch failed at environment: " + str(error))
        print("Crescendo launch failed at environment: " + str(error), file=sys.stderr)
        return 2
    if args.explicit:
        probe = probe_candidate(args.explicit, timeout=8)
        report = {"selected": args.explicit if probe["state"] == "eligible" else None, "probes": [probe]}
    else:
        report = select_runtime(os.environ.get("CRESCENDO_PREFERRED_PYTHON"), args.caller_dir, args.module_root)
    _write_log("Crescendo automatic Python selection: " + json.dumps(report, sort_keys=True))
    if not report["selected"]:
        print("Crescendo could not find an existing Python 3.6+ with working Tk/Tcl; see the private launch log.", file=sys.stderr)
        return 2
    print("Crescendo automatically selected Python: " + report["selected"], file=sys.stderr)
    if args.result_file:
        write_runtime_result(args.result_file, report["probes"][-1].get("executable", report["selected"]),
                             report["probes"][-1].get("gui_overlays", {}))
    sys.stdout.write(report["selected"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
