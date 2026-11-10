"""Shared source/wheel GUI bootstrap with private staged diagnostics.

Invoke through cadence/crescendo_launch so loader/Python overrides are removed before
this interpreter starts. The caller cwd and explicit interpreter are retained.
"""
from __future__ import print_function

import json
import os
import stat
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crescendo_engine.scratch import (register_scratch, cleanup_scratch, seal_scratch,
                                       reap_sealed_scratch, enforce_scratch_budget)

from crescendo_engine.native_environment import (
    NATIVE_ENV_NAMES, NATIVE_ENV_SCHEMA, NATIVE_ENV_SNAPSHOT_VAR,
    clean_gui_environment, parse_native_environment_snapshot,
    validate_native_values,
)


def bootstrap_environment(environ=None):
    """Consume the shell bridge once, never replacing an earlier native origin."""
    original = dict(os.environ if environ is None else environ)
    if NATIVE_ENV_SNAPSHOT_VAR in original:
        values = parse_native_environment_snapshot(original[NATIVE_ENV_SNAPSHOT_VAR])
    else:
        values = {}
        for name in NATIVE_ENV_NAMES:
            state = original.get("CRESCENDO_LAUNCH_STATE_" + name)
            value_key = "CRESCENDO_LAUNCH_VALUE_" + name
            if state == "unset" and value_key not in original:
                values[name] = None
            elif state == "set" and value_key in original:
                values[name] = original[value_key]
            else:
                raise ValueError("Launcher native environment bridge is incomplete or invalid")
        values = validate_native_values(values, require_complete=True)
    result = clean_gui_environment(original)
    for name in list(result):
        if name.startswith(("CRESCENDO_LAUNCH_STATE_", "CRESCENDO_LAUNCH_VALUE_")):
            result.pop(name, None)
    result[NATIVE_ENV_SNAPSHOT_VAR] = json.dumps(
        {"schema": NATIVE_ENV_SCHEMA, "values": values}, separators=(",", ":"),
    )
    return result


def _write_log(message):
    path = os.environ.get("CRESCENDO_LAUNCH_LOG")
    if not path:
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Launch log must be an owned private regular file")
        # Bootstrap diagnostics are small garbage, never a simulation log.
        if info.st_size > 256 * 1024:
            os.ftruncate(descriptor, 0)
        os.write(descriptor, (message + "\n").encode("utf-8", "replace"))
    finally:
        os.close(descriptor)


def record_application_initialized():
    _write_log("Crescendo launch stage: application_init PASS")


def gui_arguments(arguments, environment):
    if not arguments:
        raise ValueError("Missing launcher mode or Maestro context")
    prefix = ["--config", environment["CRESCENDO_CONFIG"]] if environment.get("CRESCENDO_CONFIG") else []
    if arguments[0] == "--standalone":
        return prefix + list(arguments[1:]) + ["gui"]
    if len(arguments) != 1 or arguments[0].startswith("--"):
        raise ValueError("Expected one Maestro context, --standalone, or --check")
    return prefix + ["gui", "--maestro-context-file", os.path.abspath(arguments[0])]


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    report = {"schema": "crescendo.launch_preflight.v1", "stages": {},
              "python": {"executable": sys.executable,
                         "version": list(sys.version_info[:3]),
                         "prefix": sys.prefix}}
    stage = "environment"
    scratch = None
    successful = False
    try:
        log_path = os.environ.get("CRESCENDO_LAUNCH_LOG")
        if log_path and os.path.basename(os.path.dirname(log_path)).startswith("crescendo-launch."):
            # Only the shell-created private temporary directory is disposable.
            # An arbitrary user-selected diagnostic file is never registered.
            scratch_parent = os.path.dirname(os.path.dirname(log_path))
            reap_sealed_scratch(scratch_parent)
            try:
                enforce_scratch_budget(scratch_parent)
            except ValueError as error:
                # Old unrelated scratch must not block a new optimization.
                # This launcher adds only bounded diagnostics; large artifacts
                # never use this root. Show the precise maintenance warning.
                _write_log("Crescendo temporary cache warning: %s" % error)
            scratch = register_scratch(os.path.dirname(log_path), kind="bootstrap")
        environment = bootstrap_environment()
        runtime_file = environment.pop("CRESCENDO_GUI_RUNTIME_FILE", None)
        if runtime_file:
            from crescendo_engine.python_runtime import read_runtime_overlays
            overlays = read_runtime_overlays(runtime_file)
            environment.update(overlays)
            report["gui_dependencies"] = overlays
        os.environ.clear()
        os.environ.update(environment)
        _write_log("Crescendo launch stage: environment PASS")
        report["stages"]["python_exec"] = "PASS"
        stage = "version"
        if sys.version_info < (3, 6):
            raise RuntimeError("Crescendo requires Python 3.6 or newer")
        report["stages"][stage] = "PASS"
        _write_log("Crescendo launch stage: version PASS")
        stage = "tk_import"
        import tkinter as tk
        from tkinter import ttk
        report["stages"][stage] = "PASS"
        _write_log("Crescendo launch stage: tk_import PASS")
        stage = "tcl_init"
        tcl = tk.Tcl()
        report["tcl_version"] = tcl.call("info", "patchlevel")
        report["stages"][stage] = "PASS"
        _write_log("Crescendo launch stage: tcl_init PASS")
        stage = "tk_init"
        root = tk.Tk()
        try:
            root.withdraw()
            root.update_idletasks()
            report["tk_version"] = root.tk.call("package", "require", "Tk")
        finally:
            root.destroy()
        report["stages"][stage] = "PASS"
        _write_log("Crescendo launch stage: tk_init PASS")
        if arguments == ["--check"]:
            report["stages"]["application_init"] = "NOT TESTED"
            print(json.dumps(report, indent=2, sort_keys=True))
            successful = True
            return 0
        stage = "application_init"
        _write_log("Crescendo launch stage: application_init START")
        try:
            # The released package opens the Crescendo window; a bare engine
            # tree (no crescendo package beside it) keeps the classic window.
            from crescendo import app as crescendo_app
        except ImportError:
            crescendo_app = None
        if crescendo_app is not None:
            crescendo_app.install()
        from crescendo_engine.__main__ import main as application_main
        status = application_main(gui_arguments(arguments, environment))
        _write_log("Crescendo application exit: %s" % (status or 0))
        successful = not status
        return status or 0
    except Exception as error:
        report["stages"][stage] = "FAIL"
        # Snapshot values and license contents are never formatted into errors.
        report["failure"] = {"stage": stage, "type": type(error).__name__, "message": str(error)}
        _write_log("Crescendo launch failed at %s: %s: %s" % (stage, type(error).__name__, error))
        print("Crescendo launch failed at %s: %s: %s" % (stage, type(error).__name__, error), file=sys.stderr)
        if arguments == ["--check"]:
            print(json.dumps(report, indent=2, sort_keys=True))
        return 1
    finally:
        if scratch is not None:
            # Python discovery/probes have returned. Bootstrap children are
            # synchronous; application failure sites and owned EDA cleanup stay
            # in their run directories, separate from this tiny launch folder.
            try:
                if successful:
                    cleanup_scratch(scratch["path"], scratch["token"], producers_stopped=True)
                else:
                    # Keep the small first-failure diagnostic retryable; only
                    # a future process may reclaim this sealed dead bootstrap.
                    seal_scratch(scratch["path"], scratch["token"])
            except (OSError, ValueError):
                pass


if __name__ == "__main__":
    sys.exit(main())
