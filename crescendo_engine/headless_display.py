from __future__ import print_function

import contextlib
import os
import random
import shutil
import subprocess
import time

from .utils import atomic_write_json, ensure_dir
from .cadence import check_command_cancellation


class HeadlessDisplayUnavailable(RuntimeError):
    pass


def _display_ready(number, process):
    if process.poll() is not None:
        return False
    try:
        with open("/tmp/.X%d-lock" % number, "r") as stream:
            owner_pid = int(stream.read().strip())
    except (IOError, OSError, ValueError):
        return False
    return owner_pid == process.pid and os.path.exists("/tmp/.X11-unix/X%d" % number)


def _stop_owned_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    return process.returncode


@contextlib.contextmanager
def owned_headless_display(environment, root, startup_timeout_seconds=5, max_attempts=8):
    """Supply a private X display for native read-only CDF editor contexts."""
    check_command_cancellation()
    environment = dict(environment)
    root = ensure_dir(root)
    manifest_path = os.path.join(root, "headless_display.json")
    if environment.get("CDS_NOGRAPH_DISPLAY"):
        manifest = {"owned": False, "display": environment["CDS_NOGRAPH_DISPLAY"],
                    "status": "provided", "manifest_path": manifest_path}
        atomic_write_json(manifest_path, manifest)
        yield environment, manifest
        return

    binary = shutil.which("Xvnc", path=environment.get("PATH"))
    if not binary:
        raise HeadlessDisplayUnavailable(
            "Read-only CDF condition evaluation requires an existing Xvnc executable "
            "or an explicitly supplied CDS_NOGRAPH_DISPLAY; no display was started."
        )
    attempts = []
    for attempt in range(int(max_attempts)):
        check_command_cancellation()
        number = random.SystemRandom().randrange(100, 1000)
        if (os.path.lexists("/tmp/.X%d-lock" % number)
                or os.path.lexists("/tmp/.X11-unix/X%d" % number)):
            attempts.append({"display": ":%d" % number, "status": "occupied"})
            continue
        display = ":%d" % number
        log_path = os.path.join(root, "headless_display_%02d.log" % attempt)
        # inetd mode uses standard input instead of opening an RFB listener.
        # X remains available through its private Unix socket without a viewer.
        argv = [binary, display, "-inetd", "-nolisten", "tcp", "-noreset", "-ac",
                "-geometry", "1024x868", "-depth", "24"]
        with open(log_path, "w") as log:
            try:
                # The system X server must not inherit Cadence runtime libraries.
                process = subprocess.Popen(argv, cwd=root, env=dict(os.environ),
                                           stdin=subprocess.DEVNULL, stdout=log,
                                           stderr=subprocess.STDOUT)
            except OSError as error:
                attempts.append({"display": display, "status": "launch_failed",
                                 "error": str(error), "log_path": log_path})
                continue
            deadline = time.monotonic() + float(startup_timeout_seconds)
            ready = False
            try:
                while process.poll() is None and time.monotonic() < deadline:
                    check_command_cancellation()
                    if _display_ready(number, process):
                        # A socket can appear before X extensions finish loading.
                        time.sleep(0.25)
                        check_command_cancellation()
                        ready = _display_ready(number, process)
                        break
                    time.sleep(0.05)
                if not ready:
                    attempts.append({"display": display, "status": "startup_failed",
                                     "log_path": log_path, "pid": process.pid})
                    continue
                check_command_cancellation()
                manifest = {"owned": True, "display": display, "pid": process.pid,
                            "binary": binary, "status": "ready", "log_path": log_path,
                            "manifest_path": manifest_path, "attempts": attempts,
                            "tcp_listening": False, "rfb_listening": False}
                atomic_write_json(manifest_path, manifest)
                environment["CDS_NOGRAPH_DISPLAY"] = display
                try:
                    yield environment, manifest
                finally:
                    manifest["returncode"] = _stop_owned_process(process)
                    manifest["status"] = "stopped"
                    atomic_write_json(manifest_path, manifest)
                return
            finally:
                _stop_owned_process(process)
    manifest = {"owned": False, "status": "failed", "binary": binary,
                "attempts": attempts, "manifest_path": manifest_path}
    atomic_write_json(manifest_path, manifest)
    raise HeadlessDisplayUnavailable(
        "Could not start a private read-only CDF display after %d bounded attempts; "
        "see %s" % (int(max_attempts), manifest_path)
    )
