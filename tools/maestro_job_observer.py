#!/usr/bin/env python3
"""Observe an owned Maestro submission boundary without changing scheduler argv.

The shell launcher strips Python/Tk loader overrides only for this interpreter.
The real command receives every captured native loader state unchanged. This
observer synchronously forwards native stdout/stderr and the real return code.
"""
from __future__ import print_function

import argparse
import datetime
import errno
import fcntl
import json
import os
import re
import signal
import select
import stat
import subprocess
import sys
import threading
import time
import uuid

# Bind the parser shipped beside this observer, including wheel resources.
# No inherited PYTHONPATH or an unrelated installed Crescendo may choose its version.
_observer_file = os.path.abspath(__file__)
_source_root = os.path.dirname(os.path.dirname(_observer_file))
_installed_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_observer_file))))
for _parser_root in (_source_root, _installed_root):
    if os.path.isfile(os.path.join(_parser_root, "crescendo_engine", "scheduler_protocol.py")):
        sys.path.insert(0, _parser_root)
        break
from crescendo_engine.scheduler_protocol import (MAX_PROTOCOL_TEXT,
                                                  parse_submit_job_id,
                                                  parse_submit_observation)

ENV_NAMES = ("LD_LIBRARY_PATH", "LD_PRELOAD", "PYTHONHOME", "PYTHONPATH",
             "TCL_LIBRARY", "TK_LIBRARY", "QT_PLUGIN_PATH")
MAX_CHUNK = 8192
MAX_OUTPUT_BYTES = 1024 * 1024
POST_EXIT_DRAIN_SECONDS = 1.0


def native_environment():
    env = dict(os.environ)
    for name in ENV_NAMES:
        marker = env.pop("CRESCENDO_JOB_ENV_%s_SET" % name, "")
        value = env.pop("CRESCENDO_JOB_ENV_%s_VALUE" % name, "")
        if marker == "x":
            env[name] = value
        else:
            env.pop(name, None)
    # The submit CLI uses the caller's existing login context. The shell
    # launcher and Ocean retain their private HOME until this boundary.
    marker = env.pop("CRESCENDO_SCHEDULER_HOME_SET", None)
    value = env.pop("CRESCENDO_SCHEDULER_HOME_VALUE", "")
    if marker is not None:
        env["CRESCENDO_SIMULATION_HOME_SET"] = "x" if "HOME" in env else ""
        env["CRESCENDO_SIMULATION_HOME_VALUE"] = env.get("HOME", "")
    if marker == "x":
        env["HOME"] = value
    elif marker == "":
        env.pop("HOME", None)
    elif marker is not None:
        raise ValueError("Scheduler HOME marker is invalid")
    return env


def read_config(path):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("Submission observer configuration has invalid ownership")
        value = json.loads(stream.read(128 * 1024))
    if (value.get("schema_version") != "crescendo.maestro_job_observer.v1"
            or not isinstance(value.get("argv"), list) or not value["argv"]
            or any(not isinstance(item, str) for item in value["argv"])
            or not os.path.isabs(value.get("log_path") or "")):
        raise ValueError("Submission observer configuration is invalid")
    return value


def append_event(path, record):
    value = dict(record, schema_version="crescendo.maestro_submission_event.v1",
                 time=datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    raw = (json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n").encode("ascii")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("Submission observer log has invalid ownership")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        offset = 0
        while offset < len(raw):
            offset += os.write(descriptor, raw[offset:])
    finally:
        os.close(descriptor)


def scheduler_job_ids(text, complete=True):
    """One unambiguous numeric submission record, not arbitrary log integers."""
    if not complete:
        # A job ID token can itself span writes. Publish complete lines only
        # until EOF; the complete terminal record is parsed again below.
        text = text.rsplit("\n", 1)[0] + "\n" if "\n" in text else ""
    identifier = parse_submit_job_id(text)
    return [identifier] if identifier is not None else []


def channel_job_ids(channels, complete=True):
    """Combine complete records while preserving each native stream's framing."""
    found = set()
    invalid = False
    for text in channels:
        if not complete:
            text = text.rsplit("\n", 1)[0] + "\n" if "\n" in text else ""
        observation = parse_submit_observation(text)
        found.update(observation["observed_job_ids"])
        invalid = invalid or observation["invalid"] or observation["rejected"]
    return sorted(found) if len(found) == 1 and not invalid else []


def run(config, native_args):
    invocation = uuid.uuid4().hex
    argv = list(config["argv"]) + list(native_args)
    common = {"invocation_id": invocation, "argv": argv, "cwd": os.getcwd(),
              "original_command": config["command"], "observer_pid": os.getpid(),
              "native_arguments": list(native_args)}
    path = config["log_path"]
    restored_environment = native_environment()
    common["scheduler_context"] = {
        "caller_home_marker_present": "CRESCENDO_SCHEDULER_HOME_SET" in os.environ,
        "home_state": "unset" if "HOME" not in restored_environment else (
            "empty" if restored_environment["HOME"] == "" else "set"),
        "home_changed_from_ocean": restored_environment.get("HOME") != os.environ.get("HOME"),
        "credential_files_inspected": False,
    }
    append_event(path, dict(common, event="submit_start"))
    try:
        child = subprocess.Popen(argv, env=restored_environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as error:
        append_event(path, dict(common, event="submit_end", returncode=127,
                                error="%s: %s" % (type(error).__name__, error), job_ids=[]))
        sys.stderr.write("Crescendo native submission failed: %s\n" % error)
        return 127
    state = {"bytes": 0, "job_ids": [], "stdout": "", "stderr": "",
             "protocol_stdout": "", "protocol_stderr": "", "protocol_limited": False,
             "child_pipes_retained": [], "output_incomplete": False, "drain_deadline": None}
    lock = threading.Lock()
    child_finished = threading.Event()

    def forward(pipe, output, name):
        try:
            descriptor = pipe.fileno()
            fcntl.fcntl(descriptor, fcntl.F_SETFL,
                        fcntl.fcntl(descriptor, fcntl.F_GETFL) | os.O_NONBLOCK)
            while True:
                if child_finished.is_set() and time.monotonic() >= state["drain_deadline"]:
                    # A scheduler helper can inherit these pipes after batch-submit has
                    # exited. Stop observing after a short drain; never signal
                    # that helper or treat its EOF as remote-job completion.
                    with lock:
                        state["child_pipes_retained"].append(name)
                        state["output_incomplete"] = True
                    break
                readable, unused_write, unused_errors = select.select([descriptor], [], [], 0.1)
                if not readable:
                    continue
                try:
                    raw = os.read(descriptor, MAX_CHUNK)
                except OSError as error:
                    if error.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR):
                        continue
                    raise
                if not raw:
                    break
                target = getattr(output, "buffer", output)
                try:
                    target.write(raw)
                    target.flush()
                except (TypeError, AttributeError):
                    output.write(raw.decode("utf-8", "replace"))
                    output.flush()
                except (BrokenPipeError, IOError):
                    pass
                text = raw.decode("utf-8", "replace")
                with lock:
                    state[name] = (state[name] + text)[-MAX_CHUNK:]
                    protocol_size = len(state["protocol_stdout"]) + len(state["protocol_stderr"])
                    if protocol_size + len(text) > MAX_PROTOCOL_TEXT:
                        state["protocol_limited"] = True
                    if not state["protocol_limited"]:
                        state["protocol_" + name] += text
                        # Each channel must complete its own line. Joining a
                        # partial stdout number with stderr must not finish it.
                        ids = channel_job_ids((state["protocol_stdout"],
                                               state["protocol_stderr"]), complete=False)
                    else:
                        ids = []
                    state["job_ids"] = ids
                    record_output = state["bytes"] < MAX_OUTPUT_BYTES
                    state["bytes"] += len(raw)
                if record_output:
                    append_event(path, dict(common, event="submit_output", stream=name,
                                            text=text, job_ids=ids))
        finally:
            pipe.close()

    def relay_signal(signum, frame):
        try:
            child.send_signal(signum)
        except OSError:
            pass
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, relay_signal)
    threads = [threading.Thread(target=forward, args=(child.stdout, sys.stdout, "stdout")),
               threading.Thread(target=forward, args=(child.stderr, sys.stderr, "stderr"))]
    for thread in threads:
        thread.daemon = True
        thread.start()
    code = child.wait()
    state["drain_deadline"] = time.monotonic() + POST_EXIT_DRAIN_SECONDS
    child_finished.set()
    for thread in threads:
        thread.join(timeout=max(0.0, state["drain_deadline"] - time.monotonic()) + 0.25)
    forward_threads_stopped = all(not thread.is_alive() for thread in threads)
    if not forward_threads_stopped:
        state["output_incomplete"] = True
    # Do not retain a once-seen ID after later records make it ambiguous.
    if state["protocol_limited"]:
        state["job_ids"] = []
    else:
        state["job_ids"] = channel_job_ids((state["protocol_stdout"],
                                            state["protocol_stderr"]),
                                           complete=not state["output_incomplete"])
    append_event(path, dict(common, event="submit_end", returncode=code,
                            stdout_tail=state["stdout"], stderr_tail=state["stderr"],
                            job_ids=sorted(state["job_ids"]), output_bytes=state["bytes"],
                            output_truncated=state["bytes"] > MAX_OUTPUT_BYTES,
                            output_incomplete=state["output_incomplete"],
                            job_id_protocol_limited=state["protocol_limited"],
                            child_pipes_retained=sorted(state["child_pipes_retained"]),
                            post_exit_drain_seconds=POST_EXIT_DRAIN_SECONDS,
                            forward_threads_stopped=forward_threads_stopped))
    return 128 - code if code < 0 else code


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("native_args", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    args = arguments.native_args
    if args[:1] == ["--"]:
        args = args[1:]
    return run(read_config(arguments.config), args)


if __name__ == "__main__":
    sys.exit(main())
