#!/usr/bin/env python3
from __future__ import print_function

import argparse
import ctypes
import errno
import fcntl
import json
import os
import select
import signal
import subprocess
import sys
import tempfile
import time


SCHEMA_VERSION = "crescendo.ocean_worker_guardian.v1"
PR_SET_PDEATHSIG = 1
PR_SET_CHILD_SUBREAPER = 36
# This file also runs outside the installed package; keep the bridge contract
# aligned with crescendo_engine.native_environment.GUI_ENV_NAMES.
CHILD_ENVIRONMENT_KEYS = (
    "LD_LIBRARY_PATH", "LD_PRELOAD", "PYTHONHOME", "PYTHONPATH",
    "TCL_LIBRARY", "TK_LIBRARY", "QT_PLUGIN_PATH",
)
CHILD_ENVIRONMENT_PREFIX = "CRESCENDO_GUARDIAN_CHILD_"
LAUNCH_TOKEN_ENVIRONMENT = "CRESCENDO_LEGACY_GUARDIAN_TOKEN"
_signal_reason = None


def _atomic_write_json(path, value):
    path = os.path.abspath(path)
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    handle, temporary = tempfile.mkstemp(
        prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory,
    )
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(temporary, path)
        try:
            directory_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _set_parent_death_signal():
    if not sys.platform.startswith("linux"):
        return
    expected_parent = os.getppid()
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    if os.getppid() != expected_parent:
        os.kill(os.getpid(), signal.SIGTERM)


def _arm_expected_parent_death_signal(expected_parent_pid):
    if expected_parent_pid is None:
        return None
    expected_parent_pid = int(expected_parent_pid)
    if expected_parent_pid <= 1:
        raise ValueError("expected parent pid must be greater than 1")
    if not sys.platform.startswith("linux"):
        return False
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0) != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    return os.getppid() == expected_parent_pid


def _set_child_subreaper():
    if not sys.platform.startswith("linux"):
        return False
    libc = ctypes.CDLL(None, use_errno=True)
    return libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0


def _child_environment(token=None):
    environment = dict(os.environ)
    for name in CHILD_ENVIRONMENT_KEYS:
        bridge = CHILD_ENVIRONMENT_PREFIX + name
        if bridge in environment:
            environment[name] = environment.pop(bridge)
    if token:
        environment[LAUNCH_TOKEN_ENVIRONMENT] = str(token)
    return environment


def _release_launch_lock(descriptor):
    if descriptor is None:
        return
    descriptor = int(descriptor)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _linux_boot_id():
    try:
        with open("/proc/sys/kernel/random/boot_id", "r") as stream:
            value = stream.read().strip()
    except (IOError, OSError):
        return None
    return value or None


def _process_stat(pid):
    if not sys.platform.startswith("linux"):
        return None
    try:
        with open("/proc/%d/stat" % int(pid)) as stream:
            text = stream.read()
    except (IOError, OSError, ValueError):
        return None
    closing = text.rfind(")")
    if closing < 0:
        return None
    fields = text[closing + 2:].split()
    if len(fields) <= 19:
        return None
    try:
        return {
            "state": fields[0],
            "ppid": int(fields[1]),
            "pgrp": int(fields[2]),
            "session": int(fields[3]),
            "start_ticks": int(fields[19]),
        }
    except (TypeError, ValueError):
        return None


def _process_group_members(expected_pgid):
    if not sys.platform.startswith("linux"):
        return []
    members = []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return members
    for entry in entries:
        if not entry.isdigit():
            continue
        info = _process_stat(int(entry))
        if info and info["pgrp"] == int(expected_pgid):
            row = dict(info)
            row["pid"] = int(entry)
            members.append(row)
    return sorted(members, key=lambda row: row["pid"])


def _process_group_exists(expected_pgid):
    try:
        os.killpg(int(expected_pgid), 0)
        return True
    except OSError as error:
        if getattr(error, "errno", None) == errno.ESRCH:
            return False
        if getattr(error, "errno", None) == errno.EPERM:
            return True
        raise


def _reap_adopted_children():
    while True:
        try:
            pid, _status = os.waitpid(-1, os.WNOHANG)
        except (ChildProcessError, OSError):
            return
        if pid <= 0:
            return


def _direct_children(parent_pid):
    if not sys.platform.startswith("linux"):
        return []
    children = []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return children
    for entry in entries:
        if not entry.isdigit():
            continue
        info = _process_stat(int(entry))
        if info and info["ppid"] == int(parent_pid):
            row = dict(info)
            row["pid"] = int(entry)
            children.append(row)
    return sorted(children, key=lambda row: row["pid"])


def _signal_direct_children(children, parent_pid, signum):
    sent = False
    for child in children:
        current = _process_stat(child["pid"])
        if (not current or current["ppid"] != int(parent_pid)
                or current["start_ticks"] != child["start_ticks"]):
            continue
        try:
            os.kill(child["pid"], signum)
            sent = True
        except OSError as error:
            if getattr(error, "errno", None) != errno.ESRCH:
                raise
    return sent


def _terminate_adopted_children(grace_seconds, poll_seconds, state_path, state, reason):
    parent_pid = os.getpid()
    _reap_adopted_children()
    children = _direct_children(parent_pid)
    term_sent = False
    kill_sent = False
    if children:
        _record(
            state_path, state, "terminating_adopted", termination_reason=reason,
            adopted_term_sent=False, adopted_kill_sent=False,
            adopted_pids=[row["pid"] for row in children],
        )
        term_sent = _signal_direct_children(children, parent_pid, signal.SIGTERM)
        deadline = time.time() + max(0.0, float(grace_seconds))
        while time.time() < deadline:
            _reap_adopted_children()
            children = _direct_children(parent_pid)
            if not children:
                break
            time.sleep(min(max(0.001, float(poll_seconds)), max(0.001, deadline - time.time())))
    children = _direct_children(parent_pid)
    if children:
        _record(
            state_path, state, "killing_adopted", termination_reason=reason,
            adopted_term_sent=term_sent, adopted_kill_sent=False,
            adopted_pids=[row["pid"] for row in children],
        )
        kill_sent = _signal_direct_children(children, parent_pid, signal.SIGKILL)
    cleanup_deadline = time.time() + max(1.0, float(poll_seconds) * 10.0)
    while time.time() < cleanup_deadline:
        _reap_adopted_children()
        children = _direct_children(parent_pid)
        if not children:
            break
        time.sleep(max(0.001, float(poll_seconds)))
    return term_sent, kill_sent, _direct_children(parent_pid)


def _record(state_path, state, status, **updates):
    state.update(updates)
    state["status"] = status
    state["updated_epoch"] = time.time()
    _atomic_write_json(state_path, state)


def _signal_handler(signum, _frame):
    global _signal_reason
    _signal_reason = "guardian_signal_%d" % int(signum)


def _owned_process_group(process, expected_pgid, expected_sid):
    if process.poll() is None:
        try:
            observed_pgid = os.getpgid(process.pid)
            observed_sid = os.getsid(process.pid)
        except OSError:
            return False
        if (observed_pgid != expected_pgid or observed_pgid != process.pid
                or observed_sid != expected_sid or observed_sid != process.pid):
            raise RuntimeError(
                "refusing to signal unexpected Ocean identity: "
                "pid=%d expected_pgid=%d observed_pgid=%d expected_sid=%d observed_sid=%d" % (
                    process.pid, expected_pgid, observed_pgid, expected_sid, observed_sid,
                )
            )
        return True
    if not _process_group_exists(expected_pgid):
        return False
    members = _process_group_members(expected_pgid)
    if not members:
        raise RuntimeError(
            "refusing to signal Ocean process group %d without inspectable members" % expected_pgid
        )
    unexpected = [row for row in members if row["session"] != expected_sid]
    if unexpected:
        raise RuntimeError(
            "refusing to signal reused Ocean process group %d with unexpected session members: %s" % (
                expected_pgid, ",".join(str(row["pid"]) for row in unexpected),
            )
        )
    return True


def _terminate_owned_group(process, expected_pgid, expected_sid, grace_seconds, poll_seconds,
                           state_path, state, reason):
    term_sent = False
    kill_sent = False
    if _owned_process_group(process, expected_pgid, expected_sid):
        _record(
            state_path, state, "terminating", termination_reason=reason,
            term_sent=False, kill_sent=False,
        )
        os.killpg(expected_pgid, signal.SIGTERM)
        term_sent = True
        deadline = time.time() + max(0.0, float(grace_seconds))
        while _owned_process_group(process, expected_pgid, expected_sid) and time.time() < deadline:
            time.sleep(min(max(0.001, float(poll_seconds)), max(0.001, deadline - time.time())))
    if _owned_process_group(process, expected_pgid, expected_sid):
        _record(
            state_path, state, "killing", termination_reason=reason,
            term_sent=term_sent, kill_sent=False,
        )
        os.killpg(expected_pgid, signal.SIGKILL)
        kill_sent = True
    return_code = process.wait()
    cleanup_deadline = time.time() + max(1.0, float(poll_seconds) * 10.0)
    while time.time() < cleanup_deadline:
        _reap_adopted_children()
        if not _process_group_exists(expected_pgid):
            break
        time.sleep(max(0.001, float(poll_seconds)))
    remaining = _process_group_members(expected_pgid) if _process_group_exists(expected_pgid) else []
    return return_code, term_sent, kill_sent, remaining


def _forward_complete_lines(process, buffer):
    while b"\n" in buffer:
        line, buffer = buffer.split(b"\n", 1)
        try:
            process.stdin.write(line + b"\n")
            process.stdin.flush()
        except (BrokenPipeError, OSError):
            return buffer, False
    return buffer, True


def _exit_status(return_code):
    if return_code is None:
        return 1
    if return_code >= 0:
        return min(int(return_code), 255)
    return min(128 + abs(int(return_code)), 255)


def run_guardian(args):
    global _signal_reason
    _signal_reason = None
    cwd = os.path.realpath(os.path.abspath(args.cwd))
    if not os.path.isdir(cwd):
        raise ValueError("Ocean worker cwd is not a directory: %s" % cwd)
    command = list(args.ocean_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ValueError("Ocean command is required after --")
    token = str(args.token or "").strip()
    if not token:
        raise ValueError("worker token must not be empty")

    state_path = os.path.abspath(args.state_path)
    stdout_path = os.path.abspath(args.stdout_path)
    stderr_path = os.path.abspath(args.stderr_path)
    for path in (stdout_path, stderr_path):
        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            os.makedirs(directory)
    child_subreaper = _set_child_subreaper()
    guardian_stat = _process_stat(os.getpid()) or {}
    state = {
        "schema_version": SCHEMA_VERSION,
        "token": token,
        "cwd": cwd,
        "boot_id": _linux_boot_id(),
        "launch_boot_id": _linux_boot_id(),
        "guardian_pid": os.getpid(),
        "guardian_start_ticks": guardian_stat.get("start_ticks"),
        "child_subreaper": child_subreaper,
        "pid": None,
        "pgid": None,
        "sid": None,
        "pid_start_ticks": None,
        "argv": command,
        "started_epoch": time.time(),
        "returncode": None,
        "termination_reason": None,
        "term_sent": False,
        "kill_sent": False,
        "expected_parent_pid": args.expected_parent_pid,
        "expected_parent_guard": None,
        "launch_lock_fd": args.launch_lock_fd,
        "launch_lock_released": args.launch_lock_fd is None,
    }
    previous_handlers = {}
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, _signal_handler)
    _record(state_path, state, "starting")
    try:
        _release_launch_lock(args.launch_lock_fd)
        state["launch_lock_released"] = True
    except Exception as error:
        _record(
            state_path, state, "launch_failed", finished_epoch=time.time(),
            error="launch lock release failed: %s: %s" % (error.__class__.__name__, error),
            returncode=127, pid=None, pgid=None, sid=None,
        )
        return 127

    if args.poll_expected_parent:
        if args.expected_parent_pid is None:
            raise ValueError("polled expected parent pid is required")
        expected_parent_guard = os.getppid() == int(args.expected_parent_pid)
    else:
        expected_parent_guard = _arm_expected_parent_death_signal(args.expected_parent_pid)
    state["expected_parent_guard"] = expected_parent_guard
    if expected_parent_guard is False or _signal_reason:
        reason = _signal_reason or "expected_parent_missing_before_ocean_spawn"
        _record(
            state_path, state, "exited", finished_epoch=time.time(), returncode=143,
            termination_reason=reason, group_cleanup_complete=True,
            remaining_group_pids=[], descendant_cleanup_complete=True,
            descendant_supervision_complete=True, remaining_adopted_pids=[],
        )
        return 143

    preexec = None if args.disable_pdeathsig else _set_parent_death_signal
    process = None
    with open(stdout_path, "ab", 0) as stdout_stream, open(stderr_path, "ab", 0) as stderr_stream:
        try:
            process = subprocess.Popen(
                command, cwd=cwd, env=_child_environment(token), stdin=subprocess.PIPE,
                stdout=stdout_stream, stderr=stderr_stream,
                start_new_session=True, preexec_fn=preexec,
            )
        except Exception as error:
            stdout_stream.close()
            stderr_stream.close()
            _record(
                state_path, state, "launch_failed", finished_epoch=time.time(),
                error="%s: %s" % (error.__class__.__name__, error), returncode=127,
                stdio_closed=True,
            )
            return 127

        expected_pgid = os.getpgid(process.pid)
        expected_sid = os.getsid(process.pid)
        if expected_pgid != process.pid or expected_sid != process.pid:
            process.kill()
            process.wait()
            stdout_stream.close()
            stderr_stream.close()
            _record(
                state_path, state, "launch_failed", finished_epoch=time.time(),
                error="Ocean did not start in an isolated process group and session", returncode=127,
                pid=process.pid, pgid=expected_pgid, sid=expected_sid,
                stdio_closed=True,
            )
            return 127
        process_stat = _process_stat(process.pid) or {}
        _record(
            state_path, state, "running", pid=process.pid, pgid=expected_pgid,
            sid=expected_sid, pid_start_ticks=process_stat.get("start_ticks"),
        )

        input_fd = sys.stdin.fileno()
        buffer = b""
        termination_reason = None
        loop_error = None
        try:
            while True:
                return_code = process.poll()
                if return_code is not None:
                    termination_reason = "ocean_exit"
                    break
                if _signal_reason:
                    termination_reason = _signal_reason
                    break
                if (args.poll_expected_parent
                        and os.getppid() != int(args.expected_parent_pid)):
                    termination_reason = "expected_parent_exit"
                    break
                try:
                    readable, _writable, _exceptional = select.select(
                        [input_fd], [], [], max(0.001, float(args.poll_interval_seconds)),
                    )
                except (OSError, select.error):
                    if _signal_reason:
                        termination_reason = _signal_reason
                        break
                    raise
                if not readable:
                    continue
                chunk = os.read(input_fd, 65536)
                if not chunk:
                    if buffer:
                        try:
                            process.stdin.write(buffer)
                            process.stdin.flush()
                        except (BrokenPipeError, OSError):
                            pass
                        buffer = b""
                    termination_reason = "stdin_eof"
                    break
                buffer += chunk
                buffer, forwarded = _forward_complete_lines(process, buffer)
                if not forwarded:
                    termination_reason = "ocean_exit"
                    break
        except Exception as error:
            loop_error = "%s: %s" % (error.__class__.__name__, error)
            termination_reason = "guardian_loop_error"
        finally:
            try:
                process.stdin.close()
            except (IOError, OSError):
                pass

        term_sent = False
        kill_sent = False
        remaining = []
        cleanup_error = None
        return_code = process.poll()
        try:
            if _owned_process_group(process, expected_pgid, expected_sid):
                return_code, term_sent, kill_sent, remaining = _terminate_owned_group(
                    process, expected_pgid, expected_sid, args.terminate_grace_seconds,
                    args.poll_interval_seconds, state_path, state, termination_reason,
                )
            elif return_code is None:
                return_code = process.wait()
        except Exception as error:
            cleanup_error = "%s: %s" % (error.__class__.__name__, error)
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=max(0.1, float(args.terminate_grace_seconds)))
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            return_code = process.returncode
            _reap_adopted_children()
            remaining = _process_group_members(expected_pgid)
        cleanup_complete = not bool(remaining) and not _process_group_exists(expected_pgid)
        adopted_term_sent, adopted_kill_sent, remaining_adopted = _terminate_adopted_children(
            args.terminate_grace_seconds, args.poll_interval_seconds,
            state_path, state, termination_reason,
        )
        descendant_supervision_complete = child_subreaper or not sys.platform.startswith("linux")
        cleanup_complete = (
            cleanup_complete and not remaining_adopted and descendant_supervision_complete
        )
        final_error = loop_error or cleanup_error
        # Cleanup observers can consume the terminal proof immediately. Release
        # our own output descriptors first so NFS cannot rename them to busy
        # .nfs files while deleting the proven-dead epoch.
        stdout_stream.close()
        stderr_stream.close()
        _record(
            state_path, state, "exited" if cleanup_complete else "cleanup_failed",
            returncode=return_code,
            termination_reason=termination_reason, term_sent=term_sent,
            kill_sent=kill_sent, finished_epoch=time.time(),
            group_cleanup_complete=cleanup_complete,
            remaining_group_pids=[row["pid"] for row in remaining],
            adopted_term_sent=adopted_term_sent,
            adopted_kill_sent=adopted_kill_sent,
            descendant_cleanup_complete=not bool(remaining_adopted),
            descendant_supervision_complete=descendant_supervision_complete,
            remaining_adopted_pids=[row["pid"] for row in remaining_adopted],
            stdio_closed=True,
            error=final_error,
        )
        # The expected-parent death signal can arrive after stdin EOF while we
        # are still terminating Ocean/adopted children. Keep our handler active
        # through the durable terminal record; default SIGTERM here would kill
        # the guardian and strand its owned Ocean group without a stop proof.
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        if final_error or not cleanup_complete:
            return 2
        return _exit_status(return_code)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Forward commands to one isolated Ocean worker and own its process-group lifecycle.",
    )
    parser.add_argument("--state-path", required=True)
    parser.add_argument("--stdout-path", required=True)
    parser.add_argument("--stderr-path", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--cwd", required=True)
    parser.add_argument("--terminate-grace-seconds", type=float, default=15.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.1)
    parser.add_argument("--disable-pdeathsig", action="store_true")
    parser.add_argument("--expected-parent-pid", type=int)
    parser.add_argument("--poll-expected-parent", action="store_true")
    parser.add_argument("--launch-lock-fd", type=int)
    parser.add_argument("ocean_command", nargs=argparse.REMAINDER)
    return parser.parse_args(argv)


def main(argv=None):
    try:
        return run_guardian(parse_args(argv))
    except Exception as error:
        sys.stderr.write("ocean worker guardian failed: %s: %s\n" % (error.__class__.__name__, error))
        return 2


if __name__ == "__main__":
    sys.exit(main())
