from __future__ import print_function

import json
import math
import os
import shutil
import signal
import subprocess
import threading
import time
import uuid

from .cadence import (
    CommandCancelled, build_guardian_command, check_command_cancellation,
    child_environment, guardian_environment,
)
from .utils import atomic_write_json, atomic_write_text, ensure_dir, require_under
from .workspace import build_workspace, verify_sources_unchanged


def _skill_quote(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


class ResidentValidationSession(object):
    """Private, headless Cadence session used only for optional range validation."""

    def __init__(self, prepared_id, prepared, config, session_root,
                 popen_factory=None, clock=None, sleeper=None):
        self.prepared_id = str(prepared_id)
        self.prepared = prepared
        self.config = config
        self.session_root = os.path.abspath(session_root)
        self.workspace_root = os.path.join(self.session_root, "workspace")
        self.control_root = ensure_dir(os.path.join(self.session_root, "control"))
        self.guardian_state_path = os.path.join(self.control_root, "guardian_state.json")
        self.stdout_path = os.path.join(self.control_root, "ocean.stdout")
        self.stderr_path = os.path.join(self.control_root, "ocean.stderr")
        self.token = "validation-%s-%s" % (self.prepared_id, uuid.uuid4().hex)
        self._popen = popen_factory or subprocess.Popen
        self._clock = clock or time.monotonic
        self._sleep = sleeper or time.sleep
        self._lock = threading.RLock()
        self.process = None
        self.workspace = None
        self.closed = False

    def _state(self):
        try:
            with open(self.guardian_state_path, "r") as stream:
                value = json.load(stream)
            return value if isinstance(value, dict) else {}
        except (IOError, OSError, ValueError):
            return {}

    def _alive(self):
        return bool(self.process and self.process.poll() is None and not self.closed)

    def start(self, timeout_seconds=None, cancel_check=None, deadline=None, parameters=None):
        with self._lock:
            check_command_cancellation()
            if cancel_check and cancel_check():
                raise CommandCancelled("optional PDK validation cancelled during startup")
            if self.closed:
                raise RuntimeError("closed resident validation session cannot be restarted")
            if self._alive():
                return self
            ensure_dir(self.session_root)
            self.workspace = build_workspace(
                self.prepared["setup"], self.prepared["hierarchy"], self.config,
                self.workspace_root,
                expected_source_identity=self.prepared.get("source_identity"),
                parameters=parameters,
            )
            timeout_seconds = float(timeout_seconds or self.config.get("runtime", {}).get(
                "ocean_startup_timeout_seconds", 90,
            ))
            startup_deadline = self._clock() + timeout_seconds
            if deadline is not None:
                startup_deadline = min(startup_deadline, float(deadline))
            check_command_cancellation()
            if self._clock() >= startup_deadline:
                raise RuntimeError("optional PDK validation startup deadline exceeded")
            cadence_command = [self.config["cadence"]["virtuoso_bin"], "-nograph"]
            command = build_guardian_command(
                cadence_command, self.workspace["root"], self.stdout_path, self.stderr_path,
                self.guardian_state_path, self.token,
                termination_grace_seconds=self.config.get("runtime", {}).get(
                    "range_validation_shutdown_timeout_seconds", 30,
                ),
                poll_expected_parent=True,
            )
            environment = guardian_environment(child_environment(
                self.config, self.workspace["root"], include_runtime_ld=True,
            ))
            environment["CRESCENDO_LEGACY_GUARDIAN_TOKEN"] = self.token
            self.process = self._popen(
                command, cwd=self.workspace["root"], env=environment,
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, universal_newlines=True, bufsize=1,
            )
            while self._clock() < startup_deadline:
                check_command_cancellation()
                if cancel_check and cancel_check():
                    raise CommandCancelled("optional PDK validation cancelled during startup")
                state = self._state()
                if state.get("token") == self.token and state.get("status") == "running":
                    break
                if self.process.poll() is not None:
                    raise RuntimeError("resident validation guardian exited during startup")
                self._sleep(0.05)
            else:
                self.close(timeout_seconds=5)
                raise RuntimeError("resident validation Cadence session startup timed out")
            ping = os.path.join(self.control_root, "startup_ping.il")
            atomic_write_text(ping, 'printf("CRESCENDO_RESIDENT_VALIDATION_READY\\n")\n')
            result = self.execute(
                ping, "startup", timeout_seconds=max(0.0, startup_deadline - self._clock()),
                cancel_check=cancel_check,
            )
            if result["returncode"] != 0:
                self.close(timeout_seconds=5)
                raise RuntimeError("resident validation Cadence session did not accept SKILL")
            check_command_cancellation()
            return self

    def execute(self, payload_path, operation, timeout_seconds, cancel_check=None):
        with self._lock:
            if not self._alive():
                raise RuntimeError("resident validation Cadence session is not alive")
            nonce = uuid.uuid4().hex
            marker = os.path.join(self.control_root, "%s_%s.status" % (operation, nonce))
            request = os.path.join(self.control_root, "%s_%s.il" % (operation, nonce))
            lines = [
                'crescResidentResult = errset(load("%s") t)' % _skill_quote(os.path.abspath(payload_path)),
                'crescResidentPort = outfile("%s" "w")' % _skill_quote(marker),
                'if(crescResidentResult then fprintf(crescResidentPort "OK\\n") else fprintf(crescResidentPort "ERROR\\n"))',
                'close(crescResidentPort)',
            ]
            atomic_write_text(request, "\n".join(lines) + "\n")
            started = self._clock()
            try:
                self.process.stdin.write('load("%s")\n' % _skill_quote(request))
                self.process.stdin.flush()
            except Exception as error:
                raise RuntimeError("failed to submit resident validation SKILL: %s" % error)
            deadline = started + float(timeout_seconds)
            cancelled = False
            while self._clock() < deadline:
                if cancel_check and cancel_check():
                    cancelled = True
                    break
                if os.path.isfile(marker):
                    with open(marker, "r") as stream:
                        status = stream.read().strip()
                    return {
                        "argv": ["resident-cadence", os.path.abspath(payload_path)],
                        "cwd": self.workspace["root"], "returncode": 0 if status == "OK" else 1,
                        "timed_out": False, "cancelled": False, "termination_failed": False,
                        "lock_wait_seconds": 0.0, "elapsed_seconds": self._clock() - started,
                        "resident_session": True,
                    }
                if self.process.poll() is not None:
                    return {
                        "argv": ["resident-cadence", os.path.abspath(payload_path)],
                        "cwd": self.workspace["root"], "returncode": self.process.returncode or 1,
                        "timed_out": False, "cancelled": False, "termination_failed": True,
                        "lock_wait_seconds": 0.0, "elapsed_seconds": self._clock() - started,
                        "resident_session": True,
                    }
                self._sleep(0.05)
            return {
                "argv": ["resident-cadence", os.path.abspath(payload_path)],
                "cwd": self.workspace["root"], "returncode": -1,
                "timed_out": not cancelled, "cancelled": cancelled,
                "termination_failed": False, "lock_wait_seconds": 0.0,
                "elapsed_seconds": self._clock() - started, "resident_session": True,
            }

    def export_diagnostics(self, destination, max_bytes=2 * 1024 * 1024):
        destination = os.path.abspath(destination)
        require_under(
            destination, self.config["runtime"]["run_root"],
            "resident validation diagnostics",
        )
        ensure_dir(destination)
        manifest = {
            "schema_version": "crescendo.resident_validation_diagnostics.v1",
            "prepared_id": self.prepared_id,
            "token": self.token,
            "files": [],
        }
        for source in (self.guardian_state_path, self.stdout_path, self.stderr_path):
            if not os.path.isfile(source) or os.path.islink(source):
                continue
            require_under(source, self.control_root, "resident validation diagnostic source")
            size = os.path.getsize(source)
            keep = min(size, int(max_bytes))
            with open(source, "rb") as stream:
                if keep < size:
                    stream.seek(size - keep)
                payload = stream.read()
            target = os.path.join(destination, os.path.basename(source))
            with open(target, "wb") as stream:
                stream.write(payload)
            manifest["files"].append({
                "name": os.path.basename(source),
                "source_size_bytes": size,
                "saved_size_bytes": len(payload),
                "tail_truncated": len(payload) < size,
            })
        atomic_write_json(os.path.join(destination, "manifest.json"), manifest)
        return manifest

    def close(self, timeout_seconds=30, preserve_on_error=False):
        timeout_seconds = float(timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds < 0:
            raise ValueError("resident validation cleanup timeout must be finite and non-negative")
        deadline = self._clock() + timeout_seconds
        if not self._lock.acquire(timeout=max(0.0, deadline - self._clock())):
            raise RuntimeError("resident validation cleanup deadline exceeded waiting for the session lock")
        try:
            if self.closed:
                return {"closed": True, "cleanup_proven": True}
            if self._clock() >= deadline:
                raise RuntimeError("resident validation cleanup deadline exceeded; owned evidence was retained")
            cleanup_proven = True
            error = None
            process = self.process
            if process and process.poll() is None:
                try:
                    process.stdin.write("exit()\n")
                    process.stdin.flush()
                    # Keep the existing TERM fallback inside the same total
                    # budget, rather than appending another five seconds.
                    term_budget = min(5.0, timeout_seconds / 2.0)
                    process.wait(timeout=max(0.0, deadline - self._clock() - term_budget))
                except Exception as close_error:
                    error = close_error
                    cleanup_proven = False
                    try:
                        if self._clock() < deadline and os.getpgid(process.pid) == process.pid:
                            os.killpg(process.pid, signal.SIGTERM)
                            process.wait(timeout=min(5.0, max(0.0, deadline - self._clock())))
                            cleanup_proven = True
                    except Exception:
                        cleanup_proven = False
            state = self._state()
            if state and not (
                    state.get("token") == self.token and state.get("status") == "exited"
                    and state.get("group_cleanup_complete") is True):
                cleanup_proven = False
            if self._clock() >= deadline:
                cleanup_proven = False
                error = RuntimeError("resident validation cleanup deadline exceeded; owned evidence was retained")
            if self.workspace and os.path.isdir(self.workspace.get("root", "")):
                source_check = verify_sources_unchanged(self.workspace)
                if not source_check.get("ok"):
                    cleanup_proven = False
                if cleanup_proven:
                    from .same_library_views import cleanup_same_library_views
                    cleanup_proven = cleanup_same_library_views(self.workspace, process_cleanup_proven=True)["cleanup_proven"]
                if cleanup_proven and not preserve_on_error:
                    require_under(self.workspace["root"], self.session_root, "resident validation workspace")
                    shutil.rmtree(self.workspace["root"])
            if not cleanup_proven:
                raise RuntimeError("resident validation Cadence cleanup could not be proven: %s" % error)
            if not preserve_on_error and os.path.isdir(self.session_root):
                require_under(
                    self.session_root, self.config["runtime"]["run_root"],
                    "resident validation session",
                )
                shutil.rmtree(self.session_root)
            self.closed = True
            return {"closed": True, "cleanup_proven": True}
        finally:
            self._lock.release()
