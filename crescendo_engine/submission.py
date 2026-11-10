from __future__ import print_function

"""Portable simulation submission contracts.

The module deliberately does not know how optimizer workspaces are built.  It
accepts a JSON-compatible request, resolves tools from that request/config/
manifest/PATH, produces a normalized command plan, and delegates process
lifecycle management to an executor.  A future scheduler only needs to replace
``LocalProcessExecutor`` while preserving the request/result schemas.
"""

import copy
import json
import os
import signal
import shutil
import subprocess
import time
import uuid

from .utils import atomic_write_json, ensure_dir, utc_timestamp


REQUEST_SCHEMA = "crescendo.simulation_request.v1"
RESULT_SCHEMA = "crescendo.simulation_result.v1"
TOOLCHAIN_SCHEMA = "crescendo.simulation_toolchain.v1"
SUPPORTED_ACTIONS = ("probe", "plan", "run")
SUPPORTED_BACKENDS = ("maestro", "adexl", "local_spectre")
PATH_FIELDS = (
    "cadence_root", "spectre_root", "virtuoso_bin", "ocean_bin", "spectre_bin",
    "cds_lib", "pdk_dir", "alps_bin", "alps_root", "alps_model_dir", "spectre_model_dir",
)


class SubmissionError(ValueError):
    pass


def _absolute(path, base_dir=None):
    if not path:
        return None
    path = os.path.expandvars(os.path.expanduser(str(path)))
    if not os.path.isabs(path) and base_dir:
        path = os.path.join(base_dir, path)
    return os.path.abspath(path)


def _path_list(values, base_dir=None):
    return [_absolute(value, base_dir) for value in values or [] if value]


def _prepend_path(existing, values):
    rows = []
    seen = set()
    for value in list(values or []) + str(existing or "").split(os.pathsep):
        if value and value not in seen:
            rows.append(value)
            seen.add(value)
    return os.pathsep.join(rows)


def _read_json(path):
    try:
        with open(path, "r") as stream:
            value = json.load(stream)
    except (IOError, ValueError) as error:
        raise SubmissionError("cannot read JSON %s: %s" % (path, error))
    if not isinstance(value, dict):
        raise SubmissionError("JSON document must be an object: %s" % path)
    return value


def _merge(base, overlay):
    result = copy.deepcopy(base or {})
    for key, value in (overlay or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _toolchain_fragment(document):
    if not isinstance(document, dict):
        return {}
    result = {}
    setup = document.get("eda_setup") or {}
    mode = setup.get("mode")
    if mode == "manual":
        result = _merge(result, setup.get("manual") or {})
    else:
        result = _merge(result, setup.get("auto_detected") or {})
    result = _merge(result, document.get("effective") or {})
    result = _merge(result, document.get("cadence") or {})
    result = _merge(result, document.get("toolchain") or {})
    return result


def _backend_settings(document):
    simulation = (document or {}).get("simulation") or {}
    return copy.deepcopy(simulation.get("backends") or {})


def _resolve_license(value, base_dir=None):
    if not value:
        return None
    rows = []
    for item in str(value).split(os.pathsep):
        if "@" in item:
            rows.append(item)
        else:
            rows.append(_absolute(item, base_dir))
    return os.pathsep.join(rows)


def _resolve_fragment(fragment, base_dir=None):
    result = copy.deepcopy(fragment or {})
    for key in PATH_FIELDS:
        if result.get(key):
            result[key] = _absolute(result[key], base_dir)
    if "extra_binary_paths" in result:
        result["extra_binary_paths"] = _path_list(result.get("extra_binary_paths"), base_dir)
    if "extra_library_paths" in result:
        result["extra_library_paths"] = _path_list(result.get("extra_library_paths"), base_dir)
    if result.get("license_file"):
        result["license_file"] = _resolve_license(result["license_file"], base_dir)
    return result


def _document_with_base(path):
    path = _absolute(path)
    return _read_json(path), os.path.dirname(path), path


def discover_toolchain(request, environ=None):
    """Resolve a toolchain without scanning vendor-specific installation paths."""
    request = request or {}
    environ = dict(os.environ if environ is None else environ)
    toolchain = {}
    backend_settings = {}
    sources = []
    for field in ("config_path", "toolchain_manifest_path"):
        if not request.get(field):
            continue
        document, base_dir, path = _document_with_base(request[field])
        toolchain = _merge(toolchain, _resolve_fragment(_toolchain_fragment(document), base_dir))
        backend_settings = _merge(backend_settings, _backend_settings(document))
        sources.append({"type": field, "path": path})
    if request.get("toolchain"):
        toolchain = _merge(
            toolchain,
            _resolve_fragment(request["toolchain"], request.get("_base_dir")),
        )
        sources.append({"type": "request", "path": None})
    environment_names = {
        "virtuoso_bin": "CRESCENDO_VIRTUOSO_BIN",
        "ocean_bin": "CRESCENDO_OCEAN_BIN",
        "spectre_bin": "CRESCENDO_SPECTRE_BIN",
        "alps_bin": "CRESCENDO_ALPS_BIN",
        "alps_root": "CRESCENDO_ALPS_ROOT",
        "cadence_root": "CRESCENDO_CADENCE_ROOT",
        "spectre_root": "CRESCENDO_SPECTRE_ROOT",
        "cds_lib": "CRESCENDO_CDS_LIB",
    }
    for key, env_name in environment_names.items():
        if not toolchain.get(key) and environ.get(env_name):
            toolchain[key] = _absolute(environ[env_name])
            sources.append({"type": "environment:%s" % env_name, "field": key})
    if not toolchain.get("license_file"):
        license_value = environ.get("CRESCENDO_LICENSE_FILE") or environ.get("CDS_LIC_FILE") or environ.get("LM_LICENSE_FILE")
        if license_value:
            toolchain["license_file"] = _resolve_license(license_value)
            sources.append({"type": "environment", "field": "license_file"})
    for key, command in (("virtuoso_bin", "virtuoso"), ("ocean_bin", "ocean"), ("spectre_bin", "spectre"), ("alps_bin", "alps")):
        if not toolchain.get(key):
            found = shutil.which(command, path=environ.get("PATH"))
            if found:
                toolchain[key] = os.path.abspath(found)
                sources.append({"type": "PATH", "field": key})
    toolchain.setdefault("extra_binary_paths", [])
    toolchain.setdefault("extra_library_paths", [])
    toolchain["schema_version"] = TOOLCHAIN_SCHEMA
    return {
        "toolchain": toolchain,
        "backend_settings": backend_settings,
        "sources": sources,
    }


def load_request(path):
    path = _absolute(path)
    request = _read_json(path)
    request["_base_dir"] = os.path.dirname(path)
    return request


def normalize_request(request, backend=None, action=None):
    request = copy.deepcopy(request or {})
    schema = request.get("schema_version") or REQUEST_SCHEMA
    if schema != REQUEST_SCHEMA:
        raise SubmissionError("unsupported request schema: %s" % schema)
    request["schema_version"] = schema
    request["action"] = action or request.get("action") or "run"
    request["backend"] = backend or request.get("backend") or "maestro"
    if request["action"] not in SUPPORTED_ACTIONS:
        raise SubmissionError("unsupported action: %s" % request["action"])
    if request["backend"] not in SUPPORTED_BACKENDS:
        raise SubmissionError("unsupported backend: %s" % request["backend"])
    request["request_id"] = str(request.get("request_id") or uuid.uuid4().hex)
    base_dir = request.get("_base_dir")
    request["workdir"] = _absolute(request.get("workdir") or base_dir or os.getcwd(), base_dir)
    request["run_dir"] = _absolute(
        request.get("run_dir") or os.path.join(request["workdir"], "submissions", request["request_id"]),
        base_dir,
    )
    if request.get("config_path"):
        request["config_path"] = _absolute(request["config_path"], base_dir)
    if request.get("toolchain_manifest_path"):
        request["toolchain_manifest_path"] = _absolute(request["toolchain_manifest_path"], base_dir)
    try:
        request["timeout_seconds"] = float(request.get("timeout_seconds") or 0)
    except (TypeError, ValueError):
        raise SubmissionError("timeout_seconds must be numeric")
    if request["timeout_seconds"] < 0:
        raise SubmissionError("timeout_seconds must be non-negative")
    return request


def build_process_environment(toolchain, request, environ=None):
    """Build a child-only environment and never mutate ``os.environ``."""
    environment = request.get("environment") or {}
    inherit = environment.get("inherit", True)
    child = dict((os.environ if environ is None else environ) if inherit else {})
    binary_paths = list(toolchain.get("extra_binary_paths") or [])
    for key in ("virtuoso_bin", "ocean_bin", "spectre_bin", "alps_bin"):
        if toolchain.get(key):
            binary_paths.append(os.path.dirname(toolchain[key]))
    child["PATH"] = _prepend_path(child.get("PATH"), binary_paths)
    child["LD_LIBRARY_PATH"] = _prepend_path(
        child.get("LD_LIBRARY_PATH"), toolchain.get("extra_library_paths") or [],
    )
    child["HOME"] = _absolute(environment.get("home") or request["workdir"], request["workdir"])
    child["CDS_LOAD_ENV"] = str(environment.get("cds_load_env") or "CWD")
    child["CDS_AUTO_64BIT"] = str(environment.get("cds_auto_64bit") or "ALL")
    license_value = toolchain.get("license_file")
    if license_value:
        child["CDS_LIC_FILE"] = str(license_value)
        child["LM_LICENSE_FILE"] = _prepend_path(child.get("LM_LICENSE_FILE"), [license_value])
    for key in environment.get("unset") or []:
        child.pop(str(key), None)
    for key, value in (environment.get("set") or {}).items():
        child[str(key)] = str(value)
    return child


def _artifact(path, kind):
    path = _absolute(path) if path else None
    exists = bool(path and os.path.exists(path))
    row = {"kind": kind, "path": path, "exists": exists}
    if exists:
        stat = os.stat(path)
        row["size_bytes"] = stat.st_size
        row["modified_epoch"] = stat.st_mtime
        row["is_directory"] = os.path.isdir(path)
    return row


def _tail(path, limit=8000):
    if not path or not os.path.isfile(path):
        return ""
    with open(path, "rb") as stream:
        try:
            stream.seek(-limit, os.SEEK_END)
        except IOError:
            stream.seek(0)
        return stream.read().decode("utf-8", "replace")


class CommandPlan(object):
    def __init__(self, request, backend, argv, cwd, environment, artifacts):
        self.request = request
        self.backend = backend
        self.argv = list(argv)
        self.cwd = os.path.abspath(cwd)
        self.environment = dict(environment)
        self.artifacts = dict(artifacts)

    def as_dict(self):
        return {
            "backend": self.backend,
            "argv": list(self.argv),
            "cwd": self.cwd,
            "environment_keys": sorted(self.environment.keys()),
            "artifacts": dict(self.artifacts),
        }


class SimulationBackend(object):
    name = None
    default_enabled = True
    required_binary = None

    def enabled(self, request, settings):
        explicit = (request.get("backend_options") or {}).get("enabled")
        if explicit is not None:
            return bool(explicit)
        configured = (settings.get(self.name) or {}).get("enabled")
        if configured is not None:
            return bool(configured)
        return self.default_enabled

    def probe(self, request, toolchain, settings):
        binary = toolchain.get(self.required_binary)
        available = bool(binary and os.path.isfile(binary) and os.access(binary, os.X_OK))
        return {
            "backend": self.name,
            "available": available,
            "enabled": self.enabled(request, settings),
            "binary": binary,
            "message": "executable is available" if available else "%s was not resolved from config, manifest, environment, or PATH" % self.required_binary,
        }

    def _require_binary(self, toolchain):
        binary = toolchain.get(self.required_binary)
        if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
            raise SubmissionError("%s is not an executable: %s" % (self.required_binary, binary))
        return binary


class MaestroBackend(SimulationBackend):
    name = "maestro"
    default_enabled = True
    required_binary = "ocean_bin"

    def plan(self, request, toolchain, settings, environ=None):
        binary = self._require_binary(toolchain)
        options = request.get("maestro") or {}
        script = options.get("ocean_script")
        if not script:
            raise SubmissionError("maestro.ocean_script is required")
        script = _absolute(script, request["workdir"])
        if not os.path.isfile(script):
            raise SubmissionError("Maestro Ocean script does not exist: %s" % script)
        argv = [binary]
        argv.extend(str(value) for value in options.get("prefix_args") or ["-nograph", "-restore"])
        argv.append(script)
        argv.extend(str(value) for value in options.get("extra_args") or [])
        artifacts = {
            "stdout": _absolute(options.get("stdout_path") or os.path.join(request["run_dir"], "ocean.stdout")),
            "stderr": _absolute(options.get("stderr_path") or os.path.join(request["run_dir"], "ocean.stderr")),
        }
        for key, value in (options.get("artifacts") or {}).items():
            artifacts[str(key)] = _absolute(value, request["workdir"])
        environment = build_process_environment(toolchain, request, environ=environ)
        return CommandPlan(request, self.name, argv, request["workdir"], environment, artifacts)


class AdeXLBackend(SimulationBackend):
    name = "adexl"
    default_enabled = True
    required_binary = "virtuoso_bin"

    def plan(self, request, toolchain, settings, environ=None):
        binary = self._require_binary(toolchain)
        options = request.get("adexl") or {}
        script = options.get("skill_script") or options.get("driver_script")
        if not script:
            raise SubmissionError("adexl.skill_script is required")
        script = _absolute(script, request["workdir"])
        if not os.path.isfile(script):
            raise SubmissionError("ADE XL SKILL script does not exist: %s" % script)
        argv = [binary]
        argv.extend(str(value) for value in options.get("prefix_args") or ["-nograph", "-restore"])
        argv.append(script)
        argv.extend(str(value) for value in options.get("extra_args") or [])
        artifacts = {
            "stdout": _absolute(options.get("stdout_path") or os.path.join(request["run_dir"], "adexl.stdout")),
            "stderr": _absolute(options.get("stderr_path") or os.path.join(request["run_dir"], "adexl.stderr")),
        }
        for key, value in (options.get("artifacts") or {}).items():
            artifacts[str(key)] = _absolute(value, request["workdir"])
        environment = build_process_environment(toolchain, request, environ=environ)
        return CommandPlan(request, self.name, argv, request["workdir"], environment, artifacts)


class LocalSpectreBackend(SimulationBackend):
    name = "local_spectre"
    default_enabled = False
    required_binary = "spectre_bin"

    def plan(self, request, toolchain, settings, environ=None):
        binary = self._require_binary(toolchain)
        options = request.get("spectre") or {}
        netlist = options.get("netlist")
        if not netlist:
            raise SubmissionError("spectre.netlist is required")
        netlist = _absolute(netlist, request["workdir"])
        if not os.path.isfile(netlist):
            raise SubmissionError("Spectre netlist does not exist: %s" % netlist)
        log_path = _absolute(options.get("log_path") or os.path.join(request["run_dir"], "spectre.out"))
        raw_path = _absolute(options.get("raw_path") or os.path.join(request["run_dir"], "psf"))
        argv = [binary, netlist]
        if options.get("escchars", True):
            argv.append("+escchars")
        if log_path:
            argv.extend(["+log", log_path])
        if options.get("use_64bit", True):
            argv.append("-64")
        output_format = options.get("output_format")
        if output_format:
            argv.extend(["-format", str(output_format)])
        if raw_path:
            argv.extend(["-raw", raw_path])
        configured_args = (settings.get(self.name) or {}).get("extra_args") or []
        argv.extend(str(value) for value in configured_args)
        argv.extend(str(value) for value in options.get("extra_args") or [])
        artifacts = {
            "stdout": _absolute(options.get("stdout_path") or os.path.join(request["run_dir"], "spectre.stdout")),
            "stderr": _absolute(options.get("stderr_path") or os.path.join(request["run_dir"], "spectre.stderr")),
            "log": log_path,
            "raw": raw_path,
        }
        for key, value in (options.get("artifacts") or {}).items():
            artifacts[str(key)] = _absolute(value, request["workdir"])
        environment = build_process_environment(toolchain, request, environ=environ)
        return CommandPlan(request, self.name, argv, request["workdir"], environment, artifacts)


class LocalProcessHandle(object):
    def __init__(self, identifier, plan, process, submitted_epoch):
        self.identifier = identifier
        self.plan = plan
        self.process = process
        self.submitted_epoch = submitted_epoch
        self.finished_epoch = None
        self.timed_out = False
        self.cancelled = False
        self.cancel_reason = None


class LocalProcessExecutor(object):
    """Process-group based executor with timeout, cancellation and collection."""

    name = "local_process"

    def __init__(self, poll_interval_seconds=0.05, terminate_grace_seconds=2.0):
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.terminate_grace_seconds = float(terminate_grace_seconds)

    def submit(self, plan):
        ensure_dir(plan.request["run_dir"])
        stdout_path = plan.artifacts["stdout"]
        stderr_path = plan.artifacts["stderr"]
        ensure_dir(os.path.dirname(stdout_path))
        ensure_dir(os.path.dirname(stderr_path))
        stdout_stream = open(stdout_path, "wb")
        stderr_stream = open(stderr_path, "wb")
        submitted = time.time()
        try:
            process = subprocess.Popen(
                plan.argv,
                cwd=plan.cwd,
                env=plan.environment,
                stdout=stdout_stream,
                stderr=stderr_stream,
                preexec_fn=os.setsid,
            )
        finally:
            stdout_stream.close()
            stderr_stream.close()
        return LocalProcessHandle(plan.request["request_id"], plan, process, submitted)

    def _terminate(self, handle):
        if handle.process.poll() is not None:
            return
        try:
            os.killpg(handle.process.pid, signal.SIGTERM)
        except OSError:
            pass
        deadline = time.time() + self.terminate_grace_seconds
        while handle.process.poll() is None and time.time() < deadline:
            time.sleep(self.poll_interval_seconds)
        if handle.process.poll() is None:
            try:
                os.killpg(handle.process.pid, signal.SIGKILL)
            except OSError:
                pass
        handle.process.wait()

    def cancel(self, handle, reason="cancelled"):
        if handle.process.poll() is None:
            handle.cancelled = reason != "timeout"
            handle.timed_out = reason == "timeout"
            handle.cancel_reason = reason
            self._terminate(handle)
        if handle.finished_epoch is None:
            handle.finished_epoch = time.time()
        return self.collect(handle, wait=False)

    def collect(self, handle, wait=True, timeout_seconds=None):
        request_timeout = float(handle.plan.request.get("timeout_seconds") or 0)
        deadlines = []
        if request_timeout > 0:
            deadlines.append(handle.submitted_epoch + request_timeout)
        if timeout_seconds is not None and float(timeout_seconds) >= 0:
            deadlines.append(time.time() + float(timeout_seconds))
        deadline = min(deadlines) if deadlines else None
        while wait and handle.process.poll() is None:
            if deadline is not None and time.time() >= deadline:
                return self.cancel(handle, reason="timeout")
            time.sleep(self.poll_interval_seconds)
        returncode = handle.process.poll()
        if returncode is not None and handle.finished_epoch is None:
            handle.finished_epoch = time.time()
        if handle.timed_out:
            status = "timed_out"
        elif handle.cancelled:
            status = "cancelled"
        elif returncode is None:
            status = "running"
        elif returncode == 0:
            status = "completed"
        else:
            status = "failed"
        artifacts = [_artifact(path, name) for name, path in sorted(handle.plan.artifacts.items())]
        result = {
            "schema_version": RESULT_SCHEMA,
            "request_id": handle.identifier,
            "action": "run",
            "backend": handle.plan.backend,
            "executor": self.name,
            "status": status,
            "returncode": returncode,
            "timed_out": handle.timed_out,
            "cancelled": handle.cancelled,
            "cancel_reason": handle.cancel_reason,
            "collected_at": utc_timestamp(),
            "timing": {
                "submitted_epoch": handle.submitted_epoch,
                "finished_epoch": handle.finished_epoch,
                "elapsed_seconds": (handle.finished_epoch or time.time()) - handle.submitted_epoch,
            },
            "command": handle.plan.as_dict(),
            "artifacts": artifacts,
            "stdout_tail": _tail(handle.plan.artifacts.get("stdout")),
            "stderr_tail": _tail(handle.plan.artifacts.get("stderr")),
        }
        return result


class SimulationSubmissionService(object):
    def __init__(self, executor=None, environ=None, backends=None):
        self.executor = executor or LocalProcessExecutor()
        self.environ = dict(os.environ if environ is None else environ)
        self.backends = backends or {
            "maestro": MaestroBackend(),
            "adexl": AdeXLBackend(),
            "local_spectre": LocalSpectreBackend(),
        }
        self.handles = {}

    def _context(self, request):
        resolved = discover_toolchain(request, environ=self.environ)
        backend = self.backends[request["backend"]]
        return resolved, backend

    def probe(self, request):
        request = normalize_request(request, action="probe")
        resolved, selected = self._context(request)
        probes = [
            self.backends[name].probe(request, resolved["toolchain"], resolved["backend_settings"])
            for name in sorted(self.backends)
        ]
        selected_probe = [row for row in probes if row["backend"] == selected.name][0]
        return {
            "schema_version": RESULT_SCHEMA,
            "request_id": request["request_id"],
            "action": "probe",
            "backend": selected.name,
            "status": "available" if selected_probe["available"] else "unavailable",
            "selected": selected_probe,
            "backends": probes,
            "toolchain": resolved["toolchain"],
            "sources": resolved["sources"],
        }

    def plan(self, request):
        request = normalize_request(request, action="plan")
        resolved, backend = self._context(request)
        plan = backend.plan(
            request,
            resolved["toolchain"],
            resolved["backend_settings"],
            environ=self.environ,
        )
        probe = backend.probe(request, resolved["toolchain"], resolved["backend_settings"])
        return plan, probe, resolved

    def submit(self, request):
        request = normalize_request(request, action="run")
        resolved, backend = self._context(request)
        probe = backend.probe(request, resolved["toolchain"], resolved["backend_settings"])
        if not probe["available"]:
            raise SubmissionError(probe["message"])
        if not probe["enabled"]:
            raise SubmissionError("backend %s is disabled; enable it explicitly in backend_options or configuration" % backend.name)
        plan = backend.plan(
            request,
            resolved["toolchain"],
            resolved["backend_settings"],
            environ=self.environ,
        )
        handle = self.executor.submit(plan)
        self.handles[handle.identifier] = handle
        return handle

    def collect(self, handle_or_id, wait=True, timeout_seconds=None):
        handle = self._handle(handle_or_id)
        return self.executor.collect(handle, wait=wait, timeout_seconds=timeout_seconds)

    def cancel(self, handle_or_id, reason="cancelled"):
        return self.executor.cancel(self._handle(handle_or_id), reason=reason)

    def run(self, request):
        return self.collect(self.submit(request), wait=True)

    def _handle(self, handle_or_id):
        if hasattr(handle_or_id, "identifier"):
            return handle_or_id
        if handle_or_id not in self.handles:
            raise SubmissionError("unknown process-local submission handle: %s" % handle_or_id)
        return self.handles[handle_or_id]


def error_result(request, error):
    request = request or {}
    return {
        "schema_version": RESULT_SCHEMA,
        "request_id": request.get("request_id"),
        "action": request.get("action"),
        "backend": request.get("backend"),
        "status": "invalid",
        "error": "%s: %s" % (error.__class__.__name__, error),
    }


def execute_request(request, service=None):
    service = service or SimulationSubmissionService()
    try:
        normalized = normalize_request(request)
        if normalized["action"] == "probe":
            return service.probe(normalized)
        if normalized["action"] == "plan":
            plan, probe, resolved = service.plan(normalized)
            return {
                "schema_version": RESULT_SCHEMA,
                "request_id": normalized["request_id"],
                "action": "plan",
                "backend": normalized["backend"],
                "status": "planned",
                "enabled": probe["enabled"],
                "available": probe["available"],
                "command": plan.as_dict(),
                "toolchain": resolved["toolchain"],
                "sources": resolved["sources"],
            }
        return service.run(normalized)
    except (SubmissionError, OSError, subprocess.SubprocessError) as error:
        return error_result(request, error)


def write_result(path, result):
    atomic_write_json(_absolute(path), result)
    return path
