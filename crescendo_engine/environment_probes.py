"""Independent, bounded observations for the one-point environment qualification.

This module never submits a job, modifies a source design, installs a package,
or grants a completion/stop proof.  Native execution remains in the existing
engines.  Every failed/unknown probe is reported and later probes still run.
"""
from __future__ import print_function

import errno
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
import os
import platform
import re
import selectors
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time

SCHEMA = "crescendo.environment_probe.v1"
OUTPUT_LIMIT = 128 * 1024
OUTPUT_TAIL = 24 * 1024
MAX_JOB_QUERIES = 64
_JOB_ID = re.compile(r"^[0-9]{1,24}$")
_PRIVATE_FIELD = re.compile(r"(?:token|nonce|password|secret|credential|cookie|api[_-]?key)", re.I)
_ENVIRONMENT_FIELDS = frozenset(("env", "environ", "environment", "launch_environment",
                               "native_environment", "native_environment_snapshot"))


def _cancelled(cancel):
    return bool(cancel and (cancel.is_set() if hasattr(cancel, "is_set") else cancel()))


def _stop_owned_group(process):
    for sig, delay in ((signal.SIGTERM, 0.2), (signal.SIGKILL, 0.2)):
        try:
            os.killpg(process.pid, sig)
        except OSError as error:
            if error.errno != errno.ESRCH:
                raise
        try:
            process.wait(timeout=delay)
        except subprocess.TimeoutExpired:
            pass
    try:
        process.wait(timeout=0.2)
    except subprocess.TimeoutExpired:
        pass


def bounded_command(argv, cwd=None, env=None, timeout=5.0, cancel=None,
                    output_limit=OUTPUT_LIMIT):
    """Run literal argv with no shell, short-lived pipes and a total output cap."""
    started = time.monotonic()
    result = {"argv": list(argv), "returncode": None, "timed_out": False,
              "cancelled": False, "output_limited": False,
              "stdout_tail": "", "stderr_tail": ""}
    process, selector = None, selectors.DefaultSelector()
    tails, byte_count = {"stdout": b"", "stderr": b""}, 0
    try:
        if _cancelled(cancel):
            result["cancelled"] = True
            return result
        process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True, close_fds=True)
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        while selector.get_map():
            elapsed = time.monotonic() - started
            if _cancelled(cancel) or elapsed >= timeout or byte_count > output_limit:
                result["cancelled"] = _cancelled(cancel)
                result["timed_out"] = not result["cancelled"] and elapsed >= timeout
                result["output_limited"] = byte_count > output_limit
                _stop_owned_group(process)
                break
            for key, _events in selector.select(min(0.05, max(0, timeout - elapsed))):
                chunk = os.read(key.fileobj.fileno(), 16384)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                byte_count += len(chunk)
                name = key.data
                tails[name] = (tails[name] + chunk)[-OUTPUT_TAIL:]
        if process.poll() is None:
            try:
                process.wait(timeout=max(0.01, timeout - (time.monotonic() - started)))
            except subprocess.TimeoutExpired:
                result["timed_out"] = True
                _stop_owned_group(process)
        result["returncode"] = process.poll()
    except (OSError, ValueError) as error:
        result["error"] = "%s: %s" % (type(error).__name__, error)
        if process is not None:
            _stop_owned_group(process)
    finally:
        selector.close()
        if process is not None:
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
        result["stdout_tail"] = tails["stdout"].decode("utf-8", "replace")
        result["stderr_tail"] = tails["stderr"].decode("utf-8", "replace")
        result["duration_seconds"] = round(time.monotonic() - started, 4)
    return result


def _command_status(result):
    if result.get("cancelled"):
        return "CANCELLED"
    if result.get("timed_out") or result.get("output_limited") or result.get("error"):
        return "FAIL"
    return "PASS" if result.get("returncode") == 0 else "FAIL"


def _redact(value, environment):
    """Do not expose complete environments, license endpoints or snapshots."""
    protected = [source.get(key) for source in (environment, os.environ) for key in source if key in
                 ("CDS_LIC_FILE", "LM_LICENSE_FILE", "CRESCENDO_NATIVE_ENV_SNAPSHOT") or
                 re.search(r"(?:TOKEN|SECRET|PASSWORD|CREDENTIAL|COOKIE|API_?KEY)", key, re.I)]
    protected = [item for item in protected if isinstance(item, str) and item]
    if isinstance(value, str):
        for item in sorted(protected, key=len, reverse=True):
            value = value.replace(item, "<redacted>")
        value = re.sub(r'(?i)(\b(?:token|password|secret|credential|cookie|api[_-]?key)\b\s*[:=]\s*)([^\s,;]+)',
                       r'\1<redacted>', value)
        value = re.sub(r'(?i)(\bBearer\s+)[A-Za-z0-9._~+/-]+', r'\1<redacted>', value)
        value = re.sub(r'\b[0-9]{2,6}@[A-Za-z0-9_.-]+\b', '<redacted-license>', value)
        return value
    if isinstance(value, dict):
        result = {}
        native_snapshot = (value.get("schema") or value.get("schema_version")) == "crescendo.native_environment.v1"
        for key, item in value.items():
            name = str(key)
            if _PRIVATE_FIELD.search(name) and item is not None and not isinstance(item, bool):
                result[key] = "<redacted>"
            elif native_snapshot and name == "values":
                result[key] = "<redacted>"
            elif name.lower() in _ENVIRONMENT_FIELDS and isinstance(item, dict):
                # Retain explicitly safe present/empty/unset metadata elsewhere;
                # complete snapshots or env.values mappings are private launch
                # data even if their license values differ from this process.
                result[key] = "<redacted>"
            elif name.lower() == 'environment_presence' and isinstance(item, dict):
                result[key] = {field: state if isinstance(state, bool) or state in ('set', 'empty', 'unset')
                               else '<redacted>' for field, state in item.items()}
            else:
                result[key] = _redact(item, environment)
        return result
    if isinstance(value, (list, tuple)):
        return [_redact(item, environment) for item in value]
    return value


class _Reporter(object):
    def __init__(self, emit, cancel, environment):
        self.emit, self.cancel, self.environment = emit, cancel, environment
        self.checks = []
        self.lock = threading.Lock()
        self.publish_errors = []

    def add(self, identifier, status, message, details=None, started=None):
        row = {"schema_version": SCHEMA, "id": identifier, "status": status,
               "message": message, "details": details or {},
               "duration_seconds": round(time.monotonic() - started, 4) if started else 0}
        row = _redact(row, self.environment)
        with self.lock:
            self.checks.append(row)
            if self.emit is not None:
                try:
                    self.emit(row)
                except Exception as error:
                    # An unavailable report destination must not prevent other
                    # observations or the exact owned-home cleanup. The return
                    # value retains the publishing error for the orchestrator.
                    message = _redact("%s: %s" % (type(error).__name__, error), self.environment)
                    row["report_publish_error"] = message
                    if len(self.publish_errors) < 8:
                        self.publish_errors.append({"id": identifier, "error": message})
        return row

    def check(self, identifier, operation, ignore_cancel=False):
        started = time.monotonic()
        if not ignore_cancel and _cancelled(self.cancel):
            return self.add(identifier, "CANCELLED", "Qualification was cancelled", started=started)
        try:
            status, message, details = operation()
        except Exception as error:
            status, message, details = "FAIL", "%s: %s" % (type(error).__name__, error), {}
        return self.add(identifier, status, message, details, started)


def _parallel_checks(reporter, operations):
    """A slow CLI consumes one slot while unrelated bounded checks continue."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(reporter.check, identifier, operation)
                   for identifier, operation in operations]
        for future in futures:
            future.result()


_PYTHON_PROBE = r'''
import importlib, json, os, platform, sys, sysconfig
r = {"executable":sys.executable,"version":list(sys.version_info[:3]),
     "prefix":sys.prefix,"base_prefix":getattr(sys,"base_prefix",sys.prefix),
     "filesystem_encoding":sys.getfilesystemencoding(),"stdout_encoding":sys.stdout.encoding,
     "imports":{},"tk":{}}
for name in ("json","sqlite3","ssl","ctypes","subprocess","selectors","multiprocessing",
             "threading","hashlib","tarfile","zlib","tkinter","tkinter.ttk"):
    try:
        module=importlib.import_module(name)
        r["imports"][name]={"ok":True,"file":getattr(module,"__file__",None)}
    except Exception as e:
        r["imports"][name]={"ok":False,"error":type(e).__name__+": "+str(e)[:2048]}
try:
    import _tkinter, tkinter as tk
    r["tk"].update(extension=_tkinter.__file__, compiled_tcl=str(tk.TclVersion), compiled_tk=str(tk.TkVersion))
    tcl=tk.Tcl()
    r["tk"]["runtime_tcl"]=str(tcl.call("info","patchlevel"))
    if tuple(int(n) for n in r["tk"]["runtime_tcl"].split('.')[:2]) < (8,5):
        raise RuntimeError("Actual Tcl runtime is older than 8.5")
    if r["tk"]["runtime_tcl"].split('.')[:2] != str(tk.TclVersion).split('.')[:2]:
        raise RuntimeError("Actual Tcl runtime does not match the compiled Tkinter ABI")
    root=None
    if os.environ.get("DISPLAY"):
        try:
            root=tk.Tk(); root.withdraw(); root.update_idletasks()
            r["tk"]["runtime_tk"]=str(root.tk.call("package","require","Tk"))
            if tuple(int(n) for n in r["tk"]["runtime_tk"].split('.')[:2]) < (8,5):
                raise RuntimeError("Actual Tk runtime is older than 8.5")
            if r["tk"]["runtime_tk"].split('.')[:2] != str(tk.TkVersion).split('.')[:2]:
                raise RuntimeError("Actual Tk runtime does not match the compiled Tkinter ABI")
            r["tk"]["display_status"]="PASS"
        finally:
            if root is not None: root.destroy()
    else:
        r["tk"]["display_status"]="NOT_TESTED"
    r["tk"]["ok"]=True
except Exception as e:
    r["tk"].update(ok=False,error=type(e).__name__+": "+str(e)[:2048])
print(json.dumps(r,sort_keys=True))
'''

_FILESYSTEM_PROBE = r'''
import errno, json, os, stat, sys, tempfile
path=os.path.abspath(sys.argv[1]); existing=path
while not os.path.exists(existing):
    parent=os.path.dirname(existing)
    if parent==existing: raise RuntimeError("No existing filesystem ancestor")
    existing=parent
if not os.path.isdir(existing): existing=os.path.dirname(existing)
info=os.stat(existing); fs=os.statvfs(existing)
r={"requested_path":path,"existing_ancestor":existing,"requested_exists":os.path.isdir(path),
   "uid":info.st_uid,"mode":oct(stat.S_IMODE(info.st_mode)),"device":info.st_dev,
   "readable":os.access(existing,os.R_OK),"searchable":os.access(existing,os.X_OK),
   "available_bytes":fs.f_bavail*fs.f_frsize,"available_inodes":fs.f_favail,
   "quota_status":"NOT_TESTED","write_probe":{},"source_views_untouched":True}
temporary=None
try:
    temporary=tempfile.mkdtemp(prefix=".crescendo-qualification-",dir=existing)
    a=os.path.join(temporary,"a"); b=os.path.join(temporary,"b")
    with open(a,"wb") as stream:
        stream.write(b"Crescendo probe\n"); stream.flush(); os.fsync(stream.fileno())
    os.rename(a,b)
    with open(b,"rb") as stream:
        if stream.read()!=b"Crescendo probe\n": raise RuntimeError("Readback did not match")
    os.unlink(b); os.rmdir(temporary); temporary=None
    r["write_probe"]={"ok":True,"bytes":10,"fsync":True,"rename":True,"readback":True,"cleaned":True}
except Exception as e:
    r["write_probe"]={"ok":False,"error":type(e).__name__+": "+str(e),"errno":getattr(e,"errno",None)}
finally:
    if temporary is not None:
        try:
            for name in ("a","b"):
                candidate=os.path.join(temporary,name)
                if os.path.lexists(candidate): os.unlink(candidate)
            os.rmdir(temporary)
            r["write_probe"]["cleaned"]=True
        except Exception as e:
            r["write_probe"].update(cleaned=False,cleanup_error=str(e),retained_path=temporary)
print(json.dumps(r,sort_keys=True))
'''


def _json_probe(source, args, root, env, cancel, timeout=5.0):
    result = bounded_command([sys.executable, "-B", "-c", source] + list(args),
                             cwd=root, env=env, timeout=timeout, cancel=cancel)
    if _command_status(result) != "PASS":
        return _command_status(result), "Probe process failed or exceeded its bound", result
    try:
        payload = json.loads(result["stdout_tail"])
    except (ValueError, TypeError):
        return "FAIL", "Probe did not publish valid bounded JSON", result
    payload["probe_duration_seconds"] = result["duration_seconds"]
    return "PASS", "Probe completed", payload


def _native_context(config, root):
    from .cadence import child_environment
    root = os.path.abspath(root)
    info = os.lstat(root)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("Environment probe root must be a directory owned by this user")
    home = tempfile.mkdtemp(prefix="environment_probe_", dir=root)
    config = dict(config or {})
    config["cadence"] = dict(config.get("cadence") or {})
    try:
        return home, child_environment(config, home)
    except Exception:
        shutil.rmtree(home)
        raise


def _cleanup_native_home(home):
    entries, size = 0, 0
    for directory, _dirs, files in os.walk(home, followlinks=False):
        entries += 1 + len(files)
        for name in files:
            size += os.lstat(os.path.join(directory, name)).st_size
        if entries > 128 or size > 1024 * 1024:
            return "WARN", "Unexpected probe-home data retained for inspection", {"path": home, "entries": entries, "bytes": size}
    shutil.rmtree(home)
    return "PASS", "Owned environment probe home removed", {"entries": entries, "bytes": size}


def _effective_probe_setup(prepared, spec):
    """Derive requested enabled bits without modifying the prepared baseline."""
    setup = (prepared or {}).get("setup") or {}
    if (spec or {}).get("execution_selection") is None:
        return setup
    from .test_selection import selection_plan, effective_setup
    return effective_setup(setup, selection_plan(setup, spec))


def _selected_probe_tests(prepared, spec):
    setup = _effective_probe_setup(prepared, spec)
    if "tests" not in setup:
        return None  # Older diagnostic snapshots did not capture a Test catalogue.
    return [row.get("name") for row in setup.get("tests") or [] if row.get("enabled", True)]


def _policy_rows(prepared, spec):
    from .job_policy import effective_job_policy, _observable_command_argv
    captured = (prepared or {}).get("job_policy")
    selected = _selected_probe_tests(prepared, spec)
    if captured and selected is not None:
        captured = dict(captured, tests=[row for row in captured.get("tests") or [] if row.get("name") in selected])
    policy = effective_job_policy(captured, (spec or {}).get("maestro_job_overrides"))
    rows = []
    for scope in [dict(policy.get("global") or {}, scope="global")] + [
            dict(test, scope="test") for test in policy.get("tests") or []]:
        props = scope.get("properties") or {}
        row = {"scope": scope["scope"], "test": scope.get("name"),
               "available": scope.get("available"), "distribution": props.get("distributionmethod"),
               "max_jobs": props.get("maxjobs"), "command": props.get("jobsubmitcommand")}
        if str(row["distribution"] or "").lower() == "command":
            try:
                row["submit_argv"] = _observable_command_argv(row["command"] or "")
            except ValueError as error:
                row["argv_observation"] = {"status": "NOT_TESTED", "reason": str(error)}
        rows.append(row)
    return policy, rows


def _scheduler_capability(config, prepared, root, env, cancel, spec=None, capability_cache=None):
    from .scheduler_cli import resolve_scheduler_binary, scheduler_capability
    _policy, rows = _policy_rows(prepared, spec)
    selected = _selected_probe_tests(prepared, spec)
    effective_rows = rows
    if selected is not None:
        global_row = next((row for row in rows if row.get("scope") == "global"), {})
        by_test = {row.get("test"): row for row in rows if row.get("scope") == "test"}
        effective_rows = []
        for name in selected:
            row = dict(global_row)
            row.update(by_test.get(name, {}))
            row["test"] = name
            effective_rows.append(row)
        # A partial per-Test policy inherits properties from Global.
        for row in effective_rows:
            for key in ("distribution", "command", "submit_argv", "max_jobs"):
                if row.get(key) is None and key in global_row:
                    row[key] = global_row[key]
    commands = [row for row in effective_rows if str(row.get("distribution") or "").lower() == "command"]
    from .execution_commands import normalize_execution_commands
    settings = normalize_execution_commands(config, spec)
    template = settings['submission_template']
    if template:
        # Observe the selected complete submission template, not an inherited
        # ADE prefix that the common execution layer no longer uses.
        commands = [{'scope': 'execution_commands', 'test': None,
                     'submit_argv': shlex.split(template)}]
    binaries = []
    for row in commands:
        argv = row.get("submit_argv") or []
        if not argv:
            continue
        if os.path.isabs(argv[0]):
            binary = argv[0] if os.path.isfile(argv[0]) and os.access(argv[0], os.X_OK) else None
        else:
            binary = shutil.which(argv[0], path=env.get('PATH'))
        binaries.append({"submit_argv": argv, "binary": binary,
                         "executable": bool(binary and os.access(binary, os.X_OK)),
                         "test": row.get("test")})
    result = dict(scheduler_capability(settings), submission_not_performed=True,
                  commands=binaries, query_supported=False)
    if not commands:
        return "NOT_TESTED", "Current Job Setup has no Command distribution", result
    if any(not row["executable"] for row in binaries):
        return "FAIL", "The configured submission executable is unavailable in the native child environment", result
    if not result['available']:
        return "FAIL", result['reason'], result
    from .execution_commands import _split_template
    for kind in ("query", "cancel"):
        argv, unused = _split_template(result[kind + "_template"])
        binary = resolve_scheduler_binary(argv[0], environment=env)
        result[kind + "_binary"] = binary
        if not binary:
            return "FAIL", "Configured scheduler " + kind + " executable is unavailable", result
    result["query_supported"] = True
    return "PASS", "User templates select one exact owned Job ID; no job submitted or help command executed", result


def query_scheduler_jobs(config, prepared, root, job_ids, emit=None, cancel=None, spec=None, capability_cache=None):
    """Observe only Job IDs obtained by the caller from this run's owned ledger.

    No all-account query, submission, cancellation or guessed scheduler syntax.
    String IDs or dictionaries with job_id/candidate_id/test/corner are accepted;
    ownership provenance must be established by the engine before calling.
    """
    reporter = _Reporter(emit, cancel, os.environ)
    root = os.path.abspath(root)
    if not job_ids:
        reporter.add("scheduler.jobs", "NOT_TESTED", "No owned submitted Job ID was available; no scheduler listing was issued")
        return reporter.checks
    seen, invalid, bounded = set(), [], []
    from .scheduler_protocol import valid_job_id
    for item in job_ids:
        row = dict(item) if isinstance(item, dict) else {"job_id": str(item)}
        identifier = str(row.get("job_id") or "")
        if not valid_job_id(identifier):
            invalid.append(identifier[:64])
        elif identifier not in seen:
            seen.add(identifier)
            bounded.append(dict((key, row.get(key)) for key in
                                ("job_id", "candidate_id", "test", "corner", "point_id", "task_id", "source", "shared", "scope")))
            bounded[-1]["job_id"] = identifier
    if invalid:
        reporter.add("scheduler.invalid_job_ids", "WARN", "Unsupported Job ID values were not queried", {"invalid": invalid[:32], "count": len(invalid)})
    if len(bounded) > MAX_JOB_QUERIES:
        reporter.add("scheduler.query_capacity", "WARN", "Exact job query capacity exceeded; remaining IDs were not queried",
                     {"limit": MAX_JOB_QUERIES, "total": len(bounded), "deferred": len(bounded) - MAX_JOB_QUERIES})
    if not bounded:
        reporter.add("scheduler.jobs", "NOT_TESTED", "No valid owned numeric Job ID was available; no exact job query was issued")
        return reporter.checks
    home = None
    try:
        home, environment = _native_context(config, root)
        from .native_environment import scheduler_environment
        private_home = environment.get("HOME")
        home_source = "captured_caller_home" if "CRESCENDO_SCHEDULER_HOME_SET" in environment else "legacy_caller_context"
        environment = scheduler_environment(environment)
        reporter.environment = environment
        reporter.add("scheduler.auth_context", "PASS" if environment.get("HOME") else "WARN",
                     "Scheduler uses captured caller authentication context; credentials are not read",
                     {"home_state": "set" if environment.get("HOME") else "unset",
                      "private_home_replaced": environment.get("HOME") != private_home,
                      "source": home_source})
        capability = reporter.check("scheduler.query_capability", lambda: _scheduler_capability(
            config, prepared, root, environment, cancel, spec, capability_cache))
        if capability["status"] != "PASS":
            reporter.add("scheduler.jobs", "NOT_TESTED", "Exact scheduler queries require configured query and cancel templates")
            return reporter.checks
        operations = []
        for row in bounded[:MAX_JOB_QUERIES]:
            def query(row=row):
                from .scheduler_cli import scheduler_command_argv
                from .scheduler_protocol import parse_scheduler_observation
                result = bounded_command(scheduler_command_argv(capability["details"]["query_template"], row["job_id"], dict(row, cwd=root)),
                    cwd=root, env=environment, timeout=3.0, cancel=cancel)
                status = _command_status(result)
                observation = parse_scheduler_observation(
                    result.get("stdout_tail", "") + "\n" + result.get("stderr_tail", ""), row["job_id"])
                query_safe = not any(result.get(key) for key in ('timed_out', 'cancelled', 'termination_failed', 'output_limited', 'error'))
                if status != 'PASS':
                    # A scheduler may age a completed job out and return nonzero.
                    # Keep that observation as unavailable scheduler history;
                    # native receipts/results retain their separate meaning.
                    if (query_safe and observation.get('not_found')
                            and not observation.get('observed_job_ids') and not observation.get('state')):
                        status = 'WARN'
                    else:
                        observation.update(state=None, terminal=False, reason='query did not succeed')
                if observation.get('not_found'):
                    status = 'WARN' if query_safe else status
                if status == "PASS" and observation.get("invalid"):
                    status = "WARN"
                state = observation.get("state") or ("NOT_FOUND" if observation.get("not_found") else "UNKNOWN")
                # A read-only query failure is unavailable evidence, not a job or
                # simulator failure. Preserve its raw result for diagnosis.
                if status == 'FAIL' or (status == 'PASS' and state == 'UNKNOWN'):
                    status = 'WARN'
                message = "Exact owned Job ID query: %s; query/queue state is not simulator completion or Stop proof" % state
                return status, message, dict(row, command=result, query_returncode=result.get("returncode"),
                    scheduler_observation=observation, native_completion_authority=False)
            operations.append(("scheduler.job.%s" % row["job_id"], query))
        _parallel_checks(reporter, operations)
    except Exception as error:
        reporter.add("scheduler.context", "FAIL", "%s: %s" % (type(error).__name__, error))
    finally:
        if home:
            reporter.check("scheduler.probe_home_cleanup", lambda: _cleanup_native_home(home), ignore_cancel=True)
    return reporter.checks


def _run_expanded_environment_probes(config, prepared, root, emit=None, cancel=None, spec=None):
    """Report bounded environment evidence independently of either real engine."""
    config, prepared, spec = config or {}, prepared or {}, spec or {}
    reporter = _Reporter(emit, cancel, os.environ)
    home, native = None, None
    root = os.path.abspath(root)
    reporter.check("host.runtime", lambda: ("PASS", "Current OS/CPU/runtime identity", {
        "os": platform.system(), "release": platform.release(), "machine": platform.machine(),
        "python": platform.python_version(), "python_executable": sys.executable,
        "libc": list(platform.libc_ver()), "cpu_count": os.cpu_count(),
        "uid": os.getuid(), "cwd": os.getcwd(), "probe_root": root}))

    def python_check():
        status, message, details = _json_probe(_PYTHON_PROBE, [], root, dict(os.environ), cancel, 8.0)
        if status == "PASS":
            failures = [name for name, row in (details.get("imports") or {}).items() if not row.get("ok")]
            optional = ("sqlite3", "ssl", "ctypes")
            required_failures = [name for name in failures if name not in optional]
            details["optional_extension_failures"] = [name for name in failures if name in optional]
            if sys.version_info < (3, 6) or required_failures or not (details.get("tk") or {}).get("ok"):
                status, message = "FAIL", "Current Python or required stdlib/Tcl/Tk check failed"
            elif failures:
                status, message = "WARN", "Optional stdlib extensions are unavailable; current Crescendo engine dependencies passed"
            elif (details.get("tk") or {}).get("display_status") == "NOT_TESTED":
                status, message = "WARN", "Python and Tcl succeeded; a graphical Tk display was unavailable"
        return status, message, details
    operations = [("python.dependencies_and_tk", python_check)]

    def resources_check():
        from .resources import cadence_resource_path, pdk_adapter_path, tool_path
        requested = [(tool_path, name) for name in ("ocean_worker_guardian.py", "maestro_job_observer.py", "submit_spectre_job.sh")]
        requested += [(cadence_resource_path, name) for name in ("crescendo_launch", "crescendo_maestro_menu.il")]
        requested += [(pdk_adapter_path, "cadence_pas_transaction.il")]
        rows = []
        for resolver, name in requested:
            try:
                path = resolver(name)
                mode = stat.S_IMODE(os.stat(path).st_mode)
                rows.append({"name": name, "path": path, "mode": oct(mode), "readable": os.access(path, os.R_OK),
                             "executable": os.access(path, os.X_OK), "bytes": os.path.getsize(path)})
            except Exception as error:
                rows.append({"name": name, "error": str(error), "readable": False})
        failure = any(not row["readable"] or (row["name"] in ("crescendo_launch", "submit_spectre_job.sh") and not row.get("executable")) for row in rows)
        return "FAIL" if failure else "PASS", "Packaged native/guardian/callback resources and execution permissions", {"resources": rows}
    reporter.check("package.resources", resources_check)

    def native_check():
        nonlocal home, native
        from .native_environment import GUI_ENV_NAMES, NATIVE_ENV_NAMES
        home, native = _native_context(config, root)
        reporter.environment = native
        snapshot = (config.get("cadence") or {}).get("launch_environment")
        states = []
        for name in NATIVE_ENV_NAMES:
            state = "unset" if name not in native else ("empty" if native[name] == "" else "set")
            expected = snapshot.get(name) if isinstance(snapshot, dict) and name in snapshot else None
            exact = ((name not in native) if expected is None else native.get(name) == expected) if isinstance(snapshot, dict) and name in snapshot else None
            states.append({"name": name, "native_state": state, "captured": isinstance(snapshot, dict) and name in snapshot,
                           "matches_capture_before_explicit_runtime_paths": exact if name not in ("PATH", "LD_LIBRARY_PATH", "CDS_LIC_FILE", "LM_LICENSE_FILE") else None,
                           "differs_from_gui": native.get(name) != os.environ.get(name)})
        wrong = [row["name"] for row in states if row["name"] in GUI_ENV_NAMES and row["matches_capture_before_explicit_runtime_paths"] is False]
        status = "FAIL" if wrong else ("PASS" if isinstance(snapshot, dict) and set(snapshot) == set(NATIVE_ENV_NAMES) else "WARN")
        return status, "Native child uses captured caller states and configured EDA paths; environment/license values are redacted", {
            "variables": states, "complete_caller_snapshot": isinstance(snapshot, dict) and set(snapshot) == set(NATIVE_ENV_NAMES),
            "explicit_binary_path_count": len((config.get("cadence") or {}).get("extra_binary_paths") or []),
            "explicit_library_path_count": len((config.get("cadence") or {}).get("extra_library_paths") or [])}
    reporter.check("native.environment_isolation", native_check)

    cadence = config.get("cadence") or {}
    solver_name = "alps_bin" if (spec.get("simulator") or cadence.get("simulator")) == "alps" else "spectre_bin"
    native_names = ("virtuoso_bin", "ocean_bin", solver_name)
    for name in native_names:
        def version_check(name=name):
            binary = cadence.get(name)
            if not native:
                return "NOT_TESTED", "Native child environment could not be established", {}
            if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
                return "FAIL", "Configured native executable is missing or not executable", {"binary": binary}
            if name == "alps_bin":
                return "NOT_TESTED", "ALPS executable present; actual selected solver run verifies its native interface", {"binary": binary, "scope": "selected_solver_executable"}
            result = bounded_command([binary, "-W"], cwd=root, env=native, timeout=5.0, cancel=cancel)
            # -W is the existing Cadence discover_eda_environment contract. No
            # speculative alternative flags are attempted after an error.
            result["option_source"] = "existing_cadence_version_probe_-W"
            return _command_status(result), "Native lightweight version invocation", result
        operations.append(("native.version.%s" % name, version_check))

    try:
        extension_spec = importlib.util.find_spec("_tkinter")
        extension = extension_spec.origin if extension_spec else None
    except (ImportError, AttributeError, ValueError):
        extension = None
    for name, target, environment in [("python", sys.executable, os.environ), ("tkinter", extension, os.environ)] + [
            (name, cadence.get(name), native) for name in native_names]:
        def dependencies_check(name=name, target=target, environment=environment):
            if not target or environment is None:
                return "NOT_TESTED", "Dependency target/environment unavailable", {"target": target}
            with open(os.path.realpath(target), "rb") as stream:
                is_elf = stream.read(4) == b"\x7fELF"
            if not is_elf:
                return "NOT_TESTED", "Selected executable is a vendor launcher, not an ELF target; the native version/run will verify its implementation", {"target": target}
            binary = shutil.which("ldd", path=environment.get("PATH"))
            if not binary:
                return "NOT_TESTED", "ldd is unavailable", {"target": target}
            result = bounded_command([binary, os.path.realpath(target)], cwd=root, env=environment,
                                     timeout=3.0, cancel=cancel)
            text = result.get("stdout_tail", "") + result.get("stderr_tail", "")
            missing = re.findall(r"(?m)^\s*(\S+)\s+=>\s+not found", text)
            result.update(target=target, missing_sonames=missing)
            status = "FAIL" if missing else _command_status(result)
            return status, "Dynamic dependency resolution in the correct Python/native child environment", result
        operations.append(("dependencies.%s" % name, dependencies_check))

    storage = None
    def storage_check():
        nonlocal storage
        from .storage_policy import resolve_storage_policy, validate_native_data_path
        storage = resolve_storage_policy(config, prepared.get("setup") or {})
        details = dict((key, storage.get(key)) for key in
                       ("work_dir", "design_root", "states_root", "cache_root", "data_root", "run_root", "source", "native_original_available", "warnings"))
        for name in ("data_root", "run_root"):
            validate_native_data_path(storage.get(name))
        return ("WARN" if storage.get("source") == "working_directory_fallback" else "PASS"), "Resolved working/state/cache/simulation hierarchy", details
    reporter.check("storage.policy", storage_check)
    paths = []
    if storage:
        paths += [("work", storage.get("work_dir")), ("state", storage.get("states_root")),
                  ("simulation", storage.get("data_root")), ("run", storage.get("run_root"))]
    local_base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    paths.append(("local_cache", os.path.join(local_base, "crescendo")))
    for name, path in paths:
        def filesystem_check(name=name, path=path):
            if not path:
                return "NOT_TESTED", "Storage target unavailable", {}
            status, message, details = _json_probe(_FILESYSTEM_PROBE, [path], root, dict(os.environ), cancel, 3.0)
            if status == "PASS" and not (details.get("write_probe") or {}).get("ok"):
                status, message = "FAIL", "Owned tiny write/fsync/rename/readback test failed"
            if status == "PASS" and details.get("available_bytes", 0) < 16 * 1024 * 1024:
                status, message = "WARN", "Write succeeded but available filesystem space is below 16 MiB"
            details["quota_note"] = "statvfs does not prove user quota headroom; the owned write probe exposes current EDQUOT/ENOSPC only"
            return status, message, details
        operations.append(("storage.filesystem.%s" % name, filesystem_check))

    def setup_check():
        setup = prepared.get("setup") or {}
        source = setup.get("source") or {}
        locator = setup.get("setup_locator") or {}
        invocation = config.get("_selected_setup_source") or (config.get("_maestro_launch_context") or {}).get("source") or {}
        effective_setup = _effective_probe_setup(prepared, spec)
        tests = effective_setup.get("tests") or []
        identity = {"library": invocation.get("library") or source.get("library") or locator.get("library_name"),
                    "cell": invocation.get("cell") or source.get("cell") or locator.get("cell_name"),
                    "view": invocation.get("view") or source.get("view") or locator.get("view_name"),
                    "setup_path": invocation.get("setup_path") or source.get("maestro_sdb") or source.get("setup_db") or locator.get("path"),
                    "work_dir": (storage or {}).get("work_dir"), "cds_lib": cadence.get("cds_lib")}
        return ("PASS" if setup and prepared.get("source_identity") else "WARN"), "Inspected setup identity and effective test/corner metadata; engine must verify the private readback", {
            "prepared_schema": prepared.get("schema_version"), "setup_source": identity,
            "source_enabled_tests": [row.get("name") for row in setup.get("tests") or [] if row.get("enabled", True)],
            "setup_backend": setup.get("backend"), "tests": [{"name": row.get("name"), "enabled": row.get("enabled"),
                "design": row.get("design") or dict((key, row.get(key)) for key in ("library", "cell", "view")),
                "simulator": row.get("simulator"), "analyses": row.get("analyses") or [],
                "variable_count": len(row.get("variables") or []),
                "corner_count": len(row.get("corners") or []),
                "corners": row.get("corners") or []} for row in tests if isinstance(row, dict)],
            "corner_count": len(setup.get("corners") or []), "corners": setup.get("corners") or [],
            "outputs": len(prepared.get("outputs") or []),
            "identity_recorded": bool(prepared.get("source_identity"))}
    reporter.check("setup.snapshot", setup_check)

    def pdk_check():
        parameters = prepared.get("parameters") or []
        requested = set(row.get("parameter_id") or row.get("id") for row in spec.get("variables") or [] if isinstance(row, dict))
        selected = [row for row in parameters if not requested or row.get("parameter_id") in requested]
        rows = []
        for row in selected:
            callback = row.get("callback")
            rows.append({"parameter_id": row.get("parameter_id"), "name": row.get("name"),
                "owner_library": row.get("owner_library"), "owner_cell": row.get("owner_cell"),
                "hierarchy_path": row.get("hierarchy_path"), "source": row.get("source"),
                "semantic": row.get("semantic"), "unit": row.get("unit"),
                "current_value": row.get("current_value"), "numeric_value": row.get("numeric_value"),
                "design_library": row.get("design_library"), "design_cell": row.get("design_cell"),
                "owner_view": row.get("owner_view"), "cdf_type": row.get("cdf_type"),
                "editable_status": row.get("editable_status"), "display_status": row.get("display_status"),
                "callback_metadata_available": "callback" in row,
                "callback_present": bool(callback) if "callback" in row else None,
                "minimum": row.get("minimum"), "maximum": row.get("maximum"), "step": row.get("step"),
                "callback_execution": "NOT_TESTED", "callback_not_run_on_source": True})
        context = (prepared.get("eda_context") or {}).get("pdk") or {}
        return ("PASS" if rows else "NOT_TESTED"), "Selected CDF/PDK callback and dimension metadata; actual callback semantics are exercised only on engine-owned views", {
            "parameters": rows, "selected_count": len(rows), "catalog_count": len(parameters),
            "pdk_schema": context.get("schema_version"), "blocking": context.get("blocking"),
            "source_cds_lib": context.get("source_cds_lib"),
            "contexts": [dict((key, item.get(key)) for key in ("id", "name", "profile_id", "package_root", "reference_libraries", "technology_libraries", "model_roots", "model_path_remaps"))
                         for item in context.get("contexts") or [] if isinstance(item, dict)],
            "profiles": [{"id": item.get("id"), "name": item.get("name")} for item in context.get("profiles") or [] if isinstance(item, dict)]}
    reporter.check("pdk.cdf_metadata", pdk_check)

    def policy_check():
        policy, rows = _policy_rows(prepared, spec)
        return ("PASS" if policy.get("available") else "WARN"), "Current captured/overridden Job Setup; no scheduler command was executed by this probe", {
            "source": policy.get("source"), "available": policy.get("available"), "overrides": policy.get("overrides"),
            "run_options": policy.get("run_options"), "policies": rows, "warnings": policy.get("warnings") or []}
    reporter.check("scheduler.current_policy", policy_check)
    if native:
        operations.append(("scheduler.cli_capability", lambda: _scheduler_capability(config, prepared, root, native, cancel, spec)))
    else:
        reporter.add("scheduler.cli_capability", "NOT_TESTED", "Native environment unavailable")
    # Tcl/Tk, version, ldd, storage writes and CLI help occupy independent slots.
    _parallel_checks(reporter, operations)
    if home:
        reporter.check("native.probe_home_cleanup", lambda: _cleanup_native_home(home), ignore_cancel=True)
    return reporter.checks



def _runtime_tool_summary(cadence):
    """Small allowlist of cached path discovery; never inspect tools or env."""
    truncated = []
    def literal(value, field, limit=128):
        if value is None or not isinstance(value, str):
            return None
        raw = value.encode("utf-8")
        if len(raw) > limit:
            truncated.append(field)
            return raw[:limit].decode("utf-8", "ignore") + "...[truncated]"
        return value
    tools = {key: literal(cadence.get(key), key, 4096) for key in
             ("ocean_bin", "spectre_bin", "alps_bin", "spectre_model_dir", "alps_model_dir")}
    discovery = cadence.get("discovery") or {}
    discovery = discovery if isinstance(discovery, dict) else {}
    effective, sources = discovery.get("effective") or {}, discovery.get("sources") or {}
    effective = effective if isinstance(effective, dict) else {}
    sources = sources if isinstance(sources, dict) else {}
    checks = discovery.get("checks") or []
    check = next((row for row in checks if isinstance(row, dict) and row.get("name") == "alps_bin"), {}) if isinstance(checks, list) else {}
    resolution = check.get("resolution") or {}
    resolution = resolution if isinstance(resolution, dict) else {}
    candidates = resolution.get("candidates") or []
    candidates = candidates if isinstance(candidates, list) else []
    alps = {key: literal(effective.get(key), key) for key in ("alps_root", "alps_bin")}
    alps.update(root_source=literal(sources.get("alps_root"), "root_source", 64),
                binary_source=literal(sources.get("alps_bin"), "binary_source", 64),
                resolution_status=literal(resolution.get("status"), "resolution_status", 48),
                resolution_message=literal(resolution.get("message"), "resolution_message", 128),
                candidates=[literal(value, "candidate_%d" % index) for index, value in enumerate(candidates[:4])],
                candidate_count=len(candidates), omitted_candidate_count=max(0, len(candidates)-4),
                truncated_fields=truncated)
    return {"tools": tools, "alps_discovery": alps}

def run_environment_probes(config, prepared, root, emit=None, cancel=None, spec=None):
    """One current-session summary; real engines perform dependency validation.

    No old-machine cache is consulted. Host/Tk/version/ldd/tiny-write/snapshot
    batteries are intentionally excluded from this user-facing entry.
    """
    config, prepared, spec = config or {}, prepared or {}, spec or {}
    reporter = _Reporter(emit, cancel, os.environ)
    from .environment_diagnostics import package_identity
    source = (prepared.get("setup") or {}).get("source") or {}
    cadence = config.get("cadence") or {}
    locator = (prepared.get("setup") or {}).get("setup_locator") or {}
    setup_summary = dict((name, source.get(name)) for name in ("library", "cell", "view", "setup_db", "maestro_sdb"))
    # SetupDB source stores file identity; its locator stores the L/C/V names.
    # Do not infer names from an unrelated history or another window.
    for name in ("library", "cell", "view"):
        setup_summary[name] = setup_summary[name] or locator.get(name + "_name")
    effective_setup = _effective_probe_setup(prepared, spec)
    _policy, policies = _policy_rows(prepared, spec)
    tool_summary = _runtime_tool_summary(cadence)
    reporter.check("runtime.summary", lambda: ("PASS", "Current running application and fresh test scope", {
        "python_executable": sys.executable, "python_version": list(sys.version_info[:3]),
        "gui_runtime": spec.get("gui_runtime"), "package": spec.get("_diagnostic_package_identity") or package_identity(),
        "tools": tool_summary["tools"], "alps_discovery": tool_summary["alps_discovery"],
        "setup": setup_summary,
        "enabled_tests": [row.get("name") for row in effective_setup.get("tests") or [] if row.get("enabled", True)],
        "enabled_analyses": {row.get("name"): [analysis.get("name") for analysis in row.get("analyses") or []
                              if analysis.get("enabled", True)] for row in effective_setup.get("tests") or [] if row.get("enabled", True)},
        "source_enabled_tests": [row.get("name") for row in (prepared.get("setup") or {}).get("tests") or [] if row.get("enabled", True)],
        "selected_measurements": {kind: [row.get("output_key") for row in spec.get(kind) or []] for kind in ("objectives", "constraints")},
        "job_policies": policies,
        "basis": "application is already running; no prior result replaces this new run",
        "execution_protocol": ["crescendo.maestro_baseline.v1", "crescendo.execution_pipeline.v1"],
        "netlist_sources": ["maestro_baseline", "private_netlist", "maestro_netlist"],
        "selected_simulator": spec.get("simulator") or cadence.get("simulator") or "spectre",
        "execution_commands": spec.get("execution_commands") or {},
        "command_scheduler": spec.get("command_scheduler", (config.get("runtime") or {}).get("command_scheduler", "auto")),
        "phase_timeouts": dict((row.get("name") or "global", dict((key, value)
                               for key, value in (row.get("properties") or {}).items()
                               if key in ("starttimeout", "configuretimeout", "configuredtimeout", "runtimeout", "lingertimeout")))
                               for row in [_policy.get("global") or {}] + (_policy.get("tests") or [])),
        "scope": "three ordered stages: native baseline then private netlist CLI then per-candidate native-generated netlist CLI; one frozen candidate, requested tests/analyses/corners/sweeps; independent stages may continue after failure; source identity unchanged"}))
    return reporter.checks


def failure_environment_details(config, root, error_text, cancel=None):
    """Inspect only the environment layer implicated by an actual failure."""
    config, text = config or {}, str(error_text or '')
    result = {}
    if re.search(r'ENOSPC|EDQUOT|no space left|quota exceeded|read.only file system', text, re.I):
        path = os.path.abspath(root)
        try:
            data = os.statvfs(path)
            result['storage_failure'] = {'path': path, 'available_bytes': data.f_bavail * data.f_frsize,
                'available_inodes': data.f_favail, 'scope': 'failure target only; statvfs does not prove quota allowance'}
        except OSError as error:
            result['storage_failure'] = {'status': 'UNREADABLE', 'reason': str(error)}
    if not re.search(r'error while loading shared libraries|cannot open shared object|'
                     r'GLIBC_[0-9.]+[^\n]*not found|undefined symbol', text, re.I):
        return result
    cadence = config.get('cadence') or {}
    # The failed invocation names its tool; no broad host/dependency battery.
    error_lines = '\n'.join(line for line in text.splitlines() if re.search(
        r'error while loading shared libraries|cannot open shared object|undefined symbol|GLIBC_', line, re.I))
    name = ('alps_bin' if re.search(r'(?:^|[\s/])alps(?:[:\s]|$)', error_lines, re.I) else
            'spectre_bin' if re.search(r'(?:^|[\s/])spectre(?:[:\s]|$)', error_lines, re.I) else 'ocean_bin')
    binary = cadence.get(name)
    if name == 'alps_bin' and not binary and cadence.get('alps_root'):
        binary = os.path.join(cadence['alps_root'], 'bin', 'alps')
    home = None
    observation = {'tool': name, 'binary': binary, 'status': 'NOT_TESTED'}
    try:
        if not binary or not os.path.isfile(binary) or not os.access(binary, os.X_OK):
            observation.update(status='FAIL', reason='failed native executable missing or not executable')
        else:
            with open(binary, 'rb') as stream:
                elf = stream.read(4) == b'\x7fELF'
            if not elf:
                observation['reason'] = 'vendor launcher is not ELF; retained actual launch error identifies the failing implementation'
            elif not _cancelled(cancel):
                home, environment = _native_context(config, root)
                ldd = shutil.which('ldd', path=environment.get('PATH'))
                if ldd:
                    command = bounded_command([ldd, binary], cwd=root, env=environment, timeout=2, cancel=cancel)
                    observation.update(status=_command_status(command), command=_redact(command, environment))
                else:
                    observation['reason'] = 'ldd unavailable; no dependency command was guessed'
            else:
                observation['reason'] = 'cancelled; no new dependency process started'
    except (OSError, ValueError) as error:
        observation.update(status='FAIL', reason=str(error))
    finally:
        if home:
            observation['probe_cleanup'] = _cleanup_native_home(home)[0]
    result['native_loader_failure'] = observation
    return result
