from __future__ import print_function

import os
import copy
import contextlib
import glob
import re
import signal
import shutil
import subprocess
import sys
import threading
import time

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .pdk import (
    GENERIC_ADAPTER_ID, adapter_identity, build_adapter_evidence,
    discover_pdk_contexts, hierarchy_library_references, normalize_adapter_policy,
    normalize_pdk_profiles, select_adapter,
)
from .resources import guardian_script_path
from .native_environment import GUI_ENV_NAMES, preserve_scheduler_home, restore_native_environment

from .legacy_guardian import LAUNCH_TOKEN_ENVIRONMENT, inspect_legacy_guardian_attempt
from .utils import atomic_write_text, cds_lib_token, ensure_dir, utc_timestamp


CADENCE_BINARY_RELATIVES = {
    "virtuoso_bin": ["bin/virtuoso", "tools.lnx86/dfII/bin/virtuoso", "tools.lnx86/dfII/bin/64bit/virtuoso", "tools/dfII/bin/64bit/virtuoso"],
    "ocean_bin": ["bin/ocean", "tools.lnx86/dfII/bin/ocean", "tools.lnx86/dfII/bin/64bit/ocean", "tools/dfII/bin/64bit/ocean"],
}


SPECTRE_BINARY_RELATIVES = [
    "bin/spectre",
    "tools.lnx86/bin/spectre",
    "tools.lnx86/spectre/bin/spectre",
    "tools.lnx86/spectre/bin/64bit/spectre",
]


CADENCE_LIBRARY_RELATIVES = [
    "tools.lnx86/lib/64bit",
    "tools.lnx86/lib/64bit/SuSE/SLES12",
    "tools.lnx86/inca/lib/64bit",
    "tools.lnx86/Qt/v5/64bit/lib",
    "tools.lnx86/sev/lib/64bit",
    "tools.lnx86/hdf5/lib/64bit",
]


SPECTRE_LIBRARY_RELATIVES = [
    "tools.lnx86/lib/64bit",
    "tools.lnx86/inca/lib/64bit",
    "tools.lnx86/SobolTable/lib/64bit",
    "tools.lnx86/nif/lib/64bit",
    "tools.lnx86/jsoncpp/lib/64bit",
    "tools.lnx86/tcltk-8.4.20/lib/64bit",
    "tools.lnx86/ktl/lib/64bit",
    "tools.lnx86/cdsgcc/gcc/6.3/install/lib64",
]


GUARDIAN_CHILD_ENVIRONMENT_KEYS = GUI_ENV_NAMES
GUARDIAN_CHILD_ENVIRONMENT_PREFIX = _contract_text("CRESCENDO_GUARDIAN_CHILD_")


class CommandCancelled(RuntimeError):
    """A cancelled read-only prepare command, with its owned cleanup evidence."""

    def __init__(self, message="ADE setup loading cancelled by Exit", result=None):
        RuntimeError.__init__(self, message)
        self.result = result
        self.cleanup_failed = bool((result or {}).get("termination_failed"))


_COMMAND_CANCELLATION = threading.local()


def _command_cancellation_events():
    return tuple(getattr(_COMMAND_CANCELLATION, "events", ()))


def _command_cancellation_requested(events=None):
    return any(event.is_set() for event in (
        _command_cancellation_events() if events is None else events
    ))


def check_command_cancellation():
    """Cooperative cancellation for Python work in the current prepare thread."""
    if _command_cancellation_requested():
        raise CommandCancelled()


@contextlib.contextmanager
def command_cancellation_scope(event):
    """Attach an Event to this thread, never to persistent/deepcopied config."""
    previous = _command_cancellation_events()
    _COMMAND_CANCELLATION.events = previous + (event,)
    try:
        check_command_cancellation()
        yield
        check_command_cancellation()
    finally:
        _COMMAND_CANCELLATION.events = previous


def guardian_environment(child_environment):
    environment = dict(child_environment or {})
    for name in GUARDIAN_CHILD_ENVIRONMENT_KEYS:
        bridge = GUARDIAN_CHILD_ENVIRONMENT_PREFIX + name
        environment.pop(bridge, None)
        if name in environment:
            environment[bridge] = environment.pop(name)
    return environment


def _prepend(existing, values):
    rows = []
    seen = set()
    for value in list(values) + (existing or "").split(":"):
        if value and value not in seen:
            rows.append(value)
            seen.add(value)
    return ":".join(rows)


def _expand(value, environ=None):
    if not value:
        return None
    if environ is None:
        expanded = os.path.expandvars(os.path.expanduser(value))
    else:
        from .native_includes import expand_native_path
        expanded = expand_native_path(value, environ, reject_unset=False)
    return os.path.realpath(os.path.abspath(expanded))


def _executable(value, environ=None):
    if environ is None:
        expanded = os.path.expandvars(os.path.expanduser(value)) if value else None
    else:
        from .native_includes import expand_native_path
        expanded = expand_native_path(value, environ, reject_unset=False) if value else None
    path = os.path.abspath(expanded) if expanded else None
    return path if path and os.path.isfile(path) and os.access(path, os.X_OK) else None


def _first_existing(root, relatives, executable=False):
    if not root:
        return None
    for relative in relatives:
        candidate = os.path.join(root, relative)
        if executable:
            candidate = _executable(candidate)
        elif os.path.isfile(candidate):
            candidate = os.path.realpath(candidate)
        else:
            candidate = None
        if candidate:
            return candidate
    return None


def _bounded_install_roots(search_roots, product_relatives):
    roots = []
    for search_root in search_roots or []:
        base = _expand(search_root)
        if not base or not os.path.isdir(base):
            continue
        candidates = [base]
        try:
            candidates.extend(os.path.join(base, name) for name in os.listdir(base))
        except OSError:
            pass
        for candidate in candidates:
            if os.path.isdir(candidate) and _first_existing(candidate, product_relatives, executable=True):
                roots.append(candidate)
    return list(dict.fromkeys(roots))


def _alps_binary_candidate(path):
    if not os.path.lexists(path):
        state = 'missing'
    elif not os.path.isfile(path):
        state = 'not_a_regular_file'
    elif not os.access(path, os.X_OK):
        state = 'not_executable'
    else:
        state = 'executable_file'
    return {'path': path, 'status': state}


def _resolve_alps_installation(roots, origin, limit=128, environ=None):
    """Find a launcher only within declared roots and one version-child level.

    Direct bin/alps (or an explicitly selected bin directory) wins within its
    own installation. Multiple discovered versions require an explicit choice.
    No executable is run and an invalid declared binary never enters this path.
    """
    roots = [_expand(root, environ) for root in roots]
    evidence = {'origin': origin, 'search_roots': list(roots), 'candidates': [],
                'rejected': [], 'directory_limit': limit, 'search_incomplete': False}
    found, seen, inspected = [], set(), 0
    for root in roots:
        if not root or not os.path.isdir(root):
            if root and len(evidence['rejected']) < 32:
                evidence['rejected'].append({'path': root, 'status': 'not_a_directory'})
            continue
        direct_found, direct_invalid = False, False
        for relative in ('bin/alps', 'alps'):
            item = _alps_binary_candidate(os.path.join(root, relative))
            if item['status'] == 'executable_file':
                found.append(item['path'])
                direct_found = True
                break
            if item['status'] != 'missing':
                direct_invalid = True
            if len(evidence['rejected']) < 32:
                evidence['rejected'].append(item)
        if direct_found:
            continue
        # ALPS_HOME can name the vendor's internal tools/alps home rather
        # than the version root. This exact, real suffix authorizes only the
        # same version's ../../bin/alps; never search ancestor installations.
        if os.path.basename(root) == 'alps' and os.path.basename(os.path.dirname(root)) == 'tools':
            version_root = os.path.dirname(os.path.dirname(root))
            item = _alps_binary_candidate(os.path.join(version_root, 'bin', 'alps'))
            if item['status'] == 'executable_file':
                found.append(item['path'])
            elif len(evidence['rejected']) < 32:
                evidence['rejected'].append(item)
            continue
        if direct_invalid:
            continue
        try:
            children = sorted(os.listdir(root))
        except OSError as error:
            evidence['search_incomplete'] = True
            evidence['rejected'].append({'path': root, 'status': 'unreadable_directory', 'reason': str(error)})
            continue
        for name in children:
            path = os.path.join(root, name)
            if not os.path.isdir(path):
                continue
            inspected += 1
            if inspected > limit:
                evidence['search_incomplete'] = True
                break
            # A version alias inside this root may refer to the same candidate;
            # do not scan a directory link leading to another installation.
            if os.path.commonpath((os.path.realpath(root), os.path.realpath(path))) != os.path.realpath(root):
                continue
            for relative in ('bin/alps', 'alps'):
                item = _alps_binary_candidate(os.path.join(path, relative))
                if item['status'] == 'executable_file':
                    found.append(item['path'])
                    break
                if item['status'] != 'missing' and len(evidence['rejected']) < 32:
                    evidence['rejected'].append(item)
    for path in found:
        identity = os.path.realpath(path)
        if identity not in seen:
            seen.add(identity)
            evidence['candidates'].append(path)
    candidates = evidence['candidates']
    if len(candidates) == 1 and not evidence['search_incomplete']:
        binary = candidates[0]
        parent = os.path.dirname(binary)
        selected_root = os.path.dirname(parent) if os.path.basename(parent) == 'bin' else parent
        evidence.update(status='selected', selected=binary, selected_root=selected_root,
                        message='Unique executable ALPS launcher located within the selected installation scope')
        return selected_root, binary, evidence
    status = 'incomplete' if evidence['search_incomplete'] else 'ambiguous' if len(candidates) > 1 else 'unavailable'
    message = ('Multiple ALPS installations are available; choose a version root or executable: ' + ', '.join(candidates)
               if status == 'ambiguous' else
               'ALPS installation search was incomplete; select an exact version root or executable' if status == 'incomplete' else
               'No executable ALPS launcher found in the selected scope; ' + '; '.join(
                   '%s (%s)' % (row['path'], row['status']) for row in evidence['rejected'][:4]))
    evidence.update(status=status, message=message)
    return None, None, evidence


def _root_from_binary(binary, marker_relatives):
    current = os.path.dirname(_expand(binary)) if binary else None
    for unused in range(10):
        if not current:
            break
        if any(os.path.exists(os.path.join(current, marker)) for marker in marker_relatives):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return None


def _runtime_paths(cadence_root, spectre_root, binaries, extra_binary_paths=None, extra_library_paths=None):
    binary_paths = list(extra_binary_paths or [])
    library_paths = list(extra_library_paths or [])
    for binary in binaries:
        if binary:
            binary_paths.append(os.path.dirname(binary))
    if cadence_root:
        binary_paths.extend(os.path.join(cadence_root, path) for path in ("bin", "tools.lnx86/dfII/bin", "tools.lnx86/dfII/bin/64bit"))
        library_paths.extend(os.path.join(cadence_root, path) for path in CADENCE_LIBRARY_RELATIVES)
        library_paths.extend(glob.glob(os.path.join(cadence_root, "oa*", "lib", "linux*_64", "opt")))
    if spectre_root:
        binary_paths.extend(os.path.join(spectre_root, path) for path in ("bin", "tools.lnx86/bin", "tools.lnx86/spectre/bin", "tools.lnx86/spectre/bin/64bit"))
        library_paths.extend(os.path.join(spectre_root, path) for path in SPECTRE_LIBRARY_RELATIVES)
    return {
        "binary_paths": list(dict.fromkeys(_expand(path) for path in binary_paths if path and os.path.isdir(_expand(path)))),
        "library_paths": list(dict.fromkeys(_expand(path) for path in library_paths if path and os.path.isdir(_expand(path)))),
    }


def _license_valid(value):
    if not value:
        return False
    for item in str(value).split(":"):
        if re.match(r"^\d+@[^@\s:]+$", item):
            return True
        if os.path.isfile(_expand(item)):
            return True
    return False


def _version_probe(name, binary, env, timeout_seconds=15):
    if not binary:
        return {"name": name, "ok": False, "binary": None, "message": "executable not found"}
    argv = [binary, "-W"]
    try:
        # Version wrappers can ignore TERM or leave a child holding their pipes.
        # Reuse the command runner's proven process-group ownership and bounded
        # cleanup instead of waiting forever after the initial probe timeout.
        command = run_command(argv, cwd=os.getcwd(), env=env,
                              timeout_seconds=timeout_seconds,
                              termination_grace_seconds=1, termination_kill_seconds=1)
        if command.get("timed_out"):
            return {"name": name, "ok": False, "binary": binary, "argv": argv,
                    "message": "version check timed out", "timed_out": True,
                    "termination_failed": bool(command.get("termination_failed")),
                    "elapsed_seconds": command.get("elapsed_seconds")}
        output = (command.get("stdout_tail", "") + command.get("stderr_tail", "")).strip()
        returncode = command["returncode"]
        return {
            "name": name,
            "ok": returncode == 0,
            "binary": binary,
            "argv": argv,
            "returncode": returncode,
            "version": output[-1200:],
            "message": "version check passed" if returncode == 0 else "version check returned %s" % returncode,
        }
    except OSError as error:
        return {"name": name, "ok": False, "binary": binary, "argv": argv, "message": "%s: %s" % (error.__class__.__name__, error)}


def discover_eda_environment(overrides=None, validate=True, environ=None):
    """Bounded EDA discovery. It never scans the full filesystem or changes the parent shell."""
    overrides = dict(overrides or {})
    environ = dict(os.environ if environ is None else environ)
    sources = {}

    def choose(key, env_names, command=None):
        value = overrides.get(key)
        if value:
            sources[key] = "manual"
            return _executable(value, environ) if key.endswith("_bin") else _expand(value, environ)
        for env_name in env_names:
            if environ.get(env_name):
                sources[key] = "environment:%s" % env_name
                return _executable(environ[env_name], environ) if key.endswith("_bin") else _expand(environ[env_name], environ)
        if command:
            found = shutil.which(command, path=environ.get("PATH", os.defpath))
            if found:
                sources[key] = "PATH"
                return _executable(found)
        return None

    cadence_root = choose("cadence_root", (_contract_text("CRESCENDO_CADENCE_ROOT"), "CDSHOME"))
    spectre_root = choose("spectre_root", (_contract_text("CRESCENDO_SPECTRE_ROOT"), "MMSIMHOME"))
    cadence_root_explicit = bool(overrides.get("cadence_root") or environ.get(_contract_text("CRESCENDO_CADENCE_ROOT")) or environ.get("CDSHOME"))
    spectre_root_explicit = bool(overrides.get("spectre_root") or environ.get(_contract_text("CRESCENDO_SPECTRE_ROOT")) or environ.get("MMSIMHOME"))
    virtuoso = choose("virtuoso_bin", (_contract_text("CRESCENDO_VIRTUOSO_BIN"),), None if cadence_root_explicit else "virtuoso")
    ocean = choose("ocean_bin", (_contract_text("CRESCENDO_OCEAN_BIN"),), None if cadence_root_explicit else "ocean")
    spectre = choose("spectre_bin", (_contract_text("CRESCENDO_SPECTRE_BIN"),), None if spectre_root_explicit else "spectre")
    explicit_binaries = dict((key, bool(overrides.get(key) or environ.get(env_name)))
                             for key, env_name in (("virtuoso_bin", _contract_text("CRESCENDO_VIRTUOSO_BIN")),
                                                   ("ocean_bin", _contract_text("CRESCENDO_OCEAN_BIN")),
                                                   ("spectre_bin", _contract_text("CRESCENDO_SPECTRE_BIN"))))
    simulator = str(overrides.get("simulator") or "spectre").strip().lower()
    if simulator not in ("spectre", "alps"):
        raise ValueError("Simulator must be spectre or alps")
    alps_explicit = bool(overrides.get("alps_bin") or environ.get(_contract_text("CRESCENDO_ALPS_BIN")))
    alps_root_explicit = bool(overrides.get("alps_root") or environ.get(_contract_text("CRESCENDO_ALPS_ROOT")) or environ.get("ALPS_HOME"))
    alps_root = choose("alps_root", (_contract_text("CRESCENDO_ALPS_ROOT"), "ALPS_HOME"))
    alps = choose("alps_bin", (_contract_text("CRESCENDO_ALPS_BIN"),), None if alps_root_explicit else "alps")
    alps_search_roots = overrides.get("alps_search_roots") or [
        "/software/empyrean/alps", "/opt/eda/empyrean/alps", "/opt/empyrean/alps"]
    alps_resolution = None
    if not alps and alps_explicit:
        declared_binary = overrides.get("alps_bin") or environ.get(_contract_text("CRESCENDO_ALPS_BIN"))
        attempted = _expand(declared_binary, environ)
        candidate = _alps_binary_candidate(attempted)
        alps_resolution = {"status": "invalid_explicit_binary", "origin": sources.get("alps_bin"),
                           "candidates": [], "rejected": [candidate],
                           "message": "Declared ALPS executable is unavailable: %s (%s); no root/PATH fallback" % (attempted, candidate["status"])}
    elif not alps:
        roots = [alps_root] if alps_root else alps_search_roots
        origin = sources.get("alps_root") or "bounded ALPS install roots"
        selected_root, alps, alps_resolution = _resolve_alps_installation(roots, origin, environ=environ)
        if alps:
            alps_root = selected_root
            sources.setdefault("alps_root", origin)
    else:
        alps_resolution = {"status": "selected", "origin": sources.get("alps_bin"),
                           "selected": alps, "candidates": [alps],
                           "message": "Configured/PATH ALPS launcher is an executable file; native execution is not yet qualified"}
    if alps_resolution is not None and alps_root_explicit:
        alps_resolution['declared_root'] = (overrides.get('alps_root') or
            environ.get(_contract_text('CRESCENDO_ALPS_ROOT')) or environ.get('ALPS_HOME'))
    if alps:
        if not alps_root:
            parent = os.path.dirname(alps)
            alps_root = os.path.dirname(parent) if os.path.basename(parent) == "bin" else parent
            sources.setdefault("alps_root", "derived from ALPS binary")
        sources.setdefault("alps_bin", "derived from ALPS root")
    search_roots = overrides.get("search_roots") or ["/opt/eda/cadence", "/opt/cadence",
        "/software/cadence", "/software/cadence/spectre"]
    if not cadence_root:
        roots = _bounded_install_roots(search_roots, CADENCE_BINARY_RELATIVES["virtuoso_bin"])
        if roots:
            cadence_root = roots[0]
            sources["cadence_root"] = "bounded common install roots"
    if not spectre_root:
        roots = _bounded_install_roots(search_roots, SPECTRE_BINARY_RELATIVES)
        if roots:
            spectre_root = roots[0]
            sources["spectre_root"] = "bounded common install roots"
    if cadence_root:
        if not virtuoso and not explicit_binaries["virtuoso_bin"]:
            virtuoso = _first_existing(cadence_root, CADENCE_BINARY_RELATIVES["virtuoso_bin"], executable=True)
        if not ocean and not explicit_binaries["ocean_bin"]:
            ocean = _first_existing(cadence_root, CADENCE_BINARY_RELATIVES["ocean_bin"], executable=True)
    if spectre_root:
        if not spectre and not explicit_binaries["spectre_bin"]:
            spectre = _first_existing(spectre_root, SPECTRE_BINARY_RELATIVES, executable=True)
    cadence_root = cadence_root or _root_from_binary(virtuoso or ocean, ("share/cdssetup/cds.lib", "tools.lnx86/dfII"))
    spectre_root = spectre_root or _root_from_binary(spectre, ("tools.lnx86/spectre", "tools.lnx86"))
    if cadence_root:
        sources.setdefault("cadence_root", "derived from Cadence binary")
        if virtuoso:
            sources.setdefault("virtuoso_bin", "derived from cadence_root")
        if ocean:
            sources.setdefault("ocean_bin", "derived from cadence_root")
    if spectre_root:
        sources.setdefault("spectre_root", "derived from spectre binary")
        if spectre:
            sources.setdefault("spectre_bin", "derived from spectre_root")

    cds_lib = overrides.get("cds_lib") or environ.get(_contract_text("CRESCENDO_CDS_LIB"))
    if cds_lib:
        cds_lib = _expand(cds_lib)
        sources["cds_lib"] = "manual" if overrides.get("cds_lib") else _contract_text("environment:CRESCENDO_CDS_LIB")
    elif cadence_root:
        cds_lib = _first_existing(cadence_root, ("share/cdssetup/cds.lib",))
        if cds_lib:
            sources["cds_lib"] = "derived from cadence_root"

    license_value = overrides.get("license_file") or environ.get(_contract_text("CRESCENDO_LICENSE_FILE"))
    if license_value:
        sources["license_file"] = "manual" if overrides.get("license_file") else _contract_text("environment:CRESCENDO_LICENSE_FILE")
    else:
        for env_name in ("CDS_LIC_FILE", "LM_LICENSE_FILE"):
            if environ.get(env_name):
                license_value = environ[env_name]
                sources["license_file"] = "environment:%s" % env_name
                break
    if not license_value:
        license_candidates = []
        for root in list(search_roots) + [cadence_root]:
            if root:
                license_candidates.extend((os.path.join(root, "license", "license.dat"), os.path.join(root, "license.dat")))
        for candidate in license_candidates:
            if os.path.isfile(_expand(candidate)):
                license_value = _expand(candidate)
                sources["license_file"] = "bounded common install roots"
                break
    if license_value and not re.search(r"\d+@", str(license_value)):
        license_value = ":".join(_expand(item) for item in str(license_value).split(":") if item)

    paths = _runtime_paths(
        cadence_root,
        spectre_root,
        (virtuoso, ocean, spectre, alps),
        overrides.get("extra_binary_paths"),
        overrides.get("extra_library_paths"),
    )
    def model_directory(simulator_name):
        key = simulator_name + "_model_dir"
        env_name = _contract_text("CRESCENDO_") + simulator_name.upper() + "_MODEL_DIR"
        if overrides.get(key):
            sources[key] = (overrides.get("_model_dir_origins") or {}).get(simulator_name, "manual")
            return _expand(overrides[key], environ)
        if environ.get(env_name):
            sources[key] = "environment:" + env_name
            return _expand(environ[env_name], environ)
        if environ.get("PDK"):
            candidate = os.path.join(_expand(environ["PDK"], environ), simulator_name)
            if os.path.isdir(candidate) and os.access(candidate, os.R_OK | os.X_OK):
                sources[key] = "environment:PDK/" + simulator_name
                return os.path.realpath(candidate)
        from .simulator_profiles import model_directory as discover_model_directory
        directory, origin = discover_model_directory(overrides, simulator_name, environ=environ)
        if directory:
            sources[key] = origin
        return directory

    effective = {
        "virtuoso_bin": virtuoso,
        "ocean_bin": ocean,
        "spectre_bin": spectre,
        "alps_bin": alps, "alps_root": alps_root,
        "alps_search_roots": list(alps_search_roots), "simulator": simulator,
        "alps_model_dir": model_directory("alps"),
        "spectre_model_dir": model_directory("spectre"),
        "alps_model_file_map": dict(overrides.get("alps_model_file_map") or {}),
        "simulator_threads": overrides.get("simulator_threads"),
        "simulator_result_reader": overrides.get("simulator_result_reader") or "spectre",
        "spectre_result_reader": overrides.get("spectre_result_reader"),
        "alps_result_reader": overrides.get("alps_result_reader"),
        "cadence_root": cadence_root,
        "spectre_root": spectre_root,
        "cds_lib": cds_lib,
        "license_file": license_value,
        "extra_binary_paths": paths["binary_paths"],
        "extra_library_paths": paths["library_paths"],
        "pdk_name": overrides.get("pdk_name"),
        _contract_text("pdk_dir"): _expand(overrides.get(_contract_text("pdk_dir"))) if overrides.get(_contract_text("pdk_dir")) else None,
        "pdk_adapter": overrides.get("pdk_adapter") or "generic-cdf",
        "pdk_adapter_policy": overrides.get("pdk_adapter_policy"),
        "pdk_profiles": list(overrides.get("pdk_profiles") or []),
        "model_roots": list(overrides.get("model_roots") or []),
        "model_path_remaps": list(overrides.get("model_path_remaps") or []),
        "startup_extensions": list(overrides.get("startup_extensions") or []),
        "search_roots": list(search_roots),
    }
    checks = [
        {"name": key, "ok": bool(effective.get(key) and (_executable(effective[key]) if key.endswith("_bin") else os.path.isdir(effective[key]))), "value": effective.get(key)}
        for key in ("virtuoso_bin", "ocean_bin", "spectre_bin", "cadence_root", "spectre_root")
    ]
    checks.append({"name": "cds_lib", "ok": bool(cds_lib and os.path.isfile(cds_lib)), "value": cds_lib})
    checks.append({"name": "license_file", "ok": _license_valid(license_value), "value": license_value})
    for row in checks:
        if row["name"] in ("spectre_bin", "spectre_root") and simulator == "alps":
            row["optional"] = True
    checks.extend([
        {"name": "alps_bin", "ok": bool(alps and _executable(alps)), "value": alps,
         "optional": simulator != "alps" and not alps_explicit,
         "version_probe": "not_probed", "resolution": alps_resolution,
         "message": (alps_resolution or {}).get("message") or "ALPS runnable/version/result capability is qualified by its real diagnostic case; Spectre -W is not used"},
        {"name": "alps_root", "ok": bool(alps_root and os.path.isdir(alps_root)), "value": alps_root,
         "optional": simulator != "alps" and not alps_root_explicit},
    ])
    for simulator_name in ("spectre", "alps"):
        reader_key = simulator_name + "_result_reader"
        reader_value = effective[reader_key]
        checks.append({"name": reader_key, "ok": reader_value in ("spectre", "alps"),
                       "value": reader_value, "optional": reader_value in (None, ""),
                       "message": "Empty inherits the existing common Ocean reader; each solver may override independently"})
        key = simulator_name + "_model_dir"
        value = effective[key]
        explicit = bool(overrides.get(key) or environ.get(_contract_text("CRESCENDO_") + simulator_name.upper() + "_MODEL_DIR"))
        checks.append({"name": key, "ok": bool(value and os.path.isdir(value) and os.access(value, os.R_OK | os.X_OK)),
                       "value": value, "optional": not explicit and (simulator_name != "alps" or simulator != "alps"),
                       "message": "Model format/section identity is qualified by the private netlist; directories are discovered independently"})
    missing = [row["name"] for row in checks if not row["ok"] and not row.get("optional")]

    recommendations = []
    if any(name in missing for name in ("virtuoso_bin", "ocean_bin", "cadence_root")):
        recommendations.append(_contract_text("Source the vendor Cadence setup before starting Crescendo, or set Cadence Root and binaries in Advanced EDA Setup."))
    if any(name in missing for name in ("spectre_bin", "spectre_root")):
        recommendations.append("Set MMSIMHOME/Spectre Root or select the Spectre executable in Advanced EDA Setup.")
    if any(name in missing for name in ("alps_bin", "alps_root")):
        recommendations.append(_contract_text("Select the ALPS executable/root in EDA Settings, or set CRESCENDO_ALPS_BIN/ALPS_HOME. ALPS and Spectre are discovered independently."))
    if any(name in missing for name in ("alps_model_dir", "spectre_model_dir")):
        recommendations.append("Select the solver's readable model directory, or inherit PDK with independent spectre/alps subdirectories; model-format conversion still requires an explicit file mapping.")
    if "cds_lib" in missing:
        recommendations.append("Select the installation cdssetup/cds.lib file; it is usually under <CDSHOME>/share/cdssetup/cds.lib.")
    if "license_file" in missing:
        recommendations.append("Set CDS_LIC_FILE/LM_LICENSE_FILE or enter a license file or port@server value.")

    validation_env = dict(environ)
    validation_env["CDS_AUTO_64BIT"] = validation_env.get("CDS_AUTO_64BIT") or "ALL"
    validation_env["PATH"] = _prepend(validation_env.get("PATH"), paths["binary_paths"])
    validation_env["LD_LIBRARY_PATH"] = _prepend(validation_env.get("LD_LIBRARY_PATH"), paths["library_paths"])
    if license_value:
        validation_env["CDS_LIC_FILE"] = license_value
        validation_env["LM_LICENSE_FILE"] = _prepend(validation_env.get("LM_LICENSE_FILE"), [license_value])
    version_checks = []
    if validate:
        version_names = ("virtuoso_bin", "ocean_bin", "spectre_bin") if simulator == "spectre" else ("virtuoso_bin", "ocean_bin")
        version_checks = [_version_probe(name, effective[name], validation_env) for name in version_names]
    validation_ok = not missing and (not validate or all(row["ok"] for row in version_checks))
    return {
        "schema_version": _contract_text("crescendo.eda_discovery.v1"),
        "discovered_at": utc_timestamp(),
        "ok": validation_ok,
        "effective": effective,
        "sources": sources,
        "checks": checks,
        "version_checks": version_checks,
        "missing": missing,
        "recommendations": recommendations,
        "compatibility": {
            "ok": validation_ok,
            "message": "Cadence and Spectre version probes passed in an isolated child environment." if validation_ok else "EDA setup is incomplete or one or more isolated version probes failed.",
        },
    }


def _native_pdk_discovery_fallback(result, setup, hierarchy, configured, profiles,
                                   cds_lib, references):
    """Downgrade classification only after a complete probe of the loaded map."""
    if not result.get("blocking"):
        return result
    native = configured.get("_native_library_mappings")
    if (not isinstance(native, dict) or not native or not cds_lib
            or not os.path.isfile(cds_lib) or not os.access(cds_lib, os.R_OK)
            or (hierarchy or {}).get("schema_version") != _contract_text("crescendo.hierarchy.v1")
            or (hierarchy or {}).get("read_only") is not True):
        return result
    designs = hierarchy.get("designs") or []
    if not designs:
        return result
    # This is additional evidence for optional discovery, not a replacement for
    # the source/config guards already enforced by the probe and workspace.
    from .hierarchy import validate_native_hierarchy
    for design in designs:
        command = design.get("command") or {}
        if (command.get("returncode") != 0 or any(command.get(key) for key in
                ("timed_out", "cancelled", "termination_failed"))):
            return result
        try:
            validate_native_hierarchy(design.get("hierarchy") or {}, design.get("design") or {})
        except (RuntimeError, ValueError, KeyError, TypeError):
            return result
    required = set((setup or {}).get("libraries") or [])
    for key in ("master_libraries", "design_libraries", "technology_libraries"):
        required.update(references.get(key) or [])
    definitions = ((result.get("library_map") or {}).get("definitions") or {})
    verified = {}
    for library in required:
        path = native.get(library)
        parsed = definitions.get(library)
        if (not isinstance(path, str) or not os.path.isabs(path) or not parsed
                or not os.path.isdir(path) or not os.access(path, os.R_OK | os.X_OK)
                or os.path.realpath(parsed) != os.path.realpath(path)):
            return result
        verified[library] = os.path.realpath(path)
    if not verified:
        return result
    excluded_ids = sorted(set(profile_id for row in result.get("ambiguous_profile_matches") or []
                              for profile_id in row.get("profile_ids") or []))
    excluded_indices = [index for index, profile in enumerate(profiles)
                        if profile.get("id") in excluded_ids]
    retained = [profile for profile in profiles if profile.get("id") not in excluded_ids]
    fallback = discover_pdk_contexts(
        cds_lib, references.get("master_libraries") or [],
        design_libraries=sorted(set((setup or {}).get("libraries") or []) |
                                set(references.get("design_libraries") or [])),
        technology_libraries=references.get("technology_libraries") or [],
        profiles=retained,
    )
    if fallback.get("unresolved_libraries") or fallback.get("ambiguous_profile_matches"):
        return result
    issues = copy.deepcopy((result.get("library_map") or {}).get("errors") or [])
    issues.extend({"code": "ambiguous_pdk_profile", "library": row.get("library"),
                   "profile_ids": list(row.get("profile_ids") or [])}
                  for row in result.get("ambiguous_profile_matches") or [])
    fallback.update(blocking=False, ok=bool(fallback.get("contexts")),
                    discovery_degraded=True, classification_warnings=issues,
                    excluded_profile_ids=excluded_ids,
                    excluded_profile_indices=excluded_indices)
    fallback.setdefault("library_map", {})["native_verified_definitions"] = verified
    fallback["recommendations"] = list(fallback.get("recommendations") or []) + [
        "Optional PDK discovery has warnings; the complete native hierarchy and loaded library map remain verified."
    ]
    return fallback


def detect_pdk_context(setup, hierarchy=None, configured=None):
    configured = dict(configured or {})
    source = (setup or {}).get("source", {})
    references = hierarchy_library_references(hierarchy)
    design_libraries = set((setup or {}).get("libraries") or [])
    design_libraries.update(references.get("design_libraries") or [])
    candidates = list(references.get("master_libraries") or [])
    technology_libraries = list(references.get("technology_libraries") or [])
    source_cds_lib = configured.get("source_cds_lib_override") or (
        nearest_cds_lib(source.get("library_root")) if source.get("library_root") else None
    )
    cds_lib = source_cds_lib or configured.get("cds_lib")
    profile_base = os.path.dirname(os.path.abspath(cds_lib)) if cds_lib else None
    profiles = normalize_pdk_profiles(configured, profile_base)
    if not candidates and profiles:
        for profile in profiles:
            candidates.extend(profile.get("reference_libraries") or [])
            technology_libraries.extend(profile.get("technology_libraries") or [])

    if cds_lib:
        result = discover_pdk_contexts(
            cds_lib, candidates, design_libraries=sorted(design_libraries),
            technology_libraries=technology_libraries, profiles=profiles,
        )
        result = _native_pdk_discovery_fallback(
            result, setup, hierarchy, configured, profiles, cds_lib, references,
        )
    else:
        excluded = design_libraries | set(("analogLib", "basic", "functional", "sample", "US_8ths"))
        unresolved = sorted(set(candidates) - excluded)
        contexts = []
        if len(unresolved) == 1 or len(profiles) == 1:
            profile = profiles[0] if profiles else None
            libraries = list(profile.get("reference_libraries") or unresolved) if profile else unresolved
            package_root = profile.get("package_root") if profile else None
            context = {
                "schema_version": _contract_text("crescendo.pdk_context.v2"),
                "id": profile.get("id") if profile else "legacy:%s" % unresolved[0],
                "name": profile.get("name") if profile else unresolved[0],
                "package_root": package_root,
                "reference_libraries": sorted(set(libraries)),
                "technology_libraries": sorted(set(
                    (profile.get("technology_libraries") or technology_libraries)
                    if profile else technology_libraries
                )),
                "library_paths": {},
                "model_roots": list(profile.get("model_roots") or []) if profile else [],
                "model_path_remaps": list(profile.get("model_path_remaps") or []) if profile else [],
                "startup_extensions": list(profile.get("startup_extensions") or []) if profile else [],
                "adapter_policy": dict(
                    profile.get("adapter_policy") or normalize_adapter_policy(
                        configured.get("pdk_adapter_policy")
                    )
                ) if profile else normalize_adapter_policy(configured.get("pdk_adapter_policy")),
                "profile_id": profile.get("id") if profile else None,
                "detected_from": "manual profile" if profile else "schematic master libraries",
            }
            context["adapter_evidence"] = build_adapter_evidence(context)
            contexts.append(context)
        result = {
            "schema_version": _contract_text("crescendo.pdk_contexts.v2"),
            "ok": bool(contexts), "blocking": False, "contexts": contexts,
            "candidate_libraries": unresolved,
            "design_libraries": sorted(design_libraries),
            "technology_libraries": sorted(set(technology_libraries)),
            "unresolved_libraries": [], "undefined_libraries": [],
            "unavailable_libraries": [], "ambiguous_profile_matches": [],
            "library_map": None, "recommendations": [],
        }

    top_policy = configured.get("pdk_adapter_policy")
    adapter_errors = []
    for context in result.get("contexts") or []:
        if context.get("profile_id") is None and top_policy is not None:
            context["adapter_policy"] = normalize_adapter_policy(top_policy)
        policy = context.get("adapter_policy") or normalize_adapter_policy(top_policy)
        legacy_direct = bool(policy.get("legacy_direct"))
        selection = select_adapter(
            policy, context.get("adapter_evidence") or build_adapter_evidence(context),
            generic_failure="cdf_callback_incompatible" if legacy_direct else None,
        )
        if legacy_direct and selection.get("used_fallback"):
            selection["reason"] = "legacy_direct_compatibility"
        selected = selection.get("selected") or {}
        try:
            identity = adapter_identity(selected)
        except (IOError, OSError, ValueError) as error:
            identity = None
            adapter_errors.append({
                "context_id": context.get("id"),
                "error": "%s: %s" % (error.__class__.__name__, error),
            })
        context["adapter_selection"] = {
            "adapter_id": selected.get("id") or GENERIC_ADAPTER_ID,
            "label": selected.get("label") or "Generic CDF",
            "used_fallback": bool(selection.get("used_fallback")),
            "reason": selection.get("reason"),
            "identity": identity,
        }

    contexts = result.get("contexts") or []
    single = contexts[0] if len(contexts) == 1 else {}
    legacy_adapter = configured.get("pdk_adapter") or GENERIC_ADAPTER_ID
    effective_adapter = legacy_adapter if legacy_adapter != GENERIC_ADAPTER_ID else GENERIC_ADAPTER_ID
    if single.get("adapter_policy", {}).get("legacy_direct"):
        selected = single.get("adapter_selection") or {}
        if selected.get("used_fallback"):
            effective_adapter = single["adapter_policy"].get("path") or legacy_adapter
    recommendations = list(result.get("recommendations") or [])
    if not result.get("ok") and not recommendations:
        recommendations.append(
            "Load an ADE setup with probed device libraries, or define PDK profiles in Advanced EDA Setup."
        )
    if adapter_errors:
        recommendations.append("Fix the configured custom PDK adapter path before enabling fallback.")
    result.update({
        "pdk_name": single.get("name") or configured.get("pdk_name"),
        _contract_text("pdk_dir"): single.get("package_root") or (
            _expand(configured.get(_contract_text("pdk_dir"))) if configured.get(_contract_text("pdk_dir")) else None
        ),
        "pdk_adapter": effective_adapter,
        "pdk_adapter_policy": normalize_adapter_policy(top_policy),
        "detected_from": single.get("detected_from"),
        "source_cds_lib": source_cds_lib,
        "recommendations": recommendations,
        "adapter_errors": adapter_errors,
    })
    return result


def child_environment(config, home, include_runtime_ld=True, extra=None):
    cadence = config["cadence"]
    env = preserve_scheduler_home(restore_native_environment(os.environ, cadence.get("launch_environment")))
    env["HOME"] = os.path.abspath(home)
    # Never inherit a GUI/cache TMPDIR into native simulation children. HOME
    # already belongs to this isolated producer; its sibling scratch is owned
    # by the same workspace and follows its exact stop/cleanup lifecycle.
    native_home = os.path.abspath(home)
    scratch_base = os.path.dirname(native_home) if os.path.basename(native_home) == "home" else native_home
    scratch = ensure_dir(os.path.join(scratch_base, "_scratch"))
    env["TMPDIR"] = scratch
    env["TMP"] = scratch
    env["TEMP"] = scratch
    env["CDS_LOAD_ENV"] = "CWD"
    env["CDS_AUTO_64BIT"] = "ALL"
    env["CDS_Netlisting_Mode"] = "Analog"
    license_file = cadence.get("license_file")
    if license_file:
        env["CDS_LIC_FILE"] = license_file
        env["LM_LICENSE_FILE"] = _prepend(env.get("LM_LICENSE_FILE"), [license_file])
    binary_paths = cadence.get("extra_binary_paths") or []
    if binary_paths:
        env["PATH"] = _prepend(env.get("PATH"), binary_paths)
    if include_runtime_ld:
        library_paths = cadence.get("extra_library_paths") or []
        if library_paths:
            env["LD_LIBRARY_PATH"] = _prepend(env.get("LD_LIBRARY_PATH"), library_paths)
    if extra:
        for key, value in extra.items():
            env[str(key)] = str(value)
    # Native -nograph still uses an X connection. Reuse the invoking display
    # instead of letting Cadence choose an unrelated shared background display.
    # A caller's explicit nograph selection (including empty) remains authoritative.
    if "CDS_NOGRAPH_DISPLAY" not in env and env.get("DISPLAY"):
        env["CDS_NOGRAPH_DISPLAY"] = env["DISPLAY"]
    from .runtime import child_environment as runtime_environment
    return runtime_environment(env)


def guardian_termination_grace(termination_grace_seconds):
    return min(
        5.0, max(0.01, (max(0.03, float(termination_grace_seconds)) - 0.01) / 3.0),
    )


def build_guardian_command(argv, cwd, stdout_path, stderr_path, guardian_state_path,
                           guardian_token, termination_grace_seconds=15,
                           guardian_launch_lock_fd=None, poll_expected_parent=False):
    guardian_script = guardian_script_path()
    if not os.path.isfile(guardian_script):
        raise ValueError("Ocean worker guardian is missing: %s" % guardian_script)
    command = [
        sys.executable, guardian_script,
        "--state-path", os.path.abspath(guardian_state_path),
        "--stdout-path", os.path.abspath(stdout_path),
        "--stderr-path", os.path.abspath(stderr_path),
        "--token", str(guardian_token),
        "--cwd", os.path.realpath(os.path.abspath(cwd)),
        "--expected-parent-pid", str(os.getpid()),
        "--terminate-grace-seconds", str(guardian_termination_grace(termination_grace_seconds)),
    ]
    if guardian_launch_lock_fd is not None:
        if isinstance(guardian_launch_lock_fd, bool) or int(guardian_launch_lock_fd) < 3:
            raise ValueError("guardian launch lock descriptor is invalid")
        os.fstat(int(guardian_launch_lock_fd))
        command.extend(("--launch-lock-fd", str(int(guardian_launch_lock_fd))))
    if poll_expected_parent:
        command.append("--poll-expected-parent")
    return command + ["--"] + list(argv)


def _close_owned_launch_lock(owner):
    descriptor = owner.get("descriptor")
    owner["descriptor"] = None
    if descriptor is None:
        return
    try:
        os.close(descriptor)
    except OSError as error:
        if getattr(error, "errno", None) != 9:
            raise


def _run_command_impl(argv, cwd, env, timeout_seconds, stdout_path=None, stderr_path=None,
                      startup_lock=None, ready_path=None, startup_timeout_seconds=None,
                      lock_timeout_seconds=None, progress_path=None,
                      progress_timeout_seconds=None, progress_initial_timeout_seconds=None,
                      cancel_check=None, termination_grace_seconds=15,
                      termination_kill_seconds=5, guardian_state_path=None,
                      guardian_token=None, guardian_launch_lock_fd=None,
                      _launch_lock_owner=None):
    started = time.time()
    lock_held = False
    original_argv = list(argv)
    launch_lock_descriptor = guardian_launch_lock_fd
    guarded = (
        guardian_state_path is not None or guardian_token is not None
        or guardian_launch_lock_fd is not None
    )
    if guarded:
        if not guardian_state_path or not guardian_token:
            raise ValueError("guardian_state_path and guardian_token must be provided together")
        if not stdout_path or not stderr_path:
            raise ValueError("guarded command requires stdout_path and stderr_path")
        argv = build_guardian_command(
            original_argv, cwd, stdout_path, stderr_path, guardian_state_path,
            guardian_token, termination_grace_seconds=termination_grace_seconds,
            guardian_launch_lock_fd=guardian_launch_lock_fd,
        )
        env = guardian_environment(env)
        env[LAUNCH_TOKEN_ENVIRONMENT] = str(guardian_token)

    def cancellation_requested():
        if not cancel_check:
            return False
        try:
            return bool(cancel_check())
        except Exception:
            return True

    def cancelled_result(message):
        if stdout_path:
            atomic_write_text(stdout_path, "")
        if stderr_path:
            atomic_write_text(stderr_path, message + "\n")
        return {
            "argv": list(argv), "cwd": cwd, "returncode": -1,
            "timed_out": False, "startup_timed_out": False,
            "progress_timed_out": False, "cancelled": True,
            "termination_failed": False,
            "lock_wait_seconds": time.time() - started,
            "elapsed_seconds": time.time() - started,
            "stdout_tail": "", "stderr_tail": message,
        }

    if cancellation_requested():
        return cancelled_result("command cancelled before Cadence startup")
    if startup_lock is not None:
        lock_limits = [float(lock_timeout_seconds)] if lock_timeout_seconds is not None else [
            float(value) for value in (timeout_seconds, startup_timeout_seconds)
            if value is not None and float(value) > 0
        ]
        if cancel_check:
            lock_deadline = time.time() + min(lock_limits) if lock_limits else None
            while not lock_held:
                if cancellation_requested():
                    return cancelled_result("command cancelled while waiting for Cadence startup lock")
                if lock_deadline is not None:
                    remaining = lock_deadline - time.time()
                    if remaining <= 0:
                        break
                    wait_seconds = min(0.1, remaining)
                else:
                    wait_seconds = 0.1
                lock_held = startup_lock.acquire(timeout=wait_seconds)
        elif lock_limits:
            lock_held = startup_lock.acquire(timeout=min(lock_limits))
        else:
            startup_lock.acquire()
            lock_held = True
        if not lock_held:
            message = "Cadence startup lock acquisition timed out"
            if stdout_path:
                atomic_write_text(stdout_path, "")
            if stderr_path:
                atomic_write_text(stderr_path, message + "\n")
            return {
                "argv": list(argv), "cwd": cwd, "returncode": -1,
                "timed_out": True, "startup_timed_out": True,
                "progress_timed_out": False, "cancelled": False,
                "termination_failed": False,
                "lock_wait_seconds": time.time() - started,
                "elapsed_seconds": time.time() - started,
                "stdout_tail": "", "stderr_tail": message,
            }
        if cancellation_requested():
            startup_lock.release()
            lock_held = False
            return cancelled_result("command cancelled after acquiring Cadence startup lock")
    lock_wait_seconds = time.time() - started
    command_started = time.time()
    try:
        popen_arguments = {
            "cwd": cwd, "env": env, "stdout": subprocess.PIPE, "stderr": subprocess.PIPE,
        }
        if guarded:
            popen_arguments.update({
                "stdin": subprocess.PIPE, "start_new_session": True, "close_fds": True,
            })
            if guardian_launch_lock_fd is not None:
                popen_arguments["pass_fds"] = (int(guardian_launch_lock_fd),)
        else:
            popen_arguments["preexec_fn"] = os.setsid
        process = subprocess.Popen(list(argv), **popen_arguments)
        if guardian_launch_lock_fd is not None:
            _close_owned_launch_lock(_launch_lock_owner)
    except Exception:
        if lock_held:
            startup_lock.release()
        raise
    try:
        process_group_id = os.getpgid(process.pid)
    except OSError:
        process_group_id = None
    if process_group_id != process.pid:
        process_group_id = None
    timed_out = False
    startup_timed_out = False
    progress_timed_out = False
    cancelled = False
    deadline = command_started + float(timeout_seconds) if timeout_seconds and timeout_seconds > 0 else None
    startup_deadline = command_started + float(startup_timeout_seconds) if startup_timeout_seconds and startup_timeout_seconds > 0 else None
    progress_deadline = (
        command_started + float(progress_initial_timeout_seconds or progress_timeout_seconds)
        if progress_path and progress_timeout_seconds and progress_timeout_seconds > 0 else None
    )
    progress_signature = None
    output_drain_deadline = None
    output_drain_timed_out = False
    output_drain_incomplete = False
    guarded_output = [None, None]

    def collect_guarded_output(timeout):
        # Python 3.6 re-registers pipes already closed by an earlier timed
        # communicate. Hide only those closed handles during the next poll;
        # retain their already-read bytes and restore the public attributes.
        hidden = []
        for name in ("stdout", "stderr"):
            stream = getattr(process, name, None)
            if stream is not None and stream.closed:
                # This Python's TimeoutExpired also omits partial output.
                # Its closed stream can only be recovered from Popen's
                # existing byte fragments before hiding the handle.
                fragments = getattr(process, "_fileobj2output", {}).get(stream)
                if isinstance(fragments, list) and all(isinstance(value, bytes) for value in fragments):
                    guarded_output[0 if name == "stdout" else 1] = b"".join(fragments)
                hidden.append((name, stream))
                setattr(process, name, None)
        try:
            try:
                output = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired as error:
                output = (error.output, error.stderr)
                for index, value in enumerate(output):
                    if value is not None:
                        guarded_output[index] = value
                raise
            for index, value in enumerate(output):
                if value is not None:
                    guarded_output[index] = value
            return tuple(guarded_output)
        finally:
            for name, stream in hidden:
                setattr(process, name, stream)

    try:
        while True:
            if lock_held and ready_path and os.path.isfile(ready_path):
                startup_lock.release()
                lock_held = False
            if cancellation_requested():
                cancelled = True
                break
            now = time.time()
            if progress_deadline:
                try:
                    stat = os.stat(progress_path)
                    signature = (stat.st_mtime, stat.st_size)
                except OSError:
                    signature = None
                if signature is not None and signature != progress_signature:
                    progress_signature = signature
                    progress_deadline = now + float(progress_timeout_seconds)
            if startup_deadline and lock_held and now >= startup_deadline:
                startup_timed_out = True
                timed_out = True
                break
            if progress_deadline and now >= progress_deadline:
                progress_timed_out = True
                timed_out = True
                break
            if deadline and now >= deadline:
                timed_out = True
                break
            waits = [0.5]
            if deadline:
                waits.append(max(0.01, deadline - now))
            if startup_deadline and lock_held:
                waits.append(max(0.01, startup_deadline - now))
            if progress_deadline:
                waits.append(max(0.01, progress_deadline - now))
            if guarded:
                if process.poll() is not None:
                    # Exiting the wrapper does not imply EOF on both pipes.
                    # communicate's short poll can expire even after waitpid
                    # succeeded. Drain under a separate bounded exit allowance,
                    # while the original command deadline and Stop stay active.
                    if output_drain_deadline is None:
                        output_drain_deadline = time.monotonic() + max(
                            0.01, float(termination_grace_seconds))
                    remaining_drain = output_drain_deadline - time.monotonic()
                    if remaining_drain <= 0:
                        output_drain_timed_out = True
                        timed_out = True
                        break
                    try:
                        stdout, stderr = collect_guarded_output(
                            min(0.1, min(waits), remaining_drain))
                        break
                    except subprocess.TimeoutExpired:
                        continue
                time.sleep(min(min(waits), 0.1))
            else:
                try:
                    stdout, stderr = process.communicate(timeout=min(waits))
                    break
                except subprocess.TimeoutExpired:
                    continue
    finally:
        if lock_held:
            startup_lock.release()

    termination_failed = False
    if (timed_out or cancelled) and guarded:
        stdout = None
        stderr = None
        try:
            process.stdin.close()
        except (IOError, OSError, AttributeError):
            pass
        process.stdin = None
        try:
            stdout, stderr = collect_guarded_output(timeout=max(0.01, float(termination_grace_seconds)))
        except subprocess.TimeoutExpired:
            try:
                if process_group_id is not None:
                    os.killpg(process_group_id, signal.SIGTERM)
                elif process.poll() is None:
                    process.terminate()
            except OSError:
                pass
            try:
                stdout, stderr = collect_guarded_output(timeout=max(0.01, float(termination_kill_seconds)))
            except subprocess.TimeoutExpired as error:
                output_drain_incomplete = True
                stdout = guarded_output[0] or b""
                stderr = guarded_output[1] or b""
                try:
                    if process_group_id is not None:
                        os.killpg(process_group_id, signal.SIGKILL)
                    elif process.poll() is None:
                        process.kill()
                except OSError:
                    pass
                try:
                    process.wait(timeout=max(0.01, float(termination_kill_seconds)))
                except subprocess.TimeoutExpired:
                    pass
        try:
            wrapper_group_alive = bool(
                process_group_id is not None and os.killpg(process_group_id, 0) is None
            )
        except OSError as error:
            wrapper_group_alive = getattr(error, "errno", None) != 3
        termination_failed = process.poll() is None or wrapper_group_alive
    elif timed_out or cancelled:
        group_identity_proven = process_group_id is not None
        direct_process_was_running = process.poll() is None

        def owned_process_group_alive():
            if process_group_id is None:
                return process.poll() is None
            try:
                os.killpg(process_group_id, 0)
                return True
            except OSError as error:
                return getattr(error, "errno", None) != 3

        def signal_owned_process_group(sig):
            if process_group_id is not None:
                try:
                    os.killpg(process_group_id, sig)
                except OSError as error:
                    if getattr(error, "errno", None) != 3:
                        raise
                return
            if process.poll() is None:
                process.send_signal(sig)

        termination_grace = max(0.01, float(termination_grace_seconds))
        termination_deadline = time.monotonic() + termination_grace
        stdout = None
        stderr = None
        try:
            signal_owned_process_group(signal.SIGTERM)
        except OSError:
            pass
        try:
            stdout, stderr = process.communicate(timeout=termination_grace)
        except subprocess.TimeoutExpired:
            pass
        while owned_process_group_alive() and time.monotonic() < termination_deadline:
            time.sleep(min(0.05, max(0.0, termination_deadline - time.monotonic())))
        if owned_process_group_alive():
            try:
                signal_owned_process_group(signal.SIGKILL)
            except OSError:
                pass
            kill_deadline = time.monotonic() + max(0.01, float(termination_kill_seconds))
            if stdout is None:
                try:
                    stdout, stderr = process.communicate(
                        timeout=max(0.01, kill_deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired as error:
                    stdout = error.output or b""
                    stderr = error.stderr or b""
            while owned_process_group_alive() and time.monotonic() < kill_deadline:
                time.sleep(min(0.05, max(0.0, kill_deadline - time.monotonic())))
        if stdout is None:
            try:
                stdout, stderr = process.communicate(timeout=0.01)
            except subprocess.TimeoutExpired as error:
                stdout = error.output or b""
                stderr = error.stderr or b""
        termination_failed = (
            process.poll() is None or owned_process_group_alive() or
            (direct_process_was_running and not group_identity_proven)
        )
    # Unclosed wrapper pipes are not a proven clean completion, including
    # unlimited candidate runs. Never replace the guardian's owned-process
    # inspection below with the wrapper return code or with pipe EOF.
    termination_failed = termination_failed or output_drain_incomplete
    if guarded and (timed_out or cancelled):
        # A retained inherited writer must not retain our reader descriptors.
        for stream in (getattr(process, "stdout", None), getattr(process, "stderr", None)):
            if stream is not None:
                try:
                    stream.close()
                except (IOError, OSError):
                    pass
    stdout = stdout or b""
    stderr = stderr or b""
    wrapper_stdout_text = stdout.decode("utf-8", "replace")
    wrapper_stderr_text = stderr.decode("utf-8", "replace")
    if guarded:
        try:
            with open(stdout_path, "rb") as stream:
                stdout_text = stream.read().decode("utf-8", "replace")
        except (IOError, OSError):
            stdout_text = ""
        try:
            with open(stderr_path, "rb") as stream:
                stderr_text = stream.read().decode("utf-8", "replace")
        except (IOError, OSError):
            stderr_text = ""
        if wrapper_stdout_text:
            stdout_text = (stdout_text.rstrip() + "\nguardian stdout:\n" + wrapper_stdout_text).lstrip("\n")
        if wrapper_stderr_text:
            stderr_text = (stderr_text.rstrip() + "\nguardian stderr:\n" + wrapper_stderr_text).lstrip("\n")
    else:
        stdout_text = wrapper_stdout_text
        stderr_text = wrapper_stderr_text
    if cancelled:
        stderr_text = (stderr_text.rstrip() + "\ncommand cancelled by request\n").lstrip("\n")
    if termination_failed:
        stderr_text = (
            stderr_text.rstrip() +
            "\nprocess group did not exit after bounded SIGTERM/SIGKILL cleanup\n"
        ).lstrip("\n")
    guardian_cleanup = None
    if guarded:
        inspection = inspect_legacy_guardian_attempt(
            os.path.dirname(os.path.abspath(guardian_state_path)),
            expected_workspace_root=cwd,
        )
        guardian_cleanup = {
            "state": inspection.get("state"),
            "reason": inspection.get("reason"),
            "processes": inspection.get("processes") or [],
            "termination_proof": inspection.get("termination_proof") or {},
            "state_path": os.path.abspath(guardian_state_path),
        }
        if inspection.get("state") != "dead":
            termination_failed = True
            stderr_text = (
                stderr_text.rstrip() + "\nguardian did not prove Ocean/descendant cleanup: %s\n"
                % (inspection.get("reason") or inspection.get("state"))
            ).lstrip("\n")
    if stdout_path and not guarded:
        atomic_write_text(stdout_path, stdout_text)
    if stderr_path and not guarded:
        atomic_write_text(stderr_path, stderr_text)
    returncode = process.returncode if process.returncode is not None else -1
    if guarded and termination_failed and returncode == 0:
        returncode = 2
    return {
        "argv": original_argv,
        "cwd": cwd,
        "returncode": returncode,
        "timed_out": timed_out,
        "startup_timed_out": startup_timed_out,
        "progress_timed_out": progress_timed_out,
        "output_drain_timed_out": output_drain_timed_out,
        "cancelled": cancelled,
        "termination_failed": termination_failed,
        "lock_wait_seconds": lock_wait_seconds,
        "elapsed_seconds": time.time() - started,
        "stdout_tail": stdout_text[-8000:],
        "stderr_tail": stderr_text[-8000:],
        "guardian_state_path": os.path.abspath(guardian_state_path) if guarded else None,
        "guardian_launch_lock_fd": int(launch_lock_descriptor) if launch_lock_descriptor is not None else None,
        "guardian_cleanup": guardian_cleanup,
    }


def run_command(argv, cwd, env, timeout_seconds, stdout_path=None, stderr_path=None,
                startup_lock=None, ready_path=None, startup_timeout_seconds=None,
                lock_timeout_seconds=None, progress_path=None,
                progress_timeout_seconds=None, progress_initial_timeout_seconds=None,
                cancel_check=None, termination_grace_seconds=15,
                termination_kill_seconds=5, guardian_state_path=None,
                guardian_token=None, guardian_launch_lock_fd=None):
    cancellation_events = _command_cancellation_events()
    original_cancel_check = cancel_check
    if cancellation_events:
        def scoped_cancel_check():
            if _command_cancellation_requested(cancellation_events):
                return True
            return bool(original_cancel_check and original_cancel_check())
        cancel_check = scoped_cancel_check
        # Read-only prepare owns this subprocess group. Exit need not wait for
        # the long simulation shutdown allowance before escalating its cleanup.
        termination_grace_seconds = min(float(termination_grace_seconds), 2.0)
        termination_kill_seconds = min(float(termination_kill_seconds), 2.0)
    descriptor = None
    if guardian_launch_lock_fd is not None:
        if isinstance(guardian_launch_lock_fd, bool) or int(guardian_launch_lock_fd) < 3:
            raise ValueError("guardian launch lock descriptor is invalid")
        descriptor = int(guardian_launch_lock_fd)
    owner = {"descriptor": descriptor}
    try:
        result = _run_command_impl(
            argv, cwd, env, timeout_seconds, stdout_path=stdout_path,
            stderr_path=stderr_path, startup_lock=startup_lock, ready_path=ready_path,
            startup_timeout_seconds=startup_timeout_seconds,
            lock_timeout_seconds=lock_timeout_seconds, progress_path=progress_path,
            progress_timeout_seconds=progress_timeout_seconds,
            progress_initial_timeout_seconds=progress_initial_timeout_seconds,
            cancel_check=cancel_check,
            termination_grace_seconds=termination_grace_seconds,
            termination_kill_seconds=termination_kill_seconds,
            guardian_state_path=guardian_state_path, guardian_token=guardian_token,
            guardian_launch_lock_fd=guardian_launch_lock_fd,
            _launch_lock_owner=owner,
        )
        if cancellation_events and result.get("termination_failed"):
            raise CommandCancelled(
                "Read-only Cadence probe cleanup could not be proven; Exit must retain its evidence",
                result=result,
            )
        if cancellation_events and result.get("cancelled"):
            raise CommandCancelled(result=result)
        if cancellation_events:
            check_command_cancellation()
        return result
    finally:
        _close_owned_launch_lock(owner)


def nearest_cds_lib(library_root):
    current = os.path.abspath(library_root)
    while True:
        candidate = os.path.join(current, "cds.lib")
        if os.path.isfile(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return None


def write_readonly_cds_context(workdir, library_name, library_root, installation_cds_lib=None,
                               source_cds_lib_override=None):
    ensure_dir(workdir)
    lines = []
    if installation_cds_lib and os.path.isfile(installation_cds_lib):
        lines.append("SOFTINCLUDE %s" % cds_lib_token(os.path.realpath(installation_cds_lib)))
    source_cds_lib = source_cds_lib_override or nearest_cds_lib(library_root)
    if source_cds_lib:
        lines.append("INCLUDE %s" % cds_lib_token(source_cds_lib))
    lines.append("DEFINE %s %s" % (cds_lib_token(library_name), cds_lib_token(os.path.abspath(library_root))))
    atomic_write_text(os.path.join(workdir, "cds.lib"), "\n".join(lines) + "\n")
    atomic_write_text(
        os.path.join(workdir, ".cdsinit"),
        _contract_text('; Crescendo read-only probe context. No source library write calls are allowed.\n'),
    )
    return {"source_cds_lib": source_cds_lib, "local_cds_lib": os.path.join(workdir, "cds.lib")}
