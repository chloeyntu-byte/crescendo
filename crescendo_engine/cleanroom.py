"""Fail-closed clean-room installation and ADE preparation harness."""

from __future__ import print_function

import argparse
import copy
import hashlib
import json
import os
import pwd
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from crescendo_engine.runtime import config_home as _config_home, bind_text
_contract_text = bind_text()

from .pdk import parse_cds_lib


SCHEMA_MANIFEST = _contract_text("crescendo.cleanroom_manifest.v1")
SCHEMA_PLAN = _contract_text("crescendo.cleanroom_plan.v1")
SCHEMA_REPORT = _contract_text("crescendo.cleanroom_report.v1")
TOKEN_ENVIRONMENT = _contract_text("CRESCENDO_CLEANROOM_TOKEN")
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
DEFAULT_WORK_PARENTS = ("/var/tmp", "/tmp")
SETUP_DATABASE_NAMES = {
    "maestro": "maestro.sdb",
    "adexl": "data.sdb",
}


class CleanroomError(RuntimeError):
    pass


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _atomic_write_json(path, value):
    directory = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(directory):
        os.makedirs(directory, 0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=".%s." % os.path.basename(path), dir=directory)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.rename(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _load_json(path):
    with open(path, "r") as stream:
        return json.load(stream)


def _resolve_path(value, base_dir):
    if value in (None, ""):
        return None
    value = os.path.expandvars(os.path.expanduser(str(value)))
    if not os.path.isabs(value):
        value = os.path.join(base_dir, value)
    return os.path.abspath(os.path.normpath(value))


def _is_under(path, root):
    path = os.path.realpath(os.path.abspath(path))
    root = os.path.realpath(os.path.abspath(root))
    return path == root or path.startswith(root + os.sep)


def _safe_identifier(value, label):
    value = str(value or "")
    if not SAFE_ID.match(value):
        raise CleanroomError("%s must match %s" % (label, SAFE_ID.pattern))
    return value


def _safe_relative(value, label):
    value = str(value or "").replace("\\", "/")
    if not value or value.startswith("/"):
        raise CleanroomError("%s must be a non-empty relative path" % label)
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise CleanroomError("%s contains an unsafe path component" % label)
    return os.path.join(*parts)


def _tree_fingerprint(path, include_mode=True):
    path = os.path.abspath(path)
    digest = hashlib.sha256()
    counts = {"directories": 0, "files": 0, "symlinks": 0, "bytes": 0}

    def update(kind, relative, mode=None, payload=None):
        digest.update(kind.encode("ascii") + b"\0")
        digest.update(relative.replace(os.sep, "/").encode("utf-8") + b"\0")
        if include_mode:
            digest.update(("%04o" % int(mode or 0)).encode("ascii") + b"\0")
        if payload is not None:
            digest.update(payload)
        digest.update(b"\0")

    if os.path.islink(path):
        info = os.lstat(path)
        target = os.readlink(path).encode("utf-8")
        update("link", ".", stat.S_IMODE(info.st_mode), target)
        counts["symlinks"] = 1
    elif os.path.isfile(path):
        info = os.stat(path)
        update("file", ".", stat.S_IMODE(info.st_mode))
        with open(path, "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                counts["bytes"] += len(block)
        counts["files"] = 1
    elif os.path.isdir(path):
        for current, directories, files in os.walk(path, topdown=True, followlinks=False):
            directories.sort()
            files.sort()
            relative_root = os.path.relpath(current, path)
            info = os.lstat(current)
            update("dir", relative_root, stat.S_IMODE(info.st_mode))
            counts["directories"] += 1
            link_directories = []
            for name in list(directories):
                candidate = os.path.join(current, name)
                if os.path.islink(candidate):
                    link_directories.append(name)
                    directories.remove(name)
            for name in sorted(link_directories + files):
                candidate = os.path.join(current, name)
                relative = os.path.relpath(candidate, path)
                info = os.lstat(candidate)
                if stat.S_ISLNK(info.st_mode):
                    update(
                        "link", relative, stat.S_IMODE(info.st_mode),
                        os.readlink(candidate).encode("utf-8"),
                    )
                    counts["symlinks"] += 1
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise CleanroomError("unsupported source file type: %s" % candidate)
                update("file", relative, stat.S_IMODE(info.st_mode))
                counts["files"] += 1
                with open(candidate, "rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                        counts["bytes"] += len(block)
    else:
        raise CleanroomError("source path does not exist: %s" % path)
    result = {"path": path, "sha256": digest.hexdigest(), "include_mode": bool(include_mode)}
    result.update(counts)
    return result


def _find_installation_cds_lib(parsed, explicit, search_roots, manifest_dir):
    if explicit:
        candidate = _resolve_path(explicit, manifest_dir)
        if not os.path.isfile(candidate):
            raise CleanroomError("installation cds.lib does not exist: %s" % candidate)
        return candidate
    roots = [os.path.realpath(path) for path in search_roots]
    for row in parsed.get("includes") or []:
        candidate = row.get("path")
        if candidate and os.path.isfile(candidate) and any(_is_under(candidate, root) for root in roots):
            return os.path.realpath(candidate)
    for root in roots:
        candidates = [root]
        try:
            candidates.extend(os.path.join(root, name) for name in sorted(os.listdir(root)))
        except OSError:
            pass
        for candidate_root in candidates:
            candidate = os.path.join(candidate_root, "share", "cdssetup", "cds.lib")
            if os.path.isfile(candidate):
                return os.path.realpath(candidate)
    raise CleanroomError("cannot locate the installation cds.lib within bounded Cadence roots")


def _relocatable_members(values, source_root, manifest_dir, label):
    rows = []
    for index, value in enumerate(values or []):
        source = _resolve_path(value, source_root if not os.path.isabs(str(value)) else manifest_dir)
        if not _is_under(source, source_root):
            raise CleanroomError("%s[%d] escapes its PDK source root" % (label, index))
        if not os.path.exists(source):
            raise CleanroomError("%s[%d] does not exist: %s" % (label, index, source))
        rows.append({"source": source, "relative": os.path.relpath(source, source_root)})
    return rows


def normalize_manifest(document, manifest_path):
    if not isinstance(document, dict) or document.get("schema_version") != SCHEMA_MANIFEST:
        raise CleanroomError("unsupported clean-room manifest schema")
    manifest_path = os.path.abspath(manifest_path)
    manifest_dir = os.path.dirname(manifest_path)
    source_cds_lib = _resolve_path(document.get("source_cds_lib"), manifest_dir)
    if not source_cds_lib or not os.path.isfile(source_cds_lib):
        raise CleanroomError("source_cds_lib must identify a readable file")
    parsed = parse_cds_lib(source_cds_lib)
    if not parsed.get("ok"):
        raise CleanroomError("source cds.lib is not resolvable: %s" % _canonical_json(parsed.get("errors")))

    cadence = copy.deepcopy(document.get("cadence") or {})
    search_roots = [
        _resolve_path(value, manifest_dir)
        for value in cadence.get("search_roots") or ("/opt/eda/cadence", "/opt/cadence")
    ]
    installation_cds_lib = _find_installation_cds_lib(
        parsed, cadence.get("installation_cds_lib"), search_roots, manifest_dir,
    )

    designs = []
    design_ids = set()
    design_names = set()
    case_ids = set()
    for index, raw in enumerate(document.get("design_libraries") or []):
        design_id = _safe_identifier(raw.get("id"), "design_libraries[%d].id" % index)
        name = _safe_identifier(raw.get("name"), "design_libraries[%d].name" % index)
        if design_id in design_ids or name in design_names:
            raise CleanroomError("duplicate design id or library name: %s/%s" % (design_id, name))
        design_ids.add(design_id)
        design_names.add(name)
        source_root = _resolve_path(raw.get("source_root"), manifest_dir)
        if not source_root or not os.path.isdir(source_root):
            raise CleanroomError("design source root does not exist: %s" % source_root)
        defined = (parsed.get("definitions") or {}).get(name)
        if not defined or not _is_under(defined, source_root):
            raise CleanroomError("design library %s is not mapped beneath its source root" % name)
        setups = []
        for setup_index, setup in enumerate(raw.get("setups") or []):
            case_id = _safe_identifier(
                setup.get("id"), "design_libraries[%d].setups[%d].id" % (index, setup_index),
            )
            if case_id in case_ids:
                raise CleanroomError("duplicate setup case id: %s" % case_id)
            case_ids.add(case_id)
            backend = str(setup.get("backend") or "maestro").lower()
            if backend not in SETUP_DATABASE_NAMES:
                raise CleanroomError(
                    "unsupported ADE setup backend for %s: %s; expected one of %s"
                    % (case_id, backend, ", ".join(sorted(SETUP_DATABASE_NAMES)))
                )
            relative = _safe_relative(
                setup.get("relative_path"),
                "design_libraries[%d].setups[%d].relative_path" % (index, setup_index),
            )
            source_setup = os.path.abspath(os.path.join(source_root, relative))
            if not _is_under(source_setup, source_root) or not os.path.isfile(source_setup):
                raise CleanroomError("setup does not exist beneath its design root: %s" % source_setup)
            expected_name = SETUP_DATABASE_NAMES[backend]
            if os.path.basename(source_setup).lower() != expected_name:
                raise CleanroomError(
                    "%s setup must end in %s: %s"
                    % (backend, expected_name, source_setup)
                )
            setups.append({"id": case_id, "backend": backend, "relative_path": relative})
        if not setups:
            raise CleanroomError("design %s has no ADE setup cases" % design_id)
        pdk_ids = [
            _safe_identifier(value, "design_libraries[%d].pdk_ids" % index)
            for value in raw.get("pdk_ids") or []
        ]
        designs.append({
            "id": design_id, "name": name, "source_root": source_root,
            "library_relative": os.path.relpath(defined, source_root),
            "pdk_ids": pdk_ids, "setups": setups,
        })
    if not designs:
        raise CleanroomError("manifest contains no design libraries")

    pdks = []
    pdk_ids = set()
    mapped_libraries = set(design_names)
    for index, raw in enumerate(document.get("pdks") or []):
        pdk_id = _safe_identifier(raw.get("id"), "pdks[%d].id" % index)
        if pdk_id in pdk_ids:
            raise CleanroomError("duplicate PDK id: %s" % pdk_id)
        pdk_ids.add(pdk_id)
        source_root = _resolve_path(raw.get("source_root"), manifest_dir)
        if not source_root or not os.path.isdir(source_root):
            raise CleanroomError("PDK source root does not exist: %s" % source_root)
        reference_libraries = [
            _safe_identifier(value, "pdks[%d].reference_libraries" % index)
            for value in raw.get("reference_libraries") or []
        ]
        technology_libraries = [
            _safe_identifier(value, "pdks[%d].technology_libraries" % index)
            for value in raw.get("technology_libraries") or []
        ]
        libraries = []
        for library in reference_libraries + technology_libraries:
            if library in [row["name"] for row in libraries]:
                continue
            if library in mapped_libraries:
                raise CleanroomError("library is assigned to multiple relocated assets: %s" % library)
            defined = (parsed.get("definitions") or {}).get(library)
            if not defined or not _is_under(defined, source_root):
                raise CleanroomError("PDK library %s is not mapped beneath %s" % (library, source_root))
            libraries.append({"name": library, "relative": os.path.relpath(defined, source_root)})
            mapped_libraries.add(library)
        if not libraries:
            raise CleanroomError("PDK %s has no mapped reference or technology library" % pdk_id)
        adapter_policy = copy.deepcopy(raw.get("adapter_policy") or {"mode": "auto"})
        pdks.append({
            "id": pdk_id,
            "name": str(raw.get("name") or pdk_id),
            "source_root": source_root,
            "reference_libraries": reference_libraries,
            "technology_libraries": technology_libraries,
            "libraries": libraries,
            "model_roots": _relocatable_members(
                raw.get("model_roots"), source_root, manifest_dir,
                "pdks[%d].model_roots" % index,
            ),
            "startup_extensions": _relocatable_members(
                raw.get("startup_extensions"), source_root, manifest_dir,
                "pdks[%d].startup_extensions" % index,
            ),
            "adapter_policy": adapter_policy,
        })
    if not pdks:
        raise CleanroomError("manifest contains no PDKs")
    for design in designs:
        unknown = sorted(set(design["pdk_ids"]) - pdk_ids)
        if unknown:
            raise CleanroomError("design %s references unknown PDKs: %s" % (
                design["id"], ", ".join(unknown),
            ))
        if not design["pdk_ids"]:
            raise CleanroomError("design %s must select at least one PDK" % design["id"])

    return {
        "schema_version": SCHEMA_MANIFEST,
        "manifest_path": manifest_path,
        "source_cds_lib": source_cds_lib,
        "installation_cds_lib": installation_cds_lib,
        "cadence": {
            "search_roots": search_roots,
            "license_file": cadence.get("license_file"),
        },
        "design_libraries": designs,
        "pdks": pdks,
    }


def _default_work_root():
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    return os.path.join("/var/tmp", _contract_text("crescendo-cleanroom-%s-%s") % (stamp, uuid.uuid4().hex[:8]))


def validate_work_root(path, source_roots, allowed_parents=None):
    path = os.path.abspath(os.path.expanduser(path))
    if os.path.lexists(path):
        raise CleanroomError("clean-room work root must not already exist: %s" % path)
    if os.path.realpath(path) != path:
        raise CleanroomError("clean-room work root may not traverse symlinks: %s" % path)
    allowed = tuple(os.path.realpath(value) for value in (allowed_parents or DEFAULT_WORK_PARENTS))
    if not any(_is_under(path, root) for root in allowed):
        raise CleanroomError("clean-room work root must be beneath: %s" % ", ".join(allowed))
    if not os.path.basename(path).startswith(_contract_text("crescendo-cleanroom-")):
        raise CleanroomError(_contract_text("clean-room work root basename must start with crescendo-cleanroom-"))
    for source in source_roots:
        if _is_under(path, source) or _is_under(source, path):
            raise CleanroomError("clean-room work root overlaps protected source: %s" % source)
    return path


def build_plan(manifest, wheel_path, work_root=None):
    wheel_path = os.path.abspath(os.path.expanduser(wheel_path))
    if not os.path.isfile(wheel_path) or not wheel_path.lower().endswith(".whl"):
        raise CleanroomError("--wheel must identify a prebuilt wheel")
    source_roots = [manifest["source_cds_lib"], wheel_path]
    source_roots.extend(row["source_root"] for row in manifest["design_libraries"])
    source_roots.extend(row["source_root"] for row in manifest["pdks"])
    work_root = validate_work_root(work_root or _default_work_root(), source_roots)
    paths = {
        "root": work_root,
        "input": os.path.join(work_root, "input"),
        "home": os.path.join(work_root, "home"),
        "xdg_config": os.path.join(work_root, "xdg", "config"),
        "xdg_cache": os.path.join(work_root, "xdg", "cache"),
        "xdg_state": os.path.join(work_root, "xdg", "state"),
        "tmp": os.path.join(work_root, "tmp"),
        "run_root": os.path.join(work_root, "runs"),
        "cwd": os.path.join(work_root, "execute"),
        "venv": os.path.join(work_root, "venv"),
        "relocated": os.path.join(work_root, "relocated"),
        "evidence": os.path.join(work_root, "evidence"),
    }
    cases = []
    for design in manifest["design_libraries"]:
        destination = os.path.join(paths["relocated"], "designs", design["id"])
        for setup in design["setups"]:
            cases.append({
                "id": setup["id"], "backend": setup["backend"],
                "design_id": design["id"], "design_library": design["name"],
                "pdk_ids": list(design["pdk_ids"]),
                "source_setup": os.path.join(design["source_root"], setup["relative_path"]),
                "relocated_setup": os.path.join(destination, setup["relative_path"]),
            })
    return {
        "schema_version": SCHEMA_PLAN,
        "dry_run": True,
        "manifest": manifest,
        "wheel": {"source": wheel_path, "staged": os.path.join(paths["input"], os.path.basename(wheel_path))},
        "paths": paths,
        "execution_identity": _default_execution_identity(),
        "cases": cases,
        "commands": [
            [sys.executable, "-m", "venv", paths["venv"]],
            [os.path.join(paths["venv"], "bin", "python"), "-m", "pip", "install", "--no-index", "--no-deps", os.path.join(paths["input"], os.path.basename(wheel_path))],
            [os.path.join(paths["venv"], "bin", _contract_text("crescendo")), "doctor"],
        ],
    }


def _case_command(plan, case, stage):
    executable = os.path.join(plan["paths"]["venv"], "bin", _contract_text("crescendo"))
    if stage == "inspect":
        return [
            executable, "inspect-setup", case["relocated_setup"],
            "--backend", case["backend"],
        ]
    return [
        executable, "--config", plan["generated"]["config"], "prepare",
        case["relocated_setup"], "--backend", case["backend"],
    ]


def _default_execution_identity():
    if os.geteuid() != 0:
        return {"uid": os.geteuid(), "gid": os.getegid(), "user": None, "dropped": False}
    try:
        account = pwd.getpwnam("nobody")
    except KeyError:
        raise CleanroomError("root execution requires an unprivileged nobody account")
    if account.pw_uid == 0 or account.pw_gid == 0:
        raise CleanroomError("the nobody account must be unprivileged")
    return {
        "uid": int(account.pw_uid), "gid": int(account.pw_gid),
        "user": account.pw_name, "dropped": True,
    }


def _copy_relocated_tree(source, destination):
    shutil.copytree(source, destination, symlinks=True)
    source_copy = _tree_fingerprint(source, include_mode=False)
    destination_copy = _tree_fingerprint(destination, include_mode=False)
    if source_copy["sha256"] != destination_copy["sha256"]:
        raise CleanroomError("relocated copy content does not match its source: %s" % source)
    source_real = os.path.realpath(source)
    for current, directories, files in os.walk(destination, topdown=True, followlinks=False):
        for name in list(directories) + list(files):
            candidate = os.path.join(current, name)
            if not os.path.islink(candidate):
                continue
            target = os.readlink(candidate)
            source_candidate = os.path.join(source, os.path.relpath(candidate, destination))
            source_target = os.path.realpath(os.path.join(os.path.dirname(source_candidate), target))
            if not _is_under(source_target, source_real):
                raise CleanroomError("relocated tree contains an external symlink: %s -> %s" % (
                    source_candidate, target,
                ))
            if os.path.isabs(target):
                relocated_target = os.path.join(destination, os.path.relpath(source_target, source_real))
                os.unlink(candidate)
                os.symlink(os.path.relpath(relocated_target, os.path.dirname(candidate)), candidate)


def _make_tree_read_only(path):
    for current, directories, files in os.walk(path, topdown=False, followlinks=False):
        for name in files:
            candidate = os.path.join(current, name)
            if os.path.islink(candidate):
                continue
            mode = stat.S_IMODE(os.stat(candidate).st_mode)
            os.chmod(candidate, (mode & 0o111) | 0o444)
        for name in directories:
            candidate = os.path.join(current, name)
            if not os.path.islink(candidate):
                os.chmod(candidate, 0o555)
        os.chmod(current, 0o555)


def _write_runtime_files(plan):
    paths = plan["paths"]
    manifest = plan["manifest"]
    for path in paths.values():
        if path == paths["root"]:
            continue
        if not os.path.isdir(path):
            os.makedirs(path, 0o700)
    shutil.copy2(plan["wheel"]["source"], plan["wheel"]["staged"])

    library_paths = {}
    profiles = []
    for design in manifest["design_libraries"]:
        destination = os.path.join(paths["relocated"], "designs", design["id"])
        _copy_relocated_tree(design["source_root"], destination)
        library_paths[design["name"]] = os.path.normpath(os.path.join(
            destination, design["library_relative"],
        ))
        _make_tree_read_only(destination)
    for pdk in manifest["pdks"]:
        destination = os.path.join(paths["relocated"], "pdks", pdk["id"])
        _copy_relocated_tree(pdk["source_root"], destination)
        for library in pdk["libraries"]:
            library_paths[library["name"]] = os.path.normpath(os.path.join(
                destination, library["relative"],
            ))
        profiles.append({
            "id": pdk["id"], "name": pdk["name"], "package_root": destination,
            "reference_libraries": list(pdk["reference_libraries"]),
            "technology_libraries": list(pdk["technology_libraries"]),
            "model_roots": [os.path.join(destination, row["relative"]) for row in pdk["model_roots"]],
            "model_path_remaps": [{
                "source": pdk["source_root"],
                "destination": destination,
            }],
            "startup_extensions": [
                os.path.join(destination, row["relative"]) for row in pdk["startup_extensions"]
            ],
            "adapter_policy": copy.deepcopy(pdk["adapter_policy"]),
        })
        _make_tree_read_only(destination)

    cds_lib = os.path.join(paths["input"], "cds.lib")
    with open(cds_lib, "w") as stream:
        stream.write("# Generated clean-room mapping; original cds.lib is never included.\n")
        stream.write("SOFTINCLUDE %s\n" % _cds_path(manifest["installation_cds_lib"]))
        for library in sorted(library_paths):
            stream.write("DEFINE %s %s\n" % (library, _cds_path(library_paths[library])))
    os.chmod(cds_lib, 0o444)

    config_path = os.path.join(paths["input"], _contract_text("optimizer.json"))
    config = {
        "cadence": {
            "setup_mode": "auto",
            "search_roots": list(manifest["cadence"]["search_roots"]),
            "pdk_profiles": profiles,
        },
        "runtime": {"run_root": paths["run_root"], "max_parallel_jobs": 1},
        "safety": {
            "allow_source_writes": False,
            "require_private_workspace": True,
            "protect_existing_processes": True,
        },
    }
    _atomic_write_json(config_path, config)
    plan["generated"] = {"cds_lib": cds_lib, "config": config_path}
    _prepare_execution_permissions(plan)


def _cds_path(path):
    return json.dumps(os.path.abspath(path), ensure_ascii=True)


def _prepare_execution_permissions(plan):
    identity = plan["execution_identity"]
    uid = int(identity["uid"])
    gid = int(identity["gid"])
    paths = plan["paths"]
    os.chmod(paths["root"], 0o755)
    xdg_parent = os.path.dirname(paths["xdg_config"])
    if os.path.isdir(xdg_parent):
        os.chmod(xdg_parent, 0o755)
    os.chmod(paths["relocated"], 0o555)
    for name in ("home", "xdg_config", "xdg_cache", "xdg_state", "tmp", "run_root", "cwd", "venv"):
        path = paths[name]
        if identity.get("dropped"):
            os.chown(path, uid, gid)
        os.chmod(path, 0o700)
    os.chmod(paths["input"], 0o555)
    for name in os.listdir(paths["input"]):
        candidate = os.path.join(paths["input"], name)
        if os.path.isfile(candidate):
            os.chmod(candidate, 0o444)


def _process_identity(pid):
    try:
        with open("/proc/%d/stat" % int(pid), "r") as stream:
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
        return {"pid": int(pid), "start_ticks": int(fields[19]), "pgrp": int(fields[2])}
    except (TypeError, ValueError):
        return None


def _identity_alive(identity):
    current = _process_identity(identity.get("pid"))
    return bool(current and current.get("start_ticks") == identity.get("start_ticks"))


def _token_processes(token):
    marker = (TOKEN_ENVIRONMENT + "=" + str(token)).encode("utf-8")
    rows = []
    try:
        entries = os.listdir("/proc")
    except OSError:
        return rows
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open("/proc/%s/environ" % entry, "rb") as stream:
                values = stream.read().split(b"\0")
        except (IOError, OSError):
            continue
        if marker not in values:
            continue
        identity = _process_identity(int(entry))
        if identity:
            rows.append(identity)
    return rows


def _signal_identity(identity, signum):
    if not _identity_alive(identity):
        return False
    try:
        os.kill(identity["pid"], signum)
        return True
    except OSError:
        return False


def _cleanup_owned(token, observed, grace_seconds=2.0):
    identities = dict(((row["pid"], row["start_ticks"]), row) for row in observed)
    for row in _token_processes(token):
        identities[(row["pid"], row["start_ticks"])] = row
    sent_term = []
    for identity in identities.values():
        if _signal_identity(identity, signal.SIGTERM):
            sent_term.append(copy.deepcopy(identity))
    deadline = time.time() + max(0.0, float(grace_seconds))
    while time.time() < deadline and any(_identity_alive(row) for row in identities.values()):
        time.sleep(0.05)
    sent_kill = []
    for identity in identities.values():
        if _signal_identity(identity, signal.SIGKILL):
            sent_kill.append(copy.deepcopy(identity))
    remaining = [row for row in identities.values() if _identity_alive(row)]
    return {
        "token": token,
        "observed": sorted(identities.values(), key=lambda row: (row["pid"], row["start_ticks"])),
        "sent_term": sent_term,
        "sent_kill": sent_kill,
        "remaining": remaining,
        "ok": not remaining,
    }


def _clean_environment(plan, token):
    paths = plan["paths"]
    path = os.pathsep.join((
        os.path.join(paths["venv"], "bin"), "/usr/local/bin", "/usr/bin", "/bin",
    ))
    environment = {
        "HOME": paths["home"],
        "LANG": "en_US.utf8",
        "LC_ALL": "en_US.utf8",
        "PATH": path,
        "TMPDIR": paths["tmp"],
        "XDG_CACHE_HOME": paths["xdg_cache"],
        "XDG_CONFIG_HOME": paths["xdg_config"],
        "XDG_STATE_HOME": paths["xdg_state"],
        _contract_text("CRESCENDO_RUN_ROOT"): paths["run_root"],
        _contract_text("CRESCENDO_CDS_LIB"): plan["generated"]["cds_lib"],
        _contract_text("CRESCENDO_USER_CONFIG"): os.path.join(paths["xdg_config"], _config_home(), _contract_text("config.json")),
        TOKEN_ENVIRONMENT: token,
    }
    license_value = plan["manifest"]["cadence"].get("license_file")
    if license_value:
        environment[_contract_text("CRESCENDO_LICENSE_FILE")] = str(license_value)
    from .runtime import child_environment as runtime_environment
    return runtime_environment(environment)


def _cadence_runtime_preflight(doctor, environment):
    cadence = ((doctor or {}).get("config") or {}).get("cadence") or {}
    cadence_root = cadence.get("cadence_root")
    if not cadence_root or not os.path.isdir(cadence_root):
        raise CleanroomError("EDA doctor did not resolve a readable Cadence root")
    additions = []
    for relative in ("tools.lnx86/dfII/bin", "tools.lnx86/dfII/bin/64bit"):
        candidate = os.path.join(cadence_root, *relative.split("/"))
        if os.path.isdir(candidate) and os.access(candidate, os.X_OK):
            additions.append(os.path.realpath(candidate))
    updated = dict(environment)
    current = updated.get("PATH") or ""
    updated["PATH"] = os.pathsep.join(additions + ([current] if current else []))
    tools = []
    for name in ("cpDataDir", "psf"):
        path = shutil.which(name, path=updated["PATH"])
        ok = bool(
            path and os.path.isfile(path) and os.access(path, os.X_OK)
            and _is_under(path, cadence_root)
        )
        tools.append({
            "name": name, "path": os.path.abspath(path) if path else None,
            "realpath": os.path.realpath(path) if path else None,
            "ok": ok,
        })
    result = {
        "cadence_root": os.path.realpath(cadence_root),
        "relative_search_directories": [
            "tools.lnx86/dfII/bin", "tools.lnx86/dfII/bin/64bit",
        ],
        "path_additions": additions,
        "tools": tools,
        "ok": bool(additions) and all(row["ok"] for row in tools),
    }
    if not result["ok"]:
        missing = [row["name"] for row in tools if not row["ok"]]
        raise CleanroomError("Cadence runtime preflight is missing: %s" % ", ".join(missing))
    return updated, result


def _child_setup(identity):
    os.setsid()
    if identity.get("dropped"):
        os.setgroups([])
        os.setgid(int(identity["gid"]))
        os.setuid(int(identity["uid"]))


def _run_owned(argv, cwd, environment, evidence_root, index, timeout_seconds,
               execution_identity=None):
    name = "%03d-%s" % (index, re.sub(r"[^A-Za-z0-9._-]+", "-", os.path.basename(argv[0])))
    stdout_path = os.path.join(evidence_root, name + ".stdout")
    stderr_path = os.path.join(evidence_root, name + ".stderr")
    token = environment[TOKEN_ENVIRONMENT]
    observed = {}
    started = time.time()
    timed_out = False
    with open(stdout_path, "wb") as stdout, open(stderr_path, "wb") as stderr:
        process = subprocess.Popen(
            argv, cwd=cwd, env=environment, stdout=stdout, stderr=stderr,
            preexec_fn=lambda: _child_setup(execution_identity or {
                "uid": os.geteuid(), "gid": os.getegid(), "dropped": False,
            }),
        )
        launcher = _process_identity(process.pid)
        deadline = started + float(timeout_seconds)
        while process.poll() is None:
            for row in _token_processes(token):
                observed[(row["pid"], row["start_ticks"])] = row
            if time.time() >= deadline:
                timed_out = True
                if launcher and _identity_alive(launcher):
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except OSError:
                        _signal_identity(launcher, signal.SIGTERM)
                break
            time.sleep(0.1)
        if timed_out:
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                if launcher and _identity_alive(launcher):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except OSError:
                        _signal_identity(launcher, signal.SIGKILL)
                process.wait()
        returncode = process.wait()
    cleanup = _cleanup_owned(token, list(observed.values()))
    metadata = {
        "argv": list(argv), "cwd": cwd, "environment_keys": sorted(environment),
        "returncode": returncode, "timed_out": timed_out,
        "elapsed_seconds": time.time() - started,
        "stdout": stdout_path, "stderr": stderr_path,
        "launcher": launcher, "owned_cleanup": cleanup,
        "execution_identity": copy.deepcopy(execution_identity),
    }
    _atomic_write_json(os.path.join(evidence_root, name + ".json"), metadata)
    if timed_out:
        raise CleanroomError("clean-room command timed out: %s" % " ".join(argv))
    if returncode != 0:
        raise CleanroomError("clean-room command failed with %d: %s" % (returncode, " ".join(argv)))
    if not cleanup["ok"]:
        raise CleanroomError("clean-room command left owned processes running")
    return metadata


def _read_command_json(metadata):
    try:
        with open(metadata["stdout"], "r") as stream:
            return json.load(stream)
    except (IOError, OSError, ValueError) as error:
        raise CleanroomError("command did not produce JSON: %s (%s)" % (metadata["stdout"], error))


def _assert_no_source_leak(value, source_roots, label):
    text = _canonical_json(value)
    leaked = sorted(root for root in source_roots if root and root in text)
    if leaked:
        raise CleanroomError("%s leaks original source paths: %s" % (label, ", ".join(leaked)))


def _prepared_source_leak_view(value):
    sanitized = copy.deepcopy(value)
    eda_context = sanitized.get("eda_context") or {}
    eda_context.pop("external_environment_diagnostics", None)
    pdk = eda_context.get("pdk") or {}
    for context in pdk.get("contexts") or []:
        for remap in context.get("model_path_remaps") or []:
            if isinstance(remap, dict) and remap.get("source"):
                remap["source"] = "<relocation-source-root>"
    return sanitized


def execute_plan(plan, stage="prepare", timeout_seconds=1800):
    if stage not in ("inspect", "prepare"):
        raise CleanroomError("stage must be inspect or prepare")
    root = plan["paths"]["root"]
    os.makedirs(root, 0o700)
    report = {
        "schema_version": SCHEMA_REPORT,
        "status": "running", "stage": stage, "root": root,
        "started_at": time.time(), "cases": [], "commands": [],
    }
    source_paths = [plan["manifest"]["source_cds_lib"], plan["wheel"]["source"]]
    source_paths.extend(row["source_root"] for row in plan["manifest"]["design_libraries"])
    source_paths.extend(row["source_root"] for row in plan["manifest"]["pdks"])
    before = dict((path, _tree_fingerprint(path, include_mode=True)) for path in sorted(set(source_paths)))
    report["source_fingerprints_before"] = before
    failure = None
    token = _contract_text("crescendo-cleanroom-%s") % uuid.uuid4().hex
    try:
        _write_runtime_files(plan)
        relocated_paths = [
            os.path.join(plan["paths"]["relocated"], "designs", row["id"])
            for row in plan["manifest"]["design_libraries"]
        ] + [
            os.path.join(plan["paths"]["relocated"], "pdks", row["id"])
            for row in plan["manifest"]["pdks"]
        ]
        relocated_before = dict(
            (path, _tree_fingerprint(path, include_mode=True)) for path in relocated_paths
        )
        report["relocated_fingerprints_before"] = relocated_before
        environment = _clean_environment(plan, token)
        evidence = plan["paths"]["evidence"]
        command_index = 0

        command_index += 1
        report["commands"].append(_run_owned(
            [sys.executable, "-m", "venv", plan["paths"]["venv"]],
            plan["paths"]["cwd"], environment, evidence, command_index, 300,
            execution_identity=plan["execution_identity"],
        ))
        command_index += 1
        report["commands"].append(_run_owned(
            [
                os.path.join(plan["paths"]["venv"], "bin", "python"), "-m", "pip",
                "install", "--no-index", "--no-deps", plan["wheel"]["staged"],
            ],
            plan["paths"]["cwd"], environment, evidence, command_index, 300,
            execution_identity=plan["execution_identity"],
        ))
        command_index += 1
        origin_metadata = _run_owned(
            [
                os.path.join(plan["paths"]["venv"], "bin", "python"), "-c",
                "import json,crescendo_engine; print(json.dumps({'module': crescendo_engine.__file__}))",
            ],
            plan["paths"]["cwd"], environment, evidence, command_index, 60,
            execution_identity=plan["execution_identity"],
        )
        report["commands"].append(origin_metadata)
        origin = _read_command_json(origin_metadata)
        if not _is_under(origin.get("module"), plan["paths"]["venv"]):
            raise CleanroomError("installed module was imported outside the clean venv")
        report["installed_origin"] = origin

        command_index += 1
        doctor_metadata = _run_owned(
            [
                os.path.join(plan["paths"]["venv"], "bin", _contract_text("crescendo")),
                "--config", plan["generated"]["config"], "doctor",
            ],
            plan["paths"]["cwd"], environment, evidence, command_index, 180,
            execution_identity=plan["execution_identity"],
        )
        report["commands"].append(doctor_metadata)
        doctor = _read_command_json(doctor_metadata)
        if not doctor.get("ok"):
            raise CleanroomError("clean-room EDA doctor did not pass")
        report["doctor"] = doctor
        environment, runtime_preflight = _cadence_runtime_preflight(doctor, environment)
        report["cadence_runtime_preflight"] = runtime_preflight

        original_roots = [
            row["source_root"] for row in plan["manifest"]["design_libraries"]
        ] + [row["source_root"] for row in plan["manifest"]["pdks"]]
        for case in plan["cases"]:
            command_index += 1
            argv = _case_command(plan, case, stage)
            metadata = _run_owned(
                argv, plan["paths"]["cwd"], environment, evidence,
                command_index, timeout_seconds,
                execution_identity=plan["execution_identity"],
            )
            report["commands"].append(metadata)
            result = _read_command_json(metadata)
            leak_view = _prepared_source_leak_view(result) if stage == "prepare" else result
            _assert_no_source_leak(leak_view, original_roots, case["id"])
            setup = result.get("setup") if stage == "prepare" else result
            if (setup or {}).get("backend") != case["backend"]:
                raise CleanroomError(
                    "case %s did not resolve as %s" % (case["id"], case["backend"])
                )
            if stage == "prepare":
                pdk = ((result.get("eda_context") or {}).get("pdk") or {})
                if not pdk.get("ok") or pdk.get("blocking"):
                    raise CleanroomError("case %s has a blocking PDK context" % case["id"])
            report["cases"].append({
                "id": case["id"], "backend": case["backend"], "ok": True,
                "pdk_ids": list(case["pdk_ids"]), "result": result,
            })
        report["status"] = "passed"
    except Exception as error:
        failure = error
        report["status"] = "failed"
        report["error"] = "%s: %s" % (error.__class__.__name__, error)
    finally:
        final_cleanup = _cleanup_owned(token, [])
        report["final_owned_cleanup"] = final_cleanup
        after = dict((path, _tree_fingerprint(path, include_mode=True)) for path in sorted(set(source_paths)))
        report["source_fingerprints_after"] = after
        mutations = [
            path for path in before
            if before[path]["sha256"] != after[path]["sha256"]
        ]
        report["source_mutations"] = mutations
        if mutations:
            report["status"] = "failed"
            report["error"] = "protected source content changed: %s" % ", ".join(mutations)
            failure = CleanroomError(report["error"])
        if report.get("relocated_fingerprints_before"):
            relocated_after = dict(
                (path, _tree_fingerprint(path, include_mode=True))
                for path in report["relocated_fingerprints_before"]
            )
            report["relocated_fingerprints_after"] = relocated_after
            relocated_mutations = [
                path for path in report["relocated_fingerprints_before"]
                if report["relocated_fingerprints_before"][path]["sha256"]
                != relocated_after[path]["sha256"]
            ]
            report["relocated_mutations"] = relocated_mutations
            if relocated_mutations:
                report["status"] = "failed"
                report["error"] = "read-only relocated content changed: %s" % ", ".join(
                    relocated_mutations,
                )
                failure = CleanroomError(report["error"])
        if not final_cleanup["ok"]:
            report["status"] = "failed"
            report["error"] = "owned process cleanup could not be proven"
            failure = CleanroomError(report["error"])
        report["finished_at"] = time.time()
        report["elapsed_seconds"] = report["finished_at"] - report["started_at"]
        _atomic_write_json(os.path.join(root, _contract_text("cleanroom-report.json")), report)
    if failure:
        raise failure
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Relocate Maestro designs and PDKs into a fail-closed clean-room",
    )
    parser.add_argument("manifest")
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--work-root", default=None)
    parser.add_argument("--execute", action="store_true", help="execute; default is dry-run")
    parser.add_argument("--stage", choices=("inspect", "prepare"), default="prepare")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    args = parser.parse_args(argv)
    try:
        manifest_path = os.path.abspath(os.path.expanduser(args.manifest))
        manifest = normalize_manifest(_load_json(manifest_path), manifest_path)
        plan = build_plan(manifest, args.wheel, args.work_root)
        if not args.execute:
            print(json.dumps(plan, indent=2, sort_keys=True))
            return 0
        report = execute_plan(plan, stage=args.stage, timeout_seconds=args.timeout_seconds)
        print(json.dumps({
            "schema_version": SCHEMA_REPORT,
            "status": report["status"], "root": report["root"],
            "report": os.path.join(report["root"], _contract_text("cleanroom-report.json")),
            "case_count": len(report["cases"]),
        }, indent=2, sort_keys=True))
        return 0
    except (CleanroomError, IOError, OSError, ValueError) as error:
        print("clean-room validation failed: %s: %s" % (
            error.__class__.__name__, error,
        ), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
