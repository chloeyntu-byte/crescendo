"""Owned saved-Maestro snapshots; original OA and PDK remain guarded references."""
from __future__ import print_function

import copy
import fnmatch
import hashlib
import json
import os
import shutil
import stat
import uuid

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json, sha256_file


SCHEMA = _contract_text("crescendo.setup_baseline.v1")


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def origin_setup_path(setup):
    source = setup.get("source") or {}
    return source.get("setup_db") or source.get("maestro_sdb") or source.get("data_sdb")


def _view_relative(setup):
    directory = os.path.dirname(origin_setup_path(setup))
    return os.path.join("native", os.path.basename(os.path.dirname(os.path.dirname(directory))),
                        os.path.basename(os.path.dirname(directory)), os.path.basename(directory))


def _files(root):
    from .cadence import check_command_cancellation
    check_command_cancellation()
    if os.path.realpath(root) != root or not stat.S_ISDIR(os.lstat(root).st_mode):
        raise ValueError("Saved Maestro snapshot directory is not a real owned directory")
    rows = []
    for directory, children, names in os.walk(root):
        check_command_cancellation()
        children[:] = sorted(name for name in children if name != "results" and not _ignored(name))
        for name in children + sorted(names):
            check_command_cancellation()
            path = os.path.join(directory, name)
            if _ignored(name) or name == "results":
                continue
            mode = os.lstat(path).st_mode
            if (stat.S_ISLNK(mode) or not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)) or
                    (stat.S_ISREG(mode) and os.lstat(path).st_nlink != 1)):
                raise ValueError("Saved Maestro snapshot contains a link or special file: %s" % path)
            if stat.S_ISREG(mode):
                rows.append({"relative_path": os.path.relpath(path, root), "sha256": sha256_file(path)})
    return rows


def _ignored(name):
    return any(fnmatch.fnmatch(name, pattern) for pattern in
               ("*.cdslck", "*.cdslck.*", "CDS.log*", ".tmpADEDir_*", "*~", "*.bak*"))


def _regular(path):
    metadata = os.lstat(path)
    if os.path.realpath(path) != path or not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise ValueError("Saved Maestro snapshot metadata is not a regular single-link file")
    return path


def verify_setup_baseline(setup):
    baseline = setup.get("baseline")
    if not baseline:
        return None
    root = baseline.get("root")
    if (baseline.get("schema_version") != SCHEMA or not root or
            not os.path.isabs(root) or os.path.realpath(root) != root or
            os.path.basename(root) != baseline.get("baseline_id")):
        raise ValueError("Saved Maestro reference identity is malformed")
    manifest_path = os.path.join(root, _contract_text("manifest.json"))
    if sha256_file(_regular(manifest_path)) != baseline.get("manifest_sha256"):
        raise ValueError("Saved Maestro reference manifest changed")
    with open(manifest_path) as stream:
        manifest = json.load(stream)
    if (manifest.get("schema_version") != SCHEMA or manifest.get("baseline_id") != baseline.get("baseline_id") or
            manifest.get("owner_uid") != os.getuid() or os.stat(root).st_uid != os.getuid() or
            manifest.get("origin_canonical_path") != os.path.realpath(origin_setup_path(setup)) or
            manifest.get("origin_setup_path") != origin_setup_path(setup) or
            manifest.get("view_relative_path") != _view_relative(setup) or
            manifest.get("files") != _files(os.path.join(root, _view_relative(setup))) or
            _digest(manifest) != baseline.get("fingerprint")):
        raise ValueError("Saved Maestro reference contents changed")
    snapshot = os.path.join(root, _contract_text("setup.json"))
    if sha256_file(_regular(snapshot)) != manifest.get("setup_snapshot_sha256"):
        raise ValueError("Saved Maestro reference settings snapshot changed")
    expected = os.path.join(root, _view_relative(setup), manifest["setup_filename"])
    if baseline.get("setup_path") != expected:
        raise ValueError("Saved Maestro reference setup path changed")
    return baseline


def effective_setup_path(setup):
    baseline = verify_setup_baseline(setup)
    return baseline["setup_path"] if baseline else origin_setup_path(setup)


def effective_setup_dir(setup):
    return os.path.dirname(effective_setup_path(setup))


def freeze_setup_baseline(prepared, config):
    """Freeze saved native files and parsed scope semantics, without copying OA."""
    setup = prepared["setup"]
    if setup.get("backend") != "maestro" or setup.get("baseline"):
        verify_setup_baseline(setup)
        return setup.get("baseline")
    source_path = origin_setup_path(setup)
    # Source library aliases are existing read-only mapping semantics; owned
    # snapshot paths themselves must remain canonical and link-free.
    source_dir = os.path.realpath(os.path.dirname(source_path))
    before = _files(source_dir)
    expected = (setup.get("source") or {}).get("sha256")
    if expected and sha256_file(source_path) != expected:
        raise ValueError("Maestro changed before reference capture; Load and Inspect again")
    runtime = config.get("runtime") or {}
    parent = os.path.join(runtime.get("design_root") or runtime["run_root"], "setup_baselines")
    if not os.path.isdir(parent):
        os.makedirs(parent, mode=0o700)
    token = uuid.uuid4().hex
    root = os.path.realpath(os.path.join(parent, token))
    os.mkdir(root, 0o700)
    view = os.path.join(root, _view_relative(setup))
    try:
        os.makedirs(os.path.dirname(view))
        from .workspace import _copy_private_file
        shutil.copytree(source_dir, view, ignore=lambda directory, names: [
            name for name in names if name == "results" or _ignored(name)], copy_function=_copy_private_file)
        if before != _files(source_dir) or before != _files(view):
            raise ValueError("Maestro changed during reference capture; Load and Inspect again")
        # Prove the copied active.state and SDB describe the inspected scopes.
        from .ade_setup import inspect_setup
        copied = inspect_setup(os.path.join(view, os.path.basename(source_path)), backend="maestro")
        fields = ("mode", "tests", "corners", "global_variables", "model_groups", "execution_settings", "outputs")
        from .single_test import source_setup
        source = source_setup(setup)
        if any(copied.get(key) != source.get(key) for key in fields):
            raise ValueError("Saved Maestro reference differs from inspected settings; Load and Inspect again")
        snapshot = {"setup": copy.deepcopy(setup), "job_policy": copy.deepcopy(prepared.get("job_policy"))}
        atomic_write_json(os.path.join(root, _contract_text("setup.json")), snapshot)
        manifest = {"schema_version": SCHEMA, "baseline_id": token,
                    "owner_uid": os.getuid(), "origin_canonical_path": os.path.realpath(source_path),
                    "view_relative_path": _view_relative(setup),
                    "origin_setup_path": source_path, "setup_filename": os.path.basename(source_path),
                    "files": before, "setup_snapshot_sha256": sha256_file(os.path.join(root, _contract_text("setup.json")))}
        atomic_write_json(os.path.join(root, _contract_text("manifest.json")), manifest)
        baseline = {"schema_version": SCHEMA, "baseline_id": token, "root": root,
                    "setup_path": os.path.join(view, os.path.basename(source_path)),
                    "manifest_sha256": sha256_file(os.path.join(root, _contract_text("manifest.json"))),
                    "fingerprint": _digest(manifest)}
        setup["baseline"] = baseline
        return baseline
    except Exception:
        shutil.rmtree(root)
        raise


def load_bound_setup(path, config, backend="auto"):
    binding = (config or {}).get("_setup_baseline")
    if binding:
        original = binding.get("origin_setup_path")
        if os.path.realpath(path) == os.path.realpath(original or ""):
            setup = {"source": {"setup_db": original}, "baseline": binding["baseline"]}
            verify_setup_baseline(setup)
            with open(os.path.join(binding["baseline"]["root"], _contract_text("setup.json"))) as stream:
                snapshot = json.load(stream)
            setup = snapshot["setup"]
            if backend not in (None, "auto", setup.get("backend")):
                raise ValueError("Saved Maestro reference backend differs from requested backend")
            setup["baseline"] = copy.deepcopy(binding["baseline"])
            setup["baseline_job_policy"] = snapshot.get("job_policy")
            return setup
    from .ade_setup import inspect_setup
    return inspect_setup(path, backend=backend)


def persist_prepared_baseline(prepared):
    baseline = verify_setup_baseline(prepared.get("setup") or {})
    if not baseline:
        return None
    payload = copy.deepcopy(prepared)
    payload.pop("setup_baseline_snapshot", None)
    name = "prepared_%s.json" % uuid.uuid4().hex
    path = os.path.join(baseline["root"], name)
    atomic_write_json(path, payload)
    record = {"schema_version": SCHEMA, "baseline_id": baseline["baseline_id"],
              "path": path, "sha256": sha256_file(path)}
    prepared["setup_baseline_snapshot"] = record
    return record


def load_prepared_baseline(record):
    path = record.get("path")
    if (record.get("schema_version") != SCHEMA or not path or not os.path.isabs(path) or
            os.path.realpath(path) != path or sha256_file(_regular(path)) != record.get("sha256")):
        raise ValueError("Checkpoint saved Maestro reference snapshot is unavailable or changed")
    with open(path) as stream:
        prepared = json.load(stream)
    baseline = verify_setup_baseline(prepared.get("setup") or {})
    if (not baseline or baseline["baseline_id"] != record.get("baseline_id") or
            os.path.dirname(path) != baseline["root"]):
        raise ValueError("Checkpoint saved Maestro reference snapshot identity differs")
    prepared["setup_baseline_snapshot"] = copy.deepcopy(record)
    return prepared
