from __future__ import print_function

import copy
import hashlib
import json
import os

from .utils import sha256_file
from .workspace import source_design_identity, source_change_summary, verify_sources_unchanged, compact_source_guard


_SCHEMA_VERSION = "crescendo.prepared_freshness.v1"


def _canonical_fingerprint(payload):
    encoded = json.dumps(
        payload, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _loaded_identity(prepared, components=False):
    payload = {
        "schema_version": "crescendo.loaded_design_identity.v1",
        "setup": prepared.get("setup"),
        "hierarchy": prepared.get("hierarchy"),
        "parameters": prepared.get("parameters"),
        "outputs": prepared.get("outputs"),
    }
    # Store only a hash of the full catalogue. Recompute it on every true
    # freshness boundary, without duplicating the entire catalogue in each
    # checkpoint/workspace/validation context. Older v1 records compare the
    # same fingerprint and remain valid.
    result = {"schema_version": payload["schema_version"],
              "fingerprint": _canonical_fingerprint(payload)}
    if components:
        # Four small digests at publication; no second catalogue is retained.
        result["components"] = {key: _canonical_fingerprint(payload[key])
                                for key in ("setup", "hierarchy", "parameters", "outputs")}
    return result


def _setup_paths(prepared):
    setup = prepared.get("setup") or {}
    source = setup.get("source") or {}
    if setup.get("baseline"):
        from .setup_baseline import effective_setup_path, effective_setup_dir
        path, directory = effective_setup_path(setup), effective_setup_dir(setup)
        return {"setup_db": path, "maestro_sdb": path,
                "data_sdb": None, "active_state": os.path.join(directory, "active.state")}
    setup_dir = source.get("setup_dir")
    active_state = source.get("active_state")
    if active_state is None and setup_dir:
        active_state = os.path.join(setup_dir, "active.state")
    return {
        "setup_db": source.get("setup_db") or source.get("maestro_sdb") or source.get("data_sdb"),
        "maestro_sdb": source.get("maestro_sdb"),
        "data_sdb": source.get("data_sdb"),
        "active_state": active_state,
    }


def _file_identity(path):
    if not path:
        return {"path": None, "state": "not_configured"}
    path = os.path.abspath(os.path.expanduser(str(path)))
    if not os.path.lexists(path):
        return {"path": path, "realpath": os.path.realpath(path), "state": "missing"}
    if not os.path.isfile(path):
        return {"path": path, "realpath": os.path.realpath(path), "state": "not_a_file"}
    return {
        "path": path,
        "realpath": os.path.realpath(path),
        "state": "file",
        "size": os.path.getsize(path),
        "sha256": sha256_file(path),
    }


def _maestro_identity(prepared):
    return dict(
        (name, _file_identity(path))
        for name, path in sorted(_setup_paths(prepared).items())
    )


def capture_prepared_freshness(prepared):
    source_identity = prepared.get("source_identity")
    if not isinstance(source_identity, dict) or not source_identity.get("fingerprint"):
        raise ValueError("prepared source design identity is missing or malformed")
    return {
        "schema_version": _SCHEMA_VERSION,
        "loaded_identity": _loaded_identity(prepared, components=True),
        "maestro_files": _maestro_identity(prepared),
        "source_identity": copy.deepcopy(source_identity),
    }


def _source_change_diagnostics(prepared, expected, observed):
    """Use an authenticated live owner's snapshots only after a guard failed."""
    previous = dict((row.get("library"), row) for row in expected.get("libraries") or []
                    if isinstance(row, dict))
    current = dict((row.get("library"), row) for row in observed.get("libraries") or []
                   if isinstance(row, dict))
    changed_libraries = sorted(name for name in set(previous) | set(current)
                               if isinstance(name, str) and name and previous.get(name) != current.get(name))
    result = {"state": "library_only", "libraries": [{"library": name,
        "changes": {"state": "scope_details_unavailable", "scopes": []}}
        for name in changed_libraries]}
    reference = prepared.get("setup_private_session")
    if not reference:
        return result
    try:
        # Do not trust arbitrary workspace.json paths from a serialized setup.
        # _controller requires both durable ownership and this UI's registry.
        from .setup_private_session import _controller
        controller = _controller(reference)
        if ((controller.prepared.get("source_identity") or {}).get("fingerprint")
                != expected.get("fingerprint")):
            result["detail_state"] = "owned_snapshot_identity_differs"
            return result
        if not controller.lock.acquire(False):
            result["detail_state"] = "owned_workspace_busy"
            return result
        try:
            workspace = dict((key, copy.deepcopy(controller.workspace.get(key)))
                for key in ("source_libraries", "source_snapshots", "config_designs", "setup"))
        finally:
            controller.lock.release()
        guard = verify_sources_unchanged(workspace)
        result["source_guard"] = compact_source_guard(guard)
        details = dict((row["library"], row.get("changes") or {})
            for row in guard["libraries"] if not row.get("unchanged"))
        for row in result["libraries"]:
            if row["library"] in details:
                row["changes"] = details[row["library"]]
        result["state"] = "owned_workspace_scopes"
    except Exception as error:
        # Diagnostic failure never weakens or creates a freshness verdict.
        result["detail_state"] = "owned_scope_details_unavailable"
        result["detail_error"] = ("%s: %s" % (error.__class__.__name__, error))[:320]
    return result


def verify_prepared_freshness(prepared, config=None):
    expected = prepared.get("prepared_freshness") if isinstance(prepared, dict) else None
    if expected is None:
        return {"ok": True, "state": "not_recorded"}
    if not isinstance(expected, dict) or expected.get("schema_version") != _SCHEMA_VERSION:
        return {
            "ok": False,
            "state": "malformed",
            "reasons": ["prepared freshness identity is missing or malformed"],
        }

    reasons = []
    observed_loaded = _loaded_identity(prepared)
    expected_loaded = expected.get("loaded_identity") or {}
    loaded_changes = []
    if observed_loaded.get("fingerprint") != expected_loaded.get("fingerprint"):
        reasons.append("loaded hierarchy or parameter catalog changed")
        components = expected_loaded.get("components")
        if isinstance(components, dict):
            loaded_changes = [key for key in ("setup", "hierarchy", "parameters", "outputs")
                              if key in components and components[key] != _canonical_fingerprint(prepared.get(key))]

    try:
        observed_maestro = _maestro_identity(prepared)
    except Exception as error:
        observed_maestro = None
        reasons.append("Maestro setup files could not be re-read: %s: %s" % (
            error.__class__.__name__, error,
        ))
    if observed_maestro is not None:
        expected_maestro = expected.get("maestro_files") or {}
        names = sorted(set(observed_maestro) | set(expected_maestro))
        for name in names:
            if observed_maestro.get(name) != expected_maestro.get(name):
                reasons.append("%s changed" % name)

    try:
        observed_source = source_design_identity(
            prepared["setup"], prepared["hierarchy"], config=config,
        )
    except Exception as error:
        observed_source = None
        reasons.append("source dependency identity could not be re-read: %s: %s" % (
            error.__class__.__name__, error,
        ))
    expected_source = expected.get("source_identity") or {}
    source_changes = None
    if observed_source is not None and (
            observed_source.get("schema_version") != expected_source.get("schema_version")
            or observed_source.get("fingerprint") != expected_source.get("fingerprint")):
        source_changes = _source_change_diagnostics(prepared, expected_source, observed_source)
        if source_changes["libraries"]:
            reasons.append(source_change_summary({"libraries": [dict(row, unchanged=False)
                for row in source_changes["libraries"]]}))
        else:
            reasons.append("source dependency identity changed (changed library/scope unavailable)")

    return {
        "ok": not reasons,
        "state": "current" if not reasons else "stale",
        "reasons": reasons,
        "changed_catalog_components": loaded_changes,
        "source_changes": source_changes,
        "observed": {
            "loaded_identity": observed_loaded,
            "maestro_files": observed_maestro,
            "source_identity": observed_source,
        },
    }


def require_prepared_freshness(prepared, config=None, action="continue"):
    result = verify_prepared_freshness(prepared, config=config)
    if result.get("ok"):
        return result
    reasons = "; ".join(result.get("reasons") or ["prepared design identity changed"])
    changed = result.get("changed_catalog_components") or []
    if changed:
        reasons += " (catalog components: %s)" % ", ".join(changed)
    error = ValueError(
        "Cannot %s because the loaded design is stale: %s. "
        "Load and Inspect again before Validate or Start." % (action, reasons)
    )
    expected = prepared.get("prepared_freshness")
    expected = expected if isinstance(expected, dict) else {}
    observed = result.get("observed") or {}
    error.detail = {
        "failure_stage": "prepared_freshness", "execution_started": False, "native_run_invoked": False,
        "prepared_freshness": {
            "state": result.get("state"), "action": action,
            "reasons": [str(value)[:2200] for value in (result.get("reasons") or [])[:16]],
            "changed_catalog_components": changed,
            "loaded_fingerprint_before": (expected.get("loaded_identity") or {}).get("fingerprint"),
            "loaded_fingerprint_after": (observed.get("loaded_identity") or {}).get("fingerprint"),
            "source_fingerprint_before": (expected.get("source_identity") or {}).get("fingerprint"),
            "source_fingerprint_after": (observed.get("source_identity") or {}).get("fingerprint"),
        }}
    source_changes = result.get("source_changes") or {}
    if source_changes.get("source_guard"):
        error.detail["source_guard"] = copy.deepcopy(source_changes["source_guard"])
    elif source_changes:
        error.detail["prepared_freshness"]["source_changes"] = copy.deepcopy(source_changes)
    raise error
