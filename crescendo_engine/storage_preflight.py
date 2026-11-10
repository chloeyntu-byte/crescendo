from __future__ import print_function

import json
import math
import os
import shutil

from .workspace import (
    _library_copy_ignore, _source_fingerprint_scopes, _workspace_copy_cells,
    copy_ignore, resolve_copy_libraries, reference_copy_ignore, workspace_reference_plan,
    build_occurrence_clone_plan, parse_cds_defines,
)


MIB = 1024 * 1024
MIN_CHECKPOINT_HEADROOM_BYTES = 16 * MIB


class StoragePreflightError(RuntimeError):
    pass


def _nearest_existing_ancestor(path):
    current = os.path.abspath(os.path.expanduser(path))
    while not os.path.exists(current):
        parent = os.path.dirname(current)
        if parent == current:
            raise StoragePreflightError(
                "cannot locate an existing filesystem ancestor for %s" % path
            )
        current = parent
    if not os.path.isdir(current):
        current = os.path.dirname(current)
    return current


def _materialized_tree_bytes(source, ignore=None):
    source = os.path.abspath(source)

    def visit(directory, ancestors):
        real_directory = os.path.realpath(directory)
        if real_directory in ancestors:
            raise StoragePreflightError(
                "workspace source contains a symbolic-link directory cycle: %s" % directory
            )
        try:
            names = sorted(os.listdir(directory))
        except OSError as error:
            raise StoragePreflightError(
                "workspace source cannot be sized: %s: %s" % (directory, error)
            )
        ignored = (ignore or copy_ignore)(directory, names)
        next_ancestors = ancestors + (real_directory,)
        total = 0
        for name in names:
            if name in ignored:
                continue
            path = os.path.join(directory, name)
            if os.path.isdir(path):
                total += visit(path, next_ancestors)
                continue
            if os.path.isfile(path):
                try:
                    total += os.path.getsize(path)
                except OSError as error:
                    raise StoragePreflightError(
                        "workspace source file cannot be sized: %s: %s" % (path, error)
                    )
                continue
            raise StoragePreflightError(
                "workspace source contains an unsupported filesystem entry: %s" % path
            )
        return total

    return visit(source, ())


def estimate_workspace_copy_bytes(setup, hierarchy, config=None, parameters=None):
    from .same_library_views import enabled as same_library_enabled
    same_library = same_library_enabled(config, parameters)
    _primary, _source_cds, sources = resolve_copy_libraries(
        setup, hierarchy, config=config,
    )
    if same_library:
        # Only exact mutable views allocate OA data. Original library metadata,
        # opaque cells and unmodified ancestors remain read-only references.
        plan = workspace_reference_plan(setup, hierarchy, sources, _primary,
                                        ((config or {}).get("runtime") or {}).get("run_root") or os.getcwd(),
                                        parameters, same_library=True)
        estimate_root = os.path.join(((config or {}).get("runtime") or {}).get("run_root") or os.getcwd(),
                                     "_cr_estimate_only")
        workspace = dict(plan, root=estimate_root, copy_policy="same_library_views",
                         same_library_objects=True, source_libraries=sources,
                         copied_libraries=dict(sources),
                         private_library_names=dict((name, name) for name in sources))
        values = dict((row["parameter_id"], 0) for row in parameters or [])
        clones = build_occurrence_clone_plan(workspace, parameters or [], values, clone_state="mixed")
        setup_path = (setup.get("source") or {}).get("maestro_sdb") or (setup.get("source") or {}).get("setup_sdb")
        if not setup_path:
            from .workspace import _setup_database_path
            setup_path = _setup_database_path(setup)
        total = _materialized_tree_bytes(os.path.dirname(setup_path))
        for clone in clones:
            path = os.path.join(sources[clone["source_library"]], clone["source_cell"], clone.get("source_view") or "schematic")
            # Working, pristine and one snapshot; no copy of the containing lib.
            total += 3 * _materialized_tree_bytes(path)
        return total
    scopes = _source_fingerprint_scopes(setup, hierarchy, sources)
    cells = _workspace_copy_cells(setup, hierarchy, sources, scopes)
    plan = workspace_reference_plan(setup, hierarchy, sources, _primary,
                                    ((config or {}).get("runtime") or {}).get("run_root") or os.getcwd(),
                                    parameters, same_library=same_library) if parameters is not None else None
    total = sum(
        _materialized_tree_bytes(source, reference_copy_ignore(source, plan["copy_views"].get(library))
                                 if plan else _library_copy_ignore(source, cells.get(library)))
        for library, source in sorted(sources.items())
    )
    if plan:
        # Native occurrence clones also allocate views. Include those mutable
        # branches in headroom instead of pretending that initial Python copies
        # account for the complete private workspace cost.
        estimate_root = os.path.join(((config or {}).get("runtime") or {}).get("run_root") or os.getcwd(),
                                     "_cr_estimate_only", "libraries")
        workspace = dict(plan, root=os.path.dirname(estimate_root), copy_policy="same_library_views" if same_library else "source_reference",
                         source_libraries=sources,
                         copied_libraries=dict((library, os.path.join(estimate_root, library))
                                               for library in sources))
        values = dict((row["parameter_id"], 0) for row in parameters or [])
        clones = build_occurrence_clone_plan(workspace, parameters or [], values, clone_state="mixed")
        definitions = parse_cds_defines(_source_cds)
        definitions.update(sources)
        for clone in clones:
            views = [(clone["source_library"], clone["source_cell"], clone.get("source_view") or "schematic")]
            if not clone.get("same_library_view") and clone["master_view"] != views[0][2]:
                views.append((clone.get("symbol_source_library") or clone["source_library"],
                              clone.get("symbol_source_cell") or clone["source_cell"], clone["master_view"]))
            for library, cell, view in views:
                path = os.path.join(definitions.get(library) or "", cell, view)
                if not definitions.get(library) or not os.path.isdir(path):
                    raise StoragePreflightError("mutable occurrence view cannot be sized: %s/%s/%s" % (library, cell, view))
                total += _materialized_tree_bytes(path)
    return total


def _checkpoint_headroom_bytes(checkpoint):
    if checkpoint is None:
        serialized = 0
    else:
        try:
            serialized = len(
                json.dumps(checkpoint, separators=(",", ":"), sort_keys=True).encode("utf-8")
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise StoragePreflightError(
                "checkpoint cannot be sized before resume: %s" % error
            )
    return max(MIN_CHECKPOINT_HEADROOM_BYTES, serialized * 4)


def optimization_workspace_required(spec, checkpoint=None):
    effective_spec = (checkpoint or {}).get("spec") or spec or {}
    if str(effective_spec.get("evaluator", "real")).strip().lower() == "mock":
        return False
    state = (checkpoint or {}).get("state") or {}
    if state.get("terminal_state") == "completed":
        return False
    try:
        generations = int(effective_spec.get("generations", 10))
        next_generation = int(state.get("next_generation", 0))
    except (TypeError, ValueError, OverflowError):
        return True
    return next_generation < generations


def startup_storage_preflight(run_root, checkpoint_anchor_root, min_free_disk_mib,
                              setup=None, hierarchy=None, config=None,
                              checkpoint=None, workspace_required=True,
                              disk_usage=None, parameters=None):
    try:
        minimum_mib = float(min_free_disk_mib)
    except (TypeError, ValueError, OverflowError):
        raise StoragePreflightError("startup minimum free disk must be a finite positive number")
    if not math.isfinite(minimum_mib) or minimum_mib <= 0:
        raise StoragePreflightError("startup minimum free disk must be a finite positive number")

    workspace_bytes = 0
    source = (setup or {}).get("source") or {}
    workspace_estimate_available = bool(source.get("library_root"))
    if workspace_required and workspace_estimate_available:
        workspace_bytes = estimate_workspace_copy_bytes(
            setup, hierarchy or {}, config=config, parameters=parameters,
        )
    checkpoint_bytes = _checkpoint_headroom_bytes(checkpoint)
    minimum_bytes = int(minimum_mib * MIB)
    run_target = _nearest_existing_ancestor(run_root)
    anchor_target = _nearest_existing_ancestor(checkpoint_anchor_root or os.path.dirname(run_root))
    targets = [
        {
            "kind": "run",
            "requested_path": os.path.abspath(run_root),
            "probe_path": run_target,
            "headroom_bytes": workspace_bytes + checkpoint_bytes,
        },
        {
            "kind": "checkpoint_anchor",
            "requested_path": os.path.abspath(checkpoint_anchor_root or os.path.dirname(run_root)),
            "probe_path": anchor_target,
            "headroom_bytes": checkpoint_bytes * 2,
        },
    ]
    from .same_library_views import enabled as same_library_enabled
    if workspace_required and workspace_estimate_available and same_library_enabled(config, parameters):
        primary, source_cds, sources = resolve_copy_libraries(setup, hierarchy or {}, config=config)
        mutable_libraries = set([primary])
        for parameter in parameters or []:
            if parameter.get("backend", "schematic") == "schematic":
                mutable_libraries.add(parameter.get("owner_library") or parameter.get("design_library"))
        for library in sorted(value for value in mutable_libraries if value):
            path = sources.get(library)
            if not path:
                raise StoragePreflightError("mutable source library is unresolved: %s" % library)
            targets.append({"kind": "owned_cellviews:" + library, "requested_path": path,
                            "probe_path": _nearest_existing_ancestor(path), "headroom_bytes": workspace_bytes})
    usage_function = disk_usage or shutil.disk_usage
    filesystems = {}
    for target in targets:
        try:
            device = os.stat(target["probe_path"]).st_dev
        except OSError as error:
            raise StoragePreflightError(
                "cannot identify startup filesystem for %s: %s" % (
                    target["requested_path"], error,
                )
            )
        row = filesystems.setdefault(device, {
            "device": device,
            "probe_path": target["probe_path"],
            "minimum_free_bytes": minimum_bytes,
            "headroom_bytes": 0,
            "required_bytes": minimum_bytes,
            "kinds": [],
            "requested_paths": [],
        })
        row["headroom_bytes"] += target["headroom_bytes"]
        row["required_bytes"] += target["headroom_bytes"]
        row["kinds"].append(target["kind"])
        row["requested_paths"].append(target["requested_path"])

    results = []
    for device, row in sorted(filesystems.items(), key=lambda item: item[0]):
        try:
            usage = usage_function(row["probe_path"])
            free_bytes = int(usage.free)
        except (AttributeError, OSError, TypeError, ValueError) as error:
            raise StoragePreflightError(
                "cannot read startup disk usage for %s: %s" % (row["probe_path"], error)
            )
        result = dict(row)
        result.update({
            "free_bytes": free_bytes,
            "ok": free_bytes >= row["required_bytes"],
        })
        results.append(result)
    failures = [row for row in results if not row["ok"]]
    if failures:
        details = "; ".join(
            "%s has %.1f MiB free but %.1f MiB is required for %s" % (
                row["probe_path"], row["free_bytes"] / float(MIB),
                row["required_bytes"] / float(MIB), ",".join(row["kinds"]),
            )
            for row in failures
        )
        raise StoragePreflightError(
            "startup disk-space preflight failed before checkpoint/workspace creation: %s" % details
        )
    return {
        "schema_version": "crescendo.startup_storage_preflight.v1",
        "ok": True,
        "minimum_free_disk_mib": minimum_mib,
        "estimated_workspace_copy_bytes": workspace_bytes,
        "workspace_estimate_required": bool(workspace_required),
        "workspace_estimate_available": bool(
            not workspace_required or workspace_estimate_available
        ),
        "checkpoint_headroom_bytes": checkpoint_bytes,
        "filesystems": results,
    }
