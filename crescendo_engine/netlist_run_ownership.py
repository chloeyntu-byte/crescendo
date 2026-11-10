"""Reconcile only capture roots belonging to this optimizer run."""
import os
import re
import json

from .private_netlist import reconcile_preparation


def reconcile_run_preparations(run_root, request_cancel=True, timeout_seconds=12, solver_cleanup_proven=False):
    run_root = os.path.abspath(run_root)
    if os.path.realpath(run_root) != run_root:
        raise ValueError("Netlist run ownership root must be an independent directory")
    roots = [os.path.join(run_root, "private_netlist", "templates")]
    candidates = os.path.join(run_root, "candidates")
    if os.path.lexists(candidates):
        if os.path.islink(candidates) or not os.path.isdir(candidates):
            raise ValueError("Unsafe netlist candidate ownership directory")
        for name in sorted(os.listdir(candidates)):
            if not re.match(r"^g[0-9]{3,}_c[0-9]{4,}$", name):
                continue
            candidate = os.path.join(candidates, name)
            root = os.path.join(candidate, "simulation")
            if os.path.islink(candidate) or os.path.islink(root):
                raise ValueError("Unsafe netlist preparation ownership directory")
            if os.path.isdir(root) and os.path.exists(os.path.join(root, "numeric")):
                roots.append(root)
    rows = [reconcile_preparation(root, request_cancel=request_cancel,
                                  timeout_seconds=timeout_seconds) for root in roots]
    shared = []
    try:
        from .shared_maestro import owner_records
        from .private_netlist import _regular, MAX_METADATA_BYTES
        from .same_library_views import cleanup_same_library_views
        owners = owner_records(run_root)
        for owner in owners:
            row = {"token": owner["token"], "root": owner["root"], "cleanup_proven": False}
            if not solver_cleanup_proven:
                row["state"] = "retained_until_solver_termination_proven"
            elif not all(item.get("cleanup_proven") for item in rows):
                row["state"] = "retained_until_all_native_generation_stopped"
            else:
                path = os.path.join(owner["workspace_root"], "workspace.json")
                if os.path.lexists(path):
                    _regular(path, owner["root"], MAX_METADATA_BYTES)
                    with open(path, "r") as stream:
                        workspace = json.load(stream)
                    if workspace.get("root") != owner["workspace_root"]:
                        raise ValueError("Shared Maestro workspace identity does not match its epoch")
                    cleanup = cleanup_same_library_views(workspace, process_cleanup_proven=True)
                    row.update(cleanup_proven=cleanup.get("cleanup_proven") is True, view_cleanup=cleanup)
                else:
                    row.update(cleanup_proven=True, state="workspace_not_created")
            shared.append(row)
    except (OSError, ValueError) as error:
        shared.append({"cleanup_proven": False, "state": "ambiguous_shared_ownership", "reason": str(error)})
    sessions = {row["setup_private_session"]["session_id"]: row["setup_private_session"]
                for row in rows if row.get("setup_private_session")}
    return {"root": run_root, "cleanup_proven": all(row.get("cleanup_proven") for row in rows + shared),
            "preparations": rows, "shared_maestro_epochs": shared,
            "setup_private_sessions_retained_until_ui_close": list(sessions.values())}
