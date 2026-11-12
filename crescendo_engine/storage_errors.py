"""Explain local write failures without changing storage or exception identity."""
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import errno
import os


def is_storage_full_error(error):
    return isinstance(error, OSError) and error.errno in (
        errno.ENOSPC, getattr(errno, "EDQUOT", 122),
    )


def annotate_storage_error(error, path, operation):
    """Retain a destination for flush/fsync errors that have no filename."""
    if is_storage_full_error(error):
        if not getattr(error, _contract_text("cr_storage_path"), None):
            setattr(error, _contract_text("cr_storage_path"), os.path.abspath(os.path.expanduser(path)))
        if not getattr(error, _contract_text("cr_storage_operation"), None):
            setattr(error, _contract_text("cr_storage_operation"), operation)
    return error


def format_task_error(error, phase, config=None):
    """Keep normal errors unchanged; distinguish quota from filesystem limits."""
    message = "{}: {}: {}".format(phase, error.__class__.__name__, error)
    if not is_storage_full_error(error):
        return message
    if error.errno == getattr(errno, "EDQUOT", 122):
        explanation = (
            "The user/group disk quota for space or file count was exceeded; "
            "free filesystem space does not rule out this quota."
        )
    else:
        explanation = (
            "The destination filesystem has no available space or inodes "
            "for this write."
        )
    details = [message, explanation]
    operation = getattr(error, _contract_text("cr_storage_operation"), None)
    if operation:
        details.append("Write operation: {}.".format(operation))
    path = getattr(error, "filename", None) or getattr(error, _contract_text("cr_storage_path"), None)
    if path:
        details.append("Write path: {}".format(os.fsdecode(path)))
    else:
        runtime = (config or {}).get("runtime") or {}
        run_root = runtime.get("run_root")
        probe_root = runtime.get("hierarchy_probe_root")
        if run_root:
            details.append("Configured run root: {}".format(run_root))
        if not probe_root and run_root:
            probe_root = os.path.join(run_root, "probes")
        if probe_root:
            details.append("Configured hierarchy probe root: {}".format(probe_root))
    details.append("Free space/quota at this location or configure another persistent storage location, then retry.")
    return "\n".join(details)
