"""Prevent accidental GUI edits to this process's exact working view trees.

This is an idle permission guard, not isolation between processes with the same
Unix UID (or from root). The caller must serialize native writers and prove they
have stopped before restoring read-only permissions. No source library/cell
directory and no Cadence lock is changed by this module.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import contextlib
import os
import stat


class PrivateViewBusy(RuntimeError):
    """A retained native lock needs the owning editor/process to release it."""

    retryable = True

    def __init__(self, locks):
        self.locks = list(locks)
        self.lock_paths = [row["path"] for row in self.locks]
        self.view_paths = sorted(set(row["view_path"] for row in self.locks))
        RuntimeError.__init__(self,
            _contract_text("CRESCENDO-SUITE-LOCK: Working view is in use: %s. Close its Maestro working copy/config/schematic "
            "editor and retry; Load and Inspect is not required. Native locks were retained.")
            % ", ".join(self.view_paths))


def _object_identity(path):
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode):
        raise ValueError("Working view contains a symbolic link: " + path)
    if not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)):
        raise ValueError("Working view contains an unsupported object: " + path)
    if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
        raise ValueError("Working view contains a hard-linked file: " + path)
    if info.st_uid != os.getuid():
        raise ValueError("Working view object belongs to another user: " + path)
    return info


def _inventory(workspace):
    from .same_library_views import MARKER, owns_view
    objects, locks, views = [], [], []
    seen = set()
    for row in workspace.get("same_library_registry") or []:
        target = row.get("target") or []
        if (len(target) != 3 or any(not isinstance(part, str) or not part
                or part in (".", "..") or os.path.basename(part) != part for part in target)):
            raise ValueError("Working view identity is invalid")
        path = row.get("path") or ""
        if (not os.path.isabs(path) or os.path.realpath(path) != path
                or not owns_view(workspace, target, role=row.get("role"))):
            raise ValueError("Working view path or ownership is unproven: " + path)
        if path in seen:
            raise ValueError("Working view registry contains a duplicate: " + path)
        seen.add(path)
        # Native creation reserves the exact absent destination before writing.
        if not os.path.lexists(path):
            continue
        root_info = _object_identity(path)
        if not stat.S_ISDIR(root_info.st_mode):
            raise ValueError("Working view is not a directory: " + path)
        marker = os.path.join(path, MARKER)
        marker_info = _object_identity(marker)
        if (not stat.S_ISREG(marker_info.st_mode)
                or not owns_view(workspace, target, role=row.get("role"), require_marker=True)):
            raise ValueError("Working view marker is unproven: " + path)
        views.append(path)
        pending = [path]
        while pending:
            current = pending.pop()
            info = _object_identity(current)
            objects.append((current, info))
            if not stat.S_ISDIR(info.st_mode):
                continue
            for name in sorted(os.listdir(current), reverse=True):
                child = os.path.join(current, name)
                if ".cdslck" in name:
                    # Presence alone is not proof of a safe read lock or a stale
                    # writer. Never inspect PID liveness to bypass/rewrite it.
                    locks.append({"path": child, "view_path": path, "lock_type": "unknown"})
                else:
                    pending.append(child)
    return objects, locks, views


def check_private_view_edit_locks(workspace):
    """Check exact owned views; every unqualified lock stays intact and busy."""
    unused, locks, views = _inventory(workspace)
    if locks:
        raise PrivateViewBusy(locks)
    return {"ok": True, "view_paths": views, "lock_paths": []}


def _set_access(workspace, writable):
    objects, locks, views = _inventory(workspace)
    if locks:
        raise PrivateViewBusy(locks)
    # Validate every tree before changing anything. Directories are enabled
    # before their children, and disabled after their children.
    objects.sort(key=lambda item: (item[0].count(os.sep), item[0]), reverse=not writable)
    changed = []
    for path, before in objects:
        if os.path.realpath(path) != path:
            raise ValueError("Working view object changed path: " + path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        if stat.S_ISDIR(before.st_mode):
            flags |= getattr(os, "O_DIRECTORY", 0)
        descriptor = os.open(path, flags)
        try:
            current = os.fstat(descriptor)
            if ((current.st_dev, current.st_ino, current.st_uid, current.st_nlink)
                    != (before.st_dev, before.st_ino, before.st_uid, before.st_nlink)
                    or stat.S_IFMT(current.st_mode) != stat.S_IFMT(before.st_mode)):
                raise ValueError("Working view object changed during access update: " + path)
            mode = stat.S_IMODE(current.st_mode) | stat.S_IRUSR
            if stat.S_ISDIR(current.st_mode):
                mode |= stat.S_IXUSR
            mode = mode | stat.S_IWUSR if writable else mode & ~0o222
            if mode != stat.S_IMODE(current.st_mode):
                os.fchmod(descriptor, mode)
                changed.append(path)
        finally:
            os.close(descriptor)
    return {"ok": True, "writable": writable, "view_paths": views, "changed_paths": changed}


def restore_private_views(workspace):
    """Restore owner write access to the registered views on suite release."""
    return _set_access(workspace, True)


def protect_private_views(workspace):
    """Remove all write bits from exact owned views while Crescendo is idle."""
    return _set_access(workspace, False)


@contextlib.contextmanager
def write_private_views(workspace, restore_when=None):
    """Permit one serialized writer, then guard old and newly created views.

    ``restore_when`` must return false if native writer termination is unproven;
    the caller must keep that session unsafe and prevent reuse in this case.
    """
    check_private_view_edit_locks(workspace)
    try:
        _set_access(workspace, True)
        yield workspace
    finally:
        if restore_when is None or restore_when():
            protect_private_views(workspace)
