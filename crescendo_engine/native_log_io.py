"""Coordinate in-process log reads and deletion of overlapping paths.

The filesystem ownership and process/remote-stop proofs remain separate.
"""
from __future__ import print_function

import contextlib
import os
import threading
import time

from crescendo_engine.runtime import bind_text

_contract_text = bind_text()
_STATE = threading.Condition()
_READERS = {}
_WRITERS = {}


class OwnedLogReadersBusy(RuntimeError):
    def __init__(self, detail):
        self.detail = detail
        message = "Owned native-log readers did not close before bounded artifact cleanup"
        if detail["state"] == "cleanup_busy":
            message = "Overlapping artifact cleanup did not finish before bounded cleanup"
        RuntimeError.__init__(self, message)


def _under(path, root):
    return path == root or path.startswith(root.rstrip(os.sep) + os.sep)


def _overlaps(first, second):
    return any(_under(left, right) or _under(right, left)
               for left in first for right in second)


def _scope_paths(paths):
    # Fence both an alias leaf and the actual file or subtree it resolves to.
    return tuple(dict.fromkeys(value for path in paths
                              for value in (os.path.abspath(path), os.path.realpath(path))))


def _matching(registry, paths):
    return [row for row in registry.values() if _overlaps(paths, row["paths"])]


def _owner(paths):
    return {"thread_name": threading.current_thread().name,
            "thread_id": threading.get_ident(), "paths": paths}


def _reader_snapshot(readers=None):
    if readers is None:
        with _STATE:
            readers = list(_READERS.values())
    return {"active_reader_count": len(readers),
            "threads": [dict(row, depth=1) for row in readers[:16]],
            "scope": "this_process_threads_only"}


@contextlib.contextmanager
def native_log_read_scope(path=None):
    """Register the open/read/close interval before opening the file."""
    paths = _scope_paths((path,)) if path is not None else (os.sep,)
    token = object()
    owner = _owner(paths)
    thread = owner["thread_id"]
    with _STATE:
        # An already-registered reader may finish nested work in its own scope
        # even after a writer has started waiting for it.
        nested = any(row["thread_id"] == thread and any(
                         _under(path, root) for path in paths for root in row["paths"])
                     for row in _READERS.values())
        if not nested:
            _STATE.wait_for(lambda: not any(row["thread_id"] != thread
                            for row in _matching(_WRITERS, paths)))
        _READERS[token] = owner
    try:
        yield
    finally:
        with _STATE:
            del _READERS[token]
            _STATE.notify_all()


@contextlib.contextmanager
def native_log_cleanup_scope(timeout=5.0, paths=None):
    """Fence only the files and subtrees touched by the deletion."""
    paths = _scope_paths(paths) if paths else (os.sep,)
    timeout = min(5.0, max(0.0, float(timeout)))
    token = object()
    owner = _owner(paths)
    thread = owner["thread_id"]
    started = time.monotonic()
    with _STATE:
        before = _reader_snapshot(_matching(_READERS, paths))

        def remaining():
            return max(0.0, timeout - (time.monotonic() - started))

        def result(state):
            detail = {"schema_version": _contract_text("crescendo.native_log_reader_barrier.v1"),
                      "state": state, "paths": paths, "timeout_seconds": timeout,
                      "wait_seconds": round(time.monotonic() - started, 6),
                      "readers_before_wait": before,
                      "readers_after_wait": _reader_snapshot(_matching(_READERS, paths)),
                      "scope": "this_process_threads_only", "cross_process_absence_proven": False}
            if state == "local_readers_closed":
                detail["readers_closed_before_delete"] = True
            else:
                detail["retryable"] = True
                if state == "cleanup_busy":
                    detail["conflicting_cleanups"] = _matching(_WRITERS, paths)[:16]
            return detail

        # A same-thread read must close before deletion. Waiting could not
        # release it because its context is on this thread's stack.
        if any(row["thread_id"] == thread for row in _matching(_READERS, paths)):
            raise OwnedLogReadersBusy(result("readers_busy"))
        if not _STATE.wait_for(lambda: not any(row["thread_id"] != thread
                               for row in _matching(_WRITERS, paths)), remaining()):
            raise OwnedLogReadersBusy(result("cleanup_busy"))
        # Claim this scope before waiting for existing readers, so new opens
        # cannot continually overtake the cleanup.
        _WRITERS[token] = owner
        try:
            if not _STATE.wait_for(lambda: not _matching(_READERS, paths), remaining()):
                raise OwnedLogReadersBusy(result("readers_busy"))
            detail = result("local_readers_closed")
        except BaseException:
            del _WRITERS[token]
            _STATE.notify_all()
            raise
    try:
        yield detail
    finally:
        with _STATE:
            del _WRITERS[token]
            _STATE.notify_all()
