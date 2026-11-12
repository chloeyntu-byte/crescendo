"""Close this process's native-log readers before deleting owned artifacts.

This is a local thread barrier, not proof that readers on other hosts or in
other processes are gone. Existing process and remote-stop proofs remain
required. No descriptor is closed on behalf of an unrelated reader.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import contextlib
import threading
import time

_IO_LOCK = threading.RLock()
_STATE_LOCK = threading.Lock()
_READERS = {}


class OwnedLogReadersBusy(RuntimeError):
    def __init__(self, detail):
        self.detail = detail
        RuntimeError.__init__(self, "Owned native-log readers did not close before bounded artifact cleanup")


def _reader_snapshot():
    with _STATE_LOCK:
        rows = [dict(row) for row in _READERS.values()]
    return {"active_reader_count": sum(row["depth"] for row in rows),
            "threads": rows[:16], "scope": "this_process_threads_only"}


@contextlib.contextmanager
def native_log_read_scope():
    """Keep the full open/read/close interval outside owned deletion."""
    _IO_LOCK.acquire()
    identity = threading.get_ident()
    with _STATE_LOCK:
        row = _READERS.setdefault(identity, {"thread_name": threading.current_thread().name,
                                             "thread_id": identity, "depth": 0})
        row["depth"] += 1
    try:
        yield
    finally:
        with _STATE_LOCK:
            _READERS[identity]["depth"] -= 1
            if not _READERS[identity]["depth"]:
                del _READERS[identity]
        _IO_LOCK.release()


@contextlib.contextmanager
def native_log_cleanup_scope(timeout=5.0):
    """Wait at most five seconds for local readers, then retain and retry."""
    timeout = min(5.0, max(0.0, float(timeout)))
    before = _reader_snapshot()
    started = time.monotonic()
    acquired = _IO_LOCK.acquire(timeout=timeout)
    detail = {"schema_version": _contract_text("crescendo.native_log_reader_barrier.v1"),
              "wait_seconds": round(time.monotonic() - started, 6),
              "timeout_seconds": timeout, "readers_before_wait": before,
              "scope": "this_process_threads_only", "cross_process_absence_proven": False}
    if not acquired:
        detail.update(state="readers_busy", readers_after_wait=_reader_snapshot(), retryable=True)
        raise OwnedLogReadersBusy(detail)
    try:
        after = _reader_snapshot()
        # RLock cannot justify deleting a file this same thread is still reading.
        if after["active_reader_count"]:
            detail.update(state="readers_busy", readers_after_wait=after, retryable=True)
            raise OwnedLogReadersBusy(detail)
        detail.update(state="local_readers_closed", readers_after_wait=after,
                      readers_closed_before_delete=True)
        yield detail
    finally:
        _IO_LOCK.release()
