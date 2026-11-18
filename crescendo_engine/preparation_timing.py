"""Exclusive, thread-local observations of netlist preparation (no authority)."""
from contextlib import contextmanager
from functools import wraps
import os
import threading
import uuid

from .candidate_timing import timestamp, elapsed
from .diagnostic_timing import timing_seconds
from .runtime import bind_text
_contract_text = bind_text()

STAGES = ('template_qualification', 'session_lifecycle', 'model_dependency_freeze',
          'native_batch_capture', 'other')
_LOCAL = threading.local()


class PreparationTiming(object):
    def __init__(self, clock=None):
        self.clock = clock
        self.seconds = dict.fromkeys(STAGES, 0.0)
        self.active = 'other'
        self.last = self._now()
        self.available = self.last is not None
        self.capture_count = 0
        self.session_samples = 0
        self.session_missing = 0
        self.session_startup = 0.0
        self.session_shutdown = 0.0

    def _now(self):
        return timestamp(self.clock)

    def _flush(self):
        now = self._now()
        value = elapsed(self.last, now)
        if value is None:
            self.available = False
        else:
            self.seconds[self.active] += value
        self.last = now

    @contextmanager
    def stage(self, name):
        self._flush()
        previous, self.active = self.active, name
        try:
            yield
        finally:
            self._flush()
            self.active = previous

    def command(self, started, finished, command, opened, closing):
        """Carve observed child intervals out of the enclosing capture stage.

        File mtimes use the parent's epoch clock; reject out-of-window markers.
        Lock wait stays in Other. Missing markers leave the command in capture,
        with session coverage explicitly incomplete, never a fabricated zero.
        """
        self._flush()
        duration = elapsed(started, finished)
        lock = timing_seconds(command.get('lock_wait_seconds'))
        if (duration is None or lock is None or lock > duration
                or lock > self.seconds[self.active]):
            self.session_missing += 1
            return
        self.seconds[self.active] -= lock
        self.seconds['other'] += lock
        try:
            a, b = os.stat(opened).st_mtime, os.stat(closing).st_mtime
            if not started + lock <= a <= b <= finished:
                raise ValueError('native marker outside command interval')
        except (OSError, ValueError):
            self.session_missing += 1
            return
        value = (a - started - lock) + (finished - b)
        if value > self.seconds[self.active] or value < 0:
            self.session_missing += 1
            return
        self.seconds[self.active] -= value
        self.seconds['session_lifecycle'] += value
        self.session_samples += 1
        self.session_startup += a - started - lock
        self.session_shutdown += finished - b

    def snapshot(self):
        self._flush()
        phases = {name: {'elapsed_seconds': seconds if self.available else None}
                  for name, seconds in self.seconds.items()}
        # The enclosing capture retains unclassified time when markers failed.
        if self.session_missing:
            phases['session_lifecycle']['elapsed_seconds'] = None
        return {'schema_version': _contract_text('crescendo.netlist_preparation_timing.v1'),
                'scope': 'exclusive_within_preparation', 'available': self.available,
                'elapsed_seconds': sum(self.seconds.values()) if self.available else None,
                'phases': phases, 'native_capture_count': self.capture_count,
                'session_samples': self.session_samples, 'session_missing': self.session_missing,
                'session_startup_observed_seconds': self.session_startup if self.available else None,
                'session_shutdown_observed_seconds': self.session_shutdown if self.available else None,
                'session_observed_seconds': self.seconds['session_lifecycle'] if self.available else None}


@contextmanager
def observe_preparation(target, identity, clock=None):
    """Nested callers share one owner; publish only at the outer boundary."""
    previous = getattr(_LOCAL, 'observer', None)
    if previous is not None:
        yield previous
        return
    # A resumed segment can retain old candidate rows from this generation.
    # Distinguish physical observations while fanout copies keep the same ID.
    try:
        identity = identity + ':' + uuid.uuid4().hex
    except Exception:
        identity = None  # Unknown identity must not control execution.
    observer = PreparationTiming(clock)
    _LOCAL.observer = observer
    completed = False
    try:
        yield observer
        completed = True
    finally:
        try:
            target.update(observer.snapshot(), batch_id=identity,
                          status='completed' if completed else 'interrupted')
        finally:
            _LOCAL.observer = previous


@contextmanager
def preparation_stage(name):
    observer = getattr(_LOCAL, 'observer', None)
    if observer is None:
        yield
    else:
        with observer.stage(name):
            yield


def stage(name):
    def decorate(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            with preparation_stage(name):
                return function(*args, **kwargs)
        return wrapped
    return decorate


def observe_capture_command(started, finished, command, opened, closing):
    observer = getattr(_LOCAL, 'observer', None)
    if observer is not None:
        observer.command(started, finished, command, opened, closing)


def capture_observed():
    observer = getattr(_LOCAL, 'observer', None)
    if observer is not None:
        observer.capture_count += 1


def candidate_phases(summary):
    identity = summary.get('batch_id')
    return {'prepare_' + name: dict(value, scope='batch', batch_id=identity,
                                   boundary='exclusive_netlist_preparation')
            for name, value in (summary.get('phases') or {}).items() if name in STAGES}
