"""Passive, best-effort timings for diagnostic lifecycle events.

The caller supplies phase boundaries that it already observes. Durations describe
those intervals, not unobserved subprocess or solver activity.
"""
import math
import time


class DiagnosticTiming(object):
    def __init__(self, clock=None):
        self._clock = clock if clock is not None else time.monotonic
        # Only the lifecycle owner publishes states. Each published phase mapping
        # is immutable by convention; readers retain one consistent version.
        self._state = ('not_started', None, None, None, {})
        self._last_time = None
        self._available = True

    def _now(self):
        if not self._available:
            return None
        try:
            value = float(self._clock())
            if not math.isfinite(value):
                raise ValueError('invalid monotonic time')
            if self._last_time is not None:
                value = max(value, self._last_time)
            self._last_time = value
            return value
        except Exception:
            # An incomplete timeline must never be reported as a measured one.
            self._available = False
            return None

    def start(self, phase):
        status, _, _, _, _ = self._state
        if status != 'not_started':
            return
        now = self._now()
        self._state = ('running', phase, now, now, {phase: 0.0})

    def transition(self, phase):
        status, active, started, phase_started, old_phases = self._state
        if status != 'running' or phase == active:
            return
        now = self._now()
        phases = dict(old_phases)
        if now is not None and phase_started is not None:
            now = max(now, phase_started)
            phases[active] += now - phase_started
        phases.setdefault(phase, 0.0)
        self._state = (status, phase, started, now, phases)

    def finish(self, status='completed'):
        previous, active, started, phase_started, old_phases = self._state
        if previous != 'running':
            return
        now = self._now()
        phases = dict(old_phases)
        if now is not None and phase_started is not None:
            now = max(now, phase_started)
            phases[active] += now - phase_started
        self._state = (status, None, started, now, phases)

    def snapshot(self):
        status, active, started, phase_started, old_phases = self._state
        phases = dict(old_phases)
        now = phase_started
        if status == 'running':
            now = self._now()
            if now is not None and phase_started is not None:
                now = max(now, phase_started)
                phases[active] += now - phase_started
        available = self._available
        if not available:
            total, phases = None, None
        elif started is None:
            total = None
        else:
            total = now - started
        return {'status': status, 'available': available,
                'total_seconds': total, 'phases_seconds': phases,
                'active_phase': active}
