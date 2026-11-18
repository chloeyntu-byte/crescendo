"""Passive, best-effort timings for diagnostic lifecycle events.

The caller supplies phase boundaries that it already observes. Durations describe
those intervals, not unobserved subprocess or solver activity.
"""
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from contextlib import contextmanager
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
        self._operations = {}
        self.prefix_scans = []
        self.generation_commit = []

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

    @property
    def active_phase(self):
        return self._state[1]

    def operation(self, name, seconds, byte_count):
        if seconds is None:
            return
        previous = self._operations.get(name, (0.0, 0, 0.0, 0))
        self._operations[name] = (previous[0] + seconds, previous[1] + 1,
                                  max(previous[2], seconds), previous[3] + byte_count)

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

    def operations_snapshot(self):
        return {key: {'seconds': seconds, 'calls': calls, 'max_seconds': maximum, 'bytes': byte_count}
                for key, (seconds, calls, maximum, byte_count) in self._operations.copy().items()}


@contextmanager
def observe_operation(observer, name, byte_count=0):
    """Read the clock around existing work, without adding storage or waits."""
    start = timing_tick() if observer is not None and observer.active_phase is not None else None
    try:
        yield
    finally:
        if start is not None:
            observer.operation(name, elapsed_seconds(start), byte_count)


CANDIDATE_PHASE_LABELS = {
    'candidate_total': 'Candidate elapsed',
    'build': 'Build',
    'worker_setup': 'Worker setup',
    'shared_setup': 'Shared setup',
    'shared_setup_wait': 'Shared setup wait',
    'shared_setup_lock_wait': 'Shared setup lock wait',
    'session_setup': 'Working session setup',
    'environment_lock_wait': 'EDA environment lock wait',
    'model_profile_lock_wait': 'Model profile lock wait',
    'session_lock_wait': 'Working session lock wait',
    'session_lock_held': 'Working session lock held',
    'native_setup_readback': 'Native setup readback',
    'workspace_build': 'Workspace build',
    'template_capture': 'Template capture',
    'native_generation': 'Native netlist',
    'native_generation_lock_wait': 'Native netlist lock wait',
    'native_generation_lock_held': 'Native netlist lock held',
    'native_capture': 'Native capture',
    'template_materialization': 'Template netlist',
    'execution': 'Simulation lifecycle',
    'execution_preparation': 'Execution preparation',
    'capacity_wait': 'Execution capacity wait',
    'measurement_queue': 'Measurement queue',
    'scheduler_queue': 'Scheduler queue',
    'scheduler_observed_pending': 'Observed scheduler pending',
    'solver': 'Solver',
    'native_collection': 'Native results',
    'shared_collection': 'Shared results',
    'native_cleanup': 'Native cleanup',
    'cleanup': 'Artifact cleanup',
    'collection': 'Collection',
    'result_aggregation': 'Result aggregation',
    'result_collection': 'Point result collection',
    'metric_evaluation': 'Metrics',
}
# Kept separate from lifecycle totals: these stages partition preparation only.
CANDIDATE_PHASE_LABELS.update({
    'prepare_template_qualification': 'Template qualification (exclusive)',
    'prepare_session_lifecycle': 'One-shot session startup / shutdown',
    'prepare_model_dependency_freeze': 'Model and dependency freeze',
    'prepare_native_batch_capture': 'Native capture (batch or scalar fallback)',
    'prepare_other': 'Other preparation / lock waits',
})
GENERATION_PHASE_LABELS = {
    'ask': 'Ask / plan',
    'netlist_prepare': 'Generation netlist preparation',
    'evaluate': 'Evaluation',
    'tell': 'Tell',
    'settlement': 'Settlement',
    'persist': 'Checkpoint persistence',
    'candidate_persist': 'Candidate persistence',
    'candidate_cleanup': 'Candidate cleanup',
}
STORAGE_OPERATION_LABELS = {
    'checkpoint_transaction': 'Checkpoint transaction',
    'checkpoint_planned_rows': 'Save planned candidates',
    'checkpoint_dataset_reference': 'Build dataset reference',
    'checkpoint_document': 'Build checkpoint document',
    'checkpoint_disk_space': 'Disk space check',
    'checkpoint_previous_anchor': 'Read and verify previous anchor',
    'checkpoint_pending_build': 'Build pending journal',
    'checkpoint_pending_write_verify': 'Write and verify pending journal',
    'checkpoint_run_write': 'Encode, write and sync run checkpoint',
    'checkpoint_run_verify': 'Verify run checkpoint',
    'checkpoint_anchor_write': 'Encode, write and sync continuation anchor',
    'checkpoint_anchor_verify': 'Verify continuation anchor',
    'checkpoint_pending_clear': 'Clear pending journal',
    'dataset_prefix_scan': 'Read and hash dataset prefix',
    'dataset_read_new_records': 'Read and parse new dataset records',
    'point_record_commit': 'Commit simulation point receipt',
    'dataset_append_write': 'Append dataset bytes',
    'dataset_append_sync': 'Sync dataset append',
}


def timing_seconds(value):
    """Accept only measured durations and leave missing or invalid data unknown."""
    if type(value) in (int, float) and 0 <= value < float('inf'):
        return value
    return None


def elapsed_seconds(start, end=None):
    if start is None:
        return None
    end = timing_tick() if end is None else end
    return timing_seconds(end - start) if end is not None else None


def timing_tick():
    try:
        value = time.monotonic()
        return value if math.isfinite(value) else None
    except Exception:
        return None


def phase_seconds(seconds, **metadata):
    return dict(metadata, elapsed_seconds=timing_seconds(seconds))


def observer_phases(observer):
    snapshot = observer.snapshot()
    return {name: phase_seconds(seconds) for name, seconds in (snapshot.get('phases_seconds') or {}).items()}


def candidate_timing_summary(timing):
    """Project only the public phase durations, without native logs or values."""
    if not isinstance(timing, dict):
        return {}
    phases = timing.get('phases')
    if not isinstance(phases, dict):
        return {}
    public = {}
    for name in CANDIDATE_PHASE_LABELS:
        phase = phases.get(name)
        if isinstance(phase, dict):
            public[name] = phase_seconds(phase.get('elapsed_seconds'), **{
                key: phase[key] for key in ('scope', 'batch_id', 'count', 'total_count',
                                            'completed', 'source', 'boundary')
                if key in phase
            })
    if not public:
        return {}
    return {'elapsed_seconds': timing_seconds(timing.get('elapsed_seconds')),
            'elapsed_scope': timing.get('elapsed_scope') or 'candidate', 'phases': public}


def _add_timing_sample(samples, name, scope, seconds, count=1, total_count=1):
    key = (name, scope)
    record = samples.setdefault(key, {'phase': name, 'scope': scope, 'elapsed_seconds': None,
                                      'count': 0, 'total_count': 0})
    record['total_count'] += total_count
    if seconds is not None:
        record['count'] += count
        record['elapsed_seconds'] = (record['elapsed_seconds'] or 0.0) + seconds


def generation_timing_summary(generation, observer, rows, resumed=False):
    """Aggregate disjoint phase groups, counting a shared batch only once."""
    samples = {}
    seen_batches = set()
    measured_candidates = 0
    for row in rows:
        timing = candidate_timing_summary(row.get('timing'))
        if not timing:
            continue
        if timing['elapsed_seconds'] is not None or any(
                phase['elapsed_seconds'] is not None for phase in timing['phases'].values()):
            measured_candidates += 1
        if timing['elapsed_scope'] == 'candidate':
            _add_timing_sample(samples, 'candidate_total', 'candidate_sum', timing['elapsed_seconds'])
        for name, phase in timing['phases'].items():
            seconds = phase['elapsed_seconds']
            scope = phase.get('scope')
            if scope == 'batch':
                identity = phase.get('batch_id')
                key = (name, identity)
                if not isinstance(identity, str) or key in seen_batches:
                    continue
                seen_batches.add(key)
                _add_timing_sample(samples, name, 'batch_sum', seconds)
            elif scope == 'task_sum':
                count, total = phase.get('count'), phase.get('total_count')
                if type(count) is not int or type(total) is not int or not 0 <= count <= total:
                    count, total = (1 if seconds is not None else 0), 1
                _add_timing_sample(samples, name, 'task_sum', seconds, count, total)
            else:
                _add_timing_sample(samples, name, 'candidate_sum', seconds)
    snapshot = observer.snapshot()
    return dict(snapshot, schema_version=_contract_text('crescendo.generation_timing.v1'), generation=generation + 1,
                resumed_segment=bool(resumed), candidate_count=len(rows),
                timed_candidate_count=measured_candidates,
                candidate_phase_sums=[samples[key] for key in sorted(samples)],
                netlist_preparation=getattr(observer, "netlist_preparation", {}),
                storage_operations=observer.operations_snapshot(),
                prefix_scans=list(observer.prefix_scans),
                generation_commit=list(observer.generation_commit))


def generation_report_timing(timing):
    """Compact shape for a diagnostic report with up to three short runs."""
    def rounded(value):
        seconds = timing_seconds(value)
        return round(seconds, 6) if seconds is not None else None

    return {
        'generation': timing.get('generation'),
        'netlist_preparation': timing.get('netlist_preparation') or {},
        'prefix_scans': timing.get('prefix_scans') or [],
        'generation_commit': timing.get('generation_commit') or [],
        'status': timing.get('status'),
        'wall_seconds': rounded(timing.get('total_seconds')),
        'phases_seconds': {phase: rounded(seconds)
                           for phase, seconds in (timing.get('phases_seconds') or {}).items()},
        'candidate_count': timing.get('candidate_count'),
        'timed_candidate_count': timing.get('timed_candidate_count'),
        'resumed_segment': timing.get('resumed_segment'),
        'storage_operations': {
            name: [rounded(item['seconds']), item['calls'], rounded(item['max_seconds']), item['bytes']]
            for name, item in (timing.get('storage_operations') or {}).items()
        },
        'phase_sums': {
            scope: {item['phase']: [rounded(item['elapsed_seconds']), item['count'], item['total_count']]
                    for item in timing.get('candidate_phase_sums') or [] if item['scope'] == scope}
            for scope in ('candidate_sum', 'batch_sum', 'task_sum')
            if any(item['scope'] == scope for item in timing.get('candidate_phase_sums') or [])
        },
    }
