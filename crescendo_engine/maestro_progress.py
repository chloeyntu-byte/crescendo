"""Bounded observations of owned Maestro jobs; never completion/stop authority.

Only this run's explicit simulation roots and batch aliases are inspected.
Snapshots use one file per batch. Native completion still requires RDB, netlist,
parameter and terminal Spectre proofs in the existing execution adapter.
"""
from __future__ import print_function

import collections
import contextlib
import copy
import json
import os
import re
import stat
import time

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json, utc_timestamp
from .native_log_io import native_log_read_scope

SCHEMA = _contract_text('crescendo.maestro_live_progress.v1')
MAX_SNAPSHOT = 32 * 1024 * 1024
SNAPSHOT_WARNING_BYTES = 4 * 1024 * 1024
SUBMISSION_OUTPUT_BYTES = 2048


def owned_regular(path, root):
    path, root = os.path.abspath(path), os.path.abspath(root)
    if not path.startswith(root + os.sep) or os.path.realpath(path) != path:
        return False
    try:
        info = os.lstat(path)
        return stat.S_ISREG(info.st_mode) and info.st_nlink == 1
    except OSError:
        return False


@contextlib.contextmanager
def _owned_binary_stream(path, root):
    with native_log_read_scope():
        with _owned_binary_stream_unlocked(path, root) as stream:
            yield stream


@contextlib.contextmanager
def _owned_binary_stream_unlocked(path, root):
    """Open the proved inode without following a swapped symlink or FIFO.

    These reads are display evidence only. Every opened handle closes before
    returning; no watcher keeps native logs open across cleanup on NFS.
    """
    descriptor = None
    stream = None
    try:
        path, root = os.path.abspath(path), os.path.abspath(root)
        if not path.startswith(root + os.sep) or os.path.realpath(path) != path:
            yield None
            return
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            yield None
            return
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
        flags |= getattr(os, 'O_CLOEXEC', 0)
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        after = os.lstat(path)
        if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                or not os.path.samestat(before, opened) or not os.path.samestat(after, opened)
                or os.path.realpath(path) != path):
            os.close(descriptor)
            descriptor = None
            yield None
            return
        stream = os.fdopen(descriptor, 'rb')
        descriptor = None
    except OSError:
        if descriptor is not None:
            os.close(descriptor)
        yield None
        return
    try:
        yield stream
    finally:
        stream.close()


def _read_small_owned(path, root, limit):
    with _owned_binary_stream(path, root) as stream:
        if stream is None or os.fstat(stream.fileno()).st_size > limit:
            return None
        value = stream.read(limit + 1)
        return value if len(value) <= limit else None


def tail_text(path, root, limit=8192):
    try:
        with _owned_binary_stream(path, root) as stream:
            if stream is None:
                return ''
            stream.seek(0, os.SEEK_END)
            stream.seek(max(0, stream.tell() - limit))
            return stream.read(limit).decode('utf-8', 'replace')
    except OSError:
        return ''


def _submission_summary(event):
    """Keep exact command/identity and bounded duplicate console evidence.

    The submission ledger remains the complete record. Repeating full stdout
    for every native point must not turn a live display snapshot into a large
    simulation artifact.
    """
    summary = copy.deepcopy(event)
    budget = SUBMISSION_OUTPUT_BYTES
    truncated = []
    for key in ('text', 'stdout_tail', 'stderr_tail', 'stdout', 'stderr', 'output'):
        value = summary.get(key)
        if not isinstance(value, str):
            continue
        raw = value.encode('utf-8')
        if len(raw) > budget:
            summary[key] = raw[-budget:].decode('utf-8', 'ignore') if budget else ''
            truncated.append(key)
        budget -= min(len(raw), budget)
    if truncated:
        summary['output_truncated_fields'] = truncated
        summary['complete_output_source'] = 'submission_log_path'
    return summary


def _publish_progress(path, snapshot):
    """Keep every native task, with an explicit display capacity fallback."""
    size = len((json.dumps(snapshot, indent=2, sort_keys=True) + '\n').encode('utf-8'))
    if size > SNAPSHOT_WARNING_BYTES:
        snapshot['telemetry_capacity'] = {
            'state': 'large_snapshot', 'bytes': size, 'limit_bytes': MAX_SNAPSHOT,
            'task_count': sum(len(row['execution_progress'].get('tasks') or [])
                              for row in snapshot['candidates']),
            'message': 'Large live task mapping; all native task identities are preserved.'}
        for row in snapshot['candidates']:
            row['execution_progress']['telemetry_capacity'] = copy.deepcopy(snapshot['telemetry_capacity'])
    size = len((json.dumps(snapshot, indent=2, sort_keys=True) + '\n').encode('utf-8'))
    if size > MAX_SNAPSHOT:
        # This file is display evidence, not completion/stop authority. Keep a
        # complete catalogue rather than silently discard valid native points.
        catalog_path = os.path.join(os.path.dirname(path), _contract_text('execution_progress_full.json'))
        atomic_write_json(catalog_path, snapshot)
        compact = copy.deepcopy(snapshot)
        compact['shared_submissions'] = []
        compact['telemetry_capacity'] = {
            'state': 'telemetry_capacity_exceeded', 'bytes': size,
            'limit_bytes': MAX_SNAPSHOT, 'complete_catalog_path': catalog_path,
            'message': 'Live mapping exceeds display capacity; candidate stages remain current. '
                       'All native task/log identities are in the complete mapping catalogue.'}
        for row in compact['candidates']:
            progress = row['execution_progress']
            progress['task_count'] = len(progress.get('tasks') or [])
            progress['tasks'] = []
            progress['telemetry_capacity'] = copy.deepcopy(compact['telemetry_capacity'])
            progress.setdefault('shared_logs', []).append({
                'path': catalog_path, 'label': 'Complete native task/log mapping (display capacity exceeded)'})
        snapshot = compact
    atomic_write_json(path, snapshot)
    return snapshot


class MaestroProgressObserver(object):
    def __init__(self, run_root, workspace, manifest, control_root):
        self.run_root = os.path.abspath(run_root)
        self.workspace = workspace
        self.manifest = manifest
        self.control_root = control_root
        self.path = os.path.join(manifest['batch_root'], _contract_text('execution_progress.json'))
        self.aliases = {}
        for candidate in manifest['candidates']:
            for alias, source in (candidate.get('source_test_map') or {}).items():
                if alias in self.aliases:
                    raise ValueError('native live test alias is ambiguous')
                self.aliases[alias] = (candidate['candidate_id'], source)
        storage = workspace.get('native_storage') or {}
        requested = [os.path.join(workspace['root'], 'simulation')]
        requested += [storage.get(key) for key in
                      ('save_res_dir', 'save_dir', 'local_psf_dir', 'local_rdb_dir')]
        self.roots = []
        for path in requested:
            if (path and os.path.abspath(path).startswith(self.run_root + os.sep)
                    and os.path.realpath(path) == os.path.abspath(path)
                    and not any(os.path.abspath(path).startswith(p + os.sep) or os.path.abspath(path) == p
                                for p in self.roots)):
                self.roots.append(os.path.abspath(path))
        self.tasks = {}
        self.started_epoch = time.time()
        self.previous = None
        self.next_poll = 0
        self.scan_queue = collections.deque()
        self.submission_offset = 0
        self.submissions = {}
        self.identity_path = os.path.join(manifest['batch_root'], _contract_text('live_identity.json'))
        atomic_write_json(self.identity_path, {'schema_version': SCHEMA,
            'protocol_identity': manifest['protocol_identity'], 'batch_id': manifest['batch_id']})

    def _scan(self):
        # Do not descend into waveform/AHDL databases or read whole libraries.
        # Continue a bounded directory queue on large tests, then repeat to
        # discover newly-created points. No directory/file handle stays open.
        if not self.scan_queue:
            self.scan_queue.extend((path, 0) for path in self.roots)
        started = time.monotonic()
        entries = 0
        while self.scan_queue and entries < 6000 and time.monotonic() - started < .08:
            queued = self.scan_queue.popleft()
            directory, depth = queued[:2]
            skipped = queued[2] if len(queued) == 3 else 0
            if os.path.realpath(directory) != directory or not os.path.isdir(directory):
                continue
            parts = directory.split(os.sep)
            if len(parts) >= 3 and parts[-1] == 'psf' and parts[-2] in self.aliases:
                alias = parts[-2]
                point = parts[-3]
                path = os.path.join(directory, 'spectre.out')
                if not owned_regular(path, self.run_root):
                    continue
                if self.manifest.get('serial_observation'):
                    try:
                        if os.stat(path).st_mtime < self.started_epoch:
                            continue
                    except OSError:
                        continue
                cid, source = self.aliases[alias]
                key = (cid, alias, point)
                task = self.tasks.setdefault(key, {'test': source, 'artifact_test': alias,
                    'point_id': point, 'corner': None, 'state': 'starting',
                    'spectre_log': path, 'identity_source': 'owned_native_point_directory'})
                text = tail_text(path, self.run_root)
                done = owned_regular(os.path.join(directory, '.simDone'), self.run_root)
                if done or re.search(r'spectre completes with\s+\d+\s+errors', text, re.I):
                    task['state'] = 'finished'
                elif text:
                    task['state'] = 'running'
                continue
            if depth >= 14:
                continue
            iterator = None
            position = 0
            try:
                iterator = os.scandir(directory)
                for entry in iterator:
                    position += 1
                    if position <= skipped:
                        continue
                    entries += 1
                    name = entry.name
                    if not name.startswith('.') and (name not in ('ahdlSimDB', 'AHDL', 'netlist', 'raw', 'psf')
                        or name == 'psf' and os.path.basename(directory) in self.aliases):
                        if entry.is_dir(follow_symlinks=False):
                            self.scan_queue.append((entry.path, depth + 1))
                    if entries >= 6000 or time.monotonic() - started >= .08:
                        self.scan_queue.appendleft((directory, depth, position))
                        break
            except OSError:
                pass
            finally:
                if iterator is not None:
                    iterator.close()

    def _submission_events(self):
        ledger_path = self.manifest.get('submission_log_path')
        if not ledger_path or not owned_regular(ledger_path, self.run_root):
            return
        changed = set()
        try:
            with _owned_binary_stream(ledger_path, self.run_root) as stream:
                if stream is None:
                    return
                if os.fstat(stream.fileno()).st_size < self.submission_offset:
                    return  # an existing invocation ledger must not rewind
                stream.seek(self.submission_offset)
                for _ in range(128):
                    raw = stream.readline(128 * 1024)
                    if not raw or not raw.endswith(b'\n'):
                        break
                    self.submission_offset = stream.tell()
                    event = json.loads(raw.decode('utf-8'))
                    if not isinstance(event, dict):
                        continue
                    invocation = event.get('invocation_id')
                    if not invocation:
                        continue
                    item = self.submissions.setdefault(invocation, {})
                    item.update(_submission_summary(event))
                    bounded_item = _submission_summary(item)
                    item.clear()
                    item.update(bounded_item)
                    changed.add(invocation)
                    from .run_log import append_run_event
                    summary = _submission_summary(event)
                    try:
                        append_run_event(self.run_root, 'maestro_submission', event.get('event') or 'native submission',
                            dict((key, summary[key]) for key in ('event', 'invocation_id', 'argv', 'cwd', 'job_ids', 'returncode', 'stdout_tail', 'stderr_tail', 'text', 'output_truncated_fields') if key in summary))
                    except (OSError, ValueError):
                        # A secondary log-write failure cannot erase the native
                        # submission identity already read from its ledger.
                        pass
        except (OSError, ValueError):
            return
        for invocation in changed:
            event = self.submissions[invocation]
            values = list(event.get('argv') or []) + [event.get('cwd') or '']
            # A native submission can include a script path. Read only that
            # small owned script; never execute it or follow a foreign path.
            for value in list(values):
                path = str(value)
                try:
                    script = _read_small_owned(path, self.run_root, 65536)
                    if script is not None:
                        values.append(script.decode('utf-8', 'replace'))
                except OSError:
                    pass
            evidence = '\n'.join(str(value) for value in values)
            matches = [alias for alias in self.aliases if re.search(
                r'(?<![A-Za-z0-9_])' + re.escape(alias) + r'(?![A-Za-z0-9_])', evidence)]
            if len(matches) != 1:
                continue  # shared/ambiguous submission remains batch evidence
            alias = matches[0]
            cid, source = self.aliases[alias]
            points = set(re.findall(r'/(\d+)/' + re.escape(alias) + r'(?:/|\b)', evidence))
            point = next(iter(points)) if len(points) == 1 else 'submission-' + invocation
            task = self.tasks.setdefault((cid, alias, point), {'test': source,
                'artifact_test': alias, 'point_id': point, 'corner': None,
                'state': 'submitted', 'identity_source': 'native_submit_argv_or_owned_script'})
            task['submission'] = _submission_summary(event)
            task['submit_log'] = ledger_path
            if not event.get('job_id') and len(event.get('job_ids') or []) == 1:
                event['job_id'] = event['job_ids'][0]
            if event.get('job_id'):
                task['job_id'] = event['job_id']
                if task['state'] in ('starting', 'submitted'):
                    task['state'] = 'queued'
            if event.get('returncode') not in (None, 0):
                task['state'] = 'submission_failed'

    def poll(self, states, force=False):
        now = time.monotonic()
        if not force and now < self.next_poll:
            return None
        self.next_poll = now + 1.
        self._scan()
        self._submission_events()
        native_stage = states[-1] if states else 'PREPARING'
        by_candidate = dict((row['candidate_id'], []) for row in self.manifest['candidates'])
        for (cid, alias, point), task in sorted(self.tasks.items()):
            by_candidate[cid].append(copy.deepcopy(task))
        candidates = []
        for row in self.manifest['candidates']:
            tasks = by_candidate[row['candidate_id']]
            task_states = set(task['state'] for task in tasks)
            if task_states & {'running', 'finished'}:
                stage = 'running' if 'running' in task_states else 'evaluating'
            elif 'queued' in task_states:
                stage = 'queued'
            elif 'submission_failed' in task_states:
                stage = 'submission_failed'
            elif 'starting' in task_states:
                stage = 'starting'
            elif tasks:
                stage = 'submitted'
            elif native_stage in ('SUBMITTED', 'SIM_DONE', 'RESULTS_READY', 'DONE'):
                stage = 'dispatching'  # run accepted; individual jobs unobserved
            else:
                stage = 'preparing'
            candidates.append({'candidate_id': row['candidate_id'], 'execution_progress': {
                'stage': 'infrastructure_failed' if native_stage == 'ERROR' else stage, 'batch_id': self.manifest['batch_id'],
                'native_stage': native_stage, 'tasks': tasks,
                'serial_observation': bool(self.manifest.get('serial_observation')),
                'protocol_identity': copy.deepcopy(self.manifest['protocol_identity']),
                'started_epoch': self.started_epoch,
                'log_search': {'roots': self.roots, 'state': 'found' if tasks else 'not_created_yet'},
                'shared_logs': [{'path': os.path.join(self.control_root, name)} for name in
                                ('ocean.stdout', 'ocean.stderr')] +
                               ([{'path': self.manifest['submission_log_path'], 'label': 'Native submission pool (shared)'}]
                                if self.manifest.get('submission_log_path') else []),
                'job_policy_readback': self.manifest.get('job_policy_readback_path'),
                'source_test_map': copy.deepcopy(row.get('source_test_map') or {})}})
        snapshot = {'schema_version': SCHEMA,
            'protocol_identity': self.manifest['protocol_identity'],
            'batch_id': self.manifest['batch_id'], 'candidates': candidates,
            'submission_log_path': self.manifest.get('submission_log_path'),
            'shared_submissions': [_submission_summary(event) for event in list(self.submissions.values())[-128:]]}
        compare = json.dumps(snapshot, sort_keys=True)
        if compare == self.previous:
            return None
        self.previous = compare
        snapshot['observed_at'] = utc_timestamp()
        return _publish_progress(self.path, snapshot)


def read_batch_progress(path, run_root, candidate_ids):
    try:
        raw = _read_small_owned(path, run_root, MAX_SNAPSHOT)
        if raw is None:
            return None
        value = json.loads(raw.decode('utf-8'))
        if value.get('schema_version') != SCHEMA:
            return None
        rows = value.get('candidates')
        if (not isinstance(rows, list) or len(rows) != len(candidate_ids)
                or set(row.get('candidate_id') for row in rows) != set(candidate_ids)):
            return None
        identity = value.get('protocol_identity') or {}
        if identity.get('candidate_id') != value.get('batch_id'):
            return None
        identity_path = os.path.join(os.path.dirname(path), _contract_text('live_identity.json'))
        if os.path.lexists(identity_path):
            identity_raw = _read_small_owned(identity_path, run_root, 16384)
            if identity_raw is None:
                return None
            receipt = json.loads(identity_raw.decode('utf-8'))
            if receipt.get('protocol_identity') != identity or receipt.get('batch_id') != value.get('batch_id'):
                return None
        # Legacy/small manifests can also be checked cheaply; never repeatedly
        # parse a large shared design catalogue for display-only telemetry.
        manifest_path = os.path.join(os.path.dirname(path), _contract_text('persistent_worker_batch.json'))
        manifest_raw = _read_small_owned(manifest_path, run_root, 65536)
        if manifest_raw is not None:
            manifest = json.loads(manifest_raw.decode('utf-8'))
            if manifest.get('protocol_identity') != identity:
                return None
        return value
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def preserve_live_logs(run_root, rows):
    """Keep bounded snapshots before owned worker files can disappear on Stop.

    Snapshot retention is diagnostic only, including unfinished/error logs. It
    grants no simulation-completion or termination authority. One stable copy
    per point, no repeated polling copies; every handle closes before cleanup.
    """
    import hashlib
    result = []
    shared = {}
    remaining_budget = 32 * 1024 * 1024
    deadline = time.monotonic() + 2.0
    for original in rows:
        row = copy.deepcopy(original)
        progress = row.get('execution_progress') or {}
        cid = row['candidate_id']
        if cid != 'g%03d_c%04d' % (row['generation'], row['index']):
            raise ValueError('log retention candidate identity changed')
        root = os.path.join(run_root, 'candidates', cid)
        if os.path.realpath(root) != root:
            raise ValueError('log retention destination is unsafe')
        os.makedirs(root, exist_ok=True)
        for task in progress.get('tasks') or []:
            source = task.get('spectre_log')
            if not source:
                continue
            if remaining_budget <= 0 or time.monotonic() >= deadline:
                task['retention_warning'] = 'diagnostic snapshot budget reached; native stop has priority'
                continue
            if not owned_regular(source, run_root):
                continue
            token = 'live_' + hashlib.sha256(source.encode('utf-8')).hexdigest()[:20]
            destination = os.path.join(root, 'retained_spectre_' + token + '.log')
            if source == destination:
                continue
            partial = destination + '.pending'
            if os.path.lexists(destination):
                existing_meta = os.path.join(root, 'retained_spectre_' + token + '.meta')
                if not owned_regular(existing_meta, run_root) or not owned_regular(destination, run_root):
                    raise ValueError('retained log identity is unavailable')
                meta_raw = _read_small_owned(existing_meta, run_root, 16384)
                if meta_raw is None:
                    raise ValueError('retained log metadata is unavailable or oversized')
                existing = dict(line.split('=', 1) for line in meta_raw.decode('utf-8', 'replace').splitlines() if '=' in line)
                if existing.get('source_path') != source or existing.get('candidate_id') != cid:
                    raise ValueError('retained log belongs to another source or candidate')
                task['original_spectre_log'] = source
                task['spectre_log'] = destination
                task['retained_snapshot'] = True
                continue
            if os.path.lexists(partial):
                raise ValueError('unfinished diagnostic copy retained; no overwrite')
            with _owned_binary_stream(source, run_root) as stream:
                if stream is None:
                    task['retention_warning'] = 'native log inode changed before snapshot; source retained for inspection'
                    continue
                if remaining_budget <= 0 or time.monotonic() >= deadline:
                    task['retention_warning'] = 'diagnostic snapshot budget reached; native stop has priority'
                    continue
                fd = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, 'wb') as target:
                    size = os.fstat(stream.fileno()).st_size
                    limit = min(512 * 1024, remaining_budget)
                    if size > limit:
                        stream.seek(size - limit)
                    remaining = min(size, limit)
                    copied = 0
                    while remaining and time.monotonic() < deadline:
                        data = stream.read(min(remaining, 65536))
                        if not data:
                            break
                        target.write(data)
                        copied += len(data)
                        remaining -= len(data)
                        remaining_budget -= len(data)
                    target.flush()
                    os.fsync(target.fileno())
            os.rename(partial, destination)
            meta = os.path.join(root, 'retained_spectre_' + token + '.meta')
            text = (_contract_text('schema_version=crescendo.retained_spectre_log.v1\nsource_path=%s\n'
                    'candidate_id=%s\nartifact_test=%s\npoint_id=%s\n'
                    'snapshot_only=true\ntruncated=%s\nfinished_epoch=%s\n') %
                    (source, cid, task.get('artifact_test') or '', '' if task.get('point_id') is None else task.get('point_id'),
                     str(size > copied).lower(), time.time()))
            from .utils import atomic_write_text
            atomic_write_text(meta, text)
            task['original_spectre_log'] = source
            task['spectre_log'] = destination
            task['retained_snapshot'] = True
        descriptors = progress.get('shared_logs') or []
        for descriptor_index, descriptor in enumerate(descriptors):
            if not isinstance(descriptor, dict):
                descriptor = {'path': descriptor}
                descriptors[descriptor_index] = descriptor
            source = descriptor.get('path') if isinstance(descriptor, dict) else descriptor
            if not source:
                continue
            if source in shared:
                descriptor['path'] = shared[source]
                descriptor['retained_snapshot'] = True
                continue
            if remaining_budget <= 0 or time.monotonic() >= deadline:
                if isinstance(descriptor, dict):
                    descriptor['retention_warning'] = 'diagnostic snapshot budget reached; native stop has priority'
                continue
            if not owned_regular(source, run_root):
                continue
            if source not in shared:
                label = 'retained_native_' + hashlib.sha256(source.encode('utf-8')).hexdigest()[:16] + '.log'
                destination = os.path.join(run_root, label)
                from .utils import atomic_write_text
                text = tail_text(source, run_root, min(65536, remaining_budget))
                atomic_write_text(destination, text)
                remaining_budget -= len(text.encode('utf-8'))
                shared[source] = destination
            descriptor['path'] = shared[source]
            descriptor['retained_snapshot'] = True
        result.append(row)
    return result
