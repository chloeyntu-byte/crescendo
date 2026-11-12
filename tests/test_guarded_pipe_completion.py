"""Guarded output drain and concurrent first cause; no native EDA launch."""
import io
import errno
import os
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import unittest
from unittest import mock

from crescendo_engine.cadence import run_command
from crescendo_engine.environment_diagnostics import failure_summary
from crescendo_engine.netlist_execution import NetlistExecutionError
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.private_netlist import PrivateNetlistError
from crescendo_engine import runner
from tests.test_netlist_optimizer import fixtures, model_task


GUARDED_ATTEMPT_ID = 'test-token'
PIPE_ATTEMPT_ID = 'pipe-fixture'


class ExitedPipeProcess(object):
    pid = 987654321
    returncode = 0

    def __init__(self, pending=1, hook=None):
        self.pending, self.hook, self.calls = pending, hook, 0
        self.stdin = io.BytesIO()

    def poll(self):
        return 0

    def wait(self, timeout=None):
        return 0

    def communicate(self, timeout=None):
        self.calls += 1
        if self.hook:
            self.hook(self.calls)
        if self.pending:
            self.pending -= 1
            time.sleep(min(float(timeout or 0.001), 0.005))
            raise subprocess.TimeoutExpired(['owned-guardian'], timeout, output=b'partial')
        return b'wrapper done', b''


class GuardedPipeCompletionTests(unittest.TestCase):
    def run_guarded(self, process, proof='dead', timeout=2, cancelled=None, grace=.02):
        with tempfile.TemporaryDirectory() as root:
            with mock.patch('crescendo_engine.cadence.subprocess.Popen', return_value=process) as launched, \
                    mock.patch('crescendo_engine.cadence.os.getpgid', return_value=process.pid), \
                    mock.patch('crescendo_engine.cadence.os.killpg', side_effect=ProcessLookupError(errno.ESRCH, 'owned group exited')), \
                    mock.patch('crescendo_engine.cadence.inspect_legacy_guardian_attempt', return_value={'state':proof}) as inspect:
                result = run_command(['/bin/true'], root, {}, timeout,
                    stdout_path=os.path.join(root,'ocean.stdout'), stderr_path=os.path.join(root,'ocean.stderr'),
                    guardian_state_path=os.path.join(root,'guardian_state.json'), guardian_token=GUARDED_ATTEMPT_ID,
                    cancel_check=cancelled, termination_grace_seconds=grace, termination_kill_seconds=.01)
                launched.assert_called_once()
                inspect.assert_called_once()
                return result

    def test_short_output_poll_is_not_command_failure(self):
        process = ExitedPipeProcess(pending=1)
        result = self.run_guarded(process)
        self.assertEqual(result['returncode'], 0)
        self.assertFalse(result['timed_out'])
        self.assertFalse(result['termination_failed'])
        self.assertEqual(result['guardian_cleanup']['state'], 'dead')
        self.assertIn('wrapper done', result['stdout_tail'])
        self.assertEqual(process.calls, 2)

    def test_real_exited_process_pipe_remains_open_until_owned_writer_finishes(self):
        # Two real Python subprocesses, no EDA. The writer is our child, so
        # this test can always reap it; only guardian argv/proof are fixtures.
        original_popen = subprocess.Popen
        children, short_timeouts = [], []
        wrapper = [sys.executable, '-c', 'import sys,time; sys.stderr.write("early stderr\\n"); sys.stderr.flush(); time.sleep(.15)']

        def launch(argv, **kwargs):
            process = original_popen(argv, **kwargs)
            children.append(process)
            writer_fd = os.open('/proc/%d/fd/1' % process.pid, os.O_WRONLY)
            try:
                writer = original_popen([sys.executable, '-c',
                    'import time; time.sleep(.45); print("late owned pipe output")'],
                    stdout=writer_fd, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
                    close_fds=True, start_new_session=True)
                children.append(writer)
            finally:
                os.close(writer_fd)
            communicate = process.communicate

            def drain(timeout=None):
                try:
                    return communicate(timeout=timeout)
                except subprocess.TimeoutExpired:
                    short_timeouts.append(timeout)
                    self.assertEqual(process.poll(), 0)
                    raise
            process.communicate = drain
            return process

        try:
            with tempfile.TemporaryDirectory() as root:
                with mock.patch('crescendo_engine.cadence.build_guardian_command', return_value=wrapper), \
                        mock.patch('crescendo_engine.cadence.subprocess.Popen', side_effect=launch) as launched, \
                        mock.patch('crescendo_engine.cadence.inspect_legacy_guardian_attempt', return_value={'state':'dead'}) as inspect:
                    result = run_command(['/bin/true'], root, os.environ.copy(), 2,
                        stdout_path=os.path.join(root,'ocean.stdout'), stderr_path=os.path.join(root,'ocean.stderr'),
                        guardian_state_path=os.path.join(root,'guardian_state.json'), guardian_token=PIPE_ATTEMPT_ID,
                        termination_grace_seconds=1, termination_kill_seconds=.05)
                    launched.assert_called_once()
                    inspect.assert_called_once()
                self.assertTrue(short_timeouts)
                self.assertTrue(all(value <= .1 for value in short_timeouts))
                self.assertEqual(result['returncode'], 0)
                self.assertFalse(result['timed_out'])
                self.assertFalse(result['termination_failed'])
                self.assertIn('late owned pipe output', result['stdout_tail'])
                self.assertIn('early stderr', result['stderr_tail'])
        finally:
            for child in children:
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=2)

    def test_exit_and_drained_output_do_not_replace_owned_stop_proof(self):
        result = self.run_guarded(ExitedPipeProcess(pending=1), proof='unknown')
        self.assertTrue(result['termination_failed'])
        self.assertNotEqual(result['returncode'], 0)

    def test_unlimited_candidate_still_bounds_post_exit_pipe_drain(self):
        started = time.monotonic()
        result = self.run_guarded(ExitedPipeProcess(pending=10000), timeout=None)
        self.assertLess(time.monotonic()-started, .5)
        self.assertTrue(result['timed_out'])
        self.assertTrue(result['termination_failed'])
        self.assertTrue(result['output_drain_timed_out'])

    def test_drain_deadline_failure_does_not_override_later_proven_cleanup(self):
        result = self.run_guarded(ExitedPipeProcess(pending=2), timeout=None, grace=.01)
        self.assertTrue(result['timed_out'])
        self.assertTrue(result['output_drain_timed_out'])
        self.assertFalse(result['termination_failed'])
        self.assertEqual(result['guardian_cleanup']['state'],'dead')
        self.assertIn('wrapper done', result['stdout_tail'])

    def test_cancel_during_pipe_drain_keeps_stop_path(self):
        stopped = [False]
        process = ExitedPipeProcess(pending=1, hook=lambda call: stopped.__setitem__(0,True))
        result = self.run_guarded(process, cancelled=lambda: stopped[0])
        self.assertTrue(result['cancelled'])
        self.assertFalse(result['timed_out'])

    def test_original_command_deadline_limits_pipe_drain(self):
        result = self.run_guarded(ExitedPipeProcess(pending=10000), timeout=.01, grace=1)
        self.assertTrue(result['timed_out'])
        self.assertFalse(result['output_drain_timed_out'])


class ConcurrentFirstCauseTests(unittest.TestCase):
    def test_peer_cancel_does_not_replace_original_fatal_or_native_cause(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root,spec,prepared,prepared['parameters'],config,runner._failure_metrics)
            adapter.preparing = True
            adapter.prepared_event.set()
            adapter.manifest = {'source_identity':{}}
            adapter.context = mock.Mock()
            adapter.context.unsafe.is_set.return_value = False
            error = NetlistExecutionError('measurement reader failed', {'failure_stage':'measurement'})
            adapter.context.evaluate.side_effect = error
            with mock.patch('crescendo_engine.netlist_optimizer.materialize_candidate', return_value={'tasks':[model_task()]}):
                first = adapter.evaluate(3,8,{'x':.4})
            with mock.patch.object(adapter,'_prepare_once', side_effect=PrivateNetlistError('Maestro generation cancelled while waiting for the shared suite')):
                peer = adapter.evaluate(0,8,{'x':.5})
            self.assertEqual(adapter.fatal_error, first['error'])
            self.assertEqual(peer['primary_failure']['candidate_id'], first['candidate_id'])
            self.assertTrue(peer['secondary_failure'])
            self.assertEqual(peer['failure_stage'],'peer_cancellation')
            self.assertNotIn('metrics',peer)
            summary = failure_summary({'state':'failed','primary_failure':{'failure_stage':'optimization','message':adapter.fatal_error}},[peer,first])
            self.assertEqual(summary['first_cause']['candidate_id'],'g008_c0003')
            self.assertEqual(summary['first_cause']['failure_stage'],'measurement')

    def test_two_threads_preserve_origin_when_unsafe_or_fatal_reaches_peer_first(self):
        for peer_first in (False, True):
            with self.subTest(peer_first=peer_first), tempfile.TemporaryDirectory() as root:
                prepared, config, spec = fixtures(root)
                adapter = PrivateNetlistOptimizer(root,spec,prepared,prepared['parameters'],config,runner._failure_metrics)
                adapter.manifest = {'source_identity':{}}
                adapter.context = mock.Mock()
                adapter.context.unsafe = threading.Event()
                origin_ready, peer_done, fatal_latched = threading.Event(), threading.Event(), threading.Event()
                local = threading.local()
                adapter.context.cancel.side_effect = fatal_latched.set

                def prepare(_phases):
                    if local.index == 0:
                        self.assertTrue(origin_ready.wait(10))
                        if not peer_first:
                            self.assertTrue(fatal_latched.wait(10))
                        raise PrivateNetlistError('Maestro generation cancelled while waiting for the shared suite')
                    return adapter.manifest

                def measure(*args, **kwargs):
                    # Measurement cleanup can publish unsafe before its
                    # original exception reaches the adapter thread.
                    adapter.context.unsafe.set()
                    origin_ready.set()
                    if peer_first:
                        self.assertTrue(peer_done.wait(10))
                    raise NetlistExecutionError('original measurement failed', {'failure_stage':'measurement'})

                def evaluate(index):
                    local.index = index
                    try:
                        return adapter.evaluate(index,8,{'x':.4})
                    finally:
                        if index == 0:
                            peer_done.set()

                adapter.context.evaluate.side_effect = measure
                with mock.patch.object(adapter,'_prepare_once', side_effect=prepare), \
                        mock.patch('crescendo_engine.netlist_optimizer.materialize_candidate', return_value={'tasks':[model_task()]}):
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        origin, peer = pool.submit(evaluate,3), pool.submit(evaluate,0)
                        origin, peer = origin.result(timeout=3), peer.result(timeout=3)
                self.assertEqual(adapter.primary_failure['candidate_id'],'g008_c0003')
                self.assertEqual(adapter.fatal_error,origin['error'])
                self.assertNotIn('metrics',origin)
                self.assertNotIn('metrics',peer)
                summary = failure_summary({'state':'failed'}, [peer,origin])
                self.assertEqual(summary['first_cause']['candidate_id'],'g008_c0003')
                self.assertEqual(summary['first_cause']['failure_stage'],'measurement')

    def test_legacy_sorted_peer_cancellation_does_not_hide_real_failure(self):
        peer = {'candidate_id':'g008_c0000','status':'infrastructure_failed','failure_stage':'netlist_preparation',
                'error':'PrivateNetlistError: Maestro generation cancelled while waiting for the shared suite'}
        real = {'candidate_id':'g008_c0003','status':'infrastructure_failed','failure_stage':'solver_submission_or_measurement',
                'error':"TimeoutExpired: Command guardian.py measurement timed out after 0.1 seconds"}
        cause = failure_summary({'state':'failed','primary_failure':{'failure_stage':'optimization','message':'aggregate'}}, [peer,real])['first_cause']
        self.assertEqual(cause['candidate_id'], real['candidate_id'])
        self.assertEqual(cause['error'], real['error'])
