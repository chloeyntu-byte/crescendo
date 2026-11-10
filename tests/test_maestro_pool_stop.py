"""A session can launch native ICRP before candidate simulation is submitted."""
import json
import os
import tempfile
import unittest
from unittest import mock
import test_persistent_maestro as fixture

class NativePoolStopTests(unittest.TestCase):
    def worker(self, root):
        worker, unused, clock = fixture.PersistentMaestroTest()._worker(root)
        props = {'distributionmethod':'Command', 'jobsubmitcommand':'batch-submit -q current',
                 'maxjobs':'2', 'preemptivestart':'1'}
        worker.config['runtime']['maestro_source_job_policy'] = {
            'available':True,'source':'invoking_session',
            'global':{'available':True,'properties':props},
            'tests':[{'name':'T','available':True,'properties':dict(props)}]}
        worker._write_bootstrap({'epoch':worker.epoch,'sequence':0,'nonce':'a'*32,'candidate_id':'__startup__'})
        return worker

    def test_command_pool_owned_before_bootstrap_native_load(self):
        with tempfile.TemporaryDirectory() as root:
            worker=self.worker(root)
            scope=worker._startup_remote_scope
            self.assertTrue(worker.remote_session_possible)
            self.assertTrue(scope['simulation_submitted'])
            self.assertFalse(scope['no_remote_jobs_started'])
            self.assertEqual(scope['submission_scope'],'owned_session_pool_may_prestart')
            with open(scope['manifest_path']) as stream:
                self.assertEqual(json.load(stream)['protocol_identity'],scope['protocol_identity'])

    def test_no_candidate_marker_and_completed_batch_still_close_pool(self):
        for completed in (False,True):
            with self.subTest(completed=completed), tempfile.TemporaryDirectory() as root:
                worker=self.worker(root)
                scope=worker._startup_remote_scope
                scope['simulation_submitted']=False
                scope['native_completion_proven']=completed
                worker._active_batch=None
                worker.process=mock.Mock()
                worker.process.poll.return_value=None
                worker.state='awaiting_cleanup' if completed else 'failed'
                with mock.patch.object(worker,'_send_load'), mock.patch.object(worker,'_write_state'), mock.patch.object(worker,'_wait_protocol',return_value=[{'state':'STOP_DONE'}]):
                    result=worker.request_batch_abort('owned fixture stop')
                self.assertTrue(result['requested'])
                self.assertTrue(result['remote_stop_proven'])
                with open(scope['abort_script_path']) as stream:script=stream.read()
                self.assertLess(script.index('maeStopSimulation(?session session)'),script.index("maeWaitUntilDone('All"))
                self.assertLess(script.index('maeCloseSession(?session session ?forceClose t)'),script.index(' "STOP_DONE"'))
                self.assertFalse(worker.remote_session_possible)

    def test_pool_stop_without_native_ack_retains_unproven_scope(self):
        with tempfile.TemporaryDirectory() as root:
            worker=self.worker(root)
            worker.process=mock.Mock();worker.process.poll.return_value=None
            with mock.patch.object(worker,'_send_load'),mock.patch.object(worker,'_wait_protocol',side_effect=RuntimeError('no native acknowledgement')):
                result=worker.request_batch_abort('owned fixture stop')
            self.assertFalse(result['remote_stop_proven'])
            self.assertTrue(worker.remote_session_possible)
            self.assertFalse(worker._startup_remote_scope['native_stop_proven'])

    def test_local_death_cannot_delete_views_of_unproven_remote_pool(self):
        for operation in ('terminate', 'close'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as root:
                worker=self.worker(root)
                with mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views') as cleanup:
                    with self.assertRaisesRegex(Exception, 'remote worker pool stop is unproven'):
                        getattr(worker, operation)()
                cleanup.assert_not_called()
                state=json.load(open(worker.worker_state_path))
                self.assertFalse(state['remote_session_pool']['native_stop_proven'])
                self.assertTrue(os.path.exists(worker._startup_remote_scope['manifest_path']))

if __name__=='__main__':unittest.main()
