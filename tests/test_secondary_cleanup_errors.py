"""Secondary state/Stop errors must not replace the first native failure."""
import errno
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.persistent_maestro import (
    PersistentMaestroWorker, PersistentMaestroWorkerProcessError,
    PersistentMaestroWorkerProtocolError, PersistentMaestroWorkerStartupError,
    PersistentMaestroWorkerTimeout,
)
from tests import test_persistent_maestro as fixtures
from tests import test_maestro_native_serial as serial_fixtures
from tests import test_maestro_candidate_batch as batch_fixtures


def quota():
    return OSError(errno.EDQUOT, 'secondary diagnostic quota')


class SecondaryCleanupErrorTests(unittest.TestCase):
    def test_all_secondary_steps_are_attempted_without_forging_any_proof(self):
        worker = object.__new__(PersistentMaestroWorker)
        worker.closed = False
        worker.remote_session_possible = True
        worker._startup_remote_scope = {'simulation_submitted': True,
            'native_stop_proven': False, 'native_session_closed': False}
        primary = PersistentMaestroWorkerTimeout('original native timeout')
        with mock.patch.object(worker, '_write_state', side_effect=quota()) as state, \
                mock.patch.object(worker, 'request_batch_abort', side_effect=quota()) as abort, \
                mock.patch.object(worker, 'terminate', side_effect=PersistentMaestroWorkerProcessError('unproven')) as terminate, \
                mock.patch.object(worker, '_log_event', side_effect=quota()):
            secondary = worker._terminate_after_failure('controlled failure', primary, request_native_stop=True)
        self.assertEqual([row['action'] for row in secondary],
                         ['failure_state_write', 'native_stop', 'owned_termination'])
        state.assert_called_once()
        abort.assert_called_once_with(reason='controlled failure')
        terminate.assert_called_once_with(reason='controlled failure')
        self.assertFalse(worker.closed)
        self.assertTrue(worker.remote_session_possible)
        self.assertFalse(worker._startup_remote_scope['native_stop_proven'])
        self.assertFalse(worker._startup_remote_scope['native_session_closed'])
        self.assertEqual(worker._secondary_cleanup_errors[0]['primary_error'], str(primary))

    def test_normal_durable_state_failure_is_not_swallowed_or_submitted(self):
        with tempfile.TemporaryDirectory() as root:
            worker, factory, _clock = fixtures.PersistentMaestroTest()._worker(root)
            self.addCleanup(worker._close_streams)
            worker.start()
            before = list(factory.loaded_paths)
            with mock.patch.object(worker, '_write_state', side_effect=quota()):
                with self.assertRaises(OSError) as raised:
                    worker.evaluate('not_submitted', {'p': 1e-6}, os.path.join(root, 'candidate'))
            self.assertEqual(raised.exception.errno, errno.EDQUOT)
            self.assertEqual(factory.loaded_paths, before)
            self.assertIsNone(worker.pending_cleanup)

    def test_serial_timeout_survives_abort_quota_and_retains_unproven_pool(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = serial_fixtures.NativeSerialTest()
            worker, _factory = fixture.worker(root, mode='stop_error')
            self.addCleanup(worker._close_streams)
            with mock.patch.object(worker, 'request_batch_abort', side_effect=quota()), \
                    mock.patch('crescendo_engine.same_library_views.cleanup_same_library_views') as cleanup_views:
                with self.assertRaises(PersistentMaestroWorkerTimeout):
                    worker.evaluate('timeout', {'p': 1e-6}, os.path.join(root, 'candidate'), timeout_seconds=.5)
            cleanup_views.assert_not_called()
            self.assertTrue(worker.remote_session_possible)
            self.assertFalse(worker.closed)
            self.assertEqual(worker._secondary_cleanup_errors[-1]['secondary_errors'][0]['action'], 'native_stop')

    def test_serial_timeout_survives_failure_state_quota(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = serial_fixtures.NativeSerialTest()
            worker, _factory = fixture.worker(root, mode='stop_error')
            self.addCleanup(worker._close_streams)
            real_write = worker._write_state
            def unavailable(message=None):
                if message and 'PersistentMaestroWorkerTimeout' in message:
                    raise quota()
                return real_write(message)
            with mock.patch.object(worker, '_write_state', side_effect=unavailable):
                with self.assertRaises(PersistentMaestroWorkerTimeout):
                    worker.evaluate('timeout', {'p': 1e-6}, os.path.join(root, 'candidate'), timeout_seconds=.5)
            self.assertEqual(worker._secondary_cleanup_errors[-1]['secondary_errors'][0]['action'], 'failure_state_write')
            self.assertFalse(worker.closed)

    def test_startup_native_error_survives_failure_state_quota(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = fixtures.PersistentMaestroTest()._worker(root, mode='startup_error')
            self.addCleanup(worker._close_streams)
            real_write = worker._write_state
            def unavailable(message=None):
                if message and 'PersistentMaestroWorkerStartupError' in message:
                    raise quota()
                return real_write(message)
            with mock.patch.object(worker, '_write_state', side_effect=unavailable):
                with self.assertRaisesRegex(PersistentMaestroWorkerStartupError, 'bootstrap failed'):
                    worker.start()
            self.assertEqual(worker._secondary_cleanup_errors[-1]['secondary_errors'][0]['action'], 'failure_state_write')

    def test_batch_protocol_error_survives_secondary_abort_and_state_errors(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = batch_fixtures.CandidateBatchTest()
            worker, _factory, _clock = fixture.worker(root)
            self.addCleanup(worker._close_streams)
            primary = PersistentMaestroWorkerProtocolError('original batch protocol failure')
            real_write = worker._write_state
            def unavailable(message=None):
                if message and str(primary) in message:
                    raise quota()
                return real_write(message)
            with mock.patch.object(worker, '_wait_protocol', side_effect=primary), \
                    mock.patch.object(worker, '_write_state', side_effect=unavailable), \
                    mock.patch.object(worker, 'request_batch_abort', side_effect=quota()):
                with self.assertRaises(PersistentMaestroWorkerProtocolError) as raised:
                    worker.evaluate_batch(fixture.requests(root, 1), os.path.join(root, 'batch'))
            self.assertIs(raised.exception, primary)
            self.assertEqual([row['action'] for row in worker._secondary_cleanup_errors[-1]['secondary_errors']][:2],
                             ['failure_state_write', 'native_stop'])

    def test_result_cleanup_error_survives_secondary_state_and_termination_errors(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = fixtures.PersistentMaestroTest()._worker(root)
            self.addCleanup(worker._close_streams)
            worker.start()
            artifact = worker.evaluate('one', {'p': 1e-6}, os.path.join(root, 'candidate'))
            primary = PersistentMaestroWorkerTimeout('original cleanup timeout')
            real_write = worker._write_state
            def unavailable(message=None):
                if message and str(primary) in message:
                    raise quota()
                return real_write(message)
            with mock.patch.object(worker, '_wait_protocol', side_effect=primary), \
                    mock.patch.object(worker, '_write_state', side_effect=unavailable), \
                    mock.patch.object(worker, 'terminate', side_effect=PersistentMaestroWorkerProcessError('unproven')):
                with self.assertRaises(PersistentMaestroWorkerTimeout) as raised:
                    worker.request_cleanup(artifact)
            self.assertIs(raised.exception, primary)
            self.assertEqual([row['action'] for row in worker._secondary_cleanup_errors[-1]['secondary_errors']],
                             ['failure_state_write', 'owned_termination'])

    def test_graceful_close_error_survives_secondary_termination_error(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = fixtures.PersistentMaestroTest()._worker(root)
            self.addCleanup(worker._close_streams)
            worker.start()
            primary = PersistentMaestroWorkerProtocolError('original graceful close error')
            with mock.patch.object(worker, '_wait_protocol', side_effect=primary), \
                    mock.patch.object(worker, 'terminate', side_effect=PersistentMaestroWorkerProcessError('unproven')):
                with self.assertRaises(PersistentMaestroWorkerProtocolError) as raised:
                    worker.close()
            self.assertIs(raised.exception, primary)
            self.assertFalse(worker.closed)
            self.assertEqual(worker._secondary_cleanup_errors[-1]['secondary_errors'][0]['action'], 'owned_termination')


if __name__ == '__main__':
    unittest.main()
