"""A completed batch does not prove closure of a persistent native job pool."""
import hashlib
import os
import unittest

from crescendo_engine.retention import (
    UnsafeCleanupError, cleanup_abandoned_worker_epoch, safe_delete_worker_epoch,
)
from tests import test_cluster_retention as batch_fixture


class SessionPoolRetentionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = batch_fixture.RemoteBatchRetentionTests()
        self.fixture.setUp()
        source_root = os.path.join(self.fixture.root, 'source')
        os.makedirs(source_root)
        self.source = os.path.join(source_root, 'original.oa')
        self.permanent = os.path.join(source_root, 'schematic_optimized.oa')
        for path in (self.source, self.permanent):
            with open(path, 'wb') as stream:
                stream.write(b'original or permanent source, never disposable')
        self.source_hash = self._source_hash()

    def tearDown(self):
        self.fixture.tearDown()

    def _source_hash(self):
        with open(self.source, 'rb') as stream:
            return hashlib.sha256(stream.read()).hexdigest()

    def state(self, pool, batch=None):
        self.fixture.write(os.path.join(self.fixture.epoch, 'worker_state.json'), {
            'remote_batch': batch or {'simulation_submitted': True, 'native_completion_proven': True},
            'remote_session_pool': pool})

    def test_completed_batch_cannot_authorize_unclosed_persistent_pool_deletion(self):
        for pool in (
                {'simulation_submitted': True, 'native_stop_proven': False, 'native_session_closed': False},
                {'simulation_submitted': True, 'native_stop_proven': True, 'native_session_closed': False},
                {'simulation_submitted': True, 'native_stop_proven': False, 'native_session_closed': True},
                {'simulation_submitted': True, 'native_stop_proven': 1, 'native_session_closed': 1}):
            self.state(pool)
            for operation in (
                    lambda: safe_delete_worker_epoch(self.fixture.epoch, self.fixture.run, {'ok': True}),
                    lambda: cleanup_abandoned_worker_epoch(
                        self.fixture.epoch, self.fixture.run,
                        lambda _: self.fail('source verification must not authorize an active session pool'),
                        termination_proof={'ok': True, 'termination_proven': True})):
                with self.assertRaisesRegex(UnsafeCleanupError, 'session pool closure/cancellation'):
                    operation()
                self.assertTrue(os.path.isfile(os.path.join(self.fixture.workspace, 'workspace.json')))
                self.assertEqual(self._source_hash(), self.source_hash)

    def test_unknown_or_numeric_pool_submission_fails_closed(self):
        for pool in ({}, [], {'simulation_submitted': 1}, {'simulation_submitted': 'false'}):
            self.state(pool)
            with self.assertRaisesRegex(UnsafeCleanupError, 'session pool state is unknown'):
                safe_delete_worker_epoch(self.fixture.epoch, self.fixture.run, {'ok': True})
            self.assertTrue(os.path.isdir(self.fixture.workspace))

    def test_closed_pool_and_real_source_guard_delete_only_owned_epoch(self):
        self.state({'simulation_submitted': True, 'native_stop_proven': True, 'native_session_closed': True})
        result = cleanup_abandoned_worker_epoch(
            self.fixture.epoch, self.fixture.run,
            lambda _: {'ok': self._source_hash() == self.source_hash},
            termination_proof={'ok': True, 'termination_proven': True})
        self.assertTrue(result['removed'])
        self.assertFalse(os.path.lexists(self.fixture.epoch))
        self.assertEqual(self._source_hash(), self.source_hash)
        self.assertTrue(os.path.isfile(self.permanent))

    def test_closed_pool_does_not_bypass_source_guard_or_unknown_batch(self):
        closed = {'simulation_submitted': True, 'native_stop_proven': True, 'native_session_closed': True}
        self.state(closed)
        with self.assertRaisesRegex(UnsafeCleanupError, 'source integrity'):
            safe_delete_worker_epoch(self.fixture.epoch, self.fixture.run, {'ok': False})
        self.state(closed, {'simulation_submitted': True})
        with self.assertRaisesRegex(UnsafeCleanupError, 'remote Maestro batch'):
            safe_delete_worker_epoch(self.fixture.epoch, self.fixture.run, {'ok': True})
        self.assertTrue(os.path.isfile(self.source))
        self.assertTrue(os.path.isfile(self.permanent))

    def test_legacy_without_pool_field_keeps_batch_contract(self):
        self.fixture.state({'simulation_submitted': True, 'native_stop_proven': True})
        self.assertTrue(safe_delete_worker_epoch(self.fixture.epoch, self.fixture.run, {'ok': True})['removed'])
        self.assertTrue(os.path.isfile(self.source))
        self.assertTrue(os.path.isfile(self.permanent))


if __name__ == '__main__':
    unittest.main()
