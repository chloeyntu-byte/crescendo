import copy
import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine import unified_configuration as config
from crescendo_engine.cadence import CommandCancelled
from crescendo_engine.private_suite_service import PrivateSuiteServiceMixin
from crescendo_engine.server import OptimizerService


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, 'configuration.json')
        self.document = config.build_configuration({})

    def test_two_writers_from_same_revision_only_one_wins(self):
        initial = config.save_configuration(self.path, self.document, return_result=True)
        barrier = threading.Barrier(2)
        outcomes = []
        def write(value):
            document = copy.deepcopy(self.document)
            document['saved_at'] = 'revision-' + value
            barrier.wait()
            try:
                outcomes.append(config.save_configuration(self.path, document,
                    expected_revision=initial['revision'], return_result=True))
            except config.ConfigurationConflict:
                outcomes.append('conflict')
        threads = [threading.Thread(target=write, args=(str(i),)) for i in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10)
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(outcomes.count('conflict'), 1)
        result = next(item for item in outcomes if isinstance(item, dict))
        self.assertEqual(config.configuration_revision(self.path), result['revision'])

    def test_replace_failure_retains_previous_and_revision(self):
        initial = config.save_configuration(self.path, self.document, return_result=True)
        with mock.patch('crescendo_engine.unified_configuration.os.replace', side_effect=OSError('disk error')):
            with self.assertRaises(OSError):
                config.save_configuration(self.path, self.document, expected_revision=initial['revision'])
        self.assertEqual(config.configuration_revision(self.path), initial['revision'])

    def test_failed_backup_sync_does_not_leave_partial_backup(self):
        initial = config.save_configuration(self.path, self.document, return_result=True)
        with mock.patch('crescendo_engine.unified_configuration.os.fsync', side_effect=OSError('backup sync failed')):
            with self.assertRaises(OSError):
                config.save_configuration(self.path, self.document, expected_revision=initial['revision'])
        self.assertEqual(config.configuration_revision(self.path), initial['revision'])
        self.assertFalse(any('.bak-' in name for name in os.listdir(self.temp.name)))

    def test_directory_sync_failure_reports_uncertain_commit(self):
        real_sync = os.fsync
        count = [0]
        def sync(fd):
            count[0] += 1
            if count[0] == 2:
                raise OSError('directory sync failure')
            real_sync(fd)
        with mock.patch('crescendo_engine.unified_configuration.os.fsync', side_effect=sync):
            with self.assertRaises(OSError) as caught:
                config.save_configuration(self.path, self.document, expected_revision=None)
        self.assertEqual(caught.exception.configuration_commit_state, 'committed_durability_unknown')
        self.assertIsNotNone(config.configuration_revision(self.path))


class SuiteFailureTests(unittest.TestCase):
    def test_failed_release_retains_service_authority_and_restores_config(self):
        svc = mock.Mock()
        svc.prepare_cancel_event = threading.Event()
        svc.config = {'source': 'old'}
        svc.prepared, svc.inspections, svc.test_inspections = {}, {}, {}
        svc.inspection_epoch = 0
        svc.lock = threading.Lock()
        svc.setup_private_sessions = {}
        def fail_after_obtain(suite_id):
            svc.config['source'] = 'new'
            svc.setup_private_sessions[suite_id] = {'session_id': suite_id}
            raise RuntimeError('failure')
        svc._load_private_suite_registered.side_effect = fail_after_obtain
        with mock.patch('crescendo_engine.private_suite_service.sessions.close_setup_private_session',
                        return_value={'cleanup_proven': False, 'state': 'unproven'}):
            with self.assertRaises(RuntimeError) as caught:
                PrivateSuiteServiceMixin.load_private_suite(svc, 'suite')
        self.assertTrue(caught.exception.cleanup_failed)
        self.assertIn('suite', svc.setup_private_sessions)
        self.assertEqual(svc.config, {'source': 'old'})

    def test_configuration_does_not_reopen_saved_suite(self):
        svc = OptimizerService({'runtime': {'run_root': '/tmp'}})
        svc.setup_private_sessions['active'] = {'session_id': 'active'}
        document = config.build_configuration({})
        document['source_setup']['path'] = '/tmp/example'
        document['private_suite'] = {'suite_id': 'a' * 32, 'source_path': '/tmp/example'}
        with mock.patch.object(svc, 'load_private_suite') as reopen:
            result = svc.load_configuration(document, path='/tmp/example')
        reopen.assert_not_called()
        self.assertTrue(result['preserve_current_suite'])
        self.assertIn('active', svc.setup_private_sessions)
        self.assertTrue(any('catalogue' in warning for warning in result['warnings']))
