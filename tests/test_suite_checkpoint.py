"""Checkpoint suite reuse requires the same frozen revision and exact selection."""
import copy
import threading
import unittest
from unittest import mock

from crescendo_engine import setup_private_session as sessions
from crescendo_engine.server import OptimizerService
from tests import test_setup_private_session as fixtures


class SuiteCheckpointTests(unittest.TestCase):
    def setUp(self):
        fixture = fixtures.SetupPrivateSessionTests('test_load_creates_real_file_view_and_all_configs_before_parameters')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.fixture = fixture
        self.config = fixture.config
        self.lock = threading.Lock()
        self.setup_private_sessions = {}
        self.warnings = []
        controller = sessions._controller(fixture.reference)
        controller.applied_selection = {'scope_ids': ['scope'], 'parameter_ids': ['parameter']}
        controller.persist()
        self.prepared = copy.deepcopy(fixture.prepared)
        self.prepared.pop('setup_private_session', None)
        self.prepared['cell_selection'] = {'scope_ids': ['scope']}
        self.prepared['parameters'] = [{'parameter_id': 'parameter', 'optimizable': True, 'backend': 'schematic'}]
        self.prepared['source_identity'] = sessions.private_session_info(fixture.reference)['source_identity']
        sessions.close_setup_private_session(fixture.reference)

    def _remember_setup_private_session(self, reference):
        self.setup_private_sessions[reference['session_id']] = reference

    def _set_activity(self, *args, **kwargs):
        self.warnings.append((args, kwargs))

    def attach(self):
        return OptimizerService._attach_live_setup_private_session(self, self.prepared)

    def test_checkpoint_reopens_exact_persisted_suite(self):
        self.attach()
        reference = self.prepared['setup_private_session']
        self.assertEqual(reference['session_id'], self.fixture.reference['session_id'])
        self.assertTrue(sessions.list_setup_private_sessions(self.config)[0]['busy'])
        sessions.close_setup_private_session(reference)

    def test_old_revision_never_borrows_new_suite(self):
        self.prepared['setup']['baseline']['baseline_id'] = 'old-revision'
        self.attach()
        self.assertNotIn('setup_private_session', self.prepared)
        self.assertFalse(sessions.list_setup_private_sessions(self.config)[0]['busy'])

    def test_source_identity_mismatch_releases_candidate(self):
        self.prepared['source_identity']['fingerprint'] = 'different-source'
        self.attach()
        self.assertNotIn('setup_private_session', self.prepared)
        self.assertFalse(sessions.list_setup_private_sessions(self.config)[0]['busy'])
        self.assertFalse(self.setup_private_sessions)

    def test_ambiguous_candidates_never_acquire_a_lease(self):
        candidate = sessions.list_setup_private_sessions(self.config)[0]
        with mock.patch.object(sessions, 'list_setup_private_sessions', return_value=[candidate, candidate]), \
                mock.patch.object(sessions, 'reopen_setup_private_session') as reopen:
            self.attach()
        reopen.assert_not_called()
        self.assertNotIn('setup_private_session', self.prepared)
        self.assertTrue(self.warnings)
