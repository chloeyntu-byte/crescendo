"""Diagnostic indexes are durable and never grant remote Stop authority."""
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.native_baseline import _NativeSession, _record_storage_evidence
from crescendo_engine.storage_policy import native_workspace_storage
from crescendo_engine.environment_test import _engine_deadline_budget


class NativeStorageProducerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.workspace = os.path.join(self.tmp.name, 'workspace')
        self.storage_root = os.path.join(self.workspace, 'native_baseline_runs', 'a' * 32)
        os.makedirs(self.storage_root)
        self.candidate = os.path.join(self.tmp.name, 'candidate')
        os.mkdir(self.candidate)
        self.driver = _NativeSession(self.candidate, {'root': self.workspace,
            'native_storage': native_workspace_storage(self.storage_root)}, {}, 'g000_c0000')
        self.path = os.path.join(self.driver.control, 'native_storage_evidence.json')

    def test_binding_is_saved_before_run_and_tracks_actual_root_readback(self):
        _record_storage_evidence(self.driver)
        with open(self.path) as stream:
            before = json.load(stream)
        self.assertFalse(before['native_readback_verified'])
        storage = self.driver.workspace['native_storage']
        # results_location is optional: the three configured roots are verified.
        storage.update(configured_roots_verified=True, verified=False)
        _record_storage_evidence(self.driver)
        with open(self.path) as stream:
            after = json.load(stream)
        self.assertEqual(before['nonce'], self.driver.nonce)
        self.assertEqual(before['storage_identity'], after['storage_identity'])
        self.assertTrue(after['native_readback_verified'])
        self.assertEqual(after['storage_root'], self.storage_root)
        self.assertFalse(self.driver.state['cleanup_proven'])
        self.assertFalse(self.driver.state['native_stop']['remote_stop_proven'])

    def test_bad_path_is_reported_without_claiming_binding(self):
        self.driver.workspace['native_storage']['readback_path'] = os.path.join(self.tmp.name, 'foreign', 'native_storage.records')
        _record_storage_evidence(self.driver)
        self.assertFalse(os.path.exists(self.path))
        self.assertIn('diagnostic_evidence_error', self.driver.state)
        self.assertFalse(self.driver.state['cleanup_proven'])

    def test_optional_manifest_write_failure_does_not_forge_success(self):
        from crescendo_engine import native_baseline
        original = native_baseline.atomic_write_json
        def fail_index(path, data):
            if path == self.path:
                raise OSError('diagnostic index full')
            return original(path, data)
        with mock.patch.object(native_baseline, 'atomic_write_json', side_effect=fail_index):
            _record_storage_evidence(self.driver)
        self.assertIn('diagnostic index full', self.driver.state['diagnostic_evidence_error'])
        self.assertFalse(self.driver.state['cleanup_proven'])

    def test_unlimited_candidate_does_not_shorten_engine_deadline_to_179(self):
        self.assertEqual(780, _engine_deadline_budget({}, {}, {'candidate_timeout_seconds': -1}))
        self.assertEqual(1380, _engine_deadline_budget({}, {}, {'candidate_timeout_seconds': 1200}))
        self.assertEqual(2400, _engine_deadline_budget({'engine_timeout_seconds': 2400}, {}, {'candidate_timeout_seconds': -1}))
        self.assertEqual(3600, _engine_deadline_budget({}, {'environment_test_engine_timeout_seconds': 3600}, {'candidate_timeout_seconds': -1}))
