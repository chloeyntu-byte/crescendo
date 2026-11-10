import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.runner import _reconcile_baseline_shutdown, _write_worker_shutdown
from crescendo_engine.shutdown_evidence import shutdown_proven
from crescendo_engine.utils import load_json


class BaselineShutdownTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.candidate = os.path.join(self.root, 'candidates', 'one')
        os.makedirs(self.candidate)
        self.complete = {'launch_attempted': True, 'cleanup_proven': True,
                         'native_session_closed': True, 'native_stop': {'requested': False},
                         'local_cleanup': {'state': 'dead', 'termination_proof': {'ok': True}}}

    def tearDown(self):
        self.temp.cleanup()

    def produce(self, proof, roots=None):
        with mock.patch('crescendo_engine.native_baseline.reconcile_native_baseline', return_value=proof) as reconcile:
            result = _reconcile_baseline_shutdown(self.root, roots if roots is not None else [self.candidate])
        _write_worker_shutdown(self.root, result['workers'], result['final_cleanup'])
        return result, load_json(os.path.join(self.root, 'worker_shutdown.json')), reconcile

    def test_completed_native_run_has_standard_proof_but_retains_raw(self):
        artifact = os.path.join(self.candidate, 'keep.raw')
        with open(artifact, 'w') as stream:
            stream.write('native data')
        result, shutdown, reconcile = self.produce(self.complete)
        self.assertTrue(shutdown_proven(shutdown))
        self.assertTrue(os.path.isfile(artifact))
        self.assertFalse(result['final_cleanup']['raw_deleted'])
        self.assertEqual(result['candidate_proofs'][self.candidate]['native_terminal_state'], 'completed')
        reconcile.assert_called_once_with(self.candidate, request_cancel=True, timeout_seconds=12)

    def test_stopped_local_request_uses_native_stop_proof(self):
        proof = copy.deepcopy(self.complete)
        proof['native_stop'] = {'requested': True, 'remote_stop_proven': True}
        result, shutdown, _ = self.produce(proof)
        self.assertTrue(shutdown_proven(shutdown))
        self.assertEqual(result['candidate_proofs'][self.candidate]['native_terminal_state'], 'cancelled')

    def test_remote_stop_unknown_cannot_be_upgraded_from_local_death(self):
        proof = copy.deepcopy(self.complete)
        proof.update(cleanup_proven=False, native_stop={'requested': True, 'remote_stop_proven': False})
        result, shutdown, _ = self.produce(proof)
        self.assertFalse(shutdown_proven(shutdown))
        self.assertTrue(result['final_cleanup']['process_termination_proven'])
        self.assertFalse(result['final_cleanup']['remote_stop_proven'])

    def test_existing_candidate_with_missing_native_state_is_unknown(self):
        result = _reconcile_baseline_shutdown(self.root, [self.candidate])
        self.assertFalse(result['workers'][0]['ok'])
        self.assertEqual(result['candidate_proofs'][self.candidate]['native_terminal_state'], 'unknown')

    def test_no_native_roots_means_no_submitted_work_not_failure(self):
        result, shutdown, reconcile = self.produce({}, [])
        reconcile.assert_not_called()
        self.assertTrue(shutdown_proven(shutdown))
        self.assertTrue(result['final_cleanup']['no_native_requests_created'])
        self.assertEqual(result['final_cleanup']['root_index']['count'], 0)

    def test_unlaunched_request_with_durable_proof_is_safe(self):
        result, shutdown, _ = self.produce({'launch_attempted': False, 'cleanup_proven': True})
        self.assertTrue(shutdown_proven(shutdown))
        self.assertEqual(result['candidate_proofs'][self.candidate]['native_terminal_state'], 'not_launched')

    def test_only_explicit_roots_reconciled_and_summary_is_bounded(self):
        roots = []
        for index in range(40):
            root = os.path.join(self.root, 'candidates', 'c%03d' % index)
            os.makedirs(root)
            roots.append(root)
        result, shutdown, reconcile = self.produce(self.complete, roots + roots[:1])
        self.assertEqual(reconcile.call_count, 40)
        self.assertEqual(result['final_cleanup']['root_index']['count'], 40)
        self.assertEqual(len(result['final_cleanup']['retained_roots_preview']), 16)
        self.assertNotIn(self.candidate, [call[0][0] for call in reconcile.call_args_list])
        self.assertLess(os.path.getsize(os.path.join(self.root, 'worker_shutdown.json')), 2048)
        self.assertTrue(shutdown_proven(shutdown))

    def test_escaping_root_never_reconciled_or_written(self):
        result, shutdown, reconcile = self.produce(self.complete, [self.root])
        reconcile.assert_not_called()
        self.assertFalse(shutdown_proven(shutdown))
        self.assertFalse(os.path.exists(os.path.join(self.root, 'raw_cleanup_proof.json')))
