import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine.lifecycle_recovery import reconcile_baseline_run
from crescendo_engine.legacy_guardian import inspect_legacy_guardian_attempt as inspect_real_guardian
from crescendo_engine.native_baseline import SCHEMA_VERSION
from crescendo_engine.server import OptimizerService
from crescendo_engine.utils import atomic_write_json


class BaselineLifecycleRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = os.path.join(self.temp.name, 'run')
        self.candidate = os.path.join(self.root, 'candidates', 'g000_c0000')
        self.control = os.path.join(self.candidate, 'native_baseline')
        os.makedirs(self.control)
        self.workspace = os.path.join(self.temp.name, 'suite-workspace')
        os.mkdir(self.workspace)
        self.owner = {'session_id': 'a'*32, 'root': self.temp.name, 'workspace_root': self.workspace}
        self.native = {'schema_version': SCHEMA_VERSION, 'candidate_id': 'g000_c0000',
                       'request_root': self.candidate, 'workspace_root': self.workspace,
                       'launch_attempted': True, 'nonce': 'bound-nonce',
                       'native_session_closed': True, 'cleanup_proven': True,
                       'native_stop': {'requested': False}}
        atomic_write_json(os.path.join(self.candidate, 'candidate.json'), {
            'candidate_id': 'g000_c0000', 'execution_backend': 'maestro_baseline',
            'status': 'infrastructure_failed', 'source_guard': None})
        atomic_write_json(os.path.join(self.candidate, 'setup_private_session_reference.json'), {'capture_kind': 'native_baseline'})
        atomic_write_json(os.path.join(self.workspace, 'workspace.json'), {'setup_private_session': self.owner,
            'source_libraries': {'fixture': self.temp.name},
            'source_snapshots': {'fixture': {'fingerprint': 'fixture-content', 'paths': ['source']}},
            'setup': {'baseline': {'baseline_id': 'frozen-fixture'}}})
        self.write_native()
        self.write_protocol(['READY','RUN_BEGIN','RUN_ID','SIM_DONE','WAIT_RETURN','HISTORY','CLOSED'])
        self.owner_patch = mock.patch('crescendo_engine.setup_private_session.preparation_session_reference', return_value=self.owner)
        self.native_patch = mock.patch('crescendo_engine.setup_private_session._reconcile_owned_capture', side_effect=lambda *a, **k: dict(self.native))
        self.local_patch = mock.patch('crescendo_engine.legacy_guardian.inspect_legacy_guardian_attempt',
                                      return_value={'state':'dead','termination_proof':{'ok':True}})
        self.guard_patch = mock.patch('crescendo_engine.workspace.verify_sources_unchanged', return_value={'ok': True})
        self.owner_mock = self.owner_patch.start()
        self.native_patch.start()
        self.local_mock = self.local_patch.start()
        self.guard_mock = self.guard_patch.start()
        for patch in (self.owner_patch,self.native_patch,self.local_patch,self.guard_patch):
            self.addCleanup(patch.stop)

    def write_native(self):
        atomic_write_json(os.path.join(self.control, 'native_baseline_state.json'), self.native)

    def write_protocol(self, states):
        with open(os.path.join(self.control, 'native_baseline_protocol.tsv'), 'w') as stream:
            for state in states:
                stream.write('bound-nonce\t%s\tvalue\n' % state)

    def test_failed_candidate_without_legacy_workspace_is_retained_after_fresh_proofs(self):
        result = reconcile_baseline_run(self.root)
        self.assertEqual(result['state'], 'completed')
        self.assertFalse(result['raw_deleted'])
        self.assertTrue(os.path.isfile(os.path.join(self.candidate, 'candidate.json')))
        self.guard_mock.assert_called_once()
        self.local_mock.assert_called_once()

    def test_real_guardian_identity_accepts_exact_nonce_and_rejects_wrong_nonce(self):
        from crescendo_engine.netlist_measurement import run_owned_command
        self.local_mock.side_effect = inspect_real_guardian
        result = run_owned_command([sys.executable, '-c', 'print("owned fixture complete")'],
            self.workspace, os.environ.copy(), 10, self.control, self.native['nonce'])
        self.assertEqual(result['returncode'], 0)
        recovered = reconcile_baseline_run(self.root)
        self.assertTrue(recovered['process_termination_proven'])
        self.assertTrue(recovered['artifact_cleanup_complete'])

        self.native['nonce'] = 'different-nonce'
        self.write_native()
        with open(os.path.join(self.control, 'native_baseline_protocol.tsv')) as stream:
            protocol = stream.read()
        with open(os.path.join(self.control, 'native_baseline_protocol.tsv'), 'w') as stream:
            stream.write(protocol.replace('bound-nonce', 'different-nonce'))
        with self.assertRaisesRegex(RuntimeError, 'local process termination'):
            reconcile_baseline_run(self.root)
        # Use the real reader, not the test's blanket dead-process substitute.
        mismatch = inspect_real_guardian(self.control,
            expected_workspace_root=self.workspace,
            expected_token_identity={'legacy': True, 'token': self.native['nonce']})
        self.assertEqual(mismatch['state'], 'ambiguous')
        self.assertIn('token does not match', mismatch['reason'])

    def test_remote_unknown_reports_real_gate_not_missing_legacy_workspace(self):
        self.native.update(cleanup_proven=False, native_stop={'requested':True,'remote_stop_proven':False})
        self.write_native()
        self.write_protocol(['RUN_ID','STOP_BEGIN','STOP_DONE'])
        with self.assertRaisesRegex(RuntimeError, 'Native/remote terminal state') as raised:
            reconcile_baseline_run(self.root)
        self.assertNotIn('lacks a successful persisted source guard', str(raised.exception))
        self.assertTrue(raised.exception.cleanup_failed)
        self.assertTrue(os.path.isdir(self.control))

    def test_dead_native_record_cannot_override_live_process_observation(self):
        self.local_mock.return_value = {'state':'active'}
        with self.assertRaisesRegex(RuntimeError, 'local process termination'):
            reconcile_baseline_run(self.root)

    def test_source_change_remains_blocked(self):
        self.guard_mock.return_value = {'ok':False, 'changed':['fixture']}
        with self.assertRaisesRegex(RuntimeError, 'Source integrity'):
            reconcile_baseline_run(self.root)

    def test_protocol_from_another_request_is_rejected(self):
        self.native['nonce'] = 'different'
        self.write_native()
        with self.assertRaisesRegex(RuntimeError, 'protocol identity mismatch'):
            reconcile_baseline_run(self.root)

    def test_prelaunch_failure_is_retained_without_claiming_source_verification(self):
        self.native.update(launch_attempted=False, native_session_closed=False, cleanup_proven=False)
        self.write_native()
        self.owner_mock.return_value = None
        result = reconcile_baseline_run(self.root)
        self.assertEqual(result['candidates'][0]['source_state'], 'not_exposed_to_native')
        self.guard_mock.assert_not_called()

    def test_service_routes_baseline_to_its_own_recovery(self):
        service = OptimizerService({'runtime':{'run_root':self.temp.name}})
        checkpoint = {'manifest':{'origin_run_id':'run'}, 'spec':{'execution_backend':'maestro_baseline'}}
        with mock.patch('crescendo_engine.server._checkpoint_origin_path', return_value=(self.root,None)), \
             mock.patch('crescendo_engine.server.cleanup_terminal_candidate_artifacts') as legacy:
            result = service._cleanup_checkpoint_origin(checkpoint)
        self.assertEqual(result['state'], 'completed')
        legacy.assert_not_called()

    def test_linked_native_evidence_is_rejected(self):
        path = os.path.join(self.control,'native_baseline_state.json')
        os.rename(path,path+'.real')
        os.symlink(path+'.real',path)
        with self.assertRaises(RuntimeError):
            reconcile_baseline_run(self.root)

    def test_empty_candidate_namespace_is_not_success(self):
        with tempfile.TemporaryDirectory() as empty:
            with self.assertRaisesRegex(RuntimeError, 'absence does not prove'):
                reconcile_baseline_run(empty)

    def test_empty_source_snapshot_is_not_source_proof(self):
        atomic_write_json(os.path.join(self.workspace, 'workspace.json'), {'setup_private_session': self.owner})
        with self.assertRaisesRegex(RuntimeError, 'snapshots.*incomplete'):
            reconcile_baseline_run(self.root)

    def test_conflicting_terminal_events_never_upgrade_remote_unknown(self):
        self.native['native_stop']['remote_stop_proven'] = False
        self.write_native()
        self.write_protocol(['READY','RUN_BEGIN','RUN_ID','SIM_DONE','WAIT_RETURN','HISTORY','CLOSED','STOP_DONE'])
        with self.assertRaisesRegex(RuntimeError, 'Native/remote terminal state'):
            reconcile_baseline_run(self.root)

    def test_disappearing_state_after_source_check_clears_success(self):
        def remove_state(workspace):
            os.unlink(os.path.join(self.control, 'native_baseline_state.json'))
            return {'ok': True}
        self.guard_mock.side_effect = remove_state
        with self.assertRaises(RuntimeError) as error:
            reconcile_baseline_run(self.root)
        self.assertFalse(error.exception.detail['artifact_cleanup_complete'])

    def test_registered_prelaunch_derivation_does_not_require_a_state_rewrite(self):
        self.native.update(launch_attempted=False, native_session_closed=False,
                           cleanup_proven=False, cleanup_pending=False)
        self.write_native()
        with mock.patch('crescendo_engine.setup_private_session._reconcile_owned_capture',
                        return_value=dict(self.native, cleanup_proven=True, cleanup_pending=False)):
            result = reconcile_baseline_run(self.root)
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(result['candidates'][0]['native_terminal_state'], 'not_launched')
