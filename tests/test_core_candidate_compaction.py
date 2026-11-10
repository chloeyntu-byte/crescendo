"""Safety gates for replacing successful sidecars with durable core evidence."""
import copy
import os
import tempfile
import unittest

from crescendo_engine.optimizers import PopulationGenerator
from crescendo_engine.maestro_batch import compact_committed_batch
from crescendo_engine.retention import (UnsafeCleanupError, compact_successful_candidate_artifacts,
    summarize_successful_candidate_artifacts)
from crescendo_engine.run_data_store import RunDataStore, dataset_from_reference
from crescendo_engine.runner import (_checkpoint_document, _checkpoint_checksum,
    checkpoint_candidates, self_contained_checkpoint, validate_checkpoint_integrity)
from crescendo_engine.utils import atomic_write_json, atomic_write_text


class CoreCandidateCompactionTest(unittest.TestCase):
    def make(self, root):
        candidate_root = os.path.join(root, 'candidates', 'g000_c0000')
        os.makedirs(candidate_root)
        spec = {'evaluator': 'mock', 'algorithm': 'random', 'population': 2,
                'variables': [{'parameter_id': 'p', 'minimum': 0.0, 'maximum': 2.0}],
                'objectives': [{'output_key': 'main::gain', 'direction': 'minimize'}], 'constraints': []}
        prepared = {'setup': {'tests': [], 'corners': []}, 'hierarchy': {'designs': []},
                    'parameters': [{'parameter_id': 'p', 'optimizable': True}],
                    'outputs': [{'key': 'main::gain'}]}
        row = {'candidate_id': 'g000_c0000', 'generation': 0, 'index': 0,
               'status': 'completed', 'values': {'p': 1.0}, 'source_guard': {'ok': True},
               'completion_evidence': {'ok': True, 'points': [{'point_id': 1}]},
               'timing': {'elapsed_seconds': 3.0},
               'parameter_application': {'ok': True, 'parameters': [{
                   'parameter_id': 'p', 'requested': 1.0, 'persisted_cdf_value': 1.01,
                   'persisted_schematic_value': 1.01}]}}
        atomic_write_json(os.path.join(candidate_root, 'candidate.json'), row)
        atomic_write_json(os.path.join(candidate_root, 'timing.json'), row['timing'])
        atomic_write_json(os.path.join(candidate_root, 'completion_evidence.json'), row['completion_evidence'])
        writer = RunDataStore(os.path.join(root, 'optimization.crescendo'), {'spec': spec})
        self.addCleanup(writer.close)
        row['artifact_summary'] = summarize_successful_candidate_artifacts(candidate_root, root)
        row['raw_cleanup_proof'] = {'ok': True, 'process_termination_proven': True, 'remote_stop_proven': True}
        writer.commit_candidates([row])
        status = {'run_id': os.path.basename(root), 'run_root': root, 'checkpoint_revision': 1,
                  'completed_candidates': 1}
        generator = PopulationGenerator(spec['variables'], 'random', 1)
        checkpoint = _checkpoint_document(spec, prepared, generator, [row], [], 1, None, status,
                                          candidate_store=writer.reference())
        atomic_write_json(os.path.join(root, 'checkpoint.json'), checkpoint)
        return writer, candidate_root, row, checkpoint

    def test_compact_success_preserves_actual_parameters_timing_and_completion(self):
        with tempfile.TemporaryDirectory() as root:
            writer, candidate_root, row, checkpoint = self.make(root)
            result = compact_successful_candidate_artifacts(candidate_root, root)
            self.assertTrue(result['artifact_cleanup_complete'])
            self.assertFalse(os.path.exists(candidate_root))
            persisted = dataset_from_reference(checkpoint['candidate_store'])['candidates'][0]
            self.assertEqual(persisted['timing']['elapsed_seconds'], 3.0)
            self.assertEqual(persisted['completion_evidence']['points'][0]['point_id'], 1)
            self.assertEqual(persisted['parameter_application']['parameters'][0]['persisted_cdf_value'], 1.01)

    def test_changed_file_or_missing_process_proof_retains_sidecars(self):
        for change_file in (True, False):
            with self.subTest(change_file=change_file), tempfile.TemporaryDirectory() as root:
                writer, candidate_root, row, checkpoint = self.make(root)
                if change_file:
                    atomic_write_text(os.path.join(candidate_root, 'timing.json'), '{}')
                else:
                    row['raw_cleanup_proof']['process_termination_proven'] = False
                    writer.commit_candidates([row])
                    checkpoint['candidate_store'] = writer.reference()
                    checkpoint['payload_sha256'] = _checkpoint_checksum(checkpoint)
                    atomic_write_json(os.path.join(root, 'checkpoint.json'), checkpoint)
                with self.assertRaises(UnsafeCleanupError):
                    compact_successful_candidate_artifacts(candidate_root, root)
                self.assertTrue(os.path.exists(os.path.join(candidate_root, 'candidate.json')))

    def test_portable_checkpoint_preserves_origin_hash_and_rejects_embedded_corruption(self):
        with tempfile.TemporaryDirectory() as root:
            writer, candidate_root, row, checkpoint = self.make(root)
            exported = self_contained_checkpoint(checkpoint)
            self.assertEqual(_checkpoint_checksum(exported), checkpoint['payload_sha256'])
            writer.close()
            os.unlink(writer.path)
            self.assertEqual(checkpoint_candidates(exported)[0]['values'], {'p': 1.0})
            corrupted = copy.deepcopy(exported)
            corrupted['candidate_store_embedded'] = corrupted['candidate_store_embedded'].replace('1.01', '1.02')
            with self.assertRaises(ValueError):
                validate_checkpoint_integrity(corrupted)

    def batch(self, root, row, nonce='verified-nonce'):
        batch_root = os.path.join(root, 'batches', 'g000_g000_c0000')
        os.makedirs(batch_root)
        identity = {'candidate_id': 'batch_1', 'epoch': 'epoch-1', 'sequence': 1, 'nonce': nonce}
        manifest = {'batch_id': identity['candidate_id'], 'protocol_identity': identity,
                    'native_completion_proven': True, 'shared_history': True,
                    'candidates': [{'candidate_id': row['candidate_id'], 'values': row['values']}]}
        marker = os.path.join(batch_root, 'persistent_worker_cleanup.tsv')
        statuses = [dict(identity, state=state, timestamp='Oct 8 20:00:00 2026', message='')
                    for state in ('CLEANUP_BEGIN', 'CLEANUP_DONE')]
        ack = {'schema_version': 'crescendo.persistent_maestro_worker.v1', 'protocol_identity': identity,
               'ok': True, 'tainted': False, 'marker_path': marker, 'status': statuses,
               'states': [item['state'] for item in statuses]}
        atomic_write_json(os.path.join(batch_root, 'persistent_worker_batch.json'), manifest)
        atomic_write_json(os.path.join(batch_root, 'persistent_worker_cleanup.json'), ack)
        atomic_write_text(marker, ''.join('\t'.join(str(item[key]) for key in
            ('epoch', 'sequence', 'nonce', 'candidate_id', 'state', 'timestamp', 'message')) + '\n'
            for item in statuses))
        atomic_write_text(os.path.join(batch_root, 'persistent_worker_batch.il'), 'complete native body\n' * 3000)
        return batch_root, identity

    def test_public_cleanup_summary_uses_exact_native_ack_before_batch_compaction(self):
        with tempfile.TemporaryDirectory() as root:
            _writer, _candidate_root, row, _checkpoint = self.make(root)
            batch_root, identity = self.batch(root, row)
            public = {'ok': True, 'state': 'cleaned', 'raw_paths_deferred': ['owned-native-history']}
            result = compact_committed_batch(root, identity['candidate_id'], public, batch_root=batch_root)
            self.assertTrue(result['artifact_cleanup_complete'], result)
            self.assertEqual(os.listdir(batch_root), ['batch_receipt.json'])
            with open(result['receipt']) as stream:
                receipt = __import__('json').load(stream)
            self.assertEqual(receipt['protocol_identity'], identity)
            self.assertEqual(receipt['native_cleanup_states'], ['CLEANUP_BEGIN', 'CLEANUP_DONE'])
            self.assertEqual(receipt['proof_scope'], 'native_batch_jobs_and_snapshot_cleanup')

    def test_public_success_cannot_substitute_missing_or_mismatched_native_ack(self):
        for fault in ('missing', 'nonce', 'marker', 'symlink'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as root:
                _writer, _candidate_root, row, _checkpoint = self.make(root)
                batch_root, identity = self.batch(root, row)
                ack_path = os.path.join(batch_root, 'persistent_worker_cleanup.json')
                if fault == 'missing':
                    os.unlink(ack_path)
                elif fault == 'nonce':
                    with open(ack_path) as stream:
                        ack = __import__('json').load(stream)
                    ack['protocol_identity']['nonce'] = 'another-request'
                    atomic_write_json(ack_path, ack)
                elif fault == 'marker':
                    atomic_write_text(os.path.join(batch_root, 'persistent_worker_cleanup.tsv'), '')
                else:
                    os.unlink(ack_path)
                    os.symlink(os.path.join(root, 'checkpoint.json'), ack_path)
                result = compact_committed_batch(root, identity['candidate_id'], {'ok': True}, batch_root=batch_root)
                self.assertFalse(result['artifact_cleanup_complete'], result)
                self.assertTrue(os.path.exists(os.path.join(batch_root, 'persistent_worker_batch.il')))


if __name__ == '__main__':
    unittest.main()
