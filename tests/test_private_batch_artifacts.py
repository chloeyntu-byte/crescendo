"""Logical audit files and compaction barriers through the formal run entry."""
import hashlib
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import generation_batch, retention, runner
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.run_data_store import dataset_from_reference
from crescendo_engine.single_test import project_setup
from crescendo_engine.utils import atomic_write_json, load_json
from tests.test_netlist_optimizer import fixtures, parsed


class PrivateBatchArtifactTests(unittest.TestCase):
    def run_case(self, stopped=False, failed=False):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            spec.update(execution_backend='maestro_netlist', native_candidate_batching='generation',
                        generations=1)
            prepared['source_identity'] = {'schema_version': 'crescendo.source_design_identity.v1',
                'fingerprint': 'fixture-source', 'libraries': []}
            prepared['setup']['tests'] = [{'name': 'main', 'enabled': True}]
            prepared['setup'] = project_setup(prepared['setup'], 'main')
            if failed:
                spec['maestro_job_overrides']['max_jobs'] = 1
            run_root = os.path.join(root, 'run')
            physical_calls, compacted, ledgers = [], [], []
            owner = self

            class Adapter(PrivateNetlistOptimizer):
                def __init__(self, *args, **kwargs):
                    super(Adapter, self).__init__(*args, **kwargs)
                    self.environment_check = None
                    self._prepare_once = mock.Mock()
                    self._apply_native_fallback = mock.Mock()
                    self.verify_sources = mock.Mock(return_value={'ok': True})
                    self.context = mock.Mock()
                    self.context.close.return_value = {'cleanup_proven': True}
                    self.maestro_producer = mock.Mock()
                    self.maestro_producer.close.return_value = {'cleanup_proven': True}
                    self.maestro_producer.generate_batch.side_effect = self.capture

                def capture(self, plan):
                    batch_root = os.path.join(run_root, 'candidates', 'batch_' + plan['batch_epoch'])
                    os.makedirs(batch_root)
                    plan['candidate_to_physical'] = {c['candidate_id']: [c['row_id']]
                        for c in plan['candidates']}
                    ledgers.append(os.path.join(batch_root, 'history_references.json'))
                    return {r['row_id']: {'candidate_id': r['candidate_ids'][0]} for r in plan['rows']}

                def _evaluate_candidate(self, index, generation, values, manifest=None):
                    cid = 'g%03d_c%04d' % (generation, index)
                    physical_calls.append(cid)
                    candidate_root = os.path.join(run_root, 'candidates', cid)
                    os.makedirs(candidate_root, exist_ok=True)
                    row = {'candidate_id': cid, 'generation': generation, 'index': index,
                        'values': values, 'status': 'completed', 'execution_backend': 'maestro_netlist',
                        'source_guard': {'ok': True},
                        'native_job_state': {'native_completion_proven': True},
                        'completion_evidence': {'ok': True, 'points': [{'point_id': 1}]},
                        'metrics': runner.evaluate_metrics(parsed(values['x']), spec['objectives'], [])}
                    if failed and index == 3:
                        row.update(status='failed', failure_class='design_or_simulation')
                        row['completion_evidence']['ok'] = False
                    atomic_write_json(os.path.join(candidate_root, 'candidate.json'), row)
                    self._progress({'candidate_id': cid, 'stage': 'candidate_completed'})
                    if stopped:
                        # The duplicate consumer remains outside the submitted worker window.
                        with open(os.path.join(run_root, 'STOP'), 'w') as stream:
                            stream.write('stop before all logical consumers commit')
                    return row

            original_compact = retention.compact_successful_candidate_artifacts
            def compact(candidate_root, run_root):
                ledger = load_json(ledgers[-1])
                owner.assertEqual(ledger['remaining'], 0)
                checkpoint = load_json(os.path.join(run_root, 'checkpoint.json'))
                owner.assertEqual(len(runner.checkpoint_candidates(checkpoint)), 4)
                compacted.append(os.path.basename(candidate_root))
                return original_compact(candidate_root, run_root)

            inputs = (0.1, 0.2, 0.1, 0.3) if failed else (0.1, 0.2, 0.3, 0.1)
            samples = [{'x': v} for v in inputs]
            original_ask = runner.PopulationGenerator.ask
            def ask(generator, count):
                original_ask(generator, count)
                return samples

            with mock.patch.object(runner, 'PrivateNetlistOptimizer', Adapter), \
                    mock.patch.object(runner.PopulationGenerator, 'ask', ask), \
                    mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                    mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile'), \
                    mock.patch.object(runner, 'compact_successful_candidate_artifacts', side_effect=compact):
                result = runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
            if stopped:
                self.assertEqual(result['state'], 'stopped')
                self.assertEqual(compacted, [])
                self.assertGreater(load_json(ledgers[0])['remaining'], 0)
                for row in result['candidates']:
                    self.assertEqual(row['artifact_compaction']['state'], 'waiting_shared_cleanup')
                    self.assertTrue(row['artifact_compaction']['ok'])
                return
            self.assertEqual(result['state'], 'completed')
            self.assertEqual(len(physical_calls), 3)
            self.assertEqual(len(compacted), 3 if failed else 4)
            for row in result['candidates']:
                if row['status'] != 'completed':
                    continue
                self.assertTrue(row['artifact_compaction']['artifact_cleanup_complete'])
                candidate_root = os.path.join(run_root, 'candidates', row['candidate_id'])
                candidate = load_json(os.path.join(candidate_root, 'candidate.json'))
                completion = load_json(os.path.join(candidate_root, 'completion_evidence.json'))
                summary = load_json(os.path.join(candidate_root, 'artifact_summary.json'))
                self.assertEqual(candidate['candidate_id'], row['candidate_id'])
                self.assertEqual(completion['candidate_id'], row['candidate_id'])
                self.assertTrue(summary['artifact_cleanup_complete'])
                self.assertEqual(summary['batch_reference']['physical_ids'], row['batch_reference']['physical_ids'])
                for item in summary['files']:
                    with open(os.path.join(candidate_root, item['name']), 'rb') as stream:
                        self.assertEqual(hashlib.sha256(stream.read()).hexdigest(), item['sha256'])
                if row['candidate_id'] == 'g000_c0003':
                    self.assertEqual(candidate['shared_with'], 'g000_c0000')
                    self.assertEqual(completion['shared_with'], 'g000_c0000')
                    self.assertEqual(summary['shared_with'], 'g000_c0000')
            checkpoint = load_json(os.path.join(run_root, 'checkpoint.json'))
            self.assertTrue(all(row['artifact_compaction']['artifact_cleanup_complete']
                for row in dataset_from_reference(checkpoint['candidate_store'])['candidates']
                if row['status'] == 'completed'))

    def test_all_logical_audit_files_survive_close_and_cleanup_waits_for_every_commit(self):
        self.run_case()

    def test_stop_retains_waiting_shared_cleanup_without_failure(self):
        self.run_case(stopped=True)

    def test_last_failed_consumer_releases_successful_candidate_compaction(self):
        self.run_case(failed=True)
