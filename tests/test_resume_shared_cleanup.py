"""Review probe: stop mid-batch, then resume, and inspect artifact_compaction."""
import os
import tempfile
from unittest import mock

import unittest

from crescendo_engine import runner, retention
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.single_test import project_setup
from crescendo_engine.utils import atomic_write_json, load_json
from tests.test_netlist_optimizer import fixtures, parsed
from tests.test_runner import _coordinated_runner_resume


class StopResumeCleanupTests(unittest.TestCase):
    def test_completed_shared_candidates_finish_cleanup_after_resume(self):
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            spec.update(execution_backend='maestro_netlist', native_candidate_batching='generation', generations=1)
            spec['simulator_model_profile_identity'] = {
                'schema_version': 'crescendo.simulator_model_profile_identity.v1',
                'fingerprint': 'review-probe-fixture', 'profiles': [], 'model_equivalence_claimed': False}
            prepared['source_identity'] = {'schema_version': 'crescendo.source_design_identity.v1',
                'fingerprint': 'fixture-source', 'libraries': []}
            prepared['setup']['tests'] = [{'name': 'main', 'enabled': True}]
            prepared['setup'] = project_setup(prepared['setup'], 'main')
            run_root = os.path.join(root, 'run')
            ledgers = []

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
                    batch_root = os.path.join(self.run_root, 'candidates', 'batch_' + plan['batch_epoch'])
                    os.makedirs(batch_root)
                    plan['candidate_to_physical'] = {c['candidate_id']: [c['row_id']] for c in plan['candidates']}
                    ledgers.append(os.path.join(batch_root, 'history_references.json'))
                    return {r['row_id']: {'candidate_id': r['candidate_ids'][0]} for r in plan['rows']}

                def _evaluate_candidate(self, index, generation, values, manifest=None):
                    cid = 'g%03d_c%04d' % (generation, index)
                    candidate_root = os.path.join(self.run_root, 'candidates', cid)
                    os.makedirs(candidate_root, exist_ok=True)
                    row = {'candidate_id': cid, 'generation': generation, 'index': index,
                        'values': values, 'status': 'completed', 'execution_backend': 'maestro_netlist',
                        'source_guard': {'ok': True},
                        'native_job_state': {'native_completion_proven': True},
                        'completion_evidence': {'ok': True, 'points': [{'point_id': 1}]},
                        'metrics': runner.evaluate_metrics(parsed(values['x']), spec['objectives'], [])}
                    atomic_write_json(os.path.join(candidate_root, 'candidate.json'), row)
                    self._progress({'candidate_id': cid, 'stage': 'candidate_completed'})
                    with open(os.path.join(run_root, 'STOP'), 'w') as stream:
                        stream.write('stop before all logical consumers commit')
                    return row

            original_compact = retention.compact_successful_candidate_artifacts
            compacted = []

            def compact(candidate_root, run_root_arg):
                compacted.append(os.path.basename(candidate_root))
                return original_compact(candidate_root, run_root_arg)

            samples = [{'x': v} for v in (0.1, 0.2, 0.3, 0.1)]
            original_ask = runner.PopulationGenerator.ask

            def ask(generator, count):
                original_ask(generator, count)
                return samples

            with mock.patch.object(runner, 'PrivateNetlistOptimizer', Adapter), \
                    mock.patch.object(runner.PopulationGenerator, 'ask', ask), \
                    mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                    mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile'), \
                    mock.patch.object(runner, 'compact_successful_candidate_artifacts', side_effect=compact):
                stopped = runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
            self.assertEqual(stopped['state'], 'stopped')
            checkpoint = load_json(os.path.join(run_root, 'checkpoint.json'))

            resumed_root = os.path.join(root, 'resumed')
            with mock.patch.object(runner, 'PrivateNetlistOptimizer', Adapter), \
                    mock.patch.object(runner.PopulationGenerator, 'ask', ask), \
                    mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                    mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile'), \
                    mock.patch.object(runner, 'compact_successful_candidate_artifacts', side_effect=compact):
                resumed = _coordinated_runner_resume(spec, config, prepared, resumed_root, checkpoint)
            bad = []
            for row in resumed['candidates']:
                ac = row.get('artifact_compaction') or {}
                if row['status'] == 'completed' and ac.get('artifact_cleanup_complete') is not True:
                    bad.append(row['candidate_id'])
            self.assertEqual(bad, [])
            self.assertEqual(resumed['state'], 'completed')
            self.assertEqual(len(resumed['candidates']), 4)
            self.assertTrue(load_json(ledgers[0])['retired_consumers'])
            self.assertTrue(all(load_json(path)['remaining'] == 0 for path in ledgers))
            for row in resumed['candidates']:
                if row['status'] == 'completed':
                    audit_root = os.path.join(resumed_root, 'candidates', row['candidate_id'])
                    for name in ('candidate.json', 'completion_evidence.json', 'timing.json', 'artifact_summary.json'):
                        self.assertTrue(os.path.isfile(os.path.join(audit_root, name)), name)

if __name__ == '__main__':
    unittest.main()
