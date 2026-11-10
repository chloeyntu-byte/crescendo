import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import netlist_optimizer as module
from crescendo_engine.utils import load_json
from tests.test_netlist_optimizer import parsed, model_task


class CandidateGateProgressTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.spec = {'execution_backend': 'maestro_netlist', 'population': 1,
            'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}], 'constraints': []}
        self.adapter = module.PrivateNetlistOptimizer(self.temp.name, self.spec,
            {'setup': {'tests': []}}, [], {'runtime': {'max_parallel_jobs': 1}}, lambda *a: {})
        self.manifest = {'root': self.temp.name, 'tasks': [dict(model_task(), corner='TT')],
            'netlist_generation': {'parameter_application': {'ok': True}},
            'manifest_sha256': 'fixture', 'source_identity': {'fingerprint': 'fixture'}}
        self.adapter._prepare_once = mock.Mock(return_value=None)
        self.adapter.maestro_producer = mock.Mock()
        self.adapter.maestro_producer.generate.return_value = self.manifest
        self.adapter.context = mock.Mock()
        self.adapter.context.unsafe.is_set.return_value = False
        self.adapter.context.evaluate.return_value = {'parsed': parsed(1.0),
            'completion': {}, 'native_job_state': {}, 'task_results': []}
        self.events = []
        original = self.adapter._progress
        def observe(event):
            self.events.append(copy.deepcopy(event)); original(event)
        self.adapter._progress = observe
        patcher = mock.patch.object(module, 'reconcile_preparation', return_value={'cleanup_proven': True})
        self.cleanup = patcher.start(); self.addCleanup(patcher.stop)
        patcher = mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile')
        self.profile = patcher.start(); self.addCleanup(patcher.stop)

    def gates(self):
        return [event['gate_progress']['passed'] for event in self.events if 'gate_progress' in event]

    def test_success_advances_only_after_each_return_and_durable_candidate(self):
        def execute(*args, **kwargs):
            self.assertEqual(self.gates(), [0, 1, 2])
            return {'parsed': parsed(1.0), 'completion': {}, 'native_job_state': {}, 'task_results': []}
        self.adapter.context.evaluate.side_effect = execute
        result = self.adapter.evaluate(0, 0, {})
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(self.gates(), [0, 1, 2, 3, 4, 5])
        self.assertEqual(result['execution_progress']['gate_progress']['passed'], 5)
        persisted = load_json(os.path.join(self.temp.name, 'candidates', 'g000_c0000', 'candidate.json'))
        self.assertEqual(persisted['status'], 'completed')
        live = load_json(os.path.join(self.adapter.root, 'progress.json'))
        self.assertEqual(live['gate_progress']['passed'], 5)
        self.assertEqual(live['latest']['gate_progress'], live['gate_progress'])

    def test_prepare_failure_does_not_pass_first_gate(self):
        self.adapter._prepare_once.side_effect = ValueError('prepare failed')
        self.assertEqual(self.adapter.evaluate(0, 0, {})['status'], 'infrastructure_failed')
        self.assertEqual(self.gates(), [0])

    def test_netlist_failure_stops_at_verified_context(self):
        self.adapter.maestro_producer.generate.side_effect = ValueError('corner failed')
        self.adapter.evaluate(0, 0, {})
        self.assertEqual(self.gates(), [0, 1])
        self.adapter.context.evaluate.assert_not_called()

    def test_model_failure_cannot_claim_fresh_netlist_gate(self):
        self.profile.side_effect = ValueError('wrong model')
        self.adapter.evaluate(0, 0, {})
        self.assertEqual(self.gates(), [0, 1])

    def test_solver_or_cancel_does_not_complete_gate(self):
        self.adapter.context.evaluate.side_effect = ValueError('stop')
        self.adapter.cancel_check = lambda: True
        self.assertEqual(self.adapter.evaluate(0, 0, {})['status'], 'cancelled')
        self.assertEqual(self.gates(), [0, 1, 2])

    def test_unproven_cleanup_does_not_pass_metric_gate(self):
        self.cleanup.return_value = {'cleanup_proven': False}
        self.adapter.evaluate(0, 0, {})
        self.assertEqual(self.gates(), [0, 1, 2, 3])

    def test_terminal_write_failure_does_not_publish_full_progress(self):
        original = module.atomic_write_json
        def write(path, value):
            if os.path.basename(path) == 'candidate.json' and value.get('status') == 'completed':
                raise OSError('fixture disk write failure')
            return original(path, value)
        with mock.patch.object(module, 'atomic_write_json', side_effect=write):
            with self.assertRaisesRegex(OSError, 'disk write'):
                self.adapter.evaluate(0, 0, {})
        self.assertEqual(self.gates(), [0, 1, 2, 3, 4])

    def test_gate_display_write_failure_cannot_overturn_durable_success(self):
        original = module.atomic_write_json
        def write(path, value):
            gate = value.get('gate_progress') or (value.get('latest') or {}).get('gate_progress') or {}
            if os.path.basename(path) in ('progress.json', 'cli_progress.json') and gate.get('passed') == 5:
                raise OSError('fixture progress display write failure')
            return original(path, value)
        with mock.patch.object(module, 'atomic_write_json', side_effect=write):
            result = self.adapter.evaluate(0, 0, {})
        self.assertEqual(result['status'], 'completed')
        persisted = load_json(os.path.join(self.temp.name, 'candidates', 'g000_c0000', 'candidate.json'))
        self.assertEqual(persisted['status'], 'completed')
        self.assertEqual(result['execution_progress']['gate_progress']['passed'], 5)
        self.assertEqual(self.gates(), [0, 1, 2, 3, 4, 5])

    def test_late_event_keeps_same_candidate_gate_and_other_candidate_is_separate(self):
        for candidate in ['g000_c0000', 'g000_c0001']:
            os.makedirs(os.path.join(self.temp.name, 'candidates', candidate))
        self.adapter._candidate_gate('g000_c0000', 3, 'Verified')
        self.adapter._candidate_gate('g000_c0000', 1, 'Late event')
        self.adapter._progress({'candidate_id': 'g000_c0000', 'stage': 'running'})
        first = self.adapter.candidate_progress['g000_c0000']['gate_progress']
        self.assertEqual((first['passed'], first['current']), (3, 'g000_c0000: Verified'))
        self.adapter._candidate_gate('g000_c0001', 0, 'New candidate')
        self.assertEqual(self.adapter.candidate_progress['g000_c0001']['gate_progress']['passed'], 0)
        self.assertNotEqual(first['scope'], self.adapter.progress['gate_progress']['scope'])
