"""Current whole-loop routing, durability and failure contracts.

The adapter is a control-flow fixture. Native CLI/receipt qualification is
covered separately; these tests do not claim an EDA or cluster simulation.
"""
import copy
import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.netlist_execution import execution_policy
from crescendo_engine.netlist_optimizer import PrivateNetlistRunError
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.optimized_schematic import build_export_evidence
from crescendo_engine.utils import atomic_write_json, atomic_write_text, load_json
from tests.test_runner import _coordinated_runner_resume
from tests.test_optimized_schematic import parameter
from tests.test_netlist_optimizer import parsed, model_task


def common_adapter(instances, behavior='normal', cleanup_proven=True):
    class FakeCommonAdapter(object):
        def __init__(self, run_root, spec, prepared, selected, config, failure_metrics,
                     cancel_check=None, **kwargs):
            self.run_root, self.spec, self.config = run_root, spec, config
            self.root = os.path.join(run_root, 'private_netlist')
            os.makedirs(self.root, exist_ok=True)
            self.policy = execution_policy(prepared, spec, config)
            self.parallel_candidates = 1
            self.cancel_check = cancel_check
            self.failure_metrics = failure_metrics
            self.fatal_error = None
            self.preparation_failure = None
            self.point_commit_callback = None
            self.calls, self.closed, self.cancelled, self.source_checks = [], [], 0, 0
            self.failure_seen = False
            instances.append(self)

        def evaluate(self, index, generation, values):
            from crescendo_engine.simulator_profiles import require_frozen_model_profile
            require_frozen_model_profile(self.spec, {"tasks": [model_task()]})
            self.calls.append((generation, index, copy.deepcopy(values)))
            if behavior == 'crash_generation' and generation == 1:
                raise RuntimeError('fixture power loss')
            candidate_id = 'g%03d_c%04d' % (generation, index)
            root = os.path.join(self.run_root, 'candidates', candidate_id)
            os.makedirs(root, exist_ok=True)
            atomic_write_text(os.path.join(root, 'raw.fixture'), 'owned fixture, no native EDA claim\n')
            x = values['x']
            metrics = {'objectives': [{'output_key': 'test::gain', 'score': -x, 'value': x}],
                       'constraints': [], 'feasible': True, 'total_violation': 0.0, 'merit': -x}
            row = {'candidate_id': candidate_id, 'generation': generation, 'index': index,
                   'values': copy.deepcopy(values), 'status': 'completed', 'metrics': metrics,
                   'execution_backend': self.spec['execution_backend'],
                   'source_guard': {'ok': True, 'source': 'fixture'},
                   'native_job_state': {'native_completion_proven': True, 'simulation_submitted': True},
                   'completion_evidence': {'schema_version': 'crescendo.private_netlist_completion.v1',
                       'ok': True, 'adapter': 'private_netlist', 'candidate_id': candidate_id,
                       'task_count': 1, 'tests': ['test'], 'corners': ['TT'],
                       'points': [{'test': 'test', 'corner': 'TT', 'point_id': 1}],
                       'parameters': copy.deepcopy(values), 'fixture': True}}
            if behavior == 'design_fail_first_generation' and generation == 0:
                row.update(status='failed', error='fixture solver convergence failure',
                           metrics=self.failure_metrics(self.spec, RuntimeError('fixture convergence')))
            if behavior in ('fatal', 'source_drift'):
                self.fatal_error = 'fixture source identity changed' if behavior == 'source_drift' else 'fixture shared preparation failed'
                row.update(status='infrastructure_failed', error=self.fatal_error,
                           failure_stage='source_integrity' if behavior == 'source_drift' else 'netlist_preparation')
                row.pop('metrics', None)
            if behavior == 'stop_after_first' and len(self.calls) == 1:
                atomic_write_text(os.path.join(self.run_root, 'STOP'), 'fixture user stop\n')
            if behavior == 'point_commit':
                self.point_commit_callback({'candidate_id': candidate_id, 'task_id': 'one',
                    'candidate_completed': False, 'outputs': [{'key': 'test::gain', 'value': x}],
                    'process_termination_proven': True, 'remote_stop_proven': True}, lambda: False)
            atomic_write_json(os.path.join(root, 'candidate.json'), row)
            atomic_write_json(os.path.join(root, 'timing.json'), {'fixture': True})
            atomic_write_json(os.path.join(root, 'completion_evidence.json'), row['completion_evidence'])
            atomic_write_json(os.path.join(root, 'execution_diagnostics.json'), {'fixture': True})
            return row

        def verify_sources(self):
            self.source_checks += 1
            if behavior == 'boundary_source_drift':
                raise PrivateNetlistRunError('fixture generation source identity changed')
            return {'ok': True}

        def cancel(self):
            self.cancelled += 1

        def close(self, cancel=False):
            self.closed.append(cancel)
            status = load_json(os.path.join(self.run_root, 'status.json'))
            self.close_observation = {'state': status['state'], 'result_exists': os.path.isfile(os.path.join(self.run_root, 'result.json'))}
            return {'cleanup_proven': cleanup_proven, 'retained': [] if cleanup_proven else [self.root]}

    return FakeCommonAdapter


class UnifiedOptimizerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo-unified-whole-loop-')
        self.root = self.temp.name
        self.prepared = {'setup': {'backend': 'maestro', 'tests': [{'name': 'test', 'enabled': True}], 'selected_test': 'test', '_source_tests': [{'name': 'test', 'enabled': True}], 'corners': []},
                         'schema_version': 'crescendo.prepared.v1',
                         'hierarchy': {'designs': []},
                         'parameters': [{'parameter_id': 'x', 'backend': 'schematic', 'optimizable': True}],
                         'outputs': [{'key': 'test::gain'}],
                         'source_identity': {'schema_version': 'crescendo.source_design_identity.v1',
                                             'libraries': [], 'fingerprint': 'fixture-source-a'},
                         'eda_context': {'cadence': {'version_checks': []},
                             'pdk': {'ok': True, 'pdk_name': 'FIXTURE', 'pdk_adapter': 'generic-cdf'},
                             'submission': {'controller': 'maestro', 'simulator': 'spectre'}}}
        self.config = {'runtime': {'run_root': self.root, 'max_parallel_jobs': 1,
                                  'candidate_timeout_seconds': 60}}
        self.spec = {'algorithm': 'random', 'generations': 2, 'population': 4, 'jobs': 1, 'seed': 7,
                     'variables': [{'parameter_id': 'x', 'minimum': 0, 'maximum': 1}],
                     'objectives': [{'output_key': 'test::gain', 'direction': 'maximize'}], 'constraints': []}

    def tearDown(self):
        self.temp.cleanup()

    def run_fixture(self, spec=None, behavior='normal', cleanup_proven=True, name='run'):
        instances = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(instances, behavior, cleanup_proven)), \
                mock.patch.object(runner, 'PersistentControllerProxy') as native, \
                mock.patch.object(runner, '_evaluate_candidate') as legacy:
            result = runner.run_optimization(spec or self.spec, self.config, prepared=self.prepared,
                                             run_root=os.path.join(self.root, name))
            native.assert_not_called()
            legacy.assert_not_called()
        return result, instances

    def test_fresh_aliases_and_both_frontends_use_one_common_adapter(self):
        for backend in ('auto', 'legacy', 'persistent_maestro', 'adexl', 'maestro_netlist', 'private_netlist'):
            spec = dict(self.spec, execution_backend=backend, generations=1)
            result, instances = self.run_fixture(spec, name=backend)
            expected = 'private_netlist' if backend == 'private_netlist' else 'maestro_netlist'
            self.assertEqual(instances[0].spec['execution_backend'], expected)
            self.assertEqual(result['state'], 'completed')
            self.assertEqual(len(result['candidates']), 4)
            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].closed, [False])

    def test_every_generation_verifies_source_identity(self):
        result, instances = self.run_fixture()
        self.assertEqual(result['state'], 'completed')
        self.assertGreaterEqual(instances[0].source_checks, 2)

    def test_candidate_checkpoint_is_durable_before_owned_raw_cleanup(self):
        original = runner.safe_delete_candidate_workspace
        cleaned = []

        def checked(candidate_root, run_root, *args, **kwargs):
            identifier = os.path.basename(candidate_root)
            checkpoint = load_json(os.path.join(run_root, 'checkpoint.json'))
            self.assertIn(identifier, [row['candidate_id'] for row in runner.checkpoint_candidates(checkpoint)])
            self.assertTrue(os.path.isfile(os.path.join(candidate_root, 'completion_evidence.json')))
            cleaned.append(identifier)
            return original(candidate_root, run_root, *args, **kwargs)

        with mock.patch.object(runner, 'safe_delete_candidate_workspace', side_effect=checked):
            result, instances = self.run_fixture(dict(self.spec, generations=1))
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(len(cleaned), 4)
        self.assertEqual(instances[0].close_observation, {'state': 'finalizing', 'result_exists': False})

    def test_stop_preserves_plan_and_resume_uses_same_values_without_repeating_completed_candidate(self):
        stopped, instances = self.run_fixture(behavior='stop_after_first')
        self.assertEqual(stopped['stopped_reason'], 'user_stop')
        self.assertEqual(len(instances[0].calls), 1)
        checkpoint = load_json(os.path.join(self.root, 'run', 'checkpoint.json'))
        saved = runner.checkpoint_candidates(checkpoint)
        self.assertEqual(len(saved), 1)
        planned = checkpoint['state']['active_plan']['planned']
        # Packed plan format is validated by coordinated resume below.
        resumed_instances = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(resumed_instances)):
            result = _coordinated_runner_resume(checkpoint['spec'], self.config, self.prepared,
                                               os.path.join(self.root, 'resumed'), checkpoint)
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(len(result['candidates']), 8)
        self.assertEqual(len(resumed_instances[0].calls), 7)
        self.assertNotIn((saved[0]['generation'], saved[0]['index']), [(generation, index) for generation, index, values in resumed_instances[0].calls])
        self.assertEqual(result['candidates'][0]['values'], saved[0]['values'])

    def test_infrastructure_failure_is_checkpointed_once_without_penalty_or_resampling(self):
        instances = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(instances, 'fatal')):
            with self.assertRaisesRegex(PrivateNetlistRunError, 'shared preparation failed'):
                runner.run_optimization(self.spec, self.config, prepared=self.prepared,
                                         run_root=os.path.join(self.root, 'fatal'))
        rows = runner.checkpoint_candidates(load_json(os.path.join(self.root, 'fatal', 'checkpoint.json')))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['status'], 'infrastructure_failed')
        self.assertNotIn('metrics', rows[0])
        self.assertEqual(len(instances[0].calls), 1)
        self.assertGreater(instances[0].cancelled, 0)
        self.assertEqual(instances[0].closed, [True])

    def test_unproven_final_cleanup_cannot_report_completed(self):
        instances = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(instances, cleanup_proven=False)):
            with self.assertRaisesRegex(RuntimeError, 'shutdown/cleanup failed'):
                runner.run_optimization(dict(self.spec, generations=1), self.config, prepared=self.prepared,
                                         run_root=os.path.join(self.root, 'unsafe'))
        self.assertEqual(load_json(os.path.join(self.root, 'unsafe', 'status.json'))['state'], 'failed')
        self.assertEqual(load_json(os.path.join(self.root, 'unsafe', 'result.json'))['state'], 'failed')
        self.assertFalse(load_json(os.path.join(self.root, 'unsafe', 'worker_shutdown.json'))['workers'][0]['ok'])

    def test_source_drift_stops_before_new_candidates_without_optimizer_penalty(self):
        instances = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(instances, 'source_drift')):
            with self.assertRaisesRegex(PrivateNetlistRunError, 'source identity changed'):
                runner.run_optimization(self.spec, self.config, prepared=self.prepared,
                                         run_root=os.path.join(self.root, 'source-drift'))
        rows = runner.checkpoint_candidates(load_json(os.path.join(self.root, 'source-drift', 'checkpoint.json')))
        self.assertEqual(len(instances[0].calls), 1)
        self.assertEqual(rows[0]['failure_stage'], 'source_integrity')
        self.assertNotIn('metrics', rows[0])

    def test_power_loss_resume_preserves_rng_and_skips_first_generation(self):
        baseline, unused = self.run_fixture(name='baseline')
        crashed = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(crashed, 'crash_generation')):
            with self.assertRaisesRegex(RuntimeError, 'fixture power loss'):
                runner.run_optimization(self.spec, self.config, prepared=self.prepared,
                                         run_root=os.path.join(self.root, 'crashed'))
        checkpoint = load_json(os.path.join(self.root, 'crashed', 'checkpoint.json'))
        self.assertEqual(len(runner.checkpoint_candidates(checkpoint)), 4)

        instances = []
        with mock.patch.object(runner, 'PrivateNetlistOptimizer', common_adapter(instances)):
            resumed = _coordinated_runner_resume(checkpoint['spec'], self.config, self.prepared,
                                                 os.path.join(self.root, 'resumed'), checkpoint)
        self.assertEqual(len(instances[0].calls), 4)
        self.assertTrue(all(generation == 1 for generation, index, values in instances[0].calls))
        self.assertEqual([row['values'] for row in resumed['candidates']], [row['values'] for row in baseline['candidates']])

    def test_fully_failed_design_generation_still_advances_algorithm(self):
        result, instances = self.run_fixture(dict(self.spec, algorithm='nsga2'), behavior='design_fail_first_generation')
        self.assertEqual(result['state'], 'completed')
        self.assertEqual(sum(row['status'] == 'failed' for row in result['candidates']), 4)
        self.assertEqual(sum(row['status'] == 'completed' for row in result['candidates']), 4)
        self.assertEqual(len(instances[0].calls), 8)

    def test_old_native_checkpoint_is_explicitly_rejected_before_any_adapter(self):
        result, instances = self.run_fixture(behavior='stop_after_first')
        checkpoint = load_json(os.path.join(self.root, 'run', 'checkpoint.json'))
        spec = copy.deepcopy(checkpoint['spec'])
        spec['execution_backend'] = 'persistent_maestro'
        checkpoint['spec'] = spec
        checkpoint['payload_sha256'] = runner._checkpoint_checksum(checkpoint)
        with mock.patch.object(runner, 'PrivateNetlistOptimizer') as adapter:
            with self.assertRaisesRegex(ValueError, 'Historical native-ADE checkpoint'):
                runner._run_optimization_unleased(spec, self.config, prepared=self.prepared,
                    run_root=os.path.join(self.root, 'old-resume'), resume_checkpoint=checkpoint)
            adapter.assert_not_called()

    def test_current_scalar_points_can_commit_while_candidate_stays_nonterminal(self):
        result, instances = self.run_fixture(dict(self.spec, generations=1), behavior='point_commit')
        self.assertEqual(result['state'], 'completed')
        checkpoint = load_json(os.path.join(self.root, 'run', 'checkpoint.json'))
        self.assertGreaterEqual(checkpoint['state']['checkpoint_revision'], 8)
        self.assertEqual(len(runner.checkpoint_candidates(checkpoint)), 4)



class UnifiedAdapterEvidenceTests(unittest.TestCase):
    def test_numeric_candidates_generate_independently_share_context_and_retain_applied_values(self):
        with tempfile.TemporaryDirectory(prefix='crescendo-numeric-adapter-') as root:
            parameters = [parameter()]
            prepared = {'parameters': parameters, 'setup': {'tests': []}, 'source_identity': {'fingerprint': 'fixture'}}
            spec = {'execution_backend': 'maestro_netlist', 'population': 4,
                    'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}], 'constraints': []}
            config = {'runtime': {'max_parallel_jobs': 2}}
            context = mock.Mock()
            context.unsafe.is_set.return_value = False
            context.close.return_value = {'cleanup_proven': True, 'retained': []}
            context.evaluate.return_value = {'parsed': parsed(1.0),
                'completion': {'adapter': 'private_netlist'},
                'native_job_state': {'native_completion_proven': True}, 'task_results': []}
            from crescendo_engine.native_maestro_parameters import GENERATION_PROTOCOL
            from crescendo_engine.optimized_schematic import OptimizedSchematicError
            application = {'proof_kind': 'native_run_device_parameters', 'ok': True,
                           'parameters': [{'parameter_id': 'p1', 'effective': 3.5e-6}]}

            def numeric(prepared, selected, values, config, capture_root, candidate_id, **kwargs):
                return {'root': capture_root, 'workspace': {}, 'source_identity': {'fingerprint': 'fixture'},
                        'manifest_sha256': 'fixture', 'source_guard': {'ok': True},
                        'values': values, 'applied_values': {'p1': 3.5e-6},
                        'netlist_generation': {'source_mode': 'maestro_netlist', 'parameter_application': application,
                                               'generation_protocol': GENERATION_PROTOCOL},
                        'tasks': [dict(model_task(), corner='TT')]}

            adapter = PrivateNetlistOptimizer(root, spec, prepared, parameters, config, runner._failure_metrics)
            with mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext', return_value=context) as executor, \
                    mock.patch('crescendo_engine.shared_maestro.SharedMaestroProducer.generate',
                        side_effect=lambda values, root, candidate_id, **kwargs:
                        numeric(prepared, parameters, values, config, root, candidate_id)) as provider, \
                    mock.patch('crescendo_engine.netlist_optimizer.prepare_private_netlist') as template, \
                    mock.patch('crescendo_engine.netlist_optimizer.materialize_candidate') as edit, \
                    mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation', return_value={'cleanup_proven': True}), \
                    mock.patch('crescendo_engine.netlist_optimizer.verify_sources_unchanged', return_value={'ok': True}):
                rows = [adapter.evaluate(index, 0, {'p1': 4e-6}) for index in range(2)]
                self.assertTrue(all(row['status'] == 'completed' for row in rows))
                self.assertEqual(provider.call_count, 2)
                self.assertEqual(executor.call_count, 1)
                self.assertEqual(context.evaluate.call_count, 2)
                template.assert_not_called()
                edit.assert_not_called()
                for row in rows:
                    self.assertEqual(row['values']['p1'], 4e-6)
                    self.assertEqual(row['completion_evidence']['parameters']['p1'], 3.5e-6)
                    self.assertNotEqual(row['completion_evidence']['parameters']['p1'], row['values']['p1'])
                    with self.assertRaisesRegex(OptimizedSchematicError, 'no verified OA/CDF backannotation snapshot'):
                        build_export_evidence(row, parameters)
                self.assertTrue(adapter.close()['cleanup_proven'])
