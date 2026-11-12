"""Generation capture barrier, frozen dependencies and exact-once CLI fanout."""
import copy
import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from unittest import mock

from crescendo_engine import generation_batch as batch
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.netlist_providers import prepare_maestro_netlists_batch
from crescendo_engine.private_netlist import validate_manifest, PrivateNetlistError
from crescendo_engine.shared_maestro import SharedMaestroProducer
from tests import test_netlist_providers as provider_fixtures
from tests.test_netlist_optimizer import fixtures, parsed


class PrivateGenerationTests(unittest.TestCase):
    def plan(self):
        return batch.build_generation_plan(0, [(i, {'p': v}) for i, v in enumerate((1, 2, 3, 1))],
            [{'parameter_id': 'p'}], {})

    def prepare(self, root, partial=False):
        fixture = provider_fixtures.NumericMaestroProviderTest()
        prepared, params, config, spec, model = fixture.fixtures(root)
        prepared['setup']['tests'][0]['analyses'] = [{'name': 'dc'}]
        prepared['hierarchy'] = {'designs': [{'hierarchy': {'instances': [
            {'kind': 'device', 'master_library': 'analogLib', 'master_cell': 'res'}]}}]}
        prepared['setup_private_session'] = {'session_id': 'fixture'}
        workspace = fixture.workspace(prepared['setup'], {}, config, os.path.join(root, 'workspace'))
        plan = self.plan()
        capture = fixture.capture(model)
        @contextmanager
        def borrow(*args, **kwargs):
            yield workspace
        def capture_batch(*args, **kwargs):
            self.assertIs(kwargs['batch_plan'], plan)
            phase = None
            partitions = {}
            physical = {}
            for index, row in enumerate(plan['rows']):
                phase = capture(workspace, params, row['values'], {}, {}, config,
                    os.path.join(args[6], row['row_id']), symbolic=False, verify_numeric_application=True)
                phase['tasks'][0]['task_id'] = 'p%d_tb' % index
                partitions[row['row_id']] = {key: phase[key] for key in ('tasks', 'parameter_application', 'applied_values')}
                physical[row['row_id']] = ['physical-%d' % index]
            plan['candidate_to_physical'] = {c['candidate_id']: physical[c['row_id']] for c in plan['candidates']}
            if partial:
                partitions.pop(plan['rows'][-1]['row_id'])
            return dict(phase, partitions=partitions)
        with ExitStack() as stack:
            stack.enter_context(mock.patch('crescendo_engine.setup_private_session.borrow_setup_private_session', side_effect=borrow))
            stack.enter_context(mock.patch('crescendo_engine.setup_private_session.write_session_reference'))
            stack.enter_context(mock.patch('crescendo_engine.netlist_providers._capture_phase', side_effect=capture_batch))
            stack.enter_context(mock.patch('crescendo_engine.private_netlist.reconcile_preparation', return_value={'cleanup_proven': True}))
            stack.enter_context(mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}))
            manifests = prepare_maestro_netlists_batch(prepared, params, config,
                os.path.join(root, 'batch_' + plan['batch_epoch']), plan, spec)
        return plan, manifests, model

    def test_all_rows_frozen_before_publish_with_duplicate_mapping(self):
        with tempfile.TemporaryDirectory() as root:
            plan, manifests, unused = self.prepare(root)
            self.assertEqual(len(manifests), 3)
            self.assertTrue(plan['inputs_qualified'])
            self.assertEqual(plan['candidate_to_physical']['g000_c0000'], plan['candidate_to_physical']['g000_c0003'])
            self.assertEqual(len({m['tasks'][0]['cwd'] for m in manifests.values()}), 3)
            for manifest in manifests.values():
                self.assertTrue(validate_manifest(manifest))
                self.assertEqual(manifest['netlist_generation']['solver_invocations'], 0)
                self.assertEqual(manifest['netlist_generation']['capture_only_wrapper_exit'], 1)

    def test_partial_capture_cannot_publish_inputs(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(KeyError):
                self.prepare(root, partial=True)

    def test_changed_include_rejects_each_frozen_closure(self):
        with tempfile.TemporaryDirectory() as root:
            plan, manifests, model = self.prepare(root)
            with open(model, 'a') as stream:
                stream.write('// changed external model\n')
            for manifest in manifests.values():
                with self.assertRaises((ValueError, PrivateNetlistError)):
                    validate_manifest(manifest)

    def adapter(self, root, template=False):
        prepared, config, spec = fixtures(root)
        if not template:
            spec['execution_backend'] = 'maestro_netlist'
        adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared['parameters'], config, lambda *a: {})
        adapter._prepare_once = mock.Mock()
        adapter.context = mock.Mock()
        adapter._apply_native_fallback = mock.Mock()
        adapter.maestro_producer = mock.Mock()
        return adapter

    def test_template_success_never_calls_native_generation(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root, template=True)
            self.assertEqual(adapter.prepare_generation(0, [(0, {'x': 1})]), {})
            adapter.maestro_producer.generate_batch.assert_not_called()
            adapter.maestro_producer.generate.assert_not_called()

    def test_numeric_fallback_uses_generation_capture_before_cli(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root, template=True)
            adapter.native_fallback = {'failure_stage': 'native_numeric_derived_parameters'}
            def capture(plan):
                os.makedirs(os.path.join(root, 'candidates', 'batch_' + plan['batch_epoch']))
                plan['candidate_to_physical'] = {c['candidate_id']: [c['row_id']] for c in plan['candidates']}
                return {r['row_id']: {'candidate_id': r['candidate_ids'][0]} for r in plan['rows']}
            adapter.maestro_producer.generate_batch.side_effect = capture
            with mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                    mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile'):
                result = adapter.prepare_generation(0, [(0, {'x': 1}), (1, {'x': 2}), (2, {'x': 1})])
            self.assertEqual(len(result), 3)
            self.assertIs(result['g000_c0000']['future'], result['g000_c0002']['future'])
            adapter.maestro_producer.generate_batch.assert_called_once()
            adapter.maestro_producer.generate.assert_not_called()
            adapter.context.evaluate.assert_not_called()

    def test_exact_once_cli_and_each_logical_candidate_has_own_record(self):
        from concurrent.futures import Future
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root)
            manifest = {'candidate_id': 'g000_c0000'}
            item = {'manifest': manifest, 'future': Future()}
            calls = []
            def evaluate(index, generation, values, manifest):
                calls.append(index)
                return {'status': 'completed', 'candidate_id': 'g000_c0000',
                    'metrics': parsed(3), 'completion_evidence': {'ok': True}}
            adapter._evaluate_candidate = evaluate
            with ThreadPoolExecutor(max_workers=4) as pool:
                rows = list(pool.map(lambda i: adapter.evaluate(i, 0, {'x': 1}, item), range(4)))
            self.assertEqual(len(calls), 1)
            self.assertEqual(len({row['candidate_id'] for row in rows}), 4)
            for row in rows:
                path = os.path.join(root, 'candidates', row['candidate_id'], 'candidate.json')
                with open(path) as stream:
                    self.assertEqual(json.load(stream)['candidate_id'], row['candidate_id'])

    def test_failed_submission_intent_never_strands_a_duplicate_future(self):
        from concurrent.futures import Future
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root)
            adapter._evaluate_candidate = mock.Mock()
            future = Future()
            plan = {'submitted': False}
            item = {'manifest': {}, 'future': future, 'plan': plan,
                    'batch_reference': {'manifest': os.path.join(root, 'batch_manifest.json')}}
            with mock.patch('crescendo_engine.netlist_optimizer.atomic_write_json', side_effect=OSError('disk full')):
                for index in (0, 1):
                    with self.assertRaises(OSError):
                        adapter.evaluate(index, 0, {'x': 1}, item)
                    self.assertFalse(future.running())
                    self.assertFalse(plan['submitted'])
            adapter._evaluate_candidate.assert_not_called()

    def test_capability_decline_freezes_all_serial_inputs_before_return(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root)
            adapter.maestro_producer.generate_batch.side_effect = batch.BatchUnsupported('missing API')
            adapter.maestro_producer.generate.side_effect = lambda values, target, cid: {'candidate_id': cid}
            with mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                    mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile'):
                result = adapter.prepare_generation(0, [(0, {'x': 1}), (1, {'x': 2})])
            self.assertEqual(len(result), 2)
            self.assertEqual(adapter.maestro_producer.generate.call_count, 2)
            self.assertTrue(any(name.startswith('batch_fallback_') for name in os.listdir(root)))

    def test_submitted_batch_cannot_switch_to_serial(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root)
            def capture(plan):
                plan['submitted'] = True
                raise batch.BatchUnsupported('late failure')
            adapter.maestro_producer.generate_batch.side_effect = capture
            with self.assertRaises(batch.BatchUnsupported):
                adapter.prepare_generation(0, [(0, {'x': 1})])
            adapter.maestro_producer.generate.assert_not_called()

    def test_cancelled_batch_cannot_fallback_or_start_solver(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = self.adapter(root)
            adapter.cancel_check = lambda: True
            adapter.maestro_producer.generate_batch.side_effect = batch.BatchUnsupported('stop')
            with self.assertRaises(batch.BatchUnsupported):
                adapter.prepare_generation(0, [(0, {'x': 1})])
            adapter.maestro_producer.generate.assert_not_called()
            adapter.context.evaluate.assert_not_called()

    def test_batch_enable_and_corner_readback_is_explicit(self):
        plan = self.plan()
        batch.build_native_columns(plan, [{'parameter_id': 'p', 'path': 'L/C/schematic/R/r'}])
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'columns.tsv')
            lines = ['L/C/schematic/R/r\t1 2 3', 'OWNED\t' + plan['owned_fingerprint'],
                'AUDIT\tgroup_enabled\tt', 'AUDIT\tall_parameters_disabled\tnil',
                'AUDIT\tall_sets_disabled\tnil', 'AUDIT\tcorners\t("cold" "hot")',
                'AUDIT\tcolumn_enabled\tL/C/schematic/R/r\tt']
            with open(path, 'w') as stream:
                stream.write('\n'.join(lines))
            self.assertTrue(batch.read_native_batch_columns(path, plan)['ok'])
            self.assertEqual(plan['native_readback']['corners'], '("cold" "hot")')
            with open(path, 'w') as stream:
                stream.write('\n'.join(lines[:-1]))
            with self.assertRaises(ValueError):
                batch.read_native_batch_columns(path, plan)


class RunnerGenerationBarrierTests(unittest.TestCase):
    def run_case(self, stop=False, legacy_backend=None):
        from crescendo_engine import runner
        from crescendo_engine.single_test import project_setup
        from crescendo_engine.utils import load_json
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            spec['native_candidate_batching'] = 'generation'
            if legacy_backend:
                spec['execution_backend'] = legacy_backend
            prepared['source_identity'] = {'schema_version': 'crescendo.source_design_identity.v1',
                'fingerprint': 'fixture-source', 'libraries': []}
            prepared['setup']['tests'] = [{'name': 'main', 'enabled': True}]
            prepared['setup'] = project_setup(prepared['setup'], 'main')
            calls = []
            run_root = os.path.join(root, 'run')
            class Adapter(object):
                def __init__(self, run_root, spec, *args, **kwargs):
                    self.root = os.path.join(run_root, 'private_netlist')
                    os.makedirs(self.root)
                    self.context = None
                    self.parallel_candidates = 2
                    self.policy = {'max_jobs': 2}
                    self.fatal_error = None
                def prepare_generation(self, generation, pending):
                    calls.append(('prepare', generation, len(pending)))
                    if stop:
                        with open(os.path.join(run_root, 'STOP'), 'w') as stream:
                            stream.write('stop during capture')
                        raise RuntimeError('capture stopped')
                    return {'g%03d_c%04d' % (generation, index): {'qualified': True}
                            for index, values in pending}
                def evaluate(self, index, generation, values, manifest=None):
                    if manifest is not None and not manifest['qualified']:
                        raise AssertionError('unqualified input')
                    calls.append(('evaluate', generation, index))
                    return {'candidate_id': 'g%03d_c%04d' % (generation, index),
                        'generation': generation, 'index': index, 'values': values,
                        'status': 'completed', 'execution_backend': 'private_netlist',
                        'metrics': runner.evaluate_metrics(parsed(values['x']), spec['objectives'], [])}
                def verify_sources(self):
                    return {'ok': True}
                def cancel(self):
                    calls.append(('cancel',))
                def close(self, **kwargs):
                    return {'cleanup_proven': True}
            original_document = runner._checkpoint_document
            def checkpoint_document(*args, **kwargs):
                document = original_document(*args, **kwargs)
                if legacy_backend:
                    for section in ('spec', 'manifest'):
                        document[section].pop('native_batching_protocol', None)
                    document['payload_sha256'] = runner._checkpoint_checksum(document)
                return document
            with mock.patch('crescendo_engine.runner.PrivateNetlistOptimizer', Adapter), \
                    mock.patch.object(runner, '_checkpoint_document', side_effect=checkpoint_document):
                result = runner.run_optimization(spec, config, prepared=prepared, run_root=run_root)
            checkpoint = load_json(os.path.join(run_root, 'checkpoint.json'))
            runner.validate_checkpoint_integrity(checkpoint)
            if stop:
                self.assertEqual(result['state'], 'stopped')
                self.assertEqual(result['stopped_reason'], 'user_stop')
                self.assertFalse(any(row[0] == 'evaluate' for row in calls))
                self.assertEqual(len(checkpoint['state']['active_plan']['planned']), 4)
                stop = False
                with mock.patch('crescendo_engine.runner.PrivateNetlistOptimizer', Adapter):
                    from tests.test_runner import _coordinated_runner_resume
                    resumed = _coordinated_runner_resume(spec, config, prepared,
                        os.path.join(root, 'resumed'), checkpoint)
                self.assertEqual(resumed['state'], 'completed')
                self.assertEqual(len(resumed['candidates']), 8)
                expected = [('prepare', 0, 4)] if legacy_backend else [
                    ('prepare', 0, 4), ('prepare', 0, 4), ('prepare', 1, 4)]
                self.assertEqual([row for row in calls if row[0] == 'prepare'], expected)
                if legacy_backend:
                    resumed_checkpoint = load_json(os.path.join(root, 'resumed', 'checkpoint.json'))
                    self.assertNotIn('native_batching_protocol', resumed_checkpoint['spec'])
                    self.assertEqual(resumed_checkpoint['spec']['native_candidate_batching'], 'generation')
            else:
                self.assertEqual(result['state'], 'completed')
                self.assertEqual([row for row in calls if row[0] == 'prepare'], [('prepare', 0, 4), ('prepare', 1, 4)])
                for generation in (0, 1):
                    self.assertLess(calls.index(('prepare', generation, 4)),
                        min(i for i, row in enumerate(calls) if row[0] == 'evaluate' and row[1] == generation))
                self.assertEqual(len(result['candidates']), 8)
                self.assertEqual(checkpoint['spec']['native_batch_schema'], batch.SCHEMA)
                protocol = batch.NETLIST_BATCH_PROTOCOLS[checkpoint['spec']['execution_backend']]
                self.assertEqual(checkpoint['spec']['native_batching_protocol'], protocol)
                corrupt = copy.deepcopy(checkpoint)
                corrupt['manifest']['native_batching_protocol'] = protocol + '.unknown'
                corrupt['payload_sha256'] = runner._checkpoint_checksum(corrupt)
                with self.assertRaisesRegex(ValueError, 'native_batching_protocol identities disagree'):
                    runner.validate_checkpoint_integrity(corrupt)

    def test_formal_entry_prepares_once_per_generation_before_all_evaluations(self):
        self.run_case()

    def test_stop_during_generation_preparation_keeps_full_resume_plan(self):
        self.run_case(stop=True)

    def test_pre_capture_protocol_checkpoints_resume_per_candidate(self):
        for backend in ('maestro_netlist', 'private_netlist'):
            with self.subTest(backend=backend):
                self.run_case(stop=True, legacy_backend=backend)
