"""Preparation ownership, exclusive boundaries and unchanged native proofs."""
import copy
import os
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine import preparation_timing as timing
from crescendo_engine.diagnostic_timing import (DiagnosticTiming, generation_timing_summary,
                                               generation_report_timing)
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine.netlist_providers import prepare_maestro_netlists
from crescendo_engine.private_netlist import prepare_private_netlist, validate_manifest
from tests import test_netlist_providers as provider_fixture


class Clock(object):
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, value):
        self.value += value


def frontend_observations(delay=0):
    """Real mapping/freeze/manifest gates, synthetic native process only."""
    fixture = provider_fixture.NumericMaestroProviderTest()
    observations = {}
    with tempfile.TemporaryDirectory() as root:
        prepared, parameters, config, spec, model = fixture.fixtures(root)
        prepared['setup']['tests'][0].update(library='PRIVATE', cell='TB', view='schematic')
        parameters[0].update(numeric_value=2e-6, name='r', owner_library='PRIVATE',
            owner_cell='TB', owner_view='schematic', hierarchy_path='/R0',
            owner_occurrence_path='/', design_library='PRIVATE', design_cell='TB', design_view='schematic')
        base_capture = fixture.native_capture(model)
        def capture(workspace, selected, values, symbols, seeds, active_config, phase_root, **kwargs):
            timing.capture_observed()
            started = time.time()
            os.makedirs(phase_root)
            opened = os.path.join(phase_root, 'opened')
            closing = os.path.join(phase_root, 'closing')
            if delay:
                time.sleep(delay)
            with open(opened, 'w') as stream:
                stream.write('observed')
            now = time.time()
            os.utime(opened, (now, now))
            if delay:
                time.sleep(delay * 3)
            result = base_capture(workspace, selected, values, {}, {}, active_config,
                                  phase_root, symbolic=False, verify_numeric_application=True)
            with open(closing, 'w') as stream:
                stream.write('observed')
            now = time.time()
            os.utime(closing, (now, now))
            if delay:
                time.sleep(delay)
            timing.observe_capture_command(started, time.time(), {'lock_wait_seconds': 0}, opened, closing)
            return result
        with mock.patch('crescendo_engine.netlist_providers.build_workspace', side_effect=fixture.workspace), \
             mock.patch('crescendo_engine.private_netlist.build_workspace', side_effect=fixture.workspace), \
             mock.patch('crescendo_engine.netlist_providers._capture_phase', side_effect=capture), \
             mock.patch('crescendo_engine.private_netlist._capture_phase', side_effect=capture), \
             mock.patch('crescendo_engine.netlist_providers.verify_sources_unchanged', return_value={'ok': True}), \
             mock.patch('crescendo_engine.private_netlist.verify_sources_unchanged', return_value={'ok': True}):
            for mode in ('template', 'maestro'):
                summary = {}
                with timing.observe_preparation(summary, mode):
                    if mode == 'template':
                        manifest = prepare_private_netlist(prepared, parameters, config,
                            os.path.join(root, mode), spec=spec)
                    else:
                        manifest = prepare_maestro_netlists(prepared, parameters, {'p': 2e-6}, config,
                            os.path.join(root, mode), 'c1', spec=spec)
                # Validation rereads all hashes, independent of the observation.
                assert validate_manifest(manifest)
                observations[mode] = summary
    return observations


class PreparationTimingTests(unittest.TestCase):
    def test_nested_reentry_is_exclusive_and_published_only_once(self):
        clock, result, nested = Clock(), {}, {}
        with timing.observe_preparation(result, 'one', clock):
            clock.advance(2)
            with timing.preparation_stage('template_qualification'):
                clock.advance(3)
                with timing.observe_preparation(nested, 'ignored'):
                    with timing.preparation_stage('model_dependency_freeze'):
                        clock.advance(5)
                        with timing.preparation_stage('model_dependency_freeze'):
                            clock.advance(7)
                clock.advance(11)
        self.assertEqual(nested, {})
        self.assertEqual(result['elapsed_seconds'], 28)
        self.assertEqual({k: v['elapsed_seconds'] for k, v in result['phases'].items()},
            dict(template_qualification=14, model_dependency_freeze=12, other=2,
                 session_lifecycle=0, native_batch_capture=0))

    def test_native_session_and_lock_are_carved_out_of_capture(self):
        clock, summary = Clock(), {}
        with tempfile.TemporaryDirectory() as root:
            opened, closing = [os.path.join(root, p) for p in ('opened', 'closing')]
            for path, epoch in ((opened, 104), (closing, 108)):
                with open(path, 'w') as stream:
                    stream.write('observed')
                os.utime(path, (epoch, epoch))
            with timing.observe_preparation(summary, 'one', clock):
                with timing.preparation_stage('native_batch_capture'):
                    clock.advance(10)
                    timing.observe_capture_command(100, 110, {'lock_wait_seconds': 2}, opened, closing)
        values = {k: v['elapsed_seconds'] for k, v in summary['phases'].items()}
        self.assertEqual(values['native_batch_capture'], 4)
        self.assertEqual(values['session_lifecycle'], 4)
        self.assertEqual(values['other'], 2)
        self.assertEqual(sum(values.values()), 10)
        self.assertEqual(summary['session_samples'], 1)

    def test_missing_or_bad_markers_do_not_create_zero_session_evidence(self):
        for lock in (2, None, 99):
            clock, summary = Clock(), {}
            with timing.observe_preparation(summary, 'one', clock):
                with timing.preparation_stage('template_qualification'):
                    clock.advance(10)
                    timing.observe_capture_command(100, 110, {'lock_wait_seconds': lock}, '', '')
            self.assertIsNone(summary['phases']['session_lifecycle']['elapsed_seconds'])
            self.assertEqual(summary['session_missing'], 1)
            self.assertEqual(summary['elapsed_seconds'], 10)
            self.assertGreaterEqual(summary['phases']['template_qualification']['elapsed_seconds'], 0)

    def test_failure_publishes_and_clock_failure_preserves_original_error(self):
        summary = {}
        with self.assertRaisesRegex(RuntimeError, 'primary'):
            with timing.observe_preparation(summary, 'one', lambda: None):
                with timing.preparation_stage('template_qualification'):
                    raise RuntimeError('primary')
        self.assertFalse(summary['available'])
        self.assertTrue(all(p['elapsed_seconds'] is None for p in summary['phases'].values()))
        self.assertIsNone(getattr(timing._LOCAL, 'observer', None))

    def test_candidate_fanout_and_report_keep_one_shared_generation_sample(self):
        clock, summary = Clock(), {}
        with timing.observe_preparation(summary, 'generation:0', clock):
            clock.advance(8)
        observer = DiagnosticTiming(clock=clock)
        observer.start('netlist_prepare')
        observer.netlist_preparation = summary
        observer.finish()
        rows = [{'timing': {'phases': timing.candidate_phases(copy.deepcopy(summary))}} for _ in range(4)]
        generation = generation_timing_summary(0, observer, rows)
        samples = [s for s in generation['candidate_phase_sums'] if s['phase'].startswith('prepare_')]
        self.assertTrue(all(s['count'] == 1 and s['scope'] == 'batch_sum' for s in samples))
        self.assertEqual(sum(s['elapsed_seconds'] for s in samples), 8)
        self.assertEqual(generation_report_timing(generation)['netlist_preparation'], summary)
        # Old results retain unknown preparation rather than invented samples.
        self.assertEqual(generation_report_timing({})['netlist_preparation'], {})

    def test_resumed_attempts_have_distinct_ids_but_each_fanout_counts_once(self):
        summaries = []
        for seconds in (4, 7):
            summary, clock = {}, Clock()
            with timing.observe_preparation(summary, 'same-run:generation:0', clock):
                clock.advance(seconds)
            summaries.append(summary)
        self.assertNotEqual(summaries[0]['batch_id'], summaries[1]['batch_id'])
        rows = [{'timing': {'phases': timing.candidate_phases(s)}}
                for s in (summaries[0], summaries[1], summaries[1])]
        observer = DiagnosticTiming(clock=lambda: 0)
        observer.start('evaluate'); observer.finish()
        samples = generation_timing_summary(0, observer, rows, resumed=True)['candidate_phase_sums']
        phase = next(s for s in samples if s['phase'] == 'prepare_other')
        self.assertEqual((phase['count'], phase['elapsed_seconds']), (2, 11))

    def test_frontends_keep_four_reference_captures_and_real_freeze_gates(self):
        observations = frontend_observations()
        self.assertEqual(observations['template']['native_capture_count'], 4)
        self.assertEqual(observations['maestro']['native_capture_count'], 1)
        for mode, summary in observations.items():
            self.assertEqual(summary['session_missing'], 0)
            self.assertEqual(summary['session_samples'], 4 if mode == 'template' else 1)
            self.assertGreater(summary['phases']['model_dependency_freeze']['elapsed_seconds'], 0)
            self.assertAlmostEqual(sum(p['elapsed_seconds'] for p in summary['phases'].values()),
                                   summary['elapsed_seconds'])
        self.assertEqual(observations['template']['phases']['native_batch_capture']['elapsed_seconds'], 0)
        self.assertEqual(observations['maestro']['phases']['template_qualification']['elapsed_seconds'], 0)

    def test_fresh_netlist_hash_is_reused_and_later_tampering_still_fails(self):
        from crescendo_engine import netlist_providers as providers
        fixture = provider_fixture.NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory() as root:
            prepared, parameters, config, spec, model = fixture.fixtures(root)
            with mock.patch.object(providers, 'build_workspace', side_effect=fixture.workspace), \
                 mock.patch.object(providers, '_capture_phase', side_effect=fixture.capture(model)), \
                 mock.patch.object(providers, 'verify_sources_unchanged', return_value={'ok': True}), \
                 mock.patch.object(providers, '_digest', wraps=providers._digest) as digest:
                manifest = prepare_maestro_netlists(prepared, parameters, {'p': 2e-6}, config,
                    os.path.join(root, 'numeric'), 'c1', spec=spec)
            task = manifest['tasks'][0]
            self.assertEqual(sum(c[0][0] == task['netlist_path'] for c in digest.call_args_list), 1)
            self.assertEqual(task['netlist_sha256'], task['artifact_sha256'][task['netlist_path']])
            self.assertTrue(validate_manifest(manifest))
            with open(task['netlist_path'], 'a') as stream:
                stream.write('// changed after freeze\n')
            with self.assertRaisesRegex(Exception, 'changed|hash|identity'):
                validate_manifest(manifest)

    def test_generation_owner_retains_failed_preparation_and_does_not_requalify(self):
        from tests.test_netlist_optimizer import fixtures
        from crescendo_engine import runner
        clock = Clock()
        with tempfile.TemporaryDirectory() as root:
            prepared, config, spec = fixtures(root)
            adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared['parameters'], config, runner._failure_metrics)
            def prepare(*args):
                with timing.preparation_stage('template_qualification'):
                    clock.advance(8)
                raise ValueError('failed reference')
            with mock.patch.object(adapter, '_prepare_generation', side_effect=prepare):
                with self.assertRaisesRegex(ValueError, 'failed reference'):
                    adapter.prepare_generation(0, [])
            self.assertIn(0, adapter.generation_preparation_timings)

    def test_generation_fallback_and_fanout_share_qualification_once(self):
        from tests.test_netlist_optimizer import fixtures
        from crescendo_engine import runner
        from crescendo_engine.generation_batch import BatchUnsupported
        for scalar_fallback in (False, True):
            clock = Clock()
            with tempfile.TemporaryDirectory() as root:
                prepared, config, spec = fixtures(root)
                adapter = PrivateNetlistOptimizer(root, spec, prepared, prepared['parameters'], config, runner._failure_metrics)
                adapter.context = mock.Mock()
                adapter.maestro_producer = mock.Mock()
                adapter.native_fallback = {'synthetic': True}
                first = [True]
                def prepare(*args):
                    if first[0]:
                        first[0] = False
                        with timing.preparation_stage('template_qualification'):
                            for _ in range(4):
                                timing.capture_observed()
                                clock.advance(3)
                def native(*args):
                    with timing.preparation_stage('native_batch_capture'):
                        timing.capture_observed()
                        clock.advance(5)
                def generate_batch(plan):
                    if scalar_fallback:
                        raise BatchUnsupported('synthetic scalar fallback')
                    native()
                    plan['candidate_to_physical'] = {c['candidate_id']: [] for c in plan['candidates']}
                    return {r['row_id']: {'candidate_id': r['candidate_ids'][0]} for r in plan['rows']}
                def generate(values, path, cid):
                    native()
                    return {'candidate_id': cid}
                adapter.maestro_producer.generate_batch.side_effect = generate_batch
                adapter.maestro_producer.generate.side_effect = generate
                pending = [(0, {'x': 0.5}), (1, {'x': 0.5})]
                with mock.patch.object(adapter, '_prepare_once', side_effect=prepare), \
                     mock.patch.object(adapter, '_apply_native_fallback'), \
                     mock.patch('crescendo_engine.private_netlist.validate_manifest'), \
                     mock.patch('crescendo_engine.simulator_profiles.require_frozen_model_profile'), \
                     mock.patch.object(timing, 'timestamp', side_effect=lambda clock=None: fake_clock()), \
                     mock.patch.object(adapter, '_evaluate_candidate', return_value={'status': 'completed', 'timing': {'phases': {}}}):
                    fake_clock = clock
                    inputs = adapter.prepare_generation(0, pending)
                    rows = [adapter.evaluate(i, 0, v, inputs['g000_c%04d' % i]) for i, v in pending]
                    adapter.prepare_generation(1, pending)
                a, b = [adapter.generation_preparation_timings[i] for i in (0, 1)]
                self.assertEqual(a['phases']['template_qualification']['elapsed_seconds'], 12)
                self.assertEqual(b['phases']['template_qualification']['elapsed_seconds'], 0)
                self.assertEqual(a['native_capture_count'], 5 if not scalar_fallback else 6)
                if not scalar_fallback:
                    observer = DiagnosticTiming(clock=clock)
                    observer.start('evaluate')
                    observer.finish()
                    sums = generation_timing_summary(0, observer, rows)['candidate_phase_sums']
                    sample = next(s for s in sums if s['phase'] == 'prepare_template_qualification')
                    self.assertEqual((sample['count'], sample['elapsed_seconds']), (1, 12))

    def test_concurrent_scalar_observers_do_not_share_thread_state(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        barrier = threading.Barrier(2)
        def observe(name, seconds):
            summary, clock = {}, Clock()
            with timing.observe_preparation(summary, name, clock):
                with timing.preparation_stage(name):
                    barrier.wait(timeout=5)
                    clock.advance(seconds)
            return summary
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(observe, 'template_qualification', 3)
            b = pool.submit(observe, 'native_batch_capture', 7)
            first, second = a.result(), b.result()
        self.assertEqual(first['phases']['template_qualification']['elapsed_seconds'], 3)
        self.assertEqual(first['phases']['native_batch_capture']['elapsed_seconds'], 0)
        self.assertEqual(second['phases']['native_batch_capture']['elapsed_seconds'], 7)
        self.assertEqual(second['phases']['template_qualification']['elapsed_seconds'], 0)

    def test_focused_report_keeps_passive_preparation_next_to_F1_F4(self):
        from crescendo_engine.diagnostic_focus import focused_checks
        summary = {'schema_version': 'crescendo.netlist_preparation_timing.v1', 'phases': {}}
        row = {'case_results': {'template': {'generation_timings': [dict(generation=1, netlist_preparation=summary)]}},
               'prepared': {'setup': {'tests': []}}, 'spec': {}}
        report = focused_checks(row)
        self.assertTrue(report['netlist_preparation']['observation_only'])
        self.assertEqual(report['netlist_preparation']['modes']['template'][0]['timing'], summary)
        self.assertTrue(all(k in report for k in ('F1', 'F2', 'F3', 'F4')))
