"""Generation axis identities, no replay, durable fanout and legacy settings."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import generation_batch as batch
from crescendo_engine.config import DEFAULTS
from crescendo_engine.native_maestro_parameters import build_native_parameter_plan
from crescendo_engine.rdb import slice_candidate_rdb
from crescendo_engine.utils import load_json


class GenerationPlanTests(unittest.TestCase):
    def plan(self):
        plan = batch.build_generation_plan(0, [(0, {'a': 1, 'b': 4}), (1, {'a': 2, 'b': 3}),
            (2, {'a': 3, 'b': 2}), (3, {'a': 1, 'b': 4})],
            [{'parameter_id': 'a'}, {'parameter_id': 'b'}], {'test': 'T', 'analysis': 'dc'})
        rows = [{'parameter_id': k, 'library': 'LIB_A', 'cell': 'C', 'view': 'schematic',
                 'instance': k, 'property': 'r', 'value': 1} for k in ('a', 'b')]
        scalar = build_native_parameter_plan(rows, ['cold', 'hot'])
        batch.build_native_columns(plan, scalar['selected'])
        return plan, scalar

    def readback(self, plan):
        parsed = {'points': [], 'test_statuses': []}
        native = []
        for i, row in enumerate(plan['rows']):
            # Deliberately non-contiguous, non-ordinal native design/physical IDs.
            design = [91, 7, 42][i]
            for j, corner in enumerate(('cold', 'hot')):
                point = [810, 11, 102, 999, 14, 55][i * 2 + j]
                parsed['points'].append({'pointID': point, 'designPointNumber': design, 'corner_name': corner})
                parsed['test_statuses'].append({'pointID': point, 'test_name': 'T', 'statusCode': 3})
                native.append({'design_point': design, 'corner': corner, 'test': 'T',
                    'results_dir': '/owned/history/%d/T' % point,
                    'conditions': {c['path']: row['values'][c['parameter_id']] for c in plan['columns']},
                    'corner_parameters': {c['path'].rsplit('/', 1)[0] + '.r': row['values'][c['parameter_id']] for c in plan['columns']}})
        return parsed, list(reversed(native))

    def test_native_run_quantizes_to_six_significant_digits(self):
        variables = [dict(parameter_id='a', minimum=-2e12, maximum=2e12)]
        for original, expected in ((1.23456789e-7, 1.23457e-7), ('123.456789n', 1.23457e-7),
                                   (1.23456789e12, 1.23457e12), (-1234.56789, -1234.57), (0, 0)):
            with self.subTest(original=original):
                proposed = {'a': original}
                canonical = batch.native_run_values(proposed, variables, quantize=True)
                self.assertEqual(canonical, {'a': expected})
                self.assertEqual(batch.native_run_values(canonical, variables), canonical)
                self.assertEqual(proposed, {'a': original})

    def test_native_run_quantization_directs_boundary_rounding_inward(self):
        for lower, upper, original, expected in (
                (1.234561, 2, 1.234561, 1.23457),
                (1, 1.234569, 1.234569, 1.23456),
                (-1.234569, -1, -1.234569, -1.23456),
                (-2, -1.234561, -1.234561, -1.23457),
                (9.999991, 10.000001, 9.999999, 10.0)):
            with self.subTest(original=original):
                variables = [dict(parameter_id='a', minimum=lower, maximum=upper)]
                self.assertEqual(batch.native_run_values({'a': original}, variables, quantize=True), {'a': expected})
        variables = [dict(parameter_id='a', minimum=1.234561, maximum=1.234569)]
        with self.assertRaisesRegex(batch.BatchUnsupported, 'within bounds'):
            batch.native_run_values({'a': 1.234565}, variables, quantize=True)

    def test_native_run_preserves_linked_coordinate_or_declines_batch(self):
        from crescendo_engine.optimizers import validate_candidate_values
        for scale in ('linear', 'log'):
            variables = [dict(parameter_id='a', minimum=1, maximum=3, link_group='pair', scale=scale),
                         dict(parameter_id='b', minimum=10, maximum=30, link_group='pair', scale=scale)]
            values = batch.native_run_values({'a': 1.23456789, 'b': 12.3456789}, variables, quantize=True)
            self.assertEqual(values, {'a': 1.23457, 'b': 12.3457})
            self.assertEqual(validate_candidate_values(values, variables), values)
        variables = [dict(parameter_id='a', minimum=1, maximum=3, link_group='pair'),
                     dict(parameter_id='b', minimum=0, maximum=1, link_group='pair')]
        original = {'a': 1.23456789, 'b': (1.23456789 - 1) / 2}
        validate_candidate_values(original, variables)
        self.assertEqual(batch.native_run_values(original, variables, quantize=True),
                         {'a': 1.23457, 'b': 0.117285})
        variables[1]['maximum'] = 1.234567891
        original['b'] *= variables[1]['maximum']
        validate_candidate_values(original, variables)
        with self.assertRaisesRegex(batch.BatchUnsupported, 'cannot preserve linked'):
            batch.native_run_values(original, variables, quantize=True)
        variables[1].update(maximum=1, step=0.5, step_count=2)
        values = batch.native_run_values({'a': 2.000001, 'b': 0.5}, variables, quantize=True)
        self.assertEqual(values, {'a': 2.0, 'b': 0.5})

    def test_fixed_and_already_recorded_native_values_are_never_rewritten(self):
        for variable in (dict(value_type='integer'), dict(step=1e-7), dict(value_type='enum')):
            configured = [dict(variable, parameter_id='a', minimum=0, maximum=2e9)]
            with self.assertRaisesRegex(batch.BatchUnsupported, 'fixed'):
                batch.native_run_values({'a': 123456789}, configured, quantize=True)
        with self.assertRaisesRegex(batch.BatchUnsupported, 'fixed'):
            batch.native_run_values({'a': 1955.0553081978717},
                                    [dict(parameter_id='a', minimum=1100, maximum=3000)])

    def test_native_run_uses_exact_grid_origin_for_global_and_device_columns(self):
        for source_kind in ('global', 'device'):
            with self.subTest(source_kind=source_kind):
                variables = [
                    dict(parameter_id='a', minimum=12, maximum=22, grid_origin=2, step=0.005),
                    dict(parameter_id='b', minimum=0, maximum=1.5e-13),
                ]
                raw = {'a': 17.990000000000002, 'b': 1.0834040395541103e-13}
                canonical = batch.native_run_values(raw, variables, quantize=True)
                self.assertEqual(canonical, {'a': 17.99, 'b': 1.08340e-13})
                self.assertEqual(batch.native_run_values(canonical, variables), canonical)
                plan = batch.build_generation_plan(0, [(0, canonical)], variables, {})
                plan['native_run_exact_values'] = True
                columns = batch.build_native_columns(plan, [
                    {'parameter_id': key, 'path': key if source_kind == 'global' else 'L/C/schematic/R/' + key,
                     'source_kind': source_kind} for key in ('a', 'b')])
                self.assertEqual([c['values'] for c in columns], [['17.99'], ['1.0834e-13']])

    def test_native_run_does_not_snap_off_grid_or_rewrite_a_frozen_tail(self):
        variables = [dict(parameter_id='a', minimum=2, maximum=8, step=0.005)]
        with self.assertRaisesRegex(batch.BatchUnsupported, 'outside its grid'):
            batch.native_run_values({'a': 3.000001}, variables, quantize=True)
        with self.assertRaisesRegex(batch.BatchUnsupported, 'fixed candidate value'):
            batch.native_run_values({'a': 2.3449999999999998}, variables)
        integers = [dict(parameter_id='a', minimum=1, maximum=50, value_type='integer')]
        result = batch.native_run_values({'a': 26.0}, integers, quantize=True)
        self.assertIs(type(result['a']), int)

    def test_native_run_ask_isolates_a_failed_link_group(self):
        variables = [dict(parameter_id='a', minimum=1, maximum=3, link_group='pair'),
                     dict(parameter_id='b', minimum=0, maximum=1.234567891, link_group='pair'),
                     dict(parameter_id='c', minimum=0, maximum=1)]
        original = {'a': 1.23456789, 'b': (1.23456789 - 1) / 2 * 1.234567891, 'c': 0.123456789}
        canonical = batch.native_run_values(original, variables, quantize=True, allow_partial=True)
        self.assertEqual(canonical, dict(original, c=0.123457))

    def test_native_run_expanded_readback_matches_exactly_and_fans_out_collisions(self):
        variables = [dict(parameter_id='a', minimum=-3000, maximum=3000),
                     dict(parameter_id='b', minimum=0, maximum=1)]
        proposed = [(0, {'a': -1955.0553081978717, 'b': 1.23456789e-7}),
                    (1, {'a': -1955.0551, 'b': 1.23456987e-7})]
        pending = [(i, batch.native_run_values(values, variables, quantize=True)) for i, values in proposed]
        plan = batch.build_generation_plan(0, pending, variables, {})
        plan['native_run_exact_values'] = True
        self.assertEqual(plan['physical_row_count'], 1)
        self.assertEqual(plan['logical_count'], 2)
        columns = batch.build_native_columns(plan, [
            {'parameter_id': 'a', 'path': 'L/C/schematic/R1/r'},
            {'parameter_id': 'b', 'path': 'L/C/schematic/R2/r'}])
        self.assertEqual([c['text'] for c in columns], ['-1955.06', '1.23457e-07'])
        tsv = ('CR_BATCH_POINTS_1\n'
               'CONDITION\t91\t"L/C/schematic/R1/r"\t"-1955.06"\n'
               'CONDITION\t91\t"L/C/schematic/R2/r"\t"1.23457e-07"\n'
               'PARAM\t91\t"cold"\t"L/C/schematic/R1.r"\t-1955.06\n'
               'PARAM\t91\t"cold"\t"L/C/schematic/R2.r"\t1.23457e-07\n'
               'POINT\t91\t"cold"\t"T"\t"/owned/history/810/T"\tnil\n'
               'END\n')
        parsed = {'points': [{'pointID': 810, 'designPointNumber': 91, 'corner_name': 'cold'}],
                  'test_statuses': [{'pointID': 810, 'test_name': 'T'}]}
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'points.tsv')
            with open(path, 'w') as stream:
                stream.write(tsv)
            native = batch.parse_native_rows(path)
        batch.map_native_points(plan, 0, 'Interactive.0', parsed, native)
        self.assertEqual(next(iter(plan['physical_to_candidates'].values())), ['g000_c0000', 'g000_c0001'])
        native[0]['corner_parameters']['L/C/schematic/R1.r'] += 1e-8
        with self.assertRaisesRegex(ValueError, 'corner device parameter differs'):
            batch.map_native_points(plan, 0, 'Interactive.0', parsed, native)
        native[0]['conditions']['L/C/schematic/R1/r'] = '-1955.06000001'
        with self.assertRaisesRegex(ValueError, 'no unique physical candidate row'):
            batch.map_native_points(plan, 0, 'Interactive.0', parsed, native)

    def test_equal_columns_single_axis_and_explicit_duplicate_fanout(self):
        plan, scalar = self.plan()
        self.assertEqual(plan['logical_count'], 4)
        self.assertEqual(plan['physical_row_count'], 3)
        self.assertEqual([len(c['values']) for c in plan['columns']], [3, 3])
        script = '\n'.join(batch.native_batch_skill_lines(plan, scalar, '/owned/readback'))
        self.assertEqual(script.count('ocnxlParametricSet('), 1)
        self.assertIn('length(crGroups)==1', script)
        self.assertIn('length(crMembers)==length(crPaths)', script)
        self.assertNotIn('ocnxlTargetCellView', script)
        self.assertNotIn('axlRemoveElement', script)

    def test_unknown_stochastic_and_user_groups_fail_before_submission(self):
        for name in ('montecarlo', 'unknown', 'mc'):
            with self.assertRaises(batch.BatchUnsupported):
                batch.qualify_deterministic({'tests': [{'analyses': [{'name': name}]}]})
        plan, scalar = self.plan()
        script = '\n'.join(batch.native_batch_skill_lines(plan, scalar, '/owned/readback'))
        self.assertLess(script.index('when(crGroups error'), script.index('ocnxlSweepParam('))
        self.assertLess(script.index('Unowned or matched source parameter conflict'), script.index('maeDeleteParameter('))
        self.assertFalse(plan['submitted'])

    def test_inactive_native_tran_seed_does_not_change_deterministic_inputs(self):
        setup = {'tests': [{'analyses': [{'name': 'tran', 'enabled': True,
            'fields': {'stop': '20u', 'noisefmax': '', 'noiseonoff': '', 'noiseseed': '1'}}]}]}
        original = copy.deepcopy(setup)
        setup['_source_tests'] = copy.deepcopy(setup['tests'])
        batch.qualify_deterministic(setup)
        self.assertEqual(setup['tests'], original['tests'])
        for fmax in ('', '0', 0):
            setup['tests'][0]['analyses'][0]['fields']['noisefmax'] = fmax
            batch.qualify_deterministic(setup)

    def test_active_tran_noise_and_unrelated_seed_remain_unqualified(self):
        for fields in ({'noisefmax': '1G'}, {'noiseonoff': 'yes'},
                       {'noisefmax': '', 'seed': '7'}):
            with self.subTest(fields=fields), self.assertRaises(batch.BatchUnsupported):
                batch.qualify_deterministic({'tests': [{'analyses': [{'name': 'tran', 'fields': fields}]}]})
        with self.assertRaises(batch.BatchUnsupported):
            batch.qualify_deterministic({'tests': [{'analyses': [{'name': 'dc'}]}],
                                        'model_metadata': {'random': 'enabled'}})

    def test_numeric_input_and_column_identity_rejection(self):
        for values in ({'a': 1}, {'a': float('inf'), 'b': 2}, {'a': '1 2', 'b': 2}):
            with self.assertRaises(batch.BatchUnsupported):
                batch.build_generation_plan(0, [(0, values)], [{'parameter_id': 'a'}, {'parameter_id': 'b'}], {})
        plan, scalar = self.plan()
        with self.assertRaises(batch.BatchUnsupported):
            batch.build_native_columns(plan, scalar['selected'][:1])

    def test_completion_order_and_reused_native_ids_across_histories(self):
        plan, _ = self.plan()
        parsed, native = self.readback(plan)
        batch.map_native_points(plan, 0, 'History.A', parsed, native)
        a = plan['candidate_to_physical']['g000_c0000']
        self.assertEqual(a, plan['candidate_to_physical']['g000_c0003'])
        self.assertEqual(len(plan['physical_to_candidates']), 6)
        self.assertTrue(all(plan['physical_to_candidates'][p] == ['g000_c0000', 'g000_c0003'] for p in a))
        old = set(plan['physical_to_candidates'])
        batch.map_native_points(plan, 0, 'History.B', parsed, native)
        self.assertFalse(old.intersection(plan['physical_to_candidates']))

    def test_missing_extra_duplicate_and_wrong_parameters_never_complete(self):
        for fault in ('missing', 'extra', 'duplicate', 'condition', 'corner_parameter', 'directory'):
            plan, _ = self.plan()
            parsed, native = self.readback(plan)
            if fault == 'missing': native.pop()
            if fault in ('extra', 'duplicate'): native.append(copy.deepcopy(native[0]))
            if fault == 'condition': native[0]['conditions'] = {}
            if fault == 'corner_parameter': native[0]['corner_parameters'] = {}
            if fault == 'directory': native[0]['results_dir'] = '/unrelated/123/T'
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                batch.map_native_points(plan, 0, 'History.A', parsed, native)

    def test_single_physical_row_declines_before_any_native_submission(self):
        from crescendo_engine.native_baseline import evaluate_native_baseline_generation

        plan = batch.build_generation_plan(0, [(0, {"a": 2}), (1, {"a": 2})],
                                           [{"parameter_id": "a"}], {})
        with self.assertRaisesRegex(batch.BatchUnsupported, "one distinct physical row"):
            evaluate_native_baseline_generation(plan, [], {}, {}, {}, "/unused")
        self.assertFalse(plan["submitted"])

    def test_failed_point_without_status_keeps_its_proven_physical_mapping(self):
        plan, _ = self.plan()
        parsed, native = self.readback(plan)
        missing = parsed['test_statuses'].pop()
        with self.assertRaisesRegex(ValueError, 'absent'):
            batch.map_native_points(plan, 0, 'History.A', parsed, native)
        mapped = batch.map_native_points(plan, 0, 'History.A', parsed, native, allow_missing_status=True)
        self.assertEqual(set(mapped), {row['row_id'] for row in plan['rows']})
        self.assertEqual(sum(point['point_id'] == missing['pointID'] and point['test'] == missing['test_name']
                             for points in mapped.values() for point in points), 1)

    def test_slice_recomputes_metric_summary_without_batch_values(self):
        values = [{'point_id': i, 'numeric': float(i)} for i in (1, 3, 9)]
        output = {'key': 'T::gain', 'test': 'T', 'name': 'gain', 'values': values}
        parsed = {'outputs': [output], 'output_map': {'T::gain': output}, 'points': [], 'parameters': [], 'test_statuses': []}
        part = slice_candidate_rdb(parsed, {'T': 'T'}, [3, 9])
        self.assertEqual(part['output_map']['T::gain']['summary']['mean'], 6)
        self.assertEqual(len(parsed['outputs'][0]['values']), 3)

    def test_reference_counts_are_durable_and_idempotent(self):
        plan, _ = self.plan()
        with tempfile.TemporaryDirectory() as root:
            root = os.path.join(root, 'batch_' + plan['batch_epoch'])
            os.mkdir(root)
            path = os.path.join(root, 'history_references.json')
            reference = {'root':root, 'batch_epoch':plan['batch_epoch']}
            refs = batch.HistoryReferences(path, plan)
            for candidate in plan['candidates']:
                self.assertGreater(load_json(path)['remaining'], 0)
                with self.assertRaises(ValueError):
                    batch.require_history_released(reference, candidate['candidate_id'])
                refs.commit(candidate['candidate_id'])
                refs.commit(candidate['candidate_id'])
            self.assertEqual(load_json(path)['remaining'], 0)
            self.assertTrue(load_json(path)['shared_paths_retained'])
            self.assertEqual(batch.require_history_released(reference, 'g000_c0000')['remaining'],0)

    def test_defaults_and_legacy_checkpoint_do_not_migrate(self):
        self.assertEqual(DEFAULTS['runtime']['native_candidate_batching'], 'generation')
        for resuming, config, expected in ((False, DEFAULTS, 'generation'), (False, {}, 'generation'),
                                           (True, DEFAULTS, 'serial')):
            spec = {}
            self.assertEqual(batch.freeze_batching(spec, config, resuming), expected)
            if resuming:
                self.assertNotIn('native_candidate_batching', spec)
                self.assertNotIn('native_batch_schema', spec)
        with self.assertRaises(ValueError):
            batch.freeze_batching({'native_candidate_batching': 'invalid'}, DEFAULTS)

    def test_netlist_protocol_is_explicit_and_legacy_generation_keeps_serial(self):
        for backend, protocol in batch.NETLIST_BATCH_PROTOCOLS.items():
            legacy = {'execution_backend': backend, 'native_candidate_batching': 'generation',
                      'native_batch_schema': batch.SCHEMA}
            original = copy.deepcopy(legacy)
            self.assertEqual(batch.freeze_batching(legacy, DEFAULTS, resuming=True), 'serial')
            self.assertEqual(legacy, original)
            self.assertFalse(batch.generation_batching_enabled(legacy))
            fresh = copy.deepcopy(legacy)
            self.assertEqual(batch.freeze_batching(fresh, DEFAULTS), 'generation')
            self.assertEqual(fresh['native_batching_protocol'], protocol)
            self.assertTrue(batch.generation_batching_enabled(fresh))
            self.assertEqual(batch.freeze_batching(fresh, DEFAULTS, resuming=True), 'generation')
            fresh['native_batching_protocol'] = protocol + '.unknown'
            with self.assertRaises(ValueError):
                batch.freeze_batching(fresh, DEFAULTS, resuming=True)
        baseline = {'execution_backend': 'maestro_baseline',
                    'native_candidate_batching': 'generation', 'native_batch_schema': batch.SCHEMA}
        self.assertEqual(batch.freeze_batching(baseline, DEFAULTS, resuming=True), 'generation')

    def test_batch_policies_keep_command_and_disable_stale_worker_reuse(self):
        from crescendo_engine.job_policy import command_job_policy
        policy = command_job_policy({}, {'command': 'local-command', 'max_jobs': 2}, ['T'])
        changed = batch.native_batch_policy(policy)
        for scope in [changed['global']] + changed['tests']:
            self.assertEqual(scope['properties']['distributionmethod'], 'Command')
            self.assertEqual(scope['properties']['usesameprocess'], '0')
        self.assertNotIn('usesameprocess', policy['global']['properties'])


class GenerationDispatchTests(unittest.TestCase):
    def execute(self, fault=None):
        from crescendo_engine import runner, native_baseline
        from tests.test_netlist_optimizer import parsed
        calls, told = [], []
        partial_done = [False]
        self.batch_plans = []
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = temp.name
        test = {'name': 'main', 'enabled': True, 'analyses': [{'name': 'dc', 'enabled': True}]}
        setup = {'backend': 'maestro', 'selected_test': 'main', 'tests': [test],
                 '_source_tests': [copy.deepcopy(test)], 'corners': [{'name': 'nominal', 'enabled': True}],
                 'test_corner_masks': {'main': {'known': True, 'disabled_corners': [], 'nominal_enabled': False}}}
        prepared = {'source_identity': {'schema_version': 'crescendo.source_design_identity.v1', 'fingerprint': 'fixture-source', 'libraries': []},
            'setup': setup, 'hierarchy': {'designs': []}, 'parameters': [
            {'parameter_id': 'x', 'backend': 'schematic', 'optimizable': True}], 'outputs': [{'key': 'main::gain'}],
            'eda_context': {'cadence': {'version_checks': []}, 'pdk': {'ok': True, 'pdk_name': 'FIXTURE', 'pdk_adapter': 'generic-cdf'},
                            'submission': {'controller': 'maestro', 'simulator': 'spectre'}}}
        config = copy.deepcopy(DEFAULTS)
        config['runtime']['run_root'] = root
        spec = {'evaluator': 'real', 'execution_backend': 'maestro_baseline', 'selected_test': 'main',
            'execution_selection': {'tests': ['main'], 'analyses': {'main': ['dc']}},
            'algorithm': 'random', 'generations': 2, 'population': 4,
            'variables': [{'parameter_id': 'x', 'minimum': 0, 'maximum': 1}],
            'objectives': [{'output_key': 'main::gain', 'direction': 'maximize', 'corners': ['nominal']}], 'constraints': []}
        proposals = [{'x': value} for value in (0.20000003, 0.40000004, 0.60000005, 0.20000006)]
        if fault == 'grid_precision':
            prepared['parameters'].append({'parameter_id': 'width', 'backend': 'schematic', 'optimizable': True})
            spec['variables'].append(dict(parameter_id='width', minimum=999990, maximum=1000000, step=0.005))
            proposals = [dict(values, width=width) for values, width in zip(
                proposals, (999995, 999995.005, 999995.01, 1000000))]

        def native_result(value):
            return {'parsed': parsed(value), 'parameter_application': {'ok': True},
                'completion_evidence': {'schema_version': 'crescendo.completion_evidence.v1', 'ok': True},
                'source_guard': {'ok': True}, 'command': {'cleanup_pending': False}}
        def generation(plan, parameters, spec, prepared, config, batch_root, **kwargs):
            calls.append(('batch', plan['generation']))
            self.batch_plans.append(copy.deepcopy(plan))
            if fault == 'before': raise batch.BatchUnsupported('synthetic capability declined')
            os.makedirs(batch_root)
            plan['submitted'] = True
            if fault in ('stop_before', 'stop_run', 'stop_readback'):
                plan['submitted'] = fault != 'stop_before'
                with open(os.path.join(root,'run','STOP'),'w') as stream: stream.write('stop')
                error = RuntimeError('synthetic stop at ' + fault)
                error.cancelled = True
                error.detail = {'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True},
                                'native_stop': {'remote_stop_proven': True}}
                raise error
            if fault == 'after':
                error = RuntimeError('synthetic post-submit crash')
                error.detail = {'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True}}
                raise error
            for ordinal, c in enumerate(reversed(plan['candidates'])):
                item = native_result(c['values']['x'])
                item['batch_reference'] = {'root': batch_root, 'batch_epoch': plan['batch_epoch']}
                kwargs['consume'](c, item)
                if fault == 'partial' and not partial_done[0] and ordinal == 1:
                    partial_done[0] = True
                    with open(os.path.join(root,'run','STOP'),'w') as stream: stream.write('partial stop')
                    error = RuntimeError('stop after durable candidate fanout')
                    error.detail = {'cleanup_pending': False, 'native_baseline': {'cleanup_proven': True},
                                    'native_stop': {'remote_stop_proven': True}}
                    raise error
        def serial(cid, values, *args, **kwargs):
            calls.append(('serial', cid))
            return native_result(values['x'])
        def tell(self, rows): told.append([r['candidate_id'] for r in rows])
        def reconcile(*args, **kwargs):
            return {'launch_attempted': True, 'cleanup_proven': True, 'native_session_closed': True,
                'native_stop': {'requested': False}, 'local_cleanup': {'state': 'dead', 'termination_proof': {'ok': True}}}
        with mock.patch.object(native_baseline, 'evaluate_native_baseline_generation', side_effect=generation), \
             mock.patch.object(native_baseline, 'evaluate_native_baseline', side_effect=serial), \
             mock.patch.object(native_baseline, 'reconcile_native_baseline', side_effect=reconcile), \
             mock.patch.object(runner, '_reverify_prepared_external_environment', return_value={'ok': True}), \
             mock.patch.object(runner.PopulationGenerator, 'ask', return_value=proposals), \
             mock.patch.object(runner.PopulationGenerator, 'tell', tell):
            if fault == 'after':
                with self.assertRaisesRegex(RuntimeError, 'post-submit crash'):
                    runner.run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root,'run'))
            else:
                result = runner.run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root,'run'))
                self.assertEqual(result['state'], 'stopped' if fault and (fault.startswith('stop_') or fault == 'partial') else 'completed')
                if fault == 'partial':
                    from tests.test_runner import _coordinated_runner_resume
                    checkpoint = load_json(os.path.join(root,'run','checkpoint.json'))
                    resumed = _coordinated_runner_resume(spec, config, prepared, os.path.join(root,'resumed'), checkpoint)
                    self.assertEqual(resumed['state'], 'completed')
        return root, calls, told

    def test_native_run_quantization_preserves_differential_evolution_target_order(self):
        from crescendo_engine.optimizers import PopulationGenerator
        from crescendo_engine.runner import _ask_native_run_candidates, _generator_state, _restore_generator

        variables = [dict(parameter_id='x', minimum=0, maximum=1)]
        generator = PopulationGenerator(variables, 'differential_evolution')
        generator.population_size = 4
        generator.previous = [
            {'candidate_id': 'target-%d' % i, 'values': {'x': i / 4}}
            for i in range(4)
        ]
        first, second = (0.12345671,), (0.12345681,)
        old, a, b, c = generator.previous
        generator._de_targets[(0.123457,)] = [old]

        def ask(count):
            self.assertEqual(count, 3)
            generator._de_targets.update({first: [a, c], second: [b]})
            return [{'x': key[0]} for key in (first, second, first)]

        with mock.patch.object(generator, 'ask', side_effect=ask):
            self.assertEqual(_ask_native_run_candidates(generator, 3), [{'x': 0.123457}] * 3)
        self.assertEqual(generator._de_targets, {(0.123457,): [old, a, b, c]})
        restored = PopulationGenerator(variables, 'differential_evolution')
        _restore_generator(restored, _generator_state(generator), generator.previous)
        self.assertEqual(restored._de_targets, generator._de_targets)

    def test_resume_skips_only_durable_completed_candidates_with_fresh_epoch(self):
        _, calls, told = self.execute('partial')
        self.assertEqual(calls, [('batch',0),('batch',0),('batch',1)])
        self.assertEqual([c['index'] for c in self.batch_plans[1]['candidates']], [0,1])
        self.assertNotEqual(self.batch_plans[0]['batch_epoch'], self.batch_plans[1]['batch_epoch'])
        self.assertEqual(list(map(len,told)), [4,4])

    def test_once_per_generation_and_every_duplicate_told(self):
        root, calls, told = self.execute()
        self.assertEqual(calls, [('batch',0),('batch',1)])
        self.assertEqual(list(map(len,told)), [4,4])
        checkpoint = load_json(os.path.join(root,'run','checkpoint.json'))
        self.assertEqual(checkpoint['spec']['native_candidate_batching'], 'generation')
        self.assertEqual(checkpoint['manifest']['native_batch_schema'], batch.SCHEMA)
        from crescendo_engine.runner import checkpoint_candidates
        self.assertEqual([r['values']['x'] for r in checkpoint_candidates(checkpoint)], [0.2, 0.4, 0.6, 0.2] * 2)
        status = load_json(os.path.join(root, 'run', 'status.json'))
        self.assertEqual([g['native_batches_effective'] for g in status['generation_timings']], [True, True])

    def test_pre_submission_fallback_is_explicit_and_serial(self):
        root, calls, told = self.execute('before')
        self.assertEqual(sum(c[0]=='serial' for c in calls),8)
        status=load_json(os.path.join(root,'run','status.json'))
        self.assertIn('synthetic capability declined', status['native_batch_fallback']['reason'])
        self.assertFalse(status['native_batch_fallback']['submitted'])

    def test_grid_precision_fallback_retains_other_quantization_and_records_values(self):
        from glob import glob
        from crescendo_engine.run_log import LOG_NAME
        from crescendo_engine.runner import checkpoint_candidates

        root, calls, _ = self.execute('grid_precision')
        run = os.path.join(root, 'run')
        self.assertEqual([action for action, _ in calls], ['serial'] * 8)
        checkpoint = load_json(os.path.join(run, 'checkpoint.json'))
        rows = checkpoint_candidates(checkpoint)
        self.assertEqual([r['values']['x'] for r in rows], [0.2, 0.4, 0.6, 0.2] * 2)
        self.assertEqual(rows[1]['values']['width'], 999995.005)
        fallback = next(item for item in map(load_json, glob(os.path.join(run, 'batch_fallback_*.json')))
                        if item['generation'] == 0)
        self.assertFalse(fallback['native_batches_effective'])
        issues = fallback['value_issues']
        self.assertEqual([issue['candidate_id'] for issue in issues], ['g000_c0001', 'g000_c0002'])
        issue = issues[0]
        self.assertEqual(issue['parameter_id'], 'width')
        self.assertEqual(issue['original_value'], 999995.005)
        self.assertEqual(issue['normalized_value'], '999995.005')
        self.assertEqual(issue['rounded_value'], '999995')
        self.assertIn('more than six significant digits', issue['reason'])
        with open(os.path.join(run, LOG_NAME), encoding='utf-8') as stream:
            events = [json.loads(line) for line in stream]
        self.assertIn(dict(issue, generation=0, batch_epoch=fallback['batch_epoch']),
                      [e['details'] for e in events if e['stage'] == 'native_batch_value_fallback'])
        self.assertEqual([e['details']['native_batches_effective'] for e in events
                          if e['stage'] == 'native_generation_batching'], [False, False])

    def test_stop_before_run_during_run_and_during_readback_do_not_tell(self):
        for phase in ('stop_before','stop_run','stop_readback'):
            _, calls, told = self.execute(phase)
            self.assertEqual(calls,[('batch',0)])
            self.assertFalse(told)

    def test_post_submission_crash_never_replays_serial(self):
        _, calls, told = self.execute('after')
        self.assertEqual(calls,[('batch',0)])
        self.assertFalse(told)


class CommandTerminalTests(unittest.TestCase):
    def proof(self, state='CANCELLED', ids=None, missing_end=False, code=0):
        import types
        from crescendo_engine.native_baseline import _command_terminal_proof
        with tempfile.TemporaryDirectory() as root:
            events = [{'event': 'submit_start', 'invocation_id': 'owned'}]
            if not missing_end:
                events.append({'event': 'submit_end', 'invocation_id': 'owned', 'job_ids': ['81'] if ids is None else ids})
            with open(os.path.join(root,'native_dispatch.jsonl'),'w') as stream:
                for event in events: stream.write(json.dumps(event)+'\n')
            driver=types.SimpleNamespace(root=root, workspace={'root':root}, config={
                'cadence':{}, 'runtime':{'execution_commands':{'scheduler_query_template':'query {job_id}'}}})
            with mock.patch('subprocess.run',return_value=types.SimpleNamespace(returncode=code,
                    stdout=json.dumps({'job_id':'81','state':state}),stderr='')) as query:
                proof=_command_terminal_proof(driver)
            return proof,query.call_count

    def test_every_owned_job_requires_exact_terminal(self):
        proof,queries=self.proof()
        self.assertTrue(proof['ok'])
        self.assertEqual(queries,1)
        for state in ('RUNNING','CANCELLING','UNKNOWN'):
            self.assertFalse(self.proof(state)[0]['ok'])
        self.assertFalse(self.proof(code=1)[0]['ok'])

    def test_incomplete_ambiguous_or_absent_receipts_stay_pending(self):
        for ids in ([],['81','82']):
            proof,count=self.proof(ids=ids)
            self.assertFalse(proof['ok'])
            self.assertEqual(count,0)
        self.assertFalse(self.proof(missing_end=True)[0]['ok'])
