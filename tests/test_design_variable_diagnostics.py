"""Priority projection across frozen execution and diagnostic verification."""
import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import runner
from crescendo_engine.design_variables import catalog, priority_report, SCHEMA
from crescendo_engine.diagnostic_plan import freeze_plan, digest, project_frozen_inputs
from crescendo_engine.environment_test import EnvironmentTestManager, _candidate_plan_observation
from crescendo_engine.metrics import evaluate_metrics
from crescendo_engine.optimizers import validate_candidate_values
from crescendo_engine.single_test import project_setup
from crescendo_engine.utils import atomic_write_json, load_json
from tests import test_environment_test_runner as runner_fixtures
from tests.test_unified_optimizer import common_adapter
from tests import test_environment_test as fixtures


class DesignVariableDiagnosticTests(unittest.TestCase):
    def fixture(self, root, multi=True):
        spec = runner_fixtures.EnvironmentTestRunnerTests()._spec()
        setup = project_setup(dict(tests=[dict(name='main', library='lib', cell='tb', view='schematic')],
                                   global_variables=[dict(name='bias', value='1')]), 'main')
        control = catalog(setup, {})[0]
        key = control['parameter_id']
        device = dict(parameter_id='x', backend='schematic', optimizable=True, library='lib',
                      cell='tb', view='schematic', instance='R1', property='r', hierarchy_path='/R1')
        prepared = dict(setup=setup, hierarchy={'designs': []}, parameters=[device, control],
                        outputs=[dict(key='main::gain'), dict(key='main::power')])
        spec['variables'].append(dict(parameter_id=key, minimum=0, maximum=2))
        spec.update(diagnostic_generations=1, diagnostic_samples=2 if multi else 1)
        plan, shape = freeze_plan(spec, {'x': .5, key: 1})
        spec.update(generations=shape['generations'], population=shape['samples_per_generation'])
        spec['algorithm_parameters']['population'] = shape['samples_per_generation']
        spec['environment_test'].update(frozen_candidate=plan[0]['values'],
                                       frozen_candidate_sha256=digest(plan[0]['values']))
        if multi:
            spec['environment_test'].update(frozen_plan=plan, frozen_plan_sha256=digest(plan))
        path = os.path.join(root, 'input.scs')
        with open(path, 'w') as stream:
            stream.write('// Library name: lib\n// Cell name: tb\n// View name: schematic\n'
                         'parameters bias=1 alias=bias*1k\nR1 (a 0) resistor r=alias\n')
        priority = priority_report([control], [device], [dict(test='main', point_id=1, corner='TT', netlist_path=path)])
        self.assertEqual(priority['locked_parameter_ids'], ['x'])
        return spec, prepared, priority, key

    def test_runner_projects_single_and_multi_plans_for_both_netlist_modes(self):
        for mode in ('maestro_netlist', 'private_netlist'):
            for multi in (False, True):
                with self.subTest(mode=mode, multi=multi), tempfile.TemporaryDirectory() as root:
                    spec, prepared, priority, key = self.fixture(root, multi)
                    spec['execution_backend'] = mode
                    original = copy.deepcopy(spec)
                    seen, instances = [], []
                    base = common_adapter(instances)
                    class Adapter(base):
                        def evaluate(self, index, generation, values):
                            validate_candidate_values(values, self.spec['variables'])
                            seen.append(dict(generation=generation, index=index, values=copy.deepcopy(values)))
                            cid = 'g%03d_c%04d' % (generation, index)
                            metrics = evaluate_metrics({'output_map': {
                                'main::gain': {'summary': {'mean': values[key]}},
                                'main::power': {'summary': {'mean': 1}}}}, self.spec['objectives'], self.spec['constraints'])
                            row = dict(candidate_id=cid, generation=generation, index=index, values=values,
                                       status='completed', metrics=metrics, execution_backend=mode,
                                       completion_evidence={'ok': True, 'fixture_only': True})
                            candidate_root = os.path.join(self.run_root, 'candidates', cid)
                            os.makedirs(candidate_root, exist_ok=True)
                            atomic_write_json(os.path.join(candidate_root, 'candidate.json'), row)
                            return row
                    with mock.patch.object(runner, 'PrivateNetlistOptimizer', Adapter), \
                         mock.patch.object(runner, 'startup_storage_preflight', return_value={'ok': True}), \
                         mock.patch.object(runner, '_reverify_prepared_external_environment', return_value={'ok': True}), \
                         mock.patch('crescendo_engine.design_variables.qualify_priority', return_value=priority), \
                         mock.patch.object(runner.PopulationGenerator, 'ask', side_effect=AssertionError('resampling')):
                        result = runner.run_optimization(spec, {'runtime': {'run_root': root}}, prepared=prepared,
                                                         run_root=os.path.join(root, 'run'))
                    self.assertEqual(result['state'], 'completed')
                    expected_marker = project_frozen_inputs(original, [original['variables'][1]])
                    expected = expected_marker.get('frozen_plan') or [dict(generation=0, index=0, values=expected_marker['frozen_candidate'])]
                    self.assertEqual(seen, expected)
                    saved = load_json(os.path.join(root, 'run', 'spec.json'))
                    self.assertEqual(saved['environment_test'], expected_marker)
                    self.assertEqual([v['parameter_id'] for v in saved['variables']], [key])
                    self.assertIn('bias -> /R1/r', saved['design_variable_priority']['warnings'][0])

    def test_projection_preserves_repeated_points_and_original_inputs(self):
        with tempfile.TemporaryDirectory() as root:
            spec, prepared, priority, key = self.fixture(root)
            marker = spec['environment_test']
            marker['frozen_plan'][1]['values'] = {'x': .75, key: 1.0}
            marker['frozen_plan_sha256'] = digest(marker['frozen_plan'])
            before = copy.deepcopy(spec)
            projected = project_frozen_inputs(spec, [spec['variables'][1]])
            self.assertEqual(spec, before)
            self.assertEqual([p['values'] for p in projected['frozen_plan']], [{key: 1}] * 2)
            self.assertEqual([(p['generation'], p['index']) for p in projected['frozen_plan']], [(0, 0), (0, 1)])
            self.assertEqual(projected['frozen_candidate_sha256'], digest({key: 1.0}))
            self.assertEqual(projected['frozen_plan_sha256'], digest(projected['frozen_plan']))

    def test_projection_rejects_tampering_even_in_removed_dimension(self):
        with tempfile.TemporaryDirectory() as root:
            spec, prepared, priority, key = self.fixture(root)
            for field in ('frozen_candidate', 'frozen_plan'):
                changed = copy.deepcopy(spec)
                if field == 'frozen_candidate':
                    changed['environment_test'][field]['x'] = .75
                else:
                    changed['environment_test'][field][1]['values']['x'] = .75
                with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'digest'):
                    project_frozen_inputs(changed, [spec['variables'][1]])

    def test_manager_uses_projection_and_rejects_retained_value_or_mode_drift(self):
        with tempfile.TemporaryDirectory() as root:
            spec, prepared, priority, key = self.fixture(root)
            effective = copy.deepcopy(spec)
            effective['variables'] = [spec['variables'][1]]
            effective['environment_test'] = project_frozen_inputs(spec, effective['variables'])
            effective['design_variable_priority'] = priority
            service = mock.Mock()
            service.read.return_value = effective
            manager = EnvironmentTestManager(service)
            row = dict(prepared=prepared, diagnostic_shape={})
            plan = manager._effective_plan(row, spec, 'owned')
            self.assertEqual(plan, effective['environment_test']['frozen_plan'])
            candidates = [dict(p, candidate_id='g%03d_c%04d' % (p['generation'], p['index'])) for p in plan]
            self.assertEqual(_candidate_plan_observation(candidates, plan, 'completed'), 'PASS')
            candidates[1]['values'] = {key: .25}
            self.assertEqual(_candidate_plan_observation(candidates, plan, 'completed'), 'FAIL')
            effective['environment_test']['frozen_plan'][1]['values'][key] = .25
            with self.assertRaisesRegex(ValueError, 'projection'):
                manager._effective_plan(row, spec, 'owned')
            effective['environment_test'] = spec['environment_test']
            effective['variables'] = spec['variables']
            effective['design_variable_priority']['locked_parameter_ids'] = []
            with self.assertRaisesRegex(ValueError, 'across execution modes'):
                manager._effective_plan(row, spec, 'other')


    def test_manager_rejects_removal_of_design_or_unknown_axes(self):
        with tempfile.TemporaryDirectory() as root:
            spec, prepared, priority, key = self.fixture(root)
            service = mock.Mock()
            manager = EnvironmentTestManager(service)
            for removed in ([key], ['unknown']):
                saved = copy.deepcopy(spec)
                saved['design_variable_priority'] = dict(schema_version=SCHEMA, locked_parameter_ids=removed)
                service.read.return_value = saved
                with self.subTest(removed=removed), self.assertRaisesRegex(ValueError, 'priority evidence'):
                    manager._effective_plan(dict(prepared=prepared), spec, 'owned')

    def test_device_only_diagnostic_expectations_keep_original_identity(self):
        spec = runner_fixtures.EnvironmentTestRunnerTests()._spec()
        plan, shape = freeze_plan(spec, {'x': .5})
        spec.update(generations=shape['generations'], population=shape['samples_per_generation'])
        spec['environment_test'].update(frozen_plan=plan, frozen_plan_sha256=digest(plan))
        service = mock.Mock()
        service.read.return_value = spec
        manager = EnvironmentTestManager(service)
        row = dict(diagnostic_shape=shape)
        self.assertEqual(manager._effective_plan(row, spec, 'legacy'), plan)
        self.assertEqual(shape['plan_sha256'], digest(plan))


class DesignVariableDiagnosticManagerTests(unittest.TestCase):
    setUp = fixtures.EnvironmentTestManagerTests.setUp
    fixture = fixtures.EnvironmentTestManagerTests.fixture
    finish = fixtures.EnvironmentTestManagerTests.finish
    checks = fixtures.EnvironmentTestManagerTests.checks
    finish_threads = fixtures.EnvironmentTestManagerTests.finish_threads

    def test_three_mode_manager_verifies_projected_plans_and_focused_expectations(self):
        manager, service, spec = self.fixture()
        prepared = service.prepared['prepared-fixture']
        prepared['parameters'][0].update(source_kind='global', source='setup global', variable_test='main')
        prepared['parameters'].append(dict(parameter_id='dependent', backend='schematic', optimizable=True, numeric_value=.5))
        spec['variables'].append(dict(parameter_id='dependent', minimum=0, maximum=1))
        original_start = service.start
        def start(prepared_id, request):
            # Match real service registration before runner qualifies dimensions.
            parent = manager.run_root(request['environment_test'], request['execution_backend'])
            effective = copy.deepcopy(request)
            effective['variables'] = [v for v in request['variables'] if v['parameter_id'] != 'dependent']
            effective['environment_test'] = project_frozen_inputs(request, effective['variables'])
            effective['design_variable_priority'] = dict(schema_version=SCHEMA, locked_parameter_ids=['dependent'])
            with mock.patch.object(manager, 'run_root', return_value=parent):
                return original_start(prepared_id, effective)
        service.start = start
        payload = self.finish(manager, manager.start('prepared-fixture', spec)['test_id'])
        self.assertEqual(payload['state'], 'completed')
        self.assertEqual(len(service.starts), 3, payload['checks'])
        self.assertFalse(self.checks(payload, 'candidate_identity'))
        row = manager.tests[payload['test_id']]
        self.assertTrue(all(set(p['values']) == {'x'} for p in row['effective_diagnostic']['plan']))
        self.assertTrue(all(set(p['values']) == {'x', 'dependent'} for p in row['frozen_plan']))
        with mock.patch('crescendo_engine.diagnostic_focus.consistency_check', return_value={}) as check:
            from crescendo_engine.diagnostic_focus import focused_checks
            focused_checks(row)
        expected = check.call_args[0][3]
        self.assertTrue(expected)
        self.assertTrue(all(set(v) == {'x'} for v in expected.values()))
