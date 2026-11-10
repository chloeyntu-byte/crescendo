import unittest

from crescendo_engine.corner_selection import eligible_corner_names, selection_plan
from crescendo_engine.metrics import evaluate_metrics
from crescendo_engine.rdb import aggregate_output


class SingleTestMetricScopeTest(unittest.TestCase):
    def setup_data(self):
        return {'selected_test': 'T', 'tests': [{'name': 'T'}],
                'corners': [{'name': 'A', 'enabled': True}, {'name': 'B', 'enabled': True},
                            {'name': 'C', 'enabled': False}, {'name': '_default', 'enabled': True}],
                'test_corner_masks': {'T': {'known': True, 'disabled_corners': ['B'],
                                            'nominal_enabled': False}}}

    def test_eligible_only_proven_source_and_test_enabled(self):
        setup = self.setup_data()
        self.assertEqual(eligible_corner_names(setup, 'T'), ['A'])
        self.assertEqual(eligible_corner_names(setup, 'unknown'), [])
        plan = selection_plan(setup, {'selected_test': 'T', 'objectives': [{'output_key': 'T::x'}]})
        self.assertEqual(plan['selected'], ['A'])
        for name in ('B', 'C', '_default'):
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                selection_plan(setup, {'selected_test': 'T', 'objectives': [{'output_key': 'T::x', 'corners': [name]}]})

    def test_aggregate_first_then_score_and_constraint(self):
        parsed = {'output_map': {'T::PN': {'values': [
            {'corner': 'A_0', 'source_corner': 'A', 'numeric': -100.0},
            {'corner': 'A_1', 'source_corner': 'A', 'numeric': -90.0},
            {'corner': 'B', 'numeric': -80.0},
            {'corner': 'C', 'numeric': 10000.0},
        ]}}}
        m = evaluate_metrics(parsed, [
            {'output_key': 'T::PN', 'corners': ['A', 'B'], 'aggregation': 'max', 'direction': 'minimize'},
            {'output_key': 'T::PN', 'corners': ['A', 'B'], 'aggregation': 'max', 'direction': 'target', 'target': -100},
            {'output_key': 'T::PN', 'corners': ['A', 'B'], 'aggregation': 'mean', 'direction': 'minimize'},
        ], [
            {'output_key': 'T::PN', 'corners': ['A', 'B'], 'aggregation': 'max', 'operator': '<=', 'limit': -90},
            {'output_key': 'T::PN', 'corners': ['A', 'B'], 'aggregation': 'min', 'operator': '>=', 'limit': -90},
            {'output_key': 'T::PN', 'corners': ['A', 'B'], 'aggregation': 'max', 'operator': 'target', 'target': -100, 'tolerance': 5},
        ])
        self.assertEqual([x['value'] for x in m['objectives']], [-80, -80, -90])
        self.assertEqual(m['objectives'][0]['score'], -80)
        self.assertAlmostEqual(m['objectives'][1]['score'], .2)
        self.assertAlmostEqual(m['constraints'][0]['violation'], 10.0 / 90)
        self.assertAlmostEqual(m['constraints'][1]['violation'], 10.0 / 90)
        self.assertAlmostEqual(m['constraints'][2]['violation'], .15)
        with self.assertRaisesRegex(ValueError, 'missing selected corners'):
            aggregate_output(parsed, 'T::PN', 'max', ['A', 'missing'])

    def test_source_identity_takes_precedence_over_native_alias(self):
        parsed = {'output_map': {'T::x': {'values': [
            {'corner': 'A', 'source_corner': 'B', 'numeric': 999},
            {'corner': 'A_0', 'source_corner': 'A', 'numeric': 1}]}}}
        self.assertEqual(aggregate_output(parsed, 'T::x', 'max', ['A']), 1)
        m = evaluate_metrics(parsed, [{'output_key': 'T::x', 'corners': ['A'], 'aggregation': 'max'}], [])
        self.assertEqual(m['selected_output_statistics'][0]['point_count'], 1)
        parsed['output_map']['T::x']['values'].pop()
        with self.assertRaisesRegex(ValueError, 'no points'):
            evaluate_metrics(parsed, [{'output_key': 'T::x', 'corners': ['A'], 'aggregation': 'max'}], [])

class SingleTestHttpStartGateTest(unittest.TestCase):
    def test_start_rejects_empty_disabled_and_unknown_before_launch(self):
        from unittest import mock
        from crescendo_engine.server import OptimizerService
        setup = SingleTestMetricScopeTest().setup_data()
        setup['_source_tests'] = [{'name':'T'}]
        setup['outputs'] = [{'key':'T::x','test':'T'}]
        service = OptimizerService({'runtime': {'run_root':'/tmp'}})
        service.prepared['p'] = {'setup':setup, 'outputs':setup['outputs'], 'parameters':[]}
        base = {'evaluator':'real','variables':[], 'constraints':[]}
        with mock.patch('crescendo_engine.server.require_prepared_freshness'), \
             mock.patch('crescendo_engine.server.require_unique_run_variables'), \
             mock.patch('crescendo_engine.server.validate_variable_request_report', return_value={'valid':True}), \
             mock.patch.object(service, '_validate_live_test_bindings'), \
             mock.patch.object(service, '_launch', return_value={'run_id':'ok'}) as launch:
            for corners in ([], ['B'], ['C'], ['_default']):
                with self.subTest(corners=corners):
                    spec = dict(base, objectives=[{'output_key':'T::x','corners':corners}])
                    with self.assertRaises(ValueError):
                        service._start_registered('p', spec)
            launch.assert_not_called()
            setup['test_corner_masks']['T']['known'] = False
            with self.assertRaisesRegex(ValueError, 'mask unavailable'):
                service._start_registered('p', dict(base, objectives=[{'output_key':'T::x','corners':['A']}]))
            launch.assert_not_called()
            setup['test_corner_masks']['T']['known'] = True
            self.assertEqual(service._start_registered('p', dict(base, objectives=[{'output_key':'T::x'}])), {'run_id':'ok'})
            launch.assert_called_once()


class SingleTestCliCaptureGateTest(unittest.TestCase):
    def test_empty_scope_rejected_before_native_profile_or_capture(self):
        from unittest import mock
        from crescendo_engine.private_netlist import _capture_request_config
        setup = SingleTestMetricScopeTest().setup_data()
        with mock.patch('crescendo_engine.simulator_profiles.apply_source_profile_defaults') as profiles:
            with self.assertRaisesRegex(ValueError, 'No enabled corner'):
                _capture_request_config({'setup':setup}, {}, {'selected_test':'T',
                    'objectives':[{'output_key':'T::x','corners':[]}]})
            profiles.assert_not_called()


if __name__ == '__main__':
    unittest.main()
