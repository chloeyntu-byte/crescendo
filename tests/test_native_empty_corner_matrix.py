import unittest
from crescendo_engine.native_identity import corner_contract, corner_matches, point_variable_checks, numeric_domain, qualify_expansion, sweep_coverage


class NativeEmptyCornerMatrixTests(unittest.TestCase):
    def test_empty_corner_value_does_not_assert_presence_or_absence(self):
        setup = {'global_variables': [{'name': 'DATA_FILE', 'value': '"probe.s1p"'}]}
        test = {'name': 'A', 'variables': []}
        corner = {'name': 'EMPTY', 'variables': [{'name': 'DATA_FILE', 'value': ''}]}
        contract = corner_contract(setup, corner, 'A')
        self.assertIsNone(contract['error'])
        absent = {'point_id': 1, 'corner': 'EMPTY', 'native_parameters': []}
        emitted = {'point_id': 1, 'corner': 'EMPTY', 'native_parameters': [
            {'name': 'DATA_FILE', 'value': None, 'expression': '"probe.s1p"'}]}
        self.assertEqual(corner_matches(absent, contract), [0])
        self.assertEqual(corner_matches(emitted, contract), [0])
        self.assertEqual(point_variable_checks(setup, test, corner, absent, {})[0], [])
        self.assertEqual(point_variable_checks(setup, test, corner, emitted, {})[0], [])
        self.assertIsNone(point_variable_checks(setup, test, corner, emitted, {})[1][0]['ok'])

    def test_repeated_corner_values_are_counted_as_distinct_occurrences(self):
        setup = {'corners': [{'name': 'C', 'variables': [{'name': 'X', 'value': '1 1 2'}]}]}
        rows = [{'test': 'A', 'corner': 'C_%d' % n, 'point_id': n,
                 'native_parameters': [{'name': 'X', 'value': v}]} for n, v in enumerate((1, 1, 2))]
        mapping = {}
        report = qualify_expansion(setup, rows, {'C': {'count': 3}}, mapping)
        self.assertEqual(report['errors'], [])
        self.assertEqual(len(mapping), 3)
        report = qualify_expansion(setup, rows[:2], {'C': {'count': 3}}, {})
        self.assertIn('native_corner_combinations_missing_or_duplicated', report['errors'])

    def test_close_corner_values_prefer_exact_native_value(self):
        setup = {'corners': [{'name': 'C', 'variables': [{'name': 'X', 'value': '1 1.00000000001'}]}]}
        rows = [{'test': 'A', 'corner': 'C_%d' % n, 'point_id': n,
                 'native_parameters': [{'name': 'X', 'value': v}]} for n, v in enumerate((1.0, 1.00000000001))]
        self.assertEqual(qualify_expansion(setup, rows, {'C': {'count': 2}}, {})['errors'], [])

    def test_string_and_numeric_design_sweep_require_all_combinations(self):
        test = {'name': 'A', 'variables': [{'name': 'X', 'value': '1 2'},
                                          {'name': 'S', 'value': '"a" "b"'}]}
        rows = [{'point_id': i, 'corner': 'C', 'native_parameters': [
            {'name': 'X', 'value': x}, {'name': 'S', 'value': None, 'expression': '"%s"' % name}]}
            for i, (x, name) in enumerate(((1, 'a'), (1, 'b'), (2, 'a'), (2, 'b')))]
        self.assertEqual(sweep_coverage({}, test, {'C': {}}, rows, {'C': 'C'}, strict_multiplicity=True), [])
        self.assertTrue(sweep_coverage({}, test, {'C': {}}, rows[:-1], {'C': 'C'}, strict_multiplicity=True))

    def test_close_design_sweep_values_prefer_exact(self):
        test = {'name': 'A', 'variables': [{'name': 'X', 'value': '1 1.00000000001'}]}
        rows = [{'point_id': i, 'corner': 'C', 'native_parameters': [{'name': 'X', 'value': value}]}
                for i, value in enumerate((1.0, 1.00000000001))]
        self.assertEqual(sweep_coverage({}, test, {'C': {}}, rows, {'C': 'C'}, strict_multiplicity=True), [])

    def test_native_constant_arithmetic_corner_and_wrong_value(self):
        corner = {'variables': [{'name': 'VCM', 'value': '2*2'}]}
        contract = corner_contract({}, corner, 'A')
        self.assertIsNone(contract['error'])
        self.assertEqual(corner_matches({'native_parameters': [{'name': 'VCM', 'value': 4}]}, contract), [0])
        self.assertEqual(corner_matches({'native_parameters': [{'name': 'VCM', 'value': 5}]}, contract), [])
        corner['variables'][0]['value'] = '2*UNKNOWN'
        self.assertEqual(corner_contract({}, corner, 'A')['error'], 'corner_identity_source_unresolved')

    def test_real_native_range_and_discrete(self):
        self.assertEqual(numeric_domain('{From/To}Linear:1:2:9{From/To}'), [1, 3, 5, 7, 9])
        self.assertEqual(numeric_domain('1 3 5'), [1, 3, 5])


if __name__ == '__main__':
    unittest.main()
