import copy
import unittest

from crescendo_engine.private_netlist import validate_capture_corners, native_capture_point_coverage


class NativeCornerMembersMappingTests(unittest.TestCase):
    def fixture(self):
        setup = {'tests': [{'name': name, 'enabled': True} for name in ('A', 'B')],
                 'corners': [{'name': name, 'enabled': True, 'variables': [{'name': 'X', 'value': '1 2'}]}
                             for name in ('Same1', 'Same2')]}
        schedule = {'read_ok': True, 'members_state': 'available',
                    'nominal_enabled': False, 'nominal_tests': {'A': False, 'B': False},
                    'groups': {
                        'Same1': {'enabled': True, 'count': 2, 'disabled_tests': ['B'],
                                  'test_members': {'A': ['opaque_a', 'opaque_b'], 'B': []}},
                        'Same2': {'enabled': True, 'count': 2, 'disabled_tests': [],
                                  'test_members': {'A': ['opaque_c', 'opaque_d'], 'B': ['opaque_c', 'opaque_d']}}}}
        tasks, points = [], []
        for point, label in enumerate(('opaque_a', 'opaque_b', 'opaque_c', 'opaque_d'), 1):
            points.append({'pointID': point, 'corner_name': label})
            for test in (['A'] if point < 3 else ['A', 'B']):
                tasks.append({'point_id': point, 'test': test, 'corner': label,
                              'native_parameters': [{'name': 'X', 'value': 1 if point % 2 else 2}]})
        parsed = {'points': points, 'test_statuses': [{'pointID': point, 'test_name': test}
                  for point in range(1, 5) for test in ('A', 'B')]}
        return setup, tasks, parsed, schedule

    def test_same_values_in_two_groups_use_native_membership_not_spelling(self):
        setup, tasks, parsed, schedule = self.fixture()
        result = validate_capture_corners(setup, tasks, parsed, schedule=schedule)
        self.assertTrue(result['ok'], result)
        self.assertEqual([row['source_corner'] for row in tasks],
                         ['Same1', 'Same1', 'Same2', 'Same2', 'Same2', 'Same2'])
        self.assertTrue(native_capture_point_coverage(tasks, parsed, ['A', 'B'], schedule)['ok'])
        self.assertTrue(all(row['mapping_evidence'] == 'native_test_corner_members'
                            for row in result['test_differences']))
        legacy = copy.deepcopy(schedule)
        legacy['members_state'] = 'unavailable'
        self.assertFalse(validate_capture_corners(setup, tasks, parsed, schedule=legacy)['ok'])

    def test_missing_unexpected_duplicate_or_wrong_value_still_fail(self):
        for change in ('missing', 'unexpected', 'duplicate', 'wrong_value'):
            setup, tasks, parsed, schedule = self.fixture()
            if change == 'missing':
                tasks.pop()
            elif change == 'unexpected':
                tasks[0]['corner'] = 'looks_like_a_saved_name_0'
            elif change == 'duplicate':
                tasks.append(copy.deepcopy(tasks[0]))
            else:
                tasks[0]['native_parameters'][0]['value'] = 99
            with self.subTest(change=change):
                self.assertFalse(validate_capture_corners(setup, tasks, parsed, schedule=schedule)['ok'])
                self.assertTrue(all('source_corner' not in row for row in tasks))

    def test_masked_placeholders_do_not_hide_missing_active_points(self):
        setup, tasks, parsed, schedule = self.fixture()
        # No source_corner aliases are required: membership was read from the
        # private native setup independently of the value qualification.
        self.assertTrue(native_capture_point_coverage(tasks, parsed, ['A', 'B'], schedule)['ok'])
        tasks = [row for row in tasks if row['point_id'] != 3]
        result = native_capture_point_coverage(tasks, parsed, ['A', 'B'], schedule)
        self.assertFalse(result['ok'])
        self.assertEqual(len(result['masked_test_points']), 2)
        self.assertEqual([(row['point_id'], row['test']) for row in result['missing_test_points']],
                         [(3, 'A'), (3, 'B')])
