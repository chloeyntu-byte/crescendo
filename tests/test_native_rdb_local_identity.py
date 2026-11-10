import hashlib
import os
import sqlite3
import tempfile
import unittest

from crescendo_engine.native_identity import _point_rdb_values, point_variable_checks
from crescendo_engine.private_netlist import validate_capture_corners


class NativeRdbLocalIdentityTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        # Literal URI delimiters must still identify exactly this file.
        self.path = os.path.join(self.directory.name, 'point ?#% history.rdb')
        with sqlite3.connect(self.path) as connection:
            connection.executescript('''
                CREATE TABLE test(testID INTEGER, name TEXT);
                CREATE TABLE parameter(parameterID INTEGER, name TEXT, type TEXT,
                                       testID INTEGER, hasLocal TEXT);
                CREATE TABLE parameterValue(pointID INTEGER, parameterID INTEGER, value NUMERIC);
                CREATE TABLE localParameterValue(pointID INTEGER, parameterID INTEGER,
                                                testID INTEGER, value NUMERIC);
                INSERT INTO test VALUES(1, 'A'), (2, 'B'), (3, 'Disabled');
                INSERT INTO parameter VALUES(8, 'V', 'desVar', '', 'true');
                INSERT INTO parameterValue VALUES(1, 8, 1), (2, 8, 2);
                INSERT INTO localParameterValue VALUES(1, 8, 1, 0), (2, 8, 1, 0.5),
                                                     (1, 8, 3, 99);
            ''')

    def sql(self, text, args=()):
        with sqlite3.connect(self.path) as connection:
            connection.execute(text, args)

    def task(self, test='A', point=1, value=0, expression=None):
        return {'test': test, 'point_id': point, 'corner': '_default',
                'native_parameters': [{'name': 'V', 'value': value,
                                       'expression': str(value) if expression is None else expression}]}

    def read(self, test='A', point=1):
        return _point_rdb_values({'path': self.path}, self.task(test, point))

    def check(self, expected='0', value=0, expression=None, test='A'):
        current = {'name': test, 'variables': [{'name': 'V', 'value': expected}]}
        return point_variable_checks({'global_variables': []}, current, {},
                                     self.task(test=test, value=value, expression=expression), {'path': self.path})

    def test_exact_test_and_point_local_override_shared_value(self):
        for test, point, expected, scope in (
                ('A', 1, 0, 'test_local'), ('A', 2, .5, 'test_local'),
                ('B', 1, 1, 'point_global_no_test_local'),
                ('B', 2, 2, 'point_global_no_test_local')):
            with self.subTest(test=test, point=point):
                values, proof, issues = self.read(test, point)
                self.assertEqual(values, {'V': expected})
                self.assertEqual(issues, [])
                self.assertEqual(proof['V']['scope'], scope)
                self.assertEqual(proof['V']['test_id'], 1 if test == 'A' else 2)
                self.assertEqual(proof['V']['point_id'], point)
                self.assertTrue(proof['V']['local_identity_proven'])

    def test_real_saved_local_zero_is_not_a_native_global_mismatch(self):
        differences, checks = self.check()
        self.assertEqual(differences, [])
        self.assertTrue(checks[0]['ok'])
        self.assertEqual(checks[0]['rdb_value'], 0)
        self.assertEqual(checks[0]['rdb_identity']['scope'], 'test_local')

    def test_enabled_global_does_not_acquire_another_tests_local(self):
        differences, checks = self.check(expected='1', value=1, test='B')
        self.assertEqual(differences, [])
        self.assertEqual(checks[0]['rdb_value'], 1)
        self.assertEqual(checks[0]['rdb_identity']['scope'], 'point_global_no_test_local')

    def test_false_haslocal_ignores_stale_local_rows(self):
        self.sql("UPDATE parameter SET hasLocal='false'")
        values, proof, issues = self.read()
        self.assertEqual(values, {'V': 1})
        self.assertEqual(issues, [])
        self.assertFalse(proof['V']['has_local'])
        self.assertEqual(proof['V']['scope'], 'point_global')

    def test_device_parameters_and_other_tests_metadata_are_not_design_variables(self):
        self.sql("INSERT INTO parameter VALUES(9, 'V', 'deviceParam', '', 'false')")
        self.sql('INSERT INTO parameterValue VALUES(1, 9, 42)')
        self.sql("INSERT INTO parameter VALUES(10, 'V', 'desVar', 3, 'false')")
        self.sql('INSERT INTO parameterValue VALUES(1, 10, 99)')
        self.assertEqual(self.read()[0], {'V': 0})
        self.assertEqual(self.read()[2], [])

    def test_native_corner_and_global_effective_value_is_not_replaced_by_saved_local(self):
        self.sql("UPDATE parameter SET hasLocal='false'")
        setup = {'global_variables': [{'name': 'V', 'value': '1', 'enabled': True}]}
        test = {'name': 'A', 'variables': [{'name': 'V', 'value': '0'}]}
        scope = {'V': {'name': 'V', 'value': '1', 'source': 'corner'}}
        differences, checks = point_variable_checks(setup, test, {}, self.task(value=1),
                                                    {'path': self.path}, effective_scope=scope)
        self.assertEqual(differences, [])
        self.assertEqual(checks[0]['source'], 'corner')
        self.assertEqual(checks[0]['rdb_value'], 1)

    def test_missing_or_duplicate_test_identity_is_a_failure_not_first_test(self):
        for test in ('Missing', None):
            with self.subTest(test=test):
                values, _proof, issues = self.read(test)
                self.assertEqual(values, {})
                self.assertEqual(issues[0]['reason'], 'native_rdb_test_identity_unqualified')
        self.sql("INSERT INTO test VALUES(4, 'A')")
        differences, _checks = self.check()
        self.assertEqual(differences[0]['reason'], 'native_rdb_test_identity_unqualified')

    def test_missing_point_identity_never_selects_another_point(self):
        values, _proof, issues = self.read(point=None)
        self.assertEqual(values, {})
        self.assertEqual(issues[0]['reason'], 'native_rdb_point_identity_unqualified')

    def test_duplicate_local_and_global_rows_are_not_deduplicated(self):
        self.sql('INSERT INTO localParameterValue VALUES(1, 8, 1, 0)')
        values, _proof, issues = self.read()
        self.assertEqual(values, {})
        self.assertEqual(issues[0]['reason'], 'native_rdb_variable_value_ambiguous')
        self.sql('INSERT INTO parameterValue VALUES(1, 8, 1)')
        self.assertEqual(self.read('B')[2][0]['reason'], 'native_rdb_variable_value_ambiguous')

    def test_duplicate_parameter_identity_is_not_chosen_by_name(self):
        self.sql("INSERT INTO parameter VALUES(9, 'V', 'desVar', '', 'false')")
        values, _proof, issues = self.read()
        self.assertEqual(values, {})
        self.assertEqual(issues[0]['reason'], 'native_rdb_parameter_identity_ambiguous')
        self.assertTrue(self.check()[0])

    def test_explicit_local_null_does_not_fallback_to_global(self):
        self.sql('UPDATE localParameterValue SET value=NULL WHERE testID=1 AND pointID=1')
        values, proof, issues = self.read()
        self.assertEqual(values, {})
        self.assertIsNone(proof['V']['raw_value'])
        self.assertEqual(proof['V']['scope'], 'test_local')
        self.assertEqual(issues[0]['reason'], 'native_rdb_variable_value_unavailable')
        differences, _checks = self.check(expected='1', value=1)
        self.assertEqual(differences[0]['reason'], 'native_rdb_variable_value_unavailable')

    def test_explicit_local_bad_numeric_never_qualifies_using_global(self):
        self.sql('UPDATE localParameterValue SET value=? WHERE testID=1 AND pointID=1', ('not-a-number',))
        values, proof, issues = self.read()
        self.assertEqual(values, {})
        self.assertEqual(proof['V']['raw_value'], 'not-a-number')
        self.assertEqual(issues, [])
        differences, checks = self.check(expected='1', value=1)
        self.assertIsNone(checks[0]['rdb_value'])
        self.assertEqual(differences[0]['reason'], 'native_rdb_numeric_value_unavailable')

    def test_non_numeric_native_path_is_preserved_without_fake_numeric_value(self):
        path = '"/models/my PWL.dat"'
        self.sql('UPDATE localParameterValue SET value=? WHERE testID=1 AND pointID=1', (path,))
        differences, checks = self.check(expected=path, value=None, expression=path)
        self.assertEqual(differences, [])
        self.assertTrue(checks[0]['ok'])
        self.assertIsNone(checks[0]['rdb_value'])
        self.assertEqual(checks[0]['evaluation'], 'saved_rdb_netlist_expression_preserved_numeric_unavailable')

    def test_non_numeric_path_internal_whitespace_is_significant(self):
        self.sql('UPDATE localParameterValue SET value=? WHERE testID=1 AND pointID=1', ('"/my  PWL.dat"',))
        differences, checks = self.check(expected='"/my PWL.dat"', value=None, expression='"/my PWL.dat"')
        self.assertEqual(differences, [])
        self.assertIsNone(checks[0]['ok'])
        self.assertEqual(checks[0]['evaluation'], 'string_value_deferred_to_simulator')
        self.assertEqual(checks[0]['rdb_literal'], '/my  PWL.dat')
        self.assertEqual(checks[0]['literal_value'], '/my PWL.dat')

    def test_native_evaluated_path_preserves_visible_numeric_unknown(self):
        path = '"/my PWL.dat"'
        self.sql('UPDATE localParameterValue SET value=? WHERE testID=1 AND pointID=1', (path,))
        differences, checks = self.check(expected='strcat("/my" " PWL.dat")', value=None, expression=path)
        self.assertEqual(differences, [])
        self.assertIsNone(checks[0]['ok'])
        self.assertIsNone(checks[0]['rdb_value'])
        self.assertEqual(checks[0]['evaluation'], 'native_rdb_netlist_expression_matches_saved_evaluation_unavailable')

    def test_arrays_blobs_and_missing_value_do_not_become_zero(self):
        for raw in ('[1 2]', '1 2', sqlite3.Binary(b'\x00\x01')):
            with self.subTest(raw=raw):
                self.sql('UPDATE localParameterValue SET value=? WHERE testID=1 AND pointID=1', (raw,))
                values, _proof, _issues = self.read()
                self.assertEqual(values, {})
                self.assertTrue(self.check()[0])
        self.sql('DELETE FROM localParameterValue WHERE testID=1 AND pointID=1')
        self.sql('DELETE FROM parameterValue WHERE pointID=1')
        self.assertEqual(self.read()[2][0]['reason'], 'native_rdb_variable_value_missing')

    def test_unknown_haslocal_is_not_false(self):
        for raw in (None, '', 'unknown'):
            with self.subTest(raw=raw):
                self.sql('UPDATE parameter SET hasLocal=?', (raw,))
                values, _proof, issues = self.read()
                self.assertEqual(values, {})
                self.assertEqual(issues[0]['reason'], 'native_rdb_local_flag_unknown')
                self.assertTrue(self.check()[0])

    def test_missing_local_table_blocks_only_when_metadata_requires_it(self):
        self.sql('DROP TABLE localParameterValue')
        differences, _checks = self.check()
        self.assertEqual(differences[0]['reason'], 'native_rdb_variable_schema_unavailable')
        self.sql("UPDATE parameter SET hasLocal='false'")
        values, _proof, issues = self.read()
        self.assertEqual(values, {'V': 1})
        self.assertEqual(issues, [])

    def test_missing_haslocal_field_is_diagnostic_not_default_false(self):
        self.sql('ALTER TABLE parameter RENAME TO old_parameter')
        self.sql('CREATE TABLE parameter(parameterID INTEGER, name TEXT, type TEXT, testID INTEGER)')
        differences, _checks = self.check()
        self.assertEqual(differences[0]['reason'], 'native_rdb_variable_schema_unavailable')
        self.assertEqual(differences[0]['missing_columns'], ['hasLocal'])

    def test_existing_unreadable_database_never_falls_back_to_flat_values(self):
        missing = os.path.join(self.directory.name, 'missing.rdb')
        values, _proof, issues = _point_rdb_values({'path': missing, 'parameters': [
            {'pointID': 1, 'name': 'V', 'value': 0}]}, self.task())
        self.assertEqual(values, {})
        self.assertFalse(os.path.exists(missing))
        self.assertEqual(issues[0]['reason'], 'native_rdb_variable_read_failed')

    def test_read_only_filename_and_database_contents_are_unchanged(self):
        with open(self.path, 'rb') as stream:
            before = hashlib.sha256(stream.read()).hexdigest()
        entries = sorted(os.listdir(self.directory.name))
        self.assertEqual(self.read()[0], {'V': 0})
        with open(self.path, 'rb') as stream:
            self.assertEqual(hashlib.sha256(stream.read()).hexdigest(), before)
        self.assertEqual(sorted(os.listdir(self.directory.name)), entries)

    def test_no_native_path_legacy_fixture_does_not_claim_local_proof(self):
        values, proof, issues = _point_rdb_values({'parameters': [
            {'pointID': 1, 'name': 'V', 'value': 1}, {'pointID': 2, 'name': 'V', 'value': 99}]}, self.task())
        self.assertEqual(values, {'V': 1})
        self.assertEqual(issues, [])
        self.assertFalse(proof['V']['local_identity_proven'])
        self.assertEqual(proof['V']['scope'], 'unproven_flat_parameters')

    def test_rdb_identity_failure_prevents_corner_qualification(self):
        self.sql('UPDATE localParameterValue SET value=NULL WHERE testID=1 AND pointID=1')
        setup = {'tests': [{'name': 'A', 'enabled': True,
                            'variables': [{'name': 'V', 'value': '0'}]}], 'corners': []}
        snapshot = {'ok': True, 'saved_source': {'corner_read_ok': True},
                    'private_setup': {'corner_read_ok': True}}
        result = validate_capture_corners(setup, [self.task()], {'path': self.path}, snapshot)
        self.assertFalse(result['ok'])
        self.assertEqual(result['test_differences'][0]['variable_mismatches'][0]['reason'],
                         'native_rdb_variable_value_unavailable')


if __name__ == '__main__':
    unittest.main()
