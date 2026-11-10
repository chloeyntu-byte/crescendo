from __future__ import print_function

import hashlib
import os
import sqlite3
import tempfile
import unittest

from crescendo_engine.rdb import aggregate_output, parse_rdb


class RdbSpecialPathTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-rdb-paths-")

    def tearDown(self):
        self.directory.cleanup()

    def _database(self, name, value):
        path = os.path.join(self.directory.name, name)
        connection = sqlite3.connect(path)
        try:
            connection.executescript('''
                CREATE TABLE test(testID INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE corner(cornerID INTEGER PRIMARY KEY, name TEXT);
                CREATE TABLE point(pointID INTEGER PRIMARY KEY, designPointNumber INTEGER, cornerID INTEGER);
                CREATE TABLE result(resultID INTEGER PRIMARY KEY, name TEXT, testID INTEGER, type TEXT, expression TEXT, resultOrder INTEGER);
                CREATE TABLE error(errorID INTEGER PRIMARY KEY, message TEXT);
                CREATE TABLE resultValue(pointID INTEGER, resultID INTEGER, value BLOB, errorID INTEGER);
                INSERT INTO test VALUES(1, 'VCO');
                INSERT INTO corner VALUES(1, 'Flow');
                INSERT INTO point VALUES(1, 1, 1);
                INSERT INTO result VALUES(1, 'frequency', 1, '0', 'frequency_expr', 1);
            ''')
            connection.execute("INSERT INTO resultValue VALUES(1, 1, ?, NULL)", (value,))
            connection.commit()
        finally:
            connection.close()
        return path

    @staticmethod
    def _digest(path):
        with open(path, "rb") as stream:
            return hashlib.sha256(stream.read()).hexdigest()

    def _assert_exact_readonly_path(self, name, decoy_name):
        decoy = self._database(decoy_name, 1.0)
        selected = self._database(name, 99.0)
        before = dict((path, self._digest(path)) for path in (decoy, selected))
        files_before = sorted(os.listdir(self.directory.name))

        parsed = parse_rdb(selected)

        self.assertEqual(parsed["path"], selected)
        self.assertEqual(aggregate_output(parsed, "VCO::frequency", "min"), 99.0)
        self.assertEqual(dict((path, self._digest(path)) for path in before), before)
        self.assertEqual(sorted(os.listdir(self.directory.name)), files_before)

    def test_literal_hash_history_opens_selected_file_instead_of_prefix(self):
        self._assert_exact_readonly_path("history.rdb#selected.rdb", "history.rdb")

    def test_literal_question_mark_history_opens_selected_file_instead_of_prefix(self):
        self._assert_exact_readonly_path("history.rdb?selected.rdb", "history.rdb")

    def test_literal_percent_triplet_history_is_not_decoded_into_another_name(self):
        self._assert_exact_readonly_path("history%23selected.rdb", "history#selected.rdb")


if __name__ == "__main__":
    unittest.main()
