from __future__ import print_function

import os
import sqlite3
import tempfile
import unittest

from crescendo_engine.rdb import aggregate_output, normalize_corner_name, parse_rdb


class RdbParserTest(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp(suffix=".rdb")
        os.close(handle)
        connection = sqlite3.connect(self.path)
        connection.executescript('''
            CREATE TABLE test(testID INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE corner(cornerID INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE point(pointID INTEGER PRIMARY KEY, designPointNumber INTEGER, cornerID INTEGER);
            CREATE TABLE result(resultID INTEGER PRIMARY KEY, name TEXT, testID INTEGER, type TEXT, expression TEXT, resultOrder INTEGER);
            CREATE TABLE error(errorID INTEGER PRIMARY KEY, message TEXT);
            CREATE TABLE resultValue(pointID INTEGER, resultID INTEGER, value BLOB, errorID INTEGER);
            CREATE TABLE parameter(parameterID INTEGER PRIMARY KEY, name TEXT, defaultValue TEXT, type TEXT);
            CREATE TABLE parameterValue(pointID INTEGER, parameterID INTEGER, value NUMERIC);
            CREATE TABLE testStatus(pointID INTEGER, testID INTEGER, statusCode INTEGER, startTime TEXT, stopTime TEXT, errorID INTEGER);
            INSERT INTO test VALUES(1, 'main');
            INSERT INTO corner VALUES(1, 'tt');
            INSERT INTO corner VALUES(2, 'ff');
            INSERT INTO point VALUES(1, 1, 1);
            INSERT INTO point VALUES(2, 1, 2);
            INSERT INTO result VALUES(1, 'gain', 1, '0', 'gain_expr', 1);
            INSERT INTO result VALUES(2, 'wave', 1, '0', 'v(out)', 2);
            INSERT INTO resultValue VALUES(1, 1, 10.0, NULL);
            INSERT INTO resultValue VALUES(2, 1, 12.0, NULL);
            INSERT INTO resultValue VALUES(1, 2, 'wave', NULL);
            INSERT INTO resultValue VALUES(2, 2, 'wave', NULL);
            INSERT INTO parameter VALUES(1, 'BIAS', '1m', 'desVar');
            INSERT INTO parameterValue VALUES(1, 1, 0.001);
            INSERT INTO testStatus VALUES(1, 1, 3, '1', '2', NULL);
        ''')
        connection.commit()
        connection.close()

    def tearDown(self):
        os.unlink(self.path)

    def test_extracts_scalar_outputs_and_aggregations(self):
        parsed = parse_rdb(self.path)
        self.assertEqual(aggregate_output(parsed, "main::gain", "min"), 10.0)
        self.assertEqual(aggregate_output(parsed, "main::gain", "max"), 12.0)
        self.assertEqual(aggregate_output(parsed, "main::gain", "mean"), 11.0)
        self.assertFalse(parsed["output_map"]["main::wave"]["scalar"])
        self.assertEqual(parsed["parameters"][0]["name"], "BIAS")

    def test_aggregate_output_filters_selected_corners(self):
        parsed = {"output_map": {"main::frequency": {
            "values": [
                {"corner": "Flow", "numeric": 4.8e9},
                {"corner": "Fhigh", "numeric": 5.3e9},
            ],
            "summary": {"mean": 5.05e9},
        }}}
        self.assertEqual(aggregate_output(parsed, "main::frequency", "max", ["Flow"]), 4.8e9)
        self.assertEqual(aggregate_output(parsed, "main::frequency", "min", ["Fhigh"]), 5.3e9)
        self.assertEqual(aggregate_output(parsed, "main::frequency", "mean", ["Flow", "Fhigh"]), 5.05e9)
        with self.assertRaises(ValueError):
            aggregate_output(parsed, "main::frequency", "mean", ["Missing"])

    def test_normalizes_empty_rdb_corner_to_default(self):
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE corner SET name='' WHERE cornerID=1")
        connection.commit()
        connection.close()

        parsed = parse_rdb(self.path)
        self.assertEqual(parsed["points"][0]["corner_name"], "_default")
        self.assertEqual(parsed["parameters"][0]["corner_name"], "_default")
        self.assertEqual(parsed["output_map"]["main::gain"]["values"][0]["corner"], "_default")
        self.assertEqual(aggregate_output(parsed, "main::gain", "mean", ["_default"]), 10.0)

    def test_corner_normalization_preserves_explicit_names(self):
        self.assertEqual(normalize_corner_name(None), "_default")
        self.assertEqual(normalize_corner_name("  "), "_default")
        self.assertEqual(normalize_corner_name(" Flow "), "Flow")


if __name__ == "__main__":
    unittest.main()
