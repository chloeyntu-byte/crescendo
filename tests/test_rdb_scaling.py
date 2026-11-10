from __future__ import print_function

import hashlib
import os
import sqlite3
import tempfile
import threading
import tracemalloc
import unittest
from unittest import mock

from crescendo_engine.rdb import RdbReadCancelled, parse_rdb, parse_rdb_summary


class RdbSummaryScalingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="crescendo-history-summary-")
        self.path = os.path.join(self.directory.name, "selected.rdb")
        self.connection = sqlite3.connect(self.path)
        self.connection.executescript('''
            CREATE TABLE test(testID INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE corner(cornerID INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE point(pointID INTEGER PRIMARY KEY, designPointNumber INTEGER, cornerID INTEGER);
            CREATE TABLE result(resultID INTEGER PRIMARY KEY, name TEXT, testID INTEGER, type TEXT, expression TEXT, resultOrder INTEGER);
            CREATE TABLE error(errorID INTEGER PRIMARY KEY, message TEXT);
            CREATE TABLE resultValue(pointID INTEGER, resultID INTEGER, value BLOB, errorID INTEGER);
            CREATE TABLE parameter(parameterID INTEGER PRIMARY KEY, name TEXT, defaultValue TEXT, type TEXT);
            CREATE TABLE parameterValue(pointID INTEGER, parameterID INTEGER, value NUMERIC);
            CREATE TABLE testStatus(pointID INTEGER, testID INTEGER, statusCode INTEGER, startTime TEXT, stopTime TEXT, errorID INTEGER);
            INSERT INTO test VALUES(1, 'VCO');
            INSERT INTO test VALUES(2, 'second');
            INSERT INTO corner VALUES(1, 'tt');
            INSERT INTO corner VALUES(2, 'ff');
            INSERT INTO point VALUES(1, 1, 1);
            INSERT INTO point VALUES(2, 1, 2);
            INSERT INTO result VALUES(1, 'frequency', 1, '0', 'freq()', 1);
            INSERT INTO result VALUES(2, 'wave', 1, '0', 'v(out)', 2);
            INSERT INTO result VALUES(3, 'not_run', 2, '0', 'gain()', 1);
            INSERT INTO resultValue VALUES(1, 1, 1234.5, NULL);
            INSERT INTO resultValue VALUES(2, 1, 2345.6, NULL);
            INSERT INTO resultValue VALUES(1, 2, 'wave', NULL);
        ''')
        self.connection.commit()

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    @staticmethod
    def digest(path):
        with open(path, "rb") as stream:
            return hashlib.sha256(stream.read()).hexdigest()

    def test_summary_matches_full_reader_counts_without_values(self):
        summary = parse_rdb_summary(self.path)
        full = parse_rdb(self.path)
        self.assertEqual(summary["point_count"], len(full["points"]))
        self.assertEqual(summary["output_count"], len(full["outputs"]))
        self.assertEqual(summary["test_count"], 2)
        self.assertEqual(summary["corner_count"], 2)
        self.assertEqual(summary["schema_version"], "crescendo.rdb.summary.v1")
        self.assertTrue(summary["metadata_only"])
        self.assertEqual(summary["path"], self.path)
        for field in ("values", "outputs", "output_map", "parameters", "test_statuses", "points"):
            self.assertNotIn(field, summary)
        # A summary must not change the complete numeric reader's output.
        self.assertEqual(full["output_map"]["VCO::frequency"]["summary"]["mean"], 1790.05)
        self.assertEqual(len(full["output_map"]["VCO::frequency"]["values"]), 2)

    def test_duplicate_definitions_use_full_reader_output_identity(self):
        self.connection.execute("INSERT INTO result VALUES(4, 'frequency', 1, '0', 'freq()', 3)")
        self.connection.commit()
        self.assertEqual(parse_rdb_summary(self.path)["output_count"], len(parse_rdb(self.path)["outputs"]))

    def test_summary_does_not_query_sweep_value_tables(self):
        statements = []
        connect = sqlite3.connect

        def traced_connect(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(lambda statement: statements.append(statement))
            return connection

        with mock.patch("crescendo_engine.rdb.sqlite3.connect", side_effect=traced_connect):
            parse_rdb_summary(self.path)
        for statement in statements:
            text = statement.lower()
            self.assertNotIn("resultvalue", text)
            self.assertNotIn("parametervalue", text)
            self.assertNotIn("teststatus", text)
            self.assertNotIn("order by", text)

    def test_large_sweep_has_bounded_python_memory(self):
        self.connection.executemany(
            "INSERT INTO point VALUES(?, ?, 1)",
            ((number, number) for number in range(3, 30003)),
        )
        self.connection.executemany(
            "INSERT INTO resultValue VALUES(?, 1, ?, NULL)",
            ((number, number * 1.23) for number in range(3, 30003)),
        )
        self.connection.commit()
        tracemalloc.start()
        try:
            summary = parse_rdb_summary(self.path)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        self.assertEqual(summary["point_count"], 30002)
        self.assertLess(peak, 1024 * 1024)

    def test_cancel_before_open_does_not_create_missing_file(self):
        event = threading.Event()
        event.set()
        missing = os.path.join(self.directory.name, "missing.rdb")
        with self.assertRaises(RdbReadCancelled):
            parse_rdb_summary(missing, cancel_event=event)
        self.assertFalse(os.path.exists(missing))

    def test_cancel_during_sqlite_query_interrupts_vm_and_closes_connection(self):
        self.connection.executemany(
            "INSERT INTO point VALUES(?, ?, 1)",
            ((number, number) for number in range(3, 1003)),
        )
        self.connection.commit()
        event = threading.Event()
        connect = sqlite3.connect
        closed = []

        class CancelConnection(sqlite3.Connection):
            def set_progress_handler(self, callback, instruction_count):
                def progress():
                    event.set()
                    return callback()
                sqlite3.Connection.set_progress_handler(self, progress, instruction_count)

            def execute(self, statement, *args, **kwargs):
                if "SELECT t.name, r.name" in statement:
                    statement = "SELECT SUM(a.pointID*b.pointID) FROM point a CROSS JOIN point b"
                return sqlite3.Connection.execute(self, statement, *args, **kwargs)

            def close(self):
                closed.append(True)
                return sqlite3.Connection.close(self)

        def cancellable_connect(*args, **kwargs):
            kwargs["factory"] = CancelConnection
            return connect(*args, **kwargs)

        with mock.patch("crescendo_engine.rdb.sqlite3.connect", side_effect=cancellable_connect):
            with self.assertRaises(RdbReadCancelled):
                parse_rdb_summary(self.path, cancel_event=event)
        self.assertTrue(event.is_set())
        self.assertEqual(closed, [True])

    def test_missing_metadata_table_is_reported_and_not_replaced_with_zero(self):
        self.connection.execute("DROP TABLE point")
        self.connection.commit()
        with self.assertRaisesRegex(ValueError, "no point table"):
            parse_rdb_summary(self.path)

    def test_schema_error_is_not_reported_as_cancellation(self):
        self.connection.execute("ALTER TABLE result RENAME TO old_result")
        self.connection.execute("CREATE TABLE result(resultID INTEGER PRIMARY KEY)")
        self.connection.commit()
        with self.assertRaises(sqlite3.OperationalError):
            parse_rdb_summary(self.path, cancel_event=threading.Event())

    def test_optional_corner_table_is_not_needed_for_metadata_counts(self):
        self.connection.execute("DROP TABLE corner")
        self.connection.commit()
        self.assertEqual(parse_rdb_summary(self.path)["corner_count"], 0)

    def test_exact_filename_readonly_source_and_directory_unchanged(self):
        selected = os.path.join(self.directory.name, "history#selected?scan%23.rdb")
        self.connection.close()
        os.rename(self.path, selected)
        self.path = selected
        self.connection = sqlite3.connect(selected)
        before_hash = self.digest(selected)
        before_names = sorted(os.listdir(self.directory.name))
        summary = parse_rdb_summary(selected)
        self.assertEqual(summary["path"], selected)
        self.assertEqual(summary["output_count"], 3)
        self.assertEqual(self.digest(selected), before_hash)
        self.assertEqual(sorted(os.listdir(self.directory.name)), before_names)


if __name__ == "__main__":
    unittest.main()
