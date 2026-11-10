import csv
import io
import os
import tempfile
import unittest

from crescendo_engine.exports import (
    candidates_csv_text, export_candidates_csv, write_candidates_csv,
)


def candidate(index):
    return {
        "candidate_id": "g%03d_c%04d" % (index // 100, index),
        "generation": index // 100,
        "index": index,
        "status": "completed" if index % 3 else "failed",
        "failure_class": "" if index % 3 else "simulation",
        "values": {
            "hash_w": 20e-9 + index * 1e-12,
            "hash_mode": "fast,accurate" if index % 2 else 'normal "quoted"',
        },
        "metrics": {
            "feasible": bool(index % 2),
            "objectives": [
                {"name": "Gain, dB", "output_key": "main::gain", "value": 60.0 + index},
                {"name": "GBW", "output_key": "main::gbw", "value": 1e6 + index},
            ],
            "constraints": [
                {"name": "Power", "output_key": "main::power", "value": 100e-6 + index * 1e-9},
            ],
        },
    }


class ExportsTest(unittest.TestCase):
    def test_csv_matches_live_candidate_fields_and_escapes_values(self):
        text = candidates_csv_text(
            [candidate(0), candidate(1)],
            variable_labels={"hash_w": "/XOPA/M1/w", "hash_mode": "/XOPA/M1/mode"},
        )
        rows = list(csv.DictReader(io.StringIO(text)))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["candidate"], "g000_c0000")
        self.assertEqual(rows[0]["generation"], "1")
        self.assertEqual(rows[0]["failure_class"], "simulation")
        self.assertEqual(rows[0]["feasible"], "no")
        self.assertEqual(rows[1]["feasible"], "yes")
        self.assertEqual(rows[0]["objective:Gain, dB"], "60.0")
        self.assertEqual(rows[1]["constraint:Power"], str(100e-6 + 1e-9))
        self.assertEqual(rows[0]["variable:/XOPA/M1/mode"], 'normal "quoted"')
        self.assertEqual(rows[1]["variable:/XOPA/M1/mode"], "fast,accurate")

    def test_failed_candidate_without_metrics_keeps_schema(self):
        completed = candidate(1)
        failed = {
            "candidate_id": "g000_c0002", "generation": 0, "index": 2,
            "status": "timeout", "failure_class": "candidate_timeout",
            "values": {"hash_w": 22e-9},
        }
        rows = list(csv.DictReader(io.StringIO(candidates_csv_text([completed, failed]))))
        self.assertEqual(rows[1]["objective:GBW"], "")
        self.assertEqual(rows[1]["constraint:Power"], "")
        self.assertEqual(rows[1]["status"], "timeout")

    def test_duplicate_metric_and_variable_labels_are_not_overwritten(self):
        row = candidate(2)
        row["metrics"]["objectives"] = [
            {"name": "FRE", "output_key": "vco::FRE", "aggregation": "max", "corners": ["Flow"], "value": 4.9e9},
            {"name": "FRE", "output_key": "vco::FRE", "aggregation": "min", "corners": ["Fhigh"], "value": 5.1e9},
        ]
        text = candidates_csv_text(
            [row],
            variable_labels={"hash_w": "/SCA/INV/M1/w", "hash_mode": "/SCA/INV/M1/w"},
        )
        parsed = list(csv.DictReader(io.StringIO(text)))[0]
        objective_headers = [key for key in parsed if key.startswith("objective:FRE")]
        variable_headers = [key for key in parsed if key.startswith("variable:/SCA/INV/M1/w")]
        self.assertEqual(len(objective_headers), 2)
        self.assertEqual(set(parsed[key] for key in objective_headers), set(("4900000000.0", "5100000000.0")))
        self.assertEqual(len(variable_headers), 2)

    def test_export_is_atomic_and_streams_ten_thousand_candidates(self):
        candidates = [candidate(index) for index in range(10000)]
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "all_candidates.csv")
            summary = export_candidates_csv(candidates, path)
            self.assertEqual(summary["rows"], 10000)
            self.assertEqual(summary["columns"], 11)
            self.assertGreater(summary["bytes"], 500000)
            self.assertFalse(os.path.exists(path + ".tmp"))
            with io.open(path, "r", encoding="utf-8", newline="") as stream:
                count = sum(1 for _row in csv.reader(stream))
            self.assertEqual(count, 10001)

    def test_stream_writer_reports_columns(self):
        stream = io.StringIO()
        summary = write_candidates_csv([candidate(3)], stream)
        self.assertEqual(summary["rows"], 1)
        self.assertIn("objective:GBW", summary["headers"])


if __name__ == "__main__":
    unittest.main()
