import csv
import io
import unittest

from crescendo_engine.exports import candidates_csv_text
from crescendo_engine.gui import _candidate_diagnostic_text


class UnifiedExportTests(unittest.TestCase):
    def test_csv_keeps_optimizer_coordinate_and_separate_proven_actual_value(self):
        rows = [{"execution_backend": "maestro_netlist", "status": "completed",
                 "values": {"w": 2e-6}, "completion": {"ok": True, "parameters": {"w": 1.9e-6}}},
                {"execution_backend": "maestro_netlist", "status": "failed", "values": {"w": 3e-6}}]
        parsed = list(csv.DictReader(io.StringIO(candidates_csv_text(rows))))
        self.assertEqual(parsed[0]["variable:w"], "2e-06")
        self.assertEqual(parsed[0]["actual_variable:w"], "1.9e-06")
        self.assertEqual(parsed[1]["actual_variable:w"], "")

    def test_alps_candidate_summary_uses_actual_simulator(self):
        text = _candidate_diagnostic_text({"execution_simulator": "alps", "candidate_status": "completed", "spectre_state": "started"})
        self.assertIn("ALPS: Started", text)
        self.assertNotIn("Spectre:", text)
