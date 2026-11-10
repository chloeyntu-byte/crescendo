import unittest
from crescendo_engine.private_netlist import qualify_symbolic_netlist, PrivateNetlistUnsupported
from crescendo_engine.retention import lightweight_candidate_record


class SameLibraryIdentityTests(unittest.TestCase):
    def text(self, first, second):
        return ("// Library name: L\n// Cell name: reused\n// View name: sch_a\n"
                "subckt reused_a a b\nM0 (a b b b) nch w=%s\nends reused_a\n"
                "// Library name: L\n// Cell name: reused\n// View name: sch_b\n"
                "subckt reused_b a b\nM0 (a b b b) nch w=%s\nends reused_b\n") % (first, second)

    def test_same_cell_distinct_views_match_correct_occurrence(self):
        text = self.text("CRESCENDO_NL_p", "CRESCENDO_NL_q")
        refs = [{"name": "native", "values": {"p": 1e-6, "q": 2e-6}, "text": self.text("1u", "2u")}]
        symbols = {"p": "CRESCENDO_NL_p", "q": "CRESCENDO_NL_q"}
        targets = {"p": ("L", "reused", "sch_a", "M0"), "q": ("L", "reused", "sch_b", "M0")}
        self.assertTrue(qualify_symbolic_netlist(text, refs, symbols, targets)["ok"])
        targets["p"] = ("L", "reused", "sch_b", "M0")
        with self.assertRaisesRegex(PrivateNetlistUnsupported, "escaped"):
            qualify_symbolic_netlist(text, refs, symbols, targets)

    def test_durable_raw_values_and_identity_survive_candidate_cleanup(self):
        row = {"candidate_id": "g000_c0000", "status": "completed", "execution_backend": "private_netlist",
               "values": {"p": 3.01e-6}, "completion_evidence": {"ok": True, "parameters": {"p": 3.01e-6},
               "template_sha256": "qualified-template", "huge_raw_psf": "unneeded"}}
        record = lightweight_candidate_record(row)
        self.assertEqual(record["completion"]["parameters"], {"p": 3.01e-6})
        self.assertEqual(record["completion"]["template_sha256"], "qualified-template")
        self.assertNotIn("huge_raw_psf", record["completion"])

    def test_native_derived_instance_snapshot_survives_cleanup(self):
        row = {"parameter_application": {"ok": True, "parameters": [], "export_snapshot":
               {"instances": [{"instance": "M0", "properties": {"derived": {"value": "6u"}}}]}}}
        self.assertEqual(lightweight_candidate_record(row)["parameter_application"]["export_snapshot"],
                         row["parameter_application"]["export_snapshot"])
