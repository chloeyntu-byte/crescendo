from __future__ import print_function

import os
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PROBE_ROOT = os.path.join(
    PROJECT_ROOT, "runtime", "experiments", "whole_run_acceptance",
)
REQUIRED_PROBE_FILES = (
    "common.py", "fault_probes.py", "opa_icrp_gap_probe.py",
    "fixed_overwrite_session_ab_probe.py",
)
missing_probe_files = [
    name for name in REQUIRED_PROBE_FILES
    if not os.path.isfile(os.path.join(PROBE_ROOT, name))
]
if missing_probe_files:
    raise unittest.SkipTest(
        "optional historical fixed-overwrite probe is not present in the portable source tree"
    )
if PROBE_ROOT not in sys.path:
    sys.path.insert(0, PROBE_ROOT)

import fixed_overwrite_session_ab_probe as probe


class FixedOverwriteSessionAbProbeTests(unittest.TestCase):
    def test_path_guard_rejects_prefix_collision(self):
        self.assertTrue(probe._path_is_under("/tmp/run/case", "/tmp/run"))
        self.assertFalse(probe._path_is_under("/tmp/run-other", "/tmp/run"))

    def test_normalized_netlist_payload_removes_only_known_roots(self):
        left = probe._normalized_netlist_payload(
            b'include "/tmp/run-a/workspace/model.scs"\nvalue=0.53\n',
            (("/tmp/run-a/workspace", "<WORKSPACE>"),),
        )
        right = probe._normalized_netlist_payload(
            b'include "/tmp/run-b/workspace/model.scs"\nvalue=0.53\n',
            (("/tmp/run-b/workspace", "<WORKSPACE>"),),
        )
        self.assertEqual(left, right)
        self.assertIn(b"value=0.53", left)

    def test_netlist_evidence_is_path_independent_and_keeps_raw_hash(self):
        class Worker(object):
            pass

        with tempfile.TemporaryDirectory() as root:
            workspace = os.path.join(root, "workspace")
            netlist = os.path.join(
                workspace, "simulation", "HH", "TB", "maestro", "results",
                "maestro", "Interactive.6", "3", "OPA_AC", "netlist", "netlist",
            )
            os.makedirs(os.path.dirname(netlist))
            with open(netlist, "wb") as stream:
                stream.write(("include \"%s/model.scs\"\ndc=530m\n" % workspace).encode("utf-8"))
            worker = Worker()
            worker.workspace = {"root": workspace}
            row = {
                "status": "completed",
                "parameter_application": {"netlists": [netlist]},
            }
            evidence = probe._netlist_evidence(row, worker, root)
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]["relative_path"], "3/OPA_AC/netlist/netlist")
        self.assertEqual(len(evidence[0]["raw_sha256"]), 64)
        self.assertEqual(len(evidence[0]["normalized_sha256"]), 64)

    def test_application_evidence_accepts_spice_unit_equivalence(self):
        row = {
            "parameter_application": {
                "ok": True,
                "parameters": [{
                    "parameter_id": "vdc-id",
                    "property": "vdc",
                    "requested": "0.53",
                    "schematic_value": "530m",
                    "cdf_value": "530m",
                    "persisted_schematic_value": "530m",
                    "persisted_cdf_value": "530m",
                    "callback_status": "not_defined",
                    "effective_netlist": [{
                        "mode": "direct_alias", "parameter": "dc",
                        "value": "530m", "statement": "V0 (PLUS 0) vsource dc=530m",
                    }],
                }],
            },
        }
        evidence = probe._application_evidence(row, "vdc-id", 0.53)
        self.assertTrue(evidence["ok"])
        self.assertEqual(len(evidence["semantic_sha256"]), 64)

    def test_comparison_matches_same_semantics_in_either_run_order(self):
        values = list(probe.DEFAULT_VALUES)

        def candidate(index):
            return {
                "candidate_id": "g000_c%04d" % index,
                "values": {"vdc-id": values[index]},
                "application": {"signature": [{"requested": str(values[index])}]},
                "netlists": [{
                    "relative_path": "3/OPA_AC/netlist/netlist",
                    "normalized_sha256": "a" * 64,
                }],
                "metrics": {
                    "values": {"objectives/gain": 10.0 + index},
                    "feasible": True,
                },
            }

        single = {
            "case": "single", "ok": True, "elapsed_seconds": 10.0,
            "rows": [candidate(index) for index in range(len(values))],
        }
        restart = {
            "case": "restart4", "ok": True, "elapsed_seconds": 12.0,
            "rows": [candidate(index) for index in range(len(values))],
        }
        forward = probe._comparison([single, restart], values)
        reverse = probe._comparison([restart, single], values)
        self.assertTrue(forward["ok"])
        self.assertEqual(forward, reverse)
        self.assertAlmostEqual(forward["restart4_over_single_ratio"], 1.2)

    def test_comparison_rejects_effective_netlist_difference(self):
        values = [0.53]
        base = {
            "candidate_id": "g000_c0000",
            "values": {"vdc-id": 0.53},
            "application": {"signature": [{"requested": "0.53"}]},
            "netlists": [{"relative_path": "point/netlist", "normalized_sha256": "a" * 64}],
            "metrics": {"values": {"objectives/gain": 10.0}},
        }
        changed = dict(base)
        changed["netlists"] = [{
            "relative_path": "point/netlist", "normalized_sha256": "b" * 64,
        }]
        result = probe._comparison([
            {"case": "single", "ok": True, "elapsed_seconds": 10.0, "rows": [base]},
            {"case": "restart4", "ok": True, "elapsed_seconds": 12.0, "rows": [changed]},
        ], values)
        self.assertFalse(result["ok"])
        self.assertFalse(result["rows"][0]["netlists_equal"])

    def test_comparison_ignores_duplicate_psf_alias_only(self):
        values = [0.53]
        base = {
            "candidate_id": "g000_c0000",
            "values": {"vdc-id": 0.53},
            "application": {"signature": [{"requested": "0.53"}]},
            "netlists": [
                {
                    "relative_path": "%d/OPA_AC/netlist/netlist" % corner,
                    "normalized_sha256": "a" * 64,
                }
                for corner in (1, 2, 3)
            ],
            "metrics": {"values": {"objectives/gain": 10.0}},
        }
        with_alias = dict(base)
        with_alias["netlists"] = list(base["netlists"]) + [{
            "relative_path": "psf/OPA_AC/netlist/netlist",
            "normalized_sha256": "a" * 64,
        }]
        result = probe._comparison([
            {"case": "single", "ok": True, "elapsed_seconds": 10.0, "rows": [base]},
            {"case": "restart4", "ok": True, "elapsed_seconds": 12.0, "rows": [with_alias]},
        ], values)
        self.assertTrue(result["ok"])
        self.assertTrue(result["rows"][0]["netlists_equal"])


if __name__ == "__main__":
    unittest.main()
