from __future__ import print_function

import copy
import hashlib
import json
import os
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine.private_netlist import (
    PrivateNetlistError, PrivateNetlistUnsupported, _assignments,
    _candidate_argv, _expression_value, _logical_statements, _manifest_digest, _qualify_setup_capture,
    _without_symbol_declarations, materialize_candidate, parameter_symbols,
    qualify_symbolic_netlist, validate_manifest, reconcile_preparation, prepare_private_netlist,
    top_level_native_parameters, resolve_native_parameters,
)


def digest(path):
    with open(path, "rb") as stream:
        return hashlib.sha256(stream.read()).hexdigest()


class PrivateNetlistMappingTest(unittest.TestCase):
    def netlist(self, width="CRESCENDO_NL_width", finger="CRESCENDO_NL_finger", area=None):
        if area is None:
            area = "(%s * %s * 0.2u)" % (width, finger)
        return '''simulator lang=spectre
global 0
parameters CRESCENDO_NL_width=2u CRESCENDO_NL_finger=4 VDD=1.2
include "/kit/native.scs" section=tt
// Library name: PRIVATE
// Cell name: branch
// View name: schematic
subckt branch D G S B
M0 (D G S B) native_rf w=%s l=0.2u nf=%s ad=%s m=1
ends branch
// Library name: SOURCE
// Cell name: root
X0 (d g 0 0) branch
R0 (d 0) resistor r=1k
tran tran stop=1n
''' % (width, finger, area)

    def references(self):
        return [
            {"name": "seed", "values": {"p": 2e-6, "q": 4},
             "text": self.netlist("2u", "4", "1.6p")},
            {"name": "probe", "values": {"p": 3e-6, "q": 6},
             "text": self.netlist("3u", "6", "3.6p")},
        ]

    def test_native_alias_and_derived_formula_are_verified_together(self):
        result = qualify_symbolic_netlist(self.netlist(), self.references(),
            {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"},
            {"p": ("PRIVATE", "branch", "M0"), "q": ("PRIVATE", "branch", "M0")})
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["references"]), 2)
        self.assertEqual({row["property"] for row in result["symbols"]["CRESCENDO_NL_width"]}, {"w", "ad"})
        self.assertFalse(result["range_validation_required"])

    def test_stale_derived_constant_is_rejected_even_when_seed_matches(self):
        with self.assertRaisesRegex(PrivateNetlistUnsupported, "derived parameter"):
            qualify_symbolic_netlist(self.netlist(area="1.6p"), self.references(),
                                    {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"})

    def test_model_or_topology_selection_is_rejected(self):
        reference = self.references()
        reference[1]["text"] = reference[1]["text"].replace("native_rf", "native_other_bin")
        with self.assertRaisesRegex(PrivateNetlistUnsupported, "model/topology"):
            qualify_symbolic_netlist(self.netlist(), reference, {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"})

    def test_missing_parameter_does_not_silently_run_seed(self):
        with self.assertRaisesRegex(PrivateNetlistUnsupported, "does not use"):
            qualify_symbolic_netlist(self.netlist("2u"), self.references(),
                                    {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"})

    def test_symbol_in_another_reused_occurrence_is_rejected(self):
        source = self.netlist().replace("R0 (d 0) resistor r=1k", "R0 (d 0) resistor r=CRESCENDO_NL_width")
        with self.assertRaisesRegex(PrivateNetlistUnsupported, "escaped"):
            qualify_symbolic_netlist(source, self.references(),
                {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"}, {"p": ("PRIVATE", "branch", "M0")})

    def test_balanced_expressions_and_both_native_continuations(self):
        text = '''simulator lang=spectre
// Library name: P
// Cell name: B
M0 (d g s b) mos w=(W * (1 + X)) \\
  ad=(W * 2u)
+ nf=N
'''
        statement = _logical_statements(text)[-1][2]
        fields = dict((name, value) for name, value, _a, _b in _assignments(statement))
        self.assertEqual(fields, {"w": "(W * (1 + X))", "ad": "(W * 2u)", "nf": "N"})

    def test_qualification_expression_reader_does_not_execute_code(self):
        with self.assertRaises(ValueError):
            _expression_value('__import__("os").system("false")', {})
        self.assertAlmostEqual(_expression_value("sqrt(W * W) + 1u", {"W": 2e-6}), 3e-6)

    def test_effective_native_parameters_are_local_to_test_and_top_scope(self):
        text = '''simulator lang=spectre
parameters VDD=1.8 CRESCENDO_NL_p=2 W=(CRESCENDO_NL_p * 2u) unknown=vendorValue
subckt inner D S
parameters VDD=99 localOnly=100
ends inner
parameters IB=600u
'''
        rows = top_level_native_parameters(text)
        values = dict((row["name"], row["value"]) for row in rows)
        self.assertEqual(values["VDD"], 1.8)
        self.assertAlmostEqual(values["IB"], 600e-6)
        self.assertNotIn("localOnly", values)
        self.assertIsNone(values["unknown"])
        changed = resolve_native_parameters(rows, {"CRESCENDO_NL_p": 3})
        self.assertAlmostEqual(next(row["value"] for row in changed if row["name"] == "W"), 6e-6)

    def test_generated_symbols_are_stable_and_occurrence_specific(self):
        rows = [{"parameter_id": "first/width"}, {"parameter_id": "second/width"}]
        first = parameter_symbols(rows)
        self.assertEqual(first, parameter_symbols(list(reversed(rows))))
        self.assertNotEqual(first["first/width"], first["second/width"])
        with self.assertRaises(PrivateNetlistUnsupported):
            parameter_symbols(rows + rows[:1])

    def test_template_includes_candidate_file_once_and_removes_old_seeds(self):
        result = _without_symbol_declarations(self.netlist(), {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"})
        self.assertEqual(result.count('include "cr_candidate_parameters.scs"'), 1)
        self.assertIn("parameters VDD=1.2", result)
        self.assertNotIn("CRESCENDO_NL_width=2u", result)
        self.assertIn("w=CRESCENDO_NL_width", result)

    def test_large_hierarchy_mapping_is_linear_in_one_netlist_read(self):
        text = self.netlist()
        branches = []
        for level in range(12):
            branches.append("// Library name: SOURCE\n// Cell name: B%d\nsubckt B%d in out\n" % (level, level))
            branches.extend("R%d (in out) resistor r=%d\n" % (i, 1000 + i) for i in range(100))
            if level: branches.append("XCHILD (in out) B%d\n" % (level - 1))
            branches.append("ends B%d\n" % level)
        suffix = "".join(branches)
        refs = self.references()
        for row in refs: row["text"] += suffix
        started = time.monotonic()
        result = qualify_symbolic_netlist(text + suffix, refs, {"p": "CRESCENDO_NL_width", "q": "CRESCENDO_NL_finger"})
        self.assertTrue(result["ok"])
        self.assertGreater(result["references"][0]["assignment_count"], 1200)
        self.assertLess(time.monotonic() - started, 5.0)


class PrivateNetlistCandidateTest(unittest.TestCase):
    def template(self, root):
        root = os.path.join(root, "template")
        netlist_dir = os.path.join(root, "tasks", "t", "netlist")
        os.makedirs(os.path.join(netlist_dir, "amap"))
        netlist = os.path.join(netlist_dir, "input.scs")
        with open(netlist, "w") as stream:
            stream.write('simulator lang=spectre\ninclude "cr_candidate_parameters.scs"\nR0 (n 0) resistor r=CRESCENDO_NL_p\n')
        metadata = os.path.join(netlist_dir, "amap", "top_level_map.net")
        with open(metadata, "w") as stream: stream.write("native-name-map\n")
        rows = [{"relative_path": os.path.relpath(path, netlist_dir), "path": path,
                 "sha256": digest(path), "metadata": path != netlist} for path in (netlist, metadata)]
        task = {"task_id": "t", "point_id": 7, "design_point": 1, "corner": "FF", "test": "test",
                "netlist_path": netlist, "cwd": netlist_dir, "psf_dir": os.path.dirname(netlist_dir) + "/psf",
                "spectre_log": os.path.dirname(netlist_dir) + "/psf/spectre.out",
                "netlist_sha256": digest(netlist), "template_files": rows,
                "artifact_sha256": {row["path"]: row["sha256"] for row in rows},
                "argv": ["/tools/spectre", "input.scs", "-inter", "-plugin", "/native/plugin",
                         "+mpssession=old", "+mpshost=old", "-format", "psfxl", "+escchars",
                         "-raw", "../psf", "+log", "../psf/spectre.out"], "variables": [{"name": "VDD", "value": "1.0"}]}
        result = {"schema_version": "crescendo.private_netlist.v1", "root": root, "parameter_symbols": {"p": "CRESCENDO_NL_p"},
                "tasks": [task], "qualification": {"ok": True},
                "workspace": {"large_source_guard": "x" * 100000}}
        result["manifest_sha256"] = _manifest_digest(result)
        return result

    def test_candidates_share_no_mutable_files_and_retain_native_test_corner(self):
        with tempfile.TemporaryDirectory() as root:
            source = self.template(root)
            first = materialize_candidate(source, {"p": 1000}, os.path.join(root, "c1"), "c1")
            second = materialize_candidate(source, {"p": 2000}, os.path.join(root, "c2"), "c2")
            one, two = first["tasks"][0], second["tasks"][0]
            self.assertEqual((one["point_id"], one["corner"], one["test"]), (7, "FF", "test"))
            self.assertNotEqual(os.stat(one["netlist_path"]).st_ino, os.stat(two["netlist_path"]).st_ino)
            with open(one["parameter_file"]) as stream: self.assertIn("CRESCENDO_NL_p=1000", stream.read())
            self.assertIn(os.path.join(one["cwd"], "amap", "top_level_map.net"), one["artifact_sha256"])
            self.assertEqual(first["template_sha256"], source["manifest_sha256"])
            self.assertNotIn("workspace", first)
            self.assertNotIn("template_files", one)
            self.assertLess(len(json.dumps(first)), 10000)

    def test_candidate_legality_is_left_to_spectre(self):
        with tempfile.TemporaryDirectory() as root:
            result = materialize_candidate(self.template(root), {"p": -2.12345}, os.path.join(root, "negative"), "negative")
            with open(result["tasks"][0]["parameter_file"]) as stream:
                self.assertIn("CRESCENDO_NL_p=-2.12345", stream.read())

    def test_cli_argv_drops_native_ipc_but_preserves_simulator_flags(self):
        task = {"argv": ["spectre", "input.scs", "-inter", "-plugin", "ipc.so", "+mpshost=x", "+mpssession=y",
                         "-format", "psfxl", "+escchars", "-raw", "old", "+log", "oldlog", "-ahdllibdir", "oldahdl"]}
        argv = _candidate_argv(task, "/new/input.scs", "/new/psf", "/new/psf/spectre.out", "/new/ahdl")
        self.assertNotIn("-inter", argv)
        self.assertNotIn("-plugin", argv)
        self.assertNotIn("old", argv)
        self.assertIn("+escchars", argv)
        self.assertIn("psfxl", argv)
        self.assertEqual(argv[:2], ["spectre", "/new/input.scs"])

    def test_template_or_parameter_artifact_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            source = self.template(root)
            result = materialize_candidate(source, {"p": 1}, os.path.join(root, "c"), "c")
            with open(result["tasks"][0]["parameter_file"], "a") as stream: stream.write("parameters injected=1\n")
            with self.assertRaisesRegex(PrivateNetlistError, "changed"):
                validate_manifest(result)

    def test_unsafe_existing_root_and_identity_mismatch_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            source = self.template(root)
            target = os.path.join(root, "occupied")
            os.mkdir(target)
            with open(os.path.join(target, "candidate.json"), "w") as stream: stream.write("user-data")
            with self.assertRaises(PrivateNetlistError): materialize_candidate(source, {"p": 1}, target, "c")
            with self.assertRaises(PrivateNetlistError): materialize_candidate(source, {"other": 1}, os.path.join(root, "c"), "c")
            with self.assertRaises(PrivateNetlistError): materialize_candidate(source, {"p": float("nan")}, os.path.join(root, "c"), "c")

    def test_metadata_symlink_is_never_followed(self):
        with tempfile.TemporaryDirectory() as root:
            source = self.template(root)
            metadata = source["tasks"][0]["template_files"][1]["path"]
            os.unlink(metadata)
            os.symlink(source["tasks"][0]["netlist_path"], metadata)
            with self.assertRaises(PrivateNetlistError): validate_manifest(source)

    def test_manifest_metadata_or_task_identity_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            source = self.template(root)
            source["tasks"][0]["corner"] = "wrong"
            with self.assertRaisesRegex(PrivateNetlistError, "identity changed"):
                validate_manifest(source)
            source["tasks"][0]["task_id"] = "../escape"
            source["manifest_sha256"] = _manifest_digest(source)
            with self.assertRaisesRegex(PrivateNetlistError, "identity is unsafe"):
                materialize_candidate(source, {"p": 1}, os.path.join(root, "candidate"), "id")


class PrivateNetlistPreparationRecoveryTest(unittest.TestCase):
    def test_direct_prepare_cannot_bypass_design_variable_mode_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(PrivateNetlistUnsupported, "original Maestro"):
                prepare_private_netlist({}, [{"backend": "design_variable"}], {}, os.path.join(root, "new"))

    def test_capture_rejects_active_hooks_and_nonstandard_run_modes(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "maestro.sdb")
            setup = {"source": {"setup_db": source}, "tests": [{"simulator": "spectre"}],
                     "mode": "Single Run, Sweeps and Corners"}
            with open(source, "w") as stream:
                stream.write('<setupdb><active><postRunScript>submit_job()</postRunScript></active></setupdb>')
            with self.assertRaisesRegex(PrivateNetlistUnsupported, "scripts require"):
                _qualify_setup_capture(setup)
            with open(source, "w") as stream: stream.write('<setupdb><active/></setupdb>')
            _qualify_setup_capture(setup)
            setup["mode"] = "Monte Carlo"
            with self.assertRaisesRegex(PrivateNetlistUnsupported, "run mode"):
                _qualify_setup_capture(setup)

    def test_missing_or_unknown_ownership_retains_capture_and_blocks_reuse(self):
        with tempfile.TemporaryDirectory() as root:
            phase = os.path.join(root, "baseline")
            os.mkdir(phase)
            with open(os.path.join(phase, "ocean.stdout"), "w") as stream: stream.write("started")
            proof = reconcile_preparation(root)
            self.assertFalse(proof["cleanup_proven"])
            self.assertTrue(os.path.isfile(os.path.join(phase, "ocean.stdout")))

    def test_exact_owned_capture_and_fallback_reconcile_independently(self):
        with tempfile.TemporaryDirectory() as root:
            for name, workspace in (("baseline", "workspace"), ("baseline_fallback", "workspace_fallback")):
                phase = os.path.join(root, name)
                os.mkdir(phase)
                with open(os.path.join(phase, "legacy_guardian_launch.json"), "w") as stream:
                    json.dump({"token": "netlist-capture-" + "a" * 32,
                               "workspace_root": os.path.join(root, workspace),
                               "argv": ["ocean", "-nograph", "-restore", os.path.join(phase, "capture.il")]}, stream)
            with mock.patch("crescendo_engine.legacy_guardian.quiesce_legacy_guardian_attempt", return_value={"state": "dead"}) as cleanup:
                proof = reconcile_preparation(root)
            self.assertTrue(proof["cleanup_proven"])
            self.assertEqual(cleanup.call_count, 2)
            self.assertEqual(cleanup.call_args_list[1][1]["expected_workspace_root"], os.path.join(root, "workspace_fallback"))


if __name__ == "__main__":
    unittest.main()
