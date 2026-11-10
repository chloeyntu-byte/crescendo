"""Large read-only catalogs retain all identities and allow bounded cancellation."""
import contextlib
import os
import tempfile
import threading
import unittest
from unittest import mock

from crescendo_engine import hierarchy, headless_display, runner
from crescendo_engine.cadence import CommandCancelled, command_cancellation_scope


class HierarchyScalingTests(unittest.TestCase):
    def records(self, count):
        yield {"kind": "CELL", "values": ["/", "L", "TOP", "schematic", 0]}
        yield {"kind": "INSTANCE", "values": ["/XDUT", "XDUT", "L", "CORE", "symbol", True, "/", 0]}
        yield {"kind": "CELL", "values": ["/XDUT", "L", "CORE", "schematic", 1]}
        for index in range(count):
            path = "/XDUT/M%d" % index
            yield {"kind": "INSTANCE", "values": [path, "M%d" % index, "PDK", "nmos", "symbol", False, "/XDUT", 1]}
            for name, value in (("w", "2u"), ("l", "60n")):
                yield {"kind": "PARAM", "values": [path, name, name, "instData", value, value, "lengthMetric", None,
                    True, True, True, None, None, "L", "CORE", None, "string"]}

    def test_large_stream_preserves_every_occurrence_parameter_and_mutable_chain(self):
        result = hierarchy.normalize_probe(self.records(2048), {"library": "L", "cell": "TOP", "view": "schematic"})
        self.assertEqual(result["counts"]["instances"], 2049)
        self.assertEqual(result["counts"]["parameters"], 4096)
        self.assertEqual(len({row["parameter_id"] for row in result["parameters"]}), 4096)
        for row in result["parameters"]:
            self.assertEqual(row["owner_occurrence_path"], "/XDUT")
            self.assertEqual([item["hierarchy_path"] for item in row["occurrence_chain"]], ["/XDUT"])
        first, second = result["parameters"][:2]
        first["occurrence_chain"][0]["name"] = "mutated-private-copy"
        self.assertEqual(second["occurrence_chain"][0]["name"], "XDUT")

    def test_public_parser_list_and_stream_normalization_remain_equivalent(self):
        payload = 'CELL\t"/"\t"L"\t"TOP"\t"schematic"\t0\nINSTANCE\t"/M0"\t"M0"\t"PDK"\t"nmos"\t"symbol"\tnil\t"/"\t0\n'
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "probe.records")
            with open(path, "w") as stream:
                stream.write(payload)
            self.assertIsInstance(hierarchy.parse_probe_records(path), list)
            self.assertEqual(hierarchy.normalize_probe(hierarchy.parse_probe_records(path)),
                             hierarchy.normalize_probe(hierarchy._iter_probe_records(path)))

    def test_cancel_large_normalization_does_not_consume_remaining_records(self):
        event = threading.Event()
        consumed = []
        def records():
            for index, record in enumerate(self.records(4096)):
                consumed.append(index)
                if index == 30:
                    event.set()
                yield record
        with self.assertRaises(CommandCancelled), command_cancellation_scope(event):
            hierarchy.normalize_probe(records())
        self.assertLessEqual(len(consumed), 257)

    def test_catalog_variable_expressions_scale_without_stealing_another_tests_value(self):
        tests = [{"name": "t%d" % index, "library": "L", "cell": "C%d" % (index // 2), "view": "schematic",
                  "variables": [{"name": "bias", "value": str(index)}]} for index in range(200)]
        designs = []
        for index in range(100):
            designs.append({"design": {"library": "L", "cell": "C%d" % index}, "hierarchy": {
                "instances": [{"hierarchy_path": "/V0", "master_library": "analogLib", "master_cell": "vdc"}],
                "parameters": [{"parameter_id": "p%d" % index, "hierarchy_path": "/V0", "name": "vdc",
                    "current_value": "bias", "source": "cdf", "editable": True}]}})
        with mock.patch.object(runner, "parse_physical_number", wraps=runner.parse_physical_number) as parse:
            rows = runner.parameter_catalog({"tests": tests}, {"designs": designs})
        # Catalog discovery cannot choose one Test/corner value for a shared
        # schematic expression. Native point netlisting resolves its scope.
        self.assertEqual(parse.call_count, 0)
        schematic = {row["parameter_id"]: row for row in rows if row["backend"] == "schematic"}
        self.assertEqual(len(schematic), 100)
        for index in range(100):
            self.assertIsNone(schematic["p%d" % index]["numeric_value"])
            self.assertEqual(schematic["p%d" % index]["current_value"], "bias")
            self.assertEqual(schematic["p%d" % index]["baseline_expression"], "bias")
            self.assertTrue(schematic["p%d" % index]["requires_point_numeric_value"])
            self.assertTrue(schematic["p%d" % index]["optimizable"])

    def test_caught_native_abort_never_presents_partial_hierarchy_as_complete(self):
        partial = hierarchy.normalize_probe([
            {"kind": "CELL", "values": ["/", "L", "TOP", "schematic", 0]},
            {"kind": "PROBE_FAILED", "values": ["/"]},
        ])
        with self.assertRaisesRegex(RuntimeError, "native hierarchy scan aborted"):
            hierarchy.validate_native_hierarchy(partial, {"library": "L", "cell": "TOP", "view": "schematic"})

    def test_cancellation_during_owned_display_start_stops_only_owned_process(self):
        event = threading.Event()
        process = mock.Mock(pid=31415, returncode=None)
        process.poll.return_value = None
        def stop():
            process.poll.return_value = -15
            process.returncode = -15
        process.terminate.side_effect = stop
        def readiness(number, owned):
            event.set()
            return False
        with tempfile.TemporaryDirectory() as root, contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(headless_display.shutil, "which", return_value="/owned/Xvnc"))
            stack.enter_context(mock.patch.object(headless_display.os.path, "lexists", return_value=False))
            launch = stack.enter_context(mock.patch.object(headless_display.subprocess, "Popen", return_value=process))
            stack.enter_context(mock.patch.object(headless_display, "_display_ready", side_effect=readiness))
            stack.enter_context(mock.patch.object(headless_display.time, "sleep"))
            with self.assertRaises(CommandCancelled), command_cancellation_scope(event):
                with headless_display.owned_headless_display({}, root):
                    self.fail("cancelled display must not yield")
            launch.assert_called_once()
            process.terminate.assert_called_once_with()
            process.kill.assert_not_called()


if __name__ == "__main__":
    unittest.main()
