"""Token-bound reader diagnostics never qualify absent candidate parameters.

The Ocean responses here are synthetic: no vendor solver support is claimed.
"""
import copy
import json
import os
import re
import sys
import tempfile
import unittest

from crescendo_engine import netlist_measurement as measurement
from crescendo_engine.legacy_guardian import close_legacy_guardian_launch_lock


class ResultReaderCapabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo-reader-capability-")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        os.mkdir(os.path.join(self.root, "psf"))
        self.task = {"task_id": "owned_rc", "test": "rc", "corner": "TT", "point_id": 1,
                     "execution_simulator": "alps", "psf_dir": os.path.join(self.root, "psf"),
                     "cwd": self.root, "netlist_sha256": "a" * 64,
                     "variables": [{"name": "CRESCENDO_NL_p", "value": 1000}],
                     "native_parameters": [{"name": "CRESCENDO_NL_p", "value": 1000}],
                     "outputs": [{"key": "rc::gain", "name": "gain", "expression": 'value(mag(VF("/out")) 1000)'}]}
        self.config = {"cadence": {"ocean_bin": sys.executable}, "runtime": {}}
        self.records = os.path.join(self.root, "fixture.tsv")
        self.calls = []

    def parse(self, body, capability=None, token="owned"):
        with open(self.records, "w") as stream:
            stream.write(measurement.PROTOCOL + "\towned\n" + body + "END\t%d\n" % len(self.task["outputs"]))
        return measurement.parse_measurement_records(self.records, token, self.task["outputs"],
            self.task, measurement.required_variables(self.task), capability=capability)

    def command_runner(self, body, returncode):
        def run(argv, **kwargs):
            self.calls.append(list(argv))
            with open(argv[-1]) as stream:
                script = stream.read()
            filename = json.loads(re.search(r'crescPort = outfile\(("(?:\\.|[^"\\])*")\)', script).group(1))
            token = re.search(r'CRESCENDO_NETLIST_MEASURE_1\\t([^"\\]+)\\n', script).group(1)
            with open(filename, "w") as stream:
                stream.write(measurement.PROTOCOL + "\t" + token + "\n" + body + "END\t%d\n" % len(self.task["outputs"]))
            close_legacy_guardian_launch_lock(kwargs.get("guardian_launch_lock_fd"))
            return {"returncode": returncode, "guardian_cleanup": {"state": "dead"}}
        return run

    def evaluate(self, body, returncode=0):
        return measurement.evaluate_task_outputs(self.task, self.config, self.root,
            timeout_seconds=30, command_runner=self.command_runner(body, returncode))

    def test_missing_metadata_keeps_waveform_diagnostic_without_returning_measurements(self):
        capability = {}
        with self.assertRaises(measurement.NetlistMeasurementError) as caught:
            self.parse('A\t0\tsp\nI\t0\tavailable\nX\t0\tunavailable_design_parameter\nV\t0\t4.2\n', capability)
        self.assertEqual(caught.exception.detail["failure_stage"], "result_reader_parameter_metadata")
        self.assertEqual(capability["analysis_names"], ["sp"])
        self.assertEqual(capability["native_parameter_readback"]["actual"], {})
        self.assertEqual(capability["native_parameter_readback"]["missing"], ["CRESCENDO_NL_p"])
        self.assertFalse(capability["native_parameter_readback"]["ok"])
        self.assertEqual(capability["output_diagnostics"][0]["status"], "PASS")
        self.assertFalse(capability["candidate_qualification"]["ok"])

    def test_own_evaluator_persists_failed_inventory_and_launches_ocean_once(self):
        with self.assertRaises(measurement.NetlistMeasurementError) as caught:
            self.evaluate('A\t0\tsp\nI\t0\tavailable\nX\t0\tunavailable_design_parameter\nV\t0\t4.2\n', 3)
        detail = caught.exception.detail
        self.assertEqual(len(self.calls), 1)
        with open(detail["capability_path"]) as stream:
            capability = json.load(stream)
        self.assertEqual(capability, detail["result_reader_capability"])
        self.assertEqual(capability["netlist_sha256"], self.task["netlist_sha256"])
        self.assertEqual(capability["psf_dir"], self.task["psf_dir"])
        self.assertEqual(capability["command_returncode"], 3)
        self.assertFalse(os.path.exists(os.path.join(os.path.dirname(detail["capability_path"]), "measurement.json")))
        self.assertNotIn("Maestro backend", str(caught.exception))

    def test_token_bound_finished_open_failure_is_a_separate_stage(self):
        with self.assertRaises(measurement.NetlistMeasurementError) as caught:
            self.evaluate('O\t0\tunavailable\n', 4)
        self.assertEqual(caught.exception.detail["failure_stage"], "result_reader_open_results")
        capability = caught.exception.detail["result_reader_capability"]
        self.assertEqual(capability["open_results_status"], "unavailable")
        self.assertEqual(capability["analysis_inventory_status"], "not_recorded")
        self.assertEqual(capability["output_diagnostics"][0]["status"], "NOT_TESTED")
        self.assertFalse(capability["candidate_qualification"]["ok"])

    def test_conflicting_open_failure_cannot_create_observation(self):
        for body in ('O\t0\tunavailable\nV\t0\t4.2\n', 'O\t0\topened\nO\t0\tunavailable\n'):
            capability = {}
            with self.assertRaises(measurement.NetlistMeasurementError):
                self.parse(body, capability)
            self.assertEqual(capability, {})

    def test_successful_actual_values_keep_native_proof_but_diagnostics_are_not_completion(self):
        result = self.evaluate('A\t0\tsp\nA\t1\tdesignParamVals\nI\t0\tavailable\nP\t0\t1000\nV\t0\t4.2\n')
        self.assertTrue(result["native_parameter_readback_verified"]["ok"])
        self.assertEqual(result["native_parameter_readback_verified"]["source"], "current_psf_designParamVals")
        capability = result["result_reader_capability"]
        self.assertTrue(capability["native_parameter_readback"]["candidate_identity_verified"])
        self.assertFalse(capability["candidate_qualification"]["ok"])

    def test_mismatched_actual_values_persist_failure_without_measurement(self):
        with self.assertRaises(measurement.NetlistMeasurementError) as caught:
            self.evaluate('A\t0\tsp\nI\t0\tavailable\nP\t0\t900\nV\t0\t4.2\n')
        self.assertEqual(caught.exception.detail["failure_stage"], "result_reader_parameter_identity")
        capability = caught.exception.detail["result_reader_capability"]
        self.assertEqual(capability["native_parameter_readback"]["actual"], {"CRESCENDO_NL_p": 900})
        self.assertFalse(capability["native_parameter_readback"]["ok"])
        self.assertFalse(os.path.exists(os.path.join(os.path.dirname(caught.exception.detail["capability_path"]), "measurement.json")))

    def test_expected_values_never_seed_ocean_and_alias_var_dependencies_are_transitive(self):
        self.task["outputs"] = [
            {"key": "rc::A", "name": "A", "expression": "B+1"},
            {"key": "rc::B", "name": "B", "expression": 'VAR("VDD")'},
            {"key": "rc::wave", "name": "wave", "expression": 'value(VF("/out") 1000)'}]
        self.task["variables"].append({"name": "VDD", "value": 99})
        script, unused = measurement.render_measurement_script(self.task, self.records, "owned")
        self.assertNotIn('desVar("VDD" 99)', script)
        self.assertNotIn('desVar("CRESCENDO_NL_p" 1000)', script)
        self.assertIn('desVar("VDD" car(crescParam))', script)
        dependencies = measurement.output_variable_dependencies(self.task)
        self.assertEqual(dependencies["rc::A"], {"VDD"})
        self.assertEqual(dependencies["rc::wave"], set())
        self.assertEqual(script.count('setof(crescVariable list("VDD")'), 2)
        self.assertNotIn("unless(crescVarFailed", script)
        self.assertLess(script.index("openResults"), script.index("results()"))

    def test_unknown_inventory_is_distinct_from_empty_available_inventory(self):
        for state in ("unavailable", "available", "truncated"):
            capability = {}
            self.parse('I\t0\t%s\nP\t0\t1000\nV\t0\t4.2\n' % state, capability)
            self.assertEqual(capability["analysis_inventory_status"], state)
            self.assertEqual(capability["analysis_names"], [])

    def test_invalid_inventory_or_token_cannot_create_capability(self):
        for prefix in ('A\t1\tsp\n', 'A\t0\tsp\nA\t0\tac\n', 'I\t0\tavailable\nI\t0\tavailable\n',
                       'I\t0\tinvented\n', 'A\t0\t"bad\\nname"\n'):
            capability = {}
            with self.subTest(prefix=prefix), self.assertRaises(measurement.NetlistMeasurementError):
                self.parse(prefix + 'P\t0\t1000\nV\t0\t4.2\n', capability)
            self.assertEqual(capability, {})
        capability = {}
        with self.assertRaises(measurement.NetlistMeasurementError):
            self.parse('I\t0\tavailable\nP\t0\t1000\nV\t0\t4.2\n', capability, token="other")
        self.assertEqual(capability, {})


if __name__ == "__main__":
    unittest.main()
