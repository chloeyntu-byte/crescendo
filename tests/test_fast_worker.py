from __future__ import print_function

import os
import tempfile
import unittest

from crescendo_engine.fast_worker import (
    PARAMETERIZED_INSTANCE_SCALAR, REQUIRES_RECOMPILE, RUNTIME_DESIGN_VAR, UNSUPPORTED,
    FastWorkerFallbackRequired, ParametricWorker, build_parametric_plan,
    candidate_design_variables, classify_parameter, validate_initialization_evidence,
)


class FastWorkerTest(unittest.TestCase):
    def _schematic_parameter(self, parameter_id="p1", occurrence="/X1", semantic="width"):
        return {
            "parameter_id": parameter_id,
            "backend": "schematic",
            "design_library": "D",
            "design_cell": "TOP",
            "design_view": "schematic",
            "hierarchy_path": "%s/M0" % occurrence,
            "owner_occurrence_path": occurrence,
            "owner_library": "D",
            "owner_cell": "SHARED",
            "name": "w" if semantic == "width" else semantic,
            "semantic": semantic,
            "current_value": "2u",
            "numeric_value": 2e-6,
            "editable": True,
            "tunable": True,
        }

    def _validated_evidence(self, plan):
        return {"parameters": [{
            "parameter_id": row["parameter_id"],
            "design_variable": row["design_variable"],
            "owner_occurrence_path": row["owner_occurrence_path"],
            "status": "validated",
        } for row in plan["parameters"] if row["classification"] == PARAMETERIZED_INSTANCE_SCALAR]}

    def test_classifies_all_required_categories(self):
        runtime = classify_parameter({
            "parameter_id": "dv", "backend": "design_variable", "name": "VDD",
        })
        scalar = classify_parameter(self._schematic_parameter())
        topology = classify_parameter(self._schematic_parameter("nf", semantic="fingers"))
        unsupported = classify_parameter({"parameter_id": "x", "backend": "unknown"})
        self.assertEqual(runtime["classification"], RUNTIME_DESIGN_VAR)
        self.assertEqual(scalar["classification"], PARAMETERIZED_INSTANCE_SCALAR)
        self.assertTrue(scalar["requires_validation"])
        self.assertEqual(topology["classification"], REQUIRES_RECOMPILE)
        self.assertEqual(unsupported["classification"], UNSUPPORTED)

    def test_reused_subcell_occurrences_receive_unique_stable_design_variables(self):
        parameters = [
            self._schematic_parameter("p1", "/X1"),
            self._schematic_parameter("p2", "/X2"),
        ]
        first = build_parametric_plan(parameters)
        second = build_parametric_plan(parameters)
        names = [row["design_variable"] for row in first["parameters"]]
        self.assertTrue(first["eligible"])
        self.assertEqual(names, [row["design_variable"] for row in second["parameters"]])
        self.assertEqual(len(set(names)), 2)
        self.assertTrue(all(name.startswith("CRESCENDOFW_") for name in names))

    def test_repeated_runtime_variable_name_is_rejected_as_ambiguous(self):
        plan = build_parametric_plan([
            {"parameter_id": "test1:VDD", "backend": "design_variable", "name": "VDD"},
            {"parameter_id": "test2:VDD", "backend": "design_variable", "name": "VDD"},
        ])
        self.assertFalse(plan["eligible"])
        self.assertTrue(all(row["classification"] == UNSUPPORTED for row in plan["parameters"]))

    def test_initialization_evidence_must_match_occurrence_and_design_variable(self):
        plan = build_parametric_plan([self._schematic_parameter()])
        evidence = self._validated_evidence(plan)
        self.assertEqual(validate_initialization_evidence(plan, evidence)["validated_parameter_ids"], ["p1"])
        evidence["parameters"][0]["owner_occurrence_path"] = "/WRONG"
        with self.assertRaisesRegex(Exception, "occurrence mismatch"):
            validate_initialization_evidence(plan, evidence)

    def test_worker_initializes_once_updates_each_candidate_and_runs_fresh_maestro(self):
        calls = {"build": 0, "initialize": 0, "update": [], "run": []}

        def build(_setup, _hierarchy, _config, root):
            calls["build"] += 1
            os.makedirs(root)
            return {"root": root, "setup_dir": root}

        def initialize(_workspace, plan, _seed_values, _worker_root):
            calls["initialize"] += 1
            return self._validated_evidence(plan)

        def update(_workspace, assignments):
            calls["update"].append(dict(assignments))
            return {"ok": True}

        def run(_workspace, _config, candidate_root):
            calls["run"].append(candidate_root)
            return {"returncode": 0, "candidate_root": candidate_root}

        with tempfile.TemporaryDirectory() as root:
            worker = ParametricWorker(
                {}, {}, {}, [self._schematic_parameter()], os.path.join(root, "worker"),
                oa_initializer=initialize, workspace_builder=build,
                design_variable_updater=update, maestro_runner=run,
            )
            first = worker.evaluate({"p1": 3e-6}, os.path.join(root, "c1"))
            second = worker.evaluate({"p1": 4e-6}, os.path.join(root, "c2"))
            self.assertEqual(first["mode"], "parametric_worker")
            self.assertEqual(second["mode"], "parametric_worker")
            self.assertEqual(calls["build"], 1)
            self.assertEqual(calls["initialize"], 1)
            self.assertEqual(len(calls["update"]), 2)
            self.assertEqual(len(calls["run"]), 2)
            variable_name = worker.plan["parameters"][0]["design_variable"]
            self.assertEqual(calls["update"], [
                {variable_name: 3e-6}, {variable_name: 4e-6},
            ])

    def test_ineligible_parameter_skips_workspace_and_uses_fallback(self):
        calls = {"build": 0, "fallback": 0}

        def build(*_args):
            calls["build"] += 1
            raise AssertionError("ineligible worker must not clone a workspace")

        def fallback(values, _candidate_root, reason):
            calls["fallback"] += 1
            return {"values": values, "reason": reason}

        parameter = self._schematic_parameter("nf", semantic="fingers")
        with tempfile.TemporaryDirectory() as root:
            worker = ParametricWorker(
                {}, {}, {}, [parameter], os.path.join(root, "worker"),
                workspace_builder=build, fallback_evaluator=fallback,
            )
            result = worker.evaluate({"nf": 4}, os.path.join(root, "candidate"))
        self.assertEqual(result["mode"], "fallback")
        self.assertEqual(calls, {"build": 0, "fallback": 1})

    def test_missing_validation_disables_fast_path_and_falls_back(self):
        calls = {"fallback": 0, "run": 0}

        def build(_setup, _hierarchy, _config, root):
            os.makedirs(root)
            return {"root": root, "setup_dir": root}

        def fallback(_values, _candidate_root, reason):
            calls["fallback"] += 1
            return reason

        def run(*_args):
            calls["run"] += 1
            return {}

        with tempfile.TemporaryDirectory() as root:
            worker = ParametricWorker(
                {}, {}, {}, [self._schematic_parameter()], os.path.join(root, "worker"),
                oa_initializer=lambda *_args: {"parameters": []},
                workspace_builder=build, fallback_evaluator=fallback, maestro_runner=run,
            )
            result = worker.evaluate({"p1": 3e-6}, os.path.join(root, "candidate"))
            self.assertEqual(worker.state, "fallback")
        self.assertEqual(result["mode"], "fallback")
        self.assertEqual(calls, {"fallback": 1, "run": 0})

    def test_no_fallback_hook_raises_explicit_request(self):
        parameter = self._schematic_parameter("nf", semantic="fingers")
        with tempfile.TemporaryDirectory() as root:
            worker = ParametricWorker({}, {}, {}, [parameter], os.path.join(root, "worker"))
            with self.assertRaises(FastWorkerFallbackRequired):
                worker.evaluate({"nf": 4}, os.path.join(root, "candidate"))

    def test_candidate_assignments_include_existing_and_generated_variables(self):
        plan = build_parametric_plan([
            {"parameter_id": "dv", "backend": "design_variable", "name": "VDD"},
            self._schematic_parameter(),
        ])
        assignments = candidate_design_variables(plan, {"dv": 1.2, "p1": 3e-6})
        self.assertEqual(assignments["VDD"], 1.2)
        generated = plan["parameters"][1]["design_variable"]
        self.assertEqual(assignments[generated], 3e-6)


if __name__ == "__main__":
    unittest.main()
