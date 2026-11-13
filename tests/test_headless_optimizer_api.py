"""Contract: the optimizer is a headless library.

External callers (for example a design-flow tool that only needs ask/tell)
import ``crescendo_engine.optimizers``, ``crescendo_engine.metrics`` and
``crescendo_engine.engineering`` without any application startup. These tests
pin that contract; see docs/HEADLESS_API.md.

* Importing those modules and running ask/tell must not call
  ``runtime.initialize``, register scheduler providers, run the worker
  bootstrap or ownership gate, add environment variables, or load the GUI,
  Tk or Cadence integration.
* Variable records are ``{"parameter_id", "minimum", "maximum", ...}``.
* Algorithm names are ``random``, ``differential_evolution``, ``nsga2`` and
  ``bayesian_optimization``.
* The same seed gives the same candidates, in-process and across processes.
"""
import json
import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALGORITHMS = ("random", "differential_evolution", "nsga2", "bayesian_optimization")

# Engine modules a headless import may load. Anything else (GUI, scheduler
# providers, Cadence bridges, server) would break the contract.
ALLOWED_ENGINE_MODULES = {
    "crescendo_engine", "crescendo_engine.engineering", "crescendo_engine.metrics",
    "crescendo_engine.optimizers", "crescendo_engine.rdb", "crescendo_engine.runtime",
}

PROBE = r"""
import hashlib, json, os, sys
sys.path.insert(0, %(root)r)
before = dict(os.environ)
import crescendo_engine
from crescendo_engine import engineering, metrics, optimizers
from crescendo_engine.optimizers import PopulationGenerator, validate_variables

variables = [
    {"parameter_id": "w", "minimum": "1u", "maximum": "20u", "scale": "log"},
    {"parameter_id": "l", "minimum": 0.18e-6, "maximum": 2e-6},
    {"parameter_id": "nf", "minimum": 1, "maximum": 8, "value_type": "integer"},
]
seed = int(sys.argv[1])
trace = {}
for algorithm in %(algorithms)r:
    generator = PopulationGenerator(validate_variables(variables), algorithm, seed=seed)
    asked = []
    for generation in range(3):
        values = generator.ask(5)
        asked.append(values)
        rows = []
        for index, row in enumerate(values):
            score = (row["w"] * 1e6 - 7.0) ** 2 + row["l"] * 1e6 + row["nf"]
            rows.append({"candidate_id": "g%%d_c%%d" %% (generation, index), "status": "completed",
                         "values": row, "metrics": {"total_violation": 0.0, "merit": score,
                                                    "objectives": [{"score": score}]}})
        generator.tell(rows)
    trace[algorithm] = asked

from crescendo_engine import runtime
loaded = sorted(name for name in sys.modules if name.startswith("crescendo"))
print(json.dumps({
    "version": crescendo_engine.__version__,
    "digest": hashlib.sha256(json.dumps(trace, sort_keys=True).encode()).hexdigest(),
    "trace": trace,
    "loaded": loaded,
    "tk": [name for name in ("tkinter", "_tkinter") if name in sys.modules],
    "environ_changed": sorted(set(os.environ.items()) ^ set(before.items())),
    "explicitly_initialized": bool(runtime._state and runtime._state.get("provider_identity")),
    "bootstrap_env": runtime.BOOTSTRAP_ENV in os.environ,
    "descriptor_env": runtime.DESCRIPTOR_ENV in os.environ,
}))
"""


def run_probe(seed):
    env = dict(os.environ)
    env.pop("DISPLAY", None)
    env.pop("PYTHONPATH", None)
    code = PROBE % {"root": ROOT, "algorithms": ALGORITHMS}
    output = subprocess.check_output([sys.executable, "-I", "-c", code, str(seed)], env=env,
                                     cwd=ROOT, universal_newlines=True)
    return json.loads(output)


class HeadlessOptimizerContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.first = run_probe(11)

    def test_import_and_ask_tell_have_no_application_side_effects(self):
        result = self.first
        self.assertTrue(set(result["loaded"]) <= ALLOWED_ENGINE_MODULES, result["loaded"])
        self.assertEqual(result["tk"], [])
        self.assertEqual(result["environ_changed"], [])
        self.assertFalse(result["explicitly_initialized"])
        self.assertFalse(result["bootstrap_env"])
        self.assertFalse(result["descriptor_env"])
        self.assertTrue(result["version"])

    def test_all_four_algorithms_run_three_generations_within_bounds(self):
        for algorithm in ALGORITHMS:
            generations = self.first["trace"][algorithm]
            self.assertEqual(len(generations), 3, algorithm)
            for values in generations:
                self.assertEqual(len(values), 5, algorithm)
                for row in values:
                    self.assertEqual(set(row), {"w", "l", "nf"}, algorithm)
                    self.assertTrue(1e-6 <= row["w"] <= 20e-6, (algorithm, row))
                    self.assertTrue(0.18e-6 <= row["l"] <= 2e-6, (algorithm, row))
                    self.assertIn(row["nf"], range(1, 9), (algorithm, row))

    def test_same_seed_reproduces_across_processes_and_other_seed_differs(self):
        again = run_probe(11)
        self.assertEqual(again["digest"], self.first["digest"])
        other = run_probe(12)
        for algorithm in ALGORITHMS:
            self.assertNotEqual(other["trace"][algorithm], self.first["trace"][algorithm], algorithm)

    def test_variable_validation_errors(self):
        sys.path.insert(0, ROOT)
        from crescendo_engine.optimizers import PopulationGenerator, validate_variables
        cases = [
            ([], "at least one optimization variable"),
            ([{"parameter_id": "w", "minimum": 2, "maximum": 1}], "minimum must be less than maximum"),
            ([{"parameter_id": "w", "minimum": 1, "maximum": 1}], "minimum must be less than maximum"),
            ([{"parameter_id": "w", "minimum": "abc", "maximum": 1}], "w minimum"),
            ([{"parameter_id": "w", "minimum": 0, "maximum": 1, "value_type": "complex"}], "unsupported value_type"),
            ([{"parameter_id": "n", "minimum": 1, "maximum": 2.5, "value_type": "integer"}], "whole numbers"),
        ]
        for variables, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    validate_variables(variables)
        with self.assertRaisesRegex(ValueError, "unsupported algorithm"):
            PopulationGenerator([{"parameter_id": "x", "minimum": 0, "maximum": 1}], "simulated_annealing")
        from crescendo_engine.optimizers import ALGORITHMS as PUBLIC_ALGORITHMS
        self.assertEqual(tuple(PUBLIC_ALGORITHMS), ALGORITHMS)
        with self.assertRaisesRegex(ValueError, "population size must be positive"):
            PopulationGenerator([{"parameter_id": "x", "minimum": 0, "maximum": 1}], "random").ask(0)


if __name__ == "__main__":
    unittest.main()
