# Headless optimizer API

Other tools can use Crescendo's search algorithms without the window, Maestro, a scheduler or any application startup. This page is the supported surface. It is pinned by `tests/test_headless_optimizer_api.py`.

## Modules

| Module | Use |
|---|---|
| `crescendo_engine.optimizers` | `PopulationGenerator`, `validate_variables`, `ALGORITHMS` |
| `crescendo_engine.metrics` | objective and constraint scoring helpers, `dominates` |
| `crescendo_engine.engineering` | engineering-number parsing (`1u`, `2.5k`, …) |
| `crescendo_engine.__version__` | engine version, for receipts |

Importing these modules and running ask/tell:

- does not call `runtime.initialize`, register scheduler providers, run the worker bootstrap or the ownership gate;
- does not add environment variables;
- does not load Tk, the GUI, the service or the Cadence integration;
- works as root and as an ordinary user, on Python 3.6 to 3.13.

These imports fix the engine's default runtime contract for the process. A host that needs its own `runtime.initialize(...)` must call it before importing any engine module, as with every other engine entry point.

## Variables

```python
{"parameter_id": "w", "minimum": "1u", "maximum": "20u",    # numbers or engineering strings
 "scale": "linear" | "log",                                  # optional, default linear
 "value_type": "float" | "integer" | "enum",                 # optional, default float
 "step": ..., "choices": [...]}                              # optional, see validate_variables
```

`validate_variables` raises `ValueError` for empty lists, `minimum >= maximum`, unparseable numbers, unknown `value_type`, non-integer integer bounds and misaligned steps.

## Algorithms

`ALGORITHMS = ("random", "differential_evolution", "nsga2", "bayesian_optimization")`. An unknown name raises `ValueError` when the generator is constructed.

## Ask / tell

```python
from crescendo_engine.optimizers import PopulationGenerator, validate_variables

generator = PopulationGenerator(validate_variables(variables), "nsga2", seed=1)
values = generator.ask(8)          # list of {parameter_id: value}
generator.tell([{"candidate_id": "g0_c0", "status": "completed", "values": values[0],
                 "metrics": {"total_violation": 0.0, "merit": score,
                             "objectives": [{"score": score}]}}, ...])
```

Scores are minimized. The same seed, variables, parameters and told rows give the same candidates, in-process and across processes.

## Stability

These names, the variable fields and the algorithm names change only in a minor or major release, and the change is announced in the changelog first. Use a released snapshot rather than the development tree.
