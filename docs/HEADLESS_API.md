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
 "step": ..., "grid_origin": ..., "choices": [...],
 "link_group": "pair"}                              # optional, see validate_variables
```

`validate_variables` raises `ValueError` for empty lists, `minimum >= maximum`, unparseable numbers, unknown `value_type`, non-integer integer bounds and misaligned steps.

## Linked parameters (Crescendo 1.2 / engine 3.1)

`link_group` is an optional string. An absent or empty string means an independent
parameter. Nonempty names identify groups within the variables list; a group
occupies one optimizer dimension, in first-member order. A one-member group is
valid. Group names are case-sensitive; whitespace-only names are rejected.

- All members must have the same `scale` (`linear` or `log`).
- Continuous members may have different bounds. Their shared coordinate
  `u in [0, 1]` maps to `min + u * (max - min)` for linear scale and
  `exp(log(min) + u * (log(max) - log(min)))` for log scale.
- Stepped and integer members must have equal `step_count = (max - min) / step`.
  Integers default to `step=1`. The shared index is `k = round(u * step_count)`;
  each member receives `min + k * step`. There are `step_count + 1` grid points.
  Each member may have a different range, step and grid origin. Existing grid
  alignment checks still apply. Stepped members require linear scale.
- Continuous and stepped members may share a linear group: continuous members
  retain the shared `u`, while stepped members use its nearest grid index `k`.
  Enum linking is currently rejected explicitly, including numeric enums.
  Errors name the group and member.

```python
variables = [
    {"parameter_id": "left_w", "minimum": "1u", "maximum": "5u",
     "step": "1u", "link_group": "widths"},
    {"parameter_id": "right_w", "minimum": "10u", "maximum": "50u",
     "step": "10u", "link_group": "widths"},
]
generator = PopulationGenerator(variables, "nsga2", seed=1)
values = generator.ask(8)
# Every dictionary contains BOTH left_w and right_w, mapped to their OWN grids.
# For example: {"left_w": 3e-6, "right_w": 30e-6} (subject to float rounding).
```

**Expansion is part of the downstream contract:** every candidate returned by
`ask()` contains a separate value for **every member's `parameter_id`**, already
mapped through that member's bounds, scale and step. There is no representative
or group key in returned values. Pass those dictionaries back to `tell()` under
the same parameter IDs; its row format is unchanged. All four algorithms use
one internal coordinate per group, including selection, mutation and surrogate
training. The seed remains reproducible across processes. Without `link_group`,
the candidate sequence is byte-for-byte identical to v1.1.4 (pinned by a golden
fixture in the headless contract tests).

`step`, `grid_origin` and `link_group` are optional fields of saved parameter
settings as well as `optimization.variables`. Grid numbers and engineering
strings are in base units, independent of the GUI's display-unit selector. Save
Configuration, Restore Last and working-copy configuration round trips retain
these fields without injecting them into old configurations. The parameter
settings allowlist is `settings_state.PARAMETER_CONFIG`, also used by unified
configuration normalization. These are additive fields in the existing settings
and configuration v1 schemas.

Linked runs produce `crescendo.checkpoint.v2`; restoring them requires Crescendo
1.2 / engine 3.1 or later. Old versions reject this unknown schema with
`unsupported checkpoint; expected crescendo.checkpoint.v1`. Do not change the
schema to v1: new versions explicitly reject linked v1 checkpoints. Unlinked
runs keep checkpoint v1 and the old restore semantics. Group membership and all
member domains are included in the saved spec, checksum and continuation
identity; active plans and resumed candidates retain complete expanded values.
The dataset retains the full variable schema in `metadata.spec.variables`.
Candidate CSV exports append a `link_group:<parameter_id>` column for each linked
member, alongside its existing value column. Direct export helpers accept the
optional `variables=` argument to include this metadata. Unlinked CSV bytes and
column names are unchanged.

In the native parameter page, Ctrl/Shift highlights multiple parameter rows;
Link Selected names and validates the group, and Unlink Selected removes it.
These controls and the Link group column appear only in Crescendo Pro mode.
Switching to Simple mode retains saved group settings.

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
