# Diagnostic scalar consistency and CLI readback

F2 compares every completed candidate with the same frozen values in all three
execution modes. `compared_candidate_count`, candidate IDs and per-mode completed
counts describe the actual population. F2 remains report-only.

The expected scalar set is the selected objectives and constraints plus their
same-Test expression dependencies. Metric corner masks propagate to dependencies.
Unknown units use relative tolerance `1e-6`, with an absolute floor equal to the
observed magnitude times `1e-12` (zero at zero; subnormal underflow is allowed).
Known units retain the existing unit tolerance table. Each output reports unit
provenance (`native_result`, `frozen_setup`, `inferred_value_dependency`, or
`fallback`), absolute policy and actual minimum/maximum pair tolerances.
`unknown_unit_fallback` is true for empty and unrecognized units. No unit is
inferred from a measurement name. A direct `value(alias frequency)` can inherit
its dependency's saved unit.

Only required `value` consumers provide structural waveform type evidence; idle
expressions cannot change a selected output's type or comparison coverage. Any
finite scalar readback keeps the output in scalar comparisons, including when
other modes or saved metadata identify it as a waveform. Missing observations
then remain unpaired instead of being hidden by type inference.

Waveforms identified from native readback, metadata or required `value` dependencies are
listed as not compared because the diagnostic evidence currently has no paired
frequency axes. They do not create unpaired scalar records. Unused expressions
are listed separately with their exclusion reason. Unknown output types remain
required when selected; they are never silently treated as idle.

The CLI reader still selects only metrics for mandatory scalar acceptance.
It evaluates same-Test dependencies in topological order. Optional `D` records
retain dependency observations (numeric, waveform or evaluation failure) under
`crescendo.output_observations.v1`, included in reader capability data and the
existing versioned batch manifest. The scalar protocol's identity/END fences are
unchanged, and old records without dependency observations remain readable.
Dependency observations do not qualify completion. Aliases are scoped and cleared
per point, and failed dependencies block their consumers rather than reuse a
previous alias value. User expressions are evaluated by Ocean, not Python.

Candidate `output_statuses`, raw diagnostic points and F2 unpaired states preserve:

- `not_collected`: no scalar readback was collected, including unselected outputs.
- `non_finite`: the reader saw a numeric NaN or infinity; candidate acceptance
  still rejects it, and JSON stores no NaN/infinity token.
- `evaluation_failed`: native or protocol expression failure, with `first_error`.
- `available` and `waveform`: finite scalar and observed non-scalar waveform.
- `unknown`: legacy null/absent diagnostic evidence without a reader state; the
  checker does not retrospectively label it as collection or evaluation failure.

The capability record includes requested keys, dependency evaluation keys,
expressions, result location and analysis inventory. `reader_first_error` retains
the first native error found in the bounded reader log. It is reader-level context:
a batch can contain several points, so this shared error is not attributed to an
individual expression without point-specific evidence. A missing/incomplete
protocol remains a collection failure and cannot ACK a point.

When diagnosing a missing derived RF output, collect the frozen metric selections
and Setup/state definitions, exact generated reader program, token-bound TSV and
capability JSON, reader stdout/stderr, exact point/task manifest, Native RDB and
corresponding PSF plus the SP sweep definition. Compare the expression frequency
to the actual PSF axis. The engine does not infer an out-of-range sweep from a
missing scalar or substitute zero. Synthetic tests exercise Native SQLite RDB
parsing and single/batch CLI contracts; they do not certify a remote PSF, native
Calculator evaluation, or physical simulation.

A local native Calculator smoke also exercises these generated scripts against a
synthetic 1–30 GHz waveform, with PSF open/inventory explicitly stubbed. Direct
native evaluation and both reader forms agree at 22 GHz (about `1.68e-14`). The old
single reader reproduces stale-alias acceptance after a dependency throws; the
fixed reader reports dependency evaluation failure. On that native installation,
`value` at 100 GHz returns the endpoint (`1.7e-14`), so an out-of-domain query is
not by itself proof of missing collection. This smoke is not a physical SP run.
