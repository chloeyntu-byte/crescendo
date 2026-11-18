# Netlist preparation timing

`crescendo.netlist_preparation_timing.v1` is optional observation data. It does
not select an execution path, qualify a circuit, change checkpoint identities or
participate in Resume/Stop authority. Existing results without it remain unknown.

The generation barrier owns a single observation, saved as `netlist_preparation`
in generation timing and its compact diagnostic projection. Candidate timing
references that observation with a `batch_id`; `prepare_*` phase sums deduplicate
by phase and identity, including logical candidates sharing a physical result.
Each physical observation has a fresh identity so resumed segments retaining old
candidate rows cannot hide the current preparation behind the earlier attempt.
Legacy scalar execution observes each candidate's actual preparation instead.
An exception still publishes the partial observation. Later generations do not
repeat qualification unless the existing execution lifecycle already requires it.

The five mutually exclusive stages partition the observed preparation boundary:

| Stage | Boundary |
| --- | --- |
| `template_qualification` | Native template qualification, including unchanged baseline, numeric, low and high captures, excluding the following nested stages |
| `session_lifecycle` | Each one-shot child's startup through successful `maeOpenSetup`, and from before `maeCloseSession` through owned child/guardian return |
| `model_dependency_freeze` | Captured model identity resolution, template/numeric dependency snapshots, model adaptation, fresh artifact hashes and frozen model-profile checks |
| `native_batch_capture` | Native generation capture after qualification fallback, or directly for numeric Maestro; scalar fallback captures use the same stage |
| `other` | Workspace/session borrowing, locks, planning, reconciliation, validation, progress and other preparation work |

Session startup includes child initialization and schematic patch loading before
opening the setup. Parent epoch time and fresh native marker-file mtimes delimit
session intervals; precision is limited by filesystem timestamp granularity.
The startup lock's existing `lock_wait_seconds` is transferred to `other`.
Missing or out-of-window markers leave that command in its enclosing capture
stage, set session duration to null, and increment `session_missing`. Observed
partial session seconds and sample counts remain explicit. Invalid clocks produce
unknown durations, never a successful timing claim. No polling, processes, native
replay or new qualification gates are introduced; marker writes use `errset`.

These stages are nested within existing generation `netlist_prepare` and candidate
build/setup intervals. Do not add them to their parent wall time or add the
candidate shared sums to the generation copy. Focused diagnostic reports carry
an observation-only `netlist_preparation` sibling beside F1–F4; their verdicts and
thresholds are unchanged. The normal generation overview also carries the data.

## Reproduction and interpretation

Run `python3 -B scripts/benchmark_preparation.py`. The developer fixture replaces
only native child capture, simulating 10 ms startup, 30 ms capture and 10 ms close.
Real symbolic qualification, dependency/model freezing, artifact hashing and
manifest validation run for both frontends. It preserves all four template
references. It is a synthetic cost example, not an EDA performance prediction.

The supplied two historical runs show a first-generation difference of about
175 seconds (and about 184 seconds in the prior run), while later preparation
costs converge. Their template mode retains four qualification capture artifacts
and subsequently uses generation-batch artifacts. This supports a one-time
qualification/fallback cost as the main explanation. Exact session, hash and lock
shares are **unproven**: archived successful capture commands report null durations
and have no session boundary markers. New runs are required for measured shares.

## Safe optimization and further work

The final freeze pass reuses its freshly computed netlist hash for the task's
netlist identity instead of reading the same path twice. It never reuses an old
capture hash. Independent manifest validation still rereads inputs and detects
post-freeze changes. Hashes across captures, candidates or model-profile checks
are not cached because freshness and mutation boundaries differ.

Reusing one live native session across the four reference captures could remove
repeated startup costs. This requires separate qualification of native reset,
CDF/callback state, history isolation and owned cleanup; it is a recommendation,
not an implemented semantic change. Similarly, sharing model digests across
captures needs an explicit immutable-snapshot contract. Keep the current four
independent reference captures until those guarantees are demonstrated.
