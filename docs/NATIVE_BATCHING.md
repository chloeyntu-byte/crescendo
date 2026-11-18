# Native Run generation batching

New runs default to `native_candidate_batching: "generation"`, including when
an engine configuration file omits the field. Restoring GUI settings without
the field keeps the current selection, or uses `generation` if the selector is
empty. To keep new runs serial, select `serial` in Pro Settings, set
`native_candidate_batching: "serial"` in the run spec, or set
`runtime.native_candidate_batching: "serial"` in the engine configuration. The
run spec takes precedence over engine configuration.

Resume uses the checkpoint's frozen batching setting. A checkpoint without the
field keeps its original serial behavior. Generation runs freeze the setting and
`crescendo.generation_batch.v1` in the spec and checkpoint manifest. Netlist
modes also freeze the mode-specific `native_batching_protocol` described below.

Native Run requires a Command Job Setup. A spec frozen as `serial` runs one
candidate at a time even when Maestro `max_jobs` is higher. That value controls
Maestro jobs within a Run. Global design-variable columns are supported, subject
to the batch qualification and precision checks below.

One generation borrows and resets the working copy once, opens one owned Ocean,
checks that Maestro and XL refer to the same session, writes equal-length Device
Parameter columns, and creates one parametric set. Native getters verify every
column, set member and enable switch; the original corner schedule is verified
independently. Existing source sets and unmatched parameters are preserved by
refusing batch qualification. Worker reuse and prestart are disabled for the
batch policy: the local IC618 fixture demonstrated a stale previous-row netlist
with worker reuse. Run Options otherwise retain their saved values.

The batch manifest is durable before a single native Run. Submission intent is
written before sending that Run. Capability/session/member/readback failures can
fall back to serial only before this intent and after cleanup is proved. The
status message and `batch_fallback_<epoch>.json` record the reason. Any exception
after intent fails the batch, including controller crashes and missing or extra
points; it cannot trigger serial replay.

Mapping reads the exact run ID and history, native design-point conditions,
corner device parameters and results directories. Every physical ID contains
the epoch, run, history, native design ID, Test, corner and resultsDir. RDB IDs
are checked against these fields, never calculated from a candidate ordinal.
Each partition still passes the existing corner/sweep, PSF, solver terminal,
output, source and netlist parameter gates before the ordinary metric aggregator.

For Native Run, both Global design variables and device parameters use the
same six-significant-digit limit. IC618 Device Parameter sets were observed to
round to seven digits. Before saving a new generation plan, the optimizer
runner rounds continuous values to six digits and directs rounding inward at a
bound. Stepped and integer values use the exact decimal point at
`grid_origin + k * step`, with the lower bound as the default origin. Float
arithmetic tails are removed without shifting to another grid index. A grid
point that needs more than six significant digits makes that candidate
ineligible for Native batching. Numeric enum choices keep their fixed values.

A value that cannot be prepared keeps its original proposal, while independent
values and other candidates keep their successful quantization. Linked
continuous members are projected from the rounded anchor and checked with the
optimizer's shared-coordinate rules. If a linked group fails, its members keep
their original proposals together. The runner checks the frozen values before
submitting the batch and uses scalar runs for the pending generation if any
candidate still fails qualification. Previously persisted plans and explicit
diagnostic values retain their recorded inputs and use scalar runs if those
inputs would change in a batch.

The fallback JSON and run log identify offending candidates and parameters
with the original value, the attempted decimal value when available, and the
reason. Precision failures on grid values include the six-digit rounded value
alongside the exact grid point. The generation timing and run log report
whether Native batching completed for each generation. The dataset, checkpoint
and optimizer use the same values as execution, including after a setup
qualification fallback. Differential evolution retains each trial's original
target in candidate order. Native condition, corner and netlist checks require
numeric equality for batch inputs.

Duplicate logical candidates have an explicit many-to-many forward/reverse
physical mapping, including candidates that coincide after rounding. Sharing requires deterministic analyses and a
closed, model-free analog primitive design; external models or unknown/random
semantics fall back to independent serial Runs. Every logical candidate is
persisted and included in the ordinary per-generation tell. There is no cache
across generations.

Candidate commits reach the core data and checkpoint before their shared-history
reference is released. Each batch has a durable consumer ledger; compaction
cannot proceed while any consumer is uncommitted. Shared native histories and
PSF retain Native Run's existing evidence-retention policy, outside individual
candidate directories. This release does not introduce eager deletion of them.
Shared setup/Run/reader/cleanup phases appear once in `batch_timing.json`;
candidate timings refer to it. Generation summaries include logical candidates,
physical rows/points and native policy capacity. Unobserved solver peaks stay
unknown.

Stop requests the owned session's Stop/Wait/Close and proves local process cleanup.
For Command jobs, all observed submissions must have complete, unambiguous Job ID
receipts and exact terminal results from the configured generic
`scheduler_query_template`. A missing job, process exit, or cancel acknowledgement
cannot substitute for terminal evidence. Otherwise `cleanup_pending` blocks
Resume. Resume reconciles the previous owner before creating or borrowing a
suite, skips only durable completed candidates, and assigns a new epoch to all
unfinished candidates. The optimization ask/tell barrier is unchanged.

## Native startup recovery

Before the first Native Run stage marker, the engine watches the owned Ocean
logs. The exact stderr line `sh: cdsCrashReport: command not found` enables
crash detection. Any stdout progress restarts the quiet window, and a startup
stage marker or a Native protocol record disables the check. Without the
signature, the regular operation deadline still applies. On the affected IC618
installation, the Ocean launcher execs Virtuoso and its native crash handler
invokes the missing helper through the shell with `-nograph`. The
`CDS_ERRORLOG` controls exercised in signal probes did not establish a fix for
the startup stall. Native Run uses the observed stderr signature.

| Runtime setting | Default | Meaning |
| --- | ---: | --- |
| `native_startup_crash_quiet_seconds` | 15 | Required uninterrupted stdout quiet time before declaring a startup crash. |
| `native_startup_crash_retries` | 1 | Additional startup attempts. Set to 0 to disable retry. |
| `native_cleanup_proof_timeout_seconds` | 15 | Time to poll the owned guardian after closing its input for cleanup. |

In the IC618 incident, eight healthy candidates reached READY in 4.24–6.47 s.
The final guardian receipt followed the pre-TERM `terminating` snapshot by
5.25 s. The 15 s defaults allow margin over both observations.

Each retry gets its own nonce, logs and guardian receipt. Native Run retries
only before submission and after the guardian proves process and descendant
cleanup. Exhausted startup retries record the first crash in
`infrastructure_failed` and let the run evaluate the other candidates. Missing
cleanup proof blocks the run. Once submission intent exists, the existing
native Stop and remote job evidence requirements still apply.

## Maestro Netlist and Template Netlist

New `maestro_netlist` runs freeze
`native_batching_protocol: "crescendo.maestro_netlist_batch.v1"`. New
`private_netlist` runs freeze `"crescendo.private_netlist_batch.v1"`. The marker
is bound into both frozen spec and checkpoint manifest. Unknown versions and
manifest/spec mismatches fail checkpoint validation. A resumed Netlist
checkpoint without the marker executes each candidate independently, including
older checkpoints that already saved `native_candidate_batching: "generation"`
and `crescendo.generation_batch.v1`. Resume preserves those frozen fields and
keeps the marker absent. Historical Native Run generation checkpoints retain their
existing generation behavior.

`maestro_netlist` uses the same generation schema, native columns, owned set,
exact-history map and `native_candidate_batching` selector. A generation-level
prepare barrier borrows and resets the applied working copy once. Its one Run
uses Local maxjobs=2 with worker reuse and prestart disabled. The owned simulator
wrapper records cwd/argv and exits 1. Full native/RDB point coverage, per-row
corner coverage and native parameter application must pass despite every
intentional wrapper failure. Capture records `capture_only` and
`crescendo.generation_capture.v1`; `submitted` denotes the later CLI submission
intent. Each physical row gets independent SCS, solver argv, AHDL output paths
and a verified model/include/generated-file closure. Every row passes the common
CLI input, output, command and resource checks before the first CLI task starts.

CLI execution retains Global/Test/Local semaphores, `local_solver_max_jobs` and
the controlled reader pool. A shared future executes each qualified physical row
once. Duplicate logical candidates retain separate records and optimizer tell
entries with explicit forward/reverse physical mappings. The consumer ledger is
released after each logical candidate reaches the core store and checkpoint.
Successful shared CLI candidates retain independent `candidate.json`,
`completion_evidence.json`, `timing.json` and `artifact_summary.json` files.
`shared_with`, `physical_execution_candidate` and the batch physical IDs identify
their shared solve. The artifact summary contains the retained file sizes and
SHA256 hashes. Raw candidate workspaces are retired after their existing process
and result proofs. Candidate summary compaction waits until every logical
consumer has committed to the core store and checkpoint, then completes for all
successful consumers. Earlier commits carry `state: "waiting_shared_cleanup"`
with `ok: true`. Interrupted batches retain that waiting evidence until Resume reconciles the old
owner. Resume explicitly retires unfinished consumers of the old epoch, preserves
the ledger and logical audit files, and completes cleanup for durable successful
consumers before starting a new epoch. Retired consumers are not successful commits. Final shutdown preserves the summarized files and their
hashes. There is no cross-generation cache. Shared capture/freezing time appears once
in `batch_timing.json`; candidate timing references it.

Capability, condition mapping or coverage failures can downgrade to scalar
capture after owned capture termination has been proved, while the generation
still has no CLI submission intent. The fallback JSON records the reason. All
scalar inputs are frozen and checked before the downgraded CLI stage. A Stop
retains the full uncommitted plan. Resume reconciles capture guardians and CLI
ownership first, keeps durable completed logical candidates and gives pending
candidates a fresh generation epoch. Failures after CLI intent cannot select a
new generation strategy.

Template Netlist uses `materialize_candidate` when template qualification passes.
Only its existing `native_numeric_derived_parameters` and
`native_selected_parameter_binding` evidence permits native generation fallback.
Those cases enter the generation barrier above. Unknown expression, topology,
source or termination failures retain their existing qualification gates.
Static file identities and frozen artifacts are checked per point before CLI.
Changed include contents, locations or generated SCS invalidate the affected
input. A later capture discovers its own include tree and structure again.

Template qualification already borrows one working copy once for all four
reference captures, with no intervening suite reset. The untouched reference
establishes the actual seeds before numeric/low/high references are constructed.
Each capture currently owns a short-lived Ocean guardian, closes its session,
and proves termination before reference comparison. This implementation retains
four Ocean launches. Reusing a live Ocean would require extending that capture
lifecycle and its per-reference failure/termination evidence. The current suite
reuse preserves all four independent reference comparisons.

## Audit and Stop prerequisites

The native batch manifest includes the column/set enable readback, unchanged
corner list, native corner schedule and effective job policy. Maestro captures
also include per-row corner coverage and all-point wrapper coverage.

Each generation borrows and resets the same applied working copy. The suite is
retained across generations. For native Command Runs, omitting
`scheduler_query_template` leaves insufficient terminal evidence when Stop
interrupts a Run. Such ownership falls into `cleanup_pending` and blocks Resume
until exact job terminal and local cleanup evidence are available.

Design-variable columns use the versioned source identity and native variable APIs described in [Design variables](DESIGN_VARIABLES.md). Maestro Netlist and Template Netlist capture retain their existing column format.
