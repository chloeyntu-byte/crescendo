# Native Baseline generation batching

New configurations select `native_candidate_batching: "generation"`; Pro Settings
can select `serial`. Old configuration files without the field, and old
checkpoints without the setting keep `serial`. The frozen spec and checkpoint
manifest carry the setting and `crescendo.generation_batch.v1`. Netlist modes
also freeze `native_batching_protocol` with the mode-specific version below.
Baseline continues to require a
Command Job Setup.

One generation borrows and resets the private suite once, opens one owned Ocean,
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

Duplicate logical candidates have an explicit many-to-many forward/reverse
physical mapping. First-edition sharing requires deterministic analyses and a
closed, model-free analog primitive design; external models or unknown/random
semantics fall back to independent serial Runs. Every logical candidate is
persisted and included in the ordinary per-generation tell. There is no cache
across generations.

Candidate commits reach the core data and checkpoint before their shared-history
reference is released. Each batch has a durable consumer ledger; compaction
cannot proceed while any consumer is uncommitted. Shared native histories and
PSF retain Baseline's existing evidence-retention policy, outside individual
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

## Private Maestro and Private Netlist

New `maestro_netlist` runs freeze
`native_batching_protocol: "crescendo.maestro_netlist_batch.v1"`. New
`private_netlist` runs freeze `"crescendo.private_netlist_batch.v1"`. The marker
is bound into both frozen spec and checkpoint manifest. Unknown versions and
manifest/spec mismatches fail checkpoint validation. A resumed Netlist
checkpoint without the marker executes each candidate independently, including
older checkpoints that already saved `native_candidate_batching: "generation"`
and `crescendo.generation_batch.v1`. Resume preserves those frozen fields and
keeps the marker absent. Historical Baseline generation checkpoints retain their
existing generation behavior.

`maestro_netlist` uses the same generation schema, native columns, owned set,
exact-history map and `native_candidate_batching` selector. A generation-level
prepare barrier borrows and resets the applied private suite once. Its one Run
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

Private Netlist uses `materialize_candidate` when template qualification passes.
Only its existing `native_numeric_derived_parameters` and
`native_selected_parameter_binding` evidence permits native generation fallback.
Those cases enter the generation barrier above. Unknown expression, topology,
source or termination failures retain their existing qualification gates.
Static file identities and frozen artifacts are checked per point before CLI.
Changed include contents, locations or generated SCS invalidate the affected
input. A later capture discovers its own include tree and structure again.

Template qualification already borrows one private suite once for all four
reference captures, with no intervening suite reset. The untouched baseline
establishes the actual seeds before numeric/low/high references are constructed.
Each capture currently owns a short-lived Ocean guardian, closes its session,
and proves termination before reference comparison. This implementation retains
four Ocean launches. Reusing a live Ocean would require extending that capture
lifecycle and its per-reference failure/termination evidence. The current suite
reuse preserves all four independent reference comparisons.

## Audit and Stop prerequisites

The native batch manifest includes the column/set enable readback, unchanged
corner list, native corner schedule and effective job policy. Private captures
also include per-row corner coverage and all-point wrapper coverage.

Each generation borrows and resets the same applied private suite. The suite is
retained across generations. For native Command Runs, omitting
`scheduler_query_template` leaves insufficient terminal evidence when Stop
interrupts a Run. Such ownership falls into `cleanup_pending` and blocks Resume
until exact job terminal and local cleanup evidence are available.
