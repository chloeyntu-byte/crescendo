# Execution timings and parallelism

The three execution modes use the same frozen optimization samples and selected corners in link diagnostics. Candidate capacity, point-task slots and solver threads are separate limits.

## Parallelism

**Native Run** uses Maestro's Run operation in a working copy, with the source simulator and effective Command Job Setup inputs. New configurations default to a generation batch; old checkpoints and explicit `serial` settings retain per-candidate Runs. Native policy controls dispatch within each Run.

**Template Netlist** and **Maestro Netlist** share the CLI solver pool. Effective capacity obeys Global/Test limits, population and explicit Local solver slots (`local_solver_max_jobs`, default 1). The reader pool has its own bounded capacity. Configured limits are not measured peak concurrency.

Template Netlist qualifies a frozen template, then materializes candidate values. Qualified cases that require native generation use the Maestro capture path. Generation mode freezes and verifies all native inputs before shared CLI execution; serial mode captures candidates individually and can overlap solving after the generation lock is released. See [native batching](NATIVE_BATCHING.md) for fallback, shared-history and Stop/Resume rules.

Cluster execution continues to use user-configured submission, query and cancel templates and the scheduler key-value protocol described in [Using with Cadence](../README.md#using-with-cadence). Timing does not infer a scheduler command, path or option.

## Reading the timings

- **Wall time** is the actual observed elapsed interval.
- **Candidate phase sums** accumulate the same phase across candidates. Parallel intervals can overlap and their sum can exceed wall time.
- **Solver elapsed** comes from an exact task's independent native Spectre log, including solver startup, parsing and computation. It is neither CPU core time nor total workflow duration. Missing, ambiguous or shared-log evidence stays unknown; coverage counts show how many tasks supplied evidence. Unsupported solver log formats also remain unknown.
- **Scheduler queue** stays unknown when trusted scheduler timestamps are unavailable. **Observed scheduler pending** records only intervals between consecutive exact-job PENDING/QUEUED observations from existing polls; it cannot reconstruct the real scheduler start time. Submission-to-completion is not queue time.
- Nested phases and point/candidate sums must not be added to wall time. Native Run overhead that cannot be separated retains its observed boundary rather than being labelled pure solving. Shared batch phases are counted once per batch in generation aggregates.

Every generation records ask/plan, evaluation, tell, settlement, checkpoint persistence, candidate persistence and candidate cleanup. Candidates record observable build, lock/setup waits, native generation or template materialization, execution capacity, simulation lifecycle, result collection, metrics and cleanup. These are passive observations of existing events and result reads: no additional simulation polls, scheduler probes or timing threads are introduced. Unavailable clocks or timing sidecars must not invalidate a candidate or prevent cleanup.

In **Live Results**, select a candidate for its main phases, then click **Timing details...** for per-task and per-generation details. Loaded or diagnostic results use their own timings without replacing the formal run or borrowing another run's timings. While a run is active, status retains the latest 20 generations; final results retain all recorded generation summaries. Resumed generations are labelled as resumed segments. Old results without detailed timing display unavailable; pure solver time cannot be reconstructed from a coarse execution duration.

## Link diagnostics

Diagnostics retain the Native Run → Template Netlist → Maestro Netlist order and the same frozen samples for each mode. The default is two generations and six candidates per mode. The diagnostic page accepts 1–10 generations, 1–16 samples per generation and at most 64 candidates per mode; total candidates must divide evenly by generations. These settings can be saved/restored and do not change formal optimization size. A small finite search domain may repeat candidates, so distinct-candidate counts are reported separately.

Choose a registered diagnostic mode and view/refresh its real results in Live Results. Parameter/objective values remain in result datasets; the concise report contains mode summaries, first causes, effective parallel limits, evidence coverage and short generation tables rather than copying full per-candidate logs or optimization arrays. Independent failures proceed to the next mode only after stop is proven; failures or unproven stops retain their original context. Advanced statistics and export round-trip checks remain deferred.

Mode totals span readiness, registration, execution, verification, cleanup and evidence collection. Modes run in sequence, but tasks within a mode can overlap. Coarse execution includes queueing, generation, solving and measurement; use detailed phases for narrower comparisons. Total UI diagnostic time also includes report finalization.

## Retained GUI and process fixes

Returning to Parameters completes a cancelled time-sliced selection refresh. Cached Canvas primitives retain their original drawing order when a primitive changes type, while unchanged drawing still avoids redundant writes. Scope indexes, cached views, background checks, incremental results and the 64 MiB full-configuration limit remain in place.

A guardian wrapper exiting does not prove both pipes reached EOF or that its owned descendants stopped. Output drains under a bounded allowance while the command deadline and Stop remain active, retaining partial bytes across supported Python versions. Concurrent cancellation fallout cannot replace the originating execution or measurement failure.

## Site performance and rollback

Whether 60 candidates finish within ten minutes depends on solve time, corner count, effective concurrency, queue wait, native generation, storage and readback overhead. Use timing evidence from your simulator, scheduler, PDK and filesystem. Synthetic fixtures and controlled Tk/subprocess tests do not establish real-site throughput.

Keep the previous program and its matching runtime data when upgrading, and exit normally before replacing program files. Saved Configuration restores settings; checkpoints retain their existing protocol. To roll back, restore the previous program and corresponding data without deleting simulation evidence. The fixes do not relax source identity, parameter, corner, result or owned-stop verification.
