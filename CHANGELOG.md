# Changelog

## Unreleased

## 1.4.5 (2026-11-18)

- Make run-data generation audit tests deterministic on coarse filesystem clocks.
  Cover a timestamp change during the scan and checksum rejection of a
  same-length rewrite made before the scan with unchanged timestamps.
- Native Run penalizes verified Calculator failures by output and corner, retains teardown diagnostics alongside valid results, retries private ADE directory errors after proven cleanup (runtime `native_directory_retries`, default 1), waits briefly for Spectre terminal logs before closing Maestro, and reports termination and timeout details.
- Fix launching Crescendo from Maestro on IC 6.1.8 by atomically creating a private local IPC directory with exclusive launch script and context files.
- Add an interactive, retained Canvas 3D Pareto view for three objectives, preserving pairwise projections and candidate selection. Include a deterministic curved-surface demo with HD rotation/generation PNG exports and compact JSON for web playback.
- Fit the projected 3D box to the viewport, tighten data bounds and ticks, show an optional translucent front mesh and generation depth cues, and export a 16:9 hero plus web triangles. Wrap sidebar version information.
- Engine version is 3.4.0.

## 1.4.4 (2026-11-18)

- Default new runs to generation batching when the engine configuration omits the
  setting. Restoring settings without it keeps the current GUI selection. Explicit
  serial settings and frozen checkpoint behavior are preserved.
- Native Run qualifies library initialization diagnostics against completed
  candidate points and continues after isolated completion failures with proven
  cleanup. Generation batches retain the outcome of each mapped candidate.
- Normalize Native Run optimizer grid values in decimal and retain valid
  quantization when another input cannot be represented. Record the candidate
  and values that caused serial fallback, and report batching per generation.
- Engine version is 3.3.4.

## 1.4.3 (2026-11-18)

- Detect stalled Native Run startup when Ocean reports the missing Cadence crash
  reporter, retry after bounded owned-process cleanup, and continue with the
  remaining candidates when startup retries are exhausted and cleanup is proven.
- Show a non-blocking advisory for selections over 50 variables in Native Run, Maestro Netlist, and Template Netlist. Run logs and diagnostics include the mode-specific advice, and Template Netlist repeats it after a reference capture crash.
- Native Run candidate batching stays as configured: runs whose specification sets `native_candidate_batching` to `serial` evaluate candidates one at a time; set it to `generation` (or `runtime.native_candidate_batching` when the specification leaves it unset) to batch each generation.
- Engine version is 3.3.3.

## 1.4.2 (2026-11-18)

- Report stackless ICRP library initialization errors as warnings only when the
  same Ocean capture proves their origin and the matching worker point
  completes netlisting with zero errors.
- Engine version is 3.3.2.

## 1.4.1 (2026-11-18)

- Preserve source config view and stop lists in working copies, keep per-occurrence
  bindings and stops explicit, and reject lists over 3,500 bytes before IC618 netlisting. Long hierarchies with over a hundred optimized instances previously crashed the netlister. Engine version is 3.3.1.

## 1.4.0 (2026-11-18)

- Persist one completed candidate from Native Run, Maestro Netlist, or Template Netlist as new schematic views. Show verified instance writes, name collisions, skipped design controls, and failed cells in a copyable report.
- Keep external EDA artifacts in the environment identity when unrelated directory entries change during reads. Detect directory replacement, symlink changes, and file changes as before.
- Report Cadence-caught SKILL library initialization errors as warnings when the native stack and matching stage completion prove the recovery. Retain the original stack in capture and compact diagnostics.
- Round continuous Native Run generation inputs to Maestro-safe precision before recording the plan. Verify exact native conditions, corner values and netlist values, and use scalar runs when batch precision would change fixed inputs or break linked ranges.
- Report unrelated Ocean tool registration and initialization messages (ADE-5066 and ADE-5067) as warnings. Preserve their native text and keep active simulator failures in the first-cause diagnostics.
- Engine version is 3.3.0.

## 1.3.2 (2026-11-18)

- Restore original first-cause text in each compact mode verdict, retaining a separate first-cause reference. Bound verdict lines to 200 characters with an explicit truncation marker, normalize whitespace and apply existing secret redaction; full reports and lossless sidecars retain their formats.
- Synchronize process fixtures with consumed descendant output and atomically published simulator PIDs. Verify owned group signals, finite cleanup wait budgets, closed pipes, cancellation identity and proven cleanup instead of treating host scheduling delays as product latency. Preserve execution, timeout and Stop policies; engine version is 3.2.2.

## 1.3.1 (2026-11-17)

- Fix compact report format markers under injected runtime identities so copied sections retain run roots, first causes and verified evidence manifests. Bind evidence schemas/filenames, instance-path and scoped-discovery schemas consistently.
- Match mixed Global/device batch template transformations to the bound SKILL symbols and retain aliased solver control evidence. Add fictional legacy identity round trips for compact/full reports, sidecar integrity, run data v2 evidence/query/deltas, F2/F3, configuration and native batch templates. Default naming, registry identity and execution gates stay unchanged; engine version is 3.2.1.

## 1.3.0 (2026-11-17)

- Split netlist preparation into exclusive template qualification, one-shot session lifecycle, model/dependency freeze, native generation capture and other/lock intervals. Carry versioned passive observations through candidate/generation timing and focused diagnostic reports, deduplicating shared generation fanout and retaining incomplete coverage on failure.
- Reuse the final freeze pass's freshly read netlist SHA256 instead of hashing that same artifact twice; retain independent manifest verification. Add synthetic frontend timing evidence and regression coverage without changing native qualification or execution semantics.
- Restrict point-receipt projections to receipts with explicit `candidate_completed: false` and a token, matching reopened writers and readers. Preserve other point receipts through ordinary evidence storage.

- Restore F3 v2 UNKNOWN counts from complete durable query logs, deduplicate originals across aliases and overlapping snapshots, and retain explicit census completeness and legacy v1 accounting. Clarify that the E5 43x size reduction includes zlib; evidence deduplication and structural reductions alone give about 11x.
- Surface sidecar integrity failures as report delivery warnings and in the diagnostic card without changing diagnostic verdicts. Validate diagnostic example counts (0–20) at configuration, spec and GUI boundaries; localize Pro log controls. Cover compact focused-summary parity and queue overflow in both report modes.

- Use versioned run data v2 for new runs: content-addressed evidence in the durable JSONL prefix, point-receipt projections, pipeline alias removal and terminal candidate deltas. Keep existing v1 bytes and Resume format unchanged.
- Retain every scheduler query original with entry/segment hashes while inlining only first/last bodies; verify lazy audit references before cleanup and portable export. Preserve exact Stop proofs, partial-candidate Resume and per-generation full-prefix audits.
- Support v2 portable checkpoints, pending journals, whole-run retention and legacy schema/dialect aliases; teach F3 to resolve v2 point identities without changing physical byte accounting.
- Default diagnostic logs to compact summaries with mode/generation phase tables, bounded focused-check examples, merged warnings and lossless content-addressed JSONL sidecars. Verify every sidecar size/hash, carry the manifest in report-ready events, and retain legacy inline output with `runtime.diagnostic_log_detail=full`. Expose detail/example preferences only in Pro mode.
- Separate native diagnostic identity budget exhaustion from unsafe metadata, retry bounded atomic replacement races without increasing read limits, and refresh post-lifecycle shutdown evidence before advancing the diagnostic ladder. Preserve strict Stop/Resume gates and report missing process, remote terminal and retention proofs.
- Add Pro instance-path entry with independent line validation and branch-only OA discovery, including exact scalar bus names and leaf devices. Preserve selected scopes in versioned configuration metadata and existing checkpoint identities, and version scoped discovery so reconstruction retains its original hierarchy; retain the tree-selection workflow and default runtime registry.
- Coalesce Pareto resize events and draw cards into one retained page canvas so wheel/scrollbar events only move the viewport. Invalidate only changed plots, retain candidate clicks, and display the selected corner name for a single corner. Keep resize timers owned by the root so host-wide cancellation and child teardown remain safe on Tk 8.5/8.6.
- Resolve ancestor instance names directly, materialize only requested synthetic parameter records on cold manual entry, and retain native startup/ancestor costs explicitly in the path documentation.

- Keep selected scalar outputs and scalar dependencies in F2 comparisons when idle or required `value` expressions reference them; prefer finite scalar readback over waveform type inference so differences and missing observations remain visible.
- Preserve the pinned headless import set with lazy Native status parsing; keep legacy missing-state evidence unclassified and show unapplied tolerance policies for missing required outputs.
- Retain CLI dependency observations and per-output collection/non-finite/evaluation states, preserve reader first errors in candidate evidence, and prevent failed dependencies from reusing stale aliases in single and batch readers.
- Scope F2 to selected scalar metrics and dependencies, separate waveform/idle outputs, report actual candidate counts and unit provenance, and use magnitude-aware relative tolerance for unknown units.
- Project frozen diagnostic candidates, plans and verification expectations onto scope-qualified surviving design dimensions. Retain point order and values, verify original digests, and reject cross-mode projection differences.
- Add explicit Global and top/Test design controls in the Pro parameter catalogue, with current values and source labels. Preserve device-only configurations and checkpoint fingerprints.
- Apply selected design controls through owned native Global columns and parametric sets; qualify generated top-level declarations and symbolic templates with native reference captures. Reject selected-variable corner collisions, retain single-value Global/Local guards, and lock device dimensions using exact lexical/alias dependency evidence.
- Persist design-variable source/Test identity with `crescendo.design_variables.v1` and opt mixed native plans into `crescendo.native_parameter_plan.v2`.

## 1.2.0 (2026-11-17)

- Freeze linked diagnostic current values as a complete group; fall back to a seeded legal group when members are missing or inconsistent. Enumerate independent enum choices without duplicates when filling linked diagnostic grids.

- Add optional parameter `link_group` in Crescendo 1.2 / engine 3.1. Linked members share one search coordinate across all four algorithms; ask returns every member mapped to its own linear/log range or equal-count grid. Support mixed linear continuous/stepped groups; reject enum linking with group/member errors. Preserve unlinked v1.1.4 candidate bytes with a golden contract fixture.
- Retain optional `step`, `grid_origin` and `link_group` through unified configuration, Restore Last and working-copy preferences. Add validated Link Selected / Unlink Selected controls and group labels in Pro mode, with English translations.
- Save linked runs as checkpoint v2 so older versions reject them; retain v1 for unlinked runs. Include group membership in continuation identity, dataset variable metadata and optional per-member CSV columns. Cover complete ask/tell expansion, seed reproducibility, stop/resume, exports and configuration/UI round trips.

## 1.1.6 (2026-11-17)

- Match both hierarchy-flattening and initial-setup Spectre transient crashes only when the current attempt also contains the internal-error marker and solver error code; keep deterministic crashes bounded by the configured retry budget.

- Exclude pre-launch log bytes from transient crash signatures, including appended unsigned errors and keywords crossing the attempt boundary; deduplicate passive recovery counts for shared physical executions by attempt identity.

- Retry profile-qualified transient solver crashes through the shared CLI path (two retries by default), retaining each attempt's logs, first error, timing, token and exact scheduler evidence. Exhaustion is an infrastructure failure without design penalties. Add frozen recovery preferences, Pro controls, an optional Local single-CPU last retry and passive per-mode diagnostic counts; old checkpoints retain absent fields and allow the default recovery policy.

- Reconcile interrupted recycle journals during service startup, list refresh and repeated Delete; account for every registered view before marking receipts complete.

- Reject unsafe capture roots and phase directories during isolation checks; preserve all live views when native shutdown evidence is malformed.

- Verify isolated working copies at service startup and list refresh using native process identity, OA locks, leases and exact ownership. Retain verification archives, expose structured block reasons in both GUI presentations, and support evidence-preserving recycling without moving locked or actively owned views.
- Cache non-default runtime literal resolution with a bounded thread-safe LRU and resolve redaction contracts once per tree, preserving emitted bytes.
- Synchronize concurrency and owned-process regression fixtures with barriers and readiness handshakes instead of scheduler-dependent sleeps.

## 1.1.5 (2026-11-17)

- Integrate compact prepared catalogues and passive focused diagnostics with generation checkpoint audits.
- Bridge parent-owned Tk heartbeat windows and frozen report-contributor registrations into GUI service diagnostics using data-only RPC. Keep callbacks in their trusted startup process, apply report redaction/bounds before publication and retain an independent cancellation/response lane.

- Report F4 logical prepared bytes, current reference copies, reachable CAS objects/bytes and reference counts, and total/optimizable parameter counts. Retain explicit legacy dispatch and mark missing or corrupt current references incomplete; never scan historical objects or repair data during diagnostics.
- Test cooperative worker CPU slices against the existing 200 ms budget, retaining wall-gap distributions while distinguishing observer descheduling from worker stalls. Add a CPU-stall negative control and a delayed-observer control. Product heartbeat thresholds remain unchanged.

- Allow concurrent prepared publishers to create their shared content directory without a check-then-create race; retain directory ownership and symlink checks.

- Preserve command cancellation types and cleanup/import evidence across GUI service RPC so cancelled configuration loads keep unsaved interface settings.

- Isolate fresh native GUI services in an owned process. Keep full legacy catalogues, original identity checks and cyclic GC outside Tk, transfer explicitly versioned non-executable summaries, and preserve concurrent control and cleanup evidence. Headless and caller-owned preloaded services retain their original lifecycle.

- Reduce large setup catalogues to eligible controls and share content-addressed prepared, hierarchy and parameter objects. Stream compact JSON and canonical hashes to keep worker operations responsive. Legacy suites/checkpoints retain their original schema and identity validation.
- Filter CDF candidates before editor-condition evaluation, reuse read-only editor contexts, preserve instance-specific qualification, and record native/parse/phase timings.
- Bound discovery, model scanning and native-script validation work between GUI scheduling yields. Precompute setting identity summaries, and retain parameter metadata when selectively updating compact suites.

- Recover cross-mode diagnostic output units from the frozen Setup and scope expected points to this run's corner selection and per-Test masks, without changing execution or qualification.

- Add observational focused diagnostic checks for generation checkpoint audits, raw cross-mode results, current-run dataset volume and Setup/Tk responsiveness; retain all phase timings and existing qualification gates.
- Add bounded, ordered, immutable-input diagnostic report contributors for applications and scheduler providers, carried by report exports and report-ready payloads.
- Retain raw output points and complete scheduler query-state counts only during diagnostics. Dispatch prepared statistics by format, including compact v2 and shared content-addressed objects.

## 1.1.4 (2026-11-16)

- Scope native-log readers and cleanup to overlapping paths, including resolved aliases. Preserve physical candidate timings during generation fanout and expose batch counts.
- Record generation netlist preparation and nested checkpoint/storage operations, with calls, elapsed time and bytes in diagnostics and Timing details. Use the startup application identity for the engine window title.
- Compute checkpoint SHA256 references incrementally from the exclusive writer's own byte slices, including short writes. Intra-generation checkpoints do not reread disk prefixes; identity, links and exact-size checks immediately reject and diagnostically scan replacement, truncation or external append.
- Verify the entire disk file once before each generation reaches optimizer `tell`. Reject mismatches or unstable scans, stop the run and retain the generation-commit first cause. Remove both post-append full scans and every-128-check audits; retain explicit full audits.
- Limit cached snapshots to the owning writer's checkpoint transaction. Ordinary first loads, other readers, new processes, existing-writer opens and Resume fully verify disk data before use. Keep v1 records, legacy identity/dialect, optimizer signatures and registry identity compatible.
- Residual risk: an external rewrite of an already written prefix can remain undetected until the current generation's commit barrier. Earlier intra-generation checkpoints reference the writer's own bytes. Checkpoints loaded for Resume or export undergo full verification, so damaged data cannot be used as valid history.
- Add exact byte-count, generation-rejection and reader-isolation regressions and a four-generation checkpoint benchmark with 32 synthetic 1 MiB point records. Per-generation reads comprise new record bytes plus one current-file audit.

## 1.1.3 (2026-11-13)

- Declare the optimizer a headless library: `crescendo_engine.optimizers` (`PopulationGenerator`, `validate_variables`, new `ALGORITHMS`), `metrics` and `engineering` import and run ask/tell without application startup, scheduler providers, bootstrap, GUI, Tk or Cadence, as root or an ordinary user. Documented in docs/HEADLESS_API.md and pinned by contract tests.
- `PopulationGenerator` rejects an unknown algorithm name when constructed instead of after the first generation.

## 1.1.2 (2026-11-12)

- Rename displayed execution modes to Native Run, Maestro Netlist and Template Netlist; use working copy for owned setup/view copies. Persisted backend IDs, configuration keys and checkpoint protocols are unchanged.
- Explain created views, ownership markers, storage defaults/overrides (including namespaced roots), lifecycle, reuse, recovery and disk cleanup in both READMEs; refresh screenshots.
- Clarify diagnostic and launch error wording (owner-only launch scripts and caches, sensitive-field redaction).

## 1.1.1 (2026-11-12)

- Tests only: widen positive synchronization and completion waits (event "entered" waits to 10 s, diagnostic manager joins to 30-60 s) so slower CI runners do not time out. Waits return as soon as the event fires, so passing runs are not slower. Waits that check latency or that a task must not finish yet are unchanged. No product code changes.

## 1.1.0 (2026-11-12)

Highlights:

- **Generation batching.** New runs evaluate a whole generation per native Run (Native Run) or per native capture (Maestro Netlist, and Template Netlist when native fallback is required), then fan results back to every candidate. On local real-EDA runs with matched samples (3 generations x 6 candidates x 2 corners) Native Run went from 433 s to 101 s (4.3x) and Maestro Netlist from 342 s to 166 s (2.1x), with identical objective values. Template Netlist template runs were already fast (148 s vs 139 s); with native fallback, batching took 130 s instead of 190 s.
- **Configurable throughput** in Pro mode: Local solver slots (default 1), reader workers, display flush interval and the batching selector. Old checkpoints keep their original serial behaviour.
- **Execution timings** per candidate, point and generation, with observed concurrency.
- **Engine extension points** (application identity, scheduler providers, adapter registry, GUI factory and diagnostic actions) for applications that embed the engine. Defaults are unchanged.
- Target-site qualification is still required for batch schedulers, complex process models, dynamic includes and large-scale throughput.

Details:

- Explain the worker bootstrap ownership/mode gate and owner-account or isolated-copy workflows for root maintenance.

- Document trusted provider/bootstrap configuration, the scope and limits of help-only execution, and engine-owned completion, Stop and cleanup authority.

- Report optional application versions separately from the engine; launch controllers at a registered application code root and recognize provider log filenames in diagnostics. Avoid repeating unavailable cancellation probes.

- Keep query-only scheduler observations separate from cancellation capability; preserve runtime identity when copying compact diagnostic sections.

- Bind storage-error annotation attributes and controller module commands to the selected runtime on both producer and consumer paths.

- Keep pure template policy validation independent of native environment construction. Preserve strict raw submission identity for receipt-limited provider capabilities without granting query or cancellation evidence.

- Support optional contextual provider capabilities with engine-bounded help-only probes, explicit deferred template defaults and unchanged frozen-command identity. Bind configurable process-directory field literals through the startup codec.

- Bind provider-declared workdir and stream argv positions before final submission freezing. Validate unique options, owned point boundaries and symlink-free normalized paths; share bindings with live logs and artifact collection.

- Preserve legacy four-key frozen command identities and resolve absent query/cancel capabilities through startup-selected providers. Validate original fingerprints, canonical field sets and exact owned-job templates.

- Add immutable scheduler enum aliases to the startup dialect, shared by checkpoint validation, execution policy, Template Netlist startup, settings and Pro restoration. Keep original persisted values and continuation hashes.

- Add explicit application GUI factories and immutable diagnostic action snapshots, writer-ready/progress/close events, main-thread button state and optional background activities.
- Add a guarded scheduler provider registry over the generic template protocol, manifest-backed application resources, startup adapter-registry replacement and validated adapter entry symbols. Keep the default adapter registry content identity unchanged. Bind validated provider log paths into remote submission templates while preserving relative-script defaults.
- Wire immutable engine identity/layout/dialect contracts through schema and storage producers/consumers, SKILL and native bridges, including versioned batch manifests. Preserve default literals and resource bytes and keep user values outside source-literal resolution. Add fictional legacy checkpoint/dataset and subprocess pairing regressions.
- Pin application-owned worker startup code separately from data-only descriptors; verify provider and injected adapter-registry content identities in workers and record them in diagnostics. Allow guarded scheduler argument preparation before final engine freezing.

- Finish durable successful shared-candidate cleanup on Resume after retiring old-epoch pending consumers. Preserve the ledger, manifests and logical audit files.

- Freeze mode-specific native batching protocols for new Netlist runs. Resume unmarked Netlist checkpoints with per-candidate execution even when their saved selector is generation. Validate protocol identity against the checkpoint manifest.
- Retain independent logical-candidate completion, timing and artifact-summary files with shared solve references. Defer shared CLI candidate compaction until every consumer is committed, record interrupted waits explicitly, and preserve audit hashes through final shutdown.

- Treat saved transient `noiseseed` metadata as inactive only when `noisefmax` and noise activation are absent/disabled. Active transient noise and other stochastic evidence still require serial execution; modeled duplicate sharing remains unqualified.

- Validate generation throughput with controlled solver/reader/native-group fixtures and matched local MOS-circuit runs across all three execution modes; report wall time separately from cumulative task service and shared stages.
- Clean reader control directories on all pre-launch preparation failures, including guardian launch-lock and manifest writes. Retain failed cleanup as unresolved ownership and preserve guardian evidence once ownership handoff begins. Clarify explicit reader-capacity reasons and test v4 checkpoint result reading and portable export independently of Resume authority.
- Document Pro batching, Local solver slots, reader workers and display flush defaults. Target-server qualification remains required for batch scheduling, complex process models, dynamic includes and the complete 10-generation × 6-candidate (two-corner) ≤600-second goal. Local evidence does not establish that target.

- Capture Maestro Netlist generations in one owned Local2 Run, freeze and qualify every point before shared CLI execution, and execute qualified duplicate physical rows once with durable logical fanout.
- Route qualified Template Netlist native-companion and selected-binding fallback through the same generation barrier. Keep template materialization, legacy serial settings, controlled readers and candidate-granular Stop/Resume.
- Persist native batch enable/corner readback and effective job policy, and document shared-suite reset and Command Stop query prerequisites.

- Advance native point qualification to v5 for lexical scopes and indirect aliases. Older native templates and manifests require fresh qualification before execution. Real shared-CLI checkpoints carrying v4 or missing qualification revisions are rejected on Resume, including completed and failed history. Their results remain readable/exportable. Start a new run to qualify inputs again. Legacy non-native and mock evidence keeps its compatibility.

- Verify execution-resource preferences in a real Crescendo window, including configuration round trips and mapped Pro/Simple visibility. Its fixture uses isolated preferences and an isolated process so presentation imports cannot affect later launcher or kernel-window tests.

- Preserve each point's reader timeout budget and split multi-point reads at a 3600-second batch ceiling. Single points retain longer explicit budgets. Remove shared reader controls after all point owners retire, release their parent directory after all candidate reader futures finish, and retain unresolved ownership evidence. Explicit reader concurrency is capped by Global capacity with requested/effective values in summaries.
- Limit display snapshot writes while preserving every incoming run-log event. Flush actual production terminal events immediately and verify log completeness with throttling enabled.

- Preserve manual EDA revalidation after a settings update: loading the cell hierarchy does not start the saved-setup automatic Apply chain or reacquire Run authority before the user applies the cells.

- Add versioned, once-per-generation Native Run batches with equal Device Parameter columns, exact session/set readback, durable submission intents and full-history point mapping. New configurations default to generation batching; old configurations and checkpoints retain serial execution.
- Keep Command semantics, fail-fast completion, candidate-granular Resume and shared-history reference ledgers. Stop requires native Stop/Wait/Close, local cleanup and exact Command job terminal queries; unresolved resources block Resume. Shared stages have one timing sidecar and logical/physical counts.
- Add a Pro batching selector and explicit pre-submission serial fallback. Source parameter sets, unknown/stochastic analyses and unqualified duplicate sharing preserve the serial path; submitted batches never replay serially.
- Neutralize native-stage fixture names and scan every tracked text file for application-specific library prefixes, foundry brands and site-specific scheduler terms.

- Resolve indirect native parameter aliases with bounded lexical dependencies shared by template qualification and point readback. Preserve numeric-companion fallback and report machine-readable binding, scope, expression and topology reasons.
- Add Pro configuration and frozen checkpoint settings for Local solver capacity (default 1), reader capacity (default min(2, G)) and progress flush intervals (default 0.5 seconds). Local capacity obeys Global and Test limits.
- Read up to four candidate points in one owned Ocean reader, isolate output aliases and current-result variables, and retain immediate point ACK and ownership persistence. New runs freeze the versioned batch-reader protocol. Older checkpoints keep their point-reader protocol.
- Coalesce display progress writes and record per-generation effective capacities with observed start/terminal concurrency peaks in timing sidecars.

- Complete interrupted parameter-selection refreshes when returning to the page, and retain cached Canvas drawing order when a primitive changes type.
- Drain guarded subprocess output within a bounded exit allowance, preserve partial output across Python versions, and retain the originating failure when peer cancellation follows it. Existing owned-process stop checks still apply.
- Record candidate, point-task and generation execution timings across all three backends, including setup/lock/capacity waits, native generation or template materialization, collection, metrics, cleanup and checkpoint persistence. Timing sidecars remain optional.
- Read solver wall elapsed only from exact independent Spectre logs; missing evidence stays unknown with coverage counts. Keep observed scheduler pending intervals separate from queue time and preserve the configurable batch templates and key-value protocol.
- Add **Timing details...** to Live Results in the Crescendo style, show candidate phases in diagnostics, and include effective parallel limits and concise generation timings in link diagnostics. New engine wording has English translations.
- Keep asynchronously updated labels in English and rank translation patterns by literal specificity consistently across Python versions.
- Document [execution timings, parallelism and diagnostics](docs/EXECUTION_TIMING.md). Real-site throughput needs site-specific qualification.

## 1.0.3 (2026-11-10)

- Range checks report integers too large for a float as non-finite on every Python version (Python 3.11+ refuses to print them).
- The saved configuration lists manually edited algorithm parameters in a stable order.
- Tests run on Python 3.6 to 3.13 with Tk 8.5 or 8.6, as a normal user as well as root: no writes to the shared `/tmp`, no dependence on a desktop file chooser, and fixtures that simulate another program's lock lift the read-only bit only while they write.
- The test runner keeps each Tk interpreter alive until the main thread releases it, so a slow worker thread can no longer abort the run.

## 1.0.2 (2026-11-10)

First public release (1.0.0 and 1.0.1 were not published).

- Remaining test fixtures use placeholder scheduler job IDs.

## 1.0.1 (2026-11-10)

- Cluster execution is a generic batch scheduler: submit, query and cancel are user-configured command templates; no scheduler executable is assumed.
- The optional visual-transfer log sender was removed.
- Test fixtures use generic host names, job IDs and user names.

## 1.0.0 (2026-11-10)

- Guided six-step window (Design, Parameters, Goals, Run, Results, Trade-offs) with Simple and Pro modes; the UI displays all text in English.
- Run dashboard: evaluated / feasible / best / generation tiles and a best-so-far convergence chart; per-objective convergence on the Results page; Pareto plots joined along the front.
- Engine: Maestro working copies, three execution modes, Spectre / ALPS, Local / Batch scheduler, NSGA-II / differential evolution / Bayesian optimization, checkpoint and resume, single-file configuration save and load, three-stage link diagnostics.
- Large projects: indexed scope matching, cached parameter views, debounced search, incremental result tables and background parameter preparation keep the window responsive at 50,000 parameters.
- Synthetic demo service, scripted smoke test and screenshot tour; no EDA tools needed to try it.
- Release preparation: machine paths, user names, internal host names, foundry/PDK names and licence strings in code and tests were replaced with generic placeholders; vendor-specific PDK adapters were replaced by fictional examples. Remaining fixtures are synthetic.
