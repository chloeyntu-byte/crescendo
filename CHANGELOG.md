# Changelog

## 1.1.1 (2026-11-12)

- Tests only: widen positive synchronization and completion waits (event "entered" waits to 10 s, diagnostic manager joins to 30-60 s) so slower CI runners do not time out. Waits return as soon as the event fires, so passing runs are not slower. Waits that check latency or that a task must not finish yet are unchanged. No product code changes.

## 1.1.0 (2026-11-12)

Highlights:

- **Generation batching.** New runs evaluate a whole generation per native Run (Baseline) or per native capture (Private Maestro, and Private Netlist when native fallback is required), then fan results back to every candidate. On local real-EDA runs with matched samples (3 generations x 6 candidates x 2 corners) Baseline went from 433 s to 101 s (4.3x) and Private Maestro from 342 s to 166 s (2.1x), with identical objective values. Private Netlist template runs were already fast (148 s vs 139 s); with native fallback, batching took 130 s instead of 190 s.
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

- Add immutable scheduler enum aliases to the startup dialect, shared by checkpoint validation, execution policy, private-netlist startup, settings and Pro restoration. Keep original persisted values and continuation hashes.

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

- Capture Private Maestro generations in one owned Local2 Run, freeze and qualify every point before shared CLI execution, and execute qualified duplicate physical rows once with durable logical fanout.
- Route qualified Private Netlist native-companion and selected-binding fallback through the same generation barrier. Keep template materialization, legacy serial settings, controlled readers and candidate-granular Stop/Resume.
- Persist native batch enable/corner readback and effective job policy, and document shared-suite reset and Command Stop query prerequisites.

- Advance native point qualification to v5 for lexical scopes and indirect aliases. Older native templates and manifests require fresh qualification before execution. Real shared-CLI checkpoints carrying v4 or missing qualification revisions are rejected on Resume, including completed and failed history. Their results remain readable/exportable. Start a new run to qualify inputs again. Legacy non-native and mock evidence keeps its compatibility.

- Verify execution-resource preferences in a real Crescendo window, including configuration round trips and mapped Pro/Simple visibility. Its fixture uses private preferences and an isolated process so presentation imports cannot affect later launcher or kernel-window tests.

- Preserve each point's reader timeout budget and split multi-point reads at a 3600-second batch ceiling. Single points retain longer explicit budgets. Remove shared reader controls after all point owners retire, release their parent directory after all candidate reader futures finish, and retain unresolved ownership evidence. Explicit reader concurrency is capped by Global capacity with requested/effective values in summaries.
- Limit display snapshot writes while preserving every incoming run-log event. Flush actual production terminal events immediately and verify log completeness with throttling enabled.

- Preserve manual EDA revalidation after a settings update: loading the cell hierarchy does not start the saved-setup automatic Apply chain or reacquire Run authority before the user applies the cells.

- Add versioned, once-per-generation native Baseline Runs with equal Device Parameter columns, exact session/set readback, durable submission intents and full-history point mapping. New configurations default to generation batching; old configurations and checkpoints retain serial execution.
- Keep Command semantics, fail-fast completion, candidate-granular Resume and shared-history reference ledgers. Stop requires native Stop/Wait/Close, local cleanup and exact Command job terminal queries; unresolved resources block Resume. Shared stages have one timing sidecar and logical/physical counts.
- Add a Pro batching selector and explicit pre-submission serial fallback. Source parameter sets, unknown/stochastic analyses and unqualified duplicate sharing preserve the serial path; submitted batches never replay serially.
- Neutralize native-stage fixture names and scan every tracked text file for private library prefixes, foundry brands and site-specific scheduler terms.

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
- Engine: private Maestro copies, three execution modes, Spectre / ALPS, Local / Batch scheduler, NSGA-II / differential evolution / Bayesian optimization, checkpoint and resume, single-file configuration save and load, three-stage link diagnostics.
- Large projects: indexed scope matching, cached parameter views, debounced search, incremental result tables and background parameter preparation keep the window responsive at 50,000 parameters.
- Synthetic demo service, scripted smoke test and screenshot tour; no EDA tools needed to try it.
- Release preparation: machine paths, user names, internal host names, foundry/PDK names and licence strings in code and tests were replaced with generic placeholders; vendor-specific PDK adapters were replaced by fictional examples. Remaining fixtures are synthetic.
