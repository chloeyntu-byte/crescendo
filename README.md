# Crescendo

[中文说明](README.zh-CN.md)

Crescendo is a guided circuit optimizer that runs inside Cadence Maestro. Pick the device parameters it may tune, state what "better" means, and it simulates, compares and converges on the best designs and the Pareto front for you. Your original design stays read-only throughout.

![Crescendo run dashboard (synthetic demo data)](docs/screenshots/dashboard.png)

## What it does

- **Six-step workflow**: Design → Parameters → Goals → Run → Results → Trade-offs, shown as the left-hand navigation with a tick for each finished step.
- **Simple and Pro modes**: the default view keeps only the main flow. Pro mode adds working copies, history, optional PDK range checks, algorithm parameters, run/submit commands, EDA overrides and three-stage link diagnostics. Switching modes changes what is shown, never the settings.
- **Search**: NSGA-II (automatic for two or more objectives), differential evolution and Bayesian optimization; per-corner objectives and constraints with `max` / `mean` aggregation.
- **Execution**: Spectre or ALPS, locally or on a batch cluster; a working copy of the Maestro setup is used for every run, and a checkpoint is written after every candidate so runs can be stopped and resumed.
- **Live results**: KPI tiles, best-so-far convergence per objective, a candidate table, Pareto plots that jump to a candidate on click, CSV and full-dataset export. **Timing details...** shows generation and selected-candidate phases, with solver evidence, coverage and parallel limits explained in the [execution timing guide](docs/EXECUTION_TIMING.md).
- **Large projects**: on a synthetic project with 4,000 blocks and 50,726 parameters the parameter tree renders without freezing the window for more than 74 ms.

## Requirements

- Python 3.6 or newer (standard library only) with Tk 8.5 or newer, and an X display. No pip packages, no virtualenv.
- For real optimization: your own Cadence Virtuoso (IC 6.1.8 tested) with Maestro and Ocean, Spectre or ALPS, your PDK and your licences. A batch cluster is optional.
- The demo mode needs none of the EDA tools.

## Quick start (no EDA tools needed)

```bash
git clone <this repository> crescendo
cd crescendo
python3 -m crescendo.demo_main --scale small --autoload
```

The demo service generates synthetic op-amp-like metrics (a seeded, analytic two-objective front). It never calls Cadence. `--scale large` and `--scale huge` create bigger projects (up to 4,000 blocks / ~50,000 parameters) for responsiveness checks.

## Using with Cadence

1. Unpack the release somewhere on the machine that runs Virtuoso.
2. Add one line to the `.cdsinit` your project loads (back it up first):

   ```lisp
   load("/path/to/crescendo/load.il")
   ```

   Optionally point the engine at your model root before that line: `setShellEnvVar("PDK=/path/to/model_root")`.
3. Restart Virtuoso and open **Maestro → Plug-in → Open Crescendo Optimizer** (or the **Crescendo** menu when the Plug-in menu is unavailable).

Crescendo reads the Maestro window it was opened from. Tools, model directories and simulators are detected from that window's environment and can be overridden in **Settings → EDA** (Pro mode).

The execution modes describe who generates and solves the simulation:

| Mode | How it runs |
| --- | --- |
| **Native Run** | Maestro runs the simulation; Crescendo sets parameters and reads results. |
| **Maestro Netlist** | Maestro generates netlists; Crescendo's solver pool solves them in parallel within configured limits. |
| **Template Netlist** | Crescendo changes parameters in a qualified template and solves it; qualified cases can fall back to Maestro generation. |

Pro execution settings include the following resource controls, frozen in each new run:

| Setting | Default | Meaning |
| --- | --- | --- |
| `native_candidate_batching` | `generation` for new configurations | One owned native Run per generation; `serial` runs candidates individually. Existing configurations without this key and old checkpoints retain serial behavior. Qualified Template Netlist runs continue to materialize directly. |
| `local_solver_max_jobs` | `1` | Explicit Local solver slots; effective concurrency also obeys Global and Test limits. |
| `measurement_jobs` | `null`, automatic `min(2, G)` | Local reader workers; explicit values are capped by Global capacity `G`. |
| `progress_flush_interval_seconds` | `0.5` | Minimum display snapshot interval; terminal events, ownership and checkpoint acknowledgements remain immediate. |

These controls appear only in Pro mode. See [native batching](docs/NATIVE_BATCHING.md) and [execution timings](docs/EXECUTION_TIMING.md).

Cluster execution uses **Settings → EDA → Solver / Submission Commands** (Pro mode),
or `runtime.execution_commands` in JSON. `submission_template` is the complete
submit command and includes `{job_script}`; `scheduler_query_template` and
`scheduler_cancel_template` each include exactly one `{job_id}`. All three are
literal argv templates: quote arguments before placeholder expansion; use an
executable wrapper for shell logic. No scheduler executable or option is inferred.
For example (replace these illustrative commands with your own wrappers):

```json
{
  "runtime": {
    "execution_commands": {
      "submission_template": "batch-submit --cwd {cwd} --stdout {scheduler_stdout} --stderr {scheduler_stderr} {job_script}",
      "scheduler_query_template": "batch-query {job_id}",
      "scheduler_cancel_template": "batch-cancel {job_id}"
    }
  }
}
```

Query/cancel templates may also bind `{cwd}`, `{job_script}`, `{test}`, `{corner}`,
`{point_id}`, `{threads}`, `{scheduler_stdout}` and `{scheduler_stderr}` from the
owned task. Standalone diagnostic queries have `{cwd}` and available task metadata;
use a wrapper with a self-contained Job ID interface when other bindings are unavailable.
The two log placeholders allocate per-task paths; your submit wrapper must
route the scheduler's output there. Missing query or cancel templates block Cluster
preparation; Local execution needs neither. `command_scheduler` accepts `auto`
(track when templates are configured), `batch` (explicit tracking), or `receipt`
(an opaque wrapper whose owned completion receipt supplies completion evidence).
Cluster preparation requires both templates in every mode.
An explicit `-R "cpu=N;..."` resource argument, if supplied, must agree with solver
threads; other scheduler resource syntaxes belong in your wrapper.

The **scheduler key-value protocol** lets any scheduler integrate through a query
wrapper. On a successful exact-job query it must emit one numeric `JOB_ID` matching
`{job_id}` and one `JOB_STATE`, separated from values by whitespace, `:` or `=`.
For example:

```text
JOB_ID 10000001
JOB_STATE RUNNING
EXIT_CODE -
EXEC_NODE node01
```

`EXIT_CODE` is optional (an integer when available; `-` while unavailable).
Optional metadata includes `EXEC_NODE`, `SUBMIT_NODE`, `JOB_NAME`, `QUEUE`,
`ACCOUNT`, `REQ_CPU`, `REQ_MEM`, `JOB_CREATE_TIME` and `EXIT_MESSAGE`.
Active states include `PENDING`, `QUEUED`, `RUNNING`, `SUSPENDED` and `COMPLETING`;
terminal states include `SUCCEEDED`, `SUCCESS`, `DONE`, `FINISHED`, `FAILED`, `EXIT`,
`TERMINATED`, `CANCELLED` and `CANCELED`. JSON and a `JOB_ID JOB_STATE` table are also
accepted. Optional `TASK[n].STATE` records must all be terminal for a terminal job.
Conflicting states, foreign IDs, unknown states, failed commands and job disappearance
never prove that a job stopped. Cancellation acknowledgement alone is insufficient;
Crescendo polls the owned ID within its stop deadline. Simulator success separately
requires the owned receipt and result evidence.

Submit wrappers return a unique `JOB_ID: 10000001`, a JSON `jobId`, a `JOBID MESSAGE` table,
or an explicit `Submitted batch job 10000001` acknowledgement. Scheduler task logs
may contain `TASK_INFO` records with the exact owned Job ID; these provide bounded
log evidence and never authorize completion by themselves.

## What Crescendo creates

Your original schematic and Maestro view stay read-only. Crescendo writes parameters and run settings only to its working copies; it does not write optimized values back to the original.

| Item | Location and purpose |
| --- | --- |
| Working views | In the original library, under each source cell: a Maestro view for the selected setup, config views for the selected designs, and occurrence-specific working schematic views for the selected hierarchy. Unchanged references stay read-only. Names follow `cr_s_<token8>_<label>_<role>_<hash10>`: a session token, sanitized optimization label (up to 32 characters), role and identity hash. Each view has a `.crescendo-owned-view.json` ownership marker, matched against the session registry. |
| Project state | `<project>/.crescendo/<TB>__<library>_<view>_<hash>/`: `states/` holds saved UI/configuration state, `index.json` indexes the design, and `cache/` holds probes. The effective metadata directory is recorded in `runtime.design_root`. `setup_private_sessions/<session-id>/` holds working-copy ownership, inspected snapshots, configuration and workspace records (under `runtime.design_root`, or `runtime.run_root` if no design root is configured). |
| Run results | Normally `<data-root>/crescendo/<design-namespace>/<optimization>__<timestamp>_<token>/`. The data root comes from the selected Maestro Save Options (initial discovery falls back to `<project>/simulation` if unavailable); **Data Root Override** (`runtime.data_root_override`) changes it. Before selecting a design, the standalone default run parent is `<project>/.crescendo/runs`. Root overrides after design selection are described below. A reloaded setup with an unconfirmed save root requires an explicit data-root choice before a real run. |
| Checkpoints and logs | Each run keeps `checkpoint.json`, the incremental `optimization.crescendo` dataset, result/status and timing evidence, and `crescendo.log`. Keep these together for Resume and export. Native histories are under the working-copy workspace's `simulation/`; its `_scratch/` holds local PSF/RDB staging. |
| Temporary files | Launch/browse probes normally live in the design's `cache/probes/`. Small launcher files also use the system temporary directory (`TMPDIR`, normally `/tmp`). Registered scratch has a `.cr_scratch_owner.json` receipt; its default budget is 128 MiB (`runtime.local_scratch_budget_bytes`). Simulation data stays in the configured data directory. |

**Root overrides.** When the UI loads an explicit configuration with `runtime.run_root: <base>`, selecting a design puts metadata in `<base>/<design-namespace>/` and runs in `<base>/<design-namespace>/runs/<run-name>/`. Relative `<base>` paths resolve against the configuration file's directory. `CRESCENDO_RUN_ROOT` takes precedence (relative to the launch working directory) and uses the same design namespace. The loader records this base as `runtime.storage_base_override`; the selected design's effective `runtime.design_root` and `runtime.run_root` then point to the namespaced directories. **Data Root Override** takes precedence for simulation results, using `<data-root>/crescendo/<design-namespace>/<run-name>/`, while metadata stays at its selected location. At the lower-level storage-policy API, `runtime.run_root_override` selects a direct run parent only when no namespace override is active; it is distinct from the UI configuration-loading flow. Changing roots selects new storage locations; retain the old results and working-copy registry until their runs and views have been retired.

**Creation and reuse.** **Load and Inspect** creates the Maestro/config working copy; **Apply Selected Cells** builds and binds the selected working hierarchy. Candidates and generations borrow and reset that copy, rather than copying the entire library for every candidate. In Pro mode, **Design → Working Copies → Manage / Load Working Copies…** lists saved copies and their member paths; it supports loading, selective updates, renaming, recovery and deletion. **Settings → Maestro Working Copy** shows inherited native settings; **Open Config** shows the bindings.

**Completion, Stop and exit.** Completed candidates are saved to the dataset and checkpoint before eligible raw CLI workspaces are removed. Shared CLI cleanup waits until every logical consumer has committed; interrupted waits are reconciled on Resume. Native Run also releases each shared-history consumer after its checkpoint commit, but retains the native history/PSF as evidence; releasing a reference does **not** mean deleting that history. Netlist preparation evidence is retained. Stop first stops owned work and proves termination; unknown remote state blocks cleanup and Resume. Normal exit stops active work, releases working-copy leases and cleans proven scratch and temporary retained logs. It preserves saved working views and their registry for reuse. Neither finishing nor stopping a run deletes all saved working copies.

**After an interruption.** On reopening, the list reads durable ownership/session records and checks leases, view markers, snapshots and pending transactions. Loading or **Resume** reconciles the exact previous native/CLI owner before new submissions; a vanished UI or job is insufficient proof of termination. Incomplete creation/refresh, changed or missing ownership evidence, active editors/locks, or an unproven stop can retain or isolate a copy and block reuse/deletion. Use the list's recovery action when offered and review its warnings; recovery can keep backups and replaced views as evidence. **Retry Exit** retries unresolved shutdown cleanup. **Close Window Only** preserves pending work and isolates its copy; it does not confirm remote termination.

**Disk space and manual cleanup.** Prefer **Delete** in the working-copy list after stopping the run and closing its Maestro/config/schematic editors; confirm the selected copy in the deletion dialog. It checks exact registry membership, markers and writer termination. Never delete views by the `cr_s_` prefix alone, remove native locks, or delete the registry while its views remain. Raw candidate cleanup is proof-based, not an age/size retention limit: native histories, failed/pending evidence and saved copies can keep growing. Export/archive results first. Only after all owned jobs/readers have stopped and Resume is no longer needed may you remove a whole retired run directory; this loses its checkpoint, results and diagnostics. Keep saved-copy metadata until its views have been deleted through the manager. Remove leftover scratch only with its ownership receipt and producer-stop proof; do not sweep unrelated temporary directories.

## CLI for agents

All commands run from the repository root:

```bash
python3 -m crescendo --version                 # Crescendo and engine versions
python3 -m crescendo doctor                    # JSON environment report
python3 -m crescendo gui                       # open the window (standalone)
python3 -m crescendo run optimization-spec.json  # headless optimization run
python3 -m crescendo resume <checkpoint>       # resume a stopped run
python3 -m crescendo parse-rdb <path>          # read a Maestro results database as JSON
cadence/crescendo_launch --check               # JSON launch preflight (Python/Tk discovery)
python3 -m crescendo.demo_smoke --scales small large   # scripted end-to-end demo, prints timings
python3 scripts/run_tests.py                   # engine test suite
```

## Using the optimizer as a library

Tools that only need the search algorithms can import `crescendo_engine.optimizers` and run ask/tell without the window, Maestro or a scheduler. See the [headless optimizer API](docs/HEADLESS_API.md); pin a released version rather than the development tree.

## Limitations

- Optimization quality and runtime depend on your simulator, models and Maestro setup; the demo numbers are synthetic and say nothing about real circuits.
- Crescendo does not write the optimized values back into your original schematic. Results, netlists and parameter evidence are kept per run.
- Global and local design variables must each hold a single value; multi-value sweeps belong in corners.
- Range checks are optional and only verify the end points of each range.
- Real-site acceptance (your ALPS/batch scheduler setup, your PDK) is yours to run; this repository is tested locally with synthetic fixtures.

## RF-FIELDWORK

Product page: https://rf-fieldwork.pages.dev/crescendo/?lang=en

Other open-source tools:

- [Flux Studio](https://github.com/chloeyntu-byte/flux-studio): parametric RF passives, EMX sweeps, multi-objective search and SNPFit
- [Cap Array Lab](https://github.com/chloeyntu-byte/cap-array-lab): capacitor-array C/Q, parasitic paths, resonance and Z11
- [Layout 3D Viewer](https://github.com/chloeyntu-byte/layout-3d-viewer): read IC layouts in 3D, locate pins and trace connections
- [VCO ISF Explorer](https://github.com/chloeyntu-byte/vco-isf-explorer): ISF, flicker-noise up-conversion and phase-noise contributions
- [VCO Design Studio](https://github.com/chloeyntu-byte/vco-design-studio): five literature LC VCO topology families, packaged schematics and native export

## License

MIT, see [LICENSE](LICENSE).

