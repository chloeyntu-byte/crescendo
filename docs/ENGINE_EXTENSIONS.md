# Application extension contracts

`crescendo_engine.ENGINE_ABI` is `crescendo.engine.v1`. Initialize the immutable
`ApplicationIdentity`, `StorageLayout` and `ProtocolDialect` once, before importing
engine consumers. Consumer imports freeze the public default if startup has not
selected a contract. Repeated and late initialization fail. An application uses
one contract per process; simultaneous presentations use separate processes.

## Identity and source literals

Identity controls display names, environment prefixes and startup aliases, schema
namespaces and exact type aliases. Optional `version` identifies the application
release in diagnostics; the engine version remains a separate field. Layout separates the XDG config directory from
the data/cache slug, project state directory, result extension, run-log basename,
owned-view marker, view prefix and staging prefix. `leaf_aliases` covers individual
filenames, including batch manifests. Dialect supplies long/short SKILL prefixes,
individual procedure/callback aliases, error prefixes, file magic, scoped guardian
tokens, adapter ABI and adapter entry symbol.

Engine producers and consumers explicitly resolve source literals before user
values are interpolated. This includes checkpoints and dataset hash chains,
continuation/suite recovery, P0–P5 batch schemas, SKILL readers/writers, native
environment bridges, guardians and observers. Aliases take precedence without
cascading. No generated script, saved document, user expression or user path is
rewritten. The public default returns the original literals directly and keeps
original resource paths and bytes. Protocol names and layout leaves are validated;
display names cannot contain quoting, escape or shell-expansion characters.

Environment aliases are folded into the selected prefix at startup, preserving
unset versus empty and giving the selected prefix precedence. Configuration and
storage discovery retain their existing semantics. `config_home` is an XDG leaf;
an explicit application-prefixed `USER_CONFIG` still selects a full file path.

A legacy application must supply its exact schema/type aliases and unchanged
application-specific leaf resources where existing content hashes require them. Reading still
validates the original dialect and checksum chain. A naming contract does not
convert, re-sign or grant Resume/Stop/cleanup authority to old evidence. Unknown
protocol revisions retain the existing rejection behavior.

## Trusted application configuration

Scheduler providers and worker bootstrap code are trusted application
configuration. Registration executes application-supplied Python with the
application user's privileges. Review and pin that code and its imported helpers
when selecting a provider or bootstrap. Runtime descriptors and content hashes
bind the selected configuration to workers; the startup environment supplies its
authority.

The contextual `help_probe` callback can execute any absolute executable path
selected by the trusted provider, with exactly one `--help` or `-h` argument.
Executable selection extends beyond the configured submission command. The
selected executable's help implementation runs with the caller's privileges,
working directory and supplied environment, and can have its own side effects.
Each call has an execution budget of at most five seconds, shortened by the
caller's remaining deadline, with engine-managed cancellation and bounded output.
The provider receives the bounded result. General command runners and process
handles stay inside the engine. This callback grants no submission, query or
cancellation capability. Provider APIs grant no completion, Stop or cleanup
authority; the engine retains the receipt, ownership and terminal-evidence gates.

## Runtime descriptors and worker startup

`runtime.initialize(...)` exports a bounded, versioned, checksummed JSON descriptor
on the fixed engine transport variable `CRESCENDO_ENGINE_RUNTIME`. Python workers,
standalone guardians and observers validate it before importing consumers. It
carries identity/layout/dialect, resource manifests, selected provider content
identity and the injected adapter-registry fingerprint. Duplicate/unknown JSON
fields, checksum/ABI mismatches and invalid names fail closed. Descriptors are
checksums, not signatures: startup environment is trusted application input and
must never be restored from a checkpoint or imported result.

For an executable provider, register it explicitly in the parent, then pass
`worker_bootstrap=/canonical/application/startup.py` to `initialize`. The file
must be a protected, owned regular file. Startup records a separate authority on
`CRESCENDO_ENGINE_BOOTSTRAP`; this authority is not a descriptor field. Children
check canonical path, ownership/mode, ABI and SHA256 and hash the exact bytes they
execute. The bootstrap only registers application plugins; it must not initialize
the runtime again or start the GUI. Its provider ABI, version and content digest
must match the parent. Missing or changed bootstrap code is rejected before use.
The bootstrap should be self-contained or import independently pinned application
helpers. No saved object or arbitrary module-name field selects executable code.

The bootstrap file must be owned by root (UID 0) or the current user
(`os.getuid()`), with both group-write and other-write bits clear (`mode & 0o022`
must be zero). It must also be a canonical, single-link regular file within the
bootstrap size limit. These checks intentionally protect worker startup authority.
When root runs directly from a source tree owned by an ordinary user, that user's
bootstrap fails the ownership check with `Worker bootstrap must be a protected
owned regular file`.

Run the application as the source-tree owner with a writable HOME and run directory.
For root maintenance or testing, export the tracked files into an isolated copy
owned by root, preserving protected file modes, and use a separate temporary HOME
and TMPDIR. Keep the maintained source tree under its intended owner's account.

`runtime.child_environment(environ)` preserves both engine channels even when a
caller constructs a reduced environment. Native child and cleanroom construction
use it explicitly. `environment_diagnostics.package_identity()` records engine
source-tree content plus runtime/provider/resource/registry identities for audit;
these diagnostic fields confer no execution or Resume authority.

## Scheduler providers

Register with `register_provider(name, instance)` before first use, then select
`initialize(scheduler=name, ...)`. The required methods are `probe`,
`prepare_submission`, `query_command`, `cancel_command`, `parse_submission`,
`parse_observation`, `resource_request` and `log_artifacts`. The default remains
`TemplateProvider`. `examples/extensions/fictional_scheduler.py` illustrates
explicit, neutral registration.

Providers receive data, not process handles. The engine runs bounded commands,
archives raw outputs, owns deadlines/Stop and applies receipt/completion gates.
Query/cancel argv must equal the configured template for one exact owned numeric
Job ID. Explicit query/cancel fields retain their configured values, including
empty strings. Provider observations remain subject to the engine evidence gates.
Submission preparation may add arguments before final engine freezing; it must
preserve the original argv order, selected executable, bindings and one exact
owned job script. It cannot replace task identity, evidence or output fields.
The engine checks shell controls, recomputes resource/log bindings and freezes the
final fingerprint. Resource requests remain subject to solver-thread matching.

An optional `submission_bindings(plan)` hook declares actual argv bindings after
argument preparation and before final freezing. Return `None` for the existing
template behavior, or exactly `workdir`, `stdout`, `stderr`. Each value is `None`
(unbound stream/default cwd) or `{'option': '--output', 'index': 3}`. Index 3 must
be the value immediately after that exact option, or the option token itself for
`--output=value`. Only one occurrence of each declared option is allowed. The
engine reads the path from argv; providers cannot supply a replacement path.

The caller must supply `bindings.owned_task_root` and `bindings.cwd` from its
owned task. The executor supplies its point directory. Workdir resolves against
cwd; streams resolve against the actual workdir. Every path, including the owned
script, must stay within that point without symlink components. Streams cannot
alias each other or the script. Normalized paths enter `scheduler_io`, stream
bindings and the final fingerprint; live identity and final artifact collection
consume that same plan. Declarations do not confer completion/Stop/cleanup rights.
Only absent options should be injected by `prepare_submission`; explicit argv
values and order remain intact. The hook is optional for existing providers.

Submission and observation results are checked against the engine's raw
key-value/JSON/table parsers. Missing, unrelated, ambiguous and mixed records
cannot become valid receipts or terminal proofs. New wire grammars require an
explicit future engine protocol contract. Preflight, execution and standalone
observer consumers use the selected provider consistently.

## Persisted scheduler dialect and frozen commands

Declare historical scheduler values at startup with
`ProtocolDialect(scheduler_aliases={'fable_queue': 'batch'})`. Alias targets are
`auto`, `batch` and `receipt`. Aliases cannot replace these public values. The
mapping is immutable and travels in the checksummed runtime descriptor to workers.
`scheduler_mode(value)` resolves execution semantics. Checkpoints, manifests,
settings, ownership and continuation schemas retain the original value and hash.
The public default accepts the three public values. The existing Pro scheduler
selector also accepts application-declared values.

The v1 frozen-command validator accepts the original four-key snapshot and the
current six-key snapshot. It first verifies the recorded fingerprint over its
original field set, then validates syntax and canonical values. The two scheduler
fields must both be present or both be absent. Unknown fields, partial snapshots,
noncanonical values and checksum changes fail validation. Restoring a frozen
snapshot preserves its exact fields and fingerprint, even when the current
configuration supplies other command defaults.

For a validated four-key snapshot, a startup-selected provider's `probe(commands)`
may supply `query_template` and `cancel_template`. These are transient execution
capabilities. Each template must bind exactly one owned numeric Job ID, and the
provider's actual query/cancel argv must equal that rendered template. The
snapshot retains its original fields and fingerprint. Provider command methods
receive that original snapshot. A six-key snapshot with empty fields keeps those capabilities unconfigured. The default provider
requires explicit user command templates. Applications supply their own pinned
worker bootstrap for executable providers. Exact terminal observations, receipts,
deadlines and cleanup proof continue to gate execution and recovery.

## Resources and adapters

`ResourceResolver.group(root, names)` creates a file-hash manifest. Pass groups
as `resource_groups={group: manifest}`. `resource_path(group, name)` checks safe
relative paths, canonical-root confinement, the allowlist and file SHA256. Application-specific
resource bytes are returned unchanged. Registration grants no Python import rights.
An optional `application_code` group selects the controller launch cwd and its
matching identity-validation root. Its canonical root/file manifest is validated
by the runtime; omitting it retains the engine source root.
Known public shell/menu source templates can be materialized for a selected
identity in an application-specific, content-verified cache, before user interpolation; the
public default uses the original source resources. Applications can instead
register their own unchanged resources in the corresponding groups.

`initialize(adapter_manifests=[...])` replaces the default registry without mixing
in public example adapters. Omitting it keeps the public registry digest. Adapter
implementation data may specify `resource_group`, `resource_name` and
`entry_symbol`; ABI/default entry come from the selected dialect. Entry symbols
are single identifiers. Every worker checks the startup registry fingerprint,
including implementation bytes, before using an injected registry. Existing
matching, fallback eligibility and effective-value verification remain in force.

## Presentation and diagnostics

`launch_application(RuntimeServices(config, service=..., diagnostic_actions=...),
gui_factory=...)` selects the presentation explicitly. Its default is
`native_gui_factory`. The public shell and `python -m crescendo` explicitly select
the public factory. Importing `crescendo.app` does not install a presentation.
The legacy explicit `install()` remains available to existing callers.

`DiagnosticAction(key, label, run, on_report_ready=..., on_progress=...,
on_close=...)` adds a native diagnostic button. The main thread manages state
and subscriptions; `run(snapshot)` uses the optional task lane and an activity.
Snapshots recursively freeze mappings and sequences. Buttons enable only after
the writer reports `report_ready is True`, disable during their task, and disable
on a new diagnostic or close. Events do not write reports or confer execution
authority. Subscription failures are activity warnings. Applications may release
their own resources in `on_close`.

## Contextual capability probes

An optional `probe_context(commands, context, help_probe)` receives selected
submission argv, config/spec/prepared data, native environment and an owned
working directory. The callback accepts only an absolute executable and `--help`
or `-h`; the engine owns execution, output limits, cancellation and the caller's
deadline. It cannot submit, query or cancel through this callback. Return the
usual capability with validated one-ID query/cancel templates and raw help
observations. Execution and diagnostic preflight use the same provider hook.

Providers may explicitly set `defer_missing_templates = True` to preserve the
already supported four-key frozen-command contract for inputs where both
scheduler fields are absent. Explicit fields, including empty strings, still
win. The engine validates the original fingerprint and uses transient resolved
templates without re-signing historical commands. An unrecognized transport can
return `receipt_only=True` with `available=False`; it then receives no scheduler
query/cancellation authority and still requires the existing receipt gates.
`submit_identity_required=True` independently retains strict raw submission-ID
validation when query capability is unavailable; it cannot supply a terminal proof.
Startup codec aliases also bind process-directory configuration field literals
and native menu resource leaves, before user data interpolation.

Read-only probes set `observation_only=True`; applications may report a validated
`query_available` template independently of full `available` query/cancel
capability. This does not authorize cancellation or completion. An optional
caller-scoped help cache remains inside the engine's help callback and does not
cross diagnostic invocations. Diagnostics recognize the provider's validated
log basenames and matching query/status/cancel auxiliary history, retaining raw
unbound scheduler text without granting it current-job authority.

## Read-only diagnostic report sections

Applications and scheduler providers can register named report contributors with
`crescendo_engine.diagnostic_sections.register_diagnostic_section(name, callback)`.
Register before starting a diagnostic. Each diagnostic freezes the registration
order; names are unique (at most 32 registrations, 128 characters per name).
This registry is separate from runtime identity, provider selection and adapter
fingerprints; it does not change the default registry digest or worker protocol.

The report drain calls each contributor once after the main verdict and Focused
checks, before detailed sections. The callback receives a recursively immutable
plain-data snapshot containing `test_id`, `report_root`, `run_ids`, `cases`,
`case_contexts` and `focused_checks`. It receives no service or process handles.
It returns `{'title': str, 'status': str, 'body': str}`; status must be `PASS`,
`WARN`, `FAIL` or `NOT_TESTED`. Callback exceptions or invalid results contribute
a WARN section, without changing the main report, mode verdicts, qualification,
Stop or cleanup gates. Registration is trusted application code: callbacks must
be bounded and read-only, and must not start work or mutate runtime state. Any
artifact reads must be confined to the current diagnostic's owned run roots;
never read historical runs. Callbacks execute in the report drain thread; blocking
callbacks delay writer readiness, so use already collected evidence.

Titles are limited to 160 characters; title plus body is limited to 8192 characters
per section and 32768 characters across sections. Limits include truncation
markers; exceeding them visibly truncates text or omits later sections. Each
section also reports `truncated` and `omitted_sections`, so truncation remains
explicit even if the remaining text budget cannot fit the marker itself. Names,
status and these fixed metadata fields have separate bounds. Existing sensitive-value redaction
applies before the length limits and publication, so replacement text cannot
expand the published section beyond its quota. The final sections appear both in the single master
log (the existing export/transfer artifact) and in the `diagnostic_sections` array
of the progress payload, including `DiagnosticAction.on_report_ready` and action
snapshots. `focused_checks` is also included in that payload. These fields are
observations only and are never added to the qualification checklist.

A fictional, precollected provider contribution:

```python
from crescendo_engine.diagnostic_sections import register_diagnostic_section

# The application collected this receipt for the current diagnostic only.
def fictional_receipt(context):
    return {
        'title': 'Fable provider receipt comparison',
        'status': 'NOT_TESTED',
        'body': 'No current owned job receipt was available; compared jobs: 0.',
    }

register_diagnostic_section('fable.receipts', fictional_receipt)
```

Focused checks preserve all existing phase timings. F1 reports per-generation
prefix scans, byte/file-size ratios, checkpoint transaction sums and explicit
generation-commit audits. Missing evidence is NOT_TESTED; failed audits are FAIL.
F2 compares raw scalar points retained only by diagnostic runs, across all three
mode pairs for completed, identically frozen candidates. Pairing uses candidate,
Test, corner and output; repeated points additionally require point IDs. Missing
or failed points and missing decisions are listed separately. Absolute tolerances
are in the output's declared units (V 1e-9, A/W 1e-12, Hz 1e-3, s/H 1e-15,
Ohm 1e-6, F 1e-18, dB 1e-6); dimensionless or unrecognized units use 1e-12,
with the fallback explicitly marked. Each numeric comparison uses the wider of
that absolute tolerance and `1e-6 * max(abs(left), abs(right))`. Reports include
all mismatch counts and at most 20 numeric mismatch examples.

F3 streams only each current run's dataset. Record byte totals are exact; top-level
field values and scheduler subtrees use canonical JSON value sizes, excluding
key/punctuation overhead. Nested subtree sizes are not additive. It reports
observation occurrences, distinct hashes, duplicate locations and record counts.
UNKNOWN query totals use diagnostic-only full query censuses, deduplicated by
owned task identity and maximum observed count; older bounded sample-only
censuses are explicitly incomplete. UNKNOWN occurrences include repeated aliases.
Point counts prefer distinct durable point receipts; Native Run falls back to
distinct retained raw Test/corner points, with multiple outputs counted once.

F4 dispatches prepared statistics by format. The legacy/v1 adapter counts only
explicit current baseline/session prepared files and inode-deduplicated physical
bytes. The v2 adapter follows those references through verified content addresses:
`logical_bytes` (also `bytes`) sums canonical resolved JSON per copy; `cas_bytes`
counts unique reachable CAS objects once. `copies` counts current prepared files,
`reference_count` counts those top-level references, `cas_reference_count` counts
their CAS edges (including shared edges), and `cas_object_count` counts unique
reachable objects. `reference_bytes` counts unique wrapper files; `physical_bytes`
is their sum with `cas_bytes`. Inline v2 files remain supported without CAS edges.
Without file references both adapters report one in-memory canonical copy; v2
reports zero physical storage bytes. Historical files and unreachable CAS objects
are never scanned, published or repaired. Missing, unsafe, corrupt or oversized
current references are explicitly incomplete (1 MiB session metadata limit).
Unknown formats remain NOT_TESTED. Parameter totals and optimizable counts come
from the frozen catalogue; v2 eligible-only catalogues normally have equal counts.
Native GUI heartbeat windows cover
the diagnostic and the latest Load/Apply actions, including synchronous setup and
completion callbacks. Pauses are actual gaps between scheduled 50 ms ticks, matching the existing
Setup profiling convention; excess delay is also retained. Counts use strictly
greater than 200 ms and warnings strictly greater than 1 s. Destroying the root
cancels only the observer timer; a closed GUI remains NOT_TESTED.
Load/Apply elapsed seconds are reported separately. Headless runs are NOT_TESTED.

When the GUI service runs in a child process, timing snapshots and registered
report contributors remain in the parent. Diagnostic start freezes the parent's
registrations. A data-only reverse RPC requests timing or bounded, redacted
sections; callbacks receive immutable context in their original process. These
requests use a separate lane from service responses and cancellation. No Tk
object, contributor callable or native ownership handle crosses the transport.

## Linked variable schema extension

Crescendo 1.2 / engine 3.1 adds optional `link_group`, `step` and `grid_origin`
parameter preferences. See [HEADLESS_API.md](HEADLESS_API.md#linked-parameters-crescendo-12--engine-31)
for the full ask/tell expansion and checkpoint v2 contract. This change leaves
the engine ABI, default application names, scheduler protocol and registry
digest unchanged. Hosts must retain the complete variable dictionaries when
freezing run identity, saving preferences and exporting datasets.

## Versioned run history

New run history uses dataset/record v2; existing files and v1 checkpoint Resume
retain v1. Exact aliases for `crescendo.optimization_dataset.v2`,
`crescendo.optimization_record.v2`, `crescendo.evidence_ref.v2`,
`crescendo.evidence_blob.v2`, `crescendo.scheduler_task_tracking.v2`,
`crescendo.scheduler_query_log.v2` and `crescendo.scheduler_query_originals.v2`
are selected before consumer import, exactly like their v1 counterparts. A v1
record alias does not implicitly alias v2. Consumers verify the selected literal
and complete hash chain; aliases never grant Stop, Resume or cleanup authority.
See [Run data v2](RUN_DATA_V2.md) for wire, audit and portable contracts.

## Compact diagnostic reports and evidence

`runtime.diagnostic_log_detail` defaults to `compact`; `full` retains the existing
inline renderer and does not create sidecars. `runtime.diagnostic_log_examples`
defaults to 3 (integer 0–20). A diagnostic spec can override either field with
`diagnostic_log_detail` / `diagnostic_log_examples`. These are presentation
preferences only, saved by unified configuration and shown only in Pro mode.
They do not change execution, check verdicts, first causes or stop authority.
Runtime configuration and diagnostic specs reject non-integer/out-of-range
example counts. Unified configuration save/load and GUI inputs accept integer
entry text and reject values outside 0–20 with a field-specific error.

Compact logs retain mode verdicts/first causes, every mode/generation phase timing
(with measured/total counts and storage scopes), F1–F4 status/criteria/numbers,
bounded examples, contributor sections, grouped nonpassing checks and collector
omissions. F2 unpaired points are counted by output/reason. UNKNOWN owned-job
queries are grouped across modes with ID range/count and bounded examples.
Repeated first causes use `first_cause_ref` pointing to the first case ID in
`diagnostic_overview.cases`; standalone Copy resolves that cause and includes the
sidecar manifest without expanding raw evidence into the physical log.
The final checklist retains every check ID and status; complete details are in
the sidecar. Standalone Copy sections carry context and sidecar references.

`diagnostic_evidence.jsonl` is a versioned, uncompressed UTF-8 content-addressed
JSONL graph in the report directory. Identical dictionaries/lists/long strings
are stored once, including structures shared across observations, checks and
focused results. Typed nodes distinguish literal `ref` fields from references.
The final row names document roots: complete `case_results`, `focused_checks`,
`checks`, `case_records`, `focused_cases`, `case_contexts`, bounded
`diagnostic_sections` and `overview`. Use
`crescendo_engine.diagnostic_log.read_evidence(path)` to restore plain JSON data;
this validates node digests without executing evidence. Pre-existing collector
bounds/redaction and omission reporting still apply: sidecars preserve everything
collected for this report, not files the collector never read.

The log and report-ready payload include `report_sidecars` entries (log prefix:
`sidecar_manifest`) containing absolute path, byte count, sha256, schema and hash
scope. Hashes cover the exact **uncompressed file bytes**, including JSONL newlines.
No sidecar content is added to the payload. Visual/log transfer reads the same
compact `main_log`; transfer sidecars separately when raw evidence is needed.
Before publishing report-ready, the writer checks every listed size/hash and
emits `report_sidecar_integrity`; collector omissions are reported separately by
`report_evidence_integrity`. Missing/corrupt files yield FAIL, never a claimed
complete receipt. `EnvironmentTestManager.verify_report_evidence(test_id)` or
`diagnostic_log.verify_sidecars(manifest)` provides an explicit fresh audit after
transfer or later modification. A previously published audit describes its
verification instant; it is not a continuous filesystem monitor or stop proof.

Progress also includes `report_state` with `status`, `warnings` and `write_error`.
Delivery status is `writing`, `ready`, `ready_with_warnings` or `write_failed`.
A sidecar integrity FAIL adds a `sidecar_integrity` warning (including audit
reasons) and is visible in the diagnostic card. The writer records the same
status in a `report_delivery_status` log row; a fresh manager audit updates the
progress status. `report_ready` continues to mean that the main report was
written successfully. Integrity warnings never change diagnostic `state`, check
verdicts, focused results or gates, and do not turn a diagnostic into a failure.

Contributor budgets **inside the compact total log** remain 8192 characters per
section and 32768 characters total for title + body, with the exact existing
truncation markers and ordering. This permits up to 131072 UTF-8 bytes for the
bounded text in the worst case, plus section metadata. Contributor bodies are
emitted once, unchanged in both modes; arbitrary application text is not parsed
or shortened to enforce the focused-check example preference. Hosts should budget
contributor text as part of their transfer limit. The standard ASCII fixture
(4 generations × 4 candidates × 3 modes × 2 corners) targets a total log under
100 KiB (required acceptance ceiling 150 KiB); maximally filled multibyte
contributors can exceed that target under the unchanged extension contract.

## Persist a selected candidate as schematic views

Call `OptimizerService.generate_optimized_schematic(run_name, candidate_id)`
after the real run has reached a terminal state and its workers have exited.
The service resolves evidence from the candidate's own local run (or its
verified origin on Resume) and checks the current source design identity
before writing. The GUI entry is `OptimizerGui.generate_optimized_schematic()`.
It submits through the existing background queue and calls
`show_optimized_schematic_result(result)` on the Tk thread. Subclasses can
override that method to display the same payload. The engine entry
`optimized_schematic.export_optimized_schematic(...)` returns the same payload.

### Naming and source views

The name comes from the selected run's saved `display_name`. The button
offers no separate name input. The service uses the saved status display name
when an older spec omits it. Direct engine calls without a display name
use the legacy spec `name` or `optimization`.

`optimized_view_name` applies Unicode NFKD normalization, drops characters
that still cannot be represented in ASCII, replaces each run outside
`[A-Za-z0-9_]` with an underscore, and strips leading and trailing underscores.
An empty token becomes `optimization`. If any spelling changed or the token
exceeds 86 characters, up to 75 characters of the token are kept (with trailing
underscores removed) and `_` plus the first 10 lowercase hexadecimal digits
of SHA-256 of the original UTF-8 name are added. The result has the
`schematic_` prefix and a maximum length of 96 characters.

An existing view is preserved. The planner adds `_p` and a deterministic
10-digit hash of the owner and occurrence identity, retrying with a counter
until the name is free. Distinct parameter sets on separate occurrences of one
cell use separate views. If a conflicting view appears during publication,
the native writer tries the preallocated unique fallback name. Every copy
uses the non-overwriting OA operation. `requested_view` is the planned name,
`base_view` is the normalized run name, and `view` is the verified final name.
`view_name_reason` is null, `existing_view`, `multiple_occurrences`, or
`concurrent_view`. Source schematics, symbols and config bindings retain
their original identities. The exported views are available for explicit
binding in an ADE setup or config.

### Evidence and result fields

Native Run matches the completed history and generated netlist to the
parameter readback at each applicable test, point and corner. The requested
value ties the readback to the candidate, while the effective value determines
what is written. Input-only or incomplete proof, conflicting PSF readback
when available, and point-dependent values are rejected. Maestro Netlist
uses the completed, captured point values or the historical verified OA
readback. Template Netlist uses the matching qualified template and completed
materialized values, with its symbolic OA snapshot when the run retained one.
Each published selected parameter is read back from both OA and CDF.

The JSON file at `evidence_path` contains the result, candidate provenance,
and `proof`. The `views[].parameters` array retains per-candidate parameter
IDs, physical values (including `actual_value`), and OA/CDF readback.
`views[].instances[].parameters` groups the write report by physical instance.
`old_value` is the original OA value or the original CDF value if OA had no
property. It can be an engineering string, expression, number, or null.
`new_value` is a number in the stated `unit` for selected parameters. Other
changed scalar properties use their final OA value. An empty unit means
dimensionless or unspecified. `design_variables_not_persisted` carries the
proven value, name, test and `source_kind` (`global` or `top`) for settings
to apply in Maestro/ADE. `locked_device_parameters` retains dependency
evidence, including the controlling variable, dependent property, hierarchy
path and native scope. A run containing only design variables can produce
the evidence and report with an empty `views` array.

A cell-specific error adds a `failures` entry without discarding confirmed
copies of other cells. `ok` is true when every requested cell succeeded,
and `partial` is true when some cells succeeded and some failed. For a
failed publish, `failures[].view` can identify the attempted view, which
does not confer successful readback. Source/candidate identity failures and
unproven writer termination raise errors.

Example result excerpt

```json
{
  "schema_version": "crescendo.optimized_schematic.v1",
  "candidate_id": "g000_c0001",
  "display_name": "Divider",
  "ok": false,
  "partial": true,
  "evidence_path": "/runs/divider/optimized_schematics/export_id/export_result.json",
  "proof": {"kind": "native_run_point_parameters", "history_name": "Interactive.0"},
  "views": [
    {
      "library": "cr_lab",
      "cell": "cr_div",
      "source_view": "schematic",
      "base_view": "schematic_Divider",
      "requested_view": "schematic_Divider_p0123456789",
      "view": "schematic_Divider_p0123456789",
      "view_name_reason": "existing_view",
      "instances": [
        {"instance": "R1", "parameters": [
          {"parameter": "r", "old_value": "1k", "new_value": 1500.0, "unit": "Ω"}
        ]}
      ]
    }
  ],
  "design_variables_not_persisted": [
    {"parameter_id": "design_variable:cr_lab:cr_div:schematic:cr_dc:cr_bias",
     "name": "cr_bias", "value": 1.2, "unit": "", "source_kind": "global", "test": "cr_dc"}
  ],
  "locked_device_parameters": [
    {"device_parameter": "p2", "hierarchy_path": "/R2", "property": "r", "name": "cr_bias"}
  ],
  "failures": [
    {"library": "cr_lab", "cell": "cr_load", "source_view": "schematic",
     "requested_view": "schematic_Divider", "view": null, "phase": "stage",
     "reason": "Verify staging OA and CDF values", "diagnostic_path": "/runs/divider/stage/ocean.stdout"}
  ]
}
```

## Ocean tool and library initialization diagnostics

Ocean can emit ADE-5066 or ADE-5067 when a library initializer looks up a
simulator tool. The engine records `WARN` when the message names a tool
outside the simulators used by that operation. It compares names
case-insensitively.

| Simulator | Protected Ocean tool names |
| --- | --- |
| Spectre, including APS | `spectre`, `aps`, `spectreaps` |
| ALPS | `alps`, `alpss` |

Capture protects both the simulator in the saved Maestro Test and the
selected CLI simulator when a bridge uses both. Native Run uses the selected
simulator, which must match the saved Tests. An unknown simulator or an
unrecognized ADE-5066/5067 message stays an error.

A SKILL `*Error*` becomes a `library_init` warning when the native stack
shows Cadence loading a library's `libInit.il` or `libInit.ils` through
`load` or `loadi`, the error originates outside an engine-generated SKILL
file, and the same console shows the enclosing stage reaching its matching
RETURN or DONE marker. The stage begins before the error. Capture, Run,
and private library lookup each emit their own boundaries. A different
stage's completion, a stack without a Cadence library-loading caller, or
an error originating in `capture.il` or `patch.il` leaves the original
failure handling in effect. Exit status, required markers, point coverage,
history qualification and solver checks remain independent gates.

ICRP worker logs apply the same simulator tool-name rule to ADE-5066 and
ADE-5067 records, including Cadence's prefixed output. A stackless worker
SKILL error requires a stack-backed library initialization warning from
the full Ocean console for that capture. The error lines must match
exactly after removal of the Cadence record prefix and trailing whitespace.
The worker must report the error between matching incremental netlisting
boundaries, report zero netlisting errors, identify a point and test whose
netlist was freshly written or whose directory appears in a current
capture invocation, and end with the normal memory report. A byte gap, truncated worker log, or reference
to generated SKILL prevents this classification. Other generation errors
in the same point still fail the capture. When ADE reuses a Job filename,
the new log must begin with a program start timestamp from the current
capture before it can qualify.

Native Run uses its owned Ocean transcript to qualify exact stackless echoes
in the same session, fresh ICRP job logs and RDB point or output messages.
It compares error text after removing the record prefix and line ending.
The Run must return, the exact history must be bound, and every expected
test, corner and point must pass the native status, coverage and solver
checks. An RDB output with a warning still requires a usable scalar.
Completion evidence records qualified warnings with their source and line.
For an RDB message the line is relative to that field, and the record
identifies its test or output and point. The Ocean diagnostic file retains
the stack as a candidate for classification before point validation.
Errors originating in generated SKILL and unmatched messages fail
completion. ADE-5066/5067 continue to use the simulator tool-name rule.

With proven cleanup and unchanged owned source/setup, a completion failure
is recorded against its candidate and optimization continues. A Native Run
generation partitions errors by exact physical test/point identity when
available. A failure shared by duplicate logical candidates applies to each
consumer of that physical row. A batch with one distinct physical row uses
the serial fallback before submission, since its native report has no set
conditions. Unproven cleanup, unsafe artifact or session
ownership, and ambiguous shared batch mapping stop the run and retain the
evidence.

Capture reports store `skill.ocean_tool_warnings` and, for worker logs,
`logs[].ocean_tool_warnings` in `native_worker_diagnostics.json`. Native Run
stores its console evidence in `native_skill_diagnostics.json`. Setup command
evidence uses `skill.ocean_tool_warnings`, and CLI reader capability records
use `ocean_tool_warnings`. Warning records carry `severity`, `native_code`,
`tool`, `simulators`, `console_source`, `console_line` and the native
`message`, including the wrapped ADE-5067 continuation.

Library initialization warnings use `library_init_warnings` at those same
locations. Each has `severity`, `category` (`library_init`),
`library_init_path`, `console_source`, `console_line`, `console_end_line`,
`stage_begin_line`, `completion_line` and `completion_marker`. The
`message` includes the original error and printed stack.
`stack_trace_truncated` marks abbreviations or evidence truncation.

Stackless worker warnings preserve the original line in `message`, set
`source: worker`, and identify the Job file and line in `console_source` and
`console_line`. They also link the matching Ocean warning, the capture
invocation or fresh netlist, the netlisting statistics line, and the
worker exit line.

The run and compact diagnostic logs show both warning types as
`ocean_tool_warning`. The verdict selects its first cause from errors.
Console diagnostics retain fatal tool messages in `first_error` and
`first_simulator_tool_error`.
