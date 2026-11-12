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
private leaf resources where existing content hashes require them. Reading still
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
relative paths, canonical-root confinement, the allowlist and file SHA256. Private
resource bytes are returned unchanged. Registration grants no Python import rights.
An optional `application_code` group selects the controller launch cwd and its
matching identity-validation root. Its canonical root/file manifest is validated
by the runtime; omitting it retains the engine source root.
Known public shell/menu source templates can be materialized for a selected
identity in a private, content-verified cache, before user interpolation; the
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
