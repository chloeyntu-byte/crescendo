# Transient solver recovery

Both Maestro Netlist and Template Netlist use this shared CLI policy. Native Run
retains its native execution lifecycle. No algorithm, adapter registry, public
headless API or runtime naming contract changes.

## Configuration

`runtime.solver_transient_retries` is a nonnegative integer (default **2**).
The first attempt plus two retries permits three solves. **0** disables retries.
`runtime.solver_transient_last_retry_single_core` is a boolean (default **false**).
The same preferences appear under **Pro / Settings / Crescendo / Diagnostics /
CLI Capacity and Progress**, and round-trip through the unified configuration.

New CLI runs freeze both fields into their spec and checkpoint manifest. A
manifest/spec mismatch is rejected. Frozen values override later runtime changes.
An old checkpoint with either field absent keeps that field absent; it is reported
as `unconfigured_defaults_allowed` and receives the missing default (2 / false),
even if the current runtime configuration differs. Recovery is allowed for old
runs because it only handles a proven infrastructure crash, without changing
candidates, circuit merit or the frozen solver command.

## Signature and safety gates

`simulator_profiles.TRANSIENT_CRASH_SIGNATURES` owns the extensible signature table;
ALPS currently has no signatures. Spectre requires return code **134** or signal
termination, **both** literal strings `Internal error found in spectre` and
`SPECTRE-18`, **and either** `during hierarchy flattening` or `during initial setup`,
all in one log of the current owned attempt. Both stage variants are defined in
the profile table; a missing internal-error marker, solver code or stage prevents
recovery. POSIX compute wrappers encode
signals as 128 + signal number; direct subprocess adapters may use negative codes.
Only the point's solver log, PSF `log`, and owned solver stdout/stderr are scanned.
Pre-launch file extents are excluded using device/inode identity and byte offsets
from the owned open descriptor. An append is scanned only after its old extent;
keywords cannot straddle that boundary. A replacement inode is scanned from byte
zero. In-place rewrites/truncations conservatively exclude the old byte range,
since timestamps cannot prove which old bytes were rewritten. Full logs are still
retained as evidence. Stale or foreign paths, links and keywords split across
logs cannot authorize recovery. Timeouts, Stop, submission identity errors and
unproven termination cannot authorize a retry. Other failures retain their
existing classification and execution path.

Each attempt has a distinct token and control files. The first keeps the existing
`execution/` control location; retries use `execution/attempt_002/`, etc. Retrying
requires the previous cleanup proof and unchanged input hashes. Cluster retries
also require the previous exact owned Job ID's terminal scheduler observation.
Each retry is a new submission with a new token, Job ID and compute receipt.
A missing scheduler terminal observation blocks a retry even when the compute
receipt proves cleanup. Stop is checked before every attempt and immediately
before submission. No existing process is taken over.

The netlist, working directory and frozen solver argv are reused. After proven
termination, the previous PSF is archived under its attempt and a fresh PSF is
created at the original path. The optional final Local retry adds
`taskset -c <lowest CPU in the current allowed affinity>` to the execution argv.
**This restricts the entire solve to one CPU, including simulation, and can slow
it down.** Cluster ignores it with `ignored_cluster_mode`; missing `taskset` or
unknown CPU affinity is recorded and execution proceeds without the prefix.

## Evidence and outcomes

`execution/solver_attempts.json` records schema `crescendo.solver_attempts.v1`.
Full logs, receipt copies and the original first error are also kept outside the
candidate raw tree under:

```
<run>/private_netlist/solver_attempts/<candidate>/<task>/attempt_001/<token>/
```

`attempt.json` includes actual argv, per-attempt elapsed time/phases, receipt,
termination proof, exact scheduler observation and log inventory. These are
observations; their copied files grant no completion or cleanup authority. Each
attempt also has distinct bounded retained log snapshots in the candidate, which
survive ordinary candidate compaction. Full attempt archives remain until the
owned run is deleted and may increase disk usage.

Per-attempt phases are summed into point timing; successful point elapsed time
includes all retries, and the candidate's existing timing covers the entire
execution. `solver_transient` is included in point commits, completion pipelines
and candidate rows (including failures).

When all allowed attempts crash with the signature, `first_cause` and
`failure_reason` are `transient_crash_exhausted`. The candidate remains
`infrastructure_failed` with the existing `private_netlist_infrastructure`
classification; no infeasibility metrics or design penalty are generated. The
existing infrastructure stop policy remains in force after exhaustion.
A deterministic crash that repeats on every attempt is bounded by the configured
N retries (N + 1 total attempts). For this observed read-in failure, each retry
costs approximately 4 seconds, so the added cost is approximately 4N seconds;
this observation is not a timeout guarantee for other solver failures.

The F1–F4 report adds `solver_transient.observation_only` with counts for each
simulator/execution mode: crashes, retries, points recovered successfully,
exhausted points and observed points. These counts do not change F1–F4 verdicts.

Shared logical candidates are counted once per physical point using their attempt
token sequence. Tokenless older observations use the batch epoch, physical
candidate and task identity when available; unidentified legacy points retain
their original per-record counts. Independent executions with distinct tokens
remain separate, even when task and point names match. Deduplication is scoped
to each diagnostic mode and never changes execution, timing or F1–F4 verdicts.
