# Run data v2

New runs use `crescendo.optimization_dataset.v2` and
`crescendo.optimization_record.v2`. The append-only JSONL envelope and canonical
JSON/SHA256 chain are unchanged. Every record in one dataset uses the same
version. Existing files retain their recorded version when reopened; Resume of a
v1 checkpoint creates v1 history, including checkpoints without a dataset.
A v2 continuation copies verified historical query originals and committed point
receipts into its own durable prefix, then rebinds lazy audit references before
committing completed rows. Portable/embedded sources work without the original
file; partial candidates still rerun under the unchanged active plan.
No migration rewrites historical bytes. Readers predating v2 reject its record
schema rather than interpreting references as evidence.

## Evidence and commit order

An `evidence` record contains a `crescendo.evidence_blob.v2` object. Its reference
has exactly `schema_version`, `type`, `schema`, `sha256`, `bytes`, and `revision`.
The digest and length describe the complete resolved canonical JSON value,
including all task, candidate, token and netlist identities. Values are stored as
JSON, or losslessly as base64/zlib canonical JSON when this saves at least 20%.
The decoder limits inflated data to 64 MiB and rejects unknown codecs, trailing
compressed bytes and checksum/length mismatches.

References are `{"$evidence": reference}`. Every referenced blob must occur
strictly earlier in the same verified prefix. There are no external blob paths.
A point record references a complete measured receipt; candidate pipeline points
can project matching fields from that receipt with `$point`, `fields`, and
`extra`. Candidate ownership propagates through referenced blobs, so a candidate
cannot borrow another candidate's point. The resolver also requires the earlier
`point` envelope in the verified prefix, binding its revision and chain hash; a
fsynced orphan blob alone is insufficient. The complete receipt still binds task,
Test, corner, point, token, parameters and netlist hashes. Point existence never
marks a candidate completed.

Candidate envelopes contain a packed `row` and a fixed allowlist of pipeline
`aliases`. Identical `completion.execution_pipeline` and
`completion_evidence.execution_pipeline` fields are stored once at the top level;
the reader restores the existing consumer object shape. Later terminal updates
use `candidate_patch`, with the exact committed base revision and canonical row
SHA256, changed top-level fields, and removed keys. Identity, parameter values,
generation, index and terminal status cannot change through a patch. Full
terminal overwrites are rejected. Earlier checkpoint prefixes remain replayable.

Each blob is written and fsynced before any referencing record. The referencing
point/candidate is then fsynced; the existing checkpoint/pending-journal
transaction must complete before the point ACK allows raw cleanup. Write/fsync
failure makes the writer unavailable. Generation audits and the writer's
incremental byte hash retain the v1.1.4 boundaries: each ordinary load/Resume
checks the full prefix, intra-generation transactions reuse only the owning
writer's issued snapshots, and one full audit precedes each optimizer `tell`.

## Scheduler originals

`scheduler_task_tracking.v2` preserves exact latest/terminal parsed observations
and command failure flags. Only the first and last query bodies are inline;
`query_count`, first/last observation samples, state transitions and omitted
counts remain available. Every query, including unchanged/UNKNOWN replies, is
appended and fsynced to `scheduler_queries.jsonl` in the point's execution
directory. Cancellation uses `cancel_scheduler_queries.jsonl`. Original stream
files are read when present; invalid UTF-8 bytes additionally have a lossless
base64 representation. Parsing and Stop proof still use the engine's original
command result and exact-ID validator.

A query log lists contiguous byte offsets, lengths and SHA256 values, plus the
entire segment length/hash. Before point cleanup its full text is committed as
an evidence blob. The log's `evidence` field is a validated `$audit` reference;
ordinary display/checkpoint readers leave originals lazy to avoid expanding all
query text into every candidate view. A fresh verified reader can recover them:

```python
reader = RunDataReader(dataset_path)
reader.read(strict=True)
originals = reader.codec.query_originals(query_log)
```

`query_log` is the resolved tracking object's `query_log`. This verifies the
blob, segment and every entry before returning query dictionaries. Missing,
modified, noncontiguous or out-of-range entries fail closed. Hashes are audit
locators, not completion or Stop proofs. UNKNOWN, cancel ACKs, unrelated IDs and
failed commands retain their original non-authoritative meaning.

## Export, pending journal and retention

`export_dataset`, embedded/self-contained checkpoints and continuation pending
journals carry the exact committed JSONL prefix, including its complete blob and
delta dependency chain. No separate blob directory needs packaging or GC.
Retention may remove acknowledged point directories and candidate raw artifacts;
it cannot remove individual blobs from the append-only core. Dataset deletion
continues to use the existing whole-run ownership/retention policy. There is no
in-place blob pruning or v1-to-v2 migration.

All v2 dataset, record, evidence, tracking and audit schema literals go through
the same startup identity/dialect resolver as v1. Applications can supply exact
aliases before importing consumers. The headless optimizer API and default
adapter registry identity are unchanged.

## F3 accounting and E5 size baseline

F3 counts v2 UNKNOWN states from every `query_log.entries` item, even when the
inline first/last samples omit all UNKNOWN replies. Observation counts and
`scheduler_unknown_occurrences` describe the resolved logical record tree,
including repeated snapshots and restored pipeline aliases, as in v1.
`scheduler_unknown_queries` counts each UNKNOWN original once per owned log
path, entry index and entry SHA256, including overlapping snapshots. Separate
query/cancellation logs remain separate. Missing states or incomplete query
coverage leave `scheduler_unknown_census_complete` false and the unique query
count unavailable. Legacy v1 state-count/sample accounting is unchanged.

The E5 synthetic benchmark's total size reduction was **43.23x** (about 43x),
including base64/zlib compression of evidence. With zlib disabled, evidence
sharing, projections, alias removal and deltas alone yielded **11.07x** (about
11x). These are distinct size measurements for that synthetic workload, not
measured guarantees for field runs or execution-time speedups.
