# Prepared catalogue storage

New native discovery produces `crescendo.prepared.v2`. Only rows accepted by the
existing optimization eligibility rules enter its parameter catalogue. Every
eligible field, callback, numeric expression, domain and occurrence chain stays
available. Native cells, instances, simulator mappings, technology bindings,
config dependencies/bindings and errors remain intact. The hierarchy's parameter
list contains the ID/path join keys used by the parameter tree. Native scope,
read-only library, ignored/opaque implementation, editor condition, source guard
and effective-value checks still run.

CDF inspection gathers a candidate list per instance before reading values or
evaluating editor conditions. The generated SKILL predicate is a conservative
superset of the Python numeric semantic classifier, generated from the same
alias table. It retains numeric string expressions and ambiguous numeric types.
Only the static name/prompt/type classification is cached; effective values and
editor gates are evaluated for each occurrence. Read-only editor windows are
reused by library/cell/view and closed after the traversal; each evaluation still
requires the matching current edit cellview and receives its own instance CDF.
Legacy catalogue discovery keeps its original window lifecycle. The authoritative Python
eligibility rules still reject irrelevant/noneditable fields. Per-instance `cdfGetInstCDF` and dynamic condition evaluation for eligible
controls are necessary: caching effective CDF values by master would incorrectly
share instance-specific controls. One native process handles the selected scope;
only candidate records cross into Python. Quoted records use a JSON string fast
path, with the historical SKILL/Python escape fallback. Each design records
`command.probe_timing` with native time, parsing time and output bytes so these
costs can be distinguished. Native CPU ticks separately identify CDF retrieval,
editor retrieval, dynamic conditions, candidate filtering and record emission;
ticks are diagnostic counters rather than a portable seconds unit.

Persisted catalogues use `crescendo.prepared_reference.v2`. A small envelope
addresses one `crescendo.prepared_payload.v2` body by its canonical SHA256. The
body addresses separate hierarchy and parameter objects. All objects live in
`prepared_objects` within the frozen baseline (or the owning session when no
baseline exists). Identical content is published once; suite and baseline
snapshots retain small references. Live authority, validation state and baseline
snapshot references are envelope annotations and do not duplicate catalogue
payloads. Objects are singly linked owned regular files; symlinks, unknown keys,
incorrect kind/path/hash combinations, changed bytes and replacement during read
are rejected. Native ownership and continuation checks remain independent.
Objects must be retained with their baseline for as long as a suite/checkpoint
references it. Moving only an envelope is insufficient, just as moving only an
old checkpoint without its baseline was insufficient. There is no new automatic
content garbage collector.

Selective suite updates resolve hierarchy join keys against the retained parameter
metadata before reclassification. Updated v2 suites are compacted again; legacy
suite discovery retains its original complete CDF protocol and schema.

Old inline prepared objects, suites and baseline records are read without
compaction, schema relabeling or identity conversion. Legacy suite hashes and
freshness fingerprints use the original sorted compact ASCII JSON byte protocol.
The new chunked encoder emits those same bytes. Checkpoint fingerprint v1/v2
validation continues to select the recorded protocol, including a startup-injected
application identity/dialect. New v2 catalogue contents have a different identity
and cannot satisfy an old fingerprint merely by using a new storage envelope.
Unknown execution protocols remain rejected by their existing consumers.

In a caller process, catalogue JSON encoding, canonical hashing and general atomic
JSON writes use the Python iterator, compact separators and bounded scheduling yields.
The owned GUI service worker may use the equivalent compact C codec: its GIL and
cyclic GC are independent of Tk, including while reading old full catalogues.
Decoding streams containers in 64 KiB buffers; the C decoder handles complete
subtrees only within that bounded buffer. Larger containers stream their children. There is no dependency or multiprocessing
startup requirement. Model include scans, parameter classification, generated
SKILL-form scans and binding-record parsing also yield in bounded batches. Individual scalar strings are still decoded/escaped as one
scalar; this is intended for structured catalogues with bounded CDF fields,
not arbitrarily large single-string documents.

The affected paths are:

| Path | Large-object handling |
| --- | --- |
| Load / cell discovery | Compact streaming atomic evidence; no CDF conditions during cell-only discovery |
| Apply | Candidate-only CDF transfer; eligible catalogue projection before freshness capture; shared content references |
| Start / prelaunch | Streaming canonical freshness and checkpoint fingerprints; compact streaming workspace/worker JSON |
| Static/native range validation | Same retained eligible fields and native gates; streaming freshness; no catalogue in GUI event payloads |
| Suite save / reopen | Content reference writer/reader; legacy suite hash verified on the original hydrated object |
| Suite refresh / recovery | Compact streaming durable metadata, legacy metadata backups and original recovery hash protocol |
| Configuration load / save / match / exit snapshot | Settings snapshots copy only matching identity fields and UI edits; general JSON writer streams compact output |
| Resume | Baseline reference hydration, original recorded file/hash identity, then existing freshness/continuation/ownership validation |
| Headless preparation | Same v2 projection and storage helpers; public optimizer algorithm/registry contracts unchanged |

Parameter identity summaries for settings are computed with the prepared defaults
in a worker. Snapshot collection copies their scalar rows and current edits without
marshalling native metadata graphs on the GUI thread.

A fresh native `OptimizerService` attaches a GUI-only worker when constructed with
`OptimizerGui`. The service object keeps its identity; GUI calls use a versioned,
data-only socket pair protocol with concurrent request IDs. The worker receives
only the explicitly selected startup runtime/bootstrap environment, never an
import path or executable callback from a saved document. Its socket cannot leak
into native subprocesses. Headless services remain in process. Injected service
implementations, services with instance callbacks, and already active/preloaded
services retain their caller-owned lifecycle; attach a fresh service before
loading a catalogue to use engine-owned isolation.

Original v1/v2 objects stay in the service worker. Replies expose
`crescendo.prepared_gui_summary.v1` with an explicit `source_schema_version`,
eligible rows and hierarchy join keys. Inspections also use
`crescendo.hierarchy_gui_summary.v1`; they never repeat the full legacy CDF graph. This view is neither relabelled original
evidence nor a new execution object: prepared persistence, compaction and loaded
identity hashing reject it. Start, validation, suite save and Resume operate on
the untouched worker object and the original recorded identity protocol. Prepared
and hierarchy objects inside error attributes use the same rendering views.

Control requests use independent RPCs. Tk's existing setup-stop/force-detach
signals enqueue delivery without waiting for a blocked worker; that receipt
claims no stop or cleanup proof. Rich validation/storage/commit errors retain
their data attributes. Worker failure fails pending calls closed and does not
restart or acquire native authority automatically. Normal exit closes the
transport only after the existing service cleanup succeeds; explicit force exit
retains its original unproven-cleanup semantics. Parent EOF invokes the existing
owned cleanup path. Native controller/worker ownership, source, continuation and
termination checks stay in that service. A failed disconnect cleanup exits the
service worker with an error and keeps its log under the selected run root.

Tk controls remain on the main thread. Catalogue operations use their existing
GUI task lanes, with the large heap, hashing and GC in the owned worker. GUI
consumers receive only the versioned rendering view. Regression tests use synthetic
catalogues and a heartbeat with an intentionally long thread switch interval.

## Working config default lists

During Apply and private netlist preparation, Crescendo reads the source config's
default viewlist and stoplist through HDB. It inherits each nonempty list in
source order and records both the source values and the reopened working config's
values. An empty or absent list uses the corresponding fallback. The copied-library
adapter reads the preserved source config copy. The same-library adapter reads
the original source config in read-only mode.

For a schematic source and for a missing config viewlist, the fallback contains
the original top view and supported native simulator views. Spectre uses
`spectre` and `veriloga`. An ALPS Test can also use `alps`. A saved Test view setting
or an observed stopped occurrence can add `ahdl`. Where available, saved switch
view order determines the preference among the supported views. The schematic
probe uses that order to identify opaque native and HDL implementations, including
a Verilog-A leaf with no Spectre or schematic view. Config-bound occurrences
retain the source HDB resolution.

The fallback stoplist includes supported terminal views that are stopped in the
observed design and are never used for traversal. If there are none, it uses
`cr_config_stop_sentinel` so Spectre SI still receives a nonempty list. A
Verilog-A terminal included in the fallback is stopped as `veriloga`. Without
that default, HDB reports an implicit Verilog-A occurrence as non-stopped.
Spectre may still emit its HDL include, as observed in a one-module fixture.
Occurrence stop points remain explicit HDB stop rules. Each optimized occurrence
has an explicit HDB binding to its working cellview. The reopened config is
checked with native `hdbBind` and `hdbIsAtStopPoint`, and the verification records
contain the native values. Working view names stay in the occurrence rules.

Both lists have a maximum of **3,500 bytes** (`CONFIG_DEFAULT_LIST_MAX_BYTES`).
Lists over that limit fail during Apply or working config preparation, with
the config identity, list name, measured byte count and limit. The guard runs
before the private config is opened for write.

The generated config bytes and their private view fingerprints change. On first
execution, a reopened suite without default-list readback evidence rebinds its
owned views and verifies them before running. Apply can also rebuild them.
Newly prepared catalogues may record saved Test view settings and resolved
native or HDL leaves. That metadata changes their content-addressed storage and
loaded freshness fingerprints. An existing snapshot retains its original
fingerprint. Checkpoint fingerprint protocols and source identity checks stay
the same. A fresh inspection that changes the resolved or optimizable hierarchy
can still fail the existing checkpoint comparison. Historical candidate results
and their recorded evidence remain readable.
