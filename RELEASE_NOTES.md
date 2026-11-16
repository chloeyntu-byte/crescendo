# Crescendo 1.1.4

Crescendo is a guided circuit optimizer for Cadence Maestro. Each release contains the window, the optimization engine, the Virtuoso menu loader and a synthetic demo.

Version 1.1.4 scopes native-log cleanup by overlapping lexical and resolved paths, preserves generation batch timings, and exposes nested storage and checkpoint measurements. Held dataset writers reuse verified disk prefixes for unchanged checkpoint references. Every append invalidates that proof; the first following evidence check rehashes the complete prefix to detect edits interleaved with the write. Unexpected timestamps also trigger full verification. Opening a writer or reading an uncached checkpoint verifies its complete disk prefix.

Every 128 evidence checks include a full audit. An older prefix modified with exactly unchanged mtime/ctime can pass intervening incremental checks and is detected at the next audit. Explicit `RunDataStore.reference(force_full=True)` requests an immediate full audit. Storage formats remain v1 and the 1.1.3 headless optimizer API retains its signatures, variable fields and algorithm names. See CHANGELOG.md and docs/EXECUTION_TIMING.md.

Verification binds its decision and disk reads to one file-state sample. Appends retain the immediate post-write sample and reject changes during synchronization instead of adopting the later stamp. Post-append full verification closes the remaining write/stat attribution window. This conservative implementation reduces repeated checks, but does not achieve linear read volume as appended history grows; that performance target remains incomplete.

Try it without any EDA tools:

    python3 -m crescendo.demo_main --scale small --autoload

Use it in Virtuoso: add `load("/path/to/crescendo/load.il")` to your `.cdsinit`, restart, then Maestro → Plug-in → Open Crescendo Optimizer.

Requirements: Python 3.6+ with Tk 8.5+. Real optimization needs your own Cadence Virtuoso/Maestro, Spectre or ALPS, PDK and licences.

Known limitations: optimized values are not written back into the original schematic; range checks verify end points only; target-site acceptance (your simulator, cluster and PDK) is up to you. The demo data is synthetic.
