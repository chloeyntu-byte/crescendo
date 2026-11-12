# Crescendo 1.1.0

Crescendo is a guided circuit optimizer for Cadence Maestro. Each release contains the window, the optimization engine, the Virtuoso menu loader and a synthetic demo.

This release evaluates a whole generation at once instead of one candidate at a time. On local real-EDA runs with identical results, Maestro Baseline became 4.3x faster and Private Maestro 2.1x faster; Private Netlist needs batching only when it falls back to native netlist generation (1.5x there). Pro mode adds Local solver slots, reader workers and a batching selector; old checkpoints keep their original behaviour. Per-candidate and per-generation timings show where the time goes. Batch schedulers, complex process models and large-scale throughput still need qualification on your own site. See CHANGELOG.md.

Try it without any EDA tools:

    python3 -m crescendo.demo_main --scale small --autoload

Use it in Virtuoso: add `load("/path/to/crescendo/load.il")` to your `.cdsinit`, restart, then Maestro → Plug-in → Open Crescendo Optimizer.

Requirements: Python 3.6+ with Tk 8.5+. Real optimization needs your own Cadence Virtuoso/Maestro, Spectre or ALPS, PDK and licences.

Known limitations: optimized values are not written back into the original schematic; range checks verify end points only; target-site acceptance (your simulator, cluster and PDK) is up to you. The demo data is synthetic.
