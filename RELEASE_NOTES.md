# Crescendo 1.0.2

Crescendo is a guided circuit optimizer for Cadence Maestro. This first public release contains the window, the optimization engine, the Virtuoso menu loader and a synthetic demo.

Try it without any EDA tools:

    python3 -m crescendo.demo_main --scale small --autoload

Use it in Virtuoso: add `load("/path/to/crescendo/load.il")` to your `.cdsinit`, restart, then Maestro → Plug-in → Open Crescendo Optimizer.

Requirements: Python 3.6+ with Tk 8.5+. Real optimization needs your own Cadence Virtuoso/Maestro, Spectre or ALPS, PDK and licences.

Known limitations: optimized values are not written back into the original schematic; range checks verify end points only; target-site acceptance (your simulator, cluster and PDK) is up to you. The demo data is synthetic.
