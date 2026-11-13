# Crescendo 1.1.3

Crescendo is a guided circuit optimizer for Cadence Maestro. Each release contains the window, the optimization engine, the Virtuoso menu loader and a synthetic demo.

Version 1.1.2 names the execution modes Native Run, Maestro Netlist and Template Netlist, and calls owned setup/view copies working copies. The English and Chinese READMEs explain what is created, where data lives, what survives Stop/exit, and how to recover or clean up. Existing configuration keys, backend IDs and checkpoint behavior are unchanged. 1.1.3 adds a documented headless optimizer API (docs/HEADLESS_API.md) for tools that only need ask/tell. See CHANGELOG.md.

Try it without any EDA tools:

    python3 -m crescendo.demo_main --scale small --autoload

Use it in Virtuoso: add `load("/path/to/crescendo/load.il")` to your `.cdsinit`, restart, then Maestro → Plug-in → Open Crescendo Optimizer.

Requirements: Python 3.6+ with Tk 8.5+. Real optimization needs your own Cadence Virtuoso/Maestro, Spectre or ALPS, PDK and licences.

Known limitations: optimized values are not written back into the original schematic; range checks verify end points only; target-site acceptance (your simulator, cluster and PDK) is up to you. The demo data is synthetic.
