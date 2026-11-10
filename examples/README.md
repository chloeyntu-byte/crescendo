# Examples

All data here is synthetic. Nothing comes from a real process kit or simulator.

- `demo-run.json`: one finished run of the built-in demo service (14 generations; 215 completed candidates, 13–16 per generation after failed simulations are excluded; objectives gain_dB ↑ and power ↓, constraint pm ≥ 55). Each candidate has its generation, the two objective values, the constraint value, feasibility and Pareto membership.

Reproduce a run like it, and watch it live, without any EDA tools:

```bash
python3 -m crescendo.demo_main --scale small --autoload
```

Then follow the six steps in the window (Load and Inspect, tick cells, Apply Selected Cells, W All / L All, add objectives, Start Optimization).

Run the same flow unattended and print timings:

```bash
python3 -m crescendo.demo_smoke --scales small
```
