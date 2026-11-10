# Changelog

## 1.0.2 (2026-11-10)

First public release (1.0.0 and 1.0.1 were not published).

- Remaining test fixtures use placeholder scheduler job IDs.

## 1.0.1 (2026-11-10)

- Cluster execution is a generic batch scheduler: submit, query and cancel are user-configured command templates; no scheduler executable is assumed.
- The optional visual-transfer log sender was removed.
- Test fixtures use generic host names, job IDs and user names.

## 1.0.0 (2026-11-10)

- Guided six-step window (Design, Parameters, Goals, Run, Results, Trade-offs) with Simple and Pro modes; the UI displays all text in English.
- Run dashboard: evaluated / feasible / best / generation tiles and a best-so-far convergence chart; per-objective convergence on the Results page; Pareto plots joined along the front.
- Engine: private Maestro copies, three execution modes, Spectre / ALPS, Local / Batch scheduler, NSGA-II / differential evolution / Bayesian optimization, checkpoint and resume, single-file configuration save and load, three-stage link diagnostics.
- Large projects: indexed scope matching, cached parameter views, debounced search, incremental result tables and background parameter preparation keep the window responsive at 50,000 parameters.
- Synthetic demo service, scripted smoke test and screenshot tour; no EDA tools needed to try it.
- Release preparation: machine paths, user names, internal host names, foundry/PDK names and licence strings in code and tests were replaced with generic placeholders; vendor-specific PDK adapters were replaced by fictional examples. Remaining fixtures are synthetic.
