# Research Contract (for the next phase)

Do not train a model until this document is made experiment-specific and frozen in Git.

Initial candidate experiment:

- Universe: Pump-created, SOL-quoted tokens passing explicitly defined data-quality filters.
- Decision horizons: 5s / 15s / 30s / 60s / 180s / 300s after a clearly defined token-origin event.
- First primary decision time: 60 seconds.
- Candidate label: whether a realistically executable position reaches +100% before -40% within the following 30 minutes.
- Secondary labels: +50% before -30%, graduation/migration, forward returns at fixed horizons, time-to-barrier.
- Split: temporal walk-forward only. No random train/test split.
- Baselines: unconditional rate, simple threshold rules, logistic regression, then gradient boosting.
- Model selection metric: economic value and calibration/precision-at-k under OOS execution assumptions; not accuracy alone.
- Execution stress tests: multiple latencies, position sizes, fee assumptions and price impact.
- Kill criteria: signal disappears net of conservative costs; only a handful of extreme winners drive PnL; performance is unstable across time; confidence intervals overlap economically irrelevant performance; point-in-time wallet/graph features fail; or the edge requires latency/infrastructure outside the project's feasible budget.

No live capital until a separate paper-trading gate is defined and passed.
