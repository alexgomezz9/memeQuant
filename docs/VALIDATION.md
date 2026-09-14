# Validation status

This document separates what has actually been exercised from what remains to be
validated on a real Solana RPC provider. Do not interpret a passing offline suite as
proof that the mainnet feed is complete.

## Exercised in the build environment

- Python source compilation with `compileall`.
- Editable package build/install without dependency downloads.
- Full offline `pytest` suite.
- Anchor/Borsh primitives and event decoding using deterministic fixtures.
- Nested Solana program-log attribution.
- Pump event normalization, including migration.
- Failed-transaction exclusion from economic normalized data.
- Signature/event deduplication and per-program observations.
- Checkpoint non-regression by slot.
- Reconciliation ordering, hard reconciliation limits, and null-transaction fail-closed behavior.
- Terminal reconciliation guards do not enter the reconnect loop; transient RPC failures do.
- Bounded WebSocket recovery buffering and live/reconciliation overlap deduplication.
- Immediate propagation of queue overflow even when buffered messages remain.
- Batched receive/enqueue/process, queue-watermark, RPC retry and HTTP 429 counters.
- Realistic `logsSubscribe` notification -> partial RAW -> direct Anchor decode -> normalized
  trade -> offline replay, with deterministic counts and values.
- Shared decoded fields from a full transaction fixture and its logs-only equivalent are equal.
- Successful and failed live `logsSubscribe` notifications avoid `getTransaction`; failed ones
  are counted and filtered before the queue.
- Global HTTP request pacing and API-key redaction in messages, structured fields and tracebacks.
- RAW gzip append/read and rebuild-from-RAW behavior.
- Replay of the 158-row local RAW fixture into Parquet (158 unique transactions, 47 events,
  46 trades) with integer financial columns.
- JSONL normalized fallback used when PyArrow is unavailable.
- Local `memequant-doctor` checks and installed console entry points.

## Not exercised in the build environment

The build environment has no user RPC credentials and no Docker daemon. Therefore these
items are intentionally **not claimed as validated** yet:

- live Solana mainnet ingestion through your Helius endpoint;
- whether your exact Helius plan exposes `blockSubscribe`;
- measured Pump.fun bandwidth / Helius credit consumption per day;
- live reconciliation after a real connection loss;
- sustained live direct-log throughput and provider-specific log truncation behavior;
- DuckDB queries in this environment;
- Docker image build/start in this environment;
- semantic equality against a statistically meaningful sample of live Pump.fun UI or
  second-provider observations;
- historical Pump schema decoding before the bundled 2026-09-10 event snapshots.

## First live acceptance test

After installing normal dependencies and adding RPC URLs locally:

1. Run `memequant-doctor --network`.
2. Run the collector for a short supervised window with `MEMEQUANT_PROTOCOLS=pump`.
3. Stop cleanly and run `memequant-stats`.
4. Rebuild with `memequant-replay --output data/rebuilt_normalized`.
5. Compare live normalized counts and rebuilt counts.
6. Inspect every `unknown_program_data` row. Schema-drift rows are a blocking issue.
7. Validate a random sample of creations/trades/migrations against an independent source.
8. Only after that run a 24-hour collection window and measure data volume, reconnects,
   fetch failures, unknown-event rate and RPC credit consumption.

No ML dataset should be treated as research-grade until this acceptance test passes.
