# Data Contract

## Ordering/time fields

- `slot`: Solana ledger slot containing the transaction. Primary coarse chain ordering key.
- `transaction_index`: order within a block when supplied by `blockSubscribe`; unavailable
  through `logsSubscribe` and `getTransaction`.
- `log_index`: index in the transaction's `meta.logMessages`; used to make event IDs deterministic.
- `block_time`: validator-estimated Unix block time. Useful event-time approximation, **not**
  receipt latency. It is unavailable in `logsSubscribe`; Pump/PumpSwap `event_timestamp` remains.
- `event_timestamp`: timestamp emitted by Pump/PumpSwap. Treat as protocol data, not as the collector's clock.
- `received_at`: UTC wall-clock time when this collector received/fetched the transaction. Reconciled transactions therefore have a later `received_at` than their actual live availability.

Never sort solely on `block_time` when slot/order is available. Never use a reconciled `received_at` to pretend the transaction arrived live at that time.

## Point-in-time rule

A feature used for a decision at time `t` must satisfy `available_at <= t`. Future token outcomes, future creator/wallet performance, metadata observed later, later graph edges, and later balances are prohibited.

The future research layer should explicitly carry `feature_cutoff` / `decision_time` and test that no source record exceeds it.

## Integer units

On-chain token/quote amounts stay as integers. Conversion to SOL or display tokens requires the corresponding decimals/mint and belongs in a view/research layer. Never put monetary accounting through IEEE floating point when exact integer/decimal arithmetic is available.

## IDs

- Transaction identity: Solana transaction signature.
- Event identity: SHA-256 of `signature | program_id | log_index | event_type`.
- Unknown program data uses the same signature/program/log location with an `unknown` discriminator class.

## Failed transactions

Failed Solana transactions are kept in RAW for audit/latency/failure analysis but are excluded from normalized economic `trades`, `tokens`, etc. Logs emitted before rollback do not constitute executed trades.

## Source of truth

`data/raw` is authoritative. It contains two honest record shapes:

- `RawLogNotification` in `log_notifications.jsonl.gz`: partial streaming evidence with
  signature, slot, error, context and logs;
- `RawTransactionEnvelope` in `transactions.jsonl.gz`: a complete RPC transaction response
  obtained from block subscription, recovery or selective enrichment.

All normalized/research tables are disposable derived artifacts and must be reproducible from
RAW plus a versioned decoder/configuration.
