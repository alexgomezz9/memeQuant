# Architecture

```text
Solana RPC/WSS
   |
   | blockSubscribe (preferred)
   | logsSubscribe -> direct Anchor event decoding (fallback)
   v
LiveCollector ---------> Reconciler
   |    bounded live queue    |
   +----------+--------------+
              |
       +------+------+
       v             v
RawLogNotification  RawTransactionEnvelope
   (streaming)       (block/recovery/enrichment)
       +------+------+
              v
     Anchor Program data
              |
       RAW JSONL.GZ  <--- authoritative write-ahead archive
              |
              v
     Anchor log attribution
              |
      IDL event decoders
       /             \
    Pump            PumpSwap
       \             /
        typed event normalization
              |
        derived Parquet
              |
          DuckDB / ML
```

## Design choices

- Provider-neutral standard JSON-RPC wherever possible.
- Pure-Python Borsh event decoding avoids coupling ingestion correctness to a large Solana SDK.
- Current IDL schemas are versioned files in the package; schema drift fails closed.
- SQLite stores only dedupe keys/checkpoints/counters, not market history.
- RAW is written before state advances.
- Streaming RAW never pretends to contain a full transaction: it preserves provider,
  program subscription, signature, slot, receipt time, error, context and `logs[]`.
- `getTransaction` is not part of the `logsSubscribe` hot path. It remains available for
  signature-based gap recovery and deliberately selected enrichment/audit samples.
- Derived datasets can be atomically rebuilt from RAW.
- No message broker/database cluster until measured throughput requires one.
- Recovery integrity failures terminate; only transient connection failures reconnect.

This architecture is intentionally boring. Kafka/ClickHouse/Rust can be justified later by measured bottlenecks rather than aesthetics.
