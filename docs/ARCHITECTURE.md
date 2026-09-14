# Architecture

```text
Solana RPC/WSS
   |
   | blockSubscribe (preferred)
   | logsSubscribe + getTransaction (fallback)
   v
LiveCollector ---------> Reconciler
   |                         |
   +----------+--------------+
              v
     RawTransactionEnvelope
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
- Derived datasets can be atomically rebuilt from RAW.
- No message broker/database cluster until measured throughput requires one.

This architecture is intentionally boring. Kafka/ClickHouse/Rust can be justified later by measured bottlenecks rather than aesthetics.
