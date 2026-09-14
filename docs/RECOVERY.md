# Recovery

Filtered Solana `blockSubscribe` only sends a notification when a block contains a transaction mentioning the filter key. Therefore a jump from slot 100 to slot 107 is **not** evidence that six matching slots were lost.

Recovery is signature-based:

1. For each enabled program, persist the latest observed signature/slot.
2. On reconnect, establish the websocket subscription first.
3. Query `getSignaturesForAddress(program, until=checkpoint_signature)` over HTTP.
4. Fetch missing transactions with `getTransaction`.
5. Replay oldest to newest.
6. Consume websocket messages that accumulated during recovery; dedupe them.

On a brand-new data directory, the collector first records a chain-tip signature as its boundary. It does not backfill arbitrarily old history. Once subscriptions are active it reconciles from that boundary, closing the startup race.

## Fail-closed cap

If more than `MEMEQUANT_MAX_RECONCILE_SIGNATURES` are missing, the collector raises instead of processing only the newest N and moving the checkpoint. Advancing over the unseen middle would create permanent silent data loss.

## Dual-program transactions

A migration transaction can mention/invoke both Pump and PumpSwap. Transaction decoding is global per signature; observation/checkpoint state is per program. A duplicate notification from the second subscription records that observation without producing duplicate events.

## Durability

RAW gzip output is flushed per transaction before SQLite checkpointing. `MEMEQUANT_RAW_FSYNC_EVERY` controls stronger OS-level durability; `1` is safest and slowest, `100` is the default compromise, `0` disables explicit fsync. After any abnormal shutdown, rebuild derived research data from RAW.

## Fail-closed transaction fetches

If `getSignaturesForAddress` lists a missing signature but `getTransaction` remains null
after bounded retries, reconciliation stops and leaves the checkpoint unchanged. Continuing
to later signatures would make a silent permanent hole possible. Investigate provider
history availability or retry later; do not bypass the signature.
