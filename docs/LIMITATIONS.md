# Known limitations of v0

1. **Live mainnet not verified in this build environment.** Unit/integration tests exercise the protocol and recovery logic offline, but a real Helius/other RPC key is required to validate provider-specific WebSocket behavior and quotas.
2. **Current, not historical, event schemas.** Bundled Pump/PumpSwap event snapshots match the official IDLs checked on 2026-09-10. Historical event layouts may differ even with the same discriminator. Backfill needs an explicit versioned schema catalog rather than guessing from short payloads.
3. **Off-chain metadata not fetched.** v0 stores creation-time `uri` but does not follow arbitrary token-controlled URLs. A later snapshotter needs SSRF protection, size/time limits, content hashing and immutable storage.
4. **Confirmed vs finalized.** Collection defaults to `confirmed` for timeliness. v0 does not yet run a later finalized reconciliation to detect rare fork/orphan changes. Add this before treating confirmed data as archival truth.
5. **PumpSwap bandwidth.** Whole-program PumpSwap collection can be much heavier than Pump. Start with `MEMEQUANT_PROTOCOLS=pump`, measure credits/GB/day, then decide whether whole-program PumpSwap is worth the cost or whether a targeted post-migration feed is needed.
6. **Derived live cache is not canonical.** Rebuild it from RAW after crashes, IDL changes or before formal research runs.
7. **No historical backfill tool yet.** The collector starts from the current tip. Historical acquisition should be a separate, rate-limited job with historical schema handling.
8. **No feature/label/execution layer yet.** Any profitability inference at this stage would be unjustified.
9. **Failed log notifications are not full RAW rows.** In `logsSubscribe` mode, notifications
   carrying `err != null` are counted but not fetched, because they cannot commit economic state.
10. **Reconnect recovery still uses HTTP.** Direct log decoding removes the steady-state
    `getTransaction` bottleneck. A long historical recovery still fetches full transactions while
    live notifications queue, so it can hit the fail-closed cap or queue bound.
11. **Streaming logs are not transaction envelopes.** They do not provide block time,
    transaction index, account keys, instructions, balance deltas, transaction fee/compute usage,
    message version or address-lookup details. Current typed Pump event research does not require
    those fields, but transaction-topology research does.
