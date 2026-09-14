# Recovery

Filtered Solana `blockSubscribe` only sends a notification when a block contains a transaction mentioning the filter key. Therefore a jump from slot 100 to slot 107 is **not** evidence that six matching slots were lost.

Recovery is signature-based:

1. For each enabled program, persist the latest observed signature/slot.
2. On reconnect, establish the websocket subscription first.
3. Query `getSignaturesForAddress(program, until=checkpoint_signature)` over HTTP.
4. Fetch missing transactions with `getTransaction`.
5. Replay oldest to newest.
6. A dedicated websocket reader buffers live notifications throughout steps 3-5.
7. Consume the bounded buffer after recovery; dedupe overlap by signature/program observation.

On a brand-new data directory, the collector first records a chain-tip signature as its boundary. It does not backfill arbitrarily old history. Once subscriptions are active it reconciles from that boundary, closing the startup race.

## Fail-closed cap

If more than `MEMEQUANT_MAX_RECONCILE_SIGNATURES` are missing, the collector exits with status
2 instead of processing only the newest N and moving the checkpoint. The integrity exception is
terminal and is deliberately excluded from the reconnect loop. Advancing over the unseen middle
would create permanent silent data loss; blindly retrying the same scan would only consume RPC
credits. Inspect the checkpoint and gap, then choose a deliberate bounded recovery plan.

The same terminal policy applies if the bounded websocket recovery queue fills or a listed
historical transaction remains unavailable after bounded retries. Ordinary WebSocket/RPC
connection failures still reconnect with exponential backoff.

## Dual-program transactions

A migration transaction can mention/invoke both Pump and PumpSwap. Transaction decoding is global per signature; observation/checkpoint state is per program. A duplicate notification from the second subscription records that observation without producing duplicate events.

## Durability

RAW gzip output is flushed per transaction/log notification before SQLite checkpointing.
`MEMEQUANT_RAW_FSYNC_EVERY` controls stronger OS-level durability; `1` is safest and slowest,
`100` is the default compromise, `0` disables explicit fsync. After any abnormal shutdown,
rebuild derived research data from RAW.

## Fail-closed transaction fetches

If `getSignaturesForAddress` lists a missing signature but `getTransaction` remains null
after bounded retries, reconciliation stops and leaves the checkpoint unchanged. Continuing
to later signatures would make a silent permanent hole possible. Investigate provider
history availability or retry later; do not bypass the signature.

## Failed log notifications

`logsSubscribe` includes the transaction error status. Notifications with `err != null` cannot
have committed economic effects, so the collector counts them and filters them before RAW.
Consequently, those notification-only failures are not present in RAW.
Failed transactions received with full data through `blockSubscribe` or replay remain in RAW and
are excluded only from normalized economic tables.

Failed log notifications are discarded by the websocket reader before enqueue. They therefore
increase `ws_notifications_received`, `failed_log_notifications`, and
`failed_log_notifications_skipped`, but not `ws_notifications_enqueued` or
`ws_notifications_processed`.

## Capacity boundary

The application queue is bounded by `MEMEQUANT_WS_QUEUE_MAXSIZE`; it prevents unbounded memory
growth and makes saturation explicit. Normal `logsSubscribe` processing is local and no longer
coupled to `MEMEQUANT_HTTP_RPS_LIMIT`. During signature-based reconnect reconciliation, however,
historical successful transactions still require `getTransaction` and new websocket messages
remain buffered. A long recovery can therefore still reach the bounded queue and fail closed;
this is a recovery-duration limit, not a steady-state 6-RPS ceiling.

## Operational counters

- `notifications`: legacy count of recognized notifications taken from the processing queue.
- `ws_notifications_received`: recognized log/block notifications received from the socket,
  before filtering or enqueue.
- `ws_notifications_enqueued`: recognized notifications successfully admitted to the queue.
- `ws_notifications_processed`: recognized notifications taken from the queue by a consumer.
- `ws_messages_received`: all application payloads. `ws_messages_enqueued` counts only recognized
  notifications admitted to the processing queue.
- `ws_subscription_acks_received`: subscription acknowledgements consumed during the handshake;
  known ACKs are not enqueued.
- `ws_non_notification_messages_received`: unexpected/intermediate application payloads.
- `ws_queue_current_size`: queue size at the latest telemetry flush.
- `ws_queue_high_watermark`: maximum queue size seen across runs using the same state database.
- `successful_log_notifications` / `failed_log_notifications`: log notifications classified at
  receipt from their `err` field.
- `getTransaction_started` / `getTransaction_completed`: logical calls started and responses
  returned by recovery/selective enrichment, including null results in `completed`. Steady-state
  successful log notifications do not increment them.
- `getTransaction_retries`: HTTP 429/503 retries plus retries after a null transaction result.
- `http_429_count`: HTTP responses with status 429.
- `successful_log_avg_millirps` / `successful_log_ema_millirps`: average and EMA successful-log
  arrival rate multiplied by 1,000. For example, `5250` means approximately 5.25/s.

Telemetry increments are committed in batches of 100 received WebSocket messages and are forced
on connection exit, overflow, and normal collector close. This avoids a SQLite commit per message.
An uncatchable process kill can omit the final sub-100 telemetry batch, but does not change RAW or
checkpoint write ordering.
