from __future__ import annotations

import logging
from collections.abc import Iterable

from memequant.models import CollectorMetrics, RawLogNotification, RawTransactionEnvelope
from memequant.protocols.decoder import ProtocolDecoder
from memequant.protocols.normalize import normalize_event
from memequant.storage.normalized import NormalizedStore
from memequant.storage.raw import RawJsonlGzipStore, RawLogJsonlGzipStore
from memequant.storage.state import StateStore
from memequant.utils import utc_now

log = logging.getLogger(__name__)


class IngestionEngine:
    """Decode/persist transaction or log records while keeping RAW authoritative.

    Live ingestion persists RAW before any checkpoint can advance. Normalized files are
    intentionally a rebuildable derived layer; `memequant-rebuild` should be used before
    serious research after crashes or schema upgrades.
    """

    def __init__(
        self,
        *,
        state: StateStore,
        raw_store: RawJsonlGzipStore,
        raw_log_store: RawLogJsonlGzipStore | None = None,
        normalized_store: NormalizedStore,
        decoders: Iterable[ProtocolDecoder],
    ):
        self.state = state
        self.raw_store = raw_store
        self.raw_log_store = raw_log_store
        self.normalized_store = normalized_store
        self.decoders_by_program = {d.program_id: d for d in decoders}
        self.metrics = CollectorMetrics(started_at=utc_now())

    def process(
        self,
        env: RawTransactionEnvelope,
        *,
        persist_raw: bool = True,
        update_checkpoint: bool = True,
    ) -> bool:
        self.metrics.transactions_seen += 1
        received_iso = env.received_at.isoformat()

        if self.state.transaction_seen(env.signature):
            self.metrics.duplicate_transactions += 1
            self.state.increment("duplicate_transactions")
            if update_checkpoint and self.state.mark_observation(
                env.signature, env.source_program, env.slot, received_iso
            ):
                self.state.set_checkpoint(
                    env.source_program, env.signature, env.slot, received_iso
                )
            return False

        # Write-ahead ordering: in live mode the authoritative raw row is flushed before
        # SQLite is allowed to remember/checkpoint the transaction.
        if persist_raw:
            self.raw_store.append(env)

        meta = env.rpc_transaction.get("meta") or {}
        failed = meta.get("err") is not None
        counters = self._decode_and_store(
            failed=failed,
            decoded=(
                decoder.decode_transaction(env)
                for decoder in self.decoders_by_program.values()
            ),
        )

        inserted = self.state.mark_transaction(
            env.signature, env.slot, env.source_program, received_iso
        )
        if update_checkpoint:
            self.state.mark_observation(
                env.signature, env.source_program, env.slot, received_iso
            )
            self.state.set_checkpoint(
                env.source_program, env.signature, env.slot, received_iso
            )
        if inserted:
            counters["transactions"] = counters.get("transactions", 0) + 1
        self.state.increment_many(counters)
        return inserted

    def process_log(
        self,
        notification: RawLogNotification,
        *,
        persist_raw: bool = True,
        update_checkpoint: bool = True,
    ) -> bool:
        """Persist and decode a logsSubscribe observation without fetching its transaction."""
        self.metrics.transactions_seen += 1
        received_iso = notification.received_at.isoformat()

        # Every accepted streaming observation is authoritative RAW, including overlap
        # with reconciliation or another program subscription. State dedupes derivation.
        if persist_raw:
            if self.raw_log_store is None:
                raise RuntimeError("raw_log_store is required to persist log notifications")
            self.raw_log_store.append(notification)

        if self.state.transaction_seen(notification.signature):
            self.metrics.duplicate_transactions += 1
            self.state.increment("duplicate_transactions")
            if update_checkpoint and self.state.mark_observation(
                notification.signature,
                notification.source_program,
                notification.slot,
                received_iso,
            ):
                self.state.set_checkpoint(
                    notification.source_program,
                    notification.signature,
                    notification.slot,
                    received_iso,
                )
            return False

        counters = self._decode_and_store(
            failed=notification.err is not None,
            decoded=(
                decoder.decode_log_notification(notification)
                for decoder in self.decoders_by_program.values()
            ),
        )
        inserted = self.state.mark_transaction(
            notification.signature,
            notification.slot,
            notification.source_program,
            received_iso,
        )
        if update_checkpoint:
            self.state.mark_observation(
                notification.signature,
                notification.source_program,
                notification.slot,
                received_iso,
            )
            self.state.set_checkpoint(
                notification.source_program,
                notification.signature,
                notification.slot,
                received_iso,
            )
        if inserted:
            counters["transactions"] = counters.get("transactions", 0) + 1
        self.state.increment_many(counters)
        return inserted

    def _decode_and_store(self, *, failed: bool, decoded) -> dict[str, int]:
        counters: dict[str, int] = {}

        def count(key: str, n: int = 1) -> None:
            counters[key] = counters.get(key, 0) + n

        if failed:
            self.metrics.failed_transactions += 1
            count("failed_transactions")
            return counters

        for events, unknown in decoded:
            for event in events:
                if not self.state.mark_event(
                    event.event_id, event.signature, event.event_type, event.slot
                ):
                    continue
                self.normalized_store.append("events", event)
                normalized = normalize_event(event)
                if normalized:
                    dataset, record = normalized
                    self.normalized_store.append(dataset, record)
                    if dataset == "tokens":
                        self.metrics.tokens_created += 1
                        count("tokens_created")
                    elif dataset == "trades":
                        self.metrics.trades += 1
                        count("trades")
                    elif dataset == "completions":
                        self.metrics.completions += 1
                        count("completions")
                    elif dataset == "migrations":
                        self.metrics.migrations += 1
                        count("migrations")
                    elif dataset == "pools":
                        self.metrics.pools_created += 1
                        count("pools_created")
                self.metrics.events_decoded += 1
                count("events_decoded")

            for item in unknown:
                if self.state.mark_event(
                    item.event_id, item.signature, "UnknownProgramData", item.slot
                ):
                    self.normalized_store.append("unknown_program_data", item)
                    self.metrics.unknown_program_data += 1
                    count("unknown_program_data")
        return counters

    def close(self) -> None:
        self.raw_store.close()
        if self.raw_log_store is not None:
            self.raw_log_store.close()
        self.normalized_store.close()
