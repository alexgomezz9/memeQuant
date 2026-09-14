from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime

import websockets

from memequant.config import Settings
from memequant.constants import PROGRAMS
from memequant.engine import IngestionEngine
from memequant.ingestion.extract import (
    envelopes_from_block_notification,
    log_notification_from_message,
)
from memequant.ingestion.reconcile import Reconciler, ReconciliationError
from memequant.rpc import RpcError, SolanaHttpRpc
from memequant.utils import utc_now

log = logging.getLogger(__name__)


class LiveQueueOverflow(ReconciliationError):
    """Live notifications exceeded the bounded recovery buffer."""


class LiveNotificationIntegrityError(ReconciliationError):
    """A websocket notification cannot be represented as authoritative RAW."""


@dataclass(frozen=True)
class BufferedWsMessage:
    raw: str
    received_at: datetime


class LiveCollector:
    def __init__(self, settings: Settings, engine: IngestionEngine):
        settings.require_live_urls()
        self.settings = settings
        self.engine = engine
        self._telemetry_pending: dict[str, int] = {}
        self._telemetry_messages_since_flush = 0
        self._queue_current_size = 0
        self._queue_high_watermark = engine.state.counters().get(
            "ws_queue_high_watermark", 0
        )
        self._successful_log_total = 0
        self._rate_started_at = time.monotonic()
        self._rate_last_at = self._rate_started_at
        self._rate_last_total = 0
        self._successful_log_ema_rps: float | None = None
        self._successful_log_avg_millirps = 0
        self._successful_log_ema_millirps = 0
        self.rpc = SolanaHttpRpc(
            settings.solana_http_rpc_url,
            concurrency=settings.http_concurrency,
            max_tx_version=settings.max_supported_transaction_version,
            rps_limit=settings.http_rps_limit,
            metric_sink=self._bump,
        )
        self.reconciler = Reconciler(
            self.rpc,
            engine.state,
            settings.provider_name,
            settings.commitment,
            settings.max_reconcile_signatures,
            metric_sink=self._bump,
        )
        self._request_ids = itertools.count(1)
        self._ever_subscribed = False
        self._early_messages: list[BufferedWsMessage] = []
        self.program_ids = tuple(PROGRAMS[name] for name in settings.enabled_protocols)

    def _bump(self, key: str, amount: int = 1) -> None:
        self._telemetry_pending[key] = self._telemetry_pending.get(key, 0) + amount

    def _flush_telemetry(self, *, force: bool = False) -> None:
        if not force and self._telemetry_messages_since_flush < 100:
            return

        if self._telemetry_messages_since_flush:
            now = time.monotonic()
            total_elapsed = max(now - self._rate_started_at, 1e-9)
            interval_elapsed = max(now - self._rate_last_at, 1e-9)
            interval_count = self._successful_log_total - self._rate_last_total
            interval_rate = interval_count / interval_elapsed
            if self._successful_log_ema_rps is None:
                self._successful_log_ema_rps = interval_rate
            else:
                self._successful_log_ema_rps = (
                    0.2 * interval_rate + 0.8 * self._successful_log_ema_rps
                )
            self._successful_log_avg_millirps = round(
                1_000 * self._successful_log_total / total_elapsed
            )
            self._successful_log_ema_millirps = round(
                1_000 * self._successful_log_ema_rps
            )
            self._rate_last_at = now
            self._rate_last_total = self._successful_log_total

        gauges = {
            "ws_queue_current_size": self._queue_current_size,
            "successful_log_avg_millirps": self._successful_log_avg_millirps,
            "successful_log_ema_millirps": self._successful_log_ema_millirps,
        }
        self.engine.state.update_counters(
            increments=self._telemetry_pending,
            gauges=gauges,
            maxima={"ws_queue_high_watermark": self._queue_high_watermark},
        )
        self._telemetry_pending.clear()
        self._telemetry_messages_since_flush = 0

    @staticmethod
    def _message_details(raw_message: str) -> tuple[bool, bool | None]:
        """Return (is notification, log success status when applicable)."""
        try:
            message = json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return False, None
        if not isinstance(message, dict):
            return False, None
        method = message.get("method")
        if method == "logsNotification":
            value = (((message.get("params") or {}).get("result") or {}).get("value") or {})
            if not isinstance(value, dict) or not value.get("signature") or "err" not in value:
                return True, None
            return True, value.get("err") is None
        return method == "blockNotification", None

    def _record_received(self, raw_message: str) -> tuple[bool, bool | None]:
        is_notification, log_succeeded = self._message_details(raw_message)
        self._bump("ws_messages_received")
        self._bump("ws_bytes_received", len(raw_message.encode("utf-8")))
        self._telemetry_messages_since_flush += 1
        if is_notification:
            self._bump("ws_notifications_received")
        else:
            self._bump("ws_non_notification_messages_received")
        if log_succeeded is True:
            self._bump("successful_log_notifications")
            self._successful_log_total += 1
        elif log_succeeded is False:
            self._bump("failed_log_notifications")
        return is_notification, log_succeeded

    def _record_enqueued(self, is_notification: bool, current_size: int) -> None:
        self._bump("ws_messages_enqueued")
        if is_notification:
            self._bump("ws_notifications_enqueued")
        self._queue_current_size = current_size
        self._queue_high_watermark = max(self._queue_high_watermark, current_size)
        self._flush_telemetry()

    def _record_subscription_ack(self, raw_message: str) -> None:
        self._bump("ws_messages_received")
        self._bump("ws_bytes_received", len(raw_message.encode("utf-8")))
        self._bump("ws_subscription_acks_received")
        self._telemetry_messages_since_flush += 1
        self._flush_telemetry()

    def _buffer_early_message(self, raw_message: str) -> None:
        """Retain notifications interleaved with multi-subscription acknowledgements."""
        is_notification, log_succeeded = self._record_received(raw_message)
        if not is_notification:
            self._flush_telemetry()
            return
        if log_succeeded is False:
            self._bump("failed_log_notifications_skipped")
            self._flush_telemetry()
            return
        if len(self._early_messages) >= self.settings.ws_queue_maxsize:
            self._bump("ws_queue_overflows")
            self._queue_current_size = len(self._early_messages)
            self._flush_telemetry(force=True)
            raise LiveQueueOverflow(
                f"websocket recovery buffer reached {self.settings.ws_queue_maxsize} messages "
                "during subscription; collector halted without accepting a possible gap"
            )
        self._early_messages.append(BufferedWsMessage(raw_message, utc_now()))
        self._record_enqueued(is_notification, len(self._early_messages))

    async def _read_messages(
        self, ws, queue: asyncio.Queue[BufferedWsMessage]
    ) -> None:
        """Continuously drain the socket into a bounded application buffer."""
        async for raw_message in ws:
            is_notification, log_succeeded = self._record_received(raw_message)
            if not is_notification:
                self._flush_telemetry()
                continue
            if log_succeeded is False:
                self._bump("failed_log_notifications_skipped")
                self._flush_telemetry()
                continue
            try:
                queue.put_nowait(BufferedWsMessage(raw_message, utc_now()))
            except asyncio.QueueFull as exc:
                self._bump("ws_queue_overflows")
                self._queue_current_size = queue.qsize()
                self._flush_telemetry(force=True)
                raise LiveQueueOverflow(
                    f"websocket recovery buffer reached {queue.maxsize} messages; "
                    "collector halted without accepting a possible gap"
                ) from exc
            self._record_enqueued(is_notification, queue.qsize())

    async def _queued_messages(
        self,
        queue: asyncio.Queue[BufferedWsMessage],
        reader: asyncio.Task[None],
    ) -> AsyncIterator[BufferedWsMessage]:
        """Yield buffered messages and surface socket closure instead of busy reconnecting."""
        while True:
            if (
                reader.done()
                and not reader.cancelled()
                and reader.exception() is not None
            ):
                await reader
            if not queue.empty():
                item = queue.get_nowait()
                self._queue_current_size = queue.qsize()
                yield item
                continue
            if reader.done():
                await reader
                raise ConnectionError("websocket closed")

            item = asyncio.create_task(queue.get())
            done, _ = await asyncio.wait({item, reader}, return_when=asyncio.FIRST_COMPLETED)
            if item in done:
                self._queue_current_size = queue.qsize()
                yield item.result()
                continue

            item.cancel()
            with suppress(asyncio.CancelledError):
                await item
            await reader
            raise ConnectionError("websocket closed")

    async def _reconcile_while_reading(
        self,
        reader: asyncio.Task[None],
    ) -> None:
        reconciliation = asyncio.create_task(self._reconcile_all())
        done, _ = await asyncio.wait(
            {reader, reconciliation}, return_when=asyncio.FIRST_COMPLETED
        )
        if reader in done:
            reconciliation.cancel()
            with suppress(asyncio.CancelledError):
                await reconciliation
            await reader
            raise ConnectionError("websocket closed during reconciliation")
        await reconciliation

    async def _reconcile_all(self) -> None:
        # Fail closed: consuming new websocket messages after a failed reconciliation
        # could advance checkpoints over data we never persisted.
        for program_id in self.program_ids:
            n = await self.reconciler.reconcile(program_id, self.engine.process)
            self.engine.metrics.reconciled_transactions += n
            if n:
                log.info(
                    "reconciled transactions", extra={"program": program_id, "count": n}
                )

    async def _subscribe_block(self, ws) -> dict[int, str]:
        pending: dict[int, str] = {}
        for program_id in self.program_ids:
            request_id = next(self._request_ids)
            pending[request_id] = program_id
            await ws.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "blockSubscribe",
                        "params": [
                            {"mentionsAccountOrProgram": program_id},
                            {
                                "commitment": self.settings.commitment,
                                "encoding": "json",
                                "transactionDetails": "full",
                                "maxSupportedTransactionVersion": self.settings.max_supported_transaction_version,
                                "showRewards": False,
                            },
                        ],
                    }
                )
            )

        subscriptions: dict[int, str] = {}
        while pending:
            raw_reply = await ws.recv()
            reply = json.loads(raw_reply)
            if "error" in reply:
                raise RpcError("blockSubscribe", reply["error"])
            request_id = reply.get("id")
            if request_id in pending:
                program = pending.pop(request_id)
                subscriptions[int(reply["result"])] = program
                self._record_subscription_ack(raw_reply)
            else:
                self._buffer_early_message(raw_reply)
        return subscriptions

    async def _subscribe_logs(self, ws) -> dict[int, str]:
        pending: dict[int, str] = {}
        for program_id in self.program_ids:
            request_id = next(self._request_ids)
            pending[request_id] = program_id
            await ws.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [program_id]},
                            {"commitment": self.settings.commitment},
                        ],
                    }
                )
            )
        subscriptions: dict[int, str] = {}
        while pending:
            raw_reply = await ws.recv()
            reply = json.loads(raw_reply)
            if "error" in reply:
                raise RpcError("logsSubscribe", reply["error"])
            request_id = reply.get("id")
            if request_id in pending:
                program = pending.pop(request_id)
                subscriptions[int(reply["result"])] = program
                self._record_subscription_ack(raw_reply)
            else:
                self._buffer_early_message(raw_reply)
        return subscriptions

    async def _consume_block(
        self,
        messages: AsyncIterator[str | BufferedWsMessage],
        subscriptions: dict[int, str],
    ) -> None:
        async for buffered in messages:
            raw_message, received = self._unpack_message(buffered)
            message = json.loads(raw_message)
            if message.get("method") != "blockNotification":
                continue
            self.engine.metrics.notifications += 1
            self._bump("notifications")
            self._bump("ws_notifications_processed")
            subscription = int((message.get("params") or {}).get("subscription", -1))
            program_id = subscriptions.get(subscription)
            if not program_id:
                continue
            result = (message.get("params") or {}).get("result") or {}
            value = result.get("value")
            if isinstance(value, dict) and value.get("err") is not None:
                raise RpcError("blockSubscribe", value["err"])
            if isinstance(value, dict) and "block" in value and value["block"] is None:
                raise RpcError(
                    "blockSubscribe",
                    {"message": "received block:null; verify maxSupportedTransactionVersion/provider"},
                )
            for env in envelopes_from_block_notification(
                message,
                program_id=program_id,
                provider=self.settings.provider_name,
                received_at=received,
            ):
                self.engine.process(env)

    async def _fetch_transaction_with_retry(self, signature: str):
        """Explicit enrichment hook; the live log consumer never calls this method."""
        # A newly confirmed signature can briefly race the provider's transaction index.
        delay = 0.15
        for attempt in range(5):
            result = await self.rpc.get_transaction(
                signature, commitment=self.settings.commitment
            )
            if result is not None:
                return result
            if attempt < 4:
                self._bump("getTransaction_retries")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.2)
        self.engine.state.increment("transaction_fetch_nulls")
        raise RpcError(
            "getTransaction",
            {"message": f"transaction {signature} remained unavailable after retries"},
        )

    async def _consume_logs(
        self,
        messages: AsyncIterator[str | BufferedWsMessage],
        subscriptions: dict[int, str],
    ) -> None:
        async for buffered in messages:
            raw_message, received_at = self._unpack_message(buffered)
            message = json.loads(raw_message)
            if message.get("method") != "logsNotification":
                continue
            self.engine.metrics.notifications += 1
            self._bump("notifications")
            self._bump("ws_notifications_processed")
            params = message.get("params") or {}
            subscription = int(params.get("subscription", -1))
            program_id = subscriptions.get(subscription)
            value = ((params.get("result") or {}).get("value") or {})
            if not program_id:
                raise LiveNotificationIntegrityError(
                    f"logsNotification references unknown subscription {subscription}"
                )
            # Failed transactions cannot create economic state. logsSubscribe already tells
            # us they failed, so count the notification without spending a getTransaction.
            if value.get("err") is not None:
                self._bump("failed_log_notifications_skipped")
                continue
            try:
                notification = log_notification_from_message(
                    message,
                    program_id=program_id,
                    provider=self.settings.provider_name,
                    received_at=received_at,
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise LiveNotificationIntegrityError(str(exc)) from exc
            self.engine.process_log(notification)

    @staticmethod
    def _unpack_message(
        message: str | BufferedWsMessage,
    ) -> tuple[str, datetime]:
        if isinstance(message, BufferedWsMessage):
            return message.raw, message.received_at
        return message, utc_now()

    async def _one_connection(self, mode: str, *, reconcile_after_subscribe: bool) -> None:
        async with websockets.connect(
            self.settings.solana_ws_rpc_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=32 * 1024 * 1024,
        ) as ws:
            self.engine.metrics.websocket_connections += 1
            self._early_messages.clear()
            if mode == "block":
                subs = await self._subscribe_block(ws)
            else:
                subs = await self._subscribe_logs(ws)
            log.info("subscriptions active", extra={"mode": mode, "count": len(subs)})

            queue: asyncio.Queue[BufferedWsMessage] = asyncio.Queue(
                maxsize=self.settings.ws_queue_maxsize
            )
            for raw_message in self._early_messages:
                queue.put_nowait(raw_message)
            self._queue_current_size = queue.qsize()
            self._queue_high_watermark = max(
                self._queue_high_watermark, self._queue_current_size
            )
            self._early_messages.clear()
            reader = asyncio.create_task(self._read_messages(ws, queue))
            try:
                # Reconcile only after subscriptions are active. A dedicated reader drains
                # the socket while HTTP recovery runs; its bounded queue is then deduplicated.
                if reconcile_after_subscribe:
                    await self._reconcile_while_reading(reader)
                self._ever_subscribed = True
                messages = self._queued_messages(queue, reader)

                if mode == "block":
                    await self._consume_block(messages, subs)
                else:
                    await self._consume_logs(messages, subs)
            finally:
                self._queue_current_size = queue.qsize()
                self._flush_telemetry(force=True)
                reader.cancel()
                with suppress(asyncio.CancelledError):
                    await reader

    async def run(self) -> None:
        configured = self.settings.subscription_mode
        active_mode = "block" if configured in {"auto", "block"} else "logs"
        delay = self.settings.reconnect_min_seconds

        if self.settings.reconcile_on_start:
            # Brand-new installs start at the current tip rather than unexpectedly
            # downloading history. The post-subscription reconciliation closes the race.
            for program_id in self.program_ids:
                await self.reconciler.bootstrap_if_needed(program_id)

        while True:
            try:
                should_reconcile = (
                    (not self._ever_subscribed and self.settings.reconcile_on_start)
                    or (self._ever_subscribed and self.settings.reconcile_on_reconnect)
                )
                await self._one_connection(
                    active_mode, reconcile_after_subscribe=should_reconcile
                )
                delay = self.settings.reconnect_min_seconds
            except asyncio.CancelledError:
                raise
            except KeyboardInterrupt:
                raise
            except ReconciliationError:
                # These are persistent integrity failures, not connection failures. Retrying
                # would repeat the same RPC work forever while leaving the checkpoint stuck.
                self.engine.state.increment("reconciliation_halted")
                log.exception("collector halted by reconciliation integrity guard")
                raise
            except RpcError as exc:
                if configured == "auto" and active_mode == "block":
                    log.warning(
                        "blockSubscribe unavailable; falling back to direct logsSubscribe decoding",
                        extra={"mode": "logs", "error_code": str(exc.error)},
                    )
                    active_mode = "logs"
                    delay = self.settings.reconnect_min_seconds
                    continue
                self.engine.metrics.rpc_errors += 1
                self.engine.metrics.reconnects += 1
                log.exception("RPC subscription error")
            except Exception:
                self.engine.metrics.reconnects += 1
                log.exception("websocket connection interrupted")

            await asyncio.sleep(delay)
            delay = min(delay * 2, self.settings.reconnect_max_seconds)

    async def close(self) -> None:
        self._flush_telemetry(force=True)
        await self.rpc.close()
