from __future__ import annotations

import asyncio
import itertools
import json
import logging

import websockets

from memequant.config import Settings
from memequant.constants import PROGRAMS
from memequant.engine import IngestionEngine
from memequant.ingestion.extract import (
    envelope_from_get_transaction,
    envelopes_from_block_notification,
)
from memequant.ingestion.reconcile import Reconciler
from memequant.rpc import RpcError, SolanaHttpRpc
from memequant.utils import utc_now

log = logging.getLogger(__name__)


class LiveCollector:
    def __init__(self, settings: Settings, engine: IngestionEngine):
        settings.require_live_urls()
        self.settings = settings
        self.engine = engine
        self.rpc = SolanaHttpRpc(
            settings.solana_http_rpc_url,
            concurrency=settings.http_concurrency,
            max_tx_version=settings.max_supported_transaction_version,
        )
        self.reconciler = Reconciler(
            self.rpc,
            engine.state,
            settings.provider_name,
            settings.commitment,
            settings.max_reconcile_signatures,
        )
        self._request_ids = itertools.count(1)
        self._ever_subscribed = False
        self.program_ids = tuple(PROGRAMS[name] for name in settings.enabled_protocols)

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
            reply = json.loads(await ws.recv())
            if "error" in reply:
                raise RpcError("blockSubscribe", reply["error"])
            request_id = reply.get("id")
            if request_id in pending:
                program = pending.pop(request_id)
                subscriptions[int(reply["result"])] = program
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
            reply = json.loads(await ws.recv())
            if "error" in reply:
                raise RpcError("logsSubscribe", reply["error"])
            request_id = reply.get("id")
            if request_id in pending:
                program = pending.pop(request_id)
                subscriptions[int(reply["result"])] = program
        return subscriptions

    async def _consume_block(self, ws, subscriptions: dict[int, str]) -> None:
        async for raw_message in ws:
            received = utc_now()
            message = json.loads(raw_message)
            if message.get("method") != "blockNotification":
                continue
            self.engine.metrics.notifications += 1
            self.engine.state.increment_many({
                "notifications": 1,
                "ws_bytes_received": len(raw_message.encode("utf-8")),
            })
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
        # A confirmed log can very briefly race an RPC node's transaction index. Retry
        # null results locally; hard RPC errors propagate to the reconnect/reconcile path.
        delay = 0.15
        for attempt in range(5):
            result = await self.rpc.get_transaction(
                signature, commitment=self.settings.commitment
            )
            if result is not None:
                return result
            if attempt < 4:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.2)
        self.engine.state.increment("transaction_fetch_nulls")
        raise RpcError(
            "getTransaction",
            {"message": f"transaction {signature} remained unavailable after retries"},
        )

    async def _consume_logs(self, ws, subscriptions: dict[int, str]) -> None:
        async for raw_message in ws:
            message = json.loads(raw_message)
            if message.get("method") != "logsNotification":
                continue
            self.engine.metrics.notifications += 1
            self.engine.state.increment_many({
                "notifications": 1,
                "ws_bytes_received": len(raw_message.encode("utf-8")),
            })
            params = message.get("params") or {}
            subscription = int(params.get("subscription", -1))
            program_id = subscriptions.get(subscription)
            value = ((params.get("result") or {}).get("value") or {})
            signature = value.get("signature")
            if (
                not program_id
                or not signature
                or self.engine.state.observation_seen(signature, program_id)
            ):
                continue
            # Fail closed on fetch errors/null exhaustion. Continuing to later signatures
            # would advance the checkpoint over a transaction we never persisted.
            result = await self._fetch_transaction_with_retry(signature)
            env = envelope_from_get_transaction(
                result,
                signature=signature,
                program_id=program_id,
                provider=self.settings.provider_name,
                received_at=utc_now(),
                source_mode="logs",
            )
            self.engine.process(env)

    async def _one_connection(self, mode: str, *, reconcile_after_subscribe: bool) -> None:
        async with websockets.connect(
            self.settings.solana_ws_rpc_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=32 * 1024 * 1024,
        ) as ws:
            self.engine.metrics.websocket_connections += 1
            if mode == "block":
                subs = await self._subscribe_block(ws)
            else:
                subs = await self._subscribe_logs(ws)
            log.info("subscriptions active", extra={"mode": mode, "count": len(subs)})

            # Reconcile only after subscriptions are active. Messages arriving while HTTP
            # reconciliation runs remain queued on the websocket and are deduplicated later.
            if reconcile_after_subscribe:
                await self._reconcile_all()
            self._ever_subscribed = True

            if mode == "block":
                await self._consume_block(ws, subs)
            else:
                await self._consume_logs(ws, subs)

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
            except RpcError as exc:
                if configured == "auto" and active_mode == "block":
                    log.warning(
                        "blockSubscribe unavailable; falling back to logsSubscribe + getTransaction",
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
        await self.rpc.close()
