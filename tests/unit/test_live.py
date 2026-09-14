import asyncio
import json
from pathlib import Path

import pytest

from memequant.config import Settings
from memequant.ingestion.live import BufferedWsMessage, LiveCollector, LiveQueueOverflow
from memequant.ingestion.reconcile import ReconciliationLimitExceeded
from memequant.rpc import RpcError
from tests.integration.test_engine import engine as make_engine

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


class FakeWs:
    def __init__(self):
        self.sent = []
        self.replies = []

    async def send(self, payload):
        body = json.loads(payload)
        self.sent.append(body)
        self.replies.append(json.dumps({"jsonrpc": "2.0", "id": body["id"], "result": 77}))

    async def recv(self):
        return self.replies.pop(0)


async def messages(*payloads):
    for payload in payloads:
        yield json.dumps(payload)


def log_notification(signature: str, *, err=None) -> dict:
    return {
        "method": "logsNotification",
        "params": {
            "subscription": 77,
            "result": {
                "context": {"slot": 11},
                "value": {"signature": signature, "err": err, "logs": []},
            },
        },
    }


@pytest.mark.asyncio
async def test_block_subscription_payload_uses_enabled_protocol_and_tx_version(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
        MEMEQUANT_PROTOCOLS="pump",
        MEMEQUANT_MAX_SUPPORTED_TRANSACTION_VERSION=1,
    )
    collector = LiveCollector(settings, e)
    ws = FakeWs()
    try:
        subs = await collector._subscribe_block(ws)
        assert subs == {77: PUMP}
        body = ws.sent[0]
        assert body["method"] == "blockSubscribe"
        assert body["params"][0] == {"mentionsAccountOrProgram": PUMP}
        assert body["params"][1]["maxSupportedTransactionVersion"] == 1
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_notification_interleaved_with_subscription_acks_is_buffered(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
        MEMEQUANT_PROTOCOLS="pump,pumpswap",
    )
    collector = LiveCollector(settings, e)

    class InterleavedWs:
        def __init__(self):
            self.sent = []
            self.replies = [
                json.dumps({"jsonrpc": "2.0", "id": 1, "result": 77}),
                json.dumps(log_notification("early")),
                json.dumps({"jsonrpc": "2.0", "id": 2, "result": 88}),
            ]

        async def send(self, payload):
            self.sent.append(json.loads(payload))

        async def recv(self):
            return self.replies.pop(0)

    try:
        subscriptions = await collector._subscribe_logs(InterleavedWs())
        assert set(subscriptions) == {77, 88}
        assert len(collector._early_messages) == 1
        assert json.loads(collector._early_messages[0].raw)["method"] == "logsNotification"
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_null_transaction_exhaustion_raises_instead_of_skipping(tmp_path: Path, monkeypatch):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)

    class NullRpc:
        async def get_transaction(self, signature, commitment="confirmed"):
            return None
        async def close(self):
            pass

    collector.rpc = NullRpc()

    async def no_sleep(_):
        return None
    monkeypatch.setattr("memequant.ingestion.live.asyncio.sleep", no_sleep)
    try:
        with pytest.raises(RpcError):
            await collector._fetch_transaction_with_retry("missing")
        assert e.state.counters()["transaction_fetch_nulls"] == 1
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_reconciliation_limit_is_terminal_not_a_reconnect_loop(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
        MEMEQUANT_RECONCILE_ON_START=False,
    )
    collector = LiveCollector(settings, e)
    calls = 0

    async def fail_once(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise ReconciliationLimitExceeded("gap is over limit")

    collector._one_connection = fail_once
    try:
        with pytest.raises(ReconciliationLimitExceeded):
            await collector.run()
        assert calls == 1
        assert e.state.counters()["reconciliation_halted"] == 1
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_transient_rpc_error_reconnects_normally(tmp_path: Path, monkeypatch):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
        MEMEQUANT_RECONCILE_ON_START=False,
        MEMEQUANT_SUBSCRIPTION_MODE="logs",
    )
    collector = LiveCollector(settings, e)
    calls = 0

    async def fail_then_cancel(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RpcError("logsSubscribe", {"message": "temporary"})
        raise asyncio.CancelledError

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("memequant.ingestion.live.asyncio.sleep", no_sleep)
    collector._one_connection = fail_then_cancel
    try:
        with pytest.raises(asyncio.CancelledError):
            await collector.run()
        assert calls == 2
        assert e.metrics.reconnects == 1
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_failed_log_notification_avoids_get_transaction(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)

    class NoFetchRpc:
        async def get_transaction(self, *_args, **_kwargs):
            raise AssertionError("failed notification must not be fetched")

        async def close(self):
            pass

    collector.rpc = NoFetchRpc()
    try:
        await collector._consume_logs(
            messages(log_notification("failed", err={"InstructionError": [0, "Custom"]})),
            {77: PUMP},
        )
        collector._flush_telemetry(force=True)
        assert e.state.counters()["failed_log_notifications_skipped"] == 1
        assert e.state.summary()["transactions"] == 0
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_live_and_reconciliation_overlap_is_deduplicated(tmp_path: Path):
    e = make_engine(tmp_path)
    e.state.set_checkpoint(PUMP, "old", 10, "t")
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)

    class OverlapRpc:
        tx_calls = 0

        async def get_signatures_for_address(self, *_args, **_kwargs):
            return [{"signature": "same", "slot": 11, "err": None}]

        async def get_transaction(self, signature, commitment="confirmed"):
            self.tx_calls += 1
            return {
                "slot": 11,
                "blockTime": 1_700_000_000,
                "meta": {"err": None, "logMessages": []},
                "transaction": {"signatures": [signature], "message": {}},
                "version": "legacy",
            }

        async def close(self):
            pass

    rpc = OverlapRpc()
    collector.rpc = rpc
    collector.reconciler.rpc = rpc
    try:
        await collector._reconcile_all()
        await collector._consume_logs(messages(log_notification("same")), {77: PUMP})
        assert rpc.tx_calls == 1
        assert e.state.summary()["transactions"] == 1
        assert e.state.get_checkpoint(PUMP)["last_signature"] == "same"
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_successful_log_avoids_get_transaction_and_advances_checkpoint(tmp_path: Path):
    e = make_engine(tmp_path)
    e.state.set_checkpoint(PUMP, "old", 10, "t")
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)

    class NoFetchRpc:
        async def get_transaction(self, *_args, **_kwargs):
            raise AssertionError("successful live logs must not fetch a transaction")

        async def close(self):
            pass

    collector.rpc = NoFetchRpc()
    try:
        await collector._consume_logs(messages(log_notification("direct")), {77: PUMP})
        assert e.state.get_checkpoint(PUMP)["last_signature"] == "direct"
        assert e.state.summary()["transactions"] == 1
        assert list((tmp_path / "raw").rglob("log_notifications.jsonl.gz"))
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_websocket_queue_is_bounded_and_fails_closed(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)
    queue = asyncio.Queue(maxsize=1)
    try:
        with pytest.raises(LiveQueueOverflow):
            await collector._read_messages(
                messages(log_notification("first"), log_notification("second")), queue
            )
        assert queue.qsize() == 1
        assert e.state.counters()["ws_queue_overflows"] == 1
        assert e.state.counters()["ws_messages_received"] == 2
        assert e.state.counters()["ws_messages_enqueued"] == 1
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_overflow_reader_error_surfaces_before_buffer_is_drained(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)
    queue = asyncio.Queue(maxsize=1)
    queue.put_nowait("already-buffered")

    async def overflow():
        raise LiveQueueOverflow("full")

    reader = asyncio.create_task(overflow())
    await asyncio.sleep(0)
    buffered = collector._queued_messages(queue, reader)
    try:
        with pytest.raises(LiveQueueOverflow):
            await anext(buffered)
        assert queue.qsize() == 1
    finally:
        await buffered.aclose()
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_overflow_during_reconciliation_is_not_hidden_by_cancellation(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)
    reconciliation_cancelled = False

    async def overflow():
        await asyncio.sleep(0)
        raise LiveQueueOverflow("full")

    async def reconcile_forever():
        nonlocal reconciliation_cancelled
        try:
            await asyncio.Event().wait()
        finally:
            reconciliation_cancelled = True

    collector._reconcile_all = reconcile_forever
    reader = asyncio.create_task(overflow())
    try:
        with pytest.raises(LiveQueueOverflow):
            await collector._reconcile_while_reading(reader)
        assert reconciliation_cancelled
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_failed_log_is_counted_but_does_not_consume_queue_capacity(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)
    queue = asyncio.Queue(maxsize=1)
    try:
        await collector._read_messages(
            messages(
                log_notification("ok"),
                log_notification("failed", err={"InstructionError": [0, "Custom"]}),
            ),
            queue,
        )
        collector._flush_telemetry(force=True)
        counters = e.state.counters()
        assert queue.qsize() == 1
        assert counters["ws_notifications_received"] == 2
        assert counters["ws_notifications_enqueued"] == 1
        assert counters["successful_log_notifications"] == 1
        assert counters["failed_log_notifications"] == 1
        assert counters["failed_log_notifications_skipped"] == 1
        assert "ws_queue_overflows" not in counters
    finally:
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_websocket_received_enqueued_processed_and_log_counters(tmp_path: Path):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, e)

    queue = asyncio.Queue(maxsize=10)
    wire = messages(
        log_notification("ok"),
        log_notification("failed", err={"InstructionError": [0, "Custom"]}),
        {"jsonrpc": "2.0", "id": 99, "result": 77},
    )
    reader = asyncio.create_task(collector._read_messages(wire, queue))
    await reader
    buffered = collector._queued_messages(queue, reader)
    try:
        with pytest.raises(ConnectionError, match="websocket closed"):
            await collector._consume_logs(buffered, {77: PUMP})
        collector._flush_telemetry(force=True)
        counters = e.state.counters()
        assert counters["ws_messages_received"] == 3
        assert counters["ws_messages_enqueued"] == 1
        assert counters["ws_non_notification_messages_received"] == 1
        assert counters["ws_notifications_received"] == 2
        assert counters["ws_notifications_enqueued"] == 1
        assert counters["ws_notifications_processed"] == 1
        assert counters["successful_log_notifications"] == 1
        assert counters["failed_log_notifications"] == 1
        assert counters["failed_log_notifications_skipped"] == 1
        assert counters["ws_queue_high_watermark"] == 1
        assert counters["ws_queue_current_size"] == 0
    finally:
        await buffered.aclose()
        await collector.close()
        e.close()
        e.state.close()


@pytest.mark.asyncio
async def test_websocket_reader_starts_before_reconciliation(tmp_path: Path, monkeypatch):
    e = make_engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
        MEMEQUANT_SUBSCRIPTION_MODE="logs",
    )
    collector = LiveCollector(settings, e)
    reader_started = asyncio.Event()
    keep_open = asyncio.Event()
    reconciled_while_reader_active = False

    class LifecycleWs(FakeWs):
        async def stream(self):
            reader_started.set()
            yield json.dumps(log_notification("buffered"))
            await keep_open.wait()

        def __aiter__(self):
            return self.stream()

    ws = LifecycleWs()

    class ConnectionContext:
        async def __aenter__(self):
            return ws

        async def __aexit__(self, *_args):
            return False

    async def reconcile():
        nonlocal reconciled_while_reader_active
        await asyncio.wait_for(reader_started.wait(), timeout=1)
        reconciled_while_reader_active = not keep_open.is_set()

    async def consume_one(buffered, _subscriptions):
        item = await anext(buffered)
        assert isinstance(item, BufferedWsMessage)
        assert json.loads(item.raw)["method"] == "logsNotification"

    monkeypatch.setattr(
        "memequant.ingestion.live.websockets.connect",
        lambda *_args, **_kwargs: ConnectionContext(),
    )
    collector._reconcile_all = reconcile
    collector._consume_logs = consume_one
    try:
        await collector._one_connection("logs", reconcile_after_subscribe=True)
        assert reconciled_while_reader_active
    finally:
        await collector.close()
        e.close()
        e.state.close()
