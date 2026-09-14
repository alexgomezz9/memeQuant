import json
from pathlib import Path

import pytest

from memequant.config import Settings
from memequant.ingestion.live import LiveCollector
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
