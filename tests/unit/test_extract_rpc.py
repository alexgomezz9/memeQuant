import json
from datetime import UTC, datetime

import httpx
import pytest

from memequant.ingestion.extract import envelopes_from_block_notification
from memequant.rpc import SolanaHttpRpc

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def test_block_notification_trusts_server_side_program_filter():
    tx_good = {"transaction": {"signatures": ["yes"]}, "meta": {"logMessages": [f"Program {PUMP} invoke [1]"]}, "version": "legacy"}
    tx_bad = {"transaction": {"signatures": ["no"]}, "meta": {"logMessages": ["Program Other111111111111111111111111111111 invoke [1]"]}, "version": "legacy"}
    msg = {"params": {"result": {"context": {"slot": 7}, "value": {"slot": 7, "block": {"blockTime": 11, "transactions": [tx_bad, tx_good]}}}}}
    got = list(envelopes_from_block_notification(msg, program_id=PUMP, provider="x", received_at=datetime.now(UTC)))
    assert [e.signature for e in got] == ["no", "yes"]
    assert got[1].transaction_index == 1
    assert got[0].slot == 7


@pytest.mark.asyncio
async def test_get_transaction_sends_configured_max_version():
    seen = {}
    async def handler(request: httpx.Request):
        body = json.loads(request.content)
        seen.update(body)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": {"slot": 1}})
    rpc = SolanaHttpRpc("https://example.invalid", max_tx_version=1)
    await rpc.client.aclose()
    rpc.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await rpc.get_transaction("sig")
    finally:
        await rpc.close()
    assert seen["method"] == "getTransaction"
    assert seen["params"][1]["maxSupportedTransactionVersion"] == 1
