from pathlib import Path

import pytest

from memequant.ingestion.reconcile import Reconciler, ReconciliationLimitExceeded
from memequant.storage.state import StateStore

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


class QueueRpc:
    def __init__(self, pages, txs=None):
        self.pages = list(pages)
        self.txs = txs or {}
        self.sig_calls = []

    async def get_signatures_for_address(self, address, **kwargs):
        self.sig_calls.append((address, kwargs))
        return self.pages.pop(0) if self.pages else []

    async def get_transaction(self, signature, commitment="confirmed"):
        return self.txs.get(signature)


@pytest.mark.asyncio
async def test_bootstrap_sets_tip_only_once(tmp_path: Path):
    state = StateStore(tmp_path / "s.db")
    rpc = QueueRpc([[{"signature": "tip", "slot": 99}]])
    r = Reconciler(rpc, state, "test", "confirmed")
    assert await r.bootstrap_if_needed(PUMP)
    assert state.get_checkpoint(PUMP)["last_signature"] == "tip"
    assert not await r.bootstrap_if_needed(PUMP)
    assert len(rpc.sig_calls) == 1
    state.close()


@pytest.mark.asyncio
async def test_missing_signatures_replayed_oldest_first(tmp_path: Path):
    state = StateStore(tmp_path / "s.db")
    state.set_checkpoint(PUMP, "checkpoint", 10, "t")
    rpc = QueueRpc([[{"signature": "s3", "slot": 13}, {"signature": "s2", "slot": 12}, {"signature": "s1", "slot": 11}]])
    r = Reconciler(rpc, state, "test", "confirmed", max_signatures=10)
    got = await r.missing_signatures(PUMP)
    assert [x["signature"] for x in got] == ["s1", "s2", "s3"]
    state.close()


@pytest.mark.asyncio
async def test_reconciliation_limit_fails_closed(tmp_path: Path):
    state = StateStore(tmp_path / "s.db")
    state.set_checkpoint(PUMP, "checkpoint", 1, "t")
    rpc = QueueRpc([
        [{"signature": "s3", "slot": 4}, {"signature": "s2", "slot": 3}],
        [{"signature": "still-more", "slot": 2}],
    ])
    r = Reconciler(rpc, state, "test", "confirmed", max_signatures=2)
    with pytest.raises(ReconciliationLimitExceeded):
        await r.missing_signatures(PUMP)
    assert state.get_checkpoint(PUMP)["last_signature"] == "checkpoint"
    state.close()


@pytest.mark.asyncio
async def test_reconcile_null_transaction_fails_closed(tmp_path: Path, monkeypatch):
    from memequant.ingestion.reconcile import ReconciliationTransactionUnavailable

    state = StateStore(tmp_path / "s.db")
    state.set_checkpoint(PUMP, "checkpoint", 10, "t")
    rpc = QueueRpc(
        [[{"signature": "missing", "slot": 11}]],
        txs={"missing": None},
    )
    r = Reconciler(rpc, state, "test", "confirmed", max_signatures=10)

    async def no_sleep(_):
        return None

    monkeypatch.setattr("memequant.ingestion.reconcile.asyncio.sleep", no_sleep)
    try:
        with pytest.raises(ReconciliationTransactionUnavailable):
            await r.reconcile(PUMP, lambda _: True)
        assert state.get_checkpoint(PUMP)["last_signature"] == "checkpoint"
    finally:
        state.close()
