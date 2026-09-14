import gzip
import json
from pathlib import Path

import orjson
import pyarrow.parquet as pq
import pytest

from memequant.config import Settings
from memequant.ingestion.live import LiveCollector
from memequant.rebuild import rebuild_derived
from tests.helpers import (
    encode_event,
    envelope,
    load_idl,
    log_event,
)
from tests.integration.test_engine import engine

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def test_rebuild_deduplicates_raw_and_does_not_reappend_source(tmp_path: Path):
    raw_root = tmp_path / "raw/date=2026-09-10/hour=16"
    raw_root.mkdir(parents=True)
    raw_event = encode_event(load_idl("pump.snapshot.json"), "CreateEvent", {"name": "REBUILD"})
    env = envelope(PUMP, log_event(PUMP, raw_event), signature="same")
    raw_file = raw_root / "transactions.jsonl.gz"
    with gzip.open(raw_file, "wb") as f:
        line = orjson.dumps(env.model_dump(mode="json")) + b"\n"
        f.write(line)
        f.write(line)  # legal RAW duplicate after crash/reconciliation

    out = tmp_path / "research"
    result = rebuild_derived(tmp_path / "raw", out, batch_size=1)
    assert result["raw_rows"] == 2
    assert result["unique_transactions"] == 1
    assert result["events"] == 1
    assert list((out / "tokens").rglob("*.*"))
    assert raw_file.exists()
    assert not (out / "raw").exists()


def test_rebuild_rejects_output_inside_raw(tmp_path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    with pytest.raises(ValueError, match="must not be inside"):
        rebuild_derived(raw_root, raw_root / "rebuilt")


@pytest.mark.asyncio
async def test_logs_notification_raw_decode_normalize_and_replay_is_deterministic(
    tmp_path: Path,
):
    idl = load_idl("pump.snapshot.json")
    encoded = encode_event(
        idl,
        "TradeEvent",
        {
            "token_amount": 123_456,
            "quote_amount": 789,
            "sol_amount": 789,
            "is_buy": True,
            "timestamp": 1_800_000_001,
            "shareholders": [],
        },
    )
    logs = log_event(PUMP, encoded)
    rpc_notification = {
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {
            "subscription": 77,
            "result": {
                "context": {"slot": 456},
                "value": {
                    "signature": "direct-log",
                    "err": None,
                    "logs": logs,
                },
            },
        },
    }

    async def stream():
        yield json.dumps(rpc_notification)

    live_engine = engine(tmp_path)
    settings = Settings(
        _env_file=None,
        SOLANA_HTTP_RPC_URL="https://example.invalid",
        SOLANA_WS_RPC_URL="wss://example.invalid",
    )
    collector = LiveCollector(settings, live_engine)
    await collector._consume_logs(stream(), {77: PUMP})
    await collector.close()
    live_engine.close()
    live_engine.state.close()

    raw_files = list((tmp_path / "raw").rglob("log_notifications.jsonl.gz"))
    assert len(raw_files) == 1
    with gzip.open(raw_files[0], "rb") as handle:
        raw_row = orjson.loads(handle.readline())
    assert raw_row["signature"] == "direct-log"
    assert raw_row["slot"] == 456
    assert raw_row["logs"] == logs
    assert "rpc_transaction" not in raw_row

    first = rebuild_derived(tmp_path / "raw", tmp_path / "replayed-1", batch_size=1)
    second = rebuild_derived(tmp_path / "raw", tmp_path / "replayed-2", batch_size=1)
    expected = {
        "raw_rows": 1,
        "raw_log_rows": 1,
        "raw_transaction_rows": 0,
        "unique_transactions": 1,
        "events": 1,
    }
    assert first == second == expected

    replay_rows = []
    for path in (tmp_path / "replayed-1" / "trades").rglob("*.parquet"):
        replay_rows.extend(pq.ParquetFile(path).read().to_pylist())
    assert len(replay_rows) == 1
    assert replay_rows[0]["signature"] == "direct-log"
    assert replay_rows[0]["base_amount_raw"] == 123_456
    assert replay_rows[0]["quote_amount_raw"] == 789
    assert replay_rows[0]["block_time"] is None
