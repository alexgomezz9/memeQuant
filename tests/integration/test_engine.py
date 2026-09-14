import gzip
import json
from pathlib import Path

from memequant.engine import IngestionEngine
from memequant.protocols.decoder import default_decoders
from memequant.storage.normalized import NormalizedStore
from memequant.storage.raw import RawJsonlGzipStore
from memequant.storage.state import StateStore
from tests.helpers import encode_event, envelope, load_idl, log_event

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
AMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"


def engine(tmp_path: Path) -> IngestionEngine:
    return IngestionEngine(
        state=StateStore(tmp_path / "state.db"),
        raw_store=RawJsonlGzipStore(tmp_path / "raw", fsync_every=0),
        normalized_store=NormalizedStore(tmp_path / "normalized", batch_size=100, prefer_parquet=False),
        decoders=default_decoders(),
    )


def test_successful_create_persists_raw_and_normalized(tmp_path: Path):
    e = engine(tmp_path)
    raw = encode_event(load_idl("pump.snapshot.json"), "CreateEvent", {"name": "ABC", "symbol": "A"})
    env = envelope(PUMP, log_event(PUMP, raw))
    assert e.process(env)
    e.close()
    assert e.state.summary()["transactions"] == 1
    assert e.metrics.tokens_created == 1
    assert list((tmp_path / "raw").rglob("*.jsonl.gz"))
    token_files = list((tmp_path / "normalized/tokens").rglob("*.jsonl.gz"))
    assert token_files
    with gzip.open(token_files[0], "rt") as f:
        row = json.loads(f.readline())
    assert row["name"] == "ABC"
    e.state.close()


def test_failed_transaction_is_raw_but_not_economic_event(tmp_path: Path):
    e = engine(tmp_path)
    raw = encode_event(load_idl("pump.snapshot.json"), "TradeEvent", {"shareholders": []})
    env = envelope(PUMP, log_event(PUMP, raw), err={"InstructionError": [0, "Custom"]})
    assert e.process(env)
    e.close()
    assert e.metrics.failed_transactions == 1
    assert e.metrics.trades == 0
    assert list((tmp_path / "raw").rglob("*.jsonl.gz"))
    assert not list((tmp_path / "normalized/trades").rglob("*.jsonl.gz"))
    e.state.close()


def test_same_transaction_observed_via_both_programs_is_decoded_once(tmp_path: Path):
    e = engine(tmp_path)
    pump_raw = encode_event(load_idl("pump.snapshot.json"), "CompleteEvent")
    amm_raw = encode_event(load_idl("pump_amm.snapshot.json"), "CreatePoolEvent")
    logs = [
        f"Program {PUMP} invoke [1]",
        "Program data: " + __import__('base64').b64encode(pump_raw).decode(),
        f"Program {AMM} invoke [2]",
        "Program data: " + __import__('base64').b64encode(amm_raw).decode(),
        f"Program {AMM} success",
        f"Program {PUMP} success",
    ]
    first = envelope(PUMP, logs, signature="migration", slot=555)
    second = first.model_copy(update={"source_program": AMM})
    assert e.process(first)
    assert not e.process(second)
    e.close()
    summary = e.state.summary()
    assert summary["transactions"] == 1
    assert summary["observations"] == 2
    assert e.metrics.completions == 1
    assert e.metrics.pools_created == 1
    e.state.close()
