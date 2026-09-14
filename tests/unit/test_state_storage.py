import gzip
import json
from pathlib import Path

from memequant.storage.normalized import NormalizedStore
from memequant.storage.raw import RawJsonlGzipStore
from memequant.storage.state import StateStore
from tests.helpers import envelope

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"


def test_checkpoint_never_regresses_to_lower_slot(tmp_path: Path):
    state = StateStore(tmp_path / "state.db")
    state.set_checkpoint(PUMP, "new", 20, "t2")
    state.set_checkpoint(PUMP, "old", 19, "t1")
    assert state.get_checkpoint(PUMP)["last_signature"] == "new"
    state.close()


def test_observations_are_per_program(tmp_path: Path):
    state = StateStore(tmp_path / "state.db")
    assert state.mark_observation("sig", "p1", 1, "t")
    assert not state.mark_observation("sig", "p1", 1, "t")
    assert state.mark_observation("sig", "p2", 1, "t")
    state.close()


def test_counter_updates_support_increments_gauges_and_maxima(tmp_path: Path):
    state = StateStore(tmp_path / "state.db")
    state.update_counters(
        increments={"received": 2},
        gauges={"queue_current": 5},
        maxima={"queue_high": 5},
    )
    state.update_counters(
        increments={"received": 3},
        gauges={"queue_current": 1},
        maxima={"queue_high": 3},
    )
    assert state.counters() == {
        "received": 5,
        "queue_current": 1,
        "queue_high": 5,
    }
    state.close()


def test_raw_roundtrip(tmp_path: Path):
    raw = RawJsonlGzipStore(tmp_path / "raw", fsync_every=1)
    env = envelope(PUMP, [])
    raw.append(env)
    raw.close()
    files = list((tmp_path / "raw").glob("date=*/hour=*/transactions.jsonl.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt") as f:
        row = json.loads(f.readline())
    assert row["signature"] == "sig1"
    assert row["slot"] == 123


def test_normalized_json_fallback_is_atomic_and_stable(tmp_path: Path):
    store = NormalizedStore(tmp_path / "normalized", batch_size=1, prefer_parquet=False)
    store.append("events", {"received_at": "2026-09-10T16:00:00Z", "payload": {"b": 2, "a": 1}})
    store.close()
    files = list((tmp_path / "normalized/events").glob("date=*/*.jsonl.gz"))
    assert len(files) == 1
    assert not list((tmp_path / "normalized/events").rglob("*.tmp"))
    with gzip.open(files[0], "rt") as f:
        row = json.loads(f.readline())
    assert "payload" not in row
    assert json.loads(row["payload_json"]) == {"a": 1, "b": 2}
