import gzip
from pathlib import Path

import orjson

from memequant.rebuild import rebuild_derived
from tests.helpers import encode_event, envelope, load_idl, log_event

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
    import pytest

    from memequant.rebuild import rebuild_derived

    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    with pytest.raises(ValueError, match="must not be inside"):
        rebuild_derived(raw_root, raw_root / "rebuilt")
