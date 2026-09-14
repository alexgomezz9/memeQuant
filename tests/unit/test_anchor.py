import base64
from pathlib import Path

import pytest

from memequant.protocols.anchor import (
    BorshDecodeError,
    BorshReader,
    IdlEventDecoder,
    iter_program_data,
)
from memequant.utils import b58encode
from tests.helpers import encode_event, load_idl

PUMP = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
AMM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"


def test_b58_leading_zeroes():
    assert b58encode(b"\x00\x00") == "11"
    assert b58encode(b"\x00\x01") == "12"


def test_borsh_rejects_invalid_bool():
    with pytest.raises(BorshDecodeError):
        BorshReader(b"\x02", {}).read("bool")


def test_program_data_attributed_to_nested_active_program():
    a = base64.b64encode(b"a").decode()
    b = base64.b64encode(b"b").decode()
    logs = [
        f"Program {PUMP} invoke [1]",
        f"Program data: {a}",
        f"Program {AMM} invoke [2]",
        f"Program data: {b}",
        f"Program {AMM} success",
        f"Program data: {a}",
        f"Program {PUMP} success",
    ]
    got = list(iter_program_data(logs))
    assert [(program, raw) for _, program, raw in got] == [(PUMP, b"a"), (AMM, b"b"), (PUMP, b"a")]


def test_current_create_event_idl_roundtrip():
    idl = load_idl("pump.snapshot.json")
    raw = encode_event(idl, "CreateEvent", {"name": "MEME", "symbol": "MM"})
    path = Path(__file__).resolve().parents[2] / "src/memequant/idl/pump.snapshot.json"
    decoded = IdlEventDecoder(path).decode(raw)
    assert decoded is not None
    assert decoded.event_type == "CreateEvent"
    assert decoded.payload["name"] == "MEME"
    assert decoded.payload["symbol"] == "MM"
    assert decoded.trailing_bytes == 0
