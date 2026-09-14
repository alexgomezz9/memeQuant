from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from memequant.models import RawTransactionEnvelope


def load_idl(name: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1] / "src" / "memequant" / "idl"
    return json.loads((root / name).read_text())


def event_spec(idl: dict[str, Any], event_name: str) -> tuple[bytes, list[dict[str, Any]], dict[str, dict]]:
    disc = bytes(next(e["discriminator"] for e in idl["events"] if e["name"] == event_name))
    types = {t["name"]: t for t in idl["types"]}
    fields = types[event_name]["type"]["fields"]
    return disc, fields, types


def _encode(type_spec: Any, value: Any, types: dict[str, dict]) -> bytes:
    if isinstance(type_spec, str):
        ints = {
            "u8": (1, False), "u16": (2, False), "u32": (4, False),
            "u64": (8, False), "u128": (16, False),
            "i8": (1, True), "i16": (2, True), "i32": (4, True),
            "i64": (8, True), "i128": (16, True),
        }
        if type_spec in ints:
            size, signed = ints[type_spec]
            return int(value).to_bytes(size, "little", signed=signed)
        if type_spec == "bool":
            return b"\x01" if value else b"\x00"
        if type_spec == "pubkey":
            raw = bytes(value)
            assert len(raw) == 32
            return raw
        if type_spec == "string":
            raw = str(value).encode()
            return len(raw).to_bytes(4, "little") + raw
        if type_spec == "bytes":
            raw = bytes(value)
            return len(raw).to_bytes(4, "little") + raw
        raise AssertionError(type_spec)
    if "vec" in type_spec:
        values = value or []
        return len(values).to_bytes(4, "little") + b"".join(
            _encode(type_spec["vec"], item, types) for item in values
        )
    if "option" in type_spec:
        return b"\x00" if value is None else b"\x01" + _encode(type_spec["option"], value, types)
    if "array" in type_spec:
        inner, length = type_spec["array"]
        assert len(value) == length
        return b"".join(_encode(inner, item, types) for item in value)
    if "defined" in type_spec:
        defined = type_spec["defined"]
        name = defined["name"] if isinstance(defined, dict) else defined
        fields = types[name]["type"]["fields"]
        return b"".join(_encode(f["type"], value[f["name"]], types) for f in fields)
    raise AssertionError(type_spec)


def default_value(type_spec: Any, seed: int = 7) -> Any:
    if isinstance(type_spec, str):
        if type_spec == "pubkey": return bytes([seed]) * 32
        if type_spec == "string": return "x"
        if type_spec == "bytes": return b"x"
        if type_spec == "bool": return False
        return 1
    if "vec" in type_spec: return []
    if "option" in type_spec: return None
    if "array" in type_spec: return [default_value(type_spec["array"][0], seed)] * int(type_spec["array"][1])
    if "defined" in type_spec: return None  # overridden when needed
    raise AssertionError(type_spec)


def encode_event(idl: dict[str, Any], event_name: str, overrides: dict[str, Any] | None = None) -> bytes:
    disc, fields, types = event_spec(idl, event_name)
    overrides = overrides or {}
    values = {}
    for i, field in enumerate(fields):
        if field["name"] in overrides:
            value = overrides[field["name"]]
        elif isinstance(field["type"], dict) and "defined" in field["type"]:
            name = field["type"]["defined"]
            if isinstance(name, dict): name = name["name"]
            nested = types[name]["type"]["fields"]
            value = {f["name"]: default_value(f["type"], 20+i) for f in nested}
        else:
            value = default_value(field["type"], 20+i)
        values[field["name"]] = value
    return disc + b"".join(_encode(f["type"], values[f["name"]], types) for f in fields)


def log_event(program: str, raw: bytes, depth: int = 1) -> list[str]:
    return [
        f"Program {program} invoke [{depth}]",
        "Program data: " + base64.b64encode(raw).decode(),
        f"Program {program} success",
    ]


def envelope(program: str, logs: list[str], *, signature: str = "sig1", err: Any = None, slot: int = 123) -> RawTransactionEnvelope:
    return RawTransactionEnvelope(
        source_provider="test",
        source_mode="block",
        source_program=program,
        slot=slot,
        block_time=1_700_000_000,
        received_at=datetime(2026, 9, 10, 16, 0, tzinfo=UTC),
        signature=signature,
        transaction_index=0,
        rpc_transaction={
            "slot": slot,
            "blockTime": 1_700_000_000,
            "meta": {"err": err, "logMessages": logs},
            "transaction": {"signatures": [signature], "message": {}},
            "version": "legacy",
        },
    )
