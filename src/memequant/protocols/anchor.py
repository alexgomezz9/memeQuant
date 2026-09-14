from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from memequant.utils import b58encode


class BorshDecodeError(ValueError):
    pass


class BorshReader:
    def __init__(self, data: bytes, types: dict[str, dict[str, Any]]):
        self.data = memoryview(data)
        self.offset = 0
        self.types = types

    @property
    def remaining(self) -> int:
        return len(self.data) - self.offset

    def take(self, n: int) -> bytes:
        if n < 0 or self.offset + n > len(self.data):
            raise BorshDecodeError(
                f"buffer underrun: need {n} bytes at offset {self.offset}, "
                f"only {self.remaining} remain"
            )
        out = bytes(self.data[self.offset : self.offset + n])
        self.offset += n
        return out

    def integer(self, size: int, signed: bool = False) -> int:
        return int.from_bytes(self.take(size), "little", signed=signed)

    def read(self, type_spec: Any) -> Any:
        if isinstance(type_spec, str):
            primitives: dict[str, tuple[int, bool]] = {
                "u8": (1, False), "u16": (2, False), "u32": (4, False),
                "u64": (8, False), "u128": (16, False),
                "i8": (1, True), "i16": (2, True), "i32": (4, True),
                "i64": (8, True), "i128": (16, True),
            }
            if type_spec in primitives:
                size, signed = primitives[type_spec]
                return self.integer(size, signed)
            if type_spec == "bool":
                value = self.integer(1)
                if value not in (0, 1):
                    raise BorshDecodeError(f"invalid bool byte {value}")
                return bool(value)
            if type_spec == "pubkey":
                return b58encode(self.take(32))
            if type_spec == "string":
                length = self.integer(4)
                if length > self.remaining:
                    raise BorshDecodeError(f"string length {length} exceeds remaining buffer")
                return self.take(length).decode("utf-8")
            if type_spec == "bytes":
                length = self.integer(4)
                return self.take(length).hex()
            raise BorshDecodeError(f"unsupported primitive type {type_spec!r}")

        if not isinstance(type_spec, dict):
            raise BorshDecodeError(f"invalid type spec {type_spec!r}")

        if "option" in type_spec:
            tag = self.integer(1)
            if tag == 0:
                return None
            if tag != 1:
                raise BorshDecodeError(f"invalid Option tag {tag}")
            return self.read(type_spec["option"])

        if "vec" in type_spec:
            length = self.integer(4)
            if length > 1_000_000:
                raise BorshDecodeError(f"refusing implausible vector length {length}")
            return [self.read(type_spec["vec"]) for _ in range(length)]

        if "array" in type_spec:
            inner, length = type_spec["array"]
            return [self.read(inner) for _ in range(int(length))]

        if "defined" in type_spec:
            defined = type_spec["defined"]
            name = defined["name"] if isinstance(defined, dict) else defined
            return self.read_defined(name)

        if "tuple" in type_spec:
            return [self.read(t) for t in type_spec["tuple"]]

        raise BorshDecodeError(f"unsupported compound type {type_spec!r}")

    def read_defined(self, name: str) -> Any:
        spec = self.types.get(name)
        if spec is None:
            raise BorshDecodeError(f"IDL type {name!r} not found")
        definition = spec["type"]
        kind = definition["kind"]
        if kind == "struct":
            fields = definition.get("fields", [])
            if fields and isinstance(fields[0], str):
                return [self.read(t) for t in fields]
            return {field["name"]: self.read(field["type"]) for field in fields}
        if kind == "enum":
            idx = self.integer(1)
            variants = definition["variants"]
            if idx >= len(variants):
                raise BorshDecodeError(f"enum {name}: invalid variant {idx}")
            variant = variants[idx]
            result: dict[str, Any] = {"variant": variant["name"]}
            fields = variant.get("fields")
            if fields:
                if isinstance(fields[0], dict) and "name" in fields[0]:
                    result["fields"] = {f["name"]: self.read(f["type"]) for f in fields}
                else:
                    result["fields"] = [self.read(f) for f in fields]
            return result
        raise BorshDecodeError(f"unsupported IDL kind {kind!r}")


@dataclass(frozen=True)
class DecodedAnchorPayload:
    event_type: str
    payload: dict[str, Any]
    trailing_bytes: int


class IdlEventDecoder:
    def __init__(self, idl_path: Path):
        self.idl_path = idl_path
        self.idl = json.loads(idl_path.read_text(encoding="utf-8"))
        self.program_id: str = self.idl["address"]
        self.types = {entry["name"]: entry for entry in self.idl.get("types", [])}
        self.events_by_disc: dict[bytes, str] = {
            bytes(event["discriminator"]): event["name"] for event in self.idl.get("events", [])
        }

    def decode(self, raw: bytes) -> DecodedAnchorPayload | None:
        if len(raw) < 8:
            return None
        event_type = self.events_by_disc.get(raw[:8])
        if not event_type:
            return None
        reader = BorshReader(raw[8:], self.types)
        payload = reader.read_defined(event_type)
        if not isinstance(payload, dict):
            raise BorshDecodeError(f"event {event_type} did not decode to a struct")
        return DecodedAnchorPayload(event_type, payload, reader.remaining)


_INVOKE_RE = re.compile(r"^Program ([1-9A-HJ-NP-Za-km-z]+) invoke \[\d+\]$")
_EXIT_RE = re.compile(r"^Program ([1-9A-HJ-NP-Za-km-z]+) (?:success|failed: .+)$")
_PROGRAM_DATA_PREFIX = "Program data: "


def iter_program_data(log_messages: list[str] | None):
    """Yield (log_index, active_program_id, decoded_program_data).

    Solana transaction logs are nested. Anchor `emit!` data belongs to the currently
    executing program, so attribution must follow the invoke/success stack instead of
    decoding every `Program data:` line in a transaction indiscriminately.
    """
    stack: list[str] = []
    for index, line in enumerate(log_messages or []):
        match = _INVOKE_RE.match(line)
        if match:
            stack.append(match.group(1))
            continue

        match = _EXIT_RE.match(line)
        if match:
            program = match.group(1)
            if stack and stack[-1] == program:
                stack.pop()
            elif program in stack:
                # Defensive recovery for malformed/truncated logs.
                while stack and stack[-1] != program:
                    stack.pop()
                if stack:
                    stack.pop()
            continue

        if line.startswith(_PROGRAM_DATA_PREFIX) and stack:
            encoded = line[len(_PROGRAM_DATA_PREFIX) :]
            try:
                decoded = base64.b64decode(encoded, validate=True)
            except Exception:
                continue
            yield index, stack[-1], decoded
