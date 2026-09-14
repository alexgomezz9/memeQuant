import hashlib
import json
from datetime import UTC, datetime
from typing import Any


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58encode(raw: bytes) -> str:
    if not raw:
        return ""
    zeros = 0
    for byte in raw:
        if byte == 0:
            zeros += 1
        else:
            break
    value = int.from_bytes(raw, "big")
    out = []
    while value:
        value, rem = divmod(value, 58)
        out.append(_B58_ALPHABET[rem])
    return "1" * zeros + "".join(reversed(out))


def utc_now() -> datetime:
    return datetime.now(UTC)


def stable_event_id(
    signature: str, program_id: str, log_index: int, event_type: str
) -> str:
    value = f"{signature}|{program_id}|{log_index}|{event_type}".encode()
    return hashlib.sha256(value).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
