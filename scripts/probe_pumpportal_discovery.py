#!/usr/bin/env python3
"""Capture PumpPortal subscribeNewToken messages without normalizing them."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import orjson
import websockets

from memequant.security import redact_text

API_KEY_ENV = "MEMEQUANT_PUMPPORTAL_API_KEY"
PUMPPORTAL_WEBSOCKET_ENDPOINT = "wss://pumpportal.fun/api/data"
SUBSCRIBE_NEW_TOKEN_REQUEST = {"method": "subscribeNewToken"}
REQUESTED_FIELD_NAMES = ("mint", "signature", "creator", "user", "timestamp")


class InvalidJsonMessage(ValueError):
    """Raised when PumpPortal sends a frame that is not valid JSON."""


@dataclass
class _FieldObservation:
    key: str
    parent_path: str
    present: int = 0
    types: Counter[str] = field(default_factory=Counter)


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    raise TypeError(f"unsupported JSON value type: {type(value).__name__}")


def _escape_json_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def decode_message(raw_message: str | bytes) -> tuple[Any, bytes]:
    """Decode a JSON WebSocket frame and retain its exact wire representation."""
    if isinstance(raw_message, bytes):
        raw_bytes = raw_message
        try:
            text = raw_message.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidJsonMessage("received a non-UTF-8 binary frame") from exc
    else:
        text = raw_message
        raw_bytes = raw_message.encode("utf-8")

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise InvalidJsonMessage("received a WebSocket frame that is not valid JSON") from exc
    json_type(payload)
    return payload, raw_bytes


class SchemaObserver:
    """Infer structural JSON facts without assigning semantic meaning to fields."""

    def __init__(self) -> None:
        self.messages = 0
        self.root_types: Counter[str] = Counter()
        self.object_occurrences: Counter[str] = Counter()
        self.fields: dict[str, _FieldObservation] = {}
        self.paths_by_casefolded_key: defaultdict[str, set[str]] = defaultdict(set)
        self.top_level_shapes: Counter[tuple[str, ...]] = Counter()

    def observe(self, payload: Any) -> None:
        self.messages += 1
        self.root_types[json_type(payload)] += 1
        if isinstance(payload, dict):
            self.top_level_shapes[tuple(sorted(str(key) for key in payload))] += 1
        self._walk("$", payload)

    def _walk(self, path: str, value: Any) -> None:
        if isinstance(value, dict):
            self.object_occurrences[path] += 1
            for raw_key, child in value.items():
                key = str(raw_key)
                child_path = f"{path}/{_escape_json_pointer(key)}"
                observation = self.fields.setdefault(
                    child_path,
                    _FieldObservation(key=key, parent_path=path),
                )
                observation.present += 1
                observation.types[json_type(child)] += 1
                self.paths_by_casefolded_key[key.casefold()].add(child_path)
                self._walk(child_path, child)
        elif isinstance(value, list):
            for child in value:
                self._walk(f"{path}/*", child)

    def summary(self) -> dict[str, Any]:
        fields = []
        for path, observation in sorted(self.fields.items()):
            eligible = self.object_occurrences[observation.parent_path]
            missing = eligible - observation.present
            fields.append(
                {
                    "path": path,
                    "key": observation.key,
                    "present": observation.present,
                    "parent_object_occurrences": eligible,
                    "missing": missing,
                    "always_when_parent_is_object": missing == 0,
                    "optional_when_parent_is_object": missing > 0,
                    "types": dict(sorted(observation.types.items())),
                }
            )

        requested = {
            name: {
                "exists": bool(self.paths_by_casefolded_key.get(name)),
                "paths": sorted(self.paths_by_casefolded_key.get(name, set())),
            }
            for name in REQUESTED_FIELD_NAMES
        }
        return {
            "messages_observed": self.messages,
            "root_types": dict(sorted(self.root_types.items())),
            "top_level_shapes": [
                {"keys": list(keys), "messages": count}
                for keys, count in sorted(
                    self.top_level_shapes.items(), key=lambda item: (-item[1], item[0])
                )
            ],
            "fields": fields,
            "requested_field_names": requested,
        }


class CaptureWriter:
    """Append immutable JSONL records and observe their unmodified JSON values."""

    def __init__(self, output: Path) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        self.output = output
        self._stream = output.open("xb")
        self.observer = SchemaObserver()
        self.messages_received = 0
        self.bytes_received = 0

    def append(self, raw_message: str | bytes, *, received_at: datetime) -> None:
        payload, raw_bytes = decode_message(raw_message)
        received_at_json = orjson.dumps(format_timestamp(received_at))
        self._stream.write(
            b'{"received_at":'
            + received_at_json
            + b',"payload":'
            + raw_bytes
            + b"}\n"
        )
        self._stream.flush()
        self.messages_received += 1
        self.bytes_received += len(raw_bytes)
        self.observer.observe(payload)

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> CaptureWriter:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def summary_path_for(output: Path) -> Path:
    return output.with_name(f"{output.name}.summary.json")


def write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    with path.open("xb") as stream:
        stream.write(orjson.dumps(summary, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))


def build_websocket_url(api_key: str) -> str:
    query = urlencode({"api-key": api_key})
    return f"{PUMPPORTAL_WEBSOCKET_ENDPOINT}?{query}"


def redact_probe_error(value: object, api_key: str | None) -> str:
    text = str(value)
    if api_key:
        text = text.replace(api_key, "REDACTED")
    return redact_text(text)


async def capture_new_tokens(
    *,
    api_key: str,
    duration_seconds: float,
    output: Path,
) -> dict[str, Any]:
    """Run one bounded subscribeNewToken capture and return its schema summary."""
    summary_path = summary_path_for(output)
    if output.exists():
        raise FileExistsError(f"refusing to overwrite output: {output}")
    if summary_path.exists():
        raise FileExistsError(f"refusing to overwrite summary: {summary_path}")

    started_at = utc_now()
    status = "completed"
    error: str | None = None
    writer = CaptureWriter(output)
    try:
        url = build_websocket_url(api_key)
        async with websockets.connect(url, max_size=4 * 1024 * 1024) as websocket:
            await websocket.send(json.dumps(SUBSCRIBE_NEW_TOKEN_REQUEST, separators=(",", ":")))
            deadline = asyncio.get_running_loop().time() + duration_seconds
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
                except TimeoutError:
                    break
                writer.append(raw_message, received_at=utc_now())
            await websocket.close(code=1000, reason="probe duration complete")
    except asyncio.CancelledError:
        status = "interrupted"
        error = "capture cancelled"
        raise
    except Exception as exc:
        status = "failed"
        error = redact_probe_error(exc, api_key)
        raise
    finally:
        finished_at = utc_now()
        writer.close()
        summary: dict[str, Any] = {
            "probe": "pumpportal_subscribe_new_token",
            "status": status,
            "started_at": format_timestamp(started_at),
            "finished_at": format_timestamp(finished_at),
            "configured_duration_seconds": duration_seconds,
            "messages_received": writer.messages_received,
            "bytes_received": writer.bytes_received,
            "schema": writer.observer.summary(),
        }
        if error is not None:
            summary["error"] = error
        write_summary(summary_path, summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture PumpPortal subscribeNewToken payloads without normalization.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Capture duration in seconds (default: 30).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="New JSONL file to create; existing files are never overwritten.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.duration <= 0:
        parser.error("--duration must be greater than zero")

    api_key = os.environ.get(API_KEY_ENV)
    if not api_key:
        print(f"error: required environment variable {API_KEY_ENV} is not set", file=sys.stderr)
        return 2

    try:
        summary = asyncio.run(
            capture_new_tokens(
                api_key=api_key,
                duration_seconds=args.duration,
                output=args.output,
            )
        )
    except KeyboardInterrupt:
        print("probe interrupted; WebSocket shutdown requested", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"probe failed: {redact_probe_error(exc, api_key)}", file=sys.stderr)
        return 1

    printable = {
        "status": summary["status"],
        "messages_received": summary["messages_received"],
        "bytes_received": summary["bytes_received"],
        "output": redact_probe_error(args.output, api_key),
        "summary": redact_probe_error(summary_path_for(args.output), api_key),
    }
    print(orjson.dumps(printable).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
