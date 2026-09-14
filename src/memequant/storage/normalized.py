from __future__ import annotations

import gzip
import importlib.util
import os
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
from pydantic import BaseModel


class NormalizedStore:
    """Buffered, partitioned derived-data sink.

    Parquet is preferred when PyArrow is installed. Files are written to a temporary
    sibling and atomically renamed so readers never observe half-written parts. In a
    minimal environment a JSONL.GZ fallback preserves functionality. These files are a
    *derived cache*; authoritative data lives under data/raw and can be rebuilt.
    """

    def __init__(self, root: Path, batch_size: int = 500, prefer_parquet: bool = True):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.parquet_available = prefer_parquet and importlib.util.find_spec("pyarrow") is not None
        self.buffers: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    @property
    def format(self) -> str:
        return "parquet" if self.parquet_available else "jsonl.gz"

    @staticmethod
    def _stable_row(dataset: str, row: dict[str, Any]) -> dict[str, Any]:
        row = dict(row)
        # Decoded Anchor payloads differ by event type and evolve with protocol IDLs.
        # Store them as canonical JSON so the generic `events` dataset has a stable
        # scalar schema across Parquet parts. Typed datasets retain typed columns.
        if dataset == "events" and isinstance(row.get("payload"), dict):
            row["payload_json"] = orjson.dumps(
                row.pop("payload"), option=orjson.OPT_SORT_KEYS
            ).decode("utf-8")
        return row

    def append(self, dataset: str, record: BaseModel | dict[str, Any]) -> None:
        row = record.model_dump(mode="json") if isinstance(record, BaseModel) else record
        row = self._stable_row(dataset, row)
        block_time = row.get("block_time")
        if block_time is not None:
            date = datetime.fromtimestamp(int(block_time), tz=UTC).date().isoformat()
        else:
            received = row.get("received_at")
            if isinstance(received, str):
                date = received[:10]
            elif isinstance(received, datetime):
                date = received.astimezone(UTC).date().isoformat()
            else:
                date = datetime.now(UTC).date().isoformat()
        key = (dataset, date)
        self.buffers[key].append(row)
        if len(self.buffers[key]) >= self.batch_size:
            self._flush_key(key)

    def _flush_key(self, key: tuple[str, str]) -> None:
        rows = self.buffers.get(key)
        if not rows:
            return
        dataset, date = key
        folder = self.root / dataset / f"date={date}"
        folder.mkdir(parents=True, exist_ok=True)
        part = uuid.uuid4().hex

        if self.parquet_available:
            import pyarrow as pa
            import pyarrow.parquet as pq

            final = folder / f"part-{part}.parquet"
            tmp = folder / f".{final.name}.tmp"
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, tmp, compression="zstd")
            os.replace(tmp, final)
        else:
            final = folder / f"part-{part}.jsonl.gz"
            tmp = folder / f".{final.name}.tmp"
            with gzip.open(tmp, "wb", compresslevel=5) as handle:
                for row in rows:
                    handle.write(orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE))
            os.replace(tmp, final)
        rows.clear()

    def flush(self) -> None:
        for key in list(self.buffers):
            self._flush_key(key)

    def close(self) -> None:
        self.flush()
