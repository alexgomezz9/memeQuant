from __future__ import annotations

import gzip
import os
import threading
from pathlib import Path
from typing import TextIO

import orjson

from memequant.models import RawTransactionEnvelope


class RawJsonlGzipStore:
    """Append-only authoritative transaction log, sharded hourly.

    Each append flushes the gzip stream before the ingestion engine advances its
    checkpoint. A process crash can therefore cause a duplicate raw record after
    reconciliation, but should not cause an already-checkpointed record to exist only
    in memory. `fsync_every` additionally bounds the OS/power-loss durability window;
    setting it to 1 is safest but significantly slower.

    Raw duplicates are intentionally allowed. Derived research datasets are rebuilt
    deterministically and deduplicated by transaction signature/event_id.
    """

    def __init__(self, root: Path, fsync_every: int = 100):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.fsync_every = fsync_every
        self._lock = threading.RLock()
        self._handle: TextIO | None = None
        self._key: tuple[str, str] | None = None
        self._written_since_fsync = 0

    def _target(self, env: RawTransactionEnvelope) -> tuple[tuple[str, str], Path]:
        dt = env.received_at
        date = dt.strftime("%Y-%m-%d")
        hour = dt.strftime("%H")
        path = self.root / f"date={date}" / f"hour={hour}" / "transactions.jsonl.gz"
        return (date, hour), path

    def append(self, env: RawTransactionEnvelope) -> None:
        key, path = self._target(env)
        payload = orjson.dumps(env.model_dump(mode="json")).decode("utf-8") + "\n"
        with self._lock:
            if self._key != key:
                self.close()
                path.parent.mkdir(parents=True, exist_ok=True)
                self._handle = gzip.open(path, "at", encoding="utf-8", compresslevel=5)
                self._key = key
            assert self._handle is not None
            self._handle.write(payload)
            # Required before StateStore can advance the checkpoint.
            self._handle.flush()
            self._written_since_fsync += 1
            if self.fsync_every and self._written_since_fsync >= self.fsync_every:
                fileobj = getattr(self._handle, "fileobj", None)
                if fileobj is not None:
                    os.fsync(fileobj.fileno())
                self._written_since_fsync = 0

    def close(self) -> None:
        with self._lock:
            if self._handle is not None:
                self._handle.flush()
                fileobj = getattr(self._handle, "fileobj", None)
                if fileobj is not None and self.fsync_every:
                    os.fsync(fileobj.fileno())
                self._handle.close()
            self._handle = None
            self._key = None
            self._written_since_fsync = 0
