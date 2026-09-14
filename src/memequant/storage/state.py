from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


class StateStore:
    """Small transactional index for idempotency and recovery metadata.

    This is deliberately not the research datastore. Raw/normalized market data live in
    immutable files; SQLite only stores dedupe keys and checkpoints.
    """

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                signature TEXT PRIMARY KEY,
                slot INTEGER NOT NULL,
                source_program TEXT NOT NULL,
                first_seen_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_transactions_slot ON transactions(slot);

            CREATE TABLE IF NOT EXISTS observations (
                signature TEXT NOT NULL,
                program_id TEXT NOT NULL,
                slot INTEGER NOT NULL,
                observed_at TEXT NOT NULL,
                PRIMARY KEY(signature, program_id)
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                signature TEXT NOT NULL,
                event_type TEXT NOT NULL,
                slot INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                program_id TEXT PRIMARY KEY,
                last_signature TEXT,
                last_slot INTEGER,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS counters (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self.conn.commit()

    def transaction_seen(self, signature: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM transactions WHERE signature=?", (signature,)
        ).fetchone()
        return row is not None

    def mark_transaction(
        self, signature: str, slot: int, source_program: str, first_seen_at: str
    ) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO transactions(signature,slot,source_program,first_seen_at) "
            "VALUES(?,?,?,?)",
            (signature, slot, source_program, first_seen_at),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def mark_observation(
        self, signature: str, program_id: str, slot: int, observed_at: str
    ) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO observations(signature,program_id,slot,observed_at) VALUES(?,?,?,?)",
            (signature, program_id, slot, observed_at),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def observation_seen(self, signature: str, program_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM observations WHERE signature=? AND program_id=?",
            (signature, program_id),
        ).fetchone()
        return row is not None

    def mark_event(self, event_id: str, signature: str, event_type: str, slot: int) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO events(event_id,signature,event_type,slot) VALUES(?,?,?,?)",
            (event_id, signature, event_type, slot),
        )
        self.conn.commit()
        return cur.rowcount == 1

    def set_checkpoint(
        self, program_id: str, signature: str, slot: int, updated_at: str
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO checkpoints(program_id,last_signature,last_slot,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(program_id) DO UPDATE SET
              last_signature=excluded.last_signature,
              last_slot=excluded.last_slot,
              updated_at=excluded.updated_at
            WHERE checkpoints.last_slot IS NULL OR excluded.last_slot >= checkpoints.last_slot
            """,
            (program_id, signature, slot, updated_at),
        )
        self.conn.commit()

    def get_checkpoint(self, program_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT last_signature,last_slot,updated_at FROM checkpoints WHERE program_id=?",
            (program_id,),
        ).fetchone()
        if not row:
            return None
        return {"last_signature": row[0], "last_slot": row[1], "updated_at": row[2]}

    def increment(self, key: str, amount: int = 1) -> None:
        self.increment_many({key: amount})

    def increment_many(self, values: dict[str, int]) -> None:
        self.update_counters(increments=values)

    def update_counters(
        self,
        *,
        increments: dict[str, int] | None = None,
        gauges: dict[str, int] | None = None,
        maxima: dict[str, int] | None = None,
    ) -> None:
        """Update cumulative counters and gauges in one SQLite transaction."""
        increments = increments or {}
        gauges = gauges or {}
        maxima = maxima or {}
        rows = [(key, int(amount)) for key, amount in increments.items() if amount]
        gauge_rows = [(key, int(value)) for key, value in gauges.items()]
        maximum_rows = [(key, int(value)) for key, value in maxima.items()]
        if not rows and not gauge_rows and not maximum_rows:
            return
        if rows:
            self.conn.executemany(
                """
                INSERT INTO counters(key,value) VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=value+excluded.value
                """,
                rows,
            )
        if gauge_rows:
            self.conn.executemany(
                """
                INSERT INTO counters(key,value) VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                gauge_rows,
            )
        if maximum_rows:
            self.conn.executemany(
                """
                INSERT INTO counters(key,value) VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value)
                """,
                maximum_rows,
            )
        self.conn.commit()

    def counters(self) -> dict[str, int]:
        return dict(self.conn.execute("SELECT key,value FROM counters").fetchall())

    def summary(self) -> dict[str, Any]:
        txs = self.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        observations = self.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        events = self.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        checkpoints = [
            {"program_id": row[0], "last_signature": row[1], "last_slot": row[2], "updated_at": row[3]}
            for row in self.conn.execute(
                "SELECT program_id,last_signature,last_slot,updated_at FROM checkpoints"
            )
        ]
        return {
            "transactions": txs,
            "observations": observations,
            "events": events,
            "counters": self.counters(),
            "checkpoints": checkpoints,
        }

    def close(self) -> None:
        self.conn.close()
