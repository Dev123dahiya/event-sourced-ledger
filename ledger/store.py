from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

from .errors import ConcurrencyError

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,   -- global order
    stream     TEXT NOT NULL,
    version    INTEGER NOT NULL,                    -- order within the stream, from 1
    type       TEXT NOT NULL,
    data       TEXT NOT NULL,
    recorded_at REAL NOT NULL,
    UNIQUE (stream, version)
);
-- The log is append-only: the database itself refuses edits and deletes.
CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;
CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;
"""


@dataclass(frozen=True)
class Event:
    seq: int
    stream: str
    version: int
    type: str
    data: dict[str, Any]
    recorded_at: float


class EventStore:
    """Append-only event log on SQLite with optimistic concurrency per stream."""

    def __init__(self, path: str, clock: Callable[[], float] = time.time) -> None:
        self.path = path
        self._clock = clock
        self._local = threading.local()
        self.conn.executescript(SCHEMA)

    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """One write transaction. Events and projections updated inside commit together."""
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def append(self, conn: sqlite3.Connection, stream: str, type_: str, data: dict[str, Any],
               expected_version: int | None = None) -> Event:
        """Append one event. Must be called inside ``transaction()``.

        ``expected_version`` is the version the caller last saw (0 for a new stream).
        If the stream has moved on, raises ConcurrencyError and nothing is written.
        """
        current = conn.execute("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream = ?", (stream,)).fetchone()[0]
        if expected_version is not None and current != expected_version:
            raise ConcurrencyError(f"{stream}: expected version {expected_version}, found {current}")
        now = self._clock()
        cur = conn.execute(
            "INSERT INTO events (stream, version, type, data, recorded_at) VALUES (?, ?, ?, ?, ?)",
            (stream, current + 1, type_, json.dumps(data, sort_keys=True), now),
        )
        return Event(cur.lastrowid, stream, current + 1, type_, data, now)

    def read(self, stream: str | None = None, after_seq: int = 0, up_to_seq: int | None = None) -> Iterator[Event]:
        """Events in global order, optionally for one stream and/or a sequence range."""
        sql, args = "SELECT * FROM events WHERE seq > ?", [after_seq]
        if stream is not None:
            sql += " AND stream = ?"
            args.append(stream)
        if up_to_seq is not None:
            sql += " AND seq <= ?"
            args.append(up_to_seq)
        for row in self.conn.execute(sql + " ORDER BY seq", args):
            yield Event(row["seq"], row["stream"], row["version"], row["type"], json.loads(row["data"]), row["recorded_at"])

    def last_seq(self) -> int:
        return self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM events").fetchone()[0]
