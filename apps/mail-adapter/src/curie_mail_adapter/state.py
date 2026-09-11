"""Crash-safe local state for the single-writer mail adapter.

The chart deliberately runs one adapter replica on one RWO volume.  This module
keeps that ownership boundary explicit: one SQLite connection, one re-entrant
lock, and short transactions around every state transition.  Provider and
platform network calls never run while the lock is held.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

SCHEMA_VERSION = 2
LEASE_SECONDS = 300.0
BODY_FAILURE_BACKOFF_SECONDS = 60.0
TERMINAL_RECEIPT_MAX = 4096
TERMINAL_COMPLETION_MAX = 4096
_TERMINAL_COMPLETION_SQL = "delivered=1 OR deleted=1"

_TERMINAL_STATES = ("accepted", "oversize", "primed", "rejected")
_COMPACTABLE_TERMINAL_STATES = ("oversize", "rejected")
_ADMISSION_BACKPRESSURE_CODES = frozenset(
    {
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
        sqlite3.SQLITE_READONLY,
        sqlite3.SQLITE_IOERR,
        sqlite3.SQLITE_FULL,
        sqlite3.SQLITE_CANTOPEN,
    }
)

DeliveryAdmission = Literal["admitted", "known", "full"]
EventClaim = Literal["claimed", "busy", "done", "deleted"]


class MailState:
    """The adapter's serialized SQLite owner."""

    def __init__(
        self,
        path: str,
        *,
        max_pending: int,
        max_bytes: int,
        terminal_completion_max: int | None = None,
        enforce_size: bool = True,
    ) -> None:
        self.path = Path(path)
        self.max_pending = max_pending
        self.max_bytes = max_bytes
        self.lock = threading.RLock()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if enforce_size and self.path.exists() and self.path.stat().st_size > max_bytes:
            raise RuntimeError(
                f"mail state {self.path} is larger than CURIE_MAIL_MAX_STATE_BYTES; "
                "recover an offline copy with python -m curie_mail_adapter recover "
                "--state <copy>"
            )
        self.connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        os.chmod(self.path, 0o600)
        with self.lock:
            self.connection.execute("PRAGMA busy_timeout=30000")
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute("PRAGMA foreign_keys=ON")
            for suffix in ("-wal", "-shm"):
                companion = Path(f"{self.path}{suffix}")
                if companion.exists():
                    os.chmod(companion, 0o600)
            version = int(self.connection.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                self.connection.close()
                raise RuntimeError(
                    f"mail state schema {version} is newer than supported {SCHEMA_VERSION}; "
                    "refusing to start"
                )
            self._migrate(version)
            page_size = int(self.connection.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(self.connection.execute("PRAGMA page_count").fetchone()[0])
            # A terminal receipt is deliberately tiny, but it still consumes a
            # b-tree slot. Bound those receipts by both a fixed ceiling and a
            # fraction of the configured page budget so rejected public-mailbox
            # traffic cannot claim the journal needed by live deliveries.
            self.terminal_receipt_max = max(
                8,
                min(TERMINAL_RECEIPT_MAX, max(1, max_bytes // page_size) // 4),
            )
            derived_completion_max = max(
                8,
                min(TERMINAL_COMPLETION_MAX, max(1, max_bytes // page_size) // 4),
            )
            self.terminal_completion_max = (
                derived_completion_max
                if terminal_completion_max is None
                else max(0, int(terminal_completion_max))
            )
            page_limit = max(page_count, max_bytes // page_size)
            self.connection.execute(f"PRAGMA max_page_count={page_limit}")
            # A replacement is the sole writer. A lease left behind by SIGKILL
            # cannot still have an owner, so reclaim it immediately on open.
            self.connection.execute(
                "UPDATE completion_events SET lease_owner=NULL, lease_until=NULL WHERE delivered=0"
            )
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                self._compact_terminal_completions(self.connection)
                self.connection.commit()
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.rollback()
                raise

    def _migrate(self, version: int) -> None:
        if version == 0:
            self.connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE deliveries (
                    message_id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    reply_ref TEXT NOT NULL,
                    summary_json TEXT NOT NULL,
                    turn_json TEXT,
                    state TEXT NOT NULL,
                    body_attempts INTEGER NOT NULL DEFAULT 0,
                    retry_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX deliveries_pending
                    ON deliveries(state, retry_at, created_at);
                CREATE TABLE reply_state (
                    conversation_id TEXT NOT NULL,
                    reply_ref TEXT NOT NULL,
                    text TEXT,
                    active INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (conversation_id, reply_ref)
                );
                CREATE INDEX reply_state_live
                    ON reply_state(conversation_id, active);
                CREATE TABLE completion_events (
                    event_id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    reply_ref TEXT,
                    delivered INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until REAL,
                    updated_at REAL NOT NULL
                );
                PRAGMA user_version=1;
                COMMIT;
                """
            )

        if version < 2:
            self.connection.executescript(
                """
                BEGIN IMMEDIATE;
                ALTER TABLE completion_events ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0;
                PRAGMA user_version=2;
                COMMIT;
                """
            )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Begin one serialized write and roll it back on every failure."""
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
                self.connection.commit()
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.rollback()
                raise

    def close(self) -> None:
        with self.lock:
            self.connection.close()

    def healthy(self) -> bool:
        with self.lock:
            try:
                return bool(self.connection.execute("SELECT 1").fetchone() == (1,))
            except sqlite3.Error:
                return False

    def is_primed(self) -> bool:
        with self.lock:
            row = self.connection.execute(
                "SELECT value FROM metadata WHERE key='initial_prime_complete'"
            ).fetchone()
            return bool(row == ("1",))

    def finish_prime(self) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO metadata(key, value) VALUES('initial_prime_complete', '1') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
            )

    def admit(self, message: dict[str, Any]) -> DeliveryAdmission:
        """Persist a listing claim, refusing before eviction when at capacity."""
        message_id = str(message["message_id"])
        conversation_id = str(message.get("thread_id") or message_id)
        now = time.time()
        try:
            with self.transaction() as connection:
                if connection.execute(
                    "SELECT 1 FROM deliveries WHERE message_id=?", (message_id,)
                ).fetchone():
                    return "known"
                self._compact_terminal_receipts(connection)
                self._compact_terminal_completions(connection)
                pending = int(
                    connection.execute(
                        "SELECT count(*) FROM deliveries "
                        "WHERE state IN ('body_pending', 'ingress_pending')"
                    ).fetchone()[0]
                )
                if pending >= self.max_pending or self._at_size_limit(connection):
                    return "full"
                connection.execute(
                    "INSERT INTO deliveries("
                    "message_id, conversation_id, reply_ref, summary_json, state, "
                    "created_at, updated_at"
                    ") VALUES(?, ?, ?, ?, 'body_pending', ?, ?)",
                    (
                        message_id,
                        conversation_id,
                        message_id,
                        json.dumps(message, separators=(",", ":"), sort_keys=True),
                        now,
                        now,
                    ),
                )
                return "admitted"
        except sqlite3.Error as error:
            if _is_admission_backpressure(error):
                return "full"
            raise

    def record_terminal(self, message_id: str, state: str) -> DeliveryAdmission:
        """Record an id-only terminal receipt without retaining listing PII."""
        if state not in _TERMINAL_STATES:
            raise ValueError(f"{state!r} is not a terminal delivery state")
        now = time.time()
        try:
            with self.transaction() as connection:
                if connection.execute(
                    "SELECT 1 FROM deliveries WHERE message_id=?", (message_id,)
                ).fetchone():
                    return "known"
                self._compact_terminal_receipts(
                    connection,
                    incoming=1 if state in _COMPACTABLE_TERMINAL_STATES else 0,
                )
                connection.execute(
                    "INSERT INTO deliveries("
                    "message_id, conversation_id, reply_ref, summary_json, state, "
                    "created_at, updated_at"
                    ") VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (
                        message_id,
                        message_id,
                        message_id,
                        _receipt_json(message_id),
                        state,
                        now,
                        now,
                    ),
                )
                return "admitted"
        except sqlite3.Error as error:
            if _is_admission_backpressure(error):
                return "full"
            raise

    def _compact_terminal_receipts(
        self, connection: sqlite3.Connection, *, incoming: int = 0
    ) -> None:
        count = int(
            connection.execute(
                "SELECT count(*) FROM deliveries WHERE state IN (?, ?)",
                _COMPACTABLE_TERMINAL_STATES,
            ).fetchone()[0]
        )
        excess = count + incoming - self.terminal_receipt_max
        if excess <= 0:
            return
        connection.execute(
            "DELETE FROM deliveries WHERE message_id IN ("
            "SELECT message_id FROM deliveries WHERE state IN (?, ?) "
            "ORDER BY updated_at, message_id LIMIT ?)",
            (*_COMPACTABLE_TERMINAL_STATES, excess),
        )

    def _compact_terminal_completions(
        self, connection: sqlite3.Connection, *, incoming: int = 0
    ) -> int:
        """Evict oldest delivered or deleted completion rows; never unresolved or leased."""
        count = int(
            connection.execute(
                f"SELECT count(*) FROM completion_events WHERE {_TERMINAL_COMPLETION_SQL}"
            ).fetchone()[0]
        )
        excess = count + incoming - self.terminal_completion_max
        if excess <= 0:
            return 0
        cursor = connection.execute(
            "DELETE FROM completion_events WHERE event_id IN ("
            f"SELECT event_id FROM completion_events WHERE {_TERMINAL_COMPLETION_SQL} "
            "ORDER BY updated_at, event_id LIMIT ?)",
            (excess,),
        )
        return max(0, int(cursor.rowcount))

    def _at_size_limit(self, connection: sqlite3.Connection) -> bool:
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        free_pages = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        return page_size * (page_count - free_pages) >= self.max_bytes

    def settle_without_turn(self, message_id: str, state: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE deliveries SET state=?, summary_json=?, turn_json=NULL, updated_at=? "
                "WHERE message_id=?",
                (state, _receipt_json(message_id), time.time(), message_id),
            )
            self._compact_terminal_receipts(connection)

    def body_failed(self, message_id: str, *, abandon_after: int) -> bool:
        """Spend one attempt, then park persistent failures without burning them."""
        with self.transaction() as connection:
            connection.execute(
                "UPDATE deliveries SET body_attempts=body_attempts+1, updated_at=? "
                "WHERE message_id=?",
                (time.time(), message_id),
            )
            attempts = int(
                connection.execute(
                    "SELECT body_attempts FROM deliveries WHERE message_id=?", (message_id,)
                ).fetchone()[0]
            )
            if attempts >= abandon_after:
                connection.execute(
                    "UPDATE deliveries SET retry_at=?, updated_at=? WHERE message_id=?",
                    (
                        time.time() + BODY_FAILURE_BACKOFF_SECONDS,
                        time.time(),
                        message_id,
                    ),
                )
                return True
            return False

    def store_turn(self, message_id: str, turn: dict[str, Any]) -> None:
        now = time.time()
        conversation_id = str(turn["conversation_id"])
        reply_ref = str(turn["reply_ref"])
        with self.transaction() as connection:
            connection.execute(
                "UPDATE deliveries SET turn_json=?, state='ingress_pending', updated_at=? "
                "WHERE message_id=?",
                (json.dumps(turn, separators=(",", ":"), sort_keys=True), now, message_id),
            )
            connection.execute(
                "INSERT INTO reply_state(conversation_id, reply_ref, text, active, updated_at) "
                "VALUES(?, ?, NULL, 1, ?) "
                "ON CONFLICT(conversation_id, reply_ref) DO UPDATE SET "
                "active=1, updated_at=excluded.updated_at",
                (conversation_id, reply_ref, now),
            )

    def pending(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.connection.execute(
                "SELECT message_id, summary_json, turn_json, state FROM deliveries "
                "WHERE state IN ('body_pending', 'ingress_pending') AND retry_at<=? "
                "ORDER BY created_at",
                (time.time(),),
            ).fetchall()
        return [
            {
                "message_id": row[0],
                "summary": json.loads(row[1]),
                "turn": json.loads(row[2]) if row[2] else None,
                "state": row[3],
            }
            for row in rows
        ]

    def defer_ingress(self, message_id: str, delay: float) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE deliveries SET retry_at=?, updated_at=? WHERE message_id=?",
                (time.time() + max(0.0, delay), time.time(), message_id),
            )

    def accept_ingress(self, message_id: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE deliveries SET state='accepted', retry_at=0, updated_at=? "
                "WHERE message_id=?",
                (time.time(), message_id),
            )
            self._compact_terminal_receipts(connection)

    def known_message_ids(self) -> list[str]:
        with self.lock:
            return [row[0] for row in self.connection.execute("SELECT message_id FROM deliveries")]

    def delivery(self, message_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT state, turn_json FROM deliveries WHERE message_id=?", (message_id,)
            ).fetchone()
        if row is None:
            return None
        return {"state": row[0], "turn": json.loads(row[1]) if row[1] else None}

    def record_text(
        self,
        conversation_id: str,
        reply_ref: str,
        text: str,
        *,
        append: bool,
        max_bytes: int,
    ) -> Literal["ok", "missing", "too_large"]:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT text FROM reply_state WHERE conversation_id=? AND reply_ref=? AND active=1",
                (conversation_id, reply_ref),
            ).fetchone()
            if row is None:
                return "missing"
            existing = row[0]
            combined = f"{existing}\n\n{text}" if append and existing else text
            if len(combined.encode("utf-8")) > max_bytes:
                return "too_large"
            connection.execute(
                "UPDATE reply_state SET text=?, updated_at=? "
                "WHERE conversation_id=? AND reply_ref=?",
                (combined, time.time(), conversation_id, reply_ref),
            )
            return "ok"

    def live_reply_refs(self, conversation_id: str) -> list[str]:
        with self.lock:
            return [
                row[0]
                for row in self.connection.execute(
                    "SELECT reply_ref FROM reply_state "
                    "WHERE conversation_id=? AND active=1 ORDER BY updated_at",
                    (conversation_id,),
                )
            ]

    def reply_text(self, conversation_id: str, reply_ref: str) -> tuple[bool, str | None]:
        with self.lock:
            row = self.connection.execute(
                "SELECT text FROM reply_state WHERE conversation_id=? AND reply_ref=?",
                (conversation_id, reply_ref),
            ).fetchone()
            return (False, None) if row is None else (True, row[0])

    def finish_reply(self, conversation_id: str, reply_ref: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE reply_state SET text=NULL, active=0, updated_at=? "
                "WHERE conversation_id=? AND reply_ref=?",
                (time.time(), conversation_id, reply_ref),
            )

    def claim_event(
        self, event_id: str, conversation_id: str, reply_ref: str | None, owner: str
    ) -> EventClaim:
        now = time.time()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT delivered, lease_owner, lease_until, deleted FROM completion_events "
                "WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if row and int(row[3]) == 1:
                return "deleted"
            if row and int(row[0]) == 1:
                return "done"
            if row and row[1] and float(row[2] or 0) > now:
                return "busy"
            connection.execute(
                "INSERT INTO completion_events("
                "event_id, conversation_id, reply_ref, delivered, lease_owner, "
                "lease_until, updated_at"
                ") VALUES(?, ?, ?, 0, ?, ?, ?) "
                "ON CONFLICT(event_id) DO UPDATE SET "
                "conversation_id=excluded.conversation_id, reply_ref=excluded.reply_ref, "
                "lease_owner=excluded.lease_owner, lease_until=excluded.lease_until, "
                "updated_at=excluded.updated_at",
                (event_id, conversation_id, reply_ref, owner, now + LEASE_SECONDS, now),
            )
            return "claimed"

    def release_event(self, event_id: str, owner: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE completion_events SET lease_owner=NULL, lease_until=NULL, updated_at=? "
                "WHERE event_id=? AND lease_owner=? AND delivered=0",
                (time.time(), event_id, owner),
            )

    def delete_event(self, event_id: str, conversation_id: str, reply_ref: str) -> None:
        """Keep a durable refusal without claiming the provider accepted a send."""
        with self.transaction() as connection:
            connection.execute(
                "UPDATE completion_events SET deleted=1, lease_owner=NULL, lease_until=NULL, "
                "updated_at=? WHERE event_id=?",
                (time.time(), event_id),
            )
            connection.execute(
                "UPDATE reply_state SET text=NULL, active=0, updated_at=? "
                "WHERE conversation_id=? AND reply_ref=?",
                (time.time(), conversation_id, reply_ref),
            )
            self._compact_terminal_completions(connection)

    def finish_event(self, event_id: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE completion_events SET delivered=1, lease_owner=NULL, lease_until=NULL, "
                "updated_at=? WHERE event_id=?",
                (time.time(), event_id),
            )
            self._compact_terminal_completions(connection)


def _receipt_json(message_id: str) -> str:
    return json.dumps({"message_id": message_id}, separators=(",", ":"), sort_keys=True)


def _is_admission_backpressure(error: sqlite3.Error) -> bool:
    code = getattr(error, "sqlite_errorcode", None)
    return code is not None and code & 0xFF in _ADMISSION_BACKPRESSURE_CODES
