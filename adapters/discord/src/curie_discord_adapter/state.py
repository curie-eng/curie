"""Small durable state owned by the Discord adapter."""

import sqlite3
from pathlib import Path

from .ingress import DiscordBinding


class DiscordState:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(path)
        path.chmod(0o600)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS continuations (
                conversation_id TEXT NOT NULL,
                reply_ref TEXT NOT NULL,
                position INTEGER NOT NULL,
                message_id TEXT NOT NULL,
                PRIMARY KEY (conversation_id, reply_ref, position)
            );
            CREATE TABLE IF NOT EXISTS completed_events (
                event_id TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS threads (
                thread_id TEXT PRIMARY KEY,
                parent_channel_id TEXT NOT NULL,
                address TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ingress_deliveries (
                delivery_id TEXT PRIMARY KEY
            );
            """
        )
        self._db.commit()

    def continuations(self, conversation_id: str, reply_ref: str) -> list[str]:
        rows = self._db.execute(
            "SELECT message_id FROM continuations "
            "WHERE conversation_id = ? AND reply_ref = ? ORDER BY position",
            (conversation_id, reply_ref),
        ).fetchall()
        return [str(row[0]) for row in rows]

    def replace_continuations(
        self, conversation_id: str, reply_ref: str, message_ids: list[str]
    ) -> None:
        with self._db:
            self._db.execute(
                "DELETE FROM continuations WHERE conversation_id = ? AND reply_ref = ?",
                (conversation_id, reply_ref),
            )
            self._db.executemany(
                "INSERT INTO continuations "
                "(conversation_id, reply_ref, position, message_id) VALUES (?, ?, ?, ?)",
                [
                    (conversation_id, reply_ref, position, message_id)
                    for position, message_id in enumerate(message_ids)
                ],
            )

    def mark_completed(self, event_id: str) -> bool:
        with self._db:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO completed_events (event_id) VALUES (?)",
                (event_id,),
            )
        return cursor.rowcount == 1

    def completed_count(self) -> int:
        row = self._db.execute("SELECT count(*) FROM completed_events").fetchone()
        return int(row[0]) if row is not None else 0

    def remember_thread(self, thread_id: str, binding: DiscordBinding) -> None:
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO threads "
                "(thread_id, parent_channel_id, address) VALUES (?, ?, ?)",
                (thread_id, binding.parent_channel_id, binding.address),
            )

    def thread_binding(self, thread_id: str) -> DiscordBinding | None:
        row = self._db.execute(
            "SELECT parent_channel_id, address FROM threads WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()
        if row is None:
            return None
        # Scoped tokens are reloaded from configuration and never persisted in
        # SQLite, whose WAL would otherwise become another credential store.
        return DiscordBinding(parent_channel_id=str(row[0]), address=str(row[1]), token="")

    def claim_delivery(self, delivery_id: str) -> bool:
        """Atomically claim a Gateway delivery before provider side effects."""

        with self._db:
            cursor = self._db.execute(
                "INSERT OR IGNORE INTO ingress_deliveries (delivery_id) VALUES (?)",
                (delivery_id,),
            )
        return cursor.rowcount == 1

    def release_delivery(self, delivery_id: str) -> None:
        """Release a failed intake so a later Gateway retry can try again."""

        with self._db:
            self._db.execute(
                "DELETE FROM ingress_deliveries WHERE delivery_id = ?", (delivery_id,)
            )

    def close(self) -> None:
        self._db.close()
