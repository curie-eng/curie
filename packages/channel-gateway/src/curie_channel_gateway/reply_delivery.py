"""Persistent replay and delivery claims for the gateway HTTP boundary.

The caller owns this SQLite file and preserves it across gateway restarts.
Delivery keys identify exact events at one configured route. Claims without a
confirmed outcome remain indeterminate; they are never silently re-executed.
"""

import hashlib
import hmac
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from .reply_errors import ConflictingReplyRequestError, ReplayedReplyRequestError
from .reply_transport import ADAPTER_SECRET_HEADER

TIMESTAMP_HEADER = "X-Curie-Reply-Timestamp"
NONCE_HEADER = "X-Curie-Reply-Nonce"
REQUEST_KEY_HEADER = "X-Curie-Reply-Request-Key"
SIGNATURE_HEADER = "X-Curie-Reply-Signature"
BEST_EFFORT_HEADER = "X-Curie-Reply-Best-Effort-Unreachable"
UNREACHABLE_ACK_HEADER = "X-Curie-Reply-Unreachable"
FRESHNESS_WINDOW_S = 120
_HEX_32 = re.compile(r"[0-9a-f]{32}\Z")
_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")


def reply_request_key(body: bytes, best_effort_unreachable: bool) -> str:
    """Stable across retries, including events on the unkeyed 1.0 wire.

    Re-emitting identical bytes to the same destination means the same delivery.
    Distinct posts with identical content must carry distinct delivery_id values.
    """
    return hashlib.sha256(bytes([best_effort_unreachable]) + body).hexdigest()


def request_signature(
    secret: str,
    *,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
    request_key: str,
    best_effort: str,
) -> str:
    message = "\n".join(
        (
            "POST",
            path,
            timestamp,
            nonce,
            request_key,
            best_effort,
            hashlib.sha256(body).hexdigest(),
        )
    )
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def signed_reply_headers(
    secret: str,
    *,
    path: str,
    body: bytes,
    best_effort_unreachable: bool,
    timestamp: int | None = None,
    nonce: str | None = None,
    request_key: str | None = None,
) -> dict[str, str]:
    stamp = str(int(time.time()) if timestamp is None else timestamp)
    unique = uuid.uuid4().hex if nonce is None else nonce
    key = reply_request_key(body, best_effort_unreachable) if request_key is None else request_key
    best = "1" if best_effort_unreachable else "0"
    return {
        "Content-Type": "application/json",
        ADAPTER_SECRET_HEADER: secret,
        TIMESTAMP_HEADER: stamp,
        NONCE_HEADER: unique,
        REQUEST_KEY_HEADER: key,
        BEST_EFFORT_HEADER: best,
        SIGNATURE_HEADER: request_signature(
            secret,
            path=path,
            body=body,
            timestamp=stamp,
            nonce=unique,
            request_key=key,
            best_effort=best,
        ),
    }


def valid_reply_signature(
    secret: str,
    *,
    path: str,
    body: bytes,
    timestamp: str,
    nonce: str,
    request_key: str,
    best_effort: str,
    signature: str,
) -> bool:
    if (
        not timestamp.isascii()
        or not timestamp.isdigit()
        or len(timestamp) > 12
        or abs(int(time.time()) - int(timestamp)) > FRESHNESS_WINDOW_S
        or _HEX_32.fullmatch(nonce) is None
        or _HEX_64.fullmatch(request_key) is None
        or _HEX_64.fullmatch(signature) is None
        or best_effort not in {"0", "1"}
    ):
        return False
    expected = request_signature(
        secret,
        path=path,
        body=body,
        timestamp=timestamp,
        nonce=nonce,
        request_key=request_key,
        best_effort=best_effort,
    )
    return hmac.compare_digest(signature, expected)


@dataclass(frozen=True)
class DeliveryResult:
    status: int
    body: bytes
    unreachable: bool = False


class ReplyDeliveryStore:
    """Atomic claims, durable results and bounded-lifetime authentication nonces."""

    def __init__(self, path: Path) -> None:
        if str(path) == ":memory:":
            raise ValueError("reply delivery state requires a persistent file")
        self._db = sqlite3.connect(path, isolation_level=None)
        try:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS gateway_reply_nonces (
                    nonce TEXT PRIMARY KEY, expires_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS gateway_reply_deliveries (
                    request_key TEXT PRIMARY KEY, content_hash TEXT NOT NULL,
                    status INTEGER, body BLOB, unreachable INTEGER NOT NULL DEFAULT 0
                );
            """)
        except BaseException:
            self._db.close()
            raise

    def consume_nonce(self, scope: str, timestamp: int) -> None:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.execute(
                "DELETE FROM gateway_reply_nonces WHERE expires_at < ?", (int(time.time()),)
            )
            try:
                self._db.execute(
                    "INSERT INTO gateway_reply_nonces VALUES (?, ?)",
                    (scope, timestamp + FRESHNESS_WINDOW_S),
                )
            except sqlite3.IntegrityError:
                raise ReplayedReplyRequestError("reply_request_replayed") from None
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def claim(self, key: str, content_hash: str) -> tuple[bool, DeliveryResult | None]:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            row = self._db.execute(
                "SELECT content_hash, status, body, unreachable "
                "FROM gateway_reply_deliveries WHERE request_key = ?",
                (key,),
            ).fetchone()
            result: tuple[bool, DeliveryResult | None]
            if row is None:
                self._db.execute(
                    "INSERT INTO gateway_reply_deliveries (request_key, content_hash) "
                    "VALUES (?, ?)",
                    (key, content_hash),
                )
                result = (True, None)
            elif row[0] != content_hash:
                raise ConflictingReplyRequestError("reply_request_conflict")
            elif row[1] is None:
                result = (False, None)
            else:
                result = (False, DeliveryResult(int(row[1]), bytes(row[2]), bool(row[3])))
            self._db.execute("COMMIT")
            return result
        except BaseException:
            self._db.execute("ROLLBACK")
            raise

    def finish(self, key: str, result: DeliveryResult) -> None:
        self._db.execute(
            "UPDATE gateway_reply_deliveries SET status = ?, body = ?, unreachable = ? "
            "WHERE request_key = ? AND status IS NULL",
            (result.status, result.body, result.unreachable, key),
        )

    def close(self) -> None:
        self._db.close()
