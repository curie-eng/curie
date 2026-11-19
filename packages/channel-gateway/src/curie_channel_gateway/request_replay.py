"""Bounded, durable request replay refusal for authenticated gateway ports."""

import hashlib
import hmac
import sqlite3
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

REQUEST_ID_HEADER = "X-Curie-Gateway-Request-Id"
REQUEST_TIME_HEADER = "X-Curie-Gateway-Request-Time"
REQUEST_PROOF_HEADER = "X-Curie-Gateway-Request-Proof"
PROVIDER_ENDPOINT_HEADER = "X-Curie-Provider-Endpoint"
BEST_EFFORT_HEADER = "X-Curie-Best-Effort-Unreachable"
MAX_REQUEST_AGE_S = 300
MAX_REPLAY_ROWS = 100_000


def request_proof(
    token: str,
    method: str,
    path: str,
    body: bytes,
    request_id: str,
    timestamp: str,
    best_effort: str,
    provider_endpoint: str = "",
) -> str:
    payload = "\n".join(
        (
            method,
            path,
            hashlib.sha256(body).hexdigest(),
            request_id,
            timestamp,
            best_effort,
            provider_endpoint,
        )
    )
    return hmac.new(token.encode(), payload.encode(), hashlib.sha256).hexdigest()


def request_headers(
    token: str,
    method: str,
    url: str,
    body: bytes = b"",
    *,
    best_effort_unreachable: bool = False,
    provider_endpoint: str | None = None,
) -> dict[str, str]:
    parts = urlsplit(url)
    path = parts.path + ("?" + parts.query if parts.query else "")
    request_id = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    best_effort = "true" if best_effort_unreachable else "false"
    headers = {
        "X-Curie-Adapter-Secret": token,
        REQUEST_ID_HEADER: request_id,
        REQUEST_TIME_HEADER: timestamp,
        BEST_EFFORT_HEADER: best_effort,
        REQUEST_PROOF_HEADER: request_proof(
            token, method, path, body, request_id, timestamp, best_effort, provider_endpoint or ""
        ),
    }

    if provider_endpoint is not None:
        headers[PROVIDER_ENDPOINT_HEADER] = provider_endpoint
    return headers


class ReplayRefusedError(RuntimeError):
    pass


class ReplayCapacityError(RuntimeError):
    pass


class RequestReplayStore:
    """Consume each authenticated nonce once, including across process restarts."""

    def __init__(self, path: str | Path) -> None:
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("""CREATE TABLE IF NOT EXISTS requests (
            caller TEXT NOT NULL, nonce TEXT NOT NULL, expires_at INTEGER NOT NULL,
            PRIMARY KEY (caller, nonce))""")

    def close(self) -> None:
        self._db.close()

    def consume(
        self,
        caller: str,
        token: str,
        method: str,
        path: str,
        body: bytes,
        headers: Mapping[str, str],
    ) -> None:
        request_id = headers.get(REQUEST_ID_HEADER, "")
        timestamp = headers.get(REQUEST_TIME_HEADER, "")
        best_effort = headers.get(BEST_EFFORT_HEADER, "false")
        try:
            nonce = uuid.UUID(request_id)
            sent_at = int(timestamp)
        except ValueError:
            raise ReplayRefusedError("request proof refused") from None
        now = int(time.time())
        if (
            nonce.version != 4
            or str(nonce) != request_id
            or best_effort not in {"true", "false"}
            or abs(now - sent_at) > MAX_REQUEST_AGE_S
            or not hmac.compare_digest(
                headers.get(REQUEST_PROOF_HEADER, ""),
                request_proof(
                    token,
                    method,
                    path,
                    body,
                    request_id,
                    timestamp,
                    best_effort,
                    headers.get(PROVIDER_ENDPOINT_HEADER, ""),
                ),
            )
        ):
            raise ReplayRefusedError("request proof refused")
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._db.execute("DELETE FROM requests WHERE expires_at < ?", (now,))
            if self._db.execute("SELECT count(*) FROM requests").fetchone()[0] >= MAX_REPLAY_ROWS:
                raise ReplayCapacityError("request ledger full")
            self._db.execute(
                "INSERT INTO requests VALUES (?, ?, ?)",
                (caller, request_id, sent_at + MAX_REQUEST_AGE_S),
            )
            self._db.execute("COMMIT")
        except sqlite3.IntegrityError:
            self._db.execute("ROLLBACK")
            raise ReplayRefusedError("request proof refused") from None
        except BaseException:  # noqa: BLE001 - roll back before propagating cancellation or faults
            self._db.execute("ROLLBACK")
            raise
