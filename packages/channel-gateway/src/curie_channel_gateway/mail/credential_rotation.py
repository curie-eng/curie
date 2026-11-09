"""Opt-in credential renewal and a private snapshot beside the delivery journal.

The API verifies signatures. Decoded claims here are scheduling hints and local
identity checks; the journal directory is trusted credential storage.
"""

from __future__ import annotations

import base64
import binascii
import http.client
import json
import logging
import os
import sqlite3
import stat
import threading
import time
from pathlib import Path
from typing import Any

from .agentmail import request
from .config import MailAdapterConfig

logger = logging.getLogger(__name__)
ADAPTER_HEADER = "X-Curie-Adapter-Principal"
MAX_TOKEN_BYTES = 16_384


def claims(token: str, prefix: str) -> dict[str, Any]:
    if len(token) > MAX_TOKEN_BYTES:
        return {}
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != prefix or not parts[2]:
        return {}
    try:
        value = json.loads(
            base64.b64decode(
                parts[1] + "=" * (-len(parts[1]) % 4),
                altchars=b"-_",
                validate=True,
            )
        )
    except (ValueError, UnicodeDecodeError, binascii.Error):
        return {}
    return value if isinstance(value, dict) else {}


def expiry(token: str, prefix: str) -> int | None:
    exp = claims(token, prefix).get("exp")
    if type(exp) is int and 0 < exp < 253_402_300_800:
        return exp
    return None


def _channel_identity(token: str) -> tuple[str, int] | None:
    value = claims(token, "chn")
    binding, generation = value.get("channel_id"), value.get("generation")
    if (
        isinstance(binding, str)
        and binding
        and type(generation) is int
        and generation >= 0
        and value.get("scope") == "channel.enqueue"
        and expiry(token, "chn") is not None
    ):
        return binding, generation
    return None


def _principal_identity(token: str) -> tuple[str, tuple[str, ...]] | None:
    value = claims(token, "adp")
    subject, bindings = value.get("sub"), value.get("bindings")
    if (
        isinstance(subject, str)
        and subject.strip()
        and isinstance(bindings, list)
        and bindings
        and all(isinstance(binding, str) for binding in bindings)
        and bindings == sorted(set(bindings))
        and value.get("kind") == "adapter"
        and value.get("scopes") == ["approvals:read", "approvals:resolve", "channels:token"]
        and expiry(token, "adp") is not None
    ):
        return subject, tuple(bindings)
    return None


class CredentialRotation:
    def __init__(self, config: MailAdapterConfig, *, provider: str, inbox: str) -> None:
        self.config = config
        self.lock = threading.RLock()
        self.channel_token = config.channel_token
        self.adapter_principal = config.adapter_principal
        self._retry_at = {"chn": 0.0, "adp": 0.0, "storage": 0.0}
        self._failures = {"chn": 0, "adp": 0, "storage": 0}
        self._dirty = False
        self._channel_rejected = False
        self._thread: threading.Thread | None = None
        self._db: sqlite3.Connection | None = None
        self._inbox = inbox
        self._context = json.dumps([config.api_base_url.rstrip("/"), provider, inbox])
        if not config.self_rotate:
            return
        channel = _channel_identity(self.channel_token)
        principal = _principal_identity(self.adapter_principal)
        if channel is None or principal is None or channel[0] not in principal[1]:
            raise RuntimeError(
                "CURIE_MAIL_SELF_ROTATE requires a scoped CURIE_CHANNEL_TOKEN and "
                "CURIE_ADAPTER_PRINCIPAL serving the same binding"
            )
        path = Path(config.state_path + ".credentials.sqlite3")
        try:
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            try:
                info = os.fstat(fd)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077
                ):
                    raise ValueError("unsafe credential file")
            finally:
                os.close(fd)
            self._db = sqlite3.connect(path, check_same_thread=False)
            self._db.execute("PRAGMA journal_mode=DELETE")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS credentials "
                "(id INTEGER PRIMARY KEY CHECK(id=1), context TEXT NOT NULL, "
                "channel TEXT NOT NULL, principal TEXT NOT NULL)"
            )
            row = self._db.execute(
                "SELECT context, channel, principal FROM credentials WHERE id=1"
            ).fetchone()
            if row is not None:
                if row[0] != self._context:
                    raise ValueError("credential file belongs to another adapter")
                self._restore(row[1], row[2])
            self._persist()
        except (OSError, ValueError, sqlite3.Error):
            if self._db is not None:
                self._db.close()
            raise RuntimeError(
                "mail credential storage is unreadable, unsafe or belongs to another adapter; "
                "restore its owner-only snapshot or provision a fresh journal directory"
            ) from None

    def _restore(self, channel: str, principal: str) -> None:
        if not isinstance(channel, str) or not isinstance(principal, str):
            raise ValueError("invalid credential snapshot")
        old_channel = _channel_identity(self.channel_token)
        new_channel = _channel_identity(channel)
        old_principal = _principal_identity(self.adapter_principal)
        new_principal = _principal_identity(principal)
        if new_channel is None or new_principal is None:
            raise ValueError("invalid credential snapshot")
        if (
            old_channel is None
            or old_principal is None
            or new_channel[0] != old_channel[0]
            or new_principal != old_principal
        ):
            raise ValueError("credential snapshot identity mismatch")
        now = time.time()
        if (expiry(channel, "chn") or 0) > now and new_channel[1] >= old_channel[1]:
            self.channel_token = channel
        if (expiry(principal, "adp") or 0) > max(now, expiry(self.adapter_principal, "adp") or 0):
            self.adapter_principal = principal

    def _persist(self) -> None:
        assert self._db is not None
        with self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO credentials VALUES (1, ?, ?, ?)",
                (self._context, self.channel_token, self.adapter_principal),
            )
        self._dirty = False
        self._failures["storage"] = 0

    def _failed(self, kind: str, now: float, exp: int | None, *, status: int = 0) -> None:
        self._failures[kind] += 1
        remaining = max(0.0, (exp or now) - now)
        delay = min(60.0, float(2 ** min(self._failures[kind] - 1, 6)))
        if remaining > 0:
            delay = min(delay, max(0.1, remaining / 4))
        self._retry_at[kind] = now + delay
        logger.error(
            "credential renewal failed kind=%s status=%d "
            "expires_in_seconds=%.0f retry_in_seconds=%.1f",
            kind,
            status,
            remaining,
            delay,
        )

    def renew_once(self, *, now: float | None = None) -> None:
        if not self.config.self_rotate:
            return
        with self.lock:
            at = time.time() if now is None else now
            if self._dirty:
                if at < self._retry_at["storage"]:
                    return
                try:
                    self._persist()
                except sqlite3.Error:
                    self._failed("storage", at, expiry(self.channel_token, "chn"))
                    return
            for kind, ttl in (
                ("adp", self.config.principal_ttl_seconds),
                ("chn", self.config.channel_token_ttl_seconds),
            ):
                current = self.adapter_principal if kind == "adp" else self.channel_token
                exp = expiry(current, kind)
                due = exp is not None and at >= exp - min(300, ttl / 2)
                if kind == "chn" and self._channel_rejected:
                    due = True
                if not due or at < self._retry_at[kind]:
                    continue
                if (expiry(self.adapter_principal, "adp") or 0) <= at:
                    self._failed(kind, at, exp)
                    continue
                body: dict[str, Any] = {"ttl_s": ttl}
                if kind == "chn":
                    body.update(kind="email", address=self._inbox)
                    route = self.config.channel_adapter
                    if route:
                        body["adapter"] = route
                path = (
                    "/approvals/principals/adapter/rotate" if kind == "adp" else "/channels/token"
                )
                try:
                    result = request(
                        "POST",
                        self.config.api_base_url.rstrip("/") + path,
                        body,
                        {ADAPTER_HEADER: self.adapter_principal},
                        max_response_bytes=MAX_TOKEN_BYTES + 4096,
                        timeout_seconds=10.0,
                    )
                except (OSError, ValueError, http.client.HTTPException):
                    self._failed(kind, at, exp)
                    continue
                candidate = result.body.get("token") if isinstance(result.body, dict) else None
                if (
                    result.status not in (200, 201)
                    or not isinstance(candidate, str)
                    or not self._successor(current, candidate, kind, at)
                ):
                    self._failed(kind, at, exp, status=result.status)
                    continue
                if kind == "adp":
                    self.adapter_principal = candidate
                else:
                    self.channel_token = candidate
                    self._channel_rejected = False
                self._failures[kind] = 0
                self._retry_at[kind] = 0.0
                self._dirty = True
                try:
                    self._persist()
                except sqlite3.Error:
                    self._failed("storage", at, expiry(candidate, kind))
                    return
                logger.info(
                    "credential renewed kind=%s expires_in_seconds=%.0f",
                    kind,
                    (expiry(candidate, kind) or at) - at,
                )

    def _successor(self, current: str, candidate: str, kind: str, now: float) -> bool:
        if (expiry(candidate, kind) or 0) <= now:
            return False
        if kind == "adp":
            return _principal_identity(candidate) == _principal_identity(current) and (
                expiry(candidate, kind) or 0
            ) > (expiry(current, kind) or 0)
        old, new = _channel_identity(current), _channel_identity(candidate)
        return old is not None and new is not None and new[0] == old[0] and new[1] > old[1]

    def channel_rejected(self) -> None:
        if self.config.self_rotate:
            with self.lock:
                self._channel_rejected = True

    def status(self) -> dict[str, Any]:
        exp = expiry(self.adapter_principal, "adp")
        return {
            "enabled": self.config.self_rotate,
            "principal_exp": exp,
            "ready": not self.config.self_rotate or (exp or 0) > time.time(),
            "persistence_pending": self._dirty,
        }

    def start(self, shutdown: threading.Event) -> None:
        if not self.config.self_rotate or self._thread is not None:
            return

        def run() -> None:
            while not shutdown.is_set():
                self.renew_once()
                shutdown.wait(0.25)

        self._thread = threading.Thread(target=run, name="mail-credential-renewal", daemon=True)
        self._thread.start()

    def close(self) -> None:
        if self._thread is not None:
            self._thread.join()
        if self._db is not None:
            self._db.close()
