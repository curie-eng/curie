"""The worker's client for the thread attachment ledger (ADR 0205, #4079).

The ledger is API-owned thread state keyed exactly like the transcript. The
worker reads it before every boot so the boot can rebuild the whole thread's
files, and appends the current message's references once they are installed.
Both routes live under ``/v1/internal`` behind the internal worker token and
nothing else: no sandbox credential can reach the ledger, so an agent cannot
plant a file id for the worker to fetch with the channel credential.

A reference never records an endpoint, a URL or bytes. The route a re-fetch
uses is resolved from the agent's bindings as they are at boot time, which is
why ``ThreadAttachmentRef`` carries only the route's kind, adapter and bot
identity.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

_QUERY_PATH = "/v1/internal/thread-attachments/query"
_APPEND_PATH = "/v1/internal/thread-attachments/append"


class LedgerUnavailable(RuntimeError):
    """The thread attachment ledger could not be read or written."""


@dataclass(frozen=True)
class ThreadAttachmentRef:
    """One recorded file of a thread, exactly the API's wire fields."""

    file_id: str
    ordinal: int
    name: str
    disk_name: str
    mime_type: str | None
    size_bytes: int | None
    sha256: str
    route_kind: str
    route_adapter: str | None
    route_identity: str

    def to_wire(self) -> dict[str, Any]:
        return {
            "file_id": self.file_id,
            "ordinal": self.ordinal,
            "name": self.name,
            "disk_name": self.disk_name,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "route_kind": self.route_kind,
            "route_adapter": self.route_adapter,
            "route_identity": self.route_identity,
        }

    @classmethod
    def from_wire(cls, raw: Mapping[str, Any]) -> ThreadAttachmentRef:
        """Decode one wire ref, raising ``LedgerUnavailable`` on any short or
        mistyped field rather than guessing a value the API did not send."""

        try:
            mime = raw["mime_type"]
            size = raw["size_bytes"]
            adapter = raw["route_adapter"]
            ordinal = raw["ordinal"]
            if isinstance(ordinal, bool) or not isinstance(ordinal, int):
                raise TypeError("ordinal is not an integer")
            if size is not None and (isinstance(size, bool) or not isinstance(size, int)):
                raise TypeError("size_bytes is not an integer")
            ref = cls(
                file_id=_text(raw["file_id"]),
                ordinal=ordinal,
                name=_text(raw["name"]),
                disk_name=_text(raw["disk_name"]),
                mime_type=None if mime is None else _text(mime),
                size_bytes=size,
                sha256=_text(raw["sha256"]),
                route_kind=_text(raw["route_kind"]),
                route_adapter=None if adapter is None else _text(adapter),
                route_identity=_text(raw["route_identity"]),
            )
        except (KeyError, TypeError) as exc:
            raise LedgerUnavailable("thread attachment ref is malformed") from exc
        return ref


def _text(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("expected a string")
    return value


class ThreadAttachmentLedgerClient:
    """Read and append a thread's attachment references through the API."""

    def __init__(
        self,
        *,
        api_base_url: str,
        worker_token: str,
        client: httpx.AsyncClient,
        append_attempts: int = 3,
        retry_backoff_s: float = 0.2,
    ) -> None:
        if not worker_token:
            raise ValueError("the thread attachment ledger requires internal worker auth")
        if append_attempts < 1:
            raise ValueError("append_attempts must be at least 1")
        if retry_backoff_s < 0:
            raise ValueError("retry_backoff_s must not be negative")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._client = client
        self._append_attempts = append_attempts
        self._retry_backoff_s = retry_backoff_s

    async def query(self, *, agent_id: str, thread_key: str) -> tuple[ThreadAttachmentRef, ...]:
        """The thread's live references in arrival order.

        Any failure is ``LedgerUnavailable``: the caller decides what an
        unreadable ledger means for its turn (ADR 0205 decision 6).
        """

        try:
            response = await self._client.post(
                f"{self._base}{_QUERY_PATH}",
                headers=self._headers,
                json={"agent_id": agent_id, "thread_key": thread_key},
                follow_redirects=False,
            )
        except httpx.HTTPError as exc:
            raise LedgerUnavailable("thread attachment ledger is unreachable") from exc
        if response.status_code != 200:
            raise LedgerUnavailable(
                f"thread attachment ledger query returned HTTP {response.status_code}"
            )
        try:
            rows = response.json()["refs"]
            if not isinstance(rows, list):
                raise TypeError("refs is not a list")
            if not all(isinstance(row, Mapping) for row in rows):
                raise TypeError("a ref is not an object")
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerUnavailable("thread attachment ledger answer was unusable") from exc
        return tuple(ThreadAttachmentRef.from_wire(row) for row in rows)

    async def append(
        self,
        *,
        agent_id: str,
        thread_key: str,
        event_id: str,
        refs: Iterable[ThreadAttachmentRef],
    ) -> int:
        """Record installed references, idempotent per (event, file id).

        A transport error or 5xx is retried up to ``append_attempts`` requests
        in total, which is safe because the API's append is idempotent. A 4xx
        is a refusal and is not retried.
        """

        wire = [ref.to_wire() for ref in refs]
        if not wire:
            return 0
        body = {
            "agent_id": agent_id,
            "thread_key": thread_key,
            "event_id": event_id,
            "refs": wire,
        }
        last: str = "no attempt was made"
        for attempt in range(self._append_attempts):
            if attempt and self._retry_backoff_s:
                await asyncio.sleep(self._retry_backoff_s * attempt)
            try:
                response = await self._client.post(
                    f"{self._base}{_APPEND_PATH}",
                    headers=self._headers,
                    json=body,
                    follow_redirects=False,
                )
            except httpx.HTTPError as exc:
                last = f"unreachable ({type(exc).__name__})"
                continue
            if response.status_code >= 500:
                last = f"HTTP {response.status_code}"
                continue
            if response.status_code != 200:
                raise LedgerUnavailable(
                    f"thread attachment ledger refused the append: HTTP {response.status_code}"
                )
            try:
                appended = response.json()["appended"]
                if isinstance(appended, bool) or not isinstance(appended, int):
                    raise TypeError("appended is not an integer")
            except (KeyError, TypeError, ValueError) as exc:
                raise LedgerUnavailable(
                    "thread attachment ledger append answer was unusable"
                ) from exc
            return appended
        raise LedgerUnavailable(
            f"thread attachment ledger append failed after "
            f"{self._append_attempts} attempts: {last}"
        )
