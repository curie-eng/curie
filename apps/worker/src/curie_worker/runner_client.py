"""Async HTTP client for the runner's ACI channel.

The runner (D1) exposes the ACI session over HTTP: ``POST /v1/event`` opens a turn
and streams outbound NDJSON to a ``final``; ``POST /v1/steer`` injects a follow-up
into the live turn (409 when no turn is active, the finish-race boundary the
kernel owns); ``POST /v1/interrupt`` hard-stops; ``GET /status`` reports turn
state. This client turns those into typed calls the kernel composes.

The turn is split into ``start_turn`` (awaits the response headers, at which point
the runner's turn is active) and iterating the returned ``TurnStream`` (the
NDJSON body). That split lets the kernel establish the active turn while holding
the per-thread lock, then release the lock and stream the body, so a concurrent
follow-up can only steer the live turn and never fork a second one.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import TypeVar

import aiohttp
from aci_protocol import Event, Interrupt, OutboundEvent, parse_ndjson_line
from curie_telemetry import inject_trace_context, operation_span, record_metric
from opentelemetry.trace import SpanKind, StatusCode

# The interrupt RPC is a control-plane POST, not a streaming turn (#742, a
# follow-up to #739): it exists only to hard-stop the live turn, never to carry
# a turn's output, so it must not inherit ``connect_timeout_s``/``total_timeout_s``,
# which are tuned for a long-running streamed turn (default 600s). A wedged
# runner that accepts the TCP connect and then answers nothing would otherwise
# hang every interrupt caller for up to that streaming budget. A healthy runner
# answers an interrupt well under a second. This bound lives here, at the RPC
# itself, so every caller inherits it for free; each caller then layers its own
# policy on top (``Kernel.release_thread`` swallows and releases,
# ``Kernel.interrupt_agent`` and the kill switch surface the failure and keep
# going) instead of re-deriving the bound -- or a coupling to this client's
# other timeouts -- at each call site.
_DEFAULT_INTERRUPT_TIMEOUT_S = 5.0
_T = TypeVar("_T")


def _auth_headers(token: str | None) -> dict[str, str] | None:
    """Per-call Authorization header for the per-sandbox runner token (issue #63).

    The ClientSession is worker-wide and dials many base_urls, so the token is a
    per-call header, never a session default -- a default would leak one sandbox's
    token to every other. Returns None (no header) when the token is unset/empty.
    """
    if token:
        return {"Authorization": f"Bearer {token}"}
    return None


class RunnerError(Exception):
    """The runner returned an unexpected HTTP status or an unreadable stream."""


@dataclass(frozen=True)
class RunnerWorkspaceSnapshot:
    """Authenticated runner snapshot after strict boundary validation."""

    repo_full_name: str
    base_sha: str
    patch: bytes
    changed_paths: tuple[str, ...]
    contains_workflow_files: bool
    publication_title: str
    publication_body: str


class TurnStream:
    """An open ``/v1/event`` response: the turn is active; iterate for frames."""

    def __init__(self, response: aiohttp.ClientResponse) -> None:
        self._response = response

    async def __aiter__(self) -> AsyncIterator[OutboundEvent]:
        async for raw in self._response.content:
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            yield parse_ndjson_line(line)

    def close(self) -> None:
        self._response.release()

    async def __aenter__(self) -> TurnStream:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class RunnerClient:
    """Dials a claimed runner over its base_url. One client serves all threads."""

    def __init__(
        self,
        *,
        connect_timeout_s: float = 10.0,
        total_timeout_s: float = 600.0,
        interrupt_timeout_s: float = _DEFAULT_INTERRUPT_TIMEOUT_S,
        snapshot_patch_max_bytes: int = 900_000,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        if snapshot_patch_max_bytes <= 0:
            raise ValueError("snapshot patch byte limit must be positive")
        self._own_session = session is None
        self._session = session or aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=total_timeout_s, connect=connect_timeout_s, sock_read=total_timeout_s
            )
        )
        # A per-request override, not folded into the session default above: it
        # replaces (not merges with) the session timeout for this one call, so
        # ``/v1/interrupt`` gets its own short control-plane budget regardless of
        # how the streaming timeouts above are tuned.
        self._interrupt_timeout = aiohttp.ClientTimeout(total=interrupt_timeout_s)
        self._snapshot_patch_max_bytes = snapshot_patch_max_bytes
        self._snapshot_body_max_bytes = (
            4 * ((snapshot_patch_max_bytes + 2) // 3) + 131_072
        )

    async def _rpc(
        self,
        operation: str,
        token: str | None,
        request: Callable[[dict[str, str] | None], Awaitable[tuple[_T, str]]],
    ) -> _T:
        """Measure one HTTP boundary and propagate only W3C trace context."""

        attributes = {
            "service.name": "curie-worker",
            "operation": operation,
            "role": "client",
        }
        started = time.monotonic()
        result: _T | None = None
        outcome = "failure"
        error: Exception | None = None
        with operation_span(
            "curie.runner.rpc",
            kind=SpanKind.CLIENT,
            attributes=attributes,
        ) as span:
            headers = dict(_auth_headers(token) or {})
            inject_trace_context(headers)
            try:
                result, outcome = await request(headers or None)
            except Exception as exc:
                error = exc
                outcome = "timeout" if isinstance(exc, TimeoutError) else "failure"
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "runner.rpc.failed",
                    {"outcome": outcome, "error.class": type(exc).__name__},
                )
            else:
                span.add_event("runner.rpc.completed", {"outcome": outcome})

        metric_attributes = {**attributes, "outcome": outcome}
        record_metric(
            "curie.runner.rpc.request.duration",
            max(0.0, time.monotonic() - started),
            attributes=metric_attributes,
        )
        record_metric("curie.runner.rpc.result", attributes=metric_attributes)
        if error is not None:
            raise error
        return result  # type: ignore[return-value]

    async def start_turn(self, base_url: str, event: Event, token: str | None = None) -> TurnStream:
        """Open a turn. Returns once the runner has accepted it (turn active)."""

        async def request(headers: dict[str, str] | None) -> tuple[TurnStream, str]:
            resp = await self._session.post(
                f"{base_url}/v1/event", json=event.model_dump(), headers=headers
            )
            if resp.status != 200:
                body = await resp.text()
                resp.release()
                raise RunnerError(f"/v1/event -> {resp.status}: {body}")
            return TurnStream(resp), "success"

        return await self._rpc("event", token, request)

    async def steer(self, base_url: str, event: Event, token: str | None = None) -> bool:
        """Inject a follow-up into the live turn. False on 409 (no active turn)."""

        async def request(headers: dict[str, str] | None) -> tuple[bool, str]:
            async with self._session.post(
                f"{base_url}/v1/steer", json=event.model_dump(), headers=headers
            ) as resp:
                if resp.status == 409:
                    return False, "conflict"
                if resp.status != 200:
                    body = await resp.text()
                    raise RunnerError(f"/v1/steer -> {resp.status}: {body}")
                return True, "success"

        return await self._rpc("steer", token, request)

    async def interrupt(self, base_url: str, reason: str, token: str | None = None) -> None:
        """Hard-stop the live turn; its final is reclassified to idle.

        Bounded to ``_DEFAULT_INTERRUPT_TIMEOUT_S`` (or the constructor
        override), never the streaming ``total_timeout_s``/``sock_read``
        budget (#742): a wedged runner that accepts the connect and then
        answers nothing must not cost the caller up to that streaming budget
        just to find out. Raises ``asyncio.TimeoutError`` on expiry, same as
        any other failure here -- callers already decide per call site whether
        to swallow-and-fallback or surface-and-continue."""
        frame = Interrupt(reason=reason)

        async def request(headers: dict[str, str] | None) -> tuple[None, str]:
            async with self._session.post(
                f"{base_url}/v1/interrupt",
                json=frame.model_dump(),
                headers=headers,
                timeout=self._interrupt_timeout,
            ) as resp:
                if resp.status not in (200, 409):
                    body = await resp.text()
                    raise RunnerError(f"/v1/interrupt -> {resp.status}: {body}")
                return None, "conflict" if resp.status == 409 else "success"

        await self._rpc("interrupt", token, request)

    async def reset(self, base_url: str, token: str | None = None) -> None:
        """Discard the runner's conversation so the next turn starts fresh (#550).

        The eval driver calls this between cases to enforce per-case isolation.
        A 409 (a turn is still active) is surfaced as a ``RunnerError`` like any
        other unexpected status: the eval flow is sequential, so a turn should
        never be live at reset time -- a 409 here is a real ordering bug, not a
        condition to swallow.
        """
        async def request(headers: dict[str, str] | None) -> tuple[None, str]:
            async with self._session.post(f"{base_url}/v1/reset", headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RunnerError(f"/v1/reset -> {resp.status}: {body}")
                return None, "success"

        await self._rpc("reset", token, request)

    async def snapshot(self, base_url: str, token: str | None = None) -> RunnerWorkspaceSnapshot:
        """Capture a bounded patch before a publication approval suspends.

        This call is always runner-token authenticated. A missing token is a
        worker invariant violation, not a request to try the unauthenticated
        route; refusing it prevents a publication snapshot from becoming a
        bearer-less sandbox endpoint on legacy claims.
        """

        if not token:
            raise RunnerError("publication snapshot requires a runner token")
        async with self._session.post(
            f"{base_url}/v1/snapshot", headers=_auth_headers(token)
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RunnerError(f"/v1/snapshot -> {resp.status}: {body}")
            try:
                raw = await resp.content.read(self._snapshot_body_max_bytes + 1)
                if len(raw) > self._snapshot_body_max_bytes:
                    raise ValueError("snapshot response exceeds its encoded byte limit")
                body = json.loads(raw)
                encoded = body["patch_base64"]
                if not isinstance(encoded, str):
                    raise TypeError("patch_base64 is not a string")
                patch = base64.b64decode(encoded, validate=True)
                if len(patch) > self._snapshot_patch_max_bytes:
                    raise ValueError(
                        f"patch exceeds {self._snapshot_patch_max_bytes} raw bytes"
                    )
                declared_size = body.get("patch_size_bytes")
                if declared_size != len(patch):
                    raise ValueError("patch size does not match decoded payload")
                paths = body["changed_paths"]
                if not isinstance(paths, list) or not all(
                    isinstance(path, str) and path for path in paths
                ):
                    raise TypeError("changed_paths is not a string list")
                title = body["publication_title"]
                description = body["publication_body"]
                if not isinstance(title, str) or not title.strip() or len(title) > 256:
                    raise TypeError("publication_title is not a bounded non-empty string")
                if (
                    not isinstance(description, str)
                    or not description.strip()
                    or len(description) > 65_536
                ):
                    raise TypeError("publication_body is not a bounded non-empty string")
                return RunnerWorkspaceSnapshot(
                    repo_full_name=str(body["repo_full_name"]),
                    base_sha=str(body["base_sha"]),
                    patch=patch,
                    changed_paths=tuple(paths),
                    contains_workflow_files=bool(body["contains_workflow_files"]),
                    publication_title=title,
                    publication_body=description,
                )
            except (KeyError, TypeError, ValueError, binascii.Error, json.JSONDecodeError) as exc:
                raise RunnerError("/v1/snapshot returned an invalid bounded payload") from exc

    async def status(self, base_url: str) -> dict[str, object]:
        async def request(headers: dict[str, str] | None) -> tuple[dict[str, object], str]:
            async with self._session.get(f"{base_url}/status", headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RunnerError(f"/status -> {resp.status}: {body}")
                data: dict[str, object] = await resp.json()
                return data, "success"

        return await self._rpc("status", None, request)

    async def close(self) -> None:
        if self._own_session:
            await self._session.close()

    async def __aenter__(self) -> RunnerClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()
