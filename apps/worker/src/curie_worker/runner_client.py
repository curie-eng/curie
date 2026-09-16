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

import asyncio
import base64
import binascii
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, TypeVar

import aiohttp
from aci_protocol import Event, Final, Interrupt, OutboundEvent, parse_ndjson_line
from aiohttp.helpers import sentinel
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
# The smallest per-request bound a spent budget may derive. See
# ``_request_timeout``: aiohttp treats a total of 0.0 as "no timeout".
_MIN_REQUEST_TIMEOUT_S = 0.05
_POST_FINAL_CLEANUP_TIMEOUT_S = 1.0
_POST_FINAL_DISCARD_CHUNK_BYTES = 64 * 1024
_TURN_EPOCH_HEADER = "X-Curie-Turn-Epoch"
_TURN_EPOCH_MIN_LENGTH = 32
_TURN_EPOCH_MAX_LENGTH = 256
_T = TypeVar("_T")

logger = logging.getLogger(__name__)


def _auth_headers(token: str | None) -> dict[str, str] | None:
    """Per-call Authorization header for the per-sandbox runner token (issue #63).

    The ClientSession is worker-wide and dials many base_urls, so the token is a
    per-call header, never a session default -- a default would leak one sandbox's
    token to every other. Returns None (no header) when the token is unset/empty.
    """
    if token:
        return {"Authorization": f"Bearer {token}"}
    return None


def _mark_rpc_failed(span: Any, outcome: str, cause: BaseException) -> None:
    """Stamp one runner-RPC span as failed, in the one shared vocabulary."""
    if hasattr(span, "set_status"):
        span.set_status(StatusCode.ERROR)
    span.add_event(
        "runner.rpc.failed",
        {"outcome": outcome, "error.class": type(cause).__name__},
    )


def _valid_turn_epoch(value: str | None) -> bool:
    """Accept only the runner's bounded, URL-safe opaque turn capability."""
    return bool(
        value
        and _TURN_EPOCH_MIN_LENGTH <= len(value) <= _TURN_EPOCH_MAX_LENGTH
        and value.isascii()
        and all(character.isalnum() or character in "-_" for character in value)
    )


class RunnerError(Exception):
    """The runner returned an unexpected HTTP status or an unreadable stream."""


class RunnerStreamTimeout(TimeoutError):
    """The streamed turn body exceeded the client's total/sock-read budget.

    A ``TimeoutError`` subclass on purpose (#2011): every existing
    ``except TimeoutError`` / ``except (aiohttp.ClientError, TimeoutError)``
    handler in the worker keeps catching this unchanged. What it adds is a
    non-empty ``str()`` -- ``str(TimeoutError())`` is the EMPTY STRING, which is
    how a real 600s cluster timeout reached the operator log as "turn stream
    dropped for <id>: " with nothing after the colon -- naming both the
    normalized underlying exception class and the budget that expired.
    """


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

    def __init__(
        self,
        response: aiohttp.ClientResponse,
        budget_s: float | None = None,
        timeout_callback: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._response = response
        self._saw_final = False
        # The streaming budget this stream is running under, carried from the
        # client so the stream can NAME the budget it blew (#2011). Optional so
        # a directly-constructed TurnStream (tests, evals) still works.
        self._budget_s = budget_s
        # A successful /v1/event response may bind its opaque turn epoch to a
        # separately bounded control call. Consume it before awaiting so a
        # repeated iterator cannot notify the same turn twice.
        self._timeout_callback = timeout_callback

    async def __aiter__(self) -> AsyncIterator[OutboundEvent]:
        try:
            async for raw in self._response.content:
                line = raw.decode("utf-8").strip()
                if not line:
                    continue
                frame = parse_ndjson_line(line)
                if isinstance(frame, Final):
                    self._saw_final = True
                yield frame
                if isinstance(frame, Final):
                    # Final is terminal for every consumer, including evals that
                    # naturally iterate to stream end. Bytes after it belong only
                    # to the bounded transport cleanup in __aexit__; they must not
                    # be parsed, applied, or allowed to occupy the 600s turn budget.
                    return
        except TimeoutError as cause:
            # ONLY TimeoutError (which covers asyncio.TimeoutError,
            # aiohttp.ServerTimeoutError and aiohttp.SocketTimeoutError). A
            # genuine connection reset is an aiohttp.ClientError and is NOT a
            # timeout: it must keep flowing to the kernel as the generic
            # runner-error it has always been. asyncio.CancelledError does not
            # subclass TimeoutError, so cooperative cancellation still passes
            # straight through -- do not broaden this clause.
            self._record_stream_timeout(cause)
            await self._notify_timeout()
            raise RunnerStreamTimeout(self._timeout_reason(cause)) from cause

    async def _notify_timeout(self) -> None:
        callback = self._timeout_callback
        self._timeout_callback = None
        if callback is None:
            return
        try:
            await callback()
        except Exception as exc:  # noqa: BLE001 - preserve the causal body timeout
            # The epoch, bearer, and response body are deliberately absent. A
            # failed notification leaves abandonment as the runner's truthful
            # best-effort terminal, but must never replace RunnerStreamTimeout.
            logger.warning(
                "runner timeout terminal notification failed (%s)",
                type(exc).__name__,
            )

    def _timeout_reason(self, cause: BaseException) -> str:
        budget = "unbounded" if self._budget_s is None else f"{self._budget_s}s"
        return (
            f"runner turn stream exceeded its {budget} total/sock-read budget "
            f"({type(cause).__name__})"
        )

    def _record_stream_timeout(self, cause: BaseException) -> None:
        """Emit the terminal record this boundary previously never produced.

        ``RunnerClient._rpc``'s span for ``start_turn`` closes as soon as the
        response HEADERS arrive, so a budget expiring while the NDJSON BODY is
        read left no evidence at the RPC boundary at all -- the only
        ``curie.runner.rpc.result`` point for the turn said ``success`` (#2011).
        The attribute values here are already in the shared allowlist, and the
        span/event keys are the same closed vocabulary ``_rpc`` uses.
        """
        reason = self._timeout_reason(cause)
        logger.warning("%s", reason)
        attributes = {
            "service.name": "curie-worker",
            "operation": "event",
            "role": "client",
        }
        record_metric(
            "curie.runner.rpc.result",
            attributes={**attributes, "outcome": "timeout"},
        )
        with operation_span(
            "curie.runner.rpc",
            kind=SpanKind.CLIENT,
            attributes=attributes,
        ) as span:
            _mark_rpc_failed(span, "timeout", cause)

    async def _discard_post_final(self) -> None:
        """Briefly keep the transport open while discarding bytes through EOF.

        ``Kernel._consume`` stops applying frames at ``Final`` so a late line
        cannot overwrite the outcome. Releasing at that moment closes the
        socket while the runner is still recording the turn and calling
        ``write_eof`` (issue #1958). The runner-controlled tail is read in
        fixed-size chunks and given its own short bound rather than the normal
        600-second stream timeout. Once a valid Final was observed, cleanup is
        best-effort and cannot turn that successful result into a retry.
        """
        if not self._saw_final or self._response.content.at_eof():
            return
        try:
            async with asyncio.timeout(_POST_FINAL_CLEANUP_TIMEOUT_S):
                while not self._response.content.at_eof():
                    chunk = await self._response.content.read(
                        _POST_FINAL_DISCARD_CHUNK_BYTES
                    )
                    if not chunk:
                        break
        except TimeoutError:
            return
        except Exception:  # noqa: BLE001 - post-Final cleanup is best-effort
            return

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
        try:
            if exc_type is None:
                await self._discard_post_final()
        finally:
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
        self._total_timeout_s = total_timeout_s
        self._own_session = session is None
        self._connect_timeout_s = connect_timeout_s
        # Since ADR-0131 this is a per-request CEILING inside the delivery's one
        # overall deadline, not an independent clock. It stays the session
        # default (so every caller without a budget is behaviourally unchanged)
        # and is the upper half of the ``min`` in ``_request_timeout``.
        self._total_timeout_s = total_timeout_s
        self._session = session or aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=total_timeout_s, connect=connect_timeout_s, sock_read=total_timeout_s
            )
        )
        # A per-request override, not folded into the session default above: it
        # replaces (not merges with) the session timeout for this one call, so
        # ``/v1/interrupt`` gets its own short control-plane budget regardless of
        # how the streaming timeouts above are tuned. The budget-derived
        # overrides below rely on exactly that mechanic.
        #
        # ``/v1/interrupt`` is DELIBERATELY excluded from the budget path and a
        # test guards it structurally: the interrupt is the fail-closed stop a
        # lost lease fires, and deriving its timeout from a budget that may
        # already be exhausted would make the fence unable to stop the runner it
        # just fenced.
        self._interrupt_timeout = aiohttp.ClientTimeout(total=interrupt_timeout_s)
        self._snapshot_patch_max_bytes = snapshot_patch_max_bytes
        self._snapshot_body_max_bytes = (
            4 * ((snapshot_patch_max_bytes + 2) // 3) + 131_072
        )

    def _request_timeout(self, remaining_s: float | None) -> aiohttp.ClientTimeout | Any:
        """The per-request timeout for a delivery with ``remaining_s`` of budget.

        Returns aiohttp's own ``sentinel`` when there is no budget in hand, so
        the session default applies and every leaseless caller is
        byte-identical in behavior. An explicit ``timeout=None`` would not do
        that: aiohttp reads it as ``ClientTimeout(total=None)``, i.e. no
        timeout at all -- the one shape this method must never produce. The
        sentinel is aiohttp's own "use the session default" value (it is what
        ``timeout`` defaults to internally); its type is private to aiohttp,
        hence the ``Any`` half of the return annotation.

        The effective bound is ``min(total_timeout_s, remaining_s)``: the budget
        can only ever SHORTEN a request. A 30-minute delivery must not hand one
        runner request a 30-minute HTTP deadline.

        A spent budget is floored to ``_MIN_REQUEST_TIMEOUT_S`` rather than
        passed through. aiohttp starts a timeout handle only ``if timeout > 0``,
        so a bounded value of exactly 0.0 disables the timeout entirely -- the
        "no timeout at all" shape this method must never produce, and it would
        arrive precisely when the delivery has the least time to spare. The
        floor is small enough that an exhausted budget still fails fast.
        """
        if remaining_s is None:
            return sentinel
        bounded = max(_MIN_REQUEST_TIMEOUT_S, min(self._total_timeout_s, remaining_s))
        return aiohttp.ClientTimeout(
            total=bounded, connect=self._connect_timeout_s, sock_read=bounded
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
                _mark_rpc_failed(span, outcome, exc)
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

    async def start_turn(
        self,
        base_url: str,
        event: Event,
        token: str | None = None,
        *,
        remaining_s: float | None = None,
    ) -> TurnStream:
        """Open a turn. Returns once the runner has accepted it (turn active)."""
        request_timeout = self._request_timeout(remaining_s)
        stream_timeout_s = (
            self._total_timeout_s
            if request_timeout is sentinel
            else request_timeout.total
        )
        if remaining_s is not None:
            logger.info(
                f"runner request timeout bound: configured ceiling {self._total_timeout_s:.3f}s, "
                f"remaining delivery {remaining_s:.3f}s, effective timeout "
                f"{stream_timeout_s:.3f}s"
            )

        async def request(headers: dict[str, str] | None) -> tuple[TurnStream, str]:
            resp = await self._session.post(
                f"{base_url}/v1/event",
                json=event.model_dump(),
                headers=headers,
                timeout=request_timeout,
            )
            if resp.status != 200:
                body = await resp.text()
                resp.release()
                raise RunnerError(f"/v1/event -> {resp.status}: {body}")
            turn_epoch = resp.headers.get(_TURN_EPOCH_HEADER)
            timeout_callback: Callable[[], Awaitable[None]] | None = None
            if _valid_turn_epoch(turn_epoch):
                # ``turn_epoch`` is narrowed by the validation above. Keep the
                # callback response-bound so a retry can never inherit a stale
                # epoch from an earlier /v1/event.
                async def notify_timeout() -> None:
                    assert turn_epoch is not None
                    await self._notify_timeout(base_url, turn_epoch, token)

                timeout_callback = notify_timeout
            return TurnStream(resp, stream_timeout_s, timeout_callback), "success"

        return await self._rpc("event", token, request)

    async def _notify_timeout(
        self,
        base_url: str,
        turn_epoch: str,
        token: str | None,
    ) -> None:
        """Best-effort causal timeout notification for one accepted turn."""

        async def request(headers: dict[str, str] | None) -> tuple[None, str]:
            control_headers = dict(headers or {})
            control_headers[_TURN_EPOCH_HEADER] = turn_epoch
            async with self._session.post(
                f"{base_url}/v1/timeout",
                headers=control_headers,
                timeout=self._interrupt_timeout,
            ) as resp:
                if resp.status not in (200, 409):
                    # Do not read or echo an arbitrary response body on this
                    # sensitive best-effort path.
                    raise RunnerError(f"/v1/timeout -> {resp.status}")
                return None, "conflict" if resp.status == 409 else "success"

        await self._rpc("timeout", token, request)

    async def steer(
        self,
        base_url: str,
        event: Event,
        token: str | None = None,
        *,
        remaining_s: float | None = None,
    ) -> bool:
        """Inject a follow-up into the live turn. False on 409 (no active turn)."""

        async def request(headers: dict[str, str] | None) -> tuple[bool, str]:
            async with self._session.post(
                f"{base_url}/v1/steer",
                json=event.model_dump(),
                headers=headers,
                timeout=self._request_timeout(remaining_s),
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

    async def reset(
        self,
        base_url: str,
        token: str | None = None,
        *,
        remaining_s: float | None = None,
    ) -> None:
        """Discard the runner's conversation so the next turn starts fresh (#550).

        The eval driver calls this between cases to enforce per-case isolation.
        A 409 (a turn is still active) is surfaced as a ``RunnerError`` like any
        other unexpected status: the eval flow is sequential, so a turn should
        never be live at reset time -- a 409 here is a real ordering bug, not a
        condition to swallow.
        """
        async def request(headers: dict[str, str] | None) -> tuple[None, str]:
            async with self._session.post(
                f"{base_url}/v1/reset",
                headers=headers,
                timeout=self._request_timeout(remaining_s),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RunnerError(f"/v1/reset -> {resp.status}: {body}")
                return None, "success"

        await self._rpc("reset", token, request)

    async def snapshot(
        self,
        base_url: str,
        token: str | None = None,
        *,
        remaining_s: float | None = None,
    ) -> RunnerWorkspaceSnapshot:
        """Capture a bounded patch before a publication approval suspends.

        This call is always runner-token authenticated. A missing token is a
        worker invariant violation, not a request to try the unauthenticated
        route; refusing it prevents a publication snapshot from becoming a
        bearer-less sandbox endpoint on legacy claims.
        """

        if not token:
            raise RunnerError("publication snapshot requires a runner token")
        async with self._session.post(
            f"{base_url}/v1/snapshot",
            headers=_auth_headers(token),
            timeout=self._request_timeout(remaining_s),
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

    async def status(
        self,
        base_url: str,
        *,
        token: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        path = "/v1/status" if token else "/status"

        async def request(headers: dict[str, str] | None) -> tuple[dict[str, object], str]:
            async with self._session.get(
                f"{base_url}{path}",
                headers=headers,
                timeout=self._request_timeout(remaining_s),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    raise RunnerError(f"{path} -> {resp.status}: {body}")
                data: dict[str, object] = await resp.json()
                return data, "success"

        return await self._rpc("status", token, request)

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
