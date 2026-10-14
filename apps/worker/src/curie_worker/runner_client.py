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
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Literal, TypeVar

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
_CAPACITY_ADMISSION_HEADER = "X-Curie-Capacity-Admission"
_TURN_EPOCH_MIN_LENGTH = 32
_TURN_EPOCH_MAX_LENGTH = 256
_T = TypeVar("_T")
TimeoutResult = Literal["accepted", "conflict", "unconfirmed"]

logger = logging.getLogger(__name__)

# @spec ACTION-EXECUTOR-5 @spec ACTION-EXECUTOR-24. The runner-private boot
# variable (outside ``BootEnv``) an executor claim sets, and its value. Frozen
# with the runner's reader by ``tests/vectors/runner-execute.json``.
RUNNER_MODE_ENV = "CURIE_RUNNER_MODE"
EXECUTOR_MODE = "execute"

# @spec ACTION-EXECUTOR-6. The executor route's one request shape: every key is
# present on every phase, and a key the phase does not use is null.
EXECUTE_PATH = "/v1/execute"
EXECUTE_REQUEST_KEYS = frozenset(
    {"execution_id", "phase", "connector", "tool", "arguments", "grant", "target"}
)
EXECUTE_PHASES = frozenset({"list", "observe", "call", "read"})
# @spec AUTOMATED-REMEDIATION-12 (executor amendment E3): the ``read`` request
# alone adds the predicate's pointer to the frozen keys.
EXECUTE_READ_KEYS = EXECUTE_REQUEST_KEYS | {"pointer"}
# @spec ACTION-EXECUTOR-20. A route refusal that provably dialed nothing (or, for
# ``connector_unreachable`` on ``list``/``observe``, only a read) maps to one
# pre-dispatch code. Ordering and shape refusals are the worker's own fault
# against a healthy runner, reported as ``runner_unavailable``.
_EXECUTE_REFUSALS = {
    "phase_out_of_order": "runner_unavailable",
    "invalid_request": "runner_unavailable",
    "tool_not_advertised": "tool_not_advertised",
    "restore_not_advertised": "restore_not_advertised",
    "restore_schema_mismatch": "restore_schema_mismatch",
    "arguments_mismatch": "arguments_mismatch",
    # A ``call`` whose connector has no URL the grant header could ride to; the
    # route refuses before dialing, so it is pre-dispatch, never response_lost.
    "connector_not_hosted": "connector_not_hosted",
}
# ``list``, ``observe`` and ``read`` dial reads only, so an unknown refusal is
# ``runner_unavailable``, never ``response_lost`` (AUTOMATED-REMEDIATION-12).
_READ_PHASE_REFUSALS = {
    **_EXECUTE_REFUSALS,
    "connector_unreachable": "connector_unreachable",
    # Executor amendment E4: a read tool not advertised ``readOnlyHint: true``.
    "tool_not_read_only": "tool_not_read_only",
}
_EXECUTE_REFUSAL_BODY_KEY = "refused"
_EXECUTE_BODY_MAX_BYTES = 1_048_576


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


class ExecuteRefused(RunnerError):
    """An executor route call ended without a usable phase response.

    ``code`` is the closed ACTION-EXECUTOR-20 code the worker reports: a
    pre-dispatch code (``runner_unavailable`` for a runner without the route, an
    unreachable runner or an ordering refusal) or, for a ``call`` whose outcome
    may have reached the connector, ``response_lost``, which is never a refusal
    (ACTION-EXECUTOR-17).
    """

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"/v1/execute: {code} ({detail})")
        self.code = code


def _execute_failure_code(phase: str, status: int | None, refused: object) -> str:
    """The ACTION-EXECUTOR-20 code for a failed executor route call.

    ``status`` is None for a transport failure. Only a refusal the route makes
    before dialing is a provable non-write; anything else on a ``call``, an
    unknown code above all, may have reached the connector and is
    ``response_lost``.
    """

    if status == 404:
        # A runner image without the route (ACTION-EXECUTOR-24); it dials nothing.
        return "runner_unavailable"
    if phase == "call":
        if isinstance(refused, str) and refused in _EXECUTE_REFUSALS:
            return _EXECUTE_REFUSALS[refused]
        return "response_lost"
    if isinstance(refused, str) and refused in _READ_PHASE_REFUSALS:
        return _READ_PHASE_REFUSALS[refused]
    return "runner_unavailable"


class RunnerSnapshotReadError(RunnerError):
    """The publication snapshot could not be read or validated at its boundary."""


class RunnerStreamTimeout(TimeoutError):
    """The turn exceeded its deadline during transport or frame handling.

    A ``TimeoutError`` subclass on purpose (#2011): every existing
    ``except TimeoutError`` / ``except (aiohttp.ClientError, TimeoutError)``
    handler in the worker keeps catching this unchanged. What it adds is a
    non-empty ``str()`` -- ``str(TimeoutError())`` is the EMPTY STRING, which is
    how a real 600s cluster timeout reached the operator log as "turn stream
    dropped for <id>: " with nothing after the colon -- naming both the
    normalized underlying exception class and the budget that expired.
    """

    def __init__(self, message: str, timeout_result: TimeoutResult) -> None:
        super().__init__(message)
        self.timeout_result = timeout_result


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
    """An open ``/v1/event`` response; capacity turns await admission."""

    def __init__(
        self,
        response: aiohttp.ClientResponse,
        budget_s: float,
        deadline: float,
        turn_epoch: str | None,
        timeout_callback: Callable[[], Awaitable[TimeoutResult]] | None = None,
    ) -> None:
        self._response = response
        self.turn_epoch = turn_epoch
        self._saw_final = False
        # The streaming budget this stream is running under, carried from the
        # client so the stream can NAME the budget it blew (#2011).
        self._budget_s = budget_s
        self._deadline_timeout = asyncio.timeout_at(deadline)
        self._stream_timeout_error: RunnerStreamTimeout | None = None
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
            raise await self._normalize_stream_timeout(cause) from cause

    async def _normalize_stream_timeout(
        self, cause: BaseException
    ) -> RunnerStreamTimeout:
        terminal = self._stream_timeout_error
        if terminal is None:
            terminal = RunnerStreamTimeout(self._timeout_reason(cause), "unconfirmed")
            self._stream_timeout_error = terminal
            self._record_stream_timeout(cause)
            terminal.timeout_result = await self._notify_timeout()
        return terminal

    async def _notify_timeout(self) -> TimeoutResult:
        callback = self._timeout_callback
        self._timeout_callback = None
        if callback is None:
            return "unconfirmed"
        try:
            return await callback()
        except Exception as exc:  # noqa: BLE001 - preserve the causal body timeout
            # The epoch, bearer, and response body are deliberately absent. A
            # failed notification leaves abandonment as the runner's truthful
            # best-effort terminal, but must never replace RunnerStreamTimeout.
            logger.warning(
                "runner timeout terminal notification failed (%s)",
                type(exc).__name__,
            )
            return "unconfirmed"

    def _timeout_reason(self, cause: BaseException) -> str:
        return (
            f"runner turn stream exceeded its {self._budget_s}s total/sock-read budget "
            f"({type(cause).__name__})"
        )

    def _record_stream_timeout(self, cause: BaseException) -> None:
        """Emit the terminal record this boundary previously never produced.

        ``RunnerClient._rpc``'s span for ``start_turn`` closes as soon as the
        response HEADERS arrive, so a deadline expiring while the NDJSON BODY
        is read or while a yielded frame is handled left no evidence at the
        RPC boundary. The only ``curie.runner.rpc.result`` point for the turn
        said ``success`` (#2011).
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
        await self._deadline_timeout.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            try:
                await self._deadline_timeout.__aexit__(exc_type, exc, tb)
            except TimeoutError as cause:
                raise await self._normalize_stream_timeout(cause) from cause
            if exc_type is None:
                await self._discard_post_final()
        finally:
            self.close()


def _rejected_frame(endpoint: str, status: int, body: str) -> RunnerError:
    """The error for a runner that refused a turn-opening frame.

    Only the status and the body's length, never the body (MEMORY-TOKEN-3). A
    runner's validation response can repeat the frame it rejected, and the frame
    carries the turn's memory credential and its publication capability. An
    older runner repeats it as pydantic's ``str(ValidationError)``, which cuts
    the input in the middle, so no string replace of the whole credential can
    redact what is left of it. This error is logged."""

    return RunnerError(f"{endpoint} -> {status} (body: {len(body)} chars, not logged)")


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

    def turn_deadline_s(self, remaining_s: float | None) -> float:
        """How long a turn opened now may stream: ``_request_timeout``'s bound.

        ``min(total_timeout_s, remaining_s)``, or the ceiling when there is no
        budget in hand. ADR-0188's per-turn memory credential expires with it.
        """
        if remaining_s is None:
            return self._total_timeout_s
        return max(_MIN_REQUEST_TIMEOUT_S, min(self._total_timeout_s, remaining_s))

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
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
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
        capacity_admission: bool = False,
        progress: Mapping[str, str] | None = None,
    ) -> TurnStream:
        """Open a turn. Capacity turns need a separate post-lock grant.

        ``progress`` is the turn's deliberate progress capability (ADR 0130),
        sent as runner control headers beside the capacity admission one;
        ``curie_worker.turn_progress`` names and mints them.
        """
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
            assert stream_timeout_s is not None
            deadline = asyncio.get_running_loop().time() + stream_timeout_s
            request_headers = dict(headers or {})
            if capacity_admission:
                request_headers[_CAPACITY_ADMISSION_HEADER] = "wait"
            if progress:
                request_headers.update(progress)
            resp = await self._session.post(
                f"{base_url}/v1/event",
                json=event.model_dump(mode="json"),
                headers=request_headers or None,
                timeout=request_timeout,
            )
            if resp.status != 200:
                try:
                    body = await resp.text()
                finally:
                    resp.release()
                raise _rejected_frame("/v1/event", resp.status, body)
            turn_epoch = resp.headers.get(_TURN_EPOCH_HEADER)
            if capacity_admission and not _valid_turn_epoch(turn_epoch):
                resp.release()
                raise RunnerError("capacity turn response carried no valid epoch")
            timeout_callback: Callable[[], Awaitable[TimeoutResult]] | None = None
            if _valid_turn_epoch(turn_epoch):
                # ``turn_epoch`` is narrowed by the validation above. Keep the
                # callback response-bound so a retry can never inherit a stale
                # epoch from an earlier /v1/event.
                async def notify_timeout() -> TimeoutResult:
                    assert turn_epoch is not None
                    return await self._notify_timeout(base_url, turn_epoch, token)

                timeout_callback = notify_timeout
            return (
                TurnStream(resp, stream_timeout_s, deadline, turn_epoch, timeout_callback),
                "success",
            )

        return await self._rpc("event", token, request)

    async def _notify_timeout(
        self,
        base_url: str,
        turn_epoch: str,
        token: str | None,
    ) -> TimeoutResult:
        """Best-effort causal timeout notification for one accepted turn."""

        async def request(
            headers: dict[str, str] | None,
        ) -> tuple[TimeoutResult, str]:
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
                if resp.status == 409:
                    return "conflict", "conflict"
                return "accepted", "success"

        try:
            return await self._rpc("timeout", token, request)
        except Exception as exc:  # noqa: BLE001 - this control call is best effort
            logger.warning(
                "runner timeout terminal notification failed (%s)",
                type(exc).__name__,
            )
            return "unconfirmed"

    async def timeout_turn(
        self, base_url: str, turn_epoch: str, *, token: str | None = None
    ) -> TimeoutResult:
        """Stop only the runner turn identified by this private epoch."""

        return await self._notify_timeout(base_url, turn_epoch, token)

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
                json=event.model_dump(mode="json"),
                headers=headers,
                timeout=self._request_timeout(remaining_s),
            ) as resp:
                if resp.status == 409:
                    return False, "conflict"
                if resp.status != 200:
                    raise _rejected_frame("/v1/steer", resp.status, await resp.text())
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
                if resp.status >= 500:
                    raise RunnerSnapshotReadError(f"/v1/snapshot -> {resp.status}: {body}")
                raise RunnerError(f"/v1/snapshot -> {resp.status}: {body}")
            raw = bytearray()
            try:
                async for chunk in resp.content.iter_chunked(65_536):
                    raw.extend(chunk)
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
                raise RunnerSnapshotReadError(
                    "/v1/snapshot returned an invalid bounded payload "
                    f"after {len(raw)} bytes: {type(exc).__name__}: {str(exc)[:160]}"
                ) from exc

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

    async def capacity_status(
        self,
        base_url: str,
        *,
        epoch: str | None = None,
        token: str | None = None,
        remaining_s: float | None = None,
    ) -> dict[str, object]:
        """Read the private epoch bound to the runner's current turn lock."""

        async def request(headers: dict[str, str] | None) -> tuple[dict[str, object], str]:
            control_headers = dict(headers or {})
            if epoch is not None:
                if not _valid_turn_epoch(epoch):
                    raise RunnerError("invalid capacity turn epoch")
                control_headers[_TURN_EPOCH_HEADER] = epoch
            async with self._session.get(
                f"{base_url}/v1/status",
                headers=control_headers or None,
                timeout=self._request_timeout(remaining_s),
            ) as resp:
                if resp.status != 200:
                    raise RunnerError(f"/v1/status -> {resp.status}")
                data: dict[str, object] = await resp.json()
                return data, "success"

        return await self._rpc("status", token, request)

    async def admit_turn(
        self,
        base_url: str,
        turn_epoch: str,
        *,
        allow: bool,
        token: str | None = None,
        remaining_s: float | None = None,
    ) -> None:
        """Grant or deny one exact runner turn after its lock is held."""

        if not _valid_turn_epoch(turn_epoch):
            raise RunnerError("invalid capacity turn epoch")

        async def request(headers: dict[str, str] | None) -> tuple[None, str]:
            control_headers = dict(headers or {})
            control_headers[_TURN_EPOCH_HEADER] = turn_epoch
            async with self._session.post(
                f"{base_url}/v1/turn-admit",
                json={"allow": allow},
                headers=control_headers,
                timeout=self._request_timeout(remaining_s),
            ) as resp:
                if resp.status != 200:
                    raise RunnerError(f"/v1/turn-admit -> {resp.status}")
                return None, "success"

        await self._rpc("turn-admit", token, request)

    async def execute(
        self,
        base_url: str,
        request: Mapping[str, Any],
        *,
        token: str,
        remaining_s: float | None = None,
    ) -> dict[str, Any]:
        """Run one executor phase on an executor-mode runner. @spec ACTION-EXECUTOR-24.

        Posts ``request`` (exactly the ACTION-EXECUTOR-6 keys, plus ``pointer``
        on a ``read``, AUTOMATED-REMEDIATION-12) to
        ``/v1/execute`` with the per-sandbox bearer and returns the phase's
        response body. Every other ending raises ``ExecuteRefused`` with the
        code the worker reports; nothing about the arguments, grant or reply is
        logged or carried in the error.
        """

        if not token:
            raise RunnerError("/v1/execute requires a runner token")
        phase = request.get("phase")
        if phase not in EXECUTE_PHASES:
            raise RunnerError("/v1/execute request names an unknown phase")
        expected = EXECUTE_READ_KEYS if phase == "read" else EXECUTE_REQUEST_KEYS
        if set(request) != expected:
            raise RunnerError("/v1/execute request does not carry exactly the frozen keys")
        body = dict(request)

        async def send(headers: dict[str, str] | None) -> tuple[dict[str, Any], str]:
            try:
                async with self._session.post(
                    f"{base_url}{EXECUTE_PATH}",
                    json=body,
                    headers=headers,
                    timeout=self._request_timeout(remaining_s),
                ) as resp:
                    raw = await resp.content.read(_EXECUTE_BODY_MAX_BYTES + 1)
                    status = resp.status
            except (aiohttp.ClientError, TimeoutError) as exc:
                code = _execute_failure_code(phase, None, None)
                raise ExecuteRefused(code, f"transport {type(exc).__name__}") from exc
            parsed: object = None
            if len(raw) <= _EXECUTE_BODY_MAX_BYTES:
                try:
                    parsed = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    parsed = None
            if status != 200:
                refused = (
                    parsed.get(_EXECUTE_REFUSAL_BODY_KEY) if isinstance(parsed, dict) else None
                )
                code = _execute_failure_code(phase, status, refused)
                raise ExecuteRefused(code, f"HTTP {status}")
            if not isinstance(parsed, dict) or parsed.get("phase") != phase:
                # A 200 the worker cannot read: on ``call`` the write may have
                # happened, so it is lost, never refused.
                code = "response_lost" if phase == "call" else "runner_unavailable"
                raise ExecuteRefused(code, "unreadable phase response")
            return parsed, "success"

        return await self._rpc("execute", token, send)

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
