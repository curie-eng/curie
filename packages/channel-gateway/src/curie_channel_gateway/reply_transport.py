"""Authenticated neutral HTTP replies and delivery observation."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from urllib.parse import urlsplit

import aiohttp
import curie_telemetry
from channel_protocol import ReplyAck, ReplyEvent, ReplyPost, TurnCompleted
from opentelemetry.trace import SpanKind, StatusCode

from .reply_errors import (
    MissingAdapterCredentialError,
    OversizedAdapterResponseError,
    RedirectedAdapterEndpointError,
    RejectedAdapterResponseError,
    reply_error_from,
)
from .reply_route import ReplyRoute

logger = logging.getLogger(__name__)
ADAPTER_SECRET_HEADER = "X-Curie-Adapter-Secret"


def redacted_endpoint(url: str | None) -> str:
    """Reduce endpoint diagnostics to scheme, host and port."""
    if not url:
        return "unset"
    parts = urlsplit(url)
    if not parts.scheme or not parts.hostname:
        return "an unparseable endpoint"
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{parts.hostname}{port}"


# The most acknowledgement body the worker will read off an adapter. The ack
# carries one optional ``ref`` string, so 64 KiB is orders of magnitude more than
# any honest adapter needs -- and the ceiling matters because the endpoint is
# operator-configured and answers on the worker's own heap: an unbounded read of
# a hostile (or merely broken) adapter's response is a worker OOM per turn, and
# the worker holds the kernel's locks. Oversize is a DELIVERY FAILURE, not a
# truncation: a body this large is not a reply the platform can interpret, and
# silently reading its first 64 KiB would hand ``reply_ref_from`` a half-parsed handle.
MAX_ACK_BODY_BYTES = 64 * 1024

# "Unreachable" for the HTTP path: the endpoint's HOST did not answer. A response
# that arrived and said no (any status) is NOT an unreachability -- it must stay
# loud even on a best-effort turn, or a rejecting adapter reads as a delivered one.
UNREACHABLE_REPLY_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ClientConnectionError,
    asyncio.TimeoutError,
)

REPLY_OBSERVATION_DEPTH: ContextVar[int] = ContextVar("curie_reply_observation_depth", default=0)
















class BestEffortUnreachableAck(ReplyAck):
    """Internal marker for an unreachable delivery deliberately acknowledged."""


async def observe_reply(
    event: ReplyEvent,
    *,
    best_effort_unreachable: bool,
    call: Callable[[], Awaitable[ReplyAck]],
) -> ReplyAck:
    """Observe one logical delivery, suppressing nested adapter double-counting."""

    operation = "post" if isinstance(event, ReplyPost) else "update"
    attributes = {
        "service.name": "curie-worker",
        "operation": operation,
        "role": "client",
    }
    started = time.monotonic()
    outcome = "failure"
    error: Exception | None = None
    ack: ReplyAck | None = None
    token = REPLY_OBSERVATION_DEPTH.set(REPLY_OBSERVATION_DEPTH.get() + 1)
    try:
        with curie_telemetry.operation_span(
            f"curie.reply.{operation}",
            kind=SpanKind.CLIENT,
            attributes=attributes,
        ) as span:
            try:
                ack = await call()
            except Exception as exc:  # noqa: BLE001 - existing broad catch retained
                error = exc
                if hasattr(span, "set_status"):
                    span.set_status(StatusCode.ERROR)
                span.add_event(
                    "reply.delivery.failed",
                    {"outcome": "failure", "error.class": type(exc).__name__},
                )
            else:
                outcome = (
                    "best-effort"
                    if isinstance(ack, BestEffortUnreachableAck)
                    else "success"
                )
                span.add_event("reply.delivered", {"outcome": outcome})
    finally:
        REPLY_OBSERVATION_DEPTH.reset(token)

    metric_attributes = {**attributes, "outcome": outcome}
    curie_telemetry.record_metric("curie.reply.delivery", attributes=metric_attributes)
    curie_telemetry.record_metric(
        f"curie.reply.{operation}.duration",
        max(0.0, time.monotonic() - started),
        attributes=metric_attributes,
    )
    if error is not None:
        raise error
    assert ack is not None
    return ack


class HttpReplyAdapter:
    """Delivers neutral JSON events to a binding's server-controlled endpoint.

    One POST per event, authenticated with the per-adapter secret. No transport
    fallback of any kind (E7): ``_with_transport_fallback``'s target is the
    worker's DEFAULT SLACK transport, so falling back here would post an email
    reply into a Slack workspace -- the exact hazard #530's issue text flagged.

    ``best_effort_unreachable`` (#708) is honored by logging and returning, with
    no fallback target, so an approval-resume turn whose adapter has died ACKs
    instead of dead-lettering. It covers an UNREACHABLE target only: a missing
    credential still raises, because fail-closed outranks best-effort.
    """

    def __init__(self, credentials: Mapping[str, str], *, timeout_s: float = 30.0) -> None:
        self._credentials = dict(credentials)
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        # ONE session for the adapter's whole life, mirroring the Slack sibling's
        # cached ``AsyncWebClient``: a session per emitted event throws away the
        # connection pool (and its keep-alive) on every streamed reply.update, so
        # a chatty turn pays a fresh TCP + TLS handshake per edit against the same
        # endpoint. Built lazily because a ``ClientSession`` binds the running
        # loop at construction and the adapter is wired at import-time in ``run``.
        self._session: aiohttp.ClientSession | None = None

    def _ensure_session(self) -> aiohttp.ClientSession:
        """The cached session, created on first use.

        No lock: this is asyncio, and there is no ``await`` between the check and
        the assignment, so two concurrent turns cannot both create one.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def aclose(self) -> None:
        """Release the cached session. Idempotent, and safe when none was built."""
        session, self._session = self._session, None
        if session is not None and not session.closed:
            await session.close()

    def credentials_for(self, route: ReplyRoute) -> tuple[str, str]:
        """The (endpoint, secret) pair, or raise having sent nothing."""
        if not route.endpoint:
            # E17: a non-Slack binding with no endpoint is an operator error.
            raise MissingAdapterCredentialError(
                "platform egress has no endpoint for this turn; refusing to deliver"
            )
        if not route.adapter:
            raise MissingAdapterCredentialError(
                f"platform egress to {redacted_endpoint(route.endpoint)} has no adapter identity; "
                "refusing to send an unauthenticated request"
            )
        secret = self._credentials.get(route.adapter)
        if not secret:
            raise MissingAdapterCredentialError(
                f"no egress credential configured for adapter {route.adapter!r}; "
                "refusing to send an unauthenticated request"
            )
        return route.endpoint, secret

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: ReplyRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        if not REPLY_OBSERVATION_DEPTH.get():
            return await observe_reply(
                event,
                best_effort_unreachable=best_effort_unreachable,
                call=lambda: self._emit(
                    event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                ),
            )
        return await self._emit(
            event,
            route=route,
            best_effort_unreachable=best_effort_unreachable,
        )

    async def _emit(
        self,
        event: ReplyEvent,
        *,
        route: ReplyRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        endpoint, secret = self.credentials_for(route)
        body = event.model_dump_json()
        headers = {"Content-Type": "application/json", ADAPTER_SECRET_HEADER: secret}
        session = self._ensure_session()
        try:
            async with session.post(
                endpoint, data=body, headers=headers, allow_redirects=False
            ) as response:
                if 300 <= response.status < 400:
                    raise RedirectedAdapterEndpointError(
                        f"adapter endpoint {redacted_endpoint(endpoint)} answered "
                        f"{response.status} (redirect); refusing to re-send the "
                        "egress credential to the redirect target"
                    )
                status = response.status
                payload = await read_capped_ack(response, endpoint)
        except UNREACHABLE_REPLY_ERRORS as exc:
            if best_effort_unreachable:
                logger.warning(
                    "%s: adapter endpoint %s is unreachable (%s); completing the turn "
                    "best-effort without delivering the event",
                    event.event, redacted_endpoint(endpoint), exc,
                )
                return BestEffortUnreachableAck(ref=None)
            raise
        if status >= 400:
            error = reply_error_from(
                status, payload, provider_outcomes=isinstance(event, TurnCompleted)
            )
            if error is not None:
                raise error
            raise RejectedAdapterResponseError(
                f"adapter endpoint {redacted_endpoint(endpoint)} answered "
                f"{status}; the delivery failed"
            )
        return ReplyAck(ref=reply_ref_from(payload))


async def read_capped_ack(response: aiohttp.ClientResponse, endpoint: str) -> bytes:
    """The acknowledgement body, or refuse it once it passes the cap.

    A LOOP, not one sized read: ``StreamReader.read(n)`` returns whatever has
    arrived so far, so a chunked response answers a short first buffer and a
    single read would let a body of any size through behind it. The running
    total is checked per chunk, so the refusal happens on the chunk that crosses
    the cap and the process never holds more than the cap plus one chunk.
    """
    chunks: list[bytes] = []
    total = 0
    async for chunk in response.content.iter_any():
        total += len(chunk)
        if total > MAX_ACK_BODY_BYTES:
            raise OversizedAdapterResponseError(
                f"adapter endpoint {redacted_endpoint(endpoint)} answered with more than "
                f"{MAX_ACK_BODY_BYTES} bytes; refusing to buffer it and treating "
                "the delivery as failed"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def reply_ref_from(payload: bytes) -> str | None:
    """The adapter-minted handle off its response, if it minted one.

    A channel with nothing addressable to hand back (email has no editable
    message) answers with no ``ref`` at all, and the kernel must not care.
    """
    try:
        decoded = json.loads(payload)
    except ValueError:
        return None
    if not isinstance(decoded, dict):
        return None
    ref = decoded.get("ref")
    return str(ref) if isinstance(ref, str) and ref else None
