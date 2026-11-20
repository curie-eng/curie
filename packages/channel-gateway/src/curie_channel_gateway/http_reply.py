"""Authenticated, replay-safe HTTP replies with durable delivery claims.

Slack per-turn endpoints and their transport fallback are outside this boundary.
A Slack gateway uses its configured provider origin and refuses endpoint overrides.
Composition must persist state_path across restarts, including unconfirmed claims.
"""

import asyncio
import hashlib
import hmac
import json
import math
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web
from channel_protocol import ReplyAck, ReplyEvent
from pydantic import TypeAdapter, ValidationError

from .ports.reply import DeclaredReplyTransport, ReplyCapabilities, ReplyEventTransport
from .reply_delivery import (
    BEST_EFFORT_HEADER,
    NONCE_HEADER,
    REQUEST_KEY_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    UNREACHABLE_ACK_HEADER,
    DeliveryResult,
    ReplyDeliveryStore,
    signed_reply_headers,
    valid_reply_signature,
)
from .reply_errors import (
    IndeterminateReplyDeliveryError,
    InvalidReplyTargetError,
    ProviderReplyUnreachableError,
    RedirectedAdapterEndpointError,
    RejectedAdapterResponseError,
    ReplyPayloadTooLargeError,
    reply_error_from,
    reply_error_response,
)
from .reply_route import ReplyRoute
from .reply_transport import (
    ADAPTER_SECRET_HEADER,
    MAX_ACK_BODY_BYTES,
    UNREACHABLE_REPLY_ERRORS,
    BestEffortUnreachableAck,
    HttpReplyAdapter,
    read_capped_ack,
    redacted_endpoint,
)

_EVENTS: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)
GATEWAY_DELIVERY_TIMEOUT_S = 120.0
GATEWAY_CLIENT_TIMEOUT_S = 180.0
MAX_REPLY_BODY_BYTES = 64 * 1024 * 1024


class _GatewayReplyClient(HttpReplyAdapter):
    """Send a fresh proof on every attempt while retaining the delivery key."""

    async def _emit(
        self, event: ReplyEvent, *, route: ReplyRoute, best_effort_unreachable: bool = False
    ) -> ReplyAck:
        endpoint, secret = self.credentials_for(route)
        body = event.model_dump_json().encode()
        if len(body) > MAX_REPLY_BODY_BYTES:
            raise ReplyPayloadTooLargeError("reply_payload_too_large")
        parsed = urlsplit(endpoint)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        headers = signed_reply_headers(
            secret, path=path, body=body, best_effort_unreachable=best_effort_unreachable
        )
        try:
            async with self._ensure_session().post(
                endpoint, data=body, headers=headers, allow_redirects=False
            ) as response:
                if 300 <= response.status < 400:
                    raise RedirectedAdapterEndpointError("redirected_adapter_endpoint")
                payload = await read_capped_ack(response, endpoint)
                status = response.status
                unreachable = response.headers.get(UNREACHABLE_ACK_HEADER) == "1"
        except UNREACHABLE_REPLY_ERRORS:
            if best_effort_unreachable:
                return BestEffortUnreachableAck(ref=None)
            raise
        if status >= 400:
            error = reply_error_from(status, payload)
            if error is not None:
                raise error
            raise RejectedAdapterResponseError(
                f"gateway endpoint {redacted_endpoint(endpoint)} answered {status}"
            )
        try:
            ack = ReplyAck.model_validate_json(payload)
        except ValidationError:
            raise RejectedAdapterResponseError("invalid gateway acknowledgement") from None
        return BestEffortUnreachableAck(ref=ack.ref) if unreachable else ack


class HttpReplyTransport[Route: ReplyRoute](DeclaredReplyTransport[Route]):
    """A TLS-only gateway transport with explicit provider capabilities."""

    def __init__(
        self,
        credentials: Mapping[str, str],
        declarations: Mapping[tuple[str, Route], ReplyCapabilities],
        *,
        timeout_s: float = GATEWAY_CLIENT_TIMEOUT_S,
        allow_insecure_http: bool = False,
    ) -> None:
        if not math.isfinite(timeout_s) or timeout_s < GATEWAY_DELIVERY_TIMEOUT_S + 30:
            raise ValueError("gateway client timeout must exceed the gateway delivery budget")
        for _, route in declarations:
            parsed = urlsplit(route.endpoint or "")
            if (
                not parsed.hostname
                or parsed.username is not None
                or parsed.fragment
                or parsed.scheme != "https"
                and not (allow_insecure_http and parsed.scheme == "http")
            ):
                raise ValueError(
                    "gateway reply endpoints require HTTPS; HTTP needs explicit opt-out"
                )
            if not parsed.path.endswith("/reply"):
                raise InvalidReplyTargetError(
                    "gateway endpoints must end in /reply; Slack per-turn endpoints are unsupported"
                )
        self._http = _GatewayReplyClient(credentials, timeout_s=timeout_s)
        super().__init__(self._http, declarations)

    async def aclose(self) -> None:
        await self._http.aclose()


def reply_application[Route: ReplyRoute](
    transport: ReplyEventTransport[Route],
    *,
    secret: str,
    route: Route,
    kind: str,
    state_path: Path,
    max_body_bytes: int = MAX_REPLY_BODY_BYTES,
    delivery_timeout_s: float = GATEWAY_DELIVERY_TIMEOUT_S,
) -> web.Application:
    """Receive the unchanged event wire under a server-selected provider route.

    A fresh timestamp and nonce authenticate each attempt, including retries.
    The persistent request claim prevents re-posting after a timeout or restart.
    Unconfirmed claims fail closed; an operator must reconcile their outcome.
    """
    if not secret.strip():
        raise ValueError("gateway reply secret must be non-blank")
    if not kind or max_body_bytes <= 0 or not 0 < delivery_timeout_s <= GATEWAY_DELIVERY_TIMEOUT_S:
        raise ValueError("invalid gateway reply limits or kind")
    if kind == "slack" and route.endpoint is not None:
        raise InvalidReplyTargetError("Slack per-turn endpoints are unsupported over gateway HTTP")
    store = ReplyDeliveryStore(state_path)
    active: dict[str, asyncio.Task[DeliveryResult]] = {}

    def failure(error: Exception) -> DeliveryResult:
        status, detail = reply_error_response(error)
        return DeliveryResult(status, json.dumps({"detail": detail}).encode())

    def response(result: DeliveryResult) -> web.Response:
        return web.Response(
            body=result.body,
            status=result.status,
            content_type="application/json",
            headers={UNREACHABLE_ACK_HEADER: "1" if result.unreachable else "0"},
        )

    async def deliver(key: str, event: ReplyEvent, best: bool) -> DeliveryResult:
        try:
            async with asyncio.timeout(delivery_timeout_s):
                ack = await transport.emit(event, route=route, best_effort_unreachable=best)
            body = ack.model_dump_json().encode()
            if len(body) > MAX_ACK_BODY_BYTES:
                from .reply_errors import OversizedAdapterResponseError

                raise OversizedAdapterResponseError("oversized_adapter_response")
            result = DeliveryResult(200, body, isinstance(ack, BestEffortUnreachableAck))
        except (TimeoutError, asyncio.CancelledError):
            result = failure(IndeterminateReplyDeliveryError("reply_delivery_indeterminate"))
        except UNREACHABLE_REPLY_ERRORS:
            result = failure(ProviderReplyUnreachableError("reply_provider_unreachable"))
        except Exception as exc:  # noqa: BLE001 - fixed closed wire diagnostics
            result = failure(exc)
        store.finish(key, result)
        return result

    async def receive(request: web.Request) -> web.Response:
        supplied = request.headers.get(ADAPTER_SECRET_HEADER, "").encode()
        if not hmac.compare_digest(supplied, secret.encode()):
            raise web.HTTPUnauthorized()
        try:
            body = await request.read()
        except web.HTTPRequestEntityTooLarge:
            return response(failure(ReplyPayloadTooLargeError("reply_payload_too_large")))
        stamp = request.headers.get(TIMESTAMP_HEADER, "")
        nonce = request.headers.get(NONCE_HEADER, "")
        key = request.headers.get(REQUEST_KEY_HEADER, "")
        best = request.headers.get(BEST_EFFORT_HEADER, "")
        path = request.rel_url.raw_path_qs
        if not valid_reply_signature(
            secret,
            path=path,
            body=body,
            timestamp=stamp,
            nonce=nonce,
            request_key=key,
            best_effort=best,
            signature=request.headers.get(SIGNATURE_HEADER, ""),
        ):
            raise web.HTTPUnauthorized()
        scope = hmac.new(secret.encode(), path.encode(), hashlib.sha256).hexdigest()
        key = scope + ":" + key
        try:
            store.consume_nonce(scope + ":" + nonce, int(stamp))
            event = _EVENTS.validate_json(body)
            if event.target.kind != kind:
                raise InvalidReplyTargetError("invalid_reply_target")
            claimed, result = store.claim(key, hashlib.sha256(best.encode() + body).hexdigest())
        except ValidationError:
            return response(failure(InvalidReplyTargetError("invalid_reply_target")))
        except (ValueError, RuntimeError) as exc:
            return response(failure(exc))
        if result is not None:
            return response(result)
        if claimed:
            task = asyncio.create_task(deliver(key, event, best == "1"))
            active[key] = task
            task.add_done_callback(lambda _task: active.pop(key, None))
        elif key not in active:
            return response(
                failure(IndeterminateReplyDeliveryError("reply_delivery_indeterminate"))
            )
        return response(await asyncio.shield(active[key]))

    async def cleanup(application: web.Application) -> None:
        if active:
            await asyncio.gather(*active.values(), return_exceptions=True)
        store.close()

    application = web.Application(client_max_size=max_body_bytes)
    application.router.add_post("/reply", receive)
    application.on_cleanup.append(cleanup)
    return application
