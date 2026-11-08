"""Worker replies cross the authenticated gateway before provider delivery."""

import json
from dataclasses import dataclass
from urllib.parse import quote

import httpx
from channel_protocol import ReplyAck, ReplyEvent
from pydantic import TypeAdapter, ValidationError

from .http_reply import GATEWAY_CLIENT_TIMEOUT_S
from .read_transport import GatewayError, GatewayUnavailableError, gateway_error, validate_endpoint
from .reply_delivery import UNREACHABLE_ACK_HEADER
from .reply_errors import (
    IndeterminateReplyDeliveryError,
    MissingAdapterCredentialError,
    ReplyPayloadTooLargeError,
    reply_error_from,
)
from .reply_route import ReplyRoute
from .reply_transport import MAX_ACK_BODY_BYTES, BestEffortUnreachableAck
from .request_replay import request_headers


@dataclass(frozen=True)
class GatewayReplyRoute:
    endpoint: str | None = None
    adapter: str | None = None


class GatewayReplySink:
    def __init__(
        self,
        endpoint: str,
        token: str,
        *,
        allow_insecure_http: bool = False,
        identities: tuple[str, ...] = ("default",),
        timeout_s: float = GATEWAY_CLIENT_TIMEOUT_S,
    ) -> None:
        validate_endpoint(endpoint, token, allow_insecure_http=allow_insecure_http)
        self._endpoint = endpoint.rstrip("/")
        self._token = token
        self._identities = identities
        self._http = httpx.AsyncClient(timeout=timeout_s, follow_redirects=False)

    async def aclose(self) -> None:
        await self._http.aclose()

    def edits_in_place(self, kind: str, route: ReplyRoute) -> bool:
        return kind == "slack"

    def undeliverable_reason(self, kind: str, route: ReplyRoute) -> str | None:
        identity = route.adapter or "default"
        return None if identity in self._identities else "gateway identity is unconfigured"

    async def emit(
        self, event: ReplyEvent, *, route: ReplyRoute, best_effort_unreachable: bool = False
    ) -> ReplyAck:
        identity = quote(route.adapter or "default", safe="")
        url = f"{self._endpoint}/v1/gateway/{identity}/reply"
        wire = event.model_dump_json().encode()
        if len(wire) > 4 * 1024 * 1024:
            raise ReplyPayloadTooLargeError("reply_payload_too_large")
        headers = request_headers(
            self._token,
            "POST",
            url,
            wire,
            best_effort_unreachable=best_effort_unreachable,
            provider_endpoint=route.endpoint,
        )
        headers["Content-Type"] = "application/json"
        try:
            async with self._http.stream(
                "POST",
                url,
                headers=headers,
                content=wire,
            ) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_ACK_BODY_BYTES:
                        raise GatewayError("gateway reply exceeded limit")
                    body.extend(chunk)
                if response.status_code != 200:
                    error = reply_error_from(response.status_code, bytes(body))
                    if error is not None:
                        raise error
                    raise gateway_error(response.status_code)
                ack = TypeAdapter(ReplyAck).validate_json(body)
                return (
                    BestEffortUnreachableAck(ref=ack.ref)
                    if response.headers.get(UNREACHABLE_ACK_HEADER) == "1"
                    else ack
                )
        except httpx.TimeoutException:
            raise IndeterminateReplyDeliveryError("reply_delivery_indeterminate") from None
        except httpx.HTTPError:
            raise GatewayUnavailableError("gateway unavailable") from None
        except (ValidationError, json.JSONDecodeError):
            raise GatewayError("gateway reply unavailable") from None


class UnconfiguredGatewayReplySink:
    """Refuse provider delivery when the application has no gateway configuration."""

    def edits_in_place(self, kind: str, route: ReplyRoute) -> bool:
        return kind == "slack"

    def undeliverable_reason(self, kind: str, route: ReplyRoute) -> str:
        return "channel gateway is unconfigured"

    async def emit(
        self, event: ReplyEvent, *, route: ReplyRoute, best_effort_unreachable: bool = False
    ) -> ReplyAck:
        raise MissingAdapterCredentialError("channel gateway is unconfigured")
