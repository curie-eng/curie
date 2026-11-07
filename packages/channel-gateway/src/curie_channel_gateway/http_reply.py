"""Compose a declared HTTP reply transport and its authenticated gateway receiver."""

import hmac
from collections.abc import Mapping

from aiohttp import web
from channel_protocol import ReplyEvent
from pydantic import TypeAdapter, ValidationError

from .ports.reply import DeclaredReplyTransport, ReplyCapabilities, ReplyEventTransport
from .reply_route import ReplyRoute
from .reply_transport import ADAPTER_SECRET_HEADER, HttpReplyAdapter

_EVENTS: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)


class HttpReplyTransport[Route: ReplyRoute](DeclaredReplyTransport[Route]):
    """Authenticated HTTP events with explicit gateway capability declarations."""

    def __init__(
        self,
        credentials: Mapping[str, str],
        declarations: Mapping[tuple[str, Route], ReplyCapabilities],
        *,
        timeout_s: float = 30.0,
    ) -> None:
        self._http = HttpReplyAdapter(credentials, timeout_s=timeout_s)
        super().__init__(self._http, declarations)

    async def aclose(self) -> None:
        await self._http.aclose()


def reply_application[Route](
    transport: ReplyEventTransport[Route], *, secret: str, route: Route
) -> web.Application:
    """Receive the existing reply wire under a server-selected provider route.

    The bearer authenticates the worker. Provider endpoints and credential
    selectors come from composition and cannot be supplied in a request body.
    This factory does not start a listener or introduce a deployment port.
    """
    if not secret.strip():
        raise ValueError("gateway reply secret must be non-blank")
    expected = secret.encode()

    async def receive(request: web.Request) -> web.Response:
        supplied = request.headers.get(ADAPTER_SECRET_HEADER, "").encode()
        if not hmac.compare_digest(supplied, expected):
            raise web.HTTPUnauthorized()
        try:
            event = _EVENTS.validate_json(await request.read())
        except ValidationError:
            raise web.HTTPBadRequest(text="invalid reply event") from None
        ack = await transport.emit(event, route=route, best_effort_unreachable=False)
        return web.json_response(ack.model_dump(mode="json"))

    application = web.Application()
    application.router.add_post("/reply", receive)
    return application
