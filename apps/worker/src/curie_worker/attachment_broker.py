"""Private credential-owning attachment stream broker (ADR 0244).

The API issues a one-use ticket for one already registered reference. This
server accepts only the internal worker credential, resolves the current route,
and streams one bounded chunk at a time. No URL or credential reaches a runner.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator, Mapping
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web

from .attachments import (
    AgentRoute,
    AttachmentFilePort,
    AttachmentReadScope,
    ChannelPortFileClient,
)

HEADER = "X-Curie-Worker-Token"
CHUNK = 1024 * 1024
CONTROL_BYTES = 65536


def capture_read(call: Callable[..., Any], *args: Any) -> tuple[bool, Any]:
    """Keep provider exceptions inside the owned thread, including on cancel."""
    try:
        return True, call(*args)
    except Exception:  # noqa: BLE001 -- no provider exception escapes the broker
        return False, None


class AttachmentBroker:
    def __init__(
        self,
        *,
        internal_token: str,
        routes_for_agent: Callable[[uuid.UUID], Awaitable[list[AgentRoute]]],
        slack_files: Mapping[str, AttachmentFilePort],
        channel_files: ChannelPortFileClient | None,
        api_url: str,
        max_concurrent: int = 8,
        transfer_timeout_seconds: int = 300,
    ) -> None:
        if not internal_token or max_concurrent < 1 or transfer_timeout_seconds < 1:
            raise ValueError("attachment broker requires authentication and finite resource bounds")
        self.internal_token = internal_token
        self.routes_for_agent = routes_for_agent
        self.slack_files = dict(slack_files)
        self.channel_files = channel_files
        self.api_url = api_url.rstrip("/")
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.transfer_timeout_seconds = transfer_timeout_seconds

    def create_app(self) -> web.Application:
        app = web.Application(client_max_size=4096)
        app.router.add_post("/health", self.health)
        app.router.add_post("/fetch", self.fetch)
        return app

    def authenticate(self, request: web.Request) -> None:
        supplied = request.headers.get(HEADER, "")
        if not supplied or not hmac.compare_digest(supplied, self.internal_token):
            raise web.HTTPUnauthorized(text="invalid internal credential")

    async def health(self, request: web.Request) -> web.Response:
        self.authenticate(request)
        return web.json_response({"attachment_access": 1})

    async def control(
        self, client: ClientSession, path: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        async with client.post(
            self.api_url + "/v1/internal/attachment-access/" + path,
            json=body,
            headers={HEADER: self.internal_token},
            allow_redirects=False,
            timeout=ClientTimeout(total=5),
        ) as response:
            payload = bytearray()
            async for part in response.content.iter_chunked(8192):
                payload.extend(part)
                if len(payload) > CONTROL_BYTES:
                    raise ValueError("oversized attachment control response")
            if response.status != 200:
                raise ValueError("attachment control refused")
            parsed = json.loads(payload)
            if not isinstance(parsed, dict):
                raise ValueError("invalid attachment control response")
            return parsed

    async def file_port(self, reference: dict[str, Any]) -> AttachmentFilePort:
        routes = await self.routes_for_agent(uuid.UUID(reference["agent_id"]))
        kind, adapter, identity = (
            reference["route_kind"],
            reference["route_adapter"],
            reference["route_identity"],
        )
        for route in routes:
            if route.kind != kind:
                continue
            if kind == "slack" and (route.adapter or "default") == identity:
                port = self.slack_files.get(identity)
                if port is not None:
                    return port
            elif (
                kind != "slack"
                and route.adapter == adapter
                and adapter == identity
                and self.channel_files is not None
            ):
                return self.channel_files.bind_route(route.adapter, route.endpoint)
        raise ValueError("attachment binding unavailable")

    async def fetch(self, request: web.Request) -> web.StreamResponse:
        self.authenticate(request)
        try:
            body = await request.json()
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="invalid ticket request") from None
        if (
            not isinstance(body, dict)
            or set(body) != {"ticket"}
            or not isinstance(body["ticket"], str)
            or not 32 <= len(body["ticket"]) <= 128
        ):
            raise web.HTTPBadRequest(text="invalid ticket request")
        if self.semaphore.locked():
            raise web.HTTPTooManyRequests(text="attachment transfer capacity")
        async with self.semaphore:
            read_scope = AttachmentReadScope(
                deadline_epoch=time.time() + self.transfer_timeout_seconds
            )
            read_scope.__enter__()
            source: Iterator[bytes] | None = None
            pending: asyncio.Task[Any] | None = None
            response: web.StreamResponse | None = None
            try:
                async with ClientSession(trust_env=False) as client:
                    reference = await self.control(client, "redeem", body)
                    remaining = min(
                        self.transfer_timeout_seconds, reference["deadline"] - time.time()
                    )
                    read_scope.deadline_epoch = time.time() + remaining
                    if remaining <= 0:
                        raise ValueError("attachment transfer expired")
                    port = await self.file_port(reference)
                    # Cancellation retains this semaphore until the owned sync
                    # read returns (production socket timeout <=30s). Never
                    # launch another read while a canceled one is outstanding.
                    async with asyncio.timeout(remaining):
                        pending = asyncio.create_task(
                            asyncio.to_thread(capture_read, port.fetch, reference["file_id"])
                        )
                        ok, source = await asyncio.shield(pending)
                        pending = None
                        if not ok:
                            raise ValueError("attachment provider open failed")
                        assert source is not None
                        response = web.StreamResponse(
                            headers={
                                "Content-Type": "application/octet-stream",
                                "Cache-Control": "no-store",
                            }
                        )
                        await response.prepare(request)
                        hashed, total = hashlib.sha256(), 0
                        while True:
                            pending = asyncio.create_task(
                                asyncio.to_thread(capture_read, next, source, None)
                            )
                            ok, part = await asyncio.shield(pending)
                            pending = None
                            if not ok:
                                raise ValueError("attachment provider read failed")
                            if part is None:
                                break
                            # The standard clients already read CHUNK. Refuse an
                            # unbounded custom client instead of buffering copies.
                            if not isinstance(part, bytes) or len(part) > CHUNK:
                                raise ValueError("attachment transport chunk invalid")
                            hashed.update(part)
                            total += len(part)
                            await response.write(part)
                        result = await self.control(
                            client,
                            "complete",
                            {
                                "transfer_id": reference["transfer_id"],
                                "sha256": hashed.hexdigest(),
                                "size_bytes": total,
                            },
                        )
                        if (
                            result.get("status") != "complete"
                            or result.get("sha256") != hashed.hexdigest()
                            or result.get("size_bytes") != total
                        ):
                            raise ValueError("attachment completion unconfirmed")
                        await response.write_eof()
                        return response
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 -- sanitize provider errors at boundary
                # Neither provider errors nor callback URLs enter logs/results.
                if response is not None and response.prepared:
                    response.force_close()
                    if request.transport is not None:
                        request.transport.close()
                    return response
                raise web.HTTPBadGateway(text="attachment transfer refused") from None
            finally:
                read_scope.cancel()
                if pending is not None:
                    # A canceled coroutine cannot interrupt urllib's socket read.
                    # Join it before releasing the slot, then close its source.
                    while not pending.done():
                        with contextlib.suppress(asyncio.CancelledError, Exception):
                            await asyncio.shield(pending)
                    if not pending.cancelled():
                        with contextlib.suppress(Exception):
                            ok, settled = pending.result()
                            if source is None and ok:
                                source = settled
                close = getattr(source, "close", None)
                if close is not None:
                    with contextlib.suppress(Exception):
                        close()
                read_scope.__exit__()
