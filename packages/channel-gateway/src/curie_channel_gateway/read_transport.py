"""Authenticated HTTP composition for the frozen provider read ports."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import sqlite3
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from urllib.parse import quote, urlsplit

import httpx
from aci_protocol.turn import SLACK_KIND, slack_speaking_identity
from aiohttp import web
from channel_protocol import ChannelCapability, ReplyEvent
from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from .group_models import UserGroupLookupError, UserGroupMembership
from .http_reply import GATEWAY_DELIVERY_TIMEOUT_S
from .ports.groups import GroupMembershipSource
from .ports.reads import CanvasReader, ChannelReader, FileSource, SenderIdentities
from .ports.reply import ReplyEventTransport
from .read_errors import ChannelReadRefused
from .read_models import (
    BindingRoute,
    CanvasDocument,
    CanvasFile,
    CanvasPage,
    ChannelMessage,
    ProviderPage,
    ResolvedWindow,
)
from .reply_delivery import (
    UNREACHABLE_ACK_HEADER,
    DeliveryResult,
    ReplyDeliveryStore,
    reply_request_key,
)
from .reply_errors import (
    IndeterminateReplyDeliveryError,
    InvalidReplyTargetError,
    OversizedAdapterResponseError,
    reply_error_response,
)
from .reply_route import ReplyRoute
from .reply_transport import (
    ADAPTER_SECRET_HEADER,
    MAX_ACK_BODY_BYTES,
    UNREACHABLE_REPLY_ERRORS,
    BestEffortUnreachableAck,
)
from .request_replay import (
    BEST_EFFORT_HEADER,
    PROVIDER_ENDPOINT_HEADER,
    ReplayCapacityError,
    ReplayRefusedError,
    RequestReplayStore,
    request_headers,
)
from .slack.files import FileResponse, SlackFileError

MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_RESULT_BYTES = 4 * 1024 * 1024
_API_OPERATIONS = frozenset(
    {
        "members",
        "fresh_members",
        "history",
        "thread",
        "message",
        "locate",
        "is_member",
        "list",
        "document",
        "replace_cell",
    }
)
_WORKER_OPERATIONS = frozenset({"identity_of", "fetch", "reply"})


class GatewayError(RuntimeError):
    """A sanitized transport failure; request and provider data never appear."""


class GatewayAuthenticationError(GatewayError):
    pass


class GatewayReplayError(GatewayError):
    pass


class GatewayLimitError(GatewayError):
    pass


class GatewayUnavailableError(GatewayError):
    pass


class GatewayTimeoutError(GatewayError):
    pass


def gateway_error(status: int) -> GatewayError:
    if status in (401, 403):
        return GatewayAuthenticationError("gateway authentication refused")
    if status == 409:
        return GatewayReplayError("gateway replay refused")
    if status == 413:
        return GatewayLimitError("gateway size limit exceeded")
    if status == 504:
        return GatewayTimeoutError("gateway outcome unknown")
    if status in (502, 503):
        return GatewayUnavailableError("gateway unavailable")
    return GatewayError("gateway request refused")


def validate_endpoint(endpoint: str, token: str, *, allow_insecure_http: bool = False) -> None:
    parts = urlsplit(endpoint)
    if (
        not token.strip()
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
        or parts.scheme not in ({"https", "http"} if allow_insecure_http else {"https"})
    ):
        raise ValueError("gateway requires a credential and a TLS endpoint")


class _Parameters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    group_id: str | None = None
    channel: str | None = None
    window: ResolvedWindow | None = None
    limit: int | None = None
    cursor: str | None = None
    boundary: int | None = None
    thread_id: str | None = None
    message_id: str | None = None
    canvas_id: str | None = None
    section_id: str | None = None
    text: str | None = None
    file: CanvasFile | None = None
    author: str | None = None


@dataclass(frozen=True)
class GatewayProviders:
    channel: ChannelReader[BindingRoute, ResolvedWindow, ProviderPage, ChannelMessage] | None = None
    canvas: CanvasReader[BindingRoute, CanvasFile, CanvasPage, CanvasDocument] | None = None
    senders: SenderIdentities | None = None


def gateway_application(
    *,
    tokens: Mapping[str, str],
    replay: RequestReplayStore,
    groups: Mapping[str, GroupMembershipSource[UserGroupMembership]] | None = None,
    files: Mapping[str, FileSource] | None = None,
    providers: GatewayProviders | None = None,
    allow_insecure_http: bool = False,
    max_file_bytes: int = MAX_FILE_BYTES,
    reply: ReplyEventTransport[ReplyRoute] | None = None,
    reply_state: ReplyDeliveryStore | None = None,
) -> web.Application:
    """Per-caller authentication and bounded provider operations, composed by the gateway."""
    if (
        set(tokens) != {"api", "worker"}
        or any(not t.strip() for t in tokens.values())
        or len(set(tokens.values())) != len(tokens)
        or max_file_bytes <= 0
    ):
        raise ValueError("gateway requires distinct non-blank caller credentials and a file cap")
    if reply is not None and reply_state is None:
        raise ValueError("gateway replies require a persistent delivery ledger")
    file_capacity = asyncio.BoundedSemaphore(1)
    active_replies: dict[str, asyncio.Task[DeliveryResult]] = {}
    expected = {name: value.encode() for name, value in tokens.items()}
    ports = providers or GatewayProviders()

    @web.middleware
    async def authenticate(
        request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
    ) -> web.StreamResponse:
        if not allow_insecure_http and not request.secure:
            return web.json_response({"error": "tls_required"}, status=403)
        supplied = request.headers.get(ADAPTER_SECRET_HEADER, "").encode()
        caller = None
        for name, value in expected.items():
            if hmac.compare_digest(supplied, value):
                caller = name
        if caller is None:
            return web.json_response({"error": "authentication_refused"}, status=401)
        operation = request.match_info.get("operation", "")
        allowed = _API_OPERATIONS if caller == "api" else _WORKER_OPERATIONS
        if operation not in allowed:
            return web.json_response({"error": "operation_refused"}, status=403)
        try:
            replay.consume(
                caller,
                tokens[caller],
                request.method,
                request.raw_path,
                await request.read(),
                request.headers,
            )
        except web.HTTPRequestEntityTooLarge:
            return web.json_response({"error": "request_size_exceeded"}, status=413)
        except ReplayRefusedError:
            return web.json_response({"error": "request_replay_refused"}, status=409)
        except (ReplayCapacityError, sqlite3.Error):
            return web.json_response({"error": "request_capacity_exhausted"}, status=503)
        try:
            return await handler(request)
        except web.HTTPRequestEntityTooLarge:
            return web.json_response({"error": "request_size_exceeded"}, status=413)

    def reply_failure(error: Exception) -> DeliveryResult:
        status, detail = reply_error_response(error)
        return DeliveryResult(status, json.dumps({"detail": detail}).encode())

    def reply_response(result: DeliveryResult) -> web.Response:
        return web.Response(
            body=result.body,
            status=result.status,
            content_type="application/json",
            headers={UNREACHABLE_ACK_HEADER: "1" if result.unreachable else "0"},
        )

    async def deliver_reply(
        key: str, event: ReplyEvent, route: ReplyRoute, best: bool
    ) -> DeliveryResult:
        assert reply is not None and reply_state is not None
        try:
            async with asyncio.timeout(GATEWAY_DELIVERY_TIMEOUT_S):
                ack = await reply.emit(event, route=route, best_effort_unreachable=best)
            body = ack.model_dump_json().encode()
            if len(body) > MAX_ACK_BODY_BYTES:
                raise OversizedAdapterResponseError("oversized_adapter_response")
            result = DeliveryResult(200, body, isinstance(ack, BestEffortUnreachableAck))
        except (TimeoutError, asyncio.CancelledError):
            result = reply_failure(IndeterminateReplyDeliveryError("reply_delivery_indeterminate"))
        except UNREACHABLE_REPLY_ERRORS:
            from .reply_errors import ProviderReplyUnreachableError

            result = reply_failure(ProviderReplyUnreachableError("reply_provider_unreachable"))
        except Exception as error:  # noqa: BLE001 - closed diagnostics from shared reply composition
            result = reply_failure(error)
        reply_state.finish(key, result)
        return result

    async def receive_reply(request: web.Request) -> web.Response:
        from .gateway_reply import GatewayReplyRoute

        if reply is None or reply_state is None:
            return web.json_response({"error": "reply_unavailable"}, status=503)
        body = await request.read()
        try:
            event: ReplyEvent = TypeAdapter(ReplyEvent).validate_json(body)
            if event.target.kind != "slack":
                raise InvalidReplyTargetError("invalid_reply_target")
            route = GatewayReplyRoute(
                endpoint=request.headers.get(PROVIDER_ENDPOINT_HEADER),
                adapter=request.match_info["identity"],
            )
        except (ValidationError, ValueError):
            return reply_response(reply_failure(InvalidReplyTargetError("invalid_reply_target")))
        best = request.headers.get(BEST_EFFORT_HEADER) == "true"
        scope = hashlib.sha256(json.dumps((route.adapter, route.endpoint)).encode()).hexdigest()
        key = scope + ":" + reply_request_key(body, best)
        try:
            claimed, result = reply_state.claim(key, hashlib.sha256(body).hexdigest())
        except (ValueError, RuntimeError) as error:
            return reply_response(reply_failure(error))
        if result is not None:
            return reply_response(result)
        if claimed:
            task = asyncio.create_task(deliver_reply(key, event, route, best))
            active_replies[key] = task
            task.add_done_callback(lambda _task: active_replies.pop(key, None))
        elif key not in active_replies:
            return reply_response(
                reply_failure(IndeterminateReplyDeliveryError("reply_delivery_indeterminate"))
            )
        return reply_response(await asyncio.shield(active_replies[key]))

    async def receive(request: web.Request) -> web.Response:
        identity = request.match_info["identity"]
        operation = request.match_info["operation"]
        if operation == "reply":
            return await receive_reply(request)
        result: Any
        adapter: TypeAdapter[Any]
        try:
            params = _Parameters.model_validate_json(await request.read())
            arguments = params.model_dump(exclude_unset=True)
            # Preserve typed dataclasses when calling the frozen ports.
            if params.window is not None:
                arguments["window"] = params.window
            if params.file is not None:
                arguments["file"] = params.file
            if operation in {"members", "fresh_members"}:
                if set(arguments) != {"group_id"} or params.group_id is None:
                    raise ValueError
                source = (groups or {}).get(identity)
                if source is None:
                    raise UserGroupLookupError("membership unavailable")
                result = await (
                    source.fresh_members(params.group_id)
                    if operation == "fresh_members"
                    else source.members(params.group_id)
                )
                membership_body = TypeAdapter(UserGroupMembership).dump_json(result)
                if len(membership_body) > MAX_RESULT_BYTES:
                    return web.json_response({"error": "response_size_exceeded"}, status=413)
                return web.Response(body=membership_body, content_type="application/json")
            if operation == "identity_of":
                if set(arguments) != {"author"} or params.author is None:
                    raise ValueError
                result = await ports.senders.identity_of(params.author) if ports.senders else None
                return web.json_response(result)
            if operation in {"history", "thread", "message"}:
                if ports.channel is None:
                    raise GatewayError
                if operation == "history":
                    result = await ports.channel.history(identity=identity, **arguments)
                elif operation == "thread":
                    result = await ports.channel.thread(identity=identity, **arguments)
                else:
                    result = await ports.channel.message(identity=identity, **arguments)
                adapter = TypeAdapter(
                    ProviderPage if operation != "message" else ChannelMessage | None
                )
            elif operation in {"locate", "is_member", "list", "document", "replace_cell"}:
                if ports.canvas is None:
                    raise GatewayError
                if operation == "locate":
                    result = await ports.canvas.locate(identity=identity, **arguments)
                    adapter = TypeAdapter(CanvasFile | None)
                elif operation == "is_member":
                    result = await ports.canvas.is_member(identity=identity, **arguments)
                    adapter = TypeAdapter(bool)
                elif operation == "list":
                    result = await ports.canvas.list(identity=identity, **arguments)
                    adapter = TypeAdapter(CanvasPage)
                elif operation == "document":
                    result = await ports.canvas.document(identity=identity, **arguments)
                    adapter = TypeAdapter(CanvasDocument)
                else:
                    await ports.canvas.replace_cell(identity=identity, **arguments)
                    return web.json_response(None)
            else:
                raise ValueError
            body = adapter.dump_json(result)
            if len(body) > MAX_RESULT_BYTES:
                raise GatewayError
            return web.Response(body=body, content_type="application/json")
        except (ValidationError, ValueError, TypeError):
            raise web.HTTPBadRequest(text="invalid gateway request") from None
        except ChannelReadRefused as exc:
            # Only the bounded refusal vocabulary crosses the transport.
            return web.json_response(
                {"code": exc.code, "retry_after": exc.retry_after}, status=exc.status
            )
        except (UserGroupLookupError, GatewayError, httpx.HTTPError):
            raise web.HTTPBadGateway(text="provider unavailable") from None
        except Exception:  # noqa: BLE001 - provider failures must never echo data
            return web.json_response({"error": "provider_unavailable"}, status=502)

    def fetch_bytes(identity: str, file_id: str) -> bytes:
        source = (files or {}).get(identity)
        if source is None:
            raise GatewayError
        chunks = source.fetch(file_id)
        try:
            body = bytearray()
            for chunk in chunks:
                if len(body) + len(chunk) > max_file_bytes:
                    raise GatewayLimitError("file size exceeded")
                body.extend(chunk)
            return bytes(body)
        finally:
            close = getattr(chunks, "close", None)
            if close is not None:
                close()

    async def fetch(request: web.Request) -> web.StreamResponse:
        if file_capacity.locked():
            return web.json_response({"error": "file_capacity_exhausted"}, status=503)
        async with file_capacity:
            download = asyncio.create_task(
                asyncio.to_thread(
                    fetch_bytes, request.match_info["identity"], request.match_info["file_id"]
                )
            )
            try:
                body = await asyncio.shield(download)
            except asyncio.CancelledError:
                # The provider thread retains its buffer until it finishes.
                await asyncio.gather(download, return_exceptions=True)
                raise
            except GatewayLimitError:
                return web.json_response({"error": "file_size_exceeded"}, status=413)
            except SlackFileError as error:
                status = (
                    error.status if error.status is not None and 400 <= error.status < 600 else 422
                )
                return web.json_response({"error": "provider_file_refused"}, status=status)
            except Exception:  # noqa: BLE001 - provider failures must never echo input
                raise web.HTTPBadGateway(text="file unavailable") from None
            response = web.StreamResponse(headers={"Content-Type": "application/octet-stream"})
            response.content_length = len(body)
            await response.prepare(request)
            await response.write(body)
            await response.write_eof()
            return response

    async def close_replay(_app: web.Application) -> None:
        if active_replies:
            await asyncio.gather(*active_replies.values(), return_exceptions=True)
        if reply_state is not None:
            reply_state.close()
        replay.close()

    app = web.Application(middlewares=[authenticate], client_max_size=MAX_RESULT_BYTES)
    app.on_cleanup.append(close_replay)
    app.router.add_post("/v1/gateway/{identity}/{operation}", receive)
    app.router.add_get("/v1/gateway/{identity}/{operation:fetch}/{file_id}", fetch)
    return app


class GatewayClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        endpoint: str,
        token: str,
        *,
        allow_insecure_http: bool = False,
        identities: tuple[str, ...] = ("default",),
    ) -> None:
        validate_endpoint(endpoint, token, allow_insecure_http=allow_insecure_http)
        self.http = http
        self.endpoint = endpoint.rstrip("/")
        self._token = token
        self.identities = identities
        self._senders: dict[str, str] = {}
        self._sender_tasks: dict[str, asyncio.Task[None]] = {}

    async def call[T](
        self, identity: str, operation: str, params: Mapping[str, Any], result: TypeAdapter[T]
    ) -> T:
        url = f"{self.endpoint}/v1/gateway/{quote(identity, safe='')}/{operation}"
        body_bytes = TypeAdapter(dict[str, Any]).dump_json(dict(params))
        try:
            async with self.http.stream(
                "POST",
                url,
                headers=request_headers(self._token, "POST", url, body_bytes),
                content=body_bytes,
                follow_redirects=False,
            ) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_RESULT_BYTES:
                        raise GatewayLimitError("gateway response exceeded limit")
                    body.extend(chunk)
                if response.status_code != 200:
                    if response.status_code in (403, 404, 409, 429, 502, 503):
                        try:
                            refusal = json.loads(body)
                            code = refusal["code"]
                            if isinstance(code, str) and code.startswith("channel_read."):
                                raise ChannelReadRefused(
                                    response.status_code,
                                    code.removeprefix("channel_read."),
                                    "provider refused the gateway operation",
                                    retry_after=refusal.get("retry_after"),
                                )
                        except (ValueError, KeyError, TypeError):
                            pass
                    raise gateway_error(response.status_code)
                return result.validate_json(body)
        except (httpx.HTTPError, ValidationError):
            raise GatewayUnavailableError("gateway unavailable") from None

    def groups(self, identity: str) -> RemoteGroups:
        return RemoteGroups(self, identity)

    async def identity_of(self, author: str) -> str | None:
        if author not in self._senders and author not in self._sender_tasks:
            self._sender_tasks[author] = asyncio.create_task(self._lookup_sender(author))
        return self._senders.get(author)

    async def _lookup_sender(self, author: str) -> None:
        try:
            identity: str | None = await self.call(
                "default", "identity_of", {"author": author}, TypeAdapter(str | None)
            )
            if identity is not None:
                self._senders[author] = identity
        except GatewayError:
            pass
        finally:
            self._sender_tasks.pop(author, None)

    async def aclose(self) -> None:
        tasks = tuple(self._sender_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class RemoteGroups:
    def __init__(self, client: GatewayClient, identity: str) -> None:
        self.client = client
        self.identity = identity

    async def _members(self, group_id: str, operation: str) -> UserGroupMembership:
        try:
            answer = await self.client.call(
                self.identity, operation, {"group_id": group_id}, TypeAdapter(UserGroupMembership)
            )
            if answer.group != group_id or (
                operation == "fresh_members" and answer.cache_age_s != 0
            ):
                raise GatewayError("invalid membership answer")
            return answer
        except GatewayError:
            raise UserGroupLookupError("gateway membership unavailable") from None

    async def members(self, group_id: str) -> UserGroupMembership:
        return await self._members(group_id, "members")

    async def fresh_members(self, group_id: str) -> UserGroupMembership:
        return await self._members(group_id, "fresh_members")


class RemoteIdentitySelection:
    def __init__(self, client: GatewayClient | None) -> None:
        self.client = client

    def identity(self, routes: list[BindingRoute]) -> str:
        identities = sorted(
            slack_speaking_identity(SLACK_KIND, route.adapter, route.endpoint) for route in routes
        )
        return "default" if "default" in identities else identities[0]

    def has_identity(self, identity: str) -> bool:
        return self.client is not None and identity in self.client.identities

    def identity_key(self, identity: str) -> str:
        return identity

    async def call[T](
        self, identity: str, operation: str, params: Mapping[str, Any], result: TypeAdapter[T]
    ) -> T:
        if self.client is None:
            raise ChannelReadRefused(503, "provider_unconfigured", "no gateway serves this binding")
        try:
            return await self.client.call(identity, operation, params, result)
        except GatewayAuthenticationError:
            raise ChannelReadRefused(
                503, "provider_unconfigured", "gateway authentication refused"
            ) from None
        except GatewayError:
            code = "edit_outcome_unknown" if operation == "replace_cell" else "provider_error"
            raise ChannelReadRefused(502, code, "gateway could not confirm the operation") from None


class RemoteChannelReader(RemoteIdentitySelection):
    capabilities = frozenset({ChannelCapability.HISTORY_READ})

    def rate_limit_key(
        self, op: Literal["history", "thread", "message"], message_id: str | None
    ) -> str:
        from .slack.reads import provider_method

        return provider_method(op, message_id)

    def valid_thread_id(self, value: str) -> bool:
        from .slack.reads import valid_slack_ts

        return valid_slack_ts(value)

    def valid_message_id(self, value: str) -> bool:
        from .slack.reads import parse_message_id

        return parse_message_id(value) is not None

    async def history(
        self,
        *,
        identity: str,
        channel: str,
        window: ResolvedWindow,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> ProviderPage:
        return await self.call(
            identity,
            "history",
            dict(channel=channel, window=window, limit=limit, cursor=cursor, boundary=boundary),
            TypeAdapter(ProviderPage),
        )

    async def thread(
        self,
        *,
        identity: str,
        channel: str,
        thread_id: str,
        window: ResolvedWindow,
        limit: int,
        cursor: str | None,
        boundary: int | None = None,
    ) -> ProviderPage:
        return await self.call(
            identity,
            "thread",
            dict(
                channel=channel,
                thread_id=thread_id,
                window=window,
                limit=limit,
                cursor=cursor,
                boundary=boundary,
            ),
            TypeAdapter(ProviderPage),
        )

    async def message(
        self, *, identity: str, channel: str, message_id: str
    ) -> ChannelMessage | None:
        return await self.call(
            identity,
            "message",
            dict(channel=channel, message_id=message_id),
            TypeAdapter(ChannelMessage | None),
        )


class RemoteCanvasReader(RemoteIdentitySelection):
    capabilities = frozenset({ChannelCapability.CANVAS_READ, ChannelCapability.CANVAS_EDIT})

    def valid_canvas_id(self, value: str) -> bool:
        from .slack.canvas import valid_canvas_id

        return valid_canvas_id(value)

    def valid_section_id(self, value: str) -> bool:
        from .slack.canvas import valid_section_id

        return valid_section_id(value)

    async def locate(self, *, identity: str, canvas_id: str) -> CanvasFile | None:
        return await self.call(
            identity, "locate", dict(canvas_id=canvas_id), TypeAdapter(CanvasFile | None)
        )

    async def is_member(self, *, identity: str, channel: str) -> bool:
        return await self.call(identity, "is_member", dict(channel=channel), TypeAdapter(bool))

    async def list(self, *, identity: str, channel: str) -> CanvasPage:
        return await self.call(identity, "list", dict(channel=channel), TypeAdapter(CanvasPage))

    async def document(self, *, identity: str, file: CanvasFile) -> CanvasDocument:
        return await self.call(identity, "document", dict(file=file), TypeAdapter(CanvasDocument))

    async def replace_cell(
        self, *, identity: str, canvas_id: str, section_id: str, text: str
    ) -> None:
        await self.call(
            identity,
            "replace_cell",
            dict(canvas_id=canvas_id, section_id=section_id, text=text),
            TypeAdapter(type(None)),
        )


class FileTransport(Protocol):
    def __call__(
        self, *, method: str, url: str, headers: Mapping[str, str], chunk_bytes: int
    ) -> FileResponse: ...


class RemoteFileSource:
    """Bounded file reads using the worker's owned, cancellable HTTP transport."""

    def __init__(
        self,
        endpoint: str,
        token: str,
        identity: str,
        *,
        allow_insecure_http: bool = False,
        transport: FileTransport | None = None,
        chunk_bytes: int = 64 * 1024,
        max_bytes: int = MAX_FILE_BYTES,
    ) -> None:
        from .slack.files import file_transport

        validate_endpoint(endpoint, token, allow_insecure_http=allow_insecure_http)
        self._url = f"{endpoint.rstrip('/')}/v1/gateway/{quote(identity, safe='')}/fetch"
        self._token = token
        self._transport = transport or file_transport
        self._chunk_bytes = chunk_bytes
        self._max_bytes = max_bytes

    def fetch(self, file_id: str) -> Iterator[bytes]:
        from .slack.files import SlackFileError

        response = self._transport(
            method="GET",
            url=f"{self._url}/{quote(file_id, safe='')}",
            headers=request_headers(self._token, "GET", f"{self._url}/{quote(file_id, safe='')}"),
            chunk_bytes=self._chunk_bytes,
        )
        if response.status != 200:
            close = getattr(response.chunks, "close", None)
            if close:
                close()
            raise SlackFileError("gateway file unavailable", status=response.status)

        def chunks() -> Iterator[bytes]:
            size = 0
            try:
                for chunk in response.chunks:
                    size += len(chunk)
                    if size > self._max_bytes:
                        raise SlackFileError("gateway file exceeded limit")
                    yield chunk
            finally:
                close = getattr(response.chunks, "close", None)
                if close:
                    close()

        return chunks()
