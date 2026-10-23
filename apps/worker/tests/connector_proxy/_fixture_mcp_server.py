"""The upstream MCP server behind the shared caller proxy (ADR 0239, cp4).

The second faked party in the shared proxy suites, beside ``_fake_provider``:
it stands in for an adapted shared connector server, so what the proxy
forwards, and what never reaches the server, is observed rather than assumed.
It speaks MCP Streamable HTTP on ``127.0.0.1`` (an ephemeral port), the subset
a shared server needs:

1. ``POST /mcp`` with ``initialize`` assigns a fresh ``Mcp-Session-Id`` on the
   response; a request naming a session the server does not hold is 404.
   After ``stream_next_initialize()`` the next initialize is answered as an SSE
   stream instead: ``Mcp-Session-Id`` in its headers, the result as the first
   event, then held open until the test releases it.
2. Notifications (``notifications/initialized``, ``notifications/cancelled``)
   and JSON-RPC responses are answered 202 with no body. Cancellation is
   recorded and otherwise ignored: a running tool is never stopped by it.
3. ``tools/list`` and ``tools/call``; a batch array is answered element by
   element.
4. ``GET /mcp`` opens an SSE stream (a ``: open`` comment first, then
   keep-alive comments) held until the client leaves or the server closes.
5. ``DELETE /mcp`` ends the session it names.

Every request is recorded before it is answered (method, path, every header
as received, the body and the JSON-RPC methods in it), so a test counts
exactly what reached the server.

The tools resolve credentials the way an adopter must: through the REAL
resolver over its Unix socket, with the reference SDK loaded by path from
``resolver/sdk/curie_resolver_sdk.py`` as a vendored copy would be, presenting
the handle from the forwarded ``X-Curie-Invocation`` and calling the fake
provider over TLS. Each provider call is one ``ProviderClient.request``, so
one resolve.

``whoami``      one provider call to ``/v1/whoami``
``get_item``    ``{"id": <alphanumeric>}``, one provider call to ``/items/<id>``
``run_task``    one provider call to ``/v1/runs``; also reports the
                resolution's ``run`` and ``work_item`` (plan R1-1)
``create_item`` one provider call to ``/v1/created`` (the gated tool)
``hold``        ``{"key": <str>, "resolve_first": <bool, default true>}``:
                resolves once (``/v1/hold/first``), parks until the test
                releases ``hold(key)``, then resolves again with the same
                handle (``/v1/hold/second``) and answers both outcomes. The
                outcome is also kept on the ``Hold`` so a test can read it
                after its client hung up.
``echo_headers`` answers the headers this request arrived with; no resolve

A provider call answers ``binding``, ``account``, ``revision`` and
``expires_at`` from the resolution, and ``provider_path`` and
``authorization_sha256`` from what the provider echoed. A resolver refusal
answers ``{"refused": <code>, "status": <status>}``.

Nothing here imports the shared proxy, so a missing production module fails
only the tests that need it.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib.util
import json
import os
import re
import secrets
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any

from _fake_provider import FakeProvider
from aiohttp import web

SDK_FILE = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "curie_connector_proxy"
    / "resolver"
    / "sdk"
    / "curie_resolver_sdk.py"
)
MCP_PATH = "/mcp"
PROTOCOL_VERSION = "2025-06-18"
TOOLS = ("whoami", "get_item", "run_task", "create_item", "hold", "echo_headers")
INVOCATION_HEADER = "X-Curie-Invocation"
SESSION_HEADER = "Mcp-Session-Id"
# Bounds every wait the server makes, so a hang fails instead of stalling.
HOLD_WAIT_S = 90.0
_ITEM_ID = re.compile(r"^[A-Za-z0-9]{1,32}$")


def load_sdk() -> ModuleType:
    """The SDK file loaded by path, unregistered, as a vendored copy would be."""

    name = f"curie_resolver_sdk_fixture_{os.urandom(4).hex()}"
    spec = importlib.util.spec_from_file_location(name, SDK_FILE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class UpstreamRequest:
    """One request exactly as the server received it."""

    method: str
    path: str
    headers: tuple[tuple[str, str], ...]
    body: bytes
    rpc: tuple[str, ...]

    def values(self, name: str) -> list[str]:
        """Every value of header ``name`` (case-insensitive), in arrival order."""

        wanted = name.lower()
        return [value for key, value in self.headers if key.lower() == wanted]

    @property
    def names(self) -> set[str]:
        return {key.lower() for key, _ in self.headers}

    @property
    def handles(self) -> list[str]:
        return self.values(INVOCATION_HEADER)

    @property
    def handle(self) -> str | None:
        """The one forwarded handle, or None when there is none."""

        found = self.handles
        assert len(found) <= 1, f"{len(found)} {INVOCATION_HEADER} headers reached the server"
        return found[0] if found else None

    @property
    def session_id(self) -> str | None:
        found = self.values(SESSION_HEADER)
        return found[0] if found else None

    @property
    def curie_headers(self) -> set[str]:
        return {name for name in self.names if name.startswith("x-curie-")}


@dataclass
class Hold:
    """One parked ``hold`` call: entered after its first resolve, released by the test."""

    key: str
    entered: threading.Event = field(default_factory=threading.Event)
    released: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    handle: str | None = None
    first: dict[str, Any] | None = None
    second: dict[str, Any] | None = None

    def wait_entered(self, timeout: float = HOLD_WAIT_S) -> None:
        assert self.entered.wait(timeout), f"hold {self.key!r} was never entered"

    def release(self) -> None:
        self.released.set()

    def wait_done(self, timeout: float = HOLD_WAIT_S) -> dict[str, Any]:
        assert self.done.wait(timeout), f"hold {self.key!r} never finished"
        return {"first": self.first, "second": self.second}


@dataclass
class OpeningStream:
    """One initialize answered as a held SSE stream; released by the test."""

    opened: threading.Event = field(default_factory=threading.Event)
    released: threading.Event = field(default_factory=threading.Event)
    closed: threading.Event = field(default_factory=threading.Event)
    session_id: str | None = None

    def wait_opened(self, timeout: float = HOLD_WAIT_S) -> str:
        assert self.opened.wait(timeout), "the streaming initialize never opened"
        assert self.session_id is not None
        return self.session_id

    def release(self) -> None:
        self.released.set()

    def wait_closed(self, timeout: float = HOLD_WAIT_S) -> None:
        assert self.closed.wait(timeout), "the streaming initialize never ended"


def _jsonrpc_result(message_id: object, result: Mapping[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": dict(result)}


def _jsonrpc_error(message_id: object, code: int, text: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": text}}


def _tool_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    failed = "refused" in payload or "error" in payload
    return {
        "content": [{"type": "text", "text": json.dumps(dict(payload), sort_keys=True)}],
        "structuredContent": dict(payload),
        "isError": failed,
    }


def _tool_list() -> list[dict[str, Any]]:
    return [
        {"name": name, "inputSchema": {"type": "object", "additionalProperties": True}}
        for name in TOOLS
    ]


def _rpc_methods(parsed: object) -> tuple[str, ...]:
    messages = parsed if isinstance(parsed, list) else [parsed]
    found: list[str] = []
    for message in messages:
        if isinstance(message, dict) and isinstance(message.get("method"), str):
            found.append(message["method"])
    return tuple(found)


class FixtureMCPServer:
    """An MCP Streamable HTTP server on its own thread and event loop."""

    def __init__(self, socket_path: str, provider: FakeProvider, *, slot: str) -> None:
        self._sdk = load_sdk()
        self._resolver = self._sdk.ResolverClient(socket_path, timeout=30.0)
        self._provider = provider
        self._slot = slot
        self._lock = threading.Lock()
        self._requests: list[UpstreamRequest] = []
        self._sessions: set[str] = set()
        self._holds: dict[str, Hold] = {}
        self._opening: list[OpeningStream] = []
        self._streams_open = 0
        self._stop_streams = threading.Event()
        self.stream_opened = threading.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=32, thread_name_prefix="fixture-mcp-tool"
        )
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._runner: web.AppRunner | None = None
        self._closed = False
        self.port: int = self._wait(self._start())
        self.url = f"http://127.0.0.1:{self.port}"

    def _wait(self, coroutine: Any, timeout: float = HOLD_WAIT_S) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout)

    async def _start(self) -> int:
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        # handler_cancellation: a client that hangs up ends its SSE handler; a
        # tool already running on the executor still finishes and records.
        self._runner = web.AppRunner(
            app, access_log=None, handler_cancellation=True, shutdown_timeout=2.0
        )
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        addresses = self._runner.addresses
        assert addresses, "the fixture MCP server bound no address"
        return int(addresses[0][1])

    # --- observations --------------------------------------------------------

    @property
    def requests(self) -> list[UpstreamRequest]:
        with self._lock:
            return list(self._requests)

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._requests)

    def since(self, count: int) -> list[UpstreamRequest]:
        """The requests received after the first ``count``."""

        with self._lock:
            return list(self._requests[count:])

    @property
    def sessions(self) -> set[str]:
        with self._lock:
            return set(self._sessions)

    @property
    def streams_open(self) -> int:
        with self._lock:
            return self._streams_open

    def hold(self, key: str) -> Hold:
        """The ``hold`` call keyed ``key``, created when first asked for."""

        with self._lock:
            found = self._holds.get(key)
            if found is None:
                found = self._holds[key] = Hold(key)
            return found

    def stream_next_initialize(self) -> OpeningStream:
        """Answer the next ``initialize`` as a held SSE stream."""

        opening = OpeningStream()
        with self._lock:
            self._opening.append(opening)
        return opening

    # --- the transport ---------------------------------------------------------

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        body = await request.read()
        try:
            parsed: object = json.loads(body) if body else None
        except ValueError:
            parsed = None
        record = UpstreamRequest(
            method=request.method,
            path=request.path,
            headers=tuple((key, value) for key, value in request.headers.items()),
            body=body,
            rpc=_rpc_methods(parsed),
        )
        with self._lock:
            self._requests.append(record)
        if request.path != MCP_PATH:
            return web.json_response({"error": "not_found"}, status=404)
        if request.method == "GET":
            return await self._stream(request, record)
        if request.method == "DELETE":
            return self._delete(record)
        if request.method != "POST":
            return web.json_response({"error": "method_not_allowed"}, status=405)
        if body and parsed is None:
            return web.json_response(_jsonrpc_error(None, -32700, "parse error"), status=400)
        return await self._post(request, record, parsed)

    def _known_session(self, record: UpstreamRequest) -> bool:
        session = record.session_id
        if session is None:
            return True
        with self._lock:
            return session in self._sessions

    async def _post(
        self, request: web.Request, record: UpstreamRequest, parsed: object
    ) -> web.StreamResponse:
        is_initialize = isinstance(parsed, dict) and parsed.get("method") == "initialize"
        if not is_initialize and not self._known_session(record):
            return web.json_response(_jsonrpc_error(None, -32001, "session not found"), status=404)
        opening: OpeningStream | None = None
        if is_initialize:
            with self._lock:
                opening = self._opening.pop(0) if self._opening else None
        if isinstance(parsed, list):
            replies: list[dict[str, Any]] = []
            for message in parsed:
                reply, _ = await self._message(record, message)
                if reply is not None:
                    replies.append(reply)
            if not replies:
                return web.Response(status=202)
            return web.json_response(replies)
        reply, session = await self._message(record, parsed)
        if reply is None:
            return web.Response(status=202)
        if opening is not None and session is not None:
            return await self._stream_initialize(request, reply, session, opening)
        headers = {SESSION_HEADER: session} if session is not None else {}
        return web.json_response(reply, headers=headers)

    async def _stream_initialize(
        self,
        request: web.Request,
        reply: Mapping[str, Any],
        session: str,
        opening: OpeningStream,
    ) -> web.StreamResponse:
        """The initialize result as the first SSE event of a stream held open."""

        try:
            response = web.StreamResponse(
                headers={
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-store",
                    SESSION_HEADER: session,
                }
            )
            await response.prepare(request)
            event = b"event: message\ndata: " + json.dumps(dict(reply)).encode() + b"\n\n"
            await response.write(event)
            opening.session_id = session
            opening.opened.set()
            for _ in range(int(HOLD_WAIT_S / 0.05)):
                if opening.released.is_set() or self._stop_streams.is_set():
                    break
                await asyncio.sleep(0.05)
            return response
        finally:
            opening.closed.set()

    async def _message(
        self, record: UpstreamRequest, message: object
    ) -> tuple[dict[str, Any] | None, str | None]:
        """One JSON-RPC message: its reply (None for none) and a session it opened."""

        if not isinstance(message, dict):
            return _jsonrpc_error(None, -32600, "invalid request"), None
        message_id = message.get("id")
        method = message.get("method")
        if not isinstance(method, str):
            return None, None  # a JSON-RPC response from the client
        if method == "initialize":
            session = secrets.token_urlsafe(18)
            with self._lock:
                self._sessions.add(session)
            result = {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "curie-shared-fixture", "version": "1"},
            }
            return _jsonrpc_result(message_id, result), session
        if method.startswith("notifications/"):
            return None, None
        if method == "tools/list":
            return _jsonrpc_result(message_id, {"tools": _tool_list()}), None
        if method == "tools/call":
            params = message.get("params")
            payload = await self._tool(record, params if isinstance(params, dict) else {})
            if "id" not in message:
                return None, None
            return _jsonrpc_result(message_id, _tool_result(payload)), None
        return _jsonrpc_error(message_id, -32601, "method not found"), None

    async def _stream(self, request: web.Request, record: UpstreamRequest) -> web.StreamResponse:
        if not self._known_session(record):
            return web.json_response(_jsonrpc_error(None, -32001, "session not found"), status=404)
        response = web.StreamResponse(
            headers={"Content-Type": "text/event-stream", "Cache-Control": "no-store"}
        )
        await response.prepare(request)
        await response.write(b": open\n\n")
        with self._lock:
            self._streams_open += 1
        self.stream_opened.set()
        try:
            for tick in range(int(HOLD_WAIT_S / 0.25)):
                if self._stop_streams.is_set():
                    break
                await asyncio.sleep(0.25)
                if tick % 8 == 7:
                    await response.write(b": keep-alive\n\n")
        except ConnectionResetError:
            pass
        finally:
            with self._lock:
                self._streams_open -= 1
        return response

    def _delete(self, record: UpstreamRequest) -> web.StreamResponse:
        session = record.session_id
        with self._lock:
            known = session is not None and session in self._sessions
            if known:
                self._sessions.discard(session or "")
        return web.Response(status=200 if known else 404)

    # --- the tools -------------------------------------------------------------

    async def _tool(self, record: UpstreamRequest, params: Mapping[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        raw_arguments = params.get("arguments")
        arguments: Mapping[str, Any] = raw_arguments if isinstance(raw_arguments, dict) else {}
        found = record.handles
        handle = found[0] if len(found) == 1 else ""
        if name == "echo_headers":
            return {"headers": [[key, value] for key, value in record.headers]}
        work: Callable[[], dict[str, Any]]
        if name == "whoami":
            work = partial(self._provider_call, handle, "/v1/whoami", "whoami")
        elif name == "get_item":
            item = arguments.get("id")
            if not isinstance(item, str) or _ITEM_ID.fullmatch(item) is None:
                return {"error": "bad_item_id"}
            work = partial(self._provider_call, handle, f"/items/{item}", "get item")
        elif name == "run_task":
            work = partial(self._provider_call, handle, "/v1/runs", "run task", run=True)
        elif name == "create_item":
            work = partial(self._provider_call, handle, "/v1/created", "create item")
        elif name == "hold":
            key = arguments.get("key")
            if not isinstance(key, str) or not key:
                return {"error": "hold_needs_a_key"}
            first = arguments.get("resolve_first", True) is not False
            work = partial(self._hold, handle, key, resolve_first=first)
        else:
            return {"error": "unknown_tool"}
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(self._executor, work)
        except Exception as error:  # noqa: BLE001 - reported to the test by class name
            return {"error": type(error).__name__}

    def _provider_call(
        self, handle: str, path: str, purpose: str, *, run: bool = False
    ) -> dict[str, Any]:
        """One SDK provider request: exactly one resolve over the real socket."""

        seen: list[Any] = []
        sdk = self._sdk

        def authorize(resolution: Any, headers: Mapping[str, str]) -> dict[str, str]:
            seen.append(resolution)
            authorized: dict[str, str] = sdk.bearer(resolution, headers)
            return authorized

        client = sdk.ProviderClient(
            self._resolver,
            self._slot,
            authorize=authorize,
            ssl_context=self._provider.client_ssl(),
        )
        try:
            response = client.request(handle, "GET", self._provider.origin + path, purpose=purpose)
        except sdk.ResolverRefused as refused:
            return {"refused": str(refused.code), "status": int(refused.status)}
        (resolution,) = seen
        echoed = json.loads(response.body)
        outcome: dict[str, Any] = {
            "binding": resolution.binding,
            "account": resolution.account,
            "revision": resolution.revision,
            "expires_at": resolution.expires_at,
            "provider_status": response.status,
            "provider_path": echoed.get("path"),
            "authorization_sha256": echoed.get("authorization_sha256"),
        }
        if run:
            outcome["run"] = resolution.run
            outcome["work_item"] = resolution.work_item
        return outcome

    def _hold(self, handle: str, key: str, *, resolve_first: bool) -> dict[str, Any]:
        hold = self.hold(key)
        hold.handle = handle
        if resolve_first:
            hold.first = self._provider_call(handle, "/v1/hold/first", "hold first")
        hold.entered.set()
        try:
            hold.released.wait(HOLD_WAIT_S)
            hold.second = self._provider_call(handle, "/v1/hold/second", "hold second")
        finally:
            hold.done.set()
        return {"first": hold.first, "second": hold.second}

    # --- lifecycle ---------------------------------------------------------------

    def release_all(self) -> None:
        with self._lock:
            holds = list(self._holds.values())
            openings = list(self._opening)
        for hold in holds:
            hold.release()
        for opening in openings:
            opening.release()
        self._stop_streams.set()

    def stop(self) -> None:
        """Stop listening, so the proxy's upstream is unreachable. Idempotent."""

        self.release_all()
        runner, self._runner = self._runner, None
        if runner is not None:
            self._wait(runner.cleanup())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.stop()
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)
            if not self._thread.is_alive():
                self._loop.close()
            self._executor.shutdown(wait=False, cancel_futures=True)
