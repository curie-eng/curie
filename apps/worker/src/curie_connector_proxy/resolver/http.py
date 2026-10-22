"""The resolver's socket: a minimal strict HTTP/1.1 server (ADR 0239).

"Bundle, identity and request contract" 4 puts the resolver on a pod-local
Unix socket in a dedicated emptyDir, the only thing the server container
shares with the proxy. This is deliberately not aiohttp's server: its parser
inflates compressed bodies before a handler runs, and its error path logs and
answers parser messages that can quote the received bytes, which here are
invocation handles. A connector speaks one request shape, so the server is
small enough to read whole:

1. One request per connection, then ``Connection: close``.
2. The head is read under an 8 KiB stream limit and a 10 second timeout; a
   longer head is 431 ``headers_too_large``, an incomplete one 400.
3. The request line must be exactly ``POST /v1/resolve HTTP/1.1`` (another path
   404, another method 405, anything else 400). Header names are tokens, values
   visible ASCII, and obs-fold is refused.
4. Before any body byte is read: ``Transfer-Encoding`` is 411, a
   ``Content-Encoding`` other than ``identity`` 415, a ``Content-Type`` other
   than ``application/json`` 415, and exactly one all-digit ``Content-Length``
   is required (400 otherwise, 413 above 64 KiB, "Bundle" 6).
5. The body is read with ``readexactly`` under a 10 second timeout.
6. At most ``max_connections`` (default 256) connections are served at once.
   One past the cap is answered a fixed 503 ``{"refusal": "busy"}`` and
   aborted as it is accepted, before any of its bytes is read, so idle
   connections cannot exhaust the resolver.
7. The response write, drain and close share a 10 second deadline; a reader
   that stops reading has its transport aborted when it expires.

Every response is fixed JSON the server builds (``{"refusal": <code>}`` or the
success body) with ``Cache-Control: no-store``. No exception text, header value
or body byte is ever echoed or logged: an unexpected failure logs its class
name and answers 500 ``internal_error``.

``start_resolver`` connects with the application role's DSN only, verifies the
role's actual privileges BEFORE binding the socket ("Custody" 4), and refuses
to start, with the pool closed and no socket created, when they are wrong.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import stat
from functools import partial
from http import HTTPStatus
from typing import Any

import asyncpg  # type: ignore[import-untyped]

from .admission import ACQUIRE_TIMEOUT_S, STORE_ERRORS
from .config import ResolverConfig
from .handles import HandleRegistry
from .privileges import PrivilegeRefusal, verify_role
from .service import Resolver
from .sources import SourceRegistry, default_registry
from .wire import (
    MAX_REQUEST_BYTES,
    RESOLVE_PATH,
    ResolveRefusal,
    parse_request,
    refusal_body,
    status_for,
)

logger = logging.getLogger("curie_connector_proxy.resolver")

HEAD_LIMIT_BYTES = 8192
HEAD_TIMEOUT_S = 10.0
BODY_TIMEOUT_S = 10.0
CLOSE_TIMEOUT_S = 5.0
WRITE_TIMEOUT_S = 10.0
MAX_CONNECTIONS = 256
APPLICATION_NAME = "curie-shared-resolver"
# The role's CONNECTION LIMIT is 32; leave room for an operator's session.
POOL_MAX_SIZE = 8
COMMAND_TIMEOUT_S = 15.0
SERVER_SETTINGS = {
    "application_name": APPLICATION_NAME,
    "lock_timeout": "10s",
    "idle_in_transaction_session_timeout": "30s",
}
SOCKET_MODE = 0o666
# A DSN asyncpg cannot parse raises ValueError; TimeoutError is already a store error.
_CONNECT_ERRORS: tuple[type[BaseException], ...] = (ValueError, *STORE_ERRORS)
_CLOSE_ERRORS: tuple[type[BaseException], ...] = STORE_ERRORS

# RFC 9110 token, and a field value of visible ASCII with inner spaces or tabs.
_TOKEN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_FIELD_VALUE = re.compile(r"^[\x21-\x7e]([\x20-\x7e\t]*[\x21-\x7e])?$|^$")
_DIGITS = re.compile(r"^[0-9]+$")
_LENGTH_DIGITS = len(str(MAX_REQUEST_BYTES))


class ResolverStartRefused(Exception):
    """The resolver refused to start. ``str`` is the code; nothing else is carried."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code

    def __str__(self) -> str:
        return self.code


def _refused(code: str) -> tuple[int, bytes]:
    return status_for(code), refusal_body(code)


def _content_length(headers: dict[str, list[str]]) -> int:
    lengths = headers.get("content-length", [])
    if len(lengths) != 1 or _DIGITS.fullmatch(lengths[0]) is None:
        raise ResolveRefusal("malformed")
    significant = lengths[0].lstrip("0") or "0"
    if len(significant) > _LENGTH_DIGITS or int(significant) > MAX_REQUEST_BYTES:
        raise ResolveRefusal("request_too_large")
    return int(significant)


def parse_head(head: bytes) -> int:
    """Check a request head; answer its Content-Length or raise a ``ResolveRefusal``."""

    try:
        text = head.decode("ascii")
    except UnicodeDecodeError:
        raise ResolveRefusal("malformed") from None
    lines = text.removesuffix("\r\n\r\n").split("\r\n")
    if any("\r" in line or "\n" in line for line in lines):
        raise ResolveRefusal("malformed")
    parts = lines[0].split(" ")
    if len(parts) != 3:
        raise ResolveRefusal("malformed")
    method, target, version = parts
    if version != "HTTP/1.1" or _TOKEN.fullmatch(method) is None or not target.startswith("/"):
        raise ResolveRefusal("malformed")
    if target != RESOLVE_PATH:
        raise ResolveRefusal("not_found")
    if method != "POST":
        raise ResolveRefusal("method_not_allowed")

    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        # A leading space or tab is obs-fold, which RFC 9112 lets a server refuse.
        if not line or line[0] in " \t":
            raise ResolveRefusal("malformed")
        name, separator, value = line.partition(":")
        value = value.strip(" \t")
        if not separator or _TOKEN.fullmatch(name) is None or not _FIELD_VALUE.fullmatch(value):
            raise ResolveRefusal("malformed")
        headers.setdefault(name.lower(), []).append(value)

    if "transfer-encoding" in headers:
        raise ResolveRefusal("length_required")
    if any(value.lower() != "identity" for value in headers.get("content-encoding", [])):
        raise ResolveRefusal("unsupported_media_type")
    types = headers.get("content-type", [])
    if len(types) != 1 or types[0].split(";", 1)[0].strip(" \t").lower() != "application/json":
        raise ResolveRefusal("unsupported_media_type")
    return _content_length(headers)


async def _exchange(resolver: Resolver, reader: asyncio.StreamReader) -> tuple[int, bytes]:
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), HEAD_TIMEOUT_S)
    except asyncio.LimitOverrunError:
        return _refused("headers_too_large")
    except (asyncio.IncompleteReadError, TimeoutError):
        return _refused("malformed")
    try:
        length = parse_head(head)
    except ResolveRefusal as refusal:
        return _refused(refusal.code)
    try:
        body = await asyncio.wait_for(reader.readexactly(length), BODY_TIMEOUT_S)
    except (asyncio.IncompleteReadError, TimeoutError):
        return _refused("malformed")
    try:
        handle, slot, purpose = parse_request(body)
        document = await resolver.resolve(handle, slot, purpose)
    except ResolveRefusal as refusal:
        return _refused(refusal.code)
    return 200, json.dumps(document, separators=(",", ":")).encode("ascii")


def _response(status: int, payload: bytes) -> bytes:
    head = (
        f"HTTP/1.1 {status} {HTTPStatus(status).phrase}\r\n"
        "Content-Type: application/json\r\n"
        "Cache-Control: no-store\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    return head.encode("ascii") + payload


class _ConnectionGate:
    """Counts the connections being served; one past the cap is refused."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.active = 0


def _refuse_busy(writer: asyncio.StreamWriter) -> None:
    """Answer the fixed busy refusal without reading anything, then abort.

    The response is small enough that the socket transport sends it at once
    on an empty buffer; whatever it could not send is dropped with the abort,
    so a refused connection holds nothing open.
    """

    transport = writer.transport
    with contextlib.suppress(Exception):
        transport.pause_reading()  # type: ignore[attr-defined]
    with contextlib.suppress(OSError, RuntimeError):
        writer.write(_response(*_refused("busy")))
    transport.abort()


async def _finish(writer: asyncio.StreamWriter, response: bytes) -> None:
    """Write, drain and close under one deadline; abort the transport when it lapses."""

    async def _write_and_close() -> None:
        try:
            with contextlib.suppress(OSError):
                writer.write(response)
                await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    try:
        await asyncio.wait_for(_write_and_close(), WRITE_TIMEOUT_S)
    except TimeoutError:
        writer.transport.abort()


async def _serve(
    resolver: Resolver,
    gate: _ConnectionGate,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    if gate.active >= gate.limit:
        _refuse_busy(writer)
        return
    gate.active += 1
    try:
        try:
            status, payload = await _exchange(resolver, reader)
        except Exception as error:  # noqa: BLE001 - logged by class name only, answered closed
            logger.error("resolver request failed: %s", type(error).__name__)
            status, payload = _refused("internal_error")
        await _finish(writer, _response(status, payload))
    except BaseException:
        # Cancelled (shutdown): answer nothing and hold nothing open.
        writer.transport.abort()
        raise
    finally:
        gate.active -= 1


async def serve_unix(
    resolver: Resolver, path: str, *, max_connections: int = MAX_CONNECTIONS
) -> asyncio.Server:
    """Serve ``resolver`` on a Unix socket at ``path``, mode 0666.

    The dedicated emptyDir directory is the access boundary, not the mode: the
    server container must be able to connect under its own UID. At most
    ``max_connections`` connections are served at once.
    """

    if type(max_connections) is not int or max_connections < 1:
        raise ValueError("max_connections must be a positive integer")
    gate = _ConnectionGate(max_connections)
    server = await asyncio.start_unix_server(
        partial(_serve, resolver, gate), path=path, limit=HEAD_LIMIT_BYTES
    )
    try:
        os.chmod(path, SOCKET_MODE)
    except OSError:
        server.close()
        raise
    return server


def _clear_stale_socket(path: str) -> None:
    """Remove a previous process's socket; anything else at the path refuses start."""

    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return
    except OSError:
        raise ResolverStartRefused("socket_unavailable") from None
    if not stat.S_ISSOCK(mode):
        raise ResolverStartRefused("socket_unavailable")
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)


def _remove_socket(path: str) -> None:
    with contextlib.suppress(OSError):
        if stat.S_ISSOCK(os.lstat(path).st_mode):
            os.unlink(path)


async def _close_pool(pool: Any) -> None:
    try:
        await asyncio.wait_for(pool.close(), CLOSE_TIMEOUT_S)
    except _CLOSE_ERRORS:
        pool.terminate()


class ResolverRuntime:
    """A running resolver: its registry, its service, and how to stop it."""

    def __init__(
        self,
        *,
        handles: HandleRegistry,
        resolver: Resolver,
        server: asyncio.Server,
        pool: Any,
        socket_path: str,
    ) -> None:
        self.handles = handles
        self.resolver = resolver
        self._server = server
        self._pool = pool
        self._socket_path = socket_path
        self._closed = False

    async def close(self) -> None:
        """Stop serving, remove the socket, then close the pool. Idempotent."""

        if self._closed:
            return
        self._closed = True
        self._server.close()
        try:
            await asyncio.wait_for(self._server.wait_closed(), CLOSE_TIMEOUT_S)
        except TimeoutError:
            self._server.abort_clients()
        _remove_socket(self._socket_path)
        await _close_pool(self._pool)


async def start_resolver(
    config: ResolverConfig,
    *,
    sources: SourceRegistry | None = None,
    handles: HandleRegistry | None = None,
    max_connections: int = MAX_CONNECTIONS,
) -> ResolverRuntime:
    """Connect as the application role, verify it, then bind the socket."""

    if type(max_connections) is not int or max_connections < 1:
        raise ValueError("max_connections must be a positive integer")
    registry = (
        HandleRegistry(installation=config.installation, application=config.application)
        if handles is None
        else handles
    )
    if registry.installation != config.installation or registry.application != config.application:
        raise ResolverStartRefused("foreign_principal")
    chosen = default_registry() if sources is None else sources
    try:
        pool = await asyncpg.create_pool(
            dsn=config.dsn,
            min_size=1,
            max_size=POOL_MAX_SIZE,
            command_timeout=COMMAND_TIMEOUT_S,
            server_settings=dict(SERVER_SETTINGS),
        )
    except _CONNECT_ERRORS:
        raise ResolverStartRefused("store_unavailable") from None
    try:
        try:
            async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as connection:
                await verify_role(connection, config.application)
        except PrivilegeRefusal as refusal:
            raise ResolverStartRefused(refusal.code) from None
        except STORE_ERRORS:
            raise ResolverStartRefused("store_unavailable") from None
        resolver = Resolver(config=config, handles=registry, sources=chosen, pool=pool)
        _clear_stale_socket(config.socket_path)
        try:
            server = await serve_unix(resolver, config.socket_path, max_connections=max_connections)
        except OSError:
            raise ResolverStartRefused("socket_unavailable") from None
    except BaseException:
        await _close_pool(pool)
        raise
    return ResolverRuntime(
        handles=registry,
        resolver=resolver,
        server=server,
        pool=pool,
        socket_path=config.socket_path,
    )
