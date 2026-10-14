"""The caller proxy's HTTP side (ADR-0168 decision 7).

It admits a request whose ``X-Curie-Caller`` token is signed by a configured
public key, unexpired, and names an agent on the connector's rendered
``admits`` list, and forwards it to the server on ``127.0.0.1``. Any other
request is refused in the shape ``tests/vectors/connector-caller-refusal.json``
freezes, without contacting the server.

Paths are not special-cased. The bundled Claude CLI sends the header on its
OAuth discovery and registration requests as well as on ``/mcp``, so those are
checked and forwarded like any other; an admitted caller reaches whatever the
server serves, and a refused one reaches nothing.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Protocol

from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector, web
from aiohttp.typedefs import Handler
from multidict import CIMultiDict, CIMultiDictProxy
from nacl.signing import VerifyKey
from yarl import URL

from . import caller
from .canonical import canonical_arguments

LISTEN_PORT_ENV = "CURIE_CALLER_PROXY_PORT"
UPSTREAM_PORT_ENV = "CURIE_CALLER_PROXY_UPSTREAM_PORT"
PUBLIC_KEYS_ENV = "CURIE_CALLER_PROXY_PUBLIC_KEYS"
ADMITS_ENV = "CURIE_CALLER_PROXY_ADMITS"
GATED_TOOLS_ENV = "CURIE_CALLER_PROXY_GATED_TOOLS"
CONNECTOR_ENV = "CURIE_CALLER_PROXY_CONNECTOR"
GRANT_STORE_ENV = "CURIE_CALLER_PROXY_GRANT_STORE"

GRANT_HEADER = "X-Curie-Connector-Grant"
REFUSAL_STATUS = 403
_REFUSAL_CODE = -32000
_MAX_BODY_BYTES = 1024 * 1024
_REFUSAL_MESSAGES = {
    caller.MISSING: "no caller token was presented",
    caller.INVALID: "the caller token is not valid",
    caller.EXPIRED: "the caller token has expired",
    caller.NOT_ADMITTED: "this agent is not admitted by the connector",
    caller.GRANT_REQUIRED: "the connector tool call has no matching grant",
}
# RFC 9110 section 7.6.1, plus the caller header itself: the token stops here,
# so the connector server never sees one.
_NOT_FORWARDED = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        caller.HEADER.lower(),
        GRANT_HEADER.lower(),
        # Inbound copies are forged until decide() sets them from the token.
        caller.AGENT_HEADER.lower(),
        caller.RUN_HEADER.lower(),
        caller.WORK_ITEM_HEADER.lower(),
    }
)

logger = logging.getLogger("curie_connector_proxy")

# A caller token, and any run of base64url long enough to be a piece of one:
# aiohttp's parse-error log quotes the bytes it refused, which can be a token
# cut anywhere.
_TOKEN_TEXT = re.compile(
    rf"(?:{caller.PREFIX}|{caller.GRANT_PREFIX})\.[A-Za-z0-9_.-]*|[A-Za-z0-9_-]{{32,}}"
)
_REDACTED = "<redacted>"


def _redact(text: str) -> str:
    return _TOKEN_TEXT.sub(_REDACTED, text)


class _RedactCallerTokens(logging.Filter):
    """Rewrite a record so nothing it prints carries a caller token."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact(record.getMessage())
        record.args = None
        if record.exc_info:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
            record.exc_info = None
        if record.exc_text:
            record.exc_text = _redact(record.exc_text)
        if record.stack_info:
            record.stack_info = _redact(record.stack_info)
        return True


def _redacting_logger(name: str) -> logging.Logger:
    named = logging.getLogger(name)
    if not any(isinstance(f, _RedactCallerTokens) for f in named.filters):
        named.addFilter(_RedactCallerTokens())
    return named


# What every connection this app serves is handled with, whoever starts it: a
# body is relayed as sent, never decoded, so the length and encoding forwarded
# with it stay true; and aiohttp's own error and access logs, which can quote a
# request's headers, go through loggers that redact a caller token.
_HANDLER_ARGS = {
    "auto_decompress": False,
    "logger": _redacting_logger("curie_connector_proxy.server"),
    "access_log": _redacting_logger("curie_connector_proxy.access"),
}


class _GrantSpender(Protocol):
    """Spend a grant id once. True only when this call stored it."""

    def spend(self, jti: str, ttl: int) -> bool: ...


def _resp(*parts: str) -> bytes:
    encoded = [part.encode("utf-8") for part in parts]
    head = f"*{len(encoded)}\r\n".encode("ascii")
    body = b"".join(f"${len(part)}\r\n".encode("ascii") + part + b"\r\n" for part in encoded)
    return head + body


def _set_nx_ex(url: str, key: str, ttl: int) -> bool:
    """SET key 1 NX EX ttl over the Redis wire. True only when this call stored it.

    The proxy package cannot import the redis client. A missing store, a TLS
    failure, or any reply other than ``+OK`` is a failed spend.
    """

    import socket
    import ssl
    from urllib.parse import unquote, urlparse

    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not host or parsed.scheme not in {"redis", "rediss"}:
        return False
    port = parsed.port or 6379
    password = unquote(parsed.password) if parsed.password else ""
    database = (parsed.path or "/0").lstrip("/") or "0"
    raw = socket.create_connection((host, port), timeout=2)
    try:
        conn: socket.socket = raw
        if parsed.scheme == "rediss":
            context = ssl.create_default_context()
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            conn = context.wrap_socket(raw, server_hostname=host)

        def reply() -> bytes:
            line = b""
            while not line.endswith(b"\r\n"):
                chunk = conn.recv(1)
                if not chunk:
                    raise OSError("the grant store closed the connection")
                line += chunk
            return line[:-2]

        def command(*parts: str) -> bytes:
            conn.sendall(_resp(*parts))
            return reply()

        if password and command("AUTH", password) != b"+OK":
            return False
        if database != "0" and command("SELECT", database) != b"+OK":
            return False
        return command("SET", key, "1", "NX", "EX", str(ttl)) == b"+OK"
    finally:
        raw.close()


class _ValkeyGrantStore:
    """SET ``connector-grant:<jti>`` NX EX without importing the redis client."""

    def __init__(self, url: str) -> None:
        self._url = url

    def spend(self, jti: str, ttl: int) -> bool:
        try:
            return _set_nx_ex(self._url, f"connector-grant:{jti}", max(ttl, 1))
        except Exception:  # noqa: BLE001 - existing broad catch retained
            logger.warning("connector grant spend failed")
            return False


def _gated_tools(env: Mapping[str, str]) -> tuple[str, ...]:
    raw = env.get(GATED_TOOLS_ENV, "")
    if not raw:
        return ()
    try:
        parsed = json.loads(raw)
    except ValueError:
        raise ValueError(f"{GATED_TOOLS_ENV} must be a JSON list of strings") from None
    if not isinstance(parsed, list) or not all(isinstance(name, str) for name in parsed):
        raise ValueError(f"{GATED_TOOLS_ENV} must be a JSON list of strings")
    return tuple(parsed)


@dataclass(frozen=True)
class ProxyConfig:
    listen_port: int
    upstream_port: int
    public_keys: tuple[VerifyKey, ...]
    admits: frozenset[str]
    gated_tools: tuple[str, ...] = ()
    connector: str = ""
    grant_store: _GrantSpender | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ProxyConfig:
        """Read the render's env. Raises ``ValueError`` naming the variable."""

        def port(name: str) -> int:
            raw = env.get(name, "")
            # `str.isdigit()` accepts non-ASCII decimal digits (Arabic-Indic,
            # for one), and Python's own `int()` parses them, so an ASCII
            # check comes first: a strict digit check alone would admit a
            # port no socket call spells the same way.
            if not raw.isascii() or not raw.isdigit() or not 0 < int(raw) < 65536:
                raise ValueError(f"{name} must be a TCP port, got {raw!r}")
            return int(raw)

        texts = [text for text in env.get(PUBLIC_KEYS_ENV, "").split(",") if text.strip()]
        if not texts:
            raise ValueError(f"{PUBLIC_KEYS_ENV} names no public key")
        try:
            keys = tuple(caller.public_key(text) for text in texts)
        except ValueError as exc:
            raise ValueError(f"{PUBLIC_KEYS_ENV}: {exc}") from None
        try:
            admits = json.loads(env.get(ADMITS_ENV, ""))
        except ValueError:
            raise ValueError(f"{ADMITS_ENV} must be a JSON list of agent names") from None
        if not isinstance(admits, list) or not all(
            isinstance(name, str) and name for name in admits
        ):
            raise ValueError(f"{ADMITS_ENV} must be a JSON list of agent names")
        store_url = env.get(GRANT_STORE_ENV, "").strip()
        return cls(
            listen_port=port(LISTEN_PORT_ENV),
            upstream_port=port(UPSTREAM_PORT_ENV),
            public_keys=keys,
            admits=frozenset(admits),
            gated_tools=_gated_tools(env),
            connector=env.get(CONNECTOR_ENV, ""),
            grant_store=_ValkeyGrantStore(store_url) if store_url else None,
        )


def refusal_body(refusal: str) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": None,
        "error": {
            "code": _REFUSAL_CODE,
            "message": f"caller refused: {_REFUSAL_MESSAGES[refusal]}",
            "data": {"curie_caller": refusal},
        },
    }


def _forwardable(headers: CIMultiDictProxy[str]) -> CIMultiDict[str]:
    # RFC 9110 7.6.1 lets a `Connection` header name additional hop-by-hop
    # headers, but `Host` is never one of them: a caller naming it there must
    # not strip the header the server's allowed-hosts check depends on.
    named = {
        token.strip().lower()
        for value in headers.getall("Connection", [])
        for token in value.split(",")
    } - {"host"}
    return CIMultiDict(
        (key, value)
        for key, value in headers.items()
        if key.lower() not in _NOT_FORWARDED and key.lower() not in named
    )


_CONFIG = web.AppKey("config", ProxyConfig)
_CLOCK: web.AppKey[Callable[[], float]] = web.AppKey("clock")
_UPSTREAM = web.AppKey("upstream", ClientSession)


@dataclass(frozen=True)
class _ToolCall:
    name: str
    arguments: object


def _tool_call(parsed: object) -> _ToolCall | None:
    """One ``tools/call`` name and its arguments object, or None."""

    if not isinstance(parsed, dict) or parsed.get("method") != "tools/call":
        return None
    params = parsed.get("params")
    if not isinstance(params, dict):
        return None
    name = params.get("name")
    if not isinstance(name, str) or not name:
        return None
    if "arguments" in params:
        return _ToolCall(name=name, arguments=params["arguments"])
    return _ToolCall(name=name, arguments={})


def _tool_calls(body: bytes) -> tuple[_ToolCall, ...]:
    """Tool calls in one JSON object or a top-level JSON array.

    Invalid JSON, and a value that is neither an object nor an array, yields
    no calls, so the body stays on the forward path. Each array element uses
    the same per-object rules as a single request.
    """

    try:
        parsed = json.loads(body)
    except ValueError:
        return ()
    if isinstance(parsed, dict):
        call = _tool_call(parsed)
        return (call,) if call is not None else ()
    if not isinstance(parsed, list):
        return ()
    found: list[_ToolCall] = []
    for item in parsed:
        call = _tool_call(item)
        if call is not None:
            found.append(call)
    return tuple(found)


def _is_gated(config: ProxyConfig, name: str) -> bool:
    candidates = (f"{config.connector}/{name}", f"mcp__{config.connector}__{name}")
    return any(
        fnmatchcase(candidate, pattern)
        for pattern in config.gated_tools
        for candidate in candidates
    )


def _canonical_arguments(arguments: object) -> str | None:
    """The shared canonical text, or None when it has none. @spec ACTION-EXECUTOR-7."""

    try:
        return canonical_arguments(arguments)
    except (TypeError, ValueError):
        return None


def _grant_spent(config: ProxyConfig, grant: caller.Grant, now: int) -> bool:
    """Spend a grant that already matches. A missing store never spends."""

    store = config.grant_store
    if store is None:
        return False
    try:
        return bool(store.spend(grant.jti, max(grant.exp - now, 1)))
    except Exception:  # noqa: BLE001 - existing broad catch retained
        logger.warning("connector grant spend failed")
        return False


def _grant_refused(request: web.Request, agent: str, body: bytes, now: int) -> bool:
    """True when a gated tools/call in this admitted POST lacks one live grant.

    Two or more gated calls are refused before a grant is spent.
    """

    config = request.app[_CONFIG]
    gated = [call for call in _tool_calls(body) if _is_gated(config, call.name)]
    if not gated:
        return False
    if len(gated) > 1:
        return True
    call = gated[0]
    presented = request.headers.getall(GRANT_HEADER, [])
    if len(presented) != 1:
        return True
    grant = caller.verify(config.public_keys, presented[0])
    if grant is None or grant.exp <= now:
        return True
    if grant.agent != agent or grant.connector != config.connector or grant.tool != call.name:
        return True
    canonical = _canonical_arguments(call.arguments)
    if canonical is None or grant.args != canonical:
        return True
    return not _grant_spent(config, grant, now)


def _decide(request: web.Request) -> caller.Decision:
    config = request.app[_CONFIG]
    tokens = request.headers.getall(caller.HEADER, [])
    if len(tokens) > 1:
        return caller.Decision(agent=None, refusal=caller.INVALID)
    return caller.decide(
        config.public_keys,
        tokens[0] if tokens else None,
        admits=config.admits,
        now=int(request.app[_CLOCK]()),
    )


@web.middleware
async def _handle(request: web.Request, handler: Handler) -> web.StreamResponse:
    del handler  # Every request is decided and answered here; never delegated.
    # A middleware, not a route: aiohttp's router unquotes a path before
    # matching it against a route's regex, and an unquoted embedded newline
    # (from `%0A`) fails `.*` (which does not match `\n`) even in a
    # catch-all, giving the router's own 404 -- a path the server was never
    # asked about answering as though it had been. A middleware runs around
    # routing's result either way, so every request, matched or not, is
    # decided and never silently let through to the router's default.
    decision = _decide(request)
    refusal = decision.refusal
    body: bytes | None = None
    buffered = False
    if refusal is None and request.method == "POST" and request.body_exists:
        try:
            body = await request.read()
        except web.HTTPRequestEntityTooLarge:
            # A body over 1 MiB is refused rather than forwarded unread.
            logger.info(
                "caller=%s outcome=too_large method=%s path=%r",
                decision.agent or "-",
                request.method,
                request.path,
            )
            return web.Response(status=REFUSAL_STATUS, text="the request body is too large\n")
        buffered = True
        if len(body) > _MAX_BODY_BYTES or _grant_refused(
            request, decision.agent or "", body, int(request.app[_CLOCK]())
        ):
            refusal = caller.GRANT_REQUIRED
    # The path is decoded, so it is quoted: an encoded newline in it stays
    # `\n` inside this line rather than starting a line of its own.
    logger.info(
        "caller=%s outcome=%s method=%s path=%r",
        decision.agent or "-",
        refusal or "admitted",
        request.method,
        request.path,
    )
    if refusal is not None:
        return web.json_response(refusal_body(refusal), status=REFUSAL_STATUS)
    config = request.app[_CONFIG]
    target = URL.build(
        scheme="http",
        host="127.0.0.1",
        port=config.upstream_port,
        path=request.rel_url.raw_path,
        query_string=request.rel_url.raw_query_string,
        encoded=True,
    )
    forwarded = _forwardable(request.headers)
    # ADR 0178 decision 5. The token stays stripped. The connector sees the
    # verified claims as headers only this proxy can set.
    if decision.agent is not None:
        forwarded.add(caller.AGENT_HEADER, decision.agent)
    if decision.run is not None and decision.work_item is not None:
        forwarded.add(caller.RUN_HEADER, decision.run)
        forwarded.add(caller.WORK_ITEM_HEADER, decision.work_item)
    try:
        answer = await request.app[_UPSTREAM].request(
            request.method,
            target,
            headers=forwarded,
            data=(
                body
                if buffered
                else request.content
                if request.body_exists
                else None
            ),
            allow_redirects=False,
        )
    except ClientError:
        return web.Response(status=502, text="the connector server is unreachable\n")
    try:
        response = web.StreamResponse(
            status=answer.status, reason=answer.reason, headers=_forwardable(answer.headers)
        )
        await response.prepare(request)
        async for chunk in answer.content.iter_any():
            await response.write(chunk)
        await response.write_eof()
        return response
    finally:
        # Also on cancellation: a caller that hangs up closes the server's
        # stream with it.
        answer.release()


def make_app(config: ProxyConfig, *, clock: Callable[[], float] = time.time) -> web.Application:
    async def upstream(app: web.Application) -> AsyncIterator[None]:
        # No total timeout: an MCP GET stream stays open for the session.
        session = ClientSession(
            connector=TCPConnector(limit=0),
            timeout=ClientTimeout(total=None, sock_connect=5),
            auto_decompress=False,
            # The forwarding client must add nothing a caller did not send: a
            # caller that sent no Accept-Encoding must not get a compressed
            # body back because the proxy's own client asked for one.
            skip_auto_headers=("Accept", "Accept-Encoding", "User-Agent"),
        )
        app[_UPSTREAM] = session
        yield
        await session.close()

    app = web.Application(middlewares=[_handle], handler_args=_HANDLER_ARGS)
    app[_CONFIG] = config
    app[_CLOCK] = clock
    app.cleanup_ctx.append(upstream)
    return app
