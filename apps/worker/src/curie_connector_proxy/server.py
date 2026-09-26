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
import time
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass

from aiohttp import ClientError, ClientSession, ClientTimeout, TCPConnector, web
from multidict import CIMultiDict, CIMultiDictProxy
from nacl.signing import VerifyKey
from yarl import URL

from . import caller

LISTEN_PORT_ENV = "CURIE_CALLER_PROXY_PORT"
UPSTREAM_PORT_ENV = "CURIE_CALLER_PROXY_UPSTREAM_PORT"
PUBLIC_KEYS_ENV = "CURIE_CALLER_PROXY_PUBLIC_KEYS"
ADMITS_ENV = "CURIE_CALLER_PROXY_ADMITS"

REFUSAL_STATUS = 403
_REFUSAL_CODE = -32000
_REFUSAL_MESSAGES = {
    caller.MISSING: "no caller token was presented",
    caller.INVALID: "the caller token is not valid",
    caller.EXPIRED: "the caller token has expired",
    caller.NOT_ADMITTED: "this agent is not admitted by the connector",
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
    }
)

logger = logging.getLogger("curie_connector_proxy")


@dataclass(frozen=True)
class ProxyConfig:
    listen_port: int
    upstream_port: int
    public_keys: tuple[VerifyKey, ...]
    admits: frozenset[str]

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ProxyConfig:
        """Read the render's env. Raises ``ValueError`` naming the variable."""

        def port(name: str) -> int:
            raw = env.get(name, "")
            if not raw.isdigit() or not 0 < int(raw) < 65536:
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
        return cls(
            listen_port=port(LISTEN_PORT_ENV),
            upstream_port=port(UPSTREAM_PORT_ENV),
            public_keys=keys,
            admits=frozenset(admits),
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
    named = {
        token.strip().lower()
        for value in headers.getall("Connection", [])
        for token in value.split(",")
    }
    return CIMultiDict(
        (key, value)
        for key, value in headers.items()
        if key.lower() not in _NOT_FORWARDED and key.lower() not in named
    )


_CONFIG = web.AppKey("config", ProxyConfig)
_CLOCK: web.AppKey[Callable[[], float]] = web.AppKey("clock")
_UPSTREAM = web.AppKey("upstream", ClientSession)


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


async def _handle(request: web.Request) -> web.StreamResponse:
    decision = _decide(request)
    logger.info(
        "caller=%s outcome=%s method=%s path=%s",
        decision.agent or "-",
        decision.refusal or "admitted",
        request.method,
        request.path,
    )
    if decision.refusal is not None:
        return web.json_response(refusal_body(decision.refusal), status=REFUSAL_STATUS)
    config = request.app[_CONFIG]
    target = URL.build(
        scheme="http",
        host="127.0.0.1",
        port=config.upstream_port,
        path=request.rel_url.raw_path,
        query_string=request.rel_url.raw_query_string,
        encoded=True,
    )
    try:
        answer = await request.app[_UPSTREAM].request(
            request.method,
            target,
            headers=_forwardable(request.headers),
            data=request.content if request.body_exists else None,
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
        )
        app[_UPSTREAM] = session
        yield
        await session.close()

    app = web.Application()
    app[_CONFIG] = config
    app[_CLOCK] = clock
    app.cleanup_ctx.append(upstream)
    app.router.add_route("*", "/{tail:.*}", _handle)
    return app
