"""Declared connector routes behind the caller proxy (ADR 0240 decision 3).

Driven over real sockets, like ``test_connector_proxy.py``: a real aiohttp
upstream records what reaches it, so a refused request is observed never to
arrive. Route tokens are minted by the worker's own ``caller_token`` with the
seed from ``tests/vectors/connector-caller-token.json``, so mint and verify are
the two real halves.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_connector_proxy import caller, server
from curie_worker import caller_token
from yarl import URL

_ROOT = Path(__file__).resolve().parents[3]
_TOKENS = json.loads(
    (_ROOT / "tests" / "vectors" / "connector-caller-token.json").read_text(encoding="utf-8")
)
_VECTOR = next(v for v in _TOKENS["vectors"] if v["name"] == "a_plain_agent_name")
_SEED = str(_VECTOR["seed"])
_EXP = int(_VECTOR["exp"])
_NOW = _EXP - 10
_CALLER_TOKEN = str(_VECTOR["minted"])
_CONNECTOR = "reviewer"
_RUN = "11111111-2222-4333-8444-555555555555"
_WORK_ITEM = "66666666-7777-4888-8999-aaaaaaaaaaaa"
_ROUTE_LIMIT = 3 * 1024 * 1024
_REVIEW = server.RouteConfig(
    path="/v1/review", methods=frozenset({"POST"}), max_body_bytes=_ROUTE_LIMIT
)


def _route_token(
    *, connector: str = _CONNECTOR, agent: str = "acme-dev", exp: int = _EXP, **claims: str
) -> str:
    return caller_token.mint_route(_SEED, agent=agent, connector=connector, exp=exp, **claims)


class _Upstream:
    """A connector server that records every request it is sent."""

    def __init__(self) -> None:
        self.seen: list[dict[str, Any]] = []
        # Bytes each request delivered before it ended, recorded even when the
        # body was cut off, so a refused upload's reach is observed.
        self.received: list[int] = []
        self.app = web.Application(client_max_size=1024**3)
        self.app.router.add_post("/early", self._early)
        self.app.router.add_route("*", "/{tail:.*}", self._echo)

    async def _early(self, request: web.Request) -> web.Response:
        # Answers without reading the body, as a server refusing it would.
        del request
        return web.Response(status=401, body=b"early")

    async def _echo(self, request: web.Request) -> web.Response:
        count = 0
        chunks: list[bytes] = []
        try:
            async for chunk in request.content.iter_any():
                count += len(chunk)
                chunks.append(chunk)
        finally:
            self.received.append(count)
        body = b"".join(chunks)
        self.seen.append(
            {
                "method": request.method,
                "path_qs": request.raw_path,
                "headers": request.headers.copy(),
                "body": body,
            }
        )
        return web.Response(status=201, body=f"got {len(body)}".encode())


@asynccontextmanager
async def _serving(
    *,
    routes: tuple[server.RouteConfig, ...] = (_REVIEW,),
    admits: frozenset[str] = frozenset({"acme-dev"}),
) -> AsyncIterator[tuple[_Upstream, TestServer]]:
    upstream = _Upstream()
    upstream_server = TestServer(upstream.app, handler_cancellation=True)
    await upstream_server.start_server(auto_decompress=False)
    config = server.ProxyConfig(
        listen_port=8480,
        upstream_port=upstream_server.port or 0,
        public_keys=(caller.public_key(str(_VECTOR["public"])),),
        admits=admits,
        connector=_CONNECTOR,
        routes=routes,
    )
    proxy = TestServer(server.make_app(config, clock=lambda: _NOW), handler_cancellation=True)
    await proxy.start_server()
    try:
        yield upstream, proxy
    finally:
        await proxy.close()
        await upstream_server.close()


def _run(body: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(body())


async def _post(
    proxy: TestServer, path: str, data: object, headers: dict[str, str], method: str = "POST"
) -> tuple[int, bytes, Any]:
    # Sent as written: the client would otherwise merge dot segments and
    # decode unreserved escapes before the proxy saw them.
    url = URL(f"http://{proxy.host}:{proxy.port}{path}", encoded=True)
    async with aiohttp.ClientSession() as client:
        async with client.request(method, url, data=data, headers=headers) as a:
            return a.status, await a.read(), a.headers


# @spec ADR-0240 d3
def test_a_route_token_reaches_its_route_with_the_body_past_one_mebibyte() -> None:
    body = bytes(range(256)) * (2 * 1024 * 1024 // 256)

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status, answer, _ = await _post(
                proxy,
                "/v1/review?draft=1",
                body,
                {caller.HEADER: _route_token(run=_RUN, work_item=_WORK_ITEM)},
            )
            assert status == 201
            assert answer == f"got {len(body)}".encode()
            [seen] = upstream.seen
            assert seen["path_qs"] == "/v1/review?draft=1"
            assert seen["body"] == body
            assert caller.HEADER not in seen["headers"]
            assert seen["headers"][caller.AGENT_HEADER] == "acme-dev"
            assert seen["headers"][caller.RUN_HEADER] == _RUN
            assert seen["headers"][caller.WORK_ITEM_HEADER] == _WORK_ITEM

    _run(go)


# @spec ADR-0240 d3
def test_a_chunked_body_within_the_route_limit_is_relayed() -> None:
    chunk = b"x" * 65536

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(32):
            yield chunk

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status, _, _ = await _post(
                proxy, "/v1/review", chunks(), {caller.HEADER: _route_token()}
            )
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == chunk * 32

    _run(go)


@pytest.mark.parametrize(
    ("token", "refusal"),
    [
        (None, caller.MISSING),
        (_CALLER_TOKEN, caller.INVALID),
        ("ccr.not.valid", caller.INVALID),
        ("connector:other", caller.INVALID),
        ("expired", caller.EXPIRED),
        ("agent:someone-else", caller.NOT_ADMITTED),
    ],
)
def test_a_route_admits_only_a_live_route_token_for_this_connector(
    token: str | None, refusal: str
) -> None:
    # @spec ADR-0240 d3: a cct never reaches a route, and a route token for
    # another connector is no better than a malformed one.
    if token == "connector:other":
        token = _route_token(connector="other")
    elif token == "expired":
        token = _route_token(exp=_NOW)
    elif token == "agent:someone-else":
        token = _route_token(agent="someone-else")
    headers = {} if token is None else {caller.HEADER: token}

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status, answer, _ = await _post(proxy, "/v1/review", b"{}", headers)
            assert status == server.REFUSAL_STATUS
            assert json.loads(answer) == server.refusal_body(refusal)
            assert upstream.seen == []

    _run(go)


@pytest.mark.parametrize("path", ["/mcp", "/v1/other", "/v1/review/extra", "/v1/Review"])
def test_a_route_token_reaches_no_other_path(path: str) -> None:
    # @spec ADR-0240 d3: matched exactly, so a route token cannot reach MCP,
    # OAuth discovery or an undeclared path.
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status, answer, _ = await _post(proxy, path, b"{}", {caller.HEADER: _route_token()})
            assert status == server.REFUSAL_STATUS
            assert json.loads(answer) == server.refusal_body(caller.INVALID)
            assert upstream.seen == []

    _run(go)


@pytest.mark.parametrize(
    "path",
    ["/v1/review", "/v1//review", "/v1/review/", "/v1/./review", "/V1/Review", "/v1/%72eview"],
)
@pytest.mark.parametrize("method", ["POST", "PUT"])
def test_no_token_reaches_another_spelling_or_method_of_a_route(path: str, method: str) -> None:
    # @spec ADR-0240 d3: a route's path, however it is spelled, is reachable
    # only as the declared route, so a lenient server cannot route a cct
    # request to the route's handler.
    if path == "/v1/review" and method == "POST":
        return

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            for token in (_CALLER_TOKEN, _route_token()):
                status, answer, _ = await _post(
                    proxy, path, b"{}", {caller.HEADER: token}, method=method
                )
                assert status == server.REFUSAL_STATUS, (path, method, token[:3])
                assert json.loads(answer) == server.refusal_body(caller.INVALID)
            assert upstream.seen == []

    _run(go)


def test_a_server_that_answers_before_the_body_ends_is_relayed() -> None:
    # @spec ADR-0240 d3: the limit refuses what the proxy would still send; a
    # server that answered first (here, refusing) is answered as it said.
    chunk = b"x" * 65536

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(_ROUTE_LIMIT // len(chunk) + 2):
            yield chunk

    early = server.RouteConfig(
        path="/early", methods=frozenset({"POST"}), max_body_bytes=_ROUTE_LIMIT
    )

    async def go() -> None:
        async with _serving(routes=(early,)) as (upstream, proxy):
            status, answer, _ = await _post(
                proxy, "/early", chunks(), {caller.HEADER: _route_token()}
            )
            assert (status, answer) in {
                (401, b"early"),
                (server.TOO_LARGE_STATUS, b"the request body is too large\n"),
            }
            assert upstream.received == []

    _run(go)


def test_a_declared_length_past_the_route_limit_never_reaches_the_server(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # @spec ADR-0240 d3
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status, answer, _ = await _post(
                proxy, "/v1/review", b"x" * (_ROUTE_LIMIT + 1), {caller.HEADER: _route_token()}
            )
            assert status == server.TOO_LARGE_STATUS
            assert answer == b"the request body is too large\n"
            assert upstream.seen == []

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "outcome=too_large method=POST path='/v1/review' route='/v1/review'" in text
    assert "ccr." not in text


def test_a_chunked_body_past_the_route_limit_is_refused() -> None:
    # @spec ADR-0240 d3: counted as it is relayed; the caller gets the refusal,
    # never the server's answer to a truncated body.
    chunk = b"x" * 65536

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(_ROUTE_LIMIT // len(chunk) + 2):
            yield chunk

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            status, answer, _ = await _post(
                proxy, "/v1/review", chunks(), {caller.HEADER: _route_token()}
            )
            assert status == server.TOO_LARGE_STATUS
            assert answer == b"the request body is too large\n"
            # The cut-off body reached the server, and never past the limit.
            assert upstream.received and all(0 < n <= _ROUTE_LIMIT for n in upstream.received)
            assert upstream.seen == []

    _run(go)


def test_an_undeclared_put_past_one_mebibyte_never_reaches_the_server() -> None:
    # @spec ADR-0240 d3: the 1 MiB limit now holds for every method.
    async def go() -> None:
        async with _serving(routes=()) as (upstream, proxy):
            status, answer, _ = await _post(
                proxy,
                "/upload",
                b"x" * (1024 * 1024 + 1),
                {caller.HEADER: _CALLER_TOKEN},
                method="PUT",
            )
            assert status == server.REFUSAL_STATUS
            assert answer == b"the request body is too large\n"
            assert upstream.seen == []

    _run(go)


def test_an_undeclared_chunked_put_past_one_mebibyte_is_refused() -> None:
    # @spec ADR-0240 d3
    chunk = b"x" * 65536

    async def chunks() -> AsyncIterator[bytes]:
        for _ in range(17):
            yield chunk

    async def go() -> None:
        async with _serving(routes=()) as (upstream, proxy):
            status, _, _ = await _post(
                proxy, "/upload", chunks(), {caller.HEADER: _CALLER_TOKEN}, method="PUT"
            )
            assert status == server.REFUSAL_STATUS
            assert upstream.received and all(0 < n <= 1024 * 1024 for n in upstream.received)
            assert upstream.seen == []

    _run(go)


def test_an_undeclared_put_within_one_mebibyte_is_streamed() -> None:
    # @spec ADR-0240 d3
    body = b"y" * 4096

    async def go() -> None:
        async with _serving(routes=()) as (upstream, proxy):
            status, _, _ = await _post(
                proxy, "/upload", body, {caller.HEADER: _CALLER_TOKEN}, method="PUT"
            )
            assert status == 201
            [seen] = upstream.seen
            assert seen["body"] == body

    _run(go)


_BASE_ENV = {
    server.LISTEN_PORT_ENV: "8480",
    server.UPSTREAM_PORT_ENV: "8000",
    server.PUBLIC_KEYS_ENV: str(_VECTOR["public"]),
    server.ADMITS_ENV: '["acme-dev"]',
}


def test_the_render_env_parses_the_routes() -> None:
    # @spec ADR-0240 d3
    config = server.ProxyConfig.from_env(
        {
            **_BASE_ENV,
            server.ROUTES_ENV: json.dumps(
                [{"path": "/v1/review", "methods": ["POST", "PUT"], "max_body_bytes": 5}]
            ),
        }
    )
    assert config.routes == (
        server.RouteConfig(path="/v1/review", methods=frozenset({"POST", "PUT"}), max_body_bytes=5),
    )
    assert server.ProxyConfig.from_env(_BASE_ENV).routes == ()


@pytest.mark.parametrize(
    "value",
    [
        "not json",
        "{}",
        '[{"path": "/a", "methods": ["POST"]}]',
        '[{"path": "/a", "methods": ["GET"], "max_body_bytes": 5}]',
        '[{"path": "/a", "methods": [], "max_body_bytes": 5}]',
        '[{"path": "a", "methods": ["POST"], "max_body_bytes": 5}]',
        '[{"path": "/a", "methods": ["POST"], "max_body_bytes": 0}]',
        '[{"path": "/a", "methods": ["POST"], "max_body_bytes": true}]',
        '[{"path": "/a", "methods": ["POST"], "max_body_bytes": 268435457}]',
        '[{"path": "/a", "methods": ["POST"], "max_body_bytes": 5, "callers": ["x"]}]',
        '[{"path": "/a", "methods": ["POST"], "max_body_bytes": 5},'
        ' {"path": "/a", "methods": ["PUT"], "max_body_bytes": 5}]',
    ],
)
def test_a_bad_routes_env_is_refused_naming_the_variable(value: str) -> None:
    # @spec ADR-0240 d3
    with pytest.raises(ValueError, match=server.ROUTES_ENV):
        server.ProxyConfig.from_env({**_BASE_ENV, server.ROUTES_ENV: value})


def test_a_route_token_names_its_connector_and_the_caller_claims() -> None:
    # @spec ADR-0240 d2: ccr carries cct's claims plus one connector, and is
    # refused wherever a cct is checked.
    keys = (caller.public_key(str(_VECTOR["public"])),)
    token = _route_token(run=_RUN, work_item=_WORK_ITEM)
    assert token.startswith("ccr.")
    decision = caller.decide_route(
        keys, token, connector=_CONNECTOR, admits=frozenset({"acme-dev"}), now=_NOW
    )
    assert decision == caller.Decision(
        agent="acme-dev", refusal=None, run=_RUN, work_item=_WORK_ITEM
    )
    assert (
        caller.decide(keys, token, admits=frozenset({"acme-dev"}), now=_NOW).refusal
        == caller.INVALID
    )
    with pytest.raises(ValueError):
        caller_token.mint_route(_SEED, agent="acme-dev", connector="", exp=_EXP)
    with pytest.raises(ValueError):
        caller_token.mint_route(_SEED, agent="acme-dev", connector="c", exp=_EXP, run=_RUN)
