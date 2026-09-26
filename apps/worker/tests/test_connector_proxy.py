"""The caller proxy in front of each hosted connector (ADR-0168 decision 7).

Driven over real sockets: a real aiohttp upstream stands in for the connector
server, so what reaches the server, and what never does, is observed rather
than assumed. The refusal shape is frozen in
tests/vectors/connector-caller-refusal.json, which the runner reads too.
"""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_connector_proxy import __main__ as proxy_main
from curie_connector_proxy import caller, server

_ROOT = Path(__file__).resolve().parents[3]
_TOKENS = json.loads(
    (_ROOT / "tests" / "vectors" / "connector-caller-token.json").read_text(encoding="utf-8")
)
_REFUSAL = json.loads(
    (_ROOT / "tests" / "vectors" / "connector-caller-refusal.json").read_text(encoding="utf-8")
)
_VECTOR = next(v for v in _TOKENS["vectors"] if v["name"] == "a_plain_agent_name")
_TOKEN = str(_VECTOR["minted"])
_BEFORE_EXPIRY = int(_VECTOR["exp"]) - 10


def _config(
    upstream_port: int, *, admits: frozenset[str] = frozenset({"acme-dev"})
) -> server.ProxyConfig:
    return server.ProxyConfig(
        listen_port=8480,
        upstream_port=upstream_port,
        public_keys=(caller.public_key(str(_VECTOR["public"])),),
        admits=admits,
    )


class _Upstream:
    """A connector server that records every request it is sent."""

    def __init__(self) -> None:
        self.seen: list[dict[str, Any]] = []
        self.stream_closed = asyncio.Event()
        self.app = web.Application()
        self.app.router.add_get("/stream", self._stream)
        self.app.router.add_route("*", "/{tail:.*}", self._echo)

    async def _echo(self, request: web.Request) -> web.Response:
        self.seen.append(
            {
                "method": request.method,
                "path_qs": request.raw_path,
                "headers": request.headers.copy(),
                "body": await request.read(),
            }
        )
        return web.Response(
            status=201,
            body=b'{"ok":true}',
            content_type="application/json",
            headers={"Mcp-Session-Id": "session-1"},
        )

    async def _stream(self, request: web.Request) -> web.StreamResponse:
        self.seen.append(
            {"method": "GET", "path_qs": request.raw_path, "headers": request.headers.copy()}
        )
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"data: first\n\n")
        try:
            await asyncio.sleep(30)
        finally:
            self.stream_closed.set()
        return response


@asynccontextmanager
async def _serving(
    *,
    admits: frozenset[str] = frozenset({"acme-dev"}),
    clock: float = _BEFORE_EXPIRY,
    upstream_port: int | None = None,
) -> AsyncIterator[tuple[_Upstream, TestServer]]:
    upstream = _Upstream()
    upstream_server = TestServer(upstream.app, handler_cancellation=True)
    await upstream_server.start_server()
    proxy = TestServer(
        server.make_app(
            _config(upstream_port or upstream_server.port or 0, admits=admits), clock=lambda: clock
        ),
        handler_cancellation=True,
    )
    await proxy.start_server()
    try:
        yield upstream, proxy
    finally:
        await proxy.close()
        await upstream_server.close()


def _run(body: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(body())


# @spec ADR-0168 d7
def test_the_refusal_vector_carries_only_known_keys() -> None:
    assert set(_REFUSAL) == {"comment", "header", "status", "content_type", "vectors"}
    assert [v["refusal"] for v in _REFUSAL["vectors"]] == list(caller.REFUSALS)
    for vector in _REFUSAL["vectors"]:
        assert set(vector) == {"refusal", "body"}
    assert _REFUSAL["header"] == caller.HEADER
    assert _REFUSAL["status"] == server.REFUSAL_STATUS


# @spec ADR-0168 d7
@pytest.mark.parametrize("vector", _REFUSAL["vectors"], ids=lambda v: v["refusal"])
def test_each_refusal_answers_the_frozen_body_and_never_reaches_the_server(
    vector: dict[str, Any],
) -> None:
    refusal = vector["refusal"]
    headers = {} if refusal == caller.MISSING else {caller.HEADER: _TOKEN}
    if refusal == caller.INVALID:
        headers = {caller.HEADER: "cct.not.valid"}

    async def go() -> None:
        async with _serving(
            admits=frozenset({"someone-else"})
            if refusal == caller.NOT_ADMITTED
            else frozenset({"acme-dev"}),
            clock=int(_VECTOR["exp"]) if refusal == caller.EXPIRED else _BEFORE_EXPIRY,
        ) as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    proxy.make_url("/mcp"), json={"x": 1}, headers=headers
                ) as answer:
                    assert answer.status == _REFUSAL["status"]
                    assert answer.content_type == _REFUSAL["content_type"]
                    # A 401 challenge sends the bundled CLI into OAuth discovery
                    # against the connector; a refusal is not a login prompt.
                    assert "WWW-Authenticate" not in answer.headers
                    assert await answer.json() == vector["body"]
            assert upstream.seen == []

    _run(go)


# @spec ADR-0168 d7
def test_an_admitted_request_reaches_the_server_unchanged_except_the_token() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    proxy.make_url("/mcp?x=1&y=a%20b%26c"),
                    data=b'{"jsonrpc":"2.0","id":1,"method":"initialize"}',
                    headers={
                        caller.HEADER: _TOKEN,
                        "Authorization": "Bearer example-connector-credential",
                        "Mcp-Session-Id": "session-0",
                        "Content-Type": "application/json",
                    },
                ) as answer:
                    assert answer.status == 201
                    assert answer.headers["Mcp-Session-Id"] == "session-1"
                    assert await answer.read() == b'{"ok":true}'
            [seen] = upstream.seen
            assert seen["method"] == "POST"
            assert seen["path_qs"] == "/mcp?x=1&y=a%20b%26c"
            assert seen["body"] == b'{"jsonrpc":"2.0","id":1,"method":"initialize"}'
            assert caller.HEADER not in seen["headers"]
            assert seen["headers"]["Authorization"] == "Bearer example-connector-credential"
            assert seen["headers"]["Mcp-Session-Id"] == "session-0"
            # The Host the sandbox dialled, which is what a server's
            # allowed-hosts list names; never the loopback address.
            assert seen["headers"]["Host"] == f"{proxy.host}:{proxy.port}"

    _run(go)


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/.well-known/oauth-protected-resource/mcp"),
        ("GET", "/.well-known/oauth-authorization-server"),
        ("GET", "/.well-known/openid-configuration"),
        ("POST", "/register"),
    ],
)
def test_oauth_discovery_paths_are_checked_like_any_other(method: str, path: str) -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.request(method, proxy.make_url(path)) as refused:
                    assert refused.status == server.REFUSAL_STATUS
                assert upstream.seen == []
                async with client.request(
                    method, proxy.make_url(path), headers={caller.HEADER: _TOKEN}
                ) as admitted:
                    assert admitted.status == 201
            assert [s["path_qs"] for s in upstream.seen] == [path]
            assert caller.HEADER not in upstream.seen[0]["headers"]

    _run(go)


# @spec ADR-0168 d7
def test_two_caller_headers_are_invalid() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    proxy.make_url("/mcp"),
                    headers=[(caller.HEADER, _TOKEN), (caller.HEADER, _TOKEN)],
                ) as answer:
                    assert answer.status == server.REFUSAL_STATUS
                    body = await answer.json()
                    assert body["error"]["data"] == {"curie_caller": caller.INVALID}
            assert upstream.seen == []

    _run(go)


# @spec ADR-0168 d7
def test_the_forwarding_client_adds_no_header_the_caller_did_not_send() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession(
                skip_auto_headers=("Accept", "Accept-Encoding", "User-Agent")
            ) as client:
                async with client.get(
                    proxy.make_url("/mcp"), headers={caller.HEADER: _TOKEN}
                ) as answer:
                    assert answer.status == 201
            [seen] = upstream.seen
            # The caller sent none of these; the proxy's own client must not
            # invent them on the hop to the server, or a caller that never
            # asked for a compressed body gets one anyway.
            for name in ("Accept", "Accept-Encoding", "User-Agent"):
                assert name not in seen["headers"]

    _run(go)


# @spec ADR-0168 d7
def test_a_path_with_an_encoded_newline_gets_the_refusal_not_a_404() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.get(proxy.make_url("/foo%0Abar")) as answer:
                    assert answer.status == server.REFUSAL_STATUS
                    assert answer.content_type == _REFUSAL["content_type"]
                    expected = next(
                        v["body"] for v in _REFUSAL["vectors"] if v["refusal"] == caller.MISSING
                    )
                    assert await answer.json() == expected
            # The server is never contacted, encoded newline or not.
            assert upstream.seen == []

    _run(go)


# @spec ADR-0168 d7
def test_a_connection_header_naming_host_does_not_drop_it() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    proxy.make_url("/mcp"),
                    headers={caller.HEADER: _TOKEN, "Connection": "Host"},
                ) as answer:
                    assert answer.status == 201
            [seen] = upstream.seen
            # Host is never hop-by-hop, whatever a caller's Connection header
            # names; the server still reads the address the sandbox dialled.
            assert seen["headers"]["Host"] == f"{proxy.host}:{proxy.port}"

    _run(go)


# @spec ADR-0168 d7
def test_hop_by_hop_headers_stop_at_the_proxy() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    proxy.make_url("/mcp"),
                    headers={
                        caller.HEADER: _TOKEN,
                        "Connection": "keep-alive, X-Hop",
                        "X-Hop": "dropped",
                        "Proxy-Authorization": "Basic dropped",
                        "X-Kept": "kept",
                    },
                ) as answer:
                    assert answer.status == 201
            headers = upstream.seen[0]["headers"]
            assert "X-Hop" not in headers
            assert "Proxy-Authorization" not in headers
            assert headers["X-Kept"] == "kept"

    _run(go)


# @spec ADR-0168 d7
def test_a_stream_is_relayed_as_it_arrives_and_closing_it_closes_the_server_side() -> None:
    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                answer = await client.get(
                    proxy.make_url("/stream"), headers={caller.HEADER: _TOKEN}
                )
                assert answer.headers["Content-Type"] == "text/event-stream"
                # The server is still inside its 30 s sleep, so a proxy that
                # buffered the body would time out here.
                first = await asyncio.wait_for(answer.content.readline(), timeout=5)
                assert first == b"data: first\n"
                answer.close()
            await asyncio.wait_for(upstream.stream_closed.wait(), timeout=5)

    _run(go)


# @spec ADR-0168 d7
def test_an_unreachable_server_is_a_bad_gateway() -> None:
    async def go() -> None:
        # A port nothing listens on: the upstream TestServer is closed first.
        closed = TestServer(web.Application())
        await closed.start_server()
        port = closed.port
        await closed.close()
        async with _serving(upstream_port=port) as (_upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.get(
                    proxy.make_url("/mcp"), headers={caller.HEADER: _TOKEN}
                ) as answer:
                    assert answer.status == 502

    _run(go)


# @spec ADR-0168 d7
def test_the_log_names_the_agent_and_the_outcome_and_never_the_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def go() -> None:
        async with _serving(admits=frozenset({"someone-else"})) as (_upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.get(proxy.make_url("/mcp"), headers={caller.HEADER: _TOKEN}):
                    pass

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "caller=acme-dev outcome=not_admitted" in text
    assert _TOKEN not in text
    assert _TOKEN.split(".")[2] not in text


def _env(**overrides: str) -> dict[str, str]:
    env = {
        server.LISTEN_PORT_ENV: "8480",
        server.UPSTREAM_PORT_ENV: "8000",
        server.PUBLIC_KEYS_ENV: f"{_VECTOR['public']},{_TOKENS['vectors'][1]['public']}",
        server.ADMITS_ENV: '["acme-dev","Acme Café"]',
    }
    env.update(overrides)
    return env


# @spec ADR-0168 d7
def test_the_render_env_parses_into_the_config() -> None:
    config = server.ProxyConfig.from_env(_env())
    assert (config.listen_port, config.upstream_port) == (8480, 8000)
    assert len(config.public_keys) == 2
    assert config.admits == frozenset({"acme-dev", "Acme Café"})


# @spec ADR-0168 d7
def test_an_empty_admits_list_parses_and_admits_nobody() -> None:
    assert server.ProxyConfig.from_env(_env(**{server.ADMITS_ENV: "[]"})).admits == frozenset()


# @spec ADR-0168 d7
@pytest.mark.parametrize(
    ("name", "value"),
    [
        (server.LISTEN_PORT_ENV, ""),
        (server.LISTEN_PORT_ENV, "0"),
        # Arabic-Indic digits for 8480: `str.isdigit()` accepts them, and
        # Python's own `int()` parses them, so a naive digit check would
        # silently admit a non-ASCII port.
        (server.LISTEN_PORT_ENV, "٨٤٨٠"),
        (server.UPSTREAM_PORT_ENV, "70000"),
        (server.PUBLIC_KEYS_ENV, ""),
        (server.PUBLIC_KEYS_ENV, "not-a-key"),
        (server.ADMITS_ENV, ""),
        (server.ADMITS_ENV, '"acme-dev"'),
        (server.ADMITS_ENV, '[""]'),
        (server.ADMITS_ENV, "[7]"),
    ],
)
def test_a_bad_render_env_is_refused_naming_the_variable(name: str, value: str) -> None:
    with pytest.raises(ValueError, match=name):
        server.ProxyConfig.from_env(_env(**{name: value}))


# @spec ADR-0168 d7
def test_the_entrypoint_refuses_to_start_without_its_env(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in (
        server.LISTEN_PORT_ENV,
        server.UPSTREAM_PORT_ENV,
        server.PUBLIC_KEYS_ENV,
        server.ADMITS_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    assert proxy_main.main() == 2
    assert server.PUBLIC_KEYS_ENV in capsys.readouterr().err


# @spec ADR-0168 d7
def test_importing_the_proxy_loads_none_of_the_worker() -> None:
    # Measured: importing through `curie_worker` loads 2297 modules and a
    # 147 MB peak RSS; aiohttp and PyNaCl alone load 342 and 45 MB.
    probe = (
        "import sys, curie_connector_proxy.server, curie_connector_proxy.__main__;"
        "heavy = [m for m in ('curie_worker', 'kubernetes', 'sqlalchemy', 'redis', 'boto3')"
        " if m in sys.modules];"
        "print(','.join(heavy))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == ""
