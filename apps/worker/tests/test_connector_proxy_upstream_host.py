"""The Host the caller proxy presents upstream (Draft ADR 0229).

Driven over real sockets against a stand-in server that applies the MCP Go SDK
rule the pinned kubernetes-mcp-server enforces: on a connection that arrived
on a loopback address, a Host that is not loopback is refused with 403. The
proxy forwards on loopback, so this is exactly what a proxied request meets.

The core property is decision 4: the declared Host is presented only on a
request the proxy admitted. A request the proxy refuses never reaches the
server, and a request that reaches the server without the proxy (the
`-direct` Service path) keeps its own Host, so the server's check still
applies to it.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_connector_proxy import caller, server

_ROOT = Path(__file__).resolve().parents[3]
_TOKENS = json.loads(
    (_ROOT / "tests" / "vectors" / "connector-caller-token.json").read_text(encoding="utf-8")
)
_VECTOR = next(v for v in _TOKENS["vectors"] if v["name"] == "a_plain_agent_name")
_TOKEN = str(_VECTOR["minted"])
_BEFORE_EXPIRY = int(_VECTOR["exp"]) - 10
# What the sandbox dials: the connector's Service, never loopback.
_SERVICE_HOST = "curie-acme-bot-mcp-kubernetes:8000"
_INITIALIZE = b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'


def _loopback_name(host: str) -> bool:
    name = host[1 : host.index("]")] if host.startswith("[") else host.rsplit(":", 1)[0]
    if name == "localhost":
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


class _LoopbackGuardedServer:
    """Refuses a non-loopback Host on a loopback connection, and records the rest."""

    def __init__(self) -> None:
        self.seen: list[dict[str, Any]] = []
        self.refused: list[str] = []
        self.app = web.Application()
        self.app.router.add_route("*", "/{tail:.*}", self._handle)

    async def _handle(self, request: web.Request) -> web.Response:
        local = request.transport.get_extra_info("sockname")[0] if request.transport else ""
        host = request.headers.get("Host", "")
        if ipaddress.ip_address(local).is_loopback and not _loopback_name(host):
            self.refused.append(host)
            return web.Response(status=403, text=f'Forbidden: invalid Host header "{host}"\n')
        self.seen.append({"host": host, "headers": request.headers.copy()})
        return web.json_response({"jsonrpc": "2.0", "id": 1, "result": {}})


def _config(upstream_port: int, upstream_host: str | None) -> server.ProxyConfig:
    extra: dict[str, Any] = {}
    if upstream_host is not None:
        extra["upstream_host"] = upstream_host
    return server.ProxyConfig(
        listen_port=8480,
        upstream_port=upstream_port,
        public_keys=(caller.public_key(str(_VECTOR["public"])),),
        admits=frozenset({"acme-dev"}),
        **extra,
    )


@asynccontextmanager
async def _serving(
    upstream_host: str | None,
) -> AsyncIterator[tuple[_LoopbackGuardedServer, TestServer, TestServer]]:
    stand_in = _LoopbackGuardedServer()
    stand_in_server = TestServer(stand_in.app, host="127.0.0.1")
    await stand_in_server.start_server()
    proxy = TestServer(
        server.make_app(
            _config(stand_in_server.port or 0, upstream_host), clock=lambda: _BEFORE_EXPIRY
        ),
        host="127.0.0.1",
    )
    await proxy.start_server()
    try:
        yield stand_in, stand_in_server, proxy
    finally:
        await proxy.close()
        await stand_in_server.close()


async def _post(port: int, headers: dict[str, str]) -> tuple[int, bytes]:
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            f"http://127.0.0.1:{port}/mcp",
            data=_INITIALIZE,
            headers={"Host": _SERVICE_HOST, "Content-Type": "application/json", **headers},
        ) as answer,
    ):
        return answer.status, await answer.read()


def _run(body: Callable[[], Coroutine[Any, Any, None]]) -> None:
    asyncio.run(body())


# @spec ADR-0229 d1
def test_without_a_declaration_the_caller_host_reaches_the_server_and_is_refused() -> None:
    # Today's behavior, kept as the default: the proxy forwards on loopback with
    # the Service Host, and a loopback-guarded server refuses it.
    async def go() -> None:
        async with _serving(None) as (stand_in, _server, proxy):
            status, body = await _post(proxy.port or 0, {caller.HEADER: _TOKEN})
            assert status == 403
            assert b"invalid Host header" in body
            assert stand_in.refused == [_SERVICE_HOST]

    _run(go)


# @spec ADR-0229 d3
@pytest.mark.parametrize(
    ("declared", "presented"),
    [("localhost", "localhost"), ("127.0.0.1", "127.0.0.1"), ("::1", "[::1]")],
)
def test_an_admitted_request_presents_the_declared_host(declared: str, presented: str) -> None:
    async def go() -> None:
        async with _serving(declared) as (stand_in, stand_in_server, proxy):
            status, _body = await _post(proxy.port or 0, {caller.HEADER: _TOKEN})
            assert status == 200
            assert stand_in.refused == []
            [seen] = stand_in.seen
            assert seen["host"] == f"{presented}:{stand_in_server.port}"
            # Nothing else about decision 5 changes: the token stays stripped
            # and the identity header comes from the verified claims.
            assert caller.HEADER not in seen["headers"]
            assert seen["headers"][caller.AGENT_HEADER] == "acme-dev"
            assert seen["headers"]["Content-Type"] == "application/json"

    _run(go)


# @spec ADR-0229 d3, d4
@pytest.mark.parametrize(
    "headers",
    [{}, {caller.HEADER: "cct.not.a-token"}],
    ids=["no_token", "bad_token"],
)
def test_a_refused_request_never_reaches_the_server(headers: dict[str, str]) -> None:
    async def go() -> None:
        async with _serving("localhost") as (stand_in, _server, proxy):
            status, body = await _post(proxy.port or 0, headers)
            assert status == server.REFUSAL_STATUS
            assert json.loads(body)["error"]["data"]["curie_caller"] in {
                caller.MISSING,
                caller.INVALID,
            }
            assert stand_in.seen == []
            assert stand_in.refused == []

    _run(go)


# @spec ADR-0229 d4
def test_an_unadmitted_agent_never_reaches_the_server() -> None:
    async def go() -> None:
        stand_in = _LoopbackGuardedServer()
        stand_in_server = TestServer(stand_in.app, host="127.0.0.1")
        await stand_in_server.start_server()
        config = server.ProxyConfig(
            listen_port=8480,
            upstream_port=stand_in_server.port or 0,
            public_keys=(caller.public_key(str(_VECTOR["public"])),),
            admits=frozenset({"someone-else"}),
            upstream_host="localhost",
        )
        proxy = TestServer(server.make_app(config, clock=lambda: _BEFORE_EXPIRY))
        await proxy.start_server()
        try:
            status, _body = await _post(proxy.port or 0, {caller.HEADER: _TOKEN})
            assert status == server.REFUSAL_STATUS
            assert stand_in.seen == [] and stand_in.refused == []
        finally:
            await proxy.close()
            await stand_in_server.close()

    _run(go)


# @spec ADR-0229 d4
def test_a_direct_request_keeps_its_host_and_meets_the_servers_own_check() -> None:
    # The `-direct` Service reaches the server's own port without the proxy.
    # The declaration lives in the proxy's admitted path only, so a direct
    # caller presenting the Service Host is still refused by the server.
    async def go() -> None:
        async with _serving("localhost") as (stand_in, stand_in_server, proxy):
            proxied, _ = await _post(proxy.port or 0, {caller.HEADER: _TOKEN})
            direct, body = await _post(stand_in_server.port or 0, {})
            assert proxied == 200
            assert direct == 403
            assert b"invalid Host header" in body
            assert stand_in.refused == [_SERVICE_HOST]

    _run(go)


# @spec ADR-0229 d3
def test_a_caller_cannot_strip_the_declared_host_through_connection() -> None:
    async def go() -> None:
        async with _serving("localhost") as (stand_in, stand_in_server, proxy):
            status, _ = await _post(
                proxy.port or 0, {caller.HEADER: _TOKEN, "Connection": "keep-alive, Host"}
            )
            assert status == 200
            assert stand_in.seen[0]["host"] == f"localhost:{stand_in_server.port}"

    _run(go)


def _env(**extra: str) -> dict[str, str]:
    return {
        server.LISTEN_PORT_ENV: "8480",
        server.UPSTREAM_PORT_ENV: "8000",
        server.PUBLIC_KEYS_ENV: str(_VECTOR["public"]),
        server.ADMITS_ENV: '["acme-dev"]',
        **extra,
    }


# @spec ADR-0229 d2
def test_the_env_omits_the_upstream_host_by_default() -> None:
    assert server.ProxyConfig.from_env(_env()).upstream_host is None


# @spec ADR-0229 d1, d2
@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_the_env_carries_a_loopback_upstream_host(host: str) -> None:
    config = server.ProxyConfig.from_env(_env(**{server.UPSTREAM_HOST_ENV: host}))
    assert config.upstream_host == host


# @spec ADR-0229 d1
@pytest.mark.parametrize(
    "host", ["example.com", "10.0.0.1", "localhost:8000", "[::1]", "0.0.0.0", "local\nhost"]
)
def test_the_env_refuses_a_non_loopback_upstream_host(host: str) -> None:
    with pytest.raises(ValueError, match=server.UPSTREAM_HOST_ENV):
        server.ProxyConfig.from_env(_env(**{server.UPSTREAM_HOST_ENV: host}))
