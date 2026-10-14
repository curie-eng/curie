"""The caller proxy in front of each hosted connector (ADR-0168 decision 7).

Driven over real sockets: a real aiohttp upstream stands in for the connector
server, so what reaches the server, and what never does, is observed rather
than assumed. The refusal shape is frozen in
tests/vectors/connector-caller-refusal.json, which the runner reads too.
"""

from __future__ import annotations

import asyncio
import gzip
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
_GRANT_SEED = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
_GATED_TOOLS = ("github/merge_pull_request",)
_CONNECTOR = "github"
_MERGE_CALL = (
    b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
    b'"params":{"name":"merge_pull_request","arguments":{"n":1}}}'
)


def _canonical(arguments: dict[str, Any]) -> str:
    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class MemoryGrantStore:
    """Shared stand-in for the grant spend. True only the first time a jti is spent."""

    def __init__(self) -> None:
        self._spent: set[str] = set()

    def spend(self, jti: str, ttl: int) -> bool:
        del ttl
        if jti in self._spent:
            return False
        self._spent.add(jti)
        return True


def _grant_required_body() -> dict[str, Any]:
    return next(v["body"] for v in _REFUSAL["vectors"] if v["refusal"] == "grant_required")


def _mint_grant(tool: str, arguments: dict[str, Any], *, jti: str) -> str:
    from curie_worker.connector_grant import mint

    token = mint(
        _GRANT_SEED,
        agent="acme-dev",
        connector=_CONNECTOR,
        tool=tool,
        args=_canonical(arguments),
        exp=int(_VECTOR["exp"]),
        jti=jti,
    )
    prefix, _payload, _signature = token.split(".")
    assert prefix == "ccg"
    return token


def _config(
    upstream_port: int,
    *,
    admits: frozenset[str] = frozenset({"acme-dev"}),
    gated_tools: tuple[str, ...] = (),
    connector: str = "",
    grant_store: Any | None = None,
) -> server.ProxyConfig:
    # Only the grant tests pass these. An empty gate must keep today's constructor
    # call, or every existing refusal would fail before its own reason.
    extra: dict[str, Any] = {}
    if gated_tools:
        extra["gated_tools"] = gated_tools
    if connector:
        extra["connector"] = connector
    if grant_store is not None:
        extra["grant_store"] = grant_store
    return server.ProxyConfig(
        listen_port=8480,
        upstream_port=upstream_port,
        public_keys=(caller.public_key(str(_VECTOR["public"])),),
        admits=admits,
        **extra,
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
    gated_tools: tuple[str, ...] = (),
    connector: str = "",
    grant_store: Any | None = None,
) -> AsyncIterator[tuple[_Upstream, TestServer]]:
    upstream = _Upstream()
    upstream_server = TestServer(upstream.app, handler_cancellation=True)
    # The stand-in server keeps what it was sent as sent, so a body the proxy
    # altered on the way cannot be decoded back into looking unchanged. Handler
    # options reach aiohttp through start_server; TestServer() drops them.
    await upstream_server.start_server(auto_decompress=False)
    proxy = TestServer(
        server.make_app(
            _config(
                upstream_port or upstream_server.port or 0,
                admits=admits,
                gated_tools=gated_tools,
                connector=connector,
                grant_store=grant_store,
            ),
            clock=lambda: clock,
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
    assert set(_REFUSAL) == {
        "comment",
        "header",
        "status",
        "content_type",
        "unpaired",
        "vectors",
    }
    unpaired = _REFUSAL["unpaired"]
    assert set(unpaired) == {"why", "token", "refusal"}
    assert unpaired["refusal"] == caller.INVALID
    assert [v["refusal"] for v in _REFUSAL["vectors"]] == list(caller.REFUSALS)
    assert caller.REFUSALS[-1] == caller.GRANT_REQUIRED == "grant_required"
    assert server.GRANT_HEADER == "X-Curie-Connector-Grant"
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
    # An empty gate does not refuse, and {"x": 1} is not a tools/call, so this
    # refusal is armed only by a gated tools/call with no grant header.
    gated = refusal == "grant_required"

    async def go() -> None:
        async with _serving(
            admits=frozenset({"someone-else"})
            if refusal == caller.NOT_ADMITTED
            else frozenset({"acme-dev"}),
            clock=int(_VECTOR["exp"]) if refusal == caller.EXPIRED else _BEFORE_EXPIRY,
            gated_tools=_GATED_TOOLS if gated else (),
            connector=_CONNECTOR if gated else "",
        ) as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                if gated:
                    posted = client.post(
                        proxy.make_url("/mcp"),
                        data=_MERGE_CALL,
                        headers={**headers, "Content-Type": "application/json"},
                    )
                else:
                    posted = client.post(proxy.make_url("/mcp"), json={"x": 1}, headers=headers)
                async with posted as answer:
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
            assert seen["headers"][caller.AGENT_HEADER] == "acme-dev"
            assert caller.RUN_HEADER not in seen["headers"]
            assert caller.WORK_ITEM_HEADER not in seen["headers"]
            assert seen["headers"]["Authorization"] == "Bearer example-connector-credential"
            assert seen["headers"]["Mcp-Session-Id"] == "session-0"
            # The Host the sandbox dialled, which is what a server's
            # allowed-hosts list names; never the loopback address.
            assert seen["headers"]["Host"] == f"{proxy.host}:{proxy.port}"

    _run(go)


# @spec ADR-0178 d5
def test_a_run_token_sets_identity_headers_and_drops_forged_ones() -> None:
    run_vector = next(v for v in _TOKENS["vectors"] if v["name"] == "a_work_item_run")

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    proxy.make_url("/mcp"),
                    data=b"{}",
                    headers={
                        caller.HEADER: str(run_vector["minted"]),
                        caller.RUN_HEADER: "forged-run",
                        caller.WORK_ITEM_HEADER: "forged-item",
                        caller.AGENT_HEADER: "forged-agent",
                    },
                ) as answer:
                    assert answer.status == 201
            [seen] = upstream.seen
            assert seen["headers"][caller.AGENT_HEADER] == "acme-dev"
            assert seen["headers"][caller.RUN_HEADER] == run_vector["run"]
            assert seen["headers"][caller.WORK_ITEM_HEADER] == run_vector["work_item"]
            assert caller.HEADER not in seen["headers"]
            assert "forged-run" not in seen["headers"].values()

    _run(go)


# @spec ADR-0178 d1
def test_an_unpaired_run_claim_is_the_invalid_refusal() -> None:
    unpaired = _REFUSAL["unpaired"]
    invalid = next(v["body"] for v in _REFUSAL["vectors"] if v["refusal"] == caller.INVALID)

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    proxy.make_url("/mcp"),
                    data=b"{}",
                    headers={caller.HEADER: str(unpaired["token"])},
                ) as answer:
                    assert answer.status == _REFUSAL["status"]
                    assert await answer.json() == invalid
            assert upstream.seen == []

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


# @spec ADR-0168 d7
def test_an_encoded_body_reaches_the_server_byte_for_byte() -> None:
    plain = b'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"pad":"'
    plain += b"a" * 2000 + b'"}}'
    encoded = gzip.compress(plain)

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                async with client.post(
                    proxy.make_url("/mcp"),
                    data=encoded,
                    headers={
                        caller.HEADER: _TOKEN,
                        "Content-Encoding": "gzip",
                        "Content-Type": "application/json",
                    },
                ) as answer:
                    assert answer.status == 201
            [seen] = upstream.seen
            # Forwarded as sent: the proxy never decodes a body it relays, so
            # the length and the encoding it passes on still describe it.
            assert seen["body"] == encoded
            assert seen["headers"]["Content-Encoding"] == "gzip"
            assert seen["headers"]["Content-Length"] == str(len(encoded))

    _run(go)


# @spec ADR-0168 d7
def test_a_path_cannot_forge_a_second_log_line(caplog: pytest.LogCaptureFixture) -> None:
    forged = "caller=a outcome=admitted method=GET path=/mcp"

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            async with aiohttp.ClientSession() as client:
                path = "/a%0Acaller=a%20outcome=admitted%20method=GET%20path=/mcp"
                async with client.get(proxy.make_url(path)) as answer:
                    assert answer.status == server.REFUSAL_STATUS
            assert upstream.seen == []

    with caplog.at_level(logging.INFO, logger="curie_connector_proxy"):
        _run(go)
    [message] = [r.getMessage() for r in caplog.records if r.name == "curie_connector_proxy"]
    assert "\n" not in message
    assert message.startswith(f"caller=- outcome={caller.MISSING} method=GET path=")
    assert not any(line.startswith(forged) for line in caplog.text.splitlines())


# @spec ADR-0168 d7
def test_a_request_the_parser_refuses_logs_no_caller_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # An obs-folded header is a parse error, and aiohttp's own error log quotes
    # the offending bytes, which here are the token.
    raw = (
        b"GET /mcp HTTP/1.1\r\nHost: connector\r\n"
        + caller.HEADER.encode()
        + b": a\r\n "
        + _TOKEN.encode()
        + b"\r\n\r\n"
    )

    async def go() -> None:
        async with _serving() as (upstream, proxy):
            reader, writer = await asyncio.open_connection(proxy.host, proxy.port)
            writer.write(raw)
            await writer.drain()
            answer = await asyncio.wait_for(reader.read(), timeout=5)
            writer.close()
            assert answer.startswith(b"HTTP/1.0 400") or answer.startswith(b"HTTP/1.1 400")
            assert upstream.seen == []

    with caplog.at_level(logging.DEBUG):
        _run(go)
    # Not vacuous: the parser's refusal was logged, only without the token.
    assert any("Error handling request" in r.getMessage() for r in caplog.records)
    assert "cct." not in caplog.text
    for segment in _TOKEN.split(".")[1:]:
        assert segment[:16] not in caplog.text


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


def _caller_headers(grant: str | None = None) -> dict[str, str]:
    headers = {caller.HEADER: _TOKEN}
    if grant is not None:
        headers[server.GRANT_HEADER] = grant
    return headers


async def _post(proxy: TestServer, body: bytes, headers: dict[str, str]) -> tuple[int, str, Any]:
    async with aiohttp.ClientSession() as client:
        async with client.post(
            proxy.make_url("/mcp"),
            data=body,
            headers={"Content-Type": "application/json", **headers},
        ) as answer:
            return answer.status, answer.content_type, await answer.json()


def _gated(**kwargs: Any) -> Any:
    return _serving(gated_tools=_GATED_TOOLS, connector=_CONNECTOR, **kwargs)


# A gated tools/call needs one matching grant. Other methods and ungated tools
# stay on the forward path, and the posted bytes are what the server receives.


def test_a_gated_tools_call_without_a_grant_is_refused_and_never_reaches_the_server() -> None:
    async def go() -> None:
        async with _gated() as (upstream, proxy):
            status, content_type, payload = await _post(proxy, _MERGE_CALL, _caller_headers())
            assert status == _REFUSAL["status"]
            assert content_type == _REFUSAL["content_type"]
            assert payload == _grant_required_body()
            assert upstream.seen == []

    _run(go)


def test_a_matching_grant_is_forwarded_once_and_a_replay_is_refused() -> None:
    grant = _mint_grant("merge_pull_request", {"n": 1}, jti="jti-1")
    headers = _caller_headers(grant)

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, _content_type, _payload = await _post(proxy, _MERGE_CALL, headers)
            assert status == 201
            assert [seen["body"] for seen in upstream.seen] == [_MERGE_CALL]
            assert caller.HEADER not in upstream.seen[0]["headers"]
            assert server.GRANT_HEADER not in upstream.seen[0]["headers"]
            status, content_type, payload = await _post(proxy, _MERGE_CALL, headers)
            assert status == _REFUSAL["status"]
            assert content_type == _REFUSAL["content_type"]
            assert payload == _grant_required_body()
            assert len(upstream.seen) == 1

    _run(go)


def test_a_second_proxy_sharing_the_grant_store_rejects_the_same_jti() -> None:
    grant = _mint_grant("merge_pull_request", {"n": 1}, jti="jti-1")
    headers = _caller_headers(grant)
    store = MemoryGrantStore()

    async def go() -> None:
        async with _gated(grant_store=store) as (first_upstream, first_proxy):
            status, _content_type, _payload = await _post(first_proxy, _MERGE_CALL, headers)
            assert status == 201
            assert [seen["body"] for seen in first_upstream.seen] == [_MERGE_CALL]
            async with _gated(grant_store=store) as (second_upstream, second_proxy):
                status, content_type, payload = await _post(second_proxy, _MERGE_CALL, headers)
                assert status == _REFUSAL["status"]
                assert content_type == _REFUSAL["content_type"]
                assert payload == _grant_required_body()
                assert second_upstream.seen == []
            assert len(first_upstream.seen) == 1

    _run(go)


@pytest.mark.parametrize(
    ("tool", "arguments", "jti"),
    [("other_tool", {"n": 1}, "jti-tool"), ("merge_pull_request", {"n": 2}, "jti-args")],
)
def test_a_grant_for_another_tool_or_other_arguments_never_reaches_the_server(
    tool: str, arguments: dict[str, int], jti: str
) -> None:
    grant = _mint_grant(tool, arguments, jti=jti)

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, content_type, payload = await _post(proxy, _MERGE_CALL, _caller_headers(grant))
            assert status == _REFUSAL["status"]
            assert content_type == _REFUSAL["content_type"]
            assert payload == _grant_required_body()
            assert upstream.seen == []

    _run(go)


def test_an_ungated_tools_call_is_forwarded_without_a_grant() -> None:
    body = (
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
        b'"params":{"name":"list_things","arguments":{}}}'
    )

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, _content_type, _payload = await _post(proxy, body, _caller_headers())
            assert status == 201
            assert [seen["body"] for seen in upstream.seen] == [body]

    _run(go)


def test_tools_list_is_forwarded_without_a_grant() -> None:
    body = b'{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, _content_type, _payload = await _post(proxy, body, _caller_headers())
            assert status == 201
            assert [seen["body"] for seen in upstream.seen] == [body]

    _run(go)


def test_a_post_that_is_not_a_tools_call_is_forwarded_without_a_grant() -> None:
    body = b'{"x":1}'

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, _content_type, _payload = await _post(proxy, body, _caller_headers())
            assert status == 201
            assert [seen["body"] for seen in upstream.seen] == [body]

    _run(go)


def test_a_batched_gated_tools_call_without_a_grant_never_reaches_the_server() -> None:
    # The gated call is the second array element.
    body = (
        b'[{"jsonrpc":"2.0","id":1,"method":"tools/list"},'
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
        b'"params":{"name":"merge_pull_request","arguments":{"n":1}}}]'
    )

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, content_type, payload = await _post(proxy, body, _caller_headers())
            assert status == _REFUSAL["status"]
            assert content_type == _REFUSAL["content_type"]
            assert payload == _grant_required_body()
            assert upstream.seen == []

    _run(go)


def test_a_batched_gated_tools_call_forwards_once_when_the_grant_matches() -> None:
    # The gated call is the second array element.
    body = (
        b'[{"jsonrpc":"2.0","id":1,"method":"tools/list"},'
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call",'
        b'"params":{"name":"merge_pull_request","arguments":{"n":1}}}]'
    )
    grant = _mint_grant("merge_pull_request", {"n": 1}, jti="jti-batch")
    headers = _caller_headers(grant)

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, _content_type, _payload = await _post(proxy, body, headers)
            assert status == 201
            assert [seen["body"] for seen in upstream.seen] == [body]
            assert caller.HEADER not in upstream.seen[0]["headers"]
            assert server.GRANT_HEADER not in upstream.seen[0]["headers"]
            status, content_type, payload = await _post(proxy, body, headers)
            assert status == _REFUSAL["status"]
            assert content_type == _REFUSAL["content_type"]
            assert payload == _grant_required_body()
            assert len(upstream.seen) == 1

    _run(go)


def test_two_gated_calls_in_one_batch_are_refused_without_spending_the_grant() -> None:
    body = (
        b'[{"jsonrpc":"2.0","id":1,"method":"tools/call",'
        b'"params":{"name":"merge_pull_request","arguments":{"n":1}}},'
        b'{"jsonrpc":"2.0","id":2,"method":"tools/call",'
        b'"params":{"name":"merge_pull_request","arguments":{"n":1}}}]'
    )
    grant = _mint_grant("merge_pull_request", {"n": 1}, jti="jti-two")
    headers = _caller_headers(grant)

    async def go() -> None:
        async with _gated(grant_store=MemoryGrantStore()) as (upstream, proxy):
            status, content_type, payload = await _post(proxy, body, headers)
            assert status == _REFUSAL["status"]
            assert content_type == _REFUSAL["content_type"]
            assert payload == _grant_required_body()
            assert upstream.seen == []
            status, _content_type, _payload = await _post(proxy, _MERGE_CALL, headers)
            assert status == 201
            assert [seen["body"] for seen in upstream.seen] == [_MERGE_CALL]

    _run(go)
