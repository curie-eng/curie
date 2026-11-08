"""The fake model calls one tool on one hosted connector, on request.

ADR 0270 identity E2E harness. Under ``--fake-model`` a turn whose person
message carries::

    [fake:connector-tool <connector> <tool> <json-object-args>]

dials exactly that hosted connector with its materialized config (the same
transport and retry rules as ``[fake:connector-call]``, one bounded dial), runs
``initialize`` and then ``tools/call``, and answers one line::

    connector-tool <connector>/<tool>: ok <first line of the first text> caller=<fp>
    connector-tool <connector>/<tool>: refused <code> caller=<fp>
    connector-tool <connector>/<tool>: failed <reason> caller=<fp>
    connector-tool <connector>: failed unknown_connector
    connector-tool: failed bad_directive

``[fake:forge-principal <value>]`` adds the literal header
``X-Curie-Principal: <value>`` to that dial only, on a copy of the config.

The connector is a real MCP server over streamable HTTP on a loopback uvicorn,
recording the headers of every request it receives. Nothing here sleeps.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import socket
import threading
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
import pytest
import uvicorn
from aci_protocol import Event
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.connectors import CALLER_HEADER
from curie_runner.fake import FakeModelSession
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from mcp import types
from mcp.server import ServerRequestContext
from mcp.server.lowlevel import Server

_REFUSAL = json.loads(
    (
        Path(__file__).resolve().parents[2] / "tests" / "vectors" / "connector-caller-refusal.json"
    ).read_text(encoding="utf-8")
)

_PRINCIPAL_HEADER = "X-Curie-Principal"
_CALLER_VALUE = "cct." + "fake-connector-tool-" + "z" * 40
_FORGED = "forged-principal-" + "f" * 12
_CALL_LINE = re.compile(r"^connector-tool[^\n]*$", re.MULTILINE)
_CALL_LINE_MARKER = "[fake:connector-call]"


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _server_config(url: str, caller: str | None = _CALLER_VALUE) -> dict[str, Any]:
    """A derived hosted entry after ``materialize_connector_caller_headers``."""

    config: dict[str, Any] = {"type": "http", "url": url}
    if caller is not None:
        config["headers"] = {CALLER_HEADER: caller}
    return config


def _directive(connector: str, tool: str, args: str) -> str:
    return f"[fake:connector-tool {connector} {tool} {args}]"


def _forge(value: str) -> str:
    return f"[fake:forge-principal {value}]"


# --------------------------------------------------------------------------- #
# A real MCP server over streamable HTTP: echo and refuse tools
# --------------------------------------------------------------------------- #

# The number of X-Curie-Principal header entries on the request being served,
# set by the ASGI wrapper before the MCP app spawns its handler task.
_PRINCIPAL_COUNT: ContextVar[int] = ContextVar("principal_count", default=-1)


@dataclass
class _Stub:
    url: str
    # Every HTTP request: {"method", "path", "headers"} (lowercased names) and
    # "principal_values", every X-Curie-Principal value in arrival order.
    http: list[dict[str, Any]] = field(default_factory=list)
    # The tools/call requests that reached the handler: (name, arguments).
    calls: list[tuple[str, dict[str, Any] | None]] = field(default_factory=list)


def _build_server(stub: _Stub) -> Server[Any]:
    async def list_tools(
        _context: ServerRequestContext[object], _params: types.PaginatedRequestParams | None
    ) -> types.ListToolsResult:
        return types.ListToolsResult(
            tools=[
                types.Tool(name="echo", description="Echo.", inputSchema={"type": "object"}),
                types.Tool(name="refuse", description="Refuse.", inputSchema={"type": "object"}),
            ]
        )

    async def call_tool(
        _context: ServerRequestContext[object], params: types.CallToolRequestParams
    ) -> types.CallToolResult:
        stub.calls.append((params.name, params.arguments))
        if params.name == "refuse":
            return types.CallToolResult(
                content=[types.TextContent(type="text", text="refused not_authorized")],
                isError=True,
            )
        text = (
            f"echo {json.dumps(params.arguments or {}, sort_keys=True)} "
            f"principal_headers={_PRINCIPAL_COUNT.get()}"
        )
        return types.CallToolResult(
            content=[
                # A second line the answer must not carry, and a second block.
                types.TextContent(type="text", text=text + "\nsecond line"),
                types.TextContent(type="text", text="second block"),
            ]
        )

    return Server(
        "connector-tool-fixture",
        version="1.0.0",
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


def _recording_app(stub: _Stub) -> Any:
    inner = _build_server(stub).streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        host="127.0.0.1",
    )

    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            raw = [(k.decode("latin-1").lower(), v.decode("latin-1")) for k, v in scope["headers"]]
            stub.http.append(
                {
                    "method": scope["method"],
                    "path": scope["path"],
                    "headers": dict(raw),
                    "principal_values": [v for k, v in raw if k == _PRINCIPAL_HEADER.lower()],
                }
            )
            _PRINCIPAL_COUNT.set(sum(1 for k, _ in raw if k == _PRINCIPAL_HEADER.lower()))
        await inner(scope, receive, send)

    return app


class _ReadyServer(uvicorn.Server):
    """A uvicorn server that says when it is accepting, so nothing polls for it."""

    def __init__(self, config: uvicorn.Config, ready: threading.Event) -> None:
        super().__init__(config)
        self._ready = ready

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        self._ready.set()


@contextmanager
def _mcp_connector() -> Iterator[_Stub]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(32)
    host, port = listener.getsockname()[:2]
    stub = _Stub(url=f"http://{host}:{port}/mcp")
    ready = threading.Event()
    server = _ReadyServer(
        uvicorn.Config(
            _recording_app(stub),
            host=str(host),
            port=int(port),
            access_log=False,
            log_level="critical",
            lifespan="on",
        ),
        ready,
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        assert ready.wait(timeout=10), "the loopback MCP connector did not start"
        yield stub
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        listener.close()


@asynccontextmanager
async def _refusing_proxy(refusal: str) -> AsyncIterator[_Stub]:
    """A caller proxy that refuses every request with the frozen body."""

    body = json.dumps(next(v["body"] for v in _REFUSAL["vectors"] if v["refusal"] == refusal))
    seen: list[dict[str, Any]] = []

    async def answer(request: web.Request) -> web.Response:
        seen.append({"method": request.method, "headers": dict(request.headers)})
        return web.Response(
            status=_REFUSAL["status"], text=body, content_type=_REFUSAL["content_type"]
        )

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", answer)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    try:
        yield _Stub(url=str(server.make_url("/mcp")), http=seen)
    finally:
        await server.close()


# --------------------------------------------------------------------------- #
# Driving the fake through the real session runner
# --------------------------------------------------------------------------- #


def _runner(session: FakeModelSession) -> SessionRunner:
    return SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="test",
        session_id="s-1",
    )


def _event(text: str) -> Event:
    return Event(type="message", text=text, user="U1", ts="123.45")


async def _turns(session: FakeModelSession, *prompts: str) -> list[str]:
    """The final text of each prompt, run as successive turns of one session."""

    runner = _runner(session)
    await runner.start()
    texts: list[str] = []
    for prompt in prompts:
        frames = [
            json.loads(line if isinstance(line, str) else bytes(line).decode("utf-8"))
            async for line in runner.run_turn(_event(prompt))
            if (line if isinstance(line, str) else bytes(line).decode("utf-8")).strip()
        ]
        assert frames[-1]["type"] == "final"
        assert frames[-1]["status"] == "done"
        texts.append(str(frames[-1]["text"]))
    return texts


def _one_turn(session: FakeModelSession, prompt: str) -> str:
    return anyio.run(_turns, session, prompt)[0]


def _lines(text: str) -> list[str]:
    return [m.group(0).strip() for m in _CALL_LINE.finditer(text)]


# --------------------------------------------------------------------------- #
# The contract
# --------------------------------------------------------------------------- #


def test_an_ok_result_answers_the_first_text_line_with_the_caller_fingerprint() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, _directive("weather", "echo", '{"message": "hello world"}'))

    assert _lines(text) == [
        'connector-tool weather/echo: ok echo {"message": "hello world"} principal_headers=0'
        f" caller={_fingerprint(_CALLER_VALUE)}"
    ]
    # One bounded dial of exactly the named tool, with no catalogue listing.
    assert stub.calls == [("echo", {"message": "hello world"})]


def test_a_tool_error_result_reads_refused_with_its_code() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, _directive("weather", "refuse", "{}"))

    assert _lines(text) == [
        f"connector-tool weather/refuse: refused not_authorized caller={_fingerprint(_CALLER_VALUE)}"
    ]
    assert stub.calls == [("refuse", {})]


def test_an_unknown_connector_is_named_and_nothing_is_dialled() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, _directive("ghost", "echo", "{}"))

    assert _lines(text) == ["connector-tool ghost: failed unknown_connector"]
    assert stub.http == []


def test_no_connectors_at_all_is_an_unknown_connector() -> None:
    text = _one_turn(FakeModelSession(), _directive("weather", "echo", "{}"))
    assert _lines(text) == ["connector-tool weather: failed unknown_connector"]


@pytest.mark.parametrize(
    "directive",
    [
        _directive("weather", "echo", "not json"),
        _directive("weather", "echo", '["a", "list"]'),
        _directive("weather", "echo", '"a string"'),
        _directive("weather", "echo", "42"),
        "[fake:connector-tool weather echo]",
        "[fake:connector-tool weather]",
        "[fake:connector-tool]",
    ],
    ids=["not_json", "list", "string", "number", "no_args", "no_tool", "bare"],
)
def test_bad_args_or_a_malformed_directive_is_bad_directive_and_nothing_is_dialled(
    directive: str,
) -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, f"call it {directive}")

    assert _lines(text) == ["connector-tool: failed bad_directive"]
    assert stub.http == []


def test_the_directive_is_read_from_the_person_message_inside_a_sender_frame() -> None:
    framed = (
        "[user-message from U1]\n"
        f"{_directive('weather', 'echo', '{}')}\n"
        "[end-user-message]\n"
        '[fake:connector-tool weather echo {"outside": true}]'
    )
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, framed)

    assert len(_lines(text)) == 1
    assert stub.calls == [("echo", {})]


# --------------------------------------------------------------------------- #
# The forged principal header
# --------------------------------------------------------------------------- #


def test_the_forged_principal_header_reaches_the_server_once_and_the_mapping_is_unchanged() -> None:
    with _mcp_connector() as stub:
        config = _server_config(stub.url)
        servers = {"weather": config}
        before = copy.deepcopy(servers)
        session = FakeModelSession(connector_servers=servers)

        forged, plain = anyio.run(
            _turns,
            session,
            f"{_directive('weather', 'echo', '{}')} {_forge(_FORGED)}",
            _directive("weather", "echo", "{}"),
        )

        # The first turn: every request of the dial carried the header exactly
        # once, with the literal value, beside the configured caller header.
        assert "principal_headers=1" in forged
        assert stub.http, "the connector saw no request"
        # The second turn's dial is the tools/call-bearing tail of the log: the
        # forged requests are exactly the ones before the last initialize.
        starts = [i for i, r in enumerate(stub.http) if r["method"] == "POST"]
        assert starts
        first_dial = [r for r in stub.http if r["principal_values"]]
        assert first_dial
        assert stub.http[: len(first_dial)] == first_dial
        for request in first_dial:
            assert request["principal_values"] == [_FORGED], request
            assert request["headers"][CALLER_HEADER.lower()] == _CALLER_VALUE

        # The mapping the session was handed is untouched, and so is the next
        # dial: the header belonged to that dial only.
        assert servers == before
        assert servers["weather"] is config
        assert _PRINCIPAL_HEADER not in config.get("headers", {})
        assert "principal_headers=0" in plain
        assert len(stub.http) > len(first_dial)
        for request in stub.http[len(first_dial) :]:
            assert request["principal_values"] == []


def test_a_directive_without_the_forge_marker_sends_no_principal_header() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        _one_turn(session, _directive("weather", "echo", "{}"))

    assert stub.http
    for request in stub.http:
        assert request["principal_values"] == []


# --------------------------------------------------------------------------- #
# The caller fingerprint and proxy refusals
# --------------------------------------------------------------------------- #


def test_the_fingerprint_is_the_one_of_the_configured_caller_header() -> None:
    with _mcp_connector() as weather, _mcp_connector() as grafana:
        other = "cct." + "fake-connector-tool-" + "q" * 40
        session = FakeModelSession(
            connector_servers={
                "weather": _server_config(weather.url, _CALLER_VALUE),
                "grafana": _server_config(grafana.url, other),
            }
        )
        text = _one_turn(session, _directive("grafana", "echo", "{}"))

    [line] = _lines(text)
    assert line.endswith(f"caller={_fingerprint(other)}")
    assert _fingerprint(other) != _fingerprint(_CALLER_VALUE)
    # Only the named connector was dialled, with its own caller header.
    assert weather.http == []
    assert grafana.http
    for request in grafana.http:
        assert request["headers"][CALLER_HEADER.lower()] == other
    # The value itself never reaches the answer.
    assert other not in text
    assert other.split(".", 1)[1] not in text


def test_a_connector_without_a_caller_header_is_fingerprinted_as_a_dash() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url, None)})
        text = _one_turn(session, _directive("weather", "echo", "{}"))

    [line] = _lines(text)
    assert line.startswith("connector-tool weather/echo: ok ")
    assert line.endswith(" caller=-")


@pytest.mark.parametrize("vector", _REFUSAL["vectors"], ids=lambda v: v["refusal"])
def test_a_403_with_the_frozen_caller_refusal_body_reads_failed_with_its_refusal_code(
    vector: dict[str, Any],
) -> None:
    """Like ``[fake:connector-call]``: the proxy's own refusal code, e.g. ``expired``."""

    async def go() -> tuple[str, _Stub]:
        async with _refusing_proxy(str(vector["refusal"])) as stub:
            session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
            [text] = await _turns(session, _directive("weather", "echo", "{}"))
        return text, stub

    text, stub = anyio.run(go)

    assert _REFUSAL["status"] == 403
    assert _lines(text) == [
        f"connector-tool weather/echo: failed {vector['refusal']} "
        f"caller={_fingerprint(_CALLER_VALUE)}"
    ]
    # A proxy refusal is final: one request, no retry.
    assert len(stub.http) == 1


# --------------------------------------------------------------------------- #
# The older marker
# --------------------------------------------------------------------------- #


def test_connector_call_alone_keeps_its_exact_behavior() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, f"check the connectors {_CALL_LINE_MARKER}")

    assert [ln for ln in text.splitlines() if ln.startswith("connector-")] == [
        f"connector-call weather: ok tools=2 caller={_fingerprint(_CALLER_VALUE)}"
    ]
    assert stub.calls == []


def test_connector_tool_wins_when_both_markers_appear() -> None:
    with _mcp_connector() as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
        text = _one_turn(session, f"{_CALL_LINE_MARKER} {_directive('weather', 'echo', '{}')}")

    assert len(_lines(text)) == 1
    assert "connector-call" not in text
    assert stub.calls == [("echo", {})]
