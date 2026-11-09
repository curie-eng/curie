"""The fake model dials each hosted connector it is given, on request.

ADR 0270 evidence (addendum E1): under ``--fake-model`` a turn whose prompt
carries ``[fake:connector-call]`` dials every derived hosted connector server
the session was handed, in sorted name order, with that server's materialized
headers: one MCP ``initialize`` and one ``tools/list`` through the same
standalone client the boot probe uses. The turn then answers one line per
server::

    connector-call <name>: ok tools=<n> caller=<fp>
    connector-call <name>: failed <reason> caller=<fp>

``<fp>`` is the first 16 lowercase hex characters of the sha256 of the
``X-Curie-Caller`` value that server was dialled with (``-`` with none), so a
black-box run can tie a dial to one token without the token ever being
printed. ``<reason>`` is the caller proxy's refusal code when the answer is a
proxy refusal (``tests/vectors/connector-caller-refusal.json``), and otherwise
the failure's exception class name. With no servers the answer is
``connector-call: none``. Without the marker the fake is unchanged and dials
nothing.

The connector is a real MCP server: the executor fixture's streamable-HTTP app
(``fixtures/mcp_executor_connector.py``) on a loopback uvicorn, wrapped so
every HTTP request's headers are recorded, not only its JSON-RPC bodies.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import logging
import re
import socket
import threading
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
import httpx2
import pytest
import uvicorn
from aci_protocol import Event
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import fake as fake_module
from curie_runner import mcp_tool_capability as capability
from curie_runner.connectors import CALLER_HEADER
from curie_runner.fake import FakeModelSession, default_turn
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier

_REFUSAL = json.loads(
    (
        Path(__file__).resolve().parents[2] / "tests" / "vectors" / "connector-caller-refusal.json"
    ).read_text(encoding="utf-8")
)
_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "mcp_executor_connector.py"

# Built at runtime, low entropy, and long enough that any echo of it is
# unmistakable. Two values so two servers can be told apart by fingerprint.
_CALLER_VALUE = "cct." + "fake-connector-call-" + "z" * 40
_OTHER_CALLER_VALUE = "cct." + "fake-connector-call-" + "q" * 40
_CALL_LINE = re.compile(r"^connector-call(?: [^:\s]+)?: .*$")
# The contract is the literal text a person or a harness types, so prompts use
# it directly; the module constant is checked against it on its own.
_MARKER = "[fake:connector-call]"


def _load_fixture() -> Any:
    spec = importlib.util.spec_from_file_location("_fake_connector_call_fixture", _FIXTURE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CONNECTOR_FIXTURE = _load_fixture()


def _tool_count(mode: str) -> int:
    return len(_CONNECTOR_FIXTURE._tools(mode))


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def _refusal_body(refusal: str) -> dict[str, Any]:
    return next(v["body"] for v in _REFUSAL["vectors"] if v["refusal"] == refusal)


def _server_config(url: str, caller: str | None = _CALLER_VALUE) -> dict[str, Any]:
    """A derived hosted entry after ``materialize_connector_caller_headers``."""

    config: dict[str, Any] = {"type": "http", "url": url}
    if caller is not None:
        config["headers"] = {CALLER_HEADER: caller}
    return config


# --------------------------------------------------------------------------- #
# A real MCP connector on a loopback port, recording every request's headers
# --------------------------------------------------------------------------- #


@dataclass
class _Stub:
    url: str
    # Every HTTP request, any method: {"method", "path", "headers"} with
    # lowercased header names.
    http: list[dict[str, Any]] = field(default_factory=list)
    # Every JSON-RPC request the fixture app parsed: {"method", "params", "headers"}.
    jsonrpc: list[dict[str, Any]] = field(default_factory=list)

    def methods(self) -> list[str]:
        return [request["method"] for request in self.jsonrpc]


class _ReadyServer(uvicorn.Server):
    """A uvicorn server that says when it is accepting, so nothing polls for it."""

    def __init__(self, config: uvicorn.Config, ready: threading.Event) -> None:
        super().__init__(config)
        self._ready = ready

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        self._ready.set()


def _recording(app: Any, seen: list[dict[str, Any]]) -> Any:
    async def wrapped(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            seen.append(
                {
                    "method": scope["method"],
                    "path": scope["path"],
                    "headers": {
                        key.decode("latin-1").lower(): value.decode("latin-1")
                        for key, value in scope["headers"]
                    },
                }
            )
        await app(scope, receive, send)

    return wrapped


@contextmanager
def _mcp_connector(tools: str = "paired") -> Iterator[_Stub]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(32)
    host, port = listener.getsockname()[:2]
    stub = _Stub(url=f"http://{host}:{port}/mcp")
    app = _recording(_CONNECTOR_FIXTURE.http_app({"tools": tools}, stub.jsonrpc), stub.http)
    ready = threading.Event()
    server = _ReadyServer(
        uvicorn.Config(
            app,
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
async def _refusing_proxy(status: int, body: bytes, content_type: str) -> AsyncIterator[_Stub]:
    """A caller proxy that refuses every request, recording what each carried."""

    seen: list[dict[str, Any]] = []

    async def answer(request: web.Request) -> web.Response:
        seen.append(
            {
                "method": request.method,
                "path": request.path,
                "headers": {key.lower(): value for key, value in request.headers.items()},
            }
        )
        return web.Response(status=status, body=body, content_type=content_type)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", answer)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    try:
        yield _Stub(url=str(server.make_url("/mcp")), http=seen)
    finally:
        await server.close()


def _closed_port() -> int:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = int(probe.getsockname()[1])
    probe.close()
    return port


# --------------------------------------------------------------------------- #
# Driving the fake: directly, and through the real session runner
# --------------------------------------------------------------------------- #


def _runner(session: FakeModelSession) -> SessionRunner:
    return SessionRunner(
        max_usd_per_day=None,
        # Nothing is held, so the runner's own redaction cannot hide a leak.
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


async def _runner_turn(session: FakeModelSession, prompt: str) -> tuple[list[str], dict[str, Any]]:
    """Every NDJSON line the turn emitted, and its final frame."""

    runner = _runner(session)
    await runner.start()
    raw: list[str] = []
    async for line in runner.run_turn(_event(prompt)):
        raw.append(line if isinstance(line, str) else bytes(line).decode("utf-8"))
    frames = [json.loads(line) for line in raw if line.strip()]
    final = frames[-1]
    assert final["type"] == "final"
    return raw, final


async def _session_turn(session: FakeModelSession, prompt: str) -> list[Any]:
    """Every message the session itself yields for one turn."""

    await session.connect()
    await session.query(prompt)
    return [message async for message in session.receive_turn()]


def _call_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if _CALL_LINE.match(line.strip())]


def _ok(name: str, tools: int, caller: str | None = _CALLER_VALUE) -> str:
    fp = _fingerprint(caller) if caller is not None else "-"
    return f"connector-call {name}: ok tools={tools} caller={fp}"


def _failed(name: str, reason: str, caller: str | None = _CALLER_VALUE) -> str:
    fp = _fingerprint(caller) if caller is not None else "-"
    return f"connector-call {name}: failed {reason} caller={fp}"


def _prompt() -> str:
    return f"check the connectors {_MARKER}"


def _assert_every_request_carried(stub: _Stub, value: str) -> None:
    assert stub.http, "the connector saw no request"
    for request in stub.http:
        assert request["headers"].get(CALLER_HEADER.lower()) == value, (
            request["method"],
            request["path"],
        )


def _assert_value_absent(texts: list[str], value: str) -> None:
    # The whole value, and its distinctive tail on its own, so a value cut at
    # the prefix is caught too.
    tail = value.split(".", 1)[1]
    for text in texts:
        assert value not in text
        assert tail not in text


# --------------------------------------------------------------------------- #
# The contract
# --------------------------------------------------------------------------- #


def test_the_marker_is_the_frozen_spelling() -> None:
    assert fake_module.CONNECTOR_CALL_MARKER == _MARKER


def test_the_fingerprint_is_sha256_hex_truncated_to_sixteen() -> None:
    # Pins this file's helper to the contract's arithmetic, so a wrong
    # expectation cannot agree with a wrong implementation by accident.
    fp = _fingerprint(_CALLER_VALUE)
    assert len(fp) == 16
    assert fp == hashlib.sha256(_CALLER_VALUE.encode()).hexdigest()[:16]
    assert re.fullmatch(r"[0-9a-f]{16}", fp)
    assert fp not in _CALLER_VALUE


# @spec CONNECTOR-REQUESTER-2
def test_the_marker_dials_the_connector_with_its_caller_header() -> None:
    with _mcp_connector("paired") as stub:
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})

        async def go() -> dict[str, Any]:
            _raw, final = await _runner_turn(session, _prompt())
            return final

        final = anyio.run(go)
        assert final["status"] == "done"
        assert _call_lines(final["text"]) == [_ok("weather", _tool_count("paired"))]
        methods = stub.methods()
        assert methods.count("initialize") == 1
        assert methods.count("tools/list") == 1
        assert "tools/call" not in methods
        _assert_every_request_carried(stub, _CALLER_VALUE)
        for request in stub.jsonrpc:
            assert request["headers"].get(CALLER_HEADER.lower()) == _CALLER_VALUE


# @spec CONNECTOR-REQUESTER-2
def test_two_connectors_answer_one_line_each_in_sorted_name_order() -> None:
    with _mcp_connector("paired") as weather, _mcp_connector("no_restore") as grafana:
        # Inserted out of order: the answer follows the names, not the mapping.
        servers = {
            "weather": _server_config(weather.url, _CALLER_VALUE),
            "grafana": _server_config(grafana.url, _OTHER_CALLER_VALUE),
        }
        session = FakeModelSession(connector_servers=servers)

        async def go() -> dict[str, Any]:
            _raw, final = await _runner_turn(session, _prompt())
            return final

        final = anyio.run(go)
        assert final["status"] == "done"
        assert _call_lines(final["text"]) == [
            _ok("grafana", _tool_count("no_restore"), _OTHER_CALLER_VALUE),
            _ok("weather", _tool_count("paired"), _CALLER_VALUE),
        ]
        # Each server got its own header, never the other's.
        _assert_every_request_carried(weather, _CALLER_VALUE)
        _assert_every_request_carried(grafana, _OTHER_CALLER_VALUE)
        assert weather.methods().count("tools/list") == 1
        assert grafana.methods().count("tools/list") == 1


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("vector", _REFUSAL["vectors"], ids=lambda v: v["refusal"])
def test_a_refusing_caller_proxy_is_named_by_its_refusal_and_the_turn_completes(
    vector: dict[str, Any],
) -> None:
    refusal = str(vector["refusal"])
    body = json.dumps(_refusal_body(refusal)).encode("utf-8")

    async def go() -> tuple[dict[str, Any], _Stub]:
        async with _refusing_proxy(_REFUSAL["status"], body, _REFUSAL["content_type"]) as stub:
            session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
            _raw, final = await _runner_turn(session, _prompt())
        return final, stub

    final, stub = anyio.run(go)
    assert _REFUSAL["status"] == 403
    assert final["status"] == "done"
    assert _call_lines(final["text"]) == [_failed("weather", refusal)]
    _assert_every_request_carried(stub, _CALLER_VALUE)


# @spec CONNECTOR-REQUESTER-2
def test_one_refused_connector_does_not_stop_the_next() -> None:
    body = json.dumps(_refusal_body("not_admitted")).encode("utf-8")

    with _mcp_connector("paired") as weather:

        async def go() -> tuple[dict[str, Any], _Stub]:
            async with _refusing_proxy(
                _REFUSAL["status"], body, _REFUSAL["content_type"]
            ) as refused:
                session = FakeModelSession(
                    connector_servers={
                        "weather": _server_config(weather.url, _CALLER_VALUE),
                        "grafana": _server_config(refused.url, _OTHER_CALLER_VALUE),
                    }
                )
                _raw, final = await _runner_turn(session, _prompt())
            return final, refused

        final, refused = anyio.run(go)
        assert final["status"] == "done"
        assert _call_lines(final["text"]) == [
            _failed("grafana", "not_admitted", _OTHER_CALLER_VALUE),
            _ok("weather", _tool_count("paired"), _CALLER_VALUE),
        ]
        _assert_every_request_carried(refused, _OTHER_CALLER_VALUE)
        _assert_every_request_carried(weather, _CALLER_VALUE)


# @spec CONNECTOR-REQUESTER-2
def test_an_unreachable_connector_is_named_by_an_exception_class() -> None:
    url = f"http://127.0.0.1:{_closed_port()}/mcp"
    session = FakeModelSession(connector_servers={"weather": _server_config(url)})

    async def go() -> dict[str, Any]:
        _raw, final = await _runner_turn(session, _prompt())
        return final

    final = anyio.run(go)
    assert final["status"] == "done"
    [line] = _call_lines(final["text"])
    match = re.fullmatch(
        r"connector-call weather: failed ([A-Za-z_][A-Za-z0-9_]*) caller=([0-9a-f]{16})", line
    )
    assert match is not None, line
    reason, fp = match.groups()
    # Not a proxy refusal: nothing answered, so the reason is a class name.
    assert reason not in {str(v["refusal"]) for v in _REFUSAL["vectors"]}
    assert fp == _fingerprint(_CALLER_VALUE)


# @spec CONNECTOR-REQUESTER-2
def test_a_server_without_a_caller_header_is_fingerprinted_as_a_dash() -> None:
    with _mcp_connector("no_restore") as stub:
        session = FakeModelSession(
            connector_servers={"weather": _server_config(stub.url, caller=None)}
        )

        async def go() -> dict[str, Any]:
            _raw, final = await _runner_turn(session, _prompt())
            return final

        final = anyio.run(go)
        assert final["status"] == "done"
        assert _call_lines(final["text"]) == [
            _ok("weather", _tool_count("no_restore"), caller=None)
        ]
        assert stub.http
        for request in stub.http:
            assert CALLER_HEADER.lower() not in request["headers"]


# --------------------------------------------------------------------------- #
# The caller value never leaves the dial
# --------------------------------------------------------------------------- #


def _texts_of(messages: list[Any]) -> list[str]:
    # repr covers every field of every SDK message dataclass: text blocks,
    # tool-use inputs, tool-result content and the result message's result.
    return [repr(message) for message in messages]


@asynccontextmanager
async def _connector_for(outcome: str) -> AsyncIterator[str]:
    if outcome == "ok":
        with _mcp_connector("paired") as stub:
            yield stub.url
        return
    body = json.dumps(_refusal_body("invalid")).encode("utf-8")
    async with _refusing_proxy(_REFUSAL["status"], body, _REFUSAL["content_type"]) as stub:
        yield stub.url


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("outcome", ["ok", "refused"])
def test_the_caller_value_never_appears_in_anything_the_session_emits(
    outcome: str, caplog: pytest.LogCaptureFixture
) -> None:
    async def go() -> tuple[list[Any], list[str], dict[str, Any]]:
        async with _connector_for(outcome) as url:
            servers = {"weather": _server_config(url)}
            messages = await _session_turn(FakeModelSession(connector_servers=servers), _prompt())
            raw, final = await _runner_turn(FakeModelSession(connector_servers=servers), _prompt())
        return messages, raw, final

    with caplog.at_level(logging.DEBUG, logger="curie_runner"):
        messages, raw, final = anyio.run(go)

    # The dial happened and was reported, so the absence below is not vacuous.
    expected = (
        _ok("weather", _tool_count("paired")) if outcome == "ok" else _failed("weather", "invalid")
    )
    assert _call_lines(final["text"]) == [expected]
    assert any(expected in text for text in _texts_of(messages))

    _assert_value_absent(_texts_of(messages), _CALLER_VALUE)
    _assert_value_absent(raw, _CALLER_VALUE)
    _assert_value_absent([final["text"]], _CALLER_VALUE)
    _assert_value_absent(
        [record.getMessage() for record in caplog.records] + [caplog.text], _CALLER_VALUE
    )


# --------------------------------------------------------------------------- #
# No servers, and no marker
# --------------------------------------------------------------------------- #


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("servers", [None, {}], ids=["none", "empty"])
def test_the_marker_with_no_connectors_answers_none(
    servers: Mapping[str, Mapping[str, Any]] | None,
) -> None:
    with _mcp_connector("paired") as bystander:
        session = FakeModelSession(connector_servers=servers)

        async def go() -> dict[str, Any]:
            _raw, final = await _runner_turn(session, _prompt())
            return final

        final = anyio.run(go)
        assert final["status"] == "done"
        assert _call_lines(final["text"]) == ["connector-call: none"]
        assert bystander.http == []


# @spec CONNECTOR-REQUESTER-2
def test_the_marker_is_the_only_way_to_dial() -> None:
    with _mcp_connector("paired") as stub:
        servers = {"weather": _server_config(stub.url)}

        async def go() -> tuple[list[Any], dict[str, Any]]:
            messages = await _session_turn(
                FakeModelSession(connector_servers=servers), "check the connectors"
            )
            _raw, final = await _runner_turn(
                FakeModelSession(connector_servers=servers), "check the connectors"
            )
            return messages, final

        messages, final = anyio.run(go)
        # The default turn, message for message, and not one request made.
        assert messages == default_turn()
        assert final["status"] == "done"
        assert final["text"] == "all done"
        assert _call_lines(final["text"]) == []
        assert stub.http == []
        assert stub.jsonrpc == []


def test_a_session_without_connector_servers_keeps_the_default_turn() -> None:
    async def go() -> list[Any]:
        return await _session_turn(FakeModelSession(), "hello")

    assert anyio.run(go) == default_turn()


# --------------------------------------------------------------------------- #
# A transport failure before the server answered is retried (<= 3 dials)
# --------------------------------------------------------------------------- #

# The MCP client's transport is httpx2 (mcp 2.x), not httpx; a dial to a
# closed port or an unresolvable name arrives as an exception group whose leaf
# is httpx2.ConnectError (recorded from the runner image).
_TRANSPORT_FAILURES = [
    httpx2.ConnectError,
    httpx2.ConnectTimeout,
    ConnectionResetError,
    socket.gaierror,
]


def _count_dials(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_first: int,
    failure: type[BaseException] = httpx2.ConnectError,
) -> list[int]:
    """Count every transport open; the first ``fail_first`` raise ``failure``.

    The wrapper replaces ``server_streams`` wherever the dial could look it up
    (the fake's own import, the shared module, and the probe's private alias),
    so the count holds however the retry is arranged. Later opens go to the
    real transport. Back-off sleeps are skipped so no test waits.
    """

    real = capability.server_streams
    dials: list[int] = []

    @asynccontextmanager
    async def counting(
        config: Mapping[str, Any], *, plugin_dir: Any, inherited_env: Mapping[str, str]
    ) -> AsyncIterator[tuple[Any, Any]]:
        dials.append(1)
        if len(dials) <= fail_first:
            raise failure("simulated transport failure before the server answered")
        async with real(config, plugin_dir=plugin_dir, inherited_env=inherited_env) as streams:
            yield streams

    for module, name in (
        (fake_module, "server_streams"),
        (capability, "server_streams"),
        (capability, "_server_streams"),
    ):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, counting)

    async def no_sleep(_delay: float = 0, *_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(anyio, "sleep", no_sleep)
    return dials


# @spec CONNECTOR-REQUESTER-2
def test_a_first_dial_that_fails_at_the_transport_is_retried_and_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _mcp_connector("paired") as stub:
        dials = _count_dials(monkeypatch, fail_first=1)
        session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})

        async def go() -> dict[str, Any]:
            _raw, final = await _runner_turn(session, _prompt())
            return final

        final = anyio.run(go)
        assert final["status"] == "done"
        assert _call_lines(final["text"]) == [_ok("weather", _tool_count("paired"))]
        assert len(dials) == 2
        # The failed attempt never reached the server, so the server saw one
        # clean handshake, not two.
        methods = stub.methods()
        assert methods.count("initialize") == 1
        assert methods.count("tools/list") == 1
        _assert_every_request_carried(stub, _CALLER_VALUE)


# @spec CONNECTOR-REQUESTER-2
def test_a_refusing_proxy_is_dialed_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    body = json.dumps(_refusal_body("invalid")).encode("utf-8")
    dials = _count_dials(monkeypatch, fail_first=0)

    async def go() -> tuple[dict[str, Any], _Stub]:
        async with _refusing_proxy(_REFUSAL["status"], body, _REFUSAL["content_type"]) as stub:
            session = FakeModelSession(connector_servers={"weather": _server_config(stub.url)})
            _raw, final = await _runner_turn(session, _prompt())
        return final, stub

    final, stub = anyio.run(go)
    assert final["status"] == "done"
    assert _call_lines(final["text"]) == [_failed("weather", "invalid")]
    assert len(dials) == 1
    assert len(stub.http) == 1


# @spec CONNECTOR-REQUESTER-2
@pytest.mark.parametrize("failure", _TRANSPORT_FAILURES, ids=lambda f: f.__name__)
def test_a_permanently_unreachable_connector_is_dialed_three_times_then_reported(
    monkeypatch: pytest.MonkeyPatch, failure: type[BaseException]
) -> None:
    dials = _count_dials(monkeypatch, fail_first=10_000, failure=failure)
    url = f"http://127.0.0.1:{_closed_port()}/mcp"
    session = FakeModelSession(connector_servers={"weather": _server_config(url)})

    async def go() -> dict[str, Any]:
        _raw, final = await _runner_turn(session, _prompt())
        return final

    final = anyio.run(go)
    assert final["status"] == "done"
    assert _call_lines(final["text"]) == [_failed("weather", failure.__name__)]
    assert len(dials) == 3


# @spec CONNECTOR-REQUESTER-2
def test_the_real_transport_failure_to_a_closed_port_is_retried_then_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No injected failure: the real MCP transport dials a closed port, so the
    # retry classifies the shape the runner actually sees.
    dials = _count_dials(monkeypatch, fail_first=0)
    url = f"http://127.0.0.1:{_closed_port()}/mcp"
    session = FakeModelSession(connector_servers={"weather": _server_config(url)})

    async def go() -> dict[str, Any]:
        _raw, final = await _runner_turn(session, _prompt())
        return final

    final = anyio.run(go)
    assert final["status"] == "done"
    assert _call_lines(final["text"]) == [_failed("weather", "ConnectError")]
    assert len(dials) == 3


def test_the_runner_package_imports_without_httpx() -> None:
    # The runner image installs only the runner's lock closure, which has no
    # httpx; importing curie_runner (what `python -m curie_runner` does) must
    # not need it.
    import subprocess
    import sys

    script = (
        "import sys; sys.modules['httpx'] = None; "
        "import curie_runner, curie_runner.fake, curie_runner.__main__"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr[-2000:]
