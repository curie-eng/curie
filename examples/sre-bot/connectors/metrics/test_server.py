"""Behavior of the metrics connector: a read-only verifier the platform can sample.

@spec AUTOMATED-REMEDIATION-12 @spec AUTOMATED-REMEDIATION-17. Every test
starts ``server.py`` as a real process and drives it through a real MCP client,
so what is pinned is the wire behavior the runner's ``read`` phase meets. Each
reply is then judged by the production readers that consume it: the runner's
``sample_of`` (one RFC 6901 pointer over the structured content) and the API's
``evaluate_sample`` (one comparator).

Prometheus is a local stand-in for ``GET /api/v1/query``. Its reply shapes are
the documented HTTP API ones: ``{"status": "success", "data": {"resultType":
..., "result": ...}}``, a vector as a list of ``{"metric", "value": [<unix
time>, "<sample>"]}``, a scalar as ``[<unix time>, "<sample>"]``, and an error
as ``{"status": "error", "errorType", "error"}`` with a 4xx or 5xx status.
"""

from __future__ import annotations

import asyncio
import http.server
import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from curie_api.remediation_predicate import evaluate_sample
from curie_runner.executor import sample_of
from mcp import Client
from mcp.types import CallToolResult

_HERE = os.path.dirname(os.path.abspath(__file__))
_SERVER = os.path.join(_HERE, "server.py")

_TOOL = "query_value"
_TOKEN = "example-metrics-token"
_QUERY = 'max(up{job="example-api"}) or vector(0)'
_VECTOR_POINTER = "/data/0/value/1"
_SCALAR_POINTER = "/data/value"


def _vector(*series: tuple[dict[str, str], str]) -> dict[str, Any]:
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {"metric": labels, "value": [1798761600.123, sample]} for labels, sample in series
            ],
        },
    }


def _scalar(sample: str) -> dict[str, Any]:
    return {
        "status": "success",
        "data": {"resultType": "scalar", "result": [1798761600.123, sample]},
    }


# --------------------------------------------------------------------------- #
# The Prometheus stand-in.
# --------------------------------------------------------------------------- #


class _Prometheus:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        # What the next queries answer: (status code, body bytes), or a delay.
        self.status = 200
        self.body: bytes = json.dumps(_vector(({"job": "example-api"}, "1"))).encode()
        self.delay = 0.0
        self.redirect_to: str | None = None
        self.requests: list[tuple[str, str, dict[str, list[str]], str | None]] = []

    def answer(self, payload: Any, status: int = 200) -> None:
        with self.lock:
            self.status = status
            self.body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()


def _handler(prometheus: _Prometheus) -> type[http.server.BaseHTTPRequestHandler]:
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def _record(self) -> tuple[str, dict[str, list[str]]]:
            parts = urlsplit(self.path)
            query = parse_qs(parts.query)
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length:
                query.update(parse_qs(self.rfile.read(length).decode()))
            prometheus.requests.append(
                (self.command, parts.path, query, self.headers.get("Authorization"))
            )
            return parts.path, query

        def _serve(self) -> None:
            path, _query = self._record()
            if prometheus.delay:
                time.sleep(prometheus.delay)
            if prometheus.redirect_to is not None:
                self.send_response(302)
                self.send_header("Location", prometheus.redirect_to)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if not path.endswith("/api/v1/query"):
                status, body = 404, b"404 page not found"
            else:
                with prometheus.lock:
                    status, body = prometheus.status, prometheus.body
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = _serve  # noqa: N815 - http.server's naming
        do_POST = _serve  # noqa: N815 - http.server's naming

    return Handler


@pytest.fixture
def prometheus() -> Iterator[tuple[_Prometheus, str]]:
    state = _Prometheus()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield state, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


# --------------------------------------------------------------------------- #
# The connector as a real process, a real MCP client against it.
# --------------------------------------------------------------------------- #


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class _Connector:
    def __init__(self, env: dict[str, str]) -> None:
        port = _free_port()
        child_env = {
            key: value for key, value in os.environ.items() if not key.startswith("METRICS_")
        }
        child_env.update({"BIND_ADDRESS": "127.0.0.1", "PORT": str(port)})
        child_env.update(env)
        self.process = subprocess.Popen(
            [sys.executable, _SERVER],
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.url = f"http://127.0.0.1:{port}/mcp"
        self.output = ""
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.output = self.process.stdout.read().decode("utf-8", "replace")  # type: ignore[union-attr]
                return
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        pytest.fail("metrics connector did not listen within 20s")

    def stop(self) -> str:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None and not self.process.stdout.closed:
            self.output += self.process.stdout.read().decode("utf-8", "replace")
            self.process.stdout.close()
        return self.output

    def tools(self) -> dict[str, Any]:
        async def go() -> dict[str, Any]:
            async with Client(self.url) as client:
                listed = await client.list_tools()
                return {tool.name: tool for tool in listed.tools}

        return asyncio.run(go())

    def call(self, arguments: dict[str, Any], name: str = _TOOL) -> CallToolResult:
        async def go() -> CallToolResult:
            async with Client(self.url) as client:
                return await client.call_tool(name, arguments)

        return asyncio.run(go())

    def sample(self, pointer: str = _VECTOR_POINTER, query: str = _QUERY) -> dict[str, Any]:
        """The sample the runner's ``read`` phase takes from one call."""

        return sample_of(self.call({"query": query}), pointer)


@pytest.fixture
def connector(prometheus: tuple[_Prometheus, str]) -> Iterator[Any]:
    _state, url = prometheus
    started: list[_Connector] = []

    def start(**env: str) -> _Connector:
        for running in started:
            running.stop()
        merged = {"METRICS_URL": url, "METRICS_TIMEOUT_SECONDS": "2"}
        merged.update(env)
        process = _Connector(merged)
        started.append(process)
        return process

    yield start
    for running in started:
        running.stop()


def _judge(sample: dict[str, Any], comparator: str, value: Any) -> str:
    return evaluate_sample({"comparator": comparator, "value": value}, sample)


# --------------------------------------------------------------------------- #
# The advertised surface.
# --------------------------------------------------------------------------- #


def test_the_connector_advertises_one_read_only_tool(connector: Any) -> None:
    tools = connector().tools()
    assert set(tools) == {_TOOL}
    annotations = tools[_TOOL].annotations
    assert annotations is not None
    assert annotations.read_only_hint is True
    assert annotations.destructive_hint is False
    assert tools[_TOOL].input_schema["required"] == ["query"]
    assert set(tools[_TOOL].input_schema["properties"]) == {"query"}


# --------------------------------------------------------------------------- #
# What the platform's pointer and comparator read.
# --------------------------------------------------------------------------- #


def test_a_vector_sample_is_numeric_at_the_documented_pointer(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    process = connector()

    state.answer(_vector(({"job": "example-api"}, "1")))
    recovered = process.sample()
    assert recovered == {"sample": "value", "value": "1"}
    assert _judge(recovered, "ge", 1) == "satisfied"

    state.answer(_vector(({"job": "example-api"}, "0")))
    broken = process.sample()
    assert broken == {"sample": "value", "value": "0"}
    assert _judge(broken, "ge", 1) == "unsatisfied"
    assert _judge(broken, "lt", 1) == "satisfied"


def test_the_query_reaches_the_instant_endpoint_unchanged(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    connector().sample()
    ((method, path, query, _auth),) = state.requests
    assert method == "GET"
    assert path == "/api/v1/query"
    assert query == {"query": [_QUERY]}


def test_a_base_url_with_a_path_prefix_is_kept(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    """A Prometheus behind a prefix (a proxy, a ``--web.route-prefix``)."""

    state, url = prometheus
    connector(METRICS_URL=f"{url}/prometheus/").sample()
    ((_method, path, _query, _auth),) = state.requests
    assert path == "/prometheus/api/v1/query"


def test_a_scalar_sample_is_numeric_at_data_value(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.answer(_scalar("0.25"))
    sample = connector().sample(_SCALAR_POINTER, "scalar(example_error_ratio)")
    assert sample == {"sample": "value", "value": "0.25"}
    assert _judge(sample, "lt", 0.5) == "satisfied"
    assert _judge(sample, "gt", 0.5) == "unsatisfied"


def test_an_empty_vector_leaves_the_pointer_absent(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.answer(_vector())
    sample = connector().sample()
    assert sample == {"sample": "pointer_absent", "value": None}
    assert _judge(sample, "absent", None) == "satisfied"
    assert _judge(sample, "ge", 1) == "unsatisfied"


def test_series_are_ordered_by_their_labels_not_by_arrival(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    """``/data/0`` names the same series however Prometheus orders its answer."""

    state, _url = prometheus
    process = connector()
    state.answer(_vector(({"pod": "example-api-b"}, "0"), ({"pod": "example-api-a"}, "1")))
    first = process.sample()
    state.answer(_vector(({"pod": "example-api-a"}, "1"), ({"pod": "example-api-b"}, "0")))
    assert process.sample() == first == {"sample": "value", "value": "1"}
    assert process.sample("/data/1/metric/pod") == {"sample": "value", "value": "example-api-b"}


def test_a_non_finite_sample_is_never_ordered(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.answer(_vector(({"job": "example-api"}, "NaN")))
    sample = connector().sample()
    assert sample == {"sample": "value", "value": "NaN"}
    for comparator in ("lt", "le", "gt", "ge"):
        assert _judge(sample, comparator, 1) == "unsatisfied"


# --------------------------------------------------------------------------- #
# Failing closed: every one is a tool error, so the sample is unsuccessful.
# --------------------------------------------------------------------------- #


def _assert_unsuccessful(process: _Connector, pointer: str = _VECTOR_POINTER) -> None:
    sample = process.sample(pointer)
    assert sample == {"sample": "tool_error", "value": None}
    assert _judge(sample, "absent", None) == "unsuccessful"


@pytest.mark.parametrize(
    ("payload", "status"),
    [
        pytest.param(
            {
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [{"metric": {"job": "example-api"}, "values": [[1798761600, "1"]]}],
                },
            },
            200,
            id="range-result",
        ),
        pytest.param(
            {"status": "success", "data": {"resultType": "string", "result": [1, "x"]}},
            200,
            id="string-result",
        ),
        pytest.param(
            {"status": "error", "errorType": "bad_data", "error": "parse error"},
            400,
            id="bad-query",
        ),
        pytest.param(
            {"status": "error", "errorType": "timeout", "error": "query timed out"},
            503,
            id="server-timeout",
        ),
        pytest.param(
            {"status": "error", "errorType": "internal", "error": "x"}, 200, id="error-status-200"
        ),
        pytest.param(b"<html>bad gateway</html>", 502, id="proxy-error"),
        pytest.param(b"not json", 200, id="not-json"),
        pytest.param({"status": "success"}, 200, id="no-data"),
        pytest.param(
            {
                "status": "success",
                "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, 2]}]},
            },
            200,
            id="sample-not-a-string",
        ),
        pytest.param(
            {"status": "success", "data": {"resultType": "scalar", "result": "1"}},
            200,
            id="scalar-malformed",
        ),
    ],
)
def test_an_unusable_answer_is_a_tool_error(
    connector: Any, prometheus: tuple[_Prometheus, str], payload: Any, status: int
) -> None:
    state, _url = prometheus
    state.answer(payload, status)
    _assert_unsuccessful(connector())


def test_a_slow_prometheus_times_out_as_a_tool_error(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.delay = 2.0
    process = connector(METRICS_TIMEOUT_SECONDS="0.5")
    started = time.monotonic()
    _assert_unsuccessful(process)
    assert time.monotonic() - started < 2.0


def test_an_unreachable_prometheus_is_a_tool_error(connector: Any) -> None:
    _assert_unsuccessful(connector(METRICS_URL=f"http://127.0.0.1:{_free_port()}"))


def test_a_redirect_is_not_followed(connector: Any, prometheus: tuple[_Prometheus, str]) -> None:
    state, url = prometheus
    state.redirect_to = f"{url}/elsewhere/api/v1/query"
    _assert_unsuccessful(connector())
    assert len(state.requests) == 1


def test_too_many_series_is_a_tool_error(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.answer(_vector(*(({"pod": f"example-api-{n}"}, "1") for n in range(4))))
    _assert_unsuccessful(connector(METRICS_MAX_SERIES="3"))


def test_an_oversized_answer_is_a_tool_error(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.answer(_vector(({"job": "example-api", "padding": "x" * 4096}, "1")))
    _assert_unsuccessful(connector(METRICS_MAX_RESPONSE_BYTES="1024"))


@pytest.mark.parametrize("query", ["", "   ", "x" * 4097])
def test_an_empty_or_oversized_query_reaches_no_prometheus(
    connector: Any, prometheus: tuple[_Prometheus, str], query: str
) -> None:
    state, _url = prometheus
    result = connector().call({"query": query})
    assert result.is_error is True
    assert state.requests == []


def test_without_a_url_every_call_is_refused(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    process = connector(METRICS_URL="")
    result = process.call({"query": _QUERY})
    assert result.is_error is True
    assert "METRICS_URL" in str(result.content)
    assert state.requests == []


# --------------------------------------------------------------------------- #
# The credential, and configuration.
# --------------------------------------------------------------------------- #


def test_a_configured_token_is_sent_and_none_without_one(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    connector().sample()
    connector(METRICS_BEARER_TOKEN=_TOKEN).sample()
    assert [auth for *_rest, auth in state.requests] == [None, f"Bearer {_TOKEN}"]


@pytest.mark.parametrize(
    "env",
    [
        {"METRICS_URL": "file:///etc/passwd"},
        {"METRICS_URL": "example-prometheus:9090"},
        {"METRICS_TIMEOUT_SECONDS": "soon"},
        {"METRICS_TIMEOUT_SECONDS": "0"},
        {"METRICS_MAX_SERIES": "-1"},
        {"METRICS_MAX_RESPONSE_BYTES": "many"},
    ],
)
def test_a_malformed_configuration_refuses_to_start(connector: Any, env: dict[str, str]) -> None:
    process = connector(**env)
    assert process.process.wait(timeout=10) == 1
    assert "refusing to start" in process.stop()


def test_nothing_secret_or_sampled_reaches_the_log(
    connector: Any, prometheus: tuple[_Prometheus, str]
) -> None:
    state, _url = prometheus
    state.answer(_vector(({"job": "example-api"}, "31337")))
    process = connector(METRICS_BEARER_TOKEN=_TOKEN)
    assert process.sample()["value"] == "31337"
    state.answer({"status": "error", "errorType": "bad_data", "error": "example-detail"}, 400)
    process.sample()
    output = process.stop()
    assert _TOKEN not in output
    assert "31337" not in output
    assert _QUERY not in output
