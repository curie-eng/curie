"""Tests for the Tempo connector.

These cover the parts that fail SILENTLY in production. A broken URL, a
swallowed error, or an uncapped limit all leave a connector that starts, passes
a health check, and returns something plausible -- so none of them show up as a
crash. Everything here is a property that would otherwise only be noticed by
someone reading a wrong answer in Slack.
"""

import importlib.util
import os
import sys
from pathlib import Path

import anyio
import httpx
import pytest
from mcp import types as mcp_types
from mcp.server.mcpserver.exceptions import ToolError

# Load server.py BY PATH under a unique module name, rather than putting this
# directory on sys.path and importing `server`.
#
# Both connectors in this bundle ship a file called server.py. With the sys.path
# approach they collide: whichever suite imports first wins the name `server`,
# the other silently gets the wrong module, and every assertion in it fails on a
# missing attribute. That only shows up when both suites run in ONE pytest
# invocation, so it passes per-directory and breaks the moment anyone points
# pytest at examples/sre-bot/connectors.
_MODULE_NAME = "sre_bot_tempo_server"
_SERVER_PY = Path(__file__).parent / "server.py"


def _load(**env):
    """Import server.py with a specific environment.

    Module-level config is read at import time, so each case needs a fresh
    import rather than monkeypatching the constants afterwards.
    """

    base = {
        "GRAFANA_URL": "https://grafana.example.com",
        "GRAFANA_SERVICE_ACCOUNT_TOKEN": "glsa_test",
    }
    base.update(env)
    for k in ("GRAFANA_URL", "GRAFANA_SERVICE_ACCOUNT_TOKEN"):
        os.environ.pop(k, None)
    os.environ.update({k: v for k, v in base.items() if v is not None})

    sys.modules.pop(_MODULE_NAME, None)
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _SERVER_PY)
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so the module can find itself if it ever needs to;
    # under the unique name only, never as `server`.
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


def _with_one_tempo(monkeypatch, srv, proxy_get, uid="tempo-prod"):
    """Route Grafana datasource discovery, then delegate the Tempo proxy call."""

    def fake_get(url, *args, **kwargs):
        if url == "https://grafana.example.com/api/datasources":
            return httpx.Response(200, json=[{"name": "Tempo", "uid": uid}])
        return proxy_get(url, *args, **kwargs)

    monkeypatch.setattr(srv.httpx, "get", fake_get)


# --------------------------------------------------------------------------- #
# The URL. Getting this wrong yields 404s that look like "no traces".
# --------------------------------------------------------------------------- #
def test_discovers_tempo_then_returns_a_curie_span_through_grafanas_proxy(monkeypatch):
    srv = _load()
    seen = []
    payload = {
        "batches": [
            {
                "resource": {"service.name": "curie-worker"},
                "spans": [{"name": "curie.run", "traceId": "curie-trace"}],
            }
        ]
    }

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.append((url, params, headers))
        if url == "https://grafana.example.com/api/datasources":
            return httpx.Response(
                200,
                json=[
                    {"name": "Loki", "uid": "loki"},
                    {"name": "Tempo", "uid": "tempo/prod arm64"},
                ],
            )
        return httpx.Response(200, json=payload)

    monkeypatch.setattr(srv.httpx, "get", fake_get)
    assert srv.get_trace("curie/trace id") == payload
    assert [call[0] for call in seen] == [
        "https://grafana.example.com/api/datasources",
        (
            "https://grafana.example.com/api/datasources/proxy/uid/"
            "tempo%2Fprod%20arm64/api/traces/curie%2Ftrace%20id"
        ),
    ]
    assert all(headers["Authorization"] == "Bearer glsa_test" for _, _, headers in seen)
    assert all("token" not in str(params).lower() for _, params, _ in seen)


@pytest.mark.parametrize(
    "datasources",
    [
        [],
        [
            {"name": "Tempo", "uid": "tempo-a"},
            {"name": "Tempo", "uid": "tempo-b"},
        ],
    ],
)
def test_zero_or_duplicate_tempo_datasources_refuse_before_proxy(monkeypatch, datasources):
    srv = _load()
    seen = []

    def fake_get(url, **kwargs):
        seen.append(url)
        assert url == "https://grafana.example.com/api/datasources"
        return httpx.Response(200, json=datasources)

    monkeypatch.setattr(srv.httpx, "get", fake_get)
    with pytest.raises(ToolError) as excinfo:
        srv.list_trace_tags()
    assert "exactly one" in str(excinfo.value)
    assert "Tempo" in str(excinfo.value)
    assert seen == ["https://grafana.example.com/api/datasources"]


# --------------------------------------------------------------------------- #
# Limits. An uncapped trace fetch is a context-window denial of service.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("asked,expected", [(99, 50), (0, 1), (-5, 1), (10, 10), ("7", 7)])
def test_limit_is_clamped(monkeypatch, asked, expected):
    srv = _load()
    seen = {}

    def fake_get(url, params=None, **kw):
        seen["p"] = params
        return httpx.Response(200, json={})

    _with_one_tempo(monkeypatch, srv, fake_get)
    srv.search_traces("{}", limit=asked)
    assert seen["p"]["limit"] == expected


# --------------------------------------------------------------------------- #
# Errors. Every one is a ToolError carrying a SENTENCE. The sentence is what
# stops the model pasting a traceback into Slack or retrying something that will
# refuse again -- FastMCP puts the message on the wire behind a fixed
# `Error executing tool <tool_name>: ` prefix and adds nothing else, no
# traceback (the wire test below pins that shape exactly).
# Raising rather than returning is what makes the result `isError: true`, so a
# refusal is not the same wire value as an answer (see the wire test below).
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "status,must_contain",
    [
        (403, "refused"),
        (401, "refused"),
        (404, "Tempo"),
        (500, "500"),
    ],
)
def test_http_errors_become_readable_tool_errors(monkeypatch, status, must_contain):
    srv = _load()
    _with_one_tempo(
        monkeypatch,
        srv,
        lambda *a, **kw: httpx.Response(status, text="boom"),
    )
    with pytest.raises(ToolError) as excinfo:
        srv.search_traces("{}")
    assert must_contain in str(excinfo.value)


# --------------------------------------------------------------------------- #
# The response-size ceiling. `limit` bounds how many traces a search returns and
# says nothing about how big one is, so `get_trace` is the uncapped path: one
# call, one trace, potentially megabytes. The connector runs under a 256Mi limit
# and pastes what it returns into a chat channel, so an uncapped body is either
# an OOMKill that looks like a random restart, or the model's whole context
# spent on one trace.
# --------------------------------------------------------------------------- #


def test_oversized_body_is_refused_by_content_length(monkeypatch):
    # Declared size only -- the body is never read, which is the point: this
    # path must refuse without materialising the megabytes it is refusing.
    srv = _load()
    oversized = str(srv.MAX_RESPONSE_BYTES + 1)
    _with_one_tempo(
        monkeypatch,
        srv,
        lambda *a, **kw: httpx.Response(
            200, json={"ok": True}, headers={"content-length": oversized}
        ),
    )
    with pytest.raises(ToolError) as excinfo:
        srv.get_trace("abc123")
    message = str(excinfo.value)
    assert "cap" in message
    # Actionable, and explicitly not retryable -- otherwise the agent tries
    # three times and reports a timeout.
    assert "Narrow the query" in message
    assert "refused again" in message


def test_oversized_body_is_refused_when_content_length_is_absent(monkeypatch):
    # A very large trace comes back chunked, so there is no content-length to
    # check. Falling back to the received body's size is what closes that hole.
    srv = _load()
    big = {"spans": ["x" * 1000] * ((srv.MAX_RESPONSE_BYTES // 1000) + 20)}
    resp = httpx.Response(200, json=big)
    del resp.headers["content-length"]
    _with_one_tempo(monkeypatch, srv, lambda *a, **kw: resp)
    with pytest.raises(ToolError) as excinfo:
        srv.get_trace("abc123")
    assert "cap" in str(excinfo.value)


def test_a_normal_sized_trace_is_still_returned(monkeypatch):
    # The bound must not swallow the ordinary case; a grader that only checks
    # the refusal would pass with the cap set to zero.
    srv = _load()
    payload = {"batches": [{"spans": [{"name": "GET /health"}]}]}
    _with_one_tempo(monkeypatch, srv, lambda *a, **kw: httpx.Response(200, json=payload))
    assert srv.get_trace("abc123") == payload


def test_search_results_are_capped_too(monkeypatch):
    # The bound lives in _proxy rather than in get_trace, so every tool is
    # covered -- a wide TraceQL search can also return more than the ceiling.
    srv = _load()
    oversized = str(srv.MAX_RESPONSE_BYTES + 1)
    _with_one_tempo(
        monkeypatch,
        srv,
        lambda *a, **kw: httpx.Response(
            200, json={"traces": []}, headers={"content-length": oversized}
        ),
    )
    with pytest.raises(ToolError) as excinfo:
        srv.search_traces("{}")
    assert "cap" in str(excinfo.value)


def test_auth_failure_says_it_will_not_fix_itself(monkeypatch):
    # Without this the agent retries a 403 three times and reports a timeout.
    srv = _load()
    _with_one_tempo(monkeypatch, srv, lambda *a, **kw: httpx.Response(403))
    with pytest.raises(ToolError) as excinfo:
        srv.search_traces("{}")
    assert "not fix itself" in str(excinfo.value)


def test_timeout_is_a_sentence_not_a_traceback(monkeypatch):
    # It IS an exception now -- a ToolError -- but that is not the property that
    # matters here. What matters is that the httpx traceback never reaches the
    # model: it gets one sentence naming the timeout and what to narrow, which
    # is what stops the retry loop.
    srv = _load()

    def boom(*a, **kw):
        raise httpx.TimeoutException("too slow")

    _with_one_tempo(monkeypatch, srv, boom)
    with pytest.raises(ToolError) as excinfo:
        srv.search_traces("{}")
    message = str(excinfo.value)
    assert "did not respond" in message
    assert "too slow" not in message


def test_transport_error_does_not_leak_the_token(monkeypatch):
    srv = _load()

    def boom(*a, **kw):
        raise httpx.ConnectError("connection refused")

    _with_one_tempo(monkeypatch, srv, boom)
    with pytest.raises(ToolError) as excinfo:
        srv.search_traces("{}")
    assert "glsa_test" not in str(excinfo.value)


# --------------------------------------------------------------------------- #
# Guard rails on input
# --------------------------------------------------------------------------- #
def test_empty_trace_id_is_rejected_before_a_request(monkeypatch):
    srv = _load()

    def explode(*a, **kw):
        raise AssertionError("should not have called Grafana")

    monkeypatch.setattr(srv.httpx, "get", explode)
    with pytest.raises(ToolError) as excinfo:
        srv.get_trace("   ")
    assert "required" in str(excinfo.value)
    with pytest.raises(ToolError) as excinfo:
        srv.list_trace_tag_values("")
    assert "required" in str(excinfo.value)


# --------------------------------------------------------------------------- #
# The wire. Every case above calls the tool function directly, and none of them
# can see the field the connector is actually judged by. FastMCP marks a result
# `isError: true` only when the call RAISED; a refusal that is `return`ed is a
# string like any other, so it goes out as `isError: false` and reads as an
# answer to anything consuming the protocol rather than the prose -- an eval
# grader, a ledger, a retry policy. "Grafana refused the request" and "no traces
# matched" were the same wire result, and the difference between them is the
# whole reason to ask Tempo anything.
#
# So this one goes through the real MCP request path, in process.
# --------------------------------------------------------------------------- #
def _call_tool(srv, name, args):
    """Call one tool through the real MCP request path and return the CallToolResult."""

    async def go():
        entry = srv.mcp._lowlevel_server.get_request_handler("tools/call")
        assert entry is not None
        return await entry.handler(None, mcp_types.CallToolRequestParams(name=name, arguments=args))

    return anyio.run(go)


def test_a_refused_read_and_an_answered_one_carry_different_is_error_flags(monkeypatch):
    srv = _load()

    _with_one_tempo(monkeypatch, srv, lambda *a, **kw: httpx.Response(403))
    refused = _call_tool(srv, "get_trace", {"trace_id": "abc123"})
    assert refused.is_error is True
    # The sentence survives the trip behind a fixed SDK prefix: FastMCP puts our
    # message text on the wire, never a traceback, so raising costs nothing the
    # prose was doing.
    assert "not fix itself" in refused.content[0].text

    # The prefix, pinned. OBSERVED against the pinned mcp==1.28.1, which builds
    # it at mcp/server/fastmcp/tools/base.py:117
    # (`raise ToolError(f"Error executing tool {self.name}: {e}") from e`) and
    # puts str(e) in as the only text content. Pinned rather than
    # substring-matched so a version bump that reshapes or drops the prefix
    # fails here instead of quietly changing what an operator reads in Slack.
    prefix = "Error executing tool get_trace: "
    refusal_text = refused.content[0].text
    assert refusal_text.startswith(prefix)
    # ...and what follows it is _proxy's sentence verbatim, so rewording the
    # refusal fails here too, not just changing the prefix.
    assert refusal_text[len(prefix) :] == (
        "Grafana refused the request (403). The service account token is "
        "missing or lacks access to the Tempo datasource. This will not fix "
        "itself on retry."
    )

    payload = {"batches": [{"spans": [{"name": "GET /health"}]}]}
    _with_one_tempo(
        monkeypatch,
        srv,
        lambda *a, **kw: httpx.Response(200, json=payload),
    )
    answered = _call_tool(srv, "get_trace", {"trace_id": "abc123"})
    assert answered.is_error is False
    assert "GET /health" in answered.content[0].text
    # The contrast in the OTHER direction: an answered read carries no prefix at
    # all, so the two results differ in text shape as well as in the flag.
    assert not answered.content[0].text.startswith("Error executing tool")

    # THE contrast, stated outright: this is the whole property. While both were
    # returned strings these two results had the same shape, and a program
    # reading them could not tell a refusal from a trace.
    assert refused.is_error != answered.is_error


def test_every_tool_is_annotated_read_only():
    # readOnlyHint is not the boundary -- the Viewer token is -- but a
    # write-shaped tool should never reach a prompt-injectable agent's context.
    srv = _load()
    assert srv.READ_ONLY.read_only_hint is True
    assert srv.READ_ONLY.destructive_hint is False


def test_streamable_http_is_mounted_at_curie_connector_path():
    srv = _load()
    assert [route.path for route in srv.mcp.streamable_http_app().routes] == ["/mcp"]


def test_missing_config_refuses_rather_than_answering_nonsense(monkeypatch):
    # A connector that answers "not configured" to every call looks healthy to
    # Kubernetes and is useless to the agent, so main() exits non-zero.
    srv = _load(GRAFANA_URL="", GRAFANA_SERVICE_ACCOUNT_TOKEN="")
    assert srv.main() == 1
