"""Tests for the curie-manager `platform` connector.

The ones that carry the security argument:

- `test_no_tool_reaches_a_credential_endpoint`: the platform key can mint
  logins and approval principals and read webhook secrets. This connector holds
  that key, so the only thing keeping those powers from the agent is that no
  tool calls those routes.
- `test_every_destructive_tool_is_gated_and_every_tool_is_classified`: the
  bundle's toolPolicy fails closed on a tool it does not name, and gates the
  destructive ones. A new tool that is not added there is unusable, and a delete
  that is listed under `allow` instead of `approvalRequired` would run without
  a human.
- `test_the_server_refuses_a_caller_without_the_token`: on the local tier
  nothing but this check stands between other sandboxes and the platform key.
"""

import importlib.util
import json
import sys
from pathlib import Path

import anyio
import httpx
import pytest
from mcp.server.mcpserver.exceptions import ToolError

_MODULE_NAME = "curie_manager_platform_server"
_SERVER_PY = Path(__file__).parent / "server.py"
_MANIFEST = Path(__file__).parents[2] / ".claude-plugin" / "plugin.json"

KEY = "platform-key-for-tests"
TOKEN = "mcp-token-for-tests"
SELF_ID = "11111111-1111-1111-1111-111111111111"
OTHER_ID = "22222222-2222-2222-2222-222222222222"
VERSION_ID = "33333333-3333-3333-3333-333333333333"
AGENTS = [
    {"id": SELF_ID, "name": "curie-manager", "channels": [], "created_at": "2026-10-02T00:00:00Z"},
    {
        "id": OTHER_ID,
        "name": "acme-bot",
        "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}],
        "created_at": "2026-10-01T00:00:00Z",
    },
]


def _load(monkeypatch, token=TOKEN, key=KEY):
    monkeypatch.setenv("MANAGER_PLATFORM_KEY", key)
    monkeypatch.setenv("MANAGER_MCP_TOKEN", token)
    monkeypatch.setenv("MANAGER_SELF_AGENT", "curie-manager")
    monkeypatch.setenv("PLATFORM_API_URL", "http://curie-api.test:8000")
    sys.modules.pop(_MODULE_NAME, None)
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _SERVER_PY)
    module = importlib.util.module_from_spec(spec)
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


class _Platform:
    """A stand-in platform API behind a real httpx.Client, recording every call."""

    def __init__(self, budget=None):
        self.calls: list[tuple[str, str, dict, object]] = []
        self.budget = budget or {"max_usd_per_day": 5.0, "max_output_tokens_per_run": 100000}

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        self.calls.append((request.method, request.url.path, dict(request.url.params), body))
        assert request.headers["x-api-key"] == KEY
        path = request.url.path
        if path == "/agents":
            return httpx.Response(200, json=AGENTS)
        if path == f"/agents/{OTHER_ID}/versions":
            return httpx.Response(200, json=[{"id": VERSION_ID, "agent_id": OTHER_ID}])
        if path == f"/agents/{OTHER_ID}/budget":
            if request.method == "PUT":
                self.budget = body
            return httpx.Response(200, json=self.budget)
        if path == "/deployments" and request.method == "POST":
            return httpx.Response(201, json={"id": "d1", **body})
        if request.method == "DELETE":
            return httpx.Response(204)
        if path.endswith("/kill"):
            return httpx.Response(200, json={"killed": True})
        return httpx.Response(200, json={})

    def client(self, srv):
        return httpx.Client(
            base_url=srv.API_URL,
            headers={"X-API-Key": srv.PLATFORM_KEY},
            transport=httpx.MockTransport(self.handler),
        )


@pytest.fixture
def platform(monkeypatch):
    srv = _load(monkeypatch)
    fake = _Platform()
    monkeypatch.setattr(srv, "_client", lambda: fake.client(srv))
    return srv, fake


def _tools(srv):
    return anyio.run(srv.mcp.list_tools)


def test_no_tool_reaches_a_credential_endpoint():
    source = _SERVER_PY.read_text()
    for route in (
        "hook-secret",
        "/console/",
        "/approvals/principals",
        "/resolve",
        "/channels/token",
        "/channels/callers",
    ):
        assert route not in source.replace("`GET /agents/{id}/hook-secret`", ""), route


def test_the_platform_key_is_sent_as_the_api_key_header_and_never_returned(platform):
    srv, fake = platform
    reply = srv.get_agent("acme-bot")
    assert fake.calls, "no request reached the platform"
    assert KEY not in json.dumps(reply)


def test_an_unknown_agent_names_the_real_ones(platform):
    srv, _ = platform
    with pytest.raises(ToolError) as excinfo:
        srv.get_agent("nope")
    assert "acme-bot" in str(excinfo.value)


@pytest.mark.parametrize("tool", ["kill_agent", "delete_agent"])
def test_it_refuses_to_kill_or_delete_itself(platform, tool):
    srv, fake = platform
    with pytest.raises(ToolError):
        getattr(srv, tool)("curie-manager")
    assert not [c for c in fake.calls if c[0] in ("POST", "DELETE")]


def test_kill_reaches_the_named_agent(platform):
    srv, fake = platform
    assert srv.kill_agent("acme-bot")["killed"] is True
    assert ("POST", f"/agents/{OTHER_ID}/kill") in [(m, p) for m, p, _, _ in fake.calls]


def test_set_budget_keeps_the_limit_it_was_not_given(platform):
    srv, fake = platform
    reply = srv.set_budget("acme-bot", max_usd_per_day=12.5)
    assert reply["after"] == {"max_usd_per_day": 12.5, "max_output_tokens_per_run": 100000}
    assert reply["before"]["max_usd_per_day"] == 5.0


def test_set_budget_with_nothing_to_change_writes_nothing(platform):
    srv, fake = platform
    with pytest.raises(ToolError):
        srv.set_budget("acme-bot")
    assert not [c for c in fake.calls if c[0] == "PUT"]


def test_deploy_version_only_deploys_a_version_the_agent_has(platform):
    srv, fake = platform
    with pytest.raises(ToolError):
        srv.deploy_version("acme-bot", "44444444-4444-4444-4444-444444444444")
    reply = srv.deploy_version("acme-bot", VERSION_ID)
    assert reply["version_id"] == VERSION_ID and reply["agent_id"] == OTHER_ID
    assert reply["environment"] == "prod"


def test_delete_memory_passes_the_version_guard(platform):
    srv, fake = platform
    srv.delete_memory("acme-bot", 2, 7)
    method, path, params, _ = fake.calls[-1]
    assert (method, path, params) == (
        "DELETE",
        f"/agents/{OTHER_ID}/memory/2",
        {"expected_version": "7"},
    )


def test_a_platform_error_reaches_the_model_as_a_tool_error(monkeypatch):
    srv = _load(monkeypatch)
    transport = httpx.MockTransport(lambda r: httpx.Response(409, json={"detail": "conflict"}))
    monkeypatch.setattr(
        srv,
        "_client",
        lambda: httpx.Client(base_url=srv.API_URL, transport=transport),
    )
    with pytest.raises(ToolError) as excinfo:
        srv.list_agents()
    assert "409" in str(excinfo.value) and "conflict" in str(excinfo.value)


def test_every_destructive_tool_is_gated_and_every_tool_is_classified(monkeypatch):
    srv = _load(monkeypatch)
    policy = json.loads(_MANIFEST.read_text())["toolPolicy"]
    allowed = {p.split("/", 1)[1] for p in policy["allow"]}
    gated = {p.split("/", 1)[1] for p in policy["approvalRequired"]}
    assert all(p.startswith("platform/") for p in policy["allow"] + policy["approvalRequired"])
    tools = {t.name: t for t in _tools(srv)}
    destructive = {name for name, t in tools.items() if t.annotations.destructive_hint}
    assert destructive == gated, "every destructive tool, and only those, waits for approval"
    assert set(tools) == allowed | gated, "a tool the policy does not name is refused"
    assert not allowed & gated
    for name in allowed:
        assert not tools[name].annotations.destructive_hint


def test_the_server_refuses_a_caller_without_the_token(monkeypatch):
    srv = _load(monkeypatch)

    async def inner(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    app = srv.BearerAuth(inner, TOKEN)

    async def status(headers):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://c") as client:
            return (await client.post("/mcp", headers=headers)).status_code

    assert anyio.run(status, {}) == 401
    assert anyio.run(status, {"Authorization": "Bearer wrong"}) == 401
    assert anyio.run(status, {"Authorization": f"Bearer {TOKEN}"}) == 200


@pytest.mark.parametrize("missing", ["MANAGER_MCP_TOKEN", "MANAGER_PLATFORM_KEY"])
def test_it_will_not_start_without_both_secrets(monkeypatch, missing):
    srv = _load(monkeypatch)
    attr = {"MANAGER_MCP_TOKEN": "MCP_TOKEN", "MANAGER_PLATFORM_KEY": "PLATFORM_KEY"}[missing]
    monkeypatch.setattr(srv, attr, "")
    with pytest.raises(SystemExit):
        srv.build_app()


def test_metrics_for_one_agent_filter_on_its_trace_token_not_its_name(platform):
    # The API's `agent` filter is a trace-name contains match on
    # `agent-<id>` (curie_api.metrics.agent_trace_filter). The name matches no
    # trace, so passing it read as zero runs for an agent that had run.
    srv, fake = platform
    srv.metrics_summary("acme-bot")
    method, path, params, _ = fake.calls[-1]
    assert (method, path) == ("GET", "/observability/metrics/summary")
    assert params == {"agent": f"agent-{OTHER_ID}"}
