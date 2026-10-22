"""The runner hosts a bundle's declared route callers (ADR 0240 decision 4).

The relay is driven through the agent SDK's own in-process server bridge, the
code that answers the CLI's ``mcp_message`` control requests, against a real
stdio child, so what the caller receives is observed rather than assumed.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import BootEnv, Budget
from claude_agent_sdk._internal.sdk_mcp_bridge import SdkMcpBridge
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.plugin import bundle_mcp_servers
from curie_runner.route_callers import (
    ROUTE_TOKENS_ENV,
    ROUTES_FD_ENV,
    CallerRelay,
    caller_launch,
    connector_urls,
    mount_route_callers,
    route_callers,
    take_route_tokens,
)
from runner_boot_session import initialized_session_sync

_SERVER = Path(__file__).parent / "fixtures" / "route_caller_server.py"
_TOKENS = {"reviewer": "ccr.reviewer-payload.signature"}
_SCOPE = {"release": "curie", "agent": "acme-dev", "namespace": "curie"}
_CONNECTORS = (
    "connectors:\n"
    "  reviewer:\n"
    "    image: ghcr.io/example/reviewer:1\n"
    "    port: 8080\n"
    "    routes:\n"
    "      - path: /v1/review\n"
    "        max_body_bytes: 67108864\n"
    "        callers: [review-tools]\n"
    "  grafana:\n"
    "    image: ghcr.io/example/grafana:1\n"
)
_REVIEWER_URL = "http://curie-acme-dev-mcp-reviewer.curie.svc.cluster.local:8080"


def _bundle(root: Path, *, connectors: str = _CONNECTORS) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "b", "version": "0.1.0", "description": "t"}), encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(connectors, encoding="utf-8")
    (root / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "review-tools": {
                        "command": sys.executable,
                        "args": [str(_SERVER)],
                        "env": {"REVIEW_MODE": "${REVIEW_MODE}"},
                    },
                    "notes": {"command": sys.executable, "args": ["-c", "pass"]},
                }
            }
        ),
        encoding="utf-8",
    )
    return root


# @spec ADR-0240 d4
def test_the_route_tokens_leave_every_env_even_when_malformed() -> None:
    process = {ROUTE_TOKENS_ENV: json.dumps({**_TOKENS, "bad": 1, "cct": "cct.x.y"})}
    spawn = dict(process)
    assert take_route_tokens(spawn, process) == _TOKENS
    assert ROUTE_TOKENS_ENV not in process
    assert ROUTE_TOKENS_ENV not in spawn
    broken = {ROUTE_TOKENS_ENV: "not json"}
    assert take_route_tokens(broken) == {}
    assert broken == {}


# @spec ADR-0240 d4
def test_callers_and_urls_come_from_the_mounted_declaration(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    assert route_callers(root, agent="acme-dev") == {"review-tools": frozenset({"reviewer"})}
    assert connector_urls(root, **_SCOPE) == {"reviewer": _REVIEWER_URL}
    # Without the connector scope there is no Service to call.
    assert connector_urls(root, release=None, agent="acme-dev", namespace=None) == {}
    (root / "deploy.yaml").write_text(
        "targets:\n  dev:\n    agent: acme-dev\n    env: dev\n    slack_channel: C0EXAMPLE2\n"
        "    connectors: [grafana]\n",
        encoding="utf-8",
    )
    # A connector this agent does not run calls nobody.
    assert route_callers(root, agent="acme-dev") == {}


# @spec ADR-0240 d4
def test_a_caller_launch_holds_no_platform_credential(tmp_path: Path) -> None:
    servers = bundle_mcp_servers(str(_bundle(tmp_path)))
    entry = servers["plugin:b:review-tools"]
    source = {
        "PATH": "/usr/bin:/bin",
        "REVIEW_MODE": "deep",
        "CURIE_STATE_TOKEN": "sbx.state",
        "CURIE_CONNECTOR_CALLER_TOKEN": "cct.caller",
        ROUTE_TOKENS_ENV: json.dumps(_TOKENS),
        "ANTHROPIC_API_KEY": "example-model-key",
        "LD_PRELOAD": "/opt/other.so",
    }
    launch = caller_launch(
        entry,
        routes={"reviewer": {"url": _REVIEWER_URL, "token": _TOKENS["reviewer"]}},
        source_env=source,
        lock_library="/opt/libproc_dumpable.so",
        cwd=str(tmp_path),
    )
    assert launch.env["REVIEW_MODE"] == "deep"
    for name in ("CURIE_STATE_TOKEN", "CURIE_CONNECTOR_CALLER_TOKEN", "ANTHROPIC_API_KEY"):
        assert name not in launch.env
    assert ROUTE_TOKENS_ENV not in launch.env
    assert launch.env["LD_PRELOAD"] == "/opt/libproc_dumpable.so:/opt/other.so"
    assert "ccr." not in json.dumps(dict(launch.env))
    assert "ccr." not in repr(launch)
    assert json.loads(launch.routes_document) == {
        "reviewer": {"url": _REVIEWER_URL, "token": _TOKENS["reviewer"]}
    }


# @spec ADR-0240 d4
def test_only_declared_callers_are_replaced_by_a_relay(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    servers = bundle_mcp_servers(str(root))
    notes = dict(servers["plugin:b:notes"])
    mounted = mount_route_callers(
        servers,
        callers=route_callers(root, agent="acme-dev"),
        tokens=_TOKENS,
        connector_urls=connector_urls(root, **_SCOPE),
        source_env={"PATH": "/usr/bin:/bin"},
        lock_library="/opt/libproc_dumpable.so",
        cwd=None,
    )
    assert mounted == frozenset({"plugin:b:review-tools"})
    relay = servers["plugin:b:review-tools"]
    assert relay["type"] == "sdk"
    assert relay["name"] == "plugin:b:review-tools"
    assert isinstance(relay["instance"], CallerRelay)
    assert "ccr." not in json.dumps({k: v for k, v in relay.items() if k != "instance"})
    assert servers["plugin:b:notes"] == notes


# @spec ADR-0240 d4: an unlocked caller is not mounted at all.
def test_without_the_lock_library_a_caller_is_not_mounted(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    servers = bundle_mcp_servers(str(root))
    mounted = mount_route_callers(
        servers,
        callers=route_callers(root, agent="acme-dev"),
        tokens=_TOKENS,
        connector_urls=connector_urls(root, **_SCOPE),
        source_env={},
        lock_library=None,
        cwd=None,
    )
    assert mounted == frozenset()
    assert "plugin:b:review-tools" not in servers
    assert "plugin:b:notes" in servers


async def _call(relay: CallerRelay) -> dict[str, Any]:
    bridge = SdkMcpBridge(relay.name, relay)  # type: ignore[arg-type]
    try:
        initialized = await bridge.handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "cli", "version": "1"},
                },
            }
        )
        assert initialized is not None
        assert initialized["result"]["serverInfo"]["name"] == "review-tools"
        await bridge.handle({"jsonrpc": "2.0", "method": "notifications/initialized"})
        listed = await bridge.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        assert listed is not None
        assert [tool["name"] for tool in listed["result"]["tools"]] == ["inspect"]
        called = await bridge.handle(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "inspect", "arguments": {}},
            }
        )
        assert called is not None
        report: dict[str, Any] = json.loads(called["result"]["content"][0]["text"])
        return report
    finally:
        await bridge.aclose()


# @spec ADR-0240 d4
def test_the_relay_hands_the_caller_its_routes_on_a_pipe(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    servers = bundle_mcp_servers(str(root))
    mount_route_callers(
        servers,
        callers=route_callers(root, agent="acme-dev"),
        tokens=_TOKENS,
        connector_urls=connector_urls(root, **_SCOPE),
        source_env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "CURIE_STATE_TOKEN": "sbx.state",
            ROUTE_TOKENS_ENV: json.dumps(_TOKENS),
        },
        # A real library is Linux-only; the child only needs to start here.
        lock_library="/nonexistent/libproc_dumpable.so",
        cwd=str(tmp_path),
    )
    relay = servers["plugin:b:review-tools"]["instance"]
    report = anyio.run(_call, relay)
    assert report["routes"] == {"reviewer": {"url": _REVIEWER_URL, "token": _TOKENS["reviewer"]}}
    assert ROUTES_FD_ENV in report["env_names"]
    assert ROUTE_TOKENS_ENV not in report["env_names"]
    assert "CURIE_STATE_TOKEN" not in report["env_names"]
    assert report["ld_preload"].split(":")[0] == "/nonexistent/libproc_dumpable.so"
    assert Path(report["cwd"]).resolve() == tmp_path.resolve()


# @spec ADR-0240 d4: each session starts its own caller with a fresh pipe.
def test_a_second_session_gets_the_routes_again(tmp_path: Path) -> None:
    root = _bundle(tmp_path)
    servers = bundle_mcp_servers(str(root))
    mount_route_callers(
        servers,
        callers=route_callers(root, agent="acme-dev"),
        tokens=_TOKENS,
        connector_urls=connector_urls(root, **_SCOPE),
        source_env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        lock_library="/nonexistent/libproc_dumpable.so",
        cwd=None,
    )
    relay = servers["plugin:b:review-tools"]["instance"]
    first = anyio.run(_call, relay)
    second = anyio.run(_call, relay)
    assert (
        first["routes"]
        == second["routes"]
        == {"reviewer": {"url": _REVIEWER_URL, "token": _TOKENS["reviewer"]}}
    )


class _CapturedSession:
    def __init__(self, options: Any) -> None:
        self.options = options

    async def connect(self) -> None:
        return None

    async def query(self, _text: str) -> None:
        return None

    async def interrupt(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def receive_turn(self):  # type: ignore[no-untyped-def]
        if False:
            yield None


# @spec ADR-0240 d4
def test_a_boot_mounts_the_caller_as_a_relay_and_drops_the_tokens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CURIE_STATE_URL", raising=False)
    root = _bundle(tmp_path)
    env = BootEnv.render_worker(
        plugin_dir=str(root),
        session_id="agent-abc-thread-1",
        budget=Budget(max_output_tokens_per_run=4096, max_usd_per_day=5.0),
        memory_ref="http://api:8000/agents/agent-abc/state/memory",
        history_ref="http://api:8000/agents/agent-abc/state/transcript/t1",
        connector_release="curie",
        connector_agent="acme-dev",
        connector_namespace="curie",
        connector_caller_token="cct.payload.signature",
    ) | {"CURIE_SANDBOX_ID": "sbx-1", "CURIE_RUNNER_PORT": "8080"}
    config = RunnerConfig.from_env(env)
    spawn = {ROUTE_TOKENS_ENV: json.dumps(_TOKENS), "CURIE_MODEL": "claude-sonnet-5"}
    monkeypatch.setenv(ROUTE_TOKENS_ENV, json.dumps(_TOKENS))

    async def probe(*_args: Any, **_kwargs: Any) -> McpToolCapabilityProbe:
        return McpToolCapabilityProbe(complete=True, has_potential_write_tool=False, tool_count=0)

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    monkeypatch.setattr(boot, "route_caller_lock_library", lambda _env: "/opt/lock.so")
    session = initialized_session_sync(build_runner(config, fake_model=False, sdk_env=spawn))
    assert isinstance(session, _CapturedSession)
    servers = session.options.mcp_servers
    assert servers["plugin:b:review-tools"]["type"] == "sdk"
    assert servers["plugin:b:notes"]["command"] == sys.executable
    assert ROUTE_TOKENS_ENV not in os.environ
    assert ROUTE_TOKENS_ENV not in spawn
    assert spawn["CURIE_MODEL"] == "claude-sonnet-5"


# @spec ADR-0240 d4: the capability probe dials the caller's own declaration,
# with no descriptor, and the caller still lists its tools.
def test_the_capability_probe_still_lists_a_callers_tools(tmp_path: Path) -> None:
    from curie_runner.mcp_tool_capability import probe_mcp_tool_capability

    root = _bundle(tmp_path)
    probe = anyio.run(
        probe_mcp_tool_capability, str(root), {}, {"PATH": os.environ.get("PATH", "")}
    )
    # ``notes`` is no MCP server at all; only the caller is under test.
    assert "review-tools" not in probe.failures
    assert "mcp__plugin_b_review-tools__inspect" in probe.observed_tools
