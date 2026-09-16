"""Hosted-connector MCP catalog visibility for approval-required mutations.

#2217 covers plugin-prefixed `.mcp.json` servers. The live #2201 recurrence is
the connectors.yaml mount: tools are named `mcp__<server>__<tool>`, and a later
resume on the same release can drop mutations from the SDK catalog while a
fresh session still sees them. These tests exercise that mount shape.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from shutil import which
from typing import Any
from uuid import uuid4

import anyio
import pytest
import uvicorn
from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
from claude_agent_sdk import SystemMessage
from claude_agent_sdk.types import PermissionResultDeny
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import (
    build_approval_gate,
    build_approval_hook,
    build_can_use_tool,
    resolve_approval_policy,
)
from curie_runner.connectors import derive_mcp_servers
from curie_runner.history import TurnRecord, build_conversation_replay
from curie_runner.mcp_tool_capability import probe_mcp_tool_capability
from mcp.server import MCPServer
from mcp.types import ToolAnnotations
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.routing import Route

from .test_connectors import _CapturedSession, _config_for

READ_ALLOWED = "mcp__operations__read_allowed"
WRITE_APPROVAL = "mcp__operations__write_approval"
WRITE_DENIED = "mcp__operations__write_denied"
WRITE_UNMATCHED = "mcp__operations__write_unmatched"

_HAS_CLAUDE_CLI = which("claude") is not None


class _TranscriptStore:
    def __init__(self) -> None:
        self.records: list[TurnRecord] = []

    async def load(self) -> list[TurnRecord]:
        return list(self.records)

    async def append(self, record: TurnRecord) -> None:
        self.records.append(record)


class _LoopbackHttp:
    """One local ASGI server on an ephemeral loopback port."""

    def __init__(self, app: Any) -> None:
        self._app = app
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self._socket: socket.socket | None = None

    def start(self) -> str:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(32)
        self._socket = listener
        host, port = listener.getsockname()[:2]
        self._server = uvicorn.Server(
            uvicorn.Config(
                self._app,
                host=str(host),
                port=int(port),
                access_log=False,
                log_level="critical",
                lifespan="on",
            )
        )
        self._thread = threading.Thread(
            target=self._server.run,
            kwargs={"sockets": [listener]},
            daemon=True,
        )
        self._thread.start()
        for _ in range(200):
            if self._server.started:
                return f"http://{host}:{port}"
            time.sleep(0.02)
        raise RuntimeError("loopback server did not start")

    def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._socket is not None:
            self._socket.close()


@contextmanager
def _serve(app: Any) -> Iterator[str]:
    server = _LoopbackHttp(app)
    try:
        yield server.start()
    finally:
        server.stop()


def _policy_catalog_app(calls: list[str]) -> Any:
    server = MCPServer("operations")

    @server.tool(
        name="read_allowed",
        annotations=ToolAnnotations(readOnlyHint=True),
        description="Read a test value without changing external state.",
    )
    def read_allowed() -> str:
        calls.append("read_allowed")
        return "read"

    @server.tool(
        name="write_approval",
        annotations=ToolAnnotations(readOnlyHint=False),
        description="Write one test marker after approval.",
    )
    def write_approval(value: str = "") -> str:
        calls.append("write_approval")
        return f"executed write_approval {value}"

    @server.tool(
        name="write_denied",
        annotations=ToolAnnotations(readOnlyHint=False),
        description="A test write forbidden by policy.",
    )
    def write_denied() -> str:
        calls.append("write_denied")
        return "executed write_denied"

    @server.tool(
        name="write_unmatched",
        description="A test write omitted from policy.",
    )
    def write_unmatched() -> str:
        calls.append("write_unmatched")
        return "executed write_unmatched"

    return server.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        host="127.0.0.1",
    )


def _write_bundle(root: Path, connector_url: str) -> Path:
    bundle = root / "bundle"
    (bundle / ".claude-plugin").mkdir(parents=True)
    (bundle / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": "acme-bot",
                "version": "0.0.0",
                "description": "Hosted MCP approval catalog fixture.",
                "toolPolicy": {
                    "enforcement": "curie/mcp-tool-policy@1",
                    "allow": ["operations/read_allowed"],
                    "approvalRequired": ["operations/write_approval"],
                    "deny": ["operations/write_denied"],
                },
            }
        ),
        encoding="utf-8",
    )
    (bundle / "connectors.yaml").write_text(
        "connectors:\n"
        "  operations:\n"
        "    image: example/operations:0.0.0\n"
        f"    unhosted_url: {connector_url}/mcp\n",
        encoding="utf-8",
    )
    return bundle


def _gate_for(bundle: Path):
    resolution = resolve_approval_policy(str(bundle))
    gate = build_approval_gate(
        operator_tools=None,
        policy_routes=resolution.route_by_tool,
        grantable_by_route=resolution.grantable_by_route,
        summary_by_tool=resolution.summary_by_tool,
        bundle_name=resolution.bundle_name,
        mcp_servers=resolution.mcp_servers,
        connector_servers=resolution.connector_servers,
        tool_policy=resolution.tool_policy,
    )
    assert gate is not None
    return gate


def _hook_call(
    gate: Any, tool_name: str, tool_input: dict[str, Any] | None = None
) -> dict[str, Any]:
    matcher = build_approval_hook(gate)["PreToolUse"][0]
    return anyio.run(
        matcher.hooks[0],
        {"tool_name": tool_name, "tool_input": tool_input or {}},
        None,
        None,
    )


def _hook_denied(result: dict[str, Any]) -> bool:
    decision = (result.get("hookSpecificOutput") or {}).get("permissionDecision")
    return decision == "deny"


def _boot_captured(monkeypatch: pytest.MonkeyPatch, bundle: Path):
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    runner = build_runner(_config_for(bundle), fake_model=False)
    session = runner._factory()
    assert isinstance(session, _CapturedSession)
    return runner, session.options


def test_hosted_connector_boot_keeps_approval_tools_visible_and_hides_refusals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    with _serve(_policy_catalog_app(calls)) as connector_url:
        bundle = _write_bundle(tmp_path, connector_url)
        derived = derive_mcp_servers(bundle, release=None, agent=None, namespace=None)
        observed = anyio.run(probe_mcp_tool_capability, bundle, derived, {})
        runner, options = _boot_captured(monkeypatch, bundle)

    assert observed.complete
    assert {READ_ALLOWED, WRITE_APPROVAL, WRITE_DENIED, WRITE_UNMATCHED} <= set(
        observed.observed_tools
    )
    hidden = set(options.disallowed_tools)
    assert WRITE_APPROVAL not in hidden
    assert READ_ALLOWED not in hidden
    assert {WRITE_DENIED, WRITE_UNMATCHED} <= hidden
    assert options.mcp_servers["operations"]["url"] == f"{connector_url}/mcp"
    assert runner._approval_gate is not None
    assert runner._approval_gate.grant_tool is None
    assert calls == []


def test_hosted_connector_denied_tools_are_refused_without_an_approval(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    with _serve(_policy_catalog_app(calls)) as connector_url:
        bundle = _write_bundle(tmp_path, connector_url)

    for tool_name in (WRITE_DENIED, WRITE_UNMATCHED):
        hook_gate = _gate_for(bundle)
        assert _hook_denied(_hook_call(hook_gate, tool_name))
        assert hook_gate.pending_summary is None

        callback_gate = _gate_for(bundle)
        callback_result = anyio.run(build_can_use_tool(callback_gate), tool_name, {}, None)
        assert isinstance(callback_result, PermissionResultDeny)
        assert callback_gate.pending_summary is None

    approval_gate = _gate_for(bundle)
    assert _hook_denied(_hook_call(approval_gate, WRITE_APPROVAL, {"value": "once"}))
    assert approval_gate.pending_granted_tool == WRITE_APPROVAL
    assert calls == []


class _ProviderCapture:
    """Anthropic-compatible loopback that records outbound tool names only."""

    def __init__(self, *, tool_use: str | None = None) -> None:
        self.tool_lists: list[tuple[str, ...]] = []
        self._tool_use = tool_use

    @staticmethod
    def _names(payload: object) -> tuple[str, ...]:
        if not isinstance(payload, dict):
            return ()
        tools = payload.get("tools")
        if not isinstance(tools, list):
            return ()
        return tuple(
            sorted(
                name
                for item in tools
                if isinstance(item, dict)
                and isinstance((name := item.get("name")), str)
                and name
            )
        )

    async def respond(self, request: Request) -> StreamingResponse:
        try:
            body: object = await request.json()
        except json.JSONDecodeError:
            body = None
        self.tool_lists.append(self._names(body))
        if self._tool_use is None:
            frames = _text_frames("ok")
        else:
            frames = _tool_use_frames(self._tool_use, {"value": "approved-once"})
        return StreamingResponse(
            _sse(frames),
            media_type="text/event-stream",
            headers={"cache-control": "no-cache"},
        )

    def app(self) -> Starlette:
        return Starlette(routes=[Route("/{path:path}", self.respond, methods=["POST"])])


def _text_frames(text: str) -> tuple[tuple[str, dict[str, Any]], ...]:
    message = {
        "id": "msg_loopback",
        "type": "message",
        "role": "assistant",
        "content": [],
        "model": "claude-loopback",
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    return (
        ("message_start", {"type": "message_start", "message": message}),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    )


def _tool_use_frames(
    name: str, tool_input: dict[str, Any]
) -> tuple[tuple[str, dict[str, Any]], ...]:
    message = {
        "id": "msg_loopback",
        "type": "message",
        "role": "assistant",
        "content": [],
        "model": "claude-loopback",
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    payload = json.dumps(tool_input, separators=(",", ":"))
    return (
        ("message_start", {"type": "message_start", "message": message}),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "tool_use",
                    "id": "toolu_loopback_1",
                    "name": name,
                    "input": {},
                },
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": payload},
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use", "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    )


async def _sse(frames: tuple[tuple[str, dict[str, Any]], ...]):
    for event, data in frames:
        yield f"event: {event}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"


def _assert_catalog(catalog: set[str]) -> None:
    assert {READ_ALLOWED, WRITE_APPROVAL} <= catalog
    assert WRITE_DENIED not in catalog
    assert WRITE_UNMATCHED not in catalog


def _sdk_env(provider_url: str, config_dir: Path) -> dict[str, str]:
    return {
        "ANTHROPIC_BASE_URL": provider_url,
        "ANTHROPIC_API_KEY": "loopback-placeholder",
        "ANTHROPIC_AUTH_TOKEN": "",
        "CLAUDE_CODE_OAUTH_TOKEN": "",
        "CLAUDE_CONFIG_DIR": str(config_dir),
        "HTTP_PROXY": "",
        "HTTPS_PROXY": "",
        "ALL_PROXY": "",
        "NO_PROXY": "127.0.0.1,localhost",
    }


def _sdk_config(bundle: Path, session_id: str) -> RunnerConfig:
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(bundle),
            "CURIE_SESSION_ID": session_id,
            "CURIE_SANDBOX_ID": f"sandbox-{session_id}",
            "CURIE_BUDGET": '{"max_output_tokens_per_run": 64, "max_usd_per_day": 1.0}',
            "CURIE_MODEL": "sonnet",
        }
    )


def _observe_catalogs(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    catalogs: list[tuple[str, ...]] = []

    class CatalogObservingSession(boot.ClaudeAgentSession):
        def receive_turn(self):
            upstream = super().receive_turn()

            async def observe():
                async for message in upstream:
                    if isinstance(message, SystemMessage) and message.subtype == "init":
                        tools = message.data.get("tools")
                        if isinstance(tools, list):
                            catalogs.append(tuple(str(tool) for tool in tools))
                    yield message

            return observe()

    monkeypatch.setattr(boot, "ClaudeAgentSession", CatalogObservingSession)
    return catalogs


def _first_provider_catalog(capture: _ProviderCapture) -> set[str]:
    for tools in capture.tool_lists:
        if tools:
            return set(tools)
    return set()


async def _drive(runner: Any, text: str, ts: str) -> Final:
    await runner.start()
    final: Final | None = None
    try:
        with anyio.fail_after(45):
            async for line in runner.run_turn(
                Event(type="message", text=text, user="U0EXAMPLE", ts=ts)
            ):
                event = parse_ndjson_line(line)
                if isinstance(event, Final):
                    final = event
    finally:
        await runner.close()
    assert final is not None
    return final


@pytest.mark.skipif(
    not _HAS_CLAUDE_CLI,
    reason="claude CLI is required for real SDK catalog evidence",
)
def test_loopback_sdk_hosted_catalog_keeps_approval_tools_across_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    capture = _ProviderCapture()
    catalogs = _observe_catalogs(monkeypatch)
    with (
        _serve(_policy_catalog_app(calls)) as connector_url,
        _serve(capture.app()) as provider_url,
    ):
        bundle = _write_bundle(tmp_path, connector_url)
        store = _TranscriptStore()
        first = build_runner(
            _sdk_config(bundle, str(uuid4())),
            sdk_env=_sdk_env(provider_url, tmp_path / "claude-a"),
            history_store=store,
        )
        first_final = anyio.run(_drive, first, "Reply with only ok.", "1")
        assert catalogs, "first boot emitted no SDK init catalog"
        _assert_catalog(set(catalogs[0]))
        provider_catalog = _first_provider_catalog(capture)
        if provider_catalog:
            _assert_catalog(provider_catalog)

        replay, _summary = build_conversation_replay(store.records)
        resumed = build_runner(
            _sdk_config(bundle, str(uuid4())),
            sdk_env=_sdk_env(provider_url, tmp_path / "claude-b"),
            conversation_replay=replay,
        )
        resumed_final = anyio.run(_drive, resumed, "Reply with only ok.", "2")

    assert first_final.status in {SessionStatus.DONE, SessionStatus.IDLE_AWAITING_INPUT}
    assert resumed_final.status in {SessionStatus.DONE, SessionStatus.IDLE_AWAITING_INPUT}
    assert len(catalogs) >= 2
    _assert_catalog(set(catalogs[-1]))
    assert calls == []


@pytest.mark.skipif(
    not _HAS_CLAUDE_CLI,
    reason="claude CLI is required for real SDK approval evidence",
)
def test_loopback_sdk_hosted_approval_call_pauses_and_does_not_execute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    capture = _ProviderCapture(tool_use=WRITE_APPROVAL)
    catalogs = _observe_catalogs(monkeypatch)
    with (
        _serve(_policy_catalog_app(calls)) as connector_url,
        _serve(capture.app()) as provider_url,
    ):
        bundle = _write_bundle(tmp_path, connector_url)
        store = _TranscriptStore()
        runner = build_runner(
            _sdk_config(bundle, str(uuid4())),
            sdk_env=_sdk_env(provider_url, tmp_path / "claude-approval"),
            history_store=store,
        )
        final = anyio.run(
            _drive,
            runner,
            "Call write_approval exactly once with value `approved-once`.",
            "1",
        )
        assert catalogs
        _assert_catalog(set(catalogs[0]))
        assert final.status is SessionStatus.AWAITING_APPROVAL
        assert final.approval_summary is not None
        assert WRITE_APPROVAL in final.approval_summary
        assert calls == []

        replay, _summary = build_conversation_replay(store.records)
        resumed = build_runner(
            _sdk_config(bundle, str(uuid4())),
            sdk_env=_sdk_env(provider_url, tmp_path / "claude-rearm"),
            conversation_replay=replay,
        )
        rearm = anyio.run(
            _drive,
            resumed,
            "Call write_approval exactly once with value `duplicate`.",
            "2",
        )

    assert len(catalogs) >= 2
    _assert_catalog(set(catalogs[-1]))
    assert rearm.status is SessionStatus.AWAITING_APPROVAL
    assert rearm.approval_summary is not None
    assert WRITE_APPROVAL in rearm.approval_summary
    assert calls == []
