"""The runner half of ADR 0100 channel read (#2877).

A granted, real-model boot mounts the platform ``curie-slack`` server, holds
the per-turn capability the kernel sends on ``/v1/event`` and ``/v1/steer``,
and advertises enforcement on ``/status``. Everything here runs through the
real paths: the production ``build_runner`` builds the mcp_servers and the
policy projection, the real aiohttp ACI app takes the event, steer, interrupt,
timeout and reset requests, and the mounted tool handlers call a local aiohttp
server playing the platform's ``POST /channel-read`` route. Only the SDK
client itself is scripted, and it calls the tools through the very server
object the boot built, which is the call the SDK makes.
"""

from __future__ import annotations

import base64
import contextlib
import json
import logging
import os
import socket
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aci_protocol import CHANNEL_READ_STATUS_FIELD, Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from curie_runner import RunnerConfig, SideEffectClassifier, create_app
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.adapter import model_message_to_conversation
from curie_runner.approval import APPROVAL_SERVER_NAME, is_platform_owned_tool
from curie_runner.harness.claude.platform_slack import build_channel_read_server
from curie_runner.history import HarnessReplayState, TurnRecord
from curie_runner.mcp_tool_capability import McpToolCapabilityProbe
from curie_runner.platform_slack.capability import ChannelReadTurn
from curie_runner.tool_names import CHANNEL_READ_TOOL_NAMES, STATE_SERVER_NAME
from plugin_format import CHANNEL_READ_SERVER_NAME
from test_connectors import _boot_env, _published_live_tool_names
from test_tool_policy_enforcement import _interception_reason

# The wire names, spelled out: a pin derived from the constant it pins would
# agree with itself and still be wrong about what the model sees.
HISTORY_TOOL = "mcp__curie-slack__read_channel_history"
THREAD_TOOL = "mcp__curie-slack__read_thread_replies"
MESSAGE_TOOL = "mcp__curie-slack__read_channel_message"
LIVE_NAMES = frozenset({HISTORY_TOOL, THREAD_TOOL, MESSAGE_TOOL})

CAPABILITY_HEADER = "X-Curie-Channel-Read"
AGENT = "00000000-0000-4000-8000-00000000a001"
DEPLOYMENT = "00000000-0000-4000-8000-00000000d001"
DEFAULT_CHANNEL = {"kind": "slack", "address": "C0EXAMPLE1"}
# Every minted token ends in this, so a leak of the whole token or of its
# signature shows up in a plain substring search.
SENTINEL = "chr-token-sentinel"
MARKER = "channel-body-marker-7f3a2c"

HISTORY_ARGS = {"oldest": "2026-10-03T00:00:00Z", "latest": "2026-10-04T00:00:00Z"}
THREAD_ARGS = {
    "thread_id": "1759449600.000100",
    "oldest": "2026-10-03T00:00:00Z",
    "latest": "2026-10-04T00:00:00Z",
}
MESSAGE_ARGS = {"message_id": "1759449600.000100:1759449700.000200"}
TOOL_CASES = [
    ("read_channel_history", HISTORY_ARGS, "history"),
    ("read_thread_replies", THREAD_ARGS, "thread"),
    ("read_channel_message", MESSAGE_ARGS, "message"),
]

# The platform route's 200 shape (plan wire contract item 3).
PAGE = {
    "messages": [
        {
            "id": "1759449600.000100",
            "timestamp": "2026-10-03T00:00:00Z",
            "author": "U0EXAMPLE1",
            "text": f"yesterday's note {MARKER}",
            "truncated": False,
            "provenance": "https://example.slack.com/archives/C0EXAMPLE1/p1759449600000100",
            "reply_count": 2,
        }
    ],
    "has_more": True,
    "next_cursor": "chc.example-cursor.signature",
}

_TOKEN = "test-token-xyz"
_AUTH = {"Authorization": f"Bearer {_TOKEN}"}
_TURN_EPOCH_HEADER = "X-Curie-Turn-Epoch"
_EVENT_FRAME = {"kind": "event", "type": "message", "text": "hi", "user": "U0EXAMPLE1", "ts": "1"}
_STEER_FRAME = {**_EVENT_FRAME, "text": "and then", "ts": "2"}
_OMIT = object()


# --------------------------------------------------------------------------- #
# Capabilities, the platform route double, and the tool call the SDK makes
# --------------------------------------------------------------------------- #


def _token(
    *,
    gen: int = 1,
    turn: str = "evt-1",
    agent: str = AGENT,
    deployment: str = DEPLOYMENT,
    default: dict[str, str] | None = DEFAULT_CHANNEL,
) -> str:
    """A token shaped like the API's ``chr`` mint: prefix, claims, signature.

    The runner cannot verify the HMAC; it reads scope and generation from the
    middle segment, so the claims are real and the signature is a sentinel.
    """

    now = int(time.time())
    claims = {
        "aud": "channel.read",
        "agent": agent,
        "deployment": deployment,
        "grant": "a" * 64,
        "turn": turn,
        "gen": gen,
        "default": default,
        "iat": now,
        "exp": now + 900,
    }
    payload = (
        base64.urlsafe_b64encode(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"chr.{payload}.{SENTINEL}-g{gen}-{turn}"


def _event(capability: dict[str, str] | None) -> Event:
    frame: dict[str, Any] = dict(_EVENT_FRAME)
    if capability is not None:
        frame["channel_read"] = capability
    return Event.model_validate(frame)


class _ChannelApi:
    """The platform ``POST /channel-read`` route, recording every request."""

    def __init__(
        self,
        status: int = 200,
        payload: Any = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.payload = PAGE if payload is None else payload
        self.headers = headers or {}
        # When set, sent verbatim instead of serializing ``payload``.
        self.raw: bytes | None = None
        self.received: list[tuple[str, str | None, dict[str, Any]]] = []

    def refuse(self, status: int, code: str, **extra: Any) -> None:
        self.status = status
        self.payload = {"detail": {"code": code, "message": "refused by the platform", **extra}}

    def app(self) -> web.Application:
        app = web.Application()

        async def read(request: web.Request) -> web.Response:
            self.received.append(
                (request.path, request.headers.get(CAPABILITY_HEADER), await request.json())
            )
            if self.raw is not None:
                return web.Response(
                    body=self.raw,
                    status=self.status,
                    headers={**self.headers, "Content-Type": "application/json"},
                )
            return web.json_response(self.payload, status=self.status, headers=self.headers)

        # Every path records, so a request sent to the wrong path still counts.
        app.router.add_post("/{tail:.*}", read)
        return app


def _origin(server: TestServer) -> tuple[str, str, int]:
    assert server.port is not None
    return ("http", server.host, server.port)


def _url(server: TestServer, path: str = "/channel-read") -> str:
    return str(server.make_url(path))


async def _call_server(server: Any, short_name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Execute one tool through the MCP server object, as the SDK does."""

    entry = server.get_request_handler("tools/call")
    assert entry is not None
    result = await entry.handler(
        None, mcp_types.CallToolRequestParams(name=short_name, arguments=dict(args))
    )
    payload = result.model_dump()
    payload = payload.get("root", payload)
    return {
        "is_error": bool(payload.get("is_error")),
        "text": " ".join(str(block.get("text") or "") for block in payload.get("content") or []),
    }


async def _listed_tools(server: Any) -> list[dict[str, Any]]:
    entry = server.get_request_handler("tools/list")
    assert entry is not None
    result = await entry.handler(None, mcp_types.PaginatedRequestParams())
    return [tool.model_dump() for tool in result.tools]


def _assert_refused(result: dict[str, Any], code: str) -> None:
    assert result["is_error"] is True, result
    assert code in result["text"], result


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# --------------------------------------------------------------------------- #
# The production boot, with the SDK client scripted
# --------------------------------------------------------------------------- #


def _result(text: str, *, is_error: bool = False, subtype: str = "success") -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="sdk-session",
        result=text,
    )


class _Sdk:
    """The SDK client ``build_runner`` constructs, scripted by the test.

    One message queue across turns, like the real streaming client (#3425).
    ``native`` is what the SDK's own session file holds: every message the CLI
    produced, whether or not the runner consumed it, which is what
    ``export_replay_state`` hands back as the native checkpoint.
    """

    def __init__(self, options: Any) -> None:
        self.options = options
        self.queue: list[Any] = []
        self.arrived = anyio.Event()
        self.queries: list[str] = []
        self.interrupts = 0
        self.interrupt_entered = anyio.Event()
        self.interrupt_release: anyio.Event | None = None
        self.native: list[dict[str, Any]] = []
        self.exports = 0

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        if self.interrupt_release is not None:
            self.interrupt_release.set()

    async def query(self, text: str) -> None:
        self.queries.append(text)

    def put(self, message: Any) -> None:
        projected = model_message_to_conversation(message)
        if projected is not None:
            self.native.append(projected.to_dict())
        self.queue.append(message)
        self.arrived.set()

    def receive_turn(self) -> AsyncIterator[Any]:
        async def _gen() -> AsyncIterator[Any]:
            while True:
                while not self.queue:
                    self.arrived = anyio.Event()
                    await self.arrived.wait()
                message = self.queue.pop(0)
                yield message
                if isinstance(message, ResultMessage):
                    return

        return _gen()

    async def interrupt(self) -> None:
        self.interrupts += 1
        self.interrupt_entered.set()
        if self.interrupt_release is not None:
            await self.interrupt_release.wait()
        self.put(_result("", is_error=True, subtype="error_during_execution"))

    async def export_replay_state(self) -> HarnessReplayState:
        entries = tuple(self.native)
        self.native = []
        self.exports += 1
        return HarnessReplayState(harness="claude", kind="checkpoint", entries=entries)

    def request_full_checkpoint(self) -> None:
        return None

    async def call(self, short_name: str, args: dict[str, Any]) -> dict[str, Any]:
        server = self.options.mcp_servers[CHANNEL_READ_SERVER_NAME]["instance"]
        return await _call_server(server, short_name, args)

    async def tool_round(
        self,
        call_id: str,
        short_name: str,
        args: dict[str, Any],
        *,
        stream_result: bool = True,
    ) -> dict[str, Any]:
        """The model calls a read tool: tool_use, execution, tool_result."""

        self.put(
            AssistantMessage(
                content=[
                    ToolUseBlock(
                        id=call_id,
                        name=f"mcp__{CHANNEL_READ_SERVER_NAME}__{short_name}",
                        input=dict(args),
                    )
                ],
                model="stub-model",
            )
        )
        result = await self.call(short_name, args)
        message = UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id=call_id,
                    content=[{"type": "text", "text": result["text"]}],
                    is_error=True if result["is_error"] else None,
                )
            ]
        )
        if stream_result:
            self.put(message)
        else:
            # The CLI has the result in its session file; the runner never saw it.
            projected = model_message_to_conversation(message)
            assert projected is not None
            self.native.append(projected.to_dict())
        return result

    def finish(self, text: str = "done") -> None:
        self.put(AssistantMessage(content=[TextBlock(text=text)], model="stub-model"))
        self.put(_result(text))


class _Store:
    def __init__(self) -> None:
        self.records: list[TurnRecord] = []

    async def load(self) -> list[TurnRecord]:
        return list(self.records)

    async def append(self, record: TurnRecord) -> bool:
        self.records.append(record)
        return record.harness_replay is not None


def _patch_sdk(monkeypatch: pytest.MonkeyPatch) -> list[_Sdk]:
    sessions: list[_Sdk] = []

    async def probe(*_args: Any, **_kwargs: Any) -> McpToolCapabilityProbe:
        return McpToolCapabilityProbe(
            complete=True,
            has_potential_write_tool=True,
            tool_count=1,
            observed_tools=frozenset(),
            readonly_tools=frozenset(),
        )

    def make(options: Any) -> _Sdk:
        session = _Sdk(options)
        sessions.append(session)
        return session

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", make)
    return sessions


def _config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api_base: str,
    *,
    grant: bool | None = True,
    policy: dict[str, list[str]] | None = None,
) -> RunnerConfig:
    """The eligible boot env, with the platform API at ``api_base``.

    ``CURIE_STATE_URL`` is the boot's platform origin; the worker mints the
    capability URL from the same base, so the read route lives on it.
    """

    env = _boot_env(monkeypatch, tmp_path, "channel-read")
    monkeypatch.setenv("CURIE_STATE_URL", f"{api_base}/agents/a/state")
    manifest_path = Path(env["CURIE_PLUGIN_DIR"]) / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if grant is not None:
        manifest["channelRead"] = grant
    if policy is not None:
        manifest["toolPolicy"] = {"enforcement": "curie/mcp-tool-policy@1", **policy}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return RunnerConfig.from_env(env)


def _boot_options(monkeypatch: pytest.MonkeyPatch, config: RunnerConfig) -> tuple[Any, Any]:
    sessions = _patch_sdk(monkeypatch)
    runner = build_runner(config, fake_model=False)
    session = runner._factory()  # noqa: SLF001 - the options the SDK receives
    assert isinstance(session, _Sdk)
    assert sessions[-1] is session
    return runner, session.options


async def _until(predicate: Callable[[], bool], timeout: float = 5) -> None:
    with anyio.fail_after(timeout):
        while not predicate():
            await anyio.sleep(0.005)


@dataclass
class _Rig:
    api: _ChannelApi
    base: str
    client: TestClient
    runner: Any
    sessions: list[_Sdk]
    store: _Store

    @property
    def sdk(self) -> _Sdk:
        return self.sessions[-1]

    def capability(self, **claims: Any) -> dict[str, str]:
        return {"url": f"{self.base}/channel-read", "token": _token(**claims)}

    async def open(self, capability: dict[str, str] | None) -> Any:
        before = len(self.sdk.queries)
        frame: dict[str, Any] = dict(_EVENT_FRAME)
        if capability is not None:
            frame["channel_read"] = capability
        response = await self.client.post("/v1/event", json=frame, headers=_AUTH)
        assert response.status == 200
        await _until(lambda: len(self.sdk.queries) > before)
        return response

    async def steer(self, capability: Any = _OMIT) -> int:
        frame: dict[str, Any] = dict(_STEER_FRAME)
        if capability is not _OMIT:
            frame["channel_read"] = capability
        response = await self.client.post("/v1/steer", json=frame, headers=_AUTH)
        return response.status

    async def read(self) -> dict[str, Any]:
        return await self.sdk.call("read_channel_history", HISTORY_ARGS)

    async def close_turn(self, response: Any, text: str = "done") -> Final:
        self.sdk.finish(text)
        events = parse_ndjson(await response.text())
        final = events[-1]
        assert isinstance(final, Final)
        return final


@contextlib.asynccontextmanager
async def _rig(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api: _ChannelApi | None = None,
) -> AsyncIterator[_Rig]:
    api = api or _ChannelApi()
    sessions = _patch_sdk(monkeypatch)
    store = _Store()
    async with TestServer(api.app()) as api_server:
        base = str(api_server.make_url("")).rstrip("/")
        config = _config(monkeypatch, tmp_path, base)
        # The boot runs its own event loop (the connector probe), so it is
        # built off this one, the way the process entrypoint builds it.
        runner = await anyio.to_thread.run_sync(
            lambda: build_runner(config, fake_model=False, history_store=store)
        )
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            yield _Rig(api, base, client, runner, sessions, store)


# --------------------------------------------------------------------------- #
# Mount, catalogue and advertisement
# --------------------------------------------------------------------------- #


async def _status_fields(runner: Any) -> list[Any]:
    async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
        values = []
        for path, headers in (("/status", {}), ("/v1/status", _AUTH)):
            response = await client.get(path, headers=headers)
            assert response.status == 200
            values.append((await response.json()).get(CHANNEL_READ_STATUS_FIELD))
        return values


@pytest.mark.parametrize("grant", [None, False], ids=["absent", "false"])
def test_an_ungranted_boot_mounts_no_curie_slack_and_advertises_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: bool | None
) -> None:
    runner, options = _boot_options(
        monkeypatch, _config(monkeypatch, tmp_path, "http://state.invalid", grant=grant)
    )

    assert CHANNEL_READ_SERVER_NAME not in options.mcp_servers
    assert set(options.mcp_servers) == {APPROVAL_SERVER_NAME, STATE_SERVER_NAME}
    published = _published_live_tool_names(options.mcp_servers)
    assert not {name for name in published if name.startswith("mcp__curie-slack__")}

    async def go() -> list[Any]:
        await runner.start()
        return await _status_fields(runner)

    # The contract: anything but literal true means unsupported.
    assert all(value is not True for value in anyio.run(go))


def test_a_granted_boot_mounts_three_read_tools_and_advertises_true(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner, options = _boot_options(
        monkeypatch, _config(monkeypatch, tmp_path, "http://state.invalid")
    )

    assert set(options.mcp_servers) == {
        APPROVAL_SERVER_NAME,
        STATE_SERVER_NAME,
        CHANNEL_READ_SERVER_NAME,
    }
    published = _published_live_tool_names(
        {CHANNEL_READ_SERVER_NAME: options.mcp_servers[CHANNEL_READ_SERVER_NAME]}
    )
    assert published == LIVE_NAMES
    assert frozenset(CHANNEL_READ_TOOL_NAMES) == LIVE_NAMES

    async def go() -> list[Any]:
        await runner.start()
        return await _status_fields(runner)

    assert anyio.run(go) == [True, True]


def test_the_fake_model_never_mounts_or_advertises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = build_runner(_config(monkeypatch, tmp_path, "http://state.invalid"), fake_model=True)

    async def go() -> list[Any]:
        await runner.start()
        return await _status_fields(runner)

    assert all(value is not True for value in anyio.run(go))


def test_policy_projection_includes_mounted_read_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A policy deny hides the mounted tools from the model's catalogue; the
    # projection reads the boot's own mounted names, since the probe never
    # sees an in-process server.
    _, denied = _boot_options(
        monkeypatch,
        _config(
            monkeypatch,
            tmp_path / "denied",
            "http://state.invalid",
            policy={"deny": ["curie-slack/*"]},
        ),
    )
    assert LIVE_NAMES <= set(denied.disallowed_tools)

    _, allowed = _boot_options(
        monkeypatch,
        _config(
            monkeypatch,
            tmp_path / "allowed",
            "http://state.invalid",
            policy={"allow": ["curie-slack/*"]},
        ),
    )
    assert not LIVE_NAMES & set(allowed.disallowed_tools)

    # Ungranted: a literal curie-slack pattern is invalid without the grant,
    # so the control is a deny-everything wildcard. It still names none of
    # them, because nothing mounted them.
    _, ungranted = _boot_options(
        monkeypatch,
        _config(
            monkeypatch,
            tmp_path / "ungranted",
            "http://state.invalid",
            grant=None,
            policy={"deny": ["*/*"]},
        ),
    )
    assert not LIVE_NAMES & set(ungranted.disallowed_tools)
    assert CHANNEL_READ_SERVER_NAME not in ungranted.mcp_servers


@pytest.mark.parametrize(
    ("policy", "refused"),
    [
        ({"allow": ["curie-slack/*"]}, False),
        ({"deny": ["curie-slack/read_channel_history"]}, True),
        ({"allow": ["curie-slack/read_thread_replies"]}, True),
    ],
    ids=["allow", "deny", "unmatched"],
)
def test_policy_governs_the_granted_tools(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    policy: dict[str, list[str]],
    refused: bool,
) -> None:
    runner, _ = _boot_options(
        monkeypatch, _config(monkeypatch, tmp_path, "http://state.invalid", policy=policy)
    )
    gate = runner._approval_gate  # noqa: SLF001 - the gate the boot built
    assert gate is not None
    for interceptor in ("hook", "callback"):
        reason = _interception_reason(gate, HISTORY_TOOL, interceptor)
        if refused:
            assert "not permitted for this agent" in reason, (interceptor, reason)
        else:
            assert reason == "", (interceptor, reason)
    for name in LIVE_NAMES:
        assert not is_platform_owned_tool(name, state_server_mounted=True)
        assert not is_platform_owned_tool(
            name, state_server_mounted=True, memory_tools_mounted=True
        )


def test_reads_are_not_side_effects() -> None:
    classifier = SideEffectClassifier()
    for name in LIVE_NAMES:
        assert classifier.is_side_effecting(name) is False, name


def test_no_tool_takes_a_credential_argument() -> None:
    turn = ChannelReadTurn(trusted_origin=("http", "127.0.0.1", 8080))
    tools = anyio.run(_listed_tools, build_channel_read_server(turn)["instance"])
    assert {tool["name"] for tool in tools} == {
        "read_channel_history",
        "read_thread_replies",
        "read_channel_message",
    }
    for tool in tools:
        schema = tool.get("inputSchema") or tool.get("input_schema") or {}
        properties = {name.lower() for name in (schema.get("properties") or {})}
        assert not properties & {"token", "url", "capability", "credential"}, tool["name"]


# --------------------------------------------------------------------------- #
# The tool handlers against the platform route double
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("state", ["never-begun", "begun-without", "ended"])
def test_a_tool_without_capability_refuses_without_http(state: str) -> None:
    api = _ChannelApi()

    async def go() -> list[dict[str, Any]]:
        async with TestServer(api.app()) as server:
            turn = ChannelReadTurn(trusted_origin=_origin(server))
            instance = build_channel_read_server(turn)["instance"]
            if state == "begun-without":
                turn.begin(_event(None))
            elif state == "ended":
                turn.begin(_event({"url": _url(server), "token": _token()}))
                turn.end()
            return [await _call_server(instance, name, args) for name, args, _ in TOOL_CASES]

    for result in anyio.run(go):
        _assert_refused(result, "channel_read.no_capability")
    assert api.received == []


@pytest.mark.parametrize(("short_name", "args", "operation"), TOOL_CASES)
def test_a_tool_with_capability_presents_the_header_and_labels_content_untrusted(
    short_name: str, args: dict[str, Any], operation: str
) -> None:
    api = _ChannelApi()
    token = _token()

    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            turn = ChannelReadTurn(trusted_origin=_origin(server))
            turn.begin(_event({"url": _url(server), "token": token}))
            return await _call_server(build_channel_read_server(turn)["instance"], short_name, args)

    result = anyio.run(go)

    assert result["is_error"] is False, result
    assert len(api.received) == 1
    path, header, body = api.received[0]
    assert path == "/channel-read"
    assert header == token
    assert body["operation"] == operation
    for key, value in args.items():
        assert body[key] == value, key
    assert SENTINEL not in json.dumps(body)
    payload = json.loads(result["text"])
    assert payload["content_trust"] == "untrusted"
    assert payload["source"] == "channel"
    assert payload["messages"] == PAGE["messages"]
    assert payload["has_more"] is True
    assert payload["next_cursor"] == PAGE["next_cursor"]
    assert SENTINEL not in result["text"]


# The API's own limits (apps/api channel_read/slack_reads.py and window.py):
# 100 records a page, text cut at 4000 characters plus the marker.
_MAX_LIMIT = 100
_TEXT_LIMIT = 4000
_TRUNCATION_MARKER = " [truncated]"


def _api_body(payload: Any) -> bytes:
    """Serialize as FastAPI's JSONResponse does (Starlette ``render``).

    ``ensure_ascii=False`` and compact separators: non-ASCII text goes out as
    raw UTF-8, so a 4-byte character costs 4 bytes, not a 12-byte surrogate
    pair escape.
    """

    return json.dumps(
        payload, ensure_ascii=False, allow_nan=False, indent=None, separators=(",", ":")
    ).encode("utf-8")


def _maximum_unicode_page() -> dict[str, Any]:
    messages = []
    for index in range(_MAX_LIMIT):
        ts = f"1759449{index:03d}.000100"
        # U+1F600 is 4 UTF-8 bytes; CJK (3 bytes) is strictly smaller.
        text = ("\U0001f600" * _TEXT_LIMIT) + _TRUNCATION_MARKER
        messages.append(
            {
                "id": f"1759449600.000100:{ts}",
                "thread_id": "1759449600.000100",
                "timestamp": "2026-10-03T00:00:00Z",
                "author": "U0EXAMPLE1",
                "text": text,
                "truncated": True,
                "provenance": (
                    f"https://example.slack.com/archives/C0EXAMPLE1/p{ts.replace('.', '')}"
                    "?thread_ts=1759449600.000100&cid=C0EXAMPLE1"
                ),
                "reply_count": 0,
            }
        )
    return {"messages": messages, "has_more": True, "next_cursor": "c" * 512}


def test_a_maximum_valid_unicode_page_is_read_whole() -> None:
    page = _maximum_unicode_page()
    api = _ChannelApi()
    api.raw = _api_body(page)
    # The case that matters: a legal page larger than 1 MiB on the wire.
    assert len(api.raw) > 1_048_576

    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            turn = ChannelReadTurn(trusted_origin=_origin(server))
            turn.begin(_event({"url": _url(server), "token": _token()}))
            return await _call_server(
                build_channel_read_server(turn)["instance"], "read_thread_replies", THREAD_ARGS
            )

    result = anyio.run(go)
    assert result["is_error"] is False, result["text"][:300]
    payload = json.loads(result["text"])
    assert len(payload["messages"]) == _MAX_LIMIT
    assert [item["id"] for item in payload["messages"]] == [item["id"] for item in page["messages"]]
    assert all(item["text"] == page["messages"][0]["text"] for item in payload["messages"])
    assert payload["content_trust"] == "untrusted"


def test_a_response_over_the_ceiling_is_unavailable_and_the_next_read_succeeds() -> None:
    api = _ChannelApi()
    # Far past any legal page: the largest one is under 2 MiB even in 4-byte
    # characters, and under 3 MiB if every character were a 6-byte \uXXXX
    # control escape. 16 MiB of otherwise valid JSON.
    oversized = {
        "messages": [
            {**PAGE["messages"][0], "id": f"1759449600.{index:06d}", "text": "x" * 170_000}
            for index in range(_MAX_LIMIT)
        ],
        "has_more": False,
    }
    api.raw = _api_body(oversized)
    assert len(api.raw) > 16 * 1_048_576

    async def go() -> tuple[dict[str, Any], dict[str, Any]]:
        async with TestServer(api.app()) as server:
            turn = ChannelReadTurn(trusted_origin=_origin(server))
            turn.begin(_event({"url": _url(server), "token": _token()}))
            instance = build_channel_read_server(turn)["instance"]
            refused = await _call_server(instance, "read_channel_history", HISTORY_ARGS)
            api.raw = None
            healthy = await _call_server(instance, "read_channel_history", HISTORY_ARGS)
            return refused, healthy

    refused, healthy = anyio.run(go)
    _assert_refused(refused, "channel_read.unavailable")
    assert "x" * 100 not in refused["text"]
    assert healthy["is_error"] is False, healthy
    assert json.loads(healthy["text"])["messages"] == PAGE["messages"]
    assert len(api.received) == 2


@pytest.mark.parametrize(
    ("status", "code", "extra", "expect"),
    [
        (403, "channel_read.not_member", {}, None),
        (429, "channel_read.provider_rate_limited", {"retry_after": 30}, "30"),
        (409, "channel_read.turn_inactive", {}, None),
    ],
    ids=["not-member", "rate-limited", "turn-inactive"],
)
def test_api_refusals_surface_their_code(
    status: int, code: str, extra: dict[str, Any], expect: str | None
) -> None:
    api = _ChannelApi()
    api.refuse(status, code, **extra)

    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            turn = ChannelReadTurn(trusted_origin=_origin(server))
            turn.begin(_event({"url": _url(server), "token": _token()}))
            return await _call_server(
                build_channel_read_server(turn)["instance"], "read_channel_history", HISTORY_ARGS
            )

    result = anyio.run(go)
    _assert_refused(result, code)
    if expect is not None:
        assert expect in result["text"]
    assert MARKER not in result["text"]
    assert len(api.received) == 1


@pytest.mark.parametrize("mismatch", ["other-origin", "other-path"])
def test_an_untrusted_origin_capability_is_refused(mismatch: str) -> None:
    trusted = _ChannelApi()
    foreign = _ChannelApi()

    async def go() -> dict[str, Any]:
        async with (
            TestServer(trusted.app()) as trusted_server,
            TestServer(foreign.app()) as foreign_server,
        ):
            turn = ChannelReadTurn(trusted_origin=_origin(trusted_server))
            url = (
                _url(foreign_server)
                if mismatch == "other-origin"
                else _url(trusted_server, "/agents/a/state")
            )
            turn.begin(_event({"url": url, "token": _token()}))
            return await _call_server(
                build_channel_read_server(turn)["instance"], "read_channel_history", HISTORY_ARGS
            )

    _assert_refused(anyio.run(go), "channel_read.no_capability")
    assert trusted.received == []
    assert foreign.received == []


def test_the_token_stays_out_of_repr_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    api = _ChannelApi()
    api.refuse(403, "channel_read.not_member")
    seen: list[str] = []
    reprs: list[str] = []

    async def go() -> None:
        async with TestServer(api.app()) as server:
            turn = ChannelReadTurn(trusted_origin=_origin(server))
            instance = build_channel_read_server(turn)["instance"]
            turn.begin(_event({"url": _url(server), "token": _token()}))
            reprs.extend((repr(turn), str(turn)))
            # A refused read, then rejected steers (mismatched scope, stale).
            refused = await _call_server(instance, "read_channel_history", HISTORY_ARGS)
            seen.append(json.dumps(refused))
            turn.steer(_event({"url": _url(server), "token": _token(gen=2, turn="evt-other")}))
            turn.steer(_event({"url": _url(server), "token": "chr.not-base64!.x" + SENTINEL}))
            turn.begin(_event({"url": _url(server), "token": _token(gen=5)}))
            turn.steer(_event({"url": _url(server), "token": _token(gen=4)}))
            reprs.extend((repr(turn), str(turn)))

        # Transport failure: the trusted origin has nothing listening.
        port = _closed_port()
        turn = ChannelReadTurn(trusted_origin=("http", "127.0.0.1", port))
        turn.begin(_event({"url": f"http://127.0.0.1:{port}/channel-read", "token": _token()}))
        failed = await _call_server(
            build_channel_read_server(turn)["instance"], "read_channel_history", HISTORY_ARGS
        )
        _assert_refused(failed, "channel_read.unavailable")
        # The endpoint is not disclosed either.
        assert str(port) not in failed["text"]
        seen.append(json.dumps(failed))

    with caplog.at_level(logging.DEBUG):
        anyio.run(go)

    for text in seen + reprs:
        assert SENTINEL not in text, text
    for text in reprs:
        # Neither the token nor where it is presented.
        assert "chr." not in text, text
        assert "/channel-read" not in text, text
    assert SENTINEL not in caplog.text
    assert all(SENTINEL not in repr(record.args) for record in caplog.records)


# --------------------------------------------------------------------------- #
# The holder lifecycle through the real ACI app
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("cleared_by", ["null", "omitted"])
def test_a_steer_with_null_clears_the_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleared_by: str
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability())
            assert (await rig.read())["is_error"] is False
            assert len(rig.api.received) == 1
            assert await rig.steer(None if cleared_by == "null" else _OMIT) == 200
            _assert_refused(await rig.read(), "channel_read.no_capability")
            assert len(rig.api.received) == 1
            assert (await rig.close_turn(response)).status is SessionStatus.DONE

    anyio.run(go)


def test_a_same_scope_newer_steer_replaces_the_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability(gen=1))
            assert await rig.steer(rig.capability(gen=2)) == 200
            assert (await rig.read())["is_error"] is False
            assert [header for _, header, _ in rig.api.received] == [_token(gen=2)]
            await rig.close_turn(response)

    anyio.run(go)


@pytest.mark.parametrize(
    "claims",
    [
        {"turn": "evt-other"},
        {"agent": "00000000-0000-4000-8000-00000000a002"},
        {"deployment": "00000000-0000-4000-8000-00000000d002"},
        {"default": {"kind": "slack", "address": "C0OTHER"}},
        {"default": None},
    ],
    ids=["turn", "agent", "deployment", "default-channel", "default-dropped"],
)
def test_a_mismatched_scope_steer_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, claims: dict[str, Any]
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability(gen=1))
            assert await rig.steer(rig.capability(gen=2, **claims)) == 200
            _assert_refused(await rig.read(), "channel_read.no_capability")
            assert rig.api.received == []
            await rig.close_turn(response)

    anyio.run(go)


def test_a_capability_on_a_steer_without_an_admitted_scope_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(None)
            assert await rig.steer(rig.capability(gen=2)) == 200
            _assert_refused(await rig.read(), "channel_read.no_capability")
            assert rig.api.received == []
            await rig.close_turn(response)

    anyio.run(go)


def test_null_then_mismatched_steer_stays_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The admitted scope outlives the null: a null steer clears only the
    # credential, so another turn's capability cannot slip in after it.
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability(gen=1, turn="evt-s"))
            assert await rig.steer(None) == 200
            assert await rig.steer(rig.capability(gen=2, turn="evt-t")) == 200
            _assert_refused(await rig.read(), "channel_read.no_capability")
            assert rig.api.received == []
            # Liveness: the admitted scope at a newer generation is accepted.
            assert await rig.steer(rig.capability(gen=3, turn="evt-s")) == 200
            assert (await rig.read())["is_error"] is False
            assert [header for _, header, _ in rig.api.received] == [_token(gen=3, turn="evt-s")]
            await rig.close_turn(response)

    anyio.run(go)


def test_reordered_generations_keep_the_newest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability(gen=1))
            assert await rig.steer(rig.capability(gen=3)) == 200
            # A delayed older steer does not roll the credential back.
            assert await rig.steer(rig.capability(gen=2)) == 200
            assert (await rig.read())["is_error"] is False
            assert [header for _, header, _ in rig.api.received] == [_token(gen=3)]
            # Nor does it come back after a null.
            assert await rig.steer(None) == 200
            assert await rig.steer(rig.capability(gen=2)) == 200
            _assert_refused(await rig.read(), "channel_read.no_capability")
            assert await rig.steer(rig.capability(gen=3)) == 200
            _assert_refused(await rig.read(), "channel_read.no_capability")
            assert len(rig.api.received) == 1
            # Liveness: a strictly newer generation is accepted.
            assert await rig.steer(rig.capability(gen=4)) == 200
            assert (await rig.read())["is_error"] is False
            assert rig.api.received[-1][1] == _token(gen=4)
            await rig.close_turn(response)

    anyio.run(go)


@pytest.mark.parametrize("path", ["completion", "interrupt", "timeout"])
def test_every_terminal_path_clears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability())
            assert (await rig.read())["is_error"] is False
            assert len(rig.api.received) == 1

            if path == "completion":
                await rig.close_turn(response)
                _assert_refused(await rig.read(), "channel_read.no_capability")
                assert len(rig.api.received) == 1
                return

            # The SDK's stop does not return until released, so only the
            # control route itself can have cleared the credential: the turn
            # is still open and its outer finally has not run.
            release = anyio.Event()
            rig.sdk.interrupt_release = release
            epoch = response.headers[_TURN_EPOCH_HEADER]
            statuses: list[int] = []
            outcomes: list[dict[str, Any]] = []

            async def control() -> None:
                if path == "interrupt":
                    reply = await rig.client.post(
                        "/v1/interrupt", json={"kind": "interrupt", "reason": "stop"}, headers=_AUTH
                    )
                else:
                    reply = await rig.client.post(
                        "/v1/timeout", headers={**_AUTH, _TURN_EPOCH_HEADER: epoch}
                    )
                statuses.append(reply.status)

            async def probe() -> None:
                await rig.sdk.interrupt_entered.wait()
                assert rig.runner.turn_active
                outcomes.append(await rig.read())
                release.set()

            async with anyio.create_task_group() as tg:
                tg.start_soon(control)
                tg.start_soon(probe)
            await response.text()

            assert statuses == [200]
            _assert_refused(outcomes[0], "channel_read.no_capability")
            assert len(rig.api.received) == 1

    anyio.run(go)


def test_a_stale_timeout_leaves_the_current_turn_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            first = await rig.open(rig.capability(turn="evt-1"))
            first_epoch = first.headers[_TURN_EPOCH_HEADER]
            await rig.close_turn(first)

            second = await rig.open(rig.capability(turn="evt-2"))
            stale = await rig.client.post(
                "/v1/timeout", headers={**_AUTH, _TURN_EPOCH_HEADER: first_epoch}
            )
            assert stale.status == 409
            assert (await rig.read())["is_error"] is False
            assert [header for _, header, _ in rig.api.received] == [_token(turn="evt-2")]
            assert (await rig.close_turn(second)).status is SessionStatus.DONE

    anyio.run(go)


def test_a_blocked_interrupt_after_disconnect_refuses_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A disconnect closes the turn's stream, exactly as server.py's aclosing
    # does when the worker holding the NDJSON response goes away. The SDK
    # stop then blocks; the credential must already be gone.
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            release = anyio.Event()
            rig.sdk.interrupt_release = release
            rig.sdk.put(
                AssistantMessage(
                    content=[ToolUseBlock(id="call-0", name="Bash", input={"command": "true"})],
                    model="stub-model",
                )
            )
            stream = rig.runner.run_turn(_event(rig.capability()))
            try:
                with anyio.fail_after(5):
                    await stream.__anext__()
                assert (await rig.read())["is_error"] is False
            except BaseException:
                release.set()
                await stream.aclose()
                raise
            outcomes: list[dict[str, Any]] = []

            async def probe() -> None:
                await rig.sdk.interrupt_entered.wait()
                outcomes.append(await rig.read())
                release.set()

            async with anyio.create_task_group() as tg:
                tg.start_soon(probe)
                with anyio.fail_after(5):
                    await stream.aclose()

            _assert_refused(outcomes[0], "channel_read.no_capability")
            assert len(rig.api.received) == 1

    anyio.run(go)


# --------------------------------------------------------------------------- #
# Retrieved bodies stay out of everything persisted
# --------------------------------------------------------------------------- #


def _tool_results(record: TurnRecord) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for message in record.messages:
        if isinstance(message.content, list):
            for block in message.content:
                if block.get("type") == "tool_result":
                    results[str(block["tool_use_id"])] = dict(block)
    return results


def _result_text(block: dict[str, Any]) -> str:
    content = block.get("content")
    if isinstance(content, str):
        return content
    return "".join(str(item.get("text") or "") for item in content or [])


def test_persisted_record_keeps_provenance_not_bodies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            # Liveness: a turn with no channel read exports native replay.
            plain = await rig.open(rig.capability(turn="evt-0"))
            await rig.close_turn(plain)

            reading = await rig.open(rig.capability(turn="evt-1"))
            read = await rig.sdk.tool_round("call-read", "read_channel_history", HISTORY_ARGS)
            assert read["is_error"] is False
            assert MARKER in read["text"]
            rig.api.refuse(403, "channel_read.not_member")
            refused = await rig.sdk.tool_round("call-refused", "read_channel_history", HISTORY_ARGS)
            assert refused["is_error"] is True
            await rig.close_turn(reading)

            later = await rig.open(None)
            await rig.close_turn(later)

            # A new SDK session carries no channel body, so native replay resumes.
            reset = await rig.client.post("/v1/reset", headers=_AUTH)
            assert reset.status == 200
            fresh = await rig.open(None)
            await rig.close_turn(fresh)

            records = rig.store.records
            assert len(records) == 4
            assert records[0].harness_replay is not None
            assert records[1].harness_replay is None
            assert records[2].harness_replay is None
            assert records[3].harness_replay is not None

            results = _tool_results(records[1])
            stub = json.loads(_result_text(results["call-read"]))
            assert stub["bodies_retained"] is False
            assert [
                {"id": item["id"], "provenance": item["provenance"]} for item in stub["messages"]
            ] == [{"id": item["id"], "provenance": item["provenance"]} for item in PAGE["messages"]]
            assert all("text" not in item for item in stub["messages"])
            assert stub["has_more"] is True
            # An error carries no body and is kept verbatim.
            assert _result_text(results["call-refused"]) == refused["text"]
            assert results["call-refused"].get("is_error") is True

            for record in records:
                assert MARKER not in json.dumps(record.to_dict())
                assert SENTINEL not in json.dumps(record.to_dict())

    anyio.run(go)


def test_disconnect_before_result_never_reaches_native_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            rig.sdk.put(
                AssistantMessage(
                    content=[ToolUseBlock(id="call-0", name="Bash", input={"command": "true"})],
                    model="stub-model",
                )
            )
            stream = rig.runner.run_turn(_event(rig.capability()))
            try:
                # Open the turn so the capability is held before the read tool runs.
                with anyio.fail_after(5):
                    await stream.__anext__()
                # The tool ran and returned a body; the runner saw the tool_use
                # but its consumer went away before the result was streamed.
                read = await rig.sdk.tool_round(
                    "call-read", "read_channel_history", HISTORY_ARGS, stream_result=False
                )
                assert read["is_error"] is False
                with anyio.fail_after(5):
                    await stream.__anext__()
            except BaseException:
                await stream.aclose()
                raise
            with anyio.fail_after(5):
                await stream.aclose()
            # The SDK's own session state does hold the body.
            assert any(MARKER in json.dumps(entry) for entry in rig.sdk.native)

            following = await rig.open(None)
            assert (await rig.close_turn(following)).status is SessionStatus.DONE

            assert rig.store.records, "the following turn was not recorded"
            last = rig.store.records[-1]
            assert last.harness_replay is None
            for record in rig.store.records:
                assert MARKER not in json.dumps(record.to_dict())

    anyio.run(go)


def test_the_token_never_reaches_env_options_records_or_logs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def go() -> None:
        async with _rig(monkeypatch, tmp_path) as rig:
            response = await rig.open(rig.capability(gen=1))
            assert await rig.steer(rig.capability(gen=2)) == 200
            read = await rig.sdk.tool_round("call-read", "read_channel_history", HISTORY_ARGS)
            assert read["is_error"] is False
            # A rejected steer logs a warning; the warning carries no token.
            assert await rig.steer(rig.capability(gen=3, turn="evt-other")) == 200
            await rig.close_turn(response)
            assert len(rig.api.received) == 1
            for value in os.environ.values():
                assert SENTINEL not in value
            assert SENTINEL not in json.dumps(dict(rig.sdk.options.env or {}))
            for record in rig.store.records:
                assert SENTINEL not in json.dumps(record.to_dict())
            for _, _, body in rig.api.received:
                assert SENTINEL not in json.dumps(body)

    with caplog.at_level(logging.DEBUG):
        anyio.run(go)
    assert SENTINEL not in caplog.text
    assert all(SENTINEL not in repr(record.args) for record in caplog.records)
