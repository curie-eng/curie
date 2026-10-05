"""The runner half of ADR 0100 channel read (#2877).

Real paths throughout: the production ``build_runner``, the real aiohttp ACI app,
and tool handlers calling a local server playing the platform's
``POST /channel-read`` route. Only the SDK client is scripted, and it calls the
tools through the server object the boot built, as the SDK does.
"""

from __future__ import annotations

import base64
import contextlib
import functools
import json
import logging
import os
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable
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

# Wire names spelled out: a pin derived from the constant it pins agrees with itself.
HISTORY_TOOL = "mcp__curie-slack__read_channel_history"
THREAD_TOOL = "mcp__curie-slack__read_thread_replies"
MESSAGE_TOOL = "mcp__curie-slack__read_channel_message"
LIVE_NAMES = frozenset({HISTORY_TOOL, THREAD_TOOL, MESSAGE_TOOL})

CAPABILITY_HEADER = "X-Curie-Channel-Read"
NO_CAP = "channel_read.no_capability"
AGENT = "00000000-0000-4000-8000-00000000a001"
DEPLOYMENT = "00000000-0000-4000-8000-00000000d001"
DEFAULT_CHANNEL = {"kind": "slack", "address": "C0EXAMPLE1"}
# Every minted token ends in this, so a leak shows up in a substring search.
SENTINEL = "chr-token-sentinel"
MARKER = "channel-body-marker-7f3a2c"

HISTORY_ARGS = {"oldest": "2026-10-03T00:00:00Z", "latest": "2026-10-04T00:00:00Z"}
THREAD_ARGS = {"thread_id": "1759449600.000100", **HISTORY_ARGS}
MESSAGE_ARGS = {"message_id": "1759449600.000100:1759449700.000200"}
TOOL_CASES = [
    ("read_channel_history", HISTORY_ARGS, "history"),
    ("read_thread_replies", THREAD_ARGS, "thread"),
    ("read_channel_message", MESSAGE_ARGS, "message"),
]

# The platform route's 200 shape (plan wire contract item 3).
PAGE: dict[str, Any] = {
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
# The history-only grant: its catalogue is unchanged by the canvas tools (ADR 0200).
_HISTORY_ONLY = frozenset({"channelRead"})


# --- Capabilities, the platform route double, and the tool call the SDK makes ---


def _token(
    *,
    gen: int = 1,
    turn: str = "evt-1",
    agent: str = AGENT,
    deployment: str = DEPLOYMENT,
    default: dict[str, str] | None = DEFAULT_CHANNEL,
) -> str:
    """A ``chr`` token with real claims; the runner cannot verify the signature."""

    now = int(time.time())
    claims: dict[str, Any] = {
        "aud": "channel.read",
        "agent": agent,
        "deployment": deployment,
        "turn": turn,
    }
    claims |= {"grant": "a" * 64, "gen": gen, "default": default, "iat": now, "exp": now + 900}
    payload = (
        base64.urlsafe_b64encode(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"chr.{payload}.{SENTINEL}-g{gen}-{turn}"


def _frame(base: dict[str, Any], capability: Any = _OMIT) -> dict[str, Any]:
    frame = dict(base)
    if capability is not _OMIT:
        frame["channel_read"] = capability
    return frame


def _event(capability: dict[str, str] | None) -> Event:
    return Event.model_validate(_frame(_EVENT_FRAME, _OMIT if capability is None else capability))


class _ChannelApi:
    """The platform ``POST /channel-read`` route, recording every request."""

    def __init__(self) -> None:
        self.status = 200
        self.payload: Any = PAGE
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
                    body=self.raw, status=self.status, headers={"Content-Type": "application/json"}
                )
            return web.json_response(self.payload, status=self.status)

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


@contextlib.asynccontextmanager
async def _served(
    api: _ChannelApi, *, capability: bool = True, token: str | None = None
) -> AsyncIterator[tuple[TestServer, ChannelReadTurn, Any]]:
    """The route double, a holder trusting it, and the built server instance."""

    async with TestServer(api.app()) as server:
        turn = ChannelReadTurn(trusted_origin=_origin(server))
        if capability:
            turn.begin(_event({"url": _url(server), "token": token or _token()}))
        yield server, turn, build_channel_read_server(turn, _HISTORY_ONLY)["instance"]


def _read_once(api: _ChannelApi, case: Any = TOOL_CASES[0], token: Any = None) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with _served(api, token=token) as (_, _, instance):
            return await _call_server(instance, case[0], case[1])

    return anyio.run(go)


def _assert_refused(result: dict[str, Any], code: str) -> None:
    assert result["is_error"] is True, result
    assert code in result["text"], result


# --- The production boot, with the SDK client scripted ---


_result = functools.partial(
    ResultMessage, duration_ms=1, duration_api_ms=1, num_turns=1, session_id="sdk-session"
)


class _Sdk:
    """The scripted SDK client: one queue across turns (#3425); ``native`` is its session file."""

    def __init__(self, options: Any) -> None:
        self.options = options
        self.queue: list[Any] = []
        self.arrived = anyio.Event()
        self.queries: list[str] = []
        self.interrupt_entered = anyio.Event()
        self.interrupt_release: anyio.Event | None = None
        self.native: list[dict[str, Any]] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        if self.interrupt_release is not None:
            self.interrupt_release.set()

    async def query(self, text: str) -> None:
        self.queries.append(text)

    def put(self, message: Any, *, stream: bool = True) -> None:
        projected = model_message_to_conversation(message)
        if projected is not None:
            self.native.append(projected.to_dict())
        if stream:
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
        self.interrupt_entered.set()
        if self.interrupt_release is not None:
            await self.interrupt_release.wait()
        self.put(_result(subtype="error_during_execution", is_error=True, result=""))

    async def export_replay_state(self) -> HarnessReplayState:
        entries = tuple(self.native)
        self.native = []
        return HarnessReplayState(harness="claude", kind="checkpoint", entries=entries)

    def request_full_checkpoint(self) -> None:
        return None

    async def call(self, short_name: str, args: dict[str, Any]) -> dict[str, Any]:
        server = self.options.mcp_servers[CHANNEL_READ_SERVER_NAME]["instance"]
        return await _call_server(server, short_name, args)

    def use(self, call_id: str, name: str, args: dict[str, Any]) -> None:
        block = ToolUseBlock(id=call_id, name=name, input=dict(args))
        self.put(AssistantMessage(content=[block], model="stub-model"))

    async def tool_round(
        self, call_id: str, short_name: str, args: dict[str, Any], *, stream_result: bool = True
    ) -> dict[str, Any]:
        """tool_use, execution, tool_result; unstreamed results reach only the session file."""

        self.use(call_id, f"mcp__{CHANNEL_READ_SERVER_NAME}__{short_name}", args)
        result = await self.call(short_name, args)
        block = ToolResultBlock(
            tool_use_id=call_id,
            content=[{"type": "text", "text": result["text"]}],
            is_error=True if result["is_error"] else None,
        )
        self.put(UserMessage(content=[block]), stream=stream_result)
        return result

    def finish(self, text: str = "done") -> None:
        self.put(AssistantMessage(content=[TextBlock(text=text)], model="stub-model"))
        self.put(_result(subtype="success", is_error=False, result=text))


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
        sessions.append(_Sdk(options))
        return sessions[-1]

    monkeypatch.setattr(boot, "probe_mcp_tool_capability", probe)
    monkeypatch.setattr(boot, "ClaudeAgentSession", make)
    return sessions


def _config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    api_base: str = "http://state.invalid",
    *,
    grant: bool | None = True,
    policy: dict[str, list[str]] | None = None,
) -> RunnerConfig:
    """The eligible boot env; ``CURIE_STATE_URL`` is the origin the read route lives on."""

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


def _boot_options(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **config: Any
) -> tuple[Any, Any]:
    sessions = _patch_sdk(monkeypatch)
    runner = build_runner(_config(monkeypatch, tmp_path, **config), fake_model=False)
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

    @property
    def headers(self) -> list[str | None]:
        return [header for _, header, _ in self.api.received]

    def capability(self, **claims: Any) -> dict[str, str]:
        return {"url": f"{self.base}/channel-read", "token": _token(**claims)}

    async def open(self, capability: dict[str, str] | None) -> Any:
        before = len(self.sdk.queries)
        frame = _frame(_EVENT_FRAME, _OMIT if capability is None else capability)
        response = await self.client.post("/v1/event", json=frame, headers=_AUTH)
        assert response.status == 200
        await _until(lambda: len(self.sdk.queries) > before)
        return response

    async def steer(self, capability: Any = _OMIT) -> int:
        response = await self.client.post(
            "/v1/steer", json=_frame(_STEER_FRAME, capability), headers=_AUTH
        )
        return response.status

    async def read(self) -> dict[str, Any]:
        return await self.sdk.call("read_channel_history", HISTORY_ARGS)

    async def close_turn(self, response: Any, text: str = "done") -> Final:
        self.sdk.finish(text)
        final = parse_ndjson(await response.text())[-1]
        assert isinstance(final, Final)
        return final

    async def read_while_stopping(self, stop: Callable[[], Awaitable[Any]]) -> tuple[Any, Any]:
        """Run ``stop`` with the SDK interrupt blocked, reading while it blocks."""

        release = self.sdk.interrupt_release = anyio.Event()
        reads: list[dict[str, Any]] = []

        async def probe() -> None:
            await self.sdk.interrupt_entered.wait()
            assert self.runner.turn_active
            reads.append(await self.read())
            release.set()

        async with anyio.create_task_group() as tg:
            tg.start_soon(probe)
            with anyio.fail_after(5):
                stopped = await stop()
        return stopped, reads[0]

    @contextlib.asynccontextmanager
    async def disconnecting_turn(self) -> AsyncIterator[Any]:
        """A direct ``run_turn`` stream, open past its first event, as server.py drives it."""

        self.sdk.use("call-0", "Bash", {"command": "true"})
        stream = self.runner.run_turn(_event(self.capability()))
        try:
            with anyio.fail_after(5):
                await stream.__anext__()
            yield stream
        except BaseException:
            await stream.aclose()
            raise


_Drive = Callable[[Callable[[_Rig], Awaitable[None]]], None]


@pytest.fixture
def drive(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _Drive:
    """Run a scenario against a granted production boot and the real ACI app."""

    async def go(scenario: Callable[[_Rig], Awaitable[None]]) -> None:
        api, sessions, store = _ChannelApi(), _patch_sdk(monkeypatch), _Store()
        async with TestServer(api.app()) as api_server:
            base = str(api_server.make_url("")).rstrip("/")
            config = _config(monkeypatch, tmp_path, base)
            # The boot runs its own event loop (the connector probe), so build it off this one.
            runner = await anyio.to_thread.run_sync(
                lambda: build_runner(config, fake_model=False, history_store=store)
            )
            await runner.start()
            async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
                await scenario(_Rig(api, base, client, runner, sessions, store))

    return lambda scenario: anyio.run(go, scenario)


# --- Mount, catalogue and advertisement ---


def _status_fields(runner: Any) -> list[Any]:
    async def go() -> list[Any]:
        await runner.start()
        async with TestClient(TestServer(create_app(runner, token=_TOKEN))) as client:
            values = []
            for path, headers in (("/status", {}), ("/v1/status", _AUTH)):
                response = await client.get(path, headers=headers)
                assert response.status == 200
                values.append((await response.json()).get(CHANNEL_READ_STATUS_FIELD))
            return values

    return anyio.run(go)


@pytest.mark.parametrize(
    ("grant", "fake", "mounted"),
    [(None, False, False), (False, False, False), (True, True, False), (True, False, True)],
    ids=["absent", "false", "granted-fake-model", "granted"],
)
def test_only_a_granted_real_model_boot_mounts_and_advertises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: bool | None, fake: bool, mounted: bool
) -> None:
    if fake:
        runner = build_runner(_config(monkeypatch, tmp_path, grant=grant), fake_model=True)
    else:
        runner, options = _boot_options(monkeypatch, tmp_path, grant=grant)
        servers = {APPROVAL_SERVER_NAME, STATE_SERVER_NAME, CHANNEL_READ_SERVER_NAME}
        assert set(options.mcp_servers) == servers - (
            set() if mounted else {CHANNEL_READ_SERVER_NAME}
        )
        published = _published_live_tool_names(options.mcp_servers)
        slack = {name for name in published if name.startswith("mcp__curie-slack__")}
        assert slack == (LIVE_NAMES if mounted else set())
    assert frozenset(CHANNEL_READ_TOOL_NAMES) == LIVE_NAMES
    # The contract: anything but literal true means unsupported.
    assert [value is True for value in _status_fields(runner)] == [mounted, mounted]


@pytest.mark.parametrize(
    ("grant", "policy", "hidden"),
    [
        (True, {"allow": ["curie-slack/*"]}, set()),
        (True, {"deny": ["curie-slack/*"]}, LIVE_NAMES),
        (
            True,
            {"allow": ["curie-slack/*"], "deny": ["curie-slack/read_channel_history"]},
            {HISTORY_TOOL},
        ),
        (True, {"allow": ["curie-slack/read_thread_replies"]}, {HISTORY_TOOL, MESSAGE_TOOL}),
        # Ungranted, a literal curie-slack pattern is invalid; deny-all still names none.
        (None, {"deny": ["*/*"]}, set()),
    ],
    ids=["allow", "deny-all", "deny-one", "unmatched", "ungranted"],
)
def test_policy_governs_the_mounted_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: Any, policy: Any, hidden: Any
) -> None:
    # The projection reads the boot's own mounted names; the probe never sees an in-process server.
    runner, options = _boot_options(monkeypatch, tmp_path, grant=grant, policy=policy)
    disallowed = LIVE_NAMES & set(options.disallowed_tools)
    assert disallowed == hidden
    gate = runner._approval_gate  # noqa: SLF001 - the gate the boot built
    assert gate is not None
    if grant:
        for interceptor in ("hook", "callback"):
            reason = _interception_reason(gate, HISTORY_TOOL, interceptor)
            assert ("not permitted for this agent" in reason) is bool(hidden), (interceptor, reason)
            assert reason == "" or hidden, (interceptor, reason)
    for name in LIVE_NAMES:
        assert not is_platform_owned_tool(name, state_server_mounted=True)
        assert not is_platform_owned_tool(
            name, state_server_mounted=True, memory_tools_mounted=True
        )


def test_reads_are_not_side_effects_and_take_no_credential_argument() -> None:
    classifier = SideEffectClassifier()
    assert not any(classifier.is_side_effecting(name) for name in LIVE_NAMES)

    async def listed() -> list[dict[str, Any]]:
        server = build_channel_read_server(
            ChannelReadTurn(trusted_origin=("http", "h", 8080)), _HISTORY_ONLY
        )
        entry = server["instance"].get_request_handler("tools/list")
        assert entry is not None
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return [tool.model_dump() for tool in result.tools]

    tools = anyio.run(listed)
    assert {tool["name"] for tool in tools} == {name for name, _, _ in TOOL_CASES}
    for tool in tools:
        schema = tool.get("inputSchema") or tool.get("input_schema") or {}
        properties = {name.lower() for name in (schema.get("properties") or {})}
        assert not properties & {"token", "url", "capability", "credential"}, tool["name"]


# --- The tool handlers against the platform route double ---


@pytest.mark.parametrize(
    "state", ["never-begun", "begun-without", "ended", "other-origin", "other-path"]
)
def test_a_tool_without_a_usable_capability_refuses_without_http(state: str) -> None:
    api, foreign = _ChannelApi(), _ChannelApi()

    async def go() -> list[dict[str, Any]]:
        async with (
            _served(api, capability=state == "ended") as (server, turn, instance),
            TestServer(foreign.app()) as foreign_server,
        ):
            if state == "begun-without":
                turn.begin(_event(None))
            elif state == "ended":
                turn.end()
            elif state.startswith("other-"):
                other = {"other-origin": _url(foreign_server), "other-path": _url(server, "/x")}
                turn.begin(_event({"url": other[state], "token": _token()}))
            return [await _call_server(instance, name, args) for name, args, _ in TOOL_CASES]

    for result in anyio.run(go):
        _assert_refused(result, NO_CAP)
    assert api.received == foreign.received == []


@pytest.mark.parametrize("case", TOOL_CASES, ids=[name for name, _, _ in TOOL_CASES])
def test_a_tool_with_capability_presents_the_header_and_labels_content_untrusted(
    case: tuple[str, dict[str, Any], str],
) -> None:
    api, token = _ChannelApi(), _token()
    result = _read_once(api, case, token)
    _, args, operation = case

    assert result["is_error"] is False, result
    [(path, header, body)] = api.received
    assert (path, header) == ("/channel-read", token)
    assert body == {**body, "operation": operation, **args}
    assert SENTINEL not in json.dumps(body)
    payload = json.loads(result["text"])
    assert (payload["content_trust"], payload["source"]) == ("untrusted", "channel")
    assert {key: payload[key] for key in PAGE} == PAGE
    assert SENTINEL not in result["text"]


# The API's own limits (apps/api channel_read): 100 records a page, text cut at 4000 characters.
_MAX_LIMIT = 100


def _api_body(payload: Any) -> bytes:
    """Serialized as FastAPI's JSONResponse does: raw UTF-8, compact separators."""

    return json.dumps(
        payload, ensure_ascii=False, allow_nan=False, indent=None, separators=(",", ":")
    ).encode("utf-8")


def _maximum_unicode_page() -> dict[str, Any]:
    thread = "1759449600.000100"
    base = {**PAGE["messages"][0], "thread_id": thread, "truncated": True, "reply_count": 0}
    # U+1F600 is 4 UTF-8 bytes, the widest legal character.
    base["text"] = ("\U0001f600" * 4000) + " [truncated]"
    archive = "https://example.slack.com/archives/C0EXAMPLE1/p"
    messages = [
        {**base, "id": f"{thread}:{ts}", "provenance": f"{archive}{ts.replace('.', '')}"}
        for ts in (f"1759449{index:03d}.000100" for index in range(_MAX_LIMIT))
    ]
    return {"messages": messages, "has_more": True, "next_cursor": "c" * 512}


def test_the_response_ceiling_refuses_oversize_and_reads_a_maximum_legal_page_whole() -> None:
    api = _ChannelApi()
    # 16 MiB of valid JSON, far past any legal page (under 3 MiB even all \uXXXX escapes).
    oversized = {
        "messages": [
            {**PAGE["messages"][0], "id": f"1759449600.{index:06d}", "text": "x" * 170_000}
            for index in range(_MAX_LIMIT)
        ],
        "has_more": False,
    }
    page = _maximum_unicode_page()

    async def go() -> tuple[dict[str, Any], dict[str, Any]]:
        async with _served(api) as (_, _, instance):
            api.raw = _api_body(oversized)
            assert len(api.raw) > 16 * 1_048_576
            refused = await _call_server(instance, "read_channel_history", HISTORY_ARGS)
            api.raw = _api_body(page)
            # The case that matters: a legal page larger than 1 MiB on the wire.
            assert len(api.raw) > 1_048_576
            whole = await _call_server(instance, "read_thread_replies", THREAD_ARGS)
            return refused, whole

    refused, whole = anyio.run(go)
    _assert_refused(refused, "channel_read.unavailable")
    assert "x" * 100 not in refused["text"]
    assert whole["is_error"] is False, whole["text"][:300]
    payload = json.loads(whole["text"])
    assert payload["messages"] == page["messages"]
    assert payload["content_trust"] == "untrusted"
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
    result = _read_once(api)
    _assert_refused(result, code)
    if expect is not None:
        assert expect in result["text"]
    assert MARKER not in result["text"]
    assert len(api.received) == 1


def test_the_token_stays_out_of_repr_and_logs(caplog: pytest.LogCaptureFixture) -> None:
    api = _ChannelApi()
    api.refuse(403, "channel_read.not_member")
    seen: list[str] = []
    reprs: list[str] = []

    async def go() -> None:
        async with _served(api) as (server, turn, instance):
            reprs.extend((repr(turn), str(turn)))
            # A refused read, then rejected steers (mismatched scope, malformed, stale).
            seen.append(
                json.dumps(await _call_server(instance, "read_channel_history", HISTORY_ARGS))
            )
            turn.steer(_event({"url": _url(server), "token": _token(gen=2, turn="evt-other")}))
            turn.steer(_event({"url": _url(server), "token": "chr.not-base64!.x" + SENTINEL}))
            turn.begin(_event({"url": _url(server), "token": _token(gen=5)}))
            turn.steer(_event({"url": _url(server), "token": _token(gen=4)}))
            reprs.extend((repr(turn), str(turn)))

        # Transport failure: the trusted origin has nothing listening.
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
        turn = ChannelReadTurn(trusted_origin=("http", "127.0.0.1", port))
        turn.begin(_event({"url": f"http://127.0.0.1:{port}/channel-read", "token": _token()}))
        failed = await _call_server(
            build_channel_read_server(turn, _HISTORY_ONLY)["instance"],
            "read_channel_history",
            HISTORY_ARGS,
        )
        _assert_refused(failed, "channel_read.unavailable")
        assert str(port) not in failed["text"]  # the endpoint is not disclosed either
        seen.append(json.dumps(failed))

    with caplog.at_level(logging.DEBUG):
        anyio.run(go)

    for text in seen + reprs:
        assert SENTINEL not in text, text
    for text in reprs:
        assert "chr." not in text and "/channel-read" not in text, text
    assert SENTINEL not in caplog.text
    assert all(SENTINEL not in repr(record.args) for record in caplog.records)


# --- The holder lifecycle through the real ACI app ---

_NULL, _OMITTED, _REFUSED = ("steer", None), ("steer", _OMIT), ("read", None)


def _st(**claims: Any) -> tuple[str, Any]:
    return ("steer", claims)


def _rd(**claims: Any) -> tuple[str, Any]:
    return ("read", claims)


_MISMATCHES: dict[str, dict[str, Any]] = {
    "turn": {"turn": "evt-other"},
    "agent": {"agent": "00000000-0000-4000-8000-00000000a002"},
    "deployment": {"deployment": "00000000-0000-4000-8000-00000000d002"},
    "default-channel": {"default": {"kind": "slack", "address": "C0OTHER"}},
    "default-dropped": {"default": None},
}
# Each scenario: the event's claims (None: no capability), then steers and the expected reads.
_STEER_SCENARIOS: dict[str, tuple[dict[str, Any] | None, list[tuple[str, Any]]]] = {
    "null-clears": ({}, [_rd(), _NULL, _REFUSED]),
    "omitted-clears": ({}, [_rd(), _OMITTED, _REFUSED]),
    "newer-replaces": ({}, [_st(gen=2), _rd(gen=2)]),
    **{f"mismatch-{k}": ({}, [_st(gen=2, **c), _REFUSED]) for k, c in _MISMATCHES.items()},
    "no-admitted-scope": (None, [_st(gen=2), _REFUSED]),
    # The admitted scope outlives a null, so another turn's capability cannot slip in.
    "null-then-mismatch": (
        {"turn": "evt-s"},
        [_NULL, _st(gen=2, turn="evt-t"), _REFUSED, _st(gen=3, turn="evt-s")]
        + [_rd(gen=3, turn="evt-s")],
    ),
    # A delayed older generation never rolls back, not even after a null.
    "reordered-generations": (
        {},
        [_st(gen=3), _st(gen=2), _rd(gen=3), _NULL, _st(gen=2), _REFUSED]
        + [_st(gen=3), _REFUSED, _st(gen=4), _rd(gen=4)],
    ),
}


@pytest.mark.parametrize(("opened", "steps"), _STEER_SCENARIOS.values(), ids=_STEER_SCENARIOS)
def test_steer_replaces_clears_or_rejects_the_capability(
    drive: _Drive,
    opened: dict[str, Any] | None,
    steps: list[tuple[str, Any]],
) -> None:
    async def scenario(rig: _Rig) -> None:
        response = await rig.open(None if opened is None else rig.capability(**opened))
        presented: list[str | None] = []
        for op, arg in steps:
            if op == "steer":
                capability = arg if arg is None or arg is _OMIT else rig.capability(**arg)
                assert await rig.steer(capability) == 200
                continue
            result = await rig.read()
            if arg is None:
                _assert_refused(result, NO_CAP)
            else:
                assert result["is_error"] is False, result
                presented.append(_token(**arg))
            assert rig.headers == presented
        assert (await rig.close_turn(response)).status is SessionStatus.DONE

    drive(scenario)


@pytest.mark.parametrize("path", ["completion", "interrupt", "timeout", "disconnect"])
def test_every_terminal_path_clears(drive: _Drive, path: str) -> None:
    async def scenario(rig: _Rig) -> None:
        if path == "disconnect":
            # Closing the turn stream is what server.py does when the worker goes away.
            async with rig.disconnecting_turn() as stream:
                assert (await rig.read())["is_error"] is False
            _, during = await rig.read_while_stopping(stream.aclose)
            _assert_refused(during, NO_CAP)
            assert len(rig.api.received) == 1
            return
        response = await rig.open(rig.capability())
        assert (await rig.read())["is_error"] is False
        if path == "completion":
            await rig.close_turn(response)
            _assert_refused(await rig.read(), NO_CAP)
            # The next turn reads with its own capability; a stale timeout does not clear it.
            second = await rig.open(rig.capability(turn="evt-2"))
            stale = {**_AUTH, _TURN_EPOCH_HEADER: response.headers[_TURN_EPOCH_HEADER]}
            assert (await rig.client.post("/v1/timeout", headers=stale)).status == 409
            assert (await rig.read())["is_error"] is False
            assert rig.headers == [_token(), _token(turn="evt-2")]
            assert (await rig.close_turn(second)).status is SessionStatus.DONE
            return

        # The SDK stop blocks until released, so only the control route can have cleared it.
        epoch = response.headers[_TURN_EPOCH_HEADER]
        if path == "interrupt":
            body = {"kind": "interrupt", "reason": "stop"}
            stop = functools.partial(rig.client.post, "/v1/interrupt", json=body, headers=_AUTH)
        else:
            headers = {**_AUTH, _TURN_EPOCH_HEADER: epoch}
            stop = functools.partial(rig.client.post, "/v1/timeout", headers=headers)
        reply, during = await rig.read_while_stopping(stop)
        await response.text()
        assert reply.status == 200
        _assert_refused(during, NO_CAP)
        assert len(rig.api.received) == 1

    drive(scenario)


# --- Retrieved bodies and the token stay out of everything persisted ---


def _tool_results(record: TurnRecord) -> dict[str, tuple[str, Any]]:
    """Each persisted tool_result as (text, is_error), keyed by tool_use_id."""

    results = {}
    for message in record.messages:
        for block in message.content if isinstance(message.content, list) else []:
            if block.get("type") == "tool_result":
                content = block.get("content")
                if not isinstance(content, str):
                    content = "".join(str(item.get("text") or "") for item in content or [])
                results[str(block["tool_use_id"])] = (content, block.get("is_error"))
    return results


def test_persisted_records_keep_provenance_not_bodies_or_the_token(
    drive: _Drive, caplog: pytest.LogCaptureFixture
) -> None:
    async def scenario(rig: _Rig) -> None:
        # Liveness: a turn with no channel read exports native replay.
        await rig.close_turn(await rig.open(rig.capability(turn="evt-0")))

        reading = await rig.open(rig.capability(turn="evt-1"))
        assert await rig.steer(rig.capability(turn="evt-1", gen=2)) == 200
        read = await rig.sdk.tool_round("call-read", "read_channel_history", HISTORY_ARGS)
        assert read["is_error"] is False
        assert MARKER in read["text"]
        rig.api.refuse(403, "channel_read.not_member")
        refused = await rig.sdk.tool_round("call-refused", "read_channel_history", HISTORY_ARGS)
        assert refused["is_error"] is True
        # A rejected steer logs a warning; the warning carries no token.
        assert await rig.steer(rig.capability(gen=3, turn="evt-other")) == 200
        await rig.close_turn(reading)

        await rig.close_turn(await rig.open(None))
        # A new SDK session carries no channel body, so native replay resumes.
        assert (await rig.client.post("/v1/reset", headers=_AUTH)).status == 200
        await rig.close_turn(await rig.open(None))

        records = rig.store.records
        replays = [record.harness_replay is not None for record in records]
        assert replays == [True, False, False, True]
        results = _tool_results(records[1])
        stub = json.loads(results["call-read"][0])
        assert stub["bodies_retained"] is False
        assert [(i["id"], i["provenance"]) for i in stub["messages"]] == [
            (i["id"], i["provenance"]) for i in PAGE["messages"]
        ]
        assert all("text" not in item for item in stub["messages"])
        assert stub["has_more"] is True
        # An error carries no body and is kept verbatim.
        assert results["call-refused"] == (refused["text"], True)

        assert rig.headers == [_token(turn="evt-1", gen=2)] * 2
        persisted = json.dumps([record.to_dict() for record in records])
        assert MARKER not in persisted and SENTINEL not in persisted
        assert SENTINEL not in json.dumps([body for _, _, body in rig.api.received])
        assert not any(SENTINEL in value for value in os.environ.values())
        assert SENTINEL not in json.dumps(dict(rig.sdk.options.env or {}))

    with caplog.at_level(logging.DEBUG):
        drive(scenario)
    assert SENTINEL not in caplog.text
    assert all(SENTINEL not in repr(record.args) for record in caplog.records)


def test_disconnect_before_result_never_reaches_native_replay(drive: _Drive) -> None:
    async def scenario(rig: _Rig) -> None:
        async with rig.disconnecting_turn() as stream:
            # The tool returned a body, but the consumer left before the result streamed.
            read = await rig.sdk.tool_round(
                "call-read", "read_channel_history", HISTORY_ARGS, stream_result=False
            )
            assert read["is_error"] is False
            with anyio.fail_after(5):
                await stream.__anext__()
        with anyio.fail_after(5):
            await stream.aclose()
        # The SDK's own session state does hold the body.
        assert any(MARKER in json.dumps(entry) for entry in rig.sdk.native)

        assert (await rig.close_turn(await rig.open(None))).status is SessionStatus.DONE
        assert rig.store.records, "the following turn was not recorded"
        assert rig.store.records[-1].harness_replay is None
        assert all(MARKER not in json.dumps(r.to_dict()) for r in rig.store.records)

    drive(scenario)
