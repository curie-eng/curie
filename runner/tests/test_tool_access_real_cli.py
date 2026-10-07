"""Read-only tool access on the real bundled CLI, through the real session.

The fake tier decides a tool call where the runner says it does. These cases put
the real bundled Claude CLI between the runner and the tools, so what is proven
is what the CLI actually does with the runner's answer: whether a refused call
runs, what the model is told, and whether an approval can still be raised. A
local stand-in for /v1/messages scripts one tool call per turn: no credential
and no network.

Measured on claude-agent-sdk 0.2.159 with bundled CLI 2.1.281 (2026-09-30):

- with no ``can_use_tool`` the session runs under ``bypassPermissions``, and a
  PreToolUse hook answering ``permissionDecision: deny`` without
  ``continue_: False`` still stops the call: a ``Bash`` ``touch`` never created
  its file;
- the model is then sent that call's result as ``is_error: true`` with the text
  ``PreToolUse:<tool> hook error: <reason>``, and the turn carries on to a
  normal ``end_turn`` result.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, ToolAccess, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk.types import SdkPluginConfig
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import (
    ApprovalGate,
    build_approval_gate,
)
from curie_runner.harness.claude.approval import (
    build_approval_hook,
    build_can_use_tool,
)
from curie_runner.session import SessionRunner
from curie_runner.side_effects import CLAUDE_READONLY_TOOLS
from curie_runner.tool_access import (
    TurnToolAccess,
    front_can_use_tool,
    front_pre_tool_use_hooks,
)
from curie_telemetry import configure_meter_provider
from curie_telemetry import metrics as curie_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader


def _user_message(prompt: str) -> str:
    start_marker = "[user-message"
    end_marker = "[end-user-message"
    if start_marker not in prompt or end_marker not in prompt:
        return prompt
    start = prompt.index("\n", prompt.index(start_marker)) + 1
    body = prompt[start : prompt.index(end_marker)]
    return body[:-1] if body.endswith("\n") else body


_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_result_server.py"
_TOOL = web.AppKey("tool", str)
_INPUT = web.AppKey("input", dict[str, Any])
_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_TURN_TIMEOUT_S = 90
_CALL_ID = "toolu_acme01"


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    """A real meter provider for this test only; the module globals are restored."""

    monkeypatch.setattr(curie_metrics, "_provider", curie_metrics._provider)
    monkeypatch.setattr(curie_metrics, "_instruments", curie_metrics._instruments)
    metric_reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[metric_reader], shutdown_on_exit=False)
    configure_meter_provider(provider)
    yield metric_reader
    provider.shutdown()


def _points(reader: InMemoryMetricReader) -> dict[tuple[str, str], float]:
    data = reader.get_metrics_data()
    points: dict[tuple[str, str], float] = {}
    if data is None:
        return points
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != "curie.tool.result":
                    continue
                for point in getattr(metric.data, "data_points", ()):
                    attributes = dict(point.attributes)
                    key = (str(attributes["origin"]), str(attributes["outcome"]))
                    points[key] = points.get(key, 0) + point.value
    return {key: value for key, value in points.items() if value}


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _tool_results(body: dict[str, Any]) -> list[dict[str, Any]]:
    # Any message, not the last one: this CLI appends ``system`` role entries
    # after the tool result.
    found: list[dict[str, Any]] = []
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list):
            found.extend(
                block
                for block in content
                if isinstance(block, dict) and block.get("type") == "tool_result"
            )
    return found


async def _messages(request: web.Request) -> web.StreamResponse:
    """One scripted tool call, then a text answer once a tool result is sent."""

    body = await request.json()
    request.app[_BODIES].append(body)
    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    message = {
        "id": f"msg_stand_in_{len(request.app[_BODIES])}",
        "type": "message",
        "role": "assistant",
        "model": body.get("model", "stand-in"),
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    events: list[tuple[str, dict[str, Any]]] = [
        ("message_start", {"type": "message_start", "message": message})
    ]
    if _tool_results(body):
        stop_reason = "end_turn"
        block: dict[str, Any] = {"type": "text", "text": ""}
        delta: dict[str, Any] = {"type": "text_delta", "text": "answered without it"}
    else:
        stop_reason = "tool_use"
        block = {"type": "tool_use", "id": _CALL_ID, "name": request.app[_TOOL], "input": {}}
        delta = {"type": "input_json_delta", "partial_json": json.dumps(request.app[_INPUT])}
    events += [
        (
            "content_block_start",
            {"type": "content_block_start", "index": 0, "content_block": block},
        ),
        ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    for event, data in events:
        await response.write(_sse(event, data))
    await response.write_eof()
    return response


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tool: str,
    tool_input: dict[str, Any],
    access: TurnToolAccess,
    tool_access: ToolAccess | None,
    gate: ApprovalGate | None = None,
) -> tuple[Final, list[str], list[dict[str, Any]]]:
    """One real turn in which the model calls ``tool`` once.

    Wired the way ``__main__`` wires a real session: the read-only front over
    the approval hook (or alone), the permission callback fronted only when a
    gate exists (so an ungated session keeps ``bypassPermissions``), and the
    same access object on the SessionRunner, which sets it before the prompt.
    Returns the final, what the connector recorded, and the stand-in's requests.
    """

    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    calls = tmp_path / "connector-calls.txt"

    async def scenario() -> tuple[Final, list[dict[str, Any]]]:
        app = web.Application()
        app[_TOOL] = tool
        app[_INPUT] = tool_input
        app[_BODIES] = []
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            options = build_options(
                plugins=[],
                model="claude-sonnet-5",
                system_prompt="You read the acme ledger.",
                max_turns=3,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                hooks=front_pre_tool_use_hooks(
                    build_approval_hook(gate) if gate is not None else None, access
                ),
                can_use_tool=(
                    front_can_use_tool(build_can_use_tool(gate), access)
                    if gate is not None
                    else None
                ),
                mcp_servers=cast(
                    "Any",
                    {
                        "acme": {
                            "type": "stdio",
                            "command": sys.executable,
                            "args": [str(_SERVER)],
                            "env": {"CURIE_TEST_TOOL_RESULT_CALLS": str(calls)},
                        }
                    },
                ),
                env={
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                },
            )
            expected_mode = "default" if gate is not None else "bypassPermissions"
            assert options.permission_mode == expected_mode
            runner = SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-read-only",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
                approval_gate=gate,
                tool_access=access,
            )
            lines: list[str] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async for line in runner.run_turn(
                        Event(
                            type="message",
                            text="check the ledger",
                            user="U0EXAMPLE1",
                            ts="1",
                            tool_access=tool_access,
                        )
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
            assert len(finals) == 1, lines
            return finals[0], list(app[_BODIES])

    final, bodies = anyio.run(scenario)
    recorded = calls.read_text(encoding="utf-8").split() if calls.exists() else []
    return final, recorded, bodies


def _refusals_sent(bodies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        result
        for body in bodies
        for result in _tool_results(body)
        if result.get("tool_use_id") == _CALL_ID
    ]


def _assert_no_approval(final: Final) -> None:
    assert final.status is SessionStatus.DONE
    assert final.approval_summary is None
    assert final.approval_granted_tool is None
    assert final.approval_gate_kind is None


def test_a_shell_write_is_refused_on_a_session_with_no_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader: InMemoryMetricReader
) -> None:
    """RUNNER-TOOL-ACCESS-2 under ``bypassPermissions``: only the hook can refuse.

    Red until the front exists. Red again if the front's deny stops being
    honored under bypassPermissions after an SDK or CLI upgrade, which would let
    the file be created.
    """

    # @spec RUNNER-TOOL-ACCESS-2 RUNNER-TOOL-ACCESS-6
    marker = tmp_path / "written-by-a-read-only-turn"
    access = TurnToolAccess(CLAUDE_READONLY_TOOLS)

    final, _, bodies = _run_turn(
        tmp_path,
        monkeypatch,
        tool="Bash",
        tool_input={"command": f"touch {marker}", "description": "write a file"},
        access=access,
        tool_access=ToolAccess.READ_ONLY,
    )

    assert not marker.exists(), "the refused Bash call ran"
    _assert_no_approval(final)
    [refusal] = _refusals_sent(bodies)
    assert refusal["is_error"] is True
    assert "read-only" in json.dumps(refusal["content"])
    assert access.refused_call_ids == {_CALL_ID}
    assert _points(reader) == {("builtin", "refused"): 1}


def test_a_gated_connector_write_is_refused_without_raising_an_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """RUNNER-TOOL-ACCESS-2, -3 and -6: the gate never sees the call.

    The same wiring pauses for approval on an ordinary turn (the next test), so
    this is the read-only decision and not a quiet gate.
    """

    # @spec RUNNER-TOOL-ACCESS-2 RUNNER-TOOL-ACCESS-3 RUNNER-TOOL-ACCESS-6
    caplog.set_level(logging.WARNING, logger="curie_runner")
    gate = build_approval_gate(
        operator_tools=["mcp__acme__delete_files"],
        policy_routes={},
        mcp_servers=set(),
        connector_servers={"acme"},
    )
    assert gate is not None

    final, ran, bodies = _run_turn(
        tmp_path,
        monkeypatch,
        tool="mcp__acme__delete_files",
        tool_input={"account": "acme-account-PLACEHOLDER"},
        access=TurnToolAccess(CLAUDE_READONLY_TOOLS, requires_approval=gate.requires_approval),
        tool_access=ToolAccess.READ_ONLY,
        gate=gate,
    )

    assert ran == []
    _assert_no_approval(final)
    assert gate.pending_summary is None
    assert gate.pending_halt is False
    [refusal] = _refusals_sent(bodies)
    assert refusal["is_error"] is True
    assert _points(reader) == {("connector", "refused"): 1}
    assert not [r for r in caplog.records if "connector tool error" in r.getMessage()]


def test_the_same_gated_write_on_an_ordinary_turn_still_pauses_for_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RUNNER-TOOL-ACCESS-7: the fronted wiring leaves an ordinary turn alone."""

    # @spec RUNNER-TOOL-ACCESS-7
    gate = build_approval_gate(
        operator_tools=["mcp__acme__delete_files"],
        policy_routes={},
        mcp_servers=set(),
        connector_servers={"acme"},
    )
    assert gate is not None

    final, ran, _ = _run_turn(
        tmp_path,
        monkeypatch,
        tool="mcp__acme__delete_files",
        tool_input={"account": "acme-account-PLACEHOLDER"},
        access=TurnToolAccess(CLAUDE_READONLY_TOOLS, requires_approval=gate.requires_approval),
        tool_access=None,
        gate=gate,
    )

    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == "mcp__acme__delete_files"
    assert ran == []


def test_a_connector_tool_classified_read_only_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader: InMemoryMetricReader
) -> None:
    """RUNNER-TOOL-ACCESS-1: an explicitly read-only MCP tool is not refused."""

    # @spec RUNNER-TOOL-ACCESS-1
    access = TurnToolAccess(CLAUDE_READONLY_TOOLS | {"mcp__acme__list_files"})

    final, ran, _ = _run_turn(
        tmp_path,
        monkeypatch,
        tool="mcp__acme__list_files",
        tool_input={"account": "acme-account-PLACEHOLDER"},
        access=access,
        tool_access=ToolAccess.READ_ONLY,
    )

    assert final.status is SessionStatus.DONE
    assert ran == ["list_files"]
    assert access.refused_call_ids == set()
    assert _points(reader) == {("connector", "success"): 1}


def test_an_unclassified_connector_tool_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RUNNER-TOOL-ACCESS-1: a reading tool without a read-only classification is denied.

    ``list_files`` reads, but nothing classified it read-only here (its server
    declared no ``readOnlyHint``), so the default is to refuse it.
    """

    # @spec RUNNER-TOOL-ACCESS-1 RUNNER-TOOL-ACCESS-2
    final, ran, _ = _run_turn(
        tmp_path,
        monkeypatch,
        tool="mcp__acme__list_files",
        tool_input={"account": "acme-account-PLACEHOLDER"},
        access=TurnToolAccess(CLAUDE_READONLY_TOOLS),
        tool_access=ToolAccess.READ_ONLY,
    )

    assert final.status is SessionStatus.DONE
    assert ran == []


def test_an_ordinary_turn_runs_the_same_shell_write_through_the_front(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reader: InMemoryMetricReader
) -> None:
    """RUNNER-TOOL-ACCESS-7 on the real path: the always-registered front abstains.

    The same ``Bash`` write the first test refuses, on an unrestricted turn of
    the same ungated session: it runs, exactly as it did before the front
    existed. Red if the front's mere presence changed an ordinary call.
    """

    # @spec RUNNER-TOOL-ACCESS-7
    marker = tmp_path / "written-by-an-ordinary-turn"
    access = TurnToolAccess(CLAUDE_READONLY_TOOLS)

    final, _, bodies = _run_turn(
        tmp_path,
        monkeypatch,
        tool="Bash",
        tool_input={"command": f"touch {marker}", "description": "write a file"},
        access=access,
        tool_access=None,
    )

    assert marker.exists(), "the ordinary Bash call did not run"
    assert final.status is SessionStatus.DONE
    [result] = _refusals_sent(bodies)
    assert result.get("is_error") is not True
    assert access.refused_call_ids == set()
    assert _points(reader) == {("builtin", "success"): 1}


# --- a turn nobody prompted ----------------------------------------------------------
#
# Measured on the same CLI: a bundle hook marked ``async`` and ``asyncRewake`` that
# exits 2 after its turn wakes the model on its own, and the runner, which reads one
# result per prompt, then ends the NEXT prompt's turn on that wake turn's result. The
# next prompt's own answer arrives later. If that prompt was read-only and the turn
# after it is ordinary, its tool call would be decided under unrestricted access.


def test_a_read_only_prompt_a_bundle_hook_delayed_never_runs_unrestricted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RUNNER-TOOL-ACCESS-11: the ordinary turn after it gets a fresh SDK session.

    Read-only A, then the hook's wake, then read-only B (whose model answer is
    slow and asks for a Bash write), then ordinary C at once. Red while C shares
    B's CLI: B's write runs under C's access. The hook must have fired, or the
    test proves nothing.
    """

    # @spec RUNNER-TOOL-ACCESS-11 RUNNER-TOOL-ACCESS-4
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    marker = tmp_path / "written-by-a-delayed-read-only-prompt"
    fired = tmp_path / "rewake-fired"
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    bundle = tmp_path / "bundle"
    (bundle / ".claude-plugin").mkdir(parents=True)
    (bundle / "hooks").mkdir()
    (bundle / ".claude-plugin" / "plugin.json").write_text(json.dumps({"name": "acme-bot"}))
    rewake = (
        f"if [ ! -e {fired} ]; then touch {fired}; sleep 2; "
        "echo 'acme reminder: recheck the ledger' >&2; exit 2; fi; exit 0"
    )
    (bundle / "hooks" / "hooks.json").write_text(
        json.dumps(
            {
                "hooks": {
                    "UserPromptSubmit": [
                        {
                            "hooks": [
                                {
                                    "type": "command",
                                    "async": True,
                                    "asyncRewake": True,
                                    "command": rewake,
                                }
                            ]
                        }
                    ]
                }
            }
        )
    )

    def last_user_text(body: dict[str, Any]) -> str:
        texts: list[str] = []
        for message in body.get("messages") or ():
            if message.get("role") != "user":
                continue
            content = message.get("content")
            blocks = [{"type": "text", "text": content}] if isinstance(content, str) else content
            for block in blocks or ():
                if isinstance(block, dict) and block.get("type") == "text":
                    texts.append(str(block.get("text", "")))
                elif isinstance(block, dict) and block.get("type") == "tool_result":
                    texts.append("<tool_result>")
        return texts[-1] if texts else ""

    async def model(request: web.Request) -> web.StreamResponse:
        body = await request.json()
        last = _user_message(last_user_text(body))
        response = web.StreamResponse(headers={"content-type": "text/event-stream"})
        await response.prepare(request)
        start = {
            "id": "msg_stand_in",
            "type": "message",
            "role": "assistant",
            "model": "stand-in",
            "content": [],
            "stop_reason": None,
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
        if body.get("tools") and "PROBE-B" in last:
            await anyio.sleep(4)
            stop = "tool_use"
            block: dict[str, Any] = {
                "type": "tool_use",
                "id": _CALL_ID,
                "name": "Bash",
                "input": {},
            }
            delta: dict[str, Any] = {
                "type": "input_json_delta",
                "partial_json": json.dumps({"command": f"touch {marker}", "description": "w"}),
            }
        else:
            stop = "end_turn"
            block = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": f"answer to {last[:24]}"}
        for event, data in (
            ("message_start", {"type": "message_start", "message": start}),
            (
                "content_block_start",
                {"type": "content_block_start", "index": 0, "content_block": block},
            ),
            ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
            ("content_block_stop", {"type": "content_block_stop", "index": 0}),
            (
                "message_delta",
                {
                    "type": "message_delta",
                    "delta": {"stop_reason": stop, "stop_sequence": None},
                    "usage": {"output_tokens": 1},
                },
            ),
            ("message_stop", {"type": "message_stop"}),
        ):
            await response.write(_sse(event, data))
        await response.write_eof()
        return response

    async def scenario() -> list[Final]:
        app = web.Application()
        app.router.add_post("/v1/messages", model)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            access = TurnToolAccess(CLAUDE_READONLY_TOOLS)
            options = build_options(
                plugins=[SdkPluginConfig(type="local", path=str(bundle))],
                model="claude-sonnet-5",
                system_prompt="You read the acme ledger.",
                max_turns=6,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                hooks=front_pre_tool_use_hooks(None, access),
                can_use_tool=None,
                env={
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                },
            )
            runner = SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-rewake",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
                tool_access=access,
            )
            finals: list[Final] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    for text, tool_access, pause in (
                        ("PROBE-A read the ledger", ToolAccess.READ_ONLY, 5.0),
                        ("PROBE-B read it again", ToolAccess.READ_ONLY, 0.0),
                        ("an ordinary question", None, 7.0),
                    ):
                        lines = [
                            line
                            async for line in runner.run_turn(
                                Event(
                                    type="message",
                                    text=text,
                                    user="U0EXAMPLE1",
                                    ts="1",
                                    tool_access=tool_access,
                                )
                            )
                        ]
                        finals += [e for e in parse_ndjson("".join(lines)) if isinstance(e, Final)]
                        await anyio.sleep(pause)
            finally:
                await runner.close()
            return finals

    finals = anyio.run(scenario)

    assert fired.exists(), "the bundle hook never woke the CLI; the scenario did not happen"
    assert not marker.exists(), "a delayed read-only prompt's write ran unrestricted"
    assert len(finals) == 3
    # The ordinary turn answers its own prompt, not a read-only leftover.
    assert finals[2].text == "answer to an ordinary question"
