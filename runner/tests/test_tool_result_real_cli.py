"""The tool result signal on the real bundled CLI, through the real session.

The fake tier builds ToolResultBlocks by hand, so it can only assert what the
runner does with a shape somebody wrote down. These cases put the real bundled
Claude CLI between the runner and a real stdio MCP connector, so the blocks the
runner counts are the ones the CLI actually delivers. A local stand-in for
/v1/messages scripts one tool call per turn: no credential and no network.

Measured on claude-agent-sdk 0.2.159 with bundled CLI 2.1.281 (2026-09-29):

- a connector answering ``isError: true`` arrives as ``is_error=True`` with the
  server's text; a normal answer arrives as ``is_error=None``;
- a PreToolUse hook deny arrives as ``is_error=True`` for a call that never
  reached the connector, which is why a call the runner's own approval gate
  held must count as awaiting_approval and never as a connector error;
- a connector answering with a JSON-RPC ``error`` instead of a result also
  arrives as ``is_error=True``, carrying the error's message;
- an SDK interrupt while a connector call is in flight is answered by the CLI
  itself with a synthetic ``is_error=True`` result ("The user doesn't want to
  proceed with this tool use..."), then a ResultMessage with subtype
  ``error_during_execution`` and terminal_reason ``aborted_tools``; the
  connector never finishes the call. An operator stop and a turn deadline both
  reach the CLI as that same interrupt;
- the CLI puts ``system`` role entries in ``messages``, so the stand-in cannot
  read "the last message is the tool result". It ends the turn once any message
  carries a tool_result block.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal, cast

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import (
    ApprovalGate,
    build_approval_gate,
    build_approval_hook,
    build_can_use_tool,
)
from curie_runner.session import SessionRunner
from curie_telemetry import configure_meter_provider
from curie_telemetry import metrics as curie_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_result_server.py"
_METRIC = "curie.tool.result"
_ARGUMENT = "acme-account-PLACEHOLDER"
# The fixture's reply to read_ledger (``LEDGER_ERROR_TEXT`` there). Restated
# rather than imported: the fixture directory is excluded from collection.
_LEDGER_ERROR_TEXT = "upstream answered 401 for acme-ledger-PLACEHOLDER"
_TOOL = web.AppKey("tool", str)
_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_TURN_TIMEOUT_S = 90
# The opaque turn epoch the server hands a turn, which a timeout names.
_EPOCH = "acme-epoch-PLACEHOLDER"


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    """A real provider for this test only; the module globals are restored after."""

    monkeypatch.setattr(curie_metrics, "_provider", curie_metrics._provider)
    monkeypatch.setattr(curie_metrics, "_instruments", curie_metrics._instruments)
    metric_reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[metric_reader], shutdown_on_exit=False)
    configure_meter_provider(provider)
    yield metric_reader
    provider.shutdown()


def _points(reader: InMemoryMetricReader) -> dict[tuple[str, str], float]:
    """Summed ``curie.tool.result`` values keyed by (origin, outcome)."""

    data = reader.get_metrics_data()
    points: dict[tuple[str, str], float] = {}
    if data is None:
        return points
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != _METRIC:
                    continue
                for point in getattr(metric.data, "data_points", ()):
                    attributes = dict(point.attributes)
                    assert set(attributes) == {"service.name", "source", "origin", "outcome"}
                    key = (str(attributes["origin"]), str(attributes["outcome"]))
                    points[key] = points.get(key, 0) + point.value
    return {key: value for key, value in points.items() if value}


def _connector_warnings(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str, str]]:
    """(server, tool, message) for each WARNING that names a server and a tool."""

    found: list[tuple[str, str, str]] = []
    for record in caplog.records:
        if record.levelno < logging.WARNING:
            continue
        message = record.getMessage()
        server = re.search(r"(?:^|\s)server=(\S+)", message)
        tool = re.search(r"(?:^|\s)tool=(\S+)", message)
        if server and tool:
            found.append((server.group(1), tool.group(1), message))
    return found


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _carries_tool_result(body: dict[str, Any]) -> bool:
    # Any message, not the last one: this CLI appends ``system`` role entries
    # after the tool result (measured, see the module docstring).
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list) and any(
            isinstance(block, dict) and block.get("type") == "tool_result" for block in content
        ):
            return True
    return False


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
    if _carries_tool_result(body):
        stop_reason = "end_turn"
        events += [
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
                    "delta": {"type": "text_delta", "text": "done"},
                },
            ),
        ]
    else:
        stop_reason = "tool_use"
        events += [
            (
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use",
                        "id": "toolu_acme01",
                        "name": request.app[_TOOL],
                        "input": {},
                    },
                },
            ),
            (
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": json.dumps({"account": _ARGUMENT}),
                    },
                },
            ),
        ]
    events += [
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


def _recorded(calls: Path) -> list[str]:
    return calls.read_text(encoding="utf-8").split() if calls.exists() else []


async def _stop_once_started(
    runner: SessionRunner, calls: Path, tool: str, stop: Literal["interrupt", "timeout"]
) -> None:
    """Stop the turn once the connector has recorded the call's start.

    Waits on the connector's own record, never on a fixed delay, so the stop
    always lands while the call is in flight.
    """

    while tool not in _recorded(calls):
        await anyio.sleep(0.05)
    if stop == "interrupt":
        await runner.interrupt()
    else:
        assert await runner.timeout(_EPOCH)


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tool: str,
    gate: ApprovalGate | None = None,
    stop: Literal["interrupt", "timeout"] | None = None,
) -> tuple[Final, list[str], list[dict[str, Any]]]:
    """One real turn in which the model calls ``mcp__acme__<tool>`` once.

    Returns the turn's final, what the connector recorded, and the requests the
    stand-in received. Wired the way ``__main__`` wires a gated session: the
    gate's PreToolUse hook, its can_use_tool backstop, and the same gate on the
    SessionRunner. With ``stop``, a second task stops the turn the way the
    server does (an operator interrupt, or the turn's deadline) once the
    connector has started the call.
    """

    # The SDK and the CLI must agree on the config directory, and a developer's
    # own OAuth token must not reach the CLI.
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    calls = tmp_path / "connector-calls.txt"

    async def scenario() -> tuple[Final, list[dict[str, Any]]]:
        app = web.Application()
        app[_TOOL] = f"mcp__acme__{tool}"
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
                hooks=build_approval_hook(gate) if gate is not None else None,
                can_use_tool=build_can_use_tool(gate) if gate is not None else None,
                # A stdio server, as a connector is mounted; the parameter is
                # typed for the in-process platform servers only.
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
            runner = SessionRunner(
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-tool-result",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
                approval_gate=gate,
            )
            lines: list[str] = []

            async def drive() -> None:
                async for line in runner.run_turn(
                    Event(type="message", text="read it", user="U0EXAMPLE1", ts="1"),
                    turn_epoch=_EPOCH,
                ):
                    lines.append(line)

            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async with anyio.create_task_group() as tasks:
                        tasks.start_soon(drive)
                        if stop is not None:
                            tasks.start_soon(_stop_once_started, runner, calls, tool, stop)
            finally:
                await runner.close()
            finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
            assert len(finals) == 1, lines
            bodies: list[dict[str, Any]] = app[_BODIES]
            return finals[0], list(bodies)

    final, bodies = anyio.run(scenario)
    return final, _recorded(calls), bodies


def test_a_connector_answering_is_error_counts_as_a_connector_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC3, AC5, AC6 on the real path: ``isError`` reaches the runner as is_error True.

    The turn completing done, the connector having run, and the stand-in having
    been sent the tool result all hold today. Red on the metric and the WARNING
    until the session records them. Red again if the CLI stops delivering an
    MCP ``isError`` result as ``is_error=True`` after an SDK or CLI upgrade.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    final, ran, bodies = _run_turn(tmp_path, monkeypatch, tool="read_ledger")

    assert final.status is SessionStatus.DONE
    assert ran == ["read_ledger"]
    assert any(_carries_tool_result(body) for body in bodies)
    assert _points(reader) == {("connector", "error"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [("acme", "read_ledger")]
    for record in caplog.records:
        if record.levelno >= logging.WARNING:
            for fragment in (_ARGUMENT, "acme-ledger-PLACEHOLDER", "upstream answered"):
                assert fragment not in record.getMessage()


def test_a_connector_answering_normally_counts_as_a_connector_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC3 on the real path: a normal answer (is_error None) is success.

    Red on the metric until the session records it. Red again if success is
    decided by ``is_error is False``, which the real CLI never sends here.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    final, ran, _ = _run_turn(tmp_path, monkeypatch, tool="list_files")

    assert final.status is SessionStatus.DONE
    assert ran == ["list_files"]
    assert _points(reader) == {("connector", "success"): 1}
    assert _connector_warnings(caplog) == []


def test_a_call_the_approval_gate_held_counts_as_awaiting_approval_not_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC4: the gate's deny arrives is_error True, and it is not a connector failure.

    The gate is armed the way an operator arms it, on the live connector name,
    and wired the way ``__main__`` wires it. The turn pausing for approval and
    the connector never running hold today. Red on the metric until the session
    records awaiting_approval. Red again if the session reads is_error alone,
    which would count this held call as a connector error, log a WARNING, and
    page for a connector that was never called.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    gate = build_approval_gate(
        operator_tools=["mcp__acme__delete_files"],
        policy_routes={},
        mcp_servers=set(),
        connector_servers={"acme"},
    )
    assert gate is not None
    final, ran, _ = _run_turn(tmp_path, monkeypatch, tool="delete_files", gate=gate)

    # Measured on this CLI: the hook's deny arrives as a ToolResultBlock with
    # is_error True and the text "PreToolUse:mcp__acme__delete_files hook error:
    # ...", while the gate already holds pending_halt and pending_granted_tool
    # for that name; the turn then ends with a success ResultMessage whose
    # terminal_reason is hook_stopped, and the model is never sent the result.
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == "mcp__acme__delete_files"
    assert ran == []
    assert _points(reader) == {("connector", "awaiting_approval"): 1}
    assert _connector_warnings(caplog) == []


def test_a_connector_answering_a_json_rpc_error_counts_as_a_connector_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC3 on the real path: a connector's second error channel is is_error too.

    A dependency pin, green when written. The fixture's ``rpc_fail`` answers
    with a JSON-RPC ``error`` response (code -32000), not an ``isError`` result.
    Measured on claude-agent-sdk 0.2.159 with bundled CLI 2.1.281 (2026-09-29):
    the CLI delivers that as ``is_error=True`` carrying the error's message, so
    the runner counts it without reading any payload. Red if an SDK or CLI
    upgrade stops setting is_error for a JSON-RPC error, which would make a
    connector that fails this way invisible to the alert.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    final, ran, _ = _run_turn(tmp_path, monkeypatch, tool="rpc_fail")

    assert final.status is SessionStatus.DONE
    assert ran == ["rpc_fail"]
    assert _points(reader) == {("connector", "error"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [("acme", "rpc_fail")]
    for record in caplog.records:
        if record.levelno >= logging.WARNING:
            for fragment in (_ARGUMENT, "acme-ledger-PLACEHOLDER", "upstream answered"):
                assert fragment not in record.getMessage()


def test_an_operator_stop_during_a_connector_call_counts_as_cancelled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An interrupted call is cancelled: the operator stopped it, the connector did not fail.

    The turn is interrupted (``SessionRunner.interrupt``) once the connector has
    started ``slow_read``. The CLI answers the cut-off call with its own
    is_error result (measured, see the module docstring), which is no evidence
    about the connector at all. The turn ending idle and the connector never
    finishing the call hold today. Red until the manifest declares
    ``cancelled`` and the session records it for a result that arrives after an
    operator interrupt; red again if that result counts as a connector error or
    logs the connector WARNING, which would page on every stopped turn.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    final, ran, _ = _run_turn(tmp_path, monkeypatch, tool="slow_read", stop="interrupt")

    assert final.status is SessionStatus.IDLE_AWAITING_INPUT
    assert ran == ["slow_read"], "the connector finished a call the stop should have cut off"
    assert _points(reader) == {("connector", "cancelled"): 1}
    assert _connector_warnings(caplog) == []


def test_a_connector_call_cut_off_by_the_turn_deadline_is_a_connector_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A call still running at the turn's deadline is an error, not a cancellation.

    Same slow call, stopped by ``SessionRunner.timeout(epoch)`` instead of an
    operator. The CLI answers it with the same synthetic is_error result, but a
    connector that holds a call until the deadline is failing, so it counts as
    connector/error and logs the WARNING. Green today; red if the cancelled
    branch reads any interrupt as an operator stop, which would hide a connector
    that hangs.
    """

    caplog.set_level(logging.WARNING, logger="curie_runner")
    final, ran, _ = _run_turn(tmp_path, monkeypatch, tool="slow_read", stop="timeout")

    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    assert ran == ["slow_read"], "the connector finished a call the deadline should have cut off"
    assert _points(reader) == {("connector", "error"): 1}
    warnings = _connector_warnings(caplog)
    assert [(server, tool) for server, tool, _ in warnings] == [("acme", "slow_read")]
