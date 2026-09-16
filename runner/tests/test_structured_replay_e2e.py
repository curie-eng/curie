"""Offline cross-runner proofs for structured replay and approval authority."""

from __future__ import annotations

from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson_line
from claude_agent_sdk import AssistantMessage, ResultMessage, ToolUseBlock
from claude_agent_sdk.types import (
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from curie_runner.approval import ApprovalGate, build_can_use_tool
from curie_runner.fake import FakeModelSession
from curie_runner.history import (
    ConversationMessage,
    TranscriptStore,
    TurnRecord,
    build_conversation_replay,
    close_suspended_tool_calls,
)
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier


class _Store:
    def __init__(self) -> None:
        self.records: list[TurnRecord] = []

    async def load(self) -> list[TurnRecord]:
        return list(self.records)

    async def append(self, record: TurnRecord) -> None:
        self.records.append(record)


def _tool_turn(*commands: str) -> list[Any]:
    messages: list[Any] = [
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id=f"tool-{index}",
                    name="Bash",
                    input={"command": command},
                )
            ],
            model="fake",
        )
        for index, command in enumerate(commands, start=1)
    ]
    messages.append(
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="fake-session",
            result="done",
        )
    )
    return messages


def _runner(
    session: FakeModelSession,
    gate: ApprovalGate,
    *,
    store: TranscriptStore | None = None,
    decision: str | None = None,
) -> SessionRunner:
    return SessionRunner(
        session_factory=lambda: session,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="structured-replay-e2e",
        session_id="curie-thread-e2e",
        history_store=store,
        approval_gate=gate,
        approval_decision=decision,
    )


async def _turn(runner: SessionRunner, text: str) -> Final:
    await runner.start()
    final: Final | None = None
    async for line in runner.run_inbound(
        Event(type="message", text=text, user="U0EXAMPLE", ts="1")
    ):
        event = parse_ndjson_line(line)
        if isinstance(event, Final):
            final = event
    await runner.close()
    assert final is not None
    return final


def test_suspended_permission_call_gets_a_structurally_valid_tool_result() -> None:
    messages = (
        ConversationMessage(role="user", content="deploy it"),
        ConversationMessage(
            role="assistant",
            content=[
                {
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Bash",
                    "input": {"command": "deploy --release"},
                }
            ],
        ),
    )

    closed = close_suspended_tool_calls(messages)

    assert closed[:2] == messages
    assert closed[-1] == ConversationMessage(
        role="user",
        content=[
            {
                "type": "tool_result",
                "tool_use_id": "tool-1",
                "content": "Tool call was not executed; awaiting human approval.",
                "is_error": True,
            }
        ],
    )
    assert close_suspended_tool_calls(closed) == closed


def test_cross_runner_approval_executes_once_then_rearms() -> None:
    store = _Store()
    blocked_gate = ApprovalGate(required=frozenset({"Bash"}))
    blocked_session = FakeModelSession(
        lambda: _tool_turn("deploy --release"),
        can_use_tool=build_can_use_tool(blocked_gate),
    )

    first = anyio.run(
        _turn,
        _runner(blocked_session, blocked_gate, store=store),
        "deploy release",
    )
    assert first.status is SessionStatus.AWAITING_APPROVAL
    assert len(store.records) == 1
    suspended = store.records[0]
    assert suspended.status == SessionStatus.AWAITING_APPROVAL.value
    assert suspended.approval is not None
    assert suspended.approval.gate_kind == "permission"
    assert suspended.approval.granted_tool == "Bash"

    replay, summary = build_conversation_replay(store.records)
    assert summary is None
    assert replay.messages[-1].role == "user"
    assert replay.messages[-1].content[0]["type"] == "tool_result"

    executions: list[str] = []
    resumed_gate = ApprovalGate(required=frozenset({"Bash"}), grant_tool="Bash")
    inner = build_can_use_tool(resumed_gate)

    async def execute_if_allowed(
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        decision = await inner(tool_name, tool_input, context)
        if isinstance(decision, PermissionResultAllow):
            executions.append(str(tool_input["command"]))
        return decision

    resumed_session = FakeModelSession(
        lambda: _tool_turn("deploy --release", "deploy --release"),
        can_use_tool=execute_if_allowed,
        replay_messages=replay.messages,
    )
    second = anyio.run(
        _turn,
        _runner(resumed_session, resumed_gate, decision="approved"),
        "approval granted; continue",
    )

    assert resumed_session.replay_messages == replay.messages
    assert executions == ["deploy --release"]
    assert second.status is SessionStatus.AWAITING_APPROVAL
    assert second.approval_summary is not None


@pytest.mark.parametrize("decision", ["rejected", "expired"])
def test_cross_runner_non_approved_decision_has_zero_authority(decision: str) -> None:
    replay_messages = (
        ConversationMessage(role="user", content="deploy release"),
        ConversationMessage(
            role="assistant",
            content=[
                {
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Bash",
                    "input": {"command": "deploy --release"},
                }
            ],
        ),
    )
    executions: list[str] = []
    gate = ApprovalGate(required=frozenset({"Bash"}), grant_tool=None)
    inner = build_can_use_tool(gate)

    async def execute_if_allowed(
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        result = await inner(tool_name, tool_input, context)
        if isinstance(result, PermissionResultAllow):
            executions.append(str(tool_input["command"]))
        return result

    session = FakeModelSession(
        lambda: _tool_turn("deploy --release"),
        can_use_tool=execute_if_allowed,
        replay_messages=close_suspended_tool_calls(replay_messages),
    )
    final = anyio.run(
        _turn,
        _runner(session, gate, decision=decision),
        f"approval {decision}",
    )

    assert executions == []
    assert final.status is SessionStatus.AWAITING_APPROVAL
