"""Per-turn read-only tool access in the runner (RUNNER-TOOL-ACCESS-1..8).

A turn whose ``Event.tool_access`` is ``read-only`` may execute only tools the
runner classifies read-only. Everything here is offline: the decision module is
driven directly, the session through ``FakeModelSession`` (the sanctioned mock at
the ``ModelSession`` seam), and the HTTP surface through aiohttp's test client.
The real bundled CLI half is ``test_tool_access_real_cli.py``.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import (
    TOOL_ACCESS_STATUS_FIELD,
    ErrorEvent,
    Event,
    Final,
    SessionStatus,
    SideEffectFlag,
    ToolAccess,
    parse_ndjson,
)
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import HookMatcher, ToolUseBlock
from claude_agent_sdk.types import (
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from curie_runner import create_app
from curie_runner.__main__ import build_runner
from curie_runner.approval import (
    APPROVAL_TOOL_NAME,
    ApprovalGate,
    build_approval_gate,
)
from curie_runner.config import RunnerConfig
from curie_runner.fake import (
    FakeModelSession,
    _assistant,
    _result,
    _tool_result,
    approval_turn,
    default_turn,
)
from curie_runner.harness.claude.approval import build_can_use_tool
from curie_runner.otel import RunTracer
from curie_runner.sender_frame import frame_user_turn
from curie_runner.session import SessionRunner
from curie_runner.side_effects import CLAUDE_READONLY_TOOLS, SideEffectClassifier
from curie_runner.tool_access import (
    TurnToolAccess,
    front_can_use_tool,
    front_pre_tool_use_hooks,
)
from curie_telemetry import configure_meter_provider
from curie_telemetry import metrics as curie_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader


def _sent(text: str, user: str = "U0EXAMPLE1") -> str:
    return frame_user_turn("message", user, text, None)


_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_READ_ONLY_MCP = "mcp__acme__list_files"
_WRITE_MCP = "mcp__acme__delete_files"


def _access(
    readonly: frozenset[str] = CLAUDE_READONLY_TOOLS | {_READ_ONLY_MCP},
    **kwargs: Any,
) -> TurnToolAccess:
    return TurnToolAccess(readonly, **kwargs)


def _read_only(access: TurnToolAccess) -> TurnToolAccess:
    access.begin(ToolAccess.READ_ONLY)
    return access


def _event(text: str = "go", *, tool_access: ToolAccess | None = None) -> Event:
    return Event(type="message", text=text, user="U0EXAMPLE1", ts="1", tool_access=tool_access)


def _hook_input(tool: str | None) -> dict[str, Any]:
    return {"hook_event_name": "PreToolUse", "tool_name": tool, "tool_input": {}}


def _is_deny(output: dict[str, Any]) -> bool:
    specific = output.get("hookSpecificOutput", {})
    return specific.get("permissionDecision") == "deny"


# --- the decision itself -------------------------------------------------------


def test_null_access_refuses_nothing() -> None:
    # @spec RUNNER-TOOL-ACCESS-7
    access = _access()
    access.begin(None)
    for tool in ("Bash", "Write", _WRITE_MCP, APPROVAL_TOOL_NAME, "Read"):
        assert access.refusal(tool) is None


def test_read_only_allows_exactly_the_classified_tools() -> None:
    # @spec RUNNER-TOOL-ACCESS-1 RUNNER-TOOL-ACCESS-2
    access = _read_only(_access())

    for tool in ("Read", "Grep", "WebFetch", "ToolSearch", _READ_ONLY_MCP):
        assert access.refusal(tool) is None, tool
    for tool in (
        "Bash",
        "Write",
        "Edit",
        "TodoWrite",
        "Task",
        "Skill",
        _WRITE_MCP,
        "mcp__acme__never_declared",
        APPROVAL_TOOL_NAME,
        "mcp__curie__report_progress",
        "mcp__curie__publish_changes",
        "mcp__curie-state__set",
    ):
        reason = access.refusal(tool)
        assert reason is not None, tool
        assert "mcp__" not in reason
        assert "was not run" in reason
        assert "read-only" in reason


def test_a_read_only_tool_that_needs_approval_is_refused() -> None:
    # @spec RUNNER-TOOL-ACCESS-1 RUNNER-TOOL-ACCESS-3: the approval it would need
    # cannot be requested on this turn, so it is refused like a write.
    access = _read_only(_access(requires_approval=lambda tool: tool == "WebFetch"))

    assert access.refusal("WebFetch") is not None
    assert access.refusal("Read") is None


def test_a_call_whose_tool_name_cannot_be_read_is_refused() -> None:
    # @spec RUNNER-TOOL-ACCESS-2: fail closed, never abstain.
    access = _read_only(_access())

    assert access.refusal(None) is not None
    assert access.refusal("") is not None


def test_the_platform_idempotent_tools_are_not_read_only() -> None:
    # @spec RUNNER-TOOL-ACCESS-1: the classifier's idempotent set is wider than
    # its read-only set, and only the read-only one may run.
    classifier = SideEffectClassifier()
    assert classifier.is_side_effecting(APPROVAL_TOOL_NAME) is False
    access = _read_only(_access(CLAUDE_READONLY_TOOLS))
    assert access.refusal(APPROVAL_TOOL_NAME) is not None
    assert access.refusal("mcp__curie__report_progress") is not None


# --- the PreToolUse front --------------------------------------------------------


def _recording(calls: list[str], answer: dict[str, Any]) -> Any:
    async def callback(hook_input: Any, _tool_use_id: str | None, _ctx: Any) -> dict[str, Any]:
        calls.append(str(hook_input.get("tool_name")))
        return answer

    return callback


def _run(
    callback: Any, tool: str | None, tool_use_id: str | None = "toolu_acme01"
) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        result: dict[str, Any] = await callback(_hook_input(tool), tool_use_id, {"signal": None})
        return result

    return anyio.run(go)


def _fronted(access: TurnToolAccess) -> tuple[dict[str, list[HookMatcher]], list[str], list[str]]:
    bundle_calls: list[str] = []
    approval_calls: list[str] = []
    hooks = {
        "PreToolUse": [
            HookMatcher(
                matcher=None,
                hooks=[
                    _recording(
                        approval_calls,
                        {
                            "hookSpecificOutput": {
                                "hookEventName": "PreToolUse",
                                "permissionDecision": "allow",
                                "permissionDecisionReason": "granted",
                            }
                        },
                    )
                ],
            ),
            HookMatcher(matcher="Bash|mcp__acme__.*", hooks=[_recording(bundle_calls, {})]),
        ]
    }
    return front_pre_tool_use_hooks(hooks, access), approval_calls, bundle_calls


def test_the_front_decides_before_every_other_pretooluse_callback() -> None:
    # @spec RUNNER-TOOL-ACCESS-2: no approval record, no grant spent, no bundle
    # command run, for a call the turn may not make.
    access = _read_only(_access())
    fronted, approval_calls, bundle_calls = _fronted(access)

    matchers = fronted["PreToolUse"]
    assert matchers[0].matcher is None, "the front must see every call"
    assert [m.matcher for m in matchers[1:]] == [None, "Bash|mcp__acme__.*"]
    for matcher in matchers:
        for callback in matcher.hooks:
            for tool in ("Bash", _WRITE_MCP):
                output = _run(callback, tool)
                assert _is_deny(output), (matcher.matcher, tool, output)
                # A deny that lets the model answer: never the turn-stopping pair.
                assert output.get("continue_") is not False
    assert approval_calls == []
    assert bundle_calls == []


def test_the_front_lets_a_read_only_call_reach_the_other_callbacks() -> None:
    # @spec RUNNER-TOOL-ACCESS-2
    access = _read_only(_access())
    fronted, approval_calls, bundle_calls = _fronted(access)

    front, approval, bundle = (m.hooks[0] for m in fronted["PreToolUse"])
    assert _run(front, _READ_ONLY_MCP) == {}
    assert _run(approval, _READ_ONLY_MCP)["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert _run(bundle, _READ_ONLY_MCP) == {}
    assert approval_calls == [_READ_ONLY_MCP]
    assert bundle_calls == [_READ_ONLY_MCP]


def test_an_ordinary_turn_delegates_every_callback_unchanged() -> None:
    # @spec RUNNER-TOOL-ACCESS-7
    access = _access()
    access.begin(None)
    fronted, approval_calls, bundle_calls = _fronted(access)

    front, approval, bundle = (m.hooks[0] for m in fronted["PreToolUse"])
    assert _run(front, "Bash") == {}
    assert _run(approval, "Bash")["hookSpecificOutput"]["permissionDecision"] == "allow"
    assert _run(bundle, "Bash") == {}
    assert approval_calls == ["Bash"]
    assert bundle_calls == ["Bash"]


def test_a_session_with_no_hooks_still_gets_the_front() -> None:
    # @spec RUNNER-TOOL-ACCESS-2: a session with no gate and no bundle hook runs
    # under bypassPermissions, where only a hook can refuse a call.
    access = _read_only(_access())

    fronted = front_pre_tool_use_hooks(None, access)

    assert list(fronted) == ["PreToolUse"]
    [matcher] = fronted["PreToolUse"]
    assert matcher.matcher is None
    assert _is_deny(_run(matcher.hooks[0], "Bash"))
    assert _is_deny(_run(matcher.hooks[0], None))


def test_a_refused_call_is_remembered_by_its_call_id() -> None:
    # @spec RUNNER-TOOL-ACCESS-6
    access = _read_only(_access())
    [matcher] = front_pre_tool_use_hooks(None, access)["PreToolUse"]

    _run(matcher.hooks[0], "Bash", "toolu_acme01")
    _run(matcher.hooks[0], "Read", "toolu_acme02")

    assert access.refused_call_ids == {"toolu_acme01"}
    access.begin(ToolAccess.READ_ONLY)
    assert access.refused_call_ids == set(), "a new prompt starts a new refusal record"


# --- the permission-callback front ------------------------------------------------


def test_the_permission_front_denies_without_stopping_the_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-2
    access = _read_only(_access())
    seen: list[str] = []

    async def inner(
        tool: str, _input: dict[str, Any], _ctx: ToolPermissionContext
    ) -> PermissionResultAllow | PermissionResultDeny:
        seen.append(tool)
        return PermissionResultAllow()

    fronted = front_can_use_tool(inner, access)

    async def go() -> tuple[Any, Any]:
        context = ToolPermissionContext(tool_use_id="toolu_acme03")
        return (await fronted("Bash", {}, context), await fronted("Read", {}, context))

    denied, allowed = anyio.run(go)
    assert isinstance(denied, PermissionResultDeny)
    assert denied.interrupt is False
    assert "read-only" in denied.message
    assert isinstance(allowed, PermissionResultAllow)
    assert seen == ["Read"]
    assert access.refused_call_ids == {"toolu_acme03"}


def test_the_permission_front_without_an_inner_callback_allows_an_ordinary_call() -> None:
    # @spec RUNNER-TOOL-ACCESS-7
    access = _access()
    access.begin(None)
    fronted = front_can_use_tool(None, access)

    async def go() -> Any:
        return await fronted("Bash", {}, ToolPermissionContext(tool_use_id="toolu_acme04"))

    assert isinstance(anyio.run(go), PermissionResultAllow)


# --- turns on the fake model session ---------------------------------------------


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


def _tool_result_points(reader: InMemoryMetricReader) -> dict[tuple[str, str], float]:
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


def _fake_runner(
    script: Any,
    *,
    gate: ApprovalGate | None,
    access: TurnToolAccess | None,
) -> tuple[SessionRunner, FakeModelSession]:
    """Wired the way ``build_runner`` wires the fake tier."""

    inner = build_can_use_tool(gate) if gate is not None else None
    session = FakeModelSession(
        script,
        can_use_tool=front_can_use_tool(inner, access) if access is not None else inner,
        approval_gate=gate,
        tool_access=access,
    )
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        approval_gate=gate,
        tool_access=access,
    )
    return runner, session


def _drive(runner: SessionRunner, *events: Event) -> list[list[Any]]:
    async def go() -> list[list[Any]]:
        await runner.start()
        turns: list[list[Any]] = []
        try:
            for event in events:
                lines = [line async for line in runner.run_turn(event)]
                turns.append(list(parse_ndjson("".join(lines))))
        finally:
            await runner.close()
        return turns

    return anyio.run(go)


def _final(frames: list[Any]) -> Final:
    finals = [frame for frame in frames if isinstance(frame, Final)]
    assert len(finals) == 1, frames
    return finals[0]


def _no_approval(final: Final) -> None:
    assert final.status is not SessionStatus.AWAITING_APPROVAL
    assert final.approval_summary is None
    assert final.approval_route is None
    assert final.approval_gate_kind is None
    assert final.approval_granted_tool is None
    assert final.approval_granted_arguments is None
    assert final.approval_display is None


def test_a_read_only_turn_never_runs_a_gated_write_or_asks_for_approval(
    reader: InMemoryMetricReader,
) -> None:
    # @spec RUNNER-TOOL-ACCESS-2 RUNNER-TOOL-ACCESS-3 RUNNER-TOOL-ACCESS-6 RUNNER-TOOL-ACCESS-8
    gate = ApprovalGate(required=frozenset({"Bash"}))
    access = _access()
    runner, _ = _fake_runner(default_turn, gate=gate, access=access)

    [frames] = _drive(runner, _event(tool_access=ToolAccess.READ_ONLY))

    final = _final(frames)
    assert final.status is SessionStatus.DONE
    _no_approval(final)
    assert gate.pending_summary is None
    assert gate.pending_halt is False
    # The fake answered the refused call with an error result in place of the
    # scripted "hi", exactly as the real CLI does, so the call closes failed.
    closing = [f for f in frames if isinstance(f, SideEffectFlag) and f.failed is not None]
    assert [(f.tool, f.failed) for f in closing] == [("Bash", True)]
    assert _tool_result_points(reader) == {("builtin", "refused"): 1}


def test_the_same_gated_write_on_an_ordinary_turn_still_asks_for_approval() -> None:
    # @spec RUNNER-TOOL-ACCESS-7: the control for the test above.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    runner, _ = _fake_runner(default_turn, gate=gate, access=_access())

    [frames] = _drive(runner, _event())

    final = _final(frames)
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == "Bash"


def test_a_read_only_turn_ignores_the_models_approval_request() -> None:
    # @spec RUNNER-TOOL-ACCESS-3 RUNNER-TOOL-ACCESS-8
    gate = build_approval_gate(operator_tools=None, policy_routes={"mcp__acme__close_issue": "ops"})
    assert gate is not None
    runner, _ = _fake_runner(
        lambda: approval_turn("deploy the fix", route="ops"), gate=gate, access=_access()
    )

    [frames] = _drive(runner, _event(tool_access=ToolAccess.READ_ONLY))

    final = _final(frames)
    assert final.status is SessionStatus.DONE
    _no_approval(final)
    assert gate.policy_requested is False, "the request tool must not have executed"


def test_the_same_approval_request_on_an_ordinary_turn_is_a_card() -> None:
    # @spec RUNNER-TOOL-ACCESS-7: the control for the test above.
    gate = build_approval_gate(operator_tools=None, policy_routes={"mcp__acme__close_issue": "ops"})
    assert gate is not None
    runner, _ = _fake_runner(
        lambda: approval_turn("deploy the fix", route="ops"), gate=gate, access=_access()
    )

    [frames] = _drive(runner, _event())

    final = _final(frames)
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_gate_kind == "policy"


def test_a_read_only_tool_runs_on_a_read_only_turn(reader: InMemoryMetricReader) -> None:
    # @spec RUNNER-TOOL-ACCESS-1: the scripted result is delivered unchanged.
    def script() -> list[Any]:
        return [
            _assistant(ToolUseBlock(id="t1", name="Read", input={"file_path": "/tmp/x"})),
            _tool_result("t1", "contents"),
            _result(text="read it"),
        ]

    access = _access()
    runner, _ = _fake_runner(script, gate=None, access=access)

    [frames] = _drive(runner, _event(tool_access=ToolAccess.READ_ONLY))

    assert _final(frames).status is SessionStatus.DONE
    assert access.refused_call_ids == set()
    assert not [f for f in frames if isinstance(f, SideEffectFlag)]
    assert _tool_result_points(reader) == {("builtin", "success"): 1}


def test_a_session_without_enforcement_refuses_a_read_only_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-5: never run the turn unrestricted.
    runner, session = _fake_runner(default_turn, gate=None, access=None)

    [frames] = _drive(runner, _event(tool_access=ToolAccess.READ_ONLY))

    final = _final(frames)
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    errors = [f for f in frames if isinstance(f, ErrorEvent)]
    assert [e.classification for e in errors] == ["tool-access-unenforced"]
    assert session.queries == [], "the model must never have been asked"
    assert runner.enforced_tool_access == ()


def test_the_access_in_force_is_the_last_prompts() -> None:
    # @spec RUNNER-TOOL-ACCESS-4
    access = _access()
    runner, _ = _fake_runner(default_turn, gate=None, access=access)

    async def go() -> list[ToolAccess | None]:
        await runner.start()
        seen: list[ToolAccess | None] = []
        try:
            async for _ in runner.run_turn(_event(tool_access=ToolAccess.READ_ONLY)):
                pass
            seen.append(access.active)
            async for _ in runner.run_turn(_event("again")):
                pass
            seen.append(access.active)
        finally:
            await runner.close()
        return seen

    assert anyio.run(go) == [ToolAccess.READ_ONLY, None]


def test_a_read_only_turn_accepts_no_steer_and_joins_no_other_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-4
    access = _access()
    runner, session = _fake_runner(default_turn, gate=None, access=access)

    async def go() -> tuple[bool, bool, bool]:
        await runner.start()
        try:
            gen = runner.run_turn(_event("first", tool_access=ToolAccess.READ_ONLY))
            await gen.__anext__()
            ordinary = await runner.steer("ordinary follow-up")
            restricted = await runner.steer("restricted", tool_access=ToolAccess.READ_ONLY)
            async for _ in gen:
                pass
            gen = runner.run_turn(_event("second"))
            await gen.__anext__()
            into_ordinary = await runner.steer("restricted", tool_access=ToolAccess.READ_ONLY)
            async for _ in gen:
                pass
        finally:
            await runner.close()
        return ordinary, restricted, into_ordinary

    assert anyio.run(go) == (False, False, False)
    assert session.queries == [_sent("first"), _sent("second")]


def _refused_before_the_model(
    frames: list[Any],
    session: FakeModelSession,
    classification: str = "tool-access-unenforced",
) -> None:
    final = _final(frames)
    assert final.status is SessionStatus.CLASSIFIED_FAILURE
    errors = [f for f in frames if isinstance(f, ErrorEvent)]
    assert [e.classification for e in errors] == [classification]
    assert "read-only" not in session.queries


def test_a_session_that_accepted_a_steer_refuses_a_read_only_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-4: the steered prompt may still be pending in
    # the CLI, and would run under whatever access the next prompt set.
    runner, session = _fake_runner(default_turn, gate=None, access=_access())

    async def go() -> list[Any]:
        await runner.start()
        try:
            gen = runner.run_turn(_event("first"))
            await gen.__anext__()
            assert await runner.steer("ordinary follow-up") is True
            async for _ in gen:
                pass
            lines = [
                line
                async for line in runner.run_turn(
                    _event("read-only", tool_access=ToolAccess.READ_ONLY)
                )
            ]
        finally:
            await runner.close()
        return list(parse_ndjson("".join(lines)))

    frames = anyio.run(go)
    _refused_before_the_model(frames, session)
    assert session.queries == [_sent("first"), _sent("ordinary follow-up", "")]


def test_a_session_that_ran_an_ordinary_turn_refuses_a_read_only_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-4 RUNNER-TOOL-ACCESS-5: an ordinary turn can
    # leave work the CLI answers as its own turn later (a background task's
    # notification), with no steer at all.
    runner, session = _fake_runner(default_turn, gate=None, access=_access())

    first, second = _drive(
        runner, _event("first"), _event("read-only", tool_access=ToolAccess.READ_ONLY)
    )

    assert _final(first).status is SessionStatus.DONE
    _refused_before_the_model(second, session)
    assert session.queries == [_sent("first")]
    assert runner.enforced_tool_access == ()


def test_a_session_that_ran_only_read_only_turns_runs_another() -> None:
    # @spec RUNNER-TOOL-ACCESS-4: the control; a read-only turn can leave no
    # such work, so a retry on the same session still runs.
    runner, session = _fake_runner(default_turn, gate=None, access=_access())

    first, second = _drive(
        runner,
        _event("probe", tool_access=ToolAccess.READ_ONLY),
        _event("probe again", tool_access=ToolAccess.READ_ONLY),
    )

    assert _final(first).status is SessionStatus.DONE
    assert _final(second).status is SessionStatus.DONE
    assert session.queries == [_sent("probe"), _sent("probe again")]
    assert runner.enforced_tool_access == ("read-only",)


def test_a_reset_session_runs_a_read_only_turn_again() -> None:
    # @spec RUNNER-TOOL-ACCESS-4: a new SDK session carries nothing over.
    runner, session = _fake_runner(default_turn, gate=None, access=_access())

    async def go() -> list[Any]:
        await runner.start()
        try:
            async for _ in runner.run_turn(_event("first")):
                pass
            assert runner.enforced_tool_access == ()
            await runner.reset()
            lines = [
                line
                async for line in runner.run_turn(_event("probe", tool_access=ToolAccess.READ_ONLY))
            ]
        finally:
            await runner.close()
        return list(parse_ndjson("".join(lines)))

    assert _final(anyio.run(go)).status is SessionStatus.DONE
    assert runner.enforced_tool_access == ("read-only",)


@pytest.mark.parametrize("text", ["/acme-bot:probe", "  /acme-bot:probe now", "\n/compact"])
def test_a_read_only_slash_command_is_refused_before_the_model(text: str) -> None:
    # @spec RUNNER-TOOL-ACCESS-9
    runner, session = _fake_runner(default_turn, gate=None, access=_access())

    [frames] = _drive(runner, _event(text, tool_access=ToolAccess.READ_ONLY))

    _refused_before_the_model(frames, session, "tool-access-refused")
    assert session.queries == []


def test_an_ordinary_slash_command_is_still_sent() -> None:
    # @spec RUNNER-TOOL-ACCESS-7 RUNNER-TOOL-ACCESS-9: the refusal is read-only only.
    runner, session = _fake_runner(default_turn, gate=None, access=_access())

    [frames] = _drive(runner, _event("/acme-bot:probe"))

    assert _final(frames).status is SessionStatus.DONE
    assert session.queries == [_sent("/acme-bot:probe")]


def test_a_read_only_turn_leaves_the_boot_grant_for_the_next_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-10: the resumed turn after a read-only one still
    # gets to run the action a human approved, once.
    gate = ApprovalGate(required=frozenset({"Bash"}), grant_tool="Bash")
    access = _access(requires_approval=gate.requires_approval)
    runner, _ = _fake_runner(default_turn, gate=gate, access=access)

    read_only, resumed = _drive(
        runner, _event("probe", tool_access=ToolAccess.READ_ONLY), _event("resume")
    )

    assert _final(read_only).status is SessionStatus.DONE
    final = _final(resumed)
    assert final.status is SessionStatus.DONE, "the approved call was re-gated"
    closing = [f for f in resumed if isinstance(f, SideEffectFlag) and f.failed is not None]
    assert [(f.tool, f.failed) for f in closing] == [("Bash", False)]


def test_a_decision_that_fails_denies(monkeypatch: pytest.MonkeyPatch) -> None:
    # @spec RUNNER-TOOL-ACCESS-2: a raising hook is reported and the call
    # PROCEEDS on the CLI, so the front must never raise.
    access = _read_only(_access())

    def broken(_tool: str | None) -> str | None:
        raise RuntimeError("classifier exploded")

    monkeypatch.setattr(access, "refusal", broken)
    fronted, approval_calls, bundle_calls = _fronted(access)
    for matcher in fronted["PreToolUse"]:
        assert _is_deny(_run(matcher.hooks[0], "Read"))
    assert approval_calls == []
    assert bundle_calls == []
    # @spec RUNNER-TOOL-ACCESS-6: recorded, so it counts refused, not error.
    assert access.refused_call_ids == {"toolu_acme01"}

    async def go() -> Any:
        return await front_can_use_tool(None, access)(
            "Read", {}, ToolPermissionContext(tool_use_id="toolu_acme05")
        )

    assert isinstance(anyio.run(go), PermissionResultDeny)


def test_a_read_only_tool_an_operator_gated_is_refused_on_the_booted_runner(
    tmp_path: Path, reader: InMemoryMetricReader
) -> None:
    # @spec RUNNER-TOOL-ACCESS-1 RUNNER-TOOL-ACCESS-3: Read is read-only, but a
    # gate names it, and the approval it needs cannot be asked for.
    runner = build_runner(_config(tmp_path, CURIE_APPROVAL_REQUIRED_TOOLS="Read"), fake_model=True)
    script = [
        _assistant(ToolUseBlock(id="t1", name="Read", input={"file_path": "/tmp/x"})),
        _tool_result("t1", "contents"),
        _result(text="read it"),
    ]
    runner._factory = _scripted(runner._factory, script)  # type: ignore[method-assign]

    [frames] = _drive(runner, _event(tool_access=ToolAccess.READ_ONLY))

    final = _final(frames)
    assert final.status is SessionStatus.DONE
    _no_approval(final)
    assert _tool_result_points(reader) == {("builtin", "refused"): 1}


def _scripted(factory: Any, script: list[Any]) -> Any:
    """The booted fake session with its script swapped, and nothing else."""

    def build() -> Any:
        session = factory()
        session._script_factory = lambda: list(script)
        return session

    return build


def test_translation_captures_no_request_on_a_read_only_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-3: the wire-level capture, on its own.
    from curie_runner.translate import TurnState, translate_message
    from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

    message = _assistant(
        ToolUseBlock(id="t1", name=APPROVAL_TOOL_NAME, input={"summary": "deploy"}),
        ToolUseBlock(id="t2", name=PLATFORM_PUBLISH_TOOL_NAME, input={"title": "t"}),
    )
    restricted = TurnState(tool_access=ToolAccess.READ_ONLY)
    ordinary = TurnState()

    translate_message(message, restricted, SideEffectClassifier(), None)
    translate_message(message, ordinary, SideEffectClassifier(), None)

    assert restricted.approval_summary is None
    assert restricted.publication_calls == []
    assert ordinary.approval_summary == "deploy"
    assert [call_id for call_id, _ in ordinary.publication_calls] == ["t2"]


def test_the_final_never_flips_to_awaiting_approval_on_a_read_only_turn() -> None:
    # @spec RUNNER-TOOL-ACCESS-3: the terminal guard, on its own.
    from curie_runner.session import _apply_approval_override
    from curie_runner.translate import TurnState

    done = Final(text="done", status=SessionStatus.DONE)
    restricted = TurnState(tool_access=ToolAccess.READ_ONLY, approval_summary="deploy")
    ordinary = TurnState(approval_summary="deploy")

    assert _apply_approval_override(done, restricted) == done
    assert _apply_approval_override(done, ordinary).status is SessionStatus.AWAITING_APPROVAL


# --- the HTTP surface and the boot wiring -----------------------------------------


def _config(tmp_path: Path, **extra: str) -> RunnerConfig:
    plugin_dir = tmp_path / "bundle"
    (plugin_dir / ".claude-plugin").mkdir(parents=True)
    (plugin_dir / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-bot"}), encoding="utf-8"
    )
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": str(plugin_dir),
            "CURIE_SESSION_ID": "session-acme-read-only",
            "CURIE_SANDBOX_ID": "sandbox-acme-read-only",
            "CURIE_BUDGET": _BUDGET,
            **extra,
        }
    )


def test_the_booted_runner_enforces_and_advertises_read_only(tmp_path: Path) -> None:
    # @spec RUNNER-TOOL-ACCESS-5
    runner = build_runner(_config(tmp_path), fake_model=True)
    token = "acme-runner-token-PLACEHOLDER"

    async def go() -> tuple[dict[str, Any], dict[str, Any], list[Any]]:
        await runner.start()
        try:
            async with TestClient(TestServer(create_app(runner, token=token))) as client:
                probe = await (await client.get("/status")).json()
                control = await (
                    await client.get("/v1/status", headers={"Authorization": f"Bearer {token}"})
                ).json()
                response = await client.post(
                    "/v1/event",
                    json={**_event(tool_access=ToolAccess.READ_ONLY).model_dump(mode="json")},
                    headers={"Authorization": f"Bearer {token}"},
                )
                frames = list(parse_ndjson(await response.text()))
        finally:
            await runner.close()
        return probe, control, frames

    probe, control, frames = anyio.run(go)
    assert probe[TOOL_ACCESS_STATUS_FIELD] == ["read-only"]
    assert control[TOOL_ACCESS_STATUS_FIELD] == ["read-only"]
    # The fake tier's default turn calls Bash; on the booted runner it is refused.
    final = _final(frames)
    assert final.status is SessionStatus.DONE
    closing = [f for f in frames if isinstance(f, SideEffectFlag) and f.failed is not None]
    assert [(f.tool, f.failed) for f in closing] == [("Bash", True)]


def test_a_runner_without_enforcement_advertises_none() -> None:
    # @spec RUNNER-TOOL-ACCESS-5
    runner, _ = _fake_runner(default_turn, gate=None, access=None)

    async def go() -> dict[str, Any]:
        await runner.start()
        try:
            async with TestClient(TestServer(create_app(runner))) as client:
                return dict(await (await client.get("/status")).json())
        finally:
            await runner.close()

    assert anyio.run(go)[TOOL_ACCESS_STATUS_FIELD] == []


def test_the_steer_route_refuses_a_different_access_with_409() -> None:
    # @spec RUNNER-TOOL-ACCESS-4
    release = anyio.Event()

    class _Held(FakeModelSession):
        async def receive_turn(self):  # type: ignore[override]
            await release.wait()
            async for message in super().receive_turn():
                yield message

    access = _access()
    session = _Held(default_turn, tool_access=access)
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        tool_access=access,
    )

    async def go() -> tuple[int, int]:
        await runner.start()
        try:
            async with TestClient(TestServer(create_app(runner))) as client:
                async with anyio.create_task_group() as tasks:

                    async def open_turn() -> None:
                        response = await client.post(
                            "/v1/event", json=_event("first").model_dump(mode="json")
                        )
                        await response.text()

                    tasks.start_soon(open_turn)
                    # Steerable once the prompt is sent, not merely accepted.
                    while not session.queries:
                        await anyio.sleep(0.01)
                    mismatched = await client.post(
                        "/v1/steer",
                        json=_event("restricted", tool_access=ToolAccess.READ_ONLY).model_dump(
                            mode="json"
                        ),
                    )
                    matched = await client.post(
                        "/v1/steer", json=_event("ordinary").model_dump(mode="json")
                    )
                    release.set()
                return mismatched.status, matched.status
        finally:
            await runner.close()

    assert anyio.run(go) == (409, 200)


def _counting_runner(access: TurnToolAccess) -> tuple[SessionRunner, list[FakeModelSession]]:
    """A runner whose factory builds a new fake session per call, all recorded."""

    sessions: list[FakeModelSession] = []

    def factory() -> FakeModelSession:
        session = FakeModelSession(
            default_turn, can_use_tool=front_can_use_tool(None, access), tool_access=access
        )
        sessions.append(session)
        return session

    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=factory,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        tool_access=access,
    )
    return runner, sessions


def test_an_ordinary_turn_after_a_read_only_one_runs_on_a_fresh_session() -> None:
    # @spec RUNNER-TOOL-ACCESS-11: the SDK session that carried the read-only
    # prompt is closed before the unrestricted prompt is sent.
    access = _access()
    runner, sessions = _counting_runner(access)

    probe, ordinary = _drive(
        runner, _event("probe", tool_access=ToolAccess.READ_ONLY), _event("hello")
    )

    assert _final(probe).status is SessionStatus.DONE
    assert _final(ordinary).status is SessionStatus.DONE
    assert [s.queries for s in sessions] == [[_sent("probe")], [_sent("hello")]]
    assert sessions[0].connected is False, "the read-only session was left running"


def test_turns_under_one_access_keep_their_session() -> None:
    # @spec RUNNER-TOOL-ACCESS-11 RUNNER-TOOL-ACCESS-7: the controls; no
    # replacement between two ordinary turns or two read-only turns.
    access = _access()
    ordinary_runner, ordinary_sessions = _counting_runner(access)
    _drive(ordinary_runner, _event("one"), _event("two"))
    assert [s.queries for s in ordinary_sessions] == [[_sent("one"), _sent("two")]]

    restricted_runner, restricted_sessions = _counting_runner(_access())
    _drive(
        restricted_runner,
        _event("probe", tool_access=ToolAccess.READ_ONLY),
        _event("probe again", tool_access=ToolAccess.READ_ONLY),
    )
    assert [s.queries for s in restricted_sessions] == [[_sent("probe"), _sent("probe again")]]


def test_a_refused_progress_demo_call_gets_only_its_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # @spec RUNNER-TOOL-ACCESS-8: the fake answers a denied call with its error
    # result in place of the scripted one. The progress demo's calls are
    # answered by the handler, not a scripted result, so a refused call must
    # skip the handler too or it gets a second, successful result.
    from claude_agent_sdk import ToolResultBlock, UserMessage
    from curie_runner.fake import progress_demo_turn

    monkeypatch.setattr("curie_runner.fake.PROGRESS_DEMO_PAUSE_S", 0.0)
    access = _read_only(_access())
    session = FakeModelSession(
        progress_demo_turn, can_use_tool=front_can_use_tool(None, access), tool_access=access
    )

    async def go() -> list[Any]:
        await session.connect()
        await session.query("[fake:progress-demo] go")
        return [message async for message in session.receive_turn()]

    messages = anyio.run(go)
    results: dict[str, list[ToolResultBlock]] = {}
    for message in messages:
        if isinstance(message, UserMessage) and not isinstance(message.content, str):
            for block in message.content:
                if isinstance(block, ToolResultBlock):
                    results.setdefault(block.tool_use_id, []).append(block)

    assert set(results) == {"p1", "p2", "p3"}
    for tool_use_id, blocks in results.items():
        assert len(blocks) == 1, (tool_use_id, blocks)
        assert blocks[0].is_error, (tool_use_id, blocks)


def test_read_only_feedback_uses_plain_action_and_keeps_exact_denied_id() -> None:
    access = _read_only(_access())
    reason = access.refuse("mcp__acme__delete_files", "call-acme-1")
    assert reason is not None
    assert "delete files" in reason
    assert "mcp__" not in reason
    assert "call-acme-1" in access.refused_call_ids
    assert access.refusal(_READ_ONLY_MCP) is None
