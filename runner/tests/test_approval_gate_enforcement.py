"""The approval gate is ENFORCED, not merely armed (#1852).

PR #1875 landed only the boot-time refusal (``assert_gates_not_shadowed``, tested
in ``test_gate_shadowing.py``). That closes the case where the bundle itself
preauthorizes a gated tool, but it cannot close the two halves this file covers:

1. **The gate has to run at a layer permission rules cannot skip.** The SDK
   documents on ``ClaudeAgentOptions.can_use_tool`` (see
   ``.venv/lib/python3.14/site-packages/claude_agent_sdk/types.py``, the
   ``can_use_tool`` field docstring) that the callback is "*not* invoked for tool
   calls already permitted by ``allowed_tools`` ... or ``permissions.allow`` rules
   in settings", and that "To observe or gate *every* tool call regardless of
   permission rules, use a ``PreToolUse`` hook via ``hooks`` instead -- but note
   that a ``PreToolUse`` hook returning an *allow* decision also skips this
   callback." So the gate's DECIDING layer must be the PreToolUse hook.

   Observed live (2026-08-29, claude-agent-sdk 0.2.135 + OpenRouter
   anthropic/claude-sonnet-4.5) with
   ``ClaudeAgentOptions(allowed_tools=["Bash"], permission_mode="default",
   can_use_tool=<cb>)``:
     - no PreToolUse hook              -> ``can_use_tool`` NEVER invoked, Bash EXECUTED;
     - hook returns deny + continue False -> hook invoked, ``can_use_tool`` never
       invoked, Bash NOT executed, and the turn ended promptly with a terminal
       ``ResultMessage``;
     - hook returns allow              -> hook invoked, ``can_use_tool`` NOT invoked,
       Bash executed.
   That third line is why the hook -- not ``can_use_tool`` -- has to be what spends
   the one-shot post-approval grant: a hook allow means the callback never runs.

2. **Stopping the turn must still produce an approval record.** A hook deny that
   stops the turn, and a ``PermissionResultDeny(interrupt=True)``, both end the
   turn in a shape the session previously read as a plain failure, so the run
   ended ``classified-failure`` with no ``approval_summary`` and nothing to
   approve -- the "denied live tool call left the turn hanging" half of the issue.
   ``_apply_approval_override`` only flipped a DONE final, so a runner-requested
   halt has to be carried on its own flag.

Everything here runs offline: no network, no credential, no mocked
Postgres/Valkey/Langfuse. The model session is replaced at the ``ModelSession``
adapter seam (``FakeModelSession``), which is the repo's sanctioned mock point.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import sys
import typing
from pathlib import Path
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aci_protocol import Event, SessionStatus, parse_ndjson
from claude_agent_sdk import (
    AssistantMessage,
    HookMatcher,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)
from claude_agent_sdk.types import (
    PermissionResultAllow,
    PermissionResultDeny,
    PreToolUseHookSpecificOutput,
    SyncHookJSONOutput,
    ToolPermissionContext,
)
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.approval import (
    _DENY_MESSAGE,
    APPROVAL_SERVER_NAME,
    APPROVAL_SUMMARY_PREFIX,
    APPROVAL_TOOL_NAME,
    PUBLISH_TOOL_NAME,
    ApprovalGate,
    ApprovalPolicyError,
    build_approval_gate,
    build_approval_hook,
    build_can_use_tool,
)
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_runner.translate import TurnState, translate_message

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'
_CAPABILITY_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_capability_server.py"


# --- fixtures: a realistic plugin bundle, not a stub ----------------------------


def _bundle(
    root: Path,
    *,
    gates: list[str] | None = None,
    skill_allowed_tools: list[str] | None = None,
    manifest_hooks: dict[str, Any] | None = None,
) -> str:
    """Write a real bundle dir: manifest + (optionally) a skill with frontmatter.

    Mirrors the ``_bundle`` idiom in ``test_gate_shadowing.py`` but adds the
    manifest ``hooks`` field, because the merge under test (#1852) is between the
    bundle's own PreToolUse matchers and the approval matcher.
    """

    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "name": "approval-demo",
        "version": "0.1.0",
        "description": "A bundle for the gate-enforcement tests.",
    }
    if gates:
        manifest["approvalPolicy"] = {"gates": [{"gate": g, "route": "ops"} for g in gates]}
    if manifest_hooks is not None:
        manifest["hooks"] = manifest_hooks
    (root / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    if skill_allowed_tools is not None:
        skill_dir = root / "skills" / "approval-demo"
        skill_dir.mkdir(parents=True, exist_ok=True)
        lines = [
            "---",
            "name: approval-demo",
            "description: Demonstrates the approval gate.",
            "allowed-tools:",
            *(f"  - {entry}" for entry in skill_allowed_tools),
            "---",
            "",
            "# approval-demo",
            "",
        ]
        (skill_dir / "SKILL.md").write_text("\n".join(lines), encoding="utf-8")
    return str(root)


def _config(plugin_dir: str, **extra: str) -> RunnerConfig:
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": plugin_dir,
            "CURIE_SESSION_ID": "s-1852",
            "CURIE_SANDBOX_ID": "b-1852",
            "CURIE_BUDGET": _BUDGET,
            **extra,
        }
    )


def _hook_callback(gate: ApprovalGate):
    """The single PreToolUse callback ``build_approval_hook`` contributes."""

    hooks = build_approval_hook(gate)
    assert set(hooks) == {"PreToolUse"}, "the approval hook registers PreToolUse only"
    matchers = hooks["PreToolUse"]
    assert len(matchers) == 1
    matcher = matchers[0]
    assert isinstance(matcher, HookMatcher)
    # matcher=None means "every tool call". A tool-name matcher string would let
    # the CLI's own matching decide which calls reach us, and the gate's whole
    # claim is that it sees every one.
    assert matcher.matcher is None
    assert len(matcher.hooks) == 1
    return matcher.hooks[0]


def _run_hook(gate: ApprovalGate, tool: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    callback = _hook_callback(gate)

    async def go() -> dict[str, Any]:
        return await callback(
            {"tool_name": tool, "tool_input": tool_input}, "tuid-1", {"signal": None}
        )

    return anyio.run(go)


def _decision(output: dict[str, Any]) -> dict[str, Any]:
    assert "hookSpecificOutput" in output, f"no hook decision in {output!r}"
    return output["hookSpecificOutput"]


def _runner_over(script: list[Any], *, gate: ApprovalGate, ceiling: int = 10_000, **kw):
    session = FakeModelSession(lambda: script, **kw)
    runner = SessionRunner(
        session_factory=lambda: session,
        ceiling=ceiling,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
        approval_gate=gate,
    )
    return runner, session


def _recording_deny(gate: ApprovalGate, *, interrupt: bool):
    """A permission callback that records the block exactly as the hook does.

    The fake tier deliberately does not run PreToolUse hooks (they shell out and
    would break its offline no-op guarantee -- see ``FakeModelSession``), so a
    session-level test drives the same state transition through the permission
    callback seam the fake DOES honor. ``interrupt`` is a parameter because the
    two turn-ending shapes under test differ: ``True`` is the SDK aborting the
    turn immediately, ``False`` lets the scripted terminal result be delivered so
    the error-result shape itself can be asserted on.
    """

    async def callback(
        tool_name: str, tool_input: dict[str, Any], _ctx: ToolPermissionContext
    ) -> PermissionResultAllow | PermissionResultDeny:
        if tool_name in gate.required:
            gate.block(tool_name, tool_input)
            return PermissionResultDeny(message=_DENY_MESSAGE, interrupt=interrupt)
        return PermissionResultAllow()

    return callback


def _gated_bash_then_error_result() -> list[Any]:
    """A gated Bash call followed by an interrupt-shaped terminal ERROR result.

    This is the shape a CLI-side stop produces: the model's tool call is emitted,
    the call never executes, and the turn terminates on an error-subtype
    ``ResultMessage`` rather than a clean success.
    """

    return [
        AssistantMessage(content=[TextBlock(text="I'll run that")], model="m"),
        AssistantMessage(
            content=[ToolUseBlock(id="t1", name="Bash", input={"command": "rm -rf /tmp/x"})],
            model="m",
        ),
        ResultMessage(
            subtype="error_during_execution",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=1,
            session_id="s",
            result="aborted",
        ),
    ]


# --- A. the exact issue repro, through the real boot path (AC1/AC2) -------------


def test_boot_refuses_the_issue_repro_bundle(tmp_path: Path) -> None:
    # revert: drop the assert_gates_not_shadowed call from build_runner -> this fails.
    # The verbatim #1852 repro: a manifest that gates Bash shipped alongside a
    # skill whose frontmatter declares `allowed-tools: [Bash]`. Driven through
    # build_runner (not the validator in isolation) because boot is the only
    # scope that holds both the assembled gate and the bundle directory.
    plugin_dir = _bundle(tmp_path, gates=["Bash"], skill_allowed_tools=["Bash"])

    with pytest.raises(ApprovalPolicyError) as exc:
        build_runner(_config(plugin_dir), fake_model=True)

    message = str(exc.value)
    assert "skills/approval-demo/SKILL.md" in message  # the file to edit
    assert "'Bash'" in message  # the entry, and the gated tool
    assert "before the approval callback runs" in message  # why it is fatal


def test_boot_accepts_the_same_bundle_once_the_entry_is_removed(tmp_path: Path) -> None:
    # revert/negative control: if assert_gates_not_shadowed over-matched, a
    # legitimately gated bundle would refuse to boot and operators would remove
    # the gate to get running again.
    plugin_dir = _bundle(tmp_path, gates=["Bash"], skill_allowed_tools=["Read"])
    runner = build_runner(_config(plugin_dir), fake_model=True)
    assert runner is not None


# --- B. an operator gate armed AFTER deployment (AC6) ---------------------------


def test_hook_denies_a_gate_the_operator_armed_after_the_bundle_shipped() -> None:
    # revert: hook returns {} (or an "ask") for a gated tool -> this fails, and the
    # tool runs unapproved.
    # The bundle ships no approvalPolicy and no allowed-tools entry, so nothing at
    # deploy time has anything to check; the gate exists only because an operator
    # set CURIE_APPROVAL_REQUIRED_TOOLS=Bash afterwards. The hook must still deny.
    gate = ApprovalGate(required=frozenset({"Bash"}))

    output = _run_hook(gate, "Bash", {"command": "rm -rf /tmp/x"})

    decision = _decision(output)
    assert decision["hookEventName"] == "PreToolUse"
    assert decision["permissionDecision"] == "deny"
    assert decision["permissionDecisionReason"] == _DENY_MESSAGE

    # The turn-stopping half. `continue_` is the Python-side spelling; the SDK
    # rewrites it to the wire's "continue" in
    # claude_agent_sdk/_internal/query.py::_convert_hook_output_for_cli, so a
    # callback that emitted the wire name would be silently ignored and the turn
    # would keep running after the deny.
    assert output["continue_"] is False
    assert "continue" not in output, "emit continue_, not the wire name; the SDK converts"
    assert isinstance(output.get("stopReason"), str) and output["stopReason"]

    # And the approval record the platform suspends on.
    assert gate.pending_summary is not None
    assert gate.pending_summary.startswith(APPROVAL_SUMMARY_PREFIX)
    assert "rm -rf /tmp/x" in gate.pending_summary
    assert gate.pending_gate_kind == "permission"
    assert gate.pending_granted_tool == "Bash"
    # The runner asked for the stop, so an interrupt-shaped turn end is still an
    # approval pause rather than a failure (see test F).
    assert gate.pending_halt is True


def test_reset_clears_the_halt_marker() -> None:
    # revert: reset() leaves pending_halt set -> the NEXT turn's unrelated failure
    # is reported as awaiting-approval on a stale flag.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    gate.block("Bash", {"command": "x"})
    assert gate.pending_halt is True
    gate.reset()
    assert gate.pending_halt is False


# --- C. a non-gated tool falls through to the existing callback -----------------


def test_hook_returns_no_decision_for_an_ungated_tool() -> None:
    # revert: hook returns an explicit "allow" for every tool -> this fails.
    # An explicit allow would be a behavior change well beyond the gate: the SDK
    # documents that a PreToolUse allow SKIPS can_use_tool entirely, so allowing
    # Read here would silently disable the approval callback for every other tool.
    gate = ApprovalGate(required=frozenset({"Bash"}))

    output = _run_hook(gate, "Read", {"file_path": "/etc/hosts"})

    assert output == {}
    assert gate.pending_summary is None
    assert gate.pending_halt is False


@pytest.mark.parametrize(
    "hook_input",
    [None, {}, {"tool_name": None}, {"tool_input": {"command": "x"}}, "not-a-mapping", []],
)
def test_hook_tolerates_a_missing_or_odd_shaped_input(hook_input: object) -> None:
    # revert: index hook_input["tool_name"] directly -> a KeyError/TypeError inside
    # a permission hook, which the CLI reports as a hook error and then lets the
    # call PROCEED. A crash in the gate must never become a fail-open.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    callback = _hook_callback(gate)

    async def go() -> Any:
        return await callback(hook_input, None, {"signal": None})

    assert anyio.run(go) == {}
    assert gate.pending_summary is None


# --- D. the one-shot grant is spent by the HOOK, not by can_use_tool (AC4) ------


def test_hook_spends_the_one_shot_grant_and_re_arms() -> None:
    # revert: hook returns {} when a grant is available (leaving the allow to
    # can_use_tool) -> the SDK never reaches can_use_tool for a hook-gated call, so
    # the approved action is denied a second time and the resume deadlocks.
    gate = ApprovalGate(required=frozenset({"Bash"}), grant_tool="Bash")

    first = _run_hook(gate, "Bash", {"command": "the approved action"})
    assert _decision(first)["permissionDecision"] == "allow"
    assert "continue_" not in first, "an allow must not stop the turn"
    assert gate.pending_summary is None  # the approved call records no block
    assert gate.pending_halt is False

    second = _run_hook(gate, "Bash", {"command": "sneak a retry"})
    assert _decision(second)["permissionDecision"] == "deny"
    assert gate.pending_summary is not None
    assert "sneak a retry" in gate.pending_summary


def test_can_use_tool_is_not_what_consumed_the_grant() -> None:
    # revert: leave consume_grant in build_can_use_tool only -> this fails.
    # Observed live: a PreToolUse hook returning "allow" SKIPS can_use_tool, so if
    # the hook allowed while can_use_tool still owned the grant, the grant would
    # never be spent and the very next gated call would be allowed too -- one
    # approval buying unlimited executions.
    gate = ApprovalGate(required=frozenset({"Bash"}), grant_tool="Bash")
    callback = build_can_use_tool(gate)

    allowed = _run_hook(gate, "Bash", {"command": "the approved action"})
    assert _decision(allowed)["permissionDecision"] == "allow"

    async def go() -> None:
        # The hook spent it: the callback now denies.
        result = await callback("Bash", {"command": "again"}, ToolPermissionContext())
        assert isinstance(result, PermissionResultDeny)

    anyio.run(go)


# --- E. the callback's deny interrupts the turn ---------------------------------


def test_can_use_tool_deny_carries_interrupt_true() -> None:
    # revert: PermissionResultDeny(message=...) without interrupt=True -> the SDK
    # default is interrupt=False (see PermissionResultDeny in
    # claude_agent_sdk/types.py), so the model keeps its turn open after the deny
    # and the run hangs instead of pausing for approval -- the live-OpenRouter half
    # of the issue.
    async def go() -> None:
        gate = ApprovalGate(required=frozenset({"Bash"}))
        callback = build_can_use_tool(gate)
        result = await callback("Bash", {"command": "x"}, ToolPermissionContext())
        assert isinstance(result, PermissionResultDeny)
        assert result.message == _DENY_MESSAGE
        assert result.interrupt is True

    anyio.run(go)


def test_can_use_tool_allow_is_unchanged_for_ungated_tools() -> None:
    # negative control: interrupt=True must ride the DENY only. An allow that
    # somehow carried a stop would kill every ungated tool call.
    async def go() -> None:
        gate = ApprovalGate(required=frozenset({"Bash"}))
        callback = build_can_use_tool(gate)
        result = await callback("Read", {"file_path": "/x"}, ToolPermissionContext())
        assert isinstance(result, PermissionResultAllow)

    anyio.run(go)


# --- F. the headline regression: a stopped turn still finalizes as an approval ---


def test_gated_turn_ending_in_an_error_result_finalizes_awaiting_approval() -> None:
    # revert: _apply_approval_override flips only a DONE final -> this fails with
    # status == classified-failure and no approval_summary, which is exactly the
    # "denied tool call left the turn hanging with no approval record" defect.
    # Stopping the turn is what the gate ASKED for, so the interrupt-shaped
    # terminal result it produces must not be read as the run failing.
    gate = ApprovalGate(required=frozenset({"Bash"}), route_by_tool={"Bash": "ops"})
    runner, _session = _runner_over(
        _gated_bash_then_error_result(),
        gate=gate,
        # interrupt=False so the scripted terminal ERROR result is actually
        # delivered; that result is the shape under test.
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.status != SessionStatus.CLASSIFIED_FAILURE
    assert final.approval_summary
    assert final.approval_summary.startswith(APPROVAL_SUMMARY_PREFIX)
    assert final.approval_gate_kind == "permission"
    assert final.approval_granted_tool == "Bash"
    assert final.approval_route == "ops"
    assert runner.status == SessionStatus.AWAITING_APPROVAL


def test_ungated_error_result_still_ends_classified_failure() -> None:
    # negative control for F: the halt flag must be the ONLY thing that rescues an
    # error result. revert: flip any error final to awaiting-approval -> this fails,
    # and every genuine crash would be parked behind a human decision forever.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    runner, _session = _runner_over(
        [
            AssistantMessage(content=[TextBlock(text="working")], model="m"),
            ResultMessage(
                subtype="error_during_execution",
                duration_ms=1,
                duration_api_ms=1,
                is_error=True,
                num_turns=1,
                session_id="s",
                result="boom",
            ),
        ],
        gate=gate,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert not events[-1].approval_summary



def _gated_bash_then_model_error_result() -> list[Any]:
    """A gated Bash call, then a REAL model failure, then the error result.

    Same shape as ``_gated_bash_then_error_result`` except the provider reported
    a failure of its own (``AssistantMessage.error``) before the turn ended --
    which is what ``translate.py`` records on ``TurnState.error_classification``.
    ``server_error`` is deliberately not ``authentication_failed``: the auth code
    fast-fails earlier in ``_drive_turn`` and would never reach the override.
    """

    return [
        AssistantMessage(content=[TextBlock(text="I'll run that")], model="m"),
        AssistantMessage(
            content=[ToolUseBlock(id="t1", name="Bash", input={"command": "rm -rf /tmp/x"})],
            model="m",
        ),
        AssistantMessage(content=[], model="m", error="server_error"),
        ResultMessage(
            subtype="error_during_execution",
            duration_ms=1,
            duration_api_ms=1,
            is_error=True,
            num_turns=1,
            session_id="s",
            result="provider unavailable",
        ),
    ]


def test_gated_turn_that_also_hit_a_model_error_ends_classified_failure() -> None:
    # revert (the mutation this kills): drop `state.error_classification is None`
    # from the halt branch of _apply_approval_override, i.e. flip ANY non-DONE
    # final whenever the halt marker is set -> this fails with
    # status == awaiting-approval.
    #
    # The halt marker is written by ApprovalGate.block at DENY time, before the
    # turn's terminal cause is known, so it alone cannot distinguish "the CLI
    # aborted because we asked it to" (test F) from "the provider fell over".
    # Relabelling a provider outage as awaiting-approval hides a real failure
    # behind a human decision that cannot fix it, and an approver resuming it
    # lands straight back in the same failure.
    gate = ApprovalGate(required=frozenset({"Bash"}), route_by_tool={"Bash": "ops"})
    runner, _session = _runner_over(
        _gated_bash_then_model_error_result(),
        gate=gate,
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    # Not vacuous: the gate really did fire and really did ask for the halt, so
    # the ONLY thing keeping this out of awaiting-approval is the model error.
    assert gate.pending_summary is not None
    assert gate.pending_halt is True

    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    assert final.status != SessionStatus.AWAITING_APPROVAL
    assert not final.approval_summary
    # And the failure is still reported as itself, not swallowed. The SDK token
    # is constrained to unclassified; the raw token survives in the message.
    error_events = [e for e in events if e.type == "error"]
    assert error_events, "the model's own error frame must survive"
    assert any(e.classification == "unclassified" for e in error_events)
    assert all(e.classification != "server_error" for e in error_events)
    assert any(
        "server_error" in getattr(e, "message", "") for e in error_events
    ), "the raw token must survive in the ErrorEvent message"


def test_a_done_gated_turn_still_flips_even_after_a_model_error_frame() -> None:
    # negative control for the guard above: the new error_classification check
    # must ride the HALT branch only. revert: apply it to the DONE branch too ->
    # this fails, and a turn the model finished cleanly after a recovered
    # (non-terminal) error would lose its approval record entirely.
    gate = ApprovalGate(required=frozenset({"Bash"}), route_by_tool={"Bash": "ops"})
    script = [
        AssistantMessage(content=[], model="m", error="server_error"),
        AssistantMessage(
            content=[ToolUseBlock(id="t1", name="Bash", input={"command": "rm -rf /tmp/x"})],
            model="m",
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="recovered and finished",
        ),
    ]
    runner, _session = _runner_over(
        script,
        gate=gate,
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.approval_summary
    assert final.approval_gate_kind == "permission"


# --- G. negative control: an operator interrupt outranks the gate ---------------


def test_operator_interrupt_on_a_gated_turn_still_ends_idle() -> None:
    # revert: copy the gate's halt marker onto the turn state unconditionally ->
    # this fails. A human pressing stop is an intentional stop, not an approval
    # request; reporting it as awaiting-approval would suspend the thread behind a
    # decision nobody asked for and no approver would recognize.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    runner, session = _runner_over(
        _gated_bash_then_error_result(),
        gate=gate,
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())  # first frame; the turn is live
        await runner.interrupt("user stop")  # the operator's own stop
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    # The gate really did block this turn -- otherwise the control is vacuous.
    assert gate.pending_summary is not None
    assert session.interrupts >= 1
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT


# --- H. negative control: a budget halt still outranks a pending approval --------


def test_budget_halt_still_outranks_a_pending_approval() -> None:
    # revert: evaluate the approval override before the budget halt -> this fails.
    # A run that blew its token ceiling has not completed cleanly; suspending it
    # behind a human decision strands a broken run, and approving it would resume
    # straight back into the same halt.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    script = [
        AssistantMessage(
            content=[ToolUseBlock(id="t1", name="Bash", input={"command": "x"})],
            model="m",
            usage={"output_tokens": 500},
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
            usage={"output_tokens": 500},
        ),
    ]
    runner, _session = _runner_over(
        script,
        gate=gate,
        ceiling=10,
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    assert gate.pending_summary is not None  # the gate did fire this turn
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert "budget" in events[-1].text


# --- I. boot merges the approval matcher WITH the bundle's own hooks -------------


class _CapturedSession:
    """Stands in for ClaudeAgentSession so the built options can be inspected.

    The real class constructs a ClaudeSDKClient; the assertion here is about what
    ``build_runner`` PUT in the options, so the transport is not needed.
    """

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

    async def receive_turn(self) -> typing.AsyncIterator[Any]:
        if False:
            yield None


def _options_from_boot(
    monkeypatch: pytest.MonkeyPatch,
    config: RunnerConfig,
    *,
    workspace_path: Path | None = None,
) -> Any:
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    runner = build_runner(config, workspace_path=workspace_path)
    session = runner._factory()
    assert isinstance(session, _CapturedSession)
    return session.options


async def _mcp_tool_names(server: Any) -> set[str]:
    entry = server["instance"].get_request_handler("tools/list")
    if entry is None:
        return set()
    result = await entry.handler(None, mcp_types.PaginatedRequestParams())
    return {tool.name for tool in result.tools}


def _add_capability_server(plugin_dir: str, mode: str) -> None:
    Path(plugin_dir, ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "operations": {
                        "command": sys.executable,
                        "args": [str(_CAPABILITY_SERVER)],
                        "env": {"CURIE_TEST_TOOL_MODE": mode},
                    }
                }
            }
        ),
        encoding="utf-8",
    )


_BUNDLE_HOOKS = {
    "PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": "true"}]}]
}


def test_boot_merges_the_approval_matcher_ahead_of_bundle_hooks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # revert: pass hooks=bundle_hooks unchanged -> the gate never runs as a hook and
    # a skill/settings permission rule silently bypasses it (the #1852 defect).
    # revert: pass hooks=approval_hook only -> the bundle's declared PreToolUse
    # guardrails (#272) are silently dropped.
    plugin_dir = _bundle(tmp_path, manifest_hooks=_BUNDLE_HOOKS)
    config = _config(plugin_dir, CURIE_APPROVAL_REQUIRED_TOOLS="Bash")

    options = _options_from_boot(monkeypatch, config)

    matchers = options.hooks["PreToolUse"]
    assert len(matchers) == 2
    # The approval matcher is first and unscoped; the bundle's keeps its own
    # matcher string, proving it was preserved rather than rebuilt.
    assert matchers[0].matcher is None
    assert matchers[1].matcher == "Bash"
    # Defense in depth: the callback stays wired too, so an ungated tool that
    # falls through the hook is still decided by the approval callback.
    assert options.can_use_tool is not None


def test_boot_wires_the_approval_matcher_when_the_bundle_declares_no_hooks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # revert: only merge when bundle hooks exist -> the common case (a gated bundle
    # with no hooks of its own) boots with no hook at all and stays bypassable.
    plugin_dir = _bundle(tmp_path)
    config = _config(plugin_dir, CURIE_APPROVAL_REQUIRED_TOOLS="Bash")

    options = _options_from_boot(monkeypatch, config)

    assert options.hooks is not None
    assert [m.matcher for m in options.hooks["PreToolUse"]] == [None]


def test_boot_adds_no_approval_matcher_when_nothing_is_gated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # negative control: an unconfigured agent must see zero behavior change --
    # no gate, no approval hook, and the bundle's own hooks untouched.
    plugin_dir = _bundle(tmp_path, manifest_hooks=_BUNDLE_HOOKS)
    config = _config(plugin_dir)

    options = _options_from_boot(monkeypatch, config)

    assert [m.matcher for m in options.hooks["PreToolUse"]] == ["Bash"]
    assert options.can_use_tool is None


def test_boot_omits_request_approval_for_an_observed_read_only_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    plugin_dir = _bundle(tmp_path)
    _add_capability_server(plugin_dir, "read-only")
    caplog.set_level(logging.INFO, logger="curie_runner")

    options = _options_from_boot(monkeypatch, _config(plugin_dir))

    assert anyio.run(
        _mcp_tool_names,
        options.mcp_servers[APPROVAL_SERVER_NAME],
    ) == {"publish_changes"}
    assert "operations" not in options.mcp_servers  # plugin-loaded, not platform-mounted
    assert any(
        "request_approval omitted" in message
        and "tool_count=1" in message
        and "probe_complete=True" in message
        for message in caplog.messages
    )


def test_boot_omits_request_approval_for_a_complete_empty_mcp_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin_dir = _bundle(tmp_path)

    options = _options_from_boot(monkeypatch, _config(plugin_dir))

    # Built-in Claude tools are not MCP actions and do not make Curie's generic
    # MCP pager useful. An explicit gate on one is the separate override pinned
    # below; with no MCP tools and no gate, there is nothing a human can unlock.
    # The separate built-in publication protocol remains discoverable and
    # independently refuses execution until a workspace is mounted.
    assert anyio.run(
        _mcp_tool_names,
        options.mcp_servers[APPROVAL_SERVER_NAME],
    ) == {"publish_changes"}


def test_boot_keeps_request_approval_for_an_observed_write_capable_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin_dir = _bundle(tmp_path)
    _add_capability_server(plugin_dir, "write")

    options = _options_from_boot(monkeypatch, _config(plugin_dir))

    assert APPROVAL_SERVER_NAME in options.mcp_servers
    assert options.mcp_servers[APPROVAL_SERVER_NAME]["name"] == APPROVAL_SERVER_NAME


def test_explicit_gate_keeps_pager_and_annotations_classify_receipt_actions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    tool = "mcp__plugin_approval-demo_operations__inspect_or_change"
    expected_flags = {
        "read-only": [],
        "write": [tool],
        "unknown": [tool],
    }
    read_only_runner: SessionRunner | None = None

    for mode, flagged_tools in expected_flags.items():
        plugin_dir = _bundle(tmp_path / mode, gates=["Bash"])
        _add_capability_server(plugin_dir, mode)

        runner = build_runner(_config(plugin_dir))
        if mode == "read-only":
            read_only_runner = runner
        session = runner._factory()
        assert isinstance(session, _CapturedSession)

        # An explicit actionable gate retains the pager regardless of MCP hints.
        assert APPROVAL_SERVER_NAME in session.options.mcp_servers
        assert "request_approval" in anyio.run(
            _mcp_tool_names,
            session.options.mcp_servers[APPROVAL_SERVER_NAME],
        )

        events = translate_message(
            AssistantMessage(
                content=[ToolUseBlock(id=f"call-{mode}", name=tool, input={})],
                model="m",
            ),
            TurnState(),
            runner._classifier,
            None,
        )
        assert [event.tool for event in events if event.type == "side_effect_flag"] == (
            flagged_tools
        )

    # The annotation is exact and fail-closed: it cannot bless an unadvertised
    # tool merely because that call shares the same MCP server.
    assert read_only_runner is not None
    unadvertised = "mcp__plugin_approval-demo_operations__not_advertised"
    events = translate_message(
        AssistantMessage(
            content=[ToolUseBlock(id="call-unadvertised", name=unadvertised, input={})],
            model="m",
        ),
        TurnState(),
        read_only_runner._classifier,
        None,
    )
    assert [event.tool for event in events if event.type == "side_effect_flag"] == [
        unadvertised
    ]


def test_explicit_tool_gate_overrides_its_read_only_annotation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    tool = "mcp__plugin_approval-demo_operations__inspect_or_change"
    cases = (
        ("tool-gated", tool, [tool]),
        # Control: a gate on an unrelated built-in must not turn this annotated
        # MCP read into a receipt mutation.
        ("bash-gated", "Bash", []),
    )

    for directory, gated_tool, expected_flags in cases:
        plugin_dir = _bundle(tmp_path / directory)
        _add_capability_server(plugin_dir, "read-only")
        runner = build_runner(
            _config(plugin_dir, CURIE_APPROVAL_REQUIRED_TOOLS=gated_tool)
        )
        session = runner._factory()
        assert isinstance(session, _CapturedSession)
        assert "request_approval" in anyio.run(
            _mcp_tool_names,
            session.options.mcp_servers[APPROVAL_SERVER_NAME],
        )

        events = translate_message(
            AssistantMessage(
                content=[ToolUseBlock(id=f"call-{directory}", name=tool, input={})],
                model="m",
            ),
            TurnState(),
            runner._classifier,
            None,
        )

        assert [event.tool for event in events if event.type == "side_effect_flag"] == (
            expected_flags
        )


def test_publish_only_gate_does_not_recreate_the_generic_pager(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin_dir = _bundle(tmp_path / "bundle")
    _add_capability_server(plugin_dir, "read-only")
    workspace = tmp_path / "workspace"
    (workspace / ".git").mkdir(parents=True)

    options = _options_from_boot(
        monkeypatch,
        _config(plugin_dir),
        workspace_path=workspace,
    )

    assert anyio.run(
        _mcp_tool_names, options.mcp_servers[APPROVAL_SERVER_NAME]
    ) == {"publish_changes"}


# --- J. fake-tier parity: a deny with interrupt=True stops the replay ------------


def _collect(session: FakeModelSession) -> list[Any]:
    async def go() -> list[Any]:
        await session.connect()
        await session.query("go")
        return [message async for message in session.receive_turn()]

    return anyio.run(go)


def test_fake_session_stops_replaying_after_an_interrupting_deny() -> None:
    # revert: FakeModelSession._apply_gate ignores the PermissionResultDeny result
    # -> the offline tier keeps replaying past a call the real SDK would have
    # aborted, so the fake/CI/chart-default path stops modelling the gate it is
    # supposed to prove (the tier-parity rule).
    sentinel = "MUST-NOT-REPLAY-AFTER-INTERRUPTING-DENY"
    gate = ApprovalGate(required=frozenset({"Bash"}))
    script = [
        AssistantMessage(
            content=[ToolUseBlock(id="t1", name="Bash", input={"command": "x"})], model="m"
        ),
        AssistantMessage(content=[TextBlock(text=sentinel)], model="m"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result=sentinel,
        ),
    ]

    session = FakeModelSession(lambda: script, can_use_tool=_recording_deny(gate, interrupt=True))
    messages = _collect(session)

    texts = [
        block.text
        for message in messages
        if isinstance(message, AssistantMessage)
        for block in message.content
        if isinstance(block, TextBlock)
    ]
    assert sentinel not in texts
    assert not any(isinstance(m, ResultMessage) for m in messages)
    assert gate.pending_summary is not None


def test_fake_session_keeps_replaying_after_a_non_interrupting_deny() -> None:
    # negative control for J: only interrupt=True truncates. Without this, an
    # implementation that truncates on ANY deny would pass J while breaking every
    # existing gated-turn test that expects the terminal result to arrive.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    script = [
        AssistantMessage(
            content=[ToolUseBlock(id="t1", name="Bash", input={"command": "x"})], model="m"
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
        ),
    ]

    session = FakeModelSession(lambda: script, can_use_tool=_recording_deny(gate, interrupt=False))
    messages = _collect(session)

    assert any(isinstance(m, ResultMessage) for m in messages)


# --- K. pin the SDK's decision-key names ----------------------------------------
#
# Source of truth: .venv/lib/python3.14/site-packages/claude_agent_sdk/types.py
# (SyncHookJSONOutput, PreToolUseHookSpecificOutput, PermissionResultDeny) and
# claude_agent_sdk/_internal/query.py::_convert_hook_output_for_cli. These are
# TypedDicts, so a rename upstream is invisible at runtime -- the hook would keep
# returning keys the CLI ignores and the gate would silently stop gating, which is
# the exact failure mode #1852 is about. Assert against the pinned SDK's own
# declarations rather than a hand-copied table.


def test_hook_output_keys_are_declared_by_the_pinned_sdk() -> None:
    # revert: rename any emitted key (continue_ -> continue, permissionDecision ->
    # decision, hookSpecificOutput -> hookOutput) -> this fails instead of the gate
    # silently ceasing to enforce.
    gate = ApprovalGate(required=frozenset({"Bash"}))
    output = _run_hook(gate, "Bash", {"command": "x"})

    top_level = set(SyncHookJSONOutput.__annotations__)
    assert set(output) <= top_level, f"undeclared top-level key(s): {set(output) - top_level}"
    assert {"continue_", "stopReason", "hookSpecificOutput"} <= set(output)

    specific = set(PreToolUseHookSpecificOutput.__annotations__)
    decision = _decision(output)
    assert set(decision) <= specific, f"undeclared decision key(s): {set(decision) - specific}"
    assert {"hookEventName", "permissionDecision", "permissionDecisionReason"} <= set(decision)


def test_the_decision_values_are_declared_by_the_pinned_sdk() -> None:
    # revert: emit "block"/"reject" instead of "deny", or "PreToolUseHook" as the
    # event name -> the CLI silently ignores an unrecognized value and the call
    # proceeds.
    annotation = PreToolUseHookSpecificOutput.__annotations__["permissionDecision"]
    literal = typing.get_args(annotation)[0]  # unwrap NotRequired[...]
    allowed = set(typing.get_args(literal))
    assert {"allow", "deny"} <= allowed

    gate = ApprovalGate(required=frozenset({"Bash"}))
    denied = _decision(_run_hook(gate, "Bash", {"command": "x"}))
    assert denied["permissionDecision"] in allowed

    granted = ApprovalGate(required=frozenset({"Bash"}), grant_tool="Bash")
    allowed_out = _decision(_run_hook(granted, "Bash", {"command": "x"}))
    assert allowed_out["permissionDecision"] in allowed

    event_literal = typing.get_args(PreToolUseHookSpecificOutput.__annotations__["hookEventName"])
    assert denied["hookEventName"] in set(event_literal)


def test_permission_result_deny_still_carries_an_interrupt_field() -> None:
    # revert / upstream drift: if the SDK drops or renames `interrupt`, the deny in
    # build_can_use_tool stops stopping the turn and the run hangs again. Fail here
    # rather than in production.
    fields = {f.name for f in dataclasses.fields(PermissionResultDeny)}
    assert {"message", "interrupt"} <= fields
    assert PermissionResultDeny().interrupt is False  # the default we must override


# --- L. the runner-owned stream fallback for publication (#2294) -----------------
#
# Both SDK-level gate layers can miss a call: ``can_use_tool`` is skipped whenever
# another permission rule already allowed it, and the PreToolUse hook was observed
# live NOT to gate an in-process ``mcp__curie__*`` call. When both miss, the
# stream still carries the ``ToolUseBlock`` -- so the runner itself observes it and
# either records the pending approval or fails the turn closed. It can never allow.


def _publish_block(
    title: str = "Ship changes",
    body: str = "The proposed change.",
    *,
    call_id: str = "p1",
) -> AssistantMessage:
    return AssistantMessage(
        content=[
            ToolUseBlock(
                id=call_id,
                name=PUBLISH_TOOL_NAME,
                input={"title": title, "body": body},
            )
        ],
        model="m",
    )


def _publish_then_clean_done(
    title: str = "Ship changes", body: str = "The proposed change."
) -> list[Any]:
    """The observed live shape: the publish call runs, then the turn ends clean.

    Neither gate layer denied, so the in-process tool body executed and returned
    its defensive ``is_error`` result; the model then finished its turn normally
    and the CLI reported a SUCCESS terminal result. Before #2294 that finalized
    DONE with no approval record at all.
    """

    return [
        _publish_block(title, body),
        AssistantMessage(content=[TextBlock(text="I requested publication.")], model="m"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="I requested publication.",
        ),
    ]


def _managed_publish_gate() -> ApprovalGate:
    """The gate a managed-workspace boot assembles: publish is the only member."""

    gate = build_approval_gate(operator_tools=None, policy_routes={}, managed_workspace=True)
    assert gate is not None
    assert PUBLISH_TOOL_NAME in gate.required
    return gate


def _run_lines(runner: SessionRunner) -> list[str]:
    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_turn(Event(type="message", text="go", user="U", ts="1")):
            lines.append(line)

    anyio.run(go)
    return lines


def test_publish_call_neither_gate_layer_saw_still_pauses_awaiting_approval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # revert: drop _observe_publication_calls from _drive_turn -> this fails with
    # status == done and no approval_summary, which is the verbatim #2294 live
    # observation: the tool body ran, returned its defensive is_error, and the
    # turn finalized DONE with nothing for a human to approve.
    #
    # NO can_use_tool is wired here on purpose: that models BOTH SDK layers
    # skipping the call (the hook did not gate the in-process MCP tool, and the
    # callback was never consulted). The stream is then the only observer left.
    gate = _managed_publish_gate()
    runner, _session = _runner_over(_publish_then_clean_done(), gate=gate)

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        lines = _run_lines(runner)

    events = parse_ndjson("".join(lines))
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.status != SessionStatus.DONE
    assert final.approval_summary
    assert final.approval_summary.startswith(APPROVAL_SUMMARY_PREFIX)
    assert PUBLISH_TOOL_NAME in final.approval_summary
    # The trusted provenance pair the worker branches on before it captures a
    # patch. A fallback record that stamped anything else would be inert.
    assert final.approval_gate_kind == "permission"
    assert final.approval_granted_tool == PUBLISH_TOOL_NAME
    assert final.approval_route is None
    assert gate.publication_title == "Ship changes"
    assert gate.publication_body == "The proposed change."
    assert runner.status == SessionStatus.AWAITING_APPROVAL
    # No gate layer denied this call, so no layer asked the CLI to stop the turn.
    # That absence is the ONLY honest signal that a layer missed the call: the
    # real SDK streams the ToolUseBlock BEFORE it dispatches the PreToolUse hook,
    # so "the observer wrote the record first" is true on the normal path too and
    # cannot be what the warning below keys on.
    assert gate.pending_halt is False

    # Operator-visible, because this path means a gate layer that was supposed to
    # decide did not: silently papering over that is how the next regression goes
    # unnoticed for a whole release.
    assert any(
        "publication" in record.getMessage().lower()
        and "fallback" in record.getMessage().lower()
        for record in caplog.records
    ), caplog.text


def test_hook_recorded_publish_is_not_recorded_twice_by_the_stream(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # revert: record unconditionally in _observe_publication_calls -> the normal
    # (working) path grows a second record, and the WARNING fires on every publish
    # so it stops meaning "a gate layer missed this call".
    gate = _managed_publish_gate()
    runner, _session = _runner_over(
        _publish_then_clean_done("Hook recorded", "hook body"),
        gate=gate,
        # The fake tier does not run PreToolUse hooks, so the callback seam is how
        # a session-level test drives a hook-shaped block (see _recording_deny).
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        lines = _run_lines(runner)

    events = parse_ndjson("".join(lines))
    final = events[-1]
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.approval_gate_kind == "permission"
    assert final.approval_granted_tool == PUBLISH_TOOL_NAME
    # Exactly one record, and it is the gate layer's own -- the observer added
    # nothing on top of it.
    assert gate.publication_title == "Hook recorded"
    assert gate.publication_body == "hook body"
    assert final.approval_summary == gate.pending_summary
    assert gate.observe_publication({"title": "another", "body": "x"}) is False
    assert gate.publication_title == "Hook recorded"
    # A gate layer DID deny this call and asked the CLI to stop, which is exactly
    # what distinguishes this path from the fallback one -- and why no warning
    # about a missed layer may fire here.
    assert gate.pending_halt is True
    assert not any(
        "fallback" in record.getMessage().lower() for record in caplog.records
    ), caplog.text


def test_malformed_publish_proposal_fails_the_turn_closed() -> None:
    # revert: swallow the ValueError and let the turn finalize as it was -> a
    # blank-titled publish call ends DONE with no approval record, which is the
    # same silent hole #2294 closed, reached through the validation path instead.
    # Failing closed is the only safe direction: the runner cannot record the
    # approval, so it must not let the turn look like it succeeded.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = _managed_publish_gate()
    runner, _session = _runner_over(_publish_then_clean_done(title="   "), gate=gate)

    events = parse_ndjson("".join(_run_lines(runner)))

    classifications = [e.classification for e in events if e.type == "error"]
    assert PUBLICATION_UNRECORDED_CLASSIFICATION in classifications
    unrecorded = next(
        e
        for e in events
        if e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
    )
    assert "title" in (unrecorded.message or "").lower()
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    assert final.status != SessionStatus.AWAITING_APPROVAL
    # A fail-closed turn carries NO approval fields: there is nothing a human
    # could resolve, and a card with an empty title is worse than none.
    assert final.approval_summary is None
    assert final.approval_gate_kind is None
    assert final.approval_granted_tool is None
    assert gate.pending_summary is None
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_two_publish_calls_in_one_turn_record_exactly_one_approval() -> None:
    # revert: record every observed call -> one action produces two approval
    # cards, and a human resolving the second leaves the first pending forever.
    # The FIRST proposal wins, matching ApprovalGate.block's first-block-wins rule.
    gate = _managed_publish_gate()
    script = [
        _publish_block("First proposal", "one", call_id="p1"),
        _publish_block("Second proposal", "two", call_id="p2"),
        AssistantMessage(content=[TextBlock(text="Requested twice.")], model="m"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="Requested twice.",
        ),
    ]
    runner, _session = _runner_over(script, gate=gate)

    events = parse_ndjson("".join(_run_lines(runner)))

    final = events[-1]
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert gate.publication_title == "First proposal"
    assert gate.publication_body == "one"
    assert "First proposal" in (final.approval_summary or "")
    assert "Second proposal" not in (final.approval_summary or "")


def test_publish_call_on_a_gate_that_does_not_carry_publish_fails_closed() -> None:
    # revert: record the publication anyway when the tool is not gated -> a model
    # that names publish_changes where it is not mounted (no managed workspace)
    # mints an approval for an action the platform has no checkout to perform,
    # and an approver would be asked to authorize a publication that cannot exist.
    # Never AWAITING_APPROVAL, never DONE: fail closed.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = build_approval_gate(operator_tools=["Bash"], policy_routes={})
    assert gate is not None
    assert PUBLISH_TOOL_NAME not in gate.required
    runner, _session = _runner_over(_publish_then_clean_done(), gate=gate)

    events = parse_ndjson("".join(_run_lines(runner)))

    assert any(
        e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
        for e in events
    ), [e.type for e in events]
    final = events[-1]
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    assert final.status != SessionStatus.AWAITING_APPROVAL
    assert final.approval_summary is None
    assert gate.pending_summary is None
    assert gate.publication_title is None


def test_operator_interrupt_outranks_a_stream_recorded_publication() -> None:
    # negative control for L: the observer records, it never decides the turn's
    # status. A human pressing stop is an intentional stop -- reporting it as
    # awaiting-approval would suspend the thread behind a decision nobody asked
    # for. revert: set pending_halt in observe_publication, or apply the approval
    # override ahead of _reclassify -> this fails with awaiting-approval.
    gate = _managed_publish_gate()
    runner, session = _runner_over(_publish_then_clean_done(), gate=gate)

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())  # first frame; the turn is live
        await runner.interrupt("user stop")  # the operator's own stop
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    # Not vacuous: the fallback really did record this publication, so the ONLY
    # thing keeping the turn out of awaiting-approval is the operator's stop.
    assert gate.pending_summary is not None
    assert gate.publication_title == "Ship changes"
    assert gate.pending_halt is False
    assert session.interrupts >= 1
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT


def test_malformed_publish_followed_by_a_valid_one_pauses_on_the_valid_record() -> None:
    # revert (the mutation this kills): make state.publication_unrecorded sticky,
    # i.e. skip every later publish call once one failed to record -> this fails
    # with classified-failure, and a model that did exactly what the deny message
    # told it to ("Correct it and retry") loses the corrected request. The
    # fail-closed rule keys on the OUTCOME at classification time -- did the gate
    # end the turn holding a publication record? -- not on whether some earlier
    # attempt was malformed.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = _managed_publish_gate()
    script = [
        _publish_block("   ", "first attempt", call_id="p1"),
        _publish_block("Ship changes", "the corrected proposal", call_id="p2"),
        AssistantMessage(content=[TextBlock(text="Requested publication.")], model="m"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="Requested publication.",
        ),
    ]
    runner, _session = _runner_over(script, gate=gate)

    events = parse_ndjson("".join(_run_lines(runner)))

    assert not any(
        e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
        for e in events
    ), "a recorded publication must not also report itself unrecorded"
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.status != SessionStatus.CLASSIFIED_FAILURE
    assert final.approval_gate_kind == "permission"
    assert final.approval_granted_tool == PUBLISH_TOOL_NAME
    # Exactly one record, and it is the VALID proposal -- not the malformed one,
    # which never wrote anything.
    assert gate.publication_title == "Ship changes"
    assert gate.publication_body == "the corrected proposal"
    assert final.approval_summary == gate.pending_summary
    assert "Ship changes" in (final.approval_summary or "")
    assert runner.status == SessionStatus.AWAITING_APPROVAL


def test_hook_recorded_publish_survives_a_later_malformed_stream_observation() -> None:
    # revert: validate the publication proposal BEFORE the first-record-wins guard
    # in _record_pending -> this fails, because the malformed second call would
    # raise on a turn that already holds a good record and fail the turn closed,
    # discarding an approval a human could have acted on. A record that already
    # stands is never re-derived from a later attempt.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = _managed_publish_gate()
    script = [
        _publish_block("Ship changes", "the proposal", call_id="p1"),
        _publish_block("", "second attempt", call_id="p2"),
        AssistantMessage(content=[TextBlock(text="Requested publication.")], model="m"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="Requested publication.",
        ),
    ]
    runner, _session = _runner_over(
        script,
        gate=gate,
        # The gate layer records the FIRST call, exactly as the PreToolUse hook
        # does in production (the fake tier does not run hooks; see _recording_deny).
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    events = parse_ndjson("".join(_run_lines(runner)))

    assert not any(
        e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
        for e in events
    ), "a later malformed attempt must not poison the record the hook already wrote"
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.status != SessionStatus.CLASSIFIED_FAILURE
    assert final.approval_gate_kind == "permission"
    assert final.approval_granted_tool == PUBLISH_TOOL_NAME
    # The hook's own record, untouched by the second observation.
    assert gate.publication_title == "Ship changes"
    assert gate.publication_body == "the proposal"
    assert final.approval_summary == gate.pending_summary
    assert "Ship changes" in (final.approval_summary or "")


def test_another_gated_tools_approval_is_kept_when_a_publish_call_cannot_take_the_slot(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # revert: fail the turn closed whenever the gate does not end holding a
    # PUBLICATION record -> this fails with classified-failure, and the Bash
    # approval a human could have acted on is destroyed by a publish call that
    # merely arrived second. Exactly one record per turn is the gate's own
    # first-block-wins rule (ApprovalGate._record_pending); a publish call losing
    # that race is not evidence that anything went wrong, so the turn parks on
    # the approval that DOES stand and the unclaimed publish intent is reported
    # as a log line, not by discarding the card.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = build_approval_gate(
        operator_tools=["Bash"], policy_routes={}, managed_workspace=True
    )
    assert gate is not None
    assert gate.required == frozenset({"Bash", PUBLISH_TOOL_NAME})
    script = [
        AssistantMessage(
            content=[ToolUseBlock(id="b1", name="Bash", input={"command": "git status"})],
            model="m",
        ),
        _publish_block("Ship changes", "the proposal", call_id="p1"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="asked for both",
        ),
    ]
    runner, _session = _runner_over(
        script,
        gate=gate,
        can_use_tool=_recording_deny(gate, interrupt=False),
        truncate_on_interrupt=False,
    )

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        events = parse_ndjson("".join(_run_lines(runner)))

    assert not any(
        e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
        for e in events
    ), "a turn that ends holding an approval has not lost the human's decision"
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.status != SessionStatus.CLASSIFIED_FAILURE
    assert final.approval_gate_kind == "permission"
    assert final.approval_granted_tool == "Bash"
    assert "git status" in (final.approval_summary or "")
    # A gate layer denied the Bash call and asked the CLI to stop.
    assert gate.pending_halt is True
    # The publish intent did not make it onto the card, so it must be visible to
    # an operator rather than silently dropped.
    assert any(
        "publication" in record.getMessage().lower()
        and "not carried" in record.getMessage().lower()
        for record in caplog.records
    ), caplog.text


def test_policy_approval_and_publish_in_one_turn_park_on_the_policy_request(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # revert: fail closed because the FINAL carries a policy approval rather than
    # the publication record -> this fails with classified-failure, and a model
    # that legitimately paged a human (request_approval) loses that card because
    # it also asked to publish. _merge_gate_block's precedence (a policy summary
    # already standing is never overwritten by a permission block) is pre-existing
    # and unchanged here; what must not happen is the turn being failed for it.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = _managed_publish_gate()
    script = [
        AssistantMessage(
            content=[
                ToolUseBlock(
                    id="a1",
                    name=APPROVAL_TOOL_NAME,
                    input={"summary": "Approve the discount before I continue"},
                )
            ],
            model="m",
        ),
        _publish_block("Ship changes", "the proposal", call_id="p1"),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="asked for both",
        ),
    ]
    # approval_gate wires the fake to the SAME route-resolution decision table the
    # real in-process request_approval tool runs (#561), so the policy flags
    # _merge_gate_block reconciles are set exactly as they are in production.
    runner, _session = _runner_over(script, gate=gate, approval_gate=gate)

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        events = parse_ndjson("".join(_run_lines(runner)))

    assert not any(
        e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
        for e in events
    ), "the turn ends on a real approval; nothing was lost to fail closed for"
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.status != SessionStatus.CLASSIFIED_FAILURE
    # A policy gate authorizes a business decision, never a tool (#544, A/C), so
    # it must not acquire a grant target from the publish call riding along.
    assert final.approval_gate_kind == "policy"
    assert final.approval_granted_tool is None
    assert "discount" in (final.approval_summary or "")
    assert any(
        "publication" in record.getMessage().lower()
        and "not carried" in record.getMessage().lower()
        for record in caplog.records
    ), caplog.text


def test_publish_call_with_no_terminal_result_still_fails_closed() -> None:
    # revert: drop the _publication_unrecorded_lines call from the ITERATOR-END
    # Final site in _drive_turn (keeping only the ResultMessage one) -> this fails
    # with status done. A turn whose generator simply ends without a terminal
    # result still emits a final, and an unrecordable publication must fail it
    # closed there too -- otherwise the silent hole #2294 closed reopens for every
    # turn the CLI ends without a ResultMessage.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = _managed_publish_gate()
    # No ResultMessage at all: receive_turn is exhausted after this message, which
    # is the second Final emission site in _drive_turn.
    runner, _session = _runner_over([_publish_block("   ", "no title")], gate=gate)

    events = parse_ndjson("".join(_run_lines(runner)))

    unrecorded = [
        e
        for e in events
        if e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
    ]
    assert len(unrecorded) == 1
    assert "title" in (unrecorded[0].message or "").lower()
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.CLASSIFIED_FAILURE
    assert final.status != SessionStatus.DONE
    assert final.approval_summary is None
    assert final.approval_granted_tool is None
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_operator_interrupt_outranks_an_unrecordable_publication() -> None:
    # revert: drop the interrupt/timeout guard from _publication_unrecorded_lines
    # -> this fails with classified-failure. A human pressing stop gets
    # idle-awaiting-input, not a failure about a request their own stop
    # prevented from ever being recorded; reporting it as a run failure would
    # trip F1's escalation path on an intentional stop.
    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    gate = _managed_publish_gate()
    runner, session = _runner_over(
        [
            _publish_block("   ", "no title"),
            AssistantMessage(content=[TextBlock(text="never delivered")], model="m"),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="s",
                result="never delivered",
            ),
        ],
        gate=gate,
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())  # first frame; the turn is live
        await runner.interrupt("user stop")  # the operator's own stop
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))

    # Not vacuous: the unrecordable call really was observed, so the ONLY thing
    # keeping this out of classified-failure is the operator's stop.
    assert session.interrupts >= 1
    assert gate.pending_summary is None
    assert not any(
        e.type == "error" and e.classification == PUBLICATION_UNRECORDED_CLASSIFICATION
        for e in events
    )
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT
