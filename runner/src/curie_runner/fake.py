"""A scripted ModelSession for tests and the conformance suite.

The fake is the mock at the adapter seam: it constructs real claude-agent-sdk
message dataclasses with canned content, so everything above it (translation,
budget, side-effect flagging, status, NDJSON, the HTTP layer) runs unmodified and
un-mocked while the model (the only external dependency) is replaced. It never
spawns the CLI or touches the network. ``aci-protocol`` is never mocked.
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator, Callable
from typing import Any

from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import CanUseTool, PermissionResultDeny, ToolPermissionContext

from .adapter import PartialMessageBoundary
from .approval import APPROVAL_TOOL_NAME, ApprovalGate, process_approval_request
from .history import ConversationMessage


def _assistant(*blocks: Any, usage: dict[str, Any] | None = None) -> AssistantMessage:
    return AssistantMessage(content=list(blocks), model="fake-model", usage=usage)


def _result(
    *,
    text: str = "",
    is_error: bool = False,
    subtype: str = "success",
    usage: dict[str, Any] | None = None,
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="fake-session",
        result=text,
        usage=usage,
    )


def _tool_result(tool_use_id: str, content: Any, *, is_error: bool = False) -> UserMessage:
    """A tool's answer, on the message the SDK actually delivers it on.

    Tool results arrive on a ``UserMessage``, not on the assistant turn, which is
    why forwarding them is a separate branch in translation. A fake that skips
    this message leaves that branch unreachable offline.
    """

    return UserMessage(
        content=[ToolResultBlock(tool_use_id=tool_use_id, content=content, is_error=is_error)]
    )


def default_turn() -> list[Any]:
    """A representative successful turn: text, a side-effecting tool, then done.

    The tool answers in prose, because ``echo hi`` has nothing else to say. That
    is the honest default and it is also the not-undoable path: no structured
    reply means no prior state, so a recorded action reports that it cannot be
    put back rather than claiming it can.
    """

    return [
        _assistant(TextBlock(text="Looking into it")),
        _assistant(ToolUseBlock(id="t1", name="Bash", input={"command": "echo hi"})),
        _tool_result("t1", "hi\n"),
        _assistant(TextBlock(text="all done"), usage={"input_tokens": 20, "output_tokens": 8}),
        _result(text="all done", usage={"input_tokens": 20, "output_tokens": 8}),
    ]


# The other half of a write connector's contract: a reply that reports what it
# read and what it left, so an offline run can exercise a reversible action end
# to end (ADR-0117). Selected by marker like the approval turn below, because the
# default turn's honest answer is prose and both paths need to exist offline.
REVERSIBLE_MARKER = "[fake:reversible-tool]"

_REVERSIBLE_REPLY = {
    "ok": True,
    "summary": "scaled public/api from 3 to 10",
    # What a restore puts back, and what it is checked against. Not
    # interchangeable: comparing the live resource to ``prior`` would refuse
    # every safe undo and permit the one that is not.
    "prior": {"spec": {"replicas": 3}},
    "post": {"spec": {"replicas": 10}},
    "target": {"kind": "Deployment", "namespace": "public", "name": "api"},
}


def reversible_turn() -> list[Any]:
    """A turn whose side-effecting call reports a state that can be restored."""

    return [
        _assistant(TextBlock(text="Scaling it")),
        _assistant(
            ToolUseBlock(
                id="t1",
                name="scale_deployment",
                input={"namespace": "public", "name": "api", "replicas": 10},
            )
        ),
        _tool_result("t1", json.dumps(_REVERSIBLE_REPLY)),
        _assistant(
            TextBlock(text="scaled public/api from 3 to 10"),
            usage={"input_tokens": 20, "output_tokens": 8},
        ),
        _result(
            text="scaled public/api from 3 to 10",
            usage={"input_tokens": 20, "output_tokens": 8},
        ),
    ]


# Explicit test-only marker: a query containing it makes the fake's default
# script raise an approval request (ADR-0010), so the awaiting-approval
# lifecycle round-trips fully offline (CI, `curie skill up --fake-model`, the
# chart's sealed default pool). Everything after the marker on the same line is
# the approval summary; the routed form ``[fake:request-approval:managers]``
# additionally names an approval route (#247). Like all fake-model behavior:
# no model call, no network.
APPROVAL_MARKER = "[fake:request-approval]"
_APPROVAL_MARKER_RE = re.compile(r"\[fake:request-approval(?::([A-Za-z0-9_-]+))?\]")


def approval_turn(summary: str, route: str | None = None) -> list[Any]:
    """A turn that calls the platform approval-request tool, then ends."""

    text = "This needs sign-off; requesting approval."
    payload: dict[str, Any] = {"summary": summary}
    if route is not None:
        payload["route"] = route
    return [
        _assistant(TextBlock(text=text)),
        _assistant(
            ToolUseBlock(
                id="t1",
                name="mcp__curie__request_approval",
                input=payload,
            )
        ),
        _result(text=text, usage={"input_tokens": 20, "output_tokens": 8}),
    ]


class FakeModelSession:
    """A ModelSession that replays a fixed script of SDK messages per turn.

    ``script_factory`` returns the messages for the next ``receive_turn``, so a
    test can vary the script across turns. ``interrupt`` truncates the current
    turn's replay at the next boundary (``truncate_on_interrupt=True``, the
    default), emulating an SDK interrupt that aborts the iterator before a result;
    set it False to model the other real shape, where the SDK still delivers a
    terminal error result after the interrupt.

    ``emit_partial_boundaries`` is an opt-in telemetry fixture. When enabled, a
    payload-free message-start boundary immediately precedes each scripted
    assistant message. The default remains the historical exact script replay.

    ``can_use_tool`` (the #245 permission gate) lets the offline path exercise the
    same intercept the SDK applies: before each scripted ``ToolUseBlock`` is
    yielded, the permission callback runs and its DECISION is honored -- a gated
    tool's deny records the block on the shared gate (flipping the turn to
    awaiting-approval), and a deny carrying ``interrupt=True`` additionally ends
    the replay, the same way the real CLI aborts the turn (#1852; see
    ``_apply_gate``). The block itself is delivered unchanged -- matching the
    real SDK, which emits the ``tool_use`` even for a denied call -- so a gated
    turn still surfaces the tool note it would in production. Defaults None, so
    a fake constructed without it behaves exactly as before. Bundle PreToolUse
    command hooks (#272) are NOT run here: they shell out and would break the
    fake's offline no-op guarantee.
    """

    def __init__(
        self,
        script_factory: Callable[[], list[Any]] | None = None,
        *,
        truncate_on_interrupt: bool = True,
        can_use_tool: CanUseTool | None = None,
        approval_gate: ApprovalGate | None = None,
        replay_messages: tuple[ConversationMessage, ...] = (),
        emit_partial_boundaries: bool = False,
        disallowed_tools: list[str] | tuple[str, ...] | None = None,
    ) -> None:
        self._script_factory = script_factory or self._default_script
        self._truncate_on_interrupt = truncate_on_interrupt
        self._can_use_tool = can_use_tool
        self._emit_partial_boundaries = emit_partial_boundaries
        self._disallowed_tools = tuple(disallowed_tools or ())
        # The shared policy gate (#561): a scripted request_approval block must
        # run the SAME route-resolution decision table the real MCP tool does, or
        # the fake tier omits the sole-route auto-bind / unknown-route refusal and
        # silently widens the card -- the exact real-path regression #544 closed.
        self._approval_gate = approval_gate
        self.replay_messages = replay_messages
        self.connected = False
        self.queries: list[str] = []
        self.interrupts = 0
        self._interrupted = False
        # Set when the permission callback denies WITH interrupt=True (#1852):
        # the real CLI aborts the turn on that flag, so the replay must stop too
        # or the offline tier silently models a gate weaker than production's.
        # Strictly per-turn -- reset in ``query`` beside ``_interrupted``, so a
        # halt can never truncate an unrelated later replay.
        self._halted = False

    def _default_script(self) -> list[Any]:
        """The default per-turn script, branching on the approval marker.

        A custom ``script_factory`` bypasses this entirely, so existing tests
        keep their exact scripts; only the no-factory default (the container
        fake-model path) reacts to the marker.
        """

        last = self.queries[-1] if self.queries else ""
        match = _APPROVAL_MARKER_RE.search(last)
        if match:
            summary = last[match.end() :].strip() or "unspecified request"
            return approval_turn(summary, route=match.group(1))
        if REVERSIBLE_MARKER in last:
            return reversible_turn()
        return default_turn()

    async def connect(self) -> None:
        self.connected = True

    async def query(self, text: str) -> None:
        self.queries.append(text)
        self._interrupted = False
        self._halted = False

    async def interrupt(self) -> None:
        self.interrupts += 1
        self._interrupted = True

    async def receive_turn(self) -> AsyncIterator[Any]:
        for message in self._script_factory():
            if self._interrupted and self._truncate_on_interrupt:
                return
            await self._apply_gate(message)
            if self._emit_partial_boundaries and isinstance(message, AssistantMessage):
                yield PartialMessageBoundary(event_type="message_start")
            yield message
            if self._halted:
                # The denied ToolUseBlock above IS delivered (the real SDK emits
                # the tool_use even for a denied call), and nothing after it is:
                # an interrupting deny aborts the turn, so any scripted text or
                # terminal result that followed never happens (#1852). This is a
                # separate mechanism from the ``_interrupted`` truncation above,
                # which models an OPERATOR stop and must keep working on its own.
                return

    async def _apply_gate(self, message: Any) -> None:
        """Run the permission gate over each ToolUseBlock and honor its decision.

        Mirrors the SDK: the gate decides a call before it executes, and a gated
        deny records the block on the shared ``ApprovalGate`` (flipping the turn to
        awaiting-approval). The block is delivered unchanged either way -- the real
        SDK emits the ``tool_use`` before the permission decision, so a denied call
        still surfaces as a tool note. A no-op unless a gate is configured, keeping
        the un-gated fake unchanged.

        The callback's return value is READ, not discarded (#1852): a
        ``PermissionResultDeny`` with ``interrupt=True`` is forwarded by the SDK
        to the CLI as ``response_data["interrupt"]``
        (``claude_agent_sdk/_internal/query.py:474-477``), which aborts the turn.
        Recording it here is what lets the offline tier prove the halt rather
        than replay past a call production would have stopped on. A deny WITHOUT
        the flag is unchanged: the replay continues and the scripted terminal
        result still arrives.
        """

        if not isinstance(message, AssistantMessage):
            return
        for block in message.content:
            if not isinstance(block, ToolUseBlock):
                continue
            if block.name == APPROVAL_TOOL_NAME and self._approval_gate is not None:
                # Run the real decision table so the container fake tier resolves
                # the route (sole-route auto-bind, unknown-route refusal) and sets
                # the same sticky gate flags _merge_gate_block reconciles (#561).
                # build_can_use_tool below leaves the (un-gated) approval tool
                # untouched, so this is the only thing that sets the policy fields.
                payload = block.input if isinstance(block.input, dict) else {}
                process_approval_request(self._approval_gate, payload)
            if block.name in self._disallowed_tools:
                # Mirror ClaudeAgentOptions.disallowed_tools: the named tool is
                # not usable. The ToolUseBlock is still delivered (the real SDK
                # emits it), then the turn stops so the scripted result cannot
                # execute the write (#2429).
                self._halted = True
                continue
            if self._can_use_tool is not None:
                decision = await self._can_use_tool(
                    block.name, block.input, ToolPermissionContext()
                )
                if isinstance(decision, PermissionResultDeny) and decision.interrupt:
                    self._halted = True

    async def close(self) -> None:
        self.connected = False
