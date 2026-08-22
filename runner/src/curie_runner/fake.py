"""A scripted ModelSession for tests and the conformance suite.

The fake is the mock at the adapter seam: it constructs real claude-agent-sdk
message dataclasses with canned content, so everything above it (translation,
budget, side-effect flagging, status, NDJSON, the HTTP layer) runs unmodified and
un-mocked while the model (the only external dependency) is replaced. It never
spawns the CLI or touches the network. ``aci-protocol`` is never mocked.
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator, Callable
from typing import Any

import aiohttp
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)
from claude_agent_sdk.types import CanUseTool, ToolPermissionContext

from .approval import APPROVAL_TOOL_NAME, ApprovalGate, process_approval_request
from .delegate import DELEGATE_SERVER_NAME, DelegateApiClient, DelegateError


def _assistant(*blocks: Any, usage: dict[str, Any] | None = None) -> AssistantMessage:
    return AssistantMessage(content=list(blocks), model="fake-model", usage=usage)


logger = logging.getLogger(__name__)


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


def default_turn() -> list[Any]:
    """A representative successful turn: text, a side-effecting tool, then done."""

    return [
        _assistant(TextBlock(text="Looking into it")),
        _assistant(ToolUseBlock(id="t1", name="Bash", input={"command": "echo hi"})),
        _assistant(TextBlock(text="all done"), usage={"input_tokens": 20, "output_tokens": 8}),
        _result(text="all done", usage={"input_tokens": 20, "output_tokens": 8}),
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


# PROTOTYPE (Draft ADR-0115, not accepted -- docs/demo/ADR-0115-PROTOTYPE-NOTES.md).
# The offline fake never runs a real model, so it cannot "decide" to call
# curie-delegate the way a live model reading its tool description would.
# Mirrors APPROVAL_MARKER exactly: a marker in the query text stands in for
# that decision so the demo (`curie local` with no model credential) can
# exercise the REAL delegate router/MCP-server/reply-sink code path end to
# end, offline. Everything after the marker on the same line is the message
# sent to the target agent.
DELEGATE_MARKER = "[fake:delegate:<target-agent>]"
_DELEGATE_MARKER_RE = re.compile(r"\[fake:delegate:([A-Za-z0-9_-]+)\]")
DELEGATE_TOOL_NAME = f"mcp__{DELEGATE_SERVER_NAME}__call_agent"


def delegate_turn(target_agent: str, message: str) -> list[Any]:
    """A turn that calls the (prototype) curie-delegate tool, then ends."""

    text = f"Asking {target_agent} about that."
    return [
        _assistant(TextBlock(text=text)),
        _assistant(
            ToolUseBlock(
                id="t1",
                name=DELEGATE_TOOL_NAME,
                input={"target_agent": target_agent, "message": message},
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

    ``can_use_tool`` (the #245 permission gate) lets the offline path exercise the
    same intercept the SDK applies: before each scripted ``ToolUseBlock`` is
    yielded, the permission callback runs for its side effect (a gated tool's deny
    records the block on the shared gate, flipping the turn to awaiting-approval).
    The block itself is delivered unchanged -- matching the real SDK, which emits
    the ``tool_use`` even for a denied call -- so a gated turn still surfaces the
    tool note it would in production. Defaults None, so a fake constructed without
    it behaves exactly as before. Bundle PreToolUse command hooks (#272) are NOT
    run here: they shell out and would break the fake's offline no-op guarantee.
    """

    def __init__(
        self,
        script_factory: Callable[[], list[Any]] | None = None,
        *,
        truncate_on_interrupt: bool = True,
        can_use_tool: CanUseTool | None = None,
        approval_gate: ApprovalGate | None = None,
        delegate_client: DelegateApiClient | None = None,
    ) -> None:
        self._script_factory = script_factory or self._default_script
        self._truncate_on_interrupt = truncate_on_interrupt
        self._can_use_tool = can_use_tool
        # The shared policy gate (#561): a scripted request_approval block must
        # run the SAME route-resolution decision table the real MCP tool does, or
        # the fake tier omits the sole-route auto-bind / unknown-route refusal and
        # silently widens the card -- the exact real-path regression #544 closed.
        self._approval_gate = approval_gate
        # PROTOTYPE (Draft ADR-0115): when set, a scripted DELEGATE_TOOL_NAME
        # block makes a REAL call through it (an actual HTTP POST to the
        # prototype delegate route), the same "fake decision, real side effect"
        # shape as request_approval above.
        self._delegate_client = delegate_client
        self.connected = False
        self.queries: list[str] = []
        self.interrupts = 0
        self._interrupted = False

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
        delegate_match = _DELEGATE_MARKER_RE.search(last)
        if delegate_match:
            message = last[delegate_match.end() :].strip() or "hello"
            return delegate_turn(delegate_match.group(1), message)
        return default_turn()

    async def connect(self) -> None:
        self.connected = True

    async def query(self, text: str) -> None:
        self.queries.append(text)
        self._interrupted = False

    async def interrupt(self) -> None:
        self.interrupts += 1
        self._interrupted = True

    async def receive_turn(self) -> AsyncIterator[Any]:
        for message in self._script_factory():
            if self._interrupted and self._truncate_on_interrupt:
                return
            await self._apply_gate(message)
            yield message

    async def _apply_gate(self, message: Any) -> None:
        """Run the permission gate over each ToolUseBlock for its side effect.

        Mirrors the SDK: the gate decides a call before it executes, and a gated
        deny records the block on the shared ``ApprovalGate`` (flipping the turn to
        awaiting-approval). The block is delivered unchanged either way -- the real
        SDK emits the ``tool_use`` before the permission decision, so a denied call
        still surfaces as a tool note. A no-op unless a gate is configured, keeping
        the un-gated fake unchanged.
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
            if block.name == DELEGATE_TOOL_NAME and self._delegate_client is not None:
                payload = block.input if isinstance(block.input, dict) else {}
                target_agent = payload.get("target_agent")
                message = payload.get("message")
                if isinstance(target_agent, str) and isinstance(message, str):
                    try:
                        await self._delegate_client.call_agent(target_agent, message)
                    except (DelegateError, aiohttp.ClientError) as exc:
                        logger.warning("fake-tier delegate call failed: %s", exc)
            if self._can_use_tool is not None:
                await self._can_use_tool(block.name, block.input, ToolPermissionContext())

    async def close(self) -> None:
        self.connected = False
