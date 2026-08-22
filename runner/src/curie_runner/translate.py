"""Translate claude-agent-sdk messages into ACI outbound events.

This is the pure mapping at the heart of the runner: it turns each SDK message
(assistant text, tool calls, terminal result, rate-limit signal) into zero or
more ACI outbound events (text_delta / tool_note / side_effect_flag / error /
final). It is stateful only through ``TurnState`` (side-effect dedup, carried
error classification) and side-effect free otherwise, so it is unit-testable
without a session, a network, or the HTTP layer.

Budget and interrupt outcomes are *not* decided here: this layer reports the
model's own terminal status (done vs classified-failure), and the session applies
budget/interrupt overrides on top. Keeping that split is what lets the same
translation serve both the live HTTP turn and the conformance producer.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field

from aci_protocol import (
    ErrorEvent,
    Final,
    OutboundEvent,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
)
from claude_agent_sdk import (
    AssistantMessage,
    RateLimitEvent,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from .approval import APPROVAL_TOOL_NAME, guard_reserved_summary
from .otel import _GenerationSpan
from .side_effects import SideEffectClassifier


@dataclass
class TurnState:
    """Mutable per-turn state threaded through translation."""

    side_effect_emitted: bool = False
    error_classification: str | None = None
    # The summary passed to the approval-request tool (ADR-0010), captured off
    # the ToolUseBlock so the session can end the turn awaiting-approval. None
    # when no approval was requested this turn.
    approval_summary: str | None = None
    # The approval route the request named (#247): a manifest-declared route
    # the platform binds to a channel per deployment. None routes to the
    # requesting channel.
    approval_route: str | None = None
    # Durable gate provenance (#544, Decision C). ``approval_gate_kind`` is
    # 'policy' when the model asked for a business-decision approval and
    # 'permission' when the runner's tool gate denied a real tool call (merged
    # from the ApprovalGate in the session). ``approval_granted_tool`` is the
    # trusted tool name a permission gate authorizes for the resume turn; a
    # policy gate never carries one (Decision A), so it stays None here.
    approval_gate_kind: str | None = None
    approval_granted_tool: str | None = None
    # Assistant text streamed during the turn, accumulated so a DONE result with
    # an empty ``result`` can still deliver the model's answer. Reasoning models
    # routed through OpenRouter (e.g. z-ai/glm-5.2) emit the answer as a TextBlock
    # but their empty-signature thinking block trips the SDK's result extraction,
    # leaving ``ResultMessage.result`` empty (issue #107).
    assistant_text: str = ""
    # Count of ALL tool calls this turn (every ToolUseBlock), the evidence signal
    # the false-completion check keys on (#517). Distinct from
    # ``side_effect_emitted``, which flips only for non-idempotent tools: a
    # read-only investigation (Read/Grep/WebSearch) IS tool-call evidence but
    # leaves ``side_effect_emitted`` False, so this counter -- not that flag -- is
    # the right "did any tool run" signal.
    tool_call_count: int = 0
    # The delivered text of the terminal ``final`` for a successful turn, set by
    # the session loop when a DONE/idle final is produced. It is the assistant
    # reply recorded into the conversation transcript (#20); left None on a
    # failure/budget/auth final so those turns are not persisted as history.
    final_text: str | None = None
    # Tool-use id -> tool name, for side-effecting calls whose result has not
    # arrived yet. A tool result lands on a LATER message, so the name has to be
    # remembered to attribute it. Entries are popped as results arrive; a call
    # whose result never comes simply leaves its argument-only record standing,
    # which is the honest outcome for a turn that died mid-call.
    pending_actions: dict[str, str] = field(default_factory=dict)


def translate_message(
    message: object,
    state: TurnState,
    classifier: SideEffectClassifier,
    gen: _GenerationSpan | None,
) -> list[OutboundEvent]:
    """Map one SDK message to the ACI outbound events it produces."""

    if isinstance(message, AssistantMessage):
        return _translate_assistant(message, state, classifier, gen)
    if isinstance(message, ResultMessage):
        return _translate_result(message, state, gen)
    if isinstance(message, UserMessage):
        return _translate_user(message, state)
    if isinstance(message, RateLimitEvent):
        # status is one of allowed / allowed_warning / rejected; only a hard
        # rejection is an ACI error. The warning states are advisory (the model
        # is still allowed to continue) and must not inject a failure event into
        # an otherwise-successful run.
        if message.rate_limit_info.status == "rejected":
            state.error_classification = "rate-limit"
            return [ErrorEvent(message="model rate limit reached", classification="rate-limit")]
        return []
    # UserMessage, SystemMessage, and StreamEvent carry no outbound-visible
    # content in the v0.1 contract; they are intentionally dropped.
    return []


def _translate_assistant(
    message: AssistantMessage,
    state: TurnState,
    classifier: SideEffectClassifier,
    gen: _GenerationSpan | None,
) -> list[OutboundEvent]:
    events: list[OutboundEvent] = []

    # Backfill the generation model from the SDK's own report when CURIE_MODEL
    # was unset at span open (record_model no-ops once a model is already stamped).
    if gen is not None:
        gen.record_model(getattr(message, "model", None))

    error = getattr(message, "error", None)
    if error:
        state.error_classification = error
        events.append(ErrorEvent(message=f"model error: {error}", classification=error))

    for block in message.content:
        if isinstance(block, TextBlock):
            if block.text:
                state.assistant_text += block.text
                events.append(TextDelta(text=block.text))
        elif isinstance(block, ToolUseBlock):
            events.append(ToolNote(text=f"running tool {block.name}", tool=block.name))
            # Every tool call is evidence for the false-completion check (#517),
            # including the approval-request tool below and read-only tools.
            state.tool_call_count += 1
            if gen is not None:
                gen.tool_span(block.name)
            if block.name == APPROVAL_TOOL_NAME:
                # A policy gate fired (ADR-0010). Capture the summary (and the
                # optional route, #247) at the wire level so the real path
                # (executed in-process tool) and the fake path (scripted
                # ToolUseBlock) exercise one seam.
                payload = block.input if isinstance(block.input, dict) else {}
                summary = str(payload.get("summary") or "").strip()
                if summary:
                    # The summary is the model's own argument (attacker-
                    # influenced). Guard it out of the reserved permission-gate
                    # namespace so it can never masquerade as a genuine
                    # can_use_tool denial the worker would grant a bypass for
                    # (#430, ADR-0035).
                    state.approval_summary = guard_reserved_summary(summary)
                    route = str(payload.get("route") or "").strip()
                    state.approval_route = route or None
                    # A policy gate authorizes a business decision, never a tool
                    # (#544, Decision A): stamp the provenance and leave
                    # approval_granted_tool None so the worker can never mint a
                    # bypass grant from a model-authored request (#430).
                    state.approval_gate_kind = "policy"
            if classifier.is_side_effecting(block.name):
                # One flag per CALL, not per turn. The no-retry rule only needs
                # to know that something mutated, and ``side_effect_emitted``
                # still latches for it below; a consumer that records what
                # happened needs each call, and the arguments are in hand here.
                state.pending_actions[block.id] = block.name
                events.append(
                    SideEffectFlag(
                        tool=block.name,
                        detail="non-idempotent tool executed",
                        arguments=block.input if isinstance(block.input, dict) else None,
                    )
                )
                state.side_effect_emitted = True
    return events


def _translate_user(message: UserMessage, state: TurnState) -> list[OutboundEvent]:
    """Forward the RESULT of a side-effecting call, and nothing else.

    A tool result arrives on a UserMessage, which the v0.1 contract dropped
    whole. Read-only results stay dropped: they are the model's working material
    and forwarding them would put file contents on the wire. A side-effecting
    call is different -- its result is the only place a connector can report what
    the call did to the world, which is what makes an action recordable.
    """

    events: list[OutboundEvent] = []
    for block in message.content:
        if not isinstance(block, ToolResultBlock):
            continue
        tool = state.pending_actions.pop(block.tool_use_id, None)
        if tool is None:
            # Not a side-effecting call, or a result for a call this turn never
            # saw. Either way there is nothing to attribute it to.
            continue
        events.append(
            SideEffectFlag(tool=tool, detail="tool result", result=_result_payload(block))
        )
    return events


def _result_payload(block: ToolResultBlock) -> dict[str, object] | None:
    """A structured reply, or None. A connector that answers in prose has none.

    Deliberately not a parse of prose: guessing structure out of a sentence is
    how a ledger ends up holding something a restore would act on. No JSON
    object means no structured result, and downstream that means not undoable.
    """

    content = block.content
    if isinstance(content, dict):
        return content
    if isinstance(content, list):
        # The SDK may wrap a text payload in content blocks.
        for part in content:
            text = part.get("text") if isinstance(part, dict) else None
            parsed = _loads_object(text)
            if parsed is not None:
                return parsed
        return None
    return _loads_object(content)


def _loads_object(raw: object) -> dict[str, object] | None:
    if not isinstance(raw, str):
        return None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _translate_result(
    message: ResultMessage,
    state: TurnState,
    gen: _GenerationSpan | None,
) -> list[OutboundEvent]:
    if gen is not None:
        gen.record_usage(message.usage)

    subtype = message.subtype or ""
    if message.is_error or subtype.startswith("error"):
        text = message.result or "run failed"
        events: list[OutboundEvent] = []
        if state.error_classification is None:
            events.append(
                ErrorEvent(message=text, classification=subtype or "server-error")
            )
        events.append(Final(text=text, status=SessionStatus.CLASSIFIED_FAILURE))
        return events

    # The SDK's ``result`` is authoritative when present. When it is empty on an
    # otherwise-successful turn, fall back to the assistant text streamed this turn
    # so a reasoning model whose result-extraction returned empty (issue #107)
    # still delivers its answer. Provider-agnostic: it only fires when result is
    # empty, so non-reasoning models and the fake-model path are unaffected.
    #
    # Stamp the turn's token usage on the successful final (#390) so a consumer
    # (the eval runner) can attribute a dollar cost to the turn. Only the clean
    # DONE final carries usage: a classified failure never grades, and the
    # interrupt/approval overrides in the session reconstruct their own final.
    input_tokens, output_tokens = _usage_tokens(message.usage)
    return [
        Final(
            text=message.result or state.assistant_text,
            status=SessionStatus.DONE,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
    ]


def _usage_tokens(usage: object) -> tuple[int | None, int | None]:
    """Read ``input_tokens``/``output_tokens`` off the SDK result's usage block.

    The SDK reports usage as a mapping (the Anthropic wire shape); a missing
    block or a non-integer value yields ``None`` so the wire never carries a
    fabricated count. Cache-token fields are deliberately not surfaced here --
    the eval cost model prices prompt/completion tokens only, and each eval case
    runs a fresh conversation (little cache benefit to attribute).
    """
    if not isinstance(usage, Mapping):
        return None, None

    def _int(key: str) -> int | None:
        value = usage.get(key)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    return _int("input_tokens"), _int("output_tokens")
