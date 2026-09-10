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
from typing import Any

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

from .approval import APPROVAL_TOOL_NAME, PUBLISH_TOOL_NAME, guard_reserved_summary
from .history import ConversationMessage
from .otel import _GenerationSpan
from .side_effects import SideEffectClassifier

# Longest raw tool reply this will parse into a recordable ``result``. The reply
# travels the wire and lands in a durable record, so an unbounded connector
# response would be an unbounded row; the existing analogue is the approval
# summary's 300-char input rendering, which is far tighter because a human reads
# it. A prior-state snapshot of a Kubernetes object is tens of kilobytes, so this
# is set to clear a realistic snapshot and refuse a blob.
RESULT_MAX_BYTES = 64_000


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
    # Bundle-authored human sentence (#2565). None means the card uses
    # approval_summary. Never grant provenance.
    approval_display: str | None = None
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
    # The delivered text of a clean DONE terminal ``final`` or an approval
    # suspension, set by the session loop. It is recorded with the structured
    # conversation transcript (#20/#1902); left None for failure, budget, auth,
    # and idle outcomes so those turns are not persisted as history.
    final_text: str | None = None
    # Ordered provider-neutral messages captured from the harness stream for
    # durable replay. The inbound user message is prepended by SessionRunner;
    # this list holds assistant output and user-shaped tool results.
    history_messages: list[ConversationMessage] = field(default_factory=list)
    # Call id -> tool name, for side-effecting calls whose result has not arrived
    # yet. A tool result lands on a LATER message, so the name has to be
    # remembered to attribute it, and the id is what joins the two frames of one
    # call into one record (ADR-0117). An entry is popped when its result
    # arrives; one left standing is a call that never came back.
    pending_actions: dict[str, str] = field(default_factory=dict)
    # Runner-internal, NOT a wire field (#1852): set by
    # ``SessionRunner._merge_gate_block`` when the runner's OWN approval gate
    # asked the CLI to stop the turn. A gated deny now carries the SDK's
    # turn-stopping flags, so the CLI aborts and its terminal result arrives
    # is_error-shaped -- which ``_translate_result`` classifies as a failure.
    # This flag is what lets ``_apply_approval_override`` flip a turn the CLI
    # reported as errored *because we interrupted it*, instead of reporting a
    # failure with nothing to approve.
    approval_halt_requested: bool = False
    # The raw ``ToolUseBlock.input`` of every publication call seen on the
    # stream this turn (#2294), in call order. Captured here, decided in
    # ``SessionRunner._observe_publication_calls``: this module stays pure and
    # never touches the ApprovalGate, so the same seam serves the live turn and
    # the offline fake. Not a rare path: against the real SDK the stream reaches
    # the session before the CLI dispatches PreToolUse, so this list is what
    # normally writes the record first and the hook's own block then finds it
    # standing. It is load-bearing for the case where neither SDK layer recorded
    # the call at all and the turn would otherwise finalize DONE with nothing to
    # approve.
    publication_calls: list[dict[str, Any]] = field(default_factory=list)
    # How many of ``publication_calls`` the session has already acted on, so the
    # observation runs exactly once per call even though it is invoked on every
    # message of the turn.
    publication_calls_observed: int = 0
    # Why a publication call could NOT be recorded (a malformed proposal, or a
    # gate that does not carry the publish tool). Runner-internal, never a wire
    # field: the session turns it into a fail-closed classified failure, because
    # a publication the runner cannot record must not look like a clean turn.
    publication_unrecorded: str | None = None


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
        return _translate_result(message, state)
    if isinstance(message, UserMessage):
        return _translate_user(message, state, gen)
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

    # Assistant usage is per-message. ResultMessage usage is a turn total and
    # must never be copied onto the last generation, where it would double-count
    # prior rounds.
    if gen is not None:
        gen.record_assistant(
            getattr(message, "model", None),
            getattr(message, "usage", None),
        )

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
                # The SDK block says only that a tool interval should be
                # inferred. It is not proof this runner executed the tool.
                gen.tool_use(block.id, block.name)
            if block.name == PUBLISH_TOOL_NAME:
                # Wire-level capture only (#2294). The session decides what to
                # do with it; recording it here would put gate state in a
                # deliberately pure module.
                state.publication_calls.append(
                    block.input if isinstance(block.input, dict) else {}
                )
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
                # One frame per CALL, not per turn. The cap was once-per-turn
                # because the only consumer was a boolean, so a turn that called
                # three mutating tools reported one; a consumer that records what
                # happened needs each call (ADR-0117). ``side_effect_emitted``
                # still latches below, so ADR-0013's no-retry rule and the
                # false-completion check read exactly the signal they read before.
                #
                # This frame is emitted when the call is MADE, before its result
                # exists, so a turn that dies mid-call has still reported that
                # something mutated. Its result arrives on a later message and
                # closes it in ``_translate_user``.
                state.pending_actions[block.id] = block.name
                events.append(
                    SideEffectFlag(
                        tool=block.name,
                        detail="non-idempotent tool executed",
                        call_id=block.id,
                        arguments=block.input if isinstance(block.input, dict) else None,
                    )
                )
                state.side_effect_emitted = True
    return events


def _translate_user(
    message: UserMessage,
    state: TurnState,
    gen: _GenerationSpan | None,
) -> list[OutboundEvent]:
    """Close a side-effecting call with what its tool answered, and nothing else.

    A tool result arrives on a ``UserMessage``, which the v0.1 contract dropped
    whole ("carry no outbound-visible content"). Read-only results stay dropped:
    file contents are the model's working material, and forwarding them would put
    a repository on the wire. A side-effecting call is different -- its reply is
    the only place a connector can report what the call did to the world, which is
    what makes the action recordable at all (ADR-0117).
    """

    if isinstance(message.content, str):
        # UserMessage.content is a plain string OR a block list. A string carries
        # no tool result, and iterating it would walk single characters.
        return []
    events: list[OutboundEvent] = []
    for block in message.content:
        if not isinstance(block, ToolResultBlock):
            continue
        if gen is not None:
            gen.tool_result(block.tool_use_id, failed=bool(block.is_error))
        tool = state.pending_actions.pop(block.tool_use_id, None)
        if tool is None:
            # Read-only, or a result for a call this turn never saw, or a second
            # result for a call already closed. There is nothing to attribute it
            # to, and minting a frame anyway would mint a second record.
            continue
        payload, oversized = _result_payload(block)
        events.append(
            SideEffectFlag(
                tool=tool,
                detail=(
                    "tool result too large to record"
                    if oversized
                    else "non-idempotent tool completed"
                ),
                call_id=block.tool_use_id,
                result=payload,
                failed=bool(block.is_error),
            )
        )
    return events


def _result_payload(block: ToolResultBlock) -> tuple[dict[str, object] | None, bool]:
    """The tool's structured reply, or None, plus whether size is why it is None.

    Deliberately not a parse of prose. A connector that answers in a sentence has
    no structured reply, and guessing one out of the sentence is how something
    downstream ends up restoring a guess. No JSON object means no result, which
    downstream means not undoable.
    """

    content = block.content
    if isinstance(content, list):
        # The SDK wraps an MCP tool's reply in content blocks.
        for part in content:
            if not isinstance(part, dict):
                continue
            parsed, oversized = _loads_object(part.get("text"))
            if parsed is not None or oversized:
                return parsed, oversized
        return None, False
    return _loads_object(content)


def _loads_object(raw: object) -> tuple[dict[str, object] | None, bool]:
    if not isinstance(raw, str):
        return None, False
    if len(raw.encode()) > RESULT_MAX_BYTES:
        # Dropped, never truncated. A truncated JSON object either fails to parse
        # or parses into a smaller object that is not what the tool said, and a
        # restore would act on the difference.
        return None, True
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None, False
    return (parsed if isinstance(parsed, dict) else None), False


def _translate_result(
    message: ResultMessage,
    state: TurnState,
) -> list[OutboundEvent]:
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
