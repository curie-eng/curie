"""The requester's reply when the runner's permission gate halts a live turn.

Drives the production Valkey consumer, the kernel, the real runner HTTP app and
its ``SessionRunner`` with the real ``ApprovalGate`` permission callback. Only
the model seam (``FakeModelSession``), the approval API and Slack are faked.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from claude_agent_sdk.types import ToolResultBlock, UserMessage
from curie_internal.turn_queue import to_stream_fields
from curie_runner.approval import ApprovalGate
from curie_runner.fake import FakeModelSession
from curie_runner.harness.claude.approval import build_can_use_tool
from curie_runner.otel import RunTracer
from curie_runner.server import create_app
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_worker.consumer import Consumer, refuse_unconfigured_reminder
from curie_worker.delivery_lease import DeliveryLeaseStore

from .test_approval_lifecycle import RecordingApprovals

sys.path.insert(0, str(Path(__file__).parent.parent))
from queue_fixtures import qevent  # noqa: E402

_GATED = "mcp__acme-access__grant_access"
_ARGS = {"principal": "U0EXAMPLE2", "resource": "sample-reports", "level": "read"}
# The model's own requester-facing sentence, written before the gated call.
_INTENT = "I'll give the requester read access to sample-reports once an administrator approves it."
# A raw tool result that also reads like a summary. It is never requester text.
_TOOL_RESULT = "Policy lookup: grants to sample-reports require administrator approval."
_SHORT_NOTICE = "Approval requested. See the card below."


def _assistant(*blocks: Any) -> AssistantMessage:
    return AssistantMessage(content=list(blocks), model="example-model")


def _script() -> list[Any]:
    # The CLI streams one assistant message per content block, so the sentence
    # and the gated call arrive as separate messages of one model response.
    return [
        _assistant(ToolUseBlock(id="t1", name="Read", input={"file_path": "policy.md"})),
        UserMessage(content=[ToolResultBlock(tool_use_id="t1", content=_TOOL_RESULT)]),
        _assistant(TextBlock(text=_INTENT)),
        _assistant(ToolUseBlock(id="t2", name=_GATED, input=dict(_ARGS))),
        # Never reached: the interrupting deny ends the turn (#1852).
        _assistant(TextBlock(text="unreachable")),
    ]


class _HaltedModel(FakeModelSession):
    """The fake model, with the terminal shape the halted CLI turn ends on."""

    def __init__(self, terminal: str, **kwargs: Any) -> None:
        super().__init__(_script, **kwargs)
        self._terminal = terminal

    async def receive_turn(self) -> AsyncIterator[Any]:
        async for message in super().receive_turn():
            if isinstance(message, ResultMessage) and self._terminal == "empty-success":
                # The shape the observed turn ended on: no terminal text.
                yield ResultMessage(
                    subtype="success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=False,
                    num_turns=1,
                    session_id="example-session",
                    result="",
                )
                continue
            yield message


async def _requester_reply(make_harness, terminal: str) -> tuple[str, Any]:
    """Run one halted turn end to end; return the requester's reply and the request."""

    gate = ApprovalGate(required=frozenset({_GATED}))
    model = _HaltedModel(terminal, can_use_tool=build_can_use_tool(gate))
    runner = SessionRunner(
        session_factory=lambda: model,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="halted-approval",
        ceiling=0,
        max_usd_per_day=None,
        held_secrets=frozenset(),
        approval_gate=gate,
    )
    approvals = RecordingApprovals()
    async with make_harness(runner_app=create_app(runner), approvals=approvals, shimmer=False) as h:
        event = qevent("Give me read access to sample-reports", placeholder="p-1")
        consumer = Consumer(
            redis=h.async_redis,
            kernel=h.kernel,
            config=h.config,
            leases=DeliveryLeaseStore(h.async_redis, h.config),
            reminders=refuse_unconfigured_reminder,
        )
        await consumer.ensure_group()
        consuming = asyncio.create_task(consumer.run())
        try:
            async with asyncio.timeout(10):
                await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
                while not await h.async_redis.exists(h.config.done_key(event.event_id)):
                    await asyncio.sleep(0)
        finally:
            consumer.request_stop()
            await asyncio.wait_for(consuming, 10)

        # The gate recorded the exact call, and the in-thread card describes it.
        [request] = approvals.requests
        assert request.gate_kind == "permission"
        assert request.granted_tool == _GATED
        assert request.granted_arguments == _ARGS
        assert _INTENT not in request.summary
        [(_, card, _, _, _)] = h.sink.posts
        assert card.interaction is not None and card.interaction.id == "appr-1"
        assert _INTENT not in (card.text or "")
        reply = [text for _, ref, text in h.sink.updates if ref == "p-1"][-1]
        assert reply.endswith(_SHORT_NOTICE), reply
        assert _TOOL_RESULT not in reply
        return reply, request


_TERMINALS = pytest.mark.parametrize("terminal", ["error-result", "empty-success"])


_FAILURE_TEXT_LEAKS = pytest.mark.xfail(
    strict=True,
    reason=(
        "The runner keeps the halted CLI's failure placeholder as the awaiting-approval "
        "final text, and the worker places it above the pause notice."
    ),
)


@pytest.mark.parametrize(
    "terminal",
    [pytest.param("error-result", marks=_FAILURE_TEXT_LEAKS), "empty-success"],
)
def test_halted_permission_pause_shows_no_failure_text(
    make_harness, thread_reset_keys, terminal: str
) -> None:
    async def scenario() -> None:
        reply, _ = await _requester_reply(make_harness, terminal)
        assert "run failed" not in reply, reply

    asyncio.run(scenario())


@_TERMINALS
@pytest.mark.xfail(
    strict=True,
    reason=(
        "ADR 0226 decision 1 drops text before the last top-level tool request, and the "
        "#1852 halt allows no text after the gated call, so ADR 0179 decision 2's "
        "model text before the pause cannot reach the requester. Needs a decision."
    ),
)
def test_halted_permission_pause_keeps_the_model_sentence_above_the_notice(
    make_harness, thread_reset_keys, terminal: str
) -> None:
    async def scenario() -> None:
        reply, _ = await _requester_reply(make_harness, terminal)
        assert reply.split("\n\n") == [_INTENT, _SHORT_NOTICE], reply

    asyncio.run(scenario())
