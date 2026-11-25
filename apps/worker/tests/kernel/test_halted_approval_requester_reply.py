"""The requester's reply when the runner's permission gate halts a live turn.

Drives the production Valkey consumer, the kernel, the real runner HTTP app and
its ``SessionRunner`` with the real ``ApprovalGate`` permission callback. Only
the model seam (``FakeModelSession``), the approval API and Slack are faked.
ADR 0295: the blocked call's own model response is the requester's text above
the notice, and the aborted CLI result's failure text never is.
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
# An earlier model response of the same turn. It is not the blocked call's.
_EARLIER = "Checking the access policy first."
# A raw tool result that also reads like a summary. It is never requester text.
_TOOL_RESULT = "Policy lookup: grants to sample-reports require administrator approval."
# Subagent text that even carries the blocked response's id. Nested, so excluded.
_NESTED = "Subagent notes: the principal is U0EXAMPLE2."
# The model's own requester-facing sentence, in the blocked call's response.
_INTENT = "I'll give the requester read access to sample-reports once an administrator approves it."
# What a provider result could carry on a halted turn that still ended cleanly.
_RESULT = "An administrator needs to approve the read access first."
_SHORT_NOTICE = "Approval requested. See the card below."


def _assistant(*blocks: Any, message_id: str, parent: str | None = None) -> AssistantMessage:
    return AssistantMessage(
        content=list(blocks),
        model="example-model",
        parent_tool_use_id=parent,
        message_id=message_id,
    )


def _script() -> list[Any]:
    # The CLI streams one assistant message per content block, each carrying
    # its provider response's message id.
    return [
        _assistant(TextBlock(text=_EARLIER), message_id="msg-1"),
        _assistant(
            ToolUseBlock(id="t1", name="Read", input={"file_path": "policy.md"}),
            message_id="msg-1",
        ),
        UserMessage(content=[ToolResultBlock(tool_use_id="t1", content=_TOOL_RESULT)]),
        _assistant(TextBlock(text=_NESTED), message_id="msg-2", parent="t1"),
        _assistant(TextBlock(text=_INTENT), message_id="msg-2"),
        _assistant(ToolUseBlock(id="t2", name=_GATED, input=dict(_ARGS)), message_id="msg-2"),
        # Never reached: the interrupting deny ends the turn (#1852).
        _assistant(TextBlock(text="unreachable"), message_id="msg-3"),
    ]


def _success(text: str) -> ResultMessage:
    return ResultMessage(
        subtype="success",
        duration_ms=1,
        duration_api_ms=1,
        is_error=False,
        num_turns=1,
        session_id="example-session",
        result=text,
    )


class _HaltedModel(FakeModelSession):
    """The fake model, with the terminal shape the halted CLI turn ends on."""

    def __init__(self, terminal: str, **kwargs: Any) -> None:
        super().__init__(_script, **kwargs)
        self._terminal = terminal

    async def receive_turn(self) -> AsyncIterator[Any]:
        async for message in super().receive_turn():
            if isinstance(message, ResultMessage) and self._terminal != "error-result":
                # The fake models the CLI's error-shaped abort; the observed
                # turn ended success-shaped, with or without result text.
                yield _success(_RESULT if self._terminal == "nonempty-success" else "")
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
        for leaked in (_TOOL_RESULT, _EARLIER, _NESTED, "U0EXAMPLE2", "unreachable", "run failed"):
            assert leaked not in reply, (leaked, reply)
        return reply, request


@pytest.mark.parametrize("terminal", ["error-result", "empty-success"])
def test_halted_permission_pause_puts_the_model_sentence_above_the_notice(
    make_harness, thread_reset_keys, terminal: str
) -> None:
    async def scenario() -> None:
        reply, _ = await _requester_reply(make_harness, terminal)
        assert reply.split("\n\n") == [_INTENT, _SHORT_NOTICE], reply

    asyncio.run(scenario())


def test_halted_permission_pause_keeps_a_nonempty_result(make_harness, thread_reset_keys) -> None:
    async def scenario() -> None:
        reply, _ = await _requester_reply(make_harness, "nonempty-success")
        assert reply.split("\n\n") == [_RESULT, _SHORT_NOTICE], reply

    asyncio.run(scenario())
