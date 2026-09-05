"""Run the missing-authority guard through the actual kernel and Valkey.

This is a bounded negative plus ordinary-turn control. It does not substitute
for the full real API/GitHub/cluster continuation acceptance journey.
"""

import asyncio
import uuid
from datetime import UTC, datetime

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta


def test_review_event_without_trusted_before_model_verifier_never_reaches_runner(
    make_harness,
) -> None:
    async def exercise() -> None:
        async with make_harness() as h:
            turn = QueuedTurn(
                event_id=f"github-feedback-{uuid.uuid4()}",
                conversation_id="1700000000.000001",
                author="github:41:example-reviewer",
                text="Please add the missing regression test.",
                reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder=None),
                received_at=datetime.now(UTC).isoformat(),
            )
            h.runner.turn_scripts.append(
                [
                    TextDelta(text="MODEL_MUST_NOT_RUN"),
                    Final(text="done", status=SessionStatus.DONE),
                ]
            )
            await h.kernel.process_event(turn)
            assert len(h.runner.opened) == 0
            assert any(
                "GitHub feedback" in update[2] for update in h.sink.updates + h.sink.text_posts
            )
            healthy = turn.model_copy(
                update={"event_id": f"Ev-{uuid.uuid4()}", "author": "U0REQUEST1"}
            )
            await h.kernel.process_event(healthy)
            assert len(h.runner.opened) == 1

    asyncio.run(exercise())
