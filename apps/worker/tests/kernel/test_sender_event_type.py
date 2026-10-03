"""Queued job turns become Event type job (#3818)."""

from __future__ import annotations

from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
from curie_worker.kernel.core import Kernel


def _turn(source: TurnSource) -> QueuedTurn:
    return QueuedTurn(
        event_id="e-sender-type",
        conversation_id="th-sender-type",
        author="U1",
        text="hello",
        reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder="p-1"),
        received_at="2026-10-01T00:00:00+00:00",
        source=source,
    )


def test_cron_and_webhook_turns_are_jobs_and_slack_stays_a_message() -> None:
    cron = Kernel._to_event(_turn(TurnSource.CRON))
    webhook = Kernel._to_event(_turn(TurnSource.WEBHOOK))
    slack = Kernel._to_event(_turn(TurnSource.SLACK))
    assert cron.type == "job"
    assert webhook.type == "job"
    assert slack.type == "message"
