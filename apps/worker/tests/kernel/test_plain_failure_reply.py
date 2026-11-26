"""A failed turn reaches a person as one plain sentence (#5984).

The kernel escalates a failed turn as a marked record (#3401). Delivered through
the production ``ReplySinkRouter``, a chat channel receives a plain sentence
chosen by failure class, while the cluster message relay, which only an API key
holder reads, still receives the marked record. The record stays in the log.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus
from channel_protocol.reply import ReplyAck, ReplyEvent, ReplyUpdate
from curie_worker import failure_notice
from curie_worker.reply_sink import CLUSTER_MESSAGE_ADAPTER, ReplySinkRouter, TargetRoute

sys.path.insert(0, str(Path(__file__).parent.parent))
from queue_fixtures import qevent  # noqa: E402

FAIL = SessionStatus.CLASSIFIED_FAILURE


class _Recorder:
    """Stands in for one egress adapter: the provider is the only fake."""

    def __init__(self) -> None:
        self.events: list[ReplyEvent] = []

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        del route, best_effort_unreachable
        self.events.append(event)
        return ReplyAck(ref=None)

    def last_text(self) -> str | None:
        texts = [e.text for e in self.events if isinstance(e, ReplyUpdate) and e.text]
        return texts[-1] if texts else None


def _router() -> tuple[ReplySinkRouter, _Recorder, _Recorder]:
    channel, relay = _Recorder(), _Recorder()
    return (
        ReplySinkRouter(adapters={"slack": channel}, default=channel, cluster_message=relay),
        channel,
        relay,
    )


@pytest.mark.parametrize(
    ("classification", "expected"),
    [
        pytest.param("budget-exceeded", failure_notice.TOO_LARGE, id="output-budget"),
        pytest.param("server-error", failure_notice.GENERIC, id="other-class"),
    ],
)
def test_channel_person_gets_a_plain_sentence(
    make_harness, caplog, classification: str, expected: str
) -> None:
    async def go() -> None:
        router, channel, relay = _router()
        async with make_harness(sink=router) as h:
            h.runner.default_script = [
                ErrorEvent(message="detail for operators", classification=classification),
                Final(text="failed", status=FAIL),
            ]
            with caplog.at_level(logging.WARNING):
                await h.kernel.process_event(qevent("go", event_id="evt-plain-failure"))

        reply = channel.last_text()
        assert reply == expected
        assert "curie-turn-failure" not in reply
        assert "evt-plain-failure" not in reply
        assert "Flagging for a human" not in reply
        assert relay.events == []
        # The operator's record is still logged in full.
        assert f"curie-turn-failure: {classification}" in caplog.text
        assert "event_id=evt-plain-failure" in caplog.text

    asyncio.run(go())


def test_cluster_message_relay_keeps_the_marked_record(make_harness) -> None:
    async def go() -> None:
        router, channel, relay = _router()
        async with make_harness(sink=router) as h:
            h.runner.default_script = [
                ErrorEvent(message="detail for operators", classification="budget-exceeded"),
                Final(text="failed", status=FAIL),
            ]
            await h.kernel.process_event(
                qevent("go", event_id="evt-relay-failure", adapter=CLUSTER_MESSAGE_ADAPTER)
            )

        reply = relay.last_text()
        assert reply is not None
        assert reply.splitlines()[0] == "curie-turn-failure: budget-exceeded"
        assert "event_id=evt-relay-failure" in reply
        assert channel.last_text() is None

    asyncio.run(go())
