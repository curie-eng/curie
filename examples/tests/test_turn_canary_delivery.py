"""Strict relay observations through production reply models.

@spec examples/turn-canary/design.md
"""

import asyncio
import copy
import json

import pytest
from channel_protocol import OutboundMessage, ReplyPost, ReplyUpdate, TurnStatus
from channel_protocol.progress import ProgressCard

from examples.tests.test_turn_canary import (
    REPLY_REF,
    CyclePlatform,
    _binding,
    _canary,
    _routes,
    reply_events,
    target_for,
)

FAST = {"turn_deadline": 0.02, "cleanup_deadline": 0.02, "poll_period": 0.001}
ROUTE = "slack|C-EXAMPLE-1|blue"


def make_turn(canary):
    return canary.build_turn(
        _binding(),
        nonce="nonce-1",
        conversation_id="eval:sample",
        reply_ref=REPLY_REF,
        event_id="EvCANARY-sample",
        received_at="2026-01-01T00:00:00Z",
    )


@pytest.mark.parametrize(
    ("text", "exact", "delivery", "reason"),
    [
        ("nonce-1", True, True, "exact_nonce"),
        ("other answer", False, True, "nonce_mismatch"),
        ("nonce-1 extra", False, True, "nonce_mismatch"),
        (None, False, False, "missing_reply"),
        ("", False, False, "blank_reply"),
        (" \n\t", False, False, "blank_reply"),
    ],
)
def test_assessment_separates_delivery_from_exact_nonce(text, exact, delivery, reason):
    """A delivered mismatch must stay an exact-nonce failure. @spec TURN-CANARY-3"""
    canary = _canary()
    turn = make_turn(canary)
    observed = canary.assess_relay(reply_events(turn, text), nonce="nonce-1", turn=turn)
    assert (observed.exact_nonce, observed.delivery_success, observed.reason) == (
        exact,
        delivery,
        reason,
    )


@pytest.mark.parametrize(
    "case",
    [
        "missing_completion",
        "duplicate",
        "dropped",
        "escalated",
        "awaiting-approval",
        "wrong_kind",
        "wrong_address",
        "wrong_conversation_id",
        "wrong_reply_ref",
        "wrong_event_id",
        "bad_version",
        "bad_text",
        "bad_outcome",
        "unknown_event",
        "non_object",
        "malformed_status",
        "missing_target",
        "late_wrong_target",
    ],
)
def test_invalid_observed_lifecycle_fails_closed(case):
    """No invalid observed completion or foreign event contributes evidence. @spec TURN-CANARY-3"""
    canary = _canary()
    turn = make_turn(canary)
    events = reply_events(turn)
    if case == "missing_completion":
        events.pop()
    elif case == "duplicate":
        events.append(copy.deepcopy(events[-1]))
    elif case in {"dropped", "escalated", "awaiting-approval"}:
        events[-1]["outcome"] = case
    elif case.startswith("wrong_") and case != "wrong_event_id":
        events[0]["target"][case.removeprefix("wrong_")] = "unrelated"
    elif case == "wrong_event_id":
        events[-1]["event_id"] = "EvCANARY-unrelated"
    elif case == "bad_version":
        events[-1]["version"] = "9.0"
    elif case == "bad_text":
        events[0]["text"] = 123
    elif case == "bad_outcome":
        events[-1]["outcome"] = "unknown"
    elif case == "unknown_event":
        events.append({"event": "unknown"})
    elif case == "non_object":
        events.append(None)
    elif case == "missing_target":
        del events[-1]["target"]
    else:
        status = TurnStatus(
            version="1.0", event="turn.status", target=target_for(turn), status=""
        ).model_dump(mode="json")
        if case == "malformed_status":
            status["status"] = []
        else:
            status["target"]["reply_ref"] = "unrelated"
        events.append(status)
    result = canary.assess_relay(events, nonce="nonce-1", turn=turn)
    assert result.exact_nonce is False
    assert result.delivery_success is False


def test_progress_and_status_are_not_answer_text():
    """Terminal progress cards cannot substitute for a text reply. @spec TURN-CANARY-3"""
    canary = _canary()
    turn = make_turn(canary)
    status = TurnStatus(
        version="1.0", event="turn.status", target=target_for(turn), status="working"
    ).model_dump(mode="json")
    progress = ReplyUpdate(
        version="1.1",
        event="reply.update",
        target=target_for(turn),
        delivery_id="00000000-0000-4000-8000-000000000002",
        progress=ProgressCard(
            kind="card", state="complete", summary="Done", revision=1, terminal=True
        ),
    ).model_dump(mode="json")
    for events in ([status], [progress], [progress, reply_events(turn)[-1]]):
        result = canary.assess_relay(events, nonce="nonce-1", turn=turn)
        assert not result.delivery_success and not result.exact_nonce


class DeliveryPlatform(CyclePlatform):
    def __init__(self, pages=None, text="other answer", **kwargs):
        super().__init__(**kwargs)
        self.pages = pages
        self.text = text
        self.turn = None

    async def enqueue(self, turn):
        self.turn = turn
        await super().enqueue(turn)

    async def replies(self, reply_ref, after):
        self.calls.append(("replies", after))
        if self.pages is not None:
            return self.pages.pop(0)
        return {"events": reply_events(self.turn, self.text), "next_cursor": 2, "terminal": True}


def cycle(canary, platform, **kwargs):
    return asyncio.run(canary.run_cycle(platform, _routes()[:1], **FAST, **kwargs))


def test_delivered_mismatch_exports_delivery_without_success_or_content():
    """A delivered mismatch diagnoses transport without passing the cycle. @spec TURN-CANARY-6"""
    canary = _canary()
    result = cycle(canary, DeliveryPlatform(text="sensitive response"))
    assert result.target_delivery_success == {ROUTE: True}
    assert result.target_success == {ROUTE: False}
    assert not result.success and result.last_success is None and result.target_last_success == {}
    assert {"phase": "relay", "outcome": "nonce_mismatch"} in result.logs
    assert "sensitive response" not in repr(result)
    metric = next(
        line
        for line in canary.prometheus_text(result, "custom").splitlines()
        if line.startswith("custom_target_delivery_success{")
    )
    label = metric.removeprefix("custom_target_delivery_success{route=").removesuffix("} 1")
    assert json.loads(json.loads(label)) == {
        "kind": "slack",
        "address": "C-EXAMPLE-1",
        "adapter": "blue",
    }
    assert (
        'target_delivery_success{agent="sample-agent",kind="slack",'
        'address="C-EXAMPLE-1",adapter="blue"} 1' in canary.prometheus_text(result, labels="split")
    )


@pytest.mark.parametrize(
    ("page", "reason"),
    [
        ({"events": [], "next_cursor": True, "terminal": False}, "malformed_page"),
        ({"events": [], "next_cursor": -1, "terminal": False}, "malformed_page"),
        ({"events": [], "next_cursor": 1, "terminal": False}, "malformed_page"),
        ({"events": [], "next_cursor": 0.0, "terminal": False}, "malformed_page"),
        ({"events": [], "next_cursor": "0", "terminal": False}, "malformed_page"),
        ({"events": [], "next_cursor": 0, "terminal": 0}, "malformed_page"),
        ({"events": [], "next_cursor": 0, "terminal": "false"}, "malformed_page"),
        ({"events": [], "next_cursor": 0}, "malformed_page"),
        ({"events": [], "next_cursor": 0, "terminal": True}, "terminal_mismatch"),
        ({"events": [None], "next_cursor": 1, "terminal": False}, "malformed_event"),
    ],
)
def test_malformed_page_fails_and_still_cleans_up(page, reason):
    """Reject coercible cursor/terminal types and impossible progression. @spec TURN-CANARY-3"""
    canary = _canary()
    platform = DeliveryPlatform(pages=[page])
    result = cycle(canary, platform)
    assert result.target_delivery_success == {ROUTE: False}
    assert result.target_success == {ROUTE: False}
    assert {"phase": "relay", "outcome": reason} in result.logs
    assert not result.cleanup_degraded and platform.active == 0


def test_streamed_reply_across_pages_then_completion_and_status_clear():
    """Streamed text and later status clear retain exact answer semantics. @spec TURN-CANARY-3"""
    canary = _canary()

    class Streaming(DeliveryPlatform):
        async def replies(self, reply_ref, after):
            self.calls.append(("replies", after))
            events = reply_events(
                self.turn, self.turn.text.split("no other text: ")[1].split(". Do not")[0]
            )
            if after == 0:
                return {
                    "events": [reply_events(self.turn, "partial")[0]],
                    "next_cursor": 1,
                    "terminal": False,
                }
            events.append(
                TurnStatus(
                    version="1.0", event="turn.status", target=target_for(self.turn), status=""
                ).model_dump(mode="json")
            )
            return {"events": events, "next_cursor": 4, "terminal": True}

    platform = Streaming()
    result = cycle(canary, platform)
    assert result.success and result.target_delivery_success == {ROUTE: True}
    assert result.target_success == {ROUTE: True}
    assert result.last_success and result.target_last_success[ROUTE] > 0
    assert [call for call in platform.calls if call[0] == "replies"] == [
        ("replies", 0),
        ("replies", 1),
    ]


def test_timeout_skip_hold_and_restart_never_fabricate_delivery(tmp_path):
    """Unattempted and restored state do not create healthy observations. @spec TURN-CANARY-6"""
    canary = _canary()
    timed_out = cycle(canary, CyclePlatform())
    assert timed_out.target_delivery_success == {ROUTE: False}
    assert {"phase": "relay", "outcome": "timeout"} in timed_out.logs
    skipped = cycle(canary, DeliveryPlatform(headroom=False))
    assert skipped.target_delivery_success == {}
    state = tmp_path / "state.json"
    with canary.StateJournal(state) as journal:
        journal.data["cleanup_blocked"] = True
        held = cycle(canary, DeliveryPlatform(), journal=journal)
        assert held.target_delivery_success == {}
        journal.save_result(timed_out)
    assert "target_delivery_success" not in json.loads(state.read_text())
    with canary.StateJournal(state) as journal:
        restored = cycle(canary, DeliveryPlatform(), journal=journal)
    assert restored.target_delivery_success == {}


def test_monitor_retains_and_prunes_delivery_and_starts_unknown():
    """Discovery pruning applies equally to delivery observations. @spec TURN-CANARY-6"""
    canary = _canary()
    monitor = canary.Monitor(None, canary.Limits())
    assert "target_delivery_success" not in monitor.metrics_text()
    monitor.observe(cycle(canary, DeliveryPlatform()))
    monitor.observe(cycle(canary, DeliveryPlatform(headroom=False)))
    assert "target_delivery_success" in monitor.metrics_text()
    samples = [
        line
        for line in monitor.metrics_text().splitlines()
        if line.startswith("turn_canary_target_delivery_success{")
    ]
    assert len(samples) == 1 and samples[0].endswith("} 1")
    monitor.observe(canary.CycleResult(inventory_labels=set()))
    assert "target_delivery_success" not in monitor.metrics_text()


@pytest.mark.parametrize(
    "case",
    [
        "wrong_target",
        "wrong_event_id",
        "duplicate",
        "terminal_false",
        "stalled_cursor",
        "jumped_cursor",
        "slow_read",
    ],
)
def test_await_uses_enqueued_identity_and_consistent_page_history(case):
    """The cycle must correlate real events and enforce a bounded read. @spec TURN-CANARY-3"""
    canary = _canary()

    class Corrupt(DeliveryPlatform):
        async def replies(self, reply_ref, after):
            events = reply_events(self.turn)
            if case == "slow_read":
                await asyncio.sleep(1)
            if case == "wrong_target":
                events[0]["target"]["address"] = "C-EXAMPLE-OTHER"
            elif case == "wrong_event_id":
                events[-1]["event_id"] = "EvCANARY-other"
            elif case == "duplicate":
                events.append(copy.deepcopy(events[-1]))
            elif case in {"stalled_cursor", "jumped_cursor"}:
                if after == 0:
                    return {"events": events[:1], "next_cursor": 1, "terminal": False}
                return {
                    "events": events[1:],
                    "next_cursor": 1 if case == "stalled_cursor" else 3,
                    "terminal": True,
                }
            return {
                "events": events,
                "next_cursor": len(events),
                "terminal": case != "terminal_false",
            }

    platform = Corrupt()
    result = cycle(canary, platform)
    assert not result.success
    assert result.target_delivery_success == {ROUTE: False}
    assert result.target_success == {ROUTE: False}
    assert not result.cleanup_degraded and platform.active == 0


def test_latest_blank_text_overrides_earlier_nonce():
    """A preceding correct stream fragment cannot hide a blank final edit. @spec TURN-CANARY-3"""
    canary = _canary()
    turn = make_turn(canary)
    events = reply_events(turn)[:1] + reply_events(turn, " ")
    result = canary.assess_relay(events, nonce="nonce-1", turn=turn)
    assert not result.exact_nonce and not result.delivery_success
    assert result.reason == "blank_reply"


@pytest.mark.parametrize("extra_event", ["progress_update", "reply_post"])
def test_delivery_evidence_does_not_broaden_legacy_nonce_success(extra_event):
    """Retain the original last-update and allowed-event rules. @spec TURN-CANARY-3"""
    canary = _canary()

    class ExtraEvent(DeliveryPlatform):
        async def replies(self, reply_ref, after):
            nonce = self.turn.text.split("no other text: ")[1].split(". Do not")[0]
            events = reply_events(self.turn, nonce)
            if extra_event == "progress_update":
                extra = ReplyUpdate(
                    version="1.1",
                    event="reply.update",
                    target=target_for(self.turn),
                    delivery_id="00000000-0000-4000-8000-000000000002",
                    progress=ProgressCard(
                        kind="card", state="complete", summary="Done", revision=1, terminal=True
                    ),
                )
            else:
                extra = ReplyPost(
                    version="1.0",
                    event="reply.post",
                    target=target_for(self.turn),
                    message=OutboundMessage(version="1.0", text="Synthetic platform notice"),
                    requested_by="turn-canary",
                )
            events.insert(1, extra.model_dump(mode="json"))
            self.events = events
            self.nonce = nonce
            return {"events": events, "next_cursor": 3, "terminal": True}

    platform = ExtraEvent()
    result = cycle(canary, platform)
    observed = canary.assess_relay(platform.events, nonce=platform.nonce, turn=platform.turn)
    assert observed.delivery_success is True
    assert observed.exact_nonce is False
    assert canary.relay_outcome(platform.events, nonce=platform.nonce, turn=platform.turn) is False
    assert result.target_delivery_success == {ROUTE: True}
    assert result.target_success == {ROUTE: False}
    assert not result.success and result.last_success is None and result.target_last_success == {}


def test_awaiting_approval_uses_nonterminal_production_page_shape():
    """The relay keeps approval pauses nonterminal. @spec TURN-CANARY-3"""
    canary = _canary()

    class AwaitingApproval(DeliveryPlatform):
        async def replies(self, reply_ref, after):
            # apps/api/src/curie_api/routers/cluster_message_replies.py::_is_terminal
            # deliberately excludes awaiting-approval because resumed events reuse the bucket.
            return {
                "events": reply_events(self.turn, outcome="awaiting-approval"),
                "next_cursor": 2,
                "terminal": False,
            }

    platform = AwaitingApproval()
    result = cycle(canary, platform)
    assert result.target_delivery_success == {ROUTE: False}
    assert result.target_success == {ROUTE: False}
    assert {"phase": "relay", "outcome": "not_delivered"} in result.logs
    assert not result.success and not result.cleanup_degraded and platform.active == 0
