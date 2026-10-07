"""Contract tests for the opt-in turn canary.

@spec examples/turn-canary/design.md
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import asyncio

import pytest
from aci_protocol.turn import QueuedTurn

CANARY = Path(__file__).resolve().parents[1] / "turn-canary" / "turn_canary.py"
REPLY_REF = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"


def _canary():
    """Assert a missing implementation explicitly. @spec TURN-CANARY-1"""
    assert CANARY.is_file(), "TURN-CANARY-1: the opt-in turn canary implementation is missing"
    spec = importlib.util.spec_from_file_location("turn_canary_example", CANARY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _binding(adapter: str = "blue") -> dict[str, str]:
    """One selected, anonymous active Slack route. @spec TURN-CANARY-1"""
    return {
        "agent_id": "00000000-0000-4000-8000-000000000001",
        "agent": "sample-agent",
        "deployment_status": "active",
        "kind": "slack",
        "address": "C-EXAMPLE-1",
        "adapter": adapter,
    }


def test_selection_requires_exact_live_active_binding() -> None:
    """@spec TURN-CANARY-1"""
    canary = _canary()
    rows = [_binding(), {**_binding(), "address": "C-EXAMPLE-2"}]
    selected = canary.select_targets(rows, [("slack", "C-EXAMPLE-1", "blue")])
    assert len(selected) == 1
    assert selected[0]["address"] == "C-EXAMPLE-1"
    assert selected[0]["adapter"] == "blue"
    with pytest.raises(ValueError, match="selected"):
        canary.select_targets(rows, [("slack", "C-ABSENT", "blue")])
    with pytest.raises(ValueError, match="selected"):
        canary.select_targets([{**_binding(), "deployment_status": "inactive"}],
                              [("slack", "C-EXAMPLE-1", "blue")])


def test_named_binding_turn_is_read_only_and_relayed_internally() -> None:
    """@spec TURN-CANARY-2"""
    canary = _canary()
    turn = canary.build_turn(
        _binding(), nonce="n-unique-1", conversation_id="eval:probe-1",
        reply_ref=REPLY_REF, event_id="EvSIM-example-1",
        received_at="2026-01-01T00:00:00Z",
    )
    wire = QueuedTurn.model_validate_json(turn.model_dump_json())
    assert wire.tool_access.value == "read-only"
    assert wire.conversation_id == "eval:probe-1"
    assert wire.reply_handle is not None
    assert wire.reply_handle.kind == "slack"
    assert wire.reply_handle.channel == "C-EXAMPLE-1"
    assert wire.reply_handle.adapter == "curie-cluster-message"
    assert wire.reply_handle.identity == "blue"
    assert wire.reply_handle.placeholder == REPLY_REF
    assert wire.attachments == []
    assert wire.hook_run is None
    assert "n-unique-1" in wire.text
    assert "approval" in wire.text.lower()
    assert "action" in wire.text.lower()


@pytest.mark.parametrize(
    ("events", "expected"),
    [
        ([{"event": "reply.update", "text": "nonce-1"},
          {"event": "turn.completed", "outcome": "delivered"}], True),
        ([{"event": "reply.update", "text": "nonce-1"}], False),
        ([{"event": "reply.update", "text": "nonce-1"},
          {"event": "turn.completed", "outcome": "awaiting-approval"}], False),
        ([{"event": "reply.update", "text": "nonce-1"},
          {"event": "turn.completed", "outcome": "dropped"}], False),
        ([{"event": "reply.update", "text": "wrong"},
          {"event": "turn.completed", "outcome": "delivered"}], False),
        ([{"event": "reply.update", "text": "nonce-1 extra"},
          {"event": "turn.completed", "outcome": "delivered"}], False),
    ],
)
def test_success_requires_delivered_completion_and_exact_nonce(events, expected) -> None:
    """@spec TURN-CANARY-3"""
    assert _canary().relay_outcome(events, nonce="nonce-1") is expected


def test_owned_reset_key_matches_named_worker_route() -> None:
    """@spec TURN-CANARY-4"""
    from channel_protocol import scoped_conversation_id

    canary = _canary()
    key = canary.scoped_reset_key(_binding(), "eval:probe-1")
    assert key == scoped_conversation_id(
        "slack", "C-EXAMPLE-1", "eval:probe-1", identity="blue"
    )
    assert key != scoped_conversation_id("slack", "C-EXAMPLE-1", "eval:probe-1")


@pytest.mark.parametrize(
    ("state", "confirmed"),
    [
        ({"requested": False, "route_existed": True}, True),
        ({"requested": True, "route_existed": None}, False),
        ({"requested": False, "route_existed": False}, False),
        ({"requested": False, "route_existed": None}, False),
    ],
)
def test_cleanup_requires_confirmed_route_release(state, confirmed) -> None:
    """@spec TURN-CANARY-4"""
    assert _canary().cleanup_confirmed(state) is confirmed


class CyclePlatform:
    """In-memory platform boundary for the async contract. @spec TURN-CANARY-3 TURN-CANARY-4"""

    def __init__(self, *, reset_state=None, reply_events=None, headroom=True):
        self.calls = []
        self.reset_state_value = reset_state or {"requested": False, "route_existed": True}
        self.reply_events = reply_events
        self.headroom = headroom
        self.active = 0
        self.peak = 0

    async def inventory(self):
        return [_binding(), {**_binding(), "address": "C-EXAMPLE-2"}]

    async def read_only_supported(self):
        return True

    async def quota_headroom(self):
        self.calls.append("quota")
        return self.headroom

    async def enqueue(self, turn):
        assert QueuedTurn.model_validate_json(turn.model_dump_json()).tool_access.value == "read-only"
        self.calls.append(("enqueue", turn.conversation_id))
        self.active += 1
        self.peak = max(self.peak, self.active)

    async def replies(self, reply_ref, after):
        self.calls.append(("replies", after))
        if self.reply_events is None:
            return {"events": [], "next_cursor": after, "terminal": False}
        return {"events": self.reply_events, "next_cursor": after + len(self.reply_events), "terminal": True}

    async def reset(self, agent_id, thread_key):
        self.calls.append(("reset", thread_key))

    async def reset_state(self, agent_id, thread_key):
        self.calls.append(("reset_state", thread_key))
        if self.reset_state_value == {"requested": False, "route_existed": True}:
            self.active -= 1
        return self.reset_state_value


def _routes():
    return [("slack", "C-EXAMPLE-1", "blue"), ("slack", "C-EXAMPLE-2", "blue")]


def test_timeout_resets_owned_route_and_stops_when_cleanup_unconfirmed():
    """@spec TURN-CANARY-3 TURN-CANARY-4"""
    canary = _canary()
    platform = CyclePlatform(reset_state={"requested": True, "route_existed": None})
    result = asyncio.run(canary.run_cycle(platform, _routes(), turn_deadline=0.02,
                                    cleanup_deadline=0.02, poll_period=0.005))
    assert len([call for call in platform.calls if isinstance(call, tuple) and call[0] == "enqueue"]) == 1
    assert len([call for call in platform.calls if isinstance(call, tuple) and call[0] == "reset"]) == 1
    assert result.cleanup_degraded is True
    assert result.success is False


def test_false_reset_result_stops_before_second_probe():
    """@spec TURN-CANARY-4"""
    platform = CyclePlatform(reset_state={"requested": False, "route_existed": False})
    result = asyncio.run(_canary().run_cycle(platform, _routes(), turn_deadline=0.02,
                                       cleanup_deadline=0.02, poll_period=0.005))
    assert result.cleanup_degraded is True
    assert sum(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple)) == 1


def test_successful_cycle_runs_probes_serially():
    """@spec TURN-CANARY-3 TURN-CANARY-5"""
    platform = CyclePlatform(reply_events=[{"event": "turn.completed", "outcome": "dropped"}])
    result = asyncio.run(_canary().run_cycle(platform, _routes(), turn_deadline=0.02,
                                       cleanup_deadline=0.02, poll_period=0.005))
    assert result.cleanup_degraded is False
    assert platform.peak == 1
    assert sum(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple)) == 2


@pytest.mark.parametrize("headroom", [False, None])
def test_quota_headroom_refuses_enqueue(headroom):
    """@spec TURN-CANARY-5"""
    platform = CyclePlatform(headroom=headroom)
    result = asyncio.run(_canary().run_cycle(platform, _routes()[:1], turn_deadline=0.02,
                                       cleanup_deadline=0.02, poll_period=0.005))
    assert result.capacity_skips == 1
    assert not any(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple))


def test_network_error_diagnostic_omits_raw_secret_and_reply():
    """@spec TURN-CANARY-6"""
    class Broken(CyclePlatform):
        async def replies(self, reply_ref, after):
            raise RuntimeError("token=private-secret reply=private-content")

    platform = Broken()
    result = asyncio.run(_canary().run_cycle(platform, _routes()[:1], turn_deadline=0.02,
                                       cleanup_deadline=0.02, poll_period=0.005))
    rendered = repr(result) + str(result.logs)
    assert "private-secret" not in rendered
    assert "private-content" not in rendered
    assert "RuntimeError" in rendered
    assert any(call[0] == "reset" for call in platform.calls if isinstance(call, tuple))
