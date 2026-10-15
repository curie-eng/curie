"""Contract tests for the opt-in turn canary.

@spec examples/turn-canary/design.md
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path

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
        canary.select_targets(
            [{**_binding(), "deployment_status": "inactive"}], [("slack", "C-EXAMPLE-1", "blue")]
        )


def test_named_binding_turn_is_read_only_and_relayed_internally() -> None:
    """@spec TURN-CANARY-2"""
    canary = _canary()
    turn = canary.build_turn(
        _binding(),
        nonce="n-unique-1",
        conversation_id="eval:probe-1",
        reply_ref=REPLY_REF,
        event_id="EvSIM-example-1",
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
        (
            [
                {"event": "reply.update", "text": "nonce-1"},
                {"event": "turn.completed", "outcome": "delivered"},
            ],
            True,
        ),
        ([{"event": "reply.update", "text": "nonce-1"}], False),
        (
            [
                {"event": "reply.update", "text": "nonce-1"},
                {"event": "turn.completed", "outcome": "awaiting-approval"},
            ],
            False,
        ),
        (
            [
                {"event": "reply.update", "text": "nonce-1"},
                {"event": "turn.completed", "outcome": "dropped"},
            ],
            False,
        ),
        (
            [
                {"event": "reply.update", "text": "wrong"},
                {"event": "turn.completed", "outcome": "delivered"},
            ],
            False,
        ),
        (
            [
                {"event": "reply.update", "text": "nonce-1 extra"},
                {"event": "turn.completed", "outcome": "delivered"},
            ],
            False,
        ),
    ],
)
def test_success_requires_delivered_completion_and_exact_nonce(events, expected) -> None:
    """@spec TURN-CANARY-3"""
    assert _canary().relay_outcome(events, nonce="nonce-1") is expected


def test_owned_reset_key_matches_named_worker_route() -> None:
    """@spec TURN-CANARY-4"""
    from channel_protocol import scoped_conversation_id
    from curie_worker.kernel.routing import _thread_key_for

    canary = _canary()
    key = canary.scoped_reset_key(_binding(), "eval:probe-1")
    turn = canary.build_turn(
        _binding(),
        nonce="nonce",
        conversation_id="eval:probe-1",
        reply_ref=REPLY_REF,
        event_id="EvCANARY-key",
        received_at="2026-01-01T00:00:00Z",
    )
    assert key == _thread_key_for(turn)
    assert key == scoped_conversation_id("slack", "C-EXAMPLE-1", "eval:probe-1", identity="blue")
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
        assert (
            QueuedTurn.model_validate_json(turn.model_dump_json()).tool_access.value == "read-only"
        )
        self.calls.append(("enqueue", turn.conversation_id))
        self.active += 1
        self.peak = max(self.peak, self.active)

    async def replies(self, reply_ref, after):
        self.calls.append(("replies", after))
        if self.reply_events is None:
            return {"events": [], "next_cursor": after, "terminal": False}
        return {
            "events": self.reply_events,
            "next_cursor": after + len(self.reply_events),
            "terminal": True,
        }

    async def reset(self, agent_id, thread_key):
        self.calls.append(("reset", thread_key))

    async def reset_state(self, agent_id, thread_key):
        self.calls.append(("reset_state", thread_key))
        if self.reset_state_value == {"requested": False, "route_existed": True}:
            self.active -= 1
        return self.reset_state_value


def _routes():
    return [("slack", "C-EXAMPLE-1", "blue"), ("slack", "C-EXAMPLE-2", "blue")]


def test_timeout_resets_each_owned_route_and_holds_only_unconfirmed_routes():
    """@spec TURN-CANARY-3 TURN-CANARY-4"""
    canary = _canary()
    platform = CyclePlatform(reset_state={"requested": True, "route_existed": None})
    result = asyncio.run(
        canary.run_cycle(
            platform, _routes(), turn_deadline=0.02, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    enqueued = [
        call[1] for call in platform.calls if isinstance(call, tuple) and call[0] == "enqueue"
    ]
    resets = [call[1] for call in platform.calls if isinstance(call, tuple) and call[0] == "reset"]
    assert len(enqueued) == 2
    assert len(resets) == 2 and len(set(resets)) == 2
    assert result.cleanup_degraded is True
    assert result.target_cleanup_degraded == {
        "slack|C-EXAMPLE-1|blue": True,
        "slack|C-EXAMPLE-2|blue": True,
    }
    assert result.success is False


def test_false_reset_result_in_cycle_is_unconfirmed_but_next_route_still_turns():
    """@spec TURN-CANARY-4"""
    platform = CyclePlatform(reset_state={"requested": False, "route_existed": False})
    result = asyncio.run(
        _canary().run_cycle(
            platform, _routes(), turn_deadline=0.02, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    assert result.cleanup_degraded is True
    assert result.success is False
    assert sum(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple)) == 2


def test_successful_cycle_runs_probes_serially():
    """@spec TURN-CANARY-3 TURN-CANARY-5"""
    platform = CyclePlatform(reply_events=[{"event": "turn.completed", "outcome": "dropped"}])
    result = asyncio.run(
        _canary().run_cycle(
            platform, _routes(), turn_deadline=0.02, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    assert result.cleanup_degraded is False
    assert platform.peak == 1
    assert sum(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple)) == 2


@pytest.mark.parametrize("headroom", [False, None])
def test_quota_headroom_refuses_enqueue(headroom):
    """@spec TURN-CANARY-5"""
    platform = CyclePlatform(headroom=headroom)
    result = asyncio.run(
        _canary().run_cycle(
            platform, _routes()[:1], turn_deadline=0.02, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    assert result.capacity_skips == 1
    assert not any(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple))
    assert result.target_success == {}
    assert result.target_capacity_skipped == {"slack|C-EXAMPLE-1|blue": True}


def test_network_error_diagnostic_omits_raw_secret_and_reply():
    """@spec TURN-CANARY-6"""

    class Broken(CyclePlatform):
        async def replies(self, reply_ref, after):
            raise RuntimeError("token=private-secret reply=private-content")

    platform = Broken()
    result = asyncio.run(
        _canary().run_cycle(
            platform, _routes()[:1], turn_deadline=0.02, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    rendered = repr(result) + str(result.logs)
    assert "private-secret" not in rendered
    assert "private-content" not in rendered
    assert "RuntimeError" in rendered
    assert any(call[0] == "reset" for call in platform.calls if isinstance(call, tuple))


@pytest.mark.parametrize("crash_phase", ["before_enqueue", "after_enqueue"])
def test_crash_keeps_durable_owned_cleanup_intent_and_fresh_run_refuses(tmp_path, crash_phase):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "canary-state.json"

    class CrashingPlatform(CyclePlatform):
        async def enqueue(self, turn):
            if crash_phase == "before_enqueue":
                raise SystemExit("crash before stream write")
            await super().enqueue(turn)

        async def replies(self, reply_ref, after):
            raise SystemExit("crash after stream write")

    first = CrashingPlatform()
    with canary.StateJournal(state_file) as journal:
        with pytest.raises(SystemExit):
            asyncio.run(
                canary.run_cycle(
                    first,
                    _routes()[:1],
                    journal=journal,
                    turn_deadline=0.02,
                    cleanup_deadline=0.02,
                    poll_period=0.005,
                )
            )
    saved = json.loads(state_file.read_text())
    pending = saved["pending_routes"]["slack|C-EXAMPLE-1|blue"]
    assert pending["agent_id"] == _binding()["agent_id"]
    assert pending["thread_key"].startswith("slack:")
    second = CyclePlatform(reset_state={"requested": True, "route_existed": None})
    with canary.StateJournal(state_file) as journal:
        result = asyncio.run(
            canary.run_cycle(
                second,
                _routes()[:1],
                journal=journal,
                turn_deadline=0.02,
                cleanup_deadline=0.02,
                poll_period=0.005,
            )
        )
    assert result.cleanup_degraded is True
    assert not any(call[0] == "enqueue" for call in second.calls if isinstance(call, tuple))


def test_durable_intent_clears_only_after_confirmed_reset(tmp_path):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "canary-state.json"
    first = CyclePlatform(reset_state={"requested": False, "route_existed": False})
    with canary.StateJournal(state_file) as journal:
        result = asyncio.run(
            canary.run_cycle(
                first,
                _routes()[:1],
                journal=journal,
                turn_deadline=0.02,
                cleanup_deadline=0.02,
                poll_period=0.005,
            )
        )
    assert result.cleanup_degraded is True
    assert json.loads(state_file.read_text())["pending_routes"] != {}


def test_existing_empty_state_fails_closed(tmp_path):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "canary-state.json"
    state_file.write_text("")
    with pytest.raises(ValueError, match="state"):
        with canary.StateJournal(state_file):
            pass


@pytest.mark.parametrize(
    "corrupt",
    [
        {"cleanup_blocked": 1},
        {"cleanup_blocked": 0},
        {"capacity_skips_total": True},
        {"capacity_skips_total": -1},
        {"last_success": True},
        {"last_success": float("nan")},
        {"target_last_success": {"slack:example": False}},
        {"target_last_success": {"slack:example": float("inf")}},
        {"pending_routes": []},
        {"pending_routes": {"slack|C-EXAMPLE-1|blue": {"agent_id": "a"}}},
        {"pending_routes": {"slack|C-EXAMPLE-1|blue": {"agent_id": "a", "thread_key": 1}}},
        {
            "pending_routes": {
                "slack|C-EXAMPLE-1|blue": {"agent_id": "a", "thread_key": "k", "checked_at": True}
            }
        },
    ],
)
def test_corrupt_boolean_or_numeric_state_refuses_before_cycle(tmp_path, corrupt):
    """@spec TURN-CANARY-4 TURN-CANARY-6"""
    canary = _canary()
    state_file = tmp_path / "canary-state.json"
    state_file.write_text(json.dumps({"pending": None, **corrupt}))
    with pytest.raises(ValueError, match="state"):
        with canary.StateJournal(state_file):
            pass


def test_journal_cleanup_hold_blocks_enqueue_without_platform_flag(tmp_path):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "canary-state.json"
    state_file.write_text(json.dumps({"pending": None, "cleanup_blocked": True}))
    platform = CyclePlatform()
    with canary.StateJournal(state_file) as journal:
        result = asyncio.run(
            canary.run_cycle(
                platform,
                _routes()[:1],
                journal=journal,
                turn_deadline=0.02,
                cleanup_deadline=0.02,
                poll_period=0.005,
            )
        )
    assert result.cleanup_degraded is True
    assert not any(call[0] == "enqueue" for call in platform.calls if isinstance(call, tuple))


@pytest.mark.parametrize("operation", ["clear", "save"])
def test_interrupted_state_replacement_preserves_pending_and_blocks_next_run(
    tmp_path, monkeypatch, operation
):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "canary-state.json"
    agent_id = _binding()["agent_id"]
    thread_key = "slack:blue:C-EXAMPLE-1:eval%3Aprobe"
    route = "slack|C-EXAMPLE-1|blue"
    with canary.StateJournal(state_file) as journal:
        journal.record_intent(agent_id, thread_key, route=route)
    before = state_file.read_text()

    def interrupt_replace(*args):
        raise SystemExit("interrupted before atomic replacement")

    with canary.StateJournal(state_file) as journal:
        with monkeypatch.context() as patch:
            patch.setattr(canary.os, "replace", interrupt_replace)
            with pytest.raises(SystemExit):
                if operation == "clear":
                    journal.clear_intent(agent_id, thread_key, route=route)
                else:
                    journal.save_result(canary.CycleResult(cleanup_degraded=True))
    assert state_file.read_text() == before
    lock_file = tmp_path / "canary-state.json.lock"
    assert lock_file.is_file()
    next_platform = CyclePlatform(reset_state={"requested": True, "route_existed": None})
    with canary.StateJournal(state_file) as journal:
        assert journal.pending_routes[route]["agent_id"] == agent_id
        assert journal.pending_routes[route]["thread_key"] == thread_key
        result = asyncio.run(
            canary.run_cycle(
                next_platform,
                _routes()[:1],
                journal=journal,
                turn_deadline=0.02,
                cleanup_deadline=0.02,
                poll_period=0.005,
            )
        )
    assert result.cleanup_degraded is True
    assert not any(call[0] == "enqueue" for call in next_platform.calls if isinstance(call, tuple))


# Operability: conversation prefix, per-route cleanup holds, capacity skips


ROUTE_A = "slack|C-EXAMPLE-1|blue"
ROUTE_B = "slack|C-EXAMPLE-2|blue"
UNCONFIRMED = {"requested": True, "route_existed": None}
RELEASED = {"requested": False, "route_existed": True}


class RoutePlatform:
    """Delivers every nonce; reset outcomes are set per address. @spec TURN-CANARY-4"""

    def __init__(self, reset_states=None, headroom=None):
        self.reset_states = dict(reset_states or {})
        self.headroom = list(headroom or [])
        self.calls: list[tuple[str, str]] = []
        self.nonces: dict[str, str] = {}
        self.cleanup_blocked = False

    async def inventory(self):
        return [_binding(), {**_binding(), "address": "C-EXAMPLE-2"}]

    async def read_only_supported(self):
        return True

    async def quota_headroom(self):
        return self.headroom.pop(0) if self.headroom else True

    async def enqueue(self, turn):
        self.calls.append(("enqueue", turn.reply_handle.channel))
        self.calls.append(("conversation", turn.conversation_id))
        nonce = turn.text.split("no other text: ", 1)[1].split(". Do not", 1)[0]
        self.nonces[turn.reply_handle.placeholder] = nonce

    async def replies(self, reply_ref, after):
        events = [
            {"event": "reply.update", "text": self.nonces[reply_ref]},
            {"event": "turn.completed", "outcome": "delivered"},
        ]
        return {"events": events, "next_cursor": after + len(events), "terminal": True}

    def _address(self, thread_key):
        return "C-EXAMPLE-2" if "C-EXAMPLE-2" in thread_key else "C-EXAMPLE-1"

    async def reset(self, agent_id, thread_key):
        self.calls.append(("reset", thread_key))

    async def reset_state(self, agent_id, thread_key):
        return self.reset_states.get(self._address(thread_key), RELEASED)

    def enqueued(self):
        return [value for name, value in self.calls if name == "enqueue"]


def _journaled_cycle(canary, platform, state_file, routes=None, **options):
    with canary.StateJournal(state_file) as journal:
        result = asyncio.run(
            canary.run_cycle(
                platform,
                routes if routes is not None else _routes(),
                journal=journal,
                turn_deadline=0.05,
                cleanup_deadline=0.02,
                poll_period=0.005,
                **options,
            )
        )
        journal.save_result(result)
    return result


def test_canary_conversation_id_has_fixed_prefix_and_reset_targets_only_it():
    """@spec TURN-CANARY-2 TURN-CANARY-4"""
    import uuid

    from channel_protocol import scoped_conversation_id

    canary = _canary()
    platform = RoutePlatform()
    result = asyncio.run(
        canary.run_cycle(
            platform, _routes()[:1], turn_deadline=0.05, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    assert result.success is True
    (conversation,) = [value for name, value in platform.calls if name == "conversation"]
    assert conversation.startswith("eval:turn-canary-")
    assert uuid.UUID(conversation.removeprefix("eval:turn-canary-")).version == 4
    key = canary.scoped_reset_key(_binding(), conversation)
    assert key == scoped_conversation_id("slack", "C-EXAMPLE-1", conversation, identity="blue")
    assert [value for name, value in platform.calls if name == "reset"] == [key]


def test_conversation_prefix_is_configurable_and_must_keep_eval_isolation():
    """@spec TURN-CANARY-2"""
    canary = _canary()
    env = {
        "TURN_CANARY_DATABASE_URL": "postgresql+asyncpg://canary@db.example.invalid/curie",
        "TURN_CANARY_VALKEY_URL": "redis://valkey.example.invalid:6379/0",
        "TURN_CANARY_API_URL": "https://platform.example.invalid",
        "TURN_CANARY_API_KEY": "example-key",
        "TURN_CANARY_STREAM": "curie:runs",
        "TURN_CANARY_STATE_PATH": "/var/lib/turn-canary/state",
        "TURN_CANARY_ROUTES": '[["slack","C-EXAMPLE-1","blue"]]',
    }
    assert canary.parse_config(env).conversation_prefix == "eval:turn-canary-"
    custom = {**env, "TURN_CANARY_CONVERSATION_PREFIX": "eval:acme-canary-"}
    assert canary.parse_config(custom).conversation_prefix == "eval:acme-canary-"
    for bad in ("turn-canary-", "eval:has space", "eval:" + "x" * 49):
        with pytest.raises(ValueError, match="PREFIX"):
            canary.parse_config({**env, "TURN_CANARY_CONVERSATION_PREFIX": bad})
    platform = RoutePlatform()
    asyncio.run(
        canary.run_cycle(
            platform,
            _routes()[:1],
            turn_deadline=0.05,
            cleanup_deadline=0.02,
            poll_period=0.005,
            conversation_prefix="eval:acme-canary-",
        )
    )
    (conversation,) = [value for name, value in platform.calls if name == "conversation"]
    assert conversation.startswith("eval:acme-canary-")


def test_unconfirmed_route_is_held_while_others_turn_and_recovers_once_released(tmp_path):
    """@spec TURN-CANARY-4 TURN-CANARY-5 TURN-CANARY-6"""
    canary = _canary()
    state_file = tmp_path / "state.json"
    platform = RoutePlatform(reset_states={"C-EXAMPLE-1": UNCONFIRMED})

    first = _journaled_cycle(canary, platform, state_file)
    assert platform.enqueued() == ["C-EXAMPLE-1", "C-EXAMPLE-2"]
    assert first.target_cleanup_degraded == {ROUTE_A: True, ROUTE_B: False}
    assert first.success is False
    assert set(json.loads(state_file.read_text())["pending_routes"]) == {ROUTE_A}

    platform.calls.clear()
    second = _journaled_cycle(canary, platform, state_file)
    assert platform.enqueued() == ["C-EXAMPLE-2"]
    assert second.target_cleanup_degraded[ROUTE_A] is True
    assert second.cleanup_degraded is True
    assert second.success is True
    assert second.target_success == {ROUTE_B: True}
    exposition = canary.prometheus_text(second)
    assert "turn_canary_cleanup_degraded 1" in exposition
    assert any(
        line.startswith("turn_canary_target_cleanup_degraded{")
        and "C-EXAMPLE-1" in line
        and line.endswith(" 1")
        for line in exposition.splitlines()
    )
    assert json.loads(state_file.read_text())["pending_routes"][ROUTE_A]["checked_at"] > 0

    platform.reset_states["C-EXAMPLE-1"] = RELEASED
    platform.calls.clear()
    third = _journaled_cycle(canary, platform, state_file)
    assert platform.enqueued() == ["C-EXAMPLE-1", "C-EXAMPLE-2"]
    assert third.target_cleanup_degraded == {ROUTE_A: False, ROUTE_B: False}
    assert third.cleanup_degraded is False
    assert third.success is True
    assert json.loads(state_file.read_text())["pending_routes"] == {}


def test_recheck_clears_a_drained_no_route_but_not_an_unknown_result(tmp_path):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "state.json"
    platform = RoutePlatform(
        reset_states={"C-EXAMPLE-1": {"requested": False, "route_existed": None}}
    )
    _journaled_cycle(canary, platform, state_file, routes=_routes()[:1])
    platform.calls.clear()
    held = _journaled_cycle(canary, platform, state_file, routes=_routes()[:1])
    assert platform.enqueued() == []
    assert held.success is False
    assert [name for name, _ in platform.calls] == ["reset"]

    platform.reset_states["C-EXAMPLE-1"] = {"requested": False, "route_existed": False}
    platform.calls.clear()
    _journaled_cycle(canary, platform, state_file, routes=_routes()[:1])
    assert platform.enqueued() == ["C-EXAMPLE-1"]


def test_legacy_single_pending_holds_every_route_until_rechecked(tmp_path):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "state.json"
    legacy = {"agent_id": _binding()["agent_id"], "thread_key": "slack:blue:C-EXAMPLE-1:eval%3Aold"}
    state_file.write_text(json.dumps({"pending": legacy, "cleanup_blocked": True}))
    platform = RoutePlatform(reset_states={"C-EXAMPLE-1": UNCONFIRMED})
    held = _journaled_cycle(canary, platform, state_file)
    assert platform.enqueued() == []
    assert held.cleanup_degraded is True
    assert held.success is False

    platform.reset_states["C-EXAMPLE-1"] = RELEASED
    platform.calls.clear()
    recovered = _journaled_cycle(canary, platform, state_file)
    assert platform.enqueued() == ["C-EXAMPLE-1", "C-EXAMPLE-2"]
    assert recovered.success is True
    saved = json.loads(state_file.read_text())
    assert saved["pending"] is None
    assert saved["cleanup_blocked"] is False


def test_operator_cleanup_hold_without_pending_record_is_kept(tmp_path):
    """@spec TURN-CANARY-4"""
    canary = _canary()
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"pending": None, "cleanup_blocked": True}))
    platform = RoutePlatform()
    _journaled_cycle(canary, platform, state_file)
    assert platform.enqueued() == []
    assert json.loads(state_file.read_text())["cleanup_blocked"] is True


def test_capacity_skip_publishes_a_skip_sample_and_no_failure():
    """@spec TURN-CANARY-5 TURN-CANARY-6"""
    canary = _canary()
    platform = RoutePlatform(headroom=[False, False])
    result = asyncio.run(
        canary.run_cycle(
            platform, _routes(), turn_deadline=0.05, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    assert platform.enqueued() == []
    assert result.success is True
    exposition = canary.prometheus_text(result)
    assert "turn_canary_target_success{" not in exposition
    assert "turn_canary_cycle_success 1" in exposition
    skipped = [
        line
        for line in exposition.splitlines()
        if line.startswith("turn_canary_target_capacity_skipped{")
    ]
    assert len(skipped) == 2 and all(line.endswith(" 1") for line in skipped)


def test_capacity_skip_beside_a_delivered_turn_keeps_cycle_success():
    """@spec TURN-CANARY-5 TURN-CANARY-6"""
    canary = _canary()
    platform = RoutePlatform(headroom=[True, None])
    result = asyncio.run(
        canary.run_cycle(
            platform, _routes(), turn_deadline=0.05, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    assert platform.enqueued() == ["C-EXAMPLE-1"]
    assert result.success is True
    assert result.target_success == {ROUTE_A: True}
    assert result.target_capacity_skipped == {ROUTE_A: False, ROUTE_B: True}


def test_exporter_keeps_previous_target_success_across_a_capacity_skip():
    """@spec TURN-CANARY-6 TURN-CANARY-10"""
    canary = _canary()
    limits = canary.Limits()
    monitor = canary.Monitor(None, limits)
    monitor.observe(
        canary.CycleResult(
            success=True,
            target_success={ROUTE_A: True},
            target_last_run={ROUTE_A: 1.0},
            target_capacity_skipped={ROUTE_A: False},
        )
    )
    monitor.observe(
        canary.CycleResult(
            success=True,
            target_last_run={ROUTE_A: 2.0},
            target_capacity_skipped={ROUTE_A: True},
        )
    )
    lines = monitor.metrics_text().splitlines()
    assert [line for line in lines if line.startswith("turn_canary_target_success{")] == [
        line
        for line in lines
        if line.startswith("turn_canary_target_success{") and line.endswith(" 1")
    ]
    assert any(line.startswith("turn_canary_target_success{") for line in lines)
    assert any(
        line.startswith("turn_canary_target_capacity_skipped{") and line.endswith(" 1")
        for line in lines
    )


def test_exporter_publishes_no_failure_for_a_never_turned_skipped_route():
    """@spec TURN-CANARY-6 TURN-CANARY-10"""
    canary = _canary()
    platform = RoutePlatform(headroom=[False])
    monitor = canary.Monitor(None, canary.Limits())
    result = asyncio.run(
        canary.run_cycle(
            platform, _routes()[:1], turn_deadline=0.05, cleanup_deadline=0.02, poll_period=0.005
        )
    )
    monitor.observe(result)
    lines = monitor.metrics_text().splitlines()
    assert not any(line.startswith("turn_canary_target_success{") for line in lines)
    assert "turn_canary_cycle_success 1" in lines
    assert any(
        line.startswith("turn_canary_target_capacity_skipped{") and line.endswith(" 1")
        for line in lines
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("16", "16"),
        (16, "16"),
        ("1500m", "1.5"),
        ("13500m", "13.5"),
        ("0.5", "0.5"),
        ("512Mi", str(512 * 2**20)),
        ("2Gi", str(2 * 2**30)),
        ("1Ki", "1024"),
        ("2k", "2000"),
        ("2K", "2000"),
        ("3M", "3000000"),
        ("1G", "1000000000"),
        ("250u", "0.00025"),
        ("1e3", "1000"),
    ],
)
def test_kubernetes_quantities_parse(raw, expected):
    """@spec TURN-CANARY-5"""
    from decimal import Decimal

    assert _canary().parse_quantity(raw) == Decimal(expected)


@pytest.mark.parametrize("raw", ["", "abc", "1.5.0", "10Mb", "Gi", True, None, "-"])
def test_malformed_quantities_refuse(raw):
    """@spec TURN-CANARY-5"""
    with pytest.raises(ValueError):
        _canary().parse_quantity(raw)


def _quota_server(items):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Quota(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler name
            body = json.dumps({"items": items}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Quota)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.mark.parametrize(
    ("resource", "hard", "used", "min_free", "expected"),
    [
        ("limits.cpu", "16", "1500m", "1", True),
        ("limits.cpu", "2", "1500m", "1", False),
        ("limits.cpu", "2", "1500m", "500m", True),
        ("limits.memory", "2Gi", "512Mi", "1Gi", True),
        ("limits.memory", "2Gi", "1536Mi", "1Gi", False),
        ("count/pods", "16", "15", "1", True),
        ("pods", "16", "16", "1", False),
        ("limits.cpu", "16", "lots", "1", None),
    ],
)
def test_runtime_quota_headroom_reads_kubernetes_quantities(
    resource, hard, used, min_free, expected
):
    """@spec TURN-CANARY-5"""
    canary = _canary()
    server = _quota_server([{"status": {"hard": {resource: hard}, "used": {resource: used}}}])

    async def scenario():
        platform = canary.RuntimePlatform(
            database_url="postgresql+asyncpg://canary@db.example.invalid/curie",
            valkey_url="redis://valkey.example.invalid:6379/0",
            api_url="https://platform.example.invalid",
            api_key="example-key",
            stream="curie:runs",
            schema="curie",
            read_only_qualified=False,
            quota_url=f"http://127.0.0.1:{server.server_address[1]}/quotas",
            quota_token="example-token",
            quota_resource=resource,
            quota_min_free=min_free,
        )
        try:
            return await platform.quota_headroom()
        finally:
            await platform.close()

    try:
        assert asyncio.run(scenario()) is expected
    finally:
        server.shutdown()
        server.server_close()
