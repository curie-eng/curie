"""Pure slot resolution for the cron scheduler (#268).

``resolve_slots`` iterates a cron expression over NAIVE local wall-clock time in
the hook's zone and maps each wall time to an aware UTC instant. A wall time in
a forward DST gap does not exist and is dropped; an ambiguous wall time in a
backward fold fires once, at its first occurrence. The window is (start, end].
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from curie_worker.cron_loop import CATCH_UP_CEILING, plan_catch_up, resolve_slots, slot_is_stale

NY = ZoneInfo("America/New_York")


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


def test_same_expression_differs_between_utc_and_an_explicit_zone() -> None:
    start, end = _utc(2026, 6, 1, 0, 0), _utc(2026, 6, 2, 0, 0)

    assert resolve_slots("0 9 * * *", "UTC", start, end) == [_utc(2026, 6, 1, 9, 0)]
    # June is EDT (UTC-4): 09:00 local is 13:00Z.
    assert resolve_slots("0 9 * * *", "America/New_York", start, end) == [
        _utc(2026, 6, 1, 13, 0)
    ]


def test_slots_are_aware_utc_and_ascending() -> None:
    slots = resolve_slots(
        "0 * * * *", "America/New_York", _utc(2026, 6, 1, 0, 0), _utc(2026, 6, 1, 5, 0)
    )
    assert len(slots) == 5
    assert slots == sorted(slots)
    assert all(s.utcoffset() == timedelta(0) for s in slots)


def test_forward_gap_wall_time_does_not_exist() -> None:
    # 2026-03-08 02:30 America/New_York falls in the spring-forward gap.
    slots = resolve_slots(
        "30 2 * * *",
        "America/New_York",
        _utc(2026, 3, 7, 12, 0),
        _utc(2026, 3, 9, 12, 0),
    )
    # Only the 9th fires (02:30 EDT = 06:30Z); nothing is invented for the 8th.
    assert slots == [_utc(2026, 3, 9, 6, 30)]


def test_backward_fold_wall_time_fires_exactly_once() -> None:
    # 2026-11-01 01:30 America/New_York happens twice: 05:30Z (EDT), 06:30Z (EST).
    slots = resolve_slots(
        "30 1 * * *",
        "America/New_York",
        _utc(2026, 10, 31, 12, 0),
        _utc(2026, 11, 1, 12, 0),
    )
    assert slots == [_utc(2026, 11, 1, 5, 30)]


def test_nine_am_local_stays_nine_am_across_both_transitions() -> None:
    for start, end, days in (
        (_utc(2026, 3, 5, 12, 0), _utc(2026, 3, 11, 12, 0), 6),
        (_utc(2026, 10, 29, 12, 0), _utc(2026, 11, 4, 12, 0), 6),
    ):
        slots = resolve_slots("0 9 * * *", "America/New_York", start, end)
        assert len(slots) == days
        local = [s.astimezone(NY) for s in slots]
        assert all((t.hour, t.minute) == (9, 0) for t in local), local
        # The UTC offset really does change inside each window.
        assert len({t.utcoffset() for t in local}) == 2


def test_window_excludes_start_and_includes_end() -> None:
    slot = _utc(2026, 6, 1, 9, 0)
    assert resolve_slots("0 9 * * *", "UTC", slot, slot + timedelta(hours=1)) == []
    assert resolve_slots("0 9 * * *", "UTC", slot - timedelta(hours=1), slot) == [slot]


# Bounded catch-up (ADR-0099, #2930): the newest missed slot fires once, every
# older one is recorded skipped, and a newest slot past the age bound is
# skipped too. The bound is the schedule's own interval, capped by a ceiling.


def test_catch_up_fires_the_newest_slot_and_skips_every_older_one() -> None:
    due = [_utc(2026, 6, 1, h, 0) for h in (6, 7, 8)]
    fire, skipped = plan_catch_up("0 * * * *", "UTC", due, _utc(2026, 6, 1, 8, 20))
    assert fire == _utc(2026, 6, 1, 8, 0)
    assert skipped == due[:2]


def test_catch_up_with_one_due_slot_fires_it_and_skips_nothing() -> None:
    slot = _utc(2026, 6, 1, 9, 0)
    assert plan_catch_up("0 9 * * *", "UTC", [slot], slot + timedelta(hours=2)) == (slot, [])


def test_catch_up_with_nothing_due_does_nothing() -> None:
    assert plan_catch_up("0 9 * * *", "UTC", [], _utc(2026, 6, 1, 9, 0)) == (None, [])


def test_a_coarse_schedule_past_the_ceiling_fires_nothing() -> None:
    # A monthly hook four weeks late starts fresh: its newest slot is skipped.
    slot = _utc(2026, 6, 1, 9, 0)
    now = slot + timedelta(weeks=4)
    assert slot_is_stale("0 9 1 * *", "UTC", slot, now)
    assert plan_catch_up("0 9 1 * *", "UTC", [slot], now) == (None, [slot])


def test_a_coarse_schedule_inside_the_ceiling_still_fires() -> None:
    slot = _utc(2026, 6, 1, 9, 0)
    now = slot + CATCH_UP_CEILING - timedelta(minutes=1)
    assert not slot_is_stale("0 9 1 * *", "UTC", slot, now)
    assert plan_catch_up("0 9 1 * *", "UTC", [slot], now) == (slot, [])


def test_a_slot_older_than_its_own_interval_is_stale() -> None:
    # Hourly: the 08:00 slot is stale once 09:00 has come due, well under the ceiling.
    slot = _utc(2026, 6, 1, 8, 0)
    assert not slot_is_stale("0 * * * *", "UTC", slot, _utc(2026, 6, 1, 8, 59))
    assert slot_is_stale("0 * * * *", "UTC", slot, _utc(2026, 6, 1, 9, 1))


def test_the_interval_is_measured_in_the_hooks_zone_across_dst() -> None:
    # 09:00 New York on 2026-03-07 (14:00Z); the next is 03-08 09:00 EDT (13:00Z),
    # a 23 h interval across spring forward. 23.5 h later the slot is stale.
    slot = _utc(2026, 3, 7, 14, 0)
    assert not slot_is_stale("0 9 * * *", "America/New_York", slot, slot + timedelta(hours=22))
    assert slot_is_stale(
        "0 9 * * *", "America/New_York", slot, slot + timedelta(hours=23, minutes=30)
    )
