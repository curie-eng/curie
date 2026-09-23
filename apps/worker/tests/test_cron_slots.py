"""Pure slot resolution for the cron scheduler (#268).

``resolve_slots`` iterates a cron expression over NAIVE local wall-clock time in
the hook's zone and maps each wall time to an aware UTC instant. A wall time in
a forward DST gap does not exist and is dropped; an ambiguous wall time in a
backward fold fires once, at its first occurrence. The window is (start, end].
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from curie_worker.cron_loop import resolve_slots

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
