"""Pure time helpers for agent-created schedules (ADR 0248).

`curie_api.agent_schedule_time` decides the minimum interval a cron expression
can fire at, the next run of a cron or one-shot schedule in its zone, the label
a person reads, and how a run_at string becomes an instant. No database.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from curie_api.agent_schedule_time import (
    local_label,
    min_interval_minutes,
    next_run,
    parse_run_at,
)

NY = "America/New_York"
NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
# Before the 2027-03-14 spring-forward in America/New_York.
BEFORE_SPRING = datetime(2027, 3, 1, 0, 0, tzinfo=UTC)
AMBIGUOUS = "ambiguous or nonexistent local time"


def test_every_fifteen_minutes_is_fifteen() -> None:
    assert min_interval_minutes("*/15 * * * *", "UTC", NOW) == 15


def test_day_restriction_does_not_hide_a_short_gap() -> None:
    # Fires every ten minutes on January 1: the restriction only lengthens
    # gaps between days, never the ones within a firing day.
    assert min_interval_minutes("*/10 * 1 1 *", "UTC", NOW) == 10


def test_weekday_morning_is_at_least_a_day() -> None:
    result = min_interval_minutes("0 9 * * 1-5", "UTC", NOW)
    assert result is not None
    assert result >= 1440


def test_yearly_schedule_is_not_refused_at_fifteen() -> None:
    result = min_interval_minutes("0 9 1 1 *", "UTC", NOW)
    assert result is None or result >= 15


def test_wrap_gap_only_counts_on_consecutive_days() -> None:
    # 23:55 to 00:05 would be 10 minutes, but the schedule fires only on the
    # first of the month, so no two firing days are consecutive.
    assert min_interval_minutes("5,55 0,23 1 * *", "UTC", NOW) == 50


def test_spring_forward_shortens_a_real_gap() -> None:
    # 01:55 EST and 03:05 EDT on the spring-forward day are 10 real minutes apart.
    assert min_interval_minutes("5,55 1,3 * * *", NY, BEFORE_SPRING) == 10


def test_same_cron_in_utc_has_no_dst_shortening() -> None:
    assert min_interval_minutes("5,55 1,3 * * *", "UTC", BEFORE_SPRING) == 50


def test_next_run_for_cron_in_zone() -> None:
    # Tuesday 2026-10-20; next Monday 08:00 EDT is 12:00 UTC.
    got = next_run("0 8 * * 1", None, NY, NOW)
    assert got == datetime(2026, 10, 26, 12, 0, tzinfo=UTC)


def test_next_run_for_cron_across_fall_back() -> None:
    # 2026-10-31 08:00 EDT already passed; 2026-11-01 08:00 is EST (UTC-5).
    now = datetime(2026, 10, 31, 13, 0, tzinfo=UTC)
    got = next_run("0 8 * * *", None, NY, now)
    assert got == datetime(2026, 11, 1, 13, 0, tzinfo=UTC)


def test_next_run_for_future_run_at() -> None:
    run_at = NOW + timedelta(days=3)
    got = next_run(None, run_at, NY, NOW)
    assert got == run_at


def test_local_label_names_weekday_abbreviation_and_zone() -> None:
    instant = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
    assert local_label(instant, NY) == "Mon 2026-10-26 08:00 EDT (America/New_York)"


def test_local_label_in_winter_uses_standard_time() -> None:
    instant = datetime(2026, 12, 7, 14, 30, tzinfo=UTC)
    assert local_label(instant, NY) == "Mon 2026-12-07 09:30 EST (America/New_York)"


def test_local_label_utc() -> None:
    assert local_label(NOW, "UTC") == "Tue 2026-10-20 12:00 UTC (UTC)"


def test_parse_run_at_accepts_offset_aware() -> None:
    got = parse_run_at("2026-12-01T09:00:00-05:00", NY)
    assert got == datetime(2026, 12, 1, 14, 0, tzinfo=UTC)
    assert got.utcoffset() is not None


def test_parse_run_at_offset_wins_over_zone() -> None:
    got = parse_run_at("2026-12-01T09:00:00+00:00", NY)
    assert got == datetime(2026, 12, 1, 9, 0, tzinfo=UTC)


def test_parse_run_at_reads_naive_as_local_time_in_zone() -> None:
    assert parse_run_at("2026-12-01T09:00:00", NY) == datetime(2026, 12, 1, 14, 0, tzinfo=UTC)
    assert parse_run_at("2026-07-01T09:00:00", NY) == datetime(2026, 7, 1, 13, 0, tzinfo=UTC)
    assert parse_run_at("2026-07-01T09:00:00", "UTC") == datetime(2026, 7, 1, 9, 0, tzinfo=UTC)


def test_parse_run_at_refuses_a_spring_forward_gap_time() -> None:
    with pytest.raises(ValueError, match=AMBIGUOUS):
        parse_run_at("2026-03-08T02:30:00", NY)


def test_parse_run_at_refuses_a_fall_back_fold_time() -> None:
    with pytest.raises(ValueError, match=AMBIGUOUS):
        parse_run_at("2026-11-01T01:30:00", NY)


def test_parse_run_at_accepts_times_next_to_the_transitions() -> None:
    # Liveness for the two refusals: the unambiguous minutes around them parse.
    assert parse_run_at("2026-03-08T03:30:00", NY) == datetime(2026, 3, 8, 7, 30, tzinfo=UTC)
    assert parse_run_at("2026-11-01T02:30:00", NY) == datetime(2026, 11, 1, 7, 30, tzinfo=UTC)
    # An explicit offset settles a fold time.
    assert parse_run_at("2026-11-01T01:30:00-04:00", NY) == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
