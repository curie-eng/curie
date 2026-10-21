"""Pure time helpers for agent-created schedules (ADR 0238). No database.

A cron expression is five fields read in a schedule's IANA zone's wall time,
the way the worker cron loop reads it (``curie_worker.cron_loop.resolve_slots``,
mirrored here because neither service imports the other's package): a wall
time inside a spring-forward gap never happens and is dropped, and a fold time
fires once, at its first occurrence.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

from cronsim import CronSim, CronSimError

AMBIGUOUS = "ambiguous or nonexistent local time"

# The widest UTC offset change a zone makes in one transition: the slot
# iteration starts this far before the window's local start, so a fold slot
# whose wall time reads earlier is still enumerated (mirrors the worker).
_ITERATION_MARGIN = timedelta(hours=3)
# How far ahead the day rules are read when deciding whether a cron fires on
# two consecutive calendar days: four years covers every leap-day rule.
_DAY_HORIZON_DAYS = 1461
# How far ahead the zone's UTC-offset transitions are examined for a real
# elapsed gap shorter than the wall-clock one.
_TRANSITION_HORIZON_DAYS = 366
_TRANSITION_SPAN = timedelta(hours=26)
_DAY_MINUTES = 1440


@cache
def _zones() -> frozenset[str]:
    return frozenset(available_timezones())


def valid_zone(name: str) -> bool:
    """Whether ``name`` is an IANA zone this host knows."""

    if name not in _zones():
        return False
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    return True


def parse_cron(cron: str) -> CronSim:
    """A five-field cron expression, parsed; ValueError when it is not one."""

    if len(cron.split()) != 5:
        raise ValueError("cron must have exactly five fields")
    try:
        return CronSim(cron, datetime(2000, 1, 1))
    except CronSimError as exc:
        raise ValueError(f"cron is not valid: {exc}") from None


def resolve_slots(
    cron: str, zone: str, window_start: datetime, window_end: datetime
) -> list[datetime]:
    """Every cron slot in (window_start, window_end], ascending aware UTC."""

    tz = ZoneInfo(zone)
    local_start = window_start.astimezone(tz).replace(tzinfo=None) - _ITERATION_MARGIN
    local_end = window_end.astimezone(tz).replace(tzinfo=None) + _ITERATION_MARGIN
    slots: list[datetime] = []
    for wall in CronSim(cron, local_start):
        if wall > local_end:
            break
        instant = wall.replace(tzinfo=tz, fold=0)
        if instant.astimezone(UTC).astimezone(tz).replace(tzinfo=None) != wall:
            continue
        utc = instant.astimezone(UTC)
        if window_start < utc <= window_end:
            slots.append(utc)
    slots.sort()
    return slots


def _fires_on(sim: CronSim, day: date) -> bool:
    return day.month in sim.months and sim.match_day(day)


def _transitions(zone: str, now: datetime) -> list[datetime]:
    """For each offset change of ``zone`` in the horizon, the instant up to one
    day before it at which the old offset still held."""

    tz = ZoneInfo(zone)
    found: list[datetime] = []
    previous = now
    offset = now.astimezone(tz).utcoffset()
    for step in range(1, _TRANSITION_HORIZON_DAYS + 1):
        instant = now + timedelta(days=step)
        current = instant.astimezone(tz).utcoffset()
        if current != offset:
            found.append(previous)
            offset = current
        previous = instant
    return found


def min_interval_minutes(cron: str, zone: str, now: datetime) -> int | None:
    """The shortest real gap, in whole minutes, between two firings of ``cron``.

    The smallest of three parts: (a) the gaps between consecutive minute-of-day
    values the minute and hour fields allow; (b) the gap across midnight from
    the last to the first of them, counted only between two firing days, using
    the closest pair of firing days in the next 1,461 days; (c) the real elapsed
    gaps between slots around each UTC-offset transition of ``zone`` in the
    next 366 days. Day rules only lengthen (a), so it is a lower bound.

    Returns None when the cron fires fewer than twice within the horizon.
    """

    sim = parse_cron(cron)
    times = sorted({hour * 60 + minute for hour in sim.hours for minute in sim.minutes})
    today = now.astimezone(ZoneInfo(zone)).date()
    firing_days = [
        day
        for day in (today + timedelta(days=offset) for offset in range(_DAY_HORIZON_DAYS))
        if _fires_on(sim, day)
    ]
    if not firing_days:
        return None
    candidates: list[int] = [
        later - earlier for earlier, later in zip(times, times[1:], strict=False)
    ]
    if len(firing_days) > 1:
        closest = min((b - a).days for a, b in zip(firing_days, firing_days[1:], strict=False))
        candidates.append(closest * _DAY_MINUTES - times[-1] + times[0])
    for before in _transitions(zone, now):
        # The change lies within a day after ``before``; the span on each side
        # reaches the slot either side of it for any schedule up to daily.
        slots = resolve_slots(
            cron, zone, before - _TRANSITION_SPAN, before + timedelta(days=1) + _TRANSITION_SPAN
        )
        candidates.extend(
            int((later - earlier).total_seconds() // 60)
            for earlier, later in zip(slots, slots[1:], strict=False)
        )
    return min(candidates) if candidates else None


def next_run(
    cron: str | None, run_at: datetime | None, zone: str, now: datetime
) -> datetime | None:
    """The next instant (UTC) the schedule fires after ``now``; None when it
    will not: a one-shot whose time has passed, or a cron with no slot in the
    next 1,461 days."""

    if cron is None:
        if run_at is None or run_at <= now:
            return None
        return run_at.astimezone(UTC)
    tz = ZoneInfo(zone)
    local_now = now.astimezone(tz).replace(tzinfo=None)
    horizon = now + timedelta(days=_DAY_HORIZON_DAYS)
    for wall in CronSim(cron, local_now - _ITERATION_MARGIN):
        instant = wall.replace(tzinfo=tz, fold=0)
        if instant.astimezone(UTC).astimezone(tz).replace(tzinfo=None) != wall:
            continue
        utc = instant.astimezone(UTC)
        if utc > horizon:
            return None
        if utc > now:
            return utc
    return None


def local_label(instant: datetime, zone: str) -> str:
    """``instant`` as a person in ``zone`` reads it, naming the zone:
    "Mon 2026-10-26 08:00 EDT (America/New_York)"."""

    local = instant.astimezone(ZoneInfo(zone))
    return f"{local:%a %Y-%m-%d %H:%M} {local.tzname()} ({zone})"


def parse_run_at(text: str, zone: str) -> datetime:
    """A one-shot's time as an aware UTC instant.

    An explicit offset wins over ``zone``; a naive time is read as wall time
    in ``zone`` and refused (ValueError naming "ambiguous or nonexistent local
    time") when it falls in a fold or a gap, so the caller asks which is meant.
    """

    raw = text.strip()
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError("run_at must be an ISO 8601 date and time") from None
    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        return parsed.astimezone(UTC)
    tz = ZoneInfo(zone)
    first = parsed.replace(tzinfo=tz, fold=0)
    second = parsed.replace(tzinfo=tz, fold=1)
    if first.utcoffset() != second.utcoffset():
        raise ValueError(
            f"{parsed.isoformat()} is an {AMBIGUOUS} in {zone}; "
            "give an explicit UTC offset to say which is meant"
        )
    return first.astimezone(UTC)
