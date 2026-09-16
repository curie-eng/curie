"""Admission decisions for a single account's request budget.

The throttle is deliberately stateless: the caller supplies the number of
requests already counted inside the current window, and this module decides
whether one more request may be admitted.
"""

from __future__ import annotations

from ratekit import parse_rate, per_second


def is_allowed(spec: str, used_in_window: int) -> bool:
    """Return whether one more request fits inside ``spec``'s budget.

    ``used_in_window`` is the number of requests already admitted in the
    current window, so the request under consideration is number
    ``used_in_window + 1``. A budget of ``n`` admits requests 1..n.
    """
    limit, _window_seconds = parse_rate(spec)
    return used_in_window <= limit


def remaining(spec: str, used_in_window: int) -> int:
    """Return how many further requests the window still admits."""
    limit, _window_seconds = parse_rate(spec)
    return max(limit - used_in_window, 0)


def sustained_rate(spec: str) -> float:
    """Return the sustained requests-per-second the spec allows."""
    return per_second(spec)
