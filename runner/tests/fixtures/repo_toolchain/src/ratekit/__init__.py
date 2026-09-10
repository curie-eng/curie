"""Parsing helpers for the rate-limit specs used across the billing service.

A rate spec is written ``<count>/<window>``, where the window is an integer
followed by a unit suffix: ``s`` seconds, ``m`` minutes, ``h`` hours. For
example ``"120/1m"`` means one hundred and twenty requests per minute.
"""

from __future__ import annotations

__version__ = "1.2.0"

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}


def parse_rate(spec: str) -> tuple[int, int]:
    """Return ``(count, window_seconds)`` for a rate spec such as ``"120/1m"``."""
    text = spec.strip()
    if "/" not in text:
        raise ValueError(f"rate spec must look like '<count>/<window>': {spec!r}")
    count_text, window_text = text.split("/", 1)
    count = int(count_text)
    if count < 0:
        raise ValueError(f"rate count must not be negative: {spec!r}")
    unit = window_text[-1:]
    if unit not in _UNIT_SECONDS:
        raise ValueError(f"unknown rate window unit {unit!r} in {spec!r}")
    magnitude = int(window_text[:-1] or "1")
    if magnitude <= 0:
        raise ValueError(f"rate window must be positive: {spec!r}")
    return count, magnitude * _UNIT_SECONDS[unit]


def per_second(spec: str) -> float:
    """Return the sustained requests-per-second implied by a rate spec."""
    count, window_seconds = parse_rate(spec)
    return count / window_seconds
