"""@spec QUOTA-TIMEOUT-DIAGNOSTIC-2: bounded observations without payloads."""

from __future__ import annotations

import json
import math
import sys
from collections.abc import Sequence
from typing import cast

PREFIX = "Quota burst diagnostic: "
UNAVAILABLE = PREFIX + '{"schema_version":1,"diagnostic_unavailable":true}'


def _duration(value: object) -> int | float:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2."""
    if type(value) not in (int, float):
        raise ValueError
    number = cast(int | float, value)
    if not math.isfinite(number) or number < 0:
        raise ValueError
    return round(number, 3)


def render_summary(
    *,
    elapsed_seconds: object,
    prior_completed_count: object,
    prior_sample_age_seconds: object,
    last_observation_seconds: object,
    max_observation_seconds: object,
    returncodes: object,
) -> str:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2."""
    try:
        if prior_completed_count is None:
            if prior_sample_age_seconds is not None:
                return UNAVAILABLE
            sample_age = None
        else:
            if type(prior_completed_count) is not int or not 0 <= prior_completed_count <= 16:
                return UNAVAILABLE
            sample_age = _duration(prior_sample_age_seconds)
        if type(returncodes) not in (list, tuple):
            return UNAVAILABLE
        codes = cast(Sequence[int | None], returncodes)
        if len(codes) != 16 or any(code is not None and type(code) is not int for code in codes):
            return UNAVAILABLE
        # A single larger integer cannot fit alongside the fixed record fields.
        if any(code is not None and code.bit_length() > 12_000 for code in codes):
            return UNAVAILABLE
        exited = sum(code is not None for code in codes)
        value = {
            "schema_version": 1,
            "elapsed_seconds": _duration(elapsed_seconds),
            "prior_completed_count": prior_completed_count,
            "prior_sample_age_seconds": sample_age,
            "fresh_exited_count": exited,
            "fresh_running_count": 16 - exited,
            "last_observation_seconds": _duration(last_observation_seconds),
            "max_observation_seconds": _duration(max_observation_seconds),
            "processes": [
                {"index": index, "state": "running" if code is None else "exited",
                 "returncode": code}
                for index, code in enumerate(codes)
            ],
            "diagnostic_unavailable": False,
        }
        line = PREFIX + json.dumps(value, separators=(",", ":"), allow_nan=False)
        return line if len((line + "\n").encode("utf-8")) < 4096 else UNAVAILABLE
    except Exception:  # noqa: BLE001  @spec QUOTA-TIMEOUT-DIAGNOSTIC-2: fixed failure boundary.
        return UNAVAILABLE


def emit_summary(**kwargs: object) -> None:
    """@spec QUOTA-TIMEOUT-DIAGNOSTIC-2."""
    try:
        line = render_summary(**kwargs)
    except Exception:  # noqa: BLE001  @spec QUOTA-TIMEOUT-DIAGNOSTIC-2: renderer is best effort.
        line = UNAVAILABLE
    try:
        sys.stdout.write(line + "\n")
    except Exception:  # noqa: BLE001  @spec QUOTA-TIMEOUT-DIAGNOSTIC-2: no second delivery.
        pass
