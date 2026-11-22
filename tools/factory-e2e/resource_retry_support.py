"""Cluster waits for the opt-in live resource retry proof (#5427).

The proof watches a remote install, so every wait is a bounded poll of
observed state, kept here beside the other live suites' support modules.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


def _pause(seconds: float) -> None:
    threading.Event().wait(max(0.0, seconds))


def poll_until[T](probe: Callable[[], T], *, timeout: float, interval: float) -> T | None:
    """Return the first truthy ``probe()`` within ``timeout`` seconds, else None."""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        _pause(min(interval, remaining))


def observe_for(seconds: float, check: Callable[[], None], *, interval: float) -> None:
    """Run ``check`` every ``interval`` seconds for ``seconds``; a failing check ends it."""
    end = time.monotonic() + seconds
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            return
        _pause(min(interval, remaining))
        check()
