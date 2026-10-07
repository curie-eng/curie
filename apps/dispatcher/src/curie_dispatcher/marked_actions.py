"""Admission of a declared bot's marked actions (ADR 0202 decisions 2–3)."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from curie_internal.driver_declaration import DeclaredDriver

from .config import DispatcherConfig

if TYPE_CHECKING:
    from redis import Redis

MARK = "[test action]"
REFUSAL = "This installation does not accept test actions."
WINDOW_SECONDS = 600


# SET NX EX reserves one of the bounded slots atomically across replicas. All
# drivers addressing this thread share them. Each slot expires after ten
# minutes, so no ten-minute interval can admit more than the configured cap.
def reserve_turn(
    redis: Redis, config: DispatcherConfig, *, identity: str, channel: str, thread: str
) -> bool:
    digest = hashlib.sha256(json.dumps([identity, channel, thread]).encode()).hexdigest()
    for slot in range(config.test_installation_thread_turn_limit):
        key = f"{config.dedupe_prefix}test-action-budget:{digest}:{slot}"
        if redis.set(key, "1", nx=True, ex=WINDOW_SECONDS):
            return True
    return False


def declared_driver(config: DispatcherConfig, event: dict[str, Any]) -> DeclaredDriver | None:
    """Look up only Slack's bot identity; never trust event.user or text."""
    matching = [
        driver
        for driver in config.test_installation_drivers
        if driver.bot_id == event.get("bot_id")
    ]
    return next(
        (driver for driver in matching if driver.channel_id == event.get("channel")),
        matching[0] if matching else None,
    )
