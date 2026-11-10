"""Broker metadata operations, @spec PROTECTED-HOOK-LANE-3/SOURCE-6.

Separate observations do not establish atomic admission or readiness.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

from curie_protected_hooks.source_fence import SourceFence, SourceState

# @spec PROTECTED-HOOK-LANE-2/3
_MAX_MILLISECOND = 9007199254740991
_RUN_ID = re.compile(r"[0-9a-f]{40}", re.ASCII)
_CONTROL_KEY = re.compile(r"protected:control:[A-Za-z0-9:_-]{1,256}", re.ASCII)

# @spec PROTECTED-HOOK-LANE-3
_PERMISSION_RESET = ("-@all", "resetkeys", "resetchannels", "clearselectors")
_HANDSHAKE = ("+auth", "+hello", "+ping", "+client|setname", "+client|setinfo")
_SOURCE_WRITER = (
    *_PERMISSION_RESET,
    "%RW~protected:source:*",
    "+get",
    "+set",
    "+eval",
    "+evalsha",
    "+script|load",
    *_HANDSHAKE,
)
_CONTROL_READER = (
    *_PERMISSION_RESET,
    "%R~protected:source:*",
    "%R~protected:control:*",
    "+get",
    "+info|server",
    "+time",
    *_HANDSHAKE,
    "(+eval %RW~protected:source:* %RW~protected:control:*)",
)


def metadata_acl_rules(role: str) -> tuple[str, ...]:
    """Return the closed permission subset, @spec PROTECTED-HOOK-LANE-3."""
    if type(role) is str:
        if role == "source_writer":
            return _SOURCE_WRITER
        if role == "control_reader":
            return _CONTROL_READER
    raise ValueError("unknown broker metadata role")


class BrokerMetadataUnavailable(Exception):
    """Safe broker refusal, @spec PROTECTED-HOOK-LANE-3."""

    def __init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        super().__init__("Broker metadata unavailable")


@dataclass(frozen=True, slots=True)
class BrokerObservation:
    """Validated broker facts, @spec PROTECTED-HOOK-LANE-2/3."""

    run_id: str
    now_ms: int

    def __post_init__(self) -> None:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        if (
            type(self.run_id) is not str
            or _RUN_ID.fullmatch(self.run_id) is None
            or type(self.now_ms) is not int
            or not 0 <= self.now_ms <= _MAX_MILLISECOND
        ):
            raise ValueError("invalid broker observation")


class AuthorityMetadataReader:
    """Scoped metadata consumer, @spec PROTECTED-HOOK-LANE-3/SOURCE-6."""

    def __init__(self, client: Redis) -> None:
        """@spec PROTECTED-HOOK-LANE-3."""
        self._client = client
        self._source = SourceFence(client)

    def read_source(self, agent_id: str, hook: str) -> SourceState:
        """@spec PROTECTED-HOOK-LANE-3/SOURCE-6."""
        try:
            return self._source.read(agent_id, hook)
        except RedisError:
            raise BrokerMetadataUnavailable() from None

    def read_control(self, key: str) -> bytes | None:
        """@spec PROTECTED-HOOK-LANE-3."""
        if type(key) is not str or _CONTROL_KEY.fullmatch(key) is None:
            raise ValueError("invalid broker control key")
        try:
            raw: Any = self._client.get(key)
            if raw is not None and type(raw) is not bytes:
                raise BrokerMetadataUnavailable()
            return raw
        except (RedisError, UnicodeError):
            raise BrokerMetadataUnavailable() from None

    def observe(self) -> BrokerObservation:
        """@spec PROTECTED-HOOK-LANE-2/3."""
        try:
            info: Any = self._client.info("server")
            clock: Any = self._client.time()
            if (
                type(info) is not dict
                or type(clock) is not tuple
                or len(clock) != 2
                or type(clock[0]) is not int
                or type(clock[1]) is not int
                or clock[0] < 0
                or not 0 <= clock[1] < 1000000
            ):
                raise BrokerMetadataUnavailable()
            run_id = info.get("run_id")
            if type(run_id) is not str:
                raise BrokerMetadataUnavailable()
            return BrokerObservation(
                run_id=run_id, now_ms=clock[0] * 1000 + clock[1] // 1000
            )
        except (RedisError, ValueError, TypeError, OverflowError, IndexError):
            raise BrokerMetadataUnavailable() from None
