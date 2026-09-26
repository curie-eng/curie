"""Turns between an installation's own identities are rate limited (ADR-0168 decision 6).

Admission belongs to each ingress; this is the worker's half, where every
ingress's turns converge before a sandbox starts. It only ever drops a turn and
grants nothing, so it is a rate limit, not a provenance check.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from enum import StrEnum
from typing import Final, Protocol
from urllib.parse import quote

from aci_protocol.turn import SLACK_KIND, route_identity
from redis.asyncio import Redis
from slack_sdk.web.async_client import AsyncWebClient

from .config import WorkerConfig

logger = logging.getLogger(__name__)

#: Sibling-written turns one session key admits per window.
SIBLING_TURN_LIMIT: Final = 5
#: Conversations one ordered pair of identities may open per window.
SIBLING_OPEN_LIMIT: Final = 5
#: The fixed window both counters share, in seconds.
SIBLING_WINDOW_SECONDS: Final = 600

#: What a dropped turn's placeholder is completed with. It names nobody, so it
#: cannot mention a sibling back into the exchange it ends.
SIBLING_LIMIT_NOTICE: Final = (
    "Stopped here: the bots in this installation have messaged each other too "
    "often. A person can pick this up."
)

_AUTH_TEST_TIMEOUT_S: Final = 5.0
_AUTH_TEST_RETRY_S: Final = 300.0

# KEYS: 1 the session key's counter, 2 the ordered pair's counter.
# ARGV: 1 window seconds, 2 turn limit, 3 open limit.
# Returns 0 admit, 1 over the turn limit, 2 over the open limit. The first
# sibling turn on a session key is the one that opens it, so only that turn
# moves the pair counter; a refused opening marks its session past the limit
# so later sibling turns in it are refused too.
_COUNT_LUA = """
local turns = redis.call('INCR', KEYS[1])
if turns == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
    local opened = redis.call('INCR', KEYS[2])
    if opened == 1 then
        redis.call('EXPIRE', KEYS[2], ARGV[1])
    end
    if opened > tonumber(ARGV[3]) then
        redis.call('SET', KEYS[1], tonumber(ARGV[2]) + 1, 'EX', ARGV[1])
        return 2
    end
end
if turns > tonumber(ARGV[2]) then
    return 1
end
return 0
"""


class SiblingLimitReason(StrEnum):
    """Why a sibling-written turn was dropped, as a stable log token."""

    CONVERSATION = "sibling_conversation_limit"
    PAIR = "sibling_pair_limit"


class SlackSenders(Protocol):
    async def identity_of(self, author: str) -> str | None: ...


class AddressIdentityLookup(Protocol):
    async def identity_for_address(self, kind: str, address: str) -> str | None: ...


class SlackSenderIdentities:
    """Each Slack identity's bot user id, from ``auth.test`` on its own token.

    Asked lazily, on the first lookup, so a worker whose Slack is unreachable
    still boots; an identity that did not answer is asked again no sooner than
    ``retry_after_s`` later, and until then its turns are not counted.
    """

    def __init__(
        self,
        tokens: Mapping[str, str],
        *,
        base_url: str | None = None,
        timeout_s: float = _AUTH_TEST_TIMEOUT_S,
        retry_after_s: float = _AUTH_TEST_RETRY_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._tokens = {name: token for name, token in tokens.items() if token.strip()}
        self._base_url = base_url or None
        self._timeout_s = timeout_s
        self._retry_after_s = retry_after_s
        self._clock = clock
        self._by_user: dict[str, str] = {}
        self._known: set[str] = set()
        self._next_try: dict[str, float] = {}
        self._lock = asyncio.Lock()

    @property
    def identities(self) -> tuple[str, ...]:
        return tuple(self._tokens)

    async def identity_of(self, author: str) -> str | None:
        if self._due():
            async with self._lock:
                due = self._due()
                answers = await asyncio.gather(*(self._ask(name) for name in due))
                for name, user_id in zip(due, answers, strict=True):
                    if user_id is None:
                        self._next_try[name] = self._clock() + self._retry_after_s
                        logger.warning(
                            "Slack identity %s: auth.test did not answer; turns its bot "
                            "writes are not counted against the sibling limit yet",
                            name,
                        )
                        continue
                    self._known.add(name)
                    self._by_user[user_id] = name
        return self._by_user.get(author)

    def _due(self) -> list[str]:
        now = self._clock()
        return [
            name
            for name in self._tokens
            if name not in self._known and self._next_try.get(name, 0.0) <= now
        ]

    async def _ask(self, name: str) -> str | None:
        kwargs: dict[str, object] = {
            "token": self._tokens[name],
            "timeout": max(1, int(self._timeout_s)),
        }
        if self._base_url is not None:
            kwargs["base_url"] = self._base_url
        client = AsyncWebClient(**kwargs)  # type: ignore[arg-type]
        try:
            response = await asyncio.wait_for(client.auth_test(), timeout=self._timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception:
            return None
        user_id = response.get("user_id") if response.get("ok") is True else None
        return user_id if isinstance(user_id, str) and user_id else None


class SiblingTurnLimit:
    """The two fixed-window counters a sibling-written turn counts against."""

    def __init__(
        self,
        redis: Redis,
        *,
        key_prefix: str,
        slack: SlackSenders | None = None,
        channel: AddressIdentityLookup | None = None,
    ) -> None:
        self._redis = redis
        self._prefix = f"{key_prefix}:sibling"
        self.slack = slack
        self.channel = channel

    async def sender_identity(self, kind: str, author: str) -> str | None:
        """The identity that wrote ``author``'s turn on ``kind``, or None."""

        if not author.strip():
            return None
        if kind == SLACK_KIND:
            return await self.slack.identity_of(author) if self.slack is not None else None
        if self.channel is None:
            return None
        return await self.channel.identity_for_address(kind, author)

    async def check(
        self, *, kind: str, adapter: str | None, author: str, session_key: str
    ) -> SiblingLimitReason | None:
        """Count a sibling-written turn and name why it is refused, or None.

        ``session_key`` is ``kernel._thread_key_for``'s key (ADR-0168 decision 4).
        A turn no identity wrote returns None before any Valkey call.
        """

        sender = await self.sender_identity(kind, author)
        if sender is None:
            return None
        addressed = route_identity(kind, adapter) or ""
        pair = ":".join(quote(part, safe="") for part in (kind, sender, addressed))
        verdict = int(
            await self._redis.eval(
                _COUNT_LUA,
                2,
                f"{self._prefix}:conversation:{session_key}",
                f"{self._prefix}:pair:{pair}",
                SIBLING_WINDOW_SECONDS,
                SIBLING_TURN_LIMIT,
                SIBLING_OPEN_LIMIT,
            )
        )
        if verdict == 1:
            return SiblingLimitReason.CONVERSATION
        if verdict == 2:
            return SiblingLimitReason.PAIR
        return None


def build_sibling_limit(
    config: WorkerConfig,
    redis: Redis,
    *,
    slack_tokens: Mapping[str, str],
    channel: AddressIdentityLookup,
) -> SiblingTurnLimit | None:
    """The limiter, or None when this installation has no sibling to count.

    A second declared Slack identity wires the Slack senders; a second adapter
    credential wires the address lookup, since every adapter that receives
    replies holds one.
    """

    slack = (
        SlackSenderIdentities(slack_tokens, base_url=config.slack_api_base_url or None)
        if len(config.slack_identities) > 1
        else None
    )
    lookup = channel if len(config.adapter_credentials) > 1 else None
    if slack is None and lookup is None:
        return None
    return SiblingTurnLimit(redis, key_prefix=config.key_prefix, slack=slack, channel=lookup)
