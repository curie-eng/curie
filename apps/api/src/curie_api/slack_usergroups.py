"""The Slack adapter behind the group-membership port (#420, ADR-0010, ADR-0034).

``usergroups.GroupMembershipSource`` is the port and states the contract; this is
its one implementation, and the only place the API asks Slack who is in a group.

The lookup is one GET against ``usergroups.users.list``, a Slack Tier 2 method
(~20 requests per minute). A busy approval channel clicks far faster than that,
so member sets are cached per group for ``ttl_s``; the cost is that a
revocation takes up to that long to bite, which is negligible against a human
approval flow measured in hours. The cache alone does not deliver that headroom:
a burst of clicks on one group would all miss together and fan out one request
each, so misses for a group are single-flighted: the first caller starts the
fetch, later callers await that same in-flight fetch, and only one reaches Slack.
Single-flighting the FETCH rather than the cache entry is what makes that true of
failures too. Sharing only the cached result coalesces successes and leaves
failures to run one after another behind the lock that was meant to help,
turning a Slack outage into N sequential timeouts, which is worse than no
coalescing at all. Failures are still deliberately NOT cached: caching one Slack
blip would extend it into a TTL-long outage of every group-bound approval, and
would keep the fail-closed denial sticky long after Slack came back. The
in-flight entry is dropped the moment the fetch settles either way, so a later
caller retries rather than replaying the failure.

Every failure mode -- HTTP error, network error, ``ok: false``, a body without
a member list -- raises the port's ``UserGroupLookupError``, which is the whole
of what the port promises: none of them yields a member set, and a lookup that
produced no member set must never be mistaken for a group with no members.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import UTC, datetime

import httpx

from .usergroups import (
    GroupMembershipSource,
    UserGroupLookupError,
    UserGroupMembership,
)

_USERGROUPS_USERS_LIST = "https://slack.com/api/usergroups.users.list"


class SlackUserGroupClient:
    """Reads Slack user-group membership, with a per-group TTL cache.

    The ``httpx.AsyncClient`` is injected so the app can share one client and
    tests can drive a ``MockTransport``; ``clock`` is injected so the TTL window
    and the recorded ``fetched_at`` come from one source that tests can crank
    without sleeping.
    """

    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        token: str,
        ttl_s: float = 60.0,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        mono: Callable[[], float] = time.monotonic,
    ) -> None:
        self._http = http
        self._token = token
        self._ttl_s = ttl_s
        # ``clock`` is the WALL clock, stamped onto the audit-facing ``fetched_at``
        # ("when Slack answered"). ``mono`` is a MONOTONIC clock used ONLY to age
        # the cache entry (#424): a wall clock can jump backward (NTP correction,
        # manual set), which would make a fresh entry look ancient (premature
        # refetch) or a stale one look fresh (never expires). TTL is a duration, so
        # it must be measured against a clock that only moves forward.
        self._clock = clock
        self._mono = mono
        self._cache: dict[str, tuple[datetime, float, frozenset[str]]] = {}
        # One in-flight fetch per group, not one global one: a slow lookup of
        # one group must not stall an unrelated group's approvals behind it. The
        # map holds only fetches that have not settled yet, so it is bounded by
        # the number of groups being resolved at this instant.
        self._inflight: dict[str, asyncio.Task[tuple[datetime, frozenset[str]]]] = {}

    async def members(self, group_id: str) -> UserGroupMembership:
        """The group's members, cached or freshly fetched.

        Concurrent misses for one group share a single fetch, so a click storm
        costs one request rather than one per click. That holds whether the
        fetch succeeds or fails: sharers of a failed fetch all raise, so an
        outage costs one request and one timeout, not one of each per caller.

        Raises ``UserGroupLookupError`` on every failure mode.
        """

        cached = self._cached(group_id)
        if cached is not None:
            return cached
        task = self._inflight.get(group_id)
        # A done task is treated as absent: attaching to one would hand a later
        # caller a settled result (or a settled cancellation) instead of the
        # retry it is owed. The finally below pops the entry before the task
        # settles, so this is belt-and-braces against a leak that would
        # otherwise poison the group permanently rather than for one fetch.
        if task is None or task.done():
            task = asyncio.create_task(self._fetch_shared(group_id))
            self._inflight[group_id] = task
        # Shielded: a caller that gives up (its request was cancelled) must not
        # cancel the fetch the other waiters are still counting on. Sharing an
        # in-flight fetch is deduplication of one simultaneous request, not a
        # cache hit, so it is correct under ttl_s <= 0 too: that lever asks for
        # a fetch per resolve, and these resolves are all riding one fetch that
        # is happening right now.
        fetched_at, users = await asyncio.shield(task)
        return UserGroupMembership(
            group=group_id, users=users, fetched_at=fetched_at, cache_age_s=0.0
        )

    async def _fetch_shared(self, group_id: str) -> tuple[datetime, frozenset[str]]:
        """The single fetch every concurrent caller for this group awaits."""

        try:
            users = await self._fetch(group_id)
            # Stamped after Slack answers, not before the request: a stamp taken
            # up front predates the answer by the request duration, which would
            # open the TTL window early and overstate the freshness of the
            # evidence the audit row records.
            fetched_at = self._clock()
            self._cache[group_id] = (fetched_at, self._mono(), users)
            return fetched_at, users
        finally:
            # In the coroutine's own finally, not a done callback: this runs
            # before the task settles, so there is no window in which a fresh
            # caller could attach to an already-failed fetch and be handed the
            # failure instead of retrying. Both paths drop the entry, so neither
            # a success nor a failure leaks one.
            self._inflight.pop(group_id, None)

    def _cached(self, group_id: str) -> UserGroupMembership | None:
        """The live cache entry for the group, or None to fetch."""

        # ttl_s <= 0 is the documented per-resolve-fetch lever, so it bypasses
        # the cache entirely rather than relying on an age comparison a frozen
        # clock would defeat.
        if self._ttl_s <= 0:
            return None
        entry = self._cache.get(group_id)
        if entry is None:
            return None
        fetched_at, mono_at, users = entry
        age_s = self._mono() - mono_at
        if age_s >= self._ttl_s:
            return None
        return UserGroupMembership(
            group=group_id, users=users, fetched_at=fetched_at, cache_age_s=age_s
        )

    async def _fetch(self, group_id: str) -> frozenset[str]:
        try:
            response = await self._http.get(
                _USERGROUPS_USERS_LIST,
                params={"usergroup": group_id},
                headers={"Authorization": f"Bearer {self._token}"},
            )
            response.raise_for_status()
            body = response.json()
        except httpx.HTTPError as exc:
            raise UserGroupLookupError(f"usergroup {group_id} lookup failed: {exc}") from exc
        except ValueError as exc:  # a 200 that is not JSON at all
            raise UserGroupLookupError(
                f"usergroup {group_id} lookup returned a non-JSON body"
            ) from exc
        if not isinstance(body, dict) or not body.get("ok"):
            error = body.get("error") if isinstance(body, dict) else None
            raise UserGroupLookupError(
                f"usergroup {group_id} lookup refused by Slack: {error or 'unknown error'}"
            )
        users = body.get("users")
        if not isinstance(users, list) or not all(isinstance(u, str) for u in users):
            raise UserGroupLookupError(f"usergroup {group_id} lookup returned no member list")
        return frozenset(users)


# The port is a checked contract rather than a claim: a signature that drifts from
# GroupMembershipSource fails type-checking here, at the adapter, rather than
# type-checking clean and diverging silently from the resolver that depends on it.
_CONFORMS: type[GroupMembershipSource] = SlackUserGroupClient
