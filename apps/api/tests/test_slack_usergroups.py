"""SlackUserGroupClient unit tests (#420): the Slack adapter's own lookup.

The issue rejects dispatcher-asserted membership as caller-asserted (and
therefore forgeable) authorization, so the API resolves Slack user-group
membership itself. Slack is an external service and is the only thing faked
here, via ``httpx.MockTransport``; the client under test is real.

Time is injected rather than slept: the client takes a ``clock`` returning an
aware UTC datetime, which the TTL window and the evidence's ``fetched_at`` /
``cache_age_s`` are both computed from. Every TTL assertion below therefore
runs instantly and deterministically.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from curie_api.slack_usergroups import SlackUserGroupClient
from curie_api.usergroups import UserGroupLookupError, UserGroupMembership

_GROUP = "S0MGRS001"
_OTHER_GROUP = "S0LEGAL01"
_TOKEN = "xoxb-fake"  # kept short of the secret scanner's length floor on purpose


class _Clock:
    """A hand-cranked UTC clock. ``advance`` moves it; nothing sleeps."""

    def __init__(self) -> None:
        self.now = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
        # A monotonic reading that advances in lockstep with the wall clock, so a
        # test that cranks the clock ages the cache (now monotonic-based, #424) by
        # the same amount. Seeded off zero -- only deltas matter.
        self._mono = 0.0

    def __call__(self) -> datetime:
        return self.now

    def mono(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
        self._mono += seconds


def _client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    calls: list[httpx.Request],
    clock: _Clock | None = None,
    ttl_s: float = 60.0,
) -> SlackUserGroupClient:
    def _recording(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return handler(request)

    clock = clock or _Clock()
    return SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_recording)),
        token=_TOKEN,
        ttl_s=ttl_s,
        clock=clock,
        mono=clock.mono,
    )


def _async_client(
    handler: Callable[[httpx.Request], Awaitable[httpx.Response]],
    *,
    calls: list[httpx.Request],
    clock: _Clock | None = None,
    ttl_s: float = 60.0,
) -> SlackUserGroupClient:
    """``_client`` for the concurrency tests: the handler is a coroutine, so a
    lookup can be held in flight while other callers pile up behind it."""

    async def _recording(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return await handler(request)

    clock = clock or _Clock()
    return SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_recording)),
        token=_TOKEN,
        ttl_s=ttl_s,
        clock=clock,
        mono=clock.mono,
    )


def _ok(users: list[str]) -> Callable[[httpx.Request], httpx.Response]:
    def _handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "users": users})

    return _handler


def test_members_parses_the_user_list_and_authenticates_with_the_bot_token() -> None:
    """The happy path: GET usergroups.users.list for the requested group with a
    Bearer bot token, and the response's ``users`` become the member set."""

    calls: list[httpx.Request] = []
    client = _client(_ok(["U0APPROV1", "U0LISTED1"]), calls=calls)

    membership = asyncio.run(client.members(_GROUP))

    assert membership.users == frozenset({"U0APPROV1", "U0LISTED1"})
    assert membership.group == _GROUP
    assert len(calls) == 1
    request = calls[0]
    assert request.url.path == "/api/usergroups.users.list"
    assert request.url.host == "slack.com"
    assert request.url.params["usergroup"] == _GROUP
    assert request.headers["authorization"] == f"Bearer {_TOKEN}"


def test_a_fresh_lookup_reports_zero_cache_age() -> None:
    """``cache_age_s`` and ``fetched_at`` are the evidence the audit row carries
    (AC3), so a fresh fetch must report itself as fresh and stamped now."""

    calls: list[httpx.Request] = []
    clock = _Clock()
    client = _client(_ok(["U0APPROV1"]), calls=calls, clock=clock)

    membership = asyncio.run(client.members(_GROUP))

    assert membership.cache_age_s == 0.0
    assert membership.fetched_at == clock.now


def test_fetched_at_is_stamped_when_slack_answers_not_when_it_was_asked() -> None:
    """The stamp must describe the answer, not the question. A stamp taken
    before the request predates Slack's reply by the request duration, which
    both overstates the freshness of the evidence the audit row carries (AC3)
    and opens the TTL window early, pushing a revoked member's real ceiling out
    to ttl_s + latency. The handler advances the clock to spend that latency."""

    calls: list[httpx.Request] = []
    clock = _Clock()
    asked_at = clock.now

    def _handler(_request: httpx.Request) -> httpx.Response:
        clock.advance(5)  # Slack took five seconds to answer.
        return httpx.Response(200, json={"ok": True, "users": ["U0APPROV1"]})

    client = _client(_handler, calls=calls, clock=clock, ttl_s=60.0)

    membership = asyncio.run(client.members(_GROUP))

    assert membership.fetched_at == asked_at + timedelta(seconds=5)

    # And the TTL window runs from that answer: 59s later the entry is still
    # live, where a stamp of asked_at would have aged it to 64s and refetched.
    clock.advance(59)
    cached = asyncio.run(client.members(_GROUP))
    assert len(calls) == 1
    assert cached.cache_age_s == 59.0


def test_second_lookup_within_the_ttl_is_served_from_cache() -> None:
    """A busy approval channel must not fan out one Slack call per click:
    usergroups.users.list is Tier 2 (~20/min). Within the TTL the cached member
    set is reused, and it reports the age of the fetch that proved it rather
    than pretending to be fresh."""

    calls: list[httpx.Request] = []
    clock = _Clock()
    client = _client(_ok(["U0APPROV1"]), calls=calls, clock=clock, ttl_s=60.0)

    first = asyncio.run(client.members(_GROUP))
    clock.advance(30)
    second = asyncio.run(client.members(_GROUP))

    assert len(calls) == 1
    assert second.users == first.users
    assert second.fetched_at == first.fetched_at
    assert second.cache_age_s == 30.0


def test_lookup_after_the_ttl_expires_refetches() -> None:
    """Past the TTL the client goes back to Slack, so a membership change
    propagates. The refetched set is what the caller gets -- the point of
    expiring is that the ANSWER changes, not merely that a call is made."""

    calls: list[httpx.Request] = []
    clock = _Clock()
    responses = [_ok(["U0APPROV1", "U0LEAVER1"]), _ok(["U0APPROV1"])]

    def _handler(request: httpx.Request) -> httpx.Response:
        return responses[min(len(calls) - 1, len(responses) - 1)](request)

    client = _client(_handler, calls=calls, clock=clock, ttl_s=60.0)

    before = asyncio.run(client.members(_GROUP))
    assert before.users == frozenset({"U0APPROV1", "U0LEAVER1"})

    clock.advance(61)
    after = asyncio.run(client.members(_GROUP))

    assert len(calls) == 2
    assert after.users == frozenset({"U0APPROV1"})
    assert after.fetched_at == clock.now
    assert after.cache_age_s == 0.0


def test_zero_ttl_forces_a_lookup_on_every_call() -> None:
    """``slack_usergroup_cache_ttl_s=0`` is the documented operator lever for
    per-resolve fetch (trading rate-limit headroom for zero revocation lag). It
    must actually bypass the cache, even with a frozen clock."""

    calls: list[httpx.Request] = []
    client = _client(_ok(["U0APPROV1"]), calls=calls, clock=_Clock(), ttl_s=0.0)

    asyncio.run(client.members(_GROUP))
    asyncio.run(client.members(_GROUP))

    assert len(calls) == 2


def test_distinct_groups_do_not_share_a_cache_entry() -> None:
    """The cache is keyed by group ID. A single shared key would hand one route's
    approver set to another route -- an authorization bug, not a caching one."""

    calls: list[httpx.Request] = []
    by_group = {_GROUP: ["U0APPROV1"], _OTHER_GROUP: ["U0LEGAL01"]}

    def _handler(request: httpx.Request) -> httpx.Response:
        group = request.url.params["usergroup"]
        return httpx.Response(200, json={"ok": True, "users": by_group[group]})

    client = _client(_handler, calls=calls, ttl_s=60.0)

    managers = asyncio.run(client.members(_GROUP))
    legal = asyncio.run(client.members(_OTHER_GROUP))

    assert managers.users == frozenset({"U0APPROV1"})
    assert legal.users == frozenset({"U0LEGAL01"})
    assert len(calls) == 2


# --- concurrency: the headroom the cache exists for is a concurrent property --
#
# The TTL cache above only helps callers that arrive AFTER a fetch has landed.
# The burst that actually threatens the Tier 2 limit (~20/min) is a click storm
# on one group, where every caller misses at once. Those tests are sequential
# and cannot see it, so these drive real concurrent callers.


def test_concurrent_lookups_of_one_group_cost_a_single_slack_call() -> None:
    """A click storm on one group must not fan out one request per click. Each
    caller misses the cache (nothing has landed yet), so without single-flight
    all of them fetch, and 25 concurrent resolutions burn 25 of the ~20/min
    budget -- tripping the very limit the cache exists to stay under, which
    under the fail-closed rule denies legitimate approvers."""

    calls: list[httpx.Request] = []

    async def _handler(_request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)  # Slack is in flight while the storm arrives.
        return httpx.Response(200, json={"ok": True, "users": ["U0APPROV1"]})

    client = _async_client(_handler, calls=calls, ttl_s=60.0)

    async def _storm() -> list[UserGroupMembership]:
        return await asyncio.gather(*(client.members(_GROUP) for _ in range(25)))

    memberships = asyncio.run(_storm())

    assert len(calls) == 1
    # Sharing the fetch must mean sharing its ANSWER: nobody gets an empty set.
    assert len(memberships) == 25
    assert all(m.users == frozenset({"U0APPROV1"}) for m in memberships)


def test_concurrent_lookups_of_distinct_groups_do_not_serialize() -> None:
    """Single-flight must be per group. One global lock would also produce one
    call per group, so counting calls cannot tell the two apart: instead both
    handlers rendezvous at a barrier and neither answers until both are in
    flight, which a shared lock makes impossible."""

    calls: list[httpx.Request] = []
    by_group = {_GROUP: ["U0APPROV1"], _OTHER_GROUP: ["U0LEGAL01"]}

    async def _main() -> list[UserGroupMembership]:
        both_in_flight = asyncio.Barrier(2)

        async def _handler(request: httpx.Request) -> httpx.Response:
            # Times out (rather than hanging the suite) if the groups serialize.
            await asyncio.wait_for(both_in_flight.wait(), 5.0)
            group = request.url.params["usergroup"]
            return httpx.Response(200, json={"ok": True, "users": by_group[group]})

        client = _async_client(_handler, calls=calls, ttl_s=60.0)
        return await asyncio.gather(client.members(_GROUP), client.members(_OTHER_GROUP))

    managers, legal = asyncio.run(_main())

    assert managers.users == frozenset({"U0APPROV1"})
    assert legal.users == frozenset({"U0LEGAL01"})
    assert len(calls) == 2


def test_a_failed_lookup_fails_every_concurrent_caller_and_is_not_cached() -> None:
    """Fail closed applies to sharers too. A caller that waited on a fetch which
    then failed must get the error, never an empty or stale member set -- an
    empty set would read as "you are not an approver" and silently deny."""

    calls: list[httpx.Request] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.01)
        return _server_error(request)

    client = _async_client(_handler, calls=calls, ttl_s=3600.0)

    async def _main() -> list[BaseException | UserGroupMembership]:
        results = await asyncio.gather(
            *(client.members(_GROUP) for _ in range(5)), return_exceptions=True
        )
        # Not cached, under a TTL that would have cached a success: the retry
        # reaches the transport again rather than replaying the failure.
        before = len(calls)
        with pytest.raises(UserGroupLookupError):
            await client.members(_GROUP)
        assert len(calls) > before
        return results

    results = asyncio.run(_main())

    assert len(results) == 5
    assert all(isinstance(r, UserGroupLookupError) for r in results)


def test_concurrent_failing_lookups_of_one_group_cost_a_single_slack_call() -> None:
    """Single-flight must coalesce the FETCH, not merely its cached result.

    Failures are not cached, so a design that shares only the cache entry
    coalesces nothing when Slack is down: each waiter finds the cache still
    empty and issues its own request, one after another. That is worse than no
    coalescing, which would at least fail everyone in parallel. With the real
    10s HTTP timeout, ten concurrent clicks would burn ten calls and leave the
    last one waiting ~100s, and /approvals/{id}/resolve awaits this inline.

    So: one call, and one delay's worth of wall clock rather than five.
    """

    calls: list[httpx.Request] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)  # Stands in for the HTTP timeout.
        return _server_error(request)

    client = _async_client(_handler, calls=calls, ttl_s=60.0)

    async def _storm() -> tuple[list[Any], float]:
        started = asyncio.get_running_loop().time()
        results = await asyncio.gather(
            *(client.members(_GROUP) for _ in range(5)), return_exceptions=True
        )
        return results, asyncio.get_running_loop().time() - started

    results, elapsed = asyncio.run(_storm())

    assert len(calls) == 1
    assert all(isinstance(r, UserGroupLookupError) for r in results)
    # Serialized failures would cost 5 * 0.05 = 0.25s; one shared fetch costs
    # one 0.05s delay. The bound sits between the two, well clear of both.
    assert elapsed < 0.15


def test_a_failure_shared_by_concurrent_callers_is_not_cached() -> None:
    """Sharing the in-flight fetch must not become caching the failure. The
    entry is dropped the moment the fetch settles, so the next click after
    Slack recovers resolves rather than replaying the storm's error."""

    calls: list[httpx.Request] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.01)
        if len(calls) == 1:  # The storm's one shared fetch; Slack recovers after.
            return _server_error(request)
        return httpx.Response(200, json={"ok": True, "users": ["U0APPROV1"]})

    client = _async_client(_handler, calls=calls, ttl_s=3600.0)

    async def _main() -> UserGroupMembership:
        results = await asyncio.gather(
            *(client.members(_GROUP) for _ in range(5)), return_exceptions=True
        )
        assert all(isinstance(r, UserGroupLookupError) for r in results)
        assert len(calls) == 1
        return await client.members(_GROUP)

    recovered = asyncio.run(_main())

    assert recovered.users == frozenset({"U0APPROV1"})
    assert len(calls) == 2


def test_concurrent_failing_lookups_of_distinct_groups_do_not_serialize() -> None:
    """The per-group property has to survive on the failure path too: one
    group's Slack outage must not park an unrelated group's approvals behind it
    for a timeout. As with the success case, counting calls cannot tell a global
    in-flight entry from a per-group one, so both handlers rendezvous at a
    barrier and neither fails until both are in flight."""

    calls: list[httpx.Request] = []

    async def _main() -> list[Any]:
        both_in_flight = asyncio.Barrier(2)

        async def _handler(request: httpx.Request) -> httpx.Response:
            # Times out (rather than hanging the suite) if the groups serialize.
            await asyncio.wait_for(both_in_flight.wait(), 5.0)
            return _server_error(request)

        client = _async_client(_handler, calls=calls, ttl_s=60.0)
        return await asyncio.gather(
            client.members(_GROUP),
            client.members(_OTHER_GROUP),
            return_exceptions=True,
        )

    results = asyncio.run(_main())

    assert all(isinstance(r, UserGroupLookupError) for r in results)
    assert len(calls) == 2


def test_a_cancelled_caller_does_not_kill_the_fetch_its_peers_share() -> None:
    """The shared fetch outlives any one caller. A click whose HTTP request is
    cancelled mid-await (the client hung up) must not cancel the fetch the other
    waiters are riding, which would fail them closed for someone else's dropped
    connection."""

    calls: list[httpx.Request] = []

    async def _handler(_request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)
        return httpx.Response(200, json={"ok": True, "users": ["U0APPROV1"]})

    client = _async_client(_handler, calls=calls, ttl_s=60.0)

    async def _main() -> UserGroupMembership:
        quitter = asyncio.create_task(client.members(_GROUP))
        stayer = asyncio.create_task(client.members(_GROUP))
        await asyncio.sleep(0)  # Both are now awaiting the one shared fetch.
        quitter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await quitter
        return await stayer

    membership = asyncio.run(_main())

    assert membership.users == frozenset({"U0APPROV1"})
    assert len(calls) == 1


# --- failures: every mode is a lookup failure, and none of them are cached ----
#
# Caching a failure would extend one Slack blip into a TTL-long outage of every
# group-bound approval, and (worse) would make the fail-closed denial sticky
# long after Slack recovered. Each test below proves the retry actually reaches
# the transport again, under a TTL that WOULD have cached a success.


def _slack_error(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"ok": False, "error": "no_such_subteam"})


def _rate_limited(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(429, headers={"retry-after": "30"}, json={"ok": False})


def _server_error(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(500, text="internal error")


def _connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("name resolution failed", request=request)


def _malformed_body(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"ok": True})


@pytest.mark.parametrize(
    "handler",
    [_slack_error, _rate_limited, _server_error, _connect_error, _malformed_body],
    ids=["ok_false", "http_429", "http_500", "connect_error", "malformed_body"],
)
def test_failed_lookups_raise_and_are_not_cached(
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    calls: list[httpx.Request] = []
    client = _client(handler, calls=calls, ttl_s=3600.0)

    with pytest.raises(UserGroupLookupError):
        asyncio.run(client.members(_GROUP))
    assert len(calls) == 1

    # Not cached: the retry hits Slack again rather than replaying the failure.
    with pytest.raises(UserGroupLookupError):
        asyncio.run(client.members(_GROUP))
    assert len(calls) == 2


def test_a_failed_lookup_does_not_poison_a_later_success() -> None:
    """The consequence that matters operationally: once Slack recovers, the very
    next click resolves. A cached failure would keep denying for the whole TTL."""

    calls: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        if len(calls) == 1:
            return _server_error(request)
        return httpx.Response(200, json={"ok": True, "users": ["U0APPROV1"]})

    client = _client(_handler, calls=calls, ttl_s=3600.0)

    with pytest.raises(UserGroupLookupError):
        asyncio.run(client.members(_GROUP))

    membership = asyncio.run(client.members(_GROUP))
    assert membership.users == frozenset({"U0APPROV1"})
    assert len(calls) == 2


def test_a_cached_success_is_not_disturbed_by_a_later_failure() -> None:
    """The inverse: within the TTL a cached success keeps serving, so a blip
    mid-window does not flip an established approver set to fail-closed."""

    calls: list[httpx.Request] = []
    clock = _Clock()

    def _handler(request: httpx.Request) -> httpx.Response:
        if len(calls) == 1:
            return httpx.Response(200, json={"ok": True, "users": ["U0APPROV1"]})
        return _server_error(request)

    client = _client(_handler, calls=calls, clock=clock, ttl_s=60.0)

    asyncio.run(client.members(_GROUP))
    clock.advance(10)
    cached: Any = asyncio.run(client.members(_GROUP))

    assert cached.users == frozenset({"U0APPROV1"})
    assert len(calls) == 1
