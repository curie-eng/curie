"""Sessions, cancellation and bounds in shared mode (ADR 0239, AC4).

"Bundle, identity and request contract" 6: sessions, resumptions, cancellation
and notifications bind principal, application and policy generation; a session
ID is never identity. Initial bounds are 1024 active invocations and 4096
sessions per application, and overflow refuses without allocation. Transport
SSE does not itself create provider-call authority, and no new short tool
timeout is imposed.

The plan's reconciliation of AC4 with AC6 is tested here as written: a session
survives a policy generation change only while the principal's own alias row
is unchanged (its membership stamp), so another member's change never breaks
it, and the principal's own change always does. Revisions settle the rest:
ownership is proved before any refusal-driven cleanup (R1-3), an authenticated
``notifications/cancelled`` revokes only the matching invocation of the same
agent, alias and session (R1-4), idle pruning never touches a session with work
in flight (R1-5), and a request id already reserved by a live exchange refuses
``request_id_in_use`` (R2-1).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _fake_provider import FakeProvider
from _fixture_mcp_server import PROTOCOL_VERSION
from _resolver_harness import Fleet, Member
from _shared_proxy_harness import (
    PROXY_WAIT_S,
    ProxyReply,
    SharedProxy,
    Worlds,
    wait_until,
)
from curie_connector_proxy.resolver.handles import MAX_INVOCATIONS, MAX_SESSIONS

INITIALIZE_PARAMS = {
    "protocolVersion": PROTOCOL_VERSION,
    "capabilities": {},
    "clientInfo": {"name": "curie-test", "version": "1"},
}
BOTH_ALIASES = [{"alias": "primary", "tools": None}, {"alias": "secondary", "tools": None}]


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def _admitted(reply: ProxyReply) -> dict[str, Any]:
    result = reply.result
    assert "refused" not in result and "error" not in result, result
    return result


def _initialize(world: SharedProxy, member: Member) -> ProxyReply:
    """An initialize the proxy may refuse (``SharedProxy.initialize`` asserts success)."""

    return world.post(member, world.message("initialize", INITIALIZE_PARAMS))


# =============================================================================
# Ownership
# =============================================================================


def test_session_another_principal_presenting_it_is_refused(worlds: Worlds) -> None:
    world = worlds()
    owner = world.member(aliases=BOTH_ALIASES)
    other = world.member()
    session = world.initialize(owner)
    # Another member.
    before = world.observe()
    reply = world.call(other, "whoami", session=session)
    world.assert_refused(reply, "session_principal_mismatch", before)
    # The owner itself through another alias.
    before = world.observe()
    reply = world.call(owner, "whoami", session=session, alias="secondary")
    world.assert_refused(reply, "session_principal_mismatch", before)
    # The owner's session is untouched by both.
    assert _admitted(world.call(owner, "whoami", session=session))["binding"] == owner.binding_id


def test_session_an_unknown_id_is_refused(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    session = world.initialize(member)
    for unknown in (f"sess-{uuid.uuid4().hex}", session.upper() + "x"):
        before = world.observe()
        world.assert_refused(
            world.call(member, "whoami", session=unknown), "session_unknown", before
        )
    assert _admitted(world.call(member, "whoami", session=session))["binding"] == (
        member.binding_id
    )


def test_session_the_principals_own_membership_change_closes_it(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    session = world.initialize(member)
    _admitted(world.call(member, "whoami", session=session))
    open_sessions = world.registry.sessions
    world.set_aliases(member, BOTH_ALIASES)
    before = world.observe()
    reply = world.call(member, "whoami", session=session)
    world.assert_refused(reply, "policy_generation_mismatch", before)
    assert world.registry.sessions == open_sessions - 1
    before = world.observe()
    world.assert_refused(world.call(member, "whoami", session=session), "session_unknown", before)
    # Liveness: a new session under the new membership works.
    fresh = world.initialize(member)
    assert _admitted(world.call(member, "whoami", session=fresh))["binding"] == member.binding_id


def test_session_a_removed_member_is_refused_and_readding_does_not_revive_it(
    worlds: Worlds,
) -> None:
    world = worlds()
    member, survivor = world.member(), world.member()
    session = world.initialize(member)
    _admitted(world.call(member, "whoami", session=session))
    open_sessions = world.registry.sessions
    world.remove(member)
    before = world.observe()
    world.assert_refused(world.call(member, "whoami", session=session), "not_member", before)
    assert world.registry.sessions == open_sessions - 1
    world.set_aliases(member, [{"alias": "primary", "tools": None}])
    before = world.observe()
    world.assert_refused(world.call(member, "whoami", session=session), "session_unknown", before)
    fresh = world.initialize(member)
    assert _admitted(world.call(member, "whoami", session=fresh))["binding"] == member.binding_id
    assert _admitted(world.call(survivor, "whoami"))["binding"] == survivor.binding_id


def test_session_removal_and_readding_between_uses_still_refuses(worlds: Worlds) -> None:
    """Re-adding recreates the alias row, so the old session's stamp no longer matches."""

    world = worlds()
    member = world.member()
    session = world.initialize(member)
    stamp = world.stamp(member)
    world.remove(member)
    world.set_aliases(member, [{"alias": "primary", "tools": None}])
    assert world.stamp(member) != stamp
    before = world.observe()
    reply = world.call(member, "whoami", session=session)
    world.assert_refused(reply, "policy_generation_mismatch", before)
    before = world.observe()
    world.assert_refused(world.call(member, "whoami", session=session), "session_unknown", before)


def test_session_survives_a_generation_bump_by_another_member(worlds: Worlds) -> None:
    world = worlds()
    member, other = world.member(), world.member()
    session = world.initialize(member)
    generation = world.generation()
    stamp = world.stamp(member)
    # A context strictly bound to the old generation, for the secondary observation.
    strict = world.resolver.handle(member, generation=generation)
    world.remove(other)
    joiner = world.member(alias="joiner")
    assert world.generation() > generation
    assert world.stamp(member) == stamp
    reply = world.call(member, "whoami", session=session)
    assert _admitted(reply)["binding"] == member.binding_id
    assert reply.result["authorization_sha256"] == world.digest(member)
    # The generation did move: a strictly bound context is refused on it.
    world.resolver.resolve(strict).refused(403, "policy_generation_mismatch")
    assert _admitted(world.call(joiner, "whoami", alias="joiner"))["binding"] == joiner.binding_id


# =============================================================================
# Transport: SSE, notifications, DELETE
# =============================================================================


def test_session_an_sse_stream_gets_no_handle_and_no_resolve(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    session = world.initialize(member)
    before = world.observe()
    stream = world.open_stream(member, session)
    try:
        assert stream.status == 200
        content_type = {name.lower(): value for name, value in stream.headers}["content-type"]
        assert content_type.startswith("text/event-stream")
        assert stream.read_line().startswith(b": open")
        wait_until(lambda: world.fixture.streams_open == 1, "the stream to open")
        (seen,) = world.fixture.since(before.upstream)
        assert seen.method == "GET" and seen.session_id == session
        assert seen.handles == []
        assert world.resolver.admissions == before.admissions
        assert world.registry.active_invocations == 0
        # Liveness: a tools/call on the same session while it streams gets a handle.
        mark = world.fixture.count
        _admitted(world.call(member, "whoami", session=session))
        (called,) = world.fixture.since(mark)
        assert called.handle is not None
    finally:
        stream.close()
    # A stream on an unknown session is refused like any request.
    before = world.observe()
    refused = world.open_stream(member, f"sess-{uuid.uuid4().hex}")
    try:
        world.assert_refused(refused.reply(), "session_unknown", before)
    finally:
        refused.close()


def test_session_notifications_and_responses_are_forwarded_without_a_handle(
    worlds: Worlds,
) -> None:
    world = worlds()
    member = world.member()
    session = world.initialize(member)
    before = world.observe()
    cancelled = world.cancel(member, 987654, session=session)
    assert cancelled.status == 202, cancelled.text
    response = world.post(
        member, {"jsonrpc": "2.0", "id": 5, "result": {"roots": []}}, session=session
    )
    assert response.status == 202, response.text
    forwarded = world.fixture.since(before.upstream)
    assert [request.rpc for request in forwarded] == [("notifications/cancelled",), ()]
    for request in forwarded:
        assert request.handles == []
        assert request.session_id == session
    assert world.resolver.admissions == before.admissions


def test_session_delete_ends_it_for_its_owner_only(worlds: Worlds) -> None:
    world = worlds()
    owner, other = world.member(), world.member()
    session, kept = world.initialize(owner), world.initialize(owner)
    open_sessions = world.registry.sessions
    # Another member cannot end it.
    before = world.observe()
    world.assert_refused(world.delete(other, session), "session_principal_mismatch", before)
    assert world.registry.sessions == open_sessions
    # The owner can.
    before = world.observe()
    deleted = world.delete(owner, session)
    assert deleted.status == 200, deleted.text
    (seen,) = world.fixture.since(before.upstream)
    assert seen.method == "DELETE" and seen.session_id == session and seen.handles == []
    assert world.registry.sessions == open_sessions - 1
    before = world.observe()
    world.assert_refused(world.call(owner, "whoami", session=session), "session_unknown", before)
    assert _admitted(world.call(owner, "whoami", session=kept))["binding"] == owner.binding_id


# =============================================================================
# Bounds
# =============================================================================


def test_session_bound_refuses_a_session_past_the_maximum(worlds: Worlds) -> None:
    world = worlds(max_sessions=2)
    member = world.member()
    first = world.initialize(member)
    world.initialize(member)
    assert world.registry.sessions == 2
    before = world.observe()
    world.assert_refused(_initialize(world, member), "sessions_full", before)
    assert world.registry.sessions == 2
    # Liveness: ending one frees its slot.
    assert world.delete(member, first).status == 200
    third = world.initialize(member)
    assert _admitted(world.call(member, "whoami", session=third))["binding"] == member.binding_id
    assert world.registry.sessions == 2


def test_session_invocation_bound_refuses_a_second_call(worlds: Worlds) -> None:
    world = worlds(max_invocations=1)
    holder, other = world.member(), world.member()
    hold, future = world.start_hold(holder, "bound")
    assert world.registry.active_invocations == 1
    before = world.observe()
    world.assert_refused(world.call(other, "whoami"), "invocations_full", before)
    assert world.registry.active_invocations == 1
    assert _admitted(world.finish_hold(hold, future))["second"]["binding"] == holder.binding_id
    world.wait_for_invocations(0)
    assert _admitted(world.call(other, "whoami"))["binding"] == other.binding_id


def test_session_default_bounds_are_the_adrs(worlds: Worlds) -> None:
    world = worlds()
    assert (MAX_INVOCATIONS, MAX_SESSIONS) == (1024, 4096)
    assert world.registry.max_invocations == MAX_INVOCATIONS
    assert world.registry.max_sessions == MAX_SESSIONS
    bounded = worlds(max_invocations=3, max_sessions=5)
    assert (bounded.registry.max_invocations, bounded.registry.max_sessions) == (3, 5)


# =============================================================================
# Revisions: ownership first, cancellation, idle pruning, request ids
# =============================================================================


def test_session_a_removed_member_cannot_disturb_another_members_session(
    worlds: Worlds,
) -> None:
    """R1-3: a refusal before ownership is proved never closes or mutates a session."""

    world = worlds()
    owner, intruder = world.member(), world.member()
    session = world.initialize(owner)
    hold, future = world.start_hold(owner, "owner-busy", session=session)
    world.remove(intruder)
    open_sessions = world.registry.sessions
    before = world.observe()
    reply = world.call(intruder, "whoami", session=session)
    world.assert_refused(reply, "session_principal_mismatch", before)
    assert world.registry.sessions == open_sessions
    outcome = _admitted(world.finish_hold(hold, future))
    assert outcome["second"]["binding"] == owner.binding_id
    assert outcome["second"]["authorization_sha256"] == world.digest(owner)
    assert _admitted(world.call(owner, "whoami", session=session))["binding"] == owner.binding_id


def test_session_cancellation_revokes_the_matching_invocation(worlds: Worlds) -> None:
    """R1-4: the server ignores cancellation; the proxy revokes the handle anyway."""

    world = worlds()
    member = world.member()
    session = world.initialize(member)
    request_id = world.next_id()
    hold, future = world.start_hold(member, "cancelled", session=session, request_id=request_id)
    assert hold.handle is not None
    before = world.observe()
    reply = world.cancel(member, request_id, session=session)
    assert reply.status == 202, reply.text
    (seen,) = world.fixture.since(before.upstream)
    assert seen.rpc == ("notifications/cancelled",) and seen.handles == []
    world.resolver.resolve(hold.handle).refused(401, "handle_unknown")
    hold.release()
    future.result(PROXY_WAIT_S)
    outcome = hold.wait_done()
    assert outcome["second"] == {"refused": "handle_unknown", "status": 401}
    assert world.provider.calls("/v1/hold/second") == []


def test_session_cancellation_from_another_agent_alias_or_session_revokes_nothing(
    worlds: Worlds,
) -> None:
    world = worlds()
    member = world.member(aliases=BOTH_ALIASES)
    other = world.member()
    session = world.initialize(member)
    second_session = world.initialize(member)
    other_session = world.initialize(other)
    request_id = world.next_id()
    hold, future = world.start_hold(member, "kept", session=session, request_id=request_id)
    assert hold.handle is not None
    attempts = [
        world.cancel(other, request_id),
        world.cancel(other, request_id, session=other_session),
        world.cancel(member, request_id, session=second_session),
        world.cancel(member, request_id),
        world.cancel(member, request_id, alias="secondary"),
    ]
    assert [reply.status for reply in attempts] == [202] * len(attempts)
    # The handle is intact: it still resolves to the member's binding.
    resolved = world.resolver.resolve(hold.handle)
    assert resolved.status == 200, resolved.text
    assert resolved.json["binding"] == member.binding_id
    outcome = _admitted(world.finish_hold(hold, future))
    assert outcome["second"]["binding"] == member.binding_id


def test_session_idle_sessions_are_pruned_but_active_ones_survive(worlds: Worlds) -> None:
    """R1-5: with a controlled clock, only an unused session past the threshold goes."""

    world = worlds(max_sessions=3, session_idle_seconds=30.0)
    member = world.member()
    busy = world.initialize(member)
    streaming = world.initialize(member)
    idle = world.initialize(member)
    hold, future = world.start_hold(member, "busy", session=busy)
    stream = world.open_stream(member, streaming)
    try:
        assert stream.status == 200
        assert stream.read_line().startswith(b": open")
        world.clock.advance(120)
        # The idle session is reclaimed, freeing a slot under max_sessions.
        fourth = world.initialize(member)
        assert world.registry.sessions == 3
        before = world.observe()
        world.assert_refused(world.call(member, "whoami", session=idle), "session_unknown", before)
        # The busy and streaming sessions were not: a fifth session has no slot.
        before = world.observe()
        world.assert_refused(_initialize(world, member), "sessions_full", before)
        outcome = _admitted(world.finish_hold(hold, future))
        assert outcome["second"]["binding"] == member.binding_id
        for session in (streaming, busy, fourth):
            reply = world.call(member, "whoami", session=session)
            assert _admitted(reply)["binding"] == member.binding_id
    finally:
        stream.close()


def test_session_a_request_id_in_use_is_refused_until_its_exchange_ends(
    worlds: Worlds,
) -> None:
    """R2-1: two overlapping session-less calls with id 1 from one member and alias."""

    world = worlds()
    member, other = world.member(), world.member()
    hold, future = world.start_hold(member, "first", request_id=1)
    assert hold.handle is not None
    before = world.observe()
    reply = world.call(member, "hold", {"key": "second"}, request_id=1)
    world.assert_refused(reply, "request_id_in_use", before)
    assert not world.fixture.hold("second").entered.is_set()
    # The same id from another member is not in use.
    assert _admitted(world.call(other, "whoami", request_id=1))["binding"] == other.binding_id
    # Cancelling id 1 revokes the first call's handle.
    assert world.cancel(member, 1).status == 202
    world.resolver.resolve(hold.handle).refused(401, "handle_unknown")
    hold.release()
    future.result(PROXY_WAIT_S)
    assert hold.wait_done()["second"] == {"refused": "handle_unknown", "status": 401}
    world.wait_for_invocations(0)
    # Once the first exchange ended, id 1 is usable again.
    assert _admitted(world.call(member, "whoami", request_id=1))["binding"] == member.binding_id


def test_session_a_tools_call_without_an_id_is_handled_normally(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    before = world.observe()
    reply = world.call(member, "whoami", notification=True)
    assert reply.status == 202, reply.text
    (seen,) = world.fixture.since(before.upstream)
    assert seen.rpc == ("tools/call",)
    assert seen.handle is not None
    provider_seen = world.provider.requests[before.provider :]
    assert [(request.path, request.authorization_sha256) for request in provider_seen] == [
        ("/v1/whoami", world.digest(member))
    ]
    world.wait_for_invocations(0)
