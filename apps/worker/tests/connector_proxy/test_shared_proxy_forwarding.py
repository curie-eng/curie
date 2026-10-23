"""What the shared proxy forwards, and the invocation handle (ADR 0239, AC2).

"Bundle, identity and request contract" 3 and 6: the proxy removes the raw
caller token, approval tokens, spoofed identity headers and incoming resolver
handles, then forwards a fresh 256-bit random invocation handle on each
``tools/call``. The handle binds installation, application, agent, alias,
policy generation and the token's run, work item and expiry; it lives for its
invocation, is removed on completion or cancellation, and a session ID is
never one. Transport SSE does not itself create a provider-call authority.

Everything is observed where it lands: the headers the fixture MCP server
received, the credential digest the fake provider saw, what a resolve over the
real socket answers, and (only where the plan names it) the registry's
context for a live handle captured while ``hold`` is parked.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _fake_provider import FakeProvider
from _fixture_mcp_server import UpstreamRequest
from _resolver_harness import Fleet
from _shared_proxy_harness import (
    HANDLE_SHAPE,
    INVOCATION_HEADER,
    ProxyReply,
    SharedProxy,
    Worlds,
    agent_name,
)
from curie_connector_proxy.resolver.handles import Principal


@pytest.fixture
def worlds(fleet: Fleet, provider: FakeProvider) -> Iterator[Worlds]:
    built = Worlds(fleet, provider)
    try:
        yield built
    finally:
        built.close()


def _only(world: SharedProxy, since: int) -> UpstreamRequest:
    (seen,) = world.fixture.since(since)
    return seen


def _admitted(reply: ProxyReply) -> dict[str, Any]:
    result = reply.result
    assert "refused" not in result and "error" not in result, result
    return result


def _handle_unknown(world: SharedProxy, handle: str) -> None:
    world.resolver.resolve(handle).refused(401, "handle_unknown")


_STRIPPED = (
    "authorization",
    "proxy-authorization",
    "cookie",
    "x-curie-caller",
    "x-curie-connector-grant",
    "x-curie-connector-alias",
    "x-curie-agent",
    "x-curie-agent-id",
    "x-curie-run",
    "x-curie-work-item",
    "x-curie-installation",
    "x-curie-made-up-identity",
)


def test_forward_strips_every_caller_identity_and_credential_header(worlds: Worlds) -> None:
    world = worlds()
    member, other = world.member(), world.member()
    spoofed_handle = "crh1." + "A" * 43
    token = world.token(member)
    stray_grant, _ = world.grant(member, "echo_headers", {})
    extra = [
        (INVOCATION_HEADER, spoofed_handle),
        ("X-Curie-Agent", agent_name(other.agent_id)),
        ("X-Curie-Agent-Id", other.agent_id),
        ("X-Curie-Run", str(uuid.uuid4())),
        ("X-Curie-Work-Item", str(uuid.uuid4())),
        ("X-Curie-Installation", "another-installation"),
        ("X-Curie-Made-Up-Identity", other.binding_id),
        ("Authorization", "Bearer caller-supplied-credential"),
        ("Proxy-Authorization", "Basic Y2FsbGVyOnN1cHBsaWVk"),
        ("Cookie", "session=caller-cookie"),
        ("X-Trace-Id", "trace-forwarded-1"),
    ]
    for tool in ("echo_headers", "whoami"):
        before = world.observe()
        reply = world.call(member, tool, tokens=[token], grants=[stray_grant], extra=extra)
        result = _admitted(reply)
        seen = _only(world, before.upstream)
        names = seen.names
        for name in _STRIPPED:
            assert name not in names, (tool, name)
        assert seen.curie_headers == {"x-curie-invocation"}, (tool, seen.names)
        handle = seen.handle
        assert handle is not None and HANDLE_SHAPE.fullmatch(handle), tool
        assert handle != spoofed_handle
        # Ordinary headers are forwarded as sent.
        assert seen.values("X-Trace-Id") == ["trace-forwarded-1"]
        assert seen.values("Content-Type") == ["application/json"]
        assert seen.values("Accept") == ["application/json, text/event-stream"]
        assert seen.values("MCP-Protocol-Version") != []
        # Neither the caller token nor the grant reached the server anywhere.
        for _, value in seen.headers:
            assert token not in value and stray_grant not in value
        assert token.encode() not in seen.body and stray_grant.encode() not in seen.body
        if tool == "echo_headers":
            assert [tuple(pair) for pair in result["headers"]] == list(seen.headers)
        else:
            # The caller's own Authorization never reached the provider either.
            assert result["binding"] == member.binding_id
            assert result["authorization_sha256"] == world.digest(member)
    _handle_unknown(world, spoofed_handle)


def test_forward_only_a_tools_call_carries_a_handle(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    start = world.observe()
    session = world.initialize(member)
    listing = world.post(member, world.message("tools/list"), session=session)
    assert listing.status == 200, listing.text
    stream = world.open_stream(member, session)
    try:
        assert stream.status == 200
        assert stream.read_line().startswith(b": open")
    finally:
        stream.close()
    deleted_session = world.initialize(member)
    deleted = world.delete(member, deleted_session)
    assert deleted.status == 200, deleted.text
    without_handles = world.fixture.since(start.upstream)
    assert [request.rpc or (request.method,) for request in without_handles] == [
        ("initialize",),
        ("notifications/initialized",),
        ("tools/list",),
        ("GET",),
        ("initialize",),
        ("notifications/initialized",),
        ("DELETE",),
    ]
    for request in without_handles:
        assert request.handles == [], request.rpc or request.method
    assert world.resolver.admissions == start.admissions

    before = world.observe()
    _admitted(world.call(member, "whoami", session=session))
    seen = _only(world, before.upstream)
    assert len(seen.handles) == 1 and HANDLE_SHAPE.fullmatch(seen.handles[0])


def test_forward_every_call_gets_a_fresh_handle_that_dies_with_it(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    baseline = world.registry.active_invocations
    handles: list[str] = []
    for _ in range(3):
        before = world.observe()
        _admitted(world.call(member, "whoami"))
        handle = _only(world, before.upstream).handle
        assert handle is not None and HANDLE_SHAPE.fullmatch(handle)
        handles.append(handle)
        assert world.registry.active_invocations == baseline
        _handle_unknown(world, handle)
    assert len(set(handles)) == 3


def test_forward_an_inbound_handle_is_never_forwarded_or_honoured(worlds: Worlds) -> None:
    world = worlds()
    holder, other = world.member(), world.member()
    hold, future = world.start_hold(holder, "inbound")
    live = hold.handle
    assert live is not None and HANDLE_SHAPE.fullmatch(live)
    # Another member presenting the holder's live handle gets its own fresh one.
    before = world.observe()
    reply = world.call(other, "whoami", extra=[(INVOCATION_HEADER, live)])
    result = _admitted(reply)
    seen = _only(world, before.upstream)
    assert seen.handles != [live] and len(seen.handles) == 1
    assert result["binding"] == other.binding_id
    assert result["authorization_sha256"] == world.digest(other)
    # The holder's handle still names the holder.
    finished = world.finish_hold(hold, future)
    outcome = _admitted(finished)
    assert outcome["second"]["binding"] == holder.binding_id
    # A random value of handle shape resolves nothing.
    _handle_unknown(world, "crh1." + "B" * 43)


def test_forward_the_handle_context_is_the_verified_token(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    session = world.initialize(member)
    run, work_item = str(uuid.uuid4()), str(uuid.uuid4())
    exp = int(time.time()) + 1200
    token = world.token(member, exp=exp, run=run, work_item=work_item)
    hold, future = world.start_hold(
        member,
        "context",
        session=session,
        tokens=[token],
        extra=[("X-Curie-Run", str(uuid.uuid4())), ("X-Curie-Work-Item", str(uuid.uuid4()))],
    )
    assert hold.handle is not None
    context = world.context(hold.handle)
    assert context.principal == Principal(
        installation=world.app.installation,
        application=world.app.application_id,
        agent_id=member.agent_id,
        agent=agent_name(member.agent_id),
        policy_generation=world.generation(),
    )
    assert context.alias == member.alias
    assert context.expires_at == exp
    assert (context.run, context.work_item) == (run, work_item)
    assert context.session_id == session
    assert context.binding == member.binding_id
    assert context.membership == world.stamp(member)
    assert hold.first is not None and hold.first["expires_at"] == exp
    reply = world.finish_hold(hold, future)
    outcome = _admitted(reply)
    assert outcome["second"]["binding"] == member.binding_id
    # After completion the handle is gone.
    _handle_unknown(world, hold.handle)
    assert world.registry.active_invocations == 0


def test_forward_a_client_hang_up_revokes_the_handle(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    hold = world.fixture.hold("hangup")
    message = world.call_message("hold", {"key": "hangup"})
    sock = world.raw_post(json.dumps(message).encode(), world.headers(member))
    try:
        hold.wait_entered()
        assert hold.handle is not None
        assert world.registry.active_invocations == 1
    finally:
        sock.close()
    world.wait_for_invocations(0)
    _handle_unknown(world, hold.handle)
    hold.release()
    outcome = hold.wait_done()
    assert outcome["second"] == {"refused": "handle_unknown", "status": 401}
    assert world.provider.calls("/v1/hold/second") == []
    assert len(world.provider.calls("/v1/hold/first")) == 1


def test_forward_arguments_cannot_choose_another_account(worlds: Worlds) -> None:
    world = worlds()
    member, other = world.member(), world.member()
    before = world.observe()
    reply = world.call(
        member,
        "whoami",
        {
            "agent_id": other.agent_id,
            "binding": other.binding_id,
            "account": other.account,
            "slot": "another_slot",
            "alias": "secondary",
        },
    )
    result = _admitted(reply)
    assert result["binding"] == member.binding_id
    assert result["account"] == member.account
    assert result["authorization_sha256"] == world.digest(member)
    seen_by_provider = world.provider.requests[before.provider :]
    assert [request.authorization_sha256 for request in seen_by_provider] == [
        world.digest(member)
    ]


def test_forward_a_session_id_is_never_a_handle(worlds: Worlds) -> None:
    world = worlds()
    member = world.member()
    session = world.initialize(member)
    _handle_unknown(world, session)
    before = world.observe()
    reply = world.call(member, "whoami", session=session, extra=[(INVOCATION_HEADER, session)])
    assert _admitted(reply)["binding"] == member.binding_id
    seen = _only(world, before.upstream)
    assert seen.session_id == session
    assert seen.handles != [session] and len(seen.handles) == 1


def test_forward_an_unreachable_server_answers_502_and_drops_the_handle(
    worlds: Worlds,
) -> None:
    world = worlds()
    member = world.member()
    _admitted(world.call(member, "whoami"))
    world.fixture.stop()
    before = world.observe()
    reply = world.call(member, "whoami")
    assert reply.status == 502, reply.text
    assert world.registry.active_invocations == 0
    assert world.observe() == before
