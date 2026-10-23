"""Two members share one server, end to end (ADR 0239, AC6).

"Consequences" 5: qualification proves two agents share one server process,
forged identity and cross-account calls refuse, and disable and member
removal preserve the specified boundary. Here that is one application, one
proxy, one fixture MCP server and one resolver, with two members holding
distinct static credentials and accounts, driven from the nearest boundary:
HTTP requests to the proxy carrying each member's ``cct2`` token. The fake
provider is the independent observer of whose credential each call used.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from _fake_provider import FakeProvider
from _resolver_harness import Fleet
from _shared_connector_support import random_installation
from _shared_proxy_harness import ProxyReply, Worlds, agent_name


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


def test_e2e_two_members_share_one_server_with_their_own_credentials(worlds: Worlds) -> None:
    world = worlds()
    first, second = world.member(), world.member()
    assert first.account != second.account and first.text != second.text
    before = world.observe()
    expected: list[tuple[str, str]] = []
    for member in (first, second, first, second):
        whoami = _admitted(world.call(member, "whoami"))
        assert (whoami["binding"], whoami["account"]) == (member.binding_id, member.account)
        assert whoami["authorization_sha256"] == world.digest(member)
        item = _admitted(world.call(member, "get_item", {"id": "42"}))
        assert (item["binding"], item["account"]) == (member.binding_id, member.account)
        assert item["provider_path"] == "/items/42"
        assert item["authorization_sha256"] == world.digest(member)
        expected += [("/v1/whoami", world.digest(member)), ("/items/42", world.digest(member))]
    # The provider's own record, in order: each call carried its caller's credential.
    observed = [
        (request.path, request.authorization_sha256)
        for request in world.provider.requests[before.provider :]
    ]
    assert observed == expected
    # One server process saw every call, each with its own fresh handle.
    calls = [r for r in world.fixture.since(before.upstream) if r.rpc == ("tools/call",)]
    assert len(calls) == 8
    assert len({request.handle for request in calls}) == 8
    assert world.registry.active_invocations == 0


def test_e2e_every_refusal_class_in_one_sequence_never_reaches_the_server(
    worlds: Worlds,
) -> None:
    world = worlds(run_tools=("run_task",), gated_tools=("create_item",))
    first, second = world.member(), world.member()
    changing = world.member()
    outsider = world.outsider()
    unbound = world.member_without_binding()
    first_session = world.initialize(first)
    changing_session = world.initialize(changing)
    world.set_aliases(changing, [{"alias": "primary", "tools": ["whoami"]}])
    hold, future = world.start_hold(first, "sequence", request_id=7)

    sequence: list[tuple[str, Callable[[], ProxyReply]]] = [
        ("missing", lambda: world.call(first, "whoami", tokens=[])),
        ("invalid", lambda: world.call(first, "whoami", tokens=["cct2.not.valid"])),
        (
            "invalid",
            lambda: world.call(first, "whoami", tokens=[world.token(first, installation="")]),
        ),
        (
            "expired",
            lambda: world.call(
                first, "whoami", tokens=[world.token(first, exp=int(time.time()) - 1)]
            ),
        ),
        (
            "not_admitted",
            lambda: world.call(
                first, "whoami", tokens=[world.token(first, installation=random_installation())]
            ),
        ),
        ("alias_required", lambda: world.call(first, "whoami", aliases=[])),
        (
            "not_member",
            lambda: world.call(
                None,
                "whoami",
                tokens=[world.token(agent_id=outsider, agent=agent_name(first.agent_id))],
                alias="primary",
            ),
        ),
        ("not_member", lambda: world.call(first, "whoami", alias="secondary")),
        (
            "binding_missing",
            lambda: world.call(
                None, "whoami", tokens=[world.token(agent_id=unbound)], alias="primary"
            ),
        ),
        ("tool_not_allowed", lambda: world.call(first, "delete_everything")),
        (
            "multiple_tool_calls",
            lambda: world.post(
                first, [world.call_message("whoami"), world.call_message("get_item", {"id": "1"})]
            ),
        ),
        ("run_required", lambda: world.call(first, "run_task")),
        ("grant_required", lambda: world.call(first, "create_item", {"title": "t"})),
        (
            "grant_required",
            lambda: world.call(
                first,
                "create_item",
                {"title": "t"},
                grants=[world.dedicated_grant(first, "create_item", {"title": "t"})],
            ),
        ),
        (
            "session_unknown",
            lambda: world.call(first, "whoami", session=f"sess-{uuid.uuid4().hex}"),
        ),
        (
            "session_principal_mismatch",
            lambda: world.call(second, "whoami", session=first_session),
        ),
        (
            "policy_generation_mismatch",
            lambda: world.call(changing, "whoami", session=changing_session),
        ),
        ("request_id_in_use", lambda: world.call(first, "whoami", request_id=7)),
        (
            "body_too_large",
            lambda: world.call(first, "whoami", {"padding": "x" * (1024 * 1024 + 16)}),
        ),
    ]
    start = world.observe()
    for code, send in sequence:
        before = world.observe()
        try:
            world.assert_refused(send(), code, before)
        except AssertionError as error:
            raise AssertionError(f"{code}: {error}") from None
    # Across the whole sequence: nothing reached the server, resolver or provider.
    assert world.observe() == start
    reply = world.finish_hold(hold, future)
    assert _admitted(reply)["second"]["binding"] == first.binding_id
    # Liveness at the end: both members are served.
    for member in (first, second):
        assert _admitted(world.call(member, "whoami"))["binding"] == member.binding_id


def test_e2e_disabling_one_binding_mid_sequence_leaves_the_other_member_working(
    worlds: Worlds,
) -> None:
    world = worlds()
    first, second = world.member(), world.member()
    for member in (first, second):
        assert _admitted(world.call(member, "whoami"))["binding"] == member.binding_id
    world.disable_binding(second)
    for _ in range(2):
        before = world.observe()
        world.assert_refused(world.call(second, "whoami"), "binding_disabled", before)
        result = _admitted(world.call(first, "get_item", {"id": "42"}))
        assert result["binding"] == first.binding_id
        assert result["authorization_sha256"] == world.digest(first)
    assert world.digest(second) not in [
        request.authorization_sha256 for request in world.provider.calls("/items/42")
    ]


def test_e2e_removing_one_member_preserves_the_others_held_call_and_session(
    worlds: Worlds,
) -> None:
    world = worlds()
    first, second = world.member(), world.member()
    session = world.initialize(first)
    _admitted(world.call(second, "whoami"))
    generation = world.generation()
    hold, future = world.start_hold(first, "survives", session=session)
    assert hold.first is not None and hold.first["binding"] == first.binding_id
    handle = hold.handle
    world.remove(second)
    assert world.generation() > generation
    before = world.observe()
    world.assert_refused(world.call(second, "whoami"), "not_member", before)
    reply = world.finish_hold(hold, future)
    outcome = _admitted(reply)
    # The held call resolved again with its same handle, as the same member.
    assert outcome["second"]["binding"] == first.binding_id
    assert outcome["second"]["authorization_sha256"] == world.digest(first)
    seconds = world.provider.calls("/v1/hold/second")
    assert [request.authorization_sha256 for request in seconds] == [world.digest(first)]
    held = [request for request in world.fixture.requests if request.handle == handle]
    assert len(held) == 1
    # The open session serves another tools/call.
    after = _admitted(world.call(first, "whoami", session=session))
    assert after["binding"] == first.binding_id
    assert after["authorization_sha256"] == world.digest(first)
