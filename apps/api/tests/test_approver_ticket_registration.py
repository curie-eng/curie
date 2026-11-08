"""ADR 0271 decision 1: register a selector inside one operator ceiling.

@spec REQUEST-APPROVAL-1 (and ADR 0270 acceptance item 2, the selector/flag half).

A registration is written only through ``PATCH /agents/{id}`` as part of the
full ``approval_routes`` replacement, compare-and-set on the agent's existing
``connector_requester_identity_version`` (plan D1, D2). The server pins the
operator-approved restricted approver role (D4); agent-owned role edits never
rewrite that pin. Every refusal reads the stored row afterwards and is followed
by the neighboring valid write.

Selection for the acceptance receipts: ``-k 'flag or concurrent or race or
invalid_stored'`` selects exactly the flag/selector invariant, concurrency and
invalid-stored-state tests (AC2); every other test here is AC1 and avoids those
words in its name and parameter ids.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import json
import threading
import uuid
from collections.abc import Callable
from functools import partial
from types import SimpleNamespace
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from approver_ticket_support import (
    CEILING_FILTER,
    CONFIG_VERSION,
    CONNECTOR,
    KEY_ID,
    OTHER_ROUTE,
    OUTSIDER,
    ROUTE,
    ROUTES_EXPECTED,
    SEED_ROUTE,
    TOOL,
    agent_state,
    api_headers,
    approval_rows,
    carried,
    create,
    detail_text,
    dynamic_request,
    enroll,
    exactly,
    operator_app,
    patch_agent,
    pinned_role,
    prepare,
    public_key_text,
    readback,
    refused_with,
    register,
    registered,
    registration,
    route_map,
    selector_key,
    set_approver_role,
    set_flag,
    store_routes,
    stored_approver_role,
    subject_entry,
    ticket_events,
    write_routes,
)
from approver_ticket_support import operator_case as operator_case
from approver_ticket_support import synthetic_provider as synthetic_provider
from approver_ticket_support import ticket_case as ticket_case
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

REGISTERED = "approver_ticket.registered"
REPLACED = "approver_ticket.replaced"
REMOVED = "approver_ticket.removed"
SEPARATION = "approval_separation.changed"
# A losing writer names the version it read or the invariant the winner created.
LOSING_CODES = (
    "agent.requester_identity_version_conflict",
    "approval_routes.version_conflict",
    "approver_ticket_requires_requester_identity",
    "requester_identity_selector_registered",
)


def events_of(tenant: str, kind: str) -> list[dict[str, Any]]:
    return [event for event in ticket_events(tenant) if event["event_type"] == kind]


def stored_ticket(agent: str, route: str = ROUTE) -> dict[str, Any]:
    binding = agent_state(agent)["approval_routes"][route]
    assert "approver_ticket" in binding, binding
    return dict(binding["approver_ticket"])


# --- AC1: registration, pinning and audit ----------------------------------------


def test_registration_pins_the_approver_role_and_defaults_exclusion(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: pinned role, default exclusion, version bump, audit."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    before = scope["config_version"]
    role_version, role = stored_approver_role(agent)
    assert role_version == scope["role_version"] and role is not None

    response = register(client, scope, case.key)
    assert response.status_code == 200, response.text
    assert response.json()[CONFIG_VERSION] == before + 1

    views = (
        readback(client, agent)["approval_routes"][ROUTE],
        agent_state(agent)["approval_routes"][ROUTE],
    )
    for view in views:
        ticket = view["approver_ticket"]
        assert (ticket["connector"], ticket["tool"], ticket["key_id"], ticket["public_key"]) == (
            CONNECTOR, TOOL, KEY_ID, public_key_text(case.key),
        )
        assert ticket["ceiling"]["approver_role_version"] == role_version
        assert pinned_role(ticket["ceiling"]["approver_role"]) == role
        assert ticket["ceiling"].get("users") is None
        assert view["separation"] == {"exclude_requester": True}
    state = agent_state(agent)
    assert (state["flag"], state["version"]) == (True, before + 1)
    for other in (OTHER_ROUTE, SEED_ROUTE):
        assert not {"approver_ticket", "separation"} & set(state["approval_routes"][other])

    events = events_of(scope["tenant"], REGISTERED)
    assert len(events) == 1, ticket_events(scope["tenant"])
    assert events[0]["actor"] == "operator"
    details = json.dumps(events[0]["details"])
    for value in (ROUTE, KEY_ID, CONNECTOR, TOOL):
        assert value in details, details
    assert public_key_text(case.key) not in json.dumps(ticket_events(scope["tenant"]))


def test_selector_writes_need_the_current_configuration_version(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: registration, replacement and removal are versioned (D1-D3)."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    address = scope["address"]

    # Selector-free writes keep today's unversioned behavior and do not bump.
    edited = route_map(address)
    edited[SEED_ROUTE]["approvers"]["users"] = ["U0EXAMPLE1", "U0EXAMPLE2"]
    plain = write_routes(client, agent, edited)
    assert plain.status_code == 200, plain.text
    before = agent_state(agent)
    assert before["version"] == scope["config_version"]

    proposed = route_map(address, ticket=registration(case.key, scope["role_version"]))
    refused_with(write_routes(client, agent, proposed), 409,
                 "approval_routes.expected_version_required")
    for stale in (before["version"] - 1, before["version"] + 1):
        refused_with(write_routes(client, agent, proposed, expected=stale), 409,
                     "approval_routes.version_conflict")
    # Strict and solitary: the expected version travels only with the route map.
    for bad in (True, "2", 0, 2_147_483_647, 1.0):
        response = patch_agent(client, agent, {"approval_routes": proposed, ROUTES_EXPECTED: bad})
        assert response.status_code == 422, (bad, response.text)
    beside = patch_agent(client, agent, {
        "approval_routes": proposed, ROUTES_EXPECTED: before["version"], "model": "claude-sonnet-5",
    })
    assert beside.status_code == 422, beside.text
    assert agent_state(agent) == before
    assert readback(client, agent)["model"] != "claude-sonnet-5"

    written = write_routes(client, agent, proposed, expected=before["version"])
    assert written.status_code == 200, written.text
    current = agent_state(agent)
    assert current["version"] == before["version"] + 1

    # Once a selector is stored, every route write names the version, even one
    # that only edits an unrelated route or removes the selector.
    unrelated = copy.deepcopy(proposed)
    unrelated[SEED_ROUTE]["approvers"]["users"] = ["U0EXAMPLE1"]
    for routes in (unrelated, route_map(address)):
        refused_with(write_routes(client, agent, routes), 409,
                     "approval_routes.expected_version_required")
        refused_with(write_routes(client, agent, routes, expected=before["version"]), 409,
                     "approval_routes.version_conflict")
        assert agent_state(agent) == current
    removed = write_routes(client, agent, route_map(address), expected=current["version"])
    assert removed.status_code == 200, removed.text
    assert not registered(agent_state(agent))
    assert agent_state(agent)["version"] == current["version"] + 1


def test_an_unusable_ceiling_refuses_registration(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-1: absent, unrestricted, empty, unreadable or stale refuse."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, role_filter=None)
    before = agent_state(agent)

    def attempt(version: int) -> Any:
        return write_routes(
            client, agent, route_map(scope["address"], ticket=registration(case.key, version)),
            expected=scope["config_version"],
        )

    # The policy exists (the invocation role) but declares no approver role.
    assert stored_approver_role(agent) == (scope["role_version"], None)
    refused_with(attempt(scope["role_version"]), 422, "approver_ticket_ceiling_invalid")
    for unusable in (None, {"entries": []}):
        version = set_approver_role(scope, unusable)
        refused_with(attempt(version), 422, "approver_ticket_ceiling_invalid")
    version = set_approver_role(scope, CEILING_FILTER)
    for stale in (version - 1, version + 1):
        refused_with(attempt(stale), 409, "stale_role_policy_version")
    saved = sql_dicts(
        "SELECT role_configuration FROM curie.agent_access_policies WHERE agent_id = :a",
        {"a": uuid.UUID(agent)},
    )[0]["role_configuration"]
    sql_rows(
        "UPDATE curie.agent_access_policies SET role_configuration = "
        "jsonb_set(role_configuration, '{approver}', CAST(:bogus AS jsonb)) WHERE agent_id = :a",
        {"bogus": json.dumps({"schema_version": 1, "legacy_state": "unknown"}),
         "a": uuid.UUID(agent)},
    )
    refused_with(attempt(version), 422, "approver_ticket_ceiling_invalid")
    assert agent_state(agent) == before
    assert ticket_events(scope["tenant"]) == []
    sql_rows(
        "UPDATE curie.agent_access_policies SET role_configuration = CAST(:saved AS jsonb) "
        "WHERE agent_id = :a",
        {"saved": json.dumps(saved), "a": uuid.UUID(agent)},
    )
    accepted = attempt(version)
    assert accepted.status_code == 200, accepted.text


def test_explicit_exclusion_opt_out_is_stored_and_audited(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: only an explicit operator false opts out, audited."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    response = register(client, scope, case.key, separation={"exclude_requester": False})
    assert response.status_code == 200, response.text
    for view in (readback(client, agent)["approval_routes"][ROUTE],
                 agent_state(agent)["approval_routes"][ROUTE]):
        assert view["separation"] == {"exclude_requester": False}
    opted = events_of(scope["tenant"], SEPARATION)
    assert len(opted) == 1, ticket_events(scope["tenant"])
    assert opted[0]["actor"] == "operator"
    assert ROUTE in json.dumps(opted[0]["details"])
    assert "false" in json.dumps(opted[0]["details"])

    # Back to exclusion: the unchanged registration is not replaced, the policy change is audited.
    again = register(client, scope, case.key, separation={"exclude_requester": True})
    assert again.status_code == 200, again.text
    assert agent_state(agent)["approval_routes"][ROUTE]["separation"] == {"exclude_requester": True}
    assert len(events_of(scope["tenant"], SEPARATION)) == 2
    assert events_of(scope["tenant"], REPLACED) == []


def test_separation_needs_a_registration_and_exempt_group_is_refused(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: no unenforced separation policy is stored before 0271b."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    before = agent_state(agent)
    ticket = registration(case.key, scope["role_version"])
    group = {"issuer": "https://identity.example.test/directory", "id": "SyntheticExempt"}
    bare = route_map(scope["address"], separation={"exclude_requester": True})
    refused_with(write_routes(client, agent, bare), 422, "separation")
    refused_with(write_routes(client, agent, bare, expected=before["version"]), 422, "separation")
    inside = route_map(scope["address"], ticket=ticket,
                       separation={"exclude_requester": True, "exempt_group": group})
    beside = route_map(scope["address"], ticket=ticket)
    beside[ROUTE]["exempt_group"] = group
    for routes in (inside, beside):
        response = write_routes(client, agent, routes, expected=before["version"])
        assert response.status_code == 422, response.text
        assert "exempt_group" in response.text
    assert agent_state(agent) == before
    accepted = write_routes(
        client, agent,
        route_map(scope["address"], ticket=ticket, separation={"exclude_requester": True}),
        expected=before["version"],
    )
    assert accepted.status_code == 200, accepted.text


def test_one_pair_on_two_routes_refuses(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-2: the owning route of a pair is never ambiguous."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    before = agent_state(agent)
    for second_key in (KEY_ID, "selector-example-2"):
        routes = route_map(scope["address"], ticket=registration(case.key, scope["role_version"]))
        routes[OTHER_ROUTE]["approver_ticket"] = registration(
            selector_key(2), scope["role_version"], key_id=second_key,
        )
        refused_with(write_routes(client, agent, routes, expected=before["version"]), 422,
                     "approver_ticket_pair_conflict")
        assert agent_state(agent) == before
    assert register(client, scope, case.key).status_code == 200


def test_agent_creation_cannot_carry_a_registration(client: TestClient, clean_db: None) -> None:
    """@spec REQUEST-APPROVAL-1: the flag is off at creation, so no route may register."""
    name = "ticket-create-" + uuid.uuid4().hex[:8]
    routes = {ROUTE: {
        "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
        "approver_ticket": registration(selector_key(), 1),
    }}
    body = {"name": name, "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approval_routes": routes}
    refused = client.post("/agents", json=body, headers=api_headers())
    assert refused.status_code == 422, refused.text
    assert "approver_ticket" in refused.text
    assert sql_dicts("SELECT id FROM curie.agents WHERE name = :n", {"n": name}) == []
    del routes[ROUTE]["approver_ticket"]
    created = client.post("/agents", json=body, headers=api_headers())
    assert created.status_code == 201, created.text


def test_agent_owned_role_edits_never_rewrite_the_pin(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-1: widening the registered ceiling needs an operator write.

    The agent-owned role writer is the access-policy role path; with this agent's
    migrated roles the team-only access-policy PUT is refused outright, so the
    widening goes through the role filter writer itself (plan D4 and its
    amendment for unchanged registrations).
    """
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    assert register(client, scope, case.key).status_code == 200
    old_version, old_role = stored_approver_role(agent)
    pin = stored_ticket(agent)
    widened = {"entries": CEILING_FILTER["entries"] + [subject_entry(OUTSIDER)]}
    new_version = set_approver_role(scope, widened)
    assert new_version == old_version + 1
    _, new_role = stored_approver_role(agent)
    assert new_role != old_role
    assert stored_ticket(agent) == pin
    assert readback(client, agent)["approval_routes"][ROUTE]["approver_ticket"]["ceiling"][
        "approver_role_version"] == old_version

    # 1. The unchanged registration, echoed from readback or with the pin omitted,
    #    beside an unrelated route edit: accepted, pin kept, nothing replaced.
    echoed = readback(client, agent)["approval_routes"][ROUTE]["approver_ticket"]
    for ticket in (echoed, registration(case.key, old_version)):
        routes = route_map(scope["address"], ticket=ticket)
        routes[SEED_ROUTE]["approvers"]["users"].append("U0EXAMPLE3")
        response = write_routes(client, agent, routes, expected=scope["config_version"])
        assert response.status_code == 200, response.text
        scope["config_version"] = response.json()[CONFIG_VERSION]
        assert stored_ticket(agent)["ceiling"]["approver_role_version"] == old_version
        assert pinned_role(stored_ticket(agent)["ceiling"]["approver_role"]) == old_role
    assert events_of(scope["tenant"], REPLACED) == []

    # 2. Any changed field still naming the old role version is stale.
    before = agent_state(agent)
    changed = registration(case.key, old_version, key_id="selector-example-2")
    refused_with(
        write_routes(client, agent, route_map(scope["address"], ticket=changed),
                     expected=scope["config_version"]),
        409, "stale_role_policy_version",
    )
    assert agent_state(agent) == before

    # 3. Naming the new version re-pins the widened role.
    repinned = write_routes(
        client, agent,
        route_map(scope["address"], ticket=registration(case.key, new_version)),
        expected=scope["config_version"],
    )
    assert repinned.status_code == 200, repinned.text
    assert stored_ticket(agent)["ceiling"]["approver_role_version"] == new_version
    assert pinned_role(stored_ticket(agent)["ceiling"]["approver_role"]) == new_role
    assert len(events_of(scope["tenant"], REPLACED)) == 1


def test_replacement_and_removal_are_versioned_and_audited(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: one audit row per changed registration."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    first = scope["config_version"]
    assert register(client, scope, case.key).status_code == 200
    rotated = selector_key(2)
    replaced = register(client, scope, rotated, key_id="selector-example-2")
    assert replaced.status_code == 200, replaced.text
    assert stored_ticket(agent)["public_key"] == public_key_text(rotated)
    assert stored_ticket(agent)["key_id"] == "selector-example-2"
    removed = write_routes(client, agent, route_map(scope["address"]),
                           expected=scope["config_version"])
    assert removed.status_code == 200, removed.text
    state = agent_state(agent)
    assert not {"approver_ticket", "separation"} & set(state["approval_routes"][ROUTE])
    assert state["version"] == first + 3
    kinds = [event["event_type"] for event in ticket_events(scope["tenant"])
             if event["event_type"].startswith("approver_ticket.")]
    assert kinds == [REGISTERED, REPLACED, REMOVED]
    text = json.dumps(ticket_events(scope["tenant"]))
    assert "selector-example-2" in text
    for key in (case.key, rotated):
        assert public_key_text(key) not in text


def test_registration_shape_refusals(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-1: the closed registration shape refuses before anything commits."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    before = agent_state(agent)
    ticket = registration(case.key, scope["role_version"])
    issuer = "https://identity.example.test/directory"
    short_key = base64.urlsafe_b64encode(bytes(case.key.verify_key)[:31]).rstrip(b"=").decode()
    variants: dict[str, dict[str, Any]] = {
        "connector_with_separator": {"connector": "acc__ess"},
        "plugin_connector": {"connector": "plugin_access"},
        "empty_connector": {"connector": ""},
        "empty_tool": {"tool": ""},
        "long_tool": {"tool": "t" * 257},
        "empty_key_id": {"key_id": ""},
        "padded_key_id": {"key_id": " selector-example-1"},
        "control_key_id": {"key_id": "selector\u0007example"},
        "long_key_id": {"key_id": "k" * 257},
        "padded_public_key": {"public_key": ticket["public_key"] + "="},
        "short_public_key": {"public_key": short_key},
        "outside_alphabet_public_key": {"public_key": ticket["public_key"][:-1] + "+"},
        "missing_ceiling": {"ceiling": None},
        "empty_users": {"ceiling": {**ticket["ceiling"], "users": []}},
        "duplicate_users": {"ceiling": {**ticket["ceiling"], "users": [
            {"issuer": issuer, "subject": "SyntheticSame"},
            {"issuer": issuer, "subject": "SyntheticSame"},
        ]}},
        "fractional_role_version": {"ceiling": {"approver_role_version": 2.5}},
        "missing_role_version": {"ceiling": {"users": [{"issuer": issuer, "subject": "S"}]}},
        "unknown_member": {"label": "synthetic"},
    }
    for name, changes in variants.items():
        malformed = {**ticket, **changes}
        if malformed.get("ceiling") is None:
            malformed.pop("ceiling")
        response = write_routes(client, agent, route_map(scope["address"], ticket=malformed),
                                expected=before["version"])
        assert response.status_code == 422, (name, response.text)
        assert agent_state(agent) == before, name
    assert ticket_events(scope["tenant"]) == []
    assert register(client, scope, case.key).status_code == 200


# --- AC2: the requester flag and selector invariant --------------------------------


def test_registration_refused_while_the_requester_flag_is_off(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1 CONNECTOR-REQUESTER-1: a selector needs the flag on."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, flag=False)
    before = agent_state(agent)
    assert before["flag"] is False
    refused_with(register(client, scope, case.key), 422,
                 "approver_ticket_requires_requester_identity")
    assert agent_state(agent) == before
    enabled = set_flag(client, agent, True, before["version"])
    assert enabled.status_code == 200, enabled.text
    scope["config_version"] = enabled.json()[CONFIG_VERSION]
    assert register(client, scope, case.key).status_code == 200


def test_flag_disable_refused_while_a_selector_is_registered(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1 CONNECTOR-REQUESTER-1: remove the selector, then disable."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    assert register(client, scope, case.key).status_code == 200
    before = agent_state(agent)
    refused_with(set_flag(client, agent, False, before["version"]), 422,
                 "requester_identity_selector_registered")
    assert agent_state(agent) == before
    removed = write_routes(client, agent, route_map(scope["address"]), expected=before["version"])
    assert removed.status_code == 200, removed.text
    disabled = set_flag(client, agent, False, before["version"] + 1)
    assert disabled.status_code == 200, disabled.text
    state = agent_state(agent)
    assert (state["flag"], state["version"], registered(state)) == (False, before["version"] + 2,
                                                                    False)
    scope["config_version"] = state["version"]
    refused_with(register(client, scope, case.key), 422,
                 "approver_ticket_requires_requester_identity")


def test_concurrent_flag_disable_and_registration_commit_exactly_once(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1 CONNECTOR-REQUESTER-1: two operators, one version, one winner.

    Two API instances each send one write naming the same stored version and
    start together at a barrier. Whichever commits moves the version; the
    other changes nothing. The stored combination is never the flag off with a
    selector registered. Repeated so both orders get a chance to occur.
    """
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    winners: list[str] = []
    with operator_app(case.runtime_for()) as first, operator_app(case.runtime_for()) as second:
        for _ in range(3):
            version = agent_state(agent)["version"]
            routes = route_map(
                scope["address"], ticket=registration(case.key, scope["role_version"]),
            )
            barrier = threading.Barrier(2)
            disabled, enrolled = asyncio.run(together(
                partial(at_barrier, barrier, partial(set_flag, first, agent, False, version)),
                partial(at_barrier, barrier,
                        partial(write_routes, second, agent, routes, expected=version)),
            ))
            statuses = sorted([disabled.status_code, enrolled.status_code])
            assert statuses[0] == 200 and statuses[1] in {409, 422}, (disabled.text, enrolled.text)
            loser = enrolled if disabled.status_code == 200 else disabled
            assert any(code in detail_text(loser) for code in LOSING_CODES), loser.text
            state = agent_state(agent)
            assert state["version"] == version + 1
            assert not (state["flag"] is False and registered(state)), state
            if disabled.status_code == 200:
                winners.append("disable")
                assert (state["flag"], registered(state)) == (False, False)
                restored = set_flag(client, agent, True, state["version"])
                assert restored.status_code == 200, restored.text
            else:
                winners.append("register")
                assert (state["flag"], registered(state)) == (True, True)
                removed = write_routes(client, agent, route_map(scope["address"]),
                                       expected=state["version"])
                assert removed.status_code == 200, removed.text
    assert len(winners) == 3


def at_barrier(barrier: threading.Barrier, call: Callable[[], Any]) -> Any:
    """Release both writers together so each reads the same stored version."""
    barrier.wait(timeout=15)
    return call()


async def together(*calls: Callable[[], Any]) -> list[Any]:
    """Run blocking request thunks on real concurrent threads and connections."""
    return list(await asyncio.gather(*(asyncio.to_thread(call) for call in calls)))


@pytest.mark.parametrize("stored", ["flag_off", "malformed", "duplicated_pair"])
def test_invalid_stored_selector_state_refuses_creation_and_disable(
    ticket_case: SimpleNamespace, stored: str,
) -> None:
    """@spec REQUEST-APPROVAL-1/2: hand-written invalid state fails closed (plan D7 rule 5).

    A registration with the flag off cannot be stored at all since migration
    0153: storage refuses it for every writer and the row keeps its state.
    """
    case = ticket_case
    agent = case.scope["agent"]
    enroll(case)
    routes = copy.deepcopy(agent_state(agent)["approval_routes"])
    expected_code = "approver_ticket_state_invalid"
    if stored == "flag_off":
        before = agent_state(agent)
        with pytest.raises(
            IntegrityError, match="agents_approver_ticket_requires_requester_identity_ck",
        ):
            store_routes(agent, routes, flag=False)
        assert agent_state(agent) == before
        return
    if stored == "malformed":
        routes[ROUTE]["approver_ticket"]["public_key"] = "not-a-key"
        store_routes(agent, routes)
    else:
        routes[OTHER_ROUTE]["approver_ticket"] = copy.deepcopy(routes[ROUTE]["approver_ticket"])
        store_routes(agent, routes)
    before = agent_state(agent)
    request = dynamic_request(case, carried(case, {"target": "acct-example-1"}))
    exactly(create(case.client, request), 409, expected_code)
    assert approval_rows(agent) == []
    refused_with(set_flag(case.client, agent, False, before["version"]), 422,
                 "requester_identity_selector_registered")
    assert agent_state(agent) == before
    removed = write_routes(case.client, agent, route_map(case.scope["address"]),
                           expected=before["version"])
    assert removed.status_code == 200, removed.text
    disabled = set_flag(case.client, agent, False, before["version"] + 1)
    assert disabled.status_code == 200, disabled.text
    state = agent_state(agent)
    assert (state["flag"], registered(state)) == (False, False)
