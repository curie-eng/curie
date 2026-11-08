"""ADR 0271 decision 6: separation of duties is explicit and canonical.

@spec REQUEST-APPROVAL-6 (with the operator refusal of REQUEST-APPROVAL-5).

Answers go through the real resolve route with receiver-signed proofs against
the roster TLS directory of ``approval_answer_support``: the requester answers
from its own Slack account and from a second account of the same canonical
person, distinct people answer from theirs. Route configuration goes through
the real operator write (``PATCH /agents/{id}``) and the agent-owned access
policy writer; only state no operator route can produce is written with SQL.
Every refusal is followed by its admitted neighbor. Synthetic, reserved example
values only.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

from approval_answer_support import (
    CONFIGURATION_VERSION,
    EQUALITY,
    EXCLUSION,
    EXEMPT_GROUP,
    EXEMPT_REF,
    EXEMPTION_USED,
    NATIVE_A,
    NATIVE_ALIAS,
    NEEDS_HTTP,
    OPERATOR,
    OPERATOR_REFUSED,
    SENDER,
    actions,
    answer_as,
    answered,
    audit_rows,
    configured,
    dynamic,
    grant_check,
    one_more_resume_refusal,
    ordinary,
    person,
    published,
    refused,
    requester,
    resume_refusals,
    separation_events,
    still_pending,
    turn_check,
    without_http_selection,
)
from approval_answer_support import selection_case as selection_case
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_ticket_support import (
    APPROVER_A,
    OTHER_ROUTE,
    REQUESTER,
    ROUTE,
    agent_state,
    api_headers,
    operator_app,
    prepare,
    refused_with,
    register,
    registration,
    route_map,
    store_routes,
    ticket_events,
    write_routes,
)
from approver_ticket_support import operator_case as operator_case
from fastapi.testclient import TestClient

from apps.api.tests.test_http_approval_legacy_writers import operator_headers

CONTINUE = ("continue", "current_authority")
REFUSE = ("refuse", "caller_not_allowed")
EXCLUDED = "separation_requester_excluded"
EXCLUDE = {"exclude_requester": True}
EXEMPTED = {"exclude_requester": True, "exempt_group": EXEMPT_REF}


# --- AC2.1: canonical equality across accounts --------------------------------------


def test_distinct_humans_answer_and_the_requester_is_excluded_on_every_account(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: equality compares tenant, issuer and subject, not accounts.

    The selector chose the requester and A. The requester is refused from its
    own Slack account and from a second account of the same canonical person,
    each audited with equality true; A, a distinct human, approves.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (REQUESTER, APPROVER_A))
    for native in (SENDER, NATIVE_ALIAS):
        evidence = refused(case, row, native, code=EXCLUDED)
        assert evidence[EQUALITY] is True and evidence[EXCLUSION] is True, evidence
        assert evidence[CONFIGURATION_VERSION] == case.scope["config_version"], evidence
        assert REQUESTER in str(evidence)
    selection = answered(case, row, NATIVE_A)
    assert selection[EQUALITY] is False and selection[EXCLUSION] is True, selection
    assert REQUESTER in str(selection) and APPROVER_A in str(selection)


# --- AC2.2: omission excludes; only an audited operator write opts out ---------------


def test_omitted_separation_excludes_and_an_audited_operator_opt_out_permits_self_approval(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: the stored effective choice, then an explicit audited ``false``."""
    case = selection_case
    scope = case.scope
    view = configured(case)
    assert view["approval_routes"][ROUTE]["separation"] == EXCLUDE
    assert agent_state(scope["agent"])["approval_routes"][ROUTE]["separation"] == EXCLUDE
    row = dynamic(case, (REQUESTER, APPROVER_A))
    refused(case, row, SENDER, code=EXCLUDED)
    assert separation_events(scope["tenant"]) == []

    old = scope["config_version"]
    configured(case, separation={"exclude_requester": False})
    new = scope["config_version"]
    assert new != old
    events = separation_events(scope["tenant"])
    assert len(events) == 1, events
    assert events[0]["actor"] == "operator"
    details = events[0]["details"]
    assert details["route"] == ROUTE
    assert (details["old_exclude_requester"], details["new_exclude_requester"]) == (True, False)
    assert (details["old_configuration_version"], details["new_configuration_version"]) == (
        old, new,
    )

    selection = answered(case, row, SENDER)
    assert selection[EXCLUSION] is False and selection[EQUALITY] is True, selection
    assert selection[CONFIGURATION_VERSION] == new


# --- AC2.3: the exemption is current membership, and waives only inequality -----------


def test_an_exemption_waives_only_requester_inequality_while_membership_is_current(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: membership, removal, unavailability and an unselected actor.

    The requester's directory groups decide, read fresh at answer and resume.
    """
    case = selection_case
    roster = case.provider.roster
    roster[SENDER] = requester(EXEMPT_GROUP)
    roster[NATIVE_ALIAS] = requester(EXEMPT_GROUP)
    configured(case, separation=EXEMPTED)
    assert agent_state(case.scope["agent"])["approval_routes"][ROUTE]["separation"] == EXEMPTED

    first = dynamic(case, (REQUESTER, APPROVER_A))
    selection = answered(case, first, SENDER)
    assert selection[EQUALITY] is True and selection[EXEMPTION_USED] is True, selection
    entry = published(case, first["id"])
    assert turn_check(case, entry, first["id"]) == CONTINUE

    # The requester leaves the exempt group: the resume and a new answer refuse.
    roster[SENDER] = requester()
    roster[NATIVE_ALIAS] = requester()
    before = len(resume_refusals(first["id"]))
    assert turn_check(case, entry, first["id"]) == REFUSE
    one_more_resume_refusal(first["id"], before, SENDER, code=EXCLUDED)
    assert grant_check(case, entry, first["id"]) == REFUSE
    one_more_resume_refusal(first["id"], before + 1, SENDER, code=EXCLUDED)

    second = dynamic(case, (REQUESTER, APPROVER_A))
    evidence = refused(case, second, SENDER, code=EXCLUDED)
    assert evidence[EQUALITY] is True and evidence[EXEMPTION_USED] is False, evidence

    # Membership the directory cannot report refuses as unavailable, never as exempt.
    roster[SENDER] = requester(EXEMPT_GROUP)
    roster[NATIVE_ALIAS] = person(REQUESTER, groups=None)
    refused(case, second, NATIVE_ALIAS, 503)
    roster[NATIVE_ALIAS] = requester(EXEMPT_GROUP)
    assert answered(case, second, NATIVE_ALIAS)[EXEMPTION_USED] is True

    # Exempt, but not selected: the exemption selects nobody.
    third = dynamic(case, (APPROVER_A,))
    refused(case, third, SENDER, code="selection_actor_not_selected")
    answered(case, third, NATIVE_A)


def test_an_exemption_without_exclusion_is_refused_at_write(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: ``exempt_group`` needs ``exclude_requester: true``."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    before, events = agent_state(agent), ticket_events(scope["tenant"])
    loose = {"exclude_requester": False, "exempt_group": EXEMPT_REF}
    selector = register(client, scope, case.key, separation=loose)
    assert selector.status_code == 422, selector.text
    static = route_map(scope["address"])
    static[OTHER_ROUTE]["separation"] = loose
    response = write_routes(client, agent, static, expected=scope["config_version"])
    assert response.status_code == 422, response.text
    assert agent_state(agent) == before
    assert ticket_events(scope["tenant"]) == events

    accepted = register(client, scope, case.key, separation=EXEMPTED)
    assert accepted.status_code == 200, accepted.text
    assert agent_state(agent)["approval_routes"][ROUTE]["separation"] == EXEMPTED


# --- AC2.4: an eligible rejection may stop the action --------------------------------


def test_the_requester_may_reject_on_an_exclusion_route(selection_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-6: exclusion refuses the requester's approval, not its rejection."""
    case = selection_case
    configured(case)
    row = dynamic(case, (REQUESTER, APPROVER_A))
    refused(case, row, SENDER, code=EXCLUDED)
    selection = answered(case, row, SENDER, decision="rejected")
    assert selection[EQUALITY] is True and selection[EXCLUSION] is True, selection
    assert selection["decision"] == "rejected"


# --- AC2.5: exclusion on a static route --------------------------------------------


def test_a_static_exclusion_route_excludes_the_requester_without_a_selector(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: the sealed original invoker is the requester on a static route.

    No selector is registered. An ordinary approval on the static exclusion
    route refuses the requester on either account and admits another
    approver; an ordinary route keeps ADR 0106 self-confirmation.
    """
    case = selection_case
    configured(case, ticket=False, static=EXCLUDE)
    stored = agent_state(case.scope["agent"])["approval_routes"]
    assert stored[OTHER_ROUTE]["separation"] == EXCLUDE
    assert not {"approver_ticket", "separation"} & set(stored[ROUTE])

    row = ordinary(case, OTHER_ROUTE)
    for native in (SENDER, NATIVE_ALIAS):
        evidence = refused(case, row, native, code=EXCLUDED)
        assert evidence[EQUALITY] is True, evidence
    selection = answered(case, row, NATIVE_A)
    assert selection[EQUALITY] is False and selection[EXCLUSION] is True, selection

    plain = ordinary(case, None)
    response = answer_as(case, plain, SENDER)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"


# --- AC2.6: enabling needs a selecting tenant and a registered receiver ---------------


def test_enabling_separation_needs_a_registered_receiver_binding(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: static exclusion or an exemption without a registered receiver.

    An API whose runtime lists none of the agent's bindings refuses both
    writes and stores nothing; the operator API whose runtime lists the
    binding accepts the same writes.
    """
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    static = route_map(scope["address"])
    static[OTHER_ROUTE]["separation"] = EXCLUDE
    before, events = agent_state(agent), ticket_events(scope["tenant"])
    with operator_app(None) as bare:
        refused_with(write_routes(bare, agent, static, expected=scope["config_version"]),
                     422, NEEDS_HTTP)
    assert agent_state(agent) == before and ticket_events(scope["tenant"]) == events
    written = write_routes(client, agent, static, expected=scope["config_version"])
    assert written.status_code == 200, written.text
    scope["config_version"] = written.json()["connector_requester_identity_version"]
    assert agent_state(agent)["approval_routes"][OTHER_ROUTE]["separation"] == EXCLUDE

    assert register(client, scope, case.key).status_code == 200
    exempted = route_map(
        scope["address"], ticket=registration(case.key, scope["role_version"]),
        separation=EXEMPTED,
    )
    before, events = agent_state(agent), ticket_events(scope["tenant"])
    with operator_app(None) as bare:
        refused_with(write_routes(bare, agent, exempted, expected=scope["config_version"]),
                     422, NEEDS_HTTP)
    assert agent_state(agent) == before and ticket_events(scope["tenant"]) == events
    written = write_routes(client, agent, exempted, expected=scope["config_version"])
    assert written.status_code == 200, written.text
    assert agent_state(agent)["approval_routes"][ROUTE]["separation"] == EXEMPTED


def test_enabling_separation_in_a_tenant_without_http_identity_is_refused(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: a tenant on its local authority cannot enable exclusion.

    Static exclusion and an exemption beside a registered selector are both
    refused while the tenant has no HTTP identity selection, storing nothing;
    with the selection back, the same writes are accepted.
    """
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    assert register(client, scope, case.key).status_code == 200
    ticket = registration(case.key, scope["role_version"])
    static = route_map(scope["address"], ticket=ticket)
    static[OTHER_ROUTE]["separation"] = EXCLUDE
    exempted = route_map(scope["address"], ticket=ticket, separation=EXEMPTED)
    before, events = agent_state(agent), ticket_events(scope["tenant"])
    with without_http_selection(scope["tenant"]):
        for routes in (static, exempted):
            refused_with(write_routes(client, agent, routes, expected=scope["config_version"]),
                         422, NEEDS_HTTP)
            assert agent_state(agent) == before and ticket_events(scope["tenant"]) == events
    written = write_routes(client, agent, static, expected=scope["config_version"])
    assert written.status_code == 200, written.text
    scope["config_version"] = written.json()["connector_requester_identity_version"]
    assert agent_state(agent)["approval_routes"][OTHER_ROUTE]["separation"] == EXCLUDE
    written = write_routes(client, agent, exempted, expected=scope["config_version"])
    assert written.status_code == 200, written.text
    assert agent_state(agent)["approval_routes"][ROUTE]["separation"] == EXEMPTED


# --- AC2.7: no agent-owned or unversioned opt-out --------------------------------------


def test_agent_owned_writers_cannot_opt_out_and_route_writes_need_the_version(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-6: the access-policy writer has no separation field.

    A separation write must name the configuration version, for a static
    route and for a selector opt-out alike; the versioned operator write is
    the one accepted and audited.
    """
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    static = route_map(scope["address"])
    static[OTHER_ROUTE]["separation"] = EXCLUDE
    before = agent_state(agent)
    refused_with(write_routes(client, agent, static), 409,
                 "approval_routes.expected_version_required")
    assert agent_state(agent) == before

    assert register(client, scope, case.key).status_code == 200
    before = agent_state(agent)
    path = f"/agents/{agent}/access-policy"
    current = client.get(path, headers=api_headers())
    assert current.status_code == 200, current.text
    policy = {
        "invocation_team_id": None, "approver_team_id": None,
        "expected_version": current.json()["version"],
    }
    for extra in (
        {"separation": {"exclude_requester": False}},
        {"approval_routes": {ROUTE: {"separation": {"exclude_requester": False}}}},
    ):
        response = client.put(path, json={**policy, **extra}, headers=api_headers())
        assert response.status_code == 422, response.text
    opted = route_map(
        scope["address"], ticket=registration(case.key, scope["role_version"]),
        separation={"exclude_requester": False},
    )
    refused_with(write_routes(client, agent, opted), 409,
                 "approval_routes.expected_version_required")
    assert agent_state(agent) == before
    assert separation_events(scope["tenant"]) == []

    written = write_routes(client, agent, opted, expected=scope["config_version"])
    assert written.status_code == 200, written.text
    assert agent_state(agent)["approval_routes"][ROUTE]["separation"] == {
        "exclude_requester": False,
    }
    assert len(separation_events(scope["tenant"])) == 1


# --- AC2.8: an operator principal never answers a canonical route ---------------------


def _operator_resolves(client: TestClient, approval_id: Any) -> Any:
    """@spec REQUEST-APPROVAL-5: the platform key plus an operator principal (U0OPERATOR1)."""
    return client.post(
        f"/approvals/{approval_id}/resolve", json={"decision": "approved"},
        headers=operator_headers(),
    )


def _operator_refused(client: TestClient, approval_id: Any) -> None:
    """403 with the authorizer's reason, one ``denied`` row naming the operator refusal.

    @spec REQUEST-APPROVAL-5.
    """
    response = _operator_resolves(client, approval_id)
    assert response.status_code == 403, response.text
    denied = actions(approval_id, "denied")
    assert len(denied) == 1, denied
    assert (denied[0]["actor"], denied[0]["principal_kind"]) == (OPERATOR, "operator")
    assert denied[0]["evidence"]["kind"] == OPERATOR_REFUSED, denied[0]
    assert response.json() == {"detail": denied[0]["reason"]}
    assert denied[0]["reason"] != "unsupported_approval"


def test_operator_principal_is_refused_on_canonical_routes_of_the_selected_path(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5 REQUEST-APPROVAL-6: dynamic and static exclusion, explicit users.

    Both routes bind an explicit user list naming the operator, which would
    make it eligible on an ordinary route. The operator is refused and
    audited; a selected canonical human on the same routes is admitted, and an
    ordinary selected approval keeps today's bare refusal.
    """
    case = selection_case
    listed = {"users": [OPERATOR, NATIVE_A]}
    configured(case, approvers=listed, static=EXCLUDE, static_approvers=listed)
    selected = dynamic(case, (APPROVER_A,))
    excluded = ordinary(case, OTHER_ROUTE)
    for row in (selected, excluded):
        _operator_refused(case.client, row["id"])
        still_pending(row["id"])

    plain = ordinary(case, None)
    response = _operator_resolves(case.client, plain["id"])
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "unsupported_approval"}
    assert audit_rows(plain["id"]) == []

    answered(case, selected, NATIVE_A)
    answered(case, excluded, NATIVE_A)


LEGACY_ROUTE = "synthetic-legacy-review"


def _legacy_approval(client: TestClient, agent: str, address: str) -> str:
    """@spec REQUEST-APPROVAL-6: an ordinary approval through the public legacy creation owner."""
    body = {
        "agent_id": agent, "conversation_id": address + ":synthetic-thread",
        "author": "U0EXAMPLE2", "summary": "Approve the synthetic legacy operation",
        "reply_kind": "slack", "reply_channel": address,
        "reply_placeholder": "1760000000.000200",
        "dedupe_key": "synthetic-legacy-" + uuid.uuid4().hex,
        "route": LEGACY_ROUTE, "card_channel": address,
        "gate_kind": "permission", "granted_tool": "synthetic_read", "granted_arguments": {},
    }
    created = client.post("/approvals", json=body, headers=api_headers())
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def test_operator_principal_is_refused_on_a_canonical_route_of_the_legacy_path(
    client: TestClient, clean_db: None,
) -> None:
    """@spec REQUEST-APPROVAL-5 REQUEST-APPROVAL-6: a tenant without HTTP identity.

    The legacy answer path keeps working for ordinary routes, but a route
    whose stored separation enables exclusion refuses the operator principal
    first, even with an explicit user list naming it. Writes cannot enable
    exclusion here, so the stored route is written with SQL.
    """
    address = "C0EXAMPLELEGACY"
    created = client.post(
        "/agents",
        json={"name": "legacy-separation-" + uuid.uuid4().hex[:8],
              "channel": {"kind": "slack", "address": address}},
        headers=api_headers(),
    )
    assert created.status_code == 201, created.text
    agent = str(created.json()["id"])
    plain: dict[str, Any] = {
        "resolution": {"kind": "slack", "address": address},
        "approvers": {"users": [OPERATOR]},
    }
    store_routes(agent, {LEGACY_ROUTE: plain})
    excluded = _legacy_approval(client, agent, address)
    neighbor = _legacy_approval(client, agent, address)

    store_routes(agent, {LEGACY_ROUTE: {**plain, "separation": EXCLUDE}})
    _operator_refused(client, excluded)
    assert agent_state(agent)["approval_routes"][LEGACY_ROUTE]["separation"] == EXCLUDE
    pending = client.get(f"/approvals/{excluded}", headers=api_headers())
    assert pending.status_code == 200 and pending.json()["status"] == "pending", pending.text

    store_routes(agent, {LEGACY_ROUTE: plain})
    response = _operator_resolves(client, neighbor)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"
