"""ADR 0271 decision 4: verify once at creation and keep the immutable selection.

@spec REQUEST-APPROVAL-4 (with the creation-time rechecks of REQUEST-APPROVAL-1/2).

Every creation goes through ``POST /v1/internal/identity/approvals`` with a real
ingress capsule from ``/channels/turns``, the synthetic TLS directory, Postgres
and Valkey, exactly like ``test_http_approval_creation.py``. Outcomes are read
at the HTTP boundary and in stored rows: the approval row and its selection
columns, the sealed capsule, the approval audit log. Every refusal is followed
by the neighboring valid input on the same agent, which must create, so a
refusal cannot pass by refusing everything. Tickets are signed with the frozen
connector helper over ``execution_arguments_sha256``; keys and people are
synthetic.
"""

from __future__ import annotations

import asyncio
import copy
import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any

import pytest
from approver_ticket_support import (
    APPROVER_A,
    APPROVER_B,
    APPROVER_GROUP,
    CEILING_FILTER,
    CONNECTOR,
    GROUP_MEMBER,
    KEY_ID,
    LIVE,
    MANAGED_BY,
    OTHER_CONNECTOR,
    OTHER_ROUTE,
    OUTSIDER,
    ROUTE,
    TOOL,
    add_principal,
    add_team,
    agent_state,
    api_headers,
    approval_audits,
    approval_rows,
    carried,
    create,
    deploy,
    dynamic_request,
    enroll,
    exactly,
    group_entry,
    ingress,
    join_team,
    legacy,
    now,
    owner_verifies,
    pinned_role,
    principal_claims,
    refused_with,
    selector_key,
    set_approver_role,
    set_idp_managed_by,
    store_routes,
    stored_version,
    subject_entry,
    surface_entry,
    ticket_payload,
)
from approver_ticket_support import synthetic_provider as synthetic_provider
from approver_ticket_support import ticket_case as ticket_case
from curie_api.action_forward import canonical_arguments
from curie_api.approver_tickets import verify_ticket
from curie_api.config import get_settings
from curie_api.identity.http_transport import HttpIdentityClient
from curie_internal import identity_approval as wire
from curie_internal.approver_ticket import TicketError, execution_arguments_sha256
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.api.tests.test_http_approval_answers import answer, resolve
from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    accepted,
    canonical,
    digest,
    opened,
    request_for,
    rows,
)
from apps.api.tests.test_http_approval_legacy_writers import operator_headers

SELECTION_FIELDS = (
    "selection_mode", "selection_requester", "selection_connector", "selection_tool",
    "selection_key_id", "selection_nonce", "selection_arguments_sha256", "selected_approvers",
)
ARGS: dict[str, Any] = {"account": "acct-example-1", "change": {"role": "reader"}, "count": 2}
# The legacy creation response shape today; ordinary legacy creation must not change it.
LEGACY_KEYS = {
    "id", "agent_id", "conversation_id", "author", "summary", "display_summary",
    "reply_channel", "reply_placeholder", "reply_endpoint", "dedupe_key", "route",
    "card_channel", "gate_kind", "granted_tool", "status", "expires_at", "resolved_by",
    "resolution_note", "created_at", "resolved_at", "requested_by",
}


def refused_creation(
    case: SimpleNamespace, request: dict[str, Any], status: int, code: str,
) -> None:
    """A refused creation answers with its code and leaves no approval for the event."""
    exactly(create(case.client, request), status, code)
    assert rows(request["approval"]["dedupe_key"]) == []


def recipient(
    scope: dict[str, Any], account: str, *, installation: str | None = None,
) -> dict[str, Any]:
    """A candidate recipient account in the identity owner's scope vocabulary."""
    namespace = scope["namespace"]
    return {
        "provider": "slack",
        "installation_id": installation or scope["installation"],
        "namespace": {
            "id": namespace.id, "authority": namespace.authority,
            "kind": namespace.kind, "key": namespace.key,
        },
        "account_id": account,
    }


# --- the valid path ---------------------------------------------------------------


def test_valid_ticket_creates_one_sealed_dynamic_approval(ticket_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-4: nonce, row, snapshot, seal and audit commit together."""
    case = ticket_case
    enroll(case)
    payload = ticket_payload(case, ARGS)
    arguments = carried(case, ARGS, payload=payload)
    request = dynamic_request(case, arguments)
    created = accepted(create(case.client, request), request)
    assert created["status"] == "pending"

    stored = rows(request["approval"]["dedupe_key"])
    assert len(stored) == 1
    row = stored[0]
    assert (row["route"], row["granted_tool"]) == (ROUTE, LIVE)
    assert row["granted_arguments"] == arguments
    assert row["selection_mode"] == "dynamic"
    assert row["selection_requester"] == payload["requester"]
    assert (row["selection_connector"], row["selection_tool"], row["selection_key_id"]) == (
        CONNECTOR, TOOL, KEY_ID,
    )
    assert row["selection_nonce"] == payload["nonce"]
    assert row["selection_arguments_sha256"] == execution_arguments_sha256(ARGS)
    assert row["selected_approvers"] == payload["approvers"]

    # The seal covers the stored snapshot, selection included (plan D11).
    _, _, capsule = opened(case, row)
    assert capsule.approval_id == created["id"] == str(row["id"])
    assert capsule.approval_record_sha256 == digest(canonical({
        key: row[key] for key in RECORD_FIELDS + SELECTION_FIELDS
    }))
    assert capsule.approval_record_sha256 != digest(canonical({
        key: row[key] for key in RECORD_FIELDS
    }))
    assert owner_verifies(case, created["id"])

    # One created audit entry with selection evidence, never the ticket text.
    audits = approval_audits(created["id"])
    created_entries = [entry for entry in audits if entry["action"] == "created"]
    assert len(created_entries) == 1, audits
    evidence = json.dumps(created_entries[0]["evidence"])
    for value in (ROUTE, KEY_ID, payload["arguments_sha256"]):
        assert value in evidence, evidence
    assert arguments["approval_ticket"] not in json.dumps(audits, default=str)


# --- wire, key, scope and requester refusals --------------------------------------


def test_wire_key_scope_and_requester_refusals_consume_nothing(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-4: one verifier; each refusal is 422 with its frozen code."""
    case = ticket_case
    enroll(case)
    tenant = case.scope["tenant"]
    other_tenant = str(uuid.uuid4())
    foreign = ticket_payload(case, ARGS)
    foreign.update(
        tenant_id=other_tenant,
        requester={**foreign["requester"], "tenant_id": other_tenant},
        approvers=[principal_claims(other_tenant, s) for s in (APPROVER_A, APPROVER_B)],
    )
    stamp = now()
    variants: list[tuple[str, dict[str, Any], str]] = [
        ("bad_signature", carried(case, ARGS, key=selector_key(2)), "invalid_ticket"),
        ("garbage", {**ARGS, "approval_ticket": "cat1.e30.AAAA"}, "invalid_ticket"),
        ("other_key_id", carried(case, ARGS, key_id="selector-example-2"), "key_mismatch"),
        ("expired", carried(case, ARGS, issued_at=stamp - 600, expires_at=stamp - 301),
         "preparation_expired"),
        ("not_yet_valid", carried(case, ARGS, issued_at=stamp + 120, expires_at=stamp + 400),
         "preparation_expired"),
        ("changed_after_signing", {**carried(case, ARGS), "count": 3}, "arguments_mismatch"),
        ("tenant", carried(case, ARGS, payload=foreign), "scope_mismatch"),
        ("agent", carried(case, ARGS, agent_id=str(uuid.uuid4())), "scope_mismatch"),
        ("connector", carried(case, ARGS, connector=OTHER_CONNECTOR), "scope_mismatch"),
        ("tool", carried(case, ARGS, tool="read"), "scope_mismatch"),
        ("route", carried(case, ARGS, route=OTHER_ROUTE), "scope_mismatch"),
        ("requester_subject",
         carried(case, ARGS, requester=principal_claims(tenant, APPROVER_A)), "requester_mismatch"),
        ("requester_issuer", carried(case, ARGS, requester={
            "tenant_id": tenant, "issuer": "https://other.example.test/directory",
            "subject": case.context.canonical.subject,
        }), "requester_mismatch"),
    ]
    for name, arguments, code in variants:
        request = dynamic_request(case, arguments)
        response = create(case.client, request)
        assert response.status_code == 422, (name, response.text)
        exactly(response, 422, f"approval_ticket_{code}")
    assert approval_rows(case.scope["agent"]) == []
    request = dynamic_request(case, carried(case, ARGS))
    accepted(create(case.client, request), request)
    assert len(approval_rows(case.scope["agent"])) == 1


def test_oversize_grant_and_duplicate_recipient_accounts_refuse(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-3/4: the complete grant must fit; one account per person."""
    case = ticket_case
    enroll(case)
    scope, tenant = case.scope, case.scope["tenant"]
    bulky = {**ARGS, "note": "x" * 3600}
    refused_creation(case, dynamic_request(case, carried(case, bulky)), 422,
                     "approval_ticket_grant_too_large")
    within = [
        {**principal_claims(tenant, APPROVER_A),
         "recipient_accounts": [recipient(scope, "U0EXAMPLEA1"), recipient(scope, "U0EXAMPLEA2")]},
        principal_claims(tenant, APPROVER_B),
    ]
    across = [
        {**principal_claims(tenant, APPROVER_A),
         "recipient_accounts": [recipient(scope, "U0EXAMPLESHARED")]},
        {**principal_claims(tenant, APPROVER_B),
         "recipient_accounts": [recipient(scope, "U0EXAMPLESHARED")]},
    ]
    for approvers in (within, across):
        payload = ticket_payload(case, ARGS)
        payload["approvers"] = approvers
        refused_creation(case, dynamic_request(case, carried(case, ARGS, payload=payload)), 422,
                         "approval_ticket_recipient_duplicate")
    distinct = [
        {**principal_claims(tenant, APPROVER_A),
         "recipient_accounts": [recipient(scope, "U0EXAMPLEA1")]},
        {**principal_claims(tenant, APPROVER_B),
         "recipient_accounts": [recipient(scope, "U0EXAMPLEB1")]},
    ]
    payload = ticket_payload(case, ARGS)
    payload["approvers"] = distinct
    request = dynamic_request(case, carried(case, ARGS, payload=payload))
    accepted(create(case.client, request), request)
    assert rows(request["approval"]["dedupe_key"])[0]["selected_approvers"] == distinct


# --- current identity and the pinned ceiling --------------------------------------


def test_selected_people_must_be_active_humans_inside_the_pinned_role(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1/4: canonical records decide, never ticket content.

    The pinned role names every refused person, so only their current record
    refuses them. An agent-owned edit that later widens the role to an outsider
    cannot widen the pinned ceiling.
    """
    case = ticket_case
    tenant = case.scope["tenant"]
    unknown, disabled, service = "SyntheticUnknown", "SyntheticDisabled", "SyntheticService"
    set_approver_role(case.scope, {"entries": CEILING_FILTER["entries"] + [
        subject_entry(unknown), subject_entry(disabled), subject_entry(service),
    ]})
    enroll(case)
    add_principal(tenant, disabled, status="disabled")
    add_principal(tenant, service, kind="service")
    add_principal(tenant, OUTSIDER)
    for approvers, code in (
        ((unknown,), "approver_inactive"),
        ((disabled,), "approver_inactive"),
        ((service,), "approver_inactive"),
        ((APPROVER_A, disabled), "approver_inactive"),
        ((OUTSIDER,), "outside_ceiling"),
        ((APPROVER_A, OUTSIDER), "outside_ceiling"),
    ):
        request = dynamic_request(case, carried(case, ARGS, approvers=approvers))
        refused_creation(case, request, 422, f"approval_ticket_{code}")
    set_approver_role(case.scope, {
        "entries": CEILING_FILTER["entries"] + [subject_entry(OUTSIDER)],
    })
    request = dynamic_request(case, carried(case, ARGS, approvers=(OUTSIDER,)))
    refused_creation(case, request, 422, "approval_ticket_outside_ceiling")
    request = dynamic_request(case, carried(case, ARGS))
    accepted(create(case.client, request), request)


def test_ceiling_users_narrow_the_selected_set(ticket_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-1: an explicit user list narrows the pinned role."""
    case = ticket_case
    enroll(case, users=[APPROVER_A])
    for approvers in ((APPROVER_B,), (APPROVER_A, APPROVER_B)):
        request = dynamic_request(case, carried(case, ARGS, approvers=approvers))
        refused_creation(case, request, 422, "approval_ticket_outside_ceiling")
    request = dynamic_request(case, carried(case, ARGS, approvers=(APPROVER_A,)))
    accepted(create(case.client, request), request)


def test_group_membership_never_proves_the_ceiling_at_creation(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: only exact canonical identity proves membership (plan D9.3).

    The person is an active human in a validly synced idp_group team (managed
    by the tenant IdP, epoch 1, synced) that the pinned role names by a group
    entry. Group entries never match at creation, so they are refused until the
    operator pins a role naming them by issuer and subject.
    """
    case = ticket_case
    tenant = case.scope["tenant"]
    member = add_principal(tenant, GROUP_MEMBER)
    set_idp_managed_by(tenant, MANAGED_BY)
    team = add_team(tenant, APPROVER_GROUP, synced=True)
    join_team(tenant, member, team)
    pinned = enroll(case)["approval_routes"][ROUTE]["approver_ticket"]["ceiling"]
    assert group_entry(APPROVER_GROUP) in pinned_role(pinned["approver_role"])["filter"]["entries"]

    def attempt() -> dict[str, Any]:
        return dynamic_request(case, carried(case, ARGS, approvers=(GROUP_MEMBER,)))

    refused_creation(case, attempt(), 422, "approval_ticket_outside_ceiling")
    set_approver_role(case.scope, {
        "entries": CEILING_FILTER["entries"] + [subject_entry(GROUP_MEMBER)],
    })
    # The agent-owned edit alone does not widen the pin; the operator re-pin does.
    refused_creation(case, attempt(), 422, "approval_ticket_outside_ceiling")
    enroll(case)
    request = attempt()
    accepted(create(case.client, request), request)


def test_surface_account_only_role_never_matches_at_creation(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-1: a surface account cannot be established from a canonical id."""
    case = ticket_case
    set_approver_role(case.scope, {"entries": [surface_entry(case.scope)]})
    enroll(case)
    request = dynamic_request(case, carried(case, ARGS, approvers=(APPROVER_A,)))
    refused_creation(case, request, 422, "approval_ticket_outside_ceiling")
    # Neighbor: the operator re-pins a subject-restricted role and the same person creates.
    set_approver_role(case.scope, CEILING_FILTER)
    enroll(case)
    request = dynamic_request(case, carried(case, ARGS, approvers=(APPROVER_A,)))
    accepted(create(case.client, request), request)


# --- JSONB, nonce, retries, races and rollback -------------------------------------


def jsonb_round_trip(arguments: dict[str, Any]) -> bool:
    """Premise oracle: whether Postgres JSONB returns these arguments canonically unchanged."""
    result: list[Any] = []

    async def probe() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result.append(await connection.scalar(
                    text("SELECT CAST(:value AS jsonb)::text"), {"value": json.dumps(arguments)},
                ))
        finally:
            await engine.dispose()

    asyncio.run(probe())
    return canonical_arguments(json.loads(result[0])) == canonical_arguments(arguments)


def test_jsonb_unstable_arguments_refuse_without_a_row(ticket_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-4: the stored arguments must be the signed arguments (plan D12).

    The oracle is Postgres itself: negative zero and exponent-sized floats come
    back as a different JSON spelling, so a grant minted from the stored row
    would not match the signed digest. Integral floats that JSONB preserves,
    an exponent spelling that parses to one of them, strings and integers stay
    creatable.
    """
    case = ticket_case
    enroll(case)
    unstable = [
        {**ARGS, "amount": -0.0},
        {**ARGS, "amount": 1e16},
        {**ARGS, "amount": 1e300},
        {**ARGS, "items": [1, {"delta": -0.0}]},
    ]
    for arguments in unstable:
        assert not jsonb_round_trip(arguments), arguments
        refused_creation(case, dynamic_request(case, carried(case, arguments)), 422,
                         "approval_ticket_arguments_unstable")
    assert approval_rows(case.scope["agent"]) == []
    stable = {**ARGS, "amount": 1.0, "scale": 100000.0, "label": "x", "total": 7}
    assert jsonb_round_trip(stable)
    request = dynamic_request(case, carried(case, stable))
    # The literal exponent spelling 1E5 parses to the float the ticket signed.
    body = json.dumps(request).replace("100000.0", "1E5")
    assert "1E5" in body
    response = case.client.post(
        "/v1/internal/identity/approvals", content=body.encode("utf-8"),
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token,
                 "Content-Type": "application/json"},
    )
    accepted(response, request)
    assert rows(request["approval"]["dedupe_key"])[0]["granted_arguments"] == {
        **stable, "approval_ticket": request["approval"]["granted_arguments"]["approval_ticket"],
    }


def test_nonce_replay_under_another_dedupe_key_refuses(ticket_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-4: a nonce is single use within the agent's selector scope."""
    case = ticket_case
    enroll(case)
    arguments = carried(case, ARGS)
    first = dynamic_request(case, arguments)
    accepted(create(case.client, first), first)
    second = ingress(case)
    replay = dynamic_request(second, arguments)
    refused_creation(case, replay, 409, "approval_ticket_replayed")
    fresh = dynamic_request(second, carried(case, ARGS))
    accepted(create(case.client, fresh), fresh)
    assert len(approval_rows(case.scope["agent"])) == 2


def test_identical_retry_keeps_its_receipt_after_preparation_expiry(
    ticket_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-4: identical retries are historical; changed ones conflict."""
    case = ticket_case
    enroll(case)
    request = dynamic_request(case, carried(case, ARGS))
    first = accepted(create(case.client, request), request)
    later = now() + 3600
    with pytest.raises(TicketError, match="preparation_expired"):
        verify_ticket(
            public_key=case.key.verify_key, key_id=KEY_ID,
            arguments=request["approval"]["granted_arguments"], clock=lambda: later,
        )
    retry = copy.deepcopy(request)
    retry["request_id"] = str(uuid.uuid4())
    with monkeypatch.context() as patched:
        patched.setattr(time, "time", lambda: float(later))
        historical = accepted(create(case.client, retry), retry, duplicate=True)
    assert historical["id"] == first["id"]
    summary = copy.deepcopy(retry)
    summary["approval"]["summary"] += " changed"
    other_ticket = copy.deepcopy(retry)
    other_ticket["approval"]["granted_arguments"] = carried(case, ARGS)
    for conflicting in (summary, other_ticket):
        exactly(create(case.client, conflicting), 409, "approval_provenance_conflict")
    assert len(rows(request["approval"]["dedupe_key"])) == 1


def test_identical_creation_race_yields_one_row_and_one_nonce(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-4: N concurrent identical posts converge on one approval.

    A loser either reads the winner's receipt (200) or, as the existing
    continuity contract allows for a stale capture, answers 503; it never
    creates a second row or spends the nonce twice.
    """
    case = ticket_case
    enroll(case)
    payload = ticket_payload(case, ARGS)
    request = dynamic_request(case, carried(case, ARGS, payload=payload))
    copies = []
    for _ in range(4):
        item = copy.deepcopy(request)
        item["request_id"] = str(uuid.uuid4())
        copies.append(item)
    barrier = threading.Barrier(len(copies))

    def post(item: dict[str, Any]) -> Any:
        barrier.wait(timeout=15)
        return create(case.client, item)

    with ThreadPoolExecutor(max_workers=len(copies)) as pool:
        responses = list(pool.map(post, copies))
    statuses = [response.status_code for response in responses]
    assert statuses.count(201) == 1, [(r.status_code, r.text) for r in responses]
    winner = accepted(responses[statuses.index(201)], copies[statuses.index(201)])
    for item, response in zip(copies, responses, strict=True):
        if response.status_code == 200:
            assert accepted(response, item, duplicate=True)["id"] == winner["id"]
        else:
            assert response.status_code in {201, 503}, response.text
    replay = copy.deepcopy(request)
    replay["request_id"] = str(uuid.uuid4())
    assert accepted(create(case.client, replay), replay, duplicate=True)["id"] == winner["id"]
    stored = approval_rows(case.scope["agent"])
    assert len(stored) == 1
    assert [row["selection_nonce"] for row in stored] == [payload["nonce"]]


def test_failed_seal_rolls_back_the_row_and_the_nonce(
    ticket_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-4: no nonce is consumed without its committed approval."""
    case = ticket_case
    enroll(case)
    request = dynamic_request(case, carried(case, ARGS))

    def broken(*_args: Any, **_kwargs: Any) -> str:
        raise RuntimeError("synthetic sealing failure after insert")

    with monkeypatch.context() as patched:
        patched.setattr(wire, "seal_approval_context", broken)
        refused_creation(case, request, 503, "authority_unavailable")
    assert approval_rows(case.scope["agent"]) == []
    accepted(create(case.client, request), request)


# --- serving bundle and snapshot rechecks (plan D9, D9a) ---------------------------


def test_creation_refuses_when_registration_and_serving_bundle_disagree(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: fail closed at creation whatever produced the mismatch."""
    case = ticket_case
    enroll(case)
    agent = case.scope["agent"]
    consistent = agent_state(agent)["approval_routes"]
    moved = copy.deepcopy(consistent)
    moved[OTHER_ROUTE]["approver_ticket"] = moved[ROUTE].pop("approver_ticket")
    moved[OTHER_ROUTE]["separation"] = moved[ROUTE].pop("separation")
    store_routes(agent, moved)
    request = dynamic_request(
        case, carried(case, ARGS, route=OTHER_ROUTE), route=OTHER_ROUTE,
    )
    refused_creation(case, request, 409, "approver_ticket.bundle_gate_mismatch")
    store_routes(agent, consistent)
    request = dynamic_request(case, carried(case, ARGS))
    accepted(create(case.client, request), request)


def test_deployment_landing_between_phases_refuses_and_spends_nothing(
    ticket_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec REQUEST-APPROVAL-2/4: the judged serving set is part of the creation snapshot.

    A second correctly gated version is deployed and stopped through the API;
    during the directory lookup, outside every transaction, its deployment is
    flipped active or stopped on each attempt.
    """
    case = ticket_case
    enroll(case)
    agent = case.scope["agent"]
    version = stored_version(case.client, agent, ((LIVE, ROUTE), ("Bash", OTHER_ROUTE)))
    deployed = deploy(case.client, agent, version)
    assert deployed.status_code == 201, deployed.text
    landing = deployed.json()["id"]
    ended = case.client.delete(f"/deployments/{landing}", headers=api_headers())
    assert ended.status_code == 204, ended.text
    original = HttpIdentityClient.lookup
    flips: list[str] = []

    async def lookup(client: HttpIdentityClient, **kwargs: Any) -> Any:
        result = await original(client, **kwargs)
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                status = await connection.scalar(text(
                    "UPDATE curie.deployments SET status = CASE WHEN status = 'active' "
                    "THEN 'stopped' ELSE 'active' END WHERE id = :id RETURNING status"
                ), {"id": uuid.UUID(landing)})
                flips.append(status)
        finally:
            await engine.dispose()
        return result

    request = dynamic_request(case, carried(case, ARGS))
    with monkeypatch.context() as patched:
        patched.setattr(HttpIdentityClient, "lookup", lookup)
        refused_creation(case, request, 503, "authority_unavailable")
    assert flips and flips[0] == "active"
    accepted(create(case.client, request), request)


# --- legacy boundary and existing ordinary approvals -------------------------------


def test_legacy_creation_refuses_tickets_and_ordinary_legacy_is_unchanged(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2/4: legacy creation cannot create a selected approval."""
    case = ticket_case
    enroll(case)
    selected = dynamic_request(case, carried(case, ARGS))["approval"]
    refused_with(legacy(case.client, selected), 409, "approval_ticket_requires_http_creation")
    assert approval_rows(case.scope["agent"]) == []

    address = "C0EXAMPLEPLAIN"
    plain = case.client.post(
        "/agents",
        json={"name": "legacy-plain-" + uuid.uuid4().hex[:8],
              "channel": {"kind": "slack", "address": address}},
        headers=api_headers(),
    )
    assert plain.status_code == 201, plain.text
    body = {
        "agent_id": plain.json()["id"], "conversation_id": address + ":synthetic-thread",
        "author": "U0EXAMPLE2", "summary": "Approve the synthetic legacy operation",
        "reply_kind": "slack", "reply_channel": address,
        "reply_placeholder": "1760000000.000200",
        "dedupe_key": "synthetic-legacy-" + uuid.uuid4().hex,
        "gate_kind": "permission", "granted_tool": "synthetic_read", "granted_arguments": {},
    }
    ticketed = {**body, "dedupe_key": "synthetic-legacy-" + uuid.uuid4().hex,
                "granted_arguments": {"approval_ticket": "cat1.e30.AAAA"}}
    refused_with(legacy(case.client, ticketed), 409, "approval_ticket_unregistered")
    assert rows(ticketed["dedupe_key"]) == []
    created = legacy(case.client, body)
    assert created.status_code == 201, created.text
    out = created.json()
    assert set(out) == LEGACY_KEYS
    for key in ("agent_id", "conversation_id", "author", "summary", "reply_channel",
                "reply_placeholder", "dedupe_key", "gate_kind", "granted_tool"):
        assert out[key] == body[key], key
    assert (out["status"], out["route"]) == ("pending", None)
    row = rows(body["dedupe_key"])[0]
    assert row["selection_mode"] == "ordinary"
    assert all(row[field] is None for field in SELECTION_FIELDS[1:])


def test_ordinary_http_approval_keeps_its_record_digest_and_seal(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-4: snapshot columns must not invalidate ordinary approvals."""
    case = ticket_case
    request = request_for(case)
    created = accepted(create(case.client, request), request)
    row = rows(request["approval"]["dedupe_key"])[0]
    assert row["selection_mode"] == "ordinary"
    assert all(row[field] is None for field in SELECTION_FIELDS[1:])
    _, _, capsule = opened(case, row)
    assert capsule.approval_record_sha256 == digest(canonical({
        key: row[key] for key in RECORD_FIELDS
    }))
    assert owner_verifies(case, created["id"])


# --- answer interlock until decision 5 (plan D15) ----------------------------------


def test_dynamic_approval_cannot_be_answered_before_answer_authority_lands(
    ticket_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-4: no answer path accepts a selected set it cannot enforce.

    The approver role is migrated and the owning route declares no legacy
    approver restriction, the shape in which the receiver answer path skips the
    shared authorizer. An ordinary approval in the same configuration is
    answered by the same human as the liveness neighbor.
    """
    case = ticket_case
    enroll(case)
    ordinary = request_for(case)
    accepted(create(case.client, ordinary), ordinary)
    answered = SimpleNamespace(
        client=case.client, scope=case.scope, row=rows(ordinary["approval"]["dedupe_key"])[0],
    )
    approved = resolve(answered, *answer(answered))
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "approved"

    second = ingress(case)
    request = dynamic_request(second, carried(case, ARGS))
    accepted(create(case.client, request), request)
    row = rows(request["approval"]["dedupe_key"])[0]
    target = SimpleNamespace(client=case.client, scope=case.scope, row=row)
    for decision in ("approved", "rejected"):
        body = json.dumps({"decision": decision}, separators=(",", ":")).encode()
        response = resolve(target, *answer(target, body=body))
        assert response.status_code == 403, response.text
        operator = case.client.post(
            f"/approvals/{row['id']}/resolve", json={"decision": decision},
            headers=operator_headers(),
        )
        assert operator.status_code == 403, operator.text
    stored = rows(request["approval"]["dedupe_key"])[0]
    assert (stored["status"], stored["resolved_by"]) == ("pending", None)
    assert [entry for entry in approval_audits(row["id"]) if entry["action"] == "resolved"] == []
