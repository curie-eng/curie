"""ADR 0272 decision 1 and 2: create one DM approval for one verified recipient.

@spec APPROVER-DM-1 APPROVER-DM-2.

Every creation goes through ``POST /v1/internal/identity/approvals`` with a real
ingress capsule and a ticket signed over the exact arguments, against the
roster TLS directory of ``approval_answer_support`` (each Slack account
resolves to its own canonical person), Postgres and Valkey. The owning route's
resolution is the DM mode, written through the real operator route. Each
refusal answers its pinned status and code, stores no approval for the event
and leaves the ticket nonce unspent; each is followed by the valid neighbor on
the same agent, which must create. Synthetic, reserved example values only.
"""

from __future__ import annotations

import copy
import uuid
from types import SimpleNamespace

from approval_answer_support import (
    NATIVE_A,
    NATIVE_B,
    NATIVE_UNRESOLVED,
    person,
)
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_dm_support import (
    DM_CHANNEL,
    account,
    configured_dm,
    create,
    dm_answer,
    dm_created,
    dm_request,
    dm_routes,
    email_answer,
    expected_delivery,
    nonce_uses,
    refused_creation,
    selected,
    set_identity_status,
    unauthenticated,
)
from approver_dm_support import dm_case as dm_case
from approver_ticket_support import APPROVER_A, APPROVER_B, ROUTE, agent_state, store_routes
from curie_api.schemas.http_identity import NamespaceV1

from apps.api.tests.test_http_approval_creation import accepted, request_for, rows


def _created(case: SimpleNamespace, receiver: str = "main") -> tuple[dict, dict]:
    """The valid neighbor: approver A, one account on the route identity's receiver.

    @spec APPROVER-DM-1 APPROVER-DM-2.
    """
    scope = case.scope
    request, payload = dm_request(
        case, [selected(case, APPROVER_A, account(scope[receiver], NATIVE_A))],
    )
    body = dm_created(create(case.client, request), request, expected_delivery(scope[receiver]))
    assert nonce_uses(scope["agent"], payload["nonce"]) == 1
    return body, request


# --- liveness and the response contract ---------------------------------------------------


def test_one_selected_recipient_creates_a_pending_dm_approval_with_its_delivery(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1/2: 201, card unset, the server-verified recipient returned.

    The recipient account is looked up through the HTTP identity owner with
    purpose ``approval``; the stored row keeps ``card_channel`` null until a
    card is registered; an identical retry is historical with the same
    delivery, recomputed from the stored row.
    """
    case = dm_case
    configured_dm(case)
    before = len(case.provider.requests)
    body, request = _created(case)
    row = rows(request["approval"]["dedupe_key"])
    assert len(row) == 1, row
    stored = row[0]
    assert str(stored["id"]) == body["id"]
    assert (stored["status"], stored["card_channel"], stored["route"]) == ("pending", None, ROUTE)
    assert stored["selection_mode"] == "dynamic"
    looked_up = [
        item for item in case.provider.requests[before:]
        if item["scope"]["sender"]["native_id"] == NATIVE_A
    ]
    assert looked_up and all(item["purpose"] == "approval" for item in looked_up), looked_up

    retry = copy.deepcopy(request)
    retry["request_id"] = str(uuid.uuid4())
    again = dm_created(
        create(case.client, retry), retry, expected_delivery(case.scope["main"]), duplicate=True,
    )
    assert again["id"] == body["id"]
    assert len(rows(request["approval"]["dedupe_key"])) == 1


def test_an_ordinary_creation_response_carries_no_delivery_key(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: old strict readers never see ``delivery`` on a non-DM row.

    ``accepted`` pins the exact ordinary key set; a ``delivery`` key, even null,
    fails it. A DM approval on the same agent then carries it.
    """
    case = dm_case
    configured_dm(case)
    request = request_for(case)
    case.turn_used = True
    body = accepted(create(case.client, request), request)
    assert "delivery" not in body
    retry = copy.deepcopy(request)
    retry["request_id"] = str(uuid.uuid4())
    assert "delivery" not in accepted(create(case.client, retry), retry, duplicate=True)
    _created(case)


# --- selection and recipient refusals -----------------------------------------------------


def test_several_selected_approvers_refuse(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-1: exactly one selected person (``approver_dm_recipient_count``)."""
    case = dm_case
    configured_dm(case)
    main = case.scope["main"]
    request, payload = dm_request(case, [
        selected(case, APPROVER_A, account(main, NATIVE_A)),
        selected(case, APPROVER_B, account(main, NATIVE_B)),
    ])
    refused_creation(case, request, payload, 422, "approver_dm_recipient_count")
    _created(case)


def test_an_approver_without_recipient_accounts_refuses(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-1: no candidate account at all (``approver_dm_account_missing``)."""
    case = dm_case
    configured_dm(case)
    request, payload = dm_request(case, [selected(case, APPROVER_A)])
    refused_creation(case, request, payload, 422, "approver_dm_account_missing")
    _created(case)


def test_accounts_only_on_another_installation_or_namespace_refuse(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: none on the receiver's installation and namespace.

    An account on another registered installation and workspace, one on the
    right installation in another namespace, and one on an installation the
    tenant does not have each refuse with ``approver_dm_wrong_installation``.
    """
    case = dm_case
    configured_dm(case)
    scope = case.scope
    main, foreign = scope["main"], scope["foreign"]
    elsewhere = NamespaceV1(
        id=foreign["namespace"].id, authority="", kind="slack_workspace",
        key=foreign["namespace"].key,
    )
    for wrong in (
        account(foreign, NATIVE_A),
        account(main, NATIVE_A, namespace=elsewhere),
        account(main, NATIVE_A, installation=str(uuid.uuid4())),
    ):
        request, payload = dm_request(case, [selected(case, APPROVER_A, wrong)])
        refused_creation(case, request, payload, 422, "approver_dm_wrong_installation")
    _created(case)


def test_a_recipient_account_the_directory_does_not_prove_refuses(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: resolved, the selected person, an active human.

    The directory reports the account unresolved, resolves it to another
    canonical person, or reports the selected person inactive: each refuses
    with ``approver_dm_account_unresolved``. The ticket's claim is never
    enough on its own.
    """
    case = dm_case
    configured_dm(case)
    main = case.scope["main"]
    for native in (NATIVE_UNRESOLVED, NATIVE_B):
        request, payload = dm_request(case, [selected(case, APPROVER_A, account(main, native))])
        refused_creation(case, request, payload, 422, "approver_dm_account_unresolved")
    saved = case.provider.roster[NATIVE_A]
    case.provider.roster[NATIVE_A] = person(APPROVER_A, status="disabled")
    try:
        request, payload = dm_request(case)
        refused_creation(case, request, payload, 422, "approver_dm_account_unresolved")
    finally:
        case.provider.roster[NATIVE_A] = saved
    _created(case)


# --- route, receiver and request refusals --------------------------------------------------


def test_a_route_identity_whose_receiver_is_not_current_refuses(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: the receiver's identity row must be current at creation.

    The route names the second bot identity on the binding's installation; it
    creates. Taken out of service, the next creation refuses with 409
    ``approver_dm_receiver_missing``; back in service it creates again.
    """
    case = dm_case
    configured_dm(case, identity=case.scope["other"]["adapter"])
    _created(case, receiver="other")
    set_identity_status(case.scope["other"]["identity"], "disabled")
    try:
        request, payload = dm_request(case, [
            selected(case, APPROVER_A, account(case.scope["other"], NATIVE_A)),
        ])
        refused_creation(case, request, payload, 409, "approver_dm_receiver_missing")
    finally:
        set_identity_status(case.scope["other"]["identity"], "active")
    _created(case, receiver="other")


def test_a_stored_route_naming_an_unregistered_identity_refuses(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: stored state no operator write produces still fails closed.

    The DM route is stored naming an identity the tenant has but the API does
    not receive for: 409 ``approver_dm_receiver_missing``.
    """
    case = dm_case
    scope = case.scope
    configured_dm(case)
    routes = copy.deepcopy(agent_state(scope["agent"])["approval_routes"])
    assert "approver_ticket" in routes[ROUTE], routes
    routes[ROUTE]["resolution"] = {
        "mode": "approver_dm", "identity": scope["unregistered"]["adapter"],
    }
    store_routes(scope["agent"], routes)
    request, payload = dm_request(case, [
        selected(case, APPROVER_A, account(scope["main"], NATIVE_A)),
    ])
    refused_creation(case, request, payload, 409, "approver_dm_receiver_missing")


def test_a_dm_route_without_a_selector_registration_refuses(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-1: a DM route stored out of band with no selector.

    An ordinary request on that route refuses with 409
    ``approver_dm_requires_dynamic`` and stores nothing.
    """
    case = dm_case
    scope = case.scope
    store_routes(scope["agent"], dm_routes(scope, case.key, ticket=False))
    request = request_for(case)
    case.turn_used = True
    request["approval"].update(route=ROUTE, card_channel=None)
    refused_creation(case, request, None, 409, "approver_dm_requires_dynamic")


def test_a_dm_request_naming_a_card_channel_refuses(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: the worker never chooses the DM card's channel.

    The requester's reply channel, a DM-shaped channel and the approver's user
    id each refuse with 409 ``unsupported_approval``.
    """
    case = dm_case
    configured_dm(case)
    for channel in (case.scope["address"], DM_CHANNEL, NATIVE_A):
        request, payload = dm_request(case)
        request["approval"]["card_channel"] = channel
        refused_creation(case, request, payload, 409, "unsupported_approval")
    _created(case)


# --- an unregistered DM is never answered on its origin -----------------------------------


def test_an_unregistered_dm_approval_cannot_be_answered_on_its_origin(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: unset ``card_channel`` means every answer refuses.

    The selected approver presents a receiver-signed interaction in the
    requester's reply channel (where an ordinary routeless card lives): 401
    with no effect. An email reply forwarded for the origin binding is refused
    too. The row stays pending with its card unset and nothing is published.
    """
    case = dm_case
    configured_dm(case)
    body, _ = _created(case)
    approval_id = body["id"]
    response = dm_answer(case, approval_id, NATIVE_A, channel=case.scope["address"])
    unauthenticated(case, approval_id, response, card_channel=None)
    emailed = email_answer(case, approval_id)
    assert emailed.status_code in (401, 403, 404), emailed.text
    unauthenticated(
        case, approval_id,
        dm_answer(case, approval_id, NATIVE_A, channel=DM_CHANNEL), card_channel=None,
    )
