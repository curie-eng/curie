"""ADR 0272 decision 2: register the one DM card's provider-returned channel.

@spec APPROVER-DM-2.

A pending DM approval is created through the real internal creation owner
(ticket, roster directory, Postgres, Valkey). The worker then registers the DM
channel Slack returned through ``POST /v1/internal/identity/approvals/{id}/card``
with the worker token and the same identity block the creation sent. Outcomes
are read at the HTTP boundary, in the stored row, its sealed continuity context
(opened with the API's context key) and its audit log. Every refusal stores
nothing and is followed by the valid neighbor. Synthetic, reserved example
values only.
"""

from __future__ import annotations

import copy
import json
import uuid
from types import SimpleNamespace
from typing import Any

from _migration_support import sql_rows
from approval_answer_support import NATIVE_A, NATIVE_B, audit_rows, ordinary
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_dm_support import (
    CARD_EVIDENCE_KEYS,
    DM_CHANNEL,
    OTHER_DM_CHANNEL,
    SEALED_ORIGIN_FIELDS,
    SELECTION_FIELDS,
    card_audits,
    card_body,
    card_registered,
    configured_dm,
    create,
    dm_created,
    dm_routes,
    dm_row,
    expected_delivery,
    lapse,
    post_card,
    refused_exactly,
    write_dm,
)
from approver_dm_support import dm_case as dm_case
from approver_ticket_support import ingress, owner_verifies, worker_headers

from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    canonical,
    digest,
    opened,
    rows,
)
from apps.api.tests.test_http_approval_legacy_writers import sweep


def _pending(case: SimpleNamespace) -> tuple[dict[str, Any], dict[str, Any], str]:
    """A DM route and one pending DM approval; returns (row, creation request, identity).

    @spec APPROVER-DM-2.
    """
    configured_dm(case)
    row, request = dm_row(case)
    return row, request, case.scope["main"]["adapter"]


def _row(request: dict[str, Any]) -> dict[str, Any]:
    """@spec APPROVER-DM-2: the stored row for the creation request."""
    found = rows(request["approval"]["dedupe_key"])
    assert len(found) == 1, found
    return found[0]


def _record_digest(row: dict[str, Any]) -> str:
    """@spec APPROVER-DM-2: the dynamic row's record digest, computed independently."""
    return digest(canonical({key: row[key] for key in RECORD_FIELDS + SELECTION_FIELDS}))


def _unregistered(
    case: SimpleNamespace, request: dict[str, Any], audits: int = 0,
) -> None:
    """@spec APPROVER-DM-2: no channel stored, no audit added, the seal still names the row."""
    row = _row(request)
    assert row["card_channel"] is None, row
    assert len(card_audits(row["id"])) == audits
    assert owner_verifies(case, str(row["id"]))


# --- the first registration and its continuity ---------------------------------------------


def test_the_first_registration_stores_the_channel_reseals_and_audits_once(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: channel, digest, seal and one audit row commit together.

    Only ``approval_record_sha256`` changes in the sealed context; the original
    creation fingerprint, source digests and requester association are kept,
    so an identical creation retry afterwards is still historical, with the
    same delivery.
    """
    case = dm_case
    row, request, identity = _pending(case)
    _, _, before = opened(case, row)
    assert before.approval_record_sha256 == _record_digest(row)

    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)

    stored = _row(request)
    assert stored["card_channel"] == DM_CHANNEL
    assert (stored["status"], stored["resolved_by"]) == ("pending", None)
    _, _, after = opened(case, stored)
    assert after.approval_record_sha256 == _record_digest(stored)
    assert after.approval_record_sha256 != before.approval_record_sha256
    for field in SEALED_ORIGIN_FIELDS:
        assert getattr(after, field) == getattr(before, field), field
    assert after.status == "pending"
    assert owner_verifies(case, str(row["id"]))

    audits = card_audits(row["id"])
    assert len(audits) == 1, audits
    entry = audits[0]
    assert (entry["actor"], entry["authorizer"], entry["authorized"]) == (
        "system", "ApproverDmRegistration", True,
    )
    evidence = entry["evidence"]
    assert set(evidence) == CARD_EVIDENCE_KEYS, evidence
    assert evidence == {
        "kind": "approver_dm_card", "identity": identity,
        "provider_installation_id": case.scope["main"]["installation"],
        "namespace_id": case.scope["main"]["namespace"].id,
        "recipient": NATIVE_A, "channel": DM_CHANNEL,
    }

    retry = copy.deepcopy(request)
    retry["request_id"] = str(uuid.uuid4())
    again = dm_created(
        create(case.client, retry), retry, expected_delivery(case.scope["main"]), duplicate=True,
    )
    assert again["id"] == str(row["id"])
    assert len(card_audits(row["id"])) == 1


def test_an_identical_registration_retry_is_idempotent(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: 200 duplicate true, no second write or audit row."""
    case = dm_case
    row, request, identity = _pending(case)
    first = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], first), first)
    sealed = _row(request)["identity_context"]

    retry = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], retry), retry, duplicate=True)
    assert len(card_audits(row["id"])) == 1
    stored = _row(request)
    assert (stored["card_channel"], stored["identity_context"]) == (DM_CHANNEL, sealed)


def test_a_conflicting_channel_is_refused_and_the_first_stays(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: one channel per approval (409 ``approval_card_conflict``)."""
    case = dm_case
    row, request, identity = _pending(case)
    first = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], first), first)
    sealed = _row(request)["identity_context"]

    other = card_body(request, row["id"], card_identity=identity, channel=OTHER_DM_CHANNEL)
    refused_exactly(post_card(case.client, row["id"], other), 409, "approval_card_conflict")
    stored = _row(request)
    assert (stored["card_channel"], stored["identity_context"]) == (DM_CHANNEL, sealed)
    assert len(card_audits(row["id"])) == 1
    again = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], again), again, duplicate=True)


# --- binding refusals ----------------------------------------------------------------------


def test_a_registration_for_another_bot_or_recipient_is_refused(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: the route's identity and the one selected recipient.

    Another registered bot identity, an identity the route does not name, and
    another recipient (even one the directory knows) each answer 409
    ``approval_card_mismatch``.
    """
    case = dm_case
    row, request, identity = _pending(case)
    for changes in (
        {"card_identity": case.scope["other"]["adapter"]},
        {"card_identity": case.scope["foreign"]["adapter"]},
        {"card_identity": "default"},
        {"card_identity": identity, "recipient": NATIVE_B},
        {"card_identity": identity, "recipient": "U0EXAMPLEZZZ"},
    ):
        body = card_body(request, row["id"], **changes)
        refused_exactly(post_card(case.client, row["id"], body), 409, "approval_card_mismatch")
        _unregistered(case, request)
    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)


def test_a_registration_for_a_receiver_that_does_not_serve_the_origin_is_refused(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2 (review r1 F3): the card's bot must answer for the origin binding.

    Creation refuses a DM identity whose receiver does not list the turn's
    binding, so such a row only exists when the route is repointed after
    creation: a pending DM row is created on the binding's own receiver, then
    the operator repoints the owning route to ``detached`` (same installation
    and namespace, registered receiver, ``binding_ids`` without the origin).
    A first registration naming ``detached`` would store a card no click can
    answer, so it answers 409 ``approval_card_mismatch`` and writes nothing.
    Repointed back, the binding's receiver registers.
    """
    case = dm_case
    scope = case.scope
    row, request, identity = _pending(case)
    detached = scope["detached"]
    assert scope["binding"] not in detached["binding_ids"]
    configured_dm(case, identity=detached["adapter"])

    body = card_body(request, row["id"], card_identity=detached["adapter"])
    refused_exactly(post_card(case.client, row["id"], body), 409, "approval_card_mismatch")
    _unregistered(case, request)

    configured_dm(case)
    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)


def test_a_registration_with_another_creation_context_is_refused(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: bound to the original creation context.

    Another accepted turn's context and payload, a tampered payload, another
    binding generation in the scope, another dedupe key and a body naming
    another approval than its path each answer 422 ``invalid_approval_context``.
    """
    case = dm_case
    row, request, identity = _pending(case)
    later = ingress(case)
    foreign_identity = copy.deepcopy(request["identity"])
    foreign_identity.update(context=later.capsule, payload=later.payload)
    payload_only = copy.deepcopy(request["identity"])
    payload_only["payload"] = later.payload
    generation = copy.deepcopy(request["identity"])
    generation["scope"]["binding_generation"] += 1
    tampered = copy.deepcopy(request["identity"])
    tampered["payload"] = json.dumps(
        {**json.loads(tampered["payload"]), "text": "changed"}, separators=(",", ":"),
    )
    for changes in (
        {"identity": foreign_identity},
        {"identity": payload_only},
        {"identity": tampered},
        {"identity": generation},
        {"dedupe_key": "synthetic-other-event-" + uuid.uuid4().hex},
        {"approval_id": str(uuid.uuid4())},
    ):
        body = card_body(request, row["id"], card_identity=identity)
        body.update(changes)
        refused_exactly(post_card(case.client, row["id"], body), 422, "invalid_approval_context")
        _unregistered(case, request)
    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)


def test_a_user_id_or_a_public_channel_is_not_a_dm_channel(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: the post target (a user id) is never the stored DM channel."""
    case = dm_case
    row, request, identity = _pending(case)
    for channel in (NATIVE_A, case.scope["address"], "C0EXAMPLEDM1", "D0SHORT", "d0exampledm1"):
        body = card_body(request, row["id"], card_identity=identity, channel=channel)
        response = post_card(case.client, row["id"], body)
        assert response.status_code == 422, response.text
        _unregistered(case, request)
    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)


def test_registration_requires_the_worker_token(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: model or runner arguments cannot perform this write."""
    case = dm_case
    row, request, identity = _pending(case)
    body = card_body(request, row["id"], card_identity=identity)
    for headers in ({}, {"X-Curie-Worker-Token": "not-the-worker-token"}):
        response = post_card(case.client, row["id"], body, headers=headers)
        assert response.status_code == 401, response.text
        _unregistered(case, request)
    response = post_card(case.client, row["id"], body, headers=worker_headers())
    card_registered(response, body)


# --- terminal rows and ordinary approvals --------------------------------------------------


def test_a_row_settled_before_registration_reports_its_status_without_writing(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: registration racing a terminal decision returns that status.

    The pending DM row lapses and expires through the HTTP expiry owner (an unregistered
    DM row has no other path to a terminal state: every answer refuses). The
    registration then answers 200 with the row's status, the stored null
    channel and ``duplicate: false``, and writes nothing.
    """
    case = dm_case
    row, request, identity = _pending(case)
    lapse(case, row)
    sweep(case, owner=True)
    expired = _row(request)
    assert expired["status"] == "expired", expired
    sealed = expired["identity_context"]

    body = card_body(request, row["id"], card_identity=identity)
    card_registered(
        post_card(case.client, row["id"], body), body, status="expired", channel=None,
    )
    stored = _row(request)
    assert (stored["status"], stored["card_channel"]) == ("expired", None)
    assert stored["identity_context"] == sealed
    assert card_audits(row["id"]) == []


def test_an_earlier_ordinary_approval_keeps_its_seal(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: the record digest function is unchanged for every other row.

    An ordinary HTTP approval created before the DM route exists, a DM
    approval created and registered after it: the ordinary row's seal still
    names it, over the ordinary record fields only.
    """
    case = dm_case
    plain = ordinary(case, None)
    row, request, identity = _pending(case)
    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)

    stored = rows(plain["dedupe_key"])[0]
    assert stored["status"] == "pending" and stored["card_channel"] == plain["card_channel"]
    assert stored["identity_context"] == plain["identity_context"]
    _, _, capsule = opened(case, stored)
    assert capsule.approval_record_sha256 == digest(canonical({
        key: stored[key] for key in RECORD_FIELDS
    }))
    assert owner_verifies(case, str(plain["id"]))
    assert card_audits(plain["id"]) == []


# --- R3: the stored record is verified before it is resealed --------------------------------


def test_a_tampered_pending_row_is_never_resealed(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-2: registration verifies the sealed record before any mutation.

    A digest-covered field (``summary``) of the pending DM row is changed
    directly in SQL. Registration answers 409 ``approval_provenance_conflict``
    and leaves the card channel, the sealed context and the audit log exactly
    as they were, so the tamper is not laundered into a fresh seal. With the
    field restored, the same registration succeeds.
    """
    case = dm_case
    row, request, identity = _pending(case)
    assert "summary" in RECORD_FIELDS
    sql_rows(
        "UPDATE curie.approvals SET summary = :summary WHERE id = :id",
        {"summary": "Synthetic tampered summary", "id": uuid.UUID(str(row["id"]))},
    )
    tampered = _row(request)
    audits = audit_rows(row["id"])

    body = card_body(request, row["id"], card_identity=identity)
    refused_exactly(post_card(case.client, row["id"], body), 409, "approval_provenance_conflict")
    stored = _row(request)
    assert stored["card_channel"] is None, stored
    assert stored["identity_context"] == tampered["identity_context"] == row["identity_context"]
    assert stored["summary"] == "Synthetic tampered summary"
    assert audit_rows(row["id"]) == audits
    assert card_audits(row["id"]) == []

    sql_rows(
        "UPDATE curie.approvals SET summary = :summary WHERE id = :id",
        {"summary": row["summary"], "id": uuid.UUID(str(row["id"]))},
    )
    body = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], body), body)
    assert len(card_audits(row["id"])) == 1


# --- R4: retries are judged against the committed registration -------------------------------


def test_retries_after_the_route_is_repointed_are_judged_by_the_committed_card(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-2: once registered, the current route is not consulted.

    After a successful registration the operator repoints the owning route to
    ``{"mode": "requesting_surface"}``. An identical registration retry still
    answers 200 ``duplicate: true``; a retry differing in channel, bot identity
    or recipient answers 409 ``approval_card_conflict`` (not a route mismatch).
    Nothing is written by any retry.
    """
    case = dm_case
    row, request, identity = _pending(case)
    first = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], first), first)
    sealed = _row(request)["identity_context"]

    routes = dm_routes(case.scope, case.key, resolution={"mode": "requesting_surface"})
    repointed = write_dm(case.client, case.scope, routes)
    assert repointed.status_code == 200, repointed.text

    retry = card_body(request, row["id"], card_identity=identity)
    card_registered(post_card(case.client, row["id"], retry), retry, duplicate=True)
    for changes in (
        {"card_identity": identity, "channel": OTHER_DM_CHANNEL},
        {"card_identity": case.scope["other"]["adapter"]},
        {"card_identity": identity, "recipient": NATIVE_B},
    ):
        body = card_body(request, row["id"], **changes)
        refused_exactly(post_card(case.client, row["id"], body), 409, "approval_card_conflict")
    stored = _row(request)
    assert (stored["card_channel"], stored["identity_context"]) == (DM_CHANNEL, sealed)
    assert stored["status"] == "pending"
    assert len(card_audits(row["id"])) == 1
