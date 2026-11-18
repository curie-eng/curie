"""ADR 0271 decision 5: the stored selection and current authority at answer and resume.

@spec REQUEST-APPROVAL-5.

Every approval is created through ``POST /v1/internal/identity/approvals`` with
a real ingress capsule and a signed ticket, and answered through the real
``POST /approvals/{id}/resolve`` with a receiver-signed answer proof, against
the roster TLS directory of ``approval_answer_support``, Postgres and Valkey.
Outcomes are read at the HTTP boundary, in the approval row and its audit log,
in the published resume wake and through the worker's current checks
(``turn-authority/check``, ``approval-grant/check``) and the production
approved-resume publication. Every refusal is followed by its neighbor on the
same agent (the selected person, or the same person once authority is
restored), which must be admitted, so a refusal cannot pass by refusing
everything. Synthetic, reserved example values only.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from _migration_support import sql_rows
from approval_answer_support import (
    ANY_CODE,
    FIXED,
    NATIVE_A,
    NATIVE_B,
    NATIVE_OUTSIDER,
    SENDER,
    actions,
    answer_as,
    answered,
    audit_rows,
    configured,
    dynamic,
    fence_stable,
    no_wake,
    one_more_resume_refusal,
    ordinary,
    person,
    published,
    refused,
    republish,
    resume_refusals,
    runtime_for,
    still_pending,
    store_snapshot,
    turn_check,
)
from approval_answer_support import selection_case as selection_case
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_ticket_support import (
    APPROVER_A,
    APPROVER_B,
    CEILING_FILTER,
    OUTSIDER,
    REQUESTER,
    ROUTE,
    agent_state,
    pinned_role,
    selector_key,
    set_approver_role,
    store_routes,
    subject_entry,
)

CONTINUE = ("continue", "current_authority")
REFUSE = ("refuse", "caller_not_allowed")


# --- AC1.1, AC1.2, AC1.6: the selected set decides, also on a migrated role ---------


# --- AC1.3: selection is fixed at creation ------------------------------------------


# --- AC1.4: current authority is revocable at answer ---------------------------------

REVOCATION_CODES: dict[str, Any] = {
    "ceiling_user": "selection_actor_outside_ceiling",
    "key_id": "selection_key_revoked",
    "registration": "selection_registration_invalid",
    # A missing route is refused by the route owner before selection is judged.
    "route_binding": ANY_CODE,
    # An inactive account is refused by the current approver permission.
    "principal": ANY_CODE,
}
BOTH = [APPROVER_A, APPROVER_B]


def _revoke(case: SimpleNamespace, revocation: str) -> Callable[[], Any]:
    """Withdraw one part of A's current authority; returns its restoration.

    The registration starts with ceiling users A and B. A missing route binding
    is stored state no operator write can produce while the serving bundle
    gates the route, so it is written with SQL. @spec REQUEST-APPROVAL-5.
    """
    agent = case.scope["agent"]
    if revocation == "ceiling_user":
        configured(case, users=[APPROVER_B])
        return lambda: configured(case, users=BOTH)
    if revocation == "key_id":
        configured(case, key=selector_key(2), key_id="selector-example-2", users=BOTH)
        return lambda: configured(case, users=BOTH)
    if revocation == "registration":
        configured(case, ticket=False)
        return lambda: configured(case, users=BOTH)
    if revocation == "route_binding":
        stored = agent_state(agent)["approval_routes"]
        store_routes(agent, {name: value for name, value in stored.items() if name != ROUTE})
        return lambda: store_routes(agent, stored)
    assert revocation == "principal"
    case.provider.roster[NATIVE_A] = person(APPROVER_A, status="disabled")

    def restore() -> None:
        """@spec REQUEST-APPROVAL-5."""
        case.provider.roster[NATIVE_A] = person(APPROVER_A)

    return restore


# --- AC1.5: widening admits nobody outside the stored set -----------------------------


def test_a_widened_ceiling_admits_nobody_outside_the_stored_selection(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: the operator re-pins a wider role after creation.

    The outsider is then inside the ceiling and the current role, yet not in
    the stored selection; the selected person stays answerable, and revoking
    them after the widening still refuses.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    set_approver_role(case.scope, {
        "entries": CEILING_FILTER["entries"] + [subject_entry(OUTSIDER)],
    })
    pinned = configured(case)["approval_routes"][ROUTE]["approver_ticket"]["ceiling"]
    assert subject_entry(OUTSIDER) in pinned_role(pinned["approver_role"])["filter"]["entries"]

    refused(case, row, NATIVE_OUTSIDER, code="selection_actor_not_selected")
    configured(case, users=[APPROVER_B, OUTSIDER])
    refused(case, row, NATIVE_A, code="selection_actor_outside_ceiling")
    configured(case)
    answered(case, row, NATIVE_A)


# --- AC1.7: raw surface-account grants are not canonical selection --------------------


# --- AC1.8: a malformed stored snapshot fails closed ---------------------------------


def test_a_malformed_stored_snapshot_refuses_every_answer(selection_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-5: the stored selection is parsed strictly at every answer.

    Each malformed value is written with SQL and resealed, so only the
    selection parser can refuse it; the restored snapshot is answerable.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A, APPROVER_B))
    original = row["selected_approvers"]
    selected = next(item for item in original if item["subject"] == APPROVER_A)
    malformed: dict[str, Any] = {
        "empty": [],
        "object": selected,
        "string_entry": [APPROVER_A],
        "missing_subject": [{k: v for k, v in selected.items() if k != "subject"}],
        "non_string_subject": [{**selected, "subject": 7}],
        "seventeen": [selected] + [
            {**selected, "subject": f"SyntheticSelected{index}"} for index in range(16)
        ],
    }
    for value in malformed.values():
        store_snapshot(case, row["id"], value)
        refused(case, row, NATIVE_A, code="selection_snapshot_invalid")
    store_snapshot(case, row["id"], original)
    answered(case, row, NATIVE_A)


# --- AC1.9: the preserved actor must still be the same canonical person ---------------


# --- AC1.10: every refusal on a canonical route is audited ---------------------------


def test_every_refusal_on_a_canonical_route_leaves_one_denied_audit_row(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: role removal, inactive actor, route denial, provider outage.

    Each refusal keeps its public status, leaves the row pending and adds one
    ``denied`` row naming that status, written after the identity fence is
    stable again; each cause is then removed and the same person answers.
    """
    case = selection_case
    scope = case.scope
    configured(case, approvers={"users": [NATIVE_A, NATIVE_B]})
    row = dynamic(case, (APPROVER_A, APPROVER_B))

    set_approver_role(scope, {"entries": [subject_entry(REQUESTER), subject_entry(APPROVER_B)]})
    refused(case, row, NATIVE_A, 403)
    set_approver_role(scope, CEILING_FILTER)

    case.provider.roster[NATIVE_A] = person(APPROVER_A, status="disabled")
    refused(case, row, NATIVE_A, 403)
    case.provider.roster[NATIVE_A] = person(APPROVER_A)

    configured(case, approvers={"users": [NATIVE_B]})
    refused(case, row, NATIVE_A, 403)
    configured(case, approvers={"users": [NATIVE_A, NATIVE_B]})

    case.provider.outage = True
    try:
        refused(case, row, NATIVE_A, 503)
    finally:
        case.provider.outage = False

    assert [entry["evidence"]["status"] for entry in actions(row["id"], "denied")] == [
        403, 403, 403, 503,
    ]
    no_wake(case, row["id"])
    answered(case, row, NATIVE_A)


# The phase 1 reader capture refuses after the actor authenticated: the provider
# credential cannot be resolved (503), or the agent is not active (403).
CAPTURE_FAILURES: dict[str, int] = {"credential": 503, "agent": 403}


def _agent_status(agent: str, status: str) -> None:
    """The stored agent lifecycle state (ADR 0166).

    No operator route writes it yet, so it is written with SQL.
    @spec REQUEST-APPROVAL-5.
    """
    sql_rows(
        "UPDATE curie.agents SET status = :status WHERE id = :id",
        {"status": status, "id": uuid.UUID(agent)},
    )


@contextmanager
def _capture_failure(case: SimpleNamespace, cause: str) -> Iterator[None]:
    """One cause the phase 1 capture refuses for, restored when the block ends.

    ``credential``: the serving API's HTTP identity runtime registers no
    provider credential, as when the provider secret is not mounted, so
    resolving it at capture is unavailable. ``agent``: the agent is paused,
    which the capture refuses for the original invoker. @spec REQUEST-APPROVAL-5.
    """
    state = case.client.app.state
    serving = state.http_identity_runtime
    if cause == "credential":
        state.http_identity_runtime = runtime_for(case.scope, credential=False)
    else:
        assert cause == "agent", cause
        _agent_status(case.scope["agent"], "paused")
    try:
        yield
    finally:
        if cause == "credential":
            state.http_identity_runtime = serving
        else:
            _agent_status(case.scope["agent"], "active")


def _refused_at_capture(
    case: SimpleNamespace, row: dict[str, Any], native: str, status: int,
) -> dict[str, Any]:
    """Refused before any directory lookup and audited once, at stage ``capture``.

    @spec REQUEST-APPROVAL-5.
    """
    lookups = len(case.provider.requests)
    evidence = refused(case, row, native, status)
    assert len(case.provider.requests) == lookups, case.provider.requests[lookups:]
    assert (evidence["status"], evidence["detail"], evidence["stage"]) == (
        status, FIXED[status], "capture",
    ), evidence
    assert "code" in evidence, evidence
    return evidence


@pytest.mark.parametrize("cause", list(CAPTURE_FAILURES))
def test_a_capture_refusal_is_audited_only_on_a_canonical_route(
    selection_case: SimpleNamespace, cause: str,
) -> None:
    """@spec REQUEST-APPROVAL-5/6: an ordinary route keeps its unaudited capture refusal.

    An ordinary HTTP approval on a route without exclusion is not canonical:
    the same cause refuses the requester's self-confirmation with the same
    public status and writes no audit row, as before; restored, ADR 0106
    self-confirmation is admitted. The next approval, dynamic, refused for the
    same cause is audited once, so the route alone decides.

    Each row is answered before the next turn is accepted (a new turn moves the
    binding generation and voids an earlier row's capture).
    """
    case = selection_case
    configured(case)
    status = CAPTURE_FAILURES[cause]
    plain = ordinary(case, None)
    with _capture_failure(case, cause):
        response = answer_as(case, plain, SENDER)
    assert response.status_code == status, response.text
    assert response.json() == {"detail": FIXED[status]}
    still_pending(plain["id"])
    assert audit_rows(plain["id"]) == []
    restored = answer_as(case, plain, SENDER)
    assert restored.status_code == 200, restored.text
    assert restored.json()["status"] == "approved"
    assert [entry["action"] for entry in audit_rows(plain["id"])] == ["resolved"]

    row = dynamic(case, (APPROVER_A,))
    with _capture_failure(case, cause):
        _refused_at_capture(case, row, NATIVE_A, status)
    answered(case, row, NATIVE_A)


def test_an_unverified_receiver_proof_on_a_canonical_route_is_not_audited(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: an actor that never authenticated leaves no ``denied`` row.

    A proof signed with a key the receiver registration does not hold is
    refused 401 before capture, with or without the provider credential, and
    adds no audit row. The correctly signed answer while the credential is
    unavailable is the authenticated neighbor: audited once at capture, then
    admitted once the credential is restored.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    forged = SimpleNamespace(client=case.client, scope={**case.scope, "receiver_key": b"u" * 32})
    lookups = len(case.provider.requests)
    response = answer_as(forged, row, NATIVE_A)
    assert response.status_code == 401, response.text
    with _capture_failure(case, "credential"):
        response = answer_as(forged, row, NATIVE_A)
    assert response.status_code == 401, response.text
    still_pending(row["id"])
    assert actions(row["id"], "denied") == []
    assert len(case.provider.requests) == lookups

    with _capture_failure(case, "credential"):
        _refused_at_capture(case, row, NATIVE_A, 503)
    answered(case, row, NATIVE_A)


def test_a_provider_outage_at_resume_leaves_resume_refused_rows(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: an unavailable directory refuses resume, audited, retryable.

    Both the approved-resume publication and the resume current check write
    one ``resume_refused`` row each; once the directory answers, both continue.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    answered(case, row, NATIVE_A)
    entry = published(case, row["id"])
    before = len(resume_refusals(row["id"]))

    case.provider.outage = True
    try:
        assert turn_check(case, entry, row["id"]) == ("unavailable", "authority_unavailable")
        evidence = one_more_resume_refusal(row["id"], before, NATIVE_A)
        assert evidence["status"] == 503
        assert republish(case, row["id"]) == "authority_unavailable"
        evidence = one_more_resume_refusal(row["id"], before + 1, NATIVE_A)
        assert evidence["status"] == 503
    finally:
        case.provider.outage = False
    fence_stable(case.valkey)

    assert turn_check(case, entry, row["id"]) == CONTINUE
    assert republish(case, row["id"]) is None
    assert len(resume_refusals(row["id"])) == before + 2


# --- AC1.11: revocation after the answer refuses the resume --------------------------


# --- AC1.10: administrative recovery on a canonical route records its judgment ---------
