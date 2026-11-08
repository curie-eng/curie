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

import json
import os
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from approval_answer_support import (
    ANY_CODE,
    CONFIGURATION_VERSION,
    EQUALITY,
    EXCLUSION,
    FIXED,
    NATIVE_A,
    NATIVE_B,
    NATIVE_OUTSIDER,
    NATIVE_UNRESOLVED,
    SELECTION,
    SENDER,
    actions,
    answer_as,
    answer_headers,
    answered,
    approval_row,
    audit_rows,
    configured,
    dynamic,
    fence_stable,
    grant_check,
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
    KEY_ID,
    OUTSIDER,
    REQUESTER,
    ROUTE,
    agent_state,
    api_headers,
    pinned_role,
    selector_key,
    set_approver_role,
    store_routes,
    stored_approver_role,
    subject_entry,
    surface_entry,
)
from curie_api.config import get_settings
from curie_api.crud.approvals import RECOVERY_AUDIT_ACTION

from apps.api.tests.test_http_approval_legacy_writers import RECOVERY_ENV

CONTINUE = ("continue", "current_authority")
REFUSE = ("refuse", "caller_not_allowed")


def _snapshot_text(approval_id: Any) -> str:
    """@spec REQUEST-APPROVAL-5: the stored selection exactly as Postgres holds it."""
    found = sql_dicts(
        "SELECT selected_approvers::text AS selected FROM curie.approvals WHERE id = :id",
        {"id": approval_id},
    )
    assert len(found) == 1, found
    return str(found[0]["selected"])


# --- AC1.1, AC1.2, AC1.6: the selected set decides, also on a migrated role ---------


def test_a_selected_approver_approves_with_selection_evidence_and_one_wake(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: the selected canonical person answers; the wake is published.

    The approver role is migrated and the owning route declares no legacy
    ``approvers`` block: the shape whose early return used to skip the shared
    authorizer (plan AC1.6). The 0271a answer interlock is gone.
    """
    case = selection_case
    configured(case)
    _, role = stored_approver_role(case.scope["agent"])
    assert pinned_role(role)["legacy_state"] == "migrated"
    assert "approvers" not in agent_state(case.scope["agent"])["approval_routes"][ROUTE]
    row = dynamic(case, (APPROVER_A,))

    selection = answered(case, row, NATIVE_A)
    assert selection["mode"] == "dynamic"
    assert (selection["route"], selection["key_id"]) == (ROUTE, KEY_ID)
    assert selection[EQUALITY] is False and selection[EXCLUSION] is True
    assert actions(row["id"], "denied") == []

    # The committed answer publishes exactly one wake, which then continues.
    entry = published(case, row["id"])
    assert approval_row(row["id"])["resumed_at"] is not None
    assert turn_check(case, entry, row["id"]) == CONTINUE
    assert grant_check(case, entry, row["id"]) == CONTINUE
    assert resume_refusals(row["id"]) == []
    again = answer_as(case, row, NATIVE_A)
    assert again.status_code == 409, again.text


def test_an_unselected_approver_inside_the_ceiling_is_refused_and_audited(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: the ceiling and the current role admit B; the selection does not.

    The same migrated role without an ``approvers`` block (plan AC1.6): the
    check is not skipped for it.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    evidence = refused(case, row, NATIVE_B, code="selection_actor_not_selected")
    assert (evidence["mode"], evidence["route"]) == ("dynamic", ROUTE)
    assert APPROVER_B in str(evidence)
    no_wake(case, row["id"])
    answered(case, row, NATIVE_A)
    published(case, row["id"])


# --- AC1.3: selection is fixed at creation ------------------------------------------


def test_the_selection_stays_fixed_when_the_registration_and_role_change(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: changed selector source data never replaces selected people.

    After creation the agent's role gains an outsider and the operator
    replaces the registration (new key under the same key id, a ceiling user
    list, a re-pinned role). The stored selection is byte-identical and still
    the only set that may answer.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    before = _snapshot_text(row["id"])
    set_approver_role(case.scope, {
        "entries": CEILING_FILTER["entries"] + [subject_entry(OUTSIDER)],
    })
    configured(case, key=selector_key(3), users=[APPROVER_A, APPROVER_B, OUTSIDER])
    assert _snapshot_text(row["id"]) == before
    assert approval_row(row["id"])["selected_approvers"] == row["selected_approvers"]

    for native in (NATIVE_B, NATIVE_OUTSIDER):
        refused(case, row, native, code="selection_actor_not_selected")
    answered(case, row, NATIVE_A)
    assert _snapshot_text(row["id"]) == before


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


@pytest.mark.parametrize("revocation", list(REVOCATION_CODES))
def test_revoked_authority_refuses_the_selected_approver_until_restored(
    selection_case: SimpleNamespace, revocation: str,
) -> None:
    """@spec REQUEST-APPROVAL-5: ceiling user, key, registration, route or active status.

    Each removal refuses the selected person and leaves the row pending;
    restoring it admits the same person on the same row.
    """
    case = selection_case
    configured(case, users=BOTH)
    row = dynamic(case)
    restore = _revoke(case, revocation)
    refused(case, row, NATIVE_A, code=REVOCATION_CODES[revocation])
    no_wake(case, row["id"])
    restore()
    answered(case, row, NATIVE_A)


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


def test_raw_surface_account_entries_never_establish_a_selected_person(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: ADR 0246 raw entries match, but select nobody.

    The pinned and current role name B's account and an account the
    directory cannot resolve by raw surface-account entries. B resolves but
    is unselected; the unresolved account has no canonical person at all.
    """
    case = selection_case
    scope = case.scope
    set_approver_role(scope, {"entries": CEILING_FILTER["entries"] + [
        surface_entry(scope, NATIVE_B), surface_entry(scope, NATIVE_UNRESOLVED),
    ]})
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    refused(case, row, NATIVE_B, code="selection_actor_not_selected")
    refused(case, row, NATIVE_UNRESOLVED, code="selection_actor_not_canonical")
    answered(case, row, NATIVE_A)


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


def test_a_preserved_actor_remapped_to_another_selected_person_is_refused_at_resume(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5: a migrated raw-account entry cannot move the answer to B.

    A and B are both selected and A's account has a raw surface-account entry,
    so the association check alone would let the remapped account through.
    After A answers, the directory maps A's account to B: publication, the
    resume current check and the grant check all refuse.
    """
    case = selection_case
    scope = case.scope
    set_approver_role(scope, {
        "entries": CEILING_FILTER["entries"] + [surface_entry(scope, NATIVE_A)],
    })
    configured(case)
    row = dynamic(case, (APPROVER_A, APPROVER_B))
    answered(case, row, NATIVE_A)
    entry = published(case, row["id"])
    assert turn_check(case, entry, row["id"]) == CONTINUE

    case.provider.roster[NATIVE_A] = person(APPROVER_B)
    before = len(resume_refusals(row["id"]))
    assert turn_check(case, entry, row["id"]) == REFUSE
    one_more_resume_refusal(row["id"], before, NATIVE_A, code="selection_actor_changed")
    assert grant_check(case, entry, row["id"]) == REFUSE
    one_more_resume_refusal(row["id"], before + 1, NATIVE_A, code="selection_actor_changed")
    assert republish(case, row["id"]) == "caller_not_allowed"
    one_more_resume_refusal(row["id"], before + 2, NATIVE_A, code="selection_actor_changed")

    case.provider.roster[NATIVE_A] = person(APPROVER_A)
    assert turn_check(case, entry, row["id"]) == CONTINUE
    assert republish(case, row["id"]) is None


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
def test_an_authenticated_answer_refused_at_capture_on_a_canonical_route_is_audited(
    selection_case: SimpleNamespace, cause: str,
) -> None:
    """@spec REQUEST-APPROVAL-5: phase 1 refusals after authentication leave a ``denied`` row.

    The selected approver's answer is correctly signed. With the provider
    credential not registered (503 ``authority_unavailable``) or the agent
    paused (403 ``caller_not_allowed``) the reader capture refuses before any
    lookup: the public status is unchanged, the row stays pending and one
    ``denied`` row names the status, its detail and stage ``capture``. Once
    the cause is removed the same approver is admitted with selection evidence.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    with _capture_failure(case, cause):
        _refused_at_capture(case, row, NATIVE_A, CAPTURE_FAILURES[cause])
    no_wake(case, row["id"])
    selection = answered(case, row, NATIVE_A)
    assert (selection["mode"], selection["route"]) == ("dynamic", ROUTE), selection
    assert len(actions(row["id"], "denied")) == 1


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


@pytest.mark.parametrize("revocation", ["ceiling_user", "key_id", "principal"])
def test_revoked_authority_after_an_approved_answer_refuses_the_resume(
    selection_case: SimpleNamespace, revocation: str,
) -> None:
    """@spec REQUEST-APPROVAL-5: the approved resume rechecks the selected approver's authority.

    The publication, ``turn-authority/check`` and ``approval-grant/check``
    each refuse ``caller_not_allowed`` and each write one ``resume_refused``
    row; restoring the authority lets the resume continue.
    """
    case = selection_case
    configured(case, users=BOTH)
    row = dynamic(case)
    answered(case, row, NATIVE_A)
    entry = published(case, row["id"])
    assert turn_check(case, entry, row["id"]) == CONTINUE
    assert grant_check(case, entry, row["id"]) == CONTINUE

    restore = _revoke(case, revocation)
    code = REVOCATION_CODES[revocation]
    before = len(resume_refusals(row["id"]))
    assert republish(case, row["id"]) == "caller_not_allowed"
    one_more_resume_refusal(row["id"], before, NATIVE_A, code=code)
    assert turn_check(case, entry, row["id"]) == REFUSE
    one_more_resume_refusal(row["id"], before + 1, NATIVE_A, code=code)
    assert grant_check(case, entry, row["id"]) == REFUSE
    one_more_resume_refusal(row["id"], before + 2, NATIVE_A, code=code)
    assert approval_row(row["id"])["status"] == "approved"

    restore()
    assert turn_check(case, entry, row["id"]) == CONTINUE


# --- AC1.10: administrative recovery on a canonical route records its judgment ---------


def _recovered(
    case: SimpleNamespace, row: dict[str, Any], native: str, recovery_key: str,
) -> dict[str, Any]:
    """``native`` recovers ``row`` as rejected through ``POST /approvals/{id}/recover``.

    The platform key, the operator's recovery grant (enabled for this request
    only) and, for a selected row, ``native``'s chat attestation plus a
    receiver-signed ``recover`` proof over the exact body. Returns the
    evidence of the one recovery audit row. @spec REQUEST-APPROVAL-5/6.
    """
    body = json.dumps({
        "disposition": "rejected", "reason": "synthetic stranded approval",
        "recovery_key": recovery_key,
    }, separators=(",", ":")).encode()
    headers = {**answer_headers(case, row, native, body, operation="recover"), **api_headers()}
    os.environ[RECOVERY_ENV] = "true"
    get_settings.cache_clear()
    try:
        response = case.client.post(
            f"/approvals/{row['id']}/recover", content=body, headers=headers,
        )
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()
    assert response.status_code == 200, response.text
    assert (response.json()["status"], response.json()["actor"]) == ("rejected", native)
    stored = approval_row(row["id"])
    assert (stored["status"], stored["resolved_by"]) == ("rejected", native)
    recovered = actions(row["id"], RECOVERY_AUDIT_ACTION)
    assert len(recovered) == 1, recovered
    entry = recovered[0]
    assert (entry["actor"], entry["decision"], entry["authorized"]) == (native, "rejected", True)
    evidence = entry["evidence"]
    assert isinstance(evidence, dict) and evidence["kind"] == "administrative_recovery", evidence
    assert evidence["recovery_key"] == recovery_key, evidence
    assert isinstance(evidence["facts"], list), evidence
    assert evidence["authenticated_actor"]["kind"] == "chat", evidence
    assert actions(row["id"], "denied") == []
    return dict(evidence)


def test_a_recovery_on_a_canonical_route_records_the_selection_judgment(
    selection_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-5/6: break-glass recovery is judged as a rejection, and says so.

    The requester is selected on a route that excludes it, which still permits
    a rejection. Its recovery of its own dynamic request keeps the
    administrative recovery evidence and adds the canonical-route judgment
    under ``selection``: the requester as the actor, equality true, exclusion
    on and the current configuration version. Recovering an ordinary approval
    keeps today's evidence, with no ``selection``.
    """
    case = selection_case
    configured(case)
    row = dynamic(case, (REQUESTER, APPROVER_A))
    evidence = _recovered(case, row, SENDER, f"rk-{uuid.uuid4().hex}")
    selection = evidence.get("selection")
    assert isinstance(selection, dict) and selection["kind"] == SELECTION, evidence
    assert (selection["mode"], selection["route"]) == ("dynamic", ROUTE), selection
    assert selection["requester"]["subject"] == REQUESTER, selection
    assert selection["actor"] == selection["requester"], selection
    assert selection[EQUALITY] is True and selection[EXCLUSION] is True, selection
    assert selection[CONFIGURATION_VERSION] == case.scope["config_version"], selection
    assert selection["decision"] == "rejected", selection

    plain = ordinary(case, None)
    evidence = _recovered(case, plain, SENDER, f"rk-{uuid.uuid4().hex}")
    assert "selection" not in evidence, evidence
