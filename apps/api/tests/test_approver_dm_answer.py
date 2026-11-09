"""ADR 0272 decision 3: only the selected person, in the registered DM, through its bot.

@spec APPROVER-DM-3.

A DM approval is created through the real internal creation owner and its DM
card registered through the real internal registration endpoint. Answers go
through the real ``POST /approvals/{id}/resolve`` with a chat attestation and a
receiver-signed answer proof whose interaction channel, receiving bot
registration and actor are each chosen by the test, against the roster TLS
directory, Postgres and Valkey. Outcomes are read at the HTTP boundary, in the
row and its audit log, in the published resume wake and through the worker's
``turn-authority/check``. Every refusal leaves the row pending on its card and
publishes nothing, and is followed by the selected approver's own answer in
the registered DM, which must approve. Synthetic, reserved example values only.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

from approval_answer_support import (
    NATIVE_A,
    NATIVE_B,
    NATIVE_OUTSIDER,
    OPERATOR,
    OPERATOR_REFUSED,
    actions,
    approval_row,
    no_wake,
    published,
    turn_check,
)
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_dm_support import (
    DM_CHANNEL,
    OTHER_DM_CHANNEL,
    configured_dm,
    dm_answer,
    dm_row,
    email_answer,
    no_effect,
    register_dm_card,
    unauthenticated,
)
from approver_dm_support import dm_case as dm_case

from apps.api.tests.test_http_approval_legacy_writers import operator_headers

CONTINUE = ("continue", "current_authority")


def _registered(
    case: SimpleNamespace, *, approvers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A DM route, one pending DM approval for approver A and its registered DM card.

    @spec APPROVER-DM-3.
    """
    configured_dm(case, approvers=approvers)
    row, request = dm_row(case)
    return register_dm_card(case, row, request)


def _approved(case: SimpleNamespace, row: dict[str, Any]) -> None:
    """The selected approver in the registered DM through the route's bot approves once.

    One ``resolved`` row by approver A, one resume wake, and the worker's
    approved-resume current check continues. @spec APPROVER-DM-3.
    """
    response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL)
    assert response.status_code == 200, response.text
    assert (response.json()["status"], response.json()["resolved_by"]) == ("approved", NATIVE_A)
    stored = approval_row(row["id"])
    assert (stored["status"], stored["resolved_by"]) == ("approved", NATIVE_A)
    assert stored["card_channel"] == DM_CHANNEL
    resolved = actions(row["id"], "resolved")
    assert len(resolved) == 1, resolved
    assert (resolved[0]["actor"], resolved[0]["decision"]) == (NATIVE_A, "approved")
    entry = published(case, row["id"])
    assert turn_check(case, entry, row["id"]) == CONTINUE
    again = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL)
    assert again.status_code == 409, again.text


def test_the_selected_approver_answers_in_the_registered_dm_and_the_resume_continues(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: right DM, right actor, right bot installation."""
    case = dm_case
    row = _registered(case)
    _approved(case, row)


def test_a_copied_card_in_another_channel_supplies_no_authority(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: the interaction channel must be the registered DM.

    The selected approver answering from another DM, from the requester's
    reply channel (where an ordinary card would be) or from a public channel
    is refused with no effect.
    """
    case = dm_case
    row = _registered(case)
    for channel in (OTHER_DM_CHANNEL, case.scope["address"], "C0EXAMPLECOPY"):
        response = dm_answer(case, row["id"], NATIVE_A, channel=channel)
        unauthenticated(case, row["id"], response, card_channel=DM_CHANNEL)
    _approved(case, row)


def test_another_person_in_the_same_dm_channel_is_refused(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-3: the actor must be the one selected recipient.

    An outsider, and approver B (inside the ceiling and the approver role but
    not selected), presenting the registered DM's channel id are refused with
    no effect.
    """
    case = dm_case
    row = _registered(case)
    for native in (NATIVE_OUTSIDER, NATIVE_B):
        response = dm_answer(case, row["id"], native, channel=DM_CHANNEL)
        assert response.status_code in (401, 403), response.text
        no_effect(case, row["id"], card_channel=DM_CHANNEL)
    assert actions(row["id"], "resolved") == []
    _approved(case, row)


def test_another_bot_identity_or_installation_is_refused(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-3: the receiving bot must be the route's identity.

    Both other registrations list the origin binding and every roster human, so
    only the identity (same installation) or the installation and workspace
    differ. Each is refused with no effect.
    """
    case = dm_case
    row = _registered(case)
    for receiver in ("other", "foreign"):
        response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL, receiver=receiver)
        unauthenticated(case, row["id"], response, card_channel=DM_CHANNEL)
    _approved(case, row)


def test_an_operator_principal_is_refused_even_when_the_route_lists_it(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: a CLI operator token is not proof of the selected person.

    The DM route binds ``approvers.users`` naming the operator (an
    operator-eligible set on an ordinary route). The operator is refused with
    403 and one ``denied`` row whose evidence kind is
    ``canonical_route_operator_refused`` (the authorizer's reason is the
    response detail, as ADR 0271's separation suite pins); the
    selected approver then approves in the DM.
    """
    case = dm_case
    row = _registered(case, approvers={"users": [OPERATOR, NATIVE_A]})
    response = case.client.post(
        f"/approvals/{row['id']}/resolve", json={"decision": "approved"},
        headers=operator_headers(),
    )
    assert response.status_code == 403, response.text
    denied = actions(row["id"], "denied")
    assert len(denied) == 1, denied
    assert (denied[0]["actor"], denied[0]["principal_kind"]) == (OPERATOR, "operator")
    assert denied[0]["evidence"]["kind"] == OPERATOR_REFUSED, denied[0]
    assert response.json() == {"detail": denied[0]["reason"]}, response.text
    no_effect(case, row["id"], card_channel=DM_CHANNEL)
    _approved(case, row)


def test_an_email_reply_cannot_answer_a_dm_card(dm_case: SimpleNamespace) -> None:
    """@spec APPROVER-DM-3: the DM is the resolution surface; email is refused."""
    case = dm_case
    row = _registered(case)
    response = email_answer(case, row["id"])
    assert response.status_code in (401, 403, 404), response.text
    no_effect(case, row["id"], card_channel=DM_CHANNEL)
    assert actions(row["id"], "resolved") == []
    _approved(case, row)


def test_racing_approve_and_reject_commit_exactly_one_decision(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: the first authorized decision wins the existing CAS.

    The selected approver's approve and reject in the registered DM are
    submitted concurrently. Exactly one succeeds; the row carries its
    decision, one ``resolved`` row is written and at most one wake.
    """
    case = dm_case
    row = _registered(case)

    async def race() -> list[Any]:
        """@spec APPROVER-DM-3."""
        return list(await asyncio.gather(*(
            asyncio.to_thread(
                dm_answer, case, row["id"], NATIVE_A, channel=DM_CHANNEL, decision=decision,
            )
            for decision in ("approved", "rejected")
        )))

    responses = asyncio.run(race())
    won = [response for response in responses if response.status_code == 200]
    assert len(won) == 1, [(r.status_code, r.text) for r in responses]
    lost = [response for response in responses if response.status_code != 200]
    assert lost[0].status_code in (409, 503), lost[0].text
    decision = won[0].json()["status"]
    stored = approval_row(row["id"])
    assert (stored["status"], stored["resolved_by"]) == (decision, NATIVE_A)
    resolved = actions(row["id"], "resolved")
    assert len(resolved) == 1 and resolved[0]["decision"] == decision, resolved
    if decision == "approved":
        published(case, row["id"])
    else:
        no_wake_or_one(case, row["id"])


def no_wake_or_one(case: SimpleNamespace, approval_id: Any) -> None:
    """@spec APPROVER-DM-3: a rejection publishes at most its one wake."""
    try:
        no_wake(case, approval_id)
    except AssertionError:
        published(case, approval_id)
