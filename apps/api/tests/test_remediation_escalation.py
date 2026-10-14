"""Not verified: report, escalate, never undo automatically (plan task 12).

@spec AUTOMATED-REMEDIATION-19

docs/superpowers/specs/2026-10-07-automated-remediation.md, AUTOMATED-REMEDIATION-19:

    Every outcome other than ``verified`` posts a failure report to the
    policy's route and the delivery's thread, opens the breaker
    (AUTOMATED-REMEDIATION-11) and, for a ``reversible`` action whose record is
    undoable, offers undo as an approval-gated decision: the API creates an undo
    approval request on the policy's route bound to the restore of that record,
    and an approval drives the existing undo ruling path (AE-3) under the
    approving principal. No code path calls the undo ruling for a
    policy-executed record without an approving principal (REMEDIATION-14).
    ``_authorize_undo`` gains authority awareness: a record with
    ``authority_kind`` ``policy`` requires a principal in the policy route's
    approver set, and a record with ``authority_kind`` ``approval`` requires one
    in the approval route's set (through ``gate_approval_id``), replacing AE-19's
    interim ``refused_authority_unresolved``.

Surface these tests fix (see ``.projects/plans/task-remediation-escalation.tests.md``):

* the escalation record, written in the transaction that writes a
  non-``verified`` outcome: ``curie.remediation_escalations`` with one row per
  nomination (``nomination_id``, ``action_id`` (the ledger record, null when
  the forward left none), ``outcome``, ``undo_approval_id`` (null when no undo
  is offered)). It is what the worker remediation loop delivers as the failure
  report to the policy's route and the delivery's thread. No row for
  ``verified``;
* the undo approval: one ``curie.approvals`` row, not ``purpose`` ``session``
  (it owes no model wake), on the policy's route, ``granted_tool``
  ``mcp__<connector>__restore``, an explicit expiry, and the reply surface of
  the protected delivery (``remediation_delivery_surfaces``);
* ``POST /approvals/{id}/resolve`` approving it as a member of the route's
  approver set creates exactly one ``restore`` execution through the undo
  ruling, whose ``requested_by``, ``authorized`` audit row ``actor`` and
  ``actor_kind`` ``undo_ruling`` name the approving principal; an outsider is
  refused ``403``; a rejection creates nothing;
* ``POST /actions/{id}/undo`` on a ``policy`` or ``approval`` record: a member
  of the route's approver set is granted (``202``), anyone else is refused
  ``403`` ``refused_unauthorized``, never ``409``
  ``refused_authority_unresolved``;
* structurally, every creation of a ``restore`` execution lives in a function
  that requires a ``principal`` and records it, and every call of such a
  function passes one.

Every identifier is a placeholder.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    ENVELOPE,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    worker_headers,
)
from aci_protocol.turn import QueuedTurn, ReplyHandle, TurnSource
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_remediation_verifier import (
    ACT_CONNECTOR,
    ACT_DIGEST,
    EVENT_ID,
    HOOK,
    OBSERVE_TOOL,
    OBSERVED_TARGET,
    OFFSETS,
    OPERATOR,
    POST_VERSION,
    READ_TOOL,
    SAMPLED,
    UNHEALTHY,
    USERS_ROUTE,
    _agent,
    _bind_policy,
    _claim,
    _document,
    _end,
    _execution,
    _fence,
    _make_next_due,
    _nominate,
    _step,
)

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled", "runs_stream")

OUTSIDER = "U0OUTSIDE9"
ALERT_CHANNEL = "C0EXAMPLE8"
RESTORE_TOOL = f"mcp__{ACT_CONNECTOR}__restore"
REPLAY_VERSION = "rv-2002"

NOT_VERIFIED = ("not-recovered", "verifier-unavailable", "superseded")

_REPO = Path(__file__).resolve().parents[3]


# --------------------------------------------------------------------------- #
# Helpers: a reversible remediation whose record is undoable
# --------------------------------------------------------------------------- #


def _reversible() -> dict[str, Any]:
    document = _document()
    document["actions"][0]["reversibility"] = "reversible"
    return document


def _surface(agent_id: str, event_id: str = EVENT_ID) -> None:
    """The reply surface the protected delivery recorded (task 9, review round 1)."""

    sql_rows(
        "INSERT INTO curie.remediation_delivery_surfaces "
        "(event_id, agent_id, hook, reply_kind, reply_channel) "
        "VALUES (:e, :agent_id, :hook, 'slack', :channel) ON CONFLICT DO NOTHING",
        {"e": event_id, "agent_id": uuid.UUID(agent_id), "hook": HOOK, "channel": ALERT_CHANNEL},
    )


def _complete(client: Any, headers: dict[str, str], action_id: Any) -> None:
    """The worker's completion of the forward's ledger record, every undo ingredient."""

    completed = client.post(
        f"/actions/{action_id}/complete",
        json={
            "failed": False,
            "result": {"ok": True},
            "prior_state": ENVELOPE,
            "post_state": {"spec": {"replicas": 4}},
            "post_version": POST_VERSION,
            "target": OBSERVED_TARGET,
            "connector": ACT_CONNECTOR,
            "connector_digest": ACT_DIGEST,
        },
        headers={**headers, **worker_headers()},
    )
    assert completed.status_code == 200, completed.text


def _claim_dispatch(client: Any, execution_id: Any) -> dict[str, Any]:
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(execution_id)
    fence = _fence(claimed.json())
    dispatched = client.post(
        f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
    )
    assert dispatched.status_code == 200, dispatched.text
    return fence


def _run(factory: Any) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await factory(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _policy_forward(client: Any, headers: dict[str, str], tmp_path: Path, document: Any) -> Any:
    """(agent id, nomination id, forward id, fence): a policy forward, dispatched."""

    agent_id = _agent(client, headers, tmp_path, observe=True)
    _bind_policy(agent_id, document)
    _surface(agent_id)
    nomination_id = _nominate(agent_id)
    forward = __import__("curie_api.remediation_forward", fromlist=["_"])
    created = _run(lambda session: forward.create_remediation_forward(session, nomination_id))
    fence = _claim_dispatch(client, created.execution_id)
    return agent_id, nomination_id, created.execution_id, fence


def _undoable_scenario(
    client: Any, headers: dict[str, str], tmp_path: Path, *, reversible: bool = True
) -> tuple[str, uuid.UUID, uuid.UUID, uuid.UUID]:
    """(agent id, nomination id, forward id, ledger id): a policy remediation of a
    ``reversible`` action whose record holds every undo ingredient, confirmed and
    verifying.
    """

    document = _reversible() if reversible else _document()
    agent_id, nomination_id, forward_id, fence = _policy_forward(
        client, headers, tmp_path, document
    )
    action_id = _execution(forward_id)["subject_action_id"]
    _complete(client, headers, action_id)
    ended = _end(client, forward_id, fence)
    assert ended.status_code == 200, ended.text
    return agent_id, nomination_id, forward_id, action_id


def _refuse_next_sample(client: Any, nomination_id: uuid.UUID) -> None:
    """The next sample refused at its claim; an observe-only beside it answered unchanged."""

    _make_next_due(nomination_id)
    for _ in range(2):
        claimed = _claim(client)
        assert claimed.status_code == 200, claimed.text
        body = claimed.json()
        if _execution(body["id"])["tool"] == OBSERVE_TOOL:
            observed = client.post(
                f"/action-executions/{body['id']}/observation",
                json={**_fence(body), "version": POST_VERSION},
                headers=worker_headers(),
            )
            assert observed.status_code == 200, observed.text
            continue
        assert _execution(body["id"])["tool"] == READ_TOOL
        refused = client.post(
            f"/action-executions/{body['id']}/outcome",
            json={**_fence(body), "state": "refused", "code": "sandbox_unavailable"},
            headers=worker_headers(),
        )
        assert refused.status_code == 200, refused.text
        return
    raise AssertionError("no sample was due beside the observe-only execution")


def _drive(client: Any, nomination_id: uuid.UUID, outcome: str) -> None:
    """Drive the verification to ``outcome`` through its real producers."""

    if outcome == "not-recovered":
        for _ in OFFSETS:
            _step(client, nomination_id, UNHEALTHY, POST_VERSION)
    elif outcome == "verifier-unavailable":
        _refuse_next_sample(client, nomination_id)
    elif outcome == "superseded":
        _step(client, nomination_id, SAMPLED, REPLAY_VERSION)
    elif outcome == "verified":
        for _ in range(3):
            _step(client, nomination_id, SAMPLED, POST_VERSION)
    else:  # pragma: no cover - a test parameter typo
        raise AssertionError(outcome)
    rows = sql_dicts(
        "SELECT verification_outcome FROM curie.remediation_nominations WHERE id = :id",
        {"id": nomination_id},
    )
    assert rows == [{"verification_outcome": outcome}]


def _escalations(nomination_id: uuid.UUID | None = None) -> list[dict[str, Any]]:
    if nomination_id is None:
        return sql_dicts("SELECT * FROM curie.remediation_escalations")
    return sql_dicts(
        "SELECT * FROM curie.remediation_escalations WHERE nomination_id = :id",
        {"id": nomination_id},
    )


def _one_escalation(nomination_id: uuid.UUID) -> dict[str, Any]:
    rows = _escalations(nomination_id)
    assert len(rows) == 1, rows
    return rows[0]


def _approvals() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.approvals ORDER BY created_at, id")


def _approval(approval_id: Any) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.approvals WHERE id = :id", {"id": approval_id})
    assert len(rows) == 1
    return rows[0]


def _restores(action_id: Any = None) -> list[dict[str, Any]]:
    if action_id is None:
        return sql_dicts("SELECT * FROM curie.action_executions WHERE kind = 'restore'")
    return sql_dicts(
        "SELECT * FROM curie.action_executions WHERE kind = 'restore' AND subject_action_id = :id",
        {"id": action_id},
    )


def _ruling_rows(action_id: Any) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT * FROM curie.action_audit_entries WHERE action_id = :id "
        "AND actor_kind = 'undo_ruling' ORDER BY created_at",
        {"id": action_id},
    )


def _open_breakers(agent_id: str) -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT * FROM curie.remediation_breakers WHERE agent_id = :a AND closed_at IS NULL",
        {"a": uuid.UUID(agent_id)},
    )


def _resolve(client: Any, approval_id: Any, decision: str, subject: str = OPERATOR) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision},
        headers=operator_headers(subject),
    )


def _undo(client: Any, action_id: Any, subject: str) -> Any:
    return client.post(f"/actions/{action_id}/undo", json={}, headers=operator_headers(subject))


def _assert_undo_approval(approval_id: Any, agent_id: str) -> dict[str, Any]:
    approval = _approval(approval_id)
    assert approval["agent_id"] == uuid.UUID(agent_id)
    assert approval["status"] == "pending"
    assert approval["route"] == USERS_ROUTE
    assert approval["granted_tool"] == RESTORE_TOOL
    # An undo decision is resolved without a model: never a session gate.
    assert approval["purpose"] not in ("session", "publication")
    assert approval["expires_at"] is not None
    # The delivery's thread, from the reply surface the protected delivery recorded.
    assert approval["reply_kind"] == "slack"
    assert approval["reply_channel"] == ALERT_CHANNEL
    return approval


def _assert_one_restore_under(action_id: Any, subject: str) -> dict[str, Any]:
    restores = _restores(action_id)
    assert len(restores) == 1, restores
    restore = restores[0]
    assert restore["state"] == "requested"
    assert restore["authority_kind"] == "undo_ruling"
    assert restore["requested_by"] == subject
    granted = [row for row in _ruling_rows(action_id) if row["action"] == "authorized"]
    assert len(granted) == 1, granted
    assert granted[0]["actor"] == subject
    assert granted[0]["authorized"] is True
    assert granted[0]["evidence"]["execution_id"] == str(restore["id"])
    assert restore["authority_ref"] == str(granted[0]["id"])
    return restore


# --------------------------------------------------------------------------- #
# Report, breaker and undo approval on every non-verified outcome
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("outcome", NOT_VERIFIED)
def test_a_non_verified_outcome_reports_opens_the_breaker_and_raises_one_undo_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, outcome: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: "Every outcome other than ``verified`` posts
    a failure report ..., opens the breaker ... and, for a ``reversible`` action
    whose record is undoable, ... the API creates an undo approval request on the
    policy's route bound to the restore of that record." No restore execution is
    created by the outcome itself.
    """

    agent_id, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    fetched = client.get(f"/actions/{action_id}", headers=auth_headers)
    assert fetched.status_code == 200, fetched.text
    # Authority-aware undo: a policy record holding every ingredient is undoable.
    assert fetched.json()["undoable"] is True

    _drive(client, nomination_id, outcome)

    escalation = _one_escalation(nomination_id)
    assert escalation["outcome"] == outcome
    assert escalation["action_id"] == action_id
    assert escalation["undo_approval_id"] is not None
    assert [row["id"] for row in _approvals()] == [escalation["undo_approval_id"]]
    _assert_undo_approval(escalation["undo_approval_id"], agent_id)
    assert len(_open_breakers(agent_id)) == 1
    assert _restores() == []
    assert _ruling_rows(action_id) == []


def test_a_replayed_deciding_report_raises_no_second_report_or_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19 @spec AUTOMATED-REMEDIATION-18: the outcome is
    written once, so its report and its undo approval are raised once.
    """

    _, nomination_id, _, _ = _undoable_scenario(client, auth_headers, tmp_path)
    _make_next_due(nomination_id)
    claimed = _claim(client)
    assert claimed.status_code == 200, claimed.text
    body = claimed.json()
    if _execution(body["id"])["tool"] == OBSERVE_TOOL:
        decided = client.post(
            f"/action-executions/{body['id']}/observation",
            json={**_fence(body), "version": REPLAY_VERSION},
            headers=worker_headers(),
        )
        replay = lambda: client.post(  # noqa: E731
            f"/action-executions/{body['id']}/observation",
            json={**_fence(body), "version": REPLAY_VERSION},
            headers=worker_headers(),
        )
    else:
        decided = client.post(
            f"/action-executions/{body['id']}/outcome",
            json={**_fence(body), "state": "refused", "code": "sandbox_unavailable"},
            headers=worker_headers(),
        )
        replay = lambda: client.post(  # noqa: E731
            f"/action-executions/{body['id']}/outcome",
            json={**_fence(body), "state": "refused", "code": "sandbox_unavailable"},
            headers=worker_headers(),
        )
    assert decided.status_code == 200, decided.text
    first = _one_escalation(nomination_id)

    assert replay().status_code == 200

    assert _escalations(nomination_id) == [first]
    assert len(_approvals()) == 1


def test_a_verified_outcome_raises_no_report_and_no_undo_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: the control: only outcomes other than
    ``verified`` escalate. Nothing is reported, no breaker opens, no approval and
    no restore follow.
    """

    agent_id, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)

    _drive(client, nomination_id, "verified")

    assert _escalations() == []
    assert _approvals() == []
    assert _open_breakers(agent_id) == []
    assert _restores() == []
    assert _ruling_rows(action_id) == []


def test_a_non_reversible_action_is_reported_without_an_undo_offer(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: undo is offered only "for a ``reversible``
    action whose record is undoable"; an ``idempotent`` one is still reported and
    still opens the breaker.
    """

    agent_id, nomination_id, _, _ = _undoable_scenario(
        client, auth_headers, tmp_path, reversible=False
    )

    _drive(client, nomination_id, "not-recovered")

    escalation = _one_escalation(nomination_id)
    assert escalation["outcome"] == "not-recovered"
    assert escalation["undo_approval_id"] is None
    assert _approvals() == []
    assert len(_open_breakers(agent_id)) == 1
    assert _restores() == []


@pytest.mark.parametrize("state", ["failed", "indeterminate"])
def test_a_forward_that_does_not_confirm_is_reported_without_an_undo_offer(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-19 @spec AUTOMATED-REMEDIATION-18: a forward
    that ends ``failed`` or ``indeterminate`` finishes ``not-recovered`` "for
    reporting": it is reported, the breaker opens, and with no succeeded record
    there is nothing undoable to offer.
    """

    agent_id, nomination_id, forward_id, fence = _policy_forward(
        client, auth_headers, tmp_path, _reversible()
    )

    ended = _end(client, forward_id, fence, state)

    assert ended.status_code == 200, ended.text
    escalation = _one_escalation(nomination_id)
    assert escalation["outcome"] == "not-recovered"
    assert escalation["undo_approval_id"] is None
    assert _approvals() == []
    assert len(_open_breakers(agent_id)) == 1
    assert _restores() == []


# --------------------------------------------------------------------------- #
# The undo approval drives one restore under the approving principal
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("outcome", NOT_VERIFIED)
def test_approving_the_undo_creates_one_restore_under_the_approving_principal(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, outcome: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: "approving it produces one restore execution
    under the approving principal": the existing undo ruling (AE-3) writes the
    ``requested`` restore and its ``authorized`` audit row naming the approver.
    """

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, outcome)
    approval_id = _one_escalation(nomination_id)["undo_approval_id"]
    assert approval_id is not None

    response = _resolve(client, approval_id, "approved")

    assert response.status_code == 200, response.text
    assert _approval(approval_id)["status"] == "approved"
    _assert_one_restore_under(action_id, OPERATOR)


def test_a_second_approval_of_the_undo_creates_no_second_restore(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: "exactly one" restore per approval."""

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, "not-recovered")
    approval_id = _one_escalation(nomination_id)["undo_approval_id"]
    assert _resolve(client, approval_id, "approved").status_code == 200

    _resolve(client, approval_id, "approved")

    _assert_one_restore_under(action_id, OPERATOR)


def test_a_principal_outside_the_routes_approvers_cannot_approve_the_undo(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: the approval is on the policy's route, so
    the route's approver set decides who may approve it: ``403``, the approval
    stays pending and nothing is restored.
    """

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, "not-recovered")
    approval_id = _one_escalation(nomination_id)["undo_approval_id"]
    assert approval_id is not None

    response = _resolve(client, approval_id, "approved", subject=OUTSIDER)

    assert response.status_code == 403, response.text
    assert _approval(approval_id)["status"] == "pending"
    assert _restores() == []
    assert [row for row in _ruling_rows(action_id) if row["authorized"]] == []


def test_a_rejected_undo_creates_no_restore(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: undo happens only on approval."""

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, "not-recovered")
    approval_id = _one_escalation(nomination_id)["undo_approval_id"]

    response = _resolve(client, approval_id, "rejected")

    assert response.status_code == 200, response.text
    assert _approval(approval_id)["status"] == "rejected"
    assert _restores() == []
    assert [row for row in _ruling_rows(action_id) if row["authorized"]] == []


# --------------------------------------------------------------------------- #
# Authority-aware undo authorization on the ruling route
# --------------------------------------------------------------------------- #


def test_a_policy_record_is_undone_by_a_member_of_the_policy_route(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: "a record with ``authority_kind`` ``policy``
    requires a principal in the policy route's approver set", replacing the
    interim ``refused_authority_unresolved``.
    """

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, "not-recovered")

    response = _undo(client, action_id, OPERATOR)

    assert response.status_code == 202, response.text
    restore = _assert_one_restore_under(action_id, OPERATOR)
    assert response.json()["execution_id"] == str(restore["id"])


def test_a_policy_record_refuses_a_principal_outside_the_policy_route(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: "an undo request for a policy record by a
    principal outside the policy route is refused ``refused_unauthorized``; an
    ungated actor can no longer undo a policy record (ADR 0117 decision 3 no
    longer applies to it)."
    """

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, "not-recovered")

    response = _undo(client, action_id, OUTSIDER)

    assert response.status_code == 403, response.text
    assert _restores() == []
    rows = _ruling_rows(action_id)
    assert [row["action"] for row in rows] == ["refused_unauthorized"]
    assert rows[0]["actor"] == OUTSIDER
    assert rows[0]["authorizer"] != "ungated"


def _turn(agent_id: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=EVENT_ID,
        conversation_id="hook-thread-1",
        author=f"hook:{HOOK}",
        text="FIRING: example error ratio",
        reply_handle=ReplyHandle(kind="slack", channel=ALERT_CHANNEL, placeholder=None),
        received_at="2026-10-07T00:00:00Z",
        source=TurnSource.WEBHOOK,
    )


def _approval_scenario(
    client: Any, headers: dict[str, str], tmp_path: Path
) -> tuple[str, uuid.UUID, uuid.UUID, uuid.UUID]:
    """(agent id, nomination id, forward approval id, ledger id): a reversible
    remediation executed under an approval (AR-15, AR-16), its record undoable,
    verification ended ``not-recovered``.
    """

    agent_id = _agent(client, headers, tmp_path, observe=True)
    _bind_policy(agent_id, _reversible())
    _surface(agent_id)
    nomination_id = _nominate(agent_id, state="received")
    approvals = __import__("curie_api.remediation_approvals", fromlist=["_"])
    requested = _run(
        lambda session: approvals.request_remediation_approval(
            session, nomination_id, turn=_turn(agent_id), check="out_of_bounds", observed=None
        )
    )
    approved = _resolve(client, requested.approval_id, "approved")
    assert approved.status_code == 200, approved.text
    forwards = sql_dicts(
        "SELECT id FROM curie.action_executions WHERE kind = 'forward' AND authority_kind = "
        "'approval' AND authority_ref = :ref",
        {"ref": str(requested.approval_id)},
    )
    assert len(forwards) == 1, forwards
    forward_id = forwards[0]["id"]
    fence = _claim_dispatch(client, forward_id)
    action_id = _execution(forward_id)["subject_action_id"]
    _complete(client, headers, action_id)
    assert _end(client, forward_id, fence).status_code == 200
    _drive(client, nomination_id, "not-recovered")
    return agent_id, nomination_id, requested.approval_id, action_id


def test_an_approval_executed_remediation_raises_its_own_undo_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: an approved action that is not verified is
    escalated too, with an undo approval distinct from the forward's approval;
    approving it restores under the approver.
    """

    agent_id, nomination_id, forward_approval, action_id = _approval_scenario(
        client, auth_headers, tmp_path
    )

    escalation = _one_escalation(nomination_id)
    undo_approval = escalation["undo_approval_id"]
    assert undo_approval is not None and undo_approval != forward_approval
    _assert_undo_approval(undo_approval, agent_id)
    assert _resolve(client, undo_approval, "approved").status_code == 200
    _assert_one_restore_under(action_id, OPERATOR)


def test_an_approval_record_is_undone_only_by_the_approval_routes_set(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-19: "a record with ``authority_kind``
    ``approval`` requires [a principal] in the approval route's set (through
    ``gate_approval_id``)": an outsider is refused ``refused_unauthorized``, a
    member is granted.
    """

    _, _, forward_approval, action_id = _approval_scenario(client, auth_headers, tmp_path)
    record = sql_dicts(
        "SELECT authority_kind, gate_approval_id FROM curie.agent_actions WHERE id = :id",
        {"id": action_id},
    )
    assert record == [{"authority_kind": "approval", "gate_approval_id": forward_approval}]

    refused = _undo(client, action_id, OUTSIDER)

    assert refused.status_code == 403, refused.text
    assert [row["action"] for row in _ruling_rows(action_id)] == ["refused_unauthorized"]
    assert _restores() == []

    granted = _undo(client, action_id, OPERATOR)

    assert granted.status_code == 202, granted.text
    _assert_one_restore_under(action_id, OPERATOR)


# --------------------------------------------------------------------------- #
# Structural: no code path calls the undo ruling without a principal
# --------------------------------------------------------------------------- #

_SOURCE_ROOTS = ("apps/api/src", "apps/worker/src")


def _sources() -> list[tuple[Path, ast.Module]]:
    found: list[tuple[Path, ast.Module]] = []
    for root in _SOURCE_ROOTS:
        for path in sorted((_REPO / root).rglob("*.py")):
            found.append((path, ast.parse(path.read_text(encoding="utf-8"), str(path))))
    return found


def _creates_restore(call: ast.Call) -> bool:
    """An ``ActionExecution(...)`` construction whose ``kind`` names ``restore``."""

    name = call.func.attr if isinstance(call.func, ast.Attribute) else getattr(call.func, "id", "")
    if name != "ActionExecution":
        return False
    for keyword in call.keywords:
        if keyword.arg == "kind":
            return "restore" in ast.unparse(keyword.value)
    return False


def _required_params(function: ast.AsyncFunctionDef | ast.FunctionDef) -> set[str]:
    args = function.args
    positional = args.posonlyargs + args.args
    required = {a.arg for a in positional[: len(positional) - len(args.defaults)]}
    required |= {
        a.arg
        for a, default in zip(args.kwonlyargs, args.kw_defaults, strict=True)
        if default is None
    }
    return required


def _ruling_violations(modules: list[tuple[Path, ast.Module]]) -> list[str]:
    """Every way a restore could be created without the approving principal.

    1. A ``restore`` ``ActionExecution`` is constructed only inside a function
       that requires a ``principal`` and records ``requested_by=principal.subject``.
    2. Every call of such a function (by name) passes a ``principal`` that is
       not the literal ``None``.
    3. No SQL text inserts a ``restore`` execution behind the ORM's back.
    """

    violations: list[str] = []
    rulings: dict[str, ast.AsyncFunctionDef | ast.FunctionDef] = {}
    for path, module in modules:
        for function in ast.walk(module):
            if not isinstance(function, ast.AsyncFunctionDef | ast.FunctionDef):
                continue
            for node in ast.walk(function):
                if isinstance(node, ast.Call) and _creates_restore(node):
                    where = f"{path.name}:{node.lineno} in {function.name}"
                    if "principal" not in _required_params(function):
                        violations.append(f"{where}: restore created without a required principal")
                    requested = [k for k in node.keywords if k.arg == "requested_by"]
                    if not requested or ast.unparse(requested[0].value) != "principal.subject":
                        violations.append(f"{where}: restore not requested by the principal")
                    rulings[function.name] = function
        for node in ast.walk(module):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                text = " ".join(node.value.lower().split())
                if "insert into" in text and "action_executions" in text and "restore" in text:
                    violations.append(f"{path.name}:{node.lineno}: SQL inserts a restore")
    for path, module in modules:
        for node in ast.walk(module):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name not in rulings:
                continue
            function = rulings[name]
            params = [a.arg for a in function.args.posonlyargs + function.args.args]
            passed: ast.expr | None = None
            for keyword in node.keywords:
                if keyword.arg == "principal":
                    passed = keyword.value
            if passed is None and "principal" in params:
                index = params.index("principal")
                if isinstance(func, ast.Attribute) and params and params[0] in ("self", "cls"):
                    index -= 1
                if index < len(node.args):
                    passed = node.args[index]
            where = f"{path.name}:{node.lineno} calls {name}"
            if passed is None:
                violations.append(f"{where} without a principal")
            elif isinstance(passed, ast.Constant) and passed.value is None:
                violations.append(f"{where} with principal=None")
    return violations


def test_no_code_path_creates_a_restore_without_an_approving_principal() -> None:
    """@spec AUTOMATED-REMEDIATION-19: "No code path calls the undo ruling for a
    policy-executed record without an approving principal (REMEDIATION-14)."
    Every restore is created by a ruling that requires the principal and records
    it, every caller of that ruling passes one, and no SQL writes a restore
    around it. The approval-driven undo path must call the same ruling.
    """

    modules = _sources()
    assert any(path.name == "actions.py" for path, _ in modules)

    assert _ruling_violations(modules) == []


def test_the_worker_never_requests_an_undo() -> None:
    """@spec AUTOMATED-REMEDIATION-19: the worker holds no approving principal, so
    nothing it runs (the remediation loop included) reaches the undo route.
    """

    offenders = []
    for path in sorted((_REPO / "apps/worker/src").rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), str(path))):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if "/undo" in node.value:
                    offenders.append(f"{path.name}:{node.lineno}")
    assert offenders == []


_BAD_CALLERS = """
async def rule_undo(session, action, principal):
    return ActionExecution(kind=ExecutionKind.restore.value, requested_by=principal.subject)

async def on_outcome(session, action):
    await rule_undo(session, action, None)

async def escalate(session, action):
    await rule_undo(session, action=action, principal=None)

async def reconcile(session, action):
    await rule_undo(session, action)
"""

_BAD_RULINGS = """
async def automatic_undo(session, action):
    session.add(ActionExecution(kind="restore", requested_by="platform"))

RAW = "INSERT INTO curie.action_executions (kind) VALUES ('restore')"
"""


def test_the_structural_check_rejects_a_ruling_called_without_a_principal() -> None:
    """@spec AUTOMATED-REMEDIATION-19: the falsifiable control for the check above:
    a principal-less call, a ``None`` principal, a restore with no principal and a
    raw SQL restore are each reported.
    """

    callers = _ruling_violations([(Path("callers.py"), ast.parse(_BAD_CALLERS))])
    assert len(callers) == 3, callers
    rulings = _ruling_violations([(Path("rulings.py"), ast.parse(_BAD_RULINGS))])
    assert any("without a required principal" in v for v in rulings), rulings
    assert any("not requested by the principal" in v for v in rulings), rulings
    assert any("SQL inserts a restore" in v for v in rulings), rulings


# --------------------------------------------------------------------------- #
# Recovery: an approved undo whose restore never committed
# --------------------------------------------------------------------------- #


@contextlib.contextmanager
def _restore_insert_fault() -> Iterator[None]:
    """A real Postgres error on inserting a ``restore`` execution, removed on exit."""

    sql_rows(
        "CREATE OR REPLACE FUNCTION curie.remesc_test_fault() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected fault'; END $$"
    )
    sql_rows(
        "CREATE TRIGGER remesc_test_fault BEFORE INSERT ON curie.action_executions "
        "FOR EACH ROW WHEN (NEW.kind = 'restore') EXECUTE FUNCTION curie.remesc_test_fault()"
    )
    try:
        yield
    finally:
        sql_rows("DROP TRIGGER IF EXISTS remesc_test_fault ON curie.action_executions")
        sql_rows("DROP FUNCTION IF EXISTS curie.remesc_test_fault()")


def _sweep(runs_stream: str) -> None:
    """One pass of the API's periodic approval sweeper, as the API runs it."""

    from curie_api.resumequeue import ResumeQueue
    from curie_api.sweeper import sweep_expired_approvals
    from redis import asyncio as aioredis

    async def sweep() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.from_url(settings.valkey_dsn())
        try:
            async with sessions() as session:
                await sweep_expired_approvals(session, ResumeQueue(valkey, stream=runs_stream))
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(sweep())


def test_an_approved_undo_whose_restore_did_not_commit_is_completed_once_by_recovery(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, runs_stream: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-19 @spec AUTOMATED-REMEDIATION-16: "approving it
    produces one restore execution under the approving principal" holds across a
    failure after the approval's claim: the restore's insert fails, and what the
    platform retries (the API's sweeper passes; a still-pending approval resolved
    again by its person) ends with exactly one restore requested by the approving
    principal and one granted ruling row. Further passes add nothing.
    """

    _, nomination_id, _, action_id = _undoable_scenario(client, auth_headers, tmp_path)
    _drive(client, nomination_id, "not-recovered")
    approval_id = _one_escalation(nomination_id)["undo_approval_id"]
    assert approval_id is not None

    with _restore_insert_fault(), contextlib.suppress(Exception):
        _resolve(client, approval_id, "approved")
    assert _restores() == []
    claimed = _approval(approval_id)["status"] == "approved"

    _sweep(runs_stream)
    if _approval(approval_id)["status"] == "pending":
        assert not claimed
        assert _resolve(client, approval_id, "approved").status_code == 200
    _sweep(runs_stream)

    assert _approval(approval_id)["status"] == "approved"
    _assert_one_restore_under(action_id, OPERATOR)

    _sweep(runs_stream)
    _sweep(runs_stream)

    _assert_one_restore_under(action_id, OPERATOR)
