"""Remediation approvals: argument-bound requests resolved without a model (plan task 10).

@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-16 @spec AUTOMATED-REMEDIATION-13

AUTOMATED-REMEDIATION-15 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
a nomination that is well formed but not admitted creates one ``Approval`` with
``purpose`` ``remediation``, ``route`` the policy's route, ``granted_tool``
``mcp__<connector>__<tool>``, ``granted_arguments`` the nomination's canonical
arguments, ``dedupe_key`` ``remediation:<nomination id>``, the reply fields of
the protected delivery's ``QueuedTurn`` (the nullable reply columns null),
``author`` the policy reference, and an explicit expiry of the policy's
``approval_ttl_seconds`` (default 14400). Deduplication is on the nomination
rows: while an identical approval is pending a further identical nomination
attaches to it and raises no new card; once it is resolved the next one raises a
new approval. The card is posted by a worker loop and rendered from the
nomination row and the policy generation, never from the approval row or the
alert body, with the model's ``reason`` inert and labeled unverified.

AUTOMATED-REMEDIATION-16: resolution keeps the resolve route's authentication,
approver set and compare and set, enqueues no model wake, and on ``approved``
builds the forward execution of AUTOMATED-REMEDIATION-13 from the nomination
row after checking the approval's ``granted_tool`` and argument digest
(``arguments_mismatch``) and the current generation (``policy_changed``). At
most one execution per approval: nominations attached to it finish with its
outcome and never execute separately (task 8's note "For task 10").

Surface these tests fix (see ``.projects/plans/task-remediation-approvals.tests.md``):

* ``curie_api.remediation_approvals.request_remediation_approval(session,
  nomination_id, *, turn, check, observed=None)``: the call admission (task 9)
  makes for a nomination it does not admit. ``turn`` is the protected
  delivery's ``QueuedTurn`` (the reply fields come from it, never its text),
  ``check`` the admission code that failed and ``observed`` the precondition
  read's value when one was taken. Returns an object with ``approval_id`` and
  ``created`` (False when the nomination attached to a pending approval or the
  call is a replay). Commits.
* ``POST /approvals/{id}/resolve`` (the existing route) resolves it: approving
  creates the execution through ``create_remediation_forward``; a refusal is
  ``409`` naming the code.
* The worker card loop ``curie_worker.remediation_cards`` (see the card test).

Nominations and policy generations are inserted as the rows tasks 3 and 6
write; admission itself is task 9. Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib
import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import redis
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    CONNECTOR,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    undoable_agent,
)
from aci_protocol.turn import QueuedTurn, ReplyHandle, TurnSource
from curie_api.config import get_settings
from curie_test_support.valkey import connect_or_skip
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled", "runs_stream")

HOOK = "alerts"
GENERATION = 3
ROUTE = "sre-oncall"
CARD_CHANNEL = "C0EXAMPLE71"
ALERT_CHANNEL = "C0EXAMPLE72"
APPROVER = "U0APPROVER1"
OUTSIDER = "U0OUTSIDE1"
ACTION_NAME = "scale-out-api"
TOOL = "scale_deployment"
GRANTED_TOOL = f"mcp__{CONNECTOR}__{TOOL}"
NOMINATED: dict[str, Any] = {"namespace": "example-ns", "deployment": "example-api", "replicas": 4}
FAILED_CHECK = "precondition_not_met"
OBSERVED = "0.31"
DEFAULT_TTL = 14400
# The alert body of the protected delivery. The card must never carry it.
BODY_MARKER = "ALERT-BODY-MARKER-7f3a"
REASON = (
    "error ratio high, ping <@U0EXAMPLE9> and <!channel>, see "
    "<https://example.invalid/runbook|the runbook>, *urgent* scale now"
)

_READ: dict[str, Any] = {
    "connector": "prometheus",
    "tool": "query",
    "arguments": {"query": "sum(rate(http_requests_errors_total[5m]))"},
    "pointer": "/data/0/value/1",
}
ACTION: dict[str, Any] = {
    "name": ACTION_NAME,
    "kind": "remediate",
    "connector": CONNECTOR,
    "tool": TOOL,
    "arguments": {
        "namespace": {"type": "string", "allowed": ["example-ns"]},
        "deployment": {"type": "string", "allowed": ["example-api", "example-worker"]},
        "replicas": {"type": "integer", "minimum": 2, "maximum": 6},
    },
    "target": {"argument": "deployment", "allowed": ["example-api", "example-worker"]},
    "reversibility": "reversible",
    "precondition": {**_READ, "comparator": "gt", "value": 0.5},
    "verifier": {
        **_READ,
        "comparator": "lt",
        "value": 0.05,
        "settle_seconds": 60,
        "deadline_seconds": 600,
        "interval_seconds": 30,
        "consecutive": 2,
    },
    "automatic": False,
    "qualification": None,
}
DOCUMENT: dict[str, Any] = {
    "route": ROUTE,
    "limits": {"per_policy_per_hour": 3, "per_incident_per_target": 1},
    "actions": [ACTION],
}


def _canonical(arguments: Any) -> str:
    """The executor's canonical argument text (ACTION-EXECUTOR-7)."""

    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(arguments: Any) -> str:
    return hashlib.sha256(_canonical(arguments).encode("utf-8")).hexdigest()


def _approvals_module() -> Any:
    """The approval source, imported per test so each reports its own failure."""

    return importlib.import_module("curie_api.remediation_approvals")


# --------------------------------------------------------------------------- #
# Rows tasks 3 and 6 write, and the route the policy names.
# --------------------------------------------------------------------------- #


def _insert_generation(agent_id: str, generation: int, document: dict[str, Any]) -> None:
    sql_rows(
        "INSERT INTO curie.remediation_policy_generations "
        "(agent_id, hook, generation, operation_id, intent_sha256, document, armed, "
        "active, bound_by, created_at) "
        "VALUES (:agent_id, :hook, :generation, :operation_id, :intent, "
        "CAST(:document AS jsonb), true, true, :bound_by, now())",
        {
            "agent_id": uuid.UUID(agent_id),
            "hook": HOOK,
            "generation": generation,
            "operation_id": uuid.uuid4(),
            "intent": f"{generation:02x}" * 32,
            "document": json.dumps(document),
            "bound_by": "U0EXAMPLE7",
        },
    )


def _bind_policy(agent_id: str, document: dict[str, Any] | None = None) -> None:
    """Generations 1 to ``GENERATION`` of the agent's ``HOOK`` policy, the last current."""

    for generation in range(1, GENERATION + 1):
        _insert_generation(agent_id, generation, document or DOCUMENT)
    sql_rows(
        "INSERT INTO curie.remediation_policies "
        "(agent_id, hook, generation, operation_id, armed, active, updated_at) "
        "VALUES (:agent_id, :hook, :generation, :operation_id, true, true, now())",
        {
            "agent_id": uuid.UUID(agent_id),
            "hook": HOOK,
            "generation": GENERATION,
            "operation_id": uuid.uuid4(),
        },
    )


def _advance_policy(agent_id: str, document: dict[str, Any]) -> None:
    """Bind a new current generation, as a later policy write would."""

    _insert_generation(agent_id, GENERATION + 1, document)
    sql_rows(
        "UPDATE curie.remediation_policies SET generation = :generation, updated_at = now() "
        "WHERE agent_id = :agent_id AND hook = :hook",
        {"agent_id": uuid.UUID(agent_id), "hook": HOOK, "generation": GENERATION + 1},
    )


def _bind_route(client: Any, headers: dict[str, str], agent_id: str) -> None:
    """The policy's route, with an explicit approver set naming only ``APPROVER``."""

    response = client.patch(
        f"/agents/{agent_id}",
        json={
            "approval_routes": {
                ROUTE: {
                    "resolution": {"kind": "slack", "address": CARD_CHANNEL},
                    "approvers": {"users": [APPROVER]},
                }
            }
        },
        headers=headers,
    )
    assert response.status_code == 200, response.text


def _setup(
    client: Any,
    headers: dict[str, str],
    tmp_path: Path,
    document: dict[str, Any] | None = None,
) -> str:
    agent_id = undoable_agent(client, headers, tmp_path)
    _bind_route(client, headers, agent_id)
    _bind_policy(agent_id, document)
    return agent_id


def _event_id(agent_id: str, n: int) -> str:
    return f"hook-{agent_id}-{HOOK}-{n:016x}"


def _nominate(
    agent_id: str,
    *,
    n: int = 1,
    arguments: dict[str, Any] | None = None,
    state: str = "received",
    generation: int | None = GENERATION,
) -> uuid.UUID:
    """One well-formed, not admitted nomination of ``ACTION_NAME`` from delivery ``n``.

    ``generation`` is its admitted and current generation; None is a delivery
    admitted before any policy existed.
    """

    agent = uuid.UUID(agent_id)
    bound = NOMINATED if arguments is None else arguments
    event_id = _event_id(agent_id, n)
    sql_rows(
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, :hook, :sha) "
        "ON CONFLICT DO NOTHING",
        {"e": event_id, "agent_id": agent, "hook": HOOK, "sha": "cd" * 32},
    )
    nomination_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, admitted_generation, current_generation, action, "
        "kind, arguments, arguments_sha256, target, reason, state) "
        "VALUES (:id, :agent_id, :hook, :e, CAST(:generation AS bigint), "
        "CAST(:generation AS bigint), :action, 'remediate', "
        ":arguments, :sha, :target, :reason, :state)",
        {
            "id": nomination_id,
            "agent_id": agent,
            "hook": HOOK,
            "e": event_id,
            "generation": generation,
            "action": ACTION_NAME,
            "arguments": _canonical(bound),
            "sha": _sha(bound),
            "target": f'{CONNECTOR}:"{bound["deployment"]}"',
            "reason": REASON,
            "state": state,
        },
    )
    return nomination_id


def _turn(agent_id: str, n: int = 1) -> QueuedTurn:
    """The protected delivery's queued turn: a reply handle with no placeholder."""

    return QueuedTurn(
        event_id=_event_id(agent_id, n),
        conversation_id=f"hook-thread-{n}",
        author=f"hook:{HOOK}",
        text=f"FIRING: api error ratio {BODY_MARKER} <@U0EXAMPLE9>",
        reply_handle=ReplyHandle(kind="slack", channel=ALERT_CHANNEL, placeholder=None),
        received_at="2026-10-07T00:00:00Z",
        source=TurnSource.WEBHOOK,
    )


def _run(coro_factory: Any) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await coro_factory(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _request(
    agent_id: str,
    nomination_id: uuid.UUID,
    *,
    n: int = 1,
    check: str = FAILED_CHECK,
    observed: Any = OBSERVED,
) -> Any:
    """Raise (or attach) the nomination's approval, as admission would."""

    return _run(
        lambda session: _approvals_module().request_remediation_approval(
            session, nomination_id, turn=_turn(agent_id, n), check=check, observed=observed
        )
    )


def _approvals() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.approvals ORDER BY created_at, id")


def _approval(approval_id: Any) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.approvals WHERE id = :id", {"id": approval_id})
    assert len(rows) == 1
    return rows[0]


def _nomination(nomination_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )
    assert len(rows) == 1
    return rows[0]


def _executions() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.action_executions ORDER BY created_at, id")


def _ledger() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.agent_actions")


def _resolve(client: Any, approval_id: Any, decision: str, subject: str = APPROVER) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": decision},
        headers=operator_headers(subject),
    )


@pytest.fixture
def stream(runs_stream: str) -> Any:
    """The test's runs stream on the real Valkey, to observe that no wake is enqueued."""

    client: redis.Redis = connect_or_skip(decode_responses=True)
    yield lambda: client.xrange(runs_stream)
    client.delete(runs_stream)
    client.close()


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-15: the argument-bound request
# --------------------------------------------------------------------------- #


def test_a_not_admitted_nomination_raises_one_argument_bound_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: purpose, route, granted tool and canonical
    arguments, dedupe key, reply fields from the delivery's turn (nullable reply
    columns null), the policy reference as author, and the nomination records it.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)

    requested = _request(agent_id, nomination_id)

    assert requested.created is True
    rows = _approvals()
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == requested.approval_id
    assert row["purpose"] == "remediation"
    assert row["status"] == "pending"
    assert str(row["agent_id"]) == agent_id
    assert row["route"] == ROUTE
    assert row["granted_tool"] == GRANTED_TOOL
    assert row["granted_arguments"] == NOMINATED
    assert _sha(row["granted_arguments"]) == _nomination(nomination_id)["arguments_sha256"]
    assert row["dedupe_key"] == f"remediation:{nomination_id}"
    assert row["conversation_id"] == "hook-thread-1"
    assert row["reply_kind"] == "slack"
    assert row["reply_channel"] == ALERT_CHANNEL
    assert row["reply_placeholder"] is None
    assert row["reply_endpoint"] is None
    assert row["reply_adapter"] is None
    assert row["author"].startswith(f"policy:{agent_id}:{HOOK}:")
    assert BODY_MARKER not in row["summary"]
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "approval_requested"
    assert nomination["approval_id"] == requested.approval_id
    assert nomination["execution_id"] is None
    assert _executions() == []


def test_the_approval_through_the_read_route_is_pending_and_names_its_route(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: the request is an ordinary approval record."""

    agent_id = _setup(client, auth_headers, tmp_path)
    requested = _request(agent_id, _nominate(agent_id))

    response = client.get(f"/approvals/{requested.approval_id}", headers=auth_headers)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert response.json()["route"] == ROUTE
    assert response.json()["expires_at"] is not None


@pytest.mark.parametrize(
    ("limits", "ttl"),
    [
        pytest.param({}, DEFAULT_TTL, id="default"),
        pytest.param({"approval_ttl_seconds": 600}, 600, id="policy"),
    ],
)
def test_the_approval_expires_after_the_policys_ttl(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    limits: dict[str, Any],
    ttl: int,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: "an explicit expiry of the policy's
    ``approval_ttl_seconds`` (default 14400)"; the create path sets none by itself.
    """

    document = {**DOCUMENT, "limits": {**DOCUMENT["limits"], **limits}}
    agent_id = _setup(client, auth_headers, tmp_path, document)
    requested = _request(agent_id, _nominate(agent_id))

    seconds = sql_dicts(
        "SELECT EXTRACT(EPOCH FROM expires_at - created_at) AS s FROM curie.approvals "
        "WHERE id = :id",
        {"id": requested.approval_id},
    )[0]["s"]
    assert seconds is not None
    assert abs(float(seconds) - ttl) <= 5


def test_a_replayed_request_creates_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: a retried admission returns the same approval."""

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    first = _request(agent_id, nomination_id)

    replay = _request(agent_id, nomination_id)

    assert replay.approval_id == first.approval_id
    assert replay.created is False
    assert len(_approvals()) == 1
    assert _nomination(nomination_id)["approval_id"] == first.approval_id


def test_an_identical_nomination_attaches_to_the_pending_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: a further identical nomination from a later
    delivery attaches to the pending approval and raises no new one.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    first_id = _nominate(agent_id, n=1)
    first = _request(agent_id, first_id, n=1)
    second_id = _nominate(agent_id, n=2)

    attached = _request(agent_id, second_id, n=2)

    assert attached.approval_id == first.approval_id
    assert attached.created is False
    assert len(_approvals()) == 1
    second = _nomination(second_id)
    assert second["approval_id"] == first.approval_id
    assert second["state"] == "approval_requested"


def test_a_nomination_with_other_arguments_raises_its_own_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: only the same agent, hook, action and
    ``arguments_sha256`` attach.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    first = _request(agent_id, _nominate(agent_id, n=1), n=1)
    other_id = _nominate(agent_id, n=2, arguments={**NOMINATED, "replicas": 5})

    other = _request(agent_id, other_id, n=2)

    assert other.created is True
    assert other.approval_id != first.approval_id
    assert _approval(other.approval_id)["granted_arguments"] == {**NOMINATED, "replicas": 5}


def test_after_a_rejection_the_next_identical_nomination_raises_a_new_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: "after that approval is rejected, a third
    identical nomination raises a new approval".
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    first = _request(agent_id, _nominate(agent_id, n=1), n=1)
    _request(agent_id, _nominate(agent_id, n=2), n=2)
    assert _resolve(client, first.approval_id, "rejected").status_code == 200

    third_id = _nominate(agent_id, n=3)
    third = _request(agent_id, third_id, n=3)

    assert third.created is True
    assert third.approval_id != first.approval_id
    assert len(_approvals()) == 2
    assert _approval(third.approval_id)["dedupe_key"] == f"remediation:{third_id}"
    assert _approval(third.approval_id)["status"] == "pending"


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-16: approval executes the bound call without a model
# --------------------------------------------------------------------------- #


def test_approving_creates_one_approval_execution_from_the_nomination_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-16 @spec AUTOMATED-REMEDIATION-13: one forward
    execution under the approval authority, its arguments hash the nomination's,
    and no resume turn on the runs stream.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    requested = _request(agent_id, nomination_id)

    response = _resolve(client, requested.approval_id, "approved")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"
    rows = _executions()
    assert len(rows) == 1
    row = rows[0]
    assert row["kind"] == "forward"
    assert row["state"] == "requested"
    assert row["connector"] == CONNECTOR
    assert row["tool"] == TOOL
    assert row["forward_arguments"] == NOMINATED
    assert row["arguments_sha256"] == _nomination(nomination_id)["arguments_sha256"]
    assert row["authority_kind"] == "approval"
    assert row["authority_ref"] == str(requested.approval_id)
    assert row["idempotency_key"] == (
        f"remediation:{nomination_id}:approval:{requested.approval_id}"
    )
    nomination = _nomination(nomination_id)
    assert nomination["state"] == "approved"
    assert nomination["execution_id"] == row["id"]
    assert nomination["decided_at"] is not None
    assert stream() == []
    assert _ledger() == []


def test_a_second_resolution_creates_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: the compare and set is kept; one execution."""

    agent_id = _setup(client, auth_headers, tmp_path)
    requested = _request(agent_id, _nominate(agent_id))
    assert _resolve(client, requested.approval_id, "approved").status_code == 200

    again = _resolve(client, requested.approval_id, "approved")

    assert again.status_code == 409, again.text
    assert len(_executions()) == 1
    assert stream() == []


def test_attached_nominations_sharing_one_approval_yield_one_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-16: attached
    nominations finish with the approval's outcome "without ever executing
    separately"; the approval authority yields at most one execution (task 8,
    "For task 10"), and an attached nomination can never be executed under it.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    raising_id = _nominate(agent_id, n=1)
    requested = _request(agent_id, raising_id, n=1)
    attached_ids = [_nominate(agent_id, n=n) for n in (2, 3)]
    for n, attached_id in zip((2, 3), attached_ids, strict=True):
        _request(agent_id, attached_id, n=n)

    assert _resolve(client, requested.approval_id, "approved").status_code == 200

    rows = _executions()
    assert len(rows) == 1
    assert _nomination(raising_id)["execution_id"] == rows[0]["id"]
    for attached_id in attached_ids:
        attached = _nomination(attached_id)
        assert attached["state"] == "finished"
        assert attached["approval_id"] == requested.approval_id
        assert attached["decided_at"] is not None
        assert attached["execution_id"] in (None, rows[0]["id"])

    seam = importlib.import_module("curie_api.action_forward")
    forward = importlib.import_module("curie_api.remediation_forward")
    for attached_id in attached_ids:
        with pytest.raises(seam.ForwardRefused):
            _run(
                lambda session, nid=attached_id: forward.create_remediation_forward(
                    session, nid, approval_id=requested.approval_id
                )
            )
    assert len(_executions()) == 1


def test_resume_reconciliation_owes_no_wake_for_a_remediation_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any, runs_stream: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: "Resume reconciliation excludes this purpose
    as it excludes ``publication``".
    """

    from curie_api.resumequeue import ResumeQueue
    from curie_api.resumereconciler import ResumeReconciler
    from redis import asyncio as aioredis

    agent_id = _setup(client, auth_headers, tmp_path)
    requested = _request(agent_id, _nominate(agent_id))
    assert _resolve(client, requested.approval_id, "approved").status_code == 200

    async def reconcile() -> int:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.from_url(settings.valkey_dsn())
        try:
            reconciler = ResumeReconciler(
                sessions,
                ResumeQueue(valkey, stream=runs_stream),
                interval_seconds=30,
                grace_seconds=0,
                batch_limit=100,
            )
            return await reconciler.reconcile_once()
        finally:
            await valkey.aclose()
            await engine.dispose()

    assert asyncio.run(reconcile()) == 0
    assert stream() == []


def test_a_principal_outside_the_routes_approvers_is_refused_and_nothing_executes(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: approver set selection is kept: ``403``."""

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    requested = _request(agent_id, nomination_id)

    response = _resolve(client, requested.approval_id, "approved", subject=OUTSIDER)

    assert response.status_code == 403, response.text
    assert _approval(requested.approval_id)["status"] == "pending"
    assert _nomination(nomination_id)["state"] == "approval_requested"
    assert _executions() == []


@pytest.mark.parametrize(
    "tamper",
    [
        pytest.param(
            "UPDATE curie.approvals SET granted_arguments = CAST(:value AS jsonb) WHERE id = :id",
            id="arguments",
        ),
        pytest.param(
            "UPDATE curie.approvals SET granted_tool = 'mcp__k8s__delete_deployment' "
            "WHERE id = :id AND CAST(:value AS text) IS NOT NULL",
            id="tool",
        ),
    ],
)
def test_an_approval_edited_after_the_card_is_refused_arguments_mismatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, tamper: str, stream: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: "editing the approval row's
    ``granted_arguments`` in the database after the card is posted and then
    approving is refused ``arguments_mismatch`` with no execution"; a different
    ``granted_tool`` likewise.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    requested = _request(agent_id, nomination_id)
    sql_rows(
        tamper,
        {"id": requested.approval_id, "value": json.dumps({**NOMINATED, "replicas": 6})},
    )

    response = _resolve(client, requested.approval_id, "approved")

    assert response.status_code == 409, response.text
    assert "arguments_mismatch" in response.text
    assert _executions() == []
    assert _nomination(nomination_id)["execution_id"] is None
    assert stream() == []


@pytest.mark.parametrize(
    "changed",
    [
        pytest.param({**DOCUMENT, "actions": [{**ACTION, "name": "restart-api"}]}, id="withdrawn"),
        pytest.param({**DOCUMENT, "actions": [{**ACTION, "tool": "patch_deployment"}]}, id="tool"),
        pytest.param(
            {**DOCUMENT, "actions": [{**ACTION, "connector": "k8s-other"}]}, id="connector"
        ),
    ],
)
def test_approving_after_the_action_changed_is_refused_policy_changed(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    changed: dict[str, Any],
    stream: Any,
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: "refuses ``policy_changed`` when the current
    policy generation no longer has the action with the same connector and tool".
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    requested = _request(agent_id, nomination_id)
    _advance_policy(agent_id, changed)

    response = _resolve(client, requested.approval_id, "approved")

    assert response.status_code == 409, response.text
    assert "policy_changed" in response.text
    assert _executions() == []
    assert _nomination(nomination_id)["execution_id"] is None
    assert stream() == []


def test_rejecting_creates_nothing_and_finishes_every_attached_nomination(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: "On ``rejected`` ... it creates nothing and
    finishes the nomination and any nominations attached to it"; no model wake.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    raising_id = _nominate(agent_id, n=1)
    requested = _request(agent_id, raising_id, n=1)
    attached_id = _nominate(agent_id, n=2)
    _request(agent_id, attached_id, n=2)

    response = _resolve(client, requested.approval_id, "rejected")

    assert response.status_code == 200, response.text
    assert _executions() == []
    raising = _nomination(raising_id)
    assert raising["state"] == "rejected"
    assert raising["decided_at"] is not None
    attached = _nomination(attached_id)
    assert attached["state"] in ("rejected", "finished")
    assert attached["decided_at"] is not None
    assert stream() == []


def test_a_resolve_after_expiry_creates_nothing_and_expires_the_nominations(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: an approval that expired yields no execution;
    the late resolve is ``410`` and wakes no model.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    raising_id = _nominate(agent_id, n=1)
    requested = _request(agent_id, raising_id, n=1)
    attached_id = _nominate(agent_id, n=2)
    _request(agent_id, attached_id, n=2)
    sql_rows(
        "UPDATE curie.approvals SET expires_at = now() - interval '1 minute' WHERE id = :id",
        {"id": requested.approval_id},
    )

    response = _resolve(client, requested.approval_id, "approved")

    assert response.status_code == 410, response.text
    assert _approval(requested.approval_id)["status"] == "expired"
    assert _executions() == []
    assert _nomination(raising_id)["state"] == "expired"
    assert _nomination(attached_id)["state"] in ("expired", "finished")
    assert stream() == []


def test_the_expiry_sweeper_expires_the_nomination_and_wakes_no_model(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any, runs_stream: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-16: "letting the approval expire yields no
    execution"; the sweeper's flip finishes the nomination and enqueues no turn.
    """

    from datetime import UTC, datetime, timedelta

    from curie_api.resumequeue import ResumeQueue
    from curie_api.sweeper import sweep_expired_approvals
    from redis import asyncio as aioredis

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    requested = _request(agent_id, nomination_id)
    later = datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=DEFAULT_TTL + 60)

    async def sweep() -> int:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.from_url(settings.valkey_dsn())
        try:
            async with sessions() as session:
                return await sweep_expired_approvals(
                    session, ResumeQueue(valkey, stream=runs_stream), now=later
                )
        finally:
            await valkey.aclose()
            await engine.dispose()

    asyncio.run(sweep())

    assert _approval(requested.approval_id)["status"] == "expired"
    assert _nomination(nomination_id)["state"] == "expired"
    assert _executions() == []
    assert stream() == []


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-15 and plan dependency M4: the card renders with no model
# --------------------------------------------------------------------------- #


class _CardMemory:
    """Stands in for ``ApprovalCardStore``: records what the loop remembers."""

    def __init__(self) -> None:
        self.remembered: list[tuple[str, dict[str, Any]]] = []

    async def remember(self, approval_id: str, **fields: Any) -> None:
        self.remembered.append((approval_id, fields))


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in _strings(item)]
    if isinstance(value, list):
        return [s for item in value for s in _strings(item)]
    return []


def _deliver_cards(times: int = 1) -> tuple[list[dict[str, Any]], _CardMemory, list[bool]]:
    """Run the worker's remediation card loop against this database with the real
    Slack renderer and a fake Slack client; returns the posts, memory and results.
    """

    cards = importlib.import_module("curie_worker.remediation_cards")
    from curie_worker.slack_sink import SlackReplyAdapter

    posts: list[dict[str, Any]] = []

    async def chat_post_message(**kwargs: Any) -> dict[str, Any]:
        posts.append(kwargs)
        return {"ok": True, "channel": kwargs["channel"], "ts": f"1700000000.{len(posts):06d}"}

    async def run() -> tuple[_CardMemory, list[bool]]:
        sink = SlackReplyAdapter("xoxb-test")
        sink._client_for(None).chat_postMessage = chat_post_message  # type: ignore[method-assign]
        memory = _CardMemory()
        engine = create_async_engine(get_settings().database_url)
        try:
            loop = cards.RemediationCardLoop(
                store=cards.PostgresRemediationCardStore(
                    engine, schema="curie", lease_owner="worker-a"
                ),
                replies=sink,
                card_store=memory,
            )
            results = [await loop.deliver_pending_card() for _ in range(times)]
        finally:
            await engine.dispose()
        return memory, results

    memory, results = asyncio.run(run())
    return posts, memory, results


def test_the_card_renders_without_a_model_turn_through_the_worker_renderer(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 (plan M4): the worker loop posts one card for a
    remediation approval created with no model turn, to the route's card channel,
    with the existing approval buttons carrying the approval id, rendered from the
    nomination row: action, target, arguments, the failed check and the observed
    value; recorded in the card store; delivered once.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    requested = _request(agent_id, _nominate(agent_id))
    approval_id = str(requested.approval_id)

    posts, memory, results = _deliver_cards(times=2)

    assert results == [True, False]
    assert len(posts) == 1
    post = posts[0]
    assert post["channel"] == CARD_CHANNEL
    text = "\n".join(_strings(post.get("blocks")) + [post["text"]])
    for expected in (ACTION_NAME, "example-api", "replicas", "4", FAILED_CHECK, OBSERVED):
        assert expected in text, expected
    buttons = [
        element
        for block in post["blocks"]
        if block.get("type") == "actions"
        for element in block["elements"]
    ]
    assert {button["value"] for button in buttons} == {approval_id}
    assert len(buttons) == 2
    assert [entry[0] for entry in memory.remembered] == [approval_id]
    assert memory.remembered[0][1]["channel"] == CARD_CHANNEL
    assert memory.remembered[0][1]["ts"] == "1700000000.000001"


def test_the_card_never_carries_the_alert_body_and_renders_the_reason_inert(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: the model's ``reason`` is escaped plain text
    (no mention, link or markup) labeled as unverified model text, and a card
    built from a delivery whose body contains a marker never contains it.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    _request(agent_id, _nominate(agent_id))

    posts, _memory, _results = _deliver_cards()

    assert len(posts) == 1
    rendered = _strings(posts[0].get("blocks")) + [posts[0]["text"]]
    joined = "\n".join(rendered)
    assert BODY_MARKER not in joined
    assert "unverified" in joined.lower()
    assert "runbook" in joined
    for live in ("<@U0EXAMPLE9>", "<!channel>", "<https://example.invalid"):
        assert live not in joined, live
    # Markup is inert when escaped or quoted as code, where Slack applies none.
    assert "*urgent*" not in re.sub(r"```.*?```|`[^`]*`", "", joined, flags=re.DOTALL)


# --------------------------------------------------------------------------- #
# Review round 1: attach racing resolution, recovery after the claim, L1, L2.
# --------------------------------------------------------------------------- #


def _sweep(runs_stream: str, now: Any = None) -> int:
    """One pass of the API's periodic approval sweeper, as the API runs it."""

    from curie_api.resumequeue import ResumeQueue
    from curie_api.sweeper import sweep_expired_approvals
    from redis import asyncio as aioredis

    async def sweep() -> int:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        valkey = aioredis.from_url(settings.valkey_dsn())
        try:
            async with sessions() as session:
                return await sweep_expired_approvals(
                    session, ResumeQueue(valkey, stream=runs_stream), now=now
                )
        finally:
            await valkey.aclose()
            await engine.dispose()

    return asyncio.run(sweep())


def _assert_ended_with(
    nomination_id: uuid.UUID, approval_id: Any, outcome: tuple[str, ...]
) -> None:
    """A nomination never stays ``approval_requested`` under a resolved approval.

    Either it is attached to ``approval_id`` and ended with its outcome, or (an
    attach that re-checked the approval under its lock) it raised or joined
    another approval that is still pending.
    """

    row = _nomination(nomination_id)
    if row["approval_id"] == approval_id:
        assert row["state"] in outcome, row["state"]
        assert row["decided_at"] is not None
    else:
        assert row["approval_id"] is not None
        assert row["state"] == "approval_requested"
        assert _approval(row["approval_id"])["status"] == "pending"


_OUTCOMES: dict[str, tuple[str, ...]] = {
    "approved": ("finished",),
    "rejected": ("rejected", "finished"),
    "expired": ("expired", "finished"),
}


def _settle(client: Any, approval_id: Any, decision: str, runs_stream: str) -> None:
    """Resolve by a person (approve or reject), or let the sweeper expire it."""

    if decision == "expired":
        sql_rows(
            "UPDATE curie.approvals SET expires_at = now() - interval '1 second' WHERE id = :id",
            {"id": approval_id},
        )
        _sweep(runs_stream)
        return
    assert _resolve(client, approval_id, decision).status_code == 200


@pytest.mark.parametrize("decision", ["approved", "rejected", "expired"])
def test_a_nomination_attaching_while_its_approval_is_resolved_ends_with_the_outcome(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    runs_stream: str,
    decision: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-16 (review M1):
    an attach that read the approval pending, interleaved with its resolution on
    real Postgres, never leaves the nomination ``approval_requested`` under the
    resolved approval: attached nominations "finish with that approval's outcome".

    The attach is held right after it found the pending approval (the injection
    point is the module's ``_pending_identical`` read); the resolution then runs
    in another thread for up to two seconds (a fix that serializes it behind the
    attach blocks there) before the attach is released.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    first = _request(agent_id, _nominate(agent_id, n=1), n=1)
    late_id = _nominate(agent_id, n=2)

    module = _approvals_module()
    original = module._pending_identical
    holding, proceed = threading.Event(), threading.Event()

    async def held(session: Any, nomination: Any) -> Any:
        found = await original(session, nomination)
        holding.set()
        await asyncio.to_thread(proceed.wait, 10)
        return found

    monkeypatch.setattr(module, "_pending_identical", held)
    outcome: dict[str, Any] = {}

    def attach() -> None:
        try:
            outcome["attach"] = _request(agent_id, late_id, n=2)
        except Exception as exc:  # noqa: BLE001 - reported by the assertions below
            outcome["attach_error"] = exc

    def settle() -> None:
        try:
            _settle(client, first.approval_id, decision, runs_stream)
        except Exception as exc:  # noqa: BLE001 - reported by the assertions below
            outcome["settle_error"] = exc

    attacher = threading.Thread(target=attach)
    attacher.start()
    assert holding.wait(10), "the attach never reached the pending approval"
    settler = threading.Thread(target=settle)
    settler.start()
    settler.join(2)
    proceed.set()
    attacher.join(20)
    settler.join(20)
    monkeypatch.setattr(module, "_pending_identical", original)

    assert "attach_error" not in outcome, outcome
    assert "settle_error" not in outcome, outcome
    _assert_ended_with(late_id, first.approval_id, _OUTCOMES[decision])
    executions = [row for row in _executions() if row["authority_ref"] == str(first.approval_id)]
    assert len(executions) == (1 if decision == "approved" else 0)


@pytest.mark.parametrize("decision", ["approved", "rejected"])
def test_concurrent_attaches_racing_a_resolution_all_end_with_the_outcome(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    runs_stream: str,
    decision: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 @spec AUTOMATED-REMEDIATION-16 (review M1),
    unpaced: identical nominations attach from several threads while the approval
    is resolved; none is left ``approval_requested`` under it, and the approval
    yields at most one execution.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    first = _request(agent_id, _nominate(agent_id, n=1), n=1)
    late_ids = [_nominate(agent_id, n=n) for n in range(2, 10)]
    start = threading.Barrier(len(late_ids) + 1)
    errors: list[BaseException] = []

    def attach(nomination_id: uuid.UUID, n: int) -> None:
        start.wait(10)
        try:
            _request(agent_id, nomination_id, n=n)
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    threads = [
        threading.Thread(target=attach, args=(nomination_id, n))
        for n, nomination_id in zip(range(2, 10), late_ids, strict=True)
    ]
    for thread in threads:
        thread.start()
    start.wait(10)
    _settle(client, first.approval_id, decision, runs_stream)
    for thread in threads:
        thread.join(30)

    assert errors == []
    for nomination_id in late_ids:
        _assert_ended_with(nomination_id, first.approval_id, _OUTCOMES[decision])
    executions = [row for row in _executions() if row["authority_ref"] == str(first.approval_id)]
    assert len(executions) == (1 if decision == "approved" else 0)


@contextlib.contextmanager
def _fault(where: str) -> Any:
    """A real Postgres error injected by trigger at one step after the claim.

    ``execution``: inserting the forward execution. ``cleanup``: moving any
    nomination off ``approval_requested``. Removed on exit.
    """

    statements = {
        "execution": (
            "CREATE TRIGGER remapr_test_fault BEFORE INSERT ON curie.action_executions "
            "FOR EACH ROW EXECUTE FUNCTION curie.remapr_test_fault()"
        ),
        "cleanup": (
            "CREATE TRIGGER remapr_test_fault BEFORE UPDATE ON curie.remediation_nominations "
            "FOR EACH ROW WHEN (OLD.state = 'approval_requested' "
            "AND NEW.state <> 'approval_requested') "
            "EXECUTE FUNCTION curie.remapr_test_fault()"
        ),
    }
    table = "action_executions" if where == "execution" else "remediation_nominations"
    sql_rows(
        "CREATE OR REPLACE FUNCTION curie.remapr_test_fault() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected fault'; END $$"
    )
    sql_rows(statements[where])
    try:
        yield
    finally:
        sql_rows(f"DROP TRIGGER IF EXISTS remapr_test_fault ON curie.{table}")
        sql_rows("DROP FUNCTION IF EXISTS curie.remapr_test_fault()")


def _settle_under_fault(client: Any, approval_id: Any, decision: str, runs_stream: str) -> None:
    """The settling step, whose post-claim work fails: an error answer or a raise."""

    with contextlib.suppress(Exception):
        _settle(client, approval_id, decision, runs_stream)


def _recover(client: Any, approval_id: Any, decision: str, runs_stream: str) -> None:
    """What the platform retries: sweeper passes; a still pending approval (a fix
    that rolls the claim back with the failed step) is resolved again by its person.
    """

    _sweep(runs_stream)
    if _approval(approval_id)["status"] == "pending":
        _settle(client, approval_id, decision, runs_stream)
    _sweep(runs_stream)
    _sweep(runs_stream)


@pytest.mark.parametrize(
    ("decision", "where"),
    [
        pytest.param("approved", "execution", id="approve-execution"),
        pytest.param("approved", "cleanup", id="approve-cleanup"),
        pytest.param("rejected", "cleanup", id="reject-cleanup"),
        pytest.param("expired", "cleanup", id="expire-cleanup"),
    ],
)
def test_a_failure_after_the_claim_is_recovered_once(
    client: Any,
    auth_headers: dict[str, str],
    tmp_path: Path,
    runs_stream: str,
    stream: Any,
    decision: str,
    where: str,
) -> None:
    """@spec AUTOMATED-REMEDIATION-16 (review M2): a database error after the
    approve, reject or expire claim commits (at the execution's creation or at
    the nominations' transition) is retried by the API's sweeper until an
    approval has exactly one execution and every nomination ended with the
    outcome; no model wake is enqueued.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    raising_id = _nominate(agent_id, n=1)
    requested = _request(agent_id, raising_id, n=1)
    attached_id = _nominate(agent_id, n=2)
    _request(agent_id, attached_id, n=2)

    with _fault(where):
        _settle_under_fault(client, requested.approval_id, decision, runs_stream)
    assert [row for row in _executions()] == []

    _recover(client, requested.approval_id, decision, runs_stream)

    assert _approval(requested.approval_id)["status"] == decision
    executions = _executions()
    raising = _nomination(raising_id)
    if decision == "approved":
        assert len(executions) == 1
        assert executions[0]["authority_kind"] == "approval"
        assert executions[0]["authority_ref"] == str(requested.approval_id)
        assert raising["state"] == "approved"
        assert raising["execution_id"] == executions[0]["id"]
    else:
        assert executions == []
        assert raising["state"] == decision
    assert raising["decided_at"] is not None
    _assert_ended_with(attached_id, requested.approval_id, _OUTCOMES[decision])
    assert stream() == []


def test_a_changed_tool_with_no_recorded_generation_is_refused_policy_changed(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, stream: Any
) -> None:
    """@spec AUTOMATED-REMEDIATION-16 (review L1): a nomination from a delivery
    admitted before any policy existed (no current generation) is raised under the
    live generation; a later generation changing the action's tool is
    ``policy_changed``, not ``arguments_mismatch``.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id, generation=None)
    requested = _request(agent_id, nomination_id)
    _advance_policy(agent_id, {**DOCUMENT, "actions": [{**ACTION, "tool": "patch_deployment"}]})

    response = _resolve(client, requested.approval_id, "approved")

    assert response.status_code == 409, response.text
    assert "policy_changed" in response.text
    assert _executions() == []
    assert stream() == []


class _Crash(BaseException):
    """The worker process dying mid-delivery: nothing in the loop catches it."""


class _FailingMemory(_CardMemory):
    def __init__(self, failure: BaseException) -> None:
        super().__init__()
        self._failure = failure

    async def remember(self, approval_id: str, **fields: Any) -> None:
        raise self._failure


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(_Crash("worker died after the post"), id="crash"),
        pytest.param(RuntimeError("card store unavailable"), id="error"),
    ],
)
def test_a_card_post_retried_after_a_failure_posts_one_card(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, failure: BaseException
) -> None:
    """@spec AUTOMATED-REMEDIATION-15 (review L2): the post succeeded, then the
    worker died (lease left to expire) or recording the card failed; the retry,
    by another worker, reaches Slack under the same idempotency key, so the
    channel holds one card and the retry adopts its ts.

    The fake Slack client honors ``client_msg_id`` as Slack does (a repeat key
    answers the first message's ts and posts nothing).
    """

    cards = importlib.import_module("curie_worker.remediation_cards")
    from curie_worker.slack_sink import SlackReplyAdapter

    agent_id = _setup(client, auth_headers, tmp_path)
    approval_id = str(_request(agent_id, _nominate(agent_id)).approval_id)
    posts: list[dict[str, Any]] = []
    messages: dict[str, str] = {}

    async def chat_post_message(**kwargs: Any) -> dict[str, Any]:
        posts.append(kwargs)
        key = kwargs.get("client_msg_id") or f"unkeyed-{len(posts)}"
        if key not in messages:
            messages[key] = f"1700000000.{len(messages) + 1:06d}"
        return {"ok": True, "channel": kwargs["channel"], "ts": messages[key]}

    async def deliver(owner: str, memory: Any) -> bool:
        sink = SlackReplyAdapter("xoxb-test")
        sink._client_for(None).chat_postMessage = chat_post_message  # type: ignore[method-assign]
        engine = create_async_engine(get_settings().database_url)
        try:
            loop = cards.RemediationCardLoop(
                store=cards.PostgresRemediationCardStore(
                    engine, schema="curie", lease_owner=owner, lease_seconds=1
                ),
                replies=sink,
                card_store=memory,
            )
            return bool(await loop.deliver_pending_card())
        finally:
            await engine.dispose()

    with pytest.raises(type(failure)):
        asyncio.run(deliver("worker-a", _FailingMemory(failure)))
    time.sleep(1.5)
    memory = _CardMemory()
    assert asyncio.run(deliver("worker-b", memory)) is True
    assert asyncio.run(deliver("worker-b", _CardMemory())) is False

    assert len(posts) == 2
    assert len(messages) == 1
    keys = {post.get("client_msg_id") for post in posts}
    assert len(keys) == 1 and None not in keys
    assert [entry[0] for entry in memory.remembered] == [approval_id]
    assert memory.remembered[0][1]["ts"] == next(iter(messages.values()))
