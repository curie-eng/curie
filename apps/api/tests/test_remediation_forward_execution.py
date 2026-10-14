"""Forward execution with an authority, and the ledger fields it records (plan task 8).

@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-14

AUTOMATED-REMEDIATION-13 (docs/superpowers/specs/2026-10-07-automated-remediation.md):
an admitted nomination, or an approved remediation approval, creates one
``forward`` execution through the ACTION-EXECUTOR-19 creation function, with
connector, tool and canonical arguments taken from the nomination row (and the
policy generation that declares its action), never from a caller;
``authority_kind`` ``policy`` with ``authority_ref``
``policy:<agent_id>:<hook>:<generation>:<nomination id>``, or ``approval`` with
the approval id; idempotency key ``remediation:<nomination id>``. At dispatch the
one ``agent_actions`` row carries those authority fields, ``gate_approval_id``
for an approval authority, and the new ``delivery_event_id`` and
``nomination_id``.

AUTOMATED-REMEDIATION-14: additive ``agent_actions`` columns
``delivery_event_id``, ``nomination_id``, ``verification_outcome``,
``verified_at`` and ``actor_kind``; ``action_audit_entries.actor_kind``; a check
constraint closing ``authority_kind``; a policy actor recorded as ``actor_kind``
``policy`` with the policy reference as ``actor``, never an empty human field;
a model turn's record keeps ``actor_kind`` ``model_turn`` and null authority.

Surface these tests fix (see ``.projects/plans/task-remediation-forward.tests.md``):

* ``curie_api.remediation_forward.create_remediation_forward(session,
  nomination_id, *, approval_id=None)``: the authority source admission (task 9)
  and the remediation approval (task 10) call. It returns the seam's
  ``ForwardCreated`` and raises the seam's ``ForwardRefused`` (no row) for a
  nomination that is not ``admitted`` (policy) or not ``approved`` under that
  approval (approval).
* Dispatch through the real worker routes writes the ledger row.
* ``GET /actions/{id}`` and ``GET /actions/{id}/audit`` expose the fields.

The policy generation and the nomination are inserted as the rows tasks 3 and 6
write (their producers are proved there); admission itself is task 9. Every
identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import io
import json
import tarfile
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    CONNECTOR,
    DIGEST,
    IMAGE,
    POST_VERSION,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    sealed_action,
    undoable_agent,
    worker_headers,
)
from curie_api.config import get_settings
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

HOOK = "alerts"
GENERATION = 3
BOUND_BY = "U0EXAMPLE7"
EVENT_ID = "event-0000000001"
ACTION_NAME = "scale-out-api"
TOOL = "scale_deployment"
NOMINATED: dict[str, Any] = {"namespace": "example-ns", "deployment": "example-api", "replicas": 4}

AUTHORITY_KINDS = ("undo_ruling", "capability_probe", "policy", "approval", "qualification")
ACTOR_KINDS = ("model_turn", "policy", "approval", "undo_ruling")
VERIFICATION_OUTCOMES = ("verified", "not-recovered", "verifier-unavailable", "superseded")

_READ: dict[str, Any] = {
    "connector": "prometheus",
    "tool": "query",
    "arguments": {"query": "sum(rate(http_requests_errors_total[5m]))"},
    "pointer": "/data/result/0/value/1",
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
    "automatic": True,
    "qualification": None,
}
DOCUMENT: dict[str, Any] = {
    "route": "sre-oncall",
    "limits": {"per_policy_per_hour": 3, "per_incident_per_target": 1},
    "actions": [ACTION],
}


def _canonical(arguments: Any) -> str:
    """The executor's canonical argument text (ACTION-EXECUTOR-7)."""

    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(arguments: Any) -> str:
    return hashlib.sha256(_canonical(arguments).encode("utf-8")).hexdigest()


def _remediation() -> Any:
    """The authority source's module, imported per test so each reports its own failure."""

    return importlib.import_module("curie_api.remediation_forward")


def _seam() -> Any:
    return importlib.import_module("curie_api.action_forward")


# --------------------------------------------------------------------------- #
# Rows tasks 3 and 6 write: a bound policy generation and one nomination.
# --------------------------------------------------------------------------- #


def _bind_policy(agent_id: str, documents: dict[int, dict[str, Any]] | None = None) -> None:
    """Generation ``GENERATION`` of the agent's ``HOOK`` policy, bound by ``BOUND_BY``.

    ``documents`` overrides the document of a given generation (``DOCUMENT`` otherwise).
    """

    agent = uuid.UUID(agent_id)
    for generation in range(1, GENERATION + 1):
        sql_rows(
            "INSERT INTO curie.remediation_policy_generations "
            "(agent_id, hook, generation, operation_id, intent_sha256, document, armed, "
            "active, bound_by, created_at) "
            "VALUES (:agent_id, :hook, :generation, :operation_id, :intent, "
            "CAST(:document AS jsonb), true, true, :bound_by, now())",
            {
                "agent_id": agent,
                "hook": HOOK,
                "generation": generation,
                "operation_id": uuid.uuid4(),
                "intent": f"{generation:02x}" * 32,
                "document": json.dumps((documents or {}).get(generation, DOCUMENT)),
                "bound_by": BOUND_BY if generation == GENERATION else "U0EXAMPLE1",
            },
        )
    sql_rows(
        "INSERT INTO curie.remediation_policies "
        "(agent_id, hook, generation, operation_id, armed, active, updated_at) "
        "VALUES (:agent_id, :hook, :generation, :operation_id, true, true, now())",
        {
            "agent_id": agent,
            "hook": HOOK,
            "generation": GENERATION,
            "operation_id": uuid.uuid4(),
        },
    )


def _nominate(
    agent_id: str,
    *,
    state: str = "admitted",
    approval_id: uuid.UUID | None = None,
    event_id: str = EVENT_ID,
    arguments: dict[str, Any] | None = None,
    admitted_generation: int | None = GENERATION,
) -> uuid.UUID:
    """One well-formed nomination of ``ACTION_NAME`` from the protected delivery ``event_id``."""

    agent = uuid.UUID(agent_id)
    bound = NOMINATED if arguments is None else arguments
    if not sql_rows(
        "SELECT 1 FROM curie.remediation_nomination_submissions WHERE event_id = :e",
        {"e": event_id},
    ):
        sql_rows(
            "INSERT INTO curie.remediation_nomination_submissions "
            "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent_id, :hook, :sha)",
            {"e": event_id, "agent_id": agent, "hook": HOOK, "sha": "cd" * 32},
        )
    nomination_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, admitted_generation, current_generation, action, "
        "kind, arguments, arguments_sha256, target, reason, state, approval_id) "
        "VALUES (:id, :agent_id, :hook, :e, :admitted, :generation, :action, 'remediate', "
        ":arguments, :sha, :target, 'error ratio above threshold', :state, :approval_id)",
        {
            "id": nomination_id,
            "agent_id": agent,
            "hook": HOOK,
            "e": event_id,
            "admitted": admitted_generation,
            "generation": GENERATION,
            "action": ACTION_NAME,
            "arguments": _canonical(bound),
            "sha": _sha(bound),
            "target": f'{CONNECTOR}:"{bound["deployment"]}"',
            "state": state,
            "approval_id": approval_id,
        },
    )
    return nomination_id


def _policy_ref(agent_id: str, nomination_id: uuid.UUID) -> str:
    return f"policy:{agent_id}:{HOOK}:{GENERATION}:{nomination_id}"


def _create(nomination_id: uuid.UUID, approval_id: uuid.UUID | None = None) -> Any:
    """Call the authority source in a session of its own, as admission or approval would."""

    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await _remediation().create_remediation_forward(
                    session, nomination_id, approval_id=approval_id
                )
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _executions() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.action_executions ORDER BY created_at, id")


def _ledger() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.agent_actions ORDER BY created_at, id")


def _nomination(nomination_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )
    assert len(rows) == 1
    return rows[0]


def _dispatch(client: Any, execution_id: Any) -> dict[str, Any]:
    """Claim and dispatch the one requested execution through the real worker routes."""

    claimed = client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(execution_id)
    fence = {"lease_owner": claimed.json()["lease_owner"], "attempt": claimed.json()["attempt"]}
    dispatched = client.post(
        f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
    )
    assert dispatched.status_code == 200, dispatched.text
    replay = client.post(
        f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
    )
    assert replay.status_code == 200, replay.text
    return dict(dispatched.json())


def _setup(client: Any, headers: dict[str, str], tmp_path: Path) -> str:
    """An agent sealing ``k8s`` at ``DIGEST`` (probed restore capable) with a bound policy."""

    agent_id = undoable_agent(client, headers, tmp_path)
    _bind_policy(agent_id)
    return agent_id


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-13: creation from an admitted nomination
# --------------------------------------------------------------------------- #


def test_an_admitted_nomination_creates_one_policy_forward_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: connector, tool and canonical arguments come
    from the nomination row and its policy generation; ``authority_kind`` is
    ``policy`` with the generation in ``authority_ref``; the key is
    ``remediation:<nomination id>:policy``; no ledger row exists before dispatch.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)

    created = _create(nomination_id)

    assert created.created is True
    assert created.state == "requested"
    rows = _executions()
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == created.execution_id
    assert row["kind"] == "forward"
    assert str(row["agent_id"]) == agent_id
    assert row["connector"] == CONNECTOR
    assert row["connector_digest"] == DIGEST
    assert row["tool"] == TOOL
    assert row["forward_arguments"] == NOMINATED
    assert row["arguments_sha256"] == _sha(NOMINATED)
    assert row["authority_kind"] == "policy"
    assert row["authority_ref"] == _policy_ref(agent_id, nomination_id)
    assert row["idempotency_key"] == f"remediation:{nomination_id}:policy"
    assert _nomination(nomination_id)["execution_id"] == created.execution_id
    assert _ledger() == []


def test_a_replayed_admission_creates_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: "a replayed admission creates nothing",
    before and after dispatch.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    first = _create(nomination_id)

    replay = _create(nomination_id)
    assert replay.execution_id == first.execution_id
    assert replay.created is False
    assert [row["id"] for row in _executions()] == [first.execution_id]

    _dispatch(client, first.execution_id)
    after = _create(nomination_id)

    assert after.execution_id == first.execution_id
    assert after.created is False
    assert [row["id"] for row in _executions()] == [first.execution_id]
    assert len(_ledger()) == 1


@pytest.mark.parametrize("state", ["received", "refused", "approval_requested", "rejected"])
def test_a_nomination_that_is_not_admitted_creates_no_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: only an admitted nomination is a policy authority."""

    agent_id = _setup(client, auth_headers, tmp_path)
    if state == "refused":
        nomination_id = _nominate(agent_id, state="admitted")
        sql_rows(
            "UPDATE curie.remediation_nominations SET state = 'refused', "
            "refusal_code = 'unknown_action' WHERE id = :id",
            {"id": nomination_id},
        )
    else:
        nomination_id = _nominate(agent_id, state=state)

    with pytest.raises(_seam().ForwardRefused):
        _create(nomination_id)

    assert _executions() == []
    assert _nomination(nomination_id)["execution_id"] is None


def test_an_approved_nomination_creates_an_approval_forward_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: an approval authority carries the approval id
    in ``authority_ref`` and, at dispatch, in ``gate_approval_id``.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    approval_id = uuid.uuid4()
    nomination_id = _nominate(agent_id, state="approved", approval_id=approval_id)

    created = _create(nomination_id, approval_id=approval_id)
    row = _executions()[0]
    assert row["authority_kind"] == "approval"
    assert row["authority_ref"] == str(approval_id)
    assert row["idempotency_key"] == f"remediation:{nomination_id}:approval:{approval_id}"
    assert row["forward_arguments"] == NOMINATED

    dispatched = _dispatch(client, created.execution_id)

    ledger = _ledger()
    assert len(ledger) == 1
    action = ledger[0]
    assert dispatched["subject_action_id"] == str(action["id"])
    assert action["authority_kind"] == "approval"
    assert action["authority_ref"] == str(approval_id)
    assert action["gate_approval_id"] == approval_id
    assert action["actor_kind"] == "approval"
    assert action["delivery_event_id"] == EVENT_ID
    assert action["nomination_id"] == nomination_id


def test_an_approval_that_is_not_the_nominations_creates_no_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: the approval named must be the one that
    approved this nomination.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id, state="approved", approval_id=uuid.uuid4())

    with pytest.raises(_seam().ForwardRefused):
        _create(nomination_id, approval_id=uuid.uuid4())

    assert _executions() == []


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-13, -14: the ledger row written at dispatch
# --------------------------------------------------------------------------- #


def test_dispatch_records_one_ledger_row_with_authority_actor_delivery_and_nomination(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-14: exactly one
    ledger row, with ``authority_kind`` ``policy``, the generation in
    ``authority_ref``, ``actor_kind`` ``policy``, the protected delivery's
    ``event_id`` and the nomination id; no gating approval; no outcome yet.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    created = _create(nomination_id)

    dispatched = _dispatch(client, created.execution_id)

    ledger = _ledger()
    assert len(ledger) == 1
    action = ledger[0]
    assert dispatched["subject_action_id"] == str(action["id"])
    assert action["tool"] == f"mcp__{CONNECTOR}__{TOOL}"
    assert action["arguments"] == NOMINATED
    assert action["connector"] == CONNECTOR
    assert action["connector_digest"] == DIGEST
    assert action["authority_kind"] == "policy"
    assert action["authority_ref"] == _policy_ref(agent_id, nomination_id)
    assert action["actor_kind"] == "policy"
    assert action["delivery_event_id"] == EVENT_ID
    assert action["nomination_id"] == nomination_id
    assert action["gate_approval_id"] is None
    assert action["verification_outcome"] is None
    assert action["verified_at"] is None


def test_two_nominations_of_one_delivery_record_one_row_each(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: every executed remediation produces exactly
    one ledger record, naming its own nomination and the shared delivery.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    first = _nominate(agent_id)
    second = _nominate(
        agent_id, arguments={**NOMINATED, "deployment": "example-worker", "replicas": 3}
    )
    for nomination_id in (first, second):
        _dispatch(client, _create(nomination_id).execution_id)

    ledger = _ledger()
    assert sorted(str(row["nomination_id"]) for row in ledger) == sorted([str(first), str(second)])
    assert {row["delivery_event_id"] for row in ledger} == {EVENT_ID}
    assert {row["authority_ref"] for row in ledger} == {
        _policy_ref(agent_id, first),
        _policy_ref(agent_id, second),
    }


def test_the_record_exposes_authority_actor_and_provenance_through_get_action(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: "a policy-executed record exposes every field
    above through ``GET /actions/{id}``".
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    dispatched = _dispatch(client, _create(nomination_id).execution_id)

    response = client.get(f"/actions/{dispatched['subject_action_id']}", headers=auth_headers)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["authority_kind"] == "policy"
    assert body["authority_ref"] == _policy_ref(agent_id, nomination_id)
    assert body["actor_kind"] == "policy"
    assert body["delivery_event_id"] == EVENT_ID
    assert body["nomination_id"] == str(nomination_id)
    assert "verification_outcome" in body
    assert body["verification_outcome"] is None
    assert "verified_at" in body
    assert body["verified_at"] is None


def test_the_audit_route_names_the_policy_actor_and_the_operator_that_bound_it(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: "A policy actor is represented as
    ``actor_kind`` ``policy`` with the policy reference as ``actor``, never as an
    empty human field"; the audit rows name the policy and generation and the
    operator principal that bound that generation.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    dispatched = _dispatch(client, _create(nomination_id).execution_id)

    response = client.get(f"/actions/{dispatched['subject_action_id']}/audit", headers=auth_headers)

    assert response.status_code == 200, response.text
    entries = response.json()
    assert entries, "a policy-executed record has an audit row naming its actor"
    assert all("actor_kind" in entry for entry in entries)
    assert all(entry["actor"] for entry in entries)
    policy = [entry for entry in entries if entry["actor_kind"] == "policy"]
    assert len(policy) == 1
    assert policy[0]["actor"].startswith(f"policy:{agent_id}:{HOOK}:{GENERATION}")
    assert BOUND_BY in json.dumps(policy[0])


def test_a_model_turn_record_keeps_model_turn_and_null_authority(
    client: Any, auth_headers: dict[str, str]
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: "a record written by a model turn keeps
    ``actor_kind`` ``model_turn`` and null authority".
    """

    opened = client.post(
        "/actions",
        json={
            "agent_id": None,
            "conversation_id": "C1",
            "call_id": "toolu_01",
            "tool": "mcp__k8s__scale",
            "arguments": {"name": "api", "replicas": 10},
            "dedupe_key": f"event-{uuid.uuid4()}:toolu_01",
        },
        headers=auth_headers,
    )
    assert opened.status_code == 201, opened.text

    row = _ledger()[0]
    assert row["actor_kind"] == "model_turn"
    assert row["authority_kind"] is None
    assert row["authority_ref"] is None
    assert row["delivery_event_id"] is None
    assert row["nomination_id"] is None
    body = client.get(f"/actions/{opened.json()['id']}", headers=auth_headers).json()
    assert body["actor_kind"] == "model_turn"
    assert body["authority_kind"] is None


# --------------------------------------------------------------------------- #
# AUTOMATED-REMEDIATION-14: the closed domains are database check constraints
# --------------------------------------------------------------------------- #


def _insert_action(**columns: Any) -> None:
    names = ["id", "conversation_id", "call_id", "tool", "dedupe_key", *columns]
    values = {
        "id": uuid.uuid4(),
        "conversation_id": "C1",
        "call_id": "toolu_01",
        "tool": "mcp__k8s__scale",
        "dedupe_key": f"check-{uuid.uuid4()}",
        **columns,
    }
    sql_rows(
        f"INSERT INTO curie.agent_actions ({', '.join(names)}) "
        f"VALUES ({', '.join(':' + name for name in names)})",
        values,
    )


def _violates_check(insert: Any) -> None:
    with pytest.raises(IntegrityError) as raised:
        insert()
    assert "check constraint" in str(raised.value).lower(), raised.value


@pytest.mark.parametrize(
    "column, value",
    [
        ("authority_kind", "operator"),
        ("authority_kind", "Policy"),
        ("actor_kind", "human"),
        ("actor_kind", ""),
        ("verification_outcome", "recovered"),
    ],
)
def test_an_unknown_ledger_value_violates_the_check(column: str, value: str) -> None:
    """@spec AUTOMATED-REMEDIATION-14: "an unknown ``authority_kind`` violates the
    check"; ``actor_kind`` and ``verification_outcome`` are closed the same way.
    """

    _violates_check(lambda: _insert_action(**{column: value}))
    assert _ledger() == []


@pytest.mark.parametrize("kind", AUTHORITY_KINDS)
def test_every_closed_authority_kind_is_accepted_on_the_ledger(kind: str) -> None:
    """@spec AUTOMATED-REMEDIATION-14: the positive control for the closed set."""

    _insert_action(authority_kind=kind, authority_ref="ref-1", actor_kind="policy")
    assert [row["authority_kind"] for row in _ledger()] == [kind]


@pytest.mark.parametrize("actor_kind", ACTOR_KINDS)
def test_every_closed_actor_kind_is_accepted_on_the_ledger(actor_kind: str) -> None:
    """@spec AUTOMATED-REMEDIATION-14: the positive control for ``actor_kind``."""

    _insert_action(actor_kind=actor_kind)
    assert [row["actor_kind"] for row in _ledger()] == [actor_kind]


@pytest.mark.parametrize("outcome", VERIFICATION_OUTCOMES)
def test_every_verification_outcome_is_accepted_on_the_ledger(outcome: str) -> None:
    """@spec AUTOMATED-REMEDIATION-14: the positive control for the outcome."""

    _insert_action(verification_outcome=outcome)
    assert [row["verification_outcome"] for row in _ledger()] == [outcome]


def test_an_unknown_execution_authority_kind_violates_the_check(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: the check closes ``authority_kind`` on the
    executions table too.
    """

    agent_id = undoable_agent(client, auth_headers, tmp_path)

    def insert(kind: str) -> None:
        sql_rows(
            "INSERT INTO curie.action_executions (id, kind, agent_id, connector, "
            "connector_digest, authority_kind, authority_ref, idempotency_key) "
            "VALUES (:id, 'probe', :agent_id, :connector, :digest, :kind, 'ref-1', :key)",
            {
                "id": uuid.uuid4(),
                "agent_id": uuid.UUID(agent_id),
                "connector": CONNECTOR,
                "digest": DIGEST,
                "kind": kind,
                "key": f"check-{uuid.uuid4()}",
            },
        )

    _violates_check(lambda: insert("operator"))
    assert _executions() == []
    insert("qualification")
    assert [row["authority_kind"] for row in _executions()] == ["qualification"]


def test_an_unknown_audit_actor_kind_violates_the_check() -> None:
    """@spec AUTOMATED-REMEDIATION-14: ``action_audit_entries.actor_kind`` is closed."""

    action_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agent_actions (id, conversation_id, call_id, tool, dedupe_key) "
        "VALUES (:id, 'C1', 'toolu_01', 'mcp__k8s__scale', :key)",
        {"id": action_id, "key": f"check-{action_id}"},
    )

    def insert(actor_kind: str) -> None:
        sql_rows(
            "INSERT INTO curie.action_audit_entries "
            "(id, action_id, action, actor, authorizer, authorized, actor_kind) "
            "VALUES (:id, :action_id, 'undone', 'U-operator', 'static', true, :actor_kind)",
            {"id": uuid.uuid4(), "action_id": action_id, "actor_kind": actor_kind},
        )

    _violates_check(lambda: insert("human"))
    insert("undo_ruling")
    rows = sql_dicts(
        "SELECT actor_kind FROM curie.action_audit_entries WHERE action_id = :id",
        {"id": action_id},
    )
    assert rows == [{"actor_kind": "undo_ruling"}]


# --------------------------------------------------------------------------- #
# Review round 1: approval of a nomination with no admitted generation
# --------------------------------------------------------------------------- #


def _audit(client: Any, headers: dict[str, str], action_id: Any) -> list[dict[str, Any]]:
    response = client.get(f"/actions/{action_id}/audit", headers=headers)
    assert response.status_code == 200, response.text
    return list(response.json())


@pytest.mark.parametrize("admitted", [None, GENERATION - 1], ids=["no-admitted", "older"])
def test_an_approved_nomination_resolves_its_action_from_the_current_generation(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, admitted: int | None
) -> None:
    """@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-14: AUTOMATED-
    REMEDIATION-4 sends a nomination with no admitted generation (a delivery
    admitted before any policy existed, or an envelope without the field), or one
    whose admitted generation is no longer current, to approval. Once approved it
    executes: the action is read from the current generation (the one the
    approval card and AR-16's ``policy_changed`` judge), ``authority_ref`` is the
    approval id (AR-13), and the audit row names that current generation and the
    operator who bound it.
    """

    agent_id = undoable_agent(client, auth_headers, tmp_path)
    older = {**DOCUMENT, "actions": [{**ACTION, "tool": "scale_deployment_legacy"}]}
    _bind_policy(agent_id, {GENERATION - 1: older})
    approval_id = uuid.uuid4()
    nomination_id = _nominate(
        agent_id, state="approved", approval_id=approval_id, admitted_generation=admitted
    )

    created = _create(nomination_id, approval_id=approval_id)

    row = _executions()[0]
    assert row["id"] == created.execution_id
    assert row["connector"] == CONNECTOR
    assert row["tool"] == TOOL
    assert row["forward_arguments"] == NOMINATED
    assert row["authority_kind"] == "approval"
    assert row["authority_ref"] == str(approval_id)
    dispatched = _dispatch(client, created.execution_id)
    action = _ledger()[0]
    assert action["tool"] == f"mcp__{CONNECTOR}__{TOOL}"
    assert action["gate_approval_id"] == approval_id
    assert action["nomination_id"] == nomination_id
    entries = [
        entry
        for entry in _audit(client, auth_headers, dispatched["subject_action_id"])
        if entry["actor_kind"] == "approval"
    ]
    assert len(entries) == 1
    assert entries[0]["actor"]
    assert entries[0]["evidence"]["generation"] == GENERATION
    assert BOUND_BY in json.dumps(entries[0])


# --------------------------------------------------------------------------- #
# Review round 1: ``not_reversible_now`` at dispatch (AR-13, executor E6)
# --------------------------------------------------------------------------- #


def _lose_capability(agent_id: str, how: str) -> None:
    params = {"agent_id": uuid.UUID(agent_id)}
    if how == "not-capable":
        sql_rows(
            "UPDATE curie.connector_capabilities SET restore_capable = false "
            "WHERE agent_id = :agent_id",
            params,
        )
    else:
        sql_rows("DELETE FROM curie.connector_capabilities WHERE agent_id = :agent_id", params)


@pytest.mark.parametrize("how", ["not-capable", "row-gone"])
def test_a_reversible_action_whose_capability_no_longer_holds_is_refused_at_dispatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, how: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: "A forward execution of a ``reversible``
    action whose capability or custody no longer holds at dispatch is refused
    ``not_reversible_now`` and its nomination goes to approval." A pre-dispatch
    refusal (E6): the execution never reaches ``dispatched``, no ledger row is
    written and the worker can no longer read the call's arguments.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    created = _create(nomination_id)
    _lose_capability(agent_id, how)
    claimed = client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )
    assert claimed.status_code == 200, claimed.text
    fence = {"lease_owner": claimed.json()["lease_owner"], "attempt": claimed.json()["attempt"]}

    response = client.post(
        f"/action-executions/{created.execution_id}/dispatch", json=fence, headers=worker_headers()
    )

    assert not (response.status_code == 200 and response.json().get("state") == "dispatched"), (
        response.text
    )
    execution = _executions()[0]
    assert execution["state"] == "refused"
    assert execution["refusal_code"] == "not_reversible_now"
    assert execution["dispatched_at"] is None
    assert execution["subject_action_id"] is None
    assert _ledger() == []
    arguments = client.post(
        f"/action-executions/{created.execution_id}/arguments",
        json=fence,
        headers=worker_headers(),
    )
    assert arguments.status_code != 200, arguments.text
    assert _nomination(nomination_id)["state"] == "approval_requested"


def test_an_idempotent_action_dispatches_without_a_restore_capability(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: the control. ``not_reversible_now`` applies
    to ``reversible`` actions only; an ``idempotent`` one needs no restore pair.
    """

    agent_id = undoable_agent(client, auth_headers, tmp_path)
    idempotent = {**DOCUMENT, "actions": [{**ACTION, "reversibility": "idempotent"}]}
    _bind_policy(agent_id, {GENERATION: idempotent})
    nomination_id = _nominate(agent_id)
    created = _create(nomination_id)
    _lose_capability(agent_id, "row-gone")

    dispatched = _dispatch(client, created.execution_id)

    assert dispatched["state"] == "dispatched"
    assert len(_ledger()) == 1


# --------------------------------------------------------------------------- #
# Review round 1: undo audit rows record the ``undo_ruling`` actor
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("closing", ["refused", "confirmed"])
def test_an_undo_rulings_audit_rows_record_the_undo_ruling_actor(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, closing: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: ``actor_kind`` ``undo_ruling`` on the
    authorized ruling row and on the restore's closing row, never empty.
    """

    agent_id = undoable_agent(client, auth_headers, tmp_path)
    action = sealed_action(client, auth_headers, agent_id)
    ruled = client.post(f"/actions/{action['id']}/undo", json={}, headers=operator_headers())
    assert ruled.status_code == 202, ruled.text
    execution_id = ruled.json()["execution_id"]
    claimed = client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )
    assert claimed.status_code == 200, claimed.text
    fence = {"lease_owner": claimed.json()["lease_owner"], "attempt": claimed.json()["attempt"]}
    if closing == "refused":
        body: dict[str, Any] = {**fence, "state": "refused", "code": "tool_not_advertised"}
    else:
        observed = client.post(
            f"/action-executions/{execution_id}/observation",
            json={**fence, "version": POST_VERSION},
            headers=worker_headers(),
        )
        assert observed.status_code == 200, observed.text
        dispatch = client.post(
            f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
        )
        assert dispatch.status_code == 200, dispatch.text
        body = {**fence, "state": "confirmed"}
    reported = client.post(
        f"/action-executions/{execution_id}/outcome", json=body, headers=worker_headers()
    )
    assert reported.status_code == 200, reported.text

    entries = _audit(client, auth_headers, action["id"])

    assert [entry["action"] for entry in entries] == ["authorized", closing]
    assert [entry["actor_kind"] for entry in entries] == ["undo_ruling", "undo_ruling"]
    assert all(entry["actor"] for entry in entries)


def test_a_refused_undo_audit_row_records_the_undo_ruling_actor(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-14: a refused ruling's row is ``undo_ruling`` too."""

    agent_id = undoable_agent(client, auth_headers, tmp_path)
    action = sealed_action(client, auth_headers, agent_id, failed=True)

    ruled = client.post(f"/actions/{action['id']}/undo", json={}, headers=operator_headers())

    assert ruled.status_code == 409, ruled.text
    entries = _audit(client, auth_headers, action["id"])
    assert len(entries) == 1
    assert entries[0]["authorized"] is False
    assert entries[0]["actor_kind"] == "undo_ruling"


# --------------------------------------------------------------------------- #
# Review round 2: one execution per authority (AR-13 per-authority key)
# --------------------------------------------------------------------------- #


def _claim_fence(client: Any, execution_id: Any) -> dict[str, Any]:
    claimed = client.post(
        "/action-executions/claim",
        json={"lease_owner": "worker-a", "lease_seconds": 60},
        headers=worker_headers(),
    )
    assert claimed.status_code == 200, claimed.text
    assert claimed.json()["id"] == str(execution_id)
    return {"lease_owner": claimed.json()["lease_owner"], "attempt": claimed.json()["attempt"]}


def _refused_not_reversible_now(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> tuple[str, uuid.UUID, Any]:
    """A policy execution refused ``not_reversible_now``; its nomination back at approval."""

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    policy = _create(nomination_id)
    _lose_capability(agent_id, "row-gone")
    fence = _claim_fence(client, policy.execution_id)
    client.post(
        f"/action-executions/{policy.execution_id}/dispatch", json=fence, headers=worker_headers()
    )
    refused = _executions()[0]
    assert refused["state"] == "refused"
    assert refused["refusal_code"] == "not_reversible_now"
    assert _nomination(nomination_id)["state"] == "approval_requested"
    return agent_id, nomination_id, policy


def _approve(nomination_id: uuid.UUID, approval_id: uuid.UUID) -> None:
    """What task 10's resolution leaves on the nomination row when its approval is approved."""

    sql_rows(
        "UPDATE curie.remediation_nominations SET state = 'approved', approval_id = :a "
        "WHERE id = :id",
        {"a": approval_id, "id": nomination_id},
    )


def test_the_approval_after_a_not_reversible_now_refusal_creates_its_own_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: the idempotency key is per authority
    (``remediation:<nomination id>:policy`` and
    ``remediation:<nomination id>:approval:<approval id>``), so the approval that
    follows a ``not_reversible_now`` refusal does not adopt the refused policy
    execution: it creates a second execution, which dispatches (the refusal
    judges a ``policy`` authority only) and records one ledger row.
    """

    _agent_id, nomination_id, policy = _refused_not_reversible_now(client, auth_headers, tmp_path)
    approval_id = uuid.uuid4()
    _approve(nomination_id, approval_id)

    approved = _create(nomination_id, approval_id=approval_id)

    assert approved.created is True
    assert approved.execution_id != policy.execution_id
    rows = {row["id"]: row for row in _executions()}
    assert set(rows) == {policy.execution_id, approved.execution_id}
    assert rows[policy.execution_id]["state"] == "refused"
    assert rows[policy.execution_id]["idempotency_key"] == f"remediation:{nomination_id}:policy"
    second = rows[approved.execution_id]
    assert second["authority_kind"] == "approval"
    assert second["authority_ref"] == str(approval_id)
    assert second["idempotency_key"] == f"remediation:{nomination_id}:approval:{approval_id}"
    assert second["state"] == "requested"
    assert _nomination(nomination_id)["execution_id"] == approved.execution_id

    dispatched = _dispatch(client, approved.execution_id)

    assert dispatched["state"] == "dispatched"
    ledger = _ledger()
    assert len(ledger) == 1
    assert ledger[0]["authority_kind"] == "approval"
    assert ledger[0]["gate_approval_id"] == approval_id
    assert ledger[0]["nomination_id"] == nomination_id


def test_a_replay_of_each_authority_adopts_its_own_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: each authority yields at most one execution;
    a replayed admission adopts the refused policy execution (it never re-runs),
    and a replayed approval adopts the approval execution.
    """

    _agent_id, nomination_id, policy = _refused_not_reversible_now(client, auth_headers, tmp_path)
    approval_id = uuid.uuid4()
    _approve(nomination_id, approval_id)
    approved = _create(nomination_id, approval_id=approval_id)

    replayed_approval = _create(nomination_id, approval_id=approval_id)
    assert replayed_approval.execution_id == approved.execution_id
    assert replayed_approval.created is False

    # The nomination is no longer ``admitted``, so a late admission replay is
    # either refused or adopts the refused policy execution; it never creates.
    try:
        replayed_policy = _create(nomination_id)
    except _seam().ForwardRefused:
        pass
    else:
        assert replayed_policy.execution_id == policy.execution_id
        assert replayed_policy.created is False
    assert len(_executions()) == 2


def test_an_approval_other_than_the_nominations_creates_nothing_after_one_executed(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13 @spec AUTOMATED-REMEDIATION-15: a nomination has
    at most one approval (the approval's ``dedupe_key`` ``remediation:<nomination
    id>`` is unique across all statuses), so a second approval id for the same
    nomination authorizes nothing, even though the per-authority key would be new.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    approval_id = uuid.uuid4()
    nomination_id = _nominate(agent_id, state="approved", approval_id=approval_id)
    first = _create(nomination_id, approval_id=approval_id)

    with pytest.raises(_seam().ForwardRefused):
        _create(nomination_id, approval_id=uuid.uuid4())

    assert [row["id"] for row in _executions()] == [first.execution_id]
    assert _nomination(nomination_id)["execution_id"] == first.execution_id


# --------------------------------------------------------------------------- #
# Review round 3: the nomination guard and the custody path of not_reversible_now
# --------------------------------------------------------------------------- #


def _refuse_at_dispatch(client: Any, execution_id: Any) -> dict[str, Any]:
    """Claim and attempt dispatch; return the execution row afterwards."""

    fence = _claim_fence(client, execution_id)
    response = client.post(
        f"/action-executions/{execution_id}/dispatch", json=fence, headers=worker_headers()
    )
    assert not (response.status_code == 200 and response.json().get("state") == "dispatched"), (
        response.text
    )
    rows = [row for row in _executions() if row["id"] == execution_id]
    assert len(rows) == 1
    return rows[0]


@pytest.mark.parametrize("state", ["approval_requested", "rejected", "expired", "finished"])
def test_a_not_reversible_now_refusal_moves_only_an_admitted_nomination_to_approval(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, state: str
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: the refusal sends the nomination to
    approval only while it is still ``admitted``; a nomination that has already
    moved on (another path decided it) keeps its state. The execution is still
    refused ``not_reversible_now`` with no ledger row.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    created = _create(nomination_id)
    sql_rows(
        "UPDATE curie.remediation_nominations SET state = :state WHERE id = :id",
        {"state": state, "id": nomination_id},
    )
    _lose_capability(agent_id, "row-gone")

    execution = _refuse_at_dispatch(client, created.execution_id)

    assert execution["state"] == "refused"
    assert execution["refusal_code"] == "not_reversible_now"
    assert _ledger() == []
    assert _nomination(nomination_id)["state"] == state


_UNSEALED_CONNECTORS = f"""connectors:
  {CONNECTOR}:
    image: {IMAGE}
"""


def _unsealed_archive(tmp_path: Path, name: str) -> bytes:
    """A bundle pinning the same ``k8s`` image but declaring no sealing key."""

    root = tmp_path / f"unsealed-{uuid.uuid4().hex[:8]}"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": name, "version": "0.2.0", "description": "t"}), encoding="utf-8"
    )
    (root / "skills" / name).mkdir(parents=True)
    (root / "skills" / name / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(_UNSEALED_CONNECTORS, encoding="utf-8")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(root, arcname=name)
    return buf.getvalue()


def _deploy_without_custody(
    client: Any, headers: dict[str, str], tmp_path: Path, agent_id: str
) -> None:
    """Put a version in force that pins the same digest but drops the sealing key."""

    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v2", "created_by": "test"},
        headers=headers,
    )
    assert version.status_code == 201, version.text
    version_id = str(version.json()["id"])
    name = client.get(f"/agents/{agent_id}", headers=headers).json()["name"]
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("bundle.tar.gz", _unsealed_archive(tmp_path, name))},
        headers=headers,
    )
    assert upload.status_code == 201, upload.text
    deployment = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=headers,
    )
    assert deployment.status_code == 201, deployment.text


def test_a_reversible_action_whose_custody_no_longer_holds_is_refused_at_dispatch(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec AUTOMATED-REMEDIATION-13: "capability **or custody**". The restore
    capability row still holds, but the version now in force pins the same image
    digest and declares no sealing key (ACTION-EXECUTOR-16), so the policy
    execution is refused ``not_reversible_now`` with no ledger row and its
    nomination goes to approval.
    """

    agent_id = _setup(client, auth_headers, tmp_path)
    nomination_id = _nominate(agent_id)
    created = _create(nomination_id)
    _deploy_without_custody(client, auth_headers, tmp_path, agent_id)
    assert sql_rows(
        "SELECT 1 FROM curie.connector_capabilities WHERE agent_id = :a AND restore_capable",
        {"a": uuid.UUID(agent_id)},
    )

    execution = _refuse_at_dispatch(client, created.execution_id)

    assert execution["state"] == "refused"
    assert execution["refusal_code"] == "not_reversible_now"
    assert execution["dispatched_at"] is None
    assert _ledger() == []
    assert _nomination(nomination_id)["state"] == "approval_requested"
