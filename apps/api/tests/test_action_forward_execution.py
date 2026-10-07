"""The forward execution seam of the action executor, on real Postgres (plan task 12).

@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-2
@spec ACTION-EXECUTOR-7 @spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-18

"A server-side API function creates a ``forward`` execution from a verified
authority record" (ACTION-EXECUTOR-19); it is "an API function, not an HTTP
route" (ACTION-EXECUTOR-1). The authority sources (admission, #4065, and the
remediation approval, #4069) are later tasks, so the tests here hand the
function the verified record those sources will build, and drive everything
after creation through the real worker routes.

Surface these tests fix (see ``.projects/plans/task-executor-forward.tests.md``):

* ``curie_api.action_forward.ForwardAuthority``: a frozen record with
  ``kind`` (``policy`` or ``approval``), ``ref``, ``agent_id``, ``connector``,
  ``connector_digest``, ``tool`` (the upstream tool name), ``arguments`` (the
  argument object the authority bound), ``arguments_sha256`` (the digest the
  authority bound over their canonical bytes), ``idempotency_key`` (supplied by
  the authority owner) and optional ``requested_by``;
* ``curie_api.action_forward.create_forward_execution(session, authority)``
  returns ``ForwardCreated(execution_id, state, created)`` and raises
  ``ForwardRefused`` (``.code``) for a refusal, which creates no row;
* ``POST /action-executions/{id}/arguments`` (internal worker token, body is
  exactly the fence) answers a claimed forward execution's ``{"tool",
  "arguments"}``, so the worker can recompute the digest before dispatch
  (ACTION-EXECUTOR-7) while the receipt (``ExecutionOut``) keeps carrying no
  argument;
* ``POST /action-executions/{id}/dispatch`` on a claimed forward execution
  commits ``dispatched`` and creates exactly one ``agent_actions`` row, whose
  id becomes the execution's ``subject_action_id``.

Every identifier is a placeholder.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from _sealed_actions import (
    CONNECTOR,
    DIGEST,
    ENVELOPE,
    POST_VERSION,
    TARGET,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    undoable_agent,
    worker_headers,
)
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db", "executor_enabled")

TOOL = "scale"
ARGUMENTS: dict[str, Any] = {"target": TARGET, "replicas": 3, "note": "réplicas"}
OTHER_ARGUMENTS: dict[str, Any] = {"target": TARGET, "replicas": 30, "note": "réplicas"}
GENERATION_REF = "policy:{agent}:example-hook:7:{nomination}"


def _canonical(arguments: Any) -> str:
    """The proxy's canonical form (ACTION-EXECUTOR-7)."""

    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(arguments: Any) -> str:
    return hashlib.sha256(_canonical(arguments).encode("utf-8")).hexdigest()


def _forward() -> Any:
    """The seam's module, imported per test so each test reports its own failure."""

    return importlib.import_module("curie_api.action_forward")


def _authority(
    agent_id: str,
    *,
    kind: str = "policy",
    ref: str | None = None,
    tool: str = TOOL,
    arguments: dict[str, Any] | None = None,
    arguments_sha256: str | None = None,
    idempotency_key: str | None = None,
    connector: str = CONNECTOR,
    digest: str = DIGEST,
) -> Any:
    bound = ARGUMENTS if arguments is None else arguments
    nomination = "00000000-0000-4000-8000-0000000000f1"
    return _forward().ForwardAuthority(
        kind=kind,
        ref=ref or GENERATION_REF.format(agent=agent_id, nomination=nomination),
        agent_id=uuid.UUID(agent_id),
        connector=connector,
        connector_digest=digest,
        tool=tool,
        arguments=bound,
        arguments_sha256=arguments_sha256 or _sha(bound),
        idempotency_key=idempotency_key or f"remediation:{nomination}",
    )


def _create(authority: Any) -> Any:
    """Call the creation function in a session of its own, as an authority source would."""

    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await _forward().create_forward_execution(session, authority)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _refusal(authority: Any) -> str:
    with pytest.raises(_forward().ForwardRefused) as refused:
        _create(authority)
    return str(refused.value.code)


def _executions() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.action_executions ORDER BY created_at, id")


def _ledger() -> list[dict[str, Any]]:
    return sql_dicts("SELECT * FROM curie.agent_actions ORDER BY created_at, id")


def _claim(client: Any, owner: str = "worker-a") -> dict[str, Any]:
    response = client.post(
        "/action-executions/claim",
        json={"lease_owner": owner, "lease_seconds": 60},
        headers=worker_headers(),
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


def _fence(claimed: dict[str, Any]) -> dict[str, Any]:
    return {"lease_owner": claimed["lease_owner"], "attempt": claimed["attempt"]}


def _post(client: Any, execution_id: Any, route: str, body: dict[str, Any]) -> Any:
    return client.post(
        f"/action-executions/{execution_id}/{route}", json=body, headers=worker_headers()
    )


def _agent(client: Any, headers: dict[str, str], tmp_path: Path) -> str:
    """An agent whose in-force version seals ``k8s`` at ``DIGEST``, probed restore capable."""

    return undoable_agent(client, headers, tmp_path)


def _created(client: Any, headers: dict[str, str], tmp_path: Path) -> tuple[str, Any]:
    agent_id = _agent(client, headers, tmp_path)
    created = _create(_authority(agent_id))
    return agent_id, created


def _dispatched(
    client: Any, headers: dict[str, str], tmp_path: Path
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    """A forward execution created, claimed and dispatched: (agent, id, fence, row)."""

    agent_id, created = _created(client, headers, tmp_path)
    claimed = _claim(client)
    assert claimed["id"] == str(created.execution_id)
    fence = _fence(claimed)
    dispatched = _post(client, created.execution_id, "dispatch", fence)
    assert dispatched.status_code == 200, dispatched.text
    return agent_id, str(created.execution_id), fence, dict(dispatched.json())


# --------------------------------------------------------------------------- #
# Creation (ACTION-EXECUTOR-19, -2, -7)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("kind", ["policy", "approval"])
def test_a_verified_authority_creates_one_requested_forward_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, kind: str
) -> None:
    """@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-2: connector, tool and
    canonical arguments come from the authority record; the execution carries
    the digest, the arguments digest and the authority fields, and no ledger
    row exists before dispatch.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    ref = (
        GENERATION_REF.format(agent=agent_id, nomination="n-1")
        if kind == "policy"
        else str(uuid.uuid4())
    )

    created = _create(_authority(agent_id, kind=kind, ref=ref))

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
    assert row["forward_arguments"] == ARGUMENTS
    assert row["arguments_sha256"] == _sha(ARGUMENTS)
    assert row["authority_kind"] == kind
    assert row["authority_ref"] == ref
    assert row["idempotency_key"] == "remediation:00000000-0000-4000-8000-0000000000f1"
    assert row["state"] == "requested"
    assert row["subject_action_id"] is None
    assert _ledger() == []


def test_a_replayed_creation_returns_the_same_execution_and_creates_nothing(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19: "a replayed creation creates nothing"."""

    agent_id, first = _created(client, auth_headers, tmp_path)

    replay = _create(_authority(agent_id))

    assert replay.execution_id == first.execution_id
    assert replay.created is False
    assert [row["id"] for row in _executions()] == [first.execution_id]
    assert _ledger() == []


def test_a_replay_after_dispatch_creates_no_second_execution_or_ledger_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-2: a replay adopts the
    existing row in whatever state it is, so a dispatched call is never made twice.
    """

    agent_id, execution_id, _fence_, _row = _dispatched(client, auth_headers, tmp_path)

    replay = _create(_authority(agent_id))

    assert str(replay.execution_id) == execution_id
    assert replay.created is False
    assert replay.state == "dispatched"
    assert len(_executions()) == 1
    assert len(_ledger()) == 1


def test_a_replay_naming_other_arguments_is_refused_and_the_first_stands(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-7: argument drift under one
    authority key is refused ``arguments_mismatch``; nothing is adopted or changed.
    """

    agent_id, first = _created(client, auth_headers, tmp_path)

    code = _refusal(_authority(agent_id, arguments=OTHER_ARGUMENTS))

    assert code == "arguments_mismatch"
    rows = _executions()
    assert [row["id"] for row in rows] == [first.execution_id]
    assert rows[0]["forward_arguments"] == ARGUMENTS
    assert rows[0]["arguments_sha256"] == _sha(ARGUMENTS)


def test_arguments_differing_from_the_authority_digest_are_refused_with_no_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19: "arguments differing from the authority record
    are refused". The record's arguments must hash to the digest the authority
    bound; any drift is ``arguments_mismatch`` and creates nothing.
    """

    agent_id = _agent(client, auth_headers, tmp_path)

    code = _refusal(
        _authority(agent_id, arguments=OTHER_ARGUMENTS, arguments_sha256=_sha(ARGUMENTS))
    )

    assert code == "arguments_mismatch"
    assert _executions() == []
    assert _ledger() == []


@pytest.mark.parametrize("kind", ["undo_ruling", "capability_probe", "qualification", "model"])
def test_an_authority_that_is_not_a_forward_authority_is_refused_unavailable(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, kind: str
) -> None:
    """@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-1: only ``policy`` or
    ``approval`` authorize a forward execution; anything else is
    ``authority_unavailable`` and creates no row.
    """

    agent_id = _agent(client, auth_headers, tmp_path)

    code = _refusal(_authority(agent_id, kind=kind, ref="ref-1"))

    assert code == "authority_unavailable"
    assert _executions() == []


def test_a_forward_execution_may_not_target_observe_version(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19: ``observe_version`` is ``reserved_verb_via_forward``."""

    agent_id = _agent(client, auth_headers, tmp_path)

    code = _refusal(_authority(agent_id, tool="observe_version", arguments={"target": TARGET}))

    assert code == "reserved_verb_via_forward"
    assert _executions() == []


def test_restore_on_a_paired_connector_is_reserved_but_a_lone_restore_is_ordinary(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19 @spec ACTION-EXECUTOR-8: ``restore`` is reserved
    only on a connector whose probe recorded the pair for that digest.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    restore_args = {"target": TARGET, "prior_state": {"spec": {"replicas": 3}}}

    paired = _refusal(
        _authority(agent_id, tool="restore", arguments=restore_args, idempotency_key="k-paired")
    )
    assert paired == "reserved_verb_via_forward"
    assert _executions() == []

    sql_rows(
        "UPDATE curie.connector_capabilities SET restore_capable = false "
        "WHERE agent_id = :agent_id",
        {"agent_id": uuid.UUID(agent_id)},
    )
    lone = _create(
        _authority(agent_id, tool="restore", arguments=restore_args, idempotency_key="k-lone")
    )

    assert lone.created is True
    assert [row["tool"] for row in _executions()] == ["restore"]


# --------------------------------------------------------------------------- #
# The worker's read of the bound arguments (ACTION-EXECUTOR-7, -18)
# --------------------------------------------------------------------------- #


def test_the_claim_and_receipt_carry_the_digests_but_never_the_arguments(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18 @spec ACTION-EXECUTOR-7."""

    _agent_id, created = _created(client, auth_headers, tmp_path)

    claimed = _claim(client)
    receipt = client.get(f"/action-executions/{created.execution_id}", headers=auth_headers)

    assert claimed["kind"] == "forward"
    assert claimed["tool"] == TOOL
    assert claimed["connector_digest"] == DIGEST
    assert claimed["arguments_sha256"] == _sha(ARGUMENTS)
    assert receipt.status_code == 200, receipt.text
    for text in (json.dumps(claimed, ensure_ascii=False), receipt.text):
        assert "forward_arguments" not in text
        assert "réplicas" not in text


def test_the_worker_reads_the_bound_arguments_of_its_claimed_forward_execution(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-7: the worker recomputes ``arguments_sha256`` over the
    text it sends, so it reads the bound arguments under its fence before dispatch.
    """

    _agent_id, created = _created(client, auth_headers, tmp_path)
    fence = _fence(_claim(client))

    answer = _post(client, created.execution_id, "arguments", fence)

    assert answer.status_code == 200, answer.text
    assert answer.json() == {"tool": TOOL, "arguments": ARGUMENTS}
    stale = _post(client, created.execution_id, "arguments", {**fence, "attempt": 99})
    assert stale.status_code == 409, stale.text
    unauthenticated = client.post(
        f"/action-executions/{created.execution_id}/arguments", json=fence, headers=auth_headers
    )
    assert unauthenticated.status_code in (401, 403), unauthenticated.text
    naming = _post(client, created.execution_id, "arguments", {**fence, "tool": "other"})
    assert naming.status_code == 422, naming.text


# --------------------------------------------------------------------------- #
# Dispatch creates the ledger row (ACTION-EXECUTOR-19, -17)
# --------------------------------------------------------------------------- #


def test_dispatch_creates_exactly_one_ledger_row_carrying_the_digest(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19: "At dispatch the API creates exactly one
    ``agent_actions`` row: ``dedupe_key`` and ``call_id`` both ``exec:<execution
    id>``, tool ``mcp__<connector>__<tool>``, the canonical arguments, the
    authority fields, and ``connector`` and ``connector_digest`` copied from the
    execution, status ``pending``."
    """

    agent_id, execution_id, _fence_, dispatched = _dispatched(client, auth_headers, tmp_path)

    assert dispatched["state"] == "dispatched"
    ledger = _ledger()
    assert len(ledger) == 1
    action = ledger[0]
    assert dispatched["subject_action_id"] == str(action["id"])
    assert str(action["agent_id"]) == agent_id
    assert action["dedupe_key"] == f"exec:{execution_id}"
    assert action["call_id"] == f"exec:{execution_id}"
    assert action["tool"] == f"mcp__{CONNECTOR}__{TOOL}"
    assert action["arguments"] == ARGUMENTS
    assert action["connector"] == CONNECTOR
    assert action["connector_digest"] == DIGEST
    assert action["authority_kind"] == "policy"
    execution = _executions()[0]
    assert action["authority_ref"] == execution["authority_ref"]
    assert execution["subject_action_id"] == action["id"]
    assert action["status"] == "pending"
    assert action["gate_approval_id"] is None


def test_a_replayed_dispatch_creates_no_second_ledger_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-18 @spec ACTION-EXECUTOR-19: the same fence replays."""

    _agent_id, execution_id, fence, first = _dispatched(client, auth_headers, tmp_path)

    replay = _post(client, execution_id, "dispatch", fence)

    assert replay.status_code == 200, replay.text
    assert replay.json()["subject_action_id"] == first["subject_action_id"]
    assert len(_ledger()) == 1


def test_a_forward_execution_refused_before_dispatch_leaves_no_ledger_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-17 @spec ACTION-EXECUTOR-19: a pre-dispatch refusal
    (here the worker's ``arguments_mismatch``) is a provable non-write and
    records no action.
    """

    _agent_id, created = _created(client, auth_headers, tmp_path)
    fence = _fence(_claim(client))

    refused = _post(
        client,
        created.execution_id,
        "outcome",
        {**fence, "state": "refused", "code": "arguments_mismatch"},
    )

    assert refused.status_code == 200, refused.text
    assert refused.json()["refusal_code"] == "arguments_mismatch"
    assert _ledger() == []


def test_the_outcome_and_completion_finish_the_forward_ledger_row(
    client: Any, auth_headers: dict[str, str], tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-19: the row created at dispatch is completed "with the
    same snapshot parsing as a model turn's call", through the ledger's completion
    route with the worker's attribution, and the execution ends ``confirmed``.
    """

    _agent_id, execution_id, fence, dispatched = _dispatched(client, auth_headers, tmp_path)
    action_id = dispatched["subject_action_id"]

    completed = client.post(
        f"/actions/{action_id}/complete",
        json={
            "failed": False,
            "result": {"ok": True, "version": POST_VERSION},
            "prior_state": ENVELOPE,
            "post_state": {"spec": {"replicas": 3}},
            "post_version": POST_VERSION,
            "target": TARGET,
            "connector": CONNECTOR,
            "connector_digest": DIGEST,
        },
        headers={**auth_headers, **worker_headers()},
    )
    assert completed.status_code == 200, completed.text
    reported = _post(client, execution_id, "outcome", {**fence, "state": "confirmed"})

    assert reported.status_code == 200, reported.text
    assert reported.json()["state"] == "confirmed"
    action = _ledger()[0]
    assert action["status"] == "succeeded"
    assert action["connector_digest"] == DIGEST
    assert action["post_version"] == POST_VERSION


# --------------------------------------------------------------------------- #
# Undo of a forward-executed record (ACTION-EXECUTOR-19, #4068)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("kind", ["policy", "approval"])
def test_undo_of_a_forward_executed_record_is_refused_authority_unresolved(
    client: Any, auth_headers: dict[str, str], tmp_path: Path, kind: str
) -> None:
    """@spec ACTION-EXECUTOR-19: "Until #4068 delivers authority-aware undo
    authorization, undo of a forward-executed record is refused
    ``refused_authority_unresolved``"; ADR 0117 decision 3's ungated default must
    not apply. The record holds every other undo ingredient, so the authority is
    the only reason; one audit row, no restore execution.
    """

    agent_id = _agent(client, auth_headers, tmp_path)
    ref = str(uuid.uuid4()) if kind == "approval" else "policy:ref"
    created = _create(_authority(agent_id, kind=kind, ref=ref))
    fence = _fence(_claim(client))
    dispatched = _post(client, created.execution_id, "dispatch", fence)
    assert dispatched.status_code == 200, dispatched.text
    action_id = dispatched.json()["subject_action_id"]
    completed = client.post(
        f"/actions/{action_id}/complete",
        json={
            "failed": False,
            "result": {"ok": True, "version": POST_VERSION},
            "prior_state": ENVELOPE,
            "post_state": {"spec": {"replicas": 3}},
            "post_version": POST_VERSION,
            "target": TARGET,
            "connector": CONNECTOR,
            "connector_digest": DIGEST,
        },
        headers={**auth_headers, **worker_headers()},
    )
    assert completed.status_code == 200, completed.text
    assert (
        _post(client, created.execution_id, "outcome", {**fence, "state": "confirmed"}).json()[
            "state"
        ]
        == "confirmed"
    )

    undo = client.post(f"/actions/{action_id}/undo", json={}, headers=operator_headers())

    assert undo.status_code == 409, undo.text
    audit = client.get(f"/actions/{action_id}/audit", headers=auth_headers)
    assert audit.status_code == 200, audit.text
    assert [(e["action"], e["authorized"]) for e in audit.json()] == [
        ("refused_authority_unresolved", False)
    ]
    assert [row["kind"] for row in _executions()] == ["forward"]
    assert _ledger()[0]["undone_at"] is None
