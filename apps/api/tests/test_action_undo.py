"""Ruling on an undo (ADR-0117 decision 4), against real Postgres.

The API rules and the executor restores. Under the connector action executor
contract (ACTION-EXECUTOR-3) a granted undo no longer hands back the call to
make: in one transaction it writes a ``requested`` restore execution and the
``authorized`` audit row, answers ``202`` with the execution id and state, and
returns neither the ``target`` nor the sealed ``prior_state``. ``undone_at`` is
written only when the restore is confirmed (ACTION-EXECUTOR-18).

The rule ADR-0117 says the feature lives or dies on (never restore over a
world that moved) still holds, but the platform now performs the observation
itself through the pinned connector, and compares versions, not states
(ACTION-EXECUTOR-15). A caller-supplied ``observed_state`` is no longer
evidence, so the ruling neither refuses for its absence nor compares it; the
conflict refusal is written by the observation route and tested with it in
``test_action_execution_routes``.

Every refusal writes an audit entry before it raises and creates no execution.
A refusal nobody can read afterwards is a bug report the operator never gets.

The ruling follows the derived ``undoable`` (ACTION-EXECUTOR-11), so the tests
start from a fully undoable sealed record (``_sealed_actions``). The executor
setting is off by default (ACTION-EXECUTOR-1); tests that expect a granted undo
turn it on.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _sealed_actions import (
    ENVELOPE,
    POST_VERSION,
    executions_of,
    executor_enabled,  # noqa: F401 - fixture, requested by name
    operator_headers,
    sealed_action,
    undoable_agent,
)
from curie_api.approval_auth import AuthenticatedApprovalPrincipal
from curie_api.config import get_settings
from curie_api.models import AgentAction
from curie_api.routers.actions import undo_action
from curie_api.schemas.actions import ActionUndo
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db")

LEFT = {"spec": {"replicas": 10}}
PRIOR = {"spec": {"replicas": 3}}
TARGET = {"kind": "Deployment", "namespace": "public", "name": "api"}


def _record(client: Any, headers: Any, **complete: Any) -> dict[str, Any]:
    """One recorded call, completed however the caller says."""

    opened = client.post(
        "/actions",
        json={
            "conversation_id": "C1",
            "call_id": "toolu_01",
            "tool": "scale_deployment",
            "arguments": {"name": "api", "replicas": 10},
            "detail": "non-idempotent tool executed",
            "dedupe_key": f"event-{uuid.uuid4()}:toolu_01",
        },
        headers=headers,
    ).json()
    body: dict[str, Any] = {
        "failed": False,
        "result": {"ok": True},
        "prior_state": PRIOR,
        "post_state": LEFT,
        "target": TARGET,
        "detail": "non-idempotent tool completed",
    }
    body.update(complete)
    return dict(client.post(f"/actions/{opened['id']}/complete", json=body, headers=headers).json())


def _sealed(client: Any, headers: Any, tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    """One fully undoable record of a sealed, probed agent, minus any overrides."""

    return sealed_action(client, headers, undoable_agent(client, headers, tmp_path), **overrides)


def _undo(client: Any, headers: Any, action_id: str, **body: Any) -> Any:
    """Rule as the authenticated operator ``U-operator``.

    The executor route decisions: the actor is derived from an authenticated
    principal, as the approval resolver derives it (ADR 0106), never from a
    body field under the platform key. ``headers`` is kept for call sites; the
    principal replaces it.
    """

    return client.post(
        f"/actions/{action_id}/undo", json=dict(body), headers=operator_headers("U-operator")
    )


def _operator(subject: str) -> AuthenticatedApprovalPrincipal:
    """The authenticated principal an in-process ruling is called with."""

    return AuthenticatedApprovalPrincipal(subject=subject, kind="operator", actor_channel=None)


def _audit(client: Any, headers: Any, action_id: str) -> list[dict[str, Any]]:
    return list(client.get(f"/actions/{action_id}/audit", headers=headers).json())


def _holds_no_snapshot(text: str) -> None:
    """No envelope and no state anywhere in a response body or audit row."""

    assert ENVELOPE["ciphertext"] not in text
    assert json.dumps(LEFT, separators=(",", ":")) not in text.replace(" ", "")
    assert '"prior_state"' not in text
    assert '"post_state"' not in text


@pytest.mark.usefixtures("executor_enabled")
def test_an_authorized_undo_requests_one_restore_and_returns_no_snapshot(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: "an authorized undo yields one ``requested``
    execution and one audit row, and the response body contains no ``prior_state``".
    """

    action = _sealed(client, auth_headers, tmp_path)
    assert action["undoable"] is True

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 202, response.text
    ruling = response.json()
    _holds_no_snapshot(response.text)
    assert "restore" not in ruling
    executions = executions_of(action["id"])
    assert len(executions) == 1
    execution = executions[0]
    assert ruling["execution_id"] == str(execution["id"])
    assert ruling["state"] == "requested"
    assert execution["state"] == "requested"
    assert execution["kind"] == "restore"
    assert execution["tool"] == "restore"
    assert execution["requested_by"] == "U-operator"
    entries = _audit(client, auth_headers, action["id"])
    assert [e["action"] for e in entries] == ["authorized"]
    assert entries[0]["authorized"] is True


@pytest.mark.usefixtures("executor_enabled")
def test_an_authorized_undo_does_not_mark_the_action_undone(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-11: ``undone_at`` and
    ``undone_by`` are written only when a restore is confirmed, never at ruling.
    """

    action = _sealed(client, auth_headers, tmp_path)

    assert _undo(client, auth_headers, action["id"]).status_code == 202

    after = client.get(f"/actions/{action['id']}", headers=auth_headers).json()
    assert after["undone_at"] is None
    assert after["undone_by"] is None
    # The live restore holds the record (ACTION-EXECUTOR-11).
    assert after["undoable"] is False


@pytest.mark.usefixtures("executor_enabled")
def test_the_authorized_audit_row_names_the_execution_key_and_version_only(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: the ``authorized`` row's evidence "names the
    execution id, the key identifier and the recorded version only".
    """

    action = _sealed(client, auth_headers, tmp_path)
    ruling = _undo(client, auth_headers, action["id"]).json()

    entry = _audit(client, auth_headers, action["id"])[0]

    assert entry["evidence"] == {
        "execution_id": ruling["execution_id"],
        "kid": ENVELOPE["kid"],
        "version": POST_VERSION,
    }
    _holds_no_snapshot(json.dumps(entry))


@pytest.mark.usefixtures("executor_enabled")
def test_the_execution_carries_the_rulings_authority_and_key(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-3: ``undo_ruling`` authority
    whose reference is the authorizing audit row, and the idempotency key
    ``restore:<action id>:<authorizing audit row id>``, under the action's agent
    and pinned to its recorded digest.
    """

    action = _sealed(client, auth_headers, tmp_path)
    _undo(client, auth_headers, action["id"])

    execution = executions_of(action["id"])[0]
    audit_id = _audit(client, auth_headers, action["id"])[0]["id"]

    assert execution["authority_kind"] == "undo_ruling"
    assert execution["authority_ref"] == audit_id
    assert execution["idempotency_key"] == f"restore:{action['id']}:{audit_id}"
    assert str(execution["agent_id"]) == action["agent_id"]
    assert execution["connector"] == "k8s"
    assert execution["connector_digest"] == "sha256:" + "ab" * 32
    assert execution["outcome"] is None


@pytest.mark.usefixtures("executor_enabled")
def test_a_caller_observation_is_no_longer_evidence(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: "the caller-supplied ``observed_state`` is no
    longer accepted as evidence".

    A caller claiming the world moved does not produce the old
    ``refused_conflict`` at ruling time; the platform observes the version
    itself before the restore (ACTION-EXECUTOR-15).
    """

    action = _sealed(client, auth_headers, tmp_path)

    response = _undo(client, auth_headers, action["id"], observed_state={"spec": {"replicas": 7}})

    assert response.status_code == 202, response.text
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == ["authorized"]


@pytest.mark.usefixtures("executor_enabled")
def test_an_undo_without_an_observation_is_not_refused_as_unobserved(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3 @spec ACTION-EXECUTOR-15: the platform performs the
    observation through the pinned connector, so the caller no longer has to.
    """

    action = _sealed(client, auth_headers, tmp_path)

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 202, response.text
    assert "refused_unobserved" not in [
        e["action"] for e in _audit(client, auth_headers, action["id"])
    ]


def test_a_refused_undo_changes_nothing(client: Any, auth_headers: Any, tmp_path: Path) -> None:
    """`an undo either restores the recorded state or changes nothing at all`.

    @spec ACTION-EXECUTOR-1 @spec ACTION-EXECUTOR-11: the record is fully
    undoable, and a refusal (here ``executor_disabled``, the executor being off
    by default) must leave it so: the read after equals the read before, field
    for field, and no execution exists.
    """

    action = _sealed(client, auth_headers, tmp_path)
    before = client.get(f"/actions/{action['id']}", headers=auth_headers).json()

    refused = _undo(client, auth_headers, action["id"])

    # Assert the refusal happened before asserting nothing moved -- otherwise
    # this passes against an API with no undo endpoint at all.
    # @spec ACTION-EXECUTOR-20: ``executor_disabled`` answers HTTP 503.
    assert refused.status_code == 503, refused.text
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == [
        "executor_disabled"
    ]
    after = client.get(f"/actions/{action['id']}", headers=auth_headers).json()
    assert after == before
    assert after["undone_at"] is None
    assert after["undoable"] is True
    assert executions_of(action["id"]) == []


def test_a_call_that_never_reported_what_it_left_is_refused(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """Deny-by-default reaches the comparison too, not just the snapshot.

    @spec ACTION-EXECUTOR-11: a sealed record that never reported the version it
    left gives the platform a state to restore and nothing to compare the live
    version against, so it is refused ``refused_unversioned`` and no
    granted-undo audit row is written.
    """

    action = _sealed(client, auth_headers, tmp_path, post_version=None)
    assert action["undoable"] is False

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 409
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == [
        "refused_unversioned"
    ]
    # @spec ACTION-EXECUTOR-3: every refusal writes its audit row and creates
    # no execution.
    assert executions_of(action["id"]) == []


def test_a_prose_reply_is_refused_with_the_reason_it_carried(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """The receipt's stated reason and the refusal's reason are the same sentence.

    @spec ACTION-EXECUTOR-11: no envelope at all is ``refused_unsealed``.
    """

    action = _sealed(
        client,
        auth_headers,
        tmp_path,
        prior_state=None,
        post_state=None,
        target=None,
        result=None,
        detail="restarting pods cannot be undone",
    )

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 409
    assert response.json()["detail"] == "restarting pods cannot be undone"
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == ["refused_unsealed"]
    assert executions_of(action["id"]) == []


def test_a_failed_call_is_refused(client: Any, auth_headers: Any, tmp_path: Path) -> None:
    """Undoing a call that did not happen would be a write, not a restore."""

    action = _sealed(client, auth_headers, tmp_path, failed=True)

    assert _undo(client, auth_headers, action["id"]).status_code == 409
    entries = _audit(client, auth_headers, action["id"])
    assert len(entries) == 1
    assert entries[0]["authorized"] is False
    assert executions_of(action["id"]) == []


@pytest.mark.usefixtures("executor_enabled")
def test_an_undo_is_authorized_once(client: Any, auth_headers: Any, tmp_path: Path) -> None:
    """@spec ACTION-EXECUTOR-3: a ruling while a restore of the same action is
    live refuses ``refused_restore_in_flight`` and requests no second restore.

    The record is not marked undone at ruling (ACTION-EXECUTOR-11), so the
    second ruling is stopped by the live execution, not by ``undone_at``.
    """

    action = _sealed(client, auth_headers, tmp_path)
    assert _undo(client, auth_headers, action["id"]).status_code == 202

    second = _undo(client, auth_headers, action["id"])

    assert second.status_code == 409, second.text
    _holds_no_snapshot(second.text)
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == [
        "authorized",
        "refused_restore_in_flight",
    ]
    assert len(executions_of(action["id"])) == 1


@pytest.mark.usefixtures("executor_enabled")
def test_two_stale_concurrent_rulings_request_one_restore(
    client: Any, auth_headers: Any, tmp_path: Path
) -> None:
    """@spec ACTION-EXECUTOR-3: "a concurrent second undo is refused
    ``refused_restore_in_flight``".

    Both real Postgres sessions read the record before either rules, so both
    pass every derived check; the partial unique index on live restores
    (ACTION-EXECUTOR-2) arbitrates, and the route turns the loss into an
    audited 409 with no snapshot, never a second execution. The eventual loser
    begins its transaction first, since PostgreSQL ``now()`` is
    transaction-start time, not audit-insert time.
    """

    action_id = uuid.UUID(_sealed(client, auth_headers, tmp_path)["id"])

    async def contend() -> tuple[dict[str, Any], int, str]:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as first_session, sessions() as second_session:
                second = await second_session.get(AgentAction, action_id)
                await asyncio.sleep(0.02)
                first = await first_session.get(AgentAction, action_id)
                assert first is not None and second is not None
                ruling = await undo_action(
                    action_id,
                    ActionUndo(),
                    first_session,
                    lambda approval, binding: None,
                    principal=_operator("U-first"),
                )
                with pytest.raises(HTTPException) as refused:
                    await undo_action(
                        action_id,
                        ActionUndo(),
                        second_session,
                        lambda approval, binding: None,
                        principal=_operator("U-second"),
                    )
                return (
                    ruling.model_dump(mode="json"),
                    refused.value.status_code,
                    str(refused.value.detail),
                )
        finally:
            await engine.dispose()

    ruling, status_code, detail = asyncio.run(contend())
    _holds_no_snapshot(json.dumps(ruling))
    _holds_no_snapshot(detail)
    assert status_code == 409
    assert [entry["action"] for entry in _audit(client, auth_headers, str(action_id))] == [
        "authorized",
        "refused_restore_in_flight",
    ]
    executions = executions_of(str(action_id))
    assert len(executions) == 1
    assert ruling["execution_id"] == str(executions[0]["id"])
    assert executions[0]["requested_by"] == "U-first"


def test_a_legacy_cleartext_row_is_refused_unsealed_and_granted_nothing(
    client: Any, auth_headers: Any
) -> None:
    """@spec ACTION-EXECUTOR-11: "a legacy cleartext row is refused ``refused_unsealed``".

    The cleartext rule would have authorized this one: the observation matches
    what the call left. The ruling now follows ``undoable``, so it refuses with
    the code and writes no granted-undo audit row, and the record is not claimed.
    """

    action = _record(client, auth_headers)
    assert action["undoable"] is False

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 409
    entries = _audit(client, auth_headers, action["id"])
    assert [e["action"] for e in entries] == ["refused_unsealed"]
    assert not any(e["authorized"] for e in entries)
    after = client.get(f"/actions/{action['id']}", headers=auth_headers).json()
    assert after["undone_at"] is None
    assert after["undone_by"] is None


def test_an_unknown_action_is_a_404(client: Any, auth_headers: Any) -> None:
    response = _undo(client, auth_headers, str(uuid.uuid4()))

    assert response.status_code == 404
    assert response.json()["detail"] == "action not found"


def test_undoing_requires_a_principal(client: Any) -> None:
    assert client.post(f"/actions/{uuid.uuid4()}/undo", json={}).status_code == 401
