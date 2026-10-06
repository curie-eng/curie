"""Ruling on an undo (ADR-0117 decision 4), against real Postgres.

The API rules; it does not execute. Nothing in the platform can reach a connector
today, so this endpoint decides whether a restore is permitted and hands back the
call to make. Keeping the two apart is what makes deferring the executor safe: a
refusal is recorded and returned before anything could act on it.

The rule this file exists for is the one ADR-0117 says the feature lives or dies
on. A blind restore silently reverts a human's manual fix, which turns an undo
button into a way for the platform to fight the operator. So a restore is refused
whenever the world no longer looks like what the action left -- and, just as
importantly, whenever the platform cannot tell.

Every refusal writes an audit entry before it raises. A refusal nobody can read
afterwards is a bug report the operator never gets.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.crud import actions as crud_actions
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


def _undo(client: Any, headers: Any, action_id: str, **body: Any) -> Any:
    payload: dict[str, Any] = {"actor": "U-operator", "observed_state": LEFT}
    payload.update(body)
    return client.post(f"/actions/{action_id}/undo", json=payload, headers=headers)


def _audit(client: Any, headers: Any, action_id: str) -> list[dict[str, Any]]:
    return list(client.get(f"/actions/{action_id}/audit", headers=headers).json())


def test_an_untouched_world_authorizes_the_restore(client: Any, auth_headers: Any) -> None:
    """The live state still matches what the action left, so putting it back is safe."""

    action = _record(client, auth_headers)

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 200
    ruling = response.json()
    # The ruling hands back the call to make. The API cannot reach a connector,
    # so naming the restore IS the output.
    assert ruling["restore"]["target"] == TARGET
    assert ruling["restore"]["prior_state"] == PRIOR
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == ["authorized"]


def test_a_moved_world_is_refused_with_both_states_named(
    client: Any, auth_headers: Any
) -> None:
    """The rule the feature lives on: a human set it to 7 by hand after the agent acted."""

    action = _record(client, auth_headers)

    response = _undo(client, auth_headers, action["id"], observed_state={"spec": {"replicas": 7}})

    assert response.status_code == 409
    entry = _audit(client, auth_headers, action["id"])[0]
    assert entry["action"] == "refused_conflict"
    assert entry["authorized"] is False
    # Both states, because an operator has to see that their own fix is what
    # stopped it -- not a platform malfunction.
    assert entry["evidence"] == {"left": LEFT, "observed": {"spec": {"replicas": 7}}}


def test_a_refused_undo_changes_nothing(client: Any, auth_headers: Any) -> None:
    """`an undo either restores the recorded state or changes nothing at all`.

    @spec ACTION-EXECUTOR-11: this record's ``prior_state`` is cleartext, so it
    is not ``undoable`` under the sealed rule before or after the refusal. What
    the refusal must not move is the whole record as the API reads it, so the
    read after is compared with the read before, field for field.
    """

    action = _record(client, auth_headers)
    before = client.get(f"/actions/{action['id']}", headers=auth_headers).json()

    refused = _undo(
        client, auth_headers, action["id"], observed_state={"spec": {"replicas": 7}}
    )

    # Assert the refusal happened before asserting nothing moved -- otherwise
    # this passes against an API with no undo endpoint at all.
    assert refused.status_code == 409
    after = client.get(f"/actions/{action['id']}", headers=auth_headers).json()
    assert after == before
    assert after["undone_at"] is None
    assert after["undone_by"] is None
    assert after["prior_state"] == PRIOR
    assert after["undoable"] is False


def test_an_unseen_world_is_refused_rather_than_assumed(
    client: Any, auth_headers: Any
) -> None:
    """No live state supplied means the platform cannot tell, which is not consent.

    412 rather than 400: the caller's request is well formed, the precondition
    the rule needs is simply absent.
    """

    action = _record(client, auth_headers)

    response = _undo(client, auth_headers, action["id"], observed_state=None)

    assert response.status_code == 412
    assert _audit(client, auth_headers, action["id"])[0]["action"] == "refused_unobserved"


def test_a_call_that_never_reported_what_it_left_is_refused(
    client: Any, auth_headers: Any
) -> None:
    """Deny-by-default reaches the comparison too, not just the snapshot.

    A connector that reports `prior` but not `post` leaves the platform holding a
    state to restore and no way to know whether restoring it is safe.
    """

    action = _record(client, auth_headers, post_state=None)

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 409
    assert _audit(client, auth_headers, action["id"])[0]["action"] == "refused_uncomparable"


def test_a_prose_reply_is_refused_with_the_reason_it_carried(
    client: Any, auth_headers: Any
) -> None:
    """The receipt's stated reason and the refusal's reason are the same sentence."""

    action = _record(
        client,
        auth_headers,
        prior_state=None,
        post_state=None,
        target=None,
        result=None,
        detail="restarting pods cannot be undone",
    )

    response = _undo(client, auth_headers, action["id"])

    assert response.status_code == 409
    assert response.json()["detail"] == "restarting pods cannot be undone"


def test_a_failed_call_is_refused(client: Any, auth_headers: Any) -> None:
    """Undoing a call that did not happen would be a write, not a restore."""

    action = _record(client, auth_headers, failed=True)

    assert _undo(client, auth_headers, action["id"]).status_code == 409


def test_an_undo_is_authorized_once(client: Any, auth_headers: Any) -> None:
    """A second ruling on a claimed record must not authorize a second restore."""

    action = _record(client, auth_headers)
    _undo(client, auth_headers, action["id"])

    second = _undo(client, auth_headers, action["id"])

    assert second.status_code == 409
    assert [e["action"] for e in _audit(client, auth_headers, action["id"])] == [
        "authorized",
        "refused_already_undone",
    ]


def test_two_sessions_with_a_stale_unclaimed_record_cannot_both_claim_undo(
    client: Any, auth_headers: Any
) -> None:
    """Only one stale claimant may receive a restore authorization.

    Both real Postgres sessions read the unclaimed record before either writes.
    The second commit deliberately follows the first to reproduce the lost-
    update interleaving without a scheduler-dependent timing window.
    """

    action_id = uuid.UUID(_record(client, auth_headers)["id"])

    async def contend() -> tuple[AgentAction | None, AgentAction | None, AgentAction]:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as first_session, sessions() as second_session:
                first = await first_session.get(AgentAction, action_id)
                second = await second_session.get(AgentAction, action_id)
                assert first is not None and second is not None
                assert first.undone_at is None and second.undone_at is None
                winner = await crud_actions.claim_action_undo(first_session, first, actor="U-first")
                await first_session.commit()
                loser = await crud_actions.claim_action_undo(
                    second_session, second, actor="U-second"
                )
                await second_session.commit()
                async with sessions() as check_session:
                    stored = await check_session.get(AgentAction, action_id)
                    assert stored is not None
                    return winner, loser, stored
        finally:
            await engine.dispose()

    winner, loser, stored = asyncio.run(contend())
    assert winner is not None
    assert loser is None
    assert stored.undone_by == "U-first"
    assert stored.undone_at is not None


def test_stale_undo_request_gets_a_refusal_not_a_second_restore(
    client: Any, auth_headers: Any
) -> None:
    """The API route must turn a lost CAS into an audited 409 without a payload."""

    action_id = uuid.UUID(_record(client, auth_headers)["id"])

    async def contend() -> tuple[dict[str, Any], int]:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as first_session, sessions() as second_session:
                second = await second_session.get(AgentAction, action_id)
                # The eventual loser begins its transaction first. PostgreSQL
                # now() is transaction-start time, not audit-insert time.
                await asyncio.sleep(0.02)
                first = await first_session.get(AgentAction, action_id)
                assert first is not None and second is not None
                # Identity-map reads inside undo_action retain the same stale
                # objects while Postgres arbitrates the actual UPDATE.
                ruling = await undo_action(
                    action_id,
                    ActionUndo(actor="U-first", observed_state=LEFT),
                    first_session,
                    lambda approval, binding: None,
                )
                with pytest.raises(HTTPException) as refused:
                    await undo_action(
                        action_id,
                        ActionUndo(actor="U-second", observed_state=LEFT),
                        second_session,
                        lambda approval, binding: None,
                    )
                return ruling.restore.model_dump(), refused.value.status_code
        finally:
            await engine.dispose()

    restore, status_code = asyncio.run(contend())
    assert restore == {"target": TARGET, "prior_state": PRIOR}
    assert status_code == 409
    assert [entry["action"] for entry in _audit(client, auth_headers, str(action_id))] == [
        "authorized",
        "refused_already_undone",
    ]


def test_an_unknown_action_is_a_404(client: Any, auth_headers: Any) -> None:
    response = _undo(client, auth_headers, str(uuid.uuid4()))

    assert response.status_code == 404
    assert response.json()["detail"] == "action not found"


def test_undoing_requires_the_api_key(client: Any) -> None:
    assert client.post(f"/actions/{uuid.uuid4()}/undo", json={"actor": "U1"}).status_code == 401
