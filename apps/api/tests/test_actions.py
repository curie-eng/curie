"""Recording what an agent did to the world (ADR-0117), against real Postgres.

The worker has no database of its own -- it persists an approval by POSTing to
this API, and it records an action the same way. So this is the surface the
kernel writes through, and the two ACI frames of one call arrive here as two
requests: a create when the call was made, and a completion when its result
came back.

What this file holds:

1. **One call is one row, under redelivery.** The worker redelivers at least
   once (ADR-0013), so a replayed turn must adopt the record it already wrote.
2. **Reversibility is deny-by-default.** ``undoable`` is derived from what the
   record actually holds, so a completion that carried no prior state produces a
   record that says it cannot be undone, with no connector declaring anything.
   Under the sealed rule (ACTION-EXECUTOR-11) a cleartext prior state is history,
   not a snapshot: it is stored as reported and never makes a record undoable.
   ``test_action_undoable_ingredients.py`` holds the positive case.
3. **A completion lands once.** Recording the same result twice must not
   overwrite a record with a second, later account of the same call.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.crud import actions as crud_actions
from curie_api.models import AgentAction
from curie_api.schemas.actions import ActionComplete
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db")


def _open_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "conversation_id": "C1",
        "call_id": "toolu_01",
        "tool": "scale_deployment",
        "arguments": {"name": "api", "replicas": 10},
        "detail": "non-idempotent tool executed",
        "dedupe_key": f"event-{uuid.uuid4()}:toolu_01",
    }
    body.update(overrides)
    return body


def _complete_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "failed": False,
        "result": {"ok": True, "summary": "scaled 3 to 10"},
        "prior_state": {"spec": {"replicas": 3}},
        "post_state": {"spec": {"replicas": 10}},
        "target": {"kind": "Deployment", "namespace": "public", "name": "api"},
        "detail": "non-idempotent tool completed",
    }
    body.update(overrides)
    return body


def test_recording_a_call_creates_a_pending_record(client: Any, auth_headers: Any) -> None:
    response = client.post("/actions", json=_open_body(), headers=auth_headers)

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "pending"
    assert body["arguments"] == {"name": "api", "replicas": 10}
    # Nothing has come back yet, so there is nothing to put back.
    assert body["undoable"] is False


def test_a_redelivered_turn_adopts_the_record_it_already_wrote(
    client: Any, auth_headers: Any
) -> None:
    body = _open_body()

    first = client.post("/actions", json=body, headers=auth_headers)
    second = client.post("/actions", json=body, headers=auth_headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    listed = client.get("/actions", params={"conversation_id": "C1"}, headers=auth_headers)
    assert len(listed.json()) == 1


def test_a_completed_call_with_a_cleartext_prior_state_is_stored_but_not_undoable(
    client: Any, auth_headers: Any
) -> None:
    """@spec ACTION-EXECUTOR-11: legacy cleartext rows are not undoable.

    The completion still lands in full -- status, prior state, completion time --
    because the record is the history of the call. What it no longer earns is an
    undo: a cleartext ``prior_state`` is not a sealed envelope (ACTION-EXECUTOR-9),
    and this record carries no version, digest, capability or custody either.
    """

    action_id = client.post("/actions", json=_open_body(), headers=auth_headers).json()["id"]

    response = client.post(
        f"/actions/{action_id}/complete", json=_complete_body(), headers=auth_headers
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["prior_state"] == {"spec": {"replicas": 3}}
    assert body["post_state"] == {"spec": {"replicas": 10}}
    assert body["target"] == {"kind": "Deployment", "namespace": "public", "name": "api"}
    assert body["completed_at"] is not None
    assert body["undoable"] is False


def test_a_call_that_reported_no_prior_state_is_not_undoable(
    client: Any, auth_headers: Any
) -> None:
    """The connector answered in prose. Deny-by-default, with a stated reason."""

    action_id = client.post("/actions", json=_open_body(), headers=auth_headers).json()["id"]

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(result=None, prior_state=None, target=None, detail="restarted"),
        headers=auth_headers,
    )

    assert response.json()["status"] == "succeeded"
    assert response.json()["undoable"] is False
    assert response.json()["detail"] == "restarted"


def test_a_completion_that_reported_nothing_stores_sql_null_not_json_null(
    client: Any, auth_headers: Any
) -> None:
    """An unreported field stays absent in the row, not the JSON value ``null``.

    Python reads both back as ``None``, so only SQL can tell them apart. A
    ledger query such as ``prior_state IS NOT NULL`` must not count a record
    with nothing to restore as holding a prior state (ADR-0117).
    """

    action_id = client.post("/actions", json=_open_body(), headers=auth_headers).json()["id"]
    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(result=None, prior_state=None, post_state=None, target=None),
        headers=auth_headers,
    )
    assert response.status_code == 200

    async def stored_nulls() -> tuple[Any, ...]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            "SELECT result IS NULL, prior_state IS NULL,"
                            " post_state IS NULL, target IS NULL"
                            " FROM curie.agent_actions WHERE id = :id"
                        ),
                        {"id": uuid.UUID(action_id)},
                    )
                ).one()
                return tuple(row)
        finally:
            await engine.dispose()

    assert asyncio.run(stored_nulls()) == (True, True, True, True)


def test_a_failed_call_is_not_undoable(client: Any, auth_headers: Any) -> None:
    """Undoing a call that did not happen would be a write, not a restore."""

    action_id = client.post("/actions", json=_open_body(), headers=auth_headers).json()["id"]

    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(failed=True),
        headers=auth_headers,
    )

    assert response.json()["status"] == "failed"
    assert response.json()["undoable"] is False


def test_a_completion_lands_once(client: Any, auth_headers: Any) -> None:
    """A redelivered completion must not rewrite the account of a call."""

    action_id = client.post("/actions", json=_open_body(), headers=auth_headers).json()["id"]
    client.post(f"/actions/{action_id}/complete", json=_complete_body(), headers=auth_headers)

    second = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(prior_state={"spec": {"replicas": 99}}),
        headers=auth_headers,
    )

    assert second.status_code == 200
    assert second.json()["prior_state"] == {"spec": {"replicas": 3}}


def test_two_sessions_with_a_stale_pending_record_cannot_replace_the_first_completion(
    client: Any, auth_headers: Any
) -> None:
    """A delayed completion read before its rival committed must adopt the winner.

    The two real Postgres sessions both load ``pending`` before either writes.
    Serializing their commits afterwards makes the race deterministic: a plain
    ORM read-modify-write would let the stale second session replace the first.
    """

    opened = client.post("/actions", json=_open_body(), headers=auth_headers)
    action_id = uuid.UUID(opened.json()["id"])

    async def contend() -> tuple[AgentAction, AgentAction, AgentAction]:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as first_session, sessions() as second_session:
                first = await first_session.get(AgentAction, action_id)
                second = await second_session.get(AgentAction, action_id)
                assert first is not None and second is not None
                assert first.status == second.status == "pending"
                winner = await crud_actions.complete_action(
                    first_session,
                    first,
                    ActionComplete.model_validate(_complete_body(prior_state={"winner": 1})),
                )
                loser = await crud_actions.complete_action(
                    second_session,
                    second,
                    ActionComplete.model_validate(_complete_body(prior_state={"loser": 2})),
                )
                async with sessions() as check_session:
                    stored = await check_session.get(AgentAction, action_id)
                    assert stored is not None
                    return winner, loser, stored
        finally:
            await engine.dispose()

    winner, loser, stored = asyncio.run(contend())
    assert winner.prior_state == {"winner": 1}
    assert loser.prior_state == {"winner": 1}
    assert stored.prior_state == {"winner": 1}
    assert stored.result == {"ok": True, "summary": "scaled 3 to 10"}


def test_a_conversation_reads_back_its_actions_in_order(client: Any, auth_headers: Any) -> None:
    """The receipt is per turn, so the listing is what renders it."""

    client.post("/actions", json=_open_body(call_id="toolu_01"), headers=auth_headers)
    client.post("/actions", json=_open_body(call_id="toolu_02"), headers=auth_headers)
    client.post(
        "/actions", json=_open_body(conversation_id="C2", call_id="toolu_03"), headers=auth_headers
    )

    listed = client.get("/actions", params={"conversation_id": "C1"}, headers=auth_headers).json()

    assert [a["call_id"] for a in listed] == ["toolu_01", "toolu_02"]


def test_an_unknown_action_is_a_404(client: Any, auth_headers: Any) -> None:
    """Asserts the MESSAGE, not just the code.

    An unrouted path is also a 404, so a status-only assertion here would pass
    against an API that has no actions surface at all.
    """

    missing = uuid.uuid4()

    fetched = client.get(f"/actions/{missing}", headers=auth_headers)
    completed = client.post(
        f"/actions/{missing}/complete", json=_complete_body(), headers=auth_headers
    )

    assert fetched.status_code == 404
    assert fetched.json()["detail"] == "action not found"
    assert completed.status_code == 404
    assert completed.json()["detail"] == "action not found"


def test_recording_requires_the_api_key(client: Any) -> None:
    assert client.post("/actions", json=_open_body()).status_code == 401


def test_a_completion_stores_the_post_version_it_reports(client: Any, auth_headers: Any) -> None:
    """@spec ACTION-EXECUTOR-9 @spec ACTION-EXECUTOR-11: the API half of ``post_version``.

    The worker records the version a sealed write left as ``post_version`` on
    the completion; the ruling (ACTION-EXECUTOR-11) and the observation
    comparison (ACTION-EXECUTOR-15) read it from the row, so the completion
    must accept and store it rather than drop it as an unknown field.
    """

    action_id = client.post("/actions", json=_open_body(), headers=auth_headers).json()["id"]
    envelope = {
        "sealed": "curie.snapshot.v1",
        "kid": "example-key-2026-10",
        "ciphertext": "ZXhhbXBsZSBzZWFsZWQgc25hcHNob3QgYnl0ZXMgMDE=",
    }
    response = client.post(
        f"/actions/{action_id}/complete",
        json=_complete_body(prior_state=envelope, post_version="rv-1041"),
        headers=auth_headers,
    )
    assert response.status_code == 200

    async def stored() -> tuple[Any, ...]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            "SELECT post_version, prior_state"
                            " FROM curie.agent_actions WHERE id = :id"
                        ),
                        {"id": uuid.UUID(action_id)},
                    )
                ).one()
                return tuple(row)
        finally:
            await engine.dispose()

    assert asyncio.run(stored()) == ("rv-1041", envelope)
    assert (
        ActionComplete.model_validate({"post_version": "rv-1041"}).model_dump().get("post_version")
        == "rv-1041"
    )
