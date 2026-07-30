"""Durable approvals (#244): real Postgres + real Valkey round-trip.

Exercises the record lifecycle against the disposable per-run database and the
compose Valkey: idempotent creation on dedupe_key, the resolve-once
compare-and-set (losers get 409 naming the winner, including under genuine
concurrency), SLA expiry (410), and the resume turn enqueued onto a
test-isolated runs stream as a valid frozen-contract QueuedTurn.
"""

import asyncio
import contextlib
import json
import os
import secrets
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import QueuedTurn
from alembic import command
from alembic.config import Config
from curie_api import crud
from curie_api.config import get_settings
from curie_api.deps import get_approver_sets
from curie_api.main import create_app
from curie_api.models import Approval
from curie_api.resumequeue import ResumeQueue
from curie_api.resumereconciler import ResumeReconciler
from curie_api.sandbox_token import mint
from curie_api.slack_approvers import SlackApproverSetSelector
from curie_api.slack_usergroups import SlackUserGroupClient
from curie_api.sweeper import run_expiry_sweeper, sweep_expired_approvals
from curie_test_support.valkey import (
    connect_or_skip,
)
from fastapi.testclient import TestClient
from sqlalchemy import make_url, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# The migration test (18) drives alembic directly against the disposable DB the
# conftest provisions, so it needs the same script location conftest uses.
ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"


@pytest.fixture
def runs_stream() -> Iterator[str]:
    """A per-test runs stream, so resolutions never feed the shared compose
    worker's real ``curie:runs`` consumer group."""

    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def approvals_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """A TestClient whose app was built after the stream override, so its
    ResumeQueue targets the isolated stream."""

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    client.close()


def _payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": "U1",
        "summary": "Give ACME a 20% discount",
        "reply_channel": "C1",
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
    }
    base.update(overrides)
    return base


def _seed_raw_approval(approval_id: uuid.UUID, summary: str) -> None:
    """Insert an approval with raw SQL, naming only the pre-#544 columns.

    Used by the migration test to seed rows as they exist BEFORE the provenance
    columns are added, so the backfill has something real to classify.
    """

    async def _run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals (id, conversation_id, author, "
                        "summary, reply_channel, reply_placeholder, dedupe_key, status) "
                        "VALUES (:id, :conv, :author, :summary, :ch, :ph, :dedupe, "
                        "'pending')"
                    ),
                    {
                        "id": approval_id,
                        "conv": f"th-{approval_id.hex[:8]}",
                        "author": "U1",
                        "summary": summary,
                        "ch": "C1",
                        "ph": "p-1",
                        "dedupe": uuid.uuid4().hex,
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(_run())


def _read_provenance(approval_id: uuid.UUID) -> tuple[str | None, str | None]:
    """(gate_kind, granted_tool) straight from the row, post-migration."""

    async def _run() -> tuple[str | None, str | None]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT gate_kind, granted_tool FROM curie.approvals WHERE id = :id"),
                    {"id": approval_id},
                )
                row = result.first()
                assert row is not None, f"approval {approval_id} vanished"
                return row[0], row[1]
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def _read_resumed_at(approval_id: str) -> datetime | None:
    """Read ``Approval.resumed_at`` straight from the DB (it is not exposed on
    ``ApprovalOut``). A fresh engine keeps this safe under ``asyncio.run`` from
    the sync test body -- the app's engine is bound to the TestClient's portal
    loop and cannot be reused across event loops."""

    async def _run() -> datetime | None:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessionmaker() as session:
                approval = await session.get(Approval, uuid.UUID(approval_id))
                return None if approval is None else approval.resumed_at
        finally:
            await engine.dispose()

    return asyncio.run(_run())


def test_resolve_endpoint_stays_200_when_enqueue_fails(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#411: a Valkey blip on the resume enqueue must not dead-end the resolve.

    The resolution itself committed (CAS won, audit written), so the endpoint
    returns 200 with the resolved record, leaves ``resumed_at`` NULL for the
    reconciler to retry, and enqueues nothing. A retry still 409s (CAS intact) --
    the owed wake belongs to the reconciler, never to a client retry (which used
    to 500 then 409-forever).
    """

    created = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()

    async def _boom(_turn: Any) -> str:
        raise RuntimeError("valkey unreachable")

    monkeypatch.setattr(approvals_client.app.state.resume_queue, "enqueue", _boom)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["status"] == "approved"

    # No resume turn on the stream, and the record still owes its wake.
    assert valkey.xrange(runs_stream) == []
    assert _read_resumed_at(created["id"]) is None

    # The CAS is untouched: a retry loses as a normal race, it does not re-enqueue.
    retry = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U2", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert retry.status_code == 409
    assert valkey.xrange(runs_stream) == []


@pytest.fixture
def reconciler_disabled_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """A TestClient built with the resume reconciler DISABLED, so a failed inline
    enqueue has no backstop. ``raise_server_exceptions=False`` lets the resulting
    500 be asserted as a response rather than re-raised into the test body."""

    os.environ["RESUME_RECONCILER_ENABLED"] = "false"
    get_settings.cache_clear()
    try:
        with TestClient(create_app(), raise_server_exceptions=False) as test_client:
            yield test_client
    finally:
        os.environ.pop("RESUME_RECONCILER_ENABLED", None)
        get_settings.cache_clear()


def test_resolve_reraises_when_reconciler_disabled(
    reconciler_disabled_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#411 hardening: returning 200 on a failed resume enqueue is only safe when a
    reconciler will recover the owed wake. With ``resume_reconciler_enabled=false``
    there is no backstop, so the endpoint must surface the enqueue failure (500)
    rather than silently stranding the suspended session behind a 200.

    The resolution CAS still committed (approved, audit written), leaving the record
    resolved-but-unresumed with nothing on the stream -- exactly the state a
    disabled deployment must not hide. (The default-enabled 200 path is pinned by
    ``test_resolve_endpoint_stays_200_when_enqueue_fails``.)
    """

    created = reconciler_disabled_client.post(
        "/approvals", json=_payload(), headers=auth_headers
    ).json()

    async def _boom(_turn: Any) -> str:
        raise RuntimeError("valkey unreachable")

    monkeypatch.setattr(reconciler_disabled_client.app.state.resume_queue, "enqueue", _boom)

    resolved = reconciler_disabled_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 500, resolved.text

    # The CAS committed -- the record is resolved -- but its wake is unrecoverably
    # owed (no reconciler) and nothing reached the stream.
    record = reconciler_disabled_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "approved"
    assert _read_resumed_at(created["id"]) is None
    assert valkey.xrange(runs_stream) == []


def test_happy_path_resolve_marks_resumed(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """#411: the normal successful resolve now records the wake as delivered by
    setting ``resumed_at``, so the reconciler's work-list excludes it. This adds
    to the resolve-once coverage without weakening the existing assertions."""

    created = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 200, resolved.text

    # The resume turn is on the stream (the existing #244 contract) ...
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"

    # ... and the record is now marked resumed (the new #411 contract).
    assert _read_resumed_at(created["id"]) is not None


def test_create_get_list_round_trip(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    payload = _payload()
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "pending"
    assert body["summary"] == payload["summary"]
    assert body["resolved_by"] is None

    got = approvals_client.get(f"/approvals/{body['id']}", headers=auth_headers)
    assert got.status_code == 200
    assert got.json() == body

    listed = approvals_client.get(
        "/approvals",
        params={"status_filter": "pending", "conversation_id": payload["conversation_id"]},
        headers=auth_headers,
    )
    assert [a["id"] for a in listed.json()] == [body["id"]]


def test_create_tolerates_unknown_field_from_a_newer_worker(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The route is a CONSUMER of the wire, so it ignores fields it does not model.

    A newer worker adding an optional field must not 422 against an older API
    during a rolling deploy -- that is what makes a new optional field a patch
    bump. Exercised through the real route, because the strictness this guards
    lives in FastAPI's own body validation, not in the model constructed by hand.
    """

    payload = _payload(future_field="from a newer worker")
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["status"] == "pending"
    assert body["summary"] == payload["summary"]
    # The unknown field is ignored, not persisted onto the durable record.
    assert "future_field" not in body


def test_create_still_rejects_an_invalid_modelled_field(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Tolerance is for UNKNOWN fields only; a modelled field's constraint still binds."""

    resp = approvals_client.post("/approvals", json=_payload(author=""), headers=auth_headers)
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"][0]["loc"] == ["body", "author"]


def test_create_is_idempotent_on_dedupe_key(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    payload = _payload()
    first = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert first.status_code == 201
    replay = approvals_client.post("/approvals", json=payload, headers=auth_headers)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]


def test_resolve_once_and_enqueue_resume_turn(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    payload = _payload(reply_endpoint="http://localhost:9999/api/")
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={
            "decision": "approved",
            "resolved_by": "U9",
            "note": "ship it",
            "actor_channel": "C1",
        },
        headers=auth_headers,
    )
    assert resolved.status_code == 200, resolved.text
    body = resolved.json()
    assert body["status"] == "approved"
    assert body["resolved_by"] == "U9"
    assert body["resolved_at"] is not None

    # The resume turn landed on the isolated runs stream as a valid frozen
    # QueuedTurn, addressed back to the requesting thread and placeholder.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == "U9"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder == "p-1"
    assert turn.reply_handle.endpoint == "http://localhost:9999/api/"
    assert "approved by U9" in turn.text
    assert "ship it" in turn.text

    # The loser of the claim race is told who resolved it.
    second = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "rejected", "resolved_by": "U2", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert second.status_code == 409
    assert "already resolved by U9" in second.json()["detail"]
    # And no second resume turn was enqueued.
    assert len(valkey.xrange(runs_stream)) == 1


def test_concurrent_resolvers_yield_exactly_one_winner(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()

    def attempt(actor: str) -> int:
        response = approvals_client.post(
            f"/approvals/{created['id']}/resolve",
            json={"decision": "approved", "resolved_by": actor, "actor_channel": "C1"},
            headers=auth_headers,
        )
        return response.status_code

    # Actors distinct from the record's author (U1), so none is blocked as
    # self-approval and the race is purely over the pending claim.
    with ThreadPoolExecutor(max_workers=4) as pool:
        codes = list(pool.map(attempt, [f"U_race_{i}" for i in range(4)]))

    assert sorted(codes) == [200, 409, 409, 409]
    assert len(valkey.xrange(runs_stream)) == 1


def test_expired_approval_resolve_returns_410_and_resumes(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    payload = _payload(expires_in_seconds=1)
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()
    assert created["expires_at"] is not None
    time.sleep(1.1)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 410
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert got.json()["status"] == "expired"

    # #412: a resolver landing after the SLA lapsed (but before the next
    # sweep) must not strand the session. The late resolve still gets 410
    # and the record still flips to expired, but it now enqueues the same
    # deterministic expiry resume turn the sweeper would have produced, so
    # the conversation resumes down its timeout branch instead of hanging.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == "system"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder == "p-1"
    assert "expired" in turn.text
    assert payload["summary"] in turn.text

    # #418: the wake made it onto the stream, so the record is marked resumed and
    # the reconciler owes it nothing. Enqueue-first-then-mark, the same ordering
    # the resolve path uses.
    assert _read_resumed_at(created["id"]) is not None


def test_unknown_approval_is_404(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = approvals_client.post(
        f"/approvals/{uuid.uuid4()}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert missing.status_code == 404


def test_requires_api_key(approvals_client: TestClient, clean_db: None) -> None:
    denied = approvals_client.post("/approvals", json=_payload())
    assert denied.status_code in (401, 403)


def test_scoped_state_token_cannot_resolve_an_approval(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The core #410 security assertion: a scoped sandbox "state" token is a
    least-privilege credential for the agent's own state namespace ONLY. It must
    NOT authorize resolving an approval -- otherwise a sandboxed agent could
    forward its own state token to /approvals/{id}/resolve and self-approve its
    own gated tool call, defeating the server-side authorizer (ADR-0010)."""

    created = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()
    resolve_url = f"/approvals/{created['id']}/resolve"

    scoped = mint(
        get_settings().api_key,
        agent=str(uuid.uuid4()),
        scope="state",
        exp=4102444800,  # 2100-01-01, valid at test time
    )
    denied = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers={"X-API-Key": scoped},
    )
    assert denied.status_code == 401, denied.text
    # The record is untouched by the rejected attempt.
    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"

    # The platform key still resolves it (no regression).
    ok = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert ok.status_code == 200, ok.text


def test_authorizer_blocks_non_member_and_self_approval(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The server-side authorizer (#246): channel membership is proven by the
    attempt's channel, self-approval is blocked unconditionally, and a denied
    attempt neither resolves the record nor enqueues a resume turn."""

    created = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()
    resolve_url = f"/approvals/{created['id']}/resolve"

    # Wrong channel: not an approver.
    wrong_channel = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C_OTHER"},
        headers=auth_headers,
    )
    assert wrong_channel.status_code == 403
    assert "not an approver" in wrong_channel.json()["detail"]

    # No channel evidence at all: not an approver.
    no_channel = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9"},
        headers=auth_headers,
    )
    assert no_channel.status_code == 403

    # Self-approval: blocked even from the right channel (the record's author
    # is U1, see _payload).
    self_approval = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U1", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert self_approval.status_code == 403
    assert "self-approval" in self_approval.json()["detail"]

    # The record is still pending and nothing was enqueued.
    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []

    # An authorized member still resolves it afterwards.
    ok = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert ok.status_code == 200
    assert len(valkey.xrange(runs_stream)) == 1


def test_bound_approver_is_accepted_and_requesting_channel_is_not(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """(16) The AC2 authority test: the direct inversion of the observed 403.

    An agent lives in the requesting channel C_LOCALDEV and declares a
    'deal-desk' route bound to C_BOUND, whose approvers are exactly [U_BOUND].
    A policy-gate approval is created WITH the resolved route -- which is the
    whole point of Decision B: the runner resolves the route against the
    manifest and refuses rather than letting route=null reach the API, where
    ADR-0034's channel-membership default (correct for genuinely generic
    approvals) silently widens authority to whoever happens to be in the
    requesting channel.

    The observed failure had exactly this polarity, reversed: the bound approver
    got a 403 and the requesting channel could resolve. Assert the inversion.
    """

    agent = approvals_client.post(
        "/agents",
        json={
            "name": f"deal-desk-{uuid.uuid4().hex[:8]}",
            "slack_channel": "C0LOCALDEV",
            "approval_routes": {
                "deal-desk": {
                    "channel": "C0BOUND01",
                    "approvers": {"users": ["U0BOUND01"]},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text

    created = approvals_client.post(
        "/approvals",
        json=_payload(
            agent_id=agent.json()["id"],
            author="U0AUTHOR1",
            reply_channel="C0LOCALDEV",
            route="deal-desk",
            card_channel="C0BOUND01",
            gate_kind="policy",
        ),
        headers=auth_headers,
    ).json()
    assert created["route"] == "deal-desk"
    assert created["gate_kind"] == "policy"
    resolve_url = f"/approvals/{created['id']}/resolve"

    # A member of the REQUESTING channel who is not a bound approver: refused.
    # This is the widening #544 is about -- today this is the path that succeeds.
    requesting = approvals_client.post(
        resolve_url,
        json={
            "decision": "approved",
            "resolved_by": "U0LOCAL01",
            "actor_channel": "C0LOCALDEV",
        },
        headers=auth_headers,
    )
    assert requesting.status_code == 403, (
        "authority must never widen to the requesting channel when the manifest "
        f"declared a route: {requesting.text}"
    )
    assert (
        approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers).json()["status"]
        == "pending"
    )
    assert valkey.xrange(runs_stream) == []

    # The BOUND approver, from the bound channel: accepted.
    bound = approvals_client.post(
        resolve_url,
        json={
            "decision": "approved",
            "resolved_by": "U0BOUND01",
            "actor_channel": "C0BOUND01",
        },
        headers=auth_headers,
    )
    assert bound.status_code == 200, (
        f"the manifest's declared approver must be able to approve: {bound.text}"
    )
    assert len(valkey.xrange(runs_stream)) == 1


def test_create_approval_persists_gate_kind_and_granted_tool(
    approvals_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """(17) The provenance columns round-trip through real Postgres.

    ``gate_kind`` answers "which path fired" (AC3 observability) and drives the
    worker's refusal; ``granted_tool`` is what is actually handed out. Both are
    written by the runner -- the only component that knows which tool
    ``can_use_tool`` denied -- and carried by the worker verbatim.
    """

    permission = approvals_client.post(
        "/approvals",
        json=_payload(
            summary='Tool call awaiting approval: Bash {"command": "deploy"}',
            gate_kind="permission",
            granted_tool="Bash",
        ),
        headers=auth_headers,
    )
    assert permission.status_code == 201, permission.text
    body = permission.json()
    assert body["gate_kind"] == "permission"
    assert body["granted_tool"] == "Bash"
    # Read back from the DB through GET, not from the create response.
    got = approvals_client.get(f"/approvals/{body['id']}", headers=auth_headers).json()
    assert got["gate_kind"] == "permission"
    assert got["granted_tool"] == "Bash"

    # A policy gate carries provenance but never authority (Decision A).
    policy = approvals_client.post(
        "/approvals",
        json=_payload(summary="Give ACME a 20% discount", gate_kind="policy"),
        headers=auth_headers,
    ).json()
    assert policy["gate_kind"] == "policy"
    assert policy["granted_tool"] is None

    # An old runner emits neither: both stay NULL, which is the rolling-deploy
    # window the worker's prefix fallback covers (edge case 7).
    legacy = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()
    assert legacy["gate_kind"] is None
    assert legacy["granted_tool"] is None


@contextlib.contextmanager
def _isolated_migration_db() -> Iterator[None]:
    """A throwaway database ALL to itself, for a test that downgrades/upgrades.

    The session ``_disposable_db`` is shared across every test in this file, so
    running ``alembic downgrade`` against it mid-suite disrupts siblings (the
    sweeper tests seed rows and count them). apps/api/CLAUDE.md is explicit:
    migrations are tested against a database of their own, never shared state.
    This provisions one, points DATABASE_URL + alembic at it for the body, and
    drops it after, restoring the session URL so no other test is perturbed.
    """

    base = make_url(get_settings().database_url)
    run_db = f"curie_test_mig_{secrets.token_hex(4)}"

    async def _admin(sql: str) -> None:
        conn = await asyncpg.connect(
            user=base.username,
            password=base.password,
            host=base.host,
            port=base.port,
            database="postgres",
        )
        try:
            await conn.execute(sql)
        finally:
            await conn.close()

    saved_url = os.environ.get("DATABASE_URL")
    asyncio.run(_admin(f'CREATE DATABASE "{run_db}"'))
    try:
        os.environ["DATABASE_URL"] = base.set(database=run_db).render_as_string(hide_password=False)
        get_settings.cache_clear()
        yield
    finally:
        if saved_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved_url
        get_settings.cache_clear()
        asyncio.run(_admin(f'DROP DATABASE IF EXISTS "{run_db}" WITH (FORCE)'))


def test_backfill_classifies_existing_rows() -> None:
    """(18) The migration backfills rows at rest, then round-trips.

    A prefixed summary is a genuine permission-gate block (the runner is the
    only writer of that reserved namespace), so it backfills to 'permission'
    plus the tool name parsed out of it. Everything else backfills to 'policy'
    with granted_tool NULL -- the safe direction: a pending policy approval
    created before this change becomes the conservative "no grant" case, never
    a surprise grant.

    Runs on its own database (see ``_isolated_migration_db``) so the downgrade
    never disturbs the shared session DB the other tests read.
    """

    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))

    with _isolated_migration_db():
        # Bring the fresh DB up to the full schema, then step back to BEFORE the
        # provenance migration (0015) to the state an existing deployment holds.
        # Target 0014 explicitly rather than a relative "-1": a later migration
        # (e.g. 0016) moving head would make "-1" stop short of undoing 0015, and
        # the backfill would never re-run on the seeded rows.
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "0014")
        permission_id = uuid.uuid4()
        policy_id = uuid.uuid4()
        _seed_raw_approval(permission_id, 'Tool call awaiting approval: Bash {"command": "deploy"}')
        _seed_raw_approval(policy_id, "Give ACME a 20% discount")
        command.upgrade(cfg, "head")

        assert _read_provenance(permission_id) == ("permission", "Bash")
        assert _read_provenance(policy_id) == ("policy", None)

        # And the revision round-trips cleanly rather than only migrating forward.
        command.downgrade(cfg, "-1")
        command.upgrade(cfg, "head")


def test_gate_kind_check_constraint_rejects_unknown_values() -> None:
    """(#544) The DB-layer guard on the security-load-bearing gate_kind column.

    ``gate_kind`` is trusted in a worker security branch (binding.py:
    ``if gate_kind == "policy": return None``), so the migration pins the column
    to the two literals the runner ever writes via ``ck_approvals_gate_kind``.
    The two literals and NULL (the rolling-window / old-runner case) are
    accepted; anything else is rejected by real Postgres. Runs on its own
    database (see ``_isolated_migration_db``) so the schema is untouched shared
    state.
    """

    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))

    async def _insert(gate_kind: str | None) -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals (id, conversation_id, "
                        "author, summary, reply_channel, reply_placeholder, "
                        "dedupe_key, status, gate_kind) VALUES (:id, :conv, "
                        ":author, :summary, :ch, :ph, :dedupe, 'pending', :gk)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "conv": "th-ck",
                        "author": "U1",
                        "summary": "s",
                        "ch": "C1",
                        "ph": "p-1",
                        "dedupe": uuid.uuid4().hex,
                        "gk": gate_kind,
                    },
                )
        finally:
            await engine.dispose()

    with _isolated_migration_db():
        command.upgrade(cfg, "head")
        # Accepted: the two literals plus NULL (NULL IN (...) is NULL, not
        # FALSE, so the CHECK passes -- the old-runner / pre-backfill case).
        asyncio.run(_insert("permission"))
        asyncio.run(_insert("policy"))
        asyncio.run(_insert(None))
        # Rejected: anything outside the closed set trips ck_approvals_gate_kind.
        with pytest.raises(IntegrityError):
            asyncio.run(_insert("bogus"))


def test_route_bound_approval_authorizes_against_card_channel(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """#247: when a route binding placed the card in another channel, THAT
    channel's members are the approvers; the requesting channel no longer is."""

    created = approvals_client.post(
        "/approvals",
        json=_payload(route="managers", card_channel="C_MGRS"),
        headers=auth_headers,
    ).json()
    assert created["route"] == "managers"
    assert created["card_channel"] == "C_MGRS"
    resolve_url = f"/approvals/{created['id']}/resolve"

    # The requesting channel (C1) is NOT the approvers' channel anymore.
    from_requesting = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert from_requesting.status_code == 403

    # A member of the route-bound channel resolves it.
    ok = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C_MGRS"},
        headers=auth_headers,
    )
    assert ok.status_code == 200
    assert len(valkey.xrange(runs_stream)) == 1


def test_audit_log_records_attempts_with_authorizer_snapshots(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """The #247 acceptance read-back: denied and resolved attempts each leave
    an append-only audit entry naming the actor, the channel evidence, and the
    authorizer snapshot that counted (or refused) them."""

    created = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()
    resolve_url = f"/approvals/{created['id']}/resolve"

    denied = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U_OUT", "actor_channel": "C_X"},
        headers=auth_headers,
    )
    assert denied.status_code == 403
    resolved = approvals_client.post(
        resolve_url,
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 200
    late = approvals_client.post(
        resolve_url,
        json={"decision": "rejected", "resolved_by": "U_LATE", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert late.status_code == 409

    audit = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers)
    assert audit.status_code == 200
    entries = audit.json()
    assert [e["action"] for e in entries] == ["denied", "resolved", "race_lost"]

    denied_entry, resolved_entry, race_entry = entries
    assert denied_entry["actor"] == "U_OUT"
    assert denied_entry["actor_channel"] == "C_X"
    assert denied_entry["authorized"] is False
    assert denied_entry["authorizer"] == "ChannelMembershipAuthorizer"
    assert "not an approver" in denied_entry["reason"]

    assert resolved_entry["actor"] == "U9"
    assert resolved_entry["authorized"] is True
    assert resolved_entry["decision"] == "approved"
    assert resolved_entry["authorizer"] == "ChannelMembershipAuthorizer"

    assert race_entry["actor"] == "U_LATE"
    assert "already resolved by U9" in race_entry["reason"]

    # The audit endpoint 404s for an unknown approval.
    missing = approvals_client.get(f"/approvals/{uuid.uuid4()}/audit", headers=auth_headers)
    assert missing.status_code == 404


# --- expiry sweeper (#412) ----------------------------------------------------
#
# These tests exercise the periodic sweeper that flips lapsed pending approvals
# to `expired` and enqueues a platform-authored resume turn, so the suspended
# session resumes down its timeout branch instead of stranding. Real Postgres +
# real Valkey, no mocks. Because `sweep_expired_approvals` is async and asyncpg
# connections are loop-bound, each test creates records via the SYNC HTTP client
# (they land in the disposable DB regardless of loop), then runs ONE
# `asyncio.run` scenario that builds its OWN async engine + sessionmaker + redis
# client + ResumeQueue against the same DB/stream, and asserts DB state via the
# sync HTTP client and stream contents via the sync `valkey` fixture. Real sleeps
# are avoided by creating records with `expires_in_seconds=1` and sweeping with
# an explicit `now` two seconds in the future (naive UTC, which also pins the
# timezone edge case).


def _naive_utc(seconds_ahead: float = 0.0) -> datetime:
    """Naive-UTC clock matching the sweeper's `now`/`expires_at` comparison."""

    return datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=seconds_ahead)


@asynccontextmanager
async def _sweeper_stack(
    stream: str,
) -> AsyncIterator[tuple[async_sessionmaker[Any], ResumeQueue, aioredis.Redis]]:
    """A self-owned async engine + sessionmaker + resume queue on the test's DB
    and isolated runs stream, disposed on exit. Built inside the running loop so
    the asyncpg/redis pools bind to it, never to the TestClient's lifespan loop."""

    settings = get_settings()
    engine = create_async_engine(settings.database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    async_client = aioredis.from_url(settings.valkey_dsn())
    queue = ResumeQueue(async_client, stream=stream)
    try:
        yield sessionmaker, queue, async_client
    finally:
        await async_client.aclose()
        await engine.dispose()


def test_sweeper_expires_lapsed_pending_and_enqueues_resume_turn(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    payload = _payload(expires_in_seconds=1, reply_endpoint="http://localhost:9999/api/")
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()
    assert created["expires_at"] is not None

    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    flipped = asyncio.run(_scenario())
    assert flipped == 1

    # DB state: flipped to expired by the platform, no human resolver recorded.
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers).json()
    assert got["status"] == "expired"
    assert got["resolved_by"] is None
    assert got["resolved_at"] is not None

    # Exactly one resume turn, a valid frozen QueuedTurn addressed back to the
    # requesting thread's placeholder and conveying the expiry.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == "system"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder == "p-1"
    assert turn.reply_handle.endpoint == "http://localhost:9999/api/"
    assert "expired" in turn.text
    assert payload["summary"] in turn.text

    # The autonomous flip is audited consistently with #247.
    audit = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers)
    assert audit.status_code == 200
    entries_audit = audit.json()
    assert len(entries_audit) == 1
    row = entries_audit[0]
    assert row["action"] == "expired"
    assert row["actor"] == "system"
    assert row["authorizer"] == "ExpirySweeper"
    assert row["authorized"] is True
    assert row["decision"] == ""

    # #418: the wake reached the stream, so the sweeper marked the record resumed.
    assert _read_resumed_at(created["id"]) is not None

    # ...which is what keeps the reconciler off a healthy sweep. Now that expired
    # rows are reconciler candidates, an unmarked one would be re-enqueued past
    # the grace horizon and steer the resumed thread a second time. Run the
    # backstop against the same DB with grace 0 (the worst case: every expired
    # row is instantly past the horizon) and it must find nothing to do.
    async def _reconcile() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            reconciler = ResumeReconciler(
                sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
            )
            return await reconciler.reconcile_once()

    assert asyncio.run(_reconcile()) == 0
    assert len(valkey.xrange(runs_stream)) == 1


def test_sweeper_ignores_unexpired_and_unbounded_records(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    future = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=3600), headers=auth_headers
    ).json()
    unbounded = approvals_client.post("/approvals", json=_payload(), headers=auth_headers).json()
    assert unbounded["expires_at"] is None

    now = _naive_utc()

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    flipped = asyncio.run(_scenario())
    assert flipped == 0

    for record in (future, unbounded):
        got = approvals_client.get(f"/approvals/{record['id']}", headers=auth_headers).json()
        assert got["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_sweeper_second_pass_is_a_no_op(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()

    now = _naive_utc(2)

    async def _scenario() -> tuple[int, int]:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                first = await sweep_expired_approvals(session, queue, now=now)
            async with sessionmaker() as session:
                second = await sweep_expired_approvals(session, queue, now=now)
            return first, second

    first, second = asyncio.run(_scenario())
    assert first == 1
    assert second == 0

    # No double-flip, no second turn, no second audit row.
    assert len(valkey.xrange(runs_stream)) == 1
    audit = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert [e["action"] for e in audit] == ["expired"]


def test_sweeper_skips_already_resolved_records(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()

    # Resolve it via the normal path BEFORE the SLA lapses (approver from C1).
    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 200
    assert len(valkey.xrange(runs_stream)) == 1  # the resolve turn

    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    flipped = asyncio.run(_scenario())
    assert flipped == 0

    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers).json()
    assert got["status"] == "approved"

    # Exactly the one resolve turn, authored by the human resolver; no expiry
    # turn was appended.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.author == "U9"


# --- fault injection (#412 hardening) -----------------------------------------
#
# Two defects an independent review surfaced, each pinned by driving a
# Valkey enqueue failure at the exact seam that used to be unguarded:
#   A. the resolve path 500s instead of 410 when the expiry resume enqueue fails;
#   B. the sweeper must isolate a per-record enqueue failure so the batch
#      continues (one poisoned record cannot strand the rest).


def test_resolve_path_expiry_returns_410_even_if_enqueue_fails(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late resolver on a lapsed approval must still get 410 when enqueuing the
    expiry resume turn fails (a Valkey blip).

    The flip to ``expired`` already committed (``expire_approval`` commits before
    the enqueue), so the resolve path owes the caller a clean 410 conveying the
    SLA outcome -- not a 500 that buries it behind an infra error. This pins the
    #412 regression where the unguarded ``resume_queue.enqueue`` on the expiry
    branch turns the 410 into an uncaught 500.
    """

    payload = _payload(expires_in_seconds=1)
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()
    assert created["expires_at"] is not None
    time.sleep(1.1)

    async def _boom(*a: Any, **k: Any) -> str:
        raise RuntimeError("valkey down")

    # The resolve endpoint reads request.app.state.resume_queue; make its enqueue
    # fail so the expiry branch's enqueue raises.
    monkeypatch.setattr(approvals_client.app.state.resume_queue, "enqueue", _boom)

    # Observe the real 500 the error middleware returns in production, rather
    # than letting TestClient (raise_server_exceptions=True) re-raise it -- the
    # regression being pinned is precisely "500 leaks out where 410 is owed".
    monkeypatch.setattr(approvals_client._transport, "raise_server_exceptions", False)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    # RED today: the unguarded enqueue raises, so the endpoint 500s (or the
    # RuntimeError propagates) instead of returning the 410 the SLA lapse owes.
    assert resolved.status_code == 410, resolved.text

    # The SLA flip still committed independently of the failed enqueue.
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert got.json()["status"] == "expired"

    # The enqueue failed, so no resume turn reached the stream.
    assert valkey.xrange(runs_stream) == []

    # #418: and because nothing was delivered, the record stays unmarked -- an
    # owed wake the reconciler picks up past the grace horizon. Marking before
    # enqueuing would write the wake off as delivered and strand the session
    # permanently, since an expired record is never re-selected as pending.
    assert _read_resumed_at(created["id"]) is None


def test_resolve_path_expiry_returns_410_when_the_resumed_mark_fails(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late resolver on a lapsed approval must still get 410 when the MARK
    fails, not just when the enqueue does.

    Distinct from ``test_resolve_path_expiry_returns_410_even_if_enqueue_fails``:
    there the enqueue raises FIRST, so the mark never runs. Here the enqueue
    SUCCEEDS and ``crud.mark_approval_resumed`` raises, which is the only ordering
    that reaches the ``except``'s ``session.rollback()`` with a live ORM instance
    still needed by the 410 message. That rollback expires every instance in the
    session, so reading ``expired.expires_at`` off the record afterwards would
    trigger an implicit reload and raise ``MissingGreenlet`` under the async
    session, turning the owed 410 into a 500. The endpoint hoists ``expires_at``
    into a local before the try to keep the deadline readable after the rollback;
    this test pins that hoist.

    ``resumed_at`` must stay NULL: the mark is what writes the wake off as
    delivered, and it did not commit, so the reconciler still owns the retry past
    its grace horizon (a redundant wake, the safe direction, since the turn did
    reach the stream).
    """

    payload = _payload(expires_in_seconds=1)
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()
    assert created["expires_at"] is not None
    time.sleep(1.1)

    real_mark = crud.mark_approval_resumed

    async def _failing_mark(session: Any, approval_id: uuid.UUID) -> Any:
        # Wrap the real collaborator: only this record's mark fails (a DB blip),
        # every other caller keeps the genuine implementation.
        if str(approval_id) == created["id"]:
            raise RuntimeError("postgres unreachable")
        return await real_mark(session, approval_id)

    monkeypatch.setattr(crud, "mark_approval_resumed", _failing_mark)

    # Observe the real 500 the error middleware would return in production rather
    # than letting TestClient re-raise it -- the regression pinned here is
    # precisely "500 leaks out where 410 is owed".
    monkeypatch.setattr(approvals_client._transport, "raise_server_exceptions", False)

    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "resolved_by": "U9", "actor_channel": "C1"},
        headers=auth_headers,
    )
    assert resolved.status_code == 410, resolved.text

    # The 410 still names the expiry deadline. This is what the hoist protects: a
    # lazy reload after the rollback 500s before this body is ever built.
    deadline = str(datetime.fromisoformat(created["expires_at"]))
    assert deadline in resolved.json()["detail"]

    # The SLA flip committed independently of the failed mark.
    got = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert got.json()["status"] == "expired"

    # The enqueue ran BEFORE the mark, so the wake did reach the stream -- this is
    # the ordering the enqueue-failure test cannot reach.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.author == "system"

    # The mark did not commit, so the record still reads as owing its wake.
    assert _read_resumed_at(created["id"]) is None


def test_sweeper_isolates_a_failing_record(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Valkey enqueue failure on ONE lapsed record must not stop the sweeper
    from flipping and resuming the others: the per-record try/except in
    ``sweep_expired_approvals`` isolates the failure so the batch continues.

    Ordering note (robust form): ``list_expired_pending_approvals`` orders by
    ``expires_at``, so which of the two records the sweep processes first is not
    asserted here. The flaky enqueue raises on its FIRST call regardless of which
    record that is; the assertions only pin the isolation contract -- the sweep
    flips BOTH records, returns 1 (exactly one enqueue succeeded), and lands
    exactly one resume turn, belonging to whichever record was processed second.

    This may already be GREEN: the per-record ``except`` exists today. Its job is
    to lock the isolation contract so it cannot regress -- the accompanying #412
    production fix also adds a ``session.rollback()`` in that ``except`` to guard
    the rarer DB-commit-failure case, matching the convention in
    routers/approvals.py and routers/agents.py.
    """

    first = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    second = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    assert first["expires_at"] is not None
    assert second["expires_at"] is not None

    ids = {first["id"], second["id"]}
    # Map conversation_id -> record id so we can identify which record the single
    # surviving turn belongs to, without assuming processing order.
    conversation_to_id = {
        first["conversation_id"]: first["id"],
        second["conversation_id"]: second["id"],
    }
    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            orig = queue.enqueue
            calls = {"n": 0}

            async def _flaky_enqueue(turn: QueuedTurn) -> str:
                # Raise on the first record's enqueue (a Valkey blip), then let
                # every later record enqueue for real.
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("valkey down")
                return await orig(turn)

            monkeypatch.setattr(queue, "enqueue", _flaky_enqueue)
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    swept = asyncio.run(_scenario())
    # One enqueue raised (record processed first), one succeeded (processed
    # second): the poisoned record did not block the batch.
    assert swept == 1

    # Both flips committed -- expire_approval commits before the enqueue runs, so
    # the record whose enqueue failed is still expired.
    for record in (first, second):
        got = approvals_client.get(f"/approvals/{record['id']}", headers=auth_headers).json()
        assert got["status"] == "expired"

    # Exactly one resume turn on the stream, for whichever record was second.
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.conversation_id in conversation_to_id
    resumed_id = conversation_to_id[turn.conversation_id]
    assert resumed_id in ids
    assert turn.event_id == f"approval-{resumed_id}-resolved"
    assert turn.author == "system"

    # #418: enqueue-first-then-mark, per record. The record whose enqueue landed
    # is marked; the poisoned one stays NULL so the reconciler re-enqueues it
    # later. Marking before the enqueue would leave the failed record marked --
    # its wake written off as delivered with nothing on the stream, which is the
    # permanent strand this issue exists to close.
    (stranded_id,) = ids - {resumed_id}
    assert _read_resumed_at(resumed_id) is not None
    assert _read_resumed_at(stranded_id) is None


def test_sweeper_disabled_when_interval_nonpositive(
    _disposable_db: Any,
) -> None:
    prior = os.environ.get("APPROVAL_SWEEP_INTERVAL_S")
    os.environ["APPROVAL_SWEEP_INTERVAL_S"] = "0"
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            # Disabled: no task started, and shutdown (the `with` exit) is
            # unconditional-safe against the None state.
            assert client.app.state.sweeper_task is None
    finally:
        if prior is None:
            os.environ.pop("APPROVAL_SWEEP_INTERVAL_S", None)
        else:
            os.environ["APPROVAL_SWEEP_INTERVAL_S"] = prior
        get_settings.cache_clear()


def test_run_expiry_sweeper_loop_sweeps_and_stops(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    approval_id = uuid.UUID(created["id"])

    async def _scenario() -> None:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, client):
            stop = asyncio.Event()
            task = asyncio.create_task(run_expiry_sweeper(sessionmaker, queue, 0.05, stop))
            try:
                # Poll (bounded) until the loop observes the lapse and flips it.
                status_deadline = asyncio.get_running_loop().time() + 5.0
                status = created["status"]
                while status != "expired":
                    assert asyncio.get_running_loop().time() < status_deadline, (
                        "sweeper loop never flipped the lapsed record"
                    )
                    await asyncio.sleep(0.1)
                    async with sessionmaker() as session:
                        record = await crud.get_approval(session, approval_id)
                        assert record is not None
                        status = record.status
                # The loop also enqueued the expiry resume turn -- but the flip
                # commits (crud.expire_approval) BEFORE the enqueue runs, so the
                # status poll above can win the race between them. Poll for the
                # stream entry too rather than asserting it immediately, or the
                # sweeper's flip-before-enqueue ordering flakes this under load.
                entries = await client.xrange(runs_stream)
                entries_deadline = asyncio.get_running_loop().time() + 5.0
                while len(entries) < 1:
                    assert asyncio.get_running_loop().time() < entries_deadline, (
                        "sweeper flipped the record but never enqueued the resume turn"
                    )
                    await asyncio.sleep(0.05)
                    entries = await client.xrange(runs_stream)
                assert len(entries) == 1
            finally:
                stop.set()
                # Wait-first loop wakes immediately on stop; it must finish
                # promptly, not hang.
                await asyncio.wait_for(task, 2)
            assert task.done()

    asyncio.run(_scenario())


# --- #420: approver sets unfused from the card's channel -----------------------
#
# Setup shape throughout: an agent binds route "managers" to the BROAD channel
# (where the card posts) and, optionally, an `approvers` block (who may act).
# The approval then names that agent + route, with card_channel=C_BROAD -- the
# exact shape the worker produces at kernel.py:626-627.
#
# Slack is the only faked service (a MockTransport behind the real
# SlackUserGroupClient, injected through the API's own dependency). Postgres and
# Valkey are real. `calls` records every request that reached the transport,
# which is how the "this path does no I/O" contracts are proven rather than
# assumed.

_GROUP = "S0MGRS001"
_APPROVER = "U0APPROV1"
_LISTED = "U0LISTED1"
_OTHER = "U0OTHER01"
_BROAD = "C0BROAD01"
_ELSEWHERE = "C0ELSE001"


def _agent_with_routes(client: TestClient, headers: dict[str, str], routes: dict[str, Any]) -> str:
    created = client.post(
        "/agents",
        json={
            "name": f"routed-{uuid.uuid4().hex[:8]}",
            "slack_channel": "C0AGENT001",
            "approval_routes": routes,
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _routed_payload(agent_id: str | None, **overrides: Any) -> dict[str, Any]:
    return _payload(agent_id=agent_id, route="managers", card_channel=_BROAD, **overrides)


def _fake_slack(
    client: TestClient,
    members: list[str],
    *,
    calls: list[httpx.Request],
    fail: bool = False,
) -> None:
    """Wire the API's usergroup client to a MockTransport-backed Slack."""

    def _handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if fail:
            return httpx.Response(500, text="slack is down")
        return httpx.Response(200, json={"ok": True, "users": members})

    group_client = SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_handler)),
        token="xoxb-test",
    )
    client.app.dependency_overrides[get_approver_sets] = lambda: SlackApproverSetSelector(
        group_client
    )


def _no_slack_configured(client: TestClient) -> None:
    """A deployment with no SLACK_BOT_TOKEN: the selector is still wired (main
    always wires one), it just has no usergroup client behind it."""

    client.app.dependency_overrides[get_approver_sets] = lambda: SlackApproverSetSelector(None)


def _resolve(
    client: TestClient,
    headers: dict[str, str],
    approval_id: str,
    actor: str,
    channel: str | None = _BROAD,
) -> Any:
    return client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": "approved", "resolved_by": actor, "actor_channel": channel},
        headers=headers,
    )


# --- AC1: a group binding narrows authority inside a broad channel -------------


def test_group_bound_route_denies_non_group_member_even_in_card_channel(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1, the headline of #420: the card sits in a BROAD channel so everyone
    can SEE the request, but only the bound user group may act on it. U_OTHER
    clicks from exactly the right channel -- under today's channel-membership
    authorizer that is a 200 -- and must be refused: right room, no authority.
    """

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]

    # The refusal is total: no claim, no wake.
    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []
    assert len(calls) == 1


def test_group_member_resolves_and_session_resumes(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1's other half: the group member's click resolves the record and wakes
    the suspended session exactly once."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"group": _GROUP}}},
    )
    payload = _routed_payload(agent_id)
    created = approvals_client.post("/approvals", json=payload, headers=auth_headers).json()

    ok = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "approved"
    assert ok.json()["resolved_by"] == _APPROVER

    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{created['id']}-resolved"
    assert turn.conversation_id == payload["conversation_id"]
    assert turn.author == _APPROVER


def test_user_list_bound_route_denies_an_unlisted_actor_without_calling_slack(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1 for the explicit allowlist. The zero-transport-call assertion is the
    substance: the user-list path is pure config, so it must not depend on Slack
    being reachable at all."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_OTHER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"users": [_LISTED]}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]
    assert calls == []
    assert valkey.xrange(runs_stream) == []


def test_user_list_bound_route_resolves_for_a_listed_actor(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1: the listed actor resolves and the session resumes, with Slack untouched."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"users": [_LISTED, _APPROVER]}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    ok = _resolve(approvals_client, auth_headers, created["id"], _LISTED)
    assert ok.status_code == 200, ok.text
    assert len(valkey.xrange(runs_stream)) == 1
    assert calls == []


def test_explicit_user_list_wins_over_the_group_binding(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC1 precedence, exactly as the issue states it: with BOTH declared,
    ``users`` decides and ``group`` is ignored. A genuine group member who is
    not on the list is refused, and Slack is never consulted."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {
            "managers": {
                "channel": _BROAD,
                "approvers": {"group": _GROUP, "users": [_LISTED]},
            }
        },
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert denied.status_code == 403, denied.text
    assert "not an approver" in denied.json()["detail"]

    ok = _resolve(approvals_client, auth_headers, created["id"], _LISTED)
    assert ok.status_code == 200, ok.text
    assert len(valkey.xrange(runs_stream)) == 1
    assert calls == []


# --- AC2: no self-approval under ANY authorizer --------------------------------


def test_requester_cannot_self_approve_under_the_group_authorizer(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC2: the author IS a member of the approver group and still cannot resolve
    their own request.

    The zero-call assertion pins the guard's ORDERING, not just its verdict: the
    self-approval check runs before any Slack fetch, so a self-attempt spends no
    rate-limit budget and cannot be used to probe group membership.
    """

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_APPROVER),
        headers=auth_headers,
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert denied.status_code == 403, denied.text
    assert "self-approval" in denied.json()["detail"]
    assert calls == []

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_requester_cannot_self_approve_under_the_user_list_authorizer(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC2: the author is on the explicit allowlist and still cannot resolve."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"users": [_LISTED, _APPROVER]}}},
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_LISTED),
        headers=auth_headers,
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _LISTED)
    assert denied.status_code == 403, denied.text
    assert "self-approval" in denied.json()["detail"]
    assert valkey.xrange(runs_stream) == []


def test_requester_cannot_self_approve_under_a_bound_channel_authorizer(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC2's third implementation. Distinct from the unbound-approval case that
    ``test_authorizer_blocks_non_member_and_self_approval`` already pins: here a
    route binding placed the card, so the resolver walks the binding path before
    landing on channel membership. The self-approval block must survive that
    route."""

    agent_id = _agent_with_routes(approvals_client, auth_headers, {"managers": {"channel": _BROAD}})
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_APPROVER),
        headers=auth_headers,
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert denied.status_code == 403, denied.text
    assert "self-approval" in denied.json()["detail"]
    assert valkey.xrange(runs_stream) == []


# --- AC3: the audit names the authorizer AND the evidence that counted ---------


def test_audit_names_authorizer_and_membership_evidence(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3: after a group-denied then group-approved sequence, each audit row
    names UserGroupAuthorizer and carries the membership evidence that decided
    it -- the group, the actor's verdict, the size of the group that proved it,
    and when that membership was fetched. The member list itself is deliberately
    NOT stored (a 500-member group would bloat an append-only table per click).
    """

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER, _LISTED], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    assert _resolve(approvals_client, auth_headers, created["id"], _OTHER).status_code == 403
    assert _resolve(approvals_client, auth_headers, created["id"], _APPROVER).status_code == 200

    entries = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert [e["action"] for e in entries] == ["denied", "resolved"]
    denied_entry, resolved_entry = entries

    assert denied_entry["authorizer"] == "UserGroupAuthorizer"
    assert denied_entry["authorized"] is False
    assert denied_entry["evidence"]["kind"] == "user_group"
    assert denied_entry["evidence"]["group"] == _GROUP
    assert denied_entry["evidence"]["actor_in_group"] is False
    assert denied_entry["evidence"]["member_count"] == 2
    assert datetime.fromisoformat(denied_entry["evidence"]["fetched_at"])
    assert denied_entry["evidence"]["cache_age_s"] >= 0

    assert resolved_entry["authorizer"] == "UserGroupAuthorizer"
    assert resolved_entry["authorized"] is True
    assert resolved_entry["evidence"]["actor_in_group"] is True
    assert resolved_entry["evidence"]["group"] == _GROUP
    # The list itself is not the snapshot; the verdict + count + timestamp are.
    assert "users" not in resolved_entry["evidence"]


def test_audit_evidence_for_a_user_list_decision(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3: a user-list resolution records the allowlist that counted and the
    actor's verdict against it."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [], calls=calls)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"users": [_LISTED, _APPROVER]}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    assert _resolve(approvals_client, auth_headers, created["id"], _OTHER).status_code == 403
    assert _resolve(approvals_client, auth_headers, created["id"], _LISTED).status_code == 200

    denied_entry, resolved_entry = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()

    assert denied_entry["authorizer"] == "ExplicitUserListAuthorizer"
    assert denied_entry["evidence"]["kind"] == "user_list"
    assert denied_entry["evidence"]["actor_listed"] is False
    assert sorted(denied_entry["evidence"]["users"]) == sorted([_LISTED, _APPROVER])

    assert resolved_entry["authorizer"] == "ExplicitUserListAuthorizer"
    assert resolved_entry["evidence"]["actor_listed"] is True


def test_audit_evidence_for_a_channel_decision(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC3 + AC4: the unchanged channel path gains evidence too, naming the
    channel that held the authority and the channel the click came from."""

    agent_id = _agent_with_routes(approvals_client, auth_headers, {"managers": {"channel": _BROAD}})
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER, _ELSEWHERE)
    assert denied.status_code == 403, denied.text
    assert _resolve(approvals_client, auth_headers, created["id"], _OTHER).status_code == 200

    denied_entry, resolved_entry = approvals_client.get(
        f"/approvals/{created['id']}/audit", headers=auth_headers
    ).json()

    assert denied_entry["authorizer"] == "ChannelMembershipAuthorizer"
    assert denied_entry["evidence"]["kind"] == "channel_membership"
    assert denied_entry["evidence"]["approvers_channel"] == _BROAD
    assert denied_entry["evidence"]["actor_channel"] == _ELSEWHERE

    assert resolved_entry["evidence"]["approvers_channel"] == _BROAD
    assert resolved_entry["evidence"]["actor_channel"] == _BROAD


def test_expiry_sweeper_audit_rows_carry_no_evidence(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Edge cases 11 and 13: ``evidence`` is a NULLABLE column on an append-only
    table. Writers that made no membership decision -- the expiry sweeper -- must
    leave it NULL rather than fabricate one, and old rows must still read back
    through ApprovalAuditOut."""

    created = approvals_client.post(
        "/approvals", json=_payload(expires_in_seconds=1), headers=auth_headers
    ).json()
    now = _naive_utc(2)

    async def _scenario() -> int:
        async with _sweeper_stack(runs_stream) as (sessionmaker, queue, _client):
            async with sessionmaker() as session:
                return await sweep_expired_approvals(session, queue, now=now)

    assert asyncio.run(_scenario()) == 1

    entries = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert [e["action"] for e in entries] == ["expired"]
    assert entries[0]["authorizer"] == "ExpirySweeper"
    assert entries[0]["evidence"] is None


# --- fail closed: a declared approvers spec never degrades to channel ----------


def test_group_binding_without_a_bot_token_denies_instead_of_channel_fallback(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Fail closed on config error. The deployment declared a group but wired no
    SLACK_BOT_TOKEN, so membership is unverifiable. The actor below is standing
    in the card channel -- a fallback to channel membership would ALLOW them,
    silently widening the approver set the operator thought they had narrowed.
    """

    _no_slack_configured(approvals_client)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "could not verify" in denied.json()["detail"]
    assert "not an approver" not in denied.json()["detail"]

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


def test_group_lookup_failure_denies_and_audits_the_failure(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """Fail closed on lookup error, plus AC3 for the failure row. Slack is down;
    the actor is in the card channel and must STILL be refused, and the audit
    must record that infrastructure -- not policy -- refused them."""

    calls: list[httpx.Request] = []
    _fake_slack(approvals_client, [_APPROVER], calls=calls, fail=True)
    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"managers": {"channel": _BROAD, "approvers": {"group": _GROUP}}},
    )
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    denied = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert denied.status_code == 403, denied.text
    assert "could not verify" in denied.json()["detail"]
    assert "not an approver" not in denied.json()["detail"]
    assert calls != []

    entries = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert [e["action"] for e in entries] == ["denied"]
    row = entries[0]
    assert row["authorized"] is False
    assert row["authorizer"] != "ChannelMembershipAuthorizer"
    assert row["evidence"]["kind"] == "user_group"
    assert row["evidence"]["group"] == _GROUP
    assert row["evidence"]["lookup_failed"] is True
    assert row["evidence"]["error"]

    record = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert record.json()["status"] == "pending"
    assert valkey.xrange(runs_stream) == []


# --- AC4: no approvers declared keeps today's behavior exactly -----------------


def test_binding_without_approvers_keeps_channel_membership(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC4: an existing deployment's bare ``{channel}`` binding keeps resolving
    against ``card_channel`` -- anyone in the card channel, nobody outside it.
    Zero-setup stays zero-setup."""

    agent_id = _agent_with_routes(approvals_client, auth_headers, {"managers": {"channel": _BROAD}})
    created = approvals_client.post(
        "/approvals", json=_routed_payload(agent_id), headers=auth_headers
    ).json()

    outside = _resolve(approvals_client, auth_headers, created["id"], _OTHER, _ELSEWHERE)
    assert outside.status_code == 403, outside.text
    assert "not an approver" in outside.json()["detail"]

    inside = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert inside.status_code == 200, inside.text
    assert len(valkey.xrange(runs_stream)) == 1


def test_agentless_approval_keeps_channel_membership(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC4 / edge case 7: ``agent_id`` is nullable by design (the generic/dev
    path). With no agent there is no binding to read, so the record falls back to
    channel membership on its card_channel -- today's behavior, unchanged."""

    created = approvals_client.post(
        "/approvals", json=_routed_payload(None, author=_APPROVER), headers=auth_headers
    ).json()
    assert created["agent_id"] is None
    assert created["route"] == "managers"

    outside = _resolve(approvals_client, auth_headers, created["id"], _OTHER, _ELSEWHERE)
    assert outside.status_code == 403, outside.text

    # AC2 survives the no-binding path: the author is refused from the card
    # channel, where anyone else would be allowed.
    author = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert author.status_code == 403, author.text
    assert "self-approval" in author.json()["detail"]

    inside = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert inside.status_code == 200, inside.text
    assert len(valkey.xrange(runs_stream)) == 1


def test_unbound_route_name_keeps_channel_membership(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    """AC4 / edge case 8: the approval names a route the agent's map does not
    bind (renamed or removed while pending). Fresh-read semantics say current
    config wins, and current config declares no approvers for this route, so it
    is channel membership -- mirroring the worker's unbound-route card fallback.
    """

    agent_id = _agent_with_routes(
        approvals_client,
        auth_headers,
        {"legal": {"channel": _ELSEWHERE, "approvers": {"users": [_LISTED]}}},
    )
    created = approvals_client.post(
        "/approvals",
        json=_routed_payload(agent_id, author=_APPROVER),
        headers=auth_headers,
    ).json()

    # The OTHER route's allowlist must not leak onto this one.
    listed_elsewhere = _resolve(approvals_client, auth_headers, created["id"], _LISTED, _ELSEWHERE)
    assert listed_elsewhere.status_code == 403, listed_elsewhere.text

    # AC2 survives the unbound-route path too.
    author = _resolve(approvals_client, auth_headers, created["id"], _APPROVER)
    assert author.status_code == 403, author.text
    assert "self-approval" in author.json()["detail"]

    inside = _resolve(approvals_client, auth_headers, created["id"], _OTHER)
    assert inside.status_code == 200, inside.text
    assert len(valkey.xrange(runs_stream)) == 1
