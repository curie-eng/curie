"""WorkItem admission, dispatch, and internal-worker HTTP contract."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from channel_protocol import scoped_conversation_id
from curie_api import workitem_dispatch, workitems
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.routers import work_items
from curie_api.workitem_dispatch import (
    acquire,
    admit,
    cancel,
    claim_termination,
    defer,
    finish,
    heartbeat,
    record_termination,
    start,
)
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

REPO = "acme-corp/acme-bot"
OTHER_REPO = "attacker/other-bot"
ADDRESS = "C0EXAMPLE1"
WIRE_CONVERSATION = "1700000000.000100"
WORKER_TOKEN = "work-item-dispatch-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
OWNER = "curie-workers-a"
OTHER_OWNER = "curie-workers-b"
OBJECTIVE = "Implement the admitted work item"
REQUESTER = "U0REQUEST1"
CLAIM_NAME = "curie-thread-dispatch-claim"
SANDBOX_NAME = "sbx-curie-thread-dispatch-claim"
TERMINATION_OBSERVATION = (
    "claims=curie-thread-dispatch-claim sandboxes=sbx-curie-thread-dispatch-claim "
    "absent_at=2026-09-18T12:00:10+00:00 observer=curie-workers-a"
)
INTERNAL_PREFIX = "/v1/internal/work-items"


def with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _code(result: object) -> str:
    code = getattr(result, "code", None)
    assert isinstance(code, str), result
    return code


def _http_code(response: Any) -> str:
    body = response.json()
    if isinstance(body.get("code"), str):
        return str(body["code"])
    detail = body.get("detail")
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return str(detail["code"])
    raise AssertionError(f"missing refusal code in {body}")


async def _now(session: AsyncSession) -> datetime:
    value = await session.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(value, datetime)
    return value


async def _agent_with_channel(
    session: AsyncSession,
    *,
    address: str = ADDRESS,
    repo: str = REPO,
) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.agents (id, name, repo_full_name) "
            "VALUES (:id, :name, :repo)"
        ),
        {
            "id": agent_id,
            "name": f"acme-bot-{agent_id.hex[:8]}",
            "repo": repo,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
            "VALUES (:id, :agent_id, 'slack', :address)"
        ),
        {"id": uuid.uuid4(), "agent_id": agent_id, "address": address},
    )
    await session.commit()
    return agent_id


def _facts(agent_id: uuid.UUID, **overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = {
        "agent_id": agent_id,
        "kind": "slack",
        "address": ADDRESS,
        "reply_conversation_id": WIRE_CONVERSATION,
        "repo_full_name": REPO,
        "github_repository_id": 101,
        "github_issue_number": 2573,
        "github_installation_id": 202,
        "objective": OBJECTIVE,
        "requester": REQUESTER,
        "request_id": uuid.uuid4(),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _facts_json(facts: SimpleNamespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in vars(facts).items():
        payload[key] = str(value) if isinstance(value, UUID) else value
    return payload


async def _request_row(session: AsyncSession, request_id: uuid.UUID) -> Any:
    row = (
        await session.execute(
            text(
                "SELECT r.id, r.work_item_id, r.status, r.wait_deadline, "
                "r.started_at, r.execution_deadline, r.version, "
                "r.execution_attempts, r.capacity_deferrals, "
                "r.dispatch_generation, r.published_generation, "
                "r.dispatch_not_before, r.last_deferral_reason, "
                "r.runtime_owner, r.runtime_epoch, "
                "r.runtime_heartbeat_expires_at, r.runtime_claim_name, "
                "r.runtime_sandbox_name, r.terminal_cause, "
                "r.termination_observation, r.acquire_owner, "
                "r.acquired_generation, w.version AS work_item_version, "
                "w.cancelled_at "
                "FROM curie.execution_requests r "
                "JOIN curie.work_items w ON w.id = r.work_item_id "
                "WHERE r.id = :id"
            ),
            {"id": request_id},
        )
    ).mappings().one()
    return row


@pytest.fixture
def allowlisted(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def dispatch_client(
    clean_db: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


def test_work_items_router_is_the_internal_worker_surface() -> None:
    assert work_items.router.prefix == INTERNAL_PREFIX


def test_admit_replay_keeps_the_stored_deadline(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        first = await admit(session, facts)
        assert first.replayed is False
        assert first.request is not None
        deadline = first.request.wait_deadline
        replay = await admit(session, facts)
        assert replay.replayed is True
        assert replay.request is not None
        assert replay.request.id == facts.request_id
        assert replay.request.wait_deadline == deadline
        row = await _request_row(session, facts.request_id)
        assert row.wait_deadline == deadline
        assert row.execution_attempts == 0
        assert row.started_at is None
        conversation = scoped_conversation_id(
            "slack", ADDRESS, WIRE_CONVERSATION
        )
        stored = await session.scalar(
            text("SELECT conversation_id FROM curie.work_items WHERE id = :id"),
            {"id": first.work_item.id},
        )
        assert stored == conversation

    with_session(body)


def test_concurrent_same_uuid_admit_inserts_one_row(
    clean_db: None, allowlisted: None
) -> None:
    async def setup(session: AsyncSession) -> uuid.UUID:
        return await _agent_with_channel(session)

    agent_id = with_session(setup)
    facts = _facts(agent_id)

    async def race() -> list[object]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def contender() -> object:
            async with maker() as session:
                return await admit(session, facts)

        try:
            gathered = asyncio.gather(contender(), contender())
            return list(await asyncio.wait_for(gathered, timeout=10))
        finally:
            await engine.dispose()

    results = asyncio.run(race())
    replayed = sorted(bool(getattr(result, "replayed", None)) for result in results)
    assert replayed == [False, True]
    ids = {
        getattr(getattr(result, "request", None), "id", None) for result in results
    }
    assert ids == {facts.request_id}

    async def verify(session: AsyncSession) -> None:
        assert await session.scalar(
            text("SELECT count(*) FROM curie.execution_requests WHERE id = :id"),
            {"id": facts.request_id},
        ) == 1

    with_session(verify)


def test_admit_refuses_active_request_unknown_binding_and_disallowed_repo(
    clean_db: None, allowlisted: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        first = await admit(session, _facts(agent_id))
        assert first.request is not None
        active = await admit(session, _facts(agent_id, request_id=uuid.uuid4()))
        assert _code(active) == "active_request"

        unbound = await _agent_with_channel(session, address="C0EXAMPLE2")
        missing = await admit(
            session,
            _facts(
                unbound,
                github_issue_number=2574,
                address=ADDRESS,
                request_id=uuid.uuid4(),
            ),
        )
        assert _code(missing) == "binding_missing"

        monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/other-bot"]')
        get_settings.cache_clear()
        refused = await admit(
            session,
            _facts(
                agent_id,
                github_issue_number=2575,
                repo_full_name=OTHER_REPO,
                request_id=uuid.uuid4(),
            ),
        )
        assert _code(refused) == "repository_not_allowed"

    with_session(body)


def test_acquire_wrong_generation_duplicate_owner_and_cancelled_item(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        admitted = await admit(session, facts)
        assert admitted.request is not None
        unpublished = await acquire(
            session, facts.request_id, owner=OWNER, generation=1
        )
        assert unpublished.generation == 1
        assert unpublished.work_item_id == admitted.work_item.id
        assert unpublished.conversation_id == scoped_conversation_id(
            "slack", ADDRESS, WIRE_CONVERSATION
        )
        assert unpublished.wait_deadline == admitted.request.wait_deadline
        assert unpublished.repo_full_name == facts.repo_full_name

        stale = await acquire(
            session, facts.request_id, owner=OWNER, generation=0
        )
        assert _code(stale) == "not_published"
        future = await acquire(
            session, facts.request_id, owner=OWNER, generation=2
        )
        assert _code(future) in {"not_published", "not_dispatchable"}
        duplicate = await acquire(
            session, facts.request_id, owner=OTHER_OWNER, generation=1
        )
        assert _code(duplicate) == "duplicate"

        cancelled = await cancel(
            session,
            work_item_id=admitted.work_item.id,
            expected_version=admitted.work_item.version,
        )
        assert cancelled.work_item.cancelled_at is not None
        refused = await acquire(
            session, facts.request_id, owner=OWNER, generation=1
        )
        assert _code(refused) == "work_item_cancelled"

    with_session(body)


def test_defer_preserves_deadline_version_start_and_attempts(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        admitted = await admit(session, facts)
        assert admitted.request is not None
        granted = await acquire(
            session, facts.request_id, owner=OWNER, generation=1
        )
        assert granted.generation == 1
        before = await _request_row(session, facts.request_id)
        now = await _now(session)
        busy = await defer(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            reason="thread_busy",
            capacity=False,
        )
        after_busy = await _request_row(session, facts.request_id)
        assert after_busy.wait_deadline == before.wait_deadline
        assert after_busy.version == before.version
        assert after_busy.work_item_version == before.work_item_version
        assert after_busy.started_at is None
        assert after_busy.execution_attempts == 0
        assert after_busy.capacity_deferrals == 0
        assert after_busy.dispatch_generation == before.dispatch_generation + 1
        assert after_busy.dispatch_not_before > now
        assert after_busy.last_deferral_reason == "thread_busy"
        assert busy.dispatch_generation == after_busy.dispatch_generation

        await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=after_busy.dispatch_generation,
        )
        capacity = await defer(
            session,
            facts.request_id,
            owner=OWNER,
            generation=after_busy.dispatch_generation,
            reason="capacity",
            capacity=True,
        )
        after_capacity = await _request_row(session, facts.request_id)
        assert after_capacity.wait_deadline == before.wait_deadline
        assert after_capacity.version == before.version
        assert after_capacity.started_at is None
        assert after_capacity.execution_attempts == 0
        assert after_capacity.capacity_deferrals == 1
        assert after_capacity.dispatch_not_before > now
        assert capacity.dispatch_generation == after_capacity.dispatch_generation

    with_session(body)


def test_start_records_attempt_epoch_deadline_and_runtime_names(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        await acquire(session, facts.request_id, owner=OWNER, generation=1)
        started = await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
        )
        row = await _request_row(session, facts.request_id)
        assert row.status == "running"
        assert row.execution_attempts == 1
        assert row.runtime_epoch == 1
        assert row.started_at is not None
        assert row.execution_deadline is not None
        assert row.execution_deadline - row.started_at == timedelta(seconds=1800)
        assert row.runtime_claim_name == CLAIM_NAME
        assert row.runtime_sandbox_name == SANDBOX_NAME
        assert started.runtime_epoch == 1
        assert started.execution_deadline == row.execution_deadline

    with_session(body)


def test_heartbeat_and_finish_refuse_a_stale_runtime_epoch(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        await acquire(session, facts.request_id, owner=OWNER, generation=1)
        await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
        )
        before = await _request_row(session, facts.request_id)
        stale_heartbeat = await heartbeat(
            session, facts.request_id, runtime_epoch=2
        )
        assert _code(stale_heartbeat) == "stale_owner"
        stale_finish = await finish(
            session,
            facts.request_id,
            runtime_epoch=2,
            outcome="completed",
            cause="completed",
            detail=None,
        )
        assert _code(stale_finish) == "stale_owner"
        after = await _request_row(session, facts.request_id)
        assert after.status == "running"
        assert after.version == before.version
        assert after.runtime_epoch == 1
        assert after.started_at == before.started_at
        assert after.execution_deadline == before.execution_deadline
        assert after.terminal_cause is None

    with_session(body)


def test_termination_claim_waits_for_heartbeat_expiry_and_maps_owner_lost(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        admitted = await admit(session, facts)
        await acquire(session, facts.request_id, owner=OWNER, generation=1)
        await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
        )
        cancelled = await cancel(
            session,
            work_item_id=admitted.work_item.id,
            expected_version=admitted.work_item.version,
        )
        assert cancelled.request is not None
        live = await claim_termination(
            session, facts.request_id, owner=OTHER_OWNER
        )
        assert _code(live) == "duplicate"

        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "runtime_heartbeat_expires_at = clock_timestamp() "
                "- interval '1 second' WHERE id = :id"
            ),
            {"id": facts.request_id},
        )
        await session.commit()
        claimed = await claim_termination(
            session, facts.request_id, owner=OTHER_OWNER
        )
        assert claimed.runtime_epoch == 2

        owner_lost_facts = _facts(agent_id, github_issue_number=2574)
        owner_lost = await admit(session, owner_lost_facts)
        await acquire(
            session, owner_lost_facts.request_id, owner=OWNER, generation=1
        )
        await start(
            session,
            owner_lost_facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
        )
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "runtime_heartbeat_expires_at = clock_timestamp() "
                "- interval '1 second' WHERE id = :id"
            ),
            {"id": owner_lost_facts.request_id},
        )
        await session.commit()
        marked = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=owner_lost.work_item.id,
            request_id=owner_lost_facts.request_id,
            expected_work_item_version=owner_lost.work_item.version,
            expected_request_version=2,
        )
        assert isinstance(marked, workitems.WorkItemOutcome), marked
        assert marked.request is not None
        assert (
            marked.request.status,
            marked.request.terminal_cause,
        ) == ("cancellation_requested", "owner_lost")
        termination = await claim_termination(
            session, owner_lost_facts.request_id, owner=OTHER_OWNER
        )
        await record_termination(
            session,
            owner_lost_facts.request_id,
            runtime_epoch=termination.runtime_epoch,
            observation=TERMINATION_OBSERVATION,
        )
        row = await _request_row(session, owner_lost_facts.request_id)
        assert row.status == "failed"
        assert row.terminal_cause == "owner_lost"
        assert row.termination_observation == TERMINATION_OBSERVATION

    with_session(body)


def _auth_routes() -> list[tuple[str, str, dict[str, Any] | None]]:
    request_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    work_item_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    admissions = _facts_json(
        _facts(
            UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
            request_id=UUID(request_id),
        )
    )
    return [
        ("POST", f"{INTERNAL_PREFIX}/admissions", admissions),
        ("GET", f"{INTERNAL_PREFIX}/requests/{request_id}", None),
        (
            "POST",
            f"{INTERNAL_PREFIX}/{work_item_id}/cancel",
            {"expected_version": 1},
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/acquire",
            {"owner": OWNER, "generation": 1},
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/defer",
            {
                "owner": OWNER,
                "generation": 1,
                "reason": "capacity",
                "capacity": True,
            },
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/start",
            {
                "owner": OWNER,
                "generation": 1,
                "claim_name": CLAIM_NAME,
                "sandbox_name": SANDBOX_NAME,
            },
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/heartbeat",
            {"runtime_epoch": 1},
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/finish",
            {
                "runtime_epoch": 1,
                "outcome": "completed",
                "cause": "completed",
            },
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/termination/claim",
            {"owner": OWNER},
        ),
        (
            "POST",
            f"{INTERNAL_PREFIX}/requests/{request_id}/termination",
            {
                "runtime_epoch": 1,
                "observation": TERMINATION_OBSERVATION,
            },
        ),
    ]


@pytest.mark.parametrize("method, path, payload", _auth_routes())
def test_internal_work_item_routes_refuse_missing_and_wrong_worker_token(
    dispatch_client: TestClient,
    method: str,
    path: str,
    payload: dict[str, Any] | None,
) -> None:
    missing = dispatch_client.request(method, path, json=payload)
    assert missing.status_code in {401, 403}, missing.text
    wrong = dispatch_client.request(
        method,
        path,
        json=payload,
        headers={"X-Curie-Worker-Token": "example-wrong-worker-token"},
    )
    assert wrong.status_code in {401, 403}, wrong.text
    platform = dispatch_client.request(
        method,
        path,
        json=payload,
        headers={"X-API-Key": get_settings().api_key},
    )
    assert platform.status_code in {401, 403}, platform.text


def test_http_admit_replay_acquire_start_heartbeat_and_stale_finish(
    dispatch_client: TestClient, auth_headers: dict[str, str]
) -> None:
    created = dispatch_client.post(
        "/agents",
        json={
            "name": f"acme-bot-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": ADDRESS},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    agent_id = UUID(created.json()["id"])
    facts = _facts(agent_id)
    admitted = dispatch_client.post(
        f"{INTERNAL_PREFIX}/admissions",
        json=_facts_json(facts),
        headers=WORKER_HEADERS,
    )
    assert admitted.status_code == 200, admitted.text
    body = admitted.json()
    assert body["replayed"] is False
    request_id = body["request"]["id"]
    deadline = body["request"]["wait_deadline"]
    replayed = dispatch_client.post(
        f"{INTERNAL_PREFIX}/admissions",
        json=_facts_json(facts),
        headers=WORKER_HEADERS,
    )
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["replayed"] is True
    assert replayed.json()["request"]["wait_deadline"] == deadline

    acquired = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/acquire",
        json={"owner": OWNER, "generation": 1},
        headers=WORKER_HEADERS,
    )
    assert acquired.status_code == 200, acquired.text
    assert acquired.json()["repo_full_name"] == facts.repo_full_name
    started = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/start",
        json={
            "owner": OWNER,
            "generation": 1,
            "claim_name": CLAIM_NAME,
            "sandbox_name": SANDBOX_NAME,
        },
        headers=WORKER_HEADERS,
    )
    assert started.status_code == 200, started.text
    start_body = started.json()
    assert start_body["runtime_epoch"] == 1
    viewed = dispatch_client.get(
        f"{INTERNAL_PREFIX}/requests/{request_id}",
        headers=WORKER_HEADERS,
    )
    assert viewed.status_code == 200, viewed.text
    view = viewed.json()
    assert view["execution_attempts"] == 1
    assert view["runtime_epoch"] == 1
    assert view["runtime_claim_name"] == CLAIM_NAME
    assert view["runtime_sandbox_name"] == SANDBOX_NAME

    stale_heartbeat = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/heartbeat",
        json={"runtime_epoch": 9},
        headers=WORKER_HEADERS,
    )
    assert stale_heartbeat.status_code == 409, stale_heartbeat.text
    assert _http_code(stale_heartbeat) == "stale_owner"
    stale_finish = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/finish",
        json={
            "runtime_epoch": 9,
            "outcome": "completed",
            "cause": "completed",
        },
        headers=WORKER_HEADERS,
    )
    assert stale_finish.status_code == 409, stale_finish.text
    assert _http_code(stale_finish) == "stale_owner"
    unchanged = dispatch_client.get(
        f"{INTERNAL_PREFIX}/requests/{request_id}",
        headers=WORKER_HEADERS,
    )
    assert unchanged.json()["status"] == "running"
    assert unchanged.json()["runtime_epoch"] == 1



# --- #3076: recover WorkItem runs orphaned by a worker restart -------------


@pytest.fixture
def short_runtime_ttl(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS", "3")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def _running(
    session: AsyncSession, agent_id: uuid.UUID, *, issue: int = 2573
) -> SimpleNamespace:
    facts = _facts(agent_id, github_issue_number=issue)
    await admit(session, facts)
    await acquire(session, facts.request_id, owner=OWNER, generation=1)
    await start(
        session,
        facts.request_id,
        owner=OWNER,
        generation=1,
        claim_name=CLAIM_NAME,
        sandbox_name=SANDBOX_NAME,
    )
    return facts


async def _notice_causes(session: AsyncSession, request_id: uuid.UUID) -> list[str]:
    rows = await session.execute(
        text(
            "SELECT terminal_cause FROM curie.factory_terminal_notices "
            "WHERE execution_request_id = :id"
        ),
        {"id": request_id},
    )
    return [row[0] for row in rows]


def test_declare_owner_lost_drives_the_terminate_chain_to_failed(
    clean_db: None, allowlisted: None, short_runtime_ttl: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _running(session, agent_id)
        before = await _request_row(session, facts.request_id)
        assert before.runtime_heartbeat_expires_at > await _now(session)

        owners = await workitem_dispatch.list_runtime_owners(session, limit=50)
        assert [
            (o.request_id, o.runtime_owner, o.runtime_epoch) for o in owners
        ] == [(facts.request_id, OWNER, 1)]

        await workitem_dispatch.declare_owner_lost(
            session, facts.request_id, owner=OWNER, runtime_epoch=1
        )
        row = await _request_row(session, facts.request_id)
        assert row.status == "cancellation_requested"
        assert row.terminal_cause == "owner_lost"
        assert row.version == before.version + 1
        assert row.runtime_heartbeat_expires_at <= await _now(session)
        assert await workitem_dispatch.list_runtime_owners(session, limit=50) == []

        published = await workitem_dispatch.claim_terminate_publishes(
            session, retry_seconds=60, limit=50
        )
        assert [p.request_id for p in published] == [facts.request_id]

        claimed = await claim_termination(
            session, facts.request_id, owner=OTHER_OWNER
        )
        assert claimed.runtime_epoch == 2
        await record_termination(
            session,
            facts.request_id,
            runtime_epoch=claimed.runtime_epoch,
            observation=TERMINATION_OBSERVATION,
        )
        final = await _request_row(session, facts.request_id)
        assert final.status == "failed"
        assert final.terminal_cause == "owner_lost"
        assert await _notice_causes(session, facts.request_id) == ["owner_lost"]

    with_session(body)


@pytest.mark.parametrize(
    "owner, epoch", [(OWNER, 2), (OTHER_OWNER, 1)], ids=["wrong_epoch", "wrong_owner"]
)
def test_declare_owner_lost_refuses_a_mismatched_owner_or_epoch(
    clean_db: None, allowlisted: None, owner: str, epoch: int
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _running(session, agent_id)
        before = await _request_row(session, facts.request_id)
        refused = await workitem_dispatch.declare_owner_lost(
            session, facts.request_id, owner=owner, runtime_epoch=epoch
        )
        assert _code(refused) == "stale_owner"
        assert await _request_row(session, facts.request_id) == before

    with_session(body)


def test_declare_owner_lost_unknown_request_is_not_found(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        refused = await workitem_dispatch.declare_owner_lost(
            session, uuid.uuid4(), owner=OWNER, runtime_epoch=1
        )
        assert _code(refused) == "not_found"

    with_session(body)


def test_approval_hold_is_not_an_orphan(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        held = await _running(session, agent_id)
        live = await _running(session, agent_id, issue=2574)
        await workitem_dispatch.hold_for_approval(
            session, held.request_id, runtime_epoch=1
        )
        before = await _request_row(session, held.request_id)
        assert before.runtime_heartbeat_expires_at == before.execution_deadline

        owners = await workitem_dispatch.list_runtime_owners(session, limit=50)
        assert [o.request_id for o in owners] == [live.request_id]

        refused = await workitem_dispatch.declare_owner_lost(
            session, held.request_id, owner=OWNER, runtime_epoch=1
        )
        assert _code(refused) == "stale_owner"
        assert await _request_row(session, held.request_id) == before

    with_session(body)


def test_http_runtime_owners_and_owner_lost(
    dispatch_client: TestClient, auth_headers: dict[str, str]
) -> None:
    created = dispatch_client.post(
        "/agents",
        json={
            "name": f"acme-bot-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": ADDRESS},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    facts = _facts(UUID(created.json()["id"]))
    admitted = dispatch_client.post(
        f"{INTERNAL_PREFIX}/admissions",
        json=_facts_json(facts),
        headers=WORKER_HEADERS,
    )
    assert admitted.status_code == 200, admitted.text
    request_id = admitted.json()["request"]["id"]
    for verb, payload in (
        ("acquire", {"owner": OWNER, "generation": 1}),
        (
            "start",
            {
                "owner": OWNER,
                "generation": 1,
                "claim_name": CLAIM_NAME,
                "sandbox_name": SANDBOX_NAME,
            },
        ),
    ):
        response = dispatch_client.post(
            f"{INTERNAL_PREFIX}/requests/{request_id}/{verb}",
            json=payload,
            headers=WORKER_HEADERS,
        )
        assert response.status_code == 200, response.text

    unauth = dispatch_client.get(f"{INTERNAL_PREFIX}/runtime-owners")
    assert unauth.status_code in {401, 403}, unauth.text
    listed = dispatch_client.get(
        f"{INTERNAL_PREFIX}/runtime-owners", headers=WORKER_HEADERS
    )
    assert listed.status_code == 200, listed.text
    assert listed.json() == {
        "requests": [
            {"request_id": request_id, "runtime_owner": OWNER, "runtime_epoch": 1}
        ]
    }

    past = dispatch_client.get(
        f"{INTERNAL_PREFIX}/runtime-owners",
        params={"after": request_id},
        headers=WORKER_HEADERS,
    )
    assert past.status_code == 200, past.text
    assert past.json() == {"requests": []}
    before_it = dispatch_client.get(
        f"{INTERNAL_PREFIX}/runtime-owners",
        params={"after": str(UUID(int=0))},
        headers=WORKER_HEADERS,
    )
    assert [r["request_id"] for r in before_it.json()["requests"]] == [request_id]
    bad = dispatch_client.get(
        f"{INTERNAL_PREFIX}/runtime-owners",
        params={"after": "not-a-uuid"},
        headers=WORKER_HEADERS,
    )
    assert bad.status_code == 422, bad.text

    path = f"{INTERNAL_PREFIX}/requests/{request_id}/owner-lost"
    unauth_post = dispatch_client.post(
        path, json={"owner": OWNER, "runtime_epoch": 1}
    )
    assert unauth_post.status_code in {401, 403}, unauth_post.text
    stale = dispatch_client.post(
        path, json={"owner": OTHER_OWNER, "runtime_epoch": 1}, headers=WORKER_HEADERS
    )
    assert stale.status_code == 409, stale.text
    assert _http_code(stale) == "stale_owner"
    declared = dispatch_client.post(
        path, json={"owner": OWNER, "runtime_epoch": 1}, headers=WORKER_HEADERS
    )
    assert declared.status_code == 200, declared.text
    assert declared.json()["status"] == "cancellation_requested"
    assert declared.json()["terminal_cause"] == "owner_lost"
    viewed = dispatch_client.get(
        f"{INTERNAL_PREFIX}/requests/{request_id}", headers=WORKER_HEADERS
    )
    assert viewed.json()["status"] == "cancellation_requested"
    after = dispatch_client.get(
        f"{INTERNAL_PREFIX}/runtime-owners", headers=WORKER_HEADERS
    )
    assert after.json() == {"requests": []}


def test_list_runtime_owners_pages_by_request_id(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        running = [await _running(session, agent_id, issue=3000 + i) for i in range(5)]
        expected = sorted(facts.request_id for facts in running)

        seen: list[uuid.UUID] = []
        after: uuid.UUID | None = None
        pages = 0
        while True:
            page = await workitem_dispatch.list_runtime_owners(
                session, limit=2, after=after
            )
            if not page:
                break
            pages += 1
            ids = [row.request_id for row in page]
            assert ids == sorted(ids)
            assert after is None or ids[0] > after
            assert len(ids) <= 2
            seen.extend(ids)
            after = ids[-1]
        assert seen == expected
        assert pages == 3

        tail = await workitem_dispatch.list_runtime_owners(
            session, limit=50, after=expected[2]
        )
        assert [row.request_id for row in tail] == expected[3:]

    with_session(body)
