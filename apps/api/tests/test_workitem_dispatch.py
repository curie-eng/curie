"""WorkItem admission, dispatch, and internal-worker HTTP contract."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from channel_protocol import scoped_conversation_id
from curie_api import models, workitem_dispatch
from curie_api.config import get_settings
from curie_api.forges.types import GITHUB, RepositoryRef, TrackerIssueRef
from curie_api.main import create_app
from curie_api.routers import work_items
from curie_api.workitem_dispatch import (
    AcquireGrant,
    Admission,
    acquire,
    admit,
    cancel,
    claim_termination,
    defer,
    finish,
    heartbeat,
    record_termination,
    redispatch_lapsed_acquisitions,
    start,
)
from curie_api.workitems import lifecycle as workitems
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_workitems import _grant_opened_pull_request

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
        text("INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)"),
        {
            "id": agent_id,
            "name": f"acme-bot-{agent_id.hex[:8]}",
            "repo": repo,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
            "VALUES (:id, :agent_id, 'slack', :address, 'default')"
        ),
        {"id": uuid.uuid4(), "agent_id": agent_id, "address": address},
    )
    await session.commit()
    return agent_id


def _facts(
    agent_id: uuid.UUID,
    *,
    issue_number: int = 2573,
    repository_path: str = REPO,
    **overrides: Any,
) -> Admission:
    values: dict[str, Any] = {
        "agent_id": agent_id,
        "kind": "slack",
        "address": ADDRESS,
        "reply_conversation_id": WIRE_CONVERSATION,
        "issue": TrackerIssueRef(GITHUB, "github.com", "101", str(issue_number)),
        "repository": RepositoryRef(GITHUB, "github.com", "101", repository_path),
        "code_host_installation_id": 202,
        "objective": OBJECTIVE,
        "requester": REQUESTER,
        "request_id": uuid.uuid4(),
    }
    values.update(overrides)
    return Admission(**values)


def _facts_json(facts: Admission) -> dict[str, Any]:
    return {
        "agent_id": str(facts.agent_id),
        "kind": facts.kind,
        "address": facts.address,
        "reply_conversation_id": facts.reply_conversation_id,
        "tracker": {
            "kind": facts.issue.kind,
            "host": facts.issue.host,
            "scope_id": facts.issue.scope_id,
            "issue_id": facts.issue.issue_id,
        },
        "repository": {
            "code_host_kind": facts.repository.kind,
            "host": facts.repository.host,
            "project_id": facts.repository.project_id,
            "path": facts.repository.path,
        },
        "code_host_installation_id": facts.code_host_installation_id,
        "objective": facts.objective,
        "requester": facts.requester,
        "request_id": str(facts.request_id),
    }


def _acquire_body(generation: int) -> dict[str, Any]:
    return {
        "owner": OWNER,
        "generation": generation,
        "wake": workitem_dispatch.FactoryWake(
            kind="slack",
            address=ADDRESS,
            adapter="default",
            conversation_id=WIRE_CONVERSATION,
        ).model_dump(mode="json"),
        "context": None,
    }


async def _request_row(session: AsyncSession, request_id: uuid.UUID) -> Any:
    row = (
        (
            await session.execute(
                text(
                    "SELECT r.id, r.work_item_id, r.status, r.wait_deadline, "
                    "r.started_at, r.execution_deadline, r.version, "
                    "r.execution_attempts, r.capacity_deferrals, r.start_deferrals, "
                    "r.dispatch_generation, r.published_generation, "
                    "r.dispatch_not_before, r.last_deferral_reason, "
                    "r.runtime_owner, r.runtime_epoch, "
                    "r.runtime_heartbeat_expires_at, r.runtime_claim_name, "
                    "r.runtime_sandbox_name, r.terminal_cause, "
                    "r.termination_observation, r.acquire_owner, r.acquire_expires_at, "
                    "r.acquired_generation, w.version AS work_item_version, "
                    "w.cancelled_at "
                    "FROM curie.execution_requests r "
                    "JOIN curie.work_items w ON w.id = r.work_item_id "
                    "WHERE r.id = :id"
                ),
                {"id": request_id},
            )
        )
        .mappings()
        .one()
    )
    return row


@pytest.fixture
def allowlisted(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def dispatch_client(clean_db: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
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


def test_admit_replay_keeps_the_stored_deadline(clean_db: None, allowlisted: None) -> None:
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
        conversation = scoped_conversation_id("slack", ADDRESS, WIRE_CONVERSATION)
        stored = await session.scalar(
            text("SELECT conversation_id FROM curie.work_items WHERE id = :id"),
            {"id": first.work_item.id},
        )
        assert stored == conversation

    with_session(body)


def test_concurrent_same_uuid_admit_inserts_one_row(clean_db: None, allowlisted: None) -> None:
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
    ids = {getattr(getattr(result, "request", None), "id", None) for result in results}
    assert ids == {facts.request_id}

    async def verify(session: AsyncSession) -> None:
        assert (
            await session.scalar(
                text("SELECT count(*) FROM curie.execution_requests WHERE id = :id"),
                {"id": facts.request_id},
            )
            == 1
        )

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
                issue_number=2574,
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
                issue_number=2575,
                repository_path=OTHER_REPO,
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
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert unpublished.generation == 1
        assert unpublished.work_item_id == admitted.work_item.id
        assert unpublished.conversation_id == scoped_conversation_id(
            "slack", ADDRESS, WIRE_CONVERSATION
        )
        assert unpublished.wait_deadline == admitted.request.wait_deadline
        assert unpublished.repository_path == facts.repository.path

        stale = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=0,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert _code(stale) == "not_published"
        future = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=2,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert _code(future) in {"not_published", "not_dispatchable"}
        duplicate = await acquire(
            session,
            facts.request_id,
            owner=OTHER_OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert _code(duplicate) == "duplicate"

        cancelled = await cancel(
            session,
            work_item_id=admitted.work_item.id,
            expected_version=admitted.work_item.version,
        )
        assert cancelled.work_item.cancelled_at is not None
        refused = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert _code(refused) == "work_item_cancelled"

    with_session(body)


def test_default_acquire_lease_and_same_owner_renewal_use_database_time(
    clean_db: None, allowlisted: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS", raising=False)
    monkeypatch.delenv("WORK_ITEM_ACQUIRE_LEASE_SECONDS", raising=False)
    get_settings.cache_clear()

    async def body(session: AsyncSession) -> None:
        facts = _facts(await _agent_with_channel(session))
        await admit(session, facts)
        first = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert isinstance(first, workitem_dispatch.AcquireGrant)
        acquired = await _request_row(session, facts.request_id)
        now = await _now(session)
        assert 59 <= (acquired.acquire_expires_at - now).total_seconds() <= 61
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "acquire_expires_at = clock_timestamp() + interval '5 seconds' "
                "WHERE id = :id"
            ),
            {"id": facts.request_id},
        )
        await session.commit()
        shortened = await _request_row(session, facts.request_id)
        renewed = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=first.context,
        )
        assert renewed == first
        after = await _request_row(session, facts.request_id)
        now = await _now(session)
        assert 59 <= (after.acquire_expires_at - now).total_seconds() <= 61
        assert after.acquire_expires_at > shortened.acquire_expires_at
        assert (after.acquire_owner, after.acquired_generation) == (OWNER, 1)
        refused = await acquire(
            session,
            facts.request_id,
            owner=OTHER_OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert _code(refused) == "duplicate"

    with_session(body)


@pytest.mark.parametrize("expired", [False, True])
def test_default_acquire_lease_redispatches_only_after_expiry(
    clean_db: None, allowlisted: None, monkeypatch: pytest.MonkeyPatch, expired: bool
) -> None:
    for name in (
        "CURIE_WORK_ITEM_ACQUIRE_LEASE_SECONDS",
        "WORK_ITEM_ACQUIRE_LEASE_SECONDS",
        "CURIE_WORK_ITEM_BACKOFF_BASE_SECONDS",
        "WORK_ITEM_BACKOFF_BASE_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()

    async def body(session: AsyncSession) -> None:
        facts = _facts(await _agent_with_channel(session))
        await admit(session, facts)
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert isinstance(acquired, workitem_dispatch.AcquireGrant)
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "acquire_expires_at = clock_timestamp() "
                "+ (:seconds * interval '1 second') WHERE id = :id"
            ),
            {"id": facts.request_id, "seconds": -1 if expired else 1},
        )
        await session.commit()
        before = await _request_row(session, facts.request_id)
        count = await redispatch_lapsed_acquisitions(session)
        after = await _request_row(session, facts.request_id)
        now = await _now(session)
        assert count == (1 if expired else 0)
        if expired:
            assert after.dispatch_generation == before.dispatch_generation + 1
            assert after.acquire_owner is None
            assert after.acquire_expires_at is None
            assert after.acquired_generation is None
            assert after.last_deferral_reason == "acquire_lost"
            assert 9 <= (after.dispatch_not_before - now).total_seconds() <= 11
            replacement = await acquire(
                session,
                facts.request_id,
                owner=OTHER_OWNER,
                generation=after.dispatch_generation,
                wake=await factory_wake(session, facts.request_id),
                context=None,
            )
            assert isinstance(replacement, workitem_dispatch.AcquireGrant)
            assert replacement.generation == before.dispatch_generation + 1
        else:
            assert after.dispatch_generation == before.dispatch_generation
            assert after.acquire_owner == OWNER
            assert after.acquire_expires_at == before.acquire_expires_at
            refused = await acquire(
                session,
                facts.request_id,
                owner=OTHER_OWNER,
                generation=after.dispatch_generation,
                wake=await factory_wake(session, facts.request_id),
                context=None,
            )
            assert _code(refused) == "duplicate"

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
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
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

        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=after_busy.dispatch_generation,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert isinstance(acquired, AcquireGrant), acquired
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
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert isinstance(acquired, AcquireGrant), acquired
        started = await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
            context=acquired.context,
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
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert isinstance(acquired, AcquireGrant), acquired
        await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
            context=acquired.context,
        )
        before = await _request_row(session, facts.request_id)
        stale_heartbeat = await heartbeat(session, facts.request_id, runtime_epoch=2)
        assert _code(stale_heartbeat) == "stale_owner"
        stale_finish = await finish(
            session,
            facts.request_id,
            runtime_epoch=2,
            outcome="completed",
            cause="completed",
            detail=None,
            ci_fix_round=None,
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


def test_late_running_heartbeat_renews_from_database_time(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = await _running(session, await _agent_with_channel(session))
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "runtime_heartbeat_expires_at = clock_timestamp() "
                "- interval '5 seconds' WHERE id = :id"
            ),
            {"id": facts.request_id},
        )
        await session.commit()
        before = await _request_row(session, facts.request_id)
        called_at = await _now(session)
        assert before.runtime_heartbeat_expires_at <= called_at - timedelta(seconds=5)
        renewed = await heartbeat(session, facts.request_id, runtime_epoch=before.runtime_epoch)
        returned_at = await _now(session)
        assert isinstance(renewed, workitem_dispatch.HeartbeatResult), renewed
        assert (
            renewed.status,
            renewed.terminal_cause,
            renewed.work_item_cancelled,
        ) == ("running", None, False)
        after = await _request_row(session, facts.request_id)
        ttl = timedelta(seconds=get_settings().work_item_runtime_ttl_seconds)
        assert called_at + ttl <= after.runtime_heartbeat_expires_at <= returned_at + ttl
        assert (
            after.status,
            after.runtime_owner,
            after.runtime_epoch,
            after.version,
            after.work_item_version,
            after.started_at,
            after.execution_deadline,
            after.execution_attempts,
        ) == (
            before.status,
            before.runtime_owner,
            before.runtime_epoch,
            before.version,
            before.work_item_version,
            before.started_at,
            before.execution_deadline,
            before.execution_attempts,
        )

    with_session(body)


@pytest.mark.parametrize("reason", ["stale_epoch", "execution_deadline"])
def test_late_heartbeat_preserves_epoch_and_execution_deadline_fencing(
    clean_db: None, allowlisted: None, reason: str
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = await _running(session, await _agent_with_channel(session))
        original = await _request_row(session, facts.request_id)
        now = await _now(session)
        started_at = (
            now - timedelta(seconds=1801) if reason == "execution_deadline" else original.started_at
        )
        if reason == "execution_deadline":
            # Seed the historical window before restoring the immutable-field guard.
            await session.execute(text("ALTER TABLE curie.execution_requests DISABLE TRIGGER USER"))
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET started_at = :started_at, "
                "execution_deadline = :deadline, "
                "runtime_heartbeat_expires_at = :expired WHERE id = :id"
            ),
            {
                "id": facts.request_id,
                "started_at": started_at,
                "deadline": started_at + timedelta(seconds=1800),
                "expired": now - timedelta(seconds=5),
            },
        )
        if reason == "execution_deadline":
            await session.execute(text("ALTER TABLE curie.execution_requests ENABLE TRIGGER USER"))
        await session.commit()
        before = await _request_row(session, facts.request_id)
        refused = await heartbeat(
            session,
            facts.request_id,
            runtime_epoch=before.runtime_epoch + (reason == "stale_epoch"),
        )
        assert _code(refused) == "stale_owner"
        after = await _request_row(session, facts.request_id)
        assert dict(after) == dict(before)

    with_session(body)


@pytest.mark.parametrize("late", [False, True], ids=["on_time", "late"])
def test_cancellation_heartbeat_reports_only_before_lease_expiry(
    clean_db: None, allowlisted: None, late: bool
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = await _running(session, await _agent_with_channel(session))
        running = await _request_row(session, facts.request_id)
        cancelled = await cancel(
            session,
            work_item_id=running.work_item_id,
            expected_version=running.work_item_version,
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        now = await _now(session)
        expiry = now + timedelta(seconds=get_settings().work_item_runtime_ttl_seconds)
        if late:
            expiry = now - timedelta(seconds=5)
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "runtime_heartbeat_expires_at = :expiry WHERE id = :id"
            ),
            {"id": facts.request_id, "expiry": expiry},
        )
        await session.commit()
        before = await _request_row(session, facts.request_id)
        result = await heartbeat(session, facts.request_id, runtime_epoch=before.runtime_epoch)
        if late:
            assert _code(result) == "stale_owner"
        else:
            assert isinstance(result, workitem_dispatch.HeartbeatResult), result
            assert (
                result.status,
                result.terminal_cause,
                result.work_item_cancelled,
            ) == ("cancellation_requested", "issue_cancelled", True)
        after = await _request_row(session, facts.request_id)
        assert dict(after) == dict(before)

    with_session(body)


def test_termination_claim_waits_for_heartbeat_expiry_and_maps_owner_lost(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        admitted = await admit(session, facts)
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert isinstance(acquired, AcquireGrant), acquired
        await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
            context=acquired.context,
        )
        cancelled = await cancel(
            session,
            work_item_id=admitted.work_item.id,
            expected_version=admitted.work_item.version,
        )
        assert cancelled.request is not None
        live = await claim_termination(session, facts.request_id, owner=OTHER_OWNER)
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
        claimed = await claim_termination(session, facts.request_id, owner=OTHER_OWNER)
        assert claimed.runtime_epoch == 2

        owner_lost_facts = _facts(agent_id, issue_number=2574)
        owner_lost = await admit(session, owner_lost_facts)
        acquired = await acquire(
            session,
            owner_lost_facts.request_id,
            owner=OWNER,
            generation=1,
            wake=await factory_wake(session, owner_lost_facts.request_id),
            context=None,
        )
        assert isinstance(acquired, AcquireGrant), acquired
        await start(
            session,
            owner_lost_facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
            context=acquired.context,
        )
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET "
                "runtime_heartbeat_expires_at = :expired WHERE id = :id"
            ),
            {
                "id": owner_lost_facts.request_id,
                "expired": await _now(session)
                - timedelta(seconds=get_settings().work_item_runtime_ttl_seconds + 5),
            },
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
            _acquire_body(1),
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
                "context": {
                    "agent_id": admissions["agent_id"],
                    "tenant_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                    "binding_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
                    "kind": "slack",
                    "address": ADDRESS,
                    "adapter": "default",
                },
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
        json=_acquire_body(1),
        headers=WORKER_HEADERS,
    )
    assert acquired.status_code == 200, acquired.text
    assert acquired.json()["repository_path"] == facts.repository.path
    started = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/start",
        json={
            "owner": OWNER,
            "generation": 1,
            "claim_name": CLAIM_NAME,
            "sandbox_name": SANDBOX_NAME,
            "context": acquired.json()["context"],
        },
        headers=WORKER_HEADERS,
    )
    assert started.status_code == 200, started.text
    start_body = started.json()
    assert start_body["runtime_epoch"] == 1
    running = dispatch_client.get(
        f"{INTERNAL_PREFIX}/running",
        params={
            "conversation_id": acquired.json()["conversation_id"],
            "wake_kind": "slack",
            "wake_address": ADDRESS,
            "wake_adapter": "default",
            "wake_conversation_id": WIRE_CONVERSATION,
        },
        headers=WORKER_HEADERS,
    )
    assert running.status_code == 200, running.text
    running_body = running.json()
    assert running_body["request_id"] == request_id
    assert running_body["agent_id"] == str(agent_id)
    assert running_body["generation"] == acquired.json()["generation"]
    assert running_body["conversation_id"] == acquired.json()["conversation_id"]
    assert running_body["wait_deadline"] == acquired.json()["wait_deadline"]
    assert running_body["repository_path"] == facts.repository.path
    assert running_body["execution_deadline"] == start_body["execution_deadline"]
    assert running_body["lease_held_to_deadline"] is False
    assert running_body["context"] == acquired.json()["context"]
    assert running_body["refusal"] is None
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


async def _running(session: AsyncSession, agent_id: uuid.UUID, *, issue: int = 2573) -> Admission:
    facts = _facts(agent_id, issue_number=issue)
    await admit(session, facts)
    acquired = await acquire(
        session,
        facts.request_id,
        owner=OWNER,
        generation=1,
        wake=await factory_wake(session, facts.request_id),
        context=None,
    )
    assert isinstance(acquired, AcquireGrant), acquired
    await start(
        session,
        facts.request_id,
        owner=OWNER,
        generation=1,
        claim_name=CLAIM_NAME,
        sandbox_name=SANDBOX_NAME,
        context=acquired.context,
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
        assert [(o.request_id, o.runtime_owner, o.runtime_epoch) for o in owners] == [
            (facts.request_id, OWNER, 1)
        ]

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

        claimed = await claim_termination(session, facts.request_id, owner=OTHER_OWNER)
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


def test_declare_owner_lost_unknown_request_is_not_found(clean_db: None, allowlisted: None) -> None:
    async def body(session: AsyncSession) -> None:
        refused = await workitem_dispatch.declare_owner_lost(
            session, uuid.uuid4(), owner=OWNER, runtime_epoch=1
        )
        assert _code(refused) == "not_found"

    with_session(body)


def test_approval_hold_is_not_an_orphan(clean_db: None, allowlisted: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        held = await _running(session, agent_id)
        live = await _running(session, agent_id, issue=2574)
        await workitem_dispatch.hold_for_approval(session, held.request_id, runtime_epoch=1)
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


def test_running_reports_approval_hold_and_heartbeat_transitions_read_only(
    clean_db: None,
    allowlisted: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = await _running(session, await _agent_with_channel(session))
        wake = await factory_wake(session, facts.request_id)
        conversation = scoped_conversation_id("slack", ADDRESS, WIRE_CONVERSATION)

        async def observe(expected: bool) -> None:
            before = await _request_row(session, facts.request_id)
            state, result = await workitem_dispatch.running_for_conversation(
                session,
                conversation,
                wake=wake,
            )
            assert state == "running"
            assert isinstance(result, workitem_dispatch.RunningResult)
            assert result.request_id == facts.request_id
            assert result.runtime_epoch == before.runtime_epoch
            assert result.lease_held_to_deadline is expected
            assert result.context is not None and result.context.agent_id == facts.agent_id
            assert result.refusal is None
            assert dict(await _request_row(session, facts.request_id)) == dict(before)

        await observe(False)
        held = await workitem_dispatch.hold_for_approval(
            session,
            facts.request_id,
            runtime_epoch=1,
        )
        assert isinstance(held, workitem_dispatch.HeartbeatResult)
        row = await _request_row(session, facts.request_id)
        assert row.runtime_heartbeat_expires_at == row.execution_deadline
        await observe(True)
        renewed = await heartbeat(session, facts.request_id, runtime_epoch=1)
        assert isinstance(renewed, workitem_dispatch.HeartbeatResult)
        row = await _request_row(session, facts.request_id)
        assert row.runtime_heartbeat_expires_at != row.execution_deadline
        await observe(False)
        held_again = await workitem_dispatch.hold_for_approval(
            session,
            facts.request_id,
            runtime_epoch=1,
        )
        assert isinstance(held_again, workitem_dispatch.HeartbeatResult)
        await observe(True)

    with_session(body)


def test_expired_approval_hold_is_an_orphan(clean_db: None, allowlisted: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        held = await _running(session, agent_id)
        await workitem_dispatch.hold_for_approval(session, held.request_id, runtime_epoch=1)
        # Move the whole window into the past; the hold parks the lease at the
        # deadline, so both columns stay equal.
        past = (await _now(session)) - timedelta(minutes=5)
        # A trigger makes the deadline write once; lift it for this fixture only.
        await session.execute(text("ALTER TABLE curie.execution_requests DISABLE TRIGGER USER"))
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET started_at = :started, "
                "execution_deadline = :past, runtime_heartbeat_expires_at = :past "
                "WHERE id = :id"
            ),
            {
                "started": past - timedelta(minutes=5),
                "past": past,
                "id": held.request_id,
            },
        )
        await session.execute(text("ALTER TABLE curie.execution_requests ENABLE TRIGGER USER"))
        await session.commit()
        before = await _request_row(session, held.request_id)
        assert before.runtime_heartbeat_expires_at == before.execution_deadline

        state, result = await workitem_dispatch.running_for_conversation(
            session,
            scoped_conversation_id("slack", ADDRESS, WIRE_CONVERSATION),
            wake=await factory_wake(session, held.request_id),
        )
        assert state == "ended"
        assert result is None
        assert dict(await _request_row(session, held.request_id)) == dict(before)

        owners = await workitem_dispatch.list_runtime_owners(session, limit=50)
        assert [o.request_id for o in owners] == [held.request_id]

        lost = await workitem_dispatch.declare_owner_lost(
            session, held.request_id, owner=OWNER, runtime_epoch=1
        )
        assert not hasattr(lost, "code"), lost
        assert lost.status == "cancellation_requested"
        assert lost.terminal_cause == "owner_lost"
        after = await _request_row(session, held.request_id)
        assert after.status == "cancellation_requested"
        assert after.terminal_cause == "owner_lost"

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
    acquired = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/acquire",
        json=_acquire_body(1),
        headers=WORKER_HEADERS,
    )
    assert acquired.status_code == 200, acquired.text
    started = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/start",
        json={
            "owner": OWNER,
            "generation": 1,
            "claim_name": CLAIM_NAME,
            "sandbox_name": SANDBOX_NAME,
            "context": acquired.json()["context"],
        },
        headers=WORKER_HEADERS,
    )
    assert started.status_code == 200, started.text

    unauth = dispatch_client.get(f"{INTERNAL_PREFIX}/runtime-owners")
    assert unauth.status_code in {401, 403}, unauth.text
    listed = dispatch_client.get(f"{INTERNAL_PREFIX}/runtime-owners", headers=WORKER_HEADERS)
    assert listed.status_code == 200, listed.text
    assert listed.json() == {
        "requests": [{"request_id": request_id, "runtime_owner": OWNER, "runtime_epoch": 1}]
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
    unauth_post = dispatch_client.post(path, json={"owner": OWNER, "runtime_epoch": 1})
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
    viewed = dispatch_client.get(f"{INTERNAL_PREFIX}/requests/{request_id}", headers=WORKER_HEADERS)
    assert viewed.json()["status"] == "cancellation_requested"
    after = dispatch_client.get(f"{INTERNAL_PREFIX}/runtime-owners", headers=WORKER_HEADERS)
    assert after.json() == {"requests": []}


def test_list_runtime_owners_pages_by_request_id(clean_db: None, allowlisted: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        running = [await _running(session, agent_id, issue=3000 + i) for i in range(5)]
        expected = sorted(facts.request_id for facts in running)

        seen: list[uuid.UUID] = []
        after: uuid.UUID | None = None
        pages = 0
        while True:
            page = await workitem_dispatch.list_runtime_owners(session, limit=2, after=after)
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

        tail = await workitem_dispatch.list_runtime_owners(session, limit=50, after=expected[2])
        assert [row.request_id for row in tail] == expected[3:]

    with_session(body)


# --- #4170: a request whose sandbox never starts stops being retried --------

START_REASONS = [f"not_started:classified_failure-{n}" for n in range(1, 6)]
START_CURVE = [10, 20, 40, 80]


async def _start_row(session: AsyncSession, request_id: uuid.UUID) -> Any:
    return (
        (
            await session.execute(
                text(
                    "SELECT status, start_deferrals, capacity_deferrals, "
                    "dispatch_generation, dispatch_not_before, started_at, "
                    "execution_deadline, terminal_at, terminal_cause, "
                    "last_deferral_reason, execution_attempts "
                    "FROM curie.execution_requests WHERE id = :id"
                ),
                {"id": request_id},
            )
        )
        .mappings()
        .one()
    )


async def _acquire_and_defer(
    session: AsyncSession,
    request_id: uuid.UUID,
    *,
    reason: str,
    capacity: bool,
) -> tuple[Any, datetime, datetime]:
    """Acquire the current generation, defer it, and bracket the defer's clock."""

    row = await _start_row(session, request_id)
    granted = await acquire(
        session,
        request_id,
        owner=OWNER,
        generation=row.dispatch_generation,
        wake=await factory_wake(session, request_id),
        context=None,
    )
    assert getattr(granted, "code", None) is None, granted
    before = await _now(session)
    result = await defer(
        session,
        request_id,
        owner=OWNER,
        generation=row.dispatch_generation,
        reason=reason,
        capacity=capacity,
    )
    after = await _now(session)
    assert getattr(result, "code", None) is None, result
    return result, before, after


def _assert_delay(not_before: datetime, before: datetime, after: datetime, seconds: int) -> None:
    delay = timedelta(seconds=seconds)
    assert before + delay <= not_before <= after + delay, (not_before, before, after, seconds)


def test_start_deferrals_back_off_exponentially_and_keep_the_request_waiting(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        for count, (reason, seconds) in enumerate(
            zip(START_REASONS[:4], START_CURVE, strict=True), start=1
        ):
            result, before, after = await _acquire_and_defer(
                session, facts.request_id, reason=reason, capacity=False
            )
            row = await _start_row(session, facts.request_id)
            assert row.status == "waiting"
            assert row.start_deferrals == count
            assert row.capacity_deferrals == 0
            assert row.last_deferral_reason == reason
            assert row.started_at is None
            assert row.terminal_cause is None
            assert row.terminal_at is None
            _assert_delay(row.dispatch_not_before, before, after, seconds)
            assert result.not_before == row.dispatch_not_before
            assert result.terminal_cause is None
            assert result.dispatch_generation == row.dispatch_generation

    with_session(body)


def test_fifth_start_deferral_fails_the_request_as_start_failed(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        for reason in START_REASONS[:4]:
            await _acquire_and_defer(session, facts.request_id, reason=reason, capacity=False)
        result, before, after = await _acquire_and_defer(
            session, facts.request_id, reason=START_REASONS[4], capacity=False
        )
        assert result.not_before is None
        assert result.terminal_cause == "start_failed"
        row = await _start_row(session, facts.request_id)
        assert row.status == "failed"
        assert row.terminal_cause == "start_failed"
        assert row.start_deferrals == 5
        assert row.capacity_deferrals == 0
        assert row.execution_attempts == 0
        assert row.started_at is None
        assert row.execution_deadline is None
        assert row.terminal_at is not None
        assert before <= row.terminal_at <= after
        assert row.last_deferral_reason == START_REASONS[4]

        again = await defer(
            session,
            facts.request_id,
            owner=OWNER,
            generation=row.dispatch_generation,
            reason="not_started:classified_failure",
            capacity=False,
        )
        assert _code(again) == "not_dispatchable"
        reacquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=row.dispatch_generation,
            wake=await factory_wake(session, facts.request_id),
            context=None,
        )
        assert _code(reacquired) == "not_dispatchable"
        unchanged = await _start_row(session, facts.request_id)
        assert unchanged.status == "failed"
        assert unchanged.start_deferrals == 5

    with_session(body)


def test_capacity_defers_never_count_toward_the_start_deferral_cap(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        for reason in START_REASONS[:4]:
            await _acquire_and_defer(session, facts.request_id, reason=reason, capacity=False)
        for count, seconds in enumerate([10, 20, 40, 80, 120, 120], start=1):
            result, before, after = await _acquire_and_defer(
                session, facts.request_id, reason="capacity", capacity=True
            )
            row = await _start_row(session, facts.request_id)
            assert row.status == "waiting"
            assert row.start_deferrals == 4
            assert row.capacity_deferrals == count
            assert row.terminal_cause is None
            _assert_delay(row.dispatch_not_before, before, after, seconds)
            assert result.terminal_cause is None

        fresh = _facts(agent_id, issue_number=4170)
        await admit(session, fresh)
        for count in range(1, 7):
            await _acquire_and_defer(session, fresh.request_id, reason="capacity", capacity=True)
            row = await _start_row(session, fresh.request_id)
            assert row.status == "waiting"
            assert row.start_deferrals == 0
            assert row.capacity_deferrals == count

    with_session(body)


def test_thread_busy_defers_wait_flat_and_never_count_toward_the_start_cap(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        for _ in range(6):
            result, before, after = await _acquire_and_defer(
                session, facts.request_id, reason="thread_busy", capacity=False
            )
            row = await _start_row(session, facts.request_id)
            assert row.status == "waiting"
            assert row.start_deferrals == 0
            assert row.capacity_deferrals == 0
            assert row.terminal_cause is None
            assert row.terminal_at is None
            assert row.last_deferral_reason == "thread_busy"
            _assert_delay(row.dispatch_not_before, before, after, 10)
            assert result.not_before == row.dispatch_not_before
            assert result.terminal_cause is None

    with_session(body)


def test_thread_busy_between_start_deferrals_neither_resets_nor_advances_the_cap(
    clean_db: None, allowlisted: None
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        for reason in START_REASONS[:4]:
            await _acquire_and_defer(session, facts.request_id, reason=reason, capacity=False)
        busy, before, after = await _acquire_and_defer(
            session, facts.request_id, reason="thread_busy", capacity=False
        )
        row = await _start_row(session, facts.request_id)
        assert row.status == "waiting"
        assert row.start_deferrals == 4
        assert row.terminal_cause is None
        assert busy.terminal_cause is None
        _assert_delay(row.dispatch_not_before, before, after, 10)

        final, _, _ = await _acquire_and_defer(
            session, facts.request_id, reason=START_REASONS[4], capacity=False
        )
        assert final.terminal_cause == "start_failed"
        row = await _start_row(session, facts.request_id)
        assert (row.status, row.terminal_cause) == ("failed", "start_failed")
        assert row.start_deferrals == 5

    with_session(body)


def test_only_non_capacity_not_started_reasons_count_toward_the_start_cap(
    clean_db: None, allowlisted: None
) -> None:
    prefix = workitem_dispatch.START_DEFERRAL_REASON_PREFIX
    assert prefix == "not_started:"

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        for _ in range(6):
            result, before, after = await _acquire_and_defer(
                session, facts.request_id, reason="runtime_unavailable", capacity=False
            )
            row = await _start_row(session, facts.request_id)
            assert row.status == "waiting"
            assert row.start_deferrals == 0
            assert row.capacity_deferrals == 0
            assert row.terminal_cause is None
            assert row.terminal_at is None
            assert row.last_deferral_reason == "runtime_unavailable"
            _assert_delay(row.dispatch_not_before, before, after, 10)
            assert result.terminal_cause is None

        counted = _facts(agent_id, issue_number=4171)
        await admit(session, counted)
        for reason in START_REASONS[:4]:
            assert reason.startswith(prefix)
            await _acquire_and_defer(session, counted.request_id, reason=reason, capacity=False)
        other, _, _ = await _acquire_and_defer(
            session, counted.request_id, reason="runtime_unavailable", capacity=False
        )
        assert other.terminal_cause is None
        row = await _start_row(session, counted.request_id)
        assert (row.status, row.start_deferrals) == ("waiting", 4)

        final, _, _ = await _acquire_and_defer(
            session, counted.request_id, reason="not_started:classified_failure", capacity=False
        )
        assert final.terminal_cause == "start_failed"
        row = await _start_row(session, counted.request_id)
        assert (row.status, row.terminal_cause) == ("failed", "start_failed")
        assert row.start_deferrals == 5

    with_session(body)


def test_start_deferral_limit_is_read_from_settings(
    clean_db: None, allowlisted: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_START_DEFERRAL_LIMIT", "2")
    get_settings.cache_clear()
    assert get_settings().work_item_start_deferral_limit == 2

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        first, _, _ = await _acquire_and_defer(
            session, facts.request_id, reason=START_REASONS[0], capacity=False
        )
        assert first.terminal_cause is None
        assert (await _start_row(session, facts.request_id)).status == "waiting"
        second, _, _ = await _acquire_and_defer(
            session, facts.request_id, reason=START_REASONS[1], capacity=False
        )
        assert second.terminal_cause == "start_failed"
        assert second.not_before is None
        row = await _start_row(session, facts.request_id)
        assert (row.status, row.terminal_cause) == ("failed", "start_failed")
        assert row.start_deferrals == 2
        assert row.last_deferral_reason == START_REASONS[1]

    with_session(body)


def test_http_defer_reports_start_failed_on_the_fifth_start_deferral(
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

    generation = 1
    bodies: list[dict[str, Any]] = []
    for reason in START_REASONS:
        acquired = dispatch_client.post(
            f"{INTERNAL_PREFIX}/requests/{request_id}/acquire",
            json=_acquire_body(generation),
            headers=WORKER_HEADERS,
        )
        assert acquired.status_code == 200, acquired.text
        deferred = dispatch_client.post(
            f"{INTERNAL_PREFIX}/requests/{request_id}/defer",
            json={
                "owner": OWNER,
                "generation": generation,
                "reason": reason,
                "capacity": False,
            },
            headers=WORKER_HEADERS,
        )
        assert deferred.status_code == 200, deferred.text
        bodies.append(deferred.json())
        generation = deferred.json()["dispatch_generation"]

    for body in bodies[:4]:
        assert body["terminal_cause"] is None
        assert body["not_before"] is not None
    assert bodies[4]["terminal_cause"] == "start_failed"
    assert bodies[4]["not_before"] is None

    viewed = dispatch_client.get(f"{INTERNAL_PREFIX}/requests/{request_id}", headers=WORKER_HEADERS)
    assert viewed.status_code == 200, viewed.text
    view = viewed.json()
    assert view["status"] == "failed"
    assert view["terminal_cause"] == "start_failed"
    assert view["started_at"] is None
    assert view["execution_deadline"] is None
    assert view["terminal_at"] is not None
    assert view["last_deferral_reason"] == START_REASONS[4]

    refused = dispatch_client.post(
        f"{INTERNAL_PREFIX}/requests/{request_id}/acquire",
        json=_acquire_body(generation),
        headers=WORKER_HEADERS,
    )
    assert refused.status_code == 409, refused.text
    assert _http_code(refused) == "not_dispatchable"


async def _assert_factory_neighbor_starts(session: AsyncSession) -> None:
    address = "C0EXAMPLE2"
    agent_id = await _agent_with_channel(session, address=address)
    facts = _facts(agent_id, issue_number=4620, address=address)
    admitted = await admit(session, facts)
    assert isinstance(admitted, workitems.WorkItemOutcome)
    acquired = await acquire(
        session,
        facts.request_id,
        owner=OTHER_OWNER,
        generation=1,
        wake=await factory_wake(session, facts.request_id),
        context=None,
    )
    assert isinstance(acquired, workitem_dispatch.AcquireGrant)
    assert acquired.context.agent_id == agent_id
    started = await start(
        session,
        facts.request_id,
        owner=OTHER_OWNER,
        generation=1,
        claim_name="claim-neighbor",
        sandbox_name="sandbox-neighbor",
        context=acquired.context,
    )
    assert isinstance(started, workitem_dispatch.StartResult)
    row = await _request_row(session, facts.request_id)
    assert (row.status, row.runtime_owner, row.execution_attempts) == ("running", OTHER_OWNER, 1)


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "github"},
        {"address": "C0EXAMPLE3"},
        {"adapter": "other-bot"},
        {"conversation_id": "1700000000.000999"},
    ],
)
def test_factory_wake_mismatch_leaves_authority_untouched_and_valid_delivery_starts(
    clean_db: None, allowlisted: None, change: dict[str, str]
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = _facts(await _agent_with_channel(session))
        await admit(session, facts)
        wake = await factory_wake(session, facts.request_id)
        before = dict(await _request_row(session, facts.request_id))
        refused = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=wake.model_copy(update=change),
            context=None,
        )
        assert _code(refused) == "factory_wake_mismatch"
        assert dict(await _request_row(session, facts.request_id)) == before
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=wake,
            context=None,
        )
        assert isinstance(acquired, workitem_dispatch.AcquireGrant)
        started = await start(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
            context=acquired.context,
        )
        assert isinstance(started, workitem_dispatch.StartResult)
        row = await _request_row(session, facts.request_id)
        assert (row.status, row.runtime_owner, row.execution_attempts) == ("running", OWNER, 1)

    with_session(body)


async def _invalidate_factory_binding(
    session: AsyncSession, agent_id: uuid.UUID, mutation: str
) -> None:
    if mutation == "tenant":
        tenant_id = uuid.uuid4()
        await session.execute(
            text(
                "INSERT INTO curie.tenants (id, deployment_id, status) "
                "VALUES (:id, :deployment, 'active')"
            ),
            {"id": tenant_id, "deployment": str(uuid.uuid4())},
        )
        await session.execute(
            text("UPDATE curie.agent_channels SET tenant_id = :tenant WHERE agent_id = :agent"),
            {"tenant": tenant_id, "agent": agent_id},
        )
    elif mutation == "removed":
        await session.execute(
            text("DELETE FROM curie.agent_channels WHERE agent_id = :agent"), {"agent": agent_id}
        )
    elif mutation == "moved":
        await session.execute(
            text("UPDATE curie.agent_channels SET address = 'C0EXAMPLE3' WHERE agent_id = :agent"),
            {"agent": agent_id},
        )
    elif mutation == "replaced":
        await session.execute(
            text("UPDATE curie.agent_channels SET id = :binding WHERE agent_id = :agent"),
            {"binding": uuid.uuid4(), "agent": agent_id},
        )
    else:
        raise AssertionError(mutation)
    await session.commit()


@pytest.mark.parametrize("phase", ["acquire", "renew", "start"])
@pytest.mark.parametrize(
    "mutation, code",
    [
        ("tenant", "factory_tenant_mismatch"),
        ("removed", "factory_binding_missing"),
        ("moved", "factory_binding_missing"),
    ],
)
def test_invalid_factory_authority_defers_without_starting_while_neighbor_progresses(
    clean_db: None, allowlisted: None, phase: str, mutation: str, code: str
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = _facts(agent_id)
        await admit(session, facts)
        wake = await factory_wake(session, facts.request_id)
        context = None
        if phase != "acquire":
            granted = await acquire(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                wake=wake,
                context=None,
            )
            assert isinstance(granted, workitem_dispatch.AcquireGrant)
            context = granted.context
        before = await _request_row(session, facts.request_id)
        await _invalidate_factory_binding(session, agent_id, mutation)
        if phase == "start":
            assert context is not None
            refused = await start(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                claim_name=CLAIM_NAME,
                sandbox_name=SANDBOX_NAME,
                context=context,
            )
        else:
            refused = await acquire(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                wake=wake,
                context=context,
            )
        assert _code(refused) == code
        after = await _request_row(session, facts.request_id)
        assert after.status == "waiting"
        assert after.last_deferral_reason == f"not_started:{code}"
        assert after.start_deferrals == before.start_deferrals + 1
        assert after.dispatch_generation == before.dispatch_generation + 1
        assert after.wait_deadline == before.wait_deadline
        assert after.execution_attempts == 0
        assert after.started_at is None
        assert after.runtime_owner is None
        assert after.runtime_epoch == before.runtime_epoch
        assert after.acquire_owner is None
        assert after.acquire_expires_at is None
        assert after.acquired_generation is None
        assert after.terminal_cause is None
        await _assert_factory_neighbor_starts(session)
        assert dict(await _request_row(session, facts.request_id)) == dict(after)

    with_session(body)


@pytest.mark.parametrize("phase", ["renew", "start"])
@pytest.mark.parametrize(
    "field, code",
    [
        ("agent_id", "factory_agent_mismatch"),
        ("tenant_id", "factory_tenant_mismatch"),
        ("binding_id", "factory_binding_changed"),
    ],
)
def test_factory_revalidation_rejects_foreign_context_and_preserves_neighbor(
    clean_db: None, allowlisted: None, phase: str, field: str, code: str
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = _facts(await _agent_with_channel(session))
        await admit(session, facts)
        wake = await factory_wake(session, facts.request_id)
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=wake,
            context=None,
        )
        assert isinstance(acquired, workitem_dispatch.AcquireGrant)
        context = acquired.context.model_copy(update={field: uuid.uuid4()})
        if phase == "renew":
            refused = await acquire(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                wake=wake,
                context=context,
            )
        else:
            refused = await start(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                claim_name=CLAIM_NAME,
                sandbox_name=SANDBOX_NAME,
                context=context,
            )
        assert _code(refused) == code
        row = await _request_row(session, facts.request_id)
        assert row.status == "waiting"
        assert row.last_deferral_reason == f"not_started:{code}"
        assert row.start_deferrals == 1
        assert row.dispatch_generation == 2
        assert row.acquire_owner is None
        assert row.execution_attempts == 0
        assert row.started_at is None
        await _assert_factory_neighbor_starts(session)

    with_session(body)


@pytest.mark.parametrize("phase", ["renew", "start"])
def test_replacing_current_binding_invalidates_the_acquired_context(
    clean_db: None, allowlisted: None, phase: str
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = _facts(await _agent_with_channel(session))
        await admit(session, facts)
        wake = await factory_wake(session, facts.request_id)
        acquired = await acquire(
            session,
            facts.request_id,
            owner=OWNER,
            generation=1,
            wake=wake,
            context=None,
        )
        assert isinstance(acquired, workitem_dispatch.AcquireGrant)
        await _invalidate_factory_binding(session, facts.agent_id, "replaced")
        if phase == "renew":
            refused = await acquire(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                wake=wake,
                context=acquired.context,
            )
        else:
            refused = await start(
                session,
                facts.request_id,
                owner=OWNER,
                generation=1,
                claim_name=CLAIM_NAME,
                sandbox_name=SANDBOX_NAME,
                context=acquired.context,
            )
        assert _code(refused) == "factory_binding_changed"
        row = await _request_row(session, facts.request_id)
        assert row.last_deferral_reason == "not_started:factory_binding_changed"
        assert row.status == "waiting"
        assert row.start_deferrals == 1
        assert row.acquire_owner is None
        assert row.started_at is None
        await _assert_factory_neighbor_starts(session)

    with_session(body)


async def _hold_running_for_ci_fix(session: AsyncSession, facts: Admission) -> None:
    row = await _request_row(session, facts.request_id)
    version_id, deployment_id = uuid.uuid4(), uuid.uuid4()
    lineage_id, approval_id, publication_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    base_sha, head_sha = "a1" * 20, "b2" * 20
    session.add(
        models.AgentVersion(
            id=version_id,
            agent_id=facts.agent_id,
            version_label="ci-hold",
            created_by="fixture",
        )
    )
    await session.flush()
    session.add(
        models.Deployment(
            id=deployment_id,
            agent_id=facts.agent_id,
            version_id=version_id,
            environment=models.Environment.dev,
        )
    )
    await session.flush()
    session.add(
        models.ThreadPublicationLineage(
            id=lineage_id,
            agent_id=facts.agent_id,
            deployment_id=deployment_id,
            conversation_id=scoped_conversation_id("slack", ADDRESS, WIRE_CONVERSATION),
            repo_full_name=REPO,
            base_sha=base_sha,
            branch=f"curie/ci-hold-{lineage_id.hex}",
            pr_number=77,
            pr_url=f"https://github.com/{REPO}/pull/77",
            head_sha=head_sha,
            code_host_kind="github",
            code_host_host="github.com",
            repository_project_id="101",
            code_host_installation_id=202,
            code_host_pr_id=f"PR_{lineage_id.hex}",
            base_ref="main",
        )
    )
    session.add(
        models.Approval(
            id=approval_id,
            agent_id=facts.agent_id,
            conversation_id=WIRE_CONVERSATION,
            author=REQUESTER,
            summary="Publish the CI fixture",
            reply_kind="slack",
            reply_channel=ADDRESS,
            dedupe_key=f"ci-hold-{publication_id.hex}",
            status="approved",
            purpose="publication",
        )
    )
    await session.flush()
    session.add(
        models.Publication(
            id=publication_id,
            approval_id=approval_id,
            deployment_id=deployment_id,
            workspace_conversation_id=scoped_conversation_id("slack", ADDRESS, WIRE_CONVERSATION),
            lineage_id=lineage_id,
            execution_request_id=facts.request_id,
            revision_number=1,
            repo_full_name=REPO,
            status="succeeded",
            base_sha=base_sha,
            changed_paths=["README.md"],
            title="Update README",
            body="Approved fixture publication.",
            reply_kind="slack",
            reply_channel=ADDRESS,
            result_url=f"https://github.com/{REPO}/pull/77",
        )
    )
    await session.commit()
    linked = await workitems.link_publication_lineage(
        session,
        work_item_id=row.work_item_id,
        request_id=facts.request_id,
        publication_lineage_id=lineage_id,
        expected_work_item_version=row.work_item_version,
        expected_request_version=row.version,
    )
    assert isinstance(linked, workitems.WorkItemOutcome)
    assert linked.request is not None
    before = await _request_row(session, facts.request_id)
    stale = await workitems.hold_for_ci_fix(
        session,
        work_item_id=row.work_item_id,
        request_id=facts.request_id,
        expected_request_version=linked.request.version,
        expected_publication_id=publication_id,
        expected_head_sha="c3" * 20,
    )
    assert stale is False
    assert dict(await _request_row(session, facts.request_id)) == dict(before)
    held = await workitems.hold_for_ci_fix(
        session,
        work_item_id=row.work_item_id,
        request_id=facts.request_id,
        expected_request_version=linked.request.version,
        expected_publication_id=publication_id,
        expected_head_sha=head_sha,
    )
    assert held is True
    current = await _request_row(session, facts.request_id)
    assert current.runtime_heartbeat_expires_at == current.execution_deadline


@pytest.mark.parametrize("hold_kind", ["none", "approval", "ci"])
def test_running_returns_real_execution_facts_and_named_current_authority_refusal(
    clean_db: None,
    allowlisted: None,
    hold_kind: str,
) -> None:
    async def body(session: AsyncSession) -> None:
        facts = await _running(session, await _agent_with_channel(session))
        if hold_kind == "ci":
            await _hold_running_for_ci_fix(session, facts)
        elif hold_kind == "approval":
            held = await workitem_dispatch.hold_for_approval(
                session,
                facts.request_id,
                runtime_epoch=1,
            )
            assert isinstance(held, workitem_dispatch.HeartbeatResult)
        wake = await factory_wake(session, facts.request_id)
        before = await _request_row(session, facts.request_id)
        conversation = scoped_conversation_id("slack", ADDRESS, WIRE_CONVERSATION)
        state, found = await workitem_dispatch.running_for_conversation(
            session,
            conversation,
            wake=wake,
        )
        assert state == "running"
        assert isinstance(found, workitem_dispatch.RunningResult)
        assert found.context is not None
        assert found.context.agent_id == facts.agent_id
        assert found.agent_id == facts.agent_id
        assert found.generation == before.dispatch_generation
        assert found.conversation_id == conversation
        assert found.repository_path == REPO
        assert found.wait_deadline == before.wait_deadline
        assert found.runtime_epoch == before.runtime_epoch
        assert found.execution_deadline == before.execution_deadline
        assert found.lease_held_to_deadline is (hold_kind != "none")
        assert found.refusal is None
        mismatched_state, mismatched = await workitem_dispatch.running_for_conversation(
            session,
            conversation,
            wake=wake.model_copy(update={"address": "C0EXAMPLE3"}),
        )
        assert mismatched_state == "running"
        assert _code(mismatched) == "factory_wake_mismatch"
        assert dict(await _request_row(session, facts.request_id)) == dict(before)
        await _invalidate_factory_binding(session, facts.agent_id, "removed")
        state, refused = await workitem_dispatch.running_for_conversation(
            session,
            conversation,
            wake=wake,
        )
        assert state == "running"
        assert isinstance(refused, workitem_dispatch.RunningResult)
        assert refused.context is None
        assert refused.refusal == "factory_binding_missing"
        assert refused.request_id == facts.request_id
        assert refused.agent_id == facts.agent_id
        assert refused.generation == found.generation
        assert refused.conversation_id == found.conversation_id
        assert refused.repository_path == found.repository_path
        assert refused.wait_deadline == found.wait_deadline
        assert refused.runtime_epoch == found.runtime_epoch
        assert refused.execution_deadline == found.execution_deadline
        assert refused.lease_held_to_deadline is (hold_kind != "none")
        assert dict(await _request_row(session, facts.request_id)) == dict(before)
        await _assert_factory_neighbor_starts(session)

    with_session(body)


# --- #4627: cancelling a held run lets another worker terminate it ----------

THIRD_OWNER = "curie-workers-c"


async def _full_request(session: AsyncSession, request_id: uuid.UUID) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                text("SELECT * FROM curie.execution_requests WHERE id = :id"),
                {"id": request_id},
            )
        )
        .mappings()
        .one()
    )
    return dict(row)


async def _held_for_approval(session: AsyncSession, agent_id: uuid.UUID) -> Admission:
    facts = await _running(session, agent_id)
    held = await workitem_dispatch.hold_for_approval(session, facts.request_id, runtime_epoch=1)
    assert not hasattr(held, "code"), held
    row = await _request_row(session, facts.request_id)
    assert row.status == "running"
    assert row.runtime_heartbeat_expires_at == row.execution_deadline
    return facts


async def _held_for_ci_fix(session: AsyncSession, agent_id: uuid.UUID) -> Admission:
    """A running request whose lease the CI gate parked for a fix turn."""

    facts = await _running(session, agent_id)
    row = await _request_row(session, facts.request_id)
    work_item = await workitems.lock_work_item(session, row.work_item_id)
    request = await workitems.lock_request(
        session, work_item_id=row.work_item_id, request_id=facts.request_id
    )
    assert work_item is not None and request is not None
    await _grant_opened_pull_request(
        session, await workitems.outcome(session, work_item, request), pr=4627
    )
    publication_id, head_sha = (
        await session.execute(
            text(
                "SELECT p.id, l.head_sha FROM curie.publications p "
                "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
                "WHERE p.execution_request_id = :id"
            ),
            {"id": facts.request_id},
        )
    ).one()
    linked = await _request_row(session, facts.request_id)
    held = await workitems.hold_for_ci_fix(
        session,
        work_item_id=linked.work_item_id,
        request_id=facts.request_id,
        expected_request_version=linked.version,
        expected_publication_id=publication_id,
        expected_head_sha=head_sha,
    )
    assert held is True
    parked = await _request_row(session, facts.request_id)
    assert parked.status == "running"
    assert parked.runtime_heartbeat_expires_at == parked.execution_deadline
    return facts


async def _assert_lease_released(
    session: AsyncSession, request_id: uuid.UUID, *, held: Any
) -> None:
    row = await _request_row(session, request_id)
    assert (row.status, row.terminal_cause) == ("cancellation_requested", "issue_cancelled")
    assert row.runtime_heartbeat_expires_at <= await _now(session)
    assert row.execution_deadline == held.execution_deadline
    assert row.runtime_owner == OWNER
    assert row.runtime_epoch == held.runtime_epoch


async def _assert_successor_terminates(
    session: AsyncSession, request_id: uuid.UUID, *, prior_epoch: int
) -> None:
    claimed = await claim_termination(session, request_id, owner=OTHER_OWNER)
    assert not hasattr(claimed, "code"), claimed
    assert claimed.runtime_epoch == prior_epoch + 1
    claimed_row = await _full_request(session, request_id)
    assert claimed_row["runtime_owner"] == OTHER_OWNER

    stale_beat = await heartbeat(session, request_id, runtime_epoch=prior_epoch)
    assert _code(stale_beat) == "stale_owner"
    stale_record = await record_termination(
        session, request_id, runtime_epoch=prior_epoch, observation=TERMINATION_OBSERVATION
    )
    assert _code(stale_record) == "stale_owner"
    assert await _full_request(session, request_id) == claimed_row

    settled = await record_termination(
        session,
        request_id,
        runtime_epoch=claimed.runtime_epoch,
        observation=TERMINATION_OBSERVATION,
    )
    assert not hasattr(settled, "code"), settled
    final = await _full_request(session, request_id)
    assert (
        final["status"],
        final["terminal_cause"],
        final["termination_observation"],
        final["teardown_unconfirmed_at"],
    ) == ("cancelled", "issue_cancelled", TERMINATION_OBSERVATION, None)


def test_held_cancel_approval_hold_lets_other_owner_terminate(
    clean_db: None, allowlisted: None
) -> None:
    # #4627: an approval hold parks the lease at the deadline; cancelling it
    # must release the lease so a successor can terminate the run.
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _held_for_approval(session, agent_id)
        held = await _request_row(session, facts.request_id)
        cancelled = await cancel(
            session, work_item_id=held.work_item_id, expected_version=held.work_item_version
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        await _assert_lease_released(session, facts.request_id, held=held)
        await _assert_successor_terminates(
            session, facts.request_id, prior_epoch=held.runtime_epoch
        )

    with_session(body)


def test_held_cancel_http_route_lets_other_owner_terminate(
    dispatch_client: TestClient,
) -> None:
    # #4627: the same approval-hold cancel, driven through the internal
    # cancel route the worker and intake call.
    async def setup(session: AsyncSession) -> tuple[Admission, Any]:
        agent_id = await _agent_with_channel(session)
        facts = await _held_for_approval(session, agent_id)
        return facts, await _request_row(session, facts.request_id)

    facts, held = with_session(setup)
    cancelled = dispatch_client.post(
        f"{INTERNAL_PREFIX}/{held.work_item_id}/cancel",
        json={"expected_version": held.work_item_version},
        headers=WORKER_HEADERS,
    )
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["request"]["status"] == "cancellation_requested"
    assert cancelled.json()["request"]["terminal_cause"] == "issue_cancelled"

    async def verify(session: AsyncSession) -> None:
        await _assert_lease_released(session, facts.request_id, held=held)
        await _assert_successor_terminates(
            session, facts.request_id, prior_epoch=held.runtime_epoch
        )

    with_session(verify)


def test_held_cancel_ci_fix_hold_lets_other_owner_terminate(
    clean_db: None, allowlisted: None
) -> None:
    # #4627: a CI-fix hold parks the lease the same way; cancel must release it.
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _held_for_ci_fix(session, agent_id)
        held = await _request_row(session, facts.request_id)
        cancelled = await cancel(
            session, work_item_id=held.work_item_id, expected_version=held.work_item_version
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        await _assert_lease_released(session, facts.request_id, held=held)
        await _assert_successor_terminates(
            session, facts.request_id, prior_epoch=held.runtime_epoch
        )

    with_session(body)


def test_held_cancel_relabel_stop_releases_the_held_lease(
    clean_db: None, allowlisted: None
) -> None:
    # #4627: the relabel stop path in readmit must release a held lease too.
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _held_for_approval(session, agent_id)
        held = await _request_row(session, facts.request_id)
        relabeled = await workitems.readmit(
            session,
            work_item_id=held.work_item_id,
            request_id=uuid.uuid4(),
            wait_deadline=await _now(session) + timedelta(hours=1),
            objective=OBJECTIVE,
            requester=REQUESTER,
            trigger_label=None,
        )
        assert isinstance(relabeled, workitems.WorkItemOutcome), relabeled
        assert relabeled.request is not None
        assert relabeled.request.id == facts.request_id
        await _assert_lease_released(session, facts.request_id, held=held)
        claimed = await claim_termination(session, facts.request_id, owner=OTHER_OWNER)
        assert not hasattr(claimed, "code"), claimed
        assert claimed.runtime_epoch == held.runtime_epoch + 1
        row = await _request_row(session, facts.request_id)
        assert row.runtime_owner == OTHER_OWNER

    with_session(body)


def test_held_cancel_leaves_a_live_unheld_lease_alone(clean_db: None, allowlisted: None) -> None:
    # #4627: only a parked lease is released; a live short lease keeps fencing.
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        for issue, relabel in ((2573, False), (2574, True)):
            facts = await _running(session, agent_id, issue=issue)
            live = await _request_row(session, facts.request_id)
            assert live.runtime_heartbeat_expires_at > await _now(session)
            assert live.runtime_heartbeat_expires_at < live.execution_deadline
            if relabel:
                stopped = await workitems.readmit(
                    session,
                    work_item_id=live.work_item_id,
                    request_id=uuid.uuid4(),
                    wait_deadline=await _now(session) + timedelta(hours=1),
                    objective=OBJECTIVE,
                    requester=REQUESTER,
                    trigger_label=None,
                )
            else:
                stopped = await cancel(
                    session,
                    work_item_id=live.work_item_id,
                    expected_version=live.work_item_version,
                )
            assert isinstance(stopped, workitems.WorkItemOutcome), stopped
            row = await _request_row(session, facts.request_id)
            assert (row.status, row.terminal_cause) == (
                "cancellation_requested",
                "issue_cancelled",
            )
            assert row.runtime_heartbeat_expires_at == live.runtime_heartbeat_expires_at
            before = await _full_request(session, facts.request_id)
            refused = await claim_termination(session, facts.request_id, owner=OTHER_OWNER)
            assert _code(refused) == "duplicate"
            assert await _full_request(session, facts.request_id) == before

    with_session(body)


def test_held_cancel_with_stale_version_leaves_the_held_row_untouched(
    clean_db: None, allowlisted: None
) -> None:
    # #4627: a refused cancel must not release the parked lease.
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _held_for_approval(session, agent_id)
        held = await _request_row(session, facts.request_id)
        before = await _full_request(session, facts.request_id)
        refused = await cancel(
            session,
            work_item_id=held.work_item_id,
            expected_version=held.work_item_version - 1,
        )
        assert _code(refused) == "stale_version"
        assert await _full_request(session, facts.request_id) == before
        after = await _request_row(session, facts.request_id)
        assert after.status == "running"
        assert after.runtime_heartbeat_expires_at == after.execution_deadline
        assert after.cancelled_at is None
        assert after.work_item_version == held.work_item_version

    with_session(body)


def test_held_cancel_termination_claimant_is_not_displaced(
    clean_db: None, allowlisted: None
) -> None:
    # #4627: once a worker owns the termination, later wakes must not steal it.
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent_with_channel(session)
        facts = await _held_for_approval(session, agent_id)
        held = await _request_row(session, facts.request_id)
        cancelled = await cancel(
            session, work_item_id=held.work_item_id, expected_version=held.work_item_version
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        # The run's own worker may always claim its termination; it is the
        # successor the later wakes race against.
        claimed = await claim_termination(session, facts.request_id, owner=OWNER)
        assert not hasattr(claimed, "code"), claimed
        assert claimed.runtime_epoch == held.runtime_epoch + 1
        before = await _full_request(session, facts.request_id)
        assert before["runtime_heartbeat_expires_at"] > await _now(session)
        for wake in (OTHER_OWNER, THIRD_OWNER):
            refused = await claim_termination(session, facts.request_id, owner=wake)
            assert _code(refused) == "duplicate"
            assert await _full_request(session, facts.request_id) == before
        assert (before["runtime_owner"], before["runtime_epoch"]) == (
            OWNER,
            claimed.runtime_epoch,
        )

    with_session(body)
