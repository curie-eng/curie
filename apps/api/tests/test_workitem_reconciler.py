"""WorkItemReconciler publishes wakes from SQL onto a real Valkey stream."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import STREAM_PAYLOAD_FIELD, WORKER_GROUP_DEFAULT
from curie_api.config import get_settings
from curie_api.workitem_dispatch import admit, fence_published
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
WIRE_CONVERSATION = "1700000000.000100"
OBJECTIVE = "Reconcile the admitted work item"
REQUESTER = "U0REQUEST1"
@pytest.fixture
def allowlisted(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("CURIE_WORK_ITEM_DISPATCH_LEASE_SECONDS", "1")
    monkeypatch.setenv("CURIE_WORK_ITEM_TERMINATE_RETRY_SECONDS", "30")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class _SessionTracker:
    current: AsyncSession | None = None


class _TrackingSessionmaker:
    def __init__(
        self, inner: async_sessionmaker[AsyncSession], tracker: _SessionTracker
    ) -> None:
        self.inner = inner
        self.tracker = tracker

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return _TrackingContext(self.inner(*args, **kwargs), self.tracker)

    def begin(self, *args: Any, **kwargs: Any) -> Any:
        return _TrackingContext(self.inner.begin(*args, **kwargs), self.tracker)


class _TrackingContext:
    def __init__(self, context: Any, tracker: _SessionTracker) -> None:
        self.context = context
        self.tracker = tracker

    async def __aenter__(self) -> AsyncSession:
        session = await self.context.__aenter__()
        self.tracker.current = session
        return session

    async def __aexit__(self, *exc: object) -> Any:
        self.tracker.current = None
        return await self.context.__aexit__(*exc)


class _XaddSpy:
    def __init__(self, inner: aioredis.Redis, tracker: _SessionTracker) -> None:
        self.inner = inner
        self.tracker = tracker
        self.in_transaction: list[bool] = []

    async def xadd(self, *args: Any, **kwargs: Any) -> Any:
        current = self.tracker.current
        self.in_transaction.append(
            False if current is None else bool(current.in_transaction())
        )
        return await self.inner.xadd(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


def _group() -> str:
    settings = get_settings()
    return str(getattr(settings, "runs_consumer_group", WORKER_GROUP_DEFAULT))


def _payloads(valkey: redis.Redis, stream: str) -> list[dict[str, Any]]:
    entries = valkey.xrange(stream)
    payloads = []
    for _entry_id, fields in entries:
        payloads.append(json.loads(fields[STREAM_PAYLOAD_FIELD]))
    return payloads


async def _now(session: AsyncSession) -> datetime:
    value = await session.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(value, datetime)
    return value


async def _agent_with_channel(session: AsyncSession) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.agents (id, name, repo_full_name) "
            "VALUES (:id, :name, :repo)"
        ),
        {
            "id": agent_id,
            "name": f"acme-bot-{agent_id.hex[:8]}",
            "repo": REPO,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
            "VALUES (:id, :agent_id, 'slack', :address)"
        ),
        {"id": uuid.uuid4(), "agent_id": agent_id, "address": ADDRESS},
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


async def _request_row(session: AsyncSession, request_id: uuid.UUID) -> Any:
    return (
        await session.execute(
            text(
                "SELECT r.status, r.terminal_cause, r.execution_attempts, "
                "r.capacity_deferrals, r.dispatch_generation, "
                "r.published_generation, r.wait_deadline, r.started_at, "
                "r.terminate_published_at, r.objective, r.reply_kind, "
                "r.reply_address, r.reply_conversation_id "
                "FROM curie.execution_requests r WHERE r.id = :id"
            ),
            {"id": request_id},
        )
    ).mappings().one()


def _run(
    steps: Callable[
        [async_sessionmaker[AsyncSession], WorkItemReconciler, aioredis.Redis],
        Awaitable[Any],
    ],
    stream: str,
    *,
    spy_xadd: bool = False,
) -> Any:
    async def main() -> Any:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        tracker = _SessionTracker()
        tracked = _TrackingSessionmaker(maker, tracker)
        client = aioredis.Redis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None
        )
        valkey: aioredis.Redis = _XaddSpy(client, tracker) if spy_xadd else client
        settings = get_settings()
        assert settings.runs_stream == stream
        reconciler = WorkItemReconciler(tracked, valkey, settings)
        try:
            return await steps(maker, reconciler, valkey)
        finally:
            await client.aclose()
            await engine.dispose()

    return asyncio.run(main())


def test_xadd_before_group_create_is_invisible_to_new_readers(
    valkey: redis.Redis, runs_stream: str
) -> None:
    valkey.xadd(runs_stream, {STREAM_PAYLOAD_FIELD: "{}"})
    valkey.xgroup_create(runs_stream, _group(), id="$", mkstream=True)
    assert valkey.xreadgroup(_group(), "reader", {runs_stream: ">"}, count=10) == []


def test_run_once_creates_the_group_then_publishes_a_readable_execute_wake(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            assert admitted.request is not None
            request_id = facts.request_id
        await reconciler.run_once()
        return request_id

    request_id = _run(steps, runs_stream, spy_xadd=True)
    group = _group()
    delivered = valkey.xreadgroup(group, "reader", {runs_stream: ">"}, count=10)
    assert delivered
    _stream, entries = delivered[0]
    assert len(entries) == 1
    payload = json.loads(entries[0][1][STREAM_PAYLOAD_FIELD])
    assert payload["event_id"] == f"work-item-{request_id}-execute-1"
    assert payload["conversation_id"] == WIRE_CONVERSATION
    assert payload["text"] == OBJECTIVE
    assert payload["author"] == REQUESTER


def test_xadd_does_not_run_inside_a_sql_transaction(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> list[bool]:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            await admit(session, facts)
        await reconciler.run_once()
        assert isinstance(client, _XaddSpy)
        return client.in_transaction

    flags = _run(steps, runs_stream, spy_xadd=True)
    assert flags
    assert flags == [False] * len(flags)


def test_crash_between_xadd_and_fence_republishes_the_same_event_id(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"n": 0}

    async def fail_once(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("injected fence failure")
        return await fence_published(*args, **kwargs)

    monkeypatch.setattr(
        "curie_api.workitem_dispatch.fence_published", fail_once
    )
    monkeypatch.setattr(
        "curie_api.workitem_reconciler.fence_published", fail_once
    )

    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            await admit(session, facts)
            request_id = facts.request_id
        with pytest.raises(RuntimeError, match="injected fence failure"):
            await reconciler.run_once()
        await asyncio.sleep(1.2)
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation == row.dispatch_generation == 1
        return request_id

    request_id = _run(steps, runs_stream)
    payloads = _payloads(valkey, runs_stream)
    event_ids = [payload["event_id"] for payload in payloads]
    assert event_ids == [
        f"work-item-{request_id}-execute-1",
        f"work-item-{request_id}-execute-1",
    ]


def test_lost_xadd_leaves_the_row_due_for_the_next_pass(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            await admit(session, facts)
            request_id = facts.request_id

        async def fail_xadd(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("injected xadd failure")

        original_xadd = client.xadd
        client.xadd = fail_xadd  # type: ignore[method-assign]
        try:
            with pytest.raises(RuntimeError, match="injected xadd failure"):
                await reconciler.run_once()
        finally:
            client.xadd = original_xadd  # type: ignore[method-assign]
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation is None
            assert row.dispatch_generation == 1
        await asyncio.sleep(1.2)
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.published_generation == 1
        return request_id

    request_id = _run(steps, runs_stream)
    payloads = _payloads(valkey, runs_stream)
    assert [payload["event_id"] for payload in payloads] == [
        f"work-item-{request_id}-execute-1"
    ]


def test_expire_waiting_records_capacity_wait_expired(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS", "3")
    get_settings.cache_clear()

    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            assert admitted.request is not None
            deadline = admitted.request.wait_deadline
            request_id = facts.request_id
        async with maker() as session:
            while True:
                if await _now(session) >= deadline:
                    break
                await asyncio.sleep(0.05)
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.status == "expired"
            assert row.terminal_cause == "capacity_wait_expired"
            assert row.execution_attempts == 0
            assert row.started_at is None
        return request_id

    _run(steps, runs_stream)
    assert _payloads(valkey, runs_stream) == []


def test_deadline_and_owner_lost_cancellation_are_requested(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> None:
        async with maker() as session:
            agent_id = await _agent_with_channel(session)
            deadline_facts = _facts(agent_id)
            owner_facts = _facts(agent_id, github_issue_number=2574)
            await admit(session, deadline_facts)
            await admit(session, owner_facts)
            await session.execute(
                text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'running', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '1801 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": deadline_facts.request_id},
            )
            await session.execute(
                text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'running', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "runtime_owner = 'worker-a', "
                    "runtime_epoch = 1, "
                    "runtime_heartbeat_expires_at = s.ts + interval '59 seconds', "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '60 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": owner_facts.request_id},
            )
            await session.commit()
            deadline_id = deadline_facts.request_id
            owner_id = owner_facts.request_id
        await reconciler.run_once()
        async with maker() as session:
            deadline_row = await _request_row(session, deadline_id)
            owner_row = await _request_row(session, owner_id)
            assert (
                deadline_row.status,
                deadline_row.terminal_cause,
            ) == ("cancellation_requested", "execution_deadline")
            assert (
                owner_row.status,
                owner_row.terminal_cause,
            ) == ("cancellation_requested", "owner_lost")

    _run(steps, runs_stream)


def test_terminate_wake_uses_the_sql_snapshot_without_an_agent_channel(
    clean_db: None,
    allowlisted: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession],
        reconciler: WorkItemReconciler,
        _client: aioredis.Redis,
    ) -> uuid.UUID:
        async with maker() as session:
            facts = _facts(await _agent_with_channel(session))
            admitted = await admit(session, facts)
            await session.execute(
                text(
                    "UPDATE curie.execution_requests e SET "
                    "status = 'cancellation_requested', "
                    "started_at = s.ts, "
                    "execution_deadline = s.ts + interval '1800 seconds', "
                    "execution_attempts = 1, "
                    "terminal_cause = 'owner_lost', "
                    "runtime_owner = NULL, "
                    "runtime_heartbeat_expires_at = s.ts + interval '59 seconds', "
                    "version = version + 1 "
                    "FROM (SELECT clock_timestamp() - interval '60 seconds' AS ts) s "
                    "WHERE e.id = :id"
                ),
                {"id": facts.request_id},
            )
            await session.execute(
                text("DELETE FROM curie.agent_channels WHERE agent_id = :id"),
                {"id": admitted.work_item.agent_id},
            )
            await session.commit()
            request_id = facts.request_id
        await reconciler.run_once()
        await reconciler.run_once()
        async with maker() as session:
            row = await _request_row(session, request_id)
            assert row.terminate_published_at is not None
            assert row.reply_kind == "slack"
            assert row.reply_address == ADDRESS
            assert row.reply_conversation_id == WIRE_CONVERSATION
        return request_id

    request_id = _run(steps, runs_stream)
    payloads = _payloads(valkey, runs_stream)
    terminate = [
        payload
        for payload in payloads
        if payload["event_id"] == f"work-item-{request_id}-terminate"
    ]
    assert len(terminate) == 1
    wake = terminate[0]
    assert wake["text"] == "terminate"
    assert wake["conversation_id"] == WIRE_CONVERSATION
    handle = wake["reply_handle"]
    assert handle["kind"] == "slack"
    assert handle["channel"] == ADDRESS
    assert handle.get("placeholder") is None
    assert handle.get("endpoint") is None
    assert handle.get("adapter") is None


def test_suite_create_app_does_not_start_the_work_item_reconciler(client: Any) -> None:
    assert client.app.state.work_item_reconciler_task is None

