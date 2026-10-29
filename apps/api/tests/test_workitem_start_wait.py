"""Worker-produced start waits retain their SQL budget and actionable cause."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.workitem_reconciler import WorkItemReconciler
from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_workitem_dispatch import (
    _now,
    with_session,
)
from test_workitem_dispatch import (
    dispatch_client as _dispatch_client,
)

from apps.worker.tests.kernel.conftest import kernel_harness
from apps.worker.tests.kernel.test_work_item_start_wait import (
    ApiBinding,
    SqlClock,
    WorkerHttp,
    WorkspaceServices,
    admitted,
    deliver,
    request_state,
)

dispatch_client = _dispatch_client


@pytest.fixture
def sql_clock(dispatch_client: TestClient) -> Iterator[SqlClock]:
    clock = SqlClock(with_session(_now))
    event.listen(Engine, "before_execute", clock.before_execute, retval=True)
    try:
        yield clock
    finally:
        event.remove(Engine, "before_execute", clock.before_execute)


@pytest.fixture
def kernel_names(valkey: redis.Redis) -> Iterator[dict[str, str]]:
    token = uuid.uuid4().hex
    names = {
        "stream": f"test:curie:runs:{token}",
        "group": f"g-{token}",
        "prefix": f"test:curie:worker:{token}",
        "sandbox_prefix": f"test:curie:sandbox:{token}",
    }
    try:
        yield names
    finally:
        for pattern in (f"{names['prefix']}*", f"{names['sandbox_prefix']}*", names["stream"]):
            keys = list(valkey.scan_iter(match=pattern))
            if keys:
                valkey.delete(*keys)


def summary(
    client: TestClient, auth_headers: dict[str, str], work_item_id: uuid.UUID
) -> dict[str, Any]:
    response = client.get(f"/work-items/{work_item_id}", headers=auth_headers)
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


@pytest.mark.parametrize("stage", ["clone-slot", "clone", "archive", "upload", "total"])
def test_worker_workspace_timeouts_wait_until_the_original_deadline(
    dispatch_client: TestClient,
    auth_headers: dict[str, str],
    sql_clock: SqlClock,
    kernel_names: dict[str, str],
    valkey: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    agent_id, request_id, work_item_id = admitted(dispatch_client, auth_headers)
    services = WorkspaceServices(tmp_path, monkeypatch, fault=stage)
    wire = WorkerHttp(dispatch_client)

    async def exercise() -> None:
        async with kernel_harness(
            kernel_names,
            valkey,
            binding=ApiBinding(agent_id),
            workspace_factory=services.coordinator,
            max_attempts=1,
        ) as harness:
            harness.kernel._work_items = wire.client
            initial, _ = await request_state(request_id)
            deadline = initial.wait_deadline
            base = get_settings().work_item_backoff_base_seconds
            assert deadline > sql_clock.now + timedelta(seconds=7 * (base + 1))
            try:
                for generation in range(1, 7):
                    services.clock.value = 0.0
                    services.clock.archive = False
                    slot = (
                        services.preparer.limiter.acquire("occupied-slot")
                        if stage == "clone-slot" else None
                    )
                    try:
                        if slot is not None:
                            slot.__enter__()
                        await deliver(harness, request_id, generation)
                    finally:
                        if slot is not None:
                            slot.__exit__(None, None, None)
                    row, detail = await request_state(request_id)
                    assert row.status == "waiting"
                    assert row.wait_deadline == deadline
                    assert (
                        row.start_deferrals == row.capacity_deferrals == row.execution_attempts == 0
                    )
                    assert row.runtime_epoch == 0
                    assert row.started_at is row.execution_deadline is row.terminal_cause is None
                    assert row.dispatch_generation == generation + 1
                    assert row.dispatch_not_before == sql_clock.now + timedelta(seconds=base)
                    assert row.last_deferral_reason == "workspace_stage_timeout"
                    assert isinstance(detail, str)
                    assert detail.startswith(f"workspace {stage} failed: exceeded the ")
                    assert len(detail) <= 300
                    sent, returned = wire.deferrals[-1]
                    assert sent["capacity"] is False
                    assert sent["start_failure_detail"] == detail
                    assert returned["terminal_cause"] is None
                    assert datetime.fromisoformat(returned["not_before"]) == row.dispatch_not_before
                    view = summary(dispatch_client, auth_headers, work_item_id)
                    assert view["state"] == "waiting"
                    assert detail in view["actionable_cause"]
                    sql_clock.now += timedelta(seconds=base + 1)
                assert len(wire.deferrals) == 6
                assert harness.runner.opened == []
                sql_clock.now = deadline + timedelta(seconds=1)
                engine = create_async_engine(get_settings().database_url)
                try:
                    reconciler = WorkItemReconciler(
                        async_sessionmaker(engine, expire_on_commit=False),
                        harness.async_redis,
                        get_settings(),
                    )
                    await reconciler._expire_waiting()
                finally:
                    await engine.dispose()
                expired, expired_detail = await request_state(request_id)
                assert expired.status == "expired"
                assert expired.terminal_cause == "capacity_wait_expired"
                assert expired.wait_deadline == deadline
                assert (
                    expired.start_deferrals
                    == expired.capacity_deferrals
                    == expired.execution_attempts
                    == 0
                )
                assert expired_detail == detail
                view = summary(dispatch_client, auth_headers, work_item_id)
                assert view["state"] == "expired"
                assert detail in view["actionable_cause"]
            finally:
                await wire.http.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("fault", ["badref", "auth", "oversize", "generic-timeout-text"])
def test_permanent_preparation_faults_still_fail_on_the_fifth_start(
    dispatch_client: TestClient,
    auth_headers: dict[str, str],
    sql_clock: SqlClock,
    kernel_names: dict[str, str],
    valkey: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    agent_id, request_id, _ = admitted(dispatch_client, auth_headers)
    services = WorkspaceServices(tmp_path, monkeypatch, fault=fault)
    wire = WorkerHttp(dispatch_client)

    async def exercise() -> None:
        async with kernel_harness(
            kernel_names,
            valkey,
            binding=ApiBinding(agent_id),
            workspace_factory=services.coordinator,
            max_attempts=1,
        ) as harness:
            harness.kernel._work_items = wire.client
            initial, _ = await request_state(request_id)
            try:
                for generation in range(1, 6):
                    await deliver(harness, request_id, generation)
                    row, detail = await request_state(request_id)
                    assert row.status == ("waiting" if generation < 5 else "failed")
                    assert row.start_deferrals == generation
                    assert row.capacity_deferrals == row.execution_attempts == 0
                    assert row.wait_deadline == initial.wait_deadline
                    assert row.started_at is row.execution_deadline is None
                    assert row.last_deferral_reason.startswith("not_started:")
                    assert isinstance(detail, str) and detail.startswith("workspace ")
                    sent, returned = wire.deferrals[-1]
                    assert sent["capacity"] is False
                    assert sent["start_failure_detail"] == detail
                    assert returned["terminal_cause"] == (
                        None if generation < 5 else "start_failed"
                    )
                    sql_clock.now = row.dispatch_not_before + timedelta(seconds=1)
                assert row.terminal_cause == "start_failed"
                assert len(wire.deferrals) == 5
                assert harness.runner.opened == []
            finally:
                await wire.http.aclose()

    asyncio.run(exercise())
