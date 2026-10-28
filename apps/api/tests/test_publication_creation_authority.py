"""A new publication proposal needs current factory execution authority (#5140).

``POST /v1/internal/publications`` creates one Approval, one Publication and,
on a first publication, one ThreadPublicationLineage. When the factory
execution request that owns the thread has lost authority (no runtime owner,
an expired runtime lease, or a passed execution deadline, all judged on the
database clock), creation is refused 409 and nothing durable changes. An exact
replay of a proposal created while authority held is still adopted, under the
current workspace and allowlist policy.

Fixtures come in two labelled kinds. "SQL boundary fixture" rows have their
initial timestamps written by SQL relative to ``clock_timestamp()``; they are
not elapsed time. "Actual lifecycle" requests go through the real acquire and
start routes, and the one elapsed-time case really waits past the deadline.
"""

from __future__ import annotations

import asyncio
import base64
import random
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.forges.types import GITHUB, RepositoryRef, TrackerIssueRef
from curie_api.main import create_app
from curie_api.workitem_dispatch import Admission, admit
from curie_api.workitems.lifecycle import WorkItemOutcome
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

REPO = "acme-corp/acme-bot"
WORKER_TOKEN = "publication-creation-authority-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
WIRE_CONVERSATION = "1700000000.000100"
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"
PATCH = (
    b"diff --git a/edit.txt b/edit.txt\n"
    b"--- a/edit.txt\n"
    b"+++ b/edit.txt\n"
    b"@@ -1 +1,2 @@\n"
    b" original line\n"
    b"+publication authority fixture\n"
)
OWNER = "fixture-worker"
PUBLICATIONS = "/v1/internal/publications"
REQUESTS = "/v1/internal/work-items/requests"
SNAPSHOT_TABLES = ("approvals", "publications", "thread_publication_lineages")
# Agents in this module run with the minimum execution deadline the schema
# allows, so the one elapsed-time case waits about a minute.
DEADLINE_SECONDS = 60

EXPIRED = "publication.work_item_authority_expired"
STALE = "publication.work_item_stale_owner"
CANCELLED = "publication.work_item_cancelled"

Snapshot = dict[str, list[dict[str, Any]]]


@dataclass(frozen=True)
class Case:
    """One admitted factory WorkItem with a selected thread workspace."""

    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    conversation_id: str
    address: str
    payload: dict[str, Any]


@pytest.fixture
def stack(_disposable_db: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            yield client
    finally:
        get_settings.cache_clear()


# --- database helpers ----------------------------------------------------------


async def _rows_async(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            result = await connection.execute(text(statement), params or {})
            return [dict(row) for row in result.mappings()] if result.returns_rows else []
    finally:
        await engine.dispose()


def _rows(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return asyncio.run(_rows_async(statement, params))


def _sql(statement: str, params: dict[str, Any] | None = None) -> None:
    _rows(statement, params)


async def _snapshot_async() -> Snapshot:
    return {
        table: await _rows_async(f"SELECT * FROM curie.{table} ORDER BY id")
        for table in SNAPSHOT_TABLES
    }


def _snapshot() -> Snapshot:
    return asyncio.run(_snapshot_async())


def _request_row(request_id: uuid.UUID) -> dict[str, Any]:
    rows = _rows(
        "SELECT status, runtime_owner, runtime_epoch, execution_deadline, "
        "runtime_heartbeat_expires_at, clock_timestamp() AS database_now "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    assert len(rows) == 1, request_id
    return rows[0]


# --- fixture construction ------------------------------------------------------


async def _admit_case(name: str) -> Case:
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    try:
        async with AsyncSession(engine) as session:
            address = "C" + uuid.uuid4().hex[:9].upper()
            agent_id = uuid.uuid4()
            await session.execute(
                text(
                    "INSERT INTO curie.agents "
                    "(id, name, repo_full_name, execution_deadline_seconds) "
                    "VALUES (:id, :name, :repo, :deadline)"
                ),
                {
                    "id": agent_id,
                    "name": f"authority-{agent_id.hex[:12]}",
                    "repo": REPO,
                    "deadline": DEADLINE_SECONDS,
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
            facts = Admission(
                agent_id=agent_id,
                kind="slack",
                address=address,
                reply_conversation_id=WIRE_CONVERSATION,
                issue=TrackerIssueRef(
                    GITHUB, "github.com", "101", str(random.randint(10_000, 2_000_000_000))
                ),
                repository=RepositoryRef(GITHUB, "github.com", "101", REPO),
                code_host_installation_id=202,
                objective="Publish the admitted work item",
                requester="U0REQUEST1",
                request_id=uuid.uuid4(),
            )
            admitted = await admit(session, facts)
            await session.commit()
            assert isinstance(admitted, WorkItemOutcome), admitted
            conversation_id = admitted.work_item.conversation_id
            version_id, deployment_id = uuid.uuid4(), uuid.uuid4()
            await session.execute(
                text(
                    "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
                    "VALUES (:id, :agent, 'v1', 'fixture')"
                ),
                {"id": version_id, "agent": agent_id},
            )
            await session.execute(
                text(
                    "INSERT INTO curie.deployments "
                    "(id, agent_id, version_id, environment, status) VALUES "
                    "(:id, :agent, :version, CAST('dev' AS curie.environment), 'active')"
                ),
                {"id": deployment_id, "agent": agent_id, "version": version_id},
            )
            await session.execute(
                text(
                    "INSERT INTO curie.thread_workspaces "
                    "(id, agent_id, selected_by_deployment_id, conversation_id, "
                    "repo_full_name, selected_by) "
                    "VALUES (:id, :agent, :deployment, :conversation, :repo, 'fixture')"
                ),
                {
                    "id": uuid.uuid4(),
                    "agent": agent_id,
                    "deployment": deployment_id,
                    "conversation": conversation_id,
                    "repo": REPO,
                },
            )
            await session.commit()
    finally:
        await engine.dispose()
    payload: dict[str, Any] = {
        "deployment_id": str(deployment_id),
        "conversation_id": conversation_id,
        "reply_conversation_id": WIRE_CONVERSATION,
        "repo_full_name": REPO,
        "author": "U0REQUEST1",
        "summary": "Publication authority fixture",
        "reply_kind": "slack",
        "reply_channel": address,
        "dedupe_key": f"authority-{name}-{facts.request_id.hex}",
        "base_sha": BASE_SHA,
        "work_item_request_id": str(facts.request_id),
        "work_item_runtime_epoch": 1,
        "patch_b64": base64.b64encode(PATCH).decode(),
        "changed_paths": ["edit.txt"],
    }
    return Case(
        agent_id=agent_id,
        deployment_id=deployment_id,
        work_item_id=admitted.work_item.id,
        request_id=facts.request_id,
        conversation_id=conversation_id,
        address=address,
        payload=payload,
    )


def _case(name: str) -> Case:
    """An admitted WorkItem whose request is still ``waiting``."""

    return asyncio.run(_admit_case(name))


async def _wake_json(request_id: uuid.UUID) -> dict[str, Any]:
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    try:
        async with AsyncSession(engine) as session:
            wake = await factory_wake(session, request_id)
            await session.commit()
    finally:
        await engine.dispose()
    dumped: dict[str, Any] = wake.model_dump(mode="json")
    return dumped


def _started_case(client: TestClient, name: str) -> Case:
    """Actual lifecycle: the request runs through the real acquire and start routes."""

    case = _case(name)
    prefix = f"{REQUESTS}/{case.request_id}"
    acquired = client.post(
        f"{prefix}/acquire",
        json={
            "owner": OWNER,
            "generation": 1,
            "wake": asyncio.run(_wake_json(case.request_id)),
            "context": None,
        },
        headers=WORKER_HEADERS,
    )
    assert acquired.status_code == 200, acquired.text
    started = client.post(
        f"{prefix}/start",
        json={
            "owner": OWNER,
            "generation": 1,
            "claim_name": f"fixture-{case.request_id.hex}",
            "sandbox_name": f"fixture-sandbox-{case.request_id.hex}",
            "context": acquired.json()["context"],
        },
        headers=WORKER_HEADERS,
    )
    assert started.status_code == 200, started.text
    assert started.json()["runtime_epoch"] == 1, started.text
    row = _request_row(case.request_id)
    assert row["status"] == "running"
    assert row["runtime_owner"] == OWNER
    assert row["database_now"] < row["runtime_heartbeat_expires_at"]
    assert row["database_now"] < row["execution_deadline"]
    return case


def _sql_running(case: Case, kind: str) -> None:
    """SQL boundary fixture: initial timestamps set by SQL, not elapsed time.

    ``running`` is live, ``lease`` has an expired runtime lease only,
    ``deadline`` has a passed execution deadline only, ``both`` has both, and
    ``no_owner`` is live on the clock with no runtime owner.
    """

    elapsed = 120 if kind in ("deadline", "both") else 0
    lease = -1 if kind in ("lease", "both") else 300
    _sql(
        "UPDATE curie.execution_requests SET status = 'running', "
        "started_at = clock_timestamp() - make_interval(secs => :elapsed), "
        "execution_deadline = clock_timestamp() - make_interval(secs => :elapsed) "
        "+ make_interval(secs => :deadline), "
        "execution_attempts = 1, runtime_owner = :owner, runtime_epoch = 1, "
        "runtime_heartbeat_expires_at = clock_timestamp() + make_interval(secs => :lease) "
        "WHERE id = :id",
        {
            "id": case.request_id,
            "elapsed": elapsed,
            "deadline": DEADLINE_SECONDS,
            "lease": lease,
            "owner": OWNER,
        },
    )
    if kind == "no_owner":
        _sql(
            "UPDATE curie.execution_requests SET runtime_owner = NULL WHERE id = :id",
            {"id": case.request_id},
        )


def _sql_ended(case: Case) -> None:
    """SQL boundary fixture: the request ran and reached a terminal status."""

    _sql_running(case, "running")
    _sql(
        "UPDATE curie.execution_requests SET status = 'failed', "
        "terminal_cause = 'fixture_failed', terminal_at = clock_timestamp() WHERE id = :id",
        {"id": case.request_id},
    )


def _requestless(payload: dict[str, Any]) -> dict[str, Any]:
    stripped = dict(payload)
    stripped.pop("work_item_request_id")
    stripped.pop("work_item_runtime_epoch")
    return stripped


# --- assertions ----------------------------------------------------------------


def _assert_refused(client: TestClient, payload: dict[str, Any], code: str, label: str) -> None:
    before = _snapshot()
    response = client.post(PUBLICATIONS, json=payload, headers=WORKER_HEADERS)
    after = _snapshot()
    assert response.status_code == 409, (label, response.status_code, response.text)
    detail = response.json()["detail"]
    assert isinstance(detail, dict), (label, response.text)
    assert detail["code"] == code, (label, response.text)
    assert after == before, f"{label}: a refused proposal changed durable rows"


def _assert_created(client: TestClient, payload: dict[str, Any], label: str) -> dict[str, Any]:
    before = _snapshot()
    response = client.post(PUBLICATIONS, json=payload, headers=WORKER_HEADERS)
    after = _snapshot()
    assert response.status_code == 201, (label, response.status_code, response.text)
    for table in SNAPSHOT_TABLES:
        assert len(after[table]) == len(before[table]) + 1, (label, table)
    body: dict[str, Any] = response.json()
    assert body["status"] == "pending", (label, body)
    assert any(row["id"] == uuid.UUID(body["id"]) for row in after["publications"]), label
    return body


# --- tests ---------------------------------------------------------------------


def test_expired_new_proposal(stack: TestClient) -> None:
    client = stack

    # SLOW (about 61 s): actual elapsed-time case. The request starts through
    # the real acquire/start routes now, then no heartbeat is ever sent. The
    # other cases below run while its clock runs, and the remaining wait
    # happens before it is checked, so at least 61 real seconds pass between
    # start and the publication attempt.
    real = _started_case(client, "real-elapsed")
    started_at = time.monotonic()

    # Positive control, actual lifecycle: a live running request publishes.
    live = _started_case(client, "live")
    _assert_created(client, live.payload, "live")

    for kind in ("no_owner", "lease", "deadline", "both"):
        case = _case(kind)
        # SQL boundary fixture: initial timestamps set by SQL, not elapsed time.
        _sql_running(case, kind)
        row = _request_row(case.request_id)
        assert row["status"] == "running", kind
        if kind == "no_owner":
            assert row["runtime_owner"] is None
            assert row["database_now"] < row["runtime_heartbeat_expires_at"]
            assert row["database_now"] < row["execution_deadline"]
        if kind == "lease":
            assert row["runtime_heartbeat_expires_at"] <= row["database_now"]
            assert row["database_now"] < row["execution_deadline"]
        if kind == "deadline":
            assert row["database_now"] < row["runtime_heartbeat_expires_at"]
            assert row["execution_deadline"] <= row["database_now"]
        if kind == "both":
            assert row["runtime_heartbeat_expires_at"] <= row["database_now"]
            assert row["execution_deadline"] <= row["database_now"]
        _assert_refused(client, case.payload, EXPIRED, f"sql-{kind}")

    # SLOW: really wait out the 60 s execution deadline with no heartbeat.
    remaining = DEADLINE_SECONDS + 1 - (time.monotonic() - started_at)
    if remaining > 0:
        time.sleep(remaining)
    assert time.monotonic() - started_at >= DEADLINE_SECONDS + 1
    row = _request_row(real.request_id)
    assert row["status"] == "running", row
    assert row["runtime_owner"] == OWNER, row
    assert row["execution_deadline"] <= row["database_now"], row
    assert row["runtime_heartbeat_expires_at"] <= row["database_now"], row

    prefix = f"{REQUESTS}/{real.request_id}"
    heartbeat = client.post(
        f"{prefix}/heartbeat", json={"runtime_epoch": 1}, headers=WORKER_HEADERS
    )
    assert heartbeat.status_code == 409, heartbeat.text
    finish = client.post(
        f"{prefix}/finish",
        json={"runtime_epoch": 1, "outcome": "failed", "cause": "fixture_failed"},
        headers=WORKER_HEADERS,
    )
    assert finish.status_code == 409, finish.text
    _assert_refused(client, real.payload, EXPIRED, "real-elapsed")


def test_fenced_new_proposal(stack: TestClient) -> None:
    client = stack

    # Explicit identity while the WorkItem's active request is still waiting.
    waiting = _case("explicit-waiting")
    assert _request_row(waiting.request_id)["status"] == "waiting"
    _assert_refused(client, waiting.payload, STALE, "explicit-waiting")

    # SQL boundary fixture: the named request has ended, so none is active.
    ended = _case("explicit-ended")
    _sql_ended(ended)
    _assert_refused(client, ended.payload, STALE, "explicit-ended")

    # SQL boundary fixture: a live running request, named wrongly.
    wrong_request = _case("wrong-request")
    _sql_running(wrong_request, "running")
    payload = dict(wrong_request.payload, work_item_request_id=str(uuid.uuid4()))
    _assert_refused(client, payload, STALE, "wrong-request")

    wrong_epoch = _case("wrong-epoch")
    _sql_running(wrong_epoch, "running")
    payload = dict(wrong_epoch.payload, work_item_runtime_epoch=99)
    _assert_refused(client, payload, STALE, "wrong-epoch")

    # SQL boundary fixture: a live running request on a cancelled WorkItem.
    cancelled = _case("cancelled")
    _sql_running(cancelled, "running")
    _sql(
        "UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :id",
        {"id": cancelled.work_item_id},
    )
    _assert_refused(client, cancelled.payload, CANCELLED, "cancelled")

    # SQL boundary fixture: the active request is pending termination.
    terminating = _case("pending-termination")
    _sql_running(terminating, "running")
    _sql(
        "UPDATE curie.execution_requests SET status = 'cancellation_requested', "
        "terminal_cause = 'issue_cancelled' WHERE id = :id",
        {"id": terminating.request_id},
    )
    _assert_refused(client, terminating.payload, CANCELLED, "pending-termination")


def test_creation_and_replay_contracts(stack: TestClient) -> None:
    client = stack

    # Requestless (a resumed approval turn) while the request is still waiting.
    waiting = _case("requestless-waiting")
    _assert_created(client, _requestless(waiting.payload), "requestless-waiting")

    # SQL boundary fixture: requestless after the request ended.
    ended = _case("requestless-ended")
    _sql_ended(ended)
    _assert_created(client, _requestless(ended.payload), "requestless-ended")

    # Interactive: the thread keeps its workspace but has no WorkItem.
    interactive = _case("interactive")
    _sql("DELETE FROM curie.work_items WHERE id = :id", {"id": interactive.work_item_id})
    _assert_created(client, _requestless(interactive.payload), "interactive")

    # Explicit identity on a thread with no WorkItem keeps today's behavior.
    orphan = _case("explicit-no-work-item")
    _sql("DELETE FROM curie.work_items WHERE id = :id", {"id": orphan.work_item_id})
    _assert_created(client, orphan.payload, "explicit-no-work-item")

    # Actual lifecycle: requestless on a live running request.
    live = _started_case(client, "requestless-live")
    _assert_created(client, _requestless(live.payload), "requestless-live")

    # Actual lifecycle: the approval hold parks the lease at the deadline.
    held = _started_case(client, "approval-hold")
    hold = client.post(
        f"{REQUESTS}/{held.request_id}/hold-approval",
        json={"runtime_epoch": 1},
        headers=WORKER_HEADERS,
    )
    assert hold.status_code == 200, hold.text
    row = _request_row(held.request_id)
    assert row["runtime_heartbeat_expires_at"] == row["execution_deadline"], row
    assert row["database_now"] < row["execution_deadline"], row
    _assert_created(client, held.payload, "approval-hold")

    # Actual lifecycle start, then the CI hold's parked state emulated by SQL:
    # hold_for_ci_fix sets runtime_heartbeat_expires_at = execution_deadline.
    ci_held = _started_case(client, "ci-hold")
    _sql(
        "UPDATE curie.execution_requests "
        "SET runtime_heartbeat_expires_at = execution_deadline WHERE id = :id",
        {"id": ci_held.request_id},
    )
    row = _request_row(ci_held.request_id)
    assert row["runtime_heartbeat_expires_at"] == row["execution_deadline"], row
    assert row["database_now"] < row["execution_deadline"], row
    _assert_created(client, ci_held.payload, "ci-hold")

    # Actual lifecycle start; SQL boundary fixture expires the lease (not
    # elapsed time) before the deadline; a real heartbeat renews it.
    renewed = _started_case(client, "heartbeat-renewal")
    _sql(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": renewed.request_id},
    )
    _assert_refused(client, renewed.payload, EXPIRED, "heartbeat-renewal-before")
    heartbeat = client.post(
        f"{REQUESTS}/{renewed.request_id}/heartbeat",
        json={"runtime_epoch": 1},
        headers=WORKER_HEADERS,
    )
    assert heartbeat.status_code == 200, heartbeat.text
    row = _request_row(renewed.request_id)
    assert row["database_now"] < row["runtime_heartbeat_expires_at"], row
    _assert_created(client, renewed.payload, "heartbeat-renewal")

    # SQL boundary fixture: requestless on a running request that lost authority.
    expired = _case("requestless-expired")
    _sql_running(expired, "both")
    _assert_refused(client, _requestless(expired.payload), EXPIRED, "requestless-expired")


def test_expired_replay(stack: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    client = stack

    # Actual lifecycle: created while the request is live.
    case = _started_case(client, "replay")
    created = _assert_created(client, case.payload, "replay-first")

    # SQL boundary fixture: the runtime lease is set past (not elapsed time).
    # Only the lease: a started request's execution deadline is write once.
    _sql(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": case.request_id},
    )
    row = _request_row(case.request_id)
    assert row["status"] == "running", row
    assert row["runtime_heartbeat_expires_at"] <= row["database_now"], row

    def replay() -> tuple[int, Any, bool]:
        before = _snapshot()
        response = client.post(PUBLICATIONS, json=case.payload, headers=WORKER_HEADERS)
        return response.status_code, response.json(), _snapshot() == before

    status, body, unchanged = replay()
    assert status == 200, body
    assert body["id"] == created["id"], body
    assert unchanged, "an exact replay changed durable rows"

    # The replay still answers to the current allowlist.
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["other-org/*"]')
    get_settings.cache_clear()
    status, body, unchanged = replay()
    assert status == 409, body
    assert body["detail"] == "thread workspace repository is no longer allowed", body
    assert unchanged, "a refused replay changed durable rows"
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()

    status, body, unchanged = replay()
    assert status == 200, body
    assert body["id"] == created["id"], body
    assert unchanged

    # And to the thread's current workspace selection.
    _sql(
        "DELETE FROM curie.thread_workspaces "
        "WHERE agent_id = :agent AND conversation_id = :conversation",
        {"agent": case.agent_id, "conversation": case.conversation_id},
    )
    status, body, unchanged = replay()
    assert status == 409, body
    assert body["detail"] == "conversation has no selected repository workspace", body
    assert unchanged, "a refused replay changed durable rows"


def test_expiry_during_binding_lock(stack: TestClient) -> None:
    client = stack

    # A first publication for the thread, so creation takes the agent_channels
    # row lock (binding_for_route for_update) on its way to a new lineage.
    case = _case("binding-lock")
    # SQL boundary fixture: live running request whose lease ends ~2 s from
    # now on the database clock; the deadline is later.
    _sql_running(case, "running")
    _sql(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() + interval '2 seconds' WHERE id = :id",
        {"id": case.request_id},
    )
    assert not _rows(
        "SELECT id FROM curie.thread_publication_lineages "
        "WHERE agent_id = :agent AND conversation_id = :conversation",
        {"agent": case.agent_id, "conversation": case.conversation_id},
    )

    async def scenario() -> tuple[Snapshot, Snapshot, list[dict[str, Any]], Any]:
        before = await _snapshot_async()
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        outcome: dict[str, Any] = {}

        def post() -> None:
            try:
                outcome["response"] = client.post(
                    PUBLICATIONS, json=case.payload, headers=WORKER_HEADERS
                )
            except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
                outcome["error"] = exc

        worker = threading.Thread(target=post, daemon=True)
        try:
            async with engine.connect() as blocker:
                transaction = await blocker.begin()
                try:
                    await blocker.execute(
                        text(
                            "SELECT id FROM curie.agent_channels "
                            "WHERE address = :address FOR UPDATE"
                        ),
                        {"address": case.address},
                    )
                    blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
                    worker.start()
                    observed: list[dict[str, Any]] = []
                    waited_from = time.monotonic()
                    while time.monotonic() - waited_from < 10 and worker.is_alive():
                        observed = await _rows_async(
                            "SELECT pid, wait_event_type, wait_event FROM pg_stat_activity "
                            "WHERE :blocker = ANY(pg_blocking_pids(pid))",
                            {"blocker": blocker_pid},
                        )
                        if observed:
                            break
                        await asyncio.sleep(0.05)
                    assert observed, ("creation never waited on the binding lock", outcome)
                    assert worker.is_alive(), outcome
                    # Hold the lock until the database clock is past the lease.
                    waited_from = time.monotonic()
                    while True:
                        current = await _rows_async(
                            "SELECT clock_timestamp() > runtime_heartbeat_expires_at "
                            "AS expired FROM curie.execution_requests WHERE id = :id",
                            {"id": case.request_id},
                        )
                        if current[0]["expired"]:
                            break
                        assert time.monotonic() - waited_from < 10, "lease never expired"
                        await asyncio.sleep(0.05)
                finally:
                    await transaction.rollback()
            await asyncio.to_thread(worker.join, 30)
        finally:
            await engine.dispose()
        assert not worker.is_alive(), "publication request did not return"
        assert "error" not in outcome, outcome
        after = await _snapshot_async()
        return before, after, observed, outcome["response"]

    before, after, observed, response = asyncio.run(scenario())
    assert observed
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == EXPIRED, response.text
    assert after == before, "creation that outlived its lease left durable rows"
