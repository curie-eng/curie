"""ADR 0269 retries stay compatible with the previous release (v0.16.2).

Two seams cross a mixed or rolled back install:

1. Upgrade. A v0.16.2 worker posts finish with no ``retry_safety``. The
   finish route accepts it as an unknown safety fact: the real terminal cause
   is recorded and no implementation retry is admitted on unknown safety.
2. Rollback. A v0.16.2 dispatcher claims any waiting request whose waiting
   deadline is ahead and whose ``dispatch_not_before`` has passed; it reads
   neither ``blocked_on`` nor ``execution_kind``. A resource gated retry and a
   publication recovery successor keep their own dispatch time in
   ``guarded_dispatch_not_before`` (migration 0182), so that claim never
   matches them, while the current claim still takes them when due.

Rows are written by the current code through the real app, worker client,
finish route and reconciler, against real Postgres and Valkey, as
``test_factory_retry.py`` does. Time is moved only by SQL on the stored rows.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import ExecutionRequest, FactoryBaseTransition, WorkItem
from curie_api.workitem_dispatch import ClaimedDispatch, claim_due
from fastapi.testclient import TestClient
from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from test_factory_publication_recovery import _captured, _settle
from test_factory_retry import (
    USAGE_ON_A_PROBED_SCOPE,
    WORKER,
    _admit,
    _agent,
    _configure,
    _db,
    _dispatch,
    _execute,
    _finish,
    _request,
    _requests,
    _rows,
    _successor,
    _used,
)
from test_publication_candidates import ARCHIVE_LIMIT

# -- pinned previous release ---------------------------------------------------

# The finish body a v0.16.2 worker posts, pinned from tag v0.16.2,
# apps/worker/src/curie_worker/workitem_dispatch.py, WorkItemDispatchClient.finish:
# runtime_epoch, outcome, cause, detail and ci_fix_round, and nothing else.
V0162_FINISH_KEYS = ("runtime_epoch", "outcome", "cause", "detail", "ci_fix_round")


def _v0162_finish_body(
    runtime_epoch: int, outcome: str, cause: str, detail: str | None
) -> dict[str, Any]:
    body = {
        "runtime_epoch": runtime_epoch,
        "outcome": outcome,
        "cause": cause,
        "detail": detail,
        "ci_fix_round": None,
    }
    assert tuple(body) == V0162_FINISH_KEYS
    return body


async def _v0162_claim_due(
    session: AsyncSession, owner: str, limit: int, lease_s: int
) -> list[ClaimedDispatch]:
    """``claim_due`` verbatim from tag v0.16.2, apps/api/src/curie_api/workitem_dispatch.py."""

    due = (
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.status == "waiting",
            ExecutionRequest.wait_deadline > func.clock_timestamp(),
            ExecutionRequest.dispatch_not_before <= func.clock_timestamp(),
            ExecutionRequest.objective.is_not(None),
            ~exists().where(
                WorkItem.id == ExecutionRequest.work_item_id,
                WorkItem.readmit_request_id.is_not(None),
            ),
            ~exists().where(
                FactoryBaseTransition.work_item_id == ExecutionRequest.work_item_id,
                or_(
                    FactoryBaseTransition.phase.not_in(("applied", "conflict", "refused")),
                    and_(
                        FactoryBaseTransition.phase == "applied",
                        FactoryBaseTransition.outcome_ready_at.is_(None),
                    ),
                    and_(
                        FactoryBaseTransition.phase.in_(("conflict", "refused")),
                        FactoryBaseTransition.request_id == ExecutionRequest.id,
                    ),
                ),
            ),
            ExecutionRequest.published_generation.is_distinct_from(
                ExecutionRequest.dispatch_generation
            ),
            or_(
                ExecutionRequest.dispatch_lease_expires_at.is_(None),
                ExecutionRequest.dispatch_lease_expires_at <= func.clock_timestamp(),
            ),
        )
        .order_by(ExecutionRequest.dispatch_not_before)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = (
        await session.execute(
            update(ExecutionRequest)
            .where(ExecutionRequest.id.in_(due))
            .values(
                dispatch_owner=owner,
                dispatch_epoch=ExecutionRequest.dispatch_epoch + 1,
                dispatch_lease_expires_at=func.clock_timestamp() + timedelta(seconds=lease_s),
                updated_at=func.clock_timestamp(),
            )
            .returning(
                ExecutionRequest.id,
                ExecutionRequest.dispatch_epoch,
                ExecutionRequest.dispatch_generation,
            )
        )
    ).all()
    await session.commit()
    return [
        ClaimedDispatch(
            request_id=row.id, epoch=row.dispatch_epoch, generation=row.dispatch_generation
        )
        for row in rows
    ]


# -- harness -------------------------------------------------------------------


@pytest.fixture
def api(
    clean_db: None, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    _configure(monkeypatch)
    # Publication recovery needs retained candidates (#5429).
    monkeypatch.setenv("CURIE_PUBLICATION_CANDIDATES_ENABLED", "true")
    monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", str(ARCHIVE_LIMIT))
    monkeypatch.delenv("CURIE_PUBLICATION_CANDIDATE_MAX_BYTES", raising=False)
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


def _old_claim() -> set[uuid.UUID]:
    async def body(session: AsyncSession) -> list[ClaimedDispatch]:
        return await _v0162_claim_due(session, "v0162-dispatcher", 50, 30)

    return {claimed.request_id for claimed in _db(body)}


def _new_claim() -> set[uuid.UUID]:
    async def body(session: AsyncSession) -> list[ClaimedDispatch]:
        return await claim_due(session, "current-dispatcher", 50, 30)

    return {claimed.request_id for claimed in _db(body)}


# Shift every stored time of a row by the same interval, as if that much time
# had passed. Triggers are lifted for the write, so the stored values move
# exactly; the guarded-dispatch CHECK still holds because all three move.
_SHIFT = (
    "UPDATE curie.execution_requests SET "
    "wait_deadline = wait_deadline - CAST(:shift AS interval), "
    "dispatch_not_before = dispatch_not_before - CAST(:shift AS interval), "
    "guarded_dispatch_not_before = guarded_dispatch_not_before - CAST(:shift AS interval) "
    "WHERE id = :id AND status = 'waiting'"
)


def _remaining(request_id: uuid.UUID, column: str) -> timedelta:
    (row,) = _rows(
        f"SELECT {column} - clock_timestamp() AS left FROM curie.execution_requests "
        "WHERE id = :id",
        {"id": request_id},
    )
    value = row["left"]
    assert isinstance(value, timedelta)
    return value


def _past_due(request_id: uuid.UUID) -> None:
    """Move time to one second after the current dispatcher's due time."""

    shift = _remaining(request_id, "COALESCE(guarded_dispatch_not_before, dispatch_not_before)")
    _execute(_SHIFT, {"id": request_id, "shift": shift + timedelta(seconds=1)}, guarded=True)


def _near_deadline(request_id: uuid.UUID) -> None:
    """Move time to five seconds before the waiting deadline."""

    shift = _remaining(request_id, "wait_deadline") - timedelta(seconds=5)
    _execute(_SHIFT, {"id": request_id, "shift": shift}, guarded=True)


def _waiting_ahead(request_id: uuid.UUID) -> dict[str, Any]:
    row = _request(request_id)
    assert row["status"] == "waiting"
    assert _remaining(request_id, "wait_deadline") > timedelta(0)
    return row


# -- upgrade: a v0.16.2 worker's finish ----------------------------------------


@pytest.mark.parametrize(
    ("outcome", "cause"),
    [("failed", "model_unreachable"), ("failed", "sandbox_memory_exhausted")],
)
def test_v0162_worker_finish_records_its_real_cause_and_admits_no_retry(
    api: TestClient, auth_headers: dict[str, str], outcome: str, cause: str
) -> None:
    """A finish with no safety fact settles with its own cause, read as unknown safety.

    Both causes admit a successor after a proved clean run; with the fact
    unknown neither may, and the request must not fall through to
    ``expired``/``execution_deadline`` as a refused finish would.
    """

    item = _admit(api, _agent(api, auth_headers), 56101)
    epoch = _dispatch(api, item.request_id)
    response = api.post(
        f"/v1/internal/work-items/requests/{item.request_id}/finish",
        json=_v0162_finish_body(epoch, outcome, cause, "provider said no"),
        headers=WORKER,
    )
    assert response.status_code == 200, response.text
    assert (response.json()["status"], response.json()["terminal_cause"]) == (outcome, cause)
    row = _request(item.request_id)
    assert (row["status"], row["terminal_cause"]) == (outcome, cause)
    assert row["retry_safety"] is None
    assert row["terminal_at"] is not None
    assert _successor(item.request_id) is None
    assert len(_requests(item.work_item_id)) == 1
    assert _used(item.work_item_id) == 0


def test_explicit_null_safety_is_still_refused(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """Only an omitted fact reads as unknown; a current worker never sends null."""

    item = _admit(api, _agent(api, auth_headers), 56103)
    epoch = _dispatch(api, item.request_id)
    before = _request(item.request_id)
    body = {**_v0162_finish_body(epoch, "failed", "model_unreachable", None), "retry_safety": None}
    response = api.post(
        f"/v1/internal/work-items/requests/{item.request_id}/finish", json=body, headers=WORKER
    )
    assert response.status_code == 422, response.text
    assert _request(item.request_id) == before


# -- rollback: a v0.16.2 dispatcher's claim ------------------------------------


def _resource_gated(api: TestClient, agent_id: uuid.UUID) -> uuid.UUID:
    item = _admit(api, agent_id, 56111)
    epoch = _dispatch(api, item.request_id)
    finished = _finish(api, item.request_id, epoch, "sandbox_memory_exhausted")
    assert finished.status_code == 200, finished.text
    successor = _successor(item.request_id)
    assert successor is not None
    assert (successor["blocked_on"], successor["execution_kind"]) == (
        "memory_limit",
        "implementation",
    )
    assert successor["resource_gate"] is not None
    return uuid.UUID(str(successor["id"]))


def _recovery(api: TestClient, agent_id: uuid.UUID) -> uuid.UUID:
    run, _reservation = _captured(api, agent_id, 56112)
    _settle(run.ref, "publication_failed")
    successor = _successor(run.ref.request_id)
    assert successor is not None
    assert (successor["blocked_on"], successor["execution_kind"]) == (
        None,
        "publication_recovery",
    )
    return uuid.UUID(str(successor["id"]))


def _provider_gated(api: TestClient, agent_id: uuid.UUID) -> uuid.UUID:
    item = _admit(api, agent_id, 56113)
    epoch = _dispatch(api, item.request_id)
    finished = _finish(
        api, item.request_id, epoch, "model_usage_limited", condition=USAGE_ON_A_PROBED_SCOPE
    )
    assert finished.status_code == 200, finished.text
    successor = _successor(item.request_id)
    assert successor is not None
    assert successor["blocked_on"] == "provider_usage_reset"
    return uuid.UUID(str(successor["id"]))


def test_v0162_claim_never_matches_new_only_retries_and_the_current_claim_does(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(api, auth_headers)
    gated = _resource_gated(api, agent_id)
    recovery = _recovery(api, agent_id)
    provider = _provider_gated(api, agent_id)
    rows = [gated, recovery, provider]

    # The stored shape an older claim reads: its dispatch time is the deadline.
    for request_id in rows:
        row = _waiting_ahead(request_id)
        assert row["dispatch_not_before"] == row["wait_deadline"], request_id
    for request_id in (gated, recovery):
        guarded = _request(request_id)["guarded_dispatch_not_before"]
        assert guarded is not None
        assert guarded < _request(request_id)["wait_deadline"]
    assert _request(provider)["guarded_dispatch_not_before"] is None

    # Before the dispatch time: neither claim takes anything.
    assert _old_claim() == set()
    assert _new_claim() == set()

    # Just after each one's dispatch time, deadline still ahead. A provider
    # gated retry's dispatch time is its deadline, so it waits near it.
    _past_due(gated)
    _past_due(recovery)
    _near_deadline(provider)
    for request_id in rows:
        _waiting_ahead(request_id)
    assert _old_claim() == set()

    # Late in the wait, deadline still ahead.
    for request_id in rows:
        _near_deadline(request_id)
        _waiting_ahead(request_id)
    assert _old_claim() == set()

    # The current claim takes the two due rows exactly as before; a provider
    # gated retry still waits for its release.
    assert _new_claim() == {gated, recovery}
    assert _request(provider)["dispatch_owner"] is None


def test_an_older_writer_rescheduling_a_guarded_row_keeps_it_unclaimable(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """A rescheduling write that knows only ``dispatch_not_before`` moves the guarded time.

    An older release's deferral and lapsed acquisition sweep write
    ``dispatch_not_before`` alone; the trigger carries that time into the
    guarded column and keeps the waiting deadline where an older claim reads.
    """

    agent_id = _agent(api, auth_headers)
    gated = _resource_gated(api, agent_id)
    recovery = _recovery(api, agent_id)
    for request_id in (gated, recovery):
        _execute(
            "UPDATE curie.execution_requests "
            "SET dispatch_not_before = clock_timestamp() + interval '10 seconds' "
            "WHERE id = :id",
            {"id": request_id},
        )
        row = _waiting_ahead(request_id)
        assert row["dispatch_not_before"] == row["wait_deadline"]
        written = row["guarded_dispatch_not_before"]
        assert written is not None
        assert timedelta(seconds=5) < _remaining(request_id, "guarded_dispatch_not_before")
        assert _remaining(request_id, "guarded_dispatch_not_before") <= timedelta(seconds=10)
        _past_due(request_id)
    assert _old_claim() == set()
    assert _new_claim() == {gated, recovery}
