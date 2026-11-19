"""A started personal request's own terminal write owes its runtime teardown (#5840).

ADR 0268 decision 4 gives a personal owner one active process: the worker
counts any SandboxClaim on another thread, suspended or not, as the owner's
slot. A request the API settles itself, such as a run parked for CI that the
CI gate then completes, used to leave that claim behind for a day. Now
``lifecycle.terminalize_execution`` marks a started personal request
``teardown_unconfirmed_at`` in the same UPDATE that settles it ``completed`` or
``failed``, and releases a lease parked at the execution deadline, so terminate
wakes go out at once and any worker can claim the teardown; recording it
clears the flag and keeps the outcome.

What does not change: an ``expired`` request already ends with a worker
termination observation, a request parked for CI or an approval stays
``running`` with its claim and slot, and a service request is never marked.

Postgres and Valkey are real, the termination claim and record go over the
worker HTTP routes, and terminate wakes come from
``workitem_dispatch.claim_terminate_publishes``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Literal
from uuid import UUID

import httpx
import personal_execution_support as support
import pytest
from curie_api import workitem_dispatch
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.workitem_dispatch import HeartbeatResult
from curie_api.workitems import lifecycle
from curie_api.workitems.lifecycle import PublicationSettlement, WorkItemOutcome
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from test_workitems import _grant_opened_pull_request

WORKER_TOKEN = "personal-slot-release-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
OBSERVATION = support.TERMINATION_OBSERVATION


@pytest.fixture
def world(clean_db: None) -> Iterator[support.World]:
    support.cleanup_owned()
    yield support.seed_world()
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


@pytest.fixture
def api(
    world: support.World, runs_stream: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    support.configure_api(monkeypatch, worker_token=WORKER_TOKEN)
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


# --- request states --------------------------------------------------------------------


def _started(
    api: TestClient, world: support.World, issue: int, *, service: bool = False
) -> tuple[support.Admitted, int]:
    agent_id = support.insert_agent(world.tenant_id)
    conversation = f"{issue}.000100"
    admitted = (
        support.admit_service(
            api, WORKER_HEADERS, agent_id, issue_number=issue, conversation=conversation
        )
        if service
        else support.admit_personal(
            api, WORKER_HEADERS, world, agent_id, issue_number=issue, conversation=conversation
        )
    )
    return admitted, support.start_request(admitted.request_id)


def _open_pull_request(admitted: support.Admitted, *, pr: int) -> None:
    """Link a succeeded publication, the opened pull request a completion needs."""

    async def body(session: AsyncSession) -> None:
        work_item = await lifecycle.lock_work_item(session, admitted.work_item_id)
        request = await lifecycle.lock_request(
            session, work_item_id=admitted.work_item_id, request_id=admitted.request_id
        )
        assert work_item is not None and request is not None
        await _grant_opened_pull_request(
            session, await lifecycle.outcome(session, work_item, request), pr=pr
        )

    support.with_session(body)


def _park_for_ci(admitted: support.Admitted, *, pr: int) -> PublicationSettlement:
    """Open the pull request, let the CI gate claim it, and park the run for CI."""

    _open_pull_request(admitted, pr=pr)
    settlement = support.with_session(
        lambda session: lifecycle.claim_publication_settlement(session, exclude=frozenset())
    )
    assert settlement is not None and settlement.request_id == admitted.request_id
    assert isinstance(settlement.subject, lifecycle.PublicationCISubject)
    held = support.with_session(
        lambda session: lifecycle.hold_for_ci_fix(
            session,
            work_item_id=settlement.work_item_id,
            request_id=settlement.request_id,
            expected_request_version=settlement.request_version,
            expected_subject=settlement.subject,
        )
    )
    assert held is True
    assert _request(admitted.request_id)["parked"] is True
    return settlement


def _settle_ci(
    settlement: PublicationSettlement, status: Literal["completed", "failed"], cause: str
) -> None:
    """The CI gate's verdict on a parked run, as ``factory_ci`` settles it."""

    settled = support.with_session(
        lambda session: lifecycle.settle_ci_verdict(
            session,
            work_item_id=settlement.work_item_id,
            request_id=settlement.request_id,
            expected_work_item_version=settlement.work_item_version,
            expected_request_version=settlement.request_version,
            expected_subject=settlement.subject,
            status=status,
            cause=cause,
            detail=None,
        )
    )
    assert isinstance(settled, WorkItemOutcome), settled
    assert settled.request is not None and settled.request.status == status, settled


def _finish(api: TestClient, request_id: UUID, epoch: int, outcome: str) -> None:
    """The running turn's own finish, as the worker's completion sends it."""

    response = api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/finish",
        json={
            "runtime_epoch": epoch,
            "outcome": outcome,
            "cause": "completed" if outcome == "completed" else "model_error",
            "retry_safety": "uncertain",
        },
        headers=WORKER_HEADERS,
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == outcome, response.text


def _request(request_id: UUID) -> dict[str, Any]:
    [row] = support.rows(
        "SELECT status, terminal_cause, terminal_at, termination_observation, "
        "teardown_unconfirmed_at, runtime_owner, runtime_epoch, version, "
        "runtime_heartbeat_expires_at, "
        "runtime_heartbeat_expires_at <= clock_timestamp() AS lease_lapsed, "
        "runtime_heartbeat_expires_at = execution_deadline AS parked "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row


def _wakes() -> list[UUID]:
    published = support.with_session(
        lambda session: workitem_dispatch.claim_terminate_publishes(
            session, retry_seconds=0, limit=50
        )
    )
    return sorted(wake.request_id for wake in published)


# --- worker routes ---------------------------------------------------------------------


def _claim(api: TestClient, request_id: UUID, owner: str) -> httpx.Response:
    return api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/termination/claim",
        json={"owner": owner},
        headers=WORKER_HEADERS,
    )


def _claimed(api: TestClient, request_id: UUID, owner: str) -> dict[str, int]:
    response = _claim(api, request_id, owner)
    assert response.status_code == 200, response.text
    return {key: int(value) for key, value in response.json().items()}


def _record(api: TestClient, request_id: UUID, claim: dict[str, int]) -> httpx.Response:
    return api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/termination",
        json={
            "runtime_epoch": claim["runtime_epoch"],
            "expected_request_version": claim["request_version"],
            "observation": OBSERVATION,
        },
        headers=WORKER_HEADERS,
    )


def _conflict(response: httpx.Response, code: str) -> None:
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": {"code": code}}


def _tear_down(
    api: TestClient, request_id: UUID, owner: str, *, status: str, cause: str, epoch: int
) -> None:
    """Claim and record the owed teardown; the outcome survives and nothing more is owed."""

    before = _request(request_id)
    claim = _claimed(api, request_id, owner)
    assert claim["runtime_epoch"] == epoch
    recorded = _record(api, request_id, claim)
    assert recorded.status_code == 200, recorded.text
    after = _request(request_id)
    assert (after["status"], after["terminal_cause"]) == (status, cause)
    for column in ("terminal_at", "runtime_epoch"):
        assert after[column] == before[column], column
    assert (after["termination_observation"], after["teardown_unconfirmed_at"]) == (
        OBSERVATION,
        None,
    )
    assert _wakes() == []
    _conflict(_claim(api, request_id, support.OTHER_RUNTIME_OWNER), "not_running")


# --- completed and failed personal requests owe their teardown -------------------------


def test_completed_personal_request_owes_its_teardown(
    api: TestClient, world: support.World
) -> None:
    """The reported shape: a run parked for CI that the CI gate completes."""

    admitted, epoch = _started(api, world, 58401)
    settlement = _park_for_ci(admitted, pr=58401)

    _settle_ci(settlement, "completed", "completed")

    row = _request(admitted.request_id)
    assert (row["status"], row["terminal_cause"], row["termination_observation"]) == (
        "completed",
        "completed",
        None,
    )
    assert row["teardown_unconfirmed_at"] is not None
    assert (row["parked"], row["lease_lapsed"]) == (False, True)
    assert _wakes() == [admitted.request_id]
    _tear_down(
        api,
        admitted.request_id,
        support.OTHER_RUNTIME_OWNER,
        status="completed",
        cause="completed",
        epoch=epoch,
    )


def test_worker_completed_personal_request_owes_its_teardown_to_its_live_owner(
    api: TestClient, world: support.World
) -> None:
    """A worker finish keeps its short live lease, so only the finisher claims at once."""

    admitted, epoch = _started(api, world, 58402)
    _open_pull_request(admitted, pr=58402)
    lease = _request(admitted.request_id)["runtime_heartbeat_expires_at"]

    _finish(api, admitted.request_id, epoch, "completed")

    row = _request(admitted.request_id)
    assert (row["status"], row["terminal_cause"]) == ("completed", "completed")
    assert row["teardown_unconfirmed_at"] is not None
    assert (row["runtime_heartbeat_expires_at"], row["lease_lapsed"]) == (lease, False)
    assert _wakes() == [admitted.request_id]
    _conflict(_claim(api, admitted.request_id, support.OTHER_RUNTIME_OWNER), "duplicate")
    _tear_down(
        api,
        admitted.request_id,
        support.RUNTIME_OWNER,
        status="completed",
        cause="completed",
        epoch=epoch,
    )


def test_failed_personal_request_owes_its_teardown(api: TestClient, world: support.World) -> None:
    admitted, epoch = _started(api, world, 58403)

    support.finish_failed(admitted.request_id, epoch, cause="model_error")

    row = _request(admitted.request_id)
    assert (row["status"], row["terminal_cause"], row["termination_observation"]) == (
        "failed",
        "model_error",
        None,
    )
    assert row["teardown_unconfirmed_at"] is not None
    assert admitted.request_id in _wakes()
    _tear_down(
        api,
        admitted.request_id,
        support.RUNTIME_OWNER,
        status="failed",
        cause="model_error",
        epoch=epoch,
    )


def test_ci_failed_verdict_releases_the_parked_personal_slot(
    api: TestClient, world: support.World
) -> None:
    """The walkthrough: parked at the execution deadline, then the CI gate fails it."""

    admitted, epoch = _started(api, world, 58404)
    settlement = _park_for_ci(admitted, pr=58404)
    parked = _request(admitted.request_id)
    assert (parked["status"], parked["lease_lapsed"]) == ("running", False)

    _settle_ci(settlement, "failed", "ci_failed")

    row = _request(admitted.request_id)
    assert (row["status"], row["terminal_cause"]) == ("failed", "ci_failed")
    assert row["teardown_unconfirmed_at"] is not None
    # The lease no longer sits at the execution deadline: it lapsed when the
    # request settled, so another worker need not wait out the deadline.
    assert row["parked"] is False
    assert row["runtime_heartbeat_expires_at"] < parked["runtime_heartbeat_expires_at"]
    assert row["lease_lapsed"] is True
    assert _wakes() == [admitted.request_id]
    _tear_down(
        api,
        admitted.request_id,
        support.OTHER_RUNTIME_OWNER,
        status="failed",
        cause="ci_failed",
        epoch=epoch,
    )


# --- an expired request already carries its observation --------------------------------


def _elapse_deadline(request_id: UUID) -> None:
    """Move the run's start and deadline into the past instead of waiting them out.

    Execution deadlines are write once, so the trigger is bypassed for this
    one test write the way other suites seed historical deadlines.
    """

    async def body(session: AsyncSession) -> None:
        await session.execute(text("SET LOCAL session_replication_role = replica"))
        await session.execute(
            text(
                "UPDATE curie.execution_requests "
                "SET started_at = clock_timestamp() - interval '61 seconds', "
                "execution_deadline = clock_timestamp() - interval '1 second' WHERE id = :id"
            ),
            {"id": request_id},
        )
        await session.commit()

    support.with_session(body)


def test_expired_personal_request_carries_its_observation_and_owes_no_teardown(
    api: TestClient, world: support.World
) -> None:
    admitted, epoch = _started(api, world, 58405)
    _elapse_deadline(admitted.request_id)
    [versions] = support.rows(
        "SELECT r.version AS request_version, w.version AS work_item_version "
        "FROM curie.execution_requests r JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE r.id = :id",
        {"id": admitted.request_id},
    )
    stopping = support.with_session(
        lambda session: lifecycle.request_execution_deadline_cancellation(
            session,
            work_item_id=admitted.work_item_id,
            request_id=admitted.request_id,
            expected_work_item_version=versions["work_item_version"],
            expected_request_version=versions["request_version"],
        )
    )
    assert isinstance(stopping, WorkItemOutcome), stopping
    assert stopping.request is not None
    assert (stopping.request.status, stopping.request.terminal_cause) == (
        "cancellation_requested",
        "execution_deadline",
    )

    claim = _claimed(api, admitted.request_id, support.RUNTIME_OWNER)
    recorded = _record(api, admitted.request_id, claim)
    assert recorded.status_code == 200, recorded.text

    row = _request(admitted.request_id)
    assert (row["status"], row["terminal_cause"]) == ("expired", "execution_deadline")
    assert row["termination_observation"] == OBSERVATION
    assert row["termination_observation"].strip()
    assert row["teardown_unconfirmed_at"] is None
    assert _wakes() == []
    _conflict(_claim(api, admitted.request_id, support.OTHER_RUNTIME_OWNER), "not_running")
    assert epoch == row["runtime_epoch"]


# --- a parked request keeps its claim and slot -----------------------------------------


def _assert_still_parked(request_id: UUID) -> None:
    row = _request(request_id)
    assert (row["status"], row["teardown_unconfirmed_at"]) == ("running", None)
    assert (row["parked"], row["lease_lapsed"]) == (True, False)
    assert _wakes() == []


def test_parked_for_ci_personal_request_keeps_its_slot(
    api: TestClient, world: support.World
) -> None:
    admitted, _ = _started(api, world, 58406)
    _park_for_ci(admitted, pr=58406)

    _assert_still_parked(admitted.request_id)
    _conflict(_claim(api, admitted.request_id, support.OTHER_RUNTIME_OWNER), "not_running")


def test_parked_for_approval_personal_request_keeps_its_slot(
    api: TestClient, world: support.World
) -> None:
    admitted, epoch = _started(api, world, 58407)
    held = support.with_session(
        lambda session: workitem_dispatch.hold_for_approval(
            session, admitted.request_id, runtime_epoch=epoch
        )
    )
    assert isinstance(held, HeartbeatResult), held

    _assert_still_parked(admitted.request_id)
    _conflict(_claim(api, admitted.request_id, support.OTHER_RUNTIME_OWNER), "not_running")


# --- a service request is unchanged ----------------------------------------------------


@pytest.mark.parametrize("ending", ["worker-fail", "pr-opened"])
def test_service_request_owes_no_teardown(
    api: TestClient, world: support.World, ending: str
) -> None:
    admitted, epoch = _started(api, world, 58408, service=True)
    if ending == "pr-opened":
        _open_pull_request(admitted, pr=58408)
    before = _request(admitted.request_id)

    if ending == "pr-opened":
        _finish(api, admitted.request_id, epoch, "completed")
    else:
        support.finish_failed(admitted.request_id, epoch, cause="model_error")

    row = _request(admitted.request_id)
    assert row["status"] == ("completed" if ending == "pr-opened" else "failed")
    assert row["teardown_unconfirmed_at"] is None
    for column in ("runtime_owner", "runtime_heartbeat_expires_at", "runtime_epoch"):
        assert row[column] == before[column], column
    assert _wakes() == []
    _conflict(_claim(api, admitted.request_id, support.OTHER_RUNTIME_OWNER), "not_running")
