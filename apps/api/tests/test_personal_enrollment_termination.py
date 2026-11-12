"""Enrollment fences and epoch-bound teardown confirmation (#5377 AC3, ADR 0268 section 3).

Replacing or disconnecting a personal enrollment fences every request of its
owner in the publishing transaction: ``lifecycle.fence_enrollment_generation``
turns a running request into ``cancellation_requested`` with cause
``enrollment_fenced`` and marks every started terminal request that carries no
termination observation ``teardown_unconfirmed_at``. Terminate wakes then run
until a worker claims the teardown and records its observation. A terminal
request keeps its status, cause, ``terminal_at`` and runtime epoch; its claim
is an exclusive lease that advances only the request version, and a record must
present the claim's epoch and version. Revalidation stays refused until the
teardown is confirmed, then binds the new generation once, and a fresh request
copies the locked WorkItem's owner tuple.

The fence is called here directly, in a transaction that locks the owner's
WorkItems, requests and enrollment header the way the publishing transaction
does; the owner routes that publish a generation are tested with the custody
protocol. Postgres is real, the worker routes are driven over HTTP, and lease
expiry is written with SQL rather than waited for.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from typing import Any
from uuid import UUID

import httpx
import personal_execution_support as support
import pytest
from curie_api import workitem_dispatch
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import ExecutionRequest, PersonalClaudeEnrollment, WorkItem
from curie_api.workitem_dispatch import AcquireGrant, DeferResult
from curie_api.workitems import lifecycle
from curie_api.workitems.lifecycle import EnrollmentFence, WorkItemOutcome
from fastapi.testclient import TestClient
from forge_fakes import factory_wake, no_external_calls
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

WORKER_TOKEN = "personal-enrollment-termination-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
OBSERVATION = support.TERMINATION_OBSERVATION
RECOVERY_BASE = "b" * 40
RECOVERY_ARCHIVE_LIMIT = 268435456
REVISION_OBJECTIVE = "Address the follow up comment on the issue"


@pytest.fixture
def world(clean_db: None) -> Iterator[support.World]:
    support.cleanup_owned()
    yield support.seed_world()
    assert support.cleanup_owned() == 0, "an owned tenant, principal or enrollment survived"


@pytest.fixture
def api(world: support.World, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    support.configure_api(monkeypatch, worker_token=WORKER_TOKEN)
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


# --- the publishing transaction's fence ------------------------------------------------


def _fence(world: support.World, *, active: bool = True) -> tuple[int, int, EnrollmentFence]:
    """Advance the owner's enrollment and fence its requests in one transaction.

    Locks in the publishing transaction's order: the owner's personal WorkItems
    by id, their requests by WorkItem and sequence, then the header. ``active``
    False is a disconnect. Returns the new generation, version and the fence.
    """

    async def body(session: AsyncSession) -> tuple[int, int, EnrollmentFence]:
        await session.execute(text("SET LOCAL lock_timeout = '5s'"))
        items = list(
            await session.scalars(
                select(WorkItem.id)
                .where(
                    WorkItem.tenant_id == world.tenant_id,
                    WorkItem.owner_principal_id == world.owner_id,
                    WorkItem.model_auth_mode == "personal",
                )
                .order_by(WorkItem.id)
                .with_for_update()
            )
        )
        await session.execute(
            select(ExecutionRequest.id)
            .where(ExecutionRequest.work_item_id.in_(items))
            .order_by(ExecutionRequest.work_item_id, ExecutionRequest.sequence)
            .with_for_update()
        )
        advanced = (
            await session.execute(
                update(PersonalClaudeEnrollment)
                .where(
                    PersonalClaudeEnrollment.tenant_id == world.tenant_id,
                    PersonalClaudeEnrollment.principal_id == world.owner_id,
                )
                .values(
                    generation=PersonalClaudeEnrollment.generation + 1,
                    version=PersonalClaudeEnrollment.version + 1,
                    active=active,
                )
                .returning(PersonalClaudeEnrollment.generation, PersonalClaudeEnrollment.version)
            )
        ).one()
        fence = await lifecycle.fence_enrollment_generation(
            session, tenant_id=world.tenant_id, owner_principal_id=world.owner_id
        )
        await session.commit()
        return int(advanced.generation), int(advanced.version), fence

    return support.with_session(body)


# --- request states --------------------------------------------------------------------


def _admit(api: TestClient, world: support.World, agent_id: UUID, issue: int) -> support.Admitted:
    return support.admit_personal(
        api, WORKER_HEADERS, world, agent_id, issue_number=issue, conversation=f"{issue}.000100"
    )


def _acquire(request_id: UUID, *, owner: str = support.RUNTIME_OWNER) -> None:
    """Take the acquire lease of a waiting request without starting it."""

    async def body(session: AsyncSession) -> None:
        generation = await session.scalar(
            text("SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id"),
            {"id": request_id},
        )
        assert isinstance(generation, int), request_id
        async with no_external_calls() as http_client:
            acquired = await workitem_dispatch.acquire(
                session,
                request_id,
                owner=owner,
                generation=generation,
                wake=await factory_wake(session, request_id),
                context=None,
                client=http_client,
            )
        assert isinstance(acquired, AcquireGrant), acquired

    support.with_session(body)


def _finished(
    api: TestClient, world: support.World, agent_id: UUID, issue: int, *, cause: str
) -> tuple[support.Admitted, int]:
    """A started request the worker finished ``failed`` with ``cause``."""

    admitted = _admit(api, world, agent_id, issue)
    epoch = support.start_request(admitted.request_id)
    support.finish_failed(admitted.request_id, epoch, cause=cause)
    return admitted, epoch


def _completed(request_id: UUID) -> None:
    """Seed a normal completion; the opened pull request it needs is not under test."""

    support.rows(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": request_id},
    )


def _revise(agent_id: UUID, issue: int) -> UUID:
    """A mention on the issue: a new request on the same WorkItem."""

    revision = support.facts(
        agent_id,
        issue_number=issue,
        conversation=f"{issue}.000100",
        requester=support.OTHER_REQUESTER,
        objective=REVISION_OBJECTIVE,
    )
    admitted = support.with_session(
        lambda session: workitem_dispatch.admit_revision(session, revision)
    )
    assert isinstance(admitted, WorkItemOutcome), admitted
    assert admitted.request is not None
    return admitted.request.id


def _request(request_id: UUID) -> dict[str, Any]:
    [row] = support.rows(
        "SELECT status, terminal_cause, terminal_at, termination_observation, "
        "teardown_unconfirmed_at, runtime_owner, runtime_epoch, version, "
        "runtime_heartbeat_expires_at, runtime_heartbeat_expires_at <= clock_timestamp() "
        "AS lease_lapsed, enrollment_generation, cancellation_requested_at "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row


def _expire_lease(request_id: UUID) -> None:
    support.rows(
        "UPDATE curie.execution_requests "
        "SET runtime_heartbeat_expires_at = clock_timestamp() - interval '1 second' "
        "WHERE id = :id",
        {"id": request_id},
    )


def _wakes() -> list[UUID]:
    published = support.with_session(
        lambda session: workitem_dispatch.claim_terminate_publishes(
            session, retry_seconds=0, limit=50
        )
    )
    return sorted(wake.request_id for wake in published)


# --- worker routes ---------------------------------------------------------------------


def _claim(api: TestClient, request_id: UUID, owner: str = support.RUNTIME_OWNER) -> httpx.Response:
    return api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/termination/claim",
        json={"owner": owner},
        headers=WORKER_HEADERS,
    )


def _claimed(
    api: TestClient, request_id: UUID, owner: str = support.RUNTIME_OWNER
) -> dict[str, int]:
    response = _claim(api, request_id, owner)
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {"runtime_epoch", "request_version"}, body
    return {key: int(value) for key, value in body.items()}


def _record(
    api: TestClient,
    request_id: UUID,
    claim: dict[str, int],
    *,
    observation: str = OBSERVATION,
    **overrides: int,
) -> httpx.Response:
    return api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/termination",
        json={
            "runtime_epoch": overrides.get("runtime_epoch", claim["runtime_epoch"]),
            "expected_request_version": overrides.get(
                "expected_request_version", claim["request_version"]
            ),
            "observation": observation,
        },
        headers=WORKER_HEADERS,
    )


def _conflict(response: httpx.Response, code: str) -> None:
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": {"code": code}}


def _finish(api: TestClient, request_id: UUID, epoch: int) -> httpx.Response:
    """The running turn's own finish, as the worker's completion sends it."""

    return api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/finish",
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "early_stop"},
        headers=WORKER_HEADERS,
    )


def _heartbeat(api: TestClient, request_id: UUID, epoch: int) -> httpx.Response:
    return api.post(
        f"{support.INTERNAL_PREFIX}/requests/{request_id}/heartbeat",
        json={"runtime_epoch": epoch},
        headers=WORKER_HEADERS,
    )


def _context(api: TestClient, world: support.World, agent_id: UUID, item: UUID) -> Any:
    response = api.get(
        f"{support.INTERNAL_PREFIX}/{item}/personal-context",
        params={
            "tenant_id": str(world.tenant_id),
            "agent_id": str(agent_id),
            "owner_principal_id": str(world.owner_id),
        },
        headers=WORKER_HEADERS,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _revalidate(
    api: TestClient, world: support.World, agent_id: UUID, item: UUID
) -> httpx.Response:
    context = _context(api, world, agent_id, item)
    return api.post(
        f"{support.INTERNAL_PREFIX}/{item}/personal-context/revalidate",
        json={
            "tenant_id": str(world.tenant_id),
            "agent_id": str(agent_id),
            "owner_principal_id": str(world.owner_id),
            "request_id": context["request"]["id"],
            "expected_work_item_version": context["work_item_version"],
            "expected_request_version": context["request"]["version"],
            "expected_enrollment_version": context["enrollment"]["version"],
        },
        headers=WORKER_HEADERS,
    )


def _seed_transcript(agent_id: UUID, work_item_id: UUID) -> str:
    [item] = support.rows(
        "SELECT conversation_id FROM curie.work_items WHERE id = :id", {"id": work_item_id}
    )
    thread = str(item["conversation_id"])
    support.rows(
        "INSERT INTO curie.thread_transcripts (id, agent_id, thread_key, value) "
        'VALUES (:id, :agent, :thread, CAST(\'[{"role": "user"}]\' AS jsonb))',
        {"id": uuid.uuid4(), "agent": agent_id, "thread": thread},
    )
    return thread


def _transcripts(agent_id: UUID, thread: str) -> int:
    [row] = support.rows(
        "SELECT count(*) AS n FROM curie.thread_transcripts "
        "WHERE agent_id = :agent AND thread_key = :thread",
        {"agent": agent_id, "thread": thread},
    )
    return int(row["n"])


# --- the fence -------------------------------------------------------------------------


def test_fence_stops_running_requests_without_cancelling_the_work_item(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    live = _admit(api, world, agent_id, 61001)
    held = _admit(api, world, agent_id, 61002)
    live_epoch = support.start_request(live.request_id)
    held_epoch = support.start_request(held.request_id)
    parked = support.with_session(
        lambda session: workitem_dispatch.hold_for_approval(
            session, held.request_id, runtime_epoch=held_epoch
        )
    )
    assert not hasattr(parked, "code"), parked
    beat = _heartbeat(api, live.request_id, live_epoch)
    assert beat.json() == {
        "status": "running",
        "terminal_cause": None,
        "work_item_cancelled": False,
    }
    before = {rid: _request(rid) for rid in (live.request_id, held.request_id)}
    assert before[held.request_id]["lease_lapsed"] is False

    generation, _, fence = _fence(world)

    assert generation == world.generation + 1
    assert sorted(fence.stopping) == sorted((live.request_id, held.request_id))
    assert fence.pending_teardown == ()
    for request_id, epoch in ((live.request_id, live_epoch), (held.request_id, held_epoch)):
        after = _request(request_id)
        assert (after["status"], after["terminal_cause"]) == (
            "cancellation_requested",
            "enrollment_fenced",
        )
        assert after["cancellation_requested_at"] is not None
        assert after["version"] == before[request_id]["version"] + 1
        assert after["runtime_epoch"] == epoch
        # The request snapshot keeps the generation it ran under.
        assert after["enrollment_generation"] == world.generation
    # The live run's next heartbeat tells its worker to stop.
    response = _heartbeat(api, live.request_id, live_epoch)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "cancellation_requested",
        "terminal_cause": "enrollment_fenced",
        "work_item_cancelled": False,
    }
    # The live lease still fences its owner; the parked approval hold is released
    # so any worker can claim the termination.
    assert (
        _request(live.request_id)["runtime_heartbeat_expires_at"]
        == before[live.request_id]["runtime_heartbeat_expires_at"]
    )
    assert _request(held.request_id)["lease_lapsed"] is True
    assert _claimed(api, held.request_id, support.OTHER_RUNTIME_OWNER)["runtime_epoch"] == (
        held_epoch + 1
    )
    _conflict(_claim(api, live.request_id, support.OTHER_RUNTIME_OWNER), "duplicate")
    assert support.rows(
        "SELECT count(*) AS n FROM curie.work_items WHERE id IN (:a, :b) "
        "AND cancelled_at IS NULL AND enrollment_generation = :generation",
        {"a": live.work_item_id, "b": held.work_item_id, "generation": world.generation},
    ) == [{"n": 2}]
    # The fenced runs end failed through the worker's termination chain. The
    # live owner's own claim renews its lease and keeps its epoch.
    claim = _claimed(api, live.request_id)
    assert claim["runtime_epoch"] == live_epoch
    recorded = _record(api, live.request_id, claim)
    assert recorded.status_code == 200, recorded.text
    assert (
        recorded.json()["status"],
        recorded.json()["terminal_cause"],
        recorded.json()["termination_observation"],
    ) == ("failed", "enrollment_fenced", OBSERVATION)


def test_fence_marks_started_terminal_requests_and_leaves_the_rest_untouched(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    failed, _ = _finished(api, world, agent_id, 61101, cause="deadline_halted")
    completed = _admit(api, world, agent_id, 61102)
    support.start_request(completed.request_id)
    _completed(completed.request_id)
    # An owner_lost failure already carries its observation.
    lost = _admit(api, world, agent_id, 61103)
    lost_epoch = support.start_request(lost.request_id)
    declared = support.with_session(
        lambda session: workitem_dispatch.declare_owner_lost(
            session, lost.request_id, owner=support.RUNTIME_OWNER, runtime_epoch=lost_epoch
        )
    )
    assert not hasattr(declared, "code"), declared
    stopping = _request(lost.request_id)
    assert (stopping["status"], stopping["terminal_cause"]) == (
        "cancellation_requested",
        "owner_lost",
    )
    # Waiting and queued requests on another WorkItem.
    waiting = _admit(api, world, agent_id, 61104)
    queued = _revise(agent_id, 61104)
    # A never started request: its wait expired.
    never = _admit(api, world, agent_id, 61105)
    support.rows(
        "UPDATE curie.execution_requests SET status = 'expired', terminal_at = clock_timestamp(), "
        "terminal_cause = 'capacity_wait_expired', version = version + 1 WHERE id = :id",
        {"id": never.request_id},
    )
    # Another owner of the same tenant, enrolled, and a service run.
    support.rows(
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, 1, true, 1)",
        {"id": uuid.uuid4(), "tenant": world.tenant_id, "principal": world.other_principal_id},
    )
    other_admission = support.facts(agent_id, issue_number=61106, conversation="61106.000100")
    other = support._admit(
        api,
        WORKER_HEADERS,
        other_admission,
        extra={
            "model_auth_mode": "personal",
            "owner": {
                "tenant_id": str(world.tenant_id),
                "principal_id": str(world.other_principal_id),
                "enrollment_generation": 1,
            },
        },
    )
    other_epoch = support.start_request(other.request_id)
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=61108, conversation="61108.000100"
    )
    service_epoch = support.start_request(service.request_id)
    untouched = [
        lost.request_id,
        waiting.request_id,
        queued,
        never.request_id,
        other.request_id,
        service.request_id,
    ]
    before = {rid: _request(rid) for rid in untouched}

    _, _, fence = _fence(world)

    assert fence.stopping == ()
    assert sorted(fence.pending_teardown) == sorted((failed.request_id, completed.request_id))
    for request_id, status, cause in (
        (failed.request_id, "failed", "deadline_halted"),
        (completed.request_id, "completed", "completed"),
    ):
        row = _request(request_id)
        assert (row["status"], row["terminal_cause"], row["termination_observation"]) == (
            status,
            cause,
            None,
        )
        assert row["teardown_unconfirmed_at"] is not None
    for request_id in untouched:
        assert _request(request_id) == before[request_id], request_id
    # Neither the other owner's run nor the service run is asked to stop.
    for request_id, epoch in ((other.request_id, other_epoch), (service.request_id, service_epoch)):
        assert _heartbeat(api, request_id, epoch).json()["status"] == "running"
    # A second fence finds nothing new to mark.
    _, _, again = _fence(world)
    assert again == EnrollmentFence(stopping=(), pending_teardown=())


def test_terminate_wakes_cover_fenced_and_unconfirmed_terminal_rows_but_not_a_live_sibling(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    running = _admit(api, world, agent_id, 61201)
    support.start_request(running.request_id)
    lone, _ = _finished(api, world, agent_id, 61202, cause="model_error")
    acquiring, _ = _finished(api, world, agent_id, 61203, cause="model_error")
    acquiring_sibling = _revise(agent_id, 61203)
    started, _ = _finished(api, world, agent_id, 61204, cause="model_error")
    started_sibling = _revise(agent_id, 61204)
    support.start_request(started_sibling)
    assert _wakes() == []

    _fence(world)
    # A sibling taking its acquire lease after the fence owns the thread's sandbox.
    _acquire(acquiring_sibling)

    # The fenced running requests stop; the stopping sibling still blocks the
    # teardown of the run before it.
    assert _request(started_sibling)["terminal_cause"] == "enrollment_fenced"
    assert _wakes() == sorted((running.request_id, lone.request_id, started_sibling))
    for request_id in (acquiring.request_id, started.request_id):
        _expire_lease(request_id)
        _conflict(_claim(api, request_id, support.OTHER_RUNTIME_OWNER), "not_running")
        assert _request(request_id)["teardown_unconfirmed_at"] is not None
    # Once the acquire lease lapses the sibling no longer owns the sandbox.
    support.rows(
        "UPDATE curie.execution_requests "
        "SET acquire_expires_at = clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": acquiring_sibling},
    )
    assert acquiring.request_id in _wakes()
    _claimed(api, acquiring.request_id, support.OTHER_RUNTIME_OWNER)


def test_waiting_sibling_without_acquire_lease_does_not_block_terminal_teardown(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first, epoch = _finished(api, world, agent_id, 61301, cause="model_error")
    sibling = _revise(agent_id, 61301)
    assert _request(sibling)["status"] == "waiting"
    old = world.generation
    generation, enrollment_version, _ = _fence(world)
    assert _request(sibling)["enrollment_generation"] == old

    # A waiting sibling nobody is acquiring owns no sandbox.
    assert _wakes() == [first.request_id]
    claim = _claimed(api, first.request_id)
    assert claim["runtime_epoch"] == epoch
    recorded = _record(api, first.request_id, claim)
    assert recorded.status_code == 200, recorded.text
    assert _request(first.request_id)["teardown_unconfirmed_at"] is None

    bound = _revalidate(api, world, agent_id, first.work_item_id)
    assert bound.status_code == 200, bound.text
    assert (
        bound.json()["request_id"],
        bound.json()["previous_generation"],
        bound.json()["enrollment_generation"],
        bound.json()["enrollment_version"],
    ) == (str(sibling), old, generation, enrollment_version)
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(generation),
        requests={first.request_id: world.personal(old), sibling: world.personal(generation)},
    )


# --- the teardown claim and record -----------------------------------------------------


def test_terminal_teardown_claim_is_an_exclusive_lease_that_keeps_the_epoch(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first, epoch = _finished(api, world, agent_id, 61401, cause="model_error")
    # Nothing is owed before the fence.
    _conflict(_claim(api, first.request_id), "not_running")
    _fence(world)
    marked = _request(first.request_id)

    # The finishing worker's lease is still live, so only it may claim.
    _conflict(_claim(api, first.request_id, support.OTHER_RUNTIME_OWNER), "duplicate")
    assert _request(first.request_id) == marked
    first_claim = _claimed(api, first.request_id)
    assert first_claim == {
        "runtime_epoch": epoch,
        "request_version": marked["version"] + 1,
    }
    leased = _request(first.request_id)
    assert (leased["runtime_owner"], leased["runtime_epoch"], leased["lease_lapsed"]) == (
        support.RUNTIME_OWNER,
        epoch,
        False,
    )
    # A second owner is refused while that lease is live.
    _conflict(_claim(api, first.request_id, support.OTHER_RUNTIME_OWNER), "duplicate")
    assert _request(first.request_id) == leased

    # After it lapses another worker takes over; the epoch still does not move.
    _expire_lease(first.request_id)
    second_claim = _claimed(api, first.request_id, support.OTHER_RUNTIME_OWNER)
    assert second_claim == {
        "runtime_epoch": epoch,
        "request_version": first_claim["request_version"] + 1,
    }
    # The superseded claimant's record presents a stale version and changes nothing.
    taken_over = _request(first.request_id)
    _conflict(_record(api, first.request_id, first_claim), "stale_version")
    assert _request(first.request_id) == taken_over
    recorded = _record(api, first.request_id, second_claim)
    assert recorded.status_code == 200, recorded.text
    final = _request(first.request_id)
    assert (final["termination_observation"], final["teardown_unconfirmed_at"]) == (
        OBSERVATION,
        None,
    )
    # Confirmed: no further claim.
    _conflict(_claim(api, first.request_id, support.OTHER_RUNTIME_OWNER), "not_running")


def test_reclaim_refuses_after_a_sibling_acquires_between_claims(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first, epoch = _finished(api, world, agent_id, 61451, cause="model_error")
    sibling = _revise(agent_id, 61451)
    _fence(world)
    first_claim = _claimed(api, first.request_id)
    claimed = _request(first.request_id)

    # Between the two claims the sibling becomes the sandbox owner, so the
    # same owner's re-claim must not interrupt a sandbox the sibling now holds.
    _acquire(sibling)
    _conflict(_claim(api, first.request_id), "not_running")
    refused = _request(first.request_id)
    assert refused["version"] == claimed["version"] == first_claim["request_version"]
    assert refused["runtime_heartbeat_expires_at"] == claimed["runtime_heartbeat_expires_at"]
    assert refused["teardown_unconfirmed_at"] is not None

    # Liveness: once the sibling no longer holds the sandbox the same claim
    # succeeds. A fenced sibling can never start, because start refuses its
    # stale generation, so the worker releases its acquire by deferring it.
    generation = support.request_column(sibling, "dispatch_generation")
    deferred = support.with_session(
        lambda session: workitem_dispatch.defer(
            session,
            sibling,
            owner=support.RUNTIME_OWNER,
            generation=generation,
            reason="not_started:enrollment_generation_mismatch",
            capacity=False,
            start_failure_detail=None,
        )
    )
    assert isinstance(deferred, DeferResult), deferred
    reclaim = _claimed(api, first.request_id)
    assert reclaim["runtime_epoch"] == epoch
    assert reclaim["request_version"] == first_claim["request_version"] + 1


@pytest.mark.parametrize("ending", ["completed", "failed"])
def test_confirmation_preserves_the_prior_terminal_outcome_and_epoch(
    api: TestClient, world: support.World, ending: str
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = _admit(api, world, agent_id, 61501)
    epoch = support.start_request(first.request_id)
    if ending == "completed":
        _completed(first.request_id)
    else:
        support.finish_failed(first.request_id, epoch, cause="model_error")
    _fence(world)
    before = _request(first.request_id)
    assert before["teardown_unconfirmed_at"] is not None

    claim = _claimed(api, first.request_id)
    assert claim["runtime_epoch"] == epoch
    recorded = _record(api, first.request_id, claim)
    assert recorded.status_code == 200, recorded.text
    assert recorded.json()["termination_observation"] == OBSERVATION

    after = _request(first.request_id)
    for column in ("status", "terminal_cause", "terminal_at", "runtime_epoch"):
        assert after[column] == before[column], column
    assert (after["termination_observation"], after["teardown_unconfirmed_at"]) == (
        OBSERVATION,
        None,
    )
    assert after["runtime_owner"] is None
    assert after["version"] == claim["request_version"] + 1
    assert after["runtime_epoch"] == epoch
    assert _wakes() == []


def test_confirmation_refuses_blank_stale_epoch_and_stale_version(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    terminal, epoch = _finished(api, world, agent_id, 61601, cause="model_error")
    running = _admit(api, world, agent_id, 61602)
    support.start_request(running.request_id)
    _fence(world)

    for request_id in (terminal.request_id, running.request_id):
        claim = _claimed(api, request_id)
        before = _request(request_id)
        _conflict(
            _record(api, request_id, claim, observation=" \t "), "termination_observation_required"
        )
        _conflict(
            _record(api, request_id, claim, runtime_epoch=claim["runtime_epoch"] + 1),
            "stale_owner",
        )
        _conflict(
            _record(api, request_id, claim, expected_request_version=claim["request_version"] - 1),
            "stale_version",
        )
        _conflict(
            _record(api, request_id, claim, expected_request_version=claim["request_version"] + 1),
            "stale_version",
        )
        assert _request(request_id) == before
        # The wire refuses an empty observation and a missing version outright.
        empty = _record(api, request_id, claim, observation="")
        assert empty.status_code == 422, empty.text
        missing = api.post(
            f"{support.INTERNAL_PREFIX}/requests/{request_id}/termination",
            json={"runtime_epoch": claim["runtime_epoch"], "observation": OBSERVATION},
            headers=WORKER_HEADERS,
        )
        assert missing.status_code == 422, missing.text
        assert _request(request_id) == before
        recorded = _record(api, request_id, claim)
        assert recorded.status_code == 200, recorded.text
    assert _request(terminal.request_id)["runtime_epoch"] == epoch
    assert (
        _request(running.request_id)["status"],
        _request(running.request_id)["terminal_cause"],
    ) == (
        "failed",
        "enrollment_fenced",
    )


def _stop_live_run(
    api: TestClient, world: support.World, agent_id: UUID, issue: int, stop: str
) -> tuple[UUID, int]:
    """A started run asked to stop while its owner's heartbeat lease is live."""

    if stop == "enrollment_fence":
        admitted = _admit(api, world, agent_id, issue)
        epoch = support.start_request(admitted.request_id)
        _fence(world)
    else:
        # The relabel arrives on the conversation the issue was admitted on.
        admitted = support.admit_service(
            api,
            WORKER_HEADERS,
            agent_id,
            issue_number=issue,
            conversation=support.WIRE_CONVERSATION,
        )
        epoch = support.start_request(admitted.request_id)
        support.relabel_while_live(
            agent_id,
            issue_number=issue,
            live_request_id=admitted.request_id,
            objective=REVISION_OBJECTIVE,
        )
    return admitted.request_id, epoch


@pytest.mark.parametrize(
    ("stop", "finish_code", "ended"),
    [
        ("enrollment_fence", "not_running", ("failed", "enrollment_fenced")),
        ("service_relabel", "work_item_cancelled", ("cancelled", "issue_cancelled")),
    ],
)
def test_live_owner_claim_keeps_the_epoch_its_interrupted_turn_finishes_with(
    api: TestClient, world: support.World, stop: str, finish_code: str, ended: tuple[str, str]
) -> None:
    """The owner's heartbeat stop re-claims before it interrupts its own turn.

    That claim renews the live lease without advancing the epoch, so the
    interrupted turn's finish, which presents the epoch it started with, is
    answered as a stopped run and not ``stale_owner``. Only a takeover, by
    another owner once the lease lapsed, advances the epoch and fences the
    previous owner; while the lease is live another owner is refused.
    """

    agent_id = support.insert_agent(world.tenant_id)
    request_id, epoch = _stop_live_run(api, world, agent_id, 61651, stop)
    stopping = _request(request_id)
    assert (stopping["status"], stopping["runtime_owner"], stopping["runtime_epoch"]) == (
        "cancellation_requested",
        support.RUNTIME_OWNER,
        epoch,
    )
    assert stopping["lease_lapsed"] is False

    # Another worker is refused while the owner's lease is live.
    _conflict(_claim(api, request_id, support.OTHER_RUNTIME_OWNER), "duplicate")
    assert _request(request_id) == stopping

    # The owner's own claim renews its lease and keeps the epoch and version.
    claim = _claimed(api, request_id)
    assert claim == {"runtime_epoch": epoch, "request_version": stopping["version"]}
    renewed = _request(request_id)
    assert (renewed["runtime_owner"], renewed["runtime_epoch"], renewed["version"]) == (
        support.RUNTIME_OWNER,
        epoch,
        stopping["version"],
    )
    assert renewed["runtime_heartbeat_expires_at"] > stopping["runtime_heartbeat_expires_at"]
    assert _claimed(api, request_id) == claim

    # The interrupted turn finishes with its start epoch: a stopped run, not a stale owner.
    _conflict(_finish(api, request_id, epoch), finish_code)
    assert _request(request_id)["status"] == "cancellation_requested"

    # The owner stalls past its lease; another worker takes the runtime over.
    _expire_lease(request_id)
    taken = _claimed(api, request_id, support.OTHER_RUNTIME_OWNER)
    assert taken == {"runtime_epoch": epoch + 1, "request_version": stopping["version"]}
    taken_over = _request(request_id)
    assert (taken_over["runtime_owner"], taken_over["lease_lapsed"]) == (
        support.OTHER_RUNTIME_OWNER,
        False,
    )
    # The superseded owner's late finish and record name the old epoch.
    _conflict(_finish(api, request_id, epoch), "stale_owner")
    _conflict(_record(api, request_id, claim), "stale_owner")
    assert _request(request_id) == taken_over

    recorded = _record(api, request_id, taken)
    assert recorded.status_code == 200, recorded.text
    assert (recorded.json()["status"], recorded.json()["terminal_cause"]) == ended


# --- outcomes ------------------------------------------------------------------------


def test_fenced_request_ends_failed_and_keeps_the_transcript(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = _admit(api, world, agent_id, 61701)
    support.start_request(first.request_id)
    thread = _seed_transcript(agent_id, first.work_item_id)
    # A personal run ending for an ordinary cause still expires its transcript.
    control = _admit(api, world, agent_id, 61702)
    control_epoch = support.start_request(control.request_id)
    control_thread = _seed_transcript(agent_id, control.work_item_id)
    support.finish_failed(control.request_id, control_epoch, cause="model_error")
    assert _transcripts(agent_id, control_thread) == 0

    _fence(world)
    claim = _claimed(api, first.request_id)
    recorded = _record(api, first.request_id, claim)
    assert recorded.status_code == 200, recorded.text
    assert (recorded.json()["status"], recorded.json()["terminal_cause"]) == (
        "failed",
        "enrollment_fenced",
    )

    assert _transcripts(agent_id, thread) == 1
    # No owner_lost successor: the owner resumes explicitly after revalidation.
    assert support.rows(
        "SELECT id FROM curie.execution_requests WHERE work_item_id = :item",
        {"item": first.work_item_id},
    ) == [{"id": first.request_id}]
    assert support.rows(
        "SELECT terminal_cause FROM curie.factory_terminal_notices "
        "WHERE execution_request_id = :id",
        {"id": first.request_id},
    ) == [{"terminal_cause": "enrollment_fenced"}]
    assert support.rows(
        "SELECT cancelled_at FROM curie.work_items WHERE id = :id", {"id": first.work_item_id}
    ) == [{"cancelled_at": None}]


def test_authentication_required_failure_keeps_owner_and_transcript(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    personal = _admit(api, world, agent_id, 61801)
    epoch = support.start_request(personal.request_id)
    thread = _seed_transcript(agent_id, personal.work_item_id)
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=61802, conversation="61802.000100"
    )
    service_epoch = support.start_request(service.request_id)
    service_thread = _seed_transcript(agent_id, service.work_item_id)

    finished = api.post(
        f"{support.INTERNAL_PREFIX}/requests/{personal.request_id}/finish",
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "authentication_required"},
        headers=WORKER_HEADERS,
    )
    assert finished.status_code == 200, finished.text
    assert (finished.json()["status"], finished.json()["terminal_cause"]) == (
        "failed",
        "authentication_required",
    )
    assert _transcripts(agent_id, thread) == 1
    support.assert_owner_tuples(
        personal.work_item_id,
        work_item=world.personal(),
        requests={personal.request_id: world.personal()},
    )
    # The carve-out is personal only: a service run expires its transcript.
    support.finish_failed(service.request_id, service_epoch, cause="authentication_required")
    assert _transcripts(agent_id, service_thread) == 0


def test_issue_cancellation_supersedes_an_enrollment_fence(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = _admit(api, world, agent_id, 61901)
    support.start_request(first.request_id)
    thread = _seed_transcript(agent_id, first.work_item_id)
    _fence(world)
    [item] = support.rows(
        "SELECT version FROM curie.work_items WHERE id = :id", {"id": first.work_item_id}
    )
    cancelled = api.post(
        f"{support.INTERNAL_PREFIX}/{first.work_item_id}/cancel",
        json={"expected_version": item["version"]},
        headers=WORKER_HEADERS,
    )
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["request"]["terminal_cause"] == "issue_cancelled"
    claim = _claimed(api, first.request_id)
    recorded = _record(api, first.request_id, claim)
    assert recorded.status_code == 200, recorded.text
    assert (recorded.json()["status"], recorded.json()["terminal_cause"]) == (
        "cancelled",
        "issue_cancelled",
    )
    # A cancelled WorkItem is not resumed, so its transcript goes.
    assert _transcripts(agent_id, thread) == 0


# --- revalidation and resume -----------------------------------------------------------


def test_revalidation_refuses_until_confirmed_then_binds_once(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = _admit(api, world, agent_id, 62001)
    support.start_request(first.request_id)
    _completed(first.request_id)
    old = world.generation
    generation, enrollment_version, _ = _fence(world)

    _conflict(_revalidate(api, world, agent_id, first.work_item_id), "runtime_live")
    claim = _claimed(api, first.request_id)
    # Claimed but not yet recorded: still live.
    _conflict(_revalidate(api, world, agent_id, first.work_item_id), "runtime_live")
    recorded = _record(api, first.request_id, claim)
    assert recorded.status_code == 200, recorded.text

    bound = _revalidate(api, world, agent_id, first.work_item_id)
    assert bound.status_code == 200, bound.text
    assert (
        bound.json()["previous_generation"],
        bound.json()["enrollment_generation"],
        bound.json()["enrollment_version"],
    ) == (old, generation, enrollment_version)
    _conflict(_revalidate(api, world, agent_id, first.work_item_id), "generation_current")
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(generation),
        requests={first.request_id: world.personal(old)},
    )


def test_disconnect_keeps_revalidation_refused_after_teardown(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first, _ = _finished(api, world, agent_id, 62101, cause="model_error")
    _fence(world, active=False)
    claim = _claimed(api, first.request_id)
    assert _record(api, first.request_id, claim).status_code == 200
    _conflict(_revalidate(api, world, agent_id, first.work_item_id), "enrollment_inactive")
    # Reconnect advances the generation again; there is nothing left to fence.
    generation, _, fence = _fence(world)
    assert fence == EnrollmentFence(stopping=(), pending_teardown=())
    bound = _revalidate(api, world, agent_id, first.work_item_id)
    assert bound.status_code == 200, bound.text
    assert bound.json()["enrollment_generation"] == generation


def test_explicit_resume_admits_a_fresh_request_copying_the_locked_owner_tuple(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first, _ = _finished(api, world, agent_id, 62201, cause="authentication_required")
    old = world.generation
    generation, _, _ = _fence(world)
    assert _record(api, first.request_id, _claimed(api, first.request_id)).status_code == 200
    assert _revalidate(api, world, agent_id, first.work_item_id).status_code == 200

    # An admission naming a different owner, or the superseded generation, is refused.
    support.rows(
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, :generation, true, 1)",
        {
            "id": uuid.uuid4(),
            "tenant": world.tenant_id,
            "principal": world.other_principal_id,
            "generation": generation,
        },
    )
    resume = support.facts(agent_id, issue_number=62201, conversation="62201.000100")
    body = support.facts_json(resume)
    for owner, code in (
        (
            {
                "tenant_id": str(world.tenant_id),
                "principal_id": str(world.other_principal_id),
                "enrollment_generation": generation,
            },
            "owner_mismatch",
        ),
        (world.owner(generation=old), "enrollment_generation_mismatch"),
    ):
        refused = api.post(
            f"{support.INTERNAL_PREFIX}/admissions",
            json={**body, "model_auth_mode": "personal", "owner": owner},
            headers=WORKER_HEADERS,
        )
        assert refused.status_code == 409, refused.text
        assert refused.json() == {"detail": {"code": code}}
    assert support.rows(
        "SELECT count(*) AS n FROM curie.execution_requests WHERE work_item_id = :item",
        {"item": first.work_item_id},
    ) == [{"n": 1}]

    resumed = api.post(
        f"{support.INTERNAL_PREFIX}/admissions",
        json={**body, "model_auth_mode": "personal", "owner": world.owner(generation=generation)},
        headers=WORKER_HEADERS,
    )
    assert resumed.status_code == 200, resumed.text
    assert resumed.json()["work_item"]["id"] == str(first.work_item_id)
    assert resumed.json()["request"]["status"] == "waiting"
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=world.personal(generation),
        requests={
            first.request_id: world.personal(old),
            resume.request_id: world.personal(generation),
        },
    )


def test_fence_and_pending_teardown_survive_an_api_restart(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    running = _admit(api, world, agent_id, 62301)
    epoch = support.start_request(running.request_id)
    terminal, _ = _finished(api, world, agent_id, 62302, cause="model_error")
    _fence(world)

    with TestClient(create_app()) as restarted:
        beat = _heartbeat(restarted, running.request_id, epoch)
        assert beat.status_code == 200, beat.text
        assert beat.json()["terminal_cause"] == "enrollment_fenced"
        assert _wakes() == sorted((running.request_id, terminal.request_id))
        _conflict(_revalidate(restarted, world, agent_id, terminal.work_item_id), "runtime_live")
        claim = _claimed(restarted, terminal.request_id)
        assert _record(restarted, terminal.request_id, claim).status_code == 200
        assert _revalidate(restarted, world, agent_id, terminal.work_item_id).status_code == 200


# --- ADR 0251 recovery capture ---------------------------------------------------------


def _recovery_authority(request_id: UUID, work_item_id: UUID) -> dict[str, Any]:
    [row] = support.rows(
        "SELECT runtime_epoch, dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return {
        "work_item_id": str(work_item_id),
        "source_request_id": str(request_id),
        "runtime_epoch": row["runtime_epoch"],
        "dispatch_generation": row["dispatch_generation"],
    }


def test_recovery_capture_completes_across_a_teardown_claim(
    world: support.World, monkeypatch: pytest.MonkeyPatch
) -> None:
    support.configure_api(monkeypatch, worker_token=WORKER_TOKEN)
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "true")
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_MAX_BYTES", "10000000")
    monkeypatch.setenv("CURIE_WORKSPACE_MAX_ARCHIVE_BYTES", str(RECOVERY_ARCHIVE_LIMIT))
    get_settings.cache_clear()
    with TestClient(create_app()) as api:
        agent_id = support.insert_agent(world.tenant_id)
        first, epoch = _finished(api, world, agent_id, 62401, cause="model_error")
        support.rows(
            "UPDATE curie.work_items SET base_branch = 'main', base_source = 'default', "
            "base_commit = :base WHERE id = :id",
            {"base": RECOVERY_BASE, "id": first.work_item_id},
        )
        authority = _recovery_authority(first.request_id, first.work_item_id)
        reserved = api.post(
            "/v1/internal/workspace-recovery/reserve",
            json={
                **authority,
                "idempotency_key": uuid.uuid4().hex,
                "repository": {
                    "provider": "github",
                    "host": "github.com",
                    "project_id": "101",
                    "path": support.REPO,
                },
                "base_commit": RECOVERY_BASE,
                "target_branch": "main",
                "base_archive_limit_bytes": RECOVERY_ARCHIVE_LIMIT,
                "objects": [
                    {
                        "kind": kind,
                        "size_bytes": size,
                        "sha256": hashlib.sha256(kind.encode()).hexdigest(),
                    }
                    for kind, size in (("base", 100), ("patch", 30), ("manifest", 20))
                ],
            },
            headers=WORKER_HEADERS,
        )
        assert reserved.status_code == 200, reserved.text
        reservation = reserved.json()

        # The fence and the teardown claim land while the capture is in flight.
        _fence(world)
        claim = _claimed(api, first.request_id)
        assert claim["runtime_epoch"] == epoch

        completed = api.post(
            "/v1/internal/workspace-recovery/complete",
            json={
                **authority,
                "checkpoint_id": reservation["checkpoint_id"],
                "receipts": reservation["objects"],
            },
            headers=WORKER_HEADERS,
        )
        assert completed.status_code == 200, completed.text
        assert completed.json()["status"] == "complete"

        # The teardown then records; the run's checkpoint survives fence and teardown.
        assert _record(api, first.request_id, claim).status_code == 200
        assert support.rows(
            "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
            {"id": reservation["checkpoint_id"]},
        ) == [{"status": "complete"}]
        assert support.rows(
            "SELECT last_good_checkpoint_id::text AS last FROM curie.workspace_recovery_groups "
            "WHERE work_item_id = :item",
            {"item": first.work_item_id},
        ) == [{"last": reservation["checkpoint_id"]}]
    get_settings.cache_clear()
