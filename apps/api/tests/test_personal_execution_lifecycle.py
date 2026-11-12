"""Personal ownership survives every lifecycle-created request (#5376 AC2).

ADR 0268 rows 4, 7 and 8: a personal WorkItem's owner tuple (``tenant_id``,
``model_auth_mode``, ``owner_principal_id``, ``enrollment_generation``) is fixed
when the trusted internal admission creates it, and every ExecutionRequest
snapshots it at insertion. Revisions, relabels, base-reconcile readmissions, a
CI fix hold, an owner-lost successor and an approval resolved by a different
principal all arrive with service-default facts or another author; none of
them may move the tuple, and none may read it from requester text.

Each step opens its own engine and session, every assertion reads the durable
rows through a fresh engine, and every test ends by projecting each request it
created through a newly constructed worker ``BindingResolver``. Postgres and
Valkey are real; only forge HTTP would be faked, and no step here calls it.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import personal_execution_support as support
import pytest
from curie_api import workitem_dispatch
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.workitem_dispatch import (
    AcquireGrant,
    DeferResult,
    DispatchConflict,
    HeartbeatResult,
    OwnerLostResult,
    StartResult,
    TerminationClaim,
)
from curie_api.workitems import lifecycle
from curie_api.workitems.lifecycle import WorkItemOutcome
from fastapi.testclient import TestClient
from forge_fakes import factory_wake, no_external_calls
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from test_workitems import _grant_opened_pull_request

WORKER_TOKEN = "personal-lifecycle-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
REVISION_OBJECTIVE = "Address the follow up comment on the issue"
RELABEL_OBJECTIVE = "Run the task again after the label was added back"
BASE_OBJECTIVE = (
    "Verify the repository after its base changed. "
    "Run fresh validation and continue the existing task."
)
ROUTE = "personal-approvers"
APPROVER = "U0APPROVER1"


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


async def _admit_pending_readmit(session: AsyncSession, work_item_id: UUID) -> object:
    now = await lifecycle.database_now(session)
    return await lifecycle.admit_pending_readmit(
        session, work_item_id=work_item_id, wait_deadline=now + timedelta(hours=1)
    )


async def _admit_next_revision(session: AsyncSession, work_item_id: UUID) -> object:
    now = await lifecycle.database_now(session)
    return await lifecycle.admit_next_revision(
        session, work_item_id=work_item_id, wait_deadline=now + timedelta(hours=1)
    )


def test_revision_from_another_requester_snapshots_the_original_owner(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53761)
    original = world.personal()
    support.assert_owner_tuples(
        first.work_item_id, work_item=original, requests={first.request_id: original}
    )
    epoch = support.start_request(first.request_id)

    # A verified mention while the run is live: service-default facts, another author.
    revision = support.facts(
        agent_id,
        issue_number=53761,
        requester=support.OTHER_REQUESTER,
        objective=REVISION_OBJECTIVE,
    )
    revised = support.with_session(
        lambda session: workitem_dispatch.admit_revision(session, revision)
    )
    assert isinstance(revised, WorkItemOutcome), revised
    assert revised.request is not None
    assert (revised.request.id, revised.request.status) == (revision.request_id, "queued")
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=original,
        requests={first.request_id: original, revision.request_id: original},
    )
    # The requester differs; the owner does not.
    assert support.request_column(revision.request_id, "requester") == support.OTHER_REQUESTER
    assert support.request_column(first.request_id, "requester") == support.REQUESTER

    # The live run ends and the queued revision is promoted.
    support.finish_failed(first.request_id, epoch)
    promoted = support.with_session(
        lambda session: _admit_next_revision(session, first.work_item_id)
    )
    assert isinstance(promoted, WorkItemOutcome), promoted
    assert promoted.request is not None
    assert (promoted.request.id, promoted.request.status) == (revision.request_id, "waiting")
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=original,
        requests={first.request_id: original, revision.request_id: original},
    )

    support.assert_resolver_projects(
        world,
        agent_id=agent_id,
        work_item_id=first.work_item_id,
        generations={first.request_id: world.generation, revision.request_id: world.generation},
    )
    # Another tenant's resolver answers exactly as it does for an unknown id.
    foreign = support.resolve_execution_owners(
        [first.request_id, uuid4()], tenant_id=world.other_tenant_id
    )
    assert set(foreign.values()) == {None}
    unknown = support.resolve_execution_owners([uuid4()], tenant_id=world.tenant_id)
    assert set(unknown.values()) == {None}


def test_relabel_and_base_reconcile_readmissions_keep_the_original_owner(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53762)
    original = world.personal()
    support.start_request(first.request_id)

    # A relabel while the run is live parks on the WorkItem; no request yet.
    relabel = support.facts(
        agent_id,
        issue_number=53762,
        requester=support.OTHER_REQUESTER,
        objective=RELABEL_OBJECTIVE,
    )
    parked = support.with_session(lambda session: workitem_dispatch.readmit(session, relabel))
    assert isinstance(parked, WorkItemOutcome), parked
    assert parked.request is not None
    assert (parked.request.id, parked.request.status) == (
        first.request_id,
        "cancellation_requested",
    )
    assert support.rows(
        "SELECT readmit_request_id FROM curie.work_items WHERE id = :id",
        {"id": first.work_item_id},
    ) == [{"readmit_request_id": relabel.request_id}]
    support.assert_owner_tuples(
        first.work_item_id, work_item=original, requests={first.request_id: original}
    )

    # The live request ends through the real termination chain.
    claimed = support.with_session(
        lambda session: workitem_dispatch.claim_termination(
            session, first.request_id, owner=support.RUNTIME_OWNER
        )
    )
    assert isinstance(claimed, TerminationClaim), claimed
    ended = support.with_session(
        lambda session: workitem_dispatch.record_termination(
            session,
            first.request_id,
            runtime_epoch=claimed.runtime_epoch,
            expected_request_version=claimed.request_version,
            observation=support.TERMINATION_OBSERVATION,
        )
    )
    assert isinstance(ended, WorkItemOutcome), ended
    assert ended.request is not None and ended.request.status == "cancelled"

    # The stored relabel is admitted with the WorkItem's tuple, not its author's.
    admitted = support.with_session(
        lambda session: _admit_pending_readmit(session, first.work_item_id)
    )
    assert isinstance(admitted, WorkItemOutcome), admitted
    assert admitted.request is not None
    assert (admitted.request.id, admitted.request.status) == (relabel.request_id, "waiting")
    assert support.request_column(relabel.request_id, "requester") == support.OTHER_REQUESTER
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=original,
        requests={first.request_id: original, relabel.request_id: original},
    )

    # A base-reconcile style readmission authored by the factory itself.
    reconcile = support.facts(
        agent_id, issue_number=53762, requester="factory", objective=BASE_OBJECTIVE
    )
    readmitted = support.with_session(lambda session: workitem_dispatch.readmit(session, reconcile))
    assert isinstance(readmitted, WorkItemOutcome), readmitted
    assert readmitted.request is not None
    assert (readmitted.request.id, readmitted.request.status) == (
        reconcile.request_id,
        "waiting",
    )
    assert support.request_column(relabel.request_id, "status") == "cancelled"
    assert support.request_column(reconcile.request_id, "requester") == "factory"
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=original,
        requests={
            first.request_id: original,
            relabel.request_id: original,
            reconcile.request_id: original,
        },
    )

    support.assert_resolver_projects(
        world,
        agent_id=agent_id,
        work_item_id=first.work_item_id,
        generations={
            first.request_id: world.generation,
            relabel.request_id: world.generation,
            reconcile.request_id: world.generation,
        },
    )


def test_ci_fix_hold_keeps_the_owner_on_the_running_request(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53763)
    original = world.personal()
    support.start_request(first.request_id)

    async def open_pull_request(session: AsyncSession) -> None:
        work_item = await lifecycle.lock_work_item(session, first.work_item_id)
        request = await lifecycle.lock_request(
            session, work_item_id=first.work_item_id, request_id=first.request_id
        )
        assert work_item is not None and request is not None
        await _grant_opened_pull_request(
            session, await lifecycle.outcome(session, work_item, request), pr=53763
        )

    support.with_session(open_pull_request)
    settlement = support.with_session(
        lambda session: lifecycle.claim_publication_settlement(session, exclude=frozenset())
    )
    assert settlement is not None and settlement.request_id == first.request_id
    assert isinstance(settlement.subject, lifecycle.PublicationCISubject)
    held = support.with_session(
        lambda session: lifecycle.hold_for_ci_fix(
            session,
            work_item_id=settlement.work_item_id,
            request_id=first.request_id,
            expected_request_version=settlement.request_version,
            expected_subject=settlement.subject,
        )
    )
    assert held is True
    assert support.rows(
        "SELECT status, runtime_heartbeat_expires_at = execution_deadline AS parked "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": first.request_id},
    ) == [{"status": "running", "parked": True}]
    assert support.rows(
        "SELECT publication_lineage_id IS NOT NULL AS linked FROM curie.work_items WHERE id = :id",
        {"id": first.work_item_id},
    ) == [{"linked": True}]
    support.assert_owner_tuples(
        first.work_item_id, work_item=original, requests={first.request_id: original}
    )

    support.assert_resolver_projects(
        world,
        agent_id=agent_id,
        work_item_id=first.work_item_id,
        generations={first.request_id: world.generation},
    )


def test_owner_lost_successor_carries_the_original_owner(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53764)
    original = world.personal()
    epoch = support.start_request(first.request_id)

    lost = support.with_session(
        lambda session: workitem_dispatch.declare_owner_lost(
            session, first.request_id, owner=support.RUNTIME_OWNER, runtime_epoch=epoch
        )
    )
    assert isinstance(lost, OwnerLostResult), lost
    claimed = support.with_session(
        lambda session: workitem_dispatch.claim_termination(
            session, first.request_id, owner=support.OTHER_RUNTIME_OWNER
        )
    )
    assert isinstance(claimed, TerminationClaim), claimed
    settled = support.with_session(
        lambda session: workitem_dispatch.record_termination(
            session,
            first.request_id,
            runtime_epoch=claimed.runtime_epoch,
            expected_request_version=claimed.request_version,
            observation=support.TERMINATION_OBSERVATION,
        )
    )
    assert isinstance(settled, WorkItemOutcome), settled
    assert settled.request is not None
    assert (settled.request.status, settled.request.terminal_cause) == ("failed", "owner_lost")

    successors = support.rows(
        "SELECT id, status, owner_lost_retry, requester FROM curie.execution_requests "
        "WHERE work_item_id = :work_item AND id <> :lost",
        {"work_item": first.work_item_id, "lost": first.request_id},
    )
    assert len(successors) == 1, successors
    [successor] = successors
    assert (successor["status"], successor["owner_lost_retry"], successor["requester"]) == (
        "waiting",
        True,
        support.REQUESTER,
    )
    support.assert_owner_tuples(
        first.work_item_id,
        work_item=original,
        requests={first.request_id: original, successor["id"]: original},
    )

    support.assert_resolver_projects(
        world,
        agent_id=agent_id,
        work_item_id=first.work_item_id,
        generations={first.request_id: world.generation, successor["id"]: world.generation},
    )


@pytest.mark.usefixtures("valkey")
def test_approval_by_another_principal_changes_only_the_approval_facts(
    api: TestClient,
    world: support.World,
    auth_headers: dict[str, str],
    runs_stream: str,
) -> None:
    agent = support.api_agent(
        api,
        auth_headers,
        tenant_id=world.tenant_id,
        approval_routes={ROUTE: support.users_route([APPROVER])},
    )
    agent_id = agent["agent_id"]
    first = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53765)
    original = world.personal()
    epoch = support.start_request(first.request_id)
    held = support.with_session(
        lambda session: workitem_dispatch.hold_for_approval(
            session, first.request_id, runtime_epoch=epoch
        )
    )
    assert isinstance(held, HeartbeatResult), held

    approval = support.tool_approval(api, auth_headers, agent_id=agent_id, route=ROUTE)
    resolved = api.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=support.operator_headers(api, auth_headers, APPROVER),
    )
    assert resolved.status_code == 200, resolved.text
    assert (resolved.json()["status"], resolved.json()["resolved_by"]) == ("approved", APPROVER)

    # The resume turn is authored by the approver, not by the execution owner.
    turns = support.stream_turns(runs_stream)
    assert [(turn.event_id, turn.author) for turn in turns] == [
        (f"approval-{approval['id']}-resolved", APPROVER)
    ]
    audit = api.get(f"/approvals/{approval['id']}/audit", headers=auth_headers)
    assert audit.status_code == 200, audit.text
    assert [
        (entry["principal_kind"], entry["actor"]) for entry in audit.json() if entry["authorized"]
    ] == [("operator", APPROVER)]

    support.assert_owner_tuples(
        first.work_item_id, work_item=original, requests={first.request_id: original}
    )
    support.assert_resolver_projects(
        world,
        agent_id=agent_id,
        work_item_id=first.work_item_id,
        generations={first.request_id: world.generation},
    )


# --- #5378: the start gate and start_failed retention -----------------------------------


def _acquire(request_id: UUID) -> tuple[int, Any]:
    """Acquire as the worker does; the generation and context its start names."""

    async def body(session: AsyncSession) -> tuple[int, Any]:
        generation = await session.scalar(
            text("SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id"),
            {"id": request_id},
        )
        assert isinstance(generation, int), request_id
        async with no_external_calls() as http_client:
            acquired = await workitem_dispatch.acquire(
                session,
                request_id,
                owner=support.RUNTIME_OWNER,
                generation=generation,
                wake=await factory_wake(session, request_id),
                context=None,
                client=http_client,
            )
        assert isinstance(acquired, AcquireGrant), acquired
        return generation, acquired.context

    return support.with_session(body)


def _start(request_id: UUID, generation: int, context: Any) -> StartResult | DispatchConflict:
    return support.with_session(
        lambda session: workitem_dispatch.start(
            session,
            request_id,
            owner=support.RUNTIME_OWNER,
            generation=generation,
            claim_name=support.CLAIM_NAME,
            sandbox_name=support.SANDBOX_NAME,
            context=context,
        )
    )


def _defer(request_id: UUID, generation: int, reason: str) -> DeferResult | DispatchConflict:
    return support.with_session(
        lambda session: workitem_dispatch.defer(
            session,
            request_id,
            owner=support.RUNTIME_OWNER,
            generation=generation,
            reason=reason,
            capacity=False,
            start_failure_detail=None,
        )
    )


def _waiting_shape(request_id: UUID) -> dict[str, Any]:
    [row] = support.rows(
        "SELECT status, acquire_owner, started_at, terminal_cause "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row


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


def test_start_refuses_a_personal_request_whose_enrollment_moved_and_leaves_it_waiting(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    moved = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53781)
    disabled = support.admit_personal(
        api, WORKER_HEADERS, world, agent_id, issue_number=53782, conversation="53782.000100"
    )
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=53783, conversation="53783.000100"
    )
    moved_generation, moved_context = _acquire(moved.request_id)
    disabled_generation, disabled_context = _acquire(disabled.request_id)

    # The owner disconnects: no personal start, and the request is not ended.
    support.set_enrollment_active(world, False)
    refused = _start(disabled.request_id, disabled_generation, disabled_context)
    assert isinstance(refused, DispatchConflict), refused
    assert refused.code == "enrollment_inactive"
    assert _waiting_shape(disabled.request_id) == {
        "status": "waiting",
        "acquire_owner": support.RUNTIME_OWNER,
        "started_at": None,
        "terminal_cause": None,
    }
    # Liveness: reconnected at the same generation, the same acquire starts.
    support.set_enrollment_active(world, True)
    started = _start(disabled.request_id, disabled_generation, disabled_context)
    assert isinstance(started, StartResult), started

    # The owner replaces the subscription: the request's generation is stale.
    support.advance_enrollment(world)
    refused = _start(moved.request_id, moved_generation, moved_context)
    assert isinstance(refused, DispatchConflict), refused
    assert refused.code == "enrollment_generation_mismatch"
    assert _waiting_shape(moved.request_id)["status"] == "waiting"
    assert _waiting_shape(moved.request_id)["acquire_owner"] == support.RUNTIME_OWNER
    # A service request never consults an enrollment.
    assert support.start_request(service.request_id) >= 1


def test_a_personal_start_failed_keeps_the_transcript_and_a_service_one_does_not(
    api: TestClient, world: support.World
) -> None:
    agent_id = support.insert_agent(world.tenant_id)
    personal = support.admit_personal(api, WORKER_HEADERS, world, agent_id, issue_number=53784)
    service = support.admit_service(
        api, WORKER_HEADERS, agent_id, issue_number=53785, conversation="53785.000100"
    )
    limit = get_settings().work_item_start_deferral_limit
    threads: dict[UUID, str] = {}
    for admitted in (personal, service):
        threads[admitted.request_id] = _seed_transcript(agent_id, admitted.work_item_id)
        # One start deferral short of the limit, as earlier refused deliveries leave it.
        support.rows(
            "UPDATE curie.execution_requests SET start_deferrals = :n WHERE id = :id",
            {"n": limit - 1, "id": admitted.request_id},
        )
        generation, _context = _acquire(admitted.request_id)
        deferred = _defer(admitted.request_id, generation, "not_started:enrollment_inactive")
        assert isinstance(deferred, DeferResult), deferred
        assert deferred.terminal_cause == "start_failed"

    # The owner resumes a personal WorkItem after reconnecting, so its history stays.
    assert _transcripts(agent_id, threads[personal.request_id]) == 1
    assert _transcripts(agent_id, threads[service.request_id]) == 0
    assert support.rows(
        "SELECT cancelled_at FROM curie.work_items WHERE id = :id",
        {"id": personal.work_item_id},
    ) == [{"cancelled_at": None}]
