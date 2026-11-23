"""Maintenance settlement, debit, replay and first dispatch against PostgreSQL."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Literal

import personal_execution_support as support
import pytest
from curie_api import factory_maintenance_reasons, workitem_dispatch
from curie_api.config import get_settings
from curie_api.factory_base import ResolvedBase
from curie_api.factory_maintenance import (
    MaintenanceObservation,
    MaintenanceRefusal,
    MaintenanceRepair,
)
from curie_api.factory_maintenance_store import commit_observation, replay
from curie_api.models import (
    DEFAULT_TENANT_ID,
    ExecutionRequest,
    FactoryMaintenanceAdmission,
    FactoryMaintenanceEvent,
    FactoryMaintenanceLineage,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
    WorkItem,
)
from curie_api.personal_execution import OwnerContext, OwnerTuple
from curie_api.workitems import lifecycle
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from test_factory_ci_wait import _attach_fix_async
from test_workitems import _grant_opened_pull_request, _start

BASE = "0123456789abcdef0123456789abcdef01234567"
REPAIR = "Repair the failing check on the published head of the same pull request."


@pytest.fixture(autouse=True)
def maintenance_enabled(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("FACTORY_MAINTENANCE_REPAIR_ENABLED", "true")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def seed_completed(
    session: AsyncSession,
    *,
    green: bool = True,
    personal: support.World | None = None,
    trigger_label: str | None = None,
) -> MaintenanceObservation:
    tenant_id = personal.tenant_id if personal else DEFAULT_TENANT_ID
    agent = await support._insert_agent(session, tenant_id, address=support.ADDRESS)
    facts = replace(
        support.facts(agent, issue_number=2573),
        base=ResolvedBase("main", "default", BASE),
        trigger_label=trigger_label,
    )
    if personal:
        facts = replace(
            facts,
            model_auth_mode="personal",
            owner=OwnerContext(personal.tenant_id, personal.owner_id, personal.generation),
        )
    admitted = await workitem_dispatch.admit(session, facts)
    assert isinstance(admitted, lifecycle.WorkItemOutcome), admitted
    running = await _start(session, admitted)
    linked = await _grant_opened_pull_request(session, running, pr=123)
    return await _settle(session, linked, green=green)


async def _settle(
    session: AsyncSession, linked: lifecycle.WorkItemOutcome, *, green: bool
) -> MaintenanceObservation:
    assert linked.request and linked.work_item.publication_lineage_id
    item = await session.get(WorkItem, linked.work_item.id)
    assert item
    item.base_resolved_request_id = linked.request.id
    lineage = await session.get(
        ThreadPublicationLineage, linked.work_item.publication_lineage_id, populate_existing=True
    )
    assert lineage and lineage.head_sha and lineage.base_ref
    publication = (
        await session.scalars(
            select(Publication).where(Publication.execution_request_id == linked.request.id)
        )
    ).one()
    if publication.terminal_at is None:
        publication.terminal_at = (await lifecycle.database_now(session)).replace(tzinfo=None)
    subject = lifecycle.PublicationCISubject(
        publication.id,
        publication.version,
        lineage.head_sha,
        lineage.base_ref,
        tuple(publication.changed_paths),
        publication.terminal_at,
    )
    completed = await lifecycle.settle_ci_verdict(
        session,
        work_item_id=linked.work_item.id,
        request_id=linked.request.id,
        expected_work_item_version=linked.work_item.version,
        expected_request_version=linked.request.version,
        expected_subject=subject,
        status="completed",
        cause="completed",
        detail=None,
        verified_ci="green" if green else "no_ci",
    )
    assert isinstance(completed, lifecycle.WorkItemOutcome) and completed.request, completed
    return MaintenanceObservation(
        work_item_id=completed.work_item.id,
        request_id=completed.request.id,
        lineage_id=lineage.id,
        work_item_version=completed.work_item.version,
        request_version=completed.request.version,
        lineage_version=lineage.version,
        head_sha=lineage.head_sha,
        base_ref=lineage.base_ref,
        base_sha=BASE,
        reason="late_ci_red",
    )


async def _counts(session: AsyncSession) -> tuple[int | None, int | None, int | None]:
    return (
        await session.scalar(select(func.count()).select_from(ExecutionRequest)),
        await session.scalar(select(func.count()).select_from(FactoryMaintenanceAdmission)),
        await session.scalar(select(FactoryMaintenanceLineage.repairs_used)),
    )


def test_racing_observers_create_one_dispatchable_successor(clean_db: None) -> None:
    async def scenario(session: AsyncSession) -> None:
        obs = await seed_completed(session)

        async def admit() -> MaintenanceRepair | MaintenanceRefusal:
            async with AsyncSession(bind=session.bind, expire_on_commit=False) as competing:
                return await commit_observation(competing, obs, objective=REPAIR, refusal=None)

        results = await asyncio.gather(admit(), admit())
        assert all(isinstance(result, MaintenanceRepair) for result in results)
        receipts = [result for result in results if isinstance(result, MaintenanceRepair)]
        assert {receipt.status for receipt in receipts} == {"admitted", "replayed"}
        assert receipts[0].request_id == receipts[1].request_id
        assert await _counts(session) == (2, 1, 1)
        claims = await workitem_dispatch.claim_due(
            session, owner="reconciler", limit=10, lease_s=15
        )
        assert [claim.request_id for claim in claims] == [receipts[0].request_id]
        wake = await workitem_dispatch.load_execute_wake(session, claims[0].request_id)
        assert wake is not None
        successor, item, binding = wake
        assert binding and successor.objective == REPAIR
        assert successor.reply_conversation_id == support.WIRE_CONVERSATION
        assert successor.reply_address == binding.address == support.ADDRESS
        assert successor.requester == support.REQUESTER
        source = await session.get(ExecutionRequest, obs.request_id)
        assert source and source.status == "completed" and source.objective == support.OBJECTIVE
        assert source.version == obs.request_version
        assert OwnerTuple.of(successor) == OwnerTuple.of(source) == OwnerTuple.of(item)
        assert item.automatic_retries_used == 0
        window = await session.get(FactoryMaintenanceLineage, obs.lineage_id)
        assert window and window.origin_completed_at == window.latest_green_at == source.terminal_at
        assert window.expires_at == window.origin_completed_at + timedelta(hours=24)
        lineage = await session.get(ThreadPublicationLineage, obs.lineage_id)
        assert lineage
        lineage.status = "closed"
        await session.commit()
        prior = await replay(session, replace(obs, work_item_version=99, lineage_version=99))
        assert prior == MaintenanceRepair(status="replayed", request_id=successor.id)
        assert await _counts(session) == (2, 1, 1)

    support.with_session(scenario)


def test_failure_to_persist_event_rolls_back_request_and_allowance(clean_db: None) -> None:
    async def scenario(session: AsyncSession) -> None:
        obs = await seed_completed(session)
        await session.execute(
            text("ALTER TABLE curie.factory_maintenance_events ADD CHECK (outcome <> 'admitted')")
        )
        with pytest.raises(IntegrityError):
            await commit_observation(session, obs, objective=REPAIR, refusal=None)
        await session.rollback()
        assert await _counts(session) == (1, 0, 0)
        assert await replay(session, obs) is None
        assert isinstance(
            await commit_observation(session, obs, objective=REPAIR, refusal=None),
            MaintenanceRepair,
        )

    support.with_session(scenario)


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ("stale_item", "snapshot_changed"),
        ("stale_request", "snapshot_changed"),
        ("stale_lineage", "snapshot_changed"),
        ("cancel", "work_item_cancelled"),
        ("closed", "pr_closed"),
        ("foreign", "foreign_head"),
        ("base", "base_changed"),
        ("no_ci", "prior_green_missing"),
        ("expired", "window_expired"),
        ("unregistered_conflict", "reason_unavailable"),
        ("provider", "failure_not_attributable"),
        ("superseded", "superseded"),
        ("reserved", "superseded"),
        ("publishing", "superseded"),
        ("retarget", "retarget_pending"),
    ],
)
def test_refusal_records_outcome_without_spending(
    clean_db: None, change: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario(session: AsyncSession) -> None:
        obs = await seed_completed(session, green=change != "no_ci")
        item = await session.get(WorkItem, obs.work_item_id)
        lineage = await session.get(ThreadPublicationLineage, obs.lineage_id)
        assert item and lineage
        if change == "stale_item":
            obs = replace(obs, work_item_version=0)
        if change == "stale_request":
            obs = replace(obs, request_version=0)
        if change == "stale_lineage":
            obs = replace(obs, lineage_version=0)
        if change == "cancel":
            item.cancelled_at = await lifecycle.database_now(session)
        if change == "closed":
            lineage.status = "closed"
        if change == "foreign":
            obs = replace(obs, head_sha="f" * 40)
        if change == "base":
            obs = replace(obs, base_ref="other")
        if change == "expired":
            window = await session.get(FactoryMaintenanceLineage, obs.lineage_id)
            assert window

            async def at_expiry(db: AsyncSession) -> datetime:
                return (await db.scalars(select(FactoryMaintenanceLineage.expires_at))).one()

            monkeypatch.setattr(lifecycle, "database_now", at_expiry)
        if change == "unregistered_conflict":
            monkeypatch.delitem(factory_maintenance_reasons._REASON_HOOKS, "base_conflict")
            obs = replace(obs, reason="base_conflict")
        if change == "reserved":
            session.add(
                PublicationReviewReservation(
                    origin_key="verified-review",
                    lineage_id=lineage.id,
                    lineage_version=lineage.version,
                    expected_head_sha=obs.head_sha,
                    revision_number=2,
                    binding_id=uuid.uuid4(),
                    binding_generation=1,
                )
            )
        if change == "publishing":
            await _attach_fix_async(
                item.id,
                obs.request_id,
                revision=2,
                head_sha="2" * 40,
                title="Pending change",
                paths=["README.md"],
                status="pending",
            )
        if change == "retarget":
            item.base_requested_branch, item.base_requested_source = "release", "label"
        if change == "superseded":
            newer = await lifecycle.create_execution_request(
                session,
                work_item_id=item.id,
                request_id=uuid.uuid4(),
                expected_work_item_version=item.version,
                wait_deadline=await lifecycle.database_now(session) + timedelta(minutes=10),
            )
            assert isinstance(newer, lifecycle.WorkItemOutcome)
        await session.commit()
        provider_refusal: Literal["failure_not_attributable"] | None = (
            "failure_not_attributable" if change == "provider" else None
        )
        for _ in range(2):
            result = await commit_observation(
                session, obs, objective=REPAIR, refusal=provider_refusal
            )
            assert isinstance(result, MaintenanceRefusal) and result.reason == expected
        assert await _counts(session) == (2 if change == "superseded" else 1, 0, 0)
        assert (
            await session.scalars(
                select(FactoryMaintenanceEvent.outcome).where(
                    FactoryMaintenanceEvent.reason == obs.reason
                )
            )
        ).all() == [expected]

    support.with_session(scenario)


@pytest.mark.parametrize("rotate", [False, True])
def test_personal_maintenance_uses_live_generation(clean_db: None, rotate: bool) -> None:
    world = support.seed_world()

    async def seed(session: AsyncSession) -> MaintenanceObservation:
        return await seed_completed(session, personal=world)

    obs = support.with_session(seed)
    if rotate:
        support.advance_enrollment(world)

    async def scenario(session: AsyncSession) -> None:
        result = await commit_observation(session, obs, objective=REPAIR, refusal=None)
        if rotate:
            assert result == MaintenanceRefusal(reason="enrollment_fenced")
            assert await _counts(session) == (1, 0, 0)
        else:
            assert isinstance(result, MaintenanceRepair)
            successor = await session.get(ExecutionRequest, result.request_id)
            assert successor and OwnerTuple.of(successor) == OwnerTuple.personal(
                OwnerContext(world.tenant_id, world.owner_id, world.generation)
            )

    support.with_session(scenario)


def test_later_green_revision_preserves_the_original_window_and_allowance(clean_db: None) -> None:
    async def scenario(session: AsyncSession) -> None:
        original = await seed_completed(session, green=False)
        item = await session.get(WorkItem, original.work_item_id)
        window = await session.get(FactoryMaintenanceLineage, original.lineage_id)
        assert item and window
        deadline, origin_time = window.expires_at, window.origin_completed_at
        assert window.latest_green_at is None
        revision = await workitem_dispatch.admit_revision(
            session, support.facts(item.agent_id, issue_number=2573, objective="Update the README")
        )
        assert isinstance(revision, lifecycle.WorkItemOutcome) and revision.request
        running = await _start(session, revision)
        await _attach_fix_async(
            item.id,
            revision.request.id,
            revision=2,
            head_sha="2" * 40,
            title="Revise README",
            paths=["README.md"],
        )
        revised = await _settle(session, running, green=True)
        assert revised.head_sha == "2" * 40
        assert (window.origin_request_id, window.origin_completed_at, window.expires_at) == (
            original.request_id,
            origin_time,
            deadline,
        )
        assert window.latest_green_at is not None and window.latest_green_at > origin_time
        assert window.latest_green_head_sha == revised.head_sha
        receipt = await commit_observation(session, revised, objective=REPAIR, refusal=None)
        assert isinstance(receipt, MaintenanceRepair)
        successor = await session.get(ExecutionRequest, receipt.request_id)
        assert successor
        waiting = await lifecycle.outcome(session, item, successor)
        repairing = await _start(session, waiting)
        await _attach_fix_async(
            item.id,
            successor.id,
            revision=3,
            head_sha="3" * 40,
            title="Repair README",
            paths=["README.md"],
        )
        repaired = await _settle(session, repairing, green=True)
        assert window.expires_at == deadline
        for reason in ("late_ci_red", "base_conflict"):
            assert await commit_observation(
                session, replace(repaired, reason=reason), objective=REPAIR, refusal=None
            ) == MaintenanceRefusal(reason="budget_exhausted")
        assert await _counts(session) == (3, 1, 1)
        assert await replay(session, revised) == MaintenanceRepair(
            status="replayed", request_id=successor.id
        )

    support.with_session(scenario)


def test_late_red_on_a_no_ci_revision_cannot_spend_an_earlier_heads_green(clean_db: None) -> None:
    async def scenario(session: AsyncSession) -> None:
        origin = await seed_completed(session)
        item = await session.get(WorkItem, origin.work_item_id)
        assert item
        revision = await workitem_dispatch.admit_revision(
            session, support.facts(item.agent_id, issue_number=2573, objective="Update the README")
        )
        assert isinstance(revision, lifecycle.WorkItemOutcome) and revision.request
        running = await _start(session, revision)
        await _attach_fix_async(
            item.id,
            revision.request.id,
            revision=2,
            head_sha="2" * 40,
            title="Revise README",
            paths=["README.md"],
        )
        observed = await _settle(session, running, green=False)
        assert await commit_observation(session, observed, objective=REPAIR, refusal=None) == (
            MaintenanceRefusal(reason="prior_green_missing")
        )
        assert await _counts(session) == (2, 0, 0)

    support.with_session(scenario)


def test_postgres_refuses_rewriting_maintenance_authority(clean_db: None) -> None:
    async def scenario(session: AsyncSession) -> None:
        obs = await seed_completed(session)
        receipt = await commit_observation(session, obs, objective=REPAIR, refusal=None)
        assert isinstance(receipt, MaintenanceRepair)
        for statement in (
            "UPDATE curie.factory_maintenance_lineages SET origin_head_sha = 'other'",
            "UPDATE curie.factory_maintenance_lineages SET "
            "origin_completed_at = origin_completed_at + interval '1 hour', "
            "expires_at = expires_at + interval '1 hour'",
            "UPDATE curie.factory_maintenance_lineages SET repairs_used = 0",
            "UPDATE curie.factory_maintenance_lineages SET latest_green_at = NULL, "
            "latest_green_head_sha = NULL",
            "UPDATE curie.factory_maintenance_admissions SET head_sha = 'other'",
            "UPDATE curie.factory_maintenance_admissions SET base_ref = 'another-base'",
            "UPDATE curie.factory_maintenance_admissions SET base_sha = 'other'",
            "UPDATE curie.factory_maintenance_admissions SET reason = 'base_conflict'",
        ):
            with pytest.raises(IntegrityError, match="authority cannot be rewritten"):
                async with session.begin_nested():
                    await session.execute(text(statement))
        assert await _counts(session) == (2, 1, 1)
        assert await replay(session, obs) == MaintenanceRepair(
            status="replayed", request_id=receipt.request_id
        )

    support.with_session(scenario)
