"""The resume tombstone read on the worker's execution side (#2753).

``approvals.resume_cancelled_at`` is a TOMBSTONE, not a CAS. A compare-and-set
on ``resumed_at`` cannot retract a stream entry that already exists, so the
database must be able to veto EXECUTION. The worker already reads the durable
approval row directly on the resume path (``binding.py`` around the
``SELECT status, summary, agent_id, gate_kind, granted_tool`` for
``approval_grant_tool``), and that is the seam the tombstone lands in.

Contract under test, the sibling of ``approval_grant_tool`` /
``approval_resumed_kind`` / ``approval_decision``:

    ``BindingResolver.approval_resume_cancelled(event_id, agent_id) -> str | None``

Non-None means REFUSE this resume turn, and the value is the structured refusal
detail the worker logs -- it names the cancellation reason and the actor who
cancelled, because #430 and #544 were both filed for authority paths that failed
SILENTLY. None means run the turn normally.

Deliberately NOT agent-bound, unlike ``approval_grant_tool``. The grant's #430
rebind guard withholds AUTHORITY on a mismatch; binding a VETO the same way
would withhold a REFUSAL, so a NULL or rebound ``agent_id`` would become
permission to run a cancelled resume. The queued event names the cancelled
approval directly, so the veto is enforced for that approval whoever is bound
now.

And it is an execution RECORD, not a claim: one UPDATE, vetoed only by the
tombstone, that stamps the delivery lease the execution runs under. It excludes
nothing, so a redelivery after a crash re-records and runs; the API refuses to
cancel any resume that carries a record, and names the lease in that refusal.

A non-approval event id fast-returns with NO database round trip, matching the
existing fast-return at the top of ``approval_grant_tool``.

Integration-style against the REAL compose Postgres, same discipline as
``test_approval_grant.py``: the ``approvals`` row is INSERTed against the same
async engine and schema, never mocked.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_api.resumequeue import resume_event_id
from curie_runner.approval import summarize_tool_call
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")

_REASON = "cancelled during the 0.9.2 upgrade drill"
_ACTOR = "U-OPERATOR-2753"


def _resolver(engine: AsyncEngine) -> BindingResolver:
    return BindingResolver(engine, WorkerConfig(db_schema=_SCHEMA))


async def _seed_agent(engine: AsyncEngine, agent_id: uuid.UUID) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": f"agent-{agent_id.hex[:8]}"},
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_channels (id, agent_id, kind, address) "
                "VALUES (:id, :agent_id, 'slack', :address)"
            ),
            {
                "id": uuid.uuid4(),
                "agent_id": agent_id,
                "address": f"C{agent_id.hex[:8].upper()}",
            },
        )


_UNSET = object()

# A fenced delivery's record arguments, as the kernel passes them.
_LEASE: dict[str, Any] = {"lease_key": "curie:lease:s:g:1-0", "owner": "owner-1", "generation": 1}


async def _seed_approval(
    engine: AsyncEngine,
    *,
    approval_id: uuid.UUID,
    agent_id: uuid.UUID | None,
    status: str = "approved",
    summary: str | None = None,
    resumed_at: Any = _UNSET,
    cancelled: bool = False,
) -> None:
    """One approval row, optionally tombstoned.

    ``resumed_at`` is left explicitly NULL by default: the crash window this
    tombstone exists for is precisely "the entry is on the stream and
    ``resumed_at`` was never written", so the default here IS the interesting
    state rather than an incidental one.
    """
    columns = [
        "id",
        "agent_id",
        "conversation_id",
        "author",
        "summary",
        "reply_kind",
        "reply_channel",
        "reply_placeholder",
        "dedupe_key",
        "status",
        "gate_kind",
        "granted_tool",
    ]
    tool = "mcp__github__create_issue"
    params: dict[str, Any] = {
        "id": approval_id,
        "agent_id": agent_id,
        "conversation_id": f"th-{approval_id.hex[:8]}",
        "author": "U1",
        "summary": summary if summary is not None else summarize_tool_call(tool, {"n": 1}),
        "reply_kind": "slack",
        "reply_channel": "C1",
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
        "status": status,
        "gate_kind": "permission",
        "granted_tool": tool,
    }
    if resumed_at is not _UNSET:
        columns.append("resumed_at")
        params["resumed_at"] = resumed_at
    if cancelled:
        columns += ["resume_cancelled_at", "resume_cancelled_reason", "resume_cancelled_by"]
        params["resume_cancelled_at"] = datetime.now(UTC).replace(tzinfo=None) - timedelta(
            seconds=5
        )
        params["resume_cancelled_reason"] = _REASON
        params["resume_cancelled_by"] = _ACTOR

    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.approvals ({', '.join(columns)}) "
                f"VALUES ({', '.join(':' + c for c in columns)})"
            ),
            params,
        )


async def _cleanup_agents(engine: AsyncEngine, ids: list[uuid.UUID]) -> None:
    async with engine.begin() as conn:
        for agent_id in ids:
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agent_channels WHERE agent_id = :id"),
                {"id": agent_id},
            )
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": agent_id}
            )


async def _skip_if_unreachable(engine: AsyncEngine) -> None:
    try:
        async with engine.connect():
            pass
    except SQLAlchemyError as exc:
        pytest.skip(f"Postgres not reachable at {_DB_URL}: {exc}")


def _cancellation(resolver: BindingResolver):  # noqa: ANN202
    """The method under test, absent until Stream C lands it.

    Resolved by name rather than called directly so the failure this file
    produces before the implementation is a NAMED contract failure rather than
    an AttributeError several frames deep.
    """
    fn = getattr(resolver, "approval_resume_cancelled", None)
    assert fn is not None, (
        "BindingResolver.approval_resume_cancelled is missing: the resume "
        "tombstone is not consulted on the worker's execution side"
    )
    return fn


def test_tombstoned_row_refuses_the_resume_naming_reason_and_actor() -> None:
    # The whole point: an approved, agent-bound, never-resumed approval whose
    # resume was cancelled must refuse execution, and must say WHY and BY WHOM.
    # A bare boolean would recreate #544's silent dead-end.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=agent_id, cancelled=True
            )
            try:
                refusal = await _cancellation(_resolver(engine))(
                    resume_event_id(approval_id), **_LEASE
                )
                assert refusal is not None, "a tombstoned resume must be refused"
                assert _REASON in refusal
                assert _ACTOR in refusal
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_untombstoned_row_is_not_refused() -> None:
    # The negative control for the read itself. Written so a tombstone check
    # that refuses unconditionally -- the cheapest wrong implementation -- fails
    # here rather than passing the file.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=agent_id, cancelled=False
            )
            try:
                resolver = _resolver(engine)
                event = resume_event_id(approval_id)
                assert await _cancellation(resolver)(event, **_LEASE) is None
                # And the row is otherwise healthy, so the existing resume-path
                # reads still answer: the control is "nothing is suppressed",
                # not merely "the tombstone reader said None".
                assert (
                    await resolver.approval_grant_tool(event, agent_id)
                    == "mcp__github__create_issue"
                )
                assert await resolver.approval_decision(event, agent_id) == "approved"
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_tombstone_is_honored_whatever_the_current_binding_owner_is() -> None:
    # A tombstone is a VETO, not a grant, and the direction matters. The grant
    # (#430) is agent-bound because leaking AUTHORITY across a rebind hands one
    # agent another's power. Binding the veto inverts it: a NULL ``agent_id``,
    # or an address rebound to another agent since the cancellation, would turn
    # a missing binding into PERMISSION TO RUN a cancelled resume. The queued
    # event names the cancelled approval directly, so the veto is enforced for
    # that approval whoever the currently resolved agent happens to be.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            owner_id = uuid.uuid4()
            other_id = uuid.uuid4()
            bound_approval = uuid.uuid4()
            unbound_approval = uuid.uuid4()
            await _seed_agent(engine, owner_id)
            await _seed_agent(engine, other_id)
            await _seed_approval(
                engine, approval_id=bound_approval, agent_id=owner_id, cancelled=True
            )
            await _seed_approval(
                engine, approval_id=unbound_approval, agent_id=None, cancelled=True
            )
            try:
                refuse = _cancellation(_resolver(engine))
                # Rebound to another agent: still refused.
                rebound = await refuse(resume_event_id(bound_approval), **_LEASE)
                assert rebound is not None, (
                    "a rebind must not convert a cancellation into permission to run"
                )
                assert _REASON in rebound
                # No agent on the row at all: still refused.
                orphaned = await refuse(resume_event_id(unbound_approval), **_LEASE)
                assert orphaned is not None, (
                    "a NULL agent_id must not convert a cancellation into permission"
                )
                assert _ACTOR in orphaned
            finally:
                await _cleanup_agents(engine, [owner_id, other_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_the_grant_stays_agent_bound_while_the_veto_does_not() -> None:
    # The negative control that keeps the inversion honest: widening the veto
    # must not have widened the GRANT. #430's rebind guard is unchanged.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            owner_id = uuid.uuid4()
            other_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, owner_id)
            await _seed_agent(engine, other_id)
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=owner_id, cancelled=False
            )
            try:
                resolver = _resolver(engine)
                event = resume_event_id(approval_id)
                assert await resolver.approval_grant_tool(event, other_id) is None
                assert (
                    await resolver.approval_grant_tool(event, owner_id)
                    == "mcp__github__create_issue"
                )
            finally:
                await _cleanup_agents(engine, [owner_id, other_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


async def _record_row(engine: AsyncEngine, approval_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT resume_executing_at, resume_executing_lease_key, "
                    "resume_executing_owner, resume_executing_generation "
                    f"FROM {_SCHEMA}.approvals WHERE id = :id"
                ),
                {"id": approval_id},
            )
        ).mappings().one()
    return dict(row)


def test_a_redelivery_after_a_crash_re_records_and_runs() -> None:
    # The regression for the lost continuation. Delivery 1 records execution
    # under its lease and the worker dies; lease recovery redelivers the entry
    # under generation 2 well inside any elapsed-time window. The redelivery
    # must RE-RECORD and run. The earlier exclusivity claim refused it as a
    # duplicate, the kernel done-marked it, and the owed resume was lost.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(engine, approval_id=approval_id, agent_id=agent_id)
            try:
                record = _cancellation(_resolver(engine))
                event = resume_event_id(approval_id)
                assert (
                    await record(event, lease_key="k:1-0", owner="dead-owner", generation=1)
                    is None
                )
                first = await _record_row(engine, approval_id)
                assert first["resume_executing_owner"] == "dead-owner"
                # Redelivered immediately: no staleness window may stand between
                # a crashed holder and its legitimate retry.
                assert (
                    await record(event, lease_key="k:1-0", owner="new-owner", generation=2)
                    is None
                ), "a redelivery after a crash was refused; the owed resume is lost"
                second = await _record_row(engine, approval_id)
                assert second["resume_executing_owner"] == "new-owner"
                assert second["resume_executing_generation"] == 2
                assert second["resume_executing_lease_key"] == "k:1-0"
                assert second["resume_executing_at"] >= first["resume_executing_at"]
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_an_unfenced_delivery_records_a_null_lease() -> None:
    # The unfenced sentinel has no lease to name. The record still lands (so
    # the API can see an execution happened) with a NULL key, which the API
    # refuses to cancel like any other recorded execution.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(engine, approval_id=approval_id, agent_id=agent_id)
            try:
                refusal = await _cancellation(_resolver(engine))(
                    resume_event_id(approval_id), lease_key=None, owner=None, generation=None
                )
                assert refusal is None
                row = await _record_row(engine, approval_id)
                assert row["resume_executing_at"] is not None
                assert row["resume_executing_lease_key"] is None
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_a_tombstoned_row_is_refused_and_records_nothing() -> None:
    # Only a tombstone vetoes, and the veto writes no execution record: a
    # record on a cancelled row would claim an execution that never happened.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=agent_id, cancelled=True
            )
            try:
                refusal = await _cancellation(_resolver(engine))(
                    resume_event_id(approval_id), lease_key="k", owner="o", generation=1
                )
                assert refusal is not None and _REASON in refusal
                row = await _record_row(engine, approval_id)
                assert row["resume_executing_at"] is None
                assert row["resume_executing_owner"] is None
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_non_approval_event_id_fast_returns_without_a_db_round_trip() -> None:
    # Same fast-return guarantee the grant and the decision already make: an
    # ordinary turn pays nothing for the tombstone. Proven by handing the
    # resolver an engine that CANNOT connect -- if the read is attempted, this
    # raises instead of returning None.
    async def go() -> None:
        bad_engine = create_async_engine(
            "postgresql+asyncpg://invalid:invalid@127.0.0.1:1/none"
        )
        try:
            resolver = BindingResolver(bad_engine, WorkerConfig(db_schema=_SCHEMA))
            assert (
                await _cancellation(resolver)("ev-slack-1699999999.123456", **_LEASE)
                is None
            )
        finally:
            await bad_engine.dispose()

    asyncio.run(go())


def test_tombstoned_row_grants_nothing_and_reports_no_decision() -> None:
    # Edit blocks C1 and C2 together. The one-shot post-approval allowance
    # (#430) must not be minted for a cancelled resume, and the second direct
    # SELECT behind approval_decision must not report a decision the execution
    # path refused to act on -- otherwise the decision path acts on a row the
    # execution path vetoed.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=agent_id, cancelled=True
            )
            try:
                resolver = _resolver(engine)
                event = resume_event_id(approval_id)
                assert await resolver.approval_grant_tool(event, agent_id) is None
                assert await resolver.approval_decision(event, agent_id) is None
                assert await resolver.approval_resumed_kind(event, agent_id) is None
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_repeated_reads_of_a_tombstoned_row_are_stable() -> None:
    # Redelivery idempotency at the read layer: the tombstone is a fact about
    # the past, so reading it twice answers the same thing twice and consumes
    # nothing. A one-shot latch here would let the SECOND delivery of the same
    # entry execute.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            approval_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(
                engine, approval_id=approval_id, agent_id=agent_id, cancelled=True
            )
            try:
                refuse = _cancellation(_resolver(engine))
                event = resume_event_id(approval_id)
                first = await refuse(event, **_LEASE)
                second = await refuse(event, **_LEASE)
                assert first is not None
                assert first == second
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_tombstone_refuses_regardless_of_resumed_at() -> None:
    # The guarantee stated in the plan, without overclaiming: once
    # resume_cancelled_at is committed, NO resume turn for that approval
    # executes, regardless of how many stream entries exist or when they were
    # appended. resumed_at NULL is the crash window (enqueued, never marked);
    # resumed_at already set is the reconciler/dead-letter re-open shape. Both
    # refuse, so the tombstone is not quietly reduced to a resumed_at CAS.
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            await _skip_if_unreachable(engine)
            agent_id = uuid.uuid4()
            crash_window_id = uuid.uuid4()
            already_marked_id = uuid.uuid4()
            await _seed_agent(engine, agent_id)
            await _seed_approval(
                engine, approval_id=crash_window_id, agent_id=agent_id, cancelled=True
            )
            await _seed_approval(
                engine,
                approval_id=already_marked_id,
                agent_id=agent_id,
                cancelled=True,
                resumed_at=datetime.now(UTC).replace(tzinfo=None),
            )
            try:
                refuse = _cancellation(_resolver(engine))
                assert await refuse(resume_event_id(crash_window_id), **_LEASE) is not None
                assert await refuse(resume_event_id(already_marked_id), **_LEASE) is not None
            finally:
                await _cleanup_agents(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())
