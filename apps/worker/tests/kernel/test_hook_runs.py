"""Scheduled hook run outcomes recorded by the worker kernel."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest
from aci_protocol import (
    ErrorEvent,
    Final,
    HookRunRef,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.binding import BindingResolver
from curie_worker.delivery_lease import DeliveryBudget, DeliveryLeaseStore
from curie_worker.hook_runs import HookRunRecorderError
from curie_worker.sandbox import QuotaRejection
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


def _event(
    *,
    source: TurnSource = TurnSource.CRON,
    hook_run: HookRunRef | None,
    event_id: str | None = None,
    conversation_id: str | None = None,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=conversation_id or f"thread-{uuid.uuid4().hex}",
        author="U1",
        text="run the scheduled hook",
        reply_handle=ReplyHandle(
            kind="slack",
            channel="C1",
            placeholder=None,
        ),
        received_at="2026-09-22T03:00:00+00:00",
        source=source,
        hook_run=hook_run,
    )


@asynccontextmanager
async def _reject_hook_run_outcome(
    engine: AsyncEngine,
    run_id: uuid.UUID,
    outcome: Literal["ran", "failed"],
) -> AsyncIterator[None]:
    token = uuid.uuid4().hex
    function_name = f"test_fail_hook_run_{token}"
    trigger_name = f"test_fail_hook_run_{token}"
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"CREATE FUNCTION curie.{function_name}() RETURNS trigger "
                "LANGUAGE plpgsql AS $$ BEGIN "
                f"RAISE EXCEPTION 'injected {outcome} hook run update failure'; "
                "END; $$"
            )
        )
        await conn.execute(
            text(
                f"CREATE TRIGGER {trigger_name} BEFORE UPDATE "
                "ON curie.hook_runs FOR EACH ROW "
                f"WHEN (OLD.id = '{run_id}'::uuid AND NEW.outcome = '{outcome}') "
                f"EXECUTE FUNCTION curie.{function_name}()"
            )
        )
    try:
        yield
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    f"DROP TRIGGER IF EXISTS {trigger_name} "
                    "ON curie.hook_runs"
                )
            )
            await conn.execute(
                text(f"DROP FUNCTION IF EXISTS curie.{function_name}()")
            )


async def _wait_for_blocked_hook_run_update(
    engine: AsyncEngine,
    blocker_pid: int,
) -> None:
    deadline = time.monotonic() + 2.0
    async with engine.connect() as conn:
        while time.monotonic() < deadline:
            # PostgreSQL 16 section 28.2.2: activity snapshots persist for a
            # transaction. https://www.postgresql.org/docs/16/monitoring-stats.html
            await conn.execute(text("SELECT pg_stat_clear_snapshot()"))
            blocked = (
                await conn.execute(
                    text(
                        "SELECT EXISTS ("
                        "SELECT 1 FROM pg_stat_activity "
                        "WHERE :blocker_pid = ANY(pg_blocking_pids(pid)) "
                        "AND query LIKE 'UPDATE curie.hook_runs%')"
                    ),
                    {"blocker_pid": blocker_pid},
                )
            ).scalar_one()
            if blocked:
                return
            await asyncio.sleep(0.01)
    raise AssertionError("hook run update did not wait for the row lock")


def _arm_started_frame(kernel: object) -> asyncio.Event:
    """Latch once the kernel applies the cron start delta.

    A cron turn does not post that partial, so the sink is not the signal that
    the stream has begun. The latch fires when ``_apply_frame`` takes the first
    text delta, which is the same moment the old partial edit was sent.
    """
    applied = asyncio.Event()
    original = kernel._apply_frame  # type: ignore[attr-defined]

    async def spy(
        frame: object,
        acc: object,
        reply: object,
        qevent: object,
        agent_id: object = None,
    ) -> None:
        await original(frame, acc, reply, qevent, agent_id)
        if isinstance(frame, TextDelta) and frame.text == "started":
            applied.set()

    kernel._apply_frame = spy  # type: ignore[attr-defined]
    return applied


@pytest.mark.parametrize(
    "status",
    [SessionStatus.DONE, SessionStatus.IDLE_AWAITING_INPUT],
)
def test_completed_cron_turn_closes_the_run_as_ran(
    make_harness,
    make_hook_run,
    status: SessionStatus,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            h.runner.default_script = [Final(text="complete", status=status)]

            event = _event(hook_run=run.ref)
            await h.kernel.process_event(event)

            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "ran"
            assert ended_at is not None
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_cron_turn_on_a_thread_with_a_live_session_records_deferred(
    make_harness,
    make_hook_run,
) -> None:
    """ADR-0099 Concurrency and idle (#2929): a cron fire whose thread holds a
    live interactive session neither steers it nor opens a second turn. The
    kernel's busy read runs under the per-thread lock, and the run is closed
    ``deferred`` so the scheduler, not stream reclaim, owns the retry."""

    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            conversation = f"thread-{uuid.uuid4().hex}"
            live = _event(
                source=TurnSource.SLACK, hook_run=None, conversation_id=conversation
            )
            first = asyncio.create_task(h.kernel.process_event(live))
            try:
                async with asyncio.timeout(5):
                    while not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                cron = _event(hook_run=run.ref, conversation_id=conversation)
                await h.kernel.process_event(cron)

                outcome, ended_at = await run.state() or (None, None)
                assert outcome == "deferred"
                assert ended_at is not None
                assert h.runner.steers == [], "a cron fire steered the live session"
                assert h.runner.opened == [live.text], "a cron fire opened a second turn"
                assert await h.async_redis.exists(h.config.done_key(cron.event_id))
            finally:
                hold.set()
                await asyncio.gather(first, return_exceptions=True)

    asyncio.run(go())


def test_cron_retry_past_its_catch_up_bound_records_skipped(
    make_harness,
    make_hook_run,
) -> None:
    """#2929 review: a deferred slot's retry that sat in the stream past its
    catch-up expiry is recorded ``skipped`` and never opens a turn, even on an
    idle thread."""
    from curie_worker.hook_runs import retry_event_id

    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            expired = datetime.now(UTC) - timedelta(seconds=1)
            event = _event(
                hook_run=run.ref,
                event_id=retry_event_id(f"cron:{run.ref.agent_id}:{run.ref.name}", expired),
            )
            await h.kernel.process_event(event)

            outcome, _ended_at = await run.state() or (None, None)
            assert outcome == "skipped"
            assert h.runner.opened == []

    asyncio.run(go())


def test_queued_cron_fire_is_rejected_after_operator_pause(
    make_harness,
    make_hook_run,
) -> None:
    """A fire already in the stream must not start after pause commits."""

    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            async with run.engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.schedule_controls "
                        "(agent_id, name, paused_at) VALUES (:agent_id, :name, now())"
                    ),
                    {"agent_id": run.agent_id, "name": run.ref.name},
                )
            await h.kernel.process_event(_event(hook_run=run.ref))

            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "skipped"
            assert ended_at is not None
            assert h.runner.opened == []

    asyncio.run(go())


def test_cron_retry_whose_bound_runs_out_during_the_claim_records_skipped(
    make_harness,
    make_hook_run,
) -> None:
    """#2929 review round 2: a retry that passes the entry check with little of
    its bound left must not start once a slow claim has used it up. The expiry
    is read again at the busy check, after the claim."""
    from curie_worker.hook_runs import retry_event_id

    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            original = h.kernel._claim_or_resume

            async def slow_claim(*args: object, **kwargs: object) -> object:
                await asyncio.sleep(3)
                return await original(*args, **kwargs)

            h.kernel._claim_or_resume = slow_claim
            # Whole seconds on the wire: at least one second of bound remains
            # at entry, and none after the 3 s claim.
            soon = datetime.now(UTC) + timedelta(seconds=2)
            event = _event(
                hook_run=run.ref,
                event_id=retry_event_id(f"cron:{run.ref.agent_id}:{run.ref.name}", soon),
            )
            await h.kernel.process_event(event)

            outcome, _ended_at = await run.state() or (None, None)
            assert outcome == "skipped"
            assert h.runner.opened == []

    asyncio.run(go())


def test_cron_retry_inside_its_catch_up_bound_runs(
    make_harness,
    make_hook_run,
) -> None:
    """The negative control: an unexpired retry runs and closes ``ran``."""
    from curie_worker.hook_runs import retry_event_id

    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            h.runner.default_script = [Final(text="done", status=SessionStatus.DONE)]
            later = datetime.now(UTC) + timedelta(minutes=5)
            event = _event(
                hook_run=run.ref,
                event_id=retry_event_id(f"cron:{run.ref.agent_id}:{run.ref.name}", later),
            )
            await h.kernel.process_event(event)

            outcome, _ended_at = await run.state() or (None, None)
            assert outcome == "ran"

    asyncio.run(go())


def test_classified_cron_failure_closes_failed_without_retry(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder(), max_attempts=3
        ) as h:
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="retryable", classification="runner-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ],
                [Final(text="must not run", status=SessionStatus.DONE)],
            ]

            await h.kernel.process_event(_event(hook_run=run.ref))

            assert h.runner.opened == ["run the scheduled hook"]
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None

    asyncio.run(go())


def test_prior_side_effect_recovery_closes_failed_without_running(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            event = _event(hook_run=run.ref)
            await h.async_redis.set(h.config.side_effect_key(event.event_id), "1")

            await h.kernel.process_event(event)

            assert h.runner.opened == []
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_exception_after_cron_start_closes_failed_and_propagates(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            h.runner.default_script = [Final(text="complete", status=SessionStatus.DONE)]
            h.sink.fail_events.add("reply.update")
            event = _event(hook_run=run.ref)

            with pytest.raises(
                RuntimeError, match="injected reply.update delivery failure"
            ):
                await h.kernel.process_event(event)

            assert h.runner.opened == ["run the scheduled hook"]
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_original_exception_survives_failed_failure_close(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            h.runner.default_script = [Final(text="complete", status=SessionStatus.DONE)]
            h.sink.fail_events.add("reply.update")
            event = _event(hook_run=run.ref)

            async with _reject_hook_run_outcome(run.engine, run.run_id, "failed"):
                with pytest.raises(
                    RuntimeError, match="injected reply.update delivery failure"
                ):
                    await h.kernel.process_event(event)

            assert h.runner.opened == ["run the scheduled hook"]
            assert await run.state() == (None, None)
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_delivery_deadline_after_cron_start_closes_failed(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder(),
            delivery_budget_s=60.0,
            runner_total_timeout_s=30.0,
        ) as h:
            await h.async_redis.xgroup_create(
                h.config.stream,
                h.config.consumer_group,
                id="0",
                mkstream=True,
            )
            entry_id = await h.async_redis.xadd(h.config.stream, {"payload": "owned"})
            await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            store = DeliveryLeaseStore(h.async_redis, h.config)
            lease = await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="started")]
            event = _event(hook_run=run.ref)
            started = _arm_started_frame(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(event, lease=lease))
            await asyncio.wait_for(started.wait(), timeout=2.0)

            seconds, microseconds = await h.async_redis.time()
            now_ms = int(seconds) * 1000 + int(microseconds) // 1000
            deadline_ms = now_ms + 100
            await h.async_redis.hset(
                h.config.delivery_state_key(
                    h.config.stream, h.config.consumer_group, entry_id
                ),
                mapping={"deadline_ms": str(deadline_ms)},
            )
            lease.budget = DeliveryBudget(
                deadline_ms=deadline_ms,
                anchor_server_ms=now_ms,
                anchor_monotonic=time.monotonic(),
            )
            await asyncio.sleep(0.2)
            hold.set()
            await task

            assert h.runner.opened == ["run the scheduled hook"]
            assert h.sink.last_text is not None
            assert "delivery deadline" in h.sink.last_text.lower()
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None

    asyncio.run(go())


@pytest.mark.parametrize("source", [TurnSource.SLACK, TurnSource.WEBHOOK])
def test_noncron_turn_never_writes_a_supplied_hook_run(
    make_harness,
    make_hook_run,
    source: TurnSource,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            h.runner.default_script = [
                Final(text="complete", status=SessionStatus.DONE)
            ]

            await h.kernel.process_event(_event(source=source, hook_run=run.ref))

            assert await run.state() == (None, None)

    asyncio.run(go())


def test_cron_without_a_run_ref_refuses_before_runner_start(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            await h.kernel.process_event(_event(hook_run=None))

            assert h.runner.opened == []
            assert h.sink.text_posts == []
            assert await run.state() == (None, None)

    asyncio.run(go())


def test_cron_with_no_matching_row_refuses_before_runner_start(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            missing = HookRunRef(
                agent_id=str(run.agent_id),
                name="missing_hook",
                slot_utc=run.ref.slot_utc,
            )

            await h.kernel.process_event(_event(hook_run=missing))

            assert h.runner.opened == []
            assert h.sink.text_posts == []
            assert await run.state() == (None, None)

    asyncio.run(go())


def test_terminal_hook_run_redelivery_never_starts_or_overwrites(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run(outcome="failed") as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            before = await run.state()

            await h.kernel.process_event(_event(hook_run=run.ref))

            assert h.runner.opened == []
            assert await run.state() == before

    asyncio.run(go())


def test_hook_run_database_failure_precedes_done_marker(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            event = _event(hook_run=run.ref)
            async with _reject_hook_run_outcome(run.engine, run.run_id, "ran"):
                with pytest.raises(HookRunRecorderError):
                    await h.kernel.process_event(event)

                assert await run.state() == (None, None)
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


@pytest.mark.parametrize("invalid_part", ["agent_id", "slot_utc"])
def test_malformed_cron_ref_refuses_before_runner_start(
    make_harness,
    make_hook_run,
    invalid_part: str,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            invalid = HookRunRef(
                agent_id=(
                    "not-a-uuid" if invalid_part == "agent_id" else run.ref.agent_id
                ),
                name=run.ref.name,
                slot_utc=(
                    "2026-09-22T04:00:00+01:00"
                    if invalid_part == "slot_utc"
                    else run.ref.slot_utc
                ),
            )

            await h.kernel.process_event(_event(hook_run=invalid))

            assert h.runner.opened == []
            assert await run.state() == (None, None)

    asyncio.run(go())


def test_quota_refusal_before_cron_start_leaves_run_open(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder(), claim_timeout_seconds=0.05
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"limits.cpu": "2"},
                used={"limits.cpu": "7"},
                hard={"limits.cpu": "8"},
            )

            await h.kernel.process_event(_event(hook_run=run.ref))

            assert h.runner.opened == []
            assert await run.state() == (None, None)

    asyncio.run(go())


def test_approval_pause_closes_cron_run_as_ran(
    make_harness,
    make_hook_run,
) -> None:
    class RecordingApprovals:
        def __init__(self) -> None:
            self.requests: list[ApprovalRequest] = []

        async def create(self, request: ApprovalRequest) -> CreatedApproval:
            self.requests.append(request)
            return CreatedApproval(id="appr-hook-run", status="pending")

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder(), approvals=approvals
        ) as h:
            h.runner.default_script = [
                Final(
                    text="Requesting sign off",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Approve the scheduled action",
                )
            ]

            await h.kernel.process_event(_event(hook_run=run.ref))

            assert len(approvals.requests) == 1
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "ran"
            assert ended_at is not None

    asyncio.run(go())


def test_cancellation_after_cron_start_closes_failed_and_propagates(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="started")]
            event = _event(hook_run=run.ref)
            started = _arm_started_frame(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(event))
            try:
                await asyncio.wait_for(started.wait(), timeout=2.0)

                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                outcome, ended_at = await run.state() or (None, None)
                assert outcome == "failed"
                assert ended_at is not None
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            finally:
                hold.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())


def test_cancellation_survives_second_cancel_and_failed_failure_close(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="started")]
            event = _event(hook_run=run.ref)
            started = _arm_started_frame(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(event))
            try:
                await asyncio.wait_for(started.wait(), timeout=2.0)

                async with _reject_hook_run_outcome(
                    run.engine, run.run_id, "failed"
                ):
                    lock_connection = await run.engine.connect()
                    lock_transaction = await lock_connection.begin()
                    try:
                        blocker_pid = (
                            await lock_connection.execute(text("SELECT pg_backend_pid()"))
                        ).scalar_one()
                        await lock_connection.execute(
                            text(
                                "SELECT id FROM curie.hook_runs "
                                "WHERE id = :run_id FOR UPDATE"
                            ),
                            {"run_id": run.run_id},
                        )

                        task.cancel()
                        await _wait_for_blocked_hook_run_update(
                            run.engine, blocker_pid
                        )
                        assert task.cancel()
                        await asyncio.sleep(0)
                        assert not task.done()

                        await lock_transaction.rollback()
                        with pytest.raises(asyncio.CancelledError):
                            await task
                    finally:
                        if lock_transaction.is_active:
                            await lock_transaction.rollback()
                        await lock_connection.close()

                assert await run.state() == (None, None)
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            finally:
                hold.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())


def test_lost_lease_after_cron_start_leaves_run_open(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder()
        ) as h:
            await h.async_redis.xgroup_create(
                h.config.stream,
                h.config.consumer_group,
                id="0",
                mkstream=True,
            )
            entry_id = await h.async_redis.xadd(h.config.stream, {"payload": "owned"})
            await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            store = DeliveryLeaseStore(h.async_redis, h.config)
            lease = await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="started")]
            h.runner.tail = [Final(text="complete", status=SessionStatus.DONE)]
            event = _event(hook_run=run.ref)
            started = _arm_started_frame(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(event, lease=lease))
            try:
                await asyncio.wait_for(started.wait(), timeout=2.0)

                await store.release(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    owner=lease.owner,
                )
                lease.lost.set()
                hold.set()
                await task

                assert await run.state() == (None, None)
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            finally:
                hold.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())


def test_bound_agent_mismatch_refuses_cron_before_runner_start(
    make_harness,
    make_hook_run,
) -> None:
    async def go() -> None:
        async with make_hook_run() as run:
            agent_id = uuid.uuid4()
            version_id = uuid.uuid4()
            deployment_id = uuid.uuid4()
            channel_id = uuid.uuid4()
            async with run.engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.agents (id, name) "
                        "VALUES (:id, :name)"
                    ),
                    {"id": agent_id, "name": f"other_agent_{uuid.uuid4().hex}"},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_versions "
                        "(id, agent_id, version_label, created_by) "
                        "VALUES (:id, :agent_id, 'mismatch', 'kernel-test')"
                    ),
                    {"id": version_id, "agent_id": agent_id},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) "
                        "VALUES (:id, :agent_id, :version_id, "
                        "CAST('prod' AS curie.environment), 'active')"
                    ),
                    {
                        "id": deployment_id,
                        "agent_id": agent_id,
                        "version_id": version_id,
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_channels "
                        "(id, agent_id, kind, address) "
                        "VALUES (:id, :agent_id, 'slack', 'C1')"
                    ),
                    {"id": channel_id, "agent_id": agent_id},
                )
            try:
                async with make_harness(
                    hook_runs=run.recorder(),
                    binding_factory=lambda config: BindingResolver(
                        run.engine, config
                    ),
                ) as h:
                    await h.kernel.process_event(_event(hook_run=run.ref))

                    assert h.runner.opened == []
                    assert await run.state() == (None, None)
            finally:
                async with run.engine.begin() as conn:
                    await conn.execute(
                        text("DELETE FROM curie.agents WHERE id = :id"),
                        {"id": agent_id},
                    )

    asyncio.run(go())
