"""A long scheduled sweep continues on one sandbox, or reports what it missed.

ADR-0160 (as amended by ADR-0188 decision 3), issue #2878. Everything runs on
the kernel harness: real Valkey (runs stream, delivery leases, markers), real
Postgres (``hook_runs``), the real substrate over the fake Kubernetes client,
the in-process fake runner, the recording sink, and a fake of the API memory
routes. Outcomes are read where a user would see them: the runs stream, the
hook run row, the sink, claim creations on the fake Kubernetes client, and
the requests the fake runner received.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from aci_protocol import ErrorEvent, Final, SessionStatus, TextDelta
from curie_worker.binding import BindingResolver
from curie_worker.cron_loop import CronPassSummary, CronSchedulerLoop, _Target
from curie_worker.hook_runs import HookRunRecorderError, retry_event_id
from curie_worker.kernel import routing
from curie_worker.killswitch import kill_key
from curie_worker.sandbox.types import RouteRecord
from sqlalchemy import text

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from sweep_fixtures import (  # noqa: E402
    PROMPT,
    SweepCase,
    arm_started,
    bump_fence,
    cron_event,
    enqueue,
    force_deadline,
    notices,
    now,
    pause_hook,
    read_next,
    redeliver,
    reply_texts,
    run_outcome,
    run_slice_to_budget_cut,
    set_outcome,
    successors,
    sweep_case,
)

DONE = SessionStatus.DONE
_SLOT = datetime(2026, 9, 22, 3, 0, tzinfo=UTC)
_FINAL = "Plan for 2026-09-21: ship the notes cleanup. Uncovered: none"


def _base(c: SweepCase) -> str:
    ref = c.run.ref
    return f"cron:{ref.agent_id}:{ref.name}:{ref.slot_utc}"


def _uncovered_notice(c: SweepCase, outcome: str, uncovered: str = "github, notes") -> str:
    return (
        f'Scheduled sweep "{c.run.ref.name}" for 2026-09-21 stopped before it finished '
        f"(run outcome: {outcome}). Not covered: {uncovered}."
    )


async def _first_slice_cut(c: SweepCase, **kwargs: Any) -> Any:
    """Slice one: the agent saves a checkpoint, then the budget cuts it."""
    first = await enqueue(c.h, cron_event(c.run))

    async def save() -> None:
        c.checkpoint(covered=("slack",), uncovered=("github", "notes"))

    await run_slice_to_budget_cut(c.h, first, at_cut=save, **kwargs)
    return first


async def _next_continuation(c: SweepCase) -> Any:
    delivery = await read_next(c.h)
    assert delivery is not None, "no continuation was queued"
    assert ":sweep:" in delivery.event.event_id
    return delivery


def _final_slice(c: SweepCase) -> None:
    c.h.runner.hold = None
    c.h.runner.tail = []
    c.h.runner.turn_scripts = [[Final(text=_FINAL, status=DONE)]]


def _claim_spy(c: SweepCase) -> list[object]:
    calls: list[object] = []
    original = c.h.kernel._claim_or_resume

    async def spy(*args: object, **kwargs: object) -> object:
        calls.append(args)
        return await original(*args, **kwargs)

    c.h.kernel._claim_or_resume = spy
    return calls


async def _lookup(c: SweepCase, event: Any) -> Any:
    return await asyncio.to_thread(c.h.substrate.lookup, routing._thread_key_for(event))


# --- AC1: the sweep continues on the same sandbox --------------------------------


def test_budget_cut_with_checkpoint_queues_a_continuation_and_keeps_the_run_open(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h, run = c.h, c.run
            first = await enqueue(h, cron_event(run))
            before: dict[str, Any] = {}

            async def save_then_cut() -> None:
                c.checkpoint(covered=("slack",), uncovered=("github", "notes"))
                before["lease"] = await run.lease_expires_at()
                before["at"] = now()

            await run_slice_to_budget_cut(h, first, at_cut=save_then_cut)

            found = await successors(h)
            assert [s.event_id for s in found] == [f"{first.event.event_id}:sweep:1:1:0"]
            successor = found[0]
            assert successor.conversation_id == first.event.conversation_id
            assert successor.author == first.event.author == f"cron:{run.ref.name}"
            assert successor.text == first.event.text == PROMPT
            assert successor.source is first.event.source
            assert successor.reply_handle == first.event.reply_handle
            assert successor.hook_run == first.event.hook_run
            assert datetime.fromisoformat(successor.received_at) > datetime.fromisoformat(
                first.event.received_at
            )

            assert await run_outcome(run) is None
            lease = await run.lease_expires_at()
            assert lease is not None and before["lease"] is not None
            assert lease >= before["lease"]
            assert lease >= before["at"] + timedelta(
                seconds=h.config.effective_hook_claim_lease_s - 5
            )

            # The slice posts nothing: no partial, no escalation, no notice.
            assert h.sink.text_posts == []
            assert reply_texts(h) == []
            assert await h.async_redis.exists(h.config.done_key(first.event.event_id))
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.runner.opened == [PROMPT]
            assert h.runner.timeout_calls == 1

    asyncio.run(go())


def test_continuation_runs_on_the_same_sandbox_and_a_final_slice_closes_ran(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            first = await _first_slice_cut(c)
            route_before = await _lookup(c, first.event)
            assert route_before is not None

            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            _final_slice(c)
            second = await _next_continuation(c)
            assert second.event.event_id == f"{first.event.event_id}:sweep:1:1:0"
            await h.kernel.process_event(second.event, lease=second.lease)

            assert h.runner.opened == [PROMPT, PROMPT]
            assert len(h.fake_k8s.claim_envs) == 1, "the continuation created a claim"
            assert h.fake_k8s.deleted_claims == []
            route_after = await _lookup(c, second.event)
            assert route_after is not None
            assert route_after.claim_name == route_before.claim_name
            assert await run_outcome(c.run) == "ran"
            assert _FINAL in reply_texts(h)
            assert notices(h) == []
            assert len(await successors(h)) == 1
            assert await h.async_redis.exists(h.config.done_key(second.event.event_id))

    asyncio.run(go())


def test_budget_cut_without_a_checkpoint_keeps_the_deadline_escalation(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await enqueue(c.h, cron_event(c.run))
            await run_slice_to_budget_cut(c.h, first)

            assert await successors(c.h) == []
            assert await run_outcome(c.run) == "failed"
            assert any("delivery deadline" in t.lower() for t in reply_texts(c.h))
            assert notices(c.h) == []
            # The service was consulted and found nothing.
            assert c.api.requests

    asyncio.run(go())


def test_budget_cut_with_unconfirmed_timeout_and_a_checkpoint_continues(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            c.h.runner.timeout_status = 500
            first = await _first_slice_cut(c)

            assert c.h.runner.timeout_calls == 1
            assert [s.event_id for s in await successors(c.h)] == [
                f"{first.event.event_id}:sweep:1:1:0"
            ]
            assert await run_outcome(c.run) is None
            assert reply_texts(c.h) == []

    asyncio.run(go())


def test_budget_cut_after_a_side_effect_continues_with_a_checkpoint(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c, side_effect=True)

            assert await c.h.async_redis.exists(c.h.config.side_effect_key(first.event.event_id))
            assert [s.event_id for s in await successors(c.h)] == [
                f"{first.event.event_id}:sweep:1:1:0"
            ]
            assert await run_outcome(c.run) is None
            assert reply_texts(c.h) == [], "a budget cut with progress escalated"

    asyncio.run(go())


def test_side_effect_failure_with_budget_left_still_halts(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            c.checkpoint()
            first = await enqueue(h, cron_event(c.run))
            from aci_protocol import SideEffectFlag

            h.runner.turn_scripts = [
                [
                    SideEffectFlag(tool="Bash"),
                    ErrorEvent(message="sandbox exec failed", classification="runner-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ]
            ]
            await h.kernel.process_event(first.event, lease=first.lease)

            texts = reply_texts(h)
            assert any("after starting an action" in t for t in texts), texts
            assert await run_outcome(c.run) == "failed"
            assert await successors(h) == []
            assert [n.text for n in notices(h)] == [_uncovered_notice(c, "failed")]
            assert h.runner.opened == [PROMPT]

    asyncio.run(go())


def test_continuation_without_progress_stops_failed_with_a_notice(
    make_hook_run, make_harness
) -> None:
    """Stall bound (review round 1, C): a slice that finishes no new source still
    continues, counting the stall in its id; the third stall in a row stops."""

    async def go() -> None:
        from curie_worker.sweep import MAX_STALLED_SLICES

        assert MAX_STALLED_SLICES == 3
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            base = first.event.event_id
            delivery = await _next_continuation(c)
            assert delivery.event.event_id == f"{base}:sweep:1:1:0"

            # Two stalled slices continue, each carrying its stall count.
            for slice_n, stalled in ((2, 1), (3, 2)):
                await run_slice_to_budget_cut(c.h, delivery)
                assert await run_outcome(c.run) is None
                assert notices(c.h) == []
                delivery = await _next_continuation(c)
                assert delivery.event.event_id == f"{base}:sweep:{slice_n}:1:{stalled}"

            # The third stall in a row stops the sweep and reports.
            await run_slice_to_budget_cut(c.h, delivery)

            assert [s.event_id for s in await successors(c.h)] == [
                f"{base}:sweep:1:1:0",
                f"{base}:sweep:2:1:1",
                f"{base}:sweep:3:1:2",
            ]
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(c.h)] == [_uncovered_notice(c, "failed")]
            assert len(c.h.fake_k8s.claim_envs) == 1
            assert c.h.runner.opened == [PROMPT] * 4

    asyncio.run(go())


def test_repeated_covered_names_are_not_progress(make_hook_run, make_harness) -> None:
    """Progress is the count of DISTINCT covered names, case-insensitively."""

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            second = await _next_continuation(c)

            async def same_source_again() -> None:
                c.checkpoint(covered=("slack", "Slack", "SLACK"), uncovered=("github", "notes"))

            await run_slice_to_budget_cut(c.h, second, at_cut=same_source_again)

            assert [s.event_id for s in await successors(c.h)] == [
                f"{first.event.event_id}:sweep:1:1:0",
                f"{first.event.event_id}:sweep:2:1:1",
            ]
            assert await run_outcome(c.run) is None

    asyncio.run(go())


def test_a_source_spanning_two_slices_continues_and_completes_on_one_claim(
    make_hook_run, make_harness
) -> None:
    """ADR-0160:186-187: a source can outlast one delivery budget."""

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            first = await _first_slice_cut(c)
            base = first.event.event_id

            # Slice two is cut midway through github: nothing new finished.
            second = await _next_continuation(c)
            await run_slice_to_budget_cut(h, second)
            third = await _next_continuation(c)
            assert third.event.event_id == f"{base}:sweep:2:1:1"

            # Slice three finishes github and is cut on notes: the stall resets.
            async def github_done() -> None:
                c.checkpoint(covered=("slack", "github"), uncovered=("notes",))

            await run_slice_to_budget_cut(h, third, at_cut=github_done)
            fourth = await _next_continuation(c)
            assert fourth.event.event_id == f"{base}:sweep:3:2:0"

            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            _final_slice(c)
            await h.kernel.process_event(fourth.event, lease=fourth.lease)

            assert await run_outcome(c.run) == "ran"
            assert h.runner.opened == [PROMPT] * 4
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.fake_k8s.deleted_claims == []
            assert notices(h) == []
            assert _FINAL in reply_texts(h)

    asyncio.run(go())


def test_slice_cap_stops_the_sweep_with_a_notice(make_hook_run, make_harness) -> None:
    async def go() -> None:
        from curie_worker.sweep import MAX_SWEEP_SLICES

        assert MAX_SWEEP_SLICES == 48
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            queued = await _next_continuation(c)
            # The same delivery, as the sweep's last allowed slice.
            last = queued.event.model_copy(
                update={"event_id": f"{first.event.event_id}:sweep:{MAX_SWEEP_SLICES}:1:0"}
            )
            capped = type(queued)(last, queued.entry_id, queued.lease)

            async def progress() -> None:
                c.checkpoint(covered=("slack", "github"), uncovered=("notes",))

            await run_slice_to_budget_cut(c.h, capped, at_cut=progress)

            assert [s.event_id for s in await successors(c.h)] == [queued.event.event_id]
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(c.h)] == [_uncovered_notice(c, "failed", "notes")]

    asyncio.run(go())


def test_budget_cut_whose_renew_raises_keeps_the_deadline_escalation(
    make_hook_run, make_harness
) -> None:
    """Review round 1, E: a renew that raises at the cut is a stop, not a crash."""
    calls: list[int] = []

    def failing_renew_after_start(recorder: Any) -> Any:
        original = recorder.renew

        async def renew(ref: Any, lease_s: float) -> bool:
            calls.append(1)
            if len(calls) > 1:
                raise HookRunRecorderError("hook run lease could not be renewed")
            return await original(ref, lease_s)

        recorder.renew = renew
        return recorder

    async def go() -> None:
        async with sweep_case(
            make_hook_run, make_harness, wrap_recorder=failing_renew_after_start
        ) as c:
            first = await _first_slice_cut(c)

            assert len(calls) >= 2, "the cut never renewed"
            assert await successors(c.h) == []
            assert await run_outcome(c.run) == "failed"
            assert any("delivery deadline" in t.lower() for t in reply_texts(c.h))
            assert await c.h.async_redis.exists(c.h.config.done_key(first.event.event_id))

    asyncio.run(go())


def test_cancelled_delivery_after_the_notice_post_posts_it_once(
    make_hook_run, make_harness
) -> None:
    """Review round 1, G: a cancellation landing after the notice reached the sink
    must not make the error path post the same pending notice again."""

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            c.checkpoint()
            first = await enqueue(h, cron_event(c.run))
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="upstream returned 500", classification="server-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ]
            ]
            original = h.sink.emit
            cancelled: list[bool] = []

            async def emit_then_cancelled(event: Any, **kwargs: Any) -> Any:
                ack = await original(event, **kwargs)
                text_value = getattr(event, "text", None) or ""
                if text_value.startswith("Scheduled sweep ") and not cancelled:
                    cancelled.append(True)
                    raise asyncio.CancelledError
                return ack

            h.sink.emit = emit_then_cancelled
            task = asyncio.create_task(h.kernel.process_event(first.event, lease=first.lease))
            results = await asyncio.gather(task, return_exceptions=True)

            assert cancelled, "the notice was never posted"
            assert isinstance(results[0], asyncio.CancelledError), results
            assert [n.text for n in notices(h)] == [_uncovered_notice(c, "failed")]
            assert await run_outcome(c.run) == "failed"

    asyncio.run(go())


def test_backoff_clamps_a_huge_attempt_number(make_harness) -> None:
    """Review round 1, B: a continuation's busy wait can run past any attempt
    count, so the backoff exponent must not overflow."""

    async def go() -> None:
        async with make_harness() as h:
            assert h.kernel._backoff(5000) == h.config.retry_backoff_max_s
            assert h.kernel._backoff(1) == h.config.retry_backoff_base_s

    asyncio.run(go())


def test_continuation_busy_for_many_probes_still_starts_once_idle(
    make_hook_run, make_harness
) -> None:
    """Liveness for B: well past the attempt count where ``2 ** (attempt - 1)``
    overflows a float, the waiting continuation still starts when the turn ends."""

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            await _first_slice_cut(c)
            h.runner.turn_active = True
            probes_before = len(h.runner.status_headers)

            async def idle_after_many_probes() -> None:
                while len(h.runner.status_headers) - probes_before < 1_100:
                    await asyncio.sleep(0.05)
                h.runner.turn_active = False

            flipper = asyncio.create_task(idle_after_many_probes())
            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            _final_slice(c)
            second = await _next_continuation(c)
            try:
                await asyncio.wait_for(
                    h.kernel.process_event(second.event, lease=second.lease), timeout=50
                )
            finally:
                flipper.cancel()
                await asyncio.gather(flipper, return_exceptions=True)
                h.runner.turn_active = False

            assert len(h.runner.status_headers) - probes_before >= 1_100
            assert h.runner.opened == [PROMPT, PROMPT]
            assert await run_outcome(c.run) == "ran"
            assert notices(h) == []

    asyncio.run(go())


def test_continuation_with_progress_queues_the_next_slice(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            second = await _next_continuation(c)

            async def more() -> None:
                c.checkpoint(covered=("slack", "github"), uncovered=("notes",))

            await run_slice_to_budget_cut(c.h, second, at_cut=more)

            assert [s.event_id for s in await successors(c.h)] == [
                f"{first.event.event_id}:sweep:1:1:0",
                f"{first.event.event_id}:sweep:2:2:0",
            ]
            assert await run_outcome(c.run) is None
            assert notices(c.h) == []
            assert reply_texts(c.h) == []
            assert len(c.h.fake_k8s.claim_envs) == 1
            assert c.h.runner.opened == [PROMPT, PROMPT]

    asyncio.run(go())


def test_budget_cut_with_nothing_uncovered_stops_with_a_notice(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await enqueue(c.h, cron_event(c.run))

            async def all_covered() -> None:
                c.checkpoint(covered=("slack", "github", "notes"), uncovered=())

            await run_slice_to_budget_cut(c.h, first, at_cut=all_covered)

            assert await successors(c.h) == []
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(c.h)] == [
                f'Scheduled sweep "{c.run.ref.name}" for 2026-09-21 stopped before it '
                "finished (run outcome: failed). Not covered: none recorded; the run "
                "stopped before it posted its result."
            ]

    asyncio.run(go())


# --- the hook lease across slices (R1, revision 2 F1) ------------------------------


def test_budget_cut_whose_run_can_no_longer_renew_stops_and_posts_nothing(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await enqueue(c.h, cron_event(c.run))

            async def reclaimed_meanwhile() -> None:
                c.checkpoint()
                await set_outcome(c.run, "reclaimed")

            await run_slice_to_budget_cut(c.h, first, at_cut=reclaimed_meanwhile)

            assert await successors(c.h) == []
            assert await run_outcome(c.run) == "reclaimed"
            assert notices(c.h) == [], "the reclaiming fire owns the record"

    asyncio.run(go())


def test_budget_cut_renews_a_lapsed_hook_lease_before_reading_coverage(
    make_hook_run, make_harness
) -> None:
    order: list[str] = []

    def spy_renew(recorder: Any) -> Any:
        original = recorder.renew

        async def renew(ref: Any, lease_s: float) -> bool:
            order.append("renew")
            return await original(ref, lease_s)

        recorder.renew = renew
        return recorder

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness, wrap_recorder=spy_renew) as c:

            async def log_read(_request: Any) -> None:
                order.append("read")

            c.api.on_request = log_read
            first = await enqueue(c.h, cron_event(c.run))

            async def lapse() -> None:
                c.checkpoint()
                async with c.run.engine.begin() as conn:
                    await conn.execute(
                        text(
                            "UPDATE curie.hook_runs SET lease_expires_at = "
                            "now() - interval '1 second' WHERE id = :id"
                        ),
                        {"id": c.run.run_id},
                    )
                order.append("cut")

            await run_slice_to_budget_cut(c.h, first, at_cut=lapse)

            assert "read" in order, order
            cut = order.index("cut")
            first_read = order.index("read")
            assert "renew" in order[cut:first_read], order
            lease = await c.run.lease_expires_at()
            assert lease is not None and lease > now()
            assert await run_outcome(c.run) is None
            assert len(await successors(c.h)) == 1

    asyncio.run(go())


def test_queued_continuation_finding_its_row_terminal_posts_nothing(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            c.checkpoint()
            await set_outcome(c.run, "reclaimed")
            continuation = await enqueue(c.h, cron_event(c.run, event_id=f"{_base(c)}:sweep:1:1:0"))

            await c.h.kernel.process_event(continuation.event, lease=continuation.lease)

            assert notices(c.h) == []
            assert c.h.fake_k8s.claim_envs == []
            assert c.h.runner.opened == []
            assert await run_outcome(c.run) == "reclaimed"

    asyncio.run(go())


async def _reclaim_pass_after_shift(c: SweepCase, shift_s: float) -> tuple[int, Any]:
    """Hold a cron delivery, age its lease by ``shift_s``, run a real reclaim pass."""
    h, run = c.h, c.run
    first = await enqueue(h, cron_event(run))
    hold = asyncio.Event()
    h.runner.hold = hold
    h.runner.turn_scripts = [[TextDelta(text="started")]]
    h.runner.tail = [Final(text="done", status=DONE)]
    started = arm_started(h.kernel)
    task = asyncio.create_task(h.kernel.process_event(first.event, lease=first.lease))
    try:
        await asyncio.wait_for(started.wait(), timeout=5.0)
        async with run.engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE curie.hook_runs SET lease_expires_at = "
                    "lease_expires_at - make_interval(secs => :shift) WHERE id = :id"
                ),
                {"shift": shift_s, "id": run.run_id},
            )

        async def never_killed(_agent: uuid.UUID) -> bool:
            return False

        loop = CronSchedulerLoop(
            engine=run.engine,
            redis=h.async_redis,
            source=c.triggers,
            is_killed=never_killed,
            db_schema="curie",
            stream=h.config.stream,
            interval_seconds=60.0,
            claim_lease_s=h.config.effective_hook_claim_lease_s,
            default_max_usd_per_day=10.0,
            default_max_output_tokens_per_run=100_000,
        )
        target = _Target(
            agent_id=run.agent_id,
            agent_name="sweep-agent",
            version_id=run.version_id,
            bundle_ref="bundles/sweep.tgz",
            deployed_at=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )
        async with run.engine.begin() as conn:
            reclaimed = await loop._lock_and_reclaim(
                conn, target, run.ref.name, _SLOT + timedelta(days=1), CronPassSummary()
            )
        state = await run.state()
    finally:
        hold.set()
        await asyncio.gather(task, return_exceptions=True)
    return reclaimed, state


def test_delivery_start_lease_survives_a_real_reclaim_pass_inside_the_margin(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        from curie_worker.sweep import HOOK_LEASE_START_MARGIN_S

        async with sweep_case(
            make_hook_run, make_harness, with_sweep=False, runner_total_timeout_s=30.0
        ) as c:
            budget = c.h.config.effective_hook_claim_lease_s
            reclaimed, state = await _reclaim_pass_after_shift(
                c, budget + HOOK_LEASE_START_MARGIN_S - 5.0
            )

            assert reclaimed == 0
            assert state is not None and state[0] is None

    asyncio.run(go())


def test_delivery_start_lease_is_reclaimed_past_the_margin(make_hook_run, make_harness) -> None:
    """Liveness of the test above: the same real pass does reclaim past the margin."""

    async def go() -> None:
        from curie_worker.sweep import HOOK_LEASE_START_MARGIN_S

        async with sweep_case(
            make_hook_run, make_harness, with_sweep=False, runner_total_timeout_s=30.0
        ) as c:
            budget = c.h.config.effective_hook_claim_lease_s
            reclaimed, state = await _reclaim_pass_after_shift(
                c, budget + HOOK_LEASE_START_MARGIN_S + 5.0
            )

            assert reclaimed == 1
            assert state is not None and state[0] == "reclaimed"

    asyncio.run(go())


# --- AC2: a sweep that stops short reports what it did not cover -------------------


def test_continuation_with_its_claim_gone_creates_no_claim_and_posts_a_notice(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            assert await asyncio.to_thread(
                c.h.substrate.release, routing._thread_key_for(first.event)
            )
            second = await _next_continuation(c)

            await c.h.kernel.process_event(second.event, lease=second.lease)

            assert len(c.h.fake_k8s.claim_envs) == 1, "the continuation created a claim"
            assert c.h.runner.opened == [PROMPT]
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(c.h)] == [_uncovered_notice(c, "failed")]
            assert await successors(c.h) == [second.event]

    asyncio.run(go())


def test_continuation_that_would_replace_its_runner_stops_without_claiming(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            thread_key = routing._thread_key_for(first.event)
            affinity = c.h.substrate._affinity
            record = affinity.get(thread_key)
            assert record is not None
            # A runner booted for a different run: the claim path would replace it.
            affinity.replace(
                thread_key,
                RouteRecord(
                    handle=replace(record.handle, caller_run="some-other-run"),
                    state=record.state,
                ),
                60,
            )
            claims = _claim_spy(c)
            second = await _next_continuation(c)

            await c.h.kernel.process_event(second.event, lease=second.lease)

            assert claims == [], "a continuation reached the claim path"
            assert len(c.h.fake_k8s.claim_envs) == 1
            assert c.h.fake_k8s.deleted_claims == []
            assert c.h.runner.opened == [PROMPT]
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(c.h)] == [_uncovered_notice(c, "failed")]

    asyncio.run(go())


def test_continuation_whose_route_moves_during_adopt_stops(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await _first_slice_cut(c)
            route_before = await _lookup(c, first.event)
            assert route_before is not None
            substrate = c.h.substrate
            original_touch = substrate.touch_live
            moved: list[str] = []

            def touch_after_a_move(thread_key: str, claim_name: str) -> bool:
                if not moved:
                    record = substrate._affinity.get(thread_key)
                    assert record is not None
                    substrate._affinity.replace(
                        thread_key,
                        RouteRecord(
                            handle=replace(record.handle, claim_name="claim-moved-elsewhere"),
                            state=record.state,
                        ),
                        60,
                    )
                    moved.append(claim_name)
                return original_touch(thread_key, claim_name)

            substrate.touch_live = touch_after_a_move
            claims = _claim_spy(c)
            second = await _next_continuation(c)

            await c.h.kernel.process_event(second.event, lease=second.lease)

            assert moved == [route_before.claim_name], "the adopt never refreshed its route"
            assert claims == []
            assert len(c.h.fake_k8s.claim_envs) == 1
            assert c.h.runner.opened == [PROMPT]
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(c.h)] == [_uncovered_notice(c, "failed")]

    asyncio.run(go())


def test_failed_sweep_slice_with_a_checkpoint_posts_a_notice(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            c.checkpoint()
            first = await enqueue(h, cron_event(c.run))
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="upstream returned 500", classification="server-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ]
            ]

            await h.kernel.process_event(first.event, lease=first.lease)

            assert await run_outcome(c.run) == "failed"
            texts = reply_texts(h)
            found = notices(h)
            assert [n.text for n in found] == [_uncovered_notice(c, "failed")]
            assert found[0].target.reply_ref is None, "the notice edited another message"
            assert len(texts) == 2 and texts[1] == found[0].text, texts
            # Two separate messages: the escalation and the notice.
            assert len(h.sink.text_posts) == 2

    asyncio.run(go())


def test_failed_cron_turn_without_a_checkpoint_posts_no_notice(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            first = await enqueue(h, cron_event(c.run))
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="upstream returned 500", classification="server-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ]
            ]

            await h.kernel.process_event(first.event, lease=first.lease)

            assert await run_outcome(c.run) == "failed"
            assert notices(h) == []
            assert len(reply_texts(h)) == 1

    asyncio.run(go())


def test_continuation_waits_out_a_winding_down_turn_then_runs(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            await _first_slice_cut(c)
            # The interrupted turn is still winding down in the live session.
            h.runner.turn_active = True

            async def wind_down() -> None:
                await asyncio.sleep(0.5)
                h.runner.turn_active = False

            flipper = asyncio.create_task(wind_down())
            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            _final_slice(c)
            second = await _next_continuation(c)
            started = time.monotonic()
            try:
                await h.kernel.process_event(second.event, lease=second.lease)
            finally:
                await flipper

            assert time.monotonic() - started >= 0.45
            assert h.runner.opened == [PROMPT, PROMPT]
            assert h.runner.steers == []
            assert await run_outcome(c.run) == "ran"
            assert notices(h) == []
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(go())


def test_continuation_busy_past_its_budget_stops_failed_with_a_notice(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            await _first_slice_cut(c)
            h.runner.turn_active = True
            second = await _next_continuation(c)
            await force_deadline(h, second, in_ms=6_500)
            try:
                await asyncio.wait_for(
                    h.kernel.process_event(second.event, lease=second.lease), timeout=20
                )
            finally:
                h.runner.turn_active = False

            assert h.runner.opened == [PROMPT]
            assert h.runner.steers == []
            assert await run_outcome(c.run) == "failed"
            texts = reply_texts(h)
            assert texts == [_uncovered_notice(c, "failed")], texts
            assert not any("delivery deadline" in t.lower() for t in texts)
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(go())


def test_paused_hook_continuation_closes_blocked_with_a_notice(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            await _first_slice_cut(c)
            await pause_hook(c.run)
            second = await _next_continuation(c)

            await c.h.kernel.process_event(second.event, lease=second.lease)

            assert c.h.runner.opened == [PROMPT]
            assert await run_outcome(c.run) == "blocked"
            assert [n.text for n in notices(c.h)] == [_uncovered_notice(c, "blocked")]
            assert len(c.h.fake_k8s.claim_envs) == 1

    asyncio.run(go())


def test_paused_first_fire_still_defers(make_hook_run, make_harness) -> None:
    """Liveness: a paused FIRST fire keeps today's ``deferred`` and posts nothing."""

    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            c.checkpoint()
            await pause_hook(c.run)
            first = await enqueue(c.h, cron_event(c.run))

            await c.h.kernel.process_event(first.event, lease=first.lease)

            assert await run_outcome(c.run) == "deferred"
            assert c.h.runner.opened == []
            assert notices(c.h) == []
            assert await successors(c.h) == []

    asyncio.run(go())


def test_targetless_budget_cut_is_unchanged(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with make_hook_run() as seed:
            # A targetless turn routes by the hook agent's active deployment
            # (#2963), so the agent needs one, resolved by the real resolver.
            deployment_id = uuid.uuid4()
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) "
                        "VALUES (:id, :agent_id, :version_id, "
                        "CAST('prod' AS curie.environment), 'active')"
                    ),
                    {"id": deployment_id, "agent_id": seed.agent_id, "version_id": seed.version_id},
                )
            try:
                async with sweep_case(
                    lambda: _existing(seed),
                    make_harness,
                    binding_factory=lambda config: BindingResolver(seed.engine, config),
                ) as c:
                    c.checkpoint()
                    first = await enqueue(c.h, cron_event(c.run, targeted=False))
                    await run_slice_to_budget_cut(c.h, first)

                    assert c.h.runner.opened == [PROMPT]
                    assert await successors(c.h) == []
                    assert c.h.sink.events == []
                    assert await run_outcome(c.run) == "failed"
                    assert c.api.requests == [], "a targetless turn read sweep coverage"
            finally:
                async with seed.engine.begin() as conn:
                    await conn.execute(
                        text("DELETE FROM curie.deployments WHERE id = :id"),
                        {"id": deployment_id},
                    )

    asyncio.run(go())


def test_continuation_of_an_expired_retry_base_is_not_refused_by_the_catch_up_bound(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h, run = c.h, c.run
            expires = datetime.now(UTC) + timedelta(seconds=3)
            retry_id = retry_event_id(f"cron:{run.ref.agent_id}:{run.ref.name}", expires)
            first = await enqueue(h, cron_event(run, event_id=retry_id))

            async def save() -> None:
                c.checkpoint()

            await run_slice_to_budget_cut(h, first, at_cut=save)
            assert [s.event_id for s in await successors(h)] == [f"{retry_id}:sweep:1:1:0"]

            # The retry's catch-up bound passes before the continuation runs.
            while datetime.now(UTC) <= expires.replace(microsecond=0) + timedelta(seconds=1):
                await asyncio.sleep(0.1)
            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            _final_slice(c)
            second = await _next_continuation(c)
            await h.kernel.process_event(second.event, lease=second.lease)

            assert await run_outcome(run) == "ran"
            assert h.runner.opened == [PROMPT, PROMPT]
            assert notices(h) == []

    asyncio.run(go())


def test_no_notice_when_ownership_moves_during_the_coverage_read(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            c.checkpoint()
            first = await enqueue(h, cron_event(c.run))
            moved: list[bool] = []

            async def transfer(_request: Any) -> None:
                if not moved:
                    moved.append(True)
                    await bump_fence(h, first)

            c.api.on_request = transfer
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="upstream returned 500", classification="server-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ]
            ]

            await h.kernel.process_event(first.event, lease=first.lease)

            assert moved, "the coverage read never happened"
            assert notices(h) == []
            assert not await h.async_redis.exists(h.config.done_key(first.event.event_id))
            assert h.sink.completions == []
            assert first.lease.lost.is_set()
            assert await run_outcome(c.run) == "failed"

    asyncio.run(go())


def test_continuation_refused_before_runner_admission_closes_failed_with_a_notice(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            await _first_slice_cut(c)
            second = await _next_continuation(c)
            h.runner.hold = None
            h.runner.event_fail_times = 1

            await h.kernel.process_event(second.event, lease=second.lease)

            assert await run_outcome(c.run) == "failed"
            texts = reply_texts(h)
            found = notices(h)
            assert [n.text for n in found] == [_uncovered_notice(c, "failed")]
            assert len(texts) == 2 and texts[-1] == found[0].text, texts
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(go())


def test_continuation_refused_by_a_paused_agent_closes_failed_with_a_notice(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with make_hook_run() as seed:
            deployment_id = uuid.uuid4()
            channel_id = uuid.uuid4()
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) "
                        "VALUES (:id, :agent_id, :version_id, "
                        "CAST('prod' AS curie.environment), 'active')"
                    ),
                    {"id": deployment_id, "agent_id": seed.agent_id, "version_id": seed.version_id},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_channels "
                        "(id, agent_id, kind, address, adapter) "
                        "VALUES (:id, :agent_id, 'slack', 'C1', 'default')"
                    ),
                    {"id": channel_id, "agent_id": seed.agent_id},
                )
            try:
                async with sweep_case(
                    lambda: _existing(seed),
                    make_harness,
                    binding_factory=lambda config: BindingResolver(seed.engine, config),
                    with_killswitch=True,
                ) as c:
                    h = c.h
                    c.checkpoint()
                    await h.async_redis.set(kill_key(seed.agent_id), "1")
                    continuation = await enqueue(
                        h, cron_event(seed, event_id=f"{_base(c)}:sweep:1:1:0")
                    )

                    await h.kernel.process_event(continuation.event, lease=continuation.lease)

                    assert h.runner.opened == []
                    assert h.fake_k8s.claim_envs == []
                    assert await run_outcome(seed) == "failed"
                    texts = reply_texts(h)
                    found = notices(h)
                    assert any("paused" in t.lower() for t in texts), texts
                    assert [n.text for n in found] == [_uncovered_notice(c, "failed")]
                    assert texts[-1] == found[0].text
            finally:
                async with seed.engine.begin() as conn:
                    await conn.execute(
                        text("DELETE FROM curie.agent_channels WHERE id = :id"),
                        {"id": channel_id},
                    )
                    await conn.execute(
                        text("DELETE FROM curie.deployments WHERE id = :id"),
                        {"id": deployment_id},
                    )

    asyncio.run(go())


@contextlib.asynccontextmanager
async def _existing(seed: Any) -> AsyncIterator[Any]:
    """An already seeded run in the ``make_hook_run()`` call shape."""
    yield seed


def test_pending_notice_is_posted_by_the_error_close_when_the_settle_raises(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            c.checkpoint()
            first = await enqueue(h, cron_event(c.run))
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="upstream returned 500", classification="server-error"),
                    Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                ]
            ]
            markers = h.kernel._markers
            original = markers.settle_fenced
            raised: list[bool] = []

            async def settle_once_fails(*args: Any, **kwargs: Any) -> Any:
                if not raised:
                    raised.append(True)
                    raise RuntimeError("injected settle failure")
                return await original(*args, **kwargs)

            markers.settle_fenced = settle_once_fails
            try:
                await h.kernel.process_event(first.event, lease=first.lease)
            except RuntimeError as exc:
                assert "injected settle failure" in str(exc)
            else:
                raise AssertionError("the settle failure did not propagate")

            assert raised
            assert await run_outcome(c.run) == "failed"
            assert [n.text for n in notices(h)] == [_uncovered_notice(c, "failed")]
            assert not await h.async_redis.exists(h.config.done_key(first.event.event_id))

            again = await redeliver(h, first)
            await h.kernel.process_event(again.event, lease=again.lease)

            assert len(notices(h)) == 1, "the redelivery posted the notice again"
            assert h.runner.opened == [PROMPT]
            assert await h.async_redis.exists(h.config.done_key(first.event.event_id))

    asyncio.run(go())


def test_error_close_that_did_not_close_posts_nothing(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness, runner_total_timeout_s=30.0) as c:
            h = c.h
            c.checkpoint()
            first = await enqueue(h, cron_event(c.run))
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.turn_scripts = [[TextDelta(text="started")]]
            h.runner.tail = [
                ErrorEvent(message="upstream returned 500", classification="server-error"),
                Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            markers = h.kernel._markers
            original = markers.settle_fenced
            raised: list[bool] = []

            async def settle_once_fails(*args: Any, **kwargs: Any) -> Any:
                if not raised:
                    raised.append(True)
                    raise RuntimeError("injected settle failure")
                return await original(*args, **kwargs)

            markers.settle_fenced = settle_once_fails
            started = arm_started(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(first.event, lease=first.lease))
            try:
                await asyncio.wait_for(started.wait(), timeout=5.0)
                await set_outcome(c.run, "reclaimed")
                hold.set()
                results = await asyncio.gather(task, return_exceptions=True)
            finally:
                hold.set()

            assert isinstance(results[0], RuntimeError), results
            assert raised
            assert await run_outcome(c.run) == "reclaimed"
            assert notices(h) == []

    asyncio.run(go())


def test_kernel_without_a_sweep_service_behaves_as_today(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness, with_sweep=False) as c:
            first = await _first_slice_cut(c)

            assert await successors(c.h) == []
            assert await run_outcome(c.run) == "failed"
            assert any("delivery deadline" in t.lower() for t in reply_texts(c.h))
            assert notices(c.h) == []
            assert c.api.requests == []
            assert await c.h.async_redis.exists(c.h.config.done_key(first.event.event_id))

    asyncio.run(go())


def test_production_sweep_coverage_publishes_a_successor_on_a_budget_cut(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness, production_factory=True) as c:
            h = c.h
            first = await _first_slice_cut(c)
            assert [s.event_id for s in await successors(h)] == [
                f"{first.event.event_id}:sweep:1:1:0"
            ]
            # The production factory read with the configured platform key.
            assert c.api.requests
            assert {r.headers["X-API-Key"] for r in c.api.requests} == {h.config.api_key}

            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            _final_slice(c)
            second = await _next_continuation(c)
            await h.kernel.process_event(second.event, lease=second.lease)

            assert await run_outcome(c.run) == "ran"
            assert h.runner.opened == [PROMPT, PROMPT]
            assert len(h.fake_k8s.claim_envs) == 1
            assert notices(h) == []

    asyncio.run(go())
