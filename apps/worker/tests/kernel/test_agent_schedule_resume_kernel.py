"""ADR 0238 review r1 F4: approval resumes of agent schedule firings, driven
through the kernel's real ``process_event``.

An approved continuation of a firing on ``hook:<agent>:agent-schedule:<id>``
is checked at lifecycle intake, again at the guarded runner start, and on the
steer path when it folds into a live session. Each refusal drops the event
(``turn.completed`` outcome ``dropped``) without starting or steering the
runner; each has a neighboring case that runs. The schedule, its firing and
the approval rows are real Postgres rows; the recorder is the real
``HookRunRecorder`` (wrapped only to count guard entries and to commit a
change between intake and start); the scheduled-origin read is the real
``BindingResolver.approval_from_cron``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import sys
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from curie_internal.sandbox_token import verify
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from curie_worker.hook_runs import HookRunRecorder

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from queue_fixtures import qevent  # noqa: E402
from test_agent_schedule_guards import SLOT, ScheduleSeed, seeded  # noqa: E402
from test_memory_token_event import DEPLOYMENT_ID, _MemoryBinding  # noqa: E402

RESUME_TEXT = "Approved: post the summary."
APPROVER = "U0APPROVE1"


class _SeedBinding(_MemoryBinding):
    """Resolves the seeded agent with agent schedules on and memory writes off.

    Boot env and mints stay real; ``approval_from_cron`` is the real resolver's
    read against the seeded approvals.
    """

    def __init__(self, seed: ScheduleSeed) -> None:
        super().__init__(memory_writes=False)
        self._seed = seed
        self._resolver = BindingResolver(seed.engine, WorkerConfig())

    def _deployment(self) -> ResolvedDeployment:
        return ResolvedDeployment(
            agent_id=self._seed.agent_id,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            version_id=self._seed.version_id,
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
        )

    async def agent_schedules_for(self, _agent_id: uuid.UUID) -> bool:
        return True

    async def approval_from_cron(self, event_id: str, agent_id: uuid.UUID) -> bool:
        return await self._resolver.approval_from_cron(event_id, agent_id)


class _CountingRecorder(HookRunRecorder):
    """The real recorder; counts ``resume_guard`` entries and can commit a
    change just before entry ``change_before`` takes the lock."""

    def __init__(
        self,
        seed: ScheduleSeed,
        *,
        change_before: int | None = None,
        change: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__(seed.engine, "curie")
        self.guards = 0
        self._change_before = change_before
        self._change = change

    @contextlib.asynccontextmanager
    async def resume_guard(
        self, agent_id: uuid.UUID, schedule_id: uuid.UUID, approval_id: uuid.UUID
    ) -> AsyncIterator[bool]:
        self.guards += 1
        if self.guards == self._change_before and self._change is not None:
            await self._change()
        async with super().resume_guard(agent_id, schedule_id, approval_id) as allowed:
            yield allowed


async def _guard_entries(make_harness: Any, *, live: bool) -> int:
    """How many guard entries a valid resume takes, idle (``live=False``) or
    steering a live session. The last entry is the one around the runner start
    or the steer; intake is the first."""

    async with seeded() as seed:
        await seed.firing(SLOT, 1)
        approval = await seed.approval(seed.event_id(SLOT))
        recorder = _CountingRecorder(seed)
        async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
            h.runner.turn_active = live
            event = _resume(seed, approval)
            await h.kernel.process_event(event)
            if live:
                assert h.runner.steers == [RESUME_TEXT]
            else:
                assert h.runner.opened == [RESUME_TEXT]
            assert recorder.guards >= 2, "a valid resume is checked at intake and again"
            return recorder.guards


def _resume(seed: ScheduleSeed, approval_id: uuid.UUID, *, thread: str | None = None) -> Any:
    turn = qevent(
        RESUME_TEXT,
        thread=thread or f"hook:{seed.agent_id}:{seed.name}",
        event_id=f"approval-{approval_id}-resolved",
        channel=seed.address,
    )
    return turn.model_copy(update={"author": APPROVER})


async def _cancel(seed: ScheduleSeed) -> None:
    await seed.execute(
        "UPDATE curie.agent_schedules SET state = 'cancelled', cancelled_at = now(), "
        "cancelled_by = 'U0EXAMPLE1' WHERE id = :id",
        id=seed.schedule_id,
    )


def _claims(seed: ScheduleSeed, token: object) -> dict[str, Any]:
    assert isinstance(token, str) and token, f"no memory_token on the frame: {token!r}"
    assert verify(token, WorkerConfig().api_key, agent=str(seed.agent_id), scope="state")
    seg = token.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    assert isinstance(payload, dict)
    return payload


def _outcomes(h: Any, event_id: str) -> list[str]:
    return [c.outcome for c in h.sink.completions if c.event_id == event_id]


# --- intake ---------------------------------------------------------------------


def test_cancelled_schedule_resume_is_dropped_at_intake(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await _cancel(seed)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [], "a cancelled schedule's resume started"
                assert h.runner.steers == []
                assert _outcomes(h, event.event_id) == ["dropped"]
                assert recorder.guards == 1, "refused at intake, before any start"

    asyncio.run(go())


def test_unchanged_schedule_resume_runs_with_scheduled_credential(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT]
                assert _outcomes(h, event.event_id) == ["delivered"]
                assert recorder.guards >= 2, "checked at intake and at the guarded start"
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["scheduled"] is True
                assert claims["schedules"] == "write"

    asyncio.run(go())


# --- guarded start ----------------------------------------------------------------


def test_cancel_between_intake_and_start_is_refused_at_the_guarded_start(
    make_harness,
) -> None:
    async def go() -> None:
        entries = await _guard_entries(make_harness, live=False)
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            # Committed after every earlier check passed, just before the
            # guard around the runner start takes the schedule's lock.
            recorder = _CountingRecorder(seed, change_before=entries, change=lambda: _cancel(seed))
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert recorder.guards == entries
                assert h.runner.opened == [], "the runner started after the cancel committed"
                assert h.runner.steers == []
                assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


def test_done_one_shot_unchanged_resume_runs(make_harness) -> None:
    async def go() -> None:
        async with seeded(one_shot=True) as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT]
                assert _outcomes(h, event.event_id) == ["delivered"]

    asyncio.run(go())


# --- ordinary approvals are unaffected ----------------------------------------------


def test_human_approval_resume_outside_a_hook_conversation_is_unaffected(
    make_harness,
) -> None:
    async def go() -> None:
        async with seeded() as seed:
            approval = await seed.approval("Ev0EXAMPLE01")
            # Cancelling the agent's schedule does not touch an ordinary thread.
            await _cancel(seed)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval, thread="th-human-approval")
                await h.kernel.process_event(event)

                assert recorder.guards == 0, "no schedule guard on an ordinary resume"
                assert h.runner.opened == [RESUME_TEXT]
                assert _outcomes(h, event.event_id) == ["delivered"]
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["scheduled"] is False

    asyncio.run(go())


# --- steer into a live session ----------------------------------------------------


def test_resume_steers_a_live_session_when_the_schedule_is_unchanged(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                h.runner.turn_active = True
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.steers == [RESUME_TEXT]
                assert h.runner.opened == []
                assert recorder.guards >= 2, "checked at intake and around the steer"

    asyncio.run(go())


def test_cancel_between_intake_and_steer_is_refused_at_the_guarded_steer(
    make_harness,
) -> None:
    async def go() -> None:
        entries = await _guard_entries(make_harness, live=True)
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            recorder = _CountingRecorder(seed, change_before=entries, change=lambda: _cancel(seed))
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                h.runner.turn_active = True
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert recorder.guards == entries
                assert h.runner.steers == [], "steered after the cancel committed"
                assert h.runner.opened == []
                assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


def test_cancelled_schedule_resume_never_steers(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await _cancel(seed)
            async with make_harness(
                binding=_SeedBinding(seed), hook_runs=_CountingRecorder(seed)
            ) as h:
                h.runner.turn_active = True
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.steers == []
                assert h.runner.opened == []
                assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


# --- F1 at the kernel boundary: a chained approval ---------------------------------


def test_chained_approval_resume_runs_and_carries_scheduled_true(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            first = await seed.approval(seed.event_id(SLOT))
            # The second gate inside the resumed continuation is stamped with
            # that continuation's event id.
            second = await seed.approval(f"approval-{first}-resolved")
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, second)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT]
                assert _outcomes(h, event.event_id) == ["delivered"]
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["scheduled"] is True

    asyncio.run(go())


def test_chained_approval_resume_is_dropped_when_cancelled(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            first = await seed.approval(seed.event_id(SLOT))
            second = await seed.approval(f"approval-{first}-resolved")
            await _cancel(seed)
            async with make_harness(
                binding=_SeedBinding(seed), hook_runs=_CountingRecorder(seed)
            ) as h:
                event = _resume(seed, second)
                await h.kernel.process_event(event)

                assert h.runner.opened == []
                assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


# --- review r4 F1: a done one-shot's continuation is revoked by cancelling it ------


def test_done_one_shot_resume_is_dropped_once_cancelled(make_harness) -> None:
    async def go() -> None:
        async with seeded(one_shot=True) as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await _cancel(seed)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [], "a cancelled one-shot's resume started"
                assert h.runner.steers == []
                assert _outcomes(h, event.event_id) == ["dropped"]
                assert recorder.guards == 1, "refused at intake, before any start"

    asyncio.run(go())


# --- review r4 F2: the guard follows the chain origin, not the conversation --------

HUMAN = "U0ALICE01"


async def _human_approval(seed: ScheduleSeed, dedupe_key: str = "Ev0EXAMPLE02") -> uuid.UUID:
    """An approval raised by a human turn (a Slack event id origin) in the
    schedule's conversation, not by one of its cron events."""

    approval_id = uuid.uuid4()
    await seed.execute(
        "INSERT INTO curie.approvals "
        "(id, agent_id, conversation_id, author, summary, reply_kind, reply_channel, "
        "dedupe_key, status, resolved_by, resolved_at) VALUES "
        "(:id, :agent, :conversation, :author, 'Post the summary?', 'slack', :address, "
        ":key, 'approved', :approver, now())",
        id=approval_id,
        agent=seed.agent_id,
        conversation=f"hook:{seed.agent_id}:{seed.name}",
        author=HUMAN,
        address=seed.address,
        key=dedupe_key,
        approver=APPROVER,
    )
    return approval_id


async def _pause(seed: ScheduleSeed) -> None:
    await seed.execute(
        "INSERT INTO curie.schedule_controls (agent_id, name, paused_at) "
        "VALUES (:agent, :name, now())",
        agent=seed.agent_id,
        name=seed.name,
    )


class _ToggleOffBinding(_SeedBinding):
    """The seeded binding with the agent's schedule toggle off."""

    async def agent_schedules_for(self, _agent_id: uuid.UUID) -> bool:
        return False


class _OriginReadFails(_SeedBinding):
    """The seeded binding whose chain origin read raises."""

    def __init__(self, seed: ScheduleSeed) -> None:
        super().__init__(seed)
        self.origin_reads = 0

    async def approval_from_cron(self, event_id: str, agent_id: uuid.UUID) -> bool:
        self.origin_reads += 1
        raise RuntimeError("connection reset by peer")


def test_human_origin_resume_in_a_schedule_conversation_runs_unscheduled(
    make_harness,
) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await _human_approval(seed)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT], "a human-origin resume was dropped"
                assert _outcomes(h, event.event_id) == ["delivered"]
                assert recorder.guards == 0, "no schedule guard on a human-origin resume"
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["scheduled"] is False

    asyncio.run(go())


def test_chained_human_origin_resume_in_a_schedule_conversation_runs_unscheduled(
    make_harness,
) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            first = await _human_approval(seed)
            second = await _human_approval(seed, f"approval-{first}-resolved")
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, second)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT], "a human-origin resume was dropped"
                assert _outcomes(h, event.event_id) == ["delivered"]
                assert recorder.guards == 0
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["scheduled"] is False

    asyncio.run(go())


def test_cron_origin_resume_is_dropped_when_paused(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await _pause(seed)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [], "a paused schedule's resume started"
                assert _outcomes(h, event.event_id) == ["dropped"]
                assert recorder.guards == 1

    asyncio.run(go())


def test_cron_origin_resume_is_dropped_with_the_toggle_off(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await seed.execute(
                "UPDATE curie.agents SET agent_schedules = false WHERE id = :id",
                id=seed.agent_id,
            )
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_ToggleOffBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [], "a resume ran with agent schedules off"
                assert h.runner.steers == []
                assert _outcomes(h, event.event_id) == ["dropped"]
                assert recorder.guards == 1

    asyncio.run(go())


def test_failed_origin_read_still_guards_the_resume(make_harness) -> None:
    # The human-origin approval would run if the origin were known; the read
    # raises, so the guard applies (restrictive) and refuses the non-cron chain.
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await _human_approval(seed)
            binding = _OriginReadFails(seed)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=binding, hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert binding.origin_reads >= 1
                assert recorder.guards == 1, "the guard was skipped after a failed read"
                assert h.runner.opened == []
                assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


def test_failed_origin_read_still_runs_an_active_cron_origin_resume(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_OriginReadFails(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT]
                assert _outcomes(h, event.event_id) == ["delivered"]
                assert recorder.guards >= 2
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["scheduled"] is True

    asyncio.run(go())


# --- review CR5-F1: an unresolved chain origin is restrictive -----------------------
#
# A chain with a missing link, a cycle, or a link into another agent's approval
# has no readable origin. It counts as a scheduled continuation: in a schedule
# conversation it takes the schedule guard (which refuses a chain it cannot
# place on a firing), and elsewhere it runs with ``scheduled`` true.


async def _approval_row(
    seed: ScheduleSeed,
    approval_id: uuid.UUID,
    dedupe_key: str,
    *,
    agent_id: uuid.UUID | None = None,
    thread: str | None = None,
) -> uuid.UUID:
    owner = agent_id or seed.agent_id
    await seed.execute(
        "INSERT INTO curie.approvals "
        "(id, agent_id, conversation_id, author, summary, reply_kind, reply_channel, "
        "dedupe_key, status, resolved_by, resolved_at) VALUES "
        "(:id, :agent, :conversation, :author, 'Post the summary?', 'slack', :address, "
        ":key, 'approved', :approver, now())",
        id=approval_id,
        agent=owner,
        conversation=thread or f"hook:{seed.agent_id}:{seed.name}",
        author=HUMAN,
        address=seed.address,
        key=dedupe_key,
        approver=APPROVER,
    )
    return approval_id


async def _cyclic(seed: ScheduleSeed, thread: str | None) -> uuid.UUID:
    a, b = uuid.uuid4(), uuid.uuid4()
    await _approval_row(seed, b, f"approval-{a}-resolved", thread=thread)
    return await _approval_row(seed, a, f"approval-{b}-resolved", thread=thread)


async def _missing_link(seed: ScheduleSeed, thread: str | None) -> uuid.UUID:
    return await _approval_row(
        seed, uuid.uuid4(), f"approval-{uuid.uuid4()}-resolved", thread=thread
    )


@contextlib.asynccontextmanager
async def _cross_agent(seed: ScheduleSeed, thread: str | None) -> AsyncIterator[uuid.UUID]:
    """This agent's approval linked to another agent's approval, whose origin
    is a person's event id there."""

    other = uuid.uuid4()
    await seed.execute(
        "INSERT INTO curie.agents (id, name, agent_schedules) VALUES (:id, :name, true)",
        id=other,
        name=f"sched_cross_{other.hex}",
    )
    try:
        theirs = await _approval_row(seed, uuid.uuid4(), "Ev0EXAMPLE09", agent_id=other)
        yield await _approval_row(seed, uuid.uuid4(), f"approval-{theirs}-resolved", thread=thread)
    finally:
        await seed.execute("DELETE FROM curie.approvals WHERE agent_id = :a", a=other)
        await seed.execute("DELETE FROM curie.agents WHERE id = :a", a=other)


@contextlib.asynccontextmanager
async def _unresolved(
    kind: str, seed: ScheduleSeed, thread: str | None = None
) -> AsyncIterator[uuid.UUID]:
    if kind == "cross_agent":
        async with _cross_agent(seed, thread) as approval:
            yield approval
    elif kind == "cycle":
        yield await _cyclic(seed, thread)
    else:
        yield await _missing_link(seed, thread)


UNRESOLVED = ("cycle", "missing_link", "cross_agent")


def _unresolved_resume_is_dropped(make_harness: Any, kind: str, *, cancelled: bool) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            if cancelled:
                await _cancel(seed)
            async with _unresolved(kind, seed) as approval:
                recorder = _CountingRecorder(seed)
                async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                    event = _resume(seed, approval)
                    await h.kernel.process_event(event)

                    assert recorder.guards == 1, f"{kind}: the schedule guard was skipped"
                    assert h.runner.opened == [], f"{kind}: an unresolved resume started"
                    assert h.runner.steers == []
                    assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


@pytest.mark.parametrize("kind", UNRESOLVED)
def test_unresolved_origin_resume_is_dropped_when_cancelled(make_harness, kind: str) -> None:
    _unresolved_resume_is_dropped(make_harness, kind, cancelled=True)


@pytest.mark.parametrize("kind", UNRESOLVED)
def test_unresolved_origin_resume_is_guarded_while_active(make_harness, kind: str) -> None:
    # The schedule is active, but the guard cannot tie an unresolved chain to
    # one of its firings, so the guarded resume is still refused.
    _unresolved_resume_is_dropped(make_harness, kind, cancelled=False)


def test_unresolved_origin_resume_never_steers_when_cancelled(make_harness) -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            await _cancel(seed)
            async with _unresolved("cycle", seed) as approval:
                recorder = _CountingRecorder(seed)
                async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                    h.runner.turn_active = True
                    event = _resume(seed, approval)
                    await h.kernel.process_event(event)

                    assert recorder.guards == 1
                    assert h.runner.steers == []
                    assert h.runner.opened == []
                    assert _outcomes(h, event.event_id) == ["dropped"]

    asyncio.run(go())


@pytest.mark.parametrize("kind", UNRESOLVED)
def test_unresolved_origin_resume_outside_a_schedule_runs_scheduled(
    make_harness, kind: str
) -> None:
    # With the toggle on, an ordinary thread's unresolved resume runs without
    # a schedule guard, and its credential says it is scheduled.
    thread = "th-unresolved-approval"

    async def go() -> None:
        async with seeded() as seed:
            async with _unresolved(kind, seed, thread) as approval:
                recorder = _CountingRecorder(seed)
                async with make_harness(binding=_SeedBinding(seed), hook_runs=recorder) as h:
                    event = _resume(seed, approval, thread=thread)
                    await h.kernel.process_event(event)

                    assert recorder.guards == 0
                    assert h.runner.opened == [RESUME_TEXT], f"{kind}: resume dropped"
                    assert _outcomes(h, event.event_id) == ["delivered"]
                    claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                    assert claims["scheduled"] is True, f"{kind}: minted scheduled false"

    asyncio.run(go())


class _SkillsOnlyBinding(_SeedBinding):
    """The seeded agent with learned skills on and agent schedules off."""

    def _deployment(self) -> ResolvedDeployment:
        return super()._deployment().model_copy(update={"learned_skills": True})

    async def agent_schedules_for(self, _agent_id: uuid.UUID) -> bool:
        return False


@pytest.mark.parametrize("origin", ["cron", "person"])
def test_learned_skills_alone_still_marks_a_scheduled_continuation(
    make_harness, origin: str
) -> None:
    # With only learned skills on, the origin is still read, so a continuation
    # of a cron run carries scheduled true (the learned-skills API then refuses
    # its writes) and a person's continuation carries scheduled false.
    thread = "th-skills-approval"

    async def go() -> None:
        async with seeded() as seed:
            key = (
                f"cron:{seed.agent_id}:nightly:2026-10-19T09:00:00+00:00"
                if origin == "cron"
                else "Ev0EXAMPLE03"
            )
            approval = await _approval_row(seed, uuid.uuid4(), key, thread=thread)
            recorder = _CountingRecorder(seed)
            async with make_harness(binding=_SkillsOnlyBinding(seed), hook_runs=recorder) as h:
                event = _resume(seed, approval, thread=thread)
                await h.kernel.process_event(event)

                assert h.runner.opened == [RESUME_TEXT]
                assert recorder.guards == 0
                claims = _claims(seed, h.runner.event_bodies[0]["memory_token"])
                assert claims["skills"] == "write"
                assert claims["schedules"] is None
                assert claims["scheduled"] is (origin == "cron")

    asyncio.run(go())
