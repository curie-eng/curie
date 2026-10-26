"""ADR 0248: approval-resume revalidation and the agent_schedules toggle copy.

``HookRunRecorder.resume_guard(agent_id, schedule_id, approval_id)`` decides
whether an approved continuation of a scheduled firing may still start. The
originating firing is the approval row's ``dedupe_key``, the cron event id
``cron:<agent>:agent-schedule:<id>:<slot iso>`` (or its retry form), and its
``agent_schedule_runs`` snapshot revision must equal the schedule's current
revision. Real Postgres, rows seeded and removed per test like
``make_hook_run``.

The kernel half: every resolve path converges on one read of
``agent_schedules_for`` that is copied onto the resolved deployment, so the
per-turn credential carries ``schedules: "write"`` with memory writes off, and a
failed read degrades to off without failing the turn.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from aci_protocol import Final, OutboundEvent, SessionStatus, TextDelta, ToolNote
from curie_worker.hook_runs import HookRunRecorder, retry_event_id
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_memory_token_event import (  # noqa: E402
    _as,
    _claims,
    _MemoryBinding,
    _qevent,
)
from test_work_item_early_stop import (  # noqa: E402
    ISSUE_PROMPT,
    _PublicationApi,
    _WorkItems,
    _Workspace,
)
from test_work_item_early_stop import _turn as _work_item_turn  # noqa: E402

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
    ),
)
PREFIX = "agent-schedule:"
SLOT = datetime(2026, 10, 20, 9, 0, tzinfo=UTC)


@dataclass
class ScheduleSeed:
    engine: AsyncEngine
    agent_id: uuid.UUID
    version_id: uuid.UUID
    schedule_id: uuid.UUID
    address: str

    @property
    def name(self) -> str:
        return f"{PREFIX}{self.schedule_id}"

    def event_id(self, slot: datetime) -> str:
        return f"cron:{self.agent_id}:{self.name}:{slot.isoformat()}"

    async def execute(self, sql: str, **params: Any) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text(sql), params)

    async def firing(self, slot: datetime, revision: int, outcome: str | None = "ran") -> None:
        run_id = uuid.uuid4()
        await self.execute(
            "INSERT INTO curie.hook_runs "
            "(id, agent_id, name, slot_utc, version_id, source, outcome, started_at, ended_at) "
            "VALUES (:id, :agent, :name, :slot, :version, 'schedule', :outcome, "
            "now() - interval '5 minutes', CASE WHEN :closed THEN now() END)",
            id=run_id,
            agent=self.agent_id,
            name=self.name,
            slot=slot,
            version=self.version_id,
            outcome=outcome,
            closed=outcome is not None,
        )
        await self.execute(
            "INSERT INTO curie.agent_schedule_runs "
            "(hook_run_id, schedule_id, agent_id, revision, prompt, cron, run_at, timezone, "
            "destination_kind, destination_address, destination_adapter) "
            "SELECT :run, id, agent_id, :revision, prompt, cron, run_at, timezone, "
            "destination_kind, destination_address, destination_adapter "
            "FROM curie.agent_schedules WHERE id = :schedule",
            run=run_id,
            revision=revision,
            schedule=self.schedule_id,
        )

    async def approval(self, dedupe_key: str) -> uuid.UUID:
        approval_id = uuid.uuid4()
        await self.execute(
            "INSERT INTO curie.approvals "
            "(id, agent_id, conversation_id, author, summary, reply_kind, reply_channel, "
            "dedupe_key, status, resolved_by, resolved_at) VALUES "
            "(:id, :agent, :conversation, :author, 'Post the summary?', 'slack', :address, "
            ":key, 'approved', 'U0APPROVE1', now())",
            id=approval_id,
            agent=self.agent_id,
            conversation=f"hook:{self.agent_id}:{self.name}",
            author=f"cron:{self.name}",
            address=self.address,
            key=dedupe_key,
        )
        return approval_id

    async def bump_revision(self) -> None:
        await self.execute(
            "UPDATE curie.agent_schedules SET revision = revision + 1, "
            "prompt = 'A changed prompt.', updated_at = now() WHERE id = :id",
            id=self.schedule_id,
        )

    async def allowed(self, approval_id: uuid.UUID) -> bool:
        recorder = HookRunRecorder(self.engine, "curie")
        async with recorder.resume_guard(self.agent_id, self.schedule_id, approval_id) as ok:
            return bool(ok)


@contextlib.asynccontextmanager
async def seeded(*, one_shot: bool = False) -> AsyncIterator[ScheduleSeed]:
    engine = create_async_engine(_DB_URL)
    token = uuid.uuid4().hex
    agent_id, version_id, schedule_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    address = "C" + token[:10].upper()
    seed = ScheduleSeed(engine, agent_id, version_id, schedule_id, address)
    try:
        await seed.execute(
            "INSERT INTO curie.agents (id, name, agent_schedules) VALUES (:id, :name, true)",
            id=agent_id,
            name=f"sched_guard_{token}",
        )
        await seed.execute(
            "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
            "VALUES (:id, :agent, :label, 'kernel-test')",
            id=version_id,
            agent=agent_id,
            label=f"sched_guard_{token}",
        )
        await seed.execute(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address, endpoint, adapter) "
            "VALUES (:id, :agent, 'slack', :address, NULL, 'default')",
            id=uuid.uuid4(),
            agent=agent_id,
            address=address,
        )
        await seed.execute(
            "INSERT INTO curie.agent_schedules "
            "(id, agent_id, revision, state, prompt, cron, run_at, timezone, "
            "destination_kind, destination_address, destination_adapter, "
            "created_by, created_source, created_conversation, created_turn, "
            "idempotency_key, effective_from, created_at, updated_at, updated_by) "
            "VALUES (:id, :agent, 1, :state, 'Post the summary.', :cron, :run_at, 'UTC', "
            "'slack', :address, 'default', 'U0EXAMPLE1', 'agent', :conversation, "
            "'evt-0001#abcd1234', :key, :created, :created, :created, 'U0EXAMPLE1')",
            id=schedule_id,
            agent=agent_id,
            state="done" if one_shot else "active",
            cron=None if one_shot else "0 * * * *",
            run_at=SLOT if one_shot else None,
            address=address,
            conversation=f"slack:{address}:1700000000.000100",
            key=token,
            created=SLOT - timedelta(days=1),
        )
        yield seed
    finally:
        for sql in (
            "DELETE FROM curie.approvals WHERE agent_id = :agent",
            "DELETE FROM curie.schedule_controls WHERE agent_id = :agent",
            "DELETE FROM curie.schedule_failure_streaks WHERE agent_id = :agent",
            "DELETE FROM curie.agent_schedule_runs WHERE agent_id = :agent",
            "DELETE FROM curie.hook_runs WHERE agent_id = :agent",
            "DELETE FROM curie.agent_schedule_revisions WHERE schedule_id IN "
            "(SELECT id FROM curie.agent_schedules WHERE agent_id = :agent)",
            "DELETE FROM curie.agent_schedules WHERE agent_id = :agent",
            "DELETE FROM curie.agent_channels WHERE agent_id = :agent",
            "DELETE FROM curie.agent_versions WHERE agent_id = :agent",
            "DELETE FROM curie.agents WHERE id = :agent",
        ):
            await seed.execute(sql, agent=agent_id)
        await engine.dispose()


def test_unchanged_approved_firing_resumes() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            assert await seed.allowed(approval) is True

    asyncio.run(go())


def test_later_firing_does_not_block_the_resume() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await seed.firing(SLOT + timedelta(hours=1), 1, outcome=None)
            assert await seed.allowed(approval) is True

    asyncio.run(go())


def test_done_one_shot_resumes() -> None:
    async def go() -> None:
        async with seeded(one_shot=True) as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            assert await seed.allowed(approval) is True

    asyncio.run(go())


def test_done_one_shot_resume_is_refused_once_cancelled() -> None:
    # Review r4 F1: cancelling a done one-shot is how its pending approval
    # continuation is revoked, so a cancelled one-shot refuses it.
    async def go() -> None:
        async with seeded(one_shot=True) as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            assert await seed.allowed(approval) is True
            await seed.execute(
                "UPDATE curie.agent_schedules SET state = 'cancelled', cancelled_at = now(), "
                "cancelled_by = 'U0EXAMPLE1' WHERE id = :id",
                id=seed.schedule_id,
            )
            assert await seed.allowed(approval) is False

    asyncio.run(go())


def test_cancelled_schedule_refuses_resume() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await seed.execute(
                "UPDATE curie.agent_schedules SET state = 'cancelled', cancelled_at = now(), "
                "cancelled_by = 'U0EXAMPLE1' WHERE id = :id",
                id=seed.schedule_id,
            )
            assert await seed.allowed(approval) is False

    asyncio.run(go())


def test_paused_schedule_refuses_resume_and_explicit_resume_passes() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await seed.execute(
                "INSERT INTO curie.schedule_controls (agent_id, name, paused_at) "
                "VALUES (:agent, :name, now())",
                agent=seed.agent_id,
                name=seed.name,
            )
            assert await seed.allowed(approval) is False
            await seed.execute(
                "UPDATE curie.schedule_controls SET paused_at = NULL "
                "WHERE agent_id = :agent AND name = :name",
                agent=seed.agent_id,
                name=seed.name,
            )
            assert await seed.allowed(approval) is True

    asyncio.run(go())


def test_toggle_off_refuses_resume() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await seed.execute(
                "UPDATE curie.agents SET agent_schedules = false WHERE id = :id",
                id=seed.agent_id,
            )
            assert await seed.allowed(approval) is False

    asyncio.run(go())


def test_destination_removed_refuses_resume_even_with_other_kind_on_address() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            approval = await seed.approval(seed.event_id(SLOT))
            await seed.execute(
                "INSERT INTO curie.agent_channels (id, agent_id, kind, address, endpoint, adapter) "
                "VALUES (:id, :agent, 'mail', :address, NULL, NULL)",
                id=uuid.uuid4(),
                agent=seed.agent_id,
                address=seed.address,
            )
            await seed.execute(
                "DELETE FROM curie.agent_channels WHERE agent_id = :agent AND kind = 'slack'",
                agent=seed.agent_id,
            )
            assert await seed.allowed(approval) is False

    asyncio.run(go())


def test_originating_revision_must_equal_current_revision() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            stale = await seed.approval(seed.event_id(SLOT))
            await seed.bump_revision()
            # A newer firing at the current revision does not vouch for the old one.
            later = SLOT + timedelta(hours=1)
            await seed.firing(later, 2)
            fresh = await seed.approval(seed.event_id(later))
            assert await seed.allowed(stale) is False
            assert await seed.allowed(fresh) is True

    asyncio.run(go())


def test_retry_suffixed_event_id_names_the_originating_firing() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            later = SLOT + timedelta(hours=1)
            await seed.firing(later, 1)
            retry = retry_event_id(seed.event_id(SLOT), SLOT + timedelta(hours=1))
            approval = await seed.approval(retry)
            assert await seed.allowed(approval) is True
            # Revision moves; a firing at the new revision exists. The retry id
            # still names the SLOT firing, whose revision is now stale.
            await seed.bump_revision()
            await seed.firing(SLOT + timedelta(hours=2), 2)
            assert await seed.allowed(approval) is False

    asyncio.run(go())


# Review r1 F1: an approval raised inside an approval-resumed continuation is
# stamped with the resume's own event id, ``approval-<first id>-resolved``. The
# guard walks that chain back to the originating cron event, bounded.


def _resumed(approval_id: uuid.UUID) -> str:
    return f"approval-{approval_id}-resolved"


async def _guard(seed: ScheduleSeed, approval_id: uuid.UUID) -> bool:
    # A cycle must refuse, never hang the guard.
    return await asyncio.wait_for(seed.allowed(approval_id), timeout=10)


def test_second_approval_in_a_resumed_continuation_walks_to_the_cron_origin() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            first = await seed.approval(seed.event_id(SLOT))
            second = await seed.approval(_resumed(first))
            assert await _guard(seed, first) is True
            assert await _guard(seed, second) is True
            third = await seed.approval(_resumed(second))
            assert await _guard(seed, third) is True

    asyncio.run(go())


def test_chained_approval_refuses_when_the_schedule_was_cancelled() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            first = await seed.approval(seed.event_id(SLOT))
            second = await seed.approval(_resumed(first))
            assert await _guard(seed, second) is True
            await seed.execute(
                "UPDATE curie.agent_schedules SET state = 'cancelled', cancelled_at = now(), "
                "cancelled_by = 'U0EXAMPLE1' WHERE id = :id",
                id=seed.schedule_id,
            )
            assert await _guard(seed, second) is False

    asyncio.run(go())


def test_chained_approval_refuses_when_the_origin_revision_is_stale() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            first = await seed.approval(seed.event_id(SLOT))
            second = await seed.approval(_resumed(first))
            assert await _guard(seed, second) is True
            await seed.bump_revision()
            await seed.firing(SLOT + timedelta(hours=1), 2)
            assert await _guard(seed, second) is False

    asyncio.run(go())


def test_chain_leading_to_a_human_message_is_not_a_scheduled_continuation() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            human = await seed.approval("Ev0EXAMPLE01")
            second = await seed.approval(_resumed(human))
            assert await _guard(seed, second) is False
            # Neighbor: the same depth ending at the cron event passes.
            origin = await seed.approval(seed.event_id(SLOT))
            assert await _guard(seed, await seed.approval(_resumed(origin))) is True

    asyncio.run(go())


def test_approval_chain_cycle_refuses() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            a, b = uuid.uuid4(), uuid.uuid4()
            for approval_id, key in ((a, _resumed(b)), (b, _resumed(a))):
                await seed.execute(
                    "INSERT INTO curie.approvals "
                    "(id, agent_id, conversation_id, author, summary, reply_kind, "
                    "reply_channel, dedupe_key, status, resolved_by, resolved_at) VALUES "
                    "(:id, :agent, :conversation, 'cron:x', 'Post the summary?', 'slack', "
                    ":address, :key, 'approved', 'U0APPROVE1', now())",
                    id=approval_id,
                    agent=seed.agent_id,
                    conversation=f"hook:{seed.agent_id}:{seed.name}",
                    address=seed.address,
                    key=key,
                )
            assert await _guard(seed, a) is False
            assert await _guard(seed, b) is False
            assert await _guard(seed, await seed.approval(seed.event_id(SLOT))) is True

    asyncio.run(go())


async def _chain(seed: ScheduleSeed, links: int) -> uuid.UUID:
    """``links`` approvals, the first stamped with the cron event, each later
    one stamped with its predecessor's resume id; returns the last."""

    current = await seed.approval(seed.event_id(SLOT))
    for _ in range(links - 1):
        current = await seed.approval(_resumed(current))
    return current


def test_approval_chain_walk_is_bounded() -> None:
    # Each chain starts at the cron event's dedupe key, which is unique, so each
    # length gets its own seed.
    async def go(links: int) -> bool:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            return await _guard(seed, await _chain(seed, links))

    assert asyncio.run(go(4)) is True
    assert asyncio.run(go(20)) is False


def test_chain_through_another_agents_approval_refuses() -> None:
    async def go() -> None:
        async with seeded() as seed:
            await seed.firing(SLOT, 1)
            other = uuid.uuid4()
            await seed.execute(
                "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
                id=other,
                name=f"sched_guard_other_{other.hex}",
            )
            try:
                foreign = uuid.uuid4()
                await seed.execute(
                    "INSERT INTO curie.approvals "
                    "(id, agent_id, conversation_id, author, summary, reply_kind, "
                    "reply_channel, dedupe_key, status, resolved_by, resolved_at) VALUES "
                    "(:id, :agent, 'th-other', 'U0OTHER01', 'Other?', 'slack', 'C0EXAMPLE2', "
                    ":key, 'approved', 'U0APPROVE1', now())",
                    id=foreign,
                    agent=other,
                    key=seed.event_id(SLOT),
                )
                second = await seed.approval(_resumed(foreign))
                assert await _guard(seed, second) is False
            finally:
                await seed.execute("DELETE FROM curie.approvals WHERE agent_id = :a", a=other)
                await seed.execute("DELETE FROM curie.agents WHERE id = :a", a=other)

    asyncio.run(go())


# The kernel copies agent_schedules onto every resolved deployment.


class _ScheduleBinding(_MemoryBinding):
    """The memory double plus the agent_schedules read; mints stay real."""

    def __init__(
        self,
        *,
        memory_writes: bool = False,
        agent_schedules: bool = True,
        error: Exception | None = None,
    ) -> None:
        super().__init__(memory_writes=memory_writes)
        self.agent_schedules = agent_schedules
        self.error = error
        self.schedule_reads = 0

    async def agent_schedules_for(self, _agent_id: uuid.UUID) -> bool:
        self.schedule_reads += 1
        if self.error is not None:
            raise self.error
        return self.agent_schedules


def test_bound_turn_carries_schedules_write_with_memory_writes_off(make_harness) -> None:
    async def go() -> None:
        binding = _ScheduleBinding()
        async with make_harness(binding=binding) as h:
            turn = _as(_qevent("remind me at nine", thread="th-sched-1"), "U0ALICE01")
            await h.kernel.process_event(turn)

            assert binding.schedule_reads >= 1
            claims = _claims(h.runner.event_bodies[0]["memory_token"])
            assert claims["memory"] == "read"
            assert claims["schedules"] == "write"
            assert claims["scheduled"] is False
            assert claims["sender"] == "U0ALICE01"
            assert binding.turn_token_calls
            assert claims["conversation"] == binding.turn_token_calls[0]["thread_key"]
            for resolved_env in binding.envs:
                assert resolved_env.get("CURIE_MEMORY_WRITES", "0") == "0"

    asyncio.run(go())


def test_factory_turn_carries_schedules_write(make_harness) -> None:
    async def go() -> None:
        binding = _ScheduleBinding()
        scripts: list[list[OutboundEvent]] = [
            [
                ToolNote(text="running tool mcp__github__get_issue", tool="mcp__github__get_issue"),
                TextDelta(text="I read the issue."),
                Final(text="I read the issue.", status=SessionStatus.DONE),
            ],
            [Final(text="I will not continue.", status=SessionStatus.DONE)],
        ]
        async with make_harness(
            binding=binding, workspace_factory=_Workspace, publication_creator=_PublicationApi()
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.turn_scripts = [list(s) for s in scripts]
            h.runner.default_script = [Final(text="unexpected", status=SessionStatus.DONE)]
            turn = _work_item_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            await h.kernel.process_event(turn)

            assert h.runner.event_bodies
            for body in h.runner.event_bodies:
                claims = _claims(body["memory_token"])
                assert claims["memory"] == "read"
                assert claims["schedules"] == "write"

    asyncio.run(go())


def test_failed_agent_schedules_read_degrades_to_off(make_harness, caplog) -> None:
    caplog.set_level(logging.WARNING)

    async def go() -> None:
        binding = _ScheduleBinding(error=RuntimeError("connection reset by peer"))
        async with make_harness(binding=binding) as h:
            await h.kernel.process_event(_qevent("hello", thread="th-sched-2"))
            assert h.sink.last_text == "ok"
            assert binding.schedule_reads >= 1
            # Memory writes off and schedules degraded off: nothing to mint.
            assert h.runner.event_bodies[0]["memory_token"] is None

        writes = _ScheduleBinding(memory_writes=True, error=RuntimeError("boom"))
        async with make_harness(binding=writes) as h:
            await h.kernel.process_event(_qevent("hello again", thread="th-sched-3"))
            claims = _claims(h.runner.event_bodies[0]["memory_token"])
            assert claims["memory"] == "write"
            assert claims.get("schedules") is None

    asyncio.run(go())
    assert any(
        "schedule" in record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING
    ), [record.getMessage() for record in caplog.records]
