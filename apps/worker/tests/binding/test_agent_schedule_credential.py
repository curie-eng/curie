"""ADR 0248: the per-turn state credential carries the schedules claims.

The worker mints the per-turn ``scope="state"`` credential when the agent has
memory writes OR agent schedules on. ``memory`` is ``"write"`` only with memory
writes; ``schedules`` is ``"write"`` only with agent schedules; ``conversation``
is the turn's thread key and ``scheduled`` says whether the turn is a scheduled
run (or its approval continuation).

DB-free like ``test_memory_credential.py`` for the mint, with an independent
claim decoder; ``agent_schedules_for`` is read against the migrated Postgres.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid
from typing import Any

from curie_internal.sandbox_token import verify
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.binding import BindingResolver, ResolvedDeployment
from curie_worker.config import WorkerConfig
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_AGENT = uuid.UUID("11111111-1111-4111-8111-111111111111")
_KEY = "curie-dev-key"
_THREAD = "slack:C0123:1700000000.000100"
_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
    ),
)


def _resolved(**overrides: object) -> ResolvedDeployment:
    fields: dict[str, object] = {
        "agent_name": "test-agent",
        "agent_id": _AGENT,
        "version_id": uuid.UUID("22222222-2222-4222-8222-222222222222"),
        "version_label": "v1",
        "bundle_ref": "bundles/x.zip",
        "max_usd_per_day": None,
        "max_output_tokens_per_run": None,
    }
    fields.update(overrides)
    return ResolvedDeployment(**fields)  # type: ignore[arg-type]


def _resolver() -> BindingResolver:
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = WorkerConfig()  # type: ignore[attr-defined]
    return resolver


def _mint(resolved: ResolvedDeployment, **overrides: Any) -> str | None:
    kwargs: dict[str, Any] = dict(
        kind="slack",
        address="C0123",
        thread_key=_THREAD,
        sender="U0SENDER1",
        turn="evt-0001#abcd1234",
        ttl_s=300.0,
    )
    kwargs.update(overrides)
    return _resolver().turn_memory_token(resolved, **kwargs)


def _claims(token: str | None) -> dict[str, Any]:
    assert isinstance(token, str) and token
    assert verify(token, _KEY, agent=str(_AGENT), scope="state") is True
    seg = token.split(".")[1]
    payload = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    assert isinstance(payload, dict)
    return payload


def test_resolved_deployment_defaults_agent_schedules_off() -> None:
    assert _resolved().agent_schedules is False


def test_schedules_on_memory_writes_off_mints_read_memory_and_write_schedules() -> None:
    claims = _claims(_mint(_resolved(memory_writes=False, agent_schedules=True)))
    assert claims["agent"] == str(_AGENT)
    assert claims["scope"] == "state"
    assert claims["memory"] == "read"
    assert claims["schedules"] == "write"
    assert claims["conversation"] == _THREAD
    assert claims["scheduled"] is False
    assert claims["binding"] == "slack:C0123"
    assert claims["sender"] == "U0SENDER1"
    assert claims["turn"] == "evt-0001#abcd1234"


def test_scheduled_turn_sets_scheduled_true() -> None:
    claims = _claims(_mint(_resolved(memory_writes=False, agent_schedules=True), scheduled=True))
    assert claims["scheduled"] is True
    assert claims["schedules"] == "write"
    assert claims["memory"] == "read"


def test_memory_writes_on_schedules_off_has_no_schedules_write() -> None:
    claims = _claims(_mint(_resolved(memory_writes=True, agent_schedules=False)))
    assert claims["memory"] == "write"
    assert claims.get("schedules") is None
    assert claims["scheduled"] is False


def test_both_on_carries_both_writes() -> None:
    claims = _claims(_mint(_resolved(memory_writes=True, agent_schedules=True)))
    assert claims["memory"] == "write"
    assert claims["schedules"] == "write"


def test_both_off_mints_nothing() -> None:
    assert _mint(_resolved(memory_writes=False, agent_schedules=False)) is None
    assert _mint(_resolved(memory_writes=False, agent_schedules=False), scheduled=True) is None


def test_agent_schedules_for_reads_the_agent_column() -> None:
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        on, off, missing = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        try:
            await pg_connect_or_skip(engine)
            async with engine.begin() as conn:
                for agent, value in ((on, True), (off, False)):
                    await conn.execute(
                        text(
                            "INSERT INTO curie.agents (id, name, agent_schedules) "
                            "VALUES (:id, :name, :value)"
                        ),
                        {"id": agent, "name": f"sched_cred_{agent.hex}", "value": value},
                    )
            resolver = BindingResolver(engine, WorkerConfig())
            assert await resolver.agent_schedules_for(on) is True
            assert await resolver.agent_schedules_for(off) is False
            assert await resolver.agent_schedules_for(missing) is False
        finally:
            async with engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM curie.agents WHERE id = ANY(CAST(:ids AS uuid[]))"),
                    {"ids": [on, off]},
                )
            await engine.dispose()

    asyncio.run(go())


# Review r1 F1: the scheduled mark follows an approval chain. An approval raised
# inside an approval-resumed continuation is stamped ``approval-<prior>-resolved``;
# the continuation is scheduled when the chain leads to a cron event of this
# agent (an agent schedule or a bundle hook), and not when it leads to a person.


def _resume_id(approval_id: uuid.UUID) -> str:
    return f"approval-{approval_id}-resolved"


def _approval_chain_case(origin_key: str, links: int) -> bool:
    """Seed ``links`` approvals whose first is stamped ``origin_key`` and whose
    later ones each carry their predecessor's resume id, then read the scheduled
    mark for a resume of the last."""

    async def go() -> bool:
        engine = create_async_engine(_DB_URL)
        agent = uuid.uuid4()
        try:
            await pg_connect_or_skip(engine)
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.agents (id, name, agent_schedules) "
                        "VALUES (:id, :name, true)"
                    ),
                    {"id": agent, "name": f"sched_chain_{agent.hex}"},
                )
                key = origin_key.format(agent=agent)
                last = uuid.uuid4()
                for _ in range(links):
                    last = uuid.uuid4()
                    await conn.execute(
                        text(
                            "INSERT INTO curie.approvals "
                            "(id, agent_id, conversation_id, author, summary, reply_kind, "
                            "reply_channel, dedupe_key, status, resolved_by, resolved_at) "
                            "VALUES (:id, :agent, 'th-chain', 'U0EXAMPLE1', 'Go?', 'slack', "
                            "'C0123', :key, 'approved', 'U0APPROVE1', now())"
                        ),
                        {"id": last, "agent": agent, "key": key},
                    )
                    key = _resume_id(last)
            resolver = BindingResolver(engine, WorkerConfig())
            return await asyncio.wait_for(
                resolver.approval_from_cron(_resume_id(last), agent), timeout=10
            )
        finally:
            async with engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM curie.approvals WHERE agent_id = :a"), {"a": agent}
                )
                await conn.execute(text("DELETE FROM curie.agents WHERE id = :a"), {"a": agent})
            await engine.dispose()

    return asyncio.run(go())


_SCHEDULE_ORIGIN = (
    "cron:{agent}:agent-schedule:33333333-3333-4333-8333-333333333333:2026-10-20T09:00:00+00:00"
)
_BUNDLE_ORIGIN = "cron:{agent}:nightly-digest:2026-10-20T09:00:00+00:00"
_HUMAN_ORIGIN = "Ev0EXAMPLE01"


def test_direct_cron_origin_is_scheduled() -> None:
    assert _approval_chain_case(_SCHEDULE_ORIGIN, 1) is True
    assert _approval_chain_case(_HUMAN_ORIGIN, 1) is False


def test_chained_agent_schedule_origin_is_scheduled() -> None:
    assert _approval_chain_case(_SCHEDULE_ORIGIN, 2) is True
    assert _approval_chain_case(_SCHEDULE_ORIGIN, 3) is True


def test_chained_bundle_hook_origin_is_scheduled() -> None:
    assert _approval_chain_case(_BUNDLE_ORIGIN, 2) is True


def test_chained_human_origin_is_not_scheduled() -> None:
    assert _approval_chain_case(_HUMAN_ORIGIN, 2) is False
    assert _approval_chain_case(_HUMAN_ORIGIN, 3) is False


def test_overlong_chain_counts_as_scheduled() -> None:
    # An unreadable origin fails toward scheduled (the restrictive direction),
    # as an approval read error does at intake.
    assert _approval_chain_case(_HUMAN_ORIGIN, 20) is True


def test_chained_cron_origin_mints_scheduled_true() -> None:
    # The mint carries what the chain walk decided, as an exact JSON boolean.
    scheduled = _approval_chain_case(_SCHEDULE_ORIGIN, 2)
    claims = _claims(
        _mint(_resolved(memory_writes=False, agent_schedules=True), scheduled=scheduled)
    )
    assert claims["scheduled"] is True
    human = _approval_chain_case(_HUMAN_ORIGIN, 2)
    claims = _claims(_mint(_resolved(memory_writes=False, agent_schedules=True), scheduled=human))
    assert claims["scheduled"] is False


# Review CR5-F1: an unresolved chain origin is restrictive. A missing link, a
# cycle, a link into another agent's approval, or an unparseable resume id
# counts as a scheduled continuation; only a resolved origin that is not a
# ``cron:<agent>:`` key is a person's.


def _approval_rows_case(build: Any) -> bool:
    """Seed the approvals ``build(agent, other)`` returns as
    ``(id, owner, dedupe_key)`` rows (``owner`` is ``"self"`` or ``"other"``),
    then read the scheduled mark for a resume of the first row."""

    async def go() -> bool:
        engine = create_async_engine(_DB_URL)
        agent, other = uuid.uuid4(), uuid.uuid4()
        try:
            await pg_connect_or_skip(engine)
            async with engine.begin() as conn:
                for agent_id in (agent, other):
                    await conn.execute(
                        text(
                            "INSERT INTO curie.agents (id, name, agent_schedules) "
                            "VALUES (:id, :name, true)"
                        ),
                        {"id": agent_id, "name": f"sched_unres_{agent_id.hex}"},
                    )
                rows = build(agent, other)
                for approval_id, owner, key in rows:
                    await conn.execute(
                        text(
                            "INSERT INTO curie.approvals "
                            "(id, agent_id, conversation_id, author, summary, reply_kind, "
                            "reply_channel, dedupe_key, status, resolved_by, resolved_at) "
                            "VALUES (:id, :agent, 'th-chain', 'U0EXAMPLE1', 'Go?', 'slack', "
                            "'C0123', :key, 'approved', 'U0APPROVE1', now())"
                        ),
                        {
                            "id": approval_id,
                            "agent": agent if owner == "self" else other,
                            "key": key,
                        },
                    )
            resolver = BindingResolver(engine, WorkerConfig())
            return await asyncio.wait_for(
                resolver.approval_from_cron(_resume_id(rows[0][0]), agent), timeout=10
            )
        finally:
            async with engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM curie.approvals WHERE agent_id = ANY(CAST(:a AS uuid[]))"),
                    {"a": [agent, other]},
                )
                await conn.execute(
                    text("DELETE FROM curie.agents WHERE id = ANY(CAST(:a AS uuid[]))"),
                    {"a": [agent, other]},
                )
            await engine.dispose()

    return asyncio.run(go())


def test_missing_link_counts_as_scheduled() -> None:
    def build(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        return [(uuid.uuid4(), "self", _resume_id(uuid.uuid4()))]

    assert _approval_rows_case(build) is True


def test_missing_link_deeper_in_the_chain_counts_as_scheduled() -> None:
    def build(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        last, first = uuid.uuid4(), uuid.uuid4()
        return [
            (last, "self", _resume_id(first)),
            (first, "self", _resume_id(uuid.uuid4())),
        ]

    assert _approval_rows_case(build) is True


def test_cyclic_chain_counts_as_scheduled() -> None:
    def build(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        a, b = uuid.uuid4(), uuid.uuid4()
        return [(a, "self", _resume_id(b)), (b, "self", _resume_id(a))]

    assert _approval_rows_case(build) is True


def test_self_loop_counts_as_scheduled() -> None:
    def build(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        a = uuid.uuid4()
        return [(a, "self", _resume_id(a))]

    assert _approval_rows_case(build) is True


def test_cross_agent_link_counts_as_scheduled() -> None:
    # The linked approval belongs to another agent; even a person's origin
    # there does not make this agent's continuation a person's.
    def build(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        mine, theirs = uuid.uuid4(), uuid.uuid4()
        return [(mine, "self", _resume_id(theirs)), (theirs, "other", _HUMAN_ORIGIN)]

    assert _approval_rows_case(build) is True


def test_resolved_human_origin_is_not_scheduled() -> None:
    def single(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        return [(uuid.uuid4(), "self", _HUMAN_ORIGIN)]

    def chained(_agent: uuid.UUID, _other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        last, first = uuid.uuid4(), uuid.uuid4()
        return [(last, "self", _resume_id(first)), (first, "self", _HUMAN_ORIGIN)]

    assert _approval_rows_case(single) is False
    assert _approval_rows_case(chained) is False


def test_other_agents_cron_origin_is_not_scheduled() -> None:
    # A resolved origin that is a cron key of a different agent is not this
    # agent's scheduled firing.
    def build(_agent: uuid.UUID, other: uuid.UUID) -> list[tuple[uuid.UUID, str, str]]:
        return [(uuid.uuid4(), "self", _SCHEDULE_ORIGIN.format(agent=other))]

    assert _approval_rows_case(build) is False


def test_unparseable_resume_id_counts_as_scheduled() -> None:
    async def go() -> bool:
        engine = create_async_engine(_DB_URL)
        try:
            resolver = BindingResolver(engine, WorkerConfig())
            return await resolver.approval_from_cron("approval-not-a-uuid-resolved", _AGENT)
        finally:
            await engine.dispose()

    assert asyncio.run(go()) is True
