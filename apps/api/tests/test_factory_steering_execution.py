"""Steering input across SQL acquisition, the runs stream and the consuming start."""

from __future__ import annotations

import hashlib
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn
from curie_api import factory_steering_ledger as ledger
from curie_api import workitem_dispatch as dispatch
from curie_api.config import get_settings
from curie_api.models import ExecutionRequest
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_api.workitems.lifecycle import WorkItemOutcome
from forge_fakes import factory_wake, no_external_calls
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_github_factory_base import (
    _execute,
    _execution,
    _label,
    base_app,  # noqa: F401
)
from test_workitem_dispatch import (
    ADDRESS,
    CLAIM_NAME,
    OBJECTIVE,
    OWNER,
    SANDBOX_NAME,
    _agent_with_channel,
    _facts,
    allowlisted,  # noqa: F401
)
from test_workitem_reconciler import _run

LIMITS = {
    "memory_bytes": None,
    "ephemeral_storage_bytes": None,
    "scratch_bytes": {},
    "max_turns": None,
}
GUIDANCE = "Change the first line.\n  Leave the résumé intact. "


async def _admit(session: AsyncSession) -> tuple[UUID, UUID]:
    agent_id = await _agent_with_channel(session)
    await session.execute(
        text(
            "UPDATE curie.agents SET factory_public_slack_channel = :channel, "
            "factory_slack_since = clock_timestamp() WHERE id = :id"
        ),
        {"channel": ADDRESS, "id": agent_id},
    )
    facts = _facts(agent_id)
    outcome = await dispatch.admit(session, facts)
    assert isinstance(outcome, WorkItemOutcome) and outcome.request is not None
    return outcome.work_item.id, facts.request_id


async def _record(
    session: AsyncSession, item: UUID, body: str, embedded: UUID | None = None
) -> UUID:
    key = uuid4()
    await ledger.record(
        session,
        steering_id=key,
        work_item_id=item,
        source_identity="default",
        source_channel=ADDRESS,
        source_root_ts="1700000000.000001",
        source_facts={},
        first_delivery_id=str(key),
        source_content_sha256=ledger.digest(body),
        accepted_text=body,
        execution_request_id=embedded,
    )
    await session.commit()
    return key


async def _acquire(
    session: AsyncSession,
    request: UUID,
    generation: int,
    received: str | None = None,
) -> dispatch.DispatchConflict | dispatch.AcquireGrant:
    async with no_external_calls() as client:
        return await dispatch.acquire(
            session,
            request,
            owner=OWNER,
            generation=generation,
            wake=await factory_wake(session, request),
            context=None,
            client=client,
            input_sha256=None
            if received is None
            else hashlib.sha256(received.encode()).hexdigest(),
        )


async def _start(
    session: AsyncSession,
    request: UUID,
    grant: dispatch.AcquireGrant,
    *,
    received: str | None,
    owner: str = OWNER,
) -> dispatch.DispatchConflict | dispatch.StartResult:
    return await dispatch.start(
        session,
        request,
        owner=owner,
        generation=grant.generation,
        claim_name=CLAIM_NAME,
        sandbox_name=SANDBOX_NAME,
        context=grant.context,
        delivered_limits=LIMITS,
        input_sha256=None if received is None else hashlib.sha256(received.encode()).hexdigest(),
    )


async def _state(session: AsyncSession, key: UUID) -> ledger.SteeringEntry:
    entry = await ledger.get(session, key)
    assert entry is not None
    return entry


def test_first_grant_after_base_label_settling_selects_queued_guidance(
    runs_stream: str,
    valkey: redis.Redis,
    base_app: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, forge = base_app
    monkeypatch.setenv("FACTORY_BASE_LABEL_SETTLE_S", "60")
    get_settings.cache_clear()
    number = 3815
    _execute(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
        "SELECT :id, id, 'slack', :channel, 'default' FROM curie.agents "
        "WHERE repo_full_name='acme-corp/acme-bot'",
        {"id": uuid4(), "channel": ADDRESS},
    )
    _execute(
        "UPDATE curie.agents SET factory_public_slack_channel=:channel, "
        "factory_slack_since=clock_timestamp() WHERE repo_full_name='acme-corp/acme-bot'",
        {"channel": ADDRESS},
    )
    assert _label(client, forge, number).json()["status"] == "factory_admitted"
    row = _execution(number)
    request, item = row["id"], row["work_item_id"]

    async def steps(
        maker: async_sessionmaker[AsyncSession], loop: WorkItemReconciler, bus: aioredis.Redis
    ) -> None:
        async def published(generation: int) -> QueuedTurn:
            await loop._publish_execute_wakes()
            record = (await bus.xrevrange(runs_stream, count=1))[0][1]
            turn = QueuedTurn.model_validate_json(record[STREAM_PAYLOAD_FIELD.encode()])
            assert turn.event_id == f"work-item-{request}-execute-{generation}"
            return turn

        old = await published(1)
        async with maker() as session:
            early = await _acquire(session, request, 1, old.text)
            assert isinstance(early, dispatch.DispatchConflict) and early.code == "not_dispatchable"
            waiting = await session.get(ExecutionRequest, request, populate_existing=True)
            assert waiting is not None and waiting.last_deferral_reason == "base_label_settling"
            assert waiting.acquired_generation is None and waiting.steering_sealed_at is None
            key = await _record(session, item, GUIDANCE)
            await session.execute(
                text(
                    "UPDATE curie.execution_requests SET base_settle_until=clock_timestamp(), "
                    "dispatch_not_before=clock_timestamp() WHERE id=:id"
                ),
                {"id": request},
            )
            await session.commit()
        eligible = await published(2)
        assert eligible.text == old.text
        async with (
            maker() as session,
            httpx.AsyncClient(transport=httpx.MockTransport(forge.handle)) as external,
        ):
            selected = await dispatch.acquire(
                session,
                request,
                owner=OWNER,
                generation=2,
                wake=await factory_wake(session, request),
                context=None,
                client=external,
                input_sha256=ledger.digest(eligible.text),
            )
            assert (
                isinstance(selected, dispatch.DispatchConflict) and selected.code == "not_published"
            )
        guided = await published(3)
        assert guided.text == old.text + "\n\nAdditional guidance:\n" + GUIDANCE
        async with maker() as session:
            stale = await _acquire(session, request, 3, old.text)
            assert isinstance(stale, dispatch.DispatchConflict) and stale.code == "not_published"
            assert (await _state(session, key)).applied_at is None
            await session.execute(
                text(
                    "UPDATE curie.execution_requests SET dispatch_not_before=clock_timestamp() "
                    "WHERE id=:id"
                ),
                {"id": request},
            )
            await session.commit()
        retry = await published(4)
        assert retry.text == guided.text
        async with maker() as session:
            grant = await _acquire(session, request, 4, retry.text)
            assert isinstance(grant, dispatch.AcquireGrant)
            refused = await _start(session, request, grant, received=old.text)
            assert (
                isinstance(refused, dispatch.DispatchConflict)
                and refused.code == "not_dispatchable"
            )
            entry = await _state(session, key)
            assert entry.execution_request_id == request and entry.applied_at is None
            assert isinstance(
                await _start(session, request, grant, received=retry.text), dispatch.StartResult
            )
            assert (await _state(session, key)).state == "applied"

    _run(steps, runs_stream)


def test_selected_guidance_republishes_and_only_the_matching_start_applies(
    clean_db: None,
    allowlisted: None,  # noqa: F811
    runs_stream: str,
    valkey: redis.Redis,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession], loop: WorkItemReconciler, bus: aioredis.Redis
    ) -> None:
        async with maker() as session:
            item, request = await _admit(session)
        await loop._publish_execute_wakes()
        old = QueuedTurn.model_validate_json(
            (await bus.xrevrange(runs_stream, count=1))[0][1][STREAM_PAYLOAD_FIELD.encode()]
        )
        assert old.text == OBJECTIVE
        async with maker() as session:
            key = await _record(session, item, GUIDANCE)
            refused = await _acquire(session, request, 1, old.text)
            assert (
                isinstance(refused, dispatch.DispatchConflict) and refused.code == "not_published"
            )
            sealed = await session.get(ExecutionRequest, request, populate_existing=True)
            assert sealed is not None and sealed.acquired_generation is None
            assert (sealed.dispatch_generation, sealed.published_generation) == (2, 1)
            assert (sealed.dispatch_owner, sealed.dispatch_lease_expires_at) == (None, None)
            assert sealed.objective == old.text
        await loop._publish_execute_wakes()
        real = QueuedTurn.model_validate_json(
            (await bus.xrevrange(runs_stream, count=1))[0][1][STREAM_PAYLOAD_FIELD.encode()]
        )
        assert real.event_id.endswith("-execute-2")
        assert real.text.startswith(OBJECTIVE) and real.text.endswith(GUIDANCE)
        assert real.text.count(GUIDANCE) == 1
        async with maker() as session:
            for generation, received in [(2, None), (3, old.text)]:
                refused = await _acquire(session, request, generation, received)
                assert (
                    isinstance(refused, dispatch.DispatchConflict)
                    and refused.code == "not_published"
                )
            row = await session.get(ExecutionRequest, request, populate_existing=True)
            assert row is not None and row.start_deferrals == 0 and row.acquired_generation is None
            assert row.dispatch_due < row.wait_deadline
            assert (await _state(session, key)).applied_at is None
            stale = await _acquire(session, request, 2, real.text)
            assert isinstance(stale, dispatch.DispatchConflict) and stale.code == "not_published"
            grant = await _acquire(session, request, 4, real.text)
            assert isinstance(grant, dispatch.AcquireGrant)
            later = await _record(session, item, "Guidance for the next request.")
            renewal = await _acquire(session, request, 4, real.text)
            assert isinstance(renewal, dispatch.AcquireGrant)
            assert (await _state(session, later)).execution_request_id is None
            for contents, owner in [(None, OWNER), (real.text, "other-worker")]:
                refused_start = await _start(
                    session, request, grant, received=contents, owner=owner
                )
                assert isinstance(refused_start, dispatch.DispatchConflict)
            assert (await _state(session, key)).applied_at is None
            await dispatch.defer(
                session,
                request,
                owner=OWNER,
                generation=4,
                capacity=True,
                reason="capacity",
                start_failure_detail=None,
            )
            rebuilt = await loop._execute_turn(request, 5)
            assert rebuilt is not None and rebuilt.text == real.text
            grant = await _acquire(session, request, 5, rebuilt.text)
            assert isinstance(grant, dispatch.AcquireGrant)
            started = await _start(session, request, grant, received=rebuilt.text)
            assert isinstance(started, dispatch.StartResult)
            applied = await _state(session, key)
            assert applied.state == "applied" and applied.execution_request_id == request
            assert applied.applied_at is not None
        async with maker() as restarted:
            replay = await _start(restarted, request, grant, received=real.text)
            assert isinstance(replay, dispatch.DispatchConflict)
            assert (await _state(restarted, key)).applied_at == applied.applied_at
            assert (await _state(restarted, later)).execution_request_id is None

    _run(steps, runs_stream)


@pytest.mark.parametrize("embedded", [True, False])
def test_cancel_settles_unconsumed_guidance_even_after_an_empty_selection(
    clean_db: None,
    allowlisted: None,  # noqa: F811
    runs_stream: str,
    valkey: redis.Redis,
    embedded: bool,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession], loop: WorkItemReconciler, _bus: aioredis.Redis
    ) -> None:
        async with maker() as session:
            item, request = await _admit(session)
            if embedded:
                key = await _record(session, item, OBJECTIVE, request)
                assert isinstance(await _acquire(session, request, 1), dispatch.DispatchConflict)
                turn = await loop._execute_turn(request, 2)
                assert turn is not None and turn.text == OBJECTIVE
                grant = await _acquire(session, request, 2, turn.text)
                assert isinstance(grant, dispatch.AcquireGrant)
            else:
                grant = await _acquire(session, request, 1)
                assert isinstance(grant, dispatch.AcquireGrant)
                key = await _record(session, item, GUIDANCE)
                await session.execute(
                    text("""
                    UPDATE curie.execution_requests SET acquire_expires_at = clock_timestamp()
                    WHERE id = :id
                """),
                    {"id": request},
                )
                await session.commit()
                assert await dispatch.redispatch_lapsed_acquisitions(session) == 1
                grant = await _acquire(session, request, 2)
                assert isinstance(grant, dispatch.AcquireGrant)
                assert (await _state(session, key)).execution_request_id is None
            await loop._redrive_steering()
            assert (await _state(session, key)).state == "queued"
            version = await session.scalar(
                text("SELECT version FROM curie.work_items WHERE id=:id"), {"id": item}
            )
            await dispatch.cancel(session, work_item_id=item, expected_version=version)
        await loop._redrive_steering()
        async with maker() as session:
            settled = await _state(session, key)
            assert settled.refusal_code == "work_item_cancelled" and settled.applied_at is None
        await loop._redrive_steering()
        async with maker() as session:
            assert (await _state(session, key)).refused_at == settled.refused_at

    _run(steps, runs_stream)


def test_oversized_and_expired_guidance_leave_an_unguided_start_available(
    clean_db: None,
    allowlisted: None,  # noqa: F811
    runs_stream: str,
    valkey: redis.Redis,
) -> None:
    async def steps(
        maker: async_sessionmaker[AsyncSession], loop: WorkItemReconciler, _bus: aioredis.Redis
    ) -> None:
        async with maker() as session:
            item, request = await _admit(session)
            oversized = await _record(session, item, "x" * 65536)
            expired = uuid4()
            await session.execute(
                text("""
                INSERT INTO curie.factory_steering_ledger
                  (steering_id, work_item_id, source_identity, source_channel, source_root_ts,
                   source_facts, first_delivery_id, source_content_sha256, accepted_text_sha256,
                   accepted_text, created_at)
                SELECT :expired, work_item_id, source_identity, source_channel, source_root_ts,
                       source_facts, 'earlier', source_content_sha256, accepted_text_sha256,
                       accepted_text, clock_timestamp() - interval '25 hours'
                FROM curie.factory_steering_ledger WHERE steering_id = :oversized
            """),
                {"expired": expired, "oversized": oversized},
            )
            await session.commit()
            grant = await _acquire(session, request, 1)
            assert isinstance(grant, dispatch.AcquireGrant)
            assert (await _state(session, expired)).refusal_code == "guidance_expired"
            assert (await _state(session, oversized)).refusal_code == "objective_too_large"
            wake = await loop._execute_turn(request, 1)
            assert wake is not None and wake.text == OBJECTIVE
            assert isinstance(
                await _start(session, request, grant, received=None), dispatch.StartResult
            )

    _run(steps, runs_stream)
