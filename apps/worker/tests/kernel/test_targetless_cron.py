"""Targetless cron turns: executed on the hook agent's active deployment (#2963).

A targetless turn is a CRON ``QueuedTurn`` with ``reply_handle=None`` and a
complete ``hook_run``. The worker routes it by the hook run's agent, never by a
channel binding, runs it on that agent's active deployment, emits nothing to
any reply sink, records the hook outcome, and marks the event done without a
completion outbox record.

Real kernel, real ``BindingResolver`` over real Postgres, real Valkey markers;
only the runner, the Kubernetes client and the reply sink are fakes. Every row
seeded here is created with fresh uuids and deleted by the test that made it.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from aci_protocol import (
    ErrorEvent,
    Final,
    HookRunRef,
    QueuedTurn,
    SessionStatus,
    TurnSource,
)
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.binding import (
    BUNDLE_REF_ENV,
    DECISION_ENV,
    GRANT_TOOL_ENV,
    MEMORY_REF_ENV,
    RESUMED_KIND_ENV,
    BindingResolver,
)
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.killswitch import kill_key
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

PROMPT = "run the nightly report"


def _targetless(
    ref: HookRunRef | None,
    *,
    event_id: str | None = None,
    source: TurnSource = TurnSource.CRON,
) -> QueuedTurn:
    """A targetless turn built without wire validation.

    ``model_construct`` so the shape-violation tests can hand the worker exactly
    what a direct caller could, and the worker's own re-check is what is tested.
    """
    return QueuedTurn.model_construct(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=f"cron-{uuid.uuid4().hex}",
        author="cron",
        text=PROMPT,
        reply_handle=None,
        received_at="2026-09-22T03:00:00+00:00",
        source=source,
        attachments=[],
        hook_run=ref,
    )


@dataclass
class _Deployments:
    winning_bundle: str
    decoy_bundle: str


@asynccontextmanager
async def _seed_deployments(
    engine: AsyncEngine, agent_id: uuid.UUID, *, with_decoy: bool = True
) -> AsyncIterator[_Deployments]:
    """Three active deployments for the hook agent, plus a channel-bound decoy.

    Hook agent: a dev deployment deployed most recently, an older prod, and a
    newer prod. The resolver order (prod first, then deployed_at DESC) makes the
    newer prod the only correct winner; the dev row catches a query that orders
    by recency alone, the older prod one that ignores recency. The hook agent
    has NO agent_channels row. The decoy agent has a channel binding and its own
    active prod deployment, so a query that joins any binding would pick it.
    """
    token = uuid.uuid4().hex
    now = datetime.now(UTC)
    version_ids: list[uuid.UUID] = []
    deployment_ids: list[uuid.UUID] = []
    decoy_agent = uuid.uuid4()
    bundles = {
        "dev_newest": f"bundles/{token}/dev-newest.zip",
        "prod_old": f"bundles/{token}/prod-old.zip",
        "prod_new": f"bundles/{token}/prod-new.zip",
        "decoy": f"bundles/{token}/decoy.zip",
    }

    async def add(
        conn: object, owner: uuid.UUID, label: str, env: str, at: datetime
    ) -> None:
        version_id = uuid.uuid4()
        deployment_id = uuid.uuid4()
        version_ids.append(version_id)
        deployment_ids.append(deployment_id)
        await conn.execute(  # type: ignore[attr-defined]
            text(
                "INSERT INTO curie.agent_versions "
                "(id, agent_id, version_label, bundle_ref, created_by) "
                "VALUES (:id, :agent_id, :label, :ref, 'kernel-test')"
            ),
            {
                "id": version_id,
                "agent_id": owner,
                "label": f"{label}_{token}",
                "ref": bundles[label],
            },
        )
        await conn.execute(  # type: ignore[attr-defined]
            text(
                "INSERT INTO curie.deployments "
                "(id, agent_id, version_id, environment, status, deployed_at) "
                "VALUES (:id, :agent_id, :version_id, "
                "CAST(:env AS curie.environment), 'active', :at)"
            ),
            {
                "id": deployment_id,
                "agent_id": owner,
                "version_id": version_id,
                "env": env,
                "at": at,
            },
        )

    try:
        async with engine.begin() as conn:
            await add(conn, agent_id, "prod_old", "prod", now - timedelta(hours=3))
            await add(conn, agent_id, "prod_new", "prod", now - timedelta(hours=2))
            await add(conn, agent_id, "dev_newest", "dev", now - timedelta(minutes=5))
            if with_decoy:
                await conn.execute(
                    text("INSERT INTO curie.agents (id, name) VALUES (:id, :name)"),
                    {"id": decoy_agent, "name": f"decoy_{token}"},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_channels "
                        "(id, agent_id, kind, address) "
                        "VALUES (:id, :agent_id, 'slack', :address)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "agent_id": decoy_agent,
                        "address": f"C{token[:10].upper()}",
                    },
                )
                await add(conn, decoy_agent, "decoy", "prod", now)
        yield _Deployments(bundles["prod_new"], bundles["decoy"])
    finally:
        async with engine.begin() as conn:
            for deployment_id in deployment_ids:
                await conn.execute(
                    text("DELETE FROM curie.deployments WHERE id = :id"),
                    {"id": deployment_id},
                )
            for version_id in version_ids:
                await conn.execute(
                    text("DELETE FROM curie.agent_versions WHERE id = :id"),
                    {"id": version_id},
                )
            await conn.execute(
                text("DELETE FROM curie.agents WHERE id = :id"), {"id": decoy_agent}
            )


def _resolver_factory(engine: AsyncEngine):  # type: ignore[no-untyped-def]
    return lambda config: BindingResolver(engine, config)


async def _owned_keys(h: object) -> set[str]:
    return {
        key
        async for key in h.async_redis.scan_iter(  # type: ignore[attr-defined]
            match=f"{h.config.key_prefix}:*"  # type: ignore[attr-defined]
        )
    }


async def _has_outbox_record(h: object, event_id: str) -> bool:
    redis = h.async_redis  # type: ignore[attr-defined]
    config = h.config  # type: ignore[attr-defined]
    if await redis.exists(config.completion_key(event_id)):
        return True
    return bool(await redis.sismember(config.completions_pending_key(), event_id))


# --- 1. executes on the intended active deployment ---------------------------


def test_targetless_cron_runs_on_the_hook_agents_winning_deployment(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ) as seeded, make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            h.runner.default_script = [Final(text="report done", status=SessionStatus.DONE)]
            event = _targetless(run.ref)

            await h.kernel.process_event(event)

            assert h.runner.opened == [PROMPT]
            assert len(h.fake_k8s.claim_envs) == 1
            env = h.fake_k8s.claim_envs[0]
            assert env is not None
            assert env[BUNDLE_REF_ENV] == seeded.winning_bundle
            assert env[BUNDLE_REF_ENV] != seeded.decoy_bundle
            assert f"/agents/{run.agent_id}/" in env[MEMORY_REF_ENV]
            assert h.sink.events == []
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "ran"
            assert ended_at is not None
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert not await _has_outbox_record(h, event.event_id)

            # Redelivery of the same event is a no-op: the runner is not reopened.
            await h.kernel.process_event(event)

            assert h.runner.opened == [PROMPT]
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.sink.events == []
            assert not await _has_outbox_record(h, event.event_id)

    asyncio.run(go())


# --- 2. unknown agent ---------------------------------------------------------


def test_targetless_cron_for_an_unknown_agent_is_dropped(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            unknown = HookRunRef(
                agent_id=str(uuid.uuid4()),
                name=run.ref.name,
                slot_utc=run.ref.slot_utc,
            )
            event = _targetless(unknown)

            await h.kernel.process_event(event)

            assert h.runner.opened == []
            assert h.fake_k8s.claim_envs == []
            assert h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert not await _has_outbox_record(h, event.event_id)
            assert await run.state() == (None, None)

    asyncio.run(go())


# --- 3. agent without an active deployment ----------------------------------


def test_targetless_cron_without_an_active_deployment_fails_the_hook_run(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            event = _targetless(run.ref)

            await h.kernel.process_event(event)

            assert h.runner.opened == []
            assert h.fake_k8s.claim_envs == []
            assert h.sink.events == []
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert not await _has_outbox_record(h, event.event_id)

    asyncio.run(go())


# --- 4. invalid identity and shape violations -------------------------------


def test_targetless_cron_with_a_non_uuid_agent_is_dropped_without_effects(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            invalid = HookRunRef.model_construct(
                agent_id="not-a-uuid",
                name=run.ref.name,
                slot_utc=run.ref.slot_utc,
            )

            await h.kernel.process_event(_targetless(invalid))

            assert h.runner.opened == []
            assert h.fake_k8s.claim_envs == []
            assert h.sink.events == []
            assert await run.state() == (None, None)

    asyncio.run(go())


@pytest.mark.parametrize(
    ("source", "with_hook_run"),
    [
        pytest.param(TurnSource.SLACK, True, id="no-handle-on-a-slack-turn"),
        pytest.param(TurnSource.WEBHOOK, True, id="no-handle-on-a-webhook-turn"),
        pytest.param(TurnSource.CRON, False, id="no-handle-cron-without-hook-run"),
    ],
)
def test_targetless_shape_violation_is_rejected_before_any_effect(
    make_harness, make_hook_run, source: TurnSource, with_hook_run: bool
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            event = _targetless(run.ref if with_hook_run else None, source=source)
            before = await _owned_keys(h)

            with pytest.raises(ValueError):
                await h.kernel.process_event(event)

            assert await _owned_keys(h) == before
            assert h.runner.opened == []
            assert h.fake_k8s.claim_envs == []
            assert h.sink.events == []
            assert await run.state() == (None, None)

            await h.kernel.notify_turn_not_started(event)

            assert h.sink.events == []

    asyncio.run(go())


# --- 5. runner failure --------------------------------------------------------


@pytest.mark.parametrize("failure", ["classified", "exhausted"])
def test_targetless_runner_failure_fails_the_hook_run_silently(
    make_harness, make_hook_run, failure: str
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
            max_attempts=2,
        ) as h:
            if failure == "classified":
                h.runner.turn_scripts = [
                    [
                        ErrorEvent(message="bad input", classification="runner-error"),
                        Final(text="failed", status=SessionStatus.CLASSIFIED_FAILURE),
                    ],
                    [Final(text="must not run", status=SessionStatus.DONE)],
                ]
            else:
                # Every attempt is refused by the runner: retries exhaust.
                h.runner.event_fail_times = 10
            event = _targetless(run.ref)

            await h.kernel.process_event(event)

            assert h.runner.opened
            assert all(opened == PROMPT for opened in h.runner.opened)
            if failure == "classified":
                assert h.runner.opened == [PROMPT]
            assert h.sink.events == []
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None
            assert not await _has_outbox_record(h, event.event_id)

    asyncio.run(go())


# --- 6. approval gate --------------------------------------------------------


def test_targetless_approval_gate_is_not_bypassed_and_fails_the_hook_run(
    make_harness, make_hook_run
) -> None:
    class RecordingApprovals:
        def __init__(self) -> None:
            self.requests: list[ApprovalRequest] = []

        async def create(self, request: ApprovalRequest) -> CreatedApproval:
            self.requests.append(request)
            return CreatedApproval(id=f"appr-{uuid.uuid4().hex}", status="pending")

    async def go() -> None:
        approvals = RecordingApprovals()
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
            approvals=approvals,
        ) as h:
            h.runner.default_script = [
                Final(
                    text="Requesting sign off",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Tool call awaiting approval: deploy",
                )
            ]
            event = _targetless(run.ref)

            await h.kernel.process_event(event)

            assert h.runner.opened == [PROMPT]
            assert approvals.requests == []
            assert h.sink.events == []
            assert h.sink.posts == []
            outcome, ended_at = await run.state() or (None, None)
            assert outcome == "failed"
            assert ended_at is not None
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert not await _has_outbox_record(h, event.event_id)

    asyncio.run(go())


# --- 7. kill switch ------------------------------------------------------------


def test_targetless_cron_for_a_killed_agent_records_blocked(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
            with_killswitch=True,
        ) as h:
            key = kill_key(run.agent_id)
            await h.async_redis.set(key, "1")
            try:
                event = _targetless(run.ref)

                await h.kernel.process_event(event)

                assert h.runner.opened == []
                assert h.fake_k8s.claim_envs == []
                assert h.sink.events == []
                outcome, ended_at = await run.state() or (None, None)
                assert outcome == "blocked"
                assert ended_at is not None
                assert not await _has_outbox_record(h, event.event_id)
            finally:
                await h.async_redis.delete(key)

    asyncio.run(go())


# --- 8. no resume authority -------------------------------------------------


def test_targetless_turn_with_a_resume_event_id_gets_no_authority(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            event = _targetless(
                run.ref, event_id=f"approval-{uuid.uuid4()}-resolved"
            )

            await h.kernel.process_event(event)

            assert h.runner.opened == []
            for env in h.fake_k8s.claim_envs:
                assert env is None or not (
                    {GRANT_TOOL_ENV, RESUMED_KIND_ENV, DECISION_ENV} & env.keys()
                )
            assert h.fake_k8s.claim_envs == []
            assert h.sink.events == []
            assert await run.state() == (None, None)

    asyncio.run(go())


# --- 9. fenced lease ------------------------------------------------------------


async def _owned_lease(h: object):  # type: ignore[no-untyped-def]
    redis = h.async_redis  # type: ignore[attr-defined]
    config = h.config  # type: ignore[attr-defined]
    await redis.xgroup_create(
        config.stream, config.consumer_group, id="0", mkstream=True
    )
    entry_id = await redis.xadd(config.stream, {"payload": "owned"})
    await redis.xreadgroup(
        config.consumer_group,
        config.consumer_name,
        {config.stream: ">"},
        count=1,
    )
    store = DeliveryLeaseStore(redis, config)
    lease = await store.acquire(
        config.stream,
        config.consumer_group,
        entry_id,
        consumer=config.consumer_name,
    )
    return store, entry_id, lease


def test_targetless_run_under_a_valid_lease_settles_done_without_outbox(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            _store, _entry_id, lease = await _owned_lease(h)
            h.runner.default_script = [Final(text="done", status=SessionStatus.DONE)]
            event = _targetless(run.ref)

            await h.kernel.process_event(event, lease=lease)

            assert h.runner.opened == [PROMPT]
            assert h.sink.events == []
            assert await h.async_redis.exists(h.config.done_key(event.event_id))
            assert not await _has_outbox_record(h, event.event_id)
            outcome, _ended = await run.state() or (None, None)
            assert outcome == "ran"

    asyncio.run(go())


def test_targetless_run_under_a_lost_lease_writes_no_done_marker(
    make_harness, make_hook_run
) -> None:
    async def go() -> None:
        async with make_hook_run() as run, _seed_deployments(
            run.engine, run.agent_id
        ), make_harness(
            hook_runs=run.recorder(),
            binding_factory=_resolver_factory(run.engine),
        ) as h:
            store, entry_id, lease = await _owned_lease(h)
            await store.release(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                owner=lease.owner,
            )
            lease.lost.set()
            h.runner.default_script = [Final(text="done", status=SessionStatus.DONE)]
            event = _targetless(run.ref)

            await h.kernel.process_event(event, lease=lease)

            assert h.sink.events == []
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            assert not await _has_outbox_record(h, event.event_id)

    asyncio.run(go())

