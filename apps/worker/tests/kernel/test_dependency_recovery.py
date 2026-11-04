"""Workspace dependency recovery and durable escalation ordering (#5365)."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus, TurnSource
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.consumer import Consumer
from curie_worker.kernel.failures import failure_class_from_reply
from curie_worker.sandbox import QuotaRejection
from curie_worker.workspace import (
    WorkspaceClaimCoordinator,
    WorkspaceCredentialClient,
    WorkspacePreparationError,
)

from .test_consumer import _capacity_consumer, _pending_local_entry, _qevent
from .test_kernel import _workspace_binding


async def _deliver(h: Any, consumer: Consumer, event: Any) -> tuple[str, dict[str, str]]:
    entry, fields = await _pending_local_entry(h, event)
    await consumer._sem.acquire()
    await consumer._handle(entry, fields)
    return entry, fields


async def _wake(h: Any, consumer: Consumer, event_id: str) -> tuple[str, dict[str, str]]:
    await h.async_redis.zadd(consumer._waits._due, {event_id: 0})
    assert await consumer._waits.wake_due() == 1
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    assert len(rows[0][1]) == 1
    return rows[0][1][0]


async def _park(h: Any, consumer: Consumer, event: Any) -> None:
    h.fake_k8s.quota_rejection = QuotaRejection(
        quota_name="acme-quota",
        requested={"pods": "1"},
        used={"pods": "1"},
        hard={"pods": "1"},
    )
    await _deliver(h, consumer, event)
    record = await consumer._waits.get(event.event_id)
    assert record is not None and record.state == "waiting"
    h.fake_k8s.quota_rejection = None


@asynccontextmanager
async def _selection_server():
    deployment = uuid.uuid4()
    state = {"status": 200}

    async def select(_request: web.Request) -> web.Response:
        state["calls"] = state.get("calls", 0) + 1
        if state.get("fail_after", 100000) <= state["calls"] <= state.get("recover_after", 0):
            return web.Response(status=503)
        if state["status"] != 200:
            return web.Response(status=state["status"])
        return web.json_response({"repo_full_name": None})

    app = web.Application()
    app.router.add_post(f"/v1/internal/workspaces/{deployment}/selection", select)
    async with TestServer(app) as server:
        yield (
            deployment,
            WorkspaceCredentialClient(
                api_url=str(server.make_url("")),
                worker_token="example-worker-token",
            ),
            state,
        )


@pytest.mark.parametrize("park_first", [True, False])
@pytest.mark.parametrize("recovery", ["sweep", "redelivery"])
@pytest.mark.parametrize("placeholder", ["p-1", None])
def test_failed_escalation_is_replayed_without_execution_or_overtaking(
    make_harness,
    recovery: str,
    placeholder: str | None,
    park_first: bool,
) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            knobs = dict(
                slack_no_edit_streaming=True,
                max_attempts=1,
                claim_timeout_seconds=0.02,
                completion_sweep_grace_s=0,
                binding=_workspace_binding(deployment),
            )
            async with make_harness(**knobs) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                first = _qevent("first", event_id="failed-original", placeholder=placeholder)
                if park_first:
                    await _park(h, consumer, first)
                state["status"] = 422
                h.sink.fail_events.add("reply.update")
                entry, fields = (
                    await _wake(h, consumer, first.event_id)
                    if park_first
                    else await _pending_local_entry(h, first)
                )
                if recovery == "redelivery":
                    async with consumer._delivery_lease(entry, fields) as lease:
                        assert lease is not None
                        from curie_worker.capacity_wait import (
                            WAIT_GENERATION_FIELD,
                            wait_delivery_scope,
                        )

                        with (
                            wait_delivery_scope(
                                consumer._waits,
                                first.event_id,
                                int(fields[WAIT_GENERATION_FIELD]),
                                lease,
                            )
                            if park_first
                            else nullcontext()
                        ):
                            await h.kernel.process_event(first, lease=lease)
                else:
                    await consumer._sem.acquire()
                    await consumer._handle(entry, fields)
                assert await h.kernel._markers.is_terminal(first.event_id)
                stored = await h.kernel._markers.read_completion(first.event_id)
                assert stored is not None and stored.done_flag
                assert stored.record.answer is not None
                assert failure_class_from_reply(stored.record.answer.text) == "workspace-error"
                assert h.runner.opened == []
                async with make_harness(**knobs) as peer:
                    peer.kernel._workspace = WorkspaceClaimCoordinator(
                        preparer=SimpleNamespace(credentials=credentials),
                        substrate=peer.substrate,
                    )
                    state["status"] = 200
                    replacement = _capacity_consumer(peer)
                    second = _qevent("second", event_id="later", placeholder="p-2")
                    await _deliver(peer, replacement, second)
                    assert peer.runner.opened == []
                    assert not await peer.kernel._markers.is_terminal(second.event_id)
                    if recovery == "redelivery":
                        await replacement._sem.acquire()
                        await replacement._handle(entry, fields)
                    else:
                        await peer.kernel.sweep_pending_completions()
                    assert peer.runner.opened == []
                    assert peer.sink.completions[-1].outcome == "escalated"
                    assert failure_class_from_reply(peer.sink.last_text) == "workspace-error"
                    assert await peer.kernel._markers.read_completion(first.event_id) is None
                    peer.runner.default_script = [
                        Final(text="later answer", status=SessionStatus.DONE)
                    ]
                    later_entry, later_fields = await _wake(peer, replacement, second.event_id)
                    await replacement._sem.acquire()
                    await replacement._handle(later_entry, later_fields)
                    assert peer.runner.opened == ["second"]
                    assert peer.sink.last_text == "later answer"

    asyncio.run(go())


@pytest.mark.parametrize("outage_s", [0.03])
def test_capacity_wake_recovers_from_workspace_outage_without_escalating(
    make_harness,
    outage_s: float,
) -> None:
    async def go() -> None:
        deployment = uuid.uuid4()
        unavailable_until = 0.0
        outage_active = False
        failed_reads = 0

        async def selection(_request: web.Request) -> web.Response:
            nonlocal failed_reads
            if outage_active and (failed_reads < 4 or time.monotonic() < unavailable_until):
                failed_reads += 1
                return web.Response(status=503)
            return web.json_response({"repo_full_name": None})

        app = web.Application()
        app.router.add_post(f"/v1/internal/workspaces/{deployment}/selection", selection)
        async with TestServer(app) as server:
            credentials = WorkspaceCredentialClient(
                api_url=str(server.make_url("")),
                worker_token="example-worker-token",
            )
            async with make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                claim_timeout_seconds=0.02,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent("original", event_id="dependency-original")
                await _park(h, consumer, event)
                original = await consumer._waits.get(event.event_id)
                assert original is not None
                outage_active = True
                unavailable_until = time.monotonic() + outage_s
                h.runner.default_script = [Final(text="recovered", status=SessionStatus.DONE)]
                for _ in range(10000):
                    entry, fields = await _wake(h, consumer, event.event_id)
                    await consumer._sem.acquire()
                    await consumer._handle(entry, fields)
                    record = await consumer._waits.get(event.event_id)
                    assert record is not None and record.deadline_ms == original.deadline_ms
                    if record.state == "done":
                        break
                    assert record.state == "waiting"
                    assert h.runner.opened == []
                    assert not await h.kernel._markers.is_terminal(event.event_id)
                else:
                    pytest.fail("the dependency recovery did not finish")
                assert failed_reads >= 4
                assert h.runner.opened == ["original"]
                assert h.sink.last_text == "recovered"
                assert len(h.sink.completions) == 1
                assert all(failure_class_from_reply(text) is None for _, _, text in h.sink.updates)

    asyncio.run(go())


def test_dependency_wait_has_one_fixed_workspace_deadline(make_harness) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            async with make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                state["status"] = 503
                event = _qevent("original", event_id="dependency-deadline")
                await _deliver(h, consumer, event)
                first = await consumer._waits.get(event.event_id)
                assert first is not None and first.state == "waiting"
                assert first.dependency_deadline_ms is not None
                assert 0 < first.dependency_deadline_ms - first.first_wait_ms <= 150000
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
                again = await consumer._waits.get(event.event_id)
                assert again is not None and again.state == "waiting"
                assert again.dependency_deadline_ms == first.dependency_deadline_ms
                assert again.deadline_ms == first.deadline_ms
                await h.async_redis.hset(
                    consumer._waits._record(event.event_id),
                    "dependency_deadline_ms",
                    "1",
                )
                requests = state["calls"]
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
                assert state["calls"] == requests
                assert h.runner.opened == []
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert failure_class_from_reply(h.sink.last_text) == "workspace-error"
                assert "retry deadline" in h.sink.last_text
                assert h.sink.completions[-1].outcome == "escalated"

    asyncio.run(go())


def test_granted_capacity_wake_retries_dependency_without_reparking(make_harness) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            async with make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                claim_timeout_seconds=0.02,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent("original", event_id="granted-dependency")
                await _park(h, consumer, event)
                state.update(fail_after=3, recover_after=6)
                h.runner.turn_scripts = [
                    [
                        ErrorEvent(
                            classification="runner-error", message="temporary model failure"
                        ),
                        Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
                    ],
                    [Final(text="recovered active turn", status=SessionStatus.DONE)],
                ]
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
                assert state["calls"] >= 7
                record = await consumer._waits.get(event.event_id)
                assert record is not None and record.state == "done"
                assert record.deferrals == 1
                assert record.grant_confirmed
                assert h.runner.opened == ["original", "original"]
                assert h.sink.last_text == "recovered active turn"
                assert len(h.sink.completions) == 1

    asyncio.run(go())


def test_expiry_notice_blocks_successor_until_repaired(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.02,
        ) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            first = _qevent("first", event_id="expiry-original")
            await _park(h, consumer, first)
            await h.async_redis.hset(consumer._waits._record(first.event_id), "deadline_ms", "1")
            h.sink.fail_events.add("reply.update")
            entry, fields = await _wake(h, consumer, first.event_id)
            await consumer._sem.acquire()
            await consumer._handle(entry, fields)
            expired = await consumer._waits.get(first.event_id)
            assert expired is not None and expired.expiry_notice_pending
            assert await h.kernel._markers.is_terminal(first.event_id)
            second = _qevent("second", event_id="expiry-successor", placeholder="p-2")
            await _deliver(h, consumer, second)
            assert h.runner.opened == []
            h.sink.fail_events.clear()
            await h.async_redis.zadd(consumer._waits._expiry_notices, {first.event_id: 0})
            await consumer._repair_expiry_notices()
            repaired = await consumer._waits.get(first.event_id)
            assert repaired is not None and not repaired.expiry_notice_pending
            h.runner.default_script = [Final(text="later answer", status=SessionStatus.DONE)]
            entry, fields = await _wake(h, consumer, second.event_id)
            await consumer._sem.acquire()
            await consumer._handle(entry, fields)
            assert h.runner.opened == ["second"]
            assert h.sink.last_text == "later answer"

    asyncio.run(go())


@pytest.mark.parametrize("lane", ["independent-thread", "webhook", "approval-resume", "pruned"])
def test_thread_gate_preserves_other_lanes_and_pruned_wait_liveness(
    make_harness, lane: str
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.02,
        ) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            first = _qevent("first", event_id="parked-first")
            await _park(h, consumer, first)
            from curie_worker.kernel.routing import _thread_key_for

            order_key = h.config.capacity_thread_order_key(_thread_key_for(first))
            assert await h.async_redis.pttl(order_key) > 0
            successor = _qevent(
                "second",
                event_id="approval-gate-resolved" if lane == "approval-resume" else "later",
                source=TurnSource.WEBHOOK if lane == "webhook" else TurnSource.SLACK,
            )
            if lane == "independent-thread":
                successor = successor.model_copy(update={"conversation_id": "other-thread"})
            elif lane == "pruned":
                await h.async_redis.delete(consumer._waits._record(first.event_id))
            h.runner.default_script = [Final(text="second answer", status=SessionStatus.DONE)]
            await _deliver(h, consumer, successor)
            assert h.runner.opened == ["second"]
            assert h.sink.last_text == "second answer"
            assert not await h.kernel._markers.is_terminal(first.event_id)
            if lane == "pruned":
                assert await h.async_redis.zcard(order_key) == 0

    asyncio.run(go())


@pytest.mark.parametrize("stage", ["repository-selection", "credential-redemption"])
@pytest.mark.parametrize("status", [500, 503, 422])
def test_workspace_dependency_classification_covers_selection_and_redemption(
    stage: str,
    status: int,
) -> None:
    from curie_worker.workspace import WorkspaceDependencyUnavailable

    async def go() -> None:
        deployment = uuid.uuid4()
        app = web.Application()

        async def unavailable(_request: web.Request) -> web.Response:
            return web.Response(status=status)

        suffix = "selection" if stage == "repository-selection" else "credential"
        app.router.add_post(f"/v1/internal/workspaces/{deployment}/{suffix}", unavailable)
        async with TestServer(app) as server:
            credentials = WorkspaceCredentialClient(
                api_url=str(server.make_url("")),
                worker_token="example-worker-token",
            )
            with pytest.raises(WorkspacePreparationError) as raised:
                if stage == "repository-selection":
                    await asyncio.to_thread(
                        credentials.select, deployment, "thread", "author", None
                    )
                else:
                    await asyncio.to_thread(credentials.redeem, deployment, "thread")
            assert raised.value.stage == stage
            assert isinstance(raised.value, WorkspaceDependencyUnavailable) == (
                status in {500, 503}
            )

    asyncio.run(go())
