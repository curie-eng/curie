"""Workspace dependency recovery and durable escalation ordering (#5365)."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
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

from .test_consumer import _capacity_consumer, _pending_local_entry, _qevent, _wait_until
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


@pytest.mark.parametrize("recovery", ["capacity", "grant"])
def test_recovered_dependency_deadline_does_not_expire_a_healthy_wait(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    recovery: str,
) -> None:
    from curie_worker.kernel import clock

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
                state["status"] = 503
                event = _qevent("original", event_id="recovered-dependency")
                await _deliver(h, consumer, event)
                first = await consumer._waits.get(event.event_id)
                assert first is not None and first.dependency_deadline_ms is not None
                state["status"] = 200
                h.runner.default_script = [
                    Final(text="healthy recovery", status=SessionStatus.DONE)
                ]
                if recovery == "capacity":
                    h.fake_k8s.quota_rejection = QuotaRejection(
                        quota_name="acme-quota",
                        requested={"pods": "1"},
                        used={"pods": "1"},
                        hard={"pods": "1"},
                    )
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
                recovered = await consumer._waits.get(event.event_id)
                assert recovered is not None
                assert recovered.dependency_deadline_ms is None
                assert not await h.async_redis.hexists(
                    consumer._waits._record(event.event_id),
                    "dependency_first_ms",
                )
                if recovery == "capacity":
                    assert recovered.state == "waiting" and h.runner.opened == []
                    assert recovered.deadline_ms == first.deadline_ms
                    h.fake_k8s.quota_rejection = None
                    monkeypatch.setattr(
                        clock,
                        "time",
                        SimpleNamespace(
                            time=lambda: first.dependency_deadline_ms / 1000 + 1,
                            monotonic=time.monotonic,
                        ),
                    )
                    entry, fields = await _wake(h, consumer, event.event_id)
                    await consumer._sem.acquire()
                    await consumer._handle(entry, fields)
                assert h.runner.opened == ["original"]
                assert h.sink.last_text == "healthy recovery"
                assert h.sink.completions[-1].outcome == "delivered"

    asyncio.run(go())


@pytest.mark.parametrize("separate_kernel", [False, True])
def test_followup_cannot_overtake_an_initial_dependency_failure(
    make_harness,
    separate_kernel: bool,
) -> None:
    async def go() -> None:
        deployment = uuid.uuid4()
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        calls = 0

        async def selection(_request: web.Request) -> web.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_entered.set()
                await release_first.wait()
                return web.Response(status=503)
            return web.json_response({"repo_full_name": None})

        app = web.Application()
        app.router.add_post(f"/v1/internal/workspaces/{deployment}/selection", selection)
        async with TestServer(app) as server, AsyncExitStack() as stack:
            knobs = dict(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                claim_timeout_seconds=0.02,
            )
            h = await stack.enter_async_context(make_harness(**knobs))
            peer = await stack.enter_async_context(make_harness(**knobs)) if separate_kernel else h
            credentials = WorkspaceCredentialClient(
                api_url=str(server.make_url("")),
                worker_token="example-worker-token",
            )
            for target in (h, peer):
                target.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=target.substrate,
                )
            first_consumer = _capacity_consumer(h)
            second_consumer = _capacity_consumer(peer)
            await first_consumer.ensure_group()
            first = _qevent("first", event_id="initial-first")
            second = _qevent("second", event_id="initial-second", placeholder="p-2")
            first_task = asyncio.create_task(_deliver(h, first_consumer, first))
            second_task = None
            try:
                await asyncio.wait_for(first_entered.wait(), timeout=2)
                second_task = asyncio.create_task(_deliver(peer, second_consumer, second))
                await asyncio.wait_for(asyncio.shield(second_task), timeout=1)
                assert h.runner.opened == peer.runner.opened == []
                waiting = await second_consumer._waits.get(second.event_id)
                assert waiting is not None and waiting.state == "waiting"
                release_first.set()
                await first_task
                h.runner.default_script = [Final(text="first answer", status=SessionStatus.DONE)]
                entry, fields = await _wake(h, first_consumer, first.event_id)
                await first_consumer._sem.acquire()
                await first_consumer._handle(entry, fields)
                assert h.runner.opened == ["first"]
                assert h.sink.last_text == "first answer"
                for target in (h, peer):
                    target.runner.default_script = [
                        Final(text="second answer", status=SessionStatus.DONE)
                    ]
                entry, fields = await _wake(peer, second_consumer, second.event_id)
                await second_consumer._sem.acquire()
                await second_consumer._handle(entry, fields)
                executed = h.runner.opened + (peer.runner.opened if separate_kernel else [])
                assert executed == ["first", "second"]
                assert peer.sink.last_text == "second answer"
            finally:
                release_first.set()
                if second_task is not None and not second_task.done():
                    second_task.cancel()
                await asyncio.gather(
                    first_task, *([second_task] if second_task else []), return_exceptions=True
                )

    asyncio.run(go())


def test_registered_running_turn_still_accepts_a_followup_steer(make_harness, sync_redis) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=False, max_concurrency=2) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            hold = asyncio.Event()
            h.runner.default_script = []
            h.runner.hold = hold
            h.runner.tail = [Final(text="first answer", status=SessionStatus.DONE)]
            first = _qevent("first", event_id="steer-first")
            task = asyncio.create_task(_deliver(h, consumer, first))
            try:
                from curie_worker.kernel.routing import _thread_key_for

                def started() -> bool:
                    return bool(h.runner.queried) and not sync_redis.zcard(
                        h.config.capacity_thread_order_key(_thread_key_for(first)),
                    )

                await _wait_until(started)
                await _deliver(
                    h, consumer, _qevent("second", event_id="steer-second", placeholder="p-2")
                )
                assert h.runner.opened == ["first"]
                assert h.runner.steers == ["second"]
                assert h.sink.completions[-1].outcome == "delivered"
            finally:
                hold.set()
                await task

    asyncio.run(go())


@pytest.mark.parametrize("separate_kernel", [False, True])
def test_pending_stream_order_wins_when_the_later_delivery_registers_first(
    make_harness,
    separate_kernel: bool,
) -> None:
    async def go() -> None:
        async with AsyncExitStack() as stack:
            h = await stack.enter_async_context(make_harness(slack_no_edit_streaming=True))
            peer = (
                await stack.enter_async_context(make_harness(slack_no_edit_streaming=True))
                if separate_kernel
                else h
            )
            original = _capacity_consumer(h)
            later = _capacity_consumer(peer)
            await original.ensure_group()
            first = _qevent("first", event_id="unregistered-original")
            second = _qevent("second", event_id="registered-later", placeholder="p-2")
            first_entry, first_fields = await _pending_local_entry(h, first)
            second_entry, second_fields = await _pending_local_entry(peer, second)
            await later._sem.acquire()
            await later._handle(second_entry, second_fields)
            assert h.runner.opened == peer.runner.opened == []
            waiting = await later._waits.get(second.event_id)
            assert waiting is not None and waiting.state == "waiting"
            h.runner.default_script = [Final(text="first answer", status=SessionStatus.DONE)]
            await original._sem.acquire()
            await original._handle(first_entry, first_fields)
            assert h.runner.opened == ["first"]
            assert h.sink.last_text == "first answer"
            for target in (h, peer):
                target.runner.default_script = [
                    Final(text="second answer", status=SessionStatus.DONE)
                ]
            entry, fields = await _wake(peer, later, second.event_id)
            await later._sem.acquire()
            await later._handle(entry, fields)
            assert h.runner.opened + (peer.runner.opened if separate_kernel else []) == [
                "first",
                "second",
            ]
            assert peer.sink.last_text == "second answer"

    asyncio.run(go())


@pytest.mark.parametrize("lane", ["other-thread", "other-identity", "approval", "webhook"])
def test_unregistered_unrelated_or_exempt_pending_messages_do_not_block(
    make_harness, lane: str
) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            earlier = _qevent(
                "earlier",
                event_id="approval-pending-resolved"
                if lane == "approval"
                else "unregistered-other",
                thread="other-thread" if lane == "other-thread" else "th-1",
                adapter="other-app" if lane == "other-identity" else None,
                source=TurnSource.WEBHOOK if lane == "webhook" else TurnSource.SLACK,
            )
            await _pending_local_entry(h, earlier)
            h.runner.default_script = [Final(text="independent answer", status=SessionStatus.DONE)]
            await _deliver(h, consumer, _qevent("later", event_id="independent-later"))
            assert h.runner.opened == ["later"]
            assert h.sink.last_text == "independent answer"

    asyncio.run(go())


def test_pending_predecessor_scan_makes_bounded_progress_across_wakes(make_harness) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            for number in range(70):
                await _pending_local_entry(
                    h,
                    _qevent(
                        "unrelated",
                        thread=f"other-{number}",
                        event_id=f"unrelated-{number}",
                    ),
                )
            event = _qevent("later", event_id="scan-later")
            h.runner.default_script = [Final(text="scan completed", status=SessionStatus.DONE)]
            await _deliver(h, consumer, event)
            for _ in range(3):
                if h.runner.opened:
                    break
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
            assert h.runner.opened == ["later"]
            assert h.sink.last_text == "scan completed"
            pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert pending["pending"] == 70

    asyncio.run(go())
