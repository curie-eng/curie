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
from curie_worker.delivery_lease import DeliveryBudget
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
    state: dict[str, Any] = {"status": 200}

    async def select(_request: web.Request) -> web.Response:
        state["calls"] = state.get("calls", 0) + 1
        observer = state.get("before_response")
        if observer is not None:
            await observer(state)
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
                max_attempts=1,
                workspace_total_timeout_seconds=1,
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
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await asyncio.wait_for(consumer._handle(entry, fields), timeout=5)
                record = await consumer._waits.get(event.event_id)
                assert record is not None and record.state == "done"
                assert record.deadline_ms == original.deadline_ms
                assert record.deferrals == 1
                assert await h.async_redis.xlen(h.config.stream) == 2
                assert failed_reads >= 4
                assert h.runner.opened == ["original"]
                assert h.sink.last_text == "recovered"
                assert len(h.sink.completions) == 1
                assert all(failure_class_from_reply(text) is None for _, _, text in h.sink.updates)

    asyncio.run(go())


@pytest.mark.parametrize("bound", ["workspace", "delivery"])
def test_dependency_wait_has_one_fixed_workspace_deadline(make_harness, bound: str) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            async with make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                max_attempts=1,
                workspace_total_timeout_seconds=1,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                state["status"] = 500
                event = _qevent("original", event_id="dependency-deadline")
                entry, fields = await _pending_local_entry(h, event)
                shortened = False

                async def constrain_delivery(_state: dict[str, Any]) -> None:
                    nonlocal shortened
                    if bound != "delivery" or shortened:
                        return
                    lease = consumer._held_leases[entry]
                    server_time = await h.async_redis.time()
                    now_ms = server_time[0] * 1000 + server_time[1] // 1000
                    await h.async_redis.hset(
                        h.config.delivery_state_key(
                            h.config.stream, h.config.consumer_group, entry
                        ),
                        "deadline_ms",
                        now_ms + 4000,
                    )
                    lease.budget = DeliveryBudget(
                        deadline_ms=now_ms + 4000,
                        anchor_server_ms=now_ms,
                        anchor_monotonic=time.monotonic(),
                    )
                    shortened = True

                state["before_response"] = constrain_delivery
                started = time.monotonic()
                await consumer._sem.acquire()
                await asyncio.wait_for(consumer._handle(entry, fields), timeout=5)
                elapsed = time.monotonic() - started
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert h.runner.opened == []
                assert await consumer._waits.get(event.event_id) is None
                assert await h.async_redis.xlen(h.config.stream) == 1
                assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                    "pending"
                ] == 0
                assert h.sink.completions[-1].outcome == "escalated"
                assert f"after {state['calls']} attempt(s)" in h.sink.last_text
                if bound == "workspace":
                    assert h.config.workspace_total_timeout_seconds <= elapsed < 5
                    assert state["calls"] > h.config.max_attempts
                    assert failure_class_from_reply(h.sink.last_text) == "workspace-error"
                    assert "repository-selection" in h.sink.last_text
                    assert "HTTP 500" in h.sink.last_text
                else:
                    assert shortened and elapsed < 5
                    assert state["calls"] == 1
                    assert failure_class_from_reply(h.sink.last_text) == "delivery-deadline"

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
def test_recovered_dependency_preserves_the_original_capacity_deadline(
    make_harness,
    recovery: str,
) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            async with make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                claim_timeout_seconds=0.02,
                max_attempts=1,
                workspace_total_timeout_seconds=1,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent("original", event_id="recovered-dependency")
                await _park(h, consumer, event)
                first = await consumer._waits.get(event.event_id)
                assert first is not None and first.state == "waiting"
                state.update(fail_after=state["calls"] + 1, recover_after=state["calls"] + 3)
                quota_set = False

                async def fill_quota_after_recovery(observed: dict[str, Any]) -> None:
                    nonlocal quota_set
                    if (
                        recovery == "capacity"
                        and observed["calls"] > observed["recover_after"]
                        and not quota_set
                    ):
                        h.fake_k8s.quota_rejection = QuotaRejection(
                            quota_name="acme-quota",
                            requested={"pods": "1"},
                            used={"pods": "1"},
                            hard={"pods": "1"},
                        )
                        quota_set = True

                state["before_response"] = fill_quota_after_recovery
                h.runner.default_script = [
                    Final(text="healthy recovery", status=SessionStatus.DONE)
                ]
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await asyncio.wait_for(consumer._handle(entry, fields), timeout=5)
                assert state["calls"] > state["recover_after"]
                recovered = await consumer._waits.get(event.event_id)
                assert recovered is not None and recovered.deadline_ms == first.deadline_ms
                if recovery == "capacity":
                    assert quota_set and recovered.state == "waiting"
                    assert recovered.deferrals == 2 and h.runner.opened == []
                    h.fake_k8s.quota_rejection = None
                    entry, fields = await _wake(h, consumer, event.event_id)
                    await consumer._sem.acquire()
                    await consumer._handle(entry, fields)
                else:
                    assert recovered.state == "done" and recovered.deferrals == 1
                assert h.runner.opened == ["original"]
                assert h.sink.last_text == "healthy recovery"
                assert h.sink.completions[-1].outcome == "delivered"
                assert (await consumer._waits.get(event.event_id)).deadline_ms == first.deadline_ms

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
        retry_entered = asyncio.Event()
        release_retry = asyncio.Event()
        calls = 0

        async def selection(_request: web.Request) -> web.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_entered.set()
                await release_first.wait()
                return web.Response(status=503)
            if calls == 2:
                retry_entered.set()
                await release_retry.wait()
            return web.json_response({"repo_full_name": None})

        app = web.Application()
        app.router.add_post(f"/v1/internal/workspaces/{deployment}/selection", selection)
        async with TestServer(app) as server, AsyncExitStack() as stack:
            knobs = dict(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                claim_timeout_seconds=0.02,
                max_attempts=1,
                workspace_total_timeout_seconds=1,
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
            h.runner.default_script = [Final(text="first answer", status=SessionStatus.DONE)]
            first_task = asyncio.create_task(_deliver(h, first_consumer, first))
            second_task = None
            try:
                await asyncio.wait_for(first_entered.wait(), timeout=2)
                second_task = asyncio.create_task(_deliver(peer, second_consumer, second))
                if separate_kernel:
                    await asyncio.wait_for(asyncio.shield(second_task), timeout=1)
                    waiting = await second_consumer._waits.get(second.event_id)
                    assert waiting is not None and waiting.state == "waiting"
                else:
                    from curie_worker.kernel.routing import _thread_key_for

                    key = _thread_key_for(first)
                    await _wait_until(lambda: h.kernel._order_locks[key].refs > 1)
                    assert not second_task.done()
                    assert await second_consumer._waits.get(second.event_id) is None
                assert h.runner.opened == peer.runner.opened == []
                release_first.set()
                await asyncio.wait_for(retry_entered.wait(), timeout=2)
                await asyncio.wait_for(asyncio.shield(second_task), timeout=1)
                waiting = await second_consumer._waits.get(second.event_id)
                assert waiting is not None and waiting.state == "waiting"
                assert not first_task.done()
                assert h.runner.opened == peer.runner.opened == []
                release_retry.set()
                await first_task
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
                release_retry.set()
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


def test_quick_local_followup_waits_for_boot_then_steers_without_notice(make_harness) -> None:
    async def go() -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        deployment = uuid.uuid4()

        async def selection(_request: web.Request) -> web.Response:
            entered.set()
            await release.wait()
            return web.json_response({"repo_full_name": None})

        app = web.Application()
        app.router.add_post(f"/v1/internal/workspaces/{deployment}/selection", selection)
        async with (
            TestServer(app) as server,
            make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=False,
            ) as h,
        ):
            h.kernel._workspace = WorkspaceClaimCoordinator(
                preparer=SimpleNamespace(
                    credentials=WorkspaceCredentialClient(
                        api_url=str(server.make_url("")),
                        worker_token="example-worker-token",
                    )
                ),
                substrate=h.substrate,
            )
            hold = asyncio.Event()
            h.runner.default_script = []
            h.runner.hold = hold
            h.runner.tail = [Final(text="first answer", status=SessionStatus.DONE)]
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            first = asyncio.create_task(
                _deliver(h, consumer, _qevent("first", event_id="boot-first"))
            )
            second = None
            try:
                await asyncio.wait_for(entered.wait(), timeout=2)
                before_followup = list(h.sink.updates)
                second = asyncio.create_task(
                    _deliver(h, consumer, _qevent("second", event_id="boot-second"))
                )
                from curie_worker.kernel.routing import _thread_key_for

                key = _thread_key_for(_qevent("first"))
                await _wait_until(lambda: second.done() or h.kernel._order_locks[key].refs > 1)
                assert not second.done()
                assert await consumer._waits.get("boot-second") is None
                assert h.sink.updates == before_followup
                release.set()
                await asyncio.wait_for(second, timeout=2)
                assert h.runner.opened == ["first"]
                assert h.runner.steers == ["second"]
                assert await consumer._waits.get("boot-second") is None
                assert all("queued" not in text for _, _, text in h.sink.updates)
            finally:
                release.set()
                hold.set()
                await asyncio.gather(first, *([second] if second else []), return_exceptions=True)

    asyncio.run(go())


@pytest.mark.parametrize("fault_on", ["start", "steer"])
def test_order_release_redis_error_does_not_abort_an_accepted_turn(make_harness, fault_on) -> None:
    from redis.asyncio import Redis

    from .conftest import FakeRunner

    async def go() -> None:
        runner = FakeRunner()
        state: dict[str, Any] = {}
        user = f"order-fault-{uuid.uuid4().hex}"

        @web.middleware
        async def reject_order_release(request, handler):
            if request.path == ("/v1/event" if fault_on == "start" else "/v1/steer") and (
                fault_on == "start" or runner.turn_active
            ):
                await state["admin"].execute_command("ACL", "SETUSER", user, "-eval")
            return await handler(request)

        runner.app.middlewares.append(reject_order_release)
        async with make_harness(runner_app=runner.app, slack_no_edit_streaming=True) as h:
            state["admin"] = h.async_redis
            await h.async_redis.execute_command(
                "ACL", "SETUSER", user, "reset", "on", ">example-password", "~*", "+@all"
            )
            connection = h.async_redis.connection_pool.connection_kwargs
            client = Redis(
                host=connection["host"],
                port=connection["port"],
                username=user,
                password="example-password",
                decode_responses=True,
            )
            consumer = _capacity_consumer(h)
            consumer._waits._redis = client
            await consumer.ensure_group()
            hold = asyncio.Event()
            first_task = None
            try:
                if fault_on == "steer":
                    runner.hold = hold
                    runner.default_script = []
                    runner.tail = [Final(text="accepted answer", status=SessionStatus.DONE)]
                    first_task = asyncio.create_task(
                        _deliver(h, consumer, _qevent("first", event_id="accepted-first"))
                    )
                    await _wait_until(lambda: runner.turn_active)
                    await _deliver(h, consumer, _qevent("second", event_id="accepted-second"))
                    assert runner.steers == ["second"]
                else:
                    runner.default_script = [
                        Final(text="accepted answer", status=SessionStatus.DONE)
                    ]
                    await _deliver(h, consumer, _qevent("first", event_id="accepted-first"))
                    assert h.sink.last_text == "accepted answer"
                assert runner.opened == ["first"]
                assert h.sink.completions[-1].outcome == "delivered"
                if first_task is not None:
                    hold.set()
                    await first_task
                assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                    "pending"
                ] == 0
            finally:
                hold.set()
                if first_task is not None:
                    await asyncio.gather(first_task, return_exceptions=True)
                await client.aclose()
                await h.async_redis.execute_command("ACL", "DELUSER", user)

    asyncio.run(go())


def test_approval_resume_escalation_stays_owed_when_adapter_is_unreachable(make_harness) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            async with make_harness(
                binding=_workspace_binding(deployment),
                slack_no_edit_streaming=True,
                completion_sweep_grace_s=0,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                state["status"] = 422
                endpoint = "https://reply.example.com/hook"
                event = _qevent("resume", event_id="approval-resolved", endpoint=endpoint)
                h.sink.dead_endpoints.add(endpoint)
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                await _deliver(h, consumer, event)
                assert h.runner.opened == []
                assert h.sink.swallowed == []
                stored = await h.kernel._markers.read_completion(event.event_id)
                assert stored is not None and stored.record.answer is not None
                h.sink.dead_endpoints.clear()
                await h.kernel.sweep_pending_completions()
                assert failure_class_from_reply(h.sink.last_text) == "workspace-error"
                assert await h.kernel._markers.read_completion(event.event_id) is None

    asyncio.run(go())


class _IdentityBindingOutage:
    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.unavailable = False
        self.successful_calls = 0
        self.failed_calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)

    async def identity_binding(self, *args: Any, **kwargs: Any) -> Any:
        if self.unavailable:
            self.failed_calls += 1
            raise ConnectionRefusedError("identity binding connection refused")
        classification = await self.delegate.identity_binding(*args, **kwargs)
        self.successful_calls += 1
        return classification


@pytest.mark.parametrize("recovery", ["delivered", "sweep", "redelivery"])
def test_capacity_wake_retains_current_identity_through_sustained_workspace_outage(
    make_harness,
    recovery: str,
) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            binding = _IdentityBindingOutage(_workspace_binding(deployment))
            knobs = dict(
                binding=binding,
                slack_no_edit_streaming=True,
                max_attempts=1,
                workspace_total_timeout_seconds=1,
                completion_sweep_grace_s=0,
            )
            async with make_harness(**knobs) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent("original", event_id="sustained-workspace-outage")
                await _park(h, consumer, event)
                claims_before_wake = [dict(env) for env in h.fake_k8s.claim_envs]
                parked = await consumer._waits.get(event.event_id)
                assert parked is not None and parked.state == "waiting"
                classified_before_wake = binding.successful_calls
                assert classified_before_wake > 0
                state["status"] = 500

                async def fail_identity_after_healthy_prepare(_state: dict[str, Any]) -> None:
                    assert binding.successful_calls > classified_before_wake
                    binding.unavailable = True

                state["before_response"] = fail_identity_after_healthy_prepare
                if recovery != "delivered":
                    h.sink.fail_events.add("reply.update")
                entry, fields = await _wake(h, consumer, event.event_id)
                woken = await consumer._waits.get(event.event_id)
                await consumer._sem.acquire()
                await asyncio.wait_for(consumer._handle(entry, fields), timeout=5)
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert binding.unavailable and binding.failed_calls == 0
                assert state["calls"] >= 3
                assert h.runner.opened == []
                assert h.fake_k8s.claim_envs == claims_before_wake
                assert await h.async_redis.xlen(h.config.stream) == 2
                assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                    "pending"
                ] == 0
                final = await consumer._waits.get(event.event_id)
                assert final is not None and final.state == "done"
                assert final.generation == woken.generation and final.deferrals == parked.deferrals
                assert final.deadline_ms == parked.deadline_ms
                assert not await h.async_redis.exists(h.config.dead_letter_stream_name())
                if recovery == "delivered":
                    reply = h.sink.last_text
                    assert reply is not None
                    assert failure_class_from_reply(reply) == "workspace-error"
                    assert "repository-selection" in reply and "HTTP 500" in reply
                    assert len(h.sink.completions) == 1
                    assert h.sink.completions[0].outcome == "escalated"
                else:
                    stored = await h.kernel._markers.read_completion(event.event_id)
                    assert stored is not None and stored.done_flag
                    assert stored.record.answer is not None
                    reply = stored.record.answer.text
                    assert failure_class_from_reply(reply) == "workspace-error"
                    assert "repository-selection" in reply and "HTTP 500" in reply
                    requests = state["calls"]
                    async with make_harness(**knobs) as peer:
                        replacement = _capacity_consumer(peer)
                        if recovery == "sweep":
                            await peer.kernel.sweep_pending_completions()
                        else:
                            replay_id = await peer.async_redis.xadd(peer.config.stream, fields)
                            rows = await peer.async_redis.xreadgroup(
                                peer.config.consumer_group,
                                peer.config.consumer_name,
                                {peer.config.stream: ">"},
                                count=1,
                            )
                            assert rows[0][1][0][0] == replay_id
                            await replacement._sem.acquire()
                            await replacement._handle(*rows[0][1][0])
                        assert peer.runner.opened == []
                        assert peer.sink.last_text == reply
                        assert len(peer.sink.completions) == 1
                        assert peer.sink.completions[0].outcome == "escalated"
                        assert await peer.kernel._markers.read_completion(event.event_id) is None
                        assert state["calls"] == requests
                        assert binding.unavailable and binding.failed_calls == 0

    asyncio.run(go())


def test_selected_http_authority_loss_during_dependency_retry_stays_pending(make_harness) -> None:
    from .test_http_identity_authority import CAPSULE, Binding, Core, pending_entry, qevent

    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            binding, authority = Binding(), Core()
            binding.resolved.deployment_id = deployment
            async with make_harness(
                binding=binding,
                identity_authority=authority,
                workspace_total_timeout_seconds=1,
                max_attempts=1,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                event = qevent("selected dependency outage", thread="selected-dependency-outage")
                fields = {
                    "payload": event.model_dump_json(),
                    "curie_identity_context_v1": CAPSULE,
                }

                async def lose_authority(_state: dict[str, Any]) -> None:
                    authority.unavailable = True
                    if _state["calls"] == 3:
                        _state["status"] = 200

                state.update(status=503, before_response=lose_authority)
                entry, observed = await pending_entry(h, consumer, fields)
                await consumer._sem.acquire()
                await asyncio.wait_for(consumer._handle(entry, observed), timeout=5)
                checks = [r for r in authority.requests if r.stage == "claim_or_resume"]
                assert len(checks) == 1
                assert state["calls"] == 3
                pending = await h.async_redis.xpending_range(
                    h.config.stream, h.config.consumer_group, entry, entry, 1
                )
                assert len(pending) == 1 and pending[0]["times_delivered"] == 1
                assert h.runner.opened == [] and h.fake_k8s.claim_envs == []
                assert h.sink.events == []
                assert not await h.kernel._markers.is_terminal(event.event_id)
                assert await h.kernel._markers.read_completion(event.event_id) is None
                assert not await h.async_redis.exists(h.config.dead_letter_stream_name())

    asyncio.run(go())


def test_fresh_capacity_wake_with_unavailable_identity_stays_pending(make_harness) -> None:
    async def go() -> None:
        async with _selection_server() as (deployment, credentials, state):
            binding = _IdentityBindingOutage(_workspace_binding(deployment))
            async with make_harness(binding=binding, slack_no_edit_streaming=True) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials),
                    substrate=h.substrate,
                )
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent("original", event_id="fresh-workspace-identity-outage")
                await _park(h, consumer, event)
                claims_before_wake = [dict(env) for env in h.fake_k8s.claim_envs]
                assert binding.successful_calls > 0
                before = list(h.sink.events)
                requests = state["calls"]
                binding.unavailable = True
                entry, fields = await _wake(h, consumer, event.event_id)
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
                pending = await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    entry,
                    entry,
                    1,
                )
                assert len(pending) == 1 and pending[0]["times_delivered"] == 1
                assert binding.failed_calls > 0
                assert state["calls"] == requests
                assert h.sink.events == before
                assert h.runner.opened == []
                assert h.fake_k8s.claim_envs == claims_before_wake
                assert not await h.kernel._markers.is_terminal(event.event_id)
                assert await h.kernel._markers.read_completion(event.event_id) is None
                assert not await h.async_redis.exists(h.config.dead_letter_stream_name())

    asyncio.run(go())


def test_unverified_http_capacity_expiry_never_emits_workspace_failure(make_harness) -> None:
    from .test_http_identity_authority import park_notice_record

    async def go() -> None:
        binding = _IdentityBindingOutage(_workspace_binding(uuid.uuid4()))
        binding.unavailable = True
        async with make_harness(binding=binding, slack_no_edit_streaming=True) as h:
            consumer = _capacity_consumer(h)
            event = _qevent("unverified", event_id="http-workspace-expiry")
            fields, record = await park_notice_record(
                h,
                consumer,
                consumer._leases,
                event,
                recognized=True,
            )
            await h.async_redis.hset(consumer._waits._record(event.event_id), "deadline_ms", "1")
            entry, wake_fields = await _wake(h, consumer, event.event_id)
            await consumer._sem.acquire()
            await consumer._handle(entry, wake_fields)
            assert await h.kernel._markers.is_terminal(event.event_id)
            assert h.sink.events == []
            assert await h.kernel._markers.read_completion(event.event_id) is None
            assert h.runner.opened == h.fake_k8s.claim_envs == []
            retained = await consumer._waits.get(event.event_id)
            assert retained is not None and retained.state == "expired"
            assert retained.fields == fields
            assert retained.generation > record.generation
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

    asyncio.run(go())
