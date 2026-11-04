"""Workspace dependency recovery and durable escalation ordering (#5365)."""

from __future__ import annotations

import asyncio
import time
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import ErrorEvent, Final, SessionStatus
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.consumer import Consumer
from curie_worker.kernel.failures import failure_class_from_reply
from curie_worker.sandbox import QuotaRejection
from curie_worker.workspace import WorkspaceClaimCoordinator, WorkspaceCredentialClient

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
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1,
    )
    assert len(rows[0][1]) == 1
    return rows[0][1][0]


async def _park(h: Any, consumer: Consumer, event: Any) -> None:
    h.fake_k8s.quota_rejection = QuotaRejection(
        quota_name="acme-quota", requested={"pods": "1"}, used={"pods": "1"}, hard={"pods": "1"},
    )
    await _deliver(h, consumer, event)
    record = await consumer._waits.get(event.event_id)
    assert record is not None and record.state == "waiting"
    h.fake_k8s.quota_rejection = None


@pytest.mark.parametrize("recovery", ["sweep", "redelivery"])
@pytest.mark.parametrize("placeholder", ["p-1", None])
def test_failed_escalation_is_replayed_without_execution_or_overtaking(
    make_harness, recovery: str, placeholder: str | None,
) -> None:
    async def go() -> None:
        knobs = dict(
            slack_no_edit_streaming=True, max_attempts=1, claim_timeout_seconds=0.02,
            completion_sweep_grace_s=0,
        )
        async with make_harness(**knobs) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            first = _qevent("first", event_id="failed-original", placeholder=placeholder)
            await _park(h, consumer, first)
            h.runner.default_script = [
                ErrorEvent(classification="runner-error", message="execution failed"),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            h.sink.fail_events.add("reply.update")
            entry, fields = await _wake(h, consumer, first.event_id)
            if recovery == "redelivery":
                async with consumer._delivery_lease(entry, fields) as lease:
                    assert lease is not None
                    from curie_worker.capacity_wait import WAIT_GENERATION_FIELD, wait_delivery_scope

                    with wait_delivery_scope(
                        consumer._waits, first.event_id, int(fields[WAIT_GENERATION_FIELD]), lease,
                    ):
                        await h.kernel.process_event(first, lease=lease)
            else:
                await consumer._sem.acquire()
                await consumer._handle(entry, fields)
            assert await h.kernel._markers.is_terminal(first.event_id)
            stored = await h.kernel._markers.read_completion(first.event_id)
            assert stored is not None and stored.done_flag
            assert stored.record.answer is not None
            assert failure_class_from_reply(stored.record.answer.text) == "runner-error"
            assert h.runner.opened == ["first"]
            async with make_harness(**knobs) as peer:
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
                assert failure_class_from_reply(peer.sink.last_text) == "runner-error"
                assert await peer.kernel._markers.read_completion(first.event_id) is None
                peer.runner.default_script = [Final(text="later answer", status=SessionStatus.DONE)]
                later_entry, later_fields = await _wake(peer, replacement, second.event_id)
                await replacement._sem.acquire()
                await replacement._handle(later_entry, later_fields)
                assert peer.runner.opened == ["second"]
                assert peer.sink.last_text == "later answer"

    asyncio.run(go())


@pytest.mark.parametrize("outage_s", [0.03])
def test_capacity_wake_recovers_from_workspace_outage_without_escalating(
    make_harness, outage_s: float,
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
                api_url=str(server.make_url("")), worker_token="example-worker-token",
            )
            async with make_harness(
                binding=_workspace_binding(deployment), slack_no_edit_streaming=True,
                claim_timeout_seconds=0.02,
            ) as h:
                h.kernel._workspace = WorkspaceClaimCoordinator(
                    preparer=SimpleNamespace(credentials=credentials), substrate=h.substrate,
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
