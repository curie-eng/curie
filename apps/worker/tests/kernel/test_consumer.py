"""Consumer tests: end-to-end stream consumption and crash-recovery reclaim,
against the real Valkey stream + consumer group.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import redis.exceptions
from aci_protocol import (
    Event,
    Final,
    OutboundEvent,
    QueuedTurn,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from curie_api.config import Settings
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_dispatcher.queue import to_stream_fields
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_telemetry import tracing as telemetry_tracing
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker import capacity_wait as capacity_wait_module
from curie_worker import consumer as consumer_module
from curie_worker import delivery_lease as delivery_lease_module
from curie_worker import kernel as kernel_module
from curie_worker import stream_consumer as stream_consumer_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.capacity_wait import (
    WAIT_GENERATION_FIELD,
    WAIT_TERMINAL_ONLY_FIELD,
    CapacityWaitStore,
)
from curie_worker.consumer import Consumer
from curie_worker.consumer_liveness import (
    ConsumerLivenessStore,
    consumer_heartbeat_capable_key,
    consumer_heartbeat_key,
)
from curie_worker.cron_loop import CronSchedulerLoop, _Target
from curie_worker.delivery_lease import DeliveryLeaseStore, LeaseLostError
from curie_worker.hook_source_guard import CronHookSourceGuard
from curie_worker.runner_client import TurnStream
from curie_worker.sandbox import QuotaRejection
from curie_worker.stream_consumer import ConsumerLivenessExpired
from curie_worker.threadlock import ThreadLock
from curie_worker.workspace import WorkspacePreparationError
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import qevent as _qevent  # noqa: E402
from queue_fixtures import wait_until as _wait_until  # noqa: E402

DONE = SessionStatus.DONE

HEARTBEAT_TTL_MS = 15_000

# ADR 0207 production clocks scaled by 0.1, including the one-heartbeat
# safety margin. The 70ms tolerance is 2% of the 3.5s ownership window.
_OUTAGE_KNOBS: dict[str, object] = {
    "delivery_lease_ttl_s": 4.5,
    "delivery_lease_heartbeat_s": 1.0,
    "consumer_heartbeat_ttl_ms": 4500,
    "consumer_capability_ttl_ms": 9000,
    "delivery_budget_s": 60.0,
    "runner_total_timeout_s": 30.0,
}


async def _outage_delivery(h: Any) -> tuple[Consumer, DeliveryLeaseStore, str, dict[str, str]]:
    store = DeliveryLeaseStore(h.async_redis, h.config)
    consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store)
    await consumer.ensure_group()
    await h.async_redis.xadd(
        h.config.stream, to_stream_fields(_qevent("outage", event_id=uuid.uuid4().hex))
    )
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
    )
    entry_id, fields = rows[0][1][0]
    return consumer, store, entry_id, dict(fields)


def test_delivery_transport_outage_loses_at_thirty_five_seconds_not_first_error(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with make_harness(**_OUTAGE_KNOBS) as h:
            consumer, store, entry_id, fields = await _outage_delivery(h)
            attempts: list[float] = []
            real_heartbeat = store.heartbeat
            real_finish_loss = consumer._finish_lease_loss
            real_on_lost = consumer._on_lease_lost
            losses = {"finish": 0, "callback": 0}
            outage_until = time.monotonic() + 5.0

            async def finish_loss(*args: Any, **kwargs: Any) -> None:
                losses["finish"] += 1
                await real_finish_loss(*args, **kwargs)

            async def on_lost(*args: Any, **kwargs: Any) -> None:
                losses["callback"] += 1
                assert real_on_lost is not None
                await real_on_lost(*args, **kwargs)

            async def unavailable(*args: Any, **kwargs: Any) -> Any:
                attempts.append(time.monotonic())
                if time.monotonic() < outage_until:
                    raise redis.exceptions.ConnectionError("injected 50s ownership outage")
                return await real_heartbeat(*args, **kwargs)

            monkeypatch.setattr(store, "heartbeat", unavailable)
            monkeypatch.setattr(consumer, "_finish_lease_loss", finish_loss)
            assert real_on_lost is not None
            consumer._on_lease_lost = on_lost
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                started = lease.local_deadline_monotonic - 3.5
                await asyncio.sleep(max(0, started + 3.43 - time.monotonic()))
                assert not lease.lost.is_set(), "a transient raise dropped a still-valid lease"
                await asyncio.wait_for(lease.lost.wait(), timeout=0.2)
                assert time.monotonic() - started == pytest.approx(3.5, abs=0.07)
                assert len(attempts) == 3, "renewals must retry at 10, 20 and 30 seconds"
                assert [instant - started for instant in attempts] == pytest.approx(
                    [1.0, 2.0, 3.0], abs=0.07
                )
                await _wait_until(lambda: losses["callback"] == 1)
                await asyncio.gather(*list(consumer._lease_loss_tasks))
                assert losses == {"finish": 1, "callback": 1}
                await asyncio.sleep(0.05)
                assert len(attempts) == 3, "a lost lease continued attempting renewals"

    asyncio.run(go())


@pytest.mark.parametrize("outage_s", [2.5, 5.0], ids=["25s-survives", "50s-expires"])
def test_liveness_transport_outage_uses_thirty_five_second_deadline(
    make_harness, monkeypatch: pytest.MonkeyPatch, outage_s: float
) -> None:
    async def go() -> None:
        async with make_harness(**_OUTAGE_KNOBS) as h:
            consumer = _capacity_consumer(h)
            await consumer._publish_liveness()
            started = consumer._last_liveness_renewal
            assert started is not None
            store = consumer._liveness_store
            assert store is not None
            real_renew = store.renew
            attempts: list[float] = []

            async def unavailable(**kwargs: Any) -> bool:
                attempts.append(time.monotonic())
                if time.monotonic() < started + outage_s:
                    raise redis.exceptions.ConnectionError("injected liveness outage")
                return await real_renew(**kwargs)

            monkeypatch.setattr(store, "renew", unavailable)
            task = asyncio.create_task(consumer._liveness_refresh_loop())
            try:
                if outage_s == 5.0:
                    await asyncio.sleep(max(0, started + 3.43 - time.monotonic()))
                    assert not task.done(), "liveness expired before its local deadline"
                    with pytest.raises(ConsumerLivenessExpired):
                        await asyncio.wait_for(task, timeout=0.2)
                    assert time.monotonic() - started == pytest.approx(3.5, abs=0.07)
                    assert len(attempts) == 3
                else:
                    await asyncio.sleep(max(0, started + 2.5 - time.monotonic()))
                    assert not task.done(), "a 25s transport outage killed the generation"
                    await _wait_until(lambda: consumer._last_liveness_renewal > started)
                    assert not task.done()
                    assert await store.is_alive(
                        stream=h.config.stream,
                        group=h.config.consumer_group,
                        consumer=h.config.consumer_name,
                    )
                    assert [instant - started for instant in attempts[:3]] == pytest.approx(
                        [1.0, 2.0, 3.0], abs=0.07
                    )
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, ConsumerLivenessExpired):
                    await task
                await consumer._cleanup_alive()

    asyncio.run(go())


def test_delivery_and_liveness_deadlines_anchor_before_sending_delayed_calls(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with make_harness(consumer_heartbeat_ttl_ms=45000) as h:
            consumer, store, entry_id, fields = await _outage_delivery(h)
            real_eval = h.async_redis.eval
            clock = SimpleNamespace(now=100.0)
            calls = 0
            phase = "delivery"
            delivery_done = asyncio.Event()
            liveness_done = asyncio.Event()
            never = asyncio.Event()

            async def delayed_reply(*args: Any, **kwargs: Any) -> Any:
                nonlocal calls
                result = await real_eval(*args, **kwargs)
                calls += 1
                clock.now += 3.0
                if phase == "delivery" and calls == 2:
                    delivery_done.set()
                return result

            async def virtual_sleep(delay: float) -> None:
                if delivery_done.is_set():
                    await never.wait()
                clock.now += delay
                await asyncio.sleep(0)

            async def virtual_generation_sleep(delay: float) -> None:
                if liveness_done.is_set():
                    await never.wait()
                clock.now += delay
                await asyncio.sleep(0)

            # Replace module references, preserving the real server TIME and
            # the process-wide asyncio/time modules used by other tests.
            for module in (delivery_lease_module, stream_consumer_module):
                monkeypatch.setattr(
                    module,
                    "time",
                    SimpleNamespace(**{**vars(time), "monotonic": lambda: clock.now}),
                )
            monkeypatch.setattr(
                stream_consumer_module,
                "asyncio",
                SimpleNamespace(**{**vars(asyncio), "sleep": virtual_sleep}),
            )
            monkeypatch.setattr(h.async_redis, "eval", delayed_reply)
            sent = clock.now
            lease = await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            assert clock.now == sent + 3.0
            assert lease.local_deadline_monotonic == sent + 35.0
            previous_deadline = lease.local_deadline_monotonic
            heartbeat = asyncio.create_task(consumer._heartbeat_lease(lease, entry_id, fields))
            try:
                await asyncio.wait_for(delivery_done.wait(), timeout=1.0)
                assert lease.budget.anchor_monotonic == sent + 10.0
                assert clock.now == sent + 13.0
                assert lease.local_deadline_monotonic == sent + 10.0 + 35.0
                assert lease.local_deadline_monotonic != clock.now + 35.0
                assert lease.local_deadline_monotonic > previous_deadline
            finally:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat
            # The liveness adapter is a real transaction. Delay its confirmed
            # response, retaining its true pre-send anchor in the consumer.
            phase = "liveness"
            liveness = consumer._liveness_store
            assert liveness is not None
            real_publish = liveness.publish
            real_renew = liveness.renew
            renewed_sends: list[float] = []

            async def delayed_publish(**kwargs: Any) -> str:
                token = await real_publish(**kwargs)
                clock.now += 3.0
                return token

            async def delayed_renew(**kwargs: Any) -> bool:
                renewed_sends.append(clock.now)
                result = await real_renew(**kwargs)
                liveness_done.set()
                return result

            monkeypatch.setattr(liveness, "publish", delayed_publish)
            monkeypatch.setattr(liveness, "renew", delayed_renew)
            monkeypatch.setattr(consumer, "_sleep_generation", virtual_generation_sleep)
            sent = clock.now
            await consumer._publish_liveness()
            assert clock.now == sent + 3.0
            assert consumer._last_liveness_renewal == sent
            previous = consumer._last_liveness_renewal
            task = asyncio.create_task(consumer._liveness_refresh_loop())
            try:
                await asyncio.wait_for(liveness_done.wait(), timeout=1.0)
                confirmed = consumer._last_liveness_renewal
                assert confirmed is not None
                assert renewed_sends == [sent + 10.0]
                assert confirmed == renewed_sends[0]
                assert clock.now == confirmed + 3.0
                assert confirmed + 35.0 < clock.now + 35.0
                assert confirmed > previous
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                await consumer._cleanup_alive()
                await store.release(
                    lease.stream,
                    lease.group,
                    lease.entry_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )

    asyncio.run(go())


@pytest.mark.parametrize("refusal", ["key-gone", "wrong-token", "wrong-generation", "pel-gone"])
def test_delivery_confirmed_refusals_drop_authority_on_the_first_renewal(
    make_harness, refusal: str
) -> None:
    async def go() -> None:
        async with make_harness(**_OUTAGE_KNOBS) as h:
            consumer, store, entry_id, fields = await _outage_delivery(h)
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                key = h.config.delivery_lease_key(lease.stream, lease.group, entry_id)
                started = time.monotonic()
                if refusal == "key-gone":
                    await h.async_redis.delete(key)
                elif refusal == "wrong-token":
                    await h.async_redis.set(key, "another-owner", px=4500)
                elif refusal == "wrong-generation":
                    await h.async_redis.hincrby(
                        h.config.delivery_state_key(lease.stream, lease.group, entry_id), "gen", 1
                    )
                else:
                    await h.async_redis.xack(lease.stream, lease.group, entry_id)
                await asyncio.wait_for(lease.lost.wait(), timeout=1.25)
                assert time.monotonic() - started < 1.2
                with pytest.raises(LeaseLostError):
                    lease.raise_if_lost()
                # A separate valid delivery still renews through the same store.
                await h.async_redis.xadd(
                    lease.stream, to_stream_fields(_qevent("control", event_id=uuid.uuid4().hex))
                )
                rows = await h.async_redis.xreadgroup(
                    lease.group, h.config.consumer_name, {lease.stream: ">"}, count=1
                )
                control_id = rows[0][1][0][0]
                control = await store.acquire(
                    lease.stream, lease.group, control_id, consumer=h.config.consumer_name
                )
                assert (
                    await store.heartbeat(
                        control.stream,
                        control.group,
                        control.entry_id,
                        consumer=h.config.consumer_name,
                        owner=control.owner,
                        generation=control.generation,
                        resume_event_id=None,
                    )
                    is not None
                )
                await store.release(
                    control.stream,
                    control.group,
                    control.entry_id,
                    owner=control.owner,
                    resume_event_id=None,
                )

    asyncio.run(go())


@pytest.mark.parametrize("refusal", ["key-gone", "new-generation"])
def test_liveness_token_refusal_never_resurrects_or_overwrites_a_generation(
    make_harness, refusal: str
) -> None:
    async def go() -> None:
        async with make_harness(**_OUTAGE_KNOBS) as h:
            consumer = _capacity_consumer(h)
            await consumer._publish_liveness()
            store = consumer._liveness_store
            assert store is not None
            old_token = consumer._liveness_token
            assert isinstance(old_token, str) and len(old_token) == 32
            key = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )
            capable = consumer_heartbeat_capable_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )
            kwargs = dict(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer=h.config.consumer_name,
                heartbeat_ttl_ms=4500,
                capability_ttl_ms=9000,
            )
            replacement = None
            if refusal == "key-gone":
                await h.async_redis.delete(key)
            else:
                replacement = await store.publish(**kwargs)
                assert replacement != old_token
            # Give the capability key a distinguishable value and TTL. A CAS
            # refusal must change neither key, even the longer-lived marker.
            await h.async_redis.set(capable, "sentinel", px=2000)
            assert await store.renew(**kwargs, token=old_token) is False
            assert await h.async_redis.get(key) == replacement
            assert await h.async_redis.get(capable) == "sentinel"
            assert 0 < await h.async_redis.pttl(capable) <= 2000
            started = time.monotonic()
            with pytest.raises(ConsumerLivenessExpired):
                await asyncio.wait_for(consumer._liveness_refresh_loop(), timeout=1.25)
            assert time.monotonic() - started < 1.2
            assert await h.async_redis.get(key) == replacement
            assert await h.async_redis.get(capable) == "sentinel"
            # The newly published generation is valid, including after a
            # durable key loss, and refreshes both TTLs through the real Lua.
            if replacement is None:
                replacement = await store.publish(**kwargs)
            assert await store.renew(**kwargs, token=replacement) is True
            assert await h.async_redis.get(key) == replacement
            assert await h.async_redis.pttl(key) > 4300
            assert await h.async_redis.pttl(capable) > 8800
            await consumer._cleanup_alive()

    asyncio.run(go())


@pytest.mark.parametrize("failure", ["connection-error", "hung-call"])
def test_terminal_xack_retries_without_starving_the_delivery_heartbeat(
    make_harness, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    async def go() -> None:
        async with make_harness(**_OUTAGE_KNOBS) as h:
            consumer, store, entry_id, fields = await _outage_delivery(h)
            real_xack = h.async_redis.xack
            real_heartbeat = store.heartbeat
            attempted = asyncio.Event()
            confirmed = asyncio.Event()
            cancelled = asyncio.Event()
            calls = 0
            outage_until: float | None = None
            retry_sleeps: list[float] = []

            async def observe_heartbeat(*args: Any, **kwargs: Any) -> Any:
                result = await real_heartbeat(*args, **kwargs)
                if result is not None:
                    confirmed.set()
                return result

            async def flaky_xack(*args: Any, **kwargs: Any) -> Any:
                nonlocal calls, outage_until
                calls += 1
                attempted.set()
                if failure == "connection-error":
                    if outage_until is None:
                        outage_until = time.monotonic() + 2.0
                    if time.monotonic() < outage_until:
                        raise redis.exceptions.ConnectionError("injected 20s terminal XACK outage")
                if failure == "hung-call" and calls == 1:
                    try:
                        await asyncio.Event().wait()
                    finally:
                        cancelled.set()
                assert confirmed.is_set(), "XACK retries retained the heartbeat lock"
                return await real_xack(*args, **kwargs)

            async def settlement_sleep(delay: float) -> None:
                task = asyncio.current_task()
                if (
                    failure == "connection-error"
                    and task is not None
                    and task.get_name() == "test:terminal-xack"
                ):
                    retry_sleeps.append(delay)
                    await asyncio.sleep(delay * 0.1)
                else:
                    await asyncio.sleep(delay)

            monkeypatch.setattr(store, "heartbeat", observe_heartbeat)
            monkeypatch.setattr(h.async_redis, "xack", flaky_xack)
            monkeypatch.setattr(
                stream_consumer_module,
                "asyncio",
                SimpleNamespace(**{**vars(asyncio), "sleep": settlement_sleep}),
            )
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                task = asyncio.create_task(consumer._ack(entry_id), name="test:terminal-xack")
                try:
                    await attempted.wait()
                    if failure == "connection-error":
                        # Sleep/backoff must happen after releasing the lock.
                        async with asyncio.timeout(0.1), lease.settlement_lock:
                            assert not task.done()
                    await asyncio.wait_for(confirmed.wait(), timeout=1.25)
                    await asyncio.wait_for(task, timeout=2.0)
                    assert not lease.lost.is_set()
                    assert lease.acknowledged.is_set()
                    assert calls == (8 if failure == "connection-error" else 2)
                    if failure == "connection-error":
                        assert outage_until is not None and time.monotonic() >= outage_until
                        assert retry_sleeps == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0, 5.0]
                    assert cancelled.is_set() is (failure == "hung-call")
                    assert (await h.async_redis.xpending(lease.stream, lease.group))["pending"] == 0
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    asyncio.run(go())


def test_terminal_xack_stops_retrying_after_a_confirmed_lease_loss(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with make_harness(**_OUTAGE_KNOBS) as h:
            consumer, _store, entry_id, fields = await _outage_delivery(h)
            attempts = 0
            failed = asyncio.Event()

            async def unavailable(*_args: Any, **_kwargs: Any) -> Any:
                nonlocal attempts
                attempts += 1
                failed.set()
                raise redis.exceptions.ConnectionError("injected settlement outage")

            monkeypatch.setattr(h.async_redis, "xack", unavailable)
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                task = asyncio.create_task(consumer._ack(entry_id))
                try:
                    await failed.wait()
                    lease.lost.set()
                    with pytest.raises(LeaseLostError):
                        await asyncio.wait_for(task, timeout=0.75)
                    assert attempts == 1
                    assert not lease.acknowledged.is_set()
                    assert (await h.async_redis.xpending(lease.stream, lease.group))["pending"] == 1
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, LeaseLostError):
                        await task

    asyncio.run(go())


@pytest.fixture
def producer_spans(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry_tracing, "_tracer", provider.get_tracer("curie-telemetry"))
    try:
        yield provider, exporter
    finally:
        provider.shutdown()


@pytest.mark.parametrize("producer", ["cron", "work_item"])
def test_enqueue_producer_is_the_real_consumer_parent(
    make_harness,
    make_hook_run,
    producer_spans: tuple[TracerProvider, InMemorySpanExporter],
    producer: str,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    provider, exporter = producer_spans

    async def go() -> None:
        async with make_hook_run() as run, make_harness(hook_runs=run.recorder()) as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            with provider.get_tracer("test-ingress").start_as_current_span("test.ingress"):
                if producer == "work_item":
                    settings = Settings(
                        runs_stream=h.config.stream,
                        runs_consumer_group=h.config.consumer_group,
                    )
                    reconciler = WorkItemReconciler(
                        async_sessionmaker(run.engine), h.async_redis, settings
                    )
                    await reconciler._xadd(
                        _qevent(
                            "work item",
                            event_id=f"work-item-{uuid.uuid4()}-execute-1",
                            source=TurnSource.WEBHOOK,
                        )
                    )
                else:

                    async def not_killed(_agent_id: uuid.UUID) -> bool:
                        return False

                    gate_engine = create_async_engine(
                        run.engine.url, pool_size=4, max_overflow=0, pool_timeout=30
                    )
                    guard = CronHookSourceGuard(SourceGate(gate_engine), run.engine)
                    try:
                        loop = CronSchedulerLoop(
                            source_guard=guard,
                            engine=run.engine,
                            redis=h.async_redis,
                            source=SimpleNamespace(triggers=lambda _bundle: []),
                            is_killed=not_killed,
                            db_schema="curie",
                            stream=h.config.stream,
                            interval_seconds=1,
                            claim_lease_s=300,
                            default_max_usd_per_day=10,
                            default_max_output_tokens_per_run=100_000,
                        )
                        async with guard.locked_snapshot(run.agent_id, run.ref.name) as context:
                            await loop._enqueue(
                                _Target(
                                    agent_id=run.agent_id,
                                    agent_name="acme-bot",
                                    version_id=run.version_id,
                                    bundle_ref=None,
                                    deployed_at=None,
                                    max_usd_per_day=None,
                                    max_output_tokens_per_run=None,
                                ),
                                {"name": run.ref.name, "prompt": "cron prompt"},
                                _qevent("route").reply_handle,
                                datetime.fromisoformat(run.ref.slot_utc),
                                run.run_id,
                                source_context=context,
                            )
                    finally:
                        await gate_engine.dispose()
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            entry_id, fields = rows[0][1][0]
            await consumer._sem.acquire()
            await consumer._handle(entry_id, fields)
            spans = exporter.get_finished_spans()
            enqueue = [span for span in spans if span.name == "curie.queue.enqueue"]
            process = [span for span in spans if span.name == "curie.queue.process"]
            assert len(enqueue) == len(process) == 1
            assert enqueue[0].kind is SpanKind.PRODUCER
            assert process[0].kind is SpanKind.CONSUMER
            assert process[0].parent.span_id == enqueue[0].context.span_id
            assert process[0].context.trace_id == enqueue[0].context.trace_id
            assert int(fields["traceparent"].split("-")[2], 16) == enqueue[0].context.span_id
            if producer == "cron":
                assert h.runner.opened == ["cron prompt"]

    asyncio.run(go())


def test_carrierless_cli_entry_starts_a_real_root_consumer_span(
    make_harness,
    producer_spans: tuple[TracerProvider, InMemorySpanExporter],
) -> None:
    provider, exporter = producer_spans

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="root answer", status=DONE)]
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent("carrierless CLI turn")
            await h.async_redis.xadd(h.config.stream, {"payload": event.model_dump_json()})
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            entry_id, fields = rows[0][1][0]
            assert set(fields) == {"payload"}
            with provider.get_tracer("test-unrelated").start_as_current_span("test.unrelated"):
                await consumer._sem.acquire()
                await consumer._handle(entry_id, fields)
            process = [
                span for span in exporter.get_finished_spans() if span.name == "curie.queue.process"
            ]
            assert len(process) == 1
            assert process[0].parent is None
            assert process[0].context.is_valid
            assert h.runner.opened == ["carrierless CLI turn"]
            assert not any(
                span.name == "curie.queue.enqueue" for span in exporter.get_finished_spans()
            )

    asyncio.run(go())


@pytest.mark.parametrize("recover", [False, True], ids=["wake", "lost_wake"])
@pytest.mark.parametrize("active", [False, True], ids=["stored", "active"])
@pytest.mark.parametrize(
    "stored",
    [None, "00-3123456789abcdef0123456789abcdef-3123456789abcdef-01"],
    ids=["absent", "present"],
)
def test_capacity_publication_injects_active_context_and_preserves_stored_fields(
    make_harness,
    producer_spans: tuple[TracerProvider, InMemorySpanExporter],
    recover: bool,
    active: bool,
    stored: str | None,
) -> None:
    provider, exporter = producer_spans

    async def go() -> None:
        async with make_harness() as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            store = CapacityWaitStore(h.async_redis, h.config)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            event = _qevent("wait for capacity")
            original = to_stream_fields(event)
            if stored is not None:
                original["traceparent"] = stored
            original["test_transport"] = "retained"
            entry_id = await h.async_redis.xadd(h.config.stream, original)
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            assert rows[0][1][0][0] == entry_id
            lease = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            try:
                parked = await store.park(entry_id, original, event.event_id, lease)
            finally:
                await leases.release(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )
            await h.async_redis.zadd(store._due, {event.event_id: 0})
            if recover:
                assert await store.wake_due() == 1
                wake_id = (await h.async_redis.xrevrange(h.config.stream, count=1))[0][0]
                await h.async_redis.xdel(h.config.stream, wake_id)
                await h.async_redis.hset(store._record(event.event_id), "deadline_ms", "0")
                await h.async_redis.zadd(store._flight, {event.event_id: 0})
            if active:
                with provider.get_tracer("test-capacity").start_as_current_span("test.capacity"):
                    published = await (
                        store.reconcile_lost_wakes() if recover else store.wake_due()
                    )
                    assert published == 1
            else:
                wake_task = asyncio.create_task(consumer._capacity_wake_loop())
                try:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        latest = await h.async_redis.xrevrange(h.config.stream, count=1)
                        if latest and latest[0][0] != entry_id:
                            break
                        if wake_task.done():
                            await wake_task
                            raise AssertionError("capacity wake loop exited before publication")
                        await asyncio.sleep(0.005)
                    else:
                        raise AssertionError("capacity wake loop did not publish the due entry")
                finally:
                    consumer.request_stop()
                    await wake_task
            rows = await h.async_redis.xrevrange(h.config.stream, count=1)
            fields = rows[0][1]
            assert fields["payload"] == original["payload"]
            assert fields["test_transport"] == "retained"
            assert fields[WAIT_GENERATION_FIELD] == str(parked.generation + (2 if recover else 1))
            assert (fields.get(WAIT_TERMINAL_ONLY_FIELD) == "1") is recover
            if active:
                maintenance = next(
                    span for span in exporter.get_finished_spans() if span.name == "test.capacity"
                )
                carrier = fields["traceparent"].split("-")
                assert int(carrier[1], 16) == maintenance.context.trace_id
                assert int(carrier[2], 16) == maintenance.context.span_id
                assert fields["traceparent"] != stored
            else:
                assert fields.get("traceparent") == stored
                assert {key: fields[key] for key in original} == original
                if stored is None:
                    assert "traceparent" not in fields
            record = await store.get(event.event_id)
            assert record is not None and record.fields == original
            assert not any(
                span.name == "curie.queue.enqueue" for span in exporter.get_finished_spans()
            )
            assert await (store.reconcile_lost_wakes() if recover else store.wake_due()) == 0

    asyncio.run(go())


async def _wait_consumer_idle(
    redis: AsyncRedis, stream: str, group: str, consumer: str, idle_ms: int
) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        rows = await redis.xinfo_consumers(stream, group)
        if any(
            str(row["name"]) == consumer and int(row.get("idle") or 0) >= idle_ms for row in rows
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("consumer did not become idle")


async def _deliveries(redis: AsyncRedis, stream: str, group: str) -> dict[str, int]:
    rows = await redis.xpending_range(stream, group, min="-", max="+", count=100)
    return {str(row["message_id"]): int(row["times_delivered"]) for row in rows}


async def _wait_key(redis: AsyncRedis, key: str, *, present: bool = True) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if bool(await redis.exists(key)) is present:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"key {key!r} did not become {'present' if present else 'absent'}")


async def _pending_owner(redis: AsyncRedis, stream: str, group: str, entry_id: str) -> str | None:
    rows = await redis.xpending_range(stream, group, min=entry_id, max=entry_id, count=1)
    return str(rows[0]["consumer"]) if rows else None


async def _wait_for_pending_count(
    redis: AsyncRedis, stream: str, group: str, expected: int
) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        summary = await redis.xpending(stream, group)
        if summary["pending"] == expected:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"pending count did not become {expected}")


async def _wait_capacity_state(
    consumer: Consumer, event_id: str, state: str, *, generation: int | None = None
) -> Any:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        record = await consumer._waits.get(event_id)
        if (
            record is not None
            and record.state == state
            and (generation is None or record.generation == generation)
        ):
            return record
        await asyncio.sleep(0.01)
    raise AssertionError(f"capacity wait did not become {state}")


async def _pending_local_entry(h: Any, event: QueuedTurn) -> tuple[str, dict[str, str]]:
    fields = to_stream_fields(event)
    entry_id = await h.async_redis.xadd(h.config.stream, fields)
    claimed = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    assert len(claimed) == 1
    assert len(claimed[0][1]) == 1
    assert claimed[0][1][0][0] == entry_id
    assert claimed[0][1][0][1] == fields
    return entry_id, fields


def _capacity_consumer(h: Any) -> Consumer:
    return Consumer(
        redis=h.async_redis,
        kernel=h.kernel,
        config=h.config,
        leases=DeliveryLeaseStore(h.async_redis, h.config),
    )


class _RenewalProbeStore:
    """Fault injector around the real liveness adapter, never around Valkey."""

    def __init__(
        self,
        delegate: ConsumerLivenessStore,
        *,
        fail_renewals: int = 0,
        timeout_renewals: int = 0,
        hang_renewals: bool = False,
        slow_renewal_s: float = 0,
    ) -> None:
        self._delegate = delegate
        self._fail_renewals = fail_renewals
        self._timeout_renewals = timeout_renewals
        self._hang_renewals = hang_renewals
        self._slow_renewal_s = slow_renewal_s
        self.renew_calls = 0
        self.slow_completed = 0
        self._never = asyncio.Event()

    async def publish(self, **kwargs: Any) -> str:
        return await self._delegate.publish(**kwargs)

    async def renew(self, **kwargs: Any) -> bool:
        self.renew_calls += 1
        if self.renew_calls <= self._fail_renewals:
            raise redis.exceptions.ConnectionError("injected transient renewal failure")
        if self.renew_calls <= self._fail_renewals + self._timeout_renewals:
            await self._never.wait()
        if self._hang_renewals:
            await self._never.wait()
        if self._slow_renewal_s > 0:
            await asyncio.sleep(self._slow_renewal_s)
            self.slow_completed += 1
        return await self._delegate.renew(**kwargs)

    async def is_alive(self, **kwargs: Any) -> bool:
        return await self._delegate.is_alive(**kwargs)

    async def is_capable(self, **kwargs: Any) -> bool:
        return await self._delegate.is_capable(**kwargs)

    async def cleanup_alive(self, **kwargs: Any) -> None:
        await self._delegate.cleanup_alive(**kwargs)

    async def try_acquire_reclaim(self, **kwargs: Any) -> str | None:
        return await self._delegate.try_acquire_reclaim(**kwargs)

    async def release_reclaim(self, **kwargs: Any) -> None:
        await self._delegate.release_reclaim(**kwargs)


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


async def _wait_until_turn_active_or_consumer_failed(
    runner: Any, task: asyncio.Task[Any], timeout: float = 5.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if task.done():
            exc = task.exception()
            if exc is not None:
                raise exc
            raise AssertionError(
                f"consumer finished before the turn became active: {task.result()!r}"
            )
        if runner.turn_active:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


def test_consumes_stream_entry_end_to_end_and_acks(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [TextDelta(text="hi "), Final(text="answer", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent("hello", thread="tc1", event_id="c1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.sink.last_text == "answer")
            consumer.request_stop()
            await task

            assert h.runner.opened == ["hello"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0  # the entry was acked

    asyncio.run(go())


def test_interactive_capacity_wait_acks_without_completing_the_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent("hello", thread="waiting-thread", event_id="waiting-turn")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))

            task = asyncio.create_task(consumer.run())
            try:
                await _wait_until(lambda: bool(h.sink.updates))
                assert "queued" in h.sink.updates[-1][2].lower()
                assert h.sink.updates[-1][:2] == ("C1", "p-1")
                assert h.runner.opened == []
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
                record = await consumer._waits.get(event.event_id)
                assert record is not None
                assert record.state == "waiting"
                assert record.deferrals == 1
                assert record.fields == to_stream_fields(event)
                assert record.first_wait_ms < record.deadline_ms
                assert await consumer._waits.snapshot() == {
                    "waiting": 1,
                    "active": 0,
                    "expired": 0,
                }
            finally:
                consumer.request_stop()
                await task

    asyncio.run(go())


def test_slack_reply_handle_does_not_make_a_webhook_turn_interactive(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent(
                "job output",
                thread="webhook-thread",
                event_id="webhook-capacity",
                source=TurnSource.WEBHOOK,
            )
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))

            task = asyncio.create_task(consumer.run())
            try:
                await _wait_key(h.async_redis, h.config.done_key(event.event_id))
                assert h.sink.updates == [
                    (
                        "C1",
                        "p-1",
                        "This agent is at capacity right now. Please try again shortly.",
                    )
                ]
                assert h.runner.opened == []
            finally:
                consumer.request_stop()
                await task

    asyncio.run(go())


def test_capacity_wait_wakes_once_after_restart_and_rejects_stale_generation(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="restart-thread", event_id="restart-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task
            parked = await first._waits.get(event.event_id)
            assert parked is not None and parked.state == "waiting"
            assert parked.deferrals == 1

            h.fake_k8s.quota_rejection = None
            h.runner.default_script = [Final(text="answer after wait", status=DONE)]
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            wakes = await asyncio.gather(
                replacement._waits.wake_due(), replacement._waits.wake_due()
            )
            assert sum(wakes) == 1
            woken = await replacement._waits.get(event.event_id)
            assert woken is not None and woken.state == "woken"
            assert woken.generation == parked.generation + 1
            assert woken.deadline_ms == parked.deadline_ms
            assert await h.async_redis.xlen(h.config.stream) == 2

            stale_fields = to_stream_fields(event)
            stale_fields[WAIT_GENERATION_FIELD] = str(parked.generation)
            await h.async_redis.xadd(h.config.stream, stale_fields)
            replacement_task = asyncio.create_task(replacement.run())
            try:
                await _wait_until(lambda: h.sink.last_text == "answer after wait")
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
                assert h.runner.opened == ["hello"]
                assert h.runner.queried == ["hello"]
                assert h.runner.admissions == [(h.runner.request_epochs[0][1], True)]
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
                finished = await replacement._waits.get(event.event_id)
                assert finished is not None and finished.state == "done"
                assert finished.deadline_ms == parked.deadline_ms
                assert [ref for _, ref, _ in h.sink.updates] == ["p-1", "p-1"]
            finally:
                replacement.request_stop()
                await replacement_task

    asyncio.run(go())


def test_repeated_capacity_refusal_keeps_first_deadline_and_delivery_budget(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            max_delivery=2,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="repeat-thread", event_id="repeat-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            replacement_task = asyncio.create_task(replacement.run())
            try:
                waiting_again = await _wait_capacity_state(
                    replacement, event.event_id, "waiting", generation=parked.generation + 2
                )
                assert waiting_again.deferrals == 2
                assert waiting_again.deadline_ms == parked.deadline_ms
                assert waiting_again.first_wait_ms == parked.first_wait_ms
                assert h.runner.opened == []
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))
                assert not await h.async_redis.exists(f"{h.config.stream}:dead")
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
            finally:
                replacement.request_stop()
                await replacement_task

    asyncio.run(go())


def test_capacity_wake_defers_when_runner_lacks_admission(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="old-runner", event_id="old-runner-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            h.runner.supports_capacity_admission = False
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            replacement_task = asyncio.create_task(replacement.run())
            try:
                deferred = await _wait_capacity_state(
                    replacement,
                    event.event_id,
                    "waiting",
                    generation=parked.generation + 2,
                )
                assert deferred.deadline_ms == parked.deadline_ms
                assert h.runner.opened == []
                assert h.runner.queried == []
                assert h.runner.admissions == []
                assert h.runner.steers == []
            finally:
                replacement.request_stop()
                await replacement_task

    asyncio.run(go())


def test_capacity_wake_defers_behind_live_turn_without_steering(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="busy-wake", event_id="busy-wake-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.turn_scripts = [[TextDelta(text="working")]]
            h.runner.tail = [Final(text="live answer", status=DONE)]
            base_url = f"http://127.0.0.1:{h.substrate._config.runner_port}"
            blocker = await h.kernel._runner.start_turn(
                base_url, Event(type="message", text="live turn", user="U", ts="1")
            )
            await _wait_until(lambda: h.runner.turn_epoch == blocker.turn_epoch)
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            replacement_task = asyncio.create_task(replacement.run())
            try:
                deferred = await _wait_capacity_state(
                    replacement,
                    event.event_id,
                    "waiting",
                    generation=parked.generation + 2,
                )
                assert deferred.deadline_ms == parked.deadline_ms
                assert h.runner.opened == ["live turn"]
                assert h.runner.queried == ["live turn"]
                assert h.runner.admissions == []
                assert h.runner.steers == []
            finally:
                hold.set()
                blocker.close()
                replacement.request_stop()
                await replacement_task

    asyncio.run(go())


def test_capacity_wake_headers_before_runner_lock_do_not_start_turn(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="lock-race", event_id="lock-race-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            h.runner.admission_timeout_s = 0.2
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.turn_scripts = [[TextDelta(text="working")]]
            h.runner.tail = [Final(text="competing answer", status=DONE)]
            real_start = h.kernel._runner.start_turn
            blocker_streams: list[Any] = []

            async def start_after_competing_turn(*args: Any, **kwargs: Any) -> Any:
                if kwargs.get("capacity_admission"):
                    blocker = await real_start(
                        args[0],
                        Event(type="message", text="competing turn", user="U", ts="1"),
                        token=kwargs.get("token"),
                    )
                    blocker_streams.append(blocker)
                    await _wait_until(lambda: h.runner.turn_epoch == blocker.turn_epoch)
                return await real_start(*args, **kwargs)

            h.kernel._runner.start_turn = start_after_competing_turn  # type: ignore[method-assign]
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            replacement_task = asyncio.create_task(replacement.run())
            try:
                await _wait_until(lambda: len(h.runner.request_epochs) == 2)
                assert h.runner.request_epochs[0][1] != h.runner.request_epochs[1][1]
                queued = await replacement._waits.get(event.event_id)
                assert queued is not None and queued.state == "woken"
                assert h.runner.turn_epoch == h.runner.request_epochs[0][1]
                assert h.runner.queried == ["competing turn"]
                assert h.runner.admissions == []
                assert h.runner.steers == []

                expired = await _wait_capacity_state(replacement, event.event_id, "expired")
                assert expired.cause == "capacity_wait_expired"
                assert expired.deadline_ms == parked.deadline_ms
                assert h.runner.queried == ["competing turn"]
                assert not any(allowed for _epoch, allowed in h.runner.admissions)
                assert h.runner.steers == []
            finally:
                hold.set()
                for blocker in blocker_streams:
                    blocker.close()
                replacement.request_stop()
                await replacement_task
            await _wait_until(lambda: not h.runner.turn_active)
            assert h.runner.queried == ["competing turn"]

    asyncio.run(go())


def test_failed_capacity_grant_cannot_start_after_original_deadline(make_harness) -> None:
    """@spec ADR-0130 d1: pre-stream admission failure closes progress authority."""

    from curie_worker.progress import ProgressStore, progress_id_for

    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
            progress_factory=lambda redis, config: ProgressStore(redis, config),
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="grant-failure", event_id="grant-failure-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            h.runner.admission_timeout_s = 0.2
            h.runner.default_script = [Final(text="must not run", status=DONE)]
            real_admit = h.kernel._runner.admit_turn
            failed_grants: list[str] = []

            async def grant_fails_before_send(*args: Any, **kwargs: Any) -> None:
                if kwargs.get("allow") is True:
                    failed_grants.append(args[1])
                    raise RuntimeError("grant request was not sent")
                await real_admit(*args, **kwargs)

            h.kernel._runner.admit_turn = grant_fails_before_send  # type: ignore[method-assign]
            owner = _capacity_consumer(h)
            await owner.ensure_group()
            await h.async_redis.zadd(owner._waits._due, {event.event_id: 0})
            assert await owner._waits.wake_due() == 1
            wake_id, wake_fields = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            await owner._sem.acquire()
            try:
                await owner._handle(wake_id, wake_fields)
            finally:
                h.kernel._runner.admit_turn = real_admit  # type: ignore[method-assign]
            progress_id = progress_id_for(kernel_module._thread_key_for(event), event.event_id)
            progress = await ProgressStore(h.async_redis, h.config).read(progress_id)
            assert progress is not None
            assert progress.active_generation == 0
            assert failed_grants == [h.runner.request_epochs[0][1]]
            assert h.runner.opened == ["hello"]
            assert h.runner.queried == []
            assert h.runner.admissions == []
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))
            await _wait_until(lambda: not h.runner.turn_active)

            server_time = await h.async_redis.time()
            now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
            await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)
            reclaimed = await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                0,
                [wake_id],
            )
            assert reclaimed == [(wake_id, wake_fields)]
            recovery = _capacity_consumer(h)
            await recovery.ensure_group()
            await recovery._sem.acquire()
            await recovery._handle(wake_id, wake_fields)

            expired = await _wait_capacity_state(recovery, event.event_id, "expired")
            assert expired.cause == "capacity_wait_expired"
            assert expired.deadline_ms == parked.deadline_ms
            assert h.runner.opened == ["hello"]
            assert h.runner.queried == []
            assert h.runner.admissions == []
            assert h.sink.last_text == (
                "Your request could not start before its capacity wait ended. Please send it again."
            )
            await _wait_for_pending_count(
                h.async_redis, h.config.stream, h.config.consumer_group, 0
            )

    asyncio.run(go())


def test_lost_capacity_grant_response_recovers_started_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=3.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="lost-grant", event_id="lost-grant-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            response_gate = asyncio.Event()
            hold = asyncio.Event()
            h.runner.admit_response_gate = response_gate
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="answer after lost grant", status=DONE)]
            owner = _capacity_consumer(h)
            await owner.ensure_group()
            await h.async_redis.zadd(owner._waits._due, {event.event_id: 0})
            assert await owner._waits.wake_due() == 1
            wake_id, wake_fields = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            await owner._sem.acquire()
            owner_task = asyncio.create_task(owner._handle(wake_id, wake_fields))
            try:
                await _wait_until(lambda: len(h.runner.admissions) == 1)
                epoch = h.runner.request_epochs[0][1]
                assert h.runner.admissions == [(epoch, True)]
                assert h.runner.admission_results[epoch] == "granted"
                await _wait_until(lambda: h.runner.queried == ["hello"])

                confirm_deadline = time.monotonic() + 5
                while time.monotonic() < confirm_deadline:
                    active = await owner._waits.get(event.event_id)
                    if active is not None and active.grant_confirmed:
                        break
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError("the admitted epoch was not confirmed")
                assert active is not None and active.state == "active"
                assert active.grant_epoch == epoch
                assert any(
                    headers.get("X-Curie-Turn-Epoch") == epoch
                    for headers in h.runner.status_headers
                )

                server_time = await h.async_redis.time()
                now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
                await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)
                assert h.runner.turn_active
                assert not owner_task.done()
                assert all("could not start" not in text for _, _, text in h.sink.updates)

                hold.set()
                await asyncio.wait_for(owner_task, timeout=5)
                done = await _wait_capacity_state(owner, event.event_id, "done")
                assert done.cause == ""
                assert h.sink.last_text == "answer after lost grant"
                assert h.runner.opened == ["hello"]
                assert h.runner.queried == ["hello"]
                assert h.runner.timeout_calls == 0
                assert all("could not start" not in text for _, _, text in h.sink.updates)
            finally:
                response_gate.set()
                hold.set()
                if not owner_task.done():
                    await asyncio.wait_for(owner_task, timeout=5)

    asyncio.run(go())


def test_expired_parked_wait_does_not_interrupt_later_turn_on_same_thread(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=2.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            parked_event = _qevent("parked", thread="shared-thread", event_id="parked-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(parked_event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, parked_event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="later answer", status=DONE)]
            later_event = _qevent("later", thread="shared-thread", event_id="later-turn")
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(later_event))
            task = asyncio.create_task(consumer.run())
            try:
                await _wait_until(lambda: h.runner.turn_active)
                assert h.runner.queried == ["later"]
                assert h.runner.steers == []
                server_time = await h.async_redis.time()
                now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
                await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)
                await consumer._waits.wake_due()
                expired = await _wait_capacity_state(consumer, parked_event.event_id, "expired")
                assert expired.cause == "capacity_wait_expired"
                assert h.runner.turn_active
                assert h.runner.interrupts == 0
                assert h.runner.timeout_calls == 0
                assert h.runner.queried == ["later"]
                assert h.runner.steers == []
                assert not await h.async_redis.exists(h.config.done_key(later_event.event_id))

                hold.set()
                await _wait_key(h.async_redis, h.config.done_key(later_event.event_id))
                assert h.sink.last_text == "later answer"
                assert h.runner.interrupts == 0
                assert h.runner.timeout_calls == 0
            finally:
                hold.set()
                consumer.request_stop()
                await task

    asyncio.run(go())


@pytest.mark.parametrize("record_old_epoch", [False, True])
def test_lost_capacity_wake_lease_does_not_stop_later_turn(
    make_harness, record_old_epoch: bool
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=3.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            parked_event = _qevent("parked", thread="lease-shared", event_id="lease-parked")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(parked_event))
            first_task = asyncio.create_task(first.run())
            try:
                await _wait_capacity_state(first, parked_event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="later answer", status=DONE)]
            later_event = _qevent("later", thread="lease-shared", event_id="lease-later")
            later_task = asyncio.create_task(h.kernel.process_event(later_event))
            try:
                await _wait_until(lambda: h.runner.queried == ["later"])
                later_epoch = h.runner.turn_epoch
                assert later_epoch is not None
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                await h.async_redis.zadd(consumer._waits._due, {parked_event.event_id: 0})
                assert await consumer._waits.wake_due() == 1
                woken = await consumer._waits.get(parked_event.event_id)
                assert woken is not None and woken.state == "woken"
                wake_id, wake_fields = (
                    await h.async_redis.xreadgroup(
                        h.config.consumer_group,
                        h.config.consumer_name,
                        {h.config.stream: ">"},
                        count=1,
                    )
                )[0][1][0]
                lease_store = DeliveryLeaseStore(h.async_redis, h.config)
                lease = await lease_store.acquire(
                    h.config.stream,
                    h.config.consumer_group,
                    wake_id,
                    consumer=h.config.consumer_name,
                )
                old_epoch = uuid.uuid4().hex
                if record_old_epoch:
                    assert old_epoch != later_epoch
                    assert (
                        await consumer._waits.mark_active(
                            parked_event.event_id, woken.generation, lease, old_epoch
                        )
                        == "active"
                    )
                    assert await consumer._waits.confirm_grant(
                        parked_event.event_id, woken.generation, lease, old_epoch
                    )
                    h.runner.admission_results[old_epoch] = "granted"
                await h.async_redis.delete(
                    h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, wake_id)
                )

                await consumer._interrupt_on_lease_lost(wake_id, wake_fields)
                assert h.runner.turn_active
                assert h.runner.turn_epoch == later_epoch
                assert not later_task.done()
                assert h.runner.interrupts == 0
                assert h.runner.timeout_calls == 0
                assert h.runner.queried == ["later"]
                if record_old_epoch:
                    assert any(
                        headers.get("X-Curie-Turn-Epoch") == old_epoch
                        for headers in h.runner.status_headers
                    )

                hold.set()
                await asyncio.wait_for(later_task, timeout=5)
                assert h.sink.last_text == "later answer"
            finally:
                hold.set()
                if not later_task.done():
                    await asyncio.wait_for(later_task, timeout=5)

    asyncio.run(go())


def test_started_wait_turn_finishes_after_wait_deadline(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="active-thread", event_id="active-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            h.fake_k8s.quota_rejection = None
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="finished after deadline", status=DONE)]
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            replacement_task = asyncio.create_task(replacement.run())
            try:
                active = await _wait_capacity_state(replacement, event.event_id, "active")
                assert active.deadline_ms == parked.deadline_ms
                await _wait_until(lambda: h.runner.turn_active)
                server_time = await h.async_redis.time()
                now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
                remaining_ms = max(0, parked.deadline_ms - now_ms)
                await asyncio.sleep(remaining_ms / 1000 + 0.05)
                assert h.runner.turn_active
                assert not await h.async_redis.exists(h.config.done_key(event.event_id))

                hold.set()
                await _wait_until(lambda: h.sink.last_text == "finished after deadline")
                done = await _wait_capacity_state(replacement, event.event_id, "done")
                assert done.cause == ""
                assert h.runner.opened == ["hello"]
                assert all("could not start" not in text for _, _, text in h.sink.updates)
            finally:
                hold.set()
                replacement.request_stop()
                await replacement_task

    asyncio.run(go())


def test_wake_before_deadline_expires_if_turn_cannot_start_until_after_it(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="late-thread", event_id="late-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            woken = await replacement._waits.get(event.event_id)
            assert woken is not None and woken.state == "woken"
            assert woken.deadline_ms == parked.deadline_ms
            server_time = await h.async_redis.time()
            now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
            await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)

            h.fake_k8s.quota_rejection = None
            replacement_task = asyncio.create_task(replacement.run())
            try:
                expired = await _wait_capacity_state(replacement, event.event_id, "expired")
                assert expired.cause == "capacity_wait_expired"
                assert h.runner.opened == []
                assert (
                    len([text for _, _, text in h.sink.updates if "could not start" in text]) == 1
                )
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
            finally:
                replacement.request_stop()
                await replacement_task

    asyncio.run(go())


@pytest.mark.parametrize("accepted_before_deadline", [False, True])
def test_runner_acceptance_boundary_respects_wait_deadline(
    make_harness, accepted_before_deadline: bool
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="late-accept", event_id="late-accept-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            accept = asyncio.Event()
            hold = asyncio.Event()
            accepted = asyncio.Event()
            if not accepted_before_deadline:
                h.runner.accept = accept
            else:
                real_start = h.kernel._runner.start_turn

                async def delayed_start(*args: Any, **kwargs: Any) -> Any:
                    turn = await real_start(*args, **kwargs)
                    accepted.set()
                    try:
                        await accept.wait()
                    except asyncio.CancelledError:
                        pass
                    return turn

                h.kernel._runner.start_turn = delayed_start  # type: ignore[method-assign]
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="late work")]
            h.runner.tail = [Final(text="late answer", status=DONE)]
            h.fake_k8s.quota_rejection = None
            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            wake_id, wake_fields = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            await replacement._sem.acquire()
            wake_task = asyncio.create_task(replacement._handle(wake_id, wake_fields))
            try:
                if accepted_before_deadline:
                    await asyncio.wait_for(accepted.wait(), timeout=2)
                else:
                    await _wait_until(lambda: h.runner.opened == ["hello"])
                waiting_for_acceptance = await replacement._waits.get(event.event_id)
                assert waiting_for_acceptance is not None
                assert waiting_for_acceptance.state == "woken"
                assert h.runner.turn_active is accepted_before_deadline
                server_time = await h.async_redis.time()
                now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
                await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)
                accept.set()
                await asyncio.wait_for(wake_task, timeout=5)

                expired = await _wait_capacity_state(replacement, event.event_id, "expired")
                assert expired.cause == "capacity_wait_expired"
                assert expired.deadline_ms == parked.deadline_ms
                assert h.runner.interrupts == 0
                assert h.runner.queried == []
                assert not any(allowed for _epoch, allowed in h.runner.admissions)
                assert not h.runner.turn_active
                assert h.sink.last_text == (
                    "Your request could not start before its capacity wait ended. "
                    "Please send it again."
                )
                assert all("late answer" not in text for _, _, text in h.sink.updates)
                assert [completion.event_id for completion in h.sink.completions] == [
                    event.event_id
                ]
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
            finally:
                accept.set()
                hold.set()
                if not wake_task.done():
                    await asyncio.wait_for(wake_task, timeout=5)

    asyncio.run(go())


def test_active_wait_delivery_recovers_after_deadline_without_expiry(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=5.0,
            capacity_wait_budget_s=60.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="recovery-thread", event_id="recovery-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            entry_id, fields = await _pending_local_entry(h, event)
            await first._sem.acquire()
            # Park through the real delivery handler before introducing the
            # wake owner, so maintenance cannot race this test's explicit wake.
            await first._handle(entry_id, fields)
            parked = await _wait_capacity_state(first, event.event_id, "waiting")
            await _wait_until(
                lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
            )

            h.fake_k8s.quota_rejection = None
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = []
            owner_stream_reading = asyncio.Event()
            real_iterate = TurnStream.__aiter__

            def iterate_turn(stream: TurnStream) -> AsyncIterator[OutboundEvent]:
                owner_stream_reading.set()
                return real_iterate(stream)

            monkeypatch.setattr(TurnStream, "__aiter__", iterate_turn)
            owner = _capacity_consumer(h)
            await owner.ensure_group()
            await h.async_redis.zadd(owner._waits._due, {event.event_id: 0})
            assert await owner._waits.wake_due() == 1
            woken = await owner._waits.get(event.event_id)
            assert woken is not None and woken.state == "woken"
            claimed = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            wake_id, wake_fields = claimed[0][1][0]
            await owner._sem.acquire()
            owner_task = asyncio.create_task(owner._handle(wake_id, wake_fields))
            try:
                active = await _wait_capacity_state(owner, event.event_id, "active")
                assert active.deadline_ms == parked.deadline_ms
                await _wait_until(lambda: h.runner.queried == ["hello"])
                assert h.runner.admissions == [(h.runner.request_epochs[0][1], True)]
                # The runner accepts the grant before the worker confirms it
                # in Valkey. Recovery requires that persisted confirmation.
                async with asyncio.timeout(10):
                    while True:
                        active = await owner._waits.get(event.event_id)
                        if active is not None and active.grant_confirmed:
                            assert active.state == "active"
                            break
                        await asyncio.sleep(0.01)
                await asyncio.wait_for(owner_stream_reading.wait(), timeout=10)
                assert not owner_task.done()
                owner_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    async with asyncio.timeout(10):
                        await owner_task
                await _wait_until(lambda: not h.runner.turn_active)
            finally:
                hold.set()
                if not owner_task.done():
                    owner_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        async with asyncio.timeout(10):
                            await owner_task

            server_time = await h.async_redis.time()
            now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
            expired_deadline_ms = now_ms - 1
            # Advance the persisted deadline after admission, without making
            # scheduler latency consume the budget needed to reach that state.
            async with h.async_redis.pipeline(transaction=True) as pipe:
                pipe.hset(
                    owner._waits._record(event.event_id),
                    "deadline_ms",
                    expired_deadline_ms,
                )
                pipe.zadd(owner._waits._flight, {event.event_id: expired_deadline_ms})
                pipe.zadd(
                    owner._waits._active,
                    {event.event_id: expired_deadline_ms + owner._waits._retention_ms},
                )
                await pipe.execute()
            expired = await owner._waits.get(event.event_id)
            assert expired is not None and expired.state == "active"
            assert expired.deadline_ms < now_ms
            assert expired.grant_confirmed

            h.runner.default_script = [Final(text="recovered answer", status=DONE)]
            recovery = _capacity_consumer(h)
            await recovery.ensure_group()
            reclaimed = await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                0,
                [wake_id],
            )
            assert reclaimed == [(wake_id, wake_fields)]
            # The cancelled owner can still look busy to the runner status
            # read. That must not park the admitted turn again and edit the
            # thread back to the queued notice.
            h.runner.turn_active = True
            await recovery._sem.acquire()
            await recovery._handle(wake_id, wake_fields)
            assert h.sink.last_text == "recovered answer"
            assert [text for _, _, text in h.sink.updates].count(
                "The agent is busy. Your request is queued and will start when space opens."
            ) == 1
            # A queued edit that loses the race with this answer must not
            # replace it. The notice path is the same one the first consumer
            # used; after the answer it has to no-op.
            before = list(h.sink.updates)
            await h.kernel.notify_capacity_queued(event)
            assert h.sink.updates == before
            assert h.sink.last_text == "recovered answer"
            done = await _wait_capacity_state(recovery, event.event_id, "done")
            assert done.cause == ""
            assert h.runner.opened == ["hello", "hello"]
            assert h.runner.queried == ["hello", "hello"]
            assert h.runner.admissions == [
                (h.runner.request_epochs[0][1], True),
                (h.runner.request_epochs[1][1], True),
            ]
            assert all("could not start" not in text for _, _, text in h.sink.updates)
            await _wait_for_pending_count(
                h.async_redis, h.config.stream, h.config.consumer_group, 0
            )

    asyncio.run(go())


def test_delayed_queued_notice_cannot_overwrite_a_woken_answer(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            monkeypatch.setattr(capacity_wait_module, "_NOTICE_LOCK_MS", 60)
            monkeypatch.setattr(capacity_wait_module, "_NOTICE_SEND_TIMEOUT_S", 0.02)
            notice_entered = asyncio.Event()
            release_notice = asyncio.Event()
            real_emit = h.sink.emit

            async def delayed_emit(
                event: Any, *, route: Any, best_effort_unreachable: bool = False
            ) -> Any:
                if event.event == "reply.update" and "queued" in (event.text or "").lower():
                    notice_entered.set()
                    await release_notice.wait()
                return await real_emit(
                    event, route=route, best_effort_unreachable=best_effort_unreachable
                )

            h.sink.emit = delayed_emit  # type: ignore[method-assign]
            event = _qevent("hello", thread="notice-race", event_id="notice-race-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            first_id, first_fields = await _pending_local_entry(h, event)
            await first._sem.acquire()
            first_task = asyncio.create_task(first._handle(first_id, first_fields))
            try:
                await asyncio.wait_for(notice_entered.wait(), timeout=2)
                parked = await first._waits.get(event.event_id)
                assert parked is not None and parked.state == "waiting"
                await asyncio.sleep(0.1)

                h.fake_k8s.quota_rejection = None
                h.runner.default_script = [Final(text="final answer", status=DONE)]
                replacement = _capacity_consumer(h)
                await replacement.ensure_group()
                await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
                woke = await replacement._waits.wake_due()
                if woke == 0:
                    release_notice.set()
                    await first_task
                    assert await replacement._waits.wake_due() == 1
                else:
                    assert woke == 1

                wake_id, wake_fields = (
                    await h.async_redis.xreadgroup(
                        h.config.consumer_group,
                        h.config.consumer_name,
                        {h.config.stream: ">"},
                        count=1,
                    )
                )[0][1][0]
                await replacement._sem.acquire()
                await replacement._handle(wake_id, wake_fields)
                assert h.sink.last_text == "final answer"
                release_notice.set()
                await first_task
                assert h.sink.last_text == "final answer"
                assert h.runner.opened == ["hello"]
                assert (await replacement._waits.get(event.event_id)).state == "done"
            finally:
                release_notice.set()
                await first_task

    asyncio.run(go())


@pytest.mark.parametrize("started", [False, True])
def test_dead_lettered_wake_gets_one_terminal_reply_and_owed_completion(
    make_harness, started: bool
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=2.0,
            completion_sweep_grace_s=0.0,
            max_delivery=2,
            reclaim_min_idle_ms=0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="dead-wake", event_id="dead-wake-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            woken = await replacement._waits.get(event.event_id)
            assert woken is not None and woken.state == "woken"
            claimed = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            wake_id, _wake_fields = claimed[0][1][0]
            if started:
                granted_epoch = uuid.uuid4().hex
                lease_store = DeliveryLeaseStore(h.async_redis, h.config)
                lease = await lease_store.acquire(
                    h.config.stream,
                    h.config.consumer_group,
                    wake_id,
                    consumer=h.config.consumer_name,
                )
                assert (
                    await replacement._waits.mark_active(
                        event.event_id, woken.generation, lease, granted_epoch
                    )
                    == "active"
                )
                assert await replacement._waits.confirm_grant(
                    event.event_id, woken.generation, lease, granted_epoch
                )
                assert await lease_store.release(
                    h.config.stream,
                    h.config.consumer_group,
                    wake_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                0,
                [wake_id],
            )
            assert (await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group))[
                wake_id
            ] == 2
            assert await replacement._dead_letter_over_cap() == {wake_id}
            assert (
                await _pending_owner(
                    h.async_redis, h.config.stream, h.config.consumer_group, wake_id
                )
                is None
            )
            assert len(await h.async_redis.xrange(h.config.dead_letter_stream_name())) == 1
            assert h.runner.opened == []

            h.sink.fail_events = {"turn.completed"}
            replacement_task = asyncio.create_task(replacement.run())
            try:
                terminal = await _wait_capacity_state(
                    replacement, event.event_id, "expired", generation=woken.generation + 1
                )
                expected_cause = "delivery_exhausted" if started else "capacity_wait_expired"
                assert terminal.cause == expected_cause
                expected_text = (
                    "Your request started but could not finish. Please send it again."
                    if started
                    else "Your request could not start before its capacity wait ended. "
                    "Please send it again."
                )
                assert [text for _, _, text in h.sink.updates].count(expected_text) == 1
                assert h.runner.opened == []
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
                assert await h.async_redis.exists(h.config.completion_key(event.event_id))
                assert await h.async_redis.xlen(h.config.stream) == 3
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
            finally:
                replacement.request_stop()
                await replacement_task

            h.sink.fail_events.clear()
            await h.kernel.sweep_pending_completions()
            assert [c.event_id for c in h.sink.completions] == [event.event_id]
            assert not await h.async_redis.exists(h.config.completion_key(event.event_id))

    asyncio.run(go())


def test_dead_lettered_terminal_recovery_is_requeued_once(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
            max_delivery=2,
            reclaim_min_idle_ms=0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="terminal-retry", event_id="terminal-retry-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            replacement = _capacity_consumer(h)
            await replacement.ensure_group()
            await h.async_redis.zadd(replacement._waits._due, {event.event_id: 0})
            assert await replacement._waits.wake_due() == 1
            wake_id, _ = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                0,
                [wake_id],
            )
            assert await replacement._dead_letter_over_cap() == {wake_id}

            server_time = await h.async_redis.time()
            now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
            await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)
            assert await replacement._waits.reconcile_lost_wakes() == 1
            first_terminal = await replacement._waits.get(event.event_id)
            assert first_terminal is not None and first_terminal.state == "woken"
            await h.async_redis.zadd(replacement._waits._flight, {event.event_id: 0})
            assert await replacement._waits.reconcile_lost_wakes() == 0
            assert await h.async_redis.xlen(h.config.stream) == 3

            terminal_id, _ = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                0,
                [terminal_id],
            )
            assert await replacement._dead_letter_over_cap() == {terminal_id}
            await h.async_redis.zadd(replacement._waits._flight, {event.event_id: 0})
            assert await replacement._waits.reconcile_lost_wakes() == 1
            retry_id, retry_fields = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            assert retry_id != terminal_id
            await replacement._sem.acquire()
            await replacement._handle(retry_id, retry_fields)

            expired = await _wait_capacity_state(replacement, event.event_id, "expired")
            assert expired.cause == "capacity_wait_expired"
            assert expired.generation == first_terminal.generation + 1
            assert h.runner.opened == []
            assert [text for _, _, text in h.sink.updates if "could not start" in text] == [
                "Your request could not start before its capacity wait ended. Please send it again."
            ]
            assert [completion.event_id for completion in h.sink.completions] == [event.event_id]
            assert await h.async_redis.xlen(h.config.stream) == 4
            assert len(await h.async_redis.xrange(h.config.dead_letter_stream_name())) == 2
            await _wait_for_pending_count(
                h.async_redis, h.config.stream, h.config.consumer_group, 0
            )

    asyncio.run(go())


def test_active_dead_letter_stops_runner_before_terminal_reply(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=2.0,
            max_delivery=2,
            reclaim_min_idle_ms=0,
            delivery_lease_ttl_s=15.0,
            delivery_lease_heartbeat_s=5.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="active-dead", event_id="active-dead-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="old answer", status=DONE)]
            h.fake_k8s.quota_rejection = None
            owner = _capacity_consumer(h)
            await owner.ensure_group()
            await h.async_redis.zadd(owner._waits._due, {event.event_id: 0})
            assert await owner._waits.wake_due() == 1
            wake_id, wake_fields = (
                await h.async_redis.xreadgroup(
                    h.config.consumer_group,
                    h.config.consumer_name,
                    {h.config.stream: ">"},
                    count=1,
                )
            )[0][1][0]
            await owner._sem.acquire()
            owner_task = asyncio.create_task(owner._handle(wake_id, wake_fields))
            try:
                assert (await _wait_capacity_state(owner, event.event_id, "active")).cause == ""
                await _wait_until(lambda: h.runner.turn_active)
                await h.async_redis.xclaim(
                    h.config.stream,
                    h.config.consumer_group,
                    h.config.consumer_name,
                    0,
                    [wake_id],
                )
                server_time = await h.async_redis.time()
                now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
                await asyncio.sleep(max(0, parked.deadline_ms - now_ms) / 1000 + 0.05)
                assert h.runner.turn_active
                assert not owner_task.done()
                await h.async_redis.delete(
                    h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, wake_id)
                )

                recovery = _capacity_consumer(h)
                await recovery.ensure_group()
                assert await recovery._dead_letter_over_cap() == {wake_id}
                assert h.runner.turn_active
                assert await recovery._waits.reconcile_lost_wakes() == 1
                terminal_id, terminal_fields = (
                    await h.async_redis.xreadgroup(
                        h.config.consumer_group,
                        h.config.consumer_name,
                        {h.config.stream: ">"},
                        count=1,
                    )
                )[0][1][0]
                active_at_terminal_reply: list[bool] = []
                real_emit = h.sink.emit

                async def observe_emit(
                    reply: Any, *, route: Any, best_effort_unreachable: bool = False
                ) -> Any:
                    if (
                        reply.event == "reply.update"
                        and reply.text
                        == "Your request started but could not finish. Please send it again."
                    ):
                        active_at_terminal_reply.append(h.runner.turn_active)
                    return await real_emit(
                        reply, route=route, best_effort_unreachable=best_effort_unreachable
                    )

                h.sink.emit = observe_emit  # type: ignore[method-assign]
                await recovery._sem.acquire()
                await recovery._handle(terminal_id, terminal_fields)
                expired = await _wait_capacity_state(recovery, event.event_id, "expired")
                assert expired.cause == "delivery_exhausted"
                assert [headers["X-Curie-Turn-Epoch"] for headers in h.runner.timeout_headers] == [
                    h.runner.request_epochs[0][1]
                ]
                assert h.runner.interrupts == 0
                assert active_at_terminal_reply == [False]
                assert not h.runner.turn_active
                assert h.sink.last_text == (
                    "Your request started but could not finish. Please send it again."
                )
                assert [completion.event_id for completion in h.sink.completions] == [
                    event.event_id
                ]
            finally:
                hold.set()
                await asyncio.wait_for(owner_task, timeout=5)

    asyncio.run(go())


def test_failed_queued_notice_is_repaired_after_consumer_restart(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            h.sink.fail_events = {"reply.update"}
            event = _qevent("hello", thread="notice-thread", event_id="notice-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                await _wait_capacity_state(first, event.event_id, "waiting")
            finally:
                first.request_stop()
                await first_task

            undelivered = await first._waits.get(event.event_id)
            assert undelivered is not None and undelivered.notice_pending
            assert h.sink.updates == []
            assert not await h.async_redis.exists(h.config.done_key(event.event_id))

            h.sink.fail_events.clear()
            replacement = _capacity_consumer(h)
            await h.async_redis.zadd(replacement._waits._notices, {event.event_id: 0})
            await replacement._repair_wait_notices()
            repaired = await replacement._waits.get(event.event_id)
            assert repaired is not None and not repaired.notice_pending
            assert len(h.sink.updates) == 1
            assert "queued" in h.sink.updates[0][2].lower()
            assert h.sink.updates[0][:2] == ("C1", "p-1")

    asyncio.run(go())


@pytest.mark.parametrize("reply_outage", [False, True])
def test_capacity_wait_expires_at_its_original_deadline_after_restart(
    make_harness, reply_outage: bool
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
            capacity_wait_budget_s=1.0,
            completion_sweep_grace_s=0.0,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"pods": "1"},
                used={"pods": "2"},
                hard={"pods": "2"},
            )
            event = _qevent("hello", thread="expiry-thread", event_id="expiry-turn")
            first = _capacity_consumer(h)
            await first.ensure_group()
            await h.async_redis.xadd(h.config.stream, to_stream_fields(event))
            first_task = asyncio.create_task(first.run())
            try:
                parked = await _wait_capacity_state(first, event.event_id, "waiting")
                await _wait_until(
                    lambda: any("queued" in text.lower() for _, _, text in h.sink.updates)
                )
            finally:
                first.request_stop()
                await first_task

            if reply_outage:
                h.sink.fail_events = {"reply.update", "turn.completed"}
            await asyncio.sleep(1.05)
            replacement = _capacity_consumer(h)
            replacement_task = asyncio.create_task(replacement.run())
            try:
                expired = await _wait_capacity_state(replacement, event.event_id, "expired")
                await _wait_for_pending_count(
                    h.async_redis, h.config.stream, h.config.consumer_group, 0
                )
                assert expired.cause == "capacity_wait_expired"
                assert expired.deadline_ms == parked.deadline_ms
                assert expired.first_wait_ms == parked.first_wait_ms
                assert expired.deferrals == 1
                assert h.runner.opened == []
                assert await h.async_redis.xlen(h.config.stream) == 2
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
                assert await replacement._waits.snapshot() == {
                    "waiting": 0,
                    "active": 0,
                    "expired": 1,
                }
                expiry_replies = [
                    text for _, _, text in h.sink.updates if "could not start" in text
                ]
                assert len(expiry_replies) == (0 if reply_outage else 1)
                if reply_outage:
                    assert await h.async_redis.exists(h.config.completion_key(event.event_id))
                else:
                    assert [c.event_id for c in h.sink.completions] == [event.event_id]
            finally:
                replacement.request_stop()
                await replacement_task

            if reply_outage:
                h.sink.fail_events.clear()
                await h.kernel.sweep_pending_completions()
                assert [c.event_id for c in h.sink.completions] == [event.event_id]
                assert not await h.async_redis.exists(h.config.completion_key(event.event_id))

    asyncio.run(go())


def test_reclaim_skips_this_consumers_own_inflight_entry(make_harness) -> None:
    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            # A turn that hangs, so its stream entry stays pending (unacked, in
            # flight) while streaming.
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent("hello", thread="ti1", event_id="i1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.runner.turn_active)

            # A reclaim pass while the turn is still in flight must NOT re-dispatch
            # our own entry (which would steer the same prompt into its own turn).
            reclaimed = await consumer._reclaim_once()
            assert reclaimed == 0
            assert h.runner.opened == ["hello"]  # no duplicate turn

            hold.set()
            await _wait_until(lambda: h.sink.last_text == "done")
            consumer.request_stop()
            await task

    asyncio.run(go())


def test_dispatch_applies_backpressure_at_capacity(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A hanging turn holds the single capacity slot; the next dispatch must
            # block (backpressure) rather than claim the entry into a local queue.
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                max_concurrency=1,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            first_id, first = await _pending_local_entry(h, _qevent("a", thread="ta", event_id="a"))
            await consumer._dispatch(first_id, first)
            await _wait_until(lambda: h.runner.turn_active)  # slot taken, turn hanging

            second_id, second_fields = await _pending_local_entry(
                h, _qevent("b", thread="tb", event_id="b")
            )
            second = asyncio.create_task(consumer._dispatch(second_id, second_fields))
            await asyncio.sleep(0.1)
            assert not second.done()  # blocked: capacity is full

            hold.set()  # first turn finishes, frees the slot
            await second  # second dispatch now proceeds
            await asyncio.gather(*list(consumer._inflight))

    asyncio.run(go())


def test_message_settlement_does_not_sample_queue_inventory(make_harness) -> None:
    """Queue gauge reads cannot retain the per-message semaphore slot."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="answer", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            observations = 0

            async def observe() -> None:
                nonlocal observations
                observations += 1

            consumer._observe_queue_state = observe  # type: ignore[method-assign]
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("hot path", thread="t-hot", event_id="hot-1")),
            )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            entry_id, fields = rows[0][1][0]
            await consumer._dispatch(entry_id, fields)
            await asyncio.gather(*list(consumer._inflight))

            assert observations == 0
            # Every configured slot is immediately acquirable again. If the
            # handler were still awaiting queue observation in its finally, the
            # last acquire would time out.
            for _ in range(16):
                await asyncio.wait_for(consumer._sem.acquire(), timeout=0.1)
            for _ in range(16):
                consumer._sem.release()

    asyncio.run(go())


def test_maintenance_tick_samples_queue_inventory_once(make_harness) -> None:
    """The maintenance cadence owns queue inventory observation."""

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            calls: list[str] = []

            async def step(name: str) -> None:
                calls.append(name)

            async def observe() -> None:
                calls.append("observe")
                consumer.request_stop()

            consumer._reclaim_once = lambda: step("reclaim")  # type: ignore[method-assign]
            h.kernel.reap_orphans = lambda: step("reap")  # type: ignore[method-assign]
            h.kernel.sweep_pending_completions = lambda: step("sweep")  # type: ignore[method-assign]
            consumer._drain_thread_reset_requests = lambda: step("reset")  # type: ignore[method-assign]
            consumer._observe_queue_state = observe  # type: ignore[method-assign]

            await consumer._maintenance_loop()

            assert calls == ["reclaim", "reap", "sweep", "reset", "observe"]

    asyncio.run(go())


def test_maintenance_tick_sweeps_the_progress_outbox_after_completions(make_harness) -> None:
    """ADR 0130: the progress outbox is swept on the maintenance cadence, right
    after the completion outbox and ahead of the thread-reset drain."""

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            calls: list[str] = []

            async def step(name: str) -> None:
                calls.append(name)

            async def observe() -> None:
                calls.append("observe")
                consumer.request_stop()

            consumer._reclaim_once = lambda: step("reclaim")  # type: ignore[method-assign]
            h.kernel.reap_orphans = lambda: step("reap")  # type: ignore[method-assign]
            h.kernel.sweep_pending_completions = lambda: step("sweep")  # type: ignore[method-assign]
            consumer._sweep_pending_progress = lambda: step("progress")  # type: ignore[method-assign]
            consumer._drain_pending_progress_inboxes = lambda: step("inboxes")  # type: ignore[method-assign]
            consumer._drain_thread_reset_requests = lambda: step("reset")  # type: ignore[method-assign]
            consumer._observe_queue_state = observe  # type: ignore[method-assign]

            await consumer._maintenance_loop()

            assert calls == [
                "reclaim",
                "reap",
                "sweep",
                "progress",
                "inboxes",
                "reset",
                "observe",
            ]

    asyncio.run(go())


def test_a_failing_progress_sweep_does_not_stop_the_maintenance_tick(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            calls: list[str] = []

            async def step(name: str) -> None:
                calls.append(name)

            async def observe() -> None:
                calls.append("observe")
                consumer.request_stop()

            async def broken_sweep(*_args: object, **_kwargs: object) -> None:
                calls.append("progress")
                raise RuntimeError("progress store unreachable")

            monkeypatch.setattr(consumer_module, "sweep_pending_progress", broken_sweep)
            consumer._reclaim_once = lambda: step("reclaim")  # type: ignore[method-assign]
            h.kernel.reap_orphans = lambda: step("reap")  # type: ignore[method-assign]
            h.kernel.sweep_pending_completions = lambda: step("sweep")  # type: ignore[method-assign]
            consumer._drain_thread_reset_requests = lambda: step("reset")  # type: ignore[method-assign]
            consumer._observe_queue_state = observe  # type: ignore[method-assign]

            with caplog.at_level(logging.ERROR, logger="curie_worker.consumer"):
                await consumer._maintenance_loop()

            assert calls == ["reclaim", "reap", "sweep", "progress", "reset", "observe"]
            assert any("progress outbox sweep failed" in r.getMessage() for r in caplog.records)

    asyncio.run(go())


def test_maintenance_tick_settles_pending_progress_without_a_deliverer(make_harness) -> None:
    """The real tick against real keys: nothing delivers progress yet, so the
    tick quarantines a malformed record and dead-letters one whose attempts are
    spent, and leaves a deliverable one owed with no attempt charged."""
    from channel_protocol import ProgressCommand, ProgressState
    from channel_protocol.reply import ReplyTarget
    from curie_worker.progress import ProgressStore, card_delivery_id
    from curie_worker.reply_sink import TargetRoute

    async def go() -> None:
        async with make_harness() as h:
            graveyard = h.config.dead_letter_stream_name()
            store = ProgressStore(h.async_redis, h.config)
            target = ReplyTarget(
                kind="slack",
                address="C0EXAMPLE1",
                conversation_id="1700000000.000100",
                reply_ref="1700000000.000200",
            )
            command = ProgressCommand(
                version="1.0",
                update_id="u1",
                state=ProgressState.INVESTIGATING,
                summary="Reading the ledger",
            )
            ids: list[str] = []
            for root in ("Ev0EXAMPLE1", "Ev0EXAMPLE2"):
                pid = await store.open_chain("slack:C0EXAMPLE1:1700000000.000100", root)
                await store.apply_model_command(
                    pid,
                    command,
                    epoch=1,
                    seq=1,
                    route=TargetRoute(adapter="acme-bot"),
                    target=target,
                )
                ids.append(card_delivery_id(pid))
            fresh, spent = ids
            await h.async_redis.hset(h.config.progress_delivery_key(spent), "attempts", "5")
            malformed = str(uuid.uuid4())
            await h.async_redis.hset(
                h.config.progress_delivery_key(malformed), mapping={"event": "{not json"}
            )
            await h.async_redis.sadd(h.config.progress_pending_key(), malformed)

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )

            async def observe() -> None:
                consumer.request_stop()

            async def noop() -> None:
                return None

            consumer._reclaim_once = noop  # type: ignore[method-assign]
            consumer._drain_thread_reset_requests = noop  # type: ignore[method-assign]
            consumer._observe_queue_state = observe  # type: ignore[method-assign]
            try:
                await consumer._maintenance_loop()

                assert await h.async_redis.smembers(h.config.progress_pending_key()) == {fresh}
                owed = await store.read_delivery(fresh)
                assert owed is not None
                assert owed.attempts == 0
                assert await h.async_redis.exists(h.config.progress_delivery_key(malformed)) == 1
                rows = [fields for _id, fields in await h.async_redis.xrange(graveyard)]
                progress_rows = [r for r in rows if r.get("dl_source") == "progress-outbox"]
                assert [r["delivery_id"] for r in progress_rows] == [spent]
                assert progress_rows[0]["dl_reason"] == "max-attempts-exceeded"
            finally:
                await h.async_redis.delete(graveyard)

    asyncio.run(go())


def test_ensure_group_does_not_replay_preexisting_backlog(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A stale entry already on the stream BEFORE the group is created (a
            # persistent Valkey carrying a backlog from a prior deploy). Creating
            # the group at "$" must skip it; creating at "0" would storm it.
            stale = _qevent("stale", thread="tb1", event_id="b1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(stale))

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            # An entry produced AFTER the group exists must still be delivered.
            fresh = _qevent("fresh", thread="tb2", event_id="b2")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(fresh))
            h.runner.default_script = [Final(text="answer", status=DONE)]

            task = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.sink.last_text == "answer")
            consumer.request_stop()
            await task

            # Only the post-group entry ran; the stale backlog was never opened.
            assert h.runner.opened == ["fresh"]

    asyncio.run(go())


def test_read_loop_survives_transient_redis_timeout(make_harness, caplog) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            # The first blocking read raises a transient redis TimeoutError (the
            # routine idle case) and the second a ConnectionError (a real fault).
            # The loop must survive both and process the next read; an unguarded
            # read would kill the worker. The two are logged at different levels:
            # an idle timeout is DEBUG (not log-worthy every idle interval), a
            # connection blip stays WARNING.
            real = h.async_redis.xreadgroup
            calls = {"n": 0}

            async def flaky(*args: object, **kwargs: object) -> object:
                calls["n"] += 1
                if calls["n"] == 1:
                    raise redis.exceptions.TimeoutError("simulated blocking-read timeout")
                if calls["n"] == 2:
                    raise redis.exceptions.ConnectionError("simulated connection blip")
                return await real(*args, **kwargs)

            consumer._redis.xreadgroup = flaky  # type: ignore[method-assign,assignment]

            h.runner.default_script = [Final(text="answer", status=DONE)]
            qe = _qevent("hello", thread="tt1", event_id="t1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            with caplog.at_level(logging.DEBUG, logger="curie_worker.consumer"):
                task = asyncio.create_task(consumer.run())
                await _wait_until(lambda: h.sink.last_text == "answer")
                consumer.request_stop()
                await task

            assert calls["n"] >= 3  # it retried after both injected faults
            assert h.runner.opened == ["hello"]

            recs = [r for r in caplog.records if r.name == "curie_worker.consumer"]
            timeout_recs = [r for r in recs if "simulated blocking-read timeout" in r.getMessage()]
            conn_recs = [r for r in recs if "simulated connection blip" in r.getMessage()]
            assert timeout_recs and all(r.levelno == logging.DEBUG for r in timeout_recs)
            assert conn_recs and all(r.levelno == logging.WARNING for r in conn_recs)

    asyncio.run(go())


def test_reclaims_and_reprocesses_a_dead_consumers_pending_entry(make_harness) -> None:
    async def go() -> None:
        async with make_harness(reclaim_min_idle_ms=0) as h:
            h.runner.default_script = [Final(text="recovered", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent("orphan", thread="tr1", event_id="r1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            # A different (now "dead") consumer takes delivery but never acks,
            # leaving the entry pending — the crash mid-run case.
            dead = await h.async_redis.xreadgroup(
                h.config.consumer_group, "dead-consumer", {h.config.stream: ">"}, count=1
            )
            assert dead

            # Our consumer reclaims the pending entry and reprocesses it.
            reclaimed = await consumer._reclaim_once()
            assert reclaimed == 1
            await _wait_until(lambda: h.sink.last_text == "recovered")
            await asyncio.gather(*list(consumer._inflight))

            assert h.runner.opened == ["orphan"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0  # reclaimed entry acked after reprocessing

    asyncio.run(go())


def test_reclaims_a_dead_consumers_pending_entry_without_waiting_min_idle(
    make_harness,
) -> None:
    """#1532 extension: a terminated consumer's pending entry is recovered
    promptly. ``reclaim_min_idle_ms`` stays at the 15-minute production default
    so XAUTOCLAIM cannot be the path that succeeds; recovery must come from the
    dead-consumer idle check instead.
    """

    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            h.runner.default_script = [Final(text="recovered", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent("orphan", thread="tr-dead", event_id="r-dead")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            dead = await h.async_redis.xreadgroup(
                h.config.consumer_group, "dead-consumer", {h.config.stream: ">"}, count=1
            )
            assert dead
            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="dead-consumer",
                heartbeat_ttl_ms=1,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            capable_key = consumer_heartbeat_capable_key(
                h.config.stream, h.config.consumer_group, "dead-consumer"
            )
            key = consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "dead-consumer")
            await _wait_key(h.async_redis, key, present=False)
            assert not await h.async_redis.exists(key)
            await _wait_consumer_idle(
                h.async_redis,
                h.config.stream,
                h.config.consumer_group,
                "dead-consumer",
                h.config.dead_consumer_idle_ms,
            )
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 1

            # One missing lease can be a Valkey blip; prompt reclaim requires a
            # second absence at least one complete heartbeat TTL later.
            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            pending = await h.async_redis.xpending_range(
                h.config.stream, h.config.consumer_group, min="-", max="+", count=1
            )
            assert int(pending[0]["time_since_delivered"]) < h.config.reclaim_min_idle_ms
            reclaimed = await consumer._prompt_reclaim_once()
            assert reclaimed == 1
            await _wait_until(lambda: h.sink.last_text == "recovered")
            await asyncio.gather(*list(consumer._inflight))
            assert h.runner.opened == ["orphan"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0
            await h.async_redis.delete(capable_key)

    asyncio.run(go())


def test_prompt_reclaim_arbitrates_across_replicas_without_burning_delivery_budget(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="reclaimed")]
            h.runner.tail = [Final(text="done", status=DONE)]
            first_config = h.config.model_copy(update={"consumer_name": "replacement-a"})
            second_config = h.config.model_copy(update={"consumer_name": "replacement-b"})
            first = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=first_config,
                leases=DeliveryLeaseStore(h.async_redis, first_config),
            )
            second = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=second_config,
                leases=DeliveryLeaseStore(h.async_redis, second_config),
            )
            await first.ensure_group()

            entry_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("race", thread="tr-race", event_id="r-race")),
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group,
                "dead-race-peer",
                {h.config.stream: ">"},
                count=1,
            )
            await h.async_redis.set(
                consumer_heartbeat_capable_key(
                    h.config.stream, h.config.consumer_group, "dead-race-peer"
                ),
                "1",
                px=h.config.consumer_capability_ttl_ms,
            )

            assert await asyncio.gather(
                first._prompt_reclaim_once(), second._prompt_reclaim_once()
            ) == [0, 0]
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            results = await asyncio.gather(
                first._prompt_reclaim_once(), second._prompt_reclaim_once()
            )
            assert sum(results) == 1
            await _wait_until(lambda: h.runner.turn_active)

            rows = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min=entry_id,
                max=entry_id,
                count=1,
            )
            assert len(rows) == 1
            assert rows[0]["consumer"] in {"replacement-a", "replacement-b"}
            assert int(rows[0]["times_delivered"]) == 2
            assert h.runner.opened == ["race"]

            hold.set()
            await asyncio.gather(*list(first._inflight | second._inflight))
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

    asyncio.run(go())


def test_local_generation_bootstrap_contends_with_peer_transfer_lease(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            owner_config = h.config.model_copy(update={"consumer_name": "restart-owner"})
            peer_config = h.config.model_copy(update={"consumer_name": "replacement-peer"})
            owner = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=owner_config,
                leases=DeliveryLeaseStore(h.async_redis, owner_config),
            )
            peer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=peer_config,
                leases=DeliveryLeaseStore(h.async_redis, peer_config),
            )
            owner._liveness_store = ConsumerLivenessStore(h.async_redis)
            peer._liveness_store = ConsumerLivenessStore(h.async_redis)
            await owner.ensure_group()

            entry_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("bootstrap-race", thread="tr-bootstrap", event_id="r-bootstrap")
                ),
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group,
                owner_config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )

            # Model a peer that has already won the transfer lease while the
            # stable-name consumer starts its replacement generation.
            token = await peer._liveness_store.try_acquire_reclaim(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer=owner_config.consumer_name,
                ttl_ms=60_000,
            )
            assert token is not None
            bootstrap = asyncio.create_task(owner._recover_local_pending_once())
            try:
                await asyncio.sleep(0.03)
                assert not bootstrap.done()
                rows = await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    min=entry_id,
                    max=entry_id,
                    count=1,
                )
                assert len(rows) == 1
                assert rows[0]["consumer"] == owner_config.consumer_name
                assert int(rows[0]["times_delivered"]) == 1
                assert h.runner.opened == []

                async with peer._reclaim_lock:
                    entries = await peer._claim_consumer_pending_locked(
                        owner_config.consumer_name, set()
                    )
                assert await peer._dispatch_reclaimed(entries) == 1
            finally:
                await peer._liveness_store.release_reclaim(
                    stream=h.config.stream,
                    group=h.config.consumer_group,
                    consumer=owner_config.consumer_name,
                    token=token,
                )

            await asyncio.wait_for(bootstrap, timeout=1)
            await _wait_until(lambda: h.runner.opened == ["bootstrap-race"])
            await asyncio.gather(*list(peer._inflight))
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

    asyncio.run(go())


def test_reclaim_does_not_promptly_steal_from_unknown_peer_without_heartbeat_capability(
    make_harness,
) -> None:
    """A pre-marker worker stays on the long XAUTOCLAIM backstop."""

    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent("unknown", thread="tr-unknown", event_id="r-unknown")
            entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            claimed = await h.async_redis.xreadgroup(
                h.config.consumer_group, "unknown-peer", {h.config.stream: ">"}, count=1
            )
            assert claimed
            capable_key = consumer_heartbeat_capable_key(
                h.config.stream, h.config.consumer_group, "unknown-peer"
            )
            heartbeat_key = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, "unknown-peer"
            )
            assert not await h.async_redis.exists(capable_key)
            assert not await h.async_redis.exists(heartbeat_key)
            await _wait_consumer_idle(
                h.async_redis,
                h.config.stream,
                h.config.consumer_group,
                "unknown-peer",
                h.config.dead_consumer_idle_ms,
            )
            before = await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group)
            assert before == {entry_id: 1}

            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            assert await consumer._prompt_reclaim_once() == 0
            assert (
                await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group) == before
            )
            pending = await h.async_redis.xpending_range(
                h.config.stream,
                h.config.consumer_group,
                min="-",
                max="+",
                count=10,
                consumername="unknown-peer",
            )
            assert [str(row["message_id"]) for row in pending] == [entry_id]
            assert h.runner.opened == []

    asyncio.run(go())


def test_reclaim_does_not_steal_from_a_fresh_live_peer_when_min_idle_is_high(
    make_harness,
) -> None:
    """A live overlapping replica (rolling update) still has a near-zero
    consumer idle because its read loop keeps issuing XREADGROUP. The
    dead-consumer path must not steal that replica's in-flight entry just
    because the entry itself is already pending.
    """

    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent("live", thread="tr-live", event_id="r-live")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            live = await h.async_redis.xreadgroup(
                h.config.consumer_group, "live-peer", {h.config.stream: ">"}, count=1
            )
            assert live
            capable_key = consumer_heartbeat_capable_key(
                h.config.stream, h.config.consumer_group, "live-peer"
            )
            await h.async_redis.set(capable_key, "1")
            key = consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "live-peer")
            await h.async_redis.set(key, "alive", px=HEARTBEAT_TTL_MS)
            await _wait_consumer_idle(
                h.async_redis,
                h.config.stream,
                h.config.consumer_group,
                "live-peer",
                h.config.dead_consumer_idle_ms,
            )
            before = await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group)
            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            reclaimed = await consumer._prompt_reclaim_once()
            assert reclaimed == 0
            assert (
                await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group) == before
            )
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 1
            assert h.runner.opened == []
            await h.async_redis.delete(key, capable_key)

    asyncio.run(go())


def test_reclaim_does_not_steal_from_a_live_saturated_peer(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            ids = []
            for text in ("busy", "queued"):
                ids.append(
                    await h.async_redis.xadd(h.config.stream, to_stream_fields(_qevent(text)))
                )
            claimed = await h.async_redis.xreadgroup(
                h.config.consumer_group, "saturated-peer", {h.config.stream: ">"}, count=2
            )
            assert claimed
            capable_key = consumer_heartbeat_capable_key(
                h.config.stream, h.config.consumer_group, "saturated-peer"
            )
            await h.async_redis.set(capable_key, "1")
            key = consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "saturated-peer")
            await h.async_redis.set(key, "alive", px=HEARTBEAT_TTL_MS)
            await _wait_consumer_idle(
                h.async_redis,
                h.config.stream,
                h.config.consumer_group,
                "saturated-peer",
                h.config.dead_consumer_idle_ms,
            )
            before = await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group)
            assert set(before) == set(ids)
            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            assert await consumer._prompt_reclaim_once() == 0
            assert (
                await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group) == before
            )
            assert h.runner.opened == []
            await h.async_redis.delete(key, capable_key)

    asyncio.run(go())


def test_consumer_publishes_before_reads_and_renews_alive_and_capability(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=3000,
            read_block_ms=10,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            alive = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )
            capable = consumer_heartbeat_capable_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )
            read_observations: list[tuple[bool, bool]] = []
            real_read = consumer._redis.xreadgroup

            async def observe_first_read(*args: Any, **kwargs: Any) -> Any:
                read_observations.append(
                    (
                        bool(await h.async_redis.exists(alive)),
                        bool(await h.async_redis.exists(capable)),
                    )
                )
                return await real_read(*args, **kwargs)

            consumer._redis.xreadgroup = observe_first_read  # type: ignore[method-assign,assignment]

            task = asyncio.create_task(consumer.run())
            await _wait_key(h.async_redis, alive)
            await _wait_key(h.async_redis, capable)
            # Live longer than the capability marker's original TTL. Both keys
            # surviving proves the refresher renews capability as well as alive.
            await asyncio.sleep(0.55)
            assert await h.async_redis.pttl(alive) > 0
            assert await h.async_redis.pttl(capable) > 0
            assert read_observations and read_observations[0] == (True, True)

            consumer.request_stop()
            await task
            assert not await h.async_redis.exists(alive)
            assert await h.async_redis.exists(capable)

    asyncio.run(go())


def test_graceful_stop_keeps_lease_through_two_ttls_of_inflight_drain(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=450,
            read_block_ms=10,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("drain", thread="th-drain", event_id="drain")),
            )
            alive = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )

            task = asyncio.create_task(consumer.run())
            await _wait_until_turn_active_or_consumer_failed(h.runner, task)
            await _wait_key(h.async_redis, alive)
            consumer.request_stop()
            await asyncio.sleep(0.32)
            assert await h.async_redis.exists(alive), (
                "graceful stop dropped liveness while the owned handler still drained"
            )
            assert not task.done()

            hold.set()
            await task
            assert not await h.async_redis.exists(alive)

    asyncio.run(go())


def test_hang_renewals_during_inflight_turn_expire_liveness(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=450,
            read_block_ms=10,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            consumer._liveness_store = _RenewalProbeStore(  # type: ignore[assignment]
                ConsumerLivenessStore(h.async_redis), hang_renewals=True
            )
            await consumer.ensure_group()
            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("hang", thread="th-hang", event_id="hang")),
            )
            alive = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )

            task = asyncio.create_task(consumer.run())
            await _wait_until_turn_active_or_consumer_failed(h.runner, task)
            with pytest.raises(ConsumerLivenessExpired):
                await asyncio.wait_for(task, timeout=2)
            assert not await h.async_redis.exists(alive)
            hold.set()

    asyncio.run(go())


def test_slow_renewal_inside_guard_keeps_lease_alive(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=450,
            read_block_ms=10,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            probe = _RenewalProbeStore(ConsumerLivenessStore(h.async_redis), slow_renewal_s=0.04)
            consumer._liveness_store = probe  # type: ignore[assignment]
            alive = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )

            task = asyncio.create_task(consumer.run())
            deadline = time.monotonic() + 2
            while probe.slow_completed < 1 and time.monotonic() < deadline:
                if task.done():
                    exc = task.exception()
                    if exc is not None:
                        raise exc
                    raise AssertionError(
                        f"consumer finished during slow renewal: {task.result()!r}"
                    )
                await asyncio.sleep(0.005)
            assert probe.slow_completed >= 1
            assert not task.done()
            assert await h.async_redis.exists(alive)

            consumer.request_stop()
            await task

    asyncio.run(go())


def test_real_saturated_consumer_renews_lease_and_peer_cannot_prompt_claim(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=6000,
            read_block_ms=10,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="busy")]
            h.runner.tail = [Final(text="done", status=DONE)]
            peer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                max_concurrency=1,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await peer.ensure_group()
            ids = [
                await h.async_redis.xadd(
                    h.config.stream,
                    to_stream_fields(_qevent(text, event_id=f"sat-{text}")),
                )
                for text in ("held", "semaphore-blocked")
            ]
            alive = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )

            peer_task = asyncio.create_task(peer.run())
            await _wait_until(lambda: h.runner.turn_active)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if set(
                    await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group)
                ) == set(ids):
                    break
                await asyncio.sleep(0.005)
            before = await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group)
            assert before == {ids[0]: 1, ids[1]: 1}
            await asyncio.sleep(0.32)
            assert await h.async_redis.exists(alive)

            replacement_config = h.config.model_copy(update={"consumer_name": "replacement"})
            replacement = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=replacement_config,
                leases=DeliveryLeaseStore(h.async_redis, replacement_config),
            )
            assert await replacement._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            assert await replacement._prompt_reclaim_once() == 0
            assert (
                await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group) == before
            )

            peer.request_stop()
            hold.set()
            await peer_task

    asyncio.run(go())


def test_alive_restoration_resets_two_absence_proof(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent("restored", event_id="restored"))
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group, "peer", {h.config.stream: ">"}, count=1
            )
            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="peer",
                heartbeat_ttl_ms=1,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            await _wait_key(
                h.async_redis,
                consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "peer"),
                present=False,
            )
            assert await consumer._prompt_reclaim_once() == 0

            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="peer",
                heartbeat_ttl_ms=100,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            assert await consumer._prompt_reclaim_once() == 0
            assert "peer" not in consumer._peer_absent_since
            assert (
                await _pending_owner(
                    h.async_redis, h.config.stream, h.config.consumer_group, entry_id
                )
                == "peer"
            )

            await h.async_redis.delete(
                consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "peer")
            )
            assert await consumer._prompt_reclaim_once() == 0
            assert (
                await _pending_owner(
                    h.async_redis, h.config.stream, h.config.consumer_group, entry_id
                )
                == "peer"
            )

    asyncio.run(go())


def test_disappeared_consumer_invalidates_first_absence_observation(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent("gone", event_id="gone"))
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group, "vanished-peer", {h.config.stream: ">"}, count=1
            )
            await h.async_redis.set(
                consumer_heartbeat_capable_key(
                    h.config.stream, h.config.consumer_group, "vanished-peer"
                ),
                "1",
                px=h.config.consumer_capability_ttl_ms,
            )
            assert await consumer._prompt_reclaim_once() == 0
            assert "vanished-peer" in consumer._peer_absent_since

            await h.async_redis.xack(h.config.stream, h.config.consumer_group, entry_id)
            await h.async_redis.xgroup_delconsumer(
                h.config.stream, h.config.consumer_group, "vanished-peer"
            )
            assert await consumer._prompt_reclaim_once() == 0
            assert "vanished-peer" not in consumer._peer_absent_since

    asyncio.run(go())


def test_live_at_cap_peer_is_not_dead_lettered_by_prompt_path(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_delivery=2,
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent("live-cap", event_id="live-cap"))
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group, "live-cap-peer", {h.config.stream: ">"}, count=1
            )
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                "live-cap-peer",
                0,
                [entry_id],
            )
            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="live-cap-peer",
                heartbeat_ttl_ms=100,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            before = await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group)
            assert before == {entry_id: 2}
            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            assert await consumer._prompt_reclaim_once() == 0
            assert (
                await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group) == before
            )
            assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0

    asyncio.run(go())


def test_proven_dead_at_cap_peer_is_dead_lettered_without_xclaim(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_delivery=2,
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent("dead-cap", event_id="dead-cap"))
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group, "dead-cap-peer", {h.config.stream: ">"}, count=1
            )
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                "dead-cap-peer",
                0,
                [entry_id],
            )
            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="dead-cap-peer",
                heartbeat_ttl_ms=1,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            await _wait_key(
                h.async_redis,
                consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "dead-cap-peer"),
                present=False,
            )
            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            assert await consumer._prompt_reclaim_once() == 0

            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0
            rows = await h.async_redis.xrange(h.config.dead_letter_stream_name())
            assert len(rows) == 1
            assert rows[0][1]["dl_original_id"] == entry_id
            # A prompt XCLAIM would increment this to 3. Direct dead-lettering
            # preserves the durable count at the configured cap.
            assert rows[0][1]["dl_delivery_count"] == "2"
            assert h.runner.opened == []

    asyncio.run(go())


def test_transient_liveness_renewal_failure_recovers_before_lease_expiry(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=450,
            read_block_ms=10,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            probe = _RenewalProbeStore(ConsumerLivenessStore(h.async_redis), fail_renewals=1)
            consumer._liveness_store = probe  # type: ignore[assignment]
            alive = consumer_heartbeat_key(
                h.config.stream, h.config.consumer_group, h.config.consumer_name
            )

            task = asyncio.create_task(consumer.run())
            deadline = time.monotonic() + 2
            while probe.renew_calls < 2 and time.monotonic() < deadline:
                await asyncio.sleep(0.005)
            assert probe.renew_calls >= 2
            assert not task.done()
            assert await h.async_redis.exists(alive)

            consumer.request_stop()
            await task

    asyncio.run(go())


def test_timed_out_liveness_renewal_retries_before_lease_expiry(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            consumer_heartbeat_ttl_ms=450,
            consumer_capability_ttl_ms=450,
            delivery_lease_heartbeat_s=0.1,
            read_block_ms=10,
        ) as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            probe = _RenewalProbeStore(ConsumerLivenessStore(h.async_redis), timeout_renewals=1)
            consumer._liveness_store = probe  # type: ignore[assignment]

            task = asyncio.create_task(consumer.run())
            deadline = time.monotonic() + 2
            while probe.renew_calls < 2 and time.monotonic() < deadline:
                await asyncio.sleep(0.005)
            assert probe.renew_calls >= 2
            assert not task.done()

            consumer.request_stop()
            await task

    asyncio.run(go())


def test_terminal_liveness_failure_cancels_generation_and_clean_restart_recovers_pel(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=300,
            reclaim_interval_s=0.02,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=150,
            consumer_capability_ttl_ms=3000,
            read_block_ms=10,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="started")]
            h.runner.tail = [Final(text="recovered", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                max_concurrency=1,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            consumer._liveness_store = _RenewalProbeStore(  # type: ignore[assignment]
                ConsumerLivenessStore(h.async_redis), hang_renewals=True
            )
            await consumer.ensure_group()
            entry_id = await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("restart", thread="th-restart", event_id="restart")),
            )

            first = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.runner.turn_active)
            consumer._peer_absent_since["stale-generation-peer"] = time.monotonic()
            with pytest.raises(ConsumerLivenessExpired):
                await asyncio.wait_for(first, timeout=2)

            assert (
                await _pending_owner(
                    h.async_redis, h.config.stream, h.config.consumer_group, entry_id
                )
                == h.config.consumer_name
            )
            assert not consumer._inflight_ids
            assert not consumer._inflight
            assert not consumer._peer_absent_since
            assert consumer._sem._value == 1  # noqa: SLF001 - generation accounting invariant
            assert not [
                task
                for task in asyncio.all_tasks()
                if task is not asyncio.current_task()
                and not task.done()
                and task.get_name().startswith("consumer:")
            ]

            # This models the top-level supervisor's clean retry generation. It
            # must recover the row canceled under its own stable consumer name;
            # otherwise the prompt path skips that name and the row waits for
            # the 15-minute XAUTOCLAIM fallback.
            hold.set()
            h.runner.hold = None
            # The canceled aiohttp stream can die before FakeRunner's normal
            # epilogue clears this test-only flag. A replacement sandbox starts
            # idle, so reset the fake's process-local state explicitly.
            h.runner.turn_active = False
            h.runner.tail = []
            h.runner.default_script = [Final(text="recovered", status=DONE)]
            consumer._liveness_store = ConsumerLivenessStore(h.async_redis)
            second = asyncio.create_task(consumer.run())
            await _wait_until(lambda: h.runner.opened.count("restart") == 2, timeout=3)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                if summary["pending"] == 0:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("restart generation did not ack its own reclaimed entry")
            consumer.request_stop()
            await second

            assert h.runner.opened.count("restart") == 2
            assert await _deliveries(h.async_redis, h.config.stream, h.config.consumer_group) == {}
            assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                "pending"
            ] == 0

    asyncio.run(go())


def test_prompt_selection_stays_timely_while_heavy_reclaim_waits_for_capacity(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            reclaim_min_idle_ms=80,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=160,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="held")]
            h.runner.tail = [Final(text="done", status=DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                max_concurrency=1,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            # Occupy the only handler slot. Reclaimed handlers will block in
            # _dispatch after ownership transfer; that wait must not retain the
            # shared selection lock.
            local_id, local_fields = await _pending_local_entry(
                h, _qevent("local", event_id="local-only")
            )
            await consumer._dispatch(local_id, local_fields)
            await _wait_until(lambda: h.runner.turn_active)

            old_id = await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent("old", event_id="old"))
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group, "backstop-peer", {h.config.stream: ">"}, count=1
            )
            await asyncio.sleep(0.09)

            prompt_id = await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent("prompt", event_id="prompt"))
            )
            assert await h.async_redis.xreadgroup(
                h.config.consumer_group, "prompt-peer", {h.config.stream: ">"}, count=1
            )
            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer="prompt-peer",
                heartbeat_ttl_ms=1,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            await _wait_key(
                h.async_redis,
                consumer_heartbeat_key(h.config.stream, h.config.consumer_group, "prompt-peer"),
                present=False,
            )

            heavy = asyncio.create_task(consumer._reclaim_once())
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                if (
                    await _pending_owner(
                        h.async_redis, h.config.stream, h.config.consumer_group, old_id
                    )
                    == h.config.consumer_name
                ):
                    break
                await asyncio.sleep(0.005)
            assert (
                await _pending_owner(
                    h.async_redis, h.config.stream, h.config.consumer_group, old_id
                )
                == h.config.consumer_name
            )
            assert not heavy.done(), "heavy dispatch should be blocked on the occupied semaphore"

            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            prompt = asyncio.create_task(consumer._prompt_reclaim_once())
            deadline = time.monotonic() + 0.25
            while time.monotonic() < deadline:
                if (
                    await _pending_owner(
                        h.async_redis, h.config.stream, h.config.consumer_group, prompt_id
                    )
                    == h.config.consumer_name
                ):
                    break
                await asyncio.sleep(0.005)
            assert (
                await _pending_owner(
                    h.async_redis, h.config.stream, h.config.consumer_group, prompt_id
                )
                == h.config.consumer_name
            )
            assert not prompt.done(), "prompt dispatch should now wait outside the selection lock"

            for task in (heavy, prompt):
                task.cancel()
            await asyncio.gather(heavy, prompt, return_exceptions=True)
            hold.set()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*list(consumer._inflight), return_exceptions=True),
                    timeout=1,
                )

    asyncio.run(go())


def test_next_turn_drains_queued_eval_reset_before_claiming(
    make_harness, thread_reset_keys
) -> None:
    """#1534: eval-owned sandboxes are SADDed onto THREAD_RESET_SET after each
    case. The next runs-lane turn must release them before it claims, so a
    following eval case or cluster message does not wait for the 30s
    maintenance tick (or the 90s claim timeout) with the quota already full.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("eval-case-1", thread="tEval1"))
            first_thread_key = _thread_key("tEval1")
            second_thread_key = _thread_key("tEval2")
            assert h.substrate.lookup(first_thread_key) is not None

            await h.async_redis.sadd(thread_reset_keys.requests, first_thread_key)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            nxt = _qevent("eval-case-2", thread="tEval2", event_id="eval-2")
            entry_id, fields = await _pending_local_entry(h, nxt)
            await consumer._sem.acquire()
            await consumer._handle(entry_id, fields)

            assert h.substrate.lookup(first_thread_key) is None
            assert h.substrate.lookup(second_thread_key) is not None
            assert await h.async_redis.scard(thread_reset_keys.requests) == 0
            assert not await h.async_redis.sismember(thread_reset_keys.inflight, first_thread_key)

    asyncio.run(go())


def test_next_turn_drains_reset_before_claiming_when_quota_is_full(
    make_harness, thread_reset_keys
) -> None:
    """#1534: drain must run BEFORE the follow-up claim. If it ran after
    process_event, tEval2 would see ResourceQuota still full (tEval1 still
    holding the slot), raise CapacityExhaustedError, and never bind.
    """

    async def go() -> None:
        async with make_harness(claim_timeout_seconds=0.2) as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("eval-case-1", thread="tEval1"))
            first_thread_key = _thread_key("tEval1")
            second_thread_key = _thread_key("tEval2")
            assert h.substrate.lookup(first_thread_key) is not None

            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"limits.cpu": "1"},
                used={"limits.cpu": "8"},
                hard={"limits.cpu": "8"},
            )
            original_delete = h.fake_k8s.delete_claim

            def delete_and_free(name: str, *, request_timeout_seconds: float) -> None:
                original_delete(
                    name,
                    request_timeout_seconds=request_timeout_seconds,
                )
                h.fake_k8s.quota_rejection = None

            h.fake_k8s.delete_claim = delete_and_free  # type: ignore[method-assign]

            await h.async_redis.sadd(thread_reset_keys.requests, first_thread_key)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            nxt = _qevent("eval-case-2", thread="tEval2", event_id="eval-2-quota")
            entry_id, fields = await _pending_local_entry(h, nxt)
            await consumer._sem.acquire()
            await consumer._handle(entry_id, fields)

            assert h.substrate.lookup(first_thread_key) is None
            assert h.substrate.lookup(second_thread_key) is not None

    asyncio.run(go())


def test_sadd_of_bare_eval_conversation_id_does_not_release_scoped_sandbox(
    make_harness, thread_reset_keys
) -> None:
    """#2259 negative: after ADR-0096 the worker keys sandboxes as
    quote(kind):quote(channel):quote(conversation_id). SADDing the bare
    ``eval:`` conversation_id (what cluster eval used to queue) is a no-op
    drain: lookup misses, the sandbox keeps its ResourceQuota slot.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            event = _qevent("eval-case-1", thread="eval:1720000000.000100")
            await h.kernel.process_event(event)
            scoped = kernel_module._thread_key_for(event)
            assert h.substrate.lookup(scoped) is not None
            assert scoped == "slack:C1:eval%3A1720000000.000100"

            await h.async_redis.sadd(thread_reset_keys.requests, event.conversation_id)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            nxt = _qevent("follow-up", thread="tNext", event_id="eval-wrong-key")
            entry_id, fields = await _pending_local_entry(h, nxt)
            await consumer._sem.acquire()
            await consumer._handle(entry_id, fields)

            assert h.substrate.lookup(scoped) is not None, (
                "a THREAD_RESET_SET member that is not the scoped thread key "
                "must not release the eval sandbox"
            )

    asyncio.run(go())


def test_sadd_of_scoped_eval_isolate_key_releases_the_sandbox(
    make_harness, thread_reset_keys
) -> None:
    """#2259: the CLI must SADD the same percent-encoded triple the worker
    claimed, including the ``eval:`` conversation_id whose colon encodes.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            event = _qevent("eval-case-1", thread="eval:1720000000.000100")
            await h.kernel.process_event(event)
            scoped = kernel_module._thread_key_for(event)
            assert h.substrate.lookup(scoped) is not None

            await h.async_redis.sadd(thread_reset_keys.requests, scoped)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            nxt = _qevent("eval-case-2", thread="eval:1720000000.000200", event_id="eval-scoped")
            entry_id, fields = await _pending_local_entry(h, nxt)
            await consumer._sem.acquire()
            await consumer._handle(entry_id, fields)

            assert h.substrate.lookup(scoped) is None

    asyncio.run(go())


def test_maintenance_tick_drains_pending_thread_reset_requests(
    make_harness, thread_reset_keys
) -> None:
    """#713: an operator-requested thread reset (the API SADDs the thread_key
    into THREAD_RESET_SET) is picked up and applied by the maintenance tick,
    releasing that thread's sandbox and popping it off the pending set."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tDrain"))
            thread_key = _thread_key("tDrain")
            assert h.substrate.lookup(thread_key) is not None

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await h.async_redis.sadd(thread_reset_keys.requests, thread_key)

            await consumer._drain_thread_reset_requests()

            assert h.substrate.lookup(thread_key) is None  # released
            # Popped, not left behind.
            assert await h.async_redis.scard(thread_reset_keys.requests) == 0
            # #812: the in-progress marker is cleared only after the release
            # actually lands, so a successful drain leaves nothing pending.
            assert not await h.async_redis.sismember(thread_reset_keys.inflight, thread_key)

    asyncio.run(go())


def test_maintenance_tick_thread_reset_failed_release_keeps_the_signal_pending(
    make_harness, caplog, thread_reset_keys
) -> None:
    """#812 (was #806 incomplete): the observable "reset outstanding" signal --
    membership of THREAD_RESET_SET UNION THREAD_RESET_INFLIGHT_SET, which the
    API's ``is_pending`` and therefore the CLI's ``reset-thread`` poll read --
    must NOT flip to done when ``release_thread`` raises or times out. The drain
    SPOPs the request (the atomic claim) and moves it into the in-progress set,
    clearing it only on SUCCESS; a failed release leaves the key in the
    in-progress set, so the signal stays pending and the CLI reports the reset as
    unconfirmed rather than a false ``released: true`` (scenario B)."""

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await h.async_redis.sadd(thread_reset_keys.requests, "tFailRelease")

            async def boom_release(thread_key: str) -> bool:
                raise RuntimeError("injected release failure")

            h.kernel.release_thread = boom_release  # type: ignore[method-assign]

            with caplog.at_level(logging.ERROR):
                await consumer._drain_thread_reset_requests()

            # Claimed off the request set (atomic SPOP: no second replica double-releases)...
            assert await h.async_redis.scard(thread_reset_keys.requests) == 0
            # ...but the in-progress marker is STILL set: the pending signal the
            # CLI gates on stays True, so it never reports a false success.
            assert await h.async_redis.sismember(thread_reset_keys.inflight, "tFailRelease")
            assert any("tFailRelease" in r.getMessage() for r in caplog.records)

    asyncio.run(go())


def _capture_thread_reset_outcomes(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, str]]:
    """Record every ``curie.sandbox.lifecycle`` attribute set the drain emits for
    ``operation=thread-reset``."""
    seen: list[dict[str, str]] = []
    real_record_metric = consumer_module.record_metric

    def record(name: str, value: float = 1, *, attributes: dict[str, str] | None = None) -> None:
        real_record_metric(name, value, attributes=attributes)
        if (
            name == "curie.sandbox.lifecycle"
            and attributes is not None
            and attributes.get("operation") == "thread-reset"
        ):
            seen.append(dict(attributes))

    monkeypatch.setattr(consumer_module, "record_metric", record)
    return seen


def _text(raw: object) -> str | None:
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


def test_thread_reset_drain_records_no_route_when_the_key_matched_no_route(
    make_harness, caplog, monkeypatch, thread_reset_keys
) -> None:
    """#3699: ``release_thread`` returns False when the key matched no route
    (a hand-built key that left out a named bot's identity segment). Nothing was
    released, so the drain records ``no-route`` where the API can read it, warns
    instead of logging a success line, and counts the outcome."""
    outcomes = _capture_thread_reset_outcomes(monkeypatch)

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            thread_key = "slack:C0EXAMPLE1:missing"
            assert h.substrate.lookup(thread_key) is None
            await h.async_redis.sadd(thread_reset_keys.requests, thread_key)

            with caplog.at_level(logging.INFO):
                await consumer._drain_thread_reset_requests()

            result_key = f"{thread_reset_keys.result_prefix}{thread_key}"
            assert _text(await h.async_redis.get(result_key)) == "no-route"
            ttl = await h.async_redis.ttl(result_key)
            assert 0 < ttl <= 3600, ttl
            # The in-flight marker is cleared only after the result is written,
            # so a poll that reads "not pending" always finds the result.
            assert not await h.async_redis.sismember(thread_reset_keys.inflight, thread_key)

            warnings = [
                r
                for r in caplog.records
                if r.levelno == logging.WARNING and thread_key in r.getMessage()
            ]
            assert warnings, "a reset that matched no route must warn"
            assert "nothing was released" in warnings[0].getMessage()
            assert not any(
                "released sandbox" in r.getMessage() and thread_key in r.getMessage()
                for r in caplog.records
            ), "a reset that released nothing must not log a release"

    asyncio.run(go())
    assert outcomes == [
        {"service.name": "curie-worker", "operation": "thread-reset", "outcome": "no-route"}
    ]


def test_thread_reset_drain_records_released_when_a_route_existed(
    make_harness, monkeypatch, thread_reset_keys
) -> None:
    """#3699: a reset that matched a route behaves as before and records
    ``released`` with the same one-hour lifetime."""
    outcomes = _capture_thread_reset_outcomes(monkeypatch)

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tResultReleased"))
            thread_key = _thread_key("tResultReleased")
            assert h.substrate.lookup(thread_key) is not None

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await h.async_redis.sadd(thread_reset_keys.requests, thread_key)

            await consumer._drain_thread_reset_requests()

            result_key = f"{thread_reset_keys.result_prefix}{thread_key}"
            assert _text(await h.async_redis.get(result_key)) == "released"
            ttl = await h.async_redis.ttl(result_key)
            assert 0 < ttl <= 3600, ttl
            assert not await h.async_redis.sismember(thread_reset_keys.inflight, thread_key)

    asyncio.run(go())
    assert outcomes == [
        {"service.name": "curie-worker", "operation": "thread-reset", "outcome": "released"}
    ]


def test_thread_reset_drain_records_failed_and_no_result_when_the_release_raises(
    make_harness, monkeypatch, thread_reset_keys
) -> None:
    """#3699: a release that raises writes no result (the request stays in
    flight, as before) and counts as ``failed``."""
    outcomes = _capture_thread_reset_outcomes(monkeypatch)

    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            thread_key = "tResultFailed"
            result_key = f"{thread_reset_keys.result_prefix}{thread_key}"
            await h.async_redis.sadd(thread_reset_keys.requests, thread_key)
            await h.async_redis.delete(result_key)

            async def boom_release(key: str) -> bool:
                raise RuntimeError("injected release failure")

            h.kernel.release_thread = boom_release  # type: ignore[method-assign]

            await consumer._drain_thread_reset_requests()

            assert not await h.async_redis.exists(result_key)
            assert await h.async_redis.sismember(thread_reset_keys.inflight, thread_key)

    asyncio.run(go())
    assert outcomes == [
        {"service.name": "curie-worker", "operation": "thread-reset", "outcome": "failed"}
    ]


def test_maintenance_tick_thread_reset_is_not_stalled_by_a_wedged_runner(
    make_harness, monkeypatch, thread_reset_keys
) -> None:
    """#739: the maintenance tick runs stream reclaim, orphan reaping, and the
    thread-reset drain in one pass, so a reset whose runner never answers the
    courtesy interrupt would otherwise block all three for the runner client's
    full 600s request timeout -- and the request is already SPOPped off the set,
    so it is lost rather than retried on the next tick. The drain must therefore
    finish in seconds and the sandbox must actually be gone afterwards."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tWedgedDrain"))
            thread_key = _thread_key("tWedgedDrain")
            assert h.substrate.lookup(thread_key) is not None

            monkeypatch.setattr(kernel_module, "_RESET_INTERRUPT_TIMEOUT_S", 0.2)

            wedged = asyncio.Event()  # never set

            async def never_answers(base_url: str, reason: str, token: str | None = None) -> None:
                await wedged.wait()

            monkeypatch.setattr(h.kernel._runner, "interrupt", never_answers)

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await h.async_redis.sadd(thread_reset_keys.requests, thread_key)

            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=2.0)

            assert h.substrate.lookup(thread_key) is None  # the reset was not lost

    asyncio.run(go())


def test_maintenance_tick_thread_reset_is_a_noop_when_nothing_pending(
    make_harness, thread_reset_keys
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer._drain_thread_reset_requests()  # must not raise

    asyncio.run(go())


def test_maintenance_tick_thread_reset_one_failure_does_not_block_the_rest(
    make_harness, caplog, thread_reset_keys
) -> None:
    """A release failure for one requested thread (e.g. a transient substrate
    error) is logged and does not prevent the rest of the batch from being
    drained -- an operator resetting several stuck threads at once should not
    have one bad apple silently strand the others unprocessed."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tOk"))

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await h.async_redis.sadd(thread_reset_keys.requests, "tBoom", "tOk")

            original_release_thread = h.kernel.release_thread

            async def flaky_release_thread(thread_key: str) -> bool:
                if thread_key == "tBoom":
                    raise RuntimeError("injected substrate failure")
                return await original_release_thread(thread_key)

            h.kernel.release_thread = flaky_release_thread  # type: ignore[method-assign]

            with caplog.at_level(logging.ERROR):
                await consumer._drain_thread_reset_requests()

            assert h.substrate.lookup("tOk") is None  # still processed despite tBoom's failure
            # Both popped either way.
            assert await h.async_redis.scard(thread_reset_keys.requests) == 0
            assert any("tBoom" in r.getMessage() for r in caplog.records)

    asyncio.run(go())


def test_maintenance_tick_reset_drain_has_a_per_tick_budget_and_defers_the_rest(
    make_harness, monkeypatch, thread_reset_keys
) -> None:
    """#743: a large operator-populated batch of wedged resets must not cost
    N x the per-request release bound inline in one maintenance tick -- that
    re-crosses the same multi-hundred-second stall #739 set out to eliminate,
    just scaled by batch size instead of by the runner's HTTP timeout. The
    drain now stops once its per-tick time budget is spent and leaves
    whatever is left in THREAD_RESET_SET for a later tick, so one call to
    ``_drain_thread_reset_requests`` never blocks proportionally to N.

    #3807: the test owns its reset keys (per-test request, in-flight and
    result keys under the ``names`` prefix), so a concurrent consumer on the
    shared Valkey cannot drain or steal them. It also drives the budget with a
    controlled clock (each release advances it by exactly one second) instead
    of wall time, so the number of requests per pass is deterministic."""

    async def go() -> None:
        async with make_harness() as h:
            # Controlled clock. Only consumer_module's ``time`` is replaced (the
            # real ``time.monotonic`` is left alone because asyncio's loop uses
            # it); every other attribute forwards to the real module.
            fake_now = [0.0]

            class _FakeTime:
                @staticmethod
                def monotonic() -> float:
                    return fake_now[0]

                def __getattr__(self, name: str) -> object:
                    return getattr(time, name)

            monkeypatch.setattr(consumer_module, "time", _FakeTime())
            monkeypatch.setattr(consumer_module, "_THREAD_RESET_DRAIN_BUDGET_S", 4.0)

            processed: list[str] = []

            async def slow_release_thread(thread_key: str) -> bool:
                processed.append(thread_key)
                fake_now[0] += 1.0  # each request "wedged" for one fake second
                return True

            h.kernel.release_thread = slow_release_thread  # type: ignore[method-assign]

            # A batch of 20 against a 4 second budget at 1 second per request.
            keys = [f"tBatch{i}" for i in range(20)]
            await h.async_redis.sadd(thread_reset_keys.requests, *keys)

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=5.0)

            # The budget check runs after each release, so exactly four requests
            # fit; the other sixteen are left for the next tick, not lost.
            assert len(processed) == 4
            assert fake_now[0] == 4.0
            assert await h.async_redis.scard(thread_reset_keys.requests) == 16
            # Each landed release SREMs it.
            assert await h.async_redis.scard(thread_reset_keys.inflight) == 0

            # A later tick picks up where this one left off: draining again
            # (with the budget restored to a generous value) finishes the batch.
            monkeypatch.setattr(consumer_module, "_THREAD_RESET_DRAIN_BUDGET_S", 30.0)
            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=5.0)
            assert await h.async_redis.scard(thread_reset_keys.requests) == 0
            assert await h.async_redis.scard(thread_reset_keys.inflight) == 0
            assert len(processed) == len(set(processed)) == 20
            assert sorted(processed) == sorted(keys)

    asyncio.run(go())


def test_maintenance_tick_thread_reset_is_not_stalled_by_a_hanging_substrate_release(
    make_harness, monkeypatch, thread_reset_keys
) -> None:
    """#743: the courtesy interrupt bound (#739) only covers a wedged runner.
    `release_thread`'s own substrate release runs on a bare `asyncio.to_thread`
    with no timeout, so a hang in the K8s control plane -- a claim delete that
    never returns -- would stall the tick just as unboundedly. The release
    call must be bounded the same way the interrupt already is."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tHangRelease"))
            thread_key = _thread_key("tHangRelease")
            assert h.substrate.lookup(thread_key) is not None

            monkeypatch.setattr(kernel_module, "_RESET_RELEASE_TIMEOUT_S", 0.2)

            def hanging_release(thread_key: str) -> bool:
                time.sleep(5.0)  # never returns within the test's window
                return True

            monkeypatch.setattr(h.substrate, "release", hanging_release)

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await h.async_redis.sadd(thread_reset_keys.requests, thread_key)

            # Must finish well under the 5s hang, bounded instead by the
            # (monkeypatched) release timeout.
            await asyncio.wait_for(consumer._drain_thread_reset_requests(), timeout=2.0)

            # The request was popped either way; a fresh reset is needed to retry.
            assert await h.async_redis.scard(thread_reset_keys.requests) == 0

    asyncio.run(go())


# --- An acked entry must never be a silent one (#2004) -----------------------
# The kernel-level regressions call ``process_event`` directly, so they can only
# see the silence. The ack is a CONSUMER fact -- a normal return from
# ``process_event`` is what makes ``Consumer`` XACK -- so the pairing the ticket
# actually reports is only observable here, against the real stream and group.


def _workspace_binding(deployment_id: uuid.UUID) -> object:
    """A workspace-enabled binding carrying a FIXED deployment id.

    Fixed on purpose: the id is what an operator greps the worker log for, so
    the assertion below checks the real value rather than that some uuid landed.
    Defined locally rather than imported from ``test_kernel``; importlib mode
    makes cross-test-module imports fragile.
    """

    class WorkspaceResolved:
        def __init__(self) -> None:
            self.agent_id = uuid.uuid4()
            self.agent_name = "test-agent"
            self.endpoint: str | None = None
            self.adapter: str | None = None
            self.deployment_id = deployment_id
            self.workspace_enabled = True

    class WorkspaceBinding:
        async def resolve(
            self, _kind: str, _adapter: str | None, _channel: str
        ) -> WorkspaceResolved:
            return WorkspaceResolved()

        def boot_env(
            self,
            _resolved: object,
            _thread_key: str,
            *,
            kind: str | None = None,
            address: str | None = None,
            **_: object,
        ) -> dict[str, str]:
            return {}

        def packs_for(self, _resolved: object) -> BehaviorPacks:
            return BehaviorPacks()

    return WorkspaceBinding()


def test_failed_workspace_preparation_acks_the_entry_and_is_not_silent(
    make_harness, caplog
) -> None:
    """#2004 end to end: the reported evidence was a group at ``pending 0`` with
    ``last-delivered-id`` equal to the turn's stream id -- yet no sandbox and an
    empty worker log.

    The refusal half of that symptom is already covered on ``next`` by the INFO
    line the refusal branch emits; this pins the PREPARATION half, which is the
    one still silent here. Selection succeeds and the clone fails, so the turn
    retries to its escalation and the entry is acked -- and only the ack makes
    the silence undiagnosable. An entry left pending is at least visible in
    XPENDING; an acked entry with nothing in the log is indistinguishable from a
    bot that was never mentioned. Driven through the real ``Consumer`` and the
    real stream so the ack is the production one, not an assertion about
    ``process_event``'s return value.
    """

    caplog.set_level(logging.WARNING, logger="curie_worker.kernel")
    deployment_id = uuid.uuid4()

    async def go() -> None:
        async with make_harness(binding=_workspace_binding(deployment_id)) as h:
            # Selection SUCCEEDS -- this is a fault, not a policy refusal -- and
            # the workspace then fails to materialize, which is the path that
            # still ends the turn without naming anything.
            class WorkspaceProbe:
                def select_repository(
                    self,
                    *,
                    thread_key: str,
                    deployment_id: uuid.UUID,
                    author: str,
                    repo_full_name: str | None,
                ) -> str:
                    assert repo_full_name is not None
                    return repo_full_name

                def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                    raise WorkspacePreparationError(
                        "clone", "git clone exited 128: repository not found"
                    )

                def touch(self, thread_key: str, *, ttl_seconds: int) -> bool:
                    return True

                def enumerate_expired(self) -> list[object]:
                    # The consumer's maintenance loop polls this every tick;
                    # without it the loop logs a repeated AttributeError
                    # traceback that pollutes this test's caplog capture.
                    return []

            h.kernel._workspace = WorkspaceProbe()  # type: ignore[assignment]

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()

            qe = _qevent(
                "Fix https://github.com/acme-corp/acme-bot",
                thread="tAckSilent",
                event_id="ws-ack-1",
            )
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))

            task = asyncio.create_task(consumer.run())
            # Wait for the escalation itself, not its classification string --
            # the turn exhausts its retries and escalates on both the pre-fix
            # and post-fix source, and only the classification name
            # ("runner-error" vs "workspace-error") differs between them.
            # Gating on the new name would time out pre-fix and never reach
            # the assertions below, which is the thing this test exists to
            # pin.
            await _wait_until(
                lambda: h.sink.last_text is not None and "Flagging for a human" in h.sink.last_text
            )
            consumer.request_stop()
            await task

            # Half one of the ticket's evidence: the entry really was consumed
            # and acked off the group -- nothing is left for an operator to find.
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0
            assert h.fake_k8s.claim_envs == []  # ...and no sandbox was ever created
            assert h.runner.opened == []  # ...and the runner never saw a turn

            # Half two, asserted second so the pairing is explicit: an acked,
            # sandbox-less turn is only a bug because the worker said nothing
            # about it. This is the assertion that fails on the current base.
            failures = [
                r
                for r in caplog.records
                if r.name == "curie_worker.kernel" and "workspace start failed" in r.getMessage()
            ]
            assert failures, (
                "the entry was acked with no sandbox and no line naming the agent -- "
                f"exactly what #2004 reports: {caplog.text!r}"
            )
            assert all(r.levelno == logging.WARNING for r in failures)
            message = failures[-1].getMessage()
            assert "agent=test-agent" in message, message
            assert f"deployment={deployment_id}" in message, message
            assert "stage=clone" in message, message

            # Last: the escalation is correctly classified post-fix. Placed
            # after the silence assertion above so a pre-fix failure reports
            # the meaningful thing (no warning line) rather than this one.
            assert h.sink.last_text is not None
            assert "workspace-error" in h.sink.last_text

    asyncio.run(go())


def test_maintenance_tick_thread_reset_claim_and_mark_is_atomic(
    make_harness, thread_reset_keys
) -> None:
    """#855: claiming a reset request and marking it in-progress must be ONE
    server-side step. As an SPOP followed by a separate SADD they are two round
    trips, and between them the thread_key is in NEITHER set -- so the API's
    ``is_pending`` (the UNION of the two) reads False for a request whose
    sandbox has not been touched yet, and the CLI's ``reset-thread`` poll takes
    that single ``requested: false`` as proof and prints "reset complete,
    sandbox released" before ``release_thread`` has even been invoked. That is
    #812's user-visible failure narrowed to one network hop.

    Both halves matter. (a) The gap must never be observable, and the observer
    that proves it sits at the CENTRAL COMMAND BOUNDARY -- ``execute_command``,
    the single funnel every redis-py call (``spop``, ``sadd``, ``eval``,
    ``srem``, a pipeline's writes) is issued through -- rather than on any one
    named method. After every command the drain issues, it reads the union on a
    SECOND, INDEPENDENT connection (so the observation never recurses through
    the client under test, and never perturbs what it measures) and records a
    violation if the live request is in neither set. That is what makes this
    mutation-resistant rather than shape-recognising: the old ``.spop()`` +
    ``.sadd()`` pair, two separate ``EVAL``s (one SPOP-ing, one SADD-ing), and a
    raw ``execute_command("SPOP", ...)`` + ``execute_command("SADD", ...)`` pair
    ALL go red, because all three are two commands with an observable gap
    between them -- the observer never has to know which method name was used.
    (b) The mark must still land, and land from the atomic claim itself:
    mid-release the key IS in the in-progress set, yet no separate marking
    command (an ``SADD``/``SMOVE`` against ``THREAD_RESET_INFLIGHT_SET``) was
    ever issued through that same boundary. (a) alone would pass vacuously if
    the drain simply stopped claiming anything; (b) is what proves it did not.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tAtomicClaim"))
            thread_key = _thread_key("tAtomicClaim")
            assert h.substrate.lookup(thread_key) is not None

            violations: list[str] = []
            observed: list[str] = []
            observer_errors: list[str] = []
            inflight_marks: list[tuple[Any, ...]] = []
            original_execute = h.async_redis.execute_command
            original_sadd = h.async_redis.sadd
            original_srem = h.async_redis.srem

            # The observer reads on its own connection: issuing its reads through
            # the spied client would recurse into the spy and would interleave
            # extra commands into the very sequence under observation.
            observer_redis: AsyncRedis = AsyncRedis(
                host=VALKEY_HOST,
                port=VALKEY_PORT,
                password=VALKEY_PW or None,
                decode_responses=True,
            )

            started = asyncio.Event()
            proceed = asyncio.Event()
            original_release = h.kernel.release_thread

            async def blocking_release(thread_key: str) -> bool:
                started.set()
                await proceed.wait()
                return await original_release(thread_key)

            async def spy_execute_command(*args: Any, **options: Any) -> Any:
                name = args[0]
                name = (name.decode() if isinstance(name, bytes) else name).upper()
                # A separate marking round trip is itself the defect: record any
                # command that writes the in-progress set from the client side.
                if (
                    not started.is_set()
                    and name in {"SADD", "SMOVE"}
                    and thread_reset_keys.inflight in args[1:]
                ):
                    inflight_marks.append(args)
                result = await original_execute(*args, **options)
                # The observation window runs from "a live request exists in the
                # request set" up to the instant the release begins. Before the
                # release starts the key MUST be somewhere in the union; once the
                # release is running the mid-release assertion below covers it,
                # and after a successful release both sets are legitimately empty
                # -- observing past that point would manufacture false
                # violations.
                if not started.is_set():
                    observed.append(name)
                    try:
                        in_requests = await observer_redis.sismember(
                            thread_reset_keys.requests, thread_key
                        )
                        in_flight = await observer_redis.sismember(
                            thread_reset_keys.inflight, thread_key
                        )
                        if not in_requests and not in_flight:
                            violations.append(name)
                    except Exception as exc:  # never raise inside the spy
                        observer_errors.append(f"{name}: {exc!r}")
                return result

            h.kernel.release_thread = blocking_release  # type: ignore[method-assign]
            task: asyncio.Task[None] | None = None
            try:
                # The request and in-progress sets are this test's own keys
                # (``thread_reset_keys``, #3807), so no other test or run can
                # leave residue in them. Residue would matter asymmetrically: a
                # leftover "tAtomicClaim" in the in-progress set makes the
                # observer read `in_flight` True at every boundary, so arm (a)
                # can never record a violation and goes SILENTLY VACUOUS rather
                # than red. The pre-clean and its assertions below keep that
                # precondition explicit, and the post-clean settles any residue
                # an assertion firing mid-release leaves behind.
                # These writes happen before the spy is installed (and, in the
                # `finally`, after it is restored), so they can never be counted
                # as violations or as inflight_marks.
                await original_srem(thread_reset_keys.requests, thread_key)
                await original_srem(thread_reset_keys.inflight, thread_key)
                assert not await observer_redis.sismember(thread_reset_keys.requests, thread_key)
                assert not await observer_redis.sismember(thread_reset_keys.inflight, thread_key), (
                    "stale in-progress residue would make arm (a) vacuous"
                )

                await original_sadd(thread_reset_keys.requests, thread_key)
                # The live request now exists; install the spy before the drain
                # can issue its first command. The spy is only ever installed
                # between here and the `finally` below, so its own presence is
                # the observation window -- `not started.is_set()` alone closes
                # it at the release.
                h.async_redis.execute_command = (  # type: ignore[method-assign,assignment]
                    spy_execute_command
                )
                consumer = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=h.config,
                    leases=DeliveryLeaseStore(h.async_redis, h.config),
                )
                task = asyncio.create_task(consumer._drain_thread_reset_requests())
                await asyncio.wait_for(started.wait(), timeout=5.0)

                # (b) The mark landed -- the pending signal is still True while
                # the release runs...
                assert await observer_redis.sismember(thread_reset_keys.inflight, thread_key)
                # ...and it came from the atomic claim, not a second round trip
                # through the command boundary.
                assert inflight_marks == []

                proceed.set()
                await asyncio.wait_for(task, timeout=10.0)
            finally:
                h.async_redis.execute_command = (  # type: ignore[method-assign,assignment]
                    original_execute
                )
                proceed.set()
                # Unblock and settle the drain first: while it is still parked in
                # the release it has not reached its own SREM, and a cleanup that
                # raced it could be undone by a late claim write.
                if task is not None and not task.done():
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(asyncio.shield(task), timeout=10.0)
                    if not task.done():
                        task.cancel()
                        with contextlib.suppress(BaseException):
                            await task
                # Exception-safe so a cleanup failure cannot mask the assertion
                # that brought us here.
                for _set in (thread_reset_keys.requests, thread_reset_keys.inflight):
                    with contextlib.suppress(Exception):
                        await original_srem(_set, thread_key)
                with contextlib.suppress(Exception):
                    await observer_redis.aclose()

            assert observer_errors == [], f"observer failed to read: {observer_errors}"
            # The observer must actually have run. An empty `violations` list is
            # only evidence if commands reached the boundary at all -- if a future
            # change stops routing the claim through ``execute_command``, arm (a)
            # fails loudly here instead of passing on zero observations.
            assert observed, (
                "no command was observed at the command boundary while the "
                "window was open; arm (a) proved nothing"
            )
            # (a) Every command the drain issued left the live request visible in
            # the union. A non-empty list names the command after which
            # `is_pending` would have read False for a request whose sandbox had
            # not been released yet.
            assert violations == [], (
                "thread_key was in NEITHER THREAD_RESET_SET nor "
                f"THREAD_RESET_INFLIGHT_SET after: {violations}"
            )

            # End state is clean: released, and nothing left pending.
            assert h.substrate.lookup(thread_key) is None
            assert not await h.async_redis.sismember(thread_reset_keys.requests, thread_key)
            assert not await h.async_redis.sismember(thread_reset_keys.inflight, thread_key)

    asyncio.run(go())


class _LockOwnerLiveness:
    """Runs-stream adapter so ThreadLock steal uses the real consumer leases."""

    def __init__(self, store: ConsumerLivenessStore, *, stream: str, group: str) -> None:
        self._store = store
        self._stream = stream
        self._group = group

    async def is_alive(self, owner: str) -> bool:
        return await self._store.is_alive(stream=self._stream, group=self._group, consumer=owner)

    async def is_capable(self, owner: str) -> bool:
        return await self._store.is_capable(stream=self._stream, group=self._group, consumer=owner)


def _wire_stealable_lock(h: Any, *, owner: str, proof_s: float) -> ThreadLock:
    """Replace the harness lock with the production steal wiring (#2500)."""

    lock = ThreadLock(
        h.async_redis,
        ttl_ms=h.config.lock_ttl_ms,
        acquire_timeout_s=h.config.lock_acquire_timeout_s,
        poll_interval_s=h.config.lock_poll_interval_s,
        owner=owner,
        owner_liveness=_LockOwnerLiveness(
            ConsumerLivenessStore(h.async_redis),
            stream=h.config.stream,
            group=h.config.consumer_group,
        ),
        dead_owner_proof_s=proof_s,
    )
    h.kernel._lock = lock
    return lock


def test_force_killed_worker_lock_is_stolen_and_cluster_message_reply_is_delivered(
    make_harness,
) -> None:
    """#2500: a SIGKILLed worker's per-thread lock must not delay PEL reclaim.

    Cluster message enqueues a QueuedTurn (XADD) and waits for the worker
    reply. The 2026-09-09 cluster reproduction force-killed replicas:1 mid
    claim: PEL moved promptly, then the replacement logged LockAcquireTimeout
    for ~90s and the CLI timed out before the recovered fakeModel reply.

    This pin drives that path against real Valkey: the dead consumer owns the
    PEL row and the thread lock (acquire without release, matching SIGKILL),
    its heartbeat is gone, and the replacement must deliver the reply in much
    less than the 60s lock TTL, then ACK so nothing stays pending under the
    dead consumer.

    Red on revert of steal: the replacement waits out acquire_timeout and the
    sink never sees ``recovered`` inside the 2s bound.
    """

    async def go() -> None:
        async with make_harness(
            lock_ttl_ms=60_000,
            lock_acquire_timeout_s=1.0,
            max_attempts=1,
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            h.runner.default_script = [Final(text="recovered", status=DONE)]
            thread = "t-2500-crash"
            dead_name = "dead-worker-2500"
            _wire_stealable_lock(
                h,
                owner=h.config.consumer_name,
                proof_s=h.config.consumer_heartbeat_ttl_ms / 1000,
            )
            dead_lock = ThreadLock(
                h.async_redis,
                ttl_ms=h.config.lock_ttl_ms,
                acquire_timeout_s=h.config.lock_acquire_timeout_s,
                poll_interval_s=h.config.lock_poll_interval_s,
                owner=dead_name,
            )
            await dead_lock.acquire(h.config.lock_key(_thread_key(thread)))

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            qe = _qevent("crash-reclaim", thread=thread, event_id="e-2500-crash")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            claimed = await h.async_redis.xreadgroup(
                h.config.consumer_group, dead_name, {h.config.stream: ">"}, count=1
            )
            assert claimed

            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer=dead_name,
                heartbeat_ttl_ms=1,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            alive_key = consumer_heartbeat_key(h.config.stream, h.config.consumer_group, dead_name)
            await _wait_key(h.async_redis, alive_key, present=False)
            await _wait_consumer_idle(
                h.async_redis,
                h.config.stream,
                h.config.consumer_group,
                dead_name,
                h.config.dead_consumer_idle_ms,
            )
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 1

            assert await consumer._prompt_reclaim_once() == 0
            await asyncio.sleep(h.config.consumer_heartbeat_ttl_ms / 1000 + 0.015)
            started = time.monotonic()
            reclaimed = await consumer._prompt_reclaim_once()
            assert reclaimed == 1
            await _wait_until(lambda: h.sink.last_text == "recovered", timeout=2.0)
            elapsed = time.monotonic() - started
            await asyncio.gather(*list(consumer._inflight))

            assert elapsed < 2.0, (
                f"recovered reply took {elapsed:.3f}s; replacement still waited "
                "out the dead worker lock TTL"
            )
            assert h.runner.opened == ["crash-reclaim"]
            assert h.sink.last_text == "recovered"
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0
            owners = await h.async_redis.xpending_range(
                h.config.stream, h.config.consumer_group, min="-", max="+", count=10
            )
            assert all(str(row["consumer"]) != dead_name for row in owners)
            assert await h.async_redis.exists(h.config.done_key(qe.event_id))

    asyncio.run(go())


def test_live_worker_lock_still_serializes_a_replacement(make_harness) -> None:
    """#2500 negative: a live owner's heartbeat blocks steal; the waiter fails.

    Same QueuedTurn / PEL / lock path as the crash pin, except the lock holder
    keeps its alive lease. The replacement must not run the turn.
    """

    async def go() -> None:
        async with make_harness(
            lock_ttl_ms=60_000,
            lock_acquire_timeout_s=0.4,
            max_attempts=1,
            reclaim_min_idle_ms=5000,
            dead_consumer_idle_ms=0,
            consumer_heartbeat_ttl_ms=30,
            consumer_capability_ttl_ms=6000,
        ) as h:
            h.runner.default_script = [Final(text="should-not-run", status=DONE)]
            thread = "t-2500-live"
            live_name = "live-worker-2500"
            _wire_stealable_lock(
                h,
                owner=h.config.consumer_name,
                proof_s=h.config.consumer_heartbeat_ttl_ms / 1000,
            )
            live_lock = ThreadLock(
                h.async_redis,
                ttl_ms=h.config.lock_ttl_ms,
                acquire_timeout_s=h.config.lock_acquire_timeout_s,
                poll_interval_s=h.config.lock_poll_interval_s,
                owner=live_name,
            )
            live_token = await live_lock.acquire(h.config.lock_key(_thread_key(thread)))

            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            qe = _qevent("live-hold", thread=thread, event_id="e-2500-live")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            claimed = await h.async_redis.xreadgroup(
                h.config.consumer_group, live_name, {h.config.stream: ">"}, count=1
            )
            assert claimed
            store = ConsumerLivenessStore(h.async_redis)
            await store.publish(
                stream=h.config.stream,
                group=h.config.consumer_group,
                consumer=live_name,
                heartbeat_ttl_ms=5_000,
                capability_ttl_ms=h.config.consumer_capability_ttl_ms,
            )
            await h.async_redis.xclaim(
                h.config.stream,
                h.config.consumer_group,
                h.config.consumer_name,
                0,
                [claimed[0][1][0][0]],
            )
            await consumer._dispatch(str(claimed[0][1][0][0]), dict(claimed[0][1][0][1]))
            await asyncio.gather(*list(consumer._inflight))

            assert h.runner.opened == []
            assert h.sink.last_text != "should-not-run"
            assert await h.async_redis.get(h.config.lock_key(_thread_key(thread))) == live_token
            await live_lock.release(h.config.lock_key(_thread_key(thread)), live_token)

    asyncio.run(go())
