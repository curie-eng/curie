"""The real Consumer, Kernel and Lua scripts under the protected worker role,
@spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6 PROTECTED-HOOK-LANE-7.

Nothing here is a lane adapter (those are S2-S4): it proves the *unchanged* worker core
runs over the pinned ``AuthenticatedWorkerClient`` handles, as the ``worker`` principal
installed from ``worker_acl_rules("worker")``, on the owned disposable TLS Valkey, with
every key the worker owns under ``protected:lane:`` (plus the admission-written
``curie:runs`` and its three stream-derived liveness families).

The kernel harness of ``apps/worker/tests/kernel/conftest.py`` is reused by path (real
Kernel, real sandbox substrate with a fake Kubernetes client, real ``RunnerClient``
against an in-process fake runner); only its two Valkey constructors are redirected to
the pinned handles. Stream entries are written by an owned *enqueue* principal, the way
admission writes them.

The one expected denial is the consumer's wrapped pre-turn drain of the
``curie:thread-reset-*`` keys (``consumer.py`` logs and continues); anything else the
ACL log records is a missing grant and fails the case.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.util
import logging
import sys
from pathlib import Path

import pytest
from curie_protected_hooks.admission_acl import admission_acl_rules

from . import worker_broker as wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker

WORKER_TESTS = Path(__file__).resolve().parents[3] / "apps" / "worker" / "tests"
# importlib import mode does not add the worker test root to sys.path.
if str(WORKER_TESTS) not in sys.path:
    sys.path.insert(0, str(WORKER_TESTS))

ALLOWED_DENIALS = {b"curie:thread-reset-requests", b"curie:thread-reset-inflight"}
ALLOWED_PREFIXES = (
    "protected:lane:",
    "curie:runs",
    "protected:control:",
    "protected:admission:",
    "protected:source:",
)


def kernel_conftest():
    """The unchanged worker kernel harness, loaded by path, @spec PROTECTED-HOOK-LANE-7."""
    path = WORKER_TESTS / "kernel" / "conftest.py"
    assert path.exists(), "worker kernel harness absent"
    name = "pworker_kernel_conftest"
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def lane_names():
    """The names the lane gives the unchanged components, @spec PROTECTED-HOOK-LANE-7."""
    return {
        "stream": wb.STREAM,
        "group": wb.GROUP,
        "prefix": wb.WORKER_PREFIX,
        "sandbox_prefix": wb.SANDBOX_PREFIX,
    }


@contextlib.asynccontextmanager
async def under_role(owned, monkeypatch):
    """A real kernel harness whose Valkey handles are the pinned worker client's."""
    conftest = kernel_conftest()
    _, client_type = (
        wb.worker_transport().WorkerCredential,
        wb.worker_transport().AuthenticatedWorkerClient,
    )
    client = await client_type.connect(
        owned.broker.manifest(), owned.credential(), owned.broker.ca_pem
    )
    lane = client.lane()
    monkeypatch.setattr(conftest, "AsyncRedis", lambda *a, **k: lane.runs)
    try:
        async with conftest.kernel_harness(
            lane_names(),
            lane.affinity,
            dead_letter_stream=wb.DEAD_LETTER,
            consumer_name=wb.CONSUMER,
            reclaim_min_idle_ms=0,
        ) as harness:
            yield harness, client
    finally:
        await client.close()


@pytest.fixture
def enqueue_writer(pworker_service):
    """An owned enqueue principal writing the stream the way admission does."""
    principal = wb.WorkerPrincipal(
        pworker_service, role="enqueue", rules=admission_acl_rules("enqueue")
    )
    principal.install()
    client = principal.raw(decode_responses=True)
    try:
        yield client
    finally:
        client.close()
        principal.remove()


def foreign_keys(broker):
    """Keys outside every family the worker may own or the provisioner seeded."""
    return [
        key.decode()
        for key in broker.command("KEYS", "*")
        if not key.decode().startswith(ALLOWED_PREFIXES)
    ]


EXPECTED_FAILURES = {
    "thread-reset drain before turn",  # prefix: logged with the entry id appended
    "maintenance tick failed",
}


def assert_only_expected_failures(caplog, *, drained=True):
    """Worker faults are logged, not raised: no Valkey error may hide in a log line other
    than the wrapped thread-reset drain's, @spec PROTECTED-HOOK-LANE-3."""
    from redis.exceptions import NoPermissionError, ResponseError

    seen = [
        (record.getMessage(), record.exc_info[0])
        for record in caplog.records
        if record.levelno >= logging.WARNING
        and record.exc_info
        and issubclass(record.exc_info[0], ResponseError)
    ]
    assert seen or not drained, "the wrapped thread-reset drain denial was not observed"
    for message, kind in seen:
        assert any(message.startswith(prefix) for prefix in EXPECTED_FAILURES), message
        assert issubclass(kind, NoPermissionError), (message, kind)


def assert_only_expected_denials(owned):
    """No missing grant: the only refusals are the wrapped thread-reset drain."""
    entries = owned.denials()
    assert {entry[b"object"] for entry in entries} <= ALLOWED_DENIALS, [
        (entry[b"reason"], entry[b"object"], entry[b"context"]) for entry in entries
    ]
    assert all(entry[b"reason"] == b"key" for entry in entries)


def test_a_turn_runs_end_to_end_and_acks_under_the_worker_role(
    owned_worker, enqueue_writer, monkeypatch, caplog
):
    """Stream read, group, delivery lease, lock, markers, completion outbox, liveness,
    sandbox affinity and the reply, from an entry written by the enqueue role,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    from aci_protocol import Final, SessionStatus, TextDelta
    from curie_dispatcher.queue import to_stream_fields
    from curie_worker.consumer import Consumer
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from queue_fixtures import qevent, wait_until

    broker = owned_worker.broker
    broker.command("SET", wb.CONTROL, "control-bytes")
    broker.command("SET", wb.BINDING, b"binding-bytes")

    async def go():
        async with under_role(owned_worker, monkeypatch) as (h, client):
            h.runner.default_script = [
                TextDelta(text="hi "),
                Final(text="answer", status=SessionStatus.DONE),
            ]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            event = qevent("hello", thread="tc1", event_id="event-example-1")
            enqueue_writer.xadd(wb.STREAM, to_stream_fields(event))
            task = asyncio.create_task(consumer.run())
            await wait_until(lambda: h.sink.last_text == "answer", "the reply")
            consumer.request_stop()
            await task
            assert h.runner.opened == ["hello"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0
            assert await h.async_redis.exists(h.config.done_key("event-example-1")) == 1
            assert await client.read_control(wb.CONTROL) == b"control-bytes"
            assert await client.read_binding(wb.BINDING_EVENT) == b"binding-bytes"

    caplog.set_level(logging.WARNING)
    asyncio.run(go())
    assert_only_expected_failures(caplog)
    assert_only_expected_denials(owned_worker)
    assert foreign_keys(broker) == []
    assert broker.command("GET", wb.CONTROL) == b"control-bytes"
    assert broker.command("GET", wb.BINDING) == b"binding-bytes"
    assert broker.command("KEYS", wb.SANDBOX_PREFIX + ":route:*"), "no affinity route was kept"
    assert broker.command("KEYS", "curie:runs:consumer-heartbeat-capable:*"), "no liveness marker"
    assert broker.command("KEYS", wb.WORKER_PREFIX + ":*"), "no worker marker was kept"


def test_an_unparseable_entry_goes_to_the_lane_graveyard_not_curie_runs_dead(
    owned_worker, enqueue_writer, monkeypatch, caplog
):
    """The graveyard is a lane key; ``curie:runs:dead`` stays ungranted,
    @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    from curie_worker.consumer import Consumer
    from curie_worker.delivery_lease import DeliveryLeaseStore

    broker = owned_worker.broker

    async def go():
        async with under_role(owned_worker, monkeypatch) as (h, _client):
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            assert h.config.dead_letter_stream_name() == wb.DEAD_LETTER
            await consumer.ensure_group()
            entry = enqueue_writer.xadd(wb.STREAM, {"garbage": "x"})
            delivered = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=10
            )
            for _stream, entries in delivered:
                for entry_id, fields in entries:
                    await consumer._dispatch(entry_id, fields)
            await asyncio.gather(*list(consumer._inflight))
            rows = await h.async_redis.xrange(wb.DEAD_LETTER)
            assert [fields["dl_original_id"] for _id, fields in rows] == [entry]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0
            assert h.runner.opened == []

    caplog.set_level(logging.WARNING)
    asyncio.run(go())
    assert_only_expected_failures(caplog, drained=False)
    assert_only_expected_denials(owned_worker)
    assert broker.command("EXISTS", "curie:runs:dead") == 0
    assert foreign_keys(broker) == []


def test_a_crashed_consumers_pending_entry_is_reclaimed_and_run_under_the_role(
    owned_worker, enqueue_writer, monkeypatch, caplog
):
    """XAUTOCLAIM, the lease transfer script, the reclaim lock and the idle-owner
    checks, @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-6."""
    from aci_protocol import Final, SessionStatus
    from curie_dispatcher.queue import to_stream_fields
    from curie_worker.consumer import Consumer
    from curie_worker.delivery_lease import DeliveryLeaseStore
    from queue_fixtures import qevent, wait_until

    broker = owned_worker.broker

    async def go():
        async with under_role(owned_worker, monkeypatch) as (h, _client):
            h.runner.default_script = [Final(text="recovered", status=SessionStatus.DONE)]
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=DeliveryLeaseStore(h.async_redis, h.config),
            )
            await consumer.ensure_group()
            event = qevent("recover me", thread="tc2", event_id="event-example-2")
            enqueue_writer.xadd(wb.STREAM, to_stream_fields(event))
            await h.async_redis.xreadgroup(
                h.config.consumer_group, "crashed-consumer", {h.config.stream: ">"}, count=1
            )
            await consumer._reclaim_once()
            await wait_until(lambda: h.sink.last_text == "recovered", "the recovered reply")
            await asyncio.gather(*list(consumer._inflight))
            assert h.runner.opened == ["recover me"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0

    caplog.set_level(logging.WARNING)
    asyncio.run(go())
    assert_only_expected_failures(caplog)
    assert_only_expected_denials(owned_worker)
    assert foreign_keys(broker) == []
