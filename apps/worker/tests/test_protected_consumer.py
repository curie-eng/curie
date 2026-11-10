"""Parking-only protected transport on actual TLS Valkey.

A real Kernel/Consumer is assembled. Kubernetes and the reply sink are external
fakes used to prove they are never called; no runner/model request is made.
This does not qualify accepted execution, capacity recovery or outbox delivery.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest
from curie_worker.config import WorkerConfig
from curie_worker.kernel.core import Kernel
from curie_worker.markers import Markers
from curie_worker.protected_binding import _CURRENT_DELIVERY
from curie_worker.run import _substrate_config
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox import AffinityStore, SandboxSubstrate
from curie_worker.threadlock import ThreadLock

sys.path.insert(0, str(Path(__file__).parent))
from test_protected_lane import pel, proof  # noqa: E402

pworker_service = proof.pworker_service
owned_worker = proof.owned_worker
kernel_fixtures = importlib.import_module("apps.worker.tests.kernel.conftest")


def product():
    result = importlib.import_module("curie_worker.protected_lane")
    assert hasattr(result, "ProtectedConsumer"), "protected consumer is absent"
    return result


@asynccontextmanager
async def assembled(owned, **overrides):
    async with proof.connected(proof.verification(), owned) as client:
        config = proof.wb.lane_config(owned.broker, reclaim_min_idle_ms=1, **overrides)
        requests = []
        trace = aiohttp.TraceConfig()

        async def record_request(_session, _context, params):
            requests.append(str(params.url))

        trace.on_request_start.append(record_request)
        session = aiohttp.ClientSession(trace_configs=[trace])
        consumer = None
        runner = None
        try:
            runner = RunnerClient(session=session, total_timeout_s=config.runner_total_timeout_s)
            k8s = kernel_fixtures.FakeK8s()
            sink = kernel_fixtures.FakeSink()
            substrate = SandboxSubstrate(
                k8s,
                AffinityStore(
                    client.lane().affinity,
                    pressure_client=client.lane().runs,
                    key_prefix=proof.wb.SANDBOX_PREFIX,
                ),
                replace(
                    _substrate_config({}),
                    namespace="test-ns",
                    warm_pool="test-pool",
                    runner_port=1,
                    key_prefix=proof.wb.SANDBOX_PREFIX,
                ),
            )
            kernel = Kernel(
                substrate=substrate,
                runner=runner,
                sink=sink,
                lock=ThreadLock(
                    client.lane().runs,
                    ttl_ms=config.lock_ttl_ms,
                    acquire_timeout_s=2.0,
                    poll_interval_s=0.01,
                ),
                pressure_lock=ThreadLock(
                    client.lane().runs,
                    ttl_ms=config.lock_ttl_ms,
                    acquire_timeout_s=2.0,
                    poll_interval_s=0.01,
                ),
                markers=Markers(client.lane().runs, config),
                config=config,
            )
            consumer = product().ProtectedConsumer(
                client=client,
                trusted_manifest=owned.broker.manifest(),
                kernel=kernel,
                config=config,
            )
            yield SimpleNamespace(
                consumer=consumer,
                client=client,
                config=config,
                kernel=kernel,
                k8s=k8s,
                sink=sink,
                requests=requests,
            )
        finally:
            if consumer is not None:
                consumer.request_stop()
                await asyncio.gather(*consumer._inflight, return_exceptions=True)
            if runner is not None:
                await runner.close()
            await session.close()


async def read_pending(h, *, owner=None):
    await h.consumer.ensure_group()
    rows = await h.client.lane().runs.xreadgroup(
        h.config.consumer_group,
        owner or h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    assert len(rows) == 1 and len(rows[0][1]) == 1, "protected first boot must read backlog"
    return rows[0][1][0]


async def dispatch(h, entry_id, fields):
    await h.consumer._dispatch(entry_id, fields)
    await asyncio.gather(*h.consumer._inflight)
    assert _CURRENT_DELIVERY.get() is None


def no_execution(h):
    assert h.k8s.claims == {}
    assert h.sink.events == []
    assert h.requests == []


def poison(broker, fields=None):
    proof.admitted(broker)
    broker.command("XTRIM", proof.wb.STREAM, "MAXLEN", "=", 0)
    fields = fields or {"payload": "malformed", "opaque": "retain-me", "dl_reason": "original"}
    return broker.command(
        "XADD", proof.wb.STREAM, "*", *sum(([k, v] for k, v in fields.items()), [])
    ).decode()


@pytest.mark.parametrize("committed", [False, True])
def test_preparing_and_unqualified_committed_work_stays_pending_without_attempt_charge(
    owned_worker, committed
):
    async def go():
        digest, fields, _ = proof.admitted(owned_worker.broker)
        if not committed:
            owned_worker.broker.command("DEL", proof.PREFIX + "commit:" + digest)
        async with assembled(owned_worker, max_delivery=2) as h:
            entry_id, actual = await read_pending(h)
            assert actual == fields
            before = await pel(h.client, entry_id)
            await dispatch(h, entry_id, fields)
            # Even an exhausted peer-owned row must not be capped or claimed.
            owned_worker.broker.command(
                "XCLAIM", h.config.stream, h.config.consumer_group, "peer", 0, entry_id
            )
            owned_worker.broker.admin.xclaim(
                h.config.stream, h.config.consumer_group, "peer", 0, [entry_id],
                idle=2000, justid=True,
            )
            exhausted = await pel(h.client, entry_id)
            assert exhausted == ("peer", 2)
            assert await h.consumer._reclaim_once() == 0
            assert await pel(h.client, entry_id) == exhausted
            assert before == (h.config.consumer_name, 1)
            assert not await h.consumer._leases.has_state(
                h.config.stream, h.config.consumer_group, entry_id
            )
            assert await h.client.lane().runs.xrange(h.config.dead_letter_stream_name()) == []
            no_execution(h)

    asyncio.run(go())


def test_owned_refusal_atomically_copies_opaque_fields_and_settles(owned_worker):
    async def go():
        poison(owned_worker.broker)
        async with assembled(owned_worker, dead_letter_maxlen=2) as h:
            entry_id, fields = await read_pending(h)
            await dispatch(h, entry_id, fields)
            assert (
                await h.client.lane().runs.xpending_range(
                    h.config.stream, h.config.consumer_group, "-", "+", 1
                )
                == []
            )
            rows = await h.client.lane().runs.xrange(h.config.dead_letter_stream_name())
            assert len(rows) == 1
            dead = rows[0][1]
            assert dead["payload"] == "malformed"
            assert dead["opaque"] == "retain-me"
            assert dead["dl_dl_reason"] == "original"
            assert dead["dl_reason"] == "protected-proof-refused"
            assert dead["dl_original_id"] == entry_id
            assert dead["dl_delivery_count"] == "1"
            assert not await h.consumer._leases.has_state(
                h.config.stream, h.config.consumer_group, entry_id
            )
            no_execution(h)

    asyncio.run(go())


@pytest.mark.parametrize(
    "changed", ["token", "generation", "pel-owner", "pel-count", "fields", "trimmed"]
)
def test_stale_owned_refusal_writes_no_dead_letter_and_does_not_ack(owned_worker, changed):
    async def go():
        poison(owned_worker.broker)
        async with assembled(owned_worker) as h:
            entry_id, fields = await read_pending(h)
            consumer = h.consumer
            before = await pel(h.client, entry_id)
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                broker = owned_worker.broker
                if changed == "token":
                    broker.command(
                        "SET",
                        h.config.delivery_lease_key(
                            h.config.stream, h.config.consumer_group, entry_id
                        ),
                        "successor-owner",
                        "PX",
                        5000,
                    )
                elif changed == "generation":
                    broker.command(
                        "HINCRBY",
                        h.config.delivery_state_key(
                            h.config.stream, h.config.consumer_group, entry_id
                        ),
                        "gen",
                        1,
                    )
                elif changed == "pel-owner":
                    broker.admin.xclaim(
                        h.config.stream, h.config.consumer_group, "peer", 0, [entry_id],
                        justid=True,
                    )
                elif changed == "pel-count":
                    broker.command(
                        "XCLAIM",
                        h.config.stream,
                        h.config.consumer_group,
                        h.config.consumer_name,
                        0,
                        entry_id,
                    )
                elif changed == "fields":
                    fields = {**fields, "opaque": "substitution"}
                else:
                    broker.command("XDEL", h.config.stream, entry_id)
                current = await pel(h.client, entry_id)
                await consumer._dead_letter(
                    entry_id, fields, reason="protected-proof-refused", delivery_count=before[1]
                )
                assert await h.client.lane().runs.xrange(h.config.dead_letter_stream_name()) == []
                assert await pel(h.client, entry_id) == current
            no_execution(h)

    asyncio.run(go())


@pytest.mark.parametrize("exhausted", [False, True])
def test_under_cap_refusal_reclaims_but_cross_owner_exhausted_refusal_stays_pending(
    owned_worker, exhausted
):
    async def go():
        poison(owned_worker.broker)
        async with assembled(owned_worker, max_delivery=2 if exhausted else 3) as h:
            entry_id, _ = await read_pending(h, owner="peer")
            owned_worker.broker.admin.xclaim(
                h.config.stream, h.config.consumer_group, "peer", 0, [entry_id],
                idle=2000, justid=not exhausted,
            )
            before = await pel(h.client, entry_id)
            if exhausted:
                assert before == ("peer", 2)
                assert await h.consumer._reclaim_once() == 0
                assert await pel(h.client, entry_id) == before
                assert await h.client.lane().runs.xrange(h.config.dead_letter_stream_name()) == []
            else:
                assert await h.consumer._reclaim_once() == 1
                await asyncio.gather(*h.consumer._inflight)
                dead = await h.client.lane().runs.xrange(h.config.dead_letter_stream_name())
                assert len(dead) == 1 and dead[0][1]["dl_delivery_count"] == "2"
            no_execution(h)

    asyncio.run(go())


@pytest.mark.parametrize(
    "path",
    [
        "_startup_completion_sweep",
        "_capacity_wake_loop",
        "_capacity_notice_loop",
        "_drain_thread_reset_requests",
        "_sweep_pending_progress",
        "_drain_pending_progress_inboxes",
    ],
)
def test_ordinary_maintenance_entrypoints_fail_closed_without_context(owned_worker, path):
    async def go():
        proof.admitted(owned_worker.broker)
        async with assembled(owned_worker) as h:
            with pytest.raises(product().ProtectedExecutionUnavailable):
                await getattr(h.consumer, path)()
            with pytest.raises(product().ProtectedExecutionUnavailable):
                await h.consumer._kernel.sweep_pending_completions()
            no_execution(h)

    asyncio.run(go())


def test_protected_dead_letter_is_bounded_under_rejected_input_flood(owned_worker):
    async def go():
        poison(owned_worker.broker)
        async with assembled(owned_worker, dead_letter_maxlen=2) as h:
            for _ in range(205):
                if _:
                    await h.client.lane().runs.xadd(h.config.stream, {"payload": "malformed"})
                entry_id, fields = await read_pending(h)
                await dispatch(h, entry_id, fields)
            rows = await h.client.lane().runs.xrange(h.config.dead_letter_stream_name())
            assert 0 < len(rows) < 205
            assert rows[-1][1]["dl_original_id"] == entry_id
            assert (
                await h.client.lane().runs.xpending_range(
                    h.config.stream, h.config.consumer_group, "-", "+", 1
                )
                == []
            )
            no_execution(h)

    asyncio.run(go())


@pytest.mark.parametrize("copy", [False, True])
def test_mutable_refusal_and_unsigned_capacity_copy_stay_parked(owned_worker, copy):
    async def go():
        _, original, _ = proof.admitted(owned_worker.broker)
        broker = owned_worker.broker
        fields = dict(original)
        if copy:
            fields["curie_capacity_wait_generation"] = "1"
            broker.command("XTRIM", proof.wb.STREAM, "MAXLEN", "=", 0)
            broker.command(
                "XADD", proof.wb.STREAM, "*", *sum(([k, v] for k, v in fields.items()), [])
            )
        else:
            broker.command("DEL", proof.PREFIX + "binding:event/example")
        async with assembled(owned_worker) as h:
            entry_id, actual = await read_pending(h)
            assert actual == fields
            before = await pel(h.client, entry_id)
            assert not await h.consumer._refusal_settlement_eligible(entry_id, fields)
            await dispatch(h, entry_id, fields)
            # Restoring a missing binding makes the exact same entry legitimate.
            if not copy:
                broker.command(
                    "SET", proof.PREFIX + "binding:event/example", original["protected_envelope"]
                )
            await h.consumer._dead_letter(
                entry_id, fields, reason="protected-proof-refused", delivery_count=1
            )
            assert await pel(h.client, entry_id) == before
            assert await h.client.lane().runs.xrange(h.config.dead_letter_stream_name()) == []
            assert not await h.consumer._leases.has_state(
                h.config.stream, h.config.consumer_group, entry_id
            )
            no_execution(h)

    asyncio.run(go())


def test_terminal_ack_preserves_the_other_group_and_physical_source_row(owned_worker):
    async def go():
        entry_id = poison(owned_worker.broker)
        broker = owned_worker.broker
        broker.command("XGROUP", "CREATE", proof.wb.STREAM, "ordinary-workers", "0")
        broker.command(
            "XREADGROUP",
            "GROUP",
            "ordinary-workers",
            "ordinary-owner",
            "COUNT",
            1,
            "STREAMS",
            proof.wb.STREAM,
            ">",
        )
        before = broker.admin.xpending_range(
            proof.wb.STREAM, "ordinary-workers", entry_id, entry_id, 1
        )
        async with assembled(owned_worker) as h:
            actual_id, fields = await read_pending(h)
            assert actual_id == entry_id
            await dispatch(h, entry_id, fields)
            after = broker.admin.xpending_range(
                proof.wb.STREAM, "ordinary-workers", entry_id, entry_id, 1
            )
            keys = ("message_id", "consumer", "times_delivered")
            assert [tuple(r[k] for k in keys) for r in after] == [
                tuple(r[k] for k in keys) for r in before
            ]
            assert await h.client.lane().runs.xrange(
                h.config.stream, min=entry_id, max=entry_id
            ) == [(entry_id, fields)]
            assert (
                await h.client.lane().runs.xpending_range(
                    h.config.stream, h.config.consumer_group, "-", "+", 1
                )
                == []
            )
            no_execution(h)

    asyncio.run(go())


def test_ordinary_consumer_group_configuration_is_rejected(owned_worker):
    async def go():
        proof.admitted(owned_worker.broker)
        with pytest.raises(ValueError, match="protected consumer configuration"):
            async with assembled(owned_worker, consumer_group="ordinary-workers"):
                pytest.fail("ordinary group must not be configurable on this lane")

    asyncio.run(go())


@pytest.mark.parametrize("block_ms", [0, -1, 1001, WorkerConfig().read_block_ms])
def test_unsafe_protected_read_block_is_rejected_before_reading(owned_worker, block_ms):
    """Removing the protected read bound must fail before XREADGROUP can poison TLS."""
    async def go():
        before = proof.wb.command_count(owned_worker.broker, "xreadgroup")
        with pytest.raises(ValueError, match="protected consumer configuration"):
            async with assembled(owned_worker, read_block_ms=block_ms):
                pytest.fail("unsafe protected read block was accepted")
        assert proof.wb.command_count(owned_worker.broker, "xreadgroup") == before

    asyncio.run(go())


@pytest.mark.parametrize("block_ms", [1, 1000])
def test_protected_boundary_idle_reads_preserve_pinned_sessions(owned_worker, block_ms):
    """Positive bounded reads retain TLS; the maximum idles beyond one socket deadline."""
    async def go():
        async with assembled(owned_worker, read_block_ms=block_ms) as h:
            await h.consumer.ensure_group()
            accepted = proof.wb.connections(owned_worker.broker)
            before = proof.wb.command_count(owned_worker.broker, "xreadgroup")
            reader = asyncio.create_task(h.consumer._read_loop())
            try:
                async with asyncio.timeout(5):
                    while proof.wb.command_count(owned_worker.broker, "xreadgroup") < before + 3:
                        await asyncio.sleep(0.02)
            finally:
                h.consumer.request_stop()
                await asyncio.wait_for(reader, timeout=2)
            assert await h.client.lane().runs.ping()
            assert h.client.lane().affinity.ping()
            assert proof.wb.connections(owned_worker.broker) == accepted
            assert owned_worker.denials() == []
            no_execution(h)

    asyncio.run(go())


def test_atomic_terminal_script_rejects_an_envelope_even_with_a_valid_owned_lease(owned_worker):
    async def go():
        proof.admitted(owned_worker.broker)
        async with assembled(owned_worker) as h:
            entry_id, fields = await read_pending(h)
            before = await pel(h.client, entry_id)
            async with h.consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                result = await h.client.lane().runs.eval(
                    product()._REFUSE_AND_SETTLE_LUA,
                    4,
                    h.config.stream,
                    h.config.dead_letter_stream_name(),
                    h.config.delivery_lease_key(h.config.stream, h.config.consumer_group, entry_id),
                    h.config.delivery_state_key(h.config.stream, h.config.consumer_group, entry_id),
                    h.config.consumer_group,
                    entry_id,
                    h.config.consumer_name,
                    lease.owner,
                    str(lease.generation),
                    str(before[1]),
                    str(h.config.dead_letter_maxlen),
                    "protected-proof-refused",
                    "2026-01-01T00:00:00+00:00",
                    json.dumps([value for item in fields.items() for value in item]),
                )
                assert result == 0
                assert await pel(h.client, entry_id) == before
                assert await h.client.lane().runs.xrange(h.config.dead_letter_stream_name()) == []
                assert await h.consumer._leases.has_state(
                    h.config.stream, h.config.consumer_group, entry_id
                )
            no_execution(h)

    asyncio.run(go())


@pytest.mark.parametrize("broken", ["dead-letter-type", "missing-group"])
def test_terminal_preflight_runtime_errors_cannot_partially_settle(owned_worker, broken):
    async def go():
        poison(owned_worker.broker)
        async with assembled(owned_worker) as h:
            entry_id, fields = await read_pending(h)
            async with h.consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                broker = owned_worker.broker
                if broken == "dead-letter-type":
                    broker.command("SET", h.config.dead_letter_stream_name(), "keep-me")
                else:
                    broker.command("XGROUP", "DESTROY", h.config.stream, h.config.consumer_group)
                await h.consumer._dead_letter(
                    entry_id, fields, reason="protected-proof-refused", delivery_count=1
                )
                assert await h.client.lane().runs.xrange(
                    h.config.stream, min=entry_id, max=entry_id
                ) == [(entry_id, fields)]
                if broken == "dead-letter-type":
                    assert broker.command("GET", h.config.dead_letter_stream_name()) == b"keep-me"
                    assert await pel(h.client, entry_id) == (h.config.consumer_name, 1)
                else:
                    assert (
                        await h.client.lane().runs.xrange(h.config.dead_letter_stream_name()) == []
                    )
            no_execution(h)

    asyncio.run(go())
