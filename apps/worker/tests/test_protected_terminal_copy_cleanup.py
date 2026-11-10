"""No-execution cleanup of a real legacy terminal copy under retained settlement.

The shared legacy producer is invoked deliberately after aging done is removed;
protected maintenance already refuses this reschedule. No witness is fabricated.
"""

import asyncio
import time
from dataclasses import replace

import pytest
from curie_protected_hooks.admission_records import DeliveryIdentity, delivery_digest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_worker.capacity_wait import CapacityWaitStore
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.protected_run import ProtectedRuntime
from sqlalchemy import text

from . import test_protected_retirement_server as actual
from .test_protected_consumer_recovery import AdmittedExternalWorkload
from .test_protected_primary_runtime import database, external_reply
from .test_protected_primary_runtime import primary_postgres as primary_postgres

pworker_service = actual.wb.pworker_service
owned_worker = actual.wb.owned_worker


@pytest.mark.parametrize("fence", ["spent", "stale-lease", "changed-wait"])
def test_actual_retained_terminal_copy_cleanup_never_restarts_execution(
    owned_worker, tmp_path, primary_postgres, monkeypatch, fence
):
    monkeypatch.setattr(actual, "ExternalProofWorkload", AdmittedExternalWorkload)

    async def go():
        async with actual.actual_pair(owned_worker, tmp_path, intake=True) as (
            _server,
            client,
            external,
            delivery,
            _unused,
            _request,
        ):
            ns = derive_lane_namespace(client._manifest)
            cfg = actual.wb.lane_config(
                owned_worker.broker,
                key_prefix=ns.prefix,
                consumer_group=ns.group,
                dead_letter_stream=ns.dead,
                max_attempts=1,
                max_concurrency=1,
                delivery_lease_ttl_s=15,
                delivery_lease_heartbeat_s=5,
                consumer_name="worker/model",
            )
            runs = client._broker.lane().runs
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            rows = await runs.xreadgroup(
                cfg.consumer_group, "worker/seed", {cfg.stream: ">"}, count=1
            )
            entry, fields = rows[0][1][0]
            seed_cfg = cfg.model_copy(update={"consumer_name": "worker/seed"})
            seed_leases = DeliveryLeaseStore(runs, seed_cfg)
            seed_lease = await seed_leases.acquire(
                cfg.stream, cfg.consumer_group, entry, consumer="worker/seed"
            )
            legacy = CapacityWaitStore(runs, seed_cfg)
            record = await legacy.park(entry, fields, delivery.turn.event_id, seed_lease)
            assert record.fields == dict(delivery.source_fields)
            await seed_leases.release(
                cfg.stream,
                cfg.consumer_group,
                entry,
                owner=seed_lease.owner,
                resume_event_id=seed_lease.resume_event_id,
            )
            seconds, micros = await runs.time()
            await runs.zadd(
                legacy._due, {delivery.turn.event_id: seconds * 1000 + micros // 1000 - 1}
            )
            assert await legacy.wake_due(limit=1) == 1
            engine, schema = await database(primary_postgres, client)
            runtimes = []
            task = None
            try:
                async with external_reply() as (router, replies):

                    def runtime(config):
                        instance = ProtectedRuntime(
                            broker=client._broker,
                            manifest=client._manifest,
                            engine=engine,
                            db_schema=schema,
                            execution=client._execution,
                            launch_client=client,
                            config=config,
                            reply_sink=router,
                        )
                        runtimes.append(instance)
                        return instance

                    model = runtime(cfg)
                    task = asyncio.create_task(model.run())
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        owed = await model.markers.restore_owed(delivery.turn.event_id)
                        if (
                            owed is not None
                            and owed.resolution is not None
                            and (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"]
                            == 0
                        ):
                            break
                        if task.done():
                            await task
                        await asyncio.sleep(0.05)
                    assert time.monotonic() < deadline
                    assert owed is not None and owed.resolution is not None
                    witness = owed.witness.canonical_bytes
                    model.request_stop()
                    await asyncio.wait_for(task, 10)
                    task = None
                    # Real old transport reproduces the previously measured aging-done
                    # defect. The new protected scheduler is intentionally not used here.
                    seconds, micros = await runs.time()
                    now = seconds * 1000 + micros // 1000
                    await runs.delete(cfg.done_key(delivery.turn.event_id))
                    await runs.hset(
                        legacy._record(delivery.turn.event_id),
                        mapping={"state": "active", "deadline_ms": str(now - 1)},
                    )
                    await runs.zadd(legacy._flight, {delivery.turn.event_id: now - 1})
                    assert await legacy.reconcile_lost_wakes(limit=1) == 1
                    copied_id = await runs.hget(
                        legacy._record(delivery.turn.event_id), "wake_entry"
                    )
                    assert copied_id and copied_id != owed.witness.as_dict()["delivery_entry_id"]
                    copied = await runs.xreadgroup(
                        cfg.consumer_group, "worker/spent", {cfg.stream: ">"}, count=1
                    )
                    assert copied[0][1][0][0] == copied_id
                    spent_cfg = cfg.model_copy(update={"consumer_name": "worker/spent"})
                    spent_leases = DeliveryLeaseStore(runs, spent_cfg)
                    spent = await spent_leases.acquire(
                        cfg.stream, cfg.consumer_group, copied_id, consumer="worker/spent"
                    )
                    # Deterministic elapsed-budget fault on a state created by the real
                    # lease store; replacement HSETNX must preserve this expired value.
                    expired = now - 1
                    await runs.hset(
                        cfg.delivery_state_key(cfg.stream, cfg.consumer_group, copied_id),
                        "deadline_ms",
                        str(expired),
                    )
                    await spent_leases.release(
                        cfg.stream,
                        cfg.consumer_group,
                        copied_id,
                        owner=spent.owner,
                        resume_event_id=spent.resume_event_id,
                    )
                    received_expired = []
                    real_acquire = DeliveryLeaseStore.acquire

                    async def observe_acquire(store, *args, **kwargs):
                        lease = await real_acquire(store, *args, **kwargs)
                        if kwargs.get("consumer") == "worker/cleanup":
                            received_expired.append(
                                lease.budget.deadline_ms == expired and lease.remaining_s() <= 0
                            )
                        return lease

                    monkeypatch.setattr(DeliveryLeaseStore, "acquire", observe_acquire)
                    cleanup = runtime(cfg.model_copy(update={"consumer_name": "worker/cleanup"}))
                    injected = asyncio.Event()
                    identity_checked = []
                    real_operation = cleanup.consumer._cleanup_operation

                    def inject(issued, lease):
                        if fence == "spent":
                            from curie_worker.protected_binding import current_protected_delivery
                            from curie_worker.protected_dispatch import ProtectedDispatchUnavailable
                            from curie_worker.protected_execution import _CURRENT
                            from curie_worker.protected_recovery import current_protected_recovery

                            assert current_protected_delivery() is None
                            assert _CURRENT.get() is None
                            assert current_protected_recovery() is None
                            assert not hasattr(issued, "turn") and not hasattr(issued, "delivery")
                            eval_before = actual.wb.command_count(owned_worker.broker, "eval")
                            with pytest.raises(ProtectedDispatchUnavailable):
                                real_operation(replace(issued), lease)
                            assert (
                                actual.wb.command_count(owned_worker.broker, "eval") == eval_before
                            )
                            identity_checked.append(True)
                        operation = real_operation(issued, lease)
                        if fence == "stale-lease":
                            owned_worker.broker.command(
                                "SET",
                                cfg.delivery_lease_key(cfg.stream, cfg.consumer_group, copied_id),
                                "successor-example",
                                "PX",
                                15000,
                            )
                        elif fence == "changed-wait":
                            owned_worker.broker.command(
                                "HSET",
                                legacy._record(delivery.turn.event_id),
                                "reply_ref",
                                "message/changed-example",
                            )
                        injected.set()
                        return operation

                    monkeypatch.setattr(cleanup.consumer, "_cleanup_operation", inject)
                    reply_count = len(replies)
                    task = asyncio.create_task(cleanup.run())
                    if fence == "spent":
                        deadline = time.monotonic() + 3
                        while (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"]:
                            assert time.monotonic() < deadline, (
                                "retained terminal copy remained pending"
                            )
                            if task.done():
                                await task
                            await asyncio.sleep(0.05)
                    else:
                        await asyncio.wait_for(injected.wait(), 3)
                        await asyncio.sleep(0.1)
                        assert (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"] == 1
                    assert received_expired and all(received_expired)
                    if fence == "spent":
                        assert identity_checked == [True]
                    retained = await cleanup.markers.restore_owed(delivery.turn.event_id)
                    assert retained is not None and retained.witness.canonical_bytes == witness
                    assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
                    assert external.events.count(("RUNNER_POST", "/v1/turn-admit")) == 1
                    assert len(replies) == reply_count
                    identity = DeliveryIdentity(**delivery.intent.as_dict()["identity"])
                    assert (
                        owned_worker.broker.command(
                            "ZSCORE", "protected:admission:quota", delivery_digest(identity)
                        )
                        is not None
                    )
            finally:
                for instance in runtimes:
                    instance.request_stop()
                if task is not None:
                    await asyncio.wait_for(task, 10)
                for instance in runtimes:
                    await instance.close()
                async with engine.begin() as connection:
                    await connection.execute(text("DROP SCHEMA " + schema + " CASCADE"))
                await engine.dispose()

    asyncio.run(go())
