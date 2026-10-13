"""Actual original-row delivery cap; diagnostic retention grants no completion."""

import asyncio
import time

import pytest
from curie_protected_hooks.admission_records import DeliveryIdentity, delivery_digest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_verification import restore_committed_event
from curie_worker.protected_run import ProtectedRuntime
from sqlalchemy import text

from . import test_protected_retirement_server as actual
from .test_protected_primary_runtime import database, external_reply
from .test_protected_primary_runtime import primary_postgres as primary_postgres

pworker_service = actual.wb.pworker_service
owned_worker = actual.wb.owned_worker


@pytest.mark.parametrize("fence", ["valid", "successor-lease", "changed-proof"])
def test_actual_original_delivery_cap_retains_acceptance_without_execution(
    owned_worker, tmp_path, primary_postgres, monkeypatch, fence
):
    async def go():
        async with actual.actual_pair(owned_worker, tmp_path, intake=True) as (
            _server,
            client,
            external,
            delivery,
            _lease,
            _request,
        ):
            ns = derive_lane_namespace(client._manifest)
            cfg = actual.wb.lane_config(
                owned_worker.broker,
                key_prefix=ns.prefix,
                consumer_group=ns.group,
                dead_letter_stream=ns.dead,
                max_attempts=1,
                max_delivery=2,
                max_concurrency=1,
                consumer_name="worker/cap",
            )
            runs = client._broker.lane().runs
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            rows = await runs.xreadgroup(
                cfg.consumer_group, "worker/failed", {cfg.stream: ">"}, count=1
            )
            entry, fields = rows[0][1][0]
            assert set(fields) == {"payload", "protected_envelope"}
            await runs.xclaim(
                cfg.stream,
                cfg.consumer_group,
                "worker/failed",
                0,
                [entry],
                idle=cfg.reclaim_min_idle_ms + 1000,
            )
            pending = await runs.xpending_range(cfg.stream, cfg.consumer_group, entry, entry, 1)
            assert pending[0]["times_delivered"] == cfg.max_delivery
            proof = await restore_committed_event(
                client._broker, delivery.turn.event_id, trusted_manifest=client._manifest
            )
            assert proof is not None
            engine, schema = await database(primary_postgres, client)
            instance = None
            task = None
            try:
                async with external_reply() as (router, replies):
                    instance = ProtectedRuntime(
                        broker=client._broker,
                        manifest=client._manifest,
                        engine=engine,
                        db_schema=schema,
                        execution=client._execution,
                        launch_client=client,
                        config=cfg,
                        reply_sink=router,
                    )
                    injected = asyncio.Event()
                    if fence != "valid":
                        original = instance.consumer._cap_operation

                        def interleave(*args):
                            operation = original(*args)
                            if fence == "successor-lease":
                                owned_worker.broker.command(
                                    "SET",
                                    cfg.delivery_lease_key(cfg.stream, cfg.consumer_group, entry),
                                    "successor-example",
                                    "PX",
                                    15000,
                                )
                            else:
                                key, raw = next(
                                    (k, v)
                                    for k, v in proof.proof_snapshots
                                    if v is not None and ":binding:" in k
                                )
                                owned_worker.broker.command("SET", key, raw + b" ")
                            injected.set()
                            return operation

                        monkeypatch.setattr(instance.consumer, "_cap_operation", interleave)
                    task = asyncio.create_task(instance.run())
                    if fence == "valid":
                        deadline = time.monotonic() + 3
                        while (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"]:
                            assert time.monotonic() < deadline, (
                                "original capped delivery remained pending"
                            )
                            if task.done():
                                await task
                            await asyncio.sleep(0.05)
                    else:
                        await asyncio.wait_for(injected.wait(), 3)
                        await asyncio.sleep(0.05)
                        assert (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"] == 1
                    graves = await runs.xrange(ns.dead)
                    assert len(graves) == (1 if fence == "valid" else 0)
                    if graves:
                        row = graves[0][1]
                        assert {k: row[k] for k in fields} == fields
                        assert row["dl_original_id"] == entry
                        assert row["dl_delivery_count"] == str(cfg.max_delivery)
                        assert row["dl_reason"] == "max-delivery-exceeded"
                    assert await instance.markers.restore_owed(delivery.turn.event_id) is None
                    assert external.events.count(("RUNNER_POST", "/v1/event")) == 0
                    assert external.events.count(("RUNNER_POST", "/v1/turn-admit")) == 0
                    assert replies == []
                    identity = DeliveryIdentity(**delivery.intent.as_dict()["identity"])
                    assert (
                        owned_worker.broker.command(
                            "ZSCORE", "protected:admission:quota", delivery_digest(identity)
                        )
                        is not None
                    )
                    if fence != "changed-proof":
                        retained = await restore_committed_event(
                            client._broker,
                            delivery.turn.event_id,
                            trusted_manifest=client._manifest,
                        )
                        assert (
                            retained is not None
                            and retained.proof_snapshots == proof.proof_snapshots
                        )
            finally:
                if instance is not None:
                    instance.request_stop()
                if task is not None:
                    await asyncio.wait_for(task, 10)
                if instance is not None:
                    await instance.close()
                async with engine.begin() as connection:
                    await connection.execute(text("DROP SCHEMA " + schema + " CASCADE"))
                await engine.dispose()

    asyncio.run(go())
