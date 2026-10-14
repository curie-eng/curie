"""Actual protected consumer recovery; only external workload/reply endpoints simulated."""

import asyncio
import json
import time
from contextlib import asynccontextmanager

import pytest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_worker.capacity_wait import CapacityWaitStore
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.protected_run import ProtectedRuntime

from . import test_protected_retirement_server as actual
from .test_protected_primary_runtime import database, external_reply
from .test_protected_primary_runtime import primary_postgres as primary_postgres

pworker_service = actual.wb.pworker_service
owned_worker = actual.wb.owned_worker


@asynccontextmanager
async def retained_ref_reply():
    """External Slack alone is synthetic; adapter and reply router are real."""
    from aiohttp import web
    from curie_worker.reply_sink import HttpReplyAdapter, ReplySinkRouter, SlackReplyAdapter

    calls = []
    posted = 0

    async def reply(request):
        nonlocal posted
        assert request.headers.get("Authorization") == "Bearer synthetic-slack-token"
        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            body = dict(await request.post())
        calls.append((request.path, body))
        if request.path.endswith("/chat.postMessage"):
            posted += 1
            ref = "message/queued-example" if posted == 1 else "message/duplicate-example"
        else:
            ref = body.get("ts", "message/queued-example")
        return web.json_response({"ok": True, "ts": ref, "channel": "channel/example"})

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", reply)
    owner = web.AppRunner(app, access_log=None)
    await owner.setup()
    router = None
    try:
        site = web.TCPSite(owner, "127.0.0.1", 0)
        await site.start()
        base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1]) + "/api/"
        slack = SlackReplyAdapter(
            "synthetic-slack-token",
            identity_tokens={"adapter/example": "synthetic-slack-token"},
            base_url=base,
        )
        router = ReplySinkRouter(adapters={"slack": slack}, default=HttpReplyAdapter({}))
        yield router, calls
    finally:
        if router is not None:
            await router.aclose()
        await owner.cleanup()


class AdmittedExternalWorkload(actual.ExternalProofWorkload):
    """The synthetic runner emits its final frame only after actual mediator admission."""

    async def runner(self, request):
        response = await super().runner(request)
        if request.path == "/v1/turn-admit" and self.decision == "granted":
            self.finish.set()
        return response


@pytest.mark.parametrize(
    "mode",
    [
        "capacity-copy",
        "capacity-terminal-schedule",
        "capacity-reply-ref",
        "live-peer",
        "sustained-live-peer",
    ],
)
def test_actual_consumer_capacity_copy_or_live_peer_fence(
    owned_worker,
    tmp_path,
    primary_postgres,
    monkeypatch,
    mode,
):
    monkeypatch.setattr(actual, "ExternalProofWorkload", AdmittedExternalWorkload)
    observations = []
    if mode == "sustained-live-peer":
        from curie_worker.consumer_liveness import ConsumerLivenessStore, consumer_heartbeat_key

        real_renew = ConsumerLivenessStore.renew

        async def observe_renew(store, **kwargs):
            key = consumer_heartbeat_key(kwargs["stream"], kwargs["group"], kwargs["consumer"])
            # Only non-secret predicates survive instrumentation; real method and Lua run.
            matches = await store._redis.get(key) == kwargs["token"]
            positive_ttl = await store._redis.pttl(key) > 0
            result = await real_renew(store, **kwargs)
            if len(observations) < 4:
                observations.append((matches, positive_ttl, result))
            return result

        monkeypatch.setattr(ConsumerLivenessStore, "renew", observe_renew)

    async def go():
        async with actual.actual_pair(owned_worker, tmp_path, intake=True) as (
            server,
            client,
            external,
            original,
            unused,
            request,
        ):
            ns = derive_lane_namespace(client._manifest)
            cfg = actual.wb.lane_config(
                owned_worker.broker,
                key_prefix=ns.prefix,
                consumer_group=ns.group,
                dead_letter_stream=ns.dead,
                max_attempts=1,
                capacity_wait_budget_s=60,
                delivery_lease_ttl_s=15,
                delivery_lease_heartbeat_s=5,
                max_concurrency=1,
                consumer_name="worker/replacement",
            )
            runs = client._broker.lane().runs
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            initial = await runs.xreadgroup(
                cfg.consumer_group,
                "worker/peer",
                {cfg.stream: ">"},
                count=1,
            )
            entry, fields = initial[0][1][0]
            peer_cfg = cfg.model_copy(update={"consumer_name": "worker/peer"})
            leases = DeliveryLeaseStore(runs, peer_cfg)
            original_lease = await leases.acquire(
                cfg.stream,
                cfg.consumer_group,
                entry,
                consumer="worker/peer",
            )
            if mode in {"capacity-copy", "capacity-terminal-schedule", "capacity-reply-ref"}:
                if mode in {"capacity-terminal-schedule", "capacity-reply-ref"}:
                    from curie_worker.protected_recovery import ProtectedCapacityWaitStore

                    wait = ProtectedCapacityWaitStore(client._broker, client._manifest, peer_cfg)
                else:
                    wait = CapacityWaitStore(runs, peer_cfg)
                record = await wait.park(entry, fields, original.turn.event_id, original_lease)
                assert record.fields == dict(original.source_fields)
                await leases.release(
                    cfg.stream,
                    cfg.consumer_group,
                    entry,
                    owner=original_lease.owner,
                    resume_event_id=original_lease.resume_event_id,
                )
                wake_id = None
                if mode not in {"capacity-terminal-schedule", "capacity-reply-ref"}:
                    deadline = time.monotonic() + 12
                    while not await wait.wake_due(limit=1):
                        assert time.monotonic() < deadline
                        await asyncio.sleep(0.1)
                    wake_id = await runs.hget(wait._record(original.turn.event_id), "wake_entry")
                    assert wake_id and wake_id != entry
            engine, schema = await database(primary_postgres, client)
            runtime = task = None
            try:
                reply_endpoint = (
                    retained_ref_reply() if mode == "capacity-reply-ref" else external_reply()
                )
                async with reply_endpoint as (router, replies):
                    runtime = ProtectedRuntime(
                        broker=client._broker,
                        manifest=client._manifest,
                        engine=engine,
                        db_schema=schema,
                        execution=client._execution,
                        launch_client=client,
                        config=cfg,
                        reply_sink=router,
                    )
                    if mode == "capacity-reply-ref":
                        # Actual notice production is deliberately not activated. Invoke
                        # the real Kernel notice once solely to seed a previously retained
                        # provider ref through the actual owned transport notice lease.
                        notice_token = await wait.claim_notice(
                            original.turn.event_id, record.generation
                        )
                        assert notice_token is not None
                        ack = await runtime.kernel._kernel.notify_capacity_queued(original.turn)
                        assert ack.ref == "message/queued-example"
                        await wait.finish_notice(
                            original.turn.event_id,
                            record.generation,
                            notice_token,
                            delivered=True,
                            reply_ref=ack.ref,
                        )
                        assert original.turn.reply_handle.placeholder is None
                        assert original.turn.event_id not in runtime.kernel._kernel._minted_refs
                        original_turn = original.turn.model_dump(mode="json")
                        from curie_worker.kernel.core import Kernel
                        from curie_worker.protected_binding import current_protected_delivery

                        real_process = Kernel.process_event
                        immutable = []

                        async def observe_process(kernel, qevent, *args, **kwargs):
                            current = current_protected_delivery()
                            immutable.append(
                                (
                                    current is not None and qevent is current.turn,
                                    qevent.model_dump(mode="json") == original_turn,
                                )
                            )
                            return await real_process(kernel, qevent, *args, **kwargs)

                        monkeypatch.setattr(Kernel, "process_event", observe_process)
                    if mode == "capacity-copy":
                        wake_rows = await runs.xrange(cfg.stream, min=wake_id, max=wake_id)
                        assert len(wake_rows) == 1
                        assert await runtime.consumer._recovery_eligible(wake_id, wake_rows[0][1])
                    task = asyncio.create_task(runtime.run())
                    if mode in {
                        "capacity-copy",
                        "capacity-terminal-schedule",
                        "capacity-reply-ref",
                    }:
                        deadline = time.monotonic() + 15
                        while time.monotonic() < deadline:
                            if mode in {"capacity-terminal-schedule", "capacity-reply-ref"}:
                                wake_id = await runs.hget(
                                    runtime.consumer._waits._record(original.turn.event_id),
                                    "wake_entry",
                                )
                            owed = await runtime.markers.restore_owed(original.turn.event_id)
                            pending = await runs.xpending(cfg.stream, cfg.consumer_group)
                            if owed is not None and pending["pending"] == 0:
                                break
                            if task.done():
                                await task
                            await asyncio.sleep(0.05)
                        assert time.monotonic() < deadline, "actual capacity copy remained pending"
                        assert wake_id and wake_id != entry
                        assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
                        assert external.events.count(("RUNNER_POST", "/v1/turn-admit")) == 1
                        assert (
                            owed is not None
                            and owed.witness.as_dict()["delivery_entry_id"] == wake_id
                        )
                        assert owed.resolution is not None
                        assert owed.resolution.as_dict()["resolution"] == "emitted"
                        assert any(
                            "synthetic reply" in str(body.get("text", "")) for _, body in replies
                        )
                        if mode == "capacity-reply-ref":
                            assert immutable == [(True, True)]
                            posts = [
                                body for path, body in replies if path.endswith("/chat.postMessage")
                            ]
                            assert len(posts) == 1, (
                                "retained wait reply ref created a duplicate post"
                            )
                            updates = [
                                body for path, body in replies if path.endswith("/chat.update")
                            ]
                            final = [
                                body
                                for body in updates
                                if "synthetic reply" in str(body.get("text", ""))
                            ]
                            assert final and all(body["ts"] == ack.ref for body in final)
                            assert all(
                                body["channel"] == original.turn.reply_handle.channel
                                for body in posts + updates
                            )
                            current_wait = await wait.get(original.turn.event_id)
                            assert current_wait is not None and current_wait.reply_ref == ack.ref
                            assert current_wait.fields == dict(original.source_fields)
                        if mode == "capacity-terminal-schedule":
                            waits = runtime.consumer._waits
                            seconds, micros = await runs.time()
                            now = seconds * 1000 + micros // 1000
                            # Exercise the old-TTL/lost-wake contract with an actual wait
                            # record and immutable genuine witness, never a synthetic one.
                            await runs.delete(cfg.done_key(original.turn.event_id))
                            await runs.hset(
                                waits._record(original.turn.event_id),
                                mapping={
                                    "state": "active",
                                    "deadline_ms": str(now - 1),
                                },
                            )
                            await runs.zadd(waits._flight, {original.turn.event_id: now - 1})
                            before = await runs.xlen(cfg.stream)
                            assert await waits.reconcile_lost_wakes(limit=1) == 0
                            assert await runs.xlen(cfg.stream) == before
                            retained = await runtime.markers.restore_owed(original.turn.event_id)
                            assert retained is not None
                            assert retained.witness.canonical_bytes == owed.witness.canonical_bytes
                    else:
                        # Allow multiple reclaim cadences while the actual peer lease is live.
                        if mode == "sustained-live-peer":
                            deadline = time.monotonic() + 17
                            next_peer_heartbeat = time.monotonic()
                            while len(observations) < 3 and time.monotonic() < deadline:
                                if task.done():
                                    await task
                                if time.monotonic() >= next_peer_heartbeat:
                                    budget = await leases.heartbeat(
                                        cfg.stream,
                                        cfg.consumer_group,
                                        entry,
                                        consumer="worker/peer",
                                        owner=original_lease.owner,
                                        generation=original_lease.generation,
                                        resume_event_id=original_lease.resume_event_id,
                                    )
                                    assert budget is not None
                                    original_lease.budget = budget
                                    original_lease.local_deadline_monotonic = (
                                        budget.anchor_monotonic + leases.ownership_window_s
                                    )
                                    next_peer_heartbeat = time.monotonic() + 4
                                await asyncio.sleep(0.1)
                            assert len(observations) >= 3
                            assert all(record == (True, True, True) for record in observations)
                        else:
                            await asyncio.sleep(0.5)
                        assert not external.events and not replies
                        pending = await runs.xpending_range(
                            cfg.stream,
                            cfg.consumer_group,
                            min=entry,
                            max=entry,
                            count=1,
                        )
                        assert len(pending) == 1
                        assert pending[0]["consumer"] == "worker/peer"
                        assert pending[0]["times_delivered"] == 1
                        assert (
                            await runs.get(
                                cfg.delivery_lease_key(
                                    cfg.stream,
                                    cfg.consumer_group,
                                    entry,
                                )
                            )
                            == original_lease.owner
                        )
            finally:
                if runtime is not None:
                    runtime.request_stop()
                if task is not None:
                    await asyncio.wait_for(task, 10)
                if runtime is not None:
                    await runtime.close()
                from sqlalchemy import text

                async with engine.begin() as connection:
                    await connection.execute(text("DROP SCHEMA " + schema + " CASCADE"))
                await engine.dispose()
                if mode not in {
                    "capacity-copy",
                    "capacity-terminal-schedule",
                    "capacity-reply-ref",
                }:
                    await leases.release(
                        cfg.stream,
                        cfg.consumer_group,
                        entry,
                        owner=original_lease.owner,
                        resume_event_id=original_lease.resume_event_id,
                    )

    asyncio.run(go())


def test_actual_prestart_capacity_observation_and_foreign_epoch_refusal(owned_worker, tmp_path):
    """Observe actual mediator traffic; no replacement transport or status provider."""
    from aiohttp import TraceConfig
    from curie_worker.kernel.routing import _thread_key_for
    from curie_worker.protected_launch_client import LaunchClientError
    from curie_worker.protected_substrate import ProtectedSandboxSubstrate

    async def go():
        async with actual.actual_pair(owned_worker, tmp_path) as (
            _server,
            client,
            external,
            delivery,
            _lease,
            _request,
        ):
            substrate = ProtectedSandboxSubstrate(client)
            handle = await substrate.claim_async(
                _thread_key_for(delivery.turn), remaining_s=10, delivery=delivery
            )
            capability = substrate.capability_for(handle)
            requests = []
            trace = TraceConfig()

            async def observe_request(_session, _context, params):
                requests.append((params.method, params.url.path))

            trace.on_request_start.append(observe_request)
            trace.freeze()
            client._transport._session._trace_configs.append(trace)
            status = await client.capacity_status(capability, remaining_s=5)
            assert status["status"] == "idle-awaiting-input"
            assert status["capacity_admission"] is True
            assert status["turn_epoch"] is None
            assert client._target(capability).epoch is None
            assert len(requests) == 1 and requests[0][0] == "GET"
            assert requests[0][1].endswith("/status")
            assert not any(method == "RUNNER_POST" for method, _ in external.events)
            before = len(requests)
            for epoch in ("11111111-1111-4111-8111-111111111111", "malformed"):
                with pytest.raises(LaunchClientError):
                    await client.capacity_status(capability, epoch=epoch, remaining_s=5)
                assert len(requests) == before
            await substrate.retire_delivery(delivery)

    asyncio.run(go())


@pytest.mark.parametrize("fence", ["valid", "stale", "local-expired"])
def test_actual_terminal_prefix_reclaimed_without_another_model_start(
    owned_worker, tmp_path, primary_postgres, monkeypatch, fence
):
    """Stop the genuine settlement prefix before XACK; reclaim immutable completion."""

    from .protected_settlement.test_actual_source import (
        accepted_start,
        composition,
        reply_endpoint,
    )

    async def go():
        async with actual.actual_pair(owned_worker, tmp_path) as (
            _server,
            client,
            external,
            delivery,
            lease,
            _request,
        ):
            cfg, substrate, markers, adapter, _sink, runner, kernel = composition(
                client, owned_worker
            )
            try:
                async with reply_endpoint() as (route, _status):
                    await accepted_start(substrate, runner, external, delivery)
                    await kernel._complete(
                        delivery.turn, route, "delivered", telemetry_outcome="done", lease=lease
                    )
                original = await markers.restore_owed(delivery.turn.event_id)
                assert original is not None
                witness = original.witness.canonical_bytes
                runs = client._broker.lane().runs
                assert (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"] == 1
                await DeliveryLeaseStore(runs, cfg).release(
                    cfg.stream,
                    cfg.consumer_group,
                    lease.entry_id,
                    owner=lease.owner,
                    resume_event_id=lease.resume_event_id,
                )
                replacement = cfg.model_copy(
                    update={
                        "consumer_name": "worker/replacement",
                        "max_attempts": 1,
                        "dead_letter_stream": derive_lane_namespace(client._manifest).dead,
                    }
                )
                engine, schema = await database(primary_postgres, client)
                runtime = task = None
                try:
                    async with external_reply() as (router, _replies):
                        runtime = ProtectedRuntime(
                            broker=client._broker,
                            manifest=client._manifest,
                            engine=engine,
                            db_schema=schema,
                            execution=client._execution,
                            launch_client=client,
                            config=replacement,
                            reply_sink=router,
                        )
                        assert await runtime.consumer._recovery_eligible(
                            lease.entry_id, dict(delivery.source_fields)
                        )
                        injected = asyncio.Event()
                        if fence != "valid":
                            # Deterministic interleaving after real proof extraction; never
                            # substitute evidence, broker response, lease or the real Lua.
                            consumer = runtime.consumer
                            real_operation = consumer._ack_operation

                            def inject_interleaving(owed, current):
                                operation = real_operation(owed, current)
                                if fence == "stale":
                                    owned_worker.broker.command(
                                        "SET", operation[0][3], "successor-example", "PX", 15000
                                    )
                                else:
                                    current.local_deadline_monotonic = time.monotonic() - 1
                                injected.set()
                                return operation

                            monkeypatch.setattr(consumer, "_ack_operation", inject_interleaving)
                        task = asyncio.create_task(runtime.run())
                        deadline = time.monotonic() + 3
                        if fence == "valid":
                            while (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"]:
                                assert time.monotonic() < deadline, (
                                    "retained terminal prefix was not acknowledged"
                                )
                                if task.done():
                                    await task
                                await asyncio.sleep(0.05)
                        else:
                            await asyncio.wait_for(injected.wait(), 3)
                            await asyncio.sleep(0.1)
                            assert (await runs.xpending(cfg.stream, cfg.consumer_group))[
                                "pending"
                            ] == 1
                        retained = await runtime.markers.restore_owed(delivery.turn.event_id)
                        assert retained is not None and retained.witness.canonical_bytes == witness
                        assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
                finally:
                    if runtime is not None:
                        runtime.request_stop()
                    if task is not None:
                        await asyncio.wait_for(task, 10)
                    if runtime is not None:
                        await runtime.close()
                    from sqlalchemy import text

                    async with engine.begin() as connection:
                        await connection.execute(text("DROP SCHEMA " + schema + " CASCADE"))
                    await engine.dispose()
            finally:
                await adapter.aclose()

    asyncio.run(go())
