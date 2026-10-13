"""Actual admission/lease/mediator/retirement/settlement/egress composition.

External Kubernetes, runner and reply HTTP alone are simulated. These bounded
source tests establish no runtime qualification or complete Kernel campaign.
"""

from __future__ import annotations

import asyncio
import contextvars
from contextlib import asynccontextmanager

import pytest
from aiohttp import web
from channel_protocol.reply import REPLY_WIRE_VERSION, TurnCompleted
from curie_protected_hooks.admission_records import DeliveryIdentity, delivery_digest
from curie_protected_hooks.atomic_admission import AtomicAdmission
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_protected_hooks.worker_transport import AuthenticatedWorkerClient
from curie_protected_hooks.worker_verification import restore_committed_event
from curie_worker.kernel.core import Kernel
from curie_worker.kernel.routing import _thread_key_for
from curie_worker.markers import CompletionRecord
from curie_worker.protected_completion import ProtectedReplySink, sweep_protected_completions
from curie_worker.protected_markers import ProtectedMarkers
from curie_worker.protected_runner import ProtectedRunnerClient
from curie_worker.protected_substrate import ProtectedSandboxSubstrate
from curie_worker.reply_sink import ADAPTER_SECRET_HEADER, HttpReplyAdapter, TargetRoute

from ..test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


@asynccontextmanager
async def reply_endpoint():
    status = {"code": 503, "attempts": 0, "confirmed": 0}

    async def external(request):
        assert request.headers[ADAPTER_SECRET_HEADER] == "synthetic-reply-secret"
        body = await request.json()
        assert body["event"] == "turn.completed"
        status["attempts"] += 1
        if status["code"] == 200:
            status["confirmed"] += 1
        return web.json_response({}, status=status["code"])

    app = web.Application()
    app.router.add_post("/reply", external)
    owner = web.AppRunner(app, access_log=None)
    await owner.setup()
    try:
        site = web.TCPSite(owner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        yield TargetRoute(endpoint=f"http://127.0.0.1:{port}/reply", adapter="fixture"), status
    finally:
        await owner.cleanup()


def composition(client, owned):
    namespace = derive_lane_namespace(client._manifest)
    cfg = wb.lane_config(
        owned.broker,
        key_prefix=namespace.prefix,
        consumer_group=namespace.group,
        delivery_lease_ttl_s=15,
        delivery_lease_heartbeat_s=5,
    )
    substrate = ProtectedSandboxSubstrate(client)
    markers = ProtectedMarkers(
        client._broker.lane().runs,
        cfg,
        broker=client._broker,
        manifest=client._manifest,
        substrate=substrate,
        launch_client=client,
    )
    adapter = HttpReplyAdapter({"fixture": "synthetic-reply-secret"}, timeout_s=2)
    sink = ProtectedReplySink(markers, adapter)
    runner = ProtectedRunnerClient(client, substrate)
    kernel = Kernel(
        substrate=substrate,
        runner=runner,
        sink=sink,
        lock=None,
        pressure_lock=None,
        markers=markers,
        config=cfg,
    )
    return cfg, substrate, markers, adapter, sink, runner, kernel


async def accepted_start(substrate, runner, external, delivery):
    handle = await substrate.claim_async(
        _thread_key_for(delivery.turn), remaining_s=10, delivery=delivery
    )
    external.finish.set()
    stream = await runner.start_turn(handle, Kernel._to_event(delivery.turn), remaining_s=5)
    async with stream:
        frames = [frame async for frame in stream]
    assert frames
    assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
    return handle


def admission_owner(owned, client, delivery):
    identity = DeliveryIdentity(**delivery.intent.as_dict()["identity"])
    owner = AtomicAdmission(
        owned.broker.admin,
        trusted_manifest=client._manifest,
        trusted_max_readiness_ms=60000,
        backlog_limit=64,
    )
    return owner, identity


async def cold_sweep(owned, client, cfg, substrate, route, status, *, generation):
    # A fresh authenticated reader and task with no handler ContextVars.
    from curie_worker import protected_execution
    from curie_worker.protected_binding import _CURRENT_DELIVERY

    assert protected_execution._CURRENT.get() is None
    assert _CURRENT_DELIVERY.get() is None
    broker = await AuthenticatedWorkerClient.connect(
        client._manifest, owned.credential(), owned.broker.ca_pem
    )
    adapter = HttpReplyAdapter({"fixture": "synthetic-reply-secret"}, timeout_s=2)
    try:
        markers = ProtectedMarkers(
            broker.lane().runs,
            cfg,
            broker=broker,
            manifest=client._manifest,
            substrate=substrate,
            launch_client=client,
        )
        sink = ProtectedReplySink(markers, adapter)
        event_id = next(iter(client._owned.values())).delivery.turn.event_id
        owed = await markers.repair_completion(event_id)
        assert owed is not None and owed.record.route == route
        assert owed.witness.as_dict()["completion_generation"] == generation
        status["code"] = 200
        assert await sweep_protected_completions(markers, sink, wall_budget_s=5) == 1
        assert await markers.is_terminal(event_id)
        assert await markers._raw(markers.context_key(event_id, generation)) is None
        assert not await broker.lane().runs.exists(cfg.completion_key(event_id))
        resolution = await markers._raw(markers.resolution_key(event_id, generation))
        assert resolution is not None and b'"resolution":"emitted"' in resolution
        assert (
            await restore_committed_event(broker, event_id, trusted_manifest=client._manifest)
            is not None
        )
    finally:
        await adapter.aclose()
        await broker.close()


def test_actual_kernel_settlement_retains_owed_refunds_before_emit_and_cold_confirms(
    owned_worker, tmp_path
):
    async def go():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            _,
        ):
            cfg, substrate, markers, adapter, sink, runner, kernel = composition(
                client, owned_worker
            )
            try:
                async with reply_endpoint() as (route, status):
                    owner, identity = admission_owner(owned_worker, client, delivery)
                    assert owner.release_accepted(identity) == "retained"
                    handle = await accepted_start(substrate, runner, external, delivery)
                    await kernel._complete(
                        delivery.turn,
                        route,
                        "delivered",
                        telemetry_outcome="done",
                        lease=lease,
                    )
                    owed = await markers.restore_owed(delivery.turn.event_id)
                    assert owed is not None and owed.context is not None
                    generation = owed.witness.as_dict()["completion_generation"]
                    assert (
                        server._registry.get("worker/example", handle.claim_name).state == "retired"
                    )
                    assert status == {"code": 503, "attempts": 1, "confirmed": 0}
                    assert not await markers.clear_completion(
                        delivery.turn.event_id, generation=generation
                    )
                    assert owner.release_accepted(identity) == "released"
                    assert (
                        owned_worker.broker.command(
                            "ZSCORE", "protected:admission:quota", delivery_digest(identity)
                        )
                        is None
                    )
                    await client._broker.lane().runs.delete(cfg.done_key(delivery.turn.event_id))
                    await asyncio.create_task(
                        cold_sweep(
                            owned_worker,
                            client,
                            cfg,
                            substrate,
                            route,
                            status,
                            generation=generation,
                        ),
                        context=contextvars.Context(),
                    )
                    assert status == {"code": 200, "attempts": 2, "confirmed": 1}
                    assert owner.release_accepted(identity) == "already_released"
                    assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
            finally:
                await adapter.aclose()

    asyncio.run(go())


@pytest.mark.parametrize("failure", ["lost", "stale"])
def test_actual_issued_retirement_never_overrides_lost_or_stale_delivery_lease(
    owned_worker, tmp_path, failure
):
    async def go():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            _,
        ):
            cfg, substrate, markers, adapter, sink, runner, kernel = composition(
                client, owned_worker
            )
            try:
                async with reply_endpoint() as (route, status):
                    handle = await accepted_start(substrate, runner, external, delivery)
                    proof = await substrate.retire_delivery(delivery)
                    assert client.is_owned_retirement(proof, delivery=delivery)
                    if failure == "lost":
                        lease.lost.set()
                    else:
                        owned_worker.broker.command(
                            "SET",
                            cfg.delivery_lease_key(lease.stream, lease.group, lease.entry_id),
                            "successor-example",
                            "PX",
                            15000,
                        )
                    await kernel._complete(
                        delivery.turn,
                        route,
                        "delivered",
                        telemetry_outcome="delivered",
                        lease=lease,
                    )
                    assert lease.lost.is_set()
                    assert (
                        await markers._raw(markers.settlement_key(delivery.turn.event_id)) is None
                    )
                    assert not await client._broker.lane().runs.exists(
                        cfg.completion_key(delivery.turn.event_id)
                    )
                    assert status["attempts"] == 0
                    owner, identity = admission_owner(owned_worker, client, delivery)
                    assert owner.release_accepted(identity) == "retained"
                    assert (
                        server._registry.get("worker/example", handle.claim_name).state == "retired"
                    )
            finally:
                await adapter.aclose()

    asyncio.run(go())


def test_actual_post_write_lua_response_error_cold_repairs_same_generation(
    owned_worker, tmp_path, monkeypatch
):
    """Actual committed response error, not dropped TLS response/process-kill qualification."""
    from curie_worker import protected_markers as product

    async def go():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            lease,
            _,
        ):
            cfg, substrate, markers, adapter, sink, runner, kernel = composition(
                client, owned_worker
            )
            try:
                async with reply_endpoint() as (route, status):
                    handle = await accepted_start(substrate, runner, external, delivery)
                    event = TurnCompleted(
                        version=REPLY_WIRE_VERSION,
                        event="turn.completed",
                        target=kernel._target_for(delivery.turn),
                        event_id=delivery.turn.event_id,
                        outcome="delivered",
                    )
                    record = CompletionRecord(
                        event_id=delivery.turn.event_id, event=event, route=route, created_at=0
                    )
                    # Fault only the actual Lua's returned response, after all durable writes.
                    # Valkey, TLS client, source proof, issuer and registry remain real.
                    with monkeypatch.context() as fault:
                        fault.setattr(
                            product,
                            "_SETTLE",
                            product._SETTLE.replace(
                                "return 1\n",
                                "return redis.error_reply('injected post-write response error')\n",
                            ),
                        )
                        assert (
                            await markers.settle_fenced(
                                delivery.turn.event_id,
                                record,
                                stream=lease.stream,
                                group=lease.group,
                                entry_id=lease.entry_id,
                                owner=lease.owner,
                                generation=lease.generation,
                                marker_value="1",
                            )
                            is None
                        )
                    owed = await markers.restore_owed(delivery.turn.event_id)
                    assert owed is not None and owed.record.created_at == 0
                    generation = owed.witness.as_dict()["completion_generation"]
                    assert (
                        server._registry.get("worker/example", handle.claim_name).state == "retired"
                    )
                    assert status["attempts"] == 0
                    # Lose volatile done/outbox state; durable witness/context reconstructs it.
                    await client._broker.lane().runs.delete(
                        cfg.done_key(delivery.turn.event_id),
                        cfg.completion_key(delivery.turn.event_id),
                    )
                    await asyncio.create_task(
                        cold_sweep(
                            owned_worker,
                            client,
                            cfg,
                            substrate,
                            route,
                            status,
                            generation=generation,
                        ),
                        context=contextvars.Context(),
                    )
                    assert status == {"code": 200, "attempts": 1, "confirmed": 1}
                    assert external.events.count(("RUNNER_POST", "/v1/event")) == 1
            finally:
                await adapter.aclose()

    asyncio.run(go())
