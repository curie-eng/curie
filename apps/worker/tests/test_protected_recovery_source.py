"""Actual wait copies; only external Kubernetes/runner endpoints are synthetic."""

import asyncio
import time
from dataclasses import replace
from unittest.mock import patch

import pytest
from curie_protected_hooks.admission_records import DeliveryIdentity, delivery_digest
from curie_protected_hooks.execution_namespace import derive_lane_namespace
from curie_worker.capacity_wait import WAIT_GENERATION_FIELD, CapacityWaitStore
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.protected_execution import (
    ExecutionOwnershipUnavailable,
    protected_execution_scope,
    require_current_execution,
)
from curie_worker.protected_recovery import (
    ProtectedRecoveryAdapter,
    RecoveryUnavailable,
    recovery_delivery_scope,
    recovery_gate_inputs,
)
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from .test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


@pytest.mark.parametrize("expired", [False, True])
def test_actual_capacity_park_wake_validates_exact_record_row_and_original_bytes(
    owned_worker,
    tmp_path,
    expired,
):
    async def go():
        async with actual_pair(owned_worker, tmp_path, intake=True) as (
            server,
            client,
            external,
            original,
            unused,
            request,
        ):
            ns = derive_lane_namespace(client._manifest)
            cfg = wb.lane_config(
                owned_worker.broker,
                key_prefix=ns.prefix,
                consumer_group=ns.group,
                dead_letter_stream=ns.dead,
                max_attempts=1,
                capacity_wait_budget_s=1 if expired else 60,
                delivery_lease_ttl_s=15,
                delivery_lease_heartbeat_s=5,
            )
            runs = client._broker.lane().runs
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            rows = await runs.xreadgroup(
                cfg.consumer_group, cfg.consumer_name, {cfg.stream: ">"}, count=1
            )
            entry, fields = rows[0][1][0]
            lease_store = DeliveryLeaseStore(runs, cfg)
            lease = await lease_store.acquire(
                cfg.stream, cfg.consumer_group, entry, consumer=cfg.consumer_name
            )
            wait = CapacityWaitStore(runs, cfg)
            record = await wait.park(entry, fields, original.turn.event_id, lease)
            assert record.fields == dict(original.source_fields)
            stop = time.monotonic() + 12
            span = NonRecordingSpan(SpanContext(1, 2, False, TraceFlags(1)))
            with trace.use_span(span):
                while not await wait.wake_due(limit=1):
                    assert time.monotonic() < stop, "actual capacity wake missed fixed due time"
                    await asyncio.sleep(0.1)
            rows = await runs.xreadgroup(
                cfg.consumer_group, cfg.consumer_name, {cfg.stream: ">"}, count=1
            )
            wake_entry, wake_fields = rows[0][1][0]
            adapter = ProtectedRecoveryAdapter(client._broker, client._manifest, cfg)
            copy = await adapter.verify(wake_entry, wake_fields)
            assert copy is not None
            assert copy.delivery.source_fields == original.source_fields
            assert copy.metadata.generation == int(wake_fields[WAIT_GENERATION_FIELD]) == 2
            assert not copy.metadata.terminal_only
            assert copy.expired is expired
            # Wake injection replaces diagnostic tracing; original bytes remain authoritative.
            assert "traceparent" not in record.fields and "traceparent" in wake_fields
            assert copy.metadata.traceparent == wake_fields["traceparent"]
            assert await adapter.verify(entry, wake_fields) is None
            assert (
                await adapter.verify(wake_entry, {**wake_fields, WAIT_GENERATION_FIELD: "1"})
                is None
            )
            assert await adapter.verify(wake_entry, {**wake_fields, "unknown": "extra"}) is None
            assert not external.events
            wake_lease = await lease_store.acquire(
                cfg.stream,
                cfg.consumer_group,
                wake_entry,
                consumer=cfg.consumer_name,
            )
            with pytest.raises(RecoveryUnavailable):
                async with recovery_delivery_scope(replace(copy)):
                    pytest.fail("copied DTO installed execution context")
            async with recovery_delivery_scope(copy):
                if expired:
                    with pytest.raises(RecoveryUnavailable):
                        await recovery_gate_inputs(client._broker, client._manifest, wake_lease)
                else:
                    gate = await recovery_gate_inputs(
                        client._broker,
                        client._manifest,
                        wake_lease,
                    )
                    assert gate is not None
                    assert dict(gate.entry_fields) == wake_fields
                    assert dict(gate.wait_snapshot)["wake_entry"] == wake_entry
                    assert dict(gate.wait_snapshot)["generation"] == "2"
                    async with protected_execution_scope(
                        client._broker,
                        trusted_manifest=client._manifest,
                        config=cfg,
                        lease=wake_lease,
                    ):
                        await require_current_execution(client._broker, client._manifest)
                        injected = False

                        async def race_snapshot(*args):
                            nonlocal injected
                            issued = await recovery_gate_inputs(*args)
                            assert issued is not None
                            # Fault hook: genuine extraction, actual authorized write, old gate.
                            await runs.hset(issued.wait_key, "reply_ref", "changed-reference")
                            injected = True
                            return issued

                        eval_before = owned_worker.broker.command("INFO", "commandstats")[
                            "cmdstat_eval"
                        ]["calls"]
                        with patch(
                            "curie_worker.protected_execution.recovery_gate_inputs",
                            new=race_snapshot,
                        ):
                            with pytest.raises(ExecutionOwnershipUnavailable):
                                await require_current_execution(client._broker, client._manifest)
                        eval_after = owned_worker.broker.command("INFO", "commandstats")[
                            "cmdstat_eval"
                        ]["calls"]
                        assert injected and eval_after - eval_before == 2
                        # Same scope, real helper restored: changed current snapshot still refuses.
                        with pytest.raises(ExecutionOwnershipUnavailable):
                            await require_current_execution(client._broker, client._manifest)
                    # Invalid authoritative current wait bytes cannot be reused from the DTO.
                    with pytest.raises(RecoveryUnavailable):
                        await recovery_gate_inputs(client._broker, client._manifest, wake_lease)
            assert not external.events
            assert not await runs.exists(cfg.key_prefix + ":settlement:" + original.turn.event_id)
            digest = delivery_digest(DeliveryIdentity(**original.intent.as_dict()["identity"]))
            assert (
                owned_worker.broker.command(
                    "ZSCORE",
                    "protected:admission:quota",
                    digest,
                )
                is not None
            )
            assert await runs.xpending(cfg.stream, cfg.consumer_group) == {
                "pending": 1,
                "min": wake_entry,
                "max": wake_entry,
                "consumers": [{"name": cfg.consumer_name, "pending": 1}],
            }
            await lease_store.release(
                cfg.stream,
                cfg.consumer_group,
                entry,
                owner=lease.owner,
                resume_event_id=lease.resume_event_id,
            )
            await lease_store.release(
                cfg.stream,
                cfg.consumer_group,
                wake_entry,
                owner=wake_lease.owner,
                resume_event_id=wake_lease.resume_event_id,
            )

    asyncio.run(go())


def test_actual_graveyard_observer_preserves_proof_without_group_ack_or_refund(
    owned_worker, tmp_path
):
    from curie_protected_hooks.worker_verification import RetainedProof
    from curie_worker.protected_recovery import ProtectedGraveyardObserver

    async def go():
        async with actual_pair(owned_worker, tmp_path, intake=True) as (
            server,
            client,
            external,
            original,
            unused,
            request,
        ):
            ns = derive_lane_namespace(client._manifest)
            runs = client._broker.lane().runs
            assert await runs.xinfo_groups(ns.stream) == []
            fields = dict(original.source_fields)
            fields.update(
                {
                    "dl_original_id": original.intent.as_dict()["reserved_stream_id"],
                    "dl_delivery_count": "5",
                    "dl_reason": "max-delivery-exceeded",
                    "dl_dead_lettered_at": "2026-10-01T00:00:00Z",
                }
            )
            await runs.xadd(ns.dead, fields, maxlen=64, approximate=True)
            observer = ProtectedGraveyardObserver(client._broker, client._manifest)
            seen = await observer.observe(limit=1)
            assert len(seen) == 1 and type(seen[0].proof) is RetainedProof
            assert seen[0].diagnostic.source_fields == original.source_fields
            assert seen[0].proof.envelope.canonical_bytes == original.envelope.canonical_bytes
            assert await runs.xinfo_groups(ns.stream) == []
            assert not external.events
            digest = delivery_digest(DeliveryIdentity(**original.intent.as_dict()["identity"]))
            assert (
                owned_worker.broker.command(
                    "ZSCORE",
                    "protected:admission:quota",
                    digest,
                )
                is not None
            )
            assert not await runs.exists(ns.prefix + ":settlement:" + original.turn.event_id)

    asyncio.run(go())


@pytest.mark.parametrize("interleaving", ["wait", "original-proof", "unknown-permanent"])
def test_actual_schedule_gate_refuses_changed_snapshot_or_unknown_permanent(
    owned_worker, tmp_path, monkeypatch, interleaving
):
    """Real extraction and Lua; only owned deterministic fault inputs are changed."""
    from curie_worker.protected_recovery import ProtectedCapacityWaitStore

    async def go():
        async with actual_pair(owned_worker, tmp_path, intake=True) as (
            _server,
            client,
            external,
            delivery,
            _unused,
            _request,
        ):
            ns = derive_lane_namespace(client._manifest)
            cfg = wb.lane_config(
                owned_worker.broker,
                key_prefix=ns.prefix,
                consumer_group=ns.group,
                dead_letter_stream=ns.dead,
                delivery_lease_ttl_s=15,
                delivery_lease_heartbeat_s=5,
            )
            runs = client._broker.lane().runs
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            rows = await runs.xreadgroup(
                cfg.consumer_group, cfg.consumer_name, {cfg.stream: ">"}, count=1
            )
            entry, fields = rows[0][1][0]
            leases = DeliveryLeaseStore(runs, cfg)
            lease = await leases.acquire(
                cfg.stream, cfg.consumer_group, entry, consumer=cfg.consumer_name
            )
            waits = ProtectedCapacityWaitStore(client._broker, client._manifest, cfg)
            try:
                # The authoritative transport creates the exact wait and original park ACK.
                await waits.park(entry, fields, delivery.turn.event_id, lease)
                seconds, micros = await runs.time()
                await runs.zadd(
                    waits._due, {delivery.turn.event_id: seconds * 1000 + micros // 1000 - 1}
                )
                real_inputs = waits._scheduling_inputs
                extracted = []

                async def inject(event_id):
                    gate = await real_inputs(event_id)
                    assert gate is not None
                    extracted.append(True)
                    if interleaving == "wait":
                        await runs.hset(
                            waits._record(event_id), "reply_ref", "message/changed-example"
                        )
                    elif interleaving == "original-proof":
                        key = next(
                            key for key in gate[0] if key.startswith("protected:admission:binding:")
                        )
                        owned_worker.broker.command("SET", key, "SYNTHETIC-CHANGED-PROOF")
                    else:
                        await runs.set(
                            ns.prefix + ":settlement:" + event_id, "SYNTHETIC-UNKNOWN-PERMANENT"
                        )
                    return gate

                monkeypatch.setattr(waits, "_scheduling_inputs", inject)
                before = await runs.xlen(cfg.stream)
                eval_before = wb.command_count(owned_worker.broker, "eval")
                assert await waits.wake_due(limit=1) == 0
                assert wb.command_count(owned_worker.broker, "eval") == eval_before + 1
                assert extracted == [True]
                assert await runs.xlen(cfg.stream) == before
                assert not external.events
                identity = DeliveryIdentity(**delivery.intent.as_dict()["identity"])
                assert (
                    owned_worker.broker.command(
                        "ZSCORE", "protected:admission:quota", delivery_digest(identity)
                    )
                    is not None
                )
                assert (await runs.xpending(cfg.stream, cfg.consumer_group))["pending"] == 0
                assert await runs.hget(waits._record(delivery.turn.event_id), "generation") == "1"
                assert await runs.hget(waits._record(delivery.turn.event_id), "state") == "waiting"
            finally:
                await leases.release(
                    cfg.stream,
                    cfg.consumer_group,
                    entry,
                    owner=lease.owner,
                    resume_event_id=lease.resume_event_id,
                )

    asyncio.run(go())
