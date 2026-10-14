"""Actual pinned TLS ownership fence. No model or runtime qualification."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import sys
import time
from pathlib import Path

import pytest
from curie_protected_hooks.admission_records import _encode
from curie_protected_hooks.worker_verification import Dispatchable
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.protected_binding import protected_delivery_scope
from curie_worker.protected_execution import (
    ExecutionOwnershipUnavailable,
    protected_execution_scope,
    require_current_execution,
)

# Explicit fresh fixture setup, never another API test's hidden state.
sys.path.insert(0, str(Path(__file__).parents[3]))
proof = importlib.import_module("packages.protected-hooks.tests.test_worker_verification")
pworker_service = proof.pworker_service
owned_worker = proof.owned_worker


def config(broker):
    m = broker.manifest().as_dict()
    g = hashlib.sha256(_encode([m["runtime_id"], m["runtime_generation"]])).hexdigest()
    return proof.wb.lane_config(
        broker,
        key_prefix=f"protected:lane:worker:{g}",
        consumer_group=f"protected-{g}",
        delivery_lease_ttl_s=15,
        delivery_lease_heartbeat_s=5,
    )


async def acquire(client, cfg):
    runs = client.lane().runs
    await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
    rows = await runs.xreadgroup(cfg.consumer_group, cfg.consumer_name, {cfg.stream: ">"}, count=1)
    entry_id = rows[0][1][0][0]
    store = DeliveryLeaseStore(runs, cfg)
    lease = await store.acquire(
        cfg.stream, cfg.consumer_group, entry_id, consumer=cfg.consumer_name
    )
    assert lease is not None
    return lease


def test_real_acquired_ownership_passes_and_scope_exit_refuses(owned_worker):
    async def go():
        _, fields, _ = proof.admitted(owned_worker.broker)
        manifest = owned_worker.broker.manifest()
        cfg = config(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            delivery = await proof.verification().verify_delivery(
                client, fields, trusted_manifest=manifest
            )
            assert type(delivery) is Dispatchable
            lease = await acquire(client, cfg)
            async with protected_delivery_scope(delivery):
                async with protected_execution_scope(
                    client, trusted_manifest=manifest, config=cfg, lease=lease
                ):
                    await require_current_execution(client, manifest)
                    async with protected_delivery_scope(None):
                        with pytest.raises(ExecutionOwnershipUnavailable):
                            await require_current_execution(client, manifest)
                    await require_current_execution(client, manifest)
                with pytest.raises(ExecutionOwnershipUnavailable):
                    await require_current_execution(client, manifest)
            assert not owned_worker.denials()

    asyncio.run(go())


def test_verified_copy_binds_current_row_and_retains_authority_after_trim(owned_worker):
    async def go():
        broker = owned_worker.broker
        _, fields, _ = proof.admitted(broker)
        manifest = broker.manifest()
        cfg = config(broker)
        broker.command("XTRIM", cfg.stream, "MAXLEN", "=", 0)
        broker.command("XADD", cfg.stream, "*", *sum(([k, v] for k, v in fields.items()), []))
        async with proof.connected(proof.verification(), owned_worker) as client:
            delivery = await proof.verification().verify_delivery(
                client, fields, trusted_manifest=manifest
            )
            assert type(delivery) is Dispatchable
            lease = await acquire(client, cfg)
            assert lease.entry_id != delivery.intent.as_dict()["reserved_stream_id"]
            async with protected_delivery_scope(delivery):
                async with protected_execution_scope(
                    client, trusted_manifest=manifest, config=cfg, lease=lease
                ):
                    await require_current_execution(client, manifest)
                    broker.command("XTRIM", cfg.stream, "MAXLEN", "=", 0)
                    await require_current_execution(client, manifest)
            assert not owned_worker.denials()

    asyncio.run(go())


@pytest.mark.parametrize(
    "mutation",
    [
        "token",
        "generation",
        "deadline",
        "pel-owner",
        "permanent-terminal",
        "local-expiry",
        "local-lost",
        "local-ack",
        "local-vanished",
        "lease-object-changed",
        "missing-ttl",
    ],
)
def test_real_broker_or_local_ownership_loss_refuses(owned_worker, mutation):
    async def go():
        broker = owned_worker.broker
        _, fields, _ = proof.admitted(broker)
        manifest = broker.manifest()
        cfg = config(broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            delivery = await proof.verification().verify_delivery(
                client, fields, trusted_manifest=manifest
            )
            assert type(delivery) is Dispatchable
            lease = await acquire(client, cfg)
            async with protected_delivery_scope(delivery):
                async with protected_execution_scope(
                    client, trusted_manifest=manifest, config=cfg, lease=lease
                ):
                    lease_key = cfg.delivery_lease_key(lease.stream, lease.group, lease.entry_id)
                    state_key = cfg.delivery_state_key(lease.stream, lease.group, lease.entry_id)
                    if mutation == "token":
                        broker.command("SET", lease_key, "different-owner", "PX", 15000)
                    elif mutation == "generation":
                        broker.command("HINCRBY", state_key, "gen", 1)
                    elif mutation == "deadline":
                        broker.command("HSET", state_key, "deadline_ms", "1")
                    elif mutation == "pel-owner":
                        # Provisioner corruption, not the product transfer algorithm.
                        broker.command(
                            "XCLAIM", lease.stream, lease.group, "other-consumer", 0, lease.entry_id
                        )
                    elif mutation == "permanent-terminal":
                        broker.command(
                            "SET",
                            f"{cfg.key_prefix}:settlement:{delivery.turn.event_id}",
                            "malformed-terminal-still-forbids-positive-work",
                        )
                    elif mutation == "local-expiry":
                        lease.local_deadline_monotonic = time.monotonic() - 1
                    elif mutation == "local-lost":
                        lease.lost.set()
                    elif mutation == "local-ack":
                        lease.acknowledged.set()
                    elif mutation == "local-vanished":
                        lease.entry_vanished.set()
                    elif mutation == "lease-object-changed":
                        lease.owner = "different-owner"
                    else:
                        broker.command("PERSIST", lease_key)
                    with pytest.raises(ExecutionOwnershipUnavailable) as caught:
                        await require_current_execution(client, manifest)
                    assert caught.value.__cause__ is None
                    assert caught.value.__context__ is None
                    assert lease.owner not in str(caught.value)
            assert not owned_worker.denials()

    asyncio.run(go())


def test_unconfirmed_constructed_lease_never_enters_scope(owned_worker):
    async def go():
        from curie_worker.delivery_lease import DeliveryLease

        _, fields, _ = proof.admitted(owned_worker.broker)
        manifest = owned_worker.broker.manifest()
        cfg = config(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            delivery = await proof.verification().verify_delivery(
                client, fields, trusted_manifest=manifest
            )
            lease = await acquire(client, cfg)
            copied = DeliveryLease(
                stream=lease.stream,
                group=lease.group,
                entry_id=lease.entry_id,
                owner=lease.owner,
                generation=lease.generation,
                budget=lease.budget,
            )
            async with protected_delivery_scope(delivery):
                with pytest.raises(ExecutionOwnershipUnavailable):
                    async with protected_execution_scope(
                        client, trusted_manifest=manifest, config=cfg, lease=copied
                    ):
                        pytest.fail("unconfirmed copied lease acquired authority")

    asyncio.run(go())


def test_inherited_task_must_refuse_after_scope_exit(owned_worker):
    async def go():
        _, fields, _ = proof.admitted(owned_worker.broker)
        manifest = owned_worker.broker.manifest()
        cfg = config(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            delivery = await proof.verification().verify_delivery(
                client, fields, trusted_manifest=manifest
            )
            lease = await acquire(client, cfg)
            gate = asyncio.Event()

            async def inherited():
                await gate.wait()
                with pytest.raises(ExecutionOwnershipUnavailable):
                    await require_current_execution(client, manifest)

            async with protected_delivery_scope(delivery):
                async with protected_execution_scope(
                    client, trusted_manifest=manifest, config=cfg, lease=lease
                ):
                    task = asyncio.create_task(inherited())
                gate.set()
                await task

    asyncio.run(go())


def test_other_pending_row_lease_must_not_authorize_delivery(owned_worker):
    async def go():
        broker = owned_worker.broker
        _, fields, _ = proof.admitted(broker)
        manifest = broker.manifest()
        cfg = config(broker)
        other_id = broker.command("XADD", cfg.stream, "*", "payload", "synthetic-unrelated-entry")
        if isinstance(other_id, bytes):
            other_id = other_id.decode()
        async with proof.connected(proof.verification(), owned_worker) as client:
            delivery = await proof.verification().verify_delivery(
                client, fields, trusted_manifest=manifest
            )
            runs = client.lane().runs
            await runs.xgroup_create(cfg.stream, cfg.consumer_group, id="0")
            await runs.xreadgroup(cfg.consumer_group, cfg.consumer_name, {cfg.stream: ">"}, count=2)
            lease = await DeliveryLeaseStore(runs, cfg).acquire(
                cfg.stream, cfg.consumer_group, other_id, consumer=cfg.consumer_name
            )
            assert lease is not None
            assert other_id != delivery.intent.as_dict()["reserved_stream_id"]
            async with protected_delivery_scope(delivery):
                with pytest.raises(ExecutionOwnershipUnavailable):
                    async with protected_execution_scope(
                        client, trusted_manifest=manifest, config=cfg, lease=lease
                    ):
                        await require_current_execution(client, manifest)

    asyncio.run(go())
