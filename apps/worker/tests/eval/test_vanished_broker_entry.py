"""Eval lane: a deleted stream entry graveyards through the shared heartbeat (#3349).

The default ``EvalStreamConsumer`` constructor omits ``leases``. These tests
pass a real ``DeliveryLeaseStore`` so the base heartbeat actually runs. No chat
notice is required; the eval graveyard row is the record.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, cast

from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.eval.stream import EvalStreamConsumer
from redis.asyncio import Redis as AsyncRedis


def _cfg(names: dict[str, str]) -> WorkerConfig:
    return WorkerConfig(
        valkey_host=VALKEY_HOST,
        valkey_port=VALKEY_PORT,
        valkey_password=VALKEY_PW,
        stream=names["stream"],
        consumer_group=names["group"],
        key_prefix=names["prefix"],
        eval_stream=f"{names['stream']}:evals",
        eval_consumer_group=f"ge-{names['group']}",
        eval_consumer_name="eval-owner",
        delivery_lease_heartbeat_s=0.2,
        delivery_lease_ttl_s=1.0,
        reclaim_interval_s=0.5,
    )


def _consumer(client: AsyncRedis, cfg: WorkerConfig) -> EvalStreamConsumer:
    # leases= is required. The default argument is None, and a leaseless
    # consumer never renews, so an XDEL would not reach the vanished path.
    return EvalStreamConsumer(
        redis=client,
        config=cfg,
        bundle_store=cast(Any, None),
        substrate=cast(Any, None),
        reporter=cast(Any, None),
        recorder=cast(Any, None),
        repo_lookup=object(),
        leases=DeliveryLeaseStore(client, cfg),
    )


async def _pending_entry(
    client: AsyncRedis, cfg: WorkerConfig, consumer: EvalStreamConsumer
) -> tuple[str, dict[str, str]]:
    await consumer.ensure_group()
    await client.xadd(cfg.eval_stream, {"payload": "minimal"})
    rows = await client.xreadgroup(
        cfg.eval_consumer_group,
        cfg.eval_consumer_name,
        {cfg.eval_stream: ">"},
        count=1,
    )
    assert rows, "expected an eval entry to read"
    entry_id, fields = rows[0][1][0]
    return entry_id, dict(fields)


async def _vanished_rows(
    client: AsyncRedis, cfg: WorkerConfig, entry_id: str
) -> list[dict[str, str]]:
    rows = await client.xrange(cfg.eval_dead_letter_stream_name())
    return [
        dict(fields)
        for _row_id, fields in rows
        if fields.get("dl_original_id") == entry_id
        and fields.get("dl_reason") == "broker-entry-vanished"
    ]


async def _cleanup(client: AsyncRedis, cfg: WorkerConfig) -> None:
    await client.delete(cfg.eval_stream, cfg.eval_dead_letter_stream_name())
    await client.aclose()


def test_eval_deleted_entry_graveyards_as_broker_entry_vanished(names: dict[str, str]) -> None:
    async def go() -> None:
        cfg = _cfg(names)
        client: AsyncRedis = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        consumer = _consumer(client, cfg)
        try:
            entry_id, fields = await _pending_entry(client, cfg, consumer)
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                await client.xdel(cfg.eval_stream, entry_id)
                deadline = time.monotonic() + 3.0
                rows: list[dict[str, str]] = []
                while time.monotonic() < deadline:
                    rows = await _vanished_rows(client, cfg, entry_id)
                    if rows:
                        break
                    await asyncio.sleep(0.05)
                assert len(rows) == 1
                assert rows[0]["dl_reason"] == "broker-entry-vanished"
                assert rows[0]["dl_original_id"] == entry_id
        finally:
            await _cleanup(client, cfg)

    asyncio.run(go())


def test_eval_pel_moved_to_another_consumer_is_not_vanished(names: dict[str, str]) -> None:
    async def go() -> None:
        cfg = _cfg(names)
        client: AsyncRedis = AsyncRedis(
            host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW, decode_responses=True
        )
        consumer = _consumer(client, cfg)
        other = "eval-other-owner"
        try:
            entry_id, fields = await _pending_entry(client, cfg, consumer)
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                # A heartbeat already inside its renewal can XCLAIM the row back.
                # Keep moving it until that renewal refuses and the loop stops.
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and not lease.lost.is_set():
                    await client.xclaim(
                        cfg.eval_stream, cfg.eval_consumer_group, other, 0, [entry_id]
                    )
                    await asyncio.sleep(0.05)
                assert lease.lost.is_set()
                await client.xclaim(cfg.eval_stream, cfg.eval_consumer_group, other, 0, [entry_id])
                await asyncio.sleep(cfg.delivery_lease_heartbeat_s + 0.05)
                assert await _vanished_rows(client, cfg, entry_id) == []
            pending = await client.xpending_range(
                cfg.eval_stream, cfg.eval_consumer_group, min="-", max="+", count=10
            )
            assert [row["message_id"] for row in pending] == [entry_id]
            assert pending[0]["consumer"] == other
        finally:
            await _cleanup(client, cfg)

    asyncio.run(go())
