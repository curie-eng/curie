"""Reference retention against real Valkey, including rolling writers."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from curie_worker.approval_cards import DEFAULT_CARD_TTL_S, ApprovalCardStore
from curie_worker.approvals import ApprovalClient, SettledApproval
from curie_worker.config import WorkerConfig
from redis.asyncio import Redis


@asynccontextmanager
async def store_for(sync_redis: Any, names: dict[str, str], *, enabled: bool = True):
    kwargs = sync_redis.connection_pool.connection_kwargs
    redis = Redis(**{key: kwargs[key] for key in ("host", "port", "password", "db")})
    config = WorkerConfig(key_prefix=names["prefix"], approval_pending_retention_enabled=enabled)
    try:
        yield ApprovalCardStore(redis, config), redis, config
    finally:
        await redis.aclose()


async def remember(store: ApprovalCardStore, approval_id: str = "acme-approval") -> None:
    await store.remember(
        approval_id,
        channel="C0EXAMPLE1",
        ts="123.1",
        summary="Approve work",
        endpoint=None,
        kind="slack",
    )
    await store.remember_notice_ref(approval_id, "123.0")
    await store.remember_reply_below_card(approval_id)


def keys(config: WorkerConfig, approval_id: str = "acme-approval") -> list[str]:
    return [
        config.approval_card_key(approval_id),
        config.approval_notice_ref_key(approval_id),
        config.approval_reply_below_card_key(approval_id),
    ]


def test_pending_references_survive_restart_and_terminal_window_is_fresh(sync_redis, names):
    async def run():
        async with store_for(sync_redis, names) as (store, redis, config):
            await remember(store)
            for key in keys(config):
                assert await redis.pttl(key) == -1
            restarted = ApprovalCardStore(redis, config)
            assert await restarted.read("acme-approval") is not None
            await restarted.settle("acme-approval")
            for key in keys(config):
                assert (
                    DEFAULT_CARD_TTL_S * 1000 - 5000
                    < await redis.pttl(key)
                    <= DEFAULT_CARD_TTL_S * 1000
                )
            before = [await redis.pttl(key) for key in keys(config)]
            await restarted.settle("acme-approval")
            assert all(
                [
                    await redis.pttl(key) <= ttl
                    for key, ttl in zip(keys(config), before, strict=True)
                ]
            )
            await restarted.remember_notice_ref("acme-approval", "late-notice")
            assert 0 < await redis.pttl(keys(config)[1]) <= before[1]

    asyncio.run(run())


def test_flag_off_writers_keep_legacy_ttl_without_expiring_persistent_pending_refs(
    sync_redis, names
):
    async def run():
        async with store_for(sync_redis, names, enabled=False) as (old, redis, config):
            await remember(old)
            assert all([await redis.ttl(key) > 0 for key in keys(config)])
            new = ApprovalCardStore(
                redis, config.model_copy(update={"approval_pending_retention_enabled": True})
            )
            await remember(new)
            await remember(old)
            assert all([await redis.pttl(key) == -1 for key in keys(config)])

    asyncio.run(run())


class Reader:
    def __init__(self, status="pending", decided=None):
        self.status, self.decided = status, decided

    async def get(self, approval_id):
        return (
            None if self.status is None else SettledApproval(self.status, None, None, self.decided)
        )


def test_post_drain_reconciliation_indexes_old_writes_and_bounds_every_terminal_purpose(
    sync_redis, names
):
    async def run():
        async with store_for(sync_redis, names) as (store, redis, config):
            for key in keys(config):
                await redis.set(key, "old-ref", ex=100)
            await store.reconcile_after_drain(Reader(), writers_drained=True)
            assert all([await redis.pttl(key) == -1 for key in keys(config)])
            await redis.set(config.approval_notice_ref_key("late-old"), "old-notice", ex=100)
            await store.reconcile_after_drain(Reader(), writers_drained=True)
            assert await redis.pttl(config.approval_notice_ref_key("late-old")) == -1
            await store.reconcile_pending(Reader("rejected", datetime.now(UTC)))
            assert all([0 < await redis.ttl(key) <= DEFAULT_CARD_TTL_S for key in keys(config)])
            assert await redis.scard(config.approval_references_pending_key()) == 0

    asyncio.run(run())


def test_terminal_reconciliation_does_not_resurrect_expired_window(sync_redis, names):
    async def run():
        async with store_for(sync_redis, names) as (store, redis, config):
            await remember(store)
            await store.reconcile_pending(
                Reader("approved", datetime.now(UTC) - timedelta(days=15))
            )
            assert not any([await redis.exists(key) for key in keys(config)])

    asyncio.run(run())


def test_default_off_cleanup_runs_and_unavailable_read_preserves_index(sync_redis, names):
    async def run():
        async with store_for(sync_redis, names, enabled=False) as (store, redis, config):
            await remember(store)
            await store.reconcile_pending(Reader(None))
            assert await redis.scard(config.approval_references_pending_key()) == 1
            await store.reconcile_pending(Reader("rejected", datetime.now(UTC)))
            assert await redis.scard(config.approval_references_pending_key()) == 0

    asyncio.run(run())


@pytest.mark.parametrize(
    "status,detail,deleted",
    [(404, "approval not found", True), (404, "Not Found", False), (503, "unavailable", False)],
)
def test_only_definitive_missing_approval_bounds_persistent_refs(
    sync_redis, names, status, detail, deleted
):
    async def run():
        async with store_for(sync_redis, names) as (store, redis, config):
            await remember(store)
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(status, json={"detail": detail})
                )
            ) as http:
                reader = ApprovalClient(
                    api_base_url="http://api.example.com",
                    api_key="test-key",
                    client=http,
                    read_timeout_s=1,
                )
                await store.reconcile_pending(reader)
            assert (await redis.pttl(keys(config)[0]) > 0) is deleted
            assert await redis.scard(config.approval_references_pending_key()) == (
                0 if deleted else 1
            )

    asyncio.run(run())


def test_publication_pop_restore_keeps_the_original_terminal_window(sync_redis, names):
    async def run():
        async with store_for(sync_redis, names) as (store, redis, config):
            await remember(store)
            ref = await store.pop("acme-approval")
            assert ref is not None
            end = await redis.get(config.approval_retention_key("acme-approval"))
            assert end is not None
            await store.restore("acme-approval", ref)
            assert await store.read("acme-approval") is not None
            assert await redis.get(config.approval_retention_key("acme-approval")) == end
            assert 0 < await redis.pttl(keys(config)[0]) <= DEFAULT_CARD_TTL_S * 1000
            await redis.delete(*keys(config), config.approval_retention_key("acme-approval"))
            await store.restore("acme-approval", ref)
            assert await store.read("acme-approval") is None

    asyncio.run(run())


def test_pending_snapshot_cannot_remove_a_concurrent_terminal_retention_window(sync_redis, names):
    async def run():
        async with store_for(sync_redis, names) as (store, redis, config):
            await remember(store)

            class RacingReader(Reader):
                async def get(self, approval_id):
                    pending = await super().get(approval_id)
                    await store.settle(approval_id)
                    return pending

            await store.reconcile_pending(RacingReader())
            assert all([0 < await redis.ttl(key) <= DEFAULT_CARD_TTL_S for key in keys(config)])
            assert await redis.scard(config.approval_references_pending_key()) == 0

    asyncio.run(run())
