"""Mechanical Lua crash/fence tests with real Valkey, not runtime qualification.

An explicitly handed-off isolated endpoint is required. Synthetic proof strings
exercise transactional mechanics only; source composition must additionally use
actual authenticated broker, admission owner, mediator, registry and reply sink.
"""

import asyncio
import os

import pytest
from curie_worker.protected_markers import _CLEAR, _GRAVEYARD, _OUTBOX, _REPAIR, _SETTLE
from redis.asyncio import Redis
from redis.exceptions import ResponseError


@pytest.fixture
def endpoint():
    # This fixture does not own/start/flush the service. Each test deletes its own keys.
    value = os.environ.get("CURIE_SETTLEMENT_TEST_REDIS_URL")
    if not value:
        pytest.fail("explicit isolated Valkey resource handoff required")
    return value


async def setup(redis):
    import uuid

    prefix = "settlement-mechanics-" + uuid.uuid4().hex
    keys = [
        prefix + ":" + part
        for part in (
            "witness",
            "context",
            "resolution",
            "repair",
            "outbox",
            "pending",
            "done",
            "lease",
            "delivery",
            "stream",
            "original",
        )
    ]
    run_id = (await redis.info("server"))["run_id"]
    await redis.xgroup_create(keys[9], "group-example", id="0", mkstream=True)
    entry = await redis.xadd(keys[9], {"payload": "synthetic"})
    await redis.xreadgroup("group-example", "consumer-example", {keys[9]: ">"})
    await redis.set(keys[7], "owner-example", px=10000)
    await redis.hset(keys[8], mapping={"gen": "1"})
    await redis.set(keys[10], "original-example")
    args = [
        run_id,
        "witness-example",
        "context-example",
        "completion-example",
        "a" * 32,
        "event-example",
        "1",
        "100",
        "owner-example",
        "1",
        "group-example",
        entry,
        "consumer-example",
        "original-example",
        "1",
    ]
    return keys, args


@pytest.mark.parametrize("fault", ["lease", "generation", "consumer", "proof", "type"])
def test_wrong_fence_proof_or_type_writes_nothing(endpoint, fault):
    async def go():
        async with Redis.from_url(endpoint, decode_responses=True) as redis:
            keys, args = await setup(redis)
            try:
                if fault == "lease":
                    args[8] = "wrong-owner"
                elif fault == "generation":
                    args[9] = "2"
                elif fault == "consumer":
                    args[12] = "wrong-consumer"
                elif fault == "proof":
                    args[13] = "wrong-snapshot"
                else:
                    await redis.set(keys[4], "wrong-type")
                assert await redis.eval(_SETTLE, len(keys), *keys, *args) == 0
                assert await redis.mget(keys[:3]) == [None, None, None]
                assert not await redis.exists(keys[3], keys[5], keys[6])
            finally:
                await redis.delete(*keys)

    asyncio.run(go())


def test_terminal_prefix_survives_repair_and_replay_preserves_generation(endpoint):
    async def go():
        async with Redis.from_url(endpoint, decode_responses=True) as redis:
            keys, args = await setup(redis)
            try:
                crash = _SETTLE.replace(_OUTBOX, "error('injected prefix failure')")
                with pytest.raises(ResponseError):
                    await redis.eval(crash, len(keys), *keys, *args)
                assert await redis.get(keys[0]) == args[1]
                assert await redis.get(keys[1]) == args[2]
                assert await redis.sismember(keys[3], args[5])
                assert not await redis.exists(keys[4], keys[6])
                await redis.delete(keys[7])  # Cold repair needs no live lease.
                repair = args.copy()
                repair[8:10] = ["emitted-example", "graveyard-example"]
                assert await redis.eval(_REPAIR, len(keys), *keys, *repair) == 1
                assert await redis.hget(keys[4], "gen") == args[4]
                assert await redis.hget(keys[4], "record") == args[3]
                assert await redis.eval(_REPAIR, len(keys), *keys, *repair) == 1
                assert await redis.hget(keys[4], "gen") == args[4]
                assert await redis.ttl(keys[0]) == -1
                assert await redis.ttl(keys[1]) == -1
            finally:
                await redis.delete(*keys)

    asyncio.run(go())


def test_resolution_prefix_repairs_without_recreating_outbox(endpoint):
    async def go():
        async with Redis.from_url(endpoint, decode_responses=True) as redis:
            keys, args = await setup(redis)
            try:
                assert await redis.eval(_SETTLE, len(keys), *keys, *args) == 1
                clearing = args.copy()
                clearing[8] = "emitted-example"
                crash = _CLEAR.replace(
                    "redis.call('DEL',KEYS[5],KEYS[2])", "error('injected resolution failure')"
                )
                with pytest.raises(ResponseError):
                    await redis.eval(crash, len(keys), *keys, *clearing)
                repair = args.copy()
                repair[8:10] = ["emitted-example", "graveyard-example"]
                assert await redis.eval(_REPAIR, len(keys), *keys, *repair) == 2
                assert not await redis.exists(keys[1], keys[4])
                assert await redis.get(keys[2]) == "emitted-example"
                assert await redis.get(keys[0]) == args[1]
            finally:
                await redis.delete(*keys)

    asyncio.run(go())


def test_clear_generation_race_retains_owed_and_graveyard_precedes_resolution(endpoint):
    async def go():
        async with Redis.from_url(endpoint, decode_responses=True) as redis:
            keys, args = await setup(redis)
            dead = keys[0] + ":dead"
            try:
                assert await redis.eval(_SETTLE, len(keys), *keys, *args) == 1
                clearing = args.copy()
                clearing[8] = "emitted-example"
                await redis.hset(keys[4], "gen", "b" * 32)
                assert await redis.eval(_CLEAR, len(keys), *keys, *clearing) == 0
                assert await redis.get(keys[1]) == args[2]
                assert await redis.get(keys[2]) is None
                await redis.hset(keys[4], "gen", args[4])
                await redis.set(dead, "invalid-type")
                grave = args.copy()
                grave[8] = "graveyard-example"
                grave.extend([10, "synthetic-completion", "thread deleted at provider", "now"])
                assert await redis.eval(_GRAVEYARD, len(keys) + 1, *keys, dead, *grave) == 0
                assert await redis.get(keys[2]) is None
                assert await redis.get(keys[1]) == args[2]
                await redis.delete(dead)
                assert await redis.eval(_GRAVEYARD, len(keys) + 1, *keys, dead, *grave) == 1
                assert await redis.xlen(dead) == 1
                assert await redis.get(keys[2]) == "graveyard-example"
                assert not await redis.exists(keys[1], keys[4])
            finally:
                await redis.delete(*keys, dead)

    asyncio.run(go())
