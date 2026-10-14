"""KillSwitch consumer against the REAL compose Valkey (never mocked).

Publishes on the L1 channel and sets/clears the flag key exactly as the API does,
and checks: is_killed reflects the flag; the subscriber invokes on_kill for a
kill event and covers a missed pubsub via the flag; a resume needs no worker
action.
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path

import pytest
import redis.asyncio as aredis
from curie_internal.keyspace import KILL_CHANNEL, kill_key
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_worker import killswitch as killswitch_module
from curie_worker.killswitch import KillSwitch

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from queue_fixtures import wait_until as _wait_until  # noqa: E402


def _client() -> aredis.Redis:
    return aredis.Redis(
        host=_VALKEY_HOST, port=_VALKEY_PORT, password=_VALKEY_PW or None, decode_responses=True
    )


def test_is_killed_reflects_the_flag_key() -> None:
    async def go() -> None:
        client = _client()
        try:
            await client.ping()
        except aredis.RedisError as exc:
            pytest.skip(f"Valkey not reachable: {exc}")
        agent_id = uuid.uuid4()
        ks = KillSwitch(client, on_kill=_noop)
        try:
            assert await ks.is_killed(agent_id) is False
            await client.set(kill_key(agent_id), "1")
            assert await ks.is_killed(agent_id) is True  # covers a missed pubsub
            await client.delete(kill_key(agent_id))
            assert await ks.is_killed(agent_id) is False
        finally:
            await client.aclose()

    asyncio.run(go())


def test_subscriber_invokes_on_kill_for_a_kill_event() -> None:
    async def go() -> None:
        client = _client()
        publisher = _client()
        try:
            await client.ping()
        except aredis.RedisError as exc:
            pytest.skip(f"Valkey not reachable: {exc}")

        killed: list[uuid.UUID] = []

        async def on_kill(agent_id: uuid.UUID) -> None:
            killed.append(agent_id)

        ks = KillSwitch(client, on_kill=on_kill)
        task = asyncio.create_task(ks.run())
        try:
            # Give the subscriber a moment to subscribe before publishing.
            await asyncio.sleep(0.2)
            agent_id = uuid.uuid4()
            await publisher.publish(
                KILL_CHANNEL,
                json.dumps({"agent_id": str(agent_id), "action": "kill", "ts": "now"}),
            )
            await _wait_until(lambda: bool(killed))
            assert killed == [agent_id]

            # A resume event triggers no interrupt callback.
            await publisher.publish(
                KILL_CHANNEL,
                json.dumps({"agent_id": str(uuid.uuid4()), "action": "resume", "ts": "now"}),
            )
            await asyncio.sleep(0.3)
            assert len(killed) == 1
        finally:
            ks.request_stop()
            await task
            await client.aclose()
            await publisher.aclose()

    asyncio.run(go())


def test_wedged_on_kill_does_not_stall_the_pubsub_loop(monkeypatch) -> None:
    """#742: on_kill (`Kernel.interrupt_agent` in production) is awaited inline
    in the read loop. If a handler call hangs -- the shape a wedged runner
    produces one layer down -- the loop must not stall behind it and drop
    every kill event that arrives afterward, which is exactly the failure the
    surrounding try/except guards against without actually preventing an
    unbounded await. Bounding the dispatch is what actually prevents it: the
    stuck first call times out and a second agent's kill is still dispatched
    promptly, against the real Valkey pubsub channel (never mocked here)."""

    async def go() -> None:
        client = _client()
        publisher = _client()
        try:
            await client.ping()
        except aredis.RedisError as exc:
            pytest.skip(f"Valkey not reachable: {exc}")

        monkeypatch.setattr(killswitch_module, "_ON_KILL_TIMEOUT_S", 0.2)

        wedged = asyncio.Event()  # never set: the first dispatch hangs forever
        calls: list[uuid.UUID] = []

        async def on_kill(agent_id: uuid.UUID) -> None:
            calls.append(agent_id)
            if len(calls) == 1:
                await wedged.wait()

        ks = KillSwitch(client, on_kill=on_kill)
        task = asyncio.create_task(ks.run())
        try:
            await asyncio.sleep(0.2)  # let the subscriber finish subscribing
            agent_a = uuid.uuid4()
            agent_b = uuid.uuid4()

            await publisher.publish(
                KILL_CHANNEL,
                json.dumps({"agent_id": str(agent_a), "action": "kill", "ts": "now"}),
            )
            await _wait_until(lambda: len(calls) >= 1)

            # agent_a's dispatch is now stuck. A second kill must still reach
            # its own handler call well before the wedge is ever released --
            # proof the read loop kept polling instead of blocking on agent_a.
            await publisher.publish(
                KILL_CHANNEL,
                json.dumps({"agent_id": str(agent_b), "action": "kill", "ts": "now"}),
            )
            await _wait_until(lambda: len(calls) >= 2, timeout=2.0)
            assert calls == [agent_a, agent_b]
        finally:
            wedged.set()
            ks.request_stop()
            await task
            await client.aclose()
            await publisher.aclose()

    asyncio.run(go())


async def _noop(_agent_id: uuid.UUID) -> None:
    return None
