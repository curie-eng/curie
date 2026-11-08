"""Real primary promotion through a repointable TCP endpoint, without client restarts."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import subprocess
import time
import uuid
from collections.abc import Iterator

import httpx
import pytest
import redis
from curie_api.cluster_message_replies import ClusterMessageReplyStore
from curie_api.config import Settings, get_settings
from curie_api.routers.cluster_message_replies import internal_router, router
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_connector_proxy.server import _set_nx_ex
from curie_dispatcher.app import build_redis
from curie_internal.keyspace import KILL_CHANNEL
from curie_internal.valkey import AsyncRedis
from curie_worker.killswitch import KillSwitch
from curie_worker.stream_consumer import ReadLoopSpec, StreamConsumer
from curie_worker.threadlock import ThreadLock
from fastapi import FastAPI
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from .queue_fixtures import wait_until

# An independent fixture password, including URL-reserved characters.
_PASSWORD = "example#managed%token+123"


def _docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout.strip()


@pytest.fixture(scope="module")
def servers() -> Iterator[tuple[redis.Redis, redis.Redis, str, str]]:
    token = uuid.uuid4().hex[:12]
    network = f"valkey-failover-{token}"
    names = [f"{network}-primary", f"{network}-replica"]
    with contextlib.ExitStack() as cleanup:
        _docker("network", "create", network)
        cleanup.callback(_docker, "network", "rm", network)
        clients = []
        for name in names:
            cleanup.callback(_docker, "rm", "-f", "-v", name)
            _docker(
                "run",
                "-d",
                "--name",
                name,
                "--network",
                network,
                "-p",
                "127.0.0.1::6379",
                "valkey/valkey:8.1.10-alpine",
                "valkey-server",
                "--appendonly",
                "no",
                "--save",
                "",
                "--maxmemory-policy",
                "noeviction",
                "--requirepass",
                _PASSWORD,
                "--masterauth",
                _PASSWORD,
            )
            port = int(_docker("port", name, "6379/tcp").rsplit(":", 1)[1])
            client = redis.Redis(
                host="127.0.0.1",
                port=port,
                password=_PASSWORD,
                decode_responses=True,
                socket_timeout=2,
            )
            cleanup.callback(client.close)
            deadline = time.monotonic() + 20
            while True:
                try:
                    client.ping()
                    break
                except RedisConnectionError:
                    if time.monotonic() >= deadline:
                        raise
            clients.append(client)
        yield clients[0], clients[1], names[0], names[1]
    remaining = _docker("ps", "-a", "--filter", f"name={network}", "--format", "{{.Names}}")
    assert not remaining, "owned failover containers survived teardown"
    assert network not in _docker("network", "ls", "--format", "{{.Name}}")


class Endpoint:
    """Existing sockets keep their destination when new connections are repointed."""

    def __init__(self, port: int) -> None:
        self.target = port
        self.writers: set[asyncio.StreamWriter] = set()
        self.connections = 0
        self.drop_command: bytes | None = None
        self.drop_remaining = 1
        self.dropped = 0
        self.rejected = asyncio.Queue[None]()
        self.unblocked = asyncio.Queue[None]()

    async def connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", self.target)
        self.connections += 1
        self.writers.update((writer, upstream_writer))

        drop_this_reply = False
        drop_transaction = False

        async def requests() -> None:
            nonlocal drop_this_reply, drop_transaction
            while header := await reader.readline():
                frame = bytearray(header)
                parts = []
                for _ in range(int(header[1:])):
                    length = await reader.readline()
                    value = await reader.readexactly(int(length[1:]) + 2)
                    frame.extend(length)
                    frame.extend(value)
                    parts.append(value[:-2])
                if self.drop_command == parts[0].upper():
                    self.drop_remaining -= 1
                    if self.drop_remaining == 0:
                        self.drop_command = None
                    drop_this_reply = True
                    drop_transaction = parts[0].upper() == b"EXEC"
                upstream_writer.write(frame)
                await upstream_writer.drain()

        async def replies() -> None:
            buffered = bytearray()
            readonly_tail = b""
            unblocked_tail = b""
            while data := await upstream_reader.read(65536):
                observed = readonly_tail + data
                if b"READONLY" in observed:
                    self.rejected.put_nowait(None)
                readonly_tail = observed[-7:]
                unblocked = unblocked_tail + data
                if b"UNBLOCKED" in unblocked:
                    self.unblocked.put_nowait(None)
                unblocked_tail = unblocked[-8:]
                if drop_this_reply:
                    buffered.extend(data)
                    # EXEC's integer array proves the transaction committed;
                    # MULTI/QUEUED acknowledgements alone do not prove that.
                    if drop_transaction and b"*1\r\n:" not in buffered:
                        continue
                    self.dropped += 1
                    writer.close()
                    return
                writer.write(data)
                await writer.drain()

        tasks = [asyncio.create_task(requests()), asyncio.create_task(replies())]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for conn in (writer, upstream_writer):
                conn.close()
                await conn.wait_closed()
                self.writers.discard(conn)

    async def close(self) -> None:
        for writer in tuple(self.writers):
            writer.close()
        for writer in tuple(self.writers):
            await writer.wait_closed()


def test_failover_recovers_consumer_lock_and_api_without_restart(
    servers: tuple[redis.Redis, redis.Redis, str, str],
) -> None:
    asyncio.run(_failover(servers))


async def _failover(servers: tuple[redis.Redis, redis.Redis, str, str]) -> None:
    primary, replica, primary_name, replica_name = servers
    replica.replicaof(primary_name, 6379)
    endpoint = Endpoint(primary.connection_pool.connection_kwargs["port"])
    server = await asyncio.start_server(endpoint.connect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    settings = Settings(
        valkey_host="127.0.0.1", valkey_port=port, valkey_password=_PASSWORD, valkey_url=""
    )
    api = AsyncRedis.from_url(settings.valkey_dsn(), decode_responses=True)
    worker = AsyncRedis(
        host="127.0.0.1", port=port, password=_PASSWORD, decode_responses=True, socket_timeout=2
    )
    dispatcher = build_redis(
        DispatcherConfig(
            valkey_host="127.0.0.1",
            valkey_port=port,
            valkey_password=_PASSWORD,
            approval_chat_attester_secret="example-dispatcher-attester",
        )
    )
    consumer = StreamConsumer(worker)
    lock = ThreadLock(worker, ttl_ms=3000, acquire_timeout_s=2, poll_interval_s=0.01)
    stream = "test:failover:stream"
    received: asyncio.Queue[str] = asyncio.Queue()
    ref = str(uuid.uuid4())
    app = FastAPI()
    app.state.valkey = api
    app.include_router(router)
    app.include_router(internal_router)
    task = None
    try:
        # The proxy consumes the same Settings DSN as API-issued connector grants.
        assert (
            await asyncio.to_thread(_set_nx_ex, settings.valkey_dsn(), "test:grant", 60) == b"+OK"
        )
        assert (
            await asyncio.to_thread(_set_nx_ex, settings.valkey_dsn(), "test:grant", 60) == b"$-1"
        )
        await worker.xgroup_create(stream, "group", id="0", mkstream=True)
        await ClusterMessageReplyStore(api, ttl_s=60, max_events=10, max_bytes=10000).append(
            ref,
            {"event": "test", "value": "before"},
            terminal=False,
        )
        token = await lock.acquire("test:lock")
        await lock.release("test:lock", token)
        await asyncio.to_thread(dispatcher.ping)

        async def handled(entry_id: str, fields: dict[str, str]) -> None:
            await worker.xack(stream, "group", entry_id)
            received.put_nowait(fields["value"])

        spec = ReadLoopSpec(
            stream,
            "group",
            "consumer",
            1,
            100,
            0.01,
            "idle %s",
            "reconnecting %s",
            logging.getLogger(__name__),
        )
        task = asyncio.create_task(consumer._consume(spec, handled))
        primary.xadd(stream, {"value": "before"})
        assert await asyncio.wait_for(received.get(), 5) == "before"
        # WAIT is solely a test barrier, not a product durability guarantee.
        assert primary.wait(1, 10000) == 1
        assert primary.info("server")["run_id"] != replica.info("server")["run_id"]
        replica.execute_command("REPLICAOF", "NO", "ONE")  # type: ignore[no-untyped-call]
        primary.replicaof(replica_name, 6379)
        # Keep the endpoint stale through >10 READONLY replies, exceeding the
        # supervisor's crash budget if these errors escaped the read loop.
        for _ in range(12):
            await asyncio.wait_for(endpoint.rejected.get(), 5)
        assert not task.done()
        before_reconnect = endpoint.connections
        with pytest.raises(ReadOnlyError):
            await lock.acquire("test:lock")
        with pytest.raises(ReadOnlyError):
            await asyncio.to_thread(dispatcher.set, "test:dispatch", "before")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as http:
            headers = {"X-API-Key": get_settings().api_key}
            event = {"event": "test", "value": "after", "target": {"reply_ref": ref}}
            internal_headers = {"X-Curie-Adapter-Secret": get_settings().internal_worker_token}
            refused = await http.post(
                f"/v1/internal/cluster-message-replies/{ref}", headers=internal_headers, json=event
            )
            assert refused.status_code == 503
            endpoint.target = replica.connection_pool.connection_kwargs["port"]
            replica.xadd(stream, {"value": "after"})
            assert await asyncio.wait_for(received.get(), 5) == "after"
            token = await lock.acquire("test:lock")
            await lock.release("test:lock", token)
            assert await asyncio.to_thread(dispatcher.set, "test:dispatch", "after")
            appended = await http.post(
                f"/v1/internal/cluster-message-replies/{ref}", headers=internal_headers, json=event
            )
            assert appended.status_code == 200
            reply = await http.get(f"/cluster-message-replies/{ref}", headers=headers)
            assert reply.status_code == 200
            assert reply.json()["events"] == [{"event": "test", "value": "before"}, event]
        assert endpoint.connections > before_reconnect
        assert not task.done()

        consumer.request_stop()
        await asyncio.wait_for(task, 5)

        # Lose the reply AFTER a real write committed: no hidden driver retry
        # may double either a command, script, or pipeline's effect.
        for operation in ("incr", "eval", "pipeline"):
            await api.ping()
            endpoint.drop_remaining = 1
            endpoint.drop_command = {"incr": b"INCRBY", "eval": b"EVAL", "pipeline": b"EXEC"}[
                operation
            ]
            with pytest.raises(RedisConnectionError):
                if operation == "incr":
                    await api.incr("test:uncertain")
                elif operation == "eval":
                    await api.eval("return redis.call('INCR', KEYS[1])", 1, "test:uncertain")
                else:
                    async with api.pipeline() as pipe:
                        pipe.incr("test:uncertain")
                        await pipe.execute()
        assert replica.get("test:uncertain") == "3"
        await api.ping()
        endpoint.drop_remaining = 1
        endpoint.drop_command = b"GET"
        assert await api.get("test:uncertain") == "3"
        for operation in ("incr", "eval", "pipeline"):
            await asyncio.to_thread(dispatcher.ping)
            endpoint.drop_remaining = 1
            endpoint.drop_command = {"incr": b"INCRBY", "eval": b"EVAL", "pipeline": b"EXEC"}[
                operation
            ]

            def write(operation: str = operation) -> None:
                if operation == "incr":
                    dispatcher.incr("test:sync-uncertain")
                elif operation == "eval":
                    dispatcher.eval("return redis.call('INCR', KEYS[1])", 1, "test:sync-uncertain")
                else:
                    with dispatcher.pipeline() as pipe:
                        pipe.incr("test:sync-uncertain")
                        pipe.execute()

            with pytest.raises(RedisConnectionError):
                await asyncio.to_thread(write)
        assert replica.get("test:sync-uncertain") == "3"
        await asyncio.to_thread(dispatcher.ping)
        endpoint.drop_remaining = 1
        endpoint.drop_command = b"GET"
        assert await asyncio.to_thread(dispatcher.get, "test:sync-uncertain") == "3"
        for synchronous in (False, True):
            endpoint.drop_remaining = 4
            endpoint.drop_command = b"GET"
            dropped_before = endpoint.dropped
            with pytest.raises(RedisConnectionError):
                if synchronous:
                    await asyncio.to_thread(dispatcher.get, "test:sync-uncertain")
                else:
                    await api.get("test:uncertain")
            assert endpoint.dropped - dropped_before == 4
        assert await api.get("test:uncertain") == "3"
    finally:
        consumer.request_stop()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await api.aclose()
        await worker.aclose()
        dispatcher.close()
        server.close()
        await server.wait_closed()
        await endpoint.close()


def test_kill_subscriber_survives_unrelated_timeout_and_socket_loss(
    servers: tuple[redis.Redis, redis.Redis, str, str],
) -> None:
    asyncio.run(_kill_subscriber(servers))


async def _kill_subscriber(servers: tuple[redis.Redis, redis.Redis, str, str]) -> None:
    primary = next(
        client for client in servers[:2] if client.info("replication")["role"] == "master"
    )
    port = primary.connection_pool.connection_kwargs["port"]
    client = AsyncRedis(
        host="127.0.0.1", port=port, password=_PASSWORD, decode_responses=True, socket_timeout=0.05
    )
    delivered: asyncio.Queue[uuid.UUID] = asyncio.Queue()

    async def on_kill(agent_id: uuid.UUID) -> None:
        delivered.put_nowait(agent_id)

    switch = KillSwitch(client, on_kill=on_kill)
    task = asyncio.create_task(switch.run())

    def subscribers() -> list[dict[str, str]]:
        return [row for row in primary.client_list() if "P" in row["flags"]]

    async def kill() -> None:
        agent_id = uuid.uuid4()
        primary.publish(KILL_CHANNEL, json.dumps({"agent_id": str(agent_id), "action": "kill"}))
        assert await asyncio.wait_for(delivered.get(), 3) == agent_id
        assert not task.done()

    try:
        await wait_until(lambda: bool(subscribers()), "kill subscription")
        original_id = subscribers()[0]["id"]
        await kill()
        with pytest.raises(RedisTimeoutError):
            await client.blpop("test:unrelated-timeout", timeout=1)
        await kill()
        assert subscribers()[0]["id"] == original_id, "a timeout reset the unrelated subscription"
        # An unrelated response error must also preserve the live subscription.
        with pytest.raises(ResponseError, match="NOGROUP"):
            await client.xreadgroup("absent-group", "reader", {"absent-stream": ">"})
        await kill()
        assert subscribers()[0]["id"] == original_id
        primary.client_kill_filter(_type="pubsub")
        await wait_until(
            lambda: bool(subscribers()) and subscribers()[0]["id"] != original_id,
            "kill resubscription",
        )
        await kill()
    finally:
        switch.request_stop()
        await asyncio.gather(task, return_exceptions=True)
        await client.aclose()


def test_blocked_stream_read_survives_primary_demotion(
    servers: tuple[redis.Redis, redis.Redis, str, str],
) -> None:
    asyncio.run(_blocked_demotion(servers))


async def _blocked_demotion(servers: tuple[redis.Redis, redis.Redis, str, str]) -> None:
    first, second, first_name, second_name = servers
    if first.info("replication")["role"] == "master":
        primary, replica, primary_name, replica_name = first, second, first_name, second_name
    else:
        primary, replica, primary_name, replica_name = second, first, second_name, first_name
    replica.replicaof(primary_name, 6379)
    endpoint = Endpoint(primary.connection_pool.connection_kwargs["port"])
    server = await asyncio.start_server(endpoint.connect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = AsyncRedis(
        host="127.0.0.1", port=port, password=_PASSWORD, decode_responses=True, socket_timeout=6
    )
    consumer = StreamConsumer(client)
    stream = "test:blocked-demotion"
    received: asyncio.Queue[str] = asyncio.Queue()

    async def handled(entry_id: str, fields: dict[str, str]) -> None:
        await client.xack(stream, "group", entry_id)
        received.put_nowait(fields["value"])

    task = None
    try:
        await client.xgroup_create(stream, "group", id="0", mkstream=True)
        assert primary.wait(1, 10000) == 1
        spec = ReadLoopSpec(
            stream,
            "group",
            "consumer",
            1,
            5000,
            0.01,
            "idle %s",
            "reconnecting %s",
            logging.getLogger(__name__),
        )
        task = asyncio.create_task(consumer._consume(spec, handled))
        await wait_until(
            lambda: any(
                row["cmd"] == "xreadgroup" and "b" in row["flags"] for row in primary.client_list()
            ),
            "blocked XREADGROUP",
        )
        replica.execute_command("REPLICAOF", "NO", "ONE")  # type: ignore[no-untyped-call]
        primary.replicaof(replica_name, 6379)
        await asyncio.wait_for(endpoint.unblocked.get(), 3)
        assert not task.done(), "UNBLOCKED escaped the shared read loop"
        endpoint.target = replica.connection_pool.connection_kwargs["port"]
        replica.xadd(stream, {"value": "after-blocked-demotion"})
        assert await asyncio.wait_for(received.get(), 5) == "after-blocked-demotion"
        assert not task.done()
    finally:
        consumer.request_stop()
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await client.aclose()
        server.close()
        await server.wait_closed()
        await endpoint.close()
