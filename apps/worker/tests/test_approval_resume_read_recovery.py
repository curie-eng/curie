"""Recover a delivered approval wake whose stream read response was lost."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
import redis
from curie_internal.valkey import AsyncRedis
from curie_worker.config import WorkerConfig
from curie_worker.consumer_liveness import ConsumerLivenessStore
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.stream_consumer import DeliverySpec, ReadLoopSpec, StreamConsumer


class _LostReadEndpoint:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.armed = False
        self.withheld = asyncio.Event()
        self.release = asyncio.Event()
        self.tasks: set[asyncio.Task[Any]] = set()
        self.writers: set[asyncio.StreamWriter] = set()

    async def connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        upstream_reader, upstream_writer = await asyncio.open_connection(self.host, self.port)
        self.writers.update((writer, upstream_writer))
        lose_response = False

        async def requests() -> None:
            nonlocal lose_response
            while header := await reader.readline():
                frame = bytearray(header)
                parts = []
                for _ in range(int(header[1:])):
                    length = await reader.readline()
                    value = await reader.readexactly(int(length[1:]) + 2)
                    frame.extend(length)
                    frame.extend(value)
                    parts.append(value[:-2])
                if self.armed and parts[0].upper() == b"XREADGROUP":
                    self.armed = False
                    lose_response = True
                upstream_writer.write(frame)
                await upstream_writer.drain()

        async def replies() -> None:
            while data := await upstream_reader.read(65536):
                if lose_response:
                    self.withheld.set()
                    await self.release.wait()
                    writer.close()
                    return
                writer.write(data)
                await writer.drain()

        pair = [asyncio.create_task(requests()), asyncio.create_task(replies())]
        try:
            await asyncio.wait(pair, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for child in pair:
                child.cancel()
            await asyncio.gather(*pair, return_exceptions=True)
            for conn in (writer, upstream_writer):
                conn.close()
                self.writers.discard(conn)
            self.tasks.discard(task)

    async def close(self) -> None:
        self.release.set()
        for writer in tuple(self.writers):
            writer.close()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest.mark.parametrize("disconnect", [True, False], ids=["disconnect", "timeout"])
@pytest.mark.parametrize("lane", ["runs", "evals"])
def test_lost_resume_read_recovers_without_restarting_consumer(
    sync_redis: redis.Redis, names: dict[str, str], lane: str, disconnect: bool
) -> None:
    async def run() -> None:
        connection = sync_redis.connection_pool.connection_kwargs
        endpoint = _LostReadEndpoint(connection["host"], connection["port"])
        server = await asyncio.start_server(endpoint.connect, "127.0.0.1", 0)
        assert server.sockets is not None
        client = AsyncRedis(
            host="127.0.0.1",
            port=server.sockets[0].getsockname()[1],
            password=connection.get("password"),
            decode_responses=True,
            socket_timeout=6 if disconnect else 1,
        )
        config = WorkerConfig(key_prefix=names["prefix"])
        observed: asyncio.Queue[str] = asyncio.Queue()
        release_control = asyncio.Event()
        consumer: StreamConsumer

        async def handled(entry_id: str, fields: dict[str, str]) -> None:
            async with consumer._delivery_lease(entry_id, fields) as lease:
                assert lease is not None
                await client.xack(names["stream"], names["group"], entry_id)
                observed.put_nowait(fields["event_id"])
                if fields["event_id"] == "control":
                    await release_control.wait()

        spec = DeliverySpec(
            stream=names["stream"],
            group=names["group"],
            consumer=lane,
            dead_letter_target=names["stream"] + ":dead",
            over_cap_reason="max-delivery-exceeded",
            max_delivery=config.max_delivery,
            dead_letter_maxlen=config.dead_letter_maxlen,
            reclaim_min_idle_ms=config.reclaim_min_idle_ms,
            dead_consumer_idle_ms=config.dead_consumer_idle_ms,
            heartbeat_ttl_ms=config.consumer_heartbeat_ttl_ms,
            capability_ttl_ms=config.consumer_capability_ttl_ms,
            read_count=1,
            cap_scan_page=100,
            telemetry_source="worker" if lane == "runs" else "eval",
            handler=handled,
            logger=logging.getLogger(__name__),
            dead_letter_log="dead letter %s %s %s %s",
            dead_letter_fail_log="dead letter failed %s",
            lease_expired_idle_ms=config.lease_expired_idle_ms_value(),
        )
        consumer = StreamConsumer(
            client,
            delivery=spec,
            leases=DeliveryLeaseStore(client, config),
            liveness_store=ConsumerLivenessStore(client),
        )
        read = ReadLoopSpec(
            names["stream"],
            names["group"],
            lane,
            1,
            5000,
            0.01,
            "idle %s",
            "read interrupted %s",
            logging.getLogger(__name__),
        )
        await client.xgroup_create(names["stream"], names["group"], id="0", mkstream=True)
        task = asyncio.create_task(
            consumer._run_consumer_generation(
                {
                    "read": lambda: consumer._consume(read, handled),
                }
            )
        )
        resume = "approval-00000000-0000-4000-8000-000000000001-resolved"
        try:
            await client.xadd(names["stream"], {"event_id": "control"})
            assert await asyncio.wait_for(observed.get(), 5) == "control"
            endpoint.armed = True
            release_control.set()
            entry_id = await client.xadd(names["stream"], {"event_id": resume})
            await asyncio.wait_for(endpoint.withheld.wait(), 5)
            pending = await asyncio.to_thread(
                sync_redis.xpending_range, names["stream"], names["group"], "-", "+", 10
            )
            assert [row["message_id"] for row in pending] == [entry_id]
            assert not await asyncio.to_thread(
                sync_redis.exists,
                config.delivery_state_key(names["stream"], names["group"], entry_id),
            )
            if disconnect:
                endpoint.release.set()
            try:
                resumed = await asyncio.wait_for(observed.get(), 5)
            except TimeoutError:
                if task.done():
                    task.result()
                raise
            assert resumed == resume
            assert not task.done()
            assert (await client.xpending(names["stream"], names["group"]))["pending"] == 0
            await client.xadd(names["stream"], {"event_id": "after"})
            assert await asyncio.wait_for(observed.get(), 5) == "after"
            assert observed.empty()
            assert await client.xlen(names["stream"]) == 3
        finally:
            endpoint.release.set()
            release_control.set()
            consumer.request_stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            server.close()
            await endpoint.close()
            await server.wait_closed()
            await client.aclose()

    asyncio.run(run())
