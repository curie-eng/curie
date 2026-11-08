"""A killable runs consumer using the production kernel and static substrate."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal

import redis
from curie_channel_gateway.reply_transport import HttpReplyAdapter
from curie_worker.binding import BindingResolver
from curie_worker.bundle_store import BundleStore
from curie_worker.config import WorkerConfig
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.kernel.core import Kernel
from curie_worker.markers import Markers
from curie_worker.runner_client import RunnerClient
from curie_worker.sandbox import (
    AffinityStore,
    SandboxSubstrate,
    StaticPoolSandboxClient,
    StaticShellSlot,
    SubstrateConfig,
    shell_ssl_context,
)
from curie_worker.threadlock import ThreadLock
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine


async def main() -> None:
    spec = json.loads(os.environ["CURIE_CRASH_PROOF"])
    config = WorkerConfig(**spec["config"])
    connection = dict(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password,
        decode_responses=True,
    )
    sync = redis.Redis(**connection)
    engine = create_async_engine(config.database_url, pool_size=4, max_overflow=0)
    async with Redis(**connection) as store:
        client = StaticPoolSandboxClient(
            slots=[StaticShellSlot("proof-0", "localhost", spec["port"], "crash-pool")],
            redis_client=sync,
            control_token=spec["control"],
            ssl_context=shell_ssl_context(spec["ca"]),
            bundle_store=BundleStore(config),
            key_prefix=spec["sandbox_prefix"],
        )
        substrate = SandboxSubstrate(
            client,
            AffinityStore(sync, pressure_client=store, key_prefix=spec["sandbox_prefix"]),
            SubstrateConfig(
                namespace="default",
                warm_pool="crash-pool",
                key_prefix=spec["sandbox_prefix"],
                claim_timeout_seconds=20,
            ),
        )
        sink = HttpReplyAdapter({"crash-proof": "fixture-secret"})
        async with RunnerClient(
            ssl_context=shell_ssl_context(spec["ca"]),
            total_timeout_s=config.runner_total_timeout_s,
            connect_timeout_s=config.runner_connect_timeout_s,
        ) as runner:
            kernel = Kernel(
                substrate=substrate,
                runner=runner,
                sink=sink,
                config=config,
                markers=Markers(store, config),
                binding=BindingResolver(engine, config),
                lock=ThreadLock(
                    store, ttl_ms=config.lock_ttl_ms, acquire_timeout_s=5, poll_interval_s=0.02
                ),
                pressure_lock=ThreadLock(
                    store, ttl_ms=config.lock_ttl_ms, acquire_timeout_s=5, poll_interval_s=0.02
                ),
            )
            consumer = Consumer(
                redis=store, kernel=kernel, config=config, leases=DeliveryLeaseStore(store, config)
            )
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, consumer.request_stop)
            try:
                await consumer.run()
            finally:
                await sink.aclose()
    sync.close()
    await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
