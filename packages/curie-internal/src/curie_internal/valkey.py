"""Primary-endpoint recovery for ordinary platform Valkey clients.

Only observational commands are retried. Writes, stream deliveries and scripts
surface uncertain outcomes to their existing caller after invalidating sockets.
Protected transports deliberately do not use these clients.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import redis
import redis.asyncio as aioredis
from redis.asyncio.client import Pipeline as AsyncPipeline
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import NoBackoff
from redis.client import Pipeline
from redis.exceptions import ConnectionError, ReadOnlyError, ResponseError
from redis.retry import Retry


def is_failover_error(error: BaseException) -> bool:
    """A stale primary connection, including a demoted server's blocked read."""
    return isinstance(error, (ConnectionError, ReadOnlyError)) or (
        isinstance(error, ResponseError) and str(error).startswith("UNBLOCKED")
    )


_READ_COMMANDS = frozenset(
    {
        "PING",
        "GET",
        "MGET",
        "EXISTS",
        "TTL",
        "PTTL",
        "TYPE",
        "STRLEN",
        "HGET",
        "HMGET",
        "HGETALL",
        "HEXISTS",
        "HLEN",
        "SCARD",
        "SISMEMBER",
        "SMEMBERS",
        "LLEN",
        "LRANGE",
        "ZRANGE",
        "ZRANGEBYSCORE",
        "ZCARD",
        "ZSCORE",
        "SCAN",
        "HSCAN",
        "SSCAN",
        "ZSCAN",
        "XLEN",
        "XRANGE",
        "XREVRANGE",
        "XPENDING",
        "XINFO",
        "INFO",
        "TIME",
    }
)
_BACKOFF_S = (0.1, 0.2, 0.4)


def _is_read(args: tuple[Any, ...]) -> bool:
    if not args:
        return False
    command = args[0]
    if isinstance(command, bytes):
        command = command.decode("ascii")
    return str(command).upper() in _READ_COMMANDS


class _Pipeline(Pipeline):
    def execute(self, raise_on_error: bool = True) -> Any:
        try:
            return super().execute(raise_on_error=raise_on_error)
        except (ConnectionError, ResponseError) as error:
            if not is_failover_error(error):
                raise
            self.connection_pool.disconnect()
            raise


class _AsyncPipeline(AsyncPipeline):
    async def execute(self, raise_on_error: bool = True) -> Any:
        try:
            return await super().execute(raise_on_error=raise_on_error)
        except (ConnectionError, ResponseError) as error:
            if not is_failover_error(error):
                raise
            await self.connection_pool.disconnect()
            raise


class Redis(redis.Redis):
    """Sync platform client; every new socket resolves the configured host again."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # redis-py 8.1 defaults to retrying even INCR/EVAL after a lost reply.
        # Disable that layer, including pools supplied by Redis.from_url.
        self.set_retry(Retry(NoBackoff(), 0))

    def _execute_command(self, *args: Any, **options: Any) -> Any:
        retries = len(_BACKOFF_S) if _is_read(args) else 0
        for attempt in range(retries + 1):
            try:
                return super()._execute_command(*args, **options)  # type: ignore[no-untyped-call]
            except (ConnectionError, ResponseError) as error:
                if not is_failover_error(error):
                    raise
                self.connection_pool.disconnect()
                if attempt == retries:
                    raise
                time.sleep(_BACKOFF_S[attempt])
        raise AssertionError("unreachable")

    def pipeline(self, transaction: bool = True, shard_hint: str | None = None) -> Pipeline:
        return _Pipeline(self.connection_pool, self.response_callbacks, transaction, shard_hint)


class AsyncRedis(aioredis.Redis):
    """Async platform client with the same command policy as the sync client."""

    def __init__(self, *args: Any, retry_reads: bool = True, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.set_retry(AsyncRetry(NoBackoff(), 0))
        self._retry_reads = retry_reads

    async def execute_command(self, *args: Any, **options: Any) -> Any:
        retries = len(_BACKOFF_S) if self._retry_reads and _is_read(args) else 0
        for attempt in range(retries + 1):
            try:
                return await super().execute_command(*args, **options)  # type: ignore[no-untyped-call]
            except (ConnectionError, ResponseError) as error:
                if not is_failover_error(error):
                    raise
                await self.connection_pool.disconnect()
                if attempt == retries:
                    raise
                await asyncio.sleep(_BACKOFF_S[attempt])
        raise AssertionError("unreachable")

    def pipeline(self, transaction: bool = True, shard_hint: str | None = None) -> AsyncPipeline:
        return _AsyncPipeline(
            self.connection_pool, self.response_callbacks, transaction, shard_hint
        )
