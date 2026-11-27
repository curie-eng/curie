"""The one builder for the API's platform Valkey clients (#5995).

Every API Valkey client is built here so none of them can drift on the
transport bounds. ``socket_timeout`` fails a command whose reply does not arrive
in time; some of these calls run inside an open database transaction or under an
advisory lock, where a long wait holds a database connection too. redis-py 8
already defaults it to 5 s; setting it here keeps the bound explicit and
configurable. The API issues no blocking read (XREAD BLOCK, BLPOP, pub/sub
listen), so no command is meant to wait server side. ``health_check_interval``
pings a pooled connection that sat idle longer than the interval before sending
a command on it, so a connection lost while idle fails on that ping, before the
command was sent.
"""

from curie_internal.valkey import AsyncRedis

from .config import Settings


def create_valkey(settings: Settings, *, retry_reads: bool = True) -> AsyncRedis:
    """A platform Valkey client with the configured read deadline and health check.

    A 0 setting becomes redis-py's own off value: no socket deadline (None) or
    no health check (0).
    """
    return AsyncRedis.from_url(
        settings.valkey_dsn(),
        retry_reads=retry_reads,
        socket_timeout=settings.valkey_socket_timeout_s or None,
        health_check_interval=settings.valkey_health_check_interval_s,
    )
