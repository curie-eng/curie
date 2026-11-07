"""Dispatcher-owned Valkey client construction."""

import redis
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_internal.valkey import Redis


def build_redis(config: DispatcherConfig) -> redis.Redis:
    """A decode_responses Valkey client (str in, str out) for stream + dedupe ops."""
    return Redis(
        host=config.valkey_host,
        port=config.valkey_port,
        password=config.valkey_password or None,
        db=config.valkey_db,
        decode_responses=True,
        ssl=config.valkey_tls,
    )
