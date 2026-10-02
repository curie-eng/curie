"""Shared Valkey request budgets for routes that explicitly opt in."""

import hashlib
from typing import Any

from fastapi import HTTPException, Request
from redis.asyncio import Redis
from redis.exceptions import RedisError

# One script owns the counter and its expiry. Rejected requests do not grow the
# counter, and every API replica observes the same remaining window.
_CHARGE = """
local count = redis.call('GET', KEYS[1])
if not count then
    redis.call('SET', KEYS[1], 1, 'EX', ARGV[2])
    return {1, 0}
end
local remaining = redis.call('PTTL', KEYS[1])
if remaining < 0 then
    return redis.error_reply('rate limit counter has no expiry')
end
if tonumber(count) >= tonumber(ARGV[1]) then
    return {0, math.floor((remaining + 999) / 1000)}
end
redis.call('INCR', KEYS[1])
return {1, 0}
"""


async def charge_rate_limit(
    client: Redis, *, route: str, address: str, limit: int, window_seconds: int
) -> int | None:
    """Return seconds until retry when this address exhausts a route budget."""
    if limit < 1 or window_seconds < 1:
        raise ValueError("rate limit and window must be positive")
    address_hash = hashlib.sha256(address.encode()).hexdigest()
    key = f"rate_limit:{route}:{address_hash}"
    result: Any = await client.eval(_CHARGE, 1, key, limit, window_seconds)
    allowed, retry_after = result
    return None if int(allowed) == 1 else max(1, int(retry_after))


async def require_rate_limit(
    request: Request, *, route: str, limit: int, window_seconds: int
) -> None:
    """Guard an opted in route before any database work starts."""
    if request.client is None or not request.client.host:
        raise HTTPException(
            503, "client address unavailable", headers={"Cache-Control": "no-store"}
        )
    try:
        retry_after = await charge_rate_limit(
            request.app.state.valkey,
            route=route,
            address=request.client.host,
            limit=limit,
            window_seconds=window_seconds,
        )
    except RedisError:
        raise HTTPException(
            503, "rate limiter unavailable", headers={"Cache-Control": "no-store"}
        ) from None
    if retry_after is not None:
        raise HTTPException(
            429,
            "rate limit exceeded",
            headers={"Retry-After": str(retry_after), "Cache-Control": "no-store"},
        )
