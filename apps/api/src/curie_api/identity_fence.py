"""Fence cached admission decisions around serialized identity mutations."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import HTTPException, Request
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

FENCE_KEY = "curie:identity:admission-fence"
_PUBLISH = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('SET', KEYS[1], ARGV[2])
end
return false
"""


def protects(method: str, path: str) -> bool:
    if method in {"GET", "HEAD", "OPTIONS"}:
        return False
    if path == "/identity/resolve":
        return False
    if path == "/agents":
        return True
    if path.startswith("/agents/"):
        parts = path.strip("/").split("/")
        return len(parts) == 2 or (len(parts) >= 3 and parts[2] in {"channels", "access-policy"})
    return any(
        path == prefix or path.startswith(prefix + "/")
        for prefix in (
            "/directory",
            "/identity",
            "/channel-identities",
            "/provider-installations",
            "/teams",
        )
    )


@asynccontextmanager
async def mutation(
    factory: async_sessionmaker[AsyncSession],
    client: Redis,
) -> AsyncIterator[None]:
    # The lock transaction owns no row locks. Requests use their own sessions,
    # and the advisory lock remains held until the new cache generation exists.
    async with factory() as session, session.begin():
        await session.execute(text("SELECT pg_advisory_xact_lock(2250220)"))
        pending = f"pending:{uuid4().hex}"
        await client.set(FENCE_KEY, pending)
        try:
            yield
        finally:
            # A publication failure deliberately leaves pending: readers must
            # obtain a fresh API answer, never reuse a previous cached allow.
            await client.eval(_PUBLISH, 1, FENCE_KEY, pending, f"stable:{uuid4().hex}")


async def lock_identity_reader(session: AsyncSession) -> None:
    """Keep current identity authorization stable until this transaction ends."""
    await session.execute(text("SELECT pg_advisory_xact_lock_shared(2250220)"))


async def prepare(request: Request) -> bool:
    """Bound body consumption and authenticate before acquiring global locks.

    Credential checks use closed, independent sessions: none of their row
    locks may be held when this request waits for the writer advisory lock.
    Endpoint-specific authority and CAS checks still run inside the fence.
    """
    from .auth import (
        require_api_key,
        require_platform_key,
        verify_platform_key,
    )
    from .config import get_settings
    from .directory_credential import verify
    from .identity.connect import browser_principal
    from .routers.identity_connect import PROOF_COOKIE
    from .wirebody import read_bounded_body

    async with asyncio.timeout(5):
        body = await read_bounded_body(request, 16 * 1024 * 1024, subject="identity write body")
    # Starlette's cached Request forwards these bytes to the route parser.
    request._body = body
    key = request.headers.get("X-API-Key")
    if request.url.path.startswith("/directory"):
        if verify_platform_key(key):
            return True
        if (
            key is None
            and verify(
                request.headers.get("X-Curie-Directory-Credential", ""), get_settings().api_key
            )
            is not None
        ):
            return True
        if request.headers.get("X-Curie-Directory-Credential"):
            raise HTTPException(401, "invalid_directory_credential")
        await require_platform_key(key)
        return True
    if request.url.path.startswith("/identity/connect/"):
        async with request.app.state.sessionmaker() as session:
            token = request.url.path.removeprefix("/identity/connect/").split("/", 1)[0]
            principal = await browser_principal(
                session, token=token, proof=request.cookies.get(PROOF_COOKIE, "")
            )
        if principal is None:
            raise HTTPException(401, "sign in required")
        return True
    await require_api_key(request, key)
    return True
