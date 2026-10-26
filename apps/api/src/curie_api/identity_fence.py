"""Fence cached admission decisions around serialized identity mutations."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import anyio
from fastapi import HTTPException, Request
from redis.asyncio import Redis
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

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


_LOG = logging.getLogger(__name__)
_OWNER_KEY = "curie_identity_writer"
_QUARANTINE: list["_WriterOwner"] = []


class IdentityWriterUnavailable(RuntimeError):
    """@spec IDENTITY-WRITER-LIFETIME-V1-2."""

    def __init__(self) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-2."""
        super().__init__("identity_writer_unavailable")


class _WriterOwner:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3/4/6. One physical serialization lease."""

    def __init__(self, connection: AsyncConnection, session: AsyncSession) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-3/4."""
        self.connection, self.session = connection, session
        sync = connection.sync_connection
        if sync is None:
            raise IdentityWriterUnavailable()
        raw = sync.connection
        self.dbapi, self.driver = raw.dbapi_connection, raw.driver_connection
        self.poisoned = False
        self.borrowable = False
        self.control_task = asyncio.current_task()
        self.allow_return = False
        self.returned = False
        self.disposed = False
        self.listeners: list[tuple[Any, str, Any]] = []
        for name in ("before_execute", "before_cursor_execute", "commit"):
            self.listen(connection.sync_connection, name, self.guard)
        self.listen(connection.engine.sync_engine.pool, "invalidate", self.invalidated)
        self.listen(connection.engine.sync_engine.pool, "checkin", self.checked_in)

    def listen(self, target: Any, name: str, callback: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4."""
        event.listen(target, name, callback)
        self.listeners.append((target, name, callback))

    def guard(self, *args: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4. Rollback cannot clear this latch."""
        connection = self.connection
        sync = connection.sync_connection
        if (
            sync is None
            or self.poisoned
            or (not self.borrowable and asyncio.current_task() is not self.control_task)
            or connection.closed
            or connection.invalidated
            or sync.connection.driver_connection is not self.driver
        ):
            self.poisoned = True
            raise IdentityWriterUnavailable()

    def invalidated(self, dbapi: Any, record: Any, exception: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4."""
        if dbapi is self.dbapi:
            self.poisoned = True

    def checked_in(self, dbapi: Any, record: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4. Intercept only the returning original entry."""
        if dbapi is not self.dbapi or self.returned:
            return
        self.returned = True
        if not self.allow_return:
            self.poisoned = True
            if record.dbapi_connection is not self.dbapi:
                return
            record.invalidate()
        # This callback runs before the entry is queued. Never act on its
        # later checkins, even if the same healthy driver is leased again.
        self.disposed = True

    async def finish(self, client: Redis, pending: str | None) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. Settle before publication; discard uncertainty."""
        self.borrowable = False
        self.control_task = asyncio.current_task()
        failure: BaseException | None = None
        deadline = asyncio.timeout(5)
        try:
            async with deadline:
                await self.session.close()
                await self.connection.rollback()
                self.guard()
                if pending is not None:
                    result = await client.eval(
                        _PUBLISH, 1, FENCE_KEY, pending, f"stable:{uuid4().hex}"
                    )
                    if result not in (b"OK", "OK"):
                        raise IdentityWriterUnavailable()
                else:
                    # Acquisition or pending acknowledgement did not complete.
                    raise IdentityWriterUnavailable()
                unlocked = await self.connection.scalar(text("SELECT pg_advisory_unlock(2250220)"))
                if unlocked is not True:
                    raise IdentityWriterUnavailable()
                await self.connection.rollback()
                self.allow_return = True
                await self.connection.close()
        except BaseException as error:  # noqa: BLE001 — every uncertain lease must be discarded
            failure = IdentityWriterUnavailable() if deadline.expired() else error
            self.poisoned = True
            if not self.disposed:
                self.allow_return = False
                try:
                    async with asyncio.timeout(5):
                        await self.connection.invalidate()
                        await self.connection.close()
                except BaseException:  # noqa: BLE001 — quarantine even on cancellation
                    # No eventual recovery claim: retain the lease and its guards, including
                    # the exact-entry checkin interceptor, until process termination.
                    _QUARANTINE.append(self)
                    _LOG.error("identity writer cleanup failed phase=discard code=unconfirmed")
                    raise failure from None
        for target, name, callback in self.listeners:
            event.remove(target, name, callback)
        self.listeners.clear()
        if failure is not None:
            raise failure


def request_owned_session(request: Request) -> AsyncSession | None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-2. A present unavailable owner never falls back."""
    state = request.scope.get("state", {})
    if _OWNER_KEY not in state:
        return None
    owner = state[_OWNER_KEY]
    if not isinstance(owner, _WriterOwner) or not owner.borrowable:
        raise IdentityWriterUnavailable()
    owner.guard()
    return owner.session


@asynccontextmanager
async def mutation(
    factory: async_sessionmaker[AsyncSession],
    client: Redis,
    *,
    request: Request | None = None,
) -> AsyncIterator[AsyncSession]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-2/3/5/6."""
    if request is not None and _OWNER_KEY in request.scope.get("state", {}):
        raise IdentityWriterUnavailable()
    engine = factory.kw.get("bind")
    if not isinstance(engine, AsyncEngine):
        raise IdentityWriterUnavailable()
    connection = await engine.connect()
    owner = _WriterOwner(connection, factory(bind=connection))
    pending: str | None = None
    primary: BaseException | None = None
    try:
        await connection.execute(text("SELECT pg_advisory_lock(2250220)"))
        await connection.commit()
        token = f"pending:{uuid4().hex}"
        if await client.set(FENCE_KEY, token) is not True:
            raise IdentityWriterUnavailable()
        pending = token
        owner.borrowable = True
        if request is not None:
            request.scope.setdefault("state", {})[_OWNER_KEY] = owner
        yield owner.session
    except BaseException as error:
        primary = error
        raise
    finally:
        owner.borrowable = False
        # AnyIO protects against an enclosing ASGI cancel scope. The shielded task
        # also survives direct asyncio cancellation, within finish's two deadlines.
        with anyio.CancelScope(shield=True):
            cleanup = asyncio.create_task(owner.finish(client, pending))
            cancelled: asyncio.CancelledError | None = None
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError as error:
                    cancelled = error
                except BaseException:  # noqa: BLE001 — inspect the cleanup task below
                    break
            try:
                cleanup.result()
            except BaseException:
                if primary is None and cancelled is None:
                    raise
                _LOG.error("identity writer cleanup failed phase=settlement code=unavailable")
            if cancelled is not None and primary is None:
                raise cancelled


class IdentityFenceMiddleware:
    """@spec IDENTITY-WRITER-LIFETIME-V1-1/8. Own the entire awaited child lifetime."""

    def __init__(self, app: ASGIApp) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-1/8."""
        if scope["type"] != "http" or not protects(scope["method"], scope["path"]):
            await self.app(scope, receive, send)
            return
        request = Request(scope, receive)
        try:
            fenced = await prepare(request)
        except (TimeoutError, HTTPException) as error:
            status = error.status_code if isinstance(error, HTTPException) else 408
            detail = error.detail if isinstance(error, HTTPException) else "request body timeout"
            headers = {"Cache-Control": "private, no-store", "Referrer-Policy": "no-referrer"}
            if isinstance(error, HTTPException) and error.headers:
                headers.update(error.headers)
            await JSONResponse({"detail": detail}, status_code=status, headers=headers)(
                scope, receive, send
            )
            return
        body = await request.body()
        replayed = False

        async def replay() -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        if fenced:
            async with mutation(
                request.app.state.sessionmaker, request.app.state.valkey, request=request
            ):
                await self.app(scope, replay, send)
        else:
            await self.app(scope, replay, send)


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
