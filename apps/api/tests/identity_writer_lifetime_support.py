"""Owned real connection observations. @spec IDENTITY-WRITER-LIFETIME-V1-3/4/10."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from curie_api.config import get_settings
from curie_api.models import Agent, Tenant
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session


def run(body: Callable[[], Awaitable[None]]) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Each scenario owns one event loop."""

    async def bounded() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        async with asyncio.timeout(35):
            await body()

    asyncio.run(bounded())


async def agents(factory: async_sessionmaker[AsyncSession], count: int = 2) -> list[uuid.UUID]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Independently committed synthetic rows."""
    async with factory() as session, session.begin():
        tenant = Tenant(deployment_id=f"writer-lifetime-{uuid.uuid4()}")
        session.add(tenant)
        await session.flush()
        rows = [
            Agent(
                tenant_id=tenant.id,
                name=f"writer-lifetime-{uuid.uuid4()}",
                model="synthetic-original",
            )
            for _ in range(count)
        ]
        session.add_all(rows)
        await session.flush()
        return [row.id for row in rows]


async def models(factory: async_sessionmaker[AsyncSession], ids: list[uuid.UUID]) -> list[str]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-10. Read through a separate real connection."""
    async with factory() as session:
        return [
            await session.scalar(select(Agent.model).where(Agent.id == identifier))
            for identifier in ids
        ]


async def blocking(observer: AsyncSession, waiter: int, holder: int) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3. Observe this scenario's actual wait edge."""
    async with asyncio.timeout(4):
        while True:
            if holder in await observer.scalar(
                text("SELECT pg_blocking_pids(:pid)"), {"pid": waiter}
            ):
                return
            await asyncio.sleep(0.01)


class ConnectionObserver:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3/4/10. Observe, never replace, real resources."""

    def __init__(self, engine: AsyncEngine, monkeypatch: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10."""
        self.engine = engine
        self.monkeypatch = monkeypatch
        self.sessions: list[AsyncSession] = []
        self.connections: list[AsyncConnection] = []
        self.transaction_sessions: dict[Any, Any] = {}
        self.lock_connections: list[Any] = []
        self.executions: list[tuple[Any, str]] = []
        self.commits: list[Any] = []

    def __enter__(self) -> ConnectionObserver:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Keep factory, get_session and SQL unchanged."""
        original_init = AsyncSession.__init__
        original_connect = AsyncEngine.connect
        observer = self

        def initialized(session: AsyncSession, *args: Any, **kwargs: Any) -> None:
            """@spec IDENTITY-WRITER-LIFETIME-V1-10. Retain the actual initialized session."""
            original_init(session, *args, **kwargs)
            observer.sessions.append(session)

        def connected(engine: AsyncEngine, *args: Any, **kwargs: Any) -> AsyncConnection:
            """@spec IDENTITY-WRITER-LIFETIME-V1-10. Return the unaltered real connection."""
            connection = original_connect(engine, *args, **kwargs)
            if engine is observer.engine:
                observer.connections.append(connection)
            return connection

        self.monkeypatch.setattr(AsyncSession, "__init__", initialized)
        self.monkeypatch.setattr(AsyncEngine, "connect", connected)
        event.listen(Session, "after_begin", self.began)
        event.listen(self.engine.sync_engine, "before_cursor_execute", self.executed)
        event.listen(self.engine.sync_engine, "commit", self.committed)
        return self

    def began(self, session: Session, transaction: Any, connection: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Public event pairs real instances."""
        if connection.engine is self.engine.sync_engine:
            self.transaction_sessions[connection] = session

    def executed(
        self,
        connection: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-3/10. Detect the real exclusive lock statement."""
        self.executions.append((connection, statement))
        normalized = statement.lower().replace(" ", "")
        if "pg_advisory_xact_lock(" in normalized or "pg_advisory_lock(" in normalized:
            self.lock_connections.append(connection)

    def committed(self, connection: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-3. Record actual transaction commit events."""
        self.commits.append(connection)

    async def lock_owner(self, index: int = 0) -> AsyncConnection:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Resolve only this observed owned lock."""
        target = self.lock_connections[index]
        for connection in self.connections:
            if connection.sync_connection is target:
                return connection
        synchronous = self.transaction_sessions[target]
        actual = next(session for session in self.sessions if session.sync_session is synchronous)
        return await actual.connection()

    def __exit__(self, *args: Any) -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-10. Remove only this observation's listeners."""
        event.remove(Session, "after_begin", self.began)
        event.remove(self.engine.sync_engine, "before_cursor_execute", self.executed)
        event.remove(self.engine.sync_engine, "commit", self.committed)


def headers() -> dict[str, str]:
    """@spec IDENTITY-WRITER-LIFETIME-V1-1. Follow the configured synthetic API key."""
    return {"X-API-Key": get_settings().api_key}
