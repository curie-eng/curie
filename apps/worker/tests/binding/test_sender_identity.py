"""Which identity is bound at an address, against the real Postgres (ADR-0168 decision 6).

A channel-port identity writes as the address it is bound at, so this is how
the worker tells a sibling inbox from a person. Rows are namespaced by a
per-test token and removed afterwards.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")


async def _bind(
    engine: AsyncEngine,
    *,
    kind: str,
    address: str,
    adapter: str | None,
    endpoint: str | None,
) -> uuid.UUID:
    agent_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": f"sibling-{agent_id.hex[:12]}"},
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_channels "
                "(id, agent_id, kind, address, endpoint, adapter) "
                "VALUES (:id, :agent_id, :kind, :address, :endpoint, :adapter)"
            ),
            {
                "id": uuid.uuid4(),
                "agent_id": agent_id,
                "kind": kind,
                "address": address,
                "endpoint": endpoint,
                "adapter": adapter,
            },
        )
    return agent_id


async def _cleanup(engine: AsyncEngine, agent_ids: list[uuid.UUID]) -> None:
    async with engine.begin() as conn:
        for agent_id in agent_ids:
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agent_channels WHERE agent_id = :id"),
                {"id": agent_id},
            )
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": agent_id}
            )


def test_a_bound_address_names_the_identity_bound_there() -> None:
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        try:
            try:
                async with engine.connect():
                    pass
            except SQLAlchemyError as exc:
                pytest.skip(f"Postgres not reachable at {_DB_URL}: {exc}")
            token = uuid.uuid4().hex[:8]
            inbox = f"b-{token}@example.com"
            room = f"room-{token}"
            agents = [
                await _bind(
                    engine,
                    kind="email",
                    address=inbox,
                    adapter="mail-b",
                    endpoint="http://mail-b.invalid/",
                ),
                await _bind(engine, kind="webhook", address=room, adapter=None, endpoint=None),
            ]
            try:
                resolver = BindingResolver(engine, WorkerConfig(db_schema=_SCHEMA))
                assert await resolver.identity_for_address("email", inbox) == "mail-b"
                assert await resolver.identity_for_address("email", inbox.upper()) == "mail-b"
                # The kind is half of the key.
                assert await resolver.identity_for_address("webhook", inbox) is None
                # An address nobody is bound at is a person.
                assert await resolver.identity_for_address(
                    "email", f"c-{token}@example.com"
                ) is None
                # A route with no adapter names no identity.
                assert await resolver.identity_for_address("webhook", room) is None
            finally:
                await _cleanup(engine, agents)
        finally:
            await engine.dispose()

    asyncio.run(go())
