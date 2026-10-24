"""Eval connector authority comes from the same tenant-scoped agent policy."""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import pytest
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.binding import BindingResolver
from curie_worker.config import WorkerConfig
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.parametrize(
    "policy", [None, [], ["mcp__acme__change"], {}, [1], ["bad,tool"], [""], ["  "]]
)
def test_eval_connector_settings_preserve_policy_and_tenant_isolation(policy: object) -> None:
    async def go() -> None:
        engine = create_async_engine(
            os.environ.get(
                "TEST_DATABASE_URL",
                "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
            )
        )
        schema = f"test_eval_connector_{uuid.uuid4().hex}"
        tenant, agent = uuid.uuid4(), uuid.uuid4()
        created = False
        try:
            await pg_connect_or_skip(engine)
            async with engine.begin() as conn:
                await conn.execute(text(f"CREATE SCHEMA {schema}"))
                created = True
                await conn.execute(
                    text(
                        f"CREATE TABLE {schema}.agents "
                        "(id uuid PRIMARY KEY, tenant_id uuid NOT NULL, "
                        "name text NOT NULL, approval_required_tools jsonb)"
                    )
                )
                await conn.execute(
                    text(
                        f"INSERT INTO {schema}.agents VALUES "
                        "(:id, :tenant, :name, CAST(:policy AS jsonb))"
                    ),
                    {
                        "id": agent,
                        "tenant": tenant,
                        "name": "acme-agent",
                        "policy": json.dumps(policy),
                    },
                )
            config = WorkerConfig(db_schema=schema)
            resolver = BindingResolver(engine, config, tenant_id=tenant)
            other = BindingResolver(engine, config, tenant_id=uuid.uuid4())
            assert await other.eval_connector_settings_for(agent) is None
            assert await resolver.eval_connector_settings_for(uuid.uuid4()) is None
            if policy is None or policy == [] or policy == ["mcp__acme__change"]:
                assert await resolver.eval_connector_settings_for(agent) == (
                    "acme-agent",
                    policy or [],
                )
            else:
                with pytest.raises(ValueError, match="approval"):
                    await resolver.eval_connector_settings_for(agent)
        finally:
            if created:
                async with engine.begin() as conn:
                    await conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
            await engine.dispose()

    asyncio.run(go())
