"""Agents, bindings, versions and deployments are tenant-scoped (#2911).

Migration 0101 gives ``agents``, ``agent_channels``, ``agent_versions`` and
``deployments`` a ``tenant_id``; gives ``agents`` a lifecycle ``status``, an
optional owning team, opaque policy refs and a ``(tenant_id, id)`` key; and
gives ``agent_channels`` an optional topic. The owning team is referenced
through a composite, tenant-scoped key, so an agent can never be owned by
another tenant's team; the ``(tenant_id, id)`` key lets a later reference name
a bot together with its tenant, so it can never name another tenant's bot.

Constraint tests run inside an outer transaction that is always rolled back
(an insert expected to fail runs in a SAVEPOINT). The status walk and the crud
tests need real commits, so they clean up the rows and tenants they create.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, async_sessionmaker, create_async_engine

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_UUID = uuid.UUID(DEFAULT_TENANT_ID)
MARK = "bot-identity-"


def _rolled_back(body: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
    """Run ``body`` in a transaction that is always rolled back."""

    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                trans = await conn.begin()
                try:
                    return await body(conn)
                finally:
                    await trans.rollback()
        finally:
            await engine.dispose()

    return asyncio.run(run())


async def _exec(
    conn: AsyncConnection, statement: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    result = await conn.execute(text(statement), params or {})
    if not result.returns_rows:
        return []
    return [dict(row) for row in result.mappings().all()]


async def _expect_integrity_error(
    conn: AsyncConnection, statement: str, params: dict[str, Any], *, constraint: str
) -> None:
    """Assert the statement fails on exactly ``constraint``, not any IntegrityError."""
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(statement), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    assert getattr(cause, "constraint_name", None) == constraint, str(exc_info.value)


async def _insert_tenant(conn: AsyncConnection) -> uuid.UUID:
    tenant_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.tenants (id, deployment_id, status) "
        "VALUES (:id, gen_random_uuid()::text, 'active')",
        {"id": tenant_id},
    )
    return tenant_id


async def _insert_agent(conn: AsyncConnection) -> uuid.UUID:
    """An agent with no tenant column named: the server default must apply."""
    agent_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"{MARK}{agent_id}"},
    )
    return agent_id


async def _insert_team(conn: AsyncConnection, tenant_id: uuid.UUID) -> uuid.UUID:
    team_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.teams (id, tenant_id, source, name) "
        "VALUES (:id, :tenant_id, 'curie_managed', :name)",
        {"id": team_id, "tenant_id": tenant_id, "name": f"{MARK}{team_id}"},
    )
    return team_id


async def _delete_agents_and_tenants(
    agent_ids: list[uuid.UUID], tenant_ids: list[uuid.UUID]
) -> None:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.begin() as conn:
            for agent_id in agent_ids:
                for table in ("deployments", "agent_versions", "agent_channels"):
                    await conn.execute(
                        text(f"DELETE FROM curie.{table} WHERE agent_id = :id"), {"id": agent_id}
                    )
                await conn.execute(
                    text("DELETE FROM curie.agents WHERE id = :id"), {"id": agent_id}
                )
            for tenant_id in tenant_ids:
                await conn.execute(
                    text("DELETE FROM curie.tenants WHERE id = :id"), {"id": tenant_id}
                )
    finally:
        await engine.dispose()


# --- ORM shape ---------------------------------------------------------------


def test_agent_status_enum_has_exactly_the_four_lifecycle_states() -> None:
    from curie_api.models import AgentStatus

    assert {member.value for member in AgentStatus} == {"active", "paused", "draining", "retired"}


def test_orm_models_carry_the_tenant_scope_columns() -> None:
    from curie_api.models import Agent, AgentChannel, AgentVersion, Deployment

    agent_columns = Agent.__table__.c
    assert agent_columns["tenant_id"].nullable is False
    assert {fk.target_fullname for fk in agent_columns["tenant_id"].foreign_keys} >= {
        "curie.tenants.id"
    }
    assert agent_columns["status"].nullable is False
    for name in (
        "owning_team_id",
        "topic_policy_ref",
        "data_classification_ref",
        "retention_policy_ref",
    ):
        assert agent_columns[name].nullable is True

    channel_columns = AgentChannel.__table__.c
    assert channel_columns["tenant_id"].nullable is False
    assert "provider_installation_id" not in channel_columns
    assert channel_columns["topic_id"].nullable is True

    assert AgentVersion.__table__.c["tenant_id"].nullable is False
    assert Deployment.__table__.c["tenant_id"].nullable is False


# --- status lifecycle ----------------------------------------------------------


def test_agent_status_walks_the_full_lifecycle(migrated: None) -> None:
    from curie_api.models import Agent, AgentStatus

    async def run() -> list[str]:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        agent_id = uuid.uuid4()
        seen: list[str] = []
        try:
            async with sessionmaker() as session:
                agent = Agent(id=agent_id, name=f"{MARK}{agent_id}")
                session.add(agent)
                await session.commit()
                await session.refresh(agent)
                seen.append(agent.status)
                for status in (
                    AgentStatus.paused,
                    AgentStatus.draining,
                    AgentStatus.retired,
                ):
                    agent.status = status
                    await session.commit()
                    async with engine.connect() as conn:
                        (row,) = await _exec(
                            conn, "SELECT status FROM curie.agents WHERE id = :id", {"id": agent_id}
                        )
                    seen.append(row["status"])
        finally:
            await engine.dispose()
            await _delete_agents_and_tenants([agent_id], [])
        return seen

    assert asyncio.run(run()) == ["active", "paused", "draining", "retired"]


def test_unknown_agent_status_is_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        agent_id = await _insert_agent(conn)
        await _expect_integrity_error(
            conn,
            "UPDATE curie.agents SET status = 'deleted' WHERE id = :id",
            {"id": agent_id},
            constraint="agents_status_ck",
        )

    _rolled_back(body)


# --- tenant-scoped cross-table references ------------------------------------


def test_owning_team_must_belong_to_the_agents_tenant(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        other_tenant = await _insert_tenant(conn)
        foreign_team = await _insert_team(conn, other_tenant)
        own_team = await _insert_team(conn, DEFAULT_TENANT_UUID)
        agent_id = await _insert_agent(conn)
        await _expect_integrity_error(
            conn,
            "UPDATE curie.agents SET owning_team_id = :team WHERE id = :id",
            {"team": foreign_team, "id": agent_id},
            constraint="agents_owning_team_fkey",
        )
        await _exec(
            conn,
            "UPDATE curie.agents SET owning_team_id = :team WHERE id = :id",
            {"team": own_team, "id": agent_id},
        )
        return await _exec(
            conn,
            "SELECT tenant_id, owning_team_id FROM curie.agents WHERE id = :id",
            {"id": agent_id},
        )

    (row,) = _rolled_back(body)
    assert row["tenant_id"] == DEFAULT_TENANT_UUID
    assert row["owning_team_id"] is not None


def test_agent_tenant_id_key_refuses_a_reference_naming_another_tenant(
    migrated: None,
) -> None:
    """A reference through ``agents_tenant_id_id_key`` must carry the agent's tenant.

    No table references the key at 0101, so a probe table (created inside the
    rolled-back transaction) stands in for the later identity link.
    """
    probe = f"curie.bot_identity_probe_{uuid.uuid4().hex[:12]}"
    insert = f"INSERT INTO {probe} (id, tenant_id, agent_id) VALUES (:id, :tenant, :agent)"

    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        await _exec(
            conn,
            f"CREATE TABLE {probe} (id uuid PRIMARY KEY, tenant_id uuid NOT NULL, "
            "agent_id uuid NOT NULL, CONSTRAINT bot_identity_probe_agent_fkey "
            "FOREIGN KEY (tenant_id, agent_id) REFERENCES curie.agents (tenant_id, id))",
        )
        other_tenant = await _insert_tenant(conn)
        agent_id = await _insert_agent(conn)
        await _expect_integrity_error(
            conn,
            insert,
            {"id": uuid.uuid4(), "tenant": other_tenant, "agent": agent_id},
            constraint="bot_identity_probe_agent_fkey",
        )
        await _exec(
            conn, insert, {"id": uuid.uuid4(), "tenant": DEFAULT_TENANT_UUID, "agent": agent_id}
        )
        return await _exec(conn, f"SELECT tenant_id, agent_id FROM {probe}")

    rows = _rolled_back(body)
    assert len(rows) == 1
    assert rows[0]["tenant_id"] == DEFAULT_TENANT_UUID


# --- crud create paths put children in the agent's tenant --------------------


async def _tenants_of(agent_id: uuid.UUID) -> dict[str, set[uuid.UUID]]:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.connect() as conn:
            out: dict[str, set[uuid.UUID]] = {}
            for table, column in (
                ("agents", "id"),
                ("agent_channels", "agent_id"),
                ("agent_versions", "agent_id"),
                ("deployments", "agent_id"),
            ):
                rows = await _exec(
                    conn,
                    f"SELECT tenant_id FROM curie.{table} WHERE {column} = :id",
                    {"id": agent_id},
                )
                out[table] = {row["tenant_id"] for row in rows}
            return out
    finally:
        await engine.dispose()


def _slack_channel_id() -> str:
    # The write schema only accepts real Slack channel IDs (C0123ABCD shape).
    return f"C{uuid.uuid4().hex[:12].upper()}"


def test_crud_create_paths_keep_children_in_the_agents_tenant(migrated: None) -> None:
    from curie_api.crud import agents as crud_agents
    from curie_api.crud import channels as crud_channels
    from curie_api.crud import deployments as crud_deployments
    from curie_api.crud import versions as crud_versions
    from curie_api.models import Environment
    from curie_api.schemas.agents import AgentCreate
    from curie_api.schemas.channels import ChannelBindingWrite

    async def run() -> tuple[
        dict[str, set[uuid.UUID]],
        dict[str, set[uuid.UUID]],
        uuid.UUID,
        dict[str, uuid.UUID],
        dict[str, uuid.UUID],
    ]:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        agent_ids: list[uuid.UUID] = []
        tenant_ids: list[uuid.UUID] = []
        try:
            async with sessionmaker() as session:
                agent = await crud_agents.create_agent(
                    session,
                    AgentCreate(
                        name=f"{MARK}{uuid.uuid4().hex[:8]}",
                        channel=ChannelBindingWrite(kind="slack", address=_slack_channel_id()),
                    ),
                )
                agent_ids.append(agent.id)
                version = await crud_versions.create_version_row(
                    session, agent.id, version_label="v1", created_by="test"
                )
                deployment = await crud_deployments.create_deployment_row(
                    session, agent.id, version.id, Environment.dev
                )
                # Read straight off the returned ORM objects, inside the async
                # session: an expired attribute here would raise MissingGreenlet.
                returned_default = {
                    "agents": agent.tenant_id,
                    "agent_channels": agent.channels[0].tenant_id,
                    "agent_versions": version.tenant_id,
                    "deployments": deployment.tenant_id,
                }
            in_default = await _tenants_of(agent.id)

            # Move the agent to a second tenant; every child created afterwards
            # must follow the AGENT's tenant, not the server default.
            async with engine.begin() as conn:
                second = await _insert_tenant(conn)
                tenant_ids.append(second)
                await _exec(
                    conn,
                    "UPDATE curie.agents SET tenant_id = :tenant WHERE id = :id",
                    {"tenant": second, "id": agent.id},
                )
                await _exec(
                    conn, "DELETE FROM curie.deployments WHERE agent_id = :id", {"id": agent.id}
                )
                await _exec(
                    conn, "DELETE FROM curie.agent_versions WHERE agent_id = :id", {"id": agent.id}
                )
                await _exec(
                    conn, "DELETE FROM curie.agent_channels WHERE agent_id = :id", {"id": agent.id}
                )

            async with sessionmaker() as session:
                binding = await crud_channels.add_channel_binding(
                    session,
                    agent.id,
                    ChannelBindingWrite(kind="slack", address=_slack_channel_id()),
                )
                binding_tenant = binding.tenant_id
                await session.commit()
                version = await crud_versions.create_version_row(
                    session, agent.id, version_label="v2", created_by="test"
                )
                deployment = await crud_deployments.create_deployment_row(
                    session, agent.id, version.id, Environment.dev
                )
                returned_second = {
                    "agent_channels": binding_tenant,
                    "agent_versions": version.tenant_id,
                    "deployments": deployment.tenant_id,
                }
            in_second = await _tenants_of(agent.id)
            return in_default, in_second, second, returned_default, returned_second
        finally:
            await engine.dispose()
            await _delete_agents_and_tenants(agent_ids, tenant_ids)

    in_default, in_second, second, returned_default, returned_second = asyncio.run(run())
    assert returned_default == {
        "agents": DEFAULT_TENANT_UUID,
        "agent_channels": DEFAULT_TENANT_UUID,
        "agent_versions": DEFAULT_TENANT_UUID,
        "deployments": DEFAULT_TENANT_UUID,
    }
    assert returned_second == {
        "agent_channels": second,
        "agent_versions": second,
        "deployments": second,
    }
    assert in_default == {
        "agents": {DEFAULT_TENANT_UUID},
        "agent_channels": {DEFAULT_TENANT_UUID},
        "agent_versions": {DEFAULT_TENANT_UUID},
        "deployments": {DEFAULT_TENANT_UUID},
    }
    assert in_second == {
        "agents": {second},
        "agent_channels": {second},
        "agent_versions": {second},
        "deployments": {second},
    }
