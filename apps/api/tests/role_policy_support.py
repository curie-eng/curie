"""Independent role storage fixtures. @spec AGENT-ROLE-PERSISTENCE-V1-1 to -10."""

from __future__ import annotations

import asyncio
import copy
import importlib
import uuid
from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import Any

from curie_api.config import get_settings
from curie_api.models import Agent, AgentAccessPolicy, Team, Tenant, TenantIdentityProvider
from curie_api.schemas.agent_access import MigrateRoleV1, SetRoleFilterV1
from principal_adoption_support import blocked, pid
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

MAX_VERSION = 2_147_483_647
ISSUER = "https://roles.example.test/directory"
SUBJECT = {"kind": "issuer_subject", "issuer": ISSUER, "subject": "SensitiveSubjectMarker"}


def api() -> ModuleType:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3. Missing behavior is a body assertion."""
    name = "curie_api.agent_role_policy"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "agent_role_policy persistence service is not implemented"
        raise
    for symbol in ("read_role_policy", "apply_role_change", "RolePolicyRefusal"):
        assert hasattr(module, symbol), f"missing role persistence interface: {symbol}"
    return module


def run(body: Callable[[async_sessionmaker[AsyncSession]], Awaitable[None]]) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-10. Follow the isolated database contract."""

    async def check() -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-10."""
        engine = create_async_engine(get_settings().database_url)
        try:
            async with asyncio.timeout(25):
                await body(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()

    asyncio.run(check())


def role(*, migrated: bool = False, filter: Any = None) -> dict[str, Any]:
    """@spec AGENT-ROLE-PERSISTENCE-V1-5. Literal independent role fixture."""
    result = {
        "schema_version": 1,
        "legacy_state": "migrated" if migrated else "retained",
        "filter": copy.deepcopy(filter),
    }
    if migrated:
        result["raw_account_revocation"] = "separate_removal_required"
    return result


def change(
    tenant: uuid.UUID,
    agent: uuid.UUID,
    *,
    version: int = 0,
    target: str = "invocation",
    operation: str = "set_filter",
    filter: Any = None,
) -> Any:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/5. Use existing typed input models."""
    data = {
        "schema_version": 1,
        "tenant_id": str(tenant),
        "agent_id": str(agent),
        "expected_version": version,
        "role": target,
        "operation": operation,
        "filter": copy.deepcopy(filter),
    }
    if operation == "migrate_role":
        return MigrateRoleV1(**data, raw_account_revocation="separate_removal_required")
    return SetRoleFilterV1(**data)


async def seed(
    factory: async_sessionmaker[AsyncSession],
    *,
    policy: bool = False,
    roles: Any = None,
    version: int = 1,
    teams: bool = False,
    tenant_status: str = "active",
    agent_status: str = "active",
    authority: bool = False,
) -> tuple[uuid.UUID, uuid.UUID]:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/4/5. Synthetic independently committed state."""
    async with factory() as s, s.begin():
        tenant = Tenant(deployment_id=f"synthetic-role-{uuid.uuid4()}", status=tenant_status)
        s.add(tenant)
        await s.flush()
        agent = Agent(
            tenant_id=tenant.id,
            name=f"synthetic-role-{uuid.uuid4()}",
            status=agent_status,
            approval_routes={"sensitive_tool": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE"},
                "approvers": {"users": ["U0EXAMPLE1"]},
            }},
        )
        s.add(agent)
        if authority:
            s.add(
                TenantIdentityProvider(
                    tenant_id=tenant.id,
                    issuer=ISSUER,
                    governed_domains=["example.test"],
                    managed_by="synthetic",
                    version=3,
                    identity_epoch=4,
                )
            )
        await s.flush()
        if policy:
            invocation, approver = None, None
            if teams:
                rows = [
                    Team(
                        tenant_id=tenant.id,
                        source="idp_group",
                        external_id=f"group-{i}",
                        name=f"synthetic-role-group-{uuid.uuid4()}",
                    )
                    for i in range(2)
                ]
                s.add_all(rows)
                await s.flush()
                invocation, approver = (row.id for row in rows)
            # Keep SQL NULL distinct from JSON null; test ORM serialization separately.
            await s.execute(
                text(
                    "INSERT INTO curie.agent_access_policies "
                    "(agent_id,tenant_id,invocation_team_id,approver_team_id,"
                    "version,role_configuration) "
                    "VALUES (:a,:t,:i,:p,:v,CAST(:r AS jsonb))"
                ),
                {
                    "a": agent.id,
                    "t": tenant.id,
                    "i": invocation,
                    "p": approver,
                    "v": version,
                    "r": json_value(roles),
                },
            )
        return tenant.id, agent.id


def json_value(value: Any) -> str | None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2. None means SQL NULL in fixture writes."""
    import json

    return None if value is None else json.dumps(value)


async def state(s: AsyncSession, agent: uuid.UUID) -> dict[str, Any] | None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/7. Independent stored-state observation."""
    row = (
        (
            await s.execute(
                text(
                    "SELECT agent_id,tenant_id,invocation_team_id,approver_team_id,"
                    "version,updated_at,"
                    "role_configuration,role_configuration IS NULL AS sql_null "
                    "FROM curie.agent_access_policies WHERE agent_id=:a"
                ),
                {"a": agent},
            )
        )
        .mappings()
        .one_or_none()
    )
    return dict(row) if row else None


async def audits(s: AsyncSession, tenant: uuid.UUID) -> list[dict[str, Any]]:
    """@spec AGENT-ROLE-PERSISTENCE-V1-8. No expected output uses implementation helpers."""
    return [
        dict(row)
        for row in (
            await s.execute(
                text(
                    "SELECT event_type,actor,principal_id,team_id,details "
                    "FROM curie.identity_audit_events "
                    "WHERE tenant_id=:t ORDER BY created_at,id"
                ),
                {"t": tenant},
            )
        ).mappings()
    ]


def refused(module: ModuleType, code: str) -> Any:
    """@spec AGENT-ROLE-PERSISTENCE-V1-7. Expected failure remains code-only."""
    import pytest

    return pytest.raises(module.RolePolicyRefusal, match=f"^{code}$")


async def scope(s: AsyncSession, tenant: uuid.UUID) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4. Prelock caller work in the real order."""
    from curie_api.principal_authority import lock_scope

    await s.execute(text("SELECT pg_advisory_xact_lock(2250220)"))
    await lock_scope(s, tenant, require_active=False)


__all__ = [
    "AgentAccessPolicy",
    "ISSUER",
    "MAX_VERSION",
    "SUBJECT",
    "api",
    "audits",
    "blocked",
    "change",
    "json_value",
    "pid",
    "refused",
    "role",
    "run",
    "scope",
    "seed",
    "state",
]
