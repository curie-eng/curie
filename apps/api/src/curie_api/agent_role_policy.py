"""Internal storage only. @spec AGENT-ROLE-PERSISTENCE-V1-1 to AGENT-ROLE-PERSISTENCE-V1-8."""

import uuid
from typing import Literal

from pydantic import TypeAdapter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from . import identity_audit, principal_authority
from .agent_role_filter import parse_agent_role_policy_v1, render_agent_role_policy_v1
from .models import Agent, AgentAccessPolicy
from .schemas.agent_access import (
    AgentRoleChangeV1,
    AgentRolePolicyV1,
    MigrateRoleV1,
    RoleFilterRefusal,
    SetRoleFilterV1,
)

_MAX_VERSION = 2_147_483_647
_CHANGE: TypeAdapter[AgentRoleChangeV1] = TypeAdapter(AgentRoleChangeV1)
Code = Literal[
    "invalid_role_policy_input",
    "invalid_role_policy_state",
    "role_policy_scope_unavailable",
    "stale_role_policy_version",
    "role_already_migrated",
    "role_policy_version_exhausted",
]


class RolePolicyRefusal(ValueError):
    """@spec AGENT-ROLE-PERSISTENCE-V1-7."""

    def __init__(self, code: Code) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-7."""
        if code not in {
            "invalid_role_policy_input",
            "invalid_role_policy_state",
            "role_policy_scope_unavailable",
            "stale_role_policy_version",
            "role_already_migrated",
            "role_policy_version_exhausted",
        }:
            code = "invalid_role_policy_input"
        self.code = code
        super().__init__(code)


def _entry(session: AsyncSession) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3. Check before begin_nested can preflush."""
    if (
        not session.in_transaction()
        or not session.is_active
        or session.new
        or session.dirty
        or session.deleted
    ):
        raise RolePolicyRefusal("invalid_role_policy_input") from None


def _snapshot(value: object, code: Code) -> AgentRolePolicyV1:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/5. The existing boundary owns the grammar and bound."""
    try:
        validated = AgentRolePolicyV1.model_validate(value)
        rendered = render_agent_role_policy_v1(validated)
        if isinstance(rendered, RoleFilterRefusal):
            raise ValueError
        detached = parse_agent_role_policy_v1(rendered)
        if isinstance(detached, RoleFilterRefusal):
            raise ValueError
        return detached
    except (ValueError, TypeError, AttributeError, RecursionError):
        raise RolePolicyRefusal(code) from None


def stored_snapshot(
    policy: AgentAccessPolicy | None,
    sql_null: bool,
    *,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
) -> AgentRolePolicyV1 | None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/6. SQL nullness is independently selected."""
    if policy is None:
        return None
    if (
        policy.tenant_id != tenant_id
        or policy.agent_id != agent_id
        or type(policy.version) is not int
        or not 1 <= policy.version <= _MAX_VERSION
    ):
        raise RolePolicyRefusal("invalid_role_policy_state") from None
    if sql_null:
        return None
    return _snapshot(
        {
            "schema_version": 1,
            "tenant_id": str(policy.tenant_id),
            "agent_id": str(policy.agent_id),
            "version": policy.version,
            "roles": policy.role_configuration,
        },
        "invalid_role_policy_state",
    )


async def lock_policy_rows(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
    require_active: bool,
) -> tuple[Agent | None, AgentAccessPolicy | None, bool]:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4. Outer callers own the prior total lock order."""
    with session.no_autoflush:
        try:
            await principal_authority.lock_scope(session, tenant_id, require_active=require_active)
        except principal_authority.PrincipalAuthorityRefusal:
            raise RolePolicyRefusal("role_policy_scope_unavailable") from None
        agent = await session.scalar(
            select(Agent)
            .where(Agent.id == agent_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if agent is None:
            return None, None, True
        if agent.tenant_id != tenant_id or (require_active and agent.status != "active"):
            raise RolePolicyRefusal("role_policy_scope_unavailable") from None
        row = (
            await session.execute(
                select(AgentAccessPolicy, AgentAccessPolicy.role_configuration.is_(None))
                .where(AgentAccessPolicy.agent_id == agent_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).one_or_none()
    return (agent, None, True) if row is None else (agent, row[0], row[1])


async def read_role_policy(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    agent_id: uuid.UUID,
) -> AgentRolePolicyV1 | None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/3/4."""
    _entry(session)
    if not isinstance(tenant_id, uuid.UUID) or not isinstance(agent_id, uuid.UUID):
        raise RolePolicyRefusal("invalid_role_policy_input") from None
    async with session.begin_nested():
        agent, policy, sql_null = await lock_policy_rows(
            session,
            tenant_id=tenant_id,
            agent_id=agent_id,
            require_active=False,
        )
        if agent is None:
            raise RolePolicyRefusal("role_policy_scope_unavailable") from None
        return stored_snapshot(policy, sql_null, tenant_id=tenant_id, agent_id=agent_id)


async def apply_role_change(
    session: AsyncSession,
    *,
    change: AgentRoleChangeV1,
) -> AgentRolePolicyV1:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/4/5/7/8."""
    _entry(session)
    try:
        if not isinstance(change, (SetRoleFilterV1, MigrateRoleV1)):
            raise ValueError
        change = _CHANGE.validate_python(change)
        tenant_id, agent_id = uuid.UUID(change.tenant_id), uuid.UUID(change.agent_id)
    except (ValueError, TypeError, AttributeError, RecursionError):
        raise RolePolicyRefusal("invalid_role_policy_input") from None
    async with session.begin_nested():
        agent, policy, sql_null = await lock_policy_rows(
            session,
            tenant_id=tenant_id,
            agent_id=agent_id,
            require_active=True,
        )
        if agent is None:
            raise RolePolicyRefusal("role_policy_scope_unavailable") from None
        previous = stored_snapshot(policy, sql_null, tenant_id=tenant_id, agent_id=agent_id)
        version = 0 if policy is None else policy.version
        if change.expected_version != version:
            raise RolePolicyRefusal("stale_role_policy_version") from None
        roles = {} if previous is None else previous.roles.model_dump(mode="json")
        old_role = roles.get(change.role)
        old_state = None if old_role is None else old_role["legacy_state"]
        if isinstance(change, MigrateRoleV1) and old_state == "migrated":
            raise RolePolicyRefusal("role_already_migrated") from None
        if version == _MAX_VERSION:
            raise RolePolicyRefusal("role_policy_version_exhausted") from None
        selected = dict(old_role or {"schema_version": 1, "legacy_state": "retained"})
        selected["filter"] = (
            None if change.filter is None else change.filter.model_dump(mode="json")
        )
        if isinstance(change, MigrateRoleV1):
            selected.update(
                legacy_state="migrated", raw_account_revocation=change.raw_account_revocation
            )
        roles[change.role] = selected
        proposed = _snapshot(
            {
                "schema_version": 1,
                "tenant_id": str(tenant_id),
                "agent_id": str(agent_id),
                "version": version + 1,
                "roles": roles,
            },
            "invalid_role_policy_input",
        )
        if policy is None:
            policy = AgentAccessPolicy(agent_id=agent_id, tenant_id=tenant_id, version=1)
            session.add(policy)
        else:
            policy.version = proposed.version
            policy.updated_at = func.now()
        policy.role_configuration = proposed.roles.model_dump(mode="json")
        identity_audit.record(
            session,
            tenant_id=tenant_id,
            event_type="identity.agent_role_policy_changed",
            actor="role_policy",
            details={
                "agent_id": str(agent_id),
                "role": change.role,
                "operation": change.operation,
                "previous_version": version,
                "version": proposed.version,
                "previous_legacy_state": old_state,
                "legacy_state": selected["legacy_state"],
            },
        )
        await session.flush()
        return proposed
