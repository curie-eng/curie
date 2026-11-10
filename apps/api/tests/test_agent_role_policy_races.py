"""Real lock schedules. @spec AGENT-ROLE-PERSISTENCE-V1-4/5/6/8/10."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from curie_api import agent_access, directory_service, directory_sync, principal_authority
from curie_api.crud import agents
from curie_api.models import Team
from curie_api.schemas.agent_access import AgentAccessPolicyWrite
from curie_api.schemas.directory import DirectoryConfigIn
from principal_adoption_support import selection
from role_policy_support import (
    ISSUER,
    api,
    audits,
    blocked,
    change,
    pid,
    refused,
    run,
    scope,
    seed,
    state,
)
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize(
    "first_role,second_role",
    [("invocation", "invocation"), ("invocation", "approver"), ("approver", "invocation")],
)
@pytest.mark.parametrize(
    "first_op,second_op", [("set_filter", "migrate_role"), ("migrate_role", "set_filter")]
)
def test_first_create_and_shared_cas_serialize_actual_waiters(
    clean_db: None, existing: bool, first_role: str, second_role: str, first_op: str, second_op: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4/5/10. Different roles do not merge stale edits."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4/5/10."""
        tenant, agent = await seed(f, policy=existing, roles={})
        initial = 1 if existing else 0
        async with f() as first, f() as second, f() as observer:
            first_pid, second_pid = await pid(first), await pid(second)
            await service.apply_role_change(
                first,
                change=change(
                    tenant, agent, version=initial, target=first_role, operation=first_op
                ),
            )
            task = asyncio.create_task(
                service.apply_role_change(
                    second,
                    change=change(
                        tenant, agent, version=initial, target=second_role, operation=second_op
                    ),
                )
            )
            try:
                await blocked(observer, second_pid, first_pid)
                await first.commit()
                with refused(service, "stale_role_policy_version"):
                    await task
                await second.commit()
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        async with f() as observer:
            stored = await state(observer, agent)
            assert stored["version"] == initial + 1
            assert set(stored["role_configuration"]) == {first_role}
            assert stored["role_configuration"][first_role]["legacy_state"] == (
                "migrated" if first_op == "migrate_role" else "retained"
            )
            assert len(await audits(observer, tenant)) == 1

    run(body)


@pytest.mark.parametrize("first", ["migration", "legacy"])
def test_migration_and_direct_legacy_writer_use_same_scope_order(
    clean_db: None, first: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4/6/10. A route guard alone cannot satisfy this race."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4/6/10."""
        tenant, agent = await seed(f)
        old_input = AgentAccessPolicyWrite(
            invocation_team_id=None, approver_team_id=None, expected_version=0
        )
        async with f() as new, f() as old, f() as observer:
            new_pid, old_pid = await pid(new), await pid(old)
            if first == "migration":
                await service.apply_role_change(
                    new, change=change(tenant, agent, operation="migrate_role")
                )
                task = asyncio.create_task(agent_access.replace_policy(old, agent, old_input))
                waiter, holder = old_pid, new_pid
            else:
                await scope(old, tenant)
                task = asyncio.create_task(
                    service.apply_role_change(
                        new, change=change(tenant, agent, operation="migrate_role")
                    )
                )
                waiter, holder = new_pid, old_pid
            try:
                await blocked(observer, waiter, holder)
                if first == "migration":
                    await new.commit()
                    with pytest.raises(
                        agent_access.PolicyConflict, match="^agent_role_policy_migrated$"
                    ):
                        await task
                    await old.commit()
                else:
                    result = await agent_access.replace_policy(old, agent, old_input)
                    assert result.version == 1
                    with refused(service, "stale_role_policy_version"):
                        await task
                    await new.commit()
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        async with f() as observer:
            assert (await state(observer, agent))["version"] == 1
            assert len(await audits(observer, tenant)) == (1 if first == "migration" else 0)

    run(body)


@pytest.mark.parametrize("first", ["role", "other"])
@pytest.mark.parametrize("writer", ["configuration", "adoption", "team"])
def test_role_and_existing_scope_writers_coexist_without_lock_cycle(
    clean_db: None, first: str, writer: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4/10. Both real orderings must complete."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4/10."""
        tenant, agent = await seed(f, policy=True, roles={}, authority=True, teams=writer == "team")
        async with f() as setup, setup.begin():
            group = await setup.scalar(select(Team).where(Team.tenant_id == tenant))
            if group:
                group.managed_by = "synthetic"
                await setup.flush()
                group_id = group.id
        async with f() as role_session, f() as other, f() as observer:
            role_pid, other_pid = await pid(role_session), await pid(other)
            # Existing outer global fence is owned by the composing old writer.
            await other.execute(text("SELECT pg_advisory_xact_lock(2250220)"))

            async def other_write() -> Any:
                """@spec AGENT-ROLE-PERSISTENCE-V1-4/10."""
                if writer == "configuration":
                    return await directory_service.update_configuration(
                        other,
                        tenant,
                        DirectoryConfigIn(
                            issuer=ISSUER,
                            governed_domains=["example.test"],
                            managed_by="synthetic",
                            expected_version=3,
                        ),
                    )
                if writer == "adoption":
                    return await principal_authority.adopt_http_authority(
                        other,
                        tenant_id=tenant,
                        expected_version=3,
                        expected_lifecycle_generation=0,
                        selection=selection(issuer=ISSUER),
                    )
                return await directory_sync.apply_snapshot(
                    other,
                    scope=directory_sync.DirectoryScope(
                        tenant, ISSUER, "synthetic", frozenset({group_id})
                    ),
                    team_id=group_id,
                    expected_epoch=0,
                    principal_ids=[],
                    complete=True,
                )

            if first == "role":
                await service.apply_role_change(
                    role_session, change=change(tenant, agent, version=1)
                )
                task = asyncio.create_task(other_write())
                waiter, holder = other_pid, role_pid
            else:
                await other_write()
                task = asyncio.create_task(
                    service.apply_role_change(role_session, change=change(tenant, agent, version=1))
                )
                waiter, holder = role_pid, other_pid
            try:
                await blocked(observer, waiter, holder)
                await (role_session.commit() if first == "role" else other.commit())
                await task
                await (other.commit() if first == "role" else role_session.commit())
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        async with f() as observer:
            assert (await state(observer, agent))["version"] == 2
            role_events = [
                row
                for row in await audits(observer, tenant)
                if row["event_type"] == "identity.agent_role_policy_changed"
            ]
            assert len(role_events) == 1

    run(body)


@pytest.mark.parametrize("operation", ["read", "apply", "legacy"])
@pytest.mark.parametrize("first", ["role", "delete"])
def test_agent_deletion_orders_preserve_committed_history_without_orphan_policy(
    clean_db: None, operation: str, first: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4/8/10. Delete must not erase committed audit history."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4/8/10."""
        tenant, agent = await seed(f, policy=True, roles={})
        async with f() as role_session, f() as deletion, f() as observer:
            role_pid, delete_pid = await pid(role_session), await pid(deletion)

            async def role_write() -> Any:
                """@spec AGENT-ROLE-PERSISTENCE-V1-4/8/10."""
                if operation == "read":
                    return await service.read_role_policy(
                        role_session, tenant_id=tenant, agent_id=agent
                    )
                if operation == "legacy":
                    return await agent_access.replace_policy(
                        role_session,
                        agent,
                        AgentAccessPolicyWrite(
                            invocation_team_id=None, approver_team_id=None, expected_version=1
                        ),
                    )
                return await service.apply_role_change(
                    role_session, change=change(tenant, agent, version=1)
                )

            if first == "delete":
                # Hold the real delete uncommitted; the production wrapper commits itself.
                await deletion.execute(text("DELETE FROM curie.agents WHERE id=:a"), {"a": agent})
                task = asyncio.create_task(role_write())
                waiter, holder = role_pid, delete_pid
            else:
                if operation == "legacy":
                    # Hold the real row until the other backend is observably waiting.
                    await scope(role_session, tenant)
                    await role_session.execute(
                        text("SELECT id FROM curie.agents WHERE id=:a FOR UPDATE"), {"a": agent}
                    )
                else:
                    await role_write()
                task = asyncio.create_task(agents.delete_agent(deletion, agent))
                waiter, holder = delete_pid, role_pid
            try:
                await blocked(observer, waiter, holder)
                if first == "delete":
                    await deletion.commit()
                    if operation == "legacy":
                        assert await task is None
                    else:
                        with refused(service, "role_policy_scope_unavailable"):
                            await task
                    await role_session.commit()
                else:
                    if operation == "legacy":
                        await role_write()
                    else:
                        await role_session.commit()
                    await task
            finally:
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        async with f() as observer:
            assert await state(observer, agent) is None
            assert len(await audits(observer, tenant)) == (
                1 if first == "role" and operation == "apply" else 0
            )

    run(body)


def test_role_entry_does_not_acquire_outer_global_identity_fence(clean_db: None) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4. Scope-only primitives cannot invert the outer fence."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4."""
        tenant, agent = await seed(f)
        async with f() as global_owner, global_owner.begin(), f() as writer, writer.begin():
            await global_owner.execute(text("SELECT pg_advisory_xact_lock(2250220)"))
            async with asyncio.timeout(3):
                assert (
                    await service.apply_role_change(writer, change=change(tenant, agent))
                ).version == 1
            assert await global_owner.scalar(text("SELECT pg_try_advisory_xact_lock(2250220)"))

    run(body)
