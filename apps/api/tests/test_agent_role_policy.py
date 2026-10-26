"""Independent role storage outcomes. @spec AGENT-ROLE-PERSISTENCE-V1-1 to -8."""

from __future__ import annotations

import copy
import json
import uuid
from typing import Any

import pytest
from curie_api import agent_access
from curie_api.models import Agent, AgentAccessPolicy, Tenant, TenantIdentityProvider
from curie_api.schemas.agent_access import AgentAccessPolicyWrite, SetRoleFilterV1
from role_policy_support import (
    MAX_VERSION,
    SUBJECT,
    api,
    audits,
    change,
    refused,
    role,
    run,
    scope,
    seed,
    state,
)
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.parametrize(
    "roles",
    [
        None,
        {},
        {"invocation": role()},
        {"approver": role(filter={"entries": []})},
        {"invocation": role(filter={"entries": [SUBJECT]}), "approver": role(migrated=True)},
    ],
)
def test_read_preserves_sql_null_and_every_present_role_shape(clean_db: None, roles: Any) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/3. Absence cannot collapse present empty roles."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-2/3."""
        tenant, agent = await seed(f, policy=True, roles=roles, version=7)
        async with f() as s, s.begin():
            result = await service.read_role_policy(s, tenant_id=tenant, agent_id=agent)
            stored = await state(s, agent)
            assert stored["sql_null"] is (roles is None)
            if roles is None:
                assert result is None
            else:
                assert result.model_dump(mode="json") == {
                    "schema_version": 1,
                    "tenant_id": str(tenant),
                    "agent_id": str(agent),
                    "version": 7,
                    "roles": roles,
                }
            assert s.in_transaction()
        absent_tenant, absent_agent = await seed(f)
        async with f() as s, s.begin():
            assert (
                await service.read_role_policy(s, tenant_id=absent_tenant, agent_id=absent_agent)
                is None
            )

    run(body)


@pytest.mark.parametrize("target", ["invocation", "approver"])
@pytest.mark.parametrize("operation", ["set_filter", "migrate_role"])
@pytest.mark.parametrize("filter", [None, {"entries": []}, {"entries": [SUBJECT]}])
@pytest.mark.parametrize("existing", [False, True])
def test_single_role_change_uses_one_counter_and_preserves_other_role_and_teams(
    clean_db: None, target: str, operation: str, filter: Any, existing: bool
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-5/8. Role writes must not clear legacy narrowing."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-5/8."""
        other = "approver" if target == "invocation" else "invocation"
        original_roles = {other: role(filter={"entries": [SUBJECT]})}
        tenant, agent = await seed(
            f, policy=existing, roles=original_roles, teams=existing, authority=True, version=4
        )
        async with f() as s:
            before = await state(s, agent)
            routes = copy.deepcopy((await s.get(Agent, agent)).approval_routes)
            authority_before = (await s.get(TenantIdentityProvider, tenant)).__dict__.copy()
        async with f() as s, s.begin():
            result = await service.apply_role_change(
                s,
                change=change(
                    tenant,
                    agent,
                    version=4 if existing else 0,
                    target=target,
                    operation=operation,
                    filter=filter,
                ),
            )
            assert result.version == (5 if existing else 1)
            expected = dict(original_roles) if existing else {}
            expected[target] = role(migrated=operation == "migrate_role", filter=filter)
            assert result.roles.model_dump(mode="json") == expected
            during = await state(s, agent)
            assert during["role_configuration"] == expected
            assert during["version"] == (5 if existing else 1)
            if before:
                assert (during["invocation_team_id"], during["approver_team_id"]) == (
                    before["invocation_team_id"],
                    before["approver_team_id"],
                )
            else:
                assert during["invocation_team_id"] is during["approver_team_id"] is None
            assert (await s.get(Agent, agent)).approval_routes == routes
            current_authority = await s.get(TenantIdentityProvider, tenant)
            for field in ("version", "identity_epoch", "lifecycle_generation", "http_selection"):
                assert getattr(current_authority, field) == authority_before[field]
            records = await audits(s, tenant)
            assert records == [
                {
                    "event_type": "identity.agent_role_policy_changed",
                    "actor": "role_policy",
                    "principal_id": None,
                    "team_id": None,
                    "details": {
                        "agent_id": str(agent),
                        "role": target,
                        "operation": operation,
                        "previous_version": 4 if existing else 0,
                        "version": 5 if existing else 1,
                        "previous_legacy_state": None,
                        "legacy_state": "migrated" if operation == "migrate_role" else "retained",
                    },
                }
            ]
            assert "SensitiveSubjectMarker" not in json.dumps(records)

    run(body)


@pytest.mark.parametrize("finish", ["commit", "rollback"])
def test_success_flushes_without_committing_and_event_shares_outer_transaction(
    clean_db: None, finish: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/7/8. A successful flush is not publication."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-3/7/8."""
        tenant, agent = await seed(f)
        async with f() as s:
            await s.begin()
            await scope(s, tenant)
            outer = Tenant(deployment_id=f"outer-role-{uuid.uuid4()}")
            s.add(outer)
            await s.flush()
            await service.apply_role_change(s, change=change(tenant, agent))
            assert s.in_transaction()
            async with f() as observer:
                assert await state(observer, agent) is None
                assert await audits(observer, tenant) == []
                assert await observer.get(Tenant, outer.id) is None
            if finish == "commit":
                await s.commit()
            else:
                await s.rollback()
        async with f() as observer:
            assert (await state(observer, agent) is not None) is (finish == "commit")
            assert len(await audits(observer, tenant)) == (1 if finish == "commit" else 0)
            assert (await observer.get(Tenant, outer.id) is not None) is (finish == "commit")

    run(body)


@pytest.mark.parametrize("entry", ["read", "apply"])
@pytest.mark.parametrize("pending", ["new", "dirty", "deleted", "no_transaction", "failed"])
def test_new_entries_refuse_unclean_caller_before_any_io(
    clean_db: None, entry: str, pending: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/7. Savepoint entry must not autoflush caller work."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-3/7."""
        tenant, agent = await seed(f)
        async with f() as s:
            if pending != "no_transaction":
                await s.begin()
            if pending == "new":
                s.add(Tenant(deployment_id=f"pending-role-{uuid.uuid4()}"))
            elif pending in {"dirty", "deleted"}:
                row = await s.get(Agent, agent)
                if pending == "dirty":
                    row.name = f"pending-role-{uuid.uuid4()}"
                else:
                    await s.delete(row)
            elif pending == "failed":
                s.add(Tenant(id=tenant, deployment_id=f"duplicate-role-{uuid.uuid4()}"))
                with pytest.raises(IntegrityError):
                    await s.flush()
            before_sets = (set(s.new), set(s.dirty), set(s.deleted))
            observed = []
            engine = s.bind.sync_engine

            def query(*args: Any) -> None:
                """@spec AGENT-ROLE-PERSISTENCE-V1-3. Observe real SQL, never replace it."""
                observed.append("query")

            def flush(*args: Any) -> None:
                """@spec AGENT-ROLE-PERSISTENCE-V1-3."""
                observed.append("flush")

            event.listen(engine, "before_cursor_execute", query)
            event.listen(s.sync_session, "before_flush", flush)
            try:
                with refused(service, "invalid_role_policy_input"):
                    if entry == "read":
                        await service.read_role_policy(s, tenant_id=tenant, agent_id=agent)
                    else:
                        await service.apply_role_change(s, change=change(tenant, agent))
                assert observed == []
                assert (set(s.new), set(s.dirty), set(s.deleted)) == before_sets
            finally:
                event.remove(engine, "before_cursor_execute", query)
                event.remove(s.sync_session, "before_flush", flush)
                await s.rollback()

    run(body)


@pytest.mark.parametrize(
    "bad", ["mapping", "bytes", "constructed", "nested_mutation", "uuid_string"]
)
def test_typed_input_is_revalidated_without_secret_echo(clean_db: None, bad: str) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/7. Constructed models cannot bypass validation."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-3/7."""
        tenant, agent = await seed(f)
        candidate = change(tenant, agent, filter={"entries": [SUBJECT]})
        if bad == "mapping":
            candidate = candidate.model_dump()
        elif bad == "bytes":
            candidate = b"SensitiveInputMarker"
        elif bad == "constructed":
            candidate = SetRoleFilterV1.model_construct(
                **{**candidate.model_dump(), "expected_version": True}
            )
        elif bad == "nested_mutation":
            candidate.filter.entries.append({"secret": "SensitiveInputMarker"})
        async with f() as s, s.begin():
            with refused(service, "invalid_role_policy_input") as caught:
                if bad == "uuid_string":
                    await service.read_role_policy(s, tenant_id=str(tenant), agent_id=agent)
                else:
                    await service.apply_role_change(s, change=candidate)
            error = caught.value
            assert error.args == ("invalid_role_policy_input",)
            for value in (str(error), repr(error), repr(error.args)):
                assert "Sensitive" not in value and str(tenant) not in value
            assert error.__cause__ is None and error.__suppress_context__
            assert await state(s, agent) is None
            assert await audits(s, tenant) == []

    run(body)


@pytest.mark.parametrize("operation", ["set_filter", "migrate_role"])
def test_equal_filter_advances_once_and_set_preserves_migrated_disclosure(
    clean_db: None, operation: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-5/8. Equal data is still a versioned operation."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-5/8."""
        tenant, agent = await seed(f)
        async with f() as s, s.begin():
            await service.apply_role_change(s, change=change(tenant, agent, operation=operation))
            result = await service.apply_role_change(s, change=change(tenant, agent, version=1))
            assert result.version == 2
            assert result.roles.model_dump(mode="json") == {
                "invocation": role(migrated=operation == "migrate_role")
            }
            assert len(await audits(s, tenant)) == 2

    run(body)


@pytest.mark.parametrize("target", ["invocation", "approver"])
@pytest.mark.parametrize("mismatch", [False, True])
def test_migration_repeat_and_stale_cas_leave_state_untouched(
    clean_db: None, target: str, mismatch: bool
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-5/7. CAS precedes repeated-migration refusal."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-5/7."""
        tenant, agent = await seed(f, policy=True, roles={target: role(migrated=True)}, version=4)
        async with f() as s:
            before = await state(s, agent)
        async with f() as s, s.begin():
            with refused(
                service, "stale_role_policy_version" if mismatch else "role_already_migrated"
            ):
                await service.apply_role_change(
                    s,
                    change=change(
                        tenant,
                        agent,
                        version=3 if mismatch else 4,
                        target=target,
                        operation="migrate_role",
                    ),
                )
            assert await state(s, agent) == before
            assert await audits(s, tenant) == []

    run(body)


@pytest.mark.parametrize(
    "invalid",
    [
        "json_null",
        "scalar",
        "array",
        "unknown_role",
        "version",
        "role_null",
        "unsupported",
        "counter",
        "oversize",
        "sql_null_counter",
    ],
)
@pytest.mark.parametrize("entry", ["read", "apply", "legacy"])
def test_corrupted_stored_state_refuses_before_cas_without_repair(
    clean_db: None, invalid: str, entry: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/6/7. Corruption never becomes absent state."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-2/6/7."""
        tenant, agent = await seed(f, policy=True, roles={})
        async with f() as s, s.begin():
            # Transaction-local corruption; DDL and invalid row roll back together.
            await s.execute(
                text(
                    "ALTER TABLE curie.agent_access_policies DROP CONSTRAINT "
                    "agent_access_policies_role_object_ck"
                )
            )
            await s.execute(
                text(
                    "ALTER TABLE curie.agent_access_policies DROP CONSTRAINT "
                    "agent_access_policies_version_ck"
                )
            )
            value: Any = {"unknown_role": {}}
            if invalid == "json_null":
                value = None
            elif invalid == "scalar":
                value = 4
            elif invalid == "array":
                value = []
            elif invalid == "role_null":
                value = {"invocation": None}
            elif invalid == "unsupported":
                value = {"invocation": {**role(), "schema_version": 2}}
            elif invalid == "version":
                value = {"version": 1}
            elif invalid in {"counter", "sql_null_counter"}:
                value = {}
            elif invalid == "oversize":
                value = {
                    "invocation": role(
                        filter={
                            "entries": [
                                {**SUBJECT, "subject": f"{i}-" + "x" * 4096} for i in range(20)
                            ]
                        }
                    )
                }
            await s.execute(
                text(
                    "UPDATE curie.agent_access_policies SET role_configuration=CAST(:r AS jsonb),"
                    "version=:v WHERE agent_id=:a"
                ),
                {
                    "a": agent,
                    "r": None if invalid == "sql_null_counter" else json.dumps(value),
                    "v": 0 if invalid in {"counter", "sql_null_counter"} else 1,
                },
            )
            before = await state(s, agent)
            if entry == "legacy":
                with pytest.raises(
                    agent_access.PolicyConflict, match="^invalid_role_policy_state$"
                ):
                    await agent_access.replace_policy(
                        s,
                        agent,
                        AgentAccessPolicyWrite(
                            invocation_team_id=None, approver_team_id=None, expected_version=0
                        ),
                    )
            else:
                with refused(service, "invalid_role_policy_state"):
                    if entry == "read":
                        await service.read_role_policy(s, tenant_id=tenant, agent_id=agent)
                    else:
                        await service.apply_role_change(s, change=change(tenant, agent, version=0))
            assert await state(s, agent) == before
            assert await audits(s, tenant) == []
            await s.rollback()

    run(body)


@pytest.mark.parametrize("sql_null", [False, True])
@pytest.mark.parametrize("entry", ["read", "apply", "legacy"])
def test_wrong_tenant_policy_is_found_and_refused_even_when_sql_null(
    clean_db: None, sql_null: bool, entry: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/6. Tenant predicates must not conceal corruption."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-2/6."""
        tenant, agent = await seed(f, policy=True, roles=None if sql_null else {})
        foreign, _ = await seed(f)
        async with f() as s, s.begin():
            await s.execute(
                text(
                    "ALTER TABLE curie.agent_access_policies DROP CONSTRAINT "
                    "agent_access_policies_agent_fkey"
                )
            )
            await s.execute(
                text("UPDATE curie.agent_access_policies SET tenant_id=:t WHERE agent_id=:a"),
                {"t": foreign, "a": agent},
            )
            before = await state(s, agent)
            if entry == "legacy":
                with pytest.raises(
                    agent_access.PolicyConflict, match="^invalid_role_policy_state$"
                ):
                    await agent_access.replace_policy(
                        s,
                        agent,
                        AgentAccessPolicyWrite(
                            invocation_team_id=None, approver_team_id=None, expected_version=0
                        ),
                    )
            else:
                with refused(service, "invalid_role_policy_state"):
                    if entry == "read":
                        await service.read_role_policy(s, tenant_id=tenant, agent_id=agent)
                    else:
                        await service.apply_role_change(s, change=change(tenant, agent))
            assert await state(s, agent) == before
            assert await audits(s, tenant) == []
            await s.rollback()

    run(body)


@pytest.mark.parametrize("target", ["tenant", "agent"])
def test_inactive_scope_remains_readable_but_not_mutable(clean_db: None, target: str) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4. Read inactivity is not write permission."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4."""
        tenant, agent = await seed(
            f,
            policy=True,
            roles={},
            tenant_status="suspended" if target == "tenant" else "active",
            agent_status="paused" if target == "agent" else "active",
        )
        async with f() as s, s.begin():
            assert (
                await service.read_role_policy(s, tenant_id=tenant, agent_id=agent)
            ).version == 1
            with refused(service, "role_policy_scope_unavailable"):
                await service.apply_role_change(s, change=change(tenant, agent, version=1))
            assert await audits(s, tenant) == []

    run(body)


@pytest.mark.parametrize("bad", ["tenant", "agent", "mismatch"])
def test_missing_or_wrong_agent_scope_does_not_return_absence(clean_db: None, bad: str) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/4."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-3/4."""
        tenant, agent = await seed(f)
        foreign, _ = await seed(f)
        requested_tenant = (
            uuid.uuid4() if bad == "tenant" else foreign if bad == "mismatch" else tenant
        )
        requested_agent = uuid.uuid4() if bad == "agent" else agent
        async with f() as s, s.begin():
            with refused(service, "role_policy_scope_unavailable"):
                await service.read_role_policy(
                    s, tenant_id=requested_tenant, agent_id=requested_agent
                )
            with refused(service, "role_policy_scope_unavailable"):
                await service.apply_role_change(s, change=change(requested_tenant, requested_agent))
            assert await state(s, agent) is None

    run(body)


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("entry", ["apply", "legacy"])
def test_maximum_counter_is_readable_and_refuses_increment_after_cas(
    clean_db: None, stale: bool, entry: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-6/7. SQL Integer cannot wrap/reset."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-6/7."""
        tenant, agent = await seed(f, policy=True, roles={}, version=MAX_VERSION)
        async with f() as s, s.begin():
            assert (
                await service.read_role_policy(s, tenant_id=tenant, agent_id=agent)
            ).version == MAX_VERSION
            before = await state(s, agent)
            expected = MAX_VERSION - 1 if stale else MAX_VERSION
            if entry == "legacy":
                code = "version mismatch" if stale else "role_policy_version_exhausted"
                with pytest.raises(agent_access.PolicyConflict, match=code):
                    await agent_access.replace_policy(
                        s,
                        agent,
                        AgentAccessPolicyWrite(
                            invocation_team_id=None,
                            approver_team_id=None,
                            expected_version=expected,
                        ),
                    )
            else:
                with refused(
                    service,
                    "stale_role_policy_version" if stale else "role_policy_version_exhausted",
                ):
                    await service.apply_role_change(
                        s, change=change(tenant, agent, version=expected)
                    )
            assert await state(s, agent) == before
            assert await audits(s, tenant) == []

    run(body)


def test_real_audit_constraint_failure_rolls_back_policy_and_preserves_flushed_outer_work(
    clean_db: None,
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-3/7/8. Test actual DB failure, not a patched recorder."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-3/7/8."""
        tenant, agent = await seed(f, policy=True, roles={}, version=4)
        async with f() as setup, setup.begin():
            await setup.execute(
                text(
                    "ALTER TABLE curie.identity_audit_events ADD CONSTRAINT "
                    "role_test_audit_failure CHECK "
                    "(event_type <> 'identity.agent_role_policy_changed') NOT VALID"
                )
            )
        try:
            async with f() as s:
                await s.begin()
                await scope(s, tenant)
                outer = Tenant(deployment_id=f"outer-kept-{uuid.uuid4()}")
                s.add(outer)
                await s.flush()
                before = await state(s, agent)
                with pytest.raises(IntegrityError):
                    await service.apply_role_change(s, change=change(tenant, agent, version=4))
                assert s.in_transaction() and s.is_active
                assert await state(s, agent) == before
                assert await audits(s, tenant) == []
                await s.commit()
            async with f() as observer:
                assert await observer.get(Tenant, outer.id) is not None
                assert await state(observer, agent) == before
                assert await audits(observer, tenant) == []
        finally:
            async with f() as cleanup, cleanup.begin():
                await cleanup.execute(
                    text(
                        "ALTER TABLE curie.identity_audit_events DROP CONSTRAINT "
                        "role_test_audit_failure"
                    )
                )

    run(body)


@pytest.mark.parametrize("migrated", ["invocation", "approver"])
@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("different", [False, True])
def test_legacy_direct_guard_rejects_either_migrated_role_before_cas_or_teams(
    clean_db: None, migrated: str, stale: bool, different: bool
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-6. Even an identical old PUT cannot erase migration."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-6."""
        tenant, agent = await seed(
            f, policy=True, roles={migrated: role(migrated=True)}, teams=True
        )
        async with f() as s:
            before = await state(s, agent)
            with pytest.raises(agent_access.PolicyConflict, match="^agent_role_policy_migrated$"):
                await agent_access.replace_policy(
                    s,
                    agent,
                    AgentAccessPolicyWrite(
                        invocation_team_id=uuid.uuid4()
                        if different
                        else before["invocation_team_id"],
                        approver_team_id=before["approver_team_id"],
                        expected_version=0 if stale else 1,
                    ),
                )
            assert s.in_transaction() and s.is_active
            await s.commit()
        async with f() as observer:
            assert await state(observer, agent) == before
            assert await audits(observer, tenant) == []

    run(body)


@pytest.mark.parametrize("migrated", ["invocation", "approver"])
def test_existing_put_route_maps_migrated_guard_to_409(
    clean_db: None, client: Any, auth_headers: dict[str, str], migrated: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-6. Independently exercise the existing route."""
    api()
    ids = []

    async def setup(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-6."""
        ids.extend(await seed(f, policy=True, roles={migrated: role(migrated=True)}))

    run(setup)
    response = client.put(
        f"/agents/{ids[1]}/access-policy",
        headers=auth_headers,
        json={"invocation_team_id": None, "approver_team_id": None, "expected_version": 0},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "agent_role_policy_migrated"}


@pytest.mark.parametrize("roles", [None, {}, {"invocation": role(filter={"entries": [SUBJECT]})}])
@pytest.mark.parametrize("inactive", [False, True])
def test_unmigrated_legacy_write_preserves_role_json_and_historical_commit(
    clean_db: None, roles: Any, inactive: bool
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-1/6. Old clients retain pre-migration semantics."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-1/6."""
        tenant, agent = await seed(
            f,
            policy=True,
            roles=roles,
            teams=True,
            tenant_status="suspended" if inactive else "active",
            agent_status="paused" if inactive else "active",
        )
        async with f() as s:
            before = await state(s, agent)
            result = await agent_access.replace_policy(
                s,
                agent,
                AgentAccessPolicyWrite(
                    invocation_team_id=before["invocation_team_id"],
                    approver_team_id=None,
                    expected_version=1,
                ),
            )
            assert result.version == 2
        async with f() as observer:
            after = await state(observer, agent)
            assert after["role_configuration"] == roles and after["sql_null"] is (roles is None)
            assert after["invocation_team_id"] == before["invocation_team_id"]
            assert after["approver_team_id"] is None and after["version"] == 2
            assert await audits(observer, tenant) == []

    run(body)


@pytest.mark.parametrize(
    "pending", ["new", "dirty", "deleted", "tenant", "authority", "agent", "policy"]
)
@pytest.mark.parametrize("failure", ["guard", "write"])
def test_legacy_preflush_preserves_caller_work_through_guard_and_actual_write_failure(
    clean_db: None, pending: str, failure: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-4/6/7. Reload/savepoint must not erase outer work."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-4/6/7."""
        tenant, agent = await seed(
            f, policy=True, roles={"invocation": role(migrated=failure == "guard")}, authority=True
        )
        extra, _ = await seed(f)
        # A standalone extra tenant, so deletion has no child or audit dependencies.
        async with f() as setup, setup.begin():
            standalone = Tenant(deployment_id=f"outer-standalone-{uuid.uuid4()}")
            setup.add(standalone)
            await setup.flush()
            standalone_id = standalone.id
            if failure == "write":
                await setup.execute(
                    text(
                        "ALTER TABLE curie.agent_access_policies ADD CONSTRAINT "
                        "role_test_legacy_failure CHECK (version <> 2) NOT VALID"
                    )
                )
        try:
            async with f() as s:
                await s.begin()
                await scope(s, tenant)
                marker = f"preserved-{uuid.uuid4()}"
                model, key, field = Tenant, tenant, "deployment_id"
                if pending == "new":
                    row = Tenant(deployment_id=marker)
                    s.add(row)
                elif pending == "deleted":
                    row = await s.get(Tenant, standalone_id)
                    await s.delete(row)
                else:
                    if pending == "dirty":
                        key = extra
                    elif pending == "authority":
                        model, field = TenantIdentityProvider, "managed_by"
                    elif pending == "agent":
                        model, key, field = Agent, agent, "name"
                    elif pending == "policy":
                        model, key, field = AgentAccessPolicy, agent, "updated_at"
                    row = await s.get(model, key)
                    if pending == "policy":
                        from datetime import UTC, datetime

                        marker = datetime(2029, 1, 1, tzinfo=UTC)
                    setattr(row, field, marker)
                observed = []

                def flushed(*args: Any) -> None:
                    """@spec AGENT-ROLE-PERSISTENCE-V1-6. Observe actual preflush completion."""
                    observed.append("flush")

                def queried(session: Any, query_state: Any = None) -> None:
                    """@spec AGENT-ROLE-PERSISTENCE-V1-6. Session event observes ORM discovery."""
                    observed.append("query")

                event.listen(s.sync_session, "after_flush", flushed)
                event.listen(s.sync_session, "do_orm_execute", queried)
                try:
                    error = agent_access.PolicyConflict if failure == "guard" else IntegrityError
                    with pytest.raises(error):
                        await agent_access.replace_policy(
                            s,
                            agent,
                            AgentAccessPolicyWrite(
                                invocation_team_id=None, approver_team_id=None, expected_version=1
                            ),
                        )
                    assert observed[0] == "flush"
                    assert s.in_transaction() and s.is_active
                    if pending != "deleted":
                        key = row.id if pending == "new" else key
                        # Nested rollback may expire the row it changed; read the DB explicitly.
                        await s.refresh(row)
                        assert (
                            getattr(row, "deployment_id" if pending == "new" else field) == marker
                        )
                    await s.commit()
                finally:
                    event.remove(s.sync_session, "after_flush", flushed)
                    event.remove(s.sync_session, "do_orm_execute", queried)
            async with f() as observer:
                if pending == "deleted":
                    assert await observer.get(Tenant, standalone_id) is None
                else:
                    persisted = await observer.get(Tenant if pending == "new" else model, key)
                    assert (
                        getattr(persisted, "deployment_id" if pending == "new" else field) == marker
                    )
                assert (await state(observer, agent))["version"] == 1
                assert await audits(observer, tenant) == []
        finally:
            if failure == "write":
                async with f() as cleanup, cleanup.begin():
                    await cleanup.execute(
                        text(
                            "ALTER TABLE curie.agent_access_policies "
                            "DROP CONSTRAINT role_test_legacy_failure"
                        )
                    )

    run(body)


def test_stale_orm_policy_is_reloaded_before_migration_guard(clean_db: None) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/4/6. Identity-map caching cannot bypass current state."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-2/4/6."""
        tenant, agent = await seed(f, policy=True, roles={})
        async with f() as stale:
            old = await stale.get(AgentAccessPolicy, agent)
            assert old.version == 1
            async with f() as writer, writer.begin():
                await service.apply_role_change(
                    writer, change=change(tenant, agent, version=1, operation="migrate_role")
                )
            result = await service.read_role_policy(stale, tenant_id=tenant, agent_id=agent)
            assert result.version == 2 and result.roles.invocation.legacy_state == "migrated"
            with pytest.raises(agent_access.PolicyConflict, match="^agent_role_policy_migrated$"):
                await agent_access.replace_policy(
                    stale,
                    agent,
                    AgentAccessPolicyWrite(
                        invocation_team_id=None, approver_team_id=None, expected_version=1
                    ),
                )
            await stale.rollback()

    run(body)


def test_conflicting_other_role_namespace_and_oversize_proposal_write_nothing(
    clean_db: None,
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/5/7. Validate the whole final snapshot before writes."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-2/5/7."""
        namespace = {
            "id": str(uuid.uuid4()),
            "authority": "",
            "kind": "slack_workspace",
            "key": "T0EXAMPLE1",
        }
        entry = {
            "kind": "surface_account",
            "provider": "slack",
            "namespace": namespace,
            "native_id": "U0EXAMPLE1",
        }
        tenant, agent = await seed(
            f, policy=True, roles={"approver": role(filter={"entries": [entry]})}
        )
        candidates = [
            {"entries": [{**entry, "namespace": {**namespace, "key": "T0EXAMPLE2"}}]},
            {"entries": [{**SUBJECT, "subject": "x" * 65_536}]},
        ]
        for candidate in candidates:
            async with f() as s, s.begin():
                before = await state(s, agent)
                with refused(service, "invalid_role_policy_input"):
                    await service.apply_role_change(
                        s, change=change(tenant, agent, version=1, filter=candidate)
                    )
                assert await state(s, agent) == before
                assert await audits(s, tenant) == []

    run(body)


def test_role_json_none_is_sql_null_through_real_orm_mapping(clean_db: None) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2. ORM None must not insert forbidden JSON null."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-2."""
        tenant, agent = await seed(f)
        async with f() as s, s.begin():
            row = AgentAccessPolicy(
                agent_id=agent, tenant_id=tenant, version=1, role_configuration=None
            )
            s.add(row)
            await s.flush()
            assert (await state(s, agent))["sql_null"] is True

    run(body)


@pytest.mark.parametrize("operation", ["set_filter", "migrate_role"])
def test_role_changes_preserve_binding_callers_and_explicit_approval_routes(
    clean_db: None, operation: str
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-1/5. Recording a role cannot widen existing routes."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-1/5."""
        from curie_api.models import AgentChannel

        tenant, agent = await seed(f)
        async with f() as setup, setup.begin():
            channel = AgentChannel(
                agent_id=agent,
                tenant_id=tenant,
                kind="slack",
                address=f"C0EXAMPLE{uuid.uuid4().hex[:8]}",
                adapter="B0EXAMPLE1",
                allowed_callers=["U0EXAMPLE1", "U0EXAMPLE2"],
            )
            setup.add(channel)
            await setup.flush()
            channel_id = channel.id
            routes = copy.deepcopy((await setup.get(Agent, agent)).approval_routes)
        async with f() as s, s.begin():
            await service.apply_role_change(s, change=change(tenant, agent, operation=operation))
        async with f() as observer:
            stored = await observer.get(AgentChannel, channel_id)
            assert stored.allowed_callers == ["U0EXAMPLE1", "U0EXAMPLE2"]
            assert stored.generation == 0
            assert (await observer.get(Agent, agent)).approval_routes == routes

    run(body)


def test_legacy_preflush_failure_remains_caller_owned_and_writes_no_policy(clean_db: None) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-6/7. Failed caller flush is not an operation savepoint."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-6/7."""
        tenant, agent = await seed(f)
        async with f() as s:
            await s.begin()
            await scope(s, tenant)
            s.add(Tenant(id=tenant, deployment_id=f"duplicate-pending-{uuid.uuid4()}"))
            with pytest.raises(IntegrityError):
                await agent_access.replace_policy(
                    s,
                    agent,
                    AgentAccessPolicyWrite(
                        invocation_team_id=None, approver_team_id=None, expected_version=0
                    ),
                )
            assert not s.is_active and s.in_transaction()
            await s.rollback()
        async with f() as observer:
            assert await state(observer, agent) is None
            assert await audits(observer, tenant) == []

    run(body)


def test_repeated_migration_is_refused_before_exhaustion(clean_db: None) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-7. Mixed failures have stable stage precedence."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec AGENT-ROLE-PERSISTENCE-V1-7."""
        tenant, agent = await seed(
            f, policy=True, roles={"invocation": role(migrated=True)}, version=MAX_VERSION
        )
        async with f() as s, s.begin():
            with refused(service, "role_already_migrated"):
                await service.apply_role_change(
                    s, change=change(tenant, agent, version=MAX_VERSION, operation="migrate_role")
                )
            assert (await state(s, agent))["version"] == MAX_VERSION
            assert await audits(s, tenant) == []

    run(body)
