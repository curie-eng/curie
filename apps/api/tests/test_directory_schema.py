"""Directory trust records use tenant-scoped, transactional database constraints."""

import asyncio
import uuid

from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def test_directory_schema_has_tenant_bound_evidence(migrated: None) -> None:
    async def check() -> None:
        from curie_api.models import (
            IdentityAuditEvent,
            IdentityConnectToken,
            TenantIdentityProvider,
        )

        assert TenantIdentityProvider.__tablename__ == "tenant_identity_providers"
        assert IdentityConnectToken.__tablename__ == "identity_connect_tokens"
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                tables = set(
                    await conn.scalars(
                        text("SELECT tablename FROM pg_tables WHERE schemaname='curie'")
                    )
                )
                assert {
                    "identity_audit_events",
                    "identity_connect_tokens",
                    "tenant_identity_providers",
                } <= tables
                checks = list(
                    await conn.scalars(
                        text(
                            "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE "
                            "conrelid='curie.identity_links'::regclass AND contype='c'"
                        )
                    )
                )
                assert any(
                    "created_by_directory" in check and "idp_directory" in check for check in checks
                )
                assert (
                    IdentityAuditEvent(
                        id=uuid.uuid4(), tenant_id=uuid.uuid4(), event_type="test", actor="test"
                    ).details
                    is None
                )
        finally:
            await engine.dispose()

    asyncio.run(check())


def test_0122_round_trip_and_tenant_provenance_constraints(isolated_migration_db) -> None:
    import pytest
    from _migration_support import alembic_config, column_names, sql_dicts
    from alembic import command
    from sqlalchemy.exc import IntegrityError

    isolated_migration_db.at("0121")
    tenant = uuid.UUID("00000000-0000-0000-0000-000000000001")
    person, namespace, old_link = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.principals (id,tenant_id,idp_issuer,idp_subject,type) VALUES "
        "(:id,:tenant,'https://idp.example.com','schema-person','human')",
        {"id": person, "tenant": tenant},
    )
    sql_dicts(
        "INSERT INTO curie.identity_namespaces (id,tenant_id,provider,kind,key) VALUES "
        "(:id,:tenant,'slack','slack_workspace','TEXAMPLE1')",
        {"id": namespace, "tenant": tenant},
    )
    sql_dicts(
        "INSERT INTO curie.identity_links (id,tenant_id,identity_namespace_id,"
        "provider_native_id,"
        "principal_id,verification_source,created_by_principal_id) VALUES "
        "(:id,:tenant,:namespace,'UEXAMPLE1',:person,'provider_event_verified',:person)",
        {"id": old_link, "tenant": tenant, "namespace": namespace, "person": person},
    )
    command.upgrade(alembic_config(), "0122")
    # Exercise the migration's server default, not an ORM Python default.
    sql_dicts(
        "INSERT INTO curie.tenant_identity_providers "
        "(tenant_id,issuer,governed_domains,managed_by) VALUES "
        "(:tenant,'https://idp.example.com','[]'::jsonb,'example-adapter')",
        {"tenant": tenant},
    )
    assert sql_dicts(
        "SELECT identity_epoch FROM curie.tenant_identity_providers WHERE tenant_id=:tenant",
        {"tenant": tenant},
    ) == [{"identity_epoch": 0}]
    # More than signed32-bit verifies the actual database column is bigint.
    assert sql_dicts(
        "UPDATE curie.tenant_identity_providers SET identity_epoch=:epoch "
        "WHERE tenant_id=:tenant RETURNING identity_epoch",
        {"tenant": tenant, "epoch": 2**31},
    ) == [{"identity_epoch": 2**31}]
    with pytest.raises(IntegrityError) as negative:
        sql_dicts(
            "UPDATE curie.tenant_identity_providers SET identity_epoch=-1 WHERE tenant_id=:tenant",
            {"tenant": tenant},
        )
    assert getattr(negative.value.orig.__cause__, "constraint_name", None) == (
        "tenant_identity_providers_identity_epoch_ck"
    )
    assert sql_dicts(
        "SELECT identity_epoch FROM curie.tenant_identity_providers WHERE tenant_id=:tenant",
        {"tenant": tenant},
    ) == [{"identity_epoch": 2**31}]
    assert "created_by_directory" in column_names("identity_links")
    assert {"verified_email", "verified_at", "verified_principal_id"} <= set(
        column_names("identity_connect_tokens")
    )
    params = {"id": uuid.uuid4(), "tenant": tenant, "namespace": namespace, "person": person}
    sql_dicts(
        "INSERT INTO curie.identity_links (id,tenant_id,identity_namespace_id,"
        "provider_native_id,"
        "principal_id,verification_source,created_by_directory) VALUES "
        "(:id,:tenant,:namespace,'UEXAMPLE2',:person,'idp_directory','example-adapter')",
        params,
    )
    with pytest.raises(IntegrityError):
        sql_dicts(
            "INSERT INTO curie.identity_links (id,tenant_id,identity_namespace_id,"
            "provider_native_id,"
            "principal_id,verification_source,created_by_principal_id) VALUES "
            "(:id,:tenant,:namespace,'UEXAMPLE3',:person,'idp_directory',:person)",
            {**params, "id": uuid.uuid4()},
        )
    with pytest.raises(IntegrityError):
        sql_dicts(
            "INSERT INTO curie.identity_links (id,tenant_id,identity_namespace_id,"
            "provider_native_id,"
            "principal_id,verification_source) VALUES "
            "(:id,:tenant,:namespace,'UEXAMPLE3',:person,'provider_event_verified')",
            {**params, "id": uuid.uuid4()},
        )
    other = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.tenants (id,deployment_id,status) VALUES (:id,:deployment,'active')",
        {"id": other, "deployment": f"schema-{other}"},
    )
    with pytest.raises(IntegrityError):
        sql_dicts(
            "INSERT INTO curie.identity_audit_events (id,tenant_id,event_type,actor,principal_id) "
            "VALUES (:id,:tenant,'test','example-adapter',:person)",
            {"id": uuid.uuid4(), "tenant": other, "person": person},
        )
    with pytest.raises(IntegrityError):
        sql_dicts(
            "INSERT INTO curie.identity_connect_tokens (token_hash,tenant_id,identity_namespace_id,"
            "provider_native_id,idp_issuer,expires_at) VALUES "
            "('example-token-hash',:tenant,:namespace,"
            "'UEXAMPLE1','https://idp.example.com',now()+interval '10 minutes')",
            {"tenant": other, "namespace": namespace},
        )
    command.downgrade(alembic_config(), "0121")
    assert "created_by_directory" not in column_names("identity_links")
    assert sql_dicts("SELECT id FROM curie.identity_links") == [{"id": old_link}]
    command.upgrade(alembic_config(), "head")


def test_0122_downgrade_refuses_team_only_audit_before_mutating_schema(
    isolated_migration_db,
) -> None:
    import pytest
    from _migration_support import alembic_config, column_names, sql_dicts
    from alembic import command

    isolated_migration_db.at("0122")
    approval_id, audit_id = uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.approvals (id,conversation_id,author,summary,reply_kind,reply_channel,"
        "reply_placeholder,dedupe_key) VALUES (:id,'example-thread','U0EXAMPLE1','Example',"
        "'slack','C0EXAMPLE1','example-ts',:dedupe)",
        {"id": approval_id, "dedupe": uuid.uuid4().hex},
    )
    sql_dicts(
        "INSERT INTO curie.approval_audit_entries (id,approval_id,action,actor,decision,authorizer,"
        "authorized) VALUES (:id,:approval,'resolved','U0EXAMPLE1','approved',NULL,true)",
        {"id": audit_id, "approval": approval_id},
    )
    before = column_names("approval_audit_entries")
    with pytest.raises(RuntimeError, match="team-only approval audit"):
        command.downgrade(alembic_config(), "0121")
    assert column_names("approval_audit_entries") == before
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == [{"version_num": "0122"}]
    assert sql_dicts(
        "SELECT authorizer FROM curie.approval_audit_entries WHERE id=:id", {"id": audit_id}
    ) == [{"authorizer": None}]
    sql_dicts("DELETE FROM curie.approval_audit_entries WHERE id=:id", {"id": audit_id})
    command.downgrade(alembic_config(), "0121")
    assert sql_dicts(
        "SELECT is_nullable FROM information_schema.columns WHERE table_schema='curie' "
        "AND table_name='approval_audit_entries' AND column_name='authorizer'"
    ) == [{"is_nullable": "NO"}]
    command.upgrade(alembic_config(), "head")


def test_0122_downgrade_preserves_team_only_action_audit(isolated_migration_db) -> None:
    import pytest
    from _migration_support import alembic_config, column_names, sql_dicts
    from alembic import command

    isolated_migration_db.at("0122")
    action_id, audit_id = uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agent_actions (id,conversation_id,call_id,tool,dedupe_key) "
        "VALUES (:id,'example-thread','example-call','example-tool',:dedupe)",
        {"id": action_id, "dedupe": uuid.uuid4().hex},
    )
    sql_dicts(
        "INSERT INTO curie.action_audit_entries (id,action_id,action,actor,authorizer,authorized) "
        "VALUES (:id,:action,'authorized','U0EXAMPLE1',NULL,true)",
        {"id": audit_id, "action": action_id},
    )
    before = column_names("action_audit_entries")
    with pytest.raises(RuntimeError, match="action audit"):
        command.downgrade(alembic_config(), "0121")
    assert column_names("action_audit_entries") == before
    assert sql_dicts("SELECT version_num FROM curie.alembic_version") == [{"version_num": "0122"}]
    assert sql_dicts(
        "SELECT authorizer FROM curie.action_audit_entries WHERE id=:id", {"id": audit_id}
    ) == [{"authorizer": None}]
    sql_dicts("DELETE FROM curie.action_audit_entries WHERE id=:id", {"id": audit_id})
    command.downgrade(alembic_config(), "0121")
    command.upgrade(alembic_config(), "head")
