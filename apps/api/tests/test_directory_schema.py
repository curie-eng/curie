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
