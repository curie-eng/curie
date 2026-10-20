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
