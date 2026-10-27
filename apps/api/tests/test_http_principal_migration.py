"""Authority storage migration preserves existing local rows on a private database.

@spec HTTP-PRINCIPAL-ADOPTION-V1-2.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from principal_adoption_support import ISSUER, api
from sqlalchemy.exc import IntegrityError


def test_upgrade_preserves_old_local_values_and_enforces_authority_storage(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2."""
    api()
    # 0131 is the actual pre-adoption merge head, not a guessed migration ID.
    isolated_migration_db.at("0131")
    tenant = uuid.uuid4()
    params = {"tenant": tenant, "issuer": ISSUER, "deployment": f"synthetic-upgrade-{tenant}"}
    sql_dicts(
        "INSERT INTO curie.tenants (id,deployment_id,status) VALUES (:tenant,:deployment,'active')",
        params,
    )
    sql_dicts(
        "INSERT INTO curie.tenant_identity_providers "
        "(tenant_id,issuer,governed_domains,managed_by,version,identity_epoch) "
        "VALUES (:tenant,:issuer,'[\"example.test\"]'::jsonb,'synthetic-directory',3,7)",
        params,
    )
    before = sql_dicts(
        "SELECT tenant_id,issuer,governed_domains,managed_by,version,identity_epoch "
        "FROM curie.tenant_identity_providers WHERE tenant_id=:tenant",
        params,
    )
    command.upgrade(alembic_config(), "head")
    after = sql_dicts(
        "SELECT tenant_id,issuer,governed_domains,managed_by,version,identity_epoch "
        "FROM curie.tenant_identity_providers WHERE tenant_id=:tenant",
        params,
    )
    assert after == before
    assert sql_dicts(
        "SELECT http_selection,lifecycle_generation FROM curie.tenant_identity_providers "
        "WHERE tenant_id=:tenant",
        params,
    ) == [{"http_selection": None, "lifecycle_generation": 0}]
    assert sql_dicts(
        "SELECT http_selection IS NULL AS is_sql_null, "
        "jsonb_typeof(http_selection) AS json_type "
        "FROM curie.tenant_identity_providers WHERE tenant_id=:tenant",
        params,
    ) == [{"is_sql_null": True, "json_type": None}]
    assert sql_dicts(
        "SELECT data_type,is_nullable FROM information_schema.columns "
        "WHERE table_schema='curie' AND table_name='tenant_identity_providers' "
        "AND column_name='lifecycle_generation'"
    ) == [{"data_type": "bigint", "is_nullable": "NO"}]
    for assignment in (
        "lifecycle_generation=-1",
        "governed_domains=NULL",
        "managed_by=NULL",
        "http_selection='{}'::jsonb,lifecycle_generation=0",
    ):
        with pytest.raises(IntegrityError):
            sql_dicts(
                f"UPDATE curie.tenant_identity_providers SET {assignment} WHERE tenant_id=:tenant",
                params,
            )
    # Exercise a true bigint value using the migrated storage, without creating columns.
    assert sql_dicts(
        "UPDATE curie.tenant_identity_providers SET lifecycle_generation=:value "
        "WHERE tenant_id=:tenant RETURNING lifecycle_generation",
        {**params, "value": 2**31},
    ) == [{"lifecycle_generation": 2**31}]
