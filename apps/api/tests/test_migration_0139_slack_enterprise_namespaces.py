"""Real migration evidence for Grid organisation namespaces. @spec HTTP-INVOCATION-EXECUTION-21.

Synthetic organisation and workspace ids only.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

KEY_CHECK = "identity_namespaces_slack_enterprise_key_ck"
ORG = "E0EXAMPLEORG"


def revision_ready() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21. Absent migration is a body assertion."""
    graph = ScriptDirectory.from_config(alembic_config())
    assert "0139" in {r.revision for r in graph.walk_revisions()}, (
        "organisation namespace migration 0139 is missing"
    )
    assert graph.get_revision("0139").down_revision == "0138"


def tenant() -> uuid.UUID:
    """@spec HTTP-INVOCATION-EXECUTION-21."""
    tenant_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"grid-migration-{tenant_id}"},
    )
    return tenant_id


def namespace(tenant_id: uuid.UUID, *, kind: str, key: str, provider: str = "slack") -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21."""
    sql_rows(
        "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, kind, key) "
        "VALUES (:id, :t, :provider, :kind, :key)",
        {"id": uuid.uuid4(), "t": tenant_id, "provider": provider, "kind": kind, "key": key},
    )


def test_upgrade_admits_only_slack_organisation_keys_and_keeps_rows(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21. Expand-only: existing rows are untouched."""
    isolated_migration_db.at("0138")
    revision_ready()
    owner = tenant()
    namespace(owner, kind="slack_workspace", key="T0EXAMPLEHOME")
    before = sql_dicts("SELECT * FROM curie.identity_namespaces WHERE tenant_id = :t", {"t": owner})
    with pytest.raises(IntegrityError):
        namespace(owner, kind="slack_enterprise", key=ORG)
    command.upgrade(alembic_config(), "0139")
    assert stamped_revision() == "0139" and constraint_exists(KEY_CHECK)
    assert sql_dicts(
        "SELECT * FROM curie.identity_namespaces WHERE tenant_id = :t", {"t": owner}
    ) == before
    namespace(owner, kind="slack_enterprise", key=ORG)
    for provider, key in (("slack", "T0EXAMPLEHOME"), ("slack", "e0exampleorg"),
                          ("email", ORG), ("slack", "E")):
        with pytest.raises(IntegrityError):
            namespace(owner, kind="slack_enterprise", key=key, provider=provider)


def test_downgrade_refuses_while_an_organisation_namespace_exists(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-21. Downgrade never discards a namespace."""
    isolated_migration_db.at("0138")
    revision_ready()
    command.upgrade(alembic_config(), "0139")
    owner = tenant()
    namespace(owner, kind="slack_enterprise", key=ORG)
    with pytest.raises(RuntimeError, match="slack_enterprise_namespaces_present"):
        command.downgrade(alembic_config(), "0138")
    assert stamped_revision() == "0139"
    sql_rows("DELETE FROM curie.identity_namespaces WHERE tenant_id = :t", {"t": owner})
    command.downgrade(alembic_config(), "0138")
    assert stamped_revision() == "0138" and not constraint_exists(KEY_CHECK)
    with pytest.raises(IntegrityError):
        namespace(owner, kind="slack_enterprise", key=ORG)
    command.upgrade(alembic_config(), "0139")
    assert stamped_revision() == "0139"
