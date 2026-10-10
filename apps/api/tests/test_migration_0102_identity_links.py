"""Migration 0102 adds curie.identity_namespaces and curie.identity_links (#2910, ADR 0198).

Expand-only: the upgrade creates two empty tables and touches no existing row,
so a database populated at 0101 (principals, installations in every status,
identities in every status, attached or not) comes through unchanged. The
downgrade drops both tables, rows included.
"""

from __future__ import annotations

import uuid

from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    sql_dicts,
)
from alembic import command
from alembic.script import ScriptDirectory

NAMED_NAMESPACE_CONSTRAINTS = {
    "identity_namespaces_provider_ck",
    "identity_namespaces_kind_ck",
    "identity_namespaces_status_ck",
    "identity_namespaces_slack_workspace_key_ck",
    "identity_namespaces_key_ck",
    "identity_namespaces_tenant_provider_authority_kind_key_key",
    "identity_namespaces_tenant_id_id_key",
}
NAMED_LINK_CONSTRAINTS = {
    "identity_links_target_xor_ck",
    "identity_links_verification_source_ck",
    "identity_links_native_id_ck",
    "identity_links_namespace_fkey",
    "identity_links_principal_fkey",
    "identity_links_created_by_fkey",
    "identity_links_bot_fkey",
}

TENANT = uuid.UUID("00000000-0000-0000-0000-000000000001")


def _regclass(table: str) -> str | None:
    (row,) = sql_dicts("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    return row["name"]


def _constraint_names(table: str) -> set[str]:
    rows = sql_dicts(
        "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:table)",
        {"table": f"curie.{table}"},
    )
    return {row["conname"] for row in rows}


def _index_def(name: str) -> str | None:
    rows = sql_dicts(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' AND indexname = :name",
        {"name": name},
    )
    return rows[0]["indexdef"] if rows else None


def test_0102_revises_0101() -> None:
    script = ScriptDirectory.from_config(alembic_config())
    revision = script.get_revision("0102")
    assert revision is not None
    assert revision.down_revision == "0101"


def test_0102_round_trip_creates_and_drops_both_tables(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0102")
    assert _regclass("identity_namespaces") is not None
    assert _regclass("identity_links") is not None
    try:
        command.downgrade(config, "0101")
        assert _regclass("identity_links") is None
        assert _regclass("identity_namespaces") is None
        # 0101's tables and the FK targets outlive the downgrade.
        for table in ("provider_installations", "channel_identities", "principals", "tenants"):
            assert _regclass(table) is not None, table
    finally:
        command.upgrade(config, "head")
    assert NAMED_NAMESPACE_CONSTRAINTS <= _constraint_names("identity_namespaces")
    assert NAMED_LINK_CONSTRAINTS <= _constraint_names("identity_links")
    index = _index_def("identity_links_active_native_key")
    assert index is not None
    assert "UNIQUE" in index
    assert "identity_namespace_id" in index and "provider_native_id" in index
    assert "revoked_at IS NULL" in index


def test_populated_upgrade_preserves_0101_rows_and_starts_empty(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0101")
    principal_id = uuid.uuid4()
    installation_id = uuid.uuid4()
    disabled_identity = uuid.uuid4()
    revoked_identity = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, email) "
        "VALUES (:id, :tenant, 'mig-0102-alice', 'human', 'alice@example.com')",
        {"id": principal_id, "tenant": TENANT},
    )
    sql_dicts(
        "INSERT INTO curie.provider_installations "
        "(id, tenant_id, provider, external_account_id, status, disconnected_at, "
        "installed_by_principal_id) "
        "VALUES (:id, :tenant, 'slack', 'THOME0001', 'disconnected', now(), :principal)",
        {"id": installation_id, "tenant": TENANT, "principal": principal_id},
    )
    sql_dicts(
        "INSERT INTO curie.channel_identities "
        "(id, tenant_id, provider, name, status, attributes, installation_mismatch, "
        "provider_installation_id) VALUES "
        "(:disabled, :tenant, 'slack', 'mig-disabled', 'disabled', "
        '\'{"app_token_ref": "env:CURIE_SLACK_APP_TOKEN"}\', true, :installation), '
        "(:revoked, :tenant, 'slack', 'mig-revoked', 'revoked', '{}', false, NULL)",
        {
            "disabled": disabled_identity,
            "revoked": revoked_identity,
            "tenant": TENANT,
            "installation": installation_id,
        },
    )

    def snapshot() -> tuple[list[dict], list[dict], list[dict]]:
        return (
            sql_dicts(
                "SELECT * FROM curie.principals WHERE id = :id ORDER BY id", {"id": principal_id}
            ),
            sql_dicts("SELECT * FROM curie.provider_installations ORDER BY id"),
            sql_dicts("SELECT * FROM curie.channel_identities ORDER BY id"),
        )

    before = snapshot()
    command.upgrade(alembic_config(), "0102")
    after = snapshot()
    assert after == before
    principals, installations, identities = after
    assert installations[0]["status"] == "disconnected"
    by_id = {row["id"]: row for row in identities}
    assert by_id[disabled_identity]["status"] == "disabled"
    assert by_id[disabled_identity]["provider_installation_id"] == installation_id
    assert by_id[disabled_identity]["installation_mismatch"] is True
    assert by_id[revoked_identity]["status"] == "revoked"
    assert sql_dicts("SELECT count(*) AS n FROM curie.identity_namespaces") == [{"n": 0}]
    assert sql_dicts("SELECT count(*) AS n FROM curie.identity_links") == [{"n": 0}]
    command.upgrade(alembic_config(), "head")


def test_downgrade_with_rows_in_new_tables_drops_them(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0102")
    principal_id = uuid.uuid4()
    namespace_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type) "
        "VALUES (:id, :tenant, 'mig-0102-down', 'human')",
        {"id": principal_id, "tenant": TENANT},
    )
    sql_dicts(
        "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, kind, key) "
        "VALUES (:id, :tenant, 'slack', 'slack_workspace', 'THOME0001')",
        {"id": namespace_id, "tenant": TENANT},
    )
    sql_dicts(
        "INSERT INTO curie.identity_links (id, tenant_id, identity_namespace_id, "
        "provider_native_id, principal_id, verification_source, created_by_principal_id) "
        "VALUES (:id, :tenant, :ns, 'UALICE001', :p, 'admin_mapped', :p)",
        {"id": uuid.uuid4(), "tenant": TENANT, "ns": namespace_id, "p": principal_id},
    )
    try:
        command.downgrade(config, "0101")
        assert _regclass("identity_links") is None
        assert _regclass("identity_namespaces") is None
        # The principal the link pointed at is untouched.
        assert sql_dicts(
            "SELECT idp_subject FROM curie.principals WHERE id = :id", {"id": principal_id}
        ) == [{"idp_subject": "mig-0102-down"}]
    finally:
        command.upgrade(config, "head")
