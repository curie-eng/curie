"""Migration 0150 adds the github_host identity namespace kind (#5414).

Downgrade to 0149 refuses, before any DDL, while a github_host namespace or a
link to one exists. With none, it restores the three-kind check: a github_host
insert is refused, and an existing slack_enterprise row and its check stay.
Upgrade puts the kind and the host key check back.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, constraint_exists, sql_dicts
from alembic import command
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

REVISION = "0150"
BELOW = "0149"
HOST = "github.example.com"
ORG = "E0EXAMPLE1"
ACCOUNT = "42"
REFUSAL = "github_host_namespaces_present"
KIND_CK = "identity_namespaces_kind_ck"
KEY_CK = "identity_namespaces_github_host_key_ck"
ENTERPRISE_CK = "identity_namespaces_slack_enterprise_key_ck"

Statement = tuple[str, dict[str, Any]]

_NS = (
    "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, authority, kind, key) "
    "VALUES (:id, :tenant_id, :provider, :authority, :kind, :key)"
)
_SNAPSHOT: list[Statement] = [
    ("SELECT version_num FROM curie.alembic_version", {}),
    (
        "SELECT conname FROM pg_constraint c "
        "JOIN pg_namespace n ON n.oid = c.connamespace "
        "WHERE n.nspname = 'curie' AND c.conname IN ("
        "'identity_namespaces_kind_ck', 'identity_namespaces_github_host_key_ck', "
        "'identity_namespaces_slack_enterprise_key_ck') ORDER BY conname",
        {},
    ),
    (
        "SELECT id, tenant_id, provider, authority, kind, key, status "
        "FROM curie.identity_namespaces ORDER BY id",
        {},
    ),
    (
        "SELECT id, tenant_id, identity_namespace_id, provider_native_id, principal_id, "
        "revoked_at FROM curie.identity_links ORDER BY id",
        {},
    ),
]


def _require_0150() -> None:
    graph = ScriptDirectory.from_config(alembic_config())
    known = {item.revision for item in graph.walk_revisions()}
    assert "0150" in known, "migration 0150 is missing"
    revision = graph.get_revision("0150")
    assert revision is not None and revision.down_revision == "0149"


def _batch(*statements: Statement) -> list[list[dict[str, Any]]]:
    """Statements on one connection in one transaction, each one's rows as dicts."""

    async def run() -> list[list[dict[str, Any]]]:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                results = []
                for statement, params in statements:
                    result = await connection.execute(text(statement), params)
                    results.append(
                        [dict(row) for row in result.mappings()] if result.returns_rows else []
                    )
                return results
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _tenant() -> tuple[uuid.UUID, Statement]:
    tenant_id = uuid.uuid4()
    return tenant_id, (
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :deployment, 'active')",
        {"id": tenant_id, "deployment": f"m0150-{tenant_id.hex[:8]}"},
    )


def _principal(tenant_id: uuid.UUID) -> tuple[uuid.UUID, Statement]:
    principal_id = uuid.uuid4()
    return principal_id, (
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status) "
        "VALUES (:id, :tenant_id, :subject, 'human', 'active')",
        {
            "id": principal_id,
            "tenant_id": tenant_id,
            "subject": f"m0150-{principal_id.hex[:8]}",
        },
    )


def _namespace(
    tenant_id: uuid.UUID,
    *,
    provider: str,
    authority: str,
    kind: str,
    key: str,
    namespace_id: uuid.UUID | None = None,
) -> Statement:
    return (
        _NS,
        {
            "id": uuid.uuid4() if namespace_id is None else namespace_id,
            "tenant_id": tenant_id,
            "provider": provider,
            "authority": authority,
            "kind": kind,
            "key": key,
        },
    )


def _github_host(tenant_id: uuid.UUID) -> Statement:
    return _namespace(tenant_id, provider="github", authority=HOST, kind="github_host", key=HOST)


def _namespace_only() -> list[Statement]:
    tenant_id, tenant = _tenant()
    return [tenant, _github_host(tenant_id)]


def _namespace_and_link() -> list[Statement]:
    tenant_id, tenant = _tenant()
    principal_id, principal = _principal(tenant_id)
    namespace_id = uuid.uuid4()
    return [
        tenant,
        principal,
        _namespace(
            tenant_id,
            provider="github",
            authority=HOST,
            kind="github_host",
            key=HOST,
            namespace_id=namespace_id,
        ),
        (
            "INSERT INTO curie.identity_links (id, tenant_id, identity_namespace_id, "
            "provider_native_id, principal_id, verification_source, created_by_principal_id) "
            "VALUES (:id, :tenant_id, :namespace_id, :native, :principal, 'admin_mapped', "
            ":creator)",
            {
                "id": uuid.uuid4(),
                "tenant_id": tenant_id,
                "namespace_id": namespace_id,
                "native": ACCOUNT,
                "principal": principal_id,
                "creator": principal_id,
            },
        ),
    ]


SEEDS = {"namespace": _namespace_only, "namespace_and_link": _namespace_and_link}


def _refused(params: dict[str, Any], constraint: str) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                transaction = await connection.begin()
                try:
                    with pytest.raises(IntegrityError) as caught:
                        await connection.execute(text(_NS), params)
                finally:
                    await transaction.rollback()
        finally:
            await engine.dispose()
        cause = caught.value.orig.__cause__
        assert getattr(cause, "constraint_name", None) == constraint, str(caught.value)

    asyncio.run(run())


@pytest.mark.parametrize("seed", sorted(SEEDS))
def test_0150_downgrade_refuses_while_a_github_host_namespace_or_link_exists(
    isolated_migration_db: IsolatedMigrationDb, seed: str
) -> None:
    _require_0150()
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    _batch(*SEEDS[seed]())
    before = _batch(*_SNAPSHOT)
    stamp, constraints, namespaces, links = before
    assert stamp == [{"version_num": REVISION}]
    assert {row["conname"] for row in constraints} >= {KIND_CK, KEY_CK, ENTERPRISE_CK}
    assert any(row["kind"] == "github_host" for row in namespaces)
    if seed == "namespace_and_link":
        assert links
    else:
        assert links == []

    with pytest.raises(RuntimeError, match=REFUSAL):
        command.downgrade(config, BELOW)

    # Refused before any DDL: the revision, checks and rows are as they were.
    assert _batch(*_SNAPSHOT) == before


def test_0150_downgrade_without_a_github_host_restores_the_kind_and_reapplies(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    _require_0150()
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    tenant_id, tenant = _tenant()
    namespace_id = uuid.uuid4()
    _batch(
        tenant,
        _namespace(
            tenant_id,
            provider="slack",
            authority="",
            kind="slack_enterprise",
            key=ORG,
            namespace_id=namespace_id,
        ),
    )
    assert constraint_exists(KEY_CK) and constraint_exists(ENTERPRISE_CK)

    command.downgrade(config, BELOW)

    assert sql_dicts("SELECT version_num FROM curie.alembic_version", {}) == [
        {"version_num": BELOW}
    ]
    assert not constraint_exists(KEY_CK)
    assert constraint_exists(ENTERPRISE_CK)
    kept = {
        "id": namespace_id,
        "provider": "slack",
        "authority": "",
        "kind": "slack_enterprise",
        "key": ORG,
        "status": "active",
    }
    namespaces = (
        "SELECT id, provider, authority, kind, key, status FROM curie.identity_namespaces "
        "WHERE tenant_id = :tenant_id ORDER BY id"
    )
    assert sql_dicts(namespaces, {"tenant_id": tenant_id}) == [kept]
    _refused(
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant_id,
            "provider": "github",
            "authority": HOST,
            "kind": "github_host",
            "key": HOST,
        },
        KIND_CK,
    )
    assert sql_dicts(namespaces, {"tenant_id": tenant_id}) == [kept]

    command.upgrade(config, REVISION)

    assert sql_dicts("SELECT version_num FROM curie.alembic_version", {}) == [
        {"version_num": REVISION}
    ]
    assert constraint_exists(KEY_CK) and constraint_exists(ENTERPRISE_CK)
    _batch(_github_host(tenant_id))
    rows = sql_dicts(namespaces, {"tenant_id": tenant_id})
    by_kind = {row["kind"]: row for row in rows}
    assert set(by_kind) == {"slack_enterprise", "github_host"}
    assert by_kind["slack_enterprise"] == kept
    assert by_kind["github_host"]["provider"] == "github"
    assert by_kind["github_host"]["authority"] == HOST
    assert by_kind["github_host"]["key"] == HOST
    assert by_kind["github_host"]["status"] == "active"


@pytest.mark.parametrize(
    ("provider", "authority", "key"),
    [
        ("slack", HOST, HOST),
        ("github", HOST, "T0EXAMPLE1"),
        ("github", "", HOST),
    ],
    ids=["provider", "key", "authority"],
)
def test_0150_github_host_key_check_refuses_beside_a_valid_row(
    isolated_migration_db: IsolatedMigrationDb,
    provider: str,
    authority: str,
    key: str,
) -> None:
    _require_0150()
    isolated_migration_db.at(REVISION)
    tenant_id, tenant = _tenant()
    valid_id = uuid.uuid4()
    _batch(
        tenant,
        _namespace(
            tenant_id,
            provider="github",
            authority=HOST,
            kind="github_host",
            key=HOST,
            namespace_id=valid_id,
        ),
    )
    _refused(
        {
            "id": uuid.uuid4(),
            "tenant_id": tenant_id,
            "provider": provider,
            "authority": authority,
            "kind": "github_host",
            "key": key,
        },
        KEY_CK,
    )
    assert sql_dicts(
        "SELECT id, provider, authority, key FROM curie.identity_namespaces "
        "WHERE tenant_id = :tenant_id",
        {"tenant_id": tenant_id},
    ) == [{"id": valid_id, "provider": "github", "authority": HOST, "key": HOST}]
