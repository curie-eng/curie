"""Principals, teams and memberships: schema-only slice (#2907).

Migration 0057 adds ``principals``, ``teams`` and ``principal_teams`` with no
callers yet. A principal is keyed by ``(tenant_id, idp_subject)``; email and
display name are attributes, never the identity. Teams are either mirrored IdP
groups (which must carry the IdP's ``external_id``) or Curie-managed. A
membership row is a composite-keyed link that cascades away with either side.

The DB-level tests share the session's migrated database, so every insert runs
inside an outer transaction that is always rolled back; an insert expected to
fail runs inside a SAVEPOINT so the outer transaction survives the error.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

import pytest
from curie_api.config import get_settings
from sqlalchemy import ForeignKeyConstraint, Table, UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_UUID = uuid.UUID(DEFAULT_TENANT_ID)


def _fk_targets(column: Any) -> set[str]:
    return {fk.target_fullname for fk in column.foreign_keys}


def _unique_constraint_names(table: Table) -> set[str | None]:
    return {c.name for c in table.constraints if isinstance(c, UniqueConstraint)}


def _foreign_key_constraints(table: Table) -> dict[str | None, ForeignKeyConstraint]:
    return {c.name: c for c in table.constraints if isinstance(c, ForeignKeyConstraint)}


# --- ORM model shape -------------------------------------------------------


def test_principal_model_shape() -> None:
    from curie_api.models import Principal

    assert Principal.__tablename__ == "principals"
    columns = Principal.__table__.c

    id_col = columns["id"]
    assert id_col.primary_key is True
    assert id_col.nullable is False
    # See test_tenants.py: SQLAlchemy wraps a zero-arg callable default.
    assert inspect.unwrap(id_col.default.arg) is uuid.uuid4

    tenant_id_col = columns["tenant_id"]
    assert tenant_id_col.nullable is False
    assert _fk_targets(tenant_id_col) == {"curie.tenants.id"}

    idp_subject_col = columns["idp_subject"]
    assert idp_subject_col.type.python_type is str
    assert idp_subject_col.nullable is False

    type_col = columns["type"]
    assert type_col.type.python_type is str
    assert type_col.nullable is False

    status_col = columns["status"]
    assert status_col.type.python_type is str
    assert status_col.nullable is False
    assert status_col.default is not None
    assert status_col.default.arg == "active"
    assert status_col.server_default is not None

    for name in ("display_name", "email"):
        col = columns[name]
        assert col.type.python_type is str
        assert col.nullable is True

    last_seen_col = columns["last_seen_at"]
    assert last_seen_col.type.python_type is datetime
    assert last_seen_col.nullable is True

    authz_col = columns["authorization_version"]
    assert authz_col.type.python_type is int
    assert authz_col.nullable is False
    assert authz_col.default is not None
    assert authz_col.default.arg == 1
    assert authz_col.server_default is not None

    assert [c.name for c in Principal.__table__.primary_key.columns] == ["id"]

    # (tenant_id, id) is the composite target principal_teams' tenant-scoped
    # FK points at, so a membership can never link across tenants.
    assert {
        "principals_tenant_idp_subject_key",
        "principals_tenant_id_id_key",
    } <= _unique_constraint_names(Principal.__table__)


def test_team_model_shape() -> None:
    from curie_api.models import Team

    assert Team.__tablename__ == "teams"
    columns = Team.__table__.c

    id_col = columns["id"]
    assert id_col.primary_key is True
    assert id_col.nullable is False
    assert inspect.unwrap(id_col.default.arg) is uuid.uuid4

    tenant_id_col = columns["tenant_id"]
    assert tenant_id_col.nullable is False
    assert _fk_targets(tenant_id_col) == {"curie.tenants.id"}

    source_col = columns["source"]
    assert source_col.type.python_type is str
    assert source_col.nullable is False

    external_id_col = columns["external_id"]
    assert external_id_col.type.python_type is str
    assert external_id_col.nullable is True

    name_col = columns["name"]
    assert name_col.type.python_type is str
    assert name_col.nullable is False

    assert [c.name for c in Team.__table__.primary_key.columns] == ["id"]

    assert {
        "teams_tenant_source_external_id_key",
        "teams_tenant_id_id_key",
    } <= _unique_constraint_names(Team.__table__)


def test_principal_team_model_shape() -> None:
    from curie_api.models import PrincipalTeam

    assert PrincipalTeam.__tablename__ == "principal_teams"
    columns = PrincipalTeam.__table__.c

    assert {c.name for c in PrincipalTeam.__table__.primary_key.columns} == {
        "principal_id",
        "team_id",
    }

    tenant_id_col = columns["tenant_id"]
    assert tenant_id_col.type.python_type is uuid.UUID
    assert tenant_id_col.nullable is False

    assert columns["principal_id"].nullable is False
    assert columns["team_id"].nullable is False

    # Both FKs are composite and tenant-scoped: a membership's tenant_id must
    # match the tenant of the principal AND of the team it links.
    fks = _foreign_key_constraints(PrincipalTeam.__table__)
    assert set(fks) == {"principal_teams_principal_fkey", "principal_teams_team_fkey"}

    principal_fk = fks["principal_teams_principal_fkey"]
    assert principal_fk.column_keys == ["tenant_id", "principal_id"]
    assert [e.target_fullname for e in principal_fk.elements] == [
        "curie.principals.tenant_id",
        "curie.principals.id",
    ]
    assert principal_fk.ondelete == "CASCADE"

    team_fk = fks["principal_teams_team_fkey"]
    assert team_fk.column_keys == ["tenant_id", "team_id"]
    assert [e.target_fullname for e in team_fk.elements] == [
        "curie.teams.tenant_id",
        "curie.teams.id",
    ]
    assert team_fk.ondelete == "CASCADE"

    source_col = columns["source"]
    assert source_col.type.python_type is str
    assert source_col.nullable is False

    version_col = columns["version"]
    assert version_col.type.python_type is int
    assert version_col.nullable is False
    assert version_col.default is not None
    assert version_col.default.arg == 1
    assert version_col.server_default is not None

    # The ORM must declare the index under the same name 0057 creates, so a
    # future metadata-vs-migration drift check sees one index, not two.
    assert "ix_principal_teams_team_id" in {index.name for index in PrincipalTeam.__table__.indexes}

    synced_at_col = columns["synced_at"]
    assert synced_at_col.type.python_type is datetime
    assert synced_at_col.nullable is False
    assert synced_at_col.server_default is not None


# --- DB-level constraints --------------------------------------------------


def _rolled_back(body: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
    """Run ``body`` in a transaction that is always rolled back."""

    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                trans = await conn.begin()
                try:
                    return await body(conn)
                finally:
                    await trans.rollback()
        finally:
            await engine.dispose()

    return asyncio.run(run())


async def _exec(
    conn: AsyncConnection, statement: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    result = await conn.execute(text(statement), params or {})
    if not result.returns_rows:
        return []
    return [dict(row) for row in result.mappings().all()]


async def _expect_integrity_error(
    conn: AsyncConnection,
    statement: str,
    params: dict[str, Any],
    *,
    constraint: str | frozenset[str],
) -> None:
    """Assert the insert fails on exactly ``constraint``, not any IntegrityError.

    ``constraint`` may be a set when more than one constraint is violated and
    Postgres does not promise which of them it reports first.

    A bare IntegrityError would let a future NOT NULL, FK or default change make
    a test pass for the wrong reason; asyncpg reports the violated constraint's
    name on the underlying exception.
    """
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(statement), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    expected = {constraint} if isinstance(constraint, str) else set(constraint)
    assert getattr(cause, "constraint_name", None) in expected, str(exc_info.value)


_INSERT_PRINCIPAL = (
    "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, email) "
    "VALUES (:id, :tenant_id, :idp_subject, :type, :email)"
)
_INSERT_PRINCIPAL_FULL = (
    "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status) "
    "VALUES (:id, :tenant_id, :idp_subject, :type, :status)"
)
_INSERT_TEAM = (
    "INSERT INTO curie.teams (id, tenant_id, source, external_id, name) "
    "VALUES (:id, :tenant_id, :source, :external_id, :name)"
)
_INSERT_MEMBERSHIP = (
    "INSERT INTO curie.principal_teams (tenant_id, principal_id, team_id, source) "
    "VALUES (:tenant_id, :principal_id, :team_id, :source)"
)


async def _insert_principal(
    conn: AsyncConnection,
    *,
    idp_subject: str | None = None,
    email: str | None = None,
    type_: str = "human",
    tenant_id: uuid.UUID = DEFAULT_TENANT_UUID,
) -> uuid.UUID:
    principal_id = uuid.uuid4()
    await _exec(
        conn,
        _INSERT_PRINCIPAL,
        {
            "id": principal_id,
            "tenant_id": tenant_id,
            "idp_subject": idp_subject or f"sub-{uuid.uuid4()}",
            "type": type_,
            "email": email,
        },
    )
    return principal_id


async def _insert_team(
    conn: AsyncConnection,
    *,
    source: str = "curie_managed",
    external_id: str | None = None,
    tenant_id: uuid.UUID = DEFAULT_TENANT_UUID,
) -> uuid.UUID:
    team_id = uuid.uuid4()
    await _exec(
        conn,
        _INSERT_TEAM,
        {
            "id": team_id,
            "tenant_id": tenant_id,
            "source": source,
            "external_id": external_id,
            "name": f"team-{team_id}",
        },
    )
    return team_id


async def _insert_tenant(conn: AsyncConnection) -> uuid.UUID:
    """A second tenant, rolled back with the rest of the test's transaction."""
    tenant_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.tenants (id, deployment_id, status) "
        "VALUES (:id, :deployment_id, 'active')",
        {"id": tenant_id, "deployment_id": f"deployment-{tenant_id}"},
    )
    return tenant_id


def test_principal_insert_applies_defaults(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        principal_id = await _insert_principal(
            conn, idp_subject="alice-sub", email="alice@example.com"
        )
        return await _exec(
            conn,
            "SELECT tenant_id, idp_subject, type, status, email, display_name, "
            "last_seen_at, authorization_version "
            "FROM curie.principals WHERE id = :id",
            {"id": principal_id},
        )

    (row,) = _rolled_back(body)
    assert str(row["tenant_id"]) == DEFAULT_TENANT_ID
    assert row["idp_subject"] == "alice-sub"
    assert row["type"] == "human"
    assert row["status"] == "active"
    assert row["email"] == "alice@example.com"
    assert row["display_name"] is None
    assert row["last_seen_at"] is None
    assert row["authorization_version"] == 1


def test_duplicate_tenant_idp_subject_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _insert_principal(conn, idp_subject="dup-sub")
        await _expect_integrity_error(
            conn,
            _INSERT_PRINCIPAL,
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "idp_subject": "dup-sub",
                "type": "human",
                "email": None,
            },
            constraint="principals_tenant_idp_subject_key",
        )

    _rolled_back(body)


def test_unique_constraint_is_named(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        return await _exec(
            conn,
            "SELECT conname FROM pg_constraint "
            "WHERE conname IN ('principals_tenant_idp_subject_key', "
            "'teams_tenant_source_external_id_key', "
            "'principals_tenant_id_id_key', 'teams_tenant_id_id_key') "
            "AND contype = 'u'",
        )

    names = {row["conname"] for row in _rolled_back(body)}
    assert names == {
        "principals_tenant_idp_subject_key",
        "teams_tenant_source_external_id_key",
        "principals_tenant_id_id_key",
        "teams_tenant_id_id_key",
    }


def test_membership_foreign_keys_are_named(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        return await _exec(
            conn,
            "SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint "
            "WHERE conrelid = 'curie.principal_teams'::regclass AND contype = 'f'",
        )

    defs = {row["conname"]: row["def"] for row in _rolled_back(body)}
    assert set(defs) == {"principal_teams_principal_fkey", "principal_teams_team_fkey"}
    assert "(tenant_id, principal_id)" in defs["principal_teams_principal_fkey"]
    assert "principals(tenant_id, id)" in defs["principal_teams_principal_fkey"]
    assert "ON DELETE CASCADE" in defs["principal_teams_principal_fkey"]
    assert "(tenant_id, team_id)" in defs["principal_teams_team_fkey"]
    assert "teams(tenant_id, id)" in defs["principal_teams_team_fkey"]
    assert "ON DELETE CASCADE" in defs["principal_teams_team_fkey"]


def test_principal_teams_team_id_index_exists(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        return await _exec(
            conn,
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' "
            "AND tablename = 'principal_teams' "
            "AND indexname = 'ix_principal_teams_team_id'",
        )

    (row,) = _rolled_back(body)
    assert "(team_id)" in row["indexdef"]


def test_same_email_different_subject_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> int:
        await _insert_principal(conn, idp_subject="sub-a", email="shared@example.com")
        await _insert_principal(conn, idp_subject="sub-b", email="shared@example.com")
        rows = await _exec(
            conn,
            "SELECT count(*) AS n FROM curie.principals WHERE email = 'shared@example.com'",
        )
        return int(rows[0]["n"])

    assert _rolled_back(body) == 2


def test_service_principal_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _insert_principal(conn, type_="service")

    _rolled_back(body)


@pytest.mark.parametrize(
    ("type_", "status", "constraint"),
    [
        ("robot", "active", "principals_type_ck"),
        ("human", "suspended", "principals_status_ck"),
    ],
    ids=["bad-type", "bad-status"],
)
def test_bad_type_or_status_rejected(
    migrated: None, type_: str, status: str, constraint: str
) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT_PRINCIPAL_FULL,
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "idp_subject": f"sub-{uuid.uuid4()}",
                "type": type_,
                "status": status,
            },
            constraint=constraint,
        )

    _rolled_back(body)


def test_authorization_version_below_one_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            "INSERT INTO curie.principals "
            "(id, tenant_id, idp_subject, type, authorization_version) "
            "VALUES (:id, :tenant_id, :idp_subject, 'human', 0)",
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "idp_subject": f"sub-{uuid.uuid4()}",
            },
            constraint="principals_authorization_version_ck",
        )

    _rolled_back(body)


def test_principal_with_nonexistent_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT_PRINCIPAL,
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.uuid4(),
                "idp_subject": f"sub-{uuid.uuid4()}",
                "type": "human",
                "email": None,
            },
            constraint="principals_tenant_id_fkey",
        )

    _rolled_back(body)


def test_idp_group_team_without_external_id_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT_TEAM,
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "source": "idp_group",
                "external_id": None,
                "name": "eng",
            },
            constraint="teams_idp_group_external_id_ck",
        )

    _rolled_back(body)


def test_idp_group_team_with_external_id_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _insert_team(conn, source="idp_group", external_id="grp-123")

    _rolled_back(body)


def test_curie_managed_team_without_external_id_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _insert_team(conn, source="curie_managed", external_id=None)

    _rolled_back(body)


def test_bad_team_source_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT_TEAM,
            {
                "id": uuid.uuid4(),
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "source": "ldap",
                "external_id": "x",
                "name": "eng",
            },
            constraint="teams_source_ck",
        )

    _rolled_back(body)


def test_membership_defaults_and_duplicate_pk_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        principal_id = await _insert_principal(conn)
        team_id = await _insert_team(conn)
        params = {
            "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
            "principal_id": principal_id,
            "team_id": team_id,
            "source": "curie_managed",
        }
        await _exec(conn, _INSERT_MEMBERSHIP, params)
        await _expect_integrity_error(
            conn, _INSERT_MEMBERSHIP, params, constraint="principal_teams_pkey"
        )
        return await _exec(
            conn,
            "SELECT version, synced_at FROM curie.principal_teams "
            "WHERE principal_id = :principal_id AND team_id = :team_id",
            {"principal_id": principal_id, "team_id": team_id},
        )

    (row,) = _rolled_back(body)
    assert row["version"] == 1
    assert row["synced_at"] is not None


def test_bad_membership_source_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        principal_id = await _insert_principal(conn)
        team_id = await _insert_team(conn)
        await _expect_integrity_error(
            conn,
            _INSERT_MEMBERSHIP,
            {
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "principal_id": principal_id,
                "team_id": team_id,
                "source": "manual",
            },
            constraint="principal_teams_source_ck",
        )

    _rolled_back(body)


def test_deleting_principal_cascades_memberships(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> tuple[int, int]:
        principal_id = await _insert_principal(conn)
        team_id = await _insert_team(conn)
        await _exec(
            conn,
            _INSERT_MEMBERSHIP,
            {
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "principal_id": principal_id,
                "team_id": team_id,
                "source": "curie_managed",
            },
        )
        await _exec(
            conn,
            "DELETE FROM curie.principals WHERE id = :id",
            {"id": principal_id},
        )
        memberships = await _exec(
            conn,
            "SELECT count(*) AS n FROM curie.principal_teams WHERE principal_id = :id",
            {"id": principal_id},
        )
        teams = await _exec(
            conn,
            "SELECT count(*) AS n FROM curie.teams WHERE id = :id",
            {"id": team_id},
        )
        return int(memberships[0]["n"]), int(teams[0]["n"])

    memberships, teams = _rolled_back(body)
    assert memberships == 0
    # The team itself survives; only the link row goes.
    assert teams == 1


def test_deleting_team_cascades_memberships(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> int:
        principal_id = await _insert_principal(conn)
        team_id = await _insert_team(conn)
        await _exec(
            conn,
            _INSERT_MEMBERSHIP,
            {
                "tenant_id": uuid.UUID(DEFAULT_TENANT_ID),
                "principal_id": principal_id,
                "team_id": team_id,
                "source": "curie_managed",
            },
        )
        await _exec(conn, "DELETE FROM curie.teams WHERE id = :id", {"id": team_id})
        rows = await _exec(
            conn,
            "SELECT count(*) AS n FROM curie.principal_teams WHERE team_id = :id",
            {"id": team_id},
        )
        return int(rows[0]["n"])

    assert _rolled_back(body) == 0


# --- Tenant isolation of memberships --------------------------------------


def _membership(
    tenant_id: uuid.UUID, principal_id: uuid.UUID, team_id: uuid.UUID
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "team_id": team_id,
        "source": "curie_managed",
    }


@pytest.mark.parametrize(
    ("membership_tenant", "constraint"),
    [
        # Under A the principal matches, so the team side is what fails.
        ("principal", "principal_teams_team_fkey"),
        # Under B the team matches, so the principal side is what fails.
        ("team", "principal_teams_principal_fkey"),
    ],
    ids=["under-principal-tenant", "under-team-tenant"],
)
def test_cross_tenant_membership_rejected(
    migrated: None, membership_tenant: str, constraint: str
) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_a = uuid.UUID(DEFAULT_TENANT_ID)
        tenant_b = await _insert_tenant(conn)
        principal_id = await _insert_principal(conn, tenant_id=tenant_a)
        team_id = await _insert_team(conn, tenant_id=tenant_b)
        tenant_id = tenant_a if membership_tenant == "principal" else tenant_b
        await _expect_integrity_error(
            conn,
            _INSERT_MEMBERSHIP,
            _membership(tenant_id, principal_id, team_id),
            constraint=constraint,
        )

    _rolled_back(body)


def test_membership_under_unrelated_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_b = await _insert_tenant(conn)
        tenant_c = await _insert_tenant(conn)
        principal_id = await _insert_principal(conn, tenant_id=tenant_b)
        team_id = await _insert_team(conn, tenant_id=tenant_b)
        # Both FKs are violated; Postgres does not promise which it reports.
        await _expect_integrity_error(
            conn,
            _INSERT_MEMBERSHIP,
            _membership(tenant_c, principal_id, team_id),
            constraint=frozenset({"principal_teams_principal_fkey", "principal_teams_team_fkey"}),
        )

    _rolled_back(body)


def test_same_tenant_membership_in_second_tenant_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        tenant_b = await _insert_tenant(conn)
        principal_id = await _insert_principal(conn, tenant_id=tenant_b)
        team_id = await _insert_team(conn, tenant_id=tenant_b)
        await _exec(conn, _INSERT_MEMBERSHIP, _membership(tenant_b, principal_id, team_id))
        return await _exec(
            conn,
            "SELECT tenant_id FROM curie.principal_teams "
            "WHERE principal_id = :principal_id AND team_id = :team_id",
            {"principal_id": principal_id, "team_id": team_id},
        )

    (row,) = _rolled_back(body)
    assert row["tenant_id"] is not None
    assert row["tenant_id"] != uuid.UUID(DEFAULT_TENANT_ID)


def test_membership_without_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        principal_id = await _insert_principal(conn)
        team_id = await _insert_team(conn)
        savepoint = await conn.begin_nested()
        try:
            with pytest.raises(IntegrityError) as exc_info:
                await conn.execute(
                    text(_INSERT_MEMBERSHIP),
                    {
                        "tenant_id": None,
                        "principal_id": principal_id,
                        "team_id": team_id,
                        "source": "curie_managed",
                    },
                )
        finally:
            if savepoint.is_active:
                await savepoint.rollback()
        # A NOT NULL violation names the column, not a constraint.
        cause = exc_info.value.orig.__cause__
        assert type(cause).__name__ == "NotNullViolationError", str(exc_info.value)
        assert getattr(cause, "column_name", None) == "tenant_id"

    _rolled_back(body)
