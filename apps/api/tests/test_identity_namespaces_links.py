"""Identity namespaces and identity links: the storage invariants (#2910, ADR 0198 decision 5).

``identity_namespaces`` holds a tenant's provider identity namespaces (for
Slack, one ``slack_workspace`` per team id), unique on
``(tenant, provider, authority, kind, key)``. ``identity_links`` binds one
native id in one namespace to exactly one principal or one bot. Among ACTIVE
links ``(namespace, native id)`` is unique; a revoked link keeps its row and
stops counting. No link crosses a tenant: the namespace, the principal target
and the creator are composite foreign keys carrying ``tenant_id``.

Every statement here runs in an outer transaction that is always rolled back;
an insert expected to fail runs in a SAVEPOINT so the outer one survives, and
the failure is asserted on the constraint NAME, since the API classifies
IntegrityErrors by name.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from curie_api.config import get_settings
from sqlalchemy import ForeignKeyConstraint, Index, UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

MARK = "inl-test-"
TEAM = "THOME0001"
USER = "UALICE001"


# --- ORM model shape -------------------------------------------------------


def test_identity_namespace_model_shape() -> None:
    from curie_api.models import IdentityNamespace

    table = IdentityNamespace.__table__
    assert IdentityNamespace.__tablename__ == "identity_namespaces"
    for name in ("id", "tenant_id", "provider", "authority", "kind", "key", "status", "created_at"):
        assert name in table.c, name
    assert table.c["authority"].nullable is False
    assert table.c["authority"].server_default is not None
    assert table.c["status"].server_default is not None
    uniques = {c.name for c in table.constraints if isinstance(c, UniqueConstraint)}
    assert {
        "identity_namespaces_tenant_provider_authority_kind_key_key",
        "identity_namespaces_tenant_id_id_key",
    } <= uniques


def test_identity_link_model_shape() -> None:
    from curie_api.models import IdentityLink

    table = IdentityLink.__table__
    assert IdentityLink.__tablename__ == "identity_links"
    assert table.c["principal_id"].nullable is True
    assert table.c["bot_id"].nullable is True
    assert table.c["revoked_at"].nullable is True
    # Link provenance is mandatory (plan r4.1 item 7).
    assert table.c["created_by_principal_id"].nullable is False
    fks = {c.name: c for c in table.constraints if isinstance(c, ForeignKeyConstraint)}
    assert fks["identity_links_namespace_fkey"].column_keys == [
        "tenant_id",
        "identity_namespace_id",
    ]
    assert fks["identity_links_principal_fkey"].column_keys == ["tenant_id", "principal_id"]
    assert fks["identity_links_created_by_fkey"].column_keys == [
        "tenant_id",
        "created_by_principal_id",
    ]
    assert "identity_links_bot_fkey" in fks
    indexes = {i.name: i for i in table.indexes if isinstance(i, Index)}
    active = indexes["identity_links_active_native_key"]
    assert active.unique is True
    assert [c.name for c in active.columns] == ["identity_namespace_id", "provider_native_id"]


# --- helpers ---------------------------------------------------------------


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
    constraint: str | set[str],
) -> None:
    """Assert the statement fails on ``constraint`` (or one of a set of them).

    A set is for a row that breaks two CHECKs at once: PostgreSQL reports the
    first by name order, and the test pins only that it is one of the two.
    """
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(statement), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    expected = {constraint} if isinstance(constraint, str) else constraint
    assert getattr(cause, "constraint_name", None) in expected, str(exc_info.value)


async def _expect_not_null(
    conn: AsyncConnection, statement: str, params: dict[str, Any], *, column: str
) -> None:
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(statement), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    assert type(cause).__name__ == "NotNullViolationError", str(exc_info.value)
    assert getattr(cause, "column_name", None) == column, str(exc_info.value)


async def _tenant(conn: AsyncConnection) -> uuid.UUID:
    tenant_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    return tenant_id


async def _principal(conn: AsyncConnection, tenant_id: uuid.UUID) -> uuid.UUID:
    principal_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type) "
        "VALUES (:id, :tenant_id, :sub, 'human')",
        {"id": principal_id, "tenant_id": tenant_id, "sub": f"{MARK}{principal_id}"},
    )
    return principal_id


async def _agent(conn: AsyncConnection, tenant_id: uuid.UUID) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.agents (id, tenant_id, name) VALUES (:id, :tenant_id, :name)",
        {"id": agent_id, "tenant_id": tenant_id, "name": f"{MARK}{agent_id}"},
    )
    return agent_id


_NS_INSERT = (
    "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, authority, kind, key, status) "
    "VALUES (:id, :tenant_id, :provider, :authority, :kind, :key, :status)"
)


def _ns_row(tenant_id: uuid.UUID, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "provider": "slack",
        "authority": "",
        "kind": "slack_workspace",
        "key": TEAM,
        "status": "active",
    }
    row.update(overrides)
    return row


async def _namespace(conn: AsyncConnection, tenant_id: uuid.UUID, **overrides: Any) -> uuid.UUID:
    row = _ns_row(tenant_id, **overrides)
    await _exec(conn, _NS_INSERT, row)
    return row["id"]


_LINK_INSERT = (
    "INSERT INTO curie.identity_links (id, tenant_id, identity_namespace_id, provider_native_id, "
    "principal_id, bot_id, verification_source, created_by_principal_id, revoked_at) "
    "VALUES (:id, :tenant_id, :ns, :native, :principal_id, :bot_id, :source, :creator, :revoked_at)"
)


def _link_row(
    tenant_id: uuid.UUID,
    namespace_id: uuid.UUID,
    principal: uuid.UUID,
    **overrides: Any,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "ns": namespace_id,
        "native": USER,
        "principal_id": principal,
        "bot_id": None,
        "source": "admin_mapped",
        "creator": principal,
        "revoked_at": None,
    }
    row.update(overrides)
    return row


async def _base(conn: AsyncConnection) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    tenant_id = await _tenant(conn)
    principal_id = await _principal(conn, tenant_id)
    namespace_id = await _namespace(conn, tenant_id)
    return tenant_id, principal_id, namespace_id


# --- identity_namespaces ---------------------------------------------------


def test_namespace_defaults(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        tenant_id = await _tenant(conn)
        namespace_id = uuid.uuid4()
        await _exec(
            conn,
            "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, kind, key) "
            "VALUES (:id, :tenant_id, 'slack', 'slack_workspace', :key)",
            {"id": namespace_id, "tenant_id": tenant_id, "key": TEAM},
        )
        return await _exec(
            conn,
            "SELECT authority, status, created_at FROM curie.identity_namespaces WHERE id = :id",
            {"id": namespace_id},
        )

    (row,) = _rolled_back(body)
    assert row["authority"] == ""
    assert row["status"] == "active"
    assert row["created_at"] is not None


@pytest.mark.parametrize("key", [TEAM, "T01", "TABC123XYZ"])
def test_slack_workspace_key_grammar_accepts(migrated: None, key: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _namespace(conn, await _tenant(conn), key=key)

    _rolled_back(body)


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"kind": "teams_tenant"}, "identity_namespaces_kind_ck"),
        ({"status": "paused"}, "identity_namespaces_status_ck"),
        ({"key": "tbad"}, "identity_namespaces_slack_workspace_key_ck"),
        # `^T[A-Z0-9]{2,}$`: a T plus at least two more characters.
        ({"key": "T0"}, "identity_namespaces_slack_workspace_key_ck"),
        ({"key": "thome0001"}, "identity_namespaces_slack_workspace_key_ck"),
        ({"key": "THOME 0001"}, "identity_namespaces_slack_workspace_key_ck"),
        ({"key": "EGRID0001"}, "identity_namespaces_slack_workspace_key_ck"),
        # The kind/provider pairing: slack_workspace belongs to slack only.
        ({"provider": "github"}, "identity_namespaces_slack_workspace_key_ck"),
        (
            {"key": ""},
            {"identity_namespaces_key_ck", "identity_namespaces_slack_workspace_key_ck"},
        ),
        (
            {"provider": "myspace"},
            {"identity_namespaces_provider_ck", "identity_namespaces_slack_workspace_key_ck"},
        ),
    ],
    ids=[
        "bad-kind",
        "bad-status",
        "lowercase-key",
        "too-short-key",
        "case-variant-key",
        "space-in-key",
        "enterprise-id-as-key",
        "slack-kind-on-github",
        "empty-key",
        "bad-provider",
    ],
)
def test_bad_namespace_rejected(
    migrated: None, overrides: dict[str, Any], constraint: str | set[str]
) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id = await _tenant(conn)
        await _expect_integrity_error(
            conn, _NS_INSERT, _ns_row(tenant_id, **overrides), constraint=constraint
        )

    _rolled_back(body)


def test_namespace_unknown_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        savepoint = await conn.begin_nested()
        try:
            with pytest.raises(IntegrityError):
                await conn.execute(text(_NS_INSERT), _ns_row(uuid.uuid4()))
        finally:
            if savepoint.is_active:
                await savepoint.rollback()

    _rolled_back(body)


def test_namespace_uniqueness(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id = await _tenant(conn)
        await _namespace(conn, tenant_id)
        await _expect_integrity_error(
            conn,
            _NS_INSERT,
            _ns_row(tenant_id),
            constraint="identity_namespaces_tenant_provider_authority_kind_key_key",
        )
        # A disabled duplicate is still a duplicate: status is not part of the key.
        await _expect_integrity_error(
            conn,
            _NS_INSERT,
            _ns_row(tenant_id, status="disabled"),
            constraint="identity_namespaces_tenant_provider_authority_kind_key_key",
        )
        # Another authority, another key, or another tenant is a different namespace.
        await _namespace(conn, tenant_id, authority="slack.example.gov")
        await _namespace(conn, tenant_id, key="TOTHER001")
        await _namespace(conn, await _tenant(conn))

    _rolled_back(body)


# --- identity_links --------------------------------------------------------


def test_link_defaults_and_both_sources_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        tenant_id, principal_id, namespace_id = await _base(conn)
        first = _link_row(tenant_id, namespace_id, principal_id, source="provider_event_verified")
        await _exec(conn, _LINK_INSERT, first)
        other_ns = await _namespace(conn, tenant_id, key="TOTHER001")
        await _exec(
            conn, _LINK_INSERT, _link_row(tenant_id, other_ns, principal_id, source="admin_mapped")
        )
        return await _exec(
            conn,
            "SELECT verified_at, revoked_at FROM curie.identity_links WHERE id = :id",
            {"id": first["id"]},
        )

    (row,) = _rolled_back(body)
    assert row["verified_at"] is not None
    assert row["revoked_at"] is None


def test_link_bot_target_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        agent_id = await _agent(conn, tenant_id)
        await _exec(
            conn,
            _LINK_INSERT,
            _link_row(tenant_id, namespace_id, principal_id, principal_id=None, bot_id=agent_id),
        )

    _rolled_back(body)


def test_link_bot_from_another_tenant_rejected(migrated: None) -> None:
    """ADR 0198 decision 5: no link crosses a tenant. With agents tenant-scoped
    (#2911, ``agents_tenant_id_id_key``), the bot target carries the link's
    tenant, so an agent of another tenant cannot be a link's bot."""

    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        other_tenant = await _tenant(conn)
        foreign_agent = await _agent(conn, other_tenant)
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(
                tenant_id, namespace_id, principal_id, principal_id=None, bot_id=foreign_agent
            ),
            constraint="identity_links_bot_fkey",
        )

    _rolled_back(body)


def test_link_needs_exactly_one_target(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        agent_id = await _agent(conn, tenant_id)
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(tenant_id, namespace_id, principal_id, principal_id=None, bot_id=None),
            constraint="identity_links_target_xor_ck",
        )
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(tenant_id, namespace_id, principal_id, bot_id=agent_id),
            constraint="identity_links_target_xor_ck",
        )

    _rolled_back(body)


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"source": "email_match"}, "identity_links_verification_source_ck"),
        ({"source": "guessed"}, "identity_links_verification_source_ck"),
        ({"native": ""}, "identity_links_native_id_ck"),
    ],
    ids=["email-source", "guessed-source", "empty-native-id"],
)
def test_bad_link_rejected(migrated: None, overrides: dict[str, Any], constraint: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(tenant_id, namespace_id, principal_id, **overrides),
            constraint=constraint,
        )

    _rolled_back(body)


def test_link_unknown_bot_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(
                tenant_id, namespace_id, principal_id, principal_id=None, bot_id=uuid.uuid4()
            ),
            constraint="identity_links_bot_fkey",
        )

    _rolled_back(body)


def test_link_creator_is_required(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        await _expect_not_null(
            conn,
            _LINK_INSERT,
            _link_row(tenant_id, namespace_id, principal_id, creator=None),
            column="created_by_principal_id",
        )

    _rolled_back(body)


def test_active_link_uniqueness(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        other_principal = await _principal(conn, tenant_id)
        agent_id = await _agent(conn, tenant_id)
        first = _link_row(tenant_id, namespace_id, principal_id)
        await _exec(conn, _LINK_INSERT, first)

        # Two active links for one (namespace, native id): rejected, whether the
        # second targets another principal, the same one, or a bot.
        for overrides in (
            {"principal_id": other_principal},
            {},
            {"principal_id": None, "bot_id": agent_id},
        ):
            await _expect_integrity_error(
                conn,
                _LINK_INSERT,
                _link_row(tenant_id, namespace_id, principal_id, **overrides),
                constraint="identity_links_active_native_key",
            )

        # The same native id in another namespace of the tenant is a different person.
        other_ns = await _namespace(conn, tenant_id, key="TOTHER001")
        await _exec(conn, _LINK_INSERT, _link_row(tenant_id, other_ns, principal_id))

        # Revoke, then re-create: the revoked row stays and stops counting.
        await _exec(
            conn,
            "UPDATE curie.identity_links SET revoked_at = now() WHERE id = :id",
            {"id": first["id"]},
        )
        await _exec(
            conn,
            _LINK_INSERT,
            _link_row(tenant_id, namespace_id, principal_id, principal_id=other_principal),
        )
        # Any number of revoked rows may coexist with the one active link.
        await _exec(
            conn,
            "INSERT INTO curie.identity_links (id, tenant_id, identity_namespace_id, "
            "provider_native_id, principal_id, verification_source, created_by_principal_id, "
            "revoked_at) VALUES (:id, :t, :ns, :native, :p, 'admin_mapped', :p, now())",
            {
                "id": uuid.uuid4(),
                "t": tenant_id,
                "ns": namespace_id,
                "native": USER,
                "p": principal_id,
            },
        )
        rows = await _exec(
            conn,
            "SELECT count(*) AS n, count(*) FILTER (WHERE revoked_at IS NULL) AS active "
            "FROM curie.identity_links WHERE identity_namespace_id = :ns",
            {"ns": namespace_id},
        )
        assert rows == [{"n": 3, "active": 1}]

    _rolled_back(body)


def test_link_cannot_cross_a_tenant(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        tenant_a, principal_a, namespace_a = await _base(conn)
        tenant_b, principal_b, namespace_b = await _base(conn)

        # Tenant B's link pointing at tenant A's namespace.
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(tenant_b, namespace_a, principal_b),
            constraint="identity_links_namespace_fkey",
        )
        # Tenant B's link (own namespace) targeting tenant A's principal.
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(tenant_b, namespace_b, principal_b, principal_id=principal_a),
            constraint="identity_links_principal_fkey",
        )
        # Tenant B's link created by tenant A's principal.
        await _expect_integrity_error(
            conn,
            _LINK_INSERT,
            _link_row(tenant_b, namespace_b, principal_b, creator=principal_a),
            constraint="identity_links_created_by_fkey",
        )
        # The all-tenant-B row is fine.
        await _exec(conn, _LINK_INSERT, _link_row(tenant_b, namespace_b, principal_b))

    _rolled_back(body)


def test_namespace_with_links_cannot_be_deleted(migrated: None) -> None:
    """All FKs are NO ACTION: removing a namespace never silently drops links."""

    async def body(conn: AsyncConnection) -> None:
        tenant_id, principal_id, namespace_id = await _base(conn)
        await _exec(conn, _LINK_INSERT, _link_row(tenant_id, namespace_id, principal_id))
        await _expect_integrity_error(
            conn,
            "DELETE FROM curie.identity_namespaces WHERE id = :id",
            {"id": namespace_id},
            constraint="identity_links_namespace_fkey",
        )

    _rolled_back(body)
