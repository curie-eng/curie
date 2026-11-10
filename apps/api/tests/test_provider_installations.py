"""Provider installations: a connected external account, table and admin CRUD (#2909).

Migration 0075 adds ``provider_installations``: the connection a tenant
authorised (ADR 0166's original meaning, restored by ADR 0193 decision 3).
``authority`` scopes ``external_account_id``: empty for a provider with one
global service, the canonical hostname for a self-hosted or per-host one, so
two installations can never collide across hosts. Who Curie speaks as through
a connection is ``channel_identities`` (ADR 0168 decision 1, as scoped by ADR
0193 decision 4; see ``test_channel_identities.py``), attached by
``provider_installation_id``.

No row is created at boot (ADR 0193 decision 3): an installation exists only
once its target is known, so until #3039 lands, an operator creates one
through these routes and attaches a channel identity to it.

The DB-level tests share the session's migrated database, so every insert runs
inside an outer transaction that is always rolled back; an insert expected to
fail runs inside a SAVEPOINT so the outer transaction survives the error. The
route tests commit, so a local fixture truncates both tables (channel
identities attach to installations) before and after each test.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import ForeignKeyConstraint, Table, UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_UUID = uuid.UUID(DEFAULT_TENANT_ID)
CANARY = "xoxb-CANARY-9f3a"
# Rows this module creates outside provider_installations carry this prefix so
# the cleanup fixture removes exactly them and never the default tenant.
MARK = "pi-test-"
BASE = "/provider-installations"


def _fk_targets(column: Any) -> set[str]:
    return {fk.target_fullname for fk in column.foreign_keys}


def _unique_constraint_names(table: Table) -> set[str | None]:
    return {c.name for c in table.constraints if isinstance(c, UniqueConstraint)}


def _foreign_key_constraints(table: Table) -> dict[str | None, ForeignKeyConstraint]:
    return {c.name: c for c in table.constraints if isinstance(c, ForeignKeyConstraint)}


# --- ORM model shape -------------------------------------------------------


def test_provider_installation_model_shape() -> None:
    from curie_api.models import ProviderInstallation

    assert ProviderInstallation.__tablename__ == "provider_installations"
    columns = ProviderInstallation.__table__.c

    id_col = columns["id"]
    assert id_col.primary_key is True
    # See test_tenants.py: SQLAlchemy wraps a zero-arg callable default.
    assert inspect.unwrap(id_col.default.arg) is uuid.uuid4

    tenant_id_col = columns["tenant_id"]
    assert tenant_id_col.nullable is False
    assert _fk_targets(tenant_id_col) >= {"curie.tenants.id"}

    for name in ("provider", "authority", "external_account_id", "status"):
        col = columns[name]
        assert col.type.python_type is str
        assert col.nullable is False

    assert columns["display_name"].type.python_type is str
    assert columns["display_name"].nullable is True

    assert columns["authority"].server_default is not None
    assert columns["status"].server_default is not None
    assert columns["installed_by_principal_id"].nullable is True
    assert columns["installed_at"].type.python_type is datetime
    assert columns["installed_at"].nullable is False
    assert columns["disconnected_at"].type.python_type is datetime
    assert columns["disconnected_at"].nullable is True

    assert "provider_installations_tenant_provider_authority_account_key" in (
        _unique_constraint_names(ProviderInstallation.__table__)
    )
    assert "provider_installations_tenant_provider_id_key" in _unique_constraint_names(
        ProviderInstallation.__table__
    )
    # The installer FK is composite so the installer must be in the same tenant.
    installer_fk = _foreign_key_constraints(ProviderInstallation.__table__)[
        "provider_installations_installer_fkey"
    ]
    assert installer_fk.column_keys == ["tenant_id", "installed_by_principal_id"]
    assert [e.target_fullname for e in installer_fk.elements] == [
        "curie.principals.tenant_id",
        "curie.principals.id",
    ]


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
    constraint: str,
) -> None:
    """Assert the statement fails on exactly ``constraint`` (see test_principals.py)."""
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(statement), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    assert getattr(cause, "constraint_name", None) == constraint, str(exc_info.value)


_INSERT = (
    "INSERT INTO curie.provider_installations "
    "(id, tenant_id, provider, authority, external_account_id, status, "
    "disconnected_at, installed_by_principal_id) "
    "VALUES (:id, :tenant_id, :provider, :authority, :external_account_id, :status, "
    ":disconnected_at, :installed_by_principal_id)"
)


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": DEFAULT_TENANT_UUID,
        "provider": "slack",
        "authority": "",
        # Unique per call: several rows in one test body must not collide on
        # (tenant, provider, authority, external_account_id) by accident.
        "external_account_id": f"T{uuid.uuid4().hex[:8]}",
        "status": "connected",
        "disconnected_at": None,
        "installed_by_principal_id": None,
    }
    row.update(overrides)
    return row


async def _insert_tenant(conn: AsyncConnection) -> uuid.UUID:
    tenant_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.tenants (id, deployment_id, status) "
        "VALUES (:id, :deployment_id, 'active')",
        {"id": tenant_id, "deployment_id": f"{MARK}{tenant_id}"},
    )
    return tenant_id


async def _insert_principal(conn: AsyncConnection, tenant_id: uuid.UUID) -> uuid.UUID:
    principal_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type) "
        "VALUES (:id, :tenant_id, :idp_subject, 'human')",
        {"id": principal_id, "tenant_id": tenant_id, "idp_subject": f"{MARK}{principal_id}"},
    )
    return principal_id


def test_insert_applies_defaults(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        installation_id = uuid.uuid4()
        await _exec(
            conn,
            "INSERT INTO curie.provider_installations "
            "(id, tenant_id, provider, external_account_id) "
            "VALUES (:id, :tenant_id, 'github', 'acme')",
            {"id": installation_id, "tenant_id": DEFAULT_TENANT_UUID},
        )
        return await _exec(
            conn,
            "SELECT authority, status, installed_at, disconnected_at "
            "FROM curie.provider_installations WHERE id = :id",
            {"id": installation_id},
        )

    (row,) = _rolled_back(body)
    assert row["authority"] == ""
    assert row["status"] == "connected"
    assert row["installed_at"] is not None
    assert row["disconnected_at"] is None


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"provider": "myspace"}, "provider_installations_provider_ck"),
        ({"status": "paused"}, "provider_installations_status_ck"),
        # disconnected_at is set iff the row is disconnected, both directions.
        ({"status": "disconnected"}, "provider_installations_disconnected_at_ck"),
        (
            {"status": "connected", "disconnected_at": datetime(2026, 1, 1)},
            "provider_installations_disconnected_at_ck",
        ),
        ({"tenant_id": uuid.uuid4()}, "provider_installations_tenant_id_fkey"),
    ],
    ids=[
        "bad-provider",
        "bad-status",
        "disconnected-without-timestamp",
        "connected-with-timestamp",
        "unknown-tenant",
    ],
)
def test_bad_row_rejected(migrated: None, overrides: dict[str, Any], constraint: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(conn, _INSERT, _row(**overrides), constraint=constraint)

    _rolled_back(body)


def test_disconnected_row_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(
            conn, _INSERT, _row(status="disconnected", disconnected_at=datetime(2026, 1, 1))
        )

    _rolled_back(body)


def test_duplicate_tenant_provider_authority_account_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(conn, _INSERT, _row(external_account_id="dup-account"))
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(external_account_id="dup-account"),
            constraint="provider_installations_tenant_provider_authority_account_key",
        )
        # A different authority (e.g. a different GHES host) is not a collision.
        await _exec(
            conn, _INSERT, _row(external_account_id="dup-account", authority="ghes.example.com")
        )
        # The same external_account_id under another provider is a different installation.
        await _exec(conn, _INSERT, _row(provider="github", external_account_id="dup-account"))

    _rolled_back(body)


def test_cross_tenant_installer_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        other_tenant = await _insert_tenant(conn)
        outsider = await _insert_principal(conn, other_tenant)
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(installed_by_principal_id=outsider),
            constraint="provider_installations_installer_fkey",
        )
        # The same principal installing into its own tenant is fine.
        await _exec(conn, _INSERT, _row(tenant_id=other_tenant, installed_by_principal_id=outsider))

    _rolled_back(body)


def test_deleting_installation_with_attached_identity_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation_id = uuid.uuid4()
        await _exec(conn, _INSERT, _row(id=installation_id))
        await _exec(
            conn,
            "INSERT INTO curie.channel_identities "
            "(id, tenant_id, provider, name, provider_installation_id) "
            "VALUES (:id, :tenant_id, 'slack', 'attached', :installation_id)",
            {
                "id": uuid.uuid4(),
                "tenant_id": DEFAULT_TENANT_UUID,
                "installation_id": installation_id,
            },
        )
        await _expect_integrity_error(
            conn,
            "DELETE FROM curie.provider_installations WHERE id = :id",
            {"id": installation_id},
            constraint="channel_identities_installation_fkey",
        )

    _rolled_back(body)


# --- committed-state fixtures ----------------------------------------------


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                return await _exec(conn, statement, params)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _cleanup() -> None:
    # Tolerates the table being absent so a missing migration fails the test
    # body with a clear error rather than erroring every fixture. Both tables
    # together: a channel identity's FK to provider_installations has no
    # cascade, so truncating them separately (in the wrong order) would fail
    # while either file's leftover rows still reference the other's.
    _sql(
        "DO $$ BEGIN "
        "IF to_regclass('curie.provider_installations') IS NOT NULL THEN "
        "TRUNCATE curie.channel_identities, curie.provider_installations; END IF; END $$"
    )
    _sql("DELETE FROM curie.principals WHERE idp_subject LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def clean_installations(migrated: None) -> Iterator[None]:
    _cleanup()
    yield
    _cleanup()


@pytest.fixture
def settings_reset() -> Iterator[None]:
    yield
    get_settings.cache_clear()


@pytest.fixture
def env(settings_reset: None, monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    return monkeypatch


@pytest.fixture
def api(clean_installations: None, env: pytest.MonkeyPatch) -> Iterator[TestClient]:
    # No Slack token: this resource's own tests don't exercise the identity
    # bootstrap (see test_channel_identities.py), and a token would create an
    # unattached identity row that these tests don't expect to see.
    env.setenv("SLACK_BOT_TOKEN", "")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()


def _create(api: TestClient, headers: dict[str, str], **body: Any) -> dict[str, Any]:
    payload = {"provider": "slack", "external_account_id": f"T{uuid.uuid4().hex[:8]}"}
    payload.update(body)
    response = api.post(BASE, json=payload, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


def _committed_tenant_and_principal() -> tuple[str, str]:
    tenant_id = uuid.uuid4()
    principal_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    _sql(
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type) "
        "VALUES (:id, :tenant_id, :sub, 'human')",
        {"id": principal_id, "tenant_id": tenant_id, "sub": f"{MARK}{principal_id}"},
    )
    return str(tenant_id), str(principal_id)


# --- routes ----------------------------------------------------------------


# A fixed id, not uuid.uuid4(): the id is collected into pytest's parametrize
# ids at import time, and CI runs this suite under pytest-xdist (-n 4). A
# fresh random value per worker process makes each worker collect a
# DIFFERENT test id for the same case, which xdist reports as "different
# tests were collected between gw.. and gw.." and fails the whole run. Safe
# to reuse one id everywhere: require_platform_key runs as a router-level
# dependency, before any path operation touches the database.
_MISSING_ID = "00000000-0000-0000-0000-0000000000fe"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", BASE),
        ("get", BASE),
        ("get", f"{BASE}/{_MISSING_ID}"),
        ("patch", f"{BASE}/{_MISSING_ID}"),
        ("delete", f"{BASE}/{_MISSING_ID}"),
    ],
)
def test_routes_require_platform_key(api: TestClient, method: str, path: str) -> None:
    kwargs: dict[str, Any] = {}
    if method in ("post", "patch"):
        kwargs["json"] = {"provider": "slack", "external_account_id": "T1"}
    assert getattr(api, method)(path, **kwargs).status_code == 401
    assert getattr(api, method)(path, headers={"X-API-Key": "wrong"}, **kwargs).status_code == 401


def test_create_list_get_patch_delete(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(
        api,
        auth_headers,
        external_account_id="T0CRUD",
        display_name="Acme Slack",
    )
    assert created["tenant_id"] == DEFAULT_TENANT_ID
    assert created["provider"] == "slack"
    assert created["authority"] == ""
    assert created["external_account_id"] == "T0CRUD"
    assert created["display_name"] == "Acme Slack"
    assert created["status"] == "connected"
    assert created["installed_by_principal_id"] is None
    assert created["installed_at"] is not None
    assert created["disconnected_at"] is None
    installation_id = created["id"]

    github = _create(api, auth_headers, provider="github", external_account_id="acme")

    listed = api.get(BASE, headers=auth_headers)
    assert listed.status_code == 200
    assert [row["id"] for row in listed.json()] == [installation_id, github["id"]]
    only_slack = api.get(BASE, params={"provider": "slack"}, headers=auth_headers).json()
    assert [row["id"] for row in only_slack] == [installation_id]
    other_tenant = api.get(BASE, params={"tenant_id": str(uuid.uuid4())}, headers=auth_headers)
    assert other_tenant.status_code == 200
    assert other_tenant.json() == []

    fetched = api.get(f"{BASE}/{installation_id}", headers=auth_headers)
    assert fetched.status_code == 200
    assert fetched.json() == created

    patched = api.patch(
        f"{BASE}/{installation_id}",
        json={"display_name": "Renamed", "authority": "ghes.example.com"},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    body = patched.json()
    assert body["display_name"] == "Renamed"
    assert body["authority"] == "ghes.example.com"
    assert body["external_account_id"] == "T0CRUD"

    assert api.delete(f"{BASE}/{installation_id}", headers=auth_headers).status_code == 204
    assert api.get(f"{BASE}/{installation_id}", headers=auth_headers).status_code == 404


def test_create_with_same_tenant_installer(api: TestClient, auth_headers: dict[str, str]) -> None:
    tenant_id, principal_id = _committed_tenant_and_principal()
    created = _create(
        api, auth_headers, tenant_id=tenant_id, installed_by_principal_id=principal_id
    )
    assert created["tenant_id"] == tenant_id
    assert created["installed_by_principal_id"] == principal_id
    listed = api.get(BASE, params={"tenant_id": tenant_id}, headers=auth_headers).json()
    assert [row["id"] for row in listed] == [created["id"]]


def test_missing_id_returns_404(api: TestClient, auth_headers: dict[str, str]) -> None:
    # A real row first, so a 404 means "no such id", not "no such route".
    _create(api, auth_headers)
    missing = f"{BASE}/{uuid.uuid4()}"
    assert api.get(missing, headers=auth_headers).status_code == 404
    assert api.patch(missing, json={"display_name": "x"}, headers=auth_headers).status_code == 404
    assert api.delete(missing, headers=auth_headers).status_code == 404


def test_duplicate_returns_409(api: TestClient, auth_headers: dict[str, str]) -> None:
    _create(api, auth_headers, external_account_id="T0DUP")
    response = api.post(
        BASE, json={"provider": "slack", "external_account_id": "T0DUP"}, headers=auth_headers
    )
    assert response.status_code == 409


def test_different_authority_same_account_id_accepted(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """One self-hosted account id on two different hosts is not a collision."""

    first = _create(
        api, auth_headers, external_account_id="shared-account", authority="ghes-a.example.com"
    )
    second = _create(
        api, auth_headers, external_account_id="shared-account", authority="ghes-b.example.com"
    )
    assert first["external_account_id"] == second["external_account_id"] == "shared-account"
    assert first["authority"] != second["authority"]
    assert first["id"] != second["id"]


def test_patch_renames_external_account_id(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(api, auth_headers, external_account_id="placeholder")
    response = api.patch(
        f"{BASE}/{created['id']}", json={"external_account_id": "T0TEST"}, headers=auth_headers
    )
    assert response.status_code == 200, response.text
    assert response.json()["external_account_id"] == "T0TEST"
    assert response.json()["id"] == created["id"]


def test_patch_external_account_id_to_null_rejected(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create(api, auth_headers)
    response = api.patch(
        f"{BASE}/{created['id']}", json={"external_account_id": None}, headers=auth_headers
    )
    assert response.status_code == 422


def test_patch_authority_to_null_rejected(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(api, auth_headers)
    response = api.patch(f"{BASE}/{created['id']}", json={"authority": None}, headers=auth_headers)
    assert response.status_code == 422
    unchanged = api.get(f"{BASE}/{created['id']}", headers=auth_headers).json()
    assert unchanged["authority"] == ""


@pytest.mark.parametrize(
    "body",
    [
        {"provider": "myspace", "external_account_id": "T1"},
        {"provider": "slack", "external_account_id": "T1", "status": "paused"},
        {"provider": "slack", "external_account_id": ""},
        {"provider": "slack"},
    ],
    ids=["bad-provider", "bad-status", "empty-external", "missing-external"],
)
def test_invalid_body_returns_422(
    api: TestClient, auth_headers: dict[str, str], body: dict[str, Any]
) -> None:
    assert api.post(BASE, json=body, headers=auth_headers).status_code == 422


def test_unknown_tenant_returns_422(api: TestClient, auth_headers: dict[str, str]) -> None:
    response = api.post(
        BASE,
        json={"provider": "slack", "external_account_id": "T1", "tenant_id": str(uuid.uuid4())},
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_installer_from_another_tenant_returns_422(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    _, outsider = _committed_tenant_and_principal()
    response = api.post(
        BASE,
        json={
            "provider": "slack",
            "external_account_id": "T1",
            "installed_by_principal_id": outsider,
        },
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert api.get(BASE, headers=auth_headers).json() == []


def test_disconnected_at_follows_status(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(api, auth_headers, status="disconnected")
    assert created["disconnected_at"] is not None

    reconnected = api.patch(
        f"{BASE}/{created['id']}", json={"status": "connected"}, headers=auth_headers
    )
    assert reconnected.status_code == 200, reconnected.text
    assert reconnected.json()["status"] == "connected"
    assert reconnected.json()["disconnected_at"] is None

    disconnected = api.patch(
        f"{BASE}/{created['id']}", json={"status": "disconnected"}, headers=auth_headers
    )
    assert disconnected.status_code == 200, disconnected.text
    assert disconnected.json()["status"] == "disconnected"
    assert disconnected.json()["disconnected_at"] is not None


def test_delete_with_attached_identity_returns_409(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create(api, auth_headers)
    _sql(
        "INSERT INTO curie.channel_identities "
        "(id, tenant_id, provider, name, provider_installation_id) "
        "VALUES (:id, :tenant_id, 'slack', 'attached', :installation_id)",
        {
            "id": uuid.uuid4(),
            "tenant_id": DEFAULT_TENANT_UUID,
            "installation_id": created["id"],
        },
    )
    response = api.delete(f"{BASE}/{created['id']}", headers=auth_headers)
    assert response.status_code == 409
    assert api.get(f"{BASE}/{created['id']}", headers=auth_headers).status_code == 200


# --- validation errors never echo input on this router ---------------------


def test_post_validation_error_never_echoes_input(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    response = api.post(BASE, json={"external_account_id": "T1"}, headers=auth_headers)
    assert response.status_code == 422, response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list) and detail
    for entry in detail:
        assert {"loc", "msg", "type"} <= set(entry)
        assert "input" not in entry
        assert "ctx" not in entry
