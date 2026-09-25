"""Provider installations: table, admin CRUD and the static Slack bootstrap (#2909).

Migration 0053 adds ``provider_installations``: one row per connected provider
account in a tenant. ``credential_ref`` and ``webhook_verification_ref`` are
pointers (``env:NAME`` or ``k8s-secret:name/key``), never secret values; the
grammar is a DB CHECK and an API check, and a rejected value is never echoed.

When ``SLACK_BOT_TOKEN`` is set, the API lifespan creates one static Slack row
at a fixed id in the default tenant, keyed on "any slack row in the default
tenant" so a rename, an operator-created row or a disconnect is never undone.

The DB-level tests share the session's migrated database, so every insert runs
inside an outer transaction that is always rolled back; an insert expected to
fail runs inside a SAVEPOINT so the outer transaction survives the error. The
route and bootstrap tests commit, so a local fixture truncates the table (and
removes the tenants and principals they create) before and after each test.
"""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import ForeignKeyConstraint, Table, UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_UUID = uuid.UUID(DEFAULT_TENANT_ID)
STATIC_ID = "00000000-0000-0000-0000-000000000101"
CANARY = "xoxb-CANARY-9f3a"
RAW_TOKEN = "xoxb-123-SECRET"  # gitleaks:allow -- fabricated test fixture, never a real Slack token
# Rows this module creates outside provider_installations carry this prefix so
# the cleanup fixture removes exactly them and never the default tenant.
MARK = "pi-test-"
ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
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

    for name in ("provider", "external_account_id", "status"):
        col = columns[name]
        assert col.type.python_type is str
        assert col.nullable is False

    for name in ("display_name", "credential_ref", "webhook_verification_ref"):
        col = columns[name]
        assert col.type.python_type is str
        assert col.nullable is True

    assert columns["status"].server_default is not None
    assert columns["scopes"].nullable is False
    assert columns["scopes"].server_default is not None
    assert columns["installed_by_principal_id"].nullable is True
    assert columns["installed_at"].type.python_type is datetime
    assert columns["installed_at"].nullable is False
    assert columns["disconnected_at"].type.python_type is datetime
    assert columns["disconnected_at"].nullable is True

    assert "provider_installations_tenant_provider_external_key" in _unique_constraint_names(
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


def test_static_slack_installation_id_is_fixed() -> None:
    from curie_api.provider_installations import STATIC_SLACK_INSTALLATION_ID

    assert str(STATIC_SLACK_INSTALLATION_ID) == STATIC_ID


@pytest.mark.parametrize(
    "value",
    ["env:SLACK_BOT_TOKEN", "env:_X1", "k8s-secret:curie-slack/bot-token", "k8s-secret:a.b/K_1.x"],
)
def test_reference_grammar_accepts_pointers(value: str) -> None:
    from curie_api.provider_installations import REFERENCE_RE

    assert REFERENCE_RE.fullmatch(value) is not None


@pytest.mark.parametrize(
    "value",
    [
        RAW_TOKEN,
        "env:",
        "env:lower",
        "env:SLACK BOT",
        "k8s-secret:name",
        "k8s-secret:Upper/key",
        "k8s-secret:-lead/key",
        "k8s-secret:name/",
        "k8s-secret:name/key/extra",
        " env:SLACK_BOT_TOKEN",
        "env:SLACK_BOT_TOKEN\n",
    ],
)
def test_reference_grammar_rejects_values(value: str) -> None:
    from curie_api.provider_installations import REFERENCE_RE

    assert REFERENCE_RE.fullmatch(value) is None


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
    "(id, tenant_id, provider, external_account_id, credential_ref, "
    "webhook_verification_ref, status, disconnected_at, installed_by_principal_id) "
    "VALUES (:id, :tenant_id, :provider, :external_account_id, :credential_ref, "
    ":webhook_verification_ref, :status, :disconnected_at, :installed_by_principal_id)"
)


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": DEFAULT_TENANT_UUID,
        "provider": "slack",
        "external_account_id": f"T{uuid.uuid4().hex[:8]}",
        "credential_ref": None,
        "webhook_verification_ref": None,
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
            "SELECT status, scopes, installed_at, disconnected_at "
            "FROM curie.provider_installations WHERE id = :id",
            {"id": installation_id},
        )

    (row,) = _rolled_back(body)
    assert row["status"] == "connected"
    assert row["scopes"] == []
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
        # Storage itself cannot hold a raw token, whoever writes it.
        ({"credential_ref": RAW_TOKEN}, "provider_installations_credential_ref_ck"),
        (
            {"webhook_verification_ref": RAW_TOKEN},
            "provider_installations_webhook_verification_ref_ck",
        ),
        (
            {"credential_ref": "env:" + "A" * 520},
            "provider_installations_credential_ref_ck",
        ),
        ({"tenant_id": uuid.uuid4()}, "provider_installations_tenant_id_fkey"),
    ],
    ids=[
        "bad-provider",
        "bad-status",
        "disconnected-without-timestamp",
        "connected-with-timestamp",
        "raw-token-credential-ref",
        "raw-token-webhook-ref",
        "overlong-ref",
        "unknown-tenant",
    ],
)
def test_bad_row_rejected(migrated: None, overrides: dict[str, Any], constraint: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(conn, _INSERT, _row(**overrides), constraint=constraint)

    _rolled_back(body)


def test_valid_references_and_disconnected_row_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(
            conn,
            _INSERT,
            _row(
                credential_ref="env:SLACK_BOT_TOKEN",
                webhook_verification_ref="k8s-secret:curie-slack/signing-secret",
                status="disconnected",
                disconnected_at=datetime(2026, 1, 1),
            ),
        )

    _rolled_back(body)


def test_duplicate_tenant_provider_external_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(conn, _INSERT, _row(external_account_id="T-DUP"))
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(external_account_id="T-DUP"),
            constraint="provider_installations_tenant_provider_external_key",
        )
        # The same account under another provider is a different installation.
        await _exec(conn, _INSERT, _row(provider="github", external_account_id="T-DUP"))

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
    # body with a clear error rather than erroring every fixture.
    _sql(
        "DO $$ BEGIN "
        "IF to_regclass('curie.provider_installations') IS NOT NULL THEN "
        "TRUNCATE curie.provider_installations; END IF; END $$"
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
    # Requested before monkeypatch so this teardown runs AFTER monkeypatch has
    # restored the environment, leaving no settings cached from the patched env.
    yield
    get_settings.cache_clear()


@pytest.fixture
def env(settings_reset: None, monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    return monkeypatch


@contextmanager
def _app(env: pytest.MonkeyPatch, token: str) -> Iterator[TestClient]:
    """Boot the real app (lifespan included) with ``SLACK_BOT_TOKEN=token``."""
    env.setenv("SLACK_BOT_TOKEN", token)
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()


@pytest.fixture
def api(clean_installations: None, env: pytest.MonkeyPatch) -> Iterator[TestClient]:
    # The developer shell may export a real token; route tests boot without one
    # so no bootstrap row appears in their listings.
    with _app(env, "") as test_client:
        yield test_client


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


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", BASE),
        ("get", BASE),
        ("get", f"{BASE}/{uuid.uuid4()}"),
        ("patch", f"{BASE}/{uuid.uuid4()}"),
        ("delete", f"{BASE}/{uuid.uuid4()}"),
    ],
)
def test_routes_require_api_key(api: TestClient, method: str, path: str) -> None:
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
        credential_ref="env:SLACK_BOT_TOKEN",
        webhook_verification_ref="k8s-secret:curie-slack/signing-secret",
        scopes=["chat:write", "users:read"],
    )
    assert created["tenant_id"] == DEFAULT_TENANT_ID
    assert created["provider"] == "slack"
    assert created["external_account_id"] == "T0CRUD"
    assert created["display_name"] == "Acme Slack"
    assert created["credential_ref"] == "env:SLACK_BOT_TOKEN"
    assert created["webhook_verification_ref"] == "k8s-secret:curie-slack/signing-secret"
    assert created["scopes"] == ["chat:write", "users:read"]
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
        json={"display_name": "Renamed", "scopes": ["chat:write"], "credential_ref": None},
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    body = patched.json()
    assert body["display_name"] == "Renamed"
    assert body["scopes"] == ["chat:write"]
    # null clears; a field left out of the body is untouched.
    assert body["credential_ref"] is None
    assert body["webhook_verification_ref"] == "k8s-secret:curie-slack/signing-secret"
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


def test_patch_rename_collision_returns_409(api: TestClient, auth_headers: dict[str, str]) -> None:
    _create(api, auth_headers, external_account_id="T0TAKEN")
    other = _create(api, auth_headers, external_account_id="T0OTHER")
    response = api.patch(
        f"{BASE}/{other['id']}", json={"external_account_id": "T0TAKEN"}, headers=auth_headers
    )
    assert response.status_code == 409
    unchanged = api.get(f"{BASE}/{other['id']}", headers=auth_headers).json()
    assert unchanged["external_account_id"] == "T0OTHER"


def test_patch_renames_external_account_id(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(api, auth_headers, external_account_id="static")
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

    degraded = api.patch(
        f"{BASE}/{created['id']}", json={"status": "degraded"}, headers=auth_headers
    )
    assert degraded.status_code == 200, degraded.text
    assert degraded.json()["disconnected_at"] is None


# --- secret values are never accepted or echoed ----------------------------

_INVALID_REFS = [RAW_TOKEN, CANARY, "env:lower-" + CANARY, "k8s-secret:" + CANARY]


@pytest.mark.parametrize("field", ["credential_ref", "webhook_verification_ref"])
@pytest.mark.parametrize("value", _INVALID_REFS)
def test_post_with_raw_value_is_rejected_without_echo(
    api: TestClient, auth_headers: dict[str, str], field: str, value: str
) -> None:
    response = api.post(
        BASE,
        json={"provider": "slack", "external_account_id": "T1", field: value},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert value not in response.text
    assert "xoxb" not in response.text
    assert api.get(BASE, headers=auth_headers).json() == []


@pytest.mark.parametrize("field", ["credential_ref", "webhook_verification_ref"])
@pytest.mark.parametrize("value", _INVALID_REFS)
def test_patch_with_raw_value_is_rejected_without_echo(
    api: TestClient, auth_headers: dict[str, str], field: str, value: str
) -> None:
    created = _create(api, auth_headers, **{field: "env:SLACK_BOT_TOKEN"})
    response = api.patch(f"{BASE}/{created['id']}", json={field: value}, headers=auth_headers)
    assert response.status_code == 422
    assert value not in response.text
    assert "xoxb" not in response.text
    unchanged = api.get(f"{BASE}/{created['id']}", headers=auth_headers).json()
    assert unchanged[field] == "env:SLACK_BOT_TOKEN"


def test_overlong_reference_rejected(api: TestClient, auth_headers: dict[str, str]) -> None:
    value = "env:" + "A" * 520
    response = api.post(
        BASE,
        json={"provider": "slack", "external_account_id": "T1", "credential_ref": value},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert value not in response.text


@pytest.mark.parametrize("value", ["env:SLACK_BOT_TOKEN", "k8s-secret:curie-slack/bot-token"])
@pytest.mark.parametrize("field", ["credential_ref", "webhook_verification_ref"])
def test_valid_reference_forms_accepted(
    api: TestClient, auth_headers: dict[str, str], field: str, value: str
) -> None:
    created = _create(api, auth_headers, **{field: value})
    assert created[field] == value
    patched = api.patch(f"{BASE}/{created['id']}", json={field: value}, headers=auth_headers)
    assert patched.status_code == 200, patched.text


def test_canary_never_appears_in_any_response(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    texts: list[str] = []

    ok = api.post(
        BASE,
        json={
            "provider": "slack",
            "external_account_id": "T0CANARY",
            "credential_ref": "env:SLACK_BOT_TOKEN",
        },
        headers=auth_headers,
    )
    assert ok.status_code == 201, ok.text
    texts.append(ok.text)
    installation_id = ok.json()["id"]

    bad_post = api.post(
        BASE,
        json={"provider": "slack", "external_account_id": "T0C2", "credential_ref": CANARY},
        headers=auth_headers,
    )
    assert bad_post.status_code == 422
    texts.append(bad_post.text)

    conflict = api.post(
        BASE,
        json={"provider": "slack", "external_account_id": "T0CANARY", "credential_ref": CANARY},
        headers=auth_headers,
    )
    # Either the grammar or the unique key rejects it; neither may echo it.
    assert conflict.status_code in (409, 422)
    texts.append(conflict.text)

    bad_patch = api.patch(
        f"{BASE}/{installation_id}",
        json={"webhook_verification_ref": CANARY},
        headers=auth_headers,
    )
    assert bad_patch.status_code == 422
    texts.append(bad_patch.text)

    good_patch = api.patch(
        f"{BASE}/{installation_id}",
        json={"webhook_verification_ref": "k8s-secret:curie-slack/signing-secret"},
        headers=auth_headers,
    )
    assert good_patch.status_code == 200
    texts.append(good_patch.text)

    texts.append(api.get(f"{BASE}/{installation_id}", headers=auth_headers).text)
    texts.append(api.get(BASE, headers=auth_headers).text)

    for body in texts:
        assert CANARY not in body


# --- static Slack bootstrap through the real lifespan ----------------------


def _slack_rows() -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, tenant_id, external_account_id, credential_ref, status, scopes "
        "FROM curie.provider_installations WHERE provider = 'slack' ORDER BY installed_at, id"
    )


def test_bootstrap_creates_one_static_row(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        response = app.get(BASE, headers=auth_headers)
    assert response.status_code == 200
    assert CANARY not in response.text
    (row,) = response.json()
    assert row["id"] == STATIC_ID
    assert row["tenant_id"] == DEFAULT_TENANT_ID
    assert row["provider"] == "slack"
    assert row["external_account_id"] == "static"
    assert row["credential_ref"] == "env:SLACK_BOT_TOKEN"
    assert row["status"] == "connected"
    assert row["scopes"] == []
    assert row["disconnected_at"] is None

    # A second boot finds the row and adds nothing.
    with _app(env, CANARY) as app:
        again = app.get(BASE, headers=auth_headers)
    assert CANARY not in again.text
    assert [r["id"] for r in again.json()] == [STATIC_ID]


def test_bootstrap_without_token_creates_nothing(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, "") as app:
        response = app.get(BASE, headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == []


def test_renamed_static_row_is_not_duplicated_on_restart(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        patched = app.patch(
            f"{BASE}/{STATIC_ID}", json={"external_account_id": "T0TEST"}, headers=auth_headers
        )
        assert patched.status_code == 200, patched.text
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert [(r["id"], r["external_account_id"]) for r in listed] == [(STATIC_ID, "T0TEST")]


def test_disconnected_static_row_is_not_resurrected(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        patched = app.patch(
            f"{BASE}/{STATIC_ID}", json={"status": "disconnected"}, headers=auth_headers
        )
        assert patched.status_code == 200, patched.text
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert len(listed) == 1
    assert listed[0]["id"] == STATIC_ID
    assert listed[0]["status"] == "disconnected"
    assert listed[0]["disconnected_at"] is not None


def test_operator_created_slack_row_suppresses_bootstrap(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, "") as app:
        operator_row = _create(
            app, auth_headers, external_account_id="T0OPERATOR", credential_ref="env:MY_TOKEN"
        )
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert [r["id"] for r in listed] == [operator_row["id"]]
    assert operator_row["id"] != STATIC_ID


def test_deleting_every_slack_row_lets_next_boot_recreate_it(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        assert app.delete(f"{BASE}/{STATIC_ID}", headers=auth_headers).status_code == 204
        assert app.get(BASE, headers=auth_headers).json() == []
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert [(r["id"], r["external_account_id"]) for r in listed] == [(STATIC_ID, "static")]


def test_other_provider_rows_do_not_suppress_bootstrap(
    clean_installations: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, "") as app:
        _create(app, auth_headers, provider="github", external_account_id="acme")
    with _app(env, CANARY) as app:
        slack = app.get(BASE, params={"provider": "slack"}, headers=auth_headers).json()
    assert [r["id"] for r in slack] == [STATIC_ID]


# --- bootstrap_static_slack called directly --------------------------------


async def _bootstrap_once(token: str) -> bool:
    from curie_api.db import create_sessionmaker
    from curie_api.provider_installations import bootstrap_static_slack

    settings = get_settings().model_copy(update={"slack_bot_token": token})
    engine = create_async_engine(settings.database_url)
    try:
        return await bootstrap_static_slack(create_sessionmaker(engine), settings)
    finally:
        await engine.dispose()


def test_concurrent_bootstrap_yields_one_row(clean_installations: None) -> None:
    async def race() -> list[bool]:
        # Separate engines, so the two inserts really run on two connections.
        return list(await asyncio.gather(_bootstrap_once(CANARY), _bootstrap_once(CANARY)))

    assert asyncio.run(race()) == [True, True]
    rows = _slack_rows()
    assert [str(r["id"]) for r in rows] == [STATIC_ID]
    assert rows[0]["external_account_id"] == "static"


def _alembic_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _regclass(name: str) -> str | None:
    value = _sql("SELECT to_regclass(:name)::text AS name", {"name": name})[0]["name"]
    return None if value is None else str(value)


def test_bootstrap_tolerates_missing_table(isolated_migration_db: None) -> None:
    config = _alembic_config()
    command.upgrade(config, "0052")
    try:
        # A rolling upgrade can boot this image before 0053 is applied.
        assert asyncio.run(_bootstrap_once(CANARY)) is False
        assert _regclass("curie.provider_installations") is None
    finally:
        command.upgrade(config, "head")
    # Once the table exists the same call finds it and bootstraps.
    assert asyncio.run(_bootstrap_once(CANARY)) is True
    assert [str(r["id"]) for r in _slack_rows()] == [STATIC_ID]


def test_lifespan_retries_until_table_appears(
    isolated_migration_db: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    config = _alembic_config()
    command.upgrade(config, "0052")
    try:
        with _app(env, CANARY):
            # Boot succeeded below 0053; the migration now lands while the API runs.
            command.upgrade(config, "head")
            deadline = time.monotonic() + 15
            rows: list[dict[str, Any]] = []
            while time.monotonic() < deadline:
                rows = _slack_rows()
                if rows:
                    break
                time.sleep(0.25)
    finally:
        command.upgrade(config, "head")
    assert [str(r["id"]) for r in rows] == [STATIC_ID]


# --- validation errors never echo input on this router ---------------------


def _assert_scrubbed_422(response: Any) -> None:
    assert response.status_code == 422, response.text
    assert CANARY not in response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list) and detail
    for entry in detail:
        assert {"loc", "msg", "type"} <= set(entry)
        # FastAPI's default puts the submitted value in `input` (and sometimes
        # `ctx`); this router must drop both.
        assert "input" not in entry
        assert "ctx" not in entry


@pytest.mark.parametrize(
    "body",
    [
        # A `missing` error's input is the whole body, canary included.
        {"credential_ref": CANARY},
        {"provider": "slack", "external_account_id": "T1", "credential_ref": {"token": CANARY}},
        {"provider": "slack", "external_account_id": "T1", "credential_ref": [CANARY]},
    ],
    ids=["missing-fields", "ref-as-object", "ref-as-list"],
)
def test_post_validation_error_never_echoes_input(
    api: TestClient, auth_headers: dict[str, str], body: dict[str, Any]
) -> None:
    _assert_scrubbed_422(api.post(BASE, json=body, headers=auth_headers))


def test_patch_validation_error_never_echoes_input(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create(api, auth_headers)
    response = api.patch(
        f"{BASE}/{created['id']}",
        json={"webhook_verification_ref": {"t": CANARY}},
        headers=auth_headers,
    )
    _assert_scrubbed_422(response)


def test_other_routers_keep_default_validation_errors(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    # The scrubbing is scoped to this router; elsewhere FastAPI's default stands.
    response = api.post("/agents", json={"bogus": CANARY}, headers=auth_headers)
    assert response.status_code == 422
    assert any("input" in entry for entry in response.json()["detail"])


# --- references never embed a credential ------------------------------------

_PREFIXED_OR_OVERLONG = [
    "k8s-secret:x/xoxb-123-abc",
    "k8s-secret:x/xapp-1-abc",
    "k8s-secret:x/ghp_abc",
    "k8s-secret:x/github_pat_abc",
    "k8s-secret:x/sk-ant-abc",
    "k8s-secret:xoxb-1-2/key",
    "k8s-secret:" + "z" * 254 + "/k",
    "k8s-secret:n/" + "k" * 254,
]
_BOUNDARY_ACCEPTED = [
    "k8s-secret:curie-slack/bot-token",
    # sk- is only a credential prefix at the start of a segment.
    "k8s-secret:desk-app/task-sk-1",
    "env:SLACK_BOT_TOKEN",
    "k8s-secret:" + "z" * 253 + "/k",
    "k8s-secret:n/" + "k" * 253,
]


@pytest.mark.parametrize("value", _PREFIXED_OR_OVERLONG)
def test_is_reference_rejects_credential_prefixes_and_overlong_segments(value: str) -> None:
    from curie_api.provider_installations import is_reference

    assert is_reference(value) is False


@pytest.mark.parametrize("value", _BOUNDARY_ACCEPTED)
def test_is_reference_accepts_boundary_pointers(value: str) -> None:
    from curie_api.provider_installations import is_reference

    assert len(value) <= 512
    assert is_reference(value) is True


@pytest.mark.parametrize("value", _PREFIXED_OR_OVERLONG)
def test_db_check_rejects_credential_prefixes_and_overlong_segments(
    migrated: None, value: str
) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(credential_ref=value),
            constraint="provider_installations_credential_ref_ck",
        )

    _rolled_back(body)


@pytest.mark.parametrize("value", _BOUNDARY_ACCEPTED)
def test_db_check_accepts_boundary_pointers(migrated: None, value: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(conn, _INSERT, _row(credential_ref=value))

    _rolled_back(body)


def test_prefixed_reference_route_returns_422_without_echo(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    value = "k8s-secret:x/xoxb-123-abc"
    response = api.post(
        BASE,
        json={"provider": "slack", "external_account_id": "T1", "credential_ref": value},
        headers=auth_headers,
    )
    assert response.status_code == 422
    assert value not in response.text
    assert "xoxb" not in response.text


# --- background bootstrap logs a failure once --------------------------------


def test_repeated_bootstrap_failure_warns_once_then_finishes(
    migrated: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import curie_api.provider_installations as pi
    from curie_api.db import create_sessionmaker

    state = {"fail": True, "calls": 0}

    async def flaky(sessionmaker: Any, settings: Any) -> bool:
        state["calls"] += 1
        if state["fail"]:
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(pi, "bootstrap_static_slack", flaky)

    async def run() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        task = None
        try:
            with caplog.at_level("WARNING", logger="curie_api.provider_installations"):
                task = await pi.start_static_slack_bootstrap(
                    create_sessionmaker(engine), settings, interval_s=0.01
                )
                assert task is not None
                await asyncio.sleep(0.2)
            assert state["calls"] >= 3
            failures = [
                r
                for r in caplog.records
                if r.levelname == "WARNING" and "bootstrap failed" in r.getMessage()
            ]
            # One warning for a failure streak, not one per 10 ms attempt.
            assert len(failures) == 1, [r.getMessage() for r in failures]

            state["fail"] = False
            await asyncio.wait_for(task, timeout=2)
            assert task.done() and task.exception() is None
        finally:
            if task is not None and not task.done():
                task.cancel()
            await engine.dispose()

    asyncio.run(run())


# --- realistic pasted credentials are not references -------------------------

_PASTED_CREDENTIALS = [
    "k8s-secret:x/xoxe.xoxp-1-abc",
    "k8s-secret:x/xoxe.xoxb-1-abc",
    "k8s-secret:x/lin_api_abc123",
    "k8s-secret:x/sk_live_abc",
    "k8s-secret:x/rk_live_abc",
    "k8s-secret:x/AIzaSyA1b2c3",  # gitleaks:allow -- fabricated, not a real Google key
    "k8s-secret:x/eyJhbGciOiJIUzI1NiJ9.e30.sig",
    "env:AKIAIOSFODNN7EXAMPLE",
    "env:ASIAIOSFODNN7EXAMPLE",
    # 32+ consecutive hex characters anywhere read as a key, not a name.
    "k8s-secret:x/" + "0123456789abcdef" * 2,
    "k8s-secret:" + "a1b2c3d4e5" * 4 + "/key",
]
_LOOKALIKE_POINTERS = [
    "k8s-secret:curie-slack/bot-token",
    "k8s-secret:desk-app/task-sk-1",
    "env:SLACK_BOT_TOKEN",
    # Only an exact AWS key id shape (AKIA|ASIA + 16) is rejected.
    "env:AKIA",
    "env:AKIA_ROLE",
    "k8s-secret:x/sha-abc123",
    # One below the 32-hex threshold.
    "k8s-secret:x/" + ("0123456789abcdef" * 2)[:31],
]


@pytest.mark.parametrize("value", _PASTED_CREDENTIALS)
def test_is_reference_rejects_pasted_credentials(value: str) -> None:
    from curie_api.provider_installations import is_reference

    assert is_reference(value) is False


@pytest.mark.parametrize("value", _LOOKALIKE_POINTERS)
def test_is_reference_accepts_lookalike_pointers(value: str) -> None:
    from curie_api.provider_installations import is_reference

    assert is_reference(value) is True


@pytest.mark.parametrize("value", _PASTED_CREDENTIALS)
def test_db_check_rejects_pasted_credentials(migrated: None, value: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(credential_ref=value),
            constraint="provider_installations_credential_ref_ck",
        )

    _rolled_back(body)


def test_db_check_rejects_pasted_webhook_secret(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(webhook_verification_ref="k8s-secret:x/" + "0123456789abcdef" * 2),
            constraint="provider_installations_webhook_verification_ref_ck",
        )

    _rolled_back(body)


@pytest.mark.parametrize("value", _LOOKALIKE_POINTERS)
def test_db_check_accepts_lookalike_pointers(migrated: None, value: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(conn, _INSERT, _row(credential_ref=value))

    _rolled_back(body)


def test_failures_after_missing_table_still_warn_once(
    migrated: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import curie_api.provider_installations as pi
    from curie_api.db import create_sessionmaker

    calls = {"n": 0}

    async def scripted(sessionmaker: Any, settings: Any) -> bool:
        # Inline attempt: table missing. Then a failure streak. Then success.
        calls["n"] += 1
        if calls["n"] == 1:
            return False
        if calls["n"] <= 10:
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(pi, "bootstrap_static_slack", scripted)

    async def run() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        task = None
        try:
            with caplog.at_level("WARNING", logger="curie_api.provider_installations"):
                task = await pi.start_static_slack_bootstrap(
                    create_sessionmaker(engine), settings, interval_s=0.01
                )
                assert task is not None
                await asyncio.wait_for(task, timeout=5)
            assert task.exception() is None
            assert calls["n"] == 11
            failures = [
                r
                for r in caplog.records
                if r.levelname == "WARNING" and "bootstrap failed" in r.getMessage()
            ]
            assert len(failures) == 1, [r.getMessage() for r in failures]
        finally:
            if task is not None and not task.done():
                task.cancel()
            await engine.dispose()

    asyncio.run(run())
