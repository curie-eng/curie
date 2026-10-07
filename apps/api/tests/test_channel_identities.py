"""Channel identities: table, admin CRUD and the static Slack bootstrap (#2909).

Migration 0083 adds ``channel_identities``: one row per channel identity --
one bot speaking through one connected account (ADR 0168 decision 1), not the
connected account itself, which is ``provider_installations`` (ADR 0193
decision 3; see ``test_provider_installations.py``). ``name`` is unique with
``(tenant_id, provider)``, so two identities can share one
``provider_installation_id`` (e.g. one Slack workspace, two bots) but never a
``name``. ``credential_ref`` and ``webhook_verification_ref`` are pointers
(``env:NAME`` or ``k8s-secret:name/key``), never secret values; the grammar is
a DB CHECK and an API check, and a rejected value is never echoed.

When ``SLACK_BOT_TOKEN`` is set, the API lifespan creates one static Slack row
at a fixed id in the default tenant, named ``default``, unattached (ADR 0193
decision 4: only the identity's own credential can later report which
installation it belongs to, #3039). Every other identity
``CURIE_SLACK_IDENTITIES`` declares with a configured bot token gets its own
row at a fresh id, named after that identity, also unattached. Every
bootstrapped row is keyed on "this (tenant, provider, name) already has a row"
so a rename, an operator-created row or a status change is never undone,
independently per name.

The DB-level tests share the session's migrated database, so every insert runs
inside an outer transaction that is always rolled back; an insert expected to
fail runs inside a SAVEPOINT so the outer transaction survives the error. The
route and bootstrap tests commit, so a local fixture truncates both tables
(and removes the tenants this module creates) before and after each test.
"""

from __future__ import annotations

import asyncio
import inspect
import json
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
# Rows this module creates outside channel_identities carry this prefix so
# the cleanup fixture removes exactly them and never the default tenant.
MARK = "ci-test-"
ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BASE = "/channel-identities"


def _fk_targets(column: Any) -> set[str]:
    return {fk.target_fullname for fk in column.foreign_keys}


def _unique_constraint_names(table: Table) -> set[str | None]:
    return {c.name for c in table.constraints if isinstance(c, UniqueConstraint)}


def _foreign_key_constraints(table: Table) -> dict[str | None, ForeignKeyConstraint]:
    return {c.name: c for c in table.constraints if isinstance(c, ForeignKeyConstraint)}


# --- ORM model shape -------------------------------------------------------


def test_channel_identity_model_shape() -> None:
    from curie_api.models import ChannelIdentity
    from sqlalchemy.dialects.postgresql import JSONB

    assert ChannelIdentity.__tablename__ == "channel_identities"
    columns = ChannelIdentity.__table__.c

    id_col = columns["id"]
    assert id_col.primary_key is True
    # See test_tenants.py: SQLAlchemy wraps a zero-arg callable default.
    assert inspect.unwrap(id_col.default.arg) is uuid.uuid4

    tenant_id_col = columns["tenant_id"]
    assert tenant_id_col.nullable is False
    assert _fk_targets(tenant_id_col) >= {"curie.tenants.id"}

    for name in ("provider", "status"):
        col = columns[name]
        assert col.type.python_type is str
        assert col.nullable is False

    for name in ("credential_ref", "webhook_verification_ref"):
        col = columns[name]
        assert col.type.python_type is str
        assert col.nullable is True

    assert columns["status"].server_default is not None
    assert columns["scopes"].nullable is False
    assert columns["scopes"].server_default is not None
    assert columns["installation_mismatch"].type.python_type is bool
    assert columns["installation_mismatch"].nullable is False
    assert columns["provider_installation_id"].nullable is True
    assert columns["created_at"].type.python_type is datetime
    assert columns["created_at"].nullable is False

    # ADR 0168 decision 1: one channel identity per row, named within the
    # tenant and provider; "default" is the one identity an install need not
    # name explicitly.
    name_col = columns["name"]
    assert name_col.type.python_type is str
    assert name_col.nullable is False
    assert name_col.default is not None
    assert name_col.server_default is not None

    attributes_col = columns["attributes"]
    assert isinstance(attributes_col.type, JSONB)
    assert attributes_col.nullable is False
    assert attributes_col.default is not None
    assert attributes_col.server_default is not None

    assert "channel_identities_tenant_provider_name_key" in _unique_constraint_names(
        ChannelIdentity.__table__
    )
    # The installation FK is composite so an identity can attach only to an
    # installation of its own tenant and provider.
    installation_fk = _foreign_key_constraints(ChannelIdentity.__table__)[
        "channel_identities_installation_fkey"
    ]
    assert installation_fk.column_keys == ["tenant_id", "provider", "provider_installation_id"]
    assert [e.target_fullname for e in installation_fk.elements] == [
        "curie.provider_installations.tenant_id",
        "curie.provider_installations.provider",
        "curie.provider_installations.id",
    ]


def test_static_slack_identity_id_is_fixed() -> None:
    from curie_api.channel_identities import STATIC_SLACK_IDENTITY_ID

    assert str(STATIC_SLACK_IDENTITY_ID) == STATIC_ID


@pytest.mark.parametrize(
    "value",
    ["env:SLACK_BOT_TOKEN", "env:_X1", "k8s-secret:curie-slack/bot-token", "k8s-secret:a.b/K_1.x"],
)
def test_reference_grammar_accepts_pointers(value: str) -> None:
    from curie_api.channel_identities import REFERENCE_RE

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
    from curie_api.channel_identities import REFERENCE_RE

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
    "INSERT INTO curie.channel_identities "
    "(id, tenant_id, provider, name, credential_ref, webhook_verification_ref, "
    "status, provider_installation_id) "
    "VALUES (:id, :tenant_id, :provider, :name, :credential_ref, "
    ":webhook_verification_ref, :status, :provider_installation_id)"
)


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": DEFAULT_TENANT_UUID,
        "provider": "slack",
        # Unique per call: several rows in one test body must not collide on
        # (tenant, provider, name) by accident.
        "name": f"identity-{uuid.uuid4().hex[:8]}",
        "credential_ref": None,
        "webhook_verification_ref": None,
        "status": "active",
        "provider_installation_id": None,
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


async def _insert_installation(
    conn: AsyncConnection, tenant_id: uuid.UUID, *, provider: str = "slack"
) -> uuid.UUID:
    installation_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.provider_installations "
        "(id, tenant_id, provider, external_account_id) "
        "VALUES (:id, :tenant_id, :provider, :external_account_id)",
        {
            "id": installation_id,
            "tenant_id": tenant_id,
            "provider": provider,
            "external_account_id": f"T{uuid.uuid4().hex[:8]}",
        },
    )
    return installation_id


def test_insert_applies_defaults(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> list[dict[str, Any]]:
        identity_id = uuid.uuid4()
        await _exec(
            conn,
            "INSERT INTO curie.channel_identities (id, tenant_id, provider) "
            "VALUES (:id, :tenant_id, 'github')",
            {"id": identity_id, "tenant_id": DEFAULT_TENANT_UUID},
        )
        return await _exec(
            conn,
            "SELECT name, attributes, status, scopes, installation_mismatch, "
            "provider_installation_id, created_at "
            "FROM curie.channel_identities WHERE id = :id",
            {"id": identity_id},
        )

    (row,) = _rolled_back(body)
    assert row["name"] == "default"
    assert row["attributes"] == {}
    assert row["status"] == "active"
    assert row["scopes"] == []
    assert row["installation_mismatch"] is False
    assert row["provider_installation_id"] is None
    assert row["created_at"] is not None


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        ({"provider": "myspace"}, "channel_identities_provider_ck"),
        ({"status": "paused"}, "channel_identities_status_ck"),
        # Storage itself cannot hold a raw token, whoever writes it.
        ({"credential_ref": RAW_TOKEN}, "channel_identities_credential_ref_ck"),
        (
            {"webhook_verification_ref": RAW_TOKEN},
            "channel_identities_webhook_verification_ref_ck",
        ),
        (
            {"credential_ref": "env:" + "A" * 520},
            "channel_identities_credential_ref_ck",
        ),
        ({"tenant_id": uuid.uuid4()}, "channel_identities_tenant_id_fkey"),
    ],
    ids=[
        "bad-provider",
        "bad-status",
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


@pytest.mark.parametrize("status", ["active", "disabled", "revoked"])
def test_valid_references_and_every_status_accepted(migrated: None, status: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(
            conn,
            _INSERT,
            _row(
                credential_ref="env:SLACK_BOT_TOKEN",
                webhook_verification_ref="k8s-secret:curie-slack/signing-secret",
                status=status,
            ),
        )

    _rolled_back(body)


def test_duplicate_tenant_provider_name_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _exec(conn, _INSERT, _row(name="dup-name"))
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(name="dup-name"),
            constraint="channel_identities_tenant_provider_name_key",
        )
        # The same name under another provider is a different identity.
        await _exec(conn, _INSERT, _row(provider="github", name="dup-name"))

    _rolled_back(body)


def test_identities_can_share_an_installation_with_different_names(migrated: None) -> None:
    """Two bots in one Slack workspace: one installation, two names.

    ADR 0168 decision 1's whole point -- a row is a channel identity, not a
    connected account -- carried over to ADR 0193's split: sharing an
    installation is no longer a collision.
    """

    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn, DEFAULT_TENANT_UUID)
        await _exec(
            conn, _INSERT, _row(name="default", provider_installation_id=installation)
        )
        await _exec(
            conn, _INSERT, _row(name="support-bot", provider_installation_id=installation)
        )

    _rolled_back(body)


def test_attach_to_installation_of_another_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        other_tenant = await _insert_tenant(conn)
        other_installation = await _insert_installation(conn, other_tenant)
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(provider_installation_id=other_installation),
            constraint="channel_identities_installation_fkey",
        )

    _rolled_back(body)


def test_attach_to_installation_of_another_provider_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        github_installation = await _insert_installation(
            conn, DEFAULT_TENANT_UUID, provider="github"
        )
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(provider="slack", provider_installation_id=github_installation),
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
        "IF to_regclass('curie.channel_identities') IS NOT NULL THEN "
        "TRUNCATE curie.channel_identities, curie.provider_installations; END IF; END $$"
    )
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def clean_identities(migrated: None) -> Iterator[None]:
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
def api(clean_identities: None, env: pytest.MonkeyPatch) -> Iterator[TestClient]:
    # The developer shell may export a real token; route tests boot without one
    # so no bootstrap row appears in their listings.
    with _app(env, "") as test_client:
        yield test_client


def _create(api: TestClient, headers: dict[str, str], **body: Any) -> dict[str, Any]:
    payload = {"provider": "slack"}
    payload.update(body)
    response = api.post(BASE, json=payload, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


def _committed_tenant() -> str:
    tenant_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    return str(tenant_id)


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
        kwargs["json"] = {"provider": "slack"}
    assert getattr(api, method)(path, **kwargs).status_code == 401
    assert getattr(api, method)(path, headers={"X-API-Key": "wrong"}, **kwargs).status_code == 401


def test_create_list_get_patch_delete(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(
        api,
        auth_headers,
        credential_ref="env:SLACK_BOT_TOKEN",
        webhook_verification_ref="k8s-secret:curie-slack/signing-secret",
        scopes=["chat:write", "users:read"],
    )
    assert created["tenant_id"] == DEFAULT_TENANT_ID
    assert created["provider"] == "slack"
    # Omitted in the request body: "default" is the one identity an install
    # need not name explicitly.
    assert created["name"] == "default"
    assert created["credential_ref"] == "env:SLACK_BOT_TOKEN"
    assert created["webhook_verification_ref"] == "k8s-secret:curie-slack/signing-secret"
    assert created["scopes"] == ["chat:write", "users:read"]
    assert created["attributes"] == {}
    assert created["status"] == "active"
    assert created["installation_mismatch"] is False
    assert created["provider_installation_id"] is None
    assert created["created_at"] is not None
    identity_id = created["id"]

    github = _create(api, auth_headers, provider="github")

    listed = api.get(BASE, headers=auth_headers)
    assert listed.status_code == 200
    assert [row["id"] for row in listed.json()] == [identity_id, github["id"]]
    only_slack = api.get(BASE, params={"provider": "slack"}, headers=auth_headers).json()
    assert [row["id"] for row in only_slack] == [identity_id]
    other_tenant = api.get(BASE, params={"tenant_id": str(uuid.uuid4())}, headers=auth_headers)
    assert other_tenant.status_code == 200
    assert other_tenant.json() == []

    fetched = api.get(f"{BASE}/{identity_id}", headers=auth_headers)
    assert fetched.status_code == 200
    assert fetched.json() == created

    patched = api.patch(
        f"{BASE}/{identity_id}",
        json={
            "scopes": ["chat:write"],
            "credential_ref": None,
            "name": "support-bot",
            "attributes": {"app_token_ref": "env:CURIE_SLACK_APP_TOKEN__1"},
        },
        headers=auth_headers,
    )
    assert patched.status_code == 200, patched.text
    body = patched.json()
    assert body["scopes"] == ["chat:write"]
    # null clears; a field left out of the body is untouched.
    assert body["credential_ref"] is None
    assert body["webhook_verification_ref"] == "k8s-secret:curie-slack/signing-secret"
    assert body["name"] == "support-bot"
    assert body["attributes"] == {"app_token_ref": "env:CURIE_SLACK_APP_TOKEN__1"}

    assert api.delete(f"{BASE}/{identity_id}", headers=auth_headers).status_code == 204
    assert api.get(f"{BASE}/{identity_id}", headers=auth_headers).status_code == 404


def test_create_attached_to_an_existing_installation(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    tenant_id = _committed_tenant()
    installation_id = _sql(
        "INSERT INTO curie.provider_installations (id, tenant_id, provider, external_account_id) "
        "VALUES (:id, :tenant_id, 'slack', 'T0ATTACH') RETURNING id",
        {"id": uuid.uuid4(), "tenant_id": tenant_id},
    )[0]["id"]
    created = _create(
        api,
        auth_headers,
        tenant_id=tenant_id,
        provider_installation_id=str(installation_id),
    )
    assert created["provider_installation_id"] == str(installation_id)


def test_create_attached_to_installation_of_another_tenant_returns_422(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    other_tenant = _committed_tenant()
    installation_id = _sql(
        "INSERT INTO curie.provider_installations (id, tenant_id, provider, external_account_id) "
        "VALUES (:id, :tenant_id, 'slack', 'T0OTHER') RETURNING id",
        {"id": uuid.uuid4(), "tenant_id": other_tenant},
    )[0]["id"]
    # Defaults to the default tenant, which does not own this installation.
    response = api.post(
        BASE,
        json={"provider": "slack", "provider_installation_id": str(installation_id)},
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_patch_attach_then_detach(api: TestClient, auth_headers: dict[str, str]) -> None:
    installation_id = _sql(
        "INSERT INTO curie.provider_installations (id, tenant_id, provider, external_account_id) "
        "VALUES (:id, :tenant_id, 'slack', 'T0PATCH') RETURNING id",
        {"id": uuid.uuid4(), "tenant_id": DEFAULT_TENANT_ID},
    )[0]["id"]
    created = _create(api, auth_headers)
    attached = api.patch(
        f"{BASE}/{created['id']}",
        json={"provider_installation_id": str(installation_id)},
        headers=auth_headers,
    )
    assert attached.status_code == 200, attached.text
    assert attached.json()["provider_installation_id"] == str(installation_id)

    detached = api.patch(
        f"{BASE}/{created['id']}",
        json={"provider_installation_id": None},
        headers=auth_headers,
    )
    assert detached.status_code == 200, detached.text
    assert detached.json()["provider_installation_id"] is None


def test_missing_id_returns_404(api: TestClient, auth_headers: dict[str, str]) -> None:
    # A real row first, so a 404 means "no such id", not "no such route".
    _create(api, auth_headers)
    missing = f"{BASE}/{uuid.uuid4()}"
    assert api.get(missing, headers=auth_headers).status_code == 404
    assert api.patch(missing, json={"status": "disabled"}, headers=auth_headers).status_code == 404
    assert api.delete(missing, headers=auth_headers).status_code == 404


def test_duplicate_returns_409(api: TestClient, auth_headers: dict[str, str]) -> None:
    # Both creates omit `name`, so both default to "default" -- the
    # collision is on (tenant, provider, name).
    _create(api, auth_headers)
    response = api.post(BASE, json={"provider": "slack"}, headers=auth_headers)
    assert response.status_code == 409


def test_patch_rename_collision_returns_409(api: TestClient, auth_headers: dict[str, str]) -> None:
    _create(api, auth_headers, name="taken")
    other = _create(api, auth_headers, name="other")
    response = api.patch(f"{BASE}/{other['id']}", json={"name": "taken"}, headers=auth_headers)
    assert response.status_code == 409
    unchanged = api.get(f"{BASE}/{other['id']}", headers=auth_headers).json()
    assert unchanged["name"] == "other"


def test_patch_name_to_null_rejected(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(api, auth_headers)
    response = api.patch(f"{BASE}/{created['id']}", json={"name": None}, headers=auth_headers)
    assert response.status_code == 422
    unchanged = api.get(f"{BASE}/{created['id']}", headers=auth_headers).json()
    assert unchanged["name"] == "default"


def test_patch_attributes_to_null_rejected(api: TestClient, auth_headers: dict[str, str]) -> None:
    created = _create(api, auth_headers)
    response = api.patch(f"{BASE}/{created['id']}", json={"attributes": None}, headers=auth_headers)
    assert response.status_code == 422
    unchanged = api.get(f"{BASE}/{created['id']}", headers=auth_headers).json()
    assert unchanged["attributes"] == {}


def test_attributes_cannot_smuggle_a_credential(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    # attributes is not a reference field -- it also holds plain identifiers
    # (a future team/app/bot user id) -- but a credential-shaped value nested
    # anywhere inside it must still be refused, not stored and echoed back.
    bad_post = api.post(
        BASE,
        json={"provider": "slack", "attributes": {"app_token_ref": CANARY}},
        headers=auth_headers,
    )
    assert bad_post.status_code == 422
    assert CANARY not in bad_post.text

    nested_post = api.post(
        BASE,
        json={"provider": "slack", "name": "attrs2", "attributes": {"nested": {"list": [CANARY]}}},
        headers=auth_headers,
    )
    assert nested_post.status_code == 422
    assert CANARY not in nested_post.text

    created = _create(api, auth_headers)
    bad_patch = api.patch(
        f"{BASE}/{created['id']}",
        json={"attributes": {"app_token_ref": CANARY}},
        headers=auth_headers,
    )
    assert bad_patch.status_code == 422
    assert CANARY not in bad_patch.text
    unchanged = api.get(f"{BASE}/{created['id']}", headers=auth_headers).json()
    assert unchanged["attributes"] == {}

    # A plain identifier -- not a reference, not a known credential shape --
    # is exactly what attributes is for, and must still be accepted.
    good_patch = api.patch(
        f"{BASE}/{created['id']}",
        json={"attributes": {"team_id": "T0123456"}},
        headers=auth_headers,
    )
    assert good_patch.status_code == 200, good_patch.text
    assert good_patch.json()["attributes"] == {"team_id": "T0123456"}


@pytest.mark.parametrize(
    "body",
    [
        {"provider": "myspace"},
        {"provider": "slack", "status": "paused"},
        {"provider": "slack", "name": ""},
    ],
    ids=["bad-provider", "bad-status", "empty-name"],
)
def test_invalid_body_returns_422(
    api: TestClient, auth_headers: dict[str, str], body: dict[str, Any]
) -> None:
    assert api.post(BASE, json=body, headers=auth_headers).status_code == 422


def test_unknown_tenant_returns_422(api: TestClient, auth_headers: dict[str, str]) -> None:
    response = api.post(
        BASE,
        json={"provider": "slack", "tenant_id": str(uuid.uuid4())},
        headers=auth_headers,
    )
    assert response.status_code == 422


# --- secret values are never accepted or echoed ----------------------------

_INVALID_REFS = [RAW_TOKEN, CANARY, "env:lower-" + CANARY, "k8s-secret:" + CANARY]


@pytest.mark.parametrize("field", ["credential_ref", "webhook_verification_ref"])
@pytest.mark.parametrize("value", _INVALID_REFS)
def test_post_with_raw_value_is_rejected_without_echo(
    api: TestClient, auth_headers: dict[str, str], field: str, value: str
) -> None:
    response = api.post(BASE, json={"provider": "slack", field: value}, headers=auth_headers)
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
        BASE, json={"provider": "slack", "credential_ref": value}, headers=auth_headers
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
        json={"provider": "slack", "credential_ref": "env:SLACK_BOT_TOKEN"},
        headers=auth_headers,
    )
    assert ok.status_code == 201, ok.text
    texts.append(ok.text)
    identity_id = ok.json()["id"]

    bad_post = api.post(
        BASE,
        json={"provider": "slack", "name": "c2", "credential_ref": CANARY},
        headers=auth_headers,
    )
    assert bad_post.status_code == 422
    texts.append(bad_post.text)

    conflict = api.post(
        BASE,
        json={"provider": "slack", "credential_ref": CANARY},
        headers=auth_headers,
    )
    # Either the grammar or the unique key rejects it; neither may echo it.
    assert conflict.status_code in (409, 422)
    texts.append(conflict.text)

    bad_patch = api.patch(
        f"{BASE}/{identity_id}",
        json={"webhook_verification_ref": CANARY},
        headers=auth_headers,
    )
    assert bad_patch.status_code == 422
    texts.append(bad_patch.text)

    good_patch = api.patch(
        f"{BASE}/{identity_id}",
        json={"webhook_verification_ref": "k8s-secret:curie-slack/signing-secret"},
        headers=auth_headers,
    )
    assert good_patch.status_code == 200
    texts.append(good_patch.text)

    texts.append(api.get(f"{BASE}/{identity_id}", headers=auth_headers).text)
    texts.append(api.get(BASE, headers=auth_headers).text)

    for body in texts:
        assert CANARY not in body


# --- static Slack bootstrap through the real lifespan ----------------------


def _slack_rows() -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, tenant_id, name, credential_ref, "
        "webhook_verification_ref, attributes, status, scopes, provider_installation_id "
        "FROM curie.channel_identities WHERE provider = 'slack' ORDER BY created_at, id"
    )


def test_bootstrap_creates_one_static_row(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        response = app.get(BASE, headers=auth_headers)
    assert response.status_code == 200
    assert CANARY not in response.text
    (row,) = response.json()
    assert row["id"] == STATIC_ID
    assert row["tenant_id"] == DEFAULT_TENANT_ID
    assert row["provider"] == "slack"
    assert row["name"] == "default"
    assert row["credential_ref"] == "env:SLACK_BOT_TOKEN"
    assert row["webhook_verification_ref"] == "env:SLACK_SIGNING_SECRET"
    assert row["attributes"] == {"app_token_ref": "env:SLACK_APP_TOKEN"}
    assert row["status"] == "active"
    assert row["scopes"] == []
    assert row["provider_installation_id"] is None

    # A second boot finds the row and adds nothing.
    with _app(env, CANARY) as app:
        again = app.get(BASE, headers=auth_headers)
    assert CANARY not in again.text
    assert [r["id"] for r in again.json()] == [STATIC_ID]


def test_bootstrap_without_token_creates_nothing(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, "") as app:
        response = app.get(BASE, headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == []


def test_renamed_static_row_survives_restart_without_duplicating(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    # The static row's id is fixed (STATIC_ID) on every boot, unlike a declared
    # identity's. Renaming it away from "default" must not make the next boot
    # try to re-insert at that same id. Calling `bootstrap_static_slack`
    # directly (not through the lifespan, like `_bootstrap_once` above) is the
    # point: `start_static_slack_bootstrap`'s retry loop swallows any
    # exception and just keeps retrying, so going through the lifespan would
    # pass this test whether or not the dedup check actually works (#3040
    # review: a name-only check tries to re-INSERT at the same primary key and
    # raises `IntegrityError`, caught and silently retried forever).
    with _app(env, CANARY) as app:
        patched = app.patch(
            f"{BASE}/{STATIC_ID}", json={"name": "renamed-default"}, headers=auth_headers
        )
        assert patched.status_code == 200, patched.text
    assert asyncio.run(_bootstrap_once(CANARY)) is True
    rows = _slack_rows()
    assert [(str(r["id"]), r["name"]) for r in rows] == [(STATIC_ID, "renamed-default")]


def test_disabled_static_row_is_not_resurrected(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        patched = app.patch(
            f"{BASE}/{STATIC_ID}", json={"status": "disabled"}, headers=auth_headers
        )
        assert patched.status_code == 200, patched.text
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert len(listed) == 1
    assert listed[0]["id"] == STATIC_ID
    assert listed[0]["status"] == "disabled"


def test_operator_created_slack_row_suppresses_bootstrap(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, "") as app:
        operator_row = _create(app, auth_headers, credential_ref="env:MY_TOKEN")
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert [r["id"] for r in listed] == [operator_row["id"]]
    assert operator_row["id"] != STATIC_ID


def test_deleting_every_slack_row_lets_next_boot_recreate_it(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, CANARY) as app:
        assert app.delete(f"{BASE}/{STATIC_ID}", headers=auth_headers).status_code == 204
        assert app.get(BASE, headers=auth_headers).json() == []
    with _app(env, CANARY) as app:
        listed = app.get(BASE, headers=auth_headers).json()
    assert [r["id"] for r in listed] == [STATIC_ID]


def test_other_provider_rows_do_not_suppress_bootstrap(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    with _app(env, "") as app:
        _create(app, auth_headers, provider="github")
    with _app(env, CANARY) as app:
        slack = app.get(BASE, params={"provider": "slack"}, headers=auth_headers).json()
    assert [r["id"] for r in slack] == [STATIC_ID]


# --- declared Slack identities beyond "default" (ADR 0168 decision 1) -------

# "default" must appear with the exact legacy env names whenever the list is
# non-empty (aci_protocol.slack_identities._check_declarations); every other
# identity reads an indexed CURIE_SLACK_*__<n> name.
_IDENTITIES_WITH_SUPPORT_BOT = json.dumps(
    [
        {
            "name": "default",
            "app_token_env": "SLACK_APP_TOKEN",
            "bot_token_env": "SLACK_BOT_TOKEN",
            "signing_secret_env": "SLACK_SIGNING_SECRET",
        },
        {
            "name": "support-bot",
            "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
            "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1",
            "signing_secret_env": "CURIE_SLACK_SIGNING_SECRET__1",
        },
    ]
)


def test_declared_identity_gets_its_own_row(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    env.setenv("CURIE_SLACK_IDENTITIES", _IDENTITIES_WITH_SUPPORT_BOT)
    env.setenv("CURIE_SLACK_BOT_TOKEN__1", "xapp-support-TOKEN")
    with _app(env, CANARY) as app:
        response = app.get(BASE, params={"provider": "slack"}, headers=auth_headers)
    assert response.status_code == 200
    rows = {row["name"]: row for row in response.json()}
    assert set(rows) == {"default", "support-bot"}

    default_row = rows["default"]
    assert default_row["id"] == STATIC_ID
    assert default_row["credential_ref"] == "env:SLACK_BOT_TOKEN"

    support_row = rows["support-bot"]
    # A fresh id, not the fixed STATIC_ID: only "default" gets that one.
    assert support_row["id"] != STATIC_ID
    assert support_row["credential_ref"] == "env:CURIE_SLACK_BOT_TOKEN__1"
    assert support_row["webhook_verification_ref"] == "env:CURIE_SLACK_SIGNING_SECRET__1"
    assert support_row["attributes"] == {"app_token_ref": "env:CURIE_SLACK_APP_TOKEN__1"}


def test_declared_identity_without_bot_token_gets_no_row(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    env.setenv("CURIE_SLACK_IDENTITIES", _IDENTITIES_WITH_SUPPORT_BOT)
    # Declared, but its bot token env var is blank: no row, same as
    # identities.slack_bot_tokens would treat it as unconfigured.
    env.setenv("CURIE_SLACK_BOT_TOKEN__1", "")
    with _app(env, CANARY) as app:
        response = app.get(BASE, params={"provider": "slack"}, headers=auth_headers)
    assert [row["name"] for row in response.json()] == ["default"]


def test_operator_created_named_row_suppresses_only_that_name(
    clean_identities: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    # No identity declared yet for the first boot, so nothing auto-bootstraps
    # "support-bot" here -- the operator's row must be the first and only one.
    with _app(env, "") as app:
        operator_row = _create(
            app, auth_headers, name="support-bot", credential_ref="env:MY_TOKEN"
        )
    env.setenv("CURIE_SLACK_IDENTITIES", _IDENTITIES_WITH_SUPPORT_BOT)
    env.setenv("CURIE_SLACK_BOT_TOKEN__1", "xapp-support-TOKEN")
    with _app(env, CANARY) as app:
        listed = app.get(BASE, params={"provider": "slack"}, headers=auth_headers).json()
    rows = {row["name"]: row for row in listed}
    # "support-bot" is suppressed (operator-created); "default" still
    # bootstraps normally -- suppression is independent per name.
    assert set(rows) == {"default", "support-bot"}
    assert rows["support-bot"]["id"] == operator_row["id"]
    assert rows["default"]["id"] == STATIC_ID


def test_concurrent_bootstrap_of_named_identity_yields_one_row(
    clean_identities: None, env: pytest.MonkeyPatch
) -> None:
    from aci_protocol.slack_identities import (
        LEGACY_APP_TOKEN_ENV,
        LEGACY_BOT_TOKEN_ENV,
        LEGACY_SIGNING_SECRET_ENV,
        SlackIdentity,
    )
    from curie_api.channel_identities import bootstrap_static_slack
    from curie_api.db import create_sessionmaker

    env.setenv("CURIE_SLACK_BOT_TOKEN__1", CANARY)
    identities = (
        SlackIdentity(
            name="default",
            app_token_env=LEGACY_APP_TOKEN_ENV,
            bot_token_env=LEGACY_BOT_TOKEN_ENV,
            signing_secret_env=LEGACY_SIGNING_SECRET_ENV,
        ),
        SlackIdentity(
            name="support-bot",
            app_token_env="CURIE_SLACK_APP_TOKEN__1",
            bot_token_env="CURIE_SLACK_BOT_TOKEN__1",
            signing_secret_env="CURIE_SLACK_SIGNING_SECRET__1",
        ),
    )

    async def bootstrap_once() -> bool:
        # No SLACK_BOT_TOKEN: isolates this race to the named identity alone.
        settings = get_settings().model_copy(
            update={"slack_bot_token": "", "slack_identities": identities}
        )
        engine = create_async_engine(settings.database_url)
        try:
            return await bootstrap_static_slack(create_sessionmaker(engine), settings)
        finally:
            await engine.dispose()

    async def race() -> list[bool]:
        return list(await asyncio.gather(bootstrap_once(), bootstrap_once()))

    assert asyncio.run(race()) == [True, True]
    rows = _slack_rows()
    assert [r["name"] for r in rows] == ["support-bot"]
    assert rows[0]["credential_ref"] == "env:CURIE_SLACK_BOT_TOKEN__1"


# --- bootstrap_static_slack called directly --------------------------------


async def _bootstrap_once(token: str) -> bool:
    from curie_api.channel_identities import bootstrap_static_slack
    from curie_api.db import create_sessionmaker

    settings = get_settings().model_copy(update={"slack_bot_token": token})
    engine = create_async_engine(settings.database_url)
    try:
        return await bootstrap_static_slack(create_sessionmaker(engine), settings)
    finally:
        await engine.dispose()


def test_concurrent_bootstrap_yields_one_row(clean_identities: None) -> None:
    async def race() -> list[bool]:
        # Separate engines, so the two inserts really run on two connections.
        return list(await asyncio.gather(_bootstrap_once(CANARY), _bootstrap_once(CANARY)))

    assert asyncio.run(race()) == [True, True]
    rows = _slack_rows()
    assert [str(r["id"]) for r in rows] == [STATIC_ID]


def _alembic_config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _regclass(name: str) -> str | None:
    value = _sql("SELECT to_regclass(:name)::text AS name", {"name": name})[0]["name"]
    return None if value is None else str(value)


def test_bootstrap_tolerates_missing_table(isolated_migration_db: None) -> None:
    config = _alembic_config()
    command.upgrade(config, "0082")
    try:
        # A rolling upgrade can boot this image before this migration is applied.
        assert asyncio.run(_bootstrap_once(CANARY)) is False
        assert _regclass("curie.channel_identities") is None
    finally:
        command.upgrade(config, "head")
    # Once the table exists the same call finds it and bootstraps.
    assert asyncio.run(_bootstrap_once(CANARY)) is True
    assert [str(r["id"]) for r in _slack_rows()] == [STATIC_ID]


def test_lifespan_retries_until_table_appears(
    isolated_migration_db: None, env: pytest.MonkeyPatch, auth_headers: dict[str, str]
) -> None:
    config = _alembic_config()
    # 0082 is the candidate schema minimum and immediately precedes the
    # channel identity migration, so this models a supported rolling boot.
    command.upgrade(config, "0082")
    try:
        with _app(env, CANARY):
            # Boot succeeded below this migration; it now lands while the API runs.
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
        {"provider": "slack", "credential_ref": {"token": CANARY}},
        {"provider": "slack", "credential_ref": [CANARY]},
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
    from curie_api.channel_identities import is_reference

    assert is_reference(value) is False


@pytest.mark.parametrize("value", _BOUNDARY_ACCEPTED)
def test_is_reference_accepts_boundary_pointers(value: str) -> None:
    from curie_api.channel_identities import is_reference

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
            constraint="channel_identities_credential_ref_ck",
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
        BASE, json={"provider": "slack", "credential_ref": value}, headers=auth_headers
    )
    assert response.status_code == 422
    assert value not in response.text
    assert "xoxb" not in response.text


# --- background bootstrap logs a failure once --------------------------------


def test_repeated_bootstrap_failure_warns_once_then_finishes(
    migrated: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import curie_api.channel_identities as ci
    from curie_api.db import create_sessionmaker

    state = {"fail": True, "calls": 0}

    async def flaky(sessionmaker: Any, settings: Any) -> bool:
        state["calls"] += 1
        if state["fail"]:
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(ci, "bootstrap_static_slack", flaky)

    async def run() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        task = None
        try:
            with caplog.at_level("WARNING", logger="curie_api.channel_identities"):
                task = await ci.start_static_slack_bootstrap(
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


def test_persistent_failure_warns_again_after_warn_period(
    migrated: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failure that never clears re-warns every ``warn_period_s``, not once ever."""

    import curie_api.channel_identities as ci
    from curie_api.db import create_sessionmaker

    async def always_fails(sessionmaker: Any, settings: Any) -> bool:
        raise RuntimeError("boom")

    monkeypatch.setattr(ci, "bootstrap_static_slack", always_fails)

    async def run() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        task = None
        try:
            with caplog.at_level("WARNING", logger="curie_api.channel_identities"):
                task = await ci.start_static_slack_bootstrap(
                    create_sessionmaker(engine), settings, interval_s=0.01, warn_period_s=0.05
                )
                assert task is not None
                await asyncio.sleep(0.3)
            failures = [
                r
                for r in caplog.records
                if r.levelname == "WARNING" and "bootstrap failed" in r.getMessage()
            ]
            # ~0.3s / 0.05s warn period is roughly 6-7 warnings; a generous
            # range absorbs scheduling jitter without accepting one-shot or
            # every-10ms behavior.
            assert 2 <= len(failures) <= 12, [r.getMessage() for r in failures]
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
    from curie_api.channel_identities import is_reference

    assert is_reference(value) is False


@pytest.mark.parametrize("value", _LOOKALIKE_POINTERS)
def test_is_reference_accepts_lookalike_pointers(value: str) -> None:
    from curie_api.channel_identities import is_reference

    assert is_reference(value) is True


@pytest.mark.parametrize("value", _PASTED_CREDENTIALS)
def test_db_check_rejects_pasted_credentials(migrated: None, value: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(credential_ref=value),
            constraint="channel_identities_credential_ref_ck",
        )

    _rolled_back(body)


def test_db_check_rejects_pasted_webhook_secret(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(
            conn,
            _INSERT,
            _row(webhook_verification_ref="k8s-secret:x/" + "0123456789abcdef" * 2),
            constraint="channel_identities_webhook_verification_ref_ck",
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
    import curie_api.channel_identities as ci
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

    monkeypatch.setattr(ci, "bootstrap_static_slack", scripted)

    async def run() -> None:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        task = None
        try:
            with caplog.at_level("WARNING", logger="curie_api.channel_identities"):
                task = await ci.start_static_slack_bootstrap(
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
