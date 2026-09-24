"""identity_links and resolve_principal (#2910, ADR 0155 step 5).

Migration 0054 adds ``identity_links``: a provider-native id (a Slack user id)
on one provider installation, linked to exactly one subject -- a principal or a
bot (an Agent). ``resolve_principal`` answers "which principal is this provider
id?" by exact equality on ``(tenant, installation, native id)`` and nothing else:
it never reads a principal's email or display name and never folds case, trims
or prefix-matches. Unresolved is an answer (HTTP 200), not an error.

The DB-level and in-process tests share the session's migrated database, so
every insert runs inside an outer transaction that is always rolled back; an
insert expected to fail runs inside a SAVEPOINT so the outer transaction
survives the error. The route tests commit, so a local fixture removes what
they create before and after each test. There is no identity_links CRUD, so
links are seeded with SQL.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import UniqueConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_UUID = uuid.UUID(DEFAULT_TENANT_ID)
# Rows this module creates outside identity_links/provider_installations carry
# this prefix so the cleanup fixture removes exactly them.
MARK = "il-test-"
RESOLVE = "/identity/resolve"

FK_VIOLATION = "23503"
UNIQUE_VIOLATION = "23505"
CHECK_VIOLATION = "23514"


# --- ORM model shape -------------------------------------------------------


def test_identity_link_model_and_installation_tenant_key() -> None:
    from curie_api.models import IdentityLink, ProviderInstallation

    assert IdentityLink.__tablename__ == "identity_links"
    columns = IdentityLink.__table__.c
    for name in ("tenant_id", "provider_installation_id", "provider_native_id"):
        assert columns[name].nullable is False
    for name in ("principal_id", "bot_id", "created_by_principal_id"):
        assert columns[name].nullable is True
    assert columns["verification_source"].nullable is False
    assert columns["verified_at"].nullable is False
    names = {
        c.name
        for c in ProviderInstallation.__table__.constraints
        if isinstance(c, UniqueConstraint)
    }
    assert "provider_installations_tenant_id_id_key" in names


# --- seeding helpers -------------------------------------------------------


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
    sqlstate: str,
    constraint: str | None = None,
) -> str | None:
    """Assert the statement fails with ``sqlstate`` (and ``constraint`` when named).

    Returns the violated constraint's name.
    """
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(statement), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    assert getattr(cause, "sqlstate", None) == sqlstate, str(exc_info.value)
    name = getattr(cause, "constraint_name", None)
    if constraint is not None:
        assert name == constraint, str(exc_info.value)
    return name


async def _insert_tenant(conn: AsyncConnection) -> uuid.UUID:
    tenant_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.tenants (id, deployment_id, status) "
        "VALUES (:id, :deployment_id, 'active')",
        {"id": tenant_id, "deployment_id": f"{MARK}{tenant_id}"},
    )
    return tenant_id


async def _insert_principal(
    conn: AsyncConnection,
    tenant_id: uuid.UUID = DEFAULT_TENANT_UUID,
    *,
    status: str = "active",
    email: str | None = None,
    display_name: str | None = None,
) -> uuid.UUID:
    principal_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.principals "
        "(id, tenant_id, idp_subject, type, status, email, display_name) "
        "VALUES (:id, :tenant_id, :idp_subject, 'human', :status, :email, :display_name)",
        {
            "id": principal_id,
            "tenant_id": tenant_id,
            "idp_subject": f"{MARK}{principal_id}",
            "status": status,
            "email": email,
            "display_name": display_name,
        },
    )
    return principal_id


async def _insert_installation(
    conn: AsyncConnection,
    tenant_id: uuid.UUID = DEFAULT_TENANT_UUID,
    *,
    provider: str = "slack",
    external_account_id: str | None = None,
    status: str = "connected",
) -> uuid.UUID:
    installation_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.provider_installations "
        "(id, tenant_id, provider, external_account_id, status, disconnected_at) "
        "VALUES (:id, :tenant_id, :provider, :external_account_id, :status, :disconnected_at)",
        {
            "id": installation_id,
            "tenant_id": tenant_id,
            "provider": provider,
            "external_account_id": external_account_id or f"T{uuid.uuid4().hex[:8]}",
            "status": status,
            "disconnected_at": datetime(2026, 1, 1) if status == "disconnected" else None,
        },
    )
    return installation_id


async def _insert_agent(conn: AsyncConnection) -> uuid.UUID:
    agent_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"{MARK}{agent_id}"},
    )
    return agent_id


_INSERT_LINK = (
    "INSERT INTO curie.identity_links "
    "(id, tenant_id, principal_id, bot_id, provider_installation_id, "
    "provider_native_id, verification_source) "
    "VALUES (:id, :tenant_id, :principal_id, :bot_id, :provider_installation_id, "
    ":provider_native_id, :verification_source)"
)


def _link(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": DEFAULT_TENANT_UUID,
        "principal_id": None,
        "bot_id": None,
        "provider_installation_id": None,
        "provider_native_id": "U0ABC",
        "verification_source": "admin_mapped",
    }
    row.update(overrides)
    return row


async def _insert_link(conn: AsyncConnection, **overrides: Any) -> uuid.UUID:
    row = _link(**overrides)
    await _exec(conn, _INSERT_LINK, row)
    return row["id"]


# --- DB-level constraints --------------------------------------------------


def test_link_needs_exactly_one_subject(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        agent = await _insert_agent(conn)
        await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(provider_installation_id=installation, principal_id=principal, bot_id=agent),
            sqlstate=CHECK_VIOLATION,
            constraint="identity_links_subject_xor_ck",
        )
        await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(provider_installation_id=installation),
            sqlstate=CHECK_VIOLATION,
            constraint="identity_links_subject_xor_ck",
        )
        # Positive controls: each subject alone is accepted.
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        await _insert_link(
            conn,
            provider_installation_id=installation,
            bot_id=agent,
            provider_native_id="U0BOT",
            verification_source="provider_event_verified",
        )

    _rolled_back(body)


def test_two_principal_links_for_one_native_id_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        first = await _insert_principal(conn)
        second = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=first)
        await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(provider_installation_id=installation, principal_id=second),
            sqlstate=UNIQUE_VIOLATION,
            constraint="identity_links_principal_native_key",
        )
        # The same native id on another installation is a different identity.
        other_installation = await _insert_installation(conn)
        await _insert_link(conn, provider_installation_id=other_installation, principal_id=second)

    _rolled_back(body)


def test_two_bot_links_for_one_native_id_allowed(migrated: None) -> None:
    """One Slack bot user legitimately fronts several Agents (partial index scope)."""

    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        first_agent = await _insert_agent(conn)
        second_agent = await _insert_agent(conn)
        await _insert_link(
            conn,
            provider_installation_id=installation,
            bot_id=first_agent,
            provider_native_id="U0BOT",
        )
        await _insert_link(
            conn,
            provider_installation_id=installation,
            bot_id=second_agent,
            provider_native_id="U0BOT",
        )
        rows = await _exec(
            conn,
            "SELECT count(*) AS n FROM curie.identity_links WHERE provider_installation_id = :i",
            {"i": installation},
        )
        assert rows[0]["n"] == 2

    _rolled_back(body)


def test_principal_from_another_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        other_tenant = await _insert_tenant(conn)
        outsider = await _insert_principal(conn, other_tenant)
        await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(provider_installation_id=installation, principal_id=outsider),
            sqlstate=FK_VIOLATION,
        )
        # The same principal linked inside its own tenant is fine.
        own_installation = await _insert_installation(conn, other_tenant)
        await _insert_link(
            conn,
            tenant_id=other_tenant,
            provider_installation_id=own_installation,
            principal_id=outsider,
        )

    _rolled_back(body)


def test_installation_from_another_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        principal = await _insert_principal(conn)
        other_tenant = await _insert_tenant(conn)
        foreign_installation = await _insert_installation(conn, other_tenant)
        await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(provider_installation_id=foreign_installation, principal_id=principal),
            sqlstate=FK_VIOLATION,
        )
        # Positive control: the same principal on an installation in its tenant.
        installation = await _insert_installation(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)

    _rolled_back(body)


def test_created_by_principal_from_another_tenant_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        other_tenant = await _insert_tenant(conn)
        outsider = await _insert_principal(conn, other_tenant)
        statement = (
            "INSERT INTO curie.identity_links "
            "(id, tenant_id, principal_id, provider_installation_id, provider_native_id, "
            "verification_source, created_by_principal_id) "
            "VALUES (:id, :tenant_id, :principal_id, :installation, 'U0ABC', "
            "'admin_mapped', :creator)"
        )
        params = {
            "id": uuid.uuid4(),
            "tenant_id": DEFAULT_TENANT_UUID,
            "principal_id": principal,
            "installation": installation,
            "creator": outsider,
        }
        await _expect_integrity_error(conn, statement, params, sqlstate=FK_VIOLATION)
        await _exec(conn, statement, {**params, "id": uuid.uuid4(), "creator": principal})

    _rolled_back(body)


@pytest.mark.parametrize("source", ["guessed", "email_match", "", "ADMIN_MAPPED"])
def test_unknown_verification_source_rejected(migrated: None, source: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        name = await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(
                provider_installation_id=installation,
                principal_id=principal,
                verification_source=source,
            ),
            sqlstate=CHECK_VIOLATION,
        )
        assert name != "identity_links_subject_xor_ck"

    _rolled_back(body)


def test_empty_native_id_rejected(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        await _expect_integrity_error(
            conn,
            _INSERT_LINK,
            _link(
                provider_installation_id=installation, principal_id=principal, provider_native_id=""
            ),
            sqlstate=CHECK_VIOLATION,
        )

    _rolled_back(body)


def test_verified_at_defaults_to_now(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        link = await _insert_link(
            conn, provider_installation_id=installation, principal_id=principal
        )
        rows = await _exec(
            conn, "SELECT verified_at FROM curie.identity_links WHERE id = :id", {"id": link}
        )
        assert rows[0]["verified_at"] is not None

    _rolled_back(body)


# --- resolve_principal in process ------------------------------------------


async def _resolve(
    conn: AsyncConnection,
    *,
    installation: uuid.UUID,
    subject: str,
    tenant_id: uuid.UUID = DEFAULT_TENANT_UUID,
) -> Any:
    from curie_api.identity.service import resolve_principal

    session = AsyncSession(bind=conn, join_transaction_mode="create_savepoint")
    try:
        return await resolve_principal(
            session,
            tenant_id=tenant_id,
            provider_installation_id=installation,
            provider_subject=subject,
        )
    finally:
        await session.close()


def _assert_unresolved(result: Any, reason: str) -> None:
    assert result.status == "unresolved", result
    assert result.reason == reason, result
    assert result.principal_id is None, result


def test_resolution_is_a_frozen_dataclass() -> None:
    import dataclasses

    from curie_api.identity.service import PrincipalResolution

    resolution = PrincipalResolution(
        status="unresolved",
        principal_id=None,
        reason="no_link",
        provider_installation_id=None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        resolution.status = "resolved"  # type: ignore[misc]


def test_linked_active_principal_resolves(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject="U0ABC")
        assert result.status == "resolved"
        assert result.reason == "linked"
        assert result.principal_id == principal
        assert result.provider_installation_id == installation

    _rolled_back(body)


def test_unlinked_id_is_no_link(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject="U0OTHER")
        _assert_unresolved(result, "no_link")
        assert result.provider_installation_id == installation

    _rolled_back(body)


def test_email_or_display_name_equal_to_the_id_is_never_a_match(migrated: None) -> None:
    """NO GUESS: identity comes from the link, never from principal attributes."""

    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        await _insert_principal(conn, email="U0LOOKALIKE", display_name="U0LOOKALIKE")
        await _insert_principal(conn, email="u0lookalike@example.com", display_name="U0LOOKALIKE ")
        result = await _resolve(conn, installation=installation, subject="U0LOOKALIKE")
        _assert_unresolved(result, "no_link")

    _rolled_back(body)


@pytest.mark.parametrize("subject", ["u0abc", "U0ABC ", " U0ABC", "U0AB", "U0ABCD", "U0ABC\n"])
def test_near_miss_of_a_linked_id_is_no_link(migrated: None, subject: str) -> None:
    """NO GUESS: no case folding, trimming or prefix matching."""

    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject=subject)
        _assert_unresolved(result, "no_link")

    _rolled_back(body)


def test_empty_subject_is_no_link(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        result = await _resolve(conn, installation=installation, subject="")
        _assert_unresolved(result, "no_link")

    _rolled_back(body)


def test_link_on_another_installation_is_no_link(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        linked_installation = await _insert_installation(conn)
        queried_installation = await _insert_installation(conn)
        principal = await _insert_principal(conn)
        await _insert_link(
            conn, provider_installation_id=linked_installation, principal_id=principal
        )
        result = await _resolve(conn, installation=queried_installation, subject="U0ABC")
        _assert_unresolved(result, "no_link")

    _rolled_back(body)


def test_only_a_bot_link_is_linked_to_bot(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        agent = await _insert_agent(conn)
        second_agent = await _insert_agent(conn)
        await _insert_link(conn, provider_installation_id=installation, bot_id=agent)
        # Several bot links for one native id must not make resolution raise.
        await _insert_link(conn, provider_installation_id=installation, bot_id=second_agent)
        result = await _resolve(conn, installation=installation, subject="U0ABC")
        _assert_unresolved(result, "linked_to_bot")

    _rolled_back(body)


def test_principal_link_wins_over_a_bot_link(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        agent = await _insert_agent(conn)
        principal = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, bot_id=agent)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject="U0ABC")
        assert result.status == "resolved"
        assert result.reason == "linked"
        assert result.principal_id == principal

    _rolled_back(body)


@pytest.mark.parametrize("status", ["revoked", "disabled"])
def test_inactive_principal_is_unresolved_without_its_id(migrated: None, status: str) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn)
        principal = await _insert_principal(conn, status=status)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject="U0ABC")
        _assert_unresolved(result, "principal_inactive")

    _rolled_back(body)


def test_disconnected_installation_is_unresolved(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn, status="disconnected")
        principal = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject="U0ABC")
        _assert_unresolved(result, "installation_disconnected")

    _rolled_back(body)


def test_degraded_installation_still_resolves(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        installation = await _insert_installation(conn, status="degraded")
        principal = await _insert_principal(conn)
        await _insert_link(conn, provider_installation_id=installation, principal_id=principal)
        result = await _resolve(conn, installation=installation, subject="U0ABC")
        assert result.status == "resolved"
        assert result.reason == "linked"
        assert result.principal_id == principal

    _rolled_back(body)


def test_nonexistent_installation_is_installation_not_found(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        result = await _resolve(conn, installation=uuid.uuid4(), subject="U0ABC")
        _assert_unresolved(result, "installation_not_found")

    _rolled_back(body)


def test_installation_of_another_tenant_is_installation_not_found(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        other_tenant = await _insert_tenant(conn)
        foreign_installation = await _insert_installation(conn, other_tenant)
        outsider = await _insert_principal(conn, other_tenant)
        await _insert_link(
            conn,
            tenant_id=other_tenant,
            provider_installation_id=foreign_installation,
            principal_id=outsider,
        )
        # Queried from the default tenant: the tenant is in the predicate.
        result = await _resolve(conn, installation=foreign_installation, subject="U0ABC")
        _assert_unresolved(result, "installation_not_found")
        # Queried from its own tenant it resolves, so the link itself is valid.
        own = await _resolve(
            conn, installation=foreign_installation, subject="U0ABC", tenant_id=other_tenant
        )
        assert own.status == "resolved" and own.principal_id == outsider

    _rolled_back(body)


def test_find_provider_installation_matches_exactly(migrated: None) -> None:
    from curie_api.identity.service import find_provider_installation

    async def body(conn: AsyncConnection) -> None:
        external = f"T0{uuid.uuid4().hex[:8].upper()}"
        installation = await _insert_installation(conn, external_account_id=external)
        other_tenant = await _insert_tenant(conn)
        session = AsyncSession(bind=conn, join_transaction_mode="create_savepoint")
        try:

            async def find(**overrides: Any) -> uuid.UUID | None:
                kwargs: dict[str, Any] = {
                    "tenant_id": DEFAULT_TENANT_UUID,
                    "provider": "slack",
                    "external_account_id": external,
                }
                kwargs.update(overrides)
                return await find_provider_installation(session, **kwargs)

            assert await find() == installation
            assert await find(tenant_id=other_tenant) is None
            assert await find(provider="github") is None
            assert await find(external_account_id=external.lower()) is None
            assert await find(external_account_id=f"{external} ") is None
            assert await find(external_account_id="T0NOPE") is None
        finally:
            await session.close()

    _rolled_back(body)


# --- committed-state fixtures for the route --------------------------------


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                return await _exec(conn, statement, params)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _commit(body: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                return await body(conn)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _cleanup() -> None:
    # Tolerates the tables being absent so a missing migration fails the test
    # body with a clear error rather than erroring every fixture.
    _sql(
        "DO $$ BEGIN "
        "IF to_regclass('curie.identity_links') IS NOT NULL THEN "
        "TRUNCATE curie.identity_links; END IF; "
        "IF to_regclass('curie.provider_installations') IS NOT NULL THEN "
        "TRUNCATE curie.provider_installations CASCADE; END IF; END $$"
    )
    _sql("DELETE FROM curie.agents WHERE name LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.principals WHERE idp_subject LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def clean_identity(migrated: None) -> Iterator[None]:
    _cleanup()
    yield
    _cleanup()


@pytest.fixture
def settings_reset() -> Iterator[None]:
    yield
    get_settings.cache_clear()


@contextmanager
def _app(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    # No SLACK_BOT_TOKEN: the static Slack row would otherwise be bootstrapped.
    monkeypatch.setenv("SLACK_BOT_TOKEN", "")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()


@pytest.fixture
def api(
    clean_identity: None, settings_reset: None, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    with _app(monkeypatch) as test_client:
        yield test_client


@pytest.fixture
def seeded(clean_identity: None) -> dict[str, Any]:
    """One Slack installation (team T0ROUTE) in the default tenant with U0LINKED linked."""

    async def body(conn: AsyncConnection) -> dict[str, Any]:
        installation = await _insert_installation(conn, external_account_id="T0ROUTE")
        principal = await _insert_principal(conn)
        await _insert_link(
            conn,
            provider_installation_id=installation,
            principal_id=principal,
            provider_native_id="U0LINKED",
        )
        other_tenant = await _insert_tenant(conn)
        foreign_installation = await _insert_installation(conn, other_tenant)
        return {
            "installation": str(installation),
            "principal": str(principal),
            "foreign_installation": str(foreign_installation),
        }

    return _commit(body)


# --- route -----------------------------------------------------------------


def test_resolve_requires_api_key(api: TestClient) -> None:
    payload = {"provider": "slack", "external_account_id": "T0ROUTE", "provider_subject": "U1"}
    assert api.post(RESOLVE, json=payload).status_code == 401
    assert api.post(RESOLVE, json=payload, headers={"X-API-Key": "wrong"}).status_code == 401


def test_resolve_by_installation_id(
    api: TestClient, seeded: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    response = api.post(
        RESOLVE,
        json={"provider_installation_id": seeded["installation"], "provider_subject": "U0LINKED"},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "resolved",
        "principal_id": seeded["principal"],
        "reason": "linked",
        "provider_installation_id": seeded["installation"],
    }


def test_resolve_by_slack_team_id(
    api: TestClient, seeded: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    response = api.post(
        RESOLVE,
        json={
            "provider": "slack",
            "external_account_id": "T0ROUTE",
            "provider_subject": "U0LINKED",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "resolved",
        "principal_id": seeded["principal"],
        "reason": "linked",
        "provider_installation_id": seeded["installation"],
    }


def test_unlinked_subject_is_200_no_link(
    api: TestClient, seeded: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    response = api.post(
        RESOLVE,
        json={
            "provider": "slack",
            "external_account_id": "T0ROUTE",
            "provider_subject": "u0linked",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "unresolved"
    assert body["reason"] == "no_link"
    assert body["principal_id"] is None


def test_unknown_team_is_200_installation_not_found(
    api: TestClient, seeded: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    response = api.post(
        RESOLVE,
        json={
            "provider": "slack",
            "external_account_id": "T0OTHER",
            "provider_subject": "U0LINKED",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "unresolved",
        "principal_id": None,
        "reason": "installation_not_found",
        "provider_installation_id": None,
    }


def test_installation_id_of_another_tenant_is_installation_not_found(
    api: TestClient, seeded: dict[str, Any], auth_headers: dict[str, str]
) -> None:
    response = api.post(
        RESOLVE,
        json={
            "provider_installation_id": seeded["foreign_installation"],
            "provider_subject": "U0LINKED",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "unresolved"
    assert body["reason"] == "installation_not_found"
    assert body["principal_id"] is None


@pytest.mark.parametrize(
    "payload",
    [
        # Both locators.
        {
            "provider_installation_id": str(uuid.uuid4()),
            "provider": "slack",
            "external_account_id": "T0ROUTE",
            "provider_subject": "U0LINKED",
        },
        # Neither locator.
        {"provider_subject": "U0LINKED"},
        # Half of the (provider, external_account_id) locator.
        {"provider": "slack", "provider_subject": "U0LINKED"},
        {"external_account_id": "T0ROUTE", "provider_subject": "U0LINKED"},
        # Empty or missing subject.
        {"provider": "slack", "external_account_id": "T0ROUTE", "provider_subject": ""},
        {"provider": "slack", "external_account_id": "T0ROUTE"},
    ],
    ids=[
        "both-locators",
        "no-locator",
        "provider-only",
        "external-only",
        "empty-subject",
        "missing-subject",
    ],
)
def test_invalid_body_is_422(
    api: TestClient, auth_headers: dict[str, str], payload: dict[str, Any]
) -> None:
    response = api.post(RESOLVE, json=payload, headers=auth_headers)
    assert response.status_code == 422, response.text
