"""Auth, health-endpoint, and readiness-endpoint behavior."""

import secrets
import time
import uuid
from typing import Any

from curie_api.config import get_settings
from curie_api.sandbox_token import mint
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

DATABASE_UNAVAILABLE = {"detail": "Database is unavailable"}


def test_health_is_open(client: Any) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_is_open_and_checks_the_real_database(client: Any) -> None:
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_sanitizes_an_unavailable_database_and_health_stays_open(
    client: Any,
) -> None:
    failed_url = make_url(get_settings().database_url).set(
        host="127.0.0.1",
        port=1,
    )
    engine = create_async_engine(failed_url)
    failed_sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    original_sessionmaker = client.app.state.sessionmaker
    try:
        client.app.state.sessionmaker = failed_sessionmaker

        ready = client.get("/ready")
        assert ready.status_code == 503
        assert ready.json() == DATABASE_UNAVAILABLE
        assert "connection" not in ready.text.lower()
        assert "127.0.0.1" not in ready.text
        assert failed_url.render_as_string(hide_password=False) not in ready.text

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}
    finally:
        client.app.state.sessionmaker = original_sessionmaker
        client.portal.call(engine.dispose)


def test_ready_times_out_before_the_probe_when_the_real_pool_is_exhausted(
    client: Any,
) -> None:
    engine = create_async_engine(
        get_settings().database_url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=4,
    )
    constrained_sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    original_sessionmaker = client.app.state.sessionmaker
    held_connection: Any | None = None
    try:
        held_connection = client.portal.call(engine.connect)
        client.app.state.sessionmaker = constrained_sessionmaker

        started = time.monotonic()
        ready = client.get("/ready")
        elapsed = time.monotonic() - started

        assert ready.status_code == 503
        assert ready.json() == DATABASE_UNAVAILABLE
        assert 1.75 <= elapsed < 3.0
        assert (
            client.portal.call(held_connection.execute, text("SELECT 1")).scalar_one()
            == 1
        )

        client.portal.call(held_connection.close)
        held_connection = None
        recovered = client.get("/ready")
        assert recovered.status_code == 200
        assert recovered.json() == {"status": "ok"}
    finally:
        client.app.state.sessionmaker = original_sessionmaker
        if held_connection is not None:
            client.portal.call(held_connection.close)
        client.portal.call(engine.dispose)


def test_ready_is_unavailable_when_select_on_agents_is_denied(
    client: Any, auth_headers: dict[str, str]
) -> None:
    # Table owners can always SELECT their tables, so REVOKE on the app role
    # does nothing. A LOGIN role that can CONNECT and run SELECT 1 but has no
    # SELECT on curie.agents is the privilege denial that /ready must see.
    def quote_ident(ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    role = f"ready_probe_{secrets.token_hex(8)}"
    password = "ready-probe-placeholder"
    quoted_role = quote_ident(role)
    quoted_password = "'" + password.replace("'", "''") + "'"
    db_url = make_url(get_settings().database_url)
    quoted_db = quote_ident(db_url.database or "")
    quoted_schema = quote_ident(get_settings().db_schema)
    quoted_table = quote_ident("agents")

    admin_engine = create_async_engine(db_url)
    restricted_url = db_url.set(username=role, password=password)
    restricted_engine: Any | None = None
    original_sessionmaker = client.app.state.sessionmaker

    async def _create_role() -> None:
        async with admin_engine.begin() as conn:
            await conn.execute(
                text(
                    f"CREATE ROLE {quoted_role} LOGIN PASSWORD {quoted_password}"
                )
            )
            await conn.execute(
                text(f"GRANT CONNECT ON DATABASE {quoted_db} TO {quoted_role}")
            )
            await conn.execute(
                text(f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role}")
            )
            await conn.execute(
                text(
                    "REVOKE SELECT ON TABLE "
                    f"{quoted_schema}.{quoted_table} FROM {quoted_role}"
                )
            )

    async def _drop_role() -> None:
        async with admin_engine.begin() as conn:
            await conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE usename = :role AND pid <> pg_backend_pid()"
                ),
                {"role": role},
            )
            exists = (
                await conn.execute(
                    text("SELECT 1 FROM pg_roles WHERE rolname = :role"),
                    {"role": role},
                )
            ).first()
            if exists is None:
                return
            await conn.execute(text(f"DROP OWNED BY {quoted_role}"))
            await conn.execute(text(f"DROP ROLE {quoted_role}"))

    try:
        client.portal.call(_create_role)
        restricted_engine = create_async_engine(restricted_url)
        restricted_connection = client.portal.call(restricted_engine.connect)
        try:
            assert (
                client.portal.call(
                    restricted_connection.execute, text("SELECT 1")
                ).scalar_one()
                == 1
            )
        finally:
            client.portal.call(restricted_connection.close)

        client.app.state.sessionmaker = async_sessionmaker(
            restricted_engine, expire_on_commit=False
        )

        ready = client.get("/ready")
        assert ready.status_code == 503
        assert ready.json() == DATABASE_UNAVAILABLE
        assert "connection" not in ready.text.lower()
        assert "asyncpg" not in ready.text.lower()
        if db_url.host:
            assert db_url.host not in ready.text
        assert (
            restricted_url.render_as_string(hide_password=False) not in ready.text
        )

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}

        transport = client._transport
        raise_exceptions = transport.raise_server_exceptions
        try:
            transport.raise_server_exceptions = False
            agents = client.get("/agents", headers=auth_headers)
            assert agents.status_code == 500
        finally:
            transport.raise_server_exceptions = raise_exceptions
    finally:
        client.app.state.sessionmaker = original_sessionmaker
        if restricted_engine is not None:
            client.portal.call(restricted_engine.dispose)
        try:
            client.portal.call(_drop_role)
        finally:
            client.portal.call(admin_engine.dispose)


def test_agents_require_api_key(client: Any) -> None:
    assert client.get("/agents").status_code == 401
    assert (
        client.get("/agents", headers={"X-API-Key": "wrong"}).status_code == 401
    )


def test_agents_accept_valid_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    assert client.get("/agents", headers=auth_headers).status_code == 200


def test_scoped_state_token_is_rejected_on_a_crud_route(client: Any) -> None:
    # A scoped sandbox "state" token authorizes the state namespace only; it must
    # be rejected by the shared require_api_key guard on every other route, so the
    # rejection is not special-cased to approvals (#410).
    token = mint(
        get_settings().api_key,
        agent=str(uuid.uuid4()),
        scope="state",
        exp=4102444800,  # 2100-01-01, valid at test time
    )
    assert client.get("/agents", headers={"X-API-Key": token}).status_code == 401
