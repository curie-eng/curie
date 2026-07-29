"""The console session store and the login-code exchange (#1044, ADR-0083).

Real Postgres round-trip via the disposable-DB conftest, so migration 0018 is
exercised too: these tests fail if the table or its unique indexes did not land.

The properties asserted here are the security ones, because they are the reason
the table exists rather than the console simply holding the platform key:

- the credential is never stored in plaintext, so a database read cannot replay
  a session;
- a login code works exactly once;
- expiry and revocation are both expressed, and revocation is a column write
  rather than waiting out a token;
- the exchange response carries no token, only a cookie, so page script never
  sees the credential it authenticates with.

Nothing consumes a session yet -- `require_api_key` starts accepting one in slice
2 (#1045) -- so there is deliberately no test here that a session authorizes a
real request. What IS asserted is that the store can express liveness, which is
what slice 2 will build on.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from curie_api import crud
from curie_api.config import get_settings
from curie_api.models import ConsoleSession
from curie_api.routers.console import SESSION_COOKIE
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


def with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    """Run ``body`` against the disposable database.

    Builds its own engine the way conftest's own ``_truncate`` does, rather than
    via a fixture: this repo configures no pytest-asyncio, so an async test would
    silently not run. Callers depend on ``clean_db`` for isolation.
    """

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


# --- the HTTP surface ------------------------------------------------------


def test_minting_requires_the_platform_key(client: Any, clean_db: None) -> None:
    # Minting is an administrative act; only the CLI, holding the platform key,
    # should be able to do it.
    assert client.post("/console/login-codes").status_code == 401


def test_mint_then_exchange_sets_an_httponly_cookie_and_returns_no_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    minted = client.post("/console/login-codes", headers=auth_headers)
    assert minted.status_code == 201, minted.text
    code = minted.json()["code"]

    # The exchange needs no credential of its own: the code IS the credential.
    exchanged = client.post("/console/session", json={"code": code})
    assert exchanged.status_code == 200, exchanged.text

    # The token must NOT be in the body -- that would hand the credential back to
    # the JavaScript this design keeps it away from.
    body = exchanged.json()
    assert "token" not in body and "session_token" not in body, body
    assert "expires_at" in body

    # ... and the cookie must be HttpOnly, so page script cannot read it.
    set_cookie = exchanged.headers.get("set-cookie", "")
    assert SESSION_COOKIE in set_cookie, set_cookie
    assert "httponly" in set_cookie.lower(), set_cookie
    assert "samesite=strict" in set_cookie.lower().replace(" ", ""), set_cookie
    assert "secure" in set_cookie.lower(), set_cookie


def test_a_login_code_works_exactly_once(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    code = client.post("/console/login-codes", headers=auth_headers).json()["code"]
    assert client.post("/console/session", json={"code": code}).status_code == 200
    # Second attempt: the code is consumed.
    again = client.post("/console/session", json={"code": code})
    assert again.status_code == 401, again.text


def test_an_unknown_code_fails_identically_to_a_consumed_one(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Indistinguishable failures, so the endpoint cannot be used to learn which
    # codes exist.
    used = client.post("/console/login-codes", headers=auth_headers).json()["code"]
    client.post("/console/session", json={"code": used})

    consumed = client.post("/console/session", json={"code": used})
    unknown = client.post("/console/session", json={"code": "not-a-real-code"})
    assert consumed.status_code == unknown.status_code == 401
    assert consumed.json()["detail"] == unknown.json()["detail"]


# --- the store's own properties -------------------------------------------


def test_no_plaintext_credential_is_ever_stored(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The property that makes a database dump useless to an attacker."""

    code = client.post("/console/login-codes", headers=auth_headers).json()["code"]
    client.post("/console/session", json={"code": code})

    async def read(session: AsyncSession) -> list[ConsoleSession]:
        return list((await session.execute(select(ConsoleSession))).scalars().all())

    rows = with_session(read)
    assert len(rows) == 1
    row = rows[0]
    assert code not in f"{row.login_code_hash}{row.session_token_hash}"
    # Hashes, not values: hex SHA-256 is 64 characters.
    assert len(row.login_code_hash) == 64
    assert row.session_token_hash is not None and len(row.session_token_hash) == 64
    assert row.login_code_hash == crud.hash_console_credential(code)


def test_an_expired_code_cannot_be_exchanged(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        # Injected clock rather than sleeping: expiry is arithmetic, not a race.
        code, row = await crud.create_console_login_code(session)
        past = row.login_code_expires_at + timedelta(seconds=1)
        assert await crud.exchange_console_login_code(session, code, now=past) is None

    with_session(body)


def test_a_live_session_is_recognized_and_expiry_ends_it(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        code, _ = await crud.create_console_login_code(session)
        exchanged = await crud.exchange_console_login_code(session, code)
        assert exchanged is not None
        token, row = exchanged

        assert await crud.live_console_session(session, token) is not None
        assert row.session_expires_at is not None
        after = row.session_expires_at + timedelta(seconds=1)
        assert await crud.live_console_session(session, token, now=after) is None

    with_session(body)


def test_revocation_kills_a_live_session_without_waiting_for_expiry(
    clean_db: None,
) -> None:
    """The reason this is a table and not a signed stateless token (ADR-0083)."""

    async def body(session: AsyncSession) -> None:
        code, _ = await crud.create_console_login_code(session)
        exchanged = await crud.exchange_console_login_code(session, code)
        assert exchanged is not None
        token, row = exchanged
        assert await crud.live_console_session(session, token) is not None

        await crud.revoke_console_session(session, row)
        # Still well inside its expiry window, and no longer valid.
        assert await crud.live_console_session(session, token) is None

    with_session(body)


def test_a_revoked_row_cannot_still_have_its_code_exchanged(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        code, row = await crud.create_console_login_code(session)
        await crud.revoke_console_session(session, row)
        assert await crud.exchange_console_login_code(session, code) is None

    with_session(body)
