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

ADR-0106 now consumes the subject-bound session only for approval resolution;
the platform-key surface remains a separate administrative boundary.
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

SUBJECT = "U0EXAMPLE1"


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
    assert client.post("/console/login-codes", json={"subject": SUBJECT}).status_code == 401


def test_mint_then_exchange_sets_an_httponly_cookie_and_returns_no_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    minted = client.post(
        "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
    )
    assert minted.status_code == 201, minted.text
    assert minted.json()["subject"] == SUBJECT
    code = minted.json()["code"]

    # The exchange needs no credential of its own: the code IS the credential.
    exchanged = client.post("/console/session", json={"code": code})
    assert exchanged.status_code == 200, exchanged.text

    # The token must NOT be in the body -- that would hand the credential back to
    # the JavaScript this design keeps it away from.
    body = exchanged.json()
    assert "token" not in body and "session_token" not in body, body
    assert "expires_at" in body
    assert body["subject"] == SUBJECT

    # ... and the cookie must be HttpOnly, so page script cannot read it.
    set_cookie = exchanged.headers.get("set-cookie", "")
    assert SESSION_COOKIE in set_cookie, set_cookie
    assert "httponly" in set_cookie.lower(), set_cookie
    assert "samesite=strict" in set_cookie.lower().replace(" ", ""), set_cookie
    assert "secure" in set_cookie.lower(), set_cookie


def test_a_login_code_works_exactly_once(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    code = client.post(
        "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
    ).json()["code"]
    assert client.post("/console/session", json={"code": code}).status_code == 200
    # Second attempt: the code is consumed.
    again = client.post("/console/session", json={"code": code})
    assert again.status_code == 401, again.text


def test_an_unknown_code_fails_identically_to_a_consumed_one(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Indistinguishable failures, so the endpoint cannot be used to learn which
    # codes exist.
    used = client.post(
        "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
    ).json()["code"]
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

    code = client.post(
        "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
    ).json()["code"]
    client.post("/console/session", json={"code": code})

    async def read(session: AsyncSession) -> list[ConsoleSession]:
        return list((await session.execute(select(ConsoleSession))).scalars().all())

    rows = with_session(read)
    assert len(rows) == 1
    row = rows[0]
    assert row.subject == SUBJECT
    assert code not in f"{row.login_code_hash}{row.session_token_hash}"
    # Hashes, not values: hex SHA-256 is 64 characters.
    assert len(row.login_code_hash) == 64
    assert row.session_token_hash is not None and len(row.session_token_hash) == 64
    assert row.login_code_hash == crud.hash_console_credential(code)


def test_an_expired_code_cannot_be_exchanged(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        # Injected clock rather than sleeping: expiry is arithmetic, not a race.
        code, row = await crud.create_console_login_code(session, subject=SUBJECT)
        past = row.login_code_expires_at + timedelta(seconds=1)
        assert await crud.exchange_console_login_code(session, code, now=past) is None

    with_session(body)


def test_a_live_session_is_recognized_and_expiry_ends_it(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        code, _ = await crud.create_console_login_code(session, subject=SUBJECT)
        exchanged = await crud.exchange_console_login_code(session, code)
        assert exchanged is not None
        token, row = exchanged

        assert await crud.live_console_session(session, token) is not None
        assert row.subject == SUBJECT
        assert row.session_expires_at is not None
        after = row.session_expires_at + timedelta(seconds=1)
        assert await crud.live_console_session(session, token, now=after) is None

    with_session(body)


def test_revocation_kills_a_live_session_without_waiting_for_expiry(
    clean_db: None,
) -> None:
    """The reason this is a table and not a signed stateless token (ADR-0083)."""

    async def body(session: AsyncSession) -> None:
        code, _ = await crud.create_console_login_code(session, subject=SUBJECT)
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
        code, row = await crud.create_console_login_code(session, subject=SUBJECT)
        await crud.revoke_console_session(session, row)
        assert await crud.exchange_console_login_code(session, code) is None

    with_session(body)


# --- slice 2 (#1045): the session actually authorizes calls -------------------
#
# Slice 1 could mint a code, exchange it, and set a cookie -- and that cookie
# authorized nothing, because `require_api_key` still only compared the platform
# key. The console could "sign in" and every call it made came back 401, which is
# the state a browser tab was actually in.


def _sign_in(client: Any, auth_headers: dict[str, str]) -> None:
    """Mint, exchange, and put the resulting cookie where the client will send it.

    The cookie is `Secure`, and this client's base URL is plain http on a host
    that is not localhost, so httpx stores it and declines to send it. A real
    browser does send a Secure cookie to http://localhost, so dev is unaffected;
    what is under test here is the dependency, not the browser's cookie policy.
    """
    # Minting binds an administrator-selected subject to the row (ADR-0106), so
    # the body is not optional.
    code = client.post(
        "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
    ).json()["code"]
    exchanged = client.post("/console/session", json={"code": code})
    assert exchanged.status_code == 200, exchanged.text
    raw = exchanged.headers["set-cookie"]
    token = raw.split(f"{SESSION_COOKIE}=", 1)[1].split(";", 1)[0]
    client.cookies.set(SESSION_COOKIE, token)


def test_a_console_session_authorizes_a_normal_call(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # No credential at all: refused.
    assert client.get("/agents").status_code == 401

    _sign_in(client, auth_headers)

    # No header, no key in page scope, and the call is authorized.
    allowed = client.get("/agents")
    assert allowed.status_code == 200, allowed.text


def test_the_platform_key_still_works_untouched(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The machine path must be unchanged: the CLI, worker and runner all use it
    # and none of them has a cookie.
    assert client.get("/agents", headers=auth_headers).status_code == 200


def test_a_revoked_session_stops_authorizing(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Revocability is the property #630 asked for: a durable row a human can
    # kill, not a signed token that stays valid until it expires.
    _sign_in(client, auth_headers)
    assert client.get("/agents").status_code == 200

    async def revoke(session: AsyncSession) -> None:
        rows = (await session.execute(select(ConsoleSession))).scalars().all()
        for row in rows:
            await crud.revoke_console_session(session, row)
        await session.commit()

    with_session(revoke)

    refused = client.get("/agents")
    assert refused.status_code == 401, refused.text


def test_a_garbage_cookie_fails_like_a_missing_one(client: Any, clean_db: None) -> None:
    # One indistinguishable failure: a forged cookie must not be told it is
    # forged rather than expired.
    client.cookies.set(SESSION_COOKIE, "not-a-real-token")
    assert client.get("/agents").status_code == 401


def test_the_platform_key_needs_no_database_session(clean_db: None) -> None:
    """A machine caller must not depend on the session store being readable.

    ADR-0083 makes the ordering load-bearing for exactly this: "a machine caller
    returns before the session store is read, so a database outage cannot take
    the platform-key path down with it." Taking the session as a FastAPI
    dependency would break that invisibly -- the session opens while dependencies
    resolve, before the function body's ordering can matter -- so this asserts
    the guarantee against an app with no sessionmaker at all.
    """
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    from curie_api.auth import require_api_key

    app = FastAPI()

    @app.get("/probe", dependencies=[Depends(require_api_key)])
    async def probe() -> dict[str, bool]:
        return {"ok": True}

    # No `state.sessionmaker`: this app has no database at all.
    with TestClient(app) as bare:
        key = get_settings().api_key
        assert bare.get("/probe", headers={"X-API-Key": key}).status_code == 200
        # And a cookie-only caller is refused rather than crashing on the
        # missing store.
        bare.cookies.set(SESSION_COOKIE, "whatever")
        assert bare.get("/probe").status_code == 401


def test_signing_out_revokes_the_session_and_clears_the_cookie(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The half of the lifecycle that was missing: `revoke_console_session` was
    # written and reachable from nowhere, so a console could be signed in but
    # never out.
    _sign_in(client, auth_headers)
    assert client.get("/agents").status_code == 200

    out = client.delete("/console/session")
    assert out.status_code == 204, out.text
    # Cleared at the browser as well as revoked at the server, so a client that
    # kept the value cannot keep presenting it.
    assert SESSION_COOKIE in out.headers.get("set-cookie", "")

    # The token is dead even for a client that held on to it.
    assert client.get("/agents").status_code == 401


def test_signing_out_is_idempotent_and_says_nothing_about_the_token(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Never having been signed in, holding a token the server has never seen,
    # and signing out twice all answer the same. A different answer for a real
    # token would make this route an oracle for whether one is valid.
    assert client.delete("/console/session").status_code == 204

    client.cookies.set(SESSION_COOKIE, "not-a-real-token")
    assert client.delete("/console/session").status_code == 204
    client.cookies.delete(SESSION_COOKIE)

    _sign_in(client, auth_headers)
    assert client.delete("/console/session").status_code == 204
    assert client.delete("/console/session").status_code == 204


def test_signing_out_does_not_disturb_the_platform_key(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Machine callers carry no cookie and must be untouched by a console signing
    # itself out. ADR-0083's ordering exists so the two paths cannot interfere.
    _sign_in(client, auth_headers)
    assert client.delete("/console/session").status_code == 204
    assert client.get("/agents", headers=auth_headers).status_code == 200
