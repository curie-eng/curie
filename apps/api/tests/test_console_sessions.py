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
import secrets
import socket
import threading
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import httpx
import pytest
import uvicorn
from curie_api.config import get_settings
from curie_api.crud import console as crud_console
from curie_api.main import create_app
from curie_api.models import ConsoleSession
from curie_api.routers.console import SESSION_COOKIE
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

SUBJECT = "U0EXAMPLE1"
POST_SESSION_BUDGET = 30
GET_SESSION_BUDGET = 120


def _client_address() -> str:
    """Give each test a fresh address inside the documentation IPv6 range."""

    suffix = secrets.token_hex(8)
    return "2001:db8::" + ":".join(suffix[index : index + 4] for index in range(0, 16, 4))


@contextmanager
def _served_with_default_proxy_policy() -> Iterator[str]:
    """Serve the real API with Uvicorn reading the shipped trust environment."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(64)
    url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(),
            proxy_headers=True,
            log_level="warning",
            access_log=False,
        )
    )
    thread = threading.Thread(
        target=lambda: asyncio.run(server.serve(sockets=[sock])), daemon=True
    )
    try:
        thread.start()
        deadline = time.monotonic() + 30
        while not server.started:
            assert thread.is_alive(), "the API server exited during startup"
            assert time.monotonic() < deadline, "the API server did not start"
            time.sleep(0.05)
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        sock.close()
        assert not thread.is_alive(), "the API server did not stop"


def _loopback_peer() -> str:
    """A private source address prevents rate limit state from crossing tests."""

    return f"127.{secrets.randbelow(254) + 1}.{secrets.randbelow(256)}.{secrets.randbelow(254) + 1}"


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
    # Host-only: the __Host- prefix plus no Domain attribute, so a sibling
    # host cannot be fixed as the cookie's scope.
    set_cookie = exchanged.headers.get("set-cookie", "")
    assert SESSION_COOKIE == "__Host-curie_console_session"
    assert SESSION_COOKIE in set_cookie, set_cookie
    assert "domain=" not in set_cookie.lower(), set_cookie
    assert "httponly" in set_cookie.lower(), set_cookie
    assert "samesite=strict" in set_cookie.lower().replace(" ", ""), set_cookie
    assert "secure" in set_cookie.lower(), set_cookie
    assert "path=/" in set_cookie.lower().replace(" ", ""), set_cookie


def test_the_legacy_cookie_name_is_not_a_console_session(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """Readers accept only the host-only name, even for a live token."""

    code = client.post(
        "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
    ).json()["code"]
    exchanged = client.post("/console/session", json={"code": code})
    assert exchanged.status_code == 200, exchanged.text
    token = client.cookies.get(SESSION_COOKIE)
    assert token
    client.cookies.clear()

    legacy = client.get(
        "/console/session",
        headers={"Cookie": f"curie_console_session={token}"},
    )
    assert legacy.status_code == 401, legacy.text


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


def test_exchange_budget_is_shared_across_replicas_and_rejects_before_database(
    clean_db: None, auth_headers: dict[str, str]
) -> None:
    address = _client_address()
    with (
        TestClient(create_app(), client=(address, 5000)) as first,
        TestClient(create_app(), client=(address, 5001)) as second,
    ):
        minted = first.post(
            "/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers
        )
        assert minted.status_code == 201, minted.text
        code = minted.json()["code"]

        for index in range(POST_SESSION_BUDGET):
            replica = first if index % 2 == 0 else second
            # Header values are attacker controlled. The ASGI server supplies
            # the client address after applying its trusted proxy policy.
            response = replica.post(
                "/console/session",
                json={"code": "not-a-real-code"},
                headers={"X-Forwarded-For": _client_address()},
            )
            assert response.status_code == 401, response.text

        statements: list[str] = []

        def observed_query(
            _connection: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            statements.append(statement)

        engine = second.app.state.engine.sync_engine
        event.listen(engine, "before_cursor_execute", observed_query)
        try:
            refused = second.post(
                "/console/session",
                json={"code": code},
                headers={"X-Forwarded-For": _client_address()},
            )
        finally:
            event.remove(engine, "before_cursor_execute", observed_query)
        assert refused.status_code == 429, refused.text
        assert int(refused.headers["Retry-After"]) > 0
        assert not statements, statements

        # The rejected request did not consume the code. A second client can
        # still complete the ordinary exchange through the same application.
        with TestClient(create_app(), client=(_client_address(), 5002)) as other:
            exchanged = other.post("/console/session", json={"code": code})
            assert exchanged.status_code == 200, exchanged.text
            assert exchanged.json()["subject"] == SUBJECT
            token = other.cookies.get(SESSION_COOKIE)
            assert token
            current = other.get(
                "/console/session", headers={"Cookie": f"{SESSION_COOKIE}={token}"}
            )
            assert current.status_code == 200, current.text
            assert current.json()["subject"] == SUBJECT


def test_current_session_budget_is_shared_across_replicas_and_rejects_before_database(
    clean_db: None,
) -> None:
    address = _client_address()
    with (
        TestClient(create_app(), client=(address, 5000)) as first,
        TestClient(create_app(), client=(address, 5001)) as second,
    ):
        for index in range(GET_SESSION_BUDGET):
            replica = first if index % 2 == 0 else second
            response = replica.get("/console/session")
            assert response.status_code == 401, response.text

        statements: list[str] = []

        def observed_query(
            _connection: Any,
            _cursor: Any,
            statement: str,
            _parameters: Any,
            _context: Any,
            _executemany: bool,
        ) -> None:
            statements.append(statement)

        engine = second.app.state.engine.sync_engine
        event.listen(engine, "before_cursor_execute", observed_query)
        try:
            refused = second.get("/console/session")
        finally:
            event.remove(engine, "before_cursor_execute", observed_query)
        assert refused.status_code == 429, refused.text
        assert int(refused.headers["Retry-After"]) > 0
        assert not statements, statements

        with TestClient(create_app(), client=(_client_address(), 5002)) as other:
            assert other.get("/console/session").status_code == 401


@pytest.mark.parametrize(
    ("method", "budget"), [("POST", POST_SESSION_BUDGET), ("GET", GET_SESSION_BUDGET)]
)
def test_uvicorn_ignores_spoofed_forwarded_addresses_with_shipped_default(
    monkeypatch: pytest.MonkeyPatch, clean_db: None, method: str, budget: int
) -> None:
    # This is the environment shipped by the image, chart and Compose. Config
    # reads it itself; passing forwarded_allow_ips would bypass that boundary.
    monkeypatch.setenv("FORWARDED_ALLOW_IPS", "")
    peer = _loopback_peer()
    other_peer = _loopback_peer()
    while other_peer == peer:
        other_peer = _loopback_peer()
    payload = {"code": "not-a-real-code"} if method == "POST" else None
    with _served_with_default_proxy_policy() as url:
        transport = httpx.HTTPTransport(local_address=peer)
        with httpx.Client(base_url=url, transport=transport, timeout=5) as http:
            for _ in range(budget):
                response = http.request(
                    method,
                    "/console/session",
                    json=payload,
                    headers={
                        "X-Forwarded-For": f"{_client_address()}, {_client_address()}",
                        "Forwarded": f"for=\"[{_client_address()}]\"",
                    },
                )
                assert response.status_code == 401, response.text

            refused = http.request(
                method,
                "/console/session",
                json=payload,
                headers={
                    "X-Forwarded-For": _client_address(),
                    "Forwarded": f"for=\"[{_client_address()}]\"",
                },
            )
            assert refused.status_code == 429, refused.text
            assert int(refused.headers["Retry-After"]) > 0

        with httpx.Client(
            base_url=url,
            transport=httpx.HTTPTransport(local_address=other_peer),
            timeout=5,
        ) as other:
            independent = other.request(method, "/console/session", json=payload)
            assert independent.status_code == 401, independent.text


@pytest.mark.parametrize(
    "allowed_peers", ["*", "127.0.0.1", "192.0.2.1/32", "127.0.0.1,*", "::/0"]
)
def test_api_startup_rejects_every_nonempty_proxy_trust_setting(
    monkeypatch: pytest.MonkeyPatch, clean_db: None, allowed_peers: str
) -> None:
    monkeypatch.setenv("FORWARDED_ALLOW_IPS", allowed_peers)
    with pytest.raises((ValueError, RuntimeError), match="FORWARDED_ALLOW_IPS"):
        with TestClient(create_app()):
            pass


@pytest.mark.parametrize("method", ["POST", "GET"])
def test_session_rate_limit_fails_closed_when_valkey_is_unavailable(
    clean_db: None, method: str
) -> None:
    # A bound socket without a listener guarantees a refused connection while
    # preventing another process from allocating this private endpoint.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        with TestClient(create_app(), client=(_client_address(), 5000)) as http:
            original_valkey = http.app.state.valkey
            valkey = Redis(
                host="127.0.0.1",
                port=unavailable.getsockname()[1],
                socket_connect_timeout=1,
                socket_timeout=1,
            )
            http.app.state.valkey = valkey
            statements: list[str] = []

            def observed_query(
                _connection: Any,
                _cursor: Any,
                statement: str,
                _parameters: Any,
                _context: Any,
                _executemany: bool,
            ) -> None:
                statements.append(statement)

            engine = http.app.state.engine.sync_engine
            event.listen(engine, "before_cursor_execute", observed_query)
            try:
                payload = {"code": "not-a-real-code"} if method == "POST" else None
                refused = http.request(method, "/console/session", json=payload)
            finally:
                event.remove(engine, "before_cursor_execute", observed_query)
                http.app.state.valkey = original_valkey
                assert http.portal is not None
                http.portal.call(valkey.aclose)
            assert refused.status_code == 503, refused.text
            assert refused.json()["detail"] == "rate limiter unavailable"
            assert refused.headers["Cache-Control"] == "no-store"
            assert not statements, statements


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
    assert row.login_code_hash == crud_console.hash_console_credential(code)


def test_an_expired_code_cannot_be_exchanged(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        # Injected clock rather than sleeping: expiry is arithmetic, not a race.
        code, row = await crud_console.create_console_login_code(session, subject=SUBJECT)
        past = row.login_code_expires_at + timedelta(seconds=1)
        assert await crud_console.exchange_console_login_code(session, code, now=past) is None

    with_session(body)


def test_a_live_session_is_recognized_and_expiry_ends_it(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        code, _ = await crud_console.create_console_login_code(session, subject=SUBJECT)
        exchanged = await crud_console.exchange_console_login_code(session, code)
        assert exchanged is not None
        token, row = exchanged

        assert await crud_console.live_console_session(session, token) is not None
        assert row.subject == SUBJECT
        assert row.session_expires_at is not None
        after = row.session_expires_at + timedelta(seconds=1)
        assert await crud_console.live_console_session(session, token, now=after) is None

    with_session(body)


def test_revocation_kills_a_live_session_without_waiting_for_expiry(
    clean_db: None,
) -> None:
    """The reason this is a table and not a signed stateless token (ADR-0083)."""

    async def body(session: AsyncSession) -> None:
        code, _ = await crud_console.create_console_login_code(session, subject=SUBJECT)
        exchanged = await crud_console.exchange_console_login_code(session, code)
        assert exchanged is not None
        token, row = exchanged
        assert await crud_console.live_console_session(session, token) is not None

        await crud_console.revoke_console_session(session, row)
        # Still well inside its expiry window, and no longer valid.
        assert await crud_console.live_console_session(session, token) is None

    with_session(body)


def test_a_revoked_row_cannot_still_have_its_code_exchanged(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        code, row = await crud_console.create_console_login_code(session, subject=SUBJECT)
        await crud_console.revoke_console_session(session, row)
        assert await crud_console.exchange_console_login_code(session, code) is None

    with_session(body)
