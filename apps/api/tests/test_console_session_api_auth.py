"""Console session acceptance on ordinary API routes (#1045, ADR-0083).

A live host-only console session authorizes the same routes as the platform
key. The platform key still wins without a session lookup. State access and
platform administration stay on their own credentials.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_api import crud
from curie_api.auth import require_api_key, verify_platform_key
from curie_api.config import get_settings
from curie_api.models import ConsoleSession
from curie_api.routers.console import SESSION_COOKIE
from curie_api.sandbox_token import mint
from fastapi import FastAPI, HTTPException
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from starlette.requests import Request

SUBJECT = "U0EXAMPLE1"
API_KEY_DETAIL = "missing or invalid API key"
PLATFORM_KEY_DETAIL = "missing or invalid platform API key"


def _with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _exchange(client: Any, auth_headers: dict[str, str]) -> str:
    minted = client.post("/console/login-codes", json={"subject": SUBJECT}, headers=auth_headers)
    assert minted.status_code == 201, minted.text
    exchanged = client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    token = client.cookies.get(SESSION_COOKIE)
    assert isinstance(token, str) and token
    client.cookies.clear()
    return token


def _cookie(token: str, *, name: str = SESSION_COOKIE) -> dict[str, str]:
    return {"Cookie": f"{name}={token}"}


def _request(cookie: str | None) -> tuple[Request, list[str]]:
    app = FastAPI()
    opened: list[str] = []

    def sessionmaker() -> None:
        opened.append("opened")
        raise AssertionError("sessionmaker opened")

    app.state.sessionmaker = sessionmaker
    headers: list[tuple[bytes, bytes]] = []
    if cookie is not None:
        headers.append((b"cookie", f"{SESSION_COOKIE}={cookie}".encode()))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/agents",
        "raw_path": b"/agents",
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 5000),
        "server": ("testserver", 443),
        "root_path": "",
        "app": app,
    }
    return Request(scope), opened


@contextmanager
def _sql(client: Any) -> Iterator[list[str]]:
    statements: list[str] = []

    def observed(
        _connection: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    engine = client.app.state.engine.sync_engine
    event.listen(engine, "before_cursor_execute", observed)
    try:
        yield statements
    finally:
        event.remove(engine, "before_cursor_execute", observed)


def _assert_api_key_refusal(response: Any) -> None:
    assert response.status_code == 401, response.text
    assert response.json()["detail"] == API_KEY_DETAIL


def _expire_sessions() -> None:
    past = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=5)

    async def body(session: AsyncSession) -> None:
        await session.execute(
            text("UPDATE curie.console_sessions SET session_expires_at = :past"),
            {"past": past},
        )
        await session.commit()

    _with_session(body)


def test_platform_key_returns_before_opening_the_session_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[str | None] = []

    def spy(header: str | None) -> bool:
        recorded.append(header)
        return verify_platform_key(header)

    monkeypatch.setattr("curie_api.auth.verify_platform_key", spy)
    request, opened = _request("not-a-session")
    assert asyncio.run(require_api_key(request, get_settings().api_key)) is None
    assert opened == []
    assert recorded == [get_settings().api_key]

    bare, still_closed = _request(None)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(require_api_key(bare, "wrong-key"))
    assert caught.value.status_code == 401
    assert caught.value.detail == API_KEY_DETAIL
    assert still_closed == []
    assert recorded == [get_settings().api_key, "wrong-key"]


def test_agents_accepts_the_platform_key_with_a_garbage_session_cookie(
    client: Any, auth_headers: dict[str, str]
) -> None:
    with _sql(client) as statements:
        response = client.get(
            "/agents",
            headers={**auth_headers, **_cookie("not-a-session")},
        )
    assert response.status_code == 200, response.text
    assert "console_sessions" not in "\n".join(statements)


def test_agents_accepts_the_platform_key_without_a_cookie(
    client: Any, auth_headers: dict[str, str]
) -> None:
    response = client.get("/agents", headers=auth_headers)
    assert response.status_code == 200, response.text


def test_a_live_session_authorizes_agents_and_approvals(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    first = client.get("/agents", headers=headers)
    assert first.status_code == 200, first.text
    second = client.get("/agents", headers=headers)
    assert second.status_code == 200, second.text
    approvals = client.get("/approvals", headers=headers)
    assert approvals.status_code == 200, approvals.text


def test_an_expired_session_is_refused_on_agents(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    _expire_sessions()
    _assert_api_key_refusal(client.get("/agents", headers=headers))


def test_a_consumed_session_that_later_expires_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))

    async def consumed(session: AsyncSession) -> datetime | None:
        row = (await session.execute(select(ConsoleSession))).scalars().one()
        return row.consumed_at

    assert _with_session(consumed) is not None
    _expire_sessions()
    _assert_api_key_refusal(client.get("/agents", headers=headers))


def test_a_revoked_session_is_refused_while_unexpired(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))

    async def revoke(session: AsyncSession) -> None:
        row = (await session.execute(select(ConsoleSession))).scalars().one()
        assert row.session_expires_at is not None
        assert row.session_expires_at > datetime.now(UTC).replace(tzinfo=None)
        await crud.revoke_console_session(session, row)

    _with_session(revoke)
    _assert_api_key_refusal(client.get("/agents", headers=headers))


def test_agents_without_credentials_does_not_query_sessions(client: Any) -> None:
    with _sql(client) as statements:
        response = client.get("/agents")
    _assert_api_key_refusal(response)
    assert "console_sessions" not in "\n".join(statements)


def test_a_wrong_api_key_does_not_query_sessions(client: Any) -> None:
    with _sql(client) as statements:
        response = client.get("/agents", headers={"X-API-Key": "wrong-key"})
    _assert_api_key_refusal(response)
    assert "console_sessions" not in "\n".join(statements)


def test_a_state_token_is_rejected_on_agents(client: Any) -> None:
    token = mint(
        get_settings().api_key,
        agent=str(uuid.uuid4()),
        scope="state",
        exp=4102444800,
    )
    _assert_api_key_refusal(client.get("/agents", headers={"X-API-Key": token}))


def test_a_console_session_is_rejected_on_state(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    response = client.get(f"/agents/{uuid.uuid4()}/state", headers=headers)
    assert response.status_code == 401, response.text
    assert response.json()["detail"] == "missing or invalid credential"


def test_a_console_session_cannot_cross_the_platform_boundary(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    minted = client.post("/console/login-codes", json={"subject": SUBJECT}, headers=headers)
    assert minted.status_code == 401, minted.text
    assert minted.json()["detail"] == PLATFORM_KEY_DETAIL
    operator = client.post(
        "/approvals/principals/operator",
        json={"subject": "operator-example"},
        headers=headers,
    )
    assert operator.status_code == 401, operator.text
    assert operator.json()["detail"] == PLATFORM_KEY_DETAIL


def test_the_legacy_cookie_name_does_not_authorize_agents(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    token = _exchange(client, auth_headers)
    response = client.get("/agents", headers=_cookie(token, name="curie_console_session"))
    _assert_api_key_refusal(response)


def test_an_adapter_header_plus_a_session_cookie_is_ambiguous(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    headers["X-Curie-Adapter-Principal"] = "not-a-token"
    response = client.get("/approvals", headers=headers)
    assert response.status_code == 401, response.text
    assert response.json()["detail"] == "ambiguous credentials"


def test_platform_key_wins_over_an_expired_session_without_a_lookup(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    token = _exchange(client, auth_headers)
    _expire_sessions()
    with _sql(client) as statements:
        response = client.get("/agents", headers={**auth_headers, **_cookie(token)})
    assert response.status_code == 200, response.text
    assert "console_sessions" not in "\n".join(statements)


def test_a_foreign_origin_rejects_a_console_session_mutation(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    headers["Origin"] = "https://evil.example"
    with _sql(client) as statements:
        response = client.post(f"/agents/{uuid.uuid4()}/kill", headers=headers)
    assert response.status_code == 403, response.text
    assert response.json()["detail"] == "console session origin rejected"
    assert "console_sessions" not in "\n".join(statements)


def test_a_matching_origin_lets_a_console_session_reach_kill(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    headers["Origin"] = "https://testserver"
    response = client.post(f"/agents/{uuid.uuid4()}/kill", headers=headers)
    assert response.status_code not in (401, 403), response.text
    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "agent not found"


def test_a_safe_method_skips_the_console_session_origin_check(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    headers = _cookie(_exchange(client, auth_headers))
    headers["Origin"] = "https://evil.example"
    response = client.get("/agents", headers=headers)
    assert response.status_code == 200, response.text


def test_the_platform_key_skips_the_console_session_origin_check(
    client: Any, auth_headers: dict[str, str]
) -> None:
    response = client.post(
        f"/agents/{uuid.uuid4()}/kill",
        headers={**auth_headers, "Origin": "https://evil.example"},
    )
    assert response.status_code == 404, response.text
    assert response.json()["detail"] == "agent not found"


def test_an_empty_session_cookie_plus_an_adapter_header_is_ambiguous(client: Any) -> None:
    response = client.get(
        "/approvals",
        headers={
            "X-Curie-Adapter-Principal": "not-a-token",
            **_cookie(""),
        },
    )
    assert response.status_code == 401, response.text
    assert response.json()["detail"] == "ambiguous credentials"


def test_an_empty_session_cookie_is_not_a_session(client: Any) -> None:
    with _sql(client) as statements:
        response = client.get("/agents", headers=_cookie(""))
    _assert_api_key_refusal(response)
    assert "console_sessions" not in "\n".join(statements)
