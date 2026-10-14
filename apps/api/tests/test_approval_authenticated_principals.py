"""Authenticated approval resolution across chat, operator, and console.

These are the red-on-revert tests for ADR-0106 and #1531.  The resolve body is
only a decision plus an optional note; every human identity and every piece of
membership evidence comes from authenticated principal material.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_api import approval_principal
from curie_api.config import get_settings
from curie_api.crud import console as crud_console
from curie_api.deps import get_approver_sets
from curie_api.main import create_app
from curie_api.models import ConsoleSession
from curie_api.routers.console import SESSION_COOKIE
from curie_api.slack_approvers import SlackApproverSetSelector
from curie_api.usergroups import UserGroupMembership
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
SUBJECT = "U0EXAMPLE1"
OTHER = "U0EXAMPLE2"
CARD_CHANNEL = "C0EXAMPLE1"
SOURCE_CHANNEL = "C0EXAMPLE2"
GROUP = "S0EXAMPLE1"


@pytest.fixture
def approvals_client(_disposable_db: Any, runs_stream: str) -> Iterator[TestClient]:
    """Build the app after the per-test runs-stream override is installed."""

    with TestClient(create_app()) as test_client:
        yield test_client


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
        "author": OTHER,
        "summary": "Confirm the requested action",
        "reply_kind": "slack",
        "reply_channel": CARD_CHANNEL,
        "reply_placeholder": "p-1",
        "dedupe_key": uuid.uuid4().hex,
    }
    payload.update(overrides)
    return payload


def _create_approval(
    client: TestClient, auth_headers: dict[str, str], **overrides: Any
) -> dict[str, Any]:
    response = client.post("/approvals", json=_payload(**overrides), headers=auth_headers)
    assert response.status_code == 201, response.text
    return response.json()


def _explicit_approval(
    client: TestClient,
    auth_headers: dict[str, str],
    *,
    users: list[str],
    author: str = OTHER,
) -> dict[str, Any]:
    route = f"operators-{uuid.uuid4().hex[:8]}"
    source_channel = f"C0SOURCE{uuid.uuid4().hex[:8].upper()}"
    agent = client.post(
        "/agents",
        json={
            "name": f"approval-principal-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": source_channel},
            "approval_routes": {
                route: {
                    "resolution": {"kind": "slack", "address": CARD_CHANNEL},
                    "approvers": {"users": users},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    return _create_approval(
        client,
        auth_headers,
        agent_id=agent.json()["id"],
        author=author,
        route=route,
        card_channel=CARD_CHANNEL,
        reply_channel=source_channel,
        gate_kind="policy",
    )


def _group_approval(
    client: TestClient, auth_headers: dict[str, str], *, author: str = OTHER
) -> dict[str, Any]:
    route = f"managers-{uuid.uuid4().hex[:8]}"
    agent = client.post(
        "/agents",
        json={
            "name": f"approval-group-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": SOURCE_CHANNEL},
            "approval_routes": {
                route: {
                    "resolution": {"kind": "slack", "address": CARD_CHANNEL},
                    "approvers": {"group": GROUP},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    return _create_approval(
        client,
        auth_headers,
        agent_id=agent.json()["id"],
        author=author,
        route=route,
        card_channel=CARD_CHANNEL,
        reply_channel=SOURCE_CHANNEL,
        gate_kind="policy",
    )


def _chat_token(
    approval_id: str,
    *,
    subject: str = SUBJECT,
    channel: str = CARD_CHANNEL,
    signing_key: str | None = None,
    scope: str = approval_principal.APPROVE_SCOPE,
    exp: int | None = None,
) -> str:
    key = signing_key or get_settings().approval_chat_attester_secret
    return approval_principal.mint(
        key,
        subject=subject,
        kind="chat",
        actor_channel=channel,
        approval_id=approval_id,
        scope=scope,
        exp=exp if exp is not None else int(time.time()) + 60,
    )


def _operator_token(
    subject: str = SUBJECT,
    *,
    scope: str = approval_principal.APPROVE_SCOPE,
    exp: int | None = None,
) -> str:
    return approval_principal.mint(
        get_settings().api_key,
        subject=subject,
        kind="operator",
        scope=scope,
        exp=exp if exp is not None else int(time.time()) + 60,
    )


def _principal_headers(token: str) -> dict[str, str]:
    return {PRINCIPAL_HEADER: token}


def _mint_console_session(
    client: TestClient, auth_headers: dict[str, str], subject: str = SUBJECT
) -> str:
    minted = client.post("/console/login-codes", json={"subject": subject}, headers=auth_headers)
    assert minted.status_code == 201, minted.text
    assert minted.json()["subject"] == subject
    exchanged = client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    assert exchanged.json()["subject"] == subject
    token = client.cookies.get(SESSION_COOKIE)
    assert token
    client.cookies.clear()
    return token


def _cookie_headers(token: str, **extra: str) -> dict[str, str]:
    # TestClient's default origin is HTTP, so its cookie jar correctly refuses
    # to auto-send a Secure cookie.  Supply the captured Set-Cookie value as a
    # browser on the production HTTPS same-origin would. A matching Origin is
    # what that same-origin browser sends on an unsafe cookie write.
    headers = {
        "Cookie": f"{SESSION_COOKIE}={token}",
        "Origin": "http://testserver",
    }
    headers.update(extra)
    return headers


def _cookie_without_matching_origin(token: str, **extra: str) -> dict[str, str]:
    """Present the console cookie without the helper's matching Origin."""

    headers = {"Cookie": f"{SESSION_COOKIE}={token}"}
    headers.update(extra)
    return headers


def _revoke_console_session(token: str) -> None:
    async def revoke() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                row = await crud_console.live_console_session(session, token)
                assert row is not None
                await crud_console.revoke_console_session(session, row)
        finally:
            await engine.dispose()

    asyncio.run(revoke())


def test_resolve_rejects_every_non_principal_credential_shape(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    created = _create_approval(approvals_client, auth_headers)
    url = f"/approvals/{created['id']}/resolve"
    attempts = [
        {},
        auth_headers,
        {"X-API-Key": "not-the-platform-key"},
        _principal_headers("not-a-principal"),
        _principal_headers(_operator_token(exp=int(time.time()) - 1)),
        _principal_headers(_operator_token(scope="approval.read")),
    ]

    for headers in attempts:
        response = approvals_client.post(url, json={"decision": "approved"}, headers=headers)
        assert response.status_code == 401, (headers.keys(), response.text)

    pending = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers)
    assert pending.json()["status"] == "pending"


@pytest.mark.parametrize("retired_field", ["resolved_by", "actor_channel"])
def test_resolve_loudly_rejects_retired_identity_fields(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    retired_field: str,
) -> None:
    created = _create_approval(approvals_client, auth_headers)
    response = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", retired_field: "caller-asserted"},
        headers=_principal_headers(_chat_token(created["id"])),
    )

    assert response.status_code == 422, response.text
    detail = response.text
    assert retired_field in detail
    assert "ADR-0106" in detail
    assert "principal" in detail.lower()


def test_chat_principal_derives_actor_channel_and_audit_proof(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    created = _create_approval(approvals_client, auth_headers)
    resolved = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved", "note": "confirmed in the card"},
        headers=_principal_headers(_chat_token(created["id"])),
    )

    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == SUBJECT
    audit = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert len(audit) == 1
    assert audit[0]["actor"] == SUBJECT
    assert audit[0]["actor_channel"] == CARD_CHANNEL
    assert audit[0]["principal_kind"] == "chat"
    assert audit[0]["authenticated"] is True


def test_chat_principal_uses_only_the_attester_key_and_is_approval_bound(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    first = _create_approval(approvals_client, auth_headers)
    second = _create_approval(approvals_client, auth_headers)

    platform_signed_chat = _chat_token(first["id"], signing_key=get_settings().api_key)
    wrong_key = approvals_client.post(
        f"/approvals/{first['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(platform_signed_chat),
    )
    assert wrong_key.status_code == 401, wrong_key.text

    first_bound = _chat_token(first["id"])
    replayed = approvals_client.post(
        f"/approvals/{second['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(first_bound),
    )
    assert replayed.status_code == 401, replayed.text

    valid = approvals_client.post(
        f"/approvals/{first['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(first_bound),
    )
    assert valid.status_code == 200, valid.text
    assert (
        approvals_client.get(f"/approvals/{second['id']}", headers=auth_headers).json()["status"]
        == "pending"
    )


def test_authorized_solo_requester_can_self_confirm_but_membership_still_denies(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
) -> None:
    operator_headers = _principal_headers(_operator_token(SUBJECT))
    admitted = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT], author=SUBJECT)
    accepted = approvals_client.post(
        f"/approvals/{admitted['id']}/resolve",
        json={"decision": "approved"},
        headers=operator_headers,
    )
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["resolved_by"] == SUBJECT

    excluded = _explicit_approval(approvals_client, auth_headers, users=[OTHER], author=SUBJECT)
    denied = approvals_client.post(
        f"/approvals/{excluded['id']}/resolve",
        json={"decision": "approved"},
        headers=operator_headers,
    )
    assert denied.status_code == 403, denied.text
    assert (
        approvals_client.get(f"/approvals/{excluded['id']}", headers=auth_headers).json()["status"]
        == "pending"
    )
    # Only the admitted approval woke its suspended turn.
    assert len(valkey.xrange(runs_stream)) == 1

    audit = approvals_client.get(f"/approvals/{admitted['id']}/audit", headers=auth_headers).json()
    assert audit[0]["actor"] == SUBJECT
    assert audit[0]["actor_channel"] is None
    assert audit[0]["principal_kind"] == "operator"
    assert audit[0]["authenticated"] is True


class _AlwaysMemberGroup:
    def __init__(self) -> None:
        self.calls = 0

    async def members(self, group_id: str) -> UserGroupMembership:
        self.calls += 1
        return UserGroupMembership(
            group=group_id,
            users=frozenset({SUBJECT}),
            fetched_at=datetime.now(UTC),
            cache_age_s=0.0,
        )


def test_operator_principals_are_explicit_user_only_even_if_group_members(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    source = _AlwaysMemberGroup()
    approvals_client.app.dependency_overrides[get_approver_sets] = lambda: SlackApproverSetSelector(
        source
    )
    group_bound = _group_approval(approvals_client, auth_headers)

    denied = approvals_client.post(
        f"/approvals/{group_bound['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(_operator_token()),
    )
    assert denied.status_code == 403, denied.text
    assert "explicit" in denied.json()["detail"].lower()
    # Eligibility is decided from the set kind before any Slack lookup. A
    # terminal credential cannot turn itself into provider membership evidence.
    assert source.calls == 0


def _assert_still_pending(
    client: TestClient, auth_headers: dict[str, str], approval_id: str
) -> None:
    pending = client.get(f"/approvals/{approval_id}", headers=auth_headers)
    assert pending.status_code == 200, pending.text
    assert pending.json()["status"] == "pending"


def test_legacy_console_cookie_name_cannot_resolve(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _mint_console_session(approvals_client, auth_headers)
    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers={
            "Cookie": f"curie_console_session={token}",
            "Origin": "http://testserver",
        },
    )
    assert denied.status_code == 401, denied.text
    _assert_still_pending(approvals_client, auth_headers, approval["id"])


@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"Origin": "https://sibling.example"},
        {"Referer": "https://sibling.example/plant"},
        {"Origin": "null"},
    ],
)
def test_console_cookie_resolve_rejects_a_bad_origin(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
    extra: dict[str, str],
) -> None:
    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _mint_console_session(approvals_client, auth_headers)
    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_cookie_without_matching_origin(token, **extra),
    )
    assert denied.status_code == 403, denied.text
    assert denied.json()["detail"] == "console session origin rejected"
    _assert_still_pending(approvals_client, auth_headers, approval["id"])


def test_console_cookie_resolve_accepts_https_origin_behind_an_http_proxy(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The UI proxy's connection is HTTP. The browser Origin stays HTTPS."""

    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _mint_console_session(approvals_client, auth_headers)
    resolved = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_cookie_headers(token, Origin="https://testserver"),
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == SUBJECT


def test_console_cookie_resolve_keeps_a_nondefault_port(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    """The browser Origin includes :28080. The UI proxy must forward that host."""

    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _mint_console_session(approvals_client, auth_headers)
    with TestClient(approvals_client.app, base_url="http://localhost:28080") as client:
        denied = client.post(
            f"/approvals/{approval['id']}/resolve",
            json={"decision": "approved"},
            headers=_cookie_headers(token, Origin="http://localhost:9999"),
        )
        assert denied.status_code == 403, denied.text
        assert denied.json()["detail"] == "console session origin rejected"
        resolved = client.post(
            f"/approvals/{approval['id']}/resolve",
            json={"decision": "approved"},
            headers=_cookie_headers(token, Origin="http://localhost:28080"),
        )
        assert resolved.status_code == 200, resolved.text


def test_ui_proxy_forwards_the_browser_host_header() -> None:
    dockerfile = (
        Path(__file__).resolve().parents[3] / "apps" / "ui" / "Dockerfile"
    ).read_text(encoding="utf-8")
    assert "proxy_set_header Host $http_host;" in dockerfile
    assert "proxy_set_header Host $host;" not in dockerfile


def test_forwarded_host_cannot_steer_the_console_origin(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _mint_console_session(approvals_client, auth_headers)
    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_cookie_without_matching_origin(
            token,
            Origin="https://sibling.example",
            **{
                "X-Forwarded-Host": "sibling.example",
                "X-Forwarded-Proto": "https",
            },
        ),
    )
    assert denied.status_code == 403, denied.text
    assert denied.json()["detail"] == "console session origin rejected"
    _assert_still_pending(approvals_client, auth_headers, approval["id"])


def test_console_cookie_resolve_accepts_a_matching_referer_without_origin(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _mint_console_session(approvals_client, auth_headers)
    resolved = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_cookie_without_matching_origin(token, Referer="http://testserver/approvals"),
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == SUBJECT


def test_operator_resolve_ignores_a_missing_origin(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT], author=SUBJECT)
    resolved = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=_principal_headers(_operator_token(SUBJECT)),
    )
    assert resolved.status_code == 200, resolved.text
    assert "origin" not in resolved.request.headers


def test_console_principal_can_resolve_as_a_verified_group_member(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    source = _AlwaysMemberGroup()
    approvals_client.app.dependency_overrides[get_approver_sets] = lambda: SlackApproverSetSelector(
        source
    )
    group_bound = _group_approval(approvals_client, auth_headers)
    cookie = _cookie_headers(_mint_console_session(approvals_client, auth_headers))

    resolved = approvals_client.post(
        f"/approvals/{group_bound['id']}/resolve",
        json={"decision": "approved"},
        headers=cookie,
    )
    assert resolved.status_code == 200, resolved.text
    assert source.calls == 1
    audit = approvals_client.get(
        f"/approvals/{group_bound['id']}/audit", headers=auth_headers
    ).json()
    assert audit[0]["principal_kind"] == "console"
    assert audit[0]["evidence"]["kind"] == "user_group"


def test_console_session_subject_is_immutable_reusable_and_revocable(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    token = _mint_console_session(approvals_client, auth_headers)
    cookie = _cookie_headers(token)
    current = approvals_client.get("/console/session", headers=cookie)
    assert current.status_code == 200, current.text
    assert current.json()["subject"] == SUBJECT

    first = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    second = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    resolved = approvals_client.post(
        f"/approvals/{first['id']}/resolve",
        json={"decision": "approved"},
        headers=cookie,
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["resolved_by"] == SUBJECT
    audit = approvals_client.get(f"/approvals/{first['id']}/audit", headers=auth_headers).json()
    assert audit[0]["actor"] == SUBJECT
    assert audit[0]["actor_channel"] is None
    assert audit[0]["principal_kind"] == "console"
    assert audit[0]["authenticated"] is True

    _revoke_console_session(token)
    assert approvals_client.get("/console/session", headers=cookie).status_code == 401
    revoked = approvals_client.post(
        f"/approvals/{second['id']}/resolve",
        json={"decision": "approved"},
        headers=cookie,
    )
    assert revoked.status_code == 401, revoked.text
    assert (
        approvals_client.get(f"/approvals/{second['id']}", headers=auth_headers).json()["status"]
        == "pending"
    )


def test_null_subject_console_session_cannot_be_an_approval_principal(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    async def make_legacy_session() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                code = crud_console.new_login_code()
                await session.execute(
                    text(
                        "INSERT INTO curie.console_sessions "
                        "(id, subject, login_code_hash, login_code_expires_at) "
                        "VALUES (:id, NULL, :code_hash, :expires_at)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "code_hash": crud_console.hash_console_credential(code),
                        "expires_at": (
                            datetime.now(UTC).replace(tzinfo=None) + crud_console.LOGIN_CODE_TTL
                        ),
                    },
                )
                await session.commit()
                return code
        finally:
            await engine.dispose()

    code = asyncio.run(make_legacy_session())
    exchanged = approvals_client.post("/console/session", json={"code": code})
    assert exchanged.status_code == 200, exchanged.text
    token = approvals_client.cookies.get(SESSION_COOKIE)
    assert token
    cookie = _cookie_headers(token)
    assert approvals_client.get("/console/session", headers=cookie).status_code == 401

    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=cookie,
    )
    assert denied.status_code == 401, denied.text


def test_two_principal_credentials_are_ambiguous_and_fail_closed(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    approval = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    cookie_token = _mint_console_session(approvals_client, auth_headers)
    headers = {
        **_principal_headers(_operator_token()),
        **_cookie_headers(cookie_token),
    }
    denied = approvals_client.post(
        f"/approvals/{approval['id']}/resolve",
        json={"decision": "approved"},
        headers=headers,
    )
    assert denied.status_code == 401, denied.text
    assert "ambiguous" in denied.json()["detail"].lower()


def test_administrative_mints_never_accept_a_cookie_or_principal_token(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    cookie = _cookie_headers(_mint_console_session(approvals_client, auth_headers))
    principal = _principal_headers(_operator_token())

    for endpoint in ("/console/login-codes", "/approvals/principals/operator"):
        for headers in (cookie, principal):
            denied = approvals_client.post(endpoint, json={"subject": OTHER}, headers=headers)
            assert denied.status_code == 401, (endpoint, headers.keys(), denied.text)

        minted = approvals_client.post(endpoint, json={"subject": OTHER}, headers=auth_headers)
        assert minted.status_code == 201, (endpoint, minted.text)


def test_console_session_rows_store_the_bound_subject(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    clean_db: None,
) -> None:
    _mint_console_session(approvals_client, auth_headers)

    async def read_subject() -> str | None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                row = (await session.execute(select(ConsoleSession))).scalar_one()
                return row.subject
        finally:
            await engine.dispose()

    assert asyncio.run(read_subject()) == SUBJECT


@pytest.fixture
def driver_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    from curie_internal.driver_declaration import DeclaredDriver

    settings = get_settings()
    monkeypatch.setattr(settings, "test_installation_enabled", True)
    monkeypatch.setattr(
        settings,
        "test_installation_drivers",
        (DeclaredDriver(CARD_CHANNEL, "B0EXAMPLE1", SUBJECT),),
    )


def _driver_token(
    approval_id: str,
    *,
    subject: str = SUBJECT,
    channel: str = CARD_CHANNEL,
    signing_key: str | None = None,
    exp: int | None = None,
) -> str:
    return approval_principal.mint(
        signing_key or get_settings().approval_chat_attester_secret,
        subject=subject,
        kind="test_driver",
        actor_channel=channel,
        approval_id=approval_id,
        scope=approval_principal.APPROVE_SCOPE,
        exp=exp if exp is not None else int(time.time()) + 60,
    )


@pytest.mark.parametrize("decision", ["approved", "rejected"])
def test_declared_driver_resolves_explicit_route_and_records_kind(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    driver_settings: None,
    decision: str,
    valkey: redis.Redis,
) -> None:
    created = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    response = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": decision},
        headers={PRINCIPAL_HEADER: _driver_token(created["id"])},
    )
    assert response.status_code == 200, response.text
    assert (response.json()["status"], response.json()["resolved_by"]) == (decision, SUBJECT)
    audit = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert [(row["principal_kind"], row["authenticated"], row["authorized"]) for row in audit] == [
        ("test_driver", True, True)
    ]
    loser = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": decision},
        headers={PRINCIPAL_HEADER: _driver_token(created["id"])},
    )
    assert loser.status_code == 409


@pytest.mark.parametrize(
    "violation",
    [
        "off",
        "subject",
        "channel",
        "card_channel",
        "unlisted",
        "membership",
        "group",
        "missing_route",
    ],
)
def test_api_independently_refuses_driver_and_audits(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    driver_settings: None,
    monkeypatch: pytest.MonkeyPatch,
    violation: str,
) -> None:
    if violation == "membership":
        created = _create_approval(approvals_client, auth_headers)
    elif violation == "group":
        created = _group_approval(approvals_client, auth_headers)
    else:
        created = _explicit_approval(
            approvals_client, auth_headers, users=[OTHER] if violation == "unlisted" else [SUBJECT]
        )
    if violation == "off":
        monkeypatch.setattr(get_settings(), "test_installation_enabled", False)
    if violation == "missing_route":
        # Delete the real binding, preserving the pending approval's named route.
        patched = approvals_client.patch(
            f"/agents/{created['agent_id']}", json={"approval_routes": {}}, headers=auth_headers
        )
        assert patched.status_code == 200, patched.text
    channel = SOURCE_CHANNEL if violation in ("channel", "card_channel") else CARD_CHANNEL
    if violation == "card_channel":
        from curie_internal.driver_declaration import DeclaredDriver

        monkeypatch.setattr(
            get_settings(),
            "test_installation_drivers",
            (DeclaredDriver(SOURCE_CHANNEL, "B0EXAMPLE1", SUBJECT),),
        )
    response = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers={
            PRINCIPAL_HEADER: _driver_token(
                created["id"], subject=OTHER if violation == "subject" else SUBJECT, channel=channel
            )
        },
    )
    assert response.status_code == 403, response.text
    current = approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers).json()
    assert current["status"] == "pending"
    audit = approvals_client.get(f"/approvals/{created['id']}/audit", headers=auth_headers).json()
    assert len(audit) == 1
    assert (audit[0]["action"], audit[0]["principal_kind"], audit[0]["authorized"]) == (
        "denied",
        "test_driver",
        False,
    )


@pytest.mark.parametrize("violation", ["forged", "platform_key", "wrong_approval", "expired"])
def test_driver_credential_cannot_cross_authentication_boundary(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    driver_settings: None,
    violation: str,
) -> None:
    created = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    token = _driver_token(
        str(uuid.uuid4()) if violation == "wrong_approval" else created["id"],
        signing_key=(
            get_settings().api_key
            if violation == "platform_key"
            else "forged-key"
            if violation == "forged"
            else None
        ),
        exp=1 if violation == "expired" else None,
    )
    response = approvals_client.post(
        f"/approvals/{created['id']}/resolve",
        json={"decision": "approved"},
        headers={PRINCIPAL_HEADER: token},
    )
    assert response.status_code == 401
    assert (
        approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers).json()["status"]
        == "pending"
    )


def test_driver_credential_is_not_a_recovery_principal(
    approvals_client: TestClient,
    auth_headers: dict[str, str],
    driver_settings: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "approval_recovery_enabled", True)
    created = _explicit_approval(approvals_client, auth_headers, users=[SUBJECT])
    response = approvals_client.post(
        f"/approvals/{created['id']}/recover",
        json={
            "disposition": "rejected",
            "reason": "Example recovery",
            "recovery_key": f"example-{uuid.uuid4().hex}",
        },
        headers={**auth_headers, PRINCIPAL_HEADER: _driver_token(created["id"])},
    )
    assert response.status_code == 401, response.text
    assert (
        approvals_client.get(f"/approvals/{created['id']}", headers=auth_headers).json()["status"]
        == "pending"
    )
