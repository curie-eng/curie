"""GitHub account proof through HTTP and the real identity stores (#5719)."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import personal_enrollment_support as support
import personal_execution_support as pe
import pytest
from _migration_support import sql_dicts, sql_rows
from curie_api import identity_fence
from curie_api.config import get_settings
from curie_api.factory_slack import resolve_github_requester
from curie_api.main import create_app
from curie_api.models import Agent
from curie_api.routers.identity_me import FLOW_COOKIE
from curie_telemetry.redact import RedactingLogFilter
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

CLIENT_ID = "Iv1.example"
CLIENT_SECRET = "EXAMPLE-github-client-secret-sentinel"
USER_TOKEN = "ghu_EXAMPLEuserTokenSentinel0123456789"
START = "/identity/me/github-link"
CALLBACK = START + "/callback"


class GitHub:
    # Provider shapes and authorization:
    # https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app
    # https://docs.github.com/en/rest/users/users#get-the-authenticated-user
    # https://docs.github.com/en/rest/apps/oauth-applications#delete-an-app-token
    def __init__(self) -> None:
        self.code = "good"
        self.account: Any = {"id": 42, "login": "acme-developer", "email": "ignored@example.com"}
        self.exchanges = 0
        self.reads = 0
        self.revocations = 0
        self.hold: str | None = None
        self.entered = threading.Event()
        self.release = threading.Event()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def answer(self, status: int, body: Any) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body).encode())

            def body(self) -> bytes:
                return self.rfile.read(int(self.headers.get("Content-Length", "0")))

            def released(self, phase: str) -> bool:
                if owner.hold != phase:
                    return True
                owner.entered.set()
                if owner.release.wait(timeout=10):
                    return True
                self.answer(503, {"error": "fixture_deadline"})
                return False

            def do_POST(self) -> None:
                assert self.path == "/login/oauth/access_token"
                form = parse_qs(self.body().decode())
                assert form["client_id"] == [CLIENT_ID]
                assert form["client_secret"] == [CLIENT_SECRET]
                assert form["redirect_uri"] == ["https://testserver/api" + CALLBACK]
                owner.code = form["code"][0]
                owner.exchanges += 1
                if not self.released("exchange"):
                    return
                if owner.code == "exchange-error":
                    self.answer(
                        200, {"error": "bad_verification_code", "error_description": CLIENT_SECRET}
                    )
                else:
                    self.answer(
                        200, {"access_token": USER_TOKEN, "token_type": "bearer", "scope": ""}
                    )

            def do_GET(self) -> None:
                assert self.path == "/api/v3/user"
                assert self.headers["Authorization"] == f"Bearer {USER_TOKEN}"
                owner.reads += 1
                if not self.released("user"):
                    return
                self.answer(403 if owner.code == "user-error" else 200, owner.account)

            def do_DELETE(self) -> None:
                assert self.path == f"/api/v3/applications/{CLIENT_ID}/token"
                expected = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
                assert self.headers["Authorization"] == "Basic " + expected
                assert json.loads(self.body()) == {"access_token": USER_TOKEN}
                owner.revocations += 1
                if not self.released("revoke"):
                    return
                self.answer(503 if owner.code == "revoke-error" else 204, {})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)


@pytest.fixture
def github_link_api(clean_db: None, tmp_path: Path) -> Iterator[tuple[TestClient, GitHub]]:
    github = GitHub()
    github.thread.start()
    key = tmp_path / "app-key"
    key.write_text("EXAMPLE-unused-private-key")
    document = {
        "code_hosts": [
            {
                "name": "github",
                "kind": "github",
                "host": urlsplit(github.base).netloc,
                "api_url": github.base + "/api/v3",
                "credential": {"type": "github_app", "app_id": "42", "private_key_file": str(key)},
            }
        ]
    }
    environment = {
        **support.app_env(namespace=None),
        "CURIE_OIDC_ISSUER": "https://login.example.com",
        "CURIE_OIDC_AUDIENCE": "console-example",
        "CURIE_OIDC_JWKS_URL": "https://login.example.com/jwks",
        "CURIE_OIDC_CLIENT_SECRET": "EXAMPLE-oidc-secret",
        "CURIE_OIDC_REDIRECT_URI": "https://testserver/api/console/oidc/callback",
        "FACTORY_MODEL_AUTH_MODE": "personal",
        "FACTORY_GITHUB_APP_CLIENT_ID": CLIENT_ID,
        "FACTORY_GITHUB_APP_CLIENT_SECRET": CLIENT_SECRET,
        "CURIE_FORGES": json.dumps(document),
        "GITHUB_API_URL": github.base + "/api/v3",
    }
    try:
        with (
            support.environment(environment),
            TestClient(create_app(), base_url=support.ORIGIN) as api,
        ):
            yield api, github
    finally:
        github.server.shutdown()
        github.thread.join(timeout=5)
        github.server.server_close()


@pytest.fixture
def github_link_owners(github_link_api: tuple[TestClient, GitHub]) -> Iterator[list[support.Owner]]:
    tenant = support.seed_tenant()
    owners = [support.seed_owner(tenant, issuer=get_settings().oidc_issuer) for _ in range(2)]
    yield owners
    sql_rows(
        "DELETE FROM curie.console_sessions WHERE principal_id IN "
        "(SELECT id FROM curie.principals WHERE tenant_id = :tenant)",
        {"tenant": tenant},
    )
    for table in (
        "github_link_states",
        "identity_audit_events",
        "identity_links",
        "identity_namespaces",
        "principals",
    ):
        sql_rows(f"DELETE FROM curie.{table} WHERE tenant_id = :tenant", {"tenant": tenant})
    sql_rows("DELETE FROM curie.tenants WHERE id = :id", {"id": tenant})


def start(api: TestClient, owner: support.Owner) -> str:
    response = api.post(START, headers=owner.headers())
    assert response.status_code == 200, response.text
    url = response.json()["authorize_url"]
    query = parse_qs(urlsplit(url).query)
    assert query["client_id"] == [CLIENT_ID]
    assert query["redirect_uri"] == ["https://testserver/api" + CALLBACK]
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "SameSite=lax" in response.headers["set-cookie"]
    assert "Secure" in response.headers["set-cookie"]
    assert "Max-Age=600" in response.headers["set-cookie"]
    assert response.cookies[FLOW_COOKIE] == owner.session
    state = query["state"][0]
    [stored] = sql_dicts(
        "SELECT * FROM curie.github_link_states WHERE principal_id = :id "
        "ORDER BY expires_at DESC LIMIT 1",
        {"id": owner.principal_id},
    )
    assert stored["state_hash"] == hashlib.sha256(state.encode()).hexdigest()
    assert state not in str(stored)
    return state


def callback(
    api: TestClient, state: str, owner: support.Owner | None, *, code: str = "good"
) -> Any:
    api.cookies.clear()
    return api.get(
        CALLBACK,
        params={"state": state, "code": code},
        headers=owner.headers(origin=None) if owner else {},
        follow_redirects=False,
    )


def links(api: TestClient, owner: support.Owner) -> dict[str, Any]:
    response = api.get("/identity/me/links", headers=owner.headers(origin=None))
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.parametrize("known_state", [True, False])
def test_github_link_unauthenticated_callback_does_not_wait_for_identity_writer(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    known_state: bool,
) -> None:
    api, github = github_link_api
    owner = github_link_owners[0]
    state = start(api, owner) if known_state else "unknown-state"
    portal = api.portal
    assert portal is not None
    client = api.app.state.valkey
    before = b"stable:github-link-before"
    portal.call(client.set, identity_fence.FENCE_KEY, before)
    connection = portal.call(api.app.state.engine.connect)
    try:
        portal.call(connection.execute, text("SELECT pg_advisory_lock(2250220)"))
        portal.call(connection.commit)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(callback, api, state, None)
            try:
                response = pending.result(timeout=3)
            except TimeoutError:
                response = None
            finally:
                portal.call(connection.execute, text("SELECT pg_advisory_unlock(2250220)"))
                portal.call(connection.commit)
                pending.result(timeout=5)
        assert response is not None, "unauthenticated callback waited for the identity writer"
        assert response.status_code == 303
        assert response.headers["location"].endswith("github=failed")
        assert portal.call(client.get, identity_fence.FENCE_KEY) == before
        assert github.exchanges == 0
        if known_state:
            [stored] = sql_dicts(
                "SELECT consumed_at FROM curie.github_link_states WHERE state_hash = :hash",
                {"hash": hashlib.sha256(state.encode()).hexdigest()},
            )
            assert stored["consumed_at"] is not None
    finally:
        portal.call(connection.close)


@pytest.mark.parametrize("phase", ["exchange", "user", "revoke"])
def test_github_link_provider_calls_leave_identity_admission_unfenced(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    phase: str,
) -> None:
    api, github = github_link_api
    owner = github_link_owners[0]
    state = start(api, owner)
    portal = api.portal
    assert portal is not None
    client = api.app.state.valkey
    before = portal.call(client.get, identity_fence.FENCE_KEY)
    assert before.startswith(b"stable:")
    github.hold = phase
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(callback, api, state, owner)
        try:
            assert github.entered.wait(timeout=5), "the GitHub request did not reach the stand-in"
            assert portal.call(client.get, identity_fence.FENCE_KEY) == before
            assert sql_rows("SELECT pg_try_advisory_lock(2250220)")[0][0] is True
        finally:
            github.release.set()
            response = pending.result(timeout=5)
    assert response.headers["location"].endswith("github=linked")
    after = portal.call(client.get, identity_fence.FENCE_KEY)
    assert after.startswith(b"stable:") and after != before
    assert len(links(api, owner)["links"]) == 1


def test_github_link_creates_once_revokes_and_keeps_secrets_out(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    caplog: pytest.LogCaptureFixture,
) -> None:
    api, github = github_link_api
    owner = github_link_owners[0]
    state = start(api, owner)
    response = callback(api, state, owner)
    assert response.status_code == 303
    assert response.headers["location"] == "/settings/claude-plan?github=linked"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert (github.exchanges, github.reads, github.revocations) == (1, 1, 1)
    [link] = sql_dicts(
        "SELECT * FROM curie.identity_links WHERE principal_id = :id", {"id": owner.principal_id}
    )
    assert link["verification_source"] == "provider_event_verified"
    assert link["provider_native_id"] == "42"
    assert link["created_by_principal_id"] == owner.principal_id
    assert link["display_label"] == "acme-developer"
    audits = sql_dicts(
        "SELECT * FROM curie.identity_audit_events WHERE principal_id = :id",
        {"id": owner.principal_id},
    )
    assert [a["event_type"] for a in audits] == ["identity.code_host_link_created"]
    own = links(api, owner)
    assert own["github_link_available"] is False
    assert own["links"][0]["display_label"] == "acme-developer"
    assert callback(api, state, owner).headers["location"].endswith("github=failed")
    assert github.exchanges == 1
    stored = sql_dicts(
        "SELECT * FROM curie.github_link_states WHERE principal_id = :id",
        {"id": owner.principal_id},
    )
    text = (
        response.text
        + str(dict(response.headers))
        + json.dumps(own)
        + caplog.text
        + str(link)
        + str(audits)
        + str(stored)
    )
    assert USER_TOKEN not in text and CLIENT_SECRET not in text


def test_github_link_start_sweeps_consumed_and_expired_state(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
) -> None:
    """#5829: every start sweeps dead one-use state in its own transaction.

    A consumed row and an expired row are dead whichever principal owns them,
    while another principal's live unconsumed row survives the sweep. The
    callback drops its own row once its consume succeeded.
    """
    api, _github = github_link_api
    owner, other = github_link_owners
    surviving = start(api, other)
    consumed = hashlib.sha256(b"consumed-example-state").hexdigest()
    expired = hashlib.sha256(b"expired-example-state").hexdigest()
    sql_rows(
        "INSERT INTO curie.github_link_states "
        "(state_hash, tenant_id, principal_id, expires_at, consumed_at) "
        "VALUES (:hash, :tenant, :principal, now() + interval '10 minutes', now())",
        {"hash": consumed, "tenant": owner.tenant_id, "principal": owner.principal_id},
    )
    sql_rows(
        "INSERT INTO curie.github_link_states "
        "(state_hash, tenant_id, principal_id, expires_at, consumed_at) "
        "VALUES (:hash, :tenant, :principal, now() - interval '1 minute', NULL)",
        {"hash": expired, "tenant": owner.tenant_id, "principal": owner.principal_id},
    )
    state = start(api, owner)
    remaining = {
        row["state_hash"] for row in sql_dicts("SELECT state_hash FROM curie.github_link_states")
    }
    assert consumed not in remaining
    assert expired not in remaining
    assert hashlib.sha256(surviving.encode()).hexdigest() in remaining
    assert hashlib.sha256(state.encode()).hexdigest() in remaining
    assert callback(api, state, owner).headers["location"].endswith("github=linked")
    assert (
        sql_dicts(
            "SELECT * FROM curie.github_link_states WHERE state_hash = :hash",
            {"hash": hashlib.sha256(state.encode()).hexdigest()},
        )
        == []
    )


@pytest.mark.parametrize(
    "failure",
    [
        "other-principal",
        "missing-session",
        "expired",
        "foreign",
        "exchange-error",
        "user-error",
        "revoke-error",
    ],
)
def test_github_link_refused_callback_consumes_and_creates_nothing(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    failure: str,
) -> None:
    api, github = github_link_api
    owner, other = github_link_owners
    state = start(api, owner)
    if failure == "expired":
        sql_rows(
            "UPDATE curie.github_link_states SET expires_at = now() - interval '1 minute' "
            "WHERE principal_id = :id",
            {"id": owner.principal_id},
        )
    actor = (
        other if failure == "other-principal" else None if failure == "missing-session" else owner
    )
    used = "foreign-state" if failure == "foreign" else state
    response = callback(api, used, actor, code=failure)
    assert response.status_code == 303
    assert response.headers["location"].endswith("github=failed")
    assert USER_TOKEN not in response.text and CLIENT_SECRET not in response.text
    assert links(api, owner)["links"] == []
    assert links(api, other)["links"] == []
    if failure != "foreign":
        assert callback(api, state, owner).headers["location"].endswith("github=failed")
    if failure in {"user-error", "revoke-error"}:
        assert github.revocations == 1
    if failure == "foreign":
        assert callback(api, state, owner).headers["location"].endswith("github=linked")


def test_github_link_taken_account_and_own_unlink(
    github_link_api: tuple[TestClient, GitHub], github_link_owners: list[support.Owner]
) -> None:
    api, github = github_link_api
    owner, other = github_link_owners
    assert callback(api, start(api, owner), owner).headers["location"].endswith("github=linked")
    assert callback(api, start(api, other), other).headers["location"].endswith("github=taken")
    assert github.revocations == 2
    link = links(api, owner)["links"][0]
    assert links(api, other)["links"] == []
    target = "/identity/me/links/" + link["id"]
    assert api.delete(target, headers=other.headers()).status_code == 404
    assert (
        api.delete(target, headers=owner.headers(origin="https://foreign.example.com")).status_code
        == 403
    )
    assert links(api, owner)["links"] == [link]
    assert api.delete(target, headers=owner.headers()).status_code == 200
    assert links(api, owner) == {"links": [], "github_link_available": True}
    assert callback(api, start(api, other), other).headers["location"].endswith("github=linked")
    audits = sql_dicts(
        "SELECT * FROM curie.identity_audit_events WHERE principal_id = :id "
        "AND event_type = 'identity.link_revoked'",
        {"id": owner.principal_id},
    )
    assert len(audits) == 1


def test_github_link_start_refuses_noncanonical_session_cookie(
    github_link_api: tuple[TestClient, GitHub], github_link_owners: list[support.Owner]
) -> None:
    api, _ = github_link_api
    owner = github_link_owners[0]
    token = "EXAMPLE-noncanonical-session!"
    sql_rows(
        "UPDATE curie.console_sessions SET session_token_hash = :hash WHERE principal_id = :id",
        {"hash": hashlib.sha256(token.encode()).hexdigest(), "id": owner.principal_id},
    )
    malformed = support.Owner(owner.tenant_id, owner.principal_id, token)
    assert links(api, malformed)["links"] == []
    response = api.post(START, headers=malformed.headers())
    assert response.status_code == 401
    assert "set-cookie" not in response.headers
    assert (
        sql_dicts(
            "SELECT * FROM curie.github_link_states WHERE principal_id = :id",
            {"id": owner.principal_id},
        )
        == []
    )


@pytest.mark.parametrize("revoked", [False, True])
def test_github_link_cross_site_return_uses_same_live_session(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    revoked: bool,
) -> None:
    api, github = github_link_api
    owner = github_link_owners[0]
    state = start(api, owner)
    if revoked:
        sql_rows(
            "UPDATE curie.console_sessions SET revoked_at = now() WHERE principal_id = :id",
            {"id": owner.principal_id},
        )
    api.cookies.clear()
    response = api.get(
        CALLBACK,
        params={"state": state, "code": "good"},
        headers={"Cookie": f"{FLOW_COOKIE}={owner.session}"},
        follow_redirects=False,
    )
    assert response.headers["location"].endswith("github=failed" if revoked else "github=linked")
    assert "Max-Age=0" in response.headers["set-cookie"]
    assert github.exchanges == (0 if revoked else 1)


def test_github_link_unlink_stops_the_production_label_owner_resolver(
    github_link_api: tuple[TestClient, GitHub], github_link_owners: list[support.Owner]
) -> None:
    api, github = github_link_api
    owner = github_link_owners[0]

    async def seed(session: AsyncSession) -> Any:
        agent = Agent(name="acme-github-link", tenant_id=owner.tenant_id)
        session.add(agent)
        await session.commit()
        return agent.id

    agent_id = pe.with_session(seed)

    async def resolve(session: AsyncSession) -> Any:
        return await resolve_github_requester(
            session, agent_id=agent_id, tracker_host=urlsplit(github.base).netloc, account_id=42
        )

    try:
        assert pe.with_session(resolve) is None
        assert callback(api, start(api, owner), owner).headers["location"].endswith("github=linked")
        assert pe.with_session(resolve) == owner.principal_id
        [link] = links(api, owner)["links"]
        assert (
            api.delete("/identity/me/links/" + link["id"], headers=owner.headers()).status_code
            == 200
        )
        assert pe.with_session(resolve) is None
    finally:
        sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})


def test_github_link_lists_and_unlinks_slack_with_native_label_fallback(
    github_link_api: tuple[TestClient, GitHub], github_link_owners: list[support.Owner]
) -> None:
    api, _ = github_link_api
    owner, other = github_link_owners

    async def seed(session: AsyncSession) -> None:
        from curie_api.identity.service import find_or_create_namespace
        from curie_api.models import IdentityLink

        namespace = await find_or_create_namespace(
            session,
            tenant_id=owner.tenant_id,
            provider="slack",
            authority="T0EXAMPLE1",
            kind="slack_workspace",
            key="T0EXAMPLE1",
        )
        session.add(
            IdentityLink(
                tenant_id=owner.tenant_id,
                identity_namespace_id=namespace.id,
                provider_native_id="U0EXAMPLE1",
                principal_id=owner.principal_id,
                created_by_principal_id=owner.principal_id,
                verification_source="admin_mapped",
            )
        )
        await session.commit()

    pe.with_session(seed)
    [link] = links(api, owner)["links"]
    assert link["provider"] == "slack"
    assert link["display_label"] == "U0EXAMPLE1"
    assert links(api, other)["links"] == []
    assert (
        api.delete("/identity/me/links/" + link["id"], headers=other.headers()).status_code == 404
    )
    assert (
        api.delete("/identity/me/links/" + link["id"], headers=owner.headers()).status_code == 200
    )
    assert links(api, owner)["links"] == []


@pytest.mark.parametrize(
    "setting,value",
    [
        ("FACTORY_GITHUB_APP_CLIENT_ID", ""),
        ("FACTORY_GITHUB_APP_CLIENT_SECRET", ""),
        ("FACTORY_MODEL_AUTH_MODE", "service"),
    ],
)
def test_github_link_unconfigured_is_hidden_and_404(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    setting: str,
    value: str,
) -> None:
    api, _ = github_link_api
    owner = github_link_owners[0]
    assert links(api, owner)["github_link_available"] is True
    with support.environment({setting: value}):
        assert api.post(START, headers=owner.headers()).status_code == 404
        assert api.get(CALLBACK, params={"code": "x", "state": "y"}).status_code == 404
        if setting != "FACTORY_MODEL_AUTH_MODE":
            assert links(api, owner)["github_link_available"] is False
    assert callback(api, start(api, owner), owner).headers["location"].endswith("github=linked")


@pytest.mark.parametrize("configured", [True, False])
@pytest.mark.parametrize(
    "header",
    [
        "X-API-Key",
        "X-Curie-Approval-Principal",
        "X-Curie-Adapter-Principal",
        "X-Curie-Worker-Token",
    ],
)
def test_github_link_callback_refuses_foreign_credentials_and_consumes_state(
    github_link_api: tuple[TestClient, GitHub],
    github_link_owners: list[support.Owner],
    header: str,
    configured: bool,
) -> None:
    api, github = github_link_api
    owner = github_link_owners[0]
    state = start(api, owner)
    api.cookies.clear()
    settings = {} if configured else {"FACTORY_GITHUB_APP_CLIENT_ID": ""}
    with support.environment(settings):
        response = api.get(
            CALLBACK,
            params={"state": state, "code": "good"},
            headers={**owner.headers(origin=None), header: "EXAMPLE-foreign-credential"},
            follow_redirects=False,
        )
    assert response.status_code == 401
    assert "location" not in response.headers
    assert github.exchanges == 0
    assert links(api, owner)["links"] == []
    [row] = sql_dicts(
        "SELECT consumed_at FROM curie.github_link_states WHERE state_hash = :hash",
        {"hash": hashlib.sha256(state.encode()).hexdigest()},
    )
    assert row["consumed_at"] is not None
    assert callback(api, state, owner).headers["location"].endswith("github=failed")


def test_github_link_requires_principal_and_same_origin(
    github_link_api: tuple[TestClient, GitHub], github_link_owners: list[support.Owner]
) -> None:
    api, _ = github_link_api
    owner = github_link_owners[0]
    assert api.post(START).status_code == 401
    assert api.post(START, headers={"X-API-Key": get_settings().api_key}).status_code == 401
    assert api.post(START, headers=owner.headers(origin=None)).status_code == 403
    assert (
        api.post(START, headers=owner.headers(origin="https://foreign.example.com")).status_code
        == 403
    )
    assert api.get("/identity/me/links").status_code == 401
    assert (
        api.get("/identity/me/links", headers={"X-API-Key": get_settings().api_key}).status_code
        == 401
    )
    assert start(api, owner)


@pytest.mark.parametrize("suffix", ["", "/"])
def test_github_link_callback_access_log_redacts_entire_query(suffix: str) -> None:
    for prefix in ("", "/api"):
        record = logging.LogRecord(
            "uvicorn.access",
            logging.INFO,
            "",
            1,
            '%s - "%s %s HTTP/%s" %d',
            (
                "127.0.0.1",
                "GET",
                prefix
                + CALLBACK
                + suffix
                + "?code=EXAMPLEcode&state=EXAMPLEstate&other=EXAMPLEother",
                "1.1",
                303,
            ),
            None,
        )
        RedactingLogFilter().filter(record)
        rendered = record.getMessage()
        assert "EXAMPLEcode" not in rendered
        assert "EXAMPLEstate" not in rendered
        assert "EXAMPLEother" not in rendered
        assert "github_link_callback_query" in rendered
