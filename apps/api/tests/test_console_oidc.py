"""Generic OIDC console login: authorization code + PKCE (#2908, ADR 0155).

The flow is driven the way a browser drives it, against a real in-process IdP
(``oidc_test_idp``) on a real socket:

1. ``GET /console/oidc/login`` on the TestClient, redirects NOT followed: a 302
   to the IdP's authorize endpoint plus a ``__Host-curie_oidc_state`` cookie.
2. The authorize URL is fetched with a plain ``httpx`` client (the IdP is not the
   ASGI app); the auto-approving IdP answers 302 to the registered redirect URI,
   ``http://testserver/console/oidc/callback?code=...&state=...``.
3. That path + query is replayed on the TestClient with the state cookie.

Secure cookies: both cookies are set ``Secure`` and the TestClient's origin is
``http://testserver``, so its jar (correctly) never sends them back. Rather than
lean on the jar, every test parses ``Set-Cookie`` off the response itself
(:func:`_set_cookies`), clears the jar, and presents a cookie with an explicit
``Cookie`` header -- what a browser on the production HTTPS origin would send.
This is the same approach ``test_approval_authenticated_principals.py`` uses
for the console session cookie, and it keeps every test explicit about exactly
which credential it presents.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import urllib.parse
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from oidc_test_idp import (  # noqa: E402
    AUDIENCE,
    CLIENT_SECRET,
    OIDC_ENV_VARS,
    REDIRECT_URI,
    TestIdP,
    enabled_env,
    oidc_env,
)

# ``__Host-``: the browser only accepts it Secure, Path=/ and host-only, so a
# sibling subdomain or a plaintext response cannot plant a state of its own.
STATE_COOKIE = "__Host-curie_oidc_state"
LEGACY_STATE_COOKIE = "curie_oidc_state"
SESSION_COOKIE = "curie_console_session"
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"


# --- plumbing ----------------------------------------------------------------


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _set_cookies(response: httpx.Response) -> dict[str, Any]:
    """Every cookie the response sets, keyed by name, as ``Morsel`` objects."""

    jar: dict[str, Any] = {}
    for header in response.headers.get_list("set-cookie"):
        parsed: SimpleCookie = SimpleCookie()
        parsed.load(header)
        for name, morsel in parsed.items():
            jar[name] = morsel
    return jar


def _clears_cookie(response: httpx.Response, name: str) -> bool:
    morsel = _set_cookies(response).get(name)
    if morsel is None:
        return False
    return morsel.value in ("", '""') or morsel["max-age"] == "0"


def _cookie(name: str, value: str) -> dict[str, str]:
    return {"Cookie": f"{name}={value}"}


@dataclass
class Callback:
    """A callback request the IdP sent the browser back with, not yet replayed."""

    path: str
    params: dict[str, str]
    state_cookie: str | None
    login: httpx.Response


def _begin_login(client: TestClient) -> Callback:
    """Steps 1 and 2: start the login and let the IdP redirect back."""

    login = client.get("/console/oidc/login", follow_redirects=False)
    assert login.status_code == 302, login.text
    state_morsel = _set_cookies(login).get(STATE_COOKIE)
    assert state_morsel is not None, login.headers
    client.cookies.clear()

    with httpx.Client(trust_env=False, follow_redirects=False, timeout=10) as browser:
        at_idp = browser.get(login.headers["location"])
    assert at_idp.status_code == 302, at_idp.text
    back = urllib.parse.urlsplit(at_idp.headers["location"])
    assert f"{back.scheme}://{back.netloc}{back.path}" == REDIRECT_URI
    return Callback(
        path=back.path,
        params=dict(urllib.parse.parse_qsl(back.query)),
        state_cookie=state_morsel.value,
        login=login,
    )


def _finish(
    client: TestClient,
    callback: Callback,
    *,
    state_cookie: str | None = "use-issued",
    params: dict[str, str] | None = None,
) -> httpx.Response:
    """Step 3: replay the callback on the API, with an explicit state cookie."""

    cookie = callback.state_cookie if state_cookie == "use-issued" else state_cookie
    headers = _cookie(STATE_COOKIE, cookie) if cookie is not None else {}
    query = urllib.parse.urlencode(params if params is not None else callback.params)
    response = client.get(
        f"{callback.path}?{query}", headers=headers, follow_redirects=False
    )
    client.cookies.clear()
    return response


def _login(client: TestClient) -> httpx.Response:
    return _finish(client, _begin_login(client))


def _session_token(response: httpx.Response) -> str:
    assert response.status_code == 303, response.text
    morsel = _set_cookies(response).get(SESSION_COOKIE)
    assert morsel is not None and morsel.value, response.headers
    return str(morsel.value)


def _assert_refused(response: httpx.Response) -> None:
    """The one indistinguishable callback failure: 401, no session, state cleared."""

    assert response.status_code == 401, response.text
    assert SESSION_COOKIE not in _set_cookies(response), response.headers
    assert _clears_cookie(response, STATE_COOKIE), response.headers
    assert response.headers.get("cache-control") == "no-store"


def _principal_rows() -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, tenant_id, idp_issuer, idp_subject, type, status, email, "
        "display_name, last_seen_at FROM curie.principals ORDER BY idp_subject"
    )


# --- fixtures ----------------------------------------------------------------


@pytest.fixture
def idp() -> Iterator[TestIdP]:
    with TestIdP(
        client_id=AUDIENCE, client_secret=CLIENT_SECRET, redirect_uri=REDIRECT_URI
    ) as server:
        yield server


@pytest.fixture
def oidc_client(
    _disposable_db: Any, clean_db: None, runs_stream: str, idp: TestIdP
) -> Iterator[TestClient]:
    """An app booted with OIDC pointed at ``idp``, on a clean principal table.

    A local analogue of conftest's ``client``: the OIDC env has to be in place
    (and ``get_settings`` re-cached) BEFORE ``create_app()``, and restored after
    the app is gone, which fixture argument order alone does not guarantee.
    ``runs_stream`` isolates the approval test's queue exactly as
    ``approvals_client`` does.
    """

    with _booted_app(enabled_env(idp)) as client:
        yield client


@contextlib.contextmanager
def _booted_app(
    env: dict[str, str | None], *, raise_server_exceptions: bool = True
) -> Iterator[TestClient]:
    """Boot the app under ``env`` on a clean principal / login-attempt table."""

    # clean_db truncates console_sessions; principals are ours to reset. CASCADE
    # reaches principal_teams and (post-0052) console_sessions.principal_id.
    _sql("TRUNCATE curie.principals CASCADE")
    _sql("TRUNCATE curie.oidc_login_attempts")
    with oidc_env(env):
        from curie_api import oidc
        from curie_api.main import create_app

        oidc.reset_caches()
        try:
            with TestClient(
                create_app(), raise_server_exceptions=raise_server_exceptions
            ) as client:
                yield client
        finally:
            oidc.reset_caches()
            _sql("TRUNCATE curie.principals CASCADE")
            _sql("TRUNCATE curie.oidc_login_attempts")


@pytest.fixture
def lenient_oidc_client(
    _disposable_db: Any, clean_db: None, runs_stream: str, idp: TestIdP
) -> Iterator[TestClient]:
    """``oidc_client``, but an escaped exception is a 500 response, not a raise.

    A misbehaving IdP must never produce a 500; answering one (rather than
    re-raising into the test) lets the malformed-response tests assert on the
    response the browser would actually get.
    """

    with _booted_app(enabled_env(idp), raise_server_exceptions=False) as client:
        yield client


@pytest.fixture
def public_idp() -> Iterator[TestIdP]:
    """An IdP that registers Curie as a public client: no secret at all."""

    with TestIdP(
        client_id=AUDIENCE,
        client_secret=None,
        redirect_uri=REDIRECT_URI,
        token_auth_methods=["none"],
    ) as server:
        yield server


@pytest.fixture
def public_oidc_client(
    _disposable_db: Any, clean_db: None, runs_stream: str, public_idp: TestIdP
) -> Iterator[TestClient]:
    """The app configured with an EMPTY ``CURIE_OIDC_CLIENT_SECRET``."""

    with _booted_app({**enabled_env(public_idp), "CURIE_OIDC_CLIENT_SECRET": ""}) as client:
        yield client


# --- login redirect ------------------------------------------------------------


def test_login_redirects_to_idp_with_pkce_state_and_nonce(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    response = oidc_client.get("/console/oidc/login", follow_redirects=False)

    assert response.status_code == 302, response.text
    assert response.headers.get("cache-control") == "no-store"
    location = urllib.parse.urlsplit(response.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == idp.authorization_endpoint
    query = dict(urllib.parse.parse_qsl(location.query))
    assert query["response_type"] == "code"
    assert query["client_id"] == AUDIENCE
    assert query["redirect_uri"] == REDIRECT_URI
    assert "openid" in query["scope"].split()
    assert query["code_challenge_method"] == "S256"
    for name in ("state", "nonce", "code_challenge"):
        # secrets.token_urlsafe(32) / a SHA-256 digest: never short.
        assert len(query.get(name, "")) >= 32, (name, query)
    assert query["state"] != query["nonce"]

    morsel = _set_cookies(response)[STATE_COOKIE]
    assert morsel.value == query["state"]
    assert morsel["httponly"]
    assert morsel["secure"]
    assert morsel["samesite"].lower() == "lax"
    # __Host- rules: Path=/ and host-only (no Domain attribute).
    assert morsel["path"] == "/"
    assert morsel["domain"] == ""
    raw = next(
        header
        for header in response.headers.get_list("set-cookie")
        if header.startswith(f"{STATE_COOKIE}=")
    )
    assert "domain=" not in raw.lower(), raw
    assert LEGACY_STATE_COOKIE not in _set_cookies(response)


def test_each_login_mints_fresh_state_nonce_and_challenge(oidc_client: TestClient) -> None:
    first = dict(
        urllib.parse.parse_qsl(
            urllib.parse.urlsplit(
                oidc_client.get("/console/oidc/login", follow_redirects=False).headers[
                    "location"
                ]
            ).query
        )
    )
    second = dict(
        urllib.parse.parse_qsl(
            urllib.parse.urlsplit(
                oidc_client.get("/console/oidc/login", follow_redirects=False).headers[
                    "location"
                ]
            ).query
        )
    )
    for name in ("state", "nonce", "code_challenge"):
        assert first[name] != second[name], name


def test_login_attempt_stores_only_a_hash_of_the_state(oidc_client: TestClient) -> None:
    callback = _begin_login(oidc_client)
    state = callback.params["state"]
    rows = _sql("SELECT state_hash, consumed_at FROM curie.oidc_login_attempts")
    assert rows, "the login must persist a server-side attempt"
    assert all(row["state_hash"] != state for row in rows)
    assert not any(state in str(row) for row in rows)


# --- the happy path -----------------------------------------------------------


def test_callback_mints_a_console_session_and_redirects_home(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    callback = _begin_login(oidc_client)
    response = _finish(oidc_client, callback)

    assert response.status_code == 303, response.text
    assert response.headers["location"] == "/"
    assert response.headers.get("cache-control") == "no-store"
    session = _set_cookies(response)[SESSION_COOKIE]
    assert session.value
    assert session["httponly"]
    assert session["secure"]
    assert session["samesite"].lower() == "strict"
    assert session["path"] == "/"
    assert _clears_cookie(response, STATE_COOKIE), response.headers
    # No token material in the body.
    assert session.value not in response.text
    assert "id_token" not in response.text

    # The API exchanged the code with the PKCE verifier and client credentials.
    (token_request,) = idp.token_requests
    assert token_request["auth_method"] == "client_secret_basic"
    assert token_request["form"]["grant_type"] == "authorization_code"
    assert token_request["form"]["code"] == callback.params["code"]
    assert token_request["form"].get("code_verifier")


def test_principal_endpoint_returns_the_logged_in_principal(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    token = _session_token(_login(oidc_client))

    response = oidc_client.get("/console/principal", headers=_cookie(SESSION_COOKIE, token))

    assert response.status_code == 200, response.text
    assert response.headers.get("cache-control") == "no-store"
    body = response.json()
    (row,) = _principal_rows()
    assert body == {
        "id": str(row["id"]),
        "tenant_id": DEFAULT_TENANT_ID,
        "idp_subject": idp.subject,
        "display_name": idp.name,
        "email": idp.email,
        "status": "active",
    }


def test_principal_is_created_lazily_then_reused_and_refreshed(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    assert _principal_rows() == []
    _session_token(_login(oidc_client))

    (first,) = _principal_rows()
    assert str(first["tenant_id"]) == DEFAULT_TENANT_ID
    assert first["idp_issuer"] == idp.issuer
    assert first["idp_subject"] == idp.subject
    assert first["type"] == "human"
    assert first["status"] == "active"
    assert first["email"] == idp.email
    assert first["display_name"] == idp.name
    assert first["last_seen_at"] is not None

    idp.email = "alice.new@example.com"
    idp.name = "Alice Renamed"
    _session_token(_login(oidc_client))

    (second,) = _principal_rows()
    assert second["id"] == first["id"]
    assert second["email"] == "alice.new@example.com"
    assert second["display_name"] == "Alice Renamed"
    assert second["last_seen_at"] > first["last_seen_at"]


def test_a_different_subject_is_a_different_principal_even_with_the_same_email(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    _session_token(_login(oidc_client))
    idp.subject = "idp-user-2"  # same email: never the identity key
    _session_token(_login(oidc_client))

    rows = _principal_rows()
    assert [row["idp_subject"] for row in rows] == ["idp-user-1", "idp-user-2"]
    assert rows[0]["id"] != rows[1]["id"]


def test_oidc_session_row_binds_the_principal_and_no_subject(
    oidc_client: TestClient,
) -> None:
    token = _session_token(_login(oidc_client))
    (principal,) = _principal_rows()

    rows = _sql(
        "SELECT principal_id, subject, session_token_hash, session_expires_at, "
        "consumed_at, revoked_at FROM curie.console_sessions"
    )
    (row,) = rows
    assert row["principal_id"] == principal["id"]
    assert row["subject"] is None
    assert row["session_expires_at"] is not None
    assert row["revoked_at"] is None
    # The login-code half is born consumed, so it can never be redeemed.
    assert row["consumed_at"] is not None
    # Only a hash is stored.
    assert row["session_token_hash"] and row["session_token_hash"] != token

    # A subject-less session is not a login-code/approval session.
    assert (
        oidc_client.get("/console/session", headers=_cookie(SESSION_COOKIE, token)).status_code
        == 401
    )


@pytest.mark.parametrize("methods", [["client_secret_post"], ["client_secret_basic"]])
def test_client_authentication_follows_what_discovery_advertises(
    oidc_client: TestClient, idp: TestIdP, methods: list[str]
) -> None:
    idp.token_auth_methods = methods
    _session_token(_login(oidc_client))
    (token_request,) = idp.token_requests
    assert token_request["auth_method"] == methods[0]
    if methods == ["client_secret_basic"]:
        assert "client_secret" not in token_request["form"]


# --- callback failures -------------------------------------------------------


def test_state_mismatch_is_refused(oidc_client: TestClient) -> None:
    callback = _begin_login(oidc_client)
    response = _finish(oidc_client, callback, state_cookie="some-other-state")
    _assert_refused(response)
    assert _principal_rows() == []


def test_missing_state_cookie_is_refused(oidc_client: TestClient, idp: TestIdP) -> None:
    callback = _begin_login(oidc_client)
    response = _finish(oidc_client, callback, state_cookie=None)
    _assert_refused(response)
    # Login-CSRF binding is checked before the code is spent at the IdP.
    assert idp.token_requests == []
    assert _principal_rows() == []


def test_legacy_state_cookie_name_is_not_accepted(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    # Only the __Host- name binds the callback: a plain-named cookie is exactly
    # what a sibling subdomain or plaintext response could plant.
    callback = _begin_login(oidc_client)
    response = oidc_client.get(
        f"{callback.path}?{urllib.parse.urlencode(callback.params)}",
        headers=_cookie(LEGACY_STATE_COOKIE, str(callback.state_cookie)),
        follow_redirects=False,
    )
    oidc_client.cookies.clear()
    _assert_refused(response)
    assert idp.token_requests == []
    assert _principal_rows() == []


def test_forged_state_matching_its_own_cookie_is_refused(oidc_client: TestClient) -> None:
    # An attacker who controls both the query and the cookie still needs a
    # server-side attempt for that state.
    callback = _begin_login(oidc_client)
    params = {**callback.params, "state": "attacker-chosen"}
    response = _finish(oidc_client, callback, state_cookie="attacker-chosen", params=params)
    _assert_refused(response)


def test_replayed_callback_is_refused(oidc_client: TestClient) -> None:
    callback = _begin_login(oidc_client)
    _session_token(_finish(oidc_client, callback))

    replay = _finish(oidc_client, callback)
    _assert_refused(replay)
    assert len(_sql("SELECT id FROM curie.console_sessions")) == 1


def test_expired_attempt_is_refused(oidc_client: TestClient, idp: TestIdP) -> None:
    callback = _begin_login(oidc_client)
    _sql("UPDATE curie.oidc_login_attempts SET expires_at = expires_at - interval '1 day'")
    response = _finish(oidc_client, callback)
    _assert_refused(response)
    assert idp.token_requests == []
    assert _principal_rows() == []


def test_idp_error_is_refused_without_reflecting_it(
    oidc_client: TestClient, idp: TestIdP
) -> None:
    idp.authorize_error = "access_denied"
    callback = _begin_login(oidc_client)
    assert callback.params["error"] == "access_denied"
    assert "code" not in callback.params

    response = _finish(oidc_client, callback)
    _assert_refused(response)
    assert "access_denied" not in response.text
    assert "script" not in response.text
    assert idp.token_requests == []


def test_callback_without_code_is_refused(oidc_client: TestClient) -> None:
    callback = _begin_login(oidc_client)
    params = {"state": callback.params["state"]}
    _assert_refused(_finish(oidc_client, callback, params=params))


@pytest.mark.parametrize("variant", ["wrong_nonce", "wrong_aud", "expired", "alg_none"])
def test_bad_id_token_from_the_idp_is_refused(
    oidc_client: TestClient, idp: TestIdP, variant: str
) -> None:
    idp.token_variant = variant
    _assert_refused(_login(oidc_client))
    assert _principal_rows() == []


def test_failures_share_one_detail(oidc_client: TestClient, idp: TestIdP) -> None:
    mismatch = _finish(oidc_client, _begin_login(oidc_client), state_cookie="nope")
    idp.token_variant = "wrong_nonce"
    bad_token = _login(oidc_client)
    assert mismatch.status_code == bad_token.status_code == 401
    assert mismatch.json() == bad_token.json()


# --- a misbehaving IdP ---------------------------------------------------------


def _refusal_body(client: TestClient) -> Any:
    """The body of an ordinary callback refusal (no attempt, no cookie)."""

    response = client.get("/console/oidc/callback?code=x&state=y", follow_redirects=False)
    client.cookies.clear()
    assert response.status_code == 401, response.text
    return response.json()


_MALFORMED_IDP = {
    # httpx raises InvalidURL (not an HTTPError) for these at request time.
    "token-endpoint-unparseable-host": lambda idp: idp.discovery_overrides.update(
        token_endpoint="http://[::1"
    ),
    "token-endpoint-nul": lambda idp: idp.discovery_overrides.update(
        token_endpoint="https://idp.example/tok\x00en"
    ),
    # Under the 64 KiB cap, far past the recursion limit: RecursionError.
    "token-deeply-nested-json": lambda idp: setattr(idp, "token_raw_body", b"[" * 60000),
    "token-not-json": lambda idp: setattr(idp, "token_raw_body", b"<html>oops</html>"),
    "token-not-utf8": lambda idp: setattr(idp, "token_raw_body", b"\xff\xfe\xfd"),
    "token-json-array": lambda idp: setattr(idp, "token_raw_body", b"[1, 2]"),
    "id-token-integer": lambda idp: idp.token_response_overrides.update(id_token=12345),
    "id-token-object": lambda idp: idp.token_response_overrides.update(id_token={"a": 1}),
    "id-token-missing": lambda idp: idp.token_response_overrides.update(id_token=None),
    # Signed by the real key, so these reach claim validation: OverflowError.
    "exp-infinity": lambda idp: idp.token_claim_overrides.update(exp=float("inf")),
    # ... and TypeError.
    "exp-list": lambda idp: idp.token_claim_overrides.update(exp=[1]),
    "exp-object": lambda idp: idp.token_claim_overrides.update(exp={"at": 1}),
    "iat-list": lambda idp: idp.token_claim_overrides.update(iat=[1]),
    "nbf-object": lambda idp: idp.token_claim_overrides.update(nbf={"at": 1}),
    "exp-string": lambda idp: idp.token_claim_overrides.update(exp="soon"),
}


@pytest.mark.parametrize("case", sorted(_MALFORMED_IDP))
def test_malformed_idp_response_is_the_one_refusal(
    lenient_oidc_client: TestClient, idp: TestIdP, case: str
) -> None:
    """Whatever shape of garbage the IdP returns, the browser sees the one 401.

    Never a 500: that would skip clearing the state cookie and tell a prober
    which step failed (the module contract in ``curie_api.oidc``).
    """

    expected = _refusal_body(lenient_oidc_client)
    _MALFORMED_IDP[case](idp)

    response = _login(lenient_oidc_client)

    _assert_refused(response)
    assert response.json() == expected
    assert _principal_rows() == []
    assert _sql("SELECT id FROM curie.console_sessions") == []


# --- bounded login attempts ------------------------------------------------------


def _attempt_count() -> int:
    return int(_sql("SELECT count(*) AS n FROM curie.oidc_login_attempts")[0]["n"])


def _cap_at(monkeypatch: pytest.MonkeyPatch, cap: int) -> None:
    from curie_api import crud

    # raising=False so a missing constant fails on behavior, not on setup.
    monkeypatch.setattr(crud, "OIDC_LOGIN_ATTEMPT_CAP", cap, raising=False)


def test_login_attempt_cap_is_a_positive_int() -> None:
    from curie_api import crud

    cap = getattr(crud, "OIDC_LOGIN_ATTEMPT_CAP", None)
    assert isinstance(cap, int) and not isinstance(cap, bool), cap
    assert cap > 0


def test_login_is_refused_at_the_live_attempt_cap(
    oidc_client: TestClient, idp: TestIdP, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cap_at(monkeypatch, 3)
    for _ in range(3):
        started = oidc_client.get("/console/oidc/login", follow_redirects=False)
        assert started.status_code == 302, started.text
    oidc_client.cookies.clear()
    assert _attempt_count() == 3

    refused = oidc_client.get("/console/oidc/login", follow_redirects=False)

    assert refused.status_code == 503, refused.text
    assert refused.headers.get("cache-control") == "no-store"
    assert "location" not in refused.headers
    assert STATE_COOKIE not in _set_cookies(refused)
    assert LEGACY_STATE_COOKIE not in _set_cookies(refused)
    assert _attempt_count() == 3
    assert idp.authorize_requests == []


def test_expired_attempts_do_not_count_toward_the_cap(
    oidc_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _cap_at(monkeypatch, 3)
    for _ in range(3):
        assert oidc_client.get("/console/oidc/login", follow_redirects=False).status_code == 302
    oidc_client.cookies.clear()
    _sql("UPDATE curie.oidc_login_attempts SET expires_at = now() - interval '1 minute'")

    response = oidc_client.get("/console/oidc/login", follow_redirects=False)

    assert response.status_code == 302, response.text
    assert STATE_COOKIE in _set_cookies(response)


def _assert_refused_at_cap(response: httpx.Response) -> None:
    assert response.status_code == 503, response.text
    assert response.headers.get("cache-control") == "no-store"
    assert "location" not in response.headers
    assert STATE_COOKIE not in _set_cookies(response)
    assert LEGACY_STATE_COOKIE not in _set_cookies(response)


def test_consumed_attempts_still_count_toward_the_cap(
    oidc_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A consumed row stays until it expires, so consuming frees no capacity.
    _cap_at(monkeypatch, 3)
    for _ in range(3):
        _session_token(_login(oidc_client))
    consumed = _sql(
        "SELECT id FROM curie.oidc_login_attempts WHERE consumed_at IS NOT NULL"
    )
    assert len(consumed) == 3

    response = oidc_client.get("/console/oidc/login", follow_redirects=False)

    _assert_refused_at_cap(response)
    assert _attempt_count() == 3


def test_idp_error_callbacks_cannot_recycle_cap_capacity(
    oidc_client: TestClient, idp: TestIdP, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An anonymous login -> callback?error=x loop must still hit the cap."""

    _cap_at(monkeypatch, 3)
    callback_path = urllib.parse.urlsplit(REDIRECT_URI).path
    for _ in range(3):
        login = oidc_client.get("/console/oidc/login", follow_redirects=False)
        assert login.status_code == 302, login.text
        state = dict(
            urllib.parse.parse_qsl(urllib.parse.urlsplit(login.headers["location"]).query)
        )["state"]
        cookie = _set_cookies(login)[STATE_COOKIE].value
        oidc_client.cookies.clear()
        query = urllib.parse.urlencode({"error": "x", "state": state})
        _assert_refused(
            oidc_client.get(
                f"{callback_path}?{query}",
                headers=_cookie(STATE_COOKIE, cookie),
                follow_redirects=False,
            )
        )
        oidc_client.cookies.clear()
    assert _attempt_count() == 3

    refused = oidc_client.get("/console/oidc/login", follow_redirects=False)

    _assert_refused_at_cap(refused)
    assert _attempt_count() == 3
    assert idp.token_requests == []


# --- principal and tenant status ----------------------------------------------


@pytest.mark.parametrize("status", ["disabled", "revoked"])
def test_inactive_principal_is_refused_at_login_and_on_existing_sessions(
    oidc_client: TestClient, status: str
) -> None:
    token = _session_token(_login(oidc_client))
    principal_headers = _cookie(SESSION_COOKIE, token)
    assert oidc_client.get("/console/principal", headers=principal_headers).status_code == 200

    _sql("UPDATE curie.principals SET status = :status", {"status": status})

    existing = oidc_client.get("/console/principal", headers=principal_headers)
    assert existing.status_code == 401, existing.text
    assert existing.headers.get("cache-control") == "no-store"

    sessions_before = len(_sql("SELECT id FROM curie.console_sessions"))
    _assert_refused(_login(oidc_client))
    assert len(_sql("SELECT id FROM curie.console_sessions")) == sessions_before
    # A refused login does not reactivate the principal.
    (row,) = _principal_rows()
    assert row["status"] == status


def test_suspended_tenant_is_refused(oidc_client: TestClient) -> None:
    token = _session_token(_login(oidc_client))
    headers = _cookie(SESSION_COOKIE, token)
    try:
        _sql(
            "UPDATE curie.tenants SET status = 'suspended' WHERE id = :id",
            {"id": uuid.UUID(DEFAULT_TENANT_ID)},
        )
        existing = oidc_client.get("/console/principal", headers=headers)
        assert existing.status_code == 401, existing.text
        _assert_refused(_login(oidc_client))
    finally:
        _sql(
            "UPDATE curie.tenants SET status = 'active' WHERE id = :id",
            {"id": uuid.UUID(DEFAULT_TENANT_ID)},
        )


def test_revoked_oidc_session_is_refused(oidc_client: TestClient) -> None:
    token = _session_token(_login(oidc_client))
    _sql("UPDATE curie.console_sessions SET revoked_at = now()")
    response = oidc_client.get("/console/principal", headers=_cookie(SESSION_COOKIE, token))
    assert response.status_code == 401, response.text


# --- require_principal_session against other credentials -----------------------


def test_login_code_session_has_no_principal(
    oidc_client: TestClient, auth_headers: dict[str, str]
) -> None:
    minted = oidc_client.post(
        "/console/login-codes", json={"subject": "U0EXAMPLE1"}, headers=auth_headers
    )
    assert minted.status_code == 201, minted.text
    exchanged = oidc_client.post("/console/session", json={"code": minted.json()["code"]})
    assert exchanged.status_code == 200, exchanged.text
    token = _set_cookies(exchanged)[SESSION_COOKIE].value
    oidc_client.cookies.clear()

    response = oidc_client.get("/console/principal", headers=_cookie(SESSION_COOKIE, token))
    assert response.status_code == 401, response.text
    assert response.headers.get("cache-control") == "no-store"


@pytest.mark.parametrize(
    "headers",
    [{}, {"Cookie": f"{SESSION_COOKIE}=not-a-session"}],
    ids=["no-cookie", "unknown-token"],
)
def test_principal_endpoint_requires_a_live_session(
    oidc_client: TestClient, auth_headers: dict[str, str], headers: dict[str, str]
) -> None:
    response = oidc_client.get("/console/principal", headers=headers)
    assert response.status_code == 401, response.text
    assert response.headers.get("cache-control") == "no-store"
    # And the platform key is not a principal session either.
    assert oidc_client.get("/console/principal", headers=auth_headers).status_code == 401


def test_oidc_session_cannot_resolve_an_approval(
    oidc_client: TestClient, idp: TestIdP, auth_headers: dict[str, str]
) -> None:
    """ADR-0106 / plan: OIDC sessions leave ``subject`` NULL, so no approval authority.

    Modeled on test_approval_authenticated_principals.py's
    ``test_null_subject_console_session_cannot_be_an_approval_principal``. The
    approver list deliberately names the IdP ``sub``: an IdP-controlled subject
    that happens to equal a provider id must NOT gain approval authority.
    """

    # A Slack-shaped ``sub``: agent creation validates approver ids as Slack
    # user ids, and a collision with one is exactly the case being guarded.
    idp.subject = "U0IDPUSER1"
    token = _session_token(_login(oidc_client))

    route = f"operators-{uuid.uuid4().hex[:8]}"
    source_channel = f"C0SOURCE{uuid.uuid4().hex[:8].upper()}"
    agent = oidc_client.post(
        "/agents",
        json={
            "name": f"oidc-approval-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": source_channel},
            "approval_routes": {
                route: {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                    "approvers": {"users": [idp.subject]},
                }
            },
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    approval = oidc_client.post(
        "/approvals",
        json={
            "conversation_id": f"th-{uuid.uuid4().hex[:8]}",
            "author": "U0EXAMPLE2",
            "summary": "Confirm the requested action",
            "reply_kind": "slack",
            "reply_channel": source_channel,
            "reply_placeholder": "p-1",
            "dedupe_key": uuid.uuid4().hex,
            "agent_id": agent.json()["id"],
            "route": route,
            "card_channel": "C0EXAMPLE1",
            "gate_kind": "policy",
        },
        headers=auth_headers,
    )
    assert approval.status_code == 201, approval.text
    approval_id = approval.json()["id"]

    denied = oidc_client.post(
        f"/approvals/{approval_id}/resolve",
        json={"decision": "approved"},
        headers=_cookie(SESSION_COOKIE, token),
    )
    assert denied.status_code == 401, denied.text
    status = oidc_client.get(f"/approvals/{approval_id}", headers=auth_headers).json()["status"]
    assert status == "pending"


# --- configuration -------------------------------------------------------------


@pytest.fixture
def disabled_client(_disposable_db: Any) -> Iterator[TestClient]:
    with oidc_env({}):
        from curie_api.main import create_app

        with TestClient(create_app()) as client:
            yield client


def test_oidc_disabled_hides_both_routes(disabled_client: TestClient) -> None:
    # Guard against a vacuous pass: OIDC is disabled because the setting exists
    # and is empty, not because the routes were never written.
    assert get_settings().oidc_issuer == ""
    for path in ("/console/oidc/login", "/console/oidc/callback?code=x&state=y"):
        response = disabled_client.get(path, follow_redirects=False)
        assert response.status_code == 404, (path, response.text)
        assert STATE_COOKIE not in _set_cookies(response)
        assert SESSION_COOKIE not in _set_cookies(response)


_FULL = {
    "CURIE_OIDC_ISSUER": "https://idp.example",
    "CURIE_OIDC_AUDIENCE": "curie-console",
    "CURIE_OIDC_JWKS_URL": "https://idp.example/jwks",
    "CURIE_OIDC_REDIRECT_URI": "https://curie.example/console/oidc/callback",
}


def test_full_oidc_settings_load() -> None:
    from curie_api.config import Settings

    with oidc_env({**_FULL, "CURIE_OIDC_CLIENT_SECRET": "s"}):
        settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.oidc_issuer == _FULL["CURIE_OIDC_ISSUER"]
    assert settings.oidc_audience == _FULL["CURIE_OIDC_AUDIENCE"]
    assert settings.oidc_jwks_url == _FULL["CURIE_OIDC_JWKS_URL"]
    assert settings.oidc_redirect_uri == _FULL["CURIE_OIDC_REDIRECT_URI"]
    assert settings.oidc_client_secret == "s"


def test_client_secret_is_optional_and_the_public_client_logs_in(
    public_oidc_client: TestClient, public_idp: TestIdP
) -> None:
    """An empty secret makes Curie a public client that still completes a login.

    The token request carries the bare ``client_id`` in the form body and no
    client authentication at all -- no Basic header and no ``client_secret`` --
    with the PKCE verifier as the only proof it started this login.
    """

    assert get_settings().oidc_client_secret == ""

    token = _session_token(_login(public_oidc_client))

    (token_request,) = public_idp.token_requests
    assert token_request["auth_method"] == "none"
    assert token_request["authorization"] is None
    assert token_request["form"]["client_id"] == AUDIENCE
    assert "client_secret" not in token_request["form"]
    assert token_request["form"].get("code_verifier")
    principal = public_oidc_client.get(
        "/console/principal", headers=_cookie(SESSION_COOKIE, token)
    )
    assert principal.status_code == 200, principal.text
    assert principal.json()["idp_subject"] == public_idp.subject


def test_no_oidc_settings_is_disabled() -> None:
    from curie_api.config import Settings

    with oidc_env({}):
        settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.oidc_issuer == ""


@pytest.mark.parametrize(
    "missing",
    [
        ("CURIE_OIDC_ISSUER",),
        ("CURIE_OIDC_AUDIENCE",),
        ("CURIE_OIDC_JWKS_URL",),
        ("CURIE_OIDC_REDIRECT_URI",),
        ("CURIE_OIDC_AUDIENCE", "CURIE_OIDC_JWKS_URL", "CURIE_OIDC_REDIRECT_URI"),
        ("CURIE_OIDC_ISSUER", "CURIE_OIDC_JWKS_URL", "CURIE_OIDC_REDIRECT_URI"),
    ],
    ids=lambda names: "missing-" + "+".join(n.removeprefix("CURIE_OIDC_") for n in names),
)
def test_partial_oidc_settings_refuse_to_load(missing: tuple[str, ...]) -> None:
    from curie_api.config import Settings
    from pydantic import ValidationError

    assert set(missing) <= set(OIDC_ENV_VARS)
    partial = {name: value for name, value in _FULL.items() if name not in missing}
    with oidc_env(partial), pytest.raises(ValidationError):
        Settings(_env_file=None)  # type: ignore[call-arg]


# Short on purpose: pydantic keeps the head and tail of a long repr, and a
# short secret at the end of the input dict survives the elision whole.
_SECRET_SENTINEL = "Xq7Zk2Wp"


@pytest.mark.parametrize(
    "present",
    [
        ("CURIE_OIDC_ISSUER",),
        ("CURIE_OIDC_ISSUER", "CURIE_OIDC_AUDIENCE", "CURIE_OIDC_JWKS_URL"),
    ],
    ids=["issuer-only", "missing-redirect"],
)
def test_partial_oidc_settings_error_does_not_print_the_client_secret(
    present: tuple[str, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The boot error lands in container logs; the client secret must not.

    The rest of the environment is emptied first: pydantic elides the middle
    of a long ``input_value`` repr, so on a busy test environment a leak could
    hide behind the ellipsis. A small config is where it prints verbatim.
    """

    import os

    from curie_api.config import Settings
    from pydantic import ValidationError

    for name in list(os.environ):
        monkeypatch.delenv(name, raising=False)

    partial = {name: _FULL[name] for name in present}
    missing = [name for name in _FULL if name not in present]
    with (
        oidc_env({**partial, "CURIE_OIDC_CLIENT_SECRET": _SECRET_SENTINEL}),
        pytest.raises(ValidationError) as caught,
    ):
        Settings(_env_file=None)  # type: ignore[call-arg]

    message = str(caught.value)
    assert _SECRET_SENTINEL not in message
    assert _SECRET_SENTINEL not in repr(caught.value)
    for name in missing:
        assert name in message, (name, message)
