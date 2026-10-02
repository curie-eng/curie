#!/usr/bin/env python3
"""Drive generic OIDC console login end to end against a real Dex (#2908, #3800).

`curie dev oidc-e2e` boots this checkout's API and console UI from
compose.dev.yaml as an isolated compose project (tools/oidc-e2e/
compose.oidc-e2e.yaml), puts a real Dex beside them, and drives every login the
way a browser does, over plain HTTP: the login starts and the callback returns
through the console's /api proxy, Dex's connector chooser and password form are
followed, and each callback is captured before it is replayed so the driver
decides which cookies it carries. Checks run in phases, one API restart each:

1. Default config: schema head, the authorize redirect (PKCE S256, nonce,
   state, state cookie flags), a password login and its session cookie, the
   principal row and its reuse on a second login, an unused callback refused
   without its state cookie and then completed with it, a replay refused,
   logout refusing a foreign or missing Origin and revoking with a matching
   one, the retired cookie name refused, and a console session refused on a
   platform-key route.
2. Required claims {"groups": "authors"} with the groups scope: Dex's mock
   connector (groups: authors) is admitted, and the password user (no groups)
   is refused without a principal being written.
3. Issuer switched, then restored: a live session is refused, then readmitted
   unchanged; nothing is revoked.
4. Rate limit: login starts from one peer reach 429 with Retry-After.
5. OIDC off: both routes are 404, and the console's existing login still
   works: a login code minted with the operator CLI is exchanged through the
   proxy and resolves a pending approval.

Every stack resource is removed on exit (`--keep` leaves it up for debugging),
and the result is a JSON evidence file. Standard library only.

Inputs, all optional:
  CURIE_BIN                      curie binary for the login-code mint (default
                                 `curie` on PATH; without one the code is minted
                                 through the API and the evidence says so)
  CURIE_OIDC_E2E_PROJECT         compose project (default curie-oidc-e2e)
  CURIE_OIDC_E2E_API_PORT        host port for the API (default 28400)
  CURIE_OIDC_E2E_UI_PORT         host port for the console UI (default 28480)
  CURIE_OIDC_E2E_DEX_PORT        host port for Dex (default 25556)
  CURIE_OIDC_E2E_IMAGE_TAG       API image tag, ghcr.io/curie-eng/curie-api:<tag>
                                 (default oidc-e2e)
  CURIE_OIDC_E2E_UI_IMAGE        console UI image (default
                                 ghcr.io/curie-eng/curie-ui:<tag>)
Images missing locally are built from this checkout; `--build` always builds.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from email.message import Message
from http.cookies import SimpleCookie
from pathlib import Path
from string import Template
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOL_DIR = Path(__file__).resolve().parent
COMPOSE_DEV = REPO_ROOT / "compose.dev.yaml"
COMPOSE_OVERLAY = TOOL_DIR / "compose.oidc-e2e.yaml"
DEX_TEMPLATE = TOOL_DIR / "dex.yaml.template"

# The multi-arch index digest, so amd64 CI and arm64 laptops pull the same pin.
DEX_IMAGE = (
    "ghcr.io/dexidp/dex:v2.41.1"
    "@sha256:bc7cfce7c17f52864e2bb2a4dc1d2f86a41e3019f6d42e81d92a301fad0c8a1d"
)
CLIENT_ID = "curie-console"
CLIENT_SECRET = "oidc-e2e-client-secret"
API_KEY = "curie-dev-key"
USER_EMAIL = "alice@example.com"
USER_PASSWORD = "password"
MOCK_EMAIL = "kilgore@kilgore.trout"
APPROVER = "U0OIDCE2E1"
STATE_COOKIE = "__Host-curie_oidc_state"
SESSION_COOKIE = "__Host-curie_console_session"
LEGACY_SESSION_COOKIE = "curie_console_session"
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
OIDC_REFUSED = "OIDC login failed"
LOGIN_BUDGET = 30


class CheckFailed(AssertionError):
    pass


# --- HTTP ----------------------------------------------------------------------


@dataclass
class Response:
    status: int
    headers: Message
    body: str

    def json(self) -> Any:
        return json.loads(self.body)

    @property
    def location(self) -> str:
        return self.headers.get("Location", "")

    def set_cookies(self) -> dict[str, Any]:
        jar: dict[str, Any] = {}
        for header in self.headers.get_all("Set-Cookie") or []:
            parsed: SimpleCookie = SimpleCookie()
            parsed.load(header)
            jar.update(parsed)
        return jar

    def raw_set_cookie(self, name: str) -> str:
        for header in self.headers.get_all("Set-Cookie") or []:
            if header.split("=", 1)[0].strip() == name:
                return header
        return ""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    json_body: Any = None,
    form: dict[str, str] | None = None,
) -> Response:
    data = None
    all_headers = dict(headers or {})
    if json_body is not None:
        data = json.dumps(json_body).encode()
        all_headers["Content-Type"] = "application/json"
    elif form is not None:
        data = urllib.parse.urlencode(form).encode()
        all_headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=data, method=method, headers=all_headers)
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return Response(resp.status, resp.headers, resp.read().decode(errors="replace"))
    except urllib.error.HTTPError as err:
        return Response(err.code, err.headers, err.read().decode(errors="replace"))


def cookie(name: str, value: str) -> dict[str, str]:
    return {"Cookie": f"{name}={value}"}


# --- evidence ------------------------------------------------------------------


@dataclass
class Evidence:
    checks: list[dict[str, Any]] = field(default_factory=list)

    def check(self, name: str, ok: bool, detail: Any = "") -> None:
        self.checks.append({"check": name, "ok": bool(ok), "detail": detail})
        mark = "PASS" if ok else "FAIL"
        print(f"{mark}  {name}" + (f"  ({detail})" if detail and not ok else ""), flush=True)
        if not ok:
            raise CheckFailed(f"{name}: {detail}")


# --- the stack -----------------------------------------------------------------


@dataclass
class Settings:
    project: str
    api_port: int
    ui_port: int
    dex_port: int
    image_tag: str
    ui_image: str
    curie_bin: str | None
    workdir: Path

    @property
    def api(self) -> str:
        return f"http://localhost:{self.api_port}"

    @property
    def ui(self) -> str:
        return f"http://localhost:{self.ui_port}"

    @property
    def issuer(self) -> str:
        return f"http://localhost:{self.dex_port}/dex"

    @property
    def dex_origin(self) -> str:
        return f"http://localhost:{self.dex_port}"

    @property
    def redirect_uri(self) -> str:
        return f"{self.ui}/api/console/oidc/callback"

    @property
    def dex_config(self) -> Path:
        return self.workdir / "dex.yaml"


def oidc_on(s: Settings, **overrides: str) -> dict[str, str]:
    config = {
        "CURIE_OIDC_ISSUER": s.issuer,
        "CURIE_OIDC_AUDIENCE": CLIENT_ID,
        "CURIE_OIDC_JWKS_URL": f"{s.issuer}/keys",
        "CURIE_OIDC_CLIENT_SECRET": CLIENT_SECRET,
        "CURIE_OIDC_REDIRECT_URI": s.redirect_uri,
        "CURIE_OIDC_SCOPES": "openid email profile",
        "CURIE_OIDC_REQUIRED_CLAIMS": "",
        "CURIE_OIDC_ADMIT_ALL_AUTHENTICATED": "false",
    }
    config.update(overrides)
    return config


def oidc_off(s: Settings) -> dict[str, str]:
    return oidc_on(
        s,
        CURIE_OIDC_ISSUER="",
        CURIE_OIDC_AUDIENCE="",
        CURIE_OIDC_JWKS_URL="",
        CURIE_OIDC_REDIRECT_URI="",
    )


class Stack:
    def __init__(self, s: Settings) -> None:
        self.s = s

    def _env(self, oidc: dict[str, str]) -> dict[str, str]:
        env = dict(os.environ)
        env.update(
            {
                "CURIE_BASE_TAG": self.s.image_tag,
                "CURIE_UI_IMAGE": self.s.ui_image,
                "CURIE_LOCAL_API_KEY": API_KEY,
                "OIDC_E2E_API_PORT": str(self.s.api_port),
                "OIDC_E2E_UI_PORT": str(self.s.ui_port),
                "OIDC_E2E_DEX_PORT": str(self.s.dex_port),
                "OIDC_E2E_DEX_IMAGE": DEX_IMAGE,
                "OIDC_E2E_DEX_CONFIG": str(self.s.dex_config),
                # The worker is off; keep compose from resolving an endpoint
                # that only the full profile would serve.
                "OTEL_EXPORTER_OTLP_ENDPOINT": "",
            }
        )
        env.update(oidc)
        return env

    def compose(
        self, *args: str, oidc: dict[str, str], capture: bool = False
    ) -> subprocess.CompletedProcess[str]:
        cmd = [
            "docker",
            "compose",
            "-p",
            self.s.project,
            "--profile",
            "core",
            "-f",
            str(COMPOSE_DEV),
            "-f",
            str(COMPOSE_OVERLAY),
            *args,
        ]
        return subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            env=self._env(oidc),
            check=True,
            text=True,
            capture_output=capture,
        )

    def up(self, oidc: dict[str, str], *services: str) -> None:
        self.compose("up", "-d", "--wait", *services, oidc=oidc)
        wait_for(f"{self.s.api}/health", 200, timeout=120)

    def down(self) -> None:
        self.compose("down", "-v", "--remove-orphans", oidc=oidc_off(self.s))

    def sql(self, query: str) -> str:
        out = self.compose(
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "postgres",
            "-tAc",
            query,
            oidc=oidc_off(self.s),
            capture=True,
        )
        return out.stdout.strip()


def wait_for(url: str, status: int, *, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        try:
            got = request("GET", url).status
            if got == status:
                return
            last = str(got)
        except OSError as err:
            last = str(err)
        time.sleep(1)
    raise CheckFailed(f"{url} never answered {status} (last: {last})")


# --- the browser half of a login -----------------------------------------------


@dataclass
class Callback:
    """A callback Dex sent the browser back with, captured before replay."""

    url: str
    state: str
    state_cookie: str


def begin_login(s: Settings, connector: str) -> Callback:
    """Start a login through the console proxy and drive Dex up to the callback."""

    started = request("GET", f"{s.ui}/api/console/oidc/login")
    if started.status != 302:
        raise CheckFailed(f"login start answered {started.status}: {started.body[:200]}")
    state_cookie = started.set_cookies()[STATE_COOKIE].value
    state = urllib.parse.parse_qs(urllib.parse.urlsplit(started.location).query)["state"][0]

    dex_cookies: dict[str, str] = {}

    def dex(method: str, url: str, form: dict[str, str] | None = None) -> Response:
        headers = (
            {"Cookie": "; ".join(f"{k}={v}" for k, v in dex_cookies.items())} if dex_cookies else {}
        )
        resp = request(method, url, headers=headers, form=form)
        for name, morsel in resp.set_cookies().items():
            dex_cookies[name] = morsel.value
        return resp

    current = started.location
    resp = dex("GET", current)
    # Dex lists one link per connector when more than one is configured.
    chooser = re.search(rf'href="(/dex/auth/{connector}[^"]*)"', resp.body)
    if chooser:
        current = urllib.parse.urljoin(s.dex_origin, html.unescape(chooser.group(1)))
        resp = dex("GET", current)
    for _ in range(10):
        if resp.status in (301, 302, 303, 307):
            target = urllib.parse.urljoin(current, resp.location)
            if target.startswith(s.redirect_uri):
                return Callback(url=target, state=state, state_cookie=state_cookie)
            current = target
            resp = dex("GET", current)
        elif resp.status == 200 and 'name="password"' in resp.body:
            resp = dex("POST", current, {"login": USER_EMAIL, "password": USER_PASSWORD})
        else:
            break
    raise CheckFailed(f"Dex never redirected to the callback (last {resp.status} at {current})")


def finish(callback: Callback, *, with_state_cookie: bool = True) -> Response:
    headers = cookie(STATE_COOKIE, callback.state_cookie) if with_state_cookie else {}
    return request("GET", callback.url, headers=headers)


def session_token(resp: Response) -> str:
    morsel = resp.set_cookies().get(SESSION_COOKIE)
    if resp.status != 303 or morsel is None or not morsel.value:
        raise CheckFailed(f"callback did not mint a session: {resp.status} {resp.body[:200]}")
    return str(morsel.value)


def cookie_flags(raw: str) -> set[str]:
    return {part.strip().split("=", 1)[0].lower() for part in raw.split(";")[1:]}


def cookie_attr(raw: str, name: str) -> str:
    for part in raw.split(";")[1:]:
        key, _, value = part.strip().partition("=")
        if key.lower() == name:
            return value
    return ""


# --- phases --------------------------------------------------------------------


def phase_default(s: Settings, stack: Stack, ev: Evidence) -> str:
    expected_head = json.loads(
        (REPO_ROOT / "apps/api/src/curie_api/schema_compat.json").read_text()
    )["schema_head"]
    head = stack.sql("SELECT version_num FROM curie.alembic_version")
    ev.check(
        "schema is at this checkout's head", head == expected_head, f"{head} vs {expected_head}"
    )

    started = request("GET", f"{s.ui}/api/console/oidc/login")
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(started.location).query)
    ev.check(
        "login start through the proxy redirects to Dex",
        started.status == 302 and started.location.startswith(f"{s.issuer}/auth"),
        started.location,
    )
    ev.check(
        "authorize request carries PKCE S256, nonce, state and the proxy redirect",
        query.get("code_challenge_method") == ["S256"]
        and len(query.get("code_challenge", [""])[0]) >= 43
        and len(query.get("nonce", [""])[0]) >= 32
        and len(query.get("state", [""])[0]) >= 32
        and query.get("redirect_uri") == [s.redirect_uri]
        and query.get("client_id") == [CLIENT_ID],
        {k: v for k, v in query.items() if k not in ("code_challenge", "nonce", "state")},
    )
    raw_state = started.raw_set_cookie(STATE_COOKIE)
    ev.check(
        "state cookie is __Host- with Secure, HttpOnly, SameSite=Lax, Path=/",
        {"secure", "httponly"} <= cookie_flags(raw_state)
        and cookie_attr(raw_state, "samesite").lower() == "lax"
        and cookie_attr(raw_state, "path") == "/"
        and "domain" not in cookie_flags(raw_state),
        raw_state.split(";", 1)[-1],
    )

    done = finish(begin_login(s, "local"))
    token = session_token(done)
    raw_session = done.raw_set_cookie(SESSION_COOKIE)
    ev.check("password login through Dex returns home (303 /)", done.location == "/", done.location)
    ev.check(
        "session cookie is __Host- with Secure, HttpOnly, SameSite=Strict, Path=/",
        {"secure", "httponly"} <= cookie_flags(raw_session)
        and cookie_attr(raw_session, "samesite").lower() == "strict"
        and cookie_attr(raw_session, "path") == "/"
        and "domain" not in cookie_flags(raw_session),
        raw_session.split(";", 1)[-1],
    )
    state_cleared = done.set_cookies().get(STATE_COOKIE)
    ev.check(
        "the callback clears the state cookie",
        state_cleared is not None
        and (state_cleared.value in ("", '""') or state_cleared["max-age"] == "0"),
    )

    me = request("GET", f"{s.ui}/api/console/principal", headers=cookie(SESSION_COOKIE, token))
    ev.check(
        "the session resolves its principal through the proxy",
        me.status == 200
        and me.json().get("email") == USER_EMAIL
        and me.json().get("tenant_id") == DEFAULT_TENANT_ID,
        me.body[:200],
    )
    row = stack.sql(
        "SELECT idp_issuer || '|' || type || '|' || status FROM curie.principals "
        f"WHERE email = '{USER_EMAIL}'"
    )
    ev.check("the principal row carries the issuer", row == f"{s.issuer}|human|active", row)
    bound = stack.sql(
        "SELECT count(*) FROM curie.console_sessions "
        "WHERE principal_id IS NOT NULL AND subject IS NULL AND revoked_at IS NULL"
    )
    ev.check("the session row binds the principal and no subject", bound == "1", bound)

    second = session_token(finish(begin_login(s, "local")))
    principals = stack.sql("SELECT count(*) FROM curie.principals")
    ev.check(
        "a second login reuses the principal", principals == "1" and second != token, principals
    )

    open_attempts = (
        "SELECT count(*) FROM curie.oidc_login_attempts "
        "WHERE consumed_at IS NULL AND expires_at > now()"
    )
    baseline = int(stack.sql(open_attempts))
    unused = begin_login(s, "local")
    refused = finish(unused, with_state_cookie=False)
    ev.check(
        "a callback without its state cookie is refused",
        refused.status == 401 and refused.json().get("detail") == OIDC_REFUSED,
        refused.body[:200],
    )
    still_open = int(stack.sql(open_attempts))
    ev.check(
        "that refusal leaves the attempt unspent",
        still_open == baseline + 1,
        f"{baseline} -> {still_open}",
    )
    completed = finish(unused)
    ev.check(
        "the same callback then completes with the cookie",
        completed.status == 303 and int(stack.sql(open_attempts)) == baseline,
    )
    replay = finish(unused)
    ev.check(
        "a replay with the cookie is refused (single use)",
        replay.status == 401 and replay.json().get("detail") == OIDC_REFUSED,
        replay.body[:200],
    )

    victim = session_token(finish(begin_login(s, "local")))
    for origin in ("https://evil.example", None):
        headers = cookie(SESSION_COOKIE, victim)
        if origin:
            headers["Origin"] = origin
        out = request("POST", f"{s.ui}/api/console/logout", headers=headers)
        still = request(
            "GET", f"{s.ui}/api/console/principal", headers=cookie(SESSION_COOKIE, victim)
        )
        ev.check(
            f"logout with {'a foreign' if origin else 'no'} Origin is 403 and leaves the session",
            out.status == 403 and still.status == 200,
            f"{out.status}/{still.status}",
        )
    out = request(
        "POST",
        f"{s.ui}/api/console/logout",
        headers={**cookie(SESSION_COOKIE, victim), "Origin": s.ui},
    )
    cleared = out.set_cookies().get(SESSION_COOKIE)
    gone = request("GET", f"{s.ui}/api/console/principal", headers=cookie(SESSION_COOKIE, victim))
    ev.check(
        "logout with a matching Origin revokes and clears the cookie",
        out.status == 204 and cleared is not None and gone.status == 401,
        f"{out.status}/{gone.status}",
    )

    legacy = request(
        "GET", f"{s.api}/console/principal", headers=cookie(LEGACY_SESSION_COOKIE, token)
    )
    ev.check("the retired cookie name is refused", legacy.status == 401, legacy.status)
    machine = request("GET", f"{s.api}/agents", headers=cookie(SESSION_COOKIE, token))
    control = request("GET", f"{s.api}/agents", headers={"X-API-Key": API_KEY})
    ev.check(
        "a console session is not a platform credential",
        machine.status == 401 and control.status == 200,
        f"{machine.status}/{control.status}",
    )
    return token


def phase_required_claims(s: Settings, stack: Stack, ev: Evidence) -> None:
    stack.up(
        oidc_on(
            s,
            CURIE_OIDC_SCOPES="openid email profile groups",
            CURIE_OIDC_REQUIRED_CLAIMS='{"groups":"authors"}',
        ),
        "curie-api",
        "dex",
    )
    admitted = finish(begin_login(s, "mock"))
    session_token(admitted)
    ev.check(
        "Dex's groups claim admits the mock user",
        stack.sql(f"SELECT count(*) FROM curie.principals WHERE email = '{MOCK_EMAIL}'") == "1",
    )
    before = stack.sql("SELECT count(*) FROM curie.principals")
    refused = finish(begin_login(s, "local"))
    after = stack.sql("SELECT count(*) FROM curie.principals")
    ev.check(
        "a user without the group is refused and nothing is written",
        refused.status == 401 and before == after,
        f"{refused.status} {before}->{after}",
    )


def phase_issuer_binding(s: Settings, stack: Stack, ev: Evidence, token: str) -> None:
    stack.up(oidc_on(s, CURIE_OIDC_ISSUER="http://other.invalid/dex"), "curie-api", "dex")
    switched = request("GET", f"{s.api}/console/principal", headers=cookie(SESSION_COOKIE, token))
    ev.check("a session is refused while another issuer is configured", switched.status == 401)
    stack.up(oidc_on(s), "curie-api", "dex")
    restored = request("GET", f"{s.api}/console/principal", headers=cookie(SESSION_COOKIE, token))
    revoked = stack.sql("SELECT count(*) FROM curie.console_sessions WHERE revoked_at IS NOT NULL")
    ev.check(
        "restoring the issuer readmits the same session", restored.status == 200, restored.status
    )
    ev.check(
        "the issuer switch revoked nothing", revoked == "1", f"{revoked} revoked (1 from logout)"
    )


def phase_rate_limit(s: Settings, ev: Evidence) -> None:
    # The direct API port is a different peer from the proxy, so this spends
    # nothing the remaining checks need.
    statuses: list[int] = []
    for _ in range(LOGIN_BUDGET + 5):
        resp = request("GET", f"{s.api}/console/oidc/login")
        statuses.append(resp.status)
        if resp.status == 429:
            break
    ev.check(
        "login starts from one peer reach 429 within the budget",
        statuses[-1] == 429
        and statuses.count(302) <= LOGIN_BUDGET
        and int(resp.headers.get("Retry-After", "0")) > 0,
        f"{statuses.count(302)} x 302 then {statuses[-1]}",
    )


def phase_oidc_off(s: Settings, stack: Stack, ev: Evidence) -> None:
    stack.up(oidc_off(s), "curie-api", "dex")
    for base in (s.api, f"{s.ui}/api"):
        for path in ("/console/oidc/login", "/console/oidc/callback?code=x&state=y"):
            resp = request("GET", base + path)
            ev.check(
                f"OIDC off: {base}{path.split('?')[0]} is 404", resp.status == 404, resp.status
            )

    platform = {"X-API-Key": API_KEY}
    suffix = uuid.uuid4().hex[:8]
    channel = f"C0OIDC{suffix.upper()}"
    agent = request(
        "POST",
        f"{s.api}/agents",
        headers=platform,
        json_body={
            "name": f"oidc-e2e-{suffix}",
            "channel": {"kind": "slack", "address": channel},
            "approval_routes": {
                "operators": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                    "approvers": {"users": [APPROVER]},
                }
            },
        },
    )
    ev.check(
        "an agent with a console-resolvable route is created", agent.status == 201, agent.body[:200]
    )
    approval = request(
        "POST",
        f"{s.api}/approvals",
        headers=platform,
        json_body={
            "conversation_id": f"th-{suffix}",
            "author": "U0EXAMPLE2",
            "summary": "oidc-e2e: approve through a login-code console session",
            "reply_kind": "slack",
            "reply_channel": channel,
            "reply_placeholder": "p-1",
            "dedupe_key": suffix,
            "agent_id": agent.json()["id"],
            "route": "operators",
            "card_channel": "C0EXAMPLE1",
            "gate_kind": "policy",
        },
    )
    ev.check(
        "a pending approval exists",
        approval.status == 201 and approval.json().get("status") == "pending",
        approval.body[:200],
    )
    approval_id = approval.json()["id"]

    code, minted_by = mint_login_code(s, agent.json()["name"])
    exchanged = request("POST", f"{s.ui}/api/console/session", json_body={"code": code})
    token = exchanged.set_cookies().get(SESSION_COOKIE)
    ev.check(
        f"the login code ({minted_by}) is exchanged through the proxy",
        exchanged.status == 200 and token is not None,
        exchanged.body[:200],
    )
    assert token is not None  # checked above; ev.check raises otherwise
    resolved = request(
        "POST",
        f"{s.ui}/api/approvals/{approval_id}/resolve",
        headers={**cookie(SESSION_COOKIE, token.value), "Origin": s.ui},
        json_body={"decision": "approved"},
    )
    record = request("GET", f"{s.api}/approvals/{approval_id}", headers=platform).json()
    ev.check(
        "the console session resolves the approval",
        resolved.status == 200
        and record.get("status") == "approved"
        and record.get("resolved_by") == APPROVER,
        {"status": resolved.status, **record},
    )


def mint_login_code(s: Settings, agent: str) -> tuple[str, str]:
    if s.curie_bin:
        out = subprocess.run(
            [
                s.curie_bin,
                "local",
                "approvals",
                agent,
                "--mint-console-login-code",
                APPROVER,
                "--api-url",
                s.api,
                "--api-key",
                API_KEY,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout.strip().splitlines()[-1].strip(), "operator CLI"
    resp = request(
        "POST",
        f"{s.api}/console/login-codes",
        headers={"X-API-Key": API_KEY},
        json_body={"subject": APPROVER},
    )
    return resp.json()["code"], "API; no curie binary found"


# --- orchestration -------------------------------------------------------------


def port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def image_present(ref: str) -> bool:
    return subprocess.run(["docker", "image", "inspect", ref], capture_output=True).returncode == 0


def ensure_images(s: Settings, build: bool) -> None:
    for ref, dockerfile in (
        (f"ghcr.io/curie-eng/curie-api:{s.image_tag}", "apps/api/Dockerfile"),
        (s.ui_image, "apps/ui/Dockerfile"),
    ):
        if build or not image_present(ref):
            print(f"building {ref} from {dockerfile}", flush=True)
            subprocess.run(
                ["docker", "build", "-f", dockerfile, "-t", ref, "."], cwd=REPO_ROOT, check=True
            )


def preflight(s: Settings) -> None:
    running = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={s.project}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if running:
        sys.exit(
            f"compose project {s.project} already has containers; remove them with "
            f"`docker compose -p {s.project} down -v` or set CURIE_OIDC_E2E_PROJECT."
        )
    busy = [p for p in (s.api_port, s.ui_port, s.dex_port) if not port_free(p)]
    if busy:
        sys.exit(f"ports in use: {busy}; set CURIE_OIDC_E2E_API_PORT/UI_PORT/DEX_PORT.")


def candidate() -> str:
    out = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True
    )
    return out.stdout.strip() or "unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="curie dev oidc-e2e",
        description="Generic OIDC console login, end to end against a real Dex.",
    )
    parser.add_argument(
        "--build", action="store_true", help="build the API and UI images even when present"
    )
    parser.add_argument(
        "--keep", action="store_true", help="leave the stack running after the checks"
    )
    parser.add_argument(
        "--evidence",
        type=Path,
        default=Path(tempfile.gettempdir()) / "oidc-e2e-evidence.json",
        help="where to write the JSON evidence",
    )
    args = parser.parse_args(argv)

    if shutil.which("docker") is None:
        sys.exit("docker is required")
    tag = os.environ.get("CURIE_OIDC_E2E_IMAGE_TAG", "oidc-e2e")
    curie_bin = os.environ.get("CURIE_BIN") or shutil.which("curie")
    workdir = Path(tempfile.mkdtemp(prefix="oidc-e2e-"))
    s = Settings(
        project=os.environ.get("CURIE_OIDC_E2E_PROJECT", "curie-oidc-e2e"),
        api_port=int(os.environ.get("CURIE_OIDC_E2E_API_PORT", "28400")),
        ui_port=int(os.environ.get("CURIE_OIDC_E2E_UI_PORT", "28480")),
        dex_port=int(os.environ.get("CURIE_OIDC_E2E_DEX_PORT", "25556")),
        image_tag=tag,
        ui_image=os.environ.get("CURIE_OIDC_E2E_UI_IMAGE", f"ghcr.io/curie-eng/curie-ui:{tag}"),
        curie_bin=curie_bin,
        workdir=workdir,
    )
    preflight(s)
    s.dex_config.write_text(
        Template(DEX_TEMPLATE.read_text()).substitute(
            issuer=s.issuer,
            dex_port=s.dex_port,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri=s.redirect_uri,
            user_email=USER_EMAIL,
        )
    )
    os.chmod(s.dex_config, 0o644)
    ensure_images(s, args.build)

    stack = Stack(s)
    ev = Evidence()
    result = "fail"
    try:
        stack.up(oidc_on(s))
        token = phase_default(s, stack, ev)
        phase_required_claims(s, stack, ev)
        phase_issuer_binding(s, stack, ev, token)
        phase_rate_limit(s, ev)
        phase_oidc_off(s, stack, ev)
        result = "pass"
    except CheckFailed as err:
        print(f"oidc-e2e failed: {err}", file=sys.stderr)
    except subprocess.CalledProcessError as err:
        ev.checks.append({"check": "command", "ok": False, "detail": f"{err.cmd}: {err.stderr}"})
        print(
            f"oidc-e2e failed: {err.cmd} exited {err.returncode}\n{err.stderr or ''}",
            file=sys.stderr,
        )
    finally:
        args.evidence.write_text(
            json.dumps(
                {
                    "candidate": candidate(),
                    "result": result,
                    "project": s.project,
                    "checks": ev.checks,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"evidence: {args.evidence}", flush=True)
        if args.keep:
            print(f"stack left up as compose project {s.project} (--keep)", flush=True)
        else:
            stack.down()
            shutil.rmtree(workdir, ignore_errors=True)
    print(f"oidc-e2e: {result} ({sum(c['ok'] for c in ev.checks)} checks passed)")
    return 0 if result == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
