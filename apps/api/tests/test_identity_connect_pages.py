"""Render-level checks for the account-connect pages. No database."""

import base64
import hashlib
import re
from html import escape
from pathlib import Path

import pytest
from curie_api.routers.identity_connect import PRIVATE_HEADERS
from curie_api.routers.identity_connect_pages import (
    BUTTON,
    RADIUS,
    REFUSALS,
    STYLE,
    STYLE_HASH,
    TOKENS,
    ConfirmForm,
    SignIn,
    SlackLabels,
    render_connect_page,
    render_connected_page,
    render_refusal_page,
)

UI_SRC = Path(__file__).resolve().parents[3] / "apps" / "ui" / "src"
HOSTILE = '<script>alert("x")</script>\' & "q"'
WARNING = (
    "Confirm only if this is your own Slack account. "
    "Anyone using it will start runs on your Claude plan."
)
NOTE = "Your original request will not run. After connecting, send it again."


def connect(*, personal: bool = True, signed_in: bool = True, **values: str) -> str:
    v = {
        "provider": "slack",
        "user_id": "UALICE001",
        "workspace_id": "TEXAMPLE1",
        "display_name": "Alice",
        "handle": "alice",
        "workspace_name": "Example Workspace",
        "who": "Alice Example",
        "login": "/console/oidc/login",
        "target": "/identity/connect/example-token",
        "csrf": "c" * 43,
        **values,
    }
    action: SignIn | ConfirmForm = (
        ConfirmForm(who=v["who"], target=v["target"], csrf=v["csrf"])
        if signed_in
        else SignIn(v["login"])
    )
    return render_connect_page(
        provider=v["provider"],
        user_id=v["user_id"],
        workspace_id=v["workspace_id"],
        slack=SlackLabels(v["display_name"], v["handle"], v["workspace_name"])
        if personal
        else None,
        action=action,
    )


def every_page() -> dict[str, str]:
    pages = {
        f"connect-{'personal' if personal else 'directory'}-{'confirm' if signed else 'sign-in'}": (
            connect(personal=personal, signed_in=signed)
        )
        for personal in (True, False)
        for signed in (True, False)
    }
    pages["connected"] = render_connected_page()
    for status in REFUSALS:
        pages[f"refusal-{status}"] = render_refusal_page(status)
    return pages


def console_tokens() -> dict[str, str]:
    source = (UI_SRC / "tokens.ts").read_text()
    block = re.search(r"export const C = \{(.*?)\} as const;", source, re.S)
    assert block is not None
    return {
        name: double if double else single
        for name, double, single in re.findall(
            r"(\w+):\s*(?:\"([^\"]*)\"|'([^']*)'),", block.group(1)
        )
    }


def test_csp_hash_is_the_emitted_style_and_the_only_style_source() -> None:
    digest = base64.b64encode(hashlib.sha256(STYLE.encode()).digest()).decode()
    assert STYLE_HASH == f"sha256-{digest}"
    policy = PRIVATE_HEADERS["Content-Security-Policy"]
    directives = dict(part.strip().split(" ", 1) for part in policy.split(";"))
    assert directives == {
        "default-src": "'none'",
        "style-src": f"'{STYLE_HASH}'",
        "form-action": "'self'",
        "frame-ancestors": "'none'",
        "base-uri": "'none'",
    }
    assert PRIVATE_HEADERS["Cache-Control"] == "private, no-store"
    assert PRIVATE_HEADERS["Referrer-Policy"] == "strict-origin"
    assert PRIVATE_HEADERS["X-Content-Type-Options"] == "nosniff"
    for name, page in every_page().items():
        assert re.findall(r"<style>(.*?)</style>", page, re.S) == [STYLE], name


def test_pages_load_nothing_external() -> None:
    for banned in ("url(", "@import", "@font-face", "http:", "https:", "expression("):
        assert banned not in STYLE.lower()
    for name, page in every_page().items():
        lowered = page.lower()
        for banned in ("<script", "<link", "<img", "<iframe", "http:", "https:", "//"):
            assert banned not in lowered, (name, banned)


def test_style_uses_only_console_tokens() -> None:
    console = console_tokens()
    for name, value in TOKENS.items():
        assert console[name] == value, name
    button = (UI_SRC / "primitives" / "Button.tsx").read_text()
    primary = re.search(
        r'primary: \{ bg: C\.brand, color: "([^"]+)", bd: C\.brand, hbg: "([^"]+)"', button
    )
    assert primary is not None
    assert (BUTTON["color"], BUTTON["hover"]) == primary.groups()
    radii = (UI_SRC / "tokens.ts").read_text()
    assert re.search(rf"\bbtn: {RADIUS['btn']},", radii)
    assert re.search(rf"\bcard: {RADIUS['card']},", radii)
    allowed = {value.lower() for value in TOKENS.values() if value.startswith("#")}
    allowed |= {value.lower() for value in BUTTON.values()}
    used = {colour.lower() for colour in re.findall(r"#[0-9a-fA-F]{3,8}\b", STYLE)}
    assert used and used <= allowed
    assert TOKENS["sans"] in STYLE and TOKENS["mono"] in STYLE
    assert "color-scheme:dark" in STYLE
    assert "overflow-wrap:anywhere" in STYLE


def test_every_page_is_a_complete_document() -> None:
    for name, page in every_page().items():
        assert page.startswith('<!doctype html><html lang="en">'), name
        assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in page
        assert re.search(r"<title>[^<]+</title>", page), name
        assert page.count("<h1") == 1, name
        assert '<div class="wordmark">' in page and "Curie</div>" in page


@pytest.mark.parametrize(
    "field",
    [
        "display_name",
        "handle",
        "workspace_name",
        "user_id",
        "workspace_id",
        "who",
        "target",
        "csrf",
    ],
)
def test_personal_confirm_escapes_every_value(field: str) -> None:
    page = connect(**{field: HOSTILE})
    assert "<script" not in page and HOSTILE not in page
    assert escape(HOSTILE, quote=True) in page


@pytest.mark.parametrize("field", ["provider", "user_id", "workspace_id", "who", "target"])
def test_directory_confirm_escapes_every_value(field: str) -> None:
    page = connect(personal=False, **{field: HOSTILE})
    assert "<script" not in page and HOSTILE not in page
    assert escape(HOSTILE, quote=True) in page


@pytest.mark.parametrize("personal", [True, False])
def test_sign_in_escapes_the_login_target(personal: bool) -> None:
    page = connect(personal=personal, signed_in=False, login=HOSTILE)
    assert "<script" not in page and HOSTILE not in page
    assert f'<a href="{escape(HOSTILE, quote=True)}">Sign in with your organization</a>' in page


def test_personal_connect_keeps_account_markup_warning_and_note() -> None:
    page = connect(display_name="Alice <Example>", signed_in=False)
    assert "<bdi>Alice &lt;Example&gt;</bdi> <bdi>@alice</bdi>" in page
    assert '<dt>User ID</dt><dd><code dir="ltr">UALICE001</code></dd>' in page
    assert "<dd><bdi>Example Workspace</bdi></dd>" in page
    assert '<code dir="ltr">TEXAMPLE1</code>' in page
    assert f'<p class="warning">{WARNING}</p>' in page
    assert NOTE in page
    assert '<a href="/console/oidc/login">Sign in with your organization</a>' in page


def test_personal_connect_names_missing_labels() -> None:
    page = connect(display_name="", handle="", workspace_name="")
    assert "<dt>Slack user</dt><dd>Name unavailable</dd>" in page
    assert "<dt>Workspace</dt><dd>Name unavailable</dd>" in page


def test_confirm_form_markup_is_stable() -> None:
    page = connect(who="Alice Example")
    assert "<p>Connect this account to Alice Example?</p>" in page
    assert '<form method="post" action="/identity/connect/example-token/confirm">' in page
    assert f'name="csrf" value="{"c" * 43}"' in page
    assert '<button type="submit">Confirm account connection</button>' in page


def test_directory_connect_has_sentence_without_warning() -> None:
    page = connect(personal=False, provider="slack")
    assert "slack user UALICE001 in workspace TEXAMPLE1" in page
    assert "Confirm only if" not in page and 'class="warning"' not in page
    assert "<dl>" not in page
    assert NOTE in page


def test_connected_page_text() -> None:
    page = render_connected_page()
    assert "<title>Account connected</title>" in page
    assert "<h1>Account connected</h1>" in page
    assert "Return to your chat and send your request again." in page


@pytest.mark.parametrize(
    ("status", "title"),
    [(400, "This link can't be used"), (401, "Sign in required"), (403, "Request not accepted")],
)
def test_refusal_pages(status: int, title: str) -> None:
    page = render_refusal_page(status)
    assert f"<title>{title}</title>" in page and f"<h1>{title}</h1>" in page
    if status == 400:
        assert "expired, been used already, or belong to a different account" in page
        assert "send your request again" in page


def test_refusal_pages_cover_only_their_statuses() -> None:
    assert set(REFUSALS) == {400, 401, 403}
    with pytest.raises(KeyError):
        render_refusal_page(404)


@pytest.mark.parametrize(
    ("accept", "html"),
    [
        ("text/html,application/xhtml+xml,*/*;q=0.8", True),
        ("TEXT/HTML ; q=0.5", True),
        ("application/json, text/html;q=0.001", True),
        ("text/html;q=1", True),
        ("*/*", False),
        ("", False),
        ("application/json", False),
        ("text/html;q=0", False),
        ("text/html;q=0.0", False),
        ("text/html;q=1.1", False),
        ("text/html;q=inf", False),
        ("text/html;q=nan", False),
        ("text/html;q=-1", False),
        ("text/html;q=abc", False),
        ('application/json; profile="text/html"', False),
        ('application/json; profile="a, text/html, b"', False),
        ('application/json; profile="a; text/html"', False),
        ('application/json; profile="x", text/html', True),
    ],
)
def test_wants_html_requires_an_explicit_valid_text_html_range(accept: str, html: bool) -> None:
    from curie_api.routers.identity_connect import wants_html
    from starlette.requests import Request

    scope = {"type": "http", "headers": [(b"accept", accept.encode())]}
    assert wants_html(Request(scope)) is html
