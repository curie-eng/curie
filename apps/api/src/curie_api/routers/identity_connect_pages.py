"""Server-rendered pages for the account-connect capability.

Pure rendering: no database, network or request access. The pages stay
server-rendered because the capability token, the `__Host-` cookies, the CSRF
binding and the strict-origin referrer policy all assume one private document
at `/identity/connect/<token>`. They reuse the Console design tokens
(`apps/ui/src/tokens.ts`), dark only, as the Console is.

The stylesheet is one inline `<style>` element allowed by its own hash in the
Content-Security-Policy, so the policy needs no `'unsafe-inline'` and loads
nothing from any origin. The hash is computed from the same constant that is
emitted, so the two cannot drift.
"""

import base64
import hashlib
from dataclasses import dataclass
from html import escape

#: Console design tokens, copied by name from `C` in `apps/ui/src/tokens.ts`.
#: A test compares every entry against that file.
TOKENS = {
    "page": "#121212",
    "card": "#1f1f1f",
    "border": "#2e2e2e",
    "borderStrong": "#363636",
    "text": "#fafafa",
    "text2": "#b4b4b4",
    "muted": "#898989",
    "brand": "#3ecf8e",
    "link": "#00c573",
    "warn": "#BF8700",
    "sans": 'system-ui, -apple-system, "Segoe UI", Helvetica, Arial, sans-serif',
    "mono": '"SF Mono", Menlo, Monaco, Consolas, "Liberation Mono", monospace',
}
#: The primary variant in `apps/ui/src/primitives/Button.tsx`.
BUTTON = {"color": "#08120c", "hover": "#37b87e"}
#: `R` in `apps/ui/src/tokens.ts`.
RADIUS = {"btn": 7, "card": 14}

_T = TOKENS
STYLE = (
    ":root{color-scheme:dark}"
    "*{box-sizing:border-box}"
    f"html,body{{margin:0;background:{_T['page']};color:{_T['text']}}}"
    f"body{{font-family:{_T['sans']};font-size:13px;line-height:1.5}}"
    "main{min-height:100vh;display:flex;align-items:center;justify-content:center;"
    "padding:20px}"
    ".frame{width:min(480px,100%);min-width:0}"
    ".wordmark{display:flex;align-items:center;gap:8px;padding:4px 8px 16px;"
    "font-size:16px;font-weight:600;letter-spacing:-0.3px}"
    ".dot{display:inline-block;width:9px;height:9px;border-radius:50%;"
    f"background:{_T['brand']};flex-shrink:0}}"
    f".card{{background:{_T['card']};border:1px solid {_T['border']};"
    f"border-radius:{RADIUS['card']}px;padding:20px;overflow-wrap:anywhere}}"
    f"h1{{font-size:19px;font-weight:500;margin:0 0 6px;color:{_T['text']}}}"
    f"p{{margin:0 0 12px;color:{_T['text2']}}}"
    f".account{{color:{_T['text']}}}"
    f".note{{margin:16px 0 0;font-size:12.5px;color:{_T['muted']}}}"
    "dl{display:grid;grid-template-columns:max-content minmax(0,1fr);gap:8px 16px;"
    "margin:12px 0 16px}"
    f"dt{{color:{_T['muted']}}}"
    f"dd{{margin:0;min-width:0;color:{_T['text']};overflow-wrap:anywhere}}"
    f"code{{font-family:{_T['mono']};font-size:12px;color:{_T['text2']};"
    "overflow-wrap:anywhere}"
    f".warning{{border:1px solid {_T['borderStrong']};border-left:3px solid {_T['warn']};"
    f"border-radius:6px;padding:8px 10px;font-size:12.5px;color:{_T['text2']}}}"
    "form,.actions{margin:16px 0 0}"
    "button,.actions a{display:inline-flex;align-items:center;gap:6px;"
    f"background:{_T['brand']};color:{BUTTON['color']};border:1px solid {_T['brand']};"
    f"padding:8px 14px;border-radius:{RADIUS['btn']}px;font:inherit;font-size:13px;"
    "font-weight:500;text-decoration:none;cursor:pointer;transition:background .12s}"
    f"button:hover,.actions a:hover{{background:{BUTTON['hover']}}}"
    f"a{{color:{_T['link']}}}"
    f"button:focus-visible,a:focus-visible{{outline:2px solid {_T['brand']};"
    "outline-offset:1px}"
)
#: CSP source expression for exactly the text inside the emitted `<style>`.
STYLE_HASH = "sha256-" + base64.b64encode(hashlib.sha256(STYLE.encode()).digest()).decode()
STYLE_SRC = f"'{STYLE_HASH}'"

ORIGINAL_REQUEST_NOTE = "Your original request will not run. After connecting, send it again."
PERSONAL_WARNING = (
    "Confirm only if this is your own Slack account. "
    "Anyone using it will start runs on your Claude plan."
)


@dataclass(frozen=True)
class SlackLabels:
    """Display labels captured for a personal-mode Slack account."""

    display_name: str | None
    handle: str | None
    workspace_name: str | None


@dataclass(frozen=True)
class SignIn:
    """No browser principal yet: link to the organization sign-in."""

    login_path: str


@dataclass(frozen=True)
class ConfirmForm:
    """A signed-in principal: the explicit confirmation form."""

    who: str
    target: str
    csrf: str


#: Refusal pages by HTTP status. Static text only; deliberately opaque about
#: why a link was refused (expired, used and wrong sender read the same).
REFUSALS: dict[int, tuple[str, tuple[str, ...]]] = {
    400: (
        "This link can't be used",
        (
            "It may have expired, been used already, or belong to a different account.",
            "Return to Slack and send your request again to get a new link.",
        ),
    ),
    401: (
        "Sign in required",
        (
            "Your organization sign-in did not carry over to this step.",
            "Open the link from Slack again and sign in.",
        ),
    ),
    403: (
        "Request not accepted",
        (
            "Confirm only from the page the link opened, not from another site.",
            "Open the link from Slack again.",
        ),
    ),
}


def _document(title: str, body: str) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="dark">'
        f"<title>{escape(title, quote=False)}</title><style>{STYLE}</style></head>"
        '<body><main><div class="frame">'
        '<div class="wordmark"><span class="dot" aria-hidden="true"></span>Curie</div>'
        f'<div class="card">{body}</div>'
        "</div></main></body></html>"
    )


def _slack_account(user_id: str, workspace_id: str, labels: SlackLabels) -> str:
    names = " ".join(
        f"<bdi>{escape(label)}</bdi>"
        for label in (labels.display_name, f"@{labels.handle}" if labels.handle else None)
        if label
    )
    workspace = (
        f"<bdi>{escape(labels.workspace_name)}</bdi>"
        if labels.workspace_name
        else "Name unavailable"
    )
    return (
        "<dl>"
        f"<dt>Slack user</dt><dd>{names or 'Name unavailable'}</dd>"
        f'<dt>User ID</dt><dd><code dir="ltr">{escape(user_id)}</code></dd>'
        f"<dt>Workspace</dt><dd>{workspace}</dd>"
        f'<dt>Workspace ID</dt><dd><code dir="ltr">{escape(workspace_id)}</code></dd>'
        "</dl>"
    )


def _action(action: SignIn | ConfirmForm) -> str:
    if isinstance(action, SignIn):
        login = escape(action.login_path, quote=True)
        return f'<p class="actions"><a href="{login}">Sign in with your organization</a></p>'
    target = escape(action.target, quote=True)
    csrf = escape(action.csrf, quote=True)
    return (
        f"<p>Connect this account to {escape(action.who)}?</p>"
        f'<form method="post" action="{target}/confirm">'
        f'<input type="hidden" name="csrf" value="{csrf}">'
        '<button type="submit">Confirm account connection</button></form>'
    )


def render_connect_page(
    *,
    provider: str,
    user_id: str,
    workspace_id: str,
    slack: SlackLabels | None,
    action: SignIn | ConfirmForm,
) -> str:
    """The connect page. `slack` is set only for a personal-mode Slack account."""
    if slack is not None:
        account = _slack_account(user_id, workspace_id, slack)
        warning = f'<p class="warning">{PERSONAL_WARNING}</p>'
    else:
        sentence = f"{provider} user {user_id} in workspace {workspace_id}"
        account = f'<p class="account">{escape(sentence)}</p>'
        warning = ""
    return _document(
        "Connect account",
        f"<h1>Connect account</h1>{account}{warning}{_action(action)}"
        f'<p class="note">{ORIGINAL_REQUEST_NOTE}</p>',
    )


def render_connected_page() -> str:
    """The confirmation result outside personal mode (personal mode redirects)."""
    return _document(
        "Account connected",
        "<h1>Account connected</h1><p>Return to your chat and send your request again.</p>",
    )


def render_refusal_page(status: int) -> str:
    """The refusal page for a 400, 401 or 403; KeyError for any other status."""
    title, lines = REFUSALS[status]
    body = "".join(f"<p>{escape(line, quote=False)}</p>" for line in lines)
    return _document(title, f"<h1>{escape(title, quote=False)}</h1>{body}")
