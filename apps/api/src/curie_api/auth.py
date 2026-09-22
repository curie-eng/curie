"""Single-API-key authentication, plus the human principal session guard.

MVP auth is one shared key delivered in the `X-API-Key` header and compared
against Settings.api_key. J1 replaces this with GitHub-App-scoped identities.

`require_principal_session` (#2908, ADR 0155) is the first human credential
dependency here. It is a separate dependency on purpose: the machine-credential
dependencies above it never learn to accept a session, so no route guarded by
one of them widens because a person logged in.
"""

import hmac
from typing import Annotated

from fastapi import Cookie, Header, HTTPException, status

from . import crud
from .config import get_settings
from .deps import SessionDep
from .models import Principal

API_KEY_HEADER = "X-API-Key"
#: The console session cookie (ADR-0083). Defined here rather than beside its
#: approval consumer so this module can read it without an import cycle.
CONSOLE_SESSION_COOKIE = "curie_console_session"


def verify_platform_key(x_api_key: str | None) -> bool:
    """True when the header carries the shared platform API key (constant-time).

    The single place that defines what 'the platform key' means, shared by
    require_api_key (raise on fail) and the state router's require_state_access
    (fall through to the scoped-token check)."""
    if x_api_key is None:
        return False
    return hmac.compare_digest(x_api_key, get_settings().api_key)


async def require_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    if not verify_platform_key(x_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
        )


async def require_platform_key(
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    """Authenticate an immutable platform-administration boundary.

    Unlike ``require_api_key``, this dependency must never grow support for a
    console session or another human credential.  Principal and console-code
    mint routes use it so a future widening of ordinary API authentication
    cannot let a logged-in browser mint an arbitrary operator identity.
    """

    if not verify_platform_key(x_api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid platform API key",
        )


def verify_internal_worker_token(value: str | None) -> bool:
    """Constant-time check for the credential-redemption trust boundary."""

    if value is None:
        return False
    expected = get_settings().internal_worker_token
    return bool(expected) and hmac.compare_digest(value, expected)


async def require_internal_worker_token(
    x_curie_worker_token: Annotated[str | None, Header(alias="X-Curie-Worker-Token")] = None,
) -> None:
    if not verify_internal_worker_token(x_curie_worker_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid internal worker token",
            headers={"Cache-Control": "no-store"},
        )


async def require_internal_adapter_secret(
    x_curie_adapter_secret: Annotated[str | None, Header(alias="X-Curie-Adapter-Secret")] = None,
) -> None:
    """Authenticate the built-in reply relay on its adapter-shaped header.

    The credential value is the internal worker token, but the header is
    deliberately distinct from both the public platform key and credential
    redemption's worker header.  A caller holding only either public key or a
    channel-scoped token therefore cannot write synthetic replies.
    """

    if not verify_internal_worker_token(x_curie_adapter_secret):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid internal adapter secret",
            headers={"Cache-Control": "no-store"},
        )


async def require_principal_session(
    session: SessionDep,
    console_session: Annotated[str | None, Cookie(alias=CONSOLE_SESSION_COOKIE)] = None,
) -> Principal:
    """Authenticate a person by their OIDC console session; return the principal.

    Accepts only a live console session bound to a principal that is active in
    an active tenant. Re-checked on every request, so revoking the session,
    disabling the principal or suspending the tenant ends access at once rather
    than at session expiry. A login-code session (no principal) and the
    platform key are both refused: neither identifies a person.

    Every refusal is the same 401, so the response does not tell a caller
    whether the cookie was unknown, expired, or valid for someone disabled.
    """

    principal = await crud.live_principal_session(session, console_session or "")
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing, invalid, or expired principal session",
            headers={"Cache-Control": "no-store"},
        )
    return principal
