"""Platform key first, then a live console session.

The platform key in the `X-API-Key` header is checked first, with no database
read. A live console session is accepted only after that check fails. J1
replaces this with GitHub-App-scoped identities.

`require_principal_session` (#2908, ADR 0155) is a separate, stricter
dependency: it accepts only a session bound to a principal (a person), never a
bare login-code session, and no route guarded by it widens through the
``require_api_key`` fallback above. But a principal-bound cookie IS one of the
live sessions ``require_api_key`` accepts, so both dependencies run the same
``_authorized_principal`` check on it: disabling the principal, suspending its
tenant, or repointing the configured issuer closes every route that cookie
reaches, not only the principal-only ones.
"""

import hmac
from typing import Annotated

from fastapi import Cookie, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .deps import SessionDep
from .models import Principal

API_KEY_HEADER = "X-API-Key"
#: The console session cookie (ADR-0083). Defined here rather than beside its
#: approval consumer so this module can read it without an import cycle.
CONSOLE_SESSION_COOKIE = "__Host-curie_console_session"


def verify_platform_key(x_api_key: str | None) -> bool:
    """True when the header carries the shared platform API key (constant-time).

    The single place that defines what 'the platform key' means, shared by
    require_api_key (raise on fail) and the state router's require_state_access
    (fall through to the scoped-token check)."""
    if x_api_key is None:
        return False
    return hmac.compare_digest(x_api_key, get_settings().api_key)


async def _authorized_principal(session: AsyncSession, token: str) -> Principal | None:
    """The principal ``token`` authenticates, if every check still holds.

    Shared by ``require_principal_session`` and ``require_api_key``'s
    principal-bound sessions, so a principal-bound cookie closes the same way
    everywhere: disabling the principal, suspending its tenant, or changing
    the configured issuer takes effect immediately on every route it reaches,
    not only ``/console/principal``.
    """
    from .crud import console as crud_console

    principal = await crud_console.live_principal_session(session, token)
    settings = get_settings()
    if (
        principal is None
        or not settings.oidc_enabled
        or principal.idp_issuer != settings.oidc_issuer
    ):
        return None
    return principal


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    if verify_platform_key(x_api_key):
        return
    token = request.cookies.get(CONSOLE_SESSION_COOKIE)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
        )
    from .approval_auth import enforce_console_cookie_origin

    enforce_console_cookie_origin(request)
    from .crud import console as crud_console

    async with request.app.state.sessionmaker() as session:
        row = await crud_console.live_console_session(session, token)
        if row is not None and row.principal_id is not None:
            # A principal-bound session is only as live as its principal: the
            # bare session row doesn't know the principal was disabled, its
            # tenant suspended, or the issuer changed out from under it.
            if await _authorized_principal(session, token) is None:
                row = None
    if row is not None:
        return
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

    The principal must also belong to the IdP configured now: with OIDC
    disabled, or with ``CURIE_OIDC_ISSUER`` pointed elsewhere, a session minted
    under the old issuer is refused. A comparison, never a revocation, so
    restoring the configuration readmits the same sessions. OIDC is the only
    source of principal sessions today; a future one (e.g. a SAML adapter via
    the service hook in #3003) must extend this check to its own issuer, or its
    sessions will be refused here.

    Every refusal is the same 401, so the response does not tell a caller
    whether the cookie was unknown, expired, or valid for someone disabled.
    """
    principal = await _authorized_principal(session, console_session or "")
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing, invalid, or expired principal session",
            headers={"Cache-Control": "no-store"},
        )
    return principal
