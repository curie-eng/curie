"""Platform key first, then a live console session.

The platform key in the `X-API-Key` header is checked first, with no database
read. A live console session is accepted only after that check fails. J1
replaces this with GitHub-App-scoped identities.

`require_principal_session` (#2908, ADR 0155) is a separate, stricter
dependency: it accepts only a session bound to a principal (a person), never a
bare login-code session, and no route guarded by it widens through the
``require_api_key`` fallback above. But a principal-bound cookie IS one of the
live sessions ``require_api_key`` accepts, so both dependencies run the same
``authorized_console_principal`` check on it: disabling the principal, suspending its
tenant, or repointing the configured issuer closes every route that cookie
reaches, not only the principal-only ones.
"""

import hmac
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Cookie, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .deps import SessionDep
from .models import ConsoleSession, Principal

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


async def authorized_console_principal(
    session: AsyncSession,
    token: str,
    *,
    live_row: ConsoleSession | None = None,
) -> Principal | None:
    """The principal ``token`` authenticates, if every check still holds.

    Shared by ``require_principal_session``, ``require_api_key``'s
    principal-bound sessions, and ``GET /console/session``, so a principal-bound
    cookie closes the same way everywhere: disabling the principal, suspending
    its tenant, or changing the configured issuer takes effect immediately on
    every route it reaches, not only ``/console/principal``.

    Pass ``live_row`` when the caller already loaded that session. The principal
    is read from the row, and the token is not looked up again.
    """
    from .crud import console as crud_console

    if live_row is None:
        principal = await crud_console.live_principal_session(session, token)
    else:
        principal = await crud_console.principal_for_live_row(session, live_row)
    settings = get_settings()
    if (
        principal is None
        or not settings.oidc_enabled
        or principal.idp_issuer != settings.oidc_issuer
    ):
        return None
    return principal


@dataclass(frozen=True)
class AdministrativeActor:
    """Who passed administrative authentication, derived only on the server.

    ``platform_key`` has no subject: the key names no person. A console
    session's subject is ``principal:<principal id>`` when the session is
    bound to a principal, else ``session:<console session id>``. Never the
    token, and never anything the request body says.
    """

    kind: Literal["platform_key", "console_session"]
    subject: str | None


async def authenticate_administrator(
    request: Request, x_api_key: str | None
) -> AdministrativeActor:
    """The ``require_api_key`` check, returning the actor it accepted (ADR-0083).

    The platform key first, with no database read; then a live console
    session, after the cookie origin check, with a principal-bound session
    also passing ``authorized_console_principal``. Every refusal is the same 401.
    """

    if verify_platform_key(x_api_key):
        return AdministrativeActor(kind="platform_key", subject=None)
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
            if await authorized_console_principal(session, token) is None:
                row = None
    if row is not None:
        subject = (
            f"principal:{row.principal_id}" if row.principal_id is not None else f"session:{row.id}"
        )
        return AdministrativeActor(kind="console_session", subject=subject)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="missing or invalid API key",
    )


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    await authenticate_administrator(request, x_api_key)


async def require_administrator(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
) -> AdministrativeActor:
    """``require_api_key`` for a route that records who acted (#5431)."""

    return await authenticate_administrator(request, x_api_key)


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
    principal = await authorized_console_principal(session, console_session or "")
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing, invalid, or expired principal session",
            headers={"Cache-Control": "no-store"},
        )
    return principal
