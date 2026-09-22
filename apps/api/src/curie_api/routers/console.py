"""Console session exchange and authenticated current-session inspection.

ADR-0083, first slice (#1044) of #630. The console holds the shared platform
administrator key in browser code today -- resolved from `?api_key=`, a build
variable, or a published dev default -- which puts a credential that authorizes
deployments, approvals, budgets and the kill switch into browser history, request
logs and referrers, revocable only by rotating the Secret and restarting the API.

Two endpoints, deliberately asymmetric:

- ``POST /console/login-codes`` requires the immutable platform-key-only
  dependency. Minting is an administrative act, and the CLI is the intended
  caller. The administrator-selected subject is bound to the row at mint time.
- ``POST /console/session`` requires NO credential, because the login code IS the
  credential being presented. It is the one unauthenticated write in the API, so
  it is bounded on purpose: the code is single-use and short-lived, a failure is
  indistinguishable from any other failure (so codes cannot be probed), and
  success grants a session, never the platform key.

ADR-0106 consumes the live session only as an approval principal. The cookie
does not become a platform key and cannot call either administrative mint.

Generic OIDC login (#2908, ADR 0155) is a second way to arrive at the same
kind of session: ``GET /console/oidc/login`` redirects to the IdP with PKCE,
``GET /console/oidc/callback`` exchanges the code server-side and mints a
session bound to a principal instead of a ``subject``. No token ever reaches
browser script -- the reason for a server-side code flow rather than an
endpoint that accepts an ID token from the page. ``GET /console/principal`` is
the first route guarded by ``require_principal_session``.
"""

import hmac
import logging
from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse, RedirectResponse

from .. import crud, oidc
from ..approval_auth import CONSOLE_SESSION_COOKIE
from ..auth import require_platform_key, require_principal_session
from ..config import get_settings
from ..deps import SessionDep
from ..models import Principal
from ..schemas import (
    ConsoleLoginCodeMint,
    ConsoleLoginCodeOut,
    ConsoleSessionExchange,
    ConsoleSessionOut,
    PrincipalOut,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/console", tags=["console"])

#: The cookie the console authenticates with. `HttpOnly` is the property that
#: makes this strictly stronger than the status quo: page script cannot read it,
#: so injected script cannot exfiltrate the credential it authenticates with.
SESSION_COOKIE = CONSOLE_SESSION_COOKIE


@router.post(
    "/login-codes",
    response_model=ConsoleLoginCodeOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_platform_key)],
)
async def create_login_code(
    data: ConsoleLoginCodeMint, session: SessionDep, response: Response
) -> ConsoleLoginCodeOut:
    """Mint a single-use login code for an operator to copy into the console."""
    code, row = await crud.create_console_login_code(session, subject=data.subject)
    response.headers["Cache-Control"] = "no-store"
    return ConsoleLoginCodeOut(
        code=code,
        subject=data.subject,
        expires_at=row.login_code_expires_at,
    )


@router.post("/session", response_model=ConsoleSessionOut)
async def exchange_login_code(
    data: ConsoleSessionExchange, response: Response, session: SessionDep
) -> ConsoleSessionOut:
    """Exchange a login code for a session cookie.

    Unauthenticated by design (see the module docstring). Every rejection is the
    same 401 with the same text: an unknown code, an already-consumed one, an
    expired one and a revoked row are indistinguishable to the caller, so this
    endpoint cannot be used to enumerate which codes exist.
    """
    exchanged = await crud.exchange_console_login_code(session, data.code)
    if exchanged is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or expired login code",
        )
    token, row = exchanged

    # The token leaves the server ONLY here and ONLY as a cookie. `httponly`
    # keeps it away from page script; `secure` keeps it off plaintext hops;
    # `samesite="strict"` means a cross-site request cannot carry it, which is the
    # CSRF property that matters for an API whose every write is authorized by it.
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="strict",
        path="/",
    )
    # Note the response body: an expiry, never the token. Returning it would hand
    # the credential back to the JavaScript this whole design keeps it from.
    assert row.session_expires_at is not None  # set by the exchange above
    response.headers["Cache-Control"] = "no-store"
    return ConsoleSessionOut(subject=row.subject, expires_at=row.session_expires_at)


@router.get("/session", response_model=ConsoleSessionOut)
async def current_session(
    session: SessionDep,
    response: Response,
    console_session: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
) -> ConsoleSessionOut:
    """Return the immutable subject of the live session in the HttpOnly cookie."""

    row = await crud.live_console_session(session, console_session or "")
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing, invalid, or expired console session",
            headers={"Cache-Control": "no-store"},
        )
    subject = row.subject
    expires_at = row.session_expires_at
    if subject is None or not subject.strip() or expires_at is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing, invalid, or expired console session",
            headers={"Cache-Control": "no-store"},
        )
    response.headers["Cache-Control"] = "no-store"
    return ConsoleSessionOut(
        subject=subject,
        expires_at=expires_at,
    )


# --- generic OIDC login (#2908) ------------------------------------------------

#: Carries ``state`` from the login redirect to the callback in the same
#: browser. The callback requires the query ``state`` to equal it, which is the
#: login-CSRF binding: an attacker cannot make a victim's browser complete a
#: login the attacker started, because the victim's browser never got this
#: cookie. SameSite=Lax (not Strict) because the callback arrives as a
#: top-level navigation FROM the IdP's site, and Strict would withhold it.
OIDC_STATE_COOKIE = "curie_oidc_state"
_OIDC_COOKIE_PATH = "/console/oidc"
_NO_STORE = {"Cache-Control": "no-store"}
#: The one body every refused callback gets. Never the IdP's `error` or
#: `error_description`: those are attacker-influenced strings, and echoing
#: which step failed would tell a prober what to fix.
_OIDC_REFUSED = "OIDC login failed"


def _require_oidc_enabled() -> None:
    # 404, not 403: with OIDC off the routes should look like they do not
    # exist, exactly as they did before this feature.
    if not get_settings().oidc_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, headers=_NO_STORE)


def _clear_state_cookie(response: Response) -> None:
    response.delete_cookie(
        OIDC_STATE_COOKIE, path=_OIDC_COOKIE_PATH, secure=True, httponly=True, samesite="lax"
    )


def _refuse_callback(reason: str) -> JSONResponse:
    """The single indistinguishable callback failure; the reason goes to logs only."""

    logger.warning("oidc callback refused: %s", reason)
    response = JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": _OIDC_REFUSED},
        headers=_NO_STORE,
    )
    _clear_state_cookie(response)
    return response


@router.get("/oidc/login", status_code=status.HTTP_302_FOUND, response_class=RedirectResponse)
async def oidc_login(session: SessionDep) -> RedirectResponse:
    """Start an OIDC login: persist an attempt and redirect to the IdP.

    Discovery runs first so a misconfigured or mixed-up IdP fails here, before
    an attempt row exists and before the browser is sent anywhere.
    """
    _require_oidc_enabled()
    try:
        metadata = await oidc.discover()
    except oidc.OidcError as exc:
        logger.warning("oidc discovery failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="identity provider unavailable",
            headers=_NO_STORE,
        ) from None
    try:
        start = await crud.create_oidc_login_attempt(session)
    except crud.OidcLoginAttemptsExhausted:
        # No row, no state cookie: a refused start leaves nothing behind.
        logger.warning("oidc login refused: live login attempt cap reached")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="too many logins in progress; try again shortly",
            headers=_NO_STORE,
        ) from None
    response = RedirectResponse(
        oidc.authorization_url(
            metadata,
            state=start.state,
            nonce=start.nonce,
            challenge=start.code_challenge,
        ),
        status_code=status.HTTP_302_FOUND,
        headers=_NO_STORE,
    )
    response.set_cookie(
        OIDC_STATE_COOKIE,
        start.state,
        max_age=int(crud.OIDC_LOGIN_TTL.total_seconds()),
        path=_OIDC_COOKIE_PATH,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


@router.get(
    "/oidc/callback",
    status_code=status.HTTP_303_SEE_OTHER,
    response_class=RedirectResponse,
    responses={401: {"description": "The login was refused"}},
)
async def oidc_callback(
    session: SessionDep,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    state_cookie: Annotated[str | None, Cookie(alias=OIDC_STATE_COOKIE)] = None,
) -> Response:
    """Finish an OIDC login and mint a principal console session.

    The order is the security argument: bind ``state`` to this browser's
    cookie, then spend the server-side attempt (single use, committed before
    anything else can fail), and only then touch the IdP with the code and the
    stored PKCE verifier. The ID token is validated against the attempt's
    nonce, the principal is resolved and must be active in an active tenant,
    and only then does a session exist. Every terminal response clears the
    state cookie, and every refusal looks the same.

    The ``code`` and ``state`` in the query may land in access logs. That is
    tolerated: the code is useless without the verifier that never left the
    server, and the state is single-use and cookie-bound.
    """
    _require_oidc_enabled()
    if not state or not state_cookie or not hmac.compare_digest(
        state.encode("utf-8"), state_cookie.encode("utf-8")
    ):
        return _refuse_callback("state does not match the state cookie")
    attempt = await crud.consume_oidc_login_attempt(session, state)
    if attempt is None:
        return _refuse_callback("unknown, consumed or expired login attempt")
    if error is not None:
        return _refuse_callback("the IdP returned an error")
    if not code:
        return _refuse_callback("no authorization code")
    try:
        id_token = await oidc.exchange_code(code, attempt.code_verifier)
        claims = await oidc.validate_id_token(id_token, nonce=attempt.nonce)
    except oidc.OidcError as exc:
        return _refuse_callback(str(exc))

    principal = await crud.resolve_principal(session, claims)
    if not await crud.principal_is_active(session, principal):
        # Undo the attribute refresh: a refused login changes nothing.
        await session.rollback()
        return _refuse_callback("principal or tenant is not active")
    token, _ = await crud.create_principal_console_session(session, principal)

    response = RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER, headers=_NO_STORE)
    # The same cookie, with the same flags and reasons, as exchange_login_code.
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="strict",
        path="/",
    )
    _clear_state_cookie(response)
    return response


@router.get("/principal", response_model=PrincipalOut)
async def current_principal(
    principal: Annotated[Principal, Depends(require_principal_session)],
    response: Response,
) -> PrincipalOut:
    """Return the principal the OIDC session cookie authenticates."""

    response.headers["Cache-Control"] = "no-store"
    return PrincipalOut.model_validate(principal)
