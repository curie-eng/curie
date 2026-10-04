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

Session request budgets are shared across API replicas through Valkey and keyed
by the socket peer address. ``POST /console/session`` permits 30 requests and
``GET /console/session`` permits 120 requests in separate 60 second windows.
Forwarded client addresses are ignored, so users behind one proxy share the
corresponding budgets. An exhausted budget returns 429 with a Retry After header
before database work; an unavailable limiter returns 503. Authenticated routes
must opt in explicitly. The administrative login code mint does not opt in.

ADR-0106 consumes the live session only as an approval principal. The cookie
does not become a platform key and cannot call either administrative mint.

Readers accept only ``__Host-curie_console_session``. Sessions already minted
under ``curie_console_session`` stop working; the session TTL is 12 hours, and
the operator exchanges a new login code.
"""

from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response, status

from .. import crud
from ..approval_auth import set_console_session_cookie
from ..auth import CONSOLE_SESSION_COOKIE, require_platform_key
from ..deps import SessionDep
from ..rate_limit import require_rate_limit
from ..schemas import (
    ConsoleLoginCodeMint,
    ConsoleLoginCodeOut,
    ConsoleSessionExchange,
    ConsoleSessionOut,
)

router = APIRouter(prefix="/console", tags=["console"])

#: The cookie the console authenticates with. `HttpOnly` is the property that
#: makes this strictly stronger than the status quo: page script cannot read it,
#: so injected script cannot exfiltrate the credential it authenticates with.
SESSION_COOKIE = CONSOLE_SESSION_COOKIE


async def limit_session_exchange(request: Request) -> None:
    await require_rate_limit(request, route="console_session_post", limit=30, window_seconds=60)


async def limit_current_session(request: Request) -> None:
    await require_rate_limit(request, route="console_session_get", limit=120, window_seconds=60)


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


@router.post(
    "/session", response_model=ConsoleSessionOut, dependencies=[Depends(limit_session_exchange)]
)
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
    # keeps it away from page script; `secure` keeps it off plaintext hops.
    # SameSite=Strict stays. It does not stop a same-site cross-origin form,
    # which is why the cookie-only origin check exists.
    set_console_session_cookie(response, token)
    # Note the response body: an expiry, never the token. Returning it would hand
    # the credential back to the JavaScript this whole design keeps it from.
    assert row.session_expires_at is not None  # set by the exchange above
    response.headers["Cache-Control"] = "no-store"
    return ConsoleSessionOut(subject=row.subject, expires_at=row.session_expires_at)


@router.get(
    "/session", response_model=ConsoleSessionOut, dependencies=[Depends(limit_current_session)]
)
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
