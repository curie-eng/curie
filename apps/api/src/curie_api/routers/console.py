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
"""

from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status

from .. import crud
from ..approval_auth import CONSOLE_SESSION_COOKIE
from ..auth import require_platform_key
from ..deps import SessionDep
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


@router.delete("/session", status_code=status.HTTP_204_NO_CONTENT)
async def sign_out(
    response: Response,
    session: SessionDep,
    console_session: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
) -> None:
    """Revoke this console's session and clear its cookie.

    Not gated on `require_api_key`. The credential being destroyed is the one
    doing the authorizing, so demanding it be valid first would mean an expired
    or already-revoked session could never clear its own cookie, which is the
    state most in need of clearing.

    Idempotent, and silent about what it found. No cookie, an unknown token and
    a session revoked a week ago all return the same 204, for the same reason
    the exchange returns one 401 for every kind of bad code: the answer must not
    say whether a token is real.

    The cookie is `samesite="strict"`, so a request from another origin does not
    carry it and cannot reach a session to revoke. That is what makes an
    unauthenticated destructive route safe here.
    """
    if console_session is not None:
        row = await crud.live_console_session(session, console_session)
        if row is not None:
            await crud.revoke_console_session(session, row)

    # Cleared whatever happened above, so a browser holding a token the server
    # has never heard of still ends up signed out rather than retrying with it.
    # The attributes must match the ones it was set with or the browser keeps it.
    response.delete_cookie(SESSION_COOKIE, httponly=True, secure=True, samesite="strict", path="/")
