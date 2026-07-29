"""Console session exchange: mint a login code, trade it for a session cookie.

ADR-0083, first slice (#1044) of #630. The console holds the shared platform
administrator key in browser code today -- resolved from `?api_key=`, a build
variable, or a published dev default -- which puts a credential that authorizes
deployments, approvals, budgets and the kill switch into browser history, request
logs and referrers, revocable only by rotating the Secret and restarting the API.

Two endpoints, deliberately asymmetric:

- ``POST /console/login-codes`` requires the platform key. Minting is an
  administrative act, and the CLI is the only intended caller.
- ``POST /console/session`` requires NO credential, because the login code IS the
  credential being presented. It is the one unauthenticated write in the API, so
  it is bounded on purpose: the code is single-use and short-lived, a failure is
  indistinguishable from any other failure (so codes cannot be probed), and
  success grants a session, never the platform key.

NOTHING CONSUMES A SESSION YET. ``require_api_key`` starts accepting one in slice
2 (#1045); until then these endpoints are inert and the console is unchanged. That
is the point of the slicing: the store and the exchange are provable on their own
before anything depends on them.
"""

from fastapi import APIRouter, Depends, HTTPException, Response, status

from .. import crud
from ..auth import require_api_key
from ..deps import SessionDep
from ..schemas import ConsoleLoginCodeOut, ConsoleSessionExchange, ConsoleSessionOut

router = APIRouter(prefix="/console", tags=["console"])

#: The cookie the console authenticates with. `HttpOnly` is the property that
#: makes this strictly stronger than the status quo: page script cannot read it,
#: so injected script cannot exfiltrate the credential it authenticates with.
SESSION_COOKIE = "curie_console_session"


@router.post(
    "/login-codes",
    response_model=ConsoleLoginCodeOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
)
async def create_login_code(session: SessionDep) -> ConsoleLoginCodeOut:
    """Mint a single-use login code for an operator to copy into the console."""
    code, row = await crud.create_console_login_code(session)
    return ConsoleLoginCodeOut(code=code, expires_at=row.login_code_expires_at)


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
    return ConsoleSessionOut(expires_at=row.session_expires_at)
