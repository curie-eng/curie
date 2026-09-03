"""Who may call this API.

Two credentials, one dependency. A machine caller (the CLI, the worker, the
runner) sends the shared platform key in `X-API-Key`. A human at the console
sends a session cookie, established by exchanging a CLI-minted login code
(ADR-0083, slice 2 of #1044/#1045).

`require_api_key` accepts either, **in that order**, and remains the single
dependency every router depends on -- extending the shared gate rather than
letting a router grow a second auth scheme, which is the boundary
`apps/api/CLAUDE.md` draws.

The order is load-bearing beyond precedence: the platform-key path returns
before the session store is read, so a database outage cannot take machine
callers down with it.

Per-user identity (GitHub-App-scoped) is still ahead of us; a console session
authenticates *that someone logged in*, not *who*.
"""

import hmac
from typing import Annotated

from fastapi import Cookie, Header, HTTPException, Request, status

from .config import get_settings

API_KEY_HEADER = "X-API-Key"


def verify_platform_key(x_api_key: str | None) -> bool:
    """True when the header carries the shared platform API key (constant-time).

    The single place that defines what 'the platform key' means, shared by
    require_api_key (raise on fail) and the state router's require_state_access
    (fall through to the scoped-token check)."""
    if x_api_key is None:
        return False
    return hmac.compare_digest(x_api_key, get_settings().api_key)


#: The cookie the console authenticates with. `HttpOnly`, so page script cannot
#: read it and injected script cannot exfiltrate the credential it authenticates
#: with. Defined here rather than in the router because this module is what
#: verifies it, and a router importing down into auth is the right direction.
SESSION_COOKIE = "curie_console_session"


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Header()] = None,
    curie_console_session: Annotated[str | None, Cookie()] = None,
) -> None:
    """Gate every router on the platform key or a live console session.

    The database session is opened HERE rather than taken as a dependency, and
    only when a cookie is actually presented. Declaring `SessionDep` would have
    FastAPI open a session while resolving dependencies for every request --
    including machine callers, and including routes that touch no database at
    all -- which would make the platform-key path fail whenever the store was
    unreachable. That is precisely the coupling ADR-0083's ordering exists to
    prevent, and it is not visible from reading the function body.

    Raises:
        HTTPException: 401 when neither credential authenticates.
    """
    # A machine caller returns before anything touches the database.
    if verify_platform_key(x_api_key):
        return

    if curie_console_session is not None:
        from . import crud  # local: avoids a cycle through models at import time

        sessionmaker = getattr(request.app.state, "sessionmaker", None)
        if sessionmaker is not None:
            async with sessionmaker() as session:
                if await crud.live_console_session(session, curie_console_session) is not None:
                    return

    # One indistinguishable failure for a missing key, a wrong key, a consumed
    # code, a revoked session and an expired one. Telling them apart would tell
    # an attacker which half of the credential to keep working on.
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="missing or invalid credentials",
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
