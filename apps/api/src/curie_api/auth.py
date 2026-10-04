"""Platform key first, then a live console session.

The platform key in the `X-API-Key` header is checked first, with no database
read. A live console session is accepted only after that check fails. J1
replaces this with GitHub-App-scoped identities.
"""

import hmac
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

from .config import get_settings

API_KEY_HEADER = "X-API-Key"
CONSOLE_SESSION_COOKIE = "__Host-curie_console_session"


def verify_platform_key(x_api_key: str | None) -> bool:
    """True when the header carries the shared platform API key (constant-time).

    The single place that defines what 'the platform key' means, shared by
    require_api_key (raise on fail) and the state router's require_state_access
    (fall through to the scoped-token check)."""
    if x_api_key is None:
        return False
    return hmac.compare_digest(x_api_key, get_settings().api_key)


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
    from . import crud

    async with request.app.state.sessionmaker() as session:
        row = await crud.live_console_session(session, token)
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
