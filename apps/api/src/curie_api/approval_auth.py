"""Authentication boundary for approval resolvers (ADR-0106, #1531)."""

from __future__ import annotations

import hmac
import uuid
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Cookie, Depends, Header, HTTPException, status

from . import approval_principal, crud
from .auth import SESSION_COOKIE
from .config import get_settings
from .deps import SessionDep

APPROVAL_PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
#: One definition, not two. Two branches independently spelled this literal --
#: here for the approval principal, and in `auth` for the dependency that reads
#: the cookie -- and they agree only by luck. `auth` is the one that verifies it
#: and imports nothing but `config`, so pointing this at it costs no import
#: cycle; the reverse would pull `crud` in eagerly and undo the lazy import
#: `require_api_key` relies on.
CONSOLE_SESSION_COOKIE = SESSION_COOKIE

AuthenticatedPrincipalKind = Literal["chat", "console", "operator"]


@dataclass(frozen=True)
class AuthenticatedApprovalPrincipal:
    """Server-derived resolver identity and its authenticated evidence."""

    subject: str
    kind: AuthenticatedPrincipalKind
    actor_channel: str | None


def _unauthorized(detail: str = "missing or invalid approval principal") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"Cache-Control": "no-store"},
    )


async def authenticate_console_session(
    session: SessionDep, token: str | None
) -> AuthenticatedApprovalPrincipal | None:
    """Return the live, subject-bound console principal for ``token``.

    Legacy sessions with no subject remain valid for unrelated future console
    uses but can never become an approval resolver.
    """

    if not token:
        return None
    row = await crud.live_console_session(session, token)
    if row is None or row.subject is None or not row.subject.strip():
        return None
    return AuthenticatedApprovalPrincipal(
        subject=row.subject,
        kind="console",
        actor_channel=None,
    )


async def require_approval_principal(
    approval_id: uuid.UUID,
    session: SessionDep,
    x_curie_approval_principal: Annotated[
        str | None, Header(alias=APPROVAL_PRINCIPAL_HEADER)
    ] = None,
    console_session: Annotated[str | None, Cookie(alias=CONSOLE_SESSION_COOKIE)] = None,
) -> AuthenticatedApprovalPrincipal:
    """Authenticate exactly one resolver credential for ``approval_id``.

    The platform key is intentionally absent: it administers principal
    issuance but is not itself a human identity.  A principal header and a
    console cookie together are ambiguous and fail closed rather than choosing
    one by precedence.
    """

    has_token = x_curie_approval_principal is not None
    has_cookie = console_session is not None
    if has_token and has_cookie:
        raise _unauthorized("ambiguous approval principal credentials")
    if not has_token and not has_cookie:
        raise _unauthorized()

    if has_cookie:
        principal = await authenticate_console_session(session, console_session)
        if principal is None:
            raise _unauthorized()
        return principal

    assert x_curie_approval_principal is not None
    settings = get_settings()
    kind = approval_principal.unverified_kind(x_curie_approval_principal)
    if kind == "chat":
        attester_secret = settings.approval_chat_attester_secret
        if not attester_secret or hmac.compare_digest(
            attester_secret.encode(), settings.api_key.encode()
        ):
            raise _unauthorized()
        claims = approval_principal.verify_claims(
            x_curie_approval_principal,
            attester_secret,
            scope=approval_principal.APPROVE_SCOPE,
            approval_id=str(approval_id),
        )
    elif kind == "operator":
        claims = approval_principal.verify_claims(
            x_curie_approval_principal,
            settings.api_key,
            scope=approval_principal.APPROVE_SCOPE,
        )
    else:
        claims = None
    if claims is None or claims.kind != kind:
        raise _unauthorized()
    return AuthenticatedApprovalPrincipal(
        subject=claims.subject,
        kind=claims.kind,
        actor_channel=claims.actor_channel,
    )


ApprovalPrincipalDep = Annotated[
    AuthenticatedApprovalPrincipal, Depends(require_approval_principal)
]
