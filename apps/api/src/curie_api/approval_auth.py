"""Authentication boundary for approval resolvers (ADR-0106, #1531).

A channel adapter principal (ADR-0154, #2806) is the one resolver credential
that is not itself the human: it transports a decision on behalf of a sender it
authenticated at ingress, named in ``X-Curie-Approval-Actor``. The adapter's
subject is kept beside that actor so the audit row names both.
"""

from __future__ import annotations

import hmac
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Cookie, Depends, Header, HTTPException, status

from . import adapter_principal, approval_principal, crud
from .auth import CONSOLE_SESSION_COOKIE as CONSOLE_SESSION_COOKIE
from .auth import require_api_key
from .config import get_settings
from .deps import SessionDep

APPROVAL_PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
ADAPTER_PRINCIPAL_HEADER = "X-Curie-Adapter-Principal"
APPROVAL_ACTOR_HEADER = "X-Curie-Approval-Actor"

AuthenticatedPrincipalKind = Literal["chat", "console", "operator", "adapter"]


@dataclass(frozen=True)
class AuthenticatedApprovalPrincipal:
    """Server-derived resolver identity and its authenticated evidence."""

    subject: str
    kind: AuthenticatedPrincipalKind
    actor_channel: str | None
    # Set only for kind "adapter": the adapter that transported the decision
    # (``subject`` is then the sender it authenticated) and the binding rows it
    # serves, which scope the approvals it may resolve.
    adapter: str | None = None
    adapter_bindings: frozenset[uuid.UUID] = frozenset()


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
    x_curie_adapter_principal: Annotated[
        str | None, Header(alias=ADAPTER_PRINCIPAL_HEADER)
    ] = None,
    x_curie_approval_actor: Annotated[str | None, Header(alias=APPROVAL_ACTOR_HEADER)] = None,
) -> AuthenticatedApprovalPrincipal:
    """Authenticate exactly one resolver credential for ``approval_id``.

    The platform key is intentionally absent: it administers principal
    issuance but is not itself a human identity.  Any two of a principal
    header, a console cookie and an adapter credential together are ambiguous
    and fail closed rather than choosing one by precedence.
    """

    has_token = x_curie_approval_principal is not None
    has_cookie = console_session is not None
    has_adapter = x_curie_adapter_principal is not None
    presented = sum((has_token, has_cookie, has_adapter))
    if presented > 1:
        raise _unauthorized("ambiguous approval principal credentials")
    if presented == 0:
        raise _unauthorized()

    if has_adapter:
        assert x_curie_adapter_principal is not None
        return _authenticate_adapter(x_curie_adapter_principal, x_curie_approval_actor)

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


def _authenticate_adapter(token: str, actor: str | None) -> AuthenticatedApprovalPrincipal:
    """The adapter principal, with the sender it vouches for as the actor.

    The actor is required: an adapter is the transport of a decision, not its
    author, so a resolution with no named sender has no one to judge. The
    sender carries no channel evidence, which is why the authorizer admits an
    adapter only on explicit-user routes.
    """

    claims = adapter_principal.verify(
        token, get_settings().api_key, scope=adapter_principal.SCOPE_APPROVALS_RESOLVE
    )
    if claims is None or actor is None or not actor.strip():
        raise _unauthorized()
    stripped_actor = actor.strip()
    return AuthenticatedApprovalPrincipal(
        subject=stripped_actor,
        kind="adapter",
        actor_channel=None,
        adapter=claims.subject,
        adapter_bindings=claims.bindings,
    )


def platform_key_or_adapter(
    scope: str,
) -> Callable[..., Awaitable[adapter_principal.AdapterClaims | None]]:
    """A dependency accepting the platform key OR an adapter credential with
    ``scope`` (ADR-0154), returning the adapter's claims or None for the key.

    Exactly one credential: both together are ambiguous and fail closed, as
    the resolver's credentials do. The platform-key half is ``require_api_key``
    unchanged, detail string included.
    """

    async def dependency(
        x_api_key: Annotated[str | None, Header()] = None,
        x_curie_adapter_principal: Annotated[
            str | None, Header(alias=ADAPTER_PRINCIPAL_HEADER)
        ] = None,
    ) -> adapter_principal.AdapterClaims | None:
        if x_curie_adapter_principal is None:
            await require_api_key(x_api_key)
            return None
        if x_api_key is not None:
            raise _unauthorized("ambiguous credentials")
        claims = adapter_principal.verify(
            x_curie_adapter_principal, get_settings().api_key, scope=scope
        )
        if claims is None:
            raise _unauthorized("missing or invalid adapter principal")
        return claims

    return dependency


async def require_adapter_principal(
    x_api_key: Annotated[str | None, Header()] = None,
    x_curie_adapter_principal: Annotated[
        str | None, Header(alias=ADAPTER_PRINCIPAL_HEADER)
    ] = None,
) -> adapter_principal.AdapterClaims:
    """The adapter credential alone, for self-rotation. The platform key is not
    accepted: it issues adapter credentials, it does not renew one. Presenting
    both together is ambiguous and fails closed, matching the other adapter
    and resolver dependencies."""

    if x_curie_adapter_principal is None:
        raise _unauthorized("missing or invalid adapter principal")
    if x_api_key is not None:
        raise _unauthorized("ambiguous credentials")
    claims = adapter_principal.verify(
        x_curie_adapter_principal,
        get_settings().api_key,
        scope=adapter_principal.SCOPE_APPROVALS_READ,
    )
    if claims is None:
        raise _unauthorized("missing or invalid adapter principal")
    return claims


ApprovalPrincipalDep = Annotated[
    AuthenticatedApprovalPrincipal, Depends(require_approval_principal)
]
