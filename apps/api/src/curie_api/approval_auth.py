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
from urllib.parse import urlsplit

from fastapi import Cookie, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.crud import console as crud_console

from . import adapter_principal, approval_principal
from .auth import CONSOLE_SESSION_COOKIE, require_api_key
from .config import get_settings
from .deps import SessionDep

APPROVAL_PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
ADAPTER_PRINCIPAL_HEADER = "X-Curie-Adapter-Principal"
APPROVAL_ACTOR_HEADER = "X-Curie-Approval-Actor"
_SAFE_ORIGIN_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_DEFAULT_ORIGIN_PORTS = {"http": 80, "https": 443}

AuthenticatedPrincipalKind = Literal["chat", "console", "operator", "adapter", "test_driver"]


def set_console_session_cookie(response: Response, token: str) -> None:
    """Set the host-only console session cookie.

    ``SameSite=Strict`` stays. It does not stop a same-site cross-origin form,
    which is why the cookie-only origin check in ``require_approval_principal``
    exists.
    """

    response.set_cookie(
        CONSOLE_SESSION_COOKIE,
        token,
        httponly=True,
        secure=True,
        samesite="strict",
        path="/",
    )


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
    row = await crud_console.live_console_session(session, token)
    if row is None or row.subject is None or not row.subject.strip():
        return None
    return AuthenticatedApprovalPrincipal(
        subject=row.subject,
        kind="console",
        actor_channel=None,
    )


def _origin_key(scheme: str, host: str, port: int | None) -> tuple[str, str, int | None] | None:
    normalized_scheme = scheme.strip().lower()
    normalized_host = host.strip().lower()
    if not normalized_scheme or not normalized_host:
        return None
    if port == _DEFAULT_ORIGIN_PORTS.get(normalized_scheme):
        port = None
    return (normalized_scheme, normalized_host, port)


def _origin_from_absolute(value: str) -> tuple[str, str, int | None] | None:
    raw = value.strip()
    if not raw or raw == "null":
        return None
    try:
        parts = urlsplit(raw)
        port = parts.port
    except ValueError:
        return None
    if not parts.scheme or parts.hostname is None or parts.hostname == "":
        return None
    return _origin_key(parts.scheme, parts.hostname, port)


def _expected_console_origin(request: Request) -> tuple[str, str, int | None] | None:
    # Host comes from the request the API actually received. Forwarded-host
    # headers are not consulted: the shipped UI proxy does not set them, and a
    # caller who can set them must not be able to aim the check at their own origin.
    try:
        port = request.url.port
    except ValueError:
        return None
    if not request.url.scheme or request.url.hostname is None:
        return None
    return _origin_key(request.url.scheme, request.url.hostname, port)


def _same_console_host(
    claimed: tuple[str, str, int | None] | None,
    expected: tuple[str, str, int | None] | None,
) -> bool:
    """True when host and port match.

    Scheme is not compared. The UI proxy connects to the API over HTTP while
    the browser's Origin uses the external scheme (HTTPS once the console is
    served securely). A different host is still rejected, which is the
    same-site sibling this check exists to stop.
    """

    if claimed is None or expected is None:
        return False
    return claimed[1:] == expected[1:]


def _claimed_console_origin(request: Request) -> tuple[str, str, int | None] | None:
    """Origin wins over Referer. A present Origin is never replaced by Referer."""

    if "origin" in request.headers:
        raw = request.headers["origin"].strip()
    elif "referer" in request.headers:
        raw = request.headers["referer"].strip()
    else:
        return None
    if not raw or raw == "null":
        return None
    return _origin_from_absolute(raw)


def _console_origin_rejected() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="console session origin rejected",
        headers={"Cache-Control": "no-store"},
    )


def enforce_console_cookie_origin(request: Request) -> None:
    """Reject an unsafe cookie-authenticated method whose origin host does not match.

    SameSite=Strict does not stop a same-site sibling form. Safe methods are
    not checked. The platform-key path must not call this.
    """
    if request.method.upper() in _SAFE_ORIGIN_METHODS:
        return
    if not _same_console_host(
        _claimed_console_origin(request), _expected_console_origin(request)
    ):
        raise _console_origin_rejected()


def principal_credentials_presented(
    x_curie_approval_principal: str | None,
    console_session: str | None,
    x_curie_adapter_principal: str | None,
) -> None:
    """Refuse no principal credential, or more than one, before any lookup.

    Any two of a principal header, a console cookie and an adapter credential
    together are ambiguous and fail closed rather than choosing one by
    precedence.
    """

    presented = sum(
        value is not None
        for value in (x_curie_approval_principal, console_session, x_curie_adapter_principal)
    )
    if presented > 1:
        raise _unauthorized("ambiguous approval principal credentials")
    if presented == 0:
        raise _unauthorized()


async def authenticate_principal(
    *,
    approval_id: uuid.UUID | None,
    request: Request,
    session: AsyncSession,
    x_curie_approval_principal: str | None,
    console_session: str | None,
    x_curie_adapter_principal: str | None,
    x_curie_approval_actor: str | None,
) -> AuthenticatedApprovalPrincipal:
    """Authenticate exactly one ADR 0106 principal credential.

    The one implementation behind the approval resolver and the undo ruling
    (ACTION-EXECUTOR-3). A chat credential is bound to ``approval_id``, so with
    none it never authenticates; an operator credential is bound to no
    approval. The platform key is intentionally absent: it administers
    principal issuance but is not itself a human identity.

    When the console cookie is the only credential on an unsafe method, the
    browser origin must match this request before the session is looked up.
    Header principals are not origin checked.
    """

    principal_credentials_presented(
        x_curie_approval_principal, console_session, x_curie_adapter_principal
    )

    if console_session is not None:
        enforce_console_cookie_origin(request)

    if x_curie_adapter_principal is not None:
        return _authenticate_adapter(x_curie_adapter_principal, x_curie_approval_actor)

    if console_session is not None:
        principal = await authenticate_console_session(session, console_session)
        if principal is None:
            raise _unauthorized()
        return principal

    assert x_curie_approval_principal is not None
    settings = get_settings()
    kind = approval_principal.unverified_kind(x_curie_approval_principal)
    if kind in ("chat", "test_driver"):
        attester_secret = settings.approval_chat_attester_secret
        if (
            approval_id is None
            or not attester_secret
            or hmac.compare_digest(attester_secret.encode(), settings.api_key.encode())
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
    if (
        kind == "test_driver"
        and getattr(request.scope.get("route"), "name", None) != "resolve_approval"
    ):
        raise _unauthorized("test driver principal is limited to approval resolution")
    return AuthenticatedApprovalPrincipal(
        subject=claims.subject,
        kind=claims.kind,
        actor_channel=claims.actor_channel,
    )


async def require_approval_principal(
    approval_id: uuid.UUID,
    request: Request,
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
    """Authenticate exactly one resolver credential for ``approval_id``."""

    return await authenticate_principal(
        approval_id=approval_id,
        request=request,
        session=session,
        x_curie_approval_principal=x_curie_approval_principal,
        console_session=console_session,
        x_curie_adapter_principal=x_curie_adapter_principal,
        x_curie_approval_actor=x_curie_approval_actor,
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
    the resolver's credentials do. The platform-key half is ``require_api_key``,
    and a live console session is that same half. An adapter plus the platform
    key, or an adapter plus a session cookie, fails closed before verification.
    """

    async def dependency(
        request: Request,
        x_api_key: Annotated[str | None, Header()] = None,
        x_curie_adapter_principal: Annotated[
            str | None, Header(alias=ADAPTER_PRINCIPAL_HEADER)
        ] = None,
    ) -> adapter_principal.AdapterClaims | None:
        if x_curie_adapter_principal is None:
            await require_api_key(request, x_api_key)
            return None
        if x_api_key is not None or CONSOLE_SESSION_COOKIE in request.cookies:
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
