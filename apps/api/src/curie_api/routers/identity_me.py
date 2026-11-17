"""Principal-owned account links (ADR 0281 sections 4 and 5)."""

from __future__ import annotations

import hashlib
import re
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Annotated
from urllib.parse import urlencode

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from .. import identity_audit, identity_fence
from ..auth import CONSOLE_SESSION_COOKIE, authorized_console_principal
from ..config import get_settings
from ..deps import SessionDep
from ..identity import github_link
from ..identity.service import find_or_create_namespace
from ..models import GithubLinkState, IdentityLink, IdentityNamespace
from ..schemas.identity import IdentityLinkRevokedOut
from .personal_enrollments import (
    EnrollmentRoute,
    OwnerDep,
    SameOriginDep,
    require_owner_session_only,
)

FLOW_COOKIE = "__Host-curie_github_link_session"
router = APIRouter(prefix="/identity/me", tags=["identity-me"], route_class=EnrollmentRoute)


class OwnedLinkOut(BaseModel):
    id: uuid.UUID
    provider: str
    display_label: str
    created_at: datetime


class OwnedLinksOut(BaseModel):
    links: list[OwnedLinkOut]
    github_link_available: bool


class GithubLinkStartOut(BaseModel):
    authorize_url: str


def _enabled() -> None:
    if not github_link.personal_linking_enabled(get_settings()):
        raise HTTPException(404, "Not Found")


@router.get(
    "/links",
    response_model=OwnedLinksOut,
    dependencies=[Depends(_enabled), Depends(require_owner_session_only)],
)
async def list_links(principal: OwnerDep, session: SessionDep) -> OwnedLinksOut:
    rows = (
        await session.execute(
            select(IdentityLink, IdentityNamespace)
            .join(IdentityNamespace, IdentityLink.identity_namespace_id == IdentityNamespace.id)
            .where(
                IdentityLink.tenant_id == principal.tenant_id,
                IdentityLink.principal_id == principal.id,
                IdentityLink.revoked_at.is_(None),
                IdentityNamespace.provider.in_(("slack", "github")),
            )
            .order_by(IdentityLink.verified_at, IdentityLink.id)
        )
    ).all()
    configured = github_link.configuration(get_settings())
    return OwnedLinksOut(
        links=[
            OwnedLinkOut(
                id=link.id,
                provider=namespace.provider,
                display_label=link.display_label or link.provider_native_id,
                created_at=link.verified_at,
            )
            for link, namespace in rows
        ],
        github_link_available=configured is not None
        and not any(
            namespace.provider == "github" and namespace.authority == configured.host
            for _, namespace in rows
        ),
    )


@router.delete(
    "/links/{link_id}",
    response_model=IdentityLinkRevokedOut,
    dependencies=[Depends(_enabled), Depends(require_owner_session_only)],
)
async def unlink(
    link_id: uuid.UUID, principal: OwnerDep, _origin: SameOriginDep, session: SessionDep
) -> IdentityLinkRevokedOut:
    link = await session.scalar(
        select(IdentityLink)
        .join(IdentityNamespace, IdentityLink.identity_namespace_id == IdentityNamespace.id)
        .where(
            IdentityLink.id == link_id,
            IdentityLink.tenant_id == principal.tenant_id,
            IdentityLink.principal_id == principal.id,
            IdentityNamespace.provider.in_(("slack", "github")),
        )
        .with_for_update()
    )
    if link is None:
        raise HTTPException(404, {"code": "link_not_found"})
    if link.revoked_at is None:
        link.revoked_at = await session.scalar(select(func.now()))
        identity_audit.record(
            session,
            tenant_id=principal.tenant_id,
            event_type="identity.link_revoked",
            actor=f"principal:{principal.id}",
            principal_id=principal.id,
            details={"link_id": str(link.id)},
        )
    assert link.revoked_at is not None
    result = IdentityLinkRevokedOut(link_id=link.id, revoked_at=link.revoked_at)
    await session.commit()
    return result


@router.post(
    "/github-link",
    response_model=GithubLinkStartOut,
    dependencies=[Depends(require_owner_session_only)],
)
async def start_github_link(
    request: Request,
    response: Response,
    principal: OwnerDep,
    _origin: SameOriginDep,
    session: SessionDep,
) -> GithubLinkStartOut:
    settings = get_settings()
    config = github_link.require_configuration(settings)
    console_session = re.fullmatch(r"[A-Za-z0-9_-]{43}", request.cookies[CONSOLE_SESSION_COOKIE])
    if console_session is None:
        raise HTTPException(401, "missing, invalid, or expired principal session")
    state = secrets.token_urlsafe(32)
    now = await session.scalar(select(func.now()))
    assert now is not None
    session.add(
        GithubLinkState(
            state_hash=hashlib.sha256(state.encode()).hexdigest(),
            tenant_id=principal.tenant_id,
            principal_id=principal.id,
            expires_at=now + timedelta(seconds=github_link.TTL_SECONDS),
        )
    )
    await session.commit()
    # The main Console cookie is Strict. Only this ten-minute return cookie is
    # Lax; the callback still verifies its live session against the one-use state.
    response.set_cookie(
        FLOW_COOKIE,
        console_session[0],
        secure=True,
        httponly=True,
        samesite="lax",
        path="/",
        max_age=github_link.TTL_SECONDS,
    )
    return GithubLinkStartOut(
        authorize_url=config.web
        + "/login/oauth/authorize?"
        + urlencode(
            {
                "client_id": settings.factory_github_app_client_id,
                "state": state,
                "redirect_uri": config.callback,
            }
        )
    )


def _result(result: str) -> RedirectResponse:
    response = RedirectResponse(
        "/settings/claude-plan?github=" + result,
        status_code=303,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )
    response.delete_cookie(FLOW_COOKIE, path="/", secure=True, httponly=True, samesite="lax")
    return response


@router.get("/github-link/callback", response_class=RedirectResponse)
async def github_link_callback(
    request: Request,
    session: SessionDep,
    console_session: Annotated[str | None, Cookie(alias=CONSOLE_SESSION_COOKIE)] = None,
    flow_session: Annotated[str | None, Cookie(alias=FLOW_COOKIE)] = None,
) -> RedirectResponse:
    settings = get_settings()
    config = github_link.require_configuration(settings)
    state = request.query_params.get("state", "")
    row = (
        await session.execute(
            update(GithubLinkState)
            .where(
                GithubLinkState.state_hash == hashlib.sha256(state.encode()).hexdigest(),
                GithubLinkState.consumed_at.is_(None),
            )
            .values(consumed_at=func.now())
            .returning(
                GithubLinkState.tenant_id,
                GithubLinkState.principal_id,
                GithubLinkState.expires_at,
                GithubLinkState.consumed_at,
            )
        )
    ).one_or_none()
    await session.commit()
    if row is None or row.expires_at <= row.consumed_at:
        return _result("failed")
    try:
        await require_owner_session_only(request)
    except HTTPException:
        return _result("failed")
    principal = await authorized_console_principal(session, console_session or flow_session or "")
    code = request.query_params.get("code", "")
    if (
        principal is None
        or (principal.tenant_id, principal.id) != (row.tenant_id, row.principal_id)
        or not code
    ):
        return _result("failed")
    tenant_id, principal_id = principal.tenant_id, principal.id
    await session.rollback()
    try:
        native_id, login = await github_link.verified_account(settings, config, code)
    except github_link.ProviderRefused:
        return _result("failed")
    async with identity_fence.mutation(
        request.app.state.sessionmaker, request.app.state.valkey, request=request
    ) as writer:
        namespace = await find_or_create_namespace(
            writer,
            tenant_id=tenant_id,
            provider="github",
            authority=config.host,
            kind="github_host",
            key=config.host,
        )
        if namespace.status != "active":
            return _result("failed")
        link = IdentityLink(
            tenant_id=tenant_id,
            identity_namespace_id=namespace.id,
            provider_native_id=native_id,
            principal_id=principal_id,
            created_by_principal_id=principal_id,
            verification_source="provider_event_verified",
            display_label=login,
        )
        writer.add(link)
        try:
            await writer.flush()
        except IntegrityError:
            await writer.rollback()
            return _result("taken")
        identity_audit.record(
            writer,
            tenant_id=tenant_id,
            principal_id=principal_id,
            actor=f"principal:{principal_id}",
            event_type="identity.code_host_link_created",
            details={"link_id": str(link.id), "namespace_id": str(namespace.id)},
        )
        await writer.commit()
    return _result("linked")
