"""Scoped authenticated IdP ingestion (ADR 0220); no hand-written identity mappings."""

import re
import time
import uuid
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from sqlalchemy import select

from .. import directory_credential, directory_service, directory_sync, identity_audit
from ..auth import require_platform_key, verify_platform_key
from ..config import get_settings
from ..deps import SessionDep, request_session
from ..directory_credential import DirectoryClaims
from ..identity_fence import lock_identity_reader
from ..models import IdentityNamespace, Principal, Team
from ..schemas.directory import (
    DirectoryAttemptIn,
    DirectoryBindingOut,
    DirectoryConfigIn,
    DirectoryConfigOut,
    DirectoryCredentialIn,
    DirectoryCredentialOut,
    DirectoryNamespaceIn,
    DirectoryNamespaceOut,
    DirectoryPeopleOut,
    DirectoryPersonOut,
    DirectorySnapshotIn,
    DirectorySnapshotOut,
    DirectoryStatusIn,
    DirectoryStatusOut,
    DirectoryTeamIn,
    DirectoryTeamOut,
)


class DirectoryRoute(APIRoute):
    """Validation never returns provider input or credential-bearing payloads."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacted(request: Request) -> Response:
            try:
                response = await handler(request)
                response.headers["Cache-Control"] = "private, no-store"
                return response
            except RequestValidationError:
                if request.method == "PUT" and request.url.path.endswith("/snapshot"):
                    claims = directory_credential.verify(
                        request.headers.get("X-Curie-Directory-Credential", ""),
                        get_settings().api_key,
                    )
                    if claims is not None and "X-API-Key" not in request.headers:
                        try:
                            body = await request.json()
                            epoch = body.get("expected_epoch") if isinstance(body, dict) else None
                            team_id = uuid.UUID(request.path_params["team_id"])
                            if type(epoch) is int and epoch >= 0:
                                async with request_session(request) as session:
                                    # @spec IDENTITY-WRITER-LIFETIME-V1-7. Discard the
                                    # incomplete dependency work before recording failure.
                                    await session.rollback()
                                    await directory_service.authenticate(session, claims)
                                    await directory_sync.record_failure(
                                        session,
                                        scope=claims.scope,
                                        team_id=team_id,
                                        expected_epoch=epoch,
                                        error="invalid_snapshot",
                                    )
                                    await session.commit()
                        except (directory_sync.DirectorySyncError, ValueError, KeyError):
                            pass
                raise HTTPException(
                    status_code=422,
                    detail="invalid_directory_request",
                    headers={"Cache-Control": "no-store"},
                ) from None

        return redacted


router = APIRouter(prefix="/directory", tags=["directory"], route_class=DirectoryRoute)


def refusal(error: directory_sync.DirectorySyncError) -> HTTPException:
    code = str(error)
    status = (
        409
        if isinstance(error, directory_sync.DirectoryConflict)
        else (403 if code.endswith("out_of_scope") else 422)
    )
    return HTTPException(status_code=status, detail=code, headers={"Cache-Control": "no-store"})


async def authenticated_directory_claims(
    session: SessionDep,
    credential: str | None,
    x_api_key: str | None,
    *,
    write: bool = False,
) -> DirectoryClaims:
    claims = directory_credential.verify(credential or "", get_settings().api_key)
    if claims is not None and x_api_key is None:
        try:
            await directory_service.authenticate(session, claims, write=write)
            return claims
        except directory_sync.DirectorySyncError:
            pass
    raise HTTPException(
        status_code=401,
        detail="invalid_directory_credential",
        headers={"Cache-Control": "no-store"},
    )


async def directory_access(
    session: SessionDep,
    credential: Annotated[str | None, Header(alias="X-Curie-Directory-Credential")] = None,
    x_api_key: Annotated[str | None, Header()] = None,
) -> DirectoryClaims:
    return await authenticated_directory_claims(session, credential, x_api_key)


DirectoryDep = Annotated[DirectoryClaims, Depends(directory_access)]


async def snapshot_access(
    body: DirectorySnapshotIn,
    session: SessionDep,
    credential: Annotated[str | None, Header(alias="X-Curie-Directory-Credential")] = None,
    x_api_key: Annotated[str | None, Header()] = None,
) -> tuple[DirectoryClaims, DirectorySnapshotIn]:
    # Validate the body before taking the exclusive config lock. A malformed
    # body's failure recorder uses another session while dependencies unwind.
    # Never nest DirectoryDep here: its shared lock would require an upgrade.
    claims = await authenticated_directory_claims(session, credential, x_api_key, write=True)
    return claims, body


SnapshotDep = Annotated[
    tuple[DirectoryClaims, DirectorySnapshotIn], Depends(snapshot_access)
]


@router.get("/binding", response_model=DirectoryBindingOut)
async def read_directory_binding(
    request: Request,
    session: SessionDep,
    credential: Annotated[str | None, Header(alias="X-Curie-Directory-Credential")] = None,
) -> DirectoryBindingOut:
    # This is credential self-description, never an administrative lookup.
    # Reject collisions instead of choosing between independently authenticated
    # identities; ordinary HTTP metadata carries no authority here.
    invalid = HTTPException(
        status_code=401,
        detail="invalid_directory_credential",
        headers={"Cache-Control": "private, no-store"},
    )
    if (
        len(request.headers.getlist("X-Curie-Directory-Credential")) != 1
        or "authorization" in request.headers
        or "cookie" in request.headers
        or "x-api-key" in request.headers
        or any(
            key.startswith("x-curie-") and key != "x-curie-directory-credential"
            for key in request.headers
        )
        or directory_credential.verify(credential or "", get_settings().api_key) is None
    ):
        raise invalid
    # Before *any* tenant/config/team/namespace read, and held until session
    # close: a committed reconfiguration cannot race an old binding answer.
    await lock_identity_reader(session)
    try:
        # Repeat cryptographic/expiry checks after any wait for the fence.
        claims = await directory_access(session, credential, None)
        config = await directory_service.configuration(session, claims.scope.tenant_id)
        team_ids = set(await session.scalars(
            select(Team.id).where(
                Team.id.in_(claims.scope.team_ids),
                Team.tenant_id == claims.scope.tenant_id,
                Team.source == "idp_group",
                Team.managed_by == claims.scope.managed_by,
            ).order_by(Team.id).with_for_update(read=True)
        ))
        if team_ids != claims.scope.team_ids:
            raise invalid
        # Reuse ingestion's active/tenant and governed-email/issuer checks.
        # Its row locks are released on session close without any mutation.
        await directory_service.scoped_namespaces(session, claims, config)
        return DirectoryBindingOut(
            tenant_id=claims.scope.tenant_id,
            issuer=config.issuer,
            managed_by=config.managed_by,
            team_ids=sorted(team_ids, key=str),
            namespace_ids=sorted(claims.namespace_ids, key=str),
            config_version=config.version,
            identity_epoch=config.identity_epoch,
            expires_at=datetime.fromtimestamp(claims.exp, UTC),
        )
    except (directory_sync.DirectorySyncError, HTTPException):
        # Do not reveal which foreign/reconfigured scope failed validation.
        raise invalid from None


@router.get(
    "/config/{tenant_id}",
    response_model=DirectoryConfigOut,
    dependencies=[Depends(require_platform_key)],
)
async def get_directory_config(tenant_id: uuid.UUID, session: SessionDep) -> DirectoryConfigOut:
    try:
        config = await directory_service.configuration(session, tenant_id)
        return DirectoryConfigOut.model_validate(config)
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None


@router.put(
    "/config/{tenant_id}",
    response_model=DirectoryConfigOut,
    dependencies=[Depends(require_platform_key)],
)
async def put_directory_config(
    tenant_id: uuid.UUID,
    body: DirectoryConfigIn,
    session: SessionDep,
) -> DirectoryConfigOut:
    try:
        config = await directory_service.update_configuration(session, tenant_id, body)
        result = DirectoryConfigOut.model_validate(config)
        await session.commit()
        return result
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None


@router.post(
    "/teams",
    response_model=DirectoryTeamOut,
    status_code=201,
    dependencies=[Depends(require_platform_key)],
)
async def provision_directory_team(body: DirectoryTeamIn, session: SessionDep) -> DirectoryTeamOut:
    try:
        config = await directory_service.configuration(session, body.tenant_id, write=True)
        team = await session.scalar(
            select(Team).where(
                Team.tenant_id == body.tenant_id,
                Team.external_id == body.external_id,
                Team.source == "idp_group",
            )
        )
        if team is None:
            team = Team(
                tenant_id=body.tenant_id,
                source="idp_group",
                external_id=body.external_id,
                name=body.name,
                managed_by=config.managed_by,
            )
            session.add(team)
            await session.flush()
            identity_audit.record(
                session,
                tenant_id=body.tenant_id,
                event_type="identity.directory_team_provisioned",
                actor="platform",
                team_id=team.id,
            )
        elif team.managed_by != config.managed_by:
            raise directory_sync.DirectorySyncError("team_out_of_scope")
        result = DirectoryTeamOut.model_validate(team)
        await session.commit()
        return result
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None


@router.post(
    "/credentials",
    response_model=DirectoryCredentialOut,
    status_code=201,
    dependencies=[Depends(require_platform_key)],
)
async def issue_directory_credential(
    body: DirectoryCredentialIn,
    session: SessionDep,
    response: Response,
) -> DirectoryCredentialOut:
    try:
        config = await directory_service.configuration(session, body.tenant_id)
        if config.version != body.expected_config_version:
            raise directory_sync.DirectoryConflict("stale_config_version")
        if len(set(body.team_ids)) != len(body.team_ids) or len(set(body.namespace_ids)) != len(
            body.namespace_ids
        ):
            raise directory_sync.DirectorySyncError("duplicate_scope")
        teams = list(
            await session.scalars(
                select(Team).where(
                    Team.id.in_(body.team_ids),
                    Team.tenant_id == body.tenant_id,
                    Team.source == "idp_group",
                    Team.managed_by == config.managed_by,
                )
            )
        )
        if len(teams) != len(body.team_ids):
            raise directory_sync.DirectorySyncError("team_out_of_scope")
        scope = directory_sync.DirectoryScope(
            body.tenant_id, config.issuer, config.managed_by, frozenset(body.team_ids)
        )
        expiry = int(time.time()) + body.expires_in_seconds
        claims = DirectoryClaims(scope, frozenset(body.namespace_ids), config.version, expiry)
        await directory_service.scoped_namespaces(session, claims, config)
        credential = directory_credential.mint(
            get_settings().api_key,
            scope=scope,
            namespace_ids=claims.namespace_ids,
            config_version=config.version,
            exp=expiry,
        )
        identity_audit.record(
            session,
            tenant_id=body.tenant_id,
            event_type="identity.directory_credential_issued",
            actor="platform",
            details={
                "config_version": config.version,
                "team_ids": sorted(map(str, scope.team_ids)),
                "namespace_ids": sorted(map(str, claims.namespace_ids)),
                "expires_at": expiry,
            },
        )
        await session.commit()
        response.headers["Cache-Control"] = "no-store"
        return DirectoryCredentialOut(
            credential=credential, expires_at=datetime.fromtimestamp(expiry, UTC)
        )
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None


@router.get("/teams/{team_id}", response_model=DirectoryTeamOut)
async def read_directory_team(
    team_id: uuid.UUID,
    session: SessionDep,
    credential: Annotated[str | None, Header(alias="X-Curie-Directory-Credential")] = None,
    x_api_key: Annotated[str | None, Header()] = None,
) -> DirectoryTeamOut:
    if verify_platform_key(x_api_key) and credential is None:
        team = await session.scalar(select(Team).where(Team.id == team_id))
    else:
        claims = await directory_access(session, credential, x_api_key)
        if team_id not in claims.scope.team_ids:
            raise HTTPException(status_code=403, detail="team_out_of_scope")
        team = await session.scalar(
            select(Team).where(
                Team.id == team_id,
                Team.tenant_id == claims.scope.tenant_id,
                Team.managed_by == claims.scope.managed_by,
                Team.source == "idp_group",
            )
        )
    if team is None:
        raise HTTPException(status_code=404, detail="team_not_found")
    return DirectoryTeamOut.model_validate(team)


@router.put("/teams/{team_id}/snapshot", response_model=DirectorySnapshotOut)
async def apply_directory_snapshot(
    team_id: uuid.UUID,
    session: SessionDep,
    access: SnapshotDep,
) -> DirectorySnapshotOut:
    claims, body = access
    try:
        config = await directory_service.authenticate(session, claims)
        result = await directory_service.ingest_snapshot(session, claims, config, team_id, body)
        await session.commit()
        return DirectorySnapshotOut(
            epoch=result.epoch, member_count=result.member_count, digest=result.digest
        )
    except directory_sync.DirectorySyncError as error:
        if not isinstance(error, directory_sync.DirectoryConflict):
            try:
                await directory_sync.record_failure(
                    session,
                    scope=claims.scope,
                    team_id=team_id,
                    expected_epoch=body.expected_epoch,
                    error="invalid_snapshot",
                )
                await session.commit()
            except directory_sync.DirectorySyncError:
                pass
        raise refusal(error) from None


@router.post("/teams/{team_id}/attempt", status_code=204)
async def record_directory_attempt(
    team_id: uuid.UUID,
    body: DirectoryAttemptIn,
    session: SessionDep,
    claims: DirectoryDep,
) -> None:
    try:
        await directory_sync.record_failure(
            session,
            scope=claims.scope,
            team_id=team_id,
            expected_epoch=body.expected_epoch,
            error=body.error,
        )
        await session.commit()
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None


@router.get("/people", response_model=DirectoryPeopleOut)
async def list_directory_people(
    session: SessionDep,
    claims: DirectoryDep,
    after_subject: Annotated[str | None, Query(max_length=512)] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> DirectoryPeopleOut:
    query = select(Principal).where(
        Principal.tenant_id == claims.scope.tenant_id,
        Principal.idp_issuer == claims.scope.issuer,
        Principal.type == "human",
    )
    if after_subject is not None:
        query = query.where(Principal.idp_subject > after_subject)
    rows = list(await session.scalars(query.order_by(Principal.idp_subject).limit(limit + 1)))
    return DirectoryPeopleOut(
        people=[
            DirectoryPersonOut(
                subject=row.idp_subject,
                status=row.status,
                authorization_version=row.authorization_version,
            )
            for row in rows[:limit]
        ],
        next_after_subject=rows[limit - 1].idp_subject if len(rows) > limit else None,
    )


@router.post("/people/status", response_model=DirectoryStatusOut)
async def change_directory_account_status(
    body: DirectoryStatusIn,
    session: SessionDep,
    claims: DirectoryDep,
) -> DirectoryStatusOut:
    try:
        principal = await session.scalar(
            select(Principal).where(
                Principal.tenant_id == claims.scope.tenant_id,
                Principal.idp_issuer == claims.scope.issuer,
                Principal.idp_subject == body.subject,
                Principal.type == "human",
            )
        )
        if principal is None:
            raise directory_sync.DirectorySyncError("unknown_subject")
        version = await directory_sync.set_account_status(
            session,
            scope=claims.scope,
            principal_id=principal.id,
            expected_version=body.expected_version,
            status=body.status,
        )
        await session.commit()
        return DirectoryStatusOut(authorization_version=version)
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None


@router.post(
    "/namespaces",
    response_model=DirectoryNamespaceOut,
    status_code=201,
    dependencies=[Depends(require_platform_key)],
)
async def provision_directory_namespace(
    body: DirectoryNamespaceIn,
    session: SessionDep,
) -> DirectoryNamespaceOut:
    try:
        config = await directory_service.configuration(session, body.tenant_id, write=True)
        valid = (
            body.kind == "slack_workspace"
            and body.provider == "slack"
            and bool(re.fullmatch(r"T[A-Z0-9]{2,63}", body.key))
        ) or (
            body.kind == "email_domain"
            and body.provider == "email"
            and body.key in config.governed_domains
            and body.authority in {"", config.issuer}
        )
        if not valid:
            raise directory_sync.DirectorySyncError("invalid_namespace")
        # Email authority is the configured IdP, never a caller-chosen mapping.
        authority = config.issuer if body.kind == "email_domain" else body.authority
        namespace = await session.scalar(
            select(IdentityNamespace).where(
                IdentityNamespace.tenant_id == body.tenant_id,
                IdentityNamespace.provider == body.provider,
                IdentityNamespace.authority == authority,
                IdentityNamespace.kind == body.kind,
                IdentityNamespace.key == body.key,
            )
        )
        if namespace is None:
            namespace = IdentityNamespace(
                tenant_id=body.tenant_id,
                provider=body.provider,
                authority=authority,
                kind=body.kind,
                key=body.key,
            )
            session.add(namespace)
            await session.flush()
            identity_audit.record(
                session,
                tenant_id=body.tenant_id,
                event_type="identity.directory_namespace_provisioned",
                actor="platform",
                details={"namespace_id": str(namespace.id)},
            )
        result = DirectoryNamespaceOut.model_validate(namespace)
        await session.commit()
        return result
    except directory_sync.DirectorySyncError as error:
        raise refusal(error) from None
