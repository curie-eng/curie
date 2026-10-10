"""Principal resolution and Slack identity reports (#2910, #3039, ADR 0198).

The platform key only, via ``require_platform_key``: the dispatcher is the
caller of both. A report attaches an identity to an installation, which the
platform key already allows through the channel identity PATCH, so the report
adds no privilege; resolution reads which principal a provider user is. An
unresolved answer is still a 200 carrying its reason, and no 422 here echoes a
submitted value.

Until migration 0102 is applied these routes answer 500, as #3040's did below
0083 (an expand-only window); the dispatcher's reporter retries.
"""

from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from ..auth import require_platform_key
from ..channel_identities import DEFAULT_TENANT_ID, IdentityInvalid
from ..deps import SessionDep
from ..identity.attach import IdentityNotFound, report_slack_identity
from ..identity.service import resolve_principal
from ..schemas.identity import (
    PrincipalResolutionOut,
    PrincipalResolveIn,
    SlackReportIn,
    SlackReportOut,
)


class _RedactedValidationRoute(APIRoute):
    """Answer a body validation failure without the rejected input.

    FastAPI's default 422 echoes ``input`` and ``ctx``; these bodies carry
    provider user and team ids, which the caller's logs should not get back.
    Scoped to this router, as on the channel identity routes.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacting_handler(request: Request) -> Response:
            try:
                return await handler(request)
            except RequestValidationError as exc:
                detail = [
                    {key: error[key] for key in ("type", "loc", "msg") if key in error}
                    for error in exc.errors()
                ]
                return JSONResponse(
                    {"detail": jsonable_encoder(detail)},
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                )

        return redacting_handler


router = APIRouter(
    route_class=_RedactedValidationRoute,
    prefix="/identity",
    tags=["identity"],
    dependencies=[Depends(require_platform_key)],
)


@router.post("/resolve", response_model=PrincipalResolutionOut)
async def resolve(data: PrincipalResolveIn, session: SessionDep) -> PrincipalResolutionOut:
    resolution = await resolve_principal(
        session,
        tenant_id=data.tenant_id or DEFAULT_TENANT_ID,
        provider=data.provider,
        channel_identity=data.channel_identity,
        slack=data.slack,
    )
    return PrincipalResolutionOut(
        status=resolution.status,
        reason=resolution.reason,
        principal_id=resolution.principal_id,
        channel_identity_id=resolution.channel_identity_id,
        namespace_id=resolution.namespace_id,
    )


@router.post("/slack-reports", response_model=SlackReportOut)
async def slack_report(data: SlackReportIn, session: SessionDep) -> SlackReportOut:
    try:
        result = await report_slack_identity(
            session,
            tenant_id=data.tenant_id or DEFAULT_TENANT_ID,
            name=data.name,
            team_id=data.team_id,
            enterprise_id=data.enterprise_id,
            enterprise_id_present=data.enterprise_id_present,
            is_enterprise_install=data.is_enterprise_install,
        )
    except IdentityNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "channel identity not found") from None
    except IdentityInvalid as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from None
    return SlackReportOut(
        identity_id=result.identity_id,
        provider_installation_id=result.provider_installation_id,
        namespace_id=result.namespace_id,
        installation_mismatch=result.installation_mismatch,
    )
