"""Admin CRUD for channel identities (#2909, ADR 0168 decision 1).

The platform key only, via ``require_platform_key``: a row decides which env
var or Secret becomes a provider credential once references are resolved, the
same class of decision as the principal and console-code mint routes, so a
future widening of ``require_api_key`` must not reach it either. The table
holds references rather than credentials, a value in a well-known credential
shape is refused, and no error body echoes a submitted value.
"""

import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from .. import channel_identities as identities
from ..auth import require_platform_key
from ..deps import SessionDep
from ..schemas.channel_identities import (
    ChannelIdentityCreate,
    ChannelIdentityOut,
    ChannelIdentityUpdate,
)
from ..schemas.common import ProviderName


class _RedactedValidationRoute(APIRoute):
    """Answer a body validation failure without the rejected input.

    FastAPI's default 422 echoes ``input`` (for a missing field, the whole
    body) and ``ctx``, so a token pasted into ``credential_ref`` would come
    straight back. Scoped to this router; every other one keeps the default.
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
    prefix="/channel-identities",
    tags=["channel-identities"],
    dependencies=[Depends(require_platform_key)],
)


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, identities.IdentityConflict):
        return HTTPException(status.HTTP_409_CONFLICT, str(exc))
    return HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc))


@router.post("", response_model=ChannelIdentityOut, status_code=status.HTTP_201_CREATED)
async def create_channel_identity(
    data: ChannelIdentityCreate, session: SessionDep
) -> ChannelIdentityOut:
    try:
        identity = await identities.create_identity(session, data)
    except (identities.IdentityConflict, identities.IdentityInvalid) as exc:
        raise _http_error(exc) from None
    return ChannelIdentityOut.model_validate(identity)


@router.get("", response_model=list[ChannelIdentityOut])
async def list_channel_identities(
    session: SessionDep,
    provider: ProviderName | None = None,
    tenant_id: uuid.UUID | None = None,
) -> list[ChannelIdentityOut]:
    rows = await identities.list_identities(session, provider=provider, tenant_id=tenant_id)
    return [ChannelIdentityOut.model_validate(row) for row in rows]


@router.get("/{identity_id}", response_model=ChannelIdentityOut)
async def get_channel_identity(identity_id: uuid.UUID, session: SessionDep) -> ChannelIdentityOut:
    identity = await identities.get_identity(session, identity_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "channel identity not found")
    return ChannelIdentityOut.model_validate(identity)


@router.patch("/{identity_id}", response_model=ChannelIdentityOut)
async def update_channel_identity(
    identity_id: uuid.UUID, data: ChannelIdentityUpdate, session: SessionDep
) -> ChannelIdentityOut:
    identity = await identities.get_identity(session, identity_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "channel identity not found")
    try:
        identity = await identities.update_identity(session, identity, data)
    except (identities.IdentityConflict, identities.IdentityInvalid) as exc:
        raise _http_error(exc) from None
    return ChannelIdentityOut.model_validate(identity)


@router.delete("/{identity_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_channel_identity(identity_id: uuid.UUID, session: SessionDep) -> None:
    identity = await identities.get_identity(session, identity_id)
    if identity is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "channel identity not found")
    await identities.delete_identity(session, identity)
