"""Admin CRUD for provider installations (#2909, ADR 0155 step 4).

Platform key only, like every other administrative router. The table holds
references rather than credentials, a value in a well-known credential shape
is refused, and no error body echoes a submitted value.
"""

import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from .. import provider_installations as installations
from ..auth import require_api_key
from ..deps import SessionDep
from ..schemas import (
    ProviderInstallationCreate,
    ProviderInstallationOut,
    ProviderInstallationUpdate,
    ProviderName,
)


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
    prefix="/provider-installations",
    tags=["provider-installations"],
    dependencies=[Depends(require_api_key)],
)


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, installations.InstallationConflict):
        return HTTPException(status.HTTP_409_CONFLICT, str(exc))
    return HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc))


@router.post("", response_model=ProviderInstallationOut, status_code=status.HTTP_201_CREATED)
async def create_provider_installation(
    data: ProviderInstallationCreate, session: SessionDep
) -> ProviderInstallationOut:
    try:
        installation = await installations.create_installation(session, data)
    except (installations.InstallationConflict, installations.InstallationInvalid) as exc:
        raise _http_error(exc) from None
    return ProviderInstallationOut.model_validate(installation)


@router.get("", response_model=list[ProviderInstallationOut])
async def list_provider_installations(
    session: SessionDep,
    provider: ProviderName | None = None,
    tenant_id: uuid.UUID | None = None,
) -> list[ProviderInstallationOut]:
    rows = await installations.list_installations(session, provider=provider, tenant_id=tenant_id)
    return [ProviderInstallationOut.model_validate(row) for row in rows]


@router.get("/{installation_id}", response_model=ProviderInstallationOut)
async def get_provider_installation(
    installation_id: uuid.UUID, session: SessionDep
) -> ProviderInstallationOut:
    installation = await installations.get_installation(session, installation_id)
    if installation is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "provider installation not found")
    return ProviderInstallationOut.model_validate(installation)


@router.patch("/{installation_id}", response_model=ProviderInstallationOut)
async def update_provider_installation(
    installation_id: uuid.UUID, data: ProviderInstallationUpdate, session: SessionDep
) -> ProviderInstallationOut:
    installation = await installations.get_installation(session, installation_id)
    if installation is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "provider installation not found")
    try:
        installation = await installations.update_installation(session, installation, data)
    except (installations.InstallationConflict, installations.InstallationInvalid) as exc:
        raise _http_error(exc) from None
    return ProviderInstallationOut.model_validate(installation)


@router.delete("/{installation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_provider_installation(installation_id: uuid.UUID, session: SessionDep) -> None:
    installation = await installations.get_installation(session, installation_id)
    if installation is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "provider installation not found")
    try:
        await installations.delete_installation(session, installation)
    except installations.InstallationConflict as exc:
        raise _http_error(exc) from None
