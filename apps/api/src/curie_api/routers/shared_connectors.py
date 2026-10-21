"""Shared connector store administration (ADR 0239).

Two routers over one store. ``router`` is the operator surface and accepts only
the platform key (``require_platform_key``, never a console session), as the
identity routes do. ``internal_router`` accepts only the internal worker token:
the worker registers the public half of each application key it generates and
reports whether the resolver's database role is ready. Neither credential is
accepted on the other's routes.

Every response from both routers carries ``Cache-Control: no-store``, and no
422 echoes a submitted value: the bodies carry sealed credentials and key
material. A malformed key registration answers one fixed code.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Coroutine
from typing import Annotated, Any, ClassVar

from fastapi import APIRouter, Depends, HTTPException, Path, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from ..auth import require_internal_worker_token, require_platform_key
from ..deps import SessionDep
from ..schemas.shared_connectors import (
    HEX64_PATTERN,
    SharedApplicationDisabled,
    SharedApplicationOut,
    SharedApplicationRegistration,
    SharedBindingCheck,
    SharedBindingCheckOut,
    SharedBindingDisabled,
    SharedBindingImport,
    SharedBindingOut,
    SharedBindingReimport,
    SharedBundledDatabaseOut,
    SharedDatabaseProvision,
    SharedDatabaseRotate,
    SharedDatabaseStatusOut,
    SharedDatabaseStatusReport,
    SharedManagedDatabaseOut,
    SharedMembershipOut,
    SharedMembershipReplace,
    SharedRecipientRegistered,
    SharedRecipientRegistration,
    SharedRecipientRetired,
)
from ..shared_connectors import database_roles, store
from ..shared_connectors.store import NO_STORE, SharedRefusal


class _SharedConnectorRoute(APIRoute):
    """No caching of any answer, and no submitted value in a 422.

    FastAPI's default 422 echoes ``input`` and ``ctx``; here that could be a
    ciphertext or a private key sent by mistake. A route that sets
    ``invalid_body_detail`` answers every body validation failure with that
    one code instead of an error list.
    """

    invalid_body_detail: ClassVar[str | None] = None

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()
        fixed = self.invalid_body_detail

        async def no_store_handler(request: Request) -> Response:
            try:
                response = await handler(request)
            except RequestValidationError as exc:
                errors = exc.errors()
                if fixed is not None and any(error["loc"][:1] == ("body",) for error in errors):
                    content: dict[str, Any] = {"detail": fixed}
                else:
                    content = {
                        "detail": jsonable_encoder(
                            [
                                {key: error[key] for key in ("type", "loc", "msg") if key in error}
                                for error in errors
                            ]
                        )
                    }
                response = JSONResponse(content, status_code=status.HTTP_422_UNPROCESSABLE_CONTENT)
            except HTTPException as exc:
                exc.headers = {**(exc.headers or {}), **NO_STORE}
                raise
            response.headers.update(NO_STORE)
            return response

        return no_store_handler


class _RecipientKeyRoute(_SharedConnectorRoute):
    invalid_body_detail = "shared_recipient_key_invalid"


router = APIRouter(
    route_class=_SharedConnectorRoute,
    prefix="/shared-connectors",
    tags=["shared-connectors"],
    dependencies=[Depends(require_platform_key)],
)

internal_router = APIRouter(
    route_class=_SharedConnectorRoute,
    prefix="/v1/internal/shared-connectors",
    tags=["internal-shared-connectors"],
    dependencies=[Depends(require_internal_worker_token)],
)

RecipientId = Annotated[str, Path(pattern=HEX64_PATTERN)]


def _not_found(code: str) -> SharedRefusal:
    return SharedRefusal(status.HTTP_404_NOT_FOUND, code)


# --- applications ----------------------------------------------------------------


@router.post(
    "/applications", response_model=SharedApplicationOut, status_code=status.HTTP_201_CREATED
)
async def register_shared_application(
    data: SharedApplicationRegistration, response: Response, session: SessionDep
) -> SharedApplicationOut:
    """201 for a new application; 200 with the same record for an identical replay."""

    record, created = await store.register_application(session, data)
    if not created:
        response.status_code = status.HTTP_200_OK
    return record


@router.get("/applications/{application_id}", response_model=SharedApplicationOut)
async def get_shared_application(
    application_id: uuid.UUID, session: SessionDep
) -> SharedApplicationOut:
    record = await store.application_record(session, application_id)
    if record is None:
        raise _not_found("shared_application_not_found")
    return record


@router.post("/applications/{application_id}/disable", response_model=SharedApplicationDisabled)
async def disable_shared_application(
    application_id: uuid.UUID, session: SessionDep
) -> SharedApplicationDisabled:
    """Answers only after the disable commits; later resolves refuse."""

    return await store.disable_application(session, application_id)


@router.put("/applications/{application_id}/members/{agent_id}", response_model=SharedMembershipOut)
async def replace_shared_members(
    application_id: uuid.UUID,
    agent_id: uuid.UUID,
    data: SharedMembershipReplace,
    session: SessionDep,
) -> SharedMembershipOut:
    return await store.replace_members(session, application_id, agent_id, data.aliases)


@router.delete(
    "/applications/{application_id}/members/{agent_id}", response_model=SharedMembershipOut
)
async def remove_shared_member(
    application_id: uuid.UUID, agent_id: uuid.UUID, session: SessionDep
) -> SharedMembershipOut:
    return await store.remove_member(session, application_id, agent_id)


@router.post(
    "/applications/{application_id}/database",
    response_model=SharedBundledDatabaseOut | SharedManagedDatabaseOut,
    status_code=status.HTTP_201_CREATED,
)
async def provision_shared_database(
    application_id: uuid.UUID, data: SharedDatabaseProvision, session: SessionDep
) -> SharedBundledDatabaseOut | SharedManagedDatabaseOut:
    return await database_roles.provision_database(session, application_id, data)


@router.post(
    "/applications/{application_id}/database/rotate", response_model=SharedBundledDatabaseOut
)
async def rotate_shared_database(
    application_id: uuid.UUID, data: SharedDatabaseRotate, session: SessionDep
) -> SharedBundledDatabaseOut:
    return await database_roles.rotate_database(session, application_id, data.expected_dsn_revision)


@router.post(
    "/applications/{application_id}/recipients/{recipient_id}/retire",
    response_model=SharedRecipientRetired,
)
async def retire_shared_recipient(
    application_id: uuid.UUID, recipient_id: RecipientId, session: SessionDep
) -> SharedRecipientRetired:
    return await store.retire_recipient(session, application_id, recipient_id)


# --- bindings ------------------------------------------------------------------


@router.post(
    "/applications/{application_id}/bindings",
    response_model=SharedBindingOut,
    status_code=status.HTTP_201_CREATED,
)
async def import_shared_binding(
    application_id: uuid.UUID, data: SharedBindingImport, session: SessionDep
) -> SharedBindingOut:
    return await store.import_binding(session, application_id, data)


@router.get("/bindings/{binding_id}", response_model=SharedBindingOut)
async def get_shared_binding(binding_id: uuid.UUID, session: SessionDep) -> SharedBindingOut:
    return await store.binding_record(session, binding_id)


@router.post("/bindings/{binding_id}/reimport", response_model=SharedBindingOut)
async def reimport_shared_binding(
    binding_id: uuid.UUID, data: SharedBindingReimport, session: SessionDep
) -> SharedBindingOut:
    return await store.reimport_binding(session, binding_id, data)


@router.post("/bindings/{binding_id}/check", response_model=SharedBindingCheckOut)
async def check_shared_binding(
    binding_id: uuid.UUID, data: SharedBindingCheck, session: SessionDep
) -> SharedBindingCheckOut:
    return await store.check_binding(session, binding_id, data)


@router.post("/bindings/{binding_id}/disable", response_model=SharedBindingDisabled)
async def disable_shared_binding(
    binding_id: uuid.UUID, session: SessionDep
) -> SharedBindingDisabled:
    """Answers only after the disable commits; later resolves refuse."""

    return await store.disable_binding(session, binding_id)


# --- worker ----------------------------------------------------------------------


async def register_shared_recipient(
    application_id: uuid.UUID,
    data: SharedRecipientRegistration,
    response: Response,
    session: SessionDep,
) -> SharedRecipientRegistered:
    """201 for a new key; 200 for a replay of a live one, current or not."""

    registered, created = await store.register_recipient(session, application_id, data)
    if not created:
        response.status_code = status.HTTP_200_OK
    return registered


# Registered by hand for its own route class: any malformed body, a private
# key field included, answers only ``shared_recipient_key_invalid``.
internal_router.add_api_route(
    "/applications/{application_id}/recipients",
    register_shared_recipient,
    methods=["POST"],
    response_model=SharedRecipientRegistered,
    status_code=status.HTTP_201_CREATED,
    route_class_override=_RecipientKeyRoute,
)


@internal_router.put(
    "/applications/{application_id}/database/status", response_model=SharedDatabaseStatusOut
)
async def report_shared_database_status(
    application_id: uuid.UUID, data: SharedDatabaseStatusReport, session: SessionDep
) -> SharedDatabaseStatusOut:
    return await database_roles.report_status(session, application_id, data)
