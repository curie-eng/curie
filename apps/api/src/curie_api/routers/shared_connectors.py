"""Shared connector store administration (ADR 0239).

Two routers over one store. ``router`` is the operator surface and accepts only
the platform key (``require_platform_key``, never a console session), as the
identity routes do. ``internal_router`` accepts only the internal worker token:
the worker registers the public half of each application key it generates,
reports whether the resolver's database role is ready, reads each application's
frozen render with its Secret plan, and resolves the shared entries of the
bundles its own readers parse. Neither credential is accepted on the other's
routes.

Every response from both routers carries ``Cache-Control: no-store``, and no
422 echoes a submitted value: the bodies carry sealed credentials and key
material. A malformed key registration answers one fixed code.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Coroutine, Iterator, Mapping
from typing import Annotated, Any, ClassVar, get_args, get_origin

from curie_internal.shared_declarations import SharedEntry
from fastapi import APIRouter, Depends, HTTPException, Path, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel

from ..auth import require_internal_worker_token, require_platform_key
from ..deps import SessionDep
from ..schemas.shared_connectors import (
    HEX64_PATTERN,
    SharedApplicationDisabled,
    SharedApplicationOut,
    SharedApplicationRegistration,
    SharedApplicationRenderOut,
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
    SharedResolveOut,
    SharedResolveRequest,
)
from ..shared_connectors import database_roles, declarations, store
from ..shared_connectors.application_render import application_render
from ..shared_connectors.store import NO_STORE

_REQUEST_MODELS: tuple[type[BaseModel], ...] = (
    SharedApplicationRegistration,
    SharedBindingCheck,
    SharedBindingImport,
    SharedBindingReimport,
    SharedDatabaseProvision,
    SharedDatabaseRotate,
    SharedDatabaseStatusReport,
    SharedMembershipReplace,
    SharedRecipientRegistration,
    SharedResolveRequest,
)
_LOC_ROOTS = frozenset({"body", "path", "query", "header", "cookie"})
_REDACTED_LOC = "<redacted>"


def _nested_models(annotation: object) -> Iterator[type[BaseModel]]:
    """Every model named anywhere in a field type: lists, unions, optionals, Annotated."""

    if (
        get_origin(annotation) is None
        and isinstance(annotation, type)
        and issubclass(annotation, BaseModel)
    ):
        yield annotation
    for argument in get_args(annotation):
        yield from _nested_models(argument)


def _declared_field_names(models: tuple[type[BaseModel], ...]) -> frozenset[str]:
    """Field names of ``models`` and of every model nested in them; none declares an alias."""

    names: set[str] = set()
    seen: set[type[BaseModel]] = set()
    pending = list(models)
    while pending:
        model = pending.pop()
        if model in seen:
            continue
        seen.add(model)
        for name, field in model.model_fields.items():
            names.add(name)
            pending.extend(_nested_models(field.annotation))
    return frozenset(names)


# Computed once: the only key names a 422 may repeat. Pydantic reports a rejected
# key (``extra_forbidden``) by its name in ``loc``, and that name is request input.
_DECLARED_LOC_NAMES = _declared_field_names(_REQUEST_MODELS)


def _redacted_error(error: Mapping[str, Any], declared: frozenset[str]) -> dict[str, Any]:
    """``type``, ``msg`` and a ``loc`` holding only names the server declared."""

    safe_loc: list[str | int] = []
    for index, segment in enumerate(error.get("loc", ())):
        if index == 0:
            keep = segment in _LOC_ROOTS
        else:
            keep = isinstance(segment, int) or segment in declared
        safe_loc.append(segment if keep else _REDACTED_LOC)
    return {"type": error.get("type"), "loc": safe_loc, "msg": error.get("msg")}


class _SharedConnectorRoute(APIRoute):
    """No caching of any answer, and no submitted value in a 422.

    FastAPI's default 422 echoes ``input`` and ``ctx``; here that could be a
    ciphertext or a private key sent by mistake. Each error keeps ``type`` and
    ``msg`` only, and its ``loc`` keeps indexes and the declared field or
    parameter names, since a rejected key's own name is request input too. A
    route that sets ``invalid_body_detail`` answers every body validation
    failure with that one code instead of an error list.
    """

    invalid_body_detail: ClassVar[str | None] = None

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()
        fixed = self.invalid_body_detail
        dependant = self.dependant
        declared = _DECLARED_LOC_NAMES | frozenset(
            parameter.alias
            for parameter in (
                *dependant.path_params,
                *dependant.query_params,
                *dependant.header_params,
                *dependant.cookie_params,
            )
        )

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
                            [_redacted_error(error, declared) for error in errors]
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
    return await store.application_record(session, application_id)


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


@internal_router.get(
    "/applications/{application_id}/render", response_model=SharedApplicationRenderOut
)
async def render_shared_application(
    application_id: uuid.UUID, namespace: str, release: str, session: SessionDep
) -> SharedApplicationRenderOut:
    """The frozen application render and its Secret plan, at the worker's root."""

    return await application_render(session, application_id, namespace=namespace, release=release)


@internal_router.post("/resolve", response_model=SharedResolveOut)
async def resolve_shared_declarations(
    data: SharedResolveRequest, session: SessionDep
) -> SharedResolveOut:
    """Resolve shared entries for one agent; the first refusal answers in its dict form."""

    entries = [
        SharedEntry(name=entry.name, application=str(entry.application)) for entry in data.entries
    ]
    resolved = await declarations.resolve(session, data.agent_id, entries)
    return SharedResolveOut.model_validate(
        {"agent_id": data.agent_id, "entries": declarations.entries_body(resolved)}
    )
