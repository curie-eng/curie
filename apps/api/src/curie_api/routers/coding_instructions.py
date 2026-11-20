"""Principal session owner surface for coding instructions (#5590)."""

from __future__ import annotations

import json
from collections.abc import Callable, Coroutine
from typing import Annotated, Any

from curie_internal.requester_coding_instructions import (
    MAX_REVISION,
    InvalidCodingInstructions,
)
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError
from starlette.exceptions import HTTPException as StarletteHTTPException

from .. import requester_coding_profiles as profiles
from ..approval_auth import enforce_console_cookie_origin
from ..auth import require_principal_session
from ..deps import SessionDep
from ..models import Principal
from ..schemas.coding_instructions import CodingInstructionsOut, CodingInstructionsWrite
from ..wirebody import read_bounded_body
from .personal_enrollments import FOREIGN_CREDENTIAL_HEADERS, UNAUTHENTICATED

NO_STORE = {"Cache-Control": "no-store"}
MAX_REQUEST_BYTES = 131_072


def _refusal(status: int, code: str, **details: int) -> HTTPException:
    return HTTPException(status, {"code": code, **details}, headers=NO_STORE)


class CodingInstructionsRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacted(request: Request) -> Response:
            try:
                if request.method == "PUT":
                    body = await read_bounded_body(
                        request, MAX_REQUEST_BYTES, subject="coding instructions body"
                    )
                    request._body = body
                    json.loads(body.decode("utf-8", errors="strict"))
                response = await handler(request)
            except (RequestValidationError, ValidationError, UnicodeError, json.JSONDecodeError):
                raise _refusal(422, "invalid_text") from None
            except InvalidCodingInstructions as error:
                raise _refusal(422, error.code) from None
            except StarletteHTTPException as error:
                if error.status_code == 413:
                    raise _refusal(413, "too_large") from None
                if error.status_code == 400:
                    raise _refusal(422, "invalid_text") from None
                error.headers = {**(error.headers or {}), **NO_STORE}
                raise
            response.headers.update(NO_STORE)
            return response

        return redacted


async def _session_only(request: Request) -> None:
    if any(name in request.headers for name in (*FOREIGN_CREDENTIAL_HEADERS, "Authorization")):
        raise HTTPException(401, UNAUTHENTICATED, headers=NO_STORE)


def _same_origin(request: Request) -> None:
    enforce_console_cookie_origin(request)


OwnerDep = Annotated[Principal, Depends(require_principal_session)]
SameOriginDep = Annotated[None, Depends(_same_origin)]
router = APIRouter(
    prefix="/coding-instructions",
    tags=["coding-instructions"],
    route_class=CodingInstructionsRoute,
    dependencies=[Depends(_session_only)],
)


@router.get("", response_model=CodingInstructionsOut)
async def read(principal: OwnerDep, session: SessionDep) -> CodingInstructionsOut:
    return await profiles.read_profile(
        session, tenant_id=principal.tenant_id, principal_id=principal.id
    )


async def _mutate(
    session: SessionDep, principal: Principal, value: str, expected_revision: int
) -> CodingInstructionsOut:
    try:
        response = await profiles.write_profile(
            session,
            tenant_id=principal.tenant_id,
            principal_id=principal.id,
            value=value,
            expected_revision=expected_revision,
        )
        await session.commit()
        return response
    except profiles.ProfileRefusal as error:
        await session.rollback()
        detail = (
            {} if error.current_revision is None else {"current_revision": error.current_revision}
        )
        raise _refusal(409, error.code, **detail) from None
    except DBAPIError:
        await session.rollback()
        raise _refusal(503, "coding_instructions_unavailable") from None


@router.put("", response_model=CodingInstructionsOut)
async def write(
    body: CodingInstructionsWrite,
    principal: OwnerDep,
    _origin: SameOriginDep,
    session: SessionDep,
) -> CodingInstructionsOut:
    return await _mutate(session, principal, body.text, body.expected_revision)


@router.delete("", response_model=CodingInstructionsOut)
async def clear(
    principal: OwnerDep,
    _origin: SameOriginDep,
    session: SessionDep,
    expected_revision: Annotated[int, Query(ge=0, le=MAX_REVISION)],
) -> CodingInstructionsOut:
    return await _mutate(session, principal, "", expected_revision)
