"""Worker current-authority boundary. @spec HTTP-INVOCATION-EXECUTION-8/16.

Resume contexts and exact approval grants share the same boundary.
@spec HTTP-APPROVAL-CONTINUITY-12.
"""

from collections.abc import Callable, Coroutine
from typing import Any

from curie_internal.identity_approval import (
    ApprovalGrantAuthorityRequestV1,
    ApprovalGrantAuthorityResponseV1,
)
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityResponseV1
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from curie_api.auth import require_internal_worker_token
from curie_api.identity.http_resume import (
    check_current_turn_authority,
    check_http_approval_grant,
)
from curie_api.schemas.http_identity import decode_json_bytes
from curie_api.wirebody import read_bounded_body

_MAX_BODY_BYTES = 65_536
_NO_STORE = {"Cache-Control": "no-store"}


class _CurrentAuthorityRoute(APIRoute):
    """@spec HTTP-INVOCATION-EXECUTION-8/16."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        handler = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            """@spec HTTP-INVOCATION-EXECUTION-16."""
            try:
                raw = await read_bounded_body(
                    request, _MAX_BODY_BYTES, subject="turn authority body",
                )
                request._body = raw
                request._json = decode_json_bytes(raw, max_bytes=_MAX_BODY_BYTES)
                response = await handler(request)
            except HTTPException as error:
                error.headers = {**(error.headers or {}), **_NO_STORE}
                raise
            except (ValueError, TypeError, RequestValidationError):
                return JSONResponse(
                    {"detail": "invalid_turn_authority_request"},
                    status_code=422, headers=_NO_STORE,
                )
            response.headers.update(_NO_STORE)
            return response

        return guarded


router = APIRouter(
    prefix="/v1/internal/identity/turn-authority", tags=["internal-identity-authority"],
    route_class=_CurrentAuthorityRoute, dependencies=[Depends(require_internal_worker_token)],
)


@router.post("/check", response_model=TurnAuthorityResponseV1)
async def check_turn_authority(
    request: Request, body: TurnAuthorityRequestV1,
) -> TurnAuthorityResponseV1:
    """@spec HTTP-INVOCATION-EXECUTION-8/16."""
    return await check_current_turn_authority(request, body)


class _ApprovalGrantRoute(APIRoute):
    """Strict bound before parsing; fixed 413/422. @spec HTTP-APPROVAL-CONTINUITY-12."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        handler = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            """@spec HTTP-APPROVAL-CONTINUITY-12."""
            try:
                raw = await read_bounded_body(
                    request, _MAX_BODY_BYTES, subject="approval grant body",
                )
                request._body = raw
                request._json = decode_json_bytes(raw, max_bytes=_MAX_BODY_BYTES)
                response = await handler(request)
            except HTTPException as error:
                error.headers = {**(error.headers or {}), **_NO_STORE}
                if error.status_code == 413:
                    error.detail = "body_too_large"
                raise
            except (ValueError, TypeError, RecursionError, RequestValidationError):
                return JSONResponse(
                    {"detail": "invalid_approval_grant_request"},
                    status_code=422, headers=_NO_STORE,
                )
            response.headers.update(_NO_STORE)
            return response

        return guarded


grant_router = APIRouter(
    prefix="/v1/internal/identity/approval-grant", tags=["internal-identity-authority"],
    route_class=_ApprovalGrantRoute, dependencies=[Depends(require_internal_worker_token)],
)


@grant_router.post("/check", response_model=ApprovalGrantAuthorityResponseV1)
async def check_approval_grant(
    request: Request, body: ApprovalGrantAuthorityRequestV1,
) -> ApprovalGrantAuthorityResponseV1:
    """@spec HTTP-APPROVAL-CONTINUITY-12."""
    return await check_http_approval_grant(request, body)
