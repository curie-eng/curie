"""Internal creation transport. @spec HTTP-APPROVAL-CONTINUITY-3."""

from collections.abc import Callable, Coroutine
from typing import Any

from curie_internal.identity_approval import ApprovalCreationRequestV1, ApprovalCreationResponseV1
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from curie_api.auth import require_internal_worker_token
from curie_api.identity.http_approval import create_http_approval, decode_creation_json
from curie_api.wirebody import read_bounded_body


class _CreationRoute(APIRoute):
    """@spec HTTP-APPROVAL-CONTINUITY-2/3."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """@spec HTTP-APPROVAL-CONTINUITY-3."""
        handler = super().get_route_handler()

        async def bounded(request: Request) -> Response:
            """@spec HTTP-APPROVAL-CONTINUITY-3."""
            try:
                raw = await read_bounded_body(request, 2_097_152, subject="approval creation body")
                request._body = raw
                request._json = decode_creation_json(raw, max_bytes=2_097_152)
                response = await handler(request)
            except HTTPException as error:
                error.headers = {**(error.headers or {}), "Cache-Control": "no-store"}
                if error.status_code == 413:
                    error.detail = "body_too_large"
                raise
            except (ValueError, TypeError, RecursionError, RequestValidationError):
                return JSONResponse(
                    {"detail": "invalid_approval_context"},
                    status_code=422,
                    headers={"Cache-Control": "no-store"},
                )
            response.headers["Cache-Control"] = "no-store"
            return response

        return bounded


router = APIRouter(
    prefix="/v1/internal/identity/approvals",
    tags=["internal-identity-authority"],
    route_class=_CreationRoute,
    dependencies=[Depends(require_internal_worker_token)],
)


@router.post("", response_model=ApprovalCreationResponseV1, status_code=201)
async def create_identity_approval(
    request: Request,
    body: ApprovalCreationRequestV1,
    response: Response,
) -> ApprovalCreationResponseV1:
    """@spec HTTP-APPROVAL-CONTINUITY-3."""
    result = await create_http_approval(request, body)
    response.status_code = 200 if result.duplicate else 201
    return result
