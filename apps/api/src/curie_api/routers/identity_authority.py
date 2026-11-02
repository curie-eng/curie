"""Worker current-authority boundary. @spec HTTP-INVOCATION-EXECUTION-8/16.

Resume contexts and exact approval grants share the same boundary.
@spec HTTP-APPROVAL-CONTINUITY-12. The worker asks its per-identity binding
classification here too. @spec HTTP-INVOCATION-EXECUTION-22.
"""

import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from curie_internal.identity_approval import (
    ApprovalGrantAuthorityRequestV1,
    ApprovalGrantAuthorityResponseV1,
)
from curie_internal.identity_authority import (
    BindingClassificationRequestV1,
    BindingClassificationResponseV1,
    TurnAuthorityRequestV1,
    TurnAuthorityResponseV1,
)
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from curie_api.auth import require_internal_worker_token
from curie_api.deps import SessionDep
from curie_api.identity.http_invocation import binding_requires_http_identity
from curie_api.identity.http_resume import (
    check_current_turn_authority,
    check_http_approval_grant,
)
from curie_api.models import AgentChannel
from curie_api.schemas.http_identity import decode_json_bytes
from curie_api.wirebody import read_bounded_body

_MAX_BODY_BYTES = 65_536
_NO_STORE = {"Cache-Control": "no-store"}


class _BoundedJsonRoute(APIRoute):
    """Bounded, strictly decoded JSON before authentication, with fixed refusals.

    Subclasses name the body subject, the fixed 422 detail, and optionally the
    fixed detail that replaces the 413 one. Excessive nesting is a 422, never a 500.
    """

    subject: str
    invalid_detail: str
    oversize_detail: str | None = None

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        handler = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            """@spec HTTP-INVOCATION-EXECUTION-16."""
            try:
                raw = await read_bounded_body(request, _MAX_BODY_BYTES, subject=self.subject)
                request._body = raw
                request._json = decode_json_bytes(raw, max_bytes=_MAX_BODY_BYTES)
                response = await handler(request)
            except HTTPException as error:
                error.headers = {**(error.headers or {}), **_NO_STORE}
                if error.status_code == 413 and self.oversize_detail:
                    error.detail = self.oversize_detail
                raise
            except (ValueError, TypeError, RecursionError, RequestValidationError):
                return JSONResponse(
                    {"detail": self.invalid_detail}, status_code=422, headers=_NO_STORE,
                )
            response.headers.update(_NO_STORE)
            return response

        return guarded


class _CurrentAuthorityRoute(_BoundedJsonRoute):
    """@spec HTTP-INVOCATION-EXECUTION-8/16."""

    subject = "turn authority body"
    invalid_detail = "invalid_turn_authority_request"


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


class _ApprovalGrantRoute(_BoundedJsonRoute):
    """Strict bound before parsing; fixed 413/422. @spec HTTP-APPROVAL-CONTINUITY-12."""

    subject = "approval grant body"
    invalid_detail = "invalid_approval_grant_request"
    oversize_detail = "body_too_large"


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


class _BindingClassificationRoute(_BoundedJsonRoute):
    """Strict bound before parsing; fixed 422. @spec HTTP-INVOCATION-EXECUTION-22."""

    subject = "binding classification body"
    invalid_detail = "invalid_binding_classification_request"



classification_router = APIRouter(
    prefix="/v1/internal/identity/binding-classification",
    tags=["internal-identity-authority"],
    route_class=_BindingClassificationRoute,
    dependencies=[Depends(require_internal_worker_token)],
)


@classification_router.post("/check", response_model=BindingClassificationResponseV1)
async def check_binding_classification(
    request: Request,
    body: BindingClassificationRequestV1,
    session: SessionDep,
) -> BindingClassificationResponseV1:
    """The worker's per-identity question. Writes and grants nothing.

    @spec HTTP-INVOCATION-EXECUTION-22.
    """
    scope = body.scope
    binding = await session.get(AgentChannel, uuid.UUID(scope.binding_id))
    if (
        binding is None
        or str(binding.tenant_id) != scope.tenant_id
        or str(binding.agent_id) != scope.agent_id
        or binding.generation != scope.binding_generation
    ):
        raise HTTPException(409, "binding_scope_changed")
    required = await binding_requires_http_identity(
        session, getattr(request.app.state, "http_identity_runtime", None), binding,
    )
    return BindingClassificationResponseV1(
        schema_version=1, request_id=body.request_id, scope=scope,
        http_identity_required=required,
    )
