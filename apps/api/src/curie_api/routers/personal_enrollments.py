"""Owner routes for a personal Claude enrollment (#5377, ADR 0268 section 3).

``GET``, ``PUT`` and ``DELETE /personal-claude/enrollment`` act on the caller's
own enrollment. The only credential is a principal console session
(``auth.require_principal_session``); tenant and principal come from it, never
from the request. A request that also presents the platform key or any other
Curie credential header is refused 401, never resolved by precedence. ``PUT``
and ``DELETE`` also require a same-host ``Origin`` (or ``Referer``), since the
cookie is ``SameSite=Strict`` but a same-site sibling form is not stopped by it.

``EnrollmentRoute`` maps every validation failure to 422
``invalid_enrollment_request`` without echoing input, so the grant never
reaches a response or a traceback, and every answer carries
``Cache-Control: no-store``. No response names the Secret namespace, name, key
or the grant.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import ValidationError

from .. import personal_enrollments, rollout_hold
from ..approval_auth import (
    ADAPTER_PRINCIPAL_HEADER,
    APPROVAL_PRINCIPAL_HEADER,
    enforce_console_cookie_origin,
)
from ..auth import API_KEY_HEADER, require_principal_session
from ..config import get_settings
from ..deps import SessionDep
from ..models import Principal
from ..personal_custody_reconciler import custody_for
from ..personal_enrollments import Custody, EnrollmentRefusal, EnrollmentStatus
from ..schemas.personal_enrollments import (
    PersonalEnrollmentOut,
    PersonalEnrollmentTeardownOut,
    PersonalEnrollmentWrite,
)

NO_STORE = "no-store"
UNAUTHENTICATED = "missing, invalid, or expired principal session"
WORKER_TOKEN_HEADER = "X-Curie-Worker-Token"
#: Credentials this owner surface never accepts, alone or beside the session.
FOREIGN_CREDENTIAL_HEADERS = (
    API_KEY_HEADER,
    APPROVAL_PRINCIPAL_HEADER,
    ADAPTER_PRINCIPAL_HEADER,
    WORKER_TOKEN_HEADER,
)
_REFUSAL_STATUS = {
    "custody_unavailable": status.HTTP_503_SERVICE_UNAVAILABLE,
    "enrollment_busy": status.HTTP_503_SERVICE_UNAVAILABLE,
}


def _refusal(status_code: int, code: str) -> HTTPException:
    return HTTPException(status_code, {"code": code}, headers={"Cache-Control": NO_STORE})


class EnrollmentRoute(APIRoute):
    """A copy of ``identity_operator.OperatorRoute``: no input echo, never cached."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacted(request: Request) -> Response:
            try:
                response = await handler(request)
            except (RequestValidationError, ValidationError):
                raise _refusal(
                    status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid_enrollment_request"
                ) from None
            except HTTPException as error:
                error.headers = {**(error.headers or {}), "Cache-Control": NO_STORE}
                raise
            response.headers["Cache-Control"] = NO_STORE
            return response

        return redacted


async def _session_only(request: Request) -> None:
    """Fail closed when any other Curie credential rides along with the cookie."""

    if any(name in request.headers for name in FOREIGN_CREDENTIAL_HEADERS):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail=UNAUTHENTICATED,
            headers={"Cache-Control": NO_STORE},
        )


def _same_origin(request: Request) -> None:
    enforce_console_cookie_origin(request)


router = APIRouter(
    prefix="/personal-claude",
    tags=["personal-enrollment"],
    route_class=EnrollmentRoute,
    dependencies=[Depends(_session_only)],
)

OwnerDep = Annotated[Principal, Depends(require_principal_session)]
SameOriginDep = Annotated[None, Depends(_same_origin)]

_RESPONSES: dict[int | str, dict[str, Any]] = {
    401: {"description": "No live principal session, or another credential was presented"},
    422: {"description": "`invalid_enrollment_request`; the input is never echoed"},
}
_WRITE_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_RESPONSES,
    403: {"description": "Console session origin rejected"},
    409: {"description": "Version or intent conflict: `detail.code` names it"},
    503: {
        "description": "`custody_unconfigured`, `custody_unavailable`, `enrollment_busy` "
        "or `personal_rollout_in_progress`"
    },
}


def _out(found: EnrollmentStatus) -> PersonalEnrollmentOut:
    return PersonalEnrollmentOut(
        state=found.state,
        generation=found.generation,
        version=found.version,
        teardown=PersonalEnrollmentTeardownOut(
            pending_runtimes=found.pending_runtimes,
            pending_custody_cleanup=found.pending_custody_cleanup,
        ),
    )


def _answer(result: EnrollmentStatus | EnrollmentRefusal, *, absent: int) -> PersonalEnrollmentOut:
    if isinstance(result, EnrollmentRefusal):
        code = result.code
        if code == "enrollment_absent":
            raise _refusal(absent, code)
        raise _refusal(_REFUSAL_STATUS.get(code, status.HTTP_409_CONFLICT), code)
    return _out(result)


def _custody(request: Request) -> Custody:
    custody = custody_for(
        request.app.state.sessionmaker,
        get_settings(),
        getattr(request.app.state, "custody_store", None),
    )
    if custody is None:
        raise _refusal(status.HTTP_503_SERVICE_UNAVAILABLE, "custody_unconfigured")
    return custody


@router.get("/enrollment", response_model=PersonalEnrollmentOut, responses=_RESPONSES)
async def read_enrollment(principal: OwnerDep, session: SessionDep) -> PersonalEnrollmentOut:
    """The caller's enrollment; ``absent`` before the first enroll."""

    found = await personal_enrollments.read_status(
        session, tenant_id=principal.tenant_id, principal_id=principal.id
    )
    return _out(found)


@router.put("/enrollment", response_model=PersonalEnrollmentOut, responses=_WRITE_RESPONSES)
async def write_enrollment(
    request: Request,
    body: PersonalEnrollmentWrite,
    principal: OwnerDep,
    _origin: SameOriginDep,
    session: SessionDep,
) -> PersonalEnrollmentOut:
    """Enroll (generation 1), or replace or reconnect at ``expected_version``."""

    tenant_id, principal_id = principal.tenant_id, principal.id
    # The custody protocol runs its own transactions; release the auth read first.
    await session.rollback()
    custody = _custody(request)
    if await rollout_hold.personal_rollout_held():
        raise _refusal(status.HTTP_503_SERVICE_UNAVAILABLE, rollout_hold.HOLD_CODE)
    result = await personal_enrollments.write(
        custody,
        tenant_id=tenant_id,
        principal_id=principal_id,
        intent=body.intent,
        expected_version=body.expected_version,
        token=body.token.get_secret_value(),
    )
    return _answer(result, absent=status.HTTP_409_CONFLICT)


@router.delete(
    "/enrollment",
    response_model=PersonalEnrollmentOut,
    responses={**_WRITE_RESPONSES, 404: {"description": "`enrollment_absent`"}},
)
async def disconnect_enrollment(
    request: Request,
    principal: OwnerDep,
    _origin: SameOriginDep,
    session: SessionDep,
    expected_version: Annotated[int, Query(ge=1)],
) -> PersonalEnrollmentOut:
    """Disconnect: inactive at a new generation, runtimes fenced, Secret retired.

    Repeating it at the inactive version answers 200 and changes nothing. It is
    never held for a rollout, since it only reduces access.
    """

    tenant_id, principal_id = principal.tenant_id, principal.id
    await session.rollback()
    result = await personal_enrollments.disconnect(
        _custody(request),
        tenant_id=tenant_id,
        principal_id=principal_id,
        expected_version=expected_version,
    )
    return _answer(result, absent=status.HTTP_404_NOT_FOUND)
