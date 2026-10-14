"""The nomination submission route, @spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7.

``POST /v1/internal/remediation/nominations`` accepts only the internal worker
token (``X-Curie-Worker-Token``); the platform key, a hook key and a sandbox
token never reach it. The body is exactly ``event_id`` and the block. In order:

1. ``remediation_disabled`` (409) unless remediation and the action executor
   are both enabled (AUTOMATED-REMEDIATION-1, -8 check 1); nothing is read;
2. the event is resolved through its protected binding
   (``curie_api.remediation_binding``): no binding is ``not_protected_event``
   (404), an unreadable broker is ``503``;
3. the event's first accepted submission wins: a byte-identical replay returns
   the same answer, different bytes are ``nomination_conflict`` (409);
4. the block is parsed and one row per entry (or one malformed row) is written;
5. admission (``curie_api.remediation_admission``) decides each ``received``
   row in the order of AUTOMATED-REMEDIATION-8: ``refused`` ``agent_stopped``,
   ``approval_requested`` naming the failed check, or ``precondition_pending``
   with its precondition read.

The worker never reads the policy and never decides admission. Responses carry
``Cache-Control: no-store``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import KillSwitchDep, SessionDep, StoreDep
from ..remediation_admission import admit_nominations
from ..remediation_binding import BindingUnavailable, resolve_protected_event
from ..remediation_nomination_store import (
    EventAgentMissing,
    NominationConflict,
    record_submission,
)
from ..schemas.remediation_nominations import (
    RemediationNominationAccepted,
    RemediationNominationRefusal,
    RemediationNominationSubmit,
)

router = APIRouter(
    prefix="/v1/internal/remediation",
    tags=["internal-remediation"],
    dependencies=[Depends(require_internal_worker_token)],
)

_NO_STORE = {"Cache-Control": "no-store"}
_REFUSAL: dict[str, Any] = {"model": RemediationNominationRefusal, "description": "Refusal"}


def _refused(status_code: int, code: str) -> JSONResponse:
    """@spec AUTOMATED-REMEDIATION-6."""
    return JSONResponse(
        status_code=status_code, content={"detail": {"code": code}}, headers=_NO_STORE
    )


@router.post(
    "/nominations",
    response_model=RemediationNominationAccepted,
    responses={404: _REFUSAL, 409: _REFUSAL, 503: {"description": "Broker unavailable"}},
)
async def submit_remediation_nominations(
    body: RemediationNominationSubmit,
    session: SessionDep,
    store: StoreDep,
    kill_switch: KillSwitchDep,
) -> JSONResponse:
    """Record one protected turn's nomination block.

    \f
    @spec AUTOMATED-REMEDIATION-1 @spec AUTOMATED-REMEDIATION-6 @spec AUTOMATED-REMEDIATION-7
    @spec AUTOMATED-REMEDIATION-8.
    """
    settings = get_settings()
    if not (settings.remediation_enabled and settings.action_executor_enabled):
        return _refused(409, "remediation_disabled")
    try:
        event = await resolve_protected_event(body.event_id)
    except BindingUnavailable:
        return JSONResponse(
            status_code=503, content={"detail": "broker_unavailable"}, headers=_NO_STORE
        )
    if event is None:
        return _refused(404, "not_protected_event")
    try:
        async with session.begin():
            ids = await record_submission(session, event, body.block)
    except EventAgentMissing:
        return _refused(404, "not_protected_event")
    except NominationConflict:
        return _refused(409, "nomination_conflict")
    # @spec AUTOMATED-REMEDIATION-8: admission decides each received row; a
    # replay finds them decided and changes nothing.
    await admit_nominations(session, store, kill_switch, ids)
    accepted = RemediationNominationAccepted(event_id=event.event_id, nomination_ids=ids)
    return JSONResponse(content=accepted.model_dump(mode="json"), headers=_NO_STORE)
