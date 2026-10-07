"""The nomination read routes behind ``curie remediation list`` and ``show``.

@spec AUTOMATED-REMEDIATION-20 @spec AUTOMATED-REMEDIATION-21

``GET /remediation-nominations`` (optional ``agent_id``, ``state`` and ``limit``,
newest first) and ``GET /remediation-nominations/{nomination_id}`` answer the
operator receipt of AUTOMATED-REMEDIATION-20. They take the platform key as the
execution receipt does; the worker token alone is not a credential here, and no
operator principal is asked of a read. A row has exactly the fields of
``RemediationNominationOut``: never the arguments, the model's reason or the
alert body. Both only read.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal, get_args

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import JSONResponse
from sqlalchemy import select

from ..auth import require_api_key
from ..deps import SessionDep
from ..models import RemediationNomination
from ..remediation_receipts import derive_receipt
from ..schemas.remediation_nominations import RemediationNominationOut

router = APIRouter(
    prefix="/remediation-nominations",
    tags=["remediation-nominations"],
    dependencies=[Depends(require_api_key)],
)

_NO_STORE = {"Cache-Control": "no-store"}
State = Literal[
    "received",
    "refused",
    "precondition_pending",
    "admitted",
    "approval_requested",
    "approved",
    "rejected",
    "expired",
    "executing",
    "verifying",
    "finished",
]
_STATES: tuple[str, ...] = get_args(State)


def _row(nomination: RemediationNomination) -> dict[str, Any]:
    stage, authority, code = derive_receipt(nomination)
    return RemediationNominationOut(
        id=nomination.id,
        agent_id=nomination.agent_id,
        hook=nomination.hook,
        kind=nomination.kind,
        action=nomination.action,
        target=nomination.target,
        state=nomination.state,
        stage=stage,
        authority=authority,
        code=code,
        verification_outcome=nomination.verification_outcome,
        approval_id=nomination.approval_id,
        execution_id=nomination.execution_id,
        created_at=nomination.created_at,
        decided_at=nomination.decided_at,
    ).model_dump(mode="json")


@router.get("", response_model=list[RemediationNominationOut])
async def list_remediation_nominations(
    session: SessionDep,
    agent_id: Annotated[uuid.UUID | None, Query()] = None,
    state: Annotated[State | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> JSONResponse:
    """Nominations, newest first: the operator receipt list.

    \f
    @spec AUTOMATED-REMEDIATION-20.
    """
    query = select(RemediationNomination)
    if agent_id is not None:
        query = query.where(RemediationNomination.agent_id == agent_id)
    if state is not None:
        query = query.where(RemediationNomination.state == state)
    rows = await session.scalars(
        query.order_by(RemediationNomination.created_at.desc(), RemediationNomination.id.desc())
        .limit(limit)
    )
    return JSONResponse(content=[_row(row) for row in rows], headers=_NO_STORE)


@router.get(
    "/{nomination_id}",
    response_model=RemediationNominationOut,
    responses={404: {"description": "No such nomination"}},
)
async def show_remediation_nomination(
    nomination_id: Annotated[uuid.UUID, Path()], session: SessionDep
) -> JSONResponse:
    """One nomination's receipt.

    \f
    @spec AUTOMATED-REMEDIATION-20.
    """
    nomination = await session.get(RemediationNomination, nomination_id)
    if nomination is None:
        raise HTTPException(
            status_code=404, detail="nomination not found", headers=_NO_STORE
        )
    return JSONResponse(content=_row(nomination), headers=_NO_STORE)
