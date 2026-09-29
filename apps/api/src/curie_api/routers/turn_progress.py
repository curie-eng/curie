"""``POST /v1/turn-progress/{progress_id}``: deliberate progress from a running turn.

ADR 0130's scoped platform-internal ingress. The route accepts exactly one
credential: a ``turn.progress`` sandbox token whose subject is the path's
``progress_id``, which the worker mints per turn for the chain that turn
belongs to. The platform key is refused here, so the route has one credential,
and a token for another chain cannot address this one. What the route does
with an accepted command is in ``curie_api.turn_progress``.
"""

from __future__ import annotations

import logging
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from .. import sandbox_token
from ..config import get_settings
from ..turn_progress import (
    TURN_PROGRESS_PATH,
    TURN_PROGRESS_SCOPE,
    TurnProgressBody,
    append_to_inbox,
    inbox_fields,
    inbox_key,
    rate_key,
    take_rate_token,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["turn-progress"])


async def require_turn_progress_token(
    progress_id: uuid.UUID,
    x_api_key: Annotated[str | None, Header()] = None,
) -> str:
    """Accept only a ``turn.progress`` token bound to the path's chain."""

    if not x_api_key or not sandbox_token.verify(
        x_api_key,
        get_settings().api_key,
        agent=str(progress_id),
        scope=TURN_PROGRESS_SCOPE,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid turn progress token",
        )
    return x_api_key


@router.post(TURN_PROGRESS_PATH, status_code=status.HTTP_202_ACCEPTED)
async def accept_turn_progress(
    progress_id: uuid.UUID,
    body: TurnProgressBody,
    request: Request,
    token: Annotated[str, Depends(require_turn_progress_token)],
) -> Any:
    """Append one command to the chain's inbox for the worker's pump.

    202 means the command is queued for the chain, not that it was applied:
    the worker's store may still refuse it as a duplicate, out of order, past
    the chain's update cap, or for a state only the platform sets.
    """

    settings = get_settings()
    valkey = request.app.state.valkey
    if not await take_rate_token(valkey, rate_key(settings.worker_key_prefix, token)):
        return JSONResponse(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content={"code": "rate_limited"},
            headers={"Retry-After": "1"},
        )
    entry_id = await append_to_inbox(
        valkey,
        inbox_key(settings.worker_key_prefix, str(progress_id)),
        inbox_fields(body),
    )
    logger.info(
        "turn progress accepted for %s at epoch %d seq %d", progress_id, body.epoch, body.seq
    )
    return {"accepted": True, "id": entry_id}
