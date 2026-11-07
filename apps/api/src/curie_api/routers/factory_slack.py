"""The worker asks whether a Slack mention in a factory run's root steers it (#5414)."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.exc import SQLAlchemyError

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..factory_slack import CodeHostUnavailable, steer
from ..schemas.workitems import FactorySlackSteerIn, FactorySlackSteerOut

internal_router = APIRouter(
    prefix="/v1/internal/factory-slack",
    tags=["internal-factory-slack"],
    dependencies=[Depends(require_internal_worker_token)],
)
logger = logging.getLogger(__name__)


@internal_router.post("/steer", response_model=FactorySlackSteerOut)
async def steer_factory_run(
    data: FactorySlackSteerIn, request: Request, response: Response, session: SessionDep
) -> FactorySlackSteerOut:
    """Admit a revision of the run's pull request, or name why not.

    Every refusal is final for the delivery and answers 200. A code host that
    cannot answer now is 503, so the worker keeps the delivery pending. A
    database failure is 503 too, raised without its exception chain: the
    driver's error carries the bound objective, which holds the Slack body.
    """

    response.headers["Cache-Control"] = "no-store"
    try:
        return await steer(
            session, data, settings=get_settings(), client=request.app.state.http_client
        )
    except CodeHostUnavailable as exc:
        raise HTTPException(
            503,
            {"code": "code_host_unavailable", "reason": exc.code},
            headers={"Retry-After": "10", "Cache-Control": "no-store"},
        ) from None
    except (SQLAlchemyError, OSError) as exc:
        logger.warning("factory_slack.steer persistence_unavailable error=%s", type(exc).__name__)
        raise HTTPException(
            503,
            {"code": "persistence_unavailable"},
            headers={"Retry-After": "10", "Cache-Control": "no-store"},
        ) from None
