"""The worker asks whether a Slack mention steers a factory run (#5414) or starts one (#5380)."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.exc import SQLAlchemyError

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..factory_slack import CodeHostUnavailable, steer
from ..factory_slack_kickoff import kickoff
from ..schemas.workitems import (
    FactorySlackKickoffIn,
    FactorySlackKickoffOut,
    FactorySlackSteerIn,
    FactorySlackSteerOut,
)

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


@internal_router.post("/kickoff", response_model=FactorySlackKickoffOut)
async def kickoff_factory_run(
    data: FactorySlackKickoffIn, request: Request, response: Response, session: SessionDep
) -> FactorySlackKickoffOut:
    """Admit a personal factory run for the one issue a mention links, or name why not.

    ``not_factory`` sends the mention down the ordinary path; every other
    refusal is final for the delivery. Both answer 200. A code host that
    cannot answer now is 503, so the worker keeps the delivery pending, and so
    is a database failure, raised without its exception chain: the driver's
    error may carry bound parameters that hold the Slack body.
    """

    response.headers["Cache-Control"] = "no-store"
    try:
        return await kickoff(
            session, data, settings=get_settings(), client=request.app.state.http_client
        )
    except CodeHostUnavailable as exc:
        raise HTTPException(
            503,
            {"code": "code_host_unavailable", "reason": exc.code},
            headers={"Retry-After": "10", "Cache-Control": "no-store"},
        ) from None
    except (SQLAlchemyError, OSError) as exc:
        logger.warning("factory_slack.kickoff persistence_unavailable error=%s", type(exc).__name__)
        raise HTTPException(
            503,
            {"code": "persistence_unavailable"},
            headers={"Retry-After": "10", "Cache-Control": "no-store"},
        ) from None
