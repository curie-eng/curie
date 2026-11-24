"""The worker asks whether a Slack mention steers a factory run (#5414) or starts one (#5380)."""

import logging
import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..factory_slack import CodeHostUnavailable, steer
from ..factory_slack_kickoff import kickoff
from ..factory_slack_public import public_publication
from ..factory_slack_voice import SlackVoice, VoiceUnavailable
from ..schemas.workitems import (
    FactorySlackKickoffIn,
    FactorySlackKickoffOut,
    FactorySlackSteerIn,
    FactorySlackSteerOut,
    FactorySlackVoiceOut,
)

internal_router = APIRouter(
    prefix="/v1/internal/factory-slack",
    tags=["internal-factory-slack"],
    dependencies=[Depends(require_internal_worker_token)],
)
logger = logging.getLogger(__name__)


class _VoiceRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def redacted(request: Request) -> Response:
            try:
                return await handler(request)
            except RequestValidationError:
                raise HTTPException(
                    422, {"code": "invalid_source"}, headers={"Cache-Control": "no-store"}
                ) from None

        return redacted


async def voice_factory_run(
    data: FactorySlackSteerIn, request: Request, response: Response
) -> FactorySlackVoiceOut:
    response.headers["Cache-Control"] = "no-store"
    adapter: SlackVoice | None = request.app.state.factory_slack_voice
    try:
        if adapter is None:
            if get_settings().factory_voice_enabled:
                raise VoiceUnavailable
            return FactorySlackVoiceOut(outcome="refused", reason="disabled")
        return await adapter.handle(data)
    except (VoiceUnavailable, CodeHostUnavailable, RedisError, SQLAlchemyError, OSError):
        raise HTTPException(
            503, {"code": "unavailable"}, headers={"Retry-After": "10", "Cache-Control": "no-store"}
        ) from None


internal_router.add_api_route(
    "/voice",
    voice_factory_run,
    methods=["POST"],
    response_model=FactorySlackVoiceOut,
    response_model_exclude_none=True,
    route_class_override=_VoiceRoute,
)


@internal_router.post("/steer", response_model=FactorySlackSteerOut)
async def steer_factory_run(
    data: FactorySlackSteerIn, request: Request, response: Response, session: SessionDep
) -> FactorySlackSteerOut:
    """Queue authorized guidance for an implementation or its published pull request.

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


@internal_router.get("/publication/{work_item_id}")
async def get_public_publication(
    work_item_id: uuid.UUID, request: Request, response: Response, session: SessionDep
) -> dict[str, str | int]:
    response.headers["Cache-Control"] = "no-store"
    return await public_publication(
        session, work_item_id, settings=get_settings(), client=request.app.state.http_client
    )
