"""The agent's bounded channel read and its worker mint route (ADR 0100, #2877)."""

import logging
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from curie_api.schemas.channel_read import (
    ChannelReadContext,
    ChannelReadContextMint,
    ChannelReadPage,
    ChannelReadRequest,
)

from ..auth import require_internal_worker_token
from ..channel_read.errors import ChannelReadRefused
from ..channel_read.ledger import ChannelReadLedger, LedgerUnavailable
from ..channel_read.provider_guard import ProviderGuard
from ..channel_read.service import authorize_and_read, channel_readers, mint_context
from ..channel_read.token import ChannelReadClaims, verify_claims
from ..config import get_settings
from ..deps import SessionDep, StoreDep

_LOG = logging.getLogger(__name__)

CAPABILITY_HEADER = "X-Curie-Channel-Read"
_NO_STORE = {"Cache-Control": "no-store"}


def _refusal(refused: ChannelReadRefused) -> HTTPException:
    detail: dict[str, Any] = {"code": refused.code, "message": refused.message}
    headers = dict(_NO_STORE)
    if refused.retry_after is not None:
        detail["retry_after"] = refused.retry_after
        headers["Retry-After"] = str(refused.retry_after)
    return HTTPException(refused.status, detail, headers=headers)


def _unavailable() -> HTTPException:
    return _refusal(
        ChannelReadRefused(503, "unavailable", "the read ledger is unavailable; retry shortly")
    )


class ChannelReadRoute(APIRoute):
    """Names a malformed read body ``channel_read.request_invalid``.

    Scoped to the read route. The message carries locations and reasons only,
    never the submitted input, so nothing the agent sent is echoed back.
    """

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            try:
                return await handler(request)
            except RequestValidationError as exc:
                reasons = "; ".join(
                    f"{'.'.join(str(part) for part in error.get('loc', ()))}: {error.get('msg')}"
                    for error in exc.errors()
                )
                return JSONResponse(
                    status_code=422,
                    content={
                        "detail": {"code": "channel_read.request_invalid", "message": reasons}
                    },
                    headers=_NO_STORE,
                )

        return guarded


router = APIRouter(tags=["channel-read"], route_class=ChannelReadRoute)
internal_router = APIRouter(prefix="/v1/internal/channel-read", tags=["internal-channel-read"])

_REFUSALS: dict[int | str, dict[str, Any]] = {
    status: {"description": description}
    for status, description in (
        (400, "No channel named and no default channel"),
        (401, "Missing or invalid channel read capability"),
        (403, "Channel not bound, or the app is not a member"),
        (404, "Message or thread not found in the channel"),
        (409, "Turn inactive or expired, grant revoked, or kind unsupported"),
        (422, "Invalid window, limit, identifier, cursor or body"),
        (429, "Page or attempt budget exhausted, or the provider rate limited"),
        (502, "The provider returned an error"),
        (503, "Ledger unavailable or no provider credential"),
    )
}


async def require_channel_read(
    credential: Annotated[str | None, Header(alias=CAPABILITY_HEADER)] = None,
) -> ChannelReadClaims:
    claims = verify_claims(credential, get_settings().api_key) if credential else None
    if claims is None:
        raise _refusal(
            ChannelReadRefused(401, "invalid_capability", "channel read credential is invalid")
        )
    return claims


def _ledger(request: Request) -> ChannelReadLedger:
    return ChannelReadLedger(request.app.state.valkey, get_settings().worker_key_prefix)


@internal_router.post(
    "/context",
    response_model=ChannelReadContext,
    dependencies=[Depends(require_internal_worker_token)],
    responses={
        409: {"description": "No capability for this turn"},
        503: {"description": "Ledger unavailable"},
    },
)
async def mint_channel_read_context(
    data: ChannelReadContextMint,
    request: Request,
    response: Response,
    session: SessionDep,
    store: StoreDep,
) -> ChannelReadContext:
    response.headers["Cache-Control"] = "no-store"
    try:
        return await mint_context(
            data, session=session, ledger=_ledger(request), store=store, settings=get_settings()
        )
    except ChannelReadRefused as refused:
        raise _refusal(refused) from None
    except LedgerUnavailable:
        _LOG.warning("channel read mint refused: the ledger is unavailable")
        raise _unavailable() from None


@router.post("/channel-read", response_model=ChannelReadPage, responses=_REFUSALS)
async def read_channel(
    data: ChannelReadRequest,
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[ChannelReadClaims, Depends(require_channel_read)],
) -> ChannelReadPage:
    response.headers["Cache-Control"] = "no-store"
    settings = get_settings()
    try:
        return await authorize_and_read(
            claims=claims,
            body=data,
            session=session,
            ledger=_ledger(request),
            guard=ProviderGuard(request.app.state.valkey, settings.worker_key_prefix),
            settings=settings,
            readers=channel_readers(settings, request.app.state.http_client),
            now=datetime.now(UTC),
        )
    except ChannelReadRefused as refused:
        raise _refusal(refused) from None
    except LedgerUnavailable:
        _LOG.warning("channel read refused: the ledger is unavailable")
        raise _unavailable() from None
