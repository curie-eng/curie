"""Authenticated relay routes for disconnected ``curie cluster message``."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from curie_api.schemas.channels import ClusterMessageReplyAck, ClusterMessageReplyPage

from ..auth import require_api_key, require_internal_adapter_secret
from ..cluster_message_replies import (
    ClusterMessageReplyStore,
    ReplyBucketBytesExceededError,
    ReplyBucketFullError,
)
from ..config import get_settings

router = APIRouter(
    prefix="/cluster-message-replies",
    tags=["cluster-message-replies"],
    dependencies=[Depends(require_api_key)],
)
internal_router = APIRouter(
    prefix="/v1/internal/cluster-message-replies",
    tags=["internal-cluster-message-replies"],
)


def _validated_ref(reply_ref: str) -> str:
    """Accept only canonical lowercase RFC UUIDv4 text."""

    try:
        parsed = uuid.UUID(reply_ref)
    except (ValueError, AttributeError) as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "reply ref must be a canonical lowercase UUIDv4",
        ) from exc
    if parsed.version != 4 or str(parsed) != reply_ref:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "reply ref must be a canonical lowercase UUIDv4",
        )
    return reply_ref


def _store(request: Request) -> ClusterMessageReplyStore:
    settings = get_settings()
    client: redis.Redis = request.app.state.valkey
    return ClusterMessageReplyStore(
        client,
        ttl_s=settings.cluster_message_replies_ttl_s,
        max_events=settings.cluster_message_replies_max_events,
        max_bytes=settings.cluster_message_replies_max_bytes,
    )


def _body_ref(event: dict[str, Any]) -> str:
    target = event.get("target")
    if not isinstance(target, dict) or not isinstance(target.get("reply_ref"), str):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "reply event target must carry a reply_ref",
        )
    return str(target["reply_ref"])


def _is_terminal(event: dict[str, Any]) -> bool:
    # Approval is a pause whose resumed events reuse this same bucket.  Every
    # other completion outcome ends polling, including explicit drops/escalation.
    return event.get("event") == "turn.completed" and event.get("outcome") in {
        "delivered",
        "dropped",
        "escalated",
    }


@internal_router.post(
    "/{reply_ref}",
    response_model=ClusterMessageReplyAck,
    dependencies=[Depends(require_internal_adapter_secret)],
)
async def append_cluster_message_reply(
    reply_ref: str,
    event: dict[str, Any],
    request: Request,
    response: Response,
) -> ClusterMessageReplyAck:
    response.headers["Cache-Control"] = "no-store"
    validated_ref = _validated_ref(reply_ref)
    if _body_ref(event) != validated_ref:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "reply event ref does not match the requested bucket",
        )
    try:
        await _store(request).append(
            validated_ref,
            event,
            terminal=_is_terminal(event),
        )
    except ReplyBucketFullError as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "cluster-message reply bucket reached its event limit",
        ) from exc
    except ReplyBucketBytesExceededError as exc:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            "cluster-message reply bucket reached its byte limit",
        ) from exc
    except redis.RedisError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "cluster-message reply store is unavailable",
        ) from exc
    return ClusterMessageReplyAck(ref=validated_ref)


@router.get("/{reply_ref}", response_model=ClusterMessageReplyPage)
async def get_cluster_message_replies(
    reply_ref: str,
    request: Request,
    response: Response,
    after: Annotated[int, Query(ge=0)] = 0,
) -> ClusterMessageReplyPage:
    response.headers["Cache-Control"] = "no-store"
    validated_ref = _validated_ref(reply_ref)
    try:
        page = await _store(request).read(validated_ref, after=after)
    except (redis.RedisError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "cluster-message reply store is unavailable",
        ) from exc
    if page is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "cluster-message reply bucket not found",
        )
    return ClusterMessageReplyPage(
        events=page.events,
        next_cursor=page.next_cursor,
        terminal=page.terminal,
    )
