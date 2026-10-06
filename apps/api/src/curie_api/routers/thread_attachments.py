"""Worker-only read and append of a thread's attachment ledger (ADR 0205, #4079).

Both routes accept only the internal worker credential. No state route
exposes the ledger and no sandbox credential reaches it, so an agent cannot
add a file id for the worker to fetch with the bot token. The worker always
writes binding scope NULL, so the body names no scope.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import select

from .. import thread_attachments
from ..auth import require_internal_worker_token
from ..deps import SessionDep
from ..models import Agent
from ..schemas.thread_attachments import (
    ThreadAttachmentAppend,
    ThreadAttachmentAppendOut,
    ThreadAttachmentQuery,
    ThreadAttachmentRefBody,
    ThreadAttachmentRefsOut,
)

_NO_STORE = {"Cache-Control": "no-store"}

internal_router = APIRouter(
    prefix="/v1/internal/thread-attachments",
    tags=["internal-thread-attachments"],
    dependencies=[Depends(require_internal_worker_token)],
)


@internal_router.post("/query", response_model=ThreadAttachmentRefsOut)
async def query_thread_attachments(
    payload: ThreadAttachmentQuery, response: Response, session: SessionDep
) -> ThreadAttachmentRefsOut:
    response.headers.update(_NO_STORE)
    rows = await thread_attachments.list_refs(session, payload.agent_id, None, payload.thread_key)
    return ThreadAttachmentRefsOut(
        refs=[ThreadAttachmentRefBody.model_validate(row) for row in rows]
    )


@internal_router.post("/append", response_model=ThreadAttachmentAppendOut)
async def append_thread_attachments(
    payload: ThreadAttachmentAppend, response: Response, session: SessionDep
) -> ThreadAttachmentAppendOut:
    response.headers.update(_NO_STORE)
    if await session.scalar(select(Agent.id).where(Agent.id == payload.agent_id)) is None:
        raise HTTPException(404, {"code": "thread_attachment.agent_not_found"}, headers=_NO_STORE)
    try:
        appended = await thread_attachments.append_refs(
            session,
            payload.agent_id,
            None,
            payload.thread_key,
            payload.event_id,
            [ref.model_dump() for ref in payload.refs],
        )
    except thread_attachments.NameMismatch as exc:
        raise HTTPException(
            409,
            {"code": "thread_attachment.name_mismatch", "file_id": exc.file_id},
            headers=_NO_STORE,
        ) from None
    except thread_attachments.NameConflict as exc:
        raise HTTPException(
            409,
            {"code": "thread_attachment.name_conflict", "disk_name": exc.disk_name},
            headers=_NO_STORE,
        ) from None
    except thread_attachments.ThreadFull as exc:
        raise HTTPException(
            413,
            {"code": "thread_attachment.thread_full", "limit": exc.limit},
            headers=_NO_STORE,
        ) from None
    return ThreadAttachmentAppendOut(appended=appended)
