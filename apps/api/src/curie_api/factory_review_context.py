"""Stored publication context for feedback on a factory pull request."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Publication, WorkItem

_MAX_PULL_REQUEST_BODY = 20000


async def pull_request_body_context(
    session: AsyncSession, work_item: WorkItem
) -> dict[str, str | bool]:
    if work_item.publication_lineage_id is None:
        return {}
    body = await session.scalar(
        select(Publication.body)
        .where(
            Publication.lineage_id == work_item.publication_lineage_id,
            Publication.status == "succeeded",
        )
        .order_by(
            Publication.revision_number.desc().nulls_last(),
            Publication.terminal_at.desc().nulls_last(),
            Publication.created_at.desc(),
            Publication.id.desc(),
        )
        .limit(1)
    )
    if body is None:
        return {}
    context: dict[str, str | bool] = {"pull_request_body": body[:_MAX_PULL_REQUEST_BODY]}
    if len(body) > _MAX_PULL_REQUEST_BODY:
        context["pull_request_body_truncated"] = True
    return context
