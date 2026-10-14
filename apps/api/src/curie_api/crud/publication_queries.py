"""Database access for publication queries."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..models import Publication


async def get_publication(session: AsyncSession, publication_id: uuid.UUID) -> Publication | None:
    publication: Publication | None = await session.scalar(
        select(Publication)
        .options(selectinload(Publication.lineage))
        .where(Publication.id == publication_id)
    )
    return publication


async def get_publication_by_approval(
    session: AsyncSession, approval_id: uuid.UUID
) -> Publication | None:
    publication: Publication | None = await session.scalar(
        select(Publication)
        .options(selectinload(Publication.lineage))
        .where(Publication.approval_id == approval_id)
    )
    return publication


async def list_publications(session: AsyncSession, *, limit: int = 100) -> list[Publication]:
    result = await session.scalars(
        select(Publication)
        .options(selectinload(Publication.lineage))
        .order_by(Publication.created_at.desc())
        .limit(limit)
    )
    return list(result)
