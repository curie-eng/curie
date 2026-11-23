"""Remove expired draft contents even when recognition is disabled."""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .factory_voice import expire_voice_drafts

_LOG = logging.getLogger(__name__)


async def purge_expired_drafts(
    sessions: async_sessionmaker[AsyncSession], *, interval: float = 60.0
) -> None:
    while True:
        try:
            async with sessions.begin() as session:
                expired = await expire_voice_drafts(session)
        except Exception:  # noqa: BLE001 - retry database and transport failures without row data
            _LOG.warning("Factory voice draft purge unavailable")
            expired = 0
        if not expired:
            await asyncio.sleep(interval)
