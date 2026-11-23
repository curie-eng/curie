"""Remove expired draft contents even when recognition is disabled."""

import asyncio
import logging

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncEngine

from .db import create_sessionmaker
from .factory_voice_processing import expire_voice_drafts, storage_available
from .models import FactoryVoiceDraft

_LOG = logging.getLogger(__name__)
SWEEP_INTERVAL = 60.0


async def purge_expired_drafts(engine: AsyncEngine, *, enabled: bool) -> None:
    sessions = create_sessionmaker(engine)
    sources_remain = select(exists().where(FactoryVoiceDraft.source.is_not(None)))
    try:
        while True:
            try:
                async with sessions.begin() as session:
                    if not enabled and (
                        not await storage_available(session)
                        or not await session.scalar(sources_remain)
                    ):
                        return
                    expired = await expire_voice_drafts(session)
            except Exception:  # noqa: BLE001 - retry without disclosing draft or connection data
                _LOG.warning("Factory voice draft purge unavailable")
                expired = 0
            if not expired:
                await asyncio.sleep(SWEEP_INTERVAL)
    finally:
        await engine.dispose()
