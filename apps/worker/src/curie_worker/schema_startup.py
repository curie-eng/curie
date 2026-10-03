"""@spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import logging
import os

from curie_protected_hooks.schema_serving import SchemaServingUnavailable, assert_servable
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .config import WorkerConfig

logger = logging.getLogger(__name__)


def _record_primary(error: BaseException) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    code = "schema_probe_unavailable"
    if isinstance(error, asyncio.CancelledError):
        code = "cancelled"
    elif isinstance(error, SchemaServingUnavailable) and error.code in {
        "schema_below_min",
        "schema_metadata_invalid",
        "schema_probe_timeout",
        "schema_probe_unavailable",
        "schema_revision_unavailable",
        "schema_structure_unavailable",
    }:
        code = error.code
    logger.warning("worker_schema_primary cause=%s", code)


async def _dispose_probe(engine: AsyncEngine) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    close = asyncio.create_task(engine.dispose())
    _, pending = await asyncio.wait({close}, timeout=5)
    if pending:
        logger.error("worker_schema_cleanup_deadline")
        os._exit(1)
    await close


async def assert_worker_schema(config: WorkerConfig) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    try:
        engine = create_async_engine(config.database_url)
    except Exception:
        raise SchemaServingUnavailable("schema_probe_unavailable") from None
    primary: BaseException | None = None
    try:
        await assert_servable(engine, metadata_schema=config.db_schema)
    except BaseException as error:
        primary = error
        _record_primary(error)
    cleanup = asyncio.create_task(_dispose_probe(engine))
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            if primary is None:
                primary = error
                _record_primary(error)
        except Exception:
            break
    try:
        cleanup.result()
    except (Exception, asyncio.CancelledError):
        if primary is None:
            raise SchemaServingUnavailable("schema_cleanup_unavailable") from None
        logger.warning("schema_cleanup_unavailable")
    if primary is not None:
        raise primary
