"""@spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from curie_protected_hooks.schema_serving import SchemaServingUnavailable, assert_servable
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .config import WorkerConfig
from .worker_lifecycle import fatal_worker_exit, worker_warning

logger = logging.getLogger(__name__)


def _primary_code(error: BaseException | None) -> str:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    code = "none" if error is None else "schema_probe_unavailable"
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
    return code


async def _dispose_probe(engine: AsyncEngine, primary_code: Callable[[], str]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    close = asyncio.create_task(engine.dispose())
    _, pending = await asyncio.wait({close}, timeout=5)
    if pending:
        fatal_worker_exit(logger, "worker_schema_cleanup_deadline", primary_code())
    try:
        await close
    except (Exception, asyncio.CancelledError):
        await worker_warning(logger, "schema_cleanup_unavailable", primary_code())
        raise


async def assert_worker_schema(config: WorkerConfig) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    try:
        engine = create_async_engine(config.database_url)
    # sanitize every engine construction fault before it reaches diagnostics.
    except Exception:  # noqa: BLE001
        raise SchemaServingUnavailable("schema_probe_unavailable") from None
    primary: BaseException | None = None
    try:
        await assert_servable(engine, metadata_schema=config.db_schema)
    # retain cancellation and primary failures while the probe is disposed.
    except BaseException as error:  # noqa: BLE001
        primary = error
    cleanup = asyncio.create_task(_dispose_probe(engine, lambda: _primary_code(primary)))
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            if primary is None:
                primary = error
        # defer cleanup faults to the sanitized result path.
        except Exception:  # noqa: BLE001
            break
    try:
        cleanup.result()
    # report cleanup failure without exposing database diagnostics.
    except (Exception, asyncio.CancelledError):  # noqa: BLE001
        if primary is None:
            raise SchemaServingUnavailable("schema_cleanup_unavailable") from None
    if primary is not None:
        raise primary
