"""A transient connection refusal must leave the retention loop running."""

import asyncio
import logging
import socket

import pytest
from curie_api.factory_voice_retention import purge_expired_drafts
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_purge_retries_a_refused_connection(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("curie_api.factory_voice_retention")
    retried = asyncio.Event()
    messages: list[str] = []

    class ObserveRetry(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())
            if len(messages) == 2:
                retried.set()

    observer = ObserveRetry()
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
        engine = create_async_engine(f"postgresql+asyncpg://example@127.0.0.1:{port}/example")
        logger.addHandler(observer)
        task = asyncio.create_task(purge_expired_drafts(async_sessionmaker(engine), interval=0))
        try:
            async with asyncio.timeout(3):
                await retried.wait()
            assert not task.done()
            assert set(messages) == {"Factory voice draft purge unavailable"}
            assert "example@" not in caplog.text
        finally:
            logger.removeHandler(observer)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            finally:
                await engine.dispose()
