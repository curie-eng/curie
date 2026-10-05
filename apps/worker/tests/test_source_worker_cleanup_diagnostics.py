"""Actual resource cleanup with TEST diagnostics; @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import select
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_worker.worker_lifecycle import WorkerLifecycleUnavailable, WorkerResources
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine


def startup_support() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_worker_startup.py")
    spec = importlib.util.spec_from_file_location("_cleanup_diagnostics_startup_tests", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


startup = startup_support()
worker_db = startup.worker_db
worker_templates = startup.worker_templates


class RaisingDiagnostic(logging.Handler):
    """TEST log sink only, @spec PROTECTED-HOOK-SOURCE-2."""

    def emit(self, record: logging.LogRecord) -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        raise RuntimeError("TEST-private-warning-handler")


@pytest.mark.parametrize("fault_site", ["stop", "closer"])
@pytest.mark.parametrize("primary_present", [False, True])
def test_raising_test_warning_cannot_skip_actual_later_engine_close(
    worker_db: Any, fault_site: str, primary_present: bool
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        owner = WorkerResources()
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        async with engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        disposed: list[bool] = []
        event.listen(engine.sync_engine, "engine_disposed", lambda _: disposed.append(True))

        def failing_stop() -> None:
            """TEST stop actor, @spec PROTECTED-HOOK-SOURCE-2."""
            raise ValueError("TEST-private-stop-error")

        async def failing_close() -> None:
            """TEST closer actor, @spec PROTECTED-HOOK-SOURCE-2."""
            raise ValueError("TEST-private-close-error")

        if fault_site == "stop":
            owner.register_stop("TEST-stop", failing_stop)
        else:
            owner.register_close("TEST-closer", failing_close, order=1)
        owner.register_close("actual-engine", engine.dispose, order=100)
        primary = ValueError("TEST-primary") if primary_present else None
        logger = logging.getLogger("curie_worker.worker_lifecycle")
        handler = RaisingDiagnostic()
        logger.addHandler(handler)
        caught: BaseException | None = None
        try:
            try:
                async with asyncio.timeout(8):
                    await owner.aclose(primary=primary)
            # observe cancellation and primary cleanup fault identity.
            except BaseException as error:  # noqa: BLE001
                caught = error
            assert disposed == [True], (
                "SOURCE-2: a faulty cleanup diagnostic cannot skip later actual resources"
            )
            async with observer.connect() as connection:
                assert not await connection.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                )
            if primary is not None:
                assert caught is primary
            else:
                assert isinstance(caught, WorkerLifecycleUnavailable)
                assert "TEST-private" not in str(caught)
        finally:
            logger.removeHandler(handler)
            handler.close()
            await engine.dispose()
            await observer.dispose()

    asyncio.run(scenario())


@pytest.mark.parametrize("fault_site", ["stop", "closer"])
def test_blocked_test_warning_fatal_exits_owned_process_with_actual_backend(
    worker_db: Any, fault_site: str
) -> None:
    """Fatal process exit only, no ordered disposal claim; @spec PROTECTED-HOOK-SOURCE-2."""
    support, _, url = worker_db
    application = "test-cleanup-diagnostic-" + uuid.uuid4().hex
    program = '''
import asyncio,json,logging,os,threading
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from curie_worker.worker_lifecycle import WorkerResources
class BlockedWarning(logging.Handler):
    """TEST diagnostic actor, @spec PROTECTED-HOOK-SOURCE-2."""
    def emit(self, record):
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if record.levelno == logging.WARNING: threading.Event().wait()
async def exercise():
    """Actual opened engine plus TEST faults, @spec PROTECTED-HOOK-SOURCE-2."""
    engine=create_async_engine(os.environ['DATABASE_URL'],pool_size=1,max_overflow=0,
        connect_args={'server_settings':{'application_name':os.environ['TEST_APPLICATION']}})
    async with engine.connect() as connection:
        pid=await connection.scalar(text('SELECT pg_backend_pid()'))
    owner=WorkerResources()
    def bad_stop():
        """TEST stop actor, @spec PROTECTED-HOOK-SOURCE-2."""
        raise ValueError('TEST-private-stop-error')
    async def bad_close():
        """TEST closer actor, @spec PROTECTED-HOOK-SOURCE-2."""
        raise ValueError('TEST-private-close-error')
    if os.environ['TEST_FAULT_SITE']=='stop': owner.register_stop('TEST-stop',bad_stop)
    else: owner.register_close('TEST-close',bad_close,order=1)
    owner.register_close('actual-engine',engine.dispose,order=100)
    logging.getLogger('curie_worker.worker_lifecycle').addHandler(BlockedWarning())
    print(json.dumps({'pid':pid}),flush=True)
    await owner.aclose(primary=ValueError('TEST-primary'))
asyncio.run(exercise())
'''
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        env=dict(
            os.environ, DATABASE_URL=url, TEST_APPLICATION=application, TEST_FAULT_SITE=fault_site
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 5)
        assert ready, "owned actor must open its actual backend before cleanup"
        pid = json.loads(process.stdout.readline())["pid"]
        started = time.monotonic()
        exceeded = False
        try:
            _, stderr = process.communicate(timeout=6.5)
        except subprocess.TimeoutExpired:
            exceeded = True
            process.kill()
            _, stderr = process.communicate(timeout=3)
        elapsed = time.monotonic() - started
        assert support.sql_dicts(
            "SELECT count(*) AS peers FROM pg_stat_activity "
            "WHERE pid=:pid AND application_name=:application",
            {"pid": pid, "application": application},
        ) == [{"peers": 0}]
        assert not exceeded, "SOURCE-2: a blocked warning must not prevent bounded fatal exit"
        assert process.returncode == 1
        assert 4.5 <= elapsed < 6.5
        assert "TEST-private" not in stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=3)
