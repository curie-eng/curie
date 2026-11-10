"""Owned subprocess fatal machinery, not runtime qualification; @spec PROTECTED-HOOK-SOURCE-2."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import select
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_worker.worker_lifecycle import WorkerLifecycleUnavailable, WorkerResources
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def startup_support() -> Any:
    """Existing isolated backing fixtures, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = Path(__file__).with_name("test_source_worker_startup.py")
    spec = importlib.util.spec_from_file_location("_fatal_diagnostics_startup_tests", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


startup = startup_support()
worker_db = startup.worker_db
worker_templates = startup.worker_templates


@pytest.mark.parametrize("diagnostic_fault", ["raising", "blocking"])
def test_actual_gate_fatal_exit_survives_test_diagnostic_fault(
    worker_db: Any, diagnostic_fault: str
) -> None:
    """Actual backend with TEST diagnostics only, @spec PROTECTED-HOOK-SOURCE-2."""
    support, _, url = worker_db
    application = "test-fatal-diagnostic-" + uuid.uuid4().hex
    program = '''
import asyncio,json,logging,os,threading,uuid
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from curie_protected_hooks.source_policy_sql import SourceGate
from curie_worker.worker_lifecycle import WorkerResources
class DiagnosticFault(logging.Handler):
    """TEST actor, @spec PROTECTED-HOOK-SOURCE-2."""
    def emit(self, record):
        """@spec PROTECTED-HOOK-SOURCE-2."""
        if os.environ['TEST_DIAGNOSTIC_FAULT']=='raising':
            raise RuntimeError('TEST-private-diagnostic-fault')
        threading.Event().wait()
async def exercise():
    """Actual gate, direct fatal machinery only; @spec PROTECTED-HOOK-SOURCE-2."""
    engine=create_async_engine(os.environ['DATABASE_URL'],pool_size=1,max_overflow=0,
        connect_args={'server_settings':{'application_name':os.environ['TEST_APPLICATION']}})
    async with SourceGate(engine).hold(uuid.uuid4()) as held:
        pid=await held._connection.scalar(text('SELECT pg_backend_pid()'))
        logger=logging.getLogger('curie_worker.worker_lifecycle')
        logger.addHandler(DiagnosticFault())
        print(json.dumps({'pid':pid}),flush=True)
        try:
            WorkerResources()._fatal('worker_producer_join_deadline')
        except BaseException:
            raise SystemExit(77)
asyncio.run(exercise())
'''
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        env=dict(
            os.environ,
            DATABASE_URL=url,
            TEST_DIAGNOSTIC_FAULT=diagnostic_fault,
            TEST_APPLICATION=application,
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 5)
        assert ready, "owned TEST actor must establish its actual gate before fatal control"
        pid = json.loads(process.stdout.readline())["pid"]
        started = time.monotonic()
        exceeded = False
        try:
            _, stderr = process.communicate(timeout=1.5)
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
        assert not exceeded, "SOURCE-2: diagnostic blocking must not defeat bounded fatal exit"
        assert process.returncode == 1, (
            "SOURCE-2: diagnostic exceptions must not escape the process fatal mechanism"
        )
        assert elapsed < 1.5
        assert "TEST-private-diagnostic-fault" not in stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=3)


@pytest.mark.parametrize("primary_present", [False, True])
def test_failed_test_producer_cannot_make_actual_resource_cleanup_report_success(
    worker_db: Any, primary_present: bool
) -> None:
    """Registry TEST actor, not cron qualification; @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        owner = WorkerResources()
        pids: list[int] = []

        async def failed_actor() -> None:
            """Actual backend then TEST task failure, @spec PROTECTED-HOOK-SOURCE-2."""
            async with SourceGate(engine).hold(uuid.uuid4()) as held:
                pids.append(await held._connection.scalar(text("SELECT pg_backend_pid()")))
                raise RuntimeError("TEST-private-producer-failure")

        task = asyncio.create_task(failed_actor())
        owner.register_task("TEST-failed-producer", task)
        owner.register_close("actual-engine", engine.dispose, order=100)
        await asyncio.gather(task, return_exceptions=True)
        assert len(pids) == 1 and isinstance(task.exception(), RuntimeError)
        primary = ValueError("TEST-primary") if primary_present else None
        caught: BaseException | None = None
        try:
            try:
                await owner.aclose(primary=primary)
            except (WorkerLifecycleUnavailable, ValueError) as error:
                caught = error
            async with observer.connect() as connection:
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pids[0]},
                )
            if primary is not None:
                assert caught is primary
            else:
                assert isinstance(caught, WorkerLifecycleUnavailable), (
                    "SOURCE-2: an uncaught registered producer failure must not report "
                    "successful cleanup without a primary cause"
                )
                assert "TEST-private-producer-failure" not in str(caught)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await engine.dispose()
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_secondary_warning_cancellation_preserves_primary_after_actual_disposal(
    worker_db: Any,
) -> None:
    """Controlled TEST diagnostic, not cron qualification; @spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        engine = create_async_engine(url, pool_size=1, max_overflow=0)
        observer = create_async_engine(url, pool_size=1, max_overflow=0)
        owner = WorkerResources()
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        primary = ValueError("TEST-primary")

        class ControlledWarning(logging.Handler):
            """Explicit TEST actor on secondary warning, @spec PROTECTED-HOOK-SOURCE-2."""

            def emit(self, record: logging.LogRecord) -> None:
                """@spec PROTECTED-HOOK-SOURCE-2."""
                if record.getMessage().startswith("worker_secondary_cleanup_unavailable"):
                    entered.set()
                    release.wait(timeout=1)
                    finished.set()

        async def failed_close() -> None:
            """Explicit TEST closer failure, @spec PROTECTED-HOOK-SOURCE-2."""
            raise RuntimeError("TEST-private-close-failure")

        async with engine.connect() as connection:
            pid = await connection.scalar(text("SELECT pg_backend_pid()"))
        owner.register_close("TEST-close-failure", failed_close, order=1)
        owner.register_close("actual-engine", engine.dispose, order=100)
        logger = logging.getLogger("curie_worker.worker_lifecycle")
        diagnostic = ControlledWarning()
        logger.addHandler(diagnostic)
        caller = asyncio.create_task(owner.aclose(primary=primary))
        caught: BaseException | None = None
        try:
            async with asyncio.timeout(3):
                while not entered.is_set():
                    await asyncio.sleep(0.005)
            caller.cancel()
            await asyncio.sleep(0.02)
            release.set()
            try:
                await caller
            except (ValueError, asyncio.CancelledError) as error:
                caught = error
            async with asyncio.timeout(3):
                while not finished.is_set():
                    await asyncio.sleep(0.005)
            async with observer.connect() as connection:
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                    {"pid": pid},
                )
            assert caught is primary, (
                "SOURCE-2: cancellation during owned secondary diagnostics must not "
                "replace the original primary failure"
            )
        finally:
            release.set()
            if not caller.done():
                caller.cancel()
                await asyncio.gather(caller, return_exceptions=True)
            logger.removeHandler(diagnostic)
            await engine.dispose()
            await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 10))
