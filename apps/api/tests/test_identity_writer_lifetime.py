"""Independent API writer ownership proof. @spec IDENTITY-WRITER-LIFETIME-V1-1 to -10."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import contextmanager
from typing import Any

import httpx
import pytest
from curie_api import deps, identity_fence
from curie_api.config import get_settings
from curie_api.crud import agents as agent_crud
from curie_api.main import create_app
from curie_api.models import Tenant
from identity_writer_lifetime_support import (
    AckClient,
    AckLost,
    ConnectionObserver,
    agents,
    api,
    backend,
    blocking,
    driver,
    headers,
    install_lifetime_route,
    models,
    request,
    run,
    wait_locks,
)
from sqlalchemy import event, text
from sqlalchemy.exc import ResourceClosedError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine
from starlette.requests import Request


def test_protected_handler_cannot_commit_after_its_lock_connection_is_invalidated(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/10. A late independent commit is the regression."""

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/10. Drive the real app and existing PATCH handler."""
        app = create_app()
        async with app.router.lifespan_context(app):
            factory = app.state.sessionmaker
            ids = await agents(factory)
            entered, release = asyncio.Event(), asyncio.Event()
            original = agent_crud.update_agent_model

            async def paused(session: Any, agent: Any, model: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-10. Delegate the actual product write."""
                if agent.id == ids[0]:
                    entered.set()
                    await release.wait()
                return await original(session, agent, model)

            with monkeypatch.context() as observed:
                observed.setattr(agent_crud, "update_agent_model", paused)
                with ConnectionObserver(app.state.engine, observed) as connections:
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                        base_url="http://testserver",
                    ) as client:
                        first = asyncio.create_task(
                            client.patch(
                                f"/agents/{ids[0]}",
                                headers=headers(),
                                json={"model": "synthetic-late-first"},
                            )
                        )
                        try:
                            await asyncio.wait_for(entered.wait(), timeout=5)
                            owner = await connections.lock_owner()
                            await owner.invalidate()
                            second = await asyncio.wait_for(
                                client.patch(
                                    f"/agents/{ids[1]}",
                                    headers=headers(),
                                    json={"model": "synthetic-valid-second"},
                                ),
                                timeout=5,
                            )
                            assert second.status_code == 200, second.text
                            successor = await app.state.valkey.get(identity_fence.FENCE_KEY)
                            assert successor.startswith(b"stable:")
                            release.set()
                            first_response = await asyncio.wait_for(first, timeout=5)
                            # Response can already be an error; persisted state is authoritative.
                            persisted = await models(factory, ids)
                            assert persisted == ["synthetic-original", "synthetic-valid-second"], (
                                "late writer committed after its serialization connection was lost",
                                persisted,
                                first_response.status_code,
                            )
                            assert await app.state.valkey.get(identity_fence.FENCE_KEY) == successor
                        finally:
                            release.set()
                            if not first.done():
                                first.cancel()
                                await asyncio.gather(first, return_exceptions=True)

    run(body)


@pytest.mark.parametrize("expire", [False, True])
def test_bound_session_preserves_real_commits_and_reader_serialization(
    migrated: None, monkeypatch: pytest.MonkeyPatch, expire: bool
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-2/3. Transaction commits must not drop the writer lock."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-2/3."""
        async with backend() as (engine, factory, client):
            factory.configure(expire_on_commit=expire)
            ids = await agents(factory)
            pending = None
            async with engine.connect() as reader, engine.connect() as observation:
                async with (
                    reader_session(reader) as reading,
                    reader_session(observation) as watching,
                ):
                    reader_pid = await reader.scalar(text("SELECT pg_backend_pid()"))
                    try:
                        async with service.mutation(factory, client) as session:
                            assert isinstance(session, AsyncSession)
                            assert session.sync_session.expire_on_commit is expire
                            connection = await session.connection()
                            original = driver(connection)
                            writer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
                            pending = asyncio.create_task(service.lock_identity_reader(reading))
                            await blocking(watching, reader_pid, writer_pid)
                            await session.execute(update_model(ids[0], "synthetic-committed-one"))
                            await session.commit()
                            assert await models(factory, ids[:1]) == ["synthetic-committed-one"]
                            assert driver(connection) is original and not pending.done()
                            await session.execute(update_model(ids[0], "synthetic-rolled-back"))
                            await session.rollback()
                            assert driver(connection) is original and not pending.done()
                            await session.close()
                            assert not connection.closed and driver(connection) is original
                            await session.execute(update_model(ids[0], "synthetic-committed-two"))
                            await session.commit()
                            assert driver(connection) is original and not pending.done()
                            await session.execute(update_model(ids[0], "synthetic-unfinished"))
                        await asyncio.wait_for(pending, timeout=4)
                        await reading.rollback()
                        assert await models(factory, ids[:1]) == ["synthetic-committed-two"]
                    finally:
                        if pending is not None and not pending.done():
                            pending.cancel()
                            await asyncio.gather(pending, return_exceptions=True)

    run(body)


def reader_session(connection: Any) -> Any:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3. A real bound session for shared-reader SQL."""
    return AsyncSession(bind=connection, expire_on_commit=False)


def update_model(identifier: Any, value: str) -> Any:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3/4. Independently authored parameterized SQL."""
    return text("UPDATE curie.agents SET model=:model WHERE id=:id").bindparams(
        model=value, id=identifier
    )


def test_request_borrowing_and_closed_marker_never_fall_back(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-2. Borrower exit does not own the protected lifetime."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-2."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            req = request(factory)
            assert service.request_owned_session(req) is None
            async with deps.request_session(req) as ordinary:
                assert isinstance(ordinary, AsyncSession)
            async with service.mutation(factory, client, request=req) as owner:
                assert service.request_owned_session(req) is owner
                connection = await owner.connection()
                physical = driver(connection)
                async with deps.request_session(req) as borrowed:
                    assert borrowed is owner
                    assert await borrowed.connection() is connection
                    await borrowed.execute(update_model(ids[0], "synthetic-borrowed-write"))
                assert owner.in_transaction(), "borrower exit ended unfinished owner work"
                assert await owner.connection() is connection and driver(connection) is physical
                await owner.commit()
                assert await models(factory, ids[:1]) == ["synthetic-borrowed-write"]
                assert driver(connection) is physical
                async with asyncio.timeout(2):
                    with pytest.raises(service.IdentityWriterUnavailable):
                        async with service.mutation(factory, client, request=req):
                            pytest.fail("nested owner was admitted")
            with pytest.raises(service.IdentityWriterUnavailable):
                service.request_owned_session(req)
            with pytest.raises(service.IdentityWriterUnavailable):
                async with deps.request_session(req):
                    pytest.fail("closed marker fell back to an independent session")

    run(body)


@pytest.mark.parametrize("operation", ["execute", "flush", "raw", "commit", "new_session"])
def test_rollback_and_session_reuse_do_not_clear_poison(migrated: None, operation: str) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/5. Every continuation must refuse before changing
    data."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/5."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            req = request(factory)
            with pytest.raises(service.IdentityWriterUnavailable):
                async with service.mutation(factory, client, request=req) as session:
                    connection = await session.connection()
                    await connection.invalidate()
                    await session.rollback()
                    with pytest.raises(service.IdentityWriterUnavailable):
                        service.request_owned_session(req)
                    with pytest.raises(service.IdentityWriterUnavailable):
                        if operation == "execute":
                            await session.execute(update_model(ids[0], "synthetic-forbidden"))
                        elif operation == "flush":
                            session.add(Tenant(deployment_id=f"forbidden-{uuid.uuid4()}"))
                            await session.flush()
                        elif operation == "raw":
                            await connection.exec_driver_sql(
                                "UPDATE curie.agents SET model='synthetic-forbidden' "
                                f"WHERE id='{ids[0]}'"
                            )
                        elif operation == "commit":
                            await connection.begin()
                            await connection.commit()
                        else:
                            async with AsyncSession(bind=connection) as reused:
                                await reused.execute(update_model(ids[0], "synthetic-forbidden"))
            assert await models(factory, ids[:1]) == ["synthetic-original"]
            assert (await client.get(service.FENCE_KEY)).startswith(b"pending:")

    run(body)


def test_unrelated_invalidation_and_listener_cleanup(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/9. A different driver cannot poison this writer."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/9."""
        async with backend() as (engine, factory, client):
            ids = await agents(factory)
            async with service.mutation(factory, client) as session:
                connection = await session.connection()
                original = driver(connection)
                async with engine.connect() as unrelated:
                    assert driver(unrelated) is not original
                    await unrelated.invalidate()
                await session.execute(update_model(ids[0], "synthetic-healthy"))
                await session.commit()
                assert driver(connection) is original
            async with service.mutation(factory, client) as next_session:
                await next_session.execute(update_model(ids[1], "synthetic-next"))
                await next_session.commit()
            assert await models(factory, ids) == ["synthetic-healthy", "synthetic-next"]

    run(body)


def test_early_owner_close_cannot_leak_a_locked_pool_connection(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/6. Early checkin must discard its own physical lease."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/6."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            with pytest.raises(service.IdentityWriterUnavailable):
                async with service.mutation(factory, client) as session:
                    connection = await session.connection()
                    old_driver = driver(connection)
                    await connection.close()
                    # A distinct engine must acquire without borrowing the locked owner pool entry.
                    verifier = create_async_engine(get_settings().database_url)
                    try:
                        async with verifier.connect() as check:
                            assert await check.scalar(
                                text("SELECT pg_try_advisory_xact_lock_shared(2250220)")
                            )
                    finally:
                        await verifier.dispose()
                    with pytest.raises((service.IdentityWriterUnavailable, ResourceClosedError)):
                        await session.execute(update_model(ids[0], "synthetic-forbidden"))
            async with factory() as fresh:
                connection = await fresh.connection()
                assert driver(connection) is not old_driver
                assert await fresh.scalar(text("SELECT pg_try_advisory_xact_lock_shared(2250220)"))
            assert await models(factory, ids[:1]) == ["synthetic-original"]

    run(body)


@pytest.mark.parametrize(
    "mode",
    [
        "pending_before",
        "pending_after",
        "pending_false",
        "pending_truthy_invalid",
        "stable_before",
        "stable_after",
        "stable_truthy_invalid",
        "changed_token",
    ],
)
def test_pending_and_stable_acknowledgement_outcomes(migrated: None, mode: str) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. Lost acknowledgements do not prove nonexecution."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-5/6."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            wrapped = AckClient(client, mode)
            entered = False
            error = (
                service.IdentityWriterUnavailable
                if mode
                in {
                    "pending_false",
                    "pending_truthy_invalid",
                    "stable_truthy_invalid",
                    "changed_token",
                }
                else AckLost
            )
            with pytest.raises(error):
                async with service.mutation(factory, wrapped) as session:
                    entered = True
                    await session.execute(update_model(ids[0], "synthetic-committed"))
                    await session.commit()
                    if mode == "changed_token":
                        await client.set(service.FENCE_KEY, "stable:synthetic-successor")
            value = await client.get(service.FENCE_KEY)
            assert entered is (not mode.startswith("pending"))
            assert await models(factory, ids[:1]) == [
                "synthetic-committed" if entered else "synthetic-original"
            ]
            if mode == "pending_before":
                assert value == b"stable:synthetic-before" and wrapped.sent == []
            elif mode in {
                "pending_after",
                "pending_false",
                "pending_truthy_invalid",
                "stable_before",
            }:
                assert value.startswith(b"pending:")
            elif mode == "changed_token":
                assert value == b"stable:synthetic-successor"
            else:
                assert value.startswith(b"stable:") and "eval" in wrapped.sent

    run(body)


def test_ordinary_error_rolls_back_unfinished_work_after_durable_commit(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3/5/6. Cleanup cannot undo an acknowledged earlier
    commit."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-3/5/6."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            with pytest.raises(RuntimeError, match="synthetic-primary"):
                async with service.mutation(factory, client) as session:
                    await session.execute(update_model(ids[0], "synthetic-committed"))
                    await session.commit()
                    await session.execute(update_model(ids[1], "synthetic-unfinished"))
                    raise RuntimeError("synthetic-primary")
            assert await models(factory, ids) == ["synthetic-committed", "synthetic-original"]
            assert (await client.get(service.FENCE_KEY)).startswith(b"stable:")

    run(body)


@pytest.mark.parametrize("fault", ["false_unlock", "lost_unlock_ack"])
def test_failed_unlock_discards_the_exact_connection(
    migrated: None, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-6. An uncertain session lock never returns to the pool."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
        async with backend() as (engine, factory, client):
            ids = await agents(factory)
            actual = None

            def lost(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6. Real unlock executes before ACK loss."""
                if connection is actual.sync_connection and "pg_advisory_unlock(" in statement:
                    raise AckLost("synthetic-unlock-ack")

            expected = service.IdentityWriterUnavailable if fault == "false_unlock" else AckLost
            with pytest.raises(expected):
                async with service.mutation(factory, client) as session:
                    actual = await session.connection()
                    original = driver(actual)
                    await session.execute(update_model(ids[0], "synthetic-committed"))
                    await session.commit()
                    if fault == "false_unlock":
                        # Release only this test's own lock; the product cleanup then sees real
                        # False.
                        assert await actual.scalar(text("SELECT pg_advisory_unlock(2250220)"))
                        await actual.commit()
                    else:
                        event.listen(engine.sync_engine, "after_cursor_execute", lost)
            if fault == "lost_unlock_ack":
                event.remove(engine.sync_engine, "after_cursor_execute", lost)
            assert original.is_closed()
            async with engine.connect() as fresh:
                assert driver(fresh) is not original
                assert await fresh.scalar(text("SELECT pg_try_advisory_xact_lock_shared(2250220)"))
            assert await models(factory, ids[:1]) == ["synthetic-committed"]

    run(body)


@pytest.mark.parametrize("primary", [False, True])
def test_cleanup_failure_preserves_primary_exception_without_payload_log(
    migrated: None, caplog: pytest.LogCaptureFixture, primary: bool
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-6. Secondary cleanup cannot mask the primary failure."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            expected = ValueError if primary else AckLost
            with pytest.raises(
                expected, match="synthetic-primary" if primary else "stable_ack_lost"
            ):
                async with service.mutation(factory, AckClient(client, "stable_before")) as session:
                    await session.execute(update_model(ids[0], "synthetic-committed"))
                    await session.commit()
                    if primary:
                        raise ValueError("synthetic-primary")
            assert await models(factory, ids[:1]) == ["synthetic-committed"]
            assert (await client.get(service.FENCE_KEY)).startswith(b"pending:")
            assert "synthetic-primary" not in caplog.text
            assert "stable_ack_lost" not in caplog.text
            if primary:
                assert caplog.records, "secondary cleanup failure must be recorded"

    run(body)


@pytest.mark.parametrize("phase", ["settlement", "publication", "unlock"])
def test_cleanup_deadline_discards_instead_of_returning_an_uncertain_lease(
    migrated: None, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-6. Bounded client-local stalls require truthful failure."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
        async with backend() as (engine, factory, client):
            ids = await agents(factory)
            stalled, reached = asyncio.Event(), asyncio.Event()
            actual_session, actual_connection = None, None
            original_close, original_rollback = AsyncSession.close, AsyncSession.rollback
            original_execute = AsyncConnection.execute
            original_scalar = AsyncConnection.scalar

            async def settle(
                session: AsyncSession, operation: Any, *args: Any, **kwargs: Any
            ) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6. Pause either allowed settlement seam."""
                if session is actual_session and phase == "settlement":
                    reached.set()
                    await stalled.wait()
                return await operation(session, *args, **kwargs)

            async def close(session: AsyncSession, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                return await settle(session, original_close, *args, **kwargs)

            async def rollback(session: AsyncSession, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                return await settle(session, original_rollback, *args, **kwargs)

            class DelayedPublication(AckClient):
                """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. Delay only the actual EVAL ACK."""

                async def eval(self, *args: Any, **kwargs: Any) -> Any:
                    """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. Server effect precedes ACK stall."""
                    result = await super().eval(*args, **kwargs)
                    if phase == "publication":
                        reached.set()
                        await stalled.wait()
                    return result

            async def execute(
                connection: AsyncConnection, statement: Any, *args: Any, **kwargs: Any
            ) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                if (
                    connection is actual_connection
                    and phase == "unlock"
                    and "pg_advisory_unlock(" in str(statement)
                ):
                    reached.set()
                    await stalled.wait()
                return await original_execute(connection, statement, *args, **kwargs)

            async def scalar(
                connection: AsyncConnection, statement: Any, *args: Any, **kwargs: Any
            ) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                if (
                    connection is actual_connection
                    and phase == "unlock"
                    and "pg_advisory_unlock(" in str(statement)
                ):
                    reached.set()
                    await stalled.wait()
                return await original_scalar(connection, statement, *args, **kwargs)

            with monkeypatch.context() as fault:
                fault.setattr(AsyncSession, "close", close)
                fault.setattr(AsyncSession, "rollback", rollback)
                fault.setattr(AsyncConnection, "execute", execute)
                fault.setattr(AsyncConnection, "scalar", scalar)
                started = asyncio.get_running_loop().time()
                try:
                    with pytest.raises(service.IdentityWriterUnavailable):
                        async with service.mutation(
                            factory, DelayedPublication(client, "normal")
                        ) as session:
                            actual_session = session
                            actual_connection = await session.connection()
                            original = driver(actual_connection)
                            await session.execute(update_model(ids[0], "synthetic-committed"))
                            await session.commit()
                            await session.execute(update_model(ids[1], "synthetic-unfinished"))
                    assert reached.is_set(), "the selected cleanup fault seam was not reached"
                    assert 4.5 <= asyncio.get_running_loop().time() - started < 12
                finally:
                    stalled.set()
            assert original.is_closed()
            assert (await client.get(service.FENCE_KEY)).startswith(
                b"pending:" if phase == "settlement" else b"stable:"
            )
            assert await models(factory, ids) == ["synthetic-committed", "synthetic-original"]
            async with engine.connect() as connection:
                assert await connection.scalar(
                    text("SELECT pg_try_advisory_xact_lock_shared(2250220)")
                )

    run(body)


def test_unconfirmed_discard_retains_quarantine_guards_until_delayed_original_checkin(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/6. Never invalidate a saved, reassigned pool entry."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/6."""
        import gc
        import weakref

        async with backend() as (engine, factory, client):
            release = asyncio.Event()
            captured = []
            actual = None
            original_listen = event.listen
            original_invalidate, original_close = AsyncConnection.invalidate, AsyncConnection.close

            def listen(target: Any, name: str, function: Any, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-4. Observe real hooks without retaining
                owners."""
                if name in {
                    "before_execute",
                    "before_cursor_execute",
                    "commit",
                    "checkin",
                    "invalidate",
                }:
                    captured.append((weakref.ref(target), name, weakref.ref(function)))
                return original_listen(target, name, function, *args, **kwargs)

            async def invalidate(connection: AsyncConnection, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6. Delay only this owned discard
                acknowledgement."""
                if connection is actual and not release.is_set():
                    await release.wait()
                return await original_invalidate(connection, *args, **kwargs)

            async def close(connection: AsyncConnection, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                if connection is actual and not release.is_set():
                    await release.wait()
                return await original_close(connection, *args, **kwargs)

            def unlock_ack(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6. Server unlock runs; client loses its ACK."""
                if (
                    actual is not None
                    and connection is actual.sync_connection
                    and "pg_advisory_unlock(" in statement
                ):
                    raise AckLost("synthetic-unlock-ack")

            req = request(factory)
            with monkeypatch.context() as fault:
                fault.setattr(event, "listen", listen)
                fault.setattr(AsyncConnection, "invalidate", invalidate)
                fault.setattr(AsyncConnection, "close", close)
                event.listen(engine.sync_engine, "after_cursor_execute", unlock_ack)
                try:
                    with pytest.raises((AckLost, service.IdentityWriterUnavailable)):
                        async with service.mutation(factory, client, request=req) as session:
                            actual = await session.connection()
                            original_driver = driver(actual)
                            await session.execute(text("SELECT 1"))
                    with pytest.raises(service.IdentityWriterUnavailable):
                        service.request_owned_session(req)
                    assert {
                        name
                        for target, name, function in captured
                        if target() is not None
                        and function() is not None
                        and event.contains(target(), name, function())
                    } >= {
                        "before_execute",
                        "before_cursor_execute",
                        "commit",
                        "checkin",
                        "invalidate",
                    }
                    connection_ref = weakref.ref(actual)
                    # Keep only weak observer references; the product must retain its failed lease.
                    del req, session
                    retained = actual
                    actual = None
                    del retained
                    gc.collect()
                    assert connection_ref() is not None
                    actual = connection_ref()
                    release.set()
                    event.remove(engine.sync_engine, "after_cursor_execute", unlock_ack)
                    await actual.close()
                    assert original_driver.is_closed()
                    async with engine.connect() as unrelated:
                        assert driver(unrelated) is not original_driver
                        assert await unrelated.scalar(
                            text("SELECT pg_try_advisory_xact_lock_shared(2250220)")
                        )
                        await unrelated.commit()
                        assert await unrelated.scalar(text("SELECT 1")) == 1
                finally:
                    release.set()
                    if event.contains(engine.sync_engine, "after_cursor_execute", unlock_ack):
                        event.remove(engine.sync_engine, "after_cursor_execute", unlock_ack)
                    if actual is not None:
                        if not actual.closed:
                            await original_invalidate(actual)
                        await original_close(actual)

    run(body)


@pytest.mark.parametrize("phase", ["background", "dependency", "stream"])
@pytest.mark.parametrize("outcome", ["success", "error", "cancel"])
def test_full_asgi_body_and_finalizer_lifetime(
    migrated: None, monkeypatch: pytest.MonkeyPatch, phase: str, outcome: str
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3/6/8. Headers are not completion of awaited work."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-3/6/8."""
        from fastapi import Depends
        from starlette.background import BackgroundTask
        from starlette.responses import Response, StreamingResponse

        app = create_app()
        entered, release = asyncio.Event(), asyncio.Event()
        seen = []
        ids = []

        async def finish(session: Any) -> None:
            """@spec IDENTITY-WRITER-LIFETIME-V1-6/8. Continue actual SQL only on the borrowed
            owner."""
            entered.set()
            await release.wait()
            if outcome == "error":
                raise ValueError("synthetic-child-error")
            await session.execute(update_model(ids[0], "synthetic-finalized"))
            await session.commit()

        async def dependency(session: deps.SessionDep) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Request finalizer remains inside owner."""
            try:
                yield session
            finally:
                seen.append("dependency-finalizer")
                if phase == "dependency":
                    await finish(session)

        borrowing = Depends(dependency, scope="request")

        async def endpoint(session: deps.SessionDep, borrowed: Any = borrowing) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Diagnostic route uses real protected
            injection."""
            assert borrowed is session
            await session.execute(update_model(ids[0], "synthetic-endpoint-committed"))
            await session.commit()
            if phase == "stream":

                async def chunks() -> Any:
                    """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
                    yield b"synthetic-first"
                    await finish(session)
                    yield b"synthetic-last"

                return StreamingResponse(chunks())
            return Response(
                b"synthetic-body",
                background=BackgroundTask(finish, session) if phase == "background" else None,
            )

        path = install_lifetime_route(app, endpoint)

        async def observed(scope: Any, receive: Any, send: Any) -> None:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Record the actual response-start fact."""

            async def sent(message: Any) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
                if message["type"] == "http.response.start":
                    seen.append(message["status"])
                await send(message)

            await app(scope, receive, sent)

        async with app.router.lifespan_context(app):
            ids.extend(await agents(app.state.sessionmaker))
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=observed), base_url="http://testserver"
            ) as client:
                with monkeypatch.context() as patch:
                    with ConnectionObserver(app.state.engine, patch) as observed_connections:
                        task = asyncio.create_task(client.post(path, headers=headers()))
                        second = None
                        try:
                            await asyncio.wait_for(entered.wait(), timeout=5)
                            assert 200 in seen
                            assert (await app.state.valkey.get(service.FENCE_KEY)).startswith(
                                b"pending:"
                            )
                            owner = await observed_connections.lock_owner()
                            holder_pid = driver(owner).get_server_pid()
                            second = asyncio.create_task(
                                client.patch(
                                    f"/agents/{ids[1]}",
                                    headers=headers(),
                                    json={"model": "synthetic-second"},
                                )
                            )
                            await wait_locks(observed_connections, 2)
                            waiter = observed_connections.lock_connections[
                                1
                            ].connection.driver_connection.get_server_pid()
                            async with app.state.sessionmaker() as observer:
                                await blocking(observer, waiter, holder_pid)
                            assert await models(app.state.sessionmaker, ids) == [
                                "synthetic-endpoint-committed",
                                "synthetic-original",
                            ]
                            if outcome == "cancel":
                                task.cancel()
                                with pytest.raises(asyncio.CancelledError):
                                    await task
                            else:
                                release.set()
                                if outcome == "error":
                                    with pytest.raises(ValueError, match="synthetic-child-error"):
                                        await task
                                else:
                                    assert (await task).status_code == 200
                            assert (await asyncio.wait_for(second, timeout=5)).status_code == 200
                            assert "dependency-finalizer" in seen
                            expected = (
                                "synthetic-finalized"
                                if outcome == "success"
                                else "synthetic-endpoint-committed"
                            )
                            assert await models(app.state.sessionmaker, ids) == [
                                expected,
                                "synthetic-second",
                            ]
                        finally:
                            release.set()
                            for pending in (task, second):
                                if pending is not None and not pending.done():
                                    pending.cancel()
                                    await asyncio.gather(pending, return_exceptions=True)

    run(body)


@pytest.mark.parametrize("kind", ["json", "form"])
def test_protected_request_replays_exact_chunked_body_once(migrated: None, kind: str) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-8. Replay is bytes, not JSON parsing/reencoding."""
    api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
        from starlette.responses import Response

        app = create_app()
        raw = (
            b'{ "key" : "value\\u0020one" } \n'
            if kind == "json"
            else b"field=one%20two&field=three+four"
        )
        received = []

        async def endpoint(request: Request, session: deps.SessionDep) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Use the actual protected session dependency."""
            received.append(await request.body())
            assert await session.scalar(text("SELECT 1")) == 1
            return Response(b"ok")

        path = install_lifetime_route(app, endpoint)

        async def chunks() -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
            for index in range(0, len(raw), 3):
                yield raw[index : index + 3]

        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                response = await client.post(
                    path,
                    content=chunks(),
                    headers={
                        **headers(),
                        "Content-Type": "application/json"
                        if kind == "json"
                        else "application/x-www-form-urlencoded",
                    },
                )
                assert response.status_code == 200
        assert received == [raw]

    run(body)


@pytest.mark.parametrize(
    "failure",
    [
        "unauthenticated",
        "timeout",
        "declared_oversize",
        "chunked_oversize",
        "cookie_missing_origin",
        "cookie_wrong_origin",
    ],
)
def test_prepare_refusals_keep_real_app_telemetry_and_perform_no_writer_sql(
    migrated: None, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-1/8. Refused preparation stays inside observability."""
    api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-1/8."""
        import curie_api.main as main
        from curie_api.auth import CONSOLE_SESSION_COOKIE

        app = create_app()
        metrics, spans, locks = [], [], []
        original_metric, original_span = main.record_metric, main.operation_span

        def measured(name: Any, *args: Any, **kwargs: Any) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Delegate actual telemetry while observing it."""
            metrics.append((name, args, kwargs))
            return original_metric(name, *args, **kwargs)

        @contextmanager
        def spanned(name: Any, *args: Any, **kwargs: Any) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
            with original_span(name, *args, **kwargs) as span:
                spans.append(name)
                yield span

        async def contents() -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Actual receive stalls/bytes drive the real
            bound."""
            if failure == "timeout":
                await asyncio.Event().wait()
            elif failure == "chunked_oversize":
                yield b"x" * (16 * 1024 * 1024)
                yield b"x"
            else:
                yield b"{}"

        def lock_sql(connection: Any, cursor: Any, statement: str, *args: Any) -> None:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
            if "pg_advisory" in statement:
                locks.append(statement)

        with monkeypatch.context() as observed:
            observed.setattr(main, "record_metric", measured)
            observed.setattr(main, "operation_span", spanned)
            async with app.router.lifespan_context(app):
                await app.state.valkey.set(identity_fence.FENCE_KEY, "stable:unchanged")
                event.listen(app.state.engine.sync_engine, "before_cursor_execute", lock_sql)
                try:
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
                    ) as client:
                        request_headers = (
                            {}
                            if failure == "unauthenticated" or failure.startswith("cookie_")
                            else headers()
                        )
                        if failure.startswith("cookie_"):
                            request_headers["Cookie"] = (
                                f"{CONSOLE_SESSION_COOKIE}=synthetic-invalid-console-token"
                            )
                            if failure == "cookie_wrong_origin":
                                request_headers["Origin"] = "https://untrusted.example.test"
                        if failure == "declared_oversize":
                            request_headers["Content-Length"] = str(16 * 1024 * 1024 + 1)
                        response = await client.patch(
                            f"/agents/{uuid.uuid4()}", headers=request_headers, content=contents()
                        )
                    expected = (
                        403
                        if failure.startswith("cookie_")
                        else 401
                        if failure == "unauthenticated"
                        else 408
                        if failure == "timeout"
                        else 413
                    )
                    assert response.status_code == expected
                    if failure == "timeout":
                        assert response.json() == {"detail": "request body timeout"}
                    if failure.startswith("cookie_"):
                        assert response.json() == {"detail": "console session origin rejected"}
                    assert locks == []
                    assert (
                        await app.state.valkey.get(identity_fence.FENCE_KEY) == b"stable:unchanged"
                    )
                finally:
                    event.remove(app.state.engine.sync_engine, "before_cursor_execute", lock_sql)
        names = [row[0] for row in metrics]
        assert "http.server.request" in spans
        assert {
            "curie.http.server.active",
            "curie.http.server.request",
            "curie.http.server.request.duration",
        } <= set(names)
        requests = [kwargs for name, _, kwargs in metrics if name == "curie.http.server.request"]
        assert requests[-1]["attributes"]["outcome"] == "4xx"
        active = [args[0] for name, args, _ in metrics if name == "curie.http.server.active"]
        assert active == [1, -1]
        assert response.headers["Referrer-Policy"] == "no-referrer"
        assert response.headers["Cache-Control"] == "private, no-store"

    run(body)


@pytest.mark.parametrize(
    "case", ["record", "poison", "bad_credential", "negative_epoch", "stale_epoch", "mixed_header"]
)
def test_directory_validation_recorder_uses_owner_and_rolls_back_incomplete_dependency_work(
    migrated: None, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-7. Malformed bodies may not open a rescue connection."""
    api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-7."""
        from curie_api import directory_sync

        app = create_app()
        async with app.router.lifespan_context(app):
            factory = app.state.sessionmaker
            async with factory() as seed_session, seed_session.begin():
                tenant = Tenant(deployment_id=f"synthetic-directory-{uuid.uuid4()}")
                seed_session.add(tenant)
                await seed_session.flush()
                tenant_id = tenant.id
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://testserver",
            ) as client:
                configured = await client.put(
                    f"/directory/config/{tenant_id}",
                    headers=headers(),
                    json={
                        "issuer": "https://directory.example.test",
                        "governed_domains": ["example.test"],
                        "managed_by": "synthetic",
                        "expected_version": 0,
                    },
                )
                assert configured.status_code == 200, configured.text
                team_response = await client.post(
                    "/directory/teams",
                    headers=headers(),
                    json={
                        "tenant_id": str(tenant_id),
                        "external_id": "synthetic-group",
                        "name": "Synthetic",
                    },
                )
                assert team_response.status_code == 201, team_response.text
                team_id = team_response.json()["id"]
                credential_response = await client.post(
                    "/directory/credentials",
                    headers=headers(),
                    json={
                        "tenant_id": str(tenant_id),
                        "expected_config_version": 1,
                        "team_ids": [team_id],
                        "namespace_ids": [],
                        "expires_in_seconds": 3600,
                    },
                )
                assert credential_response.status_code == 201, credential_response.text
                credential = credential_response.json()["credential"]
                pending_id = uuid.uuid4()
                recorded = []
                original_record = directory_sync.record_failure

                async def record(session: AsyncSession, *args: Any, **kwargs: Any) -> Any:
                    """@spec IDENTITY-WRITER-LIFETIME-V1-7. Observe the actual failure writer."""
                    connection = await session.connection()
                    recorded.append(connection.sync_connection)
                    if case == "poison":
                        await connection.invalidate()
                    return await original_record(session, *args, **kwargs)

                with monkeypatch.context() as observed:
                    observed.setattr(directory_sync, "record_failure", record)
                    with ConnectionObserver(app.state.engine, observed) as connections:
                        original_init = AsyncSession.__init__
                        staged = []

                        def stage(session: AsyncSession, *args: Any, **kwargs: Any) -> None:
                            """@spec IDENTITY-WRITER-LIFETIME-V1-7. Real pending work, not a
                            session override."""
                            original_init(session, *args, **kwargs)
                            if isinstance(kwargs.get("bind"), AsyncConnection):
                                session.add(
                                    Tenant(
                                        id=pending_id,
                                        deployment_id=f"synthetic-incomplete-{pending_id}",
                                    )
                                )
                                staged.append(session)

                        observed.setattr(AsyncSession, "__init__", stage)
                        request_headers = {"X-Curie-Directory-Credential": credential}
                        if case == "bad_credential":
                            request_headers["X-Curie-Directory-Credential"] = (
                                "dir.synthetic-invalid"
                            )
                        if case == "mixed_header":
                            request_headers.update(headers())
                        response = await asyncio.wait_for(
                            client.put(
                                f"/directory/teams/{team_id}/snapshot",
                                headers=request_headers,
                                json={
                                    "expected_epoch": -1
                                    if case == "negative_epoch"
                                    else 1
                                    if case == "stale_epoch"
                                    else 0,
                                    "complete": True,
                                    "people": [
                                        {
                                            "subject": "SyntheticPayloadMarker",
                                            "accounts": "invalid-private-shape",
                                        }
                                    ],
                                },
                            ),
                            timeout=5,
                        )
                        if case == "poison":
                            assert response.status_code >= 500
                        else:
                            assert response.status_code == (
                                401 if case == "bad_credential" else 422
                            )
                            assert "SyntheticPayloadMarker" not in response.text
                            assert "invalid-private-shape" not in response.text
                        if case in {"record", "poison", "stale_epoch"}:
                            assert staged and recorded
                            assert recorded[0] is connections.lock_connections[0]
                async with factory() as observer:
                    assert await observer.get(Tenant, pending_id) is None
                    events = list(
                        (
                            await observer.execute(
                                text(
                                    "SELECT details FROM curie.identity_audit_events "
                                    "WHERE tenant_id=:tenant "
                                    "AND event_type='identity.directory_sync_failed'"
                                ),
                                {"tenant": tenant_id},
                            )
                        ).scalars()
                    )
                    assert events == (
                        [{"epoch": 0, "error": "invalid_snapshot"}] if case == "record" else []
                    )

    run(body)


def test_unrecognized_request_marker_is_not_an_independent_session_fallback(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-2. Unknown state is not evidence of no owner."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-2."""
        async with backend() as (_, factory, client):
            req = request(factory)
            async with service.mutation(factory, client, request=req):
                state_keys = set(req.scope["state"])
                assert state_keys
            foreign = request(factory)
            foreign.scope["state"].update({key: object() for key in state_keys})
            with pytest.raises(service.IdentityWriterUnavailable):
                service.request_owned_session(foreign)
            with pytest.raises(service.IdentityWriterUnavailable):
                async with deps.request_session(foreign):
                    pytest.fail("unknown marker silently opened an independent session")

    run(body)


def test_confirmed_disposition_removes_per_owner_listeners(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/9. Completed owners leave no guards on pooled
    resources."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/9."""
        async with backend() as (_, factory, client):
            captured = []
            original = event.listen

            def listened(target: Any, name: str, callback: Any, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-4. Only observe public event registration."""
                if name in {
                    "before_execute",
                    "before_cursor_execute",
                    "commit",
                    "invalidate",
                    "checkin",
                }:
                    captured.append((target, name, callback))
                return original(target, name, callback, *args, **kwargs)

            with monkeypatch.context() as observed:
                observed.setattr(event, "listen", listened)
                async with service.mutation(factory, client) as session:
                    await session.execute(text("SELECT 1"))
            assert {name for _, name, _ in captured} >= {
                "before_execute",
                "before_cursor_execute",
                "commit",
                "invalidate",
                "checkin",
            }
            assert all(
                not event.contains(target, name, callback) for target, name, callback in captured
            )
            async with service.mutation(factory, client) as session:
                assert await session.scalar(text("SELECT 2")) == 2

    run(body)


@pytest.mark.parametrize("when", ["lock_wait", "child", "cleanup"])
def test_cancellation_preserves_outcome_and_releases_owned_session_lock(
    migrated: None, when: str
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-6/8. Cancellation does not waive owned cleanup."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-6/8."""
        async with backend() as (engine, factory, client):
            entered, release, publishing = asyncio.Event(), asyncio.Event(), asyncio.Event()

            class PausedAck(AckClient):
                """@spec IDENTITY-WRITER-LIFETIME-V1-6. Hold an actual cleanup client operation."""

                async def eval(self, *args: Any, **kwargs: Any) -> Any:
                    """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                    if when == "cleanup":
                        publishing.set()
                        await release.wait()
                    return await super().eval(*args, **kwargs)

            async def writer() -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                async with service.mutation(factory, PausedAck(client, "normal")) as session:
                    await session.execute(text("SELECT 1"))
                    entered.set()
                    if when != "cleanup":
                        await release.wait()

            async with engine.connect() as holder:
                if when == "lock_wait":
                    await holder.execute(text("SELECT pg_advisory_xact_lock(2250220)"))
                task = asyncio.create_task(writer())
                try:
                    if when == "lock_wait":
                        # An observed wait, rather than elapsed sleep, establishes lock admission.
                        async with factory() as observer:
                            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
                            async with asyncio.timeout(4):
                                while not await observer.scalar(
                                    text(
                                        "SELECT EXISTS(SELECT 1 FROM pg_locks "
                                        "WHERE locktype='advisory' "
                                        "AND objid=2250220 AND NOT granted AND pid<>:pid)"
                                    ),
                                    {"pid": holder_pid},
                                ):
                                    await asyncio.sleep(0.01)
                    elif when == "cleanup":
                        await asyncio.wait_for(publishing.wait(), timeout=4)
                    else:
                        await asyncio.wait_for(entered.wait(), timeout=4)
                    task.cancel()
                    release.set()
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(task, timeout=12)
                finally:
                    release.set()
                    await holder.rollback()
                    if not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
            async with engine.connect() as verifier:
                assert await verifier.scalar(
                    text("SELECT pg_try_advisory_xact_lock_shared(2250220)")
                )

    run(body)


@pytest.mark.parametrize(
    "method,path,expected",
    [
        ("POST", "/agents", True),
        ("PATCH", "/agents/00000000-0000-0000-0000-000000000001", True),
        ("PUT", "/agents/a/access-policy", True),
        ("PUT", "/agents/a/channels/callers", True),
        ("POST", "/directory/teams", True),
        ("POST", "/identity/slack-report", True),
        ("POST", "/identity/connect/synthetic/confirm", True),
        ("PATCH", "/channel-identities/a", True),
        ("DELETE", "/provider-installations/a", True),
        ("POST", "/identity/resolve", False),
        ("POST", "/channels/token", False),
        ("PUT", "/agents/a/versions/b/bundle", False),
        ("POST", "/agents/a/versions", False),
        ("GET", "/directory/teams/a", False),
        ("OPTIONS", "/agents", False),
    ],
)
def test_protected_inventory_and_exclusions_are_preserved(
    method: str, path: str, expected: bool
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-1. Boundary mapping must not silently widen or shrink."""
    assert identity_fence.protects(method, path) is expected


def test_non_http_and_unprotected_bodies_pass_through_unchanged(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-8. The fence does not consume excluded traffic."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
        for scope in (
            {"type": "websocket", "path": "/identity/synthetic"},
            {"type": "http", "method": "POST", "path": "/channels/token"},
        ):
            received, sent = [], []
            message = {
                "type": "http.request",
                "body": b"exact+unprotected%20bytes",
                "more_body": False,
            }

            async def receive(item: Any = message) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
                return item

            async def send(value: Any, sink: Any = sent) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
                sink.append(value)

            async def child(
                actual_scope: Any,
                actual_receive: Any,
                actual_send: Any,
                expected: Any = scope,
                observations: Any = received,
            ) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
                assert actual_scope is expected
                observations.append(await actual_receive())
                await actual_send({"type": "synthetic.pass-through"})

            await service.IdentityFenceMiddleware(child)(scope, receive, send)
            assert received == [message] and received[0] is message
            assert sent == [{"type": "synthetic.pass-through"}]

    run(body)


def test_stream_disconnect_finishes_dependency_before_owner_disposition(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-6/8. An already-started stream cannot be rewritten."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-6/8."""
        from fastapi import Depends
        from starlette.responses import StreamingResponse

        app = create_app()
        first_body, disconnected = asyncio.Event(), asyncio.Event()
        order, sent, borrowed = [], [], []

        async def dependency(session: deps.SessionDep) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
            try:
                yield session
            finally:
                order.append("dependency-finished")

        dependency_value = Depends(dependency, scope="request")

        async def endpoint(session: deps.SessionDep, resource: Any = dependency_value) -> Any:
            """@spec IDENTITY-WRITER-LIFETIME-V1-8. Preserve the real session borrower."""
            assert resource is session
            borrowed.append(session)
            await session.execute(text("SELECT 1"))

            async def chunks() -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8."""
                try:
                    yield b"synthetic-first"
                    await asyncio.Event().wait()
                finally:
                    order.append("stream-finished")

            return StreamingResponse(chunks())

        path = install_lifetime_route(app, endpoint)
        async with app.router.lifespan_context(app):
            delivered = False

            async def receive() -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8. Simulate only this client's disconnect."""
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": b"", "more_body": False}
                await disconnected.wait()
                return {"type": "http.disconnect"}

            async def send(message: Any) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-8. Record actual response facts."""
                sent.append(dict(message))
                if message["type"] == "http.response.body" and message.get("body"):
                    first_body.set()

            scope_value = {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "method": "POST",
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "headers": [
                    (b"host", b"testserver"),
                    (b"x-api-key", headers()["X-API-Key"].encode()),
                ],
                "client": ("127.0.0.1", 5000),
                "server": ("testserver", 80),
                "root_path": "",
            }
            task = asyncio.create_task(app(scope_value, receive, send))
            try:
                await asyncio.wait_for(first_body.wait(), timeout=4)
                assert (await app.state.valkey.get(service.FENCE_KEY)).startswith(b"pending:")
                disconnected.set()
                await asyncio.wait_for(task, timeout=8)
                assert order == ["stream-finished", "dependency-finished"]
                assert [
                    message["status"]
                    for message in sent
                    if message["type"] == "http.response.start"
                ] == [200]
                assert (await app.state.valkey.get(service.FENCE_KEY)).startswith(b"stable:")
                with pytest.raises((service.IdentityWriterUnavailable, ResourceClosedError)):
                    await borrowed[0].execute(text("SELECT 1"))
                async with app.state.sessionmaker() as observer:
                    assert await observer.scalar(
                        text("SELECT pg_try_advisory_xact_lock_shared(2250220)")
                    )
            finally:
                disconnected.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    run(body)


def test_healthy_unlock_returns_same_physical_driver_without_invalidation(migrated: None) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-3/4/9. Normal disposition keeps the healthy lease."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-3/4/9."""
        async with backend() as (engine, factory, client):
            invalidated = []

            def invalidation(dbapi: Any, record: Any, exception: Any) -> None:
                """@spec IDENTITY-WRITER-LIFETIME-V1-4. Public pool observation only."""
                invalidated.append(dbapi)

            event.listen(engine.sync_engine.pool, "invalidate", invalidation)
            try:
                async with service.mutation(factory, client) as session:
                    connection = await session.connection()
                    original = driver(connection)
                    await session.execute(text("SELECT 1"))
                    await session.commit()
                async with engine.connect() as fresh:
                    assert driver(fresh) is original
                    assert await fresh.scalar(
                        text("SELECT pg_try_advisory_xact_lock_shared(2250220)")
                    )
                assert invalidated == []
            finally:
                event.remove(engine.sync_engine.pool, "invalidate", invalidation)

    run(body)


def test_owner_refusal_has_only_the_fixed_sanitized_error_code() -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-2/6. Refusal exposes no request or connection data."""
    service = api()
    refusal = service.IdentityWriterUnavailable()
    assert str(refusal) == "identity_writer_unavailable"
    assert refusal.args == ("identity_writer_unavailable",)
    assert repr(refusal) == "IdentityWriterUnavailable('identity_writer_unavailable')"


def test_unknown_settlement_does_not_publish_stable(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-5/6. A client settlement exception is not success."""
    service = api()

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-5/6."""
        async with backend() as (_, factory, client):
            ids = await agents(factory)
            actual = None
            failed = False
            original_close, original_rollback = AsyncSession.close, AsyncSession.rollback

            async def settled(
                session: AsyncSession, operation: Any, *args: Any, **kwargs: Any
            ) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6. Lose ACK after the real settlement."""
                nonlocal failed
                result = await operation(session, *args, **kwargs)
                if session is actual and not failed:
                    failed = True
                    raise AckLost("synthetic-settlement-ack")
                return result

            async def close(session: AsyncSession, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                return await settled(session, original_close, *args, **kwargs)

            async def rollback(session: AsyncSession, *args: Any, **kwargs: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-6."""
                return await settled(session, original_rollback, *args, **kwargs)

            with monkeypatch.context() as fault:
                fault.setattr(AsyncSession, "close", close)
                fault.setattr(AsyncSession, "rollback", rollback)
                with pytest.raises(AckLost, match="synthetic-settlement-ack"):
                    async with service.mutation(factory, client) as session:
                        actual = session
                        physical = driver(await session.connection())
                        await session.execute(update_model(ids[0], "synthetic-committed"))
                        await session.commit()
                        await session.execute(update_model(ids[1], "synthetic-unfinished"))
            assert failed and physical.is_closed()
            assert await models(factory, ids) == ["synthetic-committed", "synthetic-original"]
            assert (await client.get(service.FENCE_KEY)).startswith(b"pending:")

    run(body)
