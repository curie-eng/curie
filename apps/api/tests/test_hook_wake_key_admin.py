"""Wake-key administration: GET wake-secret and POST wake-secret/rotate (ADR 0274 decision 2).

Driven through the real app on its own migrated database. The expected key is
the independent reference derivation in ``_hook_wake_support``.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from _hook_wake_support import (
    ADMIN_UNAUTHORIZED,
    BODY_INVALID,
    CRON,
    DECLARATION_UNAVAILABLE,
    GENERATION_EXHAUSTED,
    INVALID_SIGNATURE,
    NOT_DECLARED,
    OTHER_WAKE,
    PLAIN,
    QUERY_FORBIDDEN,
    SOURCE_KINDS,
    SOURCE_NOT_ORDINARY,
    UNDECLARED,
    WAKE,
    WakeApp,
    assert_no_store,
    assert_queued,
    assert_refused,
    drive,
    generation_column_max,
    get_bundle_object,
    hook_runs,
    put_bundle_object,
    rotate_path,
    secret_path,
    seed_configuration,
    seed_generation,
    settle_run,
    signed_headers,
    stored_generation,
    wake,
    wake_app,
    wake_environment,
)
from _migration_support import IsolatedMigrationDb
from sqlalchemy import text

OPERATIONS = ("read", "rotate")


@pytest.fixture
def wake_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    yield from wake_environment(isolated_migration_db, monkeypatch)


async def admin(
    ctx: WakeApp,
    operation: str,
    *,
    agent_id: str | None = None,
    hook: str = WAKE,
    query: str = "",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    agent = ctx.agent_id if agent_id is None else agent_id
    path = secret_path(agent, hook) if operation == "read" else rotate_path(agent, hook)
    return await ctx.client.request(
        "GET" if operation == "read" else "POST",
        path + query,
        content=body,
        headers=ctx.headers if headers is None else headers,
    )


def assert_admin_refused(response: httpx.Response, status: int, body: dict[str, Any]) -> None:
    assert_refused(response, status, body)
    assert_no_store(response)


async def wake_key_rows(ctx: WakeApp) -> list[dict[str, Any]]:
    async with ctx.app.state.engine.connect() as conn:
        result = await conn.execute(text("SELECT * FROM curie.hook_wake_keys ORDER BY hook"))
        return [dict(row) for row in result.mappings()]


# --- case 1: reproducible read -------------------------------------------------------


def test_wake_secret_read_is_reproducible_and_creates_no_row(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            first = await admin(ctx, "read")
            assert first.status_code == 200, first.text
            assert first.json() == {
                "agent_id": ctx.agent_id,
                "hook": WAKE,
                "generation": 1,
                "secret": ctx.key(),
            }
            assert_no_store(first)
            second = await admin(ctx, "read")
            assert second.status_code == 200 and second.json() == first.json()
            assert_no_store(second)
            assert await wake_key_rows(ctx) == []
            other = await admin(ctx, "read", hook=OTHER_WAKE)
            assert other.status_code == 200, other.text
            assert other.json()["secret"] == ctx.key(OTHER_WAKE) != first.json()["secret"]
            # The retrieved key is the one the wake route accepts.
            assert_queued(await wake(ctx, key=first.json()["secret"]))
            assert await wake_key_rows(ctx) == []

    drive(scenario)


# --- case 2: rotation ------------------------------------------------------------------


def test_wake_secret_rotation_invalidates_the_old_key_at_once(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            admitted = assert_queued(await wake(ctx))
            rotated = await admin(ctx, "rotate")
            assert rotated.status_code == 200, rotated.text
            assert rotated.json() == {"agent_id": ctx.agent_id, "hook": WAKE, "generation": 2}
            assert "secret" not in rotated.json()
            assert_no_store(rotated)
            assert await stored_generation(ctx.app, ctx.agent_id) == 2
            # Rotation does not cancel the admitted run.
            [run] = await hook_runs(ctx.app, ctx.agent_id)
            assert str(run["id"]) == admitted and run["outcome"] is None
            assert_refused(await wake(ctx, key=ctx.key(generation=1)), 401, INVALID_SIGNATURE)

            rotated = await admin(ctx, "rotate")
            assert rotated.status_code == 200, rotated.text
            assert rotated.json() == {"agent_id": ctx.agent_id, "hook": WAKE, "generation": 3}
            assert await stored_generation(ctx.app, ctx.agent_id) == 3
            current = await admin(ctx, "read")
            assert current.status_code == 200, current.text
            assert current.json() == {
                "agent_id": ctx.agent_id,
                "hook": WAKE,
                "generation": 3,
                "secret": ctx.key(generation=3),
            }
            for stale in (1, 2):
                assert_refused(
                    await wake(ctx, key=ctx.key(generation=stale)), 401, INVALID_SIGNATURE
                )
            await settle_run(ctx.app, admitted)  # the worker's stand-in, outside the API
            assert_queued(await wake(ctx, key=current.json()["secret"]))
            # Rotating one hook leaves another hook's key and metadata alone.
            assert await stored_generation(ctx.app, ctx.agent_id, OTHER_WAKE) is None
            assert_queued(await wake(ctx, OTHER_WAKE))

    drive(scenario)


# --- case 3: administrative authority -----------------------------------------------------


@pytest.mark.parametrize("operation", OPERATIONS)
def test_wake_secret_admin_requires_the_platform_key(wake_db: str, operation: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            wake_signed = signed_headers(ctx.key(), ctx.agent_id, WAKE, b"")
            for headers in ({}, {"X-API-Key": "not-the-platform-key"}, wake_signed):
                refused = await admin(ctx, operation, headers=headers)
                assert_refused(refused, 401, ADMIN_UNAUTHORIZED)
                assert ctx.key() not in refused.text
            assert await wake_key_rows(ctx) == []
            accepted = await admin(ctx, operation)
            assert accepted.status_code == 200, accepted.text
            assert_no_store(accepted)

    drive(scenario)


# --- case 4: no query, no body -----------------------------------------------------------


@pytest.mark.parametrize("operation", OPERATIONS)
def test_wake_secret_admin_refuses_query_and_body(wake_db: str, operation: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            for query in ("?x=", "?x=1", "?x", "?generation=1"):
                refused = await admin(ctx, operation, query=query)
                assert_admin_refused(refused, 400, QUERY_FORBIDDEN)
            assert_admin_refused(
                await admin(ctx, operation, query="?x=", body=b"{}"), 400, QUERY_FORBIDDEN
            )
            for body in (b"{}", b"x", b" "):
                assert_admin_refused(await admin(ctx, operation, body=body), 422, BODY_INVALID)
            # Authentication still comes first.
            assert_refused(
                await admin(ctx, operation, query="?x=", body=b"{}", headers={}),
                401,
                ADMIN_UNAUTHORIZED,
            )
            assert await wake_key_rows(ctx) == []
            accepted = await admin(ctx, operation)
            assert accepted.status_code == 200, accepted.text

    drive(scenario)


# --- case 5: declaration and source authority ----------------------------------------------


@pytest.mark.parametrize("operation", OPERATIONS)
def test_wake_secret_admin_answers_not_declared(wake_db: str, operation: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            bare, _name, _version = await ctx.add_agent(deploy=False)
            for agent, hook in (
                (str(uuid.uuid4()), WAKE),
                (bare, WAKE),
                (ctx.agent_id, CRON),
                (ctx.agent_id, UNDECLARED),
                (ctx.agent_id, PLAIN),
                (ctx.agent_id.upper(), WAKE),
                (ctx.agent_id.replace("-", ""), WAKE),
                (ctx.agent_id, "Queue-Ready"),
                (ctx.agent_id, "q" * 64),
            ):
                refused = await admin(ctx, operation, agent_id=agent, hook=hook)
                assert refused.status_code == 404, (agent, hook, refused.text)
                assert refused.json() == NOT_DECLARED, (agent, hook, refused.text)
                assert_no_store(refused)
            assert await wake_key_rows(ctx) == []
            assert (await admin(ctx, operation)).status_code == 200

    drive(scenario)


@pytest.mark.parametrize("operation", OPERATIONS)
@pytest.mark.parametrize("damage", ["deleted", "garbage"])
def test_wake_secret_admin_unreadable_bundle_answers_declaration_unavailable(
    wake_db: str, operation: str, damage: str
) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            original = await get_bundle_object(ctx.app, ctx.version_id)
            await put_bundle_object(
                ctx.app, ctx.version_id, None if damage == "deleted" else b"not a bundle"
            )
            assert_admin_refused(await admin(ctx, operation), 503, DECLARATION_UNAVAILABLE)
            assert await wake_key_rows(ctx) == []
            await put_bundle_object(ctx.app, ctx.version_id, original)
            assert (await admin(ctx, operation)).status_code == 200

    drive(scenario)


@pytest.mark.parametrize("kind", SOURCE_KINDS)
def test_wake_secret_admin_refuses_a_configured_source(wake_db: str, kind: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            assert (await admin(ctx, "rotate")).status_code == 200
            await seed_configuration(ctx.app, ctx.agent_id, WAKE, kind)
            for operation in OPERATIONS:
                refused = await admin(ctx, operation)
                assert_admin_refused(refused, 409, SOURCE_NOT_ORDINARY)
                assert ctx.key(generation=2) not in refused.text
            assert await stored_generation(ctx.app, ctx.agent_id) == 2
            # The same configuration on another hook leaves that hook administrable.
            assert (await admin(ctx, "read", hook=OTHER_WAKE)).status_code == 200
            rotated = await admin(ctx, "rotate", hook=OTHER_WAKE)
            assert rotated.status_code == 200 and rotated.json()["generation"] == 2

    drive(scenario)


# --- case 6: generation exhaustion ----------------------------------------------------------


def test_wake_secret_rotation_at_the_stored_maximum_is_exhausted(wake_db: str) -> None:
    async def scenario() -> None:
        async with wake_app() as ctx:
            maximum = await generation_column_max(ctx.app)
            await seed_generation(ctx.app, ctx.agent_id, WAKE, maximum)
            current = await admin(ctx, "read")
            assert current.status_code == 200, current.text
            assert current.json()["generation"] == maximum
            assert current.json()["secret"] == ctx.key(generation=maximum)
            before = await wake_key_rows(ctx)
            assert_admin_refused(await admin(ctx, "rotate"), 503, GENERATION_EXHAUSTED)
            assert await wake_key_rows(ctx) == before
            assert_queued(await wake(ctx, key=current.json()["secret"]))

    drive(scenario)
