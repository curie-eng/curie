"""Closed manual-fire source resolution, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import io
import json
import tarfile
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx
import pytest
from _migration_support import IsolatedMigrationDb
from aci_protocol import STREAM_PAYLOAD_FIELD
from curie_api.config import get_settings
from curie_api.killswitch import kill_key
from curie_api.main import create_app
from curie_protected_hooks.source_policy_records import target_intent_sha256
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

HOOK = "nightly-check"
ORDINARY = dict(
    mode="ordinary", tool_access=None, runtime_id=None, qualification_id=None, bundle_digest=None
)
PROTECTED = dict(
    mode="protected",
    tool_access="read-only",
    runtime_id="33333333-3333-4333-8333-333333333333",
    qualification_id="44444444-4444-4444-8444-444444444444",
    bundle_digest="a" * 64,
)


@pytest.fixture
def fire_db(isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch) -> None:
    """Actual isolated backing, @spec PROTECTED-HOOK-SOURCE-2/10."""
    isolated_migration_db.at("head")
    stream = "test:source-fire:" + uuid.uuid4().hex
    for name, value in {
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "OTEL_SDK_DISABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
        "RUNS_STREAM": stream,
        "CURIE_STREAM": stream,
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def bundle_bytes(name: str, *, missing_binding: bool = False) -> bytes:
    """Real validated declaration archive, @spec PROTECTED-HOOK-SOURCE-2."""
    trigger = dict(type="cron", name=name, schedule="0 9 * * *", prompt="Run the scheduled check.")
    if missing_binding:
        trigger["target"] = "unbound@example.test"
    files = {
        "source-fire/.claude-plugin/plugin.json": json.dumps(
            dict(name="source-fire", version="0.1.0", description="test", triggers=[trigger])
        ).encode(),
        "source-fire/skills/source-fire/SKILL.md": (
            b"---\nname: source-fire\ndescription: test\n---\nRead the scheduled status.\n"
        ),
    }
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for path, value in files.items():
            info = tarfile.TarInfo(path)
            info.size = len(value)
            archive.addfile(info, io.BytesIO(value))
    return output.getvalue()


@asynccontextmanager
async def fire_app(name: str = HOOK, *, outcome: str = "running") -> AsyncIterator[Any]:
    """Same-loop application with owned artifacts, @spec PROTECTED-HOOK-SOURCE-2/10."""
    app = create_app()
    headers = {"X-API-Key": get_settings().api_key}
    agent_id = version_id = None
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            try:
                response = await client.post(
                    "/agents",
                    headers=headers,
                    json=dict(
                        name="fire-" + uuid.uuid4().hex,
                        channel=dict(
                            kind="email",
                            address="fire@example.test",
                            endpoint="http://adapter.example.test",
                            adapter="mail",
                        ),
                    ),
                )
                assert response.status_code == 201, response.text
                agent_id = response.json()["id"]
                response = await client.post(
                    f"/agents/{agent_id}/versions",
                    headers=headers,
                    json=dict(version_label="v1", created_by="test"),
                )
                assert response.status_code == 201, response.text
                version_id = response.json()["id"]
                response = await client.put(
                    f"/agents/{agent_id}/versions/{version_id}/bundle",
                    headers=headers,
                    files={
                        "file": (
                            "source-fire.tar.gz",
                            bundle_bytes(name, missing_binding=outcome == "failed"),
                        )
                    },
                )
                assert response.status_code == 201, response.text
                response = await client.post(
                    "/deployments",
                    headers=headers,
                    json=dict(agent_id=agent_id, version_id=version_id, environment="dev"),
                )
                assert response.status_code == 201, response.text
                if outcome == "blocked":
                    await app.state.valkey.set(kill_key(uuid.UUID(agent_id)), "1")
                elif outcome == "skipped":
                    async with app.state.engine.begin() as conn:
                        await conn.execute(
                            text(
                                "INSERT INTO curie.hook_runs "
                                "(id,agent_id,name,slot_utc,version_id,started_at) "
                                "VALUES (:id,:agent,:name,:slot,:version,:slot)"
                            ),
                            dict(
                                id=uuid.uuid4(),
                                agent=uuid.UUID(agent_id),
                                name=name,
                                slot=datetime.now(UTC),
                                version=uuid.UUID(version_id),
                            ),
                        )
                yield app, client, headers, agent_id, version_id
            finally:
                if agent_id is not None:
                    await app.state.valkey.delete(kill_key(uuid.UUID(agent_id)))
                await app.state.valkey.delete(get_settings().runs_stream)
                if version_id is not None:
                    async with app.state.engine.connect() as conn:
                        key = await conn.scalar(
                            text("SELECT bundle_ref FROM curie.agent_versions WHERE id=:id"),
                            dict(id=uuid.UUID(version_id)),
                        )
                    if key:
                        store = app.state.bundle_store
                        await asyncio.to_thread(
                            store._client.delete_object, Bucket=store._bucket, Key=key
                        )


async def state(app: Any, agent: str) -> tuple[list[dict[str, Any]], list[Any]]:
    """Independent durable effects snapshot, @spec PROTECTED-HOOK-SOURCE-2/10."""
    async with app.state.engine.connect() as conn:
        runs = [
            dict(row)
            for row in (
                await conn.execute(
                    text("SELECT * FROM curie.hook_runs WHERE agent_id=:agent ORDER BY id"),
                    dict(agent=uuid.UUID(agent)),
                )
            ).mappings()
        ]
    return runs, await app.state.valkey.xrange(get_settings().runs_stream)


async def seed_source(app: Any, agent: str, name: str, kind: str) -> None:
    """Closed authority fixtures only, @spec PROTECTED-HOOK-SOURCE-2/10."""
    target = PROTECTED if kind == "protected" else ORDINARY
    params = dict(
        agent=uuid.UUID(agent),
        hook=name,
        op=uuid.uuid4(),
        intent=target_intent_sha256(target),
        status="pending" if kind == "pending" else "committed",
        **target,
    )
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO curie.hook_source_operations "
                "(agent_id,hook,operation_id,intent_sha256,status,generation) "
                "VALUES(:agent,:hook,:op,:intent,:status,1)"
            ),
            params,
        )
        if kind in ("protected", "tombstone"):
            await conn.execute(
                text(
                    "INSERT INTO curie.hook_source_policies "
                    "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
                    "qualification_id,bundle_digest,legacy_generation) "
                    "VALUES(:agent,:hook,:op,1,:mode,:tool_access,:runtime_id,"
                    ":qualification_id,:bundle_digest,0)"
                ),
                params,
            )


async def fire(client: Any, headers: dict[str, str], agent: str, name: str) -> Any:
    """Existing authenticated manual-fire wire, @spec PROTECTED-HOOK-SOURCE-2."""
    return await client.post(
        f"/agents/{quote(agent, safe='')}/hooks/{quote(name, safe='')}/fire", headers=headers
    )


@pytest.mark.parametrize("outcome", ["running", "blocked", "failed", "skipped"])
def test_unconfigured_ordinary_preserves_run_and_queue_outcomes(
    fire_db: None, outcome: str
) -> None:
    """Existing successful siblings, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with fire_app(outcome=outcome) as (app, client, headers, agent, _version):
            before, _ = await state(app, agent)
            response = await fire(client, headers, agent, HOOK)
            assert response.status_code == 200, response.text
            assert response.json()["outcome"] == (None if outcome == "running" else outcome)
            runs, queue = await state(app, agent)
            assert len(runs) == len(before) + 1
            assert len(queue) == (1 if outcome == "running" else 0)
            if queue:
                payload = json.loads(queue[0][1][STREAM_PAYLOAD_FIELD.encode()])
                assert payload["source"] == "cron"
                assert payload["hook_run"]["agent_id"] == agent
                assert payload["hook_run"]["name"] == HOOK
                assert str(runs[-1]["id"]) == response.json()["id"]

    asyncio.run(asyncio.wait_for(scenario(), 25))


def test_gate_loss_at_claim_commit_refuses_unguarded_failure_cleanup(fire_db: None) -> None:
    """Actual post-claim gate loss, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with fire_app() as (app, client, headers, agent, _version):
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            suffix = uuid.uuid4().hex
            audit = "fire_loss_audit_" + suffix
            function = "fire_loss_commit_" + suffix
            trigger = "fire_loss_trigger_" + suffix
            try:
                assert await state(app, agent) == ([], [])
                async with observer.begin() as conn:
                    await conn.execute(
                        text(
                            f"CREATE TABLE curie.{audit} "
                            "(claim_id uuid, outcome text, ended_at timestamptz, "
                            "gate_pid integer, terminated boolean)"
                        )
                    )
                    await conn.execute(
                        text(
                            f"CREATE FUNCTION curie.{function}() RETURNS trigger "
                            "LANGUAGE plpgsql AS $$ DECLARE gate_pid integer; BEGIN "
                            "SELECT l.pid INTO STRICT gate_pid FROM pg_locks l "
                            "JOIN pg_database d ON d.oid=l.database "
                            "WHERE d.datname=current_database() "
                            "AND l.locktype='advisory' AND l.granted "
                            "AND l.classid::bigint="
                            "((hashtextextended('hook-source:' || NEW.agent_id::text,0)>>32)"
                            "&4294967295) AND l.objid::bigint="
                            "(hashtextextended('hook-source:' || NEW.agent_id::text,0)&4294967295) "
                            "AND l.objsubid=1 AND l.pid<>pg_backend_pid(); "
                            f"INSERT INTO curie.{audit} "
                            "VALUES (NEW.id, NEW.outcome, NEW.ended_at, gate_pid, "
                            "pg_terminate_backend(gate_pid,1000)); RETURN NEW; END; $$"
                        )
                    )
                    await conn.execute(
                        text(
                            f"CREATE CONSTRAINT TRIGGER {trigger} AFTER INSERT "
                            "ON curie.hook_runs DEFERRABLE INITIALLY DEFERRED "
                            f"FOR EACH ROW WHEN (NEW.agent_id='{agent}'::uuid) "
                            f"EXECUTE FUNCTION curie.{function}()"
                        )
                    )
                response = await asyncio.wait_for(fire(client, headers, agent, HOOK), 5)
                assert response.status_code == 503, response.text
                runs, queue = await state(app, agent)
                assert queue == [], "gate loss before XADD must leave the real stream unchanged"
                async with observer.connect() as conn:
                    committed = (
                        (await conn.execute(text(f"SELECT * FROM curie.{audit}"))).mappings().one()
                    )
                    assert committed["terminated"] is True
                    assert committed["gate_pid"] != await conn.scalar(
                        text("SELECT pg_backend_pid()")
                    )
                    assert not await conn.scalar(
                        text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"),
                        {"pid": committed["gate_pid"]},
                    )
                assert len(runs) == 1
                assert runs[0]["id"] == committed["claim_id"]
                assert committed["outcome"] is None and committed["ended_at"] is None
                assert runs[0]["outcome"] is None, (
                    "PROTECTED-HOOK-SOURCE-2: detected post-commit gate loss must not "
                    "authorize failure-cleanup UPDATE"
                )
                assert runs[0]["ended_at"] is None
            finally:
                async with observer.begin() as conn:
                    await conn.execute(text(f"DROP TRIGGER IF EXISTS {trigger} ON curie.hook_runs"))
                    await conn.execute(text(f"DROP FUNCTION IF EXISTS curie.{function}()"))
                    await conn.execute(text(f"DROP TABLE IF EXISTS curie.{audit}"))
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 25))


@pytest.mark.parametrize("kind", ["protected", "tombstone", "pending", "committed_history"])
@pytest.mark.parametrize("outcome", ["running", "blocked", "failed", "skipped"])
def test_closed_sources_refuse_before_every_run_outcome(
    fire_db: None, kind: str, outcome: str
) -> None:
    """No terminal-row bypass, @spec PROTECTED-HOOK-SOURCE-2/10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with fire_app(outcome=outcome) as (app, client, headers, agent, _version):
            await seed_source(app, agent, HOOK, kind)
            before = await state(app, agent)
            response = await fire(client, headers, agent, HOOK)
            assert response.status_code == 503, response.text
            assert await state(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 25))


@pytest.mark.parametrize("name", ["Nightly Report", "Nightly-" + "x" * 70])
def test_existing_validated_legacy_names_remain_ordinary(fire_db: None, name: str) -> None:
    """Do not narrow frozen name grammar, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with fire_app(name) as (app, client, headers, agent, _version):
            response = await fire(client, headers, agent, name)
            assert response.status_code == 200, response.text
            runs, queue = await state(app, agent)
            assert runs[0]["name"] == name
            assert (
                json.loads(queue[0][1][STREAM_PAYLOAD_FIELD.encode()])["hook_run"]["name"] == name
            )

    asyncio.run(asyncio.wait_for(scenario(), 25))


def test_exact_legacy_name_history_refuses_without_aliasing(fire_db: None) -> None:
    """History absence must be real, @spec PROTECTED-HOOK-SOURCE-2/10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        name = "Nightly Report"
        async with fire_app(name) as (app, client, headers, agent, _version):
            await seed_source(app, agent, name, "pending")
            before = await state(app, agent)
            response = await fire(client, headers, agent, name)
            assert response.status_code == 503, response.text
            assert await state(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 25))


def test_actual_queue_failure_keeps_durable_failed_run(fire_db: None) -> None:
    """Existing durable failure behavior, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with fire_app() as (app, client, headers, agent, _version):
            await app.state.valkey.set(get_settings().runs_stream, "wrong-stream-type")
            response = await fire(client, headers, agent, HOOK)
            assert response.status_code == 503, response.text
            async with app.state.engine.connect() as conn:
                rows = (
                    (
                        await conn.execute(
                            text(
                                "SELECT outcome,ended_at FROM curie.hook_runs WHERE agent_id=:agent"
                            ),
                            dict(agent=uuid.UUID(agent)),
                        )
                    )
                    .mappings()
                    .all()
                )
            assert (
                len(rows) == 1
                and rows[0]["outcome"] == "failed"
                and rows[0]["ended_at"] is not None
            )
            assert await app.state.valkey.get(get_settings().runs_stream) == b"wrong-stream-type"

    asyncio.run(asyncio.wait_for(scenario(), 25))


async def wait_for_agent_gate(observer: Any) -> None:
    """Actual advisory waiter, @spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(4):
        while True:
            async with observer.connect() as conn:
                waiting = await conn.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname=current_database() AND wait_event='advisory')"
                    )
                )
            if waiting:
                return
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("change", ["pending_source", "budget"])
def test_gate_wait_releases_preliminary_work_and_reloads_changes(
    fire_db: None, change: str
) -> None:
    """Fresh source/configuration, @spec PROTECTED-HOOK-SOURCE-2/10."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2/10."""
        async with fire_app() as (app, client, headers, agent, _version):
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(fire(client, headers, agent, HOOK))
                    await wait_for_agent_gate(observer)
                    assert not task.done()
                    assert app.state.engine.pool.checkedout() == 0
                    async with asyncio.timeout(1), app.state.engine.connect() as conn:
                        assert await conn.scalar(text("SELECT 1")) == 1
                    if change == "pending_source":
                        await seed_source(app, agent, HOOK, "pending")
                    else:
                        async with observer.begin() as conn:
                            await conn.execute(
                                text("UPDATE curie.agents SET max_usd_per_day=0 WHERE id=:agent"),
                                dict(agent=uuid.UUID(agent)),
                            )
                response = await asyncio.wait_for(task, 5)
                if change == "pending_source":
                    assert response.status_code == 503, response.text
                    assert await state(app, agent) == ([], [])
                else:
                    assert response.status_code == 200, response.text
                    assert response.json()["outcome"] == "blocked"
                    assert (await state(app, agent))[1] == []
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 25))


def test_canceled_gate_waiter_releases_for_ordinary_successor(fire_db: None) -> None:
    """Actual cancellation ownership, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with fire_app() as (app, client, headers, agent, _version):
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(fire(client, headers, agent, HOOK))
                    await wait_for_agent_gate(observer)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    assert app.state.engine.pool.checkedout() == 0
                    assert await state(app, agent) == ([], [])
                response = await fire(client, headers, agent, HOOK)
                assert response.status_code == 200, response.text
                assert len((await state(app, agent))[1]) == 1
                assert app.state.source_gate.engine.pool.checkedout() == 0
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 25))


def test_gate_loss_during_existing_hook_lock_wait_refuses_before_insert(fire_db: None) -> None:
    """Exact first-effect boundary, @spec PROTECTED-HOOK-SOURCE-2."""

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-2."""
        async with fire_app() as (app, client, headers, agent, _version):
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            before = await state(app, agent)
            try:
                async with observer.begin() as blocker:
                    await blocker.execute(
                        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                        {"key": agent + ":" + HOOK},
                    )
                    blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
                    task = asyncio.create_task(fire(client, headers, agent, HOOK))
                    async with asyncio.timeout(5):
                        while True:
                            async with observer.connect() as conn:
                                waiting = await conn.scalar(
                                    text(
                                        "SELECT EXISTS (SELECT 1 FROM pg_locks l "
                                        "JOIN pg_database d ON d.oid=l.database "
                                        "WHERE d.datname=current_database() "
                                        "AND l.locktype='advisory' AND NOT l.granted "
                                        "AND l.classid::bigint="
                                        "((hashtextextended(:key,0)>>32)&4294967295) "
                                        "AND l.objid::bigint="
                                        "(hashtextextended(:key,0)&4294967295) "
                                        "AND l.objsubid=1 AND l.pid<>:holder)"
                                    ),
                                    {"key": agent + ":" + HOOK, "holder": blocker_pid},
                                )
                                if waiting:
                                    gate_pid = await conn.scalar(
                                        text(
                                            "SELECT l.pid FROM pg_locks l "
                                            "JOIN pg_database d ON d.oid=l.database "
                                            "WHERE d.datname=current_database() "
                                            "AND l.locktype='advisory' AND l.granted "
                                            "AND l.classid::bigint="
                                            "((hashtextextended(:key,0)>>32)&4294967295) "
                                            "AND l.objid::bigint="
                                            "(hashtextextended(:key,0)&4294967295) "
                                            "AND l.objsubid=1 AND l.pid<>:holder"
                                        ),
                                        {"key": "hook-source:" + agent, "holder": blocker_pid},
                                    )
                                    assert gate_pid is not None
                                    assert gate_pid != blocker_pid
                                    assert not task.done()
                                    assert await conn.scalar(
                                        text("SELECT pg_terminate_backend(:pid)"),
                                        {"pid": gate_pid},
                                    )
                                    break
                            await asyncio.sleep(0.01)
                response = await asyncio.wait_for(task, 5)
                assert response.status_code == 503, response.text
                assert await state(app, agent) == before
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 25))
