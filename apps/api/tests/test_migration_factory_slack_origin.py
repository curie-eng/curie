"""Migration 0167 adds ``factory_slack_threads.origin`` (#5380).

An expand revision above 0163. Every thread that exists before it reads
``configured``; the server default stays so an application at the prior
revision keeps inserting valid rows; the CHECK admits only ``configured`` and
``kickoff``. The downgrade to 0163 takes an exclusive table lock and refuses,
before any DDL, while a ``kickoff`` row exists. Rows are seeded with raw SQL at
the revision they belong to, never through the current models.

Behavior tests (fail if the 0167 source is reverted): the upgrade backfill, the
CHECK refusal, the downgrade refusal, and the concurrent-insert case. The
liveness tests (a kickoff row is admitted, a downgrade with only configured
rows still succeeds and re-applies) guard the refusals against over-refusing.
"""

from __future__ import annotations

import asyncio
import threading
import uuid
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

REVISION = "0167"
BELOW = "0163"
TABLE = "factory_slack_threads"
CHECK = "factory_slack_threads_origin_ck"
REFUSAL = "factory_slack_kickoff_threads_present"
CHANNEL = "C0EXAMPLE1"
ROOT_BASE = 1_800_000_000


def _insert(table: str, values: dict[str, Any]) -> None:
    columns = ", ".join(values)
    params = ", ".join(f":{name}" for name in values)
    sql_rows(f"INSERT INTO curie.{table} ({columns}) VALUES ({params})", values)


def _work_item() -> tuple[uuid.UUID, uuid.UUID]:
    """A WorkItem of a fresh agent, so thread probes never share one."""

    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    _insert("agents", {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"})
    _insert(
        "work_items",
        {
            "id": item_id,
            "tracker_kind": "github",
            "tracker_host": "github.com",
            "tracker_scope_id": "101",
            "tracker_issue_id": str(item_id.int % 10**9),
            "code_host_kind": "github",
            "code_host_host": "github.com",
            "repository_project_id": "101",
            "code_host_installation_id": 202,
            "agent_id": agent_id,
            "repository_path": "acme-corp/acme-bot",
            "conversation_id": f"acme-origin-{item_id}",
            "version": 1,
            "next_sequence": 2,
        },
    )
    return item_id, agent_id


def _thread_values(item_id: uuid.UUID, agent_id: uuid.UUID, **extra: Any) -> dict[str, Any]:
    root = f"{ROOT_BASE + item_id.int % 10**6}.000001"
    return {
        "work_item_id": item_id,
        "agent_id": agent_id,
        "channel_identity": "acme-factory",
        "channel": CHANNEL,
        "root_ts": root,
        "first_sequence": 1,
        **extra,
    }


def _thread(**extra: Any) -> uuid.UUID:
    item_id, agent_id = _work_item()
    _insert(TABLE, _thread_values(item_id, agent_id, **extra))
    return item_id


def _origins() -> dict[uuid.UUID, str]:
    return {
        row["work_item_id"]: row["origin"]
        for row in sql_dicts(f"SELECT work_item_id, origin FROM curie.{TABLE}")
    }


def _check_definition() -> str | None:
    rows = sql_rows(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = 'curie.factory_slack_threads'::regclass AND conname = :name",
        {"name": CHECK},
    )
    return str(rows[0][0]) if rows else None


def test_0167_upgrade_backfills_existing_threads_as_configured(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    assert "origin" not in column_names(TABLE)
    before = _thread()

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert "origin" in column_names(TABLE)
    assert _origins() == {before: "configured"}
    definition = _check_definition()
    assert definition is not None
    assert "configured" in definition and "kickoff" in definition


def test_0167_default_keeps_prior_application_inserts_valid(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    # An application at 0163 never names origin.
    unnamed = _thread()
    assert _origins() == {unnamed: "configured"}
    columns = sql_dicts(
        "SELECT is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table AND column_name = 'origin'",
        {"table": TABLE},
    )
    assert columns == [{"is_nullable": "NO", "column_default": "'configured'::text"}]


def test_0167_check_admits_both_origins_and_refuses_any_other(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    # Liveness: both named origins are admitted.
    configured = _thread(origin="configured")
    kickoff = _thread(origin="kickoff")
    assert _origins() == {configured: "configured", kickoff: "kickoff"}
    for bad in ("Kickoff", "", "steer"):
        item_id, agent_id = _work_item()
        with pytest.raises(IntegrityError, match=CHECK):
            _insert(TABLE, _thread_values(item_id, agent_id, origin=bad))
    item_id, agent_id = _work_item()
    with pytest.raises(IntegrityError):
        _insert(TABLE, _thread_values(item_id, agent_id, origin=None))


def test_0167_downgrade_refuses_a_kickoff_thread_before_any_ddl(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    configured = _thread(origin="configured")
    kickoff = _thread(origin="kickoff")
    before = _origins()

    with pytest.raises(RuntimeError, match=REFUSAL):
        command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == REVISION
    assert "origin" in column_names(TABLE)
    assert constraint_exists(CHECK)
    assert _origins() == before == {configured: "configured", kickoff: "kickoff"}


def test_0167_downgrade_without_a_kickoff_thread_removes_the_column_and_reapplies(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    # Liveness: configured threads, or none, never hold the downgrade.
    configured = _thread(origin="configured")

    command.downgrade(alembic_config(), BELOW)

    assert stamped_revision() == BELOW
    assert "origin" not in column_names(TABLE)
    assert not constraint_exists(CHECK)
    assert sql_rows(f"SELECT work_item_id FROM curie.{TABLE}") == [(configured,)]

    command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == REVISION
    assert _origins() == {configured: "configured"}
    assert constraint_exists(CHECK)


def test_0167_downgrade_waits_for_a_concurrent_kickoff_insert_then_refuses(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """The table lock serializes the check with a writer that has not committed yet.

    Without the lock the downgrade reads no kickoff row (the insert is not yet
    visible), then its DROP COLUMN waits for the writer and removes the column
    under a committed kickoff row.
    """
    isolated_migration_db.at(REVISION)
    item_id, agent_id = _work_item()
    values = _thread_values(item_id, agent_id, origin="kickoff")
    inserted, release = threading.Event(), threading.Event()
    failures: list[BaseException] = []

    async def writer() -> None:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                columns = ", ".join(values)
                params = ", ".join(f":{name}" for name in values)
                await connection.execute(
                    text(f"INSERT INTO curie.{TABLE} ({columns}) VALUES ({params})"), values
                )
                inserted.set()
                # Nothing else runs on this thread's loop; the transaction stays open.
                release.wait(timeout=120)
                await connection.commit()
        finally:
            await engine.dispose()

    def run_writer() -> None:
        try:
            asyncio.run(writer())
        except BaseException as exc:  # noqa: BLE001 - surfaced on the test thread
            failures.append(exc)
            inserted.set()

    outcome: list[BaseException | None] = []

    def run_downgrade() -> None:
        try:
            command.downgrade(alembic_config(), BELOW)
            outcome.append(None)
        except BaseException as exc:  # noqa: BLE001 - asserted on the test thread
            outcome.append(exc)

    writer_thread = threading.Thread(target=run_writer)
    downgrade_thread = threading.Thread(target=run_downgrade)
    writer_thread.start()
    try:
        assert inserted.wait(timeout=60), "the writer never inserted its kickoff row"
        assert not failures, failures
        downgrade_thread.start()
        # The condition to wait on: the downgrade is blocked behind the open writer.
        blocked = threading.Event()
        for _ in range(600):
            waiting = sql_rows(
                "SELECT 1 FROM pg_stat_activity WHERE datname = current_database() "
                "AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()"
            )
            if waiting:
                blocked.set()
                break
            blocked.wait(timeout=0.05)
        assert blocked.is_set(), "the downgrade never waited on the writer's table lock"
        assert outcome == [], "the downgrade finished while the kickoff insert was uncommitted"
    finally:
        release.set()
        writer_thread.join(timeout=60)
        downgrade_thread.join(timeout=60)
    assert not writer_thread.is_alive() and not downgrade_thread.is_alive()
    assert not failures, failures

    assert len(outcome) == 1 and isinstance(outcome[0], RuntimeError), outcome
    assert REFUSAL in str(outcome[0])
    assert stamped_revision() == REVISION
    assert "origin" in column_names(TABLE)
    assert _origins() == {item_id: "kickoff"}
