"""Migration 0148 adds factory Slack run threads and their owed posts (#5414).

The downgrade to 0146 refuses, before any DDL, while progress state or a
destination exists: dropping it would lose owed posts and the roots a
re-upgrade needs to resume each thread. With none, it removes exactly the two
tables and the two agent columns, and 0148 applies again.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config
from alembic import command
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

REVISION = "0148"
BELOW = "0146"
TABLES = {"factory_slack_threads", "factory_slack_posts"}
AGENT_COLUMNS = {"factory_slack_channel", "factory_slack_since"}
CHANNEL = "C0EXAMPLE1"
REFUSAL = "factory_slack_progress_state_present"


Statement = tuple[str, dict[str, Any]]


def _batch(*statements: Statement) -> list[list[dict[str, Any]]]:
    """Statements on one connection in one transaction, each one's rows as dicts.

    One connection per batch keeps the test inside its time budget: opening a
    connection per statement costs most of a second on a shared host.
    """

    async def run() -> list[list[dict[str, Any]]]:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                results = []
                for statement, params in statements:
                    result = await connection.execute(text(statement), params)
                    results.append(
                        [dict(row) for row in result.mappings()] if result.returns_rows else []
                    )
                return results
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _agent(agent_id: uuid.UUID, channel: str | None = None) -> Statement:
    return (
        "INSERT INTO curie.agents (id, name, factory_slack_channel, factory_slack_since) "
        "VALUES (:id, :name, CAST(:channel AS text), "
        "CASE WHEN CAST(:channel AS text) IS NULL THEN NULL ELSE now() END)",
        {"id": agent_id, "name": f"m0148-{agent_id.hex[:8]}", "channel": channel},
    )


def _work_item(item_id: uuid.UUID, agent_id: uuid.UUID) -> Statement:
    return (
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', "
        "'slack:C0EXAMPLE1:1700000000.000100', 1, 1)",
        {"id": item_id, "issue": str(int(agent_id.int % 100000) + 1), "agent": agent_id},
    )


def _thread_rows() -> list[Statement]:
    """A WorkItem's thread, for an agent whose destination is already cleared."""

    agent_id, item_id = uuid.uuid4(), uuid.uuid4()
    return [
        _agent(agent_id),
        _work_item(item_id, agent_id),
        (
            "INSERT INTO curie.factory_slack_threads "
            "(work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence) "
            "VALUES (:item, :agent, 'acme-factory', :channel, '1800000000.000001', 1)",
            {"item": item_id, "agent": agent_id, "channel": CHANNEL},
        ),
    ]


def _post_rows() -> list[Statement]:
    # A post needs its thread (foreign key), so this case holds both.
    thread = _thread_rows()
    item_id = thread[-1][1]["item"]
    return [
        *thread,
        (
            "INSERT INTO curie.factory_slack_posts "
            "(work_item_id, post_key, kind, sequence, rank, ordinal, payload) "
            "VALUES (:item, 'root', 'root', 0, 0, 0, CAST('{}' AS jsonb))",
            {"item": item_id},
        ),
    ]


def _destination_rows() -> list[Statement]:
    return [_agent(uuid.uuid4(), CHANNEL)]


SEEDS = {
    "thread": _thread_rows,
    "post": _post_rows,
    "destination": _destination_rows,
}

_CATALOG: list[Statement] = [
    ("SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'", {}),
    (
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'agents'",
        {},
    ),
    (
        "SELECT conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
        "WHERE n.nspname = 'curie' AND c.conname = 'agents_factory_slack_channel_ck'",
        {},
    ),
    ("SELECT version_num FROM curie.alembic_version", {}),
]
_STATE: list[Statement] = [
    (
        "SELECT work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence "
        "FROM curie.factory_slack_threads ORDER BY work_item_id",
        {},
    ),
    (
        "SELECT id, work_item_id, post_key, kind, posted_at "
        "FROM curie.factory_slack_posts ORDER BY id",
        {},
    ),
    (
        "SELECT id, factory_slack_channel, factory_slack_since FROM curie.agents ORDER BY id",
        {},
    ),
]


@dataclass(frozen=True)
class Catalog:
    tables: set[str]
    agent_columns: set[str]
    check_present: bool
    revision: str

    @property
    def present(self) -> bool:
        return TABLES <= self.tables and AGENT_COLUMNS <= self.agent_columns and self.check_present

    @property
    def absent(self) -> bool:
        return (
            not TABLES & self.tables
            and not AGENT_COLUMNS & self.agent_columns
            and not self.check_present
        )


def _catalog(*extra: Statement) -> tuple[Catalog, list[list[dict[str, Any]]]]:
    tables, columns, check, (stamp,), *rest = _batch(*_CATALOG, *extra)
    return (
        Catalog(
            {row["table_name"] for row in tables},
            {row["column_name"] for row in columns},
            bool(check),
            stamp["version_num"],
        ),
        rest,
    )


@pytest.mark.parametrize("seed", sorted(SEEDS))
def test_0148_downgrade_refuses_while_progress_state_or_a_destination_exists(
    isolated_migration_db: IsolatedMigrationDb, seed: str
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    _batch(*SEEDS[seed]())
    catalog, before = _catalog(*_STATE)
    assert catalog.present and catalog.revision == REVISION, catalog
    threads, posts, agents = before
    assert threads or posts or any(row["factory_slack_channel"] for row in agents), before

    with pytest.raises(RuntimeError, match=REFUSAL):
        command.downgrade(config, BELOW)

    # Refused before any DDL: the revision, schema and rows are as they were.
    # The private database is dropped after the test, so nothing is restored.
    catalog, after = _catalog(*_STATE)
    assert catalog.present and catalog.revision == REVISION, catalog
    assert after == before


def test_0148_downgrade_without_state_removes_the_tables_and_columns_and_reapplies(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    # An agent without a destination is not progress state.
    agent_id = uuid.uuid4()
    _batch(_agent(agent_id))
    catalog, _ = _catalog()
    assert catalog.present and catalog.revision == REVISION, catalog

    command.downgrade(config, BELOW)

    catalog, (agents,) = _catalog(("SELECT id FROM curie.agents", {}))
    assert catalog.absent and catalog.revision == BELOW, catalog
    assert agents == [{"id": agent_id}]

    command.upgrade(config, REVISION)

    catalog, (agents,) = _catalog(
        ("SELECT id, factory_slack_channel, factory_slack_since FROM curie.agents", {})
    )
    assert catalog.present and catalog.revision == REVISION, catalog
    assert agents == [{"id": agent_id, "factory_slack_channel": None, "factory_slack_since": None}]
