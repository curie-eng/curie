"""Migration 0046 makes work item execution identity durable in PostgreSQL."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0045"
REVISION = "0046"
REPO = "acme-corp/acme-bot"
CONVERSATION = "slack:C0EXAMPLE1:1700000000.000100"
STAMP = datetime(2026, 9, 18, 12, tzinfo=UTC)


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                return [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _rejects(statement: str, params: dict[str, Any]) -> None:
    with pytest.raises(DBAPIError) as excinfo:
        _sql(statement, params)
    assert getattr(excinfo.value.orig, "sqlstate", None) in {
        "23502",
        "23503",
        "23505",
        "23514",
        "P0001",
    }


def _seed_agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"work-item-migration-{agent_id.hex[:8]}"},
    )
    return agent_id


def _seed_agent_and_deployment() -> tuple[uuid.UUID, uuid.UUID]:
    agent_id = _seed_agent()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'v1', NULL, 'migration-test')",
        {"id": version_id, "agent_id": agent_id},
    )
    _sql(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, "
        "CAST('dev' AS curie.environment), 'active')",
        {
            "id": deployment_id,
            "agent_id": agent_id,
            "version_id": version_id,
        },
    )
    return agent_id, deployment_id


def _seed_lineage(
    agent_id: uuid.UUID,
    deployment_id: uuid.UUID,
    *,
    conversation_id: str = CONVERSATION,
    repo_full_name: str = REPO,
) -> uuid.UUID:
    lineage_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
        "base_sha, branch, status, version, latest_revision) VALUES "
        "(:id, :agent_id, :deployment_id, :conversation_id, :repo, "
        ":base_sha, :branch, 'open', 1, 1)",
        {
            "id": lineage_id,
            "agent_id": agent_id,
            "deployment_id": deployment_id,
            "conversation_id": conversation_id,
            "repo": repo_full_name,
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
            "branch": f"curie/publication-{lineage_id.hex}",
        },
    )
    return lineage_id


def _insert_work_item(
    agent_id: uuid.UUID,
    **overrides: Any,
) -> uuid.UUID:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "github_repository_id": 1001,
        "github_issue_number": 37,
        "github_installation_id": 2001,
        "agent_id": agent_id,
        "repo_full_name": REPO,
        "conversation_id": CONVERSATION,
        "publication_lineage_id": None,
        "cancelled_at": None,
        "version": 1,
        "next_sequence": 1,
        "created_at": STAMP,
        "updated_at": STAMP,
    }
    values.update(overrides)
    _sql(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, publication_lineage_id, "
        "cancelled_at, version, next_sequence, created_at, updated_at) VALUES "
        "(:id, :github_repository_id, :github_issue_number, "
        ":github_installation_id, :agent_id, :repo_full_name, "
        ":conversation_id, :publication_lineage_id, :cancelled_at, :version, "
        ":next_sequence, :created_at, :updated_at)",
        values,
    )
    return values["id"]


def _request_values(work_item_id: uuid.UUID, **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "work_item_id": work_item_id,
        "sequence": 1,
        "status": "waiting",
        "wait_deadline": STAMP + timedelta(hours=1),
        "started_at": None,
        "execution_deadline": None,
        "terminal_at": None,
        "terminal_cause": None,
        "termination_observation": None,
        "version": 1,
        "created_at": STAMP,
        "updated_at": STAMP,
    }
    values.update(overrides)
    return values


def _insert_request(work_item_id: uuid.UUID, **overrides: Any) -> uuid.UUID:
    values = _request_values(work_item_id, **overrides)
    _sql(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, terminal_at, terminal_cause, "
        "termination_observation, version, created_at, updated_at) VALUES "
        "(:id, :work_item_id, :sequence, :status, :wait_deadline, :started_at, "
        ":execution_deadline, :terminal_at, :terminal_cause, "
        ":termination_observation, :version, :created_at, :updated_at)",
        values,
    )
    return values["id"]


def _reject_request(work_item_id: uuid.UUID, **overrides: Any) -> None:
    values = _request_values(work_item_id, **overrides)
    _rejects(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, terminal_at, terminal_cause, "
        "termination_observation, version, created_at, updated_at) VALUES "
        "(:id, :work_item_id, :sequence, :status, :wait_deadline, :started_at, "
        ":execution_deadline, :terminal_at, :terminal_cause, "
        ":termination_observation, :version, :created_at, :updated_at)",
        values,
    )


def _constraint_definitions(table_name: str) -> list[str]:
    return [
        row["definition"].lower()
        for row in _sql(
            "SELECT pg_get_constraintdef(c.oid) AS definition "
            "FROM pg_constraint c "
            "JOIN pg_class r ON r.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = r.relnamespace "
            "WHERE n.nspname = 'curie' AND r.relname = :table_name "
            "ORDER BY c.contype, c.conname",
            {"table_name": table_name},
        )
    ]


def test_0046_catalog_contract_and_round_trip(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    assert _sql(
        "SELECT to_regclass('curie.work_items') AS work_items, "
        "to_regclass('curie.execution_requests') AS execution_requests"
    ) == [{"work_items": None, "execution_requests": None}]

    command.upgrade(config, REVISION)

    columns = _sql(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' "
        "AND table_name IN ('work_items', 'execution_requests') "
        "ORDER BY table_name, ordinal_position"
    )
    by_table = {
        table: {row["column_name"] for row in columns if row["table_name"] == table}
        for table in ("work_items", "execution_requests")
    }
    assert by_table == {
        "work_items": {
            "id",
            "github_repository_id",
            "github_issue_number",
            "github_installation_id",
            "agent_id",
            "repo_full_name",
            "conversation_id",
            "publication_lineage_id",
            "cancelled_at",
            "version",
            "next_sequence",
            "created_at",
            "updated_at",
        },
        "execution_requests": {
            "id",
            "work_item_id",
            "sequence",
            "status",
            "wait_deadline",
            "started_at",
            "execution_deadline",
            "terminal_at",
            "terminal_cause",
            "termination_observation",
            "version",
            "created_at",
            "updated_at",
        },
    }

    work_defs = _constraint_definitions("work_items")
    request_defs = _constraint_definitions("execution_requests")
    assert any("primary key (id)" in definition for definition in work_defs)
    assert any(
        "foreign key (agent_id)" in definition
        and "references curie.agents(id) on delete cascade" in definition
        for definition in work_defs
    )
    assert any(
        "foreign key (publication_lineage_id)" in definition
        and "references curie.thread_publication_lineages(id) on delete restrict"
        in definition
        for definition in work_defs
    )
    assert any(
        "unique (github_repository_id, github_issue_number)" in definition
        for definition in work_defs
    )
    assert any(
        "unique (publication_lineage_id)" in definition for definition in work_defs
    )
    for checked_column in (
        "github_repository_id",
        "github_issue_number",
        "github_installation_id",
        "version",
        "next_sequence",
    ):
        assert any(
            definition.startswith("check") and checked_column in definition
            for definition in work_defs
        )

    assert any("primary key (id)" in definition for definition in request_defs)
    assert any(
        "foreign key (work_item_id)" in definition
        and "references curie.work_items(id) on delete cascade" in definition
        for definition in request_defs
    )
    assert any(
        "unique (work_item_id, sequence)" in definition
        for definition in request_defs
    )
    request_checks = "\n".join(
        definition for definition in request_defs if definition.startswith("check")
    )
    for token in (
        "sequence",
        "version",
        "waiting",
        "running",
        "cancellation_requested",
        "completed",
        "failed",
        "expired",
        "cancelled",
        "started_at",
        "execution_deadline",
        "terminal_at",
        "terminal_cause",
        "termination_observation",
    ):
        assert token in request_checks

    indexes = _sql(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' "
        "AND tablename = 'execution_requests'"
    )
    active_indexes = [
        row["indexdef"].lower()
        for row in indexes
        if " where " in row["indexdef"].lower()
    ]
    assert len(active_indexes) == 1
    active_index = active_indexes[0]
    assert "unique index" in active_index
    assert "work_item_id" in active_index
    assert all(
        status in active_index
        for status in ("waiting", "running", "cancellation_requested")
    )

    triggers = _sql(
        "SELECT r.relname AS table_name, t.tgname AS trigger_name, "
        "p.proname AS function_name, pg_get_triggerdef(t.oid) AS trigger_def, "
        "p.prosrc AS function_body "
        "FROM pg_trigger t "
        "JOIN pg_class r ON r.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = r.relnamespace "
        "JOIN pg_proc p ON p.oid = t.tgfoid "
        "WHERE n.nspname = 'curie' AND NOT t.tgisinternal "
        "AND r.relname IN ('work_items', 'execution_requests') "
        "ORDER BY r.relname, t.tgname"
    )
    assert {row["table_name"] for row in triggers} == {
        "work_items",
        "execution_requests",
    }
    trigger_functions = {row["function_name"] for row in triggers}
    assert len(trigger_functions) == len(triggers)
    for row in triggers:
        assert "before update" in row["trigger_def"].lower()
    trigger_bodies = {
        table: "\n".join(
            row["function_body"].lower()
            for row in triggers
            if row["table_name"] == table
        )
        for table in ("work_items", "execution_requests")
    }
    for token in (
        "github_repository_id",
        "github_issue_number",
        "github_installation_id",
        "agent_id",
        "repo_full_name",
        "conversation_id",
        "publication_lineage_id",
        "cancelled_at",
        "created_at",
    ):
        assert token in trigger_bodies["work_items"]
    for token in (
        "work_item_id",
        "sequence",
        "wait_deadline",
        "started_at",
        "execution_deadline",
        "created_at",
    ):
        assert token in trigger_bodies["execution_requests"]

    command.downgrade(config, BELOW)
    assert _sql(
        "SELECT to_regclass('curie.work_items') AS work_items, "
        "to_regclass('curie.execution_requests') AS execution_requests"
    ) == [{"work_items": None, "execution_requests": None}]
    assert _sql(
        "SELECT count(*) AS count FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' "
        "AND p.proname = ANY(CAST(:function_names AS text[]))",
        {"function_names": sorted(trigger_functions)},
    ) == [{"count": 0}]

    command.upgrade(config, REVISION)
    assert _sql(
        "SELECT to_regclass('curie.work_items')::text AS work_items, "
        "to_regclass('curie.execution_requests')::text AS execution_requests"
    ) == [
        {
            "work_items": "curie.work_items",
            "execution_requests": "curie.execution_requests",
        }
    ]


def test_0046_work_item_constraints_and_immutable_fields(
    isolated_migration_db: None,
) -> None:
    command.upgrade(_config(), REVISION)
    agent_id = _seed_agent()
    second_agent_id = _seed_agent()
    work_item_id = _insert_work_item(agent_id)

    for field in (
        "github_repository_id",
        "github_issue_number",
        "github_installation_id",
        "version",
        "next_sequence",
    ):
        params = {
            "id": uuid.uuid4(),
            "github_repository_id": 1100,
            "github_issue_number": 38,
            "github_installation_id": 2100,
            "agent_id": agent_id,
            "repo_full_name": REPO,
            "conversation_id": CONVERSATION,
            "publication_lineage_id": None,
            "cancelled_at": None,
            "version": 1,
            "next_sequence": 1,
            "created_at": STAMP,
            "updated_at": STAMP,
        }
        params[field] = 0
        _rejects(
            "INSERT INTO curie.work_items "
            "(id, github_repository_id, github_issue_number, "
            "github_installation_id, agent_id, repo_full_name, conversation_id, "
            "publication_lineage_id, cancelled_at, version, next_sequence, "
            "created_at, updated_at) VALUES "
            "(:id, :github_repository_id, :github_issue_number, "
            ":github_installation_id, :agent_id, :repo_full_name, "
            ":conversation_id, :publication_lineage_id, :cancelled_at, "
            ":version, :next_sequence, :created_at, :updated_at)",
            params,
        )

    _rejects(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 1001, 37, 9999, :agent_id, :repo, :conversation_id, 1, 1, "
        ":created_at, :updated_at)",
        {
            "id": uuid.uuid4(),
            "agent_id": second_agent_id,
            "repo": "acme-corp/other-bot",
            "conversation_id": "slack:C0EXAMPLE1:1700000000.000200",
            "created_at": STAMP,
            "updated_at": STAMP,
        },
    )

    immutable_updates = {
        "id": uuid.uuid4(),
        "github_repository_id": 9001,
        "github_issue_number": 99,
        "github_installation_id": 9002,
        "agent_id": second_agent_id,
        "repo_full_name": "acme-corp/other-bot",
        "conversation_id": "slack:C0EXAMPLE1:1700000000.000300",
        "created_at": STAMP + timedelta(seconds=1),
    }
    for column, value in immutable_updates.items():
        _rejects(
            f"UPDATE curie.work_items SET {column} = :value WHERE id = :id",
            {"id": work_item_id, "value": value},
        )

    _sql(
        "UPDATE curie.work_items SET cancelled_at = :cancelled_at WHERE id = :id",
        {"id": work_item_id, "cancelled_at": STAMP + timedelta(minutes=1)},
    )
    for cancelled_at in (None, STAMP + timedelta(minutes=2)):
        _rejects(
            "UPDATE curie.work_items SET cancelled_at = :cancelled_at WHERE id = :id",
            {"id": work_item_id, "cancelled_at": cancelled_at},
        )


def test_0046_lineage_is_unique_restricted_and_write_once(
    isolated_migration_db: None,
) -> None:
    command.upgrade(_config(), REVISION)
    agent_id, deployment_id = _seed_agent_and_deployment()
    lineage_id = _seed_lineage(agent_id, deployment_id)
    other_lineage_id = _seed_lineage(
        agent_id,
        deployment_id,
        conversation_id="slack:C0EXAMPLE1:1700000000.000200",
    )
    owner_id = _insert_work_item(agent_id, publication_lineage_id=lineage_id)

    _rejects(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, publication_lineage_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 1002, 38, 2002, :agent_id, :repo, :conversation_id, "
        ":lineage_id, 1, 1, :created_at, :updated_at)",
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "repo": REPO,
            "conversation_id": CONVERSATION,
            "lineage_id": lineage_id,
            "created_at": STAMP,
            "updated_at": STAMP,
        },
    )
    _rejects(
        "DELETE FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": lineage_id},
    )
    for replacement in (None, other_lineage_id):
        _rejects(
            "UPDATE curie.work_items SET publication_lineage_id = :lineage_id "
            "WHERE id = :id",
            {"id": owner_id, "lineage_id": replacement},
        )

    unlinked_id = _insert_work_item(
        agent_id,
        github_issue_number=39,
        publication_lineage_id=None,
    )
    _sql(
        "UPDATE curie.work_items SET publication_lineage_id = :lineage_id "
        "WHERE id = :id",
        {"id": unlinked_id, "lineage_id": other_lineage_id},
    )
    assert _sql(
        "SELECT publication_lineage_id::text AS lineage_id "
        "FROM curie.work_items WHERE id = :id",
        {"id": unlinked_id},
    ) == [{"lineage_id": str(other_lineage_id)}]


def test_0046_request_identity_active_uniqueness_and_cascade(
    isolated_migration_db: None,
) -> None:
    command.upgrade(_config(), REVISION)
    agent_id = _seed_agent()
    work_item_id = _insert_work_item(agent_id)
    request_id = _insert_request(work_item_id)

    _reject_request(work_item_id, sequence=0)
    _reject_request(work_item_id, sequence=2, version=0)
    _reject_request(work_item_id, sequence=2, status="unknown")
    _reject_request(
        work_item_id,
        sequence=2,
        status="running",
        started_at=STAMP,
        execution_deadline=STAMP + timedelta(seconds=1800),
    )
    _reject_request(
        work_item_id,
        id=uuid.uuid4(),
        sequence=1,
        status="cancelled",
        terminal_at=STAMP + timedelta(minutes=1),
        terminal_cause="issue_cancelled",
    )

    assert _sql(
        "SELECT id::text FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    ) == [{"id": str(request_id)}]
    _sql("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert _sql(
        "SELECT "
        "(SELECT count(*) FROM curie.work_items WHERE id = :work_item_id) AS work_count, "
        "(SELECT count(*) FROM curie.execution_requests WHERE id = :request_id) "
        "AS request_count",
        {"work_item_id": work_item_id, "request_id": request_id},
    ) == [{"work_count": 0, "request_count": 0}]


@pytest.mark.parametrize(
    ("status", "overrides"),
    [
        ("waiting", {"started_at": STAMP, "execution_deadline": STAMP + timedelta(seconds=1800)}),
        ("waiting", {"terminal_at": STAMP, "terminal_cause": "capacity_expired"}),
        ("running", {}),
        ("running", {"started_at": STAMP}),
        (
            "running",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_at": STAMP + timedelta(seconds=1),
            },
        ),
        (
            "cancellation_requested",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_cause": "other",
            },
        ),
        (
            "cancellation_requested",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_cause": None,
            },
        ),
        ("completed", {"terminal_cause": "completed"}),
        (
            "completed",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_at": STAMP + timedelta(seconds=1200),
                "terminal_cause": None,
            },
        ),
        ("failed", {"terminal_at": STAMP}),
        (
            "cancelled",
            {
                "terminal_at": STAMP + timedelta(hours=2),
                "terminal_cause": None,
            },
        ),
        (
            "cancelled",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_at": STAMP + timedelta(seconds=10),
                "terminal_cause": "issue_cancelled",
            },
        ),
        (
            "expired",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_at": STAMP + timedelta(seconds=1801),
                "terminal_cause": "execution_deadline",
            },
        ),
        (
            "expired",
            {
                "terminal_at": STAMP + timedelta(hours=2),
                "terminal_cause": None,
            },
        ),
        (
            "expired",
            {
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
                "terminal_at": STAMP + timedelta(seconds=1801),
                "terminal_cause": None,
                "termination_observation": "fixture observed runtime termination",
            },
        ),
    ],
)
def test_0046_rejects_invalid_request_state_shapes(
    isolated_migration_db: None,
    status: str,
    overrides: dict[str, Any],
) -> None:
    command.upgrade(_config(), REVISION)
    work_item_id = _insert_work_item(_seed_agent())
    _reject_request(work_item_id, status=status, **overrides)


def test_0046_deadline_arithmetic_and_request_fields_are_write_once(
    isolated_migration_db: None,
) -> None:
    command.upgrade(_config(), REVISION)
    agent_id = _seed_agent()
    work_item_id = _insert_work_item(agent_id)
    other_work_item_id = _insert_work_item(
        agent_id,
        github_issue_number=38,
    )

    for seconds in (1799, 1801):
        _reject_request(
            work_item_id,
            sequence=seconds,
            status="running",
            started_at=STAMP,
            execution_deadline=STAMP + timedelta(seconds=seconds),
        )

    request_id = _insert_request(work_item_id)
    _sql(
        "UPDATE curie.execution_requests SET status = 'running', "
        "started_at = :started_at, execution_deadline = :execution_deadline "
        "WHERE id = :id",
        {
            "id": request_id,
            "started_at": STAMP,
            "execution_deadline": STAMP + timedelta(seconds=1800),
        },
    )
    assert _sql(
        "SELECT extract(epoch FROM execution_deadline - started_at)::integer "
        "AS seconds FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    ) == [{"seconds": 1800}]

    immutable_updates = {
        "id": uuid.uuid4(),
        "work_item_id": other_work_item_id,
        "sequence": 2,
        "wait_deadline": STAMP + timedelta(hours=2),
        "created_at": STAMP + timedelta(seconds=1),
    }
    for column, value in immutable_updates.items():
        _rejects(
            f"UPDATE curie.execution_requests SET {column} = :value WHERE id = :id",
            {"id": request_id, "value": value},
        )

    for assignment, params in (
        ("started_at = :started_at", {"started_at": None}),
        (
            "started_at = :started_at",
            {"started_at": STAMP + timedelta(seconds=1)},
        ),
        ("execution_deadline = :execution_deadline", {"execution_deadline": None}),
        (
            "execution_deadline = :execution_deadline",
            {"execution_deadline": STAMP + timedelta(seconds=1801)},
        ),
    ):
        _rejects(
            f"UPDATE curie.execution_requests SET {assignment} WHERE id = :id",
            {"id": request_id, **params},
        )

    for assignment, params in (
        (
            "started_at = :started_at, "
            "execution_deadline = :execution_deadline",
            {
                "started_at": STAMP + timedelta(seconds=1),
                "execution_deadline": STAMP + timedelta(seconds=1801),
            },
        ),
        (
            "status = 'waiting', started_at = NULL, "
            "execution_deadline = NULL",
            {},
        ),
    ):
        with pytest.raises(DBAPIError) as excinfo:
            _sql(
                f"UPDATE curie.execution_requests SET {assignment} WHERE id = :id",
                {"id": request_id, **params},
            )
        assert getattr(excinfo.value.orig, "sqlstate", None) == "23514"
        assert "execution deadlines are write once" in str(excinfo.value.orig)
        assert _sql(
            "SELECT status, started_at, execution_deadline "
            "FROM curie.execution_requests WHERE id = :id",
            {"id": request_id},
        ) == [
            {
                "status": "running",
                "started_at": STAMP,
                "execution_deadline": STAMP + timedelta(seconds=1800),
            }
        ]
