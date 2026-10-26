"""Migration selfimp_skills_0001 adds learned skills (ADR 0247, epic #4780).

Expand-only: `agents.learned_skills BOOLEAN NOT NULL DEFAULT false`, so every
existing agent comes out of the upgrade with learned skills off, plus the
`learned_skills` head table and the insert-only `learned_skill_revisions`
table. The downgrade drops both tables and the column again.
"""

from __future__ import annotations

import uuid
from typing import Any

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from alembic.script import ScriptDirectory

REVISION = "selfimp_skills_0001"
BELOW = "selfimp_safety_0001"


def _column() -> dict[str, Any] | None:
    rows = sql_dicts(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'agents' "
        "AND column_name = 'learned_skills'"
    )
    return rows[0] if rows else None


def _regclass(table: str) -> str | None:
    (row,) = sql_dicts("SELECT to_regclass(:name)::text AS name", {"name": f"curie.{table}"})
    return row["name"]


def _columns(table: str) -> set[str]:
    rows = sql_dicts(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table",
        {"table": table},
    )
    return {row["column_name"] for row in rows}


def _insert_agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"mskills-{agent_id.hex[:8]}"},
    )
    return agent_id


def test_learned_skills_follows_schedule_safety() -> None:
    script = ScriptDirectory.from_config(alembic_config())
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW


def test_learned_skills_adds_the_toggle_and_both_tables_and_downgrade_removes_them(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    assert _column() is None
    assert _regclass("learned_skills") is None
    assert _regclass("learned_skill_revisions") is None
    # A row that predates the column comes out of the upgrade with it off.
    existing = _insert_agent()
    try:
        command.upgrade(config, REVISION)
        column = _column()
        assert column is not None
        assert column["data_type"] == "boolean"
        assert column["is_nullable"] == "NO"
        assert column["column_default"] == "false"
        rows = sql_dicts("SELECT learned_skills FROM curie.agents WHERE id = :id", {"id": existing})
        assert rows == [{"learned_skills": False}]

        assert _regclass("learned_skills") == "curie.learned_skills"
        assert _regclass("learned_skill_revisions") == "curie.learned_skill_revisions"
        assert {
            "id",
            "agent_id",
            "slug",
            "description",
            "body",
            "revision",
            "deleted_at",
            "created_at",
            "updated_at",
        } <= _columns("learned_skills")
        assert {
            "id",
            "skill_id",
            "revision",
            "action",
            "slug",
            "description",
            "body",
            "digest",
            "reverted_to",
            "author",
            "conversation",
            "turn",
            "source",
            "deleted",
            "created_at",
        } <= _columns("learned_skill_revisions")

        # Only one ACTIVE skill per (agent, slug); a deleted one may share it.
        skill = uuid.uuid4()
        sql_dicts(
            "INSERT INTO curie.learned_skills "
            "(id, agent_id, slug, description, body, revision, deleted_at) "
            "VALUES (:id, :agent, 'deploy', 'd', 'b', 2, now())",
            {"id": skill, "agent": existing},
        )
        sql_dicts(
            "INSERT INTO curie.learned_skills "
            "(id, agent_id, slug, description, body, revision) "
            "VALUES (:id, :agent, 'deploy', 'd', 'b', 1)",
            {"id": uuid.uuid4(), "agent": existing},
        )
        dup_refused = False
        try:
            sql_dicts(
                "INSERT INTO curie.learned_skills "
                "(id, agent_id, slug, description, body, revision) "
                "VALUES (:id, :agent, 'deploy', 'd', 'b', 1)",
                {"id": uuid.uuid4(), "agent": existing},
            )
        except Exception:  # noqa: BLE001 - the unique violation is the point
            dup_refused = True
        assert dup_refused, "a second active skill with the same slug was accepted"

        command.downgrade(config, BELOW)
        assert _column() is None
        assert _regclass("learned_skills") is None
        assert _regclass("learned_skill_revisions") is None
    finally:
        # A failed assertion must not leave this private database below head.
        command.upgrade(config, "head")
