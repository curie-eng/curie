"""@spec PROTECTED-HOOK-SOURCE-1."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from curie_api import models
from fastapi.testclient import TestClient
from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import IntegrityError

SPEC = "PROTECTED-HOOK-SOURCE-1"
COLUMNS = {
    "agent_id": ("uuid", "NO"),
    "hook": ("character varying", "NO"),
    "generation": ("bigint", "NO"),
    "operation_id": ("uuid", "NO"),
    "mode": ("character varying", "NO"),
    "tool_access": ("character varying", "YES"),
    "runtime_id": ("character varying", "YES"),
    "qualification_id": ("character varying", "YES"),
    "bundle_digest": ("character varying", "YES"),
    "legacy_generation": ("bigint", "NO"),
    "updated_at": ("timestamp with time zone", "NO"),
}
INSERT = (
    "INSERT INTO curie.hook_source_policies "
    "(agent_id, hook, generation, operation_id, mode, tool_access, runtime_id, "
    "qualification_id, bundle_digest, legacy_generation, updated_at) "
    "VALUES (:agent_id, :hook, :generation, :operation_id, :mode, :tool_access, "
    ":runtime_id, :qualification_id, :bundle_digest, :legacy_generation, :updated_at)"
)


def _columns() -> dict[str, dict[str, Any]]:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    rows = sql_dicts(
        "SELECT column_name, data_type, is_nullable, character_maximum_length "
        "FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'hook_source_policies'"
    )
    assert rows, f"{SPEC}: Alembic head must create curie.hook_source_policies"
    return {row["column_name"]: row for row in rows}


def _agent(client: TestClient, auth_headers: dict[str, str]) -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    response = client.post(
        "/agents",
        json={
            "name": f"source-schema-{uuid.uuid4().hex}",
            "channel": {"kind": "slack", "address": f"C{uuid.uuid4().hex.upper()}"},
        },
        headers=auth_headers,
    )
    assert response.status_code == 201, response.text
    return uuid.UUID(response.json()["id"])


def _policy(agent_id: uuid.UUID, **changes: Any) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    return {
        "agent_id": agent_id,
        "hook": "daily-summary",
        "generation": 2**53 + 17,
        "operation_id": uuid.uuid4(),
        "mode": "protected",
        "tool_access": "read-only",
        "runtime_id": "runtime-test",
        "qualification_id": "qualification-test",
        "bundle_digest": "sha256:" + "a" * 64,
        "legacy_generation": 2**53 + 19,
        "updated_at": datetime.now(UTC),
        **changes,
    }


def test_source_policy_head_schema_and_orm_match(clean_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    columns = _columns()
    assert set(columns) == set(COLUMNS), "Source secrets/consumer credentials must not be stored"
    assert {
        name: (row["data_type"], row["is_nullable"]) for name, row in columns.items()
    } == COLUMNS
    assert columns["hook"]["character_maximum_length"] == 63
    primary_key = sql_dicts(
        "SELECT k.column_name FROM information_schema.table_constraints t "
        "JOIN information_schema.key_column_usage k "
        "ON (t.constraint_catalog, t.constraint_schema, t.constraint_name) = "
        "(k.constraint_catalog, k.constraint_schema, k.constraint_name) "
        "WHERE t.table_schema = 'curie' AND t.table_name = 'hook_source_policies' "
        "AND t.constraint_type = 'PRIMARY KEY' ORDER BY k.ordinal_position"
    )
    assert [row["column_name"] for row in primary_key] == ["agent_id", "hook"]
    model = getattr(models, "HookSourcePolicy", None)
    assert model is not None, f"{SPEC}: HookSourcePolicy ORM model is required"
    table = model.__table__
    assert table.schema == "curie" and table.name == "hook_source_policies"
    assert set(table.columns.keys()) == set(columns)
    assert [column.name for column in table.primary_key] == ["agent_id", "hook"]
    for name, (_, nullable) in COLUMNS.items():
        assert table.columns[name].nullable is (nullable == "YES")
    for name in ("agent_id", "operation_id"):
        assert isinstance(table.columns[name].type, UUID)
    for name in ("generation", "legacy_generation"):
        assert isinstance(table.columns[name].type, BigInteger)
    assert isinstance(table.columns["hook"].type, String)
    assert table.columns["hook"].type.length == 63
    assert isinstance(table.columns["updated_at"].type, DateTime)
    assert table.columns["updated_at"].type.timezone is True
    foreign_keys = list(table.columns["agent_id"].foreign_keys)
    assert len(foreign_keys) == 1
    assert foreign_keys[0].target_fullname == "curie.agents.id"
    assert foreign_keys[0].ondelete == "CASCADE"


def test_source_policy_round_trips_bigint_and_ordinary_tombstone(
    client: TestClient, clean_db: None, auth_headers: dict[str, str]
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    _columns()
    agent_id = _agent(client, auth_headers)
    policy = _policy(agent_id)
    sql_dicts(INSERT, policy)
    assert sql_dicts(
        "SELECT * FROM curie.hook_source_policies WHERE agent_id = :agent_id AND hook = :hook",
        {"agent_id": agent_id, "hook": policy["hook"]},
    ) == [policy]
    tombstone = _policy(
        agent_id,
        generation=policy["generation"] + 1,
        mode="ordinary",
        tool_access=None,
        runtime_id=None,
        qualification_id=None,
        bundle_digest=None,
    )
    sql_dicts(
        "UPDATE curie.hook_source_policies SET generation = :generation, "
        "operation_id = :operation_id, mode = :mode, tool_access = :tool_access, "
        "runtime_id = :runtime_id, qualification_id = :qualification_id, "
        "bundle_digest = :bundle_digest, updated_at = :updated_at "
        "WHERE agent_id = :agent_id AND hook = :hook",
        tombstone,
    )
    assert sql_dicts(
        "SELECT * FROM curie.hook_source_policies WHERE agent_id = :agent_id",
        {"agent_id": agent_id},
    ) == [tombstone]


@pytest.mark.parametrize(
    "invalid",
    [
        {"mode": "unknown"},
        {"mode": None},
        {"tool_access": "unrestricted"},
        {"tool_access": None},
        {"runtime_id": None},
        {"qualification_id": None},
        {"bundle_digest": None},
        {"generation": 0},
        {"generation": -1},
        {"generation": None},
        {"legacy_generation": None},
        {"operation_id": None},
        {"updated_at": None},
        {"mode": "ordinary"},
        *[
            {
                "mode": "ordinary",
                "tool_access": None,
                "runtime_id": None,
                "qualification_id": None,
                "bundle_digest": None,
                field: value,
            }
            for field, value in [
                ("tool_access", "read-only"),
                ("runtime_id", "runtime-test"),
                ("qualification_id", "qualification-test"),
                ("bundle_digest", "sha256:" + "a" * 64),
            ]
        ],
    ],
)
def test_source_policy_database_refuses_invalid_rows_without_persisting(
    client: TestClient, clean_db: None, auth_headers: dict[str, str], invalid: dict[str, Any]
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    _columns()
    agent_id = _agent(client, auth_headers)
    with pytest.raises(IntegrityError):
        sql_dicts(INSERT, _policy(agent_id, **invalid))
    assert (
        sql_dicts(
            "SELECT * FROM curie.hook_source_policies WHERE agent_id = :agent_id",
            {"agent_id": agent_id},
        )
        == []
    )


def test_source_policy_key_is_per_agent_and_hook_and_agent_delete_cascades(
    client: TestClient, clean_db: None, auth_headers: dict[str, str]
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    _columns()
    first, second = _agent(client, auth_headers), _agent(client, auth_headers)
    sql_dicts(INSERT, _policy(first))
    sql_dicts(INSERT, _policy(first, hook="other-hook"))
    sql_dicts(INSERT, _policy(second))
    with pytest.raises(IntegrityError):
        sql_dicts(INSERT, _policy(first))
    with pytest.raises(IntegrityError):
        sql_dicts(INSERT, _policy(uuid.uuid4()))
    sql_dicts("DELETE FROM curie.agents WHERE id = :id", {"id": first})
    assert sql_dicts("SELECT agent_id FROM curie.hook_source_policies") == [{"agent_id": second}]


def test_source_policy_additive_upgrade_preserves_existing_agent(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-1."""
    isolated_migration_db.at("0073")
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"source-schema-{uuid.uuid4().hex}"},
    )
    before = sql_dicts("SELECT * FROM curie.agents WHERE id = :id", {"id": agent_id})
    command.upgrade(alembic_config(), "head")
    _columns()
    assert sql_dicts("SELECT * FROM curie.agents WHERE id = :id", {"id": agent_id}) == before
    assert sql_dicts("SELECT * FROM curie.hook_source_policies") == []
