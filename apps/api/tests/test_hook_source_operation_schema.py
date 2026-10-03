"""@spec PROTECTED-HOOK-SOURCE-10."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts
from alembic import command
from curie_api import models
from sqlalchemy.exc import DBAPIError


@pytest.fixture
def ledger_db(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    isolated_migration_db.at("head")
    assert (
        sql_dicts("SELECT to_regclass('curie.hook_source_operations') AS ledger")[0]["ledger"]
        is not None
    ), "PROTECTED-HOOK-SOURCE-10: durable attempt ledger must exist"


def _agent() -> uuid.UUID:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"ledger-test-{agent_id.hex}"},
    )
    return agent_id


def _attempt(agent_id: uuid.UUID, **changes: Any) -> dict[str, Any]:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    return {
        "agent_id": agent_id,
        "hook": "daily-summary",
        "operation_id": uuid.uuid4(),
        "intent_sha256": "a" * 64,
        "status": "pending",
        "generation": 2**53 + 17,
        "attempted_at": datetime.now(UTC),
        **changes,
    }


def _insert(attempt: dict[str, Any]) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id, hook, operation_id, intent_sha256, status, generation, attempted_at) "
        "VALUES (:agent_id, :hook, :operation_id, :intent_sha256, :status, "
        ":generation, :attempted_at)",
        attempt,
    )


def test_ledger_has_separate_exact_columns_and_composite_identity(ledger_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    rows = sql_dicts(
        "SELECT column_name, data_type, is_nullable, character_maximum_length "
        "FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'hook_source_operations'"
    )
    columns = {row["column_name"]: row for row in rows}
    assert set(columns) == {
        "agent_id",
        "hook",
        "operation_id",
        "intent_sha256",
        "status",
        "generation",
        "attempted_at",
    }
    assert all(row["is_nullable"] == "NO" for row in rows)
    assert columns["generation"]["data_type"] == "bigint"
    assert columns["attempted_at"]["data_type"] == "timestamp with time zone"
    assert columns["hook"]["character_maximum_length"] == 63
    assert columns["intent_sha256"]["character_maximum_length"] == 64
    model = getattr(models, "HookSourceOperation", None)
    assert model is not None
    assert model.__table__.schema == "curie"
    assert set(model.__table__.columns.keys()) == set(columns)
    assert [column.name for column in model.__table__.primary_key] == [
        "agent_id",
        "hook",
        "operation_id",
    ]
    foreign_keys = list(model.__table__.columns.agent_id.foreign_keys)
    assert len(foreign_keys) == 1
    assert foreign_keys[0].target_fullname == "curie.agents.id"
    assert foreign_keys[0].ondelete == "CASCADE"
    policies = sql_dicts(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'hook_source_policies'"
    )
    assert len(policies) == 11


def test_pending_generation_survives_and_commits_without_reallocation(ledger_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    attempt = _attempt(_agent())
    _insert(attempt)
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == [attempt]
    sql_dicts(
        "UPDATE curie.hook_source_operations SET status = 'committed' "
        "WHERE operation_id = :operation_id",
        attempt,
    )
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == [
        {**attempt, "status": "committed"}
    ]


@pytest.mark.parametrize(
    "changes",
    [
        {"generation": None},
        {"generation": 0},
        {"generation": -1},
        {"generation": 2**63},
        {"status": "unknown"},
        {"status": None},
        {"intent_sha256": "A" * 64},
        {"intent_sha256": "g" * 64},
        {"intent_sha256": "a" * 63},
        {"attempted_at": None},
    ],
)
def test_ledger_rejects_invalid_attempts_without_persistence(
    ledger_db: None, changes: dict[str, Any]
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    with pytest.raises(DBAPIError):
        _insert(_attempt(_agent(), **changes))
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("agent_id", uuid.UUID("00000000-0000-0000-0000-000000000001")),
        ("hook", "other-hook"),
        ("operation_id", uuid.UUID("00000000-0000-0000-0000-000000000002")),
        ("intent_sha256", "b" * 64),
        ("generation", 2**53 + 18),
        ("attempted_at", datetime(2026, 1, 1, tzinfo=UTC)),
    ],
)
def test_attempt_identity_and_generation_are_immutable(
    ledger_db: None, field: str, value: Any
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    attempt = _attempt(_agent())
    _insert(attempt)
    with pytest.raises(DBAPIError):
        sql_dicts(
            f"UPDATE curie.hook_source_operations SET {field} = :new_value "
            "WHERE operation_id = :operation_id",
            {"new_value": value, "operation_id": attempt["operation_id"]},
        )
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == [attempt]


def test_all_attempt_generations_are_unique_and_history_only_cascades(ledger_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    agent_id = _agent()
    attempt = _attempt(agent_id)
    _insert(attempt)
    with pytest.raises(DBAPIError):
        _insert(_attempt(agent_id, generation=attempt["generation"]))
    with pytest.raises(DBAPIError):
        _insert({**attempt, "generation": attempt["generation"] + 1})
    _insert(_attempt(agent_id, hook="other-hook", generation=attempt["generation"]))
    with pytest.raises(DBAPIError):
        sql_dicts("DELETE FROM curie.hook_source_operations WHERE hook = 'daily-summary'")
    sql_dicts("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == []


def test_committed_attempt_cannot_return_to_pending(ledger_db: None) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    attempt = _attempt(_agent(), status="committed")
    _insert(attempt)
    with pytest.raises(DBAPIError):
        sql_dicts("UPDATE curie.hook_source_operations SET status = 'pending'")
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == [attempt]


def test_additive_upgrade_backfills_only_current_policy_without_changing_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    isolated_migration_db.at("0075")
    agent_id = _agent()
    operation_id = uuid.uuid4()
    target = {
        "mode": "ordinary",
        "tool_access": None,
        "runtime_id": None,
        "qualification_id": None,
        "bundle_digest": None,
    }
    updated_at = datetime.now(UTC)
    sql_dicts(
        "INSERT INTO curie.hook_source_policies (agent_id, hook, generation, "
        "operation_id, mode, tool_access, runtime_id, qualification_id, "
        "bundle_digest, legacy_generation, updated_at) VALUES "
        "(:agent_id, 'daily-summary', :generation, :operation_id, 'ordinary', "
        "NULL, NULL, NULL, NULL, 0, :updated_at)",
        {
            "agent_id": agent_id,
            "generation": 2**53 + 17,
            "operation_id": operation_id,
            "updated_at": updated_at,
        },
    )
    before = sql_dicts("SELECT * FROM curie.hook_source_policies")
    command.upgrade(alembic_config(), "head")
    assert sql_dicts("SELECT * FROM curie.hook_source_policies") == before
    expected_intent = hashlib.sha256(
        json.dumps(target, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    assert sql_dicts("SELECT * FROM curie.hook_source_operations") == [
        {
            "agent_id": agent_id,
            "hook": "daily-summary",
            "operation_id": operation_id,
            "intent_sha256": expected_intent,
            "status": "committed",
            "generation": 2**53 + 17,
            "attempted_at": updated_at,
        }
    ]
