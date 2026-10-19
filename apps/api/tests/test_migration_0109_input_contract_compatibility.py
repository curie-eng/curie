"""Apply 0109 to populated PostgreSQL and exercise receipt storage with raw SQL.

The isolated migration database is cloned at 0108 before the production ALTER
migration runs. No ORM model or Python receipt validator participates in writes.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, column_names, sql_dicts
from alembic import command
from sqlalchemy.exc import IntegrityError

RECEIPT = {
    "interface": "curie/input-contracts@1",
    "contract": "example.specification/v1",
    "deployment_id": "22222222-2222-4222-8222-222222222222",
    "version_id": "33333333-3333-4333-8333-333333333333",
    "bundle_sha256": "a" * 64,
    "body_sha256": "b" * 64,
}
BODY = "Private requirement content must not be stored in the receipt."


def _seed_legacy_request() -> uuid.UUID:
    agent, item, request = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent, "name": f"receipt-{agent.hex[:12]}"},
    )
    sql_dicts(
        "INSERT INTO curie.work_items "
        "(id, agent_id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, repository_path, "
        "code_host_installation_id, conversation_id, next_sequence) "
        "VALUES (:id, :agent, 'github', 'github.com', '4401', '12', 'github', "
        "'github.com', '4401', 'example/repository', 5501, 'issue-12', 2)",
        {"id": item, "agent": agent},
    )
    sql_dicts(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline) "
        "VALUES (:id, :item, 1, 'waiting', clock_timestamp() + interval '30 minutes')",
        {"id": request, "item": item},
    )
    return request


@pytest.fixture
def migrated_request(isolated_migration_db: IsolatedMigrationDb) -> Iterator[uuid.UUID]:
    isolated_migration_db.at("0108")
    request = _seed_legacy_request()
    before_requests = column_names("execution_requests")
    before_versions = column_names("agent_versions")
    assert "input_contract_receipt" not in before_requests
    command.upgrade(alembic_config(), "0109")
    assert column_names("execution_requests") - before_requests == {"input_contract_receipt"}
    assert column_names("agent_versions") - before_versions == {"input_contracts"}
    yield request


def _write(request: uuid.UUID, receipt: Any) -> None:
    sql_dicts(
        "UPDATE curie.execution_requests SET input_contract_receipt = CAST(:value AS jsonb) "
        "WHERE id = :id",
        {"id": request, "value": json.dumps(receipt)},
    )


def _stored(request: uuid.UUID) -> Any:
    (row,) = sql_dicts(
        "SELECT input_contract_receipt FROM curie.execution_requests WHERE id = :id",
        {"id": request},
    )
    return row["input_contract_receipt"]


def test_0109_preserves_legacy_null_and_adds_only_nullable_metadata(
    migrated_request: uuid.UUID,
) -> None:
    assert _stored(migrated_request) is None
    columns = sql_dicts(
        "SELECT table_name, column_name, udt_name, is_nullable, column_default "
        "FROM information_schema.columns WHERE table_schema = 'curie' AND "
        "((table_name = 'execution_requests' AND column_name = 'input_contract_receipt') "
        "OR (table_name = 'agent_versions' AND column_name = 'input_contracts')) "
        "ORDER BY table_name"
    )
    assert columns == [
        {
            "table_name": table,
            "column_name": column,
            "udt_name": "jsonb",
            "is_nullable": "YES",
            "column_default": None,
        }
        for table, column in [
            ("agent_versions", "input_contracts"),
            ("execution_requests", "input_contract_receipt"),
        ]
    ]
    sql_dicts(
        "UPDATE curie.execution_requests SET input_contract_receipt = NULL WHERE id = :id",
        {"id": migrated_request},
    )
    assert _stored(migrated_request) is None


def test_0109_persists_exactly_the_six_string_receipt_fields(
    migrated_request: uuid.UUID,
) -> None:
    _write(migrated_request, RECEIPT)
    stored = _stored(migrated_request)
    assert stored == RECEIPT
    assert set(stored) == set(RECEIPT)
    assert all(isinstance(value, str) for value in stored.values())
    assert BODY not in json.dumps(stored)


@pytest.mark.parametrize("missing", RECEIPT)
def test_0109_rejects_each_missing_receipt_field(
    migrated_request: uuid.UUID, missing: str
) -> None:
    value = {key: text for key, text in RECEIPT.items() if key != missing}
    with pytest.raises(IntegrityError) as caught:
        _write(migrated_request, value)
    assert caught.value.orig.sqlstate == "23514"
    assert _stored(migrated_request) is None


@pytest.mark.parametrize("field", RECEIPT)
@pytest.mark.parametrize("invalid", [None, 1, True, [], {}, ["text"]])
def test_0109_rejects_non_string_values_for_every_receipt_field(
    migrated_request: uuid.UUID, field: str, invalid: Any
) -> None:
    with pytest.raises(IntegrityError) as caught:
        _write(migrated_request, {**RECEIPT, field: invalid})
    assert caught.value.orig.sqlstate == "23514"
    assert _stored(migrated_request) is None


@pytest.mark.parametrize("extra", [{"body": BODY}, {"unexpected": "text"}])
def test_0109_rejects_body_content_and_other_extra_fields(
    migrated_request: uuid.UUID, extra: dict[str, str]
) -> None:
    with pytest.raises(IntegrityError) as caught:
        _write(migrated_request, {**RECEIPT, **extra})
    assert caught.value.orig.sqlstate == "23514"
    assert _stored(migrated_request) is None


@pytest.mark.parametrize("value", [None, [], "receipt", 1, True])
def test_0109_rejects_json_null_and_non_object_receipts(
    migrated_request: uuid.UUID, value: Any
) -> None:
    # JSON null is different from the SQL NULL retained for legacy requests.
    with pytest.raises(IntegrityError) as caught:
        _write(migrated_request, value)
    assert caught.value.orig.sqlstate == "23514"
    assert _stored(migrated_request) is None
