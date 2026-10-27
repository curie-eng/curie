"""Real migration evidence. @spec HTTP-APPROVAL-CONTINUITY-5."""

from __future__ import annotations

import uuid

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
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

CHECK = "approvals_identity_context_length_ck"


def revision_ready() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5. Absent migration is a body assertion, not setup error."""
    graph = ScriptDirectory.from_config(alembic_config())
    assert "0137" in {r.revision for r in graph.walk_revisions()}, (
        "approval provenance migration 0137 is missing"
    )
    assert graph.get_revision("0137").down_revision == "0133"
    assert graph.get_heads() == ["0137"]


def seed_approval(status: str = "pending") -> uuid.UUID:
    """@spec HTTP-APPROVAL-CONTINUITY-5. Seed only columns that exist at 0133."""
    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, "
        "reply_kind, reply_channel, reply_placeholder, dedupe_key, status) "
        "VALUES (:id, :conv, 'U0EXAMPLE', 'synthetic approval', 'slack', 'C0EXAMPLE1', "
        "'1700.0001', :dedupe, :status)",
        {
            "id": approval_id,
            "conv": f"th-{approval_id.hex[:8]}",
            "dedupe": uuid.uuid4().hex,
            "status": status,
        },
    )
    return approval_id


def approval_row(approval_id: uuid.UUID) -> dict:
    """@spec HTTP-APPROVAL-CONTINUITY-5."""
    return sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]


def test_upgrade_adds_nullable_bounded_column_without_backfill(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5. No backfill, authority conversion or status change."""
    isolated_migration_db.at("0133")
    revision_ready()
    rows = {status: seed_approval(status) for status in ("pending", "approved", "rejected")}
    before = {status: approval_row(row) for status, row in rows.items()}
    command.upgrade(alembic_config(), "head")
    assert stamped_revision() == "0137"
    assert "identity_context" in column_names("approvals")
    assert constraint_exists(CHECK)
    for status, row in rows.items():
        assert approval_row(row) == {**before[status], "identity_context": None}
    accepted = rows["pending"]
    sql_rows(
        "UPDATE curie.approvals SET identity_context=:value WHERE id=:id",
        {"id": accepted, "value": "x" * 16384},
    )
    assert len(approval_row(accepted)["identity_context"]) == 16384
    with pytest.raises(IntegrityError):
        sql_rows(
            "UPDATE curie.approvals SET identity_context=:value WHERE id=:id",
            {"id": rows["approved"], "value": "x" * 16385},
        )
    with pytest.raises(IntegrityError):
        sql_rows(
            "UPDATE curie.approvals SET identity_context=:value WHERE id=:id",
            {"id": rows["approved"], "value": "é" * 8193},
        )


def test_downgrade_restores_0133_only_when_no_context_is_retained(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5. Downgrade never discards retained authority."""
    isolated_migration_db.at("0133")
    revision_ready()
    command.upgrade(alembic_config(), "head")
    retained = seed_approval()
    sql_rows(
        "UPDATE curie.approvals SET identity_context='synthetic-sealed-context' WHERE id=:id",
        {"id": retained},
    )
    with pytest.raises(RuntimeError, match="approval_identity_context_present"):
        command.downgrade(alembic_config(), "0133")
    assert stamped_revision() == "0137"
    assert approval_row(retained)["identity_context"] == "synthetic-sealed-context"
    sql_rows("UPDATE curie.approvals SET identity_context=NULL WHERE id=:id", {"id": retained})
    command.downgrade(alembic_config(), "0133")
    assert stamped_revision() == "0133"
    assert "identity_context" not in column_names("approvals")
    assert not constraint_exists(CHECK)
    assert "identity_context" not in approval_row(retained)
    command.upgrade(alembic_config(), "head")
    assert stamped_revision() == "0137"
