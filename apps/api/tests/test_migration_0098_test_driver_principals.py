"""Audit constraint expansion preserves history and refuses unknown principals."""

import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from sqlalchemy.exc import IntegrityError


def _audit(approval_id: uuid.UUID, kind: str | None) -> uuid.UUID:
    row_id = uuid.uuid4()
    sql_rows(
        """
        INSERT INTO curie.approval_audit_entries
            (id, approval_id, action, actor, decision, authorizer, authorized,
             principal_kind, authenticated)
        VALUES (:id, :approval_id, 'denied', 'U0EXAMPLE1', 'approved',
                'ExplicitUserListAuthorizer', false, :kind, :authenticated)
        """,
        {"id": row_id, "approval_id": approval_id, "kind": kind, "authenticated": kind is not None},
    )
    return row_id


def test_0098_expands_principals_without_relabelling_or_deleting_history(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0097")
    approval_id = uuid.uuid4()
    sql_rows(
        """
        INSERT INTO curie.approvals
            (id, conversation_id, author, summary, reply_kind, reply_channel,
             reply_placeholder, dedupe_key)
        VALUES (:id, 'example-thread', 'U0EXAMPLE1', 'Example approval', 'slack',
                'C0EXAMPLE1', 'example-ts', :key)
        """,
        {"id": approval_id, "key": uuid.uuid4().hex},
    )
    historical = [
        _audit(approval_id, kind)
        for kind in (None, "chat", "console", "operator", "adapter", "platform")
    ]
    before = sql_dicts("SELECT * FROM curie.approval_audit_entries ORDER BY id")
    with pytest.raises(IntegrityError):
        _audit(approval_id, "test_driver")
    command.upgrade(alembic_config(), "0098")
    assert sql_dicts("SELECT * FROM curie.approval_audit_entries ORDER BY id") == before
    driver_id = _audit(approval_id, "test_driver")
    with pytest.raises(IntegrityError):
        _audit(approval_id, "unknown")
    after = sql_dicts("SELECT * FROM curie.approval_audit_entries ORDER BY id")
    # Honest append-only driver history makes the old constraint inapplicable;
    # rollback cannot silently relabel or remove those rows.
    with pytest.raises(IntegrityError):
        command.downgrade(alembic_config(), "0097")
    assert sql_dicts("SELECT * FROM curie.approval_audit_entries ORDER BY id") == after
    sql_rows("DELETE FROM curie.approval_audit_entries WHERE id=:id", {"id": driver_id})
    command.downgrade(alembic_config(), "0097")
    assert sql_dicts("SELECT * FROM curie.approval_audit_entries ORDER BY id") == before
    with pytest.raises(IntegrityError):
        _audit(approval_id, "test_driver")
    assert len(historical) == 6
