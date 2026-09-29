"""Migration 0038 preserves asserted history and adds principal proof fields."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from sqlalchemy.exc import IntegrityError


def test_0038_backfills_asserted_history_and_round_trips_principal_proof(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0037")

    approval_id = uuid.uuid4()
    old_audit_id = uuid.uuid4()
    old_session_id = uuid.uuid4()
    now = datetime.now(UTC).replace(tzinfo=None)
    sql_rows(
        """
        INSERT INTO curie.approvals
          (id, conversation_id, author, summary, reply_kind, reply_channel,
           reply_placeholder, dedupe_key)
        VALUES
          (:id, :conversation_id, :author, :summary, 'slack', :reply_channel,
           :reply_placeholder, :dedupe_key)
        """,
        {
            "id": approval_id,
            "conversation_id": "th-migration-0038",
            "author": "U0EXAMPLE1",
            "summary": "Historical asserted approval",
            "reply_channel": "C0EXAMPLE1",
            "reply_placeholder": "p-migration-0038",
            "dedupe_key": f"migration-0038-{uuid.uuid4()}",
        },
    )
    sql_rows(
        """
        INSERT INTO curie.approval_audit_entries
          (id, approval_id, action, actor, actor_channel, decision, authorizer,
           authorized, reason, evidence)
        VALUES
          (:id, :approval_id, 'resolved', 'U0ASSERTED1', 'C0EXAMPLE1',
           'approved', 'ChannelMembershipAuthorizer', true, NULL,
           '{"kind":"channel_membership"}'::jsonb)
        """,
        {"id": old_audit_id, "approval_id": approval_id},
    )
    sql_rows(
        """
        INSERT INTO curie.console_sessions
          (id, login_code_hash, login_code_expires_at)
        VALUES (:id, :login_hash, :expires_at)
        """,
        {
            "id": old_session_id,
            "login_hash": "0" * 64,
            "expires_at": now + timedelta(minutes=5),
        },
    )

    command.upgrade(config, "0038")

    historical_audit = sql_dicts(
        """
        SELECT principal_kind, authenticated
        FROM curie.approval_audit_entries
        WHERE id = :id
        """,
        {"id": old_audit_id},
    )
    assert historical_audit == [{"principal_kind": None, "authenticated": False}]
    historical_session = sql_dicts(
        "SELECT subject FROM curie.console_sessions WHERE id = :id",
        {"id": old_session_id},
    )
    assert historical_session == [{"subject": None}]

    # An old-shaped system writer (the expiry sweeper) gets the same honest
    # false/NULL defaults after the migration rather than being retro-labeled.
    system_audit_id = uuid.uuid4()
    sql_rows(
        """
        INSERT INTO curie.approval_audit_entries
          (id, approval_id, action, actor, decision, authorizer, authorized)
        VALUES
          (:id, :approval_id, 'expired', 'system', 'expired',
           'ExpirySweeper', true)
        """,
        {"id": system_audit_id, "approval_id": approval_id},
    )
    assert sql_dicts(
        """
        SELECT principal_kind, authenticated
        FROM curie.approval_audit_entries
        WHERE id = :id
        """,
        {"id": system_audit_id},
    ) == [{"principal_kind": None, "authenticated": False}]

    principal_audit_id = uuid.uuid4()
    sql_rows(
        """
        INSERT INTO curie.approval_audit_entries
          (id, approval_id, action, actor, decision, authorizer, authorized,
           principal_kind, authenticated)
        VALUES
          (:id, :approval_id, 'resolved', 'U0EXAMPLE2', 'approved',
           'ExplicitUserListAuthorizer', true, 'operator', true)
        """,
        {"id": principal_audit_id, "approval_id": approval_id},
    )
    assert sql_dicts(
        """
        SELECT principal_kind, authenticated
        FROM curie.approval_audit_entries
        WHERE id = :id
        """,
        {"id": principal_audit_id},
    ) == [{"principal_kind": "operator", "authenticated": True}]

    sql_rows(
        "UPDATE curie.console_sessions SET subject = :subject WHERE id = :id",
        {"subject": "U0EXAMPLE2", "id": old_session_id},
    )
    assert sql_dicts(
        "SELECT subject FROM curie.console_sessions WHERE id = :id",
        {"id": old_session_id},
    ) == [{"subject": "U0EXAMPLE2"}]

    with pytest.raises(IntegrityError):
        sql_rows(
            """
            INSERT INTO curie.approval_audit_entries
              (id, approval_id, action, actor, decision, authorizer, authorized,
               principal_kind, authenticated)
            VALUES
              (:id, :approval_id, 'resolved', 'U0EXAMPLE2', 'approved',
               'ExplicitUserListAuthorizer', true, 'asserted', true)
            """,
            {"id": uuid.uuid4(), "approval_id": approval_id},
        )

    checks = sql_dicts(
        """
        SELECT conname, pg_get_constraintdef(oid) AS definition
        FROM pg_constraint
        WHERE conrelid = 'curie.approval_audit_entries'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%principal_kind%'
        """
    )
    assert len(checks) == 1
    assert checks[0]["conname"] == "approval_audit_principal_kind_ck"
    for kind in ("chat", "console", "operator"):
        assert kind in checks[0]["definition"]

    command.downgrade(config, "0037")
    assert sql_dicts(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'curie'
          AND (
            (table_name = 'approval_audit_entries'
             AND column_name IN ('principal_kind', 'authenticated'))
            OR (table_name = 'console_sessions' AND column_name = 'subject')
          )
        """
    ) == []
