"""The reminder cursor is scheduling data outside the approval seal."""

import uuid
from types import SimpleNamespace

from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from curie_api.identity.http_approval import approval_record_digest


def test_cursor_upgrade_keeps_old_rows_disabled_and_preserves_seal(
    isolated_migration_db: IsolatedMigrationDb,
):
    isolated_migration_db.at("0152")
    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, "
        "reply_kind, reply_channel, dedupe_key) "
        "VALUES (:id, 'schedule-test', 'U0EXAMPLE1', 'Approve work', "
        "'slack', 'C0EXAMPLE1', :dedupe)",
        {"id": approval_id, "dedupe": str(approval_id)},
    )
    before = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    digest = approval_record_digest(SimpleNamespace(**before))
    command.upgrade(alembic_config(), "0153")
    after = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    assert after == {**before, "next_remind_at": None}
    sql_rows(
        "UPDATE curie.approvals SET remind_every_s=60, next_remind_at=now() WHERE id=:id",
        {"id": approval_id},
    )
    scheduled = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    assert approval_record_digest(SimpleNamespace(**scheduled)) == digest
    command.downgrade(alembic_config(), "0152")
    downgraded = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    assert "next_remind_at" not in downgraded
    assert approval_record_digest(SimpleNamespace(**downgraded)) == digest
