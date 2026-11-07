"""The schedule expansion preserves pending approval seals and deadlines."""

import uuid
from types import SimpleNamespace

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from curie_api.identity.http_approval import approval_record_digest
from sqlalchemy.exc import IntegrityError


def test_schedule_expansion_preserves_existing_approval_authority(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0149")
    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, reply_kind, "
        "reply_channel, dedupe_key, expires_at, identity_context) VALUES "
        "(:id, 'schedule-upgrade', 'U0EXAMPLE1', 'Approve work', 'slack', 'C0EXAMPLE1', "
        ":dedupe, now() + interval '30 days', 'opaque-synthetic-seal')",
        {"id": approval_id, "dedupe": str(approval_id)},
    )
    before = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    digest = approval_record_digest(SimpleNamespace(**before))
    command.upgrade(alembic_config(), "0150")
    after = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    assert after == {**before, "remind_every_s": None}
    assert approval_record_digest(SimpleNamespace(**after)) == digest
    sql_rows("UPDATE curie.approvals SET remind_every_s=60 WHERE id=:id", {"id": approval_id})
    scheduled = sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0]
    assert approval_record_digest(SimpleNamespace(**scheduled)) == digest
    with pytest.raises(IntegrityError):
        sql_rows("UPDATE curie.approvals SET remind_every_s=0 WHERE id=:id", {"id": approval_id})
    command.downgrade(alembic_config(), "0149")
    assert sql_dicts("SELECT * FROM curie.approvals WHERE id=:id", {"id": approval_id})[0] == before
