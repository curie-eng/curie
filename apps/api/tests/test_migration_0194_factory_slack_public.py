"""Physical root ownership and public outbox boundaries across the migration."""

from __future__ import annotations

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from sqlalchemy.exc import DBAPIError, IntegrityError

from apps.api.tests.test_migration_0178_factory_slack_progress_card import _thread


def test_factory_roots_survive_upgrade_and_cannot_alias_another_item(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0191")
    first = _thread(41)
    second = _thread(42)
    before = sql_dicts("SELECT * FROM curie.factory_slack_threads ORDER BY root_ts")
    command.upgrade(alembic_config(), "0194")
    assert sql_dicts("SELECT * FROM curie.factory_slack_threads ORDER BY root_ts") == before
    assert sql_rows("SELECT work_item_id FROM curie.factory_slack_public_threads") == []

    sql_rows(
        """
        INSERT INTO curie.factory_slack_public_threads
          (work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence)
        SELECT work_item_id, agent_id, channel_identity, channel, root_ts, first_sequence
          FROM curie.factory_slack_threads WHERE work_item_id = :id
    """,
        {"id": first},
    )
    assert sql_rows(
        """
        SELECT destination_role FROM curie.factory_slack_mapped_roots WHERE work_item_id = :id
    """,
        {"id": first},
    ) == [("public",)]
    assert sql_rows(
        """
        SELECT destination_role FROM curie.factory_slack_mapped_roots WHERE work_item_id = :id
    """,
        {"id": second},
    ) == [("origin",)]
    for item in (first, second):
        sql_rows(
            """
            UPDATE curie.factory_slack_threads SET card_lease_owner = 'earlier-worker',
                   card_lease_expires_at = now() + interval '1 minute' WHERE work_item_id = :id
        """,
            {"id": item},
        )
    assert sql_rows("""
        SELECT card_lease_owner FROM curie.factory_slack_threads ORDER BY root_ts
    """) == [(None,), ("earlier-worker",)]
    with pytest.raises(IntegrityError, match="factory_slack_root_conflict"):
        sql_rows(
            """
          UPDATE curie.factory_slack_public_threads SET root_ts = :other WHERE work_item_id = :id
        """,
            {"other": before[1]["root_ts"], "id": first},
        )

    sql_rows(
        """
        INSERT INTO curie.factory_slack_posts
            (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
        VALUES (:id, 'card', 'card', 0, 0, 1, '{"title":"restricted"}')
    """,
        {"id": first},
    )
    assert sql_rows(
        """
        SELECT failed_at IS NOT NULL, last_error, payload FROM curie.factory_slack_posts
        WHERE work_item_id = :id
    """,
        {"id": first},
    ) == [(True, "transferred_to_public", {})]
    with pytest.raises(IntegrityError, match="factory_slack_public_posts_payload_ck"):
        sql_rows(
            """
            INSERT INTO curie.factory_slack_public_posts
            (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
            VALUES (:id, 'card', 'card', 0, 0, 1, '{"title":"restricted"}')
        """,
            {"id": first},
        )
    with pytest.raises(DBAPIError, match="factory_slack_public_state_present"):
        command.downgrade(alembic_config(), "0191")
