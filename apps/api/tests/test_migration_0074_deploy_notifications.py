"""@spec DEPLOY-NOTICE-RELEASE-1."""

from __future__ import annotations

import uuid

from _migration_support import alembic_config, column_names, sql_rows
from alembic import command
from alembic.script import ScriptDirectory


def test_0077_follows_released_0076() -> None:
    """@spec DEPLOY-NOTICE-RELEASE-1."""
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0077")
    assert revision is not None
    assert revision.down_revision == "0076"


def test_0077_preserves_released_state_through_upgrade_and_downgrade(
    isolated_migration_db: None,
) -> None:
    """@spec DEPLOY-NOTICE-RELEASE-1."""
    config = alembic_config()
    command.upgrade(config, "0073")
    assert "deploy_notifications" not in column_names("agents")
    assert column_names("deploy_notice_outbox") == set()
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": "acme-dev"},
    )
    item_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.work_items (id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id, "
        "base_branch, base_source, base_commit, base_label_ignored, "
        "readmit_base_branch, readmit_base_source, readmit_base_commit) "
        "VALUES (:id, 4401, 74001, 5501, :agent, 'acme-corp/acme-bot', "
        "'github:acme-corp/acme-bot#74001', 'next', 'label', :sha, 'main', "
        "'main', 'default', :readmit_sha)",
        {"id": item_id, "agent": agent_id, "sha": "a" * 40, "readmit_sha": "b" * 40},
    )
    sql_rows(
        "INSERT INTO curie.factory_poll_cursors (repo_full_name, repository_id, "
        "comments_since, review_comments_since, reviews_since, etags, updated_at) "
        "VALUES ('acme-corp/acme-bot', 4401, '2026-10-01T01:00:00Z', "
        "'2026-10-01T02:00:00Z', '2026-10-01T03:00:00Z', "
        "CAST(:etags AS jsonb), '2026-10-01T04:00:00Z')",
        {"etags": '{"comments": "etag-comments", "reviews": "etag-reviews"}'},
    )
    work_item_query = (
        "SELECT base_branch, base_source, base_commit, base_label_ignored, "
        "readmit_base_branch, readmit_base_source, readmit_base_commit "
        "FROM curie.work_items WHERE id = :id"
    )
    cursor_query = (
        "SELECT repository_id, comments_since, review_comments_since, reviews_since, "
        "etags, updated_at FROM curie.factory_poll_cursors "
        "WHERE repo_full_name = 'acme-corp/acme-bot'"
    )
    released_item = sql_rows(work_item_query, {"id": item_id})
    released_cursor = sql_rows(cursor_query)
    assert len(released_item) == len(released_cursor) == 1
    try:
        command.upgrade(config, "0077")
        assert sql_rows(work_item_query, {"id": item_id}) == released_item
        assert sql_rows(cursor_query) == released_cursor
        assert (
            sql_rows(
                "SELECT deploy_notifications FROM curie.agents WHERE id = :id", {"id": agent_id}
            )[0][0]
            is False
        )
        # repo carries the case-folded repository the per-repository notice
        # bound counts by (docs/operations.md).
        assert column_names("deploy_notice_outbox") == {
            "key",
            "stream",
            "repo",
            "payload",
            "attempts",
            "created_at",
            "enqueued_at",
        }
        sql_rows(
            "INSERT INTO curie.deploy_notice_outbox (key, stream, repo, payload) "
            "VALUES (:key, :stream, :repo, :payload)",
            {
                "key": "a" * 64,
                "stream": "curie:runs:deploy-notices",
                "repo": "acme-corp/acme-bot",
                "payload": "{}",
            },
        )
        assert sql_rows(
            "SELECT attempts, enqueued_at IS NULL FROM curie.deploy_notice_outbox WHERE key = :key",
            {"key": "a" * 64},
        ) == [(0, True)]

        command.downgrade(config, "0073")
        assert sql_rows(work_item_query, {"id": item_id}) == released_item
        assert sql_rows(cursor_query) == released_cursor
        assert sql_rows("SELECT version_num FROM curie.alembic_version") == [("0073",)]
        assert "deploy_notifications" not in column_names("agents")
        assert column_names("deploy_notice_outbox") == set()
    finally:
        command.upgrade(config, "head")
