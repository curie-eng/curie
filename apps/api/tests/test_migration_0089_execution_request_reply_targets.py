"""Migration 0089 stores each execution request's typed reply target (#3831).

The backfill reads the revision objective's first line once, under the rule
the API used to apply on every read. Runs against a private database
(``isolated_migration_db``), never the shared one, per apps/api/CLAUDE.md.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, column_names, sql_dicts
from alembic import command

REPO = "acme-corp/acme-bot"
_COLUMNS = {
    "reply_target_kind",
    "reply_target_pr_number",
    "reply_target_comment_id",
    "reply_target_url",
}
_ISSUE = {
    "reply_target_kind": "issue",
    "reply_target_pr_number": None,
    "reply_target_comment_id": None,
    "reply_target_url": None,
}
_NUMBERS = iter(range(9101, 9199))


def _seed_request(objective: str | None) -> uuid.UUID:
    number = next(_NUMBERS)
    agent_id, work_item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    sql_dicts(
        "INSERT INTO curie.work_items "
        "(id, github_repository_id, github_issue_number, github_installation_id, "
        "agent_id, repo_full_name, conversation_id, next_sequence) "
        "VALUES (:id, 4401, :number, 5501, :agent, :repo, :conversation, 2)",
        {
            "id": work_item_id,
            "number": number,
            "agent": agent_id,
            "repo": REPO,
            "conversation": f"issue-{number}",
        },
    )
    snapshot = objective is not None
    sql_dicts(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, "
        "requester, reply_kind, reply_address, reply_conversation_id) "
        "VALUES (:id, :work_item, 1, 'waiting', "
        "clock_timestamp() + interval '30 seconds', :objective, "
        ":requester, :kind, :address, :conversation)",
        {
            "id": request_id,
            "work_item": work_item_id,
            "objective": objective,
            "requester": "github:6601:octocat" if snapshot else None,
            "kind": "github" if snapshot else None,
            "address": REPO if snapshot else None,
            "conversation": f"issue-{number}" if snapshot else None,
        },
    )
    return request_id


def _targets() -> dict[uuid.UUID, dict[str, Any]]:
    rows = sql_dicts(
        "SELECT id, reply_target_kind, reply_target_pr_number, "
        "reply_target_comment_id, reply_target_url FROM curie.execution_requests"
    )
    return {row.pop("id"): row for row in rows}


def _objective(first_line: str) -> str:
    return f"{first_line}\n\nHuman GitHub review feedback follows as JSON.\n{{}}"


def test_0089_backfills_typed_targets_from_the_revision_objective_and_downgrades(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0088")
    thread_url = f"https://github.com/{REPO}/pull/501#discussion_r88001"
    comment_url = f"https://github.com/{REPO}/pull/502#issuecomment-88002"
    review_url = f"https://github.com/{REPO}/pull/503#pullrequestreview-88003"
    enterprise_url = f"https://ghe.example.com/git/{REPO}/pull/504#discussion_r88004"
    try:
        thread = _seed_request(_objective(thread_url))
        comment = _seed_request(_objective(comment_url))
        review = _seed_request(_objective(review_url))
        enterprise = _seed_request(_objective(enterprise_url))
        issue = _seed_request(f"https://github.com/{REPO}/issues/9001#issuecomment-1\n\nMention.")
        other_repo = _seed_request(
            _objective("https://github.com/acme-corp/other-bot/pull/501#discussion_r88005")
        )
        suffix_repo = _seed_request(
            _objective(f"https://github.com/x{REPO}/pull/501#discussion_r88006")
        )
        second_line = _seed_request(f"Please revise.\n{thread_url}")
        trailing = _seed_request(_objective(f"{thread_url} thanks"))
        wide_pr = _seed_request(
            _objective(f"https://github.com/{REPO}/pull/2147483648#issuecomment-1")
        )
        unparsed = _seed_request("Implement the admitted work item")
        no_snapshot = _seed_request(None)

        command.upgrade(config, "0089")

        assert column_names("execution_requests") >= _COLUMNS
        targets = _targets()
        assert targets[thread] == {
            "reply_target_kind": "review_thread",
            "reply_target_pr_number": "501",
            "reply_target_comment_id": "88001",
            "reply_target_url": thread_url,
        }
        assert targets[comment] == {
            "reply_target_kind": "pull_request",
            "reply_target_pr_number": "502",
            "reply_target_comment_id": None,
            "reply_target_url": comment_url,
        }
        assert targets[review] == {
            "reply_target_kind": "pull_request",
            "reply_target_pr_number": "503",
            "reply_target_comment_id": None,
            "reply_target_url": review_url,
        }
        assert targets[enterprise] == {
            "reply_target_kind": "review_thread",
            "reply_target_pr_number": "504",
            "reply_target_comment_id": "88004",
            "reply_target_url": enterprise_url,
        }
        for refused in (
            issue,
            other_repo,
            suffix_repo,
            second_line,
            trailing,
            wide_pr,
            unparsed,
            no_snapshot,
        ):
            assert targets[refused] == _ISSUE, refused

        command.downgrade(config, "0088")

        assert not (column_names("execution_requests") & _COLUMNS)
    finally:
        command.upgrade(config, "head")


@pytest.mark.parametrize(
    "values",
    [
        {"kind": "pull_request", "pr": None, "comment": None, "url": None},
        {"kind": "review_thread", "pr": "7", "comment": None, "url": None},
        {"kind": "issue", "pr": "7", "comment": None, "url": None},
        {"kind": "issue", "pr": None, "comment": None, "url": "https://github.com/x"},
        {"kind": "pull_request", "pr": "7", "comment": "33", "url": None},
        {"kind": "pull_request", "pr": " ", "comment": None, "url": None},
        {"kind": "thread", "pr": "7", "comment": "33", "url": None},
    ],
)
def test_0089_check_refuses_a_target_whose_columns_disagree_with_its_kind(
    isolated_migration_db: IsolatedMigrationDb, values: dict[str, str | None]
) -> None:
    isolated_migration_db.at("0089")
    request_id = _seed_request(_objective("Revise."))
    with pytest.raises(Exception, match="execution_requests_reply_target_ck"):
        sql_dicts(
            "UPDATE curie.execution_requests SET reply_target_kind = :kind, "
            "reply_target_pr_number = :pr, reply_target_comment_id = :comment, "
            "reply_target_url = :url WHERE id = :id",
            {**values, "id": request_id},
        )
