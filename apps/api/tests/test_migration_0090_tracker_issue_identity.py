"""Migration 0090 keys WorkItems by their tracker issue (#3831, ADR 0197).

Pre-migration GitHub rows are seeded at 0089 with raw SQL, the database is
upgraded, and the test asserts that every primary key and request id survives,
the tracker and code host columns are backfilled from the GitHub ones, and a
replayed label notice for the same issue dedupes through the admission service
the GitHub intake calls. Runs against a private database
(``isolated_migration_db``), never the shared one, per apps/api/CLAUDE.md.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_dicts,
)
from alembic import command
from curie_api import github_factory, workitem_dispatch
from curie_api.config import get_settings
from curie_api.forges.identity import issue_lock_keys, notice_request_id
from curie_api.forges.types import GITHUB, Actor, Disposition, MarkedNotice
from curie_api.github_factory_events import FactoryNotice
from curie_api.models import AgentChannel, ExecutionRequest, WorkItem
from curie_api.threadkeys import route_thread_key
from curie_api.workitems.lifecycle import WorkItemOutcome
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

REPO = "acme-corp/acme-bot"
REPOSITORY_ID = 4401
ISSUE_NUMBER = 12
INSTALLATION_ID = 5501
LABEL_EVENT_ID = 987654321
SENDER_ID, SENDER = 6601, "octocat"
# The legacy GitHub derivations, recorded in forges/test_identity_golden.py.
GOLDEN_LABEL_EVENT_REQUEST = uuid.UUID("0632a3d4-7d07-54f5-8eb0-790f63041ca1")
GOLDEN_LOCK_KEYS = (-1010687396, -691935689)
OBJECTIVE = f"https://github.com/{REPO}/issues/{ISSUE_NUMBER}"
REQUESTER = f"github:{SENDER_ID}:{SENDER}"
REPLY = ("github", REPO, f"issue-{ISSUE_NUMBER}")

_GONE = {
    "work_items": {
        "github_repository_id",
        "github_issue_number",
        "github_installation_id",
        "repo_full_name",
    },
    "thread_publication_lineages": {
        "github_repository_id",
        "github_installation_id",
        "github_pr_node_id",
    },
    "factory_poll_cursors": {"repository_id", "repo_full_name"},
}


def _seed() -> dict[str, Any]:
    """Pre-migration GitHub rows on 0089: the factory's own shape for one issue."""

    agent_id, binding_id = uuid.uuid4(), uuid.uuid4()
    work_item_id, lineage_id, legacy_lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    deployment_id = uuid.uuid4()
    conversation = route_thread_key(REPLY[0], None, REPLY[1], REPLY[2])
    version_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)",
        {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}", "repo": REPO},
    )
    sql_dicts(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, 'github', :repo)",
        {"id": binding_id, "agent": agent_id, "repo": REPO},
    )
    sql_dicts(
        "INSERT INTO curie.agent_versions (id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent, 'v1', NULL, 'migration-test')",
        {"id": version_id, "agent": agent_id},
    )
    sql_dicts(
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent, :version, CAST('dev' AS curie.environment), 'active')",
        {"id": deployment_id, "agent": agent_id, "version": version_id},
    )
    lineage_columns = (
        "id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
        "pr_number, pr_url, head_sha, github_repository_id, github_installation_id, "
        "github_pr_node_id, base_ref"
    )
    sql_dicts(
        f"INSERT INTO curie.thread_publication_lineages ({lineage_columns}) VALUES "
        "(:id, :agent, :deployment, :conversation, :repo, :sha, :branch, 77, :url, :sha, "
        ":repo_id, :installation, 'PR_kwDOAbc', 'main')",
        {
            "id": lineage_id,
            "agent": agent_id,
            "deployment": deployment_id,
            "conversation": conversation,
            "repo": REPO,
            "sha": "a" * 40,
            "branch": f"curie/{lineage_id.hex[:12]}",
            "url": f"https://github.com/{REPO}/pull/77",
            "repo_id": REPOSITORY_ID,
            "installation": INSTALLATION_ID,
        },
    )
    # A lineage created before identity was captured stays without one.
    sql_dicts(
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
        "status) VALUES (:id, :agent, :deployment, 'legacy-thread', :repo, :sha, :branch, "
        "'closed')",
        {
            "id": legacy_lineage_id,
            "agent": agent_id,
            "deployment": deployment_id,
            "repo": REPO,
            "sha": "b" * 40,
            "branch": f"curie/{legacy_lineage_id.hex[:12]}",
        },
    )
    sql_dicts(
        "INSERT INTO curie.work_items (id, github_repository_id, github_issue_number, "
        "github_installation_id, agent_id, repo_full_name, conversation_id, "
        "publication_lineage_id, next_sequence) VALUES (:id, :repo_id, :number, "
        ":installation, :agent, :repo, :conversation, :lineage, 2)",
        {
            "id": work_item_id,
            "repo_id": REPOSITORY_ID,
            "number": ISSUE_NUMBER,
            "installation": INSTALLATION_ID,
            "agent": agent_id,
            "repo": REPO,
            "conversation": conversation,
            "lineage": lineage_id,
        },
    )
    sql_dicts(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, objective, requester, "
        "reply_kind, reply_address, reply_conversation_id) VALUES (:id, :item, 1, 'waiting', "
        "clock_timestamp() + interval '30 minutes', :objective, :requester, :kind, :address, "
        ":reply_conversation)",
        {
            "id": GOLDEN_LABEL_EVENT_REQUEST,
            "item": work_item_id,
            "objective": OBJECTIVE,
            "requester": REQUESTER,
            "kind": REPLY[0],
            "address": REPLY[1],
            "reply_conversation": REPLY[2],
        },
    )
    sql_dicts(
        "INSERT INTO curie.factory_terminal_notices (execution_request_id, work_item_id) "
        "VALUES (:request, :item)",
        {"request": GOLDEN_LABEL_EVENT_REQUEST, "item": work_item_id},
    )
    now = datetime.now(UTC)
    for repo, repository_id, updated in (
        (REPO, REPOSITORY_ID, now - timedelta(hours=1)),
        # The same repository after a rename: the newer cursor is the one kept.
        ("acme-corp/acme-bot-renamed", REPOSITORY_ID, now),
        # A cursor that never learned its repository id.
        ("acme-corp/never-read", None, now),
    ):
        sql_dicts(
            "INSERT INTO curie.factory_poll_cursors (repo_full_name, repository_id, "
            'etags, updated_at) VALUES (:repo, :id, \'{"issues": "W/1"}\'::jsonb, :updated)',
            {"repo": repo, "id": repository_id, "updated": updated},
        )
    notice = sql_dicts(
        "SELECT card_token FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": GOLDEN_LABEL_EVENT_REQUEST},
    )[0]
    return {
        "agent_id": agent_id,
        "binding_id": binding_id,
        "work_item_id": work_item_id,
        "lineage_id": lineage_id,
        "legacy_lineage_id": legacy_lineage_id,
        "conversation": conversation,
        "card_token": notice["card_token"],
    }


def _replay_label_notice() -> tuple[str, int, int]:
    """Replay the label notice for the seeded issue through admission.

    The intake path after verification: the notice becomes admission facts
    (`github_factory._facts`), the readmit service admits it, and the outcome is
    mapped to the webhook status. Returns that status and the row counts.
    """

    notice = FactoryNotice(
        uuid.uuid4(),
        "issues",
        "labeled",
        "admit",
        INSTALLATION_ID,
        REPOSITORY_ID,
        REPO,
        ISSUE_NUMBER,
        SENDER_ID,
        SENDER,
        label="factory",
        label_event_id=LABEL_EVENT_ID,
    )
    assert notice.request_id == GOLDEN_LABEL_EVENT_REQUEST

    async def go() -> tuple[str, int, int]:
        settings = get_settings()
        engine = create_async_engine(settings.database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                binding = await session.scalar(
                    select(AgentChannel).where(AgentChannel.address == REPO)
                )
                assert binding is not None
                facts = github_factory._facts(notice, binding, settings)
                result = await workitem_dispatch.readmit(session, facts)
                assert isinstance(result, WorkItemOutcome), result
                await session.commit()
                status = github_factory.admission_result(result, facts.request_id).status
            async with AsyncSession(engine) as session:
                items = await session.scalar(select(func.count()).select_from(WorkItem))
                requests = await session.scalar(select(func.count()).select_from(ExecutionRequest))
            return status, int(items or 0), int(requests or 0)
        finally:
            await engine.dispose()

    return asyncio.run(go())


@pytest.fixture
def factory_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.delenv("CURIE_MIGRATION_GITHUB_HOST", raising=False)
    monkeypatch.delenv("GITHUB_API_URL", raising=False)
    get_settings.cache_clear()


def test_0090_keeps_ids_backfills_the_tracker_identity_and_a_replay_dedupes(
    isolated_migration_db: IsolatedMigrationDb, factory_settings: None
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0089")
    try:
        seeded = _seed()

        command.upgrade(config, "0090")

        for table, gone in _GONE.items():
            assert not (column_names(table) & gone), table
        assert constraint_exists("work_items_tracker_issue_key")
        assert not constraint_exists("work_items_github_issue_key")

        (item,) = sql_dicts("SELECT * FROM curie.work_items")
        assert item["id"] == seeded["work_item_id"]
        assert {
            key: item[key]
            for key in (
                "tracker_kind",
                "tracker_host",
                "tracker_scope_id",
                "tracker_issue_id",
                "tracker_display_key",
                "code_host_kind",
                "code_host_host",
                "repository_project_id",
                "repository_path",
                "code_host_installation_id",
                "conversation_id",
                "publication_lineage_id",
                "next_sequence",
            )
        } == {
            "tracker_kind": GITHUB,
            "tracker_host": "github.com",
            "tracker_scope_id": str(REPOSITORY_ID),
            "tracker_issue_id": str(ISSUE_NUMBER),
            "tracker_display_key": None,
            "code_host_kind": GITHUB,
            "code_host_host": "github.com",
            "repository_project_id": str(REPOSITORY_ID),
            "repository_path": REPO,
            "code_host_installation_id": INSTALLATION_ID,
            "conversation_id": seeded["conversation"],
            "publication_lineage_id": seeded["lineage_id"],
            "next_sequence": 2,
        }
        assert sql_dicts("SELECT id, work_item_id FROM curie.execution_requests") == [
            {"id": GOLDEN_LABEL_EVENT_REQUEST, "work_item_id": seeded["work_item_id"]}
        ]
        assert sql_dicts(
            "SELECT execution_request_id, work_item_id, card_token "
            "FROM curie.factory_terminal_notices"
        ) == [
            {
                "execution_request_id": GOLDEN_LABEL_EVENT_REQUEST,
                "work_item_id": seeded["work_item_id"],
                "card_token": seeded["card_token"],
            }
        ]
        lineages = {
            row.pop("id"): row
            for row in sql_dicts(
                "SELECT id, code_host_kind, code_host_host, repository_project_id, "
                "code_host_installation_id, code_host_pr_id, base_ref, pr_number "
                "FROM curie.thread_publication_lineages"
            )
        }
        assert lineages[seeded["lineage_id"]] == {
            "code_host_kind": GITHUB,
            "code_host_host": "github.com",
            "repository_project_id": str(REPOSITORY_ID),
            "code_host_installation_id": INSTALLATION_ID,
            "code_host_pr_id": "PR_kwDOAbc",
            "base_ref": "main",
            "pr_number": 77,
        }
        assert lineages[seeded["legacy_lineage_id"]] == dict.fromkeys(
            lineages[seeded["legacy_lineage_id"]]
        )
        assert sql_dicts(
            "SELECT tracker_kind, tracker_host, tracker_scope_id, scope_path, etags "
            "FROM curie.factory_poll_cursors"
        ) == [
            {
                "tracker_kind": GITHUB,
                "tracker_host": "github.com",
                "tracker_scope_id": str(REPOSITORY_ID),
                "scope_path": "acme-corp/acme-bot-renamed",
                "etags": {"issues": "W/1"},
            }
        ]

        # The typed derivations over the migrated row are the legacy values.
        async def migrated_issue() -> Any:
            engine = create_async_engine(get_settings().database_url)
            try:
                async with AsyncSession(engine) as session:
                    row = await session.get(WorkItem, seeded["work_item_id"])
                    assert row is not None
                    return row.tracker_issue
            finally:
                await engine.dispose()

        issue = asyncio.run(migrated_issue())
        assert issue_lock_keys(issue) == GOLDEN_LOCK_KEYS
        replayed_notice = MarkedNotice(
            issue,
            "factory",
            Actor(str(SENDER_ID), SENDER),
            str(LABEL_EVENT_ID),
            Disposition.ADMIT,
            cursor="1",
        )
        assert notice_request_id(replayed_notice) == GOLDEN_LABEL_EVENT_REQUEST

        assert _replay_label_notice() == ("factory_duplicate", 1, 1)

        command.downgrade(config, "0089")

        assert sql_dicts(
            "SELECT id, github_repository_id, github_issue_number, github_installation_id, "
            "repo_full_name FROM curie.work_items"
        ) == [
            {
                "id": seeded["work_item_id"],
                "github_repository_id": REPOSITORY_ID,
                "github_issue_number": ISSUE_NUMBER,
                "github_installation_id": INSTALLATION_ID,
                "repo_full_name": REPO,
            }
        ]
        assert constraint_exists("work_items_github_issue_key")
        assert sql_dicts(
            "SELECT github_repository_id, github_pr_node_id FROM curie.thread_publication_lineages "
            "WHERE id = :id",
            {"id": seeded["lineage_id"]},
        ) == [{"github_repository_id": REPOSITORY_ID, "github_pr_node_id": "PR_kwDOAbc"}]
        assert sql_dicts(
            "SELECT repo_full_name, repository_id FROM curie.factory_poll_cursors"
        ) == [{"repo_full_name": "acme-corp/acme-bot-renamed", "repository_id": REPOSITORY_ID}]
    finally:
        command.upgrade(config, "head")


def test_0090_backfill_host_follows_the_configured_github(
    isolated_migration_db: IsolatedMigrationDb,
    factory_settings: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A GitHub Enterprise install's rows take its host, as the API derives it."""

    config = alembic_config()
    isolated_migration_db.at("0089")
    try:
        _seed()
        monkeypatch.setenv("GITHUB_API_URL", "https://ghe.example.com/api/v3")

        command.upgrade(config, "0090")

        assert sql_dicts("SELECT tracker_host, code_host_host FROM curie.work_items") == [
            {"tracker_host": "ghe.example.com", "code_host_host": "ghe.example.com"}
        ]
        assert sql_dicts("SELECT DISTINCT tracker_host FROM curie.factory_poll_cursors") == [
            {"tracker_host": "ghe.example.com"}
        ]
    finally:
        command.upgrade(config, "head")


def _insert_work_item(agent_id: uuid.UUID, **overrides: Any) -> uuid.UUID:
    """A WorkItem written at 0090 with the GitHub defaults and ``overrides``."""

    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tracker_kind": GITHUB,
        "tracker_host": "github.com",
        "tracker_scope_id": str(REPOSITORY_ID),
        "tracker_issue_id": "13",
        "tracker_display_key": None,
        "code_host_kind": GITHUB,
        "code_host_host": "github.com",
        "repository_project_id": str(REPOSITORY_ID),
        "repository_path": REPO,
        "code_host_installation_id": INSTALLATION_ID,
        "agent_id": agent_id,
        "conversation_id": f"issue-{uuid.uuid4().hex[:8]}",
    }
    values.update(overrides)
    columns = ", ".join(values)
    sql_dicts(
        f"INSERT INTO curie.work_items ({columns}) "
        f"VALUES ({', '.join(':' + key for key in values)})",
        values,
    )
    return uuid.UUID(str(values["id"]))


def test_0090_downgrade_refuses_a_work_item_from_another_tracker(
    isolated_migration_db: IsolatedMigrationDb, factory_settings: None
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0089")
    seeded = _seed()
    command.upgrade(config, "0090")
    # A Jira issue keyed by its site and numeric id, its key for display only.
    _insert_work_item(
        seeded["agent_id"],
        tracker_kind="jira_cloud",
        tracker_host="acme.atlassian.net",
        tracker_scope_id="cloud-1",
        tracker_issue_id="10012",
        tracker_display_key="PROJ-12",
    )

    with pytest.raises(RuntimeError, match="0090 downgrade refused"):
        command.downgrade(config, "0089")

    assert column_names("work_items") >= {"tracker_kind", "tracker_issue_id"}


def test_0090_trigger_keeps_the_tracker_identity_immutable(
    isolated_migration_db: IsolatedMigrationDb, factory_settings: None
) -> None:
    isolated_migration_db.at("0089")
    seeded = _seed()
    command.upgrade(alembic_config(), "0090")
    for column, value in (("tracker_issue_id", "99"), ("repository_project_id", "4402")):
        with pytest.raises(Exception, match="work item identity is immutable"):
            sql_dicts(
                f"UPDATE curie.work_items SET {column} = :value WHERE id = :id",
                {"value": value, "id": seeded["work_item_id"]},
            )
    # The display key is not identity: a moved Jira issue changes it.
    sql_dicts(
        "UPDATE curie.work_items SET tracker_display_key = 'PROJ-12' WHERE id = :id",
        {"id": seeded["work_item_id"]},
    )


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("tracker_issue_id", "012"),
        ("tracker_scope_id", "repo"),
        ("repository_project_id", "0"),
        ("code_host_installation_id", None),
        ("tracker_host", " "),
    ],
)
def test_0090_checks_refuse_a_github_identity_the_derivations_cannot_read(
    isolated_migration_db: IsolatedMigrationDb,
    factory_settings: None,
    column: str,
    value: str | None,
) -> None:
    isolated_migration_db.at("0089")
    seeded = _seed()
    command.upgrade(alembic_config(), "0090")
    with pytest.raises(Exception, match="work_items_(github_identity|identity_text)_ck"):
        _insert_work_item(seeded["agent_id"], **{column: value})


def test_0090_lineage_check_refuses_a_half_set_code_host_identity(
    isolated_migration_db: IsolatedMigrationDb, factory_settings: None
) -> None:
    isolated_migration_db.at("0089")
    seeded = _seed()
    command.upgrade(alembic_config(), "0090")
    with pytest.raises(Exception, match="thread_publication_lineages_code_host_identity_ck"):
        sql_dicts(
            "UPDATE curie.thread_publication_lineages SET code_host_host = NULL WHERE id = :id",
            {"id": seeded["lineage_id"]},
        )
