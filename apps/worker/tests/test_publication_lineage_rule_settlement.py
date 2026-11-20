"""The worker's terminal lineage write settles the lineage's rule candidates (#5796).

``PostgresPublicationStore.mark_lineage_terminal`` records a pull request the
publication path found merged or closed. ADR 0287 decision 5 settles the
lineage's learned rule candidates in that same transaction: a merge promotes
the newest to ``proposed`` with the next number in its scope and supersedes the
rest, a close discards them all. Each case runs against the real API schema,
migrated into a disposable database, with the rows a factory run leaves seeded
by SQL as the store's other durable tests seed them.
"""

from __future__ import annotations

import hashlib
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_worker.publication_store import PostgresPublicationStore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_publication_loop import (  # noqa: E402
    AGENT_ID,
    APPROVAL_ID,
    CONVERSATION_ID,
    LINEAGE_BRANCH,
    LINEAGE_ID,
    PR_URL,
    PRIOR_HEAD,
    PUBLICATION_ID,
    REVISION_HEAD,
    migrated_publication_database_url,  # noqa: F401  (fixture)
)

pytestmark = pytest.mark.anyio

LEASE_OWNER = "rule-settlement-test"
SCOPE_KEY = "github:github.com:101"
REPOSITORY = "acme-corp/acme-bot"
REVIEWER = "github:4242:reviewer"
OLDER_RULE = "Use the shared parse_timestamp helper for every time flag."
NEWER_RULE = "Every time flag goes through parse_timestamp in cli/src/time.rs."


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def _seed(engine: AsyncEngine) -> dict[str, uuid.UUID]:
    """An open factory pull request with a running revision publication, two
    rule candidates from two revision requests, and one rule of the same
    repository scope already proposed as number 3 by an earlier pull request."""

    ids = {
        "version": uuid.uuid4(),
        "deployment": uuid.uuid4(),
        "work_item": uuid.uuid4(),
        "older_request": uuid.uuid4(),
        "newer_request": uuid.uuid4(),
        "older_rule": uuid.uuid4(),
        "newer_rule": uuid.uuid4(),
        "earlier_rule": uuid.uuid4(),
    }
    async with engine.begin() as connection:

        async def run(statement: str, params: dict[str, Any]) -> None:
            await connection.execute(text(statement), params)

        await run(
            "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
            {"id": AGENT_ID, "name": "acme-rule-settlement"},
        )
        await run(
            "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
            "VALUES (:id, :agent, 'v1', 'publication-test')",
            {"id": ids["version"], "agent": AGENT_ID},
        )
        await run(
            "INSERT INTO curie.deployments (id, agent_id, version_id, environment) "
            "VALUES (:id, :agent, :version, 'dev')",
            {"id": ids["deployment"], "agent": AGENT_ID, "version": ids["version"]},
        )
        await run(
            "INSERT INTO curie.thread_publication_lineages "
            "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
            "pr_number, pr_url, head_sha, status, version, latest_revision) VALUES "
            "(:id, :agent, :deployment, :conversation, :repo, :head, :branch, 123, :url, "
            ":head, 'open', 2, 2)",
            {
                "id": LINEAGE_ID,
                "agent": AGENT_ID,
                "deployment": ids["deployment"],
                "conversation": CONVERSATION_ID,
                "repo": REPOSITORY,
                "head": PRIOR_HEAD,
                "branch": LINEAGE_BRANCH,
                "url": PR_URL,
            },
        )
        await run(
            "INSERT INTO curie.work_items "
            "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
            "code_host_kind, code_host_host, repository_project_id, repository_path, "
            "code_host_installation_id, agent_id, conversation_id, version, next_sequence, "
            "base_branch, base_source, base_commit, publication_lineage_id) VALUES "
            "(:id, 'github', 'github.com', '101', '77', 'github', 'github.com', '101', "
            ":repo, 202, :agent, :conversation, 3, 3, 'main', 'default', :base, :lineage)",
            {
                "id": ids["work_item"],
                "repo": REPOSITORY,
                "agent": AGENT_ID,
                "conversation": f"slack:C0EXAMPLE1:{CONVERSATION_ID}",
                "base": PRIOR_HEAD,
                "lineage": LINEAGE_ID,
            },
        )
        for sequence, key in ((1, "older_request"), (2, "newer_request")):
            await run(
                "INSERT INTO curie.execution_requests "
                "(id, work_item_id, sequence, status, wait_deadline, started_at, "
                "execution_deadline, terminal_at, terminal_cause, version, runtime_owner, "
                "runtime_epoch, runtime_heartbeat_expires_at, dispatch_generation, "
                "execution_attempts) VALUES "
                "(:id, :item, :sequence, 'failed', clock_timestamp() + interval '1 hour', "
                "clock_timestamp() - interval '1 minute', "
                "clock_timestamp() + interval '1 hour', "
                "clock_timestamp() - interval '1 second', 'runner_failed', 4, "
                "'fixture-worker', 1, clock_timestamp() + interval '1 hour', 1, 1)",
                {"id": ids[key], "item": ids["work_item"], "sequence": sequence},
            )
        await run(
            "INSERT INTO curie.approvals "
            "(id, agent_id, conversation_id, author, summary, dedupe_key, "
            "reply_kind, reply_channel, status) VALUES "
            "(:id, :agent, :conversation, 'fixture-requester', 'Publish approved change', "
            "'rule-settlement', 'slack', 'C0EXAMPLE1', 'approved')",
            {"id": APPROVAL_ID, "agent": AGENT_ID, "conversation": CONVERSATION_ID},
        )
        await run(
            "INSERT INTO curie.publications "
            "(id, approval_id, deployment_id, lineage_id, repo_full_name, status, version, "
            "revision_number, base_sha, expected_prior_head, changed_paths, title, body, "
            "reply_kind, reply_channel, lease_owner, lease_expires_at) VALUES "
            "(:id, :approval, :deployment, :lineage, :repo, 'running', 1, "
            "2, :head, :head, '[\"README.md\"]'::jsonb, 'Update repository', "
            "'Approved change', 'slack', 'C0EXAMPLE1', :owner, now() + interval '1 hour')",
            {
                "id": PUBLICATION_ID,
                "approval": APPROVAL_ID,
                "deployment": ids["deployment"],
                "lineage": LINEAGE_ID,
                "repo": REPOSITORY,
                "head": PRIOR_HEAD,
                "owner": LEASE_OWNER,
            },
        )
        rules = (
            ("earlier_rule", "Name test helpers after behavior.", "proposed", 3, None, None, 10),
            ("older_rule", OLDER_RULE, "candidate", None, "older_request", LINEAGE_ID, 2),
            ("newer_rule", NEWER_RULE, "candidate", None, "newer_request", LINEAGE_ID, 1),
        )
        for key, rule_text, state, number, request, lineage, minutes_ago in rules:
            await run(
                "INSERT INTO curie.memory_rules "
                "(id, tenant_id, agent_id, scope_kind, scope_key, scope_label, number, state, "
                "text, sha256, format_version, reason, proposed_by, proposed_at, "
                "source_request_id, source_lineage_id, source_url) VALUES "
                "(:id, (SELECT tenant_id FROM curie.agents WHERE id = :agent), :agent, "
                "'repository', :scope, :repo, :number, :state, :text, :sha256, 1, "
                "'The reviewer stated it for every flag.', :reviewer, "
                "clock_timestamp() - make_interval(mins => CAST(:minutes AS integer)), "
                ":request, :lineage, :url)",
                {
                    "id": ids[key],
                    "scope": SCOPE_KEY,
                    "repo": REPOSITORY,
                    "number": number,
                    "state": state,
                    "text": rule_text,
                    "sha256": hashlib.sha256(rule_text.encode()).hexdigest(),
                    "reviewer": REVIEWER,
                    "minutes": minutes_ago,
                    "request": ids[request] if request else None,
                    "lineage": lineage,
                    "url": f"{PR_URL}#issuecomment-{minutes_ago}" if lineage else None,
                    "agent": AGENT_ID,
                },
            )
            await run(
                "INSERT INTO curie.memory_rule_events "
                "(rule_id, tenant_id, agent_id, event, from_state, to_state, actor_kind, actor) "
                "VALUES (:id, (SELECT tenant_id FROM curie.agents WHERE id = :agent), :agent, "
                "'propose', NULL, :state, 'agent', :actor)",
                {"id": ids[key], "state": state, "actor": str(AGENT_ID), "agent": AGENT_ID},
            )
    return ids


async def _rules(engine: AsyncEngine) -> dict[uuid.UUID, dict[str, Any]]:
    async with engine.connect() as connection:
        rows = (await connection.execute(text("SELECT * FROM curie.memory_rules"))).mappings()
        return {row["id"]: dict(row) for row in rows}


async def _events(engine: AsyncEngine) -> list[tuple[Any, ...]]:
    async with engine.connect() as connection:
        rows = await connection.execute(
            text(
                "SELECT rule_id, event, from_state, to_state, actor_kind, actor "
                "FROM curie.memory_rule_events ORDER BY id"
            )
        )
        return [tuple(row) for row in rows]


async def _mark_terminal(store: PostgresPublicationStore, state: str) -> None:
    await store.mark_lineage_terminal(
        LINEAGE_ID,
        expected_version=2,
        expected_stored_head=PRIOR_HEAD,
        state=state,
        pr_number=123,
        pr_url=PR_URL,
        head_sha=REVISION_HEAD,
        publication_id=PUBLICATION_ID,
        expected_publication_version=1,
        lease_owner=LEASE_OWNER,
        first_parent_sha=None,
    )


@pytest.mark.parametrize("state", ["merged", "closed"])
async def test_terminal_write_settles_the_lineage_rule_candidates(
    migrated_publication_database_url: str,  # noqa: F811
    state: str,
) -> None:
    engine = create_async_engine(migrated_publication_database_url)
    try:
        ids = await _seed(engine)
        store = PostgresPublicationStore(engine, schema="curie", lease_owner=LEASE_OWNER)
        before = await _rules(engine)
        events_before = await _events(engine)

        await _mark_terminal(store, state)

        async with engine.connect() as connection:
            status = (
                await connection.execute(
                    text("SELECT status FROM curie.thread_publication_lineages WHERE id = :id"),
                    {"id": LINEAGE_ID},
                )
            ).scalar_one()
        assert status == state
        after = await _rules(engine)
        older, newer = after[ids["older_rule"]], after[ids["newer_rule"]]
        assert after[ids["earlier_rule"]] == before[ids["earlier_rule"]]
        if state == "merged":
            assert (newer["state"], newer["number"], newer["version"]) == ("proposed", 4, 2)
            assert (older["state"], older["number"], older["version"]) == (
                "superseded",
                None,
                2,
            )
            settled = [
                (ids["older_rule"], "supersede", "candidate", "superseded"),
                (ids["newer_rule"], "promote", "candidate", "proposed"),
            ]
        else:
            assert (newer["state"], newer["number"], newer["version"]) == ("discarded", None, 2)
            assert (older["state"], older["number"], older["version"]) == ("discarded", None, 2)
            settled = [
                (ids["newer_rule"], "discard", "candidate", "discarded"),
                (ids["older_rule"], "discard", "candidate", "discarded"),
            ]
        changed = ("state", "number", "version", "updated_at")
        for row in (older, newer):
            prior = before[row["id"]]
            assert row["updated_at"] > prior["updated_at"]
            assert {k: v for k, v in row.items() if k not in changed} == {
                k: v for k, v in prior.items() if k not in changed
            }
        assert await _events(engine) == [
            *events_before,
            *(
                (rule_id, event, from_state, to_state, "platform", "platform")
                for rule_id, event, from_state, to_state in settled
            ),
        ]

        # The identical replay of the terminal fact changes nothing.
        settled_rules, settled_events = after, await _events(engine)
        await _mark_terminal(store, state)
        assert await _rules(engine) == settled_rules
        assert await _events(engine) == settled_events
    finally:
        await engine.dispose()
