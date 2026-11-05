"""The 0149 join converges the next (0146) and main (0147) trains, #5421 train convergence."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_dicts, sql_rows
from alembic import command
from alembic.script import ScriptDirectory
from curie_api.schema_compat import current_revision

ROOT = Path(__file__).resolve().parents[3]
CANDIDATE = {"schema_min": "0149", "schema_head": "0149"}
EVIDENCE_TABLES = {
    "publication_ui_evidence",
    "publication_ui_evidence_images",
    "publication_lineage_ui_evidence",
}


def _load(relative: str) -> Any:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def test_graph_and_every_schema_consumer_agree_on_the_0149_join() -> None:
    graph = ScriptDirectory.from_config(alembic_config())
    assert graph.get_heads() == ["0149"]
    join = graph.get_revision("0149")
    assert join is not None and set(join.down_revision) == {"0146", "0147"}
    for side in ("0146", "0147"):
        revision = graph.get_revision(side)
        assert revision is not None and revision.down_revision == "0145", side

    kinds = _load("apps/api/src/curie_api/revision_kinds.json")
    assert {revision: kinds[revision] for revision in ("0146", "0147", "0149")} == {
        "0146": "expand",
        "0147": "expand",
        "0149": "expand",
    }

    catalog = _load("cli/src/application_schema_windows.json")
    serving = _load("packages/protected-hooks/src/curie_protected_hooks/schema_serving.json")
    chart = _load("charts/curie/files/schema-compat.json")
    assert _load("apps/api/src/curie_api/schema_compat.json") == CANDIDATE
    assert catalog["candidate"] == CANDIDATE
    assert {key: chart[key] for key in CANDIDATE} == CANDIDATE
    assert {key: serving[key] for key in CANDIDATE} == CANDIDATE
    assert sorted(serving["revision_parents"]["0149"]) == ["0146", "0147"]
    assert catalog["revisions"][-3:] == ["0146", "0147", "0149"]
    chart_rows = [row for row in chart["revisions"] if row["revision"] == "0149"]
    assert len(chart_rows) == 1
    assert chart_rows[0]["parents"] == ["0146", "0147"]


def _cancel_deleted_issue() -> uuid.UUID:
    """Cancel a fresh execution request with cause issue_deleted, as main's 0147 test does."""
    from curie_api.workitems import lifecycle
    from test_workitems import _agent, _item, _request, with_session

    async def cancel(session: Any) -> uuid.UUID:
        agent = await _agent(session)
        waiting = await _request(session, (await _item(session, agent)).work_item)
        cancelled = await lifecycle.request_cancellation(
            session,
            work_item_id=waiting.work_item.id,
            expected_work_item_version=waiting.work_item.version,
            cause="issue_deleted",
        )
        assert isinstance(cancelled, lifecycle.WorkItemOutcome)
        assert cancelled.request.terminal_cause == "issue_deleted"
        return cancelled.request.id

    return with_session(cancel)


def _seed_ui_evidence(agent_id: uuid.UUID) -> dict[str, uuid.UUID]:
    """Raw SQL rows in all three 0146 evidence tables, hung off a real publication."""
    ids = {name: uuid.uuid4() for name in ("version", "deployment", "lineage", "approval", "pub")}
    statements = [
        "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
        "VALUES (:version, :agent, 'v1', 'fixture')",
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:deployment, :agent, :version, CAST('dev' AS curie.environment), 'active')",
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
        "status, version, latest_revision) VALUES (:lineage, :agent, :deployment, "
        "'conv-converge', 'acme-corp/acme-bot', :base, :branch, 'open', 1, 1)",
        "INSERT INTO curie.approvals (id, agent_id, conversation_id, author, summary, "
        "reply_kind, reply_channel, dedupe_key, status, purpose) VALUES (:approval, :agent, "
        "'conv-converge', 'U0REQUEST1', 'Publish repository changes', 'github', "
        "'acme-corp/acme-bot', :dedupe, 'approved', 'publication')",
        "INSERT INTO curie.publications (id, approval_id, deployment_id, lineage_id, "
        "revision_number, repo_full_name, status, base_sha, changed_paths, title, body, "
        "reply_kind, reply_channel, terminal_at) VALUES (:pub, :approval, :deployment, "
        ":lineage, 1, 'acme-corp/acme-bot', 'succeeded', :base, "
        "CAST('[\"README.md\"]' AS jsonb), 'Update README', 'Approved publication.', "
        "'github', 'acme-corp/acme-bot', clock_timestamp())",
        "INSERT INTO curie.publication_ui_evidence (publication_id, lineage_id, status, "
        "index_sha256, manifest_state, base_sha, upload_deadline_at) VALUES (:pub, "
        ":lineage, 'published', :index, 'valid', :base, clock_timestamp())",
        "INSERT INTO curie.publication_ui_evidence_images (publication_id, ordinal, path, "
        "label, state, bound) VALUES (:pub, 0, 'ui/after.png', 'After', 'after', true)",
        "INSERT INTO curie.publication_lineage_ui_evidence (lineage_id, comment_revision, "
        "comment_kind) VALUES (:lineage, 1, 'evidence')",
    ]
    params = {
        **ids,
        "agent": agent_id,
        "base": "a" * 40,
        "index": "b" * 64,
        "branch": f"curie/publication-{ids['lineage'].hex}",
        "dedupe": f"converge-{ids['pub'].hex}",
    }
    for statement in statements:
        sql_rows(statement, params)
    return ids


def _state_shape() -> str:
    rows = sql_rows(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = 'curie.execution_requests'::regclass "
        "AND conname = 'execution_requests_state_shape_ck'"
    )
    assert len(rows) == 1, rows
    return str(rows[0][0])


@pytest.mark.parametrize("source", ["0146", "0147", "0145"])
def test_upgrade_from_each_train_head_reaches_the_join_with_rows_intact(
    isolated_migration_db: IsolatedMigrationDb, source: str
) -> None:
    isolated_migration_db.at(source)
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, 'acme-converge')",
        {"id": agent_id},
    )
    evidence = _seed_ui_evidence(agent_id) if source == "0146" else None
    deleted_request = _cancel_deleted_issue() if source == "0147" else None

    command.upgrade(alembic_config(), "head")

    assert current_revision() == "0149"
    assert sql_dicts("SELECT name FROM curie.agents WHERE id = :id", {"id": agent_id}) == [
        {"name": "acme-converge"}
    ]
    tables = {
        row["table_name"]
        for row in sql_dicts(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'"
        )
    }
    assert EVIDENCE_TABLES <= tables
    assert "'issue_deleted'" in _state_shape()
    if evidence is not None:
        assert sql_dicts(
            "SELECT lineage_id, status FROM curie.publication_ui_evidence "
            "WHERE publication_id = :pub",
            {"pub": evidence["pub"]},
        ) == [{"lineage_id": evidence["lineage"], "status": "published"}]
        assert sql_dicts(
            "SELECT ordinal, label, state FROM curie.publication_ui_evidence_images "
            "WHERE publication_id = :pub",
            {"pub": evidence["pub"]},
        ) == [{"ordinal": 0, "label": "After", "state": "after"}]
        assert sql_dicts(
            "SELECT comment_revision, comment_kind FROM curie.publication_lineage_ui_evidence "
            "WHERE lineage_id = :lineage",
            {"lineage": evidence["lineage"]},
        ) == [{"comment_revision": 1, "comment_kind": "evidence"}]
    if deleted_request is not None:
        assert sql_dicts(
            "SELECT id, terminal_cause FROM curie.execution_requests WHERE id = :id",
            {"id": deleted_request},
        ) == [{"id": deleted_request, "terminal_cause": "issue_deleted"}]


def test_next_side_accepts_a_deleted_issue_cancellation_after_the_join(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0146")
    assert "'issue_deleted'" not in _state_shape()

    command.upgrade(alembic_config(), "head")

    assert current_revision() == "0149"
    request_id = _cancel_deleted_issue()
    assert sql_dicts(
        "SELECT terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    ) == [{"terminal_cause": "issue_deleted"}]
