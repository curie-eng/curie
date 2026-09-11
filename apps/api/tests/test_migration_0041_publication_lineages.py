"""Migration 0041 gives resumable publications one durable thread lineage."""

from __future__ import annotations

import asyncio
import base64
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from curie_api.deps import get_session
from curie_api.main import create_app
from curie_api.models import Publication, ThreadPublicationLineage
from fastapi.testclient import TestClient
from sqlalchemy import CheckConstraint, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"
BELOW = "0040"
BASE_SHA = "0123456789abcdef0123456789abcdef01234567"
REPO = "acme-corp/acme-bot"
REPLY_KIND = "slack"
REPLY_CHANNEL = "C0EXAMPLE1"


def _config() -> Config:
    config = Config()
    config.set_main_option("script_location", str(ALEMBIC_DIR))
    return config


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                result = await connection.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _scoped_thread(conversation_id: str) -> str:
    return ":".join(
        quote(part, safe="")
        for part in (REPLY_KIND, REPLY_CHANNEL, conversation_id)
    )


@contextmanager
def _api_client() -> Iterator[TestClient]:
    """Drive the real publication router without starting unrelated resources."""

    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def isolated_session() -> AsyncIterator[AsyncSession]:
        async with sessions() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_session] = isolated_session
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
        asyncio.run(engine.dispose())


def _seed_deployment() -> tuple[uuid.UUID, uuid.UUID]:
    agent_id = uuid.uuid4()
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"lineage-migration-{agent_id.hex[:8]}"},
    )
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'v1', NULL, 'migration-test')",
        {"id": version_id, "agent_id": agent_id},
    )
    _sql(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, "
        "CAST('dev' AS curie.environment), 'active')",
        {
            "id": deployment_id,
            "agent_id": agent_id,
            "version_id": version_id,
        },
    )
    return agent_id, deployment_id


def _seed_redeployment(agent_id: uuid.UUID) -> uuid.UUID:
    version_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agent_versions "
        "(id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent_id, 'v2', NULL, 'migration-test')",
        {"id": version_id, "agent_id": agent_id},
    )
    _sql(
        "INSERT INTO curie.deployments "
        "(id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent_id, :version_id, "
        "CAST('dev' AS curie.environment), 'active')",
        {
            "id": deployment_id,
            "agent_id": agent_id,
            "version_id": version_id,
        },
    )
    return deployment_id


def _seed_publication(
    *,
    agent_id: uuid.UUID,
    deployment_id: uuid.UUID,
    conversation_id: str,
    workspace_conversation_id: str | None = None,
    status: str,
    result_url: str | None = None,
    created_at: datetime | None = None,
) -> tuple[uuid.UUID, uuid.UUID]:
    approval_id = uuid.uuid4()
    publication_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.approvals "
        "(id, agent_id, conversation_id, author, summary, reply_kind, "
        "reply_channel, dedupe_key, status, purpose) VALUES "
        "(:id, :agent_id, :conversation_id, 'U0REQUEST1', "
        "'Publish repository changes', 'slack', 'C0EXAMPLE1', :dedupe_key, "
        ":approval_status, 'publication')",
        {
            "id": approval_id,
            "agent_id": agent_id,
            "conversation_id": conversation_id,
            "dedupe_key": f"migration-lineage-{publication_id.hex}",
            "approval_status": "approved" if status != "pending" else "pending",
        },
    )
    _sql(
        "INSERT INTO curie.publications "
        "(id, approval_id, deployment_id, workspace_conversation_id, "
        "repo_full_name, status, base_sha, "
        "patch_bytes, changed_paths, title, body, reply_kind, reply_channel, "
        "result_url, created_at, updated_at) VALUES "
        "(:id, :approval_id, :deployment_id, :workspace_conversation_id, "
        "'acme-corp/acme-bot', :status, "
        ":base_sha, :patch_bytes, CAST('[\"README.md\"]' AS jsonb), "
        "'Update README', 'Approved platform publication.', 'slack', "
        "'C0EXAMPLE1', :result_url, "
        "COALESCE(CAST(:created_at AS timestamp), now()), "
        "COALESCE(CAST(:created_at AS timestamp), now()))",
        {
            "id": publication_id,
            "approval_id": approval_id,
            "deployment_id": deployment_id,
            "workspace_conversation_id": workspace_conversation_id,
            "status": status,
            "base_sha": BASE_SHA,
            "patch_bytes": (
                b"migration-private-patch"
                if status in ("pending", "approved", "launching", "running")
                else None
            ),
            "result_url": (
                None
                if status in ("pending", "approved", "launching", "running")
                else result_url
                or "https://github.com/acme-corp/acme-bot/pull/123"
            ),
            "created_at": created_at,
        },
    )
    return approval_id, publication_id


def _seed_thread_workspace(
    *,
    agent_id: uuid.UUID,
    deployment_id: uuid.UUID,
    conversation_id: str,
) -> None:
    _sql(
        "INSERT INTO curie.thread_workspaces "
        "(id, agent_id, selected_by_deployment_id, conversation_id, "
        "repo_full_name, selected_by) VALUES "
        "(:id, :agent_id, :deployment_id, :conversation_id, :repo, 'U0REQUEST1')",
        {
            "id": uuid.uuid4(),
            "agent_id": agent_id,
            "deployment_id": deployment_id,
            "conversation_id": conversation_id,
            "repo": REPO,
        },
    )


def test_thread_publication_lineage_orm_metadata_mirrors_0041_checks() -> None:
    """Declarative test schemas must enforce the same lineage invariants."""

    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in ThreadPublicationLineage.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert (
        checks["thread_publication_lineages_status_ck"]
        == "status IN ('open', 'merged', 'closed')"
    )
    assert checks["thread_publication_lineages_version_ck"] == "version >= 1"
    assert (
        checks["thread_publication_lineages_latest_revision_ck"]
        == "latest_revision >= 1"
    )
    assert (
        checks["thread_publication_lineages_pr_identity_ck"]
        == "(pr_number IS NULL) = (pr_url IS NULL)"
    )


def test_publication_orm_metadata_requires_lineage_for_active_statuses() -> None:
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in Publication.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }

    assert (
        checks["publications_active_lineage_ck"]
        == "status NOT IN ('pending', 'approved', 'launching', 'running') "
        "OR lineage_id IS NOT NULL"
    )


def test_0041_contract_rejects_n_minus_one_active_write_but_keeps_terminal_history(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    agent_id, deployment_id = _seed_deployment()
    _, terminal_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-terminal-contract-history",
        status="failed",
    )

    command.upgrade(config, "head")

    assert _sql(
        "SELECT status, lineage_id FROM curie.publications WHERE id = :id",
        {"id": terminal_id},
    ) == [{"status": "failed", "lineage_id": None}]

    with pytest.raises(IntegrityError) as excinfo:
        # This is the 0040 writer shape: it starts pending and knows none of
        # 0041's lineage columns.
        _seed_publication(
            agent_id=agent_id,
            deployment_id=deployment_id,
            conversation_id="thread-post-contract-n-minus-one",
            status="pending",
        )
    assert "publications_active_lineage_ck" in str(excinfo.value)
    assert _sql(
        "SELECT p.id FROM curie.publications p JOIN curie.approvals a "
        "ON a.id = p.approval_id WHERE a.conversation_id = :conversation_id",
        {"conversation_id": "thread-post-contract-n-minus-one"},
    ) == []


def test_0041_collapses_duplicate_preupgrade_thread_publications(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    agent_id, deployment_id = _seed_deployment()
    redeployment_id = _seed_redeployment(agent_id)

    _, older_succeeded_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-duplicate-succeeded",
        status="succeeded",
        result_url="https://github.com/acme-corp/acme-bot/pull/121",
        created_at=datetime(2026, 1, 1),
    )
    _, newer_succeeded_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-duplicate-succeeded",
        workspace_conversation_id=_scoped_thread("thread-duplicate-succeeded"),
        status="succeeded",
        result_url="https://github.com/acme-corp/acme-bot/pull/122",
        created_at=datetime(2026, 1, 2),
    )

    older_active_approval_id, older_active_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-active-precedence",
        status="pending",
        created_at=datetime(2026, 2, 1),
    )
    _, newer_active_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=redeployment_id,
        conversation_id="thread-active-precedence",
        workspace_conversation_id=_scoped_thread("thread-active-precedence"),
        status="running",
        created_at=datetime(2026, 2, 2),
    )
    _, newest_succeeded_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-active-precedence",
        status="succeeded",
        result_url="https://github.com/acme-corp/acme-bot/pull/203",
        created_at=datetime(2026, 2, 3),
    )

    command.upgrade(config, "head")

    assert _sql(
        "SELECT p.id::text, p.lineage_id::text, p.revision_number, l.pr_number "
        "FROM curie.publications p LEFT JOIN curie.thread_publication_lineages l "
        "ON l.id = p.lineage_id WHERE p.id IN (:older, :newer) ORDER BY p.created_at",
        {"older": older_succeeded_id, "newer": newer_succeeded_id},
    ) == [
        {
            "id": str(older_succeeded_id),
            "lineage_id": None,
            "revision_number": None,
            "pr_number": None,
        },
        {
            "id": str(newer_succeeded_id),
            "lineage_id": str(newer_succeeded_id),
            "revision_number": 1,
            "pr_number": 122,
        },
    ]

    active_rows = _sql(
        "SELECT p.id::text, p.status, p.lineage_id::text, p.revision_number, p.error, "
        "p.patch_bytes IS NULL AS patch_cleared, "
        "p.terminal_at IS NOT NULL AS terminal, a.status AS approval_status "
        "FROM curie.publications p JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.id IN (:older_active, :newer_active, :succeeded) ORDER BY p.created_at",
        {
            "older_active": older_active_id,
            "newer_active": newer_active_id,
            "succeeded": newest_succeeded_id,
        },
    )
    assert active_rows == [
        {
            "id": str(older_active_id),
            "status": "failed",
            "lineage_id": None,
            "revision_number": None,
            "error": "superseded by another publication during lineage migration",
            "patch_cleared": True,
            "terminal": True,
            "approval_status": "expired",
        },
        {
            "id": str(newer_active_id),
            "status": "running",
            "lineage_id": str(newer_active_id),
            "revision_number": 1,
            "error": None,
            "patch_cleared": False,
            "terminal": False,
            "approval_status": "approved",
        },
        {
            "id": str(newest_succeeded_id),
            "status": "succeeded",
            "lineage_id": None,
            "revision_number": None,
            "error": None,
            "patch_cleared": True,
            "terminal": False,
            "approval_status": "approved",
        },
    ]
    assert _sql(
        "SELECT resolved_at IS NOT NULL AS resolved, resumed_at IS NOT NULL AS resumed "
        "FROM curie.approvals WHERE id = :id",
        {"id": older_active_approval_id},
    ) == [{"resolved": True, "resumed": True}]

    assert _sql(
        "SELECT conversation_id FROM curie.thread_publication_lineages "
        "ORDER BY conversation_id"
    ) == [
        {"conversation_id": _scoped_thread("thread-active-precedence")},
        {"conversation_id": _scoped_thread("thread-duplicate-succeeded")},
    ]


def test_0041_never_infers_opaque_legacy_reply_id_is_already_scoped(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    agent_id, deployment_id = _seed_deployment()
    native_reply_id = "thread-x"
    scoped_lookalike_reply_id = _scoped_thread(native_reply_id)
    _, native_publication_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id=native_reply_id,
        status="succeeded",
        result_url="https://github.com/acme-corp/acme-bot/pull/130",
    )
    _, lookalike_publication_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id=scoped_lookalike_reply_id,
        status="succeeded",
        result_url="https://github.com/acme-corp/acme-bot/pull/131",
    )

    command.upgrade(config, "head")

    expected_identities = {
        str(native_publication_id): (
            native_reply_id,
            _scoped_thread(native_reply_id),
        ),
        str(lookalike_publication_id): (
            scoped_lookalike_reply_id,
            _scoped_thread(scoped_lookalike_reply_id),
        ),
    }
    rows = _sql(
        "SELECT p.id::text, p.lineage_id::text, p.workspace_conversation_id, "
        "a.conversation_id AS reply_conversation_id, l.conversation_id "
        "FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
        "WHERE p.id IN (:native_id, :lookalike_id) ORDER BY p.id",
        {
            "native_id": native_publication_id,
            "lookalike_id": lookalike_publication_id,
        },
    )
    assert len(rows) == 2
    for row in rows:
        reply_id, canonical_id = expected_identities[row["id"]]
        assert row == {
            "id": row["id"],
            "lineage_id": row["id"],
            "workspace_conversation_id": canonical_id,
            "reply_conversation_id": reply_id,
            "conversation_id": canonical_id,
        }

    _sql(
        "UPDATE curie.publications SET "
        "approval_card_delivery_dead_lettered_at = now(), "
        "resource_cleanup_completed_at = now() "
        "WHERE id IN (:native_id, :lookalike_id)",
        {
            "native_id": native_publication_id,
            "lookalike_id": lookalike_publication_id,
        },
    )

    async def migrated_result(publication_id: uuid.UUID) -> Any:
        from curie_worker.publication_store import PostgresPublicationStore

        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner=f"opaque-{publication_id.hex}"
        )
        try:
            return await store.pending_result(publication_id)
        finally:
            await engine.dispose()

    for publication_id in (native_publication_id, lookalike_publication_id):
        result = asyncio.run(migrated_result(publication_id))
        assert result is not None
        reply_id, canonical_id = expected_identities[str(publication_id)]
        assert result.target.conversation_id == reply_id
        assert result.workspace_conversation_id == canonical_id


def test_0041_scopes_migrated_lineage_but_keeps_reply_identity_bare(
    isolated_migration_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    agent_id, deployment_id = _seed_deployment()
    reply_thread = "1700000000.000100"
    scoped_thread = _scoped_thread(reply_thread)
    _seed_thread_workspace(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id=scoped_thread,
    )
    approval_id, publication_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id=reply_thread,
        workspace_conversation_id=None,
        status="pending",
    )

    command.upgrade(config, "head")

    assert _sql(
        "SELECT l.id::text AS lineage_id, l.conversation_id, "
        "p.workspace_conversation_id, "
        "a.conversation_id AS reply_conversation_id "
        "FROM curie.publications p "
        "JOIN curie.thread_publication_lineages l ON l.id = p.lineage_id "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.id = :id",
        {"id": publication_id},
    ) == [
        {
            "lineage_id": str(publication_id),
            "conversation_id": scoped_thread,
            "workspace_conversation_id": scoped_thread,
            "reply_conversation_id": reply_thread,
        }
    ]

    # Credential authorization must use the canonical migration snapshot, not
    # the Approval's intentionally bare Slack reply id.
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("GITHUB_TOKEN", "migration-publication-token")
    get_settings.cache_clear()
    worker_headers = {
        "X-Curie-Worker-Token": get_settings().internal_worker_token,
    }
    _sql(
        "UPDATE curie.approvals SET status = 'approved', resolved_at = now() "
        "WHERE id = :id",
        {"id": approval_id},
    )
    _sql(
        "UPDATE curie.publications SET status = 'approved' WHERE id = :id",
        {"id": publication_id},
    )
    with _api_client() as client:
        credential = client.post(
            f"/v1/internal/publications/{publication_id}/credential",
            headers=worker_headers,
        )
        assert credential.status_code == 200, credential.text

    # Settle the migrated revision without changing either identity. The result
    # outbox must append to canonical transcript history while still addressing
    # the adapter reply with its bare Slack thread timestamp.
    _sql(
        "UPDATE curie.publications SET status = 'denied', patch_bytes = NULL, "
        "approval_card_delivery_dead_lettered_at = now(), terminal_at = now() "
        "WHERE id = :id",
        {"id": publication_id},
    )
    _sql(
        "UPDATE curie.approvals SET status = 'denied', resolved_at = now() WHERE id = :id",
        {"id": approval_id},
    )

    async def migrated_result() -> Any:
        from curie_worker.publication_store import PostgresPublicationStore

        engine = create_async_engine(get_settings().database_url)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner="migration-scoped-lineage"
        )
        try:
            return await store.pending_result(publication_id)
        finally:
            await engine.dispose()

    result = asyncio.run(migrated_result())
    assert result is not None
    assert result.workspace_conversation_id == scoped_thread
    assert result.target.conversation_id == reply_thread
    _sql(
        "UPDATE curie.publications SET outcome_history_ready_at = now(), "
        "lease_owner = NULL, lease_expires_at = NULL WHERE id = :id",
        {"id": publication_id},
    )

    with _api_client() as client:
        lineage = client.get(
            "/v1/internal/publications/lineage",
            params={
                "deployment_id": str(deployment_id),
                "conversation_id": scoped_thread,
                "repo_full_name": REPO,
            },
            headers=worker_headers,
        )
        assert lineage.status_code == 200, lineage.text
        assert lineage.json()["id"] == str(publication_id)

        revision = client.post(
            "/v1/internal/publications",
            json={
                "deployment_id": str(deployment_id),
                "conversation_id": scoped_thread,
                "reply_conversation_id": reply_thread,
                "repo_full_name": REPO,
                "author": "U0REQUEST1",
                "summary": "Publish the next revision",
                "reply_kind": REPLY_KIND,
                "reply_channel": REPLY_CHANNEL,
                "dedupe_key": "migration-scoped-lineage-next-revision",
                "base_sha": BASE_SHA,
                "patch_b64": base64.b64encode(b"next migration patch").decode(),
                "changed_paths": ["README.md"],
            },
            headers=worker_headers,
        )
        assert revision.status_code == 201, revision.text

    assert _sql(
        "SELECT p.lineage_id::text, p.revision_number, "
        "a.conversation_id AS reply_conversation_id "
        "FROM curie.publications p "
        "JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.id = :id",
        {"id": uuid.UUID(revision.json()["id"])},
    ) == [
        {
            "lineage_id": str(publication_id),
            "revision_number": 2,
            "reply_conversation_id": reply_thread,
        }
    ]
    assert _sql(
        "SELECT count(*) AS count FROM curie.thread_publication_lineages "
        "WHERE agent_id = :agent_id AND repo_full_name = :repo",
        {"agent_id": agent_id, "repo": REPO},
    ) == [{"count": 1}]


def test_0041_backfills_active_and_succeeded_pr_publications_and_round_trips(
    isolated_migration_db: None,
) -> None:
    config = _config()
    command.upgrade(config, BELOW)
    agent_id, deployment_id = _seed_deployment()
    _, active_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-active-publication",
        workspace_conversation_id=_scoped_thread("thread-active-publication"),
        status="running",
    )
    _, terminal_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-terminal-publication",
        status="succeeded",
    )
    _, unsafe_terminal_id = _seed_publication(
        agent_id=agent_id,
        deployment_id=deployment_id,
        conversation_id="thread-unsafe-terminal-publication",
        status="succeeded",
        result_url="https://github.com/acme-corp/acme-bot/issues/123",
    )

    # This round trip owns 0041; later revisions have independent downgrade guards.
    command.upgrade(config, "0041")

    active = _sql(
        "SELECT p.lineage_id::text, p.revision_number, p.expected_prior_head, "
        "l.agent_id::text, l.conversation_id, l.repo_full_name, l.base_sha, "
        "l.branch, l.pr_number, l.pr_url, l.head_sha, l.status, l.version, "
        "l.latest_revision FROM curie.publications p JOIN "
        "curie.thread_publication_lineages l ON l.id = p.lineage_id "
        "WHERE p.id = :id",
        {"id": active_id},
    )
    assert len(active) == 1
    assert active[0] == {
        "lineage_id": active[0]["lineage_id"],
        "revision_number": 1,
        "expected_prior_head": BASE_SHA,
        "agent_id": str(agent_id),
        "conversation_id": _scoped_thread("thread-active-publication"),
        "repo_full_name": "acme-corp/acme-bot",
        "base_sha": BASE_SHA,
        "branch": f"curie/publication-{active_id.hex}",
        "pr_number": None,
        "pr_url": None,
        "head_sha": None,
        "status": "open",
        "version": 1,
        "latest_revision": 1,
    }
    uuid.UUID(active[0]["lineage_id"])
    terminal = _sql(
        "SELECT p.lineage_id::text, p.revision_number, p.expected_prior_head, "
        "p.workspace_conversation_id, a.conversation_id AS reply_conversation_id, "
        "l.conversation_id, l.branch, l.pr_number, l.pr_url, l.head_sha, l.status "
        "FROM curie.publications p JOIN curie.thread_publication_lineages l "
        "ON l.id = p.lineage_id JOIN curie.approvals a ON a.id = p.approval_id "
        "WHERE p.id = :id",
        {"id": terminal_id},
    )
    assert terminal == [
        {
            "lineage_id": str(terminal_id),
            "revision_number": 1,
            "expected_prior_head": BASE_SHA,
            "workspace_conversation_id": _scoped_thread(
                "thread-terminal-publication"
            ),
            "reply_conversation_id": "thread-terminal-publication",
            "conversation_id": _scoped_thread("thread-terminal-publication"),
            "branch": f"curie/publication-{terminal_id.hex}",
            "pr_number": 123,
            "pr_url": "https://github.com/acme-corp/acme-bot/pull/123",
            "head_sha": None,
            "status": "open",
        }
    ]
    assert _sql(
        "SELECT lineage_id, revision_number, expected_prior_head "
        "FROM curie.publications WHERE id = :id",
        {"id": unsafe_terminal_id},
    ) == [
        {
            "lineage_id": None,
            "revision_number": None,
            "expected_prior_head": None,
        }
    ]
    assert _sql("SELECT count(*) AS count FROM curie.thread_publication_lineages") == [
        {"count": 2}
    ]

    with pytest.raises(IntegrityError):
        _sql(
            "INSERT INTO curie.thread_publication_lineages "
            "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
            "status, version, latest_revision) VALUES "
            "(:id, :agent_id, :deployment_id, :conversation_id, "
            "'acme-corp/acme-bot', :base_sha, :branch, 'open', 1, 1)",
            {
                "id": uuid.uuid4(),
                "agent_id": agent_id,
                "deployment_id": deployment_id,
                "conversation_id": _scoped_thread("thread-active-publication"),
                "base_sha": "abcdef0123456789abcdef0123456789abcdef01",
                "branch": f"curie/publication-{uuid.uuid4().hex}",
            },
        )

    command.downgrade(config, BELOW)
    assert _sql(
        "SELECT table_name FROM information_schema.tables WHERE "
        "table_schema = 'curie' AND table_name = 'thread_publication_lineages'"
    ) == []
    assert _sql(
        "SELECT column_name FROM information_schema.columns WHERE "
        "table_schema = 'curie' AND table_name = 'publications' AND "
        "column_name IN ('lineage_id', 'revision_number', 'expected_prior_head')"
    ) == []

    command.upgrade(config, "0041")
    assert _sql(
        "SELECT l.branch, p.revision_number FROM curie.publications p JOIN "
        "curie.thread_publication_lineages l ON l.id = p.lineage_id "
        "WHERE p.id = :id",
        {"id": active_id},
    ) == [{"branch": f"curie/publication-{active_id.hex}", "revision_number": 1}]
