"""A publication approval card is shown on its route's fixed Slack channel (#5542).

ADR 0177 decisions 1 and 2: a route's fixed resolution target is where the
card is shown and answered, whatever surface the turn replies on. A factory run
admitted from a tracker issue replies on the tracker, so the kernel carries the
route's fixed channel on the publication request, and the card outbox posts the
card there with the kernel's policy-routed target rule. Without a fixed target
the card keeps the reply route exactly as before.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import Final, SessionStatus, ToolNote
from channel_protocol.reply import ReplyPost
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.approval_cards import ApprovalCardRef
from curie_worker.approvals import CreatedPublication, PublicationCreateRequest
from curie_worker.publication_loop import PublicationReconciler
from curie_worker.publication_store import PostgresPublicationStore
from curie_worker.reply_sink import CLUSTER_MESSAGE_ADAPTER, TargetRoute
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from .test_work_item_workspace import (
    ISSUE_URL,
    PUBLISH_TOOL,
    WORK_ITEM_REPO,
    _Binding,
    _escalations,
    _NoExistingPublication,
    _patch_publication_snapshot,
    _turn,
    _WorkItems,
    _Workspace,
)

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    os.environ.get(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres",
    ),
)
ROUTE = "publication"
FIXED_ROUTE: dict[str, Any] = {
    "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
    "approvers": {"users": ["U0EXAMPLE1"]},
}


# --- the kernel's publication request -------------------------------------------


class _RoutedBinding(_Binding):
    """The factory binding double, with the agent's approval route map."""

    def __init__(self, routes: dict[str, Any] | None) -> None:
        self._routes = routes

    def _deployment(self) -> object:
        return SimpleNamespace(**{**vars(super()._deployment()), "approval_routes": self._routes})


class _RecordingPublications(_NoExistingPublication):
    """Publication API double that records each create request it is sent."""

    def __init__(self) -> None:
        self.creates: list[PublicationCreateRequest] = []

    async def create_publication(
        self, request: PublicationCreateRequest, *, budget_s: float = 120
    ) -> CreatedPublication:
        self.creates.append(request)
        return CreatedPublication(
            id=f"publication-{len(self.creates)}",
            approval_id=f"approval-{len(self.creates)}",
            status="pending",
        )


def _publish_turn(route: str | None) -> list[Any]:
    return [
        ToolNote(text=f"running tool {PUBLISH_TOOL}", tool=PUBLISH_TOOL),
        Final(
            text="Ready to publish",
            status=SessionStatus.AWAITING_APPROVAL,
            approval_summary="Publish the change",
            approval_gate_kind="permission",
            approval_granted_tool=PUBLISH_TOOL,
            approval_route=route,
        ),
    ]


@contextlib.asynccontextmanager
async def _factory_publication(
    make_harness: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    *,
    kind: str,
    routes: dict[str, Any] | None,
    route: str | None,
) -> AsyncIterator[tuple[Any, list[PublicationCreateRequest]]]:
    """Drive one factory execution turn that ends at a publication approval gate."""

    publications = _RecordingPublications()
    async with make_harness(
        binding=_RoutedBinding(routes),
        workspace_factory=_Workspace,
        publication_creator=publications,
    ) as h:
        h.kernel._work_items = _WorkItems()
        _patch_publication_snapshot(h, monkeypatch)
        h.runner.turn_scripts = [_publish_turn(route)]
        h.runner.default_script = [Final(text="must not run", status=SessionStatus.DONE)]
        await h.kernel.process_event(
            _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}", kind=kind)
        )
        yield h, publications.creates


def test_publication_card_target_rides_a_tracker_publication_request(
    make_harness: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO", logger="curie_worker.kernel")

    async def exercise() -> list[PublicationCreateRequest]:
        async with _factory_publication(
            make_harness, monkeypatch, kind="github", routes={ROUTE: FIXED_ROUTE}, route=ROUTE
        ) as (_h, creates):
            return list(creates)

    creates = asyncio.run(exercise())

    assert len(creates) == 1
    request = creates[0]
    assert request.reply_kind == "github"
    assert request.reply_channel == WORK_ITEM_REPO
    assert request.route == ROUTE
    assert request.card_channel == "C0EXAMPLE1"
    assert request.to_json()["card_channel"] == "C0EXAMPLE1"
    assert _escalations(caplog) == []


@pytest.mark.parametrize(
    ("kind", "routes", "route", "expected"),
    [
        pytest.param("github", None, None, None, id="routeless"),
        pytest.param(
            "github",
            {ROUTE: {"resolution": {"mode": "requesting_surface"}}},
            ROUTE,
            None,
            id="requesting-surface",
        ),
        pytest.param(
            "slack",
            {ROUTE: {"resolution": {"kind": "slack", "address": "C0EXAMPLE2"}}},
            ROUTE,
            "C0EXAMPLE2",
            id="slack-reply-fixed-channel",
        ),
    ],
)
def test_publication_card_target_is_sent_only_for_a_fixed_route(
    make_harness: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    kind: str,
    routes: dict[str, Any] | None,
    route: str | None,
    expected: str | None,
) -> None:
    caplog.set_level("INFO", logger="curie_worker.kernel")

    async def exercise() -> list[PublicationCreateRequest]:
        async with _factory_publication(
            make_harness, monkeypatch, kind=kind, routes=routes, route=route
        ) as (_h, creates):
            return list(creates)

    creates = asyncio.run(exercise())

    assert len(creates) == 1
    request = creates[0]
    assert request.reply_kind == kind
    assert request.route == route
    assert request.card_channel == expected
    payload = request.to_json()
    if expected is None:
        assert "card_channel" not in payload
    else:
        assert payload["card_channel"] == expected
    assert _escalations(caplog) == []


# --- the card outbox --------------------------------------------------------------


@pytest.fixture(scope="module")
def migrated_database_url() -> Iterator[str]:
    """Replay the real API migrations into a database this module owns.

    The kernel suite's shared database is not migrated, and the card outbox
    reads the API's own tables. An unreachable Postgres skips only an optional
    local loop; required CI fails instead (``pg_connect_or_skip``).
    """

    base = make_url(_DB_URL)
    name = f"curie_card_target_{uuid.uuid4().hex}"
    url = base.set(database=name).render_as_string(hide_password=False)

    async def admin(statement: str) -> None:
        engine = create_async_engine(
            base.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
        )
        try:
            await pg_connect_or_skip(engine)
            async with engine.connect() as connection:
                await connection.exec_driver_sql(statement)
        finally:
            await engine.dispose()

    asyncio.run(admin(f'CREATE DATABASE "{name}"'))
    try:
        migrated = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=Path(__file__).resolve().parents[4] / "apps/api",
            env={**os.environ, "DATABASE_URL": url},
            check=False,
            capture_output=True,
            text=True,
        )
        assert migrated.returncode == 0, migrated.stderr[-4000:]
        yield url
    finally:
        asyncio.run(admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


@dataclass(frozen=True)
class _StoredPublication:
    """The reply and card facts the API stores for one pending publication."""

    conversation_id: str
    workspace_conversation_id: str
    dedupe_key: str
    author: str
    summary: str
    repo_full_name: str
    base_sha: str
    reply_kind: str
    reply_channel: str
    reply_placeholder: str | None
    reply_endpoint: str | None
    reply_adapter: str | None
    reply_identity: str | None
    route: str | None
    card_channel: str | None


def _stored_from_request(request: PublicationCreateRequest) -> _StoredPublication:
    """What the API stores for this request, by the rule its own suite proves."""

    return _StoredPublication(
        conversation_id=request.reply_conversation_id or request.conversation_id,
        workspace_conversation_id=request.conversation_id,
        dedupe_key=request.dedupe_key,
        author=request.author,
        summary=request.summary,
        repo_full_name=request.repo_full_name,
        base_sha=request.base_sha,
        reply_kind=request.reply_kind,
        reply_channel=request.reply_channel,
        reply_placeholder=request.reply_placeholder,
        reply_endpoint=request.reply_endpoint,
        reply_adapter=request.reply_adapter,
        reply_identity=request.reply_identity,
        route=request.route,
        card_channel=request.card_channel or request.reply_channel,
    )


@dataclass(frozen=True)
class _Seeded:
    agent_id: uuid.UUID
    version_id: uuid.UUID
    deployment_id: uuid.UUID
    approval_id: uuid.UUID
    lineage_id: uuid.UUID
    publication_id: uuid.UUID


async def _seed(engine: AsyncEngine, stored: _StoredPublication) -> _Seeded:
    seeded = _Seeded(
        agent_id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        deployment_id=uuid.uuid4(),
        approval_id=uuid.uuid4(),
        lineage_id=uuid.uuid4(),
        publication_id=uuid.uuid4(),
    )
    async with engine.begin() as connection:
        await connection.execute(
            text("INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)"),
            {
                "id": seeded.agent_id,
                "name": f"card-target-{seeded.agent_id.hex[:12]}",
                "repo": stored.repo_full_name,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
                "VALUES (:id, :agent_id, 'v1', 'kernel-test')"
            ),
            {"id": seeded.version_id, "agent_id": seeded.agent_id},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.deployments "
                "(id, agent_id, version_id, environment, workspace_enabled, status) "
                "VALUES (:id, :agent_id, :version_id, 'dev', true, 'active')"
            ),
            {
                "id": seeded.deployment_id,
                "agent_id": seeded.agent_id,
                "version_id": seeded.version_id,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.approvals "
                "(id, agent_id, conversation_id, author, summary, reply_kind, reply_channel, "
                "reply_placeholder, reply_endpoint, reply_adapter, reply_identity, dedupe_key, "
                "route, card_channel, gate_kind, granted_tool, purpose, status) VALUES "
                "(:id, :agent_id, :conversation_id, :author, :summary, :reply_kind, "
                ":reply_channel, :reply_placeholder, :reply_endpoint, :reply_adapter, "
                ":reply_identity, :dedupe_key, :route, :card_channel, 'permission', "
                ":granted_tool, 'publication', 'pending')"
            ),
            {
                "id": seeded.approval_id,
                "agent_id": seeded.agent_id,
                "conversation_id": stored.conversation_id,
                "author": stored.author,
                "summary": stored.summary,
                "reply_kind": stored.reply_kind,
                "reply_channel": stored.reply_channel,
                "reply_placeholder": stored.reply_placeholder,
                "reply_endpoint": stored.reply_endpoint,
                "reply_adapter": stored.reply_adapter,
                "reply_identity": stored.reply_identity,
                "dedupe_key": stored.dedupe_key,
                "route": stored.route,
                "card_channel": stored.card_channel,
                "granted_tool": PUBLISH_TOOL,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.thread_publication_lineages "
                "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, "
                "branch, status, version, latest_revision) VALUES "
                "(:id, :agent_id, :deployment_id, :conversation_id, :repo, :base_sha, "
                ":branch, 'open', 1, 1)"
            ),
            {
                "id": seeded.lineage_id,
                "agent_id": seeded.agent_id,
                "deployment_id": seeded.deployment_id,
                "conversation_id": stored.workspace_conversation_id,
                "repo": stored.repo_full_name,
                "base_sha": stored.base_sha,
                "branch": f"curie/card-target-{seeded.lineage_id.hex}",
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.publications "
                "(id, approval_id, deployment_id, workspace_conversation_id, lineage_id, "
                "revision_number, expected_prior_head, repo_full_name, status, base_sha, "
                "patch_bytes, changed_paths, title, body, reply_kind, reply_channel, "
                "reply_placeholder, reply_endpoint, reply_adapter, reply_identity, "
                "approval_card_reported_at) VALUES "
                "(:id, :approval_id, :deployment_id, :workspace_conversation_id, :lineage_id, "
                "1, :base_sha, :repo, 'pending', :base_sha, :patch, "
                "CAST(:changed_paths AS jsonb), 'Fix the widget parser', 'Fixes the parser.', "
                ":reply_kind, :reply_channel, :reply_placeholder, :reply_endpoint, "
                ":reply_adapter, :reply_identity, NULL)"
            ),
            {
                "id": seeded.publication_id,
                "approval_id": seeded.approval_id,
                "deployment_id": seeded.deployment_id,
                "workspace_conversation_id": stored.workspace_conversation_id,
                "lineage_id": seeded.lineage_id,
                "base_sha": stored.base_sha,
                "repo": stored.repo_full_name,
                "patch": b"diff --git a/src/widget.py b/src/widget.py\n",
                "changed_paths": json.dumps(["src/widget.py"]),
                "reply_kind": stored.reply_kind,
                "reply_channel": stored.reply_channel,
                "reply_placeholder": stored.reply_placeholder,
                "reply_endpoint": stored.reply_endpoint,
                "reply_adapter": stored.reply_adapter,
                "reply_identity": stored.reply_identity,
            },
        )
    return seeded


async def _unseed(engine: AsyncEngine, seeded: _Seeded) -> None:
    async with engine.begin() as connection:
        for table, row_id in (
            ("publications", seeded.publication_id),
            ("thread_publication_lineages", seeded.lineage_id),
            ("approvals", seeded.approval_id),
            ("deployments", seeded.deployment_id),
            ("agent_versions", seeded.version_id),
            ("agents", seeded.agent_id),
        ):
            await connection.execute(
                text(f"DELETE FROM curie.{table} WHERE id = :id"), {"id": row_id}
            )


@dataclass(frozen=True)
class _Delivered:
    returned: bool
    posts: list[tuple[ReplyPost, TargetRoute]]
    card: ApprovalCardRef | None
    reported: bool
    approval_id: uuid.UUID


async def _deliver(h: Any, database_url: str, stored: _StoredPublication) -> _Delivered:
    """Seed one pending publication and run the real outbox and reconciler on it."""

    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        seeded = await _seed(engine, stored)
        try:
            inert = object()
            reconciler = PublicationReconciler(
                store=PostgresPublicationStore(
                    engine, schema="curie", lease_owner="card-target-worker"
                ),
                # deliver_pending_card uses only the store, replies and card store.
                credentials=inert,  # type: ignore[arg-type]
                cluster=inert,  # type: ignore[arg-type]
                code_host=inert,  # type: ignore[arg-type]
                replies=h.sink,
                lineage=inert,  # type: ignore[arg-type]
                job_settings=inert,  # type: ignore[arg-type]
                card_store=h.card_store,
                transcript=inert,  # type: ignore[arg-type]
            )
            before = len(h.sink.events)
            returned = await reconciler.deliver_pending_card()
            posts = [
                (event, route)
                for event, route, _best_effort in h.sink.events[before:]
                if isinstance(event, ReplyPost)
            ]
            remembered = await h.card_store.read(str(seeded.approval_id))
            async with engine.connect() as connection:
                reported_at = (
                    await connection.execute(
                        text(
                            "SELECT approval_card_reported_at FROM curie.publications "
                            "WHERE id = :id"
                        ),
                        {"id": seeded.publication_id},
                    )
                ).scalar_one()
            return _Delivered(
                returned=returned,
                posts=posts,
                card=None if remembered is None else remembered[0],
                reported=reported_at is not None,
                approval_id=seeded.approval_id,
            )
        finally:
            await _unseed(engine, seeded)
    finally:
        await engine.dispose()


def _only_post(delivered: _Delivered) -> tuple[ReplyPost, TargetRoute]:
    assert delivered.returned is True
    assert delivered.reported
    assert len(delivered.posts) == 1
    event, route = delivered.posts[0]
    assert getattr(event.message.interaction, "id", None) == str(delivered.approval_id)
    return event, route


def test_publication_card_target_outbox_posts_a_tracker_card_on_the_fixed_channel(
    make_harness: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    async def exercise() -> tuple[PublicationCreateRequest, _Delivered]:
        async with _factory_publication(
            make_harness, monkeypatch, kind="github", routes={ROUTE: FIXED_ROUTE}, route=ROUTE
        ) as (h, creates):
            assert len(creates) == 1
            stored = _stored_from_request(creates[0])
            return creates[0], await _deliver(h, migrated_database_url, stored)

    request, delivered = asyncio.run(exercise())

    assert request.card_channel == "C0EXAMPLE1"
    event, route = _only_post(delivered)
    assert event.target.kind == "slack"
    assert event.target.address == "C0EXAMPLE1"
    assert event.target.conversation_id is None
    assert event.target.reply_ref is None
    assert route.endpoint is None
    assert route.adapter is None
    assert delivered.card is not None
    assert delivered.card.channel == "C0EXAMPLE1"
    assert delivered.card.kind == "slack"
    assert delivered.card.adapter is None


def test_publication_card_target_outbox_keeps_a_routeless_card_on_the_reply_route(
    make_harness: Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    async def exercise() -> tuple[PublicationCreateRequest, _Delivered]:
        async with _factory_publication(
            make_harness, monkeypatch, kind="github", routes=None, route=None
        ) as (h, creates):
            assert len(creates) == 1
            stored = _stored_from_request(creates[0])
            return creates[0], await _deliver(h, migrated_database_url, stored)

    request, delivered = asyncio.run(exercise())

    assert request.card_channel is None
    event, route = _only_post(delivered)
    assert event.target.kind == "github"
    assert event.target.address == WORK_ITEM_REPO
    assert event.target.conversation_id == (
        request.reply_conversation_id or request.conversation_id
    )
    assert event.target.reply_ref is None
    assert route == TargetRoute(endpoint=request.reply_endpoint, adapter=request.reply_adapter)
    assert delivered.card is not None
    assert delivered.card.channel == WORK_ITEM_REPO
    assert delivered.card.kind == "github"


@pytest.mark.parametrize(
    ("reply_adapter", "reply_identity"),
    [
        pytest.param("acme-named", None, id="named-identity"),
        pytest.param(CLUSTER_MESSAGE_ADAPTER, "acme-named", id="relay-identity"),
    ],
)
def test_publication_card_target_outbox_lends_the_slack_reply_identity(
    make_harness: Callable[..., Any],
    migrated_database_url: str,
    reply_adapter: str,
    reply_identity: str | None,
) -> None:
    stored = _StoredPublication(
        conversation_id="1700000000.000001",
        workspace_conversation_id="slack:C0EXAMPLE1:1700000000.000001",
        dedupe_key=f"card-target-{uuid.uuid4().hex}",
        author="U0EXAMPLE1",
        summary="Publish the change",
        repo_full_name=WORK_ITEM_REPO,
        base_sha="a1" * 20,
        reply_kind="slack",
        reply_channel="C0EXAMPLE1",
        reply_placeholder="1700000000.000002",
        reply_endpoint=None,
        reply_adapter=reply_adapter,
        reply_identity=reply_identity,
        route=ROUTE,
        card_channel="C0EXAMPLE2",
    )

    async def exercise() -> _Delivered:
        async with make_harness() as h:
            return await _deliver(h, migrated_database_url, stored)

    delivered = asyncio.run(exercise())

    event, route = _only_post(delivered)
    assert event.target.kind == "slack"
    assert event.target.address == "C0EXAMPLE2"
    assert event.target.conversation_id is None
    assert event.target.reply_ref is None
    assert route.endpoint is None
    assert route.adapter == "acme-named"
    assert delivered.card is not None
    assert delivered.card.channel == "C0EXAMPLE2"
    assert delivered.card.adapter == "acme-named"
