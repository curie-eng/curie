"""WorkItem lifecycle tests against the migrated PostgreSQL database."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.models import ThreadPublicationLineage
from curie_api.workitems import lifecycle as workitems
from sqlalchemy import DateTime, event, literal, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.visitors import replacement_traverse

REPO = "acme-corp/acme-bot"
CONVERSATION = "slack:C0EXAMPLE1:1700000000.000100"
FIXTURE_TERMINATION = "fixture observation: runtime termination was observed"


def with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


async def _now(session: AsyncSession) -> datetime:
    value = await session.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(value, datetime)
    return value


async def _agent(session: AsyncSession, name: str = "acme-bot") -> uuid.UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        text("INSERT INTO curie.agents (id, name) VALUES (:id, :name)"),
        {"id": agent_id, "name": f"{name}-{agent_id.hex[:8]}"},
    )
    await session.commit()
    return agent_id


async def _item(
    session: AsyncSession,
    agent_id: uuid.UUID,
    *,
    issue: int = 2573,
    installation: int = 202,
    repo: str = REPO,
    conversation: str = CONVERSATION,
) -> workitems.WorkItemOutcome:
    result = await workitems.create_or_get_work_item(
        session,
        github_repository_id=101,
        github_issue_number=issue,
        github_installation_id=installation,
        agent_id=agent_id,
        repo_full_name=repo,
        conversation_id=conversation,
    )
    assert isinstance(result, workitems.WorkItemOutcome), result
    return result


async def _request(
    session: AsyncSession,
    item: workitems.WorkItemSnapshot,
    *,
    request_id: uuid.UUID | None = None,
    deadline: datetime | None = None,
) -> workitems.WorkItemOutcome:
    result = await workitems.create_execution_request(
        session,
        work_item_id=item.id,
        request_id=request_id or uuid.uuid4(),
        wait_deadline=(
            deadline if deadline is not None else await _now(session) + timedelta(hours=1)
        ),
        expected_work_item_version=item.version,
    )
    assert isinstance(result, workitems.WorkItemOutcome), result
    assert result.request is not None
    return result


async def _start(
    session: AsyncSession, outcome: workitems.WorkItemOutcome
) -> workitems.WorkItemOutcome:
    request = outcome.request
    assert request is not None
    result = await workitems.start_execution(
        session,
        work_item_id=outcome.work_item.id,
        request_id=request.id,
        expected_work_item_version=outcome.work_item.version,
        expected_request_version=request.version,
    )
    assert isinstance(result, workitems.WorkItemOutcome), result
    assert result.request is not None
    return result


def _conflict(
    result: workitems.WorkItemResult, code: workitems.ConflictCode
) -> workitems.WorkItemConflict:
    assert isinstance(result, workitems.WorkItemConflict), result
    assert result.code == code
    return result


async def _lineage(
    session: AsyncSession,
    agent_id: uuid.UUID,
    *,
    conversation: str = CONVERSATION,
    repo: str = REPO,
    pr: int = 123,
    status: str = "open",
    github_repository_id: int | None = None,
    github_installation_id: int | None = None,
) -> uuid.UUID:
    version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.agent_versions "
            "(id, agent_id, version_label, created_by) "
            "VALUES (:id, :agent, 'v1', 'fixture')"
        ),
        {"id": version_id, "agent": agent_id},
    )
    await session.execute(
        text(
            "INSERT INTO curie.deployments "
            "(id, agent_id, version_id, environment, status) VALUES "
            "(:id, :agent, :version, CAST('dev' AS curie.environment), 'active')"
        ),
        {"id": deployment_id, "agent": agent_id, "version": version_id},
    )
    await session.execute(
        text(
            "INSERT INTO curie.thread_publication_lineages "
            "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
            "base_sha, branch, pr_number, pr_url, head_sha, status, version, "
            "latest_revision, github_repository_id, github_installation_id, "
            "github_pr_node_id, base_ref) VALUES "
            "(:id, :agent, :deployment, :conversation, :repo, :base_sha, "
            ":branch, :pr, :url, :head_sha, :status, 1, 1, "
            ":github_repository_id, :github_installation_id, :github_pr_node_id, "
            ":base_ref)"
        ),
        {
            "id": lineage_id,
            "agent": agent_id,
            "deployment": deployment_id,
            "conversation": conversation,
            "repo": repo,
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
            "branch": f"curie/publication-{lineage_id.hex}",
            "pr": pr,
            "url": f"https://github.com/{repo}/pull/{pr}",
            "head_sha": "1123456789abcdef0123456789abcdef01234567",
            "status": status,
            "github_repository_id": github_repository_id,
            "github_installation_id": github_installation_id,
            "github_pr_node_id": (
                f"PR_kwDO{lineage_id.hex}" if github_repository_id is not None else None
            ),
            "base_ref": "main" if github_repository_id is not None else None,
        },
    )
    await session.commit()
    return lineage_id


async def _grant_opened_pull_request(
    session: AsyncSession,
    outcome: workitems.WorkItemOutcome,
    *,
    pr: int,
) -> workitems.WorkItemOutcome:
    """Link a succeeded publication so complete_execution can mean a pull request."""

    item = outcome.work_item
    request = outcome.request
    assert request is not None
    lineage_id = await _lineage(
        session,
        item.agent_id,
        conversation=item.conversation_id,
        repo=item.repo_full_name,
        pr=pr,
        github_repository_id=item.github_repository_id,
        github_installation_id=item.github_installation_id,
    )
    deployment_id = await session.scalar(
        text("SELECT deployment_id FROM curie.thread_publication_lineages WHERE id = :id"),
        {"id": lineage_id},
    )
    approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
    pr_url = f"https://github.com/{item.repo_full_name}/pull/{pr}"
    await session.execute(
        text(
            "INSERT INTO curie.approvals "
            "(id, agent_id, conversation_id, author, summary, reply_kind, "
            "reply_channel, dedupe_key, status, purpose) VALUES "
            "(:id, :agent, :conversation, 'U0REQUEST1', "
            "'Publish repository changes', 'github', :channel, :dedupe, "
            "'approved', 'publication')"
        ),
        {
            "id": approval_id,
            "agent": item.agent_id,
            "conversation": item.conversation_id,
            "channel": item.repo_full_name,
            "dedupe": f"opened-pr-{publication_id.hex}",
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.publications "
            "(id, approval_id, deployment_id, workspace_conversation_id, "
            "lineage_id, execution_request_id, revision_number, repo_full_name, "
            "status, base_sha, changed_paths, title, body, reply_kind, "
            "reply_channel, result_url) "
            "VALUES "
            "(:id, :approval, :deployment, :conversation, :lineage, :request, 1, "
            ":repo, 'succeeded', :base_sha, CAST('[\"README.md\"]' AS jsonb), "
            "'Update README', 'Approved platform publication.', 'github', "
            ":channel, :result_url)"
        ),
        {
            "id": publication_id,
            "approval": approval_id,
            "deployment": deployment_id,
            "conversation": item.conversation_id,
            "lineage": lineage_id,
            "request": request.id,
            "repo": item.repo_full_name,
            "channel": item.repo_full_name,
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
            "result_url": pr_url,
        },
    )
    await session.commit()
    linked = await workitems.link_publication_lineage(
        session,
        work_item_id=item.id,
        request_id=request.id,
        publication_lineage_id=lineage_id,
        expected_work_item_version=item.version,
        expected_request_version=request.version,
    )
    assert isinstance(linked, workitems.WorkItemOutcome), linked
    return linked


async def _elapsed_running(
    session: AsyncSession,
    agent_id: uuid.UUID,
    *,
    lineage_id: uuid.UUID | None = None,
    issue: int = 2573,
) -> tuple[uuid.UUID, uuid.UUID, datetime, datetime]:
    """Insert internally consistent historical deadlines exactly once."""

    item_id, request_id = uuid.uuid4(), uuid.uuid4()
    started_at = await _now(session) - timedelta(seconds=1801)
    execution_deadline = started_at + timedelta(seconds=1800)
    await session.execute(
        text(
            "INSERT INTO curie.work_items "
            "(id, github_repository_id, github_issue_number, "
            "github_installation_id, agent_id, repo_full_name, conversation_id, "
            "publication_lineage_id, version, next_sequence) VALUES "
            "(:id, 101, :issue, 202, :agent, :repo, :conversation, :lineage, 2, 2)"
        ),
        {
            "id": item_id,
            "issue": issue,
            "agent": agent_id,
            "repo": REPO,
            "conversation": CONVERSATION,
            "lineage": lineage_id,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.execution_requests "
            "(id, work_item_id, sequence, status, wait_deadline, started_at, "
            "execution_deadline, version, execution_attempts) VALUES "
            "(:id, :item, 1, 'running', :wait, :started, :deadline, 2, 1)"
        ),
        {
            "id": request_id,
            "item": item_id,
            "wait": started_at - timedelta(seconds=1),
            "started": started_at,
            "deadline": execution_deadline,
        },
    )
    await session.commit()
    return item_id, request_id, started_at, execution_deadline


def test_work_item_identity_replay_and_conflicting_provenance(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        created = await _item(session, agent_id)
        assert created.request is None
        assert created.replayed is False
        assert (created.work_item.version, created.work_item.next_sequence) == (1, 1)

        replay = await _item(session, agent_id)
        assert replay.replayed is True
        assert replay.work_item == created.work_item

        variants: list[dict[str, Any]] = [
            {"github_installation_id": 999},
            {"agent_id": await _agent(session, "other-agent")},
            {"repo_full_name": "acme-corp/another-bot"},
            {"conversation_id": "slack:C0EXAMPLE1:1700000000.000200"},
        ]
        base: dict[str, Any] = {
            "github_repository_id": 101,
            "github_issue_number": 2573,
            "github_installation_id": 202,
            "agent_id": agent_id,
            "repo_full_name": REPO,
            "conversation_id": CONVERSATION,
        }
        for variant in variants:
            conflict = _conflict(
                await workitems.create_or_get_work_item(session, **(base | variant)),
                "identity_mismatch",
            )
            assert conflict.work_item_id == created.work_item.id
            assert conflict.work_item_version == 1
            assert await session.scalar(text("SELECT 1")) == 1

        missing = _conflict(
            await workitems.create_execution_request(
                session,
                work_item_id=uuid.uuid4(),
                request_id=uuid.uuid4(),
                wait_deadline=await _now(session) + timedelta(hours=1),
                expected_work_item_version=1,
            ),
            "not_found",
        )
        assert missing.work_item_id is not None

    with_session(body)


def test_concurrent_duplicate_work_item_intake_returns_one_replay(clean_db: None) -> None:
    async def setup(session: AsyncSession) -> uuid.UUID:
        return await _agent(session, "duplicate-intake-agent")

    agent_id = with_session(setup)

    async def race() -> list[workitems.WorkItemResult]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def contender() -> workitems.WorkItemResult:
            async with maker() as session:
                return await workitems.create_or_get_work_item(
                    session,
                    github_repository_id=101,
                    github_issue_number=2573,
                    github_installation_id=202,
                    agent_id=agent_id,
                    repo_full_name=REPO,
                    conversation_id=CONVERSATION,
                )

        try:
            gathered = asyncio.gather(contender(), contender())
            return list(await asyncio.wait_for(gathered, timeout=10))
        finally:
            await engine.dispose()

    results = asyncio.run(race())
    assert all(isinstance(result, workitems.WorkItemOutcome) for result in results)
    outcomes = [result for result in results if isinstance(result, workitems.WorkItemOutcome)]
    assert sorted(outcome.replayed for outcome in outcomes) == [False, True]
    assert len({outcome.work_item.id for outcome in outcomes}) == 1
    assert all(outcome.work_item.version == 1 for outcome in outcomes)

    async def verify(session: AsyncSession) -> None:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM curie.work_items "
                    "WHERE github_repository_id = 101 AND github_issue_number = 2573"
                )
            )
            == 1
        )

    with_session(verify)


def test_request_replay_fences_and_sequential_lifecycle(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        item = (await _item(session, await _agent(session))).work_item
        request_id = uuid.uuid4()
        deadline = await _now(session) - timedelta(seconds=1)
        first = await _request(session, item, request_id=request_id, deadline=deadline)
        request = first.request
        assert request is not None
        assert (first.work_item.version, first.work_item.next_sequence) == (2, 2)
        assert (request.sequence, request.version, request.status) == (1, 1, "waiting")

        replay = await workitems.create_execution_request(
            session,
            work_item_id=item.id,
            request_id=request_id,
            wait_deadline=deadline,
            expected_work_item_version=item.version,
        )
        assert isinstance(replay, workitems.WorkItemOutcome), replay
        assert replay.replayed is True
        assert replay.work_item == first.work_item and replay.request == request
        _conflict(
            await workitems.create_execution_request(
                session,
                work_item_id=item.id,
                request_id=request_id,
                wait_deadline=deadline + timedelta(seconds=1),
                expected_work_item_version=first.work_item.version,
            ),
            "identity_mismatch",
        )
        active = _conflict(
            await workitems.create_execution_request(
                session,
                work_item_id=item.id,
                request_id=uuid.uuid4(),
                wait_deadline=deadline + timedelta(hours=1),
                expected_work_item_version=first.work_item.version,
            ),
            "active_request",
        )
        assert (active.request_id, active.request_version) == (request_id, 1)
        stale = _conflict(
            await workitems.create_execution_request(
                session,
                work_item_id=item.id,
                request_id=uuid.uuid4(),
                wait_deadline=deadline + timedelta(hours=1),
                expected_work_item_version=item.version,
            ),
            "stale_version",
        )
        assert stale.work_item_version == 2

        late_start = _conflict(
            await workitems.start_execution(
                session,
                work_item_id=item.id,
                request_id=request.id,
                expected_work_item_version=first.work_item.version,
                expected_request_version=request.version,
            ),
            "waiting_deadline_elapsed",
        )
        assert (late_start.work_item_version, late_start.request_version) == (2, 1)
        unchanged_waiting = (
            (
                await session.execute(
                    text(
                        "SELECT w.version AS work_version, r.status, "
                        "r.version AS request_version, r.wait_deadline, r.started_at, "
                        "r.execution_deadline FROM curie.work_items w "
                        "JOIN curie.execution_requests r ON r.work_item_id = w.id "
                        "WHERE w.id = :id AND r.id = :request_id"
                    ),
                    {"id": item.id, "request_id": request.id},
                )
            )
            .mappings()
            .one()
        )
        assert (
            unchanged_waiting.work_version,
            unchanged_waiting.status,
            unchanged_waiting.request_version,
            unchanged_waiting.wait_deadline,
            unchanged_waiting.started_at,
            unchanged_waiting.execution_deadline,
        ) == (2, "waiting", 1, deadline, None, None)

        expired = await workitems.expire_waiting(
            session,
            work_item_id=item.id,
            request_id=request.id,
            expected_work_item_version=first.work_item.version,
            expected_request_version=request.version,
        )
        assert isinstance(expired, workitems.WorkItemOutcome), expired
        assert expired.request is not None
        assert (expired.request.status, expired.request.terminal_cause) == (
            "expired",
            "capacity_wait_expired",
        )
        assert (expired.work_item.version, expired.request.version) == (2, 2)
        _conflict(
            await workitems.start_execution(
                session,
                work_item_id=item.id,
                request_id=request.id,
                expected_work_item_version=2,
                expected_request_version=2,
            ),
            "illegal_transition",
        )
        second = await _request(session, expired.work_item)
        assert second.request is not None
        assert (second.request.sequence, second.work_item.version) == (2, 3)

    with_session(body)


def test_concurrent_request_creation_has_one_winner(clean_db: None) -> None:
    async def setup(session: AsyncSession) -> tuple[uuid.UUID, datetime]:
        item = (await _item(session, await _agent(session))).work_item
        return item.id, await _now(session) + timedelta(hours=1)

    item_id, deadline = with_session(setup)

    async def race() -> list[workitems.WorkItemResult]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def contender() -> workitems.WorkItemResult:
            async with maker() as session:
                return await workitems.create_execution_request(
                    session,
                    work_item_id=item_id,
                    request_id=uuid.uuid4(),
                    wait_deadline=deadline,
                    expected_work_item_version=1,
                )

        try:
            return list(await asyncio.gather(contender(), contender()))
        finally:
            await engine.dispose()

    results = asyncio.run(race())
    winners = [r for r in results if isinstance(r, workitems.WorkItemOutcome)]
    losers = [r for r in results if isinstance(r, workitems.WorkItemConflict)]
    assert len(winners) == len(losers) == 1
    assert losers[0].code in {"stale_version", "active_request"}

    async def verify(session: AsyncSession) -> None:
        row = (
            await session.execute(
                text(
                    "SELECT version, next_sequence, "
                    "(SELECT count(*) FROM curie.execution_requests "
                    "WHERE work_item_id = w.id) AS requests "
                    "FROM curie.work_items w WHERE id = :id"
                ),
                {"id": item_id},
            )
        ).one()
        assert tuple(row) == (2, 2, 1)

    with_session(verify)


def test_concurrent_request_id_reuse_across_work_items_is_an_identity_conflict(
    clean_db: None,
) -> None:
    async def setup(
        session: AsyncSession,
    ) -> tuple[workitems.WorkItemSnapshot, workitems.WorkItemSnapshot, datetime]:
        agent_id = await _agent(session, "request-identity-agent")
        first = (await _item(session, agent_id)).work_item
        second = (await _item(session, agent_id, issue=2574)).work_item
        return first, second, await _now(session) + timedelta(hours=1)

    first, second, deadline = with_session(setup)
    request_id = uuid.uuid4()

    async def race() -> list[workitems.WorkItemResult]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def contender(item: workitems.WorkItemSnapshot) -> workitems.WorkItemResult:
            async with maker() as session:
                return await workitems.create_execution_request(
                    session,
                    work_item_id=item.id,
                    request_id=request_id,
                    wait_deadline=deadline,
                    expected_work_item_version=item.version,
                )

        try:
            gathered = asyncio.gather(contender(first), contender(second))
            return list(await asyncio.wait_for(gathered, timeout=10))
        finally:
            await engine.dispose()

    results = asyncio.run(race())
    winners = [r for r in results if isinstance(r, workitems.WorkItemOutcome)]
    losers = [r for r in results if isinstance(r, workitems.WorkItemConflict)]
    assert len(winners) == len(losers) == 1
    assert winners[0].request is not None
    assert winners[0].request.id == request_id
    assert losers[0].code == "identity_mismatch"
    assert losers[0].request_id == request_id
    assert losers[0].work_item_id is not None
    assert losers[0].work_item_id != winners[0].work_item.id

    async def verify(session: AsyncSession) -> None:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT id, version, next_sequence FROM curie.work_items "
                        "WHERE id IN (:first, :second)"
                    ),
                    {"first": first.id, "second": second.id},
                )
            )
            .mappings()
            .all()
        )
        state = {row.id: (row.version, row.next_sequence) for row in rows}
        assert state[winners[0].work_item.id] == (2, 2)
        assert state[losers[0].work_item_id] == (1, 1)
        assert (
            await session.scalar(
                text("SELECT count(*) FROM curie.execution_requests WHERE id = :id"),
                {"id": request_id},
            )
            == 1
        )

    with_session(verify)


def test_start_terminal_transitions_and_deadlines_are_fenced(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        item = (await _item(session, await _agent(session))).work_item
        waiting = await _request(session, item)
        request = waiting.request
        assert request is not None
        immutable = (request.id, request.wait_deadline)
        _conflict(
            await workitems.expire_waiting(
                session,
                work_item_id=item.id,
                request_id=request.id,
                expected_work_item_version=waiting.work_item.version,
                expected_request_version=request.version,
            ),
            "illegal_transition",
        )

        running = await _start(session, waiting)
        request = running.request
        assert request is not None
        assert request.started_at is not None and request.execution_deadline is not None
        assert request.execution_deadline - request.started_at == timedelta(seconds=1800)
        assert (request.id, request.wait_deadline) == immutable
        assert (running.work_item.version, request.version) == (2, 2)
        early_deadline = _conflict(
            await workitems.request_execution_deadline_cancellation(
                session,
                work_item_id=item.id,
                request_id=request.id,
                expected_work_item_version=running.work_item.version,
                expected_request_version=request.version,
            ),
            "illegal_transition",
        )
        assert (early_deadline.work_item_version, early_deadline.request_version) == (
            2,
            2,
        )
        unchanged_running = (
            (
                await session.execute(
                    text(
                        "SELECT w.version AS work_version, r.status, "
                        "r.version AS request_version, r.started_at, "
                        "r.execution_deadline, r.terminal_at, r.terminal_cause "
                        "FROM curie.work_items w JOIN curie.execution_requests r "
                        "ON r.work_item_id = w.id WHERE w.id = :id AND r.id = :request_id"
                    ),
                    {"id": item.id, "request_id": request.id},
                )
            )
            .mappings()
            .one()
        )
        assert (
            unchanged_running.work_version,
            unchanged_running.status,
            unchanged_running.request_version,
            unchanged_running.started_at,
            unchanged_running.execution_deadline,
            unchanged_running.terminal_at,
            unchanged_running.terminal_cause,
        ) == (2, "running", 2, request.started_at, request.execution_deadline, None, None)
        _conflict(
            await workitems.fail_execution(
                session,
                work_item_id=item.id,
                request_id=request.id,
                cause=" ",
                expected_work_item_version=2,
                expected_request_version=2,
            ),
            "illegal_transition",
        )
        _conflict(
            await workitems.start_execution(
                session,
                work_item_id=item.id,
                request_id=request.id,
                expected_work_item_version=2,
                expected_request_version=2,
            ),
            "illegal_transition",
        )
        stale = _conflict(
            await workitems.start_execution(
                session,
                work_item_id=item.id,
                request_id=request.id,
                expected_work_item_version=2,
                expected_request_version=1,
            ),
            "stale_version",
        )
        assert stale.request_version == 2

        opened = await _grant_opened_pull_request(session, running, pr=659)
        assert opened.request is not None
        completed = await workitems.complete_execution(
            session,
            work_item_id=item.id,
            request_id=request.id,
            expected_work_item_version=opened.work_item.version,
            expected_request_version=opened.request.version,
        )
        assert isinstance(completed, workitems.WorkItemOutcome), completed
        assert completed.request is not None
        assert (completed.request.status, completed.request.version) == ("completed", 3)
        assert completed.work_item.version == opened.work_item.version
        _conflict(
            await workitems.fail_execution(
                session,
                work_item_id=item.id,
                request_id=request.id,
                cause="rewrite",
                expected_work_item_version=completed.work_item.version,
                expected_request_version=3,
            ),
            "illegal_transition",
        )

        second = await _start(session, await _request(session, completed.work_item))
        assert second.request is not None
        failed = await workitems.fail_execution(
            session,
            work_item_id=item.id,
            request_id=second.request.id,
            cause="fixture_machine_failure",
            expected_work_item_version=second.work_item.version,
            expected_request_version=second.request.version,
        )
        assert isinstance(failed, workitems.WorkItemOutcome), failed
        assert failed.request is not None
        assert (
            failed.request.status,
            failed.request.sequence,
            failed.request.terminal_cause,
            failed.request.version,
            failed.work_item.version,
        ) == ("failed", 2, "fixture_machine_failure", 3, 4)

        row = (
            await session.execute(
                text(
                    "SELECT started_at, execution_deadline FROM curie.execution_requests "
                    "WHERE id = :id"
                ),
                {"id": request.id},
            )
        ).one()
        assert tuple(row) == (request.started_at, request.execution_deadline)

    with_session(body)


@pytest.mark.parametrize("mismatch", ["agent", "conversation", "repository"])
def test_publication_linkage_validates_lineage_identity(clean_db: None, mismatch: str) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        running = await _start(
            session, await _request(session, (await _item(session, agent_id)).work_item)
        )
        assert running.request is not None
        lineage_id = await _lineage(
            session,
            await _agent(session, "other-agent") if mismatch == "agent" else agent_id,
            conversation=(
                "slack:C0EXAMPLE1:1700000000.000200" if mismatch == "conversation" else CONVERSATION
            ),
            repo="acme-corp/another-bot" if mismatch == "repository" else REPO,
        )
        _conflict(
            await workitems.link_publication_lineage(
                session,
                work_item_id=running.work_item.id,
                request_id=running.request.id,
                publication_lineage_id=lineage_id,
                expected_work_item_version=running.work_item.version,
                expected_request_version=running.request.version,
            ),
            "lineage_mismatch",
        )

    with_session(body)


@pytest.mark.parametrize(
    ("repository_id", "installation_id", "matches"),
    [(101, 202, True), (999, 202, False), (101, 999, False)],
)
def test_publication_linkage_validates_verified_github_identity(
    clean_db: None,
    repository_id: int,
    installation_id: int,
    matches: bool,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        running = await _start(
            session, await _request(session, (await _item(session, agent_id)).work_item)
        )
        assert running.request is not None
        lineage_id = await _lineage(
            session,
            agent_id,
            github_repository_id=repository_id,
            github_installation_id=installation_id,
        )
        stored_identity = (
            await session.execute(
                text(
                    "SELECT github_repository_id, github_installation_id "
                    "FROM curie.thread_publication_lineages WHERE id = :id"
                ),
                {"id": lineage_id},
            )
        ).one()
        assert tuple(stored_identity) == (repository_id, installation_id)

        result = await workitems.link_publication_lineage(
            session,
            work_item_id=running.work_item.id,
            request_id=running.request.id,
            publication_lineage_id=lineage_id,
            expected_work_item_version=running.work_item.version,
            expected_request_version=running.request.version,
        )
        if matches:
            assert isinstance(result, workitems.WorkItemOutcome), result
            assert result.work_item.publication_lineage_id == lineage_id
            assert result.work_item.version == 3
        else:
            conflict = _conflict(result, "lineage_mismatch")
            assert (conflict.work_item_version, conflict.request_version) == (2, 2)
            persisted = await session.scalar(
                text("SELECT publication_lineage_id FROM curie.work_items WHERE id = :id"),
                {"id": running.work_item.id},
            )
            assert persisted is None

    with_session(body)


def test_publication_linkage_refreshes_cached_verified_github_identity(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        running = await _start(
            session, await _request(session, (await _item(session, agent_id)).work_item)
        )
        assert running.request is not None
        lineage_id = await _lineage(session, agent_id)
        cached = await session.scalar(
            select(ThreadPublicationLineage).where(ThreadPublicationLineage.id == lineage_id)
        )
        assert cached is not None
        assert (cached.github_repository_id, cached.github_installation_id) == (
            None,
            None,
        )

        update_engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(update_engine) as updater:
                await updater.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET "
                        "github_repository_id = 999, github_installation_id = 999, "
                        "github_pr_node_id = :node_id, base_ref = 'main' WHERE id = :id"
                    ),
                    {"id": lineage_id, "node_id": f"PR_kwDO{lineage_id.hex}"},
                )
                await updater.commit()
        finally:
            await update_engine.dispose()

        persisted_identity = (
            await session.execute(
                text(
                    "SELECT github_repository_id, github_installation_id "
                    "FROM curie.thread_publication_lineages WHERE id = :id"
                ),
                {"id": lineage_id},
            )
        ).one()
        assert tuple(persisted_identity) == (999, 999)
        assert (cached.github_repository_id, cached.github_installation_id) == (
            None,
            None,
        )

        conflict = _conflict(
            await workitems.link_publication_lineage(
                session,
                work_item_id=running.work_item.id,
                request_id=running.request.id,
                publication_lineage_id=lineage_id,
                expected_work_item_version=running.work_item.version,
                expected_request_version=running.request.version,
            ),
            "lineage_mismatch",
        )
        assert (conflict.work_item_version, conflict.request_version) == (2, 2)
        assert (
            await session.scalar(
                text("SELECT publication_lineage_id FROM curie.work_items WHERE id = :id"),
                {"id": running.work_item.id},
            )
            is None
        )

    with_session(body)


def test_publication_link_is_unique_idempotent_and_restricts_deletion(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        lineage_id = await _lineage(
            session,
            agent_id,
            repo="ACME-CORP/ACME-BOT",
            status="merged",
        )
        historical_identity = (
            await session.execute(
                text(
                    "SELECT status, github_repository_id, github_installation_id "
                    "FROM curie.thread_publication_lineages WHERE id = :id"
                ),
                {"id": lineage_id},
            )
        ).one()
        assert tuple(historical_identity) == ("merged", None, None)
        waiting = await _request(session, (await _item(session, agent_id)).work_item)
        assert waiting.request is not None
        _conflict(
            await workitems.link_publication_lineage(
                session,
                work_item_id=waiting.work_item.id,
                request_id=waiting.request.id,
                publication_lineage_id=lineage_id,
                expected_work_item_version=waiting.work_item.version,
                expected_request_version=waiting.request.version,
            ),
            "publication_ineligible",
        )
        first = await _start(session, waiting)
        assert first.request is not None
        linked = await workitems.link_publication_lineage(
            session,
            work_item_id=first.work_item.id,
            request_id=first.request.id,
            publication_lineage_id=lineage_id,
            expected_work_item_version=first.work_item.version,
            expected_request_version=first.request.version,
        )
        assert isinstance(linked, workitems.WorkItemOutcome), linked
        assert linked.request is not None
        assert linked.work_item.publication_lineage_id == lineage_id
        assert (linked.work_item.version, linked.request.version) == (3, 2)

        replay = await workitems.link_publication_lineage(
            session,
            work_item_id=linked.work_item.id,
            request_id=linked.request.id,
            publication_lineage_id=lineage_id,
            expected_work_item_version=3,
            expected_request_version=2,
        )
        assert isinstance(replay, workitems.WorkItemOutcome), replay
        assert replay.replayed is True and replay.work_item == linked.work_item

        other_lineage = await _lineage(
            session,
            agent_id,
            conversation="slack:C0EXAMPLE1:1700000000.000200",
            pr=124,
        )
        _conflict(
            await workitems.link_publication_lineage(
                session,
                work_item_id=linked.work_item.id,
                request_id=linked.request.id,
                publication_lineage_id=other_lineage,
                expected_work_item_version=3,
                expected_request_version=2,
            ),
            "lineage_already_owned",
        )

        second = await _start(
            session,
            await _request(session, (await _item(session, agent_id, issue=2574)).work_item),
        )
        assert second.request is not None
        _conflict(
            await workitems.link_publication_lineage(
                session,
                work_item_id=second.work_item.id,
                request_id=second.request.id,
                publication_lineage_id=lineage_id,
                expected_work_item_version=second.work_item.version,
                expected_request_version=second.request.version,
            ),
            "lineage_already_owned",
        )

        with pytest.raises(IntegrityError):
            await session.execute(
                text("DELETE FROM curie.thread_publication_lineages WHERE id = :id"),
                {"id": lineage_id},
            )
            await session.commit()
        await session.rollback()
        assert (
            await session.scalar(
                text("SELECT count(*) FROM curie.thread_publication_lineages WHERE id = :id"),
                {"id": lineage_id},
            )
            == 1
        )

    with_session(body)


def test_cancellation_seals_idle_waiting_terminal_and_running_work(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        idle = (await _item(session, agent_id)).work_item
        cancelled = await workitems.request_cancellation(
            session, work_item_id=idle.id, expected_work_item_version=1
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        assert cancelled.request is None and cancelled.work_item.cancelled_at is not None
        assert cancelled.work_item.version == 2
        replay = await workitems.request_cancellation(
            session, work_item_id=idle.id, expected_work_item_version=2
        )
        assert isinstance(replay, workitems.WorkItemOutcome), replay
        assert replay.replayed is True and replay.work_item == cancelled.work_item
        _conflict(
            await workitems.request_cancellation(
                session, work_item_id=idle.id, expected_work_item_version=1
            ),
            "stale_version",
        )
        _conflict(
            await workitems.create_execution_request(
                session,
                work_item_id=idle.id,
                request_id=uuid.uuid4(),
                wait_deadline=await _now(session) + timedelta(hours=1),
                expected_work_item_version=2,
            ),
            "work_item_cancelled",
        )

        waiting = await _request(session, (await _item(session, agent_id, issue=2574)).work_item)
        stopped = await workitems.request_cancellation(
            session, work_item_id=waiting.work_item.id, expected_work_item_version=2
        )
        assert isinstance(stopped, workitems.WorkItemOutcome), stopped
        assert stopped.request is not None
        assert (
            stopped.request.status,
            stopped.request.terminal_cause,
            stopped.request.termination_observation,
            stopped.request.version,
            stopped.work_item.version,
        ) == ("cancelled", "issue_cancelled", None, 2, 3)

        terminal = await _start(
            session,
            await _request(
                session,
                (
                    await _item(
                        session,
                        agent_id,
                        issue=2575,
                        conversation=f"slack:C0EXAMPLE1:{uuid.uuid4().hex[:12]}",
                    )
                ).work_item,
            ),
        )
        assert terminal.request is not None
        terminal = await _grant_opened_pull_request(session, terminal, pr=125)
        assert terminal.request is not None
        terminal = await workitems.complete_execution(
            session,
            work_item_id=terminal.work_item.id,
            request_id=terminal.request.id,
            expected_work_item_version=terminal.work_item.version,
            expected_request_version=terminal.request.version,
        )
        assert isinstance(terminal, workitems.WorkItemOutcome), terminal
        sealed = await workitems.request_cancellation(
            session,
            work_item_id=terminal.work_item.id,
            expected_work_item_version=terminal.work_item.version,
        )
        assert isinstance(sealed, workitems.WorkItemOutcome), sealed
        assert sealed.request is None and sealed.work_item.version == 4

        running_item = (await _item(session, agent_id, issue=2576)).work_item
        lineage_id = await _lineage(session, agent_id, pr=126)
        running = await _start(session, await _request(session, running_item))
        assert running.request is not None
        linked = await workitems.link_publication_lineage(
            session,
            work_item_id=running.work_item.id,
            request_id=running.request.id,
            publication_lineage_id=lineage_id,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(linked, workitems.WorkItemOutcome), linked
        assert linked.request is not None
        requested = await workitems.request_cancellation(
            session, work_item_id=linked.work_item.id, expected_work_item_version=3
        )
        assert isinstance(requested, workitems.WorkItemOutcome), requested
        assert requested.request is not None
        assert (
            requested.request.status,
            requested.request.terminal_cause,
            requested.request.version,
            requested.work_item.version,
            requested.work_item.publication_lineage_id,
        ) == ("cancellation_requested", "issue_cancelled", 3, 4, lineage_id)
        for result in (
            await workitems.complete_execution(
                session,
                work_item_id=requested.work_item.id,
                request_id=requested.request.id,
                expected_work_item_version=4,
                expected_request_version=3,
            ),
            await workitems.fail_execution(
                session,
                work_item_id=requested.work_item.id,
                request_id=requested.request.id,
                cause="fixture failure",
                expected_work_item_version=4,
                expected_request_version=3,
            ),
            await workitems.link_publication_lineage(
                session,
                work_item_id=requested.work_item.id,
                request_id=requested.request.id,
                publication_lineage_id=lineage_id,
                expected_work_item_version=4,
                expected_request_version=3,
            ),
        ):
            _conflict(result, "work_item_cancelled")
        _conflict(
            await workitems.record_runtime_termination(
                session,
                work_item_id=requested.work_item.id,
                request_id=requested.request.id,
                termination_observation=" ",
                expected_work_item_version=4,
                expected_request_version=3,
            ),
            "termination_observation_required",
        )
        observed = await workitems.record_runtime_termination(
            session,
            work_item_id=requested.work_item.id,
            request_id=requested.request.id,
            termination_observation=FIXTURE_TERMINATION,
            expected_work_item_version=4,
            expected_request_version=3,
        )
        assert isinstance(observed, workitems.WorkItemOutcome), observed
        assert observed.request is not None
        assert (
            observed.request.status,
            observed.request.termination_observation,
            observed.request.version,
            observed.work_item.version,
            observed.work_item.publication_lineage_id,
        ) == ("cancelled", FIXTURE_TERMINATION, 4, 4, lineage_id)

    with_session(body)


def test_elapsed_execution_is_observed_before_reuse_and_can_be_cancelled_explicitly(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        lineage_id = await _lineage(session, agent_id)
        item_id, request_id, started_at, deadline = await _elapsed_running(
            session, agent_id, lineage_id=lineage_id
        )
        for operation in (
            workitems.complete_execution(
                session,
                work_item_id=item_id,
                request_id=request_id,
                expected_work_item_version=2,
                expected_request_version=2,
            ),
            workitems.fail_execution(
                session,
                work_item_id=item_id,
                request_id=request_id,
                cause="fixture failure",
                expected_work_item_version=2,
                expected_request_version=2,
            ),
            workitems.link_publication_lineage(
                session,
                work_item_id=item_id,
                request_id=request_id,
                publication_lineage_id=lineage_id,
                expected_work_item_version=2,
                expected_request_version=2,
            ),
        ):
            _conflict(await operation, "execution_deadline_elapsed")

        deadline_requested = await workitems.request_execution_deadline_cancellation(
            session,
            work_item_id=item_id,
            request_id=request_id,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(deadline_requested, workitems.WorkItemOutcome), deadline_requested
        assert deadline_requested.request is not None
        assert (
            deadline_requested.request.status,
            deadline_requested.request.terminal_cause,
            deadline_requested.request.version,
            deadline_requested.work_item.version,
        ) == ("cancellation_requested", "execution_deadline", 3, 2)
        assert deadline_requested.request.started_at == started_at
        assert deadline_requested.request.execution_deadline == deadline
        assert deadline_requested.work_item.cancelled_at is None
        assert deadline_requested.work_item.publication_lineage_id == lineage_id

        _conflict(
            await workitems.create_execution_request(
                session,
                work_item_id=item_id,
                request_id=uuid.uuid4(),
                wait_deadline=await _now(session) + timedelta(hours=1),
                expected_work_item_version=2,
            ),
            "active_request",
        )
        explicit = await workitems.request_cancellation(
            session, work_item_id=item_id, expected_work_item_version=2
        )
        assert isinstance(explicit, workitems.WorkItemOutcome), explicit
        assert explicit.request is not None
        assert (
            explicit.request.terminal_cause,
            explicit.request.version,
            explicit.work_item.version,
        ) == ("issue_cancelled", 4, 3)
        observed = await workitems.record_runtime_termination(
            session,
            work_item_id=item_id,
            request_id=request_id,
            termination_observation=FIXTURE_TERMINATION,
            expected_work_item_version=3,
            expected_request_version=4,
        )
        assert isinstance(observed, workitems.WorkItemOutcome), observed
        assert observed.request is not None
        assert observed.request.status == "cancelled"
        assert observed.work_item.publication_lineage_id == lineage_id

        other_agent = await _agent(session, "deadline-agent")
        other_lineage = await _lineage(session, other_agent, pr=124)
        other_item, other_request, _, _ = await _elapsed_running(
            session, other_agent, lineage_id=other_lineage, issue=2574
        )
        timeout = await workitems.request_execution_deadline_cancellation(
            session,
            work_item_id=other_item,
            request_id=other_request,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(timeout, workitems.WorkItemOutcome), timeout
        assert timeout.request is not None
        _conflict(
            await workitems.record_runtime_termination(
                session,
                work_item_id=other_item,
                request_id=other_request,
                termination_observation="",
                expected_work_item_version=2,
                expected_request_version=3,
            ),
            "termination_observation_required",
        )
        expired = await workitems.record_runtime_termination(
            session,
            work_item_id=other_item,
            request_id=other_request,
            termination_observation=FIXTURE_TERMINATION,
            expected_work_item_version=2,
            expected_request_version=3,
        )
        assert isinstance(expired, workitems.WorkItemOutcome), expired
        assert expired.request is not None
        assert (expired.request.status, expired.request.version) == ("expired", 4)
        assert expired.work_item.cancelled_at is None
        assert expired.work_item.publication_lineage_id == other_lineage
        next_request = await _request(session, expired.work_item)
        assert next_request.request is not None
        assert (next_request.request.sequence, next_request.work_item.version) == (2, 3)

    with_session(body)


def test_link_rechecks_deadline_after_a_real_database_lock_wait(clean_db: None) -> None:
    async def run() -> None:
        setup_engine = create_async_engine(get_settings().database_url)
        lock_engine = create_async_engine(get_settings().database_url)
        service_engine = create_async_engine(get_settings().database_url)
        observer_engine = create_async_engine(get_settings().database_url)
        service_task: asyncio.Task[workitems.WorkItemResult] | None = None
        service_now: datetime | None = None
        lineage_lock_observed = False
        clock_inputs: list[tuple[bool, bool, datetime]] = []

        def use_controlled_sql_clock(
            _connection: Connection,
            statement: ClauseElement,
            multiparams: Any,
            params: Any,
            _execution_options: Any,
        ) -> tuple[ClauseElement, Any, Any]:
            def replace_clock(element: ClauseElement) -> ClauseElement | None:
                if (
                    isinstance(element, FunctionElement)
                    and getattr(element, "name", None) == "clock_timestamp"
                ):
                    assert service_now is not None
                    clock_inputs.append(
                        (lineage_lock_observed, getattr(statement, "is_update", False), service_now)
                    )
                    return literal(service_now, type_=DateTime(timezone=True))
                return None

            # PostgreSQL still acquires locks and evaluates the original
            # predicates against the immutable stored deadline.
            return replacement_traverse(statement, {}, replace_clock), multiparams, params

        event.listen(
            service_engine.sync_engine,
            "before_execute",
            use_controlled_sql_clock,
            retval=True,
        )
        try:
            async with AsyncSession(setup_engine) as setup:
                agent_id = await _agent(setup, "deadline-crossing-agent")
                lineage_id = await _lineage(setup, agent_id, pr=125)
                item_id, request_id = uuid.uuid4(), uuid.uuid4()
                database_now = await _now(setup)
                service_now = database_now
                execution_deadline = database_now + timedelta(hours=2)
                started_at = database_now - timedelta(minutes=30)
                await setup.execute(
                    text(
                        "INSERT INTO curie.work_items "
                        "(id, github_repository_id, github_issue_number, "
                        "github_installation_id, agent_id, repo_full_name, "
                        "conversation_id, version, next_sequence) VALUES "
                        "(:id, 101, 2577, 202, :agent, :repo, :conversation, 2, 2)"
                    ),
                    {
                        "id": item_id,
                        "agent": agent_id,
                        "repo": REPO,
                        "conversation": CONVERSATION,
                    },
                )
                await setup.execute(
                    text(
                        "INSERT INTO curie.execution_requests "
                        "(id, work_item_id, sequence, status, wait_deadline, "
                        "started_at, execution_deadline, version, "
                        "execution_attempts) VALUES "
                        "(:id, :item, 1, 'running', :wait, :started, :deadline, 2, 1)"
                    ),
                    {
                        "id": request_id,
                        "item": item_id,
                        "wait": started_at + timedelta(seconds=1),
                        "started": started_at,
                        "deadline": execution_deadline,
                    },
                )
                await setup.commit()

            async with (
                AsyncSession(lock_engine) as holder,
                AsyncSession(service_engine) as service,
                AsyncSession(observer_engine) as observer,
            ):
                holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
                service_pid = await service.scalar(text("SELECT pg_backend_pid()"))
                assert isinstance(holder_pid, int)
                assert isinstance(service_pid, int)
                await holder.execute(
                    text("LOCK TABLE curie.thread_publication_lineages IN ACCESS EXCLUSIVE MODE")
                )

                service_task = asyncio.create_task(
                    workitems.link_publication_lineage(
                        service,
                        work_item_id=item_id,
                        request_id=request_id,
                        publication_lineage_id=lineage_id,
                        expected_work_item_version=2,
                        expected_request_version=2,
                    )
                )
                holder_released = False

                try:

                    async def observe_lineage_lock_wait() -> None:
                        while True:
                            # pg_stat_get_activity is STABLE. The first read in
                            # this observer transaction freezes the activity
                            # snapshot, so a cold connection that samples before
                            # the waiter parks never sees it. Drop that snapshot
                            # on every poll; pg_locks and pg_blocking_pids are
                            # already volatile.
                            await observer.execute(text("SELECT pg_stat_clear_snapshot()"))
                            row = (
                                (
                                    await observer.execute(
                                        text(
                                            "SELECT a.wait_event_type, a.query, "
                                            "EXISTS (SELECT 1 FROM pg_locks l "
                                            "WHERE l.pid = a.pid AND NOT l.granted) "
                                            "AS waiting_lock, CAST(:holder AS integer) = "
                                            "ANY(pg_blocking_pids(a.pid)) "
                                            "AS blocked_by_holder FROM pg_stat_activity a "
                                            "WHERE a.pid = :service"
                                        ),
                                        {"holder": holder_pid, "service": service_pid},
                                    )
                                )
                                .mappings()
                                .one_or_none()
                            )
                            if row is not None and (
                                row.wait_event_type == "Lock"
                                and row.waiting_lock
                                and row.blocked_by_holder
                                and "thread_publication_lineages" in row.query.lower()
                            ):
                                return
                            if service_task.done():
                                raise AssertionError(
                                    "lineage link completed before reaching the lock gate"
                                )
                            await asyncio.sleep(0.01)

                    await asyncio.wait_for(observe_lineage_lock_wait(), timeout=60)
                    assert any(
                        not after_lock and clock == database_now
                        for after_lock, _is_update, clock in clock_inputs
                    )
                    service_now = execution_deadline + timedelta(seconds=1)
                    lineage_lock_observed = True
                    await holder.rollback()
                    holder_released = True

                    result = await asyncio.wait_for(service_task, timeout=60)
                    assert any(
                        after_lock and is_update and clock == service_now
                        for after_lock, is_update, clock in clock_inputs
                    ), result
                    conflict = _conflict(result, "execution_deadline_elapsed")
                    assert (conflict.work_item_version, conflict.request_version) == (
                        2,
                        2,
                    )

                    persisted = (
                        (
                            await observer.execute(
                                text(
                                    "SELECT w.publication_lineage_id, "
                                    "w.version AS work_version, r.status, "
                                    "r.version AS request_version, r.started_at, "
                                    "r.execution_deadline FROM curie.work_items w "
                                    "JOIN curie.execution_requests r "
                                    "ON r.work_item_id = w.id WHERE w.id = :id"
                                ),
                                {"id": item_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    assert persisted.publication_lineage_id is None
                    assert (
                        persisted.work_version,
                        persisted.status,
                        persisted.request_version,
                    ) == (2, "running", 2)
                    assert persisted.started_at == started_at
                    assert persisted.execution_deadline == execution_deadline
                finally:
                    if not holder_released:
                        await holder.rollback()
                    if not service_task.done():
                        service_task.cancel()
                        await asyncio.gather(service_task, return_exceptions=True)
        finally:
            if service_task is not None and not service_task.done():
                service_task.cancel()
                await asyncio.gather(service_task, return_exceptions=True)
            event.remove(
                service_engine.sync_engine,
                "before_execute",
                use_controlled_sql_clock,
            )
            await asyncio.gather(
                setup_engine.dispose(),
                lock_engine.dispose(),
                service_engine.dispose(),
                observer_engine.dispose(),
            )

    asyncio.run(run())


def test_start_and_cancellation_race_has_only_serial_outcomes(clean_db: None) -> None:
    async def setup(session: AsyncSession) -> tuple[uuid.UUID, uuid.UUID]:
        waiting = await _request(session, (await _item(session, await _agent(session))).work_item)
        assert waiting.request is not None
        return waiting.work_item.id, waiting.request.id

    item_id, request_id = with_session(setup)

    async def race() -> list[workitems.WorkItemResult]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def start() -> workitems.WorkItemResult:
            async with maker() as session:
                return await workitems.start_execution(
                    session,
                    work_item_id=item_id,
                    request_id=request_id,
                    expected_work_item_version=2,
                    expected_request_version=1,
                )

        async def cancel() -> workitems.WorkItemResult:
            async with maker() as session:
                return await workitems.request_cancellation(
                    session,
                    work_item_id=item_id,
                    expected_work_item_version=2,
                )

        try:
            return list(await asyncio.gather(start(), cancel()))
        finally:
            await engine.dispose()

    started, cancelled = asyncio.run(race())
    assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
    assert cancelled.request is not None and cancelled.work_item.cancelled_at is not None
    if isinstance(started, workitems.WorkItemOutcome):
        assert started.request is not None and started.request.status == "running"
        assert cancelled.request.status == "cancellation_requested"
    else:
        assert started.code == "work_item_cancelled"
        assert cancelled.request.status == "cancelled"


@pytest.mark.parametrize("operation", ["complete", "fail", "link"])
def test_terminal_and_link_races_cannot_win_after_sticky_cancellation(
    clean_db: None, operation: str
) -> None:
    async def setup(
        session: AsyncSession,
    ) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID | None]:
        agent_id = await _agent(session)
        running = await _start(
            session, await _request(session, (await _item(session, agent_id)).work_item)
        )
        assert running.request is not None
        lineage_id = None
        if operation == "link":
            lineage_id = await _lineage(session, agent_id)
        elif operation == "complete":
            running = await _grant_opened_pull_request(session, running, pr=4100)
            assert running.request is not None
            lineage_id = running.work_item.publication_lineage_id
        return (
            running.work_item.id,
            running.request.id,
            lineage_id,
            running.work_item.version,
        )

    item_id, request_id, lineage_id, work_version = with_session(setup)

    async def race() -> list[workitems.WorkItemResult]:
        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)

        async def mutate() -> workitems.WorkItemResult:
            async with maker() as session:
                common = {
                    "work_item_id": item_id,
                    "request_id": request_id,
                    "expected_work_item_version": work_version,
                    "expected_request_version": 2,
                }
                if operation == "complete":
                    return await workitems.complete_execution(session, **common)
                if operation == "fail":
                    return await workitems.fail_execution(
                        session, cause="fixture failure", **common
                    )
                assert lineage_id is not None
                return await workitems.link_publication_lineage(
                    session, publication_lineage_id=lineage_id, **common
                )

        async def cancel() -> workitems.WorkItemResult:
            async with maker() as session:
                return await workitems.request_cancellation(
                    session,
                    work_item_id=item_id,
                    expected_work_item_version=work_version,
                )

        try:
            return list(await asyncio.gather(mutate(), cancel()))
        finally:
            await engine.dispose()

    mutation, cancellation = asyncio.run(race())
    if isinstance(cancellation, workitems.WorkItemConflict):
        assert operation == "link" and cancellation.code == "stale_version"
        assert isinstance(mutation, workitems.WorkItemOutcome), mutation
        assert mutation.request is not None
        assert mutation.work_item.version == 3
        assert mutation.request.status == "running"
        assert mutation.request.version == 2
        assert mutation.work_item.publication_lineage_id == lineage_id
        assert cancellation.work_item_version == mutation.work_item.version

        async def retry(session: AsyncSession) -> workitems.WorkItemOutcome:
            result = await workitems.request_cancellation(
                session, work_item_id=item_id, expected_work_item_version=3
            )
            assert isinstance(result, workitems.WorkItemOutcome), result
            return result

        retried = with_session(retry)
        assert retried.request is not None
        assert (
            retried.work_item.version,
            retried.request.status,
            retried.request.version,
            retried.work_item.publication_lineage_id,
        ) == (4, "cancellation_requested", 3, lineage_id)
        expected = (4, "cancellation_requested", 3, lineage_id)
    else:
        assert cancellation.work_item.cancelled_at is not None
        if cancellation.request is not None:
            assert cancellation.request.status == "cancellation_requested"
            assert isinstance(mutation, workitems.WorkItemConflict), mutation
            assert mutation.code == "work_item_cancelled"
            assert (
                cancellation.work_item.version,
                cancellation.request.version,
                cancellation.work_item.publication_lineage_id,
            ) == (work_version + 1, 3, lineage_id if operation == "complete" else None)
            expected = (
                work_version + 1,
                "cancellation_requested",
                3,
                lineage_id if operation == "complete" else None,
            )
        else:
            assert operation in {"complete", "fail"}
            assert isinstance(mutation, workitems.WorkItemOutcome), mutation
            assert mutation.request is not None
            terminal_status = "completed" if operation == "complete" else "failed"
            assert mutation.request.status == terminal_status
            assert mutation.request.version == 3
            assert cancellation.work_item.version == work_version + 1
            expected = (
                work_version + 1,
                terminal_status,
                3,
                lineage_id if operation == "complete" else None,
            )

    async def verify(session: AsyncSession) -> None:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT w.version AS work_version, w.cancelled_at, "
                        "w.publication_lineage_id, r.status, r.version AS request_version "
                        "FROM curie.work_items w JOIN curie.execution_requests r "
                        "ON r.work_item_id = w.id WHERE w.id = :id AND r.id = :request_id"
                    ),
                    {"id": item_id, "request_id": request_id},
                )
            )
            .mappings()
            .one()
        )
        assert row.cancelled_at is not None
        assert (
            row.work_version,
            row.status,
            row.request_version,
            row.publication_lineage_id,
        ) == expected

    with_session(verify)


async def _elapsed_running_with_lapsed_heartbeat(
    session: AsyncSession,
    agent_id: uuid.UUID,
    *,
    issue: int = 2573,
) -> tuple[uuid.UUID, uuid.UUID]:
    item_id, request_id = uuid.uuid4(), uuid.uuid4()
    started_at = await _now(session) - timedelta(seconds=1801)
    execution_deadline = started_at + timedelta(seconds=1800)
    await session.execute(
        text(
            "INSERT INTO curie.work_items "
            "(id, github_repository_id, github_issue_number, "
            "github_installation_id, agent_id, repo_full_name, conversation_id, "
            "version, next_sequence) VALUES "
            "(:id, 101, :issue, 202, :agent, :repo, :conversation, 2, 2)"
        ),
        {
            "id": item_id,
            "issue": issue,
            "agent": agent_id,
            "repo": REPO,
            "conversation": CONVERSATION,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.execution_requests "
            "(id, work_item_id, sequence, status, wait_deadline, started_at, "
            "execution_deadline, version, execution_attempts, "
            "runtime_heartbeat_expires_at) VALUES "
            "(:id, :item, 1, 'running', :wait, :started, :deadline, 2, 1, "
            ":heartbeat)"
        ),
        {
            "id": request_id,
            "item": item_id,
            "wait": started_at - timedelta(seconds=1),
            "started": started_at,
            "deadline": execution_deadline,
            "heartbeat": started_at,
        },
    )
    await session.commit()
    return item_id, request_id


def test_owner_lost_cancellation_fails_with_observation_and_yields_to_issue_cancel(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, request_id = await _elapsed_running_with_lapsed_heartbeat(session, agent_id)
        marked = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=item_id,
            request_id=request_id,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(marked, workitems.WorkItemOutcome), marked
        assert marked.request is not None
        assert (
            marked.request.status,
            marked.request.terminal_cause,
            marked.request.termination_observation,
        ) == ("cancellation_requested", "owner_lost", None)

        observed = await workitems.record_runtime_termination(
            session,
            work_item_id=item_id,
            request_id=request_id,
            termination_observation=FIXTURE_TERMINATION,
            expected_work_item_version=2,
            expected_request_version=3,
        )
        assert isinstance(observed, workitems.WorkItemOutcome), observed
        assert observed.request is not None
        assert (
            observed.request.status,
            observed.request.terminal_cause,
            observed.request.termination_observation,
        ) == ("failed", "owner_lost", FIXTURE_TERMINATION)

        other_item, other_request = await _elapsed_running_with_lapsed_heartbeat(
            session, agent_id, issue=2574
        )
        owner_lost = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=other_item,
            request_id=other_request,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(owner_lost, workitems.WorkItemOutcome), owner_lost
        explicit = await workitems.request_cancellation(
            session, work_item_id=other_item, expected_work_item_version=2
        )
        assert isinstance(explicit, workitems.WorkItemOutcome), explicit
        assert explicit.request is not None
        assert explicit.request.terminal_cause == "issue_cancelled"
        assert explicit.request.status == "cancellation_requested"

    with_session(body)


@pytest.mark.parametrize("past_grace", [False, True], ids=["within_grace", "past_grace"])
def test_owner_lost_cancellation_waits_one_runtime_ttl_after_heartbeat_expiry(
    clean_db: None, past_grace: bool
) -> None:
    async def body(session: AsyncSession) -> None:
        running = await _start(
            session,
            await _request(session, (await _item(session, await _agent(session))).work_item),
        )
        assert running.request is not None
        ttl = get_settings().work_item_runtime_ttl_seconds
        elapsed = ttl + 5 if past_grace else 5
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET runtime_owner = 'worker-a', "
                "runtime_epoch = 1, runtime_heartbeat_expires_at = :expired "
                "WHERE id = :id"
            ),
            {
                "id": running.request.id,
                "expired": await _now(session) - timedelta(seconds=elapsed),
            },
        )
        await session.commit()
        result = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=running.work_item.id,
            request_id=running.request.id,
            expected_work_item_version=running.work_item.version,
            expected_request_version=running.request.version,
        )
        if past_grace:
            assert isinstance(result, workitems.WorkItemOutcome), result
            assert result.request is not None
            assert (
                result.request.status,
                result.request.terminal_cause,
                result.request.version,
                result.work_item.version,
                result.work_item.cancelled_at,
            ) == (
                "cancellation_requested",
                "owner_lost",
                running.request.version + 1,
                running.work_item.version,
                None,
            )
        else:
            refused = _conflict(result, "illegal_transition")
            assert refused.request_version == running.request.version
            assert refused.work_item_version == running.work_item.version
        row = (
            (
                await session.execute(
                    text(
                        "SELECT status, terminal_cause, version, cancellation_requested_at "
                        "FROM curie.execution_requests WHERE id = :id"
                    ),
                    {"id": running.request.id},
                )
            )
            .mappings()
            .one()
        )
        assert (row.status, row.terminal_cause, row.version) == (
            ("cancellation_requested", "owner_lost", running.request.version + 1)
            if past_grace
            else ("running", None, running.request.version)
        )
        assert (row.cancellation_requested_at is not None) == past_grace

    with_session(body)


@pytest.mark.parametrize("stale", ["work_item", "request"])
def test_owner_lost_cancellation_preserves_stale_version_fencing(
    clean_db: None, stale: str
) -> None:
    async def body(session: AsyncSession) -> None:
        running = await _start(
            session,
            await _request(session, (await _item(session, await _agent(session))).work_item),
        )
        assert running.request is not None
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET runtime_owner = 'worker-a', "
                "runtime_epoch = 1, runtime_heartbeat_expires_at = :expired "
                "WHERE id = :id"
            ),
            {
                "id": running.request.id,
                "expired": await _now(session)
                - timedelta(seconds=get_settings().work_item_runtime_ttl_seconds + 5),
            },
        )
        await session.commit()
        result = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=running.work_item.id,
            request_id=running.request.id,
            expected_work_item_version=running.work_item.version - (stale == "work_item"),
            expected_request_version=running.request.version - (stale == "request"),
        )
        _conflict(result, "stale_version")
        row = (
            (
                await session.execute(
                    text(
                        "SELECT w.version AS work_item_version, r.version AS request_version, "
                        "r.status, r.terminal_cause, r.cancellation_requested_at "
                        "FROM curie.work_items w JOIN curie.execution_requests r "
                        "ON r.work_item_id = w.id WHERE r.id = :id"
                    ),
                    {"id": running.request.id},
                )
            )
            .mappings()
            .one()
        )
        assert (
            row.work_item_version,
            row.request_version,
            row.status,
            row.terminal_cause,
            row.cancellation_requested_at,
        ) == (running.work_item.version, running.request.version, "running", None, None)
        accepted = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=running.work_item.id,
            request_id=running.request.id,
            expected_work_item_version=running.work_item.version,
            expected_request_version=running.request.version,
        )
        assert isinstance(accepted, workitems.WorkItemOutcome), accepted
        assert accepted.request is not None
        assert (accepted.request.status, accepted.request.terminal_cause) == (
            "cancellation_requested",
            "owner_lost",
        )

    with_session(body)


@pytest.mark.parametrize("past_ttl", [False, True], ids=["recent_start", "owner_absent"])
def test_owner_absent_cancellation_keeps_its_start_time_threshold(
    clean_db: None, past_ttl: bool
) -> None:
    async def body(session: AsyncSession) -> None:
        running = await _start(
            session,
            await _request(session, (await _item(session, await _agent(session))).work_item),
        )
        assert running.request is not None
        ttl = get_settings().work_item_runtime_ttl_seconds
        now = await _now(session)
        started_at = now - timedelta(seconds=ttl + 5 if past_ttl else ttl / 2)
        # Seed the historical window before restoring the immutable-field guard.
        await session.execute(text("ALTER TABLE curie.execution_requests DISABLE TRIGGER USER"))
        await session.execute(
            text(
                "UPDATE curie.execution_requests SET started_at = :started, "
                "execution_deadline = :deadline, runtime_owner = NULL, "
                "runtime_heartbeat_expires_at = :live WHERE id = :id"
            ),
            {
                "id": running.request.id,
                "started": started_at,
                "deadline": started_at + timedelta(seconds=1800),
                "live": now + timedelta(seconds=ttl),
            },
        )
        await session.execute(text("ALTER TABLE curie.execution_requests ENABLE TRIGGER USER"))
        await session.commit()
        result = await workitems.request_owner_lost_cancellation(
            session,
            work_item_id=running.work_item.id,
            request_id=running.request.id,
            expected_work_item_version=running.work_item.version,
            expected_request_version=running.request.version,
        )
        if past_ttl:
            assert isinstance(result, workitems.WorkItemOutcome), result
            assert result.request is not None
            assert (result.request.status, result.request.terminal_cause) == (
                "cancellation_requested",
                "owner_lost",
            )
        else:
            refused = _conflict(result, "illegal_transition")
            assert refused.request_version == running.request.version
        row = (
            (
                await session.execute(
                    text(
                        "SELECT status, terminal_cause, version "
                        "FROM curie.execution_requests WHERE id = :id"
                    ),
                    {"id": running.request.id},
                )
            )
            .mappings()
            .one()
        )
        assert (row.status, row.terminal_cause, row.version) == (
            ("cancellation_requested", "owner_lost", running.request.version + 1)
            if past_ttl
            else ("running", None, running.request.version)
        )

    with_session(body)


def test_start_execution_spends_exactly_one_attempt(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        waiting = await _request(session, (await _item(session, await _agent(session))).work_item)
        assert waiting.request is not None
        waiting_attempts = await session.scalar(
            text("SELECT execution_attempts FROM curie.execution_requests WHERE id = :id"),
            {"id": waiting.request.id},
        )
        assert waiting_attempts == 0
        running = await _start(session, waiting)
        assert running.request is not None
        started_attempts = await session.scalar(
            text("SELECT execution_attempts FROM curie.execution_requests WHERE id = :id"),
            {"id": running.request.id},
        )
        assert started_attempts == 1
        assert running.request.started_at is not None

    with_session(body)


def test_an_opened_pull_request_completes_after_the_execution_deadline(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        lineage_id = await _lineage(session, agent_id, pr=77)
        item_id, request_id, _, _ = await _elapsed_running(
            session, agent_id, lineage_id=lineage_id, issue=2924
        )
        deployment_id = await session.scalar(
            text("SELECT deployment_id FROM curie.thread_publication_lineages WHERE id = :id"),
            {"id": lineage_id},
        )
        approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
        pr_url = f"https://github.com/{REPO}/pull/77"
        await session.execute(
            text(
                "INSERT INTO curie.approvals "
                "(id, agent_id, conversation_id, author, summary, reply_kind, "
                "reply_channel, dedupe_key, status, purpose) VALUES "
                "(:id, :agent, :conversation, 'U0REQUEST1', "
                "'Publish repository changes', 'github', :channel, :dedupe, "
                "'approved', 'publication')"
            ),
            {
                "id": approval_id,
                "agent": agent_id,
                "conversation": CONVERSATION,
                "channel": REPO,
                "dedupe": f"late-pr-{publication_id.hex}",
            },
        )
        await session.execute(
            text(
                "INSERT INTO curie.publications "
                "(id, approval_id, deployment_id, workspace_conversation_id, "
                "lineage_id, execution_request_id, revision_number, repo_full_name, "
                "status, base_sha, changed_paths, title, body, reply_kind, "
                "reply_channel, result_url) "
                "VALUES "
                "(:id, :approval, :deployment, :conversation, :lineage, :request, 1, "
                ":repo, 'succeeded', :base_sha, CAST('[\"README.md\"]' AS jsonb), "
                "'Update README', 'Approved platform publication.', 'github', "
                ":channel, :result_url)"
            ),
            {
                "id": publication_id,
                "approval": approval_id,
                "deployment": deployment_id,
                "conversation": CONVERSATION,
                "lineage": lineage_id,
                "request": request_id,
                "repo": REPO,
                "channel": REPO,
                "base_sha": "0123456789abcdef0123456789abcdef01234567",
                "result_url": pr_url,
            },
        )
        await session.commit()
        completed = await workitems.complete_execution(
            session,
            work_item_id=item_id,
            request_id=request_id,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(completed, workitems.WorkItemOutcome), completed
        assert completed.request is not None
        assert (completed.request.status, completed.request.terminal_cause) == (
            "completed",
            "completed",
        )

    with_session(body)


async def _seed_transcript(session: AsyncSession, agent_id: uuid.UUID, thread: str) -> None:
    await session.execute(
        text(
            "INSERT INTO curie.thread_transcripts (id, agent_id, thread_key, value) "
            'VALUES (:id, :agent, :thread, CAST(\'[{"role": "user"}]\' AS jsonb))'
        ),
        {"id": uuid.uuid4(), "agent": agent_id, "thread": thread},
    )
    await session.commit()


async def _transcript_threads(session: AsyncSession, agent_id: uuid.UUID) -> set[str]:
    rows = await session.scalars(
        text("SELECT thread_key FROM curie.thread_transcripts WHERE agent_id = :agent"),
        {"agent": agent_id},
    )
    return set(rows)


def test_a_terminal_work_item_expires_only_its_own_transcript(clean_db: None) -> None:
    """ADR-0170 (#3070): the terminal transition deletes the thread's history."""

    other_thread = "slack:C0EXAMPLE1:1700000000.000200"

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item = (await _item(session, agent_id)).work_item
        running = await _start(session, await _request(session, item))
        await _seed_transcript(session, agent_id, CONVERSATION)
        await _seed_transcript(session, agent_id, other_thread)
        request = running.request
        assert request is not None

        stale = await workitems.fail_execution(
            session,
            work_item_id=running.work_item.id,
            request_id=request.id,
            cause="runner_escalated",
            expected_work_item_version=running.work_item.version,
            expected_request_version=request.version + 1,
        )
        _conflict(stale, "stale_version")
        assert await _transcript_threads(session, agent_id) == {CONVERSATION, other_thread}

        failed = await workitems.fail_execution(
            session,
            work_item_id=running.work_item.id,
            request_id=request.id,
            cause="runner_escalated",
            expected_work_item_version=running.work_item.version,
            expected_request_version=request.version,
        )
        assert isinstance(failed, workitems.WorkItemOutcome), failed
        assert failed.request is not None and failed.request.status == "failed"
        assert await _transcript_threads(session, agent_id) == {other_thread}

    with_session(body)


def test_cancelling_a_waiting_work_item_expires_its_transcript(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        waiting = await _request(session, (await _item(session, agent_id)).work_item)
        await _seed_transcript(session, agent_id, CONVERSATION)

        cancelled = await workitems.request_cancellation(
            session,
            work_item_id=waiting.work_item.id,
            expected_work_item_version=waiting.work_item.version,
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        assert cancelled.request is not None and cancelled.request.status == "cancelled"
        assert await _transcript_threads(session, agent_id) == set()

    with_session(body)


def test_cancelling_a_work_item_with_no_active_request_expires_its_transcript(
    clean_db: None,
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item = (await _item(session, agent_id)).work_item
        await _seed_transcript(session, agent_id, CONVERSATION)

        cancelled = await workitems.request_cancellation(
            session, work_item_id=item.id, expected_work_item_version=item.version
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        assert cancelled.request is None
        assert cancelled.work_item.cancelled_at is not None
        assert await _transcript_threads(session, agent_id) == set()

    with_session(body)


# --- per-agent execution deadline (#3071) -----------------------------------


async def _set_agent_deadline(
    session: AsyncSession, agent_id: uuid.UUID, seconds: int | None
) -> None:
    await session.execute(
        text("UPDATE curie.agents SET execution_deadline_seconds = :s WHERE id = :id"),
        {"s": seconds, "id": agent_id},
    )
    await session.commit()


async def _stored_deadline_span(session: AsyncSession, request_id: uuid.UUID) -> timedelta:
    row = (
        await session.execute(
            text(
                "SELECT execution_deadline - started_at AS span "
                "FROM curie.execution_requests WHERE id = :id"
            ),
            {"id": request_id},
        )
    ).one()
    assert isinstance(row.span, timedelta)
    return row.span


def test_start_uses_the_owning_agents_execution_deadline(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        await _set_agent_deadline(session, agent_id, 90)
        item = await _item(session, agent_id)
        running = await _start(session, await _request(session, item.work_item))
        assert running.request is not None
        assert await _stored_deadline_span(session, running.request.id) == timedelta(seconds=90)

    with_session(body)


def test_start_uses_1800_when_the_agent_deadline_is_null(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        # Another agent's override must not leak into this one's start.
        await _set_agent_deadline(session, await _agent(session, "other-bot"), 90)
        await _set_agent_deadline(session, agent_id, None)
        item = await _item(session, agent_id)
        running = await _start(session, await _request(session, item.work_item))
        assert running.request is not None
        assert await _stored_deadline_span(session, running.request.id) == timedelta(seconds=1800)

    with_session(body)


@pytest.mark.parametrize("seconds", [59, 10801])
def test_agent_execution_deadline_column_check_refuses_out_of_range(
    clean_db: None, seconds: int
) -> None:
    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        with pytest.raises(IntegrityError):
            await _set_agent_deadline(session, agent_id, seconds)

    with_session(body)


async def _insert_running_with_span(session: AsyncSession, seconds: int) -> None:
    agent_id = await _agent(session)
    item_id = uuid.uuid4()
    started_at = await _now(session)
    await session.execute(
        text(
            "INSERT INTO curie.work_items "
            "(id, github_repository_id, github_issue_number, "
            "github_installation_id, agent_id, repo_full_name, conversation_id, "
            "version, next_sequence) VALUES "
            "(:id, 101, 2573, 202, :agent, :repo, :conversation, 2, 2)"
        ),
        {"id": item_id, "agent": agent_id, "repo": REPO, "conversation": CONVERSATION},
    )
    await session.execute(
        text(
            "INSERT INTO curie.execution_requests "
            "(id, work_item_id, sequence, status, wait_deadline, started_at, "
            "execution_deadline, version, execution_attempts) VALUES "
            "(:id, :item, 1, 'running', :wait, :started, :deadline, 2, 1)"
        ),
        {
            "id": uuid.uuid4(),
            "item": item_id,
            "wait": started_at - timedelta(seconds=1),
            "started": started_at,
            "deadline": started_at + timedelta(seconds=seconds),
        },
    )
    await session.commit()


@pytest.mark.parametrize("seconds", [90, 10800])
def test_execution_request_check_accepts_deadlines_up_to_10800(
    clean_db: None, seconds: int
) -> None:
    with_session(lambda session: _insert_running_with_span(session, seconds))


def test_execution_request_check_refuses_a_deadline_past_10800(clean_db: None) -> None:
    async def body(session: AsyncSession) -> None:
        with pytest.raises(IntegrityError) as excinfo:
            await _insert_running_with_span(session, 10801)
        assert "execution_requests_deadline_ck" in str(excinfo.value)

    with_session(body)


# --- owner_lost successor (ADR 0206, #4168) ---------------------------------

SNAPSHOT = {
    "objective": "Fix the flaky retry test",
    "requester": "github:6601:octocat",
    "reply_kind": "github",
    "reply_address": REPO,
    "reply_conversation_id": "issue-2573",
}
SNAPSHOT_COLUMNS = tuple(SNAPSHOT)
PR_OBJECTIVE = (
    f"https://github.com/{REPO}/pull/123#discussion_r88201\n\n"
    "Review feedback asked for another revision."
)


async def _set_snapshot(
    session: AsyncSession, request_id: uuid.UUID, **overrides: str
) -> dict[str, str]:
    """Write the dispatch snapshot once, as intake does after admission."""

    snapshot = SNAPSHOT | overrides
    await session.execute(
        text(
            "UPDATE curie.execution_requests SET objective = :objective, "
            "requester = :requester, reply_kind = :reply_kind, "
            "reply_address = :reply_address, "
            "reply_conversation_id = :reply_conversation_id WHERE id = :id"
        ),
        {"id": request_id, **snapshot},
    )
    await session.commit()
    return snapshot


async def _versions(session: AsyncSession, request_id: uuid.UUID) -> Any:
    return (
        (
            await session.execute(
                text(
                    "SELECT r.work_item_id, w.version AS work_version, "
                    "r.version AS request_version, r.status "
                    "FROM curie.execution_requests r "
                    "JOIN curie.work_items w ON w.id = r.work_item_id WHERE r.id = :id"
                ),
                {"id": request_id},
            )
        )
        .mappings()
        .one()
    )


async def _start_by_id(session: AsyncSession, request_id: uuid.UUID) -> None:
    row = await _versions(session, request_id)
    started = await workitems.start_execution(
        session,
        work_item_id=row.work_item_id,
        request_id=request_id,
        expected_work_item_version=row.work_version,
        expected_request_version=row.request_version,
    )
    assert isinstance(started, workitems.WorkItemOutcome), started


async def _mark_owner_lost(
    session: AsyncSession, request_id: uuid.UUID
) -> workitems.WorkItemOutcome:
    """Start the request if needed, lapse its heartbeat and declare it owner_lost."""

    if (await _versions(session, request_id)).status == "waiting":
        await _start_by_id(session, request_id)
    await session.execute(
        text(
            "UPDATE curie.execution_requests SET runtime_owner = 'worker-a', "
            "runtime_epoch = GREATEST(runtime_epoch, 1), "
            "runtime_heartbeat_expires_at = :expired WHERE id = :id"
        ),
        {
            "id": request_id,
            "expired": await _now(session)
            - timedelta(seconds=get_settings().work_item_runtime_ttl_seconds + 5),
        },
    )
    await session.commit()
    row = await _versions(session, request_id)
    marked = await workitems.request_owner_lost_cancellation(
        session,
        work_item_id=row.work_item_id,
        request_id=request_id,
        expected_work_item_version=row.work_version,
        expected_request_version=row.request_version,
    )
    assert isinstance(marked, workitems.WorkItemOutcome), marked
    assert marked.request is not None
    assert (marked.request.status, marked.request.terminal_cause) == (
        "cancellation_requested",
        "owner_lost",
    )
    return marked


async def _observe(session: AsyncSession, request_id: uuid.UUID) -> workitems.WorkItemOutcome:
    """Record the termination observation the worker's teardown reports."""

    row = await _versions(session, request_id)
    observed = await workitems.record_runtime_termination(
        session,
        work_item_id=row.work_item_id,
        request_id=request_id,
        termination_observation=FIXTURE_TERMINATION,
        expected_work_item_version=row.work_version,
        expected_request_version=row.request_version,
    )
    assert isinstance(observed, workitems.WorkItemOutcome), observed
    assert observed.request is not None
    return observed


async def _lose(session: AsyncSession, request_id: uuid.UUID) -> None:
    await _mark_owner_lost(session, request_id)
    observed = await _observe(session, request_id)
    assert observed.request is not None
    assert (observed.request.status, observed.request.terminal_cause) == (
        "failed",
        "owner_lost",
    )


async def _requests(session: AsyncSession, work_item_id: uuid.UUID) -> list[Any]:
    return list(
        (
            await session.execute(
                text(
                    "SELECT id, sequence, status, terminal_cause, wait_deadline, "
                    f"{', '.join(SNAPSHOT_COLUMNS)} FROM curie.execution_requests "
                    "WHERE work_item_id = :id ORDER BY sequence"
                ),
                {"id": work_item_id},
            )
        )
        .mappings()
        .all()
    )


async def _owner_lost_retry(session: AsyncSession, request_id: uuid.UUID) -> bool:
    value = await session.scalar(
        text("SELECT owner_lost_retry FROM curie.execution_requests WHERE id = :id"),
        {"id": request_id},
    )
    assert isinstance(value, bool)
    return value


async def _snapshotted_waiting(
    session: AsyncSession, agent_id: uuid.UUID
) -> tuple[uuid.UUID, uuid.UUID]:
    waiting = await _request(session, (await _item(session, agent_id)).work_item)
    assert waiting.request is not None
    await _set_snapshot(session, waiting.request.id)
    return waiting.work_item.id, waiting.request.id


def test_an_owner_lost_request_is_followed_by_one_successor(clean_db: None) -> None:
    """AC1: same transaction, sequence + 1, copied snapshot, fresh wait budget."""

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, lost_id = await _snapshotted_waiting(session, agent_id)
        await _seed_transcript(session, agent_id, CONVERSATION)
        await _mark_owner_lost(session, lost_id)

        budget = timedelta(seconds=get_settings().work_item_wait_budget_seconds)
        before = await _now(session)
        await _observe(session, lost_id)
        after = await _now(session)

        rows = await _requests(session, item_id)
        lost, *successors = rows
        assert (lost.id, lost.status, lost.terminal_cause) == (lost_id, "failed", "owner_lost")
        assert len(successors) == 1, rows
        (successor,) = successors
        assert successor.id != lost_id
        assert (successor.sequence, successor.status, successor.terminal_cause) == (
            lost.sequence + 1,
            "waiting",
            None,
        )
        assert (
            {c: successor[c] for c in SNAPSHOT_COLUMNS}
            == {c: lost[c] for c in SNAPSHOT_COLUMNS}
            == SNAPSHOT
        )
        assert before + budget <= successor.wait_deadline <= after + budget
        assert await _owner_lost_retry(session, successor.id) is True
        assert await _owner_lost_retry(session, lost_id) is False
        # The WorkItem continues, so its history survives (ADR 0170).
        assert await _transcript_threads(session, agent_id) == {CONVERSATION}

    with_session(body)


def test_the_third_consecutive_owner_lost_admits_no_successor(clean_db: None) -> None:
    """AC2: losses one and two are retried; the third leaves the WorkItem terminal."""

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, current = await _snapshotted_waiting(session, agent_id)
        await _seed_transcript(session, agent_id, CONVERSATION)
        for loss in (1, 2):
            await _lose(session, current)
            rows = await _requests(session, item_id)
            assert len(rows) == loss + 1, rows
            successor = rows[-1]
            assert (successor.status, successor.sequence) == ("waiting", loss + 1)
            assert {c: successor[c] for c in SNAPSHOT_COLUMNS} == SNAPSHOT
            assert await _owner_lost_retry(session, successor.id) is True
            assert await _transcript_threads(session, agent_id) == {CONVERSATION}
            current = successor.id

        await _lose(session, current)
        rows = await _requests(session, item_id)
        assert [(r.sequence, r.status, r.terminal_cause) for r in rows] == [
            (1, "failed", "owner_lost"),
            (2, "failed", "owner_lost"),
            (3, "failed", "owner_lost"),
        ]
        # Nothing continues the WorkItem now, so its history expires.
        assert await _transcript_threads(session, agent_id) == set()

    with_session(body)


def test_a_different_terminal_between_losses_resets_the_owner_lost_count(
    clean_db: None,
) -> None:
    """AC2: only consecutive owner_lost terminals count toward the limit."""

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, first = await _snapshotted_waiting(session, agent_id)
        await _lose(session, first)
        rows = await _requests(session, item_id)
        assert len(rows) == 2, rows
        await _lose(session, rows[-1].id)
        rows = await _requests(session, item_id)
        assert len(rows) == 3, rows
        third = rows[-1]
        assert third.status == "waiting", third

        # The retried run fails for its own reason: the streak ends here.
        await _start_by_id(session, third.id)
        row = await _versions(session, third.id)
        failed = await workitems.fail_execution(
            session,
            work_item_id=item_id,
            request_id=third.id,
            cause="runner_escalated",
            expected_work_item_version=row.work_version,
            expected_request_version=row.request_version,
        )
        assert isinstance(failed, workitems.WorkItemOutcome), failed
        assert len(await _requests(session, item_id)) == 3

        fresh_id = uuid.uuid4()
        fresh = await workitems.create_execution_request(
            session,
            work_item_id=item_id,
            request_id=fresh_id,
            wait_deadline=await _now(session) + timedelta(hours=1),
            expected_work_item_version=(await _versions(session, third.id)).work_version,
        )
        assert isinstance(fresh, workitems.WorkItemOutcome), fresh
        await _set_snapshot(session, fresh_id)
        # Three owner_lost terminals on the WorkItem, but only one in a row.
        await _lose(session, fresh_id)

        rows = await _requests(session, item_id)
        assert [(r.sequence, r.status, r.terminal_cause) for r in rows] == [
            (1, "failed", "owner_lost"),
            (2, "failed", "owner_lost"),
            (3, "failed", "runner_escalated"),
            (4, "failed", "owner_lost"),
            (5, "waiting", None),
        ]
        assert await _owner_lost_retry(session, rows[-1].id) is True

    with_session(body)


def test_issue_cancel_during_owner_lost_admits_no_successor(clean_db: None) -> None:
    """AC3: a cancelled WorkItem; the cancel rewrites the cause to issue_cancelled."""

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, lost_id = await _snapshotted_waiting(session, agent_id)
        marked = await _mark_owner_lost(session, lost_id)
        cancelled = await workitems.request_cancellation(
            session, work_item_id=item_id, expected_work_item_version=marked.work_item.version
        )
        assert isinstance(cancelled, workitems.WorkItemOutcome), cancelled
        assert cancelled.request is not None
        assert cancelled.request.terminal_cause == "issue_cancelled"
        observed = await _observe(session, lost_id)
        assert observed.request is not None
        assert (observed.request.status, observed.request.terminal_cause) == (
            "cancelled",
            "issue_cancelled",
        )
        assert observed.work_item.cancelled_at is not None
        assert [r.id for r in await _requests(session, item_id)] == [lost_id]

    with_session(body)


def test_a_pending_relabel_wins_over_an_owner_lost_successor(clean_db: None) -> None:
    """AC3: the relabel is admitted by its own path; no retry is added beside it."""

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, lost_id = await _snapshotted_waiting(session, agent_id)
        await _mark_owner_lost(session, lost_id)
        relabel_id = uuid.uuid4()
        deferred = await workitems.readmit(
            session,
            work_item_id=item_id,
            request_id=relabel_id,
            wait_deadline=await _now(session) + timedelta(hours=1),
            objective="Fix the flaky retry test, again",
            requester="github:6601:octocat",
        )
        assert isinstance(deferred, workitems.WorkItemOutcome), deferred
        assert (
            await session.scalar(
                text("SELECT readmit_request_id FROM curie.work_items WHERE id = :id"),
                {"id": item_id},
            )
            == relabel_id
        )
        observed = await _observe(session, lost_id)
        assert observed.request is not None
        assert (observed.request.status, observed.request.terminal_cause) == (
            "cancelled",
            "issue_cancelled",
        )
        assert [r.id for r in await _requests(session, item_id)] == [lost_id]

        admitted = await workitems.admit_pending_readmit(
            session,
            work_item_id=item_id,
            wait_deadline=await _now(session) + timedelta(hours=1),
        )
        assert isinstance(admitted, workitems.WorkItemOutcome), admitted
        rows = await _requests(session, item_id)
        assert [(r.id, r.status) for r in rows] == [
            (lost_id, "cancelled"),
            (relabel_id, "waiting"),
        ]
        assert rows[1].objective == "Fix the flaky retry test, again"

    with_session(body)


def test_an_execution_deadline_expiry_admits_no_successor(clean_db: None) -> None:
    """AC3: only owner_lost is retried; an elapsed run settles expired."""

    async def body(session: AsyncSession) -> None:
        item_id, request_id, _, _ = await _elapsed_running(session, await _agent(session))
        await _set_snapshot(session, request_id)
        marked = await workitems.request_execution_deadline_cancellation(
            session,
            work_item_id=item_id,
            request_id=request_id,
            expected_work_item_version=2,
            expected_request_version=2,
        )
        assert isinstance(marked, workitems.WorkItemOutcome), marked
        observed = await _observe(session, request_id)
        assert observed.request is not None
        assert (observed.request.status, observed.request.terminal_cause) == (
            "expired",
            "execution_deadline",
        )
        assert [r.id for r in await _requests(session, item_id)] == [request_id]

    with_session(body)


@pytest.mark.parametrize(
    ("lineage", "retried"),
    [("missing", False), ("closed", False), ("other_pr", False), ("open", True)],
)
def test_a_pr_targeted_owner_lost_is_retried_only_on_its_open_lineage(
    clean_db: None, lineage: str, retried: bool
) -> None:
    """AC3: the admit_next_revision lineage rule; an open matching lineage retries."""

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        lineage_id = (
            None
            if lineage == "missing"
            else await _lineage(
                session,
                agent_id,
                pr=124 if lineage == "other_pr" else 123,
                status="closed" if lineage == "closed" else "open",
            )
        )
        item_id, request_id, _, _ = await _elapsed_running(session, agent_id, lineage_id=lineage_id)
        await _set_snapshot(session, request_id, objective=PR_OBJECTIVE)
        await _lose(session, request_id)

        rows = await _requests(session, item_id)
        if not retried:
            assert [r.id for r in rows] == [request_id]
            return
        assert len(rows) == 2, rows
        successor = rows[1]
        assert (successor.status, successor.objective) == ("waiting", PR_OBJECTIVE)
        assert await _owner_lost_retry(session, successor.id) is True

    with_session(body)


async def _attach_own_publication(
    session: AsyncSession, work_item_id: uuid.UUID, request_id: uuid.UUID, *, status: str
) -> None:
    """Give the request its own publication row on a fresh lineage, in ``status``."""

    item = (
        (
            await session.execute(
                text(
                    "SELECT agent_id, conversation_id, repo_full_name "
                    "FROM curie.work_items WHERE id = :id"
                ),
                {"id": work_item_id},
            )
        )
        .mappings()
        .one()
    )
    lineage_id = await _lineage(session, item["agent_id"], conversation=item["conversation_id"])
    deployment_id = await session.scalar(
        text("SELECT deployment_id FROM curie.thread_publication_lineages WHERE id = :id"),
        {"id": lineage_id},
    )
    approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.approvals "
            "(id, agent_id, conversation_id, author, summary, reply_kind, "
            "reply_channel, dedupe_key, status, purpose) VALUES "
            "(:id, :agent, :conversation, 'U0REQUEST1', "
            "'Publish repository changes', 'github', :channel, :dedupe, "
            "'approved', 'publication')"
        ),
        {
            "id": approval_id,
            "agent": item["agent_id"],
            "conversation": item["conversation_id"],
            "channel": item["repo_full_name"],
            "dedupe": f"owner-lost-publication-{publication_id.hex}",
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.publications "
            "(id, approval_id, deployment_id, workspace_conversation_id, "
            "lineage_id, execution_request_id, revision_number, repo_full_name, "
            "status, base_sha, changed_paths, title, body, reply_kind, reply_channel) "
            "VALUES "
            "(:id, :approval, :deployment, :conversation, :lineage, :request, 1, "
            ":repo, :status, :base_sha, CAST('[\"README.md\"]' AS jsonb), "
            "'Update README', 'Approved platform publication.', 'github', :channel)"
        ),
        {
            "id": publication_id,
            "approval": approval_id,
            "deployment": deployment_id,
            "conversation": item["conversation_id"],
            "lineage": lineage_id,
            "request": request_id,
            "repo": item["repo_full_name"],
            "status": status,
            "channel": item["repo_full_name"],
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
        },
    )
    await session.commit()


@pytest.mark.parametrize("publication_status", ["pending", "approved", "launching", "running"])
def test_an_owner_lost_request_waiting_on_its_publication_admits_no_successor(
    clean_db: None, publication_status: str
) -> None:
    """ADR 0206 item 6: a request waiting on publication is never re-admitted.

    The no-publication neighbour is AC1 above, which must still retry.
    """

    async def body(session: AsyncSession) -> None:
        agent_id = await _agent(session)
        item_id, lost_id = await _snapshotted_waiting(session, agent_id)
        await _start_by_id(session, lost_id)
        await _attach_own_publication(session, item_id, lost_id, status=publication_status)

        await _lose(session, lost_id)

        rows = await _requests(session, item_id)
        assert [(r.id, r.status, r.terminal_cause) for r in rows] == [
            (lost_id, "failed", "owner_lost")
        ], rows

    with_session(body)
