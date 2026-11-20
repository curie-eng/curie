"""Agent deletion and a learned rule's proposal or settlement serialize instead of
deadlocking (#5796, ADR 0287).

Agent deletion locks the agent row ``FOR UPDATE`` first and then reaches the
WorkItems, requests, rule rows and lineages through its deletes and cascades.
A proposal and a terminal lineage writer therefore take the agent row ``FOR
KEY SHARE`` before their request and lineage locks, and the settlement itself
takes no agent lock (``memory_rule_lifecycle`` lock order). Each test holds
the proposal or the terminal writer at the point where it owns its locks,
starts the real ``crud.agents.delete_agent``, observes through
``pg_blocking_pids`` (no sleeps) that the deletion waits for it, then lets it
finish: both commit, and no ``DeadlockDetected`` is raised. In the earlier
order the deletion took the agent row and then waited on the proposal's
request (or the writer's lineage), while the proposal's insert (or the
settlement's agent lock) waited on the agent row.

The core functions are called directly, as the route and the reconciler call
them, because the TestClient cannot interleave with an open transaction.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Awaitable
from pathlib import Path
from typing import Any

import pytest
from curie_api import memory_rule_lifecycle
from curie_api.config import get_settings
from curie_api.crud import agents as crud_agents
from curie_api.crud import lineages as crud_lineages
from curie_api.forges.types import PullRequestState
from curie_api.lineage_reconciler import LineageReconciler, PassResult
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_github_factory_ingress import _rows  # noqa: E402
from test_memory_rule_proposal import (  # noqa: E402
    REASON,
    RULE,
    PullStates,
    ReviewGitHubAPI,
    eligible_revision,
    propose,
    rules,  # noqa: F401  (fixture)
    stored_events,
    stored_rules,
)


async def _paused(paused: asyncio.Event, work: asyncio.Task[Any]) -> None:
    """Wait until ``work`` reaches its pause; fail if it ends without pausing."""

    waiting = asyncio.ensure_future(paused.wait())
    await asyncio.wait({waiting, work}, return_when=asyncio.FIRST_COMPLETED)
    if not paused.is_set():
        waiting.cancel()
        await work
        pytest.fail("the proposal or settlement ended before its pause")


async def _blocked_by(
    observer: AsyncConnection, waiter: int, holder: int, waiting: Awaitable[Any]
) -> None:
    """Return once ``waiter``'s backend waits on ``holder``'s locks."""

    task = asyncio.ensure_future(waiting)
    # Each observation is a database round trip, which yields to the task;
    # the bound only stops a broken run.
    for _ in range(10_000):
        blockers = await observer.scalar(text("SELECT pg_blocking_pids(:pid)"), {"pid": waiter})
        if holder in (blockers or []):
            return
        assert not task.done(), "the deletion finished without waiting"
    pytest.fail("the deletion never waited on the held locks")


async def _pid(session: AsyncSession) -> int:
    return int(await session.scalar(text("SELECT pg_backend_pid()")))


def _agent_rows(agent_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows("SELECT id FROM curie.agents WHERE id = :id", {"id": agent_id})


def test_agent_deletion_waits_for_a_proposal_holding_its_locks(
    rules: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = rules
    run = eligible_revision(client, api)
    paused, release = asyncio.Event(), asyncio.Event()
    original = memory_rule_lifecycle.rule_proposal_eligibility
    reads = 0

    async def recheck_pauses(session: AsyncSession, **kwargs: Any) -> Any:
        # The second read is the recheck under the proposal's own locks.
        nonlocal reads
        reads += 1
        if reads == 2:
            paused.set()
            await release.wait()
        return await original(session, **kwargs)

    monkeypatch.setattr(memory_rule_lifecycle, "rule_proposal_eligibility", recheck_pauses)

    async def scenario() -> None:
        engine = create_async_engine(get_settings().database_url)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with (
                factory() as proposer,
                factory() as deleter,
                engine.connect() as observer,
            ):
                proposer_pid, deleter_pid = await _pid(proposer), await _pid(deleter)
                proposal = asyncio.create_task(
                    memory_rule_lifecycle.propose_candidate(
                        proposer,
                        request_id=run.request_id,
                        work_item_id=run.work_item_id,
                        rule=RULE,
                        reason=REASON,
                    )
                )
                await _paused(paused, proposal)
                deletion = asyncio.create_task(crud_agents.delete_agent(deleter, run.agent_id))
                await _blocked_by(observer, deleter_pid, proposer_pid, deletion)
                release.set()
                await proposal
                await deletion
        finally:
            await engine.dispose()

    asyncio.run(scenario())

    # The proposal committed first; the deletion then removed the agent and
    # the candidate with it.
    assert reads == 2
    assert _agent_rows(run.agent_id) == []
    assert stored_rules() == []
    assert stored_events() == []


def test_agent_deletion_waits_for_a_terminal_write_settling_its_candidate(
    rules: tuple[TestClient, ReviewGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = rules
    run = eligible_revision(client, api)
    stored = propose(client, run.capability, {"rule": RULE, "reason": REASON})
    assert stored.status_code == 200, stored.text
    paused, release = asyncio.Event(), asyncio.Event()
    writer: list[int] = []
    original = crud_lineages.settle_lineage_rule_candidates

    async def settle_pauses(session: AsyncSession, lineage_id: uuid.UUID, state: str) -> int:
        # The terminal writer has written the lineage and holds its locks.
        writer.append(await _pid(session))
        paused.set()
        await release.wait()
        return await original(session, lineage_id, state)

    monkeypatch.setattr(crud_lineages, "settle_lineage_rule_candidates", settle_pauses)

    async def scenario() -> PassResult:
        engine = create_async_engine(get_settings().database_url)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as deleter, engine.connect() as observer:
                deleter_pid = await _pid(deleter)
                reconciler = LineageReconciler(
                    factory,
                    get_settings(),
                    lambda _repository: PullStates(  # type: ignore[arg-type, return-value]
                        {run.pr: PullRequestState.MERGED}
                    ),
                    interval_seconds=60,
                )
                reconcile = asyncio.create_task(reconciler.run_once())
                await _paused(paused, reconcile)
                deletion = asyncio.create_task(crud_agents.delete_agent(deleter, run.agent_id))
                await _blocked_by(observer, deleter_pid, writer[0], deletion)
                release.set()
                result = await reconcile
                await deletion
                return result
        finally:
            await engine.dispose()

    result = asyncio.run(scenario())

    # The merge was recorded and settled first; the deletion then removed the
    # agent, its lineage and its rule.
    assert result.merged == 1
    assert _agent_rows(run.agent_id) == []
    assert stored_rules() == []
    assert stored_events() == []
