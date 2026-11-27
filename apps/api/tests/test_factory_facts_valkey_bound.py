"""The factory facts delivery's Valkey client is bounded (#5995).

One owed request row in the real test Postgres is delivered through the real
``FactsExchange``, whose Valkey address is a loopback port that accepts and
never answers. The tracker is a minimal in-memory ``TrackerFacts`` that lets
the delivery reach ``read_facts``; the agent bound by the ``facts_world``
fixture makes ``read_facts`` ask Valkey for its kill switch and for the
upgrade pause, both while the delivery holds a database session.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

from _tcp_blackhole import silent_listener, within
from curie_api.factory_facts_exchange import FactsExchange, _answer_facts
from curie_api.factory_facts_schema import REQUEST_MARKER, parse_request
from curie_api.forges.types import (
    Actor,
    MarkedComment,
    ReplyTarget,
    RepositoryRef,
    TrackerComment,
    TrackerIssueRef,
    UpsertResult,
)
from curie_api.models import FactoryFactsRequest
from forge_fakes.facts import facts_world as facts_world
from forge_fakes.github import LABEL, REPO, REPO_ID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

ISSUE = 42
REQUEST_ID = 101


@dataclass
class OneRequestTracker:
    """Serves one request comment by a writer and records every answer written."""

    repository: RepositoryRef
    comment: TrackerComment
    written: list[str] = field(default_factory=list)

    @property
    def marked_comments(self) -> OneRequestTracker:
        return self

    async def read_comment(self, comment_id: int) -> TrackerComment:
        assert comment_id == self.comment.id
        return self.comment

    async def facts_issue_is_plain(self, issue: TrackerIssueRef) -> bool:
        return True

    async def facts_writer(self, comment: TrackerComment) -> Actor | None:
        return comment.actor

    async def find_facts(self, target: ReplyTarget, prefix: str) -> MarkedComment | None:
        return None

    async def upsert_facts(
        self, target: ReplyTarget, *, prefix: str, marker: str, body: str
    ) -> UpsertResult:
        self.written.append(body)
        return UpsertResult(MarkedComment("9001", target, body), True)


def test_facts_delivery_against_a_silent_valkey_finishes_within_the_bound(
    facts_world: Any,
) -> None:
    world = facts_world
    repository = RepositoryRef("github", "github.com", str(REPO_ID), REPO)
    body = (
        "@curie-factory-bot\n\n"
        + REQUEST_MARKER
        + "\n```json\n"
        + json.dumps(
            {"schema_version": 1, "trigger_label": LABEL, "executables": [], "services": []}
        )
        + "\n```"
    )
    comment = TrackerComment(
        id=REQUEST_ID,
        issue=TrackerIssueRef("github", "github.com", str(REPO_ID), str(ISSUE)),
        body=body,
        actor=Actor("77", "dana"),
        via_app=False,
        updated_at=None,
    )
    tracker = OneRequestTracker(repository, comment)

    async def check() -> None:
        engine = create_async_engine(world.settings.database_url, pool_size=2, max_overflow=0)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                session.add(
                    FactoryFactsRequest(
                        tracker_host=repository.host,
                        repository_id=repository.project_id,
                        issue_number=ISSUE,
                        trigger_label=LABEL,
                        request_comment_id=REQUEST_ID,
                        digest=parse_request(body).digest(),
                        actor_id="77",
                        state="owed",
                    )
                )
                await session.commit()
            async with silent_listener() as listener:
                settings = world.settings.model_copy(
                    update={
                        "valkey_url": f"redis://127.0.0.1:{listener.port}/0",
                        "valkey_socket_timeout_seconds": 0.5,
                        "valkey_socket_connect_timeout_seconds": 0.5,
                    }
                )
                exchange = FactsExchange(sessions, settings, tracker)
                started = time.monotonic()
                # Well under redis-py 8.1.0's own default socket timeout of 5 s.
                async with within(3, "a facts delivery against a silent Valkey"):
                    await exchange.deliver_owed()
                assert time.monotonic() - started < 3
                assert listener.accepted.is_set()
            assert engine.pool.checkedout() == 0  # type: ignore[attr-defined]
            # read_facts reports an unreadable Valkey as unknown and the
            # delivery completes with that answer.
            assert len(tracker.written) == 1
            answer = _answer_facts(tracker.written[0])
            assert answer is not None
            assert answer.factory.kill_switch == "unknown"
            assert answer.factory.upgrade_pause == "unknown"
            async with sessions() as session:
                row = await session.scalar(select(FactoryFactsRequest))
                assert row is not None and row.state == "answered"
        finally:
            await engine.dispose()

    asyncio.run(check())
