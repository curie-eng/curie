"""Resume reconciler (#411): the backstop that re-enqueues owed wakes.

A resolved approval whose resume enqueue failed is left ``resolved_at`` set but
``resumed_at`` NULL -- a stranded suspended session. ``ResumeReconciler`` sweeps
those rows on an interval and re-enqueues the resume turn onto the runs stream,
setting ``resumed_at`` only AFTER a successful enqueue (enqueue-first-then-mark),
so a failed enqueue is retried on the next pass rather than lost.

Real Postgres + real Valkey from the compose dev stack -- never mocked; the only
injected failure is a wrapped ``enqueue`` that raises for a chosen record. Every
assertion is on a real outcome: the deterministic ``event_id`` present/absent on
the actual stream, the ``resumed_at`` NULL<->set transition read back from the
DB, and ``reconcile_once()``'s return count.

The ``runs_stream``/``valkey`` fixtures are duplicated from ``test_approvals.py``
on purpose: pytest fixtures are module-local, and copying them keeps that file's
definitions untouched (true verbatim reuse of behavior) without hoisting
Valkey-specific setup into the shared conftest.
"""

import asyncio
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta

import pytest
import redis
import redis.asyncio as aioredis
from aci_protocol import QueuedTurn
from curie_api.config import get_settings
from curie_api.models import Approval, ApprovalStatus
from curie_api.resumequeue import ResumeQueue
from curie_api.resumereconciler import ResumeReconciler
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_test_support.valkey import (
    connect_or_skip,
)
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


@pytest.fixture
def runs_stream() -> Iterator[str]:
    """A per-test runs stream so reconciler enqueues never feed the shared
    compose worker's real ``curie:runs`` consumer group."""

    name = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = name
    get_settings.cache_clear()
    yield name
    os.environ.pop("RUNS_STREAM", None)
    get_settings.cache_clear()


@pytest.fixture
def valkey(runs_stream: str) -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.delete(runs_stream)
    # #532's graveyard-scan tests write to the dead-letter stream too; clean it
    # between tests so a persisted row never leaks into the next test's scan.
    client.delete(runs_stream + ":dead")
    client.close()


def _naive(seconds_ago: float = 0.0) -> datetime:
    """Naive UTC ``now - seconds_ago``, matching the DateTime columns."""

    return datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=seconds_ago)


def _run_async[T](
    steps: Callable[[async_sessionmaker[AsyncSession], ResumeQueue], Awaitable[T]],
    stream: str,
) -> T:
    """Drive an async ``steps(sessionmaker, queue)`` with fresh loop-bound
    resources. A fresh engine + async Valkey client (not the app's, which are
    bound to the TestClient portal loop) makes ``asyncio.run`` from the sync
    test body safe -- the same pattern conftest's ``_truncate`` uses."""

    async def _main() -> T:
        engine = create_async_engine(get_settings().database_url)
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        client = aioredis.Redis(
            host=_VALKEY_HOST, port=_VALKEY_PORT, password=_VALKEY_PW or None
        )
        queue = ResumeQueue(client, stream=stream)
        try:
            return await steps(sessionmaker, queue)
        finally:
            await client.aclose()
            await engine.dispose()

    return asyncio.run(_main())


def _detached_approval(
    status: str,
    *,
    resolved_by: str | None = "U9",
    reply_kind: str = "slack",
    reply_adapter: str | None = None,
    reply_placeholder: str | None = "p-1",
) -> Approval:
    """An unpersisted ``Approval`` in a chosen status, for the pure-unit selector
    test: the turn builders read the record's fields only, so no DB round-trip is
    needed to exercise the status -> builder mapping.

    Also the single construction site for ``_insert_approval`` below, so a new
    non-nullable column on ``Approval`` is added once, and the record the unit
    test exercises cannot drift from the one the DB tests insert.
    """

    return Approval(
        id=uuid.uuid4(),
        conversation_id=f"th-{uuid.uuid4().hex[:8]}",
        author="U1",
        summary="Give ACME a 20% discount",
        # The routing half of the durable record (ADR-0096 phase 2, EB-A7). NOT
        # NULL with no server_default, so every construction site states it: an
        # approval that cannot say which channel raised it cannot be resumed to
        # the right one.
        reply_kind=reply_kind,
        reply_channel="C1",
        reply_placeholder=reply_placeholder,
        reply_adapter=reply_adapter,
        dedupe_key=uuid.uuid4().hex,
        status=status,
        resolved_by=resolved_by,
    )


async def _insert_approval(
    sessionmaker: async_sessionmaker[AsyncSession],
    *,
    status: str,
    resolved_at: datetime | None,
    resumed_at: datetime | None,
    resolved_by: str | None = "U9",
    reply_endpoint: str | None = None,
    reply_kind: str = "slack",
    reply_channel: str | None = None,
    reply_adapter: str | None = None,
    reply_placeholder: str | None = "p-1",
) -> uuid.UUID:
    """Insert an approval row in a chosen lifecycle state (bypassing the resolve
    endpoint), so a test can construct the exact stranded/settled/expired shapes
    the reconciler must include or exclude."""

    approval = _detached_approval(
        status,
        resolved_by=resolved_by,
        reply_kind=reply_kind,
        reply_adapter=reply_adapter,
        reply_placeholder=reply_placeholder,
    )
    approval.reply_endpoint = reply_endpoint
    if reply_channel is not None:
        approval.reply_channel = reply_channel
    approval.resolved_at = resolved_at
    approval.resumed_at = resumed_at

    async with sessionmaker() as session:
        session.add(approval)
        await session.commit()
        await session.refresh(approval)
        return approval.id


async def _resumed_at(
    sessionmaker: async_sessionmaker[AsyncSession], approval_id: uuid.UUID
) -> datetime | None:
    async with sessionmaker() as session:
        approval = await session.get(Approval, approval_id)
        assert approval is not None
        return approval.resumed_at


def test_reconciler_reenqueues_stranded_resolved_record(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A resolved, past-grace, unresumed record is re-enqueued exactly once and
    marked resumed. This is the core AC1 outcome."""

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[uuid.UUID, int, datetime | None]:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
            reply_placeholder="reply placeholder",
        )
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=0,
            batch_limit=100,
        )
        count = await reconciler.reconcile_once()
        return approval_id, count, await _resumed_at(sessionmaker, approval_id)

    approval_id, count, resumed_at = _run_async(steps, runs_stream)

    assert count == 1
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{approval_id}-resolved"
    assert turn.reply_handle.placeholder == "reply placeholder"
    assert resumed_at is not None


def test_reconciler_preserves_a_post_once_reply_handle(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A stranded post once approval requeues without inventing a placeholder."""

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[uuid.UUID, int]:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
            reply_endpoint="http://localhost:9999/api/",
            reply_placeholder=None,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        return approval_id, await reconciler.reconcile_once()

    approval_id, count = _run_async(steps, runs_stream)

    assert count == 1
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{approval_id}-resolved"
    assert turn.reply_handle.channel == "C1"
    assert turn.reply_handle.placeholder is None
    assert turn.reply_handle.endpoint == "http://localhost:9999/api/"


def test_reconciler_skips_already_resumed_record(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A record whose wake was already delivered (``resumed_at`` set) is off the
    work-list -- no re-enqueue, no double wake."""

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, datetime | None]:
        resolved = _naive(120)
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=resolved,
            resumed_at=resolved,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        count = await reconciler.reconcile_once()
        return count, await _resumed_at(sessionmaker, approval_id)

    count, resumed_at = _run_async(steps, runs_stream)

    assert count == 0
    assert valkey.xrange(runs_stream) == []
    assert resumed_at is not None


async def _seed_binding(
    sessionmaker: async_sessionmaker[AsyncSession], *, kind: str, address: str
) -> uuid.UUID:
    """One agent and its `agent_channels` binding, written directly.

    Used only by the re-point test below: the point is to move a LIVE binding
    out from under a suspended approval, which needs a real row for the rejected
    lookup-at-resume design to have found.
    """

    agent_id = uuid.uuid4()
    async with sessionmaker() as session:
        await session.execute(
            text("INSERT INTO curie.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": f"agent-{agent_id.hex[:8]}"},
        )
        await session.execute(
            text(
                "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
                "VALUES (:id, :agent, :kind, :addr)"
            ),
            {"id": uuid.uuid4(), "agent": agent_id, "kind": kind, "addr": address},
        )
        await session.commit()
    return agent_id


async def _repoint_binding(
    sessionmaker: async_sessionmaker[AsyncSession], *, address: str, kind: str
) -> None:
    """Re-point a live binding to another kind, as `crud.update_channel_binding`
    does in place on an ordinary binding move."""

    async with sessionmaker() as session:
        await session.execute(
            text("UPDATE curie.agent_channels SET kind = :kind WHERE address = :addr"),
            {"kind": kind, "addr": address},
        )
        await session.commit()


def test_the_resume_carries_the_persisted_kind_after_the_binding_moved(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """T-A8 / AC3, THE decision-proving test (plan edge case E6).

    An approval is raised on an `email` binding and suspended. While it waits, an
    operator re-points that address at a `webhook` adapter -- an ordinary PATCH,
    not an exotic state. The resume must carry `kind == "email"`: the persisted
    value is a FACT ABOUT THE ORIGINAL TURN, and the human on the other end of
    that email thread is still waiting there.

    This is the test that fails under the rejected design. Replace
    `kind=approval.reply_kind` in `resumequeue._build_turn` with a fresh lookup
    against `agent_channels` and every other resume test still passes -- the
    lookup agrees with the record in every case except this one, which is
    precisely why it would have shipped.
    """

    address = "ops@example.test"

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> uuid.UUID:
        await _seed_binding(sessionmaker, kind="email", address=address)
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
            reply_kind="email",
            reply_channel=address,
            reply_adapter="agentmail-sandbox",
        )
        # The binding moves out from under the suspended approval.
        await _repoint_binding(sessionmaker, address=address, kind="webhook")

        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        assert await reconciler.reconcile_once() == 1
        return approval_id

    approval_id = _run_async(steps, runs_stream)

    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{approval_id}-resolved"
    assert turn.reply_handle.kind == "email", (
        "the resume re-derived the kind from the CURRENT binding; it must replay "
        "the kind persisted on the approval"
    )
    assert turn.reply_handle.channel == address


def test_both_resume_flavors_replay_the_adapter_from_the_record(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """T-A18 / AC10f (plan EB-A8, round-5 item 2).

    `_build_turn` is the SINGLE constructor for both resume flavors -- the
    resolve re-enqueue (`resumequeue.py:138`) and the expiry re-enqueue (`:166`)
    -- so a dropped `adapter` there loses the egress-credential selector for
    every resumed non-Slack turn at once, and only ever surfaces on a resumed
    email turn hitting the pre-resolution escalate path.

    Both flavors are driven in one test precisely because they share that
    constructor: asserting only the resolve flavor would leave a future split
    (say, an expiry-specific builder) free to drop it on the quieter lane.

    Mutation: drop `adapter=approval.reply_adapter` from `_build_turn` and BOTH
    assertions below fail.
    """

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[uuid.UUID, uuid.UUID]:
        resolved_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
            reply_kind="email",
            reply_channel="resolve@example.test",
            reply_endpoint="http://curie-mail-adapter:8080/",
            reply_adapter="agentmail-sandbox",
        )
        expired_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.expired,
            resolved_at=_naive(120),
            resumed_at=None,
            resolved_by=None,
            reply_kind="email",
            reply_channel="expiry@example.test",
            reply_endpoint="http://curie-mail-adapter:8080/",
            reply_adapter="agentmail-sandbox",
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        assert await reconciler.reconcile_once() == 2
        return resolved_id, expired_id

    resolved_id, expired_id = _run_async(steps, runs_stream)

    enqueued = [
        QueuedTurn.model_validate(json.loads(fields["payload"]))
        for _, fields in valkey.xrange(runs_stream)
    ]
    turns = {turn.event_id: turn for turn in enqueued}
    assert set(turns) == {
        f"approval-{resolved_id}-resolved",
        f"approval-{expired_id}-resolved",
    }

    for event_id, turn in turns.items():
        assert turn.reply_handle.kind == "email", event_id
        assert turn.reply_handle.adapter == "agentmail-sandbox", event_id
        assert turn.reply_handle.endpoint == "http://curie-mail-adapter:8080/", event_id


def test_a_slack_resume_replays_no_adapter(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """T-A18, the sibling lane. A Slack approval persists NULL and must replay
    NULL: fabricating a slug here would make the worker look up a credential for
    an adapter that does not own this turn, and D4.4 already says Slack's route
    is the worker's configured origin.
    """

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> uuid.UUID:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        assert await reconciler.reconcile_once() == 1
        return approval_id

    _run_async(steps, runs_stream)

    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.reply_handle.kind == "slack"
    assert turn.reply_handle.adapter is None


def test_reconciler_reenqueues_stranded_expired_record(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """#418: an expired record whose expiry wake never reached the stream is an
    owed wake, and the reconciler must deliver it -- with the EXPIRY turn.

    Since #412 both expiry paths (the sweeper and the resolve-path expiry branch)
    enqueue a wake, so ``status=expired, resolved_at`` set, ``resumed_at`` NULL
    means the enqueue failed and the session is stranded. It gets the same
    durable backstop a resolved record already had.

    ``resolved_by`` is None because expiry records no human decision. That is
    also why the builder choice is load-bearing rather than cosmetic: sending
    this row through ``build_resume_turn`` would author the wake as "approver"
    and tell the model the request "was expired by None". The text/author
    assertions below are what prove the expiry builder was dispatched.
    """

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[uuid.UUID, int, datetime | None]:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.expired,
            resolved_at=_naive(120),
            resumed_at=None,
            resolved_by=None,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        count = await reconciler.reconcile_once()
        return approval_id, count, await _resumed_at(sessionmaker, approval_id)

    approval_id, count, resumed_at = _run_async(steps, runs_stream)

    assert count == 1
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))

    # The shared deterministic key: the ``-resolved`` suffix is historical and
    # must NOT fork per status, or the worker's done-marker stops recognizing a
    # redelivery of the already-finished turn for this approval.
    assert turn.event_id == f"approval-{approval_id}-resolved"

    # The expiry turn, not the human-decision one.
    assert turn.text.startswith("[approval expired]")
    assert turn.author == "system"

    assert resumed_at is not None


def test_resume_turn_selector_domain_matches_resumable_statuses() -> None:
    """The status -> turn-builder mapping is total over the resumable statuses,
    and its domain cannot silently desync from ``crud._RESUMABLE_STATUSES``.

    The selector lives beside the builders while the finder and the per-row claim
    are fenced by crud's private tuple. Nothing in the type system ties the two
    together, so a future status added to the tuple would reach the selector and
    raise, or a status added to the selector would never be selected. The set
    equality below is the executable form of that invariant.

    Pure unit: no DB, no Valkey -- the selector is a function of the record.
    """

    from curie_api import crud
    from curie_api.resumequeue import resume_turn_for

    approved = resume_turn_for(_detached_approval(ApprovalStatus.approved))
    assert "[approval resolved]" in approved.text

    rejected = resume_turn_for(_detached_approval(ApprovalStatus.rejected))
    assert "[approval resolved]" in rejected.text

    expired = resume_turn_for(
        _detached_approval(ApprovalStatus.expired, resolved_by=None)
    )
    assert "[approval expired]" in expired.text
    assert expired.author == "system"

    # A pending record owes no wake: it has not been decided and has not lapsed.
    with pytest.raises(ValueError):
        resume_turn_for(_detached_approval(ApprovalStatus.pending, resolved_by=None))

    assert {s for s in ApprovalStatus if s is not ApprovalStatus.pending} == set(
        crud._RESUMABLE_STATUSES
    )


def test_reconciler_skips_pending_record(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A still-pending record (no ``resolved_at``) is never on the work-list."""

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> int:
        await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.pending,
            resolved_at=None,
            resumed_at=None,
            resolved_by=None,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        return await reconciler.reconcile_once()

    count = _run_async(steps, runs_stream)

    assert count == 0
    assert valkey.xrange(runs_stream) == []


def test_reconciler_ignores_backfilled_historical_row(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """Historical rows are backfilled to ``resumed_at = resolved_at``; the
    reconciler must treat them as settled and never auto-wake them after deploy.

    This pins the reconciler-level steady-state contract only: a row that
    already carries ``resumed_at`` -- whoever set it, migration 0011 (#411),
    migration 0012 (#418), or the runtime -- is never re-woken, because
    ``list_resolved_unresumed`` filters on ``resumed_at IS NULL``. That contract
    is what makes settling history sufficient; without it, the first pass after
    deploy would re-deliver stale wakes into long-finished threads (past the
    worker done-marker's 24h TTL an old wake is re-delivered, not absorbed, so it
    steers a thread that already moved on).

    It does NOT verify either migration's WHERE clause: the rows here are
    hand-inserted already carrying ``resumed_at``, so deleting a migration leaves
    this test green. The 24h window is verified separately against a disposable
    database.
    """

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> int:
        resolved = _naive(7200)
        await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=resolved,
            resumed_at=resolved,
        )
        # The exact shape migration 0012 produces for a historical expired row.
        await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.expired,
            resolved_at=resolved,
            resumed_at=resolved,
            resolved_by=None,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        return await reconciler.reconcile_once()

    count = _run_async(steps, runs_stream)

    assert count == 0
    assert valkey.xrange(runs_stream) == []


def test_reconciler_grace_applies_to_expired_rows(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """The grace horizon guards expired rows exactly as it guards resolved ones.

    Grace is what keeps the reconciler from racing an inline enqueue that is
    still being consumed: it must exceed the worker's max turn duration. For an
    expired row the horizon keys off the ``resolved_at`` the expiry CAS stamps at
    flip time, so a freshly expired record whose sweeper-delivered wake is still
    in flight is skipped, and only a genuinely stale one is re-enqueued. A
    work-list that special-cased expired rows out of the grace filter would pass
    every other test here and fail this one.
    """

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, int, datetime | None]:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.expired,
            resolved_at=_naive(0),
            resumed_at=None,
            resolved_by=None,
        )
        within = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=3600, batch_limit=100
        )
        count_within = await within.reconcile_once()

        # Backdate the expiry well past the grace horizon.
        async with sessionmaker() as session:
            await session.execute(
                update(Approval)
                .where(Approval.id == approval_id)
                .values(resolved_at=_naive(7200))
            )
            await session.commit()

        past = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=3600, batch_limit=100
        )
        count_past = await past.reconcile_once()
        return count_within, count_past, await _resumed_at(sessionmaker, approval_id)

    count_within, count_past, resumed_at = _run_async(steps, runs_stream)

    assert count_within == 0
    assert count_past == 1
    assert len(valkey.xrange(runs_stream)) == 1
    assert resumed_at is not None


def test_reconciler_respects_grace_window(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A record resolved within the grace window is skipped (avoids racing the
    inline enqueue path); the same record, once past grace, is picked up."""

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, int, datetime | None]:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(0),
            resumed_at=None,
        )
        within = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=3600,
            batch_limit=100,
        )
        count_within = await within.reconcile_once()

        # Backdate the resolution well past the grace horizon.
        async with sessionmaker() as session:
            await session.execute(
                update(Approval)
                .where(Approval.id == approval_id)
                .values(resolved_at=_naive(7200))
            )
            await session.commit()

        past = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=3600,
            batch_limit=100,
        )
        count_past = await past.reconcile_once()
        return count_within, count_past, await _resumed_at(sessionmaker, approval_id)

    count_within, count_past, resumed_at = _run_async(steps, runs_stream)

    assert count_within == 0
    assert count_past == 1
    assert len(valkey.xrange(runs_stream)) == 1
    assert resumed_at is not None


def test_reconciler_is_idempotent_across_runs(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """After one successful pass marks a record resumed, a second pass finds
    nothing to do and enqueues nothing new."""

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, int]:
        await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
        )
        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        first = await reconciler.reconcile_once()
        second = await reconciler.reconcile_once()
        return first, second

    first, second = _run_async(steps, runs_stream)

    assert first == 1
    assert second == 0
    assert len(valkey.xrange(runs_stream)) == 1


def test_reconciler_isolates_per_record_failure(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """One record's enqueue failure must not abort the batch nor mark that
    record resumed; the other record is still enqueued and marked.

    Marking-before-enqueue would leave the failing record marked -- this asserts
    the enqueue-first-then-mark ordering by requiring the failing record's
    ``resumed_at`` to stay NULL for the next pass.
    """

    async def steps(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[uuid.UUID, uuid.UUID, int, datetime | None, datetime | None]:
        fail_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
        )
        ok_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(120),
            resumed_at=None,
        )

        real_enqueue = queue.enqueue
        fail_event = f"approval-{fail_id}-resolved"

        async def flaky(turn: QueuedTurn) -> str:
            if turn.event_id == fail_event:
                raise RuntimeError("valkey blip for one record")
            return await real_enqueue(turn)

        queue.enqueue = flaky  # type: ignore[method-assign]

        reconciler = ResumeReconciler(
            sessionmaker, queue, interval_seconds=30, grace_seconds=0, batch_limit=100
        )
        count = await reconciler.reconcile_once()
        return (
            fail_id,
            ok_id,
            count,
            await _resumed_at(sessionmaker, fail_id),
            await _resumed_at(sessionmaker, ok_id),
        )

    fail_id, ok_id, count, resumed_fail, resumed_ok = _run_async(steps, runs_stream)

    assert count == 1
    assert resumed_fail is None
    assert resumed_ok is not None
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{ok_id}-resolved"


def test_reconciler_skips_row_locked_by_concurrent_claim(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A row a concurrent replica already holds under ``FOR UPDATE`` is skipped by
    this pass's ``SELECT ... FOR UPDATE SKIP LOCKED`` claim -- never double-enqueued
    -- and picked up on the next pass once the lock releases. Two API replicas'
    overlapping reconcile passes therefore never both re-run one approved action.

    The whole interleave runs on one event loop across two distinct engines (two
    real Postgres connections that genuinely contend). A ``statement_timeout`` on
    the reconciler's engine keeps the pre-fix path -- which selects the locked row
    and then blocks forever on its post-enqueue ``mark_approval_resumed`` UPDATE
    against the held lock -- from deadlocking the suite: today it surfaces the bug
    as a non-zero locked count and a non-empty stream instead of hanging.
    """

    async def coro() -> tuple[uuid.UUID, int, int, int, datetime | None]:
        engine = create_async_engine(
            get_settings().database_url,
            connect_args={"server_settings": {"statement_timeout": "3000"}},
        )
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        lock_engine = create_async_engine(get_settings().database_url)
        lock_sessionmaker = async_sessionmaker(lock_engine, expire_on_commit=False)
        client = aioredis.Redis(
            host=_VALKEY_HOST, port=_VALKEY_PORT, password=_VALKEY_PW or None
        )
        queue = ResumeQueue(client, stream=runs_stream)
        try:
            approval_id = await _insert_approval(
                sessionmaker,
                status=ApprovalStatus.approved,
                resolved_at=_naive(120),
                resumed_at=None,
            )
            reconciler = ResumeReconciler(
                sessionmaker,
                queue,
                interval_seconds=30,
                grace_seconds=0,
                batch_limit=100,
            )

            # A concurrent replica holds the row under FOR UPDATE, uncommitted.
            async with lock_sessionmaker() as holder:
                await holder.execute(
                    select(Approval)
                    .where(Approval.id == approval_id)
                    .with_for_update()
                )
                # This pass must skip the locked row (0, nothing enqueued).
                try:
                    count_locked = await reconciler.reconcile_once()
                except Exception:  # noqa: BLE001
                    # Pre-fix: the locked row is selected, enqueued, then the mark
                    # UPDATE blocks on the held lock until statement_timeout fires.
                    count_locked = -1
                locked_len = len(await client.xrange(runs_stream))
                await holder.rollback()

            # Lock released: the next pass claims and enqueues it exactly once.
            count_free = await reconciler.reconcile_once()
            resumed_at = await _resumed_at(sessionmaker, approval_id)
            return approval_id, count_locked, locked_len, count_free, resumed_at
        finally:
            await client.aclose()
            await engine.dispose()
            await lock_engine.dispose()

    approval_id, count_locked, locked_len, count_free, resumed_at = asyncio.run(coro())

    # While the row was locked, the pass skipped it: no work, nothing enqueued.
    assert count_locked == 0
    assert locked_len == 0

    # Once the lock released, the same row is picked up exactly once and marked.
    assert count_free == 1
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{approval_id}-resolved"
    assert resumed_at is not None


# --- #532: dead-lettered resume backstop ---------------------------------------
#
# The NULL-gated finder (`list_resolved_unresumed`) misses a resume turn that
# DID reach the runs stream (so `resumed_at` is SET) and then died at the worker's
# delivery cap (#505): the worker moves it to the `<runs>:dead` graveyard and acks
# it off, leaving `resumed_at` set but the sandbox never woken. `#532` adds a
# graveyard-scan pass that RE-OPENS such approvals (clears `resumed_at`) so the
# existing reconcile pass re-enqueues them. These tests seed a real graveyard row
# and assert the real `resumed_at` NULL<->set transitions and stream landings.


def _dl_iso(seconds_ago: float = 0.0) -> str:
    """A tz-aware UTC ISO-8601 ``now - seconds_ago`` ending in ``+00:00``.

    The worker stamps ``dl_dead_lettered_at`` with ``datetime.now(UTC).isoformat()``;
    the backstop parses it to naive UTC to compare against the naive ``resumed_at``
    column. Seeding with the same tz-aware shape keeps the parse honest.
    """

    return (datetime.now(UTC) - timedelta(seconds=seconds_ago)).isoformat()


def _seed_graveyard_row(
    valkey: redis.Redis,
    runs_stream: str,
    approval_id: uuid.UUID,
    *,
    dead_lettered_at: str,
    status: str = ApprovalStatus.approved,
    resolved_by: str | None = "U9",
) -> None:
    """Seed one dead-lettered resume turn onto ``<runs>:dead``, sync, as the worker
    would: the QueuedTurn's ``event_id`` is ``resume_event_id(approval_id)`` (so the
    backstop parses it back to this approval), plus the ``dl_*`` metadata columns."""

    from curie_api.resumequeue import build_resume_turn

    approval = _detached_approval(status, resolved_by=resolved_by)
    approval.id = approval_id
    turn = build_resume_turn(approval)
    valkey.xadd(
        f"{runs_stream}:dead",
        {
            "payload": turn.model_dump_json(),
            "dl_dead_lettered_at": dead_lettered_at,
            "dl_original_id": "1-0",
            "dl_delivery_count": "5",
            "dl_reason": "max delivery exceeded",
        },
    )


def test_reopen_dead_lettered_resume_reenqueues_owed_wake(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """Core AC: a delivered-but-dead-lettered wake is detected, re-opened, and
    re-enqueued -- the gap the NULL-gated finder alone cannot close.

    The row carries ``resolved_at`` set AND ``resumed_at`` set (the wake reached
    the stream, so the inline path marked it) yet the sandbox never woke because
    the turn died at the delivery cap and was dead-lettered. First prove the gap:
    a bare ``reconcile_once`` enqueues nothing and leaves ``resumed_at`` set,
    because ``list_resolved_unresumed`` filters ``resumed_at IS NULL``. Then the
    graveyard scan re-opens it (``resumed_at`` -> NULL) and the very next
    ``reconcile_once`` re-enqueues the resume turn and re-marks it.
    """

    async def insert_and_prove_gap(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[uuid.UUID, int, datetime | None]:
        approval_id = await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(3600),
            resumed_at=_naive(1800),
        )
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=0,
            batch_limit=100,
            dead_letter_scan_limit=1000,
        )
        gap_count = await reconciler.reconcile_once()
        return approval_id, gap_count, await _resumed_at(sessionmaker, approval_id)

    approval_id, gap_count, gap_resumed = _run_async(insert_and_prove_gap, runs_stream)

    # The gap: the finder never re-selects a row whose resumed_at is set.
    assert gap_count == 0
    assert valkey.xrange(runs_stream) == []
    assert gap_resumed is not None

    # A graveyard row dead-lettered AFTER the wake was marked delivered.
    _seed_graveyard_row(
        valkey, runs_stream, approval_id, dead_lettered_at=_dl_iso(0)
    )

    async def reopen_then_reconcile(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, datetime | None, int, datetime | None]:
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=0,
            batch_limit=100,
            dead_letter_scan_limit=1000,
        )
        reopened = await reconciler.reopen_dead_lettered_resumes()
        resumed_after_reopen = await _resumed_at(sessionmaker, approval_id)
        reconcile_count = await reconciler.reconcile_once()
        return (
            reopened,
            resumed_after_reopen,
            reconcile_count,
            await _resumed_at(sessionmaker, approval_id),
        )

    reopened, resumed_after_reopen, reconcile_count, resumed_final = _run_async(
        reopen_then_reconcile, runs_stream
    )

    assert reopened == 1
    assert resumed_after_reopen is None  # the backstop cleared it
    assert reconcile_count == 1
    assert resumed_final is not None  # the re-enqueue re-marked it
    entries = valkey.xrange(runs_stream)
    assert len(entries) == 1
    turn = QueuedTurn.model_validate(json.loads(entries[0][1]["payload"]))
    assert turn.event_id == f"approval-{approval_id}-resolved"


def test_reopen_is_idempotent_against_persistent_graveyard_row(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A graveyard row that survives across passes (the stream's MAXLEN has not
    evicted it) is re-opened at most once: the ``resumed_at < dl_dead_lettered_at``
    guard fails on the second pass because the re-enqueue re-marked ``resumed_at``
    to a time NEWER than the row's dead-letter timestamp. No duplicate wake.
    """

    async def insert(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> uuid.UUID:
        return await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(3600),
            resumed_at=_naive(1800),
        )

    approval_id = _run_async(insert, runs_stream)

    # Dead-lettered between the original wake (30m ago) and now, so the FIRST
    # reopen matches; the re-enqueue then re-marks resumed_at to now (> this).
    _seed_graveyard_row(
        valkey, runs_stream, approval_id, dead_lettered_at=_dl_iso(1500)
    )

    async def reopen_reconcile_reopen(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, datetime | None, int, int, datetime | None]:
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=0,
            batch_limit=100,
            dead_letter_scan_limit=1000,
        )
        first = await reconciler.reopen_dead_lettered_resumes()
        resumed_after_reopen = await _resumed_at(sessionmaker, approval_id)
        reconcile_count = await reconciler.reconcile_once()
        # The row is STILL present in the graveyard; a second scan must no-op.
        second = await reconciler.reopen_dead_lettered_resumes()
        return (
            first,
            resumed_after_reopen,
            reconcile_count,
            second,
            await _resumed_at(sessionmaker, approval_id),
        )

    first, resumed_after_reopen, reconcile_count, second, resumed_final = _run_async(
        reopen_reconcile_reopen, runs_stream
    )

    assert first == 1
    assert resumed_after_reopen is None
    assert reconcile_count == 1
    assert second == 0  # the guard rejects the now-newer resumed_at
    assert resumed_final is not None  # not re-cleared: no duplicate wake
    # Exactly one re-enqueue reached the stream across both scans.
    assert len(valkey.xrange(runs_stream)) == 1


def test_reopen_ignores_stale_graveyard_row(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """A graveyard row OLDER than the approval's current ``resumed_at`` is a
    successful LATER delivery's stale corpse, not an owed wake: re-opening it would
    re-wake a session that already resumed. The ``resumed_at < dl_dead_lettered_at``
    guard excludes it -- 0 re-opened, ``resumed_at`` untouched.
    """

    async def insert(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> uuid.UUID:
        return await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(3600),
            resumed_at=_naive(0),  # woke just now, AFTER the dead-letter below
        )

    approval_id = _run_async(insert, runs_stream)
    _seed_graveyard_row(
        valkey, runs_stream, approval_id, dead_lettered_at=_dl_iso(3600)
    )

    async def reopen(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, datetime | None]:
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=0,
            batch_limit=100,
            dead_letter_scan_limit=1000,
        )
        count = await reconciler.reopen_dead_lettered_resumes()
        return count, await _resumed_at(sessionmaker, approval_id)

    count, resumed_at = _run_async(reopen, runs_stream)

    assert count == 0
    assert resumed_at is not None  # the later delivery's mark stands
    assert valkey.xrange(runs_stream) == []


def test_reopen_ignores_malformed_and_non_resume_rows(
    clean_db: None, valkey: redis.Redis, runs_stream: str
) -> None:
    """The scan tolerates graveyard rows it cannot act on -- no payload, an
    unparseable payload, or a valid turn whose ``event_id`` is not a resume id --
    returning 0 and never raising. An unrelated delivered approval is untouched,
    proving the scan clears only rows it positively matched to a resume event.
    """

    from aci_protocol import ReplyHandle

    async def insert(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> uuid.UUID:
        return await _insert_approval(
            sessionmaker,
            status=ApprovalStatus.approved,
            resolved_at=_naive(3600),
            resumed_at=_naive(0),
        )

    unrelated_id = _run_async(insert, runs_stream)

    dead = f"{runs_stream}:dead"
    # (a) metadata-only row: no payload field at all.
    valkey.xadd(
        dead,
        {
            "dl_dead_lettered_at": _dl_iso(0),
            "dl_original_id": "1-0",
            "dl_delivery_count": "5",
            "dl_reason": "max delivery exceeded",
        },
    )
    # (b) payload that is not valid QueuedTurn JSON.
    valkey.xadd(
        dead,
        {"payload": "this is not json", "dl_dead_lettered_at": _dl_iso(0)},
    )
    # (c) a valid QueuedTurn whose event_id is NOT a resume id.
    non_resume = QueuedTurn(
        event_id="eval-123",
        conversation_id="th-eval",
        author="system",
        text="not a resume turn",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1", endpoint=None),
        received_at=datetime.now(UTC).isoformat(),
    )
    valkey.xadd(
        dead,
        {"payload": non_resume.model_dump_json(), "dl_dead_lettered_at": _dl_iso(0)},
    )

    async def reopen(
        sessionmaker: async_sessionmaker[AsyncSession], queue: ResumeQueue
    ) -> tuple[int, datetime | None]:
        reconciler = ResumeReconciler(
            sessionmaker,
            queue,
            interval_seconds=30,
            grace_seconds=0,
            batch_limit=100,
            dead_letter_scan_limit=1000,
        )
        count = await reconciler.reopen_dead_lettered_resumes()
        return count, await _resumed_at(sessionmaker, unrelated_id)

    count, unrelated_resumed = _run_async(reopen, runs_stream)

    assert count == 0
    assert unrelated_resumed is not None  # untouched by the tolerant scan
    assert valkey.xrange(runs_stream) == []


def test_parse_resume_event_id_inverts_resume_event_id() -> None:
    """``parse_resume_event_id`` is the exact inverse of ``resume_event_id`` for a
    real resume key, and returns None for anything that is not one -- so the scan
    never mistakes a non-resume graveyard row for an approval to re-open.

    Pure unit: no DB, no Valkey. The UUID round-trip is the load-bearing case;
    the None cases fence the malformed shapes the scan must skip.
    """

    from curie_api.resumequeue import parse_resume_event_id, resume_event_id

    approval_id = uuid.uuid4()
    assert parse_resume_event_id(resume_event_id(approval_id)) == approval_id

    assert parse_resume_event_id("eval-123") is None
    assert parse_resume_event_id("approval--resolved") is None
    assert parse_resume_event_id("approval-not-a-uuid-resolved") is None
    assert parse_resume_event_id(uuid.uuid4().hex) is None
