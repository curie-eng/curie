"""Publish WorkItem execute and terminate wakes onto the runs stream."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import redis.asyncio as redis
from aci_protocol import (
    STREAM_PAYLOAD_FIELD,
    WORKER_GROUP_DEFAULT,
    QueuedTurn,
    ReplyHandle,
    TurnSource,
)
from redis.exceptions import ResponseError
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import factory_ci, factory_label_reconcile, factory_notices, workitems
from .config import Settings
from .models import ExecutionRequest, Publication, WorkItem
from .workitem_dispatch import (
    claim_due,
    claim_terminate_publishes,
    fence_published,
    load_execute_wake,
    redispatch_lapsed_acquisitions,
)

logger = logging.getLogger(__name__)


# Set the round's enqueue marker NX and append the turn atomically, so no crash
# can leave a marker without its turn.
_MARK_AND_XADD = """
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  redis.call('XADD', KEYS[2], '*', ARGV[2], ARGV[3])
  return 1
end
return 0
"""


class WorkItemReconciler:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        valkey: redis.Redis,
        settings: Settings,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._valkey = valkey
        self._settings = settings
        self._owner = f"work-item-reconciler:{uuid.uuid4()}"
        # Next CI observation per request waiting on CI (#3097), in database time.
        self._ci_next_poll: dict[uuid.UUID, datetime] = {}
        # Pass number of each request's last CI observation, so the per-pass
        # budget goes to the least recently observed due requests first.
        self._ci_observed: dict[uuid.UUID, int] = {}
        self._ci_pass = 0
        # Monotonic time of the next missed-label listing (#3081).
        self._labels_due = 0.0

    def _stream(self) -> str:
        return self._settings.runs_stream

    def _group(self) -> str:
        return self._settings.runs_consumer_group or WORKER_GROUP_DEFAULT

    async def _ensure_group(self) -> None:
        try:
            await self._valkey.xgroup_create(
                self._stream(), self._group(), id="$", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _xadd(self, turn: QueuedTurn, *, marker: tuple[str, int] | None = None) -> None:
        if marker is not None:
            # Set the round's marker NX and append atomically; a marker that is
            # already set means the turn is already on the stream.
            key, ttl = marker
            await self._ensure_group()
            await self._valkey.eval(
                _MARK_AND_XADD,
                2,
                key,
                self._stream(),
                ttl,
                STREAM_PAYLOAD_FIELD,
                turn.model_dump_json(),
            )
            return
        try:
            await self._ensure_group()
            await self._valkey.xadd(
                self._stream(), {STREAM_PAYLOAD_FIELD: turn.model_dump_json()}
            )
        except ResponseError as exc:
            message = str(exc)
            if "NOGROUP" in message or "no such key" in message.lower():
                await self._ensure_group()
                await self._valkey.xadd(
                    self._stream(),
                    {STREAM_PAYLOAD_FIELD: turn.model_dump_json()},
                )
                return
            raise

    async def run_once(self) -> None:
        await self._settle_publications()
        await self._expire_waiting()
        await self._request_deadline_cancellations()
        await self._request_owner_lost_cancellations()
        # Terminate wakes go out before the settle pass: a forced settle
        # requires a published wake, so the worker was told to tear down.
        await self._publish_terminate_wakes()
        await self._settle_overdue_cancellations()
        await self._readmit_pending()
        await self._reconcile_missed_labels()
        await self._sync_status_comments()
        async with self._sessionmaker() as session:
            await redispatch_lapsed_acquisitions(session)
        await self._publish_execute_wakes()

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("work item reconciler pass failed")
            await asyncio.sleep(self._settings.work_item_reconciler_interval_seconds)

    async def _expire_waiting(self) -> None:
        async with self._sessionmaker() as session:
            now = await workitems._database_now(session)
            overdue = (
                await session.execute(
                    select(
                        ExecutionRequest.id,
                        ExecutionRequest.work_item_id,
                        ExecutionRequest.version,
                        ExecutionRequest.capacity_deferrals,
                        WorkItem.version.label("work_item_version"),
                    )
                    .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
                    .where(
                        ExecutionRequest.status == "waiting",
                        ExecutionRequest.wait_deadline <= now,
                    )
                )
            ).all()
        for row in overdue:
            async with self._sessionmaker() as session:
                result = await workitems.expire_waiting(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_work_item_version=row.work_item_version,
                    expected_request_version=row.version,
                )
            if isinstance(result, workitems.WorkItemOutcome):
                logger.warning(
                    "work item request %s expired waiting for capacity after %d deferrals",
                    row.id,
                    row.capacity_deferrals,
                )

    async def _request_deadline_cancellations(self) -> None:
        async with self._sessionmaker() as session:
            now = await workitems._database_now(session)
            rows = (
                await session.execute(
                    select(
                        ExecutionRequest.id,
                        ExecutionRequest.work_item_id,
                        ExecutionRequest.version,
                        WorkItem.version.label("work_item_version"),
                    )
                    .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
                    .where(
                        ExecutionRequest.status == "running",
                        ExecutionRequest.execution_deadline.is_not(None),
                        ExecutionRequest.execution_deadline <= now,
                    )
                )
            ).all()
        for row in rows:
            async with self._sessionmaker() as session:
                await workitems.request_execution_deadline_cancellation(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_work_item_version=row.work_item_version,
                    expected_request_version=row.version,
                )

    async def _request_owner_lost_cancellations(self) -> None:
        ttl = timedelta(seconds=self._settings.work_item_runtime_ttl_seconds)
        async with self._sessionmaker() as session:
            now = await workitems._database_now(session)
            rows = (
                await session.execute(
                    select(
                        ExecutionRequest.id,
                        ExecutionRequest.work_item_id,
                        ExecutionRequest.version,
                        WorkItem.version.label("work_item_version"),
                    )
                    .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
                    .where(
                        ExecutionRequest.status == "running",
                        # A published request waits on CI; the CI gate owns it.
                        ~exists().where(
                            Publication.execution_request_id == ExecutionRequest.id,
                            Publication.status == "succeeded",
                        ),
                        (
                            (
                                ExecutionRequest.runtime_heartbeat_expires_at.is_not(
                                    None
                                )
                                & (
                                    ExecutionRequest.runtime_heartbeat_expires_at
                                    <= now
                                )
                            )
                            | (
                                ExecutionRequest.runtime_owner.is_(None)
                                & ExecutionRequest.started_at.is_not(None)
                                & (ExecutionRequest.started_at <= now - ttl)
                            )
                        ),
                    )
                )
            ).all()
        for row in rows:
            async with self._sessionmaker() as session:
                await workitems.request_owner_lost_cancellation(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_work_item_version=row.work_item_version,
                    expected_request_version=row.version,
                )

    async def _settle_overdue_cancellations(self) -> None:
        settle_seconds = self._settings.work_item_cancel_settle_seconds
        async with self._sessionmaker() as session:
            now = await workitems._database_now(session)
            rows = (
                await session.execute(
                    select(
                        ExecutionRequest.id,
                        ExecutionRequest.work_item_id,
                        ExecutionRequest.version,
                    )
                    .where(
                        ExecutionRequest.status == "cancellation_requested",
                        ExecutionRequest.terminal_cause == "issue_cancelled",
                        ExecutionRequest.cancellation_requested_at.is_not(None),
                        ExecutionRequest.cancellation_requested_at
                        <= now - timedelta(seconds=settle_seconds),
                        ExecutionRequest.terminate_published_at.is_not(None),
                        ExecutionRequest.runtime_owner.is_(None)
                        | (
                            ExecutionRequest.runtime_heartbeat_expires_at.is_not(None)
                            & (ExecutionRequest.runtime_heartbeat_expires_at <= now)
                        ),
                    )
                    .limit(self._settings.work_item_batch_limit)
                )
            ).all()
        for row in rows:
            async with self._sessionmaker() as session:
                result = await workitems.settle_overdue_cancellation(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_request_version=row.version,
                    settle_seconds=settle_seconds,
                )
            if isinstance(result, workitems.WorkItemOutcome):
                logger.warning(
                    "work item request %s cancellation settled after %ds "
                    "without a worker teardown receipt",
                    row.id,
                    settle_seconds,
                )

    async def _readmit_pending(self) -> None:
        async with self._sessionmaker() as session:
            ids = (
                await session.scalars(
                    select(WorkItem.id)
                    .where(WorkItem.readmit_request_id.is_not(None))
                    .limit(self._settings.work_item_batch_limit)
                )
            ).all()
        for work_item_id in ids:
            async with self._sessionmaker() as session:
                now = await workitems._database_now(session)
                await workitems.admit_pending_readmit(
                    session,
                    work_item_id=work_item_id,
                    wait_deadline=now
                    + timedelta(
                        seconds=self._settings.work_item_wait_budget_seconds
                    ),
                )

    async def _settle_publications(self) -> None:
        """Settle linked publications; a succeeded one goes through the CI gate.

        ``skip`` holds every request handled this pass, so one request waiting
        on CI cannot starve the others (#3097).
        """

        skip: set[uuid.UUID] = set()
        observations = 0
        self._ci_pass += 1

        def may_observe(request_id: uuid.UUID) -> bool:
            nonlocal observations
            if observations >= factory_ci.CI_OBSERVATIONS_PER_PASS:
                return False
            observations += 1
            self._ci_observed[request_id] = self._ci_pass
            return True

        client: httpx.AsyncClient | None = None
        gated: list[workitems.PublicationSettlement] = []
        try:
            for _ in range(self._settings.work_item_batch_limit):
                async with self._sessionmaker() as session:
                    settlement = await workitems.claim_publication_settlement(
                        session, exclude=frozenset(skip)
                    )
                    if settlement is None:
                        await session.rollback()
                        break
                    skip.add(settlement.request_id)
                    if settlement.cause != "completed":
                        await workitems.fail_execution(
                            session,
                            work_item_id=settlement.work_item_id,
                            request_id=settlement.request_id,
                            cause=settlement.cause,
                            expected_work_item_version=settlement.work_item_version,
                            expected_request_version=settlement.request_version,
                        )
                        continue
                    # No network call under the claim's row lock.
                    await session.rollback()
                gated.append(settlement)
            # Least recently observed first, so slow observations of older
            # requests cannot keep a later one out of the budget (#3097).
            gated.sort(key=lambda item: self._ci_observed.get(item.request_id, 0))
            for settlement in gated:
                if client is None:
                    client = httpx.AsyncClient(
                        timeout=self._settings.github_app_timeout_seconds
                    )
                result = await factory_ci.gate(
                    self._sessionmaker,
                    self._valkey,
                    self._settings,
                    client,
                    settlement,
                    owner=self._owner,
                    next_poll=self._ci_next_poll,
                    dispatch=self._dispatch_ci_turn,
                    may_observe=may_observe,
                )
                if result != "waiting":
                    self._ci_observed.pop(settlement.request_id, None)
        finally:
            if client is not None:
                await client.aclose()

    async def _dispatch_ci_turn(
        self, request: ExecutionRequest, round_: int, text: str
    ) -> bool:
        """Enqueue a CI fix turn for the same request, like its execute wake."""

        async with self._sessionmaker() as session:
            loaded = await load_execute_wake(session, request.id)
        if loaded is None:
            return False
        current, _work_item, binding = loaded
        if (
            current.requester is None
            or current.reply_kind is None
            or current.reply_address is None
            or current.reply_conversation_id is None
            or binding is None
        ):
            logger.warning(
                "work item request %s cannot route its CI fix turn; leaving it waiting",
                request.id,
            )
            return False
        await self._xadd(
            QueuedTurn(
                event_id=factory_ci.continuation_event_id(request.id, round_),
                conversation_id=current.reply_conversation_id,
                author=current.requester,
                text=text,
                source=TurnSource.WEBHOOK,
                reply_handle=ReplyHandle(
                    kind=current.reply_kind,
                    channel=current.reply_address,
                    placeholder=None,
                    endpoint=binding.endpoint,
                    adapter=binding.adapter,
                ),
                received_at=datetime.now(UTC).isoformat(),
            ),
            marker=(
                factory_ci.enqueue_marker(request.id, round_),
                factory_ci.round_ttl(request, datetime.now(UTC)),
            ),
        )
        # Already-enqueued counts as published: the round has its turn.
        return True

    async def _reconcile_missed_labels(self) -> None:
        """Admit labeled issues whose delivery GitHub never retried (#3081)."""

        settings = self._settings
        interval = settings.github_factory_reconcile_interval_s
        if not settings.github_factory_ingress_enabled or interval <= 0:
            return
        clock = asyncio.get_running_loop().time()
        if clock < self._labels_due:
            return
        self._labels_due = clock + interval
        async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
            await factory_label_reconcile.reconcile_missed_labels(
                self._sessionmaker, settings, client, now=datetime.now(UTC)
            )

    async def _sync_status_comments(self) -> None:
        async with self._sessionmaker() as session:
            await factory_notices.sync_status_comments(session, self._settings)

    async def _publish_execute_wakes(self) -> None:
        async with self._sessionmaker() as session:
            claimed = await claim_due(
                session,
                owner=self._owner,
                limit=self._settings.work_item_batch_limit,
                lease_s=self._settings.work_item_dispatch_lease_seconds,
            )
        for item in claimed:
            turn = await self._execute_turn(item.request_id, item.generation)
            if turn is None:
                continue
            await self._xadd(turn)
            async with self._sessionmaker() as session:
                await fence_published(
                    session, item.request_id, item.epoch, item.generation
                )

    async def _execute_turn(
        self, request_id: uuid.UUID, generation: int
    ) -> QueuedTurn | None:
        async with self._sessionmaker() as session:
            loaded = await load_execute_wake(session, request_id)
        if loaded is None:
            return None
        request, _work_item, binding = loaded
        if (
            request.objective is None
            or request.requester is None
            or request.reply_kind is None
            or request.reply_address is None
            or request.reply_conversation_id is None
        ):
            return None
        if binding is None:
            logger.warning(
                "work item request %s binding_missing; leaving row due",
                request_id,
            )
            return None
        return QueuedTurn(
            event_id=f"work-item-{request_id}-execute-{generation}",
            conversation_id=request.reply_conversation_id,
            author=request.requester,
            text=request.objective,
            source=TurnSource.WEBHOOK,
            reply_handle=ReplyHandle(
                kind=request.reply_kind,
                channel=request.reply_address,
                placeholder=None,
                endpoint=binding.endpoint,
                adapter=binding.adapter,
            ),
            received_at=datetime.now(UTC).isoformat(),
        )

    async def _publish_terminate_wakes(self) -> None:
        async with self._sessionmaker() as session:
            published = await claim_terminate_publishes(
                session,
                retry_seconds=self._settings.work_item_terminate_retry_seconds,
                limit=self._settings.work_item_batch_limit,
            )
        for item in published:
            turn = QueuedTurn(
                event_id=f"work-item-{item.request_id}-terminate",
                conversation_id=item.reply_conversation_id,
                author=item.requester or "work-item",
                text="terminate",
                source=TurnSource.WEBHOOK,
                reply_handle=ReplyHandle(
                    kind=item.reply_kind,
                    channel=item.reply_address,
                    placeholder=None,
                    endpoint=None,
                    adapter=None,
                ),
                received_at=datetime.now(UTC).isoformat(),
            )
            await self._xadd(turn)
