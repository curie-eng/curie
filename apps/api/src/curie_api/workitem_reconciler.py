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
from channel_protocol.work_item_events import execute_event_id, terminate_event_id
from curie_internal.streams import ensure_group
from curie_telemetry import (
    TRACEPARENT_STREAM_FIELD,
    inject_trace_context,
    operation_span,
    record_metric,
)
from opentelemetry.trace import SpanKind
from redis.exceptions import ResponseError
from redis.typing import EncodableT
from sqlalchemy import exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from curie_api.workitems import lifecycle

from . import factory_ci, factory_label_reconcile, factory_notices, factory_poll_intake
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
  local args = {KEYS[2], '*', ARGV[2], ARGV[3]}
  if ARGV[4] ~= '' and ARGV[5] ~= '' then
    table.insert(args, ARGV[4])
    table.insert(args, ARGV[5])
  end
  redis.call('XADD', unpack(args))
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
        self._step_consecutive_failures: dict[str, int] = {}

    def _stream(self) -> str:
        return self._settings.runs_stream

    def _group(self) -> str:
        return self._settings.runs_consumer_group or WORKER_GROUP_DEFAULT

    async def _ensure_group(self) -> None:
        await ensure_group(self._valkey, self._stream(), self._group(), start_id="$")

    async def _xadd(self, turn: QueuedTurn, *, marker: tuple[str, int] | None = None) -> None:
        with operation_span(
            "curie.queue.enqueue",
            kind=SpanKind.PRODUCER,
            attributes={"service.name": "curie-api", "source": "api"},
        ):
            carrier: dict[str, str] = {STREAM_PAYLOAD_FIELD: turn.model_dump_json()}
            inject_trace_context(carrier)
            fields: dict[EncodableT, EncodableT] = {}
            fields.update(carrier)
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
                    carrier[STREAM_PAYLOAD_FIELD],
                    TRACEPARENT_STREAM_FIELD if TRACEPARENT_STREAM_FIELD in carrier else "",
                    carrier.get(TRACEPARENT_STREAM_FIELD, ""),
                )
                return
            try:
                await self._ensure_group()
                await self._valkey.xadd(self._stream(), fields)
            except ResponseError as exc:
                message = str(exc)
                if "NOGROUP" in message or "no such key" in message.lower():
                    await self._ensure_group()
                    await self._valkey.xadd(self._stream(), fields)
                    return
                raise

    async def run_once(self) -> None:
        for name, step in (
            ("settle_publications", self._settle_publications),
            ("expire_waiting", self._expire_waiting),
            ("request_deadline_cancellations", self._request_deadline_cancellations),
            (
                "request_owner_lost_cancellations",
                self._request_owner_lost_cancellations,
            ),
            # A forced settle requires a published terminate wake, so the
            # worker was told to tear down before the settle pass.
            ("publish_terminate_wakes", self._publish_terminate_wakes),
            ("settle_overdue_cancellations", self._settle_overdue_cancellations),
            ("readmit_pending", self._readmit_pending),
            ("reconcile_missed_labels", self._reconcile_missed_labels),
            ("redispatch_lapsed_acquisitions", self._redispatch_lapsed_acquisitions),
            ("publish_execute_wakes", self._publish_execute_wakes),
        ):
            attributes = {"service.name": "curie-api", "step": name}
            try:
                await step()
            except Exception:
                logger.exception("work item reconciler step %s failed", name)
                self._step_consecutive_failures[name] = (
                    self._step_consecutive_failures.get(name, 0) + 1
                )
                record_metric(
                    "curie.work_item.reconciler.step.failure", attributes=attributes
                )
            else:
                self._step_consecutive_failures[name] = 0
            record_metric(
                "curie.work_item.reconciler.step.consecutive_failures",
                self._step_consecutive_failures[name],
                attributes=attributes,
            )

    async def _redispatch_lapsed_acquisitions(self) -> None:
        async with self._sessionmaker() as session:
            await redispatch_lapsed_acquisitions(session)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("work item reconciler pass failed")
            await asyncio.sleep(self._settings.work_item_reconciler_interval_seconds)

    async def run_status_comments_forever(self) -> None:
        """Keep GitHub status I/O independent of dispatch and lease reconciliation."""
        while True:
            try:
                await self._sync_status_comments()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("work item status comment pass failed")
            await asyncio.sleep(self._settings.work_item_reconciler_interval_seconds)

    async def _expire_waiting(self) -> None:
        async with self._sessionmaker() as session:
            now = await lifecycle.database_now(session)
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
                result = await lifecycle.expire_waiting(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_work_item_version=row.work_item_version,
                    expected_request_version=row.version,
                )
            if isinstance(result, lifecycle.WorkItemOutcome):
                logger.warning(
                    "work item request %s expired waiting for capacity after %d deferrals",
                    row.id,
                    row.capacity_deferrals,
                )

    async def _request_deadline_cancellations(self) -> None:
        async with self._sessionmaker() as session:
            now = await lifecycle.database_now(session)
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
                await lifecycle.request_execution_deadline_cancellation(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_work_item_version=row.work_item_version,
                    expected_request_version=row.version,
                )

    async def _request_owner_lost_cancellations(self) -> None:
        ttl = timedelta(seconds=self._settings.work_item_runtime_ttl_seconds)
        async with self._sessionmaker() as session:
            now = await lifecycle.database_now(session)
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
                                    <= now - ttl
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
                await lifecycle.request_owner_lost_cancellation(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_work_item_version=row.work_item_version,
                    expected_request_version=row.version,
                )

    async def _settle_overdue_cancellations(self) -> None:
        settle_seconds = self._settings.work_item_cancel_settle_seconds
        async with self._sessionmaker() as session:
            now = await lifecycle.database_now(session)
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
                result = await lifecycle.settle_overdue_cancellation(
                    session,
                    work_item_id=row.work_item_id,
                    request_id=row.id,
                    expected_request_version=row.version,
                    settle_seconds=settle_seconds,
                )
            if isinstance(result, lifecycle.WorkItemOutcome):
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
                now = await lifecycle.database_now(session)
                await lifecycle.admit_pending_readmit(
                    session,
                    work_item_id=work_item_id,
                    wait_deadline=now
                    + timedelta(
                        seconds=self._settings.work_item_wait_budget_seconds
                    ),
                )
        async with self._sessionmaker() as session:
            active = aliased(ExecutionRequest)
            queued_ids = (
                await session.scalars(
                    select(WorkItem.id)
                    .join(
                        ExecutionRequest,
                        ExecutionRequest.work_item_id == WorkItem.id,
                    )
                    .where(
                        ExecutionRequest.status == "queued",
                        WorkItem.readmit_request_id.is_(None),
                        ~exists().where(
                            active.work_item_id == WorkItem.id,
                            active.status.in_(
                                ("waiting", "running", "cancellation_requested")
                            ),
                        ),
                    )
                    .group_by(WorkItem.id)
                    .order_by(func.min(ExecutionRequest.created_at), WorkItem.id)
                    .limit(self._settings.work_item_batch_limit)
                )
            ).all()
        for work_item_id in queued_ids:
            async with self._sessionmaker() as session:
                now = await lifecycle.database_now(session)
                await lifecycle.admit_next_revision(
                    session,
                    work_item_id=work_item_id,
                    wait_deadline=now
                    + timedelta(seconds=self._settings.work_item_wait_budget_seconds),
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
        gated: list[lifecycle.PublicationSettlement] = []
        try:
            for _ in range(self._settings.work_item_batch_limit):
                async with self._sessionmaker() as session:
                    settlement = await lifecycle.claim_publication_settlement(
                        session, exclude=frozenset(skip)
                    )
                    if settlement is None:
                        await session.rollback()
                        break
                    skip.add(settlement.request_id)
                    if settlement.cause != "completed":
                        await lifecycle.fail_execution(
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
        if settings.github_factory_intake == "poll":
            interval = settings.github_factory_poll_interval_s
        else:
            interval = settings.github_factory_reconcile_interval_s
        if not settings.github_factory_ingress_enabled or interval <= 0:
            return
        clock = asyncio.get_running_loop().time()
        if clock < self._labels_due:
            return
        self._labels_due = clock + interval
        async with httpx.AsyncClient(timeout=settings.github_app_timeout_seconds) as client:
            if settings.github_factory_intake == "poll":
                await factory_poll_intake.poll_once(self._sessionmaker, settings, client)
            else:
                await factory_label_reconcile.reconcile_missed_labels(
                    self._sessionmaker, settings, client, now=datetime.now(UTC)
                )

    async def _sync_status_comments(self) -> None:
        paused_for_upgrade = False
        try:
            paused_for_upgrade = bool(
                await self._valkey.exists(self._settings.upgrade_quiesce_key())
            )
        except Exception:
            logger.warning(
                "work item reconciler could not read the upgrade quiesce marker;"
                " treating the installation as not paused",
                exc_info=True,
            )
        await factory_notices.sync_status_comments(
            self._sessionmaker,
            self._settings,
            owner=self._owner,
            paused_for_upgrade=paused_for_upgrade,
        )

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
            event_id=execute_event_id(request_id, generation),
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
                event_id=terminate_event_id(item.request_id),
                conversation_id=item.reply_conversation_id,
                author=item.requester or "work-item",
                text="terminate",
                source=TurnSource.WEBHOOK,
                reply_handle=ReplyHandle(
                    kind=item.reply_kind,
                    channel=item.reply_address,
                    placeholder=None,
                    endpoint=None,
                    adapter=item.reply_adapter,
                ),
                received_at=datetime.now(UTC).isoformat(),
            )
            await self._xadd(turn)
