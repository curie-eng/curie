"""Publish WorkItem execute and terminate wakes onto the runs stream."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

import redis.asyncio as redis
from aci_protocol import (
    STREAM_PAYLOAD_FIELD,
    WORKER_GROUP_DEFAULT,
    QueuedTurn,
    ReplyHandle,
    TurnSource,
)
from redis.exceptions import ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import factory_notices, workitems
from .config import Settings
from .models import ExecutionRequest, WorkItem
from .workitem_dispatch import (
    claim_due,
    claim_terminate_publishes,
    fence_published,
    load_execute_wake,
    redispatch_lapsed_acquisitions,
)

logger = logging.getLogger(__name__)


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

    async def _xadd(self, turn: QueuedTurn) -> None:
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
        await self._post_terminal_notices()
        async with self._sessionmaker() as session:
            await redispatch_lapsed_acquisitions(session)
        await self._publish_execute_wakes()
        await self._publish_terminate_wakes()

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

    async def _settle_publications(self) -> None:
        for _ in range(self._settings.work_item_batch_limit):
            async with self._sessionmaker() as session:
                settlement = await workitems.claim_publication_settlement(session)
                if settlement is None:
                    await session.rollback()
                    return
                if settlement.cause == "completed":
                    await workitems.complete_execution(
                        session,
                        work_item_id=settlement.work_item_id,
                        request_id=settlement.request_id,
                        expected_work_item_version=settlement.work_item_version,
                        expected_request_version=settlement.request_version,
                    )
                else:
                    await workitems.fail_execution(
                        session,
                        work_item_id=settlement.work_item_id,
                        request_id=settlement.request_id,
                        cause=settlement.cause,
                        expected_work_item_version=settlement.work_item_version,
                        expected_request_version=settlement.request_version,
                    )

    async def _post_terminal_notices(self) -> None:
        async with self._sessionmaker() as session:
            await factory_notices.post_due_notices(session, self._settings)

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
