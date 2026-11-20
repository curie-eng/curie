"""Best-effort reminder publication on the existing runs stream. ADR 0273."""

from typing import Any

from curie_internal.approval_reminder import APPROVAL_REMINDER_FIELD, ApprovalReminderV1
from curie_telemetry import inject_trace_context
from redis.asyncio import Redis


async def enqueue_reminder(redis: Redis, stream: str, reminder: ApprovalReminderV1) -> None:
    fields: dict[Any, Any] = {APPROVAL_REMINDER_FIELD: reminder.model_dump_json()}
    inject_trace_context(fields)
    await redis.xadd(stream, fields)
