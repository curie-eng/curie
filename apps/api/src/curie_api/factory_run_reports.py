"""Bounded terminal diagnostics for factory executions (#5601).

The active conversation is still removed at WorkItem termination. This module
captures an operator-only projection in the same transaction, before removal.
Only explicitly selected text and phase metadata enter the retained report.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

from curie_telemetry.redact import redact_text
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from .config import get_settings
from .models import (
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryRunReport,
    Publication,
    ThreadTranscript,
    WorkItem,
)

_TEXT_LIMIT = 4000
_TRANSCRIPT_EVENT_LIMIT = 120
_PHASE_EVENT_LIMIT = 80
_CREDENTIAL_FIELD = re.compile(
    r"(?i)((?<![A-Za-z0-9_])(?:['\"]?(?:authorization|api[_-]?key|access[_-]?token|"
    r"secret|password|passwd|credential)['\"]?)\s*[:=]\s*)"
    r"(?:\[[^\]]+\]|\"[^\"]*\"|'[^']*'|[^\r\n,}\]]+)"
)


def _safe_text(value: Any) -> str:
    raw = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    redacted = _CREDENTIAL_FIELD.sub(r"\1[REDACTED:credential_field]", raw)
    redacted = redact_text(redacted)
    redacted = redacted.replace("\r", "\\r").replace("\n", "\\n")
    return redacted[:_TEXT_LIMIT] + ("... [truncated]" if len(redacted) > _TEXT_LIMIT else "")


def _safe_reference_url(value: str | None) -> str | None:
    if not value or len(value) > 2048 or redact_text(value) != value:
        return None
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username:
        return None
    return parsed._replace(query="", fragment="").geturl()


def _event_time(raw: Any) -> datetime | None:
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(UTC)
        except ValueError:
            pass
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _transcript_events(
    value: Any, started_at: datetime | None, terminal_at: datetime
) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(value, list):
        return [], "malformed"
    records = value[-_TRANSCRIPT_EVENT_LIMIT:]
    status = "truncated" if len(records) < len(value) else "complete"
    events: list[dict[str, Any]] = []
    unattributed = False
    offset = len(value) - len(records)
    for index, record in enumerate(records, start=offset):
        if not isinstance(record, dict):
            status = "malformed"
            continue
        at = _event_time(record.get("ts"))
        if at is None or started_at is None:
            unattributed = True
            continue
        if not started_at <= at <= terminal_at:
            continue
        lines: list[str] = []
        for field in ("user", "assistant", "summary"):
            content = record.get(field)
            if isinstance(content, str) and content.strip():
                lines.append(f"{field}: {_safe_text(content)}")
        messages = record.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if not isinstance(message, dict):
                    continue
                role = message.get("role")
                if role not in ("user", "assistant", "system", "tool"):
                    role = "unknown"
                blocks = message.get("content", [])
                if isinstance(blocks, str):
                    if blocks.strip():
                        lines.append(f"{role}: {_safe_text(blocks)}")
                    continue
                if not isinstance(blocks, list):
                    status = "malformed"
                    continue
                for block in blocks:
                    if not isinstance(block, dict):
                        continue
                    kind = block.get("type")
                    if kind == "text":
                        lines.append(f"{role}: {_safe_text(block.get('text', ''))}")
                    elif kind == "tool_use":
                        lines.append(f"tool call {_safe_text(block.get('name', 'unknown'))}")
                    elif kind == "tool_result":
                        result = "error" if block.get("is_error") is True else "returned"
                        lines.append(f"tool result: {result}")
        elif messages is not None:
            status = "malformed"
        if not lines:
            continue
        events.append(
            {
                "id": f"transcript:{index}",
                "at": at.isoformat(),
                "kind": "conversation",
                "text": "\n".join(lines)[:_TEXT_LIMIT],
            }
        )
    if unattributed and status == "complete":
        status = "unattributed"
    return events, status


def _analysis(cause: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    assistant = [
        (event, line[len("assistant:") :].strip())
        for event in events
        if event["kind"] == "conversation"
        for line in event["text"].splitlines()
        if line.startswith("assistant:") and line[len("assistant:") :].strip()
    ]
    if not assistant:
        return {
            "immediate_cause": cause,
            "summary": (
                "The underlying reason is unknown because no conversation evidence was retained."
            ),
            "evidence": [],
        }
    final, reply = assistant[-1]
    return {
        "immediate_cause": cause,
        "summary": (
            f"The run ended without a pull request. The agent reported: {reply[:300]}"
            if cause == "no_pull_request"
            else "The agent's final reply is available for inspection."
        ),
        "evidence": [{"event_id": final["id"], "statement": "Agent's reported explanation"}],
    }


async def capture(
    session: AsyncSession,
    work_item: WorkItem,
    request: ExecutionRequest,
    *,
    detail: str | None,
) -> None:
    """Insert one report for a terminal request without committing the caller's transaction."""

    if request.terminal_at is None or request.terminal_cause is None:
        return
    if await session.get(FactoryRunReport, request.id) is not None:
        return
    transcript = await session.scalar(
        select(ThreadTranscript).where(
            ThreadTranscript.agent_id == work_item.agent_id,
            ThreadTranscript.binding_scope.is_(None),
            ThreadTranscript.thread_key == work_item.conversation_id,
        )
    )
    if transcript is None:
        conversation_events: list[dict[str, Any]] = []
        transcript_status = "missing"
    else:
        conversation_events, transcript_status = _transcript_events(
            transcript.value, request.started_at, request.terminal_at
        )
    phase_rows = list(
        await session.scalars(
            select(ExecutionRequestPhaseReport)
            .where(ExecutionRequestPhaseReport.execution_request_id == request.id)
            .order_by(ExecutionRequestPhaseReport.id)
        )
    )
    phase_status = "missing" if not phase_rows else "complete"
    if len(phase_rows) > _PHASE_EVENT_LIMIT:
        phase_rows = phase_rows[-_PHASE_EVENT_LIMIT:]
        phase_status = "truncated"
    phase_events = [
        {
            "id": f"phase:{row.id}",
            "at": _utc(row.reported_at).isoformat(),
            "kind": "phase",
            "text": _safe_text(row.note or row.phase),
            "phase": row.phase,
            "round": row.loop_round,
        }
        for row in phase_rows
    ]
    publications = list(
        await session.scalars(
            select(Publication)
            .where(Publication.execution_request_id == request.id)
            .order_by(Publication.created_at, Publication.id)
        )
    )
    publication_status = "missing" if not publications else "complete"
    publication_events: list[dict[str, Any]] = []
    for row in publications[-20:]:
        publication_events.append(
            {
                "id": f"publication:{row.id}:requested",
                "at": _utc(row.created_at).isoformat(),
                "kind": "publication",
                "text": "Publication requested",
            }
        )
        publication_events.append(
            {
                "id": f"publication:{row.id}:status",
                "at": _utc(row.terminal_at or row.updated_at).isoformat(),
                "kind": "publication",
                "text": _safe_text(f"Publication {row.status}"),
                **(
                    {"reference_url": safe_url}
                    if (safe_url := _safe_reference_url(row.result_url))
                    else {}
                ),
            }
        )
    if len(publications) > 20:
        publication_status = "truncated"
    state_events = [
        {
            "id": "created",
            "at": _utc(request.created_at).isoformat(),
            "kind": "state",
            "text": "Request created",
        }
    ]
    state_events.extend(
        [
            {
                "id": "started",
                "at": _utc(request.started_at).isoformat(),
                "kind": "state",
                "text": "Execution started",
            }
        ]
        if request.started_at is not None
        else []
    )
    if request.cancellation_requested_at is not None:
        state_events.append(
            {
                "id": "cancellation_requested",
                "at": _utc(request.cancellation_requested_at).isoformat(),
                "kind": "state",
                "text": "Cancellation requested",
            }
        )
    terminal_event = {
        "id": "terminal",
        "at": request.terminal_at.astimezone(UTC).isoformat(),
        "kind": "terminal",
        "text": _safe_text(detail or request.terminal_cause)[:1000],
    }
    events = sorted(
        [*state_events, *conversation_events, *phase_events, *publication_events, terminal_event],
        key=lambda event: (event["at"], event["id"]),
    )
    report: dict[str, Any] = {
        "work_item_id": str(work_item.id),
        "execution_request_id": str(request.id),
        "status": request.status,
        "terminal_cause": request.terminal_cause,
        "terminal_at": request.terminal_at.astimezone(UTC).isoformat(),
        "sources": {
            "transcript": transcript_status,
            "phase_reports": phase_status,
            "state_changes": "partial",
            "publications": publication_status,
            "traces": "unavailable",
            "artifacts": (
                "complete"
                if any(_safe_reference_url(row.result_url) for row in publications)
                else "unavailable"
            ),
        },
        "events": events,
    }
    budget = get_settings().factory_run_report_max_bytes
    report["analysis"] = _analysis(request.terminal_cause, events)
    while len(json.dumps(report, ensure_ascii=False).encode()) > budget and len(events) > 1:
        # Preserve the terminal event and newest evidence. Never present a
        # shortened timeline as complete.
        earliest = next((event for event in events if event["id"] != "terminal"), None)
        if earliest is None:
            break
        events.remove(earliest)
        source = {
            "conversation": "transcript",
            "phase": "phase_reports",
            "publication": "publications",
            "state": "state_changes",
        }.get(earliest["kind"])
        if source is not None:
            report["sources"][source] = "truncated"
            if earliest.get("reference_url"):
                report["sources"]["artifacts"] = "truncated"
        report["analysis"] = _analysis(request.terminal_cause, events)
    expires_at = request.terminal_at + timedelta(
        seconds=get_settings().factory_run_report_retention_seconds
    )
    await session.execute(
        insert(FactoryRunReport)
        .values(
            execution_request_id=request.id,
            work_item_id=work_item.id,
            report=report,
            expires_at=expires_at,
        )
        .on_conflict_do_nothing(index_elements=["execution_request_id"])
    )


async def get_report(
    session: AsyncSession, work_item_id: Any, request_id: Any
) -> dict[str, Any] | None:
    return await session.scalar(
        select(FactoryRunReport.report).where(
            FactoryRunReport.work_item_id == work_item_id,
            FactoryRunReport.execution_request_id == request_id,
            FactoryRunReport.expires_at > func.now(),
        )
    )


async def sweep_expired(session: AsyncSession) -> int:
    expired_ids = (
        select(FactoryRunReport.execution_request_id)
        .where(FactoryRunReport.expires_at <= func.now())
        .order_by(FactoryRunReport.expires_at)
        .limit(100)
    )
    result = await session.execute(
        delete(FactoryRunReport)
        .where(FactoryRunReport.execution_request_id.in_(expired_ids))
        .returning(FactoryRunReport.execution_request_id)
    )
    return len(result.scalars().all())
