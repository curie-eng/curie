"""Phase reports from the sandbox's ``report_progress`` tool (#3077).

The runner POSTs one report per phase transition with a request-bound
``work_item.progress`` sandbox token. This module validates the wire body,
records the report onto the WorkItem's currently active request, and derives
the pure phase view the status comment and the SVG card render from.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import ExecutionRequest, ExecutionRequestPhaseReport, FactoryStatusComment
from .workitems import _lock_active_request

PROGRESS_SCOPE = "work_item.progress"
REPORT_LIMIT = 200
PHASE_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PhaseDeclaration(_Strict):
    id: str = Field(pattern=PHASE_PATTERN)
    label: str = Field(min_length=1, max_length=40)


class LoopDeclaration(_Strict):
    start: str = Field(pattern=PHASE_PATTERN)
    review: str = Field(pattern=PHASE_PATTERN)
    cap: int = Field(ge=1, le=5)


class Declaration(_Strict):
    phases: list[PhaseDeclaration] = Field(min_length=1, max_length=12)
    loops: list[LoopDeclaration] = Field(default_factory=list, max_length=4)

    @model_validator(mode="after")
    def _consistent(self) -> Declaration:
        ids = [phase.id for phase in self.phases]
        if len(set(ids)) != len(ids):
            raise ValueError("declaration phase ids must be unique")
        for loop in self.loops:
            if loop.start not in ids or loop.review not in ids:
                raise ValueError("a loop names an undeclared phase")
            if ids.index(loop.start) >= ids.index(loop.review):
                raise ValueError("a loop's start must precede its review")
        return self

    def loop_for(self, phase: str) -> LoopDeclaration | None:
        for loop in self.loops:
            if phase in (loop.start, loop.review):
                return loop
        return None


class Activity(_Strict):
    model: str | None = Field(default=None, max_length=120)
    turns: int | None = Field(default=None, ge=0, le=1_000_000)
    tool_calls: int | None = Field(default=None, ge=0, le=1_000_000)
    last_tool: str | None = Field(default=None, max_length=120)

    @field_validator("model", "last_tool")
    @classmethod
    def _not_blank(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is not None and not value.strip():
            raise ValueError(f"{info.field_name} must not be empty; send null to omit it")
        return value


class ProgressReport(_Strict):
    phase: str = Field(pattern=PHASE_PATTERN)
    note: str | None = None
    round: int | None = None
    declaration: Declaration
    activity: Activity | None = None

    @field_validator("note")
    @classmethod
    def _strip_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not 1 <= len(stripped) <= 280:
            raise ValueError("note must be 1 to 280 characters after stripping")
        return stripped

    @model_validator(mode="after")
    def _against_declaration(self) -> ProgressReport:
        if self.phase not in {phase.id for phase in self.declaration.phases}:
            raise ValueError("phase is not declared")
        if self.round is not None:
            loop = self.declaration.loop_for(self.phase)
            if loop is None:
                raise ValueError("round is only valid on a looped phase")
            if not 1 <= self.round <= loop.cap:
                raise ValueError("round must be between 1 and the loop cap")
        return self


@dataclass(frozen=True)
class RecordResult:
    outcome: Literal[
        "recorded", "request_not_found", "no_active_request", "declaration_changed", "report_limit"
    ]
    request_id: uuid.UUID | None = None


async def record_report(
    session: AsyncSession, *, token_request_id: uuid.UUID, body: ProgressReport
) -> RecordResult:
    """Store one report on the token's request, or on the newer request of its WorkItem
    when the token's request completed and a revision took over its sandbox."""

    token_request: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest).where(ExecutionRequest.id == token_request_id).with_for_update()
    )
    if token_request is None:
        await session.rollback()
        return RecordResult("request_not_found")
    active = await _lock_active_request(session, token_request.work_item_id)
    # A completed request's sandbox can carry on into a revision on the same
    # thread, so its token follows the WorkItem's newer request. A token from a
    # failed or cancelled request never writes onto the run that replaced it.
    if active is not None and active.id != token_request.id and token_request.status != "completed":
        active = None
    if active is None:
        await session.rollback()
        return RecordResult("no_active_request")
    # Read before any rollback below expires the instance.
    active_id = active.id

    row: FactoryStatusComment | None = await session.scalar(
        select(FactoryStatusComment)
        .where(FactoryStatusComment.execution_request_id == active.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if row is None:
        # Admitted before 0055 and still active: give it its status row now.
        row = FactoryStatusComment(
            execution_request_id=active.id,
            work_item_id=active.work_item_id,
            applied_label=None,
        )
        session.add(row)

    declaration = body.declaration.model_dump()
    if row.declaration is None:
        row.declaration = declaration
    elif row.declaration != declaration:
        await session.rollback()
        return RecordResult("declaration_changed", active_id)

    count = await session.scalar(
        select(func.count())
        .select_from(ExecutionRequestPhaseReport)
        .where(ExecutionRequestPhaseReport.execution_request_id == active.id)
    )
    if (count or 0) >= REPORT_LIMIT:
        await session.rollback()
        return RecordResult("report_limit", active_id)

    session.add(
        ExecutionRequestPhaseReport(
            execution_request_id=active.id,
            phase=body.phase,
            note=body.note,
            loop_round=body.round,
        )
    )
    if body.activity is not None:
        row.activity = body.activity.model_dump(exclude_none=True)
    await session.commit()
    return RecordResult("recorded", active.id)


WAIT_CI_PHASE = "wait_ci"


async def record_wait_ci(session: AsyncSession, request_id: uuid.UUID) -> bool:
    """Record ``wait_ci`` on a request the CI gate now owns (#3179).

    The agent's turn ends when it requests publication, so it can never report
    this phase itself. Writes only when the request's declaration names the
    phase and the latest report is not already ``wait_ci``; a CI fix round's
    ``implement`` report makes the next publication record it again.
    """

    row: FactoryStatusComment | None = await session.scalar(
        select(FactoryStatusComment)
        .where(FactoryStatusComment.execution_request_id == request_id)
        .with_for_update()
    )
    declared = (
        row is not None
        and row.declaration is not None
        and any(phase.get("id") == WAIT_CI_PHASE for phase in row.declaration.get("phases", []))
    )
    latest = await session.scalar(
        select(ExecutionRequestPhaseReport.phase)
        .where(ExecutionRequestPhaseReport.execution_request_id == request_id)
        .order_by(ExecutionRequestPhaseReport.id.desc())
        .limit(1)
    )
    if not declared or latest == WAIT_CI_PHASE:
        await session.rollback()
        return False
    session.add(ExecutionRequestPhaseReport(execution_request_id=request_id, phase=WAIT_CI_PHASE))
    await session.commit()
    return True


# --- the phase view (pure) ------------------------------------------------------

PhaseState = Literal["done", "current", "redo", "pending"]


@dataclass(frozen=True)
class PhaseSlot:
    id: str
    label: str
    state: PhaseState
    round_label: str | None


@dataclass(frozen=True)
class LoopView:
    start: str
    review: str
    cap: int
    round: int
    kickbacks: int
    approved: bool
    active: bool


@dataclass(frozen=True)
class PhaseView:
    phases: tuple[PhaseSlot, ...]
    loops: tuple[LoopView, ...]
    current: str | None


def phase_view(
    declaration: dict[str, Any],
    reports: Sequence[ExecutionRequestPhaseReport],
    status: str,
    terminal_cause: str | None,
) -> PhaseView:
    """Derive every declared phase's state and every loop's round from reports.

    ``terminal_cause`` is accepted for the caller's symmetry with the status
    row; the view depends only on ``status`` among the terminal facts.
    """

    del terminal_cause
    phases = [(str(p["id"]), str(p["label"])) for p in declaration.get("phases", [])]
    ids = [phase_id for phase_id, _label in phases]
    ordered = sorted(reports, key=lambda report: report.id or 0)
    completed = status == "completed"
    latest = ordered[-1].phase if ordered else None
    current = None if completed or latest not in ids else latest
    reported = {report.phase for report in ordered}

    loops: list[LoopView] = []
    labels: dict[str, str] = {}
    redo: set[str] = set()
    for raw in declaration.get("loops", []):
        start, review, cap = str(raw["start"]), str(raw["review"]), int(raw["cap"])
        rounds = [
            report.loop_round
            for report in ordered
            if report.phase in (start, review) and report.loop_round is not None
        ]
        loop_round = max(rounds, default=1)
        after = ids[ids.index(review) + 1 :] if review in ids else []
        approved = any(phase in reported for phase in after)
        active = current in (start, review)
        if active:
            labels[start] = labels[review] = f"round {loop_round} of {cap}"
        elif approved:
            noun = "round" if loop_round == 1 else "rounds"
            labels[review] = f"approved, {loop_round} {noun}"
        if current == start and loop_round >= 2:
            redo.add(review)
        loops.append(
            LoopView(
                start=start,
                review=review,
                cap=cap,
                round=loop_round,
                kickbacks=loop_round - 1,
                approved=approved,
                active=active,
            )
        )

    current_index = ids.index(current) if current is not None else -1
    slots: list[PhaseSlot] = []
    for index, (phase_id, label) in enumerate(phases):
        state: PhaseState
        if completed:
            state = "done"
        elif phase_id == current:
            state = "current"
        elif phase_id in redo:
            state = "redo"
        elif index < current_index:
            state = "done"
        else:
            state = "pending"
        slots.append(PhaseSlot(phase_id, label, state, labels.get(phase_id)))
    return PhaseView(phases=tuple(slots), loops=tuple(loops), current=current)


_PILLS: dict[str, tuple[str, str, bool]] = {
    "waiting": ("QUEUED", "#9a6700", False),
    "running": ("RUNNING", "#2f81f7", True),
    "cancellation_requested": ("STOPPING", "#bc4c00", True),
    "completed": ("SUCCEEDED", "#1a7f37", False),
    "failed": ("FAILED", "#cf222e", False),
    "expired": ("EXPIRED", "#953800", False),
    "cancelled": ("CANCELLED", "#6e7781", False),
}
_PUBLISHING = ("PUBLISHING", "#8250df", True)
_UNKNOWN_COLOR = "#6e7781"


def pill_for(status: str, publishing: bool) -> tuple[str, str, bool]:
    """The status pill: label, color, and whether its dot pulses."""

    if status == "running" and publishing:
        return _PUBLISHING
    return _PILLS.get(status, (status.upper(), _UNKNOWN_COLOR, False))
