"""Phase reports from the sandbox's ``report_progress`` tool (#3077).

The runner POSTs one report per phase transition with a request-bound
``work_item.progress`` sandbox token. This module validates the wire body,
records the report onto the WorkItem's currently active request, and derives
the pure phase view the status comment and the SVG card render from.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.workitems.lifecycle import lock_active_request

from .models import ExecutionRequest, ExecutionRequestPhaseReport, FactoryStatusComment

logger = logging.getLogger(__name__)

PROGRESS_SCOPE = "work_item.progress"
REPORT_LIMIT = 200
PHASE_PATTERN = r"^[a-z][a-z0-9_]{0,63}$"
VERIFICATION_PREFLIGHT_PHASE = "verification_preflight"
VERIFICATION_CHECK_PATTERN = r"^[a-z][a-z0-9_]{0,31}$"
VERIFICATION_CHECK_LIMIT = 4
PYTHON_CHECK_ID = "python"
_DELEGATED_FORBIDDEN = re.compile(r"[\x00-\x1f\x7f`]")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PhaseDeclaration(_Strict):
    id: str = Field(pattern=PHASE_PATTERN)
    label: str = Field(min_length=1, max_length=40)


class LoopDeclaration(_Strict):
    start: str = Field(pattern=PHASE_PATTERN)
    review: str = Field(pattern=PHASE_PATTERN)
    cap: int = Field(ge=1, le=5)


class StageDeclaration(_Strict):
    id: str = Field(pattern=PHASE_PATTERN)
    label: str = Field(min_length=1, max_length=40)
    phases: list[str] = Field(min_length=1, max_length=12)


class Declaration(_Strict):
    phases: list[PhaseDeclaration] = Field(min_length=1, max_length=12)
    loops: list[LoopDeclaration] = Field(default_factory=list, max_length=4)
    stages: list[StageDeclaration] | None = Field(default=None, min_length=1, max_length=12)
    reviewer_model: str | None = Field(default=None, min_length=1, max_length=120)

    @field_validator("reviewer_model")
    @classmethod
    def _reviewer_not_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("reviewer_model must not be blank")
        return value

    @model_validator(mode="after")
    def _consistent(self) -> Declaration:
        ids = [phase.id for phase in self.phases]
        if len(set(ids)) != len(ids):
            raise ValueError("declaration phase ids must be unique")
        if VERIFICATION_PREFLIGHT_PHASE in ids:
            raise ValueError("verification_preflight is a reserved phase")
        for loop in self.loops:
            if loop.start not in ids or loop.review not in ids:
                raise ValueError("a loop names an undeclared phase")
            if ids.index(loop.start) >= ids.index(loop.review):
                raise ValueError("a loop's start must precede its review")
        if self.stages is not None:
            stage_ids = [stage.id for stage in self.stages]
            if len(set(stage_ids)) != len(stage_ids):
                raise ValueError("stage ids must be unique")
            grouped = [phase for stage in self.stages for phase in stage.phases]
            if grouped != ids:
                raise ValueError("stages must cover each declared phase once in order")
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
        if self.phase == VERIFICATION_PREFLIGHT_PHASE:
            raise ValueError("verification_preflight is a reserved phase")
        if self.phase not in {phase.id for phase in self.declaration.phases}:
            raise ValueError("phase is not declared")
        if self.round is not None:
            loop = self.declaration.loop_for(self.phase)
            if loop is None:
                raise ValueError("round is only valid on a looped phase")
            if not 1 <= self.round <= loop.cap:
                raise ValueError("round must be between 1 and the loop cap")
        return self


class VerificationObservation(_Strict):
    """One declared check's sandbox result recorded before the model starts.

    ``check`` is the declared check id and ``command`` the exact command the
    runner ran for it. ``not_declared`` records that neither the bundle nor the
    repository declared a check, so no command ran: its check, command and
    exit_status are null and it carries no blockers. ``delegated_to`` optionally
    names the required CI check that stands in for a declared check; it is
    never valid on ``not_declared``.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    check: str | None = Field(pattern=VERIFICATION_CHECK_PATTERN)
    command: str | None = Field(min_length=1, max_length=180)
    outcome: Literal["passed", "unavailable", "failed", "not_declared"]
    exit_status: int | None = Field(ge=-255, le=255)
    missing_binaries: list[str] = Field(max_length=8)
    blocked_services: list[str] = Field(max_length=8)
    delegated_to: str | None = Field(default=None, min_length=1, max_length=64)

    @field_validator("delegated_to")
    @classmethod
    def _delegated_to_printable(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value != value.strip() or _DELEGATED_FORBIDDEN.search(value):
            raise ValueError(
                "delegated_to must be printable characters without backticks or surrounding spaces"
            )
        return value

    @field_validator("command")
    @classmethod
    def _command_not_blank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("command must not be blank")
        return value

    @field_validator("missing_binaries", "blocked_services")
    @classmethod
    def _unique_nonblank_names(cls, values: list[str], info: ValidationInfo) -> list[str]:
        normalized: list[str] = []
        for value in values:
            name = value.strip()
            if not name or len(name) > 64:
                raise ValueError(f"{info.field_name} entries must be 1 to 64 characters")
            normalized.append(name)
        if len(set(normalized)) != len(normalized):
            raise ValueError(f"{info.field_name} entries must be unique")
        return normalized

    @model_validator(mode="after")
    def _consistent_result(self) -> VerificationObservation:
        has_blocker = bool(self.missing_binaries or self.blocked_services)
        if self.outcome == "not_declared":
            if (
                self.check is not None
                or self.command is not None
                or self.exit_status is not None
                or has_blocker
                or self.delegated_to is not None
            ):
                raise ValueError(
                    "not_declared requires a null check, command and exit_status, "
                    "no blockers and no delegated_to"
                )
        elif self.check is None or self.command is None:
            raise ValueError(f"{self.outcome} requires a declared check and its command")
        elif self.outcome == "unavailable":
            if self.exit_status is not None or not has_blocker:
                raise ValueError("unavailable requires blockers and a null exit_status")
        elif self.outcome == "passed":
            if self.exit_status != 0 or has_blocker:
                raise ValueError("passed requires exit_status 0 and no blockers")
        elif self.exit_status in (None, 0):
            raise ValueError("failed requires a nonzero exit_status")
        if len(verification_observation_note(self).encode("utf-8")) > 280:
            raise ValueError("verification observation exceeds the 280-byte storage limit")
        return self


def verification_observation_note(observation: VerificationObservation) -> str:
    """Serialize a validated observation into its compact, deterministic note.

    A ``None`` ``delegated_to`` is omitted: that is the canonical form, so every
    note stored before the field existed stays canonical.
    """

    record = observation.model_dump()
    if record["delegated_to"] is None:
        del record["delegated_to"]
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def verification_route(
    observation: VerificationObservation,
) -> Literal["sandbox", "required_ci", "blocked"] | None:
    """Where an observed check is proven: the sandbox, the required CI check it
    delegates to, or nowhere yet. ``None`` when no check was declared."""

    if observation.outcome in ("passed", "failed"):
        return "sandbox"
    if observation.outcome == "unavailable":
        return "required_ci" if observation.delegated_to is not None else "blocked"
    return None


@dataclass(frozen=True)
class RecordResult:
    outcome: Literal[
        "recorded",
        "replayed",
        "request_not_found",
        "no_active_request",
        "declaration_changed",
        "report_limit",
        "verification_exists",
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
    active = await lock_active_request(session, token_request.work_item_id)
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

    # Admitted before 0055 and still active: give it its status row now.
    await session.execute(
        insert(FactoryStatusComment)
        .values(
            execution_request_id=active_id,
            work_item_id=active.work_item_id,
            applied_label=None,
        )
        .on_conflict_do_nothing(index_elements=["execution_request_id"])
    )
    declaration = body.declaration.model_dump(exclude_none=True)
    await session.execute(
        update(FactoryStatusComment)
        .where(
            FactoryStatusComment.execution_request_id == active_id,
            FactoryStatusComment.declaration.is_(None),
        )
        .values(declaration=declaration)
    )
    stored = await session.scalar(
        select(FactoryStatusComment.declaration).where(
            FactoryStatusComment.execution_request_id == active_id
        )
    )
    if stored != declaration:
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
        await session.execute(
            update(FactoryStatusComment)
            .where(FactoryStatusComment.execution_request_id == active_id)
            .values(activity=body.activity.model_dump(exclude_none=True))
        )
    await session.commit()
    return RecordResult("recorded", active.id)


async def record_verification(
    session: AsyncSession,
    *,
    token_request_id: uuid.UUID,
    body: VerificationObservation,
) -> RecordResult:
    """Store one immutable preflight observation per declared check on the token's
    active request.

    A repeated check id with the same command is ``replayed``; the first
    observation stays authoritative, even when the replay's result differs.
    A changed command, mixing ``not_declared`` with declared checks, or a fifth
    distinct check is ``verification_exists``.
    """

    token_request: ExecutionRequest | None = await session.scalar(
        select(ExecutionRequest).where(ExecutionRequest.id == token_request_id).with_for_update()
    )
    if token_request is None:
        await session.rollback()
        return RecordResult("request_not_found")
    active = await lock_active_request(session, token_request.work_item_id)
    if active is None or active.id != token_request.id:
        await session.rollback()
        return RecordResult("no_active_request")
    active_id = active.id

    try:
        existing = await read_verification_observations(session, active_id)
    except ValueError:
        # Unreadable stored evidence is never extended; the gates fail closed on it.
        await session.rollback()
        return RecordResult("verification_exists", active_id)
    for observation in existing:
        if observation.check != body.check:
            continue
        if observation.command != body.command:
            await session.rollback()
            return RecordResult("verification_exists", active_id)
        if observation != body:
            logger.info(
                "verification replay retained first observation: "
                "request_id=%s check=%s stored_outcome=%s replayed_outcome=%s",
                active_id,
                body.check,
                observation.outcome,
                body.outcome,
            )
        await session.rollback()
        return RecordResult("replayed", active_id)
    if existing and (
        body.outcome == "not_declared"
        or len(existing) >= VERIFICATION_CHECK_LIMIT
        or any(observation.outcome == "not_declared" for observation in existing)
    ):
        await session.rollback()
        return RecordResult("verification_exists", active_id)

    count = await session.scalar(
        select(func.count())
        .select_from(ExecutionRequestPhaseReport)
        .where(ExecutionRequestPhaseReport.execution_request_id == active_id)
    )
    if (count or 0) >= REPORT_LIMIT:
        await session.rollback()
        return RecordResult("report_limit", active_id)

    session.add(
        ExecutionRequestPhaseReport(
            execution_request_id=active_id,
            phase=VERIFICATION_PREFLIGHT_PHASE,
            note=verification_observation_note(body),
        )
    )
    await session.commit()
    return RecordResult("recorded", active_id)


async def read_verification_observations(
    session: AsyncSession, request_id: uuid.UUID
) -> list[VerificationObservation]:
    """Read and validate the stored preflight observations in recorded order.

    An empty list means no preflight was recorded, which is distinct from an
    unreadable one. Malformed or noncanonical persisted data, a repeated check
    id, more than the check limit, or ``not_declared`` mixed with checks raises
    ``ValueError`` so callers can fail closed instead of treating it as a
    successful sandbox verification.
    """

    notes = list(
        await session.scalars(
            select(ExecutionRequestPhaseReport.note)
            .where(
                ExecutionRequestPhaseReport.execution_request_id == request_id,
                ExecutionRequestPhaseReport.phase == VERIFICATION_PREFLIGHT_PHASE,
            )
            .order_by(ExecutionRequestPhaseReport.id)
            .limit(VERIFICATION_CHECK_LIMIT + 1)
        )
    )
    if len(notes) > VERIFICATION_CHECK_LIMIT:
        raise ValueError("too many stored verification observations are unreadable")
    observations: list[VerificationObservation] = []
    for note in notes:
        if not isinstance(note, str):
            raise ValueError("stored verification observation is unreadable")
        try:
            raw = json.loads(note)
            observation = VerificationObservation.model_validate(raw)
        except (TypeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError("stored verification observation is unreadable") from exc
        if verification_observation_note(observation) != note:
            raise ValueError("stored verification observation is not canonical")
        observations.append(observation)
    checks = [observation.check for observation in observations]
    if len(observations) > 1 and None in checks:
        raise ValueError("not_declared mixed with other stored observations is unreadable")
    if len(set(checks)) != len(checks):
        raise ValueError("duplicate stored verification check ids are unreadable")
    return observations


def python_verification(
    observations: Sequence[VerificationObservation],
) -> VerificationObservation | None:
    """The observation of the declared ``python`` check, if one was recorded.

    Its absence stamps the not-declared publication pair when no check was
    unavailable; failures and unavailability under any check id are judged
    across every observation.
    """

    return next(
        (observation for observation in observations if observation.check == PYTHON_CHECK_ID),
        None,
    )


def failed_verification(
    observations: Sequence[VerificationObservation],
) -> VerificationObservation | None:
    """The failed observation that refuses a Python change, if any failed.

    A Python check can be declared under any id, so every failed check fails
    closed: the ``python`` check's failure first, otherwise the first failure
    in recorded order.
    """

    failed = [observation for observation in observations if observation.outcome == "failed"]
    return next(
        (observation for observation in failed if observation.check == PYTHON_CHECK_ID),
        failed[0] if failed else None,
    )


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


async def record_ci_rerun(session: AsyncSession, request_id: uuid.UUID, note: str) -> None:
    """Record one flake-rerun attempt on the run (#3741).

    The note is platform text, short enough for the status card, which renders
    the latest phase-report note. A second ``wait_ci`` row does not carry a
    loop round, so it does not consume a fix round.
    """

    if not 1 <= len(note) <= 280:
        raise ValueError("ci rerun note length")
    found = await session.scalar(
        select(ExecutionRequestPhaseReport.id).where(
            ExecutionRequestPhaseReport.execution_request_id == request_id,
            ExecutionRequestPhaseReport.note == note,
        )
    )
    if found is not None:
        await session.rollback()
        return
    session.add(
        ExecutionRequestPhaseReport(
            execution_request_id=request_id,
            phase=WAIT_CI_PHASE,
            note=note,
        )
    )
    await session.commit()


# --- the phase view (pure) ------------------------------------------------------

PhaseState = Literal["done", "current", "redo", "pending", "blocked"]


@dataclass(frozen=True)
class PhaseSlot:
    id: str
    label: str
    state: PhaseState
    round_label: str | None


@dataclass(frozen=True)
class StageSlot:
    id: str
    label: str
    phase_ids: tuple[str, ...]
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
    stage_start: str | None
    stage_review: str | None


@dataclass(frozen=True)
class PhaseView:
    phases: tuple[PhaseSlot, ...]
    loops: tuple[LoopView, ...]
    current: str | None
    stages: tuple[StageSlot, ...]
    reviewer_model: str | None
    staged: bool


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
    declared_stages = declaration.get("stages")
    staged = declared_stages is not None
    stage_specs = (
        [
            (str(stage["id"]), str(stage["label"]), tuple(str(p) for p in stage["phases"]))
            for stage in declared_stages
        ]
        if declared_stages is not None
        else [(phase_id, label, (phase_id,)) for phase_id, label in phases]
    )
    stage_by_phase = {
        phase_id: stage_id for stage_id, _label, phase_ids in stage_specs for phase_id in phase_ids
    }
    ordered = sorted(reports, key=lambda report: report.id or 0)
    completed = status == "completed"
    current: str | None = None
    if not completed:
        for report in ordered:
            if report.phase not in ids:
                continue
            loop_return = any(
                report.phase == str(loop["start"])
                and (
                    current == str(loop["review"])
                    or (current == WAIT_CI_PHASE and str(loop["review"]) == "review_diff")
                )
                for loop in declaration.get("loops", [])
            )
            if current is None or ids.index(report.phase) >= ids.index(current) or loop_return:
                current = report.phase
    reported = {report.phase for report in ordered}
    latest_ci = max(
        (index for index, report in enumerate(ordered) if report.phase == WAIT_CI_PHASE),
        default=-1,
    )
    latest_diff_review = max(
        (index for index, report in enumerate(ordered) if report.phase == "review_diff"),
        default=-1,
    )

    loops: list[LoopView] = []
    labels: dict[str, str] = {}
    redo: set[str] = set()
    for raw in declaration.get("loops", []):
        start, review, cap = str(raw["start"]), str(raw["review"]), int(raw["cap"])
        if staged:
            kickbacks = 0
            saw_review = False
            for report in ordered:
                if report.phase == review:
                    saw_review = True
                elif report.phase == start:
                    if saw_review:
                        kickbacks += 1
                    saw_review = False
                elif review == "review_diff" and report.phase == WAIT_CI_PHASE:
                    # A CI retry starts a new diff pass, not a reviewer kickback.
                    saw_review = False
            if review == WAIT_CI_PHASE:
                marker_rounds = [
                    report.loop_round
                    for report in ordered
                    if report.phase == review and report.loop_round is not None
                ]
                kickbacks = max(kickbacks, max(marker_rounds, default=1) - 1)
            loop_round = min(cap, kickbacks + 1)
        else:
            rounds = [
                report.loop_round
                for report in ordered
                if report.phase in (start, review) and report.loop_round is not None
            ]
            loop_round = max(rounds, default=1)
            kickbacks = loop_round - 1
        after = ids[ids.index(review) + 1 :] if review in ids else []
        approved = completed or (
            review != WAIT_CI_PHASE and any(phase in reported for phase in after)
        )
        active = status in ("running", "cancellation_requested") and current in (start, review)
        if staged and review == "review_diff" and current == start:
            # A CI return does not undo the approval of the preceding diff review.
            active = active and latest_ci <= latest_diff_review
        if staged and review == WAIT_CI_PHASE:
            active = status == "running" and (
                current == review or (current == start and latest_ci > latest_diff_review)
            )
        if active:
            round_label = f"round {loop_round} of {cap}"
            if review != WAIT_CI_PHASE:
                labels[start] = round_label
            labels[review] = round_label
        elif approved:
            noun = "round" if loop_round == 1 else "rounds"
            separator = " · " if staged else ", "
            labels[review] = f"approved{separator}{loop_round} {noun}"
        if active and current == start and kickbacks >= 1:
            redo.add(review)
        loops.append(
            LoopView(
                start=start,
                review=review,
                cap=cap,
                round=loop_round,
                kickbacks=kickbacks,
                approved=approved,
                active=active,
                stage_start=stage_by_phase.get(start),
                stage_review=stage_by_phase.get(review),
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
    current_stage = stage_by_phase.get(current) if current is not None else None
    stage_ids = [stage_id for stage_id, _label, _phase_ids in stage_specs]
    current_stage_index = stage_ids.index(current_stage) if current_stage is not None else -1
    approved_ci_return_stages = {
        loop.stage_review
        for loop in loops
        if loop.review == "review_diff"
        and loop.approved
        and current == loop.start
        and latest_ci > latest_diff_review
    }
    stages: list[StageSlot] = []
    for index, (stage_id, label, phase_ids) in enumerate(stage_specs):
        stage_state: PhaseState
        if completed:
            stage_state = "done"
        elif stage_id == current_stage:
            stage_state = "blocked" if status in ("failed", "expired") else "current"
        elif stage_id in approved_ci_return_stages:
            stage_state = "done"
        elif any(phase_id in redo for phase_id in phase_ids):
            stage_state = "redo"
        elif index < current_stage_index:
            stage_state = "done"
        else:
            stage_state = "pending"
        stage_round_label = next((labels[p] for p in phase_ids if p in labels), None)
        stages.append(StageSlot(stage_id, label, phase_ids, stage_state, stage_round_label))
    return PhaseView(
        phases=tuple(slots),
        loops=tuple(loops),
        current=current,
        stages=tuple(stages),
        reviewer_model=declaration.get("reviewer_model"),
        staged=staged,
    )


_PILLS: dict[str, tuple[str, str, bool]] = {
    "queued": ("QUEUED", "#9a6700", False),
    "waiting": ("QUEUED", "#9a6700", False),
    "running": ("RUNNING", "#2f81f7", True),
    "cancellation_requested": ("STOPPING", "#bc4c00", True),
    "completed": ("SUCCEEDED", "#1a7f37", False),
    # Failed and expired runs need a human unless an owner_lost successor was admitted.
    "failed": ("NEEDS HUMAN", "#bf8700", False),
    "expired": ("NEEDS HUMAN", "#bf8700", False),
    "cancelled": ("CANCELLED", "#6e7781", False),
}
_PUBLISHING = ("PUBLISHING", "#8250df", True)
_RETRYING = ("RETRYING", "#2f81f7", False)
_UNKNOWN_COLOR = "#6e7781"


def pill_for(status: str, publishing: bool, *, retrying: bool = False) -> tuple[str, str, bool]:
    """The status pill: label, color, and whether its dot pulses."""

    if retrying:
        return _RETRYING
    if status == "running" and publishing:
        return _PUBLISHING
    return _PILLS.get(status, (status.upper(), _UNKNOWN_COLOR, False))
