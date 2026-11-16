"""The pure phase view of a factory run's declared stages (#3077, #5715).

The status comment, the SVG card, the console progress route and the Slack
progress card all derive a run's step states from the same function, so it
lives here, importable by both the API and the worker. It reads only the
bundle's progress declaration and the request's ordered phase reports.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

WAIT_CI_PHASE = "wait_ci"


class PhaseReport(Protocol):
    """The fields of one recorded phase report the view reads."""

    @property
    def id(self) -> int | None: ...

    @property
    def phase(self) -> str: ...

    @property
    def loop_round(self) -> int | None: ...


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
    reports: Sequence[PhaseReport],
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
            completion_label = "approved" if review == WAIT_CI_PHASE else "completed"
            labels[review] = f"{completion_label}{separator}{loop_round} {noun}"
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
