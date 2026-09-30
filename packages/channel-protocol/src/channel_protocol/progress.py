"""Deliberate progress: the closed vocabulary and its three models (ADR-0130).

``ProgressCommand`` is what a model may submit through the platform's
``curie_progress`` tool. It never reaches an adapter. ``ProgressCard`` and
``ProgressMilestone`` are the rendering-free payloads the platform sends
adapters in the reply wire's 1.1 ``progress`` field (``channel_protocol.reply``).

Every model is closed. For the command that is the point of ADR-0130 section 1:
the model cannot supply a channel kind, channel address, reply ref, endpoint,
credential, adapter payload, delivery id, progress record or milestone budget,
because no field accepts one, and an unknown field is refused rather than
ignored. The command names no progress record at all: which record it updates
is for the ingress to resolve from the authenticated running turn, never for the
command to say.

A card whose ``terminal`` flag disagrees with its state is refused with the
error type ``progress_terminal``, for the reason ``channel_protocol.reply`` gives
for its own rule types.

``progress_text`` is the one channel-neutral plain-text rendering of a card or a
milestone, for a channel with nothing richer, and ``progress_heading`` is its
first half, for a renderer that shows the summary separately.
"""

from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from pydantic_core import PydanticCustomError

ProgressCommandVersion = Literal["1.0"]
PROGRESS_COMMAND_VERSION: ProgressCommandVersion = "1.0"
MAX_PROGRESS_MILESTONES = 3
_STRICT = ConfigDict(extra="forbid")


class ProgressState(StrEnum):
    """The initial state set, in ADR-0130 section 1's order."""

    QUEUED = "queued"
    INVESTIGATING = "investigating"
    AWAITING_APPROVAL = "awaiting-approval"
    PREPARING_WORKSPACE = "preparing-workspace"
    TESTING = "testing"
    PUBLISHING = "publishing"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_PROGRESS_STATES: frozenset[ProgressState] = frozenset(
    {ProgressState.COMPLETE, ProgressState.FAILED, ProgressState.CANCELLED}
)


class MilestoneClass(StrEnum):
    """Why a durable interruption is warranted (ADR-0130 section 3).

    A class is a reason, not a step: a chain may skip one or repeat one while
    its milestone budget lasts.
    """

    EVIDENCE = "evidence"
    """Intake, or material evidence acquired."""
    SCOPE = "scope"
    """A material hypothesis or scope change."""
    VERIFICATION = "verification"
    """A verification result."""


# Every character ``str.splitlines`` breaks on. The pattern, not a Python
# validator, is what refuses them, so the committed JSON Schema carries the same
# rule to a consumer in another language.
_LINE_BOUNDARIES = "\n\r\x0b\x0c\x1c\x1d\x1e\x85  "

ProgressSummary = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=200,
        pattern=f"^[^{_LINE_BOUNDARIES}]+$",
    ),
]
"""One line of short task state. Never reasoning, tool output or a draft answer."""

UpdateId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")]


class ProgressCommand(BaseModel):
    """One deliberate progress update, as a model submits it."""

    model_config = _STRICT

    version: ProgressCommandVersion
    update_id: UpdateId = Field(
        description="Identifies this update within its record, for idempotency."
    )
    state: ProgressState
    summary: ProgressSummary
    milestone: MilestoneClass | None = Field(
        default=None,
        description=(
            "Requests a durable milestone reply of this class. The milestone "
            "budget is the platform's to apply, not the command's to state."
        ),
    )


class ProgressCard(BaseModel):
    """The semantic form of a turn chain's one mutable progress card."""

    model_config = _STRICT

    kind: Literal["card"]
    state: ProgressState
    summary: ProgressSummary
    revision: int = Field(
        ge=1,
        description=(
            "Counts the record's accepted updates from 1. A higher revision "
            "supersedes a lower one, so an adapter can drop a stale redelivery."
        ),
    )
    terminal: bool = Field(
        description=(
            "True exactly when state is complete, failed or cancelled. Carried so "
            "an adapter renders the closed card without its own copy of that set."
        )
    )

    @model_validator(mode="after")
    def _terminal_matches_state(self) -> Self:
        if self.terminal != (self.state in TERMINAL_PROGRESS_STATES):
            raise PydanticCustomError(
                "progress_terminal",
                "terminal must be true exactly when state is complete, failed or "
                "cancelled; got state {state} with terminal {terminal}",
                {"state": self.state.value, "terminal": self.terminal},
            )
        return self


class ProgressMilestone(BaseModel):
    """One durable milestone reply of a turn chain."""

    model_config = _STRICT

    kind: Literal["milestone"]
    milestone: MilestoneClass
    summary: ProgressSummary
    ordinal: int = Field(
        ge=1,
        le=MAX_PROGRESS_MILESTONES,
        description=(
            "The chain's milestone slot this reply holds, from 1. The bound makes "
            "a fourth milestone inexpressible on the wire."
        ),
    )


# The words a reader sees for each state and milestone class. Closed like the
# enums they key, so a value added there without words here fails
# ``test_every_state_and_class_has_a_heading`` rather than rendering a slug.
_STATE_WORDS: dict[ProgressState, str] = {
    ProgressState.QUEUED: "Queued",
    ProgressState.INVESTIGATING: "Investigating",
    ProgressState.AWAITING_APPROVAL: "Waiting for approval",
    ProgressState.PREPARING_WORKSPACE: "Preparing the workspace",
    ProgressState.TESTING: "Testing",
    ProgressState.PUBLISHING: "Publishing",
    ProgressState.COMPLETE: "Complete",
    ProgressState.FAILED: "Failed",
    ProgressState.CANCELLED: "Cancelled",
}
_MILESTONE_WORDS: dict[MilestoneClass, str] = {
    MilestoneClass.EVIDENCE: "Evidence acquired",
    MilestoneClass.SCOPE: "Scope changed",
    MilestoneClass.VERIFICATION: "Verification result",
}


def progress_heading(progress: ProgressCard | ProgressMilestone) -> str:
    """The line a reader sees above the summary, in plain words.

    @spec ADR-0130 d2, d3. A closed card says so in the heading itself, so a
    renderer that shows only this line still shows the card is closed.
    """

    if isinstance(progress, ProgressMilestone):
        return f"Milestone: {_MILESTONE_WORDS[progress.milestone]}"
    words = _STATE_WORDS[progress.state]
    if progress.terminal:
        return f"Task {words.lower()}"
    return f"Task status: {words}"


def progress_text(progress: ProgressCard | ProgressMilestone) -> str:
    """The channel-neutral plain text of a card or a milestone.

    @spec ADR-0130 d2. A card edit carries no ``message``, so a channel with
    nothing richer takes its text from here. The summary is carried verbatim:
    escaping it, or disabling mentions, is each channel's own rendering job.
    """

    return f"{progress_heading(progress)}. {progress.summary}"
