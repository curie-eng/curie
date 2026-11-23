"""The bounded run-handoff payload shared by the runner and API (#5771)."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Finding(_Strict):
    summary: str = Field(min_length=1, max_length=160)
    owner: Literal["agent", "human", "platform", "unknown"]
    next_step: str = Field(min_length=1, max_length=160)
    source: Literal["agent_reported"]


CriterionId = Annotated[str, Field(pattern=r"^AC[1-9][0-9]*$")]


class Verification(_Strict):
    check: str = Field(min_length=1, max_length=100)
    command: str = Field(min_length=1, max_length=240)
    state: Literal["passed", "failed", "unverified", "not_run"]
    source: Literal["agent_reported"]
    acceptance_criteria: list[CriterionId] = Field(
        min_length=1, max_length=10, json_schema_extra={"uniqueItems": True}
    )

    @field_validator("acceptance_criteria")
    @classmethod
    def _unique_criterion_ids(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("acceptance criteria must not repeat within one check")
        return value


class RunHandoff(_Strict):
    next_owner: Literal["agent", "human", "platform", "unknown"]
    next_action: str = Field(min_length=1, max_length=200)
    human_decision: str | None = Field(default=None, min_length=1, max_length=200)
    later_gate: str | None = Field(default=None, min_length=1, max_length=200)
    findings: list[Finding] = Field(default_factory=list, max_length=10)
    verification: list[Verification] = Field(default_factory=list, max_length=20)

    @field_validator("next_action", "human_decision", "later_gate")
    @classmethod
    def _nonblank(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("handoff text must not be blank")
        return value
