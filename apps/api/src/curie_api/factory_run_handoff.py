"""Bounded, agent-sourced facts for a terminal tracker summary (#5771)."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .factory_comment_text import redact_factory_comment


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Finding(_Strict):
    summary: str = Field(min_length=1, max_length=160)
    owner: Literal["agent", "human", "platform", "unknown"]
    next_step: str = Field(min_length=1, max_length=160)
    source: Literal["agent_reported"]


class Verification(_Strict):
    check: str = Field(min_length=1, max_length=100)
    command: str | None = Field(default=None, min_length=1, max_length=240)
    state: Literal["passed", "failed", "unverified", "not_run"]
    source: Literal["agent_reported"]
    acceptance_criteria: list[str] = Field(default_factory=list, max_length=10)

    @field_validator("acceptance_criteria")
    @classmethod
    def _criterion_ids(cls, value: list[str]) -> list[str]:
        if any(not re.fullmatch(r"AC[1-9][0-9]*", item) for item in value):
            raise ValueError("acceptance criteria must use numbered AC identifiers")
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


_MARKDOWN = re.compile(r"([\\`*_{}\[\]()#+.!|>])")
_CUSTOMER_IDENTIFIER = re.compile(
    r"(?<![./@-])\b(?P<label>(?i:customer|client|tenant|account|organization|org))"
    r"(?P<separator>\s*(?:[:#=]\s*|\s+))"
    r"(?P<value>(?!(?i:to|for|needs|should|is|has|can|must|will|the|a|an|with)\b)"
    r"[A-Za-z0-9._-]+(?:\s+[A-Z][A-Za-z0-9._-]+){0,3})\b"
)


def redact_tracker_text(value: str) -> str:
    """Redact known credentials and explicit customer identifier labels."""

    line = " ".join(redact_factory_comment(value).split())
    return _CUSTOMER_IDENTIFIER.sub(
        lambda match: f"{match['label']}{match['separator']}[REDACTED:customer_identifier]",
        line,
    )


def safe_tracker_line(value: str) -> str:
    """One inert tracker line, after the shared credential redactor."""

    line = redact_tracker_text(value)
    line = line.replace("<!--", "<\u200b!--").replace("@", "@\u200b")
    return _MARKDOWN.sub(r"\\\1", line)


def format_handoff(
    handoff: RunHandoff,
    *,
    no_pull_request: bool,
    platform_checks: Sequence[tuple[str, str | None, str, str]] = (),
) -> str:
    """Present only attributed agent claims; publication remains a platform fact."""

    lines = ["## Run outcome"]
    decision = (
        safe_tracker_line(handoff.human_decision) if handoff.human_decision else "None reported"
    )
    if no_pull_request:
        lines.append("**No pull request was published.**")
    lines.extend(
        [
            f"**Next owner:** {handoff.next_owner} (agent reported)",
            f"**Next action:** {safe_tracker_line(handoff.next_action)} (agent reported)",
            f"**Human decision now:** {decision} (agent reported)",
            "",
            "### Open findings",
        ]
    )
    lines.extend(
        f"- {safe_tracker_line(item.summary)} · owner: {item.owner} · "
        f"next: {safe_tracker_line(item.next_step)} (agent reported)"
        for item in handoff.findings
    )
    if not handoff.findings:
        lines.append("None reported; this is not a finding-free verification.")
    lines.extend(["", "### Verification against acceptance criteria"])
    mapped = [item for item in handoff.verification if item.acceptance_criteria]
    for item in mapped:
        for criterion in item.acceptance_criteria:
            command = safe_tracker_line(item.command) if item.command else "not reported"
            lines.append(
                f"- **{criterion} — {safe_tracker_line(item.check)}:** "
                f"{item.state.replace('_', ' ')} (agent reported) · **Command:** {command}"
            )
    if not mapped:
        lines.append("No acceptance-criterion results reported.")
    lines.extend(["", "### Other verification observations"])
    for check, platform_command, state, source in platform_checks:
        command_text = safe_tracker_line(platform_command) if platform_command else "not recorded"
        lines.append(
            f"- **{safe_tracker_line(check)}:** {state} ({source}) · "
            f"**Command:** {command_text}"
        )
    for item in handoff.verification:
        if item.acceptance_criteria:
            continue
        command = safe_tracker_line(item.command) if item.command else "not reported"
        lines.append(
            f"- **Unmapped check — {safe_tracker_line(item.check)}:** "
            f"{item.state.replace('_', ' ')} (agent reported) · **Command:** {command}"
        )
    if not platform_checks and all(item.acceptance_criteria for item in handoff.verification):
        lines.append("No other checks reported.")
    if handoff.later_gate:
        lines.extend(
            ["", f"**Later merge gate:** {safe_tracker_line(handoff.later_gate)} (agent reported)"]
        )
    lines.append("")
    return "\n".join(lines)


def fallback_summary(
    *,
    status: str,
    no_pull_request: bool,
    retrying: bool = False,
    platform_checks: Sequence[tuple[str, str | None, str, str]] = (),
    agent_detail: str | None = None,
) -> str:
    """Keep platform facts separate from a bounded, attributed agent fallback."""

    reason, question = _agent_failure_lines(agent_detail) if no_pull_request else (None, None)
    lines = ["## Run outcome"]
    if no_pull_request:
        lines.append("**No pull request was published.**")
    if retrying:
        action = "allow the recorded automatic retry to run."
    elif status == "completed":
        action = "review the published pull request or recorded answer."
    elif status == "cancelled":
        action = "review the cancellation before selecting this issue again."
    elif reason is not None:
        action = "resolve the agent-reported blocker below before retrying."
    else:
        action = "inspect the recorded cause and run evidence before retrying."
    lines.extend(
        [
            "**Next owner:** unknown; no structured handoff was recorded.",
            f"**Next action:** {action}",
            (
                f"**Human decision now:** {question} (agent reported)"
                if question is not None else "**Human decision now:** none recorded."
            ),
            *([f"**Agent-reported reason:** {reason}"] if reason is not None else []),
            *(["**Recovery of uncommitted work:** unknown."] if status != "completed" else []),
            "",
            "### Verification against acceptance criteria",
            "No acceptance-criterion results recorded; no structured handoff was saved.",
            "",
            "### Other verification observations",
        ]
    )
    for check, command, state, source in platform_checks:
        command_text = safe_tracker_line(command) if command else "not recorded"
        lines.append(
            f"- **{safe_tracker_line(check)}:** {state} ({source}) · "
            f"**Command:** {command_text}"
        )
    if not platform_checks:
        lines.append("No structured verification observations recorded.")
    lines.append("")
    return "\n".join(lines)


def _agent_failure_lines(detail: str | None) -> tuple[str | None, str | None]:
    """Take only the declared reason and first question from an agent final."""

    if not detail:
        return None, None
    lines = [
        line.strip() for line in detail.replace("\r\n", "\n").replace("\r", "\n").splitlines()
    ]
    if not lines or not lines[0].startswith("Could not complete:"):
        return None, None

    def bounded(value: str) -> str | None:
        rendered = safe_tracker_line(value.strip())
        if not rendered:
            return None
        return rendered if len(rendered) <= 240 else rendered[:237].rstrip() + "..."

    reason = bounded(lines[0].removeprefix("Could not complete:"))
    question = None
    if "Decisions needed:" in lines:
        after = lines.index("Decisions needed:") + 1
        if after < len(lines):
            match = re.fullmatch(r"[1-9][0-9]*\.\s+(.+)", lines[after])
            if match:
                question = bounded(match.group(1))
    return reason, question
