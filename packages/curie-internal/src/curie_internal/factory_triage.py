"""The shared ending vocabulary of a factory triage run (ADR 0277, #5586 decision 12).

A tracker-result request ends in one of seven ways once its result is
delivered. The API's status comment and state label and the worker's Slack
progress line all derive the ending here, from the delivered receipt's facts
(``TriageResult``) and, once #5588 records it, the implementation handoff
(``HandoffOutcome``). Nothing here reads a database or the network.

The status comment states the ending, the revision when known and plain links:
the published ticket for ticket endings, and the marked result comment, which
carries the reproduction command and observed result or what was tried and
what is missing. Submitted text is never repeated here (the stage 1 storage
boundary keeps it out of workflow rows).
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from typing import Final

TRIAGED_LABEL: Final = "curie-factory:triaged"

TICKET: Final = "ticket"
FINDINGS: Final = "findings"

_REVISION = re.compile(r"^[0-9a-f]{40}$")


class TriageEnding(enum.StrEnum):
    """The seven endings, with a delivered ticket that has no handoff yet as its own member."""

    TICKET_IMPLEMENTATION_ADMITTED = "ticket_implementation_admitted"
    TICKET_IMPLEMENTATION_REFUSED = "ticket_implementation_refused"
    TICKET_PUBLISHED = "ticket_published"
    NOT_REPRODUCED = "not_reproduced"
    ALREADY_FIXED = "already_fixed"
    PRODUCT_DECISION_NEEDED = "product_decision_needed"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"
    BUDGET_EXHAUSTED = "budget_exhausted"


_FINDINGS_ENDINGS: Final = {
    "not_reproduced": TriageEnding.NOT_REPRODUCED,
    "already_fixed": TriageEnding.ALREADY_FIXED,
    "product_decision_needed": TriageEnding.PRODUCT_DECISION_NEEDED,
    "capability_unavailable": TriageEnding.CAPABILITY_UNAVAILABLE,
    "budget_exhausted": TriageEnding.BUDGET_EXHAUSTED,
}


@dataclass(frozen=True)
class TriageResult:
    """The facts of one delivered result, as its receipt records them.

    ``ticket_url`` is the published linked issue (ticket results only);
    ``result_comment_url`` is the marked result comment on the source issue.
    """

    result_kind: str
    reason: str | None
    revision: str | None
    ticket_url: str | None
    result_comment_url: str | None


@dataclass(frozen=True)
class HandoffOutcome:
    """Whether the published ticket's implementation was admitted (#5588 records it)."""

    admitted: bool
    implementation_status_url: str | None
    refusal_reason: str | None


def _plain(value: str | None) -> str | None:
    """One line, no HTML comment opener; None when blank."""

    if value is None:
        return None
    line = " ".join(value.split()).replace("<!--", "<​!--")
    return line or None


def ending_for(result: TriageResult, handoff: HandoffOutcome | None) -> TriageEnding:
    """The ending a delivered result shows.

    Raises:
        ValueError: on a result kind or findings reason outside the vocabulary,
            a ticket with no ticket link, or a findings result with no reason.
    """

    if result.result_kind == TICKET:
        if _plain(result.ticket_url) is None:
            raise ValueError("a ticket result needs its ticket link")
        if handoff is None:
            return TriageEnding.TICKET_PUBLISHED
        if handoff.admitted:
            return TriageEnding.TICKET_IMPLEMENTATION_ADMITTED
        return TriageEnding.TICKET_IMPLEMENTATION_REFUSED
    if result.result_kind == FINDINGS:
        ending = _FINDINGS_ENDINGS.get(result.reason or "")
        if ending is None:
            raise ValueError(f"unknown findings reason {result.reason!r}")
        return ending
    raise ValueError(f"unknown result kind {result.result_kind!r}")


_FINDINGS_HEADLINE: Final = {
    TriageEnding.NOT_REPRODUCED: (
        "Triage reported: not reproduced. No implementation ticket was produced."
    ),
    TriageEnding.ALREADY_FIXED: (
        "Triage reported: already fixed. The reported behavior no longer fails."
    ),
    TriageEnding.PRODUCT_DECISION_NEEDED: (
        "Triage reported: product decision needed. An unresolved choice prevents a ticket."
    ),
    TriageEnding.CAPABILITY_UNAVAILABLE: (
        "Triage reported: capability unavailable. The investigation could not verify the report."
    ),
    TriageEnding.BUDGET_EXHAUSTED: (
        "Investigation stopped: budget exhausted; findings delivered. "
        "The investigation reached its limit before it finished."
    ),
}
_REPRODUCTION_LINE: Final = "Reproduction command and observed result: {url}"
_EVIDENCE_LINE: Final = {
    TriageEnding.TICKET_IMPLEMENTATION_ADMITTED: _REPRODUCTION_LINE,
    TriageEnding.TICKET_IMPLEMENTATION_REFUSED: _REPRODUCTION_LINE,
    TriageEnding.TICKET_PUBLISHED: _REPRODUCTION_LINE,
    TriageEnding.NOT_REPRODUCED: "What ran, the observed behavior and the missing evidence: {url}",
    TriageEnding.ALREADY_FIXED: "The observed passing behavior: {url}",
    TriageEnding.PRODUCT_DECISION_NEEDED: (
        "The unresolved choice and why it prevents a ticket: {url}"
    ),
    TriageEnding.CAPABILITY_UNAVAILABLE: (
        "The capability, the failed observation and what could not be verified: {url}"
    ),
    TriageEnding.BUDGET_EXHAUSTED: "The limit reached and what was checked: {url}",
}


def status_comment_text(result: TriageResult, handoff: HandoffOutcome | None) -> str:
    """The terminal result lines of the source issue's status comment, newline terminated."""

    ending = ending_for(result, handoff)
    lines: list[str] = []
    if result.result_kind == TICKET:
        lines.append(f"Ticket published: {_plain(result.ticket_url)}.")
    else:
        lines.append(_FINDINGS_HEADLINE[ending])
    revision = _plain(result.revision)
    if revision is not None and _REVISION.fullmatch(revision):
        lines.append(
            "Revision checked out: " + revision
            if ending is TriageEnding.BUDGET_EXHAUSTED
            else "Revision: " + revision
        )
    if ending is TriageEnding.TICKET_IMPLEMENTATION_ADMITTED:
        assert handoff is not None
        status_url = _plain(handoff.implementation_status_url)
        lines.append(
            f"Implementation admitted: {status_url}"
            if status_url is not None
            else "Implementation admitted."
        )
    elif ending is TriageEnding.TICKET_IMPLEMENTATION_REFUSED:
        assert handoff is not None
        reason = _plain(handoff.refusal_reason)
        lines.append(
            f"Implementation not started: {reason.rstrip('.')}."
            if reason is not None
            else "Implementation not started."
        )
    comment_url = _plain(result.result_comment_url)
    if comment_url is not None:
        lines.append(_EVIDENCE_LINE[ending].format(url=comment_url))
    return "\n".join(lines) + "\n"


_SLACK_SENTENCE: Final = {
    TriageEnding.NOT_REPRODUCED: "Triage reported: not reproduced.",
    TriageEnding.ALREADY_FIXED: "Triage reported: already fixed.",
    TriageEnding.PRODUCT_DECISION_NEEDED: "Triage reported: product decision needed.",
    TriageEnding.CAPABILITY_UNAVAILABLE: "Triage reported: capability unavailable.",
    TriageEnding.BUDGET_EXHAUSTED: "Investigation stopped: budget exhausted; findings delivered.",
}


def slack_line(result: TriageResult, handoff: HandoffOutcome | None) -> str:
    """The one Slack line of the ending, carrying the ticket or result comment link.

    Ticket endings carry the ticket link in the sentence; findings endings
    append the source issue's marked result comment link when it is known.
    The text is unescaped; the Slack sender escapes it.
    """

    ending = ending_for(result, handoff)
    if result.result_kind == TICKET:
        line = f"Triage published a ticket: {_plain(result.ticket_url)}."
        if ending is TriageEnding.TICKET_IMPLEMENTATION_ADMITTED:
            line += " Implementation admitted."
        elif ending is TriageEnding.TICKET_IMPLEMENTATION_REFUSED:
            assert handoff is not None
            reason = _plain(handoff.refusal_reason)
            line += (
                f" Implementation not started: {reason.rstrip('.')}."
                if reason is not None
                else " Implementation not started."
            )
        return line
    line = _SLACK_SENTENCE[ending]
    comment_url = _plain(result.result_comment_url)
    if comment_url is not None:
        line += f" {comment_url}"
    return line


__all__ = [
    "FINDINGS",
    "TICKET",
    "TRIAGED_LABEL",
    "HandoffOutcome",
    "TriageEnding",
    "TriageResult",
    "ending_for",
    "slack_line",
    "status_comment_text",
]
