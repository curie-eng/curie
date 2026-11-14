"""The factory Slack line of a tracker-result (triage) request (#5586 decision 12, AC8).

A tracker-result request whose result was delivered (its receipt exists) owes
one ``triage`` post, key ``triage:<request id>``, rendered with
``curie_internal.factory_triage.slack_line``. Its generic ``terminal`` post is
withheld while its delivery is owed or delivered, and used when it has no
delivery or the delivery failed, so the thread carries exactly one result line
per request. A failed delivery never resumes, so a request never gets both.
Implementation requests have no delivery and post exactly as before.

The payload holds the receipt's structured facts only; the line is rendered at
delivery, and the progress store escapes it.
"""

from __future__ import annotations

from typing import Any, Final

from curie_internal.factory_triage import HandoffOutcome, TriageResult, slack_line

TRIAGE_KIND: Final = "triage"


def _text(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    return value if isinstance(value, str) and value else None


def render_triage(payload: dict[str, Any]) -> str:
    """The unescaped Slack line of a ``triage`` post.

    Raises:
        ValueError: when the payload names no result the vocabulary knows.
    """

    result = TriageResult(
        result_kind=_text(payload, "result_kind") or "",
        reason=_text(payload, "reason"),
        revision=_text(payload, "revision"),
        ticket_url=_text(payload, "ticket_url"),
        result_comment_url=_text(payload, "result_comment_url"),
    )
    return slack_line(result, _handoff(payload))


def _handoff(payload: dict[str, Any]) -> HandoffOutcome | None:
    """The optional handoff outcome (#5588 records it); absent keys mean None."""

    admitted = payload.get("handoff_admitted")
    if admitted is None:
        return None
    if not isinstance(admitted, bool):
        raise ValueError("handoff_admitted must be a boolean")
    values: dict[str, str | None] = {}
    for key in ("implementation_status_url", "refusal_reason"):
        value = payload.get(key)
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{key} must be a string")
        values[key] = value or None
    return HandoffOutcome(
        admitted=admitted,
        implementation_status_url=values["implementation_status_url"],
        refusal_reason=values["refusal_reason"],
    )


__all__ = ["TRIAGE_KIND", "render_triage"]
