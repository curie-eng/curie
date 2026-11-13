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
_TRACKER_RESULT: Final = "tracker_result"


def terminal_post_owed(requests_alias: str, deliveries: str) -> str:
    """A SQL condition: the generic terminal post of ``requests_alias`` is owed.

    True for every implementation request; for a tracker-result request only
    while it has no owed or delivered result.
    """

    return f"""
        ({requests_alias}.completion_contract <> '{_TRACKER_RESULT}'
         OR NOT EXISTS (
             SELECT 1
               FROM {deliveries} d
              WHERE d.execution_request_id = {requests_alias}.id
                AND d.state IN ('owed', 'delivered')
         ))
    """


def triage_posts_sql(*, posts: str, requests: str, receipts: str, live_threads: str) -> str:
    """The insert of one ``triage`` post per delivered result of an eligible request."""

    return f"""
        INSERT INTO {posts} AS p
               (work_item_id, post_key, kind, sequence, rank, ordinal, payload)
        SELECT t.work_item_id, 'triage:' || q.id, '{TRIAGE_KIND}', q.sequence, 3, 0,
               jsonb_build_object(
                   'result_kind', r.result_kind,
                   'reason', r.reason,
                   'revision', r.revision,
                   'ticket_url', r.output_url,
                   'result_comment_url', r.result_comment_url
               )
          FROM ({live_threads}) t
          JOIN {requests} q
            ON q.work_item_id = t.work_item_id AND q.sequence >= t.first_sequence
          JOIN {receipts} r
            ON r.execution_request_id = q.id
         WHERE q.completion_contract = '{_TRACKER_RESULT}'
        ON CONFLICT (work_item_id, post_key) DO NOTHING
    """


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


__all__ = ["TRIAGE_KIND", "render_triage", "terminal_post_owed", "triage_posts_sql"]
