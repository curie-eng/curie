"""Assertions of the scripted ``one-ticket`` scenario (#5788).

One label on one GitHub issue leads to questions on that issue, a plain answer
and one pull request that closes the same issue. The scenario drives the
installed platform with only the model and GitHub boundaries scripted, then
reads the outcome from outside: the GitHub stub's issues, comments and pull
requests, and read-only SQL. Each function below takes those plain
observations and raises ``OneTicketAssertionError`` when one of the flow's
points does not hold. They read no prompt or skill text.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

ISSUE = 3911
LABEL = "factory"
QUESTIONS_LABEL = "curie-factory:questions"
QUESTIONS_MARKER = "curie-factory-questions:"
INVESTIGATING_LINE = "Investigating this ticket."
ANSWER_RECEIVED_LINE = "Answer received."
# The delivery's bound on a first visible response, and on the answer's two
# visible effects.
RESPONSE_SECONDS = 180
EVIDENCE_KEY = "one_ticket"
# A request in one of these statuses is still owned by the platform.
LIVE_STATUSES = frozenset({"queued", "waiting", "running", "cancellation_requested"})


class OneTicketAssertionError(AssertionError):
    """The one-ticket scenario observed something the flow forbids."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise OneTicketAssertionError(message)


def instant(value: Any) -> datetime:
    """An ISO 8601 timestamp, from the stub (``Z``) or from SQL (``+00:00``), as an aware time."""

    text = str(value).replace("Z", "+00:00")
    moment = datetime.fromisoformat(text)
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _within(start: datetime, end: datetime | None, seconds: int, what: str) -> None:
    _require(end is not None, f"{what} was never seen")
    assert end is not None
    elapsed = (end - start).total_seconds()
    _require(elapsed >= 0, f"{what} was seen {-elapsed:.0f} seconds before its cause")
    _require(elapsed <= seconds, f"{what} took {elapsed:.0f} seconds, bound {seconds}")


def first_line_instant(
    history: Iterable[Mapping[str, Any]], marker: str, line: str
) -> datetime | None:
    """The instant a comment first held both ``marker`` and ``line``, from its body history.

    The marker binds the line to one execution request's own status comment, so a
    line another request's comment carried, or carried earlier, never counts.
    """

    for revision in history:
        body = revision["body"]
        if marker in body and line in body:
            return instant(revision["at"])
    return None


def labels_of(issue: Mapping[str, Any]) -> set[str]:
    return {label["name"] for label in issue.get("labels", [])}


def assert_one_work_item(work_items: Sequence[Mapping[str, Any]], agent: str) -> Mapping[str, Any]:
    """One human label produced one WorkItem on the factory agent, under its label."""

    _require(len(work_items) == 1, f"expected one WorkItem, found {len(work_items)}")
    item = work_items[0]
    _require(str(item["agent_id"]) == agent, "the WorkItem belongs to another agent")
    _require(item.get("trigger_label") == LABEL, "the WorkItem froze another label")
    _require(item.get("cancelled_at") is None, "the WorkItem was cancelled")
    return item


def assert_first_response(label_at: datetime, investigating_at: datetime | None) -> None:
    """The investigating status line was on the issue within the first-response bound."""

    _within(label_at, investigating_at, RESPONSE_SECONDS, f"the line {INVESTIGATING_LINE!r}")


def question_comments(comments: Iterable[Mapping[str, Any]], issue: int) -> list[Mapping[str, Any]]:
    return [
        comment
        for comment in comments
        if comment["issue_number"] == issue and QUESTIONS_MARKER in comment["body"]
    ]


def assert_one_question_comment(
    comments: Sequence[Mapping[str, Any]], issue: int, bot_login: str
) -> Mapping[str, Any]:
    """Session 1 posted exactly one marked comment, numbered ``1.`` and ``2.`` and no more."""

    found = question_comments(comments, issue)
    _require(len(found) == 1, f"expected one {QUESTIONS_MARKER} comment, found {len(found)}")
    comment = found[0]
    _require(comment["user"]["login"] == bot_login, "the questions were not posted by the App")
    body = comment["body"]
    items = re.findall(r"^(\d+)\. ", body, flags=re.MULTILINE)
    _require(items == ["1", "2"], f"expected question items 1 and 2, found {items}")
    return comment


def assert_questions_state(
    issue: Mapping[str, Any], requests: Sequence[Mapping[str, Any]]
) -> Mapping[str, Any]:
    """The issue shows the questions label and the asking request is the only one, finished."""

    _require(QUESTIONS_LABEL in labels_of(issue), f"the issue lacks the {QUESTIONS_LABEL} label")
    live = [str(r["id"]) for r in requests if r.get("status") in LIVE_STATUSES]
    _require(not live, f"a request is still running after the questions: {live}")
    _require(len(requests) == 1, f"expected one request after the questions, found {len(requests)}")
    asking = requests[0]
    _require(asking.get("status") == "completed", "the asking request did not complete")
    _require(asking.get("terminal_cause") == "questions_asked", "the request did not ask")
    return asking


def assert_ignored_comments(before: int, after: int) -> None:
    """A read-permission comment and a bot comment on the open question start nothing."""

    _require(
        before == after,
        f"a comment that is not an answer changed the request count: {before} -> {after}",
    )


def assert_answer_admitted(
    requests: Sequence[Mapping[str, Any]],
    asking_request: str,
    answer_at: datetime,
    received_at: datetime | None,
) -> Mapping[str, Any]:
    """The plain answer made the second request and its status line, each within the bound."""

    _require(len(requests) == 2, f"expected two requests after the answer, found {len(requests)}")
    second = requests[1]
    _require(str(second["id"]) != asking_request, "the answer reused the asking request")
    _require(second.get("completion_contract") == "implementation", "the answer is not a run")
    _within(
        answer_at,
        instant(second["created_at"]),
        RESPONSE_SECONDS,
        "the second request",
    )
    _within(answer_at, received_at, RESPONSE_SECONDS, f"the line {ANSWER_RECEIVED_LINE!r}")
    return second


def assert_answer_receipts(
    receipts: Sequence[Mapping[str, Any]], answer: str, second_request: str
) -> None:
    """The answer comment made the second request, and nothing else made one.

    The durable receipt is the link between a comment and what it started: it
    rules out a second request that no answer started, and an answer that
    started some other request.
    """

    by_comment = {str(r["comment_key"]): r for r in receipts}
    mine = by_comment.get(answer)
    _require(mine is not None, f"no receipt for the answer comment {answer}")
    assert mine is not None
    _require(mine["outcome"] == "admitted", f"the answer was {mine['outcome']}, not admitted")
    _require(str(mine["request_id"]) == second_request, "the answer started another request")
    started = [str(r["request_id"]) for r in receipts if r.get("request_id") is not None]
    _require(started == [second_request], f"requests started by comments: {started}")


def closing_pulls(pulls: Iterable[Mapping[str, Any]], issue: int) -> list[Mapping[str, Any]]:
    pattern = re.compile(rf"\bCloses #{issue}(?!\d)")
    return [pull for pull in pulls if pattern.search(pull.get("body") or "")]


def new_pulls(pulls: Iterable[Mapping[str, Any]], before: Iterable[int]) -> list[Mapping[str, Any]]:
    """The pull requests whose numbers were not there before the scenario began."""

    known = set(before)
    return [pull for pull in pulls if int(pull["number"]) not in known]


def assert_no_new_pulls(pulls: Iterable[Mapping[str, Any]], before: Iterable[int]) -> None:
    """Nothing was opened yet: a run that asked its questions published nothing."""

    opened = sorted(int(pull["number"]) for pull in new_pulls(pulls, before))
    _require(not opened, f"pull requests opened before the answer: {opened}")


def assert_one_pull_request(
    pulls: Sequence[Mapping[str, Any]], issue: int, before: Iterable[int]
) -> Mapping[str, Any]:
    """Session 2 opened exactly one new pull request, and that one closes this issue.

    The closing reference is read from the new pull request itself, so an older
    pull request edited to name the issue cannot stand in for it.
    """

    opened = new_pulls(pulls, before)
    _require(len(opened) == 1, f"expected one new pull request, found {len(opened)}")
    _require(
        bool(closing_pulls(opened, issue)),
        f"the new pull request is not one closing #{issue}",
    )
    return opened[0]


def assert_no_factory_issues(before: Iterable[int], issues: Iterable[Mapping[str, Any]]) -> None:
    """The factory created no issue of its own: the ticket is the only one."""

    known = set(before)
    created = sorted(int(issue["number"]) for issue in issues if int(issue["number"]) not in known)
    _require(not created, f"the factory created issues: {created}")


def assert_finished(
    work_items: Sequence[Mapping[str, Any]],
    requests: Sequence[Mapping[str, Any]],
    final_comments: Sequence[Mapping[str, Any]],
    pull: Mapping[str, Any],
) -> None:
    """One WorkItem and two requests at the end, the second ended in the pull request.

    The two requests are the whole history: nothing but the label and the one
    answer started a request.
    """

    _require(len(work_items) == 1, f"expected one WorkItem at the end, found {len(work_items)}")
    _require(len(requests) == 2, f"expected two requests at the end, found {len(requests)}")
    _require(requests[1].get("status") == "completed", "the answered run did not complete")
    _require(len(final_comments) == 1, f"expected one final comment, found {len(final_comments)}")
    _require(pull["html_url"] in final_comments[0]["body"], "the final comment omits the PR")
