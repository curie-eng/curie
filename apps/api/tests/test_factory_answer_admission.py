"""The one rule that decides whether a ticket comment answers the factory (#5786).

`factory_questions.admit_answer` has no production caller yet (each
tracker's intake calls it once it recognises a comment, #5787 and #5789), so
these tests call it directly with the facts an intake hands over. Every state
it acts on is produced by the production path: a labelled issue admitted
through the signed webhook, the request acquired and started as the worker
does, questions asked over HTTP through the question route with the minted
capability, settlement through the worker's finish dispatch path, promotion
through `lifecycle.admit_next_revision`, cancellation through
`lifecycle.request_cancellation`, and status comments through the reconciler.
Each answer is a comment seeded on the ticket's in-memory tracker, which
supplies its id; its creation time is set against the round's stored
``answer_after``.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_factory_tracker_results as tr
from curie_api import factory_questions
from curie_api.factory_notices import INVESTIGATING_LINE
from curie_api.factory_questions import AnswerFacts, AnswerOutcome
from curie_api.forges.capabilities import Operation
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import Actor, ReplyTarget, TrackerIssueRef
from curie_api.models import FACTORY_ANSWER_RECEIPT_OUTCOMES
from curie_api.workitem_dispatch import claim_due
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)
from test_factory_publication_recovery import api  # noqa: F401  (fixture)
from test_factory_question_ending import (
    _ask,
    _asked,
    _cancel,
    _promote,
    _refused_fourth,
    _request,
    _requests,
    _round_row,
    _running,
    _status_body,
    _take_over,
)
from test_factory_terminus import _reconcile

ANSWER_LINE = "Answer received. The factory is working on this ticket again."
AUTHOR = Actor("4242", "answer-author")
REQUESTER = f"github:{AUTHOR.id}:{AUTHOR.login}"
ANSWER_TEXT = "Replace the rows on a re-run."
SECOND = timedelta(seconds=1)


def _comment(triage: tr._Triage, text: str = ANSWER_TEXT, author: Actor = AUTHOR) -> str:
    """A comment a person left on the ticket; the tracker's id for it."""

    return triage.tracker.marked_comments.seed_comment(
        ReplyTarget.on_issue(triage.issue), author, text
    )


def _fence(triage: tr._Triage) -> datetime:
    row = _round_row(triage.work_item_id)
    assert row is not None and row["answer_after"] is not None
    answer_after: datetime = row["answer_after"]
    return answer_after


def _admit(
    triage: tr._Triage,
    comment_id: str,
    created_at: datetime | None,
    *,
    requester: str = REQUESTER,
    issue: TrackerIssueRef | None = None,
) -> AnswerOutcome:
    facts = AnswerFacts(
        work_item_id=triage.work_item_id,
        issue=issue or triage.issue,
        comment_id=comment_id,
        created_at=created_at,
        requester=requester,
    )

    async def go() -> AnswerOutcome:
        async with triage.client.app.state.sessionmaker() as session:
            return await factory_questions.admit_answer(session, facts)

    result: AnswerOutcome = triage.client.portal.call(go)
    return result


def _answer(
    triage: tr._Triage,
    delta: timedelta = SECOND,
    *,
    text: str = ANSWER_TEXT,
    requester: str = REQUESTER,
) -> tuple[str, AnswerOutcome]:
    """A new comment created ``delta`` after the current round's fence."""

    comment_id = _comment(triage, text)
    return comment_id, _admit(triage, comment_id, _fence(triage) + delta, requester=requester)


def _counts() -> tuple[int, int]:
    (row,) = _rows(
        "SELECT (SELECT count(*) FROM curie.execution_requests) AS requests, "
        "(SELECT count(*) FROM curie.factory_answer_receipts) AS receipts"
    )
    return int(row["requests"]), int(row["receipts"])


def _receipts(work_item_id: uuid.UUID) -> dict[str, tuple[str, uuid.UUID | None]]:
    rows = _rows(
        "SELECT comment_key, outcome, request_id FROM curie.factory_answer_receipts "
        "WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return {row["comment_key"]: (row["outcome"], row["request_id"]) for row in rows}


def _snapshot(request_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT status, objective, completion_contract, requester, reply_kind, reply_address, "
        "reply_conversation_id, reply_target_kind FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return dict(row)


def _replays(triage: tr._Triage, comment_id: str, stored: AnswerOutcome) -> None:
    """A redelivery returns the stored outcome and writes nothing, whatever
    its reported creation time."""

    before = _counts()
    replay = _admit(triage, comment_id, datetime.now(UTC) + timedelta(hours=1))
    assert replay == AnswerOutcome(stored.outcome, stored.request_id, True)
    assert _counts() == before


def _claimable(triage: tr._Triage) -> set[uuid.UUID]:
    async def go() -> set[uuid.UUID]:
        async with triage.client.app.state.sessionmaker() as session:
            return {claim.request_id for claim in await claim_due(session, "answer-probe", 10, 30)}

    claimed: set[uuid.UUID] = triage.client.portal.call(go)
    return claimed


def _settled_asked(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    """Asked round 1, then ended without a pull request: outstanding, nothing live."""

    triage = _asked(admitted, monkeypatch, number)
    tr._finish(triage, "failed", "no_pull_request")
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["state"], row["pending_request_id"]) == ("outstanding", None)
    return triage


# Criterion 1: an answer with nothing live admits one request -------------------------


def test_admits_an_answer_on_an_outstanding_round_with_nothing_live(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5101)
    before = _round_row(triage.work_item_id)
    assert before is not None

    comment_id, result = _answer(triage)

    issue = triage.issue
    expected = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"curie-answer:{issue.kind}:{issue.host}:{issue.scope_id}:{issue.issue_id}:{comment_id}",
    )
    assert factory_questions.answer_request_id(issue, comment_id) == expected
    assert result == AnswerOutcome("admitted", expected, False)
    assert [r["id"] for r in _requests(triage.work_item_id)] == [triage.request_id, expected]
    first, created = _snapshot(triage.request_id), _snapshot(expected)
    assert created["status"] == "waiting"
    # The WorkItem's issue objective: the bare issue link the label admitted.
    assert created["objective"] == first["objective"]
    assert created["objective"].endswith("/issues/5101")
    assert created["completion_contract"] == "implementation"
    assert created["requester"] == REQUESTER
    for column in ("reply_kind", "reply_address", "reply_conversation_id"):
        assert created[column] == first[column]
    assert created["reply_target_kind"] == "issue"
    assert _receipts(triage.work_item_id) == {comment_id: ("admitted", expected)}
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"], row["rounds_used"]) == (
        "answered",
        expected,
        1,
    )
    assert row["answer_after"] == before["answer_after"]
    # The admission snapshot was written, so the dispatcher offers it.
    assert expected in _claimable(triage)


def test_admits_refuses_a_comment_on_another_ticket_writing_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5102)
    before = _counts()
    other = TrackerIssueRef(triage.issue.kind, triage.issue.host, triage.issue.scope_id, "5999")
    with pytest.raises(ValueError):
        _admit(triage, _comment(triage), _fence(triage) + SECOND, issue=other)
    with pytest.raises(ValueError):
        _admit(triage, " ", _fence(triage) + SECOND)
    assert _counts() == before
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "outstanding"


# Criterion 2: answers before the attempt starts coalesce ----------------------------


def test_coalesce_a_second_answer_before_the_request_starts(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5111)
    _first_id, first = _answer(triage)
    assert first.outcome == "admitted" and first.request_id is not None
    requests = _requests(triage.work_item_id)

    second_id, second = _answer(triage, 2 * SECOND)

    assert second == AnswerOutcome("coalesced", None, False)
    assert _requests(triage.work_item_id) == requests
    assert _receipts(triage.work_item_id)[second_id] == ("coalesced", None)
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"]) == ("answered", first.request_id)

    _take_over(triage, first.request_id)
    assert _request(first.request_id)["status"] == "running"
    _replays(triage, second_id, second)
    assert _requests(triage.work_item_id)[-1]["id"] == first.request_id


# Criterion 3: answers during the answered run queue one follow-up --------------------


def test_follow_up_queues_once_behind_the_running_answered_request(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5121)
    _first_id, first = _answer(triage)
    assert first.request_id is not None
    _take_over(triage, first.request_id)
    assert _request(first.request_id)["status"] == "running"

    third_id, third = _answer(triage, 3 * SECOND)
    fourth_id, fourth = _answer(triage, 4 * SECOND)

    follow_up = factory_questions.answer_request_id(triage.issue, third_id)
    assert third == AnswerOutcome("queued_follow_up", follow_up, False)
    assert fourth == AnswerOutcome("coalesced", None, False)
    queued = [r["id"] for r in _requests(triage.work_item_id) if r["status"] == "queued"]
    assert queued == [follow_up]
    assert _snapshot(follow_up)["objective"] == _snapshot(triage.request_id)["objective"]
    assert _request(first.request_id)["status"] == "running"
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"]) == ("answered", follow_up)
    assert _receipts(triage.work_item_id)[fourth_id] == ("coalesced", None)


# Criterion 4: an answer while the asking request is still finishing -----------------


def test_asking_still_running_queues_one_follow_up_that_survives_settlement(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    asker = _asked(admitted, monkeypatch, 5131)
    assert _request(asker.request_id)["status"] == "running"
    row = _round_row(asker.work_item_id)
    assert row is not None and row["state"] == "outstanding"
    requests_before = len(_requests(asker.work_item_id))

    first_id, first = _answer(asker)

    follow_up = factory_questions.answer_request_id(asker.issue, first_id)
    assert first == AnswerOutcome("queued_follow_up", follow_up, False)
    assert _request(follow_up)["status"] == "queued"
    row = _round_row(asker.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"]) == ("answered", follow_up)
    assert _request(asker.request_id)["status"] == "running"

    second_id, second = _answer(asker, 2 * SECOND)
    assert second == AnswerOutcome("coalesced", None, False)
    assert [r["id"] for r in _requests(asker.work_item_id)] == [asker.request_id, follow_up]

    tr._finish(asker, "failed", "no_pull_request")

    assert _request(asker.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }
    # Zero automatic successors: the follow-up is the only other request.
    assert [r["id"] for r in _requests(asker.work_item_id)] == [asker.request_id, follow_up]
    row = _round_row(asker.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"]) == ("answered", follow_up)
    assert _request(follow_up)["status"] == "queued"
    _promote(asker)
    assert _request(follow_up)["status"] == "waiting"
    assert len(_requests(asker.work_item_id)) == requests_before + 1

    _replays(asker, first_id, first)
    _replays(asker, second_id, second)


# Criterion 5: the fence ---------------------------------------------------------------


def test_fence_refuses_a_comment_not_created_after_the_question(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5141)
    fence = _fence(triage)
    # Created when the question was, before it, with no time, and with a time
    # that names no zone.
    for created_at in (fence, fence - SECOND, None, fence.replace(tzinfo=None) + SECOND):
        before = _counts()
        comment_id = _comment(triage)
        refused = _admit(triage, comment_id, created_at)
        assert refused == AnswerOutcome("not_after_question", None, False)
        assert _counts() == (before[0], before[1] + 1)
        assert _receipts(triage.work_item_id)[comment_id] == ("not_after_question", None)
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["state"], row["pending_request_id"]) == ("outstanding", None)

    # Written before the question, delivered after a later answer.
    old_id = _comment(triage)
    _later_id, later = _answer(triage, timedelta(microseconds=1))
    assert later.outcome == "admitted"
    before = _counts()
    late = _admit(triage, old_id, fence - 5 * SECOND)
    assert late == AnswerOutcome("not_after_question", None, False)
    assert _counts() == (before[0], before[1] + 1)
    row = _round_row(triage.work_item_id)
    assert row is not None and row["pending_request_id"] == later.request_id


def test_fence_moves_to_the_second_round(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5142)
    round_one = _fence(triage)
    _first_id, first = _answer(triage)
    assert first.outcome == "admitted" and first.request_id is not None

    # The answer-started request asks round 2, then ends without a pull request.
    successor = _take_over(triage, first.request_id)
    asked = _ask(successor, ["Round 2: which file holds the rows?"])
    assert asked.status_code == 200, asked.text
    assert asked.json() == {"round": 2, "rounds_limit": 3}
    tr._finish(successor, "failed", "no_pull_request")
    assert _request(first.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["state"], row["rounds_used"]) == ("outstanding", 2)
    round_two = row["answer_after"]
    assert round_two > round_one

    before = _counts()
    between = _admit(triage, _comment(triage), round_one + (round_two - round_one) / 2)
    assert between == AnswerOutcome("not_after_question", None, False)
    assert _counts() == (before[0], before[1] + 1)

    _second_id, second = _answer(triage)
    assert second.outcome == "admitted"


# Criterion 6: no open round, a cancelled WorkItem, and replays -------------------------


def _absent(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    return _running(admitted, monkeypatch, number)


def _posting(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    """The question write failed at the tracker, which leaves the round ``posting``."""

    triage = _running(admitted, monkeypatch, number)
    triage.tracker.write_faults.abort_before_next(
        Operation.UPSERT_MARKED, Unavailable("tracker down")
    )
    assert _ask(triage).status_code == 503
    return triage


def _cleared(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    triage = _asked(admitted, monkeypatch, number)
    tr._finish(triage, "failed", "budget_exceeded")
    return triage


_NO_OPEN_ROUND: dict[str, Callable[[Any, pytest.MonkeyPatch, int], tr._Triage]] = {
    "absent": _absent,
    "posting": _posting,
    "cleared": _cleared,
    "exhausted": _refused_fourth,
}


@pytest.mark.parametrize(
    ("state", "number"),
    [("absent", 5161), ("posting", 5162), ("cleared", 5163), ("exhausted", 5164)],
)
def test_not_outstanding_when_no_round_is_open(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, state: str, number: int
) -> None:
    triage = _NO_OPEN_ROUND[state](admitted, monkeypatch, number)
    row = _round_row(triage.work_item_id)
    if state == "absent":
        assert row is None
    else:
        assert row is not None and row["state"] == state
    # A creation time the fence would pass, so only the round refuses it.
    fence = row["answer_after"] if row is not None else None
    created_at = (fence or datetime.now(UTC)) + SECOND
    requests = _requests(triage.work_item_id)
    before = _counts()

    comment_id = _comment(triage)
    result = _admit(triage, comment_id, created_at)

    assert result == AnswerOutcome("not_outstanding", None, False)
    assert _requests(triage.work_item_id) == requests
    assert _counts() == (before[0], before[1] + 1)
    assert _receipts(triage.work_item_id) == {comment_id: ("not_outstanding", None)}
    assert _round_row(triage.work_item_id) == row
    _replays(triage, comment_id, result)


def test_cancelled_work_item_takes_no_answer(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _settled_asked(admitted, monkeypatch, 5171)
    fence = _fence(triage)
    _cancel(triage)
    requests = _requests(triage.work_item_id)
    before = _counts()

    comment_id = _comment(triage)
    result = _admit(triage, comment_id, fence + SECOND)

    assert result == AnswerOutcome("work_item_cancelled", None, False)
    assert _requests(triage.work_item_id) == requests
    assert _counts() == (before[0], before[1] + 1)
    assert _receipts(triage.work_item_id) == {comment_id: ("work_item_cancelled", None)}
    _replays(triage, comment_id, result)


def _every_outcome(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    base: int,
    *,
    text: str = ANSWER_TEXT,
    requester: str = REQUESTER,
) -> list[tuple[tr._Triage, str, AnswerOutcome]]:
    """One comment reaching each outcome, in the order they were admitted."""

    reached: list[tuple[tr._Triage, str, AnswerOutcome]] = []

    def admit(triage: tr._Triage, created_at: datetime) -> None:
        comment_id = _comment(triage, text)
        reached.append(
            (triage, comment_id, _admit(triage, comment_id, created_at, requester=requester))
        )

    asking = _asked(admitted, monkeypatch, base)
    admit(asking, _fence(asking) - SECOND)
    admit(asking, _fence(asking) + SECOND)
    admit(asking, _fence(asking) + 2 * SECOND)
    settled = _settled_asked(admitted, monkeypatch, base + 1)
    admit(settled, _fence(settled) + SECOND)
    idle = _running(admitted, monkeypatch, base + 2)
    admit(idle, datetime.now(UTC))
    _cancel(idle)
    admit(idle, datetime.now(UTC))
    assert [outcome.outcome for _t, _c, outcome in reached] == [
        "not_after_question",
        "queued_follow_up",
        "coalesced",
        "admitted",
        "not_outstanding",
        "work_item_cancelled",
    ]
    return reached


def test_replay_of_every_outcome_returns_it_and_writes_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert factory_questions.ANSWER_OUTCOMES == FACTORY_ANSWER_RECEIPT_OUTCOMES
    reached = _every_outcome(admitted, monkeypatch, 5181)
    assert {outcome.outcome for _t, _c, outcome in reached} == set(FACTORY_ANSWER_RECEIPT_OUTCOMES)
    # The state the outcomes were decided on moves on; the receipts do not.
    asking = reached[0][0]
    tr._finish(asking, "failed", "no_pull_request")
    _promote(asking)
    for triage, comment_id, outcome in reached:
        _replays(triage, comment_id, outcome)


# Criterion 7: the answer-received line --------------------------------------------------


def test_status_line_leads_an_admitted_answer_while_waiting_and_running(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 5191)
    _reconcile()
    first_body = _status_body(sink, triage.request_id)
    assert first_body.startswith(INVESTIGATING_LINE)
    assert ANSWER_LINE not in first_body
    tr._finish(triage, "failed", "no_pull_request")

    _comment_id, result = _answer(triage)
    assert result.outcome == "admitted" and result.request_id is not None
    _reconcile()
    waiting = _status_body(sink, result.request_id)
    assert waiting.startswith(ANSWER_LINE)
    assert INVESTIGATING_LINE not in waiting

    _take_over(triage, result.request_id)
    assert _request(result.request_id)["status"] == "running"
    _reconcile()
    running = _status_body(sink, result.request_id)
    assert running.startswith(ANSWER_LINE)
    assert INVESTIGATING_LINE not in running


def test_status_line_leads_a_queued_follow_up_once_promoted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    asker = _asked(admitted, monkeypatch, 5192)
    _comment_id, result = _answer(asker)
    assert result.outcome == "queued_follow_up" and result.request_id is not None
    tr._finish(asker, "failed", "no_pull_request")
    _promote(asker)
    assert _request(result.request_id)["status"] == "waiting"
    _reconcile()
    body = _status_body(sink, result.request_id)
    assert body.startswith(ANSWER_LINE)
    assert INVESTIGATING_LINE not in body


# Criterion 8: logs carry identifiers only -------------------------------------------------


def test_log_records_of_every_outcome_carry_identifiers_only(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="curie_api.factory_questions")
    # The comment's text lives only on the tracker; the author's login is in
    # the requester the request stores. Neither belongs in a log line.
    text_sentinel = f"answertext{uuid.uuid4().hex[:8]}"
    login_sentinel = f"answerlogin{uuid.uuid4().hex[:8]}"
    reached = _every_outcome(
        admitted,
        monkeypatch,
        5195,
        text=f"Please replace the rows. {text_sentinel}",
        requester=f"github:{AUTHOR.id}:{login_sentinel}",
    )
    triage, replayed_id, replayed = reached[1]
    _replays(triage, replayed_id, replayed)

    records = [
        record
        for record in caplog.records
        if record.name == "curie_api.factory_questions"
        and record.getMessage().startswith("factory answer")
    ]
    expected = [
        (outcome.outcome, t.work_item_id, comment_id, outcome.request_id)
        for t, comment_id, outcome in reached
    ] + [(replayed.outcome, triage.work_item_id, replayed_id, replayed.request_id)]
    assert [record.args for record in records] == expected
    assert "replayed" in records[-1].getMessage()
    for record, (outcome, work_item_id, comment_id, _request_id) in zip(
        records, expected, strict=True
    ):
        message = record.getMessage()
        assert outcome in message
        assert str(work_item_id) in message
        assert f"comment={comment_id}" in message
        assert text_sentinel not in message
        assert login_sentinel not in message
    assert text_sentinel not in caplog.text
