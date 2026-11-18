"""How a request that asked questions ends, and what the ticket shows (#5785).

A request whose round is posted and that then ends without a pull request
completes with ``questions_asked``, admits no retry, and its status comment
names the round and carries ``curie-factory:questions``. An answer queued
before the asking request settles keeps its revision request. A fourth ask is
refused, and the request that was refused fails ``question_rounds_exhausted``,
which needs a human. Cancelling the WorkItem drops its round record.

Each case admits a labelled issue through a signed webhook, acquires and
starts its request as the worker does, asks through the route over HTTP with
the minted issue read capability, and finishes through the dispatch path the
worker's finish route calls. The answer core (#5786) is not built yet, so an
answer is the row change it makes: ``answered`` with the revision request it
queued as ``pending_request_id``.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_factory_tracker_results as tr
from curie_api import factory_questions
from curie_api.factory_comment_text import marker_for
from curie_api.workitem_dispatch import DispatchConflict, acquire
from curie_api.workitems import lifecycle
from forge_fakes import factory_wake
from forge_fakes.github import LABEL
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)
from sqlalchemy import text
from test_factory_publication_recovery import api  # noqa: F401  (fixture)
from test_factory_status_comment import _live_revision
from test_factory_terminus import _reconcile

QUESTIONS_URL = "/work-items/questions"
SUMMARY = "The importer reproduces the duplicate on a second run."
QUESTIONS = ["Should a re-run replace the rows or append them?"]
QUESTIONS_LABEL = "curie-factory:questions"
STATE_LABELS = {
    "curie-factory:queued",
    "curie-factory:running",
    "curie-factory:pr-open",
    "curie-factory:needs-human",
    "curie-factory:triaged",
    QUESTIONS_LABEL,
}
INVESTIGATING = (
    "Investigating this ticket. The factory will open a pull request or post its questions here."
)


def _ask(triage: tr._Triage, questions: list[str] | None = None) -> Any:
    return triage.client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": triage.capability},
        json={"summary": SUMMARY, "questions": questions or QUESTIONS},
    )


def _running(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    _client, _github, sink = admitted
    sink.issue_labels[number] = {LABEL}
    triage = tr._triage(admitted, monkeypatch, number, tracker_result=False)
    tr._begin(triage)
    return triage


def _asked(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    triage = _running(admitted, monkeypatch, number)
    response = _ask(triage)
    assert response.status_code == 200, response.text
    return triage


def _round_row(work_item_id: uuid.UUID) -> dict[str, Any] | None:
    rows = _rows(
        "SELECT * FROM curie.factory_question_rounds WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return rows[0] if rows else None


def _request(request_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return dict(row)


def _requests(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, status, terminal_cause FROM curie.execution_requests "
        "WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )


def _version(work_item_id: uuid.UUID) -> int:
    (row,) = _rows("SELECT version FROM curie.work_items WHERE id = :id", {"id": work_item_id})
    return int(row["version"])


def _queue_revision(triage: tr._Triage) -> uuid.UUID:
    """A follow-up revision request on the same WorkItem, as an answer queues it."""

    (source,) = _rows(
        "SELECT requester, reply_kind, reply_address, reply_conversation_id "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": triage.request_id},
    )
    revision_id = uuid.uuid4()
    version = _version(triage.work_item_id)

    async def go() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            return await lifecycle.create_revision_request(
                session,
                work_item_id=triage.work_item_id,
                request_id=revision_id,
                wait_deadline=datetime.now(UTC) + timedelta(hours=1),
                expected_work_item_version=version,
                snapshot={
                    "objective": "Answer: replace the rows on a re-run.",
                    "requester": source["requester"],
                    "reply_kind": source["reply_kind"],
                    "reply_address": source["reply_address"],
                    "reply_conversation_id": source["reply_conversation_id"],
                },
            )

    created = triage.client.portal.call(go)
    assert isinstance(created, lifecycle.WorkItemOutcome), created
    return revision_id


def _answer(work_item_id: uuid.UUID, pending: uuid.UUID) -> None:
    """The row change the answer core makes when an answer queues ``pending``."""

    tr._exec(
        "UPDATE curie.factory_question_rounds SET state = 'answered', "
        "pending_request_id = :pending, version = version + 1 WHERE work_item_id = :id",
        {"pending": pending, "id": work_item_id},
    )


def _promote(triage: tr._Triage) -> None:
    async def go() -> None:
        async with triage.client.app.state.sessionmaker() as session:
            await lifecycle.admit_next_revision(
                session,
                work_item_id=triage.work_item_id,
                wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            )

    triage.client.portal.call(go)


def _take_over(triage: tr._Triage, request_id: uuid.UUID) -> tr._Triage:
    """Promote, acquire and start ``request_id`` and mint its capability."""

    _promote(triage)

    async def go() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=triage.client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            return acquired.context

    context = triage.client.portal.call(go)
    minted = triage.client.post(
        tr.MINT_URL,
        headers=tr.WORKER_HEADERS,
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    successor = tr._Triage(
        client=triage.client,
        request_id=request_id,
        work_item_id=triage.work_item_id,
        tracker=triage.tracker,
        issue=triage.issue,
        capability=minted.json()["capability"],
        context=context,
    )
    tr._begin(successor)
    return successor


def _status_body(sink: Any, request_id: uuid.UUID) -> str:
    everything = list(sink.comments) + [c for listed in sink.lists.values() for c in listed]
    (comment,) = [c for c in everything if marker_for(request_id) in (c.get("body") or "")]
    return str(comment["body"])


def _state_labels(sink: Any, number: int) -> set[str]:
    return sink.issue_labels.get(number, set()) & STATE_LABELS


# Criterion 5: the request completes with questions_asked ---------------------------


def test_an_asking_request_that_ends_without_a_pull_request_settles_questions_asked(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _asked(admitted, monkeypatch, 901)
    tr._finish(triage, "failed", "no_pull_request")

    assert _request(triage.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["pending_request_id"]) == (
        "outstanding",
        1,
        None,
    )
    # A completed request admits no retry successor.
    assert [r["id"] for r in _requests(triage.work_item_id)] == [triage.request_id]
    _client, _github, sink = admitted
    _reconcile()
    assert _state_labels(sink, 901) == {QUESTIONS_LABEL}


@pytest.mark.parametrize("cause", ["early_stop", "revision_unchanged"])
def test_every_unpublished_ending_of_an_asking_request_settles_questions_asked(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, cause: str
) -> None:
    triage = _asked(admitted, monkeypatch, 902)
    tr._finish(triage, "failed", cause)
    assert _request(triage.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }


def test_a_request_that_never_asked_settles_its_own_cause(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 903)
    tr._finish(triage, "failed", "no_pull_request")
    assert _request(triage.request_id)["terminal_cause"] == "no_pull_request"
    assert _round_row(triage.work_item_id) is None


def test_a_published_failure_after_asking_keeps_its_cause_and_clears_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _asked(admitted, monkeypatch, 904)
    tr._finish(triage, "failed", "budget_exceeded")
    assert _request(triage.request_id) == {
        "status": "failed",
        "terminal_cause": "budget_exceeded",
    }
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["state"], row["rounds_used"]) == ("cleared", 1)


# Criterion 6: an answer before the asking request settles ------------------------------


@pytest.mark.parametrize(
    ("cause", "settled"),
    [
        ("no_pull_request", {"status": "completed", "terminal_cause": "questions_asked"}),
        ("budget_exceeded", {"status": "failed", "terminal_cause": "budget_exceeded"}),
    ],
)
def test_answered_before_settle_keeps_the_queued_revision(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, cause: str, settled: dict[str, str]
) -> None:
    triage = _asked(admitted, monkeypatch, 911)
    before = _round_row(triage.work_item_id)
    assert before is not None
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)

    tr._finish(triage, "failed", cause)

    assert _request(triage.request_id) == settled
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"], row["rounds_used"]) == (
        "answered",
        revision,
        1,
    )
    assert row["answer_after"] == before["answer_after"]
    # No automatic successor; the follow-up waits queued for its promotion.
    assert [r["id"] for r in _requests(triage.work_item_id)] == [triage.request_id, revision]
    assert _request(revision)["status"] == "queued"
    _promote(triage)
    assert _request(revision)["status"] == "waiting"


# Criterion 7: the status comment and its label ---------------------------------------


def test_status_comment_names_the_round_and_carries_the_questions_label(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 921)
    tr._finish(triage, "failed", "no_pull_request")
    _reconcile()

    body = _status_body(sink, triage.request_id)
    assert "Questions asked (round 1 of 3). Answer in a plain comment on this ticket." in body
    assert "Status: NEEDS HUMAN" not in body
    assert "Could not complete" not in body
    assert _state_labels(sink, 921) == {QUESTIONS_LABEL}


# Criterion 8: the investigating line ---------------------------------------------------


def test_investigating_line_leads_a_running_implementation_status_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    triage = _running(admitted, monkeypatch, 931)
    _reconcile()
    body = _status_body(sink, triage.request_id)
    assert body.startswith(INVESTIGATING)

    tr._finish(triage, "failed", "runner_escalated")
    _reconcile()
    assert INVESTIGATING not in _status_body(sink, triage.request_id)


def test_investigating_line_is_absent_for_a_tracker_result_request(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    sink.issue_labels[932] = {LABEL}
    triage = tr._triage(admitted, monkeypatch, 932, tracker_result=True)
    tr._begin(triage)
    _reconcile()
    assert INVESTIGATING not in _status_body(sink, triage.request_id)


@pytest.mark.parametrize("fragment", ["discussion_r88301", "issuecomment-88302"])
def test_investigating_line_is_absent_for_pull_request_feedback(
    admitted: Any, fragment: str
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _number, _pr, revision, _work_item_id = _live_revision(client, github, sink, fragment)
    assert INVESTIGATING not in _status_body(sink, revision)


# Criterion 11: three rounds, then the cap ----------------------------------------------


def test_cap_refuses_a_fourth_round_and_the_request_needs_a_human(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    current = _asked(admitted, monkeypatch, 941)
    for expected_round in (2, 3, 4):
        # Each answer arrives while the asker still runs, so its revision queues.
        revision = _queue_revision(current)
        _answer(current.work_item_id, revision)
        tr._finish(current, "failed", "no_pull_request")
        assert _request(current.request_id)["terminal_cause"] == "questions_asked"
        current = _take_over(current, revision)
        response = _ask(current, [f"Round {expected_round}: which file holds the rows?"])
        if expected_round <= 3:
            assert response.status_code == 200, response.text
            assert response.json() == {"round": expected_round, "rounds_limit": 3}
        else:
            assert response.status_code == 409
            assert response.json()["detail"]["code"] == "question_rounds_exhausted"

    row = _round_row(current.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["asking_request_id"]) == (
        "exhausted",
        3,
        current.request_id,
    )
    tr._finish(current, "failed", "no_pull_request")
    assert _request(current.request_id) == {
        "status": "failed",
        "terminal_cause": "question_rounds_exhausted",
    }
    # Unknown to the retry policy: terminal, no successor.
    assert _requests(current.work_item_id)[-1]["id"] == current.request_id
    _reconcile()
    assert _state_labels(sink, 941) == {"curie-factory:needs-human"}
    assert len(current.tracker.marked_comments.comments(_target(current))) >= 3


def _target(triage: tr._Triage) -> Any:
    from curie_api.forges.types import ReplyTarget

    return ReplyTarget.on_issue(triage.issue)


# Criterion 12: cancellation -----------------------------------------------------------


def _cancel(triage: tr._Triage) -> None:
    version = _version(triage.work_item_id)

    async def go() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            result = await lifecycle.request_cancellation(
                session, work_item_id=triage.work_item_id, expected_work_item_version=version
            )
            await session.commit()
            return result

    assert isinstance(triage.client.portal.call(go), lifecycle.WorkItemOutcome)


def test_cancel_drops_the_round_record(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    triage = _asked(admitted, monkeypatch, 951)
    assert _round_row(triage.work_item_id) is not None
    _cancel(triage)
    assert _round_row(triage.work_item_id) is None
    refused = _ask(triage)
    assert (refused.status_code, refused.json()["detail"]["code"]) == (
        409,
        "work_item_cancelled",
    )


def test_cancel_while_the_comment_is_written_refuses_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 952)
    original = factory_questions.post_round

    async def cancelled_meanwhile(*args: Any, **kwargs: Any) -> str:
        comment_id = await original(*args, **kwargs)
        async with triage.client.app.state.sessionmaker() as session:
            version = await session.scalar(
                text("SELECT version FROM curie.work_items WHERE id = :id"),
                {"id": triage.work_item_id},
            )
            result = await lifecycle.request_cancellation(
                session, work_item_id=triage.work_item_id, expected_work_item_version=version
            )
            await session.commit()
            assert isinstance(result, lifecycle.WorkItemOutcome), result
        return comment_id

    monkeypatch.setattr(factory_questions, "post_round", cancelled_meanwhile)
    refused = _ask(triage)
    assert (refused.status_code, refused.json()["detail"]["code"]) == (
        409,
        "work_item_cancelled",
    )
    assert _round_row(triage.work_item_id) is None
