"""How a request that asked questions ends, and what the ticket shows (#5785).

A request whose round is posted and that then ends without a pull request
completes with ``questions_asked``, admits no retry, and its status comment
names the round and carries ``curie-factory:questions``. An answer queued
before the asking request settles keeps its revision request. A fourth ask is
refused, and the request that was refused fails ``question_rounds_exhausted``
however it fails, owner loss included, which needs a human and admits no
successor. Cancelling the WorkItem deletes its question row and keeps the
lifetime round count, so a relabel does not reset the cap (ADR 0286
decision 8).

Each case admits a labelled issue through a signed webhook, acquires and
starts its request as the worker does, asks through the route over HTTP with
the minted issue read capability, and finishes through the dispatch path the
worker's finish route calls. The answer core (#5786) is not built yet, so an
answer is the row change it makes: ``answered`` with the revision request it
queued as ``pending_request_id``.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import factory_run_support as tr
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
    QUESTIONS_LABEL,
}
INVESTIGATING = (
    "Investigating this ticket. The factory will open a pull request or post its questions here."
)


def _ask(job: tr._Run, questions: list[str] | None = None) -> Any:
    return job.client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": job.capability},
        json={"summary": SUMMARY, "questions": questions or QUESTIONS},
    )


def _running(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Run:
    _client, _github, sink = admitted
    sink.issue_labels[number] = {LABEL}
    job = tr._admit_run(admitted, monkeypatch, number)
    tr._begin(job)
    return job


def _asked(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Run:
    job = _running(admitted, monkeypatch, number)
    response = _ask(job)
    assert response.status_code == 200, response.text
    return job


def _round_row(work_item_id: uuid.UUID) -> dict[str, Any] | None:
    rows = _rows(
        "SELECT * FROM curie.factory_question_rounds WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return rows[0] if rows else None


def _round_count(work_item_id: uuid.UUID) -> tuple[int, uuid.UUID | None] | None:
    rows = _rows(
        "SELECT rounds_used, last_asking_request_id FROM curie.factory_question_round_counts "
        "WHERE work_item_id = :id",
        {"id": work_item_id},
    )
    return (int(rows[0]["rounds_used"]), rows[0]["last_asking_request_id"]) if rows else None


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


def _queue_revision(job: tr._Run) -> uuid.UUID:
    """A follow-up revision request on the same WorkItem, as an answer queues it."""

    (source,) = _rows(
        "SELECT requester, reply_kind, reply_address, reply_conversation_id "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": job.request_id},
    )
    revision_id = uuid.uuid4()
    version = _version(job.work_item_id)

    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            return await lifecycle.create_revision_request(
                session,
                work_item_id=job.work_item_id,
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

    created = job.client.portal.call(go)
    assert isinstance(created, lifecycle.WorkItemOutcome), created
    return revision_id


def _answer(work_item_id: uuid.UUID, pending: uuid.UUID) -> None:
    """The row change the answer core makes when an answer queues ``pending``."""

    tr._exec(
        "UPDATE curie.factory_question_rounds SET state = 'answered', "
        "pending_request_id = :pending, version = version + 1 WHERE work_item_id = :id",
        {"pending": pending, "id": work_item_id},
    )


def _promote(job: tr._Run) -> None:
    async def go() -> None:
        async with job.client.app.state.sessionmaker() as session:
            await lifecycle.admit_next_revision(
                session,
                work_item_id=job.work_item_id,
                wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            )

    job.client.portal.call(go)


def _take_over(job: tr._Run, request_id: uuid.UUID) -> tr._Run:
    """Promote, acquire and start ``request_id`` and mint its capability."""

    _promote(job)

    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=job.client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            return acquired.context

    context = job.client.portal.call(go)
    minted = job.client.post(
        tr.MINT_URL,
        headers=tr.WORKER_HEADERS,
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    successor = tr._Run(
        client=job.client,
        request_id=request_id,
        work_item_id=job.work_item_id,
        tracker=job.tracker,
        issue=job.issue,
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
    job = _asked(admitted, monkeypatch, 901)
    tr._finish(job, "failed", "no_pull_request")

    assert _request(job.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["pending_request_id"]) == (
        "outstanding",
        1,
        None,
    )
    # A completed request admits no retry successor.
    assert [r["id"] for r in _requests(job.work_item_id)] == [job.request_id]
    _client, _github, sink = admitted
    _reconcile()
    assert _state_labels(sink, 901) == {QUESTIONS_LABEL}


@pytest.mark.parametrize("cause", ["early_stop", "revision_unchanged"])
def test_every_unpublished_ending_of_an_asking_request_settles_questions_asked(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, cause: str
) -> None:
    job = _asked(admitted, monkeypatch, 902)
    tr._finish(job, "failed", cause)
    assert _request(job.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }


def test_a_request_that_never_asked_settles_its_own_cause(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 903)
    tr._finish(job, "failed", "no_pull_request")
    assert _request(job.request_id)["terminal_cause"] == "no_pull_request"
    assert _round_row(job.work_item_id) is None


def test_a_published_failure_after_asking_keeps_its_cause_and_clears_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _asked(admitted, monkeypatch, 904)
    tr._finish(job, "failed", "budget_exceeded")
    assert _request(job.request_id) == {
        "status": "failed",
        "terminal_cause": "budget_exceeded",
    }
    row = _round_row(job.work_item_id)
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
    job = _asked(admitted, monkeypatch, 911)
    before = _round_row(job.work_item_id)
    assert before is not None
    revision = _queue_revision(job)
    _answer(job.work_item_id, revision)

    tr._finish(job, "failed", cause)

    assert _request(job.request_id) == settled
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"], row["rounds_used"]) == (
        "answered",
        revision,
        1,
    )
    assert row["answer_after"] == before["answer_after"]
    # No automatic successor; the follow-up waits queued for its promotion.
    assert [r["id"] for r in _requests(job.work_item_id)] == [job.request_id, revision]
    assert _request(revision)["status"] == "queued"
    _promote(job)
    assert _request(revision)["status"] == "waiting"


# Criterion 7: the status comment and its label ---------------------------------------


def test_status_comment_names_the_round_and_carries_the_questions_label(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    job = _asked(admitted, monkeypatch, 921)
    tr._finish(job, "failed", "no_pull_request")
    _reconcile()

    body = _status_body(sink, job.request_id)
    assert "Questions asked (round 1 of 3). Answer in a plain comment on this ticket." in body
    assert "Status: NEEDS HUMAN" not in body
    assert "Could not complete" not in body
    assert _state_labels(sink, 921) == {QUESTIONS_LABEL}


# Criterion 8: the investigating line ---------------------------------------------------


def test_investigating_line_leads_a_running_implementation_status_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    job = _running(admitted, monkeypatch, 931)
    _reconcile()
    body = _status_body(sink, job.request_id)
    assert body.startswith(INVESTIGATING)

    tr._finish(job, "failed", "runner_escalated")
    _reconcile()
    assert INVESTIGATING not in _status_body(sink, job.request_id)


@pytest.mark.parametrize("fragment", ["discussion_r88301", "issuecomment-88302"])
def test_investigating_line_is_absent_for_pull_request_feedback(
    admitted: Any, fragment: str
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _number, _pr, revision, _work_item_id = _live_revision(client, github, sink, fragment)
    assert INVESTIGATING not in _status_body(sink, revision)


# Criterion 11: three rounds, then the cap ----------------------------------------------


def _asked_rounds(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int, rounds: int
) -> tr._Run:
    """``rounds`` real rounds, each answered while its asker ran; the last
    asker is still running."""

    current = _asked(admitted, monkeypatch, number)
    for expected_round in range(2, rounds + 1):
        revision = _queue_revision(current)
        _answer(current.work_item_id, revision)
        tr._finish(current, "failed", "no_pull_request")
        current = _take_over(current, revision)
        response = _ask(current, [f"Round {expected_round}: which file holds the rows?"])
        assert response.status_code == 200, response.text
        assert response.json() == {"round": expected_round, "rounds_limit": 3}
    return current


def _refused_fourth(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Run:
    """A running request whose fourth ask got a real 409 for the cap."""

    current = _asked_rounds(admitted, monkeypatch, number, 3)
    revision = _queue_revision(current)
    _answer(current.work_item_id, revision)
    tr._finish(current, "failed", "no_pull_request")
    current = _take_over(current, revision)
    refused = _ask(current, ["Round 4: which file holds the rows?"])
    assert (refused.status_code, refused.json()["detail"]["code"]) == (
        409,
        "question_rounds_exhausted",
    )
    row = _round_row(current.work_item_id)
    assert row is not None and (row["state"], row["asking_request_id"]) == (
        "exhausted",
        current.request_id,
    )
    return current


def _versions(job: tr._Run, request_id: uuid.UUID) -> tuple[int, int]:
    (row,) = _rows(
        "SELECT w.version AS work_item_version, r.version AS request_version "
        "FROM curie.work_items w JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE r.id = :id",
        {"id": request_id},
    )
    return int(row["work_item_version"]), int(row["request_version"])


def _lose_owner(job: tr._Run) -> None:
    """The heartbeat lapses past the runtime TTL, the reconciler's owner loss
    cancellation runs, and the worker records the sandbox's termination."""

    from curie_api.config import get_settings

    ttl = get_settings().work_item_runtime_ttl_seconds
    tr._exec(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - make_interval(secs => :lapse) WHERE id = :id",
        {"lapse": ttl + 5, "id": job.request_id},
    )

    work_item_version, request_version = _versions(job, job.request_id)

    async def cancel() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            result = await lifecycle.request_owner_lost_cancellation(
                session,
                work_item_id=job.work_item_id,
                request_id=job.request_id,
                expected_work_item_version=work_item_version,
                expected_request_version=request_version,
            )
            await session.commit()
            return result

    assert isinstance(job.client.portal.call(cancel), lifecycle.WorkItemOutcome)
    assert _request(job.request_id) == {
        "status": "cancellation_requested",
        "terminal_cause": "owner_lost",
    }

    work_item_version, request_version = _versions(job, job.request_id)

    async def terminate() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            result = await lifecycle.record_runtime_termination(
                session,
                work_item_id=job.work_item_id,
                request_id=job.request_id,
                termination_observation="sandbox gone after the owner stopped heartbeating",
                expected_work_item_version=work_item_version,
                expected_request_version=request_version,
            )
            await session.commit()
            return result

    assert isinstance(job.client.portal.call(terminate), lifecycle.WorkItemOutcome)


def _finish_clean(job: tr._Run, cause: str) -> Any:
    """The worker's failed finish with proved ``no_effects``, which lets a
    retryable cause admit an automatic successor."""

    from curie_api.workitem_dispatch import finish

    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            return await finish(
                session,
                job.request_id,
                runtime_epoch=job.epoch,
                outcome="failed",
                cause=cause,
                detail=None,
                ci_fix_round=None,
                retry_safety="no_effects",
                retry_condition=None,
                exhausted_volume=None,
            )

    return job.client.portal.call(go)


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


def _target(job: tr._Run) -> Any:
    from curie_api.forges.types import ReplyTarget

    return ReplyTarget.on_issue(job.issue)


def test_cap_owner_loss_after_the_refusal_settles_exhausted_without_a_successor(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    current = _refused_fourth(admitted, monkeypatch, 943)
    _lose_owner(current)
    assert _request(current.request_id) == {
        "status": "failed",
        "terminal_cause": "question_rounds_exhausted",
    }
    assert _requests(current.work_item_id)[-1]["id"] == current.request_id
    _reconcile()
    assert _state_labels(sink, 943) == {"curie-factory:needs-human"}


def test_cap_a_retryable_failure_after_the_refusal_settles_exhausted_without_a_successor(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    current = _refused_fourth(admitted, monkeypatch, 944)
    # Outside the unpublished causes, and with proved clean effects the
    # retry policy would admit a successor for it.
    _finish_clean(current, "sandbox_node_disrupted")
    assert _request(current.request_id) == {
        "status": "failed",
        "terminal_cause": "question_rounds_exhausted",
    }
    assert _requests(current.work_item_id)[-1]["id"] == current.request_id
    _reconcile()
    assert _state_labels(sink, 944) == {"curie-factory:needs-human"}


def test_cap_owner_loss_below_the_cap_still_admits_its_successor(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _asked(admitted, monkeypatch, 945)
    _lose_owner(job)
    assert _request(job.request_id) == {"status": "failed", "terminal_cause": "owner_lost"}
    successor = _requests(job.work_item_id)[-1]
    assert successor["id"] != job.request_id
    assert successor["status"] == "waiting"


def test_cap_a_retryable_failure_below_the_cap_still_admits_its_successor(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _asked(admitted, monkeypatch, 946)
    _finish_clean(job, "sandbox_node_disrupted")
    assert _request(job.request_id) == {
        "status": "failed",
        "terminal_cause": "sandbox_node_disrupted",
    }
    successor = _requests(job.work_item_id)[-1]
    assert successor["id"] != job.request_id
    assert successor["status"] == "waiting"


# Criterion 12: cancellation -----------------------------------------------------------


def _cancel(job: tr._Run) -> None:
    version = _version(job.work_item_id)

    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            result = await lifecycle.request_cancellation(
                session, work_item_id=job.work_item_id, expected_work_item_version=version
            )
            await session.commit()
            return result

    assert isinstance(job.client.portal.call(go), lifecycle.WorkItemOutcome)


def test_cancel_deletes_the_question_cancels_the_queued_answer_and_keeps_the_cap(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, github, sink = admitted
    current = _asked_rounds(admitted, monkeypatch, 951, 3)
    revision = _queue_revision(current)
    _answer(current.work_item_id, revision)
    tr._finish(current, "failed", "no_pull_request")
    row = _round_row(current.work_item_id)
    assert row is not None and (row["state"], row["pending_request_id"]) == ("answered", revision)
    assert _request(revision)["status"] == "queued"

    _cancel(current)

    # The row is gone with the cancellation; the lifetime count stays.
    assert _round_row(current.work_item_id) is None
    assert _round_count(current.work_item_id) == (3, current.request_id)
    # The cancellation's own sweep cancels the queued answer follow-up.
    assert _request(revision) == {"status": "cancelled", "terminal_cause": "issue_cancelled"}
    refused = _ask(current)
    assert (refused.status_code, refused.json()["detail"]["code"]) == (
        409,
        "work_item_cancelled",
    )

    # A relabel reuses the WorkItem: its first ask is still past the cap.
    tr._label(current.client, github, 951)
    relabelled = _requests(current.work_item_id)[-1]["id"]
    assert relabelled not in (current.request_id, revision)
    fresh = _take_over(current, relabelled)
    again = _ask(fresh)
    assert (again.status_code, again.json()["detail"]["code"]) == (
        409,
        "question_rounds_exhausted",
    )
    row = _round_row(fresh.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["asking_request_id"]) == (
        "exhausted",
        3,
        fresh.request_id,
    )
    tr._finish(fresh, "failed", "no_pull_request")
    assert _request(fresh.request_id) == {
        "status": "failed",
        "terminal_cause": "question_rounds_exhausted",
    }
    assert _requests(fresh.work_item_id)[-1]["id"] == fresh.request_id
    _reconcile()
    assert _state_labels(sink, 951) == {"curie-factory:needs-human"}


def test_cancel_after_the_question_settled_stops_asking_on_the_ticket(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    job = _asked(admitted, monkeypatch, 956)
    tr._finish(job, "failed", "no_pull_request")
    _reconcile()
    assert _state_labels(sink, 956) == {QUESTIONS_LABEL}

    _cancel(job)
    _reconcile()
    assert _round_row(job.work_item_id) is None
    assert _round_count(job.work_item_id) == (1, job.request_id)
    assert _state_labels(sink, 956) == set()


def test_a_question_comment_rendered_after_cancel_does_not_ask_for_an_answer(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, _github, sink = admitted
    job = _asked(admitted, monkeypatch, 957)
    tr._finish(job, "failed", "no_pull_request")
    _cancel(job)
    _reconcile()
    body = _status_body(sink, job.request_id)
    assert "Answer in a plain comment" not in body
    assert "Questions asked (round 1 of 3). The factory label was removed" in body
    assert _state_labels(sink, 957) == set()


async def _cancel_now(job: tr._Run) -> Any:
    async with job.client.app.state.sessionmaker() as session:
        version = await session.scalar(
            text("SELECT version FROM curie.work_items WHERE id = :id"),
            {"id": job.work_item_id},
        )
        result = await lifecycle.request_cancellation(
            session, work_item_id=job.work_item_id, expected_work_item_version=version
        )
        await session.commit()
        return result


def test_cancel_before_the_comment_is_written_refuses_with_no_write(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 952)
    original = factory_questions.begin_round

    async def cancelled_after_posting(*args: Any, **kwargs: Any) -> Any:
        begun = await original(*args, **kwargs)
        assert isinstance(begun, factory_questions.Posting)
        assert isinstance(await _cancel_now(job), lifecycle.WorkItemOutcome)
        return begun

    monkeypatch.setattr(factory_questions, "begin_round", cancelled_after_posting)
    refused = _ask(job)
    assert (refused.status_code, refused.json()["detail"]["code"]) == (
        409,
        "work_item_cancelled",
    )
    assert job.tracker.ledger.writes == 0
    assert _round_row(job.work_item_id) is None
    assert _round_count(job.work_item_id) == (0, None)


def test_cancel_during_the_comment_write_waits_for_the_round_then_deletes_it(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 958)
    original = factory_questions.post_round
    pending: list[asyncio.Task[Any]] = []

    async def cancelled_meanwhile(*args: Any, **kwargs: Any) -> str:
        comment_id = await original(*args, **kwargs)
        # The cancellation takes the WorkItem lock and then waits for the
        # question row, which the round being recorded holds.
        pending.append(asyncio.create_task(_cancel_now(job)))
        finished, _ = await asyncio.wait({pending[0]}, timeout=0.5)
        assert not finished
        return comment_id

    monkeypatch.setattr(factory_questions, "post_round", cancelled_meanwhile)
    posted = _ask(job)
    assert posted.status_code == 200, posted.text

    async def cancelled() -> Any:
        return await asyncio.wait_for(pending[0], timeout=30)

    assert isinstance(job.client.portal.call(cancelled), lifecycle.WorkItemOutcome)
    assert job.tracker.ledger.writes == 1
    assert _round_row(job.work_item_id) is None
    assert _round_count(job.work_item_id) == (1, job.request_id)


def test_cancel_then_relabel_keeps_the_round_cap(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    _client, github, _sink = admitted
    job = _asked(admitted, monkeypatch, 953)
    # Two more rounds were posted by earlier attempts on this WorkItem.
    tr._exec(
        "UPDATE curie.factory_question_rounds SET rounds_used = 3 WHERE work_item_id = :id",
        {"id": job.work_item_id},
    )
    tr._exec(
        "UPDATE curie.factory_question_round_counts SET rounds_used = 3 WHERE work_item_id = :id",
        {"id": job.work_item_id},
    )
    tr._finish(job, "failed", "no_pull_request")
    _cancel(job)
    tr._label(job.client, github, 953)
    relabelled = _requests(job.work_item_id)[-1]["id"]
    assert relabelled != job.request_id
    current = _take_over(job, relabelled)
    refused = _ask(current)
    assert (refused.status_code, refused.json()["detail"]["code"]) == (
        409,
        "question_rounds_exhausted",
    )


def test_a_relabel_that_supersedes_the_waiting_answer_clears_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from forge_fakes.github import _issue_event, _post

    client, github, _sink = admitted
    job = _asked(admitted, monkeypatch, 954)
    revision = _queue_revision(job)
    _answer(job.work_item_id, revision)
    tr._finish(job, "failed", "no_pull_request")
    _promote(job)
    assert _request(revision)["status"] == "waiting"

    github.advance_label_event(954)
    relabelled = _post(client, "issues", _issue_event("labeled", 954, label={"name": LABEL}))
    assert relabelled.status_code == 200, relabelled.text
    _reconcile()

    assert _request(revision)["status"] == "cancelled"
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"]) == ("cleared", 1)


# The halted turn's handoff (ADR 0286 decisions 2 and 11) -------------------------------

HANDOFF = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests" / "vectors" / "question-halt-turn.json"
    ).read_text(encoding="utf-8")
)


def _view(job: tr._Run) -> dict[str, Any]:
    response = job.client.get(
        f"/v1/internal/work-items/requests/{job.request_id}", headers=tr.WORKER_HEADERS
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


def test_the_request_view_reports_the_posted_round_the_worker_reads(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The worker skips its publish continuation on this field, so it is pinned.

    Red without the API change: the view has no ``questions_posted``.
    """

    job = _running(admitted, monkeypatch, 951)
    assert _view(job)["questions_posted"] is False
    assert _ask(job).status_code == 200
    view = _view(job)
    assert {key: view[key] for key in HANDOFF["request_view"]} == HANDOFF["request_view"]


def test_a_refused_ask_leaves_the_view_unposted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Liveness: a running request whose ask got a 409 for the cap posted nothing,
    so the worker still re-prompts it."""

    job = _refused_fourth(admitted, monkeypatch, 952)
    view = _view(job)
    assert (view["status"], view["questions_posted"]) == ("running", False)


def test_the_halted_turn_finish_settles_questions_asked_and_labels_on_one_pass(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The worker's finish for the runner's halted turn, exactly as pinned.

    The worker's handoff test reports this finish from the replayed frames; one
    status pass then sets the questions label (the reconciler runs every 5 s).
    """

    _client, _github, sink = admitted
    job = _asked(admitted, monkeypatch, 954)
    finish = HANDOFF["worker_finish"]
    tr._finish(job, finish["outcome"], finish["cause"])

    assert _request(job.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }
    _reconcile()
    assert _state_labels(sink, 954) == {QUESTIONS_LABEL}


def test_an_answer_before_the_halted_turn_settles_starts_exactly_one_next_request(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One plain answer queued before settlement starts one next request, no retry."""

    job = _asked(admitted, monkeypatch, 955)
    revision = _queue_revision(job)
    _answer(job.work_item_id, revision)
    finish = HANDOFF["worker_finish"]
    tr._finish(job, finish["outcome"], finish["cause"])

    assert _request(job.request_id) == {
        "status": "completed",
        "terminal_cause": "questions_asked",
    }
    _promote(job)
    assert [r["id"] for r in _requests(job.work_item_id)] == [job.request_id, revision]
    assert _request(revision)["status"] == "waiting"
