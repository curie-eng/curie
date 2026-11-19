"""A question round posted by a new API replica ends with the question and
stays answerable when an older replica settles it during a rolling upgrade
(#5785, ADR 0286).

The API rolls with ``maxUnavailable: 0`` and ``maxSurge: 1``, so a replica
built before the question rounds can serve beside one built after. Only a
new replica serves ``POST /work-items/questions`` (an older one answers 404,
and the runner tool reports a refusal), so a round row exists only once a new
replica posted it. An older replica can still take the worker's finish, an
owner loss, a cancellation, the promotion of a queued answer, or a status
comment refresh, and it runs each without the round hooks.

An older replica is modelled as this lifecycle with the round hooks removed
(`_older_replica`): a release built before #5785 has no question table, so it
never calls them, and it runs no re-drive. Its status pass is modelled by
the writes it makes (`_older_status_pass`): it maps a ``completed`` request
to ``curie-factory:pr-open`` and renders an unknown cause as a failure. The
answer core (#5786) is not built yet, so an answer is the row change it makes
on a new replica: ``answered`` with the revision request it queued.

Each case ends the roll (only new replicas reconcile from there) and checks
the outcome a ticket sees: the asking run ends ``completed`` with
``questions_asked``, the ticket shows ``curie-factory:questions`` and a
status comment that asks for an answer, and an answer starts exactly one
continuation, after which the labels leave the question state. The guard at
the end fails when a new reader or writer of the round state appears, so the
next settlement path cannot skip this proof.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import ast
import contextlib
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_factory_tracker_results as tr
from curie_api import factory_questions
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import body_digest
from curie_api.workitems import lifecycle
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    admitted,
    comments,
)
from test_factory_publication_recovery import api  # noqa: F401  (fixture)
from test_factory_question_ending import (
    QUESTIONS_LABEL,
    _answer,
    _ask,
    _asked,
    _cancel,
    _lose_owner,
    _queue_revision,
    _request,
    _requests,
    _round_row,
    _state_labels,
    _status_body,
    _take_over,
    _versions,
)
from test_factory_terminus import _reconcile

NEEDS_HUMAN = "curie-factory:needs-human"
PR_OPEN = "curie-factory:pr-open"
RUNNING = "curie-factory:running"
ASKS_FOR_ANSWER = "Questions asked (round 1 of 3). Answer in a plain comment on this ticket."
QUESTION_ENDING = {"status": "completed", "terminal_cause": "questions_asked"}

# The settlement hooks #5785 added to the lifecycle. A replica built before
# #5785 runs the same transitions without them.
OLDER_REPLICA_LACKS = frozenset(
    {"settle_round", "question_ending", "rounds_exhausted_for", "delete_for_cancellation"}
)


@contextmanager
def _older_replica() -> Iterator[None]:
    """Settlement and reconciliation as a replica built before the rounds runs them."""

    async def no_round(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def not_exhausted(*_args: Any, **_kwargs: Any) -> bool:
        return False

    async def nothing_to_redrive(*_args: Any, **_kwargs: Any) -> list[uuid.UUID]:
        return []

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(factory_questions, "settle_round", no_round)
        patch.setattr(factory_questions, "question_ending", no_round)
        patch.setattr(factory_questions, "rounds_exhausted_for", not_exhausted)
        patch.setattr(factory_questions, "delete_for_cancellation", no_round)
        # Absent on a build without the re-drive; an older replica has none.
        patch.setattr(lifecycle, "question_rounds_to_redrive", nothing_to_redrive, raising=False)
        yield


def _older_status_pass(
    sink: Any,
    request_id: uuid.UUID,
    number: int,
    *,
    label: str = PR_OPEN,
    cause: str = "questions_asked",
) -> None:
    """What an older replica's status pass writes for a finished request: a
    failure body (it renders ``questions_asked``, a cause it does not know,
    as one), ``label`` added (``pr-open`` for a ``completed`` request) while
    removing only the labels it knows, never the question label, recorded as
    applied, and the row finalized with that body's digest."""

    everything = list(sink.comments) + [c for listed in sink.lists.values() for c in listed]
    (comment,) = [c for c in everything if marker_for(request_id) in (c.get("body") or "")]
    body = (
        f"{marker_for(request_id)}\n"
        "Could not complete: the run stopped for a reason Curie did not recognize.\n"
        f"Cause: {cause}\n"
    )
    comment["body"] = body
    labels = sink.issue_labels.setdefault(number, set())
    labels -= {"curie-factory:queued", RUNNING, NEEDS_HUMAN, PR_OPEN}
    labels.add(label)
    tr._exec(
        "UPDATE curie.factory_terminal_notices SET applied_label = :label, "
        "rendered_digest = :digest, finalized_at = clock_timestamp() "
        "WHERE execution_request_id = :id",
        {"label": label, "digest": body_digest(body), "id": request_id},
    )


def _after_the_roll() -> None:
    """The roll is done: a new replica's reconciler and status pass run."""

    _reconcile()


def _shows_the_question(sink: Any, triage: tr._Triage, number: int) -> None:
    assert _request(triage.request_id) == QUESTION_ENDING
    row = _round_row(triage.work_item_id)
    assert row is not None and row["asking_request_id"] == triage.request_id
    assert _state_labels(sink, number) == {QUESTIONS_LABEL}
    body = _status_body(sink, triage.request_id)
    assert ASKS_FOR_ANSWER in body
    assert "Could not complete" not in body


def _after_asker(triage: tr._Triage) -> list[tuple[uuid.UUID, str]]:
    """Every request the WorkItem admitted after the asker, any status."""

    requests = _requests(triage.work_item_id)
    after = requests[[r["id"] for r in requests].index(triage.request_id) + 1 :]
    return [(r["id"], r["status"]) for r in after]


def _to_redrive(triage: tr._Triage) -> list[uuid.UUID]:
    """The WorkItems the re-drive would select now."""

    async def go() -> list[uuid.UUID]:
        async with triage.client.app.state.sessionmaker() as session:
            return await lifecycle.question_rounds_to_redrive(session, limit=100)

    return [i for i in triage.client.portal.call(go) if i == triage.work_item_id]


def _redrive(triage: tr._Triage) -> None:
    async def go() -> None:
        async with triage.client.app.state.sessionmaker() as session:
            await lifecycle.redrive_question_round(session, work_item_id=triage.work_item_id)
            await session.commit()

    triage.client.portal.call(go)


def _answer_snapshot(triage: tr._Triage, revision: uuid.UUID) -> None:
    """The run snapshot of a promoted answer, as the answer core writes it:
    the asker's requester and reply route and the answer as the objective.
    `_queue_revision` leaves it empty, and a request without one is never
    dispatched."""

    tr._exec(
        "UPDATE curie.execution_requests AS answer SET "
        "objective = 'Answer: replace the rows on a re-run.', "
        "requester = asker.requester, reply_kind = asker.reply_kind, "
        "reply_address = asker.reply_address, "
        "reply_conversation_id = asker.reply_conversation_id "
        "FROM curie.execution_requests AS asker "
        "WHERE answer.id = :answer AND answer.status = 'waiting' AND asker.id = :asker",
        {"answer": revision, "asker": triage.request_id},
    )


def _run_continuation(triage: tr._Triage, revision: uuid.UUID) -> tr._Triage:
    _answer_snapshot(triage, revision)
    return _take_over(triage, revision)


def _continued(sink: Any, triage: tr._Triage, number: int, revision: uuid.UUID) -> dict[str, Any]:
    """Run the answer's continuation to its end and record what the ticket
    shows while it runs and after."""

    continuation = _run_continuation(triage, revision)
    _reconcile()
    running = _state_labels(sink, number)
    tr._finish(continuation, "failed", "no_pull_request")
    _reconcile()
    row = _round_row(triage.work_item_id)
    return {
        "running": running,
        "labels": _state_labels(sink, number),
        "asks": ASKS_FOR_ANSWER in _status_body(sink, revision),
        "requests": [(r["status"], r["terminal_cause"]) for r in _requests(triage.work_item_id)],
        "row": None if row is None else row["state"],
    }


def _one_continuation(triage: tr._Triage, revision: uuid.UUID) -> None:
    """The answer's revision is the only request after the asker, and live."""

    requests = _requests(triage.work_item_id)
    after = requests[[r["id"] for r in requests].index(triage.request_id) + 1 :]
    assert [r["id"] for r in after] == [revision], requests
    assert after[0]["status"] == "waiting"


def _snapshot(sink: Any, triage: tr._Triage, number: int, revision: uuid.UUID) -> dict[str, Any]:
    """What the ticket and its WorkItem show once the answer is promoted."""

    requests = _requests(triage.work_item_id)
    after = requests[[r["id"] for r in requests].index(triage.request_id) + 1 :]
    row = _round_row(triage.work_item_id)
    return {
        "requests": [(r["status"], r["terminal_cause"]) for r in requests],
        "one_continuation": [r["id"] for r in after] == [revision],
        "row": None
        if row is None
        else (row["state"], row["rounds_used"], row["pending_request_id"] == revision),
        "labels": _state_labels(sink, number),
        "asks": ASKS_FOR_ANSWER in _status_body(sink, triage.request_id),
    }


def _new_replicas_only(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int, *, answer_at: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """The same ask, answer and continuation with no older replica anywhere.
    ``answer_at`` places the answer as the mixed run did: before the asker
    finished, before the first pass after it, or after that pass."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, number)
    revision: uuid.UUID | None = None
    if answer_at == "before_finish":
        revision = _queue_revision(triage)
        _answer(triage.work_item_id, revision)
    tr._finish(triage, "failed", "no_pull_request")
    if answer_at == "before_pass":
        revision = _queue_revision(triage)
        _answer(triage.work_item_id, revision)
    _reconcile()
    if revision is None:
        revision = _queue_revision(triage)
        _answer(triage.work_item_id, revision)
    _reconcile()
    _reconcile()
    return _snapshot(sink, triage, number, revision), _continued(sink, triage, number, revision)


def _matches_new_replicas_only(
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    triage: tr._Triage,
    revision: uuid.UUID,
    number: int,
    *,
    answer_at: str = "after_pass",
) -> None:
    """After the answer the ticket ends where it ends with no older replica:
    the asker ended with the question, the answer is its one continuation,
    and the labels and comments are the same. The continuation then runs and
    ends: the ticket leaves the question state while it runs, the round
    clears, and the labels and comment show its own outcome."""

    _client, _github, sink = admitted
    _reconcile()
    _one_continuation(triage, revision)
    mixed = _snapshot(sink, triage, number, revision)
    assert mixed["requests"][0] == ("completed", "questions_asked")
    assert mixed["one_continuation"] is True
    after = _continued(sink, triage, number, revision)
    assert after["running"] == {RUNNING}
    assert after["labels"] == {NEEDS_HUMAN}
    assert after["asks"] is False
    assert after["row"] == "cleared"
    assert after["requests"] == [
        ("completed", "questions_asked"),
        ("failed", "no_pull_request"),
    ]
    baseline = _new_replicas_only(admitted, monkeypatch, number + 50, answer_at=answer_at)
    assert (mixed, after) == baseline


# The worker's finish reaches an older replica ----------------------------------------


@pytest.mark.parametrize("promoted_by", ["older", "new"])
def test_a_finish_on_an_older_replica_still_ends_with_the_question(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, promoted_by: str
) -> None:
    """The older replica fails the asker (``no_pull_request``) and its status
    pass shows needs-human. After the roll the run ends with the question, the
    ticket asks for an answer, and the answer, promoted by either replica,
    starts exactly one continuation."""

    number = 1201 if promoted_by == "older" else 1202
    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, number)
    with _older_replica():
        tr._finish(triage, "failed", "no_pull_request")
        assert _request(triage.request_id) == {
            "status": "failed",
            "terminal_cause": "no_pull_request",
        }
        _reconcile()
        assert _state_labels(sink, number) == {NEEDS_HUMAN}

    _after_the_roll()
    _shows_the_question(sink, triage, number)
    assert _round_row(triage.work_item_id)["state"] == "outstanding"  # type: ignore[index]
    assert [r["id"] for r in _requests(triage.work_item_id)] == [triage.request_id]

    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    if promoted_by == "older":
        with _older_replica():
            _reconcile()
    else:
        _reconcile()
    _one_continuation(triage, revision)
    _matches_new_replicas_only(admitted, monkeypatch, triage, revision, number)


def test_an_answer_before_the_older_finish_keeps_one_continuation(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The answer arrives while the asker runs; the asker's finish then lands
    on an older replica. The run still ends with the question and the queued
    answer is the one continuation."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1203)
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    with _older_replica():
        tr._finish(triage, "failed", "no_pull_request")
        _reconcile()
    assert _request(triage.request_id)["status"] == "failed"

    _after_the_roll()
    assert _request(triage.request_id) == QUESTION_ENDING
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"]) == ("answered", revision)
    _one_continuation(triage, revision)
    _matches_new_replicas_only(
        admitted, monkeypatch, triage, revision, 1203, answer_at="before_finish"
    )


def test_an_answer_between_the_older_finish_and_the_roll_keeps_one_continuation(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The asker settled failed on an older replica, and a new replica takes
    the answer before any re-drive (the row still reads ``outstanding``)."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1204)
    with _older_replica():
        tr._finish(triage, "failed", "no_pull_request")
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)

    _after_the_roll()
    assert _request(triage.request_id) == QUESTION_ENDING
    _one_continuation(triage, revision)
    _matches_new_replicas_only(
        admitted, monkeypatch, triage, revision, 1204, answer_at="before_pass"
    )


def test_a_relabel_before_the_roll_is_the_one_continuation(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After the older finish someone re-adds the label, which starts a fresh
    run. The re-drive then leaves the failed asker as it is and clears the
    round, so the ticket runs exactly one attempt and never asks for an
    answer no run will take."""

    _client, github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1205)
    with _older_replica():
        tr._finish(triage, "failed", "no_pull_request")
        tr._label(triage.client, github, 1205)
    relabelled = _requests(triage.work_item_id)[-1]["id"]
    assert relabelled != triage.request_id

    _after_the_roll()
    assert _request(triage.request_id)["status"] == "failed"
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "cleared"
    assert _after_asker(triage) == [(relabelled, "waiting")]
    assert QUESTIONS_LABEL not in _state_labels(sink, 1205)
    assert _to_redrive(triage) == []


def test_a_relabel_beside_a_queued_answer_is_not_redriven_again(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An answer is queued while the asker runs, the asker's finish lands on
    an older replica, and the label is re-added before the answer is
    promoted. The re-drive leaves the failed asker as it is (the relabel
    already continued the attempt) and the row is not selected again on the
    next pass. The requests after the asker are the ones a ticket gets with
    no older replica: the relabel's run, then the queued answer."""

    _client, github, sink = admitted

    def run(number: int, *, older: bool) -> tuple[tr._Triage, uuid.UUID, uuid.UUID]:
        triage = _asked(admitted, monkeypatch, number)
        revision = _queue_revision(triage)
        _answer(triage.work_item_id, revision)
        with _older_replica() if older else contextlib.nullcontext():
            tr._finish(triage, "failed", "no_pull_request")
        tr._label(triage.client, github, number)
        relabelled = _requests(triage.work_item_id)[-1]["id"]
        return triage, revision, relabelled

    triage, revision, relabelled = run(1207, older=True)
    _after_the_roll()
    assert _request(triage.request_id)["status"] == "failed"
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["state"], row["pending_request_id"]) == ("answered", revision)
    assert _to_redrive(triage) == []
    mixed = _after_asker(triage)
    assert mixed == [(revision, "queued"), (relabelled, "waiting")]

    baseline, base_revision, base_relabelled = run(1257, older=False)
    _reconcile()
    assert _after_asker(baseline) == [(base_revision, "queued"), (base_relabelled, "waiting")]


# Owner loss on an older replica -------------------------------------------------------


def test_an_owner_loss_on_an_older_replica_settles_as_on_the_new_one(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Owner loss is not the question ending on either version (ADR 0206 owns
    it): the asker fails ``owner_lost`` and one successor reruns the attempt.
    The older replica keeps the row ``outstanding``; after the roll it is
    ``cleared`` as the new settlement leaves it, so no comment can answer a
    lost run, and the successor can still ask and end with the question."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1206)
    with _older_replica():
        _lose_owner(triage)
    assert _round_row(triage.work_item_id)["state"] == "outstanding"  # type: ignore[index]

    _after_the_roll()
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "cleared"
    requests = _requests(triage.work_item_id)
    assert len(requests) == 2, requests
    successor = _take_over(triage, requests[-1]["id"])
    posted = _ask(successor, ["Round 2: should a re-run replace the rows?"])
    assert posted.status_code == 200, posted.text
    assert posted.json() == {"round": 2, "rounds_limit": 3}
    tr._finish(successor, "failed", "no_pull_request")
    assert _request(successor.request_id) == QUESTION_ENDING
    _reconcile()
    assert _state_labels(sink, 1206) == {QUESTIONS_LABEL}


# Cancellation on an older replica ------------------------------------------------------


def test_an_older_replica_cancelling_after_the_question_ending_drops_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The label is removed and an older replica cancels the WorkItem but keeps
    the row. After the roll the row is gone, as the new cancellation deletes
    it, the label leaves the question state, and a relabel asks round 2."""

    _client, github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1211)
    tr._finish(triage, "failed", "no_pull_request")
    _reconcile()
    assert _state_labels(sink, 1211) == {QUESTIONS_LABEL}
    with _older_replica():
        _cancel(triage)
    assert _round_row(triage.work_item_id) is not None

    _after_the_roll()
    assert _round_row(triage.work_item_id) is None
    assert _state_labels(sink, 1211) == set()

    tr._label(triage.client, github, 1211)
    relabelled = _requests(triage.work_item_id)[-1]["id"]
    fresh = _take_over(triage, relabelled)
    posted = _ask(fresh, ["Round 2: should a re-run replace the rows?"])
    assert posted.status_code == 200, posted.text
    assert posted.json() == {"round": 2, "rounds_limit": 3}


def test_a_relabel_after_an_older_cancellation_drops_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An older replica cancels the WorkItem after the question ending and
    keeps the row; the label is re-added on a new replica before any re-drive.
    The relabel drops the cancelled round, as the new cancellation would have,
    so the fresh run is not taken for an answer to it and asks round 2."""

    _client, github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1213)
    tr._finish(triage, "failed", "no_pull_request")
    _reconcile()
    with _older_replica():
        _cancel(triage)
    assert _round_row(triage.work_item_id) is not None

    tr._label(triage.client, github, 1213)
    assert _round_row(triage.work_item_id) is None
    relabelled = _requests(triage.work_item_id)[-1]["id"]
    assert _after_asker(triage) == [(relabelled, "waiting")]
    _after_the_roll()
    assert _round_row(triage.work_item_id) is None
    fresh = _take_over(triage, relabelled)
    posted = _ask(fresh, ["Round 2: should a re-run replace the rows?"])
    assert posted.status_code == 200, posted.text
    assert posted.json() == {"round": 2, "rounds_limit": 3}


def test_a_new_round_after_the_selection_is_left_to_its_own_settlement(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The re-drive selects a WorkItem whose queued answer an older replica
    ended without the round hook. Before it takes the lock, the label is
    re-added and the fresh run asks round 2, which keeps the old pending
    request on the row. The stale selection must not settle that request
    against the new round."""

    _client, github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1214)
    tr._finish(triage, "failed", "no_pull_request")
    _reconcile()
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    # The older replica ended the queued answer without settling the round.
    tr._exec(
        "UPDATE curie.execution_requests SET status = 'cancelled', "
        "terminal_cause = 'issue_cancelled', terminal_at = clock_timestamp(), "
        "version = version + 1 WHERE id = :id",
        {"id": revision},
    )
    assert _to_redrive(triage) == [triage.work_item_id]

    tr._label(triage.client, github, 1214)
    relabelled = _requests(triage.work_item_id)[-1]["id"]
    fresh = _take_over(triage, relabelled)
    posted = _ask(fresh, ["Round 2: should a re-run replace the rows?"])
    assert posted.status_code == 200, posted.text
    before = _round_row(triage.work_item_id)
    assert before is not None
    assert (before["asking_request_id"], before["pending_request_id"]) == (relabelled, revision)

    _redrive(triage)
    after = _round_row(triage.work_item_id)
    assert after is not None
    assert (after["state"], after["asking_request_id"]) == ("outstanding", relabelled)
    tr._finish(fresh, "failed", "no_pull_request")
    assert _request(relabelled) == QUESTION_ENDING


def test_a_round_after_a_superseded_answer_keeps_its_question_ending(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A relabel supersedes round 1's waiting answer, which stays on the row;
    the relabelled run asks round 2 and an older replica takes its finish.
    The re-drive gives round 2 its question ending and leaves it
    ``outstanding``: the superseded answer belongs to round 1."""

    _client, github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1215)
    tr._finish(triage, "failed", "no_pull_request")
    _reconcile()
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    _reconcile()
    tr._label(triage.client, github, 1215)
    assert _request(revision) == {"status": "cancelled", "terminal_cause": "issue_cancelled"}
    relabelled = _requests(triage.work_item_id)[-1]["id"]
    fresh = _take_over(triage, relabelled)
    posted = _ask(fresh, ["Round 2: should a re-run replace the rows?"])
    assert posted.json() == {"round": 2, "rounds_limit": 3}
    with _older_replica():
        tr._finish(fresh, "failed", "no_pull_request")
    assert _request(relabelled)["status"] == "failed"

    _after_the_roll()
    assert _request(relabelled) == QUESTION_ENDING
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["asking_request_id"], row["rounds_used"]) == (
        "outstanding",
        relabelled,
        2,
    )
    assert _state_labels(sink, 1215) == {QUESTIONS_LABEL}


def test_an_answer_that_ran_before_the_redrive_still_gives_the_asker_its_ending(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An older replica fails the asker and promotes its queued answer; the
    continuation runs and ends on a new replica, which clears the round before
    any re-drive. The asker still ends with the question, and the ticket shows
    the continuation's outcome, as with no older replica."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1216)
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    with _older_replica():
        tr._finish(triage, "failed", "no_pull_request")
        _reconcile()
    continuation = _run_continuation(triage, revision)
    tr._finish(continuation, "failed", "no_pull_request")
    assert _round_row(triage.work_item_id)["state"] == "cleared"  # type: ignore[index]
    assert _request(triage.request_id)["status"] == "failed"

    _after_the_roll()
    assert _request(triage.request_id) == QUESTION_ENDING
    assert _round_row(triage.work_item_id)["state"] == "cleared"  # type: ignore[index]
    assert _after_asker(triage) == [(revision, "failed")]
    assert _state_labels(sink, 1216) == {NEEDS_HUMAN}
    assert _to_redrive(triage) == []


def test_an_older_status_pass_on_the_continuation_does_not_keep_the_question_label(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The answer's continuation runs and ends while older replicas take its
    status updates. An older pass removes only the labels it knows, so the
    question label stays beside the continuation's own. After the roll the
    ticket carries the continuation's label alone."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1217)
    tr._finish(triage, "failed", "no_pull_request")
    _reconcile()
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    _reconcile()
    continuation = _run_continuation(triage, revision)
    # The continuation's comment is posted; the older pass that takes its
    # running update adds the running label and keeps the question label.
    _reconcile()
    sink.issue_labels[1217] = {QUESTIONS_LABEL, RUNNING}
    with _older_replica():
        tr._finish(continuation, "failed", "no_pull_request")
    _older_status_pass(sink, revision, 1217, label=NEEDS_HUMAN, cause="no_pull_request")
    assert _state_labels(sink, 1217) == {QUESTIONS_LABEL, NEEDS_HUMAN}

    _after_the_roll()
    assert _round_row(triage.work_item_id)["state"] == "cleared"  # type: ignore[index]
    assert _state_labels(sink, 1217) == {NEEDS_HUMAN}


def _record_termination(triage: tr._Triage) -> None:
    """The worker records that the cancelled request's sandbox is gone."""

    work_item_version, request_version = _versions(triage, triage.request_id)

    async def terminate() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            result = await lifecycle.record_runtime_termination(
                session,
                work_item_id=triage.work_item_id,
                request_id=triage.request_id,
                termination_observation="sandbox gone after the cancellation",
                expected_work_item_version=work_item_version,
                expected_request_version=request_version,
            )
            await session.commit()
            return result

    assert isinstance(triage.client.portal.call(terminate), lifecycle.WorkItemOutcome)


def test_an_older_replica_cancelling_the_running_asker_drops_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The label is removed while the asker runs and an older replica takes
    the cancellation; the worker's termination record lands on a new one."""

    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, 1212)
    with _older_replica():
        _cancel(triage)
    assert _request(triage.request_id)["status"] == "cancellation_requested"
    _record_termination(triage)
    assert _request(triage.request_id)["status"] == "cancelled"

    _after_the_roll()
    assert _round_row(triage.work_item_id) is None
    assert QUESTIONS_LABEL not in _state_labels(sink, 1212)


# The status comment refresh on an older replica ---------------------------------------


@pytest.mark.parametrize("older_first", [True, False])
def test_an_older_status_pass_is_rewritten_after_the_roll(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, older_first: bool
) -> None:
    """A question ending settled on a new replica is rendered by an older
    replica's status pass, before or after the new pass. After the roll the
    ticket carries exactly the question label and a comment asking for an
    answer, and the answer starts one continuation."""

    number = 1221 if older_first else 1222
    _client, _github, sink = admitted
    triage = _asked(admitted, monkeypatch, number)
    # The status comment is posted while the run is live, as at admission.
    _reconcile()
    assert _state_labels(sink, number) == {RUNNING}
    tr._finish(triage, "failed", "no_pull_request")
    assert _request(triage.request_id) == QUESTION_ENDING
    if not older_first:
        _reconcile()
        _shows_the_question(sink, triage, number)
    _older_status_pass(sink, triage.request_id, number)
    assert PR_OPEN in _state_labels(sink, number)

    _after_the_roll()
    _shows_the_question(sink, triage, number)
    revision = _queue_revision(triage)
    _answer(triage.work_item_id, revision)
    _reconcile()
    _one_continuation(triage, revision)
    _matches_new_replicas_only(admitted, monkeypatch, triage, revision, number)


# Guard: every reader and writer of the round state is covered above --------------------

_API_SRC = Path(__file__).resolve().parents[1] / "src" / "curie_api"
_WORKER_SRC = Path(__file__).resolve().parents[2] / "worker" / "src"
_DISPATCHER_SRC = Path(__file__).resolve().parents[2] / "dispatcher" / "src"
_MODELS = frozenset({"FactoryQuestionRound", "FactoryQuestionRoundCount", "FactoryAnswerReceipt"})
_TABLES = ("factory_question_round", "factory_answer_receipt")

# Each module outside factory_questions.py and models.py that reads or
# writes the round state, and what it uses. The lifecycle names are the
# hooks an older replica lacks (OLDER_REPLICA_LACKS) and the re-drive that
# repairs what it skipped; the route is served by new replicas only; the
# status pass is modelled by `_older_status_pass`. A new entry is a
# settlement path whose behaviour beside an older replica is not yet proved:
# add its case here first.
_EXPECTED_USES = {
    "curie_api/factory_notices.py": {
        "QUESTIONS_ASKED",
        "QUESTIONS_LABEL",
        "ROUND_LIMIT",
        "asked_round",
    },
    "curie_api/routers/work_item_issue.py": {
        "ALREADY_ASKED",
        "ALREADY_PUBLISHED",
        "INVALID_CONTEXT",
        "INVALID_QUESTIONS",
        "NOT_IMPLEMENTATION_CONTRACT",
        "PULL_REQUEST_OPEN",
        "Posting",
        "QUESTION_ROUNDS_EXHAUSTED",
        "QuestionsRefused",
        "REQUEST_NOT_RUNNING",
        "ROUND_LIMIT",
        "TrackerUnavailable",
        "WORK_ITEM_CANCELLED",
        "begin_round",
        "input_valid",
        "post_and_record",
    },
    "curie_api/workitems/lifecycle.py": {
        "ANSWERED",
        "CLEARED",
        "OUTSTANDING",
        "QUESTIONS_ASKED",
        "QUESTION_ROUNDS_EXHAUSTED",
        "locked_round",
        "rows_an_older_replica_settled",
        *OLDER_REPLICA_LACKS,
    },
}


def _uses(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    aliases: set[str] = set()
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if (node.module or "").endswith("factory_questions"):
                    found.add(alias.name)
                elif alias.name == "factory_questions":
                    aliases.add(alias.asname or alias.name)
                if alias.name in _MODELS:
                    found.add(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.endswith("factory_questions"):
                    aliases.add(alias.asname or alias.name)
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in aliases
        ):
            found.add(node.attr)
        elif isinstance(node, ast.Name) and node.id in _MODELS:
            found.add(node.id)
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and any(table in node.value for table in _TABLES)
        ):
            found.add("<sql>")
    return found


def test_every_reader_and_writer_of_the_round_state_is_covered() -> None:
    uses: dict[str, set[str]] = {}
    for root in (_API_SRC, _WORKER_SRC, _DISPATCHER_SRC):
        for path in sorted(root.rglob("*.py")):
            if path.parent == _API_SRC and path.name in {"factory_questions.py", "models.py"}:
                continue
            found = _uses(path)
            if found:
                relative = path.relative_to(_API_SRC.parent if root == _API_SRC else root)
                uses[relative.as_posix()] = found
    assert uses == _EXPECTED_USES, (
        "A module began or stopped reading or writing the question round state. "
        "Prove its path beside an older replica in this file, then update "
        "_EXPECTED_USES."
    )


def test_the_older_replica_model_removes_hooks_that_exist() -> None:
    for name in OLDER_REPLICA_LACKS:
        assert callable(getattr(factory_questions, name)), name
    assert callable(lifecycle.question_rounds_to_redrive)
