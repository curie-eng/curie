"""The factory question route, ``POST /work-items/questions`` (#5785).

Each case admits a labelled issue through a signed GitHub issues webhook
against create_app(), acquires and starts its implementation request as the
worker does, and calls the route over HTTP with the issue read capability the
internal mint route issues (the helpers of test_factory_tracker_results). The
tracker is the in-memory adapter keyed to that WorkItem's issue, the GitHub
comment fake through the production resolver, or the Jira Cloud fake through
the production Jira adapter.

The expected comment frame is spelled out here, independent of the code that
composes it.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import hashlib
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_factory_tracker_results as tr
import test_workitems as wi
from curie_api import factory_questions, factory_tracker_result_runtime
from curie_api.forges.capabilities import Operation
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import ReplyTarget
from curie_api.workitems import lifecycle
from forge_fakes import jira as jira_fake
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)
from test_factory_publication_recovery import api  # noqa: F401  (fixture)

QUESTIONS_URL = "/work-items/questions"
SUMMARY = "The export test reproduces: 2 rows are written for 3 input rows."
QUESTIONS = ["Should the trailing empty row be kept?", "Which delimiter do callers expect?"]
ANSWER_LINE = (
    "Answer in a plain comment on this ticket. Anyone who could have started this "
    "ticket can answer; no mention or relabel is needed."
)


def _frame(round_number: int, summary: str, questions: list[str]) -> str:
    items = "\n".join(f"{n}. {q}" for n, q in enumerate(questions, start=1))
    return (
        f"### Questions from the factory (round {round_number} of 3)\n\n{summary}\n\n"
        f"{items}\n\n{ANSWER_LINE}\n"
    )


def _ask(triage: tr._Triage, summary: Any = SUMMARY, questions: Any = None) -> Any:
    return triage.client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": triage.capability},
        json={"summary": summary, "questions": QUESTIONS if questions is None else questions},
    )


def _running(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Triage:
    """An implementation request, started, on the in-memory tracker."""

    triage = tr._triage(admitted, monkeypatch, number, tracker_result=False)
    tr._begin(triage)
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
    return row


def _marked(triage: tr._Triage) -> list[str]:
    marker = f"curie-factory-questions:{triage.request_id}"
    target = ReplyTarget.on_issue(triage.issue)
    return [
        body for _author, body in triage.tracker.marked_comments.comments(target) if marker in body
    ]


def _code(response: Any) -> str:
    return str(response.json()["detail"]["code"])


# Criterion 1: the memory tracker ----------------------------------------------------


def test_two_questions_post_one_marked_comment_and_record_round_one(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 801)
    response = _ask(triage)
    assert response.status_code == 200, response.text
    assert response.json() == {"round": 1, "rounds_limit": 3}
    expected = _frame(1, SUMMARY, QUESTIONS)
    (body,) = _marked(triage)
    assert body.startswith(expected)
    assert "\n1. Should the trailing empty row be kept?\n2. Which delimiter" in body
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["rounds_used"], row["state"]) == (1, "outstanding")
    assert row["answer_after"] is not None
    assert row["asking_request_id"] == triage.request_id
    assert row["pending_request_id"] is None
    assert row["comment_id"] is not None
    assert row["body_sha256"] == hashlib.sha256(expected.encode()).hexdigest()
    assert triage.tracker.ledger.writes == 1


def test_inputs_at_the_bounds_are_posted(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    triage = _running(admitted, monkeypatch, 802)
    questions = ["q" * 1000 for _ in range(9)] + ["first line\nsecond line"]
    response = _ask(triage, summary="s" * 4000, questions=questions)
    assert response.status_code == 200, response.text
    (body,) = _marked(triage)
    assert "10. first line\nsecond line" in body


def test_model_text_cannot_open_an_html_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 803)
    response = _ask(triage, summary="See <!-- curie-status:final -->", questions=["<!-- x -->?"])
    assert response.status_code == 200, response.text
    (body,) = _marked(triage)
    head = body.split("\n\n<!-- curie-factory-questions:")[0]
    assert "<!--" not in head


def test_model_text_is_redacted_mention_free_and_cannot_hold_the_frame_in_a_fence(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "ghp_" + "A1b2C3d4E5" * 3 + "F6g7H8"
    triage = _running(admitted, monkeypatch, 804)
    response = _ask(
        triage,
        summary=f"Ran it with {secret} and saw:\n```\ntraceback, never closed",
        questions=["Should @someone own this?"],
    )
    assert response.status_code == 200, response.text
    (body,) = _marked(triage)
    assert secret not in body
    assert "[REDACTED" in body
    assert "@someone" not in body and "@\u200bsomeone" in body
    # No line of the composed comment opens a fence, so the numbered questions
    # and the answer line render as the frame, not as code.
    assert not any(line.lstrip().startswith(("```", "~~~")) for line in body.splitlines())
    assert "\n1. Should @\u200bsomeone own this?\n" in body


# Criterion 2: GitHub and Jira Cloud ------------------------------------------------


def test_github_writes_one_comment_through_the_production_resolver(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 811)
    # Undo the in-memory substitution: the WorkItem's own GitHub tracker writes.
    monkeypatch.setattr(factory_tracker_result_runtime, "work_item_tracker", _REAL_TRACKER)
    _client, github, _server = admitted
    response = _ask(triage)
    assert response.status_code == 200, response.text
    marker = f"curie-factory-questions:{triage.request_id}"
    # The GitHub fake behind the app's HTTP client keeps each issue's comments.
    written = [c for c in github.issue_comments.get(811, []) if marker in c["body"]]
    assert len(written) == 1
    assert written[0]["body"].startswith(_frame(1, SUMMARY, QUESTIONS))
    assert written[0]["performed_via_github_app"] is not None
    assert _marked(triage) == []
    row = _round_row(triage.work_item_id)
    assert row is not None and row["comment_id"] == str(written[0]["id"])


def test_jira_writes_one_comment_with_the_marker_as_a_comment_property(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 812)
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Export drops a row", "Three rows in, two out.")
    ref = fake.ref(issue)

    def jira_target(_settings: Any, _client: Any, _work_item: Any) -> Any:
        tracker = jira_fake.jira_tracker(fake, client=jira_fake.fake_client(fake))
        return tracker.marked_comments, ref

    monkeypatch.setattr(factory_questions, "question_target", jira_target)
    writes = fake.writes
    response = _ask(triage)
    assert response.status_code == 200, response.text
    (comment,) = fake.issue(issue.id).comments
    marker = f"curie-factory-questions:{triage.request_id}"
    assert comment.properties["curie.factory"]["marker"] == marker
    text = jira_fake.flatten(comment.body)
    assert marker not in text
    assert "Questions from the factory (round 1 of 3)" in text
    assert "Which delimiter do callers expect?" in text
    assert fake.writes - writes >= 1
    row = _round_row(triage.work_item_id)
    assert row is not None and row["comment_id"] == str(comment.id)


_REAL_TRACKER = factory_tracker_result_runtime.work_item_tracker


# Criterion 3: refusals and replay ------------------------------------------------------


def test_replay_of_the_same_body_returns_the_first_answer_with_one_write(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 821)
    first = _ask(triage)
    again = _ask(triage)
    assert first.status_code == again.status_code == 200
    assert again.json() == first.json() == {"round": 1, "rounds_limit": 3}
    assert triage.tracker.ledger.writes == 1
    assert len(_marked(triage)) == 1
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["rounds_used"], row["version"]) == (1, 2)


def test_refused_a_different_body_from_the_same_request_is_already_asked(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 822)
    assert _ask(triage).status_code == 200
    refused = _ask(triage, questions=["A different question?"])
    assert refused.status_code == 409
    assert _code(refused) == "already_asked"
    assert triage.tracker.ledger.writes == 1


@pytest.mark.parametrize(
    ("summary", "questions"),
    [
        ("", QUESTIONS),
        ("   ", QUESTIONS),
        ("s" * 4001, QUESTIONS),
        (SUMMARY, []),
        (SUMMARY, ["q?"] * 11),
        (SUMMARY, [""]),
        (SUMMARY, ["q" * 1001]),
        (SUMMARY, ["first\n\nsecond"]),
        (SUMMARY, ["first\r\n  \r\nsecond"]),
        (SUMMARY, [3]),
        (["not", "text"], QUESTIONS),
    ],
)
def test_refused_input_outside_the_bounds_is_invalid_questions(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, summary: Any, questions: Any
) -> None:
    triage = _running(admitted, monkeypatch, 823)
    refused = _ask(triage, summary=summary, questions=questions)
    assert refused.status_code == 409
    assert refused.json()["detail"] == {
        "code": "invalid_questions",
        "message": "the questions are outside the tool's bounds",
    }
    assert triage.tracker.ledger.writes == 0
    assert _round_row(triage.work_item_id) is None


@pytest.mark.parametrize("body", [{"questions": ["Which branch?"]}, {"summary": "Found it."}, {}])
def test_refused_a_missing_field_is_invalid_questions_not_a_422(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    triage = _running(admitted, monkeypatch, 826)
    refused = triage.client.post(
        QUESTIONS_URL, headers={"X-Curie-Issue-Read": triage.capability}, json=body
    )
    assert refused.status_code == 409
    assert _code(refused) == "invalid_questions"
    assert triage.tracker.ledger.writes == 0


def test_a_question_with_one_crlf_line_break_is_accepted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The runner accepts a single CRLF; the route must not read it as a blank line.
    triage = _running(admitted, monkeypatch, 827)
    asked = _ask(triage, questions=["first line\r\nsecond line?"])
    assert asked.status_code == 200
    assert asked.json() == {"round": 1, "rounds_limit": 3}


def test_refused_a_tracker_result_request_is_not_implementation_contract(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = tr._triage(admitted, monkeypatch, 824, tracker_result=True)
    tr._begin(triage)
    refused = _ask(triage)
    assert (refused.status_code, _code(refused)) == (409, "not_implementation_contract")
    assert _round_row(triage.work_item_id) is None


def test_refused_a_request_that_is_not_running(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = tr._triage(admitted, monkeypatch, 825, tracker_result=False)
    refused = _ask(triage)
    assert (refused.status_code, _code(refused)) == (409, "request_not_running")
    assert _round_row(triage.work_item_id) is None


def _cancel(triage: tr._Triage) -> None:
    (row,) = _rows(
        "SELECT version FROM curie.work_items WHERE id = :id", {"id": triage.work_item_id}
    )

    async def go() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            result = await lifecycle.request_cancellation(
                session,
                work_item_id=triage.work_item_id,
                expected_work_item_version=row["version"],
            )
            await session.commit()
            return result

    assert isinstance(triage.client.portal.call(go), lifecycle.WorkItemOutcome)


def test_refused_a_cancelled_work_item(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    triage = _running(admitted, monkeypatch, 826)
    _cancel(triage)
    refused = _ask(triage)
    assert (refused.status_code, _code(refused)) == (409, "work_item_cancelled")
    assert triage.tracker.ledger.writes == 0


def _with_session(triage: tr._Triage, work: Any) -> Any:
    async def go() -> Any:
        async with triage.client.app.state.sessionmaker() as session:
            return await work(session)

    return triage.client.portal.call(go)


def test_refused_while_the_work_item_has_an_open_pull_request(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 827)
    (item,) = _rows(
        "SELECT agent_id, conversation_id FROM curie.work_items WHERE id = :id",
        {"id": triage.work_item_id},
    )

    async def open_lineage(session: Any) -> uuid.UUID:
        lineage = await wi._lineage(
            session, item["agent_id"], conversation=item["conversation_id"], status="open"
        )
        await session.commit()
        return lineage

    lineage_id = _with_session(triage, open_lineage)
    tr._exec(
        "UPDATE curie.work_items SET publication_lineage_id = :lineage WHERE id = :id",
        {"lineage": lineage_id, "id": triage.work_item_id},
    )
    refused = _ask(triage)
    assert (refused.status_code, _code(refused)) == (409, "pull_request_open")
    # A closed lineage is no open pull request: the same request may ask.
    tr._exec(
        "UPDATE curie.thread_publication_lineages SET status = 'closed' WHERE id = :id",
        {"id": lineage_id},
    )
    assert _ask(triage).status_code == 200


def test_refused_after_the_request_published(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 828)

    async def publish(session: Any) -> None:
        await wi._attach_own_publication(
            session, triage.work_item_id, triage.request_id, status="failed"
        )

    _with_session(triage, publish)
    refused = _ask(triage)
    assert (refused.status_code, _code(refused)) == (409, "already_published")
    assert _round_row(triage.work_item_id) is None


def test_refused_past_three_rounds_marks_the_row_exhausted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 829)
    tr._exec(
        "INSERT INTO curie.factory_question_rounds "
        "(work_item_id, rounds_used, state, asking_request_id, body_sha256) "
        "VALUES (:id, 3, 'cleared', :asker, :digest)",
        {"id": triage.work_item_id, "asker": uuid.uuid4(), "digest": "cd" * 32},
    )
    refused = _ask(triage)
    assert (refused.status_code, _code(refused)) == (409, "question_rounds_exhausted")
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["asking_request_id"]) == (
        "exhausted",
        3,
        triage.request_id,
    )
    again = _ask(triage)
    assert (again.status_code, _code(again)) == (409, "question_rounds_exhausted")
    assert triage.tracker.ledger.writes == 0


def test_refused_a_capability_for_another_issue(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 830)
    refused = triage.client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": "wir.not-a-capability"},
        json={"summary": SUMMARY, "questions": QUESTIONS},
    )
    assert refused.status_code == 401
    assert _round_row(triage.work_item_id) is None


# Criterion 4: the tracker is unavailable ----------------------------------------------


def test_tracker_unavailable_leaves_the_round_posting_and_the_request_settles_as_today(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 841)
    triage.tracker.write_faults.abort_before_next(
        Operation.UPSERT_MARKED, Unavailable("tracker down")
    )
    refused = _ask(triage)
    assert refused.status_code == 503
    assert _code(refused) == "tracker_unavailable"
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["comment_id"]) == ("posting", 0, None)
    assert _marked(triage) == []

    tr._finish(triage, "failed", "no_pull_request")

    assert _request(triage.request_id) == {"status": "failed", "terminal_cause": "no_pull_request"}
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"]) == ("cleared", 0)


def test_a_retry_after_an_unavailable_tracker_posts_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    triage = _running(admitted, monkeypatch, 842)
    triage.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    lost = _ask(triage)
    assert (lost.status_code, _code(lost)) == (503, "tracker_unavailable")
    # The comment landed but its answer was lost; the retry edits it in place.
    retried = _ask(triage)
    assert retried.status_code == 200, retried.text
    assert retried.json() == {"round": 1, "rounds_limit": 3}
    assert len(_marked(triage)) == 1
    row = _round_row(triage.work_item_id)
    assert row is not None and (row["state"], row["rounds_used"]) == ("outstanding", 1)
