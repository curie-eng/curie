"""The factory question route, ``POST /work-items/questions`` (#5785).

Each case admits a labelled issue through a signed GitHub issues webhook
against create_app(), acquires and starts its implementation request as the
worker does, and calls the route over HTTP with the issue read capability the
internal mint route issues (the helpers of factory_run_support). The
tracker is the in-memory adapter keyed to that WorkItem's issue, the GitHub
comment fake through the production resolver, or the Jira Cloud fake through
the production Jira adapter.

The expected comment frame is spelled out here, independent of the code that
composes it.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from markdown_it import MarkdownIt

sys.path.insert(0, str(Path(__file__).resolve().parent))

import factory_run_support as tr
import test_workitems as wi
from curie_api import completion_contract, factory_questions
from curie_api.config import get_settings
from curie_api.forges.capabilities import Operation
from curie_api.forges.errors import Unavailable
from curie_api.forges.jira_cloud.comments import BODY_LIMIT
from curie_api.forges.types import ReplyTarget
from curie_api.workitems import lifecycle
from forge_fakes import jira as jira_fake
from forge_fakes.github import forges_env_json
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
        f"### Questions from the factory (round {round_number} of 3)\n\n"
        f"{summary}\n\n{items}\n\n{ANSWER_LINE}\n"
    )


def test_questions_comment_keeps_a_short_summary() -> None:
    summary = "Steps tried:\n1. run the export and compare the row counts."
    questions = [
        "Should the trailing empty row be kept?",
        "Which delimiter do callers expect?",
    ]
    body = factory_questions.compose_body(1, summary, questions)
    assert "### Questions from the factory (round 1 of 3)" in body
    assert "Steps tried:" in body
    assert "1\\. run the export and compare the row counts." in body
    assert summary not in body
    assert "1. Should the trailing empty row be kept?" in body
    assert "2. Which delimiter do callers expect?" in body
    assert body.rstrip().endswith(ANSWER_LINE)


def test_a_posted_question_is_redacted_and_a_fence_cannot_swallow_the_answer() -> None:
    secret = "ghp_" + "A1b2C3d4E5" * 3 + "F6g7H8"
    summary = f"Ran it with {secret} and saw:\n```\ntraceback, never closed"
    questions = [f"Should @someone own this?\nToken {secret}\n```\nnever closed"]
    body = factory_questions.compose_body(1, summary, questions)
    assert secret not in body
    assert "[REDACTED" in body
    assert "Ran it with" in body
    assert "traceback, never closed" in body
    assert "@someone" not in body and "@\u200bsomeone" in body
    assert not any(line.lstrip().startswith(("```", "~~~")) for line in body.splitlines())
    assert "\n1. Should @\u200bsomeone own this?\n" in body
    assert body.rstrip().endswith(ANSWER_LINE)


def _ask(job: tr._Run | _JiraCase, summary: Any = SUMMARY, questions: Any = None) -> Any:
    return job.client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": job.capability},
        json={"summary": summary, "questions": QUESTIONS if questions is None else questions},
    )


def _running(admitted: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> tr._Run:
    """An implementation request, started, on the in-memory tracker."""

    job = tr._admit_run(admitted, monkeypatch, number)
    tr._begin(job)
    return job


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


def _marked(job: tr._Run) -> list[str]:
    marker = f"curie-factory-questions:{job.request_id}"
    target = ReplyTarget.on_issue(job.issue)
    return [
        body for _author, body in job.tracker.marked_comments.comments(target) if marker in body
    ]


def _code(response: Any) -> str:
    return str(response.json()["detail"]["code"])


# Criterion 1: the memory tracker ----------------------------------------------------


def test_two_questions_post_one_marked_comment_and_record_round_one(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 801)
    response = _ask(job)
    assert response.status_code == 200, response.text
    assert response.json() == {"round": 1, "rounds_limit": 3}
    expected = _frame(1, SUMMARY, QUESTIONS)
    (body,) = _marked(job)
    assert body.startswith(expected)
    assert "\n1. Should the trailing empty row be kept?\n2. Which delimiter" in body
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["rounds_used"], row["state"]) == (1, "outstanding")
    assert row["answer_after"] is not None
    assert row["asking_request_id"] == job.request_id
    assert row["pending_request_id"] is None
    assert row["comment_id"] is not None
    assert row["body_sha256"] == hashlib.sha256(expected.encode()).hexdigest()
    assert job.tracker.ledger.writes == 1


def test_inputs_at_the_bounds_are_posted(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    job = _running(admitted, monkeypatch, 802)
    questions = ["q" * 1000 for _ in range(9)] + ["first line\nsecond line"]
    response = _ask(job, summary="s" * 600, questions=questions)
    assert response.status_code == 200, response.text
    (body,) = _marked(job)
    # The continuation line sits at the item's content column, inside item 10.
    assert "\n10. first line\n    second line\n\n" in body


def test_model_text_cannot_open_an_html_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 803)
    response = _ask(job, summary="See <!-- curie-status:final -->", questions=["<!-- x -->?"])
    assert response.status_code == 200, response.text
    (body,) = _marked(job)
    head = body.split("\n\n<!-- curie-factory-questions:")[0]
    assert "<!--" not in head


def test_model_text_is_redacted_mention_free_and_cannot_hold_the_frame_in_a_fence(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "ghp_" + "A1b2C3d4E5" * 3 + "F6g7H8"
    job = _running(admitted, monkeypatch, 804)
    response = _ask(
        job,
        summary=f"Ran it with {secret} and saw:\n```\ntraceback, never closed",
        questions=[f"Should @someone own this?\nToken {secret}\n```\nnever closed"],
    )
    assert response.status_code == 200, response.text
    (body,) = _marked(job)
    assert secret not in body
    assert "[REDACTED" in body
    assert "Ran it with" in body
    assert "traceback, never closed" in body
    assert "@someone" not in body and "@\u200bsomeone" in body
    # No line of the composed comment opens a fence, so the numbered questions
    # and the answer line render as the frame, not as code.
    assert not any(line.lstrip().startswith(("```", "~~~")) for line in body.splitlines())
    assert "\n1. Should @\u200bsomeone own this?\n" in body


# Criterion 2: GitHub and Jira Cloud ------------------------------------------------


def test_github_writes_one_comment_through_the_production_resolver(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 811)
    # Undo the in-memory substitution: the WorkItem's own GitHub tracker writes.
    monkeypatch.setattr(completion_contract, "work_item_tracker", _REAL_TRACKER)
    _client, github, _server = admitted
    response = _ask(job)
    assert response.status_code == 200, response.text
    marker = f"curie-factory-questions:{job.request_id}"
    # The GitHub fake behind the app's HTTP client keeps each issue's comments.
    written = [c for c in github.issue_comments.get(811, []) if marker in c["body"]]
    assert len(written) == 1
    assert written[0]["body"].startswith(_frame(1, SUMMARY, QUESTIONS))
    assert written[0]["performed_via_github_app"] is not None
    assert _marked(job) == []
    row = _round_row(job.work_item_id)
    assert row is not None and row["comment_id"] == str(written[0]["id"])


_REAL_TRACKER = completion_contract.work_item_tracker


# The Jira Cloud tracker through the production resolver -------------------------


class _ByHost(httpx.AsyncBaseTransport):
    """The Jira fixture site to the Jira fake, every other host to the GitHub
    stand-in the app already talks to."""

    def __init__(self, jira: httpx.AsyncBaseTransport, default: httpx.AsyncBaseTransport) -> None:
        self._jira = jira
        self._default = default

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        jira_hosts = {jira_fake.SITE_HOST, jira_fake.GATEWAY_HOST}
        transport = self._jira if request.url.host in jira_hosts else self._default
        return await transport.handle_async_request(request)


@dataclass(frozen=True)
class _JiraCase:
    client: Any
    capability: str
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    fake: jira_fake.JiraFake
    issue_id: int


@pytest.fixture
def jira_case(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[_JiraCase]:
    """A running implementation request whose WorkItem is a Jira Cloud issue.

    The install's forge config gains the Jira fixture site and its binding, and
    the app's HTTP client sends that site to the Jira fake. Nothing replaces the
    resolver: the route reaches the production Jira adapter through
    `work_item_tracker` and the binding that serves the issue's site.
    """

    client, github, _server = admitted
    document = json.loads(os.environ["CURIE_FORGES"])
    site = json.loads(forges_env_json(jira_fake.jira_document(), tmp_path / "jira"))
    document["trackers"].extend(site["trackers"])
    document["bindings"].extend(site["bindings"])
    monkeypatch.setenv("CURIE_FORGES", json.dumps(document))
    get_settings.cache_clear()
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue("Export drops a row", "Three rows in, two out.")
    (agent,) = _rows("SELECT id FROM curie.agents")
    work_item_id, request_id = uuid.uuid4(), uuid.uuid4()
    now = datetime.now(UTC)
    tr._exec(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "tracker_display_key, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, code_host_installation_id, agent_id, conversation_id) "
        "VALUES (:id, 'jira_cloud', :site, :cloud, :issue, :key, 'github', 'github.com', "
        ":repository, :repo, 77, :agent, :conversation)",
        {
            "id": work_item_id,
            "site": jira_fake.SITE_HOST,
            "cloud": jira_fake.CLOUD_ID,
            "issue": str(issue.id),
            "key": issue.key,
            "repository": jira_fake.GITHUB_REPO_ID,
            "repo": jira_fake.GITHUB_REPO,
            "agent": agent["id"],
            "conversation": f"questions-{uuid.uuid4().hex}",
        },
    )
    tr._exec(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, started_at, "
        "execution_deadline, execution_attempts, runtime_owner, runtime_epoch, "
        "runtime_heartbeat_expires_at) "
        "VALUES (:id, :item, 1, 'running', :wait, :started, :deadline, 1, "
        "'fixture-runner', 7, :lease)",
        {
            "id": request_id,
            "item": work_item_id,
            "wait": now + timedelta(minutes=5),
            "started": now - timedelta(minutes=1),
            "deadline": now + timedelta(hours=2),
            "lease": now + timedelta(minutes=5),
        },
    )
    minted = client.post(
        tr.MINT_URL,
        headers=tr.WORKER_HEADERS,
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    original = client.app.state.http_client
    injected = httpx.AsyncClient(
        transport=_ByHost(httpx.ASGITransport(app=fake.app), httpx.MockTransport(github.handle))
    )
    client.app.state.http_client = injected
    try:
        yield _JiraCase(
            client=client,
            capability=minted.json()["capability"],
            request_id=request_id,
            work_item_id=work_item_id,
            fake=fake,
            issue_id=issue.id,
        )
    finally:
        client.app.state.http_client = original
        client.portal.call(injected.aclose)


def _nodes(node: Any, kind: str) -> list[dict[str, Any]]:
    """Every ADF node of ``kind`` in ``node``, at any depth."""

    if isinstance(node, list):
        return [found for child in node for found in _nodes(child, kind)]
    if not isinstance(node, dict):
        return []
    own = [node] if node.get("type") == kind else []
    return own + _nodes(node.get("content", []), kind)


def _assert_one_list_and_the_answer_line_last(document: dict[str, Any], items: int) -> None:
    """The rendered Jira comment: one ordered list of ``items`` items, the frame's
    heading as its only heading, and the answer line as its last block."""

    (ordered,) = _nodes(document, "orderedList")
    assert len(ordered["content"]) == items
    assert len(_nodes(document, "heading")) == 1
    last = document["content"][-1]
    assert last["type"] == "paragraph"
    assert jira_fake.flatten(last) == f"{ANSWER_LINE}\n"


def test_jira_writes_one_comment_through_the_production_resolver_with_a_property_marker(
    jira_case: _JiraCase,
) -> None:
    case = jira_case
    writes = case.fake.writes
    response = _ask(case)
    assert response.status_code == 200, response.text
    assert response.json() == {"round": 1, "rounds_limit": 3}
    (comment,) = case.fake.issue(case.issue_id).comments
    marker = f"curie-factory-questions:{case.request_id}"
    # The marker is the comment entity property, never visible text.
    assert comment.properties["curie.factory"] == {
        "marker": marker,
        "body_sha256": hashlib.sha256(_frame(1, SUMMARY, QUESTIONS).encode()).hexdigest(),
    }
    text = jira_fake.flatten(comment.body)
    assert "curie-factory-questions" not in text
    assert "Questions from the factory (round 1 of 3)" in text
    assert comment.author.account_id == jira_fake.BOT_ACCOUNT_ID
    _assert_one_list_and_the_answer_line_last(comment.body, len(QUESTIONS))
    assert case.fake.writes - writes >= 1
    row = _round_row(case.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["comment_id"]) == (
        "outstanding",
        1,
        str(comment.id),
    )


# Questions whose continuation lines open a block on some renderer.
_BLOCK_QUESTIONS = [
    "Which export path is meant?\u2028# Is this a heading on the continuation line?",
    "Keep the trailing row?\n> a quote\n- a bullet\n2. a number\n---\n```\n| a | b |\n<pre>",
    "Which delimiter do callers expect?\r\n===",
]
_LISTED_SUMMARY = "Steps tried:\n1. run the export\n2) count the rows\n<pre>"


def test_jira_keeps_one_ordered_list_when_a_continuation_line_opens_a_block(
    jira_case: _JiraCase,
) -> None:
    response = _ask(jira_case, summary=_LISTED_SUMMARY, questions=_BLOCK_QUESTIONS)
    assert response.status_code == 200, response.text
    (comment,) = jira_case.fake.issue(jira_case.issue_id).comments
    _assert_one_list_and_the_answer_line_last(comment.body, len(_BLOCK_QUESTIONS))
    (ordered,) = _nodes(comment.body, "orderedList")
    first = jira_fake.flatten(ordered["content"][0])
    assert "# Is this a heading on the continuation line?" in first


# The same block openers, each followed by a Unicode space instead of an
# ASCII one, which the Jira renderer also reads as a marker.
_UNICODE_SPACED_QUESTIONS = [
    "Which export path is meant?\n#\u3000Is this a heading?",
    "Keep the trailing row?\n-\u00a0a bullet\n2.\u2003a number\n>\u2009a quote",
    "Which delimiter do callers expect?\n\u3000\u3000-\u00a0indented bullet",
]


def test_jira_keeps_one_ordered_list_when_unicode_spaces_follow_a_block_marker(
    jira_case: _JiraCase,
) -> None:
    summary = "Steps tried:\n1.\u00a0run the export\n2)\u3000count the rows"
    response = _ask(jira_case, summary=summary, questions=_UNICODE_SPACED_QUESTIONS)
    assert response.status_code == 200, response.text
    (comment,) = jira_case.fake.issue(jira_case.issue_id).comments
    _assert_one_list_and_the_answer_line_last(comment.body, len(_UNICODE_SPACED_QUESTIONS))
    assert _nodes(comment.body, "bulletList") == []
    assert _nodes(comment.body, "blockquote") == []
    (ordered,) = _nodes(comment.body, "orderedList")
    assert "# Is this a heading?" in jira_fake.flatten(ordered["content"][0])


def test_a_continuation_line_that_opens_a_block_keeps_one_commonmark_list(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The GitHub surface: CommonMark keeps each question inside its item.
    job = _running(admitted, monkeypatch, 813)
    response = _ask(job, summary=_LISTED_SUMMARY, questions=_BLOCK_QUESTIONS)
    assert response.status_code == 200, response.text
    (body,) = _marked(job)
    tokens = MarkdownIt("commonmark").parse(body)
    kinds = [token.type for token in tokens]
    assert kinds.count("ordered_list_open") == 1
    assert kinds.count("list_item_open") == len(_BLOCK_QUESTIONS)
    assert kinds.count("heading_open") == 1
    assert kinds.count("blockquote_open") == kinds.count("bullet_list_open") == 0
    assert kinds.count("html_block") == 1  # the platform's marker, nothing else
    last = [token for token in tokens if token.type == "inline"][-1]
    assert last.content == ANSWER_LINE


def test_jira_posts_the_largest_ordinary_input_with_the_answer_line_last(
    jira_case: _JiraCase,
) -> None:
    # Liveness: the biggest input inside decision 2's bounds still posts whole.
    prose = "The export writes two rows for three input rows when the last is blank. "
    summary = (prose * 60)[:600].strip()
    questions = [f"{n}: " + (prose * 15)[: 1000 - 4].strip() + "?" for n in range(10)]
    questions[-1] = questions[-1][:900] + "\nSecond line?"
    assert len(summary) <= 600 and all(len(question) <= 1000 for question in questions)
    response = _ask(jira_case, summary=summary, questions=questions)
    assert response.status_code == 200, response.text
    (comment,) = jira_case.fake.issue(jira_case.issue_id).comments
    _assert_one_list_and_the_answer_line_last(comment.body, 10)
    assert "truncated" not in jira_fake.flatten(comment.body)


def test_refused_input_that_redaction_grows_past_the_bound_writes_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Each ``token=x`` (7 characters) is redacted to a 33 character
    # placeholder assignment. The summary stays under 600 characters. The ten
    # questions alone still compose past Jira's 30,000 character cut.
    summary = ("token=x " * 70).strip()
    questions = [("token=x " * 125).strip()] * 10
    assert len(summary) <= 600 and all(len(question) <= 1000 for question in questions)
    assert len(factory_questions.compose_body(1, summary, questions)) > BODY_LIMIT
    job = _running(admitted, monkeypatch, 814)
    refused = _ask(job, summary=summary, questions=questions)
    assert refused.status_code == 409
    assert refused.json()["detail"] == {
        "code": "invalid_questions",
        "message": "the questions are outside the tool's bounds",
    }
    assert job.tracker.ledger.writes == 0
    assert _round_row(job.work_item_id) is None


# Criterion 3: refusals and replay ------------------------------------------------------


def test_replay_of_the_same_body_returns_the_first_answer_with_one_write(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 821)
    first = _ask(job)
    again = _ask(job)
    assert first.status_code == again.status_code == 200
    assert again.json() == first.json() == {"round": 1, "rounds_limit": 3}
    assert job.tracker.ledger.writes == 1
    assert len(_marked(job)) == 1
    row = _round_row(job.work_item_id)
    assert row is not None and (row["rounds_used"], row["version"]) == (1, 2)


def test_concurrent_replays_of_one_ask_post_one_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 823)
    marked = job.tracker.marked_comments
    original_find = marked.find_marked
    lookups: list[str] = []
    second_lookup = asyncio.Event()

    async def racing_find(target: Any, marker: str) -> Any:
        # Hold each marker lookup until a second one starts, as two racing
        # writers would interleave; a lone writer waits out the window.
        lookups.append(marker)
        if len(lookups) >= 2:
            second_lookup.set()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(second_lookup.wait(), timeout=3)
        return await original_find(target, marker)

    monkeypatch.setattr(marked, "find_marked", racing_find)
    original_begin = factory_questions.begin_round
    entered: list[uuid.UUID] = []

    async def counted_begin(*args: Any, **kwargs: Any) -> Any:
        entered.append(kwargs["request_id"])
        return await original_begin(*args, **kwargs)

    monkeypatch.setattr(factory_questions, "begin_round", counted_begin)

    async def both() -> list[httpx.Response]:
        transport = httpx.ASGITransport(app=job.client.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as http:
            calls = [
                http.post(
                    QUESTIONS_URL,
                    headers={"X-Curie-Issue-Read": job.capability},
                    json={"summary": SUMMARY, "questions": QUESTIONS},
                )
                for _ in range(2)
            ]
            return list(await asyncio.gather(*calls))

    first, second = job.client.portal.call(both)
    assert (first.status_code, second.status_code) == (200, 200), (first.text, second.text)
    assert first.json() == second.json() == {"round": 1, "rounds_limit": 3}
    assert entered == [job.request_id, job.request_id]
    assert len(_marked(job)) == 1
    assert job.tracker.ledger.writes == 1
    assert len(lookups) == 1
    row = _round_row(job.work_item_id)
    assert row is not None and (row["state"], row["rounds_used"]) == ("outstanding", 1)


def test_refused_a_different_body_from_the_same_request_is_already_asked(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 822)
    assert _ask(job).status_code == 200
    refused = _ask(job, questions=["A different question?"])
    assert refused.status_code == 409
    assert _code(refused) == "already_asked"
    assert job.tracker.ledger.writes == 1


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
    job = _running(admitted, monkeypatch, 823)
    refused = _ask(job, summary=summary, questions=questions)
    assert refused.status_code == 409
    assert refused.json()["detail"] == {
        "code": "invalid_questions",
        "message": "the questions are outside the tool's bounds",
    }
    assert job.tracker.ledger.writes == 0
    assert _round_row(job.work_item_id) is None


@pytest.mark.parametrize("body", [{"questions": ["Which branch?"]}, {"summary": "Found it."}, {}])
def test_refused_a_missing_field_is_invalid_questions_not_a_422(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    job = _running(admitted, monkeypatch, 826)
    refused = job.client.post(
        QUESTIONS_URL, headers={"X-Curie-Issue-Read": job.capability}, json=body
    )
    assert refused.status_code == 409
    assert _code(refused) == "invalid_questions"
    assert job.tracker.ledger.writes == 0


_SENT = "sent-value-7c1e-never-echoed"
_SENT_NUMBER = "424242424242"


# Valid fields beside a value that is not JSON (RFC 8259), though Python's
# parser takes NaN and the infinities by default.
def _with_constant(value: str) -> str:
    return '{"summary": "' + _SENT + '", "questions": ["Which?"], "x": ' + value + "}"


# Every body that is not one JSON object with the two fields in their shapes.
_MALFORMED_BODIES = {
    "array": json.dumps([_SENT, {"summary": _SENT, "questions": [_SENT]}]),
    "string": json.dumps(_SENT),
    "number": _SENT_NUMBER,
    "null": "null",
    "boolean": "true",
    "invalid json": '{"summary": "' + _SENT,
    "not json": _SENT,
    "empty": "",
    "summary object": json.dumps({"summary": {"text": _SENT}, "questions": [_SENT]}),
    "questions string": json.dumps({"summary": _SENT, "questions": _SENT}),
    "question object": json.dumps({"summary": _SENT, "questions": [{"text": _SENT}]}),
    "question number": json.dumps({"summary": _SENT, "questions": [int(_SENT_NUMBER)]}),
    "oversized": json.dumps({"summary": _SENT * 20_000, "questions": [_SENT]}),
    "nan beside the fields": _with_constant("NaN"),
    "infinity beside the fields": _with_constant("Infinity"),
    "negative infinity in a list": _with_constant("[-Infinity]"),
    # A number too large for a float decodes to an infinity.
    "overflowing number beside the fields": _with_constant("1e999"),
    "negative overflowing number nested": _with_constant('{"y": [-1e999]}'),
    "overflowing number at the top level": "1e999",
}


def _post_raw(client: Any, capability: str, raw: str) -> Any:
    return client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": capability, "Content-Type": "application/json"},
        content=raw.encode(),
    )


@pytest.mark.parametrize("raw", list(_MALFORMED_BODIES.values()), ids=list(_MALFORMED_BODIES))
def test_refused_a_malformed_body_is_invalid_questions_echoing_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    job = _running(admitted, monkeypatch, 831)
    refused = _post_raw(job.client, job.capability, raw)
    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"] == {
        "code": "invalid_questions",
        "message": "the questions are outside the tool's bounds",
    }
    assert _SENT not in refused.text and _SENT_NUMBER not in refused.text
    assert job.tracker.ledger.writes == 0
    assert _round_row(job.work_item_id) is None


@pytest.mark.parametrize("raw", list(_MALFORMED_BODIES.values()), ids=list(_MALFORMED_BODIES))
def test_refused_a_malformed_body_checks_the_capability_first(admitted: Any, raw: str) -> None:
    client, _github, _server = admitted
    refused = _post_raw(client, "wir.not-a-capability", raw)
    assert refused.status_code == 401, refused.text
    assert refused.json()["detail"]["code"] == "invalid_capability"
    assert _SENT not in refused.text and _SENT_NUMBER not in refused.text


# A blank line spelled with another line break form, or kept from looking
# blank only by an invisible character, is a blank line once posted.
_SEPARATED_BLANK_LINES = {
    "line separator": "First\u2028\u2028second?",
    "paragraph separators": "First\u2029 \u2029second?",
    "next line": "First\x85\x85second?",
    "lone carriage returns": "First\r\rsecond?",
    "vertical tabs": "First\x0b\x0bsecond?",
    "form feed line": "First\n\x0c\nsecond?",
    "zero width line": "First\n\u200b\nsecond?",
}


@pytest.mark.parametrize(
    "question", list(_SEPARATED_BLANK_LINES.values()), ids=list(_SEPARATED_BLANK_LINES)
)
def test_refused_a_blank_line_of_any_line_break_form(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, question: str
) -> None:
    job = _running(admitted, monkeypatch, 832)
    refused = _ask(job, questions=[question])
    assert (refused.status_code, _code(refused)) == (409, "invalid_questions")
    assert job.tracker.ledger.writes == 0
    assert _round_row(job.work_item_id) is None


def test_a_question_with_one_crlf_line_break_is_accepted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The runner accepts a single CRLF; the route must not read it as a blank line.
    job = _running(admitted, monkeypatch, 827)
    asked = _ask(job, questions=["first line\r\nsecond line?"])
    assert asked.status_code == 200
    assert asked.json() == {"round": 1, "rounds_limit": 3}


def test_refused_a_request_that_is_not_running(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = tr._admit_run(admitted, monkeypatch, 825)
    refused = _ask(job)
    assert (refused.status_code, _code(refused)) == (409, "request_not_running")
    assert _round_row(job.work_item_id) is None


def _cancel(job: tr._Run) -> None:
    (row,) = _rows("SELECT version FROM curie.work_items WHERE id = :id", {"id": job.work_item_id})

    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            result = await lifecycle.request_cancellation(
                session,
                work_item_id=job.work_item_id,
                expected_work_item_version=row["version"],
            )
            await session.commit()
            return result

    assert isinstance(job.client.portal.call(go), lifecycle.WorkItemOutcome)


def test_refused_a_cancelled_work_item(admitted: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    job = _running(admitted, monkeypatch, 826)
    _cancel(job)
    refused = _ask(job)
    assert (refused.status_code, _code(refused)) == (409, "work_item_cancelled")
    assert job.tracker.ledger.writes == 0


def _with_session(job: tr._Run, work: Any) -> Any:
    async def go() -> Any:
        async with job.client.app.state.sessionmaker() as session:
            return await work(session)

    return job.client.portal.call(go)


def test_refused_while_the_work_item_has_an_open_pull_request(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 827)
    (item,) = _rows(
        "SELECT agent_id, conversation_id FROM curie.work_items WHERE id = :id",
        {"id": job.work_item_id},
    )

    async def open_lineage(session: Any) -> uuid.UUID:
        lineage = await wi._lineage(
            session, item["agent_id"], conversation=item["conversation_id"], status="open"
        )
        await session.commit()
        return lineage

    lineage_id = _with_session(job, open_lineage)
    tr._exec(
        "UPDATE curie.work_items SET publication_lineage_id = :lineage WHERE id = :id",
        {"lineage": lineage_id, "id": job.work_item_id},
    )
    refused = _ask(job)
    assert (refused.status_code, _code(refused)) == (409, "pull_request_open")
    # A closed lineage is no open pull request: the same request may ask.
    tr._exec(
        "UPDATE curie.thread_publication_lineages SET status = 'closed' WHERE id = :id",
        {"id": lineage_id},
    )
    assert _ask(job).status_code == 200


def test_refused_after_the_request_published(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 828)

    async def publish(session: Any) -> None:
        await wi._attach_own_publication(session, job.work_item_id, job.request_id, status="failed")

    _with_session(job, publish)
    refused = _ask(job)
    assert (refused.status_code, _code(refused)) == (409, "already_published")
    assert _round_row(job.work_item_id) is None


def test_refused_past_three_rounds_marks_the_row_exhausted(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 829)
    tr._exec(
        "INSERT INTO curie.factory_question_rounds "
        "(work_item_id, rounds_used, state, asking_request_id, body_sha256) "
        "VALUES (:id, 3, 'cleared', :asker, :digest)",
        {"id": job.work_item_id, "asker": uuid.uuid4(), "digest": "cd" * 32},
    )
    tr._exec(
        "INSERT INTO curie.factory_question_round_counts (work_item_id, rounds_used) "
        "VALUES (:id, 3)",
        {"id": job.work_item_id},
    )
    refused = _ask(job)
    assert (refused.status_code, _code(refused)) == (409, "question_rounds_exhausted")
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["asking_request_id"]) == (
        "exhausted",
        3,
        job.request_id,
    )
    again = _ask(job)
    assert (again.status_code, _code(again)) == (409, "question_rounds_exhausted")
    assert job.tracker.ledger.writes == 0


def test_refused_a_capability_for_another_issue(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 830)
    refused = job.client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": "wir.not-a-capability"},
        json={"summary": SUMMARY, "questions": QUESTIONS},
    )
    assert refused.status_code == 401
    assert _round_row(job.work_item_id) is None


# Criterion 4: the tracker is unavailable ----------------------------------------------


def test_tracker_unavailable_leaves_the_round_posting_and_the_request_settles_as_today(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 841)
    job.tracker.write_faults.abort_before_next(Operation.UPSERT_MARKED, Unavailable("tracker down"))
    refused = _ask(job)
    assert refused.status_code == 503
    assert _code(refused) == "tracker_unavailable"
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"], row["comment_id"]) == ("posting", 0, None)
    assert _marked(job) == []

    tr._finish(job, "failed", "no_pull_request")

    assert _request(job.request_id) == {"status": "failed", "terminal_cause": "no_pull_request"}
    row = _round_row(job.work_item_id)
    assert row is not None
    assert (row["state"], row["rounds_used"]) == ("cleared", 0)


def test_a_retry_after_an_unavailable_tracker_posts_the_round(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _running(admitted, monkeypatch, 842)
    job.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    lost = _ask(job)
    assert (lost.status_code, _code(lost)) == (503, "tracker_unavailable")
    # The comment landed but its answer was lost; the retry edits it in place.
    retried = _ask(job)
    assert retried.status_code == 200, retried.text
    assert retried.json() == {"round": 1, "rounds_limit": 3}
    assert len(_marked(job)) == 1
    row = _round_row(job.work_item_id)
    assert row is not None and (row["state"], row["rounds_used"]) == ("outstanding", 1)
