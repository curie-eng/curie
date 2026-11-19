"""A plain answer comment on a GitHub issue restarts the factory (#5787).

Every case drives a production door: a signed ``POST /github/webhook``
delivery, or a real poll pass (`factory_poll_intake.poll_once`) over the
GitHub stand-in. The question round is reached the production way: the issue
is admitted by a signed label webhook, its request acquired and started as the
worker does, and the questions are asked over HTTP through
``POST /work-items/questions`` with the minted issue read capability. The
question comment is written by the real `GitHubTracker` marked-comment write
into `forge_fakes.github.IssueCommentsAPI`, so it has a real numeric id and a
whole-second ``created_at``, exactly as an answer comment does. Settlement is
the worker's finish dispatch path. Nothing in the code under test is
substituted; GitHub is faked only at its HTTP seam.

GitHub fields these tests rely on:
1. ``issue_comment`` actions (``created``, ``edited``) and the comment's
   ``user`` and ``performed_via_github_app`` in the webhook payload:
   https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
2. The comment as the App reads it (``user.type`` ``User`` or ``Bot``,
   ``performed_via_github_app``, ``issue_url``, ``created_at``):
   https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
3. The repository comment listing a poll pass reads, ``since`` on
   ``updated_at``: https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
4. The issue's ``state`` and ``labels`` read fresh at admission:
   https://docs.github.com/en/rest/issues/issues#get-an-issue
5. The author's effective permission by immutable user id:
   https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import json
import logging
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_factory_tracker_results as tr
from curie_api import factory_questions
from curie_api.config import get_settings
from curie_api.factory_facts_schema import REQUEST_MARKER
from curie_api.factory_poll_intake import poll_once
from curie_api.forges.types import TrackerIssueRef
from curie_api.github_factory_events import FactoryNotice
from curie_api.main import create_app
from curie_api.workitem_dispatch import DispatchConflict, acquire
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from forge_fakes import factory_wake
from forge_fakes.github import (
    APP_BOT_USER,
    APP_ID,
    APP_SLUG,
    INSTALLATION_ID,
    LABEL,
    MENTION,
    REPO,
    REPO_ID,
    IssueCommentsAPI,
    PlantedIssue,
    _issue_event,
    _post,
    _signature,
    fake_installation_tokens,
)
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    comments,
)
from redis.asyncio import Redis
from test_factory_question_ending import _ask, _request, _requests, _round_row, _take_over

PATHS = ("webhook", "poll")
TOKEN = "ghs_answer_intake_fixture"
ANSWER = "Replace the rows on a re-run."
REFUSED_PREFIX = "factory answer refused"

WRITER = {"id": 7101, "login": "writer-user", "type": "User"}
MAINTAINER = {"id": 7102, "login": "maintainer-user", "type": "User"}
READER = {"id": 7103, "login": "reader-user", "type": "User"}
HELPER_BOT = {"id": 7104, "login": "helper-tool[bot]", "type": "Bot"}
# The App installed again on the repository under a new installation id.
REINSTALLED = INSTALLATION_ID + 1
# The factory's own App, and another App a person may act through.
OWN_APP = {"id": int(APP_ID), "slug": APP_SLUG}
OTHER_APP = {"id": 8001, "slug": "other-tool"}


class _Credentials:
    """The App's installation token for the fixture repository, on every path."""

    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        if repo != REPO or installation_id not in (INSTALLATION_ID, REINSTALLED):
            raise RuntimeError("unexpected installation")
        return TOKEN

    def token_for(self, repo: str) -> str:
        if repo != REPO:
            raise RuntimeError("unexpected repository")
        return TOKEN

    def fresh_installation_token(
        self, repo: str, installation_id: int | None = None
    ) -> tuple[int, str]:
        if repo != REPO or installation_id not in (None, INSTALLATION_ID):
            raise RuntimeError("unexpected installation")
        return INSTALLATION_ID, TOKEN


@dataclass
class World:
    client: TestClient
    github: IssueCommentsAPI
    sink: Any


@pytest.fixture
def world(comments: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[World]:
    """The API with the GitHub stand-in behind its HTTP client.

    The question write, the webhook's comment read and a poll pass all reach
    `IssueCommentsAPI`; status comments and state labels go to the threaded
    ``comments`` server, as in the other factory suites.
    """

    fake_installation_tokens(monkeypatch, _Credentials())
    host, port = comments.server_address
    github = IssueCommentsAPI(api=f"http://{host}:{port}")
    github.grant(WRITER, "write")
    github.grant(MAINTAINER, "maintain")
    github.grant(READER, "read")
    with TestClient(create_app()) as client:
        external = httpx.AsyncClient(transport=httpx.MockTransport(github.handle_async))
        client.app.state.http_client = external
        created = client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert created.status_code == 201, created.text
        yield World(client, github, comments)
        client.portal.call(external.aclose)


# The question round, the production way ------------------------------------------


def _running(world: World, number: int) -> tr._Triage:
    """A labelled issue admitted by webhook, its request acquired and started."""

    world.sink.issue_labels[number] = {LABEL}
    tr._label(world.client, world.github, number)
    (row,) = _rows(
        "SELECT r.id AS request_id, w.id AS work_item_id, w.tracker_kind, w.tracker_host, "
        "w.tracker_scope_id, w.tracker_issue_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )
    request_id = row["request_id"]

    async def go() -> Any:
        async with world.client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=world.client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            return acquired.context

    context = world.client.portal.call(go)
    minted = world.client.post(
        tr.MINT_URL,
        headers=tr.WORKER_HEADERS,
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    triage = tr._Triage(
        client=world.client,
        request_id=request_id,
        work_item_id=row["work_item_id"],
        # Unused here: this suite writes through the real GitHub tracker.
        tracker=None,  # type: ignore[arg-type]
        issue=TrackerIssueRef(
            row["tracker_kind"], row["tracker_host"], row["tracker_scope_id"], str(number)
        ),
        capability=minted.json()["capability"],
        context=context,
    )
    tr._begin(triage)
    return triage


def _question(world: World, number: int) -> dict[str, Any]:
    """The factory's one question comment on the issue, as GitHub stores it."""

    (question,) = [c for c in world.github.on_issue(number) if c["user"] == APP_BOT_USER]
    return question


def _settle(triage: tr._Triage) -> None:
    """The asking request ends without a pull request: the round is outstanding."""

    tr._finish(triage, "failed", "no_pull_request")
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "outstanding", row


def _outstanding(world: World, number: int) -> tr._Triage:
    triage = _running(world, number)
    response = _ask(triage)
    assert response.status_code == 200, response.text
    row = _round_row(triage.work_item_id)
    assert row is not None and row["comment_id"] == str(_question(world, number)["id"])
    _settle(triage)
    return triage


# Delivery through each door ---------------------------------------------------------


def _payload(comment: dict[str, Any], number: int, action: str = "created") -> dict[str, Any]:
    return _issue_event(action, number, comment=dict(comment), sender=dict(comment["user"]))


def _poll(world: World) -> None:
    client = world.client

    async def go() -> None:
        await poll_once(client.app.state.sessionmaker, get_settings(), client.app.state.http_client)

    client.portal.call(go)


def _deliver(
    world: World, path: str, comment: dict[str, Any], number: int, action: str = "created"
) -> dict[str, Any] | None:
    """The webhook's JSON answer, or None after a poll pass."""

    if path == "poll":
        _poll(world)
        return None
    response = _post(world.client, "issue_comment", _payload(comment, number, action))
    assert response.status_code == 200, response.text
    result: dict[str, Any] = response.json()
    return result


def _code(result: dict[str, Any] | None) -> str | None:
    if result is None or not result.get("errors"):
        return None
    return str(result["errors"][0]["code"])


def _ids(triage: tr._Triage) -> list[uuid.UUID]:
    return [row["id"] for row in _requests(triage.work_item_id)]


def _answer_id(triage: tr._Triage, comment: dict[str, Any]) -> uuid.UUID:
    return factory_questions.answer_request_id(triage.issue, str(comment["id"]))


def _receipts(triage: tr._Triage) -> dict[str, tuple[str, uuid.UUID | None]]:
    rows = _rows(
        "SELECT comment_key, outcome, request_id FROM curie.factory_answer_receipts "
        "WHERE work_item_id = :id",
        {"id": triage.work_item_id},
    )
    return {row["comment_key"]: (row["outcome"], row["request_id"]) for row in rows}


def _refusals(caplog: pytest.LogCaptureFixture, comment: dict[str, Any]) -> set[str]:
    """The reasons the answer intake recorded for ``comment``."""

    return {
        str(record.args[0])
        for record in caplog.records
        if record.getMessage().startswith(REFUSED_PREFIX)
        and isinstance(record.args, tuple)
        and record.args[2] == comment["id"]
    }


def _admits(
    world: World,
    path: str,
    triage: tr._Triage,
    number: int,
    user: dict[str, Any] = WRITER,
    app: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A new plain answer by ``user`` admits exactly one request."""

    before = _ids(triage)
    comment = world.github.add_comment(number, ANSWER, user=user, app=app)
    result = _deliver(world, path, comment, number)
    if result is not None:
        assert result["status"] == "factory_admitted", result
    assert _ids(triage) == [*before, _answer_id(triage, comment)]
    return comment


# Criterion 1 and 2: one plain answer, by either door, is one request ----------------


def test_webhook_admits_a_plain_answer(world: World) -> None:
    triage = _outstanding(world, 6101)
    comment = world.github.add_comment(6101, ANSWER, user=WRITER)

    result = _deliver(world, "webhook", comment, 6101)

    assert result is not None and result["status"] == "factory_admitted", result
    expected = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, expected]
    (created,) = _rows(
        "SELECT work_item_id, status, requester FROM curie.execution_requests WHERE id = :id",
        {"id": expected},
    )
    assert created == {
        "work_item_id": triage.work_item_id,
        "status": "waiting",
        "requester": f"github:{WRITER['id']}:{WRITER['login']}",
    }
    assert _receipts(triage) == {str(comment["id"]): ("admitted", expected)}


def test_poll_admits_a_plain_answer(world: World) -> None:
    triage = _outstanding(world, 6102)
    comment = world.github.add_comment(6102, ANSWER, user=WRITER)

    _poll(world)
    _poll(world)

    expected = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, expected]
    assert _request(expected)["status"] == "waiting"
    assert _receipts(triage) == {str(comment["id"]): ("admitted", expected)}


@pytest.mark.parametrize("first", PATHS)
def test_dedupe_one_comment_by_both_doors(world: World, first: str) -> None:
    number = 6103 if first == "webhook" else 6104
    triage = _outstanding(world, number)
    comment = world.github.add_comment(number, ANSWER, user=WRITER)

    _deliver(world, first, comment, number)
    second = _deliver(world, "poll" if first == "webhook" else "webhook", comment, number)

    if second is not None:
        assert second["status"] == "factory_duplicate", second
    expected = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, expected]
    assert _receipts(triage) == {str(comment["id"]): ("admitted", expected)}


# Criterion 3: who may answer --------------------------------------------------------

AUTHOR_REFUSALS: dict[str, tuple[dict[str, Any], dict[str, Any] | None, str]] = {
    "read_permission": (READER, None, "answer_not_authorized"),
    "bot_sender": (HELPER_BOT, None, "answer_from_bot"),
    "factory_app": (APP_BOT_USER, OWN_APP, "answer_from_bot"),
    "own_app_user": (WRITER, OWN_APP, "answer_from_bot"),
}


@pytest.mark.parametrize("case", sorted(AUTHOR_REFUSALS))
@pytest.mark.parametrize("path", PATHS)
def test_refused_author(
    world: World, caplog: pytest.LogCaptureFixture, path: str, case: str
) -> None:
    caplog.set_level(logging.INFO)
    number = 6110 + sorted(AUTHOR_REFUSALS).index(case) + (0 if path == "webhook" else 5)
    triage = _outstanding(world, number)
    user, app, reason = AUTHOR_REFUSALS[case]
    refused = world.github.add_comment(number, ANSWER, user=user, app=app)

    result = _deliver(world, path, refused, number)

    if result is not None:
        assert _code(result) == reason, result
    assert _refusals(caplog, refused) == {reason}
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}
    # The question is still open to a person who may answer it.
    _admits(world, path, triage, number)


@pytest.mark.parametrize("path", PATHS)
def test_refused_edited_comment(world: World, path: str) -> None:
    number = 6120 if path == "webhook" else 6121
    triage = _running(world, number)
    earlier = world.github.add_comment(number, "Draft note.", user=WRITER)
    world.github.tick()
    assert _ask(triage).status_code == 200
    _settle(triage)
    world.github.tick()
    edited = world.github.edit_comment(earlier["id"], ANSWER)

    # An edit is not a new comment: the webhook's edit is unsupported, and a
    # poll pass sees only a newer ``updated_at`` on a comment older than the
    # question.
    result = _deliver(world, path, edited, number, action="edited")

    if result is not None:
        assert _code(result) == "unsupported_action", result
    else:
        assert _receipts(triage) == {str(edited["id"]): ("not_after_question", None)}
    assert _ids(triage) == [triage.request_id]
    _admits(world, path, triage, number)


@pytest.mark.parametrize("path", PATHS)
def test_refused_without_trigger_label(
    world: World, caplog: pytest.LogCaptureFixture, path: str
) -> None:
    caplog.set_level(logging.INFO)
    number = 6122 if path == "webhook" else 6123
    triage = _outstanding(world, number)
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    # The label is gone on GitHub; no unlabel event has been processed.
    world.github.planted_issues[number] = PlantedIssue(labels=())

    result = _deliver(world, path, comment, number)

    if result is not None:
        assert _code(result) == "trigger_label_absent", result
    assert _refusals(caplog, comment) == {"trigger_label_absent"}
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


@pytest.mark.parametrize("path", PATHS)
def test_refused_on_cancelled_work_item(world: World, path: str) -> None:
    number = 6124 if path == "webhook" else 6125
    triage = _outstanding(world, number)
    first = _admits(world, path, triage, number)
    world.github.planted_issues[number] = PlantedIssue(labels=())
    unlabeled = _post(
        world.client, "issues", _issue_event("unlabeled", number, label={"name": LABEL})
    )
    assert unlabeled.json()["status"] in {
        "factory_cancelled",
        "factory_cancellation_requested",
    }, unlabeled.text
    # Relabelled on GitHub without an admission: only the cancellation refuses.
    del world.github.planted_issues[number]
    requests = _ids(triage)
    comment = world.github.add_comment(number, ANSWER, user=WRITER)

    result = _deliver(world, path, comment, number)

    if result is not None:
        assert _code(result) == "ordinary_comment", result
    assert _ids(triage) == requests
    assert _round_row(triage.work_item_id) is None
    assert set(_receipts(triage)) == {str(first["id"])}


@pytest.mark.parametrize("path", PATHS)
def test_refused_without_open_question(world: World, path: str) -> None:
    number = 6126 if path == "webhook" else 6127
    triage = _running(world, number)
    early = world.github.add_comment(number, ANSWER, user=WRITER)

    result = _deliver(world, path, early, number)

    if result is not None:
        assert _code(result) == "ordinary_comment", result
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}
    world.github.tick()
    assert _ask(triage).status_code == 200
    _settle(triage)
    _admits(world, path, triage, number)
    assert _answer_id(triage, early) not in _ids(triage)


ACCEPTED = {
    "maintainer": (MAINTAINER, None),
    "other_app_writer": (WRITER, OTHER_APP),
}


@pytest.mark.parametrize("case", sorted(ACCEPTED))
@pytest.mark.parametrize("path", PATHS)
def test_accepted_author(world: World, path: str, case: str) -> None:
    number = 6130 + sorted(ACCEPTED).index(case) + (0 if path == "webhook" else 2)
    triage = _outstanding(world, number)
    user, app = ACCEPTED[case]

    comment = _admits(world, path, triage, number, user=user, app=app)

    (created,) = _rows(
        "SELECT requester FROM curie.execution_requests WHERE id = :id",
        {"id": _answer_id(triage, comment)},
    )
    assert created["requester"] == f"github:{user['id']}:{user['login']}"


# Criterion 4: the ticket closed before the comment was delivered --------------------


@pytest.mark.parametrize("path", PATHS)
def test_closed_before_delivery(world: World, caplog: pytest.LogCaptureFixture, path: str) -> None:
    caplog.set_level(logging.INFO)
    number = 6140 if path == "webhook" else 6141
    triage = _outstanding(world, number)
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    # Closed on GitHub after the comment; its close event is not processed.
    world.github.planted_issues[number] = PlantedIssue(state="closed", labels=(LABEL,))

    result = _deliver(world, path, comment, number)

    if result is not None:
        assert _code(result) == "ticket_closed", result
    assert _refusals(caplog, comment) == {"ticket_closed"}
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


# Criterion 5: several answers fold into one attempt, or one follow-up ----------------


@pytest.mark.parametrize("path", PATHS)
def test_coalesce_two_answers_before_the_start(world: World, path: str) -> None:
    number = 6150 if path == "webhook" else 6151
    triage = _outstanding(world, number)
    first = _admits(world, path, triage, number)
    second = world.github.add_comment(number, "Also keep the header row.", user=MAINTAINER)

    result = _deliver(world, path, second, number)

    if result is not None:
        assert result["status"] == "factory_coalesced", result
    started = _answer_id(triage, first)
    assert _ids(triage) == [triage.request_id, started]
    assert _request(started)["status"] == "waiting"
    assert _receipts(triage) == {
        str(first["id"]): ("admitted", started),
        str(second["id"]): ("coalesced", None),
    }


@pytest.mark.parametrize("path", PATHS)
def test_follow_up_queues_behind_the_running_answer(world: World, path: str) -> None:
    number = 6152 if path == "webhook" else 6153
    triage = _outstanding(world, number)
    first = _admits(world, path, triage, number)
    running = _answer_id(triage, first)
    _take_over(triage, running)
    assert _request(running)["status"] == "running"
    third = world.github.add_comment(number, "One more detail.", user=WRITER)

    result = _deliver(world, path, third, number)

    if result is not None:
        assert result["status"] == "factory_queued", result
    follow_up = _answer_id(triage, third)
    assert _ids(triage) == [triage.request_id, running, follow_up]
    assert _request(running)["status"] == "running"
    assert _request(follow_up)["status"] == "queued"
    assert _receipts(triage)[str(third["id"])] == ("queued_follow_up", follow_up)


# Criterion 6: a mention is an answer while a question is open; facts keep theirs ------


def _mention_id(number: int, comment: dict[str, Any]) -> uuid.UUID:
    """The request id a mention revision of ``comment`` by the writer would take."""

    return FactoryNotice(
        uuid.uuid4(),
        "issue_comment",
        "created",
        "mention",
        INSTALLATION_ID,
        REPO_ID,
        REPO,
        number,
        WRITER["id"],
        WRITER["login"],
        label_event_at=None,
        comment_id=comment["id"],
    ).request_id


def _facts_body() -> str:
    """A facts request `parse_request` accepts."""

    query = {
        "schema_version": 1,
        "trigger_label": LABEL,
        "executables": ["uv", "python3.13"],
        "services": ["postgres"],
    }
    return f"@{APP_SLUG}\n\n{REQUEST_MARKER}\n```json\n{json.dumps(query)}\n```"


@pytest.mark.parametrize("path", PATHS)
def test_mention_while_outstanding_is_one_answer(world: World, path: str) -> None:
    number = 6160 if path == "webhook" else 6161
    triage = _outstanding(world, number)
    comment = world.github.add_comment(
        number, f"@{MENTION} replace the rows on a re-run.", user=WRITER
    )

    result = _deliver(world, path, comment, number)

    if result is not None:
        assert result["status"] == "factory_admitted", result
    assert _ids(triage) == [triage.request_id, _answer_id(triage, comment)]
    assert _mention_id(number, comment) not in _ids(triage)


@pytest.mark.parametrize("path", PATHS)
def test_facts_request_comment_is_never_an_answer(world: World, path: str) -> None:
    number = 6162 if path == "webhook" else 6163
    triage = _outstanding(world, number)
    request = world.github.add_comment(number, _facts_body(), user=WRITER)

    result = _deliver(world, path, request, number)

    if result is not None:
        # The facts exchange answers it before the factory intake sees it.
        assert result["status"] == "ignored", result
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}
    _admits(world, path, triage, number)
    assert str(request["id"]) not in _receipts(triage)


# Criterion 7: a refusal is recorded by reason and identity only ------------------------


@pytest.mark.parametrize("path", PATHS)
def test_log_records_carry_codes_only(
    world: World, caplog: pytest.LogCaptureFixture, path: str
) -> None:
    caplog.set_level(logging.INFO)
    number = 6170 if path == "webhook" else 6171
    triage = _outstanding(world, number)
    sentinel = "SENTINEL-answer-body-7c1f: replace the rows on a re-run."
    refused = world.github.add_comment(number, sentinel, user=READER)

    _deliver(world, path, refused, number)

    assert _refusals(caplog, refused) == {"answer_not_authorized"}
    (record,) = [
        r
        for r in caplog.records
        if r.getMessage().startswith(REFUSED_PREFIX) and r.args and r.args[2] == refused["id"]
    ]
    assert record.args == (
        "answer_not_authorized",
        triage.work_item_id,
        refused["id"],
        READER["id"],
    )
    for captured in caplog.records:
        rendered = f"{captured.getMessage()} {captured.args!r} {captured.exc_text or ''}"
        assert "SENTINEL-answer-body" not in rendered
        assert TOKEN not in rendered
    assert _ids(triage) == [triage.request_id]


# The fence on GitHub's own ids (coordinator X1) ----------------------------------------


@pytest.mark.parametrize("path", PATHS)
def test_immediate_answer_in_the_question_second(world: World, path: str) -> None:
    number = 6180 if path == "webhook" else 6181
    triage = _running(world, number)
    assert _ask(triage).status_code == 200
    question = _question(world, number)
    # Same clock second as the question, the very next comment id.
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    assert comment["created_at"] == question["created_at"]
    assert comment["id"] == question["id"] + 1
    _settle(triage)

    result = _deliver(world, path, comment, number)

    if result is not None:
        assert result["status"] == "factory_admitted", result
    assert _ids(triage) == [triage.request_id, _answer_id(triage, comment)]


@pytest.mark.parametrize("path", PATHS)
def test_late_delivery_of_an_earlier_comment_admits_nothing(world: World, path: str) -> None:
    number = 6182 if path == "webhook" else 6183
    triage = _running(world, number)
    earlier = world.github.add_comment(number, ANSWER, user=WRITER)
    world.github.tick()
    assert _ask(triage).status_code == 200
    assert earlier["id"] < _question(world, number)["id"]
    _settle(triage)

    result = _deliver(world, path, earlier, number)

    if result is not None:
        assert _code(result) == "not_after_question", result
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {str(earlier["id"]): ("not_after_question", None)}


# The question write landed but its round is still posting (coordinator X2) -------------


def _retry_keys() -> set[str]:
    keys: set[str] = set()
    for row in _rows("SELECT etags FROM curie.factory_poll_cursors"):
        keys.update(key for key in (row["etags"] or {}) if key.startswith("answer-retry:"))
    return keys


@pytest.mark.parametrize("path", PATHS)
def test_posting_window_answer_is_admitted_once(world: World, path: str) -> None:
    number = 6190 if path == "webhook" else 6191
    triage = _running(world, number)
    # GitHub stores the question comment, but its answer to the write is lost.
    world.github.store_then_fail = 502
    assert _ask(triage).status_code == 503
    question = _question(world, number)
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "posting"
    world.github.tick()
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    assert comment["id"] > question["id"]
    delivery = str(uuid.uuid4())
    payload = _payload(comment, number)

    if path == "webhook":
        deferred = _post(world.client, "issue_comment", payload, delivery=delivery)
        assert deferred.status_code == 503, deferred.text
        assert deferred.json()["detail"] == {"code": "question_posting"}
    else:
        _poll(world)
        assert _retry_keys() == {f"answer-retry:{comment['id']}"}
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}

    # The runner retries its ask: the marked write finds the comment it posted.
    assert _ask(triage).status_code == 200
    assert _question(world, number) == question
    row = _round_row(triage.work_item_id)
    assert row is not None
    assert (row["state"], row["comment_id"]) == ("outstanding", str(question["id"]))
    _settle(triage)

    if path == "webhook":
        redelivered = _post(world.client, "issue_comment", payload, delivery=delivery)
        assert redelivered.status_code == 200, redelivered.text
        assert redelivered.json()["status"] == "factory_admitted", redelivered.text
    else:
        _poll(world)
        assert _retry_keys() == set()
    expected = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, expected]
    assert _receipts(triage) == {str(comment["id"]): ("admitted", expected)}


def _reconciler_tick(world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    """One `WorkItemReconciler` tick, its scheduled entry point, with the
    clients it opens behind the GitHub stand-in."""

    github = world.github
    original = httpx.AsyncClient

    class Client(original):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(github.handle_async)
            super().__init__(*args, **kwargs)

    async def go() -> None:
        valkey = Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
        try:
            await WorkItemReconciler(
                world.client.app.state.sessionmaker, valkey, get_settings()
            ).run_once()
        finally:
            await valkey.aclose()

    with monkeypatch.context() as patched:
        patched.setattr(httpx, "AsyncClient", Client)
        world.client.portal.call(go)


def test_posting_window_answer_is_admitted_by_the_reconciler_tick(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Webhook mode: GitHub never redelivers the 503 by itself, so the webhook
    keeps the comment in its scope's cursor and every reconciler tick reads
    it again, even with the missed-label sweep switched off."""

    monkeypatch.setenv("FACTORY_RECONCILE_INTERVAL_S", "0")
    get_settings.cache_clear()
    settings = get_settings()
    assert (settings.factory_intake, settings.factory_reconcile_interval_s) == ("webhook", 0)
    number = 6192
    triage = _running(world, number)
    world.github.store_then_fail = 502
    assert _ask(triage).status_code == 503
    world.github.tick()
    comment = world.github.add_comment(number, ANSWER, user=WRITER)

    deferred = _post(world.client, "issue_comment", _payload(comment, number))

    assert deferred.status_code == 503, deferred.text
    assert _retry_keys() == {f"answer-retry:{comment['id']}"}
    assert _ask(triage).status_code == 200
    _settle(triage)
    assert _ids(triage) == [triage.request_id]

    _reconciler_tick(world, monkeypatch)

    expected = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, expected]
    assert _receipts(triage) == {str(comment["id"]): ("admitted", expected)}
    assert _retry_keys() == set()

    _reconciler_tick(world, monkeypatch)

    assert _ids(triage) == [triage.request_id, expected]
    assert _retry_keys() == set()


def test_posting_window_webhook_key_survives_a_concurrent_poll_pass(world: World) -> None:
    """A webhook records its key after a poll pass loaded its cursor; the
    pass's cursor save keeps it, and the next pass's drain admits the answer."""

    number = 6193
    triage = _running(world, number)
    world.github.store_then_fail = 502
    assert _ask(triage).status_code == 503
    world.github.tick()
    answered: list[dict[str, Any]] = []
    app = world.client.app

    async def answer_mid_pass() -> None:
        # Posted after the pass's listing was answered, so the pass never
        # lists it; delivered while the pass still holds its cursor.
        comment = world.github.add_comment(number, ANSWER, user=WRITER)
        answered.append(comment)
        body = json.dumps(_payload(comment, number)).encode()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as webhook:
            response = await webhook.post(
                "/github/webhook",
                content=body,
                headers={
                    "X-GitHub-Delivery": str(uuid.uuid4()),
                    "X-GitHub-Event": "issue_comment",
                    "X-Hub-Signature-256": _signature(body),
                    "Content-Type": "application/json",
                },
            )
        assert response.status_code == 503, response.text

    world.github.after_listing = answer_mid_pass

    _poll(world)

    (comment,) = answered
    assert _retry_keys() == {f"answer-retry:{comment['id']}"}
    assert _ids(triage) == [triage.request_id]
    assert _ask(triage).status_code == 200
    _settle(triage)

    _poll(world)

    expected = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, expected]
    assert _receipts(triage) == {str(comment["id"]): ("admitted", expected)}
    assert _retry_keys() == set()


def test_posting_window_answer_edited_into_facts_is_never_an_answer(world: World) -> None:
    """A deferred comment whose body becomes a facts request before its retry
    goes to the facts exchange: no answer and no mention request."""

    number = 6194
    triage = _running(world, number)
    world.github.store_then_fail = 502
    assert _ask(triage).status_code == 503
    world.github.tick()
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    _poll(world)
    assert _retry_keys() == {f"answer-retry:{comment['id']}"}
    world.github.tick()
    world.github.edit_comment(comment["id"], _facts_body())
    assert _ask(triage).status_code == 200
    _settle(triage)

    _poll(world)

    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}
    assert _retry_keys() == set()


# Review round 1 (#5787) ---------------------------------------------------------------


@pytest.mark.parametrize("first", PATHS)
def test_dedupe_mention_answer_after_the_round_clears(world: World, first: str) -> None:
    """An answer that also mentions the factory stays an answer: once its
    attempt ends and the round is cleared, the other door replays it and
    never admits it as a mention revision."""

    number = 6200 if first == "webhook" else 6201
    triage = _outstanding(world, number)
    comment = world.github.add_comment(number, f"@{MENTION} replace the rows.", user=WRITER)
    _deliver(world, first, comment, number)
    answered = _answer_id(triage, comment)
    assert _ids(triage) == [triage.request_id, answered]
    successor = _take_over(triage, answered)
    tr._finish(successor, "failed", "budget_exceeded")
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "cleared", row
    before = _ids(triage)

    second = _deliver(world, "poll" if first == "webhook" else "webhook", comment, number)

    if second is not None:
        assert second["status"] == "factory_duplicate", second
    assert _ids(triage) == before
    assert _mention_id(number, comment) not in _ids(triage)


def test_refused_edit_after_permission_granted(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    """A poll pass sees an edit only as a newer ``updated_at``; a comment
    created before the listing's threshold is never decided again."""

    caplog.set_level(logging.INFO)
    number = 6202
    triage = _outstanding(world, number)
    refused = world.github.add_comment(number, ANSWER, user=READER)
    world.github.tick()
    world.github.add_comment(number, "Build status noted.", user=HELPER_BOT)
    _poll(world)
    assert _refusals(caplog, refused) == {"answer_not_authorized"}
    world.github.grant(READER, "write")
    world.github.tick()
    world.github.edit_comment(refused["id"], "Replace the rows on a re-run, please.")

    _poll(world)

    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


def test_facts_body_on_the_authenticated_read_is_never_an_answer(world: World) -> None:
    """The webhook payload claims a plain answer; GitHub's own read of the
    comment returns a facts request, which keeps precedence."""

    number = 6203
    triage = _outstanding(world, number)
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    payload = _payload(comment, number)
    world.github.edit_comment(comment["id"], _facts_body())

    response = _post(world.client, "issue_comment", payload)

    assert response.status_code == 200, response.text
    assert _code(response.json()) == "ordinary_comment", response.text
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


def test_refused_binding_missing(world: World) -> None:
    number = 6204
    triage = _outstanding(world, number)
    (item,) = _rows(
        "SELECT agent_id FROM curie.work_items WHERE id = :id", {"id": triage.work_item_id}
    )
    # The agent's only GitHub surface moves to another repository.
    moved = world.client.patch(
        f"/agents/{item['agent_id']}/channels",
        params={"kind": "github", "address": REPO},
        headers={"X-API-Key": get_settings().api_key},
        json={"kind": "github", "address": "acme-corp/other-bot"},
    )
    assert moved.status_code == 200, moved.text
    comment = world.github.add_comment(number, ANSWER, user=WRITER)

    result = _deliver(world, "webhook", comment, number)

    assert _code(result) == "binding_missing", result
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


def test_refused_installation_mismatch(world: World) -> None:
    number = 6205
    triage = _outstanding(world, number)
    (item,) = _rows(
        "SELECT code_host_installation_id FROM curie.work_items WHERE id = :id",
        {"id": triage.work_item_id},
    )
    assert item["code_host_installation_id"] == INSTALLATION_ID
    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    payload = _payload(comment, number)
    payload["installation"] = {"id": REINSTALLED}

    response = _post(world.client, "issue_comment", payload)

    assert response.status_code == 200, response.text
    assert _code(response.json()) == "installation_mismatch", response.text
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


def test_refused_payload_claims_another_author(world: World) -> None:
    """Authority comes from GitHub's read of the comment, not the payload."""

    number = 6206
    triage = _outstanding(world, number)
    comment = world.github.add_comment(number, ANSWER, user=READER)
    claimed = {**comment, "user": dict(WRITER)}
    payload = _issue_event("created", number, comment=claimed, sender=dict(WRITER))

    response = _post(world.client, "issue_comment", payload)

    assert response.status_code == 200, response.text
    assert _code(response.json()) == "answer_not_authorized", response.text
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


def test_refused_edit_at_the_cursor_boundary(
    world: World, caplog: pytest.LogCaptureFixture
) -> None:
    """The refused comment is the newest a pass lists, so the next listing's
    inclusive threshold is its own second; its later edit is still an edit."""

    caplog.set_level(logging.INFO)
    number = 6207
    triage = _outstanding(world, number)
    world.github.tick()
    refused = world.github.add_comment(number, ANSWER, user=READER)
    _poll(world)
    assert _refusals(caplog, refused) == {"answer_not_authorized"}
    world.github.grant(READER, "write")
    world.github.tick()
    world.github.edit_comment(refused["id"], "Replace the rows on a re-run, please.")

    _poll(world)

    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}


def test_boundary_reply_in_the_cursor_second_is_new(world: World) -> None:
    """A reply created in the threshold's own second and never edited is new."""

    number = 6208
    triage = _outstanding(world, number)
    question = _question(world, number)
    _poll(world)
    reply = world.github.add_comment(number, ANSWER, user=WRITER)
    assert reply["created_at"] == reply["updated_at"] == question["created_at"]

    _poll(world)

    expected = _answer_id(triage, reply)
    assert _ids(triage) == [triage.request_id, expected]
    assert _receipts(triage) == {str(reply["id"]): ("admitted", expected)}


@pytest.mark.parametrize("path", PATHS)
def test_refused_retained_service_item_in_personal_mode(
    world: World, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, path: str
) -> None:
    """A WorkItem kept from service mode is refused an answer once the install
    runs personal mode, with the code a mention gets."""

    caplog.set_level(logging.INFO)
    number = 6209 if path == "webhook" else 6210
    triage = _outstanding(world, number)
    (item,) = _rows(
        "SELECT model_auth_mode FROM curie.work_items WHERE id = :id",
        {"id": triage.work_item_id},
    )
    assert item["model_auth_mode"] == "service"
    monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", "personal")
    get_settings.cache_clear()
    comment = world.github.add_comment(number, ANSWER, user=WRITER)

    result = _deliver(world, path, comment, number)

    if result is not None:
        assert _code(result) == "personal_owner_required", result
    assert _refusals(caplog, comment) == {"personal_owner_required"}
    assert _ids(triage) == [triage.request_id]
    assert _receipts(triage) == {}
