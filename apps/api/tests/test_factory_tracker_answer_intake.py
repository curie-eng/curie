"""Plain answers through the shared tracker intake (#5789).

A person's comment on a ticket whose factory question is outstanding starts
the next attempt with no mention and no relabel. Every case drives the
production caller, `factory_tracker_intake.poll_trackers`: on Jira through the
real Jira Cloud adapter over the Jira fake (`forge_fakes/jira.py`, whose
payloads follow the REST v3 reference), on GitLab through the offline GitLab
install (`forge_fakes/gitlab_lifecycle.py`, the real reconciler pass), and on
the memory tracker registered as the install's ``jira_cloud`` kind. Postgres is
real.

Every prior state comes from the production path: the label is admitted by a
poll, the request is acquired and started through the worker routes, the
issue-read capability is minted on the worker route, the questions are asked
over HTTP on the question route (the question comment lands on the tracker
through its real adapter), and the worker's finish path settles the request,
which leaves the round ``outstanding``. Outcomes are read from
``execution_requests``, ``factory_answer_receipts`` and
``factory_question_rounds``. Placeholder identifiers only.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api import factory_questions
from curie_api.config import get_settings
from curie_api.forges.memory import InMemoryTracker
from curie_api.forges.types import Actor, TrackerIssueRef
from forge_fakes import gitlab_lifecycle as gl
from forge_fakes import jira as jira_fake
from forge_fakes.bitbucket_lifecycle import install_memory_jira, memory_jira_tracker
from forge_fakes.github_comments import _rows
from test_factory_answer_admission import _receipts
from test_factory_question_ending import QUESTIONS, QUESTIONS_URL, SUMMARY, _round_row
from test_factory_tracker_intake import (  # noqa: F401  (fixtures)
    HOLD_MARKER,
    LABEL,
    STARTER_ID,
    STARTERS_GROUP,
    Intake,
    _ByHost,
    _cursor,
    _dc_document,
    _document,
    _held_tickets,
    _jira_binding,
    _marker,
    _requests,
    _work_items,
    intake,
    make_intake,
)
from test_factory_tracker_results import MINT_URL

pytestmark = pytest.mark.usefixtures("clean_db")

ANSWER_TEXT = "Replace the rows on a re-run."
APP_ID = "5b10a0000000000000000a33"
MEMBER_ID = "5b10a0000000000000000a44"
STARTER = Actor(STARTER_ID, "Ada Starter")


# --- the production path to an outstanding round ----------------------------------------


def _worker() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def _issue(intake: Intake) -> jira_fake.JiraIssue:
    return intake.jira.seed_issue(
        "Widget parser drops the last field", "Parsing a three-field line yields two fields."
    )


def _admitted(intake: Intake, issue: jira_fake.JiraIssue) -> tuple[uuid.UUID, uuid.UUID]:
    """The starter labels ``issue`` and a poll admits it: its WorkItem and request."""

    intake.jira.add_label(issue.id, LABEL, intake.starter)
    intake.poll()
    [item] = _work_items(issue)
    [request] = _requests(item["id"])
    return item["id"], request["id"]


def _start(intake: Intake, request_id: uuid.UUID) -> int:
    """The worker acquires and starts ``request_id``; its runtime epoch."""

    acquired = intake.acquire(request_id)
    assert acquired.status_code == 200, acquired.text
    grant = acquired.json()
    started = intake.client.post(
        f"/v1/internal/work-items/requests/{request_id}/start",
        headers=_worker(),
        json={
            "owner": "factory-intake-owner",
            "generation": grant["generation"],
            "claim_name": f"claim-{request_id.hex[:12]}",
            "sandbox_name": f"sbx-{request_id.hex[:12]}",
            "context": grant["context"],
            "delivered_limits": {
                "memory_bytes": None,
                "ephemeral_storage_bytes": None,
                "scratch_bytes": {},
                "max_turns": None,
            },
        },
    )
    assert started.status_code == 200, started.text
    return int(started.json()["runtime_epoch"])


def _mint(client: Any, request_id: uuid.UUID) -> str:
    """The issue-read capability the worker mints for the sandbox."""

    minted = client.post(
        MINT_URL,
        headers=_worker(),
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    return str(minted.json()["capability"])


def _post_questions(client: Any, capability: str) -> httpx.Response:
    response: httpx.Response = client.post(
        QUESTIONS_URL,
        headers={"X-Curie-Issue-Read": capability},
        json={"summary": SUMMARY, "questions": QUESTIONS},
    )
    return response


def _ask(intake: Intake, capability: str) -> httpx.Response:
    """The runner's ``ask_questions`` call, with the app's client reaching the
    Jira fake, so the question comment is written by the real Jira adapter."""

    external = httpx.AsyncClient(
        transport=_ByHost(intake.jira, intake.github, intake.dc, dc_down=False)
    )
    original = intake.client.app.state.http_client
    intake.client.app.state.http_client = external
    try:
        return _post_questions(intake.client, capability)
    finally:
        intake.client.app.state.http_client = original
        intake.client.portal.call(external.aclose)


def _finish(client: Any, request_id: uuid.UUID, epoch: int) -> None:
    """The worker's finish path: the run ends without a pull request."""

    from curie_api.workitem_dispatch import finish

    async def go() -> Any:
        async with client.app.state.sessionmaker() as session:
            return await finish(
                session,
                request_id,
                runtime_epoch=epoch,
                outcome="failed",
                cause="no_pull_request",
                detail=None,
                ci_fix_round=None,
                retry_safety="uncertain",
                retry_condition=None,
                exhausted_volume=None,
            )

    client.portal.call(go)


def _settled_outstanding(work_item_id: uuid.UUID, request_id: uuid.UUID) -> None:
    assert _status(request_id) == ("completed", "questions_asked")
    row = _round_row(work_item_id)
    assert row is not None
    assert (row["state"], row["pending_request_id"]) == ("outstanding", None)


def _ask_and_settle(intake: Intake, work_item_id: uuid.UUID, request_id: uuid.UUID) -> None:
    epoch = _start(intake, request_id)
    asked = _ask(intake, _mint(intake.client, request_id))
    assert asked.status_code == 200, asked.text
    _finish(intake.client, request_id, epoch)
    _settled_outstanding(work_item_id, request_id)


def _asked(intake: Intake, issue: jira_fake.JiraIssue) -> uuid.UUID:
    """``issue`` admitted, asked and settled on Jira: an outstanding round with
    nothing live. Returns the WorkItem id."""

    work_item_id, request_id = _admitted(intake, issue)
    _ask_and_settle(intake, work_item_id, request_id)
    return work_item_id


# --- observations -------------------------------------------------------------------------


def _status(request_id: uuid.UUID) -> tuple[str, str | None]:
    (row,) = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row["status"], row["terminal_cause"]


def _ids(work_item_id: uuid.UUID) -> list[uuid.UUID]:
    return [row["id"] for row in _requests(work_item_id)]


def _round_state(work_item_id: uuid.UUID) -> tuple[str, uuid.UUID | None] | None:
    row = _round_row(work_item_id)
    return None if row is None else (row["state"], row["pending_request_id"])


def _answer_id(issue: TrackerIssueRef, comment_id: str) -> uuid.UUID:
    return factory_questions.answer_request_id(issue, comment_id)


def _assert_admitted(
    work_item_id: uuid.UUID, issue: TrackerIssueRef, comment_id: str, before: list[uuid.UUID]
) -> uuid.UUID:
    """Exactly one new waiting request on the WorkItem, for ``comment_id``."""

    expected = _answer_id(issue, comment_id)
    assert _ids(work_item_id) == [*before, expected]
    assert _status(expected) == ("waiting", None)
    assert _receipts(work_item_id)[comment_id] == ("admitted", expected)
    assert _round_state(work_item_id) == ("answered", expected)
    return expected


def _assert_untouched(work_item_id: uuid.UUID, before: list[uuid.UUID]) -> None:
    """No request and no receipt; the round still waits for an answer."""

    assert _ids(work_item_id) == before
    assert _receipts(work_item_id) == {}
    assert _round_state(work_item_id) == ("outstanding", None)


# --- AC3: refused answers, and the liveness case ---------------------------------------


def test_answer_not_refused_from_the_allowlisted_starter_admits_one_request(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    """The starter's plain comment, no ADF mention node, on an open labelled
    issue with an outstanding question admits one waiting request within one
    poll pass."""

    caplog.set_level(logging.DEBUG)
    intake.agent()
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)
    ref = intake.ref(issue)

    # A plain ADF paragraph: no ``mention`` node names the bot.
    # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
    comment_id = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()

    created = _assert_admitted(work_item_id, ref, comment_id, before)
    [row] = [r for r in _requests(work_item_id) if r["id"] == created]
    assert row["requester"] == f"jira_cloud:{STARTER_ID}:{intake.starter.display_name}"
    assert row["objective"].splitlines()[0] == f"{jira_fake.SITE}/browse/{issue.key}"
    # Identifiers only: the comment's text reaches no log.
    assert ANSWER_TEXT not in caplog.text

    # A second pass replays nothing.
    intake.poll()
    assert _ids(work_item_id) == [*before, created]


@pytest.mark.parametrize("who", ["outsider", "bot", "app"])
def test_refused_an_answer_by_an_account_that_may_not_start(
    make_intake: Callable[[dict[str, Any]], Intake], who: str
) -> None:
    """An account outside the allowlist and any group, the bot's own account,
    and an ``app`` account (both allowlisted by mistake) answer nothing. Jira
    reports ``accountType`` on every user bean:
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-get
    """

    allowlist = {"type": "allowlist", "accounts": [STARTER_ID, jira_fake.BOT_ACCOUNT_ID, APP_ID]}
    intake = make_intake(_document(jira_binding=_jira_binding(start_authority=allowlist)))
    intake.agent()
    author = {
        "outsider": intake.outsider,
        "bot": intake.jira.bot,
        "app": intake.jira.add_user(APP_ID, "Some App", account_type="app"),
    }[who]
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)

    intake.jira.add_comment(issue.id, author, ANSWER_TEXT)
    intake.poll()

    _assert_untouched(work_item_id, before)
    # Liveness: the starter's plain comment on the same issue admits.
    comment_id = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()
    _assert_admitted(work_item_id, intake.ref(issue), comment_id, before)


def test_refused_an_answer_after_the_trigger_label_was_removed(intake: Intake) -> None:
    """The comment is made, then the label goes, before the poll that would see
    the comment: no answer is admitted. The same pass applies the label
    removal, which cancels the WorkItem and deletes its round (decision 15)."""

    intake.agent()
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)

    intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.jira.remove_label(issue.id, LABEL, intake.starter)
    intake.poll()

    assert _ids(work_item_id) == before
    assert _receipts(work_item_id) == {}
    [item] = _work_items(issue)
    assert item["cancelled_at"] is not None
    assert _round_row(work_item_id) is None


def test_refused_a_plain_comment_on_an_issue_with_no_outstanding_question(
    intake: Intake,
) -> None:
    intake.agent()
    issue = _issue(intake)
    work_item_id, request_id = _admitted(intake, issue)
    early = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)

    intake.poll()

    assert _ids(work_item_id) == [request_id]
    assert _status(request_id) == ("waiting", None)
    assert _receipts(work_item_id) == {}
    assert _round_row(work_item_id) is None

    # Liveness: once the run asks and settles, a new comment is an answer. The
    # earlier comment, if read again, is before the question (the fence).
    _ask_and_settle(intake, work_item_id, request_id)
    comment_id = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()
    _assert_admitted(work_item_id, intake.ref(issue), comment_id, [request_id])
    assert _receipts(work_item_id).get(early, ("not_after_question", None)) == (
        "not_after_question",
        None,
    )


# --- AC4: closed before delivery, on every tracker ----------------------------------------


@pytest.mark.parametrize("closed", [True, False], ids=["done", "open"])
def test_answer_closed_before_delivery_on_jira(intake: Intake, closed: bool) -> None:
    """The comment is created, then the issue moves to a done status category,
    before the poll pass that observes the comment and before any
    cancellation was processed. Jira's search JQL excludes ``statusCategory =
    Done`` and the issue read reports the category:
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/#api-rest-api-3-issue-issueidorkey-get
    """

    intake.agent()
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)

    comment_id = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    if closed:
        intake.jira.set_status(issue.id, "Done", intake.starter)
    [item] = _work_items(issue)
    assert item["cancelled_at"] is None
    intake.poll()

    if closed:
        assert _ids(work_item_id) == before
        assert _receipts(work_item_id) == {}
    else:
        _assert_admitted(work_item_id, intake.ref(issue), comment_id, before)


# GitLab: the offline install, its poll driven by the real reconciler pass.


@pytest.fixture
def gitlab(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[gl.GitLabInstall]:
    yield from gl.gitlab_install(monkeypatch, tmp_path)


def _gitlab_asked(
    install: gl.GitLabInstall, *, before_ask: Callable[[], None] | None = None
) -> tuple[int, uuid.UUID]:
    """A developer's issue admitted, asked and settled on GitLab; ``before_ask``
    runs just before the question route. Returns the iid and the WorkItem id."""

    iid = gl.admit(install)
    [item] = gl.work_items(iid)
    [first] = gl.requests_of(item["id"])
    epoch = gl.start_request(install.client, first["id"])
    capability = _mint(install.client, first["id"])
    if before_ask is not None:
        before_ask()
    asked = _post_questions(install.client, capability)
    assert asked.status_code == 200, asked.text
    _finish(install.client, first["id"], epoch)
    _settled_outstanding(item["id"], first["id"])
    return iid, item["id"]


@pytest.mark.parametrize("closed", [True, False], ids=["closed", "open"])
def test_answer_closed_before_delivery_on_gitlab(gitlab: gl.GitLabInstall, closed: bool) -> None:
    """The note is created, then the issue is closed, before the pass that
    observes the note. GitLab reports ``state`` on the issue
    (https://docs.gitlab.com/api/issues/#single-project-issue) and the note's
    ``created_at`` (https://docs.gitlab.com/api/notes/#list-project-issue-notes).
    """

    backend = gitlab.backend
    iid, work_item_id = _gitlab_asked(gitlab)
    before = _ids(work_item_id)

    note_id = backend.issue_note(iid, backend.developer, ANSWER_TEXT)
    if closed:
        gitlab.fake.set_issue_state(iid, "closed", backend.developer)
    [item] = gl.work_items(iid)
    assert item["cancelled_at"] is None
    gl.reconcile()

    if closed:
        assert _ids(work_item_id) == before
        assert _receipts(work_item_id) == {}
    else:
        _assert_admitted(work_item_id, gitlab.issue_ref(iid), str(note_id), before)


# The memory tracker, registered as the install's ``jira_cloud`` kind.


@pytest.fixture
def memory(
    monkeypatch: pytest.MonkeyPatch, make_intake: Callable[[dict[str, Any]], Intake]
) -> tuple[Intake, InMemoryTracker]:
    tracker = memory_jira_tracker(
        host=jira_fake.SITE_HOST,
        scope_id=jira_fake.CLOUD_ID,
        label=LABEL,
        mention=jira_fake.BOT_ACCOUNT_ID,
    )
    # Before the app boots, so the registry it builds serves this tracker.
    install_memory_jira(monkeypatch, tracker)
    return make_intake(_document()), tracker


def _memory_work_item(issue: TrackerIssueRef) -> uuid.UUID:
    (row,) = _rows(
        "SELECT id FROM curie.work_items WHERE tracker_kind = :kind AND tracker_host = :host "
        "AND tracker_issue_id = :issue",
        {"kind": issue.kind, "host": issue.host, "issue": issue.issue_id},
    )
    work_item_id: uuid.UUID = row["id"]
    return work_item_id


def _memory_admitted(
    intake: Intake, tracker: InMemoryTracker
) -> tuple[TrackerIssueRef, uuid.UUID, uuid.UUID]:
    issue = tracker.seed_issue(
        "Widget parser drops the last field", "Parsing a three-field line yields two fields."
    )
    tracker.apply_label(issue, LABEL, STARTER)
    intake.poll()
    work_item_id = _memory_work_item(issue)
    [request_id] = _ids(work_item_id)
    return issue, work_item_id, request_id


def _memory_asked(intake: Intake, tracker: InMemoryTracker) -> tuple[TrackerIssueRef, uuid.UUID]:
    issue, work_item_id, request_id = _memory_admitted(intake, tracker)
    _ask_and_settle(intake, work_item_id, request_id)
    return issue, work_item_id


@pytest.mark.parametrize("closed", [True, False], ids=["closed", "open"])
def test_answer_closed_before_delivery_on_the_memory_tracker(
    memory: tuple[Intake, InMemoryTracker], closed: bool
) -> None:
    intake, tracker = memory
    intake.agent()
    issue, work_item_id = _memory_asked(intake, tracker)
    before = _ids(work_item_id)

    comment_id = tracker.comment(issue, STARTER, ANSWER_TEXT)
    if closed:
        tracker.close_issue(issue)
    intake.poll()

    if closed:
        assert _ids(work_item_id) == before
        assert _receipts(work_item_id) == {}
        assert _round_state(work_item_id) == ("outstanding", None)
    else:
        _assert_admitted(work_item_id, issue, comment_id, before)


# --- AC5: who may answer is who may start ------------------------------------------------


def test_answer_authority_is_the_bindings_jira_group(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    """With the binding's start authority a Jira group, the issue's reporter
    (on no allowlist, in no group) answers nothing and a group member who is
    on no allowlist answers. Membership is read from the user groups route:
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-users/#api-rest-api-3-user-groups-get
    """

    group = {"type": "group", "group_id": STARTERS_GROUP}
    intake = make_intake(_document(jira_binding=_jira_binding(start_authority=group)))
    member = intake.jira.add_user(MEMBER_ID, "Gail Member")
    intake.jira.set_group_member(STARTERS_GROUP, STARTER_ID, True)
    intake.jira.set_group_member(STARTERS_GROUP, MEMBER_ID, True)
    intake.agent()
    issue = _issue(intake)
    reporter = issue.reporter
    assert reporter.account_id not in {STARTER_ID, MEMBER_ID}
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)

    intake.jira.add_comment(issue.id, reporter, ANSWER_TEXT)
    intake.poll()
    _assert_untouched(work_item_id, before)

    comment_id = intake.jira.add_comment(issue.id, member, ANSWER_TEXT)
    intake.poll()
    created = _assert_admitted(work_item_id, intake.ref(issue), comment_id, before)
    [row] = [r for r in _requests(work_item_id) if r["id"] == created]
    assert row["requester"] == f"jira_cloud:{MEMBER_ID}:Gail Member"


# --- AC6: coalescing and the follow-up -----------------------------------------------------


def _two_answers_in_one_pass(
    intake: Intake,
) -> tuple[jira_fake.JiraIssue, uuid.UUID, list[uuid.UUID], uuid.UUID]:
    intake.agent()
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)

    first = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    second = intake.jira.add_comment(issue.id, intake.starter, "Keep the header row too.")
    intake.poll()

    expected = _answer_id(intake.ref(issue), first)
    assert _ids(work_item_id) == [*before, expected]
    assert _receipts(work_item_id) == {
        first: ("admitted", expected),
        second: ("coalesced", expected),
    }
    assert _round_state(work_item_id) == ("answered", expected)
    return issue, work_item_id, before, expected


def test_two_answers_in_one_pass_coalesce_into_one_request(intake: Intake) -> None:
    issue, _, _, request = _two_answers_in_one_pass(intake)
    outsider = intake.jira.add_user(MEMBER_ID, "Report Reader")
    intake.jira.add_comment(issue.id, outsider, ANSWER_TEXT)
    intake.poll()
    _start(intake, request)
    token = _mint(intake.client, request)
    external = httpx.AsyncClient(
        transport=_ByHost(intake.jira, intake.github, intake.dc, dc_down=False)
    )
    original = intake.client.app.state.http_client
    intake.client.app.state.http_client = external
    try:
        read = _read_issue(intake.client, token)
    finally:
        intake.client.app.state.http_client = original
        intake.client.portal.call(external.aclose)
    assert read["thread_context"].count(ANSWER_TEXT) == 2
    assert read["answer_attempt"] is True
    assert [(a["author_id"], a["text"]) for a in read["admitted_answers"]] == [
        (STARTER_ID, ANSWER_TEXT),
        (STARTER_ID, "Keep the header row too."),
    ]


def _read_issue(client: Any, token: str) -> dict[str, Any]:
    response = client.post("/work-items/issue-read", headers={"X-Curie-Issue-Read": token})
    assert response.status_code == 200, response.text
    return dict(response.json())


def test_an_answer_while_the_answer_runs_queues_one_follow_up(intake: Intake) -> None:
    issue, work_item_id, before, running = _two_answers_in_one_pass(intake)
    _start(intake, running)
    assert _status(running) == ("running", None)

    third = intake.jira.add_comment(issue.id, intake.starter, "Also skip blank lines.")
    intake.poll()
    follow_up = _answer_id(intake.ref(issue), third)
    assert _ids(work_item_id) == [*before, running, follow_up]
    assert _status(follow_up) == ("queued", None)
    assert _receipts(work_item_id)[third] == ("queued_follow_up", follow_up)
    assert _round_state(work_item_id) == ("answered", follow_up)

    fourth = intake.jira.add_comment(issue.id, intake.starter, "And trim trailing spaces.")
    intake.poll()
    assert _ids(work_item_id) == [*before, running, follow_up]
    assert _receipts(work_item_id)[fourth] == ("coalesced", follow_up)
    assert _status(running) == ("running", None)
    assert _round_state(work_item_id) == ("answered", follow_up)


# --- AC9, memory half -------------------------------------------------------------------------


def test_memory_answer_admits_and_an_issue_without_a_question_yields_nothing(
    memory: tuple[Intake, InMemoryTracker],
) -> None:
    intake, tracker = memory
    intake.agent()
    asked, asked_item = _memory_asked(intake, tracker)
    quiet, quiet_item, quiet_request = _memory_admitted(intake, tracker)
    asked_before = _ids(asked_item)

    tracker.comment(quiet, STARTER, ANSWER_TEXT)
    comment_id = tracker.comment(asked, STARTER, ANSWER_TEXT)
    intake.poll()

    _assert_admitted(asked_item, asked, comment_id, asked_before)
    assert _ids(quiet_item) == [quiet_request]
    assert _status(quiet_request) == ("waiting", None)
    assert _receipts(quiet_item) == {}
    assert _round_row(quiet_item) is None


# --- posting-state recovery ------------------------------------------------------------------


def test_posting_an_answer_waits_for_the_question_to_be_recorded(
    intake: Intake, caplog: pytest.LogCaptureFixture
) -> None:
    """The question comment lands on Jira but the create's answer is lost, so
    the round stays ``posting``. An answer seen then is held, not refused: no
    request, no receipt, no hold comment, and the cursor stays. The run
    retries the route, which finds its marked comment and records the round;
    the next pass admits the answer."""

    caplog.set_level(logging.INFO)
    intake.agent()
    issue = _issue(intake)
    work_item_id, request_id = _admitted(intake, issue)
    epoch = _start(intake, request_id)
    capability = _mint(intake.client, request_id)
    marker = factory_questions.marker_for(request_id)

    # The comment create is applied, then answered with a gateway error.
    # https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-post
    intake.jira.fail_next("comment_create", status=502, applied=True)
    lost = _ask(intake, capability)
    assert lost.status_code == 503, lost.text
    assert [_marker(c) for c in intake.jira.own_comments(issue.id)] == [marker]
    assert _round_state(work_item_id) == ("posting", None)

    cursor = _cursor()
    assert cursor is not None
    comment_id = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()

    assert _ids(work_item_id) == [request_id]
    assert _receipts(work_item_id) == {}
    assert _round_state(work_item_id) == ("posting", None)
    assert [_marker(c) for c in intake.jira.own_comments(issue.id)] == [marker]
    assert HOLD_MARKER not in {_marker(c) for c in intake.jira.own_comments(issue.id)}
    assert _cursor() == cursor
    assert "still posting" in caplog.text
    assert ANSWER_TEXT not in caplog.text

    # The run retries the same call: the marked comment is found, not written again.
    retried = _ask(intake, capability)
    assert retried.status_code == 200, retried.text
    assert retried.json()["round"] == 1
    assert [_marker(c) for c in intake.jira.own_comments(issue.id)] == [marker]
    _finish(intake.client, request_id, epoch)
    _settled_outstanding(work_item_id, request_id)

    intake.poll()

    expected = _assert_admitted(work_item_id, intake.ref(issue), comment_id, [request_id])
    assert _receipts(work_item_id) == {comment_id: ("admitted", expected)}
    assert _cursor() != cursor


# --- the fence: each comment's tracker id and creation time -----------------------------------


def test_fence_a_jira_answer_in_the_questions_second_with_a_higher_id_is_admitted(
    intake: Intake,
) -> None:
    """Jira reports ``created`` to the millisecond and numbers comments in
    creation order; an answer stamped with the question comment's own
    ``created`` but a higher id is after it.
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-comments/#api-rest-api-3-issue-issueidorkey-comment-get
    """

    intake.agent()
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)
    [question] = intake.jira.own_comments(issue.id)
    row = _round_row(work_item_id)
    assert row is not None and row["comment_id"] == str(question.id)

    comment_id = intake.jira.add_comment(
        issue.id, intake.starter, ANSWER_TEXT, at=question.created_ms
    )
    assert intake.jira.comment(comment_id).created_ms == question.created_ms
    assert int(comment_id) > question.id
    intake.poll()

    _assert_admitted(work_item_id, intake.ref(issue), comment_id, before)


def test_fence_a_gitlab_note_in_the_question_notes_second_is_admitted(
    gitlab: gl.GitLabInstall,
) -> None:
    """GitLab note ids grow with creation and ``created_at`` is whole
    milliseconds (https://docs.gitlab.com/api/notes/). The question note and
    the answer share one creation time, before the round's recorded fence, so
    only the bare note id, compared with the question note's, admits it; the
    ``note:<id>`` event id would fall back to the time and refuse the tie."""

    fake = gitlab.fake
    backend = gitlab.backend

    def clock_to_now() -> None:
        # The fake's clock to a few seconds before the database's, so the
        # question note is stamped before the round's fence is read.
        (row,) = _rows("SELECT clock_timestamp() AS now")
        fake.tick(at=row["now"] - timedelta(seconds=5) - fake.step)

    iid, work_item_id = _gitlab_asked(gitlab, before_ask=clock_to_now)
    before = _ids(work_item_id)
    [question] = [
        note
        for note in fake.issue_notes(iid)
        if str(note.author) == backend.bot.id and factory_questions.MARKER_PREFIX in note.body
    ]
    row = _round_row(work_item_id)
    assert row is not None and row["comment_id"] == str(question.id)
    assert question.created_at < row["answer_after"]

    note_id = fake.issue_note(iid, backend.developer, ANSWER_TEXT, at=question.created_at)
    assert note_id > question.id
    gl.reconcile()

    _assert_admitted(work_item_id, gitlab.issue_ref(iid), str(note_id), before)


# --- review round 1: one comment, one decision -------------------------------------------


@pytest.fixture
def gitlab_two_labels(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[gl.GitLabInstall]:
    """Two surfaces on one binding: the tracker's own label and ``FAST``."""

    yield from gl.gitlab_install(monkeypatch, tmp_path, labels=(None, gl.FAST))


def test_answer_mention_on_a_second_labels_poll_starts_one_request_on_gitlab(
    gitlab_two_labels: gl.GitLabInstall,
) -> None:
    """A developer's reply that mentions the bot, on an answering issue that
    carries the tracker's own label. GitLab's incremental listing is not label
    filtered (``updated_after``,
    https://docs.gitlab.com/api/issues/#list-project-issues), so the same pass
    delivers the note as an answer under that label's poll and as a mention
    under the other label's. The comment starts exactly one request."""

    install = gitlab_two_labels
    backend = install.backend
    # The other surface is in use: its label's first poll lists an issue, so
    # that label's cursor is read from GitLab's own issue times.
    other = backend.add_issue("Widget parser drops a quoted field", author=backend.developer)
    backend.label_issue(other, gl.FAST, backend.developer)
    iid, work_item_id = _gitlab_asked(install)
    [fast_item] = gl.work_items(other)
    assert fast_item["trigger_label"] == gl.FAST
    before = _ids(work_item_id)

    note_id = backend.issue_note(iid, backend.developer, gl.mention_body(install))
    gl.reconcile()

    _assert_admitted(work_item_id, install.issue_ref(iid), str(note_id), before)
    assert _receipts(work_item_id) == {
        str(note_id): ("admitted", _answer_id(install.issue_ref(iid), str(note_id)))
    }
    gl.reconcile()
    assert len(_ids(work_item_id)) == len(before) + 1


def test_answer_mention_replayed_after_its_round_cleared_starts_nothing(intake: Intake) -> None:
    """Issue A's answer is held (its question is ``posting``), so the label's
    cursor stays and the next pass replays the page. In the held pass issue
    B's reply, which mentions the bot, is admitted as B's answer. B's answer
    request then runs and ends, which clears B's round, so the replayed page
    delivers B's reply as a mention. The receipt it already has decides it:
    no second request. A is admitted once its round is recorded."""

    intake.agent()
    first, second = _issue(intake), _issue(intake)
    intake.jira.add_label(first.id, LABEL, intake.starter)
    intake.jira.add_label(second.id, LABEL, intake.starter)
    intake.poll()
    [a_item] = _work_items(first)
    [b_item] = _work_items(second)
    [a_request] = _ids(a_item["id"])
    [b_request] = _ids(b_item["id"])
    _ask_and_settle(intake, b_item["id"], b_request)

    a_epoch = _start(intake, a_request)
    a_capability = _mint(intake.client, a_request)
    intake.jira.fail_next("comment_create", status=502, applied=True)
    assert _ask(intake, a_capability).status_code == 503
    assert _round_state(a_item["id"]) == ("posting", None)

    cursor = _cursor()
    a_answer = intake.jira.add_comment(first.id, intake.starter, ANSWER_TEXT)
    # An ADF ``mention`` node naming the bot (the comment body is ADF,
    # https://developer.atlassian.com/cloud/jira/platform/apis/document/nodes/mention/).
    b_reply = intake.jira.add_comment(
        second.id,
        intake.starter,
        jira_fake.adf_mention(jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" the blue one"),
    )
    intake.poll()

    assert _cursor() == cursor
    assert _ids(a_item["id"]) == [a_request]
    b_answer = _assert_admitted(b_item["id"], intake.ref(second), b_reply, [b_request])

    # B's answer runs and ends without a question: its round clears.
    _finish(intake.client, b_answer, _start(intake, b_answer))
    assert _round_state(b_item["id"]) == ("cleared", b_answer)
    b_after = _ids(b_item["id"])
    # A's run retries the route and ends with the question.
    assert _ask(intake, a_capability).status_code == 200
    _finish(intake.client, a_request, a_epoch)
    _settled_outstanding(a_item["id"], a_request)

    intake.poll()

    assert _ids(b_item["id"]) == b_after
    assert _receipts(b_item["id"]) == {b_reply: ("admitted", b_answer)}
    _assert_admitted(a_item["id"], intake.ref(first), a_answer, [a_request])
    assert _cursor() != cursor


def test_answer_first_poll_again_after_a_held_page_admits_the_answer(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    """The label's first page holds an issue (its code host does not answer),
    so no cursor is saved and every pass is a first poll. Another issue is
    admitted, asked and settled, and the starter answers it: the next pass,
    still a first poll, admits exactly one request for the answer."""

    intake = make_intake(_dc_document())
    held, issue, _event = _held_tickets(intake)
    intake.dc_down = True
    intake.poll()
    assert _cursor() is None
    assert _work_items(held) == []
    [item] = _work_items(issue)
    [request_id] = _ids(item["id"])
    _ask_and_settle(intake, item["id"], request_id)

    comment_id = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()

    assert _cursor() is None
    _assert_admitted(item["id"], intake.ref(issue), comment_id, [request_id])

    # The outage ends: the page is applied, the cursor saved, nothing repeats.
    intake.dc_down = False
    intake.poll()
    assert _cursor() is not None
    assert len(_work_items(held)) == 1
    assert len(_ids(item["id"])) == 2
    assert _receipts(item["id"]) == {
        comment_id: ("admitted", _answer_id(intake.ref(issue), comment_id))
    }


# --- review round 2: a mention's answer currency, and a snapshot that went stale ----------

# A second surface label that sorts before the tracker's own, so its poll runs
# first in a pass (the intake polls a binding's labels in sorted order).
EARLY = "early-fast"


@pytest.fixture
def gitlab_early_label(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[gl.GitLabInstall]:
    """Two surfaces on one binding: ``EARLY`` and the tracker's own label."""

    yield from gl.gitlab_install(monkeypatch, tmp_path, labels=(None, EARLY))


@pytest.mark.parametrize("change", ["closed", "unlabelled", "open"])
def test_answer_mention_on_an_earlier_labels_poll_needs_the_open_labelled_issue(
    gitlab_early_label: gl.GitLabInstall, change: str
) -> None:
    """A developer's reply that mentions the bot on an answering issue, then the
    issue is closed or loses the tracker's own label (its WorkItem's trigger
    label) before the next pass. The earlier label's poll runs first and
    delivers the reply as a mention, whose own check reads only the note
    (https://docs.gitlab.com/api/notes/#get-single-issue-note); answering it
    needs the issue open (``state``,
    https://docs.gitlab.com/api/issues/#single-project-issue) and labelled, so
    no request starts, and the own label's poll then cancels the WorkItem.
    Left open and labelled, the reply starts exactly one request."""

    install = gitlab_early_label
    backend = install.backend
    # The earlier surface is in use, so its label's poll reads a cursor.
    other = backend.add_issue("Widget parser drops a quoted field", author=backend.developer)
    backend.label_issue(other, EARLY, backend.developer)
    iid, work_item_id = _gitlab_asked(install)
    [early_item] = gl.work_items(other)
    assert early_item["trigger_label"] == EARLY
    before = _ids(work_item_id)

    note_id = backend.issue_note(iid, backend.developer, gl.mention_body(install))
    if change == "closed":
        install.fake.set_issue_state(iid, "closed", backend.developer)
    elif change == "unlabelled":
        backend.label_issue(iid, install.label, backend.developer, add=False)
    gl.reconcile()

    if change == "open":
        _assert_admitted(work_item_id, install.issue_ref(iid), str(note_id), before)
        return
    assert _ids(work_item_id) == before
    assert _receipts(work_item_id) == {}
    [item] = gl.work_items(iid)
    assert item["cancelled_at"] is not None


def test_answer_snapshot_taken_before_the_round_cleared_keeps_a_fresh_mention(
    intake: Intake,
) -> None:
    """The pass takes its answering set while the round is ``answered``. Before
    the tracker read, the answer's run ends without a question, which clears
    the round, and the starter's newer reply mentions the bot. The pass
    delivers the reply as an answer: it is held with no receipt and the cursor
    stays. The next pass, whose snapshot leaves the issue out, delivers it as
    a mention, which starts exactly one request."""

    intake.agent()
    issue = _issue(intake)
    work_item_id = _asked(intake, issue)
    before = _ids(work_item_id)
    first = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()
    answer = _assert_admitted(work_item_id, intake.ref(issue), first, before)
    epoch = _start(intake, answer)

    # An ADF ``mention`` node naming the bot (the comment body is ADF,
    # https://developer.atlassian.com/cloud/jira/platform/apis/document/nodes/mention/).
    reply = intake.jira.add_comment(
        issue.id,
        intake.starter,
        jira_fake.adf_mention(jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" also trim"),
    )
    cursor = _cursor()
    # The pass's JQL search is its first tracker read, after the snapshot
    # (https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-post).
    intake.jira.before_next("search", lambda: _finish(intake.client, answer, epoch))
    intake.poll()

    assert _round_state(work_item_id) == ("cleared", answer)
    assert _cursor() == cursor
    assert _receipts(work_item_id) == {first: ("admitted", answer)}
    assert _ids(work_item_id) == [*before, answer]

    intake.poll()

    ids = _ids(work_item_id)
    assert ids[: len(before) + 1] == [*before, answer]
    assert len(ids) == len(before) + 2
    assert _answer_id(intake.ref(issue), reply) not in ids
    assert _receipts(work_item_id) == {first: ("admitted", answer)}
    assert _cursor() != cursor


# --- final review: a held reply on a first poll, and its replay after closure -------------


def _mention_reply(intake: Intake, issue: jira_fake.JiraIssue) -> str:
    # An ADF ``mention`` node naming the bot (the comment body is ADF,
    # https://developer.atlassian.com/cloud/jira/platform/apis/document/nodes/mention/).
    return intake.jira.add_comment(
        issue.id,
        intake.starter,
        jira_fake.adf_mention(jira_fake.BOT_ACCOUNT_ID, jira_fake.BOT_NAME, after=" also trim"),
    )


def test_answer_snapshot_stale_on_a_first_poll_delivers_the_held_mention_on_jira(
    make_intake: Callable[[dict[str, Any]], Intake],
) -> None:
    """The label's first page holds a ticket (its code host does not answer),
    so every pass is a first poll. Another ticket is admitted, asked and
    answered. Its answer's run ends after a pass's snapshot, while the
    starter's reply mentions the bot: that pass holds the reply. The next pass,
    still a first poll, delivers it as a mention, which starts exactly one
    request, and nothing repeats once the outage ends and the cursor is saved."""

    intake = make_intake(_dc_document())
    _held, issue, _event = _held_tickets(intake)
    intake.dc_down = True
    intake.poll()
    assert _cursor() is None
    [item] = _work_items(issue)
    work_item_id = item["id"]
    [request_id] = _ids(work_item_id)
    _ask_and_settle(intake, work_item_id, request_id)
    first = intake.jira.add_comment(issue.id, intake.starter, ANSWER_TEXT)
    intake.poll()
    answer = _assert_admitted(work_item_id, intake.ref(issue), first, [request_id])
    epoch = _start(intake, answer)

    reply = _mention_reply(intake, issue)
    # The pass's JQL search is its first tracker read, after the snapshot
    # (https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/#api-rest-api-3-search-jql-post).
    intake.jira.before_next("search", lambda: _finish(intake.client, answer, epoch))
    intake.poll()

    assert _cursor() is None
    assert _round_state(work_item_id) == ("cleared", answer)
    assert _ids(work_item_id) == [request_id, answer]
    assert _receipts(work_item_id) == {first: ("admitted", answer)}

    intake.poll()

    ids = _ids(work_item_id)
    assert ids[:2] == [request_id, answer]
    assert len(ids) == 3
    assert _answer_id(intake.ref(issue), reply) not in ids
    assert _receipts(work_item_id) == {first: ("admitted", answer)}

    intake.dc_down = False
    intake.poll()
    assert _cursor() is not None
    intake.poll()
    assert _ids(work_item_id) == ids


def _gitlab_cursor(install: gl.GitLabInstall) -> str | None:
    rows = _rows("SELECT etags FROM curie.factory_poll_cursors WHERE tracker_kind = 'gitlab'", {})
    if not rows:
        return None
    value = (rows[0]["etags"] or {}).get(f"marked:{install.label}")
    return None if value is None else str(value)


def _issues_listing(install: gl.GitLabInstall) -> str:
    """The project's issue listing, the GitLab poll's first read after its
    snapshot (https://docs.gitlab.com/api/issues/#list-project-issues)."""

    return f"/projects/{install.backend.project_id}/issues"


def _answered_on_gitlab(
    install: gl.GitLabInstall, *, before_pass: Callable[[], None]
) -> tuple[int, uuid.UUID, uuid.UUID, uuid.UUID, int]:
    """A developer's issue admitted, asked, settled and answered on GitLab,
    and the answer's run started; ``before_pass`` runs before every pass.
    Returns the iid, the WorkItem, its first request, the answer request and
    the answer's runtime epoch."""

    backend = install.backend
    before_pass()
    iid, work_item_id = _gitlab_asked(install)
    [first_request] = _ids(work_item_id)
    note_id = backend.issue_note(iid, backend.developer, ANSWER_TEXT)
    before_pass()
    gl.reconcile()
    answer = _assert_admitted(work_item_id, install.issue_ref(iid), str(note_id), [first_request])
    return iid, work_item_id, first_request, answer, gl.start_request(install.client, answer)


def test_answer_snapshot_stale_on_a_first_poll_delivers_the_held_mention_on_gitlab(
    gitlab: gl.GitLabInstall,
) -> None:
    """The GitLab twin of the Jira case: another labelled issue's admission is
    held on every pass (its issue read answers 503,
    https://docs.gitlab.com/api/rest/troubleshooting/#status-codes), so every
    pass is a first poll. The answer's run ends after a pass's snapshot while
    the developer's reply mentions the bot: that pass holds the reply, and the
    next first poll delivers it as a mention that starts exactly one request."""

    backend = gitlab.backend
    blocked = backend.add_issue("Widget exporter drops the header row", author=backend.developer)
    backend.label_issue(blocked, gitlab.label, backend.developer)
    blocked_read = f"/projects/{backend.project_id}/issues/{blocked}"

    def hold_blocked() -> None:
        gitlab.fake.script(503, path=blocked_read, method="GET")

    iid, work_item_id, first_request, answer, epoch = _answered_on_gitlab(
        gitlab, before_pass=hold_blocked
    )
    assert _gitlab_cursor(gitlab) is None

    reply = backend.issue_note(iid, backend.developer, gl.mention_body(gitlab))
    gitlab.fake.before_next(
        lambda: _finish(gitlab.client, answer, epoch), method="GET", path=_issues_listing(gitlab)
    )
    hold_blocked()
    gl.reconcile()

    assert _gitlab_cursor(gitlab) is None
    assert _round_state(work_item_id) == ("cleared", answer)
    assert _ids(work_item_id) == [first_request, answer]

    hold_blocked()
    gl.reconcile()

    ids = _ids(work_item_id)
    assert ids[:2] == [first_request, answer]
    assert len(ids) == 3
    assert _answer_id(gitlab.issue_ref(iid), str(reply)) not in ids
    assert str(reply) not in _receipts(work_item_id)

    gl.reconcile()
    assert _gitlab_cursor(gitlab) is not None
    assert len(gl.work_items(blocked)) == 1
    gl.reconcile()
    assert _ids(work_item_id) == ids


@pytest.mark.parametrize("change", ["closed", "unlabelled", "open"])
def test_answer_held_reply_replayed_after_the_issue_changed_on_gitlab(
    gitlab: gl.GitLabInstall, change: str
) -> None:
    """The answer's run ends after a pass's snapshot while the developer's
    reply mentions the bot, so that pass holds the reply. Before the replay
    the issue is closed or loses the trigger label: the replayed mention, whose
    own check reads only the note, starts nothing, and the same pass cancels
    the WorkItem. Left open and labelled, the replay starts one request."""

    backend = gitlab.backend
    iid, work_item_id, first_request, answer, epoch = _answered_on_gitlab(
        gitlab, before_pass=lambda: None
    )
    reply = backend.issue_note(iid, backend.developer, gl.mention_body(gitlab))
    gitlab.fake.before_next(
        lambda: _finish(gitlab.client, answer, epoch), method="GET", path=_issues_listing(gitlab)
    )
    gl.reconcile()
    assert _round_state(work_item_id) == ("cleared", answer)
    assert _ids(work_item_id) == [first_request, answer]

    if change == "closed":
        gitlab.fake.set_issue_state(iid, "closed", backend.developer)
    elif change == "unlabelled":
        backend.label_issue(iid, gitlab.label, backend.developer, add=False)
    gl.reconcile()

    ids = _ids(work_item_id)
    assert str(reply) not in _receipts(work_item_id)
    [item] = gl.work_items(iid)
    if change == "open":
        assert ids[:2] == [first_request, answer]
        assert len(ids) == 3
        assert item["cancelled_at"] is None
        return
    assert ids == [first_request, answer]
    assert item["cancelled_at"] is not None


# --- 04-tu: a plain answer read under a snapshot from before its question ----------------


def test_answer_plain_reply_after_a_first_polls_snapshot_is_admitted_on_gitlab(
    gitlab: gl.GitLabInstall,
) -> None:
    """The binding's first poll takes its answering snapshot before the
    question exists. Between that snapshot and the poll's note read, the run
    posts its question, ends, and the developer replies with a plain note:
    the first poll sees a plain note on an issue that is not answering, so it
    starts nothing, and it saves the label's first cursor. The next poll, with
    the issue now answering, admits that reply as the answer, exactly once.
    Another labelled issue's read answers 503 on the admitting pass
    (https://docs.gitlab.com/api/rest/troubleshooting/#status-codes), so the
    cursor stays unsaved until the pass under test."""

    backend = gitlab.backend
    blocked = backend.add_issue("Widget exporter drops the header row", author=backend.developer)
    backend.label_issue(blocked, gitlab.label, backend.developer)
    gitlab.fake.script(503, path=f"/projects/{backend.project_id}/issues/{blocked}", method="GET")
    iid = gl.admit(gitlab)
    assert _gitlab_cursor(gitlab) is None
    [item] = gl.work_items(iid)
    work_item_id = item["id"]
    [first] = _ids(work_item_id)
    epoch = gl.start_request(gitlab.client, first)
    capability = _mint(gitlab.client, first)
    replies: list[int] = []

    def ask_then_reply() -> None:
        asked = _post_questions(gitlab.client, capability)
        assert asked.status_code == 200, asked.text
        _finish(gitlab.client, first, epoch)
        replies.append(backend.issue_note(iid, backend.developer, ANSWER_TEXT))

    gitlab.fake.before_next(ask_then_reply, method="GET", path=_issues_listing(gitlab))
    gl.reconcile()

    [reply] = replies
    _settled_outstanding(work_item_id, first)
    assert _gitlab_cursor(gitlab) is not None
    assert _ids(work_item_id) == [first]

    gl.reconcile()

    answered = _assert_admitted(work_item_id, gitlab.issue_ref(iid), str(reply), [first])
    gl.reconcile()
    assert len(_ids(work_item_id)) == 2
    gl.start_request(gitlab.client, answered)
    read = _read_issue(gitlab.client, _mint(gitlab.client, answered))
    assert read["admitted_answers"] == [
        {"comment_id": str(reply), "author_id": str(backend.developer.id), "text": ANSWER_TEXT}
    ]
