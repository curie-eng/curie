"""One bot-authored status comment per request, edited in place (#3077).

The real reconciler drives the threaded GitHub fake from
test_factory_terminus.py, which records every comment create, comment edit,
label add and label removal it receives:
https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
https://docs.github.com/en/rest/issues/labels#add-labels-to-an-issue
https://docs.github.com/en/rest/issues/labels#remove-a-label-from-an-issue

Admission is a signed issues webhook; progress arrives through the real
``/v1/work-item-progress`` route. Machine fixtures drive the events.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.factory_notices import FINAL_MARKER, marker_for
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_factory_progress import DECLARATION, report
from test_factory_terminus import (  # noqa: F401  (fixtures)
    _LABELS,
    REPO,
    _attach_publication,
    _attach_revision_publication,
    _insert_revision,
    _notices,
    _patches,
    _posts,
    _published_issue,
    _reconcile,
    _request,
    _revision_objective,
    _rows,
    _start_running,
    admitted,
    comments,
)
from test_github_factory_ingress import LABEL, _issue_event, _post

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}
STATE_LABELS = {"curie:queued", "curie:running", "curie:pr-open", "curie:needs-human"}
WAITING = "_Waiting for the agent to report progress._"
CARD_BASE = "https://curie.example.com"


def _writes(sink: Any) -> list[tuple[str, str, str | None]]:
    return [r for r in sink.requests if r[0] in {"POST", "PATCH", "DELETE"}]


def _label_writes(sink: Any) -> list[tuple[str, str, str | None]]:
    return [r for r in sink.requests if r[0] in {"POST", "DELETE"} and _LABELS.match(r[1])]


def _curie_labels(sink: Any, number: int) -> set[str]:
    return sink.issue_labels.get(number, set()) & STATE_LABELS


def _marked(sink: Any, request_id: uuid.UUID) -> list[dict[str, Any]]:
    everything = list(sink.comments) + [c for listed in sink.lists.values() for c in listed]
    return [c for c in everything if marker_for(request_id) in (c.get("body") or "")]


def _admit(client: Any, github: Any, sink: Any, number: int) -> uuid.UUID:
    github.issue_number = number
    github.labels = [LABEL]
    sink.issue_labels[number] = {LABEL, "bug"}
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.json()["status"] == "factory_admitted", response.text
    return _request(number)["id"]


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _finish_failed(
    client: Any, request_id: uuid.UUID, epoch: int, cause: str, detail: str | None = None
) -> None:
    body: dict[str, Any] = {"runtime_epoch": epoch, "outcome": "failed", "cause": cause}
    if detail is not None:
        body["detail"] = detail
    finished = client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish", headers=WORKER, json=body
    )
    assert finished.status_code == 200, finished.text


# --- 1: created at admission ------------------------------------------------------


def test_admission_creates_one_queued_status_comment(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9901
    request_id = _admit(client, github, sink, number)
    _reconcile()

    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/issues/{number}/comments"]
    body = posts[0][1] or ""
    assert marker_for(request_id) in body
    assert "Status: QUEUED" in body
    assert WAITING in body
    assert FINAL_MARKER not in body
    assert "![Curie status]" not in body
    assert body.rstrip().endswith(marker_for(request_id))
    assert _curie_labels(sink, number) == {"curie:queued"}
    # Human and admission labels are never touched.
    assert {LABEL, "bug"} <= sink.issue_labels[number]
    row = _notices(request_id)[0]
    assert row["comment_list"] == "issue"
    assert row["finalized_at"] is None


def test_the_card_image_is_linked_when_a_base_url_is_set(
    admitted: Any, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    client, github, sink = admitted
    monkeypatch.setenv("GITHUB_FACTORY_CARD_BASE_URL", CARD_BASE)
    get_settings.cache_clear()
    number = 9902
    request_id = _admit(client, github, sink, number)
    _reconcile()

    token = _notices(request_id)[0]["card_token"]
    body = _posts(sink)[0][1] or ""
    assert f"![Curie status]({CARD_BASE}/v1/factory/cards/{token}.svg)" in body
    assert "width" not in body
    # The card carries the phases, so the comment adds no placeholder (#3125).
    assert WAITING not in body
    assert "Status: QUEUED" in body


def test_a_card_url_replaces_the_checklist_as_phases_advance(
    admitted: Any, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    client, github, sink = admitted
    monkeypatch.setenv("GITHUB_FACTORY_CARD_BASE_URL", CARD_BASE)
    get_settings.cache_clear()
    number = 9912
    request_id = _admit(client, github, sink, number)
    _reconcile()
    token = _notices(request_id)[0]["card_token"]
    sink.requests.clear()

    _start_running(request_id)
    for phase, loop_round in (("read_issue", None), ("plan", 1)):
        assert report(client, request_id, phase, round=loop_round).status_code == 201
    _reconcile()

    body = _patches(sink)[-1][1] or ""
    assert f"![Curie status]({CARD_BASE}/v1/factory/cards/{token}.svg)" in body
    assert "Status: RUNNING" in body
    assert "- [" not in body
    assert "Read issue" not in body
    assert WAITING not in body
    assert body.rstrip().endswith(marker_for(request_id))


# --- 2: edited in place as phases advance --------------------------------------------


def test_progress_edits_the_same_comment_and_moves_the_label_to_running(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9903
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()

    _start_running(request_id)
    for phase, loop_round in (("read_issue", None), ("pin_criteria", None), ("plan", 1)):
        assert report(client, request_id, phase, round=loop_round).status_code == 201
    _reconcile()

    assert _posts(sink) == []
    patches = _patches(sink)
    assert [path for path, _ in patches] == [f"/repos/{REPO}/issues/comments/{comment_id}"]
    body = patches[0][1] or ""
    assert "- [x] Read issue" in body
    assert "- [x] Pin acceptance criteria" in body
    assert "- [ ] **Plan** (in progress, round 1 of 3)" in body
    assert "- [ ] Plan review" in body
    assert "- [ ] Wait for CI" in body
    assert "Status: RUNNING" in body
    assert WAITING not in body
    assert FINAL_MARKER not in body
    # Model notes never reach the Markdown, only the card.
    assert marker_for(request_id) in body
    assert ("POST", f"/repos/{REPO}/issues/{number}/labels", '["curie:running"]') in sink.requests
    assert ("DELETE", f"/repos/{REPO}/issues/{number}/labels/curie:queued", None) in sink.requests
    assert _curie_labels(sink, number) == {"curie:running"}
    assert {LABEL, "bug"} <= sink.issue_labels[number]

    sink.requests.clear()
    _reconcile()
    assert _writes(sink) == []


def test_a_note_is_not_rendered_into_the_comment(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9904
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    note = "ping @octocat and see [this](https://evil.example)"
    assert report(client, request_id, "read_issue", note=note).status_code == 201
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "@octocat" not in comment["body"]
    assert "evil.example" not in comment["body"]


def test_phase_labels_are_markdown_escaped(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9905
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    declaration = {
        "phases": [
            {"id": "look", "label": "Look *here* [x](y)"},
            {"id": "done_now", "label": "Done"},
        ],
        "loops": [],
    }
    assert report(client, request_id, "done_now", declaration=declaration).status_code == 201
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert r"- [x] Look \*here\* \[x\]\(y\)" in comment["body"]


# --- 3: completed, one comment, no separate final comment ---------------------------------


def test_completion_patches_the_pr_link_and_finalizes(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9906
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    _start_running(request_id)
    assert report(client, request_id, "publish").status_code == 201
    work_item_id = _request(number)["work_item_id"]
    _attach_publication(work_item_id, status="succeeded", pr=77)
    sink.requests.clear()
    _reconcile()

    assert _request(number)["status"] == "completed"
    assert _posts(sink) == []
    patches = _patches(sink)
    assert patches and {path for path, _ in patches} == {
        f"/repos/{REPO}/issues/comments/{comment_id}"
    }
    body = patches[-1][1] or ""
    pr_url = f"https://github.com/{REPO}/pull/77"
    assert f"Completed: {pr_url}" in body
    assert "Status: SUCCEEDED" in body
    assert "- [x] Wait for CI" in body
    assert (
        body.index("Completed:")
        < body.index("Status: SUCCEEDED")
        < body.index(FINAL_MARKER)
        < body.index(marker_for(request_id))
    )
    assert _notices(request_id)[0]["finalized_at"] is not None
    assert _curie_labels(sink, number) == {"curie:pr-open"}
    assert sink.posts == 1
    assert len(_marked(sink, request_id)) == 1

    sink.requests.clear()
    _reconcile()
    assert _writes(sink) == []


def test_a_pending_publication_shows_publishing_and_stays_live(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9907
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    _attach_publication(_request(number)["work_item_id"], status="approved", pr=None)
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert FINAL_MARKER not in comment["body"]
    assert "Status: PUBLISHING" in comment["body"]
    assert _notices(request_id)[0]["finalized_at"] is None


# --- 4 and 5: failure and cancel -----------------------------------------------------------


def test_a_failure_patches_the_plain_reason_and_needs_a_human(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9908
    request_id = _admit(client, github, sink, number)
    _reconcile()
    epoch = _start_running(request_id)
    _finish_failed(
        client, request_id, epoch, "runner_escalated", detail="the tool call kept failing"
    )
    _reconcile()

    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    assert body.startswith("Could not complete:")
    assert "Provider message: the tool call kept failing" in body
    assert "Cause: runner_escalated" in body
    assert "Status: FAILED" in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie:needs-human"}
    assert sink.posts == 1


@pytest.mark.parametrize(
    ("number", "detail"),
    [
        (9916, "run failed: error_max_budget_usd"),
        (9917, "The run reached its output token limit"),
    ],
)
def test_budget_failure_comment_names_both_limits_and_the_usd_command(
    admitted: Any, number: int, detail: str  # noqa: F811
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, number)
    _reconcile()
    comment_id = _notices(request_id)[0]["comment_id"]
    sink.requests.clear()

    epoch = _start_running(request_id)
    _finish_failed(client, request_id, epoch, "budget_exceeded", detail=detail)
    _reconcile()

    assert _posts(sink) == []
    assert [path for path, _ in _patches(sink)] == [
        f"/repos/{REPO}/issues/comments/{comment_id}"
    ]
    (comment,) = _marked(sink, request_id)
    body = comment["body"]
    headline = body.splitlines()[0]
    assert headline.startswith("Could not complete:")
    assert "USD cap" in headline
    assert "output token limit" in headline
    assert "`curie cluster budget <agent> --limit <usd>`" in headline
    assert f"Provider message: {detail}" in body
    assert "Cause: budget_exceeded" in body
    assert "Status: FAILED" in body
    assert FINAL_MARKER in body
    assert _curie_labels(sink, number) == {"curie:needs-human"}
    assert sink.posts == 1


def test_unlabel_while_waiting_stops_and_clears_every_state_label(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9909
    request_id = _admit(client, github, sink, number)
    _reconcile()
    assert _curie_labels(sink, number) == {"curie:queued"}
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancelled"
    _reconcile()

    (comment,) = _marked(sink, request_id)
    assert "Stopped:" in comment["body"]
    assert "Cause: issue_cancelled" in comment["body"]
    assert "Status: CANCELLED" in comment["body"]
    assert FINAL_MARKER in comment["body"]
    assert _curie_labels(sink, number) == set()
    assert ("DELETE", f"/repos/{REPO}/issues/{number}/labels/curie:queued", None) in sink.requests
    assert "bug" in sink.issue_labels[number]
    assert sink.posts == 1


def test_a_running_cancellation_shows_stopping(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9910
    request_id = _admit(client, github, sink, number)
    _start_running(request_id)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "Status: STOPPING" in comment["body"]
    assert FINAL_MARKER not in comment["body"]
    assert _curie_labels(sink, number) == {"curie:running"}


# --- 6: relabel supersedes ------------------------------------------------------------------


def test_relabel_while_waiting_finalizes_the_old_comment_and_opens_a_new_one(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9911
    old_id = _admit(client, github, sink, number)
    _reconcile()
    again = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert again.json()["status"] == "factory_admitted"
    rows = _rows(
        "SELECT r.id FROM curie.execution_requests r JOIN curie.work_items w "
        "ON w.id = r.work_item_id WHERE w.github_issue_number = :n ORDER BY r.sequence",
        {"n": number},
    )
    new_id = rows[1]["id"]
    _reconcile()
    _reconcile()

    (old,) = _marked(sink, old_id)
    assert "a new run replaced this one" in old["body"]
    assert "Cause: issue_cancelled" in old["body"]
    assert FINAL_MARKER in old["body"]
    (new,) = _marked(sink, new_id)
    assert "Status: QUEUED" in new["body"]
    assert FINAL_MARKER not in new["body"]
    assert sink.posts == 2
    assert _curie_labels(sink, number) == {"curie:queued"}
    # The superseded row never cleared the successor's label.
    assert (
        "DELETE",
        f"/repos/{REPO}/issues/{number}/labels/curie:queued",
        None,
    ) not in sink.requests


# --- 7 and 8: lost edit responses and deleted comments ----------------------------------------


def test_a_failed_edit_is_retried_and_never_becomes_a_second_comment(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9912
    request_id = _admit(client, github, sink, number)
    _reconcile()
    _start_running(request_id)
    sink.patch_statuses = [500]
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "Status: QUEUED" in comment["body"]
    _reconcile()
    (comment,) = _marked(sink, request_id)
    assert "Status: RUNNING" in comment["body"]
    assert len(_patches(sink)) == 2
    assert sink.posts == 1


def test_a_comment_deleted_by_a_human_is_recreated_once(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9913
    request_id = _admit(client, github, sink, number)
    _reconcile()
    first_id = _notices(request_id)[0]["comment_id"]
    sink.delete_comment(first_id)
    _start_running(request_id)
    sink.requests.clear()
    _reconcile()
    _reconcile()

    assert sink.posts == 2
    (comment,) = _marked(sink, request_id)
    assert comment["id"] != first_id
    assert "Status: RUNNING" in comment["body"]
    methods = [(method, path) for method, path, _ in sink.requests]
    patch_at = methods.index(("PATCH", f"/repos/{REPO}/issues/comments/{first_id}"))
    list_path = f"/repos/{REPO}/issues/{number}/comments"
    post_at = methods.index(("POST", list_path))
    assert ("GET", list_path) in methods[patch_at:post_at]
    row = _notices(request_id)[0]
    assert row["comment_id"] == comment["id"]

    sink.requests.clear()
    _reconcile()
    assert _posts(sink) == []


# --- 9: revision threads -------------------------------------------------------------------


def _live_revision(
    client: Any, github: Any, sink: Any, fragment: str
) -> tuple[int, int, uuid.UUID, Any]:
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    revision = _insert_revision(first["work_item_id"], number, _revision_objective(pr, fragment))
    _start_running(revision)
    # A revision inserted without a status row gets one from its first report.
    assert report(client, revision, "implement", round=1).status_code == 201
    _reconcile()
    return number, pr, revision, first["work_item_id"]


def test_a_review_thread_revision_replies_then_edits_the_review_comment(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, revision, work_item_id = _live_revision(client, github, sink, "discussion_r88201")
    assert [path for path, _ in _posts(sink)] == [
        f"/repos/{REPO}/pulls/{pr}/comments/88201/replies"
    ]
    row = _notices(revision)[0]
    assert row["comment_list"] == "review"
    reply_id = row["comment_id"]

    _attach_revision_publication(work_item_id, revision)
    sink.requests.clear()
    _reconcile()

    assert _posts(sink) == []
    assert {path for path, _ in _patches(sink)} == {f"/repos/{REPO}/pulls/comments/{reply_id}"}
    (comment,) = _marked(sink, revision)
    assert FINAL_MARKER in comment["body"]
    assert "The requested revision is pushed to this pull request." in comment["body"]
    assert _curie_labels(sink, pr) == set()
    assert _curie_labels(sink, number) == {"curie:pr-open"}


def test_a_refused_thread_reply_lives_on_and_is_edited_on_the_conversation(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    for candidate in range(501, 600):
        sink.refuse_paths[f"/repos/{REPO}/pulls/{candidate}/comments/88202/replies"] = 422
    number, pr, revision, work_item_id = _live_revision(client, github, sink, "discussion_r88202")
    assert [path for path, _ in _posts(sink)] == [
        f"/repos/{REPO}/pulls/{pr}/comments/88202/replies",
        f"/repos/{REPO}/issues/{pr}/comments",
    ]
    row = _notices(revision)[0]
    assert row["comment_list"] == "issue"

    _attach_revision_publication(work_item_id, revision)
    sink.requests.clear()
    _reconcile()
    assert {path for path, _ in _patches(sink)} == {
        f"/repos/{REPO}/issues/comments/{row['comment_id']}"
    }
    assert _curie_labels(sink, pr) == set()


# --- 10: refusal ---------------------------------------------------------------------------


def test_a_refused_edit_records_the_refusal_and_stops_writing(admitted: Any) -> None:  # noqa: F811
    client, github, sink = admitted
    number = 9914
    request_id = _admit(client, github, sink, number)
    _reconcile()
    epoch = _start_running(request_id)
    sink.patch_statuses = [403]
    _reconcile()
    row = _notices(request_id)[0]
    assert row["refused_at"] is not None
    assert row["refusal"] == "http_403"

    _finish_failed(client, request_id, epoch, "runner_failed")
    sink.requests.clear()
    _reconcile()
    _reconcile()
    assert _patches(sink) == []
    assert _posts(sink) == []


# --- 11: a request admitted before the status row existed -----------------------------------------


def test_a_request_without_an_admission_row_still_gets_exactly_one_comment(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    number = 9915
    request_id = _admit(client, github, sink, number)
    _execute(
        "DELETE FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert len(_notices(request_id)) == 1
    _reconcile()
    _reconcile()
    assert sink.posts == 1
    (comment,) = _marked(sink, request_id)
    assert "Stopped:" in comment["body"]
    assert FINAL_MARKER in comment["body"]


def test_declaration_fixture_matches_nine_phases() -> None:
    assert len(DECLARATION["phases"]) == 9
