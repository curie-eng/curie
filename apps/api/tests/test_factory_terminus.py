"""A labelled issue ends as a pull request or exactly one comment.

GitHub issue comments follow
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
and
https://docs.github.com/en/rest/issues/comments#list-issue-comments
Admission is a signed issues webhook against create_app(). Work items are not
inserted by this file.

A revision asked for from pull request review feedback (#2798) answers on the
pull request instead:
https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment
https://docs.github.com/en/rest/pulls/comments#list-review-comments-on-a-pull-request
and a pull request's conversation comments use the issue comment endpoints with
the pull request number. Those revision requests are inserted directly with the
objective the review ingress writes; machine fixtures drive the events, not
human-authored GitHub proof.
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import FINAL_MARKER, cause_text, result_section
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from forge_fakes.github import INSTALLATION_ID, LABEL, REPO, REPO_ID, GitHubAPI, _issue_event, _post
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _LABELS,
    HEAD_A,
    HEAD_B,
    _CommentServer,
    _rows,
    admitted,
    comments,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

pytestmark = pytest.mark.usefixtures("clean_db")


def test_completed_issue_comment_body_requires_pull_request_url() -> None:
    with pytest.raises(ValueError, match="requires its pull request URL"):
        result_section("completed", pr_url=None)


def test_failed_comment_leads_with_a_plain_sentence_not_the_cause_code() -> None:
    body = result_section(
        "model_credit_exhausted",
        pr_url=None,
        detail="API Error: 402 This request requires more credits",
    )
    headline = body.splitlines()[0]
    assert headline.startswith("Could not complete: the model provider refused")
    assert "run out of credits" in headline
    assert "model_credit_exhausted" not in headline
    assert "Provider message: API Error: 402 This request requires more credits" in body
    assert "Cause: model_credit_exhausted" in body


@pytest.mark.parametrize(
    "cause",
    [
        "model_credit_exhausted",
        "model_usage_limited",
        "model_credential_rejected",
        "model_rate_limited",
        "model_error",
        "budget_exceeded",
        "runner_timeout",
        "workspace_error",
        "runner_escalated",
        "unclassified",
        "max_turns",
        "runner_failed",
        "approval_create_failed",
        "no_pull_request",
        "execution_deadline",
        "capacity_wait_expired",
        "owner_lost",
        "issue_cancelled",
        "publication_denied",
        "publication_expired",
        "publication_failed",
        "ci_failed",
        "ci_timeout",
        "ci_unverified",
        "ci_fix_unpublished",
    ],
)
def test_every_terminus_cause_has_its_own_plain_sentence(cause: str) -> None:
    assert cause_text(cause) != cause_text("not-a-cause")
    assert cause not in cause_text(cause)


def test_the_result_section_carries_no_marker() -> None:
    """The marker belongs to the whole status comment, not to its result lines."""

    body = result_section("runner_failed", pr_url=None)
    assert "curie-execution-request" not in body
    assert FINAL_MARKER not in body


def test_superseded_cancel_says_a_new_run_replaced_this_one() -> None:
    body = result_section("issue_cancelled", pr_url=None, superseded=True)
    assert body == (
        "Stopped: the label was added again, so a new run replaced this one.\n"
        "Cause: issue_cancelled\n"
    )


def test_unknown_cause_still_gets_a_sentence_and_its_code() -> None:
    body = result_section("something_new", pr_url=None)
    assert body.splitlines()[0] == (
        "Could not complete: the run stopped for a reason Curie did not recognize."
    )
    assert "Provider message" not in body
    assert "Cause: something_new" in body


def test_ci_failed_notice_labels_its_details_not_a_provider_message() -> None:
    detail = (
        'Rounds: 3\nTried: round 2: "Fix the test" (1 files: src/app.py)\n'
        "Failing checks: unit-tests (failure)"
    )
    body = result_section("ci_failed", pr_url=None, detail=detail)
    assert body.startswith("Could not complete:")
    assert "3 rounds" in body.splitlines()[0]
    assert "Provider message:" not in body
    assert "Details: Rounds: 3" in body
    assert "Failing checks: unit-tests (failure)" in body
    assert "Cause: ci_failed" in body


def test_ci_unverified_notice_says_it_is_not_a_success() -> None:
    body = result_section("ci_unverified", pr_url=None, detail="Reason: github_forbidden")
    assert body.startswith("Could not complete:")
    assert "unverified" in body.splitlines()[0]
    assert "Reason: github_forbidden" in body
    assert "Completed:" not in body


def test_completed_notice_carries_the_no_ci_note() -> None:
    url = f"https://github.com/{REPO}/pull/77"
    body = result_section("completed", pr_url=url, detail="No CI checks appeared within 120 s.")
    assert body.startswith(f"Completed: {url}")
    assert "Note: No CI checks appeared within 120 s." in body


def test_revision_completed_notice_carries_the_no_ci_note() -> None:
    url = f"https://github.com/{REPO}/pull/77"
    body = result_section(
        "completed",
        pr_url=url,
        feedback_url=f"{url}#issuecomment-1",
        detail="No CI checks appeared within 120 s.",
    )
    assert body.startswith("The requested revision is pushed")
    assert "Note: No CI checks appeared within 120 s." in body


def _request(number: int) -> dict[str, Any]:
    rows = _rows(
        "SELECT r.id, r.status, r.terminal_cause, r.version, w.id AS work_item_id, "
        "w.version AS work_version "
        "FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.github_repository_id = :repo AND w.github_issue_number = :number",
        {"repo": REPO_ID, "number": number},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _notices(request_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT execution_request_id, terminal_cause, attempts, posted_at, "
        "comment_id, comment_list, finalized_at, card_token, applied_label, "
        "refused_at, refusal, detail "
        "FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )


def _label(client: Any, github: GitHubAPI, number: int) -> None:
    github.issue_number = number
    github.advance_label_event(number)
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"


def _reconcile() -> None:
    async def go() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        client = aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
        reconciler = WorkItemReconciler(maker, client, get_settings())
        try:
            await reconciler.run_once()
        finally:
            await client.aclose()
            await engine.dispose()

    asyncio.run(go())


def _reconcile_later(seconds: int) -> None:
    """Advance only the reconciler clock. Stored deadlines stay write-once."""

    import curie_api.workitems.lifecycle as workitems

    original = workitems.database_now

    async def later(session: AsyncSession) -> Any:
        real = await original(session)
        return real + timedelta(seconds=seconds)

    workitems.database_now = later
    try:
        _reconcile()
    finally:
        workitems.database_now = original


def _observe_termination(client: Any, request_id: uuid.UUID) -> None:
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    claimed = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination/claim",
        headers=headers,
        json={"owner": "factory-owner"},
    )
    assert claimed.status_code == 200, claimed.text
    recorded = client.post(
        f"/v1/internal/work-items/requests/{request_id}/termination",
        headers=headers,
        json={
            "runtime_epoch": claimed.json()["runtime_epoch"],
            "observation": "runtime stopped",
        },
    )
    assert recorded.status_code == 200, recorded.text


def _start_running(request_id: uuid.UUID) -> int:
    async def go() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                acquired = await acquire(session, request_id, owner="factory-owner", generation=1)
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="factory-owner",
                    generation=1,
                    claim_name="claim-factory",
                    sandbox_name="sbx-factory",
                )
                assert not isinstance(started, DispatchConflict), started
                return int(started.runtime_epoch)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def test_capacity_wait_expiry_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9201
    _label(client, github, number)
    _reconcile_later(31)
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("expired", "capacity_wait_expired")
    notices = _notices(row["id"])
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == "capacity_wait_expired"
    assert notices[0]["posted_at"] is not None
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert body.startswith("Could not complete:")
    assert "Cause: capacity_wait_expired" in body
    assert marker_for(row["id"]) in body
    assert FINAL_MARKER in body
    assert notices[0]["finalized_at"] is not None
    _reconcile()
    assert sink.posts == 1
    assert len(_notices(row["id"])) == 1


def test_label_removal_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9202
    _label(client, github, number)
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancelled"
    row = _request(number)
    assert (row["status"], row["terminal_cause"]) == ("cancelled", "issue_cancelled")
    assert _notices(row["id"])[0]["posted_at"] is None
    _reconcile()
    assert sink.posts == 1
    assert "Stopped:" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_runner_escalation_posts_one_comment_and_completed_needs_a_pull_request(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9203
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    refused = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers=headers,
        json={"runtime_epoch": epoch, "outcome": "completed", "cause": "completed"},
    )
    assert refused.status_code == 409, refused.text
    assert _request(number)["status"] == "running"
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers=headers,
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_escalated"},
    )
    assert failed.status_code == 200, failed.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", "runner_escalated")
    version = terminal["version"]
    _reconcile()
    assert sink.posts == 1
    assert "runner_escalated" in sink.comments[0]["body"]
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])
    assert _request(number)["version"] == version


def test_approval_create_failure_posts_terminal_issue_notice_and_clears_running_label(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9294
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    sink.issue_labels.setdefault(number, set()).add("curie:running")

    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "approval_create_failed",
        },
    )
    assert failed.status_code == 200, failed.text
    _reconcile()

    assert _request(number)["terminal_cause"] == "approval_create_failed"
    assert sink.posts == 1
    assert sink.comments[0]["body"].startswith("Could not complete:")
    assert "curie:running" not in sink.issue_labels[number]
    assert "curie-factory:needs-human" in sink.issue_labels[number]


def test_approval_create_failure_after_first_publication_ends_ci_fix_round(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9295
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    _attach_publication(row["work_item_id"], status="succeeded", pr=4295)
    sink.issue_labels.setdefault(number, set()).add("curie:running")

    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "approval_create_failed",
        },
    )
    assert failed.status_code == 200, failed.text
    _reconcile()

    assert _request(number)["terminal_cause"] == "approval_create_failed"
    assert sink.comments[0]["body"].startswith("Could not complete:")
    assert "curie:running" not in sink.issue_labels[number]


def test_credit_exhausted_finish_comments_the_redacted_provider_message(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9213
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    key = "sk-or-v1-" + "0123456789abcdef" * 4
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": "model_credit_exhausted",
            "detail": f"model error: unknown: API Error: 402 requires more credits {key}",
        },
    )
    assert failed.status_code == 200, failed.text
    notice = _notices(row["id"])[0]
    assert key not in notice["detail"]
    assert "[REDACTED" in notice["detail"]
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert body.startswith("Could not complete: the model provider refused")
    assert "Provider message: model error: unknown: API Error: 402 requires more credits" in body
    assert key not in body


@pytest.mark.parametrize(("number", "cause"), [(9291, "early_stop"), (9292, "no_pull_request")])
def test_an_unpublished_finish_comments_the_agents_redacted_last_message(
    admitted: Any, number: int, cause: str
) -> None:
    """#3128: the agent's final message survives on the notice row and the comment."""

    client, github, sink = admitted
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    key = "sk-or-v1-" + "0123456789abcdef" * 4
    detail = f"I read the issue and stopped. <!-- curie-status:final --> @octocat {key}"
    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": cause, "detail": detail},
    )
    assert finished.status_code == 200, finished.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", cause)
    notice = _notices(row["id"])[0]
    assert notice["detail"] is not None
    assert "I read the issue and stopped." in notice["detail"]
    assert key not in notice["detail"]
    assert "[REDACTED" in notice["detail"]
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert body.startswith("Could not complete:")
    assert "Agent's last message:" in body
    assert "I read the issue and stopped." in body
    assert key not in body
    assert f"Cause: {cause}" in body
    # The model's copy of the final marker is broken; the platform's own is the
    # only one, so the comment is still recognised as exactly one final notice.
    assert body.count(FINAL_MARKER) == 1
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])


@pytest.mark.parametrize("cause", ["early_stop", "approval_create_failed"])
def test_an_early_stop_finish_defers_to_an_in_flight_publication(
    admitted: Any, cause: str
) -> None:
    """#3128: like ``no_pull_request``, publication owns the terminus."""

    client, github, sink = admitted
    number = 9293
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    _attach_publication(row["work_item_id"], status="pending", pr=None)

    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch,
            "outcome": "failed",
            "cause": cause,
            "detail": "stopping",
        },
    )

    assert finished.status_code == 409, finished.text
    assert "publication_pending" in finished.text
    assert _request(number)["status"] == "running"


def test_a_refused_post_leaves_the_terminal_row_unchanged(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9204
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)
    version = row["version"]
    sink.refuse_status = 403
    _reconcile()
    assert sink.posts == 1
    notices = _notices(row["id"])
    assert notices[0]["posted_at"] is None
    assert notices[0]["refused_at"] is not None
    assert notices[0]["refusal"] == "http_403"
    again = _request(number)
    assert again["version"] == version
    assert (again["status"], again["terminal_cause"]) == ("cancelled", "issue_cancelled")


def test_a_crash_between_commit_and_post_still_posts_once(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9205
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)
    sink.comments.append(
        {
            "id": 7444,
            "body": result_section("issue_cancelled", pr_url=None)
            + f"\n{FINAL_MARKER}\n{marker_for(row['id'])}\n",
        }
    )
    _reconcile()
    assert sink.posts == 0
    notices = _notices(row["id"])
    assert notices[0]["posted_at"] is not None
    assert notices[0]["comment_id"] == 7444
    _reconcile()
    assert sink.posts == 0
    assert len(_notices(row["id"])) == 1


def test_a_running_cancellation_comments_only_after_the_runtime_is_observed(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9207
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])
    github.labels = []
    removed = _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    assert removed.json()["status"] == "factory_cancellation_requested"
    requested = _request(number)
    assert requested["status"] == "cancellation_requested"
    # The status row exists from admission; no terminal detail is staged yet.
    assert _notices(row["id"])[0]["terminal_cause"] is None
    headers = {"X-Curie-Worker-Token": "factory-terminus-worker"}
    claimed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/termination/claim",
        headers=headers,
        json={"owner": "factory-owner"},
    )
    assert claimed.status_code == 200, claimed.text
    recorded = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/termination",
        headers=headers,
        json={
            "runtime_epoch": claimed.json()["runtime_epoch"],
            "observation": "runtime stopped after the issue was unlabelled",
        },
    )
    assert recorded.status_code == 200, recorded.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("cancelled", "issue_cancelled")
    _reconcile()
    assert sink.posts == 1
    assert "Stopped:" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_execution_deadline_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9210
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])
    _reconcile_later(1900)
    requested = _request(number)
    assert (requested["status"], requested["terminal_cause"]) == (
        "cancellation_requested",
        "execution_deadline",
    )
    assert _notices(row["id"])[0]["terminal_cause"] is None
    assert FINAL_MARKER not in sink.comments[0]["body"]
    _observe_termination(client, row["id"])
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == (
        "expired",
        "execution_deadline",
    )
    _reconcile()
    assert sink.posts == 1
    assert "execution_deadline" in sink.comments[0]["body"]
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])
    _reconcile()
    assert sink.posts == 1


def test_owner_lost_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9211
    _label(client, github, number)
    row = _request(number)
    _start_running(row["id"])

    async def lapse_heartbeat() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                changed = await connection.execute(
                    text(
                        "UPDATE curie.execution_requests "
                        "SET runtime_heartbeat_expires_at = clock_timestamp() "
                        "- interval '1 second' "
                        "WHERE id = :id AND status = 'running'"
                    ),
                    {"id": row["id"]},
                )
                assert changed.rowcount == 1
        finally:
            await engine.dispose()

    asyncio.run(lapse_heartbeat())
    _reconcile()
    requested = _request(number)
    assert (requested["status"], requested["terminal_cause"]) == (
        "cancellation_requested",
        "owner_lost",
    )
    assert _notices(row["id"])[0]["terminal_cause"] is None
    _observe_termination(client, row["id"])
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", "owner_lost")
    _reconcile()
    assert sink.posts == 1
    assert "owner_lost" in sink.comments[0]["body"]
    _assert_one_final_comment([c["body"] for c in sink.comments], row["id"])


def test_runner_failure_posts_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9212
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    failed = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={"runtime_epoch": epoch, "outcome": "failed", "cause": "runner_failed"},
    )
    assert failed.status_code == 200, failed.text
    terminal = _request(number)
    assert (terminal["status"], terminal["terminal_cause"]) == ("failed", "runner_failed")
    _reconcile()
    assert sink.posts == 1
    assert "runner_failed" in sink.comments[0]["body"]
    assert marker_for(row["id"]) in sink.comments[0]["body"]


def test_publication_expiry_and_failure_each_post_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    cases = ((9213, "expired", "publication_expired"), (9214, "failed", "publication_failed"))
    for number, status, _cause in cases:
        _label(client, github, number)
        row = _request(number)
        _start_running(row["id"])
        _attach_publication(row["work_item_id"], status=status, pr=None)
    _reconcile()
    assert sink.posts == 2
    bodies = [comment["body"] for comment in sink.comments]
    for number, _status, cause in cases:
        row = _request(number)
        assert (row["status"], row["terminal_cause"]) == ("failed", cause)
        assert sum(cause in body for body in bodies) == 1
        assert len(_notices(row["id"])) == 1


def test_failure_and_opened_pull_request_each_post_one_final_comment(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    denied_number = 9208
    opened_number = 9209
    _label(client, github, denied_number)
    _label(client, github, opened_number)
    denied = _request(denied_number)
    opened = _request(opened_number)
    _start_running(denied["id"])
    _start_running(opened["id"])
    _attach_publication(denied["work_item_id"], status="denied", pr=None)
    _attach_publication(opened["work_item_id"], status="succeeded", pr=77)
    _reconcile()
    denied_row = _request(denied_number)
    opened_row = _request(opened_number)
    assert (denied_row["status"], denied_row["terminal_cause"]) == (
        "failed",
        "publication_denied",
    )
    assert (opened_row["status"], opened_row["terminal_cause"]) == ("completed", "completed")
    assert sink.posts == 2
    bodies = [comment["body"] for comment in sink.comments]
    assert sum("Cause: publication_denied" in body for body in bodies) == 1
    opened_url = f"https://github.com/{REPO}/pull/77"
    assert sum(opened_url in body for body in bodies) == 1
    _assert_one_final_comment(bodies, opened_row["id"])
    _assert_one_final_comment(bodies, denied_row["id"])
    assert len(_notices(opened_row["id"])) == 1
    assert len(_notices(denied_row["id"])) == 1


def _attach_publication(
    work_item_id: uuid.UUID, *, status: str, pr: int | None, head_sha: str = HEAD_A
) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    (
                        await conn.execute(
                            text(
                                "SELECT agent_id, conversation_id, repo_full_name, "
                                "github_repository_id, github_installation_id, version "
                                "FROM curie.work_items WHERE id = :id"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                request = (
                    (
                        await conn.execute(
                            text(
                                "SELECT id, version FROM curie.execution_requests "
                                "WHERE work_item_id = :id AND status = 'running'"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                version_id, deployment_id, lineage_id = (
                    uuid.uuid4(),
                    uuid.uuid4(),
                    uuid.uuid4(),
                )
                approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
                pr_url = (
                    None if pr is None else f"https://github.com/{item['repo_full_name']}/pull/{pr}"
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_versions "
                        "(id, agent_id, version_label, created_by) "
                        "VALUES (:id, :agent, 'v1', 'fixture')"
                    ),
                    {"id": version_id, "agent": item["agent_id"]},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) VALUES "
                        "(:id, :agent, :version, CAST('dev' AS curie.environment), 'active')"
                    ),
                    {
                        "id": deployment_id,
                        "agent": item["agent_id"],
                        "version": version_id,
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.thread_publication_lineages "
                        "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                        "base_sha, branch, pr_number, pr_url, head_sha, status, version, "
                        "latest_revision) VALUES "
                        "(:id, :agent, :deployment, :conversation, :repo, :base, "
                        ":branch, :pr, :url, :head, 'open', 1, 1)"
                    ),
                    {
                        "id": lineage_id,
                        "agent": item["agent_id"],
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "repo": item["repo_full_name"],
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "branch": f"curie/publication-{lineage_id.hex}",
                        "pr": pr,
                        "url": pr_url,
                        "head": head_sha if pr is not None else None,
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals "
                        "(id, agent_id, conversation_id, author, summary, reply_kind, "
                        "reply_channel, dedupe_key, status, purpose) VALUES "
                        "(:id, :agent, :conversation, 'U0REQUEST1', "
                        "'Publish repository changes', 'github', :channel, :dedupe, "
                        "'approved', 'publication')"
                    ),
                    {
                        "id": approval_id,
                        "agent": item["agent_id"],
                        "conversation": item["conversation_id"],
                        "channel": item["repo_full_name"],
                        "dedupe": f"terminus-{publication_id.hex}",
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.publications "
                        "(id, approval_id, deployment_id, workspace_conversation_id, "
                        "lineage_id, execution_request_id, revision_number, repo_full_name, "
                        "status, base_sha, changed_paths, title, body, reply_kind, "
                        "reply_channel, result_url, terminal_at) "
                        "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                        ":request_id, 1, :repo, :status, :base, "
                        "CAST('[\"README.md\"]' AS jsonb), "
                        "'Update README', 'Approved platform publication.', 'github', "
                        ":channel, :result, clock_timestamp())"
                    ),
                    {
                        "id": publication_id,
                        "approval": approval_id,
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "lineage": lineage_id,
                        "request_id": request["id"],
                        "repo": item["repo_full_name"],
                        "status": status,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "channel": item["repo_full_name"],
                        "result": pr_url,
                    },
                )
                changed = await conn.execute(
                    text(
                        "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
                        "version = version + 1 WHERE id = :id AND version = :version"
                    ),
                    {
                        "lineage": lineage_id,
                        "id": work_item_id,
                        "version": item["version"],
                    },
                )
                if changed.rowcount != 1:
                    raise AssertionError(f"work item {work_item_id} was not linked")
                if request["id"] is None:
                    raise AssertionError("running request is missing")
        finally:
            await engine.dispose()

    asyncio.run(go())


def _set_base_ref(work_item_id: uuid.UUID, base_ref: str) -> None:
    """Give the work item's lineage a PR base branch (#4105).

    The identity check constraint wants the repository id, installation id, PR
    node id and base ref all set together, so they are written in one UPDATE.
    """

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                changed = await conn.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET "
                        "github_repository_id = :repo_id, "
                        "github_installation_id = :installation_id, "
                        "github_pr_node_id = :node_id, base_ref = :base_ref "
                        "WHERE id = (SELECT publication_lineage_id FROM curie.work_items "
                        "WHERE id = :id)"
                    ),
                    {
                        "repo_id": REPO_ID,
                        "installation_id": INSTALLATION_ID,
                        "node_id": f"PR_fixture_{work_item_id.hex}",
                        "base_ref": base_ref,
                        "id": work_item_id,
                    },
                )
                if changed.rowcount != 1:
                    raise AssertionError(f"work item {work_item_id} has no lineage")
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_concurrent_reconcilers_post_one_comment(admitted: Any) -> None:
    client, github, sink = admitted
    number = 9206
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)

    async def both() -> None:
        import redis.asyncio as aioredis

        engine = create_async_engine(get_settings().database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        clients = [
            aioredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
            for _ in range(2)
        ]
        reconcilers = [WorkItemReconciler(maker, client, get_settings()) for client in clients]
        try:
            await asyncio.gather(*(item._sync_status_comments() for item in reconcilers))
        finally:
            for client in clients:
                await client.aclose()
            await engine.dispose()

    asyncio.run(both())
    assert sink.posts == 1
    assert len(_notices(row["id"])) == 1
    assert _notices(row["id"])[0]["posted_at"] is not None


# --- #2798: a revision asked for on the pull request answers on the pull request.

_REVISION_PR = iter(range(501, 600))


def _revision_objective(pr: int, fragment: str) -> str:
    """Line 1 is the canonical URL. Notice routing reads only that line."""

    url = f"https://github.com/{REPO}/pull/{pr}#{fragment}"
    provenance = json.dumps(
        {
            "event": "pull_request_review_comment",
            "url": url,
            "sender": "octocat",
            "body": "@curie please rename the helper",
        }
    )
    return f"{url}\n\nReview feedback asked for another revision.\n{provenance}"


def _work_item_row(work_item_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT w.id, w.conversation_id, w.agent_id, w.github_issue_number, "
        "w.publication_lineage_id, l.deployment_id, l.pr_number "
        "FROM curie.work_items w "
        "JOIN curie.thread_publication_lineages l ON l.id = w.publication_lineage_id "
        "WHERE w.id = :id",
        {"id": work_item_id},
    )[0]


def _insert_revision(work_item_id: uuid.UUID, number: int, objective: str) -> uuid.UUID:
    request_id = uuid.uuid4()

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.execution_requests "
                        "(id, work_item_id, sequence, status, wait_deadline, objective, "
                        "requester, reply_kind, reply_address, reply_conversation_id) "
                        "VALUES (:id, :work_item, 2, 'waiting', "
                        "clock_timestamp() + interval '30 seconds', :objective, "
                        "'github:6601:octocat', 'github', :repo, :conversation)"
                    ),
                    {
                        "id": request_id,
                        "work_item": work_item_id,
                        "objective": objective,
                        "repo": REPO,
                        "conversation": f"issue-{number}",
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.work_items SET next_sequence = 3, "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"id": work_item_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())
    return request_id


def _attach_revision_publication(
    work_item_id: uuid.UUID, request_id: uuid.UUID, *, head_sha: str = HEAD_B
) -> None:
    item = _work_item_row(work_item_id)
    approval_id, publication_id = uuid.uuid4(), uuid.uuid4()
    pr_url = f"https://github.com/{REPO}/pull/{item['pr_number']}"

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.approvals "
                        "(id, agent_id, conversation_id, author, summary, reply_kind, "
                        "reply_channel, dedupe_key, status, purpose) VALUES "
                        "(:id, :agent, :conversation, 'U0REQUEST1', "
                        "'Publish repository changes', 'github', :channel, :dedupe, "
                        "'approved', 'publication')"
                    ),
                    {
                        "id": approval_id,
                        "agent": item["agent_id"],
                        "conversation": item["conversation_id"],
                        "channel": REPO,
                        "dedupe": f"terminus-revision-{publication_id.hex}",
                    },
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.publications "
                        "(id, approval_id, deployment_id, workspace_conversation_id, "
                        "lineage_id, execution_request_id, revision_number, repo_full_name, "
                        "status, base_sha, changed_paths, title, body, reply_kind, "
                        "reply_channel, result_url, terminal_at) "
                        "VALUES (:id, :approval, :deployment, :conversation, :lineage, "
                        ":request_id, 2, :repo, 'succeeded', :base, "
                        "CAST('[\"README.md\"]' AS jsonb), "
                        "'Rename the helper', 'Approved platform publication.', 'github', "
                        ":channel, :result, clock_timestamp())"
                    ),
                    {
                        "id": publication_id,
                        "approval": approval_id,
                        "deployment": item["deployment_id"],
                        "conversation": item["conversation_id"],
                        "lineage": item["publication_lineage_id"],
                        "request_id": request_id,
                        "repo": REPO,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "channel": REPO,
                        "result": pr_url,
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.thread_publication_lineages SET latest_revision = 2, "
                        "head_sha = :head WHERE id = :id"
                    ),
                    {"id": item["publication_lineage_id"], "head": head_sha},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())


def _published_issue(client: Any, github: GitHubAPI, sink: _CommentServer) -> tuple[int, int, Any]:
    """An issue whose first run opened a PR and posted its one final comment."""

    number = next(_REVISION_ISSUES)
    pr = next(_REVISION_PR)
    _label(client, github, number)
    first = _request(number)
    _start_running(first["id"])
    _attach_publication(first["work_item_id"], status="succeeded", pr=pr)
    _reconcile()
    done = _request(number)
    assert (done["status"], done["terminal_cause"]) == ("completed", "completed")
    notices = _notices(first["id"])
    assert len(notices) == 1
    assert notices[0]["posted_at"] is not None
    assert sink.posts == 1
    path = f"/repos/{REPO}/issues/{number}/comments"
    body = sink.lists[path][0]["body"]
    assert f"https://github.com/{REPO}/pull/{pr}" in body
    assert marker_for(first["id"]) in body
    return number, pr, first


_REVISION_ISSUES = iter(range(9301, 9399))


def _complete_revision(
    client: Any, github: GitHubAPI, sink: _CommentServer, fragment: str
) -> tuple[int, int, uuid.UUID]:
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    objective = _revision_objective(pr, fragment)
    revision = _insert_revision(first["work_item_id"], number, objective)
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)
    _reconcile()
    status = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": revision},
    )[0]
    assert (status["status"], status["terminal_cause"]) == ("completed", "completed")
    return number, pr, revision


def _posts(sink: _CommentServer) -> list[tuple[str, str | None]]:
    """Comment creations only; label writes are asserted separately."""

    return [
        (path, body)
        for method, path, body in sink.requests
        if method == "POST" and not _LABELS.match(path)
    ]


def _patches(sink: _CommentServer) -> list[tuple[str, str | None]]:
    return [(path, body) for method, path, body in sink.requests if method == "PATCH"]


def _assert_one_final_comment(bodies: list[str], request_id: uuid.UUID) -> str:
    """Exactly one comment carries this request's marker, and it is final."""

    marked = [body for body in bodies if marker_for(request_id) in body]
    assert len(marked) == 1, marked
    assert FINAL_MARKER in marked[0]
    return marked[0]


def test_a_completed_first_request_posts_one_comment_naming_its_pull_request(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, _first = _published_issue(client, github, sink)
    posts = _posts(sink)
    assert [path for path, _body in posts] == [f"/repos/{REPO}/issues/{number}/comments"]
    assert f"https://github.com/{REPO}/pull/{pr}" in (posts[0][1] or "")
    _reconcile()
    assert len(_posts(sink)) == 1


def test_a_completed_revision_queues_exactly_one_notice(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _number, _pr, revision = _complete_revision(client, github, sink, "discussion_r88101")
    notices = _notices(revision)
    assert len(notices) == 1
    assert notices[0]["terminal_cause"] == "completed"
    _reconcile()
    assert len(_notices(revision)) == 1


def test_a_review_comment_revision_replies_in_its_thread(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    _number, pr, revision = _complete_revision(client, github, sink, "discussion_r88102")
    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/pulls/{pr}/comments/88102/replies"]
    assert marker_for(revision) in (posts[0][1] or "")
    assert _notices(revision)[0]["comment_list"] == "review"
    assert _notices(revision)[0]["posted_at"] is not None
    _reconcile()
    assert len(_posts(sink)) == 1


@pytest.mark.parametrize("fragment", ["issuecomment-88103", "pullrequestreview-88104"])
def test_conversation_and_review_revisions_comment_on_the_pull_request(
    admitted: Any, fragment: str
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, pr, revision = _complete_revision(client, github, sink, fragment)
    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/issues/{pr}/comments"]
    assert pr != number
    body = posts[0][1] or ""
    assert f"https://github.com/{REPO}/pull/{pr}#{fragment}" in body
    assert marker_for(revision) in body
    assert _notices(revision)[0]["posted_at"] is not None


def test_a_refused_thread_reply_falls_back_to_a_pull_request_comment(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    reply = None
    # The PR number is only known once the first run publishes, so refuse every
    # reply path this test could produce.
    for pr in range(501, 600):
        reply = f"/repos/{REPO}/pulls/{pr}/comments/88105/replies"
        sink.refuse_paths[reply] = 422
    _number, pr, revision = _complete_revision(client, github, sink, "discussion_r88105")
    posts = [path for path, _ in _posts(sink)]
    assert posts == [
        f"/repos/{REPO}/pulls/{pr}/comments/88105/replies",
        f"/repos/{REPO}/issues/{pr}/comments",
    ]
    fallback = sink.lists[f"/repos/{REPO}/issues/{pr}/comments"][0]["body"]
    assert marker_for(revision) in fallback
    assert f"https://github.com/{REPO}/pull/{pr}#discussion_r88105" in fallback
    assert _notices(revision)[0]["posted_at"] is not None


@pytest.mark.parametrize(
    ("fragment", "listed"),
    [
        ("discussion_r88106", "pulls/{pr}/comments"),
        ("discussion_r88107", "issues/{pr}/comments"),
        ("issuecomment-88108", "issues/{pr}/comments"),
    ],
)
def test_a_marker_already_on_the_pull_request_is_not_posted_again(
    admitted: Any, fragment: str, listed: str
) -> None:
    """A crash after the post leaves the marker on either PR list. No second post."""

    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    revision = _insert_revision(first["work_item_id"], number, _revision_objective(pr, fragment))
    sink.lists[f"/repos/{REPO}/{listed.format(pr=pr)}"] = [
        {
            "id": 7555,
            "body": result_section(
                "completed",
                pr_url=None,
                feedback_url=_revision_objective(pr, fragment),
            )
            + f"\n{marker_for(revision)}\n",
        }
    ]
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)
    _reconcile()
    assert _posts(sink) == []
    notices = _notices(revision)
    assert len(notices) == 1
    assert notices[0]["comment_id"] == 7555
    assert notices[0]["posted_at"] is not None


def _scan_page(request_id: uuid.UUID) -> int:
    return _rows(
        "SELECT scan_page FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )[0]["scan_page"]


def test_a_lost_thread_reply_response_rescans_every_list(admitted: Any) -> None:
    """A thread reply that posts but whose response is lost must not double-post.

    The conversation list holds more than 500 comments, so the first pass
    exhausts the review list, then advances the cursor past the conversation
    list's offset without finishing it. A second pass finishes the conversation
    list and attempts the thread reply, whose response is then lost.
    """

    client, github, sink = admitted
    sink.by_path = True
    number, pr, first = _published_issue(client, github, sink)
    sink.requests.clear()
    fragment = "discussion_r88109"
    revision = _insert_revision(first["work_item_id"], number, _revision_objective(pr, fragment))
    sink.lists[f"/repos/{REPO}/issues/{pr}/comments"] = [
        {"id": 9000 + i, "body": f"unrelated comment {i}"} for i in range(550)
    ]
    reply_path = f"/repos/{REPO}/pulls/{pr}/comments/88109/replies"
    sink.lost_response_paths.add(reply_path)
    _start_running(revision)
    _attach_revision_publication(first["work_item_id"], revision)

    _reconcile()
    assert _posts(sink) == []
    assert _notices(revision)[0]["posted_at"] is None
    assert _scan_page(revision) > 1_000_000

    _reconcile()
    assert [path for path, _ in _posts(sink)] == [reply_path]
    assert _notices(revision)[0]["posted_at"] is None
    assert _scan_page(revision) == 1

    _reconcile()
    assert [path for path, _ in _posts(sink)] == [reply_path]
    notices = _notices(revision)
    assert notices[0]["posted_at"] is not None
    assert notices[0]["comment_id"] == 8002


def test_an_issue_originated_notice_still_comments_on_the_issue(admitted: Any) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number = 9398
    _label(client, github, number)
    github.labels = []
    _post(client, "issues", _issue_event("unlabeled", number, label={"name": LABEL}))
    row = _request(number)
    _reconcile()
    posts = _posts(sink)
    assert [path for path, _ in posts] == [f"/repos/{REPO}/issues/{number}/comments"]
    assert marker_for(row["id"]) in (posts[0][1] or "")



def test_factory_notices_usage_limited_finish_posts_the_reset_remedy_once(
    admitted: Any,
) -> None:
    client, github, sink = admitted
    number = 9214
    _label(client, github, number)
    row = _request(number)
    epoch = _start_running(row["id"])
    finished = client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers={"X-Curie-Worker-Token": "factory-terminus-worker"},
        json={
            "runtime_epoch": epoch, "outcome": "failed", "cause": "model_usage_limited",
            "detail": "You've hit your session limit · resets 3pm (UTC)",
        },
    )
    assert finished.status_code == 200, finished.text
    assert _request(number)["terminal_cause"] == "model_usage_limited"
    _reconcile()
    _reconcile()
    assert sink.posts == 1
    body = sink.comments[0]["body"]
    assert "the model provider's usage limit for this credential was reached" in body
    assert "re-add the label after the limit resets" in body
    assert "add credits" not in body
    assert "Cause: model_usage_limited" in body
    assert "Failure class: model-usage-limited" in body
